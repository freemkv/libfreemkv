//! The tree sink (pipeline design §2.2, slice 7): where a whole-disc copy to `dir://` goes.
//! A block sink takes sectors; a tree sink takes the disc's files, one at a time, and lays
//! them out as the decrypted folder: host-safe names, no two disc paths on one host file,
//! each file written `<name>.partial` and published by rename once whole. The disc's
//! `AACS/`, `CERTIFICATE/` and HD DVD AACS directories are not part of a decrypted tree
//! (X-8): the sink drops them. [`open_tree_sink`] opens one from a URL.

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Attempts to delete the case-probe marker before giving up.
const PROBE_REMOVE_ATTEMPTS: u32 = 3;

/// A decrypted file-tree output (`dir://`).
pub struct TreeSink {
    dest: PathBuf,
    // Whether the target volume folds case (APFS/NTFS): two names differing only by case
    // are one host file there.
    case_insensitive: bool,
    // Host paths handed out so far (case-folded where the volume folds) → their disc path.
    seen: HashMap<String, String>,
}

impl TreeSink {
    /// Create (or reuse) the folder at `dest`. A non-empty folder is refused
    /// ([`Error::DirNotEmpty`]) unless `force`: two discs' trees must not mix.
    pub fn create(dest: &Path, force: bool) -> Result<TreeSink> {
        std::fs::create_dir_all(dest).map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
        if !force && dir_is_non_empty(dest) {
            return Err(Error::DirNotEmpty);
        }
        Ok(TreeSink {
            dest: dest.to_path_buf(),
            case_insensitive: false,
            seen: HashMap::new(),
        })
    }

    /// The folder the tree is written into.
    pub fn dest(&self) -> &Path {
        &self.dest
    }

    // Probe the target volume's case rule; called once the structure is about to be planned.
    pub(crate) fn probe_case(&mut self) {
        self.case_insensitive = dir_is_case_insensitive(&self.dest);
    }

    #[cfg(test)]
    pub(crate) fn with_case_insensitive(mut self, yes: bool) -> Self {
        self.case_insensitive = yes;
        self
    }

    /// Whether a top-level disc entry belongs in the decrypted tree (X-8): the AACS and
    /// certificate directories (and HD DVD's discovered AACS directory, `aacs_dir`) do not.
    pub(crate) fn keeps_top_level(name: &str, aacs_dir: bool) -> bool {
        !(name.eq_ignore_ascii_case("AACS") || name.eq_ignore_ascii_case("CERTIFICATE") || aacs_dir)
    }

    /// Claim the host path for disc entry `disc_path` (named `name`) under `parent`: the
    /// sanitized component, refused ([`Error::DirNameCollision`]) when another disc path
    /// already maps to it. A file also claims its `.partial` name.
    pub(crate) fn claim(
        &mut self,
        parent: &Path,
        name: &str,
        disc_path: &str,
        is_dir: bool,
    ) -> Result<PathBuf> {
        let rel = parent.join(sanitize_component(name));
        self.register(&rel, disc_path)?;
        if !is_dir {
            self.register(&with_partial_suffix(&rel), disc_path)?;
        }
        Ok(rel)
    }

    // Collision: two distinct disc paths → one host FILE. Case-insensitive folds
    // `Movie`/`movie`; case-sensitive collides only on EXACT match (else names coexist).
    // `.partial` shares the final-name namespace, so `X`/`X.partial` collide.
    fn register(&mut self, key: &Path, disc_path: &str) -> Result<()> {
        let folded = if self.case_insensitive {
            key.to_string_lossy().to_lowercase()
        } else {
            key.to_string_lossy().into_owned()
        };
        if self.seen.insert(folded, disc_path.to_string()).is_some() {
            return Err(Error::DirNameCollision {
                host: key.to_string_lossy().into_owned(),
            });
        }
        Ok(())
    }

    /// Refuse up front when `required` bytes will not fit (best-effort: only where the
    /// platform reports free space).
    pub(crate) fn reserve(&self, required: u64) -> Result<()> {
        match available_space(&self.dest) {
            Some(available) if available < required => Err(Error::DirInsufficientSpace {
                required,
                available,
            }),
            _ => Ok(()),
        }
    }

    /// Create the host directories `dirs` (relative), parents first.
    pub(crate) fn make_dirs(&self, dirs: &[PathBuf]) -> Result<()> {
        for d in dirs {
            std::fs::create_dir_all(self.dest.join(d)).map_err(|e| Error::DirWriteFailed {
                errno: e.raw_os_error(),
            })?;
        }
        Ok(())
    }

    /// Start the file at host path `rel` (declared `size` bytes): written as `.partial`.
    pub(crate) fn begin(&self, rel: &Path, size: u64) -> Result<TreeFile> {
        let final_path = self.dest.join(rel);
        let partial = with_partial_suffix(&final_path);
        let writer =
            crate::io::WritebackFile::create_with_size_hint(&partial, size).map_err(|e| {
                Error::DirWriteFailed {
                    errno: e.raw_os_error(),
                }
            })?;
        Ok(TreeFile {
            writer,
            partial,
            final_path,
            size,
        })
    }
}

/// One file of a [`TreeSink`], being written.
pub(crate) struct TreeFile {
    writer: crate::io::WritebackFile,
    partial: PathBuf,
    final_path: PathBuf,
    size: u64,
}

impl TreeFile {
    /// Append `data`. A failed write removes the `.partial`.
    pub(crate) fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data).map_err(|e| {
            let _ = std::fs::remove_file(&self.partial);
            Error::DirWriteFailed {
                errno: e.raw_os_error(),
            }
        })
    }

    /// Publish the file: flush, sync, set its exact declared length, rename `.partial` →
    /// final, sync the directory entry. A file left unfinished (a Stop) stays `.partial`.
    pub(crate) fn finish(self) -> Result<()> {
        let TreeFile {
            mut writer,
            partial,
            final_path,
            size,
        } = self;
        writer.sync_all().map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
        drop(writer);
        // Set the exact declared length (covers both an over-read final sector and
        // an under-covered sparse tail).
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&partial)
            .map_err(|e| Error::DirWriteFailed {
                errno: e.raw_os_error(),
            })?;
        f.set_len(size).map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
        // `set_len` is a separate kernel op on this second handle; without an fsync
        // here, a crash between `set_len` and the rename can leave the file at its
        // pre-truncation length. Sync the new metadata (length) before publishing.
        f.sync_all().map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
        drop(f);
        std::fs::rename(&partial, &final_path).map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
        // Durably commit the new dirent: on POSIX filesystems a crash right after a
        // rename can lose the directory entry even though the rename returned. Best
        // effort (swallowed on failure); no-op on Windows. Matches `write_atomic`.
        if let Some(dir) = final_path.parent() {
            crate::io::fsync::dir(dir);
        }
        Ok(())
    }
}

/// Open the tree output `url` (`dir://<path>`); `force` writes into a non-empty folder.
/// Any other scheme is [`Error::StreamUrlInvalid`].
pub fn open_tree_sink(url: &str, force: bool) -> Result<TreeSink> {
    match crate::mux::parse_url(url) {
        crate::mux::StreamUrl::Dir { path } => TreeSink::create(&path, force),
        _ => Err(Error::StreamUrlInvalid {
            url: url.to_string(),
        }),
    }
}

/// Whether the filesystem holding `dir` folds case: create a lowercase marker, test for its
/// uppercase spelling. Two disc names differing only in case are distinct on a
/// case-sensitive volume, so folding them together would wrongly abort a legitimate
/// extract. If the probe can't run (e.g. a read-only dir), assume case-insensitive — the
/// conservative choice that never MISSES a real overwrite collision.
pub(crate) fn dir_is_case_insensitive(dir: &Path) -> bool {
    probe_case_insensitive(dir, |p| std::fs::remove_file(p))
}

// `dir_is_case_insensitive` with the marker removal injectable (tests fail it).
pub(crate) fn probe_case_insensitive(
    dir: &Path,
    mut remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> bool {
    // Unique per attempt: a FIXED name let concurrent extracts race — one's
    // `remove_file` could delete another's marker between create and `exists()`
    // (a TOCTOU flip). A per-call token keeps each probe's pair on private paths.
    let token = unique_probe_token();
    let lower_name = format!(".fmkv_case_probe_{token}");
    let lower = dir.join(&lower_name);
    if std::fs::File::create(&lower).is_err() {
        return true;
    }
    // The upper spelling is the SAME name uppercased end-to-end (hex token
    // letters `a-f` fold to `A-F`), so on a case-insensitive volume it resolves
    // to the file just created and on a case-sensitive one it does not exist.
    let upper = dir.join(lower_name.to_ascii_uppercase());
    let insensitive = upper.exists();
    // Retry: a transient delete failure (Windows AV/indexer handle) would leave the
    // marker in the user's target, failing a re-run's non-empty check.
    for attempt in 0..PROBE_REMOVE_ATTEMPTS {
        match remove(&lower) {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) if attempt + 1 < PROBE_REMOVE_ATTEMPTS => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            // Still failing: best-effort by design; the probe's answer stands.
            Err(_) => {}
        }
    }
    insensitive
}

/// A process-unique, lowercase-hex token for the case-probe filename. No `rand`
/// dependency: a monotonic counter (distinguishes concurrent same-process
/// probes) mixed with the pid and a nanosecond clock (distinguishes processes /
/// runs). Hex digits are case-foldable, which the probe relies on.
pub(crate) fn unique_probe_token() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{pid:x}_{nanos:x}_{n:x}")
}

/// Append `.partial` to a path's filename.
pub(crate) fn with_partial_suffix(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".partial");
    path.with_file_name(name)
}

// Sanitizes ONE disc-path component: host-illegal chars and control bytes become `_`, trailing
// dot/space are stripped, a Windows reserved device name gets a `_` prefix. Names that
// collapse together are caught by the collision check.
pub(crate) fn sanitize_component(name: &str) -> String {
    let mapped: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();
    let trimmed = mapped.trim_end_matches([' ', '.']);
    if trimmed.is_empty() {
        return "_".to_string();
    }
    // The device name is the stem before any extension, ignoring trailing spaces.
    let base = trimmed.split('.').next().unwrap_or(trimmed);
    if is_windows_reserved(base.trim_end_matches(' ')) {
        return format!("_{trimmed}");
    }
    trimmed.to_string()
}

/// Whether `base` (the name component before any extension) matches a Windows
/// reserved device name. These are reserved by the OS regardless of extension
/// and silently alias a device (e.g. `NUL` discards writes). Case-insensitive.
pub(crate) fn is_windows_reserved(base: &str) -> bool {
    let up = base.trim_end_matches(' ').to_ascii_uppercase();
    if matches!(
        up.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
    ) {
        return true;
    }
    ["COM", "LPT"].iter().any(|p| {
        up.strip_prefix(p).is_some_and(|d| {
            let mut c = d.chars();
            matches!(
                (c.next(), c.next()),
                (Some('0'..='9' | '\u{B9}' | '\u{B2}' | '\u{B3}'), None)
            )
        })
    })
}

/// Available free bytes on the filesystem holding `dir`, or `None` when the
/// platform doesn't expose it (the free-space gate is then skipped).
#[cfg(unix)]
pub(crate) fn available_space(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let cpath = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // No safe std API for free space. SAFETY: `statvfs` is plain-old-data (all-zero is
    // valid); `cpath` is NUL-terminated and outlives the call; `st` is a valid out-pointer.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut st) };
    if rc != 0 {
        return None;
    }
    // `statvfs` field integer widths differ by platform; cast both to `u64`
    // for the product (no-op where already `u64` — allow lint for portability).
    #[allow(clippy::unnecessary_cast, clippy::useless_conversion)]
    let avail = (st.f_bavail as u64).saturating_mul(st.f_frsize as u64);
    Some(avail)
}

// Windows has no `statvfs`; queries free space via `GetDiskFreeSpaceExW` (declared directly
// against kernel32, matching `scsi::windows`) so the free-space gate still runs.
#[cfg(windows)]
pub(crate) fn available_space(dir: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;

    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    // Wide, NUL-terminated. An interior NUL cannot reach the API, so reject it
    // rather than silently truncating the path and measuring the wrong volume.
    let mut wide: Vec<u16> = dir.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return None;
    }
    wide.push(0);

    let mut avail: u64 = 0;
    // FreeBytesAvailableToCaller honours per-user quotas (unix `f_bavail`).
    // SAFETY: `wide` is NUL-terminated and outlives the call; `avail` is a valid
    // out-pointer; the two NULL out-params are documented optional.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut avail,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if ok == 0 { None } else { Some(avail) }
}

/// Neither unix nor Windows: no way to ask, so the gate is skipped.
#[cfg(not(any(unix, windows)))]
pub(crate) fn available_space(_dir: &Path) -> Option<u64> {
    None
}

/// Whether a directory exists and contains any entry.
pub(crate) fn dir_is_non_empty(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|mut it| it.next().is_some())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dir_url_opens_a_tree_sink_and_other_schemes_do_not() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("tree");
        let sink = open_tree_sink(&format!("dir://{}", out.display()), false).unwrap();
        assert_eq!(sink.dest(), out.as_path());
        assert!(out.is_dir());
        assert!(open_tree_sink("iso:///tmp/x.iso", false).is_err());
    }

    #[test]
    fn a_published_file_has_its_declared_length_and_no_partial() {
        let dir = tempfile::tempdir().unwrap();
        let sink = TreeSink::create(dir.path(), false).unwrap();
        let mut f = sink.begin(Path::new("a.bin"), 3).unwrap();
        f.write(b"abcdef").unwrap();
        f.finish().unwrap();
        assert_eq!(std::fs::read(dir.path().join("a.bin")).unwrap(), b"abc");
        assert!(!dir.path().join("a.bin.partial").exists());
    }

    #[test]
    fn the_aacs_directories_are_not_part_of_the_tree() {
        assert!(!TreeSink::keeps_top_level("AACS", false));
        assert!(!TreeSink::keeps_top_level("certificate", false));
        assert!(!TreeSink::keeps_top_level("X!", true));
        assert!(TreeSink::keeps_top_level("BDMV", false));
    }
}
