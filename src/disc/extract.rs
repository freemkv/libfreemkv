//! `Disc::extract_tree` — decrypted file-tree extraction (`dir://`).
//!
//! Sibling of [`Disc::copy`](super::Disc::copy) (disc → ISO sector dump),
//! specialized to write **per file** rather than a whole image, applying
//! decryption on the way out, and **without** any multipass / recovery
//! orchestration. 1-shot, decrypt-only.
//!
//! The filesystem enumeration and decryption are entirely reused:
//! [`udf::read_filesystem`] yields the recursive [`UdfFs`] tree (BD and DVD
//! alike — DVD-Video is a UDF bridge volume), and
//! [`DecryptingSectorSource`](crate::sector::DecryptingSectorSource) applies
//! AACS / CSS in-place. This module is the focused per-file producer: tree
//! walk, host-path mapping + per-component sanitization, per-VTS CSS key
//! grouping, decrypt-and-stream-to-disk, `AACS/` strip, sparse-gap handling,
//! truncate-to-size, and `.partial` + rename. No new error codes — it reuses
//! the existing `Error` set (`error.rs` is numeric-only, no English).

use super::Disc;
use crate::decrypt::DecryptKeys;
use crate::error::{Error, Result};
use crate::sector::{DecryptingSectorSource, SectorSource};
use crate::udf::{self, DirEntry, UdfFs};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

const SECTOR_LEN: usize = 2048;
/// AACS aligned unit = 3 sectors / 6144 bytes. Content reads are issued in
/// multiples of this so the decrypt step always sees whole units.
const AACS_UNIT_SECTORS: u32 = 3;
/// Read batch in sectors for content streaming (a throughput knob, not a
/// correctness one). A multiple of 3 so AACS units stay whole.
const READ_BATCH_SECTORS: u32 = 1536; // 3 MiB, multiple of 3
/// Bounded per-extent retries on a read that fails before a recorded hole.
const READ_RETRIES: u32 = 3;

/// Options for [`Disc::extract_tree`].
#[derive(Default)]
pub struct ExtractOptions<'a> {
    /// Overwrite into a non-empty destination directory. Without it a
    /// non-empty target is refused (mixing two discs' trees).
    pub force: bool,
    /// Optional progress sink. `report` returning `false` requests an early
    /// stop (the run finalizes whatever completed; in-flight files stay
    /// `.partial`).
    pub progress: Option<&'a dyn crate::progress::Progress>,
    /// Cooperative cancel token. When cancelled (e.g. the CLI bridges its
    /// SIGINT flag here), the run stops at the next file / batch boundary and
    /// the in-flight file is left as `.partial` — never a half-written file
    /// that looks complete. `None` disables cancellation.
    pub halt: Option<crate::halt::Halt>,
}

impl ExtractOptions<'_> {
    /// Whether the caller asked to stop (halt cancelled or a progress sink
    /// returned `false` on its last report).
    fn cancelled(&self, progress_continue: bool) -> bool {
        !progress_continue || self.halt.as_ref().is_some_and(|h| h.is_cancelled())
    }
}

/// Per-file extraction outcome.
#[derive(Debug, Clone)]
pub struct FileResult {
    /// Host-relative path (the mirrored disc path, sanitized).
    pub path: PathBuf,
    /// Bytes written that decrypted cleanly.
    pub bytes_good: u64,
    /// Bytes lost to unreadable sectors (zero-filled holes).
    pub bytes_unreadable: u64,
    /// Bytes lost to undecryptable AACS/CSS units (still ciphertext / dropped).
    pub bytes_undecryptable: u64,
    /// True when the file was fully written (renamed from `.partial`).
    pub complete: bool,
}

/// Aggregate result of an [`extract_tree`](Disc::extract_tree) run.
#[derive(Debug, Clone, Default)]
pub struct ExtractResult {
    /// Per-file results, in extraction order.
    pub files: Vec<FileResult>,
    /// Aggregate good bytes across all files.
    pub bytes_good: u64,
    /// Aggregate unreadable (bad-sector) bytes.
    pub bytes_unreadable: u64,
    /// Aggregate undecryptable (decrypt-loss) bytes.
    pub bytes_undecryptable: u64,
    /// True when every file completed and no loss was recorded.
    pub complete: bool,
    /// True when the run stopped early on an interrupt / progress halt.
    pub halted: bool,
}

impl ExtractResult {
    /// Total bytes lost (unreadable + undecryptable). A non-zero value means
    /// the extraction is holed; the CLI exits non-zero so a script can re-run
    /// through the `iso://` multipass path.
    pub fn bytes_lost(&self) -> u64 {
        self.bytes_unreadable + self.bytes_undecryptable
    }
}

/// One file scheduled for extraction, resolved against the raw reader in the
/// structure phase (before the reader is moved into the decrypting decorator).
struct PlannedFile {
    /// Host-relative path (sanitized, collision-checked).
    host_rel: PathBuf,
    /// Disc path components (for VTS grouping / diagnostics).
    disc_name: String,
    /// Declared file size in bytes (trim target).
    size: u64,
    /// Inline (ICB-embedded) data, if any. When `Some`, `extents` is empty.
    inline: Option<Vec<u8>>,
    /// Absolute disc extents `(abs_lba, byte_len)`.
    extents: Vec<(u32, u32)>,
}

impl Disc {
    /// Extract this disc's **decrypted file tree** to `dest`. 1-shot,
    /// decrypt-only, no recovery loop.
    ///
    /// `reader` is consumed for content reads (it is moved into a
    /// [`DecryptingSectorSource`]); the structure + extent metadata are read
    /// first with the raw reader. `dest` receives the tree STRAIGHT IN (no
    /// auto-named subfolder). The caller must have run the pre-flight decrypt
    /// gate ([`ensure_decryptable`](Disc::ensure_decryptable)); this method
    /// resolves per-VTS CSS keys itself for DVD.
    ///
    /// Bad sectors become recorded zero-filled holes in the affected file
    /// (the run does not abort); undecryptable units are counted as loss.
    /// Files are written `<name>.partial` and renamed on success, so an
    /// interrupted run never leaves a half-written file that looks complete.
    pub fn extract_tree(
        &self,
        reader: &mut dyn SectorSource,
        dest: &Path,
        opts: &ExtractOptions,
    ) -> Result<ExtractResult> {
        // ── Output dir policy (pre-flight, before any read) ──────────────
        std::fs::create_dir_all(dest).map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
        if !opts.force && dir_is_non_empty(dest) {
            return Err(Error::DirNotEmpty);
        }

        // ── Phase 1: read the FS structure + all file extents (raw) ──────
        let fs = udf::read_filesystem(reader)?;
        let mut planned: Vec<PlannedFile> = Vec::new();
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut seen_hosts: std::collections::HashMap<PathBuf, String> =
            std::collections::HashMap::new();
        plan_tree(
            reader,
            &fs,
            &fs.root,
            Path::new(""),
            "",
            true,
            &mut planned,
            &mut dirs,
            &mut seen_hosts,
        )?;

        // Free-space pre-check: refuse up front if the tree won't fit, before
        // writing a single file (best-effort; only enforced where the platform
        // exposes free space).
        let required: u64 = planned
            .iter()
            .map(|p| p.size)
            .fold(0u64, |a, b| a.saturating_add(b));
        if let Some(available) = available_space(dest) {
            if available < required {
                return Err(Error::DirInsufficientSpace {
                    required,
                    available,
                });
            }
        }

        // Create directories up-front so a leaf write never races a missing
        // parent. The root itself already exists (create_dir_all above).
        for d in &dirs {
            let abs = dest.join(d);
            std::fs::create_dir_all(&abs).map_err(|e| Error::DirWriteFailed {
                errno: e.raw_os_error(),
            })?;
        }

        // Per-VTS CSS key map (DVD only): "VTS_xx" -> DecryptKeys. Built lazily
        // when a scrambled VOB group needs it. AACS / None discs keep the
        // disc-wide keys for every file.
        let base_keys = self.decrypt_keys();

        // ── Phase 2: stream each file through the decrypting decorator ────
        // The decorator owns its inner source for its lifetime. We hand it a
        // borrowing wrapper (so the caller keeps `reader`), swap keys per CSS
        // VTS group via `set_keys`; AACS/None keep `base_keys` throughout.
        let mut dec = DecryptingSectorSource::new(Borrowed(reader), base_keys.clone());
        let decrypt_loss = dec.decrypt_loss();

        let mut result = ExtractResult::default();
        let total_bytes = required;
        let mut done_bytes: u64 = 0;

        // CSS per-VTS key cache; only consulted for CSS discs.
        let is_css = matches!(base_keys, DecryptKeys::Css { .. });
        let mut vts_keys: std::collections::HashMap<String, DecryptKeys> =
            std::collections::HashMap::new();

        for pf in &planned {
            if opts.cancelled(true) {
                result.halted = true;
                break;
            }
            // Resolve the key for this file. CSS title VOBs need a per-VTS key;
            // clear nav (.IFO/.BUP/menu VOB) descrambles as a no-op with any
            // key, so the disc-wide key is fine for them too.
            if is_css {
                if let Some(vts) = vts_group_of(&pf.disc_name) {
                    let key = match vts_keys.get(&vts) {
                        Some(k) => k.clone(),
                        None => {
                            let k = self.resolve_vts_key(&vts, &planned, &mut dec, &base_keys);
                            vts_keys.insert(vts.clone(), k.clone());
                            k
                        }
                    };
                    dec.set_keys(key);
                } else {
                    dec.set_keys(base_keys.clone());
                }
            }

            // Acquire (rather than Relaxed) on these per-file delta loads:
            // `extract_tree` drives `dec` single-threaded so there is no race
            // today, but Acquire costs nothing on x86 and gives a happens-
            // before edge if file extraction is ever parallelised, so the
            // delta can never read a torn/stale counter across iterations.
            let before_loss = decrypt_loss.load(Ordering::Acquire);
            let (mut fr, halted) =
                extract_one_file(&mut dec, dest, pf, total_bytes, &mut done_bytes, opts)?;
            let after_loss = decrypt_loss.load(Ordering::Acquire);
            fr.bytes_undecryptable = after_loss.saturating_sub(before_loss);
            fr.bytes_good = fr.bytes_good.saturating_sub(fr.bytes_undecryptable);

            result.bytes_good = result.bytes_good.saturating_add(fr.bytes_good);
            result.bytes_unreadable = result.bytes_unreadable.saturating_add(fr.bytes_unreadable);
            result.bytes_undecryptable = result
                .bytes_undecryptable
                .saturating_add(fr.bytes_undecryptable);
            result.files.push(fr);
            if halted {
                result.halted = true;
                break;
            }
        }

        result.complete = !result.halted
            && result.bytes_unreadable == 0
            && result.bytes_undecryptable == 0
            && result.files.iter().all(|f| f.complete);
        Ok(result)
    }

    /// Resolve the CSS title key for a VTS group by cracking from its title
    /// VOB extents (keyless Stevenson attack). The disc-wide key is reused when
    /// it already covers this VTS (single-VTS discs, or this VTS's span). The
    /// reader is borrowed from the decrypting decorator (its inner source).
    fn resolve_vts_key<S: SectorSource>(
        &self,
        vts: &str,
        planned: &[PlannedFile],
        dec: &mut DecryptingSectorSource<S>,
        base_keys: &DecryptKeys,
    ) -> DecryptKeys {
        // Gather the title VOB extents for this VTS (VTS_xx_1.VOB .. _9.VOB;
        // VTS_xx_0.VOB is the menu and is clear, so excluded from the crack).
        let mut extents: Vec<crate::disc::Extent> = Vec::new();
        for pf in planned {
            if vts_group_of(&pf.disc_name).as_deref() == Some(vts) && is_title_vob(&pf.disc_name) {
                for &(abs_lba, byte_len) in &pf.extents {
                    extents.push(crate::disc::Extent {
                        start_lba: abs_lba,
                        sector_count: (byte_len as u64).div_ceil(SECTOR_LEN as u64) as u32,
                    });
                }
            }
        }
        if extents.is_empty() {
            return base_keys.clone();
        }
        // Largest extent first (movie body), matching the scan heuristic.
        extents.sort_by_key(|e| std::cmp::Reverse(e.sector_count));
        // Crack against the raw (still-scrambled) inner reader, NOT the
        // decrypting view — `crack_key` runs the descrambler itself.
        match crate::css::crack_key(dec.inner_mut(), &extents, 64) {
            Some(state) => DecryptKeys::Css {
                title_key: state.title_key,
            },
            None => base_keys.clone(),
        }
    }
}

/// A borrowing `SectorSource` wrapper. Lets the decrypting decorator "own" an
/// inner source for its lifetime while the caller keeps the underlying
/// `&mut dyn SectorSource` (the decorator is a `DecryptingSectorSource<S>`
/// generic over `S`, so it does NOT require a `'static` boxed inner — unlike
/// the mux highway, which takes the reader by value). The decorator is dropped
/// before `extract_tree` returns, so the borrow never escapes.
struct Borrowed<'a>(&'a mut dyn SectorSource);

impl SectorSource for Borrowed<'_> {
    fn capacity_sectors(&self) -> u32 {
        self.0.capacity_sectors()
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.0.read_sectors(lba, count, buf, recovery)
    }
    fn set_speed(&mut self, kbs: u16) {
        self.0.set_speed(kbs)
    }
    fn set_unit_base(&mut self, lba: u32) {
        self.0.set_unit_base(lba)
    }
}

/// Recursively plan the host tree: collect directories to create and files to
/// extract, sanitizing each component and detecting host-path collisions.
/// Skips the top-level `AACS/` and `CERTIFICATE/` directories (§7).
#[allow(clippy::too_many_arguments)]
fn plan_tree(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    dir: &DirEntry,
    host_rel: &Path,
    disc_path: &str,
    is_root: bool,
    files: &mut Vec<PlannedFile>,
    dirs: &mut Vec<PathBuf>,
    seen_hosts: &mut std::collections::HashMap<PathBuf, String>,
) -> Result<()> {
    for entry in &dir.entries {
        if entry.name.is_empty() {
            // The "parent" FID (".") has an empty name — skip.
            continue;
        }
        // Strip AACS / CERTIFICATE at the top level only (a deeper dir of the
        // same name, if it ever existed, is content).
        if is_root
            && (entry.name.eq_ignore_ascii_case("AACS")
                || entry.name.eq_ignore_ascii_case("CERTIFICATE"))
        {
            continue;
        }
        let safe = sanitize_component(&entry.name)?;
        let child_rel = host_rel.join(&safe);
        let child_disc = format!("{disc_path}/{}", entry.name);
        // Collision: two distinct disc paths → same host path.
        if let Some(prev) = seen_hosts.insert(child_rel.clone(), child_disc.clone()) {
            if prev != child_disc {
                return Err(Error::DirNameCollision {
                    host: child_rel.to_string_lossy().into_owned(),
                });
            }
        }
        if entry.is_dir {
            dirs.push(child_rel.clone());
            plan_tree(
                reader,
                fs,
                entry,
                &child_rel,
                &child_disc,
                false,
                files,
                dirs,
                seen_hosts,
            )?;
        } else {
            let inline = fs.inline_data_at(reader, entry.meta_lba)?;
            let extents = if inline.is_some() {
                Vec::new()
            } else {
                fs.extents_abs_at(reader, entry.meta_lba)?
            };
            files.push(PlannedFile {
                host_rel: child_rel,
                disc_name: entry.name.clone(),
                size: entry.size,
                inline,
                extents,
            });
        }
    }
    Ok(())
}

/// Extract a single planned file: open `<host>.partial`, stream its extents
/// through the decrypting decorator (bad sectors → recorded zero-filled
/// holes), truncate to the declared size, then rename to the final name.
///
/// Returns `(FileResult, halted)`. When `halted` is true the file was left as
/// `.partial` (incomplete) because the caller cancelled mid-file.
fn extract_one_file<S: SectorSource>(
    dec: &mut DecryptingSectorSource<S>,
    dest: &Path,
    pf: &PlannedFile,
    total_bytes: u64,
    done_bytes: &mut u64,
    opts: &ExtractOptions,
) -> Result<(FileResult, bool)> {
    let final_path = dest.join(&pf.host_rel);
    let partial_path = with_partial_suffix(&final_path);

    let file =
        crate::io::WritebackFile::create_with_size_hint(&partial_path, pf.size).map_err(|e| {
            Error::DirWriteFailed {
                errno: e.raw_os_error(),
            }
        })?;
    let mut writer = file;

    let mut fr = FileResult {
        path: pf.host_rel.clone(),
        bytes_good: 0,
        bytes_unreadable: 0,
        bytes_undecryptable: 0,
        complete: false,
    };

    // Inline (ICB-embedded) file: data already in hand, no decrypt path (nav
    // files are clear). Write verbatim, trimmed to size.
    if let Some(bytes) = &pf.inline {
        let n = (pf.size as usize).min(bytes.len());
        write_all(&mut writer, &bytes[..n], &partial_path)?;
        fr.bytes_good = n as u64;
        finalize_file(writer, &partial_path, pf.size, &final_path)?;
        fr.complete = true;
        *done_bytes = done_bytes.saturating_add(pf.size);
        report(opts, *done_bytes, total_bytes);
        return Ok((fr, false));
    }

    let mut written: u64 = 0;
    let mut buf = vec![0u8; READ_BATCH_SECTORS as usize * SECTOR_LEN];
    'extents: for &(abs_lba, byte_len) in &pf.extents {
        if written >= pf.size {
            break;
        }
        // Anchor AACS unit alignment at THIS extent's start (clip-anchored
        // gate, not absolute LBA 0 and NOT the file's first extent). A file
        // may span multiple extents (fragmented / Long-AD / continuation ICB
        // allocation); each extent's start LBA is arbitrary and generally not
        // a multiple of 3 sectors from the previous extent. The decrypt-on-
        // read gate (`is_unit_aligned(lba, unit_base)`) measures every read's
        // LBA relative to `unit_base`, so the base MUST be re-anchored per
        // extent — otherwise the first read of every later extent fails the
        // gate and the whole extent is zero-filled as a (false) hole. Matches
        // the per-extent re-anchoring in the mux read paths
        // (`mux/disc.rs`, `sector/prefetched.rs`). No-op for CSS / None.
        dec.set_unit_base(abs_lba);
        let sectors = (byte_len as u64).div_ceil(SECTOR_LEN as u64) as u32;
        let mut sector_off: u32 = 0;
        while sector_off < sectors {
            let mut batch = (sectors - sector_off).min(READ_BATCH_SECTORS);
            // AACS: read whole units. Round the batch DOWN to a multiple of 3
            // unless this is the final (possibly short) tail of the extent.
            // Every preceding batch is a whole number of units, so the tail
            // batch always BEGINS on a unit boundary (the gate measures
            // `lba - unit_base`, which stays unit-aligned). The tail itself may
            // be 1–2 sectors past a unit boundary; `decrypt_sectors` handles
            // that trailing partial unit explicitly (see its "Trailing-partial
            // contract"): a clear partial is left in the clear (the conformant
            // case — AACS leaves the final short unit unencrypted on disc), a
            // scrambled partial fails loud as DecryptFailed. So the short tail
            // is correct without padding the read up to a whole unit.
            if batch >= AACS_UNIT_SECTORS && (sector_off + batch) < sectors {
                batch -= batch % AACS_UNIT_SECTORS;
            }
            let lba = abs_lba + sector_off;
            let want = batch as usize * SECTOR_LEN;
            let read_ok = read_batch(dec, lba, batch, &mut buf[..want]);
            let chunk_bytes = want as u64;
            // Clip the chunk to the remaining file size on the final extent.
            let remaining = pf.size.saturating_sub(written);
            let usable = chunk_bytes.min(remaining) as usize;
            if read_ok {
                write_all(&mut writer, &buf[..usable], &partial_path)?;
                fr.bytes_good = fr.bytes_good.saturating_add(usable as u64);
            } else {
                // Bad sector(s): zero-fill this byte range, record the hole,
                // keep going (no abort, no sweep-skip).
                for b in buf[..usable].iter_mut() {
                    *b = 0;
                }
                write_all(&mut writer, &buf[..usable], &partial_path)?;
                fr.bytes_unreadable = fr.bytes_unreadable.saturating_add(usable as u64);
            }
            written = written.saturating_add(usable as u64);
            *done_bytes = done_bytes.saturating_add(usable as u64);
            let cont = report(opts, *done_bytes, total_bytes);
            sector_off += batch;
            if opts.cancelled(cont) {
                // Leave the `.partial`; do NOT rename. The aggregate run
                // records the halt and this file stays incomplete.
                return Ok((fr, true));
            }
            if written >= pf.size {
                break 'extents;
            }
        }
    }

    // Pad with a zero hole if the extents under-covered the declared size
    // (sparse / allocated-not-recorded). The size hint already set the file
    // length target; explicit truncate guarantees it.
    finalize_file(writer, &partial_path, pf.size, &final_path)?;
    fr.complete = true;
    Ok((fr, false))
}

/// Read one batch through the decrypting decorator with bounded retries.
/// Returns `true` on success, `false` once retries are exhausted (the caller
/// then records a hole). A `DecryptFailed` (unit-alignment / no-key) is NOT
/// retried — it would never succeed; treat it as a content read failure so the
/// range becomes a recorded hole rather than aborting the whole run.
fn read_batch<S: SectorSource>(
    dec: &mut DecryptingSectorSource<S>,
    lba: u32,
    count: u32,
    buf: &mut [u8],
) -> bool {
    for attempt in 0..=READ_RETRIES {
        match dec.read_sectors(lba, count as u16, buf, true) {
            Ok(_) => return true,
            Err(Error::DecryptFailed) => return false,
            Err(_) if attempt < READ_RETRIES => continue,
            Err(_) => return false,
        }
    }
    false
}

fn write_all(writer: &mut crate::io::WritebackFile, data: &[u8], path: &Path) -> Result<()> {
    writer.write_all(data).map_err(|e| {
        let _ = std::fs::remove_file(path);
        Error::DirWriteFailed {
            errno: e.raw_os_error(),
        }
    })
}

/// Flush, sync, set the final length, and rename `.partial` → final.
fn finalize_file(
    mut writer: crate::io::WritebackFile,
    partial: &Path,
    size: u64,
    final_path: &Path,
) -> Result<()> {
    writer.sync_all().map_err(|e| Error::DirWriteFailed {
        errno: e.raw_os_error(),
    })?;
    drop(writer);
    // Set the exact declared length (covers both an over-read final sector and
    // an under-covered sparse tail).
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(partial)
        .map_err(|e| Error::DirWriteFailed {
            errno: e.raw_os_error(),
        })?;
    f.set_len(size).map_err(|e| Error::DirWriteFailed {
        errno: e.raw_os_error(),
    })?;
    // `set_len` is a separate kernel op on this second handle (opened solely to
    // truncate); the content fsync above was on the now-dropped writer handle.
    // Without an fsync here, a crash between `set_len` and the rename can leave
    // the file at its pre-truncation length — e.g. a sparse/over-read tail keeps
    // its oversized form. Sync the new metadata (length) before publishing.
    f.sync_all().map_err(|e| Error::DirWriteFailed {
        errno: e.raw_os_error(),
    })?;
    drop(f);
    std::fs::rename(partial, final_path).map_err(|e| Error::DirWriteFailed {
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

/// Emit a progress report. Returns `true` to continue, `false` if the sink
/// requested an early stop (or there is no sink — always continue).
fn report(opts: &ExtractOptions, done: u64, total: u64) -> bool {
    match opts.progress {
        Some(p) => {
            let pp = crate::progress::PassProgress {
                kind: crate::progress::PassKind::Mux,
                work_done: done,
                work_total: total,
                bytes_good_total: done,
                bytes_unreadable_total: 0,
                bytes_pending_total: 0,
                bytes_total_disc: total,
                disc_duration_secs: None,
                bytes_bad_in_main_title: 0,
                main_title_duration_secs: None,
                main_title_size_bytes: None,
            };
            p.report(&pp)
        }
        None => true,
    }
}

/// Append `.partial` to a path's filename.
fn with_partial_suffix(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".partial");
    path.with_file_name(name)
}

/// Available free bytes on the filesystem holding `dir`, or `None` when the
/// platform doesn't expose it (the free-space gate is then skipped).
#[cfg(unix)]
fn available_space(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let cpath = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut st) };
    if rc != 0 {
        return None;
    }
    // `statvfs` field integer widths differ by platform (e.g. macOS `u64`,
    // Linux glibc `u64`/`c_ulong`); cast both to `u64` for the product. The
    // cast is a no-op where the field is already `u64` — allow the lint so the
    // same source stays portable.
    #[allow(clippy::unnecessary_cast, clippy::useless_conversion)]
    let avail = (st.f_bavail as u64).saturating_mul(st.f_frsize as u64);
    Some(avail)
}

#[cfg(not(unix))]
fn available_space(_dir: &Path) -> Option<u64> {
    None
}

/// Whether a directory exists and contains any entry.
fn dir_is_non_empty(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|mut it| it.next().is_some())
        .unwrap_or(false)
}

/// Sanitize ONE disc-path component for the host filesystem. Rejects `..`,
/// host-illegal characters, and control bytes; strips a trailing dot/space
/// (Windows). A name whose base matches a Windows reserved device name (`NUL`,
/// `CON`, `COM1`..) is **substituted** (prefixed with `_`) rather than rejected
/// — a Linux-authored disc may legally carry such a file and a single reserved
/// name must not abort the whole tree walk. An empty result after stripping is
/// an error.
fn sanitize_component(name: &str) -> Result<String> {
    if name == ".." || name == "." {
        return Err(Error::DirNameCollision {
            host: name.to_string(),
        });
    }
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        match ch {
            '\0' | '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*' => {
                return Err(Error::DirNameCollision {
                    host: name.to_string(),
                });
            }
            c if (c as u32) < 0x20 => {
                return Err(Error::DirNameCollision {
                    host: name.to_string(),
                });
            }
            c => out.push(c),
        }
    }
    // Trailing dot / space are illegal on Windows.
    let trimmed = out.trim_end_matches([' ', '.']);
    if trimmed.is_empty() {
        return Err(Error::DirNameCollision {
            host: name.to_string(),
        });
    }
    // Windows reserved device names (case-insensitive, base name before any
    // extension). `NUL` silently discards all writes on Windows, so a disc file
    // literally named `NUL.cfg` (legal on UDF/Linux) must NOT pass through
    // verbatim — but it also must not abort the whole extraction. Substitute by
    // prefixing `_`, which can never itself collide with another reserved name
    // and is host-legal everywhere. The substituted name still flows through the
    // caller's host-path collision check.
    let base = trimmed.split('.').next().unwrap_or(trimmed);
    if is_windows_reserved(base) {
        return Ok(format!("_{trimmed}"));
    }
    Ok(trimmed.to_string())
}

/// Whether `base` (the name component before any extension) matches a Windows
/// reserved device name. These are reserved by the OS regardless of extension
/// and silently alias a device (e.g. `NUL` discards writes). Case-insensitive.
fn is_windows_reserved(base: &str) -> bool {
    const RESERVED: &[&str] = &["CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "CLOCK$"];
    if RESERVED.iter().any(|r| base.eq_ignore_ascii_case(r)) {
        return true;
    }
    let up = base.to_ascii_uppercase();
    for prefix in ["COM", "LPT"] {
        if let Some(rest) = up.strip_prefix(prefix) {
            if rest.len() == 1 && matches!(rest.as_bytes()[0], b'1'..=b'9') {
                return true;
            }
        }
    }
    false
}

/// DVD VTS group key for a `VTS_xx_*` file name, else `None`. e.g.
/// `VTS_01_1.VOB` → `Some("VTS_01")`. Case-insensitive on the prefix.
fn vts_group_of(name: &str) -> Option<String> {
    let up = name.to_ascii_uppercase();
    let rest = up.strip_prefix("VTS_")?;
    // rest like "01_1.VOB" — take the 2-digit group number.
    let group = rest.split('_').next()?;
    if group.len() == 2 && group.bytes().all(|b| b.is_ascii_digit()) {
        Some(format!("VTS_{group}"))
    } else {
        None
    }
}

/// True for a CSS-scrambled title VOB (`VTS_xx_1.VOB`..`_9.VOB`). The menu
/// VOB `VTS_xx_0.VOB` and the `.IFO`/`.BUP` are clear, so they are excluded
/// from the per-VTS key crack.
fn is_title_vob(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    if !up.ends_with(".VOB") {
        return false;
    }
    // VTS_xx_y.VOB → y is the part number; 0 = menu (clear), 1..9 = title.
    let stem = up.trim_end_matches(".VOB");
    match stem.rsplit_once('_') {
        Some((_, part)) => part.len() == 1 && matches!(part.as_bytes()[0], b'1'..=b'9'),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::css::lfsr;
    use std::collections::HashMap;

    // ── Self-contained in-memory UDF fixture toolkit ──────────────────────
    // Modeled on the bluray.rs / dvd.rs test fixtures: a `MemDisc` SectorSource
    // backed by an explicit absolute-LBA map, plus recursive UDF builders.
    // PART_START == metadata_start so file/ICB LBAs share one address space
    // (single partition map). Encrypted content is faked at the buffer level
    // (CSS via `lfsr::scramble_sector`); AACS-content tests use clear bytes
    // through `DecryptKeys::None`, since per-unit AACS decrypt is already
    // exhaustively tested in `decrypt.rs` / `decrypting.rs`.

    const PART_START: u32 = 2000;

    struct MemDisc {
        sectors: HashMap<u32, [u8; 2048]>,
        /// Absolute LBAs that fail to read (bad-sector fixture).
        bad: std::collections::HashSet<u32>,
    }

    impl MemDisc {
        fn new() -> Self {
            Self {
                sectors: HashMap::new(),
                bad: std::collections::HashSet::new(),
            }
        }
        fn put(&mut self, lba: u32, data: [u8; 2048]) {
            self.sectors.insert(lba, data);
        }
        fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
            for (i, chunk) in bytes.chunks(2048).enumerate() {
                let mut s = [0u8; 2048];
                s[..chunk.len()].copy_from_slice(chunk);
                self.put(lba + i as u32, s);
            }
        }
    }

    impl SectorSource for MemDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let need = count as usize * 2048;
            for i in 0..count as u32 {
                if self.bad.contains(&(lba + i)) {
                    return Err(Error::DiscRead {
                        sector: (lba + i) as u64,
                        status: None,
                        sense: None,
                    });
                }
            }
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(need)
        }
    }

    struct FileSpec {
        name: String,
        icb_lba: u32,
        data_lba: u32,
        size: u32,
        long_ad: bool,
        contents: Vec<u8>,
    }

    struct DirSpec {
        name: String,
        icb_lba: u32,
        dir_data_lba: u32,
        files: Vec<FileSpec>,
        subdirs: Vec<DirSpec>,
    }

    fn file(name: &str, icb_lba: u32, data_lba: u32, contents: Vec<u8>, long_ad: bool) -> FileSpec {
        FileSpec {
            name: name.to_string(),
            icb_lba,
            data_lba,
            size: contents.len() as u32,
            long_ad,
            contents,
        }
    }

    fn build_file_icb(size: u32, data_lba: u32, long_ad: bool) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
        if long_ad {
            s[34..36].copy_from_slice(&1u16.to_le_bytes()); // Long AD
        }
        s[56..64].copy_from_slice(&(size as u64).to_le_bytes()); // info_length
        s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
        let ad_size: u32 = if long_ad { 16 } else { 8 };
        s[212..216].copy_from_slice(&ad_size.to_le_bytes()); // l_ad
        s[216..220].copy_from_slice(&(size & 0x3FFF_FFFF).to_le_bytes());
        s[220..224].copy_from_slice(&data_lba.to_le_bytes());
        s
    }

    fn build_dir_icb(dir_data_lba: u32, dir_data_len: u32) -> [u8; 2048] {
        build_file_icb(dir_data_len, dir_data_lba, false)
    }

    fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
        let start = buf.len();
        let name_field: Vec<u8> = if is_parent {
            Vec::new()
        } else {
            let mut v = vec![0x08u8];
            v.extend_from_slice(name.as_bytes());
            v
        };
        let l_fi = name_field.len();
        let mut fid = vec![0u8; 38];
        fid[0..2].copy_from_slice(&257u16.to_le_bytes());
        let mut file_chars = 0u8;
        if is_dir {
            file_chars |= 0x02;
        }
        if is_parent {
            file_chars |= 0x08;
        }
        fid[18] = file_chars;
        fid[19] = l_fi as u8;
        fid[24..28].copy_from_slice(&icb_lba.to_le_bytes());
        fid[36..38].copy_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&fid);
        buf.extend_from_slice(&name_field);
        let used = buf.len() - start;
        buf.resize(start + ((used + 3) & !3), 0);
    }

    fn lay_dir(disc: &mut MemDisc, dir: &DirSpec) {
        let mut fids = Vec::new();
        push_fid(&mut fids, "", dir.icb_lba, true, true);
        for f in &dir.files {
            push_fid(&mut fids, &f.name, f.icb_lba, false, false);
            disc.put(
                PART_START + f.icb_lba,
                build_file_icb(f.size, f.data_lba, f.long_ad),
            );
            if !f.contents.is_empty() {
                disc.put_bytes(PART_START + f.data_lba, &f.contents);
            }
        }
        for sub in &dir.subdirs {
            push_fid(&mut fids, &sub.name, sub.icb_lba, true, false);
        }
        disc.put(
            PART_START + dir.icb_lba,
            build_dir_icb(dir.dir_data_lba, fids.len() as u32),
        );
        disc.put_bytes(PART_START + dir.dir_data_lba, &fids);
        for sub in &dir.subdirs {
            lay_dir(disc, sub);
        }
    }

    fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
        let mut avdp = [0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put(256, avdp);
        let mut pd = [0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put(32, pd);
        let mut lvd = [0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        disc.put(33, lvd);
        let mut td = [0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put(34, td);
        let mut fsd = [0u8; 2048];
        fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
        fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
        disc.put(PART_START, fsd);
    }

    /// Lay a full root DirSpec and return a navigable disc.
    fn build_disc(root: DirSpec) -> MemDisc {
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, root.icb_lba);
        lay_dir(&mut disc, &root);
        disc
    }

    /// A unique temp dir for one test's output, removed on drop.
    struct TmpDir(PathBuf);
    impl TmpDir {
        fn new(tag: &str) -> Self {
            let mut p = std::env::temp_dir();
            let uniq = format!(
                "freemkv_extract_{tag}_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            p.push(uniq);
            Self(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Build a file ICB with TWO Short ADs (a fragmented / multi-extent file).
    /// Each AD records `sectors_each` sectors at its own partition-relative
    /// `data_lba`. Used to exercise the per-extent AACS unit-base re-anchoring:
    /// the two extents' absolute starts differ by a non-multiple-of-3 so a
    /// single (first-extent) unit base mis-aligns the second extent.
    fn build_two_extent_icb(sectors_each: u32, data_lba_a: u32, data_lba_b: u32) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
        // ad_type 0 = Short AD (icb flags low 3 bits at offset 34).
        s[34..36].copy_from_slice(&0u16.to_le_bytes());
        let size = sectors_each * SECTOR_LEN as u32 * 2;
        s[56..64].copy_from_slice(&(size as u64).to_le_bytes()); // info_length
        s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
        s[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad = 2 Short ADs
        let ext_len = sectors_each * SECTOR_LEN as u32; // bytes, type-0 recorded
        // AD #0
        s[216..220].copy_from_slice(&(ext_len & 0x3FFF_FFFF).to_le_bytes());
        s[220..224].copy_from_slice(&data_lba_a.to_le_bytes());
        // AD #1
        s[224..228].copy_from_slice(&(ext_len & 0x3FFF_FFFF).to_le_bytes());
        s[228..232].copy_from_slice(&data_lba_b.to_le_bytes());
        s
    }

    /// Build a clear 6144-byte AACS unit (TS syncs at the 192-byte BD-TS
    /// stride) then encrypt it under `unit_key` so `aacs::decrypt_unit`
    /// recovers it cleanly (zero decrypt loss). Mirrors the encrypt helper in
    /// `sector/decrypting.rs` tests. `tag` distinguishes two units' payloads.
    fn encrypt_aacs_unit(unit_key: &[u8; 16], tag: u8) -> Vec<u8> {
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
        let mut unit = vec![0u8; crate::aacs::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < unit.len() {
            unit[off] = 0x47; // TS sync
            if off + 1 < unit.len() {
                unit[off + 1] = tag; // payload marker so the two extents differ
            }
            off += 192;
        }
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = crate::aacs::decrypt::aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = crate::aacs::decrypt::AACS_IV;
        let blocks = (crate::aacs::ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..blocks {
            let o = 16 + i * 16;
            for j in 0..16 {
                unit[o + j] ^= prev[j];
            }
            let mut blk = GenericArray::clone_from_slice(&unit[o..o + 16]);
            cipher.encrypt_block(&mut blk);
            unit[o..o + 16].copy_from_slice(&blk);
            prev.copy_from_slice(&unit[o..o + 16]);
        }
        unit
    }

    /// The plaintext that `encrypt_aacs_unit(_, tag)` decrypts back to.
    fn clear_aacs_unit(tag: u8) -> Vec<u8> {
        let mut unit = vec![0u8; crate::aacs::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < unit.len() {
            unit[off] = 0x47;
            if off + 1 < unit.len() {
                unit[off + 1] = tag;
            }
            off += 192;
        }
        unit
    }

    /// A `Disc` carrying an AACS unit key so `decrypt_keys()` returns
    /// `DecryptKeys::Aacs` (engaging the unit-alignment gate). The content laid
    /// down by the fixture is genuinely encrypted under the key, so a clean
    /// per-extent decrypt records zero loss — letting the test isolate the GATE
    /// (alignment) from a false decrypt-loss tally.
    fn aacs_disc() -> Disc {
        let mut d = clear_disc();
        d.encrypted = true;
        d.aacs = Some(crate::disc::AacsState {
            version: 1,
            bus_encryption: false,
            mkb_version: None,
            disc_hash: String::new(),
            key_source: crate::disc::KeyOrigin::ExternalUk,
            vuk: None,
            unit_keys: vec![(0u32, [0u8; 16])],
            read_data_key: None,
            volume_id: [0u8; 16],
            uk_ro: Vec::new(),
            mkb: Vec::new(),
        });
        d
    }

    /// A `Disc` with no cipher state (clear content → `DecryptKeys::None`).
    fn clear_disc() -> Disc {
        Disc {
            volume_id: "TEST".into(),
            meta_title: Some("TEST".into()),
            format: crate::disc::DiscFormat::BluRay,
            capacity_sectors: 100_000,
            capacity_bytes: 100_000 * 2048,
            layers: 1,
            titles: Vec::new(),
            region: crate::disc::DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: false,
            aacs_error: None,
            css_error: None,
            content_format: crate::disc::ContentFormat::BdTs,
        }
    }

    fn read_out(dir: &Path, rel: &str) -> Option<Vec<u8>> {
        std::fs::read(dir.join(rel)).ok()
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    /// BDMV extraction: STREAM/*.m2ts written decrypted (here clear via
    /// None keys), nav (index.bdmv / MovieObject.bdmv / PLAYLIST / CLIPINF)
    /// verbatim, and the top-level AACS/ directory stripped entirely.
    #[test]
    fn bdmv_extracts_streams_and_nav_and_strips_aacs() {
        let m2ts = vec![0xABu8; 3 * 2048]; // one AACS unit's worth
        let index = b"INDEX-NAV".to_vec();
        let movieobj = b"MOVIEOBJECT-NAV".to_vec();
        let mpls = b"MPLS-PLAYLIST".to_vec();
        let clpi = b"CLPI-CLIPINF".to_vec();
        let aacs_inf = b"AACS-KEY-FILE".to_vec();

        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "BDMV".to_string(),
                    icb_lba: 20,
                    dir_data_lba: 21,
                    files: vec![
                        file("index.bdmv", 30, 31, index.clone(), false),
                        file("MovieObject.bdmv", 32, 33, movieobj.clone(), false),
                    ],
                    subdirs: vec![
                        DirSpec {
                            name: "STREAM".to_string(),
                            icb_lba: 40,
                            dir_data_lba: 41,
                            files: vec![file("00001.m2ts", 42, 5000, m2ts.clone(), true)],
                            subdirs: vec![],
                        },
                        DirSpec {
                            name: "PLAYLIST".to_string(),
                            icb_lba: 44,
                            dir_data_lba: 45,
                            files: vec![file("00000.mpls", 46, 47, mpls.clone(), false)],
                            subdirs: vec![],
                        },
                        DirSpec {
                            name: "CLIPINF".to_string(),
                            icb_lba: 48,
                            dir_data_lba: 49,
                            files: vec![file("00001.clpi", 50, 51, clpi.clone(), false)],
                            subdirs: vec![],
                        },
                    ],
                },
                DirSpec {
                    name: "AACS".to_string(),
                    icb_lba: 60,
                    dir_data_lba: 61,
                    files: vec![file("Unit_Key_RO.inf", 62, 63, aacs_inf, false)],
                    subdirs: vec![],
                },
            ],
        };
        let mut disc = build_disc(root);
        let out = TmpDir::new("bdmv");
        let res = clear_disc()
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect("extract");

        assert_eq!(
            read_out(out.path(), "BDMV/STREAM/00001.m2ts"),
            Some(m2ts),
            "m2ts content extracted intact"
        );
        assert_eq!(read_out(out.path(), "BDMV/index.bdmv"), Some(index));
        assert_eq!(
            read_out(out.path(), "BDMV/MovieObject.bdmv"),
            Some(movieobj)
        );
        assert_eq!(read_out(out.path(), "BDMV/PLAYLIST/00000.mpls"), Some(mpls));
        assert_eq!(read_out(out.path(), "BDMV/CLIPINF/00001.clpi"), Some(clpi));
        // AACS/ stripped: neither the dir nor its file exists.
        assert!(!out.path().join("AACS").exists(), "AACS/ must be stripped");
        assert!(res.complete, "clean extraction is complete");
        assert_eq!(res.bytes_lost(), 0);
    }

    /// VIDEO_TS extraction writes VOBs + IFO/BUP. Here the content is clear
    /// (None keys), proving the tree walk + per-file write for the DVD layout;
    /// CSS descramble correctness is tested separately below.
    #[test]
    fn video_ts_extracts_vobs_and_ifo() {
        let ifo = b"VIDEO_TS.IFO".to_vec();
        let vob = vec![0x5Au8; 2 * 2048];
        let bup = b"VIDEO_TS.BUP".to_vec();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "VIDEO_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![
                    file("VIDEO_TS.IFO", 30, 31, ifo.clone(), false),
                    file("VTS_01_1.VOB", 32, 5000, vob.clone(), false),
                    file("VIDEO_TS.BUP", 34, 35, bup.clone(), false),
                ],
                subdirs: vec![],
            }],
        };
        let mut disc = build_disc(root);
        let out = TmpDir::new("videots");
        let mut d = clear_disc();
        d.content_format = crate::disc::ContentFormat::MpegPs;
        let res = d
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect("extract");
        assert_eq!(read_out(out.path(), "VIDEO_TS/VIDEO_TS.IFO"), Some(ifo));
        assert_eq!(read_out(out.path(), "VIDEO_TS/VTS_01_1.VOB"), Some(vob));
        assert_eq!(read_out(out.path(), "VIDEO_TS/VIDEO_TS.BUP"), Some(bup));
        assert!(res.complete);
    }

    /// A CSS-scrambled title VOB is descrambled on extraction: the producer
    /// cracks the per-VTS key (keyless Stevenson) and the output VOB is plain.
    #[test]
    fn css_title_vob_is_descrambled() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        // Build a scrambled sector with a crackable repeating crib (period 8),
        // mirroring css::mod tests' `crackable_sector`.
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let mut plain = vec![0u8; 2048];
        plain[0x14] = 0x10; // scramble flag
        let pat: Vec<u8> = (0..8)
            .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
            .collect();
        for (i, b) in plain.iter_mut().enumerate().skip(0x59) {
            *b = pat[i % 8];
        }
        plain[0x54..0x59].copy_from_slice(&seed);
        let mut scrambled = plain.clone();
        lfsr::scramble_sector(&title_key, &mut scrambled);

        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "VIDEO_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![file("VTS_01_1.VOB", 30, 5000, scrambled, false)],
                subdirs: vec![],
            }],
        };
        let mut disc = build_disc(root);
        let out = TmpDir::new("css");
        // A CSS disc with a (provenance-unknown) cracked key; the producer
        // re-cracks per VTS, so the disc-wide key value is irrelevant here.
        let mut d = clear_disc();
        d.content_format = crate::disc::ContentFormat::MpegPs;
        d.css = Some(crate::css::CssState {
            title_key,
            crack_span: None,
        });
        let res = d
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect("extract");
        let got = read_out(out.path(), "VIDEO_TS/VTS_01_1.VOB").expect("vob");
        // Descrambled output matches the plaintext, with the scramble flag
        // cleared by the descrambler.
        let mut expect = plain.clone();
        expect[0x14] = 0x00;
        assert_eq!(got, expect, "VOB descrambled to plaintext");
        assert!(res.complete);
    }

    /// A bad sector inside a file becomes a recorded zero-filled hole; the run
    /// does not abort, the file is still written, and loss is accounted.
    #[test]
    fn bad_sector_holes_file_and_accounts_loss() {
        let good = vec![0x77u8; 4 * 2048];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: vec![file("00001.m2ts", 24, 5000, good.clone(), true)],
                    subdirs: vec![],
                }],
            }],
        };
        let mut disc = build_disc(root);
        // Mark the whole 4-sector extent bad so a batch read fails. Abs LBAs
        // are PART_START + data_lba (5000) .. +3.
        for i in 0..4u32 {
            disc.bad.insert(PART_START + 5000 + i);
        }
        let out = TmpDir::new("badsector");
        let res = clear_disc()
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect("extract does not abort on bad sectors");
        let got = read_out(out.path(), "BDMV/STREAM/00001.m2ts").expect("file written");
        assert_eq!(
            got.len(),
            good.len(),
            "holed file still sized to declared size"
        );
        assert!(got.iter().all(|&b| b == 0), "bad range zero-filled");
        assert!(!res.complete, "lossy extraction is not complete");
        assert_eq!(res.bytes_unreadable, good.len() as u64);
        assert_eq!(res.files.len(), 1);
        assert_eq!(res.files[0].bytes_unreadable, good.len() as u64);
    }

    /// Path sanitization rejects a host-illegal component in a disc file name.
    #[test]
    fn sanitize_rejects_illegal_component() {
        assert!(sanitize_component("good_name.m2ts").is_ok());
        assert!(sanitize_component("..").is_err());
        assert!(sanitize_component("a/b").is_err());
        assert!(sanitize_component("a:b").is_err());
        assert!(sanitize_component("a*b").is_err());
        // Windows reserved device names are substituted (prefixed `_`), not
        // rejected — a single such file must not abort the whole tree walk.
        assert_eq!(sanitize_component("CON").unwrap(), "_CON");
        assert_eq!(sanitize_component("com1").unwrap(), "_com1");
        assert_eq!(sanitize_component("LPT9").unwrap(), "_LPT9");
        // Reserved base with an extension is still substituted (the device name
        // aliases regardless of extension on Windows).
        assert_eq!(sanitize_component("NUL.cfg").unwrap(), "_NUL.cfg");
        assert_eq!(sanitize_component("conin$").unwrap(), "_conin$");
        // A non-reserved lookalike is untouched.
        assert_eq!(sanitize_component("COM10").unwrap(), "COM10");
        assert_eq!(sanitize_component("CONSOLE").unwrap(), "CONSOLE");
        // A trailing dot/space is stripped, not rejected outright.
        assert_eq!(sanitize_component("name. ").unwrap(), "name");
        // ...unless stripping empties it.
        assert!(sanitize_component(". ").is_err());
    }

    /// Two distinct disc paths that sanitize to the same host path are a hard
    /// error (collision), never a silent overwrite.
    #[test]
    fn name_collision_is_error() {
        // Two files in the same dir whose names both reduce to "movie" after
        // the trailing-dot/space strip ("movie" and "movie.").
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: vec![
                file("movie", 30, 31, b"a".to_vec(), false),
                file("movie.", 32, 33, b"b".to_vec(), false),
            ],
            subdirs: vec![],
        };
        let mut disc = build_disc(root);
        let out = TmpDir::new("collision");
        let err = clear_disc()
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect_err("collision must error");
        assert!(matches!(err, Error::DirNameCollision { .. }));
    }

    /// A non-empty target dir is refused without `--force`, and accepted with.
    #[test]
    fn non_empty_target_requires_force() {
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: vec![file("a.bin", 30, 31, b"hello".to_vec(), false)],
            subdirs: vec![],
        };
        let out = TmpDir::new("nonempty");
        std::fs::create_dir_all(out.path()).unwrap();
        std::fs::write(out.path().join("preexisting.txt"), b"x").unwrap();

        let mut disc = build_disc(root);
        let err = clear_disc()
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect_err("non-empty dir without --force must error");
        assert!(matches!(err, Error::DirNotEmpty));

        // With --force it proceeds.
        let mut disc2 = build_disc(DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: vec![file("a.bin", 30, 31, b"hello".to_vec(), false)],
            subdirs: vec![],
        });
        let opts = ExtractOptions {
            force: true,
            ..Default::default()
        };
        let res = clear_disc()
            .extract_tree(&mut disc2, out.path(), &opts)
            .expect("force proceeds");
        assert_eq!(read_out(out.path(), "a.bin"), Some(b"hello".to_vec()));
        assert!(res.complete);
    }

    /// VTS grouping + title-VOB classification used for per-VTS CSS keys.
    #[test]
    fn vts_grouping_and_title_vob_classification() {
        assert_eq!(vts_group_of("VTS_01_1.VOB").as_deref(), Some("VTS_01"));
        assert_eq!(vts_group_of("VTS_12_0.VOB").as_deref(), Some("VTS_12"));
        assert_eq!(vts_group_of("VIDEO_TS.IFO"), None);
        assert!(is_title_vob("VTS_01_1.VOB"));
        assert!(is_title_vob("VTS_01_9.VOB"));
        assert!(
            !is_title_vob("VTS_01_0.VOB"),
            "menu VOB is clear, not title"
        );
        assert!(!is_title_vob("VTS_01_1.IFO"));
    }

    /// Regression (rc.6 audit, finding #449): a MULTI-EXTENT AACS file must
    /// re-anchor the unit-alignment base PER extent, not once at the first
    /// extent. The two extents here start at absolute LBAs whose difference is
    /// NOT a multiple of 3 (PART_START+5000 vs PART_START+5004 → Δ4 sectors).
    /// With a single first-extent unit base the second extent's first read is
    /// off the unit grid (offset 4, 4 % 3 == 1) → `is_unit_aligned` fails →
    /// `DecryptFailed` → the whole extent becomes a zero-filled (false) hole.
    /// With per-extent anchoring both extents read clean: bytes_unreadable == 0
    /// and the file content survives. The fixture content is clear (no TS
    /// syncs) so the decrypt step restores each unit verbatim — this isolates
    /// the GATE from the cipher math.
    #[test]
    fn multi_extent_aacs_anchors_unit_base_per_extent() {
        const SECTORS_EACH: u32 = 3; // one AACS unit per extent
        const DATA_A: u32 = 5000; // abs PART_START+5000 (≡ 7000)
        const DATA_B: u32 = 5004; // abs PART_START+5004 — Δ4 (not mult of 3)

        let key = [0u8; 16];
        // Each extent is exactly one encrypted AACS unit (distinct payloads).
        let ext_a = encrypt_aacs_unit(&key, 0xA1);
        let ext_b = encrypt_aacs_unit(&key, 0xB2);
        // Expected plaintext after a correct per-extent decrypt.
        let mut expect = clear_aacs_unit(0xA1);
        expect.extend_from_slice(&clear_aacs_unit(0xB2));

        // Lay the disc by hand: root dir with one BDMV/STREAM/00001.m2ts whose
        // ICB carries two Short ADs.
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);

        // root → BDMV → STREAM → 00001.m2ts
        let mut stream_fids = Vec::new();
        push_fid(&mut stream_fids, "", 40, true, true);
        push_fid(&mut stream_fids, "00001.m2ts", 42, false, false);
        disc.put(
            PART_START + 42,
            build_two_extent_icb(SECTORS_EACH, DATA_A, DATA_B),
        );
        disc.put_bytes(PART_START + DATA_A, &ext_a);
        disc.put_bytes(PART_START + DATA_B, &ext_b);
        disc.put(PART_START + 40, build_dir_icb(41, stream_fids.len() as u32));
        disc.put_bytes(PART_START + 41, &stream_fids);

        let mut bdmv_fids = Vec::new();
        push_fid(&mut bdmv_fids, "", 20, true, true);
        push_fid(&mut bdmv_fids, "STREAM", 40, true, false);
        disc.put(PART_START + 20, build_dir_icb(21, bdmv_fids.len() as u32));
        disc.put_bytes(PART_START + 21, &bdmv_fids);

        let mut root_fids = Vec::new();
        push_fid(&mut root_fids, "", 10, true, true);
        push_fid(&mut root_fids, "BDMV", 20, true, false);
        disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
        disc.put_bytes(PART_START + 11, &root_fids);

        let out = TmpDir::new("multiextent_aacs");
        let res = aacs_disc()
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect("extract");

        let got = read_out(out.path(), "BDMV/STREAM/00001.m2ts").expect("file written");
        assert_eq!(
            got, expect,
            "both extents extract verbatim — the second extent is NOT a hole"
        );
        assert_eq!(
            res.bytes_unreadable, 0,
            "per-extent unit base must keep the second extent off the hole path"
        );
        assert_eq!(
            res.bytes_undecryptable, 0,
            "clear units decrypt-restore clean"
        );
        assert!(
            res.complete,
            "a clean multi-extent AACS file extracts complete"
        );
    }

    /// Focused alignment-computation check underpinning the per-extent fix:
    /// when each extent anchors its OWN start as the unit base, the extent's
    /// own batch starts are always unit-aligned; anchoring a later extent
    /// against the FIRST extent's base mis-aligns whenever the extents' starts
    /// differ by a non-multiple of 3 sectors. This is the exact arithmetic the
    /// decrypt-on-read gate (`aacs::is_unit_aligned`) performs.
    #[test]
    fn per_extent_base_is_aligned_first_extent_base_is_not() {
        use crate::aacs::is_unit_aligned;
        let ext_a_start = 7000u32; // first extent abs LBA
        let ext_b_start = 7004u32; // second extent abs LBA (Δ4 — not mult of 3)

        // Per-extent base: extent B's first read anchors on B's start → aligned.
        assert!(
            is_unit_aligned(ext_b_start, ext_b_start),
            "per-extent base keeps the extent's own first read aligned"
        );
        // Stale (first-extent) base: extent B's first read measured against A's
        // start is OFF the unit grid → the gate would (wrongly) reject it.
        assert!(
            !is_unit_aligned(ext_b_start, ext_a_start),
            "first-extent base mis-aligns a Δ-non-multiple-of-3 later extent"
        );
        // Sanity: a later extent whose Δ from the first IS a multiple of 3 would
        // have masked the bug — that's why the regression fixture uses Δ4.
        let ext_c_start = ext_a_start + 6; // Δ6 == 2 units
        assert!(
            is_unit_aligned(ext_c_start, ext_a_start),
            "a Δ-multiple-of-3 extent happens to stay aligned even on a stale base"
        );
    }

    /// An inline (ICB-embedded) file extracts from its embedded bytes.
    #[test]
    fn inline_file_extracts() {
        // Build an ICB whose flags select AD type 3 (embedded) with the data
        // stored inline after the ADs field.
        let payload = b"INLINE-NAV-DATA".to_vec();
        let mut disc = MemDisc::new();
        let mut root_fids = Vec::new();
        push_fid(&mut root_fids, "", 10, true, true);
        push_fid(&mut root_fids, "tiny.inf", 30, false, false);
        // Inline ICB (tag 266): flags low 3 bits = 3, l_ad = payload len, the
        // data living at offset 216.
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        icb[34..36].copy_from_slice(&3u16.to_le_bytes()); // embedded
        icb[56..64].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        icb[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
        icb[212..216].copy_from_slice(&(payload.len() as u32).to_le_bytes()); // l_ad
        icb[216..216 + payload.len()].copy_from_slice(&payload);
        disc.put(PART_START + 30, icb);
        disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
        disc.put_bytes(PART_START + 11, &root_fids);
        build_udf_skeleton(&mut disc, 10);

        let out = TmpDir::new("inline");
        let res = clear_disc()
            .extract_tree(&mut disc, out.path(), &ExtractOptions::default())
            .expect("extract");
        assert_eq!(read_out(out.path(), "tiny.inf"), Some(payload));
        assert!(res.complete);
    }
}
