//! The artifact lock (stop design §2.5, T10): a sidecar `<final>.lock` next to a
//! resumable output, held for the whole op, so two freemkv processes on one host never
//! write one `.partial` (+ mapfile) at once.
//!
//! The sidecar is named after the FINAL artifact (`Movie.iso.lock`), the same before and
//! after the `.partial` → final rename, so a Resume that knows only `Movie.iso` and a live
//! op writing `Movie.iso.partial` contend on one file. It is not the mapfile (renamed on
//! every flush) and not the `.partial` (renamed or deleted). Exclusion is same-host only
//! (J-5.5-1): across NFS clients it depends on the mount's locking and is not claimed.
//!
//! Acquire = open read-write, exclusive lock, then check the open file is still the one
//! the path names (the holder deletes the sidecar only while holding it), else retry.
//! On Unix that check also writes a per-acquisition token into the locked file and reads
//! it back by path (inode numbers can follow names on smbfs); a read refused by a lock is
//! re-read with ours released. One process's holders also exclude each other through a
//! process-wide set: smbfs `flock` does not exclude two fds of one process.
//! The wait is halt-aware and stall-based: it fails `TimedOut { op: "artifact_lock" }`
//! (E9073) only after 30 s in which neither the `.partial` nor a watched file changed.

use crate::error::{Error, Result};
use crate::halt::{Halt, Liveness, Stall, StallTimer, WAIT_SLICE};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// T10: the lock wait fails after this long with no change in the holder's output.
pub const ARTIFACT_LOCK_WINDOW: Duration = Duration::from_secs(30);

/// Filesystem identity for alias detection: device/inode on Unix, volume/file
/// index on Windows. This identifies a file, not its contents or a stable disc ID.
pub fn file_identity(path: &Path) -> io::Result<(u64, u64)> {
    os::path_id(path)
}

/// The sidecar for the artifact whose final name is `final_path`: `<final>.lock`.
pub fn lock_path(final_path: &Path) -> PathBuf {
    with_suffix(final_path, ".lock")
}

fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut name = p.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// A held `<final>.lock`. Dropping it releases the lock and keeps the file, which then
/// guards the resumable artifact (after a Stop, a failure or a crash);
/// [`delete`](Self::delete) removes it on success or Discard.
#[derive(Debug)]
pub struct ArtifactLock {
    file: File,
    path: PathBuf,
    claim: Claim,
}

// This process's held sidecars, by absolute path with a canonical parent.
static HELD: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

#[derive(Debug)]
struct Claim(PathBuf);

impl Claim {
    fn take(key: &Path) -> Option<Claim> {
        let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
        if held.iter().any(|p| p == key) {
            return None;
        }
        held.push(key.to_path_buf());
        Some(Claim(key.to_path_buf()))
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        HELD.lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|p| p != &self.0);
    }
}

fn claim_key(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => parent
            .canonicalize()
            .map_or_else(|_| absolute.clone(), |parent| parent.join(name)),
        _ => absolute,
    }
}

impl ArtifactLock {
    /// Take the sidecar lock for `final_path`, waiting while another process holds it.
    /// The holder's progress is a change in the size or mtime of `<final>.partial` or of
    /// any `watch` path (e.g. the mapfile), read by path on each poll (T10).
    ///
    /// # Errors
    ///
    /// [`Error::Halted`] on a Stop (within one [`WAIT_SLICE`]);
    /// [`Error::TimedOut`] `{ op: "artifact_lock" }` after [`ARTIFACT_LOCK_WINDOW`] with
    /// no progress (on Windows also the surface of a permission failure);
    /// [`Error::IoError`] when the sidecar cannot be opened or locked.
    pub fn acquire(final_path: &Path, watch: &[&Path], halt: &Halt) -> Result<ArtifactLock> {
        Self::acquire_within(final_path, watch, halt, ARTIFACT_LOCK_WINDOW)
    }

    // `acquire` with the T10 window as a parameter, so tests run in milliseconds.
    pub(crate) fn acquire_within(
        final_path: &Path,
        watch: &[&Path],
        halt: &Halt,
        window: Duration,
    ) -> Result<ArtifactLock> {
        Self::acquire_ids(
            final_path,
            watch,
            halt,
            window,
            &|f, p| (os::file_id(f), os::path_id(p)),
            &os::try_lock_exclusive,
        )
    }

    // `acquire_within` with the identity check as a parameter (tests inject ESTALE and
    // a lasting mismatch): `ids` is (the open file's id, the path's id).
    fn acquire_ids(
        final_path: &Path,
        watch: &[&Path],
        halt: &Halt,
        window: Duration,
        ids: &Ids<'_>,
        lock: &Lock<'_>,
    ) -> Result<ArtifactLock> {
        let hooks = Hooks {
            ids,
            lock,
            names: &os::names_this_file,
        };
        Self::acquire_full(final_path, watch, halt, window, &hooks)
    }

    // `acquire_ids` with every injectable check, the token proof included.
    fn acquire_full(
        final_path: &Path,
        watch: &[&Path],
        halt: &Halt,
        window: Duration,
        hooks: &Hooks<'_>,
    ) -> Result<ArtifactLock> {
        let Hooks { ids, lock, names } = *hooks;
        let path = lock_path(final_path);
        let mut wait = LockWait::new(final_path, watch, window);
        let key = claim_key(&path);
        let claim = loop {
            halt.check()?;
            match Claim::take(&key) {
                Some(claim) => break claim,
                None => wait.slice(halt, &io::Error::from(io::ErrorKind::WouldBlock))?,
            }
        };
        loop {
            halt.check()?;
            let file = match os::open_rw(&path) {
                Ok(f) => f,
                // Windows: a delete-pending sidecar refuses the open; the holder is leaving.
                Err(e) if os::open_retryable(&e) => {
                    wait.slice(halt, &e)?;
                    continue;
                }
                Err(e) => return Err(Error::IoError { source: e }),
            };
            // SS-8 flock(2): "Only one process may hold an exclusive lock for a given file at
            // a given time." SS-10 LockFileEx: LOCKFILE_EXCLUSIVE_LOCK, FAIL_IMMEDIATELY.
            while !lock(&file).map_err(|source| Error::IoError { source })? {
                wait.slice(halt, &io::Error::from(io::ErrorKind::WouldBlock))?;
            }
            // SS-9 XBD <sys/stat.h>: "A file identity is uniquely determined by the combination
            // of st_dev and st_ino." SS-10: "the identifier … and the volume serial number".
            let proof = match ids(&file, &path) {
                (Ok(held), Ok(named)) if held == named => {
                    names(&file, &path, lock).map_err(|source| Error::IoError { source })?
                }
                // The holder deleted it while we waited: we hold an unlinked file.
                (Ok(_), Ok(_)) => Proof::Retry(replaced()),
                // §2.5: ENOENT/ESTALE from `fstat` or `stat` (another client deleted it).
                (Err(e), _) | (_, Err(e)) if os::id_retryable(&e) => Proof::Retry(e),
                (Err(e), _) | (_, Err(e)) => return Err(Error::IoError { source: e }),
            };
            let retry = match proof {
                Proof::Ours => return Ok(ArtifactLock { file, path, claim }),
                Proof::Retry(e) => e,
            };
            // A retry is a wait like any other: halt-aware, and under T10 (never a spin).
            drop(file);
            wait.slice(halt, &retry)?;
        }
    }

    /// The sidecar's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Delete the sidecar while still holding it (success or Discard, §2.5), then release.
    ///
    /// # Errors
    ///
    /// [`Error::IoError`] if the file exists but cannot be removed.
    pub fn delete(self) -> Result<()> {
        let ArtifactLock { file, path, claim } = self;
        let removed = match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(Error::IoError { source }),
        };
        // Released only now: nobody can lock the path between the check and the unlink.
        drop(file);
        drop(claim);
        removed
    }
}

// (the open file's identity, the path's identity).
type Ids<'a> = dyn Fn(&File, &Path) -> (io::Result<(u64, u64)>, io::Result<(u64, u64)>) + 'a;
// The OS exclusive try-lock (tests model filesystems where it does not exclude).
type Lock<'a> = dyn Fn(&File) -> io::Result<bool> + 'a;
// Whether the path names the locked file (the token proof; tests inject its IO). It may
// release and retake the lock through the given try-lock.
type Names<'a> = dyn Fn(&File, &Path, &Lock<'_>) -> io::Result<Proof> + 'a;

// The injectable checks of one acquire.
struct Hooks<'a> {
    ids: &'a Ids<'a>,
    lock: &'a Lock<'a>,
    names: &'a Names<'a>,
}

// The token proof's verdict; `Retry` carries the OS answer logged if the wait expires.
#[derive(Debug)]
enum Proof {
    Ours,
    Retry(io::Error),
}

fn replaced() -> io::Error {
    io::Error::other("the sidecar was replaced")
}

// The T10 wait between lock attempts: halt-aware, failing only after `window` with no
// change in any watched file.
struct LockWait {
    watched: Vec<PathBuf>,
    seen: Vec<Option<(u64, Option<SystemTime>)>>,
    progress: Liveness,
    timer: StallTimer,
}

impl LockWait {
    fn new(final_path: &Path, watch: &[&Path], window: Duration) -> Self {
        let mut watched = vec![with_suffix(final_path, ".partial")];
        watched.extend(watch.iter().map(|p| p.to_path_buf()));
        let seen = signatures(&watched);
        let progress = Liveness::new();
        let timer = StallTimer::new(window, &progress);
        LockWait {
            watched,
            seen,
            progress,
            timer,
        }
    }

    // One `WAIT_SLICE`: `Halted` on a Stop, `TimedOut` once the window passes with no
    // change. `last` is the OS answer that made us wait (logged on expiry).
    fn slice(&mut self, halt: &Halt, last: &io::Error) -> Result<()> {
        let now = signatures(&self.watched);
        if now != self.seen {
            self.seen = now;
            self.progress.bump();
        }
        if self.timer.poll(&self.progress) == Stall::Expired {
            tracing::warn!(
                target: "freemkv::io",
                phase = "artifact_lock",
                last_os_error = %last,
                "another process holds the artifact lock and its output has not changed"
            );
            return Err(Error::TimedOut {
                op: "artifact_lock",
            });
        }
        halt.wait(WAIT_SLICE)
    }
}

// Size and mtime of each path, read by path (a path `stat` follows a rename).
fn signatures(paths: &[PathBuf]) -> Vec<Option<(u64, Option<SystemTime>)>> {
    paths
        .iter()
        .map(|p| {
            std::fs::metadata(p)
                .ok()
                .map(|m| (m.len(), m.modified().ok()))
        })
        .collect()
}

#[cfg(unix)]
mod os {
    use super::{Lock, Proof, replaced};
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    // Read-write, always: an NFS client emulates flock as a byte-range lock, which needs
    // the file open for writing (SS-8).
    pub(super) fn open_rw(p: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(p)
    }

    pub(super) fn open_retryable(_e: &io::Error) -> bool {
        false
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(super) fn try_lock_exclusive(f: &File) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        loop {
            // SAFETY: the fd is open for the whole call (it is borrowed from `f`).
            let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc == 0 {
                return Ok(true);
            }
            let e = io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EWOULDBLOCK) => return Ok(false),
                Some(libc::EINTR) => continue,
                _ => return Err(e),
            }
        }
    }

    // No `libc` on this target: no OS lock, so no exclusion (not a shipped platform).
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) fn try_lock_exclusive(_f: &File) -> io::Result<bool> {
        Ok(true)
    }

    pub(super) fn file_id(f: &File) -> io::Result<(u64, u64)> {
        let m = f.metadata()?;
        Ok((m.dev(), m.ino()))
    }

    pub(super) fn path_id(p: &Path) -> io::Result<(u64, u64)> {
        let m = std::fs::metadata(p)?;
        Ok((m.dev(), m.ino()))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(super) fn unlock(f: &File) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        loop {
            // SAFETY: the fd is open for the whole call (it is borrowed from `f`).
            if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) } == 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EINTR) {
                return Err(e);
            }
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) fn unlock(_f: &File) -> io::Result<()> {
        Ok(())
    }

    // Only a file's lock holder writes it, so reading our fresh token back by path
    // proves the path names the file we locked, whatever its inode number says.
    pub(super) fn names_this_file(f: &File, p: &Path, relock: &Lock<'_>) -> io::Result<Proof> {
        names_with(f, p, &write_token, &read_back, relock)
    }

    fn write_token(f: &File, token: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        f.set_len(0)?;
        f.write_all_at(token, 0)
    }

    fn read_back(p: &Path, max: u64) -> io::Result<Vec<u8>> {
        use std::io::Read;
        let mut named = Vec::new();
        File::open(p)?.take(max).read_to_end(&mut named)?;
        Ok(named)
    }

    // Our own fd's contents, which only a lock holder writes.
    fn read_held(f: &File, max: usize) -> io::Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut held = vec![0; max];
        let mut n = 0;
        while n < max {
            match f.read_at(&mut held[n..], n as u64) {
                Ok(0) => break,
                Ok(k) => n += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        held.truncate(n);
        Ok(held)
    }

    // No room for the token (full volume, quota, size limit): the caller has already
    // matched dev/ino, as in 1.8.3. Name-derived-inode filesystems then rely on that.
    fn token_unavailable(e: &io::Error) -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if matches!(
            e.raw_os_error(),
            Some(libc::ENOSPC | libc::EDQUOT | libc::EFBIG)
        ) {
            return true;
        }
        let _ = e;
        false
    }

    // A stale handle to a replaced or unreachable file fails IO this way: unproven, retry.
    fn identity_uncertain(e: &io::Error) -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if matches!(
            e.raw_os_error(),
            Some(
                libc::EBADF
                    | libc::EACCES
                    | libc::EPERM
                    | libc::ENOTCONN
                    | libc::ETIMEDOUT
                    | libc::EIO
            )
        ) {
            return true;
        }
        id_retryable(e)
    }

    // SMB/CIFS mandatory byte-range locks refuse a read while any fd holds the lock.
    fn read_conflict(e: &io::Error) -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if matches!(
            e.raw_os_error(),
            Some(libc::EACCES | libc::EAGAIN | libc::EBUSY | libc::EPERM)
        ) {
            return true;
        }
        e.kind() == io::ErrorKind::PermissionDenied
    }

    pub(super) fn names_with(
        f: &File,
        p: &Path,
        write: &dyn Fn(&File, &[u8]) -> io::Result<()>,
        read: &dyn Fn(&Path, u64) -> io::Result<Vec<u8>>,
        relock: &Lock<'_>,
    ) -> io::Result<Proof> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let token = format!("{}:{nanos}:{sequence}\n", std::process::id());
        let token = token.as_bytes();
        let max = token.len() as u64 + 1;
        match write(f, token) {
            Ok(()) => {}
            Err(e) if token_unavailable(&e) => return Ok(Proof::Ours),
            Err(e) if identity_uncertain(&e) => return Ok(Proof::Retry(e)),
            Err(e) => return Err(e),
        }
        match read(p, max) {
            Ok(named) if named == token => Ok(Proof::Ours),
            Ok(_) => Ok(Proof::Retry(replaced())),
            Err(e) if read_conflict(&e) => prove_unlocked(f, p, token, read, relock),
            Err(e) if id_retryable(&e) => Ok(Proof::Retry(e)),
            Err(e) => Err(e),
        }
    }

    // The refusal may be another holder's lock on a recreated sidecar. With ours released,
    // our token by path proves the path names our file; still finding it in the file
    // after the relock proves no other acquirer took the file in between.
    fn prove_unlocked(
        f: &File,
        p: &Path,
        token: &[u8],
        read: &dyn Fn(&Path, u64) -> io::Result<Vec<u8>>,
        relock: &Lock<'_>,
    ) -> io::Result<Proof> {
        let max = token.len() as u64 + 1;
        if let Err(e) = unlock(f) {
            return Ok(Proof::Retry(e));
        }
        match read(p, max) {
            Ok(named) if named == token => {}
            Ok(_) => return Ok(Proof::Retry(replaced())),
            Err(e) if read_conflict(&e) || id_retryable(&e) => return Ok(Proof::Retry(e)),
            Err(e) => return Err(e),
        }
        match relock(f) {
            Ok(true) => {}
            Ok(false) => return Ok(Proof::Retry(io::ErrorKind::WouldBlock.into())),
            Err(e) => return Ok(Proof::Retry(e)),
        }
        match read_held(f, token.len() + 1) {
            Ok(held) if held == token => Ok(Proof::Ours),
            Ok(_) => Ok(Proof::Retry(replaced())),
            Err(e) => Ok(Proof::Retry(e)),
        }
    }

    // Gone (ENOENT), or deleted by another NFS client (ESTALE): retry.
    pub(super) fn id_retryable(e: &io::Error) -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if e.raw_os_error() == Some(libc::ESTALE) {
            return true;
        }
        e.kind() == io::ErrorKind::NotFound
    }
}

#[cfg(windows)]
mod os {
    use super::{Lock, Proof};
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;

    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x1 | 0x2 | 0x4;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x1;
    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x2;
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_LOCK_VIOLATION: i32 = 33;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        event: *mut c_void,
    }

    #[repr(C)]
    #[derive(Default)]
    struct ByHandleFileInformation {
        attributes: u32,
        created: [u32; 2],
        accessed: [u32; 2],
        written: [u32; 2],
        volume_serial: u32,
        size_high: u32,
        size_low: u32,
        links: u32,
        index_high: u32,
        index_low: u32,
    }

    unsafe extern "system" {
        fn LockFileEx(
            file: *mut c_void,
            flags: u32,
            reserved: u32,
            bytes_low: u32,
            bytes_high: u32,
            overlapped: *mut Overlapped,
        ) -> i32;
        fn GetFileInformationByHandle(file: *mut c_void, info: *mut ByHandleFileInformation)
        -> i32;
    }

    // GENERIC_READ | GENERIC_WRITE, sharing read, write and delete, so the holder can
    // delete the sidecar while a waiter has it open (SS-10 FILE_SHARE_DELETE).
    pub(super) fn open_rw(p: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .share_mode(FILE_SHARE_READ_WRITE_DELETE)
            .open(p)
    }

    // A delete-pending file refuses the open with ERROR_ACCESS_DENIED, which a real
    // permission error also returns: retry, and let T10 surface a lasting one (§2.5).
    pub(super) fn open_retryable(e: &io::Error) -> bool {
        e.raw_os_error() == Some(ERROR_ACCESS_DENIED)
    }

    pub(super) fn try_lock_exclusive(f: &File) -> io::Result<bool> {
        let mut ov = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            event: std::ptr::null_mut(),
        };
        let flags = LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY;
        // SAFETY: the handle is open for the call; `ov` is a zeroed OVERLAPPED that
        // outlives it (the handle is synchronous, so the call completes before return).
        let ok = unsafe { LockFileEx(f.as_raw_handle(), flags, 0, u32::MAX, u32::MAX, &mut ov) };
        if ok != 0 {
            return Ok(true);
        }
        let e = io::Error::last_os_error();
        match e.raw_os_error() {
            Some(ERROR_LOCK_VIOLATION) => Ok(false),
            _ => Err(e),
        }
    }

    fn handle_id(f: &File) -> io::Result<(u64, u64)> {
        let mut info = ByHandleFileInformation::default();
        // SAFETY: the handle is open for the call; `info` is a valid out-pointer.
        let ok = unsafe { GetFileInformationByHandle(f.as_raw_handle(), &mut info) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let index = (u64::from(info.index_high) << 32) | u64::from(info.index_low);
        Ok((u64::from(info.volume_serial), index))
    }

    pub(super) fn file_id(f: &File) -> io::Result<(u64, u64)> {
        handle_id(f)
    }

    pub(super) fn path_id(p: &Path) -> io::Result<(u64, u64)> {
        // No access rights: enough to query the file's identity.
        let f = std::fs::OpenOptions::new()
            .access_mode(0)
            .share_mode(FILE_SHARE_READ_WRITE_DELETE)
            // Directory identity guards use the same volume/file-index query.
            // This flag permits opening directories and does not change file IDs.
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(p)?;
        handle_id(&f)
    }

    // File indexes are real here, and an exclusive LockFileEx would block the read-back.
    pub(super) fn names_this_file(_f: &File, _p: &Path, _relock: &Lock<'_>) -> io::Result<Proof> {
        Ok(Proof::Ours)
    }

    pub(super) fn id_retryable(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::NotFound || e.raw_os_error() == Some(ERROR_ACCESS_DENIED)
    }
}

#[cfg(test)]
mod tests;
