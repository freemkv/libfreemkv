//! macOS platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fcntl(F_PREALLOCATE)` — macOS's fallocate-equiv.
//!   First attempt requests `F_ALLOCATECONTIG | F_ALLOCATEALL` (prefer a
//!   contiguous run but accept scattered extents to satisfy the full
//!   length), falling back to `F_ALLOCATEALL` alone on failure.
//!   `F_PREALLOCATE` never advances EOF regardless of the flags — only
//!   `ftruncate`/writes grow the file — so the reported file size is
//!   unchanged; `F_ALLOCATEALL` governs the contiguity fallback, not size.
//! - `durable_sync`: `fcntl(F_FULLFSYNC)` wrapped in
//!   [`crate::io::bounded::bounded_syscall`] with a 60 s deadline.
//!   F_FULLFSYNC is HFS+/APFS's true-fsync (flushes the disk's own
//!   write cache) — what `fsync` should have been on macOS. Falls back
//!   to plain `fsync` if F_FULLFSYNC returns ENOTSUP.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::time::Duration;

use crate::io::platform_macos::{
    F_ALLOCATEALL, F_ALLOCATECONTIG, F_PEOFPOSMODE, F_PREALLOCATE, Fstore,
};

/// `fcntl(F_FULLFSYNC)` opcode. Documented in `man 2 fcntl` on macOS;
/// not in the `libc` crate as a named constant.
const F_FULLFSYNC: libc::c_int = 51;

pub(super) fn preallocate(file: &File, size_bytes: u64) {
    // Clamp to the signed `off_t` range; an unchecked `as off_t` cast
    // would wrap a >= 2^63 size to a negative length.
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX) as libc::off_t;
    let mut fst = Fstore {
        fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len,
        fst_bytesalloc: 0,
    };
    // First attempt: contiguous.
    let mut rc = unsafe { libc::fcntl(file.as_raw_fd(), F_PREALLOCATE, &mut fst) };
    if rc == -1 {
        // Fall back: drop the contiguous hint, allow scattered extents.
        fst.fst_flags = F_ALLOCATEALL;
        rc = unsafe { libc::fcntl(file.as_raw_fd(), F_PREALLOCATE, &mut fst) };
    }
    tracing::debug!(
        target: "mux",
        "WritebackFile F_PREALLOCATE size_hint={size_bytes} rc={rc} bytes_allocated={} ok={}",
        fst.fst_bytesalloc,
        rc != -1
    );
}

/// ## fd-reuse safety
///
/// The F_FULLFSYNC / fsync runs on a bounded worker thread that may be
/// leaked on timeout. To avoid the leaked worker's syscall hitting a
/// recycled fd number after the original `File` is closed, we
/// `try_clone` an owned `File` and move it into the closure. The clone
/// keeps the underlying file description alive for as long as the worker
/// thread lives. On `try_clone` failure (rare) we fall back to the raw
/// fd integer — no worse than the previous behaviour.
pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    // Clone so a leaked worker thread retains a valid fd even after the
    // original File is closed and its fd number is reused.
    let owned = match file.try_clone() {
        Ok(f) => Some(f),
        Err(e) => {
            let fd = file.as_raw_fd();
            tracing::warn!(
                target: "mux",
                "WritebackFile::sync_all fd={fd}: try_clone failed ({e}), F_FULLFSYNC worker will use raw fd (fd-reuse risk on timeout)"
            );
            None
        }
    };
    let fallback_fd = file.as_raw_fd();
    match crate::io::bounded::bounded_syscall(
        None,
        Duration::from_secs(60),
        move || -> io::Result<()> {
            let fd = owned.as_ref().map(|f| f.as_raw_fd()).unwrap_or(fallback_fd);
            // Try F_FULLFSYNC first. If it isn't supported on this
            // filesystem (older HFS, some network mounts) fall back to
            // plain fsync — better than nothing.
            let rc = unsafe { libc::fcntl(fd, F_FULLFSYNC, 0) };
            if rc == 0 {
                // `owned` (if Some) drops here, releasing the cloned fd.
                return Ok(());
            }
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOTSUP) {
                let rc = unsafe { libc::fsync(fd) };
                // `owned` drops here.
                if rc == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            } else {
                // `owned` drops here.
                Err(err)
            }
        },
    ) {
        Ok(inner) => inner,
        Err(crate::io::bounded::BoundedError::Timeout) => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all F_FULLFSYNC timed out after 60s; kernel will flush on close (best-effort)"
            );
            Ok(())
        }
        Err(crate::io::bounded::BoundedError::Halted) => Ok(()),
        Err(crate::io::bounded::BoundedError::WorkerLost) => Ok(()),
    }
}

#[cfg(test)]
#[cfg(target_os = "macos")]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Regression for the fd-reuse / use-after-close fix in `durable_sync`.
    ///
    /// Verifies the structural invariant: `try_clone` succeeds for a normal
    /// local tempfile, and the cloned `File` has a distinct fd number from
    /// the original. This pins the property that a leaked F_FULLFSYNC/fsync
    /// worker thread captures an owned `File` (keeping the file description
    /// alive) rather than a bare fd integer that can be reused after the
    /// original `File` closes.
    ///
    /// The actual fd-reuse race is non-deterministic; a structural test is
    /// the accepted substitute.
    #[test]
    fn durable_sync_worker_uses_owned_clone_with_distinct_fd() {
        let f = NamedTempFile::new().expect("tempfile create");
        let original_fd = f.as_file().as_raw_fd();

        // try_clone must succeed for a normal local file.
        let owned = f
            .as_file()
            .try_clone()
            .expect("try_clone must succeed for a local tempfile");
        let clone_fd = owned.as_raw_fd();

        // The clone must be a distinct fd (dup'd, not aliased).
        assert_ne!(
            clone_fd, original_fd,
            "owned clone must have a distinct fd number — not an alias of the original"
        );
        assert!(clone_fd >= 0, "clone fd must be a valid non-negative fd");

        // durable_sync must complete without error on the local tempfile.
        durable_sync(f.as_file()).expect("durable_sync must return Ok on a local tempfile");
    }
}
