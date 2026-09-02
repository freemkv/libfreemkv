//! Linux platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fallocate(FALLOC_FL_KEEP_SIZE)` — reserve extents
//!   without growing the reported file size. Reduces extent
//!   fragmentation on large sequential writes (mux output on NFS in
//!   particular).
//! - `durable_sync`: `fsync` wrapped in
//!   [`crate::io::bounded::bounded_syscall`] with a 60 s deadline so a
//!   wedged NFS server can't trap the calling thread indefinitely.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::time::Duration;

/// Pre-reserve extents for `size_bytes` of upcoming sequential writes.
/// Best-effort: a non-zero rc is logged but not propagated, since the
/// caller would just continue with the unreserved file anyway.
pub(super) fn preallocate(file: &File, size_bytes: u64) {
    // FALLOC_FL_KEEP_SIZE keeps the reported file size at 0 (writes grow it normally)
    // while pre-reserving extents. Clamp to `off_t` range; an unchecked `as i64`
    // cast would wrap a >= 2^63 size to a negative length (EINVAL no-op).
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX);
    let rc = unsafe { libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, len) };
    tracing::debug!(
        target: "mux",
        "WritebackFile fallocate size_hint={size_bytes} rc={rc} ok={}",
        rc == 0
    );
}

// Bounded `fsync` (60 s deadline); a zero return is the only durability
// signal POSIX gives, so timeout/halt/lost-worker all map to `Err`.
// See docs/writeback-file-linux.md — durable_sync rationale & fd-reuse safety.
pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    // Clone so a leaked worker thread retains a valid fd even after the
    // original File is closed and its fd number is reused.
    let owned = match file.try_clone() {
        Ok(f) => Some(f),
        Err(e) => {
            let fd = file.as_raw_fd();
            tracing::warn!(
                target: "mux",
                "WritebackFile::sync_all fd={fd}: try_clone failed ({e}), fsync worker will use raw fd (fd-reuse risk on timeout)"
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
            let rc = unsafe { libc::fsync(fd) };
            // `owned` (if Some) drops here, releasing the cloned fd.
            if rc == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        },
    ) {
        Ok(inner) => inner,
        Err(e) => bounded_failure_to_result(e),
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    // Regression for the fd-reuse / use-after-close fix in `durable_sync`.
    // Structural test only (the race is non-deterministic and not cleanly testable).
    // See docs/writeback-file-linux.md — durable_sync_worker_uses_owned_clone_with_distinct_fd.
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

// Maps a bounded-fsync failure onto the `io::Error` `durable_sync` returns.
// Every arm means "no sync observably ran"; mirrors the macOS `F_FULLFSYNC` mapping.
// See docs/writeback-file-linux.md — bounded_failure_to_result rationale.
fn bounded_failure_to_result(e: crate::io::bounded::BoundedError) -> io::Result<()> {
    match e {
        crate::io::bounded::BoundedError::Timeout => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all fsync timed out after 60s; data NOT durably flushed, kernel will flush on close"
            );
            Err(crate::error::Error::SyncTimeout.into())
        }
        crate::io::bounded::BoundedError::Halted => {
            tracing::warn!(
                target: "mux",
                "WritebackFile::sync_all fsync skipped (halt requested); data NOT durably flushed, kernel will flush on close"
            );
            Err(crate::error::Error::Halted.into())
        }
        crate::io::bounded::BoundedError::WorkerLost => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all fsync worker lost before completion; data NOT durably flushed, kernel will flush on close"
            );
            // EIO, matching the macOS sibling: a consumer distinguishing these
            // three failures does so on the same value on every platform.
            // ErrorKind::Other carries nothing a caller can branch on.
            Err(crate::error::Error::SyncWorkerLost.into())
        }
    }
}

#[cfg(test)]
mod bounded_failure_tests {
    use super::*;
    use crate::io::bounded::BoundedError;

    /// Every bounded-fsync failure must be an error. Asserted per variant rather
    /// than as a loop so a new variant defaulting to Ok cannot slip through.
    #[test]
    fn no_bounded_fsync_failure_maps_to_ok() {
        assert_eq!(
            bounded_failure_to_result(BoundedError::Timeout)
                .expect_err("a timed-out fsync must be an error")
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            bounded_failure_to_result(BoundedError::Halted)
                .expect_err("a halted fsync must be an error")
                .kind(),
            io::ErrorKind::Interrupted
        );
        assert!(
            bounded_failure_to_result(BoundedError::WorkerLost).is_err(),
            "a lost fsync worker must be an error"
        );
    }
}
