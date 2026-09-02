//! macOS platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fcntl(F_PREALLOCATE)`, macOS's fallocate-equiv.
//!   Tries a contiguous allocation first, falls back to scattered extents.
//! - `durable_sync`: `fcntl(F_FULLFSYNC)` wrapped in
//!   [`crate::io::bounded::bounded_syscall`] with a 60 s deadline; falls
//!   back to plain `fsync` if unsupported.
//!
//! See docs/writeback-file-macos.md for the full rationale.

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

// fd-reuse safety: a leaked worker thread must not hit a recycled fd
// number, so we try_clone an owned File into the closure instead of a
// bare fd. See docs/writeback-file-macos.md.
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
        Err(e) => bounded_failure_to_result(e),
    }
}

// Maps a BoundedError onto the io::Error durable_sync returns. Every arm
// means no sync observably ran — never mapped to Ok. No message text
// (no user-facing English); see docs/writeback-file-macos.md.
fn bounded_failure_to_result(e: crate::io::bounded::BoundedError) -> io::Result<()> {
    match e {
        crate::io::bounded::BoundedError::Timeout => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all F_FULLFSYNC timed out after 60s; data NOT durably flushed, kernel will flush on close"
            );
            Err(crate::error::Error::SyncTimeout.into())
        }
        crate::io::bounded::BoundedError::Halted => {
            tracing::warn!(
                target: "mux",
                "WritebackFile::sync_all F_FULLFSYNC skipped (halt requested); data NOT durably flushed, kernel will flush on close"
            );
            Err(crate::error::Error::Halted.into())
        }
        crate::io::bounded::BoundedError::WorkerLost => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all F_FULLFSYNC worker lost before completion; data NOT durably flushed, kernel will flush on close"
            );
            Err(crate::error::Error::SyncWorkerLost.into())
        }
    }
}

#[cfg(test)]
#[cfg(target_os = "macos")]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    // Regression for the fd-reuse fix in durable_sync: try_clone succeeds
    // and yields a distinct fd. The real race is non-deterministic; this
    // structural check is the accepted substitute (docs/writeback-file-macos.md).
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

    // Every BoundedError arm means no sync observably ran; asserted on the
    // concrete ErrorKind/errno so a future arm reverting to Ok(()) fails
    // here. See docs/writeback-file-macos.md.
    #[test]
    fn every_bounded_failure_is_reported_as_an_error() {
        use crate::io::bounded::BoundedError;

        let timeout = bounded_failure_to_result(BoundedError::Timeout)
            .expect_err("a timed-out F_FULLFSYNC must be an error");
        assert_eq!(
            timeout.kind(),
            io::ErrorKind::TimedOut,
            "a timed-out F_FULLFSYNC must not be reported as a completed sync"
        );

        let halted = bounded_failure_to_result(BoundedError::Halted)
            .expect_err("a halted F_FULLFSYNC must be an error");
        assert_eq!(
            halted.kind(),
            io::ErrorKind::Interrupted,
            "a halted F_FULLFSYNC must not be reported as a completed sync"
        );

        // The three arms must be DISTINGUISHABLE, not merely non-Ok. Each carries its
        // own numeric code via the "E<code>" prefix `From<Error> for io::Error` mints —
        // a bare `ErrorKind` can't be classified, so a user cancel used to read as I/O failure.
        let lost = bounded_failure_to_result(BoundedError::WorkerLost)
            .expect_err("a lost F_FULLFSYNC worker must be an error");
        assert!(
            lost.to_string()
                .starts_with(&format!("E{}", crate::error::E_SYNC_WORKER_LOST)),
            "a lost worker must be identifiable, got {lost}"
        );
        assert!(
            timeout
                .to_string()
                .starts_with(&format!("E{}", crate::error::E_SYNC_TIMEOUT)),
            "a timeout must be distinguishable from a lost worker, got {timeout}"
        );
        assert!(
            crate::error::is_halt(&halted),
            "a halt must satisfy the crate's own is_halt(), or the CLI reports a \
             user cancel as a failure; got {halted}"
        );
    }

    // A WritebackFile::sync_all hitting any of these arms must surface Err,
    // not a silent Ok. Pinned here since the timeout isn't deterministically
    // inducible in a unit test.
    #[test]
    fn bounded_failures_are_never_mapped_to_ok() {
        use crate::io::bounded::BoundedError;
        for e in [
            BoundedError::Timeout,
            BoundedError::Halted,
            BoundedError::WorkerLost,
        ] {
            assert!(
                bounded_failure_to_result(e).is_err(),
                "a bounded F_FULLFSYNC failure must never map to Ok"
            );
        }
    }
}
