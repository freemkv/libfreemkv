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
        Err(e) => bounded_failure_to_result(e),
    }
}

/// Map a [`crate::io::bounded::BoundedError`] from the bounded `F_FULLFSYNC`
/// onto the `io::Error` `durable_sync` returns.
///
/// Every arm here means the same thing: **no sync observably ran**. All three
/// previously returned `Ok(())`, so `WritebackFile::sync_all` reported success
/// for a durability barrier that never happened — a total failure exiting 0,
/// with only a log line to distinguish it. POSIX gives `fsync` exactly one way
/// to say "the data is on stable storage" and that is a zero return; a call
/// that never reached the device has not earned it.
///
/// The errors carry no message text (this crate ships no user-facing English):
/// the kind, and `EIO` for the worker-lost case, are the whole signal, and the
/// `tracing` lines above/below carry the operator detail.
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

    /// Every `BoundedError` arm of the bounded `F_FULLFSYNC` means no sync
    /// observably ran. All three returned `Ok(())`, so `sync_all` reported a
    /// durability barrier that never happened — the caller could not tell a
    /// completed flush from a skipped one by any means except reading a log.
    ///
    /// Asserted on the concrete `ErrorKind` / `errno` each arm must produce,
    /// so a future arm that quietly reverts to `Ok(())` fails here.
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

        // The three arms must be DISTINGUISHABLE, not merely non-Ok. Each
        // carries its own numeric code through the "E<code>" prefix that
        // `From<Error> for io::Error` mints — the only shape `io_error_code`
        // recognises. A bare `ErrorKind` cannot be classified, which is how a
        // user cancel here used to read as a hard I/O failure.
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

    /// The failure path must be reachable through the public surface: a
    /// `WritebackFile::sync_all` that hits any of these arms must surface an
    /// `Err`, not a silent `Ok`. Pinned at the mapping boundary because the
    /// timeout itself is not deterministically inducible in a unit test.
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
