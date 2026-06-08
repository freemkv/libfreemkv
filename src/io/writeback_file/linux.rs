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
    // FALLOC_FL_KEEP_SIZE = 0x01 — keep the reported file size at 0
    // (writes grow it normally) while still pre-reserving the extents.
    // Clamp to the signed `off_t` range; an unchecked `as i64` cast
    // would wrap a >= 2^63 size to a negative length (EINVAL no-op).
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX);
    let rc = unsafe { libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, len) };
    tracing::debug!(
        target: "mux",
        "WritebackFile fallocate size_hint={size_bytes} rc={rc} ok={}",
        rc == 0
    );
}

/// Run `fsync` on `file` with a 60 s deadline. On timeout — and
/// likewise on halt or a lost worker — we log and return `Ok(())`: the
/// kernel will still flush on close, so the data is best-effort durable.
/// The alternative (trap the thread for the rest of the rip, or return
/// an error that aborts an otherwise-complete mux) is worse, so all
/// three fallbacks return `Ok(())`. `Ok(())` from these paths is NOT a
/// durability barrier — the durable flush did not complete; only the
/// hang is bounded.
pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();
    match crate::io::bounded::bounded_syscall(
        None,
        Duration::from_secs(60),
        move || -> io::Result<()> {
            let rc = unsafe { libc::fsync(fd) };
            if rc == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        },
    ) {
        Ok(inner) => inner,
        Err(crate::io::bounded::BoundedError::Timeout) => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all fsync timed out after 60s; kernel will flush on close (best-effort)"
            );
            Ok(())
        }
        Err(crate::io::bounded::BoundedError::Halted) => {
            tracing::warn!(
                target: "mux",
                "WritebackFile::sync_all fsync skipped (halt requested); data not durably flushed, kernel will flush on close"
            );
            Ok(())
        }
        Err(crate::io::bounded::BoundedError::WorkerLost) => {
            tracing::error!(
                target: "mux",
                "WritebackFile::sync_all fsync worker lost before completion; data not durably flushed, kernel will flush on close"
            );
            Ok(())
        }
    }
}
