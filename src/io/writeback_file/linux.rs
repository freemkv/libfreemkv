//! Linux platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fallocate(0)` — reserve extents AND extend the
//!   reported file size up-front. iter12 (2026-05-17): switched from
//!   `FALLOC_FL_KEEP_SIZE` to plain mode 0. With KEEP_SIZE the file's
//!   reported length stayed at 0 and every write past the previous
//!   EOF triggered an NFS SETATTR (server-side metadata commit) to
//!   grow the file. With mode 0, the file is full-size from the
//!   start; subsequent writes overwrite pre-extended region in place
//!   with zero metadata ops. At end of mux, caller `ftruncate`s down
//!   to actual content size if hint was an overestimate.
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
    // Mode 0 (no KEEP_SIZE) — reserve extents AND extend the
    // reported file size to `size_bytes`. On NFS this eliminates the
    // per-write SETATTR that would otherwise fire each time writes
    // crossed the previous EOF.
    let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size_bytes as i64) };
    tracing::debug!(
        target: "mux",
        "WritebackFile fallocate size_hint={size_bytes} rc={rc} ok={}",
        rc == 0
    );
}

/// Run `fsync` on `file` with a 60 s deadline. On timeout we log loudly
/// and return `Ok(())` — the kernel will still flush on close, so the
/// data is best-effort durable; the alternative (trap the thread for
/// the rest of the rip) defeats `/api/stop`.
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
        Err(crate::io::bounded::BoundedError::Halted) => Ok(()),
        Err(crate::io::bounded::BoundedError::WorkerLost) => Ok(()),
    }
}
