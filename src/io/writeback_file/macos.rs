//! macOS platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fcntl(F_PREALLOCATE)` — macOS's fallocate-equiv.
//!   Reserves a contiguous extent when possible, falling back to a
//!   non-contiguous reservation if the FS can't satisfy it. Reported
//!   file size is unchanged (`F_ALLOCATEALL` is not set, so allocation
//!   is "best effort up to length"; growth happens via writes).
//! - `durable_sync`: `fcntl(F_FULLFSYNC)` wrapped in
//!   [`crate::io::bounded::bounded_syscall`] with a 60 s deadline.
//!   F_FULLFSYNC is HFS+/APFS's true-fsync (flushes the disk's own
//!   write cache) — what `fsync` should have been on macOS. Falls back
//!   to plain `fsync` if F_FULLFSYNC returns ENOTSUP.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::time::Duration;

/// libc `F_PREALLOCATE` — not exposed by the `libc` crate on all macOS
/// SDK versions, so define it here.
const F_PREALLOCATE: libc::c_int = 42;
/// Allocate from current EOF.
const F_PEOFPOSMODE: libc::c_int = 3;
/// Hint: contiguous extent preferred.
const F_ALLOCATECONTIG: libc::c_uint = 0x00000002;
/// Allocate all the requested bytes (fall back to non-contig if needed).
const F_ALLOCATEALL: libc::c_uint = 0x00000004;

/// `fcntl(F_FULLFSYNC)` opcode. Documented in `man 2 fcntl` on macOS;
/// not in the `libc` crate as a named constant.
const F_FULLFSYNC: libc::c_int = 51;

/// `fstore_t` layout matches `sys/fcntl.h`. Repr is C-stable so we can
/// build it manually.
#[repr(C)]
struct Fstore {
    fst_flags: libc::c_uint,
    fst_posmode: libc::c_int,
    fst_offset: libc::off_t,
    fst_length: libc::off_t,
    fst_bytesalloc: libc::off_t,
}

pub(super) fn preallocate(file: &File, size_bytes: u64) {
    let mut fst = Fstore {
        fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: size_bytes as libc::off_t,
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

pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();
    match crate::io::bounded::bounded_syscall(
        None,
        Duration::from_secs(60),
        move || -> io::Result<()> {
            // Try F_FULLFSYNC first. If it isn't supported on this
            // filesystem (older HFS, some network mounts) fall back to
            // plain fsync — better than nothing.
            let rc = unsafe { libc::fcntl(fd, F_FULLFSYNC, 0) };
            if rc == 0 {
                return Ok(());
            }
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOTSUP) {
                let rc = unsafe { libc::fsync(fd) };
                if rc == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            } else {
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
