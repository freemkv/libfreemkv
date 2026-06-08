//! Linux `fallocate(FALLOC_FL_KEEP_SIZE)` preallocation.
//!
//! `KEEP_SIZE` reserves extents without changing the apparent file
//! length, which matches the muxer's expectation that writes still grow
//! the file naturally.

use std::fs::File;
use std::os::unix::io::AsRawFd;

pub(super) fn preallocate_impl(file: &File, size_bytes: u64) {
    let fd = file.as_raw_fd();
    // Clamp to the signed `off_t` range fallocate expects; an unchecked
    // `as i64` cast would wrap a >= 2^63 size to a negative length that
    // fallocate rejects with EINVAL (silent no-op).
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX);
    // FALLOC_FL_KEEP_SIZE = 0x01.
    let rc = unsafe { libc::fallocate(fd, libc::FALLOC_FL_KEEP_SIZE, 0, len) };
    tracing::debug!(
        target: "mux",
        "LocalFileSink fallocate size_hint={size_bytes} rc={rc} ok={}",
        rc == 0
    );
}
