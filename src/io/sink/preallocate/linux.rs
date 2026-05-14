//! Linux `fallocate(FALLOC_FL_KEEP_SIZE)` preallocation.
//!
//! `KEEP_SIZE` reserves extents without changing the apparent file
//! length, which matches the muxer's expectation that writes still grow
//! the file naturally.

use std::fs::File;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

pub(super) fn preallocate_impl(file: &File, size_bytes: u64) {
    let fd = file.as_raw_fd();
    // FALLOC_FL_KEEP_SIZE = 0x01.
    let rc = unsafe { libc::fallocate(fd, libc::FALLOC_FL_KEEP_SIZE, 0, size_bytes as i64) };
    tracing::debug!(
        target: "mux",
        "LocalFileSink fallocate size_hint={size_bytes} rc={rc} ok={}",
        rc == 0
    );
}
