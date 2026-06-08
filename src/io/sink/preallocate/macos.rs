//! macOS `F_PREALLOCATE` extent reservation.
//!
//! `fcntl(F_PREALLOCATE)` with `F_ALLOCATECONTIG | F_ALLOCATEALL` first
//! (prefer a contiguous run but accept scattered extents to satisfy the
//! full length) and fall back to `F_ALLOCATEALL` alone on failure.
//! Reported file size is unchanged — the muxer's writes still grow it.

use std::fs::File;
use std::os::unix::io::AsRawFd;

use crate::io::platform_macos::{
    F_ALLOCATEALL, F_ALLOCATECONTIG, F_PEOFPOSMODE, F_PREALLOCATE, Fstore,
};

pub(super) fn preallocate_impl(file: &File, size_bytes: u64) {
    let fd = file.as_raw_fd();
    // Clamp to the signed `off_t` range; an unchecked `as off_t` cast
    // would wrap a >= 2^63 size to a negative length.
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX) as libc::off_t;
    let mut store = Fstore {
        // Prefer a contiguous run but accept scattered extents to
        // satisfy the full length. Without F_ALLOCATEALL the first
        // attempt is best-effort and can return rc=0 with a partial
        // allocation, so the fallback below would never fire. Matches
        // writeback_file/macos.rs.
        fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len,
        fst_bytesalloc: 0,
    };
    let mut rc = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut Fstore) };
    if rc == -1 {
        // Fall back to non-contiguous only.
        store.fst_flags = F_ALLOCATEALL;
        rc = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut Fstore) };
    }
    tracing::debug!(
        target: "mux",
        "LocalFileSink F_PREALLOCATE size_hint={size_bytes} rc={rc} bytesalloc={}",
        store.fst_bytesalloc
    );
}
