//! macOS `F_PREALLOCATE` extent reservation.
//!
//! `fcntl(F_PREALLOCATE)` with `F_ALLOCATECONTIG` first (try for a
//! contiguous run) and fall back to `F_ALLOCATEALL` (non-contig OK).
//! Reported file size is unchanged — the muxer's writes still grow it.

use std::fs::File;
use std::os::unix::io::AsRawFd;

use crate::io::platform_macos::{
    F_ALLOCATEALL, F_ALLOCATECONTIG, F_PEOFPOSMODE, F_PREALLOCATE, Fstore,
};

pub(super) fn preallocate_impl(file: &File, size_bytes: u64) {
    let fd = file.as_raw_fd();
    let mut store = Fstore {
        fst_flags: F_ALLOCATECONTIG,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: size_bytes as libc::off_t,
        fst_bytesalloc: 0,
    };
    let mut rc = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut Fstore) };
    if rc == -1 {
        // Fall back to non-contiguous.
        store.fst_flags = F_ALLOCATEALL;
        rc = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut Fstore) };
    }
    tracing::debug!(
        target: "mux",
        "LocalFileSink F_PREALLOCATE size_hint={size_bytes} rc={rc} bytesalloc={}",
        store.fst_bytesalloc
    );
}
