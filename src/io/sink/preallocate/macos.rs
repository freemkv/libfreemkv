//! macOS `F_PREALLOCATE` extent reservation.
//!
//! `fcntl(F_PREALLOCATE)` with `F_ALLOCATECONTIG` first (try for a
//! contiguous run) and fall back to `F_ALLOCATEALL` (non-contig OK).
//! Reported file size is unchanged — the muxer's writes still grow it.

use std::fs::File;
use std::os::unix::io::AsRawFd;

// Mirror the Darwin `fstore_t` struct from `<sys/fcntl.h>`. libc on
// some Rust toolchains/versions doesn't ship this binding, so define
// it locally with the layout the kernel ABI requires.
#[repr(C)]
struct Fstore {
    fst_flags: libc::c_uint,
    fst_posmode: libc::c_int,
    fst_offset: libc::off_t,
    fst_length: libc::off_t,
    fst_bytesalloc: libc::off_t,
}

// Constants from <sys/fcntl.h>.
const F_PREALLOCATE: libc::c_int = 42;
const F_ALLOCATECONTIG: libc::c_uint = 0x0000_0002;
const F_ALLOCATEALL: libc::c_uint = 0x0000_0004;
const F_PEOFPOSMODE: libc::c_int = 3;

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
