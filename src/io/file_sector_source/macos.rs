//! macOS: hint the kernel to prefetch a generous chunk. macOS has no
//! direct `POSIX_FADV_SEQUENTIAL` equivalent; the idiomatic hint is
//! `fcntl(F_RDADVISE, &radvisory)` describing the byte range you
//! intend to read soon. We point it at the whole file (clamped to a
//! ceiling so a multi-TB ISO doesn't ask the kernel to prefetch
//! everything at once).

use std::fs::File;
use std::os::unix::io::AsRawFd;

/// `F_RDADVISE` opcode — not in libc's named constants on all SDKs.
const F_RDADVISE: libc::c_int = 44;

/// Cap on the byte length we pass to `F_RDADVISE`. Asking for a
/// multi-GB readahead window is counterproductive — the OS doesn't
/// have that much cache to throw at one fd. 64 MiB is generous for
/// our use case (sweep, mux) and matches the byte-channel cap so the
/// kernel's prefetch ≥ our app-level pipeline depth.
const RDADVISE_MAX_BYTES: i64 = 64 * 1024 * 1024;

/// `radvisory` per `<sys/fcntl.h>`. repr(C) layout is stable.
#[repr(C)]
struct RadAdvisory {
    ra_offset: libc::off_t,
    ra_count: libc::c_int,
}

pub(super) fn hint_sequential(file: &File, len_bytes: u64) {
    let bytes = (len_bytes as i64).min(RDADVISE_MAX_BYTES);
    let mut ra = RadAdvisory {
        ra_offset: 0,
        ra_count: bytes as libc::c_int,
    };
    // Best-effort.
    unsafe {
        libc::fcntl(file.as_raw_fd(), F_RDADVISE, &mut ra);
    }
}
