// macOS: hint the kernel to prefetch via fcntl(F_RDADVISE, &radvisory),
// the closest equivalent to POSIX_FADV_SEQUENTIAL.
// See docs/file-sector-source-macos.md — F_RDADVISE background.

use std::fs::File;
use std::os::unix::io::AsRawFd;

// Cap on the byte length we pass to `F_RDADVISE`; 64 MiB is generous
// without over-asking the OS's cache.
// See docs/file-sector-source-macos.md — RDADVISE_MAX_BYTES rationale.
const RDADVISE_MAX_BYTES: i64 = 64 * 1024 * 1024;

pub(crate) fn hint_sequential(file: &File, len_bytes: u64) {
    let bytes = (len_bytes as i64).min(RDADVISE_MAX_BYTES);
    let mut ra = libc::radvisory {
        ra_offset: 0,
        ra_count: bytes as libc::c_int,
    };
    // Best-effort.
    unsafe {
        libc::fcntl(file.as_raw_fd(), libc::F_RDADVISE, &mut ra);
    }
}

// No direct POSIX_FADV_DONTNEED equivalent; F_NOCACHE is too coarse
// (disables caching for the whole fd), so this is a best-effort no-op.
// See docs/file-sector-source-macos.md — drop_window rationale.
pub(crate) fn drop_window(_file: &File, _start: u64, _len: u64) {}

// Async-prefetch `[offset, offset+len)` via the same F_RDADVISE
// primitive as the open-time hint, targeted at a moving window.
// See docs/file-sector-source-macos.md — prefetch details.
pub(crate) fn prefetch(file: &File, offset: u64, len: u64) {
    let bytes = (len as i64).min(RDADVISE_MAX_BYTES);
    let mut ra = libc::radvisory {
        ra_offset: offset as libc::off_t,
        ra_count: bytes as libc::c_int,
    };
    // Best-effort — kernel hint only.
    unsafe {
        libc::fcntl(file.as_raw_fd(), libc::F_RDADVISE, &mut ra);
    }
}
