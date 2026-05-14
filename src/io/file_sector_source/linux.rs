//! Linux: hint the kernel that this fd will be read sequentially so
//! readahead widens. `posix_fadvise(POSIX_FADV_SEQUENTIAL)` is a hint,
//! not a guarantee — the kernel still owns the policy decision.

use std::fs::File;
use std::os::unix::io::AsRawFd;

pub(super) fn hint_sequential(file: &File, _len_bytes: u64) {
    // Best-effort: return value ignored. A fadvise failure has no
    // user-observable consequence (reads still work, just without the
    // widened readahead window).
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
}
