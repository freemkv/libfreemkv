//! macOS platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fcntl(F_PREALLOCATE)`, macOS's fallocate-equiv.
//!   Tries a contiguous allocation first, falls back to scattered extents.
//!
//! The durable flush (`F_FULLFSYNC`) lives in [`crate::io::flush`] (stop design §2.10).

use std::fs::File;
use std::os::unix::io::AsRawFd;

use crate::io::platform_macos::{
    F_ALLOCATEALL, F_ALLOCATECONTIG, F_PEOFPOSMODE, F_PREALLOCATE, Fstore,
};

pub(super) fn preallocate(file: &File, size_bytes: u64) -> bool {
    // Clamp to the signed `off_t` range; an unchecked `as off_t` cast
    // would wrap a >= 2^63 size to a negative length.
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX) as libc::off_t;
    let mut fst = Fstore {
        fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len,
        fst_bytesalloc: 0,
    };
    // First attempt: contiguous.
    // SAFETY: a valid borrowed fd and a live stack `Fstore`.
    let mut rc = unsafe { libc::fcntl(file.as_raw_fd(), F_PREALLOCATE, &mut fst) };
    if rc == -1 {
        // Fall back: drop the contiguous hint, allow scattered extents.
        fst.fst_flags = F_ALLOCATEALL;
        // SAFETY: as above.
        rc = unsafe { libc::fcntl(file.as_raw_fd(), F_PREALLOCATE, &mut fst) };
    }
    tracing::debug!(
        target: "mux",
        "WritebackFile F_PREALLOCATE size_hint={size_bytes} rc={rc} bytes_allocated={} ok={}",
        fst.fst_bytesalloc,
        rc != -1
    );
    // Not released at close here (as before): the trim is Linux KEEP_SIZE only.
    false
}
