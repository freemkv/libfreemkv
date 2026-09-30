//! Linux platform impl for [`super::WritebackFile`].
//!
//! - `preallocate`: `fallocate(FALLOC_FL_KEEP_SIZE)` — reserve extents
//!   without growing the reported file size. Reduces extent
//!   fragmentation on large sequential writes (mux output on NFS in
//!   particular).
//!
//! The durable flush lives in [`crate::io::flush`] (stop design §2.10).

use std::fs::File;
use std::os::unix::io::AsRawFd;

use crate::platform::fs_type::{FsType, detect_fd};

/// Pre-reserve extents for `size_bytes` of upcoming sequential writes.
/// Best-effort: a non-zero rc is logged but not propagated, since the
/// caller would just continue with the unreserved file anyway. True if extents were
/// reserved on a detected local filesystem, where releasing them at close is cheap.
pub(super) fn preallocate(file: &File, size_bytes: u64) -> bool {
    // FALLOC_FL_KEEP_SIZE keeps the reported file size at 0 (writes grow it normally)
    // while pre-reserving extents. Clamp to `off_t` range; an unchecked `as i64`
    // cast would wrap a >= 2^63 size to a negative length (EINVAL no-op).
    let len = i64::try_from(size_bytes).unwrap_or(i64::MAX);
    // SAFETY: a valid borrowed fd; the kernel reads no user memory.
    let rc = unsafe { libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, len) };
    tracing::debug!(
        target: "mux",
        "WritebackFile fallocate size_hint={size_bytes} rc={rc} ok={}",
        rc == 0
    );
    rc == 0 && detect_fd(file.as_raw_fd()) == FsType::Local
}
