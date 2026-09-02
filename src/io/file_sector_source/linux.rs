//! Linux read-side platform hooks: sequential-access hint at open +
//! periodic page-cache eviction during streaming reads.
//!
//! `POSIX_FADV_SEQUENTIAL` at open widens the kernel's readahead window
//! so each pread aggregates into fewer NFS round-trips. `DONTNEED` on
//! the consumed window (called periodically by the caller) drops the
//! already-read pages from the page cache so a large streaming read
//! doesn't fill memory and starve concurrent writes. Together they
//! mirror the write-side WritebackPipeline's policy.
// See docs/file-sector-source-linux.md — rationale and regression history.

use std::fs::File;
use std::os::unix::io::AsRawFd;

pub(crate) fn hint_sequential(file: &File, _len_bytes: u64) {
    // Best-effort: return value ignored. A fadvise failure has no
    // user-observable consequence.
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
}

/// Drop pages in the half-open byte range `[start, start+len)` from
/// the page cache. Called periodically by `read_sectors` to bound the
/// read-side page cache pressure.
pub(crate) fn drop_window(file: &File, start: u64, len: u64) {
    unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            start as i64,
            len as i64,
            libc::POSIX_FADV_DONTNEED,
        );
    }
}

// Async-prefetch `len` bytes at `offset`: queues readahead(2) without
// waiting, so the next batch's I/O overlaps current-batch processing.
// See docs/file-sector-source-linux.md — why this beats kernel readahead alone.
pub(crate) fn prefetch(file: &File, offset: u64, len: u64) {
    unsafe {
        libc::readahead(file.as_raw_fd(), offset as i64, len as usize);
    }
}
