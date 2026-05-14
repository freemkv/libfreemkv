//! Linux read-side platform hooks: sequential-access hint at open +
//! periodic page-cache eviction during streaming reads.
//!
//! ## Why both
//!
//! `POSIX_FADV_SEQUENTIAL` at open widens the kernel's readahead window
//! so each pread aggregates into fewer NFS round-trips. `DONTNEED` on
//! the consumed window (called periodically by the caller) drops the
//! already-read pages from the page cache so an 85 GB streaming ISO
//! read doesn't fill memory and starve concurrent writes (the MKV
//! output during mux). Together they mirror the write-side
//! WritebackPipeline's policy.
//!
//! ## History
//!
//! Pre-Phase-1 (0.20.7 baseline) had both. Phase 1's introduction of
//! `FileSectorSource` silently dropped the read-side DONTNEED, and
//! 0.21.2's revert of `SEQUENTIAL` (mistakenly attributing a regression
//! to it) removed the hint. Net effect: 85 GB of ISO reads pinned in
//! the page cache + no readahead widening → mux throughput collapse
//! from 18 MB/s historical to 2.7-8 MB/s on 0.21.x. Restored in 0.21.6.

use std::fs::File;
use std::os::unix::io::AsRawFd;

pub(super) fn hint_sequential(file: &File, _len_bytes: u64) {
    // Best-effort: return value ignored. A fadvise failure has no
    // user-observable consequence.
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
}

/// Drop pages in the half-open byte range `[start, start+len)` from
/// the page cache. Called periodically by `read_sectors` to bound the
/// read-side page cache pressure.
pub(super) fn drop_window(file: &File, start: u64, len: u64) {
    unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            start as i64,
            len as i64,
            libc::POSIX_FADV_DONTNEED,
        );
    }
}
