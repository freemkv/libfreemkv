//! Windows: the canonical sequential-access hint is
//! `FILE_FLAG_SEQUENTIAL_SCAN`, which must be passed to `CreateFile`
//! at open time and cannot be set afterward via
//! `SetFileInformationByHandle`. Since `FileSectorSource::open` uses a
//! plain `File::open`, the hints in this module are no-op stubs.

use std::fs::File;

/// No-op stub. `FILE_FLAG_SEQUENTIAL_SCAN` can only be set at
/// `CreateFile` open time, which the plain `File::open` path does not
/// do, so there is no post-open hint to issue here.
pub(super) fn hint_sequential(_file: &File, _len_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "FileSectorSource hint_sequential: windows no-op stub"
    );
}

/// Windows page-cache eviction is not exposed via a posix_fadvise
/// equivalent. The kernel does its own working-set management. No-op
/// for now.
pub(super) fn drop_window(_file: &File, _start: u64, _len: u64) {}

/// Windows async-prefetch hint. With FILE_FLAG_SEQUENTIAL_SCAN at
/// open the kernel already prefetches aggressively, so there's no
/// per-range hint we'd add on top. No-op stub for parity with the
/// posix platforms.
pub(super) fn prefetch(_file: &File, _offset: u64, _len: u64) {}
