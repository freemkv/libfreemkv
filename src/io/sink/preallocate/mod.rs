//! Per-OS extent preallocation. Best-effort; failures are logged at
//! debug and otherwise swallowed because the file is still usable
//! without the size reservation — only large-file fragmentation gets
//! marginally worse.

use std::fs::File;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod other;

#[cfg(target_os = "linux")]
use linux::preallocate_impl;
#[cfg(target_os = "macos")]
use macos::preallocate_impl;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use other::preallocate_impl;

/// Reserve `size_bytes` of disk space for `file`'s on-disk extents.
/// Reported file size is unchanged — writes still grow the file
/// naturally; only the allocator's extent map is primed.
pub(super) fn preallocate(file: &File, size_bytes: u64) {
    preallocate_impl(file, size_bytes);
}
