//! Filesystem-type detection.
//!
//! The buffering architecture (Phase 2) selects different output sinks
//! for local vs network filesystems: NFS gets the adaptive
//! `WritebackFile` machinery on Linux; local disks get `LocalFileSink`
//! and rely on the kernel's default writeback policy. This module
//! provides the construction-site primitive that picks which one.
//!
//! Per the per-OS file-split convention, the actual `statfs` call lives
//! in the matching platform file (`linux.rs`, `macos.rs`, `windows.rs`,
//! `other.rs`); this `mod.rs` exposes only the cross-platform enum and
//! the `detect` entry point.

use std::path::Path;

/// What kind of filesystem a path lives on, to the extent we can tell
/// cheaply at construction time.
///
/// `Unknown` is the fail-open default: a misdetection here should not
/// be load-bearing for correctness, only for the choice of sink (and
/// hence buffering policy). Callers that need a binary local/non-local
/// answer should treat `Unknown` as "probably local".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsType {
    /// A local on-disk filesystem (ext4, xfs, btrfs, apfs, ntfs, …).
    Local,
    /// A network filesystem with NFS-like semantics. The current
    /// detector lumps SMB / UNC into this on Windows because the
    /// buffering policy outcome is the same.
    Nfs,
    /// `statfs` failed, the filesystem type is not on our recognised
    /// list, or we are on an OS without a real implementation.
    Unknown,
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod other;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux::detect_impl;
#[cfg(target_os = "macos")]
use macos::detect_impl;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use other::detect_impl;
#[cfg(target_os = "windows")]
use windows::detect_impl;

/// Best-effort classification of the filesystem under `path`.
///
/// Falls back to [`FsType::Unknown`] on any syscall error or unrecognised
/// filesystem signature. Never panics. Never blocks beyond the cost of
/// a single `statfs(2)` (Unix) or a string check (Windows).
pub fn detect(path: &Path) -> FsType {
    detect_impl(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_does_not_panic_on_missing_path() {
        // Non-existent path should fall through to Unknown, not panic.
        let p = std::path::Path::new("/this/path/should/not/exist/freemkv-test");
        let r = detect(p);
        // We don't assert == Unknown because Windows' heuristic looks at
        // the leading bytes and might still classify; just confirm the
        // call returns rather than panicking.
        let _ = r;
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_tmp_is_local() {
        // `/tmp` on macOS dev rigs is APFS via the symlink to
        // `/private/tmp`. Either way, never NFS.
        let r = detect(std::path::Path::new("/tmp"));
        assert!(
            matches!(r, FsType::Local | FsType::Unknown),
            "expected Local or Unknown for /tmp on macOS, got {r:?}",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_tmp_is_local_or_unknown() {
        // `/tmp` is tmpfs on most distros (which we recognise) but
        // could be ext4 on others. NFS would be unusual.
        let r = detect(std::path::Path::new("/tmp"));
        assert!(
            matches!(r, FsType::Local | FsType::Unknown),
            "expected Local or Unknown for /tmp on Linux, got {r:?}",
        );
    }

    /// Real NFS exercise needs an actual NFS mount and isn't available
    /// in CI. Kept here as a manual probe.
    #[test]
    #[ignore = "needs an NFS mount path (e.g. /mnt/nfs/...) to validate live"]
    fn nfs_path_classified_as_nfs() {
        // Operator passes the mount as FREEMKV_NFS_PROBE; gated behind
        // `--ignored` because there's no portable NFS path.
        let p = std::env::var("FREEMKV_NFS_PROBE").expect("set FREEMKV_NFS_PROBE");
        assert_eq!(detect(std::path::Path::new(&p)), FsType::Nfs);
    }
}
