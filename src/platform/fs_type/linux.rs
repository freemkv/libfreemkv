//! Linux `statfs64`-based filesystem type detection.
//!
//! Recognised local-FS magics: ext2/3/4, xfs, btrfs, tmpfs. NFS is the
//! one network FS this layer cares about (the buffering decision keys
//! off it). Anything else maps to [`FsType::Unknown`].

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;

use super::FsType;

// Magic numbers from `<linux/magic.h>`. Kept literal here so we don't
// depend on libc exposing each one — only `NFS_SUPER_MAGIC` is
// guaranteed to be present across libc / musl revisions.
const EXT2_SUPER_MAGIC: i64 = 0xEF53;
const XFS_SUPER_MAGIC: i64 = 0x5846_5342;
const BTRFS_SUPER_MAGIC: i64 = 0x9123_683E;
const TMPFS_MAGIC: i64 = 0x0102_1994;

/// Classify an `f_type` magic from `statfs`/`fstatfs`. Single source
/// of truth for the magic comparisons used by both the path-based
/// (`detect_impl`) and fd-based (`detect_fd_impl`) entry points.
///
/// Cast to `i64` because `statfs::f_type` is signed `__fsword_t` on
/// glibc and unsigned `c_ulong` on musl; a portable comparison needs a
/// common type. On glibc x86_64 both already are i64 — clippy flags
/// the cast as unnecessary on that target only, but we need it for
/// musl.
#[allow(clippy::unnecessary_cast)]
fn classify_f_type(f_type: i64) -> FsType {
    let nfs_magic = libc::NFS_SUPER_MAGIC as i64;
    if f_type == nfs_magic {
        return FsType::Nfs;
    }
    match f_type {
        EXT2_SUPER_MAGIC | XFS_SUPER_MAGIC | BTRFS_SUPER_MAGIC | TMPFS_MAGIC => FsType::Local,
        _ => FsType::Unknown,
    }
}

pub(super) fn detect_impl(path: &Path) -> FsType {
    let cpath = match CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return FsType::Unknown,
    };
    // `statfs64` is repr(C); zeroing is the documented init pattern for
    // the kernel uapi struct.
    let mut buf: libc::statfs64 = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs64(cpath.as_ptr(), &mut buf) };
    if rc != 0 {
        return FsType::Unknown;
    }
    #[allow(clippy::unnecessary_cast)]
    classify_f_type(buf.f_type as i64)
}

/// fd-based variant of [`detect_impl`]. Same return semantics. Used
/// by the writeback pipeline, which knows the open `File` but not its
/// original path.
pub(super) fn detect_fd_impl(fd: RawFd) -> FsType {
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstatfs(fd, &mut buf) };
    if rc != 0 {
        return FsType::Unknown;
    }
    #[allow(clippy::unnecessary_cast)]
    classify_f_type(buf.f_type as i64)
}
