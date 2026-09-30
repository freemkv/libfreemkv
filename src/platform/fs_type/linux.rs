//! Linux `fstatfs`-based filesystem type detection.
//!
//! Recognised local-FS magics: ext2/3/4, xfs, btrfs, tmpfs. NFS is the
//! one network FS this layer cares about (the buffering decision keys
//! off it). Anything else maps to [`FsType::Unknown`].

use std::os::unix::io::RawFd;

use super::FsType;

// Magic numbers from `<linux/magic.h>`. Kept literal here so we don't
// depend on libc exposing each one — only `NFS_SUPER_MAGIC` is
// guaranteed to be present across libc / musl revisions.
const EXT2_SUPER_MAGIC: i64 = 0xEF53;
const XFS_SUPER_MAGIC: i64 = 0x5846_5342;
const BTRFS_SUPER_MAGIC: i64 = 0x9123_683E;
const TMPFS_MAGIC: i64 = 0x0102_1994;

// Classify an `f_type` magic from `fstatfs`.
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

/// `fstatfs` classification of an open fd. Used by the writeback pipeline,
/// which knows the open `File` but not its original path.
pub(super) fn detect_fd_impl(fd: RawFd) -> FsType {
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstatfs(fd, &mut buf) };
    if rc != 0 {
        return FsType::Unknown;
    }
    #[allow(clippy::unnecessary_cast)]
    classify_f_type(buf.f_type as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Magics as literals (<linux/magic.h>), not the constants above, so a wrong
    // constant cannot pass by agreeing with itself.
    #[test]
    fn classify_f_type_maps_magics() {
        assert_eq!(classify_f_type(0x6969), FsType::Nfs);
        for magic in [0xEF53, 0x5846_5342, 0x9123_683E, 0x0102_1994] {
            assert_eq!(classify_f_type(magic), FsType::Local, "{magic:#x}");
        }
        assert_eq!(classify_f_type(0), FsType::Unknown);
        assert_eq!(classify_f_type(0x6969 + 1), FsType::Unknown);
    }
}
