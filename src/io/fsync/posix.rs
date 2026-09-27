//! POSIX directory-fsync. Active on unix and any non-Windows fallback target
//! (BSD, illumos, …) — all share the same `File::open(dir).sync_all()`
//! semantics. The Windows no-op lives in the sibling `windows` module.

use std::io;
use std::path::Path;

// Some filesystems refuse to open or fsync a directory; like PostgreSQL's
// fsync_fname_ext, treat those as "nothing to sync" and propagate the rest.
pub(super) fn fsync_dir(dir: &Path) -> io::Result<()> {
    let f = match std::fs::File::open(dir) {
        Ok(f) => f,
        Err(e) if has_errno(&e, OPEN_TOLERATED) => return Ok(()),
        Err(e) => return Err(e),
    };
    match f.sync_all() {
        Err(e) if !has_errno(&e, SYNC_TOLERATED) => Err(e),
        _ => Ok(()),
    }
}

// libc is only a dependency on Linux/macOS; other POSIX targets tolerate nothing.
#[cfg(any(target_os = "linux", target_os = "macos"))]
const OPEN_TOLERATED: &[i32] = &[libc::EACCES, libc::EISDIR];
#[cfg(any(target_os = "linux", target_os = "macos"))]
const SYNC_TOLERATED: &[i32] = &[libc::EBADF, libc::EINVAL, libc::ENOTSUP];
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const OPEN_TOLERATED: &[i32] = &[];
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const SYNC_TOLERATED: &[i32] = &[];

fn has_errno(e: &io::Error, errnos: &[i32]) -> bool {
    e.raw_os_error().is_some_and(|n| errnos.contains(&n))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[test]
    fn only_listed_errnos_are_tolerated() {
        let open = |n| has_errno(&io::Error::from_raw_os_error(n), OPEN_TOLERATED);
        let sync = |n| has_errno(&io::Error::from_raw_os_error(n), SYNC_TOLERATED);
        assert!(open(libc::EACCES) && open(libc::EISDIR));
        assert!(!open(libc::EIO) && !open(libc::ENOENT));
        assert!(sync(libc::EBADF) && sync(libc::EINVAL) && sync(libc::ENOTSUP));
        assert!(!sync(libc::EIO) && !sync(libc::ENOSPC));
        assert!(!has_errno(&io::Error::other("x"), OPEN_TOLERATED));
    }

    // Unreadable directory (EACCES on open) is tolerated; a missing one is not.
    #[test]
    fn unreadable_dir_is_tolerated_missing_dir_is_not() {
        use std::os::unix::fs::PermissionsExt;
        let td = tempfile::tempdir().unwrap();
        let dir = td.path().join("wx-only");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o300)).unwrap();
        let res = fsync_dir(&dir);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(res.is_ok(), "{res:?}");
        assert!(fsync_dir(&td.path().join("missing")).is_err());
    }
}
