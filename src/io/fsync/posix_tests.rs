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
