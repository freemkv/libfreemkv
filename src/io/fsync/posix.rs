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
// ENOTSUP goes beyond PostgreSQL's list on purpose: some FUSE/network mounts return it.
const SYNC_TOLERATED: &[i32] = &[libc::EBADF, libc::EINVAL, libc::ENOTSUP];
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const OPEN_TOLERATED: &[i32] = &[];
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const SYNC_TOLERATED: &[i32] = &[];

fn has_errno(e: &io::Error, errnos: &[i32]) -> bool {
    e.raw_os_error().is_some_and(|n| errnos.contains(&n))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[path = "posix_tests.rs"]
mod tests;
