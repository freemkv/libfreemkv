//! POSIX directory-fsync. Active on unix and any non-Windows fallback target
//! (BSD, illumos, …) — all share the same `File::open(dir).sync_all()`
//! semantics. The Windows no-op lives in the sibling `windows` module.

use std::path::Path;

pub(super) fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}
