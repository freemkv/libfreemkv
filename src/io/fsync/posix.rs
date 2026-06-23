//! POSIX directory-fsync. Active on unix and any non-Windows fallback target
//! (BSD, illumos, …) — all share the same `File::open(dir).sync_all()`
//! semantics. The Windows no-op lives in the sibling `windows` module.

use std::path::Path;

pub(super) fn fsync_dir(dir: &Path) {
    match std::fs::File::open(dir) {
        Ok(f) => {
            if let Err(e) = f.sync_all() {
                tracing::warn!(path = %dir.display(), error = %e, "failed to fsync directory");
            }
        }
        Err(e) => {
            tracing::warn!(path = %dir.display(), error = %e, "could not open directory to fsync");
        }
    }
}
