//! Windows directory-fsync: a no-op.
//!
//! Directory fsync is a POSIX concept. std cannot open a directory as a `File`
//! on Windows (it does not set `FILE_FLAG_BACKUP_SEMANTICS`), so the POSIX impl
//! could only ever fail the open and log a spurious warning on every marker /
//! mapfile write. NTFS/ReFS commit a rename's directory entry without an
//! explicit directory flush, so skipping it here is correct — not a durability
//! regression. (File-content durability is handled platform-uniformly by
//! [`super::file_durable`].)

use std::path::Path;

pub(super) fn fsync_dir(_dir: &Path) {}
