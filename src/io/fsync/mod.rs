//! Platform-aware crash-durability primitives.
//!
//! Two flush operations need OS-specific handling to make a write survive a
//! crash / power loss:
//!
//! - [`dir`] — fsync a directory so a prior `rename(2)` into it is durable.
//!   After a crash a renamed file's dirent can otherwise be lost even though
//!   the rename returned, because it is still page-cache-only. This is a POSIX
//!   concept: on Windows std cannot even open a directory as a `File` (it does
//!   not set `FILE_FLAG_BACKUP_SEMANTICS`), and NTFS/ReFS commit the rename's
//!   dirent without an explicit directory flush — so it is a no-op there
//!   rather than a failed open that logs on every marker write.
//!
//! - [`file_durable`] — fsync a file's contents + metadata. Opens the file
//!   **read+write**: on Windows `File::sync_all` maps to `FlushFileBuffers`,
//!   which requires a handle with write access and returns
//!   `ERROR_ACCESS_DENIED` (os error 5) on a read-only handle. (A read-only
//!   `File::open` + `sync_all` is legal on POSIX, which is why that bug only
//!   bit Windows.) The open mode is platform-uniform, so this lives here with
//!   no dispatch.
//!
//! Per the crate convention (see [`crate::io::writeback_file`]), platform
//! dispatch happens once here via cfg-gated `mod` decls — callers carry no
//! inline `#[cfg(...)]`.

use std::io;
use std::path::Path;

#[cfg(not(windows))]
mod posix;
#[cfg(windows)]
mod windows;

#[cfg(not(windows))]
use posix as platform;
#[cfg(windows)]
use windows as platform;

/// fsync a directory so a prior `rename(2)` into it is durable. Best-effort:
/// failures are logged and swallowed, never propagated — the renamed file's
/// bytes are already synced and the caller's write itself succeeded. No-op on
/// Windows (see module docs).
pub fn dir(path: &Path) {
    platform::fsync_dir(path)
}

/// Durably flush an existing file's contents + metadata to stable storage.
///
/// Opens the file read+write (not read-only) so the flush succeeds on every
/// platform — see the module docs for the Windows `FlushFileBuffers` rationale.
/// The file must already exist; its bytes are left intact (no create/truncate).
pub fn file_durable(path: &Path) -> io::Result<()> {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `file_durable` opens read+write (so the flush works on Windows) and
    /// syncs an existing file; a missing path surfaces as `Err` so the caller
    /// treats it as "not durably synced". Platform-uniform — same on
    /// unix/windows.
    #[test]
    fn file_durable_ok_for_existing_err_for_missing() {
        let td = tempfile::tempdir().unwrap();
        let f = td.path().join("data.bin");
        std::fs::write(&f, b"durable").unwrap();
        assert!(
            file_durable(&f).is_ok(),
            "an existing file must open read+write and fsync cleanly"
        );
        assert!(
            file_durable(&td.path().join("absent.bin")).is_err(),
            "a missing file must surface the open failure as Err"
        );
    }

    /// `dir` is best-effort: it must return normally for a real directory
    /// (POSIX fsyncs it, Windows no-ops) and must swallow — never panic on —
    /// a missing directory.
    #[test]
    fn dir_is_best_effort_never_panics() {
        let td = tempfile::tempdir().unwrap();
        dir(td.path());
        dir(&td.path().join("does-not-exist"));
    }
}
