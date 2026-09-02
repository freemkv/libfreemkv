//! Platform-aware crash-durability primitives.
//!
//! [`dir`] fsyncs a directory so a prior `rename(2)` into it is durable
//! (no-op on Windows); [`file_durable`] fsyncs a file's contents + metadata,
//! opened read+write so the flush also succeeds on Windows. See
//! docs/fsync.md for the full per-platform rationale.
//!
//! Per the crate convention (see [`crate::io::writeback_file`]), platform
//! dispatch happens once here via cfg-gated `mod` decls — no inline `#[cfg]`.

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

    // Opens read+write (works on Windows) and syncs an existing file;
    // missing path surfaces as `Err` ("not durably synced").
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
