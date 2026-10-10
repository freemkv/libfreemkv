//! Platform-aware crash-durability primitives.
//!
//! [`dir`] fsyncs a directory so a prior `rename(2)` into it is durable (no-op on Windows);
//! [`file_durable`] fsyncs a file's contents + metadata, opened read+write so the flush also
//! succeeds on Windows.
//!
//! Per the crate convention (see `crate::io::writeback_file`), platform
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
    if let Err(e) = dir_checked(path) {
        tracing::warn!(path = %path.display(), error = %e, "failed to fsync directory");
    }
}

/// Like [`dir`] but returns the failure, for callers whose success claim
/// depends on a new directory entry being durable. `Ok` on Windows.
pub fn dir_checked(path: &Path) -> io::Result<()> {
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
#[path = "mod_tests.rs"]
mod tests;
