//! Windows platform impl for [`super::WritebackFile`].
//!
//! Current behaviour:
//!
//! - `preallocate` is a debug-logged no-op (no `fallocate`-equivalent
//!   that keeps the reported size).
//! - `durable_sync` delegates to `File::sync_all` (`FlushFileBuffers`),
//!   unbounded unlike the Linux/macOS impls.
//!
//! See docs/writeback-file-windows.md for the rationale.

use std::fs::File;
use std::io;

pub(super) fn preallocate(_file: &File, size_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "WritebackFile preallocate size_hint={size_bytes} skipped (no-op on windows)"
    );
}

pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    // `File::sync_all` on Windows is `FlushFileBuffers`. Not wrapped in
    // the bounded-syscall primitive (see the module doc) — unbounded.
    file.sync_all()
}
