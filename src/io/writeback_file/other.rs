//! Fallback platform impl for [`super::WritebackFile`] on targets
//! without a dedicated implementation (BSDs, illumos, etc.).
//!
//! - `preallocate` is a logged no-op.
//! - `durable_sync` calls `File::sync_all` directly (no bounded-syscall
//!   wrapper — the wrapper depends on Linux/macOS unix idioms that
//!   aren't universally portable). If a future BSD impl needs the
//!   60-s deadline, it should land in its own per-OS file rather than
//!   bloat this fallback.

use std::fs::File;
use std::io;

pub(super) fn preallocate(_file: &File, size_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "WritebackFile preallocate size_hint={size_bytes} skipped (no impl on this target)"
    );
}

pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    file.sync_all()
}
