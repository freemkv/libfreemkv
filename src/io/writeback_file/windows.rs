//! Windows platform impl for [`super::WritebackFile`].
//!
//! Current behaviour:
//!
//! - `preallocate` is a debug-logged no-op. Windows has no
//!   `fallocate`-equivalent that keeps the reported size, so extent
//!   reservation is not wired up.
//! - `durable_sync` delegates to the std `File::sync_all`, which on
//!   Windows maps to `FlushFileBuffers`. Unlike the Linux/macOS impls
//!   this is NOT wrapped in the bounded-syscall primitive (that would
//!   need an `unsafe impl Send` for `RawHandle`, which cannot be
//!   validated without a Windows test env), so a wedged UNC/SMB share
//!   can block the final flush. This deviation is documented on
//!   [`super::WritebackFile::sync_all`] and the parent module's
//!   Halt-safety section.

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
