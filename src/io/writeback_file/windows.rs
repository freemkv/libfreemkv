//! Windows platform impl for [`super::WritebackFile`].
//!
//! TODO: this stub matches the design's "validate without a Windows
//! build env, leave a stub" carve-out. The real impl should use:
//!
//! - `SetEndOfFile` + `SetFileValidData` for extent preallocation
//!   (caller needs `SE_MANAGE_VOLUME_NAME` privilege; if unavailable
//!   fall back to a write-zero path or just skip).
//! - `FlushFileBuffers` for fsync-equivalent durable flush.
//!
//! Until then: preallocate is a debug-logged no-op; durable_sync calls
//! the std `File::sync_all` (which on Windows maps to
//! `FlushFileBuffers` internally).

use std::fs::File;
use std::io;

pub(super) fn preallocate(_file: &File, size_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "WritebackFile preallocate size_hint={size_bytes} skipped (windows stub; TODO: SetFileValidData)"
    );
}

pub(super) fn durable_sync(file: &File) -> io::Result<()> {
    // `File::sync_all` on Windows is `FlushFileBuffers`. Acceptable
    // for now; the bounded-syscall wrapper is not used here because
    // the stub also skips the worker-thread + leak machinery (the
    // wrapper would need an `unsafe impl Send` for `RawHandle`, and
    // designing that without a Windows test env is asking for it).
    file.sync_all()
}
