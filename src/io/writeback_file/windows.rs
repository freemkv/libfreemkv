//! Windows platform impl for [`super::WritebackFile`].
//!
//! `preallocate` is a debug-logged no-op (no `fallocate`-equivalent that keeps the
//! reported size). The durable flush (`FlushFileBuffers`, halt-aware and stall-bounded)
//! lives in [`crate::io::flush`] (stop design §2.10).

use std::fs::File;

pub(super) fn preallocate(_file: &File, size_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "WritebackFile preallocate size_hint={size_bytes} skipped (no-op on windows)"
    );
}
