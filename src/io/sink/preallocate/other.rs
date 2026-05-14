//! Fallback preallocate impl. No-op.

use std::fs::File;

pub(super) fn preallocate_impl(_file: &File, size_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "LocalFileSink preallocate size_hint={size_bytes} skipped (no platform impl)"
    );
}
