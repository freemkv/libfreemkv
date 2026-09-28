//! Fallback platform impl for [`super::WritebackFile`] on targets
//! without a dedicated implementation (BSDs, illumos, etc.).
//!
//! `preallocate` is a logged no-op; the durable flush lives in [`crate::io::flush`].

use std::fs::File;

pub(super) fn preallocate(_file: &File, size_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "WritebackFile preallocate size_hint={size_bytes} skipped (no impl on this target)"
    );
}
