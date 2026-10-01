//! The one event vocabulary every stage reports through (pipeline design §2.5).
//!
//! A stage emits through [`Ctx::events`](crate::Ctx); a consumer implements [`Events`] once
//! and matches the variants it renders. Data only: no display logic, no English text.

use crate::disc::DiscTitle;
use crate::progress::PassProgress;

/// What a stage reports while it runs. Borrowed: an [`Events`] impl copies out what it keeps.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Event<'a> {
    /// Source bytes delivered so far by the read stage, of `total` planned (0 if unknown).
    BytesRead { bytes: u64, total: u64 },
    /// A read error at `lba` was zero-filled and skipped (skip-errors policy).
    SectorSkipped { lba: u64 },
    /// `units` damaged AACS units in the read starting at `lba` were blanked and counted.
    UnitBlanked { lba: u64, units: u64 },
    /// The adaptive live read batch changed to `new_size` sectors.
    BatchSizeChanged {
        new_size: u16,
        reason: BatchSizeReason,
    },
    /// A recovery, extract or image pass's running totals.
    Pass(&'a PassProgress),
    /// Bytes handed to the output so far, of the planned `total` (0 if unknown).
    BytesWritten { bytes: u64, total: u64 },
    /// While the output is flushed at the end: bytes made durable so far. At most 4 per
    /// second and none while nothing moves, so silence means a stalled flush (stop §4.5).
    BytesDurable { bytes: u64, total: u64 },
    /// The output opened for `title` as it will be written: streams the sink refused are
    /// left out (indices compact); `MuxOutcome::undelivered_streams` keeps source indices.
    OutputOpened { title: &'a DiscTitle },
}

/// Why the adaptive batch sizer changed size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSizeReason {
    /// Read failed; sizer halved the batch.
    Shrunk,
    /// Clean-read streak threshold hit; sizer doubled toward preferred.
    Probed,
}

/// A consumer of [`Event`]s. Called from stage threads (the read producer, the mux pump):
/// keep it cheap and non-blocking.
pub trait Events: Send + Sync {
    /// One event. The default ignores it.
    fn event(&self, _e: &Event<'_>) {}
}

/// An [`Events`] that ignores everything.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoEvents;

impl Events for NoEvents {}

impl<F: Fn(&Event<'_>) + Send + Sync> Events for F {
    fn event(&self, e: &Event<'_>) {
        self(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shrunk != Probed: distinct meanings (error vs. recovery), must not compare equal.
    #[test]
    fn batch_size_reason_variants_are_not_equal() {
        assert_ne!(BatchSizeReason::Shrunk, BatchSizeReason::Probed);
    }
}
