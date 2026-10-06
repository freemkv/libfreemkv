//! The one run context (pipeline design §2.5): every stage of a run reads its stop token,
//! reports through its events and counts loss into its stats.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::event::{Event, Events, NoEvents};
use crate::halt::Halt;

/// What every stage of one run shares: the stop token, the event sink, the loss counters
/// and the diagnostic switches. Clones share all of them, so a clone observes the same run.
#[derive(Clone)]
pub struct Ctx {
    /// The run's one stop token. A stage that observes it returns [`Error::Halted`](crate::Error::Halted).
    pub halt: Halt,
    /// Where every stage reports progress.
    pub events: Arc<dyn Events>,
    /// Run-wide loss counters, cumulative over every stage built with this context.
    pub stats: Arc<Stats>,
    /// Developer diagnostics, fixed for the run.
    pub diag: Diag,
}

impl Ctx {
    /// A context over `halt` with no events, fresh stats and diagnostics off.
    pub fn new(halt: Halt) -> Self {
        Self {
            halt,
            events: Arc::new(NoEvents),
            stats: Arc::default(),
            diag: Diag::default(),
        }
    }

    /// This context reporting to `events`.
    pub fn with_events(mut self, events: Arc<dyn Events>) -> Self {
        self.events = events;
        self
    }

    /// This context with diagnostics `diag`.
    pub fn with_diag(mut self, diag: Diag) -> Self {
        self.diag = diag;
        self
    }

    pub(crate) fn emit(&self, e: Event<'_>) {
        self.events.event(&e);
    }
}

impl Default for Ctx {
    fn default() -> Self {
        Self::new(Halt::new())
    }
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("halt", &self.halt)
            .field("stats", &self.stats)
            .field("diag", &self.diag)
            .finish_non_exhaustive()
    }
}

/// Loss counters every source reports through, so a count means the same thing on every
/// path. Atomic: the read producer and the mux pump update them while the run goes.
#[derive(Debug, Default)]
pub struct Stats {
    read_skips: AtomicU64,
    bytes_lost: AtomicU64,
    units_blanked: AtomicU64,
    resync_dropped: AtomicU64,
}

impl Stats {
    /// The counters now.
    pub fn snapshot(&self) -> LossReport {
        LossReport {
            read_skips: self.read_skips.load(Ordering::Relaxed),
            bytes_lost: self.bytes_lost.load(Ordering::Relaxed),
            units_blanked: self.units_blanked.load(Ordering::Relaxed),
            resync_dropped: self.resync_dropped.load(Ordering::Relaxed),
        }
    }

    // One read error zero-filled `bytes` of output.
    pub(crate) fn add_skip(&self, bytes: u64) {
        self.read_skips.fetch_add(1, Ordering::Relaxed);
        self.bytes_lost.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_blanked(&self, units: u64) {
        self.units_blanked.fetch_add(units, Ordering::Relaxed);
    }

    pub(crate) fn add_resync_dropped(&self, frames: u64) {
        self.resync_dropped.fetch_add(frames, Ordering::Relaxed);
    }
}

/// A snapshot of [`Stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LossReport {
    /// Zero-filled read-error skips: one per skipped read (live mux) or lossy batch
    /// (extract); the byte count is `bytes_lost`.
    pub read_skips: u64,
    /// Bytes those skips zero-filled (blanked AACS units are not included).
    pub bytes_lost: u64,
    /// Damaged AACS units blanked (each one whole 6144-byte unit of zeros).
    pub units_blanked: u64,
    /// Video frames dropped after a gap until the next keyframe.
    pub resync_dropped: u64,
}

/// Developer diagnostics a run carries instead of stages reading the environment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Diag {
    /// Bypass the codec parsers (raw demux throughput profiling).
    pub skip_parse: bool,
    /// Log per-stage read/feed/parse timings.
    pub profile: bool,
}

impl Diag {
    /// `FREEMKV_SKIP_PARSE` and `FREEMKV_PROFILE` (set = on), read once by the caller.
    pub fn from_env() -> Self {
        Self {
            skip_parse: std::env::var_os("FREEMKV_SKIP_PARSE").is_some(),
            profile: std::env::var_os("FREEMKV_PROFILE").is_some(),
        }
    }
}

#[cfg(test)]
#[path = "ctx_tests.rs"]
mod tests;
