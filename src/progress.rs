//! Pipeline-progress reporting for the rip pipeline.
//!
//! v0.13.16 architecture rule: ONE progress signal type. Every long-running
//! pipeline operation (`Disc::copy`, `Disc::patch`, mux) emits the same
//! `PassProgress` shape via the `Progress` trait. Consumers (autorip) compute
//! a single `PipelineStats` derived view and never reach into per-pass
//! internals.
//!
//! Why this matters: pre-0.13.16 the API leaked `pos`, `bytes_good`,
//! `work_done`, `bytes_pending`, `Finished/NonTrimmed` mapfile semantics —
//! and consumers reinvented the math each time they wanted a percentage.
//! UIs ended up reading one source while server-side computed from another,
//! producing wrong percentages without anyone noticing.

/// Identifies which pipeline phase the progress event belongs to.
///
/// Consumers can render a phase-specific label (e.g. "Sweep", "Trim
/// (reverse)", "Scrape", "Mux") or just use a generic "Pass N" label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassKind {
    /// `Disc::copy` — initial sweep across the entire disc.
    Sweep,
    /// `Disc::patch` retry pass with `block_sectors >= 2`. `reverse=true`
    /// means walking bad ranges from highest to lowest LBA.
    Trim { reverse: bool },
    /// `Disc::patch` final pass at 1 sector per block.
    Scrape { reverse: bool },
    /// Demux ISO → output (MKV / M2TS / network). Single phase that runs
    /// after all rip passes complete.
    Mux,
}

/// One progress sample from a pipeline phase.
///
/// `work_done / work_total` is the per-pass percentage — always 0..=100%
/// regardless of which kind of pass is running. `bytes_good_total` is the
/// cumulative count of confirmed-clean bytes across the whole rip; useful
/// for the "data recovered" stat the user sees.
#[derive(Debug, Clone, Copy)]
pub struct PassProgress {
    pub kind: PassKind,
    /// Bytes processed in this pass so far. Monotonically non-decreasing.
    pub work_done: u64,
    /// Total bytes this pass will process. Constant for the duration of
    /// the pass.
    pub work_total: u64,
    /// Cumulative bytes confirmed clean (`Finished` mapfile state) across
    /// every pass run on this rip. Doesn't change across pass boundaries.
    pub bytes_good_total: u64,
    /// Total disc capacity in bytes. Constant.
    pub bytes_total_disc: u64,
}

/// A consumer of pipeline progress events. Library code calls
/// `Progress::report` once per inner-loop iteration (throttling is the
/// consumer's job — `report` is cheap; the library doesn't gate it).
///
/// No `Send`/`Sync` bound — `report` is always called from the same thread
/// running the rip pipeline, so closures with non-`Sync` captures (e.g.
/// `RefCell<PassProgressState>`) work directly. Blanket impl below lets
/// callers pass closures without explicit struct types.
pub trait Progress {
    fn report(&self, p: &PassProgress);
}

impl<F: Fn(&PassProgress)> Progress for F {
    fn report(&self, p: &PassProgress) {
        (self)(p)
    }
}
