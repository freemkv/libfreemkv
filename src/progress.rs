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
    pub work_done: u64,
    pub work_total: u64,
    pub bytes_good_total: u64,
    pub bytes_unreadable_total: u64,
    pub bytes_pending_total: u64,
    pub bytes_total_disc: u64,
    pub disc_duration_secs: Option<f64>,
    /// How many bytes of the worst-case damage (unreadable + pending) fall
    /// within the main title's extents. Zero means none of the damage
    /// affects the main movie — it's all in extras/menus.
    pub bytes_bad_in_main_title: u64,
    /// Main title duration in seconds. Same as disc_duration_secs when the
    /// disc has one dominant title, but separate so consumers can show both.
    pub main_title_duration_secs: Option<f64>,
    /// Main title size in bytes (sum of extent sizes).
    pub main_title_size_bytes: Option<u64>,
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
