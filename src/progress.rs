//! Pipeline-progress reporting for the rip pipeline.
//!
//! Architecture rule: ONE progress signal type. Every long-running
//! pipeline operation (`freemkv_engine::recovery::{copy, patch}`, extract) emits the
//! same [`PassProgress`] shape as [`Event::Pass`](crate::Event::Pass) through the run's
//! [`Ctx`](crate::Ctx). Consumers compute their own single derived view from these fields
//! and never reach into per-pass internals.

/// Identifies which pipeline phase the progress event belongs to.
///
/// Consumers can render a phase-specific label (e.g. "Sweep", "Trim
/// (reverse)", "Scrape", "Mux") or just use a generic "Pass N" label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassKind {
    /// `freemkv_engine::recovery::copy` — initial sweep across the entire disc.
    Sweep,
    /// `freemkv_engine::recovery::patch` retry pass with `block_sectors >= 2`. `reverse=true`
    /// means walking bad ranges from highest to lowest LBA.
    Trim { reverse: bool },
    /// `freemkv_engine::recovery::patch` final pass at 1 sector per block.
    Scrape { reverse: bool },
    /// Demux ISO → output (MKV / M2TS / network). Single phase that runs
    /// after all rip passes complete. The library's mux pipeline does not
    /// currently emit `PassProgress` itself, so this variant exists for
    /// consumers (e.g. autorip) that label their own mux phase with the
    /// same `PassKind` vocabulary.
    Mux,
    /// Sector verification — reads every sector and classifies health.
    Verify,
    /// A disc's decrypted file tree written out (`dir://`).
    Extract,
}

/// One located bad/not-yet-good range, annotated with the chapter and movie
/// time it falls in. This is the *rendered* drilldown a client draws: the LBA
/// and sector count place it on the disc map, while `chapter` and
/// `time_offset_secs` tell the user *what* is affected. Computed by the library
/// (which owns the mapfile and title) so no client ever re-derives it. If the
/// mapfile becomes a mapdb, this type and its producer change; clients don't.
#[derive(Debug, Clone)]
pub struct LocatedRange {
    /// First sector (LBA) of the range.
    pub lba: u64,
    /// Length of the range in sectors.
    pub count: u32,
    /// Movie time this range spans, in milliseconds (range bytes ÷ title
    /// bytes/sec). Used to sort the drilldown and size the "largest gap".
    pub duration_ms: f64,
    /// 1-based chapter the range falls in, if it lands inside the title.
    pub chapter: Option<u32>,
    /// Movie time offset (seconds) where the range begins, if in-title.
    pub time_offset_secs: Option<f64>,
}

/// The fully-rendered "where is the damage" view for one progress sample: the
/// located drilldown plus the derived movie-time figures. A client maps this
/// straight to its UI — it never touches the mapfile. `Default` is the empty
/// (no-damage / not-applicable) view, used by phases that don't locate ranges
/// (verify, extract, mux-label).
#[derive(Debug, Clone, Default)]
pub struct LocatedProgress {
    /// Located not-yet-good ranges, largest-movie-time first, capped (see
    /// `truncated`).
    pub ranges: Vec<LocatedRange>,
    /// Total number of located ranges before the cap (so a client can say
    /// "N sections").
    pub num_ranges: u32,
    /// How many ranges were dropped by the display cap (`ranges.len()` is the
    /// kept count; this is the "+X more").
    pub truncated: u32,
    /// Main-feature movie time still at risk: duration of the not-yet-good
    /// ranges that intersect the title extents, in milliseconds. `0` when all
    /// damage is out-of-feature (menus/extras).
    pub main_at_risk_ms: f64,
    /// Movie time of the single largest range, in milliseconds.
    pub largest_gap_ms: f64,
}

/// One progress sample from a pipeline phase.
///
/// `work_done / work_total` is the per-pass percentage — always 0..=100%
/// regardless of which kind of pass is running. `bytes_good_total` is the
/// cumulative count of confirmed-clean bytes across the whole rip; useful
/// for the "data recovered" stat the user sees.
#[derive(Debug, Clone)]
pub struct PassProgress {
    pub kind: PassKind,
    pub work_done: u64,
    pub work_total: u64,
    pub bytes_good_total: u64,
    pub bytes_unreadable_total: u64,
    pub bytes_pending_total: u64,
    /// Bytes that FAILED to read and await retry (NonTrimmed/NonScraped) —
    /// distinct from `bytes_pending_total` which also includes not-yet-attempted
    /// (NonTried) bytes. Used so "lost" counts only failed reads, never unread
    /// sectors.
    pub bytes_retryable_total: u64,
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
    /// The fully-rendered "where is the damage" drilldown for this sample:
    /// located ranges + at-risk movie time. Empty (`Default`) for phases that
    /// don't locate ranges. A client renders the disc map + section list from
    /// this and NEVER reads the mapfile itself.
    pub located: LocatedProgress,
}

impl PassProgress {
    /// Percentage of work completed for this pass (0..=100).
    ///
    /// Returns `100.0` if `work_total` is zero to avoid division by zero.
    /// Clamped to `0..=100` so a transient `work_done > work_total`
    /// (e.g. a count that briefly overshoots) never reports above 100%.
    pub fn work_pct(&self) -> f64 {
        if self.work_total == 0 {
            return 100.0;
        }
        (self.work_done as f64 / self.work_total as f64 * 100.0).clamp(0.0, 100.0)
    }

    /// Percentage of the disc that is confirmed clean (0..=100).
    ///
    /// Computed from `bytes_good_total / bytes_total_disc`, clamped to
    /// `0..=100`.
    pub fn good_pct(&self) -> f64 {
        if self.bytes_total_disc == 0 {
            return 100.0;
        }
        (self.bytes_good_total as f64 / self.bytes_total_disc as f64 * 100.0).clamp(0.0, 100.0)
    }

    /// Percentage of the disc that is unreadable (0..=100).
    pub fn bad_pct(&self) -> f64 {
        if self.bytes_total_disc == 0 {
            return 0.0;
        }
        (self.bytes_unreadable_total as f64 / self.bytes_total_disc as f64 * 100.0)
            .clamp(0.0, 100.0)
    }

    /// Percentage of the disc that is still pending (not yet attempted or needs retry).
    pub fn pending_pct(&self) -> f64 {
        if self.bytes_total_disc == 0 {
            return 0.0;
        }
        (self.bytes_pending_total as f64 / self.bytes_total_disc as f64 * 100.0).clamp(0.0, 100.0)
    }
}

/// Throttled liveness beacon for long-running loops.
///
/// "No silent hangs": every loop that can block for a long time (sector
/// sweep, CSS crack, UDF prefetch, mux feed, key trials, drive poll) holds a
/// `Heartbeat` and calls [`tick`](Heartbeat::tick) each iteration. `tick`
/// emits a `DEBUG` event on target `freemkv::heartbeat` at most once per
/// interval (default 5s), so a stalled loop is visible in the log as the
/// absence of a beat.
#[derive(Debug)]
pub struct Heartbeat {
    phase: &'static str,
    interval: std::time::Duration,
    start: std::time::Instant,
    last: std::time::Instant,
    /// Counter for the CPU-loop fast path (clock read every 256 calls).
    cpu_counter: u32,
}

impl Heartbeat {
    /// Default heartbeat interval.
    pub const DEFAULT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

    /// Construct a heartbeat for `phase` with the default 5s interval.
    pub fn new(phase: &'static str) -> Self {
        Self::with_interval(phase, Self::DEFAULT_INTERVAL)
    }

    /// Construct a heartbeat with an explicit interval (used by tests).
    pub fn with_interval(phase: &'static str, interval: std::time::Duration) -> Self {
        let now = std::time::Instant::now();
        Self {
            phase,
            interval,
            start: now,
            last: now,
            cpu_counter: 0,
        }
    }

    /// Record a heartbeat at position `pos` of `total`. Emits at most once per
    /// interval. Returns `true` if a beat was actually emitted (mostly useful
    /// for tests).
    pub fn tick(&mut self, pos: u64, total: u64) -> bool {
        let now = std::time::Instant::now();
        if now.duration_since(self.last) < self.interval {
            return false;
        }
        self.last = now;
        self.emit(pos, total, now);
        true
    }

    /// CPU-loop variant: only consults the clock every 256 calls, so the cost
    /// on a tight pure-CPU inner loop is a single increment + compare most
    /// iterations. Otherwise identical to [`tick`](Heartbeat::tick).
    pub fn tick_cpu(&mut self, pos: u64, total: u64) -> bool {
        self.cpu_counter = self.cpu_counter.wrapping_add(1);
        if !self.cpu_counter.is_multiple_of(256) {
            return false;
        }
        self.tick(pos, total)
    }

    fn emit(&self, pos: u64, total: u64, now: std::time::Instant) {
        let pct = if total == 0 {
            0.0
        } else {
            (pos as f64 / total as f64 * 100.0).clamp(0.0, 100.0)
        };
        let elapsed_ms = now.duration_since(self.start).as_millis() as u64;
        tracing::debug!(
            target: "freemkv::heartbeat",
            phase = self.phase,
            pos,
            total,
            pct,
            elapsed_ms,
            "alive"
        );
    }
}

#[cfg(test)]
#[path = "progress_heartbeat_tests.rs"]
mod heartbeat_tests;

#[cfg(test)]
#[path = "progress_pass_progress_tests.rs"]
mod pass_progress_tests;
