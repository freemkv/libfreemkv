//! Pipeline-progress reporting for the rip pipeline.
//!
//! Architecture rule: ONE progress signal type. Every long-running
//! pipeline operation (`Disc::copy`, `Disc::patch`, `verify_title`) emits the
//! same [`PassProgress`] shape via the [`Progress`] trait. Consumers (autorip)
//! compute their own single derived view from these fields and never reach
//! into per-pass internals.
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
    /// after all rip passes complete. The library's mux pipeline does not
    /// currently emit `PassProgress` itself, so this variant exists for
    /// consumers (e.g. autorip) that label their own mux phase with the
    /// same `PassKind` vocabulary.
    Mux,
    /// Sector verification — reads every sector and classifies health.
    Verify,
}

/// One progress sample from a pipeline phase.
///
/// `work_done / work_total` is the per-pass percentage — always 0..=100%
/// regardless of which kind of pass is running. `bytes_good_total` is the
/// cumulative count of confirmed-clean bytes across the whole rip; useful
/// for the "data recovered" stat the user sees.
///
/// For `PassKind::Verify`, the fields map as follows:
/// - `work_done` = sectors read so far
/// - `work_total` = total sectors in title
/// - `bytes_good_total` = good + slow + recovered sectors × 2048
/// - `bytes_unreadable_total` = bad sectors × 2048
/// - `bytes_pending_total` = 0 (verify processes sequentially, nothing pending)
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
/// absence of a beat, and a slow-but-alive loop shows steady progress.
///
/// `tick` is cheap on the hot path: it reads one `Instant` and compares. For
/// pure-CPU inner loops where even that is too much, use
/// [`tick_cpu`](Heartbeat::tick_cpu), which only consults the clock every 256
/// calls.
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
        if self.cpu_counter % 256 != 0 {
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

/// A consumer of pipeline progress events. Library code calls
/// `Progress::report` once per inner-loop iteration (throttling is the
/// consumer's job — `report` is cheap; the library doesn't gate it).
///
/// Returns `true` to continue, `false` to request early stop.
///
/// No `Send`/`Sync` bound — `report` is always called from the same thread
/// running the pipeline, so closures with non-`Sync` captures (e.g.
/// `RefCell<PassProgressState>`) work directly. Blanket impl below lets
/// callers pass closures without explicit struct types.
pub trait Progress {
    fn report(&self, p: &PassProgress) -> bool;
}

impl<F: Fn(&PassProgress) -> bool> Progress for F {
    fn report(&self, p: &PassProgress) -> bool {
        (self)(p)
    }
}

#[cfg(test)]
mod heartbeat_tests {
    use super::Heartbeat;
    use std::time::Duration;

    /// A fresh heartbeat does not beat on the first tick — the interval has not
    /// elapsed — so a fast loop is not spammed.
    #[test]
    fn first_tick_does_not_beat() {
        let mut hb = Heartbeat::with_interval("test", Duration::from_secs(60));
        assert!(!hb.tick(0, 100));
        assert!(!hb.tick(50, 100));
    }

    /// Once the interval elapses, exactly one beat fires, then the throttle
    /// resets.
    #[test]
    fn beats_once_per_interval() {
        let mut hb = Heartbeat::with_interval("test", Duration::from_millis(10));
        assert!(!hb.tick(1, 100));
        std::thread::sleep(Duration::from_millis(15));
        assert!(hb.tick(2, 100), "should beat after interval elapsed");
        // Immediately after, throttle suppresses the next.
        assert!(!hb.tick(3, 100));
    }

    /// tick_cpu only consults the clock every 256 calls: the first 255 calls
    /// never beat even with a zero interval.
    #[test]
    fn tick_cpu_throttles_clock_reads() {
        let mut hb = Heartbeat::with_interval("test", Duration::from_nanos(0));
        for _ in 0..255 {
            assert!(!hb.tick_cpu(0, 100));
        }
        // 256th call consults the clock; with a zero interval it beats.
        assert!(hb.tick_cpu(0, 100));
    }
}
