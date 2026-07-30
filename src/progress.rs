//! Pipeline-progress reporting for the rip pipeline.
//!
//! Architecture rule: ONE progress signal type. Every long-running
//! pipeline operation (`freemkv_engine::recovery::{copy, patch}`) emits the
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
///
/// For `PassKind::Verify`, the fields map as follows:
/// - `work_done` = sectors read so far
/// - `work_total` = total sectors in title
/// - `bytes_good_total` = good + slow + recovered sectors × 2048
/// - `bytes_unreadable_total` = bad sectors × 2048
/// - `bytes_pending_total` = 0 (verify processes sequentially, nothing pending)
///
/// NOT `Copy`: `located` carries a `Vec`. Constructed once per (throttled)
/// emission and passed by reference to `Progress::report`, so this costs one
/// small heap alloc per UI tick — cheap, and it makes `PassProgress` the single
/// complete contract a client renders from.
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

#[cfg(test)]
mod pass_progress_tests {
    use super::*;

    /// A zeroed sample. Each test sets only the fields its percentage reads, so
    /// a failure names the field that mattered rather than drowning in a
    /// thirteen-field literal.
    fn sample() -> PassProgress {
        PassProgress {
            kind: PassKind::Sweep,
            work_done: 0,
            work_total: 0,
            bytes_good_total: 0,
            bytes_unreadable_total: 0,
            bytes_pending_total: 0,
            bytes_retryable_total: 0,
            bytes_total_disc: 0,
            disc_duration_secs: None,
            bytes_bad_in_main_title: 0,
            main_title_duration_secs: None,
            main_title_size_bytes: None,
            located: LocatedProgress::default(),
        }
    }

    /// The ordinary case: a quarter of the work done reads 25%, not some other
    /// arrangement of the same three numbers. The exact value is what pins the
    /// arithmetic — `done / total * 100` and `done * total / 100` and
    /// `done / total + 100` all "look like" a percentage and only one is right.
    #[test]
    fn work_pct_is_done_over_total_scaled_to_a_hundred() {
        let p = PassProgress {
            work_done: 250,
            work_total: 1000,
            ..sample()
        };
        assert_eq!(p.work_pct(), 25.0);
    }

    /// Zero total is the divide-by-zero guard, and it must report COMPLETE, not
    /// zero: a pass with no work to do has finished all of it. A UI that read
    /// 0% here would sit at "0%" forever on an empty pass.
    #[test]
    fn work_pct_with_no_work_reports_complete() {
        assert_eq!(sample().work_pct(), 100.0);
    }

    /// The guard must fire on `total == 0` ONLY. With work present the real
    /// arithmetic has to run — a guard inverted to `!=` would short-circuit
    /// every real pass to 100% and divide by zero on the empty one.
    #[test]
    fn work_pct_guard_fires_only_on_zero_total() {
        let p = PassProgress {
            work_done: 1,
            work_total: 4,
            ..sample()
        };
        assert_eq!(p.work_pct(), 25.0, "a non-empty pass must not report 100%");
    }

    /// A transient overshoot clamps rather than reporting above 100%. Sector
    /// counts briefly exceed the total when a pass re-reads, and a progress bar
    /// fed 137% renders past its own end.
    #[test]
    fn work_pct_clamps_an_overshoot_to_a_hundred() {
        let p = PassProgress {
            work_done: 1370,
            work_total: 1000,
            ..sample()
        };
        assert_eq!(p.work_pct(), 100.0);
    }

    #[test]
    fn good_pct_is_good_bytes_over_disc_size() {
        let p = PassProgress {
            bytes_good_total: 750,
            bytes_total_disc: 1000,
            ..sample()
        };
        assert_eq!(p.good_pct(), 75.0);
    }

    /// An unknown disc size reports 100% clean, matching `bad_pct` and
    /// `pending_pct` both reporting 0% there: the triple is the coherent
    /// "nothing known to be damaged" state a client renders before the disc
    /// size is established, rather than three percentages that disagree.
    #[test]
    fn good_pct_with_unknown_disc_size_reports_clean() {
        assert_eq!(sample().good_pct(), 100.0);
    }

    #[test]
    fn good_pct_guard_fires_only_on_zero_disc_size() {
        let p = PassProgress {
            bytes_good_total: 1,
            bytes_total_disc: 2,
            ..sample()
        };
        assert_eq!(p.good_pct(), 50.0, "a sized disc must not report 100%");
    }

    #[test]
    fn bad_pct_is_unreadable_bytes_over_disc_size() {
        let p = PassProgress {
            bytes_unreadable_total: 125,
            bytes_total_disc: 1000,
            ..sample()
        };
        assert_eq!(p.bad_pct(), 12.5);
    }

    /// Unknown disc size reports 0% bad — the opposite default from `good_pct`,
    /// and deliberately so. Reporting 100% bad on an unsized disc would show a
    /// fully-damaged disc the instant a rip started.
    #[test]
    fn bad_pct_with_unknown_disc_size_reports_none() {
        assert_eq!(sample().bad_pct(), 0.0);
    }

    #[test]
    fn bad_pct_guard_fires_only_on_zero_disc_size() {
        let p = PassProgress {
            bytes_unreadable_total: 1,
            bytes_total_disc: 4,
            ..sample()
        };
        assert_eq!(p.bad_pct(), 25.0, "a sized disc must not report 0% bad");
    }

    #[test]
    fn pending_pct_is_pending_bytes_over_disc_size() {
        let p = PassProgress {
            bytes_pending_total: 400,
            bytes_total_disc: 1000,
            ..sample()
        };
        assert_eq!(p.pending_pct(), 40.0);
    }

    #[test]
    fn pending_pct_with_unknown_disc_size_reports_none() {
        assert_eq!(sample().pending_pct(), 0.0);
    }

    #[test]
    fn pending_pct_guard_fires_only_on_zero_disc_size() {
        let p = PassProgress {
            bytes_pending_total: 3,
            bytes_total_disc: 4,
            ..sample()
        };
        assert_eq!(p.pending_pct(), 75.0, "a sized disc must not report 0%");
    }

    /// The three disc-relative percentages clamp an overshoot too, not just
    /// `work_pct`. A counter can transiently exceed the disc size while a pass
    /// re-reads a region, and a client fed 137% renders past the end of its bar.
    #[test]
    fn the_disc_percentages_clamp_an_overshoot_to_a_hundred() {
        let over = |f: fn(&PassProgress) -> f64, set: fn(&mut PassProgress)| {
            let mut p = PassProgress {
                bytes_total_disc: 1000,
                ..sample()
            };
            set(&mut p);
            f(&p)
        };
        assert_eq!(
            over(PassProgress::good_pct, |p| p.bytes_good_total = 5000),
            100.0
        );
        assert_eq!(
            over(PassProgress::bad_pct, |p| p.bytes_unreadable_total = 5000),
            100.0
        );
        assert_eq!(
            over(PassProgress::pending_pct, |p| p.bytes_pending_total = 5000),
            100.0
        );
    }

    /// The three disc-relative percentages read three DIFFERENT byte counters.
    /// Nothing above would catch `bad_pct` reading `bytes_pending_total`: each
    /// test sets one counter and leaves the others zero, so a swapped field
    /// still returns the right answer for its own test. This one sets all three
    /// to distinct values at once.
    #[test]
    fn the_disc_percentages_read_distinct_counters() {
        let p = PassProgress {
            bytes_good_total: 500,
            bytes_unreadable_total: 200,
            bytes_pending_total: 300,
            bytes_total_disc: 1000,
            ..sample()
        };
        assert_eq!(p.good_pct(), 50.0, "good_pct must read bytes_good_total");
        assert_eq!(
            p.bad_pct(),
            20.0,
            "bad_pct must read bytes_unreadable_total"
        );
        assert_eq!(
            p.pending_pct(),
            30.0,
            "pending_pct must read bytes_pending_total"
        );
    }

    /// A closure IS a `Progress` via the blanket impl, and its return value is
    /// the cancellation signal: `false` means stop. A blanket body that ignored
    /// the closure and returned a constant would make every closure-based
    /// consumer uncancellable — the caller asks to stop, the rip keeps going.
    #[test]
    fn the_closure_blanket_impl_returns_the_closures_own_verdict() {
        fn ask<P: Progress>(p: &P, s: &PassProgress) -> bool {
            p.report(s)
        }

        let keep_going = |_: &PassProgress| true;
        let cancel = |_: &PassProgress| false;

        assert!(ask(&keep_going, &sample()), "true must survive the forward");
        assert!(
            !ask(&cancel, &sample()),
            "a closure returning false is a CANCEL and must not be reported as \
             keep-going; a constant-true blanket impl makes cancellation a no-op"
        );
    }

    /// The blanket impl must hand the closure the caller's sample, not a
    /// fabricated one — a consumer decides whether to cancel FROM the numbers.
    #[test]
    fn the_closure_blanket_impl_passes_the_sample_through() {
        use std::sync::{Arc, Mutex};
        fn ask<P: Progress>(p: &P, s: &PassProgress) -> bool {
            p.report(s)
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let recorder = move |p: &PassProgress| {
            sink.lock().unwrap().push((p.work_done, p.work_total));
            true
        };

        let s = PassProgress {
            work_done: 7,
            work_total: 9,
            ..sample()
        };
        assert!(ask(&recorder, &s));
        assert_eq!(*seen.lock().unwrap(), vec![(7, 9)]);
    }
}
