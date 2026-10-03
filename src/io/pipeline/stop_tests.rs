//! Stop design §5.1 "Pipeline, IO and threads": the join and grace rules (T7, T8) on a
//! scaled clock — a 600 s window runs as [`WINDOW`], the 5 s grace as [`GRACE`].
//! Stall pairs per §5.0: (a) progress at 0.2 × window for over 3 windows is not a
//! timeout; (b) a stall fires within window (+ grace) + [`SLACK`]. The windows are 1 s so
//! a 200 ms sleep overshoot between two progress bumps stays inside them.

use super::*;
use std::sync::atomic::AtomicUsize;

const WINDOW: Duration = Duration::from_secs(1);
const GRACE: Duration = Duration::from_secs(1);
const SLACK: Duration = Duration::from_secs(2);
// One progress bump per STEP while a close runs.
const STEP: Duration = Duration::from_millis(200);

struct Sum(u64);

impl Sink<u64> for Sum {
    type Output = u64;
    fn apply(&mut self, v: u64) -> Result<Flow, Error> {
        self.0 += v;
        Ok(Flow::Continue)
    }
    fn close(self) -> Result<u64, Error> {
        Ok(self.0)
    }
}

fn timing() -> JoinTiming {
    JoinTiming {
        join_window: WINDOW,
        grace: GRACE,
    }
}

// Blocks in `apply` until `release`; flags `started` so a test can sync on it.
struct Wedge {
    release: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
}

impl Sink<u64> for Wedge {
    type Output = ();
    fn apply(&mut self, _: u64) -> Result<Flow, Error> {
        self.started.store(true, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(5));
        }
        Ok(Flow::Continue)
    }
    fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

fn wedged() -> (Pipeline<u64, ()>, Arc<AtomicBool>) {
    let release = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        Wedge {
            release: release.clone(),
            started: started.clone(),
        },
    )
    .unwrap();
    pipe.send(0).unwrap();
    wait_until(&started);
    (pipe, release)
}

fn wait_until(flag: &AtomicBool) {
    let t = Instant::now();
    while !flag.load(Ordering::SeqCst) {
        assert!(t.elapsed() < SLACK * 5, "the consumer never got there");
        thread::sleep(Duration::from_millis(1));
    }
}

// `close()` runs a script: sleep `stall` without progress, then `steps` sleeps of
// `step`, bumping the shared progress after each. `in_close` flags the entry.
struct ScriptedClose {
    progress: Liveness,
    in_close: Arc<AtomicBool>,
    stall: Duration,
    step: Duration,
    steps: u32,
}

impl Sink<u64> for ScriptedClose {
    type Output = u32;
    fn apply(&mut self, _: u64) -> Result<Flow, Error> {
        Ok(Flow::Continue)
    }
    fn close(self) -> Result<u32, Error> {
        self.in_close.store(true, Ordering::SeqCst);
        thread::sleep(self.stall);
        for _ in 0..self.steps {
            thread::sleep(self.step);
            self.progress.bump();
        }
        Ok(self.steps)
    }
}

fn scripted(stall: Duration, steps: u32) -> (Pipeline<u64, u32>, Arc<AtomicBool>) {
    let progress = Liveness::new();
    let in_close = Arc::new(AtomicBool::new(false));
    let sink = ScriptedClose {
        progress: progress.clone(),
        in_close: in_close.clone(),
        stall,
        step: STEP,
        steps,
    };
    let pipe = Pipeline::spawn_named_with_progress("t-closing", 4, sink, progress).unwrap();
    (pipe, in_close)
}

/// LP1 (pipeline half; the `*.partial` output rule is ST-L3's LP3/LP1 half): a
/// halted RUNNING consumer is abandoned after the RUNNING grace, `Halted` ≤ grace + 1 s.
#[test]
fn halted_running_consumer_abandoned_after_grace() {
    let (pipe, release) = wedged();
    let halt = Halt::new();
    halt.cancel();
    let t = Instant::now();
    let r = pipe.finish_with_halt_timing(Some(&halt), timing());
    let took = t.elapsed();
    release.store(true, Ordering::SeqCst);
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(
        took >= GRACE,
        "abandoned before the grace ran out: {took:?}"
    );
    assert!(took <= GRACE + SLACK, "grace overran: {took:?}");
}

/// LP2 (T8, D2): CLOSING entered before the cancel is Done. The caller waits for
/// `close()` past the 5 s (scaled) grace while it bumps the consumer's progress.
#[test]
fn closing_before_cancel_is_done() {
    let (pipe, in_close) = scripted(Duration::ZERO, 12);
    let halt = Halt::new();
    let h2 = halt.clone();
    let t = Instant::now();
    let caller = thread::spawn(move || pipe.finish_with_halt_timing(Some(&h2), timing()));
    wait_until(&in_close);
    halt.cancel();
    let r = caller.join().unwrap();
    assert!(
        matches!(r, Ok(12)),
        "a committed close is never abandoned: {r:?}"
    );
    assert!(
        t.elapsed() > GRACE * 2,
        "the close outlived two grace windows"
    );
}

/// LP4a (T7, stall pair a): one item applied per 0.2 × window for over 3 windows is
/// progress, so the join waits and returns `Ok` — 600 s is a stall window, not a total.
#[test]
fn join_rearms_on_consumer_progress() {
    let count = Arc::new(AtomicUsize::new(0));
    let sink = SlowSinkFor {
        delay: WINDOW / 5,
        count: count.clone(),
    };
    let pipe = Pipeline::spawn(16, sink).unwrap();
    for _ in 0..16 {
        pipe.send(()).unwrap();
    }
    let t = Instant::now();
    let r = pipe.finish_with_halt_timing(None, timing());
    assert!(
        matches!(r, Ok(16)),
        "a progressing consumer is not a stall: {r:?}"
    );
    assert!(
        t.elapsed() > WINDOW * 3,
        "the join outlived several windows"
    );
}

struct SlowSinkFor {
    delay: Duration,
    count: Arc<AtomicUsize>,
}

impl Sink<()> for SlowSinkFor {
    type Output = usize;
    fn apply(&mut self, _: ()) -> Result<Flow, Error> {
        thread::sleep(self.delay);
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(Flow::Continue)
    }
    fn close(self) -> Result<usize, Error> {
        Ok(self.count.load(Ordering::SeqCst))
    }
}

/// LP4b (T7, stall pair b): a frozen consumer fails `PipelineJoinTimeout` (E9014)
/// within window + grace + 1 s, and not before the window.
#[test]
fn join_times_out_after_stall() {
    let (pipe, release) = wedged();
    let t = Instant::now();
    let r = pipe.finish_with_halt_timing(None, timing());
    let took = t.elapsed();
    release.store(true, Ordering::SeqCst);
    assert!(matches!(r, Err(Error::PipelineJoinTimeout)), "{r:?}");
    assert!(took >= WINDOW, "fired before the window: {took:?}");
    assert!(took <= WINDOW + GRACE + SLACK, "fired late: {took:?}");
}

/// LP5a (T8, non-halted): after a join stall the CLOSING window re-arms on each
/// progress bump, so a slow but progressing close returns its output.
#[test]
fn non_halted_closing_rearms() {
    let (pipe, _in_close) = scripted(WINDOW * 3 / 2, 8);
    let r = pipe.finish_with_halt_timing(None, timing());
    assert!(
        matches!(r, Ok(8)),
        "a progressing close is waited for: {r:?}"
    );
}

// `close()` blocks until released: a frozen finalise.
struct FrozenClose {
    in_close: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}

impl Sink<u64> for FrozenClose {
    type Output = ();
    fn apply(&mut self, _: u64) -> Result<Flow, Error> {
        Ok(Flow::Continue)
    }
    fn close(self) -> Result<(), Error> {
        self.in_close.store(true, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }
}

/// LP5b (T8, non-halted, stall pair b): a frozen close is leaked with E9014 after
/// the join window and one CLOSING grace with no progress.
#[test]
fn non_halted_closing_stalled_leaks() {
    let (in_close, release) = (Arc::default(), Arc::<AtomicBool>::default());
    let sink = FrozenClose {
        in_close: Arc::clone(&in_close),
        release: release.clone(),
    };
    let pipe = Pipeline::spawn(4, sink).unwrap();
    let t = Instant::now();
    let r = pipe.finish_with_halt_timing(None, timing());
    let took = t.elapsed();
    release.store(true, Ordering::SeqCst);
    assert!(in_close.load(Ordering::SeqCst));
    assert!(matches!(r, Err(Error::PipelineJoinTimeout)), "{r:?}");
    assert!(took <= WINDOW + GRACE * 2 + SLACK, "fired late: {took:?}");
}

/// LP6 / G8: `send_with_halt` on a full channel returns the item on a cancel,
/// within a slice. Guard: per §5.9 G8, do not change without a design change.
#[test]
fn send_with_halt_full_channel_cancel() {
    let release = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let sink = Wedge {
        release: release.clone(),
        started: started.clone(),
    };
    let pipe = Pipeline::spawn(1, sink).unwrap();
    pipe.send(0).unwrap();
    wait_until(&started);
    pipe.send(1).unwrap();
    let halt = Halt::new();
    let h2 = halt.clone();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let at = Instant::now();
        h2.cancel();
        at
    });
    let r = pipe.send_with_halt(7, &halt, Duration::from_secs(10));
    let returned = Instant::now();
    let cancelled_at = canceller.join().unwrap();
    let took = returned.saturating_duration_since(cancelled_at);
    release.store(true, Ordering::SeqCst);
    let _ = pipe.finish();
    assert_eq!(r, Err(7), "the refused item comes back");
    assert!(took < SLACK / 2, "{took:?}");
}

/// G7: a non-halted pipeline that finishes normally returns `Ok` with its output
/// and adds no wait on the happy path. Guard (§5.9 G7).
#[test]
fn non_halted_happy_path_returns_at_once() {
    let pipe = Pipeline::spawn(4, Sum(0)).unwrap();
    for i in 1..=4u64 {
        pipe.send(i).unwrap();
    }
    let t = Instant::now();
    let r = pipe.finish_with_halt(None);
    assert!(matches!(r, Ok(10)), "{r:?}");
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
}

/// The consumer bumps its progress per item applied and at `close()` entry and
/// exit (§2.5), so `progress()` has moved by at least items + 2 after the join.
#[test]
fn consumer_bumps_progress_per_item_and_close() {
    let pipe = Pipeline::spawn(4, Sum(0)).unwrap();
    let progress = pipe.progress().clone();
    for i in 0..3u64 {
        pipe.send(i).unwrap();
    }
    pipe.finish().unwrap();
    assert!(progress.get() >= 5, "{}", progress.get());
}

// ── The ST-L3 `.partial` rule (§2.5, §2.6) ──

// Writes each item to `<out>.partial`; `close` renames it to `<out>` (the final name),
// `close_stopped` keeps it. Each close bumps `progress` per `step` for `steps` steps.
struct PartialFile {
    file: std::fs::File,
    partial: std::path::PathBuf,
    out: std::path::PathBuf,
    progress: Liveness,
    step: Duration,
    steps: u32,
    done: Arc<AtomicBool>,
}

impl PartialFile {
    fn slow_close(&mut self) {
        use std::io::Write;
        for _ in 0..self.steps {
            thread::sleep(self.step);
            self.progress.bump();
        }
        self.file.flush().unwrap();
    }
}

impl Sink<u64> for PartialFile {
    type Output = ();
    fn apply(&mut self, v: u64) -> Result<Flow, Error> {
        use std::io::Write;
        self.file.write_all(&v.to_le_bytes()).unwrap();
        Ok(Flow::Continue)
    }
    fn close(mut self) -> Result<(), Error> {
        self.slow_close();
        std::fs::rename(&self.partial, &self.out).unwrap();
        self.done.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn close_stopped(mut self) -> Result<(), Error> {
        self.slow_close();
        self.done.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct PartialRun {
    _dir: tempfile::TempDir,
    partial: std::path::PathBuf,
    out: std::path::PathBuf,
    done: Arc<AtomicBool>,
    pipe: Pipeline<u64, ()>,
}

// A pipeline over a `PartialFile` sink whose closes take `steps` × STEP.
fn partial_run(steps: u32) -> PartialRun {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("Movie.mkv");
    let partial = dir.path().join("Movie.mkv.partial");
    let progress = Liveness::new();
    let done = Arc::new(AtomicBool::new(false));
    let sink = PartialFile {
        file: std::fs::File::create(&partial).unwrap(),
        partial: partial.clone(),
        out: out.clone(),
        progress: progress.clone(),
        step: STEP,
        steps,
        done: done.clone(),
    };
    let pipe = Pipeline::spawn_named_with_progress("t-partial", 4, sink, progress).unwrap();
    for i in 0..4 {
        pipe.send(i).unwrap();
    }
    PartialRun {
        _dir: dir,
        partial,
        out,
        done,
        pipe,
    }
}

/// LP3 (the ST-L3 rule, §2.5): "A consumer that entered CLOSING **after** the cancel
/// keeps its output under `*.partial`". It runs `close_stopped`: it finishes writing and
/// never renames (§2.6 "A post-cancel `close()` leaves `*.partial`").
#[test]
fn closing_after_cancel_keeps_partial() {
    let run = partial_run(0);
    let halt = Halt::new();
    halt.cancel();
    let r = run.pipe.finish_with_halt_timing(Some(&halt), timing());
    assert!(r.is_ok(), "the stopped close's own result: {r:?}");
    assert!(run.done.load(Ordering::SeqCst), "the stopped close ran");
    assert!(!run.out.exists(), "never a final-named file after a Stop");
    let kept = std::fs::metadata(&run.partial).expect("`.partial` kept");
    assert_eq!(kept.len(), 32, "every applied item is in the `.partial`");
}

/// LP3 with T8 (D2: "5 s for a CLOSING one"): a post-cancel close gets one fixed grace,
/// not the re-arming wait a pre-cancel close gets (LP2). The leaked consumer still
/// cannot produce a final-named file.
#[test]
fn closing_after_cancel_gets_one_grace_and_never_renames() {
    // The close outlasts both graces (join, then the one CLOSING grace) by a full second.
    let run = partial_run(20);
    let halt = Halt::new();
    halt.cancel();
    let t = Instant::now();
    let r = run.pipe.finish_with_halt_timing(Some(&halt), timing());
    let took = t.elapsed();
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(took <= GRACE * 2 + SLACK, "no re-arming wait: {took:?}");
    let end = Instant::now();
    while !run.done.load(Ordering::SeqCst) {
        assert!(end.elapsed() < SLACK * 5, "the leaked close never ended");
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!run.out.exists(), "the leaked consumer kept `*.partial`");
    assert!(run.partial.exists());
}

/// LP3 guard (GUARD): the default `close_stopped` is `close`, so a sink that never
/// renames (the engine's sweep keeps its summary after a Stop) returns its output
/// unchanged. Per spec; do not change without a spec citation proving otherwise.
#[test]
fn closing_after_cancel_default_sink_keeps_its_output() {
    let pipe = Pipeline::spawn(4, Sum(0)).unwrap();
    pipe.send(7).unwrap();
    let halt = Halt::new();
    halt.cancel();
    let r = pipe.finish_with_halt_timing(Some(&halt), timing());
    assert!(matches!(r, Ok(7)), "{r:?}");
}

/// LP1 (the `*.partial` half, GUARD): a halted RUNNING consumer is abandoned after the
/// grace and never runs `close()`, so its output stays `*.partial`.
#[test]
fn halted_running_consumer_output_stays_partial() {
    let dir = tempfile::tempdir().unwrap();
    let partial = dir.path().join("Movie.iso.partial");
    let out = dir.path().join("Movie.iso");
    struct Wedged {
        inner: PartialFile,
        release: Arc<AtomicBool>,
        started: Arc<AtomicBool>,
    }
    impl Sink<u64> for Wedged {
        type Output = ();
        fn apply(&mut self, v: u64) -> Result<Flow, Error> {
            self.started.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            self.inner.apply(v)
        }
        fn close(self) -> Result<(), Error> {
            self.inner.close()
        }
    }
    let (release, started) = (Arc::new(AtomicBool::new(false)), Arc::default());
    let sink = Wedged {
        inner: PartialFile {
            file: std::fs::File::create(&partial).unwrap(),
            partial: partial.clone(),
            out: out.clone(),
            progress: Liveness::new(),
            step: Duration::ZERO,
            steps: 0,
            done: Arc::default(),
        },
        release: release.clone(),
        started: Arc::clone(&started),
    };
    let pipe = Pipeline::spawn(4, sink).unwrap();
    pipe.send(1).unwrap();
    wait_until(&started);
    let halt = Halt::new();
    halt.cancel();
    let r = pipe.finish_with_halt_timing(Some(&halt), timing());
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    release.store(true, Ordering::SeqCst);
    thread::sleep(GRACE);
    assert!(!out.exists(), "an abandoned consumer never finalises");
    assert!(partial.exists());
}
