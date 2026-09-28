//! Stop design §5.1 "Pipeline, IO and threads": the join and grace rules (T7, T8) on a
//! scaled clock — a 600 s window runs as [`WINDOW`], the 5 s grace as [`GRACE`].
//! Stall pairs per §5.0: (a) progress at 0.5 × window for ≥ 4 windows is not a
//! timeout; (b) a stall fires within window (+ grace) + 1 s. Wall bounds keep ≥ 5× margin.

use super::*;
use std::sync::atomic::AtomicUsize;

const WINDOW: Duration = Duration::from_millis(200);
const GRACE: Duration = Duration::from_millis(200);
const SLACK: Duration = Duration::from_secs(1);

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
    progress: Progress,
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
    let progress = Progress::new();
    let in_close = Arc::new(AtomicBool::new(false));
    let sink = ScriptedClose {
        progress: progress.clone(),
        in_close: in_close.clone(),
        stall,
        step: GRACE / 2,
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
    let (pipe, in_close) = scripted(Duration::ZERO, 8);
    let halt = Halt::new();
    let h2 = halt.clone();
    let t = Instant::now();
    let caller = thread::spawn(move || pipe.finish_with_halt_timing(Some(&h2), timing()));
    wait_until(&in_close);
    halt.cancel();
    let r = caller.join().unwrap();
    assert!(
        matches!(r, Ok(8)),
        "a committed close is never abandoned: {r:?}"
    );
    assert!(
        t.elapsed() > GRACE * 2,
        "the close outlived two grace windows"
    );
}

/// LP4a (T7, stall pair a): one item applied per 0.5 × window for 4 windows is
/// progress, so the join waits and returns `Ok` — 600 s is a stall window, not a total.
#[test]
fn join_rearms_on_consumer_progress() {
    let count = Arc::new(AtomicUsize::new(0));
    let sink = SlowSinkFor {
        delay: WINDOW / 2,
        count: count.clone(),
    };
    let pipe = Pipeline::spawn(16, sink).unwrap();
    for _ in 0..8 {
        pipe.send(()).unwrap();
    }
    let t = Instant::now();
    let r = pipe.finish_with_halt_timing(None, timing());
    assert!(
        matches!(r, Ok(8)),
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
        h2.cancel();
    });
    let t = Instant::now();
    let r = pipe.send_with_halt(7, &halt, Duration::from_secs(10));
    let took = t.elapsed();
    canceller.join().unwrap();
    release.store(true, Ordering::SeqCst);
    let _ = pipe.finish();
    assert_eq!(r, Err(7), "the refused item comes back");
    assert!(took < Duration::from_millis(50) + SLACK / 2, "{took:?}");
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
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "{:?}",
        t.elapsed()
    );
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
