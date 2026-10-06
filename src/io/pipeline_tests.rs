use super::*;

// An overflowing grace (`Duration::MAX`) must not panic `Instant + Duration`.
#[test]
fn finish_with_grace_accepts_duration_max() {
    let handle = thread::spawn(|| Ok::<u32, Error>(3));
    let state = Arc::new(AtomicU8::new(state::RUNNING));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finish_with_grace(
            handle,
            &state,
            &Liveness::new(),
            Duration::MAX,
            Error::Halted,
            &mut || {},
        )
    }));
    assert!(matches!(r.expect("must not panic"), Ok(3)));
}
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Sums u64s; returns the total from `close`.
struct SumSink {
    total: u64,
}

impl Sink<u64> for SumSink {
    type Output = u64;

    fn apply(&mut self, item: u64) -> Result<Flow, Error> {
        self.total += item;
        Ok(Flow::Continue)
    }

    fn close(self) -> Result<u64, Error> {
        Ok(self.total)
    }
}

#[test]
fn happy_path_sums_items() {
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, SumSink { total: 0 })
        .expect("spawn should succeed");
    let mut expected = 0u64;
    for i in 0..100u64 {
        expected += i;
        pipe.send(i).expect("send should succeed");
    }
    let total = pipe.finish().expect("finish should succeed");
    assert_eq!(total, expected);
    assert_eq!(total, (0..100u64).sum::<u64>());
}

/// Sleeps `delay` per apply; counts how many it received.
struct SlowSink {
    delay: Duration,
    count: Arc<AtomicUsize>,
}

impl Sink<()> for SlowSink {
    type Output = usize;

    fn apply(&mut self, _item: ()) -> Result<Flow, Error> {
        std::thread::sleep(self.delay);
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(Flow::Continue)
    }

    fn close(self) -> Result<usize, Error> {
        Ok(self.count.load(Ordering::SeqCst))
    }
}

#[test]
fn back_pressure_blocks_sender() {
    // depth=2 + 5 sends + 50ms/apply: producer buffers 3 items (2 channel cap +
    // 1 in flight) before sends 4 and 5 must block, giving a ~100ms wall-clock
    // floor. Assert 80ms to tolerate CI jitter while still proving blocking.
    let count = Arc::new(AtomicUsize::new(0));
    let sink = SlowSink {
        delay: Duration::from_millis(50),
        count: count.clone(),
    };
    let pipe = Pipeline::spawn(2, sink).expect("spawn should succeed");

    let start = Instant::now();
    for _ in 0..5 {
        pipe.send(()).expect("send should succeed");
    }
    let elapsed_send = start.elapsed();

    let total = pipe.finish().expect("finish should succeed");
    assert_eq!(total, 5);
    assert!(
        elapsed_send >= Duration::from_millis(80),
        "back-pressure not observed: 5 sends with depth=2 and 50ms/apply \
             took {elapsed_send:?}, expected ≥ ~100ms (one or more sends \
             should have blocked behind the consumer)"
    );
}

/// Returns `Err` on the Nth apply (1-indexed). Tracks all calls.
struct FailOnNthSink {
    n: usize,
    seen: Arc<AtomicUsize>,
    close_called: Arc<AtomicUsize>,
}

impl Sink<u64> for FailOnNthSink {
    type Output = ();

    fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
        let i = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if i == self.n {
            Err(Error::DecryptFailed)
        } else {
            Ok(Flow::Continue)
        }
    }

    fn close(self) -> Result<(), Error> {
        self.close_called.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn apply_error_drains_then_propagates() {
    let seen = Arc::new(AtomicUsize::new(0));
    let close_called = Arc::new(AtomicUsize::new(0));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        FailOnNthSink {
            n: 3,
            seen: seen.clone(),
            close_called: close_called.clone(),
        },
    )
    .expect("spawn should succeed");

    // Send 10 items. Subsequent sends after the 3rd error must
    // still succeed (the consumer is draining).
    for i in 0..10u64 {
        pipe.send(i).expect("send should succeed even after error");
    }

    let res = pipe.finish();
    assert!(matches!(res, Err(Error::DecryptFailed)));
    assert_eq!(
        close_called.load(Ordering::SeqCst),
        0,
        "close() must not be called when apply returned Err"
    );
    // The consumer kept calling `recv` to drain after the error;
    // it just stopped invoking `apply`. So `seen` is exactly 3
    // (apply was called for items 1, 2, 3).
    assert_eq!(seen.load(Ordering::SeqCst), 3);
}

/// Returns `Flow::Stop` on the Nth apply.
struct StopOnNthSink {
    n: usize,
    seen: Arc<AtomicUsize>,
    close_called: Arc<AtomicUsize>,
}

impl Sink<u64> for StopOnNthSink {
    type Output = usize;

    fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
        let i = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if i >= self.n {
            Ok(Flow::Stop)
        } else {
            Ok(Flow::Continue)
        }
    }

    fn close(self) -> Result<usize, Error> {
        self.close_called.fetch_add(1, Ordering::SeqCst);
        Ok(self.seen.load(Ordering::SeqCst))
    }
}

#[test]
fn apply_stop_calls_close_and_returns_output() {
    let seen = Arc::new(AtomicUsize::new(0));
    let close_called = Arc::new(AtomicUsize::new(0));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        StopOnNthSink {
            n: 3,
            seen: seen.clone(),
            close_called: close_called.clone(),
        },
    )
    .expect("spawn should succeed");

    // Send 10 items. After Stop, subsequent sends may either succeed (already
    // buffered) or fail with Err(I) (channel closed); both are valid, so we
    // don't assert on the send results.
    for i in 0..10u64 {
        let _ = pipe.send(i);
    }

    let out = pipe.finish().expect("finish should succeed after Stop");
    assert_eq!(close_called.load(Ordering::SeqCst), 1);
    // At least 3 items processed (the one that returned Stop).
    assert!(
        out >= 3,
        "expected ≥ 3 applies before Stop took effect, got {out}"
    );
}

/// Panics on the first apply call.
struct PanickingSink;

impl Sink<u64> for PanickingSink {
    type Output = ();

    fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
        // resume_unwind skips the process-global panic hook, so no test has to
        // swap the hook (a race between parallel tests) to keep output quiet.
        std::panic::resume_unwind(Box::new("synthetic test panic"));
    }

    fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn consumer_panic_becomes_io_error() {
    let pipe =
        Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, PanickingSink).expect("spawn should succeed");
    // First send may succeed (item buffered before panic) or fail
    // (channel closed after panic) — either is fine.
    let _ = pipe.send(1);
    // Drain a few more sends; once the channel is closed they'll
    // return Err(I), which we just discard.
    for i in 0..5u64 {
        let _ = pipe.send(i);
    }
    let res = pipe.finish();

    // A consumer panic surfaces as the numeric variant, not an
    // English-carrying io::Error. The original panic payload is
    // logged at the join site, not embedded in the error value.
    assert!(
        matches!(res, Err(Error::PipelineConsumerPanicked)),
        "expected Err(PipelineConsumerPanicked), got {res:?}"
    );
}

// Never-completing sink: `apply` blocks until cancelled, signalling
// `started` so tests can sync on the consumer being wedged. Drives the
// halt/timeout paths of send_with_halt / finish_with_halt.
struct NeverDrainsSink {
    cancel: Arc<std::sync::atomic::AtomicBool>,
    started: Arc<std::sync::atomic::AtomicBool>,
}

impl Sink<u64> for NeverDrainsSink {
    type Output = ();

    fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
        self.started.store(true, Ordering::SeqCst);
        while !self.cancel.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(Flow::Continue)
    }

    fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

/// Spin until `started` flips or `bail` elapses. Used by the
/// send_with_halt tests to synchronise with the consumer thread
/// before exercising the bounded-send timeout path.
fn wait_for_started(started: &Arc<std::sync::atomic::AtomicBool>, bail: Duration) {
    let end = Instant::now() + bail;
    while !started.load(Ordering::SeqCst) {
        assert!(Instant::now() < end, "consumer never started apply()");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn send_with_halt_returns_item_on_deadline() {
    // depth=1 + wedged consumer + a loaded buffer slot means further `try_send`
    // sees Full, so send_with_halt must return `Err(item)` around the 200ms
    // deadline. Sync on `started` first so the consumer is wedged before we load the slot.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        1,
        NeverDrainsSink {
            cancel: cancel.clone(),
            started: started.clone(),
        },
    )
    .expect("spawn should succeed");
    // First send: consumer recv()s it and wedges in apply.
    pipe.send(0u64).expect("first send hands off to consumer");
    wait_for_started(&started, Duration::from_secs(2));
    // Second send: lands in the depth=1 buffer slot, consumer
    // can't pick it up because it's wedged in apply. Channel now
    // full from the producer's perspective.
    pipe.send(1u64).expect("second send fills the buffer");

    let halt = crate::halt::Halt::new();
    let start = Instant::now();
    let res = pipe.send_with_halt(99u64, &halt, Duration::from_millis(200));
    let elapsed = start.elapsed();

    // Release the leaked consumer so the test process winds down.
    cancel.store(true, Ordering::SeqCst);
    let _ = pipe.finish();

    assert!(matches!(res, Err(99)), "expected item returned on deadline");
    assert!(
        elapsed >= Duration::from_millis(150),
        "deadline returned too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "deadline blew past tolerance: {elapsed:?}"
    );
}

#[test]
fn send_with_halt_returns_item_on_halt() {
    // Same setup, but the halt fires before the deadline elapses.
    // The send loop must observe the halt within ~250 ms (the
    // SEND_HALT_CHECK_INTERVAL) and return the item.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        1,
        NeverDrainsSink {
            cancel: cancel.clone(),
            started: started.clone(),
        },
    )
    .expect("spawn should succeed");
    pipe.send(0u64).expect("first send hands off to consumer");
    wait_for_started(&started, Duration::from_secs(2));
    pipe.send(1u64).expect("second send fills the buffer");

    let halt = crate::halt::Halt::new();
    let halt2 = halt.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        halt2.cancel();
    });

    let start = Instant::now();
    let res = pipe.send_with_halt(7u64, &halt, Duration::from_secs(10));
    let elapsed = start.elapsed();

    cancel.store(true, Ordering::SeqCst);
    let _ = pipe.finish();

    assert!(matches!(res, Err(7)), "expected item returned on halt");
    assert!(
        elapsed < Duration::from_secs(2),
        "halt observation took too long: {elapsed:?}"
    );
}

// `Duration::MAX` (a natural "no deadline") overflowed `Instant + Duration`
// and panicked; it must mean unbounded, still halt-aware.
#[test]
fn send_with_halt_accepts_duration_max_as_unbounded() {
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        1,
        NeverDrainsSink {
            cancel: cancel.clone(),
            started: started.clone(),
        },
    )
    .expect("spawn should succeed");
    pipe.send(0u64).expect("first send hands off to consumer");
    wait_for_started(&started, Duration::from_secs(2));
    pipe.send(1u64).expect("second send fills the buffer");

    let halt = crate::halt::Halt::new();
    let halt2 = halt.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        halt2.cancel();
    });
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pipe.send_with_halt(7u64, &halt, Duration::MAX)
    }));

    cancel.store(true, Ordering::SeqCst);
    let _ = pipe.finish();
    let res = res.expect("Duration::MAX must not panic");
    assert!(matches!(res, Err(7)), "expected item returned on halt");
}

#[test]
fn finish_with_halt_returns_halted_when_consumer_wedged() {
    // Consumer wedges on the first apply; halt fires; finish
    // returns Error::Halted rather than blocking forever.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        NeverDrainsSink {
            cancel: cancel.clone(),
            started: started.clone(),
        },
    )
    .expect("spawn should succeed");
    pipe.send(0u64).expect("seed item the consumer wedges on");
    wait_for_started(&started, Duration::from_secs(2));

    let halt = crate::halt::Halt::new();
    let halt2 = halt.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(400));
        halt2.cancel();
    });

    let start = Instant::now();
    let res = pipe.finish_with_halt(Some(&halt));
    let elapsed = start.elapsed();

    // Release the leaked consumer so the test process exits cleanly.
    cancel.store(true, Ordering::SeqCst);

    assert!(
        matches!(res, Err(Error::Halted)),
        "expected Err(Halted), got {res:?}"
    );
    // Bailed within the grace period plus margin: grace spin-poll adds up to
    // FINISH_GRACE_SECS (5s) for the deliberately-unreleased, wedged consumer.
    // 15s stays well under the 10-minute JOIN_TIMEOUT, proving it doesn't block forever.
    assert!(
        elapsed < Duration::from_secs(15),
        "halt observation took too long: {elapsed:?}"
    );
}

#[test]
fn finish_with_halt_happy_path_returns_output() {
    // No halt token, sink completes normally — finish_with_halt
    // must return the same Output that `finish` would.
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, SumSink { total: 0 })
        .expect("spawn should succeed");
    for i in 0..10u64 {
        pipe.send(i).expect("send should succeed");
    }
    let total = pipe
        .finish_with_halt(None)
        .expect("happy-path finish_with_halt should succeed");
    assert_eq!(total, (0..10u64).sum::<u64>());
}

// ── Added hardening tests ───────────────────────────────────────

// Zero items sent must still call close() exactly once.
#[test]
fn empty_pipeline_still_calls_close() {
    let close_called = Arc::new(AtomicUsize::new(0));
    struct CountClose(Arc<AtomicUsize>);
    impl Sink<u64> for CountClose {
        type Output = ();
        fn apply(&mut self, _: u64) -> Result<Flow, Error> {
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let pipe =
        Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, CountClose(close_called.clone())).expect("spawn");
    pipe.finish().expect("finish on empty pipeline");
    assert_eq!(close_called.load(Ordering::SeqCst), 1);
}

// close() returning Err must surface from finish(), not be swallowed.
#[test]
fn close_error_propagates_from_finish() {
    struct CloseFails;
    impl Sink<u64> for CloseFails {
        type Output = ();
        fn apply(&mut self, _: u64) -> Result<Flow, Error> {
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<(), Error> {
            Err(Error::DecryptFailed)
        }
    }
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, CloseFails).expect("spawn");
    pipe.send(1).expect("send");
    let res = pipe.finish();
    assert!(matches!(res, Err(Error::DecryptFailed)));
}

// try_send must report Full when saturated and the consumer is wedged, NOT block.
#[test]
fn try_send_reports_full_when_saturated() {
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        1,
        NeverDrainsSink {
            cancel: cancel.clone(),
            started: started.clone(),
        },
    )
    .expect("spawn");
    pipe.send(0u64).expect("first send hands off to consumer");
    wait_for_started(&started, Duration::from_secs(2));
    pipe.send(1u64)
        .expect("second send fills the depth-1 buffer");
    // Channel is now full and the consumer is wedged.
    let r = pipe.try_send(2u64);
    assert!(
        matches!(r, Err(TrySendError::Full(2))),
        "expected Full(2), got {r:?}"
    );
    cancel.store(true, Ordering::SeqCst);
    let _ = pipe.finish();
}

// try_send must report Disconnected once the consumer has exited (via panic here).
#[test]
fn try_send_reports_disconnected_after_consumer_gone() {
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, PanickingSink).expect("spawn");
    // Drive the consumer to panic and fully exit. Spin until a
    // try_send observes the closed channel.
    let end = Instant::now() + Duration::from_secs(2);
    let mut saw_disconnect = false;
    let mut last = None;
    while Instant::now() < end {
        match pipe.try_send(1u64) {
            Err(TrySendError::Disconnected(_)) => {
                saw_disconnect = true;
                break;
            }
            other => last = Some(format!("{other:?}")),
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = pipe.finish();
    assert!(
        saw_disconnect,
        "try_send never reported Disconnected; last was {last:?}"
    );
}

// Plain send must hand the item back via Err(item) once the consumer has panicked.
#[test]
fn send_returns_item_after_consumer_panicked() {
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, PanickingSink).expect("spawn");
    let end = Instant::now() + Duration::from_secs(2);
    let mut returned = None;
    while Instant::now() < end {
        // Use a distinctive sentinel so we can prove identity.
        if let Err(item) = pipe.send(0xDEAD_BEEF_u64) {
            returned = Some(item);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = pipe.finish();
    assert_eq!(
        returned,
        Some(0xDEAD_BEEF_u64),
        "send did not hand back the exact item after consumer death"
    );
}

// send_with_halt must return the exact item via Err(item) on the Disconnected arm.
#[test]
fn send_with_halt_returns_item_on_disconnect() {
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, PanickingSink).expect("spawn");
    // Force the consumer to panic + exit: send until the channel
    // closes (plain send returns Err).
    let end = Instant::now() + Duration::from_secs(2);
    while Instant::now() < end {
        if pipe.send(1u64).is_err() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let halt = crate::halt::Halt::new(); // never cancelled
    let res = pipe.send_with_halt(0xABCD_u64, &halt, Duration::from_secs(5));
    let _ = pipe.finish();
    assert!(
        matches!(res, Err(0xABCD)),
        "expected disconnected item returned, got {res:?}"
    );
    assert!(!halt.is_cancelled(), "halt must not have been the cause");
}

// A pre-cancelled halt must return the item immediately without
// attempting to enqueue (pins the is_cancelled() pre-check).
#[test]
fn send_with_halt_precancelled_returns_item_without_send() {
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, SumSink { total: 0 }).expect("spawn");
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let res = pipe.send_with_halt(77u64, &halt, Duration::from_secs(5));
    assert!(
        matches!(res, Err(77)),
        "pre-cancelled halt must return item"
    );
    // The item must NOT have been enqueued: finishing yields sum 0.
    let total = pipe.finish().expect("finish");
    assert_eq!(total, 0, "item was enqueued despite pre-cancelled halt");
}

// finish_with_halt(None) + wedged consumer must NOT spuriously return Halted (no halt
// supplied to observe).
#[test]
fn finish_with_halt_none_does_not_spuriously_halt() {
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        NeverDrainsSink {
            cancel: cancel.clone(),
            started: started.clone(),
        },
    )
    .expect("spawn");
    pipe.send(0u64).expect("seed");
    wait_for_started(&started, Duration::from_secs(2));

    // Run finish_with_halt(None) on a helper thread; it should be
    // blocked (not returning Halted) while the consumer is wedged.
    let cancel2 = cancel.clone();
    let (tx, rx) = bounded::<Result<(), Error>>(1);
    std::thread::spawn(move || {
        let r = pipe.finish_with_halt(None);
        let _ = tx.send(r);
    });
    // It must NOT complete within 600 ms (consumer still wedged).
    assert!(
        rx.recv_timeout(Duration::from_millis(600)).is_err(),
        "finish_with_halt(None) returned while consumer was wedged"
    );
    // Release the consumer; finish_with_halt should now return Ok.
    cancel2.store(true, Ordering::SeqCst);
    let final_res = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("finish_with_halt should return after consumer unwedges");
    assert!(
        final_res.is_ok(),
        "expected Ok after release, got {final_res:?}"
    );
}

// Once a sink returns Stop, the consumer must stop calling apply for
// all subsequent items and call close() exactly once.
#[test]
fn stop_halts_further_apply_calls() {
    let seen = Arc::new(AtomicUsize::new(0));
    let close_called = Arc::new(AtomicUsize::new(0));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        StopOnNthSink {
            n: 2,
            seen: seen.clone(),
            close_called: close_called.clone(),
        },
    )
    .expect("spawn");
    for i in 0..100u64 {
        let _ = pipe.send(i);
    }
    let out = pipe.finish().expect("finish after stop");
    assert_eq!(
        close_called.load(Ordering::SeqCst),
        1,
        "close must run exactly once"
    );
    // apply ran for items 1 and 2 (item 2 returned Stop); never for
    // the remaining 98 even though they were drained.
    assert_eq!(out, 2, "apply was called after Stop");
}

// ── Bug-fix regression tests ────────────────────────────────────────

// Regression: halt fires but consumer finishes WITHIN the grace period — finish_with_halt
// must join cleanly and return Ok.
#[test]
fn finish_with_halt_joins_cleanly_when_consumer_finishes_in_grace() {
    // A sink that adds a short artificial delay in `close` to
    // simulate a consumer that is "nearly done" when halt fires.
    struct SlowCloseSink {
        close_delay: Duration,
        total: u64,
    }
    impl Sink<u64> for SlowCloseSink {
        type Output = u64;
        fn apply(&mut self, item: u64) -> Result<Flow, Error> {
            self.total += item;
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<u64, Error> {
            std::thread::sleep(self.close_delay);
            Ok(self.total)
        }
    }

    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        SlowCloseSink {
            // close() sleeps 500ms — well inside the 5s grace period.
            close_delay: Duration::from_millis(500),
            total: 0,
        },
    )
    .expect("spawn");
    for i in 0..5u64 {
        pipe.send(i).expect("send");
    }

    // Fire halt immediately (before the consumer has had a chance
    // to finish its close() delay).
    let halt = crate::halt::Halt::new();
    halt.cancel();

    let start = Instant::now();
    // finish_with_halt drops tx (EOF), observes the pre-cancelled halt, and
    // enters the grace spin; the consumer finishes close() within 500ms, so
    // it must join cleanly and return Ok with the correct total.
    let res = pipe.finish_with_halt(Some(&halt));
    let elapsed = start.elapsed();

    assert!(
        matches!(res, Ok(10)),
        "expected Ok(10) from clean grace join, got {res:?}"
    );
    // Must return well before the full grace timeout (the consumer
    // finishes in ~500ms, so total elapsed should be well under 3s).
    assert!(
        elapsed < Duration::from_secs(3),
        "grace join took too long: {elapsed:?}"
    );
}

// Regression: a leaked consumer must not finalise an abandoned output — once released it
// must skip close().
#[test]
fn leaked_consumer_skips_close_after_abandonment() {
    struct WedgeThenRecord {
        release: Arc<std::sync::atomic::AtomicBool>,
        started: Arc<std::sync::atomic::AtomicBool>,
        closed: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Sink<u64> for WedgeThenRecord {
        type Output = ();
        fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
            self.started.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<(), Error> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        WedgeThenRecord {
            release: release.clone(),
            started: started.clone(),
            closed: closed.clone(),
        },
    )
    .expect("spawn");
    pipe.send(0u64)
        .expect("seed the item the consumer wedges on");
    wait_for_started(&started, Duration::from_secs(2));

    // Halt is already cancelled when finish_with_halt is called, so
    // it enters the grace spin immediately; the consumer is wedged in
    // apply for the whole grace window, so the thread is leaked.
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let res = pipe.finish_with_halt(Some(&halt));
    assert!(
        matches!(res, Err(Error::Halted)),
        "expected Err(Halted) after grace-expiry leak, got {res:?}"
    );

    // The thread is now leaked but still parked in apply. Release it
    // and give it time to drain to EOF and reach the close() decision.
    release.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && !closed.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        !closed.load(Ordering::SeqCst),
        "abandoned consumer called close() — it must skip finalisation \
             of an output the caller already reported as failed"
    );
}

// Companion to the above: the abandonment guard must NOT fire when the consumer finishes
// inside the grace window.
#[test]
fn consumer_finishing_in_grace_still_calls_close() {
    struct ReleasableClose {
        release: Arc<std::sync::atomic::AtomicBool>,
        started: Arc<std::sync::atomic::AtomicBool>,
        closed: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Sink<u64> for ReleasableClose {
        type Output = ();
        fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
            self.started.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<(), Error> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        ReleasableClose {
            release: release.clone(),
            started: started.clone(),
            closed: closed.clone(),
        },
    )
    .expect("spawn");
    pipe.send(0u64).expect("seed");
    wait_for_started(&started, Duration::from_secs(2));

    // Release the consumer almost immediately — well inside the grace
    // window — so finish_with_halt joins cleanly and close() runs.
    let release2 = release.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        release2.store(true, Ordering::SeqCst);
    });

    let halt = crate::halt::Halt::new();
    halt.cancel();
    let res = pipe.finish_with_halt(Some(&halt));
    assert!(res.is_ok(), "expected clean Ok join in grace, got {res:?}");
    assert!(
        closed.load(Ordering::SeqCst),
        "consumer that finished inside grace must have called close()"
    );
}

// Regression: finish_with_halt with no halt token must still return Ok
// on normal completion (None-halt path unchanged by the grace fix).
#[test]
fn finish_with_halt_no_halt_token_normal_completion() {
    let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, SumSink { total: 0 }).expect("spawn");
    for i in 0..20u64 {
        pipe.send(i).expect("send");
    }
    let res = pipe.finish_with_halt(None);
    assert!(matches!(res, Ok(190)), "expected Ok(190), got {res:?}");
}

// A fatal apply error must become visible to the PRODUCER, not only to finish().
#[test]
fn send_with_halt_fails_fast_once_apply_has_failed() {
    struct FailFirst {
        failed: Arc<AtomicUsize>,
    }
    impl Sink<u64> for FailFirst {
        type Output = ();
        fn apply(&mut self, _item: u64) -> Result<Flow, Error> {
            self.failed.fetch_add(1, Ordering::SeqCst);
            Err(Error::DecryptFailed)
        }
        fn close(self) -> Result<(), Error> {
            Ok(())
        }
    }
    let applied = Arc::new(AtomicUsize::new(0));
    let pipe = Pipeline::spawn(
        DEFAULT_PIPELINE_DEPTH,
        FailFirst {
            failed: applied.clone(),
        },
    )
    .expect("spawn");
    let halt = crate::halt::Halt::new();
    let deadline = Duration::from_secs(5);

    // Feed one item and wait until the consumer has actually applied (and
    // failed on) it, so the check below is deterministic rather than racy.
    pipe.send_with_halt(0u64, &halt, deadline)
        .expect("the first send lands");
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until && applied.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(applied.load(Ordering::SeqCst), 1, "apply ran and failed");

    assert!(pipe.consumer_failed(), "the failure must be observable");
    // The very next send must hand the item straight back — the producer's
    // signal to stop reading the disc.
    assert_eq!(
        pipe.send_with_halt(1u64, &halt, deadline),
        Err(1u64),
        "send_with_halt must fail fast once the consumer's apply has failed"
    );
    // The halt was never fired, so this is not a cancellation: the real error
    // still comes out of finish().
    assert!(matches!(pipe.finish(), Err(Error::DecryptFailed)));
    assert_eq!(
        applied.load(Ordering::SeqCst),
        1,
        "no further item was applied"
    );
}

// The abandon/finalise race: a consumer already committed to close() when grace expires
// must be waited for, not abandoned.
#[test]
fn abandon_loses_to_a_close_already_committed() {
    let state = Arc::new(AtomicU8::new(state::RUNNING));
    let release = Arc::new(AtomicBool::new(false));
    let in_close = Arc::new(AtomicBool::new(false));

    let (st, rel, inc) = (state.clone(), release.clone(), in_close.clone());
    let handle = thread::Builder::new()
        .name("test-consumer".into())
        .spawn(move || -> Result<u64, Error> {
            // Exactly what the consumer does before finalising: claim the
            // right to close.
            assert!(
                st.compare_exchange(
                    state::RUNNING,
                    state::CLOSING,
                    Ordering::AcqRel,
                    Ordering::Acquire
                )
                .is_ok(),
                "the consumer claims the finalise first"
            );
            inc.store(true, Ordering::SeqCst);
            // Inside `close()`, finalising the container.
            while !rel.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(42)
        })
        .expect("spawn");

    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until && !in_close.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(in_close.load(Ordering::SeqCst), "consumer reached close()");

    // The caller's own poll releases the close 0.3 grace into the second window: after
    // the abandon decision, well before the CLOSING grace ends, on no other thread.
    let grace = Duration::from_secs(1);
    let started = Instant::now();
    let rel = release.clone();
    let res = finish_with_grace(
        handle,
        &state,
        &Liveness::new(),
        grace,
        Error::Halted,
        &mut || {
            if started.elapsed() >= grace * 13 / 10 {
                rel.store(true, Ordering::SeqCst);
            }
        },
    );
    assert!(
        matches!(res, Ok(42)),
        "a finalise already in flight must be waited for, not abandoned: {res:?}"
    );
    assert_eq!(
        state.load(Ordering::SeqCst),
        state::CLOSING,
        "the caller must not have overwritten the consumer's claim"
    );
}
