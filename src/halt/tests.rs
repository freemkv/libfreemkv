use super::diag::NoBlockingScope;
use super::*;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};

// Test windows are scaled: a 60 s production window is 100 ms here (stop design §5.0).
const WINDOW: Duration = Duration::from_millis(100);
// Headroom on wall-clock upper bounds for a loaded CI runner.
const SLACK: Duration = Duration::from_millis(500);

fn catch<T>(f: impl FnOnce() -> T) -> std::thread::Result<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
}

#[test]
fn fresh_is_not_cancelled() {
    let h = Halt::new();
    assert!(!h.is_cancelled());
}

#[test]
fn cancel_flips_state_and_is_idempotent() {
    let h = Halt::new();
    h.cancel();
    h.cancel();
    assert!(h.is_cancelled());
}

#[test]
fn clone_shares_state_both_directions() {
    let a = Halt::new();
    let b = a.clone();
    b.cancel();
    assert!(a.is_cancelled());
    let c = Halt::new();
    let d = c.clone();
    c.cancel();
    assert!(d.is_cancelled());
}

#[test]
fn clone_shares_state_across_threads() {
    let h = Halt::new();
    let h2 = h.clone();
    std::thread::spawn(move || h2.cancel()).join().unwrap();
    assert!(h.is_cancelled());
}

/// G10: `from_arc`/`as_arc` are pointer-exact views over one flag (repr unchanged).
#[test]
fn from_arc_as_arc_are_pointer_exact() {
    let arc = Arc::new(AtomicBool::new(false));
    let halt = Halt::from_arc(arc.clone());
    assert!(Arc::ptr_eq(halt.as_arc(), &arc));
    halt.cancel();
    assert!(arc.load(Ordering::Acquire));
    let arc2 = Arc::new(AtomicBool::new(false));
    let halt2 = Halt::from_arc(arc2.clone());
    arc2.store(true, Ordering::Release);
    assert!(halt2.is_cancelled());
}

/// §2.1: `POLL_INTERVAL` becomes an alias of the 20 ms `WAIT_SLICE`.
#[test]
fn poll_interval_aliases_wait_slice() {
    assert_eq!(WAIT_SLICE, Duration::from_millis(20));
    assert_eq!(POLL_INTERVAL, WAIT_SLICE);
}

/// LT1: `check` → `Halted` after cancel; `wait(10 s)` returns `Halted` ≤ 1 s after a
/// cancel from another thread; an uncancelled `wait(d)` returns `Ok` after `d`.
#[test]
fn halt_check_and_wait_observe_cancel() {
    let h = Halt::new();
    assert!(h.check().is_ok());
    let t = Instant::now();
    assert!(h.wait(WINDOW).is_ok());
    assert!(t.elapsed() >= WINDOW);
    assert!(h.wait(Duration::ZERO).is_ok());

    let h2 = h.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(WINDOW);
        let at = Instant::now();
        h2.cancel();
        at
    });
    let r = h.wait(Duration::from_secs(10));
    let returned = Instant::now();
    let cancelled_at = canceller.join().unwrap();
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(returned.duration_since(cancelled_at) <= Duration::from_secs(1));
    assert!(matches!(h.check(), Err(Error::Halted)));
    assert!(matches!(h.wait(Duration::MAX), Err(Error::Halted)));
}

// Run `f` on a thread, cancel `h` `after` it starts, and return its result.
fn cancel_during<T: Send + 'static>(
    h: &Halt,
    after: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> (T, Duration) {
    let start = Arc::new(Barrier::new(2));
    let s2 = start.clone();
    let waiter = std::thread::spawn(move || {
        s2.wait();
        let r = f();
        (r, Instant::now())
    });
    start.wait();
    std::thread::sleep(after);
    let at = Instant::now();
    h.cancel();
    let (r, returned) = waiter.join().unwrap();
    (r, returned.saturating_duration_since(at))
}

/// LT2: `recv_timeout` returns `Halted` mid-wait; `send_timeout` returns mid-wait and
/// gives the item back.
#[test]
fn halt_recv_send_timeout_return_on_cancel() {
    let (_tx, rx) = mpsc::channel::<u32>();
    let h = Halt::new();
    let h2 = h.clone();
    let (r, took) = cancel_during(&h, WINDOW, move || {
        h2.recv_timeout(&rx, Duration::from_secs(10))
    });
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(took <= Duration::from_secs(1));

    // A rendezvous channel nobody receives on: every send blocks.
    let (tx, _rx) = mpsc::sync_channel::<String>(0);
    let h = Halt::new();
    let h2 = h.clone();
    let (r, took) = cancel_during(&h, WINDOW, move || {
        h2.send_timeout(&tx, "frame".to_string(), Duration::from_secs(10))
    });
    assert_eq!(r, Err(Halted("frame".to_string())));
    assert!(
        matches!(Error::from(Halted(())), Error::Halted),
        "`?` gives Error::Halted"
    );
    assert!(took <= Duration::from_secs(1));
}

/// Uncancelled `recv_timeout`/`send_timeout`: item, budget expiry, and a gone peer.
#[test]
fn recv_send_timeout_without_cancel() {
    let h = Halt::new();
    let (tx, rx) = mpsc::channel::<u32>();
    tx.send(5).unwrap();
    assert_eq!(h.recv_timeout(&rx, WINDOW).unwrap(), Recv::Item(5));
    let t = Instant::now();
    assert_eq!(h.recv_timeout(&rx, WINDOW).unwrap(), Recv::TimedOut);
    assert!(t.elapsed() >= WINDOW && t.elapsed() < WINDOW + SLACK);
    drop(tx);
    let t = Instant::now();
    assert_eq!(
        h.recv_timeout(&rx, Duration::from_secs(10)).unwrap(),
        Recv::Disconnected
    );
    assert!(t.elapsed() < SLACK, "a gone sender ends the wait at once");

    let (stx, srx) = mpsc::sync_channel::<u32>(1);
    assert_eq!(h.send_timeout(&stx, 1, WINDOW), Ok(SendOutcome::Sent));
    let t = Instant::now();
    assert_eq!(
        h.send_timeout(&stx, 2, WINDOW),
        Ok(SendOutcome::TimedOut(2))
    );
    assert!(t.elapsed() >= WINDOW && t.elapsed() < WINDOW + SLACK);
    drop(srx);
    assert_eq!(
        h.send_timeout(&stx, 3, Duration::from_secs(10)),
        Ok(SendOutcome::Disconnected(3))
    );

    h.cancel();
    let (tx, rx) = mpsc::channel::<u32>();
    tx.send(9).unwrap();
    assert!(matches!(h.recv_timeout(&rx, WINDOW), Err(Error::Halted)));
    assert_eq!(h.send_timeout(&stx, 4, WINDOW), Err(Halted(4)));
}

#[cfg(feature = "rip")]
#[test]
fn crossbeam_channels_are_timed() {
    let h = Halt::new();
    let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
    assert_eq!(h.send_timeout(&tx, 1, WINDOW), Ok(SendOutcome::Sent));
    assert_eq!(h.send_timeout(&tx, 2, WINDOW), Ok(SendOutcome::TimedOut(2)));
    assert_eq!(h.recv_timeout(&rx, WINDOW).unwrap(), Recv::Item(1));
    assert_eq!(h.recv_timeout(&rx, WINDOW).unwrap(), Recv::TimedOut);
    assert_eq!(
        h.send_timeout(&tx, 3, Duration::ZERO),
        Ok(SendOutcome::Sent)
    );
    assert_eq!(
        h.send_timeout(&tx, 4, Duration::ZERO),
        Ok(SendOutcome::TimedOut(4))
    );
    assert_eq!(h.recv_timeout(&rx, Duration::ZERO).unwrap(), Recv::Item(3));
    assert_eq!(h.recv_timeout(&rx, Duration::ZERO).unwrap(), Recv::TimedOut);
    drop(tx);
    assert_eq!(
        h.recv_timeout(&rx, Duration::MAX).unwrap(),
        Recv::Disconnected
    );
}

/// A zero budget still makes one non-blocking attempt: a queued item is taken and
/// a send into room succeeds; a full or empty channel reports at once.
#[test]
fn zero_budget_makes_one_attempt() {
    let h = Halt::new();
    let (tx, rx) = mpsc::sync_channel::<u32>(1);
    let t = Instant::now();
    assert_eq!(
        h.send_timeout(&tx, 1, Duration::ZERO),
        Ok(SendOutcome::Sent)
    );
    assert_eq!(
        h.send_timeout(&tx, 2, Duration::ZERO),
        Ok(SendOutcome::TimedOut(2))
    );
    assert_eq!(h.recv_timeout(&rx, Duration::ZERO).unwrap(), Recv::Item(1));
    assert_eq!(h.recv_timeout(&rx, Duration::ZERO).unwrap(), Recv::TimedOut);
    assert!(t.elapsed() < SLACK);
}

/// A disconnected channel with an unbounded wait returns `Disconnected` at once, and
/// every repeat does too, so a caller looping on it exits instead of spinning.
#[test]
fn disconnected_unbounded_recv_returns_at_once() {
    let h = Halt::new();
    let (tx, rx) = mpsc::channel::<u32>();
    let worker = std::thread::spawn(move || {
        let _tx = tx;
        panic!("worker died");
    });
    assert!(worker.join().is_err());
    let t = Instant::now();
    assert_eq!(
        h.recv_timeout(&rx, Duration::MAX).unwrap(),
        Recv::Disconnected
    );
    assert_eq!(h.recv_timeout(&rx, WAIT_SLICE).unwrap(), Recv::Disconnected);
    assert!(t.elapsed() < SLACK, "{:?}", t.elapsed());
}

/// A gone receiver with an unbounded wait returns `Disconnected` at once (item back),
/// and so does every repeat, so a caller looping on it exits instead of spinning.
#[test]
fn disconnected_unbounded_send_returns_at_once() {
    let h = Halt::new();
    let (tx, rx) = mpsc::sync_channel::<u32>(0);
    let worker = std::thread::spawn(move || {
        let _rx = rx;
        panic!("consumer died");
    });
    assert!(worker.join().is_err());
    let t = Instant::now();
    let r = h.send_timeout(&tx, 7, Duration::MAX);
    assert_eq!(r, Ok(SendOutcome::Disconnected(7)));
    let r = h.send_timeout(&tx, 8, WAIT_SLICE);
    assert_eq!(r, Ok(SendOutcome::Disconnected(8)));
    assert!(t.elapsed() < SLACK, "{:?}", t.elapsed());
}

/// The `SyncSender` poll backs off to one attempt per `WAIT_SLICE`: blocked for 10
/// slices it makes ~15 attempts (1, 2, 4, 8, 16 ms, then 20 ms), not ~200 at 1 ms.
#[test]
fn sync_sender_poll_backs_off_to_a_slice() {
    let mut attempts = 0;
    let t = Instant::now();
    let r = backoff_poll(1u32, WAIT_SLICE * 10, |v| {
        attempts += 1;
        Err(SendFail::Timeout(v))
    });
    assert!(matches!(r, Err(SendFail::Timeout(1))));
    assert!(t.elapsed() >= WAIT_SLICE * 10);
    assert!((5..=25).contains(&attempts), "{attempts} attempts");
    let (tx, _rx) = mpsc::sync_channel::<u32>(0);
    assert!(matches!(
        tx.send_slice(2, WAIT_SLICE),
        Err(SendFail::Timeout(2))
    ));
}

// A thread blocked until the returned sender sends or drops.
fn parked_thread() -> (JoinHandle<u32>, mpsc::Sender<()>) {
    let (tx, rx) = mpsc::channel::<()>();
    let h = std::thread::spawn(move || {
        let _ = rx.recv();
        11
    });
    (h, tx)
}

/// LT4: `Joined::{Done, Halted, Pending}`; never blocks past `d` + 1 slice.
#[test]
fn join_within_finished_cancelled_and_expired() {
    let done = std::thread::spawn(|| 3u32);
    while !done.is_finished() {
        std::thread::yield_now();
    }
    assert!(matches!(
        join_within(done, Duration::ZERO, None),
        Joined::Done(Ok(3))
    ));

    let (h, release) = parked_thread();
    let t = Instant::now();
    let Joined::Pending(h) = join_within(h, WINDOW, None) else {
        panic!("a running thread past d is Pending");
    };
    let took = t.elapsed();
    assert!(
        took >= WINDOW && took <= WINDOW + WAIT_SLICE + SLACK,
        "{took:?}"
    );

    let halt = Halt::new();
    halt.cancel();
    let Joined::Halted(h) = join_within(h, Duration::from_secs(10), Some(&halt)) else {
        panic!("a cancelled token is Halted");
    };
    drop(release);
    assert!(matches!(
        join_within(h, Duration::from_secs(10), Some(&Halt::new())),
        Joined::Done(Ok(11))
    ));

    let panicked = std::thread::spawn(|| -> u32 { panic!("consumer died") });
    assert!(matches!(
        join_within(panicked, Duration::from_secs(10), None),
        Joined::Done(Err(_))
    ));
}

/// LT5a (HR1): slow but progressing — a bump every 0.5 × window for 4 windows —
/// never expires.
#[test]
fn stall_timer_rearms_on_progress() {
    let p = Progress::new();
    let mut t = StallTimer::new(WINDOW, &p);
    let start = Instant::now();
    let mut next_bump = start + WINDOW / 2;
    while start.elapsed() < WINDOW * 4 {
        if Instant::now() >= next_bump {
            p.bump();
            next_bump += WINDOW / 2;
        }
        assert_ne!(t.poll(&p), Stall::Expired, "at {:?}", start.elapsed());
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(p.get() >= 6);
}

/// LT5b (HR1): no progress → `StalledFor` until the window, then `Expired` within
/// window + slack; never before the window.
#[test]
fn stall_timer_expires_without_progress() {
    let p = Progress::new();
    p.bump();
    let start = Instant::now();
    let mut t = StallTimer::new(WINDOW, &p);
    let expired_at = loop {
        match t.poll(&p) {
            Stall::Expired => break start.elapsed(),
            Stall::StalledFor(d) => assert!(d < WINDOW),
            Stall::Progressing => panic!("nothing moved"),
        }
        assert!(start.elapsed() < WINDOW + SLACK, "never expired");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(expired_at >= WINDOW, "{expired_at:?}");
    p.bump();
    assert_eq!(
        t.poll(&p),
        Stall::Progressing,
        "progress re-arms after expiry"
    );
    assert!(matches!(t.poll(&p), Stall::StalledFor(_)));
}

/// LT5c (ST3-5): with a live `busy()` guard an `idle_only` timer does not expire,
/// while a plain timer on the same `Progress` expires at its window.
#[test]
fn busy_pauses_only_idle_only_timers() {
    let p = Progress::new();
    let mut plain = StallTimer::new(WINDOW, &p);
    let mut idle = StallTimer::idle_only(WINDOW, &p);
    let guard = p.busy();
    assert_eq!(p.get(), 0, "busy() never bumps");
    let start = Instant::now();
    let mut plain_expired = None;
    while start.elapsed() < WINDOW * 3 {
        if plain.poll(&p) == Stall::Expired && plain_expired.is_none() {
            plain_expired = Some(start.elapsed());
        }
        assert_ne!(
            idle.poll(&p),
            Stall::Expired,
            "idle_only expired while busy"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let at = plain_expired.expect("the plain timer ignores busy()");
    assert!(at >= WINDOW && at < WINDOW + SLACK, "{at:?}");

    // Negative control: once idle, the idle_only timer expires after a window.
    drop(guard);
    let idle_from = Instant::now();
    while idle.poll(&p) != Stall::Expired {
        assert!(
            idle_from.elapsed() < WINDOW + SLACK,
            "idle_only never expired"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(idle_from.elapsed() >= WINDOW);
}

/// A busy span that starts and ends between two polls counts as busy, not idle.
#[test]
fn idle_only_ignores_a_busy_span_between_polls() {
    let p = Progress::new();
    let mut idle = StallTimer::idle_only(WINDOW, &p);
    let g = p.busy();
    std::thread::sleep(WINDOW * 2);
    drop(g);
    assert!(matches!(idle.poll(&p), Stall::StalledFor(d) if d < WINDOW));
}

/// LT5c grep: `StallTimer::idle_only` is the T29 probe's alone (freemkv GUI), so no
/// libfreemkv source outside `halt` constructs one; T7 and T12 stay plain.
#[test]
fn idle_only_is_constructed_only_at_the_t29_site() {
    fn walk(dir: &std::path::Path, hits: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                walk(&path, hits);
            } else if path.extension().is_some_and(|x| x == "rs") {
                let src = std::fs::read_to_string(&path).unwrap();
                if src.contains("idle_only(") {
                    hits.push(path.display().to_string());
                }
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    walk(&root, &mut hits);
    let halt = root.join("halt");
    hits.retain(|h| h != &root.join("halt.rs").display().to_string());
    hits.retain(|h| !h.starts_with(&halt.display().to_string()));
    assert!(hits.is_empty(), "idle_only outside halt: {hits:?}");
}

// Serialise on the holder count; a poisoned lock (a failed sibling test) still serialises.
fn holder_lock() -> std::sync::MutexGuard<'static, ()> {
    DRIVE_HOLDER_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

// Wait (bounded) for detached holders to finish, so the next test sees `before`.
fn settle_holders(before: usize) {
    let t = Instant::now();
    while live_drive_holders() != before {
        assert!(t.elapsed() < SLACK * 4, "holders never settled");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn drive_holder_joins_and_counts() {
    let _serial = holder_lock();
    let (tx, rx) = mpsc::channel::<()>();
    let before = live_drive_holders();
    let holder = spawn_drive_holder("test-holder", move || {
        let _ = rx.recv();
        std::thread::current().name().map(str::to_owned)
    })
    .unwrap();
    assert_eq!(holder.role(), "test-holder");
    assert_eq!(live_drive_holders(), before + 1);
    assert!(!holder.is_finished());
    drop(tx);
    let name = holder.join().unwrap();
    assert_eq!(name.as_deref(), Some("freemkv-test-holder"));
    assert_eq!(
        live_drive_holders(),
        before,
        "the count drops before join returns"
    );
}

/// §2.1: a Drive holder dropped unjoined panics in debug builds.
#[cfg(debug_assertions)]
#[test]
fn drive_holder_dropped_unjoined_panics_in_debug() {
    let _serial = holder_lock();
    let before = live_drive_holders();
    let holder = spawn_drive_holder("leaky", || 1u8).unwrap();
    let r = catch(move || drop(holder));
    assert!(r.is_err());
    settle_holders(before);
}

/// LT6: a debug panic for each forbidden call (exec, sleep, join) under the scope;
/// `join_finished` (and a zero-length wait) are exempt; scopes nest.
#[cfg(debug_assertions)]
#[test]
fn no_blocking_scope_trips_on_exec_sleep_join() {
    let _serial = holder_lock();
    let before = live_drive_holders();
    let h = Halt::new();
    let (running, release) = parked_thread();
    let finished = std::thread::spawn(|| 1u8);
    while !finished.is_finished() {
        std::thread::yield_now();
    }
    let (hold, held) = mpsc::channel::<()>();
    let holder = spawn_drive_holder("scoped", move || held.recv().is_err()).unwrap();
    {
        let _outer = NoBlockingScope::enter();
        let inner = NoBlockingScope::enter();
        drop(inner);
        assert!(NoBlockingScope::active());
        assert!(catch(|| h.wait(Duration::from_millis(1))).is_err(), "sleep");
        assert!(
            catch(move || join_within(running, WINDOW, None)).is_err(),
            "join"
        );
        assert!(catch(move || holder.join()).is_err(), "holder join");
        #[cfg(feature = "rip")]
        assert!(catch(scoped_exec).is_err(), "exec");
        assert!(catch(|| h.wait(Duration::ZERO)).is_ok());
        assert!(matches!(join_finished(finished), Ok(1)));
    }
    assert!(!NoBlockingScope::active());
    assert!(h.wait(Duration::from_millis(1)).is_ok());
    #[cfg(feature = "rip")]
    assert!(catch(scoped_exec_outside).unwrap());
    drop((release, hold));
    settle_holders(before);
}

#[cfg(all(debug_assertions, feature = "rip"))]
fn test_drive() -> crate::drive::Drive {
    use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};
    struct Ok0;
    impl ScsiTransport for Ok0 {
        fn execute(
            &mut self,
            _: &[u8],
            _: DataDirection,
            _: &mut [u8],
            _: u32,
        ) -> Result<ScsiResult> {
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 0,
                sense: [0u8; 32],
            })
        }
    }
    crate::drive::Drive::from_transport_for_test(Box::new(Ok0))
}

#[cfg(all(debug_assertions, feature = "rip"))]
fn scoped_exec() {
    let mut d = test_drive();
    let tur = [0u8; 6];
    let _ = d.checked_exec(&tur, crate::scsi::DataDirection::None, &mut [], 1000);
}

#[cfg(all(debug_assertions, feature = "rip"))]
fn scoped_exec_outside() -> bool {
    let mut d = test_drive();
    let tur = [0u8; 6];
    d.checked_exec(&tur, crate::scsi::DataDirection::None, &mut [], 1000)
        .is_ok()
}

const LT7_RUNS: usize = 20;
// Cancel this long after the waiter starts: well inside a 250 ms slice, so the old
// `POLL_INTERVAL` wakes ~220 ms late on every run, far past the 150 ms bound.
const LT7_CANCEL_AFTER: Duration = Duration::from_millis(30);
const LT7_BOUND: Duration = Duration::from_millis(150);
const BUDGET: Duration = Duration::from_secs(10);

// Median over `LT7_RUNS` of cancel → return; each run asserts the wait was halted.
fn median_wake(what: &str, run: impl Fn(&Halt) -> Duration) -> Duration {
    let mut v: Vec<Duration> = (0..LT7_RUNS).map(|_| run(&Halt::new())).collect();
    v.sort();
    let m = v[LT7_RUNS / 2];
    assert!(
        m <= LT7_BOUND,
        "{what}: median wake {m:?} > {LT7_BOUND:?} ({v:?})"
    );
    m
}

/// LT7: every halt-aware wait, blocked on a 10 s budget, returns within a slice of a
/// cancel from another thread (≤ 150 ms, median of 20). Replaces the 250 ms
/// constant-equality test: a slice regressed to 250 ms or more fails it.
#[test]
fn every_halt_aware_wait_wakes_within_a_slice() {
    median_wake("Halt::wait", |h| {
        let h2 = h.clone();
        let (r, took) = cancel_during(h, LT7_CANCEL_AFTER, move || h2.wait(BUDGET));
        assert!(matches!(r, Err(Error::Halted)));
        took
    });
    median_wake("Halt::recv_timeout", |h| {
        let (tx, rx) = mpsc::channel::<u8>();
        let h2 = h.clone();
        let (r, took) = cancel_during(h, LT7_CANCEL_AFTER, move || h2.recv_timeout(&rx, BUDGET));
        drop(tx);
        assert!(matches!(r, Err(Error::Halted)));
        took
    });
    median_wake("Halt::send_timeout", |h| {
        let (tx, rx) = mpsc::sync_channel::<u8>(0);
        let h2 = h.clone();
        let (r, took) = cancel_during(h, LT7_CANCEL_AFTER, move || h2.send_timeout(&tx, 1, BUDGET));
        drop(rx);
        assert_eq!(r, Err(Halted(1)));
        took
    });
    median_wake("join_within", |h| {
        let (target, release) = parked_thread();
        let h2 = h.clone();
        let (r, took) = cancel_during(h, LT7_CANCEL_AFTER, move || {
            join_within(target, BUDGET, Some(&h2))
        });
        drop(release);
        let Joined::Halted(target) = r else {
            panic!("join_within was not halted");
        };
        target.join().unwrap();
        took
    });
    #[cfg(feature = "rip")]
    median_wake("Pipeline::send_with_halt", pipeline_send_wake);
}

// A depth-1 pipeline whose consumer holds its first item until released, so the
// next sends fill the channel and the one after blocks.
#[cfg(feature = "rip")]
fn pipeline_send_wake(h: &Halt) -> Duration {
    use crate::io::pipeline::{Flow, Pipeline, Sink};
    struct Gate(mpsc::Receiver<()>);
    impl Sink<u32> for Gate {
        type Output = ();
        fn apply(&mut self, _: u32) -> Result<Flow> {
            let _ = self.0.recv();
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<()> {
            Ok(())
        }
    }
    let (open, gate) = mpsc::channel::<()>();
    let pipe = Arc::new(Pipeline::spawn(1, Gate(gate)).unwrap());
    let idle = Halt::new();
    pipe.send_with_halt(1, &idle, BUDGET).unwrap();
    // Fill the channel: the consumer may or may not have taken item 1 yet.
    while pipe
        .send_with_halt(2, &idle, Duration::from_millis(50))
        .is_ok()
    {}
    let (p2, h2) = (pipe.clone(), h.clone());
    let (r, took) = cancel_during(h, LT7_CANCEL_AFTER, move || {
        p2.send_with_halt(3, &h2, BUDGET)
    });
    assert_eq!(r, Err(3));
    drop(open);
    let pipe = Arc::try_unwrap(pipe).ok().expect("sole owner");
    pipe.finish().unwrap();
    took
}

/// per spec; do not change without a spec citation proving otherwise — SS-23: the
/// Release/Acquire pair `cancel`/`is_cancelled` rely on (LT3 model-checks it).
#[test]
fn ss_23_release_acquire_quote_backs_the_token() {
    let t = crate::spec::stop::SS_23_RELEASE_ACQUIRE.text;
    assert!(t.contains("all previous writes become visible to all threads"));
    assert!(t.contains("Acquire (or stronger) load of this value"));
    assert!(t.contains("all subsequent loads will see data written before the store"));
}
