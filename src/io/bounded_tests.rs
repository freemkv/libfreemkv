use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

// `Duration::MAX` overflowed `Instant + Duration` and panicked; it means unbounded.
#[test]
fn duration_max_timeout_is_unbounded_not_a_panic() {
    let r = bounded_syscall(None, Duration::MAX, || 7u32);
    assert!(matches!(r, Ok(7)));
}

#[test]
fn op_completes_quickly() {
    let r = bounded_syscall(None, Duration::from_secs(2), || 42u32);
    assert!(matches!(r, Ok(42)));
}

#[test]
fn op_exceeds_timeout() {
    // Op sleeps longer than the deadline → Timeout.
    let r = bounded_syscall(None, Duration::from_millis(300), || {
        thread::sleep(Duration::from_secs(2));
        0u32
    });
    assert!(matches!(r, Err(BoundedError::Timeout)));
}

#[test]
fn halt_fires_during_wait() {
    let halt = Halt::new();
    let halt2 = halt.clone();
    // Flip halt after ~300ms, long enough that the receive loop has rolled
    // several WAIT_SLICE polls and is back in `recv_timeout`.
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        halt2.cancel();
    });
    let r = bounded_syscall(Some(&halt), Duration::from_secs(5), || {
        thread::sleep(Duration::from_secs(5));
        0u32
    });
    assert!(matches!(r, Err(BoundedError::Halted)));
}

#[test]
fn worker_panics() {
    // Worker panics → sender drops without sending → recv sees Disconnected →
    // WorkerLost. Panic is in the op closure, not the channel machinery; it's
    // contained (no process abort) because we don't `.join()` the thread.
    let r = bounded_syscall(None, Duration::from_secs(2), || -> u32 {
        panic!("intentional test panic");
    });
    assert!(matches!(r, Err(BoundedError::WorkerLost)));
}

#[test]
fn ok_path_takes_no_halt_token() {
    // Sanity: the `None` halt path is the documented zero-config
    // form (matches the 0.20.5 `wait_after_with_timeout`
    // behaviour). Op returns immediately; we must observe Ok.
    let flag = Arc::new(AtomicBool::new(false));
    let f2 = flag.clone();
    let r = bounded_syscall(None, Duration::from_secs(2), move || {
        f2.store(true, Ordering::Relaxed);
        "ok"
    });
    assert!(matches!(r, Ok("ok")));
    assert!(flag.load(Ordering::Relaxed));
}

// ── Added hardening tests ───────────────────────────────────────

// Pre-cancelled halt must short-circuit before spawning the
// worker, so `op` must never run. Verified via a side-effect flag.
#[test]
fn pre_cancelled_halt_never_runs_op() {
    let halt = Halt::new();
    halt.cancel();
    let ran = Arc::new(AtomicBool::new(false));
    let r2 = ran.clone();
    let r = bounded_syscall(Some(&halt), Duration::from_secs(2), move || {
        r2.store(true, Ordering::SeqCst);
        7u32
    });
    assert!(matches!(r, Err(BoundedError::Halted)));
    // The op closure must not have been scheduled at all.
    assert!(
        !ran.load(Ordering::SeqCst),
        "op ran despite pre-cancelled halt — start() short-circuit broken"
    );
}

// Must return Timeout near the deadline, not after the op
// finishes — the worker is leaked, not awaited.
#[test]
fn timeout_returns_near_deadline_not_after_op() {
    let started = Instant::now();
    let r = bounded_syscall(None, Duration::from_millis(100), || {
        thread::sleep(Duration::from_secs(3));
        0u32
    });
    let elapsed = started.elapsed();
    assert!(matches!(r, Err(BoundedError::Timeout)));
    // Must bail near the 100ms deadline (one WAIT_SLICE slack at
    // most), not after the 3s op. Allow generous CI slack but stay
    // well under the op's 3s sleep.
    assert!(
        elapsed < Duration::from_millis(1500),
        "timeout did not return near deadline: {elapsed:?} (op should be leaked, not awaited)"
    );
}

/// LP12: a halted wait returns at once and leaks the worker, and the worker's
/// rendezvous send to the dropped receiver does not wedge it: it ends once the
/// op returns. Guard.
#[test]
fn bounded_syscall_halt_leaks_worker_without_blocking() {
    let halt = Halt::new();
    let h2 = halt.clone();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let at = Instant::now();
        h2.cancel();
        at
    });
    let alive = Arc::new(());
    let held = alive.clone();
    let release = Arc::new(AtomicBool::new(false));
    let rel = release.clone();
    let r = bounded_syscall(Some(&halt), Duration::from_secs(60), move || {
        // Blocked until the test releases it: the wait cannot end by the op returning.
        while !rel.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(5));
        }
        drop(held);
        1u8
    });
    let returned = Instant::now();
    let cancelled_at = canceller.join().unwrap();
    assert!(matches!(r, Err(BoundedError::Halted)));
    let woke = returned.saturating_duration_since(cancelled_at);
    assert!(woke < Duration::from_secs(1), "{woke:?}");
    assert!(Arc::strong_count(&alive) > 1, "the worker is still blocked");
    release.store(true, Ordering::SeqCst);
    let t = Instant::now();
    while Arc::strong_count(&alive) > 1 {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "the leaked worker wedged"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

/// A panicking worker is `WorkerLost` in the stall variant, not a stall `Timeout`.
#[test]
fn stall_variant_reports_worker_lost_on_panic() {
    let p = Liveness::new();
    let mut timer = StallTimer::new(Duration::from_secs(5), &p);
    let r = bounded_syscall_stall(None, &p, &mut timer, &mut || {}, || -> u8 {
        panic!("intentional test panic");
    });
    assert!(matches!(r, Err(BoundedError::WorkerLost)));
}

/// HR1 for one blocked call: a worker blocked past the window while `tick` bumps the
/// progress is waited for; the same call with no progress times out at the window.
#[test]
fn stall_bound_rearms_on_progress_and_expires_without() {
    let w = Duration::from_secs(1);
    let p = Liveness::new();
    let mut timer = StallTimer::new(w, &p);
    let bump = p.clone();
    let r = bounded_syscall_stall(None, &p, &mut timer, &mut || bump.bump(), || {
        thread::sleep(Duration::from_millis(2500));
        3u8
    });
    assert!(matches!(r, Ok(3)));
    let mut timer = StallTimer::new(w, &p);
    let t = Instant::now();
    let r = bounded_syscall_stall(None, &p, &mut timer, &mut || {}, || {
        thread::sleep(Duration::from_secs(10));
        0u8
    });
    assert!(matches!(r, Err(BoundedError::Timeout)));
    assert!(t.elapsed() >= w, "fired before the window");
    assert!(
        t.elapsed() < w + Duration::from_secs(2),
        "{:?}",
        t.elapsed()
    );
}
