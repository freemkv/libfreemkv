//! Bounded-syscall primitive: run a (potentially-blocking) operation on a worker thread
//! while the caller waits halt-aware, bounded by a deadline (`bounded_syscall`, one
//! call's "no answer" bound) or by a stall window over a [`Progress`]
//! ([`bounded_syscall_stall`], HR1). The calling thread is never trapped in a kernel call.
//!
//! Escape hatch for syscalls a cooperative [`Halt`] can't interrupt (`sync_file_range`,
//! `fsync`, `FlushFileBuffers`, NFS writes). The worker thread is leaked on timeout or halt.

use std::sync::mpsc::{Receiver, sync_channel};
use std::thread;
#[cfg(any(target_os = "linux", test))]
use std::time::Duration;

use crate::halt::{Halt, Progress, Recv, Stall, StallTimer, WAIT_SLICE};

/// Failure outcome from a bounded syscall wrapper.
#[derive(Debug)]
pub(crate) enum BoundedError {
    /// The user-visible halt token fired during the wait. The worker
    /// thread is intentionally leaked — the caller should fall back to
    /// a degraded code path rather than waiting on the syscall to
    /// return.
    Halted,
    /// The deadline (or stall window) elapsed before the syscall returned.
    /// Same leak semantics as `Halted`.
    Timeout,
    /// The worker thread panicked, the OS rejected the thread spawn,
    /// or its sender disconnected before sending a result. Treat as a
    /// benign no-op (callers usually log and continue) rather than a
    /// hard error — by definition no syscall observably ran to
    /// completion in this case. In the spawn-failure case no thread is
    /// leaked.
    WorkerLost,
}

// Start `op` on a worker; `None` if the caller already requested halt (don't spawn and
// leak a worker that would run `op` in the background).
fn start<F, R>(halt: Option<&Halt>, op: F) -> Option<Receiver<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    if halt.is_some_and(Halt::is_cancelled) {
        return None;
    }
    // Rendezvous channel: worker sends one value then exits. On timeout/halt
    // the receiver is dropped, so the worker's send returns Err (ignored).
    let (tx, rx) = sync_channel::<R>(0);
    let _ = thread::Builder::new()
        .name("freemkv-bounded-syscall".into())
        .spawn(move || {
            let _ = tx.send(op());
        });
    Some(rx)
}

// Runs `op` on a worker thread with a deadline + optional [`Halt`] poll; returns Ok, or Err on
// Halted/Timeout/WorkerLost (worker leaked).
#[cfg(any(target_os = "linux", test))]
pub(crate) fn bounded_syscall<F, R>(
    halt: Option<&Halt>,
    timeout: Duration,
    op: F,
) -> Result<R, BoundedError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let rx = start(halt, op).ok_or(BoundedError::Halted)?;
    // Halt-aware in WAIT_SLICE slices; a `timeout` past `Instant`'s range is unbounded.
    let never = Halt::new();
    match halt.unwrap_or(&never).recv_timeout(&rx, timeout) {
        Ok(Recv::Item(v)) => Ok(v),
        Ok(Recv::TimedOut) => Err(BoundedError::Timeout),
        // Worker spawn failed, or it panicked before sending: "no syscall ran".
        Ok(Recv::Disconnected) => Err(BoundedError::WorkerLost),
        Err(_) => Err(BoundedError::Halted),
    }
}

// Runs `op` on a worker thread; the caller waits halt-aware, calling `tick` every slice
// (a sampler may bump `progress`), and gives up with `Timeout` only once `timer` expires
// on `progress` (HR1: no progress for its window, never elapsed time).
pub(crate) fn bounded_syscall_stall<F, R>(
    halt: Option<&Halt>,
    progress: &Progress,
    timer: &mut StallTimer,
    tick: &mut dyn FnMut(),
    op: F,
) -> Result<R, BoundedError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let rx = start(halt, op).ok_or(BoundedError::Halted)?;
    let never = Halt::new();
    let halt = halt.unwrap_or(&never);
    loop {
        match halt.recv_timeout(&rx, WAIT_SLICE) {
            Ok(Recv::Item(v)) => return Ok(v),
            Ok(Recv::Disconnected) => return Err(BoundedError::WorkerLost),
            Err(_) => return Err(BoundedError::Halted),
            Ok(Recv::TimedOut) => {}
        }
        tick();
        if timer.poll(progress) == Stall::Expired {
            return Err(BoundedError::Timeout);
        }
    }
}

#[cfg(test)]
mod tests {
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
        // Must bail near the 100ms deadline (one POLL_INTERVAL slack at
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
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            h2.cancel();
        });
        let alive = Arc::new(());
        let held = alive.clone();
        let t = Instant::now();
        let r = bounded_syscall(Some(&halt), Duration::from_secs(10), move || {
            thread::sleep(Duration::from_millis(200));
            drop(held);
            1u8
        });
        assert!(matches!(r, Err(BoundedError::Halted)));
        assert!(
            t.elapsed() < Duration::from_millis(500),
            "{:?}",
            t.elapsed()
        );
        let t = Instant::now();
        while Arc::strong_count(&alive) > 1 {
            assert!(
                t.elapsed() < Duration::from_secs(2),
                "the leaked worker wedged"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// A panicking worker is `WorkerLost` in the stall variant, not a stall `Timeout`.
    #[test]
    fn stall_variant_reports_worker_lost_on_panic() {
        let p = Progress::new();
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
        let w = Duration::from_millis(100);
        let p = Progress::new();
        let mut timer = StallTimer::new(w, &p);
        let bump = p.clone();
        let r = bounded_syscall_stall(None, &p, &mut timer, &mut || bump.bump(), || {
            thread::sleep(Duration::from_millis(400));
            3u8
        });
        assert!(matches!(r, Ok(3)));
        let mut timer = StallTimer::new(w, &p);
        let t = Instant::now();
        let r = bounded_syscall_stall(None, &p, &mut timer, &mut || {}, || {
            thread::sleep(Duration::from_secs(2));
            0u8
        });
        assert!(matches!(r, Err(BoundedError::Timeout)));
        assert!(
            t.elapsed() < Duration::from_millis(1100),
            "{:?}",
            t.elapsed()
        );
    }
}
