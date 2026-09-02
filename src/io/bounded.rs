//! Bounded-syscall primitive: run a (potentially-blocking) operation
//! on a worker thread, with a hard wall-clock deadline and an optional
//! cooperative [`Halt`] poll. The calling thread is never trapped
//! inside a kernel call.
//!
//! Escape hatch for syscalls that trap a thread inside the kernel
//! (`sync_file_range`, `fsync`, NFS writes) where a cooperative
//! [`Halt`] can't interrupt. The worker thread is leaked on timeout
//! or halt. See docs/bounded.md for rationale, trade-offs, and platform notes.

use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use crate::halt::{Halt, POLL_INTERVAL};

/// Failure outcome from a bounded syscall wrapper.
#[derive(Debug)]
pub(crate) enum BoundedError {
    /// The user-visible halt token fired during the wait. The worker
    /// thread is intentionally leaked — the caller should fall back to
    /// a degraded code path rather than waiting on the syscall to
    /// return.
    Halted,
    /// The deadline elapsed before the syscall returned. Same leak
    /// semantics as `Halted`.
    Timeout,
    /// The worker thread panicked, the OS rejected the thread spawn,
    /// or its sender disconnected before sending a result. Treat as a
    /// benign no-op (callers usually log and continue) rather than a
    /// hard error — by definition no syscall observably ran to
    /// completion in this case. In the spawn-failure case no thread is
    /// leaked.
    WorkerLost,
}

// Runs `op` on a worker thread with a deadline + optional [`Halt`]
// poll; returns Ok, or Err on Halted/Timeout/WorkerLost (worker leaked).
// See docs/bounded.md for halt polling, `op` ownership, and semantics.
pub(crate) fn bounded_syscall<F, R>(
    halt: Option<&Halt>,
    timeout: Duration,
    op: F,
) -> Result<R, BoundedError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // If the caller already requested halt, don't spawn (and leak) a
    // worker that would run `op` to completion in the background.
    if halt.is_some_and(|h| h.is_cancelled()) {
        return Err(BoundedError::Halted);
    }

    // Rendezvous channel: worker sends one value then exits. On timeout/halt
    // the receiver is dropped, so the worker's send returns Err (ignored).
    let (tx, rx) = sync_channel::<R>(0);
    let _ = thread::Builder::new()
        .name("freemkv-bounded-syscall".into())
        .spawn(move || {
            // Ignore send error: if we time out/halt before the worker finishes,
            // the receiver is dropped and `tx.send` returns Err — nothing more to do.
            let _ = tx.send(op());
        });

    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        let slice = remaining.min(POLL_INTERVAL);
        match rx.recv_timeout(slice) {
            Ok(v) => return Ok(v),
            Err(RecvTimeoutError::Timeout) => {
                if let Some(h) = halt
                    && h.is_cancelled()
                {
                    return Err(BoundedError::Halted);
                }
                if Instant::now() >= deadline {
                    return Err(BoundedError::Timeout);
                }
                // Otherwise: another slice.
            }
            Err(RecvTimeoutError::Disconnected) => {
                // Worker thread spawn failed, or it panicked before
                // sending. Caller treats this as "no syscall ran" —
                // typically a no-op + log.
                return Err(BoundedError::WorkerLost);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
        // at least one 250ms slice and is back in `recv_timeout`.
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
    fn halt_already_set_before_call_still_returns_halted() {
        // Halt observed on the very first poll slice. The op blocks
        // forever; we must not wait the full timeout to notice the
        // halt is already set.
        let halt = Halt::new();
        halt.cancel();
        let started = Instant::now();
        let r = bounded_syscall(Some(&halt), Duration::from_secs(10), || {
            thread::sleep(Duration::from_secs(10));
            0u32
        });
        assert!(matches!(r, Err(BoundedError::Halted)));
        // Should bail out within ~1 s; allow 2 s of slack for slow
        // CI hosts.
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "halt-already-set took {:?}",
            started.elapsed()
        );
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
            "op ran despite pre-cancelled halt — short-circuit at line 108 broken"
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
}
