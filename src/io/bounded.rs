//! Bounded-syscall primitive: run a (potentially-blocking) operation
//! on a worker thread, with a hard wall-clock deadline and an optional
//! cooperative [`Halt`] poll. The calling thread is never trapped
//! inside a kernel call.
//!
//! ## Why this exists
//!
//! [`crate::halt::Halt`] is cooperative: callers poll
//! `is_cancelled()`. It cannot reach inside a syscall the kernel
//! currently owns the thread for — `libc::sync_file_range`,
//! `libc::fsync`, `File::write` on NFS, and so on. `/api/stop` from
//! autorip therefore can't unstick a thread sitting in such a syscall.
//!
//! [`bounded_syscall`] is the escape hatch: it runs `op` on a fresh
//! worker thread, then `recv_timeout`s on a rendezvous channel for the
//! result. The wait is broken into ~250 ms slices so the calling
//! thread can poll the supplied [`Halt`] in between. If the deadline
//! elapses or the halt fires, the worker is intentionally leaked — the
//! syscall will unwind whenever the kernel decides, or at process
//! exit, but the caller is free to fall back to a degraded code path
//! (skip the sync, log loudly, etc.).
//!
//! ## Trade-offs
//!
//! - **Thread per call.** Cheap (`std::thread::spawn` is < 100 µs on
//!   Linux/macOS), but not free. Use on coarse-grained finalisation
//!   syscalls (`sync_all`, `sync_file_range(WAIT_AFTER)`), not on hot
//!   inner-loop writes.
//! - **Leak on timeout.** A wedged syscall keeps a kernel slot and a
//!   user-space thread around for the rest of the process's life.
//!   Bounded by the number of independent rip/mux sessions, which is
//!   one per disc. The alternative — trapping the caller forever —
//!   defeats the entire purpose of `/api/stop`.
//! - **Halt granularity ~250 ms.** Halt observation is not instant;
//!   it's the worst-case latency of the `recv_timeout` slice. Good
//!   enough for human-driven stop requests; not suitable for hard
//!   real-time deadlines.
//!
//! ## Single source of truth
//!
//! Do NOT inline this pattern. Every blocking-syscall wrapper in the
//! rip + mux pipeline calls this helper, so changes (e.g. swapping the
//! channel impl, adjusting the poll slice, adding metrics) land in one
//! place.
//!
//! ## Platform
//!
//! Pure `std::thread` + `std::sync::mpsc`. No `cfg(target_os)` needed
//! here — the helper itself is platform-agnostic. Callers that wrap
//! Linux-only syscalls (`sync_file_range`) still need their own
//! `#[cfg(target_os = "linux")]` gates; this helper does not.

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

/// Run a (potentially-blocking) operation on a worker thread with a
/// deadline and an optional cooperative halt-token poll. Returns the
/// operation's result if it completes within `timeout`; otherwise one
/// of [`BoundedError::Halted`] / [`BoundedError::Timeout`] /
/// [`BoundedError::WorkerLost`].
///
/// On `Halted` / `Timeout` the worker thread is intentionally leaked:
/// the syscall will unwind whenever the kernel decides, or when the
/// process exits. The calling thread is never trapped inside a kernel
/// call.
///
/// `halt` is polled at [`POLL_INTERVAL`] granularity. Pass `None` for
/// callers that don't (yet) have a halt token plumbed through —
/// behaviour degrades to deadline-only, matching the 0.20.5
/// `wait_after_with_timeout` shape this helper generalises.
///
/// `op` returns `R: Send + 'static`. The closure must own everything
/// it touches because it may outlive this call (timeout / halt cases).
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

    // Rendezvous channel: the worker sends exactly one value (the
    // op's return) and then exits. Capacity-0 means the send blocks
    // until we receive — fine on the happy path; on the timeout /
    // halt path the receiver is dropped and the worker's send
    // returns Err, which the worker ignores.
    let (tx, rx) = sync_channel::<R>(0);
    let _ = thread::Builder::new()
        .name("freemkv-bounded-syscall".into())
        .spawn(move || {
            // Ignore the send error: if we time out (or get halted)
            // before the worker finishes, the receiver is dropped
            // and `tx.send` returns Err. Either way, the worker has
            // nothing more to do.
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
                if let Some(h) = halt {
                    if h.is_cancelled() {
                        return Err(BoundedError::Halted);
                    }
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
        // Flip the halt from a side thread after ~300 ms — long
        // enough that the receive loop has rolled at least one
        // 250 ms slice and is sitting in `recv_timeout` again when
        // the bit flips.
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
        // Worker panics → sender drops without sending → recv sees
        // Disconnected → WorkerLost. We use an explicit panic in the
        // op closure rather than `panic!()` from inside the channel
        // machinery; the spawned thread's panic is contained (no
        // process abort) because we don't `.join()` it.
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

    /// Doc contract (lines 106-110): "If the caller already requested
    /// halt, don't spawn (and leak) a worker that would run `op`."
    /// When halt is pre-cancelled the op closure must NEVER run — the
    /// short-circuit returns Halted before spawning the worker. We
    /// prove the op did not execute by checking a side-effect flag.
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

    /// Boundary: an op that finishes well within the deadline returns
    /// Ok even when a (live, never-cancelled) halt token is supplied.
    /// The halt-poll path must not spuriously convert a completed op
    /// into Halted/Timeout. Grounds the `Ok(v) => return Ok(v)` arm of
    /// the recv_timeout match (line 134) with a non-None halt.
    #[test]
    fn live_halt_token_does_not_interfere_with_fast_op() {
        let halt = Halt::new(); // never cancelled
        let r = bounded_syscall(Some(&halt), Duration::from_secs(5), || 123u64);
        assert!(matches!(r, Ok(123)));
        assert!(!halt.is_cancelled());
    }

    /// The op's return value is propagated byte-for-byte, not just a
    /// success flag. A non-Copy heap type proves the worker's
    /// `tx.send(op())` moves the real value across the rendezvous
    /// channel (line 125) to the receiver (line 134).
    #[test]
    fn returns_owned_value_unchanged() {
        let r = bounded_syscall(None, Duration::from_secs(2), || vec![9u8, 8, 7, 6]);
        match r {
            Ok(v) => assert_eq!(v, vec![9u8, 8, 7, 6]),
            other => panic!("expected Ok(vec), got {other:?}"),
        }
    }

    /// Timeout boundary: with a tiny deadline and an op that sleeps
    /// much longer, the helper must return Timeout and must do so
    /// roughly at the deadline — NOT wait for the op to finish (that
    /// is the whole point of the bounded wrapper; the worker is
    /// leaked). Grounds the `Instant::now() >= deadline` arm (line 141)
    /// and the leak contract (doc lines 84-88).
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

    /// A worker that returns a non-Copy value AND completes within the
    /// deadline must hand the value back; the rendezvous channel has
    /// capacity 0, so the worker's send blocks until the receiver is
    /// ready — exercising the happy-path handshake rather than the
    /// buffered-send path. Mutation: changing `sync_channel::<R>(0)` to
    /// a buffered channel would still pass; changing the recv arm to
    /// drop the value would fail here.
    #[test]
    fn zero_capacity_rendezvous_delivers_string() {
        let r = bounded_syscall(None, Duration::from_secs(2), || String::from("rendezvous"));
        assert!(matches!(r.as_deref(), Ok("rendezvous")));
    }

    /// Halt that fires AFTER the op has already completed must still
    /// yield Ok — there is no race that turns a delivered result into
    /// Halted. The op completes instantly; we cancel the halt
    /// afterwards and confirm the earlier call returned Ok. This pins
    /// the precedence: a value already in the channel wins over a
    /// subsequent halt.
    #[test]
    fn op_completion_wins_over_later_halt() {
        let halt = Halt::new();
        let r = bounded_syscall(Some(&halt), Duration::from_secs(2), || 55u32);
        halt.cancel();
        assert!(matches!(r, Ok(55)));
    }
}
