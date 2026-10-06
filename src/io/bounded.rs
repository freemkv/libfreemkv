//! Bounded-syscall primitive: run a (potentially-blocking) operation on a worker thread
//! while the caller waits halt-aware, bounded by a deadline (`bounded_syscall`, one
//! call's "no answer" bound) or by a stall window over a [`Liveness`]
//! ([`bounded_syscall_stall`], HR1). The calling thread is never trapped in a kernel call.
//!
//! Escape hatch for syscalls a cooperative [`Halt`] can't interrupt (`sync_file_range`,
//! `fsync`, `FlushFileBuffers`, NFS writes). The worker thread is leaked on timeout or halt.

use std::sync::mpsc::{Receiver, sync_channel};
use std::thread;
#[cfg(any(target_os = "linux", test))]
use std::time::Duration;

use crate::halt::{Halt, Liveness, Recv, Stall, StallTimer, WAIT_SLICE};

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
    progress: &Liveness,
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
#[path = "bounded_tests.rs"]
mod tests;
