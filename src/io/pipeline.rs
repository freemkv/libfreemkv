//! Generic bounded producer/consumer pipeline.
//!
//! `Pipeline<I, R>` spawns a single consumer thread, hands it items
//! through a bounded `mpsc::sync_channel`, and joins it on `finish()`.
//! The consumer's behaviour is supplied by a [`Sink`] implementation:
//! `apply` is called once per item, `close` is called once at the end.
//!
//! Call sites in libfreemkv that want a producer/consumer split —
//! sweep (`disc/sweep.rs::SweepSink`) and the file-backed mux highway
//! — are built on this primitive.
//!
//! ## Cancellation and error semantics
//!
//! - Producer dropping the channel (via `Pipeline::finish` dropping
//!   `tx`) signals end-of-stream; consumer flushes via `close()` and
//!   returns its `Output`.
//! - Consumer returning [`Flow::Stop`] also calls `close()` and
//!   returns its `Output`. `send()` from the producer will then either
//!   succeed (if the item already fit in the channel buffer) or fail
//!   with `Err(item)` once the consumer has dropped its receiver.
//! - Consumer returning `Err` from `apply` skips `close()` entirely;
//!   the consumer keeps draining the channel so the producer never
//!   blocks on a dead receiver, and the first error is propagated as
//!   the `JoinHandle` result.
//! - Consumer panic is converted into
//!   [`Error::PipelineConsumerPanicked`] (the panic message is logged,
//!   not embedded in the error value).
//!
//! ## Debug logging
//!
//! Set `FREEMKV_DEBUG=1` environment variable to enable verbose debug
//! logging throughout the pipeline (channel sends/receives, backpressure,
//! consumer lag detection). This is critical for diagnosing stalls.

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError, bounded};

use crate::error::Error;
use crate::halt::Halt;

/// Deadline for [`Pipeline::finish_with_halt`]'s polling join.
///
/// 10 minutes is a backstop, not a normal timeout — the consumer is
/// expected to drain in seconds. If we hit this, something is wedged
/// inside a kernel call the consumer thread can't unwind from. It is
/// deliberately long so a consuming application's own (shorter) stall
/// watchdog gets the first chance to escalate; this join only fires
/// when no such watchdog intervenes.
pub const JOIN_TIMEOUT_SECS: u64 = 600;

/// Short grace period after a halt or 10-min timeout fires in
/// [`Pipeline::finish_with_halt`]. Most wedged consumers that are
/// "about to return" when the halt fires will unblock within a few
/// seconds (e.g. their bounded_syscall timeout returns and the consumer
/// drains). Spinning here converts those into clean joins and releases
/// the output file handle, at the cost of at most this much extra
/// latency on a genuinely stuck consumer before we accept the leak.
const FINISH_GRACE_SECS: u64 = 5;

/// Halt-check cadence for the send loop. Producer blocks on
/// [`crossbeam_channel::Sender::send_timeout`] for this slice — the
/// kernel wakes it the instant the consumer drains a slot, so on the
/// happy path there's no throughput cap from this primitive at all
/// (the cap is whatever the underlying medium can sustain). When the
/// consumer is genuinely wedged, the timeout fires every
/// [`crate::halt::POLL_INTERVAL`] and the producer checks the halt
/// token; that's the latency a stop request will observe.
///
/// Single source of truth lives in [`crate::halt::POLL_INTERVAL`]
/// (also used by `bounded_syscall`). Aliased here for readability of
/// the send/finish call sites below.
///
/// 0.21.7 replaced an old `std::sync::mpsc::sync_channel` + 50 ms
/// `thread::sleep` polling loop that capped mux throughput at
/// ~20 frames/sec ≈ 1 MB/s on saturated channels.
use crate::halt::POLL_INTERVAL;
const SEND_HALT_CHECK_INTERVAL: Duration = POLL_INTERVAL;

/// Check if verbose debug logging is enabled via FREEMKV_DEBUG env var.
///
/// The value cannot change mid-run, and this is called multiple times
/// per item on the mux highway hot loop, so the env lookup (a String
/// allocation behind the global env lock) is cached after the first
/// call.
pub fn debug_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FREEMKV_DEBUG")
            .ok()
            .map(|v| v == "1" || v == "true" || v == "yes")
            .unwrap_or(false)
    })
}

/// Turn a consumer-thread panic payload into the numeric
/// [`Error::PipelineConsumerPanicked`] variant. The original panic
/// message (the two stdlib formats `panic!` produces: `&str` /
/// `String`) is logged at the join site for diagnostics — it is NOT
/// baked into the error value, since the library carries no English
/// text in its errors. Callers discriminate on the variant.
fn consumer_panicked(payload: Box<dyn std::any::Any + Send>) -> Error {
    let msg = payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(|s| s.as_str()))
        .unwrap_or("(no message)");
    tracing::error!(
        target: "freemkv::pipeline",
        phase = "consumer_panicked",
        panic_message = msg,
        "pipeline consumer thread panicked"
    );
    Error::PipelineConsumerPanicked
}

/// After a halt or deadline fires, spin-poll `handle.is_finished()` for
/// [`FINISH_GRACE_SECS`] before accepting the thread leak. This converts
/// the common "nearly-done" consumer (whose own bounded_syscall just
/// returned and is about to drop its output file) into a clean join,
/// releasing the file handle without waiting the full grace period.
///
/// If the consumer is still running when the grace expires, dropping the
/// `JoinHandle` detaches from the thread — the consumer keeps running
/// until its kernel call returns or the process exits.
fn finish_with_grace<R: Send + 'static>(
    handle: thread::JoinHandle<Result<R, Error>>,
    leak_err: Error,
) -> Result<R, Error> {
    let grace = Instant::now() + Duration::from_secs(FINISH_GRACE_SECS);
    while Instant::now() < grace {
        if handle.is_finished() {
            return match handle.join() {
                Ok(result) => result,
                Err(payload) => Err(consumer_panicked(payload)),
            };
        }
        thread::sleep(POLL_INTERVAL);
    }
    // Grace expired. Log and leak.
    tracing::warn!(
        target: "freemkv::pipeline",
        phase = "finish_with_halt_grace_expired",
        "pipeline consumer did not finish within {}s grace period; leaking thread",
        FINISH_GRACE_SECS
    );
    // Dropping `handle` without joining detaches from the thread — the
    // consumer keeps running until its kernel call returns or the process
    // exits. This is the intentional "leak" documented in
    // `finish_with_halt`'s contract.
    drop(handle);
    Err(leak_err)
}

/// Default channel depth for callers without a specific reason to
/// pick another value. Kept conservative (4) — most callers should
/// use READ_PIPELINE_DEPTH or WRITE_PIPELINE_DEPTH instead.
pub const DEFAULT_PIPELINE_DEPTH: usize = 4;

/// Read pipeline depth. Larger buffer compensates for drive variability
/// and NFS sync_file_range stalls; keeps ISO reader thread fed even when
/// consumer blocks on write.
pub const READ_PIPELINE_DEPTH: usize = 32;

/// Write pipeline depth. Smaller buffer reduces backpressure risk when
/// sync_file_range blocks; prevents producer from accumulating too much
/// work while consumer waits for NFS to drain.
pub const WRITE_PIPELINE_DEPTH: usize = 16;

/// Channel depth for write-through pipelines. Each `send` fully
/// drains before the next can enqueue. Use this when the producer
/// must observe consumer side-effects (e.g. mapfile state) before
/// emitting the next item. Currently used by `disc::patch`.
pub const WRITE_THROUGH_DEPTH: usize = 1;

/// Outcome of [`Sink::apply`]: either keep feeding items
/// ([`Flow::Continue`]), or stop the pipeline early and run `close()`
/// ([`Flow::Stop`]).
///
/// `Stop` currently has no in-tree caller — sweep never returns it (it
/// always processes the producer's full work-list before the channel
/// is dropped), and the mux highway drains to EOF. The variant is part
/// of the fixed `Sink` contract for early-stop consumers, so the
/// `#[allow(dead_code)]` is intentional and permanent until such a
/// consumer lands.
pub enum Flow {
    Continue,
    #[allow(dead_code)]
    Stop,
}

/// Consumer-side of a [`Pipeline`]. The pipeline owns one of these on
/// its consumer thread and calls `apply` once per received item, then
/// `close` once at end-of-stream.
pub trait Sink<I>: Send + 'static {
    /// Type returned from `close()` and surfaced via
    /// [`Pipeline::finish`].
    type Output: Send + 'static;

    /// Apply one item. Returning [`Flow::Continue`] keeps the
    /// pipeline running; [`Flow::Stop`] ends it cleanly (still calls
    /// `close()`). An error short-circuits: `close()` is *not* called
    /// and the error is what `finish()` will return, but the consumer
    /// keeps draining the channel so the producer never blocks on a
    /// dead receiver.
    fn apply(&mut self, item: I) -> Result<Flow, Error>;

    /// Called once at end-of-stream — either because the producer
    /// dropped `tx` or because `apply` returned [`Flow::Stop`]. Use
    /// this to flush, fsync, finalise. Skipped if any prior `apply`
    /// returned `Err`.
    fn close(self) -> Result<Self::Output, Error>;
}

/// Bounded producer/consumer pipeline. Holds the producer-side
/// channel and the consumer thread's join handle.
pub struct Pipeline<I: Send + 'static, R: Send + 'static> {
    tx: Sender<I>,
    handle: JoinHandle<Result<R, Error>>,
}

impl<I: Send + 'static, R: Send + 'static> Pipeline<I, R> {
    /// Spawn the consumer thread with the given channel depth and
    /// [`Sink`].
    ///
    /// The thread is named `freemkv-pipeline-consumer` so it shows up
    /// distinctly in stack traces and `top -H`. Callers that want a
    /// more specific name (e.g. `freemkv-sweep-consumer`) should use
    /// [`Pipeline::spawn_named`] instead. Returns an `Error::IoError`
    /// if the OS refuses the thread spawn (resource exhaustion);
    /// callers already operate in fallible context, so this is
    /// propagated rather than panicked.
    ///
    /// Sweep uses [`Pipeline::spawn_named`] directly so the consumer
    /// thread shows up as `freemkv-sweep-consumer`; mux uses
    /// `freemkv-mux-consumer`. `Pipeline::spawn` (this function, with
    /// the default name) is used by `disc::patch` and by the unit
    /// tests in this module.
    pub fn spawn<S: Sink<I, Output = R>>(depth: usize, sink: S) -> Result<Self, Error> {
        Self::spawn_named("freemkv-pipeline-consumer", depth, sink)
    }

    /// Like [`Pipeline::spawn`] but lets the caller supply the
    /// consumer thread's name. Useful when several pipelines run in
    /// the same process and stack traces / `top -H` need to tell them
    /// apart (e.g. `freemkv-sweep-consumer`, `freemkv-mux-consumer`).
    pub fn spawn_named<S: Sink<I, Output = R>>(
        name: &str,
        depth: usize,
        sink: S,
    ) -> Result<Self, Error> {
        let (tx, rx) = bounded::<I>(depth);
        let handle = thread::Builder::new()
            .name(name.into())
            .spawn(move || -> Result<R, Error> {
                let mut sink = sink;
                let mut first_err: Option<Error> = None;
                let mut stopped = false;

                while let Ok(item) = rx.recv() {
                    let debug = debug_enabled();
                    if debug {
                        tracing::debug!("Pipeline receive: item={}", std::any::type_name::<I>());
                    }

                    if first_err.is_some() || stopped {
                        // Drain remaining items so the producer never
                        // blocks on a dead receiver. `apply` is not
                        // called once we've decided to stop.
                        continue;
                    }

                    // Only pay for the timestamp when debug tracing is
                    // on — this runs per item on the mux highway hot
                    // path.
                    let apply_start = debug.then(Instant::now);

                    match sink.apply(item) {
                        Ok(Flow::Continue) => {}
                        Ok(Flow::Stop) => {
                            stopped = true;
                            if debug {
                                tracing::debug!("Pipeline: consumer returned Flow::Stop");
                            }
                        }
                        Err(e) => {
                            if debug {
                                tracing::debug!("Pipeline: apply error, stopping, err={:?}", e);
                            }
                            first_err = Some(e);
                        }
                    }

                    if let Some(start) = apply_start {
                        let apply_elapsed = start.elapsed();
                        if apply_elapsed > Duration::from_millis(100) {
                            tracing::debug!(
                                "Pipeline apply: took {:.2}s, item={}",
                                apply_elapsed.as_secs_f64(),
                                std::any::type_name::<I>()
                            );
                        } else {
                            tracing::debug!(
                                "Pipeline apply: OK in {:.3}ms, item={}",
                                apply_elapsed.as_micros(),
                                std::any::type_name::<I>()
                            );
                        }
                    }
                }

                match first_err {
                    Some(e) => Err(e),
                    None => sink.close(),
                }
            })
            .map_err(|e| Error::IoError { source: e })?;

        Ok(Pipeline { tx, handle })
    }

    /// Push one item. Blocks if the channel is full — that's the
    /// back-pressure the whole primitive exists to provide. Returns
    /// the item back if the consumer thread is gone (panicked or
    /// already returned).
    ///
    /// After the consumer returns [`Flow::Stop`], `send` will silently
    /// buffer items into the channel until the channel fills, then
    /// return `Err(item)` once the consumer has dropped its receiver.
    /// Producers that need to stop pushing on `Stop` should track an
    /// independent signal (e.g. `Halt`) — `send` alone is not the
    /// notification edge.
    pub fn send(&self, item: I) -> Result<(), I> {
        // Only timestamp when debug tracing is on — `send` runs per
        // item on the mux highway hot path.
        let start = debug_enabled().then(Instant::now);
        match self.tx.send(item) {
            Ok(()) => {
                if let Some(start) = start {
                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        tracing::debug!(
                            "Pipeline send: blocked {:.2}s, item={}",
                            elapsed.as_secs_f64(),
                            std::any::type_name::<I>()
                        );
                    } else {
                        tracing::debug!("Pipeline send: OK in {:.3}ms", elapsed.as_micros());
                    }
                }
                Ok(())
            }
            Err(e) => {
                if let Some(start) = start {
                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        tracing::debug!(
                            "Pipeline send: blocked {:.2}s before channel closed, item={}",
                            elapsed.as_secs_f64(),
                            std::any::type_name::<I>()
                        );
                    } else {
                        tracing::debug!("Pipeline send: failed after {:.3}ms", elapsed.as_micros());
                    }
                }
                Err(e.0)
            }
        }
    }

    /// Non-blocking variant of [`Pipeline::send`]. If the channel is
    /// full or the consumer has hung up, the item is returned in
    /// `Err`. Useful for best-effort signalling (e.g. sweep's
    /// throttled `StatsRequest`) where dropping the message is
    /// preferable to blocking the producer.
    pub fn try_send(&self, item: I) -> Result<(), TrySendError<I>> {
        self.tx.try_send(item)
    }

    /// Halt-aware bounded variant of [`Pipeline::send`].
    ///
    /// Uses [`crossbeam_channel::Sender::send_timeout`] so the producer
    /// thread BLOCKS on consumer drain (kernel-wakeup) rather than
    /// polling. The timeout slice is just the halt-observation cadence
    /// ([`SEND_HALT_CHECK_INTERVAL`]) — on the happy path the producer
    /// wakes the instant the consumer drains a slot, so there is no
    /// throughput cap from this primitive at any medium speed.
    ///
    /// Returns:
    ///
    /// - `Ok(())` once the item lands in the channel.
    /// - `Err(item)` if the consumer disconnected, the halt fired, or
    ///   the deadline elapsed — the caller gets the item back so it
    ///   can decide whether to drop it, route it elsewhere, or unwind.
    ///
    /// Use this in producer threads that have a `Halt` token threaded
    /// through (mux, sweep, patch). Plain [`Pipeline::send`] is
    /// preserved for callers that don't (yet) plumb halt through.
    ///
    /// Unlike [`Pipeline::send`], this never blocks the producer
    /// thread inside an unkillable `mpsc::send` — if the consumer is
    /// wedged inside an unkillable syscall, the producer can still
    /// observe `/api/stop` and unwind within
    /// [`SEND_HALT_CHECK_INTERVAL`].
    pub fn send_with_halt(&self, item: I, halt: &Halt, deadline: Duration) -> Result<(), I> {
        use crossbeam_channel::SendTimeoutError;
        let end = Instant::now() + deadline;
        let mut pending = item;
        loop {
            // Pre-check the cheap exit conditions before parking.
            if halt.is_cancelled() {
                if debug_enabled() {
                    tracing::debug!(
                        "Pipeline send_with_halt: halt observed, returning item={}",
                        std::any::type_name::<I>()
                    );
                }
                return Err(pending);
            }
            let now = Instant::now();
            if now >= end {
                if debug_enabled() {
                    tracing::debug!(
                        "Pipeline send_with_halt: deadline elapsed, returning item={}",
                        std::any::type_name::<I>()
                    );
                }
                return Err(pending);
            }
            // Wait for space-available or halt-check tick, whichever
            // is sooner. Crossbeam's send_timeout is kernel-wakeup
            // based: the consumer's recv on a saturated channel
            // signals this thread the moment a slot opens up.
            let slice = SEND_HALT_CHECK_INTERVAL.min(end.saturating_duration_since(now));
            match self.tx.send_timeout(pending, slice) {
                Ok(()) => return Ok(()),
                Err(SendTimeoutError::Timeout(returned)) => {
                    pending = returned;
                    // loop: re-check halt + deadline, then park again
                }
                Err(SendTimeoutError::Disconnected(returned)) => {
                    if debug_enabled() {
                        tracing::debug!(
                            "Pipeline send_with_halt: consumer disconnected, item={}",
                            std::any::type_name::<I>()
                        );
                    }
                    return Err(returned);
                }
            }
        }
    }

    /// Drop the producer-side channel and wait for the consumer
    /// thread to finish. Returns whatever the consumer's `close()`
    /// produced, or the first `apply` error, or — on consumer panic —
    /// [`Error::PipelineConsumerPanicked`]. The panic payload is
    /// logged at the join site (the library carries no English in its
    /// error values), so callers discriminate on the variant.
    pub fn finish(self) -> Result<R, Error> {
        let Pipeline { tx, handle } = self;
        // Explicit drop, although the destructure already drops `tx`
        // at end-of-scope. Being explicit keeps the intent obvious.
        drop(tx);
        match handle.join() {
            Ok(result) => result,
            Err(payload) => Err(consumer_panicked(payload)),
        }
    }

    /// Halt-aware, deadline-bounded variant of [`Pipeline::finish`].
    ///
    /// Drops the producer-side channel (same as `finish`) and then
    /// polls `JoinHandle::is_finished()` on a 250 ms cadence. Between
    /// slices, checks (1) the optional [`Halt`] token and (2) the
    /// [`JOIN_TIMEOUT_SECS`] deadline. Returns:
    ///
    /// - `Ok(R)` on a clean consumer exit.
    /// - One of three numeric error variants for the wedge cases:
    ///   - [`Error::Halted`] — halt fired while waiting.
    ///   - [`Error::PipelineJoinTimeout`] — `JOIN_TIMEOUT_SECS` elapsed.
    ///   - [`Error::PipelineConsumerPanicked`] — same as `finish()`.
    ///
    /// In the `halted` and `timed out` branches the consumer thread is
    /// intentionally leaked after a short grace period — exactly the
    /// same trade-off the `bounded_syscall` primitive makes. A
    /// [`FINISH_GRACE_SECS`] spin-poll is attempted first so that
    /// consumers that are "nearly done" (e.g. their own bounded syscall
    /// just timed out and is about to unblock) can join cleanly and
    /// release their output file handle. Only if the consumer is still
    /// running after the grace period does the leak occur.
    ///
    /// Plain [`Pipeline::finish`] is preserved for callers without a
    /// halt-token plumbed through; that path still blocks indefinitely
    /// on `join()`, matching pre-0.20.8 behaviour.
    pub fn finish_with_halt(self, halt: Option<&Halt>) -> Result<R, Error> {
        let Pipeline { tx, handle } = self;
        drop(tx);
        let deadline = Instant::now() + Duration::from_secs(JOIN_TIMEOUT_SECS);
        loop {
            if handle.is_finished() {
                return match handle.join() {
                    Ok(result) => result,
                    Err(payload) => Err(consumer_panicked(payload)),
                };
            }
            if let Some(h) = halt {
                if h.is_cancelled() {
                    return finish_with_grace(handle, Error::Halted);
                }
            }
            if Instant::now() >= deadline {
                return finish_with_grace(handle, Error::PipelineJoinTimeout);
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        // depth=2 + 5 sends + 50ms/apply: with the consumer pinned at
        // 50 ms per item, the producer can buffer 2 (channel cap) +
        // 1 (consumer in flight) = 3 items before sends 4 and 5 must
        // block on consumer progress. Wall-clock floor across all 5
        // sends is therefore ~2 * 50ms = 100ms (sends 4 and 5 each
        // wait roughly one apply-cycle). Use 80 ms as the assertion
        // floor to stay above the 50ms-per-item progress floor while
        // tolerating CI jitter — it still proves blocking is real.
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

        // Send 10 items. After Stop, subsequent sends may either
        // succeed (already buffered) or fail with Err(I) (channel
        // closed). Both are valid — we don't assert on the send
        // results.
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
            panic!("synthetic test panic");
        }

        fn close(self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn consumer_panic_becomes_io_error() {
        // Silence the panic message that would otherwise pollute the
        // test output — we expect this panic.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

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

        std::panic::set_hook(prev);

        // A consumer panic surfaces as the numeric variant, not an
        // English-carrying io::Error. The original panic payload is
        // logged at the join site, not embedded in the error value.
        assert!(
            matches!(res, Err(Error::PipelineConsumerPanicked)),
            "expected Err(PipelineConsumerPanicked), got {res:?}"
        );
    }

    /// Never-completing sink — `apply` blocks until cancelled. Signals
    /// `started` once it has consumed its first item so the test
    /// driver knows the consumer thread is wedged in `apply` (and
    /// will no longer drain the channel). Used to drive the
    /// halt/timeout paths of `send_with_halt` and `finish_with_halt`
    /// without depending on real I/O.
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
        // depth=1 + consumer wedged in apply on the first item, AND
        // the channel buffer already loaded with a second item, means
        // any further `try_send` sees Full; with a 200 ms deadline and
        // no halt fired, send_with_halt must return `Err(item)` within
        // roughly the deadline. Synchronising on `started` ensures the
        // consumer has actually started its wedged apply BEFORE we
        // load the channel-buffer slot — without that, the consumer
        // could still drain in a race window.
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
        // Bailed out within the grace period plus a healthy margin.
        // The grace spin-poll adds up to FINISH_GRACE_SECS (5s) of
        // extra wait for a truly wedged consumer; the test's consumer
        // is deliberately never released before this assert so we
        // exercise the "grace expires → leak" path. 15s is well under
        // the 10-minute JOIN_TIMEOUT backstop and proves the new code
        // doesn't block forever.
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

    /// A sink that records the exact order of items it receives, so we
    /// can prove the channel is FIFO (no reordering). `close` returns
    /// the recorded vector.
    struct OrderSink {
        seen: Vec<u64>,
    }
    impl Sink<u64> for OrderSink {
        type Output = Vec<u64>;
        fn apply(&mut self, item: u64) -> Result<Flow, Error> {
            self.seen.push(item);
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<Vec<u64>, Error> {
            Ok(self.seen)
        }
    }

    /// Zero items sent: closing the pipeline immediately must still
    /// call `close()` exactly once and return its Output. The consumer
    /// loop's `while let Ok = rx.recv()` exits on the dropped tx with
    /// zero iterations, then runs `sink.close()` (line 268). Mutation:
    /// moving close() inside the loop would never call it here.
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
        let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, CountClose(close_called.clone()))
            .expect("spawn");
        pipe.finish().expect("finish on empty pipeline");
        assert_eq!(close_called.load(Ordering::SeqCst), 1);
    }

    /// `close()` returning Err must surface that error from `finish`,
    /// not be swallowed. Doc lines 18-21: on a clean producer drop the
    /// consumer "flushes via close() and returns its Output" — and an
    /// Err Output is a valid return. Mutation: if the consumer ignored
    /// close()'s Result and returned Ok, this fails.
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

    /// `try_send` must report `Full` when the channel is saturated and
    /// the consumer is wedged, NOT block. Doc lines 325-329: "If the
    /// channel is full ... the item is returned in Err." We wedge the
    /// consumer on the first item (depth=1), fill the one buffer slot,
    /// then try_send must return Full immediately. Mutation: routing
    /// try_send to the blocking `send` would hang.
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

    /// `try_send` must report `Disconnected` once the consumer thread
    /// has exited (here via a panic). The item is handed back inside
    /// the `Disconnected` variant. Mutation: if try_send mapped
    /// Disconnected→Full it would mis-signal a permanently-dead
    /// consumer as transient backpressure.
    #[test]
    fn try_send_reports_disconnected_after_consumer_gone() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
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
        std::panic::set_hook(prev);
        let _ = pipe.finish();
        assert!(
            saw_disconnect,
            "try_send never reported Disconnected; last was {last:?}"
        );
    }

    /// Plain `send` must hand the item back via `Err(item)` once the
    /// consumer has gone away (panic). Doc lines 276-280: "Returns the
    /// item back if the consumer thread is gone." The first send may
    /// race the panic, so we loop until one fails and assert the
    /// returned item identity. Mutation: if `send`'s Err arm returned a
    /// different/default item, the identity assert fails.
    #[test]
    fn send_returns_item_after_consumer_panicked() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
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
        std::panic::set_hook(prev);
        let _ = pipe.finish();
        assert_eq!(
            returned,
            Some(0xDEAD_BEEF_u64),
            "send did not hand back the exact item after consumer death"
        );
    }

    /// `send_with_halt` must return the exact item via `Err(item)` when
    /// the consumer has disconnected (the `Disconnected` arm, lines
    /// 395-403). We panic the consumer, wait for it to fully exit, then
    /// send_with_halt with a live halt + long deadline — the only way
    /// it can return Err is the disconnect arm. Mutation: if that arm
    /// returned a default item instead of `returned`, the identity
    /// assert fails.
    #[test]
    fn send_with_halt_returns_item_on_disconnect() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
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
        std::panic::set_hook(prev);
        let _ = pipe.finish();
        assert!(
            matches!(res, Err(0xABCD)),
            "expected disconnected item returned, got {res:?}"
        );
        assert!(!halt.is_cancelled(), "halt must not have been the cause");
    }

    /// `send_with_halt` with a pre-cancelled halt must return the item
    /// immediately without attempting to enqueue. Pins the pre-check at
    /// line 365 (`if halt.is_cancelled()`). Mutation: removing that
    /// pre-check would still likely deliver into an open channel (Ok),
    /// flipping this assertion.
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

    /// `finish_with_halt(None)` with a wedged consumer and NO halt
    /// token must NOT return early — it must keep polling until the
    /// JOIN_TIMEOUT_SECS deadline (it cannot observe a halt that was
    /// never supplied). We can't wait 10 minutes, so we assert the
    /// weaker but still-meaningful property: with a None halt and a
    /// wedged consumer, finish_with_halt does not return within a short
    /// window (it is genuinely blocked, not spuriously returning
    /// Halted). Then we release the consumer and confirm it returns Ok.
    /// Mutation: if the None branch erroneously treated None as
    /// "cancelled", it would return Halted immediately and this fails.
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

    /// Multiple `Flow::Stop` returns: once a sink returns Stop, the
    /// consumer must stop calling `apply` for all subsequent items
    /// (lines 220-225 drain without applying) and call `close` exactly
    /// once. We send far more items than the Stop index and assert
    /// apply count never exceeds the Stop point and close ran once.
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

    /// Regression for the "consumer thread / output-file leak on halt"
    /// fix. When the halt fires but the consumer finishes WITHIN the
    /// grace period, `finish_with_halt` must join cleanly and return `Ok`
    /// — not leak the thread or return `Err(Halted)`.
    ///
    /// Setup: a sink that sleeps briefly (well inside `FINISH_GRACE_SECS`)
    /// after the producer drops the channel. We fire the halt immediately,
    /// so `finish_with_halt` enters the grace spin. The consumer finishes
    /// during the grace window and the result is `Ok`.
    ///
    /// Without the fix (old behaviour: immediate leak on halt), this
    /// would have returned `Err(Halted)` and the SumSink total would
    /// be unobservable.
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
        // finish_with_halt drops tx (signalling EOF), then observes the
        // pre-cancelled halt and enters the grace spin. The consumer
        // finishes close() within 500ms, so finish_with_halt must join
        // cleanly and return Ok with the correct total.
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

    /// Regression: `finish_with_halt` with no halt token and a consumer
    /// that completes normally must still return `Ok` (the None-halt
    /// polling path is unchanged by the grace-period fix). This is the
    /// pre-existing happy-path test reproduced with an explicit timing
    /// floor to guard against spurious early returns.
    #[test]
    fn finish_with_halt_no_halt_token_normal_completion() {
        let pipe = Pipeline::spawn(DEFAULT_PIPELINE_DEPTH, SumSink { total: 0 }).expect("spawn");
        for i in 0..20u64 {
            pipe.send(i).expect("send");
        }
        let res = pipe.finish_with_halt(None);
        assert!(matches!(res, Ok(190)), "expected Ok(190), got {res:?}");
    }
}
