//! Generic bounded producer/consumer pipeline.
//!
//! `Pipeline<I, R>` spawns a single consumer thread, hands it items
//! through a bounded `crossbeam_channel`, and joins it on `finish()`.
//! The consumer's behaviour is supplied by a [`Sink`] implementation:
//! `apply` is called once per item, `close` is called once at the end.
//!
//! See docs/pipeline.md for full cancellation/error semantics and the
//! `FREEMKV_DEBUG=1` debug-logging switch.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError, bounded};

use crate::error::Error;
use crate::halt::Halt;

// Deadline for finish_with_halt's polling join. See docs/pipeline.md —
// it's a backstop for a wedged kernel call, not a normal timeout.
pub const JOIN_TIMEOUT_SECS: u64 = 600;

// Grace period after a halt/timeout fires in finish_with_halt, to let a
// "nearly done" consumer join cleanly. See docs/pipeline.md.
const FINISH_GRACE_SECS: u64 = 5;

// Halt-check cadence for the send loop; aliases crate::halt::POLL_INTERVAL.
// See docs/pipeline.md — SEND_HALT_CHECK_INTERVAL.
use crate::halt::POLL_INTERVAL;
const SEND_HALT_CHECK_INTERVAL: Duration = POLL_INTERVAL;

// Cached FREEMKV_DEBUG=1 lookup — called per item on the mux hot loop,
// so the env lock is paid once, not per call.
pub fn debug_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FREEMKV_DEBUG")
            .ok()
            .map(|v| v == "1" || v == "true" || v == "yes")
            .unwrap_or(false)
    })
}

// Converts a consumer-thread panic payload into Error::PipelineConsumerPanicked.
// The panic message is logged here for diagnostics, not baked into the error
// value. See docs/pipeline.md — consumer_panicked.
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

// Consumer lifecycle state, shared between caller and consumer thread. Each
// transition is a compare-exchange out of RUNNING so abandon vs. finalise
// stays mutually exclusive. See docs/pipeline.md — `state` module.
mod state {
    /// Consumer is running; neither side has committed yet.
    pub const RUNNING: u8 = 0;
    /// The caller gave up on the consumer and will report failure — the consumer
    /// must NOT finalise the output.
    pub const ABANDONED: u8 = 1;
    /// The consumer has committed to `close()` (finalising the output). The caller
    /// can no longer abandon it; it must wait for the result it is about to
    /// produce.
    pub const CLOSING: u8 = 2;
}

// Spin-polls handle.is_finished() for `grace` before accepting the thread
// leak, so a "nearly done" consumer still joins cleanly. See docs/pipeline.md
// — finish_with_grace.
fn finish_with_grace<R: Send + 'static>(
    handle: thread::JoinHandle<Result<R, Error>>,
    state: &Arc<AtomicU8>,
    grace: Duration,
    leak_err: Error,
) -> Result<R, Error> {
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if handle.is_finished() {
            return match handle.join() {
                Ok(result) => result,
                Err(payload) => Err(consumer_panicked(payload)),
            };
        }
        thread::sleep(POLL_INTERVAL);
    }
    // Grace expired. CLAIM abandonment before dropping the handle so the leaked consumer
    // skips further `apply`/`close()` once its wedged syscall returns. Compare-exchange,
    // not a store: if it already committed to `close()`, wait for its result instead.
    if state
        .compare_exchange(
            state::RUNNING,
            state::ABANDONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_err()
    {
        tracing::warn!(
            target: "freemkv::pipeline",
            phase = "finish_with_halt_close_in_flight",
            "pipeline consumer had already committed to finalising the output; \
             waiting for it rather than reporting an unfinalised output"
        );
        let close_deadline = Instant::now() + grace;
        while Instant::now() < close_deadline {
            if handle.is_finished() {
                return match handle.join() {
                    Ok(result) => result,
                    Err(payload) => Err(consumer_panicked(payload)),
                };
            }
            thread::sleep(POLL_INTERVAL);
        }
        // Still finalising after a second grace window: leak and report the wedge.
        // The output may end up finalised by the leaked thread — but that is now a
        // wedged-`close()` case, not the check-then-finalise race.
        drop(handle);
        return Err(leak_err);
    }
    tracing::warn!(
        target: "freemkv::pipeline",
        phase = "finish_with_halt_grace_expired",
        "pipeline consumer did not finish within {}s grace period; abandoning thread \
         (output will not be finalised)",
        FINISH_GRACE_SECS
    );
    // Dropping `handle` without joining detaches the thread; the consumer keeps
    // running until its kernel call returns or the process exits. This is the
    // intentional "leak" from `finish_with_halt`'s contract, bounded by `abandoned`.
    drop(handle);
    Err(leak_err)
}

/// Default channel depth for callers without a specific reason to
/// pick another value. Kept conservative (4) — most callers should
/// use WRITE_PIPELINE_DEPTH instead.
pub const DEFAULT_PIPELINE_DEPTH: usize = 4;

/// Write pipeline depth. Smaller buffer reduces backpressure risk when
/// sync_file_range blocks; prevents producer from accumulating too much
/// work while consumer waits for NFS to drain.
pub const WRITE_PIPELINE_DEPTH: usize = 16;

/// Channel depth for write-through pipelines. Each `send` fully
/// drains before the next can enqueue. Use this when the producer
/// must observe consumer side-effects (e.g. mapfile state) before
/// emitting the next item. Used by `freemkv_engine::recovery::patch` — the
/// recovery strategy moved to that crate in 1.6.0, so there is no `patch` here.
pub const WRITE_THROUGH_DEPTH: usize = 1;

/// Outcome of [`Sink::apply`]: either keep feeding items
/// ([`Flow::Continue`]), or stop the pipeline early and run `close()`
/// ([`Flow::Stop`]).
///
/// `Stop` currently has no in-tree caller (sweep always processes its
/// full work-list; the mux highway drains to EOF), but it's part of the
/// fixed `Sink` contract, so `#[allow(dead_code)]` is intentional.
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
    /// Set by [`finish_with_grace`] when the grace period expires and the
    /// consumer thread is about to be leaked: it stops applying further
    /// items and does NOT call `close()`, so a leaked consumer can't
    /// finalise an output already reported as failed. One of
    /// [`state::RUNNING`] / [`state::ABANDONED`] / [`state::CLOSING`];
    /// both transitions are compare-exchanges so abandoning and
    /// finalising are mutually exclusive rather than racing. See
    /// docs/pipeline.md for the full race this prevents.
    state: Arc<AtomicU8>,
    /// Set by the consumer the moment an `apply` returns `Err`, since the
    /// consumer keeps draining afterwards (so the producer never blocks
    /// on a dead receiver) and a producer watching only `send`'s return
    /// value can't otherwise tell "consumed" from "discarded after a
    /// fatal write error". [`Pipeline::send_with_halt`] fails fast on
    /// this; [`Pipeline::consumer_failed`] exposes it to plain
    /// [`Pipeline::send`] users.
    failed: Arc<AtomicBool>,
}

impl<I: Send + 'static, R: Send + 'static> Pipeline<I, R> {
    /// Spawn the consumer thread with the given channel depth and
    /// [`Sink`]. Named `freemkv-pipeline-consumer`; callers that want a
    /// more specific name should use [`Pipeline::spawn_named`] instead.
    /// Returns `Error::IoError` if the OS refuses the thread spawn
    /// (resource exhaustion) rather than panicking. See docs/pipeline.md
    /// for which in-crate callers use this vs. `spawn_named`.
    pub fn spawn<S: Sink<I, Output = R>>(depth: usize, sink: S) -> Result<Self, Error> {
        Self::spawn_named("freemkv-pipeline-consumer", depth, sink)
    }

    /// Like [`Pipeline::spawn`] but lets the caller supply the
    /// consumer thread's name. Useful when several pipelines run in
    /// the same process and stack traces / `top -H` need to tell them
    /// apart (e.g. `freemkv-mux-consumer`).
    pub fn spawn_named<S: Sink<I, Output = R>>(
        name: &str,
        depth: usize,
        sink: S,
    ) -> Result<Self, Error> {
        let (tx, rx) = bounded::<I>(depth);
        let state = Arc::new(AtomicU8::new(state::RUNNING));
        let state_consumer = state.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let failed_consumer = failed.clone();
        let handle = thread::Builder::new()
            .name(name.into())
            .spawn(move || -> Result<R, Error> {
                let mut sink = sink;
                let mut first_err: Option<Error> = None;
                let mut stopped = false;

                // Rolling apply-throughput summary: the per-item "apply: OK" line was
                // 99% of the mux log, so collapse it into a periodic summary (count,
                // avg ms, items/s) every ~5s. Slow-apply STALL events stay visible below.
                let mut summary_count: u64 = 0;
                let mut summary_nanos: u128 = 0;
                let mut summary_since = Instant::now();
                const SUMMARY_INTERVAL: Duration = Duration::from_secs(5);

                while let Ok(item) = rx.recv() {
                    let debug = debug_enabled();
                    if debug {
                        tracing::debug!("Pipeline receive: item={}", std::any::type_name::<I>());
                    }

                    // Abandoned (grace expired, JoinHandle dropped): keep draining so a
                    // still-alive producer never blocks on a dead receiver, but touch
                    // the output no further. Post-loop check returns error, skips close().
                    if state_consumer.load(Ordering::Acquire) == state::ABANDONED {
                        continue;
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
                            // Publish the failure so the producer stops feeding a dead
                            // write side instead of learning at `finish()`, after reading
                            // the rest of the disc. `Release` pairs with `send_with_halt`.
                            failed_consumer.store(true, Ordering::Release);
                        }
                    }

                    if let Some(start) = apply_start {
                        let apply_elapsed = start.elapsed();
                        if apply_elapsed > Duration::from_millis(100) {
                            // STALL event — a single slow apply. Keep it visible:
                            // its presence is a signal, not per-frame noise.
                            tracing::debug!(
                                "Pipeline apply: took {:.2}s, item={}",
                                apply_elapsed.as_secs_f64(),
                                std::any::type_name::<I>()
                            );
                        }
                        // Benign per-item OK: roll into the periodic summary
                        // rather than logging one line per frame.
                        summary_count += 1;
                        summary_nanos += apply_elapsed.as_nanos();
                        if summary_since.elapsed() >= SUMMARY_INTERVAL && summary_count > 0 {
                            let secs = summary_since.elapsed().as_secs_f64();
                            let avg_ms = (summary_nanos as f64 / summary_count as f64) / 1_000_000.0;
                            tracing::debug!(
                                "Pipeline apply summary: {} items in {:.1}s, avg {:.3}ms, {:.0} items/s, type={}",
                                summary_count,
                                secs,
                                avg_ms,
                                summary_count as f64 / secs.max(1e-9),
                                std::any::type_name::<I>()
                            );
                            summary_count = 0;
                            summary_nanos = 0;
                            summary_since = Instant::now();
                        }
                    }
                }

                // Flush the residual apply-summary tail at end-of-stream so the
                // last partial window's item count isn't silently dropped.
                if summary_count > 0 && debug_enabled() {
                    let secs = summary_since.elapsed().as_secs_f64();
                    let avg_ms = (summary_nanos as f64 / summary_count as f64) / 1_000_000.0;
                    tracing::debug!(
                        "Pipeline apply summary (final): {} items in {:.1}s, avg {:.3}ms, type={}",
                        summary_count,
                        secs,
                        avg_ms,
                        std::any::type_name::<I>()
                    );
                }

                // Final abandonment check: a consumer wedged in a blocking `apply` write
                // can outlive the producer dropping `tx`, landing here via `recv -> Err`.
                // Skip `close()` if abandoned meanwhile — it would race the write.
                match first_err {
                    // No `close()` on this path, so there is nothing to claim —
                    // just report, unless the caller has already given up on us.
                    Some(e) => {
                        if state_consumer.load(Ordering::Acquire) == state::ABANDONED {
                            Err(Error::Halted)
                        } else {
                            Err(e)
                        }
                    }
                    // CLAIM the finalise: a plain load could race the caller marking
                    // `abandoned`, letting `close()` finalise output already reported
                    // interrupted. Compare-exchange: loser skips `close()`, waits for winner.
                    None => {
                        if state_consumer
                            .compare_exchange(
                                state::RUNNING,
                                state::CLOSING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            return Err(Error::Halted);
                        }
                        sink.close()
                    }
                }
            })
            .map_err(|e| Error::IoError { source: e })?;

        Ok(Pipeline {
            tx,
            handle,
            state,
            failed,
        })
    }

    /// Whether the consumer's `apply` has already failed fatally.
    ///
    /// The consumer keeps draining the channel after an `apply` error (so the
    /// producer never blocks on a dead receiver), which means `send` keeps
    /// succeeding and a producer has no other way to tell that everything it feeds
    /// is being discarded. A long-running producer — the mux frame pump reading a
    /// 60 GB title off an optical drive — should check this and unwind instead of
    /// reading the rest of the disc for a write that has already failed.
    /// [`Pipeline::send_with_halt`] checks it automatically.
    pub fn consumer_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Push one item. Blocks if the channel is full — that's the
    /// back-pressure the whole primitive exists to provide. Returns
    /// the item back if the consumer thread is gone (panicked or
    /// already returned).
    ///
    /// After [`Flow::Stop`], `send` silently buffers until the channel
    /// fills, then returns `Err(item)` once the consumer drops its
    /// receiver — producers that need to stop pushing on `Stop` should
    /// track an independent signal (e.g. `Halt`) instead.
    pub fn send(&self, item: I) -> Result<(), I> {
        // Only timestamp when debug tracing is on — `send` runs per
        // item on the mux highway hot path.
        let start = debug_enabled().then(Instant::now);
        match self.tx.send(item) {
            Ok(()) => {
                if let Some(start) = start {
                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        // BLOCKED event — back-pressure stall. Keep visible.
                        tracing::debug!(
                            "Pipeline send: blocked {:.2}s, item={}",
                            elapsed.as_secs_f64(),
                            std::any::type_name::<I>()
                        );
                    } else {
                        // Benign per-item OK: trace-level (L4) only; the
                        // apply-side rolling summary carries throughput.
                        tracing::trace!(
                            "Pipeline send: OK in {:.3}ms",
                            elapsed.as_secs_f64() * 1000.0
                        );
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
                        tracing::debug!(
                            "Pipeline send: failed after {:.3}ms",
                            elapsed.as_secs_f64() * 1000.0
                        );
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

    /// Halt-aware bounded variant of [`Pipeline::send`]. Uses
    /// [`crossbeam_channel::Sender::send_timeout`] so the producer
    /// BLOCKS on consumer drain rather than polling, in slices of
    /// [`SEND_HALT_CHECK_INTERVAL`].
    ///
    /// Returns `Ok(())` once the item lands in the channel, or
    /// `Err(item)` if the consumer disconnected, the halt fired, or the
    /// deadline elapsed. NOT a `foo_with_X` variant of
    /// [`Pipeline::send`] despite the name — see docs/pipeline.md.
    pub fn send_with_halt(&self, item: I, halt: &Halt, deadline: Duration) -> Result<(), I> {
        use crossbeam_channel::SendTimeoutError;
        let end = Instant::now() + deadline;
        let mut pending = item;
        loop {
            // The consumer's `apply` failed fatally: hand the item back now, since
            // otherwise sends keep succeeding and the producer reads a whole UHD
            // title before learning at `finish()` the write died on frame one.
            if self.consumer_failed() {
                if debug_enabled() {
                    tracing::debug!(
                        "Pipeline send_with_halt: consumer apply failed, returning item={}",
                        std::any::type_name::<I>()
                    );
                }
                return Err(pending);
            }
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
            // Wait for space-available or halt-check tick, whichever is sooner.
            // send_timeout is kernel-wakeup based: recv on a saturated channel
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
        let Pipeline {
            tx,
            handle,
            state: _,
            failed: _,
        } = self;
        // Explicit drop, although the destructure already drops `tx`
        // at end-of-scope. Being explicit keeps the intent obvious.
        drop(tx);
        match handle.join() {
            Ok(result) => result,
            Err(payload) => Err(consumer_panicked(payload)),
        }
    }

    /// Halt-aware, deadline-bounded variant of [`Pipeline::finish`].
    /// Drops the producer-side channel, then polls
    /// `JoinHandle::is_finished()`, checking the optional [`Halt`]
    /// token and the [`JOIN_TIMEOUT_SECS`] deadline between slices.
    ///
    /// Returns `Ok(R)` on a clean exit, or one of [`Error::Halted`],
    /// [`Error::PipelineJoinTimeout`], [`Error::PipelineConsumerPanicked`]
    /// for the wedge cases (leaks the consumer after a grace spin). NOT
    /// a `foo_with_X` variant of [`Pipeline::finish`] — see docs/pipeline.md.
    pub fn finish_with_halt(self, halt: Option<&Halt>) -> Result<R, Error> {
        let Pipeline {
            tx,
            handle,
            state,
            failed: _,
        } = self;
        drop(tx);
        let deadline = Instant::now() + Duration::from_secs(JOIN_TIMEOUT_SECS);
        loop {
            if handle.is_finished() {
                return match handle.join() {
                    Ok(result) => result,
                    Err(payload) => Err(consumer_panicked(payload)),
                };
            }
            if let Some(h) = halt
                && h.is_cancelled()
            {
                return finish_with_grace(
                    handle,
                    &state,
                    Duration::from_secs(FINISH_GRACE_SECS),
                    Error::Halted,
                );
            }
            if Instant::now() >= deadline {
                return finish_with_grace(
                    handle,
                    &state,
                    Duration::from_secs(FINISH_GRACE_SECS),
                    Error::PipelineJoinTimeout,
                );
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
    // See docs/pipeline.md — empty_pipeline_still_calls_close.
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

    // close() returning Err must surface from finish(), not be swallowed.
    // See docs/pipeline.md — close_error_propagates_from_finish.
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

    // try_send must report Full when saturated and the consumer is wedged,
    // NOT block. See docs/pipeline.md — try_send_reports_full_when_saturated.
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

    // try_send must report Disconnected once the consumer has exited (via
    // panic here). See docs/pipeline.md — try_send_reports_disconnected.
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

    // Plain send must hand the item back via Err(item) once the consumer
    // has panicked. See docs/pipeline.md — send_returns_item_after_panic.
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

    // send_with_halt must return the exact item via Err(item) on the
    // Disconnected arm. See docs/pipeline.md — send_with_halt_on_disconnect.
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

    // finish_with_halt(None) + wedged consumer must NOT spuriously return
    // Halted (no halt supplied to observe). See docs/pipeline.md —
    // finish_with_halt_none_does_not_spuriously_halt.
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

    // Regression: halt fires but consumer finishes WITHIN the grace period —
    // finish_with_halt must join cleanly and return Ok. See docs/pipeline.md.
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

    // Regression: a leaked consumer must not finalise an abandoned output —
    // once released it must skip close(). See docs/pipeline.md.
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

    // Companion to the above: the abandonment guard must NOT fire when the
    // consumer finishes inside the grace window. See docs/pipeline.md.
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

    // A fatal apply error must become visible to the PRODUCER, not only to
    // finish() — see docs/pipeline.md for the ENOSPC-on-60GB-mux scenario.
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

    // The abandon/finalise race: a consumer already committed to close() when
    // grace expires must be waited for, not abandoned. See docs/pipeline.md.
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

        // Finish the close only AFTER the first grace window has expired, so the
        // caller genuinely reaches the abandon decision with a close in flight.
        let rel = release.clone();
        thread::spawn(move || {
            // Release inside the second grace window. Old 600ms/300ms-grace intervals
            // left no margin and flaked on loaded runners; scaled up so windows end
            // at ~1.0-1.25s and ~2.0-2.25s, releasing at 1.6s for ~350ms slack.
            thread::sleep(Duration::from_millis(1600));
            rel.store(true, Ordering::SeqCst);
        });

        let grace = Duration::from_secs(1);
        let res = finish_with_grace(handle, &state, grace, Error::Halted);
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
}
