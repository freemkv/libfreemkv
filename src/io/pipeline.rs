//! Generic bounded producer/consumer pipeline.
//!
//! `Pipeline<I, R>` spawns a single consumer thread, hands it items
//! through a bounded `crossbeam_channel`, and joins it on `finish()`.
//! The consumer's behaviour is supplied by a [`Sink`] implementation:
//! `apply` is called once per item, `close` is called once at the end.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError, bounded};

use crate::error::Error;
use crate::halt::join_within;
use crate::halt::{Halt, Halted, Joined, Liveness, SendOutcome, Stall, StallTimer, WAIT_SLICE};

/// `finish_with_halt`'s join window (T7): this long with no consumer progress is
/// `PipelineJoinTimeout`. A stall window, not a total (HR1).
pub const JOIN_TIMEOUT_SECS: u64 = 600;

// Grace after a halt or a join stall (T8, D2): a RUNNING consumer gets this long, then a
// CLOSING one waits in windows of it that re-arm on each progress bump.
const FINISH_GRACE_SECS: u64 = 5;

// The join's stall window (T7) and grace (T8): a parameter so tests run in ms.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JoinTiming {
    pub(crate) join_window: Duration,
    pub(crate) grace: Duration,
}

impl Default for JoinTiming {
    fn default() -> Self {
        Self {
            join_window: Duration::from_secs(JOIN_TIMEOUT_SECS),
            grace: Duration::from_secs(FINISH_GRACE_SECS),
        }
    }
}

// Cached FREEMKV_DEBUG lookup ("1", "true" or "yes" enables per-item debug tracing) — called
// per item on the mux hot loop, so the env lock is paid once, not per call.
pub fn debug_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FREEMKV_DEBUG")
            .ok()
            .map(|v| v == "1" || v == "true" || v == "yes")
            .unwrap_or(false)
    })
}

// Converts a consumer-thread panic payload into Error::PipelineConsumerPanicked. The panic
// message is logged here for diagnostics, not baked into the error value.
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

// Consumer lifecycle state, shared between caller and consumer thread. Each transition is a
// compare-exchange out of RUNNING so abandon vs. finalise stays mutually exclusive.
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
    /// The consumer committed to finalising after the op token was cancelled: it runs
    /// `Sink::close_stopped` (the output stays `*.partial`), and gets one grace (§2.5).
    pub const CLOSING_STOPPED: u8 = 3;
}

fn join_result<R>(r: thread::Result<Result<R, Error>>) -> Result<R, Error> {
    r.unwrap_or_else(|payload| Err(consumer_panicked(payload)))
}

// The grace (T8, D2): wait `grace` for the consumer; then abandon a RUNNING one, but wait
// for a CLOSING one while its `progress` moves, `grace` re-arming on each bump (a committed
// close is never abandoned for being slow). Only a CLOSING stall leaks it.
fn finish_with_grace<R: Send + 'static>(
    handle: thread::JoinHandle<Result<R, Error>>,
    state: &Arc<AtomicU8>,
    progress: &Liveness,
    grace: Duration,
    leak_err: Error,
    on_poll: &mut dyn FnMut(),
) -> Result<R, Error> {
    let mut handle = match wait_grace(handle, grace, on_poll) {
        Ok(r) => return r,
        Err(h) => h,
    };
    // CLAIM abandonment before dropping the handle so the leaked consumer skips further
    // `apply`/`close()`. Compare-exchange, not a store: a consumer already CLOSING wins.
    if state
        .compare_exchange(
            state::RUNNING,
            state::ABANDONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
    {
        tracing::warn!(
            target: "freemkv::pipeline",
            phase = "finish_with_halt_grace_expired",
            "pipeline consumer did not finish within {}s grace period; abandoning thread \
             (output will not be finalised)",
            grace.as_secs()
        );
        // Dropping the handle detaches the thread until its kernel call returns.
        drop(handle);
        return Err(leak_err);
    }
    if state.load(Ordering::Acquire) == state::CLOSING_STOPPED {
        // T8 (D2): a close begun after the Stop gets one grace, then is leaked; it can
        // only leave `*.partial` behind.
        return grace_then_leak(handle, grace, leak_err, on_poll);
    }
    tracing::warn!(
        target: "freemkv::pipeline",
        phase = "finish_with_halt_close_in_flight",
        "pipeline consumer had already committed to finalising the output; \
         waiting for it while it makes progress"
    );
    let mut timer = StallTimer::new(grace, progress);
    loop {
        handle = match join_within(handle, WAIT_SLICE, None) {
            Joined::Done(r) => return join_result(r),
            Joined::Halted(h) | Joined::Pending(h) => h,
        };
        on_poll();
        if timer.poll(progress) == Stall::Expired {
            // A close with no progress for a whole window: leak and report the wedge.
            drop(handle);
            return Err(leak_err);
        }
    }
}

// Wait `grace` for the consumer, then leak it with `leak_err`.
fn grace_then_leak<R: Send + 'static>(
    handle: thread::JoinHandle<Result<R, Error>>,
    grace: Duration,
    leak_err: Error,
    on_poll: &mut dyn FnMut(),
) -> Result<R, Error> {
    match wait_grace(handle, grace, on_poll) {
        Ok(r) => r,
        Err(handle) => {
            drop(handle);
            Err(leak_err)
        }
    }
}

// Join within `grace` (polling `on_poll`); `Err(handle)` if it is still running.
fn wait_grace<R: Send + 'static>(
    handle: thread::JoinHandle<Result<R, Error>>,
    grace: Duration,
    on_poll: &mut dyn FnMut(),
) -> Result<Result<R, Error>, thread::JoinHandle<Result<R, Error>>> {
    // `None` = a grace past `Instant`'s range: wait unbounded.
    let end = Instant::now().checked_add(grace);
    let mut handle = handle;
    loop {
        let left = end.map_or(WAIT_SLICE, |e| e.saturating_duration_since(Instant::now()));
        handle = match join_within(handle, left.min(WAIT_SLICE), None) {
            Joined::Done(r) => return Ok(join_result(r)),
            Joined::Halted(h) | Joined::Pending(h) => h,
        };
        on_poll();
        if end.is_some_and(|e| Instant::now() >= e) {
            return Err(handle);
        }
    }
}

/// Default channel depth for callers without a specific reason to
/// pick another value. Kept conservative (4) — most callers should
/// use WRITE_PIPELINE_DEPTH instead.
pub const DEFAULT_PIPELINE_DEPTH: usize = 4;

/// Write pipeline depth: deeper than the default (16 vs 4), so the producer keeps
/// running while a slow flush (`sync_file_range`, NFS drain) blocks the consumer.
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
/// `Stop` currently has no in-tree caller (the mux highway drains to EOF; the sweep
/// moved to freemkv-engine), but it's part of the fixed `Sink` contract.
pub enum Flow {
    Continue,
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

    /// `close` for a consumer that committed to finalising only after the op was stopped
    /// (stop design §2.5, §2.6: "A post-cancel `close()` leaves `*.partial`"): finish
    /// writing, but keep the output under its `*.partial` name. Its result is the
    /// pipeline's; the caller classifies the op by its token (§2.6). Defaults to `close`,
    /// right for a sink that never renames its output.
    fn close_stopped(self) -> Result<Self::Output, Error>
    where
        Self: Sized,
    {
        self.close()
    }
}

/// Bounded producer/consumer pipeline. Holds the producer-side
/// channel and the consumer thread's join handle.
pub struct Pipeline<I: Send + 'static, R: Send + 'static> {
    tx: Sender<I>,
    handle: JoinHandle<Result<R, Error>>,
    /// Set by [`finish_with_grace`] when the grace period expires and the consumer thread is
    /// about to be leaked: it stops applying further items and does NOT call `close()`, so a
    /// leaked consumer can't finalise an output already reported as failed. One of
    /// [`state::RUNNING`] / [`state::ABANDONED`] / [`state::CLOSING`] / [`state::CLOSING_STOPPED`]; both transitions are
    /// compare-exchanges so abandoning and finalising are mutually exclusive rather than
    /// racing.
    state: Arc<AtomicU8>,
    /// Set by the consumer the moment an `apply` returns `Err`, since the
    /// consumer keeps draining afterwards (so the producer never blocks
    /// on a dead receiver) and a producer watching only `send`'s return
    /// value can't otherwise tell "consumed" from "discarded after a
    /// fatal write error". [`Pipeline::send_with_halt`] fails fast on
    /// this; [`Pipeline::consumer_failed`] exposes it to plain
    /// [`Pipeline::send`] users.
    failed: Arc<AtomicBool>,
    /// The consumer's forward progress (T7): shared with the sink's output.
    progress: Liveness,
    /// The op's token, read by the consumer when it commits to `close()` (§2.5).
    op: Arc<OnceLock<Halt>>,
}

impl<I: Send + 'static, R: Send + 'static> Pipeline<I, R> {
    /// Spawn the consumer thread with the given channel depth and [`Sink`]. Named
    /// `freemkv-pipeline-consumer`; callers that want a more specific name should use
    /// [`Pipeline::spawn_named`] instead. Returns `Error::IoError` if the OS refuses the thread
    /// spawn (resource exhaustion) rather than panicking.
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
        Self::spawn_named_with_progress(name, depth, sink, Liveness::new())
    }

    /// Like [`Pipeline::spawn_named`], with the consumer's [`Liveness`] supplied, so the
    /// sink's output (a [`WritebackFile`](crate::io::WritebackFile) given the same counter)
    /// shares it (stop design §2.10 item 3).
    pub fn spawn_named_with_progress<S: Sink<I, Output = R>>(
        name: &str,
        depth: usize,
        sink: S,
        progress: Liveness,
    ) -> Result<Self, Error> {
        let (tx, rx) = bounded::<I>(depth);
        let state = Arc::new(AtomicU8::new(state::RUNNING));
        let state_consumer = state.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let failed_consumer = failed.clone();
        let progress_consumer = progress.clone();
        let op: Arc<OnceLock<Halt>> = Arc::default();
        let op_consumer = op.clone();
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

                    let applied = sink.apply(item);
                    progress_consumer.bump();
                    match applied {
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
                        // §2.6: Done only if the commit comes before the cancel.
                        let stopped = op_consumer.get().is_some_and(Halt::is_cancelled);
                        let commit = match stopped {
                            true => state::CLOSING_STOPPED,
                            false => state::CLOSING,
                        };
                        if state_consumer
                            .compare_exchange(
                                state::RUNNING,
                                commit,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            return Err(Error::Halted);
                        }
                        progress_consumer.bump();
                        // §2.5: after the cancel the output stays `*.partial`.
                        let closed = match stopped {
                            true => sink.close_stopped(),
                            false => sink.close(),
                        };
                        progress_consumer.bump();
                        closed
                    }
                }
            })
            .map_err(|e| Error::IoError { source: e })?;

        Ok(Pipeline {
            tx,
            handle,
            state,
            failed,
            progress,
            op,
        })
    }

    /// Give the consumer the op's token (stop design §2.5): a consumer that reaches
    /// `close()` after it is cancelled runs [`Sink::close_stopped`] instead and returns
    /// that close's own result; only a close leaked after its grace gives `Halted`. The
    /// first token set wins;
    /// [`finish_with_halt`](Self::finish_with_halt) sets its `halt` if none was.
    pub fn set_op_token(&self, halt: &Halt) {
        let _ = self.op.set(halt.clone());
    }

    /// The consumer's forward-progress counter: bumped per item applied and at
    /// `close()` entry and exit; [`finish_with_halt`](Self::finish_with_halt) waits on it.
    pub fn progress(&self) -> &Liveness {
        &self.progress
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
    /// After [`Flow::Stop`] the consumer keeps draining (discarding) until the producer
    /// drops `tx`, so `send` keeps returning `Ok` — producers that need to stop pushing on
    /// `Stop` should track an independent signal (e.g. `Halt`) instead.
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

    /// Halt-aware bounded variant of [`Pipeline::send`]: blocks on consumer drain via
    /// [`Halt::send_timeout`], observing a cancel or a failed consumer within one
    /// [`WAIT_SLICE`].
    ///
    /// Returns `Ok(())` once the item lands in the channel, or `Err(item)` if the consumer
    /// disconnected or failed, the halt fired, or the deadline elapsed. NOT a `foo_with_X`
    /// variant of [`Pipeline::send`] despite the name.
    pub fn send_with_halt(&self, item: I, halt: &Halt, deadline: Duration) -> Result<(), I> {
        // `None` = a deadline past `Instant`'s range (e.g. `Duration::MAX`): unbounded.
        let end = Instant::now().checked_add(deadline);
        let mut pending = item;
        loop {
            // The consumer's `apply` failed fatally: hand the item back now, or the
            // producer reads a whole title before `finish()` reports frame one's failure.
            if self.consumer_failed() {
                return Err(self.refused(pending, "consumer apply failed"));
            }
            let slice = end.map_or(WAIT_SLICE, |end| {
                end.saturating_duration_since(Instant::now())
                    .min(WAIT_SLICE)
            });
            match halt.send_timeout(&self.tx, pending, slice) {
                Ok(SendOutcome::Sent) => return Ok(()),
                Ok(SendOutcome::TimedOut(back)) if end.is_some_and(|e| Instant::now() >= e) => {
                    return Err(self.refused(back, "deadline elapsed"));
                }
                Ok(SendOutcome::TimedOut(back)) => pending = back,
                Ok(SendOutcome::Disconnected(back)) => {
                    return Err(self.refused(back, "consumer disconnected"));
                }
                Err(Halted(back)) => return Err(self.refused(back, "halt observed")),
            }
        }
    }

    fn refused(&self, item: I, why: &str) -> I {
        if debug_enabled() {
            tracing::debug!(
                "Pipeline send_with_halt: {why}, returning item={}",
                std::any::type_name::<I>()
            );
        }
        item
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
            progress: _,
            op: _,
        } = self;
        // Explicit drop, although the destructure already drops `tx`
        // at end-of-scope. Being explicit keeps the intent obvious.
        drop(tx);
        match handle.join() {
            Ok(result) => result,
            Err(payload) => Err(consumer_panicked(payload)),
        }
    }

    /// Halt-aware, stall-bounded variant of [`Pipeline::finish`]. Drops the producer-side
    /// channel, then waits for the consumer, checking the optional [`Halt`] every
    /// [`WAIT_SLICE`]. It fails only after [`JOIN_TIMEOUT_SECS`] with no consumer progress
    /// (see [`Pipeline::progress`]), never on total time.
    ///
    /// Returns `Ok(R)` on a clean exit, or one of [`Error::Halted`],
    /// [`Error::PipelineJoinTimeout`], [`Error::PipelineConsumerPanicked`] for the wedge cases
    /// (leaks the consumer after the grace). NOT a `foo_with_X` variant of [`Pipeline::finish`].
    pub fn finish_with_halt(self, halt: Option<&Halt>) -> Result<R, Error> {
        self.finish_with_halt_timing(halt, JoinTiming::default())
    }

    // `finish_with_halt_timing`, calling `on_poll` on every slice of the wait (the
    // driver forwards flush progress from it, §4.5).
    pub(crate) fn finish_with_halt_observed(
        self,
        halt: Option<&Halt>,
        timing: JoinTiming,
        on_poll: &mut dyn FnMut(),
    ) -> Result<R, Error> {
        let Pipeline {
            tx,
            handle,
            state,
            failed: _,
            progress,
            op,
        } = self;
        // Before the consumer can commit to `close()`: it must see the op's token.
        if let Some(h) = halt {
            let _ = op.set(h.clone());
        }
        drop(tx);
        // T7: a stall window over the consumer's progress, not a total from here.
        let mut timer = StallTimer::new(timing.join_window, &progress);
        let mut handle = handle;
        loop {
            handle = match join_within(handle, WAIT_SLICE, halt) {
                Joined::Done(r) => return join_result(r),
                Joined::Halted(h) => {
                    let (g, e) = (timing.grace, Error::Halted);
                    return finish_with_grace(h, &state, &progress, g, e, on_poll);
                }
                Joined::Pending(h) => h,
            };
            on_poll();
            if timer.poll(&progress) == Stall::Expired {
                let (g, e) = (timing.grace, Error::PipelineJoinTimeout);
                return finish_with_grace(handle, &state, &progress, g, e, on_poll);
            }
        }
    }

    // `finish_with_halt` with its windows as parameters (T7, T8).
    pub(crate) fn finish_with_halt_timing(
        self,
        halt: Option<&Halt>,
        timing: JoinTiming,
    ) -> Result<R, Error> {
        self.finish_with_halt_observed(halt, timing, &mut || {})
    }
}

#[cfg(test)]
mod stop_tests;

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;
