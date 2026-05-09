//! Generic bounded producer/consumer pipeline.
//!
//! `Pipeline<I, R>` spawns a single consumer thread, hands it items
//! through a bounded `mpsc::sync_channel`, and joins it on `finish()`.
//! The consumer's behaviour is supplied by a [`Sink`] implementation:
//! `apply` is called once per item, `close` is called once at the end.
//!
//! Three call sites in libfreemkv have grown a producer/consumer split
//! independently — sweep already has one (in `disc/sweep_pipeline.rs`),
//! patch and mux do not. 0.18 collapses all three onto this primitive.
//! See `freemkv-private/memory/0_18_redesign.md` for the full picture.
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
//!   `Error::IoError { source: io::Error::other(...) }`.
//!
//! ## Dead-code suppression
//!
//! The `Pipeline` / `Sink` / `Flow` / `DEFAULT_PIPELINE_DEPTH` /
//! `WRITE_THROUGH_DEPTH` items are crate-internal API today (the
//! parent `io` module is `pub(crate)`) but have no in-tree callers
//! in this slice — sweep is still on `disc/sweep_pipeline.rs`, patch
//! and mux still have no pipeline at all. Wiring them up is the
//! next slice of the 0.18 redesign. The `#[allow]` below is removed
//! once any of those three call sites lands on this primitive.

#![allow(dead_code)]

use std::io;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use crate::error::Error;

/// Default channel depth for callers without a specific reason to
/// pick another value.
///
/// Empirically tuned for sweep and mux — both want enough slack that
/// short consumer stalls don't immediately back up onto the producer,
/// but not so much that a producer outpacing the consumer accumulates
/// arbitrary buffered work. `4` matches the depth `disc/sweep_pipeline.rs`
/// has used since 0.17.11. Patch should usually use
/// [`WRITE_THROUGH_DEPTH`] (`1`) instead — write-through gives clean
/// back-pressure between every read attempt and the matching write,
/// which matters when the consumer is updating the mapfile in lockstep.
pub const DEFAULT_PIPELINE_DEPTH: usize = 4;

/// Channel depth for write-through pipelines. Each `send` fully
/// drains before the next can enqueue. Use this when the producer
/// must observe consumer side-effects (e.g. mapfile state) before
/// emitting the next item.
pub const WRITE_THROUGH_DEPTH: usize = 1;

/// Outcome of [`Sink::apply`]: either keep feeding items
/// ([`Flow::Continue`]), or stop the pipeline early and run `close()`
/// ([`Flow::Stop`]).
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
}

/// Bounded producer/consumer pipeline. Holds the producer-side
/// channel and the consumer thread's join handle.
pub struct Pipeline<I: Send + 'static, R: Send + 'static> {
    tx: SyncSender<I>,
    handle: JoinHandle<Result<R, Error>>,
}

impl<I: Send + 'static, R: Send + 'static> Pipeline<I, R> {
    /// Spawn the consumer thread with the given channel depth and
    /// [`Sink`].
    ///
    /// The thread is named `freemkv-pipeline-consumer` so it shows up
    /// distinctly in stack traces and `top -H`. Returns an
    /// `Error::IoError` if the OS refuses the thread spawn (resource
    /// exhaustion); callers already operate in fallible context, so
    /// this is propagated rather than panicked.
    pub fn spawn<S: Sink<I, Output = R>>(depth: usize, sink: S) -> Result<Self, Error> {
        let (tx, rx) = sync_channel::<I>(depth);
        let handle = thread::Builder::new()
            .name("freemkv-pipeline-consumer".into())
            .spawn(move || -> Result<R, Error> {
                let mut sink = sink;
                let mut first_err: Option<Error> = None;
                let mut stopped = false;

                while let Ok(item) = rx.recv() {
                    if first_err.is_some() || stopped {
                        // Drain remaining items so the producer never
                        // blocks on a dead receiver. `apply` is not
                        // called once we've decided to stop.
                        continue;
                    }
                    match sink.apply(item) {
                        Ok(Flow::Continue) => {}
                        Ok(Flow::Stop) => {
                            stopped = true;
                        }
                        Err(e) => {
                            first_err = Some(e);
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
        self.tx.send(item).map_err(|e| e.0)
    }

    /// Drop the producer-side channel and wait for the consumer
    /// thread to finish. Returns whatever the consumer's `close()`
    /// produced, or the first `apply` error, or — on consumer panic —
    /// an `Error::IoError` whose source is `io::Error::other(...)`
    /// with a "pipeline consumer panicked: <payload>" message
    /// (callers can match on the constant prefix).
    pub fn finish(self) -> Result<R, Error> {
        let Pipeline { tx, handle } = self;
        // Explicit drop, although the destructure already drops `tx`
        // at end-of-scope. Being explicit keeps the intent obvious.
        drop(tx);
        match handle.join() {
            Ok(result) => result,
            Err(payload) => {
                // Preserve the original panic message when the
                // consumer's panic payload was a `&str` or `String`
                // (the two stdlib formats that `panic!` produces).
                // Anything else falls back to "(no message)".
                let msg = payload
                    .downcast_ref::<&'static str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(|s| s.as_str()))
                    .unwrap_or("(no message)");
                Err(Error::IoError {
                    source: io::Error::other(format!("pipeline consumer panicked: {msg}")),
                })
            }
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

        match res {
            Err(Error::IoError { source }) => {
                let msg = source.to_string();
                // Constant prefix lets callers match without parsing
                // the variable payload tail.
                assert!(
                    msg.contains("pipeline consumer panicked"),
                    "expected constant panic prefix, got: {msg}"
                );
                // The original `panic!` payload (a `&'static str`) must
                // be preserved — without the downcast the message
                // would just be the prefix.
                assert!(
                    msg.contains("synthetic test panic"),
                    "expected original panic payload, got: {msg}"
                );
            }
            other => panic!("expected Err(IoError), got {other:?}"),
        }
    }
}
