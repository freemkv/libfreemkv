//! Generic bounded producer/consumer pipeline.
//!
//! `Pipeline<I, R>` spawns a single consumer thread, hands it items
//! through a bounded `mpsc::sync_channel`, and joins it on `finish()`.
//! The consumer's behaviour is supplied by a [`Sink`] implementation:
//! `apply` is called once per item, `close` is called once at the end.
//!
//! Three call sites in libfreemkv want a producer/consumer split —
//! sweep (migrated to `disc/sweep.rs::SweepSink`), patch, and mux.
//! 0.18 collapses all three onto this primitive; sweep is in,
//! patch and mux migrate in later 0.18 slices.
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
//! Both `Disc::sweep` (`DEFAULT_PIPELINE_DEPTH`, `spawn_named`,
//! `try_send`) and `Disc::patch` (`WRITE_THROUGH_DEPTH`, `Flow::Stop`)
//! are now in-tree callers (0.18 round 2). The targeted `#[allow]`
//! markers below cover the corners of the API not exercised by either
//! call site (e.g. `Pipeline::spawn` without a name; `Flow::Stop`
//! return path from sweep's sink — sweep always consumes the producer's
//! full work-list). Drop the allows once mux migrates.

use std::io;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use crate::error::Error;

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
/// emitting the next item.
#[allow(dead_code)]
pub const WRITE_THROUGH_DEPTH: usize = 1;

/// Outcome of [`Sink::apply`]: either keep feeding items
/// ([`Flow::Continue`]), or stop the pipeline early and run `close()`
/// ([`Flow::Stop`]).
///
/// `Stop` has no in-tree caller in this slice — sweep never returns
/// it (it always processes the producer's full work-list before the
/// channel is dropped). Patch and mux are the intended consumers and
/// migrate in later 0.18 slices. The variant ships now so the contract
/// is fixed; the targeted `#[allow]` is removed when patch lands.
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
    tx: SyncSender<I>,
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
    /// thread shows up as `freemkv-sweep-consumer`; this function has
    /// no in-tree caller yet. Patch and mux migrate in later 0.18
    /// slices. The targeted `#[allow]` is removed when one of them
    /// lands on the default name.
    #[allow(dead_code)]
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
        let (tx, rx) = sync_channel::<I>(depth);
        let handle = thread::Builder::new()
            .name(name.into())
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

    /// Non-blocking variant of [`Pipeline::send`]. If the channel is
    /// full or the consumer has hung up, the item is returned in
    /// `Err`. Useful for best-effort signalling (e.g. sweep's
    /// throttled `StatsRequest`) where dropping the message is
    /// preferable to blocking the producer.
    pub fn try_send(&self, item: I) -> Result<(), std::sync::mpsc::TrySendError<I>> {
        self.tx.try_send(item)
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
