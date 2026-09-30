//! `BytePrefetcher` — `std::io::Read` analogue of [`crate::sector::PrefetchedSectorSource`].
//! Spawns a producer thread that fills a bounded pool of `Vec<u8>` chunks from the underlying
//! reader and ships them through a channel; the consumer recycles emptied buffers back so the
//! producer re-fills in place, for zero allocations and zero cross-thread frees in the hot
//! loop. Works for any stream whose source is an `io::Read`, not just a `SectorSource`.

use crate::halt::{Halt, Joined, Recv, SendOutcome, join_within};
use crossbeam_channel::{Receiver, Sender, bounded};
use std::io::Read;
use std::thread::JoinHandle;
use std::time::Duration;

/// Items flowing through the forward channel.
pub type Batch = std::io::Result<Vec<u8>>;

/// Forward channel depth — how many filled buffers the producer can
/// stay ahead by. Two is enough to absorb a moderate consumer stall
/// without piling up bytes.
const FORWARD_DEPTH: usize = 2;

/// Recycle channel depth = forward + 1 so the producer always has at
/// least one buffer to fill while the consumer holds one.
const RECYCLE_DEPTH: usize = FORWARD_DEPTH + 1;

/// Default chunk size — 16 MiB matches the ISO-mux sector batch and
/// is large enough that per-chunk overhead is amortised; small
/// enough that the in-flight memory footprint stays bounded.
pub const DEFAULT_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Returned from [`BytePrefetcher::into_channels`]. Owns the
/// producer-thread join handle so dropping the shell joins the
/// producer.
///
/// Drop waits a bounded grace for the producer, then detaches it (a `read()` that never
/// returns cannot be interrupted). For a prompt exit, drop the forward receiver and recycle
/// sender first, or cancel the [`Halt`] passed to [`BytePrefetcher::new`].
pub struct PrefetchShell {
    producer: Option<JoinHandle<()>>,
}

// How long Drop waits for the producer before detaching it.
const DROP_GRACE: Duration = if cfg!(test) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(5)
};

// Join the producer within `DROP_GRACE`; a producer still blocked in `read()` is detached.
fn join_or_detach(h: JoinHandle<()>) {
    if !matches!(join_within(h, DROP_GRACE, None), Joined::Done(_)) {
        tracing::warn!(target: "freemkv::io", "byte prefetch producer blocked in read; detached");
    }
}

impl Drop for PrefetchShell {
    fn drop(&mut self) {
        if let Some(h) = self.producer.take() {
            join_or_detach(h);
        }
    }
}

/// Spawned byte prefetcher. Drop joins the producer thread (bounded; see [`PrefetchShell`]).
pub struct BytePrefetcher {
    // Non-`Option`: `into_channels`/`Drop` swap in a disconnected stand-in (dead-channel
    // `mem::replace`, as `sector::PrefetchedSectorSource` does), so a missing `rx` can
    // never read as a silent, truncating clean EOF.
    rx: Receiver<Batch>,
    recycle_tx: Sender<Vec<u8>>,
    producer: Option<JoinHandle<()>>,
}

impl BytePrefetcher {
    /// Spawn the producer thread. `reader` must be `Send` because it
    /// moves into the thread. `chunk_bytes` is the size of each
    /// recycled buffer; pick the natural batch size of the
    /// downstream demuxer (16 MiB for the BD-TS mux pipeline).
    pub fn new<R: Read + Send + 'static>(
        mut reader: R,
        chunk_bytes: usize,
        halt: Option<Halt>,
    ) -> std::io::Result<Self> {
        // A zero-length chunk would read Ok(0) at once: a silent empty stream.
        if chunk_bytes == 0 {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let (tx, rx) = bounded::<Batch>(FORWARD_DEPTH);
        let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(RECYCLE_DEPTH);

        // Seed the recycle pool; otherwise the first `recycle_rx.recv()` blocks
        // forever since no consumer has returned a buffer yet.
        for _ in 0..RECYCLE_DEPTH {
            let _ = recycle_tx.send(vec![0u8; chunk_bytes]);
        }

        // A never-cancelled stand-in keeps one halt-aware code path without a token.
        let wait = halt.unwrap_or_default();
        let producer = std::thread::Builder::new()
            .name("freemkv-byte-prefetch".into())
            .spawn(move || {
                // catch_unwind: a clean exit drops `tx` (demux reads RecvError as EOF),
                // but a panic sends an error sentinel first so demux gets a typed error
                // instead of finalizing a truncated mux as success.
                let body = std::panic::AssertUnwindSafe(|| {
                    // Liveness heartbeat: a stalled consumer or wedged reader shows up
                    // as the beat going silent. Total is unknown, so `pos` is cumulative.
                    let mut hb = crate::progress::Heartbeat::new("byte_prefetch");
                    let mut produced_bytes: u64 = 0;
                    loop {
                        hb.tick(produced_bytes, 0);
                        // Halt-aware: a cancel does not disconnect the channel, so a
                        // plain recv() would never re-reach the check. Disconnected =
                        // the consumer dropped both channels.
                        let Ok(Recv::Item(mut buf)) = wait.recv_timeout(&recycle_rx, Duration::MAX)
                        else {
                            return;
                        };
                        // Regrow to chunk_bytes (a short read may have truncated len): safe
                        // resize, was `unsafe set_len` (GHSA-j8ww-f5fg-9pmh in `sector::prefetched`).
                        buf.resize(chunk_bytes, 0);
                        // Short reads are valid: truncate so the consumer sees only what arrived.
                        let n = loop {
                            match reader.read(&mut buf[..]) {
                                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                                r => break r,
                            }
                        };
                        let n = match n {
                            Ok(0) => return, // EOF — drop tx, consumer sees RecvError
                            Ok(n) => n,
                            Err(e) => {
                                let _ = wait.send_timeout(&tx, Err(e), Duration::MAX);
                                return;
                            }
                        };
                        produced_bytes += n as u64;
                        buf.truncate(n);
                        // Hand off the filled buffer, re-polling halt on
                        // each timeout slice so a cancel can interrupt a
                        // producer parked on a saturated forward channel.
                        let sent = wait.send_timeout(&tx, Ok(buf), Duration::MAX);
                        if !matches!(sent, Ok(SendOutcome::Sent)) {
                            return; // consumer dropped, or stopped
                        }
                    }
                });
                if std::panic::catch_unwind(body).is_err() {
                    // Producer panicked mid-stream — surface a typed terminal
                    // error so the demux thread does NOT read the dropped channel
                    // as a clean EOF and truncate output.
                    let e = crate::error::Error::DemuxThreadPanicked.into();
                    let _ = wait.send_timeout(&tx, Err(e), Duration::MAX);
                }
            })?;

        Ok(Self {
            rx,
            recycle_tx,
            producer: Some(producer),
        })
    }

    /// Peel off the channels for zero-copy pipeline consumption. The
    /// caller (typically [`crate::mux::demux_thread::DemuxThread`])
    /// drains `rx`, runs the demuxer in place on each filled buffer,
    /// and recycles back through `recycle_tx`.
    pub fn into_channels(mut self) -> (Receiver<Batch>, Sender<Vec<u8>>, PrefetchShell) {
        // Same dead-channel swap as `Drop for BytePrefetcher` below: `self` is left
        // holding only disconnected placeholders — no `unsafe`, no double-drop, and (being
        // non-`Option`) no fallback branch that could hand back a dead `rx`.
        let (dead_tx, dead_rx) = bounded::<Batch>(0);
        drop(dead_tx);
        let rx = std::mem::replace(&mut self.rx, dead_rx);
        let (dead_send, dead_recv) = bounded::<Vec<u8>>(0);
        drop(dead_recv);
        let recycle = std::mem::replace(&mut self.recycle_tx, dead_send);
        let producer = self.producer.take();
        // `self` drops here: dead endpoints no-op, `producer` is `None` (no join).
        (rx, recycle, PrefetchShell { producer })
    }
}

impl Drop for BytePrefetcher {
    fn drop(&mut self) {
        // Drop channel endpoints BEFORE joining so the producer observes Disconnected
        // (send/recv) and exits promptly. Otherwise a non-EOF source fills the forward
        // channel and spins in send_timeout forever since rx is never drained.
        let (dead_tx, dead_rx) = bounded::<Batch>(0);
        drop(dead_tx);
        drop(std::mem::replace(&mut self.rx, dead_rx));
        let (dead_send, dead_recv) = bounded::<Vec<u8>>(0);
        drop(dead_recv);
        drop(std::mem::replace(&mut self.recycle_tx, dead_send));
        if let Some(h) = self.producer.take() {
            join_or_detach(h);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RECYCLE_DEPTH must be one MORE than FORWARD_DEPTH so the producer always has a spare
    // buffer while the consumer holds the rest in flight.
    #[test]
    fn recycle_depth_is_forward_depth_plus_one() {
        assert_eq!(RECYCLE_DEPTH, FORWARD_DEPTH + 1);
        assert_eq!(RECYCLE_DEPTH, 3, "FORWARD_DEPTH is 2, so recycle must be 3");
    }

    // Pins DEFAULT_CHUNK_BYTES (16 MiB) as a literal so a mutation on
    // 16 * 1024 * 1024 is caught by a concrete expected value instead
    // of by recomputing the same expression.
    #[test]
    fn default_chunk_bytes_is_16_mib() {
        assert_eq!(DEFAULT_CHUNK_BYTES, 16_777_216, "documented as 16 MiB");
    }

    // Endless reader: every read fills the buffer and never hits EOF, so
    // the producer keeps pushing until the forward channel disconnects —
    // the shape that wedged the pre-1.0.0 clone+mem::forget into_channels.
    struct EndlessReader;
    impl Read for EndlessReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            buf.fill(0);
            Ok(buf.len())
        }
    }

    /// Run `f` on a helper thread and fail if it does not finish within
    /// `secs`. Turns a join-deadlock into a test failure instead of a
    /// hung CI run.
    fn within<F: FnOnce() + Send + 'static>(secs: u64, f: F) {
        let (done_tx, done_rx) = bounded::<()>(1);
        std::thread::spawn(move || {
            f();
            let _ = done_tx.send(());
        });
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(secs))
                .is_ok(),
            "operation did not complete within {secs}s (deadlock)"
        );
    }

    // A reader blocked forever in read() cannot be interrupted: both Drops must return
    // after the grace (detaching the producer) instead of wedging the dropping thread.
    #[test]
    fn drop_detaches_producer_blocked_in_read() {
        struct Blocked(crossbeam_channel::Receiver<()>);
        impl Read for Blocked {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                let _ = self.0.recv();
                Ok(0)
            }
        }
        let (release, blocked) = bounded::<()>(0);
        let pf = BytePrefetcher::new(Blocked(blocked.clone()), 8, None).expect("spawn");
        within(5, move || drop(pf));
        let (_rx, _recycle, shell) = BytePrefetcher::new(Blocked(blocked), 8, None)
            .expect("spawn")
            .into_channels();
        within(5, move || drop(shell));
        drop(release);
    }

    // CRITICAL regression: dropping the forward receiver + recycle sender after into_channels
    // must let the producer see disconnection and exit, so dropping PrefetchShell (join)
    // returns promptly.
    #[test]
    fn into_channels_drop_releases_producer() {
        within(10, || {
            // Small chunk so the producer cycles quickly and fills the
            // forward channel without allocating much.
            let pf = BytePrefetcher::new(EndlessReader, 4096, None).expect("spawn");
            let (rx, recycle_tx, shell) = pf.into_channels();
            // Consumer goes away early (halt / abort analogue): drop
            // both channel endpoints without draining to EOF.
            drop(rx);
            drop(recycle_tx);
            // Joining the producer must not hang.
            drop(shell);
        });
    }

    // L106 (safe `Option::take` rewrite, no `unsafe`): `shell` drop must join within the
    // bound below (a leaked handle hangs it). No-double-drop is structural: an explicit
    // second `Drop::drop` call is a compile error (E0040), so it can't be attempted.
    #[test]
    fn into_channels_then_drop_leaks_nothing() {
        within(10, || {
            let pf = BytePrefetcher::new(EndlessReader, 4096, None).expect("spawn");
            let (rx, recycle_tx, shell) = pf.into_channels();
            drop(rx);
            drop(recycle_tx);
            drop(shell); // joins: a leaked/duplicated handle would hang this within(10, ..).
        });
    }

    // Channels handed back by `into_channels` must still carry real bytes end-to-end — the
    // safe rewrite must not have handed back the dead placeholder channels by mistake.
    #[test]
    fn into_channels_channels_still_deliver_bytes() {
        within(10, || {
            let src = vec![7u8; 10_000];
            let pf = BytePrefetcher::new(Cursor::new(src.clone()), 4096, None).expect("spawn");
            let (out, err) = drain_to_vec(pf);
            assert!(err.is_none(), "no read error expected: {err:?}");
            assert_eq!(
                out, src,
                "all bytes delivered, byte-identical, after into_channels"
            );
        });
    }

    /// Same property via the halt path: cancel the token, then the
    /// producer must exit and the shell join must complete.
    #[test]
    fn halt_releases_producer() {
        within(10, || {
            let halt = Halt::new();
            let pf = BytePrefetcher::new(EndlessReader, 4096, Some(halt.clone())).expect("spawn");
            let (_rx, _recycle_tx, shell) = pf.into_channels();
            halt.cancel();
            drop(shell);
        });
    }

    // ── Added hardening tests ───────────────────────────────────────

    use std::io::Cursor;

    // Drain the forward channel, recycling every buffer, and reassemble
    // the bytes. Stops on RecvError (EOF) or the first Err batch
    // (returned separately).
    fn drain_to_vec(pf: BytePrefetcher) -> (Vec<u8>, Option<std::io::Error>) {
        let (rx, recycle_tx, shell) = pf.into_channels();
        let mut out = Vec::new();
        let mut err = None;
        while let Ok(batch) = rx.recv() {
            match batch {
                Ok(buf) => {
                    out.extend_from_slice(&buf);
                    // Recycle so the producer can refill. Ignore send
                    // error (producer may have already exited at EOF).
                    let _ = recycle_tx.send(buf);
                }
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        drop(rx);
        drop(recycle_tx);
        drop(shell);
        (out, err)
    }

    // CORE CONTRACT: every source byte delivered in order, exactly once. 5000-byte source, 1024
    // chunk size forces multiple chunks.
    #[test]
    fn delivers_all_bytes_in_order_across_chunks() {
        within(10, || {
            let src: Vec<u8> = (0..5000u32).map(|i| (i & 0xff) as u8).collect();
            let pf = BytePrefetcher::new(Cursor::new(src.clone()), 1024, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert!(err.is_none(), "unexpected error batch: {err:?}");
            assert_eq!(got, src, "prefetcher truncated or reordered bytes");
        });
    }

    // Short-read truncation: a reader returning fewer bytes than requested must not leave stale
    // tail bytes; 10-byte source with a 4096 chunk must yield exactly 10 bytes.
    #[test]
    fn short_read_truncates_to_actual_length() {
        within(10, || {
            let src = vec![0xAB; 10];
            let pf = BytePrefetcher::new(Cursor::new(src.clone()), 4096, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert!(err.is_none());
            assert_eq!(got.len(), 10, "delivered chunk padded past actual read");
            assert_eq!(got, src);
        });
    }

    // EOF semantics: an empty source yields read() == Ok(0) on the first
    // call, which the producer treats as EOF; consumer sees RecvError
    // (zero batches), not an Err or zero-length Ok batch (see docs).
    #[test]
    fn empty_source_yields_clean_eof_no_batches() {
        within(10, || {
            let pf = BytePrefetcher::new(Cursor::new(Vec::<u8>::new()), 4096, None).expect("spawn");
            let (rx, recycle_tx, shell) = pf.into_channels();
            // No Ok batch should ever arrive; first recv must be Err
            // (producer dropped tx at EOF).
            let first = rx.recv();
            assert!(
                first.is_err(),
                "empty source produced a batch instead of clean EOF: {first:?}"
            );
            drop(rx);
            drop(recycle_tx);
            drop(shell);
        });
    }

    // Error propagation: a reader that fails mid-stream must surface the io::Error as an Err
    // batch, not swallow it — one good chunk then the error.
    #[test]
    fn read_error_is_propagated_as_err_batch() {
        within(10, || {
            struct OneThenError {
                served: bool,
            }
            impl Read for OneThenError {
                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                    if !self.served {
                        self.served = true;
                        let n = buf.len().min(8);
                        buf[..n].fill(0x11);
                        Ok(n)
                    } else {
                        Err(std::io::Error::other("synthetic mid-stream read failure"))
                    }
                }
            }
            let pf = BytePrefetcher::new(OneThenError { served: false }, 8, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert_eq!(got, vec![0x11; 8], "good chunk lost");
            let err = err.expect("read error must surface as an Err batch");
            assert_eq!(err.kind(), std::io::ErrorKind::Other);
        });
    }

    // PANIC propagation: a reader that PANICS mid-stream must not read as a clean EOF;
    // catch_unwind sends an explicit Err sentinel first.
    #[test]
    fn read_panic_surfaces_as_err_batch_not_clean_eof() {
        within(10, || {
            struct OneThenPanic {
                served: bool,
            }
            impl Read for OneThenPanic {
                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                    if !self.served {
                        self.served = true;
                        let n = buf.len().min(8);
                        buf[..n].fill(0x22);
                        Ok(n)
                    } else {
                        panic!("synthetic mid-stream reader panic");
                    }
                }
            }
            let pf = BytePrefetcher::new(OneThenPanic { served: false }, 8, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert_eq!(got, vec![0x22; 8], "good chunk lost before the panic");
            assert!(
                err.is_some(),
                "a mid-stream producer PANIC must surface as an Err batch, \
                 not a clean EOF (which would silently truncate the mux)"
            );
        });
    }

    // Exercises the regrow-fill's now-plain `resize` when len is already chunk_bytes (was
    // `unsafe set_len`): three full 8-byte chunks back to back, no short read in between.
    #[test]
    fn full_chunks_back_to_back_exercise_the_no_shrink_resize_path() {
        within(10, || {
            let src = vec![0x5Cu8; 24]; // 3 whole 8-byte chunks, source ends exactly on a chunk
            let pf = BytePrefetcher::new(Cursor::new(src.clone()), 8, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert!(err.is_none());
            assert_eq!(
                got, src,
                "back-to-back full-size chunks must round-trip exactly"
            );
        });
    }

    // Recycle-buffer reuse must not leak stale bytes between chunks of different lengths;
    // source 8×0xAA + 3×0xBB, chunk_bytes=8.
    #[test]
    fn recycled_buffer_carries_no_stale_tail() {
        within(10, || {
            let mut src = vec![0xAA; 8];
            src.extend_from_slice(&[0xBB; 3]);
            let pf = BytePrefetcher::new(Cursor::new(src.clone()), 8, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert!(err.is_none());
            assert_eq!(
                got, src,
                "stale bytes from recycled buffer leaked into short chunk"
            );
        });
    }

    // Exact-multiple boundary: source length an exact multiple of
    // chunk_bytes must yield Ok(0) EOF after the last chunk, never a
    // spurious empty Ok batch. 12 bytes, chunk_bytes=4 → 3 chunks.
    #[test]
    fn exact_multiple_length_no_trailing_empty_batch() {
        within(10, || {
            let src = vec![0x42u8; 12];
            let pf = BytePrefetcher::new(Cursor::new(src.clone()), 4, None).expect("spawn");
            let (rx, recycle_tx, shell) = pf.into_channels();
            let mut total = 0usize;
            let mut batch_count = 0usize;
            while let Ok(Ok(buf)) = rx.recv() {
                assert!(!buf.is_empty(), "producer emitted a zero-length batch");
                total += buf.len();
                batch_count += 1;
                let _ = recycle_tx.send(buf);
            }
            assert_eq!(total, 12);
            assert_eq!(batch_count, 3, "expected exactly 3 full chunks");
            drop(rx);
            drop(recycle_tx);
            drop(shell);
        });
    }

    #[test]
    fn zero_chunk_is_invalid_input() {
        let e = BytePrefetcher::new(Cursor::new(vec![1u8; 4]), 0, None)
            .err()
            .expect("zero chunk must be rejected");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
    }

    // EINTR from the reader is retried, not surfaced as a terminal error.
    #[test]
    fn interrupted_read_is_retried() {
        within(10, || {
            struct Flaky(u8);
            impl Read for Flaky {
                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                    self.0 += 1;
                    match self.0 {
                        1 => Err(std::io::ErrorKind::Interrupted.into()),
                        2 => {
                            buf[..3].fill(9);
                            Ok(3)
                        }
                        _ => Ok(0),
                    }
                }
            }
            let pf = BytePrefetcher::new(Flaky(0), 8, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert!(err.is_none(), "EINTR surfaced: {err:?}");
            assert_eq!(got, vec![9; 3]);
        });
    }

    // Dropping BytePrefetcher directly (without into_channels) must join the producer cleanly
    // on a finite source: EOF drops tx, Drop's join returns.
    #[test]
    fn drop_finite_prefetcher_joins_cleanly() {
        within(10, || {
            let pf = BytePrefetcher::new(Cursor::new(vec![1u8; 100]), 4096, None).expect("spawn");
            // Drop without consuming — producer fills the forward
            // channel (capacity 2), reaches EOF on the third read since
            // 100 < 4096 (single chunk + EOF), drops tx, exits.
            drop(pf);
        });
    }

    // Regression: dropping a BytePrefetcher directly with an ENDLESS source must not deadlock.
    // Fix drops rx+recycle_tx BEFORE the join.
    #[test]
    fn drop_endless_prefetcher_joins_cleanly() {
        within(10, || {
            let pf = BytePrefetcher::new(EndlessReader, 4096, None).expect("spawn");
            // Drop without consuming — the old Drop deadlocked here.
            drop(pf);
        });
    }

    // A cancel must release a producer parked on the recycle channel (consumer holds every
    // buffer). Producer exit shows as the forward channel disconnecting.
    #[test]
    fn halt_releases_producer_parked_on_recycle() {
        let halt = Halt::new();
        let pf = BytePrefetcher::new(EndlessReader, 64, Some(halt.clone())).expect("spawn");
        let (rx, recycle_tx, shell) = pf.into_channels();
        // Take all RECYCLE_DEPTH buffers and never return them.
        let held: Vec<_> = (0..RECYCLE_DEPTH)
            .map(|_| rx.recv().expect("batch").expect("read ok"))
            .collect();
        halt.cancel();
        let r = rx.recv_timeout(Duration::from_secs(5));
        assert!(
            matches!(r, Err(crossbeam_channel::RecvTimeoutError::Disconnected)),
            "producer still parked on recycle after cancel: {r:?}"
        );
        drop((held, recycle_tx, shell));
    }

    // A buffer truncated by a short read must be regrown before its next read, or the chunk
    // size shrinks for good. Records the buffer length each read() is offered.
    #[test]
    fn truncated_buffer_is_regrown_when_reused() {
        use std::sync::{Arc, Mutex};
        struct ShortReads {
            left: u8,
            lens: Arc<Mutex<Vec<usize>>>,
        }
        impl Read for ShortReads {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.lens.lock().unwrap().push(buf.len());
                if self.left == 0 {
                    return Ok(0);
                }
                self.left -= 1;
                buf[..3].fill(1);
                Ok(3)
            }
        }
        within(10, || {
            let lens = Arc::new(Mutex::new(Vec::new()));
            // 6 short reads over 3 buffers: every buffer is truncated, then reused.
            let r = ShortReads {
                left: 6,
                lens: lens.clone(),
            };
            let pf = BytePrefetcher::new(r, 8, None).expect("spawn");
            let (got, err) = drain_to_vec(pf);
            assert!(err.is_none());
            assert_eq!(got.len(), 18);
            let lens = lens.lock().unwrap();
            assert_eq!(lens.len(), 7);
            assert!(lens.iter().all(|&l| l == 8), "buffer not regrown: {lens:?}");
        });
    }

    /// LP9 (×2, §2.1 "Unbounded Drop joins stay plain joins"): after a cancel, dropping
    /// the prefetcher, or its shell with both channel ends still held, returns within
    /// 1 s. Guard.
    #[test]
    fn byte_prefetcher_drop_after_cancel_returns() {
        let halt = Halt::new();
        let pf = BytePrefetcher::new(EndlessReader, 4096, Some(halt.clone())).expect("spawn");
        std::thread::sleep(std::time::Duration::from_millis(50));
        halt.cancel();
        within(1, move || drop(pf));

        let halt = Halt::new();
        let pf = BytePrefetcher::new(EndlessReader, 4096, Some(halt.clone())).expect("spawn");
        let (rx, recycle_tx, shell) = pf.into_channels();
        std::thread::sleep(std::time::Duration::from_millis(50));
        halt.cancel();
        within(1, move || drop(shell));
        drop((rx, recycle_tx));
    }
}
