//! `BytePrefetcher` — `std::io::Read` analogue of
//! [`crate::sector::PrefetchedSectorSource`]. Spawns a producer
//! thread that fills a bounded pool of `Vec<u8>` chunks from the
//! underlying reader and ships them through a channel; the consumer
//! recycles emptied buffers back so the producer re-fills in place,
//! for zero allocations and zero cross-thread frees in the hot loop.
//! Works for any stream whose source is an `io::Read`, not just a
//! `SectorSource`. See docs/byte-prefetcher.md for the full note.

use crate::halt::{Halt, POLL_INTERVAL};
use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender, bounded};
use std::io::Read;
use std::thread::JoinHandle;

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
/// Drop blocks until the producer exits. For a prompt exit, drop the
/// forward receiver and recycle sender first (channel disconnection)
/// or cancel the [`Halt`] passed to [`BytePrefetcher::new`], polled
/// at [`POLL_INTERVAL`] granularity even while parked on a channel op.
pub struct PrefetchShell {
    producer: Option<JoinHandle<()>>,
}

impl Drop for PrefetchShell {
    fn drop(&mut self) {
        if let Some(h) = self.producer.take() {
            let _ = h.join();
        }
    }
}

/// Spawned byte prefetcher. Drop joins the producer thread.
pub struct BytePrefetcher {
    rx: Option<Receiver<Batch>>,
    recycle_tx: Option<Sender<Vec<u8>>>,
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
        // A zero-length chunk makes every recycled buffer empty; `reader.read(&mut [])`
        // returns Ok(0), which the loop below treats as EOF, producing a silent
        // zero-byte stream. Callers must pass the downstream demuxer's batch size (> 0).
        debug_assert!(chunk_bytes > 0, "BytePrefetcher chunk_bytes must be > 0");
        let (tx, rx) = bounded::<Batch>(FORWARD_DEPTH);
        let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(RECYCLE_DEPTH);

        // Seed the recycle pool; otherwise the first `recycle_rx.recv()` blocks
        // forever since no consumer has returned a buffer yet.
        for _ in 0..RECYCLE_DEPTH {
            let _ = recycle_tx.send(vec![0u8; chunk_bytes]);
        }

        let producer = std::thread::Builder::new()
            .name("freemkv-byte-prefetch".into())
            .spawn(move || {
                // catch_unwind: a clean exit drops `tx` (demux reads RecvError as EOF),
                // but a panic sends an error sentinel first so demux gets a typed error
                // instead of finalizing a truncated mux as success.
                let body = std::panic::AssertUnwindSafe(|| {
                    let cancelled = || halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false);
                    // Liveness heartbeat: a stalled consumer or wedged reader shows up
                    // as the beat going silent. Total is unknown, so `pos` is cumulative.
                    let mut hb = crate::progress::Heartbeat::new("byte_prefetch");
                    let mut produced_bytes: u64 = 0;
                    loop {
                        hb.tick(produced_bytes, 0);
                        if cancelled() {
                            return;
                        }
                        // Re-poll halt every POLL_INTERVAL: a pure-AtomicBool Halt does
                        // not disconnect the channel, so a blocking recv() would never
                        // re-reach the cancel check.
                        let mut buf = loop {
                            match recycle_rx.recv_timeout(POLL_INTERVAL) {
                                Ok(b) => break b,
                                Err(RecvTimeoutError::Timeout) => {
                                    if cancelled() {
                                        return;
                                    }
                                }
                                // Consumer dropped both channels.
                                Err(RecvTimeoutError::Disconnected) => return,
                            }
                        };
                        // Regrow to chunk_bytes: a prior short read truncated len to
                        // n < chunk_bytes. No realloc — capacity was fixed at
                        // construction and never shrinks.
                        if buf.len() < chunk_bytes {
                            buf.resize(chunk_bytes, 0);
                        } else {
                            // SAFETY: capacity is at least chunk_bytes
                            // after construction.
                            unsafe { buf.set_len(chunk_bytes) };
                        }
                        // Read up to one full chunk. Short reads are
                        // valid and common — pipe `truncate` so the
                        // consumer sees only the bytes that arrived.
                        let n = match reader.read(&mut buf[..]) {
                            Ok(0) => return, // EOF — drop tx, consumer sees RecvError
                            Ok(n) => n,
                            Err(e) => {
                                let _ = tx.send(Err(e));
                                return;
                            }
                        };
                        produced_bytes += n as u64;
                        buf.truncate(n);
                        // Hand off the filled buffer, re-polling halt on
                        // each timeout slice so a cancel can interrupt a
                        // producer parked on a saturated forward channel.
                        let mut pending = Ok(buf);
                        loop {
                            match tx.send_timeout(pending, POLL_INTERVAL) {
                                Ok(()) => break,
                                Err(SendTimeoutError::Timeout(returned)) => {
                                    if cancelled() {
                                        return;
                                    }
                                    pending = returned;
                                }
                                // Consumer dropped.
                                Err(SendTimeoutError::Disconnected(_)) => return,
                            }
                        }
                    }
                });
                if std::panic::catch_unwind(body).is_err() {
                    // Producer panicked mid-stream — surface a typed terminal
                    // error so the demux thread does NOT read the dropped channel
                    // as a clean EOF and truncate output.
                    let _ = tx.send(Err(crate::error::Error::DemuxThreadPanicked.into()));
                }
            })?;

        Ok(Self {
            rx: Some(rx),
            recycle_tx: Some(recycle_tx),
            producer: Some(producer),
        })
    }

    /// Peel off the channels for zero-copy pipeline consumption. The
    /// caller (typically [`crate::mux::demux_thread::DemuxThread`])
    /// drains `rx`, runs the demuxer in place on each filled buffer,
    /// and recycles back through `recycle_tx`.
    pub fn into_channels(self) -> (Receiver<Batch>, Sender<Vec<u8>>, PrefetchShell) {
        // MOVE the fields out, never clone: the pre-1.0.0 impl cloned + `mem::forget`-ed
        // `self`, leaking endpoints that defeated disconnection shutdown (producer join
        // hung forever). `ManuallyDrop` + `ptr::read` moves fields out cleanly instead.
        let me = std::mem::ManuallyDrop::new(self);
        // SAFETY: `me` is `ManuallyDrop`, so none of these fields are dropped by `me`.
        // Each `ptr::read` is one bitwise move, read exactly once — no double-frees.
        let producer = unsafe { std::ptr::read(&me.producer) };
        // SAFETY: `rx` and `recycle_tx` are always `Some` here —
        // `into_channels` is the only way to consume a live
        // `BytePrefetcher`; `Drop::drop` is suppressed by `ManuallyDrop`.
        let rx = unsafe { std::ptr::read(&me.rx) }.expect("rx always Some before drop");
        let recycle =
            unsafe { std::ptr::read(&me.recycle_tx) }.expect("recycle_tx always Some before drop");
        (rx, recycle, PrefetchShell { producer })
    }
}

impl Drop for BytePrefetcher {
    fn drop(&mut self) {
        // Drop channel endpoints BEFORE joining so the producer observes Disconnected
        // (send/recv) and exits promptly. Otherwise a non-EOF source fills the forward
        // channel and spins in send_timeout forever since rx is never drained.
        drop(self.rx.take());
        drop(self.recycle_tx.take());
        if let Some(h) = self.producer.take() {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RECYCLE_DEPTH must be one MORE than FORWARD_DEPTH so the producer
    // always has a spare buffer while the consumer holds the rest in
    // flight. See docs/byte-prefetcher.md for the mutation rationale.
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

    // CRITICAL regression: dropping the forward receiver + recycle sender
    // after into_channels must let the producer see disconnection and
    // exit, so dropping PrefetchShell (join) returns promptly (see docs/byte-prefetcher.md).
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

    // CORE CONTRACT: every source byte delivered in order, exactly once.
    // 5000-byte source, 1024 chunk size forces multiple chunks; see
    // docs/byte-prefetcher.md for the mutation this test grounds.
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

    // Short-read truncation: a reader returning fewer bytes than
    // requested must not leave stale tail bytes; 10-byte source with a
    // 4096 chunk must yield exactly 10 bytes (see docs/byte-prefetcher.md).
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

    // Error propagation: a reader that fails mid-stream must surface the
    // io::Error as an Err batch, not swallow it — one good chunk then
    // the error (see docs/byte-prefetcher.md for the mutation grounded).
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

    // PANIC propagation: a reader that PANICS mid-stream must not read as a
    // clean EOF; catch_unwind sends an explicit Err sentinel first (see
    // docs/byte-prefetcher.md for why a bare drop of tx would be unsafe).
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

    // Recycle-buffer reuse must not leak stale bytes between chunks of
    // different lengths; source 8×0xAA + 3×0xBB, chunk_bytes=8 (see
    // docs/byte-prefetcher.md for the regrow/truncate sequence detail).
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

    // Dropping BytePrefetcher directly (without into_channels) must join
    // the producer cleanly on a finite source: EOF drops tx, Drop's join
    // returns (see docs/byte-prefetcher.md for the mutation grounded).
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

    // Regression: dropping a BytePrefetcher directly with an ENDLESS
    // source must not deadlock. Fix drops rx+recycle_tx BEFORE the join
    // (see docs/byte-prefetcher.md for the old sibling-drop-order bug).
    #[test]
    fn drop_endless_prefetcher_joins_cleanly() {
        within(10, || {
            let pf = BytePrefetcher::new(EndlessReader, 4096, None).expect("spawn");
            // Drop without consuming — the old Drop deadlocked here.
            drop(pf);
        });
    }
}
