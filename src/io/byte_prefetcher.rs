//! `BytePrefetcher` — `std::io::Read` analogue of
//! [`crate::sector::PrefetchedSectorSource`].
//!
//! Spawns a producer thread that fills a bounded pool of `Vec<u8>`
//! chunks from the underlying reader and ships them through a
//! channel; the consumer pulls filled chunks, uses them, and sends
//! the empty `Vec<u8>` back through a recycle channel so the
//! producer can re-fill in place. Result: zero allocations and zero
//! cross-thread frees in the steady-state hot loop.
//!
//! This is the byte-stream half of the freemkv mux highway —
//! `BytePrefetcher` feeds [`crate::mux::demux_thread::DemuxThread`]
//! for `m2ts://` (the only in-tree caller today, via
//! [`crate::mux::resolve`]), and works for any stream whose source is
//! an `io::Read` rather than a `SectorSource`.

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
/// Drop blocks the calling thread until the producer exits. To
/// guarantee a prompt exit, drop the forward receiver and the recycle
/// sender first so the producer observes channel disconnection (or
/// cancel the [`Halt`] passed to [`BytePrefetcher::new`], which the
/// producer polls at [`POLL_INTERVAL`] granularity even while parked
/// on a channel op).
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
        // A zero-length chunk makes every recycled buffer an empty
        // slice; `reader.read(&mut [])` returns Ok(0), which the loop
        // below treats as EOF — the consumer would see a clean,
        // silent zero-byte stream. Callers pass the downstream
        // demuxer's batch size, which is always > 0.
        debug_assert!(chunk_bytes > 0, "BytePrefetcher chunk_bytes must be > 0");
        let (tx, rx) = bounded::<Batch>(FORWARD_DEPTH);
        let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(RECYCLE_DEPTH);

        // Seed the recycle pool. Without these the first
        // `recycle_rx.recv()` would block forever (no consumer has
        // returned a buffer yet).
        for _ in 0..RECYCLE_DEPTH {
            let _ = recycle_tx.send(vec![0u8; chunk_bytes]);
        }

        let producer = std::thread::Builder::new()
            .name("freemkv-byte-prefetch".into())
            .spawn(move || {
                // Wrap the feed loop in catch_unwind so a panic in the inner
                // `reader.read` (e.g. a decrypt-on-read slice/arith bug) is NOT
                // indistinguishable from a clean finish at the demux boundary. A
                // clean exit (EOF, halt, consumer disconnect) returns and drops
                // `tx` → the demux loop reads RecvError as EOF (correct). A PANIC
                // sends an explicit error sentinel first so the demux loop's
                // `Ok(Err(_))` arm fires and propagates a typed error instead of
                // converting the dropped channel into a clean `DemuxBatch::Eof`
                // that would finalize a TRUNCATED mux while reporting success.
                let body = std::panic::AssertUnwindSafe(|| {
                    let cancelled = || halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false);
                    // Liveness heartbeat: the producer blocks on the recycle and
                    // forward channels; a stalled consumer or a wedged reader shows
                    // up as the beat going silent. Total is unknown, so `pos` is
                    // cumulative bytes read.
                    let mut hb = crate::progress::Heartbeat::new("byte_prefetch");
                    let mut produced_bytes: u64 = 0;
                    loop {
                        hb.tick(produced_bytes, 0);
                        if cancelled() {
                            return;
                        }
                        // Park on the recycle channel, but re-poll halt
                        // every POLL_INTERVAL: a pure-AtomicBool Halt does
                        // not disconnect the channel, so a blocking recv()
                        // would never re-reach the cancel check.
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
                        // Re-expose the full extent. After a short read the
                        // prior iteration truncated to n < chunk_bytes, so
                        // this regrows the length back to chunk_bytes
                        // without reallocating (capacity was fixed at
                        // construction and never shrinks).
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
        // MOVE the three fields out cleanly — never clone. Each of
        // `rx` and `recycle_tx` ends up with exactly ONE live copy:
        // the one in the returned tuple. The pre-1.0.0 implementation
        // cloned both and then `mem::forget`-ed `self`, leaking the
        // originals so an extra live receiver + sender survived
        // forever. That defeated the channel-disconnection shutdown:
        // when the demux consumer exited early (halt, or a `tx.send`
        // error in `demux_thread`), the producer's `recycle_rx.recv()`
        // and `tx.send()` never saw all-peers-dropped, so the producer
        // never returned and `PrefetchShell::drop`'s `join()` hung.
        //
        // `ManuallyDrop` + `ptr::read` reads each field out by value
        // and suppresses `self`'s own `Drop` (which would otherwise
        // double-`join`), leaving NO extra live endpoint behind. This
        // is the panic-free equivalent of the `Option::take` approach
        // and mirrors `sector::prefetched::into_channels`.
        let me = std::mem::ManuallyDrop::new(self);
        // SAFETY: `me` is `ManuallyDrop`, so none of these fields will
        // be dropped by `me`. Each `ptr::read` performs exactly one
        // bitwise move out; every field is read exactly once and never
        // touched again, so there are no double-frees and no aliasing.
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
        // Drop channel endpoints BEFORE joining the producer so the
        // producer observes SendTimeoutError::Disconnected (forward tx)
        // or RecvTimeoutError::Disconnected (recycle rx) and exits
        // promptly. Without this, a non-EOF source fills the depth-2
        // forward channel and then spins in send_timeout(POLL_INTERVAL)
        // forever because rx is never drained, causing join() to
        // deadlock.
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

    /// Endless reader: every `read` fills the whole buffer and never
    /// hits EOF, so the producer keeps trying to push batches forward
    /// until the forward channel disconnects. Exactly the shape that
    /// wedged the pre-1.0.0 `clone + mem::forget` `into_channels`.
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

    /// The CRITICAL regression: after `into_channels`, dropping the
    /// returned forward receiver + recycle sender must let the producer
    /// observe disconnection and exit, so dropping the `PrefetchShell`
    /// (which joins the producer) returns promptly. With the old
    /// clone+forget the leaked endpoints kept the producer blocked and
    /// this join hung forever.
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

    /// Drain the forward channel, recycling every buffer, and
    /// reassemble the bytes. Returns the concatenation of every
    /// delivered chunk. Stops on RecvError (producer dropped tx == EOF)
    /// or on the first Err batch (which it returns separately).
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

    /// CORE CONTRACT: the prefetcher must deliver every source byte,
    /// in order, exactly once — never silently truncate or duplicate.
    /// Source is 5000 bytes; chunk size 1024 forces multiple chunks
    /// (4 full + 1 short of 904). The reassembled stream must equal the
    /// source. Mutation: replacing `buf.truncate(n)` (line 141) with a
    /// no-op would over-report bytes on the final short read and this
    /// fails.
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

    /// Short-read truncation: a reader that returns fewer bytes than
    /// requested per call must NOT leave stale tail bytes in the
    /// delivered chunk. Cursor over 10 bytes with a 4096 chunk yields a
    /// single 10-byte chunk; the consumer must see exactly 10 bytes,
    /// not 4096. Grounds `buf.truncate(n)` at line 141. Mutation:
    /// delete the truncate and the chunk would carry 4086 zero bytes of
    /// padding, failing the length assert.
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

    /// EOF semantics: an empty source (Cursor over `[]`) yields
    /// `read() == Ok(0)` on the first call, which the producer treats
    /// as EOF and returns, dropping tx. The consumer sees RecvError
    /// (zero batches), NOT an Err batch and NOT a zero-length Ok batch.
    /// Grounds the `Ok(0) => return` arm at line 134. Mutation:
    /// changing `Ok(0) => return` to `Ok(0) => continue` would spin
    /// forever (within() would time out).
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

    /// Error propagation: a reader that fails mid-stream must surface
    /// the io::Error as an `Err` batch on the forward channel (line
    /// 137), not swallow it. We deliver one good chunk then an error.
    /// The consumer must see the good bytes followed by the error.
    /// Mutation: changing `let _ = tx.send(Err(e)); return;` to a plain
    /// `return` would drop the error silently and this fails.
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

    /// PANIC propagation: a reader that PANICS mid-stream must NOT be read as a
    /// clean EOF at the demux boundary. The producer's catch_unwind sends an
    /// explicit `Err` sentinel before the thread unwinds, so the consumer sees
    /// the good bytes followed by an error batch — never a silent truncation.
    /// Without the catch_unwind the panic would just drop `tx`, the consumer
    /// would see RecvError (== clean EOF) and the partial output would be
    /// finalized as if complete.
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

    /// Recycle-buffer reuse must NOT leak stale bytes between chunks of
    /// different lengths. After a full chunk, a short read reuses the
    /// same recycled buffer; lines 123-129 regrow it to chunk_bytes
    /// before reading, then line 141 truncates to the short count. We
    /// verify the short chunk carries only fresh bytes by reassembling
    /// the full stream. Source: 8 bytes of 0xAA + 3 bytes of 0xBB, with
    /// chunk_bytes=8 → chunk0 = 8×0xAA, chunk1 = 3×0xBB.
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

    /// Exact-multiple boundary: when the source length is an exact
    /// multiple of chunk_bytes, the final non-empty chunk is followed
    /// by an `Ok(0)` EOF read, NOT a spurious empty Ok batch. 12 bytes
    /// with chunk_bytes=4 → three 4-byte chunks then clean EOF. Total
    /// bytes must equal 12 and no zero-length batch may appear.
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

    /// Dropping the BytePrefetcher directly (without into_channels)
    /// must join the producer cleanly when the source is finite. The
    /// producer reaches EOF, drops tx, and exits; Drop's join returns.
    /// Grounds the BytePrefetcher Drop impl (lines 202-208). Mutation:
    /// removing the `Ok(0) => return` EOF exit would hang this join.
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

    /// Regression: dropping a BytePrefetcher directly (without
    /// into_channels) with an ENDLESS source must not deadlock. Before
    /// the fix, Drop joined the producer while rx/recycle_tx were still
    /// alive (sibling field drop order), so the producer filled the
    /// depth-2 forward channel and then spun in send_timeout forever
    /// (rx never drained, halt=None). The fix drops rx+recycle_tx
    /// BEFORE the join so the producer sees SendTimeoutError::Disconnected
    /// and exits.
    #[test]
    fn drop_endless_prefetcher_joins_cleanly() {
        within(10, || {
            let pf = BytePrefetcher::new(EndlessReader, 4096, None).expect("spawn");
            // Drop without consuming — the old Drop deadlocked here.
            drop(pf);
        });
    }
}
