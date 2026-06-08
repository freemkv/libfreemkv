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
                let cancelled = || halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false);
                loop {
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
        let rx = unsafe { std::ptr::read(&me.rx) };
        let recycle = unsafe { std::ptr::read(&me.recycle_tx) };
        (rx, recycle, PrefetchShell { producer })
    }
}

impl Drop for BytePrefetcher {
    fn drop(&mut self) {
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
}
