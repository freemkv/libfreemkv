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
//! for `m2ts://`, `network://`, `stdio://`, and any other stream
//! whose source is an `io::Read` rather than a `SectorSource`.

use crate::halt::Halt;
use crossbeam_channel::{Receiver, Sender, bounded};
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
    ) -> Self {
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
                loop {
                    if halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false) {
                        return;
                    }
                    let mut buf = match recycle_rx.recv() {
                        Ok(b) => b,
                        Err(_) => return, // consumer dropped both channels
                    };
                    // Re-expose the full extent (previous iteration
                    // may have truncated after a short read).
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
                    if tx.send(Ok(buf)).is_err() {
                        return; // consumer dropped
                    }
                }
            })
            .expect("freemkv-byte-prefetch thread spawn failed");

        Self {
            rx,
            recycle_tx,
            producer: Some(producer),
        }
    }

    /// Peel off the channels for zero-copy pipeline consumption. The
    /// caller (typically [`crate::mux::demux_thread::DemuxThread`])
    /// drains `rx`, runs the demuxer in place on each filled buffer,
    /// and recycles back through `recycle_tx`.
    pub fn into_channels(self) -> (Receiver<Batch>, Sender<Vec<u8>>, PrefetchShell) {
        let mut me = self;
        let producer = me.producer.take();
        let rx = me.rx.clone();
        let recycle = me.recycle_tx.clone();
        std::mem::forget(me);
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
