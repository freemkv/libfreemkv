//! `PrefetchedSectorSource` — runs the wrapped read+decrypt in a
//! dedicated producer thread and surfaces the prepared plaintext
//! buffers on demand via a bounded channel.
//!
//! ## Why
//!
//! The mux consumer (demux + codec parsing + frame output) is
//! single-threaded by nature (streams are sequential). The mux
//! producer (read sectors + AACS decrypt) is also single-threaded
//! per-call but does CPU-heavy work (AES per 6144-byte unit). Running
//! both on the same thread means the disk and decrypt cores sit idle
//! while the demux runs, and vice versa.
//!
//! Splitting them across two threads with a bounded channel between
//! lets both run in parallel — peak throughput becomes
//! `min(producer_rate, consumer_rate)` instead of
//! `1 / (1/producer + 1/consumer)`.
//!
//! ## Lifecycle
//!
//! The producer thread is spawned by [`PrefetchedSectorSource::new`].
//! It walks the supplied extent list in order, reads the configured
//! batch size at each LBA, and sends the resulting plaintext buffer
//! into a [`crossbeam_channel::bounded`] channel of small depth (so
//! the producer stays a couple of batches ahead without unbounded
//! memory growth).
//!
//! When the channel sender drops (either because all extents were
//! served or because the [`Halt`] token cancelled), the consumer
//! observes `RecvError` on the next `read_sectors` and treats it as
//! end-of-stream. Errors from the underlying reader are forwarded
//! verbatim through the channel.
//!
//! ## Read API
//!
//! `read_sectors` ignores its `lba`/`count` arguments — the producer
//! has already chosen what to read, in the order the extents dictate.
//! This is sound for the mux read path, which always walks extents
//! sequentially and never seeks. For random-access callers (sweep
//! patch retries) this wrapper is the wrong tool — they should keep
//! reading the underlying source directly.

use crate::error::Result;
use crate::event::{Event, EventKind};
use crate::halt::Halt;
use crate::sector::SectorSource;
use crossbeam_channel::{Receiver, Sender, bounded};
use std::thread::JoinHandle;

/// Producer-thread event callback. Fires `BytesRead` after every
/// successful batch read so the consumer side can update a UI
/// progress indicator without polling. `Send + 'static` because the
/// callback runs on the producer thread.
pub type EventFn = Box<dyn Fn(Event) + Send + 'static>;

const PREFETCH_CHANNEL_DEPTH: usize = 2;

/// Smallest sector source the producer will issue per read. AACS
/// alignment requires multiples of 3 sectors so a unit doesn't span
/// two reads.
const SECTOR_ALIGNMENT: u16 = 3;

/// Item flowing through the prefetch forward channel.
pub type Batch = std::result::Result<Vec<u8>, std::io::Error>;

/// Producer-thread-backed [`SectorSource`] decorator. Construct it
/// with the real reader, the extent list to walk, and the batch
/// size; the wrapper spawns the producer immediately and starts
/// filling the channel.
pub struct PrefetchedSectorSource {
    rx: Receiver<Batch>,
    /// Recycle channel — consumer returns drained buffers here; the
    /// producer re-fills them in place. Lets the producer/consumer
    /// reuse a fixed pool of `PREFETCH_CHANNEL_DEPTH+1` buffers
    /// instead of `Vec::new()`-ing one per batch (musl mallocng
    /// cross-thread alloc/free was the dominant cost in the demux
    /// thread before this).
    recycle_tx: Sender<Vec<u8>>,
    /// Joined on drop so producer cleanup runs deterministically.
    producer: Option<JoinHandle<()>>,
    /// Cumulative bytes drained by `read_sectors` calls. Exposed via
    /// [`capacity_sectors`] indirectly: the consumer-side state needs
    /// this to advance its position bookkeeping in lockstep with what
    /// the producer fed.
    total_sectors: u32,
}

impl PrefetchedSectorSource {
    /// Spawn the producer thread. `reader` must already be the fully
    /// composed read+decrypt stack (e.g.
    /// [`DecryptingSectorSource`](crate::sector::DecryptingSectorSource))
    /// — every byte the producer emits is what the consumer's demux
    /// will feed to its codec parsers.
    pub fn new<S>(
        reader: S,
        extents: Vec<crate::disc::Extent>,
        batch_sectors: u16,
        halt: Option<Halt>,
    ) -> Self
    where
        S: SectorSource + Send + 'static,
    {
        Self::new_with_events(reader, extents, batch_sectors, halt, None)
    }

    /// Same as [`new`] but with a callback fired from the producer
    /// thread after each successful batch — used by autorip's mux
    /// path to surface `BytesRead` progress to the UI without the
    /// consumer thread having to poll.
    pub fn new_with_events<S>(
        mut reader: S,
        extents: Vec<crate::disc::Extent>,
        batch_sectors: u16,
        halt: Option<Halt>,
        event_fn: Option<EventFn>,
    ) -> Self
    where
        S: SectorSource + Send + 'static,
    {
        let total_sectors: u32 = extents.iter().map(|e| e.sector_count).sum();
        let bytes_total_extents: u64 = extents.iter().map(|e| e.sector_count as u64 * 2048).sum();
        let (tx, rx) = bounded::<Batch>(PREFETCH_CHANNEL_DEPTH);
        let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(PREFETCH_CHANNEL_DEPTH + 1);
        let batch_bytes = batch_sectors as usize * 2048;

        // Seed the recycle pool so the producer always has a buffer
        // to fill on the first iteration. Without these, the first
        // `recycle_rx.recv()` would block forever (no consumer has
        // returned a buffer yet).
        for _ in 0..(PREFETCH_CHANNEL_DEPTH + 1) {
            let _ = recycle_tx.send(vec![0u8; batch_bytes]);
        }

        let producer = std::thread::Builder::new()
            .name("freemkv-prefetch".into())
            .spawn(move || {
                let mut ext_idx = 0usize;
                let mut offset: u32 = 0;
                let mut bytes_read_total: u64 = 0;
                while ext_idx < extents.len() {
                    if halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false) {
                        return;
                    }
                    let extent = &extents[ext_idx];
                    let remaining = extent.sector_count.saturating_sub(offset);
                    if remaining == 0 {
                        ext_idx += 1;
                        offset = 0;
                        continue;
                    }
                    let mut sectors = remaining.min(batch_sectors as u32) as u16;
                    if sectors >= SECTOR_ALIGNMENT {
                        sectors -= sectors % SECTOR_ALIGNMENT;
                    }
                    let bytes = sectors as usize * 2048;
                    let mut buf = match recycle_rx.recv() {
                        Ok(b) => b,
                        Err(_) => return, // consumer dropped both channels
                    };
                    if bytes <= buf.capacity() {
                        // Re-expose `bytes` without zero-filling pages that
                        // `read_sectors` is about to overwrite. The capacity
                        // guard makes the `set_len` provably sound even if a
                        // recycled buffer ever comes back smaller than the
                        // `vec![0u8; batch_bytes]` it was born with.
                        unsafe { buf.set_len(bytes) };
                    } else {
                        buf.resize(bytes, 0);
                    }
                    let lba = extent.start_lba + offset;
                    match reader.read_sectors(lba, sectors, &mut buf[..bytes], false) {
                        Ok(n) => {
                            buf.truncate(n);
                            bytes_read_total = bytes_read_total.saturating_add(n as u64);
                            if let Some(ref f) = event_fn {
                                f(Event {
                                    kind: EventKind::BytesRead {
                                        bytes: bytes_read_total,
                                        total: bytes_total_extents,
                                    },
                                });
                            }
                            if tx.send(Ok(buf)).is_err() {
                                return; // consumer dropped
                            }
                            offset += sectors as u32;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e.into()));
                            return;
                        }
                    }
                }
                // Drop tx implicitly — consumer sees RecvError → EOF.
            })
            .expect("freemkv-prefetch producer spawn failed");

        Self {
            rx,
            recycle_tx,
            producer: Some(producer),
            total_sectors,
        }
    }

    /// Peel off the receivers for zero-copy pipeline mode. The
    /// caller (typically [`super::super::mux::demux_thread::DemuxThread`])
    /// pulls buffers from `rx`, consumes them, and pushes the empty
    /// `Vec<u8>` back through `recycle_tx` so the producer can
    /// re-fill it. The producer-thread `JoinHandle` stays with the
    /// returned `PrefetchedSectorSource` shell; drop that to join.
    ///
    /// Returns `(forward_rx, recycle_tx, shell)`. The shell only
    /// holds the join handle and total_sectors for `capacity_sectors`
    /// queries; its `SectorSource` impl becomes invalid after this
    /// call (data has been moved out).
    pub fn into_channels(self) -> (Receiver<Batch>, Sender<Vec<u8>>, PrefetchShell) {
        let total = self.total_sectors;
        // Drop the SectorSource side; transfer the producer join
        // handle to a shell that just waits on Drop.
        let mut me = self;
        let producer = me.producer.take();
        let rx = me.rx.clone();
        let recycle = me.recycle_tx.clone();
        std::mem::forget(me);
        (rx, recycle, PrefetchShell { producer, total })
    }
}

/// Returned from [`PrefetchedSectorSource::into_channels`]. Owns the
/// producer thread join handle so dropping the shell joins the
/// producer, even though the channels have been peeled off.
pub struct PrefetchShell {
    producer: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    total: u32,
}

impl Drop for PrefetchShell {
    fn drop(&mut self) {
        if let Some(h) = self.producer.take() {
            let _ = h.join();
        }
    }
}

impl Drop for PrefetchedSectorSource {
    fn drop(&mut self) {
        // Dropping the receiver closes the channel, which makes the
        // next producer `send` return Err and exits the loop. Joining
        // here gives us a deterministic shutdown — no detached thread
        // can outlive the source.
        if let Some(h) = self.producer.take() {
            let _ = h.join();
        }
    }
}

impl SectorSource for PrefetchedSectorSource {
    fn capacity_sectors(&self) -> u32 {
        self.total_sectors
    }

    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        // The producer has already decided the next batch. lba/count
        // are advisory; the consumer's fill_extents will advance its
        // own bookkeeping using the returned byte count, not the
        // requested count.
        match self.rx.recv() {
            Ok(Ok(filled)) => {
                let n = filled.len().min(buf.len());
                buf[..n].copy_from_slice(&filled[..n]);
                Ok(n)
            }
            Ok(Err(e)) => Err(crate::error::Error::IoError { source: e }),
            // Channel closed (producer finished or panicked).
            Err(_) => Ok(0),
        }
    }
}
