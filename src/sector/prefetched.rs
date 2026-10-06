//! `PrefetchedSectorSource` — runs the wrapped read+decrypt in a
//! dedicated producer thread and surfaces the prepared plaintext
//! buffers on demand via a bounded channel, so disk+decrypt and
//! demux run in parallel instead of serialized on one thread.
//!
//! The producer thread ([`PrefetchedSectorSource::new`]) walks the extent list in order,
//! sending batches into a bounded channel; sender drop reads as end-of-stream. `read_sectors`
//! ignores its `lba`/`count` args.

use crate::ctx::Ctx;
use crate::error::Result;
use crate::halt::{DriveHolder, Halt, Recv, SendOutcome};
use crate::sector::SectorSource;
use crossbeam_channel::{Receiver, Sender, bounded};
use std::time::Duration;

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
    /// The producer (a Drive holder for a disc or image, §2.5): joined on drop.
    producer: Option<Producer>,
    /// Total sector count across all extents, computed once at
    /// construction (the sum of each extent's `sector_count`) and
    /// returned by [`capacity_sectors`]. Never updated by reads.
    ///
    /// [`capacity_sectors`]: SectorSource::capacity_sectors
    total_sectors: u32,
    /// Latched the moment a terminal error crosses the channel. The
    /// producer NEVER resumes after sending one (every error arm
    /// `return`s), so the closed channel that follows is a dead source,
    /// not end-of-stream — and `read_sectors` must keep saying so instead
    /// of answering `Ok(0)` for the rest of the title.
    producer_failed: bool,
    /// The reader's unmapped stream files, snapshotted before it moved to the producer.
    unmapped: Vec<crate::sector::bus_removal::UnmappedStreamFile>,
    /// The op's token: once cancelled, the closed channel is a stop, not EOF (L096).
    halt: Halt,
}

// The producer thread: a Drive holder (§2.5) for a disc or image read, a plain thread for a
// file's byte view (`m2ts://`), which holds no Drive.
enum Producer {
    Drive(DriveHolder<()>),
    File(std::thread::JoinHandle<()>),
}

impl Producer {
    fn join(self) {
        match self {
            Producer::Drive(h) => drop(h.join()),
            Producer::File(h) => drop(h.join()),
        }
    }
}

/// A file's byte view through the Read stage (`m2ts://`): `prefix` (the bytes already read
/// past the head scan) goes out first, then the chunks, ending after `len` chunk bytes (the
/// file's real length, short of its zero-padded last sector).
pub(crate) struct ByteView {
    pub(crate) prefix: Vec<u8>,
    pub(crate) len: u64,
}

// Hand `item` to the consumer, waiting on a full channel; `false` once the consumer is
// gone or `halt` is cancelled (observed within a slice), so the producer returns.
fn send_or_stop(halt: &Halt, tx: &Sender<Batch>, item: Batch) -> bool {
    matches!(
        halt.send_timeout(tx, item, Duration::MAX),
        Ok(SendOutcome::Sent)
    )
}

impl PrefetchedSectorSource {
    /// Spawn the producer thread. `reader` must already be the fully
    /// composed read+decrypt stack — every byte the producer emits is
    /// what the consumer's demux will feed to its codec parsers.
    ///
    /// Reads come in whole units; a shorter extent tail is one read the decrypt stage judges.
    /// A non-whole-sector read surfaces [`Error::ExtentNotUnitAligned`] through the channel.
    ///
    /// [`Error::ExtentNotUnitAligned`]: crate::error::Error::ExtentNotUnitAligned
    ///
    /// `ctx.halt` stops the producer; a `BytesRead` event goes to `ctx.events` after every
    /// batch, from the producer thread.
    pub fn new<S>(
        reader: S,
        extents: Vec<crate::disc::Extent>,
        batch_sectors: u16,
        ctx: &Ctx,
    ) -> Result<Self>
    where
        S: SectorSource + Send + 'static,
    {
        Self::with_alignment(reader, extents, batch_sectors, SECTOR_ALIGNMENT, ctx)
    }

    /// [`Self::new`] reading in whole `unit_align`-sector units (1 for CSS/clear).
    pub(crate) fn with_alignment<S>(
        reader: S,
        extents: Vec<crate::disc::Extent>,
        batch_sectors: u16,
        unit_align: u16,
        ctx: &Ctx,
    ) -> Result<Self>
    where
        S: SectorSource + Send + 'static,
    {
        let policy = crate::sector::read_stage::ReadPolicy::Image {
            batch: batch_sectors,
        };
        Self::with_policy(reader, extents, policy, unit_align, ctx).map(|(s, _)| s)
    }

    /// The producer running the Read stage over `extents` under `policy` (an image's fixed
    /// batches, or a live drive's adaptive, recovering reads), in `unit_align`-sector
    /// units. Also returns the read loss the stage counts.
    pub(crate) fn with_policy<S>(
        reader: S,
        extents: Vec<crate::disc::Extent>,
        policy: crate::sector::read_stage::ReadPolicy,
        unit_align: u16,
        ctx: &Ctx,
    ) -> Result<(Self, std::sync::Arc<crate::sector::read_stage::ReadLoss>)>
    where
        S: SectorSource + Send + 'static,
    {
        Self::spawn(reader, extents, policy, unit_align, ctx, None)
    }

    /// The producer over a file's byte view (`m2ts://`): the same Read stage under `policy`
    /// in single-sector units, its chunks preceded by `view.prefix` and clipped to `view.len`.
    pub(crate) fn file_bytes<S>(
        reader: S,
        extents: Vec<crate::disc::Extent>,
        policy: crate::sector::read_stage::ReadPolicy,
        ctx: &Ctx,
        view: ByteView,
    ) -> Result<(Self, std::sync::Arc<crate::sector::read_stage::ReadLoss>)>
    where
        S: SectorSource + Send + 'static,
    {
        Self::spawn(reader, extents, policy, 1, ctx, Some(view))
    }

    fn spawn<S>(
        mut reader: S,
        extents: Vec<crate::disc::Extent>,
        policy: crate::sector::read_stage::ReadPolicy,
        unit_align: u16,
        ctx: &Ctx,
        view: Option<ByteView>,
    ) -> Result<(Self, std::sync::Arc<crate::sector::read_stage::ReadLoss>)>
    where
        S: SectorSource + Send + 'static,
    {
        let mut walk =
            crate::sector::read_stage::ExtentWalk::new(extents, policy, unit_align, ctx)?;
        let loss = walk.loss();
        let total_sectors = walk.total_sectors();
        let batch_sectors = policy.batch();
        let unmapped = reader.unmapped_stream_files().to_vec();
        let (tx, rx) = bounded::<Batch>(PREFETCH_CHANNEL_DEPTH);
        let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(PREFETCH_CHANNEL_DEPTH + 1);
        let batch_bytes = batch_sectors as usize * crate::consts::SECTOR_BYTES;
        let wait = ctx.halt.clone();

        // Seed the recycle pool so the producer has a buffer on the first
        // iteration; otherwise the first recycle_rx.recv() blocks forever.
        for _ in 0..(PREFETCH_CHANNEL_DEPTH + 1) {
            let _ = recycle_tx.send(vec![0u8; batch_bytes]);
        }

        #[cfg(test)]
        assert!(
            view.is_some() || crate::halt::DRIVE_HOLDER_TEST_LOCK.try_lock().is_err(),
            "a test that spawns a prefetcher (a Drive holder) must hold DRIVE_HOLDER_TEST_LOCK"
        );
        let file_view = view.is_some();
        let body = move || {
            // A byte view: what is still to go out after the prefix.
            let (prefix, mut left) = match view {
                Some(v) => (v.prefix, Some(v.len)),
                None => (Vec::new(), None),
            };
            // catch_unwind so a panic (decrypt source, read path, events) isn't
            // mistaken for clean EOF: a dropped `tx` alone would finalize a
            // TRUNCATED mux as success. Locals are thread-local, so this is sound.
            let body = std::panic::AssertUnwindSafe(|| {
                if !prefix.is_empty() && !send_or_stop(&wait, &tx, Ok(prefix)) {
                    return;
                }
                loop {
                    if wait.is_cancelled() {
                        return;
                    }
                    // Halt-aware: a cancel does not disconnect the channel, so a
                    // plain recv() would never re-reach the check. Disconnected =
                    // the consumer dropped both channels.
                    let Ok(Recv::Item(mut buf)) = wait.recv_timeout(&recycle_rx, Duration::MAX)
                    else {
                        return;
                    };
                    match walk.next(&mut reader, &mut buf) {
                        Ok(true) => {
                            // A byte view ends at the file's real length.
                            let last = left.is_some_and(|l| buf.len() as u64 >= l);
                            if let Some(l) = left.as_mut() {
                                buf.truncate((*l).min(buf.len() as u64) as usize);
                                *l -= buf.len() as u64;
                            }
                            if !buf.is_empty() && !send_or_stop(&wait, &tx, Ok(buf)) {
                                return; // consumer dropped, or stopped
                            }
                            if last {
                                return;
                            }
                        }
                        // Drop tx — the consumer sees RecvError → EOF.
                        Ok(false) => return,
                        Err(e) => {
                            send_or_stop(&wait, &tx, Err(e.into()));
                            return;
                        }
                    }
                }
            });
            if std::panic::catch_unwind(body).is_err() {
                // Panicked mid-stream — surface a typed error so the demux
                // doesn't read the dropped channel as clean EOF and truncate.
                // Ignore send failure: consumer already gone, nothing to report.
                let e = crate::error::Error::DemuxThreadPanicked.into();
                send_or_stop(&wait, &tx, Err(e));
            }
        };
        let producer = match file_view {
            false => crate::halt::spawn_drive_holder("prefetch", body).map(Producer::Drive),
            true => std::thread::Builder::new()
                .name("freemkv-prefetch".into())
                .spawn(body)
                .map(Producer::File),
        }
        .map_err(|e| crate::error::Error::IoError { source: e })?;

        Ok((
            Self {
                rx,
                recycle_tx,
                producer: Some(producer),
                total_sectors,
                producer_failed: false,
                unmapped,
                halt: ctx.halt.clone(),
            },
            loss,
        ))
    }

    /// Peel off the receivers for zero-copy pipeline mode: the caller pulls buffers from `rx`
    /// and pushes drained ones back through `recycle_tx`. Returns `(forward_rx, recycle_tx,
    /// shell)`; the shell holds the producer's `JoinHandle` (drop it to join) plus
    /// `total_sectors`.
    pub fn into_channels(mut self) -> (Receiver<Batch>, Sender<Vec<u8>>, PrefetchShell) {
        // Same dead-channel swap `Drop for PrefetchedSectorSource` uses below: leaves `self`
        // holding only harmless placeholders, so no `unsafe`/`ManuallyDrop`/`ptr::read`, and
        // no double-drop (real endpoints already moved out via `mem::replace`/`Option::take`).
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

/// Returned from [`PrefetchedSectorSource::into_channels`]. Owns the
/// producer thread join handle so dropping the shell joins the
/// producer, even though the channels have been peeled off.
pub struct PrefetchShell {
    producer: Option<Producer>,
}

// Every test that spawns a producer holds this (the halt module's Drive-holder test lock).
#[cfg(test)]
pub(crate) fn holder_test_lock() -> std::sync::MutexGuard<'static, ()> {
    crate::halt::DRIVE_HOLDER_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

impl Drop for PrefetchShell {
    fn drop(&mut self) {
        if let Some(h) = self.producer.take() {
            h.join();
        }
    }
}

impl Drop for PrefetchedSectorSource {
    fn drop(&mut self) {
        // Drop endpoints BEFORE joining: siblings drop only after this body
        // returns, and joining first left a producer blocked forever in
        // `tx.send`. Swap in disconnected stand-ins to drop the real ones.
        let (dead_tx, dead_rx) = bounded::<Batch>(0);
        drop(dead_tx);
        drop(std::mem::replace(&mut self.rx, dead_rx));
        let (dead_send, dead_recv) = bounded::<Vec<u8>>(0);
        drop(dead_recv);
        drop(std::mem::replace(&mut self.recycle_tx, dead_send));
        // Now the producer's next `send`/`recv` returns Err and its loop exits;
        // joining gives a deterministic shutdown — no detached thread can outlive
        // the source.
        if let Some(h) = self.producer.take() {
            h.join();
        }
    }
}

impl SectorSource for PrefetchedSectorSource {
    fn capacity_sectors(&self) -> u32 {
        self.total_sectors
    }

    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        &self.unmapped
    }

    // The producer decides the next batch; "lba/count are advisory" (read_sectors below).
    fn random_access(&self) -> bool {
        false
    }

    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        // The producer already decided the next batch. lba/count are
        // advisory; fill_extents advances its own bookkeeping using the
        // returned byte count, not the requested count.
        match self.rx.recv() {
            Ok(Ok(filled)) => {
                // Precondition: buf must hold the whole batch, else we'd
                // silently drop the tail and desync the stream. Production
                // uses `into_channels` directly, so this is a caller bug.
                if filled.len() > buf.len() {
                    // Recycle before erroring, or the pool loses one buffer per
                    // error; after PREFETCH_CHANNEL_DEPTH+1 errors it's exhausted
                    // and producer/consumer deadlock on recycle_rx/rx.recv().
                    let _ = self.recycle_tx.send(filled);
                    return Err(crate::error::Error::IoError {
                        source: std::io::Error::from(std::io::ErrorKind::InvalidInput),
                    });
                }
                let n = filled.len();
                buf[..n].copy_from_slice(&filled[..n]);
                // Recycle so the producer can re-fill: without it the seeded pool
                // drains after PREFETCH_CHANNEL_DEPTH+1 reads and both sides
                // deadlock on recycle_rx/rx.recv(), same as `into_channels`.
                let _ = self.recycle_tx.send(filled);
                Ok(n)
            }
            // Recover the producer's TYPED error, not a blanket `Error::IoError`:
            // that also matches `is_scsi_transport_failure`, so a wrapped MEDIUM
            // ERROR bad sector looked like a wedged bridge instead of skippable.
            Ok(Err(e)) => {
                // The producer `return`s after every error it sends, so
                // this is also the moment the source dies. Latch it: the
                // closed channel that follows must not read as EOF.
                self.producer_failed = true;
                Err(crate::error::Error::from(e))
            }
            // Channel closed. A stop first (L096, LP17): a halted producer returns
            // silently, and `Ok(0)` would read as a short, complete source.
            Err(_) if self.halt.is_cancelled() => Err(crate::error::Error::Halted),
            // Clean EOF only if the producer never signalled a failure — else `Ok(0)`
            // lets fill_extents zero-fill a dead source's rest as "complete".
            Err(_) if self.producer_failed => Err(crate::error::Error::SourceTerminated),
            Err(_) => Ok(0),
        }
    }
}

#[cfg(test)]
#[path = "prefetched_tests.rs"]
mod tests;
