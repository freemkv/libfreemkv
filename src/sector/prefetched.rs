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
//! The producer thread is spawned by [`PrefetchedSectorSource::new`]
//! (which returns `Err` if the OS refuses the thread spawn).
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
    /// Total sector count across all extents, computed once at
    /// construction (the sum of each extent's `sector_count`) and
    /// returned by [`capacity_sectors`]. Never updated by reads.
    ///
    /// [`capacity_sectors`]: SectorSource::capacity_sectors
    total_sectors: u32,
}

impl PrefetchedSectorSource {
    /// Spawn the producer thread. `reader` must already be the fully
    /// composed read+decrypt stack (e.g.
    /// [`DecryptingSectorSource`](crate::sector::DecryptingSectorSource))
    /// — every byte the producer emits is what the consumer's demux
    /// will feed to its codec parsers.
    ///
    /// ## Unit-alignment precondition
    ///
    /// Each extent's `sector_count` should be a multiple of
    /// [`SECTOR_ALIGNMENT`] (3 sectors / one 6144-byte AACS aligned
    /// unit). Blu-ray m2ts extents satisfy this by spec. If an extent
    /// has a trailing 1-2 sectors that cannot fill a complete unit,
    /// the producer surfaces [`Error::ExtentNotUnitAligned`] through
    /// the channel rather than handing the decrypt step a sub-unit
    /// chunk it would silently leave encrypted.
    ///
    /// [`Error::ExtentNotUnitAligned`]: crate::error::Error::ExtentNotUnitAligned
    pub fn new<S>(
        reader: S,
        extents: Vec<crate::disc::Extent>,
        batch_sectors: u16,
        halt: Option<Halt>,
    ) -> Result<Self>
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
    ) -> Result<Self>
    where
        S: SectorSource + Send + 'static,
    {
        // A zero batch would make the producer loop forever emitting
        // empty batches (sectors = remaining.min(0) = 0, offset never
        // advances). All production callers pass a nonzero constant; a
        // public-API caller passing 0 is a programming error, so reject
        // it rather than spin a thread that never makes progress.
        if batch_sectors == 0 {
            return Err(crate::error::Error::IoError {
                source: std::io::Error::from(std::io::ErrorKind::InvalidInput),
            });
        }
        // Accumulate in u64 then clamp: extents can derive from
        // untrusted nav/MPLS/UDF data, so a naive u32 `sum()` could
        // panic in debug / wrap in release on a hostile total. The
        // clamp only affects the advisory `capacity_sectors` figure;
        // the producer walks each extent independently below.
        let total_sectors: u32 = extents
            .iter()
            .map(|e| e.sector_count as u64)
            .sum::<u64>()
            .min(u32::MAX as u64) as u32;
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
                    // The AACS aligned unit is SECTOR_ALIGNMENT (3)
                    // sectors / 6144 bytes; the decrypt step only
                    // processes full units and silently leaves a
                    // shorter trailing chunk encrypted. So a batch must
                    // be a whole number of units — except for the final
                    // batch of an extent whose `sector_count` is itself
                    // unit-aligned (then the remaining tail is exactly
                    // 0 mod 3 and forms full units on its own).
                    //
                    // If the trailing sectors of this extent cannot fill
                    // a complete unit (`remaining < SECTOR_ALIGNMENT`
                    // with nothing more to read, or a 1-2 sector
                    // leftover after the last full unit), there is no
                    // way to hand the decrypt step an aligned chunk —
                    // surface a typed error instead of emitting
                    // still-encrypted bytes.
                    if remaining % SECTOR_ALIGNMENT as u32 != 0
                        && remaining < SECTOR_ALIGNMENT as u32
                    {
                        let _ = tx.send(Err(crate::error::Error::ExtentNotUnitAligned.into()));
                        return;
                    }
                    let mut sectors = remaining.min(batch_sectors as u32) as u16;
                    // Trim to a whole number of units. Once trimmed to 0
                    // here it means `remaining >= SECTOR_ALIGNMENT` but
                    // the *batch window* landed on a sub-unit boundary —
                    // never the trailing-tail case, which the guard
                    // above already rejected. Clamp to one unit so we
                    // always make forward progress.
                    if sectors >= SECTOR_ALIGNMENT {
                        sectors -= sectors % SECTOR_ALIGNMENT;
                    } else {
                        sectors = SECTOR_ALIGNMENT;
                    }
                    let bytes = sectors as usize * 2048;
                    let mut buf = match recycle_rx.recv() {
                        Ok(b) => b,
                        Err(_) => return, // consumer dropped both channels
                    };
                    if bytes <= buf.capacity() {
                        // Re-expose `bytes` without zero-filling pages that
                        // `read_sectors` is about to overwrite. The enclosing
                        // capacity guard makes the `set_len` provably sound even
                        // if a recycled buffer ever comes back smaller than the
                        // `vec![0u8; batch_bytes]` it was born with.
                        debug_assert!(bytes <= buf.capacity(), "set_len exceeds capacity");
                        unsafe { buf.set_len(bytes) };
                    } else {
                        buf.resize(bytes, 0);
                    }
                    // `start_lba + offset` derives from untrusted extent
                    // data — saturate rather than wrap/panic on a
                    // hostile start_lba near u32::MAX.
                    let lba = extent.start_lba.saturating_add(offset);
                    match reader.read_sectors(lba, sectors, &mut buf[..bytes], false) {
                        Ok(n) => {
                            // A short read must not silently desync the
                            // stream: advance the extent cursor by the
                            // sectors actually read, not the requested
                            // count, and reject a byte count that isn't a
                            // whole number of sectors (it would split a
                            // sector and leave the decrypt step a partial
                            // unit). The sole production inner source
                            // (FileSectorSource) read_exact's the full
                            // request, so this is belt-and-braces against
                            // a future short-reading source.
                            if n % 2048 != 0 {
                                let _ =
                                    tx.send(Err(crate::error::Error::ExtentNotUnitAligned.into()));
                                return;
                            }
                            let sectors_read = (n / 2048) as u32;
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
                            // A genuine zero-byte read with no error would
                            // otherwise spin this loop forever; treat it
                            // as end-of-source.
                            if sectors_read == 0 {
                                return;
                            }
                            offset = offset.saturating_add(sectors_read);
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e.into()));
                            return;
                        }
                    }
                }
                // Drop tx implicitly — consumer sees RecvError → EOF.
            })
            .map_err(|e| crate::error::Error::IoError { source: e })?;

        Ok(Self {
            rx,
            recycle_tx,
            producer: Some(producer),
            total_sectors,
        })
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
        // is the panic-free equivalent of the `Option::take` approach.
        let total = self.total_sectors;
        let me = std::mem::ManuallyDrop::new(self);
        // SAFETY: `me` is `ManuallyDrop`, so none of these fields will
        // be dropped by `me`. Each `ptr::read` performs exactly one
        // bitwise move out; every field is read exactly once and never
        // touched again, so there are no double-frees and no aliasing.
        let producer = unsafe { std::ptr::read(&me.producer) };
        let rx = unsafe { std::ptr::read(&me.rx) };
        let recycle = unsafe { std::ptr::read(&me.recycle_tx) };
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
                // Precondition: the caller's buffer must be large
                // enough to hold the producer's batch. If it is not we
                // would silently drop `filled[buf.len()..]`, desyncing
                // the stream. The production zero-copy path never uses
                // this method (it consumes the channel directly via
                // `into_channels`), so a too-small buffer here is a
                // caller bug — surface it instead of corrupting data.
                if filled.len() > buf.len() {
                    return Err(crate::error::Error::IoError {
                        source: std::io::Error::from(std::io::ErrorKind::InvalidInput),
                    });
                }
                let n = filled.len();
                buf[..n].copy_from_slice(&filled[..n]);
                // Return the buffer to the recycle pool so the producer
                // can re-fill it. Without this the pool (seeded with
                // PREFETCH_CHANNEL_DEPTH+1 buffers) drains after that
                // many reads and the producer blocks forever on
                // `recycle_rx.recv()` while the consumer blocks on the
                // next `rx.recv()` — a permanent deadlock. The
                // `into_channels` zero-copy path recycles explicitly;
                // this direct-read path must do the same.
                let _ = self.recycle_tx.send(filled);
                Ok(n)
            }
            Ok(Err(e)) => Err(crate::error::Error::IoError { source: e }),
            // Channel closed (producer finished or panicked).
            Err(_) => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::Extent;
    use crate::error::Result;
    use std::sync::mpsc;
    use std::time::Duration;

    /// Endless zero-yielding source: every read succeeds, so the
    /// producer keeps trying to push batches forward until the forward
    /// channel disconnects. Exactly the shape that wedged the pre-1.0.0
    /// `clone + mem::forget` `into_channels`.
    struct EndlessZeroSource;
    impl SectorSource for EndlessZeroSource {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }
    }

    /// Synthetic source that fills `buf` with a per-sector byte
    /// pattern derived from the LBA, always satisfying the full
    /// request (mirrors `FileSectorSource`'s read_exact contract).
    struct PatternSource {
        capacity: u32,
    }

    impl SectorSource for PatternSource {
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }

        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            for s in 0..count as usize {
                let base = s * 2048;
                let tag = (lba.wrapping_add(s as u32) & 0xff) as u8;
                for b in &mut buf[base..base + 2048] {
                    *b = tag;
                }
            }
            Ok(bytes)
        }
    }

    /// Source that returns a short read (fewer sectors than
    /// requested) on its very first call, then full reads. Used to
    /// prove the producer advances by sectors actually read.
    struct ShortFirstSource {
        capacity: u32,
        first: bool,
    }

    impl SectorSource for ShortFirstSource {
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }

        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let give = if self.first {
                self.first = false;
                // Short read: hand back one aligned unit (3 sectors)
                // regardless of the larger request.
                SECTOR_ALIGNMENT.min(count)
            } else {
                count
            };
            let bytes = give as usize * 2048;
            for s in 0..give as usize {
                let base = s * 2048;
                let tag = (lba.wrapping_add(s as u32) & 0xff) as u8;
                for b in &mut buf[base..base + 2048] {
                    *b = tag;
                }
            }
            Ok(bytes)
        }
    }

    fn big_extent() -> Vec<Extent> {
        // One huge extent so the producer never reaches EOF on its own;
        // the only way it can exit is by observing channel disconnection.
        vec![Extent {
            start_lba: 0,
            sector_count: u32::MAX,
        }]
    }

    /// Run `f` on a helper thread and fail if it does not finish within
    /// `secs`. Used to turn a join-deadlock into a test failure instead
    /// of a hung CI run.
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

    /// Run `f` on a worker thread; fail (rather than hang) if it does
    /// not finish within `timeout`. Guards the deadlock regression so
    /// a reintroduced bug fails the suite instead of wedging it.
    fn with_watchdog<F>(timeout: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let h = std::thread::spawn(move || {
            f();
            let _ = done_tx.send(());
        });
        match done_rx.recv_timeout(timeout) {
            Ok(()) => {
                let _ = h.join();
            }
            Err(_) => panic!("watchdog timeout — likely deadlock/hang in prefetch read path"),
        }
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
            let src = PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, None)
                .expect("spawn");
            let (rx, recycle_tx, shell) = src.into_channels();
            // Consumer goes away early (halt / abort analogue): drop both
            // channel endpoints without draining to EOF.
            drop(rx);
            drop(recycle_tx);
            // Joining the producer must not hang.
            drop(shell);
        });
    }

    /// Same property via the halt path: cancel the token, then the
    /// producer must exit and the shell join must complete.
    ///
    /// The producer parks in a BLOCKING `tx.send` on the forward channel
    /// and only checks `halt` at the loop top, so `halt.cancel()` cannot
    /// interrupt a send that is already blocked on a full channel. To
    /// keep the test deterministic under load, we drain the forward
    /// receiver on a background thread: every send then makes progress,
    /// the producer reaches the loop top, observes the cancelled halt,
    /// and exits — so the shell join completes promptly regardless of
    /// scheduling. (Channel-disconnection shutdown is covered separately
    /// by `into_channels_drop_releases_producer`.)
    #[test]
    fn halt_releases_producer() {
        within(10, || {
            let halt = Halt::new();
            let src =
                PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, Some(halt.clone()))
                    .expect("spawn");
            let (rx, recycle_tx, shell) = src.into_channels();
            // Drain the forward channel so the producer's sends always
            // make progress and it can reach the halt check at the loop
            // top, recycling buffers so it never blocks on the pool.
            let drainer = std::thread::spawn(move || {
                while let Ok(item) = rx.recv() {
                    if let Ok(buf) = item {
                        let _ = recycle_tx.send(buf);
                    }
                }
            });
            halt.cancel();
            drop(shell);
            let _ = drainer.join();
        });
    }

    /// `batch_sectors == 0` is rejected rather than spawning a thread
    /// that spins forever emitting empty batches.
    #[test]
    fn zero_batch_rejected() {
        let err = PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 0, None);
        assert!(err.is_err(), "zero batch_sectors must be rejected");
    }

    /// >3 sequential direct `read_sectors` calls must succeed. The
    /// recycle pool seeds PREFETCH_CHANNEL_DEPTH+1 (3) buffers; before
    /// the fix the direct path dropped each drained buffer, so the 4th
    /// call deadlocked. Watchdog-guarded.
    #[test]
    fn direct_reads_past_pool_depth_do_not_deadlock() {
        with_watchdog(Duration::from_secs(10), || {
            // 24 sectors = 8 aligned units; batch of 3 sectors gives 8
            // sequential batches, well past the 3-buffer pool depth.
            let extents = vec![Extent {
                start_lba: 0,
                sector_count: 24,
            }];
            let src = PatternSource { capacity: 24 };
            let mut pf = PrefetchedSectorSource::new(src, extents, 3, None).expect("spawn");

            let mut buf = vec![0u8; 3 * 2048];
            let mut total = 0usize;
            for _ in 0..16 {
                let n = pf.read_sectors(0, 3, &mut buf, false).unwrap();
                if n == 0 {
                    break; // EOF
                }
                total += n;
            }
            assert_eq!(total, 24 * 2048, "all 24 sectors should be drained");
        });
    }

    /// An extent whose sector_count is not a multiple of 3 must not
    /// emit a still-encrypted sub-unit tail. The producer delivers the
    /// readable full units, then surfaces a typed error on the tail
    /// instead of a short batch. Watchdog-guarded so a regression that
    /// hangs (rather than errors) still fails.
    #[test]
    fn non_multiple_of_three_extent_errors_on_tail() {
        with_watchdog(Duration::from_secs(10), || {
            // 8 sectors = 2 full units (6 sectors) + 2 leftover.
            let extents = vec![Extent {
                start_lba: 100,
                sector_count: 8,
            }];
            let src = PatternSource { capacity: 200 };
            let mut pf = PrefetchedSectorSource::new(src, extents, 3, None).expect("spawn");

            let mut buf = vec![0u8; 3 * 2048];
            // First two reads: the 6 unit-aligned sectors come through
            // as full 3-sector (6144-byte) batches.
            let n0 = pf.read_sectors(0, 3, &mut buf, false).unwrap();
            assert_eq!(n0, 3 * 2048);
            let n1 = pf.read_sectors(0, 3, &mut buf, false).unwrap();
            assert_eq!(n1, 3 * 2048);
            // Third read hits the 2-sector tail: it must be an error,
            // never a 4096-byte (sub-unit) batch that decrypt would
            // leave encrypted.
            let err = pf.read_sectors(0, 3, &mut buf, false);
            assert!(
                err.is_err(),
                "non-unit-aligned tail must error, got Ok({:?})",
                err
            );
        });
    }

    /// A short read (inner source returns fewer sectors than
    /// requested) must advance the extent cursor by the sectors
    /// actually read, not the requested count — otherwise the bytes
    /// between the short read and the request size are silently
    /// skipped. We verify every sector of the extent is delivered.
    #[test]
    fn short_read_does_not_desync_stream() {
        with_watchdog(Duration::from_secs(10), || {
            // 9 sectors = 3 full units. batch of 9 means the first
            // request is for 9 sectors; ShortFirstSource hands back
            // only 3, so the producer must re-request the remaining 6.
            let extents = vec![Extent {
                start_lba: 0,
                sector_count: 9,
            }];
            let src = ShortFirstSource {
                capacity: 9,
                first: true,
            };
            let mut pf = PrefetchedSectorSource::new(src, extents, 9, None).expect("spawn");

            let mut buf = vec![0u8; 9 * 2048];
            let mut total = 0usize;
            for _ in 0..16 {
                let n = pf.read_sectors(0, 9, &mut buf, false).unwrap();
                if n == 0 {
                    break;
                }
                total += n;
            }
            assert_eq!(
                total,
                9 * 2048,
                "short read must not drop sectors; all 9 must be delivered"
            );
        });
    }
}
