//! `Disc::sweep`'s consumer-side `Sink<WorkItem>`.
//!
//! Background: the original sweep loop runs strictly serialised —
//! SCSI read → decrypt → seek + write → mapfile.record → next iter.
//! On a healthy disc the SCSI read costs ~5-12 ms per 64 KB batch and
//! the post-read work (decrypt 1-3 ms + file write + mapfile fsync
//! 5-15 ms) adds another batch's worth of latency. The drive idles
//! during the post-read work; throughput tops out at the *sum* of
//! both costs.
//!
//! 0.17.11 introduced a bespoke producer/consumer split (the now-
//! removed `disc/sweep_pipeline.rs`) to overlap the two stages. 0.18
//! collapses that split — together with the analogous splits patch
//! and mux need — onto the generic [`crate::io::Pipeline`] +
//! [`crate::io::Sink`] primitive. This module is the sweep-specific
//! `Sink` impl; the producer-side state machine (read_error context,
//! decrypt, set_speed, halt) stays in `Disc::sweep` in `disc/mod.rs`.
//!
//! Correctness invariants preserved (same as 0.17.11):
//! - Mapfile is single-writer (consumer-only). No locking.
//! - All `read_error::ReadCtx` state stays on the producer thread.
//! - `set_speed` calls happen on the producer thread (same thread that
//!   owns the `SectorSource`). No new SCSI concurrency.
//! - Per-iteration ordering of file-write → mapfile-record is kept
//!   intact in the consumer (write before record), so the on-disk
//!   invariant "mapfile only marks Finished what the file has
//!   received" survives a crash mid-pass.
//! - The BU40N+Initio bridge wedge concern is unchanged: only one
//!   SCSI command in flight at a time, error-path timing identical,
//!   no new retry logic.

use std::io::{Seek, SeekFrom, Write};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use crate::error::Error;
use crate::io::{Flow, Sink};

use super::mapfile::{MapStats, Mapfile, SectorStatus};

/// Reusable zero buffer for SkipFill / GapFill / BisectBad. 64 KB
/// matches the existing zero_gap chunk size used by the pre-split
/// sweep loop.
const ZERO_CHUNK: usize = 65 * 1024;

/// Producer → Consumer messages. The consumer applies these in FIFO
/// order; ordering of file writes and mapfile records across items is
/// preserved.
pub(super) enum WorkItem {
    /// Successful batch read. Producer has already decrypted `buf` if
    /// `opts.decrypt` was set. Consumer writes `buf` at `pos` and
    /// records the range as `Finished`.
    Good { pos: u64, buf: Vec<u8> },

    /// Bisect inner-loop good single sector (already decrypted by the
    /// producer). 2048 bytes.
    BisectGood { pos: u64, buf: Box<[u8; 2048]> },

    /// Bisect inner-loop bad single sector. Consumer writes 2048
    /// zeros at `pos` and records the sector as `NonTrimmed`.
    BisectBad { pos: u64 },

    /// Whole-batch zero-fill (failed batch on `SkipBlock`, or the
    /// failed batch portion of `JumpAhead`). Consumer streams zeros
    /// across `[pos, pos+len)` and records the range as `NonTrimmed`.
    SkipFill { pos: u64, len: u64 },

    /// Gap fill following a `JumpAhead`. Same effect as `SkipFill`;
    /// distinguished only so future logging / instrumentation can
    /// tell them apart without parsing a flag.
    GapFill { pos: u64, len: u64 },

    /// Producer wants the latest mapfile stats for the progress
    /// callback. Consumer responds on `prog_tx` with a fresh
    /// [`ProgressSnapshot`]. Best-effort: if the producer hasn't
    /// drained the previous snapshot, the new one is silently
    /// dropped — the producer's local cache stays current enough.
    StatsRequest,
}

/// Snapshot the consumer sends back to the producer for the progress
/// callback.
pub(super) struct ProgressSnapshot {
    pub stats: MapStats,
    pub bad_ranges: Vec<(u64, u64)>,
}

/// Final summary returned by the consumer thread on shutdown — what
/// `SweepSink::close` produces, surfaced to the producer via
/// `Pipeline::finish`.
pub(super) struct ConsumerSummary {
    pub stats: MapStats,
}

/// Drain any pending progress snapshots from the consumer. Returns
/// the most recent one, if any. The producer caches it and uses it
/// for subsequent progress callbacks until a fresh one arrives.
pub(super) fn try_recv_progress(rx: &Receiver<ProgressSnapshot>) -> Option<ProgressSnapshot> {
    let mut latest = None;
    while let Ok(snap) = rx.try_recv() {
        latest = Some(snap);
    }
    latest
}

/// `Sink<WorkItem>` for sweep. Owns the writeback file + mapfile +
/// progress back-channel. `apply` carries the file-write +
/// mapfile.record per item; `close` drains the writeback pipeline,
/// fsyncs the ISO, and flushes the mapfile.
pub(super) struct SweepSink {
    file: crate::io::WritebackFile,
    map: Mapfile,
    /// `sync_all`-on-failure-is-an-error iff the output is a regular
    /// file. `/dev/null` and pipes always fail `sync_all`; that's not
    /// a real error.
    is_regular: bool,
    /// Back-channel for `StatsRequest` responses. The producer caches
    /// the latest snapshot and uses it for the progress callback;
    /// dropped sends on a full channel are by design.
    prog_tx: SyncSender<ProgressSnapshot>,
    /// Reusable zero buffer for SkipFill / GapFill / BisectBad. Held
    /// in the sink so each apply call doesn't reallocate.
    zero: Box<[u8; ZERO_CHUNK]>,
}

impl SweepSink {
    /// Construct a new `SweepSink` plus the matching progress
    /// receiver. Channel depth on the back-channel is `1` — the
    /// producer's cache is the source of truth between snapshots.
    pub(super) fn new(
        file: crate::io::WritebackFile,
        map: Mapfile,
        is_regular: bool,
    ) -> (Self, Receiver<ProgressSnapshot>) {
        let (prog_tx, prog_rx) = sync_channel::<ProgressSnapshot>(1);
        let sink = SweepSink {
            file,
            map,
            is_regular,
            prog_tx,
            zero: Box::new([0u8; ZERO_CHUNK]),
        };
        (sink, prog_rx)
    }
}

impl Sink<WorkItem> for SweepSink {
    type Output = ConsumerSummary;

    fn apply(&mut self, item: WorkItem) -> Result<Flow, Error> {
        match item {
            WorkItem::Good { pos, buf } => {
                // Decrypt is on the producer; consumer assumes plaintext.
                let len = buf.len() as u64;
                self.file
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| Error::IoError { source: e })?;
                self.file
                    .write_all(&buf)
                    .map_err(|e| Error::IoError { source: e })?;
                self.map
                    .record(pos, len, SectorStatus::Finished)
                    .map_err(|e| Error::IoError { source: e })?;
            }
            WorkItem::BisectGood { pos, buf } => {
                self.file
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| Error::IoError { source: e })?;
                self.file
                    .write_all(&buf[..])
                    .map_err(|e| Error::IoError { source: e })?;
                self.map
                    .record(pos, 2048, SectorStatus::Finished)
                    .map_err(|e| Error::IoError { source: e })?;
            }
            WorkItem::BisectBad { pos } => {
                self.file
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| Error::IoError { source: e })?;
                self.file
                    .write_all(&self.zero[..2048])
                    .map_err(|e| Error::IoError { source: e })?;
                self.map
                    .record(pos, 2048, SectorStatus::NonTrimmed)
                    .map_err(|e| Error::IoError { source: e })?;
            }
            WorkItem::SkipFill { pos, len } | WorkItem::GapFill { pos, len } => {
                self.file
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| Error::IoError { source: e })?;
                // Subsequent writes are sequential; `WritebackFile`'s
                // seek-elision keeps them on the writeback pipeline path.
                let mut filled = 0u64;
                while filled < len {
                    let chunk = (len - filled).min(self.zero.len() as u64) as usize;
                    self.file
                        .write_all(&self.zero[..chunk])
                        .map_err(|e| Error::IoError { source: e })?;
                    filled += chunk as u64;
                }
                self.map
                    .record(pos, len, SectorStatus::NonTrimmed)
                    .map_err(|e| Error::IoError { source: e })?;
            }
            WorkItem::StatsRequest => {
                let stats = self.map.stats();
                let bad_ranges = self.map.ranges_with(&[
                    SectorStatus::NonTrimmed,
                    SectorStatus::Unreadable,
                    SectorStatus::NonScraped,
                    SectorStatus::NonTried,
                ]);
                // Best-effort: drop on backpressure; producer's cache
                // stays current enough.
                let _ = self
                    .prog_tx
                    .try_send(ProgressSnapshot { stats, bad_ranges });
            }
        }
        Ok(Flow::Continue)
    }

    fn close(mut self) -> Result<Self::Output, Error> {
        // Drain the writeback pipeline + fsync the ISO, then persist
        // any pending mapfile state. Same finalisation order as the
        // pre-Pipeline consumer loop.
        if let Err(e) = self.file.sync_all() {
            if self.is_regular {
                return Err(Error::IoError { source: e });
            }
            // Non-regular outputs (/dev/null, pipes) always fail
            // sync_all; that's not a real error.
        }
        self.map.flush().map_err(|e| Error::IoError { source: e })?;

        Ok(ConsumerSummary {
            stats: self.map.stats(),
        })
    }
}
