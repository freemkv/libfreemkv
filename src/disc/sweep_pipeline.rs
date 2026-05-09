//! Producer / consumer split for `Disc::sweep`.
//!
//! Background: the original sweep loop runs strictly serialised —
//! SCSI read → decrypt → seek + write → mapfile.record → next iter.
//! On a healthy disc the SCSI read costs ~5-12 ms per 64 KB batch and
//! the post-read work (decrypt 1-3 ms + file write + mapfile fsync
//! 5-15 ms) adds another batch's worth of latency. The drive idles
//! during the post-read work; throughput tops out at the *sum* of
//! both costs.
//!
//! This module decouples them. A consumer thread owns the
//! [`crate::io::WritebackFile`] (the ISO file) and the
//! [`super::mapfile::Mapfile`]. The producer thread (the caller of
//! `Disc::sweep`) keeps the [`crate::sector::SectorReader`], the
//! [`super::read_error`] state machine, and decrypt — so what enters
//! the channel is already-clean cleartext bytes, matching the "disc
//! hands plaintext to its consumers" semantic that
//! [`crate::mux::DiscStream`] already follows. Producer and consumer
//! run concurrently; with a healthy disc the drive can read the next
//! batch while the previous one is being written and recorded.
//!
//! Correctness invariants preserved:
//! - Mapfile is single-writer (consumer-only). No locking.
//! - All `read_error::ReadCtx` state stays on the producer thread.
//! - `set_speed` calls happen on the producer thread (same thread that
//!   owns the `SectorReader`). No new SCSI concurrency.
//! - Per-iteration ordering of file-write → mapfile-record is kept
//!   intact in the consumer (write before record, same as today), so
//!   the on-disk invariant "mapfile only marks Finished what the file
//!   has received" survives a crash mid-pass.
//! - The BU40N+Initio bridge wedge concern is unchanged: only one
//!   SCSI command in flight at a time, error-path timing identical,
//!   no new retry logic.

use std::io::{Seek, SeekFrom, Write};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::{self, JoinHandle};

use crate::error::{Error, Result};

use super::mapfile::{MapStats, Mapfile, SectorStatus};

/// Channel depth for in-flight work items. 4 is enough to absorb a
/// mapfile-flush burst on the consumer without growing memory
/// unboundedly. Producer back-pressure is the natural rate limiter:
/// `SyncSender::send` blocks when the channel is full.
const CHANNEL_DEPTH: usize = 4;

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

    /// Producer is done. Consumer drains, runs `sync_all` on the
    /// file, and exits. Final stats are returned via the
    /// `JoinHandle<ConsumerSummary>` from `spawn_consumer`.
    Finish,
}

/// Snapshot the consumer sends back to the producer for the progress
/// callback.
pub(super) struct ProgressSnapshot {
    pub stats: MapStats,
    pub bad_ranges: Vec<(u64, u64)>,
}

/// Final summary returned by the consumer thread on shutdown. The
/// producer reads it via the `JoinHandle` returned from
/// [`spawn_consumer`].
pub(super) struct ConsumerSummary {
    pub stats: MapStats,
    /// First mapfile/write error the consumer hit, if any. The
    /// producer treats this as fatal on the way back up.
    pub error: Option<Error>,
}

/// Owned bundle the consumer thread takes ownership of. Decrypt
/// happens on the producer side before send, so the consumer never
/// sees keys.
pub(super) struct ConsumerInputs {
    pub file: crate::io::WritebackFile,
    pub map: Mapfile,
    /// `sync_all`-on-failure-is-an-error iff the output is a regular
    /// file. `/dev/null` and pipes always fail `sync_all`; that's not
    /// a real error.
    pub is_regular: bool,
}

/// Spawn the consumer thread. The producer keeps the work-tx and
/// prog-rx; the join handle yields the final summary on `Finish` (or
/// on channel close).
pub(super) fn spawn_consumer(
    inputs: ConsumerInputs,
) -> (
    SyncSender<WorkItem>,
    Receiver<ProgressSnapshot>,
    JoinHandle<ConsumerSummary>,
) {
    let (work_tx, work_rx) = sync_channel::<WorkItem>(CHANNEL_DEPTH);
    let (prog_tx, prog_rx) = sync_channel::<ProgressSnapshot>(1);

    let handle = thread::Builder::new()
        .name("freemkv-sweep-consumer".into())
        .spawn(move || consumer_loop(inputs, work_rx, prog_tx))
        .expect("spawning sweep consumer thread should not fail");

    (work_tx, prog_rx, handle)
}

/// Send a work item, translating a `SendError` (consumer thread died
/// / panicked) into a useful library error so the caller can
/// propagate cleanly.
pub(super) fn send_or_abort(tx: &SyncSender<WorkItem>, item: WorkItem) -> Result<()> {
    tx.send(item).map_err(|_| Error::IoError {
        source: std::io::Error::other("sweep consumer terminated unexpectedly"),
    })
}

/// Best-effort `StatsRequest` send. If the channel is full, skip —
/// the producer's cached snapshot is fine for one more iteration.
pub(super) fn try_request_stats(tx: &SyncSender<WorkItem>) {
    if let Err(TrySendError::Full(_)) = tx.try_send(WorkItem::StatsRequest) {
        // expected when the consumer is busy; cached snapshot is
        // still recent enough.
    }
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

fn consumer_loop(
    mut inputs: ConsumerInputs,
    work_rx: Receiver<WorkItem>,
    prog_tx: SyncSender<ProgressSnapshot>,
) -> ConsumerSummary {
    let zero = [0u8; ZERO_CHUNK];
    let mut first_error: Option<Error> = None;

    // Channel closed without a Finish == treat as Finish (producer
    // dropped tx without explicit teardown — should not happen in
    // normal operation but be defensive).
    while let Ok(item) = work_rx.recv() {
        // Once we have an error, drain remaining items without
        // applying side-effects so the producer never blocks on a
        // dead consumer. Loop until Finish or channel close.
        if first_error.is_some() {
            if matches!(item, WorkItem::Finish) {
                break;
            }
            continue;
        }

        match apply_item(&mut inputs, item, &zero, &prog_tx) {
            Ok(true) => {}
            Ok(false) => break, // Finish received
            Err(e) => first_error = Some(e),
        }
    }

    // Final flush — drain the writeback pipeline + fsync the ISO,
    // then persist any pending mapfile state.
    if first_error.is_none() {
        if let Err(e) = inputs.file.sync_all() {
            if inputs.is_regular {
                first_error = Some(Error::IoError { source: e });
            }
            // Non-regular outputs (/dev/null, pipes) always fail
            // sync_all; that's not a real error.
        }
        if let Err(e) = inputs.map.flush() {
            first_error = Some(Error::IoError { source: e });
        }
    }

    ConsumerSummary {
        stats: inputs.map.stats(),
        error: first_error,
    }
}

/// Apply a single `WorkItem`. Returns `Ok(true)` to continue, `Ok(false)`
/// to break the consumer loop on `Finish`, `Err(_)` on first failure
/// (caller captures and continues draining).
fn apply_item(
    inputs: &mut ConsumerInputs,
    item: WorkItem,
    zero: &[u8; ZERO_CHUNK],
    prog_tx: &SyncSender<ProgressSnapshot>,
) -> Result<bool> {
    match item {
        WorkItem::Good { pos, buf } => {
            // Decrypt is on the producer; consumer assumes plaintext.
            let len = buf.len() as u64;
            inputs
                .file
                .seek(SeekFrom::Start(pos))
                .map_err(|e| Error::IoError { source: e })?;
            inputs
                .file
                .write_all(&buf)
                .map_err(|e| Error::IoError { source: e })?;
            inputs
                .map
                .record(pos, len, SectorStatus::Finished)
                .map_err(|e| Error::IoError { source: e })?;
        }
        WorkItem::BisectGood { pos, buf } => {
            inputs
                .file
                .seek(SeekFrom::Start(pos))
                .map_err(|e| Error::IoError { source: e })?;
            inputs
                .file
                .write_all(&buf[..])
                .map_err(|e| Error::IoError { source: e })?;
            inputs
                .map
                .record(pos, 2048, SectorStatus::Finished)
                .map_err(|e| Error::IoError { source: e })?;
        }
        WorkItem::BisectBad { pos } => {
            inputs
                .file
                .seek(SeekFrom::Start(pos))
                .map_err(|e| Error::IoError { source: e })?;
            inputs
                .file
                .write_all(&zero[..2048])
                .map_err(|e| Error::IoError { source: e })?;
            inputs
                .map
                .record(pos, 2048, SectorStatus::NonTrimmed)
                .map_err(|e| Error::IoError { source: e })?;
        }
        WorkItem::SkipFill { pos, len } | WorkItem::GapFill { pos, len } => {
            inputs
                .file
                .seek(SeekFrom::Start(pos))
                .map_err(|e| Error::IoError { source: e })?;
            // Subsequent writes are sequential; `crate::io::WritebackFile`'s
            // seek-elision keeps them on the writeback pipeline path.
            let mut filled = 0u64;
            while filled < len {
                let chunk = (len - filled).min(zero.len() as u64) as usize;
                inputs
                    .file
                    .write_all(&zero[..chunk])
                    .map_err(|e| Error::IoError { source: e })?;
                filled += chunk as u64;
            }
            inputs
                .map
                .record(pos, len, SectorStatus::NonTrimmed)
                .map_err(|e| Error::IoError { source: e })?;
        }
        WorkItem::StatsRequest => {
            let stats = inputs.map.stats();
            let bad_ranges = inputs.map.ranges_with(&[
                SectorStatus::NonTrimmed,
                SectorStatus::Unreadable,
                SectorStatus::NonScraped,
                SectorStatus::NonTried,
            ]);
            // Best-effort: drop on backpressure; producer's cache
            // stays current enough.
            let _ = prog_tx.try_send(ProgressSnapshot { stats, bad_ranges });
        }
        WorkItem::Finish => return Ok(false),
    }
    Ok(true)
}
