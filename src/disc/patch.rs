//! Producer / consumer split for `Disc::patch`.
//!
//! Background: pre-0.18 patch ran strictly serial — single-sector
//! recovery read → seek + write recovered bytes → mapfile.record →
//! next iteration. The drive sat idle while the previous block's
//! recovered bytes were committed. On a damaged disc with many bad
//! sectors that adds up: per-sector write + mapfile.record costs a
//! handful of milliseconds each, which the drive could be using to
//! issue the next per-sector retry.
//!
//! This module decouples them. A consumer thread owns the
//! [`crate::io::WritebackFile`] (the ISO file) and the
//! [`super::mapfile::Mapfile`]. The producer thread (`Disc::patch`)
//! keeps the [`crate::sector::SectorSource`], the wedge / damage-window
//! state, the per-range watchdog, decrypt — so what enters the channel
//! is already-clean cleartext bytes (or an "Unreadable" terminal mark).
//!
//! Producer and consumer run concurrently; the channel uses
//! [`crate::io::pipeline::WRITE_THROUGH_DEPTH`] (=1) so back-pressure
//! kicks in immediately. We want the drive's per-sector retry budget
//! to stay in lockstep with the writer — sweep's `DEFAULT_PIPELINE_DEPTH`
//! (4) would let several sectors of recovered bytes queue up between
//! the producer's retry decisions and the writer, and patch's recovery
//! loop reads stats (`bytes_good`, range progress) inline to drive its
//! skip / wedge decisions. WRITE_THROUGH_DEPTH gives "read N+1 while
//! writing N", no further pipelining — exactly the model the producer
//! logic was written against.
//!
//! Correctness invariants preserved:
//! - Mapfile is single-writer (consumer-only). No locking on it.
//! - All recovery state (damage window, consecutive_failures, skip
//!   escalation, range watchdog) stays on the producer thread.
//! - `set_speed` calls happen on the producer thread (same thread that
//!   owns the `SectorSource`). No new SCSI concurrency.
//! - Per-iteration ordering of file-write → mapfile-record is kept
//!   intact in the consumer (write before record), so the on-disk
//!   invariant "mapfile only marks Finished what the file has received"
//!   survives a crash mid-pass.
//! - The BU40N+Initio bridge wedge concern is unchanged: only one
//!   SCSI command in flight at a time, error-path timing identical,
//!   no new retry logic. The threading primitive only overlaps the
//!   *write* with the *next read*; the per-sector single-shot read
//!   budget that the bridge wedge concern was originally about is
//!   untouched.
//!
//! Per-range watchdog (`range_sectors × SECONDS_PER_SECTOR`, capped at `RANGE_BUDGET_CAP_SECS`)
//! checks `bytes_good` for forward progress. With work in flight on
//! the consumer, the producer would otherwise see stale values; the
//! sink publishes a [`SharedPatchState`] snapshot after every record
//! so the producer's stall guards observe consumer side-effects with
//! at most one item of lag (which is fine — the watchdog uses minute-
//! scale budgets, not single-record latency).

use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::io::pipeline::{Flow, Sink};

use super::mapfile::{self, MapStats, Mapfile, SectorStatus};
use super::section_recover::{
    Bisect, HandlerCtx, HandlerOutcome, Jump, Linear, RecoverySink, SectionHandler, run_handlers,
};

/// Wall-clock budget one recovery handler gets on a section before the chain
/// moves to the next idea (#55). Tight and bounded — this is what guarantees a
/// pass never hangs: a handler that can't shrink the still-bad set within this
/// window returns, the next handler tries a different idea, and whatever is
/// still bad becomes NonTrimmed residue so recovery advances to the next range.
/// Replaces the old 1800 s/range + 3600 s/pass grind budgets on the live path.
const PER_HANDLER_BUDGET_SECS: u64 = 60;

/// Minimum interval between progress heartbeats pushed from inside a handler, so
/// the UI's bar/speed move continuously during a long section without flooding
/// the reporter (see the tick closure in `recover_section`).
const PROGRESS_TICK_MS: u64 = 250;

/// Bridges the decoupled [`RecoverySink`] a handler writes to onto the live
/// patch consumer pipe: each recovered span becomes a [`PatchItem::Recovered`]
/// the consumer thread seeks + writes + records `Finished`. `recovered` can't
/// return an error (the trait is infallible so handlers stay simple), so a
/// pipe-closed / halt error is captured in `err` and surfaced by the caller
/// after `run_handlers` returns.
struct PatchRecoverySink<'a> {
    pipe: &'a Pipeline<PatchItem, PatchSummary>,
    err: Option<Error>,
}

impl RecoverySink for PatchRecoverySink<'_> {
    fn recovered(&mut self, pos: u64, buf: &[u8]) {
        if self.err.is_some() {
            return;
        }
        if let Err(e) = send_or_abort(
            self.pipe,
            PatchItem::Recovered {
                pos,
                buf: buf.to_vec(),
            },
        ) {
            self.err = Some(e);
        }
    }
}

/// Item the producer hands to the patch consumer. One per per-sector
/// recovery decision.
pub(super) enum PatchItem {
    /// Sector / small batch successfully recovered (and decrypted on the
    /// producer side if `opts.decrypt` was set). Consumer seeks to
    /// `pos`, writes `buf`, records the range as `Finished`.
    Recovered { pos: u64, buf: Vec<u8> },

    /// Producer exhausted retries on `[pos, pos+len)`. Consumer records
    /// the range as `Unreadable`. No file write — the existing zero-fill
    /// from sweep is preserved in place.
    ///
    /// Currently unused by `Disc::patch` itself (2026-05-11 design call:
    /// patch never marks `Unreadable` mid-multipass; bytes stay
    /// `NonTrimmed` so future passes get another shot at them). Kept
    /// in the enum for the orchestrator-side end-of-recovery promotion
    /// (autorip, after the final retry pass completes, promotes
    /// still-NonTrimmed bytes to Unreadable). The orchestrator (autorip)
    /// performs this promotion directly via `Mapfile::record()` after all
    /// retry passes complete, not by emitting to `PatchSink`. This variant
    /// remains unused by the library itself.
    #[allow(dead_code)]
    Unreadable { pos: u64, len: u64 },

    /// Producer marks `[pos, pos+len)` as `NonTrimmed`. Used for BOTH
    /// the per-range skip-limit case (remaining bytes never tried) AND
    /// individual sector failures (tried-but-failed within a pass).
    /// Both stay "hopeful" — a later pass retries them.
    ///
    /// CRITICAL: "NonTrimmed in pass N" does NOT mean "Unreadable
    /// forever." Drive reads are stochastic: the same sector that
    /// fails 10 times in Pass 2 may succeed on attempt 1 in Pass 3
    /// after temperature / bus state / prior-read patterns shift.
    /// Pre-2026-05-11 patch marked individual failures Unreadable,
    /// which gave up on sectors that subsequent passes could have
    /// recovered (historical: ~36% of patch-marked Unreadable
    /// sectors turned out to be readable in re-rip experiments).
    /// Promotion to true Unreadable is the orchestrator's job,
    /// applied once after all retry passes complete.
    NonTrimmed { pos: u64, len: u64 },
}

/// Mapfile snapshot the sink republishes after every record so the
/// producer can drive its stall / progress logic without holding the
/// mapfile lock for long. `bad_ranges` mirrors what
/// `Mapfile::ranges_with(&[NonTrimmed, Unreadable, NonScraped, NonTried])`
/// would return — same set the pre-split patch loop computed inline
/// for the progress callback.
pub(super) struct SharedPatchState {
    pub stats: MapStats,
    pub bad_ranges: Vec<(u64, u64)>,
}

impl SharedPatchState {
    /// Cap on the republished `bad_ranges` Vec. Consumers (progress display,
    /// scheduler) only sample the head of the list; the full set is bounded by
    /// the mapfile entry cap so a pathologically fragmented disc can't make
    /// every per-record republish allocate unboundedly.
    const MAX_BAD_RANGES: usize = 8192;

    fn from_map(map: &Mapfile) -> Self {
        let mut bad_ranges = map.ranges_with(&[
            SectorStatus::NonTrimmed,
            SectorStatus::Unreadable,
            SectorStatus::NonScraped,
            SectorStatus::NonTried,
        ]);
        bad_ranges.truncate(Self::MAX_BAD_RANGES);
        Self {
            stats: map.stats(),
            bad_ranges,
        }
    }
}

/// Final summary returned by [`Sink::close`] when the consumer drains
/// cleanly. Mirrors what the pre-split patch loop computed at the end
/// of the function — final mapfile stats plus whether `sync_all`
/// failed on a regular file (the only kind of fsync error patch ever
/// surfaced; `/dev/null` and pipes always fail `sync_all`, that's not
/// a real error).
pub(super) struct PatchSummary {
    pub stats: MapStats,
}

/// Consumer-side of the patch pipeline. Owns the ISO writeback file
/// and the mapfile; publishes a shared snapshot after every record so
/// the producer can read `bytes_good` for stall detection and
/// progress reporting.
pub(super) struct PatchSink {
    file: crate::io::WritebackFile,
    map: Mapfile,
    /// Whether the output is a regular file (so a `sync_all` failure
    /// is real). `/dev/null` etc. always fail `sync_all`; ignore those.
    is_regular: bool,
    /// Snapshot the producer reads. Updated after every successful
    /// `record()` call. `Mutex` rather than separate atomics because
    /// the producer wants stats + bad_ranges as a coherent pair.
    shared: Arc<Mutex<SharedPatchState>>,
    /// Last time the shared snapshot was republished. `from_map` allocates
    /// O(bad_ranges) every call, so the per-record path throttles to a time
    /// cadence (`REPUBLISH_CADENCE`); the final close always forces a publish.
    last_republish: Option<std::time::Instant>,
}

/// Minimum interval between per-record snapshot republishes.
const REPUBLISH_CADENCE: std::time::Duration = std::time::Duration::from_millis(250);

impl PatchSink {
    /// Open `path` as a [`crate::io::WritebackFile`] and pair it with
    /// `map` for the consumer. The producer holds onto the returned
    /// `Arc<Mutex<SharedPatchState>>` so it can poll mapfile state
    /// while the consumer is mutating it.
    pub(super) fn new(
        path: &std::path::Path,
        map: Mapfile,
        is_regular: bool,
    ) -> Result<(Self, Arc<Mutex<SharedPatchState>>)> {
        let file =
            crate::io::WritebackFile::open(path).map_err(|e| Error::IoError { source: e })?;
        let shared = Arc::new(Mutex::new(SharedPatchState::from_map(&map)));
        let shared_clone = shared.clone();
        Ok((
            Self {
                file,
                map,
                is_regular,
                shared,
                last_republish: None,
            },
            shared_clone,
        ))
    }

    /// Republish the shared snapshot. When `force` is false the update is
    /// throttled to `REPUBLISH_CADENCE`; `force` (used at close) always
    /// publishes the final state.
    fn republish(&mut self, force: bool) {
        let now = std::time::Instant::now();
        if !force {
            if let Some(prev) = self.last_republish {
                if now.duration_since(prev) < REPUBLISH_CADENCE {
                    return;
                }
            }
        }
        self.last_republish = Some(now);
        self.publish_now();
    }

    fn publish_now(&self) {
        // Best-effort lock — only the producer reads, only the consumer
        // writes; contention is single-acquire so the lock is never
        // poisoned in practice. If it ever did get poisoned we'd want
        // the underlying error surfaced rather than silently swallowed,
        // so we propagate the poison panic rather than silently
        // continuing with stale shared state.
        let mut guard = self
            .shared
            .lock()
            .expect("PatchSink shared state mutex poisoned");
        *guard = SharedPatchState::from_map(&self.map);
    }
}

impl Sink<PatchItem> for PatchSink {
    type Output = PatchSummary;

    fn apply(&mut self, item: PatchItem) -> std::result::Result<Flow, Error> {
        match item {
            PatchItem::Recovered { pos, buf } => {
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
            PatchItem::Unreadable { pos, len } => {
                self.map
                    .record(pos, len, SectorStatus::Unreadable)
                    .map_err(|e| Error::IoError { source: e })?;
            }
            PatchItem::NonTrimmed { pos, len } => {
                self.map
                    .record(pos, len, SectorStatus::NonTrimmed)
                    .map_err(|e| Error::IoError { source: e })?;
            }
        }
        self.republish(false);
        Ok(Flow::Continue)
    }

    fn close(mut self) -> std::result::Result<Self::Output, Error> {
        // Drain in-flight writeback then issue a full fsync. A failure
        // here matters only on regular files — pipes / `/dev/null` etc.
        // always fail `sync_all`.
        if let Err(e) = self.file.sync_all() {
            if self.is_regular {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "patch.sync.failed",
                    error = %e,
                    os_error = e.raw_os_error(),
                    error_kind = ?e.kind(),
                    "patch: sync_all failed"
                );
                return Err(Error::IoError { source: e });
            }
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch.sync.skipped",
                error = %e,
                "patch: sync_all failed for non-regular file; ignoring"
            );
        }
        self.map.flush().map_err(|e| Error::IoError { source: e })?;
        // Final republish so anyone reading the shared snapshot after
        // `Pipeline::finish` sees the post-flush state. (The producer
        // already has its own copy of the final `MapStats` in the
        // returned `PatchSummary`, but the snapshot is part of the
        // public-ish contract of the consumer: it stays current
        // through close.)
        self.republish(true);
        Ok(PatchSummary {
            stats: self.map.stats(),
        })
    }
}

// ─────────────────────────────────────────────────────────────────
// Disc::patch + bytes_bad_in_title — extracted from disc/mod.rs in
// 0.20.1. Behavior unchanged; the move splits the 3,900-line mod.rs
// into a cleaner-to-read file.
// ─────────────────────────────────────────────────────────────────

use super::{Disc, DiscTitle, PatchOptions, PatchOutcome, bytes_bad_in_title};
use crate::io::pipeline::Pipeline;
use crate::sector::SectorSource;

/// Breadth-first recovery tiers. Tier 0 fast-sweeps every bad range; tier 1
/// deep-recovers the residual. See `PatchCtx::run`.
const PATCH_TIERS: usize = 2;

/// Send a `PatchItem` and translate a `SendError` (consumer thread died
/// / panicked) into a library error so the caller propagates cleanly.
pub(super) fn send_or_abort(
    pipe: &Pipeline<PatchItem, PatchSummary>,
    item: PatchItem,
) -> Result<()> {
    pipe.send(item).map_err(|_| Error::PipelineConsumerGone)
}

/// Phase A pre-snapshot. Loads the mapfile, captures the fields the
/// patch loop needs after the live `Mapfile` moves into the consumer
/// thread (`bytes_good` baseline, total stats, entry snapshot for
/// the diagnostic dump, the initial bad-range work list, total work
/// in bytes, and the `is_regular` test that gates the post-pass
/// `sync_all` error policy). Returned `Mapfile` is the same object
/// that was loaded — caller passes ownership into `PatchSink::new`.
#[allow(clippy::type_complexity)]
pub(super) fn compute_initial_state(
    path: &std::path::Path,
    opts: &PatchOptions,
    mapfile_path: &std::path::Path,
) -> Result<(
    Mapfile,
    MapStats,
    Vec<mapfile::MapEntry>,
    u64,
    Vec<(u64, u64)>,
    u64,
    bool,
)> {
    let map = mapfile::Mapfile::load(mapfile_path).map_err(|e| Error::IoError { source: e })?;
    let total_bytes = map.total_size();
    let initial_stats = map.stats();
    let initial_entries: Vec<_> = map.entries().to_vec();
    // Every retry pass acts on NonTrimmed, NonScraped, and Unreadable
    // ranges. Including Unreadable means a sector that failed in pass N
    // gets a fresh shot in pass N+1 — drive state evolves, the same
    // read can succeed later. Each pass owns its own jumps/skips; if
    // pass 5 jumps over the same zone as pass 2, fine. NonTried ranges
    // are intentionally excluded — they are covered by a preceding
    // sweep pass, not by patch.
    let mut bad_ranges = map.ranges_with(&[
        mapfile::SectorStatus::NonTrimmed,
        mapfile::SectorStatus::NonScraped,
        mapfile::SectorStatus::Unreadable,
    ]);
    if opts.reverse {
        bad_ranges.reverse();
    }
    let work_total: u64 = bad_ranges.iter().map(|(_, sz)| *sz).sum();
    let is_regular = std::fs::metadata(path)
        .map(|m| m.file_type().is_file())
        .unwrap_or(false);
    Ok((
        map,
        initial_stats,
        initial_entries,
        total_bytes,
        bad_ranges,
        work_total,
        is_regular,
    ))
}

/// One recovery read of `[lba, lba+count)` into `buf[..count*2048]`.
///
/// On an AACS disc a mid-unit window (start or length not unit-aligned)
/// is widened to the enclosing aligned 3-sector unit, decrypted, and the
/// originally-requested window copied back out: the decrypting reader
/// rejects an unaligned read (`DecryptFailed`) and the sector would be
/// abandoned without the drive ever being asked. Units anchor at offset
/// 0, so the widened start is always unit-aligned. All recovery
/// accounting upstream (pos, block_bytes, dispatched lba/count) is
/// unchanged — only the physical read widens, so the cursor cannot
/// desync. `recovery` selects the SCSI timeout (true = 60 s deep
/// recovery, false = the fast path).
pub(super) fn recovery_read<R: SectorSource + ?Sized>(
    reader: &mut R,
    decrypt_is_aacs: bool,
    lba: u32,
    count: u16,
    buf: &mut [u8],
    recovery: bool,
) -> Result<usize> {
    let bytes = count as usize * 2048;
    if decrypt_is_aacs && (lba % 3 != 0 || count % 3 != 0) {
        const U: u32 = 3;
        let aligned_lba = lba - (lba % U);
        let head = (lba - aligned_lba) as usize; // lead-in sectors
        let span = head + count as usize;
        let aligned_count = span + ((U as usize - span % U as usize) % U as usize);
        let mut scratch = vec![0u8; aligned_count * 2048];
        reader.read_sectors(aligned_lba, aligned_count as u16, &mut scratch, recovery)?;
        buf[..bytes].copy_from_slice(&scratch[head * 2048..head * 2048 + bytes]);
        Ok(bytes)
    } else {
        reader.read_sectors(lba, count, &mut buf[..bytes], recovery)
    }
}

/// The still-bad `[pos, len)` sub-ranges of one bad section, in byte offsets
/// (all multiples of 2048), kept sorted and non-overlapping. The per-section
/// recovery rework (#50) threads one of these through the recovery phase
/// helpers: each phase RECOVERS some bytes and calls [`SubRanges::remove`] to
/// shrink the set; whatever remains after all phases is the dead residue that
/// gets recorded NonTrimmed. Pure data structure — no I/O — so each phase
/// helper is unit-testable by asserting the residual `SubRanges`.
///
/// Foundation for the phased `recover_section` orchestrator; not yet wired
/// into the live loop (see the deferral note in the #50 work).
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SubRanges {
    /// (pos, len) pairs, sorted by pos, non-overlapping, all non-zero len.
    ranges: Vec<(u64, u64)>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl SubRanges {
    /// One whole bad section.
    pub(super) fn from_section(pos: u64, len: u64) -> Self {
        let ranges = if len == 0 {
            Vec::new()
        } else {
            vec![(pos, len)]
        };
        Self { ranges }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Total still-bad bytes across all sub-ranges.
    pub(super) fn total_len(&self) -> u64 {
        self.ranges.iter().map(|&(_, l)| l).sum()
    }

    pub(super) fn ranges(&self) -> &[(u64, u64)] {
        &self.ranges
    }

    /// Remove the recovered byte-range `[pos, pos+len)` from the bad set,
    /// splitting any sub-range it bisects and trimming any it overlaps. A
    /// range fully covered is dropped; a removal landing in a gap is a no-op.
    /// This is how a phase helper records "these bytes are no longer bad".
    pub(super) fn remove(&mut self, pos: u64, len: u64) {
        if len == 0 {
            return;
        }
        let rend = pos + len;
        let mut out: Vec<(u64, u64)> = Vec::with_capacity(self.ranges.len() + 1);
        for &(rp, rl) in &self.ranges {
            let re = rp + rl;
            // Disjoint: keep whole.
            if rend <= rp || pos >= re {
                out.push((rp, rl));
                continue;
            }
            // Left remainder [rp, pos).
            if pos > rp {
                out.push((rp, pos - rp));
            }
            // Right remainder [rend, re).
            if rend < re {
                out.push((rend, re - rend));
            }
            // Otherwise the overlap consumed this whole sub-range.
        }
        self.ranges = out;
    }
}

/// Pre-loop diagnostic dump: emits `patch_mapfile_snapshot` plus the
/// first/last 10 entries (info + per-entry debug). Pure logging — no
/// state mutation. Pulled out of `Disc::patch` so the coordination
/// body stays compact; the operator's grep patterns for
/// `[disc] patch_mapfile_snapshot`, `patch_mapfile_entries_start`,
/// `patch_mapfile_entry_start`, `patch_mapfile_entries_end`,
/// `patch_mapfile_entry_end` are unchanged.
pub(super) fn log_patch_start_snapshot(
    initial_entries: &[mapfile::MapEntry],
    initial_stats: &mapfile::MapStats,
    bytes_good_before: u64,
) {
    tracing::info!(
        target: "freemkv::disc",
        phase = "patch.mapfile.snapshot",
        total_entries = initial_entries.len(),
        bytes_good_before,
        bytes_retryable = initial_stats.bytes_retryable,
        bytes_unreadable = initial_stats.bytes_unreadable,
        bytes_nontried = initial_stats.bytes_nontried,
        "Mapfile state snapshot at patch start"
    );

    if !initial_entries.is_empty() {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.mapfile.entries.start",
            num_to_log = (initial_entries.len().min(10)) as u32,
            "First 10 entries"
        );
        for entry in initial_entries.iter().take(10) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch.mapfile.entry.start",
                pos_hex = format!("0x{:09x}", entry.pos),
                size_mb = entry.size as f64 / 1_048_576.0,
                status_char = entry.status.to_char() as u8 as i32,
                "Mapfile entry"
            );
        }
    }
    if initial_entries.len() > 10 {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.mapfile.entries.end",
            num_to_log = (initial_entries.len().min(10)) as u32,
            "Last 10 entries"
        );
        for entry in initial_entries.iter().skip(initial_entries.len() - 10) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch.mapfile.entry.end",
                pos_hex = format!("0x{:09x}", entry.pos),
                size_mb = entry.size as f64 / 1_048_576.0,
                status_char = format!("{}", entry.status.to_char()),
                "Mapfile entry"
            );
        }
    }
}

/// Bundle final mapfile stats + accumulated loop counters into the
/// public `PatchOutcome` the caller consumes. The post-loop tracing
/// (`patch_iso_size_end`, `patch_done`) is also emitted here so the
/// coordination body has one less inline stanza.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_outcome(
    state: &PatchLoopState,
    summary: &PatchSummary,
    path: &std::path::Path,
    total_bytes: u64,
    num_ranges: usize,
    wedged_threshold: u64,
) -> PatchOutcome {
    let stats = summary.stats;

    if let Ok(metadata) = std::fs::metadata(path) {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.iso_size.end",
            iso_bytes = metadata.len(),
            bytes_recovered = stats.bytes_good.saturating_sub(state.bytes_good_before),
            "ISO file size at patch end"
        );
    }

    tracing::info!(
        target: "freemkv::disc",
        phase = "patch.done",
        blocks_attempted = state.blocks_attempted,
        blocks_read_ok = state.blocks_read_ok,
        blocks_read_failed = state.blocks_read_failed,
        unreadable_count = state.unreadable_count,
        wedged_exit = state.wedged_exit,
        halted = state.halted,
        bytes_recovered = stats.bytes_good.saturating_sub(state.bytes_good_before),
        final_bytes_good = stats.bytes_good,
        final_bytes_unreadable = stats.bytes_unreadable,
        final_bytes_pending = stats.bytes_pending,
        total_ranges_processed = num_ranges,
        "Disc::patch returning"
    );

    PatchOutcome {
        bytes_total: total_bytes,
        bytes_good: stats.bytes_good,
        bytes_unreadable: stats.bytes_unreadable,
        bytes_pending: stats.bytes_pending,
        bytes_recovered_this_pass: stats.bytes_good.saturating_sub(state.bytes_good_before),
        halted: state.halted,
        blocks_attempted: state.blocks_attempted,
        blocks_read_ok: state.blocks_read_ok,
        blocks_read_failed: state.blocks_read_failed,
        wedged_exit: state.wedged_exit,
        wedged_threshold,
    }
}

/// Per-pass loop state, accumulated across every range and every read
/// inside `Disc::patch`. Lives on the producer thread; helpers take
/// `&mut PatchLoopState` so they can mutate counters and per-range
/// scratch without an explosion of parameters at the call site.
pub(super) struct PatchLoopState {
    // Counters
    pub halted: bool,
    pub wedged_exit: bool,
    pub blocks_attempted: u64,
    pub blocks_read_ok: u64,
    pub blocks_read_failed: u64,
    pub unreadable_count: u64,
    pub work_done: u64,
    // Clock seam: the handler chain reads wall time through this rather than
    // calling `Instant::now()` inline, so the per-handler deadline is driven by
    // an injectable clock and deterministic tests can wind it forward.
    pub now: fn() -> std::time::Instant,
    // Snapshot at construction — these stay constant for the whole pass.
    pub bytes_good_before: u64,
    #[allow(dead_code)]
    pub total_bytes: u64,
    pub initial_batch: u16,
    pub work_total: u64,
}

impl PatchLoopState {
    pub(super) fn new(
        bytes_good_before: u64,
        total_bytes: u64,
        initial_batch: u16,
        work_total: u64,
    ) -> Self {
        // Production clock: the real monotonic wall clock.
        Self::new_with_clock(
            bytes_good_before,
            total_bytes,
            initial_batch,
            work_total,
            std::time::Instant::now,
        )
    }

    /// Like `new`, but with an injectable monotonic clock so a test can wind a
    /// fake clock forward to drive the per-handler deadline deterministically.
    /// `new` passes `Instant::now`, so the production path is unchanged.
    pub(super) fn new_with_clock(
        bytes_good_before: u64,
        total_bytes: u64,
        initial_batch: u16,
        work_total: u64,
        now: fn() -> std::time::Instant,
    ) -> Self {
        Self {
            halted: false,
            wedged_exit: false,
            blocks_attempted: 0,
            blocks_read_ok: 0,
            blocks_read_failed: 0,
            unreadable_count: 0,
            work_done: 0,
            now,
            bytes_good_before,
            total_bytes,
            initial_batch,
            work_total,
        }
    }
}

/// Why [`PatchCtx::patch_region`] returned. The orchestrator
/// ([`PatchCtx::run`]) advances to the next bad range on `Completed` (the
/// handler chain always drains a section to recovered-or-residue, so there is
/// no per-range abort), and ends the whole pass only on `Halted` or
/// `TransportFault` — for which the matching `state.halted` / `state.wedged_exit`
/// flag was already set, so `build_outcome` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegionOutcome {
    /// Section drained: recovered what was readable, left the rest NonTrimmed.
    Completed,
    /// Halt requested — the halt token or the progress reporter.
    /// `state.halted` is set.
    Halted,
    /// USB-bridge transport fault: a dead bus, not a bad sector.
    /// `state.wedged_exit` is set.
    TransportFault,
}

/// Per-pass coordination state for one `Disc::patch` run: the decrypting
/// reader, the consumer pipe + its shared mapfile snapshot, the options,
/// and the accumulating [`PatchLoopState`]. Bundling these lets the
/// orchestrator ([`PatchCtx::run`]) and the focused per-range recovery
/// loop ([`PatchCtx::patch_region`]) be methods rather than free
/// functions threading a dozen arguments. `state` carries ACROSS ranges
/// (counters, stall timers, NOT_READY/last-skip cursors); the per-range
/// scratch inside it is reset at the top of each `patch_region`.
struct PatchCtx<'a, 'o> {
    disc: &'a Disc,
    reader: &'a mut dyn SectorSource,
    pipe: &'a Pipeline<PatchItem, PatchSummary>,
    shared: &'a Mutex<SharedPatchState>,
    opts: &'a PatchOptions<'o>,
    total_bytes: u64,
    decrypt_is_aacs: bool,
    state: PatchLoopState,
}

impl PatchCtx<'_, '_> {
    /// Orchestrator (one pass): walk the ordered bad ranges. Apply the
    /// inter-range cooldown only after a range that grinded, then recover
    /// the range; stop the whole pass the moment a range reports
    /// halt / wedge / transport-fault.
    fn run(&mut self, bad_ranges: &[(u64, u64)]) -> Result<()> {
        let num_ranges = bad_ranges.len();
        // Attack the LARGEST ranges first. The big NonTrimmed regions are usually
        // sweep-jump over-marks that read straight back, so ordering them ahead of
        // the many tiny dead fragments lets tier 0 recover the bulk of the disc in
        // its first minutes instead of grinding fragments first (ties: low LBA
        // first for a predictable, mostly-sequential walk).
        let mut ordered: Vec<(u64, u64)> = bad_ranges.to_vec();
        ordered.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        // Per-range still-bad sets, persisted ACROSS the breadth-first tiers so
        // tier N+1 works on exactly what tier N left behind.
        let mut sections: Vec<SubRanges> = ordered
            .iter()
            .map(|&(p, l)| SubRanges::from_section(p, l))
            .collect();

        // BREADTH-FIRST recovery. Tier 0 fast-sweeps EVERY range first — grabbing
        // the easily-readable bulk across the whole disc (sweep-jump over-marks a
        // big region NonTrimmed without testing each sector, so most of it reads
        // back in seconds) — BEFORE any range's slow per-sector grind. Tier 1
        // then deep-recovers only the residual. This fixes the depth-first
        // starvation bug: the full chain used to run per range, so a small dead
        // cluster at the front burned ~5 min/range and the big
        // mostly-recoverable ranges were never reached.
        for tier in 0..PATCH_TIERS {
            let final_tier = tier + 1 == PATCH_TIERS;
            for (range_idx, &(range_pos, range_size)) in ordered.iter().enumerate() {
                if sections[range_idx].is_empty() {
                    continue; // already fully recovered by an earlier tier
                }
                let outcome = self.recover_section(
                    tier,
                    range_idx,
                    num_ranges,
                    range_pos,
                    range_size,
                    &mut sections[range_idx],
                    final_tier,
                )?;
                match outcome {
                    RegionOutcome::Completed => {}
                    RegionOutcome::Halted | RegionOutcome::TransportFault => return Ok(()),
                }
            }
        }
        Ok(())
    }

    /// Run ONE breadth-first tier of the handler chain over one range's still-bad
    /// set `bad`. Tier 0 = the fast breadth handlers (grab the readable bulk,
    /// fast-fail the rest); tier 1 = deep recovery (slow reads) + bisect on the
    /// residual. `final_tier` records the surviving residue as NonTrimmed and
    /// accounts the range toward progress exactly once. Cross-range scheduling
    /// lives in [`PatchCtx::run`]; this owns one (tier, range) unit of work.
    #[allow(clippy::too_many_arguments)]
    fn recover_section(
        &mut self,
        tier: usize,
        range_idx: usize,
        num_ranges: usize,
        range_pos: u64,
        range_size: u64,
        bad: &mut SubRanges,
        final_tier: bool,
    ) -> Result<RegionOutcome> {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.region.enter",
            tier,
            range_index = range_idx,
            num_total_ranges = num_ranges,
            range_lba = range_pos / 2048,
            range_size_mb = range_size as f64 / 1_048_576.0,
            bad_bytes = bad.total_len(),
            "entering patch range"
        );

        // Enter at max read speed; a handler drops to the deep-recovery read
        // itself via its `fast` flag.
        self.reader.set_speed(0xFFFF);

        // Tier 0: the fast handlers only — sweep the readable bulk of EVERY range
        // before any slow grind. Tier 1: slow deep-recovery + bisect on what tier
        // 0 left. Adding a recovery idea is one more entry in the right tier (#55).
        let mut handlers: Vec<Box<dyn SectionHandler>> = if tier == 0 {
            // Tier 0 = a SINGLE fast scout (Jump only). It streams the big
            // readable wins back and skips dead runs in seconds — so the pass
            // sweeps every range fast, recovers the recoverable bulk largest
            // first, and converges to the small genuine-dead residue instead of
            // spending 3 handlers × 60 s grinding every dead fragment. Order of
            // recovery is exactly big-wins → smaller → smallest, then grind.
            vec![Box::new(Jump)]
        } else {
            // Tier 1 = deep recovery on the (now small) residue: fast full-batch
            // mop-up of anything Jump stepped over, then slow deep-recovery reads,
            // then Bisect for readable islands inside a mostly-dead chunk.
            vec![
                Box::new(Linear {
                    reverse: true,
                    fast: true,
                }),
                Box::new(Linear {
                    reverse: false,
                    fast: true,
                }),
                Box::new(Linear {
                    reverse: true,
                    fast: false,
                }),
                Box::new(Linear {
                    reverse: false,
                    fast: false,
                }),
                Box::new(Bisect),
            ]
        };

        // Clock seam: handlers read wall time through this so tests can wind a
        // fake clock (the same seam the pass uses for its own timing).
        let now_ptr = self.state.now;
        let now_fn = move || now_ptr();

        let mut sink = PatchRecoverySink {
            pipe: self.pipe,
            err: None,
        };

        let bad_before = bad.total_len();
        let outcome = {
            // Progress heartbeat: a throttled closure that pushes a fresh
            // snapshot to the reporter as recovery happens (called from every
            // read via `HandlerCtx::progress`), so the bar and speed move DURING
            // a handler instead of only when a section finishes. Scoped to this
            // block so its borrow of `self.state` ends before the post-tier
            // accounting below.
            let disc = self.disc;
            let opts = self.opts;
            let shared = self.shared;
            let total_bytes = self.total_bytes;
            let state = &self.state;
            let last_tick = std::cell::Cell::new(now_ptr());
            let mut tick = move || {
                let t = now_ptr();
                if t.duration_since(last_tick.get())
                    >= std::time::Duration::from_millis(PROGRESS_TICK_MS)
                {
                    last_tick.set(t);
                    let _ = disc.report_patch_progress(state, opts, total_bytes, shared);
                }
            };
            let mut ctx = HandlerCtx {
                reader: &mut *self.reader,
                sink: &mut sink,
                now: &now_fn,
                halt: self.opts.halt.as_deref(),
                decrypt_is_aacs: self.decrypt_is_aacs,
                tick: Some(&mut tick),
            };
            run_handlers(&mut ctx, &mut handlers, bad, |_bad| {
                now_ptr() + std::time::Duration::from_secs(PER_HANDLER_BUDGET_SECS)
            })
        };

        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.region.exit",
            tier,
            range_index = range_idx,
            range_lba = range_pos / 2048,
            outcome = ?outcome,
            bad_bytes_before = bad_before,
            bad_bytes_after = bad.total_len(),
            recovered = bad_before.saturating_sub(bad.total_len()),
            "region tier finished"
        );

        // A pipe-closed / halt error captured while emitting recovered spans is
        // fatal to the pass.
        if let Some(e) = sink.err.take() {
            return Err(e);
        }

        // On the FINAL tier, whatever is still bad is this pass's residue: record
        // NonTrimmed and account the range toward progress (once). A later pass —
        // or a future handler — gets another shot; the orchestrator promotes
        // still-NonTrimmed to Unreadable only after the final pass completes.
        if final_tier {
            for &(pos, len) in bad.ranges() {
                send_or_abort(self.pipe, PatchItem::NonTrimmed { pos, len })?;
            }
            self.state.work_done = self.state.work_done.saturating_add(range_size);
        }

        if self
            .disc
            .report_patch_progress(&self.state, self.opts, self.total_bytes, self.shared)
        {
            self.state.halted = true;
            return Ok(RegionOutcome::Halted);
        }

        match outcome {
            // Whether the chain cleared the section or left residue, we always
            // advance to the next range — never hang, never abort mid-pass.
            HandlerOutcome::Complete | HandlerOutcome::Remaining => Ok(RegionOutcome::Completed),
            HandlerOutcome::Halted => {
                self.state.halted = true;
                Ok(RegionOutcome::Halted)
            }
            // Bridge/transport crash: end the pass so the orchestrator can
            // spin-cycle the drive and resume from the mapfile next pass.
            HandlerOutcome::TransportFault => {
                self.state.wedged_exit = true;
                Ok(RegionOutcome::TransportFault)
            }
        }
    }
}

impl Disc {
    /// Build + dispatch a `PassProgress` to the caller's reporter,
    /// using the current pipeline-shared mapfile snapshot. Needs
    /// `&self` for `self.titles`. Returns `true` if the reporter
    /// asked us to halt (i.e. the outer loop should set
    /// `state.halted` and break).
    pub(super) fn report_patch_progress(
        &self,
        state: &PatchLoopState,
        opts: &PatchOptions,
        total_bytes: u64,
        shared: &Mutex<SharedPatchState>,
    ) -> bool {
        let Some(reporter) = opts.progress else {
            return false;
        };
        let (s, bad_ranges_now) = {
            let g = shared
                .lock()
                .expect("PatchSink shared state mutex poisoned");
            (g.stats, g.bad_ranges.clone())
        };
        let kind = if state.initial_batch == 1 {
            crate::progress::PassKind::Scrape {
                reverse: opts.reverse,
            }
        } else {
            crate::progress::PassKind::Trim {
                reverse: opts.reverse,
            }
        };
        let main_title_bad = self
            .titles
            .first()
            .map(|t| bytes_bad_in_title(t, &bad_ranges_now))
            .unwrap_or(0);
        let main_title = self.titles.first();
        // Progress = bytes RECOVERED so far (initial bad − still-pending), not a
        // per-range counter. With breadth-first tiers the readable bulk comes
        // back during tier 0 before any range is "finished", so a range-counter
        // sits at 0% while hundreds of MB are actually recovered. Deriving it
        // from the live pending count makes the bar (and the speed the client
        // computes from its delta) reflect real recovery the instant it happens.
        let recovered = state.work_total.saturating_sub(s.bytes_pending);
        let pp = crate::progress::PassProgress {
            kind,
            work_done: recovered,
            work_total: state.work_total,
            bytes_good_total: s.bytes_good,
            bytes_unreadable_total: s.bytes_unreadable,
            bytes_pending_total: s.bytes_pending,
            bytes_retryable_total: s.bytes_retryable,
            bytes_total_disc: total_bytes,
            disc_duration_secs: main_title.map(|t| t.duration_secs),
            bytes_bad_in_main_title: main_title_bad,
            main_title_duration_secs: main_title.map(|t| t.duration_secs),
            main_title_size_bytes: main_title.map(|t| t.size_bytes),
            // The rendered drilldown — located ranges + at-risk movie time —
            // computed here from the in-memory bad-range set + title so the
            // client renders it verbatim and never reads the mapfile.
            located: main_title
                .map(|t| crate::disc::locate_ranges(&bad_ranges_now, t))
                .unwrap_or_default(),
        };
        !reporter.report(&pp)
    }

    /// Bytes of bad/unreadable data in a title's extents, from a mapfile.
    ///
    /// Consumers (CLI, autorip) call this after a rip pass to determine
    /// how much damage affects a particular title — useful for showing
    /// "42s lost (12s in main movie)" in the UI.
    pub fn bytes_bad_in_title(&self, mapfile_path: &std::path::Path, title: &DiscTitle) -> u64 {
        let map = match mapfile::Mapfile::load(mapfile_path) {
            Ok(m) => m,
            Err(_) => return 0,
        };
        let bad_ranges = map.ranges_with(&[
            mapfile::SectorStatus::NonTrimmed,
            mapfile::SectorStatus::Unreadable,
            mapfile::SectorStatus::NonScraped,
            mapfile::SectorStatus::NonTried,
        ]);
        bytes_bad_in_title(title, &bad_ranges)
    }

    /// Pass 2..N of a multipass rip: re-read the bad ranges
    /// recorded in the sidecar mapfile and try to recover them.
    /// With `reverse: true` (the default for the recovery walker),
    /// the bad-range walk runs end-to-start so escalating skips
    /// converge on the actual bad sub-zones inside any
    /// `NonTrimmed` block. Returns a [`PatchOutcome`] with
    /// recovered byte counts and wedge-detection signals.
    ///
    /// Paired with [`Disc::sweep`] as the library's other flat
    /// rip-phase verb. Caller drives the retry loop and the
    /// sweep-vs-patch dispatch.
    pub fn patch(
        &self,
        reader: &mut dyn SectorSource,
        path: &std::path::Path,
        opts: &PatchOptions,
    ) -> Result<PatchOutcome> {
        use crate::io::pipeline::{Pipeline, WRITE_THROUGH_DEPTH};
        use crate::sector::DecryptingSectorSource;

        // Pre-flight decrypt gate (also enforced in `copy`; re-checked here so a
        // direct `patch` caller can't bypass it). A decrypting patch pass of an
        // encrypted disc with no usable key would write ciphertext into the ISO's
        // recovered ranges; refuse before reading any sector. No-op for `--raw`
        // (`opts.decrypt == false`) and unencrypted discs.
        self.ensure_decryptable(!opts.decrypt)?;

        let patch_t0 = std::time::Instant::now();
        let mapfile_path = self.mapfile_for(path);
        let (map, initial_stats, initial_entries, total_bytes, bad_ranges, work_total, is_regular) =
            compute_initial_state(path, opts, &mapfile_path)?;
        tracing::info!(
            target: "freemkv::scan",
            phase = "patch",
            num_ranges = bad_ranges.len(),
            reverse = opts.reverse,
            "begin"
        );
        let bytes_good_before = initial_stats.bytes_good;
        let bytes_good_start = bytes_good_before;

        // Post-read verify gate for the patch pass (ciphertext multipass only,
        // `!opts.decrypt`). Built here from the raw reader's UDF enumeration;
        // reused AFTER the recovery loop (`reverify_iso`) to re-check the units
        // this pass touched by reading them WHOLE back from the patched ISO —
        // patch re-reads only the bad sectors of a unit, so per-unit verify
        // can't run live. Fail-safe `None` when disabled / non-AACS / no keys.
        let mut verifier = if opts.decrypt {
            None
        } else {
            let verify_keys = self.decrypt_keys();
            let layouts = crate::disc::extract::clip_layouts(&mut *reader);
            crate::disc::verify::UnitVerifier::new(&layouts, &verify_keys, opts.key_fetch.clone())
        };
        // Decrypt-aware read — symmetric with `Disc::sweep`. A decrypting patch
        // (`opts.decrypt`) decrypts in place (plaintext ISO). A NON-decrypting
        // patch (the multipass / `--raw --multipass` path) resolves the keys and
        // VERIFIES each unit on a scratch copy: a re-read that STILL won't decrypt
        // fails the read (`DECRYPT_VERIFY_READ`) and stays NonTrimmed, so the
        // retry loop keeps re-reading it "until it decrypts or retries exhaust"
        // exactly as for a SCSI read error — and a unit that DOES decrypt on a
        // fresh read (the drive returned different bytes) is recovered for free.
        // With no usable AACS keys this degrades to a plain pass-through.
        // Symmetric with `Disc::sweep`: the patch COPIES ciphertext (multipass /
        // `--raw`) or decrypts IN PLACE (`opts.decrypt`). It does NOT decrypt-
        // VERIFY — the disc-absolute read can't anchor to a clip's file-relative
        // unit grid (see `Disc::sweep` + `Disc::verify_clips`). Re-reads recover
        // bad sectors; the clip-anchored verify pass re-checks them afterward.
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };
        let decrypt_is_aacs = matches!(keys, crate::decrypt::DecryptKeys::Aacs { .. });
        let content_ranges = self.encrypted_content_ranges();
        let can_gate = !content_ranges.is_empty();
        let mut reader = {
            let mut dec = DecryptingSectorSource::new(reader, keys);
            if opts.decrypt && can_gate {
                dec = dec.with_content_ranges(std::sync::Arc::from(content_ranges));
            }
            if decrypt_is_aacs && opts.decrypt {
                if let Some(cb) = &opts.key_fetch {
                    dec = dec.with_key_fetch(cb.clone());
                }
            }
            dec
        };
        let reader = &mut reader;

        // Spawn the consumer. The `WritebackFile` (same bounded-cache
        // wrapper sweep uses, so patch's recovery writes — sparse but
        // can be many across a damaged region — get the burst-flush
        // protection on slow / NFS-backed staging) and the `Mapfile`
        // both move into the sink. We hold an `Arc<Mutex<…>>` snapshot
        // the sink republishes after every record so producer-side
        // stall guards / progress callbacks can read consumer side-
        // effects.
        let (sink, shared) = PatchSink::new(path, map, is_regular)?;
        // Why: WRITE_THROUGH_DEPTH (=1) — patch reads ONE sector per
        // recovery decision and the producer's stall / damage-window
        // logic checks consumer-published stats inline. Sweep's
        // DEFAULT_PIPELINE_DEPTH (=4) would let several sectors of
        // recovered bytes queue up between producer decisions and
        // writes, which conflicts with the per-sector lockstep this
        // loop was written against.
        let pipe = Pipeline::<PatchItem, _>::spawn(WRITE_THROUGH_DEPTH, sink)?;

        // Log ISO file size at patch start for write monitoring
        if let Ok(metadata) = std::fs::metadata(path) {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch.iso_size.start",
                iso_bytes = metadata.len(),
                "ISO file size at patch start"
            );
        }

        // Adaptive batching: read at `state.current_batch`, HALVE on a
        // batch-read failure (bisect to isolate the bad sector), and
        // DOUBLE back toward `state.initial_batch` on each clean read.
        // Rationale: dense damage scattered through a
        // NonTrimmed range is rare — most "bad ranges" in pass N have
        // lots of good sectors that swept-by-default landed inside.
        // Batch reads walk those at ~32x the speed of singles,
        // dropping to 1 only when the drive actually returns an error.
        // Guarantees:
        //   - no good sector is ever marked NonTrimmed because it
        //     was bundled in a failed batch — failed batches are
        //     "split decisions", not recorded failures
        //   - drop-to-1 retries the SAME starting position, so every
        //     sector in the failed batch is individually probed
        // Clamp to at least 1 sector. block_sectors is public
        // (Option<u16>); Some(0) would compute a zero-length read per
        // iteration, never advance block_end, and busy-spin the range
        // until its watchdog fired.
        let initial_batch = opts.block_sectors.unwrap_or(1).max(1);
        let recovery = opts.full_recovery;
        log_patch_start_snapshot(&initial_entries, &initial_stats, bytes_good_before);

        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.ranges",
            num_ranges = bad_ranges.len(),
            work_total,
            reverse_mode = opts.reverse,
            "Bad ranges for patch"
        );
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.start",
            block_sectors = initial_batch,
            recovery,
            reverse = opts.reverse,
            wedged_threshold = opts.wedged_threshold,
            num_ranges = bad_ranges.len(),
            work_total,
            bytes_good_start,
            "Disc::patch entered"
        );

        // Drive the recovery: build the per-pass context, then walk the
        // ordered bad ranges. `run` owns inter-range cooldown + the
        // pass-ending conditions; `patch_region` owns one range's loop.
        let mut ctx = PatchCtx {
            disc: self,
            reader,
            pipe: &pipe,
            shared: &shared,
            opts,
            total_bytes,
            decrypt_is_aacs,
            state: PatchLoopState::new(bytes_good_before, total_bytes, initial_batch, work_total),
        };
        ctx.run(&bad_ranges)?;
        let PatchCtx { state, .. } = ctx;

        // Drain the consumer thread: drop tx, wait for `close` to run
        // sync_all + mapfile.flush, then take the final stats from the
        // sink's summary. `close` failing on a regular-file sync_all is
        // surfaced here as `Error::IoError`, matching pre-split
        // behaviour.
        let summary = pipe.finish()?;

        // Scoped post-read re-verify (decrypt-fail == bad read). The consumer
        // has flushed the ISO + mapfile; re-read each clip unit this pass touched
        // WHOLE from the patched ISO and downgrade any that still won't decrypt
        // to NonTrimmed, so the orchestrator's end-of-recovery promotion
        // terminalizes it. Reuses the same verifier as the sweep. Fail-safe:
        // disabled gate / unreadable ISO / load failure all leave the pass as-is.
        if let Some(mut v) = verifier.take() {
            if let Ok(mut m) = mapfile::Mapfile::load(&mapfile_path) {
                // Only units whose every backing sector was actually READ
                // (Finished) may be re-verified — we can't verify what wasn't read
                // (a non-Finished sector is zero-filled because the read failed),
                // and must not waste a key lookup on a known-bad block.
                let finished = m.ranges_with(&[mapfile::SectorStatus::Finished]);
                let is_finished = |lba: u32| -> bool {
                    let p = lba as u64 * 2048;
                    finished.iter().any(|&(s, sz)| p >= s && p < s + sz)
                };
                if let Ok(mut iso) = crate::io::file_sector_source::FileSectorSource::open(path) {
                    let bad = v.reverify_iso(&mut iso, &bad_ranges, &is_finished);
                    if !bad.is_empty() {
                        let n: usize = bad.len();
                        for (lba, cnt) in bad {
                            let _ = m.record(
                                lba as u64 * 2048,
                                cnt as u64 * 2048,
                                mapfile::SectorStatus::NonTrimmed,
                            );
                        }
                        let _ = m.flush();
                        tracing::info!(
                            target: "freemkv::verify",
                            phase = "patch.reverify",
                            downgraded_ranges = n,
                            "post-read re-verify downgraded undecryptable units to NonTrimmed"
                        );
                    }
                }
            }
        }

        let outcome = build_outcome(
            &state,
            &summary,
            path,
            total_bytes,
            bad_ranges.len(),
            opts.wedged_threshold,
        );
        tracing::info!(
            target: "freemkv::scan",
            phase = "patch",
            recovered = outcome.bytes_recovered_this_pass,
            halted = outcome.halted,
            wedged_exit = outcome.wedged_exit,
            elapsed_ms = patch_t0.elapsed().as_millis() as u64,
            "end"
        );
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Transport failure (status=0xFF, USB-bridge crash) must be recognised by
    /// the gate `handle_read_failure` now checks FIRST, so it aborts the pass
    /// (wedged_exit + BreakOuter) instead of treating the bridge crash as an
    /// ordinary bad sector and hammering the crashed device for up to the
    /// per-range watchdog budget. `handle_read_failure` is not unit-testable in
    /// isolation, so this guards the classification predicate the production
    /// early-return keys off, and the contrast that an ordinary read error is
    /// NOT misclassified as a transport failure.
    #[test]
    fn transport_failure_is_recognised_for_patch_abort() {
        use crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE;

        // The exact shape Drive::read surfaces on a bridge crash.
        let tf = Error::DiscRead {
            sector: 1_392_314,
            status: Some(SCSI_STATUS_TRANSPORT_FAILURE),
            sense: None,
        };
        assert!(
            tf.is_scsi_transport_failure(),
            "a DiscRead with status=0xFF must classify as a transport failure so \
             patch aborts the pass"
        );

        // The raw ScsiError form (e.g. straight from the transport) too.
        let tf_raw = Error::ScsiError {
            opcode: 0x28,
            status: SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        };
        assert!(tf_raw.is_scsi_transport_failure());

        // An ordinary recoverable bad sector (CHECK CONDITION with sense) must
        // NOT trip the transport-failure abort — it should still be retried /
        // marked NonTrimmed, not abort the whole pass.
        let bad_sector = Error::DiscRead {
            sector: 1_392_314,
            status: Some(crate::scsi::SCSI_STATUS_CHECK_CONDITION),
            sense: Some(crate::scsi::ScsiSense {
                sense_key: 0x03,
                asc: 0x11,
                ascq: 0x00,
            }),
        };
        assert!(
            !bad_sector.is_scsi_transport_failure(),
            "an ordinary bad-sector CHECK CONDITION must not be misclassified as \
             a transport failure"
        );
    }

    #[test]
    fn recovery_read_widens_unaligned_aacs_window() {
        // A mid-unit AACS read must widen to the enclosing 3-sector unit
        // (so the decrypting source accepts it) and copy back exactly the
        // requested sector. Each sector is filled with its own LBA's low
        // byte so we can prove which window came back.
        struct RecordReader {
            saw_lba: u32,
            saw_count: u16,
        }
        impl SectorSource for RecordReader {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                self.saw_lba = lba;
                self.saw_count = count;
                for s in 0..count as usize {
                    buf[s * 2048..(s + 1) * 2048].fill((lba as usize + s) as u8);
                }
                Ok(count as usize * 2048)
            }
        }
        let mut rr = RecordReader {
            saw_lba: 0,
            saw_count: 0,
        };
        let mut buf = vec![0u8; 2048];
        // Request lba=4 (4 % 3 == 1, mid-unit), count=1.
        let n = recovery_read(&mut rr, true, 4, 1, &mut buf, true).unwrap();
        assert_eq!(n, 2048);
        assert_eq!(rr.saw_lba, 3, "widened down to the unit-aligned start");
        assert_eq!(rr.saw_count, 3, "widened to a whole 3-sector unit");
        assert_eq!(
            buf[0], 4u8,
            "copied back the requested sector (lba 4), not the unit head (lba 3)"
        );
    }

    // ----------------------------------------------------------------
    // SubRanges — the still-bad work-list the per-section recovery
    // phases (#50) shrink. Pure data structure; exhaustively tested so
    // each future phase helper can assert on its residual ranges.
    // ----------------------------------------------------------------

    #[test]
    fn subranges_from_section_and_basics() {
        let s = SubRanges::from_section(2048, 10 * 2048);
        assert!(!s.is_empty());
        assert_eq!(s.total_len(), 10 * 2048);
        assert_eq!(s.ranges(), &[(2048, 10 * 2048)]);
        assert!(SubRanges::from_section(2048, 0).is_empty());
        assert!(SubRanges::default().is_empty());
    }

    #[test]
    fn subranges_remove_middle_splits() {
        // [0,20k) minus [8k,12k) -> [0,8k) + [12k,20k)
        let mut s = SubRanges::from_section(0, 20 * 1024);
        s.remove(8 * 1024, 4 * 1024);
        assert_eq!(s.ranges(), &[(0, 8 * 1024), (12 * 1024, 8 * 1024)]);
        assert_eq!(s.total_len(), 16 * 1024);
    }

    #[test]
    fn subranges_remove_prefix_suffix_and_whole() {
        // prefix
        let mut s = SubRanges::from_section(1000, 1000);
        s.remove(900, 200); // [1000,1100) trimmed off the front
        assert_eq!(s.ranges(), &[(1100, 900)]);
        // suffix
        let mut s = SubRanges::from_section(1000, 1000);
        s.remove(1800, 500); // [1800,2000) trimmed off the back
        assert_eq!(s.ranges(), &[(1000, 800)]);
        // whole (exact + over-cover both clear it)
        let mut s = SubRanges::from_section(1000, 1000);
        s.remove(1000, 1000);
        assert!(s.is_empty());
        let mut s = SubRanges::from_section(1000, 1000);
        s.remove(0, 100_000);
        assert!(s.is_empty());
    }

    #[test]
    fn subranges_remove_gap_and_zero_are_noops() {
        let mut s = SubRanges::from_section(1000, 1000);
        s.remove(5000, 1000); // disjoint, after
        s.remove(0, 500); // disjoint, before
        s.remove(1200, 0); // zero-len
        assert_eq!(s.ranges(), &[(1000, 1000)]);
    }

    #[test]
    fn subranges_remove_spanning_two_ranges() {
        // two sub-ranges, removal straddling the gap trims the inner edges
        let mut s = SubRanges::from_section(0, 4096);
        s.remove(1024, 1024); // -> [0,1024) + [2048,4096)
        assert_eq!(s.ranges(), &[(0, 1024), (2048, 2048)]);
        s.remove(512, 2048); // covers tail of first + head of second
        assert_eq!(s.ranges(), &[(0, 512), (2560, 1536)]);
    }
}
