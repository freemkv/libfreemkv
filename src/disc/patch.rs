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
//! Per-range watchdog (`MAX_RANGE_SECS` / `RANGE_BUDGET_CAP_SECS`)
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
    /// still-NonTrimmed bytes to Unreadable). When that ships, this
    /// becomes the variant the orchestrator emits to the same
    /// PatchSink.
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
    fn from_map(map: &Mapfile) -> Self {
        Self {
            stats: map.stats(),
            bad_ranges: map.ranges_with(&[
                SectorStatus::NonTrimmed,
                SectorStatus::Unreadable,
                SectorStatus::NonScraped,
                SectorStatus::NonTried,
            ]),
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
}

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
            },
            shared_clone,
        ))
    }

    fn republish(&self) {
        // Best-effort lock — only the producer reads, only the consumer
        // writes; contention is single-acquire so the lock is never
        // poisoned in practice. If it ever did get poisoned we'd want
        // the underlying error surfaced rather than silently swallowed,
        // so we propagate the poison panic. (Same posture as
        // `sweep_pipeline.rs` — it never recovers from a poisoned
        // mutex either.)
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
        self.republish();
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
                    phase = "patch_sync_failed",
                    error = %e,
                    os_error = e.raw_os_error(),
                    error_kind = ?e.kind(),
                    "patch: sync_all failed"
                );
                return Err(Error::IoError { source: e });
            }
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch_sync_skipped",
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
        self.republish();
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

// Pass-N tunables. Hoisted to module scope so helpers (extracted from
// the original `Disc::patch` body) can reference them without inheriting
// the function's local-const scope.
const BRIDGE_DEGRADATION_PAUSE_SECS: u64 = 10;
const POST_FAILURE_PAUSE_SECS: u64 = 1;
const CONSECUTIVE_FAIL_LONG_PAUSE: u64 = 5;
const CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD: u64 = 10;
// Adaptive batching: climb back to `initial_batch` after this many
// consecutive clean single-sector successes.
const ADAPTIVE_UPSCALE_THRESHOLD: u32 = 16;
// Wedge-family (HARDWARE_ERROR / ILLEGAL_REQUEST) cooldown and abort
// thresholds — see `handle_read_failure` below for context.
const WEDGE_FAMILY_COOLDOWN_SECS: u64 = 30;
const WEDGE_ABORT_THRESHOLD: u32 = 16;
// Whole-pass stall watchdog: bytes_good must increase within
// STALL_SECS or the pass bails out as wedged.
const STALL_SECS: u64 = 3600;
// Per-range budget = sectors_in_range × SECONDS_PER_SECTOR, capped at
// RANGE_BUDGET_CAP_SECS. See `Disc::patch` block-comment for details.
const SECONDS_PER_SECTOR: u64 = 25;
const RANGE_BUDGET_CAP_SECS: u64 = 1800;
const MAX_SKIPS_PER_RANGE: u32 = 10;
// Pass-N damage-window / skip tunables.
const PASSN_DAMAGE_WINDOW: usize = 16;
// Single source of truth lives in `disc::read_error` so the patch loop's
// `compute_damage_skip` cannot drift from `ReadCtx::for_patch()`'s
// `damage_threshold_pct`. See `PATCH_DAMAGE_THRESHOLD_PCT` for context.
// (v0.20.8 release: the larger "route Pass-N MEDIUM/NOT_READY through
// handle_read_error" unification was attempted and backed out — the
// patch loop's size-aware `range_remaining/4` skip cap has no
// equivalent in `handle_read_error::JumpAhead`, and routing through
// the unified handler would regress the size-aware-skip A/B fixture
// in `tests/passn_handler_ab.rs`. Pulling the threshold into the
// shared constant is the safe, behavior-preserving first step.)
const PASSN_DAMAGE_THRESHOLD_PCT: usize = crate::disc::read_error::PATCH_DAMAGE_THRESHOLD_PCT;
const PASSN_SKIP_SECTORS_BASE: u64 = 32;
const PASSN_SKIP_SECTORS_CAP: u64 = 4096;
const PASSN_ESCALATION_RESET_GOOD: u32 = 4;
// Cache-prime: number of single-sector throwaway reads issued at LBAs
// immediately preceding the target before a count==1 recovery read.
const CACHE_PRIME_SECTORS: u32 = 3;

/// Probe-offset escalation: returns a per-probe skip distance in
/// sectors that doubles every three indices, capped at
/// `PASSN_SKIP_SECTORS_CAP`. Used by the wedge-vs-bad-sector probe to
/// scatter its sample LBAs across the failing region rather than
/// hammering the same neighborhood.
pub(super) fn skip_sectors_for_probe(idx: usize) -> u64 {
    let base = PASSN_SKIP_SECTORS_BASE as i64;
    let escalation = (idx * 3) as i64;
    let shifted = if escalation < 64 {
        base << escalation
    } else {
        base
    };
    shifted.min(PASSN_SKIP_SECTORS_CAP as i64) as u64
}

/// Send a `PatchItem` and translate a `SendError` (consumer thread died
/// / panicked) into a library error so the caller propagates cleanly.
/// Mirrors `sweep_pipeline.rs`'s `send_or_abort`.
pub(super) fn send_or_abort(
    pipe: &Pipeline<PatchItem, PatchSummary>,
    item: PatchItem,
) -> Result<()> {
    pipe.send(item).map_err(|_| Error::IoError {
        source: std::io::Error::other("patch consumer terminated unexpectedly"),
    })
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
    // Every retry pass acts on every non-Finished range. Including
    // Unreadable means a sector that failed in pass N gets a fresh
    // shot in pass N+1 — drive state evolves, the same read can
    // succeed later. Each pass owns its own jumps/skips; if pass 5
    // jumps over the same zone as pass 2, fine.
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

/// Cache priming: before the recovery read, issue `CACHE_PRIME_SECTORS`
/// throwaway single-sector reads at the LBAs immediately preceding
/// `lba`. The drive's read-ahead cache prefetches forward on
/// sequential reads — by the time the caller asks for `lba` it may
/// already be cached, even if a cold read of the same LBA would fail.
/// Proven 2026-05-07 with dd-as-oracle: 8/8 sectors recoverable when
/// primed vs 6/8 cold. Failures are best-effort: we already have these
/// bytes Finished from a prior pass, so prime failures don't update
/// mapfile state. Only runs on count==1 reads (the genuine recovery
/// path) and when `lba >= CACHE_PRIME_SECTORS` so the subtraction
/// doesn't underflow.
pub(super) fn prime_cache<R: SectorSource + ?Sized>(reader: &mut R, lba: u32, count: u16) {
    if !(lba >= CACHE_PRIME_SECTORS && count == 1) {
        return;
    }
    let mut prime_buf = [0u8; 2048];
    for i in 0..CACHE_PRIME_SECTORS {
        let prime_lba = lba - CACHE_PRIME_SECTORS + i;
        // Best-effort; ignore errors. Recovery=false is intentional:
        // a fast 1.5s timeout is fine because we don't need the data.
        let _ = reader.read_sectors(prime_lba, 1, &mut prime_buf[..], false);
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
        phase = "patch_mapfile_snapshot",
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
            phase = "patch_mapfile_entries_start",
            num_to_log = (initial_entries.len().min(10)) as u32,
            "First 10 entries"
        );
        for entry in initial_entries.iter().take(10) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch_mapfile_entry_start",
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
            phase = "patch_mapfile_entries_end",
            num_to_log = (initial_entries.len().min(10)) as u32,
            "Last 10 entries"
        );
        for entry in initial_entries.iter().skip(initial_entries.len() - 10) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch_mapfile_entry_end",
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
            phase = "patch_iso_size_end",
            iso_bytes = metadata.len(),
            bytes_recovered = stats.bytes_good.saturating_sub(state.bytes_good_before),
            "ISO file size at patch end"
        );
    }

    tracing::info!(
        target: "freemkv::disc",
        phase = "patch_done",
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
    pub wedge_count: u32,
    pub work_done: u64,
    // Per-range scratch (reset at each range boundary)
    pub consecutive_failures: u64,
    pub consecutive_skips_without_recovery: u32,
    pub consecutive_good_since_skip: u32,
    pub last_skip_from: Option<u64>,
    pub skip_count: u32,
    pub damage_window: Vec<bool>,
    // Stall tracking
    pub bytes_good_last: u64,
    pub stall_start: std::time::Instant,
    pub range_start: std::time::Instant,
    pub range_bytes_good: u64,
    // Adaptive batch
    pub current_batch: u16,
    pub consecutive_singles_ok: u32,
    // Snapshot at construction — these stay constant for the whole pass
    pub bytes_good_before: u64,
    pub bytes_good_start: u64,
    #[allow(dead_code)]
    pub total_bytes: u64,
    pub initial_batch: u16,
    pub recovery: bool,
    pub work_total: u64,
}

impl PatchLoopState {
    pub(super) fn new(
        bytes_good_before: u64,
        total_bytes: u64,
        initial_batch: u16,
        recovery: bool,
        work_total: u64,
    ) -> Self {
        let now = std::time::Instant::now();
        Self {
            halted: false,
            wedged_exit: false,
            blocks_attempted: 0,
            blocks_read_ok: 0,
            blocks_read_failed: 0,
            unreadable_count: 0,
            wedge_count: 0,
            work_done: 0,
            consecutive_failures: 0,
            consecutive_skips_without_recovery: 0,
            consecutive_good_since_skip: 0,
            last_skip_from: None,
            skip_count: 0,
            damage_window: Vec::with_capacity(PASSN_DAMAGE_WINDOW),
            bytes_good_last: bytes_good_before,
            stall_start: now,
            range_start: now,
            range_bytes_good: bytes_good_before,
            current_batch: initial_batch,
            consecutive_singles_ok: 0,
            bytes_good_before,
            bytes_good_start: bytes_good_before,
            total_bytes,
            initial_batch,
            recovery,
            work_total,
        }
    }
}

/// Phase F: the Ok arm of the patch read result. Records the recovery,
/// dispatches the bytes to the consumer, runs the stall guard, and
/// (in reverse mode) runs the post-recovery backtrack that fills the
/// gap left by the most recent damage-skip. Returns `OuterAction` —
/// `Break` if the stall guard fired or backtrack hit a halt.
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_read_success<R: SectorSource + ?Sized>(
    state: &mut PatchLoopState,
    frame: &RangeFrame,
    opts: &PatchOptions,
    lba: u32,
    count: u16,
    pos: u64,
    block_bytes: u64,
    bytes: usize,
    buf: &mut [u8],
    read_duration_ms: u128,
    pipe: &Pipeline<PatchItem, PatchSummary>,
    shared: &Mutex<SharedPatchState>,
    reader: &mut R,
) -> Result<OuterAction> {
    state.blocks_read_ok += 1;
    state.consecutive_failures = 0;
    state.consecutive_good_since_skip += 1;
    if state.consecutive_good_since_skip >= PASSN_ESCALATION_RESET_GOOD {
        state.consecutive_skips_without_recovery = 0;
    }
    // Adaptive batching: track clean single-sector reads to decide
    // when to climb back to `state.initial_batch`. A batch read
    // succeeding (count > 1) tells us the drive is healthy but doesn't
    // accumulate toward upscale — we got back to batch=1 because of a
    // failure here, we need consistent health at the slow tempo
    // before scaling up again.
    if count == 1 && state.current_batch < state.initial_batch {
        state.consecutive_singles_ok += 1;
        if state.consecutive_singles_ok >= ADAPTIVE_UPSCALE_THRESHOLD {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_adaptive_upscale",
                from = state.current_batch,
                to = state.initial_batch,
                consecutive_singles_ok = state.consecutive_singles_ok,
                lba,
                "adaptive batching: drive stable, climbing back to initial_batch"
            );
            state.current_batch = state.initial_batch;
            state.consecutive_singles_ok = 0;
        }
    }
    state.damage_window.push(true);
    if state.damage_window.len() > PASSN_DAMAGE_WINDOW {
        state.damage_window.remove(0);

        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_read_ok",
            lba,
            count,
            bytes,
            blocks_read_ok = state.blocks_read_ok,
            consecutive_failures = state.consecutive_failures,
            read_duration_ms,
            range_idx = frame.range_idx,
            pos,
            "Read succeeded"
        );
    }
    // Plaintext: DecryptingSectorSource applied AACS / CSS in-place
    // during the read_sectors call above. The pre-0.18 inline
    // decrypt_sectors call lived here.
    let write_start = std::time::Instant::now();
    tracing::debug!(
        target: "freemkv::disc",
        phase = "patch_write_start",
        pos,
        bytes,
        "Starting ISO write"
    );
    // Hand the recovered bytes off to the consumer: seek + write +
    // mapfile.record(Finished) all happen on the consumer thread,
    // so the producer can immediately move on to the next read while
    // these bytes are being committed.
    send_or_abort(
        pipe,
        PatchItem::Recovered {
            pos,
            buf: buf[..bytes].to_vec(),
        },
    )?;
    let write_duration_ms = write_start.elapsed().as_millis();
    tracing::info!(
        target: "freemkv::disc",
        phase = "patch_write_ok",
        pos,
        bytes,
        write_duration_ms,
        "ISO write succeeded"
    );
    tracing::info!(
        target: "freemkv::disc",
        phase = "patch_mapfile_record_ok",
        pos,
        block_bytes,
        "Mapfile record dispatched"
    );

    // Stall guard: watch bytes_good (real progress), not pos
    // (advances on skips). With the consumer running in its own
    // thread, this read can lag by up to one item; the watchdog
    // operates at STALL_SECS=3600 granularity so single-item lag is
    // irrelevant.
    let bytes_good_now = {
        let g = shared
            .lock()
            .expect("PatchSink shared state mutex poisoned");
        g.stats.bytes_good
    };
    if bytes_good_now > state.bytes_good_last {
        state.stall_start = std::time::Instant::now();
        state.bytes_good_last = bytes_good_now;
    }
    if state.stall_start.elapsed() > std::time::Duration::from_secs(STALL_SECS) {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch_stall",
            elapsed_secs = state.stall_start.elapsed().as_secs(),
            bytes_good = bytes_good_now,
            bytes_good_start = state.bytes_good_start,
            "Patch stalled - no recovery for {}s, exiting pass",
            STALL_SECS
        );
        state.wedged_exit = true;
        return Ok(OuterAction::Break);
    }

    if let Some(skip_from) = state.last_skip_from.take() {
        let backtrack_start = frame.block_end;
        let backtrack_end = skip_from;
        if opts.reverse && backtrack_start < backtrack_end {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_backtrack_start",
                from_lba = pos,
                to_lba = backtrack_end / 2048,
                "recovered after skip; backtracking into gap"
            );
            let mut bt_pos = backtrack_start;
            while bt_pos < backtrack_end {
                // Honor cancellation inside the backtrack inner loop.
                // A long backtrack span can run minutes of single-
                // sector reads; without this check the outer halt
                // only takes effect when control returns to the
                // per-range loop.
                if let Some(h) = &opts.halt {
                    if h.load(std::sync::atomic::Ordering::Relaxed) {
                        return Err(crate::error::Error::Halted);
                    }
                }
                let span =
                    // Backtrack always at count=1: this path fills a
                    // gap that the main loop's damage-window skip
                    // jumped over. Using batched reads here would
                    // lump good sectors into NonTrimmed marks when
                    // the gap contains even one bad sector. Backtrack
                    // is rare enough that the per-sector cost is fine.
                    (backtrack_end - bt_pos).min(2048);
                let bt_lba = (bt_pos / 2048) as u32;
                let bt_count = (span / 2048) as u16;
                let bt_bytes = bt_count as usize * 2048;
                match reader.read_sectors(bt_lba, bt_count, &mut buf[..bt_bytes], state.recovery) {
                    Ok(_) => {
                        state.blocks_read_ok += 1;
                        // Plaintext via DecryptingSectorSource
                        // wrapping; same path the main read takes
                        // above.
                        send_or_abort(
                            pipe,
                            PatchItem::Recovered {
                                pos: bt_pos,
                                buf: buf[..bt_bytes].to_vec(),
                            },
                        )?;
                    }
                    Err(_err) => {
                        state.blocks_read_failed += 1;
                        // Leave NonTrimmed (not Unreadable) so a
                        // later pass gets another shot. Per the
                        // project goal — "recover 100% of readable
                        // data" — and the multi-pass design's
                        // promise: bytes stay Good-or-Maybe across
                        // passes; promotion to Unreadable is the
                        // orchestrator's job at end-of-recovery
                        // (final retry pass complete). Reference:
                        // 2026-05-11 design call.
                        send_or_abort(
                            pipe,
                            PatchItem::NonTrimmed {
                                pos: bt_pos,
                                len: span,
                            },
                        )?;
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_backtrack_stop",
                            lba = bt_lba,
                            "backtrack hit damage; stopping"
                        );
                        break;
                    }
                }
                state.work_done = state.work_done.saturating_add(span);
                bt_pos += span;
            }
        }
    }
    Ok(OuterAction::Continue)
}

/// Phase G: the Err arm of the patch read result. Handles the
/// adaptive-batch split decision (count > 1 failures don't count),
/// records the failure, applies the NOT_READY retry pause, dispatches
/// the NonTrimmed PatchItem, runs the stall guard, runs the probe-
/// wedge-vs-bad-sector diagnostic, classifies the wedge family and
/// picks the right cooldown pause, then sleeps. Returns the verdict
/// for the outer loop.
///
/// Left as one large function (not sub-split into
/// `handle_not_ready_retry` / `probe_drive_responsive` /
/// `classify_wedge_family`) — that's the gold-plating tier deferred
/// to a future PR.
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_read_failure<R: SectorSource + ?Sized>(
    state: &mut PatchLoopState,
    frame: &RangeFrame,
    opts: &PatchOptions,
    err: &Error,
    lba: u32,
    count: u16,
    pos: u64,
    block_bytes: u64,
    bytes: usize,
    read_duration_ms: u128,
    pipe: &Pipeline<PatchItem, PatchSummary>,
    shared: &Mutex<SharedPatchState>,
    reader: &mut R,
) -> Result<FailureAction> {
    // Adaptive batching split decision: a batch-read failure
    // (count > 1) is NOT a recorded failure. We don't yet know which
    // sector in the batch was actually bad — could be one, could be
    // many. Drop to count=1 and retry the SAME starting position so
    // every sector gets individually probed. Cursor stays put; loop
    // continues. Invariants: no good sector ever gets lumped into a
    // NonTrimmed mark, no spurious consecutive_failures (which drives
    // wedge detection), no damage_window pollution from batch-level
    // signals.
    if count > 1 {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_adaptive_split",
            lba,
            count,
            from_batch = state.current_batch,
            err_code = err.code(),
            "adaptive batching: batch read failed, dropping to count=1 to probe individually"
        );
        state.current_batch = 1;
        state.consecutive_singles_ok = 0;
        return Ok(FailureAction::ContinueInner);
    }

    state.blocks_read_failed += 1;
    state.consecutive_failures += 1;
    state.consecutive_good_since_skip = 0;
    state.consecutive_singles_ok = 0;
    state.unreadable_count += 1;

    tracing::warn!(
        target: "freemkv::disc",
        phase = "patch_read_err",
        lba,
        count,
        bytes,
        blocks_read_failed = state.blocks_read_failed,
        consecutive_failures = state.consecutive_failures,
        read_duration_ms,
        error_code = err.code(),
        range_idx = frame.range_idx,
        pos,
        "Read failed"
    );

    // Check if this is a NOT_READY error that should be retried
    let sense = err.scsi_sense();

    // ASC values indicating temporary drive unresponsiveness:
    // 0x02 = medium not present, 0x03 = becoming ready, 0x04 = initialization required
    let is_not_ready_retryable = sense
        .map(|s| s.sense_key == 0x02 && (s.asc == 0x02 || s.asc == 0x03 || s.asc == 0x04))
        .unwrap_or(false);

    // For retryable NOT_READY errors, pause longer and don't mark as Unreadable yet
    if is_not_ready_retryable {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_not_ready_retry",
            lba,
            consecutive_failures = state.consecutive_failures,
            err_asc = sense.map(|s| s.asc as u32).unwrap_or(0),
            "NOT_READY with ASC=0x03/0x04; pausing for drive recovery before retry"
        );

        // Extended pause for NOT_READY - let drive complete internal mechanical recovery
        let pause_secs = 15u64;
        tracing::debug!(
            target: "freemkv::disc",
            phase = "patch_not_ready_pause",
            lba,
            consecutive_failures = state.consecutive_failures,
            pause_secs,
            "Waiting for drive to become ready"
        );
        std::thread::sleep(std::time::Duration::from_secs(pause_secs));

        // Don't mark as Unreadable yet - will retry on next iteration
        state.damage_window.push(false);
        if state.damage_window.len() > PASSN_DAMAGE_WINDOW {
            state.damage_window.remove(0);
        }
        return Ok(FailureAction::ContinueInner);
    }

    // (Removed in 0.20.2) The previous code retried non-NOT_READY
    // errors on encrypted discs with an "exponential backoff: 2s, 4s,
    // 8s" comment — but `retry_count` was declared inside the per-
    // iteration `Err` arm so it reset to 0 every iteration. The
    // "MAX_NON_NOT_READY_RETRIES=3" budget actually fired exactly
    // once (1s pause + 1 retry), then fell through to the NonTrimmed
    // dispatch below. The block was a 100-line illusion. Cross-pass
    // NonTrimmed retry (next pass gives the same sectors another
    // shot) already covers the recovery case it was supposed to
    // handle — and it gives the drive minutes between attempts
    // instead of 1-8 seconds, which empirically matters for stochastic
    // recovery on the BU40N.

    // All retries exhausted IN THIS PASS — leave NonTrimmed so a
    // subsequent pass gets another shot. Bytes stay Good-or-Maybe
    // across passes; only the orchestrator (autorip) promotes still-
    // NonTrimmed → Unreadable after the FINAL retry pass completes.
    // Reference: 2026-05-11 design call ("good or maybe until all
    // passes are done, then it's gone"). Pre-fix the patch loop
    // marked Unreadable here, which gave up on sectors that a later
    // pass might have recovered (drive reads are stochastic — same
    // sector that fails 10x in Pass 2 might succeed on attempt 1 in
    // Pass 3 after the drive state has shifted).
    send_or_abort(
        pipe,
        PatchItem::NonTrimmed {
            pos,
            len: block_bytes,
        },
    )?;

    state.damage_window.push(false);
    if state.damage_window.len() > PASSN_DAMAGE_WINDOW {
        state.damage_window.remove(0);
    }

    // Stall guard: check on failures too, not just successes
    let bytes_good_now = {
        let g = shared
            .lock()
            .expect("PatchSink shared state mutex poisoned");
        g.stats.bytes_good
    };
    if bytes_good_now > state.bytes_good_last {
        state.stall_start = std::time::Instant::now();
        state.bytes_good_last = bytes_good_now;
    }
    if state.stall_start.elapsed() > std::time::Duration::from_secs(STALL_SECS) {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch_stall",
            elapsed_secs = state.stall_start.elapsed().as_secs(),
            consecutive_failures = state.consecutive_failures,
            bytes_good = bytes_good_now,
            bytes_good_start = state.bytes_good_start,
            "Patch stalled - no recovery for {}s, exiting pass",
            STALL_SECS
        );
        state.wedged_exit = true;
        return Ok(FailureAction::BreakOuter);
    }

    // Log every 10 failures or when approaching wedged threshold
    if state.consecutive_failures % 10 == 0 || state.consecutive_failures >= opts.wedged_threshold {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch_failure_count",
            lba,
            consecutive_failures = state.consecutive_failures,
            wedged_threshold = opts.wedged_threshold,
            "Failure count"
        );
    }

    // Probe good sectors to differentiate wedge vs bad sector
    if state.consecutive_failures >= 3 && state.consecutive_failures % 5 == 0 {
        let probe_offsets: [u64; 3] = [0, skip_sectors_for_probe(1), skip_sectors_for_probe(2)];
        let mut probes_ok = 0;

        for (probe_idx, &offset) in probe_offsets.iter().enumerate() {
            if offset >= block_bytes || (offset == 0 && state.consecutive_failures < 5) {
                continue;
            }

            let probe_pos = pos + offset;
            let probe_lba = (probe_pos / 2048) as u32;
            let probe_count = 1u16;
            let mut probe_buf = [0u8; 2048];

            match reader.read_sectors(probe_lba, probe_count, &mut probe_buf[..], state.recovery) {
                Ok(_) => {
                    probes_ok += 1;
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "patch_probe_ok",
                        lba = probe_lba,
                        offset_from_current = offset,
                        probe_idx,
                        "Probe read succeeded — drive responsive"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "patch_probe_err",
                        lba = probe_lba,
                        offset_from_current = offset,
                        probe_idx,
                        "Probe read failed"
                    );
                }
            }
        }

        if probes_ok > 0 {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_drive_responsive",
                consecutive_failures = state.consecutive_failures,
                probes_ok,
                total_probes = 3,
                lba,
                range_idx = frame.range_idx,
                "Drive responsive — bad sector cluster, not wedged"
            );
        } else if probes_ok == 0 && state.consecutive_failures >= 10 {
            // Heuristic suspicion of wedge — NOT the confirmed
            // wedge_transition log that fires when the SCSI sense
            // family flips into Hardware/IllegalRequest. This log
            // just says "the local zone is fully bad" which could
            // mean a real wedge OR a fully-bad cluster on a non-
            // wedged drive. The wedge_skip handler in read_error.rs
            // is what actually decides + acts.
            tracing::warn!(
                target: "freemkv::disc",
                phase = "patch_zone_fully_bad",
                consecutive_failures = state.consecutive_failures,
                lba,
                range_idx = frame.range_idx,
                "patch zone fully bad (10+ failures, all probes failed); \
                 not a wedge unless read_error.rs's wedge_transition also fires"
            );
        }
    }

    // (Removed in 0.20.2) Duplicate NonTrimmed dispatch. The earlier
    // `send_or_abort(PatchItem::NonTrimmed)` already recorded the
    // range. `Mapfile::record` is idempotent so it wasn't a
    // correctness bug, but it doubled the consumer's per-failure work.

    // Wedge-family detection: HARDWARE_ERROR / ILLEGAL_REQUEST are
    // the senses the BU40N's firmware fast-fail mode returns. When
    // the drive is wedged, every subsequent read returns these in
    // <100ms — exactly the rapid-retry cadence that bricks the drive
    // further. Long cooldown (30s, matching
    // read_error::ZONE_ENTRY_COOLDOWN_SECS) gives the firmware
    // breathing room to clear the fast-fail state. After
    // WEDGE_ABORT_THRESHOLD consecutive wedge senses with no recovery,
    // bail to autorip so it can eject + reload (the only thing that
    // reliably clears a real wedge).
    let is_wedge_family = err
        .scsi_sense()
        .map(|s| {
            s.sense_key == crate::scsi::SENSE_KEY_HARDWARE_ERROR
                || s.sense_key == crate::scsi::SENSE_KEY_ILLEGAL_REQUEST
        })
        .unwrap_or(false);

    let pause_secs = if is_wedge_family {
        state.wedge_count += 1;
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch_wedge_family",
            lba,
            wedge_count = state.wedge_count,
            wedge_abort_threshold = WEDGE_ABORT_THRESHOLD,
            sense_key = err.scsi_sense().map(|s| s.sense_key as u32).unwrap_or(0),
            "HARDWARE_ERROR / ILLEGAL_REQUEST sense — wedge family, applying long cooldown"
        );
        if state.wedge_count >= WEDGE_ABORT_THRESHOLD {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "patch_wedge_abort",
                wedge_count = state.wedge_count,
                WEDGE_ABORT_THRESHOLD,
                "Drive appears wedged ({} consecutive wedge-family senses); aborting pass for autorip eject+reload",
                state.wedge_count
            );
            state.wedged_exit = true;
            return Ok(FailureAction::BreakOuter);
        }
        WEDGE_FAMILY_COOLDOWN_SECS
    } else if err.is_bridge_degradation() {
        tracing::debug!(
            target: "freemkv::disc",
            phase = "patch_bridge_degradation",
            lba,
            consecutive_failures = state.consecutive_failures,
            error = %err,
            "bridge degradation; cooling down"
        );
        BRIDGE_DEGRADATION_PAUSE_SECS
    } else if state.consecutive_failures >= CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD {
        CONSECUTIVE_FAIL_LONG_PAUSE
    } else {
        POST_FAILURE_PAUSE_SECS
    };

    // Any non-wedge-family read clears the wedge counter.
    if !is_wedge_family {
        state.wedge_count = 0;
    }

    tracing::debug!(
        target: "freemkv::disc",
        phase = "patch_post_failure_pause",
        lba,
        consecutive_failures = state.consecutive_failures,
        pause_secs,
        "breathing room after failure"
    );
    std::thread::sleep(std::time::Duration::from_secs(pause_secs));
    Ok(FailureAction::Continue)
}

/// Return value of [`handle_read_success`] and [`handle_read_failure`]:
/// tells the outer coordination loop whether to break out of the
/// `'outer` for-loop entirely (`Break`) or fall through to the
/// per-iteration damage-skip / progress logic (`Continue`).
pub(super) enum OuterAction {
    /// Continue with the remaining iteration body (damage-skip,
    /// progress dispatch, wedged-threshold check).
    Continue,
    /// Break out of the outer `'outer` loop. `state.halted` /
    /// `state.wedged_exit` has already been set by the helper.
    Break,
}

/// Failure helper's outcome — distinguished from `OuterAction` because
/// the failure path has its own special "continue inner loop without
/// running per-iteration damage-skip / progress dispatch" verdict for
/// NOT_READY retries and adaptive-split decisions.
pub(super) enum FailureAction {
    /// Run the per-iteration damage-skip / wedge-threshold / progress
    /// logic, then loop. Same as `OuterAction::Continue`.
    Continue,
    /// Skip the per-iteration damage-skip / progress logic and `continue`
    /// the inner loop directly. Used for the NOT_READY retry path
    /// (don't advance cursor, retry same LBA next iteration) and the
    /// adaptive batch-split decision (drop to count=1, retry).
    ContinueInner,
    /// Break out of the outer `'outer` loop. `state.wedged_exit` has
    /// already been set.
    BreakOuter,
}

/// Per-range constants captured once when the outer loop enters a
/// range. Avoids threading `range_pos`, `range_size`, derived `end` /
/// `range_sectors` / `range_budget_secs` through every helper. The
/// only field that changes between iterations is `block_end` — the
/// per-iteration cursor; helpers that move it (damage-skip, advance)
/// take `&mut RangeFrame`.
pub(super) struct RangeFrame {
    pub range_idx: usize,
    pub range_pos: u64,
    #[allow(dead_code)]
    pub range_size: u64,
    pub end: u64,
    pub block_end: u64,
    pub range_budget_secs: u64,
    pub range_sectors: u64,
}

/// Per-range watchdog: combines the elapsed-budget check with a
/// no-forward-progress check (reset `state.range_start` whenever
/// `bytes_good` advances). Returns `true` when the inner loop should
/// `break` out to the next range. Emits the same `patch_range_timeout`
/// / `patch_range_stall` warnings as the inline original.
pub(super) fn check_range_watchdog(
    state: &mut PatchLoopState,
    frame: &RangeFrame,
    shared: &Mutex<SharedPatchState>,
) -> bool {
    if state.range_start.elapsed().as_secs() > frame.range_budget_secs {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch_range_timeout",
            range_lba = frame.range_pos / 2048,
            range_sectors = frame.range_sectors,
            elapsed_secs = state.range_start.elapsed().as_secs(),
            budget_secs = frame.range_budget_secs,
            bytes_recovered = state.range_bytes_good.saturating_sub(state.bytes_good_before),
            "Range timeout - moving to next range"
        );
        return true;
    }

    let bytes_good_now = {
        let g = shared
            .lock()
            .expect("PatchSink shared state mutex poisoned");
        g.stats.bytes_good
    };
    if bytes_good_now > state.range_bytes_good {
        state.range_bytes_good = bytes_good_now;
        state.range_start = std::time::Instant::now();
    }
    if state.range_start.elapsed().as_secs() > frame.range_budget_secs {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch_range_stall",
            range_lba = frame.range_pos / 2048,
            range_sectors = frame.range_sectors,
            elapsed_secs = state.range_start.elapsed().as_secs(),
            budget_secs = frame.range_budget_secs,
            bytes_recovered = state.range_bytes_good.saturating_sub(state.bytes_good_before),
            "Range stalled - moving to next range"
        );
        return true;
    }
    false
}

/// Phase D3: skip-limit reached for the current range. Emit the
/// `patch_skip_limit` warn and dispatch the appropriate NonTrimmed
/// PatchItem for the remaining (never-attempted) bytes. Caller breaks
/// the inner loop after this returns.
pub(super) fn handle_skip_limit(
    state: &PatchLoopState,
    frame: &RangeFrame,
    opts: &PatchOptions,
    pipe: &Pipeline<PatchItem, PatchSummary>,
) -> Result<()> {
    tracing::warn!(
        target: "freemkv::disc",
        phase = "patch_skip_limit",
        range_lba = frame.range_pos / 2048,
        skip_count = state.skip_count,
        "Skip limit reached - leaving remaining bytes NonTrimmed for next pass",
    );
    // CRITICAL: don't mark sectors we NEVER ATTEMPTED as Unreadable.
    // Only sectors we actually read+failed get the terminal `-`
    // status. Sectors we jumped over are hopeful — the drive may read
    // them on a later pass when state has evolved (cache, mechanical
    // settle). 2026-05-07 dd-as-oracle test confirmed ~36% of patch-
    // marked Unreadable sectors are actually readable.
    let unmarked_bytes = frame.block_end.saturating_sub(frame.range_pos);
    if opts.reverse {
        send_or_abort(
            pipe,
            PatchItem::NonTrimmed {
                pos: frame.range_pos,
                len: unmarked_bytes,
            },
        )?;
    } else {
        let remaining_start = frame.range_pos + (frame.end - frame.block_end);
        if remaining_start < frame.end {
            send_or_abort(
                pipe,
                PatchItem::NonTrimmed {
                    pos: remaining_start,
                    len: frame.end - remaining_start,
                },
            )?;
        }
    }
    Ok(())
}

/// Damage-cluster size-aware skip decision. Inspects `state.damage_window`
/// against the `PASSN_DAMAGE_THRESHOLD_PCT` threshold; if crossed,
/// advances `frame.block_end` by an escalating skip (capped at 1/4 of
/// the remaining bad range so a single jump can't blow past a good
/// middle). Returns `true` iff a skip was applied — caller then
/// suppresses the normal block-cursor advance.
pub(super) fn compute_damage_skip(
    state: &mut PatchLoopState,
    frame: &mut RangeFrame,
    opts: &PatchOptions,
    lba: u32,
    _block_bytes: u64,
) -> bool {
    let bad_count = state.damage_window.iter().filter(|&&b| !b).count();
    if !(state.damage_window.len() >= PASSN_DAMAGE_WINDOW
        && bad_count * 100 / state.damage_window.len() >= PASSN_DAMAGE_THRESHOLD_PCT)
    {
        return false;
    }

    // Size-aware cap: never skip more than 1/4 of the remaining bad
    // range. A 100-sector bad range is really 25-bad + 50-good + 25-
    // bad in disguise; a hardcoded MB-scale skip would leap over the
    // entire thing and miss the good middle. Capping at
    // range_remaining/4 forces convergence on the actual bad sub-zones.
    let range_remaining_bytes = if opts.reverse {
        frame.block_end.saturating_sub(frame.range_pos)
    } else {
        frame.end.saturating_sub(frame.block_end)
    };
    let range_remaining_sectors = range_remaining_bytes / 2048;
    let range_quarter = (range_remaining_sectors / 4).max(1);
    let escalated = (PASSN_SKIP_SECTORS_BASE << state.consecutive_skips_without_recovery)
        .min(PASSN_SKIP_SECTORS_CAP);
    let skip_sectors = escalated.min(range_quarter);
    let skip_bytes = skip_sectors * 2048;
    let new_block_end = if opts.reverse {
        frame
            .block_end
            .saturating_sub(skip_bytes)
            .max(frame.range_pos)
    } else {
        (frame.block_end + skip_bytes).min(frame.end)
    };
    if new_block_end == frame.block_end {
        return false;
    }
    tracing::info!(
        target: "freemkv::disc",
        phase = "patch_damage_skip",
        from_lba = lba,
        skip_sectors,
        escalation = state.consecutive_skips_without_recovery,
        bad_pct = bad_count * 100 / state.damage_window.len(),
        "damage cluster detected; skipping within range"
    );
    let gap_bytes = if opts.reverse {
        frame.block_end.saturating_sub(new_block_end)
    } else {
        new_block_end.saturating_sub(frame.block_end)
    };
    state.work_done = state.work_done.saturating_add(gap_bytes);
    state.last_skip_from = Some(frame.block_end);
    frame.block_end = new_block_end;
    state.consecutive_skips_without_recovery += 1;
    state.skip_count += 1;
    true
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
        let pp = crate::progress::PassProgress {
            kind,
            work_done: state.work_done,
            work_total: state.work_total,
            bytes_good_total: s.bytes_good,
            bytes_unreadable_total: s.bytes_unreadable,
            bytes_pending_total: s.bytes_pending,
            bytes_total_disc: total_bytes,
            disc_duration_secs: main_title.map(|t| t.duration_secs),
            bytes_bad_in_main_title: main_title_bad,
            main_title_duration_secs: main_title.map(|t| t.duration_secs),
            main_title_size_bytes: main_title.map(|t| t.size_bytes),
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
    /// 0.18: paired with [`Disc::sweep`] as the library's other flat
    /// rip-phase verb. Caller drives the retry loop and the
    /// sweep-vs-patch dispatch.
    pub fn patch(
        &self,
        reader: &mut dyn SectorSource,
        path: &std::path::Path,
        opts: &PatchOptions,
    ) -> Result<PatchOutcome> {
        use crate::io::pipeline::{Pipeline, WRITE_THROUGH_DEPTH};
        use crate::sector::{DecryptingSectorSource, SectorSource};

        let mapfile_path = self.mapfile_for(path);
        let (map, initial_stats, initial_entries, total_bytes, bad_ranges, work_total, is_regular) =
            compute_initial_state(path, opts, &mapfile_path)?;
        let bytes_good_before = initial_stats.bytes_good;
        let bytes_good_start = bytes_good_before;
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        // Wrap the producer-side reader once so every read_sectors
        // call (the main recovery read, the backtrack read, and the
        // non-NOT_READY retry read) yields plaintext. Replaces three
        // inline decrypt_sectors call sites that all keyed off the
        // same `keys`. `DecryptKeys::None` keeps the unencrypted /
        // --raw path a pass-through.
        let mut reader = DecryptingSectorSource::new(reader, keys);
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
                phase = "patch_iso_size_start",
                iso_bytes = metadata.len(),
                "ISO file size at patch start"
            );
        }

        // Adaptive batching: read at `state.current_batch`, drop to 1
        // on batch-read failure, climb back to `state.initial_batch`
        // after ADAPTIVE_UPSCALE_THRESHOLD consecutive single-sector
        // successes. Rationale: dense damage scattered through a
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
        let initial_batch = opts.block_sectors.unwrap_or(1);
        let recovery = opts.full_recovery;
        let mut state = PatchLoopState::new(
            bytes_good_before,
            total_bytes,
            initial_batch,
            recovery,
            work_total,
        );
        let mut buf = vec![0u8; initial_batch as usize * 2048];

        reader.set_speed(0x0000);

        log_patch_start_snapshot(&initial_entries, &initial_stats, bytes_good_before);

        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_bad_ranges",
            num_ranges = bad_ranges.len(),
            work_total,
            reverse_mode = opts.reverse,
            "Bad ranges for patch"
        );
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_start",
            block_sectors = initial_batch,
            recovery,
            reverse = opts.reverse,
            wedged_threshold = opts.wedged_threshold,
            num_ranges = bad_ranges.len(),
            work_total,
            bytes_good_start,
            "Disc::patch entered"
        );

        'outer: for (range_idx, (range_pos, range_size)) in bad_ranges.iter().enumerate() {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_range_start",
                range_index = range_idx,
                num_total_ranges = bad_ranges.len(),
                range_lba = *range_pos / 2048,
                range_size_mb = *range_size as f64 / 1_048_576.0,
                "Starting patch range"
            );
            let end = *range_pos + *range_size;
            let range_sectors = *range_size / 2048;
            let range_budget_secs = (range_sectors * SECONDS_PER_SECTOR).min(RANGE_BUDGET_CAP_SECS);
            let mut frame = RangeFrame {
                range_idx,
                range_pos: *range_pos,
                range_size: *range_size,
                end,
                block_end: if opts.reverse { end } else { *range_pos },
                range_budget_secs,
                range_sectors,
            };
            state.damage_window.clear();
            state.consecutive_skips_without_recovery = 0;
            state.consecutive_good_since_skip = 0;
            state.range_start = std::time::Instant::now();
            state.range_bytes_good = state.bytes_good_before;
            state.skip_count = 0;
            // Reset consecutive_failures at each range boundary. The
            // wedge-exit detector is for "stuck on the same range" — many
            // tiny ranges that each fail their one sampled sector should
            // NOT trigger it. Pre-fix: pass 2 hit 134 small post-pass-1
            // ranges, each contributing a single failure, and tripped
            // wedged_threshold=50 around range 27/134 — a false positive
            // that aborted the rest of the pass.
            state.consecutive_failures = 0;
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch_range_budget",
                range_lba = *range_pos / 2048,
                range_sectors,
                range_budget_secs,
                "Per-range time budget computed"
            );
            loop {
                if let Some(ref h) = opts.halt {
                    if h.load(std::sync::atomic::Ordering::Relaxed) {
                        state.halted = true;
                        break 'outer;
                    }
                }

                if check_range_watchdog(&mut state, &frame, &shared) {
                    break;
                }

                // Test 3: Skip count - max 10 skips per range
                if state.skip_count >= MAX_SKIPS_PER_RANGE {
                    handle_skip_limit(&state, &frame, opts, &pipe)?;
                    break;
                }
                let (pos, block_bytes) = if opts.reverse {
                    if frame.block_end <= frame.range_pos {
                        break;
                    }
                    let span =
                        (frame.block_end - frame.range_pos).min(state.current_batch as u64 * 2048);
                    (frame.block_end - span, span)
                } else {
                    if frame.block_end >= frame.end {
                        break;
                    }
                    let span = (frame.end - frame.block_end).min(state.current_batch as u64 * 2048);
                    (frame.block_end, span)
                };
                let lba = (pos / 2048) as u32;
                let count = (block_bytes / 2048) as u16;
                let bytes = count as usize * 2048;
                state.blocks_attempted += 1;

                tracing::debug!(
                    target: "freemkv::disc",
                    phase = "patch_read_start",
                    lba,
                    count,
                    bytes,
                    attempt_num = state.blocks_attempted,
                    range_index = range_idx,
                    pos_byte = pos,
                    "Starting sector read"
                );

                prime_cache(reader, lba, count);

                // Single-shot read. Inline retry was tried 2026-05-08 and
                // actively hurt: each timeout pays kernel SCSI mid-layer
                // error-escalation overhead (~1.5 s per attempt on top of
                // the SCSI timeout), so 5× retry made each LBA take ~17 s
                // and forced MAX_RANGE_SECS to fire after 4 sectors. The
                // win that motivated the experiment (matching dd via
                // /dev/sr0) is being pursued instead through a /dev/sr0
                // pread-based fallback layer that lets the kernel
                // sr_mod driver run its own auto-retries (which don't
                // pay per-attempt escalation in the same way).
                let read_start = std::time::Instant::now();
                let read_result =
                    reader.read_sectors(lba, count, &mut buf[..bytes], state.recovery);
                let read_duration_ms = read_start.elapsed().as_millis();

                match read_result {
                    Ok(_) => {
                        match handle_read_success(
                            &mut state,
                            &frame,
                            opts,
                            lba,
                            count,
                            pos,
                            block_bytes,
                            bytes,
                            &mut buf,
                            read_duration_ms,
                            &pipe,
                            &shared,
                            reader,
                        )? {
                            OuterAction::Break => break 'outer,
                            OuterAction::Continue => {}
                        }
                    }
                    Err(err) => {
                        match handle_read_failure(
                            &mut state,
                            &frame,
                            opts,
                            &err,
                            lba,
                            count,
                            pos,
                            block_bytes,
                            bytes,
                            read_duration_ms,
                            &pipe,
                            &shared,
                            reader,
                        )? {
                            FailureAction::Continue => {}
                            FailureAction::ContinueInner => continue,
                            FailureAction::BreakOuter => break 'outer,
                        }
                    }
                }

                let did_skip = compute_damage_skip(&mut state, &mut frame, opts, lba, block_bytes);

                if !did_skip {
                    if opts.reverse {
                        frame.block_end = frame.block_end.saturating_sub(block_bytes);
                    } else {
                        frame.block_end += block_bytes;
                    }
                }

                if opts.wedged_threshold > 0 && state.consecutive_failures >= opts.wedged_threshold
                {
                    // Only exit wedged after attempting multiple ranges with zero recovery.
                    // Single-range terminal failures should not abort the entire pass.
                    let multi_range_attempted = frame.range_idx > 0;
                    if multi_range_attempted {
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_wedged_exit",
                            consecutive_failures = state.consecutive_failures,
                            blocks_read_failed = state.blocks_read_failed,
                            blocks_read_ok = state.blocks_read_ok,
                            range_index = frame.range_idx,
                            total_ranges = bad_ranges.len(),
                            "Disc::patch giving up — drive appears wedged after multiple ranges"
                        );
                        state.wedged_exit = true;
                        break 'outer;
                    }
                }

                state.work_done = state.work_done.saturating_add(block_bytes);

                if self.report_patch_progress(&state, opts, total_bytes, &shared) {
                    state.halted = true;
                    break 'outer;
                }
            }
        }

        // Drain the consumer thread: drop tx, wait for `close` to run
        // sync_all + mapfile.flush, then take the final stats from the
        // sink's summary. `close` failing on a regular-file sync_all is
        // surfaced here as `Error::IoError`, matching pre-split
        // behaviour.
        let summary = pipe.finish()?;

        Ok(build_outcome(
            &state,
            &summary,
            path,
            total_bytes,
            bad_ranges.len(),
            opts.wedged_threshold,
        ))
    }
}
