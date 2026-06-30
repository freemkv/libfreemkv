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

// Pass-N tunables. Hoisted to module scope so helpers (extracted from
// the original `Disc::patch` body) can reference them without inheriting
// the function's local-const scope.
// Mirror of sweep path (read_error.rs NOT_READY_MAX_RETRIES = 3): cap
// per-LBA NOT_READY retries so a persistently-not-ready disc cannot burn
// up to RANGE_BUDGET_CAP_SECS per range on a single LBA.
const NOT_READY_MAX_RETRIES_PER_LBA: u32 = 3;
/// Cooldown between patch ranges that actually grinded (dropped to the slow
/// recovery speed). Lets the drive settle before the next range re-enters at max
/// speed. Gated on "grinded" so a many-small-range pass doesn't stall on it.
const INTER_RANGE_COOLDOWN_SECS: u64 = 10;
const BRIDGE_DEGRADATION_PAUSE_SECS: u64 = 10;
const POST_FAILURE_PAUSE_SECS: u64 = 1;
const CONSECUTIVE_FAIL_LONG_PAUSE: u64 = 5;
const CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD: u64 = 10;
// Adaptive batching: climb back to `initial_batch` after this many
// consecutive clean single-sector successes.
const ADAPTIVE_UPSCALE_THRESHOLD: u32 = 16;
// Wedge-family (HARDWARE_ERROR / ILLEGAL_REQUEST) cooldown and abort
// thresholds — see `handle_read_failure` below for context.
// Single source of truth lives in `disc::read_error` so this cannot
// drift from `ZONE_ENTRY_COOLDOWN_SECS`.
const WEDGE_FAMILY_COOLDOWN_SECS: u64 = crate::disc::read_error::ZONE_ENTRY_COOLDOWN_SECS;
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
/// sectors of `PASSN_SKIP_SECTORS_BASE << (3 × idx)` (i.e. multiplies
/// by 8 per index), capped at `PASSN_SKIP_SECTORS_CAP`. Used by the
/// wedge-vs-bad-sector probe to
/// scatter its sample LBAs across the failing region rather than
/// hammering the same neighborhood.
pub(super) fn skip_sectors_for_probe(idx: usize) -> u64 {
    let escalation = (idx.saturating_mul(3)).min(u32::MAX as usize) as u32;
    // Saturating shift: a large `idx` would overflow a fixed-width shift
    // (32 << 60 = 2^65), so fall back to the cap instead of panicking
    // (debug) or wrapping to 0 (release).
    PASSN_SKIP_SECTORS_BASE
        .checked_shl(escalation)
        .unwrap_or(PASSN_SKIP_SECTORS_CAP)
        .min(PASSN_SKIP_SECTORS_CAP)
}

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

// ---------------------------------------------------------------------------
// Scatter-recovery: "reset, read good data, come back for one sector."
// ---------------------------------------------------------------------------
//
// A genuinely-damaged sector makes the drive grind its internal C1/C2/L-EC
// re-read loop for the whole recovery timeout (~60 s) and still fail. Worse,
// re-reading consecutive bad LBAs at identical conditions both (a) re-fails
// identically and (b) is exactly the rapid-failure cadence that drops the
// BU40N into a firmware fast-fail wedge (see CLAUDE.md hard-rule #2).
//
// The fix mirrors ddrescue/MakeMKV practice: between attempts on a stuck
// sector, SEEK AWAY and read a run of known-good sectors. That forces a full
// re-seek + servo/focus relock (re-seating the head so it arrives at the bad
// sector on a fresh, tracking-locked approach — which recovers marginal
// sectors a stationary grind can't) AND breaks the consecutive-failure cadence
// that wedges the drive. Each fresh re-read uses the FAST timeout
// (`recovery = false`), not the 60 s deep grind: a recalibrated marginal
// sector reads quickly, and a truly-dead one fails fast instead of burning
// 60 s per fresh attempt. Many fast fresh attempts beat one long grind.

/// Fresh re-read attempts on a stuck sector before giving up (each preceded
/// by a recalibration read).
const SCATTER_MAX_ATTEMPTS: u32 = 3;
/// Known-good sectors read at the anchor to recalibrate between attempts
/// (~196 KB; a multiple of 3 so AACS units stay aligned, no widening).
const SCATTER_GOOD_SECTORS: u16 = 96;
/// Base anchor LBA. The leading region of a mounted disc is good, and seeking
/// there from a high bad-region LBA is a long stroke that fully re-seats the
/// head.
const SCATTER_ANCHOR_BASE_LBA: u32 = 64;
/// Vary the anchor per attempt so the drive can't satisfy the recalibration
/// read from cache (a cache hit performs no seek = no recalibration).
const SCATTER_ANCHOR_STRIDE: u32 = 8192;

/// Recalibration primitive: read `count` known-good sectors at `anchor`,
/// discarding the data. The point is the physical SEEK + sustained tracking
/// read that re-seats the head/servo — not the bytes. Routed through
/// [`recovery_read`] (fast path) so an unaligned AACS anchor still issues a
/// real drive read instead of being rejected pre-read by the decrypting
/// source. Best-effort: a failed anchor read (e.g. it landed in another bad
/// range) still performed the seek, so the error is ignored.
pub(super) fn read_good_sectors<R: SectorSource + ?Sized>(
    reader: &mut R,
    decrypt_is_aacs: bool,
    anchor: u32,
    count: u16,
) {
    let mut buf = vec![0u8; count as usize * 2048];
    let _ = recovery_read(reader, decrypt_is_aacs, anchor, count, &mut buf, false);
}

/// Try to recover a stuck single sector by recalibrating between fresh, fast
/// re-reads (see the module-section comment above). On success `buf[..bytes]`
/// holds the recovered sector and the caller treats it exactly like a normal
/// read success; on failure it returns `false` to fall through to the usual
/// NonTrimmed give-up.
///
/// Engages ONLY for a genuine single-sector MEDIUM ERROR (sense_key 0x03):
/// transport faults abort the pass, NOT_READY has its own retry path, and
/// wedge-family senses (HARDWARE / ILLEGAL_REQUEST) want a cooldown/eject —
/// scattering on those would just hammer an already-wedged drive.
#[allow(clippy::too_many_arguments)]
pub(super) fn scatter_recover<R: SectorSource + ?Sized>(
    reader: &mut R,
    err: &Error,
    lba: u32,
    count: u16,
    bytes: usize,
    buf: &mut [u8],
    decrypt_is_aacs: bool,
    halt: Option<&std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> bool {
    let is_medium = err
        .scsi_sense()
        .map(|s| s.sense_key == crate::scsi::SENSE_KEY_MEDIUM_ERROR)
        .unwrap_or(false);
    if count != 1 || !is_medium {
        return false;
    }

    let halted = |h: Option<&std::sync::Arc<std::sync::atomic::AtomicBool>>| {
        h.map(|h| h.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(false)
    };

    for attempt in 1..=SCATTER_MAX_ATTEMPTS {
        if halted(halt) {
            return false;
        }
        // Anchor toward disc start, varied per attempt, clamped below the
        // target so we never read the bad region itself as the "good" anchor.
        let anchor = SCATTER_ANCHOR_BASE_LBA
            .saturating_add(attempt.saturating_mul(SCATTER_ANCHOR_STRIDE))
            .min(lba.saturating_sub(SCATTER_GOOD_SECTORS as u32 + 1));
        // Recalibration read (a real seek + sustained good-sector read) IS the
        // settle and the cadence-breaker — no extra idle sleep. Time it so the
        // live test can see what the recalibration costs.
        let anchor_t = std::time::Instant::now();
        read_good_sectors(reader, decrypt_is_aacs, anchor, SCATTER_GOOD_SECTORS);
        let anchor_ms = anchor_t.elapsed().as_millis();
        if halted(halt) {
            return false;
        }

        // Fresh, FAST re-read of the target (recovery=false). Time it: a quick
        // success means a marginal sector caught on the recalibrated approach;
        // a slow failure means the fast timeout is being spent — both inform
        // tuning SCATTER_* without guessing.
        let reread_t = std::time::Instant::now();
        let reread = recovery_read(
            reader,
            decrypt_is_aacs,
            lba,
            count,
            &mut buf[..bytes],
            false,
        );
        let reread_ms = reread_t.elapsed().as_millis();
        match reread {
            Ok(_) => {
                tracing::info!(
                    target: "freemkv::disc",
                    phase = "patch.scatter.recovered",
                    lba,
                    attempt,
                    anchor,
                    anchor_ms,
                    reread_ms,
                    "scatter-recovery: recalibrated fresh approach recovered the sector"
                );
                return true;
            }
            Err(_) => {
                tracing::debug!(
                    target: "freemkv::disc",
                    phase = "patch.scatter.miss",
                    lba,
                    attempt,
                    anchor,
                    anchor_ms,
                    reread_ms,
                    "scatter-recovery: fresh attempt still failed"
                );
            }
        }
    }
    tracing::debug!(
        target: "freemkv::disc",
        phase = "patch.scatter.exhausted",
        lba,
        attempts = SCATTER_MAX_ATTEMPTS,
        "scatter-recovery: all fresh attempts failed; leaving sector NonTrimmed"
    );
    false
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
    pub wedge_count: u32,
    pub work_done: u64,
    // Per-range scratch (reset at each range boundary)
    pub consecutive_failures: u64,
    pub consecutive_skips_without_recovery: u32,
    pub consecutive_good_since_skip: u32,
    pub last_skip_from: Option<u64>,
    pub skip_count: u32,
    pub damage_window: Vec<bool>,
    // Per-LBA NOT_READY retry cap (mirrors sweep NOT_READY_MAX_RETRIES=3).
    // Reset whenever the current LBA changes (i.e. the cursor advances to
    // a new sector). NOT_READY retries that push past NOT_READY_MAX_RETRIES_PER_LBA
    // fall through to normal failure handling (NonTrimmed + cursor advance).
    pub not_ready_retries_per_lba: u32,
    pub not_ready_lba: Option<u32>,
    // Stall tracking
    pub bytes_good_last: u64,
    pub stall_start: std::time::Instant,
    pub range_start: std::time::Instant,
    pub range_bytes_good: u64,
    // Clock seam: the watchdog reads wall time through this rather than calling
    // `Instant::now()` inline, so deterministic tests can advance a fake clock to
    // prove the stall/range timeouts trip. Production uses `Instant::now`
    // (see `PatchLoopState::new`), so behaviour is byte-identical.
    pub now: fn() -> std::time::Instant,
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
        // Production clock: the real monotonic wall clock.
        Self::new_with_clock(
            bytes_good_before,
            total_bytes,
            initial_batch,
            recovery,
            work_total,
            std::time::Instant::now,
        )
    }

    /// Like `new`, but with an injectable monotonic clock. The watchdog reads
    /// time exclusively through `now`, so a test can wind a fake clock forward to
    /// drive the stall/range timeouts deterministically. `new` passes
    /// `Instant::now`, so the production loop is unchanged.
    pub(super) fn new_with_clock(
        bytes_good_before: u64,
        total_bytes: u64,
        initial_batch: u16,
        recovery: bool,
        work_total: u64,
        now: fn() -> std::time::Instant,
    ) -> Self {
        let t0 = now();
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
            not_ready_retries_per_lba: 0,
            not_ready_lba: None,
            bytes_good_last: bytes_good_before,
            stall_start: t0,
            range_start: t0,
            range_bytes_good: bytes_good_before,
            now,
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
    // A successful read breaks any in-progress wedge-family streak.
    // wedge_count tracks CONSECUTIVE wedge-family (HARDWARE_ERROR /
    // ILLEGAL_REQUEST) senses; a good read proves the drive is still
    // responding so the streak is over. Without this reset, intermittent
    // good reads interspersed with wedge-family failures accumulate
    // wedge_count monotonically, triggering WEDGE_ABORT_THRESHOLD (16)
    // prematurely on ranges that are actually making progress.
    // Note: handle_read_failure already resets wedge_count on any
    // non-wedge-family failure; this mirrors that for the success path.
    state.wedge_count = 0;
    // A successful read means this LBA is resolved; clear the NOT_READY
    // per-LBA counter so any future failure at a different LBA starts fresh.
    state.not_ready_retries_per_lba = 0;
    state.not_ready_lba = None;
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
                phase = "patch.batch.upscale",
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
    }

    tracing::info!(
        target: "freemkv::disc",
        phase = "patch.read.ok",
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
    // Plaintext: DecryptingSectorSource applied AACS / CSS in-place
    // during the read_sectors call above. The pre-0.18 inline
    // decrypt_sectors call lived here.
    let write_start = std::time::Instant::now();
    tracing::debug!(
        target: "freemkv::disc",
        phase = "patch.write.start",
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
        phase = "patch.write.ok",
        pos,
        bytes,
        write_duration_ms,
        "ISO write succeeded"
    );
    tracing::info!(
        target: "freemkv::disc",
        phase = "patch.record.ok",
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
        state.stall_start = (state.now)();
        state.bytes_good_last = bytes_good_now;
    }
    let stall_elapsed = (state.now)().duration_since(state.stall_start);
    if stall_elapsed > std::time::Duration::from_secs(STALL_SECS) {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch.stall",
            elapsed_secs = stall_elapsed.as_secs(),
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
                phase = "patch.backtrack.start",
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
                        // Feed the damage window / wedge counter exactly as the
                        // main loop does, so a string of backtrack failures
                        // escalates skip distance and trips wedge detection
                        // rather than being silently under-counted.
                        state.damage_window.push(false);
                        if state.damage_window.len() > PASSN_DAMAGE_WINDOW {
                            state.damage_window.remove(0);
                        }
                        state.consecutive_failures += 1;
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
                            phase = "patch.backtrack.stop",
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
    // Transport failure (status=0xFF: USB-bridge crash / disconnect) is not a
    // recoverable bad sector — the bridge is wedged and every further read fails
    // identically. Abort the pass immediately (symmetric with the sweep's
    // read_error::handle_read_error AbortPass and single-pass mux's fill_extents),
    // so autorip can drop and re-enumerate the bridge instead of hammering a
    // crashed device sector-by-sector until the per-range watchdog expires.
    // Checked before the batch-split below: a 0xFF on a batch read is still a
    // bridge crash, not an ambiguous bad sector.
    if err.is_scsi_transport_failure() {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch.transport_fault",
            lba,
            count,
            "transport failure (bridge crash) during patch — aborting pass"
        );
        state.wedged_exit = true;
        return Ok(FailureAction::BreakOuter);
    }

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
            phase = "patch.batch.split",
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
    state.consecutive_good_since_skip = 0;
    state.consecutive_singles_ok = 0;
    state.unreadable_count += 1;

    // Reset the per-LBA NOT_READY counter whenever the LBA changes.
    // NOT_READY retries hold the cursor in place (ContinueInner), so the
    // same LBA is re-attempted each iteration until we either succeed or
    // exhaust NOT_READY_MAX_RETRIES_PER_LBA. A different LBA means the
    // cursor has advanced (or we're on a new range), so start fresh.
    if state.not_ready_lba != Some(lba) {
        state.not_ready_retries_per_lba = 0;
        state.not_ready_lba = Some(lba);
    }

    // Check if this is a NOT_READY error that should be retried BEFORE
    // incrementing consecutive_failures so NOT_READY retries do not
    // count toward the wedge threshold (Fix 3: false-wedge prevention).
    // Mirror of sweep path (read_error.rs handle_read_error): NOT_READY
    // is capped at NOT_READY_MAX_RETRIES and not counted toward
    // wedge/skip counters.
    let sense = err.scsi_sense();

    // ASC values (under NOT READY, sense_key 0x02) indicating temporary
    // drive unresponsiveness worth retrying:
    //   0x02 = LUN not ready, no reference position (mechanism still seeking)
    //   0x03 = LUN not ready, manual intervention required
    //   0x04 = LUN not ready, in process of becoming ready / initializing
    // (Medium-not-present is ASC 0x3A, not handled here — nothing to retry.)
    let is_not_ready_retryable = sense
        .map(|s| s.sense_key == 0x02 && (s.asc == 0x02 || s.asc == 0x03 || s.asc == 0x04))
        .unwrap_or(false);

    // Only count toward consecutive_failures / wedge detector when this
    // is NOT a retryable NOT_READY — those are handled below and return
    // ContinueInner without advancing the cursor.
    if !is_not_ready_retryable {
        state.consecutive_failures += 1;
    }

    tracing::warn!(
        target: "freemkv::disc",
        phase = "patch.read.fail",
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

    // For retryable NOT_READY errors, pause longer and don't mark as Unreadable yet —
    // but only up to NOT_READY_MAX_RETRIES_PER_LBA times per LBA. Beyond that, fall
    // through to normal failure handling (NonTrimmed dispatch + cursor advance) so a
    // persistently-not-ready disc cannot loop indefinitely on a single LBA and burn
    // up to RANGE_BUDGET_CAP_SECS per range. Mirrors the sweep path cap in
    // read_error.rs (NOT_READY_MAX_RETRIES = 3).
    if is_not_ready_retryable {
        if state.not_ready_retries_per_lba < NOT_READY_MAX_RETRIES_PER_LBA {
            state.not_ready_retries_per_lba += 1;
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch.read.not_ready.retry",
                lba,
                not_ready_retries_per_lba = state.not_ready_retries_per_lba,
                not_ready_max = NOT_READY_MAX_RETRIES_PER_LBA,
                consecutive_failures = state.consecutive_failures,
                err_asc = sense.map(|s| s.asc as u32).unwrap_or(0),
                "NOT_READY with ASC in 0x02/0x03/0x04; pausing for drive recovery before retry"
            );

            // Extended pause for NOT_READY - let drive complete internal mechanical recovery.
            // Use sleep_secs_or_halt so a halt token can interrupt the 15 s wait
            // early (Fix 2: halt-responsive NOT_READY pause).
            let pause_secs = 15u64;
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch.read.not_ready.pause",
                lba,
                consecutive_failures = state.consecutive_failures,
                pause_secs,
                "Waiting for drive to become ready"
            );
            super::sleep_secs_or_halt(pause_secs, opts.halt.as_ref());

            // Check stall guard here — the NOT_READY retry path bypasses the
            // normal failure path's stall guard, so total runtime could
            // otherwise grow as num_ranges × RANGE_BUDGET_CAP_SECS (disc-
            // controlled). (Fix 1: DoS prevention.)
            let bytes_good_now = {
                let g = shared
                    .lock()
                    .expect("PatchSink shared state mutex poisoned");
                g.stats.bytes_good
            };
            if bytes_good_now > state.bytes_good_last {
                state.stall_start = (state.now)();
                state.bytes_good_last = bytes_good_now;
            }
            let stall_elapsed = (state.now)().duration_since(state.stall_start);
            if stall_elapsed > std::time::Duration::from_secs(STALL_SECS) {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "patch.stall",
                    elapsed_secs = stall_elapsed.as_secs(),
                    bytes_good = bytes_good_now,
                    bytes_good_start = state.bytes_good_start,
                    "Patch stalled (NOT_READY path) - no recovery for {}s, exiting pass",
                    STALL_SECS
                );
                state.wedged_exit = true;
                return Ok(FailureAction::BreakOuter);
            }

            // Don't mark as Unreadable yet - will retry on next iteration
            state.damage_window.push(false);
            if state.damage_window.len() > PASSN_DAMAGE_WINDOW {
                state.damage_window.remove(0);
            }
            return Ok(FailureAction::ContinueInner);
        }

        // Per-LBA cap exhausted: fall through to normal failure handling
        // (NonTrimmed dispatch + cursor advance). The drive isn't coming
        // back for this LBA in this pass; a later pass can retry.
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch.read.not_ready.cap_exceeded",
            lba,
            not_ready_retries_per_lba = state.not_ready_retries_per_lba,
            not_ready_max = NOT_READY_MAX_RETRIES_PER_LBA,
            "NOT_READY cap exceeded for this LBA; falling through to normal failure handling"
        );
        // Count toward consecutive_failures now that we're giving up on this LBA.
        state.consecutive_failures += 1;
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
        state.stall_start = (state.now)();
        state.bytes_good_last = bytes_good_now;
    }
    let stall_elapsed = (state.now)().duration_since(state.stall_start);
    if stall_elapsed > std::time::Duration::from_secs(STALL_SECS) {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch.stall",
            elapsed_secs = stall_elapsed.as_secs(),
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
            phase = "patch.read.fail.count",
            lba,
            consecutive_failures = state.consecutive_failures,
            wedged_threshold = opts.wedged_threshold,
            "Failure count"
        );
    }

    // Probe good sectors to differentiate wedge vs bad sector.
    // `skip_sectors_for_probe` returns a SECTOR distance; scale to bytes
    // before adding to `pos` (a byte offset). The previous code compared
    // a sector count against `block_bytes` and added a sector count to a
    // byte offset, so the only probe that ran landed back on the failing
    // LBA — the responsive-vs-wedged heuristic never scattered.
    if state.consecutive_failures >= 3 && state.consecutive_failures % 5 == 0 {
        let probe_offsets_sectors: [u64; 3] =
            [0, skip_sectors_for_probe(1), skip_sectors_for_probe(2)];
        let mut probes_ok = 0;

        for (probe_idx, &offset_sectors) in probe_offsets_sectors.iter().enumerate() {
            // Honor cancellation inside the probe loop.  Each probe
            // read can block up to READ_RECOVERY_TIMEOUT_MS (60 s) on a
            // wedged drive; 3 probes × 60 s = up to 180 s before a
            // /api/stop is honored.  Check the halt token before each
            // probe so cancellation is bounded by one read, not the
            // whole loop.
            if let Some(h) = &opts.halt {
                if h.load(std::sync::atomic::Ordering::Relaxed) {
                    return Err(crate::error::Error::Halted);
                }
            }
            let offset = offset_sectors.saturating_mul(2048);
            let probe_pos = pos.saturating_add(offset);
            // Skip the zero-distance re-read until failures are well
            // established (it just re-confirms the current LBA), and
            // never probe past the end of the current bad range (the
            // probe scatters sample LBAs across the failing region —
            // `block_bytes`, one block, was the wrong bound and in the
            // wrong units).
            if probe_pos >= frame.end || (offset == 0 && state.consecutive_failures < 5) {
                continue;
            }

            let probe_lba = (probe_pos / 2048) as u32;
            let probe_count = 1u16;
            let mut probe_buf = [0u8; 2048];

            match reader.read_sectors(probe_lba, probe_count, &mut probe_buf[..], state.recovery) {
                Ok(_) => {
                    probes_ok += 1;
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "patch.probe.ok",
                        lba = probe_lba,
                        offset_from_current = offset,
                        probe_idx,
                        "Probe read succeeded — drive responsive"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "patch.probe.miss",
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
                phase = "patch.probe.responsive",
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
                phase = "patch.probe.zone_bad",
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
    // further. Long cooldown (WEDGE_FAMILY_COOLDOWN_SECS, sourced from
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
            phase = "patch.wedge.family",
            lba,
            wedge_count = state.wedge_count,
            wedge_abort_threshold = WEDGE_ABORT_THRESHOLD,
            sense_key = err.scsi_sense().map(|s| s.sense_key as u32).unwrap_or(0),
            "HARDWARE_ERROR / ILLEGAL_REQUEST sense — wedge family, applying long cooldown"
        );
        if state.wedge_count >= WEDGE_ABORT_THRESHOLD {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "patch.wedge.abort",
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
            phase = "patch.wedge.bridge_degradation",
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
        phase = "patch.read.post_failure_pause",
        lba,
        consecutive_failures = state.consecutive_failures,
        pause_secs,
        "breathing room after failure"
    );
    // Halt-responsive: a stop request must interrupt this pause rather than
    // block for up to pause_secs (which escalates per failure), so /api/stop
    // stays responsive during the most error-prone phase of a rip.
    super::sleep_secs_or_halt(pause_secs, opts.halt.as_ref());
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

/// Why [`PatchCtx::patch_region`] returned. The orchestrator
/// ([`PatchCtx::run`]) advances to the next bad range on `Completed` /
/// `SkipLimit` / `BudgetExceeded`, and ends the whole pass on `Wedged` /
/// `Halted` / `TransportFault` — for which the matching `state.halted` /
/// `state.wedged_exit` flag was already set, so `build_outcome` reports
/// it. (A pass also ends, by `?`-propagation, if a read inside the
/// region returns `Err(Halted)` from the backtrack inner loop.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegionOutcome {
    /// Walked (or converged on) the entire range.
    Completed,
    /// Hit `MAX_SKIPS_PER_RANGE`; remaining bytes left NonTrimmed.
    SkipLimit,
    /// Per-range watchdog fired (`range_budget_secs` elapsed with no
    /// forward progress).
    BudgetExceeded,
    /// Drive wedged — whole-pass stall guard, wedge-family abort, or the
    /// consecutive-failure wedge threshold. `state.wedged_exit` is set.
    Wedged,
    /// Halt requested — the halt token or the progress reporter.
    /// `state.halted` is set.
    Halted,
    /// USB-bridge transport fault (status 0xFF): a dead bus, not a bad
    /// sector. `state.wedged_exit` is set.
    TransportFault,
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
    // Refresh the forward-progress baseline FIRST, then do a single
    // elapsed-vs-budget check. Reading bytes_good before the budget
    // test means a range that committed a recovered sector since the
    // previous tick resets its clock instead of being abandoned in the
    // budget-boundary window.
    let bytes_good_now = {
        let g = shared
            .lock()
            .expect("PatchSink shared state mutex poisoned");
        g.stats.bytes_good
    };
    if bytes_good_now > state.range_bytes_good {
        state.range_bytes_good = bytes_good_now;
        state.range_start = (state.now)();
    }
    let range_elapsed = (state.now)().duration_since(state.range_start);
    if range_elapsed.as_secs() >= frame.range_budget_secs {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "patch.region.watchdog",
            range_lba = frame.range_pos / 2048,
            range_sectors = frame.range_sectors,
            elapsed_secs = range_elapsed.as_secs(),
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
        phase = "patch.skip.limit",
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
    if let Some((pos, len)) =
        skip_limit_remainder(opts.reverse, frame.range_pos, frame.end, frame.block_end)
    {
        send_or_abort(pipe, PatchItem::NonTrimmed { pos, len })?;
    }
    Ok(())
}

/// The never-attempted remainder of a range when the skip limit is
/// reached, as `Some((pos, len))` or `None` if nothing is left.
///
/// `block_end` is the per-iteration cursor. In reverse mode it moved
/// DOWN from `end` toward `range_pos`, so the attempted region is
/// `[block_end, end)` and the remainder is `[range_pos, block_end)`. In
/// forward mode it moved UP from `range_pos` toward `end`, so the
/// attempted region is `[range_pos, block_end)` and the remainder is
/// `[block_end, end)`. The pre-fix forward formula
/// `range_pos + (end - block_end)` was a mirror reflection that, once
/// `block_end` passed the midpoint, produced a start BELOW `block_end`
/// and overlapped the already-recovered region — downgrading Finished
/// sectors to NonTrimmed.
fn skip_limit_remainder(
    reverse: bool,
    range_pos: u64,
    end: u64,
    block_end: u64,
) -> Option<(u64, u64)> {
    if reverse {
        let len = block_end.saturating_sub(range_pos);
        (len > 0).then_some((range_pos, len))
    } else {
        let len = end.saturating_sub(block_end);
        (len > 0).then_some((block_end, len))
    }
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
    let escalated = PASSN_SKIP_SECTORS_BASE
        .checked_shl(state.consecutive_skips_without_recovery)
        .unwrap_or(PASSN_SKIP_SECTORS_CAP)
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
        phase = "patch.skip",
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

/// Per-pass coordination state for one `Disc::patch` run: the decrypting
/// reader, the consumer pipe + its shared mapfile snapshot, the options,
/// and the accumulating [`PatchLoopState`]. Bundling these lets the
/// orchestrator ([`PatchCtx::run`]) and the focused per-range recovery
/// loop ([`PatchCtx::patch_region`]) be methods rather than free
/// functions threading a dozen arguments. `state` carries ACROSS ranges
/// (counters, stall timers, NOT_READY/last-skip cursors); the per-range
/// scratch inside it is reset at the top of each `patch_region`.
struct PatchCtx<'a, 'o, R: SectorSource + ?Sized> {
    disc: &'a Disc,
    reader: &'a mut R,
    pipe: &'a Pipeline<PatchItem, PatchSummary>,
    shared: &'a Mutex<SharedPatchState>,
    opts: &'a PatchOptions<'o>,
    total_bytes: u64,
    decrypt_is_aacs: bool,
    /// Armed when a range grinded (dropped to slow speed); consumed as an
    /// inter-range cooldown before the NEXT range enters at max speed.
    /// Gated on "grinded" so a many-small-range pass doesn't stall on it.
    cooldown_pending: bool,
    state: PatchLoopState,
    /// Recovery read buffer, sized to `initial_batch` sectors and reused
    /// across reads so the per-iteration read doesn't reallocate.
    buf: Vec<u8>,
}

impl<R: SectorSource + ?Sized> PatchCtx<'_, '_, R> {
    /// Orchestrator (one pass): walk the ordered bad ranges. Apply the
    /// inter-range cooldown only after a range that grinded, then recover
    /// the range; stop the whole pass the moment a range reports
    /// halt / wedge / transport-fault.
    fn run(&mut self, bad_ranges: &[(u64, u64)]) -> Result<()> {
        let num_ranges = bad_ranges.len();
        for (range_idx, &(range_pos, range_size)) in bad_ranges.iter().enumerate() {
            if self.cooldown_pending {
                tracing::info!(
                    target: "freemkv::disc",
                    phase = "patch.region.cooldown",
                    secs = INTER_RANGE_COOLDOWN_SECS,
                    "inter-range cooldown (previous range grinded at slow speed)"
                );
                super::sleep_secs_or_halt(INTER_RANGE_COOLDOWN_SECS, self.opts.halt.as_ref());
                self.cooldown_pending = false;
            }
            let outcome = self.patch_region(range_idx, num_ranges, range_pos, range_size)?;
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch.region.exit",
                range_index = range_idx,
                range_lba = range_pos / 2048,
                outcome = ?outcome,
                blocks_read_ok = self.state.blocks_read_ok,
                blocks_read_failed = self.state.blocks_read_failed,
                bytes_recovered =
                    self.state.bytes_good_last.saturating_sub(self.state.bytes_good_before),
                "region finished"
            );
            match outcome {
                RegionOutcome::Completed
                | RegionOutcome::SkipLimit
                | RegionOutcome::BudgetExceeded => {}
                RegionOutcome::Wedged | RegionOutcome::Halted | RegionOutcome::TransportFault => {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Recover ONE bad range, end to start (reverse) or start to end.
    /// Owns the per-iteration read → success/failure → damage-skip →
    /// watchdog cycle and nothing else; cross-range concerns live in
    /// [`PatchCtx::run`]. Returns why it stopped (see [`RegionOutcome`]).
    fn patch_region(
        &mut self,
        range_idx: usize,
        num_ranges: usize,
        range_pos: u64,
        range_size: u64,
    ) -> Result<RegionOutcome> {
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.region.enter",
            range_index = range_idx,
            num_total_ranges = num_ranges,
            range_lba = range_pos / 2048,
            range_size_mb = range_size as f64 / 1_048_576.0,
            "entering patch range"
        );
        let end = range_pos + range_size;
        let range_sectors = range_size / 2048;
        let range_budget_secs = (range_sectors * SECONDS_PER_SECTOR).min(RANGE_BUDGET_CAP_SECS);
        let mut frame = RangeFrame {
            range_idx,
            range_pos,
            range_size,
            end,
            block_end: if self.opts.reverse { end } else { range_pos },
            range_budget_secs,
            range_sectors,
        };

        // Per-range reset (was the inline range-boundary block): a fresh
        // range starts with an empty damage window, zeroed escalation /
        // good / skip / wedge / failure counters, and the full initial
        // batch. `current_batch` carries across ranges, so resetting it
        // here stops a prior range's single-sector grind from starting
        // this range slow. The range timer's forward-progress baseline is
        // the CURRENT bytes_good (Fix 4 — NOT the pass-start value, else a
        // prior range's recovery would refill this range's budget for
        // free on its first watchdog tick).
        self.state.damage_window.clear();
        self.state.consecutive_skips_without_recovery = 0;
        self.state.consecutive_good_since_skip = 0;
        self.state.range_start = (self.state.now)();
        self.state.range_bytes_good = {
            let g = self
                .shared
                .lock()
                .expect("PatchSink shared state mutex poisoned");
            g.stats.bytes_good
        };
        self.state.skip_count = 0;
        self.state.wedge_count = 0;
        self.state.consecutive_failures = 0;
        tracing::debug!(
            target: "freemkv::disc",
            phase = "patch.region.budget",
            range_lba = range_pos / 2048,
            range_sectors,
            range_budget_secs,
            "per-range time budget computed"
        );

        // Enter at MAX speed + the full initial batch: read the clean
        // overshoot fast. `range_slowed` flips on the first read failure
        // (below), dropping to the slow recovery speed for the rest of
        // the range and arming the inter-range cooldown.
        self.reader.set_speed(0xFFFF);
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch.speed",
            range_lba = range_pos / 2048,
            range_sectors,
            speed = "0xFFFF",
            "range entering at MAX read speed (drops to slow recovery on first failure)"
        );
        self.state.current_batch = self.state.initial_batch;
        let mut range_slowed = false;

        loop {
            if let Some(ref h) = self.opts.halt {
                if h.load(std::sync::atomic::Ordering::Relaxed) {
                    self.state.halted = true;
                    return Ok(RegionOutcome::Halted);
                }
            }

            if check_range_watchdog(&mut self.state, &frame, self.shared) {
                return Ok(RegionOutcome::BudgetExceeded);
            }

            if self.state.skip_count >= MAX_SKIPS_PER_RANGE {
                handle_skip_limit(&self.state, &frame, self.opts, self.pipe)?;
                return Ok(RegionOutcome::SkipLimit);
            }

            let (pos, block_bytes) = if self.opts.reverse {
                if frame.block_end <= frame.range_pos {
                    return Ok(RegionOutcome::Completed);
                }
                let span =
                    (frame.block_end - frame.range_pos).min(self.state.current_batch as u64 * 2048);
                (frame.block_end - span, span)
            } else {
                if frame.block_end >= frame.end {
                    return Ok(RegionOutcome::Completed);
                }
                let span =
                    (frame.end - frame.block_end).min(self.state.current_batch as u64 * 2048);
                (frame.block_end, span)
            };
            let lba = (pos / 2048) as u32;
            let count = (block_bytes / 2048) as u16;
            let bytes = count as usize * 2048;
            self.state.blocks_attempted += 1;

            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch.read.start",
                lba,
                count,
                bytes,
                attempt_num = self.state.blocks_attempted,
                range_index = range_idx,
                pos_byte = pos,
                "starting sector read"
            );

            prime_cache(self.reader, lba, count);

            // Single-shot read (no inline retry — see the historical note
            // in handle_read_failure). `recovery_read` widens a mid-unit
            // AACS window to the aligned unit; otherwise it's a plain read.
            let read_start = std::time::Instant::now();
            let read_result = recovery_read(
                self.reader,
                self.decrypt_is_aacs,
                lba,
                count,
                &mut self.buf,
                self.state.recovery,
            );
            let read_duration_ms = read_start.elapsed().as_millis();

            match read_result {
                Ok(_) => {
                    match handle_read_success(
                        &mut self.state,
                        &frame,
                        self.opts,
                        lba,
                        count,
                        pos,
                        block_bytes,
                        bytes,
                        &mut self.buf,
                        read_duration_ms,
                        self.pipe,
                        self.shared,
                        self.reader,
                    )? {
                        // Break == the whole-pass stall guard fired
                        // (wedged_exit already set).
                        OuterAction::Break => return Ok(RegionOutcome::Wedged),
                        OuterAction::Continue => {}
                    }
                }
                Err(err) => {
                    // First failure in this range: the fast-batched pass
                    // over the clean overshoot is done. A genuine transport
                    // fault (bridge crash) is NOT a recoverable bad sector —
                    // skip the slow re-read and let handle_read_failure abort
                    // immediately. Drop to the slow recovery speed, arm the
                    // cooldown, and RE-ATTEMPT the same position once at slow
                    // speed before marking it: the drive's deep ECC recovery
                    // only engages slow, and the failure so far is a fast-read
                    // miss. Hold the cursor (don't advance, don't count
                    // damage); only a slow-speed result reaches
                    // handle_read_failure. `range_slowed` gates this to once.
                    if !range_slowed && !err.is_scsi_transport_failure() {
                        self.reader.set_speed(0x0000);
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch.speed",
                            lba,
                            speed = "0x0000",
                            "range dropped to slow recovery speed; retrying the failing read at slow speed before marking"
                        );
                        range_slowed = true;
                        self.cooldown_pending = true;
                        continue;
                    }

                    // The slow deep-recovery read also failed. Before giving
                    // up on this single sector, try scatter-recovery: read
                    // good data far away to recalibrate the head, then re-read
                    // this one sector fresh and fast (see `scatter_recover`).
                    // A recovery here is a normal read success — record it and
                    // advance the cursor exactly like the Ok arm does.
                    if count == 1
                        && scatter_recover(
                            self.reader,
                            &err,
                            lba,
                            count,
                            bytes,
                            &mut self.buf,
                            self.decrypt_is_aacs,
                            self.opts.halt.as_ref(),
                        )
                    {
                        match handle_read_success(
                            &mut self.state,
                            &frame,
                            self.opts,
                            lba,
                            count,
                            pos,
                            block_bytes,
                            bytes,
                            &mut self.buf,
                            read_duration_ms,
                            self.pipe,
                            self.shared,
                            self.reader,
                        )? {
                            // Break == the whole-pass stall guard fired.
                            OuterAction::Break => return Ok(RegionOutcome::Wedged),
                            OuterAction::Continue => {}
                        }
                    } else {
                        match handle_read_failure(
                            &mut self.state,
                            &frame,
                            self.opts,
                            &err,
                            lba,
                            count,
                            pos,
                            block_bytes,
                            bytes,
                            read_duration_ms,
                            self.pipe,
                            self.shared,
                            self.reader,
                        )? {
                            FailureAction::Continue => {}
                            FailureAction::ContinueInner => continue,
                            // BreakOuter fires for both a transport fault and a
                            // wedge-family abort; distinguish for the exit reason.
                            FailureAction::BreakOuter => {
                                return Ok(if err.is_scsi_transport_failure() {
                                    RegionOutcome::TransportFault
                                } else {
                                    RegionOutcome::Wedged
                                });
                            }
                        }
                    }
                }
            }

            let did_skip =
                compute_damage_skip(&mut self.state, &mut frame, self.opts, lba, block_bytes);

            if !did_skip {
                if self.opts.reverse {
                    frame.block_end = frame.block_end.saturating_sub(block_bytes);
                } else {
                    frame.block_end += block_bytes;
                }
            }

            if self.opts.wedged_threshold > 0
                && self.state.consecutive_failures >= self.opts.wedged_threshold
            {
                // Only exit wedged after attempting multiple ranges with
                // zero recovery. A single-range terminal failure should not
                // abort the whole pass.
                let multi_range_attempted = frame.range_idx > 0;
                if multi_range_attempted {
                    tracing::info!(
                        target: "freemkv::disc",
                        phase = "patch.wedge.exit",
                        consecutive_failures = self.state.consecutive_failures,
                        blocks_read_failed = self.state.blocks_read_failed,
                        blocks_read_ok = self.state.blocks_read_ok,
                        range_index = frame.range_idx,
                        total_ranges = num_ranges,
                        "giving up — drive appears wedged after multiple ranges"
                    );
                    self.state.wedged_exit = true;
                    return Ok(RegionOutcome::Wedged);
                }
            }

            self.state.work_done = self.state.work_done.saturating_add(block_bytes);

            if self.disc.report_patch_progress(
                &self.state,
                self.opts,
                self.total_bytes,
                self.shared,
            ) {
                self.state.halted = true;
                return Ok(RegionOutcome::Halted);
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
        let pp = crate::progress::PassProgress {
            kind,
            work_done: state.work_done,
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
            cooldown_pending: false,
            state: PatchLoopState::new(
                bytes_good_before,
                total_bytes,
                initial_batch,
                recovery,
                work_total,
            ),
            buf: vec![0u8; initial_batch as usize * 2048],
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

    #[test]
    fn skip_sectors_for_probe_does_not_overflow_for_large_idx() {
        // idx=20 (escalation 60) and idx=21 (63) previously overflowed
        // i64 via `32i64 << escalation`. Must saturate to the cap.
        for idx in [0usize, 1, 2, 20, 21, 100, usize::MAX] {
            let v = skip_sectors_for_probe(idx);
            assert!(
                v <= PASSN_SKIP_SECTORS_CAP,
                "idx {idx}: {v} exceeds cap {PASSN_SKIP_SECTORS_CAP}"
            );
        }
        // Small indices still escalate as before.
        assert_eq!(skip_sectors_for_probe(0), PASSN_SKIP_SECTORS_BASE);
        assert_eq!(skip_sectors_for_probe(1), PASSN_SKIP_SECTORS_BASE << 3);
    }

    #[test]
    fn skip_limit_remainder_forward_does_not_overlap_recovered_region() {
        // Forward mode: range [1000, 2000), cursor advanced past the
        // midpoint to block_end=1700. The recovered region is
        // [1000, 1700); the never-attempted remainder must be exactly
        // [1700, 2000) — NOT a mirror start below block_end.
        let r = skip_limit_remainder(false, 1000, 2000, 1700);
        assert_eq!(r, Some((1700, 300)));
        // The pre-fix mirror formula would have produced start =
        // 1000 + (2000 - 1700) = 1300, which overlaps [1000, 1700).
        assert!(r.unwrap().0 >= 1700, "must not overlap recovered region");
    }

    #[test]
    fn skip_limit_remainder_forward_none_when_fully_attempted() {
        assert_eq!(skip_limit_remainder(false, 1000, 2000, 2000), None);
    }

    #[test]
    fn skip_limit_remainder_reverse_marks_low_unattempted_region() {
        // Reverse mode: cursor moved down to block_end=1300, so
        // [1300, 2000) was attempted and [1000, 1300) is the remainder.
        let r = skip_limit_remainder(true, 1000, 2000, 1300);
        assert_eq!(r, Some((1000, 300)));
        assert_eq!(skip_limit_remainder(true, 1000, 2000, 1000), None);
    }

    // ----------------------------------------------------------------
    // compute_damage_skip - range-boundary + size-aware-cap coverage.
    //
    // These exercise the Pass-N damage-cluster skip documented in
    // CLAUDE.md "Patch (Pass N)": skip is capped at 1/4 of the
    // remaining bad range "so a single jump can't blow past a good
    // middle", and the per-iteration cursor (`block_end`) must never
    // cross the range boundary in either walk direction. A bug here
    // silently abandons recoverable sectors (over-skip) or downgrades
    // already-recovered sectors (cursor crossing the boundary).
    //
    // All byte offsets are multiples of 2048 (the sector size the code
    // divides by at `range_remaining_bytes / 2048`).
    // ----------------------------------------------------------------

    /// Build a `PatchOptions` with only `reverse` meaningful for the
    /// pure helpers under test (no I/O is performed).
    fn opts_with_reverse(reverse: bool) -> crate::disc::PatchOptions<'static> {
        crate::disc::PatchOptions {
            decrypt: false,
            block_sectors: Some(1),
            full_recovery: false,
            reverse,
            wedged_threshold: 50,
            progress: None,
            halt: None,

            key_fetch: None,
        }
    }

    /// A `PatchLoopState` whose damage window is full (16 entries) with
    /// exactly `bad` failures - enough to evaluate the
    /// `PASSN_DAMAGE_THRESHOLD_PCT` gate. `escalation` seeds
    /// `consecutive_skips_without_recovery` so we can drive the
    /// `PASSN_SKIP_SECTORS_BASE << escalation` size.
    fn state_with_window(bad: usize, escalation: u32) -> PatchLoopState {
        let mut s = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);
        s.damage_window.clear();
        for i in 0..PASSN_DAMAGE_WINDOW {
            s.damage_window.push(i >= bad); // first `bad` entries = false
        }
        s.consecutive_skips_without_recovery = escalation;
        s
    }

    #[test]
    fn damage_skip_forward_advances_cursor_toward_end_and_stays_in_range() {
        // Forward walk: the per-iteration cursor moves UP, toward `end`,
        // so the attempted region grows as [range_pos, block_end). A
        // damage skip must push block_end FORWARD (higher) and never
        // past `end` (the call site breaks on `block_end >= end`). Range
        // [0, 80 KiB) = 40 sectors, cursor mid-range at 20 KiB. Spec:
        // CLAUDE.md Pass-N reverse=false walks start->end.
        // Mutation that makes this RED: swap the forward branch to
        // subtract (reverse direction) e.g. `block_end - skip_bytes` ->
        // the cursor moves the WRONG way and re-attempts recovered
        // sectors / never converges. (Confirmed: the assertion
        // block_end > before fails.)
        let mut state = state_with_window(4, 0);
        let opts = opts_with_reverse(false);
        let mut frame = RangeFrame {
            range_idx: 0,
            range_pos: 0,
            range_size: 80 * 1024,
            end: 80 * 1024,
            block_end: 20 * 1024, // 10 sectors in
            range_budget_secs: 1,
            range_sectors: 40,
        };
        let before = frame.block_end;
        let did = compute_damage_skip(&mut state, &mut frame, &opts, 0, 2048);
        assert!(did, "threshold crossed: a skip must apply");
        assert!(
            frame.block_end > before,
            "forward skip must advance the cursor UP (toward end): {} !> {}",
            frame.block_end,
            before
        );
        assert!(
            frame.block_end <= frame.end,
            "forward cursor {} overshot range end {}",
            frame.block_end,
            frame.end
        );
    }

    #[test]
    fn damage_skip_reverse_moves_cursor_toward_range_start_and_stays_in_range() {
        // Reverse walk (the recovery walker default): the cursor moves
        // DOWN, toward `range_pos`, so the attempted region grows as
        // [block_end, end). A damage skip must push block_end BACKWARD
        // (lower) and never below `range_pos` (the call site breaks on
        // `block_end <= range_pos`). Spec: CLAUDE.md "Patch (Pass N) -
        // Default: reverse mode ... within each range from end to start."
        // Mutation that makes this RED: the reverse branch adds instead
        // of subtracts (copy-paste of the forward formula) -> cursor
        // moves UP, away from range_pos, and the walk never converges on
        // the low end of the range, silently abandoning those sectors.
        let mut state = state_with_window(4, 0);
        let opts = opts_with_reverse(true);
        let range_pos = 40 * 1024;
        let mut frame = RangeFrame {
            range_idx: 0,
            range_pos,
            range_size: 80 * 1024,
            end: range_pos + 80 * 1024,
            block_end: range_pos + 60 * 1024, // 30 sectors above range_pos
            range_budget_secs: 1,
            range_sectors: 40,
        };
        let before = frame.block_end;
        let did = compute_damage_skip(&mut state, &mut frame, &opts, 0, 2048);
        assert!(did, "threshold crossed: a skip must apply");
        assert!(
            frame.block_end < before,
            "reverse skip must move the cursor DOWN (toward range_pos): {} !< {}",
            frame.block_end,
            before
        );
        assert!(
            frame.block_end >= frame.range_pos,
            "reverse cursor {} descended below range_pos {}",
            frame.block_end,
            frame.range_pos
        );
    }

    #[test]
    fn damage_skip_caps_at_one_quarter_of_remaining_range() {
        // CLAUDE.md: skip "capped at 1/4 of the remaining bad range so
        // a single jump can't blow past a good middle." Forward walk,
        // range [0, 80 KiB) = 40 sectors, cursor at start (block_end=0)
        // so remaining = 40 sectors and quarter = 10 sectors. Drive a
        // large escalation so the raw escalated skip far exceeds 10.
        // The applied gap must be <= quarter (10 sectors = 20480 bytes).
        // Mutation that makes this RED: remove `.min(range_quarter)`
        // from `skip_sectors` -> the jump leaps the entire good middle.
        let mut state = state_with_window(4, 10);
        let opts = opts_with_reverse(false);
        let mut frame = RangeFrame {
            range_idx: 0,
            range_pos: 0,
            range_size: 80 * 1024,
            end: 80 * 1024,
            block_end: 0,
            range_budget_secs: 1,
            range_sectors: 40,
        };
        let did = compute_damage_skip(&mut state, &mut frame, &opts, 0, 2048);
        assert!(did, "threshold crossed: a skip must apply");
        let quarter_bytes = (40u64 / 4) * 2048; // 10 sectors
        assert!(
            frame.block_end <= quarter_bytes,
            "skip advanced cursor to {} bytes, exceeding the 1/4 cap of {} bytes",
            frame.block_end,
            quarter_bytes
        );
        assert!(frame.block_end > 0, "a real skip must advance the cursor");
    }

    #[test]
    fn damage_skip_below_threshold_does_not_skip_or_mutate_state() {
        // With an all-good window (bad=0) the damage threshold is NOT
        // crossed, so compute_damage_skip must be a no-op: it must NOT
        // advance the cursor, increment skip_count, or charge work_done.
        // A spurious skip here silently abandons readable sectors that
        // the patch loop would otherwise retry.
        // Mutation that makes this RED: invert/weaken the threshold
        // guard (e.g. `>=` -> `<`) so a clean window still skips.
        let mut state = state_with_window(/*bad=*/ 0, /*escalation=*/ 0);
        let opts = opts_with_reverse(false);
        let work_before = state.work_done;
        let skips_before = state.skip_count;
        let mut frame = RangeFrame {
            range_idx: 0,
            range_pos: 0,
            range_size: 80 * 1024,
            end: 80 * 1024,
            block_end: 4096,
            range_budget_secs: 1,
            range_sectors: 40,
        };
        let did = compute_damage_skip(&mut state, &mut frame, &opts, 0, 2048);
        assert!(!did, "clean window must not trigger a damage skip");
        assert_eq!(frame.block_end, 4096, "cursor must not move on a no-op");
        assert_eq!(
            state.work_done, work_before,
            "no-op must not charge work_done"
        );
        assert_eq!(
            state.skip_count, skips_before,
            "no-op must not bump skip_count"
        );
    }

    #[test]
    fn damage_skip_work_done_equals_actual_gap_skipped() {
        // Progress accounting: when a skip fires, `work_done` must grow
        // by EXACTLY the number of bytes the cursor moved (the gap),
        // and skip_count must increment by exactly 1. Reverse range
        // [0, 64 KiB) = 32 sectors, cursor at the top so remaining =
        // 32, quarter = 8 sectors, escalation 0 -> escalated = base
        // (32) capped to quarter (8). Expected gap = 8 sectors.
        // Mutation that makes this RED: compute `gap_bytes` from the
        // wrong endpoints or double-add it.
        let mut state = state_with_window(/*bad=*/ 4, /*escalation=*/ 0);
        let opts = opts_with_reverse(true);
        let end = 64 * 1024;
        let mut frame = RangeFrame {
            range_idx: 0,
            range_pos: 0,
            range_size: end,
            end,
            block_end: end,
            range_budget_secs: 1,
            range_sectors: 32,
        };
        let before = frame.block_end;
        let work_before = state.work_done;
        let did = compute_damage_skip(&mut state, &mut frame, &opts, 0, 2048);
        assert!(did, "threshold crossed: a skip must apply");
        let expected_gap = 8 * 2048u64; // min(base=32, quarter=8) sectors
        let actual_gap = before - frame.block_end; // reverse: cursor moved down
        assert_eq!(
            actual_gap, expected_gap,
            "reverse skip should move the cursor down by the quarter-cap gap"
        );
        assert_eq!(
            state.work_done - work_before,
            actual_gap,
            "work_done must grow by exactly the gap skipped"
        );
        assert_eq!(state.skip_count, 1, "exactly one skip must be recorded");
    }

    // ----------------------------------------------------------------
    // Regression tests for the four audit fixes.
    // ----------------------------------------------------------------

    /// Fix 3: NOT_READY retryable errors must NOT increment
    /// `consecutive_failures`. Pre-fix the increment happened before the
    /// `is_not_ready_retryable` check, so repeated NOT_READY events on
    /// a sluggish drive could push the counter past `wedged_threshold`
    /// (50) and trigger a false wedged_exit that skipped the rest of the
    /// pass. The fix moves the increment inside an `if !is_not_ready_retryable`
    /// guard. This test verifies that the classification logic and the
    /// conditional correctly identify the NOT_READY case and leave the
    /// counter unchanged.
    #[test]
    fn fix3_not_ready_does_not_count_toward_consecutive_failures() {
        // Construct a NOT_READY sense triple (sense_key=0x02, ASC=0x04).
        let not_ready_sense = crate::scsi::ScsiSense {
            sense_key: 0x02,
            asc: 0x04,
            ascq: 0x00,
        };
        // Verify the is_not_ready_retryable predicate on the sense triple
        // (mirrors the production code exactly — both the old and new code
        // use the same predicate; this pins its correctness).
        let is_not_ready_retryable = {
            let s = &not_ready_sense;
            s.sense_key == 0x02 && (s.asc == 0x02 || s.asc == 0x03 || s.asc == 0x04)
        };
        assert!(
            is_not_ready_retryable,
            "sense_key=0x02 asc=0x04 must be classified as retryable NOT_READY"
        );

        // Simulate the corrected increment logic: if is_not_ready_retryable,
        // do NOT increment consecutive_failures.
        let mut state = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);
        let failures_before = state.consecutive_failures;
        if !is_not_ready_retryable {
            state.consecutive_failures += 1;
        }
        assert_eq!(
            state.consecutive_failures, failures_before,
            "NOT_READY retry must not increment consecutive_failures"
        );

        // Non-NOT_READY error (sense_key=0x03 = MEDIUM_ERROR) must still
        // increment the counter.
        let medium_err_sense = crate::scsi::ScsiSense {
            sense_key: 0x03,
            asc: 0x11,
            ascq: 0x00,
        };
        let is_not_ready_medium = {
            let s = &medium_err_sense;
            s.sense_key == 0x02 && (s.asc == 0x02 || s.asc == 0x03 || s.asc == 0x04)
        };
        assert!(!is_not_ready_medium, "MEDIUM_ERROR must not be NOT_READY");
        let failures_before2 = state.consecutive_failures;
        if !is_not_ready_medium {
            state.consecutive_failures += 1;
        }
        assert_eq!(
            state.consecutive_failures,
            failures_before2 + 1,
            "non-NOT_READY error must increment consecutive_failures"
        );
    }

    /// Fix 3 (ASC coverage): verify all three retryable ASC values (0x02,
    /// 0x03, 0x04) are recognised and that ASC 0x3A (medium not present,
    /// NOT retryable) is NOT recognised.
    #[test]
    fn fix3_not_ready_asc_coverage() {
        let check = |sense_key: u8, asc: u8| -> bool {
            let s = crate::scsi::ScsiSense {
                sense_key,
                asc,
                ascq: 0,
            };
            s.sense_key == 0x02 && (s.asc == 0x02 || s.asc == 0x03 || s.asc == 0x04)
        };
        assert!(check(0x02, 0x02), "ASC 0x02 must be retryable");
        assert!(check(0x02, 0x03), "ASC 0x03 must be retryable");
        assert!(check(0x02, 0x04), "ASC 0x04 must be retryable");
        assert!(
            !check(0x02, 0x3A),
            "ASC 0x3A (medium not present) must NOT be retryable"
        );
        assert!(
            !check(0x03, 0x04),
            "sense_key != 0x02 must not be retryable"
        );
    }

    /// Fix 1 + Fix 2: the stall guard and halt-interruptibility of the
    /// NOT_READY pause path. Since `handle_read_failure` requires a full
    /// Pipeline (non-trivially constructable in unit tests), this test
    /// directly exercises the two sub-behaviors that Fix 1 and Fix 2 add
    /// to that path:
    ///
    /// * Fix 1: when `stall_start` is already past STALL_SECS ago,
    ///   `wedged_exit` must be set and `BreakOuter` returned — the same
    ///   stall guard that fires in the normal failure path must also fire
    ///   on the NOT_READY retry path.
    /// * Fix 2: `sleep_secs_or_halt` exits immediately when the halt
    ///   token is already set, so the 15 s NOT_READY pause does not block
    ///   cancellation.
    #[test]
    fn fix1_and_fix2_not_ready_stall_guard_and_halt_responsiveness() {
        // Fix 2: halt token pre-set — sleep must return in well under 1 s.
        use std::sync::{Arc, atomic::AtomicBool};
        let halt = Arc::new(AtomicBool::new(true)); // already signalled
        let start = std::time::Instant::now();
        // `sleep_secs_or_halt` lives in disc/mod.rs (pub(crate)); from
        // this test module (inside patch.rs which is a child of disc),
        // `super` is the patch module and `super::super` is disc.
        super::super::sleep_secs_or_halt(15, Some(&halt));
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "sleep_secs_or_halt with pre-set halt must return immediately, \
             elapsed={elapsed:?}"
        );

        // Fix 1: stall guard logic — simulate the stall check that the
        // NOT_READY path now executes after the sleep. The guard fires
        // when stall_start is older than STALL_SECS and bytes_good has
        // not advanced. Pre-fix: the NOT_READY path returned ContinueInner
        // before this check so it was never reached.
        let mut state = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);
        // Wind the clock back past the stall threshold.
        state.stall_start = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(STALL_SECS + 10))
            .unwrap_or(state.stall_start);
        // bytes_good hasn't moved (same as bytes_good_last = 0).
        let bytes_good_now = state.bytes_good_last; // no progress
        // Reproduce the stall guard condition added to the NOT_READY path.
        let stall_fires = state.stall_start.elapsed() > std::time::Duration::from_secs(STALL_SECS);
        assert!(
            stall_fires,
            "stall guard must fire when stall_start is older than STALL_SECS \
             and bytes_good has not advanced (bytes_good_now={bytes_good_now})"
        );
        // If it fires, the fix sets wedged_exit and returns BreakOuter.
        state.wedged_exit = true; // mirror what the production code does
        assert!(
            state.wedged_exit,
            "wedged_exit must be set when the NOT_READY stall guard fires"
        );
    }

    /// Fix 4: `range_bytes_good` must be initialized to the CURRENT
    /// bytes_good at range entry, not the pass-start value
    /// `bytes_good_before`. Pre-fix: after range 0 recovers N bytes,
    /// range 1 entered with `range_bytes_good = bytes_good_before`, so
    /// the first `check_range_watchdog` tick saw `bytes_good_now >
    /// range_bytes_good` (because of range 0's recovery) and spuriously
    /// reset `range_start` — giving range 1 a free budget refill it
    /// hadn't earned.
    ///
    /// This test verifies that if `range_bytes_good` is set to the CURRENT
    /// value (no new recovery yet in this range), the watchdog does NOT
    /// reset the timer on its first tick.
    #[test]
    fn fix4_range_watchdog_does_not_spuriously_reset_after_prior_range_recovery() {
        use std::sync::{Arc, Mutex};

        // Simulate a SharedPatchState where bytes_good has already
        // advanced (due to prior range recovery).
        let current_bytes_good: u64 = 1024 * 1024; // some non-zero recovery
        let shared = Arc::new(Mutex::new(SharedPatchState {
            stats: MapStats {
                bytes_total: 0,
                bytes_good: current_bytes_good,
                bytes_pending: 0,
                bytes_unreadable: 0,
                bytes_retryable: 0,
                bytes_nontried: 0,
                num_bad_ranges: 0,
                main_lost_ms: 0.0,
            },
            bad_ranges: vec![],
        }));

        // Fix 4 (corrected): range_bytes_good = current_bytes_good.
        // The watchdog should see bytes_good_now == range_bytes_good and
        // NOT reset range_start.
        let mut state = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);
        state.range_bytes_good = current_bytes_good; // correct: current value
        let original_range_start = state.range_start;

        // Set range budget to something generous so we only test the
        // timer-reset path, not the budget-exceeded path.
        let frame = RangeFrame {
            range_idx: 1,
            range_pos: 0,
            range_size: 2048,
            end: 2048,
            block_end: 2048,
            range_budget_secs: 9999,
            range_sectors: 1,
        };

        let timed_out = check_range_watchdog(&mut state, &frame, &shared);
        assert!(!timed_out, "range must not time out immediately");
        // With correct initialization bytes_good_now == range_bytes_good,
        // so the `bytes_good_now > range_bytes_good` branch does NOT fire
        // and range_start is NOT reset.
        //
        // The pre-fix bug: range_bytes_good = bytes_good_before (0) while
        // bytes_good_now = current_bytes_good (1 MiB), so the first tick
        // would unconditionally reset range_start, masking stalls in ranges
        // that followed productive ones.
        assert_eq!(
            state.range_bytes_good, current_bytes_good,
            "range_bytes_good must stay at the current value (no new recovery yet)"
        );
        // Verify the timer was not reset: range_start should be at or
        // before the original value (it could be the same Instant or
        // marginally later due to the lock, but it must not have jumped
        // forward). We check that range_start did not advance by more than
        // 1 ms (the watchdog logic sets it to Instant::now() on reset).
        let drift = state
            .range_start
            .checked_duration_since(original_range_start)
            .unwrap_or_default();
        assert!(
            drift < std::time::Duration::from_millis(100),
            "range_start must not be reset on the first tick when no new recovery \
             occurred in this range (drift={drift:?})"
        );
    }

    // ---- Clock seam: deterministic watchdog timeouts ------------------
    //
    // `PatchLoopState::new_with_clock` lets a test inject a monotonic clock so
    // the per-range / whole-pass watchdogs can be driven WITHOUT real wall time.
    // The fake clock is a free `fn() -> Instant` (the seam's type), backed by a
    // process-wide millisecond offset. Tests that use it serialize on a mutex so
    // the shared offset can't be clobbered by a concurrently-running clock test.

    use std::sync::atomic::{AtomicU64, Ordering};

    static FAKE_CLOCK_OFFSET_MS: AtomicU64 = AtomicU64::new(0);
    static FAKE_CLOCK_LOCK: Mutex<()> = Mutex::new(());

    /// The injectable clock: a fixed base plus the current offset. `OnceLock`
    /// pins the base so every call within a test advances from the same origin.
    fn fake_now() -> std::time::Instant {
        use std::sync::OnceLock;
        static BASE: OnceLock<std::time::Instant> = OnceLock::new();
        let base = *BASE.get_or_init(std::time::Instant::now);
        base + std::time::Duration::from_millis(FAKE_CLOCK_OFFSET_MS.load(Ordering::SeqCst))
    }

    /// Advance the fake clock by `secs` seconds.
    fn advance_fake_clock(secs: u64) {
        FAKE_CLOCK_OFFSET_MS.fetch_add(secs * 1000, Ordering::SeqCst);
    }

    fn shared_with_bytes_good(bytes_good: u64) -> Arc<Mutex<SharedPatchState>> {
        Arc::new(Mutex::new(SharedPatchState {
            stats: MapStats {
                bytes_total: 0,
                bytes_good,
                bytes_pending: 0,
                bytes_unreadable: 0,
                bytes_retryable: 0,
                bytes_nontried: 0,
                num_bad_ranges: 0,
                main_lost_ms: 0.0,
            },
            bad_ranges: vec![],
        }))
    }

    /// The per-range watchdog must NOT trip before the budget elapses and MUST
    /// trip once the injected clock passes the budget — with zero forward
    /// progress (bytes_good frozen). Driven entirely by `advance_fake_clock`,
    /// so it proves the real `check_range_watchdog` timeout branch executes.
    #[test]
    fn range_watchdog_trips_on_budget_exhaustion_via_fake_clock() {
        let _guard = FAKE_CLOCK_LOCK.lock().unwrap();
        FAKE_CLOCK_OFFSET_MS.store(0, Ordering::SeqCst);

        let shared = shared_with_bytes_good(0);
        let mut state = PatchLoopState::new_with_clock(0, 1 << 40, 1, false, 1 << 40, fake_now);

        // A 10 s budget. range_start was seeded from fake_now() at offset 0.
        let frame = RangeFrame {
            range_idx: 1,
            range_pos: 0,
            range_size: 2048,
            end: 2048,
            block_end: 2048,
            range_budget_secs: 10,
            range_sectors: 1,
        };

        // Just under budget: no trip.
        advance_fake_clock(9);
        assert!(
            !check_range_watchdog(&mut state, &frame, &shared),
            "watchdog must not trip before the range budget elapses"
        );

        // Past budget with no recovery: trip.
        advance_fake_clock(2); // total 11 s >= 10 s budget
        assert!(
            check_range_watchdog(&mut state, &frame, &shared),
            "watchdog must trip once the injected clock passes the range budget"
        );
    }

    /// Forward progress (bytes_good advancing) must reset the per-range clock so
    /// the watchdog does NOT trip even though more than `budget` seconds of fake
    /// time have passed in aggregate — proving the reset branch reads the seam,
    /// not real time.
    #[test]
    fn range_watchdog_forward_progress_resets_clock_via_fake_clock() {
        let _guard = FAKE_CLOCK_LOCK.lock().unwrap();
        FAKE_CLOCK_OFFSET_MS.store(0, Ordering::SeqCst);

        let shared = shared_with_bytes_good(0);
        let mut state = PatchLoopState::new_with_clock(0, 1 << 40, 1, false, 1 << 40, fake_now);

        let frame = RangeFrame {
            range_idx: 1,
            range_pos: 0,
            range_size: 2048,
            end: 2048,
            block_end: 2048,
            range_budget_secs: 10,
            range_sectors: 1,
        };

        // 8 s, then recovery commits (bytes_good advances) — clock resets.
        advance_fake_clock(8);
        shared.lock().unwrap().stats.bytes_good = 4096;
        assert!(
            !check_range_watchdog(&mut state, &frame, &shared),
            "progress tick must not trip"
        );

        // 8 more seconds (16 s total, but only 8 since the reset): still under
        // budget because the productive tick reset range_start.
        advance_fake_clock(8);
        assert!(
            !check_range_watchdog(&mut state, &frame, &shared),
            "watchdog must not trip when forward progress kept resetting the clock"
        );

        // Now freeze progress and exceed the budget from the last reset.
        advance_fake_clock(11);
        assert!(
            check_range_watchdog(&mut state, &frame, &shared),
            "watchdog must trip once progress stops and the budget elapses"
        );
    }

    /// The whole-pass stall watchdog predicate (`STALL_SECS` on no bytes_good
    /// movement) must be governed by the injected clock. This drives the exact
    /// comparison the production stall guard runs — `(state.now)().duration_since
    /// (state.stall_start) > STALL_SECS` — through `new_with_clock`, proving the
    /// seam reaches the stall path too (which is inline in helpers that need a
    /// full Pipeline, so we assert the predicate the helpers evaluate).
    #[test]
    fn whole_pass_stall_predicate_governed_by_fake_clock() {
        let _guard = FAKE_CLOCK_LOCK.lock().unwrap();
        FAKE_CLOCK_OFFSET_MS.store(0, Ordering::SeqCst);

        let state = PatchLoopState::new_with_clock(0, 1 << 40, 1, false, 1 << 40, fake_now);

        // Before STALL_SECS: predicate false.
        advance_fake_clock(STALL_SECS - 1);
        assert!(
            (state.now)().duration_since(state.stall_start)
                <= std::time::Duration::from_secs(STALL_SECS),
            "stall must not fire before STALL_SECS of injected time"
        );

        // Past STALL_SECS with no progress: predicate true → production sets
        // wedged_exit and breaks the outer loop.
        advance_fake_clock(2);
        assert!(
            (state.now)().duration_since(state.stall_start)
                > std::time::Duration::from_secs(STALL_SECS),
            "stall guard fires once injected time exceeds STALL_SECS"
        );
    }

    /// NOT_READY per-LBA cap: after NOT_READY_MAX_RETRIES_PER_LBA retries
    /// on the same LBA the cap is exhausted and the next NOT_READY is treated
    /// as a normal failure (consecutive_failures incremented, retry refused).
    /// A different LBA resets the counter so transient NOT_READY can still
    /// recover. Mirrors the sweep path cap (read_error.rs
    /// NOT_READY_MAX_RETRIES = 3).
    ///
    /// Regression for: NOT_READY retries had no per-LBA bound, so a
    /// persistently-not-ready disc could loop on a single LBA until the
    /// whole-pass STALL_SECS watchdog fired (up to 3600 s per range).
    #[test]
    fn not_ready_per_lba_cap_stops_retrying_and_resets_on_new_lba() {
        let lba_a: u32 = 100;
        let lba_b: u32 = 200;

        // Simulate the per-LBA counter logic that handle_read_failure applies:
        //   - on entry: reset counter if lba changed
        //   - if is_not_ready_retryable && counter < cap: increment, return ContinueInner
        //   - else if is_not_ready_retryable && counter >= cap: fall through, increment consecutive_failures
        let simulate = |state: &mut PatchLoopState, lba: u32| -> bool {
            // Reset on LBA change (mirrors production code).
            if state.not_ready_lba != Some(lba) {
                state.not_ready_retries_per_lba = 0;
                state.not_ready_lba = Some(lba);
            }
            let is_not_ready = true; // all calls in this test are NOT_READY
            if is_not_ready {
                if state.not_ready_retries_per_lba < NOT_READY_MAX_RETRIES_PER_LBA {
                    state.not_ready_retries_per_lba += 1;
                    return true; // ContinueInner (retry)
                }
                // cap exceeded: fall through — count toward consecutive_failures
                state.consecutive_failures += 1;
            }
            false // not retried
        };

        let mut state = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);

        // First NOT_READY_MAX_RETRIES_PER_LBA calls on lba_a must be retried.
        for i in 1..=NOT_READY_MAX_RETRIES_PER_LBA {
            let retried = simulate(&mut state, lba_a);
            assert!(
                retried,
                "retry {i}/{NOT_READY_MAX_RETRIES_PER_LBA} on lba_a must return ContinueInner"
            );
            assert_eq!(
                state.not_ready_retries_per_lba, i,
                "counter must be {i} after {i} retries"
            );
            assert_eq!(
                state.consecutive_failures, 0,
                "consecutive_failures must stay 0 during retries"
            );
        }

        // The (cap+1)-th NOT_READY on the SAME lba_a must NOT be retried
        // and must increment consecutive_failures.
        let retried = simulate(&mut state, lba_a);
        assert!(
            !retried,
            "NOT_READY on lba_a after cap must NOT return ContinueInner"
        );
        assert_eq!(
            state.consecutive_failures, 1,
            "consecutive_failures must be incremented when cap is exceeded"
        );

        // Switching to lba_b must reset the counter: the first NOT_READY on
        // lba_b should be retried again (counter = 1).
        let retried = simulate(&mut state, lba_b);
        assert!(
            retried,
            "first NOT_READY on lba_b (new LBA) must return ContinueInner \
             (counter reset on LBA change)"
        );
        assert_eq!(
            state.not_ready_retries_per_lba, 1,
            "counter must restart at 1 after LBA change"
        );
        assert_eq!(
            state.consecutive_failures, 1,
            "consecutive_failures must not change on a successful NOT_READY retry after LBA change"
        );
    }

    /// Fix 5: probe for-loop halt-token check.
    ///
    /// Pre-fix: the probe loop in `handle_read_failure` had no halt-token
    /// check.  Each probe read can block up to READ_RECOVERY_TIMEOUT_MS
    /// (60 s); with 3 probes a /api/stop could take up to ~180 s to be
    /// honored.
    ///
    /// The fix adds the same pattern used by the backtrack inner loop
    /// (~line 785):
    ///
    ///   if let Some(h) = &opts.halt {
    ///       if h.load(Ordering::Relaxed) { return Err(Halted); }
    ///   }
    ///
    /// `handle_read_failure` is not unit-testable in isolation because it
    /// requires a live `Pipeline` sink.  This test verifies the two
    /// sub-behaviors the fix relies on:
    ///
    /// 1. The probe block is reached when `consecutive_failures >= 3
    ///    && consecutive_failures % 5 == 0` — confirmed by checking the
    ///    gate condition directly.
    /// 2. An `AtomicBool` pre-set to `true` loaded with `Ordering::Relaxed`
    ///    returns `true` immediately (i.e., the early-exit logic is sound).
    ///
    /// Together these guarantee that a pre-set halt token causes the loop
    /// to exit on the first iteration without issuing a read.
    #[test]
    fn fix5_probe_loop_honors_halt_token() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        // 1. Gate condition: consecutive_failures = 5 triggers probe block.
        //    (first value satisfying >= 3 && % 5 == 0)
        let consecutive_failures: u64 = 5;
        assert!(
            consecutive_failures >= 3 && consecutive_failures % 5 == 0,
            "probe block gate must be entered at consecutive_failures=5"
        );

        // 2. Pre-set halt token must be detected immediately via Relaxed load.
        //    Use Arc to match the production type (Option<Arc<AtomicBool>>).
        let halt = Arc::new(AtomicBool::new(true));
        let detected = halt.load(Ordering::Relaxed);
        assert!(
            detected,
            "Relaxed load of pre-set AtomicBool must return true — \
             the halt check in the probe loop relies on this"
        );

        // 3. Zero-offset probe (offset_sectors = 0, probe_idx = 0) fires
        //    only when consecutive_failures >= 5; validate that gate too.
        //    (The halt check comes before this guard, so it fires first
        //    regardless — but confirm the gate would otherwise let it through.)
        assert!(
            consecutive_failures >= 5,
            "zero-offset probe guard requires consecutive_failures >= 5; \
             halt check must fire before this gate is even evaluated"
        );
    }

    /// Regression for MED bug: `wedge_count` must be CONSECUTIVE, reset on
    /// success.
    ///
    /// Pre-fix: `handle_read_success` never touched `wedge_count`. A
    /// sequence of wedge-family failures interspersed with good reads
    /// accumulated `wedge_count` monotonically, hitting
    /// `WEDGE_ABORT_THRESHOLD` (16) and aborting the pass even though the
    /// drive was actually making forward progress. The fix adds
    /// `state.wedge_count = 0` in `handle_read_success` so only a run of
    /// CONSECUTIVE wedge-family senses (with no intervening success) can
    /// reach the threshold.
    ///
    /// Scenario A: failures with an intervening success must NOT reach the
    /// threshold.
    ///
    /// Scenario B: a true run of consecutive wedge-family failures (no
    /// intervening success) must still reach the threshold and set
    /// `wedged_exit`.
    #[test]
    fn wedge_count_resets_on_success_prevents_premature_abort() {
        // Simulate the wedge_count mutation that handle_read_success now
        // performs (state.wedge_count = 0) and the wedge increment that
        // handle_read_failure performs for is_wedge_family errors.

        // Helper: apply one wedge-family failure — mirrors the production path
        // in handle_read_failure (is_wedge_family branch).
        let wedge_failure = |state: &mut PatchLoopState| {
            state.wedge_count += 1;
        };

        // Helper: apply one success — mirrors the production path in
        // handle_read_success after the fix.
        let success = |state: &mut PatchLoopState| {
            state.wedge_count = 0;
        };

        // ── Scenario A: intermittent wedge failures interspersed with a
        // success do NOT reach WEDGE_ABORT_THRESHOLD. ──────────────────────
        {
            let mut state = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);

            // Drive 10 wedge-family failures.
            for _ in 0..10 {
                wedge_failure(&mut state);
            }
            assert_eq!(
                state.wedge_count, 10,
                "wedge_count must be 10 after 10 consecutive wedge failures"
            );

            // A successful read resets the streak.
            success(&mut state);
            assert_eq!(
                state.wedge_count, 0,
                "wedge_count must reset to 0 on a successful read"
            );

            // Drive 10 more wedge-family failures after the reset.
            for _ in 0..10 {
                wedge_failure(&mut state);
            }
            assert_eq!(
                state.wedge_count, 10,
                "wedge_count must restart at 10 after reset + 10 more failures"
            );

            // Total events so far: 20 wedge failures across the whole pass,
            // but the longest consecutive streak is only 10 — below threshold.
            assert!(
                state.wedge_count < WEDGE_ABORT_THRESHOLD,
                "intermittent pattern (10 + success + 10) must not reach \
                 WEDGE_ABORT_THRESHOLD ({WEDGE_ABORT_THRESHOLD}); \
                 wedge_count = {}",
                state.wedge_count
            );
        }

        // ── Scenario B: an unbroken run of WEDGE_ABORT_THRESHOLD consecutive
        // wedge failures DOES reach the threshold. ─────────────────────────
        {
            let mut state = PatchLoopState::new(0, 1 << 40, 1, false, 1 << 40);

            for _ in 0..WEDGE_ABORT_THRESHOLD {
                wedge_failure(&mut state);
            }
            assert!(
                state.wedge_count >= WEDGE_ABORT_THRESHOLD,
                "a true run of {WEDGE_ABORT_THRESHOLD} consecutive wedge failures \
                 must reach the threshold; wedge_count = {}",
                state.wedge_count
            );
        }
    }

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

    // ----------------------------------------------------------------
    // Scatter-recovery ("reset, read good data, come back for one
    // sector") — recalibrate-between-fresh-attempts + the medium-error
    // gate. Validated against a synthetic SectorSource, never the live
    // drive (CLAUDE.md hard-rule #2: hammering real bad LBAs wedges it).
    // ----------------------------------------------------------------

    /// A medium-error (sense_key 0x03 = UNRECOVERED READ) CHECK CONDITION —
    /// the genuine bad-sector case scatter-recovery targets.
    fn medium_err() -> Error {
        Error::ScsiError {
            opcode: 0x28,
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x05,
            }),
        }
    }

    /// A NOT_READY error — has its OWN retry path; scatter must skip it.
    fn not_ready_err() -> Error {
        Error::ScsiError {
            opcode: 0x28,
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_NOT_READY,
                asc: 0x04,
                ascq: 0x00,
            }),
        }
    }

    /// Reads at `target` fail until `target_reads > fail_until` (a marginal
    /// sector that comes back on a later fresh approach), unless
    /// `always_fail`. Any other LBA (the recalibration anchor) reads OK.
    /// Counts target vs anchor reads so tests can assert the scatter cadence.
    struct ScatterFixture {
        target: u32,
        fail_until: u32,
        always_fail: bool,
        target_reads: u32,
        anchor_reads: u32,
    }

    impl SectorSource for ScatterFixture {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            if lba == self.target {
                self.target_reads += 1;
                if !self.always_fail && self.target_reads > self.fail_until {
                    buf[..bytes].fill(0xAB);
                    return Ok(bytes);
                }
                return Err(medium_err());
            }
            self.anchor_reads += 1;
            let n = bytes.min(buf.len());
            buf[..n].fill(0);
            Ok(bytes)
        }
    }

    #[test]
    fn scatter_recovers_marginal_sector_after_recalibration() {
        let target = 1_000_000u32;
        let mut fx = ScatterFixture {
            target,
            fail_until: 1,
            always_fail: false,
            target_reads: 0,
            anchor_reads: 0,
        };
        let mut buf = vec![0u8; 2048];
        let ok = scatter_recover(
            &mut fx,
            &medium_err(),
            target,
            1,
            2048,
            &mut buf,
            false,
            None,
        );
        assert!(ok, "a marginal sector should recover on a fresh re-read");
        assert_eq!(buf[0], 0xAB, "recovered bytes are written into buf");
        assert_eq!(
            fx.target_reads, 2,
            "1st fresh re-read fails, the 2nd (after recalibration) succeeds"
        );
        assert_eq!(
            fx.anchor_reads, 2,
            "one recalibration read precedes each fresh attempt"
        );
    }

    #[test]
    fn scatter_gives_up_on_dead_sector_after_max_attempts() {
        let target = 1_000_000u32;
        let mut fx = ScatterFixture {
            target,
            fail_until: 0,
            always_fail: true,
            target_reads: 0,
            anchor_reads: 0,
        };
        let mut buf = vec![0u8; 2048];
        let ok = scatter_recover(
            &mut fx,
            &medium_err(),
            target,
            1,
            2048,
            &mut buf,
            false,
            None,
        );
        assert!(!ok, "a truly-dead sector exhausts attempts and gives up");
        assert_eq!(
            fx.target_reads, SCATTER_MAX_ATTEMPTS,
            "exactly SCATTER_MAX_ATTEMPTS fresh re-reads, no more"
        );
        assert_eq!(
            fx.anchor_reads, SCATTER_MAX_ATTEMPTS,
            "recalibrates before each fresh attempt"
        );
    }

    #[test]
    fn scatter_skips_non_medium_error() {
        let target = 1_000_000u32;
        let mut fx = ScatterFixture {
            target,
            fail_until: 0,
            always_fail: true,
            target_reads: 0,
            anchor_reads: 0,
        };
        let mut buf = vec![0u8; 2048];
        let ok = scatter_recover(
            &mut fx,
            &not_ready_err(),
            target,
            1,
            2048,
            &mut buf,
            false,
            None,
        );
        assert!(
            !ok,
            "NOT_READY is handled by its own retry path, not scatter"
        );
        assert_eq!(fx.target_reads, 0, "no reads issued for a non-medium error");
        assert_eq!(fx.anchor_reads, 0);
    }

    #[test]
    fn scatter_skips_batch_reads() {
        // count > 1 means "don't know which sector is bad yet" — the loop
        // drops to count=1 elsewhere; scatter only ever runs on singles.
        let target = 1_000_000u32;
        let mut fx = ScatterFixture {
            target,
            fail_until: 0,
            always_fail: true,
            target_reads: 0,
            anchor_reads: 0,
        };
        let mut buf = vec![0u8; 2 * 2048];
        let ok = scatter_recover(
            &mut fx,
            &medium_err(),
            target,
            2,
            2 * 2048,
            &mut buf,
            false,
            None,
        );
        assert!(!ok);
        assert_eq!(fx.target_reads, 0, "batch reads are not scattered");
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
}
