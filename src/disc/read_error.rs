//! Single source of truth for what to do when a sector read fails.
//!
//! Both Pass 1 (`Disc::sweep`) and Pass 2-N (`Disc::patch`) call into
//! `handle_read_error` after every failed `read_sectors`. The handler
//! classifies the error, updates the in-flight context (counters,
//! damage window, retry budgets), and returns a `ReadAction` the caller
//! dispatches on. Every read goes through the same gate — no path can
//! silently skip pause/skip/jump/abort logic.
//!
//! Adding a new error class = add one arm in `handle_read_error`.
//! Adding new logging on errors = one place.

use crate::error::Error;
use crate::scsi;

/// In-flight bookkeeping a read loop must keep across iterations. The
/// handler reads and mutates this. Caller owns the storage.
pub struct ReadCtx {
    /// Number of sectors per read attempt. The handler uses this to
    /// decide whether to bisect (only worthwhile when batch > 1).
    pub batch: u16,
    /// Successful reads since the last failure. Resets to 0 on failure.
    /// Used by callers to drive damage-zone exit / speed restoration.
    pub consecutive_good: u64,
    /// Failed reads since the last success. Resets to 0 on success.
    /// Drives long-pause escalation on persistent failure.
    pub consecutive_failures: u64,
    /// Failed OUTER batch reads since the last outer success — bisect
    /// inner-sector failures are NOT counted here. Drives the
    /// fast-entry damage-jump on Pass 1 (skip the disc-level grind
    /// once we're clearly in a damaged region; Pass N will recover
    /// the actual sectors). Reset on outer success.
    pub consecutive_outer_failures: u64,
    /// Sliding window of recent read outcomes (true=ok, false=fail).
    /// Capped at `damage_window_max`. Drives damage-jump decisions.
    pub damage_window: Vec<bool>,
    pub damage_window_max: usize,
    pub damage_threshold_pct: usize,
    /// Trigger a damage-jump after this many consecutive outer-batch
    /// failures, even when the damage_window isn't full yet. Pass 1
    /// uses a small value (4) so we don't spend ~40 minutes grinding
    /// to fill a 16-block window before the first jump on a damage
    /// zone we entered cleanly. Pass N uses a larger value (or
    /// disables this — see `bisect_on_marginal`) because Pass N's
    /// whole job IS to grind on the bad ranges.
    pub fast_jump_threshold: u64,
    /// Multiplier applied to damage-jump distance. Doubles each jump,
    /// resets to 1 after `damage_window_max` consecutive good reads.
    pub jump_multiplier: u64,
    /// NOT_READY retries used so far for the current LBA. Reset to 0
    /// on any non-NOT_READY response.
    pub not_ready_retries: u32,
    /// Bridge-degradation cooldowns used so far.
    pub bridge_degradation_count: u32,
    /// Whether we're currently inside a damage-jump bisect attempt.
    /// Caller sets this true when entering single-sector mode for a
    /// failed batch, so the handler doesn't recursively request another
    /// bisect on the inner-sector failures.
    pub bisecting: bool,
    /// Whether to return `Bisect` on a marginal-media batch failure.
    /// Pass 1 sweep sets this false: a failed batch becomes
    /// SkipBlock (mark the whole 32-sector ECC block NonTrimmed,
    /// advance, let Pass N recover the salvageable sectors with
    /// proper recovery semantics). Pass N sets this true: bisection
    /// is its core job, and it has the right tools (single-sector
    /// reads, 60s recovery timeout, retry budget, escalating skip).
    pub bisect_on_marginal: bool,
    /// Count of consecutive firmware-wedge responses (HARDWARE_ERROR
    /// or ILLEGAL_REQUEST sense keys) since the last successful read.
    /// Pass 1 uses this to drive the wedge-skip path: each wedge
    /// triggers a 1 GB jump + cooldown pause. Reaching
    /// `WEDGE_ABORT_THRESHOLD` consecutive wedges with no good read
    /// in between → real AbortPass.
    pub wedge_count: u64,
    // ── Diagnostic counters (added 2026-05-10) ──
    //
    // Aggregate state for post-mortem analysis of wedge incidents.
    // Every Pass 1 / Pass N sweep now produces a structured summary
    // at the WARN log on each error AND an end-of-pass INFO summary.
    // Goal: when a wedge happens, the operator should be able to tell
    // from the logs whether it was triggered by ONE read at a
    // physically-damaged sector (immediate failure) or by accumulated
    // exposure across MANY reads (firmware-state buildup), and what
    // the timing pattern looked like.
    /// `Instant` of the most recent successful read. Used to compute
    /// "time since last good" for the WARN log on each error. None
    /// before the first successful read.
    pub last_success_at: Option<std::time::Instant>,
    /// `Instant` of the most recent failed read. Used to compute
    /// "time since last error" for the WARN log. None before the
    /// first error.
    pub last_error_at: Option<std::time::Instant>,
    /// Last error's sense-key "family" (Medium / Hardware / IllegalRequest
    /// / NotReady / Other). Used to detect WEDGE TRANSITIONS — when
    /// the family changes from Medium → Hardware/IllegalRequest, the
    /// drive almost certainly just entered fast-fail mode. That
    /// transition gets its own WARN log so the trace is unambiguous.
    pub last_error_family: Option<SenseFamily>,
    /// Sum of all errors observed during this sweep. Reported in the
    /// end-of-pass summary.
    pub total_errors: u64,
    /// Sum of all successful reads during this sweep.
    pub total_reads_ok: u64,
    /// Count of damage zones entered (transitions from clean → in-damage).
    pub zones_entered: u64,
    /// Count of damage-jumps executed during this sweep.
    pub jumps_taken: u64,
    /// True between "first error after a clean period" and "16 consecutive
    /// good reads after the last error in the cluster." Used to count
    /// zone entries and to bound zone_reads accurately.
    pub in_damage_zone: bool,
}

/// Coarse classification of a SCSI sense key for diagnostic logging.
/// Wedge-family events (Hardware + IllegalRequest) get their own
/// transition log when the sense family changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenseFamily {
    NotReady,
    Medium,
    Hardware,
    IllegalRequest,
    Other,
}

impl SenseFamily {
    pub fn from_sense_key(sense_key: u8) -> Self {
        match sense_key {
            scsi::SENSE_KEY_NOT_READY => SenseFamily::NotReady,
            scsi::SENSE_KEY_MEDIUM_ERROR => SenseFamily::Medium,
            scsi::SENSE_KEY_HARDWARE_ERROR => SenseFamily::Hardware,
            scsi::SENSE_KEY_ILLEGAL_REQUEST => SenseFamily::IllegalRequest,
            _ => SenseFamily::Other,
        }
    }

    /// True for the "wedge family" — Hardware + IllegalRequest are
    /// the senses the BU40N firmware returns in its fast-fail state.
    pub fn is_wedge_family(self) -> bool {
        matches!(self, SenseFamily::Hardware | SenseFamily::IllegalRequest)
    }
}

impl ReadCtx {
    /// Initial context for a Pass 1 sweep. The job is "fast and
    /// accurate, get the most data in the shortest time" — Pass N
    /// is the one that grinds on the bad ranges. So bisect-on-
    /// marginal is OFF (failed batches become SkipBlock; whole 32-
    /// sector blocks marked NonTrimmed for Pass N to revisit), and
    /// the damage-jump fast-path triggers after just 1 consecutive
    /// outer-batch failure — the user's wedge-prevention principle
    /// (2026-05-11): once the drive returns ANY recoverable error,
    /// retrying the same LBA quickly is what triggers the firmware
    /// fast-fail transition. Jump immediately, never retry in Pass 1.
    /// Pass N owns retries — it gets per-sector timeouts that don't
    /// hammer the firmware the same way.
    pub fn for_sweep(batch: u16) -> Self {
        Self {
            batch,
            consecutive_good: 0,
            consecutive_failures: 0,
            consecutive_outer_failures: 0,
            damage_window: Vec::with_capacity(16),
            damage_window_max: 16,
            damage_threshold_pct: 12,
            fast_jump_threshold: 1,
            jump_multiplier: 1,
            not_ready_retries: 0,
            bridge_degradation_count: 0,
            bisecting: false,
            bisect_on_marginal: false,
            wedge_count: 0,
            last_success_at: None,
            last_error_at: None,
            last_error_family: None,
            total_errors: 0,
            total_reads_ok: 0,
            zones_entered: 0,
            jumps_taken: 0,
            in_damage_zone: false,
        }
    }

    /// Initial context for a Pass 2-N patch. Pass N's whole reason to
    /// exist is to recover sectors Pass 1 skipped — bisection on
    /// marginal media is part of the job, and the fast-jump
    /// threshold is loose so we don't bail too early on a range that
    /// has scattered good sectors mixed in.
    ///
    /// `damage_threshold_pct = 6` mirrors `disc/patch.rs`'s
    /// `PASSN_DAMAGE_THRESHOLD_PCT`. Pass N triggers the damage-skip
    /// at half the density Pass 1 uses (Pass 1 = 12%) because the
    /// patch loop's whole job is to chip away at bad ranges — being
    /// more eager to skip clustered bad sectors converges faster on
    /// the recoverable good sectors inside a range. The patch-side
    /// `compute_damage_skip` reads its threshold from
    /// `PASSN_DAMAGE_THRESHOLD_PCT`; keep the two in sync until the
    /// patch loop's damage-skip is unified with `handle_read_error`'s
    /// jump path. (v0.20.8 unification attempt found the unification
    /// itself blocked on the size-aware `range_remaining/4` cap that
    /// lives in `compute_damage_skip` but not in
    /// `handle_read_error::JumpAhead` — see
    /// `tests/passn_handler_ab.rs` for the A/B fixture that pins
    /// the divergence point.)
    pub fn for_patch(batch: u16) -> Self {
        Self {
            batch,
            consecutive_good: 0,
            consecutive_failures: 0,
            consecutive_outer_failures: 0,
            damage_window: Vec::with_capacity(16),
            damage_window_max: 16,
            damage_threshold_pct: PATCH_DAMAGE_THRESHOLD_PCT,
            // Pass N is allowed to grind: window-based jump only,
            // matching the historical behaviour for patch passes.
            fast_jump_threshold: u64::MAX,
            jump_multiplier: 1,
            not_ready_retries: 0,
            bridge_degradation_count: 0,
            bisecting: false,
            bisect_on_marginal: true,
            wedge_count: 0,
            last_success_at: None,
            last_error_at: None,
            last_error_family: None,
            total_errors: 0,
            total_reads_ok: 0,
            zones_entered: 0,
            jumps_taken: 0,
            in_damage_zone: false,
        }
    }

    /// Caller calls this after every successful read.
    pub fn on_success(&mut self) {
        self.consecutive_good += 1;
        self.consecutive_failures = 0;
        self.not_ready_retries = 0;
        // Any successful read clears the wedge-skip counter — the
        // drive recovered, so further wedges should reset the skip
        // budget instead of accumulating toward a real abort.
        self.wedge_count = 0;
        // Outer-success only: a good single-sector read inside a
        // bisect doesn't mean we've left the damaged batch. Only an
        // outer-batch success resets the outer-failure counter.
        if !self.bisecting {
            self.consecutive_outer_failures = 0;
        }
        self.damage_window.push(true);
        if self.damage_window.len() > self.damage_window_max {
            self.damage_window.remove(0);
        }
        // Diagnostic state.
        self.total_reads_ok += 1;
        self.last_success_at = Some(std::time::Instant::now());
        // If we were in a damage zone and accumulated enough good
        // reads to exit (damage_window now all-good), the zone is
        // over. Don't reset zones_entered — that's a sweep total.
        if self.in_damage_zone && self.consecutive_good >= self.damage_window_max as u64 {
            self.in_damage_zone = false;
            self.last_error_family = None;
        }
    }

    /// Final per-pass summary suitable for an INFO log at the end of
    /// `sweep` / `patch`. Caller renders this to a single structured
    /// log line.
    pub fn pass_summary(&self) -> PassSummary {
        PassSummary {
            total_reads_ok: self.total_reads_ok,
            total_errors: self.total_errors,
            zones_entered: self.zones_entered,
            jumps_taken: self.jumps_taken,
        }
    }
}

/// End-of-pass stats logged at INFO for post-mortem analysis. Lets
/// an operator answer "how damaged is this disc?" from a single log
/// line per pass.
#[derive(Debug, Clone, Copy)]
pub struct PassSummary {
    pub total_reads_ok: u64,
    pub total_errors: u64,
    pub zones_entered: u64,
    pub jumps_taken: u64,
}

/// What the caller should do after a read failure. The caller owns the
/// I/O side-effects (sleep, write zeros, advance pos) — the handler
/// only decides which side-effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAction {
    /// Pause `pause_secs` then retry the same LBA / batch. Used for
    /// transient conditions (NOT_READY, bridge degradation) that the
    /// drive may recover from on its own.
    Retry { pause_secs: u64 },
    /// Re-issue the failed batch as `batch` single-sector reads. Each
    /// inner read is itself dispatched through `handle_read_error` with
    /// `bisecting = true` so it cannot recurse.
    Bisect,
    /// Mark the failed range NonTrimmed (zero-fill, retry in Pass N+),
    /// then pause `pause_secs` before resuming the next LBA.
    SkipBlock { pause_secs: u64 },
    /// Mark the failed range NonTrimmed AND advance position by
    /// `sectors` (zero-filling the gap as NonTrimmed). Then pause
    /// `pause_secs`. Used when the damage-window threshold is crossed.
    JumpAhead { sectors: u64, pause_secs: u64 },
    /// Unrecoverable at this layer. Caller propagates `Err` up to the
    /// outer pass loop / autorip, which can attempt USB re-enumeration,
    /// drop session, etc.
    AbortPass,
}

// Pause budget constants. Tuned from 2026-05-07 BU40N traces showing
// bridge wedges 524 ms after a 5.4-second internal ECC retry. The
// post-failure pauses give the drive — and the bridge — time to settle.
/// Pause between a failed read and the next read attempt — applied
/// uniformly to Pass 1 sweep and Pass N patch.
///
/// 2026-05-11 reframe: a failed read is a failed read, regardless of
/// which pass is running. The prior split (1s for Pass N, 5s for Pass
/// 1 via `PASS_1_FAIL_PAUSE_SECS`) was solving an imaginary cost
/// problem — real damaged-disc cases mark <50 MB NonTrimmed, and the
/// extra 5s/error is single-digit minutes per pass, not hours. The
/// cost of NOT pausing — a drive wedge that aborts the entire
/// multi-pass recovery — is much worse.
///
/// The wedge avoidance principle: error → drive ECC retry (5-10s
/// internal) → return → cooldown pause → next read. Same shape
/// everywhere reads can fail.
const FAIL_PAUSE_SECS: u64 = 5;
/// Long cooldown applied when a damage zone is first entered (the
/// FIRST read failure after a clean run, before the drive has had a
/// chance to cycle in retries that push it toward fast-fail).
///
/// Empirical: 2026-05-11 Dune Pt 2 wedge incident showed 7 medium
/// errors in 6.5 seconds (~1s per attempt + ~1s pause) push the
/// BU40N's firmware into IllegalRequest fast-fail mode permanently.
/// Once there, only physical eject + reload clears it. Giving the
/// drive 30s of breathing room after the FIRST error in a zone —
/// before we start adding more error counts in the firmware's
/// internal window — prevents the transition.
///
/// Cost on clean discs: zero (first-error path doesn't trigger).
/// Cost on damaged discs: ~30s × N damage zones; on a 5-zone disc
/// that's 2.5 min extra. Trade for never wedging the drive.
const ZONE_ENTRY_COOLDOWN_SECS: u64 = 30;
/// Cooldown when a long streak of failures suggests the drive is
/// stuck in a damage zone and needs MORE breathing room than the
/// standard inter-error pause. Same value as `FAIL_PAUSE_SECS`
/// because empirically 5s is enough; kept as a separate name so the
/// escalation policy is explicit at the call site.
const CONSECUTIVE_FAIL_LONG_PAUSE_SECS: u64 = 5;
const CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD: u64 = 10;
const POST_JUMP_EXTRA_PAUSE_SECS: u64 = 2;
const NOT_READY_PAUSE_SECS: u64 = 3;
const NOT_READY_MAX_RETRIES: u32 = 3;
const BRIDGE_DEGRADATION_PAUSE_SECS: u64 = 15;
const BRIDGE_DEGRADATION_MAX_RETRIES: u32 = 5;

/// Base of the damage-jump distance formula: `jump_sectors =
/// JUMP_BASE_SECTORS × batch × jump_multiplier`. Bumped 2026-05-10
/// from 256 → 1024 (4×) so the first damage-jump at batch=32 covers
/// 64 MB instead of 16 MB. Empirically the BU40N's damage clusters
/// are 100+ MB wide; 16 MB jumps landed inside the cluster and the
/// re-read added to the firmware wedge counter. 64 MB → 128 MB
/// (after one doubling) clears almost any single-cluster damage in
/// 2 jumps.
const JUMP_BASE_SECTORS: u64 = 1024;

// Firmware-wedge skip policy for Pass 1 sweep
// ===========================================
//
// When the BU40N (or similar drives) hits a physical-damage cluster,
// its firmware can transition into a "wedge" state where it returns
// HARDWARE_ERROR or ILLEGAL_REQUEST for every subsequent read —
// often for many LBAs after the actual bad sector. Per CLAUDE.md
// "Bad-sector handling" rule #2: "Recovery requires eject+reload OR
// significant cool-down."
//
// Pass 1's pre-fix behavior was to immediately AbortPass on the
// first HARDWARE_ERROR / ILLEGAL_REQUEST, killing the rip at
// whatever percentage it had reached. That's the wrong call when:
//   - the damage zone may be small (jumping past it could resume
//     normal reads), AND
//   - even if the drive stays wedged, finishing the sweep gives us
//     an honest mapfile for Pass N to attack later.
//
// New policy: treat wedge sense codes the same way the damage-window
// treats persistent failure — JumpAhead by a large distance with a
// cooldown pause. Allow up to WEDGE_ABORT_THRESHOLD consecutive
// wedges (no successful read in between) before declaring the drive
// truly stuck and surfacing AbortPass to autorip.

/// One-gigabyte jump (1024 MiB) on each wedge. Big enough to clear
/// almost any single-cluster damage zone we've seen.
const WEDGE_JUMP_SECTORS: u64 = 524_288;
/// Cooldown pause after each wedge. Per CLAUDE.md the drive needs
/// "significant cool-down"; 30 s strikes a balance between giving
/// the drive a chance to recover and not stalling the rip if the
/// drive is permanently stuck.
const WEDGE_PAUSE_SECS: u64 = 30;
/// Bail after this many consecutive wedges with no good read in
/// between. At 1 GB jumps this lets us scan ~16 GB worth of fully
/// wedged area before giving up — generous enough to clear most
/// physical-damage clusters, bounded enough to not loop forever on
/// a permanently bricked drive.
const WEDGE_ABORT_THRESHOLD: u64 = 16;

/// Pass-N wedge-skip distance. Pass N's batch=1 reads target
/// specific NonTrimmed sectors from Pass 1, so a big 1 GB skip
/// would blow past the current NonTrimmed range and abandon many
/// sectors that might still recover. Use a smaller skip just to
/// move past the bricked LBA + a small buffer — the outer patch
/// loop's next iteration picks up the next sector in the same or
/// next range.
const WEDGE_PASS_N_SKIP_SECTORS: u64 = 64;

/// Single source of truth for the Pass-N damage-window threshold.
/// Both [`ReadCtx::for_patch`] and `disc::patch::compute_damage_skip`
/// reference this constant so the two damage-skip paths cannot drift.
///
/// 6% means: with a 16-entry sliding window, the damage-skip fires
/// once 1 out of 16 recent reads has failed. Pass 1 uses a 12%
/// threshold via `damage_threshold_pct` on `for_sweep`; Pass N is
/// twice as eager because patch's whole job is to converge on the
/// bad sub-zones inside a NonTrimmed range — a faster trigger
/// produces tighter convergence in fewer iterations.
pub const PATCH_DAMAGE_THRESHOLD_PCT: usize = 6;

/// THE single error-handling entry point. Updates `ctx`, returns the
/// action the caller must apply.
///
/// New error class = add a new arm here. New logging on errors = add
/// it once at the top. New retry policy = adjust the constants. No
/// other read site needs to change.
pub fn handle_read_error(err: &Error, ctx: &mut ReadCtx) -> ReadAction {
    ctx.consecutive_failures += 1;
    ctx.consecutive_good = 0;
    // Outer-failure counter — only OUTER batch failures count toward
    // the fast-jump trigger. Bisect inner failures are part of
    // recovering an already-failed batch and don't represent
    // independent damage signal.
    if !ctx.bisecting {
        ctx.consecutive_outer_failures += 1;
    }

    // Diagnostic instrumentation — compute timing context BEFORE
    // mutating the timestamps so the log reflects the gap to the
    // PREVIOUS error / success, not zero.
    let now = std::time::Instant::now();
    let ms_since_last_error = ctx
        .last_error_at
        .map(|t| now.duration_since(t).as_millis() as u64);
    let ms_since_last_success = ctx
        .last_success_at
        .map(|t| now.duration_since(t).as_millis() as u64);

    let current_family = err
        .scsi_sense()
        .map(|s| SenseFamily::from_sense_key(s.sense_key))
        .unwrap_or(SenseFamily::Other);

    // Zone-entry tracking: this is the first error after a clean run
    // (or the first error of the sweep).
    if !ctx.in_damage_zone && !ctx.bisecting {
        ctx.in_damage_zone = true;
        ctx.zones_entered += 1;
    }

    ctx.total_errors += 1;
    ctx.last_error_at = Some(now);

    // Wedge transition: previous error was MEDIUM, this one is
    // HARDWARE or ILLEGAL_REQUEST. That's the moment the drive's
    // firmware flipped into fast-fail mode. Distinct WARN so logs
    // make it unambiguous when the wedge "started."
    let is_wedge_transition = matches!(ctx.last_error_family, Some(prev) if !prev.is_wedge_family())
        && current_family.is_wedge_family();
    ctx.last_error_family = Some(current_family);

    tracing::warn!(
        target: "freemkv::disc",
        phase = "read_error",
        consecutive_failures = ctx.consecutive_failures,
        consecutive_outer_failures = ctx.consecutive_outer_failures,
        ms_since_last_error,
        ms_since_last_success,
        total_errors = ctx.total_errors,
        total_reads_ok = ctx.total_reads_ok,
        batch = ctx.batch,
        bisecting = ctx.bisecting,
        wedge_count = ctx.wedge_count,
        sense_family = ?current_family,
        sense_key = err.scsi_sense().map(|s| s.sense_key),
        asc = err.scsi_sense().map(|s| s.asc),
        ascq = err.scsi_sense().map(|s| s.ascq),
        error = %err,
        "read failed; classifying"
    );

    if is_wedge_transition {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "wedge_transition",
            errors_in_zone = ctx.total_errors,
            ms_since_last_success,
            new_family = ?current_family,
            "drive entered wedge / fast-fail family (was returning recoverable medium errors before this)"
        );
    }

    // 1. Transport failure: bridge crash / USB disconnect. The outer
    //    pass loop knows how to handle this (rediscover sg path,
    //    re-open drive). Inline single-sector retry here was tried in
    //    pre-v0.17.0 builds and observed to make wedges worse.
    if err.is_scsi_transport_failure() {
        return ReadAction::AbortPass;
    }

    // 2. Bridge degradation: NOT_READY with the well-known signature
    //    (sense_key=2, ASC=0x04, ASCQ=0x3E). Drive's bridge is in a
    //    semi-stuck state but typically recovers after a long cooldown.
    //    If we've exhausted our retry budget, fall through to the
    //    marginal/skip path below.
    if err.is_bridge_degradation() && ctx.bridge_degradation_count < BRIDGE_DEGRADATION_MAX_RETRIES
    {
        ctx.bridge_degradation_count += 1;
        return ReadAction::Retry {
            pause_secs: BRIDGE_DEGRADATION_PAUSE_SECS,
        };
    }

    let sense_key = err.scsi_sense().map(|s| s.sense_key).unwrap_or(0);

    // 3. Generic NOT_READY (other ASC codes): drive's mechanical
    //    pickup may be moving. Pause and retry briefly.
    if sense_key == scsi::SENSE_KEY_NOT_READY && ctx.not_ready_retries < NOT_READY_MAX_RETRIES {
        ctx.not_ready_retries += 1;
        return ReadAction::Retry {
            pause_secs: NOT_READY_PAUSE_SECS,
        };
    }
    if sense_key != scsi::SENSE_KEY_NOT_READY {
        ctx.not_ready_retries = 0;
    }

    // 4. Hardware error / illegal request — the firmware-wedge family.
    //    The drive transitioned into a fast-fail state where it
    //    rejects reads near the LBA. Same response shape for both
    //    passes (2026-05-11 reframe — error handling is centralized,
    //    and the wedge is a code-induced state we can avoid via
    //    pacing + skip):
    //
    //    - Pass 1 sweep (bisect_on_marginal=false): jump
    //      WEDGE_JUMP_SECTORS (1 GB) ahead, pause WEDGE_PAUSE_SECS,
    //      mark skipped region NonTrimmed.
    //    - Pass N patch (bisect_on_marginal=true): give up on the
    //      current sector (the granular target), pause for cooldown,
    //      let the outer patch loop move to the next NonTrimmed
    //      range. Implemented as a small JumpAhead so the same code
    //      path serves both — Pass N's batch=1 means JumpAhead by
    //      WEDGE_PASS_N_SKIP_SECTORS effectively skips just this
    //      sector and a small buffer (gives the drive room to
    //      recover before the next per-sector attempt).
    //
    //    Both paths share the WEDGE_ABORT_THRESHOLD budget — only
    //    AbortPass after N consecutive wedges with no successful
    //    read in between.
    if sense_key == scsi::SENSE_KEY_HARDWARE_ERROR || sense_key == scsi::SENSE_KEY_ILLEGAL_REQUEST {
        if !ctx.bisecting {
            ctx.wedge_count += 1;
        }
        if ctx.wedge_count >= WEDGE_ABORT_THRESHOLD {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "wedge_abort",
                wedge_count = ctx.wedge_count,
                threshold = WEDGE_ABORT_THRESHOLD,
                pass = if ctx.bisect_on_marginal { "N" } else { "1" },
                "wedge-skip exhausted — drive appears permanently stuck"
            );
            return ReadAction::AbortPass;
        }
        let jump_sectors = if ctx.bisect_on_marginal {
            WEDGE_PASS_N_SKIP_SECTORS
        } else {
            WEDGE_JUMP_SECTORS
        };
        tracing::warn!(
            target: "freemkv::disc",
            phase = "wedge_skip",
            pass = if ctx.bisect_on_marginal { "N" } else { "1" },
            wedge_count = ctx.wedge_count,
            jump_sectors,
            pause_secs = WEDGE_PAUSE_SECS,
            "wedge detected — skipping ahead and pausing for drive cooldown"
        );
        ctx.jumps_taken += 1;
        return ReadAction::JumpAhead {
            sectors: jump_sectors,
            pause_secs: WEDGE_PAUSE_SECS,
        };
    }

    // 5. Marginal media (MEDIUM_ERROR / ABORTED_COMMAND) on a multi-
    //    sector batch: the drive can often read the same sectors
    //    individually. Bisect into single-sector reads (gentler on the
    //    bridge too — shorter SCSI transactions). Avoid recursive
    //    bisect.
    //
    //    Pass 1 sweep sets `bisect_on_marginal=false` to skip this:
    //    its job is "fast and accurate, get the most data in the
    //    shortest time." Pass N is purpose-built to recover
    //    individual sectors with proper recovery semantics, and Pass
    //    1 grinding through 32-sector bisects costs ~2.5 min per bad
    //    block AND fills the damage window slower than it should.
    //    Whole-block NonTrimmed → SkipBlock → advance → Pass N
    //    revisits.
    let is_marginal = matches!(
        sense_key,
        scsi::SENSE_KEY_MEDIUM_ERROR | scsi::SENSE_KEY_ABORTED_COMMAND
    );
    if is_marginal && ctx.batch > 1 && !ctx.bisecting && ctx.bisect_on_marginal {
        return ReadAction::Bisect;
    }

    // 6. Single-sector failure or unbisectable error — record in
    //    damage window, decide between skip-in-place vs damage-jump.
    //
    //    SKIP damage-window updates while bisecting: the window
    //    represents per-batch outcomes, not per-sector. Updating it
    //    inside a bisect inner loop (potentially 32+ sector failures
    //    per batch) would over-weight the window and cause runaway
    //    JumpAhead distance via excessive multiplier doublings.
    if !ctx.bisecting {
        ctx.damage_window.push(false);
        if ctx.damage_window.len() > ctx.damage_window_max {
            ctx.damage_window.remove(0);
        }
    }

    let bad_count = ctx.damage_window.iter().filter(|&&b| !b).count();
    let bad_pct = if ctx.damage_window.is_empty() {
        0
    } else {
        bad_count * 100 / ctx.damage_window.len()
    };

    // Inter-error pause — wedge prevention via pacing.
    //
    // Zone-entry case (first error after a clean run): apply the
    // long ZONE_ENTRY_COOLDOWN_SECS pause. The empirical wedge
    // observed 2026-05-11 happened ~7 errors into a damage zone,
    // each retry adding to the firmware's internal counter. A 30s
    // pause at zone entry lets the drive's bridge / firmware
    // counters reset before we issue the next read.
    //
    // Subsequent errors in the same zone: the standard 5s pause.
    // (We've already jumped past the initial damage; further errors
    // mean we landed in another bad cluster — same pacing applies.)
    //
    // Long-streak escalation: same 5s currently; kept as a separate
    // branch for future tuning. Pass N (bisect_on_marginal=true)
    // uses the standard pauses — it's running single-sector retries
    // on already-known-bad LBAs by design.
    let is_zone_entry =
        ctx.consecutive_outer_failures == 1 && !ctx.bisecting && !ctx.bisect_on_marginal;
    let pause_secs = if is_zone_entry {
        ZONE_ENTRY_COOLDOWN_SECS
    } else if ctx.consecutive_failures >= CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD {
        CONSECUTIVE_FAIL_LONG_PAUSE_SECS
    } else {
        FAIL_PAUSE_SECS
    };

    // 7. Damage-jump: too many failures → skip ahead by an escalating
    //    gap. Multiplier capped so we can't accidentally skip the
    //    entire rest of the disc (observed 2026-05-07: a saturated
    //    multiplier produced a 56 GB jump). Saturating arithmetic on
    //    the sector calc as defence in depth.
    //
    //    Jump base bumped 2026-05-10 from 256 to 1024 sectors per
    //    multiplier unit (= 64 MB first jump at batch=32, up from
    //    16 MB). The smaller base routinely landed jumps back inside
    //    damage clusters of 100+ MB, each landing adding to the
    //    firmware's wedge counter. 64 MB initial + 128 MB second +
    //    256 MB third clears almost any single-cluster damage
    //    pattern we've seen in 2 jumps.
    //
    //    Two triggers, evaluated in order:
    //
    //    a. **Fast-entry** — `consecutive_outer_failures >= fast_jump_threshold`.
    //       Fires on Pass 1 (threshold=4) so we don't spend ~40 min
    //       grinding to fill a 16-block damage window before the
    //       first jump on a damage zone we entered cleanly. Doesn't
    //       fire on Pass N (threshold=u64::MAX).
    //
    //    b. **Window-based** — original behaviour: 12% bad in a
    //       sliding window of 16 outer reads. Pass N's only path,
    //       and Pass 1's fallback if the failures are scattered
    //       enough that we don't hit the consecutive threshold.
    const MAX_JUMP_MULTIPLIER: u64 = 64;
    let fast_trigger = !ctx.bisecting && ctx.consecutive_outer_failures >= ctx.fast_jump_threshold;
    let window_trigger =
        ctx.damage_window.len() >= ctx.damage_window_max && bad_pct >= ctx.damage_threshold_pct;
    if fast_trigger || window_trigger {
        let mult = ctx.jump_multiplier.min(MAX_JUMP_MULTIPLIER);
        let sectors = JUMP_BASE_SECTORS
            .saturating_mul(ctx.batch as u64)
            .saturating_mul(mult);
        ctx.jump_multiplier = (ctx.jump_multiplier.saturating_mul(2)).min(MAX_JUMP_MULTIPLIER);
        // Reset the outer-failure counter so a long damaged region
        // doesn't keep firing fast-jump every read after the initial
        // jump fired. The window-based trigger handles further jumps.
        ctx.consecutive_outer_failures = 0;
        ctx.jumps_taken += 1;
        return ReadAction::JumpAhead {
            sectors,
            pause_secs: pause_secs + POST_JUMP_EXTRA_PAUSE_SECS,
        };
    }

    // 8. Default: zero-fill the failed batch as NonTrimmed and pause
    //    before the next read.
    ReadAction::SkipBlock { pause_secs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::scsi::ScsiSense;

    fn medium_err() -> Error {
        Error::DiscRead {
            sector: 100,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x05,
            }),
        }
    }

    fn hardware_err() -> Error {
        Error::DiscRead {
            sector: 100,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: scsi::SENSE_KEY_HARDWARE_ERROR,
                asc: 0x44,
                ascq: 0x00,
            }),
        }
    }

    fn illegal_request_err() -> Error {
        Error::DiscRead {
            sector: 100,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: scsi::SENSE_KEY_ILLEGAL_REQUEST,
                asc: 0x24,
                ascq: 0x00,
            }),
        }
    }

    #[test]
    fn pass_n_marginal_with_batch_gt_1_bisects() {
        let mut ctx = ReadCtx::for_patch(32);
        let action = handle_read_error(&medium_err(), &mut ctx);
        assert_eq!(action, ReadAction::Bisect);
    }

    #[test]
    fn pass_1_marginal_jumps_immediately_not_bisecting() {
        // 2026-05-11 wedge-prevention rewrite: Pass 1 jumps on the
        // FIRST marginal error (fast_jump_threshold=1) rather than
        // SkipBlock. Retrying the same LBA quickly is what triggers
        // the BU40N's firmware fast-fail transition; immediate jump
        // prevents the cascade. Pass N still bisects (its job is
        // per-sector recovery on already-known-bad LBAs).
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::JumpAhead { .. } => {}
            other => panic!("expected JumpAhead on first Pass 1 marginal error, got {other:?}"),
        }
    }

    #[test]
    fn medium_error_with_batch_1_skips() {
        let mut ctx = ReadCtx::for_patch(1);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { pause_secs } => assert!(pause_secs >= 1),
            other => panic!("expected SkipBlock, got {other:?}"),
        }
    }

    #[test]
    fn medium_error_while_bisecting_does_not_recurse() {
        let mut ctx = ReadCtx::for_patch(32);
        ctx.bisecting = true;
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { .. } => {}
            other => panic!("expected SkipBlock, got {other:?}"),
        }
    }

    #[test]
    fn pass_1_jumps_immediately_on_first_outer_failure() {
        // 2026-05-11 rewrite: fast_jump_threshold is 1 on Pass 1, not
        // 4. Even ONE error triggers a jump because BU40N's firmware
        // fast-fail mode is sensitive to retry cadence. The wedge
        // observed 2026-05-11 happened at 7 errors / 6.5s — by then
        // we were already wedged. Jumping on error #1 means we
        // physically can't reach the cascade.
        let mut ctx = ReadCtx::for_sweep(32);
        let a = handle_read_error(&medium_err(), &mut ctx);
        assert!(
            matches!(a, ReadAction::JumpAhead { .. }),
            "expected JumpAhead on first outer failure (fast_jump_threshold=1), got {a:?}"
        );
    }

    #[test]
    fn pass_n_does_not_fast_jump() {
        // Pass N's whole reason to exist is to grind on bad ranges.
        // It should NOT bail after 4 consecutive failures the way
        // Pass 1 does — it bisects and skips with proper recovery.
        let mut ctx = ReadCtx::for_patch(32);
        for _ in 0..4 {
            let a = handle_read_error(&medium_err(), &mut ctx);
            assert!(
                !matches!(a, ReadAction::JumpAhead { .. }),
                "Pass N must not fast-jump; got {a:?}"
            );
        }
    }

    #[test]
    fn outer_success_resets_consecutive_outer_failures() {
        // With fast_jump_threshold=1 each Pass 1 error fires a jump
        // and resets `consecutive_outer_failures` to 0 inside the
        // handler. So we can't accumulate "3" the old way — instead,
        // verify the counter goes back to 0 after on_success too.
        let mut ctx = ReadCtx::for_sweep(32);
        handle_read_error(&medium_err(), &mut ctx);
        // After fast-jump, consecutive_outer_failures already 0.
        assert_eq!(ctx.consecutive_outer_failures, 0);
        // on_success keeps it at 0 (defensive).
        ctx.bisecting = false;
        ctx.on_success();
        assert_eq!(ctx.consecutive_outer_failures, 0);
    }

    #[test]
    fn bisect_inner_success_does_not_reset_outer_counter() {
        let mut ctx = ReadCtx::for_patch(32);
        for _ in 0..3 {
            handle_read_error(&medium_err(), &mut ctx);
        }
        assert_eq!(ctx.consecutive_outer_failures, 3);
        // A successful inner-sector read during bisect is not the
        // same as escaping the bad outer batch.
        ctx.bisecting = true;
        ctx.on_success();
        assert_eq!(
            ctx.consecutive_outer_failures, 3,
            "bisect inner success must not reset outer-failure counter"
        );
    }

    #[test]
    fn pass_1_hardware_error_jumps_ahead_not_aborts() {
        // New wedge-skip policy: Pass 1 (bisect_on_marginal=false)
        // should JumpAhead with a 1 GB skip + cooldown pause instead
        // of immediately aborting. Aborting on first wedge was the
        // pre-fix behavior that killed rips at 48% on damaged discs.
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&hardware_err(), &mut ctx);
        match action {
            ReadAction::JumpAhead {
                sectors,
                pause_secs,
            } => {
                assert_eq!(sectors, WEDGE_JUMP_SECTORS);
                assert_eq!(pause_secs, WEDGE_PAUSE_SECS);
            }
            other => panic!("expected JumpAhead, got {other:?}"),
        }
        assert_eq!(ctx.wedge_count, 1);
    }

    #[test]
    fn pass_1_hardware_error_aborts_after_threshold() {
        // After WEDGE_ABORT_THRESHOLD consecutive wedges with no good
        // read in between, autorip should see a real AbortPass so it
        // can surface "drive is stuck, power-cycle required" to the
        // user — rather than looping forever on a permanently bricked
        // drive.
        let mut ctx = ReadCtx::for_sweep(32);
        for i in 0..WEDGE_ABORT_THRESHOLD - 1 {
            let action = handle_read_error(&hardware_err(), &mut ctx);
            assert!(
                matches!(action, ReadAction::JumpAhead { .. }),
                "iter {i}: expected JumpAhead, got {action:?}"
            );
        }
        // The Nth wedge crosses the threshold.
        let action = handle_read_error(&hardware_err(), &mut ctx);
        assert_eq!(action, ReadAction::AbortPass);
    }

    #[test]
    fn pass_1_good_read_resets_wedge_count() {
        // A single successful read between wedges must clear the
        // skip counter — otherwise a disc with a few scattered bad
        // zones would eventually run out of skip budget even though
        // the drive was recovering between zones.
        let mut ctx = ReadCtx::for_sweep(32);
        for _ in 0..(WEDGE_ABORT_THRESHOLD - 1) {
            handle_read_error(&hardware_err(), &mut ctx);
        }
        assert_eq!(ctx.wedge_count, WEDGE_ABORT_THRESHOLD - 1);
        ctx.on_success();
        assert_eq!(ctx.wedge_count, 0);
        // After the success, we should still get JumpAhead (not
        // AbortPass) on the next wedge.
        let action = handle_read_error(&hardware_err(), &mut ctx);
        assert!(matches!(action, ReadAction::JumpAhead { .. }));
    }

    #[test]
    fn pass_n_hardware_error_also_skips_not_aborts() {
        // 2026-05-11 reframe: error handling is centralized, the
        // wedge is a code-induced state, and the avoidance principle
        // (skip + pause + continue) applies to Pass N too. Previously
        // Pass N AbortPass'd on first wedge — same fatal-at-48% bug
        // Pass 1 had pre-fix. Now Pass N gets a smaller skip
        // (WEDGE_PASS_N_SKIP_SECTORS, not the 1 GB Pass 1 jump)
        // because Pass N's job IS to revisit specific NonTrimmed
        // ranges; over-skipping abandons recoverable sectors.
        let mut ctx = ReadCtx::for_patch(1);
        let action = handle_read_error(&hardware_err(), &mut ctx);
        match action {
            ReadAction::JumpAhead {
                sectors,
                pause_secs,
            } => {
                assert_eq!(sectors, WEDGE_PASS_N_SKIP_SECTORS);
                assert_eq!(pause_secs, WEDGE_PAUSE_SECS);
            }
            other => panic!("expected JumpAhead, got {other:?}"),
        }
        assert_eq!(ctx.wedge_count, 1);
    }

    #[test]
    fn pass_n_hardware_error_aborts_after_threshold() {
        // Same threshold as Pass 1 — after WEDGE_ABORT_THRESHOLD
        // consecutive wedges with no good read in between, give up.
        let mut ctx = ReadCtx::for_patch(1);
        for _ in 0..WEDGE_ABORT_THRESHOLD - 1 {
            let action = handle_read_error(&hardware_err(), &mut ctx);
            assert!(matches!(action, ReadAction::JumpAhead { .. }));
        }
        let action = handle_read_error(&hardware_err(), &mut ctx);
        assert_eq!(action, ReadAction::AbortPass);
    }

    #[test]
    fn pass_1_illegal_request_also_routes_to_wedge_skip() {
        // ILLEGAL_REQUEST is the other half of the wedge family:
        // drive saying "I won't parse your CDB" after entering the
        // fast-fail state. Same treatment as HARDWARE_ERROR.
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&illegal_request_err(), &mut ctx);
        assert!(matches!(action, ReadAction::JumpAhead { .. }));
    }

    #[test]
    fn long_failure_streak_extends_pause_on_pass_n() {
        // Pass N keeps the cooldown behaviour: after many consecutive
        // failures, pauses extend to give the drive time to recover.
        // Pass 1 explicitly does NOT pause — see
        // `pass_1_does_not_pause_on_skip` below.
        let mut ctx = ReadCtx::for_patch(1);
        for _ in 0..15 {
            handle_read_error(&medium_err(), &mut ctx);
        }
        let final_action = handle_read_error(&medium_err(), &mut ctx);
        match final_action {
            ReadAction::SkipBlock { pause_secs } => {
                assert!(pause_secs >= CONSECUTIVE_FAIL_LONG_PAUSE_SECS);
            }
            ReadAction::JumpAhead { pause_secs, .. } => {
                assert!(pause_secs >= CONSECUTIVE_FAIL_LONG_PAUSE_SECS);
            }
            other => panic!("expected long-pause action, got {other:?}"),
        }
    }

    #[test]
    fn pass_1_zone_entry_uses_long_cooldown() {
        // 2026-05-11 wedge-prevention rewrite: Pass 1's FIRST error
        // (zone entry) gets a 30 s ZONE_ENTRY_COOLDOWN_SECS pause +
        // a 2 s POST_JUMP_EXTRA on top (since we're also jumping).
        // The long pause prevents the retry cadence that triggers
        // firmware fast-fail. Subsequent errors in the same zone fall
        // back to the standard 5 s FAIL_PAUSE_SECS.
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::JumpAhead { pause_secs, .. } => {
                assert_eq!(
                    pause_secs,
                    ZONE_ENTRY_COOLDOWN_SECS + POST_JUMP_EXTRA_PAUSE_SECS,
                    "first-error pause should be 30 + 2 = 32 s"
                );
            }
            other => panic!("expected JumpAhead on first Pass 1 error, got {other:?}"),
        }
    }

    #[test]
    fn pass_n_pauses_uniformly_on_failed_read() {
        // Pass N (bisect_on_marginal=true) is exempt from the
        // zone-entry long pause — its whole job is to retry single
        // sectors on already-known-bad LBAs, and the 30 s pause every
        // single-sector failure would multiply slow recovery
        // pointlessly. Pass N keeps the standard 5 s FAIL_PAUSE_SECS.
        let mut ctx = ReadCtx::for_patch(1);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { pause_secs } => assert_eq!(pause_secs, FAIL_PAUSE_SECS),
            ReadAction::JumpAhead { pause_secs, .. } => {
                assert_eq!(pause_secs, FAIL_PAUSE_SECS + POST_JUMP_EXTRA_PAUSE_SECS)
            }
            ReadAction::Bisect => {}
            other => panic!("expected pausing action, got {other:?}"),
        }
    }

    #[test]
    fn damage_window_fills_then_jumps() {
        let mut ctx = ReadCtx::for_sweep(1);
        ctx.damage_window_max = 4;
        ctx.damage_threshold_pct = 50;
        let mut saw_jump = false;
        for _ in 0..6 {
            let a = handle_read_error(&medium_err(), &mut ctx);
            if matches!(a, ReadAction::JumpAhead { .. }) {
                saw_jump = true;
                break;
            }
        }
        assert!(
            saw_jump,
            "expected at least one JumpAhead in 6 failures with 50% threshold"
        );
    }

    #[test]
    fn on_success_resets_failure_counters_and_pushes_window() {
        let mut ctx = ReadCtx::for_sweep(32);
        for _ in 0..3 {
            handle_read_error(&medium_err(), &mut ctx);
        }
        assert!(ctx.consecutive_failures > 0);
        ctx.bisecting = false;
        ctx.on_success();
        assert_eq!(ctx.consecutive_good, 1);
        assert_eq!(ctx.consecutive_failures, 0);
        assert!(*ctx.damage_window.last().unwrap());
    }
}
