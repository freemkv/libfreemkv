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
}

impl ReadCtx {
    /// Initial context for a Pass 1 sweep. The job is "fast and
    /// accurate, get the most data in the shortest time" — Pass N
    /// is the one that grinds on the bad ranges. So bisect-on-
    /// marginal is OFF (failed batches become SkipBlock; whole 32-
    /// sector blocks marked NonTrimmed for Pass N to revisit), and
    /// the damage-jump fast-path triggers after just 4 consecutive
    /// outer-batch failures.
    pub fn for_sweep(batch: u16) -> Self {
        Self {
            batch,
            consecutive_good: 0,
            consecutive_failures: 0,
            consecutive_outer_failures: 0,
            damage_window: Vec::with_capacity(16),
            damage_window_max: 16,
            damage_threshold_pct: 12,
            fast_jump_threshold: 4,
            jump_multiplier: 1,
            not_ready_retries: 0,
            bridge_degradation_count: 0,
            bisecting: false,
            bisect_on_marginal: false,
            wedge_count: 0,
        }
    }

    /// Initial context for a Pass 2-N patch. Pass N's whole reason to
    /// exist is to recover sectors Pass 1 skipped — bisection on
    /// marginal media is part of the job, and the fast-jump
    /// threshold is loose so we don't bail too early on a range that
    /// has scattered good sectors mixed in.
    pub fn for_patch(batch: u16) -> Self {
        Self {
            batch,
            consecutive_good: 0,
            consecutive_failures: 0,
            consecutive_outer_failures: 0,
            damage_window: Vec::with_capacity(16),
            damage_window_max: 16,
            damage_threshold_pct: 12,
            // Pass N is allowed to grind: window-based jump only,
            // matching the historical behaviour for patch passes.
            fast_jump_threshold: u64::MAX,
            jump_multiplier: 1,
            not_ready_retries: 0,
            bridge_degradation_count: 0,
            bisecting: false,
            bisect_on_marginal: true,
            wedge_count: 0,
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
    }
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
const POST_FAILURE_PAUSE_SECS: u64 = 1;
const CONSECUTIVE_FAIL_LONG_PAUSE_SECS: u64 = 5;
const CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD: u64 = 10;
const POST_JUMP_EXTRA_PAUSE_SECS: u64 = 2;
const NOT_READY_PAUSE_SECS: u64 = 3;
const NOT_READY_MAX_RETRIES: u32 = 3;
const BRIDGE_DEGRADATION_PAUSE_SECS: u64 = 15;
const BRIDGE_DEGRADATION_MAX_RETRIES: u32 = 5;

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

    tracing::warn!(
        target: "freemkv::disc",
        phase = "read_error",
        consecutive_failures = ctx.consecutive_failures,
        batch = ctx.batch,
        bisecting = ctx.bisecting,
        sense_key = err.scsi_sense().map(|s| s.sense_key),
        asc = err.scsi_sense().map(|s| s.asc),
        ascq = err.scsi_sense().map(|s| s.ascq),
        error = %err,
        "read failed; classifying"
    );

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
    //    rejects reads near the LBA. Two policies:
    //
    //    Pass 1 sweep (bisect_on_marginal=false): the wedge is
    //      *recoverable* by skipping. Jump a large distance ahead
    //      (1 GB), pause for cooldown, mark the skipped region
    //      NonTrimmed so Pass N revisits. Allow up to
    //      WEDGE_ABORT_THRESHOLD consecutive wedges before truly
    //      giving up. This replaces the pre-fix "abort the entire
    //      rip on first wedge" behavior that caused 48%-and-die
    //      failures on discs with one bad cluster.
    //
    //    Pass N patch (bisect_on_marginal=true): Pass N's job IS
    //      single-sector recovery; a wedge means the drive won't
    //      give us the specific sectors we asked for. Skipping
    //      doesn't help here. Abort and let autorip decide whether
    //      to retry, eject, or surface the failure to the user.
    if sense_key == scsi::SENSE_KEY_HARDWARE_ERROR || sense_key == scsi::SENSE_KEY_ILLEGAL_REQUEST {
        if ctx.bisect_on_marginal {
            return ReadAction::AbortPass;
        }
        // Pass 1 wedge-skip path.
        if !ctx.bisecting {
            ctx.wedge_count += 1;
        }
        if ctx.wedge_count >= WEDGE_ABORT_THRESHOLD {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "wedge_abort",
                wedge_count = ctx.wedge_count,
                threshold = WEDGE_ABORT_THRESHOLD,
                "Pass 1 wedge-skip exhausted — drive appears permanently stuck"
            );
            return ReadAction::AbortPass;
        }
        tracing::warn!(
            target: "freemkv::disc",
            phase = "wedge_skip",
            wedge_count = ctx.wedge_count,
            jump_sectors = WEDGE_JUMP_SECTORS,
            pause_secs = WEDGE_PAUSE_SECS,
            "Pass 1 wedge detected — jumping ahead and pausing for drive cooldown"
        );
        return ReadAction::JumpAhead {
            sectors: WEDGE_JUMP_SECTORS,
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

    // Pass 1's job is to get to the end of the disc fast. Inter-
    // block pauses help the drive cool down on Pass N (gentle
    // recovery on bad ranges) but on Pass 1 they just turn a 30 s
    // damage zone into 30 minutes of dead-air sleep. Pass N
    // (`bisect_on_marginal=true`) keeps the original cooldown logic.
    let pause_secs = if !ctx.bisect_on_marginal {
        0
    } else if ctx.consecutive_failures >= CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD {
        CONSECUTIVE_FAIL_LONG_PAUSE_SECS
    } else {
        POST_FAILURE_PAUSE_SECS
    };

    // 7. Damage-jump: too many failures → skip ahead by an escalating
    //    gap. Multiplier capped so we can't accidentally skip the
    //    entire rest of the disc (observed 2026-05-07: a saturated
    //    multiplier produced a 56 GB jump). Saturating arithmetic on
    //    the sector calc as defence in depth.
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
    const MAX_JUMP_MULTIPLIER: u64 = 64; // 64 × 256 × batch sectors
    let fast_trigger = !ctx.bisecting && ctx.consecutive_outer_failures >= ctx.fast_jump_threshold;
    let window_trigger =
        ctx.damage_window.len() >= ctx.damage_window_max && bad_pct >= ctx.damage_threshold_pct;
    if fast_trigger || window_trigger {
        let mult = ctx.jump_multiplier.min(MAX_JUMP_MULTIPLIER);
        let sectors = 256u64.saturating_mul(ctx.batch as u64).saturating_mul(mult);
        ctx.jump_multiplier = (ctx.jump_multiplier.saturating_mul(2)).min(MAX_JUMP_MULTIPLIER);
        // Reset the outer-failure counter so a long damaged region
        // doesn't keep firing fast-jump every read after the initial
        // jump fired. The window-based trigger handles further jumps.
        ctx.consecutive_outer_failures = 0;
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
    fn pass_1_marginal_skips_instead_of_bisecting() {
        // Pass 1's job is "fast and accurate" — leave bisection to
        // Pass N. A failed batch becomes SkipBlock (whole 32-sector
        // block marked NonTrimmed for Pass N to revisit).
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { .. } => {}
            other => panic!("expected SkipBlock for Pass 1, got {other:?}"),
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
    fn pass_1_jumps_after_4_consecutive_outer_failures() {
        // The fast-entry trigger: Pass 1 should JumpAhead after 4
        // consecutive outer-batch failures, BEFORE the 16-block
        // damage window has filled. Otherwise we spend ~40 minutes
        // of bisecting/grinding to fill the window before the first
        // jump on a damage zone we entered cleanly.
        let mut ctx = ReadCtx::for_sweep(32);
        // First three should NOT jump (still under threshold of 4).
        for _ in 0..3 {
            let a = handle_read_error(&medium_err(), &mut ctx);
            assert!(
                !matches!(a, ReadAction::JumpAhead { .. }),
                "should not jump until 4 consecutive outer failures"
            );
        }
        // Fourth should jump.
        let a = handle_read_error(&medium_err(), &mut ctx);
        assert!(
            matches!(a, ReadAction::JumpAhead { .. }),
            "expected JumpAhead at 4th consecutive outer failure, got {a:?}"
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
        let mut ctx = ReadCtx::for_sweep(32);
        for _ in 0..3 {
            handle_read_error(&medium_err(), &mut ctx);
        }
        assert_eq!(ctx.consecutive_outer_failures, 3);
        // An outer-success (bisecting=false) should reset the counter.
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
    fn pass_n_hardware_error_still_aborts() {
        // Pass N (bisect_on_marginal=true) keeps the original
        // AbortPass behavior — single-sector recovery can't make
        // progress through a wedge, so the right answer is to bail
        // and let the outer layer decide.
        let mut ctx = ReadCtx::for_patch(1);
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
    fn pass_1_does_not_pause_on_skip() {
        // Pass 1 must zoom — a damage zone is Pass N's problem to
        // recover from. Sleeping between failed batches turned a 30s
        // damaged region into a 30-minute Pass-1 grind on real discs.
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { pause_secs } => assert_eq!(pause_secs, 0),
            other => panic!("expected SkipBlock, got {other:?}"),
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
