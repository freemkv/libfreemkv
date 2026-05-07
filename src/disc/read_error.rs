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
    /// Sliding window of recent read outcomes (true=ok, false=fail).
    /// Capped at `damage_window_max`. Drives damage-jump decisions.
    pub damage_window: Vec<bool>,
    pub damage_window_max: usize,
    pub damage_threshold_pct: usize,
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
}

impl ReadCtx {
    /// Initial context for a Pass 1 sweep with the documented constants.
    pub fn for_sweep(batch: u16) -> Self {
        Self {
            batch,
            consecutive_good: 0,
            consecutive_failures: 0,
            damage_window: Vec::with_capacity(16),
            damage_window_max: 16,
            damage_threshold_pct: 12,
            jump_multiplier: 1,
            not_ready_retries: 0,
            bridge_degradation_count: 0,
            bisecting: false,
        }
    }

    /// Initial context for a Pass 2-N patch.
    pub fn for_patch(batch: u16) -> Self {
        Self {
            batch,
            consecutive_good: 0,
            consecutive_failures: 0,
            damage_window: Vec::with_capacity(16),
            damage_window_max: 16,
            damage_threshold_pct: 12,
            jump_multiplier: 1,
            not_ready_retries: 0,
            bridge_degradation_count: 0,
            bisecting: false,
        }
    }

    /// Caller calls this after every successful read.
    pub fn on_success(&mut self) {
        self.consecutive_good += 1;
        self.consecutive_failures = 0;
        self.not_ready_retries = 0;
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

/// THE single error-handling entry point. Updates `ctx`, returns the
/// action the caller must apply.
///
/// New error class = add a new arm here. New logging on errors = add
/// it once at the top. New retry policy = adjust the constants. No
/// other read site needs to change.
pub fn handle_read_error(err: &Error, ctx: &mut ReadCtx) -> ReadAction {
    ctx.consecutive_failures += 1;
    ctx.consecutive_good = 0;

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

    // 4. Hardware / illegal request: the drive said "no, won't do it".
    //    Retrying won't change the answer. Surface to the outer layer
    //    so it can eject + prompt user.
    if sense_key == scsi::SENSE_KEY_HARDWARE_ERROR || sense_key == scsi::SENSE_KEY_ILLEGAL_REQUEST {
        return ReadAction::AbortPass;
    }

    // 5. Marginal media (MEDIUM_ERROR / ABORTED_COMMAND) on a multi-
    //    sector batch: the drive can often read the same sectors
    //    individually. Bisect into single-sector reads (gentler on the
    //    bridge too — shorter SCSI transactions). Avoid recursive
    //    bisect.
    let is_marginal = matches!(
        sense_key,
        scsi::SENSE_KEY_MEDIUM_ERROR | scsi::SENSE_KEY_ABORTED_COMMAND
    );
    if is_marginal && ctx.batch > 1 && !ctx.bisecting {
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

    let pause_secs = if ctx.consecutive_failures >= CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD {
        CONSECUTIVE_FAIL_LONG_PAUSE_SECS
    } else {
        POST_FAILURE_PAUSE_SECS
    };

    // 7. Damage-jump: too many failures in window → skip ahead by an
    //    escalating gap. Multiplier capped so we can't accidentally
    //    skip the entire rest of the disc (observed 2026-05-07: a
    //    saturated multiplier produced a 56 GB jump). Saturating
    //    arithmetic on the sector calc as defence in depth.
    const MAX_JUMP_MULTIPLIER: u64 = 64; // 64 × 256 × batch sectors
    if ctx.damage_window.len() >= ctx.damage_window_max && bad_pct >= ctx.damage_threshold_pct {
        let mult = ctx.jump_multiplier.min(MAX_JUMP_MULTIPLIER);
        let sectors = 256u64.saturating_mul(ctx.batch as u64).saturating_mul(mult);
        ctx.jump_multiplier = (ctx.jump_multiplier.saturating_mul(2)).min(MAX_JUMP_MULTIPLIER);
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

    #[test]
    fn medium_error_with_batch_gt_1_bisects() {
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&medium_err(), &mut ctx);
        assert_eq!(action, ReadAction::Bisect);
    }

    #[test]
    fn medium_error_with_batch_1_skips() {
        let mut ctx = ReadCtx::for_sweep(1);
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { pause_secs } => assert!(pause_secs >= 1),
            other => panic!("expected SkipBlock, got {other:?}"),
        }
    }

    #[test]
    fn medium_error_while_bisecting_does_not_recurse() {
        let mut ctx = ReadCtx::for_sweep(32);
        ctx.bisecting = true;
        let action = handle_read_error(&medium_err(), &mut ctx);
        match action {
            ReadAction::SkipBlock { .. } => {}
            other => panic!("expected SkipBlock, got {other:?}"),
        }
    }

    #[test]
    fn hardware_error_aborts() {
        let mut ctx = ReadCtx::for_sweep(32);
        let action = handle_read_error(&hardware_err(), &mut ctx);
        assert_eq!(action, ReadAction::AbortPass);
    }

    #[test]
    fn long_failure_streak_extends_pause() {
        let mut ctx = ReadCtx::for_sweep(1);
        for _ in 0..15 {
            handle_read_error(&medium_err(), &mut ctx);
        }
        // After many consecutive failures we should be in the long-pause regime
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
        assert_eq!(*ctx.damage_window.last().unwrap(), true);
    }
}
