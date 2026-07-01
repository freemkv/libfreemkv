//! Handler-chain recovery of a single bad section (Pass-N rework, #55).
//!
//! The pre-existing patch loop grinds one bad range end-to-end, and when the
//! drive wedges it aborts the WHOLE pass — so a dead cluster at the *front* of
//! a range starves every later range of any attempt. This module replaces that
//! with a chain of time-bounded recovery *handlers*, each a single recovery
//! *idea* (read backwards, forwards, fast, slow, bisect...). A coordinator runs
//! them in sequence over one section's still-bad sub-ranges:
//!
//! - each handler gets a hard wall-clock `deadline` and MUST return promptly
//!   once it passes — no handler ever blocks unbounded (that is the whole
//!   point);
//! - a handler recovers what it can, shrinking the shared [`SubRanges`] via
//!   [`SubRanges::remove`], and returns [`HandlerOutcome::Remaining`] with the
//!   rest still bad — the NEXT handler then tries a different idea on what is
//!   left;
//! - whatever is still bad after every handler is the residue the caller
//!   records as loss (NonTrimmed) before MOVING ON to the next section.
//!
//! Adding a new recovery idea is one new [`SectionHandler`] impl pushed onto the
//! chain — nothing else changes.
//!
//! This module is deliberately decoupled from the live `patch` machinery
//! (`PatchSink`, `PatchItem`, mapfile locks): recovered bytes flow through the
//! tiny [`RecoverySink`] trait, and the clock is injected as `&dyn Fn`, so every
//! handler and the coordinator are unit-testable against a synthetic
//! `SectorSource` with a fake clock — no live drive, no real sleeps.
//!
//! Wired into `patch_region` (#55): [`run_handlers`] is the live Pass-N recovery
//! engine. `SubRanges` stays the shared still-bad set.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use super::patch::{SubRanges, recovery_read};
use super::read_error::SenseFamily;
use crate::sector::SectorSource;

/// One 2048-byte sector.
const SECTOR: u64 = 2048;
/// Batch size a linear handler reads at once (sectors). A partially-dead batch
/// falls back to single-sector reads, so this only trades throughput on clean
/// spans against granularity on dead ones.
const BATCH_SECTORS: u64 = 32;

/// `Jump` handler: after this many consecutive failed batches it jumps to the
/// middle of the remaining span (see the handler) to find where readable data
/// resumes rather than reading every dead sector.
const JUMP_AFTER_FAILS: u32 = 2;

/// Early-yield threshold: after this many consecutive reads that recover NOTHING,
/// a handler hands the still-bad set to the next handler instead of grinding out
/// its whole time budget on a dead zone. The baton comes back — a later handler,
/// or the next pass, retries the same sectors from a different angle / after the
/// drive state has shifted (recovery is stochastic). This is what turns a
/// "60 s of 0 B/s" stall into a fast hand-off.
const UNPRODUCTIVE_YIELD: u32 = 4;

/// Wedge abort: after this many CONSECUTIVE wedge-family senses (Hardware /
/// IllegalRequest — the BU40N firmware's fast-fail state, where it rejects every
/// CDB in <100 ms without attempting recovery) the drive is wedged. `read_span`
/// escalates the read to `Transport`, which every handler propagates as
/// `TransportFault` → the whole pass aborts and the caller spin-cycles the drive
/// instead of hammering all remaining sections (which only deepens the wedge).
/// Any Good read or non-wedge (medium-error) read resets the streak, so only a
/// sustained fast-fail run — never scattered bad sectors on real media — trips
/// it. Counted at the PASS level (persisted across sections) so a wedge is caught
/// even when every bad sub-range is smaller than the streak. Learned the hard way
/// (2026-07-01): the handler chain ground a wedged drive for 28 min at 0 B/s
/// because a fast-fail sense was classified as an ordinary bad sector.
///
/// Detection latency scales with how much streak one section can build. Tier 0's
/// 4 handlers × [`UNPRODUCTIVE_YIELD`] = 16 reads, so a single large wedged
/// section trips it within one `run_handlers` call. Tier 1 has only 2 handlers
/// (max 8 per section), so a wedge seen only in tier 1 relies on the pass-level
/// streak PERSISTING across sections to reach the threshold — regression-tested
/// by `wedge_streak_persists_across_sections_for_tier1`.
const WEDGE_ABORT_STREAK: u32 = 16;

/// A wedge-family failure only counts toward [`WEDGE_ABORT_STREAK`] if it came
/// back faster than this — the fast-fail wedge rejects a CDB in <100ms with no
/// recovery attempt, whereas a genuine uncorrectable sector on Hardware-error
/// media spends real time on ECC recovery before failing. Gating on latency stops
/// slow, real damage that happens to report a Hardware sense from false-tripping
/// the wedge abort. Generous (500ms) so a slow bus adds margin without admitting
/// a true fast-fail.
const WEDGE_FASTFAIL_MS: u64 = 500;

/// Max read speed sentinel for `SET CD SPEED` (0xFFFF = "as fast as the drive
/// will go"). The default for every read; a handler that wants to slow the
/// spindle passes [`SpeedPref::Min`] and [`read_span`] restores this on exit.
const SPEED_MAX_KBS: u16 = 0xFFFF;

/// Min read speed (~DVD 1×; the drive clamps up to its own supported minimum).
/// Slower rotation gives the servo more dwell and the ECC engine more
/// integration time per sector — the SlowSpin / SpeedSweep lever. The exact
/// value only has to be well below max; the drive rounds it to a supported step.
const SPEED_MIN_KBS: u16 = 1385;

/// Which spindle speed a read requests. `Max` is the streaming default; `Min`
/// slows the spindle for marginal-sector recovery (more servo dwell + ECC
/// integration). `Mid` is reserved for a future resonance step (SpeedSweep).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SpeedPref {
    Max,
    Min,
}

impl SpeedPref {
    /// The `SET CD SPEED` value (KB/s) this preference maps to.
    fn kbs(self) -> u16 {
        match self {
            SpeedPref::Max => SPEED_MAX_KBS,
            SpeedPref::Min => SPEED_MIN_KBS,
        }
    }
}

/// Which SCSI read timeout a read requests. `Fast` is the 10 s single-attempt
/// budget (scouting); `Deep` is the 60 s ECC-recovery budget (deep recovery).
/// Maps onto `recovery_read`'s `recovery` bool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TimeoutPref {
    Fast,
    Deep,
}

impl TimeoutPref {
    /// The `recovery` bool (true = 60 s deep) this timeout maps to.
    fn recovery(self) -> bool {
        matches!(self, TimeoutPref::Deep)
    }
}

/// The per-read knobs a handler hands to [`read_span`]. A handler is a point in
/// the (direction × speed × cache × timeout) space; `ReadParams` carries the
/// speed / cache(FUA) / timeout axes (direction is the handler's own walk), so
/// the SAME read primitive serves every handler — a new technique is a new
/// *parameterisation*, never a bypass of the wedge-safe read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReadParams {
    pub speed: SpeedPref,
    pub fua: bool,
    pub timeout: TimeoutPref,
}

impl ReadParams {
    /// Tier-0 scout read: max speed, cache on, 10 s single-attempt.
    pub(super) fn fast() -> Self {
        Self {
            speed: SpeedPref::Max,
            fua: false,
            timeout: TimeoutPref::Fast,
        }
    }

    /// Tier-1 deep read: max speed, cache on, 60 s ECC-recovery budget.
    pub(super) fn deep() -> Self {
        Self {
            speed: SpeedPref::Max,
            fua: false,
            timeout: TimeoutPref::Deep,
        }
    }

    /// Scorecard tag for the speed / cache / timeout axes, e.g. `min:fua:deep`.
    /// The handler prepends its own name + direction (`linear:fwd:` + tag).
    fn tag(&self) -> String {
        let speed = match self.speed {
            SpeedPref::Max => "max",
            SpeedPref::Min => "min",
        };
        let timeout = match self.timeout {
            TimeoutPref::Fast => "fast",
            TimeoutPref::Deep => "deep",
        };
        if self.fua {
            format!("{speed}:fua:{timeout}")
        } else {
            format!("{speed}:{timeout}")
        }
    }
}

/// Where a handler left the section after its bounded attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HandlerOutcome {
    /// The still-bad set is now empty — the section is fully recovered. The
    /// coordinator stops the chain.
    Complete,
    /// The handler finished or hit its deadline with bad sub-ranges remaining —
    /// the coordinator moves to the next handler.
    Remaining,
    /// The caller's halt token was observed set — abort the chain.
    Halted,
    /// A transport-layer fault (bridge wedge / dead bus) — the device never
    /// answered. The coordinator returns this so the caller can un-wedge
    /// (spin-cycle) before deciding whether to continue.
    TransportFault,
}

/// Receives sectors a handler successfully read back. Kept minimal and
/// decoupled from `PatchSink` so handlers are unit-testable in isolation; the
/// live wiring maps `recovered` onto the mapfile write + Finished mark.
pub(super) trait RecoverySink {
    /// `buf` holds the plaintext bytes for the byte-range `[pos, pos+buf.len())`
    /// (all multiples of [`SECTOR`]).
    fn recovered(&mut self, pos: u64, buf: &[u8]);
}

/// Everything a handler needs, borrowed for the duration of one `recover` call.
/// The `deadline` is passed separately to `recover` (not stored here) so each
/// handler invocation is independently bounded.
pub(super) struct HandlerCtx<'a> {
    pub reader: &'a mut dyn SectorSource,
    pub sink: &'a mut dyn RecoverySink,
    /// Clock seam — handlers read wall time through this, never `Instant::now()`
    /// inline, so tests advance a fake clock deterministically.
    pub now: &'a dyn Fn() -> Instant,
    pub halt: Option<&'a AtomicBool>,
    /// Widen mid-unit reads to the aligned AACS unit (see [`recovery_read`]).
    pub decrypt_is_aacs: bool,
    /// Progress heartbeat. Handlers call [`HandlerCtx::progress`] frequently (it
    /// is internally throttled); this pushes a fresh progress snapshot to the
    /// caller's reporter DURING a handler, not just at range boundaries — so the
    /// bar and speed move as recovery happens instead of jumping once per
    /// section. `None` in tests (no reporter).
    pub tick: Option<&'a mut dyn FnMut()>,
    /// Consecutive reads that recovered nothing, updated by [`read_span`]. When
    /// it reaches [`UNPRODUCTIVE_YIELD`] the handler should yield to the next one
    /// (see [`HandlerCtx::stalled`]). Reset to 0 before each handler runs.
    pub unproductive: u32,
    /// Consecutive wedge-family senses (Hardware / IllegalRequest), updated by
    /// [`read_span`]. At [`WEDGE_ABORT_STREAK`] the drive is wedged and the read
    /// escalates to `Transport`. Seeded from and read back into the pass-level
    /// counter so the streak spans sections; a Good or non-wedge read resets it.
    pub wedge_streak: u32,
    /// The spindle speed (`SET CD SPEED` KB/s) currently programmed into the
    /// drive. [`read_span`] issues `SET CD SPEED` only when a read's requested
    /// speed DIFFERS from this (a `SET CD SPEED` per read would thrash the
    /// spindle), and [`run_handlers`] restores [`SPEED_MAX_KBS`] after each
    /// handler. Seeded to max — the caller resets the drive to max before the
    /// chain runs.
    pub cur_speed: u16,
}

impl HandlerCtx<'_> {
    fn halted(&self) -> bool {
        self.halt.is_some_and(|h| h.load(Ordering::Relaxed))
    }

    /// The universal "stop this handler now" check every handler loop already
    /// calls between reads. True when the deadline passed OR the handler has hit
    /// its early-yield dead streak — folding the yield in here means every
    /// handler hands the baton off on a dead zone with no per-handler edits.
    fn past(&self, deadline: Instant) -> bool {
        self.stalled() || (self.now)() >= deadline
    }

    /// True once the handler has read `UNPRODUCTIVE_YIELD` sectors in a row with
    /// no recovery — its cue to hand the baton to the next handler instead of
    /// grinding a dead zone for its whole budget.
    fn stalled(&self) -> bool {
        self.unproductive >= UNPRODUCTIVE_YIELD
    }

    /// Deadline-only stop check (ignores the early-yield stall streak). Used
    /// inside Bisect's boundary-probing loops, where a short run of failing
    /// reads is the *expected* way to home in on a dead edge — not a stall.
    fn timed_out(&self, deadline: Instant) -> bool {
        (self.now)() >= deadline
    }

    /// Emit a progress heartbeat (throttling lives in the tick closure).
    fn progress(&mut self) {
        if let Some(t) = self.tick.as_mut() {
            t();
        }
    }
}

/// Outcome of one physical read attempt, before the caller decides what to do
/// with the still-bad set.
enum ReadHit {
    /// Bytes came back and were handed to the sink.
    Good,
    /// A recoverable bad-sector error (media / check-condition). Leave the span
    /// bad and move on.
    Bad,
    /// Transport-layer fault — the bus is gone. Abort now.
    Transport,
}

/// Read `count` sectors at byte offset `pos` and, on success, hand them to the
/// sink. Does NOT touch the still-bad set — the caller removes recovered spans
/// so the read helper stays independent of `SubRanges`.
fn read_span(
    ctx: &mut HandlerCtx,
    buf: &mut [u8],
    pos: u64,
    count: u16,
    params: ReadParams,
) -> ReadHit {
    let lba = (pos / SECTOR) as u32;
    let bytes = count as usize * SECTOR as usize;
    // Every SubRange enters via `from_section` / `remove`, which keep byte
    // offsets sector-aligned, so a handler never asks for a sub-sector span. Pin
    // that invariant: a zero `count` (span < SECTOR) would be a 0-sector read
    // that silently "recovers" nothing — surface the caller bug in tests.
    debug_assert!(
        count >= 1 && pos % SECTOR == 0,
        "read_span requires a sector-aligned, >=1-sector span (pos={pos}, count={count})"
    );
    // Program the spindle speed ONLY when it changes — a `SET CD SPEED` per read
    // would thrash the drive. `run_handlers` restores max after the handler.
    let want_speed = params.speed.kbs();
    if want_speed != ctx.cur_speed {
        ctx.reader.set_speed(want_speed);
        ctx.cur_speed = want_speed;
    }
    let recovery = params.timeout.recovery();
    let read_started = (ctx.now)();
    let hit = match recovery_read(
        ctx.reader,
        ctx.decrypt_is_aacs,
        lba,
        count,
        buf,
        recovery,
        params.fua,
    ) {
        Ok(_) => {
            ctx.sink.recovered(pos, &buf[..bytes]);
            ReadHit::Good
        }
        Err(e) if e.is_scsi_transport_failure() => ReadHit::Transport,
        Err(e) => {
            // Wedge watch: the drive's fast-fail wedge REJECTS every CDB in <100ms
            // without attempting recovery, with a Hardware / IllegalRequest sense.
            // Both signals are required to count toward the streak:
            //   (1) wedge-family sense (Hardware / IllegalRequest), AND
            //   (2) the failure came back FAST (< WEDGE_FASTFAIL_MS).
            // The latency gate is what keeps a genuine uncorrectable sector on
            // Hardware-error media from false-tripping the wedge abort: a real
            // ECC-recovery attempt takes far longer than a fast-fail rejection, so
            // a SLOW Hardware-error is real damage (resets the streak, retried
            // next pass), while only the fast rejections — the actual wedge —
            // accumulate. A medium error or any success below also resets it.
            let sense_is_wedge = e
                .scsi_sense()
                .map(|s| SenseFamily::from_sense_key(s.sense_key).is_wedge_family())
                .unwrap_or(false);
            let elapsed = (ctx.now)().duration_since(read_started);
            let fast_fail = elapsed.as_millis() < WEDGE_FASTFAIL_MS as u128;
            if sense_is_wedge && fast_fail {
                ctx.wedge_streak = ctx.wedge_streak.saturating_add(1);
                if ctx.wedge_streak >= WEDGE_ABORT_STREAK {
                    ReadHit::Transport
                } else {
                    ReadHit::Bad
                }
            } else {
                ctx.wedge_streak = 0;
                ReadHit::Bad
            }
        }
    };
    // Track the dead streak for the early-yield hand-off: a recovering read
    // resets it, a fruitless one advances it toward UNPRODUCTIVE_YIELD.
    match hit {
        ReadHit::Good => {
            ctx.unproductive = 0;
            ctx.wedge_streak = 0;
        }
        // A Bad read is unproductive grinding — advance the yield streak.
        ReadHit::Bad => ctx.unproductive = ctx.unproductive.saturating_add(1),
        // A Transport hit aborts the handler immediately (bus fault / wedge
        // escalation), so it is NOT unproductive grinding — leave the streak
        // untouched (the counter is never read again after TransportFault, but
        // keep the semantics honest in case an arm is ever reordered).
        ReadHit::Transport => {}
    }
    // Heartbeat after every read (the tick closure throttles to ~250 ms) so the
    // UI's bar/speed move DURING a handler, not just when the section finishes.
    ctx.progress();
    hit
}

/// One recovery idea, given a bounded shot at the section's still-bad set.
///
/// Contract: check `ctx.halted()` and `ctx.past(deadline)` between reads and
/// return promptly (`Halted` / `Remaining`) — never loop past the deadline. On a
/// good read call `ctx.sink.recovered` and [`SubRanges::remove`] the span; on a
/// bad read leave it in `bad` and advance (skip-and-move-on); on a transport
/// fault return [`HandlerOutcome::TransportFault`] immediately.
pub(super) trait SectionHandler {
    /// Scorecard identity — the FULL config (technique + direction + speed +
    /// cache + timeout), e.g. `linear:fwd:min:fua:deep`. The scoreboard keys on
    /// this, so two instances of the same handler at different [`ReadParams`]
    /// score independently and can flip past each other.
    fn name(&self) -> String;
    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome;
}

/// Which end a [`Linear`] sweep walks from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Direction {
    /// start→end (the front the reverse pass kept dying on).
    Forward,
    /// end→start (the disc sweep overshoots forward, so a NonTrimmed range's
    /// good data sits at its tail — reverse hits it first).
    Reverse,
}

impl Direction {
    fn is_reverse(self) -> bool {
        matches!(self, Direction::Reverse)
    }

    fn tag(self) -> &'static str {
        match self {
            Direction::Forward => "fwd",
            Direction::Reverse => "rev",
        }
    }
}

/// Linear batch sweep of each bad sub-range, in `direction`, at `params`. The
/// direction × the [`ReadParams`] axes (speed / FUA / timeout) give every
/// backwards/forwards × fast/slow × max/min × cache/FUA combination from one
/// handler — the tier-0 fast scouts, the tier-1 deep sweeps, and the tier-2
/// SlowSpin / FuaRetry / SlowFua specialists are all just `Linear` at different
/// `params`.
pub(super) struct Linear {
    pub direction: Direction,
    pub params: ReadParams,
}

impl SectionHandler for Linear {
    fn name(&self) -> String {
        format!("linear:{}:{}", self.direction.tag(), self.params.tag())
    }

    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome {
        let reverse = self.direction.is_reverse();
        let batch_bytes = BATCH_SECTORS * SECTOR;
        let mut buf = vec![0u8; batch_bytes as usize];
        // Snapshot the sub-ranges: we mutate `bad` via remove() as we recover,
        // and iterating the snapshot keeps that from disturbing the walk.
        let mut snapshot: Vec<(u64, u64)> = bad.ranges().to_vec();
        if reverse {
            snapshot.reverse();
        }

        for (rp, rl) in snapshot {
            // Position within the range, in bytes, walked from whichever end.
            let mut done = 0u64;
            while done < rl {
                if ctx.halted() {
                    return HandlerOutcome::Halted;
                }
                if ctx.past(deadline) {
                    return HandlerOutcome::Remaining;
                }
                let span = batch_bytes.min(rl - done);
                let pos = if reverse {
                    rp + (rl - done - span)
                } else {
                    rp + done
                };
                let count = (span / SECTOR) as u16;
                match read_span(ctx, &mut buf, pos, count, self.params) {
                    ReadHit::Good => bad.remove(pos, span),
                    // Keep reads at the full batch — no per-sector grind (proven
                    // worse on the BU40N, and it's what stalled a handler on a
                    // dead front). Leave the failed batch bad and advance; the
                    // readable tail past it is reached by the next batch, and
                    // Bisect salvages readable islands inside a dead batch.
                    ReadHit::Bad => {}
                    ReadHit::Transport => return HandlerOutcome::TransportFault,
                }
                done += span;
            }
        }

        if bad.is_empty() {
            HandlerOutcome::Complete
        } else {
            HandlerOutcome::Remaining
        }
    }
}

/// Bisect + expand. Probe the middle sector of a bad sub-range; when it reads,
/// EXPAND outward from it — forward and backward in full batches — until a read
/// fails, recovering the whole readable island around the good centre in large
/// reads. The two failing ends become smaller bad sub-ranges, pushed back to be
/// bisected again. A dead middle just splits into halves. This shreds one huge
/// bad range into precisely-located small dead clusters (a handful of sectors)
/// instead of leaving the whole thing bad. `params` is normally fast reads: it
/// LOCATES readable data; deep-recovering the dead sectors is the slow linear
/// handlers' job. Tier 2 also runs a Bisect at FUA/deep params to shred islands
/// under cache-bypass.
pub(super) struct Bisect {
    pub params: ReadParams,
}

impl SectionHandler for Bisect {
    fn name(&self) -> String {
        format!("bisect:{}", self.params.tag())
    }

    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome {
        let batch = BATCH_SECTORS * SECTOR;
        let mut buf = vec![0u8; batch as usize];
        let mut probe = [0u8; SECTOR as usize];
        // Work stack of still-bad chunks. A good probe recovers the readable
        // island around it and pushes the two (smaller) failing ends; a dead
        // probe pushes the two halves. Either way the stack shrinks toward small
        // bad clusters, so it drains in bounded steps.
        let mut stack: Vec<(u64, u64)> = bad.ranges().to_vec();
        while let Some((rp, rl)) = stack.pop() {
            if rl == 0 {
                continue;
            }
            if ctx.halted() {
                return HandlerOutcome::Halted;
            }
            if ctx.past(deadline) {
                return HandlerOutcome::Remaining;
            }
            let end = rp + rl;
            let mid = rp + (rl / SECTOR / 2) * SECTOR;
            match read_span(ctx, &mut probe, mid, 1, self.params) {
                ReadHit::Good => {
                    bad.remove(mid, SECTOR);
                    // Expand FORWARD from mid+1 in batches until a read fails.
                    let mut fwd = mid + SECTOR;
                    let mut step = batch;
                    while fwd < end {
                        if ctx.halted() {
                            return HandlerOutcome::Halted;
                        }
                        if ctx.timed_out(deadline) {
                            return HandlerOutcome::Remaining;
                        }
                        let span = step.min(end - fwd);
                        let count = (span / SECTOR) as u16;
                        match read_span(ctx, &mut buf[..span as usize], fwd, count, self.params) {
                            ReadHit::Good => {
                                bad.remove(fwd, span);
                                fwd += span;
                                step = batch;
                            }
                            // Halve at the dead boundary instead of giving up, so
                            // the readable sectors right up to the dead one are
                            // recovered in ~log2(batch) reads (no per-sector grind).
                            ReadHit::Bad => {
                                if span > SECTOR {
                                    step = ((span / SECTOR) / 2).max(1) * SECTOR;
                                } else {
                                    break;
                                }
                            }
                            ReadHit::Transport => return HandlerOutcome::TransportFault,
                        }
                    }
                    // Expand BACKWARD from mid toward rp until a read fails.
                    let mut bwd = mid;
                    let mut step = batch;
                    while bwd > rp {
                        if ctx.halted() {
                            return HandlerOutcome::Halted;
                        }
                        if ctx.timed_out(deadline) {
                            return HandlerOutcome::Remaining;
                        }
                        let span = step.min(bwd - rp);
                        let pos = bwd - span;
                        let count = (span / SECTOR) as u16;
                        match read_span(ctx, &mut buf[..span as usize], pos, count, self.params) {
                            ReadHit::Good => {
                                bad.remove(pos, span);
                                bwd = pos;
                                step = batch;
                            }
                            ReadHit::Bad => {
                                if span > SECTOR {
                                    step = ((span / SECTOR) / 2).max(1) * SECTOR;
                                } else {
                                    break;
                                }
                            }
                            ReadHit::Transport => return HandlerOutcome::TransportFault,
                        }
                    }
                    // Locating this readable island was productive work; the
                    // failed reads that pinned its dead edges are boundary probes,
                    // not a stall. Clear the streak so the re-bisect (and the next
                    // handler) start fresh.
                    ctx.unproductive = 0;
                    // The two failing ends stay bad — bisect them again to pin
                    // the exact dead sectors.
                    if bwd > rp {
                        stack.push((rp, bwd - rp));
                    }
                    if fwd < end {
                        stack.push((fwd, end - fwd));
                    }
                }
                ReadHit::Bad => {
                    // Dead middle: split and keep hunting for a good centre.
                    if mid > rp {
                        stack.push((rp, mid - rp));
                    }
                    let right = mid + SECTOR;
                    if right < end {
                        stack.push((right, end - right));
                    }
                }
                ReadHit::Transport => return HandlerOutcome::TransportFault,
            }
        }

        if bad.is_empty() {
            HandlerOutcome::Complete
        } else {
            HandlerOutcome::Remaining
        }
    }
}

/// Blow through a LARGE dead run fast. Reads forward in batches; after
/// [`JUMP_AFTER_FAILS`] consecutive failed batches it SKIPS AHEAD an escalating
/// distance (1 MiB → 2 → 4 … capped at [`JUMP_CAP_BYTES`]), leaving the skipped
/// span bad, to find where readable data RESUMES — mirroring the Pass-1
/// damage-jump. A later handler / `Bisect` pins the exact good/bad boundary the
/// jump stepped over. Uses fast reads (this is a scout, not a deep-recovery
/// pass). Without it a linear walk pays one up-to-10 s read per dead batch
/// across the whole run, so a deadline-bounded pass never reaches readable data
/// buried behind a big dead front (exactly the 192 MB range on Dune).
pub(super) struct Jump {
    pub params: ReadParams,
}

impl SectionHandler for Jump {
    fn name(&self) -> String {
        format!("jump:{}", self.params.tag())
    }

    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome {
        let batch = BATCH_SECTORS * SECTOR;
        let mut buf = vec![0u8; batch as usize];
        let snapshot: Vec<(u64, u64)> = bad.ranges().to_vec();
        for (rp, rl) in snapshot {
            let mut off = 0u64;
            let mut consec_fail = 0u32;
            while off < rl {
                if ctx.halted() {
                    return HandlerOutcome::Halted;
                }
                if ctx.past(deadline) {
                    return HandlerOutcome::Remaining;
                }
                let span = batch.min(rl - off);
                let pos = rp + off;
                let count = (span / SECTOR) as u16;
                match read_span(ctx, &mut buf[..span as usize], pos, count, self.params) {
                    ReadHit::Good => {
                        bad.remove(pos, span);
                        consec_fail = 0;
                        off += span;
                    }
                    ReadHit::Bad => {
                        consec_fail += 1;
                        if consec_fail >= JUMP_AFTER_FAILS {
                            // Sustained dead run — jump to the MIDDLE of the
                            // remaining span (never overshoot the range). Halving
                            // adapts to any size: a big dead run is crossed in
                            // ~log2 jumps, and a small range lands mid-range
                            // instead of being skipped past entirely (the 8 MiB
                            // fixed jump used to leap clean over a <8 MiB range and
                            // miss readable data in its middle). The skipped span
                            // stays bad for Bisect to reclaim.
                            let remaining = rl - off;
                            let step = ((remaining / 2) / SECTOR).max(1) * SECTOR;
                            off += step;
                            consec_fail = 0;
                        } else {
                            off += span;
                        }
                    }
                    ReadHit::Transport => return HandlerOutcome::TransportFault,
                }
            }
        }
        if bad.is_empty() {
            HandlerOutcome::Complete
        } else {
            HandlerOutcome::Remaining
        }
    }
}

/// SpeedSweep — per residual sector, try Max→Min spindle speeds until one reads.
/// *Failure mode:* speed resonance — the best speed is NOT always the slowest;
/// some marginal sectors hit a read-channel sweet spot at a higher speed, so a
/// per-sector search beats committing to min. Distinct from SlowSpin (a `Linear`
/// pinned to min): this searches. `params` carries the FUA / timeout axes; the
/// speed axis is what it sweeps. Single-sector, so it runs on the true residual.
pub(super) struct SpeedSweep {
    pub params: ReadParams,
}

impl SectionHandler for SpeedSweep {
    fn name(&self) -> String {
        format!("speedsweep:{}", self.params.tag())
    }

    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome {
        // Fastest first — resonance means the sweet spot isn't always the
        // slowest, and the fast read costs least when it happens to work.
        const SWEEP: [SpeedPref; 2] = [SpeedPref::Max, SpeedPref::Min];
        let mut probe = [0u8; SECTOR as usize];
        let snapshot: Vec<(u64, u64)> = bad.ranges().to_vec();
        for (rp, rl) in snapshot {
            let mut off = 0u64;
            while off < rl {
                if ctx.halted() {
                    return HandlerOutcome::Halted;
                }
                if ctx.past(deadline) {
                    return HandlerOutcome::Remaining;
                }
                let pos = rp + off;
                for speed in SWEEP {
                    let params = ReadParams {
                        speed,
                        fua: self.params.fua,
                        timeout: self.params.timeout,
                    };
                    match read_span(ctx, &mut probe, pos, 1, params) {
                        ReadHit::Good => {
                            bad.remove(pos, SECTOR);
                            break;
                        }
                        // This speed didn't read it; try the next one.
                        ReadHit::Bad => continue,
                        ReadHit::Transport => return HandlerOutcome::TransportFault,
                    }
                }
                off += SECTOR;
            }
        }
        if bad.is_empty() {
            HandlerOutcome::Complete
        } else {
            HandlerOutcome::Remaining
        }
    }
}

/// EWMA smoothing factor for the decayed recovery rate. Each new attempt is
/// weighted `α`, the running average `1-α`, so a handler's score tracks its
/// RECENT performance and forgets its distant past at a rate set by `α`. Higher
/// = more reactive (leadership flips sooner); lower = steadier. 0.5 halves the
/// weight of the previous score on every attempt — reactive enough that a proven
/// early winner whose territory is exhausted decays out of the lead within a few
/// barren attempts, while a late-starting specialist climbs as it earns.
const SCORE_EWMA_ALPHA: f64 = 0.5;

/// Per-rip handler scorecard. Grades each handler by a DECAYED recovery rate (an
/// EWMA of bytes-recovered-per-second, [`SCORE_EWMA_ALPHA`]) so the coordinator
/// runs whoever is winning *now* FIRST on later sections. The residual shrinks
/// and hardens mid-pass, so the best technique CHANGES: the fast scouts clean
/// the range-fronts, then the leftovers are exactly the marginal sectors where
/// the specialists win — and the ranking must FLIP. A cumulative rate froze the
/// early winner in the lead forever; the EWMA re-prices continuously — a handler
/// that stops earning decays down, one that starts earning climbs. Ephemeral —
/// reset each rip, no persistence. A handler not yet tried ranks top
/// (`u64::MAX`) so every handler is calibrated once before the ranking narrows.
#[derive(Default)]
pub(super) struct HandlerScoreboard {
    stats: std::collections::HashMap<String, ScoreStat>,
}

#[derive(Default, Clone, Copy)]
struct ScoreStat {
    /// Decayed recovery rate (bytes/second), the ranking signal. `None` until
    /// the first attempt that spent measurable time (a zero-elapsed call proves
    /// no rate). Seeded to the first timed sample, then EWMA'd.
    ewma_rate: Option<f64>,
    // Cumulative totals — for the operator log line only, NOT for ranking.
    recovered: u64,
    nanos: u128,
    attempts: u64,
}

impl HandlerScoreboard {
    /// Fold one timed sample (bytes/second) into the decayed rate.
    fn decay(prev: Option<f64>, sample: f64) -> f64 {
        match prev {
            None => sample,
            Some(p) => SCORE_EWMA_ALPHA * sample + (1.0 - SCORE_EWMA_ALPHA) * p,
        }
    }

    /// Record one attempt: `recovered` bytes over `elapsed`. A timed attempt
    /// (elapsed > 0) decays a fresh bytes/second sample into `ewma_rate` — a
    /// barren attempt (recovered = 0) contributes a 0 sample that decays the
    /// score DOWN, which is exactly what lets an exhausted early winner lose its
    /// lead. A zero-elapsed call (handler yielded before any timed read)
    /// contributes no rate sample.
    fn record(&mut self, name: &str, recovered: u64, elapsed: std::time::Duration) {
        let e = self.stats.entry(name.to_string()).or_default();
        e.recovered = e.recovered.saturating_add(recovered);
        e.nanos = e.nanos.saturating_add(elapsed.as_nanos());
        e.attempts += 1;
        let secs = elapsed.as_secs_f64();
        if secs > 0.0 {
            let sample = recovered as f64 / secs;
            e.ewma_rate = Some(Self::decay(e.ewma_rate, sample));
        }
    }

    /// Ranking key (higher runs earlier). Untried → top, so it gets calibrated.
    fn rank(&self, name: &str) -> u64 {
        match self.stats.get(name) {
            // Never attempted → top, so every handler is calibrated once.
            None => u64::MAX,
            // Attempted but no timed sample yet — e.g. it returned `Halted` on
            // its first check or did zero reads. It proved nothing, so rank it at
            // the BOTTOM (0), not the top: otherwise a called-but-idle handler
            // perpetually crowds out proven performers.
            Some(s) => match s.ewma_rate {
                None => 0,
                Some(r) => r.max(0.0).min(u64::MAX as f64) as u64,
            },
        }
    }

    /// Emit the scorecard to the log so the operator can see, per rip, which
    /// handler is pulling the weight and which is a dud on this drive/disc.
    pub(super) fn log(&self) {
        let mut rows: Vec<_> = self.stats.iter().collect();
        // Rank by the decayed rate (the live signal), highest first.
        rows.sort_by(|a, b| self.rank(b.0).cmp(&self.rank(a.0)));
        for (name, s) in rows {
            let mbps = s.recovered as f64 / (s.nanos as f64 / 1e9).max(1e-9) / 1_048_576.0;
            tracing::info!(
                target: "freemkv::disc",
                phase = "scorecard",
                handler = name.as_str(),
                recovered_mb = s.recovered as f64 / 1_048_576.0,
                attempts = s.attempts,
                decayed_bytes_per_s = s.ewma_rate.unwrap_or(0.0),
                mb_per_s = mbps,
                "handler scorecard (this rip)"
            );
        }
    }
}

/// Run the handler chain over one section's still-bad set, ordered best-first by
/// the rip scorecard. Never-hang guarantee: each handler is deadline-bounded and
/// the loop always drains to `Complete`/`Remaining`. `Halted` / `TransportFault`
/// short-circuit so the caller can abort or un-wedge. Each attempt is scored so
/// later sections run the winners first.
pub(super) fn run_handlers(
    ctx: &mut HandlerCtx,
    handlers: &mut [Box<dyn SectionHandler>],
    bad: &mut SubRanges,
    scoreboard: &mut HandlerScoreboard,
    section_deadline_for: impl Fn(&SubRanges) -> Instant,
) -> HandlerOutcome {
    // Best-first by recovery rate so far; untried handlers rank top (calibrate).
    handlers.sort_by_key(|h| std::cmp::Reverse(scoreboard.rank(&h.name())));
    for handler in handlers.iter_mut() {
        if bad.is_empty() {
            return HandlerOutcome::Complete;
        }
        let name = handler.name();
        let before = bad.total_len();
        let deadline = section_deadline_for(bad);
        let started = (ctx.now)();
        // Fresh dead-streak budget per handler: each gets its own chance before
        // the early-yield trips.
        ctx.unproductive = 0;
        let outcome = handler.recover(ctx, bad, deadline);
        // A handler may have dropped the spindle (SlowSpin / SpeedSweep) or set
        // FUA; restore max speed before the next handler so it starts from the
        // streaming default (FUA is a per-read param, so nothing to unwind there).
        if ctx.cur_speed != SPEED_MAX_KBS {
            ctx.reader.set_speed(SPEED_MAX_KBS);
            ctx.cur_speed = SPEED_MAX_KBS;
        }
        let elapsed = (ctx.now)().duration_since(started);
        let after = bad.total_len();
        scoreboard.record(&name, before.saturating_sub(after), elapsed);
        tracing::info!(
            target: "freemkv::disc",
            phase = "section_recover.handler",
            handler = name.as_str(),
            bad_bytes_before = before,
            bad_bytes_after = after,
            recovered = before.saturating_sub(after),
            outcome = ?outcome,
            "handler finished; remaining bad bytes carry to the next handler"
        );
        match outcome {
            HandlerOutcome::Complete => return HandlerOutcome::Complete,
            HandlerOutcome::Remaining => continue,
            HandlerOutcome::Halted => return HandlerOutcome::Halted,
            HandlerOutcome::TransportFault => return HandlerOutcome::TransportFault,
        }
    }
    if bad.is_empty() {
        HandlerOutcome::Complete
    } else {
        HandlerOutcome::Remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, Result};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    /// Synthetic disc: a set of dead LBAs, an optional transport-fault LBA, and
    /// an injectable per-read time cost that advances a shared fake clock. No
    /// real sleeps — the clock is an `AtomicU64` of nanoseconds so the reader
    /// (which owns `&mut self`) and the `now` closure share one timeline while
    /// staying `Send`.
    struct FakeDisc {
        dead: HashSet<u32>,
        /// LBAs that return a wedge-family sense (IllegalRequest) — the drive
        /// fast-fail state, distinct from an ordinary dead sector (which carries
        /// no sense). Used to exercise wedge detection.
        wedge: HashSet<u32>,
        transport_at: Option<u32>,
        clock_nanos: Arc<AtomicU64>,
        per_read: Duration,
        reads: Arc<AtomicU64>,
        // ── Physical failure-mode models (all default-empty) ─────────────────
        // Each conditional sector reads ONLY when the drive state the handler
        // manipulates (speed / FUA / approach direction) matches — so a test
        // that recovers it PROVES the technique was actually exercised, not that
        // a plain read happened to work.
        /// Current `SET CD SPEED` value (updated by `set_speed`); max at build.
        speed: u16,
        /// Reads ONLY at min speed (fails at max) → SlowSpin / SpeedSweep.
        slow_only: HashSet<u32>,
        /// Reads ONLY on the Nth *physical* (FUA) attempt; a cached (non-FUA)
        /// re-read never gets it → FuaRetry. Maps LBA → attempts required.
        fua_need: HashMap<u32, u32>,
        /// Physical (FUA) attempts observed so far, per LBA.
        fua_seen: HashMap<u32, u32>,
        /// Reads ONLY when approached from ABOVE (the previous physical access
        /// was a higher LBA) → Oscillate's reverse-into pass.
        dir_reverse_only: HashSet<u32>,
        /// Reads ONLY when the immediately-preceding sector was the previous
        /// physical access (PLL/servo primed) → CachePrime.
        prime_only: HashSet<u32>,
        /// LBA of the last sector physically accessed (success or fail) — the
        /// approach-direction / priming signal the specialists drive.
        last_lba: Option<u32>,
    }

    impl SectorSource for FakeDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            // Bulk (non-FUA) path.
            self.read_sectors_fua(lba, count, buf, recovery, false)
        }

        fn read_sectors_fua(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
            fua: bool,
        ) -> Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.clock_nanos
                .fetch_add(self.per_read.as_nanos() as u64, Ordering::Relaxed);
            // The head moved across this span; record where it ended so the NEXT
            // read can see the approach direction / priming (both success and
            // failure move the head).
            let prev = self.last_lba;
            self.last_lba = Some(lba + count as u32 - 1);
            if let Some(t) = self.transport_at {
                if (lba..lba + count as u32).contains(&t) {
                    return Err(Error::ScsiError {
                        opcode: crate::scsi::SCSI_READ_10,
                        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                        sense: None,
                    });
                }
            }
            for l in lba..lba + count as u32 {
                if self.wedge.contains(&l) {
                    // Fast-fail wedge sense: ILLEGAL REQUEST / INVALID FIELD IN
                    // CDB (0x05/0x24), the real BU40N wedge signature. Non-
                    // transport status so it isn't caught as a bus fault, but
                    // carries sense so the wedge classifier sees it.
                    return Err(Error::ScsiError {
                        opcode: crate::scsi::SCSI_READ_10,
                        status: 0x02,
                        sense: Some(crate::scsi::ScsiSense {
                            sense_key: crate::scsi::SENSE_KEY_ILLEGAL_REQUEST,
                            asc: 0x24,
                            ascq: 0x00,
                        }),
                    });
                }
                if self.dead.contains(&l) {
                    // Non-transport bad-sector error (CHECK CONDITION, 0x02).
                    return Err(Error::DiscRead {
                        sector: l as u64,
                        status: Some(0x02),
                        sense: None,
                    });
                }
                // Marginal sector: reads only at min spindle speed.
                if self.slow_only.contains(&l) && self.speed != SPEED_MIN_KBS {
                    return Err(bad_sector(l));
                }
                // Stochastic sector: needs N physical (FUA) reads; a cached read
                // can never land it (cache masks the good re-read).
                if let Some(need) = self.fua_need.get(&l).copied() {
                    if !fua {
                        return Err(bad_sector(l));
                    }
                    let seen = self.fua_seen.entry(l).or_insert(0);
                    *seen += 1;
                    if *seen < need {
                        return Err(bad_sector(l));
                    }
                }
                // Direction-dependent tracking: reads only when approached from
                // above (previous physical access was a higher LBA).
                if self.dir_reverse_only.contains(&l) && prev.is_none_or(|p| p <= l) {
                    return Err(bad_sector(l));
                }
                // Boundary sector: reads only when the preceding sector was the
                // previous physical access (servo primed).
                if self.prime_only.contains(&l) && prev != l.checked_sub(1) {
                    return Err(bad_sector(l));
                }
            }
            let bytes = count as usize * SECTOR as usize;
            for (i, b) in buf[..bytes].iter_mut().enumerate() {
                *b = (lba as usize + i / SECTOR as usize) as u8;
            }
            Ok(bytes)
        }

        fn set_speed(&mut self, kbs: u16) {
            self.speed = kbs;
        }
    }

    /// The ordinary recoverable bad-sector error (CHECK CONDITION, no sense) the
    /// conditional failure modes return when their precondition isn't met.
    fn bad_sector(l: u32) -> Error {
        Error::DiscRead {
            sector: l as u64,
            status: Some(0x02),
            sense: None,
        }
    }

    /// Records every recovered span so a test can assert which sectors came back.
    #[derive(Default)]
    struct RecordSink {
        got: HashMap<u64, usize>, // pos -> bytes
    }
    impl RecoverySink for RecordSink {
        fn recovered(&mut self, pos: u64, buf: &[u8]) {
            self.got.insert(pos, buf.len());
        }
    }

    /// A fake clock plus a disc sharing its timeline.
    struct Harness {
        clock_nanos: Arc<AtomicU64>,
        reads: Arc<AtomicU64>,
        base: Instant,
    }

    impl Harness {
        fn build(dead: &[u32], transport_at: Option<u32>, per_read: Duration) -> (Self, FakeDisc) {
            let clock_nanos = Arc::new(AtomicU64::new(0));
            let reads = Arc::new(AtomicU64::new(0));
            let disc = FakeDisc {
                dead: dead.iter().copied().collect(),
                wedge: HashSet::new(),
                transport_at,
                clock_nanos: clock_nanos.clone(),
                per_read,
                reads: reads.clone(),
                speed: SPEED_MAX_KBS,
                slow_only: HashSet::new(),
                fua_need: HashMap::new(),
                fua_seen: HashMap::new(),
                dir_reverse_only: HashSet::new(),
                prime_only: HashSet::new(),
                last_lba: None,
            };
            (
                Harness {
                    clock_nanos,
                    reads,
                    base: Instant::now(),
                },
                disc,
            )
        }

        fn now_fn(&self) -> impl Fn() -> Instant {
            let c = self.clock_nanos.clone();
            let base = self.base;
            move || base + Duration::from_nanos(c.load(Ordering::Relaxed))
        }

        fn read_count(&self) -> u64 {
            self.reads.load(Ordering::Relaxed)
        }
    }

    fn lba(pos: u64) -> u32 {
        (pos / SECTOR) as u32
    }

    #[test]
    fn chain_recovers_readable_in_a_dead_batch_leaving_only_dead() {
        // Section [0, 10 sectors). Dead: sectors 3 and 7. Linear reads it as one
        // batch, which fails (it contains dead sectors), so Linear leaves the
        // whole batch bad — NO per-sector grind (that's the point of dropping
        // narrow_batch). Bisect then probes/expands and salvages the 8 readable
        // sectors, leaving ONLY 3 and 7. Proves the Linear→Bisect division of
        // labour: Linear sweeps at batch granularity, Bisect finds the islands.
        let dead = [3u32, 7u32];
        let (h, disc) = Harness::build(&dead, None, Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 10 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        // Linear leaves the failed 10-sector batch whole.
        Linear {
            direction: Direction::Forward,
            params: ReadParams::deep(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(
            bad.total_len(),
            10 * SECTOR,
            "linear leaves the dead batch whole"
        );
        // Bisect salvages the readable sectors around the dead ones.
        ctx.unproductive = 0;
        let out = Bisect {
            params: ReadParams::fast(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        // Exactly the two dead sectors remain.
        assert_eq!(bad.total_len(), 2 * SECTOR);
        for &(p, l) in bad.ranges() {
            assert_eq!(l, SECTOR);
            assert!(
                lba(p) == 3 || lba(p) == 7,
                "unexpected bad sector {}",
                lba(p)
            );
        }
    }

    #[test]
    fn linear_forward_front_dead_still_reaches_readable_tail() {
        // THE bug: front dead, tail readable. Section [0, 40 sectors). First 32
        // (one whole batch) are dead; the tail 8 are readable. Forward linear
        // must recover the tail — it does not hang at the front.
        let dead: Vec<u32> = (0..32).collect();
        let (h, disc) = Harness::build(&dead, None, Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 40 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            direction: Direction::Forward,
            params: ReadParams::deep(),
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        // The 32 dead front sectors remain; the 8-sector readable tail is
        // recovered as one clean batch (one sink span covering 8 sectors).
        assert_eq!(bad.total_len(), 32 * SECTOR);
        assert_eq!(sink.got.len(), 1, "tail is one clean 8-sector batch");
        assert_eq!(
            sink.got.get(&(32 * SECTOR)).copied(),
            Some(8 * SECTOR as usize),
            "tail batch not recovered"
        );
    }

    #[test]
    fn linear_honors_deadline_and_returns_promptly() {
        // 1000 clean sectors, but each read costs 1 s and the budget is 3 s. The
        // handler must stop after ~3 reads, NOT drain all 1000 — proving bounded
        // wall-clock even on a huge range.
        let (h, disc) = Harness::build(&[], None, Duration::from_secs(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 1000 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(3);
        let mut lin = Linear {
            direction: Direction::Forward,
            params: ReadParams::fast(),
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        // Batch=32 clean sectors per read: a handful of reads at most, not 1000.
        assert!(
            h.read_count() <= 5,
            "ran {} reads, expected <=5",
            h.read_count()
        );
        assert!(bad.total_len() > 0, "should not have drained the range");
    }

    #[test]
    fn bisect_finds_good_middle_in_mostly_dead_range() {
        // 9 sectors, only the middle (sector 4) readable. Bisect probes the
        // middle first, recovers it, and the recursive halves' middles are dead.
        let dead: Vec<u32> = (0..9).filter(|&l| l != 4).collect();
        let (h, disc) = Harness::build(&dead, None, Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 9 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut bis = Bisect {
            params: ReadParams::fast(),
        };
        let out = bis.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        assert!(
            sink.got.contains_key(&(4 * SECTOR)),
            "good middle not found"
        );
        assert_eq!(
            bad.total_len(),
            8 * SECTOR,
            "only the middle should recover"
        );
    }

    #[test]
    fn coordinator_reverse_then_forward_makes_progress_direction_matters() {
        // Two dead sectors at opposite ends won't both be cleared by one
        // direction alone in this contrived fixture, but the CHAIN clears every
        // readable sector regardless of order. Prove the coordinator runs
        // handler after handler and drains the readable set.
        let dead = [0u32, 15u32]; // ends of a 16-sector section
        let (h, disc) = Harness::build(&dead, None, Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 16 * SECTOR);
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![
            Box::new(Linear {
                direction: Direction::Reverse,
                params: ReadParams::deep(),
            }),
            Box::new(Linear {
                direction: Direction::Forward,
                params: ReadParams::deep(),
            }),
            Box::new(Bisect {
                params: ReadParams::fast(),
            }),
        ];
        let deadline_base = (ctx.now)();
        let mut scoreboard = HandlerScoreboard::default();
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, &mut scoreboard, |_| {
            deadline_base + Duration::from_secs(30)
        });
        assert_eq!(out, HandlerOutcome::Remaining);
        // 14 readable sectors recovered, only the two dead ends remain.
        assert_eq!(bad.total_len(), 2 * SECTOR);
        for &(p, _) in bad.ranges() {
            assert!(lba(p) == 0 || lba(p) == 15);
        }
    }

    #[test]
    fn coordinator_completes_when_no_dead_sectors() {
        // A clean section drains to Complete on the first handler.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 64 * SECTOR);
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![Box::new(Linear {
            direction: Direction::Forward,
            params: ReadParams::fast(),
        })];
        let base = (ctx.now)();
        let mut scoreboard = HandlerScoreboard::default();
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, &mut scoreboard, |_| {
            base + Duration::from_secs(30)
        });
        assert_eq!(out, HandlerOutcome::Complete);
        assert!(bad.is_empty());
    }

    #[test]
    fn transport_fault_short_circuits() {
        // A transport fault mid-range returns TransportFault immediately so the
        // caller can un-wedge the drive.
        let (h, disc) = Harness::build(&[], Some(5), Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        // Single-sector batches so the transport LBA is hit directly.
        let mut bad = SubRanges::from_section(0, 8 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            direction: Direction::Forward,
            params: ReadParams::fast(),
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::TransportFault);
    }

    #[test]
    fn wedged_drive_aborts_fast_instead_of_grinding() {
        // Regression for the 2026-07-01 incident: a fast-fail wedge (drive
        // returns ILLEGAL REQUEST on every CDB) was classified as an ordinary
        // bad sector, so the chain ground a dead drive for 28 min at 0 B/s.
        // Now a sustained run of wedge-family senses escalates to TransportFault
        // so the pass aborts and the caller spin-cycles. A big section (1000
        // sectors) that is ENTIRELY wedged must bail after ~WEDGE_ABORT_STREAK
        // reads, not after reading the whole thing.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        disc.wedge = (0..1000u32).collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 1000 * SECTOR);
        // The full tier-0 chain: the wedge streak persists across handlers (only
        // `unproductive` resets per handler), so it reaches the abort threshold
        // even though each handler yields early on the dead streak.
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![
            Box::new(Bisect {
                params: ReadParams::fast(),
            }),
            Box::new(Jump {
                params: ReadParams::fast(),
            }),
            Box::new(Linear {
                direction: Direction::Reverse,
                params: ReadParams::fast(),
            }),
            Box::new(Linear {
                direction: Direction::Forward,
                params: ReadParams::fast(),
            }),
        ];
        let mut scoreboard = HandlerScoreboard::default();
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, &mut scoreboard, |_| {
            (h.now_fn())() + Duration::from_secs(60)
        });
        assert_eq!(
            out,
            HandlerOutcome::TransportFault,
            "a wholly-wedged section must escalate to TransportFault"
        );
        // The whole point: it bailed after a short streak, not after grinding all
        // 1000 sectors. Generous bound (handlers read in batches) but far below
        // the section size.
        assert!(
            h.read_count() < 100,
            "wedge must abort fast; did {} reads on a 1000-sector wedged section",
            h.read_count()
        );
    }

    #[test]
    fn slow_hardware_error_media_does_not_false_trip_wedge_abort() {
        // A genuine uncorrectable sector on Hardware-error media reports a
        // wedge-FAMILY sense (IllegalRequest here) but comes back SLOW — the drive
        // spent real time on ECC recovery before failing. That must NOT count
        // toward the wedge abort (which targets the drive's <100ms fast-fail
        // rejection). Each read here costs 600ms (> WEDGE_FASTFAIL_MS), so even a
        // wholly-"wedge-sense" section never escalates to TransportFault — it just
        // leaves the residue bad for the next pass, exactly like ordinary damage.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(600));
        let mut disc = disc;
        disc.wedge = (0..1000u32).collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: None,
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 1000 * SECTOR);
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![
            Box::new(Bisect {
                params: ReadParams::fast(),
            }),
            Box::new(Jump {
                params: ReadParams::fast(),
            }),
            Box::new(Linear {
                direction: Direction::Reverse,
                params: ReadParams::fast(),
            }),
            Box::new(Linear {
                direction: Direction::Forward,
                params: ReadParams::fast(),
            }),
        ];
        let mut scoreboard = HandlerScoreboard::default();
        // Long per-handler deadline so the deadline (not the wedge) is never the
        // reason a handler stops — we're isolating the wedge-escalation decision.
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, &mut scoreboard, |_| {
            (h.now_fn())() + Duration::from_secs(3600)
        });
        assert_ne!(
            out,
            HandlerOutcome::TransportFault,
            "slow (ECC-recovery) Hardware-error reads must NOT trip the fast-fail wedge abort"
        );
        assert_eq!(
            ctx.wedge_streak, 0,
            "slow wedge-family reads must not accumulate the streak"
        );
    }

    #[test]
    fn wedge_streak_persists_across_sections_for_tier1() {
        // Tier 1 is only TWO handlers, so one wedged section builds at most
        // 2 × UNPRODUCTIVE_YIELD = 8 streak — below WEDGE_ABORT_STREAK (16). The
        // wedge is caught only because the pass-level wedge_streak PERSISTS across
        // sections. Simulate what PatchCtx does: carry wedge_streak in/out of each
        // per-section run_handlers call, and assert the abort lands on a LATER
        // section, not the first.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        disc.wedge = (0..4000u32).collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut carried = 0u32; // the pass-level wedge_streak
        let mut caught_on: Option<usize> = None;
        for section in 0..6usize {
            let mut ctx = HandlerCtx {
                reader: &mut disc,
                sink: &mut sink,
                now: &now,
                halt: None,
                decrypt_is_aacs: false,
                tick: None,
                unproductive: 0,
                wedge_streak: carried,
                cur_speed: SPEED_MAX_KBS,
            };
            // Distinct 100-sector section per iteration, all within the wedge set.
            let pos = (section as u64) * 100 * SECTOR;
            let mut bad = SubRanges::from_section(pos, 100 * SECTOR);
            // Tier-1 shape: two slow Linear handlers, nothing that reaches 16 alone.
            let mut handlers: Vec<Box<dyn SectionHandler>> = vec![
                Box::new(Linear {
                    direction: Direction::Reverse,
                    params: ReadParams::deep(),
                }),
                Box::new(Linear {
                    direction: Direction::Forward,
                    params: ReadParams::deep(),
                }),
            ];
            let mut sb = HandlerScoreboard::default();
            let out = run_handlers(&mut ctx, &mut handlers, &mut bad, &mut sb, |_| {
                (h.now_fn())() + Duration::from_secs(60)
            });
            carried = ctx.wedge_streak;
            if out == HandlerOutcome::TransportFault {
                caught_on = Some(section);
                break;
            }
        }
        let caught = caught_on.expect("a two-handler tier must still catch the wedge");
        assert!(
            caught >= 1,
            "one 2-handler section can't reach the streak alone; the wedge must be \
             caught via cross-section accumulation, not on section 0 (caught on {caught})"
        );
    }

    #[test]
    fn halt_token_returns_promptly() {
        // Halt set before the call: the handler returns Halted on its first
        // check, having done no reads.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let halt = AtomicBool::new(true);
        let mut ctx = HandlerCtx {
            reader: &mut disc,
            sink: &mut sink,
            now: &now,
            halt: Some(&halt),
            decrypt_is_aacs: false,
            tick: None,
            unproductive: 0,
            wedge_streak: 0,
            cur_speed: SPEED_MAX_KBS,
        };
        let mut bad = SubRanges::from_section(0, 100 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            direction: Direction::Forward,
            params: ReadParams::fast(),
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Halted);
        assert_eq!(h.read_count(), 0, "halt must precede any read");
    }

    #[test]
    fn scorecard_decays_so_a_late_starter_overtakes_an_early_winner() {
        // The whole point of the DECAYED rate: the residual hardens mid-pass, so
        // leadership must hand off. A cumulative rate would freeze "early" in the
        // lead forever; the EWMA re-prices continuously.
        let mut sb = HandlerScoreboard::default();
        let dt = Duration::from_secs(1);

        // Round 1 — "early" cleans the easy bulk; "late" finds nothing yet.
        sb.record("early", 1_000_000, dt);
        sb.record("late", 0, dt);
        assert!(
            sb.rank("early") > sb.rank("late"),
            "early must lead once it's the only one recovering"
        );

        // The bulk is gone. Now "early"'s technique no longer fits the hardened
        // residual (barren attempts) while "late"'s specialist starts winning.
        for _ in 0..4 {
            sb.record("early", 0, dt);
            sb.record("late", 1_000_000, dt);
        }
        assert!(
            sb.rank("late") > sb.rank("early"),
            "a handler that stops earning must LOSE its lead to a late starter \
             (late={}, early={})",
            sb.rank("late"),
            sb.rank("early")
        );

        // Calibration invariants preserved: an untried handler still ranks top
        // (one-shot calibration), and a handler attempted with no timed read
        // (zero elapsed) ranks bottom rather than crowding out proven performers.
        assert_eq!(sb.rank("never_tried"), u64::MAX, "untried → top");
        sb.record("idle", 0, Duration::ZERO);
        assert_eq!(sb.rank("idle"), 0, "attempted-but-zero-time → bottom");
    }

    /// Build a ctx over `disc` with the fake clock — the common per-test setup.
    macro_rules! ctx {
        ($h:expr, $disc:expr, $sink:expr, $now:expr) => {
            HandlerCtx {
                reader: &mut $disc,
                sink: &mut $sink,
                now: &$now,
                halt: None,
                decrypt_is_aacs: false,
                tick: None,
                unproductive: 0,
                wedge_streak: 0,
                cur_speed: SPEED_MAX_KBS,
            }
        };
    }

    fn min_deep() -> ReadParams {
        ReadParams {
            speed: SpeedPref::Min,
            fua: false,
            timeout: TimeoutPref::Deep,
        }
    }

    #[test]
    fn slow_spin_recovers_a_min_speed_only_sector_that_max_linear_misses() {
        // Sector 5 reads ONLY at min spindle speed (weak signal / servo drift):
        // a max-speed deep Linear leaves it bad; SlowSpin (Linear pinned to min)
        // recovers it. Single-sector residual so Linear reads it directly.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        disc.slow_only = [5u32].into_iter().collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = ctx!(h, disc, sink, now);
        let mut bad = SubRanges::from_section(5 * SECTOR, SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(30);

        // Max-speed deep Linear cannot read a min-only sector.
        let out = Linear {
            direction: Direction::Forward,
            params: ReadParams::deep(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        assert_eq!(bad.total_len(), SECTOR, "max-speed linear must leave it bad");

        // SlowSpin = Linear at min speed — recovers it.
        let out = Linear {
            direction: Direction::Forward,
            params: min_deep(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Complete);
        assert!(bad.is_empty(), "SlowSpin must recover the min-only sector");
        assert_eq!(sink.got.get(&(5 * SECTOR)).copied(), Some(SECTOR as usize));
    }

    #[test]
    fn speed_sweep_recovers_a_min_speed_only_sector() {
        // SpeedSweep sweeps Max→Min per sector, so it reaches the min-only
        // sector 7 that a max-only read never gets — proving the sweep actually
        // drops the spindle when the fast read fails.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        disc.slow_only = [7u32].into_iter().collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = ctx!(h, disc, sink, now);
        let mut bad = SubRanges::from_section(7 * SECTOR, SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(30);

        let out = SpeedSweep {
            params: ReadParams::deep(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Complete);
        assert!(bad.is_empty(), "SpeedSweep must reach min and recover it");
        assert_eq!(sink.got.get(&(7 * SECTOR)).copied(), Some(SECTOR as usize));
        // It tried the fast (max) read first, then the min read — 2 reads.
        assert_eq!(h.read_count(), 2, "swept max then min");
    }

    fn max_fua_deep() -> ReadParams {
        ReadParams {
            speed: SpeedPref::Max,
            fua: true,
            timeout: TimeoutPref::Deep,
        }
    }

    #[test]
    fn fua_retry_recovers_a_stochastic_sector_a_cached_read_keeps_missing() {
        // Sector 9 lands only on its 2nd PHYSICAL (FUA) read; a cached (non-FUA)
        // re-read never gets it (the cache masks the good re-read). FuaRetry =
        // the Linear fwd + rev + Bisect group at FUA params: across its reads the
        // sector gets enough physical attempts to land, where cached reads can't.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        disc.fua_need = [(9u32, 2u32)].into_iter().collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = ctx!(h, disc, sink, now);
        let mut bad = SubRanges::from_section(9 * SECTOR, SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(30);

        // Cached (non-FUA) reads keep missing — twice, and the sector stays bad
        // (a cached miss never even counts as a physical attempt).
        for _ in 0..2 {
            let out = Linear {
                direction: Direction::Forward,
                params: ReadParams::deep(),
            }
            .recover(&mut ctx, &mut bad, deadline);
            assert_eq!(out, HandlerOutcome::Remaining);
            assert_eq!(bad.total_len(), SECTOR, "cached read must keep missing");
        }

        // FuaRetry group: Linear fwd (FUA attempt 1) leaves it, Linear rev (FUA
        // attempt 2) lands it.
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![
            Box::new(Linear {
                direction: Direction::Forward,
                params: max_fua_deep(),
            }),
            Box::new(Linear {
                direction: Direction::Reverse,
                params: max_fua_deep(),
            }),
            Box::new(Bisect {
                params: max_fua_deep(),
            }),
        ];
        let mut sb = HandlerScoreboard::default();
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, &mut sb, |_| deadline);
        assert_eq!(out, HandlerOutcome::Complete);
        assert!(bad.is_empty(), "FuaRetry must land the stochastic sector");
        assert_eq!(sink.got.get(&(9 * SECTOR)).copied(), Some(SECTOR as usize));
    }

    #[test]
    fn slow_fua_recovers_the_hardest_sector_needing_both_min_and_fua() {
        // Sector 11 is the hardest case: it reads ONLY at min speed AND ONLY on a
        // physical (FUA) read. Neither lever alone works — SlowFua (Linear at
        // {min, fua, deep}) is the combination that recovers it.
        let (h, disc) = Harness::build(&[], None, Duration::from_millis(1));
        let mut disc = disc;
        disc.slow_only = [11u32].into_iter().collect();
        disc.fua_need = [(11u32, 1u32)].into_iter().collect();
        let mut sink = RecordSink::default();
        let now = h.now_fn();
        let mut ctx = ctx!(h, disc, sink, now);
        let mut bad = SubRanges::from_section(11 * SECTOR, SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(30);

        // FUA but max speed → wrong speed, fails.
        let out = Linear {
            direction: Direction::Forward,
            params: max_fua_deep(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        assert_eq!(bad.total_len(), SECTOR, "max+fua must miss the min-only sector");

        // Min speed but cached (no FUA) → no physical attempt, fails.
        let out = Linear {
            direction: Direction::Forward,
            params: min_deep(),
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Remaining);
        assert_eq!(bad.total_len(), SECTOR, "min+cached must miss the FUA-only sector");

        // Both levers: min speed AND FUA → recovers.
        let out = Linear {
            direction: Direction::Forward,
            params: ReadParams {
                speed: SpeedPref::Min,
                fua: true,
                timeout: TimeoutPref::Deep,
            },
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Complete);
        assert!(bad.is_empty(), "SlowFua (min+fua) must recover the hardest sector");
        assert_eq!(sink.got.get(&(11 * SECTOR)).copied(), Some(SECTOR as usize));
    }
}
