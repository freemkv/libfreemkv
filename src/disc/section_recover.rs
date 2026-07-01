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
    recovery: bool,
) -> ReadHit {
    let lba = (pos / SECTOR) as u32;
    let bytes = count as usize * SECTOR as usize;
    let hit = match recovery_read(ctx.reader, ctx.decrypt_is_aacs, lba, count, buf, recovery) {
        Ok(_) => {
            ctx.sink.recovered(pos, &buf[..bytes]);
            ReadHit::Good
        }
        Err(e) if e.is_scsi_transport_failure() => ReadHit::Transport,
        Err(_) => ReadHit::Bad,
    };
    // Track the dead streak for the early-yield hand-off: a recovering read
    // resets it, a fruitless one advances it toward UNPRODUCTIVE_YIELD.
    match hit {
        ReadHit::Good => ctx.unproductive = 0,
        _ => ctx.unproductive = ctx.unproductive.saturating_add(1),
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
    fn name(&self) -> &'static str;
    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome;
}

/// Linear sweep of each bad sub-range. `reverse` walks end→start (the disc
/// sweep overshoots forward, so a NonTrimmed range's good data sits at its tail
/// — reverse hits it first); `!reverse` walks start→end (the front the reverse
/// pass kept dying on). `fast` selects the single-attempt read (`recovery =
/// false`) over the 60 s deep-recovery read. The two bools give backwards /
/// forwards / fast / slow from one handler.
pub(super) struct Linear {
    pub reverse: bool,
    pub fast: bool,
}

impl SectionHandler for Linear {
    fn name(&self) -> &'static str {
        match (self.reverse, self.fast) {
            (true, true) => "linear:reverse:fast",
            (true, false) => "linear:reverse:slow",
            (false, true) => "linear:forward:fast",
            (false, false) => "linear:forward:slow",
        }
    }

    fn recover(
        &mut self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
    ) -> HandlerOutcome {
        let recovery = !self.fast;
        let batch_bytes = BATCH_SECTORS * SECTOR;
        let mut buf = vec![0u8; batch_bytes as usize];
        // Snapshot the sub-ranges: we mutate `bad` via remove() as we recover,
        // and iterating the snapshot keeps that from disturbing the walk.
        let mut snapshot: Vec<(u64, u64)> = bad.ranges().to_vec();
        if self.reverse {
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
                let pos = if self.reverse {
                    rp + (rl - done - span)
                } else {
                    rp + done
                };
                let count = (span / SECTOR) as u16;
                match read_span(ctx, &mut buf, pos, count, recovery) {
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
/// instead of leaving the whole thing bad. Uses fast reads: it LOCATES readable
/// data; deep-recovering the dead sectors is the slow linear handlers' job.
pub(super) struct Bisect;

impl SectionHandler for Bisect {
    fn name(&self) -> &'static str {
        "bisect"
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
            match read_span(ctx, &mut probe, mid, 1, false) {
                ReadHit::Good => {
                    bad.remove(mid, SECTOR);
                    // Expand FORWARD from mid+1 in batches until a read fails.
                    let mut fwd = mid + SECTOR;
                    let mut step = batch;
                    while fwd < end {
                        if ctx.timed_out(deadline) {
                            return HandlerOutcome::Remaining;
                        }
                        let span = step.min(end - fwd);
                        let count = (span / SECTOR) as u16;
                        match read_span(ctx, &mut buf[..span as usize], fwd, count, false) {
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
                        if ctx.timed_out(deadline) {
                            return HandlerOutcome::Remaining;
                        }
                        let span = step.min(bwd - rp);
                        let pos = bwd - span;
                        let count = (span / SECTOR) as u16;
                        match read_span(ctx, &mut buf[..span as usize], pos, count, false) {
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
pub(super) struct Jump;

impl SectionHandler for Jump {
    fn name(&self) -> &'static str {
        "jump"
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
                match read_span(ctx, &mut buf[..span as usize], pos, count, false) {
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

/// Per-rip handler scorecard. Grades each handler by the recovery RATE it has
/// achieved so far (bytes recovered per second of wall time) so the coordinator
/// runs the best-performing handler FIRST on later sections and lets a proven
/// dud fall to the back. Ephemeral — reset each rip, no persistence. A handler
/// not yet tried ranks top (`u64::MAX`) so every handler is calibrated once
/// before the ranking narrows to the winners ("try each quick, then prioritise").
#[derive(Default)]
pub(super) struct HandlerScoreboard {
    stats: std::collections::HashMap<&'static str, ScoreStat>,
}

#[derive(Default, Clone, Copy)]
struct ScoreStat {
    recovered: u64,
    nanos: u128,
    attempts: u64,
}

impl HandlerScoreboard {
    fn rate(s: &ScoreStat) -> u64 {
        if s.nanos == 0 {
            0
        } else {
            ((s.recovered as u128 * 1_000_000_000) / s.nanos).min(u64::MAX as u128) as u64
        }
    }

    /// Record one attempt: bytes recovered over `elapsed`.
    fn record(&mut self, name: &'static str, recovered: u64, elapsed: std::time::Duration) {
        let e = self.stats.entry(name).or_default();
        e.recovered = e.recovered.saturating_add(recovered);
        e.nanos = e.nanos.saturating_add(elapsed.as_nanos());
        e.attempts += 1;
    }

    /// Ranking key (higher runs earlier). Untried → top, so it gets calibrated.
    fn rank(&self, name: &str) -> u64 {
        match self.stats.get(name) {
            None => u64::MAX,
            Some(s) if s.nanos == 0 => u64::MAX,
            Some(s) => Self::rate(s),
        }
    }

    /// Emit the scorecard to the log so the operator can see, per rip, which
    /// handler is pulling the weight and which is a dud on this drive/disc.
    pub(super) fn log(&self) {
        let mut rows: Vec<_> = self.stats.iter().collect();
        rows.sort_by_key(|(_, s)| std::cmp::Reverse(Self::rate(s)));
        for (name, s) in rows {
            let mbps = s.recovered as f64 / (s.nanos as f64 / 1e9).max(1e-9) / 1_048_576.0;
            tracing::info!(
                target: "freemkv::disc",
                phase = "scorecard",
                handler = *name,
                recovered_mb = s.recovered as f64 / 1_048_576.0,
                attempts = s.attempts,
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
    handlers.sort_by_key(|h| std::cmp::Reverse(scoreboard.rank(h.name())));
    for handler in handlers.iter_mut() {
        if bad.is_empty() {
            return HandlerOutcome::Complete;
        }
        let before = bad.total_len();
        let deadline = section_deadline_for(bad);
        let started = (ctx.now)();
        // Fresh dead-streak budget per handler: each gets its own chance before
        // the early-yield trips.
        ctx.unproductive = 0;
        let outcome = handler.recover(ctx, bad, deadline);
        let elapsed = (ctx.now)().duration_since(started);
        let after = bad.total_len();
        scoreboard.record(handler.name(), before.saturating_sub(after), elapsed);
        tracing::info!(
            target: "freemkv::disc",
            phase = "section_recover.handler",
            handler = handler.name(),
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
        transport_at: Option<u32>,
        clock_nanos: Arc<AtomicU64>,
        per_read: Duration,
        reads: Arc<AtomicU64>,
    }

    impl SectorSource for FakeDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.clock_nanos
                .fetch_add(self.per_read.as_nanos() as u64, Ordering::Relaxed);
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
                if self.dead.contains(&l) {
                    // Non-transport bad-sector error (CHECK CONDITION, 0x02).
                    return Err(Error::DiscRead {
                        sector: l as u64,
                        status: Some(0x02),
                        sense: None,
                    });
                }
            }
            let bytes = count as usize * SECTOR as usize;
            for (i, b) in buf[..bytes].iter_mut().enumerate() {
                *b = (lba as usize + i / SECTOR as usize) as u8;
            }
            Ok(bytes)
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
                transport_at,
                clock_nanos: clock_nanos.clone(),
                per_read,
                reads: reads.clone(),
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
        };
        let mut bad = SubRanges::from_section(0, 10 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        // Linear leaves the failed 10-sector batch whole.
        Linear {
            reverse: false,
            fast: false,
        }
        .recover(&mut ctx, &mut bad, deadline);
        assert_eq!(
            bad.total_len(),
            10 * SECTOR,
            "linear leaves the dead batch whole"
        );
        // Bisect salvages the readable sectors around the dead ones.
        ctx.unproductive = 0;
        let out = Bisect.recover(&mut ctx, &mut bad, deadline);
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
        };
        let mut bad = SubRanges::from_section(0, 40 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            reverse: false,
            fast: false,
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
        };
        let mut bad = SubRanges::from_section(0, 1000 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(3);
        let mut lin = Linear {
            reverse: false,
            fast: true,
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
        };
        let mut bad = SubRanges::from_section(0, 9 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut bis = Bisect;
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
        };
        let mut bad = SubRanges::from_section(0, 16 * SECTOR);
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![
            Box::new(Linear {
                reverse: true,
                fast: false,
            }),
            Box::new(Linear {
                reverse: false,
                fast: false,
            }),
            Box::new(Bisect),
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
        };
        let mut bad = SubRanges::from_section(0, 64 * SECTOR);
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![Box::new(Linear {
            reverse: false,
            fast: true,
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
        };
        // Single-sector batches so the transport LBA is hit directly.
        let mut bad = SubRanges::from_section(0, 8 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            reverse: false,
            fast: true,
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::TransportFault);
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
        };
        let mut bad = SubRanges::from_section(0, 100 * SECTOR);
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            reverse: false,
            fast: true,
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
        assert_eq!(out, HandlerOutcome::Halted);
        assert_eq!(h.read_count(), 0, "halt must precede any read");
    }
}
