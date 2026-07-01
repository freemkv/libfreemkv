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

/// `Jump` handler: after this many consecutive failed batches, skip ahead to
/// find where readable data resumes rather than reading every dead sector.
const JUMP_AFTER_FAILS: u32 = 2;
/// `Jump` initial skip distance; doubles after each jump, capped at
/// [`JUMP_CAP_BYTES`]. Mirrors the escalating Pass-1 damage-jump.
const JUMP_BASE_BYTES: u64 = 1 << 20; // 1 MiB
const JUMP_CAP_BYTES: u64 = 256 << 20; // 256 MiB

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
}

impl HandlerCtx<'_> {
    fn halted(&self) -> bool {
        self.halt.is_some_and(|h| h.load(Ordering::Relaxed))
    }

    fn past(&self, deadline: Instant) -> bool {
        (self.now)() >= deadline
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
    match recovery_read(ctx.reader, ctx.decrypt_is_aacs, lba, count, buf, recovery) {
        Ok(_) => {
            ctx.sink.recovered(pos, &buf[..bytes]);
            ReadHit::Good
        }
        Err(e) if e.is_scsi_transport_failure() => ReadHit::Transport,
        Err(_) => ReadHit::Bad,
    }
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

impl Linear {
    /// A batch read failed as a unit — retry it sector-by-sector so the readable
    /// sectors of a partially-dead batch are still recovered and only the dead
    /// ones stay bad. Bounded: at most `span_bytes / SECTOR` single reads.
    fn narrow_batch(
        &self,
        ctx: &mut HandlerCtx,
        bad: &mut SubRanges,
        deadline: Instant,
        pos: u64,
        span_bytes: u64,
    ) -> Option<HandlerOutcome> {
        let recovery = !self.fast;
        let mut buf = [0u8; SECTOR as usize];
        let mut off = 0;
        while off < span_bytes {
            if ctx.halted() {
                return Some(HandlerOutcome::Halted);
            }
            if ctx.past(deadline) {
                return Some(HandlerOutcome::Remaining);
            }
            let spos = pos + off;
            match read_span(ctx, &mut buf, spos, 1, recovery) {
                ReadHit::Good => bad.remove(spos, SECTOR),
                ReadHit::Bad => {}
                ReadHit::Transport => return Some(HandlerOutcome::TransportFault),
            }
            off += SECTOR;
        }
        None
    }
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
                    ReadHit::Bad => {
                        // Recover the readable sectors inside the dead batch,
                        // leave the truly-dead ones bad, and keep moving.
                        if let Some(o) = self.narrow_batch(ctx, bad, deadline, pos, span) {
                            return o;
                        }
                    }
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

/// Probe the MIDDLE sector of each bad sub-range; if it reads, remove it and
/// recurse on the two halves to converge on good centers. If the middle is dead,
/// leave that chunk for another handler / pass. Finds islands of readable data
/// inside a mostly-dead range that a linear sweep would tar with one failing
/// batch.
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
        let mut buf = [0u8; SECTOR as usize];
        // Explicit work stack of (pos, len) chunks still to probe. Each good
        // probe removes one sector and pushes its two halves; each read consumes
        // a sector, so the stack drains in bounded steps.
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
            // Middle sector, floored to a sector boundary.
            let sectors = rl / SECTOR;
            let mid = rp + (sectors / 2) * SECTOR;
            match read_span(ctx, &mut buf, mid, 1, true) {
                ReadHit::Good => {
                    bad.remove(mid, SECTOR);
                    // Left half [rp, mid), right half [mid+SECTOR, rp+rl).
                    if mid > rp {
                        stack.push((rp, mid - rp));
                    }
                    let right = mid + SECTOR;
                    if right < rp + rl {
                        stack.push((right, rp + rl - right));
                    }
                }
                // Dead middle: leave the chunk bad and move on.
                ReadHit::Bad => {}
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
            let mut jump = JUMP_BASE_BYTES;
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
                        jump = JUMP_BASE_BYTES;
                        off += span;
                    }
                    ReadHit::Bad => {
                        consec_fail += 1;
                        if consec_fail >= JUMP_AFTER_FAILS {
                            // Sustained dead run — skip ahead (sector-aligned so
                            // the walk stays batch-aligned) and escalate the next
                            // jump. The skipped span stays bad for Bisect / a
                            // later handler to pin the boundary.
                            let step = (jump / SECTOR).max(1) * SECTOR;
                            off = (off + step).min(rl);
                            jump = jump.saturating_mul(2).min(JUMP_CAP_BYTES);
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

/// Run the handler chain over one section's still-bad set. This is the
/// never-hang guarantee: each handler is bounded by the deadline
/// `section_deadline_for(bad)` returns, and the loop always drains to
/// `Complete`/`Remaining` (whatever is still bad is the caller's residue to
/// record as loss). `Halted` / `TransportFault` short-circuit so the caller can
/// abort or un-wedge.
pub(super) fn run_handlers(
    ctx: &mut HandlerCtx,
    handlers: &mut [Box<dyn SectionHandler>],
    bad: &mut SubRanges,
    section_deadline_for: impl Fn(&SubRanges) -> Instant,
) -> HandlerOutcome {
    for handler in handlers.iter_mut() {
        if bad.is_empty() {
            return HandlerOutcome::Complete;
        }
        let before = bad.total_len();
        let deadline = section_deadline_for(bad);
        let outcome = handler.recover(ctx, bad, deadline);
        tracing::info!(
            target: "freemkv::disc",
            phase = "section_recover.handler",
            handler = handler.name(),
            bad_bytes_before = before,
            bad_bytes_after = bad.total_len(),
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
    fn linear_forward_recovers_all_readable_and_leaves_only_dead() {
        // Section [0, 10 sectors). Dead: sectors 3 and 7. Forward linear must
        // recover the other 8 and leave ONLY 3 and 7 bad — proving it moves past
        // a dead sector instead of stalling on it. Batch=1-effective here since
        // the dead sectors force the narrow path; use a small section.
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
        };
        let mut bad = SubRanges::from_section(0, 10 * SECTOR);
        // Generous deadline: 10 s from start.
        let deadline = (ctx.now)() + Duration::from_secs(10);
        let mut lin = Linear {
            reverse: false,
            fast: false,
        };
        let out = lin.recover(&mut ctx, &mut bad, deadline);
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
        // All eight readable sectors were handed to the sink.
        assert_eq!(sink.got.len(), 8);
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
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, |_| {
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
        };
        let mut bad = SubRanges::from_section(0, 64 * SECTOR);
        let mut handlers: Vec<Box<dyn SectionHandler>> = vec![Box::new(Linear {
            reverse: false,
            fast: true,
        })];
        let base = (ctx.now)();
        let out = run_handlers(&mut ctx, &mut handlers, &mut bad, |_| {
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
