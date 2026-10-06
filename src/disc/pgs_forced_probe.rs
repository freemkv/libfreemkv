//! Content-based forced-subtitle detection for Blu-ray/UHD PGS tracks.
//!
//! Gives `freemkv info` the muxer's own verdict up front, by reading the title's PGS streams
//! through the shared [`crate::mux::codec::pgs::ForcedTracker`] classifier. A track is forced
//! iff EVERY display set carries `forced_on_flag`; the read budget ([`PROBE_BUDGET_SECTORS`])
//! is SPREAD over each extent as sample windows ([`plan_windows`]), and content may CLEAR
//! (never set) a vendor flag behind [`crate::mux::codec::pgs::demotable`]. [`StopReason`]
//! narrows what a truncated read may assert.

use crate::disc::{Codec, DiscTitle, Stream};
use crate::mux::codec::CodecParser;
use crate::mux::codec::pgs::{ForcedTracker, PgsParser};
use crate::mux::ts::TsDemuxer;
use crate::sector::SectorSource;
use std::collections::HashMap;

use crate::consts::SECTOR_BYTES;
// Read the clip in ~2 MiB chunks: a whole number of AACS aligned units (3 sectors / 6144 B;
// 1023 = 341 units).
const CHUNK_SECTORS: u16 = 1023;

// The alignment requirement above is enforced, not just described.
const _: () = assert!(
    (CHUNK_SECTORS as u32).is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS),
    "probe chunks must be a whole number of AACS aligned units"
);

// Retries for a read that came back short of one AACS aligned unit before the run is declared
// truncated (`ReadFailed`, inconclusive).
const STALL_RETRY_LIMIT: u32 = 2;

// Hard ceiling on sectors read per probe call (256 MiB) — the same total the old head-first
// design used, now SPREAD via `plan_windows` instead of spent on the title's first 27 seconds.
pub(super) const PROBE_BUDGET_SECTORS: u32 = 131_072;

// Display sets a SAMPLED run must see on a track before "all forced" may be asserted: one hit
// alone can wrongly promote a mostly-unflagged track.
pub(super) const PROMOTE_MIN_DISPLAY_SETS: u32 = 2;

// One sample window: ~32 MiB, a whole number of AACS aligned units (16_383 = 5461 units). Sized
// against measured subtitle density.
const WINDOW_SECTORS: u32 = 16_383;

// Floor on a window (2 MiB): below this a window is too short to likely hit a display set.
const MIN_WINDOW_SECTORS: u32 = CHUNK_SECTORS as u32;

// Most windows spent on a single extent: past this, extra windows cost a seek each for no extra
// expected observations.
const MAX_WINDOWS_PER_EXTENT: u32 = 8;

// Windows must start (and, so that every chunk inside them does too, be sized)
// on the AACS aligned-unit grid — same requirement as CHUNK_SECTORS.
const _: () = assert!(
    WINDOW_SECTORS.is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS),
    "a sample window must be a whole number of AACS aligned units"
);

// One sampled run of sectors inside an extent: `offset` from the extent's
// `start_lba`, `len` sectors long. Both are whole AACS aligned units, so
// every read inside stays on the unit grid the decrypting source demands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct SampleWindow {
    pub(super) offset: u32,
    pub(super) len: u32,
}

/// Round DOWN to the AACS aligned-unit grid.
fn align_down(sectors: u32) -> u32 {
    sectors - sectors % crate::aacs::content::ALIGNED_UNIT_SECTORS
}

// Where to read inside one extent, given the sector budget `share`: a pure function of
// `(sector_count, share)` so the per-extent memo is reproducible.
pub(super) fn plan_windows(sector_count: u32, share: u32) -> Vec<SampleWindow> {
    if sector_count == 0 {
        return Vec::new();
    }
    // Cheap enough to read outright — the complete answer, and the case every
    // small extent (menus, clips shorter than the share) takes.
    if sector_count <= share {
        return vec![SampleWindow {
            offset: 0,
            len: sector_count,
        }];
    }
    let windows = (share / WINDOW_SECTORS).clamp(1, MAX_WINDOWS_PER_EXTENT);
    let len = align_down((share / windows).max(MIN_WINDOW_SECTORS));
    if len == 0 || len >= sector_count {
        return vec![SampleWindow {
            offset: 0,
            len: sector_count.min(len.max(crate::aacs::content::ALIGNED_UNIT_SECTORS)),
        }];
    }
    // The last window ENDS at the extent's end, so the plan covers the whole
    // extent's span rather than clustering near its start.
    let span = sector_count - len;
    // Never overlap: overlapping windows re-read bytes already seen and buy no
    // new observation, so drop the surplus windows instead.
    let windows = windows.min(span / len + 1);
    (0..windows)
        .map(|i| SampleWindow {
            offset: if windows == 1 {
                // A single window per extent placed at the head would sample the
                // first clip's start — the one span a film reliably lacks subtitles.
                // Use the middle instead.
                align_down(span / 2)
            } else {
                // u64: `span * i` overflows u32 for a large extent.
                align_down((u64::from(span) * u64::from(i) / u64::from(windows - 1)) as u32)
            },
            len,
        })
        .collect()
}

/// Sectors a plan reads — the coverage a cached observation of this extent is
/// worth, and what a later playlist compares its own plan against.
fn planned_coverage(sector_count: u32, share: u32) -> u32 {
    plan_windows(sector_count, share)
        .iter()
        .fold(0u32, |acc, w| acc.saturating_add(w.len))
}

// What one probed extent showed about one PGS track — the monotone facts a `ForcedTracker`
// accumulates. Keeping evidence (not a composed verdict) is what makes per-extent memoisation
// sound.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(crate) struct TrackEvidence {
    /// A PGS display set was actually seen for this track in this extent.
    observed: bool,
    /// At least one of those display sets was NOT forced.
    non_forced: bool,
    /// At least one of them WAS forced. Monotone like the other two, and the
    /// fact the demotion guard rests on: it proves the authoring house sets
    /// `forced_on_flag` at all (see [`crate::mux::codec::pgs::demotable`]).
    forced_seen: bool,
    /// How many display sets were seen. Saturating, so a pathological stream
    /// pins the count instead of wrapping it.
    displays: u32,
    /// The bytes behind this evidence are a SAMPLE of the extent, not all of it
    /// — so "every display set seen was forced" is a claim about the sample.
    /// Merges by OR: a title is sampled if any part of it was.
    sampled: bool,
}

impl TrackEvidence {
    fn merge(&mut self, other: Self) {
        self.observed |= other.observed;
        self.non_forced |= other.non_forced;
        self.forced_seen |= other.forced_seen;
        self.displays = self.displays.saturating_add(other.displays);
        self.sampled |= other.sampled;
    }

    fn facts(&self) -> crate::mux::codec::pgs::ForcedFacts {
        crate::mux::codec::pgs::ForcedFacts {
            displays: self.displays,
            // Only the "did any set carry the flag" bit is kept per extent, so
            // the count is reconstructed at its weakest true value: enough to
            // tell "mixed" from "none forced", which is all `demotable` reads.
            forced_displays: u32::from(self.forced_seen),
        }
    }
}

// One extent's memoised evidence for one track, WITH the coverage it rests on — so a sampled
// read is never replayed as if the whole extent was read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct CachedEvidence {
    evidence: TrackEvidence,
    /// Sectors of the extent actually read and demuxed to produce `evidence`.
    covered: u32,
}

impl CachedEvidence {
    // Whether this entry may stand in for re-reading the extent for a run that
    // would otherwise cover `wanted` sectors: `non_forced` settles it outright
    // (positive evidence); an absence claim needs coverage >= `wanted`.
    fn answers(&self, wanted: u32) -> bool {
        self.evidence.non_forced || !self.evidence.sampled || self.covered >= wanted
    }
}

// Memoises probe results across titles, keyed PER PHYSICAL EXTENT and per PGS track —
// `(start_lba, sector_count, pid)`, so playlists sharing clips without sharing extent LISTS
// still de-dupe.
pub(crate) type ForcedProbeCache = HashMap<(u32, u32, u16), CachedEvidence>;

// Why the read loop stopped — decides whether observations may be applied as an authoritative
// verdict: "not forced" is positive evidence (sound however the loop stopped); "forced" is an
// absence claim.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StopReason {
    /// Every extent was read to its end, or every track had already settled as
    /// not-forced. The observation is as complete as it will ever get.
    Exhausted,
    // `PROBE_BUDGET_SECTORS` was reached: a DESIGNED stop, not a failure — a forced track's
    // display sets appear throughout the title, so a bounded prefix is representative.
    Budget,
    // Operator cancellation: bytes read were read correctly, but the cut-off is arbitrary, same
    // as a read fault.
    Halted,
    /// A read error, or a short/zero-length read. The rest of the data was never
    /// seen; what was accumulated is an arbitrary prefix. Genuinely inconclusive.
    ReadFailed,
}

impl StopReason {
    /// Whether "no non-forced display set was seen" is a meaningful statement
    /// about the track — i.e. whether a `forced` verdict may be asserted and the
    /// result memoised.
    fn absence_is_conclusive(self) -> bool {
        match self {
            Self::Exhausted | Self::Budget => true,
            Self::Halted | Self::ReadFailed => false,
        }
    }
}

// Read the title's PGS streams and set `SubtitleStream::forced` from their
// content (only PGS; DVD VobSub forced comes from the IFO/vendor path).
// Best-effort: never fails, and an inconclusive run is NOT memoised.
pub(crate) fn probe_and_set_forced<S: SectorSource + ?Sized>(
    reader: &mut S,
    title: &mut DiscTitle,
    cache: &mut ForcedProbeCache,
    halt: Option<&crate::halt::Halt>,
) {
    let pg_pids: Vec<u16> = title
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(sub) if sub.codec == Codec::Pgs => Some(sub.pid),
            _ => None,
        })
        .collect();
    if pg_pids.is_empty() {
        return;
    }

    // The vendor-label flag each track arrived with. Read-only here: it decides
    // whether there is anything for content evidence to CORRECT, and hence
    // whether reading further can still change this track's outcome.
    let vendor_forced: HashMap<u16, bool> = title
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(sub) if sub.codec == Codec::Pgs => Some((sub.pid, sub.forced)),
            _ => None,
        })
        .collect();

    // Each extent's share of the sector budget is proportional to its size,
    // computed over the title's whole extent list (not just uncached ones), so the
    // sampling plan is a function of the title alone, not cache state or order.
    let total_sectors: u64 = title
        .extents
        .iter()
        .map(|e| u64::from(e.sector_count))
        .sum();
    let share = |sector_count: u32| -> u32 {
        if total_sectors == 0 {
            return 0;
        }
        ((u64::from(PROBE_BUDGET_SECTORS) * u64::from(sector_count)) / total_sectors)
            .min(u64::from(u32::MAX)) as u32
    };

    // Same extent, same plan → same evidence. Take from the cache what is already
    // known (per extent AND per track: evidence for one PID never stands in for
    // another) and read only what is missing.
    let mut evidence: HashMap<u16, TrackEvidence> = pg_pids
        .iter()
        .map(|&p| (p, TrackEvidence::default()))
        .collect();
    // Per extent, the tracks whose evidence must be READ because the cache has
    // nothing usable for them. A track already answered by the cache is not
    // demuxed again from that extent, so its evidence is counted exactly once.
    let mut todo: Vec<(crate::disc::Extent, Vec<u16>)> = Vec::new();
    for ext in &title.extents {
        let wanted = planned_coverage(ext.sector_count, share(ext.sector_count));
        let mut fresh: Vec<u16> = Vec::new();
        for &pid in &pg_pids {
            match cache.get(&(ext.start_lba, ext.sector_count, pid)) {
                // The entry covers at least as much of the extent as this run
                // meant to, or settles the track outright.
                Some(hit) if hit.answers(wanted) => {
                    if let Some(slot) = evidence.get_mut(&pid) {
                        slot.merge(hit.evidence);
                    }
                }
                // No entry, or one whose coverage doesn't support this run's claim —
                // includes a PGS PID a previous playlist didn't declare, so it's
                // genuinely probed.
                _ => fresh.push(pid),
            }
        }
        if !fresh.is_empty() {
            todo.push((*ext, fresh));
        }
    }

    // Compose what is known so far for one track: the evidence carried in from
    // other extents / the cache, plus what THIS extent's trackers have seen.
    fn live_evidence(
        pids: &[u16],
        evidence: &HashMap<u16, TrackEvidence>,
        trackers: &HashMap<u16, ForcedTracker>,
    ) -> Vec<(u16, TrackEvidence)> {
        pids.iter()
            .map(|&pid| {
                let mut e = evidence.get(&pid).copied().unwrap_or_default();
                if let Some(t) = trackers.get(&pid) {
                    // Mid-extent, so whatever this extent yields is by definition
                    // still partial — the strongest claim available is "sampled".
                    e.merge(tracker_evidence(t, true));
                }
                (pid, e)
            })
            .collect()
    }

    // Per-track early exit: once a track is disproven with nothing left to
    // correct, it stops asking for budget; when no track is asking, the run stops.
    let anything_left_to_learn = |live: &[(u16, TrackEvidence)]| -> bool {
        let disc_uses_forced_flag = live.iter().any(|(_, e)| e.forced_seen);
        let busiest = live.iter().map(|(_, e)| e.displays).max().unwrap_or(0);
        live.iter().any(|(pid, e)| {
            // Not yet disproven: the track can still be disproven (one non-forced
            // set) or confirmed forced. Always worth reading.
            if !e.non_forced {
                return true;
            }
            // Disproven, and its label already agrees — nothing to correct.
            if !vendor_forced.get(pid).copied().unwrap_or(false) {
                return false;
            }
            // Disproven but labelled forced: keep reading only while the evidence a
            // demotion needs (see `demotable`) is still incomplete.
            !crate::mux::codec::pgs::demotable(e.facts(), disc_uses_forced_flag, busiest)
        })
    };

    if todo.is_empty() {
        // Every extent's evidence reached a designed stop and covered at least as
        // much as planned, so an absence claim over it is as sound as its source.
        apply_verdicts(title, &verdicts(&evidence, true));
        return;
    }

    let mut buf = vec![0u8; CHUNK_SECTORS as usize * SECTOR_BYTES];
    let mut sectors_read: u32 = 0;
    // Record WHY the loop ended rather than leaving it implicit in the control
    // flow: every exit below names its reason, and the reason decides what may be
    // asserted from what was observed.
    let mut stop = StopReason::Exhausted;
    'outer: for (ext, fresh_pids) in &todo {
        // Trackers are per-extent, for tracks this extent still owes evidence for;
        // an already-answered track isn't demuxed again, so no observation is
        // double-counted and no budget is spent on tracks with nothing left to say.
        let mut trackers: HashMap<u16, ForcedTracker> = fresh_pids
            .iter()
            .map(|&p| (p, ForcedTracker::new()))
            .collect();
        // Nothing on this extent can change any track's outcome — skip it whole.
        if !anything_left_to_learn(&live_evidence(fresh_pids, &evidence, &trackers)) {
            continue;
        }
        let plan = plan_windows(ext.sector_count, share(ext.sector_count));
        // Read end to end (the only shape that can claim completeness).
        let complete_plan =
            matches!(plan.as_slice(), [w] if w.offset == 0 && w.len == ext.sector_count);
        // Sectors of this extent actually fed to the demuxer — the coverage the
        // memo will claim, never more.
        let mut covered: u32 = 0;
        // AACS units are anchored at this extent's start LBA, so gate a decrypt-
        // on-read source relative to it (not disc LBA 0), or the first read of a
        // non-3-aligned clip is rejected. Mirrors the mux read paths.
        reader.set_unit_base(ext.start_lba);
        // `None` = the extent's whole plan ran. `Some(reason)` = it stopped early.
        let mut cut_short: Option<StopReason> = None;
        for window in &plan {
            // Demux/parse state is per-window: a window is a discontiguous run of
            // the clip, so carrying a demuxer across the gap would splice unrelated
            // byte runs into one PES (same trade as the per-extent reset).
            let mut demux = TsDemuxer::new(fresh_pids);
            let mut parsers: HashMap<u16, PgsParser> =
                fresh_pids.iter().map(|&p| (p, PgsParser::new())).collect();
            // A window that does not fit the 32-bit LBA space cannot be read;
            // skipping it is the only bounds-safe answer (and it can only arise
            // from a malformed extent).
            let Some(start) =
                u32::try_from(u64::from(ext.start_lba) + u64::from(window.offset)).ok()
            else {
                continue;
            };
            let mut lba = start;
            let mut remaining = window.len;
            // Consecutive reads that came back with less than one AACS aligned unit, so
            // the read position could not move (see the short-read handling below).
            let mut stalled: u32 = 0;
            while remaining > 0 {
                // Bounded work and a responsive cancel: without these the probe
                // reads the entire title whenever a track really is forced.
                if halt.is_some_and(|h| h.is_cancelled()) {
                    cut_short = Some(StopReason::Halted);
                    break;
                }
                if sectors_read >= PROBE_BUDGET_SECTORS {
                    cut_short = Some(StopReason::Budget);
                    break;
                }
                let budget_left = PROBE_BUDGET_SECTORS - sectors_read;
                let count = remaining.min(CHUNK_SECTORS as u32).min(budget_left) as u16;
                let want = count as usize * SECTOR_BYTES;
                let n = match reader.read_sectors(lba, count, &mut buf[..want], false) {
                    Ok(n) => n,
                    // Best-effort — stop reading, but the data past here was never
                    // seen, so the observation is a truncated prefix.
                    Err(_) => {
                        cut_short = Some(StopReason::ReadFailed);
                        break;
                    }
                };
                // Advance by what was actually READ, not requested: a short-but-
                // nonzero read (e.g. `PrefetchedSectorSource`) used to advance by
                // the full `count`, silently skipping the unread tail as `Exhausted`.
                let served = (n.min(want) / SECTOR_BYTES) as u32;
                // Advancing by raw sector count would break unit-alignment (every
                // read must begin on an AACS aligned unit), so short reads advance only
                // by whole units; the residue sectors are simply re-read next pass.
                let got = if served >= u32::from(count) {
                    u32::from(count)
                } else {
                    served - served % crate::aacs::content::ALIGNED_UNIT_SECTORS
                };
                if got == 0 {
                    // Less than one aligned unit came back: feed the real bytes (never
                    // lose an observation), then retry the same lba, bounded by
                    // [`STALL_RETRY_LIMIT`] since boolean evidence is monotone to repeats.
                    for pes in demux.feed(&buf[..n.min(want)]) {
                        if let (Some(parser), Some(tracker)) =
                            (parsers.get_mut(&pes.pid), trackers.get_mut(&pes.pid))
                        {
                            for frame in parser.parse(&pes) {
                                tracker.observe(&frame.data);
                            }
                        }
                    }
                    stalled += 1;
                    if stalled > STALL_RETRY_LIMIT {
                        tracing::debug!(
                            target: "freemkv::scan",
                            lba,
                            requested = count,
                            served,
                            "forced-subtitle probe stalled below one aligned unit; stopping"
                        );
                        cut_short = Some(StopReason::ReadFailed);
                        break;
                    }
                    continue;
                }
                stalled = 0;
                for pes in demux.feed(&buf[..got as usize * SECTOR_BYTES]) {
                    if let (Some(parser), Some(tracker)) =
                        (parsers.get_mut(&pes.pid), trackers.get_mut(&pes.pid))
                    {
                        for frame in parser.parse(&pes) {
                            tracker.observe(&frame.data);
                        }
                    }
                }
                // Saturating: a malformed extent can put the last chunk at the top of
                // the 32-bit LBA space; `remaining` is already 0 by then, so pinning is
                // harmless — wrapping (or a debug-build panic) is not.
                lba = lba.saturating_add(got);
                remaining -= got;
                sectors_read += got;
                covered = covered.saturating_add(got);
                // Per-track early exit: the moment every track is either disproven or
                // has all the evidence its outcome can use, stop — there is nothing
                // further to learn from this (huge) clip.
                if !anything_left_to_learn(&live_evidence(fresh_pids, &evidence, &trackers)) {
                    cut_short = Some(StopReason::Exhausted);
                    break;
                }
            }

            // Drain the window's tail: the demuxer holds the last PES open until the
            // next PUSI, which a sampled read may put in another window (or nowhere),
            // so without this every window loses its last display set.
            for pes in demux.flush() {
                if let (Some(parser), Some(tracker)) =
                    (parsers.get_mut(&pes.pid), trackers.get_mut(&pes.pid))
                {
                    for frame in parser.parse(&pes) {
                        tracker.observe(&frame.data);
                    }
                }
            }
            // ...then any display set the PARSER still holds pending.
            for (pid, parser) in parsers.iter_mut() {
                if let Some(tracker) = trackers.get_mut(pid) {
                    for frame in parser.flush() {
                        tracker.observe(&frame.data);
                    }
                }
            }
            if cut_short.is_some() {
                break;
            }
        }

        // Fold in this extent's evidence; memoise only on a DESIGNED stop (plan
        // done, budget hit, or all tracks settled), with coverage stored so a later
        // playlist re-reads rather than inherits a halt/fault's arbitrary cutoff.
        let cacheable = cut_short.is_none_or(StopReason::absence_is_conclusive);
        let sampled = !(complete_plan && cut_short.is_none());
        for (&pid, t) in trackers.iter() {
            let ev = tracker_evidence(t, sampled);
            if let Some(slot) = evidence.get_mut(&pid) {
                slot.merge(ev);
            }
            if cacheable {
                let fresh = CachedEvidence {
                    evidence: ev,
                    covered,
                };
                // Never replace a richer memo with a thinner one (a re-read under a
                // smaller share would downgrade it): facts merge monotonically, and
                // the coverage claimed is the larger of the two, which is conservative.
                cache
                    .entry((ext.start_lba, ext.sector_count, pid))
                    .and_modify(|prev| {
                        prev.evidence.observed |= fresh.evidence.observed;
                        prev.evidence.non_forced |= fresh.evidence.non_forced;
                        prev.evidence.forced_seen |= fresh.evidence.forced_seen;
                        // MAX, not sum: the two reads overlap on the same extent,
                        // so adding them would count the same display sets twice
                        // and inflate the count the demotion shape test reads.
                        prev.evidence.displays =
                            prev.evidence.displays.max(fresh.evidence.displays);
                        prev.evidence.sampled &= fresh.evidence.sampled;
                        prev.covered = prev.covered.max(fresh.covered);
                    })
                    .or_insert(fresh);
            }
        }
        if let Some(reason) = cut_short {
            // Exhausted is judged from THIS extent's tracks only; a later extent may still owe
            // evidence for a track the cache answered here.
            if reason == StopReason::Exhausted {
                continue;
            }
            stop = reason;
            break 'outer;
        }
    }

    let conclusive = stop.absence_is_conclusive();
    let verdicts = verdicts(&evidence, conclusive);
    if !conclusive {
        tracing::debug!(
            target: "freemkv::scan",
            stop = ?stop,
            sectors_read,
            asserted = verdicts.len(),
            tracks = pg_pids.len(),
            "forced-subtitle probe truncated; verdicts limited and truncated extents not cached"
        );
    }
    apply_verdicts(title, &verdicts);
}

/// One tracker's accumulated state as mergeable, memoisable evidence.
fn tracker_evidence(t: &ForcedTracker, sampled: bool) -> TrackEvidence {
    let facts = t.facts();
    TrackEvidence {
        observed: t.observed(),
        non_forced: t.settled_not_forced(),
        forced_seen: facts.forced_displays > 0,
        displays: facts.displays,
        sampled,
    }
}

// Compose the per-track verdicts a run is ENTITLED to assert from the evidence it gathered
// (four gates: observed, non_forced-on-truncation, demotable, PROMOTE_MIN_DISPLAY_SETS).
fn verdicts(evidence: &HashMap<u16, TrackEvidence>, conclusive: bool) -> HashMap<u16, bool> {
    // Disc-level facts the demotion gate rests on, over the tracks judged
    // together: does the authoring house set the flag at all, and how busy is the
    // busiest track (the yardstick a forced-narrative track is small against).
    let disc_uses_forced_flag = evidence.values().any(|e| e.forced_seen);
    let busiest = evidence.values().map(|e| e.displays).max().unwrap_or(0);
    evidence
        .iter()
        .filter(|(_, e)| e.observed && (conclusive || e.non_forced))
        .filter(|(_, e)| {
            if e.non_forced {
                // Clearing a label: the demotion guard.
                return crate::mux::codec::pgs::demotable(
                    e.facts(),
                    disc_uses_forced_flag,
                    busiest,
                );
            }
            // Calling a track FORCED is also an absence claim ("no set here was
            // un-flagged"); over a SAMPLE of a mixed track, one flagged set alone
            // is a wrong promotion, so require a minimum unless the read was complete.
            !e.sampled || e.displays >= PROMOTE_MIN_DISPLAY_SETS
        })
        .map(|(&pid, e)| (pid, !e.non_forced))
        .collect()
}

/// Set `forced` on every PGS subtitle track named in `verdicts`. A track absent
/// from the map was never observed and keeps its vendor-derived flag.
fn apply_verdicts(title: &mut DiscTitle, verdicts: &HashMap<u16, bool>) {
    for s in &mut title.streams {
        if let Stream::Subtitle(sub) = s
            && sub.codec == Codec::Pgs
            && let Some(&forced) = verdicts.get(&sub.pid)
        {
            sub.forced = forced;
            // A demoted track must not go on describing itself as forced: flag and
            // qualifier render one fact for different consumers, so leaving `Forced`
            // behind contradicts the header. The probe outranks the vendor's claim.
            if !forced && sub.qualifier == crate::disc::LabelQualifier::Forced {
                sub.qualifier = crate::disc::LabelQualifier::None;
            }
        }
    }
}

#[cfg(test)]
#[path = "pgs_forced_probe_tests.rs"]
mod tests;
