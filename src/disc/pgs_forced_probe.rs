//! Content-based forced-subtitle detection for Blu-ray/UHD PGS tracks.
//!
//! `freemkv info` and the muxer must agree on which subtitle tracks are forced.
//! The muxer derives it from the PGS `forced_on_flag` while muxing a rip; this
//! module gives `info` the SAME verdict up front by reading the title's PGS
//! streams and feeding them through the one shared classifier
//! ([`crate::mux::codec::pgs::ForcedTracker`]) — so the two never diverge.
//!
//! WHERE it reads is the whole design. A track is forced iff EVERY display set
//! carries `forced_on_flag`, so one non-forced set disproves forced for good,
//! while proving forced needs the whole track — and the tracks that are
//! expensive to prove are the cheap ones to read (a forced-narrative track is
//! tens of display sets; a full dialogue track is thousands). A bounded budget
//! spent on the title's HEAD therefore learns nothing at all: a feature's
//! subtitles begin minutes in, past the end of any affordable prefix. The budget
//! ([`PROBE_BUDGET_SECTORS`]) is instead SPREAD over each extent as sample
//! windows ([`plan_windows`]), so every window is an independent chance to catch
//! a display set.
//!
//! Cost: the budget is a hard ceiling per call. A run stops early once no track
//! can still change its outcome — disproven, and with no wrong forced label left
//! to correct.
//!
//! Contradicting a label: content may CLEAR a vendor forced flag as well as set
//! one, but only behind [`crate::mux::codec::pgs::demotable`] — an absence of
//! `forced_on_flag` proves nothing on a disc whose authoring never sets it.
//!
//! Encrypted content: the probe reuses whatever [`SectorSource`] the scan holds.
//! With a decrypting source it sees real PGS; without keys it reads ciphertext
//! and observes no display sets, in which case it leaves each track's existing
//! (vendor-label-derived) forced flag untouched rather than asserting anything.
//!
//! Truncated reads: the probe is best-effort, but "best-effort" must not mean
//! "assert a verdict from an arbitrary prefix". [`StopReason`] records why the
//! read loop ended and narrows what may be asserted accordingly.

use crate::disc::{Codec, DiscTitle, Stream};
use crate::mux::codec::CodecParser;
use crate::mux::codec::pgs::{ForcedTracker, PgsParser};
use crate::mux::ts::TsDemuxer;
use crate::sector::SectorSource;
use std::collections::HashMap;

const SECTOR_BYTES: usize = 2048;
/// Read the clip in ~2 MiB chunks.
///
/// A whole number of AACS aligned units (3 sectors / 6144 B), because with a
/// decrypting source — the case this module's doc promises — every read must
/// begin on a unit boundary measured from the extent base or
/// `DecryptingSectorSource` rejects it outright with `DecryptFailed`. At 1024
/// (`1024 % 3 == 1`) every chunk after the first drifted off the boundary, so
/// content-based forced detection was unreachable past the first chunk of an
/// AACS disc. 1023 = 341 units.
const CHUNK_SECTORS: u16 = 1023;

// The alignment requirement above is enforced, not just described.
const _: () = assert!(
    (CHUNK_SECTORS as u32).is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS),
    "probe chunks must be a whole number of AACS aligned units"
);

/// How many times a read that came back with less than one AACS aligned unit — so
/// the read position could not advance without leaving the unit grid — is retried
/// at the same LBA before the run is declared truncated. A couple of retries covers
/// a source whose batching straddles the request (a short call followed by a
/// satisfying one); a source that can never yield a whole unit must not spin, so
/// the count is small and the stop is `ReadFailed` (inconclusive, not memoised).
const STALL_RETRY_LIMIT: u32 = 2;

/// Hard ceiling on sectors read per probe call (256 MiB).
///
/// The probe's natural exit is "every track has shown a non-forced display set",
/// which a genuinely FORCED track never satisfies — so on the common authoring
/// (a forced-narrative track for foreign dialogue) the loop would otherwise read
/// the title's whole extent set, tens of GB, at optical-drive speed.
///
/// UNCHANGED from the head-first design this replaced: the same 256 MiB buys a
/// completely different observation now that it is SPREAD (see
/// [`plan_windows`]) instead of spent on the title's first 27 seconds.
const PROBE_BUDGET_SECTORS: u32 = 131_072;

/// One sample window: ~16 MiB, a whole number of AACS aligned units
/// (8190 = 2730 units).
///
/// Sized in PLAYBACK time, not bytes: a window has to be long enough that a
/// dialogue track's display sets are likely to fall inside it. At UHD feature
/// bitrates 16 MiB is a couple of seconds, and a full subtitle track carries a
/// display set every few seconds, so each window is a fair coin — which is why
/// the plan takes many of them rather than one big one.
const WINDOW_SECTORS: u32 = 8_190;

/// Floor on a window (2 MiB). A window smaller than this is too short to be
/// likely to contain a display set at all, so it would spend drive time to learn
/// nothing; a title fragmented into so many extents that its per-extent share
/// falls below the floor instead samples fewer extents (the global budget stops
/// the run) rather than sampling all of them uselessly.
const MIN_WINDOW_SECTORS: u32 = CHUNK_SECTORS as u32;

/// Most windows spent on a single extent. Past this, extra windows buy
/// diminishing spread for the same bytes.
const MAX_WINDOWS_PER_EXTENT: u32 = 16;

// Windows must start (and, so that every chunk inside them does too, be sized)
// on the AACS aligned-unit grid — same requirement as CHUNK_SECTORS.
const _: () = assert!(
    WINDOW_SECTORS.is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS),
    "a sample window must be a whole number of AACS aligned units"
);

/// One sampled run of sectors inside an extent: `offset` sectors from the
/// extent's `start_lba`, `len` sectors long. Both are whole numbers of AACS
/// aligned units, so every read inside the window stays on the unit grid the
/// decrypting source demands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct SampleWindow {
    offset: u32,
    len: u32,
}

/// Round DOWN to the AACS aligned-unit grid.
fn align_down(sectors: u32) -> u32 {
    sectors - sectors % crate::aacs::content::ALIGNED_UNIT_SECTORS
}

/// Where to read inside one extent, given the sector budget `share` allotted to
/// it — the core of the redesign.
///
/// The forced predicate is ASYMMETRIC: a track is forced iff EVERY display set
/// carries `forced_on_flag`, so
///
///   * ONE non-forced display set DISPROVES forced, permanently — and the tracks
///     that need disproving are the big ones (full dialogue tracks: measured
///     shape, one to two thousand display sets spread over the whole feature);
///   * PROVING forced needs to see the whole track — but a genuine forced track
///     is tiny (measured shape: tens of display sets, well under a megabyte).
///
/// So the expensive-to-prove case is the cheap-to-read one, and the case that
/// dominates the budget is disproved by a single hit anywhere in the title. A
/// head-first prefix is therefore the worst possible allocation: it reads the
/// start of everything, where a feature has no subtitles at all (measured: the
/// first display set lands well past the first 256 MiB), so it disproves nothing
/// and observes nothing. Spreading the SAME budget over the extent gives every
/// window an independent chance of landing on a display set.
///
/// The plan is a pure function of `(sector_count, share)` — no clock, no
/// randomness, no dependence on what has been read so far — so two runs over the
/// same extent with the same share read the same bytes, which is what makes the
/// per-extent memo (see [`ForcedProbeCache`]) reproducible rather than a snapshot
/// of one run's timing.
fn plan_windows(sector_count: u32, share: u32) -> Vec<SampleWindow> {
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
                0
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

/// What one probed extent showed about one PGS track — the two monotone facts a
/// [`ForcedTracker`] accumulates, and nothing else.
///
/// Keeping the EVIDENCE (rather than a composed forced/not-forced verdict) is
/// what makes per-extent memoisation sound: both fields only ever go from
/// `false` to `true` as more data is seen, so a title's verdict is the
/// field-wise OR over its extents, in any order, with no dependence on how the
/// extents were grouped into playlists.
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
}

impl TrackEvidence {
    fn merge(&mut self, other: Self) {
        self.observed |= other.observed;
        self.non_forced |= other.non_forced;
        self.forced_seen |= other.forced_seen;
        self.displays = self.displays.saturating_add(other.displays);
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

/// One extent's memoised evidence for one track, WITH the coverage it rests on.
///
/// The coverage is the whole point. Evidence from a sampled (or budget-cut) read
/// describes the sectors that were actually fed to the demuxer, not the extent —
/// memoising it under the extent's full key and replaying it to another playlist
/// asserts a completeness the read never had. Recording `covered` and `complete`
/// lets a later run decide whether the entry answers ITS question or whether the
/// extent has to be read again.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct CachedEvidence {
    evidence: TrackEvidence,
    /// Sectors of the extent actually read and demuxed to produce `evidence`.
    covered: u32,
    /// The extent was read from end to end — no sampling, no early stop.
    complete: bool,
}

impl CachedEvidence {
    /// Whether this entry may stand in for reading the extent again, for a run
    /// that would otherwise cover `wanted` sectors of it.
    ///
    /// `non_forced` is POSITIVE evidence (a non-forced display set was seen on
    /// the wire) and settles the track outright, so its coverage is irrelevant.
    /// Everything else is an ABSENCE claim, and an absence is only worth what was
    /// looked at: honour it only when at least as much of the extent was covered
    /// as this run intended to cover.
    fn answers(&self, wanted: u32) -> bool {
        self.evidence.non_forced || self.complete || self.covered >= wanted
    }
}

/// Memoises probe results across titles, keyed PER PHYSICAL EXTENT and per PGS
/// track — `(start_lba, sector_count, pid)`.
///
/// Many playlists on one disc reference the same clips (main feature, play-all,
/// seamless-branch variants) but rarely with byte-identical extent LISTS: 00800
/// = [A, B], 00801 = [A], 00802 = [B] are three different lists over two clips.
/// Keying on the whole list de-duplicated only exactly-identical playlists and
/// re-read every shared clip once per list — up to `PROBE_BUDGET_SECTORS`
/// (256 MiB) of optical-drive time each. Per-extent keying reads each physical
/// extent at most once per disc, and per-track keying means a playlist that
/// declares MORE PGS tracks over the same extents still probes the extra ones
/// instead of silently taking a verdict map that has no entry for them.
///
/// Only extents whose read reached a DESIGNED stop are memoised (see
/// `probe_and_set_forced`), so one cancellation or read fault is never frozen in
/// as an extent's answer — and each entry carries the COVERAGE behind it (see
/// [`CachedEvidence`]), so a sampled observation is never replayed as if the
/// whole extent had been read.
pub(crate) type ForcedProbeCache = HashMap<(u32, u32, u16), CachedEvidence>;

/// Why the read loop stopped — which decides whether the observations it
/// accumulated may be applied as an authoritative verdict.
///
/// The distinction matters because the two kinds of per-track verdict rest on
/// opposite kinds of evidence:
///
///   * "not forced" is POSITIVE evidence — a non-forced display set was actually
///     seen on the wire. Nothing read later can retract it, so it is sound no
///     matter how the loop stopped.
///   * "forced" is an ABSENCE claim — display sets were seen and none of them was
///     non-forced. It is only sound if the read got far enough for that absence
///     to mean something. On an arbitrarily truncated prefix it does not.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StopReason {
    /// Every extent was read to its end, or every track had already settled as
    /// not-forced. The observation is as complete as it will ever get.
    Exhausted,
    /// [`PROBE_BUDGET_SECTORS`] was reached. A DESIGNED stop, not a failure: the
    /// natural exit never fires for a genuinely forced track, so the budget
    /// exists precisely so that a forced verdict can be accepted from a bounded
    /// prefix. A forced track's display sets appear throughout the title, so the
    /// prefix is representative — treating this as inconclusive would disable
    /// forced detection outright, the very thing the budget was added to enable.
    Budget,
    /// Operator cancellation. The bytes read were read correctly, but the cut-off
    /// point is arbitrary — cancellation can land after a single chunk (or, as
    /// with an already-cancelled halt, after none at all). Epistemically that is
    /// the same arbitrary prefix as a read fault, so an absence claim from it is
    /// not trustworthy.
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

/// Read the title's PGS streams and set `SubtitleStream::forced` from their
/// content. Only PGS tracks are touched (DVD VobSub forced comes from the
/// IFO/vendor path).
///
/// Best-effort by design: this returns `()` and never fails. Where the read is
/// cut short, the probe narrows what it is willing to assert instead of failing
/// — see [`StopReason`]. A track whose verdict is not assertable keeps its
/// existing vendor-label-derived flag, and an inconclusive run is NOT memoised,
/// so one transient read fault cannot be replayed onto every other playlist
/// sharing those extents.
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

    // Budget allocation: each extent gets a share of the sector budget in
    // proportion to its size, computed over the title's WHOLE extent list rather
    // than over the extents that happen to be uncached. That keeps an extent's
    // sampling plan — and therefore the coverage its cache entry claims — a
    // function of the title alone, not of cache state or probe order.
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
                // No entry, or one whose coverage does not support the claim this
                // run needs from it. (A playlist that declares a PGS PID a
                // previous playlist did not also lands here, so the extra track is
                // genuinely probed.)
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
                    e.merge(tracker_evidence(t));
                }
                (pid, e)
            })
            .collect()
    }

    // Whether ANY declared track could still have its outcome changed by reading
    // more. This is the per-track early exit: the moment a track is disproven
    // (and there is nothing left for its evidence to correct) it stops asking for
    // budget, and when no track is asking, the run stops.
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
            // Disproven but labelled forced: keep reading only while the evidence
            // a demotion needs is still incomplete (see `demotable`) — otherwise
            // the run would stop one window short of being ALLOWED to act on what
            // it has already seen.
            !crate::mux::codec::pgs::demotable(e.facts(), disc_uses_forced_flag, busiest)
        })
    };

    if todo.is_empty() {
        // Every extent's evidence came from a run that reached a designed stop and
        // covered at least as much as this run planned to, so an absence claim
        // over the composed evidence is as sound as the run that produced each
        // part.
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
        // Trackers are PER EXTENT and only for the tracks this extent still owes
        // evidence for — a track the cache already answered is not demuxed again,
        // so no observation is ever counted twice, and budget is not spent on
        // tracks that have nothing left to say.
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
        // AACS aligned units are anchored at THIS extent's start LBA, so tell a
        // decrypt-on-read source to gate relative to it rather than absolute disc
        // LBA 0 — without this the very first read of a clip whose start_lba is
        // not itself 3-aligned is rejected. Mirrors the mux read paths. Every
        // window offset is a whole number of units from that base, so sampling
        // does not disturb the gate.
        reader.set_unit_base(ext.start_lba);
        // `None` = the extent's whole plan ran. `Some(reason)` = it stopped early.
        let mut cut_short: Option<StopReason> = None;
        for window in &plan {
            // Demux/parse state is PER WINDOW, because a window is a
            // DISCONTIGUOUS run of the clip: carrying a demuxer across the gap
            // would splice two unrelated byte runs into one PES. The cost is at
            // most a display set straddling a window edge — the same trade the
            // per-extent reset already made at extent edges.
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
                // Advance by what was actually READ, not by what was requested. A
                // short-but-nonzero read (a source whose batch is smaller than the
                // request — `PrefetchedSectorSource` returns its producer's batch)
                // used to advance `lba`/`remaining`/`sectors_read` by the full
                // `count`, silently SKIPPING the unread tail of the chunk while
                // `stop` stayed `Exhausted` — so an absence-based forced verdict was
                // asserted (and memoised) over data that was never seen.
                let served = (n.min(want) / SECTOR_BYTES) as u32;
                // ...but advancing by the raw sector count breaks the OTHER invariant
                // this loop rests on: with a decrypting source every read must BEGIN a
                // whole number of AACS aligned units past `ext.start_lba`, which is why
                // `CHUNK_SECTORS` is a multiple of `ALIGNED_UNIT_SECTORS` (see the
                // const-assert above). A short read of, say, 64 sectors is not a whole
                // number of units, so the next `lba` would be off the unit grid,
                // `DecryptingSectorSource` would reject it with `DecryptFailed` before
                // reading, and the run would end `ReadFailed` — inconclusive, nothing
                // memoised, forced detection silently back to the vendor label on
                // exactly the encrypted discs the aligned chunk size was chosen for.
                //
                // So a read that did NOT satisfy the whole request advances only by
                // whole aligned units, and the residue sectors are simply RE-READ from
                // the next unit boundary on the following pass: at most two sectors of
                // duplicated drive work, no gap, no double-feed of any byte to the
                // demuxer (only the aligned prefix is fed), and no trailing partial
                // unit — whose plaintext a unit-anchored decrypt cannot produce anyway
                // — is ever handed to the parsers.
                //
                // A read that satisfied the whole request advances by all of it even
                // when `count` itself was not unit-aligned: `count` is only ever below
                // `CHUNK_SECTORS` on the extent's final chunk or at the sector budget,
                // and both end the loop before another read of this extent.
                let got = if served >= u32::from(count) {
                    u32::from(count)
                } else {
                    served - served % crate::aacs::content::ALIGNED_UNIT_SECTORS
                };
                if got == 0 {
                    // Less than one whole aligned unit came back, so the read position
                    // cannot move: the next aligned boundary IS the one just read. The
                    // bytes are real, so feed them (never lose an observation), then
                    // RETRY the same aligned lba — a source that short-changed one call
                    // commonly satisfies the next, and only when it repeatedly cannot
                    // yield a whole unit is the run the truncated prefix an error is.
                    // Bounded retries are what keep a source that never yields a unit
                    // (including one that claims sectors and returns none) from
                    // spinning here for ever. A retry that re-serves the same bytes
                    // feeds them twice; the evidence a [`ForcedTracker`] keeps is
                    // monotone (observed / saw-a-non-forced-set), so a repeat cannot
                    // change a verdict.
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
                // Saturating: a malformed extent can put the window's last chunk at
                // the top of the 32-bit LBA space, and `remaining` has already
                // reached 0 by then, so pinning the position is harmless — wrapping
                // (or panicking in a debug build) is not.
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

            // Drain any buffered final display set of THIS window.
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

        // Fold this extent's evidence in, and memoise it if the read reached a
        // DESIGNED stop — the whole plan ran, the sector budget was hit, or every
        // track had already settled. The budget is a designed stop for exactly the
        // reason [`StopReason`] documents, and it is the stop that fires on every
        // disc that HAS a forced track — excluding it from the cache would mean
        // nothing is ever memoised on precisely those discs.
        //
        // What makes THAT sound is the coverage stored alongside: the entry says
        // how much of the extent was actually read, so a later playlist that
        // intended to read more of it will re-read instead of inheriting an
        // absence claim from a prefix (see [`CachedEvidence::answers`]).
        //
        // A halt or a read fault is different: the cut-off point is arbitrary, so
        // its evidence is real for THIS title (nothing observed is retracted) but
        // must not be frozen in as the extent's answer, or one transient fault
        // would be replayed onto every other playlist sharing the clip.
        let cacheable = cut_short.is_none_or(StopReason::absence_is_conclusive);
        for (&pid, t) in trackers.iter() {
            let ev = tracker_evidence(t);
            if let Some(slot) = evidence.get_mut(&pid) {
                slot.merge(ev);
            }
            if cacheable {
                cache.insert(
                    (ext.start_lba, ext.sector_count, pid),
                    CachedEvidence {
                        evidence: ev,
                        covered,
                        complete: complete_plan && cut_short.is_none(),
                    },
                );
            }
        }
        if let Some(reason) = cut_short {
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
fn tracker_evidence(t: &ForcedTracker) -> TrackEvidence {
    let facts = t.facts();
    TrackEvidence {
        observed: t.observed(),
        non_forced: t.settled_not_forced(),
        forced_seen: facts.forced_displays > 0,
        displays: facts.displays,
    }
}

/// Compose the per-track verdicts a run is ENTITLED to assert from the evidence
/// it gathered. A track absent from the result keeps its vendor-derived flag.
///
/// Three gates, all PER TRACK, because the evidence is per track:
///   * `observed` — saw no display set at all, so nothing is known. (Never assert
///     "not forced" from having seen nothing.)
///   * on a truncated run, `non_forced` — the track saw an actual non-forced
///     display set, which no further reading could retract, so that verdict
///     stands even though the run was cut short. A track that merely hadn't YET
///     seen a non-forced set is exactly the claim the truncation invalidates, so
///     it is dropped and keeps the vendor flag.
///   * for the NOT-FORCED verdict, [`crate::mux::codec::pgs::demotable`] — the
///     verdict may be contradicting a vendor label, and "no display set carried
///     `forced_on_flag`" says nothing at all on a disc whose authoring never sets
///     that flag. Discs like that exist (measured: not one track on the disc
///     carries it), and without this gate the probe would strip the correct
///     forced label off every track on every one of them.
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
            !e.non_forced
                || crate::mux::codec::pgs::demotable(e.facts(), disc_uses_forced_flag, busiest)
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{ContentFormat, Extent, LabelQualifier, SubtitleStream};

    /// A reader that yields all-zeros (an encrypted / unreadable clip) for a
    /// bounded span, then EOF.
    struct ZeroReader {
        served: u32,
        cap: u32,
    }
    impl SectorSource for ZeroReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            if self.served >= self.cap {
                return Ok(0);
            }
            self.served += count as u32;
            buf.fill(0);
            Ok(buf.len())
        }
        fn capacity_sectors(&self) -> u32 {
            self.cap
        }
    }

    fn pgs_title(pid: u16, vendor_forced: bool) -> DiscTitle {
        DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: vec![],
            streams: vec![Stream::Subtitle(SubtitleStream {
                pid,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: vendor_forced,
                qualifier: LabelQualifier::None,
                codec_data: None,
            })],
            chapters: vec![],
            extents: vec![Extent {
                start_lba: 0,
                sector_count: 4,
            }],
            content_format: ContentFormat::BdTs,
            codec_privates: vec![None],
        }
    }

    /// A reader that counts sectors served and never runs out — stands in for a
    /// real title whose forced track means the probe's natural exit never fires.
    struct EndlessReader {
        served: u32,
    }
    impl SectorSource for EndlessReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _skip_errors: bool,
        ) -> crate::error::Result<usize> {
            let want = count as usize * SECTOR_BYTES;
            buf[..want].fill(0);
            self.served += count as u32;
            Ok(want)
        }
        fn capacity_sectors(&self) -> u32 {
            u32::MAX
        }
    }

    #[test]
    fn probe_stops_at_the_sector_budget() {
        // The probe's only natural exit is "every track settled as NOT forced",
        // which a genuinely forced track never satisfies. Without a budget the
        // loop reads the whole title — tens of GB off an optical drive.
        let mut reader = EndlessReader { served: 0 };
        let mut title = pgs_title(0x1200, true);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: u32::MAX,
        }];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert!(
            reader.served <= PROBE_BUDGET_SECTORS,
            "the probe must never read past the budget, got {}",
            reader.served
        );
        // ...and must actually SPEND it: a plan that quietly reads a fraction of
        // the budget would look "safe" while observing even less than the
        // head-first read it replaced. One window's slack is allowed (windows are
        // whole numbers of aligned units, so 16 of them need not divide the
        // budget exactly).
        assert!(
            reader.served + WINDOW_SECTORS > PROBE_BUDGET_SECTORS,
            "the probe must spend the budget it is given, got {}",
            reader.served
        );
    }

    #[test]
    fn probe_honours_halt() {
        // `info -v` must stay cancellable: an already-cancelled halt means no
        // sectors are read at all.
        let mut reader = EndlessReader { served: 0 };
        let mut title = pgs_title(0x1200, true);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: u32::MAX,
        }];
        let halt = crate::halt::Halt::new();
        halt.cancel();
        probe_and_set_forced(
            &mut reader,
            &mut title,
            &mut ForcedProbeCache::new(),
            Some(&halt),
        );
        assert_eq!(
            reader.served, 0,
            "a cancelled halt must stop the probe dead"
        );
    }

    #[test]
    fn identical_extents_are_served_from_cache_not_reread() {
        // A disc's playlists overwhelmingly share clips. The second title with the
        // same extent list must cost ZERO further reads, or `info -v` re-reads the
        // same physical clip once per playlist.
        let mut reader = EndlessReader { served: 0 };
        let mut cache = ForcedProbeCache::new();

        let mut first = pgs_title(0x1200, true);
        first.extents = vec![Extent {
            start_lba: 0,
            sector_count: u32::MAX,
        }];
        probe_and_set_forced(&mut reader, &mut first, &mut cache, None);
        let after_first = reader.served;
        assert!(after_first > 0, "the first title must actually read");

        let mut second = pgs_title(0x1200, true);
        second.extents = vec![Extent {
            start_lba: 0,
            sector_count: u32::MAX,
        }];
        probe_and_set_forced(&mut reader, &mut second, &mut cache, None);
        assert_eq!(
            reader.served, after_first,
            "identical extents must be served from cache with no further reads"
        );

        // A DIFFERENT extent list is a cache miss and must still be probed.
        let mut third = pgs_title(0x1200, true);
        third.extents = vec![Extent {
            start_lba: 9_000,
            sector_count: u32::MAX,
        }];
        probe_and_set_forced(&mut reader, &mut third, &mut cache, None);
        assert!(
            reader.served > after_first,
            "a different extent list must not hit the cache"
        );
    }

    #[test]
    fn no_observed_content_preserves_vendor_forced() {
        // An unreadable/encrypted clip yields no PGS display sets — the probe must
        // leave the existing vendor-derived forced flag untouched, never assert
        // "not forced" from having seen nothing.
        let mut reader = ZeroReader { served: 0, cap: 4 };
        let mut title = pgs_title(0x1200, true);
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(s.forced, "no content observed → vendor forced preserved");
    }

    /// A reader that serves a fixed BD-TS byte stream once (across sequential
    /// `read_sectors` calls), then EOF — so the probe's demux→parse→observe→apply
    /// path runs on real synthetic PGS content.
    ///
    /// Sector-granular, like every real [`SectorSource`]: a read that is served
    /// from the payload's short tail zero-pads to the sector boundary and reports
    /// whole sectors. (The probe accounts in SECTORS, so a source that returned a
    /// sub-sector byte count could never advance.)
    struct TsReader {
        data: Vec<u8>,
        pos: usize,
    }
    impl SectorSource for TsReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let n = buf.len().min(self.data.len() - self.pos);
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            let padded = n.div_ceil(SECTOR_BYTES) * SECTOR_BYTES;
            let out = padded.min(buf.len());
            buf[n..out].fill(0);
            Ok(out)
        }
        fn capacity_sectors(&self) -> u32 {
            self.data.len().div_ceil(SECTOR_BYTES) as u32
        }
    }

    // PGS PCS layout (matches the private constants in mux::codec::pgs): a
    // display-set frame begins with a PCS (segment type 0x16); byte 13 is
    // number_of_composition_objects; byte 17 is the first object's flags, whose
    // 0x40 bit is forced_on_flag.
    const PCS_SEG: u8 = 0x16;
    const PCS_NUM_OBJECTS_OFF: usize = 13;
    const PCS_FLAGS_OFF: usize = 17;
    const PCS_FORCED_FLAG: u8 = 0x40;

    /// One PGS display-set elementary payload with a single composition object;
    /// `forced` sets forced_on_flag.
    fn pcs_display(forced: bool) -> Vec<u8> {
        let mut d = vec![0u8; 18];
        d[0] = PCS_SEG;
        d[PCS_NUM_OBJECTS_OFF] = 1;
        d[PCS_FLAGS_OFF] = if forced { PCS_FORCED_FLAG } else { 0 };
        d
    }

    /// Wrap an elementary payload in one 192-byte BD-TS PES packet (PUSI, PTS
    /// present) on `pid`. `cc` is the 4-bit continuity counter.
    fn bd_pes_packet(pid: u16, cc: u8, es: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; 192];
        // pkt[0..4] = TP_extra_header (zeros). TS packet starts at pkt[4].
        pkt[4] = 0x47; // sync
        pkt[5] = 0x40 | ((pid >> 8) & 0x1F) as u8; // PUSI + PID high 5 bits
        pkt[6] = (pid & 0xFF) as u8; // PID low 8 bits
        pkt[7] = 0x10 | (cc & 0x0F); // adaptation=payload-only + continuity counter
        // PES header (at ts payload = pkt[8..]): 00 00 01 stream_id len flags.
        let p = 8;
        pkt[p] = 0x00;
        pkt[p + 1] = 0x00;
        pkt[p + 2] = 0x01;
        pkt[p + 3] = 0xBD; // private_stream_1 (carries the standard PES extension)
        pkt[p + 4] = 0x00; // PES packet length hi (0 = unbounded; ignored by demux)
        pkt[p + 5] = 0x00; // PES packet length lo
        pkt[p + 6] = 0x80; // flags1 ('10' marker)
        pkt[p + 7] = 0x80; // flags2 → PTS present
        pkt[p + 8] = 0x05; // PES_header_data_length = 5 (one PTS)
        // 5-byte PTS with the mandatory marker bits (bytes 0,2,4 low bit = 1).
        pkt[p + 9] = 0x21;
        pkt[p + 10] = 0x00;
        pkt[p + 11] = 0x01;
        pkt[p + 12] = 0x00;
        pkt[p + 13] = 0x01;
        let es_off = p + 14; // ES data follows the 14-byte PES header
        let n = es.len().min(192 - es_off);
        pkt[es_off..es_off + n].copy_from_slice(&es[..n]);
        pkt
    }

    /// Two BD-TS PES on `pid`: the FIRST carries `es` (the observed display set);
    /// the second (a fresh PUSI) exists only to flush the first PES out of the
    /// demuxer — the probe never calls `TsDemuxer::flush`, so an open PES stays
    /// buffered until the next PES start arrives.
    fn ts_stream(pid: u16, es: &[u8]) -> Vec<u8> {
        let mut s = bd_pes_packet(pid, 0, es);
        s.extend_from_slice(&bd_pes_packet(pid, 1, &pcs_display(false)));
        s
    }

    #[test]
    fn forced_display_sets_apply_forced_verdict() {
        // Feed REAL synthetic PGS bytes through the full demux→parse→observe→apply
        // path: a forced display set must flip a vendor-not-forced PGS track to
        // forced. Mutation guard: inverting ForcedTracker::is_forced flips this.
        let pid = 0x1200u16;
        let mut reader = TsReader {
            data: ts_stream(pid, &pcs_display(true)),
            pos: 0,
        };
        let mut title = pgs_title(pid, false); // vendor label says NOT forced
        // One sector, which is exactly what the reader serves: an extent that
        // claims more sectors than the source yields is a SHORT read, and a short
        // read is (correctly) inconclusive — see
        // `read_error_after_partial_content_preserves_vendor_forced`.
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1,
        }];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            s.forced,
            "an all-forced PGS track → forced verdict applied onto the stream"
        );
    }

    #[test]
    fn nonforced_display_sets_clear_a_not_yet_forced_track() {
        // A non-forced display set observed on the wire settles the track as
        // not-forced. The vendor label here already agrees, so this pins the
        // verdict itself (the track must not come back FORCED off one non-forced
        // set) without engaging the demotion guard — that gets its own tests.
        let pid = 0x1200u16;
        let mut reader = TsReader {
            data: ts_stream(pid, &pcs_display(false)),
            pos: 0,
        };
        let mut title = pgs_title(pid, false);
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(!s.forced, "a non-forced display set observed → not forced");
    }

    /// What a [`PartialTsReader`] does once its BD-TS payload is exhausted.
    enum ThenWhat {
        /// Fail the read — the data past this point is never seen.
        Error,
        /// Keep serving readable (but PGS-free) sectors forever, so the probe
        /// runs on to the sector budget instead.
        Zeros,
    }

    /// Serves a fixed BD-TS byte stream (as [`TsReader`] does), then either fails
    /// or runs on with zeros. Models the two truncated-run shapes: real content
    /// observed, then the read abandoned mid-title, versus real content observed
    /// and then a designed stop at the budget.
    struct PartialTsReader<'a> {
        inner: TsReader,
        then: ThenWhat,
        /// Cancelled the moment the payload runs out, to model an operator
        /// cancelling MID-run — after real content has been observed.
        cancel: Option<&'a crate::halt::Halt>,
    }
    impl PartialTsReader<'_> {
        fn new(data: Vec<u8>, then: ThenWhat) -> Self {
            Self {
                inner: TsReader { data, pos: 0 },
                then,
                cancel: None,
            }
        }
    }
    impl SectorSource for PartialTsReader<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            if self.inner.pos < self.inner.data.len() {
                return self.inner.read_sectors(lba, count, buf, recovery);
            }
            if let Some(h) = self.cancel.take() {
                h.cancel();
            }
            match self.then {
                ThenWhat::Error => Err(crate::error::Error::DiscRead {
                    sector: lba as u64,
                    status: None,
                    sense: None,
                }),
                ThenWhat::Zeros => {
                    buf.fill(0);
                    Ok(buf.len())
                }
            }
        }
        fn capacity_sectors(&self) -> u32 {
            u32::MAX
        }
    }

    /// A title whose extents need more than one read, so a reader can serve
    /// content on the first call and stop (error / budget) on a later one.
    fn multi_read_pgs_title(pid: u16, vendor_forced: bool) -> DiscTitle {
        let mut t = pgs_title(pid, vendor_forced);
        t.extents = vec![
            Extent {
                start_lba: 0,
                sector_count: 4,
            },
            Extent {
                start_lba: 100,
                sector_count: u32::MAX,
            },
        ];
        t
    }

    #[test]
    fn read_error_after_partial_content_preserves_vendor_forced() {
        // The defect: a forced verdict rests on the ABSENCE of a non-forced
        // display set. When the read dies mid-title that absence means nothing —
        // the rest of the track was never seen — yet the probe used to apply it as
        // authoritative, overwriting the vendor flag from a fraction of the data.
        let pid = 0x1200u16;
        let mut reader = PartialTsReader::new(ts_stream(pid, &pcs_display(true)), ThenWhat::Error);
        let mut title = multi_read_pgs_title(pid, false); // vendor label: NOT forced
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            !s.forced,
            "a forced verdict from a read-truncated prefix must not overwrite the vendor flag"
        );
        // And it must not be memoised: the cache is keyed on the extent list, so a
        // cached inconclusive result would spread this one read fault across every
        // playlist sharing these clips and block any later re-read.
        assert!(
            cache.is_empty(),
            "an inconclusive probe must not be cached against these extents"
        );
    }

    #[test]
    fn read_error_keeps_a_track_that_already_settled_not_forced() {
        // Per-track, not per-title: "not forced" is POSITIVE evidence — a
        // non-forced display set was actually seen — and no amount of unread data
        // could retract it. That verdict survives the truncation even though a
        // forced verdict would not. The vendor label already says not-forced, so
        // what is pinned here is that the truncation does not let the track come
        // back FORCED.
        let pid = 0x1200u16;
        let mut reader = PartialTsReader::new(ts_stream(pid, &pcs_display(false)), ThenWhat::Error);
        let mut title = multi_read_pgs_title(pid, false);
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            !s.forced,
            "an observed non-forced display set stands even when the read was cut short"
        );
        assert!(cache.is_empty(), "still an inconclusive run — not cached");
    }

    #[test]
    fn budget_stop_still_applies_the_forced_verdict() {
        // The budget is a DESIGNED stop, not a failure: the probe's natural exit
        // never fires for a genuinely forced track, so the budget is exactly the
        // mechanism by which a forced verdict gets accepted from a bounded prefix.
        // Classifying it as inconclusive would disable forced detection.
        let pid = 0x1200u16;
        let mut reader = PartialTsReader::new(ts_stream(pid, &pcs_display(true)), ThenWhat::Zeros);
        let mut title = multi_read_pgs_title(pid, false); // vendor label: NOT forced
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            s.forced,
            "a forced verdict from a budget-bounded prefix must still be applied"
        );
        // One entry per (extent, PGS track): both extents reached a designed stop
        // (the first was read to its end, the second stopped at the budget), so
        // both are memoised — the budget is the stop that fires on every disc that
        // HAS a forced track, so excluding it would memoise nothing there.
        assert_eq!(cache.len(), 2, "a conclusive probe is memoised per extent");
        assert!(cache.contains_key(&(0, 4, pid)));
        assert!(cache.contains_key(&(100, u32::MAX, pid)));
    }

    #[test]
    fn cancelled_probe_is_neither_asserted_nor_cached() {
        // A halt lands at an arbitrary chunk boundary, so an absence claim from it
        // is worth no more than one from a read fault. Here the cancel arrives
        // AFTER a forced display set has been observed — the same setup that,
        // uncancelled, reaches the budget and legitimately asserts forced (see
        // budget_stop_still_applies_the_forced_verdict).
        let pid = 0x1200u16;
        let halt = crate::halt::Halt::new();
        let mut reader = PartialTsReader {
            cancel: Some(&halt),
            ..PartialTsReader::new(ts_stream(pid, &pcs_display(true)), ThenWhat::Zeros)
        };
        let mut title = multi_read_pgs_title(pid, false);
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, Some(&halt));
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            !s.forced,
            "a verdict from a cancelled probe must not overwrite the vendor flag"
        );
        // The cancel landed inside the SECOND extent, whose read is therefore an
        // arbitrary prefix: that extent must not be memoised, or the one
        // cancellation would be replayed onto every other playlist sharing the
        // clip. (The first extent WAS read to its end before the cancel, so its
        // own evidence is sound and keeping it is the point of per-extent keying.)
        assert!(
            !cache.contains_key(&(100, u32::MAX, pid)),
            "a cancelled probe must not poison the cancelled extent's cache entry"
        );
    }

    #[test]
    fn no_pgs_streams_is_noop() {
        // A title with no PGS subtitle streams is a no-op (the reader is never
        // touched — a DVD/VobSub or audio-only title).
        let mut reader = ZeroReader { served: 0, cap: 0 };
        let mut title = pgs_title(0x1200, false);
        // Swap the PGS sub for an audio stream so there are no PGS PIDs.
        title.streams.clear();
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert_eq!(reader.served, 0, "no PGS PIDs → no reads");
    }

    // ── per-extent, per-track memoisation ───────────────────────────────────

    /// MEASURED: overlapping-but-not-identical extent lists must not re-read the
    /// shared clips. A disc's playlists share clips without sharing whole extent
    /// LISTS (00800 = [X, Y], 00801 = [X], 00802 = [Y]), and keying the cache on
    /// the whole list de-duplicated only exactly-identical playlists: each of the
    /// three lists missed, so clip X was read twice and Y twice — up to
    /// PROBE_BUDGET_SECTORS (256 MiB) of optical-drive time per miss.
    #[test]
    fn overlapping_extent_lists_read_each_clip_once() {
        let pid = 0x1200u16;
        let x = Extent {
            start_lba: 0,
            sector_count: 600,
        };
        let y = Extent {
            start_lba: 10_000,
            sector_count: 900,
        };
        let mut reader = EndlessReader { served: 0 };
        let mut cache = ForcedProbeCache::new();

        let mut both = pgs_title(pid, true);
        both.extents = vec![x, y];
        probe_and_set_forced(&mut reader, &mut both, &mut cache, None);
        let after_both = reader.served;
        assert_eq!(
            after_both,
            x.sector_count + y.sector_count,
            "the first title reads both clips exactly once"
        );

        // A playlist over X alone, and one over Y alone: every extent is already
        // known, so neither costs a single further sector.
        let mut only_x = pgs_title(pid, true);
        only_x.extents = vec![x];
        probe_and_set_forced(&mut reader, &mut only_x, &mut cache, None);
        let mut only_y = pgs_title(pid, true);
        only_y.extents = vec![y];
        probe_and_set_forced(&mut reader, &mut only_y, &mut cache, None);
        assert_eq!(
            reader.served, after_both,
            "clips shared with an already-probed playlist must not be re-read"
        );

        // And a list that mixes a known extent with a NEW one reads only the new
        // one.
        let z = Extent {
            start_lba: 50_000,
            sector_count: 300,
        };
        let mut mixed = pgs_title(pid, true);
        mixed.extents = vec![x, z];
        probe_and_set_forced(&mut reader, &mut mixed, &mut cache, None);
        assert_eq!(
            reader.served,
            after_both + z.sector_count,
            "a partially-known list reads only the extents it adds"
        );
    }

    /// A later playlist that declares MORE PGS tracks over the SAME extents must
    /// still probe the extra track. With the cache keyed on the extent list alone,
    /// the verdict map it hit had no entry for the new PID, so that track was never
    /// probed and silently kept its vendor-label flag — `info` then reported a
    /// different forced flag for it depending purely on playlist ordering.
    #[test]
    fn extra_pgs_track_over_known_extents_is_still_probed() {
        let ext = Extent {
            start_lba: 0,
            sector_count: 600,
        };
        let mut reader = EndlessReader { served: 0 };
        let mut cache = ForcedProbeCache::new();

        let mut one_track = pgs_title(0x1200, true);
        one_track.extents = vec![ext];
        probe_and_set_forced(&mut reader, &mut one_track, &mut cache, None);
        let after_first = reader.served;
        assert_eq!(after_first, ext.sector_count);

        // Same extents, two declared PGS tracks.
        let mut two_tracks = pgs_title(0x1200, true);
        two_tracks.extents = vec![ext];
        two_tracks.streams.push(Stream::Subtitle(SubtitleStream {
            pid: 0x1201,
            codec: Codec::Pgs,
            language: "fra".into(),
            forced: true,
            qualifier: LabelQualifier::None,
            codec_data: None,
        }));
        probe_and_set_forced(&mut reader, &mut two_tracks, &mut cache, None);
        assert!(
            reader.served > after_first,
            "a newly declared PGS track must be probed, not served from a verdict \
             map that has no entry for it"
        );
        assert!(
            cache.contains_key(&(ext.start_lba, ext.sector_count, 0x1201)),
            "the new track gets its own per-extent evidence"
        );
    }

    /// Records every (lba, count) served and every `set_unit_base` call.
    struct AlignSpy {
        reads: Vec<(u32, u16)>,
        bases: Vec<u32>,
    }
    impl SectorSource for AlignSpy {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.reads.push((lba, count));
            let want = count as usize * SECTOR_BYTES;
            buf[..want].fill(0);
            Ok(want)
        }
        fn capacity_sectors(&self) -> u32 {
            u32::MAX
        }
        fn set_unit_base(&mut self, lba: u32) {
            self.bases.push(lba);
        }
    }

    /// Every probe read must begin on an AACS aligned-unit boundary measured from
    /// the extent's own base, and the probe must declare that base to the source.
    /// A `DecryptingSectorSource` holding AACS keys rejects any other read outright
    /// (`DecryptFailed`) — and with a 1024-sector chunk (`1024 % 3 == 1`) every
    /// read after the first was misaligned, so content-based forced detection was
    /// unreachable past the first chunk of an encrypted disc, silently.
    #[test]
    fn probe_reads_stay_on_aacs_unit_boundaries() {
        let pid = 0x1200u16;
        // A start_lba that is NOT itself 3-aligned, so absolute `lba % 3` and the
        // base-relative gate disagree — the case the gate exists for.
        let base = 4_001u32;
        let mut reader = AlignSpy {
            reads: Vec::new(),
            bases: Vec::new(),
        };
        let mut title = pgs_title(pid, true);
        title.extents = vec![Extent {
            start_lba: base,
            sector_count: CHUNK_SECTORS as u32 * 3,
        }];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);

        assert_eq!(
            reader.bases,
            vec![base],
            "the probe must anchor the source's unit gate at the extent's start_lba"
        );
        assert!(reader.reads.len() > 1, "more than one chunk was read");
        for &(lba, _) in &reader.reads {
            assert!(
                crate::aacs::content::is_unit_aligned(lba, base),
                "read at lba {lba} is not on an aligned-unit boundary from base {base}"
            );
        }
    }

    /// A source that never serves more than `batch` sectors per call, never
    /// erroring — what `PrefetchedSectorSource` does (it returns its producer's
    /// batch, not `count * 2048`). Records what it actually served, per call.
    struct ShortReader {
        batch: u32,
        served: Vec<(u32, u32)>,
    }
    impl SectorSource for ShortReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let give = u32::from(count).min(self.batch);
            self.served.push((lba, give));
            let n = give as usize * SECTOR_BYTES;
            buf[..n].fill(0);
            Ok(n)
        }
        fn capacity_sectors(&self) -> u32 {
            u32::MAX
        }
    }

    /// A short-but-nonzero read must advance by what was READ, not by what was
    /// requested. Advancing by the request skipped the unread tail of every chunk
    /// — silently, with `StopReason` still `Exhausted`, so the absence-based
    /// forced verdict was asserted (and memoised) over sectors nobody read.
    #[test]
    fn short_reads_do_not_skip_sectors() {
        let pid = 0x1200u16;
        let count = CHUNK_SECTORS as u32 * 2;
        let mut reader = ShortReader {
            batch: 64,
            served: Vec::new(),
        };
        let mut title = pgs_title(pid, true);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: count,
        }];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);

        // The served ranges must cover the extent with NO GAP. A residue re-read
        // (the sectors of a partial aligned unit, read again from the unit
        // boundary) is allowed — what must never happen is an unread sector.
        let mut covered = 0u32;
        for &(lba, given) in &reader.served {
            assert!(
                lba <= covered,
                "gap: sectors {covered}..{lba} were never read"
            );
            covered = covered.max(lba + given);
        }
        assert_eq!(
            covered, count,
            "every sector of the extent must be read when the source short-reads"
        );
    }

    /// A short read must not break the aligned-unit invariant the chunk size
    /// exists to hold. `CHUNK_SECTORS` is a multiple of `ALIGNED_UNIT_SECTORS` so
    /// that every read BEGINS on an AACS aligned-unit boundary measured from the
    /// extent base; a source that serves fewer sectors than requested (a 64-sector
    /// prefetch batch: `64 % 3 == 1`) used to advance `lba` by that raw count, so
    /// every subsequent read of the extent was off the unit grid.
    /// `DecryptingSectorSource` rejects those before reading (`DecryptFailed`) →
    /// `ReadFailed` → `absence_is_conclusive()` false → no verdict asserted and
    /// nothing memoised, i.e. content-based forced detection silently degraded to
    /// the vendor label on precisely the encrypted discs it was fixed for.
    #[test]
    fn short_reads_stay_on_aacs_unit_boundaries() {
        let pid = 0x1200u16;
        // A base that is NOT itself 3-aligned, so only the base-relative gate is
        // satisfiable — absolute `lba % 3` would disagree.
        let base = 4_001u32;
        let count = CHUNK_SECTORS as u32 * 2;
        let mut reader = ShortReader {
            batch: 64,
            served: Vec::new(),
        };
        let mut title = pgs_title(pid, true);
        title.extents = vec![Extent {
            start_lba: base,
            sector_count: count,
        }];
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);

        assert!(reader.served.len() > 2, "several short reads happened");
        for &(lba, _) in &reader.served {
            assert!(
                crate::aacs::content::is_unit_aligned(lba, base),
                "read at lba {lba} is off the aligned-unit grid from base {base}"
            );
        }
        // ...and the extent still gets read to its end, so the run reaches a
        // designed stop and its (absence-based) evidence is memoisable.
        let last = reader.served.last().copied().unwrap_or_default();
        assert_eq!(
            last.0 + last.1,
            base + count,
            "the extent must still be read to its end"
        );
        assert!(
            cache.contains_key(&(base, count, pid)),
            "a fully-read extent must be memoised, not discarded as inconclusive"
        );
    }

    /// A source that can never yield a whole aligned unit cannot be advanced past
    /// without leaving the unit grid — so the loop retries a bounded number of
    /// times and then stops. It must NOT spin: the test simply completing is the
    /// assertion, plus a bounded read count and an inconclusive (uncached,
    /// vendor-flag-preserving) outcome.
    #[test]
    fn a_source_below_one_aligned_unit_stops_instead_of_spinning() {
        let pid = 0x1200u16;
        let mut reader = ShortReader {
            batch: 1, // one sector: less than an aligned unit, for ever
            served: Vec::new(),
        };
        let mut title = pgs_title(pid, true);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: CHUNK_SECTORS as u32,
        }];
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);

        assert!(
            reader.served.len() as u32 <= STALL_RETRY_LIMIT + 1,
            "a stalled source must be retried a bounded number of times, got {} reads",
            reader.served.len()
        );
        for &(lba, _) in &reader.served {
            assert_eq!(lba, 0, "a stalled read never advances off the unit grid");
        }
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(s.forced, "inconclusive run keeps the vendor flag");
        assert!(cache.is_empty(), "inconclusive run is not memoised");
    }

    // ── mutation-triage additions ───────────────────────────────────────────

    /// Mutation guard for the `sub.codec == Codec::Pgs` match guard (probe's PID
    /// collection): only PGS subtitle tracks are ever probed by content — DVD
    /// VobSub forced comes from the IFO/vendor path, never from sniffing PGS
    /// segments over non-PGS bytes. If the guard were dropped, a non-PGS
    /// subtitle stream would be treated as a PGS PID and the reader would be
    /// touched even though there is nothing PGS to probe.
    #[test]
    fn non_pgs_subtitle_codec_is_excluded_from_the_probe() {
        let mut reader = EndlessReader { served: 0 };
        let mut title = pgs_title(0x1200, true);
        title.streams = vec![Stream::Subtitle(SubtitleStream {
            pid: 0x1200,
            codec: Codec::DvdSub,
            language: "eng".into(),
            forced: true,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })];
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: u32::MAX,
        }];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert_eq!(
            reader.served, 0,
            "a non-PGS subtitle codec must never be probed as if it were PGS"
        );
    }

    /// Mutation guard for `stalled > STALL_RETRY_LIMIT`: exactly
    /// `STALL_RETRY_LIMIT` retries are allowed (`STALL_RETRY_LIMIT + 1` total
    /// read attempts) before the stalled run gives up. Weakening the
    /// comparison to `==` or `>=` still stops the spin (so a `<=` bound alone
    /// does not catch it), but one retry early — after `STALL_RETRY_LIMIT`
    /// attempts instead of `STALL_RETRY_LIMIT + 1`.
    #[test]
    fn stalled_retries_stop_at_exactly_the_limit() {
        let pid = 0x1200u16;
        let mut reader = ShortReader {
            batch: 1, // never a whole aligned unit
            served: Vec::new(),
        };
        let mut title = pgs_title(pid, true);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: CHUNK_SECTORS as u32,
        }];
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);

        assert_eq!(
            reader.served.len() as u32,
            STALL_RETRY_LIMIT + 1,
            "expected exactly STALL_RETRY_LIMIT + 1 read attempts before giving up, got {}",
            reader.served.len()
        );
    }

    /// `STALL_RETRY_LIMIT` padding TS packets (sync byte only, PID 0 →
    /// `adaptation == 0` → discarded harmlessly by the demuxer) so the real
    /// display set lands at a byte offset that survives a correct
    /// `got * SECTOR_BYTES` feed length but is cut off by a mutated
    /// `got + SECTOR_BYTES`.
    fn filler_packets(count: usize) -> Vec<u8> {
        let mut v = vec![0u8; count * 192];
        for i in 0..count {
            v[i * 192 + 4] = 0x47; // sync byte only; pid 0, adaptation 0 → discarded
        }
        v
    }

    /// Mutation guard for `got as usize * SECTOR_BYTES` (the feed-length
    /// computation on a fully-served chunk): a 3-sector read must hand the
    /// WHOLE 6144-byte chunk to the demuxer. Padding pushes the real display
    /// set to byte 4032 — past `got + SECTOR_BYTES` (2051) but inside
    /// `got * SECTOR_BYTES` (6144) — so a mutated addition would silently
    /// drop it from the feed and the run would never observe it.
    #[test]
    fn feed_uses_the_full_read_length_not_a_truncated_one() {
        let pid = 0x1200u16;
        let mut data = filler_packets(21); // 21 * 192 = 4032 bytes of padding
        data.extend_from_slice(&ts_stream(pid, &pcs_display(true)));
        let mut reader = TsReader { data, pos: 0 };
        let mut title = pgs_title(pid, false); // vendor label: NOT forced
        title.extents = vec![Extent {
            start_lba: 0,
            // 3 * 2048 = 6144 B: covers all 4416 B of real data plus the
            // reader's zero padding to the sector boundary, in one read.
            sector_count: 3,
        }];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            s.forced,
            "the padding-shifted forced display set must still reach the demuxer \
             and raise the forced flag"
        );
    }

    /// A reader that serves fixed content until exhausted, then unlimited
    /// zeros — like [`PartialTsReader`]'s `ThenWhat::Zeros`, but also counts
    /// every sector requested (not just what one extent's read attempted), so
    /// a test can measure how much of a SECOND, effectively infinite extent
    /// actually got read.
    struct RealThenZerosReader {
        inner: TsReader,
        served: u32,
    }
    impl SectorSource for RealThenZerosReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            self.served += count as u32;
            if self.inner.pos < self.inner.data.len() {
                self.inner.read_sectors(lba, count, buf, recovery)
            } else {
                let want = count as usize * SECTOR_BYTES;
                buf[..want].fill(0);
                Ok(want)
            }
        }
        fn capacity_sectors(&self) -> u32 {
            u32::MAX
        }
    }

    /// Mutation guard for the `||` in the early-exit check ("every track has
    /// already shown a non-forced set — counting evidence CARRIED IN from
    /// other extents"): non-forced evidence carried in from a prior extent
    /// must stop reading a later extent immediately, even though that later
    /// extent's OWN fresh tracker has not itself observed anything. Weakening
    /// `||` to `&&` requires local confirmation too, so a huge trailing extent
    /// with no PGS content of its own would be read all the way to the sector
    /// budget instead of one chunk.
    #[test]
    fn carried_non_forced_evidence_stops_reading_a_content_free_extent() {
        let pid = 0x1200u16;
        let mut reader = RealThenZerosReader {
            inner: TsReader {
                data: ts_stream(pid, &pcs_display(false)),
                pos: 0,
            },
            served: 0,
        };
        let mut title = multi_read_pgs_title(pid, false);
        let mut cache = ForcedProbeCache::new();
        probe_and_set_forced(&mut reader, &mut title, &mut cache, None);

        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            !s.forced,
            "non-forced evidence from extent 1 must still clear the forced flag"
        );
        assert!(
            reader.served < PROBE_BUDGET_SECTORS,
            "carried non-forced evidence must stop extent 2's read after a single \
             chunk, not run it to the sector budget; served {}",
            reader.served
        );
    }

    /// A stop reason decides whether the absence of a display set proves
    /// anything — and therefore whether the probe reports the run as truncated.
    #[test]
    fn a_stop_reason_decides_whether_absence_is_conclusive() {
        // The debug line at the end of `probe_and_set_forced` is gated on
        // `!stop.absence_is_conclusive()`. Assert that PREDICATE rather than
        // counting emitted events.
        //
        // Reading a log line back needs a capturing subscriber, which is
        // thread-local, while tracing's callsite-interest cache is global.
        // Those race: the sibling test in disc/encrypt.rs that did this failed
        // roughly one full-suite run in ten while passing every time in
        // isolation, and serialising the captures crate-wide was not enough —
        // the cache can still be re-evaluated against the process default
        // dispatch. A boolean does not need a subscriber to check.
        //
        // ECMA of the decision: a stop that saw everything it was ever going to
        // see (Exhausted) or stopped by DESIGN at the budget is conclusive, so
        // the absence of a display set means the track is not forced and there
        // is nothing to report. A stop that was cut short (Halted, ReadFailed)
        // is not, and that is exactly what the operator needs told.
        assert!(
            StopReason::Exhausted.absence_is_conclusive(),
            "reading every extent to its end is a complete observation"
        );
        assert!(
            StopReason::Budget.absence_is_conclusive(),
            "the budget is a DESIGNED stop: a forced track's display sets appear \
             throughout the title, so a bounded prefix is representative. \
             Treating it as inconclusive would disable forced detection outright"
        );
        assert!(
            !StopReason::ReadFailed.absence_is_conclusive(),
            "a read that died mid-title saw less than the whole; absence proves \
             nothing and the operator must be told"
        );
        assert!(
            !StopReason::Halted.absence_is_conclusive(),
            "a cancelled probe is cut short, not complete"
        );
    }

    // ── sampling: reading where the subtitles actually are ──────────────────
    //
    // SYNTHETIC FIXTURES. The clips below are generated, not captured: they
    // reproduce the measured SHAPE of real discs — a feature's subtitles begin
    // minutes into the title, a full dialogue track carries one to two thousand
    // display sets spread over the whole runtime, a forced-narrative track
    // carries tens — and nothing else. They prove the probe's LOGIC over that
    // shape. They do not prove the shape itself; that comes from measurement on
    // real discs, and if the shape is wrong these fixtures are wrong with it.

    /// One synthetic PGS track: `count` display sets, the first at
    /// `first_sector`, then every `period_sectors`.
    ///
    /// `first_sector` and `period_sectors` must be multiples of
    /// `ALIGNED_UNIT_SECTORS`: a BD-TS packet is 192 bytes and the demuxer works
    /// on that grid from the start of each feed, so only every third sector
    /// (3 * 2048 = 32 * 192) begins on the grid.
    #[derive(Clone, Copy)]
    struct TrackShape {
        pid: u16,
        first_sector: u32,
        period_sectors: u32,
        count: u32,
        forced: bool,
    }

    /// A feature-length clip: zeros everywhere except where a [`TrackShape`] puts
    /// a display set. Serves any LBA asked for (unlike [`TsReader`], which must be
    /// read in order) — which is exactly what a sampling probe has to be tested
    /// against.
    struct SyntheticClipReader {
        tracks: Vec<TrackShape>,
        served: u32,
        reads: Vec<(u32, u32)>,
    }

    impl SyntheticClipReader {
        fn new(tracks: Vec<TrackShape>) -> Self {
            for t in &tracks {
                assert!(
                    t.first_sector
                        .is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS)
                        && t.period_sectors
                            .is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS),
                    "a synthetic display set must sit on the BD-TS packet grid"
                );
            }
            Self {
                tracks,
                served: 0,
                reads: Vec::new(),
            }
        }

        /// Distinct read regions, i.e. runs of reads with no gap between them —
        /// one per sample window the probe actually visited.
        fn regions(&self) -> Vec<(u32, u32)> {
            let mut sorted = self.reads.clone();
            sorted.sort_unstable();
            let mut out: Vec<(u32, u32)> = Vec::new();
            for (lba, count) in sorted {
                match out.last_mut() {
                    Some(last) if lba <= last.1 => last.1 = last.1.max(lba + count),
                    _ => out.push((lba, lba + count)),
                }
            }
            out
        }
    }

    impl SectorSource for SyntheticClipReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let want = count as usize * SECTOR_BYTES;
            buf[..want].fill(0);
            self.served += u32::from(count);
            self.reads.push((lba, u32::from(count)));
            let end = u64::from(lba) + u64::from(count);
            for (idx, t) in self.tracks.iter().enumerate() {
                for i in 0..t.count {
                    let at = u64::from(t.first_sector) + u64::from(i) * u64::from(t.period_sectors);
                    if at < u64::from(lba) || at >= end {
                        continue;
                    }
                    // Slot per track so two tracks sharing a sector do not
                    // overwrite each other; both stay on the 192-byte grid.
                    let off = (at - u64::from(lba)) as usize * SECTOR_BYTES + idx * 192;
                    let pkt = bd_pes_packet(t.pid, (i % 16) as u8, &pcs_display(t.forced));
                    if off + pkt.len() <= want {
                        buf[off..off + pkt.len()].copy_from_slice(&pkt);
                    }
                }
            }
            Ok(want)
        }
        fn capacity_sectors(&self) -> u32 {
            u32::MAX
        }
    }

    /// A feature-length clip: ~4 GB, one extent.
    fn feature_extent() -> Extent {
        Extent {
            start_lba: 0,
            sector_count: 2_000_000,
        }
    }

    fn subtitle_stream(pid: u16, forced: bool) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }

    fn forced_flag(title: &DiscTitle, pid: u16) -> bool {
        title
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Subtitle(sub) if sub.pid == pid => Some(sub.forced),
                _ => None,
            })
            .expect("track present")
    }

    /// THE headline fix. A feature's subtitles do not start at the top of the
    /// title — measured, they begin well past the first 256 MiB — so a head-first
    /// budget read the opening minute of black and logos, observed NOTHING, and
    /// contributed nothing to the verdict on any disc long enough to matter.
    /// Spreading the SAME budget over the extent puts windows where the subtitles
    /// are.
    #[test]
    fn subtitles_beyond_the_old_head_budget_are_observed() {
        let pid = 0x1200u16;
        let mut reader = SyntheticClipReader::new(vec![TrackShape {
            pid,
            // Three times the entire old budget into the clip.
            first_sector: 3 * PROBE_BUDGET_SECTORS,
            period_sectors: 300,
            count: 2_000,
            forced: true,
        }]);
        let mut title = pgs_title(pid, false); // vendor label: NOT forced
        title.extents = vec![feature_extent()];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert!(
            forced_flag(&title, pid),
            "the probe must observe a track whose subtitles begin past the old \
             head-first budget and apply its verdict"
        );
        assert!(
            reader.served <= PROBE_BUDGET_SECTORS,
            "and must do it inside the same sector budget as before, got {}",
            reader.served
        );
    }

    /// The allocation, not just the total: the budget must be spread across the
    /// extent instead of poured into its head.
    ///
    /// The track here is forced throughout, so it never settles and the run
    /// spends its whole budget — which is precisely the run whose ALLOCATION
    /// matters. (A track disproven by its first display set stops the run early,
    /// by design; that is the per-track exit, tested separately.)
    #[test]
    fn the_budget_is_spread_across_the_extent() {
        let pid = 0x1200u16;
        let ext = feature_extent();
        let mut reader = SyntheticClipReader::new(vec![TrackShape {
            pid,
            first_sector: 3 * PROBE_BUDGET_SECTORS,
            period_sectors: 300,
            count: 2_000,
            forced: true,
        }]);
        let mut title = pgs_title(pid, false);
        title.extents = vec![ext];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);

        let regions = reader.regions();
        assert!(
            regions.len() >= 8,
            "the budget must be spent in many separate places, got {} region(s)",
            regions.len()
        );
        let furthest = regions.iter().map(|r| r.1).max().unwrap_or(0);
        assert!(
            furthest >= ext.sector_count / 10 * 9,
            "the sample must reach the far end of the extent; furthest read ended \
             at {furthest} of {}",
            ext.sector_count
        );
    }

    /// Per-track early exit, from the budget's point of view: a track that is
    /// already disproven, and whose label needs no correcting, asks for nothing —
    /// so an extent that only owes evidence for THAT track is not read at all.
    #[test]
    fn a_settled_track_buys_no_further_reads() {
        let pid = 0x1200u16;
        let known = Extent {
            start_lba: 0,
            sector_count: 1,
        };
        let mut cache = ForcedProbeCache::new();
        let mut reader = TsReader {
            data: ts_stream(pid, &pcs_display(false)),
            pos: 0,
        };
        let mut first = pgs_title(pid, false);
        first.extents = vec![known];
        probe_and_set_forced(&mut reader, &mut first, &mut cache, None);
        assert!(
            !cache.is_empty(),
            "the first title must have settled the track as not forced"
        );

        // A second playlist over the same clip PLUS a huge unread one. The track
        // is already disproven and its label already agrees, so there is nothing
        // the new clip could teach: not one sector of it may be read.
        let mut endless = EndlessReader { served: 0 };
        let mut second = pgs_title(pid, false);
        second.extents = vec![
            known,
            Extent {
                start_lba: 500_000,
                sector_count: u32::MAX,
            },
        ];
        probe_and_set_forced(&mut endless, &mut second, &mut cache, None);
        assert_eq!(
            endless.served, 0,
            "budget must go to undecided tracks only; a settled track read {} sectors",
            endless.served
        );
    }

    /// The memoisation hazard. A sampled read covers a FRACTION of an extent, so
    /// its evidence is a statement about that fraction — but it used to be filed
    /// under the extent's full key and replayed to every other playlist sharing
    /// the clip, including playlists that would have read far more of it. An
    /// absence claim ("no non-forced display set here") inherited that way asserts
    /// a completeness the read never had.
    #[test]
    fn a_thin_sample_is_not_replayed_to_a_playlist_that_would_read_more() {
        let pid = 0x1200u16;
        let clip = Extent {
            start_lba: 0,
            sector_count: 1_200_000,
        };
        let filler = Extent {
            start_lba: 2_000_000,
            sector_count: 3_600_000,
        };
        // What each playlist's plan covers of `clip`: the four-extent playlist
        // gets a quarter of the budget for it, the single-extent one gets all of
        // it — so the second reads far more of the same clip.
        let total = u64::from(clip.sector_count) + u64::from(filler.sector_count);
        let thin_share =
            (u64::from(PROBE_BUDGET_SECTORS) * u64::from(clip.sector_count) / total) as u32;
        let thin = plan_windows(clip.sector_count, thin_share);
        let full = plan_windows(clip.sector_count, PROBE_BUDGET_SECTORS);
        // A sector the thorough plan reads and the thin one does not.
        let covered_by = |plan: &[SampleWindow], s: u32| {
            plan.iter().any(|w| s >= w.offset && s < w.offset + w.len)
        };
        let hidden = full
            .iter()
            .flat_map(|w| (0..w.len / 3).map(move |k| w.offset + k * 3))
            .find(|&s| !covered_by(&thin, s))
            .expect("the thorough plan reads sectors the thin one misses");

        // The track is forced everywhere the thin sample looks, and NOT forced at
        // the one place only the thorough plan reaches.
        let shapes = vec![
            TrackShape {
                pid,
                first_sector: 0,
                period_sectors: 300,
                count: 4_000,
                forced: true,
            },
            TrackShape {
                pid,
                first_sector: hidden,
                period_sectors: 3,
                count: 1,
                forced: false,
            },
        ];
        let mut cache = ForcedProbeCache::new();
        let mut thin_reader = SyntheticClipReader::new(shapes.clone());
        let mut thin_title = pgs_title(pid, false);
        thin_title.extents = vec![clip, filler];
        probe_and_set_forced(&mut thin_reader, &mut thin_title, &mut cache, None);
        assert!(
            forced_flag(&thin_title, pid),
            "precondition: the thin sample sees only forced display sets"
        );

        let mut full_reader = SyntheticClipReader::new(shapes);
        let mut full_title = pgs_title(pid, false);
        full_title.extents = vec![clip];
        probe_and_set_forced(&mut full_reader, &mut full_title, &mut cache, None);
        assert!(
            full_reader.served > 0,
            "a playlist that would cover more of the clip must re-read it, not \
             inherit a thinner sample's answer"
        );
        assert!(
            !forced_flag(&full_title, pid),
            "the non-forced display set only the thorough plan reaches must decide \
             that playlist's verdict"
        );
    }

    // ── demotion: when content may contradict a vendor label ────────────────

    /// The case the guard exists for. On a disc whose authoring never sets
    /// `forced_on_flag` — measured: not one track on the disc carries it — the
    /// absence of the flag says nothing whatsoever about any track. Demoting on it
    /// would strip the correct forced label off every forced track on every such
    /// disc.
    #[test]
    fn a_disc_that_never_sets_the_forced_flag_cannot_demote_anything() {
        let pid = 0x1200u16;
        let mut reader = SyntheticClipReader::new(vec![TrackShape {
            pid,
            first_sector: 300_000,
            period_sectors: 300,
            count: 2_000,
            forced: false,
        }]);
        let mut title = pgs_title(pid, true); // vendor label: forced
        title.extents = vec![feature_extent()];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert!(
            forced_flag(&title, pid),
            "with no track on the disc using forced_on_flag, absence proves nothing \
             and the vendor label must stand"
        );
    }

    /// ...and when another track DOES use the flag, the authoring house
    /// demonstrably sets it, so a busy track with none is a full dialogue track
    /// mislabelled forced — the defect this fixes.
    #[test]
    fn content_demotes_a_wrong_forced_label_when_a_sibling_track_uses_the_flag() {
        let mislabelled = 0x1200u16;
        let genuine = 0x1201u16;
        let mut reader = SyntheticClipReader::new(vec![
            TrackShape {
                pid: mislabelled,
                first_sector: 300_000,
                period_sectors: 300,
                count: 2_000,
                forced: false,
            },
            TrackShape {
                pid: genuine,
                first_sector: 300_003,
                period_sectors: 3_000,
                count: 200,
                forced: true,
            },
        ]);
        let mut title = pgs_title(mislabelled, true); // vendor label: forced
        title.streams.push(subtitle_stream(genuine, false));
        title.codec_privates.push(None);
        title.extents = vec![feature_extent()];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert!(
            !forced_flag(&title, mislabelled),
            "a busy track with no forced display set, on a disc that provably uses \
             the flag, must lose the wrong label"
        );
        assert!(
            forced_flag(&title, genuine),
            "and the track that is actually forced must be flagged forced"
        );
    }

    /// The other side of the guard: a track with the SHAPE of a forced track
    /// (measured: tens of display sets against a full track's thousands) keeps its
    /// label even on a disc that uses the flag. The flag being in use elsewhere
    /// does not oblige every forced track to carry it.
    #[test]
    fn a_forced_shaped_track_keeps_its_label_on_a_disc_that_uses_the_flag() {
        let small = 0x1200u16;
        let full = 0x1201u16;
        let ext = feature_extent();
        // Put the small track's handful of display sets inside one sample window,
        // so it is genuinely OBSERVED (several sets, none forced) and the verdict
        // turns on its shape rather than on having seen nothing.
        let window = plan_windows(ext.sector_count, PROBE_BUDGET_SECTORS)[8];
        let mut reader = SyntheticClipReader::new(vec![
            TrackShape {
                pid: small,
                first_sector: window.offset + 3,
                period_sectors: 300,
                count: 20,
                forced: false,
            },
            TrackShape {
                pid: full,
                first_sector: 100_002,
                period_sectors: 300,
                count: 2_000,
                forced: true,
            },
        ]);
        let mut title = pgs_title(small, true); // vendor label: forced
        title.streams.push(subtitle_stream(full, false));
        title.codec_privates.push(None);
        title.extents = vec![ext];
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        assert!(
            forced_flag(&title, small),
            "a track the size of a forced-narrative track must keep its label even \
             where the flag is in use"
        );
    }

    // ── the sampling plan itself ────────────────────────────────────────────

    #[test]
    fn a_small_extent_is_read_whole_not_sampled() {
        let plan = plan_windows(5_000, PROBE_BUDGET_SECTORS);
        assert_eq!(
            plan,
            vec![SampleWindow {
                offset: 0,
                len: 5_000
            }],
            "an extent that fits in its share is read end to end — the complete \
             answer, and the only one that may be cached as complete"
        );
    }

    #[test]
    fn a_plan_stays_on_the_unit_grid_inside_the_extent_and_within_budget() {
        // Sizes around the interesting boundaries: below/at/above one window, and
        // a feature-sized clip.
        for &sectors in &[3u32, 8_191, 200_000, 2_000_000, u32::MAX] {
            for &share in &[
                0,
                1,
                MIN_WINDOW_SECTORS,
                WINDOW_SECTORS,
                PROBE_BUDGET_SECTORS,
            ] {
                let plan = plan_windows(sectors, share);
                let mut prev_end = 0u64;
                for w in &plan {
                    assert!(
                        w.offset
                            .is_multiple_of(crate::aacs::content::ALIGNED_UNIT_SECTORS),
                        "window {w:?} starts off the AACS unit grid ({sectors}, {share})"
                    );
                    assert!(
                        u64::from(w.offset) + u64::from(w.len) <= u64::from(sectors),
                        "window {w:?} runs past the extent ({sectors}, {share})"
                    );
                    assert!(
                        u64::from(w.offset) >= prev_end,
                        "window {w:?} overlaps the previous one ({sectors}, {share})"
                    );
                    prev_end = u64::from(w.offset) + u64::from(w.len);
                }
                assert!(
                    plan.len() as u32 <= MAX_WINDOWS_PER_EXTENT,
                    "too many windows for ({sectors}, {share})"
                );
            }
        }
    }

    #[test]
    fn a_sampled_plan_reaches_the_end_of_the_extent() {
        let sectors = 2_000_000u32;
        let plan = plan_windows(sectors, PROBE_BUDGET_SECTORS);
        assert!(plan.len() > 1, "a feature-sized extent must be sampled");
        let last = plan.last().copied().expect("non-empty");
        // The final window ends AT the extent's end (bar the unit-grid rounding of
        // its start), so the sample spans the whole clip rather than its head.
        assert!(
            last.offset + last.len + crate::aacs::content::ALIGNED_UNIT_SECTORS >= sectors,
            "the last window ends at {} of {sectors}",
            last.offset + last.len
        );
    }

    #[test]
    fn planned_coverage_matches_the_plan_and_respects_the_share() {
        for &share in &[MIN_WINDOW_SECTORS, WINDOW_SECTORS, PROBE_BUDGET_SECTORS] {
            let sectors = 2_000_000u32;
            let plan = plan_windows(sectors, share);
            let summed: u32 = plan.iter().map(|w| w.len).sum();
            assert_eq!(planned_coverage(sectors, share), summed);
            assert!(
                summed <= share.max(MIN_WINDOW_SECTORS),
                "a plan may not spend more than its share ({summed} > {share})"
            );
        }
    }

    /// Coverage is what makes a memo replayable. An entry from a thin sample must
    /// not answer a question that needs a thorough one — but positive evidence
    /// (a non-forced display set was SEEN) settles the track whatever the
    /// coverage, and a complete read answers everything.
    #[test]
    fn cached_evidence_answers_only_what_its_coverage_supports() {
        let absence = CachedEvidence {
            evidence: TrackEvidence {
                observed: true,
                non_forced: false,
                forced_seen: true,
                displays: 4,
            },
            covered: 1_000,
            complete: false,
        };
        assert!(absence.answers(1_000), "as much coverage as asked for");
        assert!(
            !absence.answers(1_001),
            "an absence claim must not answer for sectors nobody read"
        );
        let positive = CachedEvidence {
            evidence: TrackEvidence {
                observed: true,
                non_forced: true,
                ..Default::default()
            },
            covered: 1,
            complete: false,
        };
        assert!(
            positive.answers(u32::MAX),
            "a non-forced display set was seen on the wire; no further reading \
             could retract it"
        );
        let whole = CachedEvidence {
            evidence: TrackEvidence {
                observed: true,
                ..Default::default()
            },
            covered: 10,
            complete: true,
        };
        assert!(
            whole.answers(u32::MAX),
            "the extent was read end to end; there is nothing left to cover"
        );
    }
}
