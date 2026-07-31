//! Content-based forced-subtitle detection for Blu-ray/UHD PGS tracks.
//!
//! `freemkv info` and the muxer must agree on which subtitle tracks are forced.
//! The muxer derives it from the PGS `forced_on_flag` while muxing a rip; this
//! module gives `info` the SAME verdict up front by reading the title's PGS
//! streams and feeding them through the one shared classifier
//! ([`crate::mux::codec::pgs::ForcedTracker`]) — so the two never diverge.
//!
//! Cost: a track is only confirmed forced once EVERY display set is seen to be
//! forced, so a disc that has a forced track is read through — the
//! accuracy-over-speed tradeoff `info` opts into. Full tracks early-exit as soon
//! as they show a single non-forced subtitle, and a whole run stops early once
//! every track has settled.
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
/// the title's whole extent set, tens of GB, at optical-drive speed. A forced
/// track's display sets appear throughout the title, so a bounded prefix is
/// enough to classify it; the budget only decides how long we keep looking for a
/// non-forced set before accepting the forced verdict.
const PROBE_BUDGET_SECTORS: u32 = 131_072;

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
}

impl TrackEvidence {
    fn merge(&mut self, other: Self) {
        self.observed |= other.observed;
        self.non_forced |= other.non_forced;
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
/// as an extent's answer.
pub(crate) type ForcedProbeCache = HashMap<(u32, u32, u16), TrackEvidence>;

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

    // Same extent → same evidence. Take from the cache what is already known and
    // read only the extents that are not (for every declared track).
    let mut evidence: HashMap<u16, TrackEvidence> = pg_pids
        .iter()
        .map(|&p| (p, TrackEvidence::default()))
        .collect();
    let mut todo: Vec<crate::disc::Extent> = Vec::new();
    for ext in &title.extents {
        let hits: Option<Vec<TrackEvidence>> = pg_pids
            .iter()
            .map(|&p| cache.get(&(ext.start_lba, ext.sector_count, p)).copied())
            .collect();
        match hits {
            Some(known) => {
                for (&pid, ev) in pg_pids.iter().zip(known) {
                    if let Some(slot) = evidence.get_mut(&pid) {
                        slot.merge(ev);
                    }
                }
            }
            // At least one declared track has no evidence for this extent — read
            // it. (A playlist that declares a PGS PID a previous playlist did not
            // lands here, so the extra track is genuinely probed.)
            None => todo.push(*ext),
        }
    }
    if todo.is_empty() {
        // Every extent's evidence came from a run that reached a designed stop, so
        // an absence claim over the composed evidence is as sound as the run that
        // produced each part.
        apply_verdicts(title, &verdicts(&evidence, true));
        return;
    }

    let mut buf = vec![0u8; CHUNK_SECTORS as usize * SECTOR_BYTES];
    let mut sectors_read: u32 = 0;
    // Record WHY the loop ended rather than leaving it implicit in the control
    // flow: every exit below names its reason, and the reason decides what may be
    // asserted from what was observed.
    let mut stop = StopReason::Exhausted;
    'outer: for ext in &todo {
        // Demux/parse state is PER EXTENT, so the evidence an extent yields is
        // derived from that extent's own bytes and nothing else — which is what
        // makes the per-extent cache entry mean what it claims, and is required
        // now that a cache hit can make the read skip an extent in the middle of
        // the title (a demuxer carried across a skipped extent would splice two
        // non-adjacent byte runs into one PES). Each extent is a clip's own
        // contiguous run, so this loses at most a display set that straddles an
        // extent boundary of a fragmented file.
        let mut demux = TsDemuxer::new(&pg_pids);
        let mut parsers: HashMap<u16, PgsParser> =
            pg_pids.iter().map(|&p| (p, PgsParser::new())).collect();
        let mut trackers: HashMap<u16, ForcedTracker> =
            pg_pids.iter().map(|&p| (p, ForcedTracker::new())).collect();
        // AACS aligned units are anchored at THIS extent's start LBA, so tell a
        // decrypt-on-read source to gate relative to it rather than absolute disc
        // LBA 0 — without this the very first read of a clip whose start_lba is
        // not itself 3-aligned is rejected. Mirrors the mux read paths.
        reader.set_unit_base(ext.start_lba);
        let mut lba = ext.start_lba;
        let mut remaining = ext.sector_count;
        // `None` = this extent was read to its end, so its evidence is complete
        // and may be memoised. `Some(reason)` = the read stopped early.
        let mut cut_short: Option<StopReason> = None;
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
            lba += got;
            remaining -= got;
            sectors_read += got;
            // Every track has already shown a non-forced set — counting the
            // evidence carried in from other extents — so there is nothing left to
            // learn; stop reading the (huge) clip.
            if pg_pids.iter().all(|p| {
                let carried = evidence.get(p).copied().unwrap_or_default().non_forced;
                carried
                    || trackers
                        .get(p)
                        .is_some_and(ForcedTracker::settled_not_forced)
            }) {
                cut_short = Some(StopReason::Exhausted);
                break;
            }
        }

        // Drain any buffered final display set of THIS extent.
        for (pid, parser) in parsers.iter_mut() {
            if let Some(tracker) = trackers.get_mut(pid) {
                for frame in parser.flush() {
                    tracker.observe(&frame.data);
                }
            }
        }

        // Fold this extent's evidence in, and memoise it if the extent's read
        // reached a DESIGNED stop — read to its end, stopped at the sector budget,
        // or stopped because every track had already settled. The budget is a
        // designed stop for exactly the reason [`StopReason`] documents (a forced
        // track's display sets appear throughout, so a bounded prefix is
        // representative), and it is the stop that fires on every disc that HAS a
        // forced track — excluding it from the cache would mean nothing is ever
        // memoised on precisely those discs.
        //
        // A halt or a read fault is different: the cut-off point is arbitrary, so
        // its evidence is real for THIS title (nothing observed is retracted) but
        // must not be frozen in as the extent's answer, or one transient fault
        // would be replayed onto every other playlist sharing the clip.
        let cacheable = cut_short.is_none_or(StopReason::absence_is_conclusive);
        for (&pid, t) in trackers.iter() {
            let ev = TrackEvidence {
                observed: t.observed(),
                non_forced: t.settled_not_forced(),
            };
            if let Some(slot) = evidence.get_mut(&pid) {
                slot.merge(ev);
            }
            if cacheable {
                cache.insert((ext.start_lba, ext.sector_count, pid), ev);
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

/// Compose the per-track verdicts a run is ENTITLED to assert from the evidence
/// it gathered. A track absent from the result keeps its vendor-derived flag.
///
/// Two gates, both PER TRACK, because the evidence is per track:
///   * `observed` — saw no display set at all, so nothing is known. (Never assert
///     "not forced" from having seen nothing.)
///   * on a truncated run, `non_forced` — the track saw an actual non-forced
///     display set, which no further reading could retract, so that verdict
///     stands even though the run was cut short. A track that merely hadn't YET
///     seen a non-forced set is exactly the claim the truncation invalidates, so
///     it is dropped and keeps the vendor flag.
fn verdicts(evidence: &HashMap<u16, TrackEvidence>, conclusive: bool) -> HashMap<u16, bool> {
    evidence
        .iter()
        .filter(|(_, e)| e.observed && (conclusive || e.non_forced))
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
        assert_eq!(
            reader.served, PROBE_BUDGET_SECTORS,
            "the probe must stop at exactly the budget, not read the whole extent"
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
    fn nonforced_display_sets_clear_forced_verdict() {
        // A non-forced display set observed on the wire overrides a vendor-forced
        // label → the track settles as not-forced.
        let pid = 0x1200u16;
        let mut reader = TsReader {
            data: ts_stream(pid, &pcs_display(false)),
            pos: 0,
        };
        let mut title = pgs_title(pid, true); // vendor label says forced
        probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            !s.forced,
            "a non-forced display set observed → forced verdict cleared"
        );
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
        // forced verdict would not.
        let pid = 0x1200u16;
        let mut reader = PartialTsReader::new(ts_stream(pid, &pcs_display(false)), ThenWhat::Error);
        let mut title = multi_read_pgs_title(pid, true); // vendor label: forced
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
        data.extend_from_slice(&ts_stream(pid, &pcs_display(false)));
        let mut reader = TsReader { data, pos: 0 };
        let mut title = pgs_title(pid, true); // vendor label: forced
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
            !s.forced,
            "the padding-shifted non-forced display set must still reach the demuxer \
             and clear the vendor-forced flag"
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
        let mut title = multi_read_pgs_title(pid, true); // vendor label: forced
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

    /// Counts `tracing` events on target `freemkv::scan`, so a test can prove
    /// a debug log fires (or doesn't) without depending on any output
    /// formatting.
    #[derive(Clone)]
    struct ScanDebugCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl tracing::Subscriber for ScanDebugCounter {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "freemkv::scan"
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() == "freemkv::scan" {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// Mutation guard for the `!` in `if !conclusive { tracing::debug!(...) }`:
    /// the "truncated; verdicts limited" log must fire exactly on an
    /// INCONCLUSIVE run, never on one that reached a designed stop.
    #[test]
    fn truncated_run_logs_but_a_conclusive_run_does_not() {
        let pid = 0x1200u16;

        // Conclusive: one exactly-sized read, extent read to its end, no stall.
        let conclusive_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Serialised crate-wide — see `harness::with_captured_tracing`. These
        // race the capture in disc/encrypt.rs otherwise: the dispatch is
        // thread-local but the callsite-interest cache is global.
        crate::harness::with_captured_tracing(ScanDebugCounter(conclusive_count.clone()), || {
            let mut reader = TsReader {
                data: ts_stream(pid, &pcs_display(true)),
                pos: 0,
            };
            let mut title = pgs_title(pid, false);
            title.extents = vec![Extent {
                start_lba: 0,
                sector_count: 1,
            }];
            probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        });
        assert_eq!(
            conclusive_count.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a conclusive (Exhausted) run must not log the truncation debug message"
        );

        // Inconclusive: dies mid-title with a read error → ReadFailed.
        let truncated_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::harness::with_captured_tracing(ScanDebugCounter(truncated_count.clone()), || {
            let mut reader =
                PartialTsReader::new(ts_stream(pid, &pcs_display(true)), ThenWhat::Error);
            let mut title = multi_read_pgs_title(pid, false);
            probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
        });
        assert_eq!(
            truncated_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a truncated (ReadFailed) run must log the truncation debug message exactly once"
        );
    }
}
