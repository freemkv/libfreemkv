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
/// Read the clip in 2 MiB chunks.
const CHUNK_SECTORS: u16 = 1024;

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

/// Memoises probe results across titles. Keyed by the title's exact extent
/// list, so a hit returns a result computed from byte-identical input — many
/// playlists on one disc reference the same clips (main feature, play-all,
/// seamless-branch variants), and without this the same physical extents are
/// re-read from the drive once per playlist.
pub(crate) type ForcedProbeCache = HashMap<Vec<(u32, u32)>, HashMap<u16, bool>>;

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

    // Same extents → same verdicts. Serve from cache rather than re-reading.
    let key: Vec<(u32, u32)> = title
        .extents
        .iter()
        .map(|e| (e.start_lba, e.sector_count))
        .collect();
    if let Some(hit) = cache.get(&key) {
        apply_verdicts(title, hit);
        return;
    }

    let mut demux = TsDemuxer::new(&pg_pids);
    let mut parsers: HashMap<u16, PgsParser> =
        pg_pids.iter().map(|&p| (p, PgsParser::new())).collect();
    let mut trackers: HashMap<u16, ForcedTracker> =
        pg_pids.iter().map(|&p| (p, ForcedTracker::new())).collect();

    let extents = title.extents.clone();
    let mut buf = vec![0u8; CHUNK_SECTORS as usize * SECTOR_BYTES];
    let mut sectors_read: u32 = 0;
    // Record WHY the loop ended rather than leaving it implicit in the control
    // flow: every exit below names its reason, and the reason decides what may be
    // asserted from what was observed.
    let stop = 'outer: {
        for ext in &extents {
            let mut lba = ext.start_lba;
            let mut remaining = ext.sector_count;
            while remaining > 0 {
                // Bounded work and a responsive cancel: without these the probe
                // reads the entire title whenever a track really is forced.
                if halt.is_some_and(|h| h.is_cancelled()) {
                    break 'outer StopReason::Halted;
                }
                if sectors_read >= PROBE_BUDGET_SECTORS {
                    break 'outer StopReason::Budget;
                }
                let budget_left = PROBE_BUDGET_SECTORS - sectors_read;
                let count = remaining.min(CHUNK_SECTORS as u32).min(budget_left) as u16;
                let want = count as usize * SECTOR_BYTES;
                let n = match reader.read_sectors(lba, count, &mut buf[..want], false) {
                    Ok(n) => n,
                    // Best-effort — stop reading, but the data past here was never
                    // seen, so the observation is a truncated prefix.
                    Err(_) => break 'outer StopReason::ReadFailed,
                };
                if n == 0 {
                    // Short read: the extent claimed sectors the source would not
                    // yield. Same truncated prefix as an error.
                    break 'outer StopReason::ReadFailed;
                }
                for pes in demux.feed(&buf[..n]) {
                    if let (Some(parser), Some(tracker)) =
                        (parsers.get_mut(&pes.pid), trackers.get_mut(&pes.pid))
                    {
                        for frame in parser.parse(&pes) {
                            tracker.observe(&frame.data);
                        }
                    }
                }
                // Every track has already shown a non-forced set → nothing left to
                // learn; stop reading the (huge) clip.
                if trackers.values().all(ForcedTracker::settled_not_forced) {
                    break 'outer StopReason::Exhausted;
                }
                lba += count as u32;
                remaining -= count as u32;
                sectors_read += count as u32;
            }
        }
        StopReason::Exhausted
    };

    // Drain any buffered final display set.
    for (pid, parser) in parsers.iter_mut() {
        if let Some(tracker) = trackers.get_mut(pid) {
            for frame in parser.flush() {
                tracker.observe(&frame.data);
            }
        }
    }

    // Collect the verdicts we are entitled to assert and apply them. A track
    // absent from the map keeps its vendor-derived flag.
    //
    // Two gates, both PER TRACK, because the evidence is per track:
    //   * `observed()` — saw no display set at all, so nothing is known. (Never
    //     assert "not forced" from having seen nothing.)
    //   * on a truncated run, `settled_not_forced()` — the track saw an actual
    //     non-forced display set, which no further reading could retract, so that
    //     verdict stands even though the run was cut short. A track that merely
    //     hadn't YET seen a non-forced set is exactly the claim the truncation
    //     invalidates, so it is dropped and keeps the vendor flag.
    let conclusive = stop.absence_is_conclusive();
    let verdicts: HashMap<u16, bool> = trackers
        .iter()
        .filter(|(_, t)| t.observed() && (conclusive || t.settled_not_forced()))
        .map(|(&pid, t)| (pid, t.is_forced()))
        .collect();
    // Only memoise a run that reached a designed stop. The cache key is the
    // extent list, so caching a truncated run would replay one read fault (or one
    // cancellation) onto every other playlist that shares these clips, and a later
    // title would never get the chance to re-read them successfully.
    if conclusive {
        cache.insert(key, verdicts.clone());
    } else {
        tracing::debug!(
            target: "freemkv::scan",
            stop = ?stop,
            sectors_read,
            asserted = verdicts.len(),
            tracks = pg_pids.len(),
            "forced-subtitle probe truncated; verdicts limited and not cached"
        );
    }
    apply_verdicts(title, &verdicts);
}

/// Set `forced` on every PGS subtitle track named in `verdicts`. A track absent
/// from the map was never observed and keeps its vendor-derived flag.
fn apply_verdicts(title: &mut DiscTitle, verdicts: &HashMap<u16, bool>) {
    for s in &mut title.streams {
        if let Stream::Subtitle(sub) = s {
            if sub.codec == Codec::Pgs {
                if let Some(&forced) = verdicts.get(&sub.pid) {
                    sub.forced = forced;
                }
            }
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
            Ok(n)
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
        assert_eq!(cache.len(), 1, "a conclusive probe is memoised");
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
        assert!(
            cache.is_empty(),
            "a cancelled probe must not poison the extent cache"
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
}
