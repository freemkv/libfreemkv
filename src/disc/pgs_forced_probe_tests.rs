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
        selection_evidence: Default::default(),
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
    // ...and must actually SPEND it: quietly reading a fraction would look
    // "safe" while observing less than the head-first read it replaced.
    // One window's slack is allowed (windows need not divide the budget exactly).
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

// A reader that serves a fixed BD-TS byte stream once, then EOF, so the probe's
// demux→parse→observe→apply path runs on real synthetic PGS content. Sector-granular like
// every real `SectorSource`
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

// PGS PCS layout (matches mux::codec::pgs): a display-set frame begins with a
// PCS (segment type 0x16); byte 13 is number_of_composition_objects; byte 17
// is the first object's flags, whose 0x40 bit is forced_on_flag.
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
    // Stuff through the adaptation field so the PES ends exactly at its last
    // ES byte rather than carrying zero padding after it.
    let len = es_off + n - p;
    if len < 184 {
        let start = 192 - len;
        pkt.copy_within(p..p + len, start);
        pkt[7] |= 0x20;
        pkt[8] = (start - 9) as u8;
        if start > 9 {
            pkt[9] = 0;
            pkt[10..start].fill(0xFF);
        }
    }
    pkt
}

// Two BD-TS PES on `pid`, both carrying `es`: an open PES only completes when the next PES
// start arrives. The follower deliberately repeats `es` rather than a fixed filler.
fn ts_stream(pid: u16, es: &[u8]) -> Vec<u8> {
    let mut s = bd_pes_packet(pid, 0, es);
    s.extend_from_slice(&bd_pes_packet(pid, 1, es));
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
    // One sector, exactly what the reader serves: an extent claiming more
    // sectors than the source yields is a SHORT read, correctly inconclusive.
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
    // not-forced; this pins the verdict itself (must not come back FORCED off
    // one non-forced set) without engaging the demotion guard.
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

// Serves a fixed BD-TS byte stream, then either fails or runs on with
// zeros: models the two truncated-run shapes (abandoned vs. budget stop).
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
    // The defect: a forced verdict rests on the ABSENCE of a non-forced set.
    // When the read dies mid-title that absence means nothing, yet the probe
    // used to apply it as authoritative, overwriting the vendor flag.
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
    // And it must not be memoised: caching an inconclusive result would spread
    // this one read fault across every playlist sharing these clips.
    assert!(
        cache.is_empty(),
        "an inconclusive probe must not be cached against these extents"
    );
}

#[test]
fn read_error_keeps_a_track_that_already_settled_not_forced() {
    // Per-track, not per-title: "not forced" is POSITIVE evidence (a non-forced
    // set was actually seen), so no amount of unread data retracts it — that
    // verdict survives truncation even though a forced verdict would not.
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
    // never fires for a genuinely forced track, so the budget is how a forced
    // verdict gets accepted from a bounded prefix; inconclusive would disable it.
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
    // (first read to end, second stopped at budget), so both are memoised —
    // excluding the budget stop would memoise nothing on forced-track discs.
    assert_eq!(cache.len(), 2, "a conclusive probe is memoised per extent");
    assert!(cache.contains_key(&(0, 4, pid)));
    assert!(cache.contains_key(&(100, u32::MAX, pid)));
}

#[test]
fn cancelled_probe_is_neither_asserted_nor_cached() {
    // A halt lands at an arbitrary chunk boundary, so an absence claim from it
    // is worth no more than one from a read fault. Here the cancel arrives
    // AFTER a forced set was observed, unlike the uncancelled budget case.
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
    // The cancel landed inside the SECOND extent, an arbitrary prefix that must
    // not be memoised, or the cancellation replays onto every playlist sharing
    // the clip. (The first extent read to its end, so it stays cached.)
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

// ── per-extent, per-track memoisation ─────────────────────────────────── MEASURED:
// overlapping-but-not-identical extent lists must not re-read shared clips.
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

// A later playlist declaring MORE PGS tracks over the SAME extents must still probe the
// extra track, not silently keep its vendor flag.
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

// Every probe read must begin on an AACS aligned-unit boundary measured from the extent's
// own base, or a decrypting source rejects it.
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

// A short-but-nonzero read must advance by what was READ, not requested, or a forced
// verdict gets asserted over sectors nobody read.
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

// A short read must not break the aligned-unit invariant `CHUNK_SECTORS` exists to hold:
// `lba` must advance only by whole units.
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

// A source that can never yield a whole aligned unit must not spin: the loop retries a
// bounded number of times, then stops inconclusively.
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

// ── mutation-triage additions ─────────────────────────────────────────── Mutation guard
// for `sub.codec == Codec::Pgs`: only PGS subtitle tracks are ever probed by content.
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

// Mutation guard for `stalled > STALL_RETRY_LIMIT`: exactly the limit's worth of retries
// are allowed before giving up.
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

// Padding TS packets (sync byte only, PID 0, discarded by the demuxer) to push the real
// display set past a mutated `got + SECTOR_BYTES` offset.
fn filler_packets(count: usize) -> Vec<u8> {
    let mut v = vec![0u8; count * 192];
    for i in 0..count {
        v[i * 192 + 4] = 0x47; // sync byte only; pid 0, adaptation 0 → discarded
    }
    v
}

// Mutation guard for `got as usize * SECTOR_BYTES` (feed-length on a fully-served chunk):
// must hand the WHOLE chunk to the demuxer.
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

// Like `PartialTsReader`'s `ThenWhat::Zeros`, but counts every sector
// requested, so a test can measure a SECOND, effectively infinite extent.
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

// Mutation guard for the `||` in the early-exit check: non-forced evidence CARRIED IN from
// a prior extent must stop reading a later one immediately.
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
    // Assert the PREDICATE directly rather than counting log events (capturing
    // a tracing log line is racy — see the sibling test in disc/encrypt.rs).
    // Exhausted/Budget stops are conclusive; Halted/ReadFailed are not.
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

// Synthetic fixtures reproduce the measured shape of real discs to test the probe's logic,
// not the shape itself.
#[derive(Clone, Copy)]
struct TrackShape {
    pid: u16,
    first_sector: u32,
    period_sectors: u32,
    count: u32,
    forced: bool,
}

// A feature-length clip: zeros except where a `TrackShape` puts a display
// set. Serves any LBA asked for (unlike `TsReader`), as a sampling probe needs.
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
                // PCS + END: a display set cut off before its END is not
                // emitted when a stream gap follows it.
                let mut set = pcs_display(t.forced);
                set[2] = (set.len() - 3) as u8;
                set.extend_from_slice(&[0x80, 0, 0]);
                let pkt = bd_pes_packet(t.pid, (i % 16) as u8, &set);
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

// Spec: a verdict that DEMOTES a track clears a `Forced` qualifier too — `forced` and
// `qualifier` are one fact for two consumers.
#[test]
fn a_demoted_track_stops_calling_itself_forced() {
    let mut title = pgs_title(0x1200, true);
    if let Stream::Subtitle(sub) = &mut title.streams[0] {
        sub.qualifier = LabelQualifier::Forced;
    }
    apply_verdicts(&mut title, &HashMap::from([(0x1200u16, false)]));
    let Stream::Subtitle(sub) = &title.streams[0] else {
        unreachable!()
    };
    assert!(!sub.forced);
    assert_eq!(
        sub.qualifier,
        LabelQualifier::None,
        "the content outranks the vendor's claim, and both renderings of it move together"
    );
}

// Spec: a qualifier that is not a forced claim (e.g. `Sdh`) is not the probe's to touch.
#[test]
fn a_demoted_track_keeps_a_qualifier_that_is_not_a_forced_claim() {
    let mut title = pgs_title(0x1200, true);
    if let Stream::Subtitle(sub) = &mut title.streams[0] {
        sub.qualifier = LabelQualifier::Sdh;
    }
    apply_verdicts(&mut title, &HashMap::from([(0x1200u16, false)]));
    let Stream::Subtitle(sub) = &title.streams[0] else {
        unreachable!()
    };
    assert_eq!(sub.qualifier, LabelQualifier::Sdh);
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

// THE headline fix: a feature's subtitles begin well past the old head-first budget's
// reach.
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

// The allocation, not just the total: the budget must be spread across the extent, not
// poured into its head.
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

// The memoisation hazard: a sampled read's evidence must not be replayed to a playlist that
// would have read far more of the extent.
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
    let covered_by =
        |plan: &[SampleWindow], s: u32| plan.iter().any(|w| s >= w.offset && s < w.offset + w.len);
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

// Demotion — the case the guard exists for: on a disc whose authoring never sets
// `forced_on_flag`, its absence says nothing about any track.
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

// The other side of the guard: a track SHAPED like a forced track keeps its label even on a
// disc that uses the flag.
#[test]
fn a_forced_shaped_track_keeps_its_label_on_a_disc_that_uses_the_flag() {
    let small = 0x1200u16;
    let full = 0x1201u16;
    let ext = feature_extent();
    // Put the small track's handful of display sets inside one sample window,
    // so it is genuinely OBSERVED (several sets, none forced) and the verdict
    // turns on its shape rather than on having seen nothing.
    let plan = plan_windows(ext.sector_count, PROBE_BUDGET_SECTORS);
    let window = plan[plan.len() / 2];
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

// A truncated run may still assert "not forced" (positive evidence: a non-forced set was
// seen) when the shape gate allows demotion, but never a forced verdict (an absence claim).
#[test]
fn a_truncated_run_asserts_only_a_demotable_not_forced_verdict() {
    let full = TrackEvidence {
        observed: true,
        non_forced: true,
        displays: 20,
        ..Default::default()
    };
    let flagged = TrackEvidence {
        observed: true,
        forced_seen: true,
        displays: 20,
        ..Default::default()
    };
    let ev = HashMap::from([(0x1200u16, full), (0x1201u16, flagged)]);
    assert_eq!(verdicts(&ev, false), HashMap::from([(0x1200u16, false)]));
}

// Coverage is what makes a memo replayable: an entry from a thin sample must not answer a
// question needing a thorough one.
#[test]
fn cached_evidence_answers_only_what_its_coverage_supports() {
    let absence = CachedEvidence {
        evidence: TrackEvidence {
            observed: true,
            non_forced: false,
            forced_seen: true,
            displays: 4,
            sampled: true,
        },
        covered: 1_000,
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
            sampled: true,
            ..Default::default()
        },
        covered: 1,
    };
    assert!(
        positive.answers(u32::MAX),
        "a non-forced display set was seen on the wire; no further reading \
             could retract it"
    );
    let whole = CachedEvidence {
        evidence: TrackEvidence {
            observed: true,
            sampled: false,
            ..Default::default()
        },
        covered: 10,
    };
    assert!(
        whole.answers(u32::MAX),
        "the extent was read end to end; there is nothing left to cover"
    );
}

// A playlist may list the SAME clip twice; the second read must merge into the extent's
// memo, not double `displays`.
#[test]
fn re_reading_one_extent_merges_its_memo_instead_of_doubling_it() {
    let pid = 0x1200u16;
    let ext = Extent {
        start_lba: 0,
        sector_count: 1,
    };
    // Two display sets: the second PUSI is what completes the first PES.
    let mut reader = SyntheticClipReader::new(vec![TrackShape {
        pid,
        first_sector: 0,
        period_sectors: 3,
        count: 2,
        forced: false,
    }]);
    let mut title = pgs_title(pid, false);
    title.extents = vec![ext, ext];
    let mut cache = ForcedProbeCache::new();
    probe_and_set_forced(&mut reader, &mut title, &mut cache, None);
    let entry = cache
        .get(&(ext.start_lba, ext.sector_count, pid))
        .copied()
        .expect("the extent is memoised");
    assert_eq!(
        entry.evidence.displays, 1,
        "one display set read twice is still one display set"
    );
    assert_eq!(
        entry.covered, ext.sector_count,
        "coverage is the extent, not twice the extent"
    );
}

// The tail of a sampled run must not be discarded: the demuxer holds the last PES open
// waiting for the next PUSI, which sampling may not supply.
#[test]
fn the_last_display_set_of_a_run_is_not_thrown_away() {
    let pid = 0x1200u16;
    let mut reader = SyntheticClipReader::new(vec![TrackShape {
        pid,
        first_sector: 0,
        period_sectors: 3,
        count: 1, // a lone PES: nothing follows to complete it
        forced: true,
    }]);
    let mut title = pgs_title(pid, false);
    title.extents = vec![Extent {
        start_lba: 0,
        sector_count: 6,
    }];
    probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
    assert!(
        forced_flag(&title, pid),
        "the run's final display set must still be observed"
    );
}

// A single-window extent's window must not sit at the extent's head — the opening of the
// feature reliably has no subtitles.
#[test]
fn a_single_window_sample_is_taken_from_the_middle_of_the_extent() {
    let sectors = 524_288u32;
    let share = 2_439u32; // the shape a 50-clip feature produces
    let plan = plan_windows(sectors, share);
    assert_eq!(plan.len(), 1, "one window's worth of share");
    let w = plan[0];
    assert!(
        w.offset > sectors / 4 && w.offset + w.len < sectors / 4 * 3,
        "the lone window must be taken from the middle, got {w:?} of {sectors}"
    );
}

// Promotion is an absence claim too, and a SAMPLE cannot support it off one display set.
#[test]
fn one_display_set_in_a_sampled_run_does_not_prove_a_track_forced() {
    let pid = 0x1200u16;
    let ext = feature_extent();
    let plan = plan_windows(ext.sector_count, PROBE_BUDGET_SECTORS);
    let window = plan[plan.len() / 2];
    let mut reader = SyntheticClipReader::new(vec![TrackShape {
        pid,
        first_sector: window.offset + 3,
        period_sectors: 3,
        count: 1, // the one flagged set of a track that is mostly unflagged
        forced: true,
    }]);
    let mut title = pgs_title(pid, false);
    title.extents = vec![ext];
    probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
    assert!(
        !forced_flag(&title, pid),
        "a single display set out of a sampled feature is not evidence that \
             EVERY display set on the track is forced"
    );
}

// A window that runs past the top of the 32-bit LBA space is skipped, never wrapped
// onto an unrelated low region.
#[test]
fn a_window_past_the_top_of_the_lba_space_is_not_wrapped() {
    struct LbaLog(Vec<u32>);
    impl SectorSource for LbaLog {
        fn read_sectors(
            &mut self,
            lba: u32,
            _count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.0.push(lba);
            buf.fill(0);
            Ok(buf.len())
        }
    }
    let start = u32::MAX - 10;
    let mut title = pgs_title(0x1200, false);
    title.extents = vec![Extent {
        start_lba: start,
        sector_count: 2_000_000,
    }];
    let mut reader = LbaLog(Vec::new());
    probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
    assert!(
        reader.0.iter().all(|&l| l >= start),
        "read below the extent: {:?}",
        reader
            .0
            .iter()
            .filter(|&&l| l < start)
            .take(3)
            .collect::<Vec<_>>()
    );
}

// ...but a COMPLETE read has no unread gap to hide a non-forced set in, so a genuine
// single-sign forced track is still promoted.
#[test]
fn a_complete_read_may_still_promote_from_one_display_set() {
    let pid = 0x1200u16;
    let mut reader = SyntheticClipReader::new(vec![TrackShape {
        pid,
        first_sector: 0,
        period_sectors: 3,
        count: 1,
        forced: true,
    }]);
    let mut title = pgs_title(pid, false);
    title.extents = vec![Extent {
        start_lba: 0,
        sector_count: 6,
    }];
    probe_and_set_forced(&mut reader, &mut title, &mut ForcedProbeCache::new(), None);
    assert!(forced_flag(&title, pid), "nothing was left unread");
}

// An early Exhausted decided on one extent's fresh tracks must not skip a later extent that
// still owes evidence for a track the cache answered on the first.
#[test]
fn exhausted_on_one_extent_does_not_skip_a_later_extent_owing_another_track() {
    let (x, y) = (0x1200u16, 0x1201u16);
    // A spans several read chunks so X settles mid-extent.
    let a = Extent {
        start_lba: 0,
        sector_count: 3000,
    };
    let b = Extent {
        start_lba: 4002,
        sector_count: 30,
    };
    let shape = |pid, first_sector, count, forced| TrackShape {
        pid,
        first_sector,
        period_sectors: 3,
        count,
        forced,
    };
    let mut reader = SyntheticClipReader::new(vec![shape(x, 0, 4, false), shape(y, 4002, 2, true)]);
    let mut title = pgs_title(x, false);
    title.streams.push(subtitle_stream(y, false));
    title.extents = vec![a, b];
    let mut cache = ForcedProbeCache::new();
    cache.insert(
        (a.start_lba, a.sector_count, y),
        CachedEvidence {
            evidence: TrackEvidence {
                observed: true,
                ..TrackEvidence::default()
            },
            covered: a.sector_count,
        },
    );
    probe_and_set_forced(&mut reader, &mut title, &mut cache, None);
    assert!(
        reader.reads.iter().any(|&(lba, _)| lba >= b.start_lba),
        "extent B still owes evidence for track Y and must be read"
    );
}
