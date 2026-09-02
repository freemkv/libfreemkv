//! HDMV PGS (Presentation Graphics Stream) subtitle parser.
//!
//! PGS segments: PCS, WDS, PDS, ODS, END. Each PES packet starts with
//! one of those (segment_type byte at offset 0).
//!
//! Subtitle display lifecycle (BD spec): a "display" PCS
//! (number_of_composition_objects > 0) starts a visible subtitle, and a
//! later "empty" PCS (== 0) clears it. For Matroska output we collapse that
//! pair into one block with `BlockDuration` set to (clear_pts - display_pts).
// See docs/pgs.md — why a missing BlockDuration makes hardware players linger.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SEGMENT_PCS: u8 = 0x16;
// Upper bound on a pending display set's bytes (real sets are well under 1 MB).
// Caps a malformed stream that appends non-PCS segments forever without a PCS,
// dropping further appends until the next PCS resyncs — mirrors DTS/AC-3 caps.
const MAX_PGS_PENDING_BYTES: usize = 4 * 1024 * 1024;
// Offset of number_of_composition_objects in a PCS: 3-byte segment header +
// 10 bytes of PCS fields (video_w/h, frame_rate, comp_num, comp_state,
// palette_update, palette_id_ref) = 13.
const PCS_NUM_OBJECTS_OFFSET: usize = 13;
// Offset of the first composition_object's flags byte within a PCS PES payload:
// PCS header(13) + number_of_composition_objects(1) + object_id_ref(2) +
// window_id_ref(1) = 17. `forced_on_flag` is bit 0x40 of that byte (HDMV PCS).
const PCS_FIRST_OBJECT_FLAGS_OFFSET: usize = 17;
const PCS_FORCED_ON_FLAG: u8 = 0x40;

/// Whether an emitted PGS display-set frame is a FORCED subtitle — the
/// `forced_on_flag` (0x40) on its first composition object. The frame data
/// begins with the display PCS (segment type 0x16), so the flag is read
/// directly from it. Returns `None` when the block is not a display PCS with
/// a composition object (clear PCS, non-PCS segment, or truncated header).
///
/// See `docs/pgs.md` for why the mux uses this for forced-narrative-track
/// detection.
pub fn display_set_is_forced(frame_data: &[u8]) -> Option<bool> {
    if frame_data.first() != Some(&SEGMENT_PCS) {
        return None;
    }
    if *frame_data.get(PCS_NUM_OBJECTS_OFFSET)? == 0 {
        return None; // clear PCS — no composition to classify
    }
    let flags = *frame_data.get(PCS_FIRST_OBJECT_FLAGS_OFFSET)?;
    Some(flags & PCS_FORCED_ON_FLAG != 0)
}

/// Accumulates the "is this PGS subtitle track a forced-narrative track?" verdict
/// from its display sets. A track is forced iff it displayed at least one subtitle
/// and EVERY display set carried the forced_on_flag — a dedicated forced track,
/// as opposed to a full track that merely has occasional forced signs.
///
/// This is the SINGLE classification used by both the MKV muxer (accumulating a
/// track's frames during a rip) and the `info`-time forced probe (feeding the
/// demuxed display sets), so both reach the identical verdict.
#[derive(Debug, Clone)]
pub struct ForcedTracker {
    has_display: bool,
    all_forced: bool,
    displays: u32,
    forced_displays: u32,
}

impl Default for ForcedTracker {
    fn default() -> Self {
        Self {
            has_display: false,
            all_forced: true,
            displays: 0,
            forced_displays: 0,
        }
    }
}

/// The disc-shaped facts about ONE subtitle track that a demotion decision
/// rests on — how many display sets were seen, and how many of them carried the
/// HDMV `forced_on_flag`.
///
/// Split out from [`ForcedTracker`] so the two places that can contradict a
/// vendor label (the scan-time probe, which accumulates per-extent evidence,
/// and the muxer, which holds a live tracker per track) feed the SAME rule.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ForcedFacts {
    /// Display sets observed on this track.
    pub displays: u32,
    /// How many of them carried `forced_on_flag`.
    pub forced_displays: u32,
}

/// A track must have shown at least this many display sets before "none of them
/// was forced" is allowed to contradict a vendor forced label.
///
/// Absence is weak evidence on a handful of sets: a genuine forced-narrative
/// track is SMALL (measured shape: tens of display sets for a whole feature), so
/// a couple of unflagged sets is exactly what one looks like on a disc whose
/// authoring never sets the flag.
pub const DEMOTE_MIN_DISPLAY_SETS: u32 = 8;

/// ...and it must carry at least this fraction (1/N) of the display sets of the
/// BUSIEST subtitle track on the disc.
///
/// This is the shape test that separates the two populations. Measured: a
/// dedicated forced track carries a low-tens count of display sets for a whole
/// feature, a full dialogue track carries one to two thousand — two orders of
/// magnitude apart. A track sitting within a quarter of the busiest track's
/// count is a full track, whatever its label says; a track at one percent of it
/// is the forced-narrative track its label claims and must keep that label.
pub const DEMOTE_MIN_DISPLAY_SHARE_DIVISOR: u32 = 4;

/// Whether content evidence is strong enough to CONTRADICT a vendor label that
/// says a track is forced — i.e. to demote 1 → 0. Promotion needs no such
/// gate; demotion requires, in order: something observed at all; the flag IN
/// USE (`disc_uses_forced_flag`, or on this very track); and the track's
/// SHAPE ([`DEMOTE_MIN_DISPLAY_SETS`], [`DEMOTE_MIN_DISPLAY_SHARE_DIVISOR`])
/// matching a full dialogue track rather than a forced-narrative one.
/// `busiest_displays` is the largest `displays` over every subtitle track
/// judged together. See `docs/pgs.md` for the full rationale and shapes.
pub fn demotable(facts: ForcedFacts, disc_uses_forced_flag: bool, busiest_displays: u32) -> bool {
    if facts.displays == 0 {
        return false;
    }
    let flag_in_use = disc_uses_forced_flag || facts.forced_displays > 0;
    if !flag_in_use || facts.displays < DEMOTE_MIN_DISPLAY_SETS {
        return false;
    }
    // `displays >= busiest / DIVISOR`, multiplied out (u64: `displays` is a
    // disc-derived count, so the product must not be able to wrap).
    u64::from(facts.displays) * u64::from(DEMOTE_MIN_DISPLAY_SHARE_DIVISOR)
        >= u64::from(busiest_displays)
}

impl ForcedTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one emitted PGS block into the verdict. Non-display blocks (clear
    /// PCS, other segments) are ignored.
    pub fn observe(&mut self, frame_data: &[u8]) {
        if let Some(forced) = display_set_is_forced(frame_data) {
            self.has_display = true;
            self.all_forced &= forced;
            // Saturating: the counts drive a shape comparison between tracks, so
            // a pathological stream must pin them, never wrap (and never panic
            // on an overflow in a debug build).
            self.displays = self.displays.saturating_add(1);
            if forced {
                self.forced_displays = self.forced_displays.saturating_add(1);
            }
        }
    }

    /// The counts behind the verdict: how many display sets were seen and how
    /// many carried `forced_on_flag`. Feeds [`demotable`].
    pub fn facts(&self) -> ForcedFacts {
        ForcedFacts {
            displays: self.displays,
            forced_displays: self.forced_displays,
        }
    }

    /// Whether the track has already shown a NON-forced subtitle — i.e. its
    /// verdict is settled at "not forced" and further observation can be skipped
    /// (the early-exit the probe uses to avoid reading the whole clip).
    pub fn settled_not_forced(&self) -> bool {
        self.has_display && !self.all_forced
    }

    /// Whether ANY display set was observed. When false the track's forced state
    /// is unknown (no PGS content seen — e.g. an undecrypted/unread stream), so a
    /// probe should leave any existing (vendor-derived) flag untouched rather
    /// than assert "not forced".
    pub fn observed(&self) -> bool {
        self.has_display
    }

    /// Final verdict: forced iff it displayed subtitles and every one was forced.
    pub fn is_forced(&self) -> bool {
        self.has_display && self.all_forced
    }
}

/// Stateful parser that collapses PGS display/clear PCS pairs into
/// duration-bearing Matroska frames. Implements [`CodecParser`].
pub struct PgsParser {
    /// The display set being accumulated, with the facts of the PES that
    /// STARTED it. A set spans PES packets — it opens on a display PCS and
    /// closes on the next one — so its timestamp and its source offset are the
    /// opening packet's, never the closing packet's. Same rule the other
    /// buffering parsers get from `PesBuf::front`.
    pending: Option<(super::pesbuf::PesFacts, Vec<u8>)>,
}

impl Default for PgsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PgsParser {
    /// Create a fresh PGS parser with no pending display set.
    pub fn new() -> Self {
        Self { pending: None }
    }

    // Emit the pending display set as a Frame from its start PTS to
    // `end_pts_ns`, clamped to >= 0. Shared by the clear-PCS and replace-PCS
    // arms so the Frame shape stays in one place.
    fn emit_pending(&mut self, end_pts_ns: i64) -> Option<Frame> {
        let (facts, data) = self.pending.take()?;
        let start_pts = facts.presentation_ns().unwrap_or(0);
        let duration = end_pts_ns.saturating_sub(start_pts).max(0) as u64;
        Some(Frame {
            discontinuity: false,
            coding: None,
            source: facts.source,
            pts_ns: start_pts,
            keyframe: true,
            data,
            duration_ns: Some(duration),
        })
    }
}

impl CodecParser for PgsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        // Keep PTS as Option: collapsing a missing PTS to 0 gives a wrong start
        // and an absurd duration (full disc runtime). Well-formed BD PCS packets
        // always carry a PTS, so a missing one is a malformed-stream path we skip.
        let pts = pes.pts.map(pts_to_ns);

        let is_pcs = pes.data[0] == SEGMENT_PCS;

        // A PCS too short for number_of_composition_objects is malformed. Don't
        // let it fall to the non-PCS arm (would pollute the pending set): close
        // any pending set undurated and drop the header to resync on next PCS.
        if is_pcs && pes.data.len() <= PCS_NUM_OBJECTS_OFFSET {
            return self
                .pending
                .take()
                .map(|(facts, data)| {
                    vec![Frame {
                        discontinuity: false,
                        coding: None,
                        source: facts.source,
                        pts_ns: facts.presentation_ns().unwrap_or(0),
                        keyframe: true,
                        data,
                        duration_ns: None,
                    }]
                })
                .unwrap_or_default();
        }

        let pcs_num_objects = if is_pcs {
            Some(pes.data[PCS_NUM_OBJECTS_OFFSET])
        } else {
            None
        };

        let mut out = Vec::new();
        match pcs_num_objects {
            // Clear/empty PCS closes any pending display; drop the segment
            // itself (BlockDuration covers the wipe). No PTS means no
            // duration, so the pending set is emitted lingering to EOF.
            Some(0) => {
                let frame = match pts {
                    Some(end) => self.emit_pending(end),
                    None => self.pending.take().map(|(facts, data)| Frame {
                        discontinuity: false,
                        coding: None,
                        source: facts.source,
                        pts_ns: facts.presentation_ns().unwrap_or(0),
                        keyframe: true,
                        data,
                        duration_ns: None,
                    }),
                };
                out.extend(frame);
            }
            // Display PCS — start a new pending. If a prior display
            // was never explicitly cleared (replace-without-clear),
            // emit it with the new PCS's PTS as its end.
            Some(_) => match pts {
                Some(start) => {
                    out.extend(self.emit_pending(start));
                    // The set's facts are THIS packet's — the one that opened
                    // it. `start` is that packet's PTS by construction.
                    self.pending = Some((super::pesbuf::PesFacts::of(pes), pes.data.clone()));
                    debug_assert_eq!(
                        super::pesbuf::PesFacts::of(pes).presentation_ns(),
                        Some(start),
                        "the opening packet's PTS is the set's start"
                    );
                }
                // A display PCS with no PTS has an unknown start time. Don't
                // store it with a 0 sentinel (wrong start, absurd duration).
                // Flush any prior pending undurated and skip storing this one.
                None => {
                    out.extend(self.pending.take().map(|(facts, data)| Frame {
                        discontinuity: false,
                        coding: None,
                        source: facts.source,
                        pts_ns: facts.presentation_ns().unwrap_or(0),
                        keyframe: true,
                        data,
                        duration_ns: None,
                    }));
                }
            },
            // Non-PCS first segment — either a continuation of the
            // current display set, or non-standard layout. If we have
            // a pending display, append; otherwise emit as-is.
            None => {
                if let Some((_, ref mut buf)) = self.pending {
                    // Bound accumulation: a well-formed display set is small.
                    // Past the cap, drop further appends (malformed stream);
                    // the next PCS will take/replace `pending` and resync.
                    if buf.len() + pes.data.len() <= MAX_PGS_PENDING_BYTES {
                        buf.extend_from_slice(&pes.data);
                    }
                } else if pes.pts.is_some() {
                    // A lone non-PCS segment with a real PTS — pass it through.
                    // (A missing PTS falls through to the drop path below: a
                    // bitmap with no timing reference would land at 00:00:00.)
                    out.push(Frame {
                        discontinuity: false,
                        coding: None,
                        // Emitted straight from THIS packet, so its facts are
                        // this packet's -- the same rule as a pending set,
                        // which takes the facts of the packet that opened it.
                        source: super::pesbuf::PesFacts::of(pes).source,
                        pts_ns: pts.unwrap_or(0),
                        keyframe: true,
                        data: pes.data.clone(),
                        duration_ns: None,
                    });
                }
                // No pending set AND no PTS: drop it. Emitting at pts_ns=0 would
                // place a stray bitmap at 00:00:00.000 with no timing reference
                // — same reason the no-PTS PCS arms above avoid the 0 sentinel.
            }
        }

        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        // A display set is only emitted when the next PCS arrives; at EOS there
        // is no follower, so without this the last subtitle would be silently
        // dropped. Emit it with no duration — it lingers to EOF (see module doc).
        match self.pending.take() {
            Some((facts, data)) => vec![Frame {
                discontinuity: false,
                coding: None,
                source: facts.source,
                pts_ns: facts.presentation_ns().unwrap_or(0),
                keyframe: true,
                data,
                duration_ns: None,
            }],
            None => Vec::new(),
        }
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    /// A PCS display-set block with one composition object; `forced` sets the
    /// forced_on_flag (0x40) in its flags byte at offset 17.
    fn pcs_display(forced: bool) -> Vec<u8> {
        let mut d = vec![0u8; 18];
        d[0] = SEGMENT_PCS;
        d[PCS_NUM_OBJECTS_OFFSET] = 1;
        d[PCS_FIRST_OBJECT_FLAGS_OFFSET] = if forced { PCS_FORCED_ON_FLAG } else { 0 };
        d
    }

    #[test]
    fn display_set_forced_flag_detection() {
        assert_eq!(display_set_is_forced(&pcs_display(true)), Some(true));
        assert_eq!(display_set_is_forced(&pcs_display(false)), Some(false));
        // Other flag bits set but not forced_on_flag → still not forced.
        let mut cropped = pcs_display(false);
        cropped[PCS_FIRST_OBJECT_FLAGS_OFFSET] = 0x80; // object_cropped_flag only
        assert_eq!(display_set_is_forced(&cropped), Some(false));
    }

    #[test]
    fn display_set_forced_none_for_non_display() {
        // Clear PCS (0 objects) → None.
        let mut clear = pcs_display(false);
        clear[PCS_NUM_OBJECTS_OFFSET] = 0;
        assert_eq!(display_set_is_forced(&clear), None);
        // Non-PCS segment → None.
        let mut ods = pcs_display(true);
        ods[0] = 0x15; // ODS
        assert_eq!(display_set_is_forced(&ods), None);
        // Truncated (no flags byte) → None, no panic.
        assert_eq!(display_set_is_forced(&pcs_display(true)[..15]), None);
        assert_eq!(display_set_is_forced(&[]), None);
    }

    // `observed()` distinguishes "unknown" (leave the vendor flag alone) from a
    // settled verdict, so an unread/undecrypted track can't overwrite a correct
    // vendor "forced" flag with "not forced". See docs/pgs.md for detail.
    #[test]
    fn observed_stays_false_until_a_real_display_set_is_seen() {
        let mut t = ForcedTracker::new();
        assert!(!t.observed(), "a fresh tracker has seen nothing");
        assert!(!t.is_forced(), "and has no verdict to give");

        // Blocks that carry no display set must not count as observation: a clear
        // PCS (zero composition objects), a non-PCS segment, a truncated PCS, and
        // an empty frame.
        let mut clear = pcs_display(false);
        clear[PCS_NUM_OBJECTS_OFFSET] = 0;
        let mut ods = pcs_display(true);
        ods[0] = 0x15;
        for block in [clear, ods, pcs_display(true)[..15].to_vec(), Vec::new()] {
            t.observe(&block);
            assert!(
                !t.observed(),
                "a block with no display set leaves the verdict unknown"
            );
        }

        // The first real display set is what flips it.
        t.observe(&pcs_display(true));
        assert!(t.observed());
        assert!(t.is_forced(), "the only display set seen was forced");
    }

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1200,
            pts,
            dts: None,
            data,
            discontinuity: false,
        }
    }

    // Minimum-viable PCS bytes: type 0x16, segment_length (2 bytes),
    // then 11 bytes of PCS fields ending in number_of_composition_objects.
    fn pcs_bytes(num_objects: u8) -> Vec<u8> {
        let mut v = vec![SEGMENT_PCS, 0x00, 0x0B];
        v.extend_from_slice(&[0x07, 0x80, 0x04, 0x38]); // 1920x1080
        v.push(0x10); // frame_rate
        v.extend_from_slice(&[0x00, 0x01]); // composition_number
        v.push(0x80); // composition_state = EpochStart
        v.push(0x00); // palette_update + reserved
        v.push(0x00); // palette_id_ref
        v.push(num_objects);
        v
    }

    #[test]
    fn display_then_clear_yields_duration() {
        let mut parser = PgsParser::new();

        // Display PCS at PTS 90000 (= 1s)
        let display = pcs_bytes(1);
        let frames = parser.parse(&make_pes(display.clone(), Some(90000)));
        assert!(frames.is_empty(), "display PCS should be pending");

        // Empty PCS at PTS 270000 (= 3s)
        let clear = pcs_bytes(0);
        let frames = parser.parse(&make_pes(clear, Some(270000)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
        assert_eq!(frames[0].duration_ns, Some(2_000_000_000));
        assert_eq!(frames[0].data, display);
    }

    #[test]
    fn replace_without_clear_still_emits_prior_with_duration() {
        let mut parser = PgsParser::new();
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
        let frames = parser.parse(&make_pes(pcs_bytes(1), Some(180000)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
        assert_eq!(frames[0].duration_ns, Some(1_000_000_000));
    }

    #[test]
    fn non_pcs_segment_appends_to_pending() {
        let mut parser = PgsParser::new();
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
        // ODS-like segment (type 0x15)
        let frames = parser.parse(&make_pes(vec![0x15, 0x00, 0x02, 0xAA, 0xBB], Some(90000)));
        assert!(frames.is_empty());
        // Clear closes the set; data should include the appended bytes.
        let frames = parser.parse(&make_pes(pcs_bytes(0), Some(180000)));
        assert_eq!(frames.len(), 1);
        let data = &frames[0].data;
        assert!(data.windows(5).any(|w| w == [0x15, 0x00, 0x02, 0xAA, 0xBB]));
    }

    #[test]
    fn pending_buffer_is_capped() {
        let mut parser = PgsParser::new();
        // Open a display set.
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));

        // Flood with non-PCS segments far exceeding the cap.
        let chunk = vec![0x15u8; 256 * 1024]; // 256 KB ODS-like segment
        let floods = (MAX_PGS_PENDING_BYTES / chunk.len()) + 32;
        for _ in 0..floods {
            let frames = parser.parse(&make_pes(chunk.clone(), Some(90000)));
            assert!(frames.is_empty(), "non-PCS appends should not emit");
        }

        // The pending buffer must not have grown without bound.
        let pending_len = parser.pending.as_ref().map(|(_, b)| b.len()).unwrap_or(0);
        assert!(
            pending_len <= MAX_PGS_PENDING_BYTES,
            "pending buffer {pending_len} exceeded cap {MAX_PGS_PENDING_BYTES}"
        );

        // A following PCS still resyncs and emits the (capped) pending set.
        let frames = parser.parse(&make_pes(pcs_bytes(0), Some(180000)));
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn flush_emits_final_pending_subtitle() {
        let mut parser = PgsParser::new();

        // Display PCS at PTS 90000 — buffered as pending, no follower.
        let display = pcs_bytes(1);
        let frames = parser.parse(&make_pes(display.clone(), Some(90000)));
        assert!(frames.is_empty(), "display PCS should be pending");

        // EOF: without flush() this last subtitle would be dropped.
        let frames = parser.flush();
        assert_eq!(frames.len(), 1, "final pending subtitle must flush");
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
        assert_eq!(frames[0].data, display);
        // Trailing block lingers to EOF — no duration per module doc.
        assert_eq!(frames[0].duration_ns, None);
    }

    #[test]
    fn display_pcs_without_pts_is_not_stored_with_zero_start() {
        // A display PCS with no PTS has an unknown start time; must NOT store it
        // with a 0 sentinel, or a later clear PCS at real PTS T would emit
        // pts_ns=0, duration_ns=T. The malformed display PCS is skipped instead.
        let mut parser = PgsParser::new();
        let frames = parser.parse(&make_pes(pcs_bytes(1), None));
        assert!(frames.is_empty(), "no-PTS display PCS emits nothing");
        assert!(
            parser.pending.is_none(),
            "no-PTS display PCS must not be stored as pending"
        );

        // A subsequent well-formed display + clear pair must time correctly,
        // unpolluted by the skipped no-PTS PCS.
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
        let f = parser.parse(&make_pes(pcs_bytes(0), Some(270000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 1_000_000_000);
        assert_eq!(f[0].duration_ns, Some(2_000_000_000));
    }

    #[test]
    fn clear_pcs_without_pts_emits_pending_undurated() {
        // A clear PCS that lacks a PTS can't compute a duration; the pending
        // display is still emitted, but with no duration (lingers to EOF)
        // instead of a bogus absurd one.
        let mut parser = PgsParser::new();
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
        let f = parser.parse(&make_pes(pcs_bytes(0), None));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "pending keeps its real start");
        assert_eq!(f[0].duration_ns, None, "no duration without a clear PTS");
    }

    #[test]
    fn truncated_pcs_flushes_pending_and_resyncs() {
        // A PCS too short to carry number_of_composition_objects arriving with a
        // pending display must close that display (undurated) and drop the
        // truncated header, not append its bytes into the pending bitmap.
        let mut parser = PgsParser::new();
        let display = pcs_bytes(1);
        assert!(
            parser
                .parse(&make_pes(display.clone(), Some(90000)))
                .is_empty()
        );

        // A 13-byte (<= PCS_NUM_OBJECTS_OFFSET) PCS: truncated.
        let truncated = vec![SEGMENT_PCS; PCS_NUM_OBJECTS_OFFSET];
        let frames = parser.parse(&make_pes(truncated, Some(180000)));
        assert_eq!(frames.len(), 1, "pending display flushed on truncated PCS");
        assert_eq!(frames[0].data, display, "pending bitmap not polluted");
        assert_eq!(frames[0].duration_ns, None, "flushed undurated");
        assert!(parser.pending.is_none(), "parser resynced");
    }

    #[test]
    fn lone_non_pcs_without_pts_is_dropped() {
        // A non-PCS segment with no pending set and no PTS must be dropped, not
        // emitted at pts_ns = 0 (which would land a stray bitmap at time zero).
        let mut parser = PgsParser::new();
        let frames = parser.parse(&make_pes(vec![0x15, 0x00, 0x02, 0xAA], None));
        assert!(frames.is_empty(), "no pending + no PTS → dropped");
    }

    #[test]
    fn lone_non_pcs_with_pts_passes_through() {
        // A lone non-PCS segment WITH a PTS still passes through.
        let mut parser = PgsParser::new();
        let frames = parser.parse(&make_pes(vec![0x15, 0x00, 0x02, 0xAA], Some(90000)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn flush_with_nothing_pending_is_empty() {
        let mut parser = PgsParser::new();
        assert!(parser.flush().is_empty());
    }

    #[test]
    fn codec_private_none() {
        let parser = PgsParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = PgsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    // --- number_of_composition_objects lives at byte 13 ---

    #[test]
    fn num_objects_read_from_offset_13() {
        // PCS_NUM_OBJECTS_OFFSET = 3-byte seg header + 10 PCS field bytes = 13.
        // A byte at offset 13 of 0 = clear, > 0 = display. Build a PCS where
        // every byte before 13 is non-zero noise and byte 13 alone decides.
        let mut display = vec![SEGMENT_PCS];
        display.extend_from_slice(&[0xFF; 12]); // bytes 1..=12 noise
        display.push(1); // byte 13: num_objects = 1 → display
        let mut parser = PgsParser::new();
        assert!(
            parser.parse(&make_pes(display, Some(90000))).is_empty(),
            "byte 13 == 1 → display PCS (pending), no emit yet"
        );
        // Now a clear: byte 13 == 0.
        let mut clear = vec![SEGMENT_PCS];
        clear.extend_from_slice(&[0xFF; 12]);
        clear.push(0); // byte 13 = 0 → clear
        let f = parser.parse(&make_pes(clear, Some(270000)));
        assert_eq!(f.len(), 1, "byte 13 == 0 closes the pending display");
    }

    // --- duration computation and clamping ---

    #[test]
    fn duration_clamps_to_zero_when_clear_precedes_display() {
        // A clear PTS earlier than the display PTS (corrupt/out-of-order stream)
        // must clamp duration to 0 via saturating_sub, never wrap to a huge u64.
        let mut parser = PgsParser::new();
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(270000))); // display @ 3s
        let f = parser.parse(&make_pes(pcs_bytes(0), Some(90000))); // clear @ 1s
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 3_000_000_000, "keeps display start");
        assert_eq!(
            f[0].duration_ns,
            Some(0),
            "clear-before-display clamps to 0, no u64 wrap"
        );
    }

    #[test]
    fn duration_zero_when_equal_pts() {
        let mut parser = PgsParser::new();
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
        let f = parser.parse(&make_pes(pcs_bytes(0), Some(90000)));
        assert_eq!(f[0].duration_ns, Some(0));
    }

    // --- clear / replace edge cases ---

    #[test]
    fn clear_with_no_pending_emits_nothing() {
        // An empty PCS arriving with no pending display is a no-op.
        let mut parser = PgsParser::new();
        let f = parser.parse(&make_pes(pcs_bytes(0), Some(90000)));
        assert!(f.is_empty(), "clear with nothing pending → no frame");
        assert!(parser.pending.is_none());
    }

    #[test]
    fn three_displays_each_close_the_previous() {
        // Successive display PCS (no intervening clear) each emit the prior one
        // timed to the new display's PTS. display@1s, display@2s, display@3s →
        // emits [1s dur 1s], [2s dur 1s]; the last (3s) is held.
        let mut parser = PgsParser::new();
        let f0 = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
        assert!(f0.is_empty());
        let f1 = parser.parse(&make_pes(pcs_bytes(1), Some(180000)));
        assert_eq!(f1.len(), 1);
        assert_eq!(f1[0].pts_ns, 1_000_000_000);
        assert_eq!(f1[0].duration_ns, Some(1_000_000_000));
        let f2 = parser.parse(&make_pes(pcs_bytes(1), Some(270000)));
        assert_eq!(f2.len(), 1);
        assert_eq!(f2[0].pts_ns, 2_000_000_000);
        assert_eq!(f2[0].duration_ns, Some(1_000_000_000));
        // Third held; flush emits it undurated.
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].pts_ns, 3_000_000_000);
        assert_eq!(tail[0].duration_ns, None);
    }

    #[test]
    fn pcs_exactly_at_offset_boundary_is_truncated() {
        // A PCS of EXACTLY PCS_NUM_OBJECTS_OFFSET (13) bytes has no byte at index
        // 13 → treated as truncated (`<= PCS_NUM_OBJECTS_OFFSET`). With a pending
        // display it flushes that undurated and resyncs.
        let mut parser = PgsParser::new();
        let display = pcs_bytes(1);
        let _ = parser.parse(&make_pes(display.clone(), Some(90000)));
        let exactly_13 = vec![SEGMENT_PCS; PCS_NUM_OBJECTS_OFFSET]; // 13 bytes
        let f = parser.parse(&make_pes(exactly_13, Some(180000)));
        assert_eq!(f.len(), 1, "13-byte PCS is truncated → flush pending");
        assert_eq!(f[0].duration_ns, None);
        assert!(parser.pending.is_none());
    }

    #[test]
    fn pcs_one_byte_past_offset_reads_num_objects() {
        // A PCS of PCS_NUM_OBJECTS_OFFSET + 1 (14) bytes is the minimum that can
        // carry number_of_composition_objects (index 13 exists). It must be read
        // as a real PCS, not truncated.
        let mut parser = PgsParser::new();
        let mut display = vec![SEGMENT_PCS; PCS_NUM_OBJECTS_OFFSET];
        display.push(1); // index 13 = 1 → display, 14 bytes total
        assert!(
            parser.parse(&make_pes(display, Some(90000))).is_empty(),
            "14-byte display PCS is pending (not truncated)"
        );
        assert!(parser.pending.is_some(), "stored as pending display");
    }

    #[test]
    fn non_pcs_without_pending_with_pts_passes_through_keyframe() {
        // A lone non-PCS segment (first byte != 0x16) with a PTS and no pending
        // set passes through as a keyframe frame at its PTS.
        let mut parser = PgsParser::new();
        let f = parser.parse(&make_pes(vec![0x14, 0x00, 0x01, 0xAA], Some(90000)));
        assert_eq!(f.len(), 1);
        assert!(f[0].keyframe);
        assert_eq!(f[0].pts_ns, 1_000_000_000);
        assert_eq!(f[0].duration_ns, None);
    }

    #[test]
    fn display_pcs_data_preserved_verbatim() {
        // The emitted frame data is the display PCS bytes (plus any appended
        // non-PCS continuation), verbatim — the bitmap must not be altered.
        let mut parser = PgsParser::new();
        let display = pcs_bytes(2); // num_objects = 2
        let _ = parser.parse(&make_pes(display.clone(), Some(90000)));
        let f = parser.parse(&make_pes(pcs_bytes(0), Some(180000)));
        assert_eq!(f[0].data, display, "display PCS data emitted verbatim");
    }

    // ── the demotion guard ──────────────────────────────────────────────────

    fn facts(displays: u32, forced: u32) -> ForcedFacts {
        ForcedFacts {
            displays,
            forced_displays: forced,
        }
    }

    /// The case the guard exists for: a disc whose authoring never sets
    /// `forced_on_flag`. Nothing about the absence of a flag nobody uses can
    /// contradict a vendor label, however many display sets confirm the absence.
    #[test]
    fn nothing_is_demotable_on_a_disc_that_never_sets_the_flag() {
        for displays in [1u32, DEMOTE_MIN_DISPLAY_SETS, 2_000, u32::MAX] {
            assert!(
                !demotable(facts(displays, 0), false, displays),
                "{displays} unflagged display sets on a flagless disc prove nothing"
            );
        }
    }

    // A track that mixes forced and non-forced sets needs no sibling
    // corroboration: the flag is in use ON THIS TRACK. See docs/pgs.md.
    #[test]
    fn a_mixed_track_corroborates_the_flag_itself() {
        assert!(demotable(facts(108, 2), false, 137));
    }

    /// ...but the shape test still applies to it. A SMALL track with a couple of
    /// flagged sets is a forced track whose authoring flagged some of its signs —
    /// demoting that is the exact mistake the shape test exists to prevent.
    #[test]
    fn a_small_mixed_track_is_not_demotable_against_a_busy_disc() {
        assert!(!demotable(facts(30, 1), true, 2_000));
        assert!(
            !demotable(facts(4, 1), true, 4),
            "and too few sets to say anything either way"
        );
    }

    /// With the flag in use elsewhere on the disc, the shape decides. Measured:
    /// a forced-narrative track carries tens of display sets, a full dialogue
    /// track one to two thousand.
    #[test]
    fn shape_decides_once_the_disc_is_known_to_use_the_flag() {
        assert!(
            demotable(facts(2_000, 0), true, 2_000),
            "the busiest track on the disc, with no forced set on it, is a full track"
        );
        assert!(
            !demotable(facts(20, 0), true, 2_000),
            "a track at one percent of the busiest is the forced track its label claims"
        );
        assert!(
            !demotable(facts(DEMOTE_MIN_DISPLAY_SETS - 1, 0), true, 8),
            "too few display sets for their absence of flags to mean anything"
        );
        assert!(
            demotable(facts(DEMOTE_MIN_DISPLAY_SETS, 0), true, 8),
            "at the threshold, with the shape of the busiest track, it is demotable"
        );
    }

    /// Never on no evidence at all: a track nobody observed cannot contradict
    /// anything.
    #[test]
    fn an_unobserved_track_is_never_demotable() {
        assert!(!demotable(facts(0, 0), true, 2_000));
    }

    /// Saturating counters: a pathological stream must pin the counts, never wrap
    /// them (and never panic on overflow in a debug build).
    #[test]
    fn display_counts_saturate_instead_of_wrapping() {
        let mut t = ForcedTracker::new();
        t.displays = u32::MAX;
        t.forced_displays = u32::MAX;
        let mut pcs = vec![0u8; 18];
        pcs[0] = SEGMENT_PCS;
        pcs[PCS_NUM_OBJECTS_OFFSET] = 1;
        pcs[PCS_FIRST_OBJECT_FLAGS_OFFSET] = PCS_FORCED_ON_FLAG;
        t.observe(&pcs);
        assert_eq!(t.facts().displays, u32::MAX);
        assert_eq!(t.facts().forced_displays, u32::MAX);
    }

    // A lone non-PCS segment with a PTS is emitted straight through rather
    // than accumulated, and must still carry provenance. See docs/pgs.md.
    #[test]
    fn a_lone_segment_emitted_directly_still_carries_provenance() {
        let mut parser = PgsParser::new();
        // A non-PCS segment (type 0x15 = ODS) with a PTS and no pending set.
        let mut p = make_pes(vec![0x15, 0x00, 0x00, 0x00, 0x04, 1, 2, 3, 4], Some(90_000));
        p.source = Some(crate::pes::SourcePos::at_byte(7_777));
        let frames = parser.parse(&p);
        assert!(
            !frames.is_empty(),
            "a lone segment with a PTS is passed through"
        );
        assert_eq!(
            frames[0].source.map(|s| s.byte),
            Some(7_777),
            "emitted straight from this packet, so it carries this packet's offset"
        );
    }
}
