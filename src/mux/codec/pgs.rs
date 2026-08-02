//! HDMV PGS (Presentation Graphics Stream) subtitle parser.
//!
//! PGS segments: PCS, WDS, PDS, ODS, END. Each PES packet starts with
//! one of those (segment_type byte at offset 0).
//!
//! Subtitle display lifecycle (BD spec):
//! - A "display" PCS (number_of_composition_objects > 0) starts a
//!   visible subtitle. Its WDS/PDS/ODS follow.
//! - A later "empty" PCS (number_of_composition_objects == 0) clears
//!   the screen.
//!
//! For Matroska output we collapse that pair into one block with
//! `BlockDuration` set to (clear_pts - display_pts). Without a
//! duration, hardware players linger on the last bitmap until the
//! next subtitle replaces it — which can be many seconds, and on a
//! disc where the final subtitle has no follower, until end of file.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SEGMENT_PCS: u8 = 0x16;
// Upper bound on a pending display set's accumulated bytes. Real PGS
// display sets are small (a 1080p RLE bitmap plus palette is well under
// 1 MB); a stream that keeps appending non-PCS segments without ever
// emitting a PCS is malformed. Cap accumulation to bound memory and
// drop further appends until the next PCS resyncs the parser. Mirrors
// the MAX_*_BYTES / MAX_*_BUF caps in the DTS and AC-3 parsers.
const MAX_PGS_PENDING_BYTES: usize = 4 * 1024 * 1024;
// Offset within the PES payload at which number_of_composition_objects
// lives in a PCS: 3-byte segment header + 10 bytes of PCS fields
// (video_w/h, frame_rate, comp_num, comp_state, palette_update,
// palette_id_ref) = 13.
const PCS_NUM_OBJECTS_OFFSET: usize = 13;
// Offset of the first composition_object's flags byte within a PCS PES payload:
// PCS header(13) + number_of_composition_objects(1) + object_id_ref(2) +
// window_id_ref(1) = 17. `forced_on_flag` is bit 0x40 of that byte (HDMV PCS).
const PCS_FIRST_OBJECT_FLAGS_OFFSET: usize = 17;
const PCS_FORCED_ON_FLAG: u8 = 0x40;

/// Whether an emitted PGS display-set frame is a FORCED subtitle — the
/// `forced_on_flag` (0x40) on its first composition object. The frame data an
/// emitted PGS block carries begins with the display PCS (segment type 0x16), so
/// the flag is read directly from it. Returns `None` when the block is not a
/// display PCS with a composition object (nothing to classify — a clear PCS, a
/// non-PCS segment, or a truncated header).
///
/// The mux uses this to detect a *forced-narrative track* (every displayed
/// subtitle forced) without relying on the disc's vendor label metadata, so
/// forced subs are flagged `FlagForced` even on discs that carry no such blob.
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
/// says a track is forced — i.e. to demote 1 → 0.
///
/// Promotion (0 → 1) needs no such gate: it rests on positive evidence (every
/// display set carried `forced_on_flag`). Demotion rests on an ABSENCE, and an
/// absence is only meaningful if the flag is in use at all. Measured: discs
/// exist on which NO track carries `forced_on_flag`; there, "this track has no
/// forced display sets" is a fact about the authoring house, not about the
/// track, and demoting on it would strip a correct forced label from every
/// track on the disc.
///
/// So the rule is:
///   * a track that itself mixes forced and non-forced sets is self-evidently
///     not a forced-only track — demote, no further evidence needed; otherwise
///   * some OTHER track must demonstrably use the flag (`disc_uses_forced_flag`),
///     proving the authoring house sets it, AND this track must have the SHAPE of
///     a full track ([`DEMOTE_MIN_DISPLAY_SETS`] and
///     [`DEMOTE_MIN_DISPLAY_SHARE_DIVISOR`]) rather than of a forced-narrative one.
///
/// `busiest_displays` is the largest `displays` over every subtitle track judged
/// together (the same title's tracks for the probe, the same file's tracks for
/// the muxer).
pub fn demotable(facts: ForcedFacts, disc_uses_forced_flag: bool, busiest_displays: u32) -> bool {
    if facts.displays == 0 {
        return false;
    }
    // Mixed: forced sets AND non-forced sets on the same track. The flag is in
    // use right here, so its absence on the other sets is real evidence.
    if facts.forced_displays > 0 && facts.forced_displays < facts.displays {
        return true;
    }
    if !disc_uses_forced_flag || facts.displays < DEMOTE_MIN_DISPLAY_SETS {
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
    pending: Option<(i64, Vec<u8>)>,
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

    /// Take the pending display set (if any) and emit it as a Frame whose
    /// duration runs from its start PTS to `end_pts_ns` (the PTS of the PCS that
    /// closes or replaces it), clamped to >= 0. Shared by the clear-PCS and
    /// replace-PCS arms so the Frame shape stays in one place.
    fn emit_pending(&mut self, end_pts_ns: i64) -> Option<Frame> {
        let (start_pts, data) = self.pending.take()?;
        let duration = end_pts_ns.saturating_sub(start_pts).max(0) as u64;
        Some(Frame {
            discontinuity: false,
            coding: None,
            source: None,
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
        // Keep PTS as Option: a PCS with no PTS has an UNKNOWN start/clear time.
        // Collapsing it to a 0 sentinel produces a frame with a wrong start time
        // and an absurd duration (the full elapsed time of the disc). PGS PCS
        // packets carry a PTS on well-formed BD streams, so a missing PTS is a
        // malformed-stream path that we skip cleanly rather than corrupt.
        let pts = pes.pts.map(pts_to_ns);

        let is_pcs = pes.data[0] == SEGMENT_PCS;

        // A PCS too short to carry number_of_composition_objects is malformed.
        // Don't let it fall through to the non-PCS arm (where it would pollute
        // the pending display set or pass through as a lone frame): close any
        // pending set undurated (mirroring the no-PTS display path) and drop
        // the truncated header so the parser resyncs on the next PCS.
        if is_pcs && pes.data.len() <= PCS_NUM_OBJECTS_OFFSET {
            return self
                .pending
                .take()
                .map(|(start_pts, data)| {
                    vec![Frame {
                        discontinuity: false,
                        coding: None,
                        source: None,
                        pts_ns: start_pts,
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
            // Clear/empty PCS — closes any pending display. Drop the
            // clear segment itself; BlockDuration covers the screen
            // wipe. A clear PCS with no PTS can't time the duration, so
            // emit the pending set with no duration (it lingers to EOF).
            Some(0) => {
                let frame = match pts {
                    Some(end) => self.emit_pending(end),
                    None => self.pending.take().map(|(start_pts, data)| Frame {
                        discontinuity: false,
                        coding: None,
                        source: None,
                        pts_ns: start_pts,
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
                    self.pending = Some((start, pes.data.clone()));
                }
                // A display PCS with no PTS has an unknown start time. Don't
                // store it with a 0 sentinel (wrong start, absurd duration).
                // Flush any prior pending undurated and skip storing this one.
                None => {
                    out.extend(self.pending.take().map(|(start_pts, data)| Frame {
                        discontinuity: false,
                        coding: None,
                        source: None,
                        pts_ns: start_pts,
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
                        source: None,
                        pts_ns: pts.unwrap_or(0),
                        keyframe: true,
                        data: pes.data.clone(),
                        duration_ns: None,
                    });
                }
                // No pending set AND no PTS: drop it. Emitting at pts_ns=0 would
                // place a stray bitmap at 00:00:00.000 with no timing reference;
                // the no-PTS PCS arms above avoid the 0 sentinel for the same
                // reason.
            }
        }

        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        // A display set is only emitted when the *next* PCS arrives
        // (either an empty clear PCS or a replacing display PCS). At
        // end-of-stream there is no follower, so without this the last
        // subtitle of every PGS track would be silently dropped. Emit
        // the pending set with no duration — the trailing block lingers
        // until end of file, which is exactly the desired behavior for
        // the final on-screen subtitle (see the module doc).
        match self.pending.take() {
            Some((start_pts, data)) => vec![Frame {
                discontinuity: false,
                coding: None,
                source: None,
                pts_ns: start_pts,
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

    /// `observed()` is the probe's "did I actually see any PGS content?" signal.
    /// When it is false the track's forced state is UNKNOWN, and the probe leaves
    /// whatever flag the disc's own metadata supplied alone; when it is true the
    /// probe overwrites that flag with its own verdict. A tracker that always
    /// claims to have observed something therefore lets an unread or undecrypted
    /// subtitle track — where `is_forced()` is vacuously false — overwrite a
    /// correct vendor "forced" flag with "not forced".
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
        // A display PCS with no PTS has an unknown start time. It must NOT be
        // stored with a 0 sentinel — otherwise a later clear PCS at real PTS T
        // would emit a frame with pts_ns=0 and duration_ns=T (hours of ns for a
        // mid-disc subtitle). The malformed display PCS is skipped instead.
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

    /// A track that itself mixes forced and non-forced display sets needs no
    /// corroboration: the flag is demonstrably in use ON THIS TRACK, so it is a
    /// full track carrying occasional forced signs — not a forced-only track.
    #[test]
    fn a_mixed_track_is_demotable_on_its_own_evidence() {
        assert!(demotable(facts(4, 1), false, 4));
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
}
