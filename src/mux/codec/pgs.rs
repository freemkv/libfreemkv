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
}
