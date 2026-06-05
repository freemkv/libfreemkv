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

pub struct PgsParser {
    pending: Option<(i64, Vec<u8>)>,
}

impl Default for PgsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PgsParser {
    pub fn new() -> Self {
        Self { pending: None }
    }
}

impl CodecParser for PgsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        let is_pcs = pes.data[0] == SEGMENT_PCS;
        let pcs_num_objects = if is_pcs && pes.data.len() > PCS_NUM_OBJECTS_OFFSET {
            Some(pes.data[PCS_NUM_OBJECTS_OFFSET])
        } else {
            None
        };

        let mut out = Vec::new();
        match pcs_num_objects {
            // Clear/empty PCS — closes any pending display. Drop the
            // clear segment itself; BlockDuration covers the screen
            // wipe.
            Some(0) => {
                if let Some((start_pts, data)) = self.pending.take() {
                    let duration = pts_ns.saturating_sub(start_pts).max(0) as u64;
                    out.push(Frame {
                        pts_ns: start_pts,
                        keyframe: true,
                        data,
                        duration_ns: Some(duration),
                    });
                }
            }
            // Display PCS — start a new pending. If a prior display
            // was never explicitly cleared (replace-without-clear),
            // emit it with the new PCS's PTS as its end.
            Some(_) => {
                if let Some((start_pts, data)) = self.pending.take() {
                    let duration = pts_ns.saturating_sub(start_pts).max(0) as u64;
                    out.push(Frame {
                        pts_ns: start_pts,
                        keyframe: true,
                        data,
                        duration_ns: Some(duration),
                    });
                }
                self.pending = Some((pts_ns, pes.data.clone()));
            }
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
                } else {
                    out.push(Frame {
                        pts_ns,
                        keyframe: true,
                        data: pes.data.clone(),
                        duration_ns: None,
                    });
                }
            }
        }

        out
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
            pid: 0x1200,
            pts,
            dts: None,
            data,
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
}
