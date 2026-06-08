//! MPEG-2 Video elementary stream parser.
//!
//! Extracts sequence headers for MKV codecPrivate.
//! Detects keyframes (I-frames from picture headers).
//! Each PES packet = one access unit = one frame.
//!
//! Start codes:
//! - Sequence header: 00 00 01 B3
//! - Sequence extension: 00 00 01 B5
//! - Picture header: 00 00 01 00

use super::startcode::find_start_code;
use super::{CodecParser, Frame, pts_to_ns};
use crate::mux::ts::PesPacket;

/// Sequence header start code suffix.
const SEQ_HEADER_CODE: u8 = 0xB3;

/// Sequence extension start code suffix.
const SEQ_EXT_CODE: u8 = 0xB5;

/// Picture start code suffix.
const PICTURE_CODE: u8 = 0x00;

/// Picture coding type: I-frame.
const PICTURE_TYPE_I: u8 = 1;

/// Frame rate table (index from sequence header frame_rate_code).
const FRAME_RATES: [(u32, u32); 9] = [
    (0, 1),        // 0: forbidden
    (24000, 1001), // 1: 23.976
    (24, 1),       // 2: 24
    (25, 1),       // 3: 25
    (30000, 1001), // 4: 29.97
    (30, 1),       // 5: 30
    (50, 1),       // 6: 50
    (60000, 1001), // 7: 59.94
    (60, 1),       // 8: 60
];

/// Aspect ratio table (index from sequence header aspect_ratio_information).
const ASPECT_RATIOS: [(u8, u8); 5] = [
    (0, 0),     // 0: forbidden
    (1, 1),     // 1: square pixels (1:1 SAR)
    (4, 3),     // 2: 4:3 display
    (16, 9),    // 3: 16:9 display
    (221, 100), // 4: 2.21:1 display
];

/// MPEG-2 Video elementary stream parser.
pub struct Mpeg2Parser {
    /// Raw bytes of the last seen sequence header (+ sequence extension if found).
    seq_header: Option<Vec<u8>>,
    /// Whether we've captured the sequence extension (B5) already.
    has_extension: bool,
}

impl Default for Mpeg2Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Mpeg2Parser {
    /// Create a new MPEG-2 parser with no captured sequence-header state.
    pub fn new() -> Self {
        Self {
            seq_header: None,
            has_extension: false,
        }
    }

    /// Extract resolution from a captured sequence header.
    /// Returns (width, height) or None if the header is too short.
    pub fn resolution(&self) -> Option<(u16, u16)> {
        let hdr = self.seq_header.as_ref()?;
        parse_resolution(hdr)
    }

    /// Extract frame rate from a captured sequence header.
    /// Returns (numerator, denominator) or None.
    pub fn frame_rate(&self) -> Option<(u32, u32)> {
        let hdr = self.seq_header.as_ref()?;
        parse_frame_rate(hdr)
    }

    /// Extract aspect ratio from a captured sequence header.
    /// Returns (width, height) for display aspect ratio, or None.
    pub fn aspect_ratio(&self) -> Option<(u8, u8)> {
        let hdr = self.seq_header.as_ref()?;
        parse_aspect_ratio(hdr)
    }
}

impl CodecParser for Mpeg2Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // MKV block timecodes are PRESENTATION timestamps; frames are stored in
        // decode order and the player reorders by timecode. Use PTS, not DTS —
        // DTS presents B-frames in decode order (visible judder) and breaks
        // PTS-based seeking. Fall back to DTS only if PTS is absent.
        let pts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);
        let data = &pes.data;
        // Keyframe-ness is a property of the coded PICTURE, not of a sequence
        // header. A PES may carry a sequence header followed by a P/B-frame
        // (open-GOP / re-encoded MPEG-2); the picture, not the seq header,
        // decides the cue point. Set this only from the PICTURE_CODE arm.
        let mut picture_is_keyframe = false;
        let mut has_picture = false;
        let mut saw_seq_header = false;

        // Scan for start codes in the elementary stream data.
        let mut pos = 0;
        while let Some(sc) = find_start_code(data, pos) {
            if sc + 3 >= data.len() {
                break;
            }
            let code = data[sc + 3];

            match code {
                SEQ_HEADER_CODE => {
                    // MPEG-2 sequence header: 00 00 01 B3 + variable data.
                    // Base header: 8 bytes after start code = 12 bytes total.
                    // Then possibly 64 intra quantizer values (bit-packed from bit 63).
                    // Then possibly 64 non-intra quantizer values.
                    // Then extensions (00 00 01 B5).
                    //
                    // Capture to the next start code within this PES data.
                    // If no next start code exists (extension in next PES), capture
                    // just the sequence header without extensions.
                    let hdr_start = sc;
                    let next_sc = find_start_code(data, sc + 4);
                    let hdr_end = match next_sc {
                        Some(next) if next + 3 < data.len() => {
                            let mut end = next;
                            // Include B5 extensions
                            while end + 3 < data.len() && data[end + 3] == SEQ_EXT_CODE {
                                end = find_start_code(data, end + 4).unwrap_or(data.len());
                            }
                            end
                        }
                        _ => {
                            // No next start code in this PES — calculate exact header size.
                            // Bit 62: load_intra_quantiser_matrix
                            // Bit 62+1+512: load_non_intra_quantiser_matrix (if intra present)
                            // Bit 62+1: load_non_intra_quantiser_matrix (if intra absent)
                            if sc + 12 > data.len() {
                                data.len()
                            } else {
                                let mut bits = 63u32; // bits consumed so far
                                let intra = (data[sc + 11] & 0x02) != 0;
                                if intra {
                                    bits += 64 * 8;
                                }
                                // Non-intra flag is at current bit position
                                let byte_pos = (bits / 8) as usize;
                                let bit_pos = 7 - (bits % 8) as u8;
                                if sc + 4 + byte_pos < data.len() {
                                    let non_intra = (data[sc + 4 + byte_pos] >> bit_pos) & 1 != 0;
                                    bits += 1;
                                    if non_intra {
                                        bits += 64 * 8;
                                    }
                                }
                                let total_bytes = 4 + bits.div_ceil(8) as usize;
                                (sc + total_bytes).min(data.len())
                            }
                        }
                    };

                    self.seq_header = Some(data[hdr_start..hdr_end].to_vec());
                    // A NEW sequence header replaces the stored one, so its B5
                    // sequence extension must be re-captured. Reset the flag the
                    // SEQ_EXT_CODE arm guards on; otherwise, once the first
                    // header's B3+B5 pair was seen, every later header (channel
                    // change, title boundary, parser reuse) would be stored
                    // without its extension bytes — corrupting codecPrivate
                    // (interlace, chroma format, progressive-sequence flags).
                    self.has_extension = false;
                    // NOTE: a sequence header does NOT make the access unit a
                    // keyframe — that is decided solely by the PICTURE_CODE arm
                    // (picture_is_keyframe). Setting it here would mis-cue a
                    // seq-header-followed-by-P/B-frame PES.
                    saw_seq_header = true;
                    pos = if next_sc.is_some() { hdr_end } else { sc + 4 };
                }
                SEQ_EXT_CODE if self.seq_header.is_some() && !self.has_extension => {
                    // Sequence extension appears after seq header (may be in next PES).
                    // Append it to the stored seq_header.
                    let ext_end = find_start_code(data, sc + 4).unwrap_or(data.len());
                    if let Some(ref mut hdr) = self.seq_header {
                        hdr.extend_from_slice(&data[sc..ext_end]);
                    }
                    self.has_extension = true;
                    pos = ext_end;
                }
                PICTURE_CODE => {
                    // Picture header: bytes after start code contain temporal_reference
                    // (10 bits) + picture_coding_type (3 bits).
                    has_picture = true;
                    if sc + 5 < data.len() {
                        let picture_coding_type = (data[sc + 5] >> 3) & 0x07;
                        if picture_coding_type == PICTURE_TYPE_I {
                            picture_is_keyframe = true;
                        }
                    }
                    pos = sc + 4;
                }
                _ => {
                    pos = sc + 4;
                }
            }
        }

        // A PES that carried a sequence header but no picture start code is a
        // parameter-set-only access unit: it has no coded picture to emit.
        // Emitting it as a standalone keyframe would put bare sequence-header
        // bytes into frame data with no picture. The sequence header is
        // captured into codec_private above and is re-emitted in-band on the
        // next real picture's PES, so dropping the empty access unit loses
        // nothing. Mirrors how the H.264/HEVC parsers skip parameter-set-only
        // access units.
        //
        // Conservative: only drop when this PES actually contained a sequence
        // header and no picture. A PES with neither (e.g. a slice
        // continuation) still passes through unchanged, preserving real
        // keyframe detection.
        // `saw_seq_header` is set by the scan loop's SEQ_HEADER_CODE arm above,
        // so this reuses that single pass instead of re-scanning the PES bytes.
        if !has_picture && saw_seq_header {
            return Vec::new();
        }

        vec![Frame {
            pts_ns,
            keyframe: picture_is_keyframe,
            data: pes.data.clone(),
            duration_ns: None,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        self.seq_header.clone()
    }
}

/// Parse horizontal and vertical resolution from sequence header bytes.
/// The sequence header must start with 00 00 01 B3.
fn parse_resolution(hdr: &[u8]) -> Option<(u16, u16)> {
    // Need at least start code (4) + 4 bytes of header data = 8 bytes.
    if hdr.len() < 8 {
        return None;
    }
    // Bytes 4-5: horizontal_size_value (12 bits) | vertical_size_value top 4 bits
    // Bytes 5-6: vertical_size_value bottom 8 bits (12 bits total)
    let h = ((hdr[4] as u16) << 4) | ((hdr[5] as u16) >> 4);
    let v = (((hdr[5] & 0x0F) as u16) << 8) | hdr[6] as u16;
    Some((h, v))
}

/// Parse frame rate code from sequence header.
fn parse_frame_rate(hdr: &[u8]) -> Option<(u32, u32)> {
    if hdr.len() < 8 {
        return None;
    }
    let frame_rate_code = (hdr[7] & 0x0F) as usize;
    if frame_rate_code == 0 || frame_rate_code >= FRAME_RATES.len() {
        return None;
    }
    Some(FRAME_RATES[frame_rate_code])
}

/// Parse aspect ratio information from sequence header.
fn parse_aspect_ratio(hdr: &[u8]) -> Option<(u8, u8)> {
    if hdr.len() < 8 {
        return None;
    }
    let ar_code = ((hdr[7] >> 4) & 0x0F) as usize;
    if ar_code == 0 || ar_code >= ASPECT_RATIOS.len() {
        return None;
    }
    Some(ASPECT_RATIOS[ar_code])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1011,
            pts,
            dts: None,
            data,
        }
    }

    /// Build a minimal MPEG-2 sequence header.
    /// 00 00 01 B3 [h_size:12][v_size:12] [aspect:4][frame_rate:4] ...
    fn make_seq_header(width: u16, height: u16, aspect: u8, frame_rate: u8) -> Vec<u8> {
        let mut hdr = vec![0x00, 0x00, 0x01, SEQ_HEADER_CODE];
        hdr.push((width >> 4) as u8);
        hdr.push(((width & 0x0F) as u8) << 4 | ((height >> 8) & 0x0F) as u8);
        hdr.push((height & 0xFF) as u8);
        hdr.push((aspect << 4) | (frame_rate & 0x0F));
        // Bit rate (18 bits) + marker + VBV buffer size (10 bits) etc — pad minimally.
        hdr.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x00]);
        hdr
    }

    /// Build a picture header with the given coding type.
    fn make_picture_header(coding_type: u8) -> Vec<u8> {
        // 00 00 01 00 [temporal_ref:10][picture_coding_type:3][...]
        // temporal_reference = 0 for simplicity
        // byte4 = temporal_ref[9:2] = 0x00
        // byte5 = temporal_ref[1:0] | picture_coding_type[2:0] << 3 | ...
        let byte5 = (coding_type & 0x07) << 3;
        vec![0x00, 0x00, 0x01, PICTURE_CODE, 0x00, byte5, 0x00, 0x00]
    }

    // --- Sequence header parsing ---

    #[test]
    fn parse_sequence_header_resolution() {
        let hdr = make_seq_header(720, 480, 2, 4);
        let res = parse_resolution(&hdr);
        assert_eq!(res, Some((720, 480)));
    }

    #[test]
    fn parse_sequence_header_1920x1080() {
        let hdr = make_seq_header(1920, 1080, 3, 4);
        let res = parse_resolution(&hdr);
        assert_eq!(res, Some((1920, 1080)));
    }

    #[test]
    fn parse_sequence_header_frame_rate() {
        let hdr = make_seq_header(720, 480, 2, 4); // frame_rate_code 4 = 29.97
        let fr = parse_frame_rate(&hdr);
        assert_eq!(fr, Some((30000, 1001)));
    }

    #[test]
    fn parse_sequence_header_aspect_ratio() {
        let hdr = make_seq_header(720, 480, 3, 4); // aspect code 3 = 16:9
        let ar = parse_aspect_ratio(&hdr);
        assert_eq!(ar, Some((16, 9)));
    }

    #[test]
    fn parse_sequence_header_too_short() {
        let hdr = vec![0x00, 0x00, 0x01, SEQ_HEADER_CODE];
        assert!(parse_resolution(&hdr).is_none());
        assert!(parse_frame_rate(&hdr).is_none());
        assert!(parse_aspect_ratio(&hdr).is_none());
    }

    // --- I-frame detection ---

    #[test]
    fn detect_i_frame() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        // Some payload data after the picture header.
        data.extend_from_slice(&[0xFF; 16]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe, "I-frame should be detected as keyframe");
    }

    #[test]
    fn detect_p_frame_not_keyframe() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_picture_header(2)); // P-frame
        data.extend_from_slice(&[0xFF; 16]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "P-frame should not be keyframe");
    }

    #[test]
    fn detect_b_frame_not_keyframe() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_picture_header(3)); // B-frame
        data.extend_from_slice(&[0xFF; 16]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "B-frame should not be keyframe");
    }

    // --- Sequence header → codec_private ---

    #[test]
    fn codec_private_from_sequence_header() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        let seq = make_seq_header(720, 480, 3, 4);
        data.extend_from_slice(&seq);
        // Follow with a picture header (I-frame).
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 8]);

        let pes = make_pes(data, Some(0));
        let _frames = parser.parse(&pes);

        let cp = parser.codec_private();
        assert!(
            cp.is_some(),
            "codec_private should be available after sequence header"
        );
        let cp = cp.unwrap();
        // Should start with the sequence header start code.
        assert_eq!(&cp[..4], &[0x00, 0x00, 0x01, SEQ_HEADER_CODE]);
    }

    #[test]
    fn codec_private_none_initially() {
        let parser = Mpeg2Parser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- Sequence header with extension ---

    #[test]
    fn codec_private_includes_extension() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        let seq = make_seq_header(1920, 1080, 3, 4);
        data.extend_from_slice(&seq);
        // Sequence extension: 00 00 01 B5 [ext data]
        data.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE]);
        data.extend_from_slice(&[0x14, 0x8A, 0x00, 0x01, 0x00, 0x00]); // ext payload
        // Picture header follows.
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 4]);

        let pes = make_pes(data, Some(0));
        let _frames = parser.parse(&pes);

        let cp = parser.codec_private().unwrap();
        // Should contain both sequence header and sequence extension start codes.
        let has_ext = cp.windows(4).any(|w| w == [0x00, 0x00, 0x01, SEQ_EXT_CODE]);
        assert!(has_ext, "codec_private should include sequence extension");
    }

    // --- sequence header + picture = keyframe ---

    #[test]
    fn sequence_header_with_picture_is_keyframe() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_seq_header(720, 480, 3, 4));
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 16]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe);
        // codecPrivate is still captured.
        assert!(parser.codec_private().is_some());
    }

    // --- seq-header keyframe flag must not leak into a P/B-frame ---

    #[test]
    fn seq_header_then_p_frame_is_not_keyframe() {
        // A PES carrying a sequence header followed by a P-frame (open-GOP /
        // re-encoded MPEG-2) must NOT be flagged a keyframe — the keyframe-ness
        // belongs to the coded picture, not the sequence header. A spurious
        // keyframe here produces a bad MKV cue point.
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_seq_header(720, 480, 3, 4));
        data.extend_from_slice(&make_picture_header(2)); // P-frame
        data.extend_from_slice(&[0xFF; 16]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(
            !frames[0].keyframe,
            "seq-header + P-frame must not be a keyframe"
        );
        // The sequence header is still captured for codecPrivate.
        assert!(parser.codec_private().is_some());
    }

    // --- parameter-set-only PES (seq header, no picture) emits no frame ---

    #[test]
    fn sequence_header_only_pes_emits_no_frame() {
        let mut parser = Mpeg2Parser::new();

        // A PES carrying only a sequence header (+ extension), no picture.
        let mut data = Vec::new();
        data.extend_from_slice(&make_seq_header(1920, 1080, 3, 4));
        data.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE]);
        data.extend_from_slice(&[0x14, 0x8A, 0x00, 0x01, 0x00, 0x00]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        // No coded picture → no frame emitted, but the sequence header is
        // still captured for codecPrivate.
        assert!(
            frames.is_empty(),
            "parameter-set-only PES should not emit a frame"
        );
        assert!(
            parser.codec_private().is_some(),
            "sequence header should still be captured into codec_private"
        );

        // A following picture-bearing PES emits the real keyframe.
        let mut data2 = Vec::new();
        data2.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data2.extend_from_slice(&[0xFF; 16]);
        let frames2 = parser.parse(&make_pes(data2, Some(3600)));
        assert_eq!(frames2.len(), 1);
        assert!(frames2[0].keyframe);
    }

    // --- a SECOND sequence header re-captures its extension ---

    #[test]
    fn new_sequence_header_recaptures_extension() {
        // Regression: has_extension was never reset when a new sequence header
        // replaced the stored one, so a second header (channel change / title
        // boundary) was stored WITHOUT its B5 sequence extension. To exercise
        // the SEQ_EXT_CODE arm (which the has_extension flag guards), each
        // header and its extension arrive in SEPARATE PES packets.
        let mut parser = Mpeg2Parser::new();

        // Header A (no trailing start code → captured alone), then its B5
        // extension in the next PES.
        let _ = parser.parse(&make_pes(make_seq_header(1920, 1080, 3, 4), Some(0)));
        let mut ext_a = vec![0x00, 0x00, 0x01, SEQ_EXT_CODE];
        ext_a.extend_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        let _ = parser.parse(&make_pes(ext_a, Some(0)));
        assert!(
            parser
                .codec_private()
                .unwrap()
                .windows(6)
                .any(|w| w == [0x11, 0x22, 0x33, 0x44, 0x55, 0x66]),
            "first header's extension captured (has_extension now true)"
        );

        // A NEW header B, then ITS extension in a separate PES. With the bug,
        // has_extension stayed true and this extension would be dropped.
        let _ = parser.parse(&make_pes(make_seq_header(720, 480, 2, 4), Some(3600)));
        let mut ext_b = vec![0x00, 0x00, 0x01, SEQ_EXT_CODE];
        ext_b.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
        let _ = parser.parse(&make_pes(ext_b, Some(3600)));

        let cp2 = parser.codec_private().unwrap();
        assert!(
            cp2.windows(6)
                .any(|w| w == [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]),
            "second header's extension must be re-captured, not dropped"
        );
        // It is header B (720x480), not stale header A.
        assert_eq!(parser.resolution(), Some((720, 480)));
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion_to_nanoseconds() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 4]);

        // 90000 ticks = 1 second = 1_000_000_000 ns
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // --- Empty PES ---

    #[test]
    fn empty_pes_no_frames() {
        let mut parser = Mpeg2Parser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- Resolution helper methods ---

    // --- parse_resolution: 12-bit field packing (ISO 13818-2 §6.2.2.1) ---

    #[test]
    fn resolution_packs_split_nibble_correctly() {
        // h_size is bytes4-5[7:4] (12 bits), v_size is byte5[3:0]+byte6 (12 bits).
        // Use a width/height whose nibbles differ so a swap would be caught:
        // 0xABC x 0xDEF. byte4=0xAB, byte5=0xCD, byte6=0xEF.
        let hdr = make_seq_header(0xABC, 0xDEF, 1, 1);
        assert_eq!(parse_resolution(&hdr), Some((0xABC, 0xDEF)));
    }

    #[test]
    fn resolution_max_12bit() {
        // Max 12-bit dimension = 4095 (0xFFF) each.
        let hdr = make_seq_header(4095, 4095, 1, 1);
        assert_eq!(parse_resolution(&hdr), Some((4095, 4095)));
    }

    #[test]
    fn resolution_too_short_none() {
        // < 8 bytes → None, no panic.
        assert_eq!(parse_resolution(&[0x00, 0x00, 0x01, 0xB3, 0x07]), None);
    }

    // --- parse_frame_rate: full table + reserved codes ---

    #[test]
    fn frame_rate_all_valid_codes() {
        // ISO 13818-2 Table 6-4 frame_rate_code 1..=8.
        let expect = [
            (24000u32, 1001u32),
            (24, 1),
            (25, 1),
            (30000, 1001),
            (30, 1),
            (50, 1),
            (60000, 1001),
            (60, 1),
        ];
        for (i, &want) in expect.iter().enumerate() {
            let code = (i + 1) as u8;
            let hdr = make_seq_header(720, 480, 1, code);
            assert_eq!(parse_frame_rate(&hdr), Some(want), "frame_rate_code {code}");
        }
    }

    #[test]
    fn frame_rate_code_zero_forbidden_none() {
        // Code 0 is forbidden → None.
        let hdr = make_seq_header(720, 480, 1, 0);
        assert_eq!(parse_frame_rate(&hdr), None);
    }

    #[test]
    fn frame_rate_code_out_of_range_none() {
        // Codes 9..=15 are reserved (table has 9 entries, index 9..). 0x0F → None.
        let hdr = make_seq_header(720, 480, 1, 0x0F);
        assert_eq!(parse_frame_rate(&hdr), None);
    }

    // --- parse_aspect_ratio: table + reserved codes ---

    #[test]
    fn aspect_ratio_all_valid_codes() {
        // ISO 13818-2 Table 6-3 aspect_ratio_information 1..=4.
        let expect = [(1u8, 1u8), (4, 3), (16, 9), (221, 100)];
        for (i, &want) in expect.iter().enumerate() {
            let code = (i + 1) as u8;
            let hdr = make_seq_header(720, 480, code, 4);
            assert_eq!(parse_aspect_ratio(&hdr), Some(want), "aspect code {code}");
        }
    }

    #[test]
    fn aspect_ratio_code_zero_none() {
        let hdr = make_seq_header(720, 480, 0, 4);
        assert_eq!(parse_aspect_ratio(&hdr), None);
    }

    #[test]
    fn aspect_ratio_code_out_of_range_none() {
        // Codes 5..=15 reserved. 0x0F → None.
        let hdr = make_seq_header(720, 480, 0x0F, 4);
        assert_eq!(parse_aspect_ratio(&hdr), None);
    }

    // --- picture_coding_type: byte position + bit field ---

    #[test]
    fn picture_coding_type_bits_5_3() {
        // picture_coding_type is byte5 bits 5-3 (>> 3 & 0x07). I=1 (keyframe),
        // P=2, B=3, all others (D=4, reserved) not keyframes.
        for (ct, is_kf) in [(1u8, true), (2, false), (3, false), (4, false)] {
            let mut parser = Mpeg2Parser::new();
            let mut data = make_picture_header(ct);
            data.extend_from_slice(&[0xFF; 8]);
            let f = parser.parse(&make_pes(data, Some(0)));
            assert_eq!(f.len(), 1);
            assert_eq!(
                f[0].keyframe, is_kf,
                "picture_coding_type {ct}: keyframe={is_kf}"
            );
        }
    }

    #[test]
    fn picture_header_too_short_not_keyframe() {
        // A picture start code with too few following bytes to read byte5 must
        // NOT panic and must NOT be flagged a keyframe (the `sc + 5 < len` guard
        // is false). 00 00 01 00 + only 1 byte.
        let mut parser = Mpeg2Parser::new();
        let data = vec![0x00, 0x00, 0x01, PICTURE_CODE, 0x00];
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1, "picture present but header truncated");
        assert!(!f[0].keyframe, "truncated picture header → not keyframe");
    }

    // --- seq-header exact-size calc when no following start code (quantizers) ---

    #[test]
    fn seq_header_without_following_sc_captures_base_when_no_quantizers() {
        // When a sequence header has no following start code in the PES, the
        // parser computes its exact byte length. With load_intra_quantiser_matrix
        // = 0 and load_non_intra = 0 (byte11 bit1 clear), the header is the base
        // size (no 64-byte quantizer blocks appended). make_seq_header sets
        // byte11 (index sc+11) — our 8-byte tail's last byte is 0x00 → both flags
        // clear. The captured codecPrivate must be the base header only.
        let mut parser = Mpeg2Parser::new();
        let seq = make_seq_header(1920, 1080, 3, 4);
        let base_len = seq.len();
        // Sequence header alone in the PES (no picture, no next SC). It is a
        // parameter-set-only AU → no frame, but codecPrivate is captured.
        let f = parser.parse(&make_pes(seq, Some(0)));
        assert!(f.is_empty(), "seq-header-only PES emits no frame");
        let cp = parser.codec_private().expect("seq header captured");
        // The capture must not run past the buffer; length <= what we provided.
        assert!(
            cp.len() <= base_len,
            "captured header bounded by provided bytes"
        );
        assert_eq!(&cp[..4], &[0x00, 0x00, 0x01, SEQ_HEADER_CODE]);
    }

    #[test]
    fn picture_without_start_code_passes_through_keyframe_false() {
        // A PES with neither a sequence header nor a picture start code (a slice
        // continuation) passes through unchanged and is not a keyframe (the
        // `!has_picture && saw_seq_header` drop only fires when a seq header was
        // seen).
        let mut parser = Mpeg2Parser::new();
        // 00 00 01 01 is a slice start code (0x01), not picture/seq/ext.
        let data = vec![0x00, 0x00, 0x01, 0x01, 0xAA, 0xBB, 0xCC];
        let f = parser.parse(&make_pes(data.clone(), Some(0)));
        assert_eq!(f.len(), 1, "slice continuation passes through");
        assert!(!f[0].keyframe);
        assert_eq!(f[0].data, data, "data passed through verbatim");
    }

    #[test]
    fn mpeg2_dts_fallback_and_zero() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(PICTURE_TYPE_I);
        data.extend_from_slice(&[0xFF; 4]);
        let pes = PesPacket {
            pid: 0x1011,
            pts: None,
            dts: Some(90000),
            data: data.clone(),
        };
        let f = parser.parse(&pes);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "DTS fallback");

        let mut parser2 = Mpeg2Parser::new();
        let pes2 = PesPacket {
            pid: 0x1011,
            pts: None,
            dts: None,
            data,
        };
        let f2 = parser2.parse(&pes2);
        assert_eq!(f2[0].pts_ns, 0, "no PTS/DTS → 0");
    }

    #[test]
    fn frame_data_is_whole_pes_not_just_picture() {
        // The emitted frame data is the ENTIRE PES payload (pes.data.clone()),
        // not just the picture NAL — MPEG-2 ES is muxed as-is. Confirm a seq
        // header + picture PES emits the whole buffer.
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(720, 480, 3, 4);
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0x12, 0x34]);
        let f = parser.parse(&make_pes(data.clone(), Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, data, "frame data = whole PES payload");
    }

    #[test]
    fn parser_resolution_method() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_seq_header(720, 576, 2, 3));
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 4]);

        let pes = make_pes(data, Some(0));
        let _ = parser.parse(&pes);

        assert_eq!(parser.resolution(), Some((720, 576)));
        assert_eq!(parser.frame_rate(), Some((25, 1))); // frame_rate_code 3 = 25fps
        assert_eq!(parser.aspect_ratio(), Some((4, 3))); // aspect code 2 = 4:3
    }
}
