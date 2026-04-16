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

use super::{pts_to_ns, CodecParser, Frame};
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

        let pts_ns = pes.dts.or(pes.pts).map(pts_to_ns).unwrap_or(0);
        let data = &pes.data;
        let mut keyframe = false;

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
                                let total_bytes = 4 + ((bits + 7) / 8) as usize;
                                (sc + total_bytes).min(data.len())
                            }
                        }
                    };

                    self.seq_header = Some(data[hdr_start..hdr_end].to_vec());
                    keyframe = true;
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
                    if sc + 5 < data.len() {
                        let picture_coding_type = (data[sc + 5] >> 3) & 0x07;
                        if picture_coding_type == PICTURE_TYPE_I {
                            keyframe = true;
                        }
                    }
                    pos = sc + 4;
                }
                _ => {
                    pos = sc + 4;
                }
            }
        }

        vec![Frame {
            pts_ns,
            keyframe,
            data: pes.data.clone(),
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

/// Find the position of the next start code (00 00 01) at or after `from`.
fn find_start_code(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < from + 3 {
        return None;
    }
    (from..data.len() - 2).find(|&i| data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01)
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

    // --- I-frame with sequence header = keyframe ---

    #[test]
    fn sequence_header_implies_keyframe() {
        let mut parser = Mpeg2Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&make_seq_header(720, 480, 3, 4));
        // Even without an explicit picture header, a sequence header implies I-frame.
        data.extend_from_slice(&[0xFF; 16]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe);
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
