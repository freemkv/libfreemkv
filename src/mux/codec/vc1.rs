//! VC-1 (SMPTE 421M) elementary stream parser.
//!
//! VC-1 uses start codes similar to MPEG-2.
//! Sequence header (0x0F) contains codec initialization data.
//! Frame start = Frame header start code (0x0D).
//! I-frames (keyframes) are identified from the frame header.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

const SC_SEQUENCE_HEADER: u8 = 0x0F;
const SC_ENTRY_POINT: u8 = 0x0E;
const SC_FRAME: u8 = 0x0D;

pub struct Vc1Parser {
    seq_header: Option<Vec<u8>>,
    entry_point: Option<Vec<u8>>,
}

impl Default for Vc1Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Vc1Parser {
    pub fn new() -> Self {
        Self {
            seq_header: None,
            entry_point: None,
        }
    }
}

impl CodecParser for Vc1Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Use DTS when available (monotonic for B-frame content), fall back to PTS
        let ts_ns = pes.dts.or(pes.pts).map(pts_to_ns).unwrap_or(0);
        let mut has_seq_header = false;
        let mut frame_start: Option<usize> = None;

        // Scan for start codes (00 00 01 XX)
        let data = &pes.data;
        let mut i = 0;
        while i + 3 < data.len() {
            if data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01 {
                let sc_type = data[i + 3];
                match sc_type {
                    SC_SEQUENCE_HEADER => {
                        let end = find_next_sc(data, i + 4).unwrap_or(data.len());
                        self.seq_header = Some(data[i..end].to_vec());
                        has_seq_header = true;
                    }
                    SC_ENTRY_POINT => {
                        let end = find_next_sc(data, i + 4).unwrap_or(data.len());
                        self.entry_point = Some(data[i..end].to_vec());
                    }
                    SC_FRAME => {
                        // Frame data starts at this start code
                        if frame_start.is_none() {
                            frame_start = Some(i);
                        }
                    }
                    _ => {}
                }
                i += 4;
            } else {
                i += 1;
            }
        }

        // Keyframe = this PES contains a sequence header (I-frame indicator in BD)
        let keyframe = has_seq_header;

        // Strip sequence header + entry point from frame data — those are in codecPrivate.
        // Only include data from the frame start code onwards.
        let frame_data = match frame_start {
            Some(start) => &data[start..],
            None => data, // no frame start code found, pass through entire PES
        };

        vec![Frame {
            pts_ns: ts_ns,
            keyframe,
            data: frame_data.to_vec(),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        // MKV V_MS/VFW/FOURCC requires BITMAPINFOHEADER (40 bytes) + extra codec data.
        // The sequence header + entry point go as extra data after the header.
        let sh = self.seq_header.as_ref()?;
        let ep = self.entry_point.as_ref()?;

        let extra_len = sh.len() + ep.len();
        let header_size: u32 = 40 + extra_len as u32;

        let mut cp = Vec::with_capacity(header_size as usize);

        // BITMAPINFOHEADER (40 bytes, little-endian)
        cp.extend_from_slice(&header_size.to_le_bytes()); // biSize
        cp.extend_from_slice(&1920u32.to_le_bytes()); // biWidth (updated by player)
        cp.extend_from_slice(&1080u32.to_le_bytes()); // biHeight
        cp.extend_from_slice(&1u16.to_le_bytes()); // biPlanes
        cp.extend_from_slice(&24u16.to_le_bytes()); // biBitCount
        cp.extend_from_slice(b"WVC1"); // biCompression = "WVC1" FOURCC
        cp.extend_from_slice(&0u32.to_le_bytes()); // biSizeImage
        cp.extend_from_slice(&0u32.to_le_bytes()); // biXPelsPerMeter
        cp.extend_from_slice(&0u32.to_le_bytes()); // biYPelsPerMeter
        cp.extend_from_slice(&0u32.to_le_bytes()); // biClrUsed
        cp.extend_from_slice(&0u32.to_le_bytes()); // biClrImportant

        // Extra codec data: sequence header + entry point (Annex B)
        cp.extend_from_slice(sh);
        cp.extend_from_slice(ep);

        Some(cp)
    }
}

fn find_next_sc(data: &[u8], from: usize) -> Option<usize> {
    for i in from..data.len().saturating_sub(2) {
        if data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01 {
            return Some(i);
        }
    }
    None
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

    /// Build a VC-1 PES with sequence header + entry point + frame start code.
    fn build_vc1_iframe_pes() -> Vec<u8> {
        let mut data = Vec::new();
        // Sequence header: 00 00 01 0F + payload
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
        // Entry point: 00 00 01 0E + payload
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_ENTRY_POINT]);
        data.extend_from_slice(&[0x11, 0x22, 0x33, 0x44]);
        // Frame: 00 00 01 0D + payload
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66, 0x77, 0x88, 0x99]);
        data
    }

    // --- sequence header detection ---

    #[test]
    fn parse_sequence_header() {
        let mut parser = Vc1Parser::new();

        let data = build_vc1_iframe_pes();
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        // Sequence header present → keyframe
        assert!(
            frames[0].keyframe,
            "PES with sequence header should be keyframe"
        );
        // seq_header should be stored internally
        assert!(parser.seq_header.is_some());
    }

    #[test]
    fn parse_entry_point() {
        let mut parser = Vc1Parser::new();

        let data = build_vc1_iframe_pes();
        let pes = make_pes(data, Some(0));
        parser.parse(&pes);

        assert!(parser.entry_point.is_some());
    }

    // --- codec_private is BITMAPINFOHEADER (40+ bytes) ---

    #[test]
    fn codec_private_bitmapinfoheader() {
        let mut parser = Vc1Parser::new();

        let data = build_vc1_iframe_pes();
        let pes = make_pes(data, Some(0));
        parser.parse(&pes);

        let cp = parser.codec_private();
        assert!(
            cp.is_some(),
            "codec_private should be Some after seq header + entry point"
        );

        let cp = cp.unwrap();
        // BITMAPINFOHEADER is 40 bytes + extra data
        assert!(
            cp.len() >= 40,
            "codec_private should be at least 40 bytes (BITMAPINFOHEADER)"
        );

        // biSize (first 4 bytes, little-endian) should equal total length
        let bi_size = u32::from_le_bytes([cp[0], cp[1], cp[2], cp[3]]);
        assert_eq!(
            bi_size as usize,
            cp.len(),
            "biSize should match total codec_private length"
        );

        // biCompression = "WVC1" at offset 16
        assert_eq!(&cp[16..20], b"WVC1", "FOURCC should be WVC1");

        // biWidth at offset 4 (little-endian u32) = 1920
        let width = u32::from_le_bytes([cp[4], cp[5], cp[6], cp[7]]);
        assert_eq!(width, 1920);

        // biHeight at offset 8 (little-endian u32) = 1080
        let height = u32::from_le_bytes([cp[8], cp[9], cp[10], cp[11]]);
        assert_eq!(height, 1080);
    }

    #[test]
    fn codec_private_none_before_data() {
        let parser = Vc1Parser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn codec_private_none_missing_entry_point() {
        let mut parser = Vc1Parser::new();

        // Only sequence header, no entry point
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66]);

        let pes = make_pes(data, Some(0));
        parser.parse(&pes);

        assert!(
            parser.codec_private().is_none(),
            "should be None without entry point"
        );
    }

    // --- frame without sequence header → not keyframe ---

    #[test]
    fn parse_non_keyframe() {
        let mut parser = Vc1Parser::new();

        // PES with only a frame start code (no sequence header)
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66, 0x77]);

        let pes = make_pes(data, Some(180000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(
            !frames[0].keyframe,
            "frame without sequence header should not be keyframe"
        );
    }

    // --- frame data starts from frame start code ---

    #[test]
    fn frame_data_starts_at_frame_sc() {
        let mut parser = Vc1Parser::new();

        let data = build_vc1_iframe_pes();
        let pes = make_pes(data.clone(), Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        // Frame data should start with the frame start code (00 00 01 0D)
        assert!(frames[0].data.len() >= 4);
        assert_eq!(&frames[0].data[0..4], &[0x00, 0x00, 0x01, SC_FRAME]);
    }

    // --- empty PES ---

    #[test]
    fn parse_empty_pes() {
        let mut parser = Vc1Parser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion() {
        let mut parser = Vc1Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // --- DTS preferred over PTS ---

    #[test]
    fn dts_preferred_over_pts() {
        let mut parser = Vc1Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66]);

        let pes = PesPacket {
            pid: 0x1011,
            pts: Some(180000),
            dts: Some(90000),
            data,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // --- find_next_sc utility ---

    #[test]
    fn find_next_sc_basic() {
        let data = [0xAA, 0x00, 0x00, 0x01, 0x0D, 0xBB];
        assert_eq!(find_next_sc(&data, 0), Some(1));
    }

    #[test]
    fn find_next_sc_none() {
        let data = [0xAA, 0xBB, 0xCC];
        assert_eq!(find_next_sc(&data, 0), None);
    }

    // --- codec_private extra data contains seq header + entry point ---

    #[test]
    fn codec_private_contains_extra_data() {
        let mut parser = Vc1Parser::new();

        let data = build_vc1_iframe_pes();
        let pes = make_pes(data, Some(0));
        parser.parse(&pes);

        let cp = parser.codec_private().unwrap();
        // After the 40-byte BITMAPINFOHEADER, we should have seq_header + entry_point data
        let extra = &cp[40..];
        assert!(
            !extra.is_empty(),
            "extra data after BITMAPINFOHEADER should not be empty"
        );
        // Extra data should start with the sequence header start code
        assert_eq!(&extra[0..4], &[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    }
}
