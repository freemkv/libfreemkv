//! VC-1 (SMPTE 421M) elementary stream parser.
//!
//! VC-1 uses start codes similar to MPEG-2.
//! Sequence header (0x0F) contains codec initialization data.
//! Frame start = Frame header start code (0x0D).
//! I-frames (keyframes) are signalled by the presence of a Sequence Header
//! (0x0F) in the PES, per the BD VC-1 convention (see `parse`).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SC_SEQUENCE_HEADER: u8 = 0x0F;
const SC_ENTRY_POINT: u8 = 0x0E;
const SC_FRAME: u8 = 0x0D;

pub struct Vc1Parser {
    seq_header: Option<Vec<u8>>,
    entry_point: Option<Vec<u8>>,
    width: u32,
    height: u32,
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
            width: 1920,
            height: 1080,
        }
    }
}

impl CodecParser for Vc1Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // MKV block timecodes are PRESENTATION timestamps; frames are stored in
        // decode order and the player reorders by timecode. Use PTS, not DTS —
        // DTS presents B-frames in decode order (visible judder) and breaks
        // PTS-based seeking. Fall back to DTS only if PTS is absent.
        let ts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);
        let mut has_seq_header = false;
        let mut has_entry_point = false;
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
                        let sh = &data[i..end];
                        self.seq_header = Some(sh.to_vec());
                        // Try to parse resolution from advanced profile sequence header
                        if let Some((w, h)) = parse_vc1_resolution(sh) {
                            self.width = w;
                            self.height = h;
                        }
                        has_seq_header = true;
                    }
                    SC_ENTRY_POINT => {
                        let end = find_next_sc(data, i + 4).unwrap_or(data.len());
                        self.entry_point = Some(data[i..end].to_vec());
                        has_entry_point = true;
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

        // Strip sequence header + entry point from frame data — those are in
        // codecPrivate, not coded-picture data. Only include data from the
        // frame start code onwards.
        let frame_data = match frame_start {
            Some(start) => &data[start..],
            None => {
                // No frame start code. If this PES carried only parameter sets
                // (sequence header / entry point, captured above into
                // codecPrivate), there is no coded picture to emit — drop it
                // rather than passing parameter bytes through as a bogus
                // keyframe. Mirrors how the H.264/HEVC parsers skip
                // parameter-set-only access units.
                if has_seq_header || has_entry_point {
                    return Vec::new();
                }
                data // genuine picture payload with no leading 0x0D — pass through
            }
        };

        vec![Frame {
            pts_ns: ts_ns,
            keyframe,
            data: frame_data.to_vec(),
            duration_ns: None,
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
        cp.extend_from_slice(&self.width.to_le_bytes()); // biWidth
        cp.extend_from_slice(&self.height.to_le_bytes()); // biHeight
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

/// Parse width and height from a VC-1 advanced profile sequence header.
/// The sequence header starts with 00 00 01 0F. After the start code:
///   byte 0 bits 7-6: profile (3 = advanced)
/// For advanced profile, the coded dimensions are encoded as 12-bit fields.
fn parse_vc1_resolution(sh: &[u8]) -> Option<(u32, u32)> {
    // sh starts at the start code (00 00 01 0F ...)
    if sh.len() < 8 {
        return None;
    }
    let byte4 = sh[4]; // first byte after start code
    let profile = (byte4 >> 6) & 0x03;
    if profile != 3 {
        // Simple/Main profile: resolution not in sequence header
        return None;
    }
    // Advanced profile sequence-header layout (SMPTE 421M, bit-level from sh[4]):
    // PROFILE(2) + LEVEL(3) + COLORDIFF_FORMAT(2) + FRMRTQ_POSTPROC(3) +
    // BITRTQ_POSTPROC(5) + POSTPROCFLAG(1) + MAX_CODED_WIDTH(12) +
    // MAX_CODED_HEIGHT(12) ...
    // Total bits before MAX_CODED_WIDTH: 2+3+2+3+5+1 = 16 bits.
    // We need 16+12+12 = 40 bits = 5 de-escaped bytes from sh[4..].
    if sh.len() < 9 {
        return None;
    }
    // VC-1 Annex-B EBDU payload may carry emulation-prevention bytes (an
    // inserted 0x03 after a 00 00 run). De-escape the payload before bit
    // extraction so an EP byte landing within the first few bytes can't shift
    // every subsequent bit and corrupt MAX_CODED_WIDTH/HEIGHT. Collect just the
    // 5 de-escaped bytes the bit fields need.
    let payload = &sh[4..];
    let mut deesc = Vec::with_capacity(5);
    let mut zeros = 0u8;
    for &b in payload {
        if zeros >= 2 && b == 0x03 {
            zeros = 0; // drop the emulation-prevention byte
            continue;
        }
        deesc.push(b);
        if deesc.len() == 5 {
            break;
        }
        zeros = if b == 0x00 { zeros + 1 } else { 0 };
    }
    if deesc.len() < 5 {
        return None;
    }
    // Build a u64 from the 5 de-escaped bytes for easy bit extraction.
    let mut bits: u64 = 0;
    for &b in &deesc {
        bits = (bits << 8) | b as u64;
    }
    // bits holds 40 significant bits laid out as:
    //   [16 leading bits][MAX_CODED_WIDTH:12][MAX_CODED_HEIGHT:12]
    // so MAX_CODED_WIDTH starts 12 bits from the LSB end and MAX_CODED_HEIGHT
    // occupies the low 12 bits (shift 0).
    const WIDTH_SHIFT: u64 = 12; // 40 - 16 - 12
    let coded_width = ((bits >> WIDTH_SHIFT) & 0xFFF) as u32 + 1;
    let coded_height = (bits & 0xFFF) as u32 + 1;
    // coded_width/height are `(bits & 0xFFF) + 1`, so always >= 1; after the
    // ×2 both are always >= 2. Only the upper bound can fail.
    let w = coded_width * 2;
    let h = coded_height * 2;
    if w <= 8192 && h <= 8192 {
        Some((w, h))
    } else {
        None
    }
}

fn find_next_sc(data: &[u8], from: usize) -> Option<usize> {
    (from..data.len().saturating_sub(2))
        .find(|&i| data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01)
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

    // --- parameter-set-only PES (seq header + entry point, no frame SC) ---

    #[test]
    fn param_set_only_pes_emits_no_frame() {
        let mut parser = Vc1Parser::new();

        // Sequence header + entry point, but NO frame start code (0x0D).
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_ENTRY_POINT]);
        data.extend_from_slice(&[0x11, 0x22, 0x33, 0x44]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        // No coded picture → no frame emitted (parameter bytes must not be
        // passed through as a bogus keyframe).
        assert!(
            frames.is_empty(),
            "parameter-set-only PES should not emit a frame"
        );
        // But codecPrivate is still captured.
        assert!(parser.seq_header.is_some());
        assert!(parser.entry_point.is_some());
        assert!(parser.codec_private().is_some());

        // A following frame-bearing PES still emits its picture.
        let mut data2 = Vec::new();
        data2.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data2.extend_from_slice(&[0x55, 0x66, 0x77]);
        let frames2 = parser.parse(&make_pes(data2, Some(180000)));
        assert_eq!(frames2.len(), 1);
        assert_eq!(&frames2[0].data[0..4], &[0x00, 0x00, 0x01, SC_FRAME]);
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

    // --- PTS (presentation) used for the MKV block timecode, not DTS ---

    #[test]
    fn pts_preferred_over_dts() {
        let mut parser = Vc1Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66]);

        let pes = PesPacket {
            pid: 0x1011,
            pts: Some(180000), // presentation
            dts: Some(90000),  // decode
            data,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        // PTS must be used — MKV block timecodes are presentation timestamps.
        assert_eq!(frames[0].pts_ns, 2_000_000_000);
    }

    // --- advanced-profile resolution parsing (bit-offset regression) ---

    /// Build an advanced-profile VC-1 sequence header encoding the given
    /// width/height. Layout from sh[4]: PROFILE(2)=3, LEVEL(3), COLORDIFF(2),
    /// FRMRTQ(3), BITRTQ(5), POSTPROCFLAG(1) = 16 bits, then
    /// MAX_CODED_WIDTH(12) = width/2 - 1, MAX_CODED_HEIGHT(12) = height/2 - 1.
    fn make_ap_seq_header(width: u32, height: u32) -> Vec<u8> {
        let coded_w = (width / 2) - 1;
        let coded_h = (height / 2) - 1;
        // Accumulate 40 bits MSB-first: 16 leading bits then 12+12.
        let mut acc: u64 = 0;
        let mut nbits = 0u32;
        let put = |val: u64, n: u32, acc: &mut u64, nbits: &mut u32| {
            *acc = (*acc << n) | (val & ((1u64 << n) - 1));
            *nbits += n;
        };
        // PROFILE = 3 (advanced), then 14 more leading bits (all zero here).
        put(0b11, 2, &mut acc, &mut nbits);
        put(0, 14, &mut acc, &mut nbits); // level+colordiff+frmrtq+bitrtq+postproc
        put(coded_w as u64, 12, &mut acc, &mut nbits);
        put(coded_h as u64, 12, &mut acc, &mut nbits);
        // 40 bits → 5 bytes, MSB-first.
        let mut payload = Vec::with_capacity(5);
        for i in (0..5).rev() {
            payload.push(((acc >> (i * 8)) & 0xFF) as u8);
        }
        let mut sh = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER];
        sh.extend_from_slice(&payload);
        sh
    }

    #[test]
    fn advanced_profile_resolution_uses_16bit_offset() {
        // Regression: the parser skipped 11 bits (omitting BITRTQ_POSTPROC's 5
        // bits) instead of 16, reading width/height 5 bits too early. Encode a
        // non-default 1280x720 and confirm it round-trips, proving the 16-bit
        // pre-width offset.
        let mut parser = Vc1Parser::new();
        let mut data = make_ap_seq_header(1280, 720);
        // A frame so the parser emits and stores the header.
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
        data.extend_from_slice(&[0x55, 0x66]);
        parser.parse(&make_pes(data, Some(0)));

        let cp = parser.codec_private();
        // codec_private needs an entry point too; resolution is in width/height
        // fields regardless. Read them off the parser via codec_private when
        // available, else assert the internal fields directly.
        assert_eq!(parser.width, 1280, "width parsed at the 16-bit offset");
        assert_eq!(parser.height, 720, "height parsed at the 16-bit offset");
        let _ = cp;
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
