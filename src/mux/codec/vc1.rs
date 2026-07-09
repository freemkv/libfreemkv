//! VC-1 (SMPTE 421M) elementary stream parser.
//!
//! VC-1 uses start codes similar to MPEG-2.
//! Sequence header (0x0F) contains codec initialization data.
//! Frame start = Frame header start code (0x0D).
//! I-frames (keyframes) are signalled by the presence of a Sequence Header
//! (0x0F) in the PES, per the BD VC-1 convention (see `parse`).

use super::coding::{CodingType, PictureInfo};
use super::startcode::BitReader;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SC_SEQUENCE_HEADER: u8 = 0x0F;
const SC_ENTRY_POINT: u8 = 0x0E;
const SC_FRAME: u8 = 0x0D;

/// Read the advanced-profile sequence header's `INTERLACE` flag (SMPTE 421M
/// §6.1.1): bit 41 after the start code — after PROFILE(2) LEVEL(3)
/// COLORDIFF_FORMAT(2) FRMRTQ(3) BITRTQ(5) POSTPROCFLAG(1) MAX_CODED_WIDTH(12)
/// MAX_CODED_HEIGHT(12) PULLDOWN(1). `None` for simple/main profile or a header
/// too short / over-escaped to reach the bit. De-escapes emulation-prevention
/// bytes first (as `parse_vc1_resolution` does) so the bit offset is exact.
fn parse_vc1_interlace(sh: &[u8]) -> Option<bool> {
    if sh.len() < 8 || (sh[4] >> 6) & 0x03 != 3 {
        return None; // need the start code + advanced profile (PROFILE == 3)
    }
    // Collect the first 6 de-escaped bytes (48 bits ≥ the 42 we need).
    let mut deesc = Vec::with_capacity(6);
    let mut zeros = 0u8;
    for &b in &sh[4..] {
        if zeros >= 2 && b == 0x03 {
            zeros = 0; // drop the emulation-prevention byte
            continue;
        }
        deesc.push(b);
        if deesc.len() == 6 {
            break;
        }
        zeros = if b == 0x00 { zeros + 1 } else { 0 };
    }
    if deesc.len() < 6 {
        return None;
    }
    let mut bits: u64 = 0;
    for &b in &deesc {
        bits = (bits << 8) | b as u64;
    }
    // 48 bits; INTERLACE is bit index 41 from the MSB → (48 - 1 - 41) = 6 from LSB.
    Some((bits >> 6) & 1 == 1)
}

/// Decode the advanced-profile **progressive** picture PTYPE VLC (SMPTE 421M
/// §7.1.1.4, Table): `0`=P, `10`=B, `110`=I, `1110`=BI (intra → I), `1111`=
/// Skipped (predicted, no residual → P). Only valid when the sequence is
/// progressive — for interlaced an FCM code (and, for field pictures, a combined
/// FPTYPE) precedes/replaces PTYPE, so the caller declines those.
fn vc1_progressive_ptype(br: &mut BitReader) -> Option<CodingType> {
    if br.read_bit()? == 0 {
        return Some(CodingType::P); // 0
    }
    if br.read_bit()? == 0 {
        return Some(CodingType::B); // 10
    }
    if br.read_bit()? == 0 {
        return Some(CodingType::I); // 110
    }
    // 1110 = BI (intra) → I; 1111 = Skipped (predicted) → P.
    Some(if br.read_bit()? == 0 {
        CodingType::I
    } else {
        CodingType::P
    })
}

/// Measure the coding type of an advanced-profile frame from its picture header.
/// `frame_rbsp` starts immediately after the frame start code (`00 00 01 0D`).
/// Decodes PTYPE only for a PROGRESSIVE sequence (where PTYPE is the first
/// picture-layer field); declines (`None`) for interlaced/simple-main/unknown
/// rather than guess at the wrong bit offset.
fn vc1_frame_coding_type(frame_rbsp: &[u8], seq_header: Option<&[u8]>) -> Option<CodingType> {
    if parse_vc1_interlace(seq_header?)? {
        return None; // interlaced: FCM/FPTYPE not decoded here
    }
    vc1_progressive_ptype(&mut BitReader::new(frame_rbsp))
}

pub struct Vc1Parser {
    // First-seen seq_header + entry_point seed the MKV codecPrivate
    // (BITMAPINFOHEADER extra data). These are the only out-of-band copies
    // the player gets. A stream may redefine either header mid-title; any
    // occurrence whose body DIFFERS from the active value must be emitted
    // IN-BAND at each point it appears, and at every keyframe (RAP) if the
    // active value differs from the codecPrivate copy, so seek points carry
    // valid decoder state (SMPTE 421M requires seq+entry before every RAP).
    seq_header: Option<Vec<u8>>,
    entry_point: Option<Vec<u8>>,
    // Currently-ACTIVE body of each type — the most recent the bitstream
    // defined. Distinct from the fixed codecPrivate copies above. The
    // strip/emit decision is made against `cur_*`, not the first-seen copy:
    // a switch BACK to the first-seen body (== codecPrivate) is still a
    // change a streaming decoder must be told about.
    cur_seq_header: Option<Vec<u8>>,
    cur_entry_point: Option<Vec<u8>>,
    width: u32,
    height: u32,
    /// Display-order PTS reconstruction, enabled only on the program-stream
    /// (HD-DVD EVO) path where the source stamps a PTS once per GOP. `None` on
    /// the BD/UHD transport path, which carries a per-frame PTS.
    reorder: Option<super::reorder::SparsePtsReorder>,
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
            cur_seq_header: None,
            cur_entry_point: None,
            width: 1920,
            height: 1080,
            reorder: None,
        }
    }

    /// Enable display-order PTS reconstruction for a program-stream source.
    /// No-op (leaves timestamps as parsed) for a transport-stream source.
    pub(crate) fn with_ps_reorder(mut self, enabled: bool) -> Self {
        if enabled {
            self.reorder = Some(super::reorder::SparsePtsReorder::new());
        }
        self
    }

    /// Route a finished frame through the PTS reorderer when enabled, else emit
    /// it directly (unchanged transport-stream behaviour).
    fn finish(&mut self, explicit: Option<i64>, frame: Frame) -> Vec<Frame> {
        match self.reorder.as_mut() {
            Some(r) => r.push(explicit, frame),
            None => vec![frame],
        }
    }
}

/// Handle a seq_header or entry_point start-code unit (Annex B raw bytes).
///
/// Decision is against the currently-ACTIVE body `cur`, not the codecPrivate
/// copy `first`:
/// - First of its type → seeds codecPrivate; stripped (decoder gets it from
///   the BITMAPINFOHEADER extra data at init).
/// - Equal to the active set `cur` → redundant; stripped.
/// - Different from `cur` (a change in EITHER direction, including reverting
///   to the codecPrivate/first value) → prepended into `prefix` in Annex B
///   form and `cur` updated.
///
/// Returns `true` when the unit was emitted into `prefix`.
fn handle_header(
    first: &mut Option<Vec<u8>>,
    cur: &mut Option<Vec<u8>>,
    unit: &[u8],
    prefix: &mut Vec<u8>,
) -> bool {
    let is_first = first.is_none();
    if is_first {
        first.replace(unit.to_vec()); // seeds codecPrivate; stripped here
    }
    let changed = cur.as_deref() != Some(unit);
    if changed {
        *cur = Some(unit.to_vec());
    }
    // Strip the seeding occurrence and any unit that doesn't change the
    // active header. Emit only a genuine change.
    if is_first || !changed {
        return false;
    }
    prefix.extend_from_slice(unit);
    true
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
        let explicit_pts = pes.pts.or(pes.dts).map(pts_to_ns);
        let ts_ns = explicit_pts.unwrap_or(0);
        let mut has_seq_header = false;
        let mut has_entry_point = false;
        let mut frame_start: Option<usize> = None;
        // Track whether this AU carried a redefined (in-band) copy of each
        // header type.  These are collected into separate temporaries so the
        // final keyframe prefix can be assembled in the canonical SMPTE 421M
        // order (seq_header then entry_point) regardless of bitstream scan
        // order.
        let mut redefined_seq: Option<Vec<u8>> = None;
        let mut redefined_ep: Option<Vec<u8>> = None;

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
                        // Try to parse resolution from advanced profile sequence header
                        if self.seq_header.is_none() {
                            if let Some((w, h)) = parse_vc1_resolution(sh) {
                                self.width = w;
                                self.height = h;
                            }
                        }
                        // Collect into a scratch Vec so handle_header can
                        // append; we discard the Vec and only keep the flag.
                        let mut scratch = Vec::new();
                        let changed = handle_header(
                            &mut self.seq_header,
                            &mut self.cur_seq_header,
                            sh,
                            &mut scratch,
                        );
                        if changed {
                            redefined_seq = Some(scratch);
                        }
                        has_seq_header = true;
                    }
                    SC_ENTRY_POINT => {
                        let end = find_next_sc(data, i + 4).unwrap_or(data.len());
                        let mut scratch = Vec::new();
                        let changed = handle_header(
                            &mut self.entry_point,
                            &mut self.cur_entry_point,
                            &data[i..end],
                            &mut scratch,
                        );
                        if changed {
                            redefined_ep = Some(scratch);
                        }
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

        // Build the in-band prefix in the canonical SMPTE 421M order:
        //   sequence_header (0x0F) THEN entry_point (0x0E).
        //
        // For each header type, use the in-band-redefined body when the AU
        // carried a change; otherwise re-assert the active body (unchanged
        // repeat) so every RAP is self-contained.  At non-keyframes only
        // genuine redefinitions are emitted.
        //
        // Assembling into separate seq/ep slots and concatenating in fixed
        // order avoids the ordering hazard that arose when the scan loop
        // appended headers in bitstream order and reassert() later appended
        // to whatever was already there: if seq was unchanged (stripped) but
        // entry_point was redefined (appended), the old code would produce
        // [entry_point] then reassert seq AFTER it → [entry_point,
        // seq_header], inverting the required order.
        let mut prefix: Vec<u8> = Vec::new();
        if keyframe {
            // seq_header slot: prefer the in-band-redefined body, else active.
            match redefined_seq {
                Some(body) => prefix.extend_from_slice(&body),
                None => {
                    if let Some(active) = self.cur_seq_header.as_deref() {
                        prefix.extend_from_slice(active);
                    }
                }
            }
            // entry_point slot: prefer the in-band-redefined body, else active.
            match redefined_ep {
                Some(body) => prefix.extend_from_slice(&body),
                None => {
                    if let Some(active) = self.cur_entry_point.as_deref() {
                        prefix.extend_from_slice(active);
                    }
                }
            }
        } else {
            // Non-keyframe: only genuine redefinitions go into the prefix.
            if let Some(body) = redefined_seq {
                prefix.extend_from_slice(&body);
            }
            if let Some(body) = redefined_ep {
                prefix.extend_from_slice(&body);
            }
        }

        // Assemble frame data: any in-band header changes + picture data from
        // the first SC_FRAME onwards.
        let frame_data = match frame_start {
            Some(start) => {
                if prefix.is_empty() {
                    data[start..].to_vec()
                } else {
                    let mut out = prefix;
                    out.extend_from_slice(&data[start..]);
                    out
                }
            }
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
                data.to_vec() // genuine picture payload with no leading 0x0D — pass through
            }
        };

        // Measure the coding type from the picture header (advanced-profile
        // progressive PTYPE; interlaced/simple-main declined → None). The frame
        // RBSP begins just past the 4-byte frame start code (00 00 01 0D).
        let coding_type = frame_start.and_then(|fs| {
            vc1_frame_coding_type(data.get(fs + 4..)?, self.cur_seq_header.as_deref())
        });

        let frame = Frame {
            // Coding-type only: VC-1 field order is not decoded here, so
            // field_order() stays None — honestly absent, never guessed.
            coding: coding_type.map(PictureInfo::coding_type_only),
            source: pes.source,
            pts_ns: ts_ns,
            keyframe,
            // One frame per PES (BD-TS aligns frames to PES), so the gap signal
            // maps straight onto this frame.
            discontinuity: pes.discontinuity,
            data: frame_data,
            duration_ns: None,
        };
        self.finish(explicit_pts, frame)
    }

    fn flush(&mut self) -> Vec<Frame> {
        match self.reorder.as_mut() {
            Some(r) => r.flush(),
            None => Vec::new(),
        }
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
            source: None,
            pid: 0x1011,
            pts,
            dts: None,
            data,
            discontinuity: false,
        }
    }

    #[test]
    fn vc1_populates_measured_coding_type_and_source() {
        use super::super::coding::CodingType;
        // Advanced-profile sequence header: 00 00 01 0F, PROFILE=3 (0xC0), then
        // zeros so INTERLACE (bit 41) = 0 → progressive.
        let seq_prog = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0];
        // Frame: 00 00 01 0D then the PTYPE VLC as the first RBSP bits:
        //   0xC0 = '110' → I; 0x00 = '0' → P; 0x80 = '10' → B.
        let frame = |ptype: u8| vec![0x00, 0x00, 0x01, SC_FRAME, ptype];
        let src = crate::pes::SourcePos::at_byte(2048);
        let mut p = Vc1Parser::new();

        // I-frame carrying the seq header → keyframe, sets the active seq header.
        let mut pe = make_pes([seq_prog.clone(), frame(0xC0)].concat(), Some(0));
        pe.source = Some(src);
        let fi = p.parse(&pe);
        assert_eq!(fi.len(), 1);
        assert!(fi[0].keyframe, "seq header present → keyframe");
        let ci = fi[0].coding.expect("VC-1 frame carries PictureInfo");
        assert_eq!(ci.coding_type(), CodingType::I, "PTYPE 110 → I");
        assert!(
            ci.field_order().is_none(),
            "VC-1 field order undecoded → None, never faked"
        );
        assert_eq!(
            fi[0].source.unwrap().byte,
            2048,
            "source provenance carried"
        );

        // P / B frames (no seq header; the active progressive seq header
        // persists) → measured P / B, not keyframes.
        let fp = p.parse(&make_pes(frame(0x00), Some(0)));
        assert!(!fp[0].keyframe);
        assert_eq!(
            fp[0].coding.unwrap().coding_type(),
            CodingType::P,
            "PTYPE 0 → P"
        );
        let fb = p.parse(&make_pes(frame(0x80), Some(0)));
        assert_eq!(
            fb[0].coding.unwrap().coding_type(),
            CodingType::B,
            "PTYPE 10 → B"
        );
    }

    #[test]
    fn vc1_interlaced_declines_coding_type_never_guesses() {
        // Interlaced sequence (INTERLACE bit 41 = 1): FCM/FPTYPE precede PTYPE
        // and are NOT decoded here, so the coding type is honestly omitted
        // rather than read at the wrong bit offset.
        let seq_int = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0x40];
        let frame = vec![0x00, 0x00, 0x01, SC_FRAME, 0xC0];
        let mut p = Vc1Parser::new();
        let f = p.parse(&make_pes([seq_int, frame].concat(), Some(0)));
        assert!(
            f[0].coding.is_none(),
            "interlaced VC-1 → coding omitted, never a guessed type"
        );
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
        // Seq+entry seed codecPrivate on first occurrence, but because this is a
        // keyframe (RAP) they are re-asserted in-band so the RAP is
        // self-contained. Frame data therefore STARTS with the seq_header start
        // code, and the SC_FRAME picture data follows.
        let fd = &frames[0].data;
        assert!(fd.len() >= 4);
        assert_eq!(&fd[0..4], &[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
        let frame_sc = fd
            .windows(4)
            .position(|w| w == [0x00, 0x00, 0x01, SC_FRAME]);
        assert!(
            frame_sc.is_some(),
            "SC_FRAME picture data must follow the re-asserted headers"
        );
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
            source: None,
            pid: 0x1011,
            pts: Some(180000), // presentation
            dts: Some(90000),  // decode
            data,
            discontinuity: false,
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

    // --- parse_vc1_resolution: profile gating + bounds + de-escaping ---

    #[test]
    fn resolution_none_for_non_advanced_profile() {
        // Simple (profile 0) and Main (profile 2) don't carry resolution in the
        // sequence header → parse returns None and the parser keeps the 1920x1080
        // default. PROFILE is byte4 bits 7-6.
        for profile in [0u8, 1, 2] {
            let mut sh = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER];
            sh.push(profile << 6); // byte4: profile in top 2 bits
            sh.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
            assert_eq!(
                parse_vc1_resolution(&sh),
                None,
                "profile {profile} (not advanced) has no header resolution"
            );
        }
    }

    #[test]
    fn resolution_too_short_returns_none() {
        // < 8 bytes can't carry the bit fields → None, no panic.
        let sh = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0x00];
        assert_eq!(parse_vc1_resolution(&sh), None);
    }

    #[test]
    fn resolution_round_trips_4k() {
        // Advanced profile 3840x2160: coded_w = 1920-1 = 1919, coded_h = 1080-1.
        let sh = make_ap_seq_header(3840, 2160);
        assert_eq!(parse_vc1_resolution(&sh), Some((3840, 2160)));
    }

    #[test]
    fn resolution_max_encodable_is_8192_within_bound() {
        // MAX_CODED_WIDTH/HEIGHT are 12-bit fields (max 4095). The decoded
        // dimension is (coded + 1) * 2, so the largest representable value is
        // (4095 + 1) * 2 = 8192 — exactly the `<= 8192` accept bound. A real
        // header therefore always satisfies the bound; the guard exists for
        // corrupt input but the field width makes 8192 the ceiling. Encoding
        // 8192x8192 (coded = 4095) must round-trip.
        let sh = make_ap_seq_header(8192, 8192);
        assert_eq!(parse_vc1_resolution(&sh), Some((8192, 8192)));
    }

    // --- codec_private BITMAPINFOHEADER field layout ---

    #[test]
    fn codec_private_bitmapinfoheader_fixed_fields() {
        // BITMAPINFOHEADER (40 bytes, little-endian). Verify the fixed fields:
        // biPlanes (u16 @ 12) = 1, biBitCount (u16 @ 14) = 24, biCompression
        // (@16) = "WVC1", and the five trailing u32 fields (@20..40) = 0.
        let mut parser = Vc1Parser::new();
        parser.parse(&make_pes(build_vc1_iframe_pes(), Some(0)));
        let cp = parser.codec_private().unwrap();
        assert_eq!(u16::from_le_bytes([cp[12], cp[13]]), 1, "biPlanes");
        assert_eq!(u16::from_le_bytes([cp[14], cp[15]]), 24, "biBitCount");
        assert_eq!(&cp[16..20], b"WVC1", "biCompression FOURCC");
        // biSizeImage, biXPelsPerMeter, biYPelsPerMeter, biClrUsed, biClrImportant.
        for (i, off) in (20..40).step_by(4).enumerate() {
            let v = u32::from_le_bytes([cp[off], cp[off + 1], cp[off + 2], cp[off + 3]]);
            assert_eq!(v, 0, "BITMAPINFOHEADER trailing field {i} must be 0");
        }
    }

    #[test]
    fn codec_private_extra_data_is_seq_header_then_entry_point() {
        // The extra codec data after the 40-byte header is sequence header bytes
        // immediately followed by entry-point bytes, in that order. Build a
        // header whose seq/entry payloads are distinguishable.
        let mut parser = Vc1Parser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
        data.extend_from_slice(&[0x11, 0x22, 0x33]);
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_ENTRY_POINT]);
        data.extend_from_slice(&[0x44, 0x55]);
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME, 0x66]);
        parser.parse(&make_pes(data, Some(0)));
        let cp = parser.codec_private().unwrap();
        let extra = &cp[40..];
        // seq header: 00 00 01 0F 11 22 33, then entry point: 00 00 01 0E 44 55.
        assert_eq!(
            extra,
            &[
                0x00,
                0x00,
                0x01,
                SC_SEQUENCE_HEADER,
                0x11,
                0x22,
                0x33,
                0x00,
                0x00,
                0x01,
                SC_ENTRY_POINT,
                0x44,
                0x55
            ],
            "extra = seq header then entry point, both Annex B"
        );
    }

    #[test]
    fn codec_private_none_missing_sequence_header() {
        // Entry point alone (no sequence header) → None.
        let mut parser = Vc1Parser::new();
        let mut data = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0xAA, 0xBB];
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME, 0xCC]);
        parser.parse(&make_pes(data, Some(0)));
        assert!(parser.codec_private().is_none());
    }

    // --- frame start code: only the FIRST 0x0D anchors frame data ---

    #[test]
    fn frame_data_anchors_at_first_frame_sc_includes_later_codes() {
        // frame_start is set once (the first 0x0D). Frame data runs from there to
        // the end, INCLUDING any later start codes (e.g. slice/field codes). It
        // must not be re-anchored by a second 0x0D.
        let mut parser = Vc1Parser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME, 0xAA]); // frame 1 SC
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x0B, 0xBB]); // slice code 0x0B
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        // Data begins at the first frame SC and includes everything after.
        assert_eq!(&f[0].data[0..4], &[0x00, 0x00, 0x01, SC_FRAME]);
        assert_eq!(f[0].data.len(), 10, "all bytes from first 0x0D to end kept");
    }

    #[test]
    fn no_start_code_passthrough_as_picture() {
        // A PES with no start code at all (no seq header / entry point either) is
        // a genuine picture payload continuation → passed through whole, not a
        // keyframe.
        let mut parser = Vc1Parser::new();
        let data = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        let f = parser.parse(&make_pes(data.clone(), Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, data, "passthrough whole");
        assert!(!f[0].keyframe);
    }

    #[test]
    fn entry_point_without_frame_or_seq_header_emits_no_frame() {
        // A PES with ONLY an entry point (no frame SC, no seq header) is a
        // parameter-set-only AU → no coded picture → no frame (has_entry_point
        // path of the None arm).
        let mut parser = Vc1Parser::new();
        let data = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0xAA, 0xBB];
        let f = parser.parse(&make_pes(data, Some(0)));
        assert!(f.is_empty(), "entry-point-only PES emits no frame");
        assert!(parser.entry_point.is_some(), "but entry point captured");
    }

    #[test]
    fn find_next_sc_respects_from_offset() {
        // find_next_sc must begin at `from`: a start code before `from` is
        // ignored. Code at offset 1 and 6; from=2 finds the second (offset 6).
        let data = [0xAA, 0x00, 0x00, 0x01, 0x0D, 0xBB, 0x00, 0x00, 0x01, 0x0E];
        assert_eq!(find_next_sc(&data, 0), Some(1));
        assert_eq!(find_next_sc(&data, 2), Some(6));
    }

    #[test]
    fn vc1_dts_fallback_and_zero_default() {
        // PTS absent → DTS used; both absent → 0.
        let mut parser = Vc1Parser::new();
        let pes = PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: Some(90000),
            data: vec![0x00, 0x00, 0x01, SC_FRAME, 0x55],
            discontinuity: false,
        };
        let f = parser.parse(&pes);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "DTS fallback");

        let mut parser2 = Vc1Parser::new();
        let pes2 = PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: None,
            data: vec![0x00, 0x00, 0x01, SC_FRAME, 0x55],
            discontinuity: false,
        };
        let f2 = parser2.parse(&pes2);
        assert_eq!(f2[0].pts_ns, 0, "no PTS/DTS → 0");
    }

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

    // --- regression: mid-stream entry_point A→B→A revert emitted in-band ---

    /// Regression: entry_point is redefined from A (== codecPrivate) to B, then
    /// switched BACK to A. A streaming decoder applied codecPrivate at init and
    /// is now on B; the revert to A must be emitted IN-BAND even though A ==
    /// codecPrivate, or the A-segment decodes against the wrong entry point.
    #[test]
    fn vc1_emits_entry_point_revert_to_first_value() {
        let sh = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xAA, 0xBB];
        let ep_a = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x11, 0x22];
        let ep_b = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x33, 0x44, 0x55];
        let frame = [0x00, 0x00, 0x01, SC_FRAME, 0x77];

        let mut parser = Vc1Parser::new();

        // AU1: seeds codecPrivate with sh + ep_a. Both are first → stripped from frame.
        let au1: Vec<u8> = sh
            .iter()
            .chain(ep_a.iter())
            .chain(frame.iter())
            .cloned()
            .collect();
        let f1 = parser.parse(&make_pes(au1, Some(0)));
        assert_eq!(f1.len(), 1, "AU1 emits a frame");
        // seq+entry seed codecPrivate, but this is a keyframe (RAP) so the active
        // headers are re-asserted in-band (self-contained RAP) — ep_a present.
        assert!(
            contains_sc(&f1[0].data, SC_ENTRY_POINT),
            "AU1: keyframe re-asserts the active entry_point in-band"
        );
        assert!(
            f1[0].data.windows(ep_a.len()).any(|w| w == ep_a),
            "AU1 carries the active ep_a bytes in-band"
        );

        // AU2: entry_point redefined to B → must be emitted in-band.
        let au2: Vec<u8> = ep_b.iter().chain(frame.iter()).cloned().collect();
        let f2 = parser.parse(&make_pes(au2, Some(90000)));
        assert_eq!(f2.len(), 1, "AU2 emits a frame");
        assert!(
            contains_sc(&f2[0].data, SC_ENTRY_POINT),
            "AU2: redefined entry_point B must be in-band"
        );
        assert!(
            f2[0].data.windows(ep_b.len()).any(|w| w == ep_b),
            "AU2 must carry the ep_b bytes"
        );

        // AU3: entry_point reverts to A (== codecPrivate). Active was B; this is
        // a real change and must still be emitted in-band.
        let au3: Vec<u8> = ep_a.iter().chain(frame.iter()).cloned().collect();
        let f3 = parser.parse(&make_pes(au3, Some(180000)));
        assert_eq!(f3.len(), 1, "AU3 emits a frame");
        assert!(
            f3[0].data.windows(ep_a.len()).any(|w| w == ep_a),
            "AU3: revert to A (== codecPrivate) must be emitted in-band"
        );
    }

    /// Regression: a bare keyframe (no seq_header / entry_point in PES) after
    /// a mid-title redefinition must re-assert the active headers in-band so
    /// seek points carry valid decoder state (SMPTE 421M).
    #[test]
    fn vc1_reasserts_active_headers_at_bare_keyframe() {
        let sh_a = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xAA, 0xBB];
        let ep_a = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x11, 0x22];
        let ep_b = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x33, 0x44, 0x55];
        let frame = [0x00, 0x00, 0x01, SC_FRAME, 0x77];

        let mut parser = Vc1Parser::new();

        // AU1: seed codecPrivate.
        let au1: Vec<u8> = sh_a
            .iter()
            .chain(ep_a.iter())
            .chain(frame.iter())
            .cloned()
            .collect();
        parser.parse(&make_pes(au1, Some(0)));

        // AU2: redefine entry_point to B at a keyframe.
        let au2: Vec<u8> = sh_a
            .iter()
            .chain(ep_b.iter())
            .chain(frame.iter())
            .cloned()
            .collect();
        parser.parse(&make_pes(au2, Some(90000)));

        // AU3: bare keyframe — only SC_SEQUENCE_HEADER (keyframe signal) + SC_FRAME,
        // no entry_point. Active entry_point is B (differs from codecPrivate A);
        // must be re-asserted in-band so seeks into this frame don't revert to A.
        let au3: Vec<u8> = sh_a.iter().chain(frame.iter()).cloned().collect();
        let f3 = parser.parse(&make_pes(au3, Some(180000)));
        assert_eq!(f3.len(), 1, "AU3 emits a frame");
        assert!(
            f3[0].data.windows(ep_b.len()).any(|w| w == ep_b),
            "bare keyframe must re-assert active entry_point B in-band"
        );
        assert!(
            !f3[0].data.windows(ep_a.len()).any(|w| w == ep_a),
            "must not re-assert stale codecPrivate entry_point A"
        );
    }

    /// Regression: keyframe where seq_header is UNCHANGED (stripped by scan) but
    /// entry_point is REDEFINED (changed). Before the fix, the old code appended
    /// entry_point during the scan, then reassert() appended seq_header AFTER it,
    /// producing [entry_point, seq_header] — entry_point before seq_header,
    /// violating SMPTE 421M. After the fix, assembly is always seq-then-entry.
    #[test]
    fn vc1_keyframe_prefix_order_seq_unchanged_entry_redefined() {
        let sh = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xAA, 0xBB, 0xCC];
        let ep_a = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x11, 0x22];
        let ep_b = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x33, 0x44, 0x55];
        let frame = [0x00, 0x00, 0x01, SC_FRAME, 0x77];

        let mut parser = Vc1Parser::new();

        // AU1: seed codecPrivate (sh + ep_a, both first → stripped, then
        // re-asserted as active at keyframe in seq-then-entry order).
        let au1: Vec<u8> = sh
            .iter()
            .chain(ep_a.iter())
            .chain(frame.iter())
            .cloned()
            .collect();
        parser.parse(&make_pes(au1, Some(0)));

        // AU2: keyframe — seq_header UNCHANGED (same bytes as AU1), entry_point
        // REDEFINED to B. This is the bug trigger: the scan emits ep_b into the
        // accumulator but strips sh; the keyframe reassert must then prepend sh
        // BEFORE ep_b, not after.
        let au2: Vec<u8> = sh
            .iter()
            .chain(ep_b.iter())
            .chain(frame.iter())
            .cloned()
            .collect();
        let f2 = parser.parse(&make_pes(au2, Some(90000)));
        assert_eq!(f2.len(), 1, "AU2 must emit a frame");

        // Find positions of seq_header and entry_point start codes in the output.
        let data = &f2[0].data;
        let seq_pos = data
            .windows(4)
            .position(|w| w == [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
        let ep_pos = data
            .windows(4)
            .position(|w| w == [0x00, 0x00, 0x01, SC_ENTRY_POINT]);
        assert!(
            seq_pos.is_some(),
            "seq_header must be present in the keyframe prefix"
        );
        assert!(
            ep_pos.is_some(),
            "entry_point must be present in the keyframe prefix"
        );
        assert!(
            seq_pos.unwrap() < ep_pos.unwrap(),
            "seq_header (pos {}) must precede entry_point (pos {}) — SMPTE 421M order",
            seq_pos.unwrap(),
            ep_pos.unwrap()
        );
        // The redefined entry_point body (ep_b) must appear, not the old ep_a.
        assert!(
            data.windows(ep_b.len()).any(|w| w == ep_b),
            "redefined ep_b must be present"
        );
        assert!(
            !data.windows(ep_a.len()).any(|w| w == ep_a),
            "stale ep_a must not be present"
        );
    }

    /// Helper: does `data` contain a start-code unit with the given type byte?
    fn contains_sc(data: &[u8], sc_type: u8) -> bool {
        data.windows(4)
            .any(|w| w[0] == 0x00 && w[1] == 0x00 && w[2] == 0x01 && w[3] == sc_type)
    }
}
