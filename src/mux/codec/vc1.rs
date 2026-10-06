//! VC-1 (SMPTE 421M) elementary stream parser.
//!
//! VC-1 uses start codes similar to MPEG-2.
//! Sequence header (0x0F) contains codec initialization data.
//! Frame start = Frame header start code (0x0D).
//! I-frames (keyframes) are signalled by the presence of a Sequence Header
//! (0x0F) in the PES, per the BD VC-1 convention (see `parse`).

use super::coding::{CodingType, PictureInfo};
use super::decodable::{Decodable, Need};
use super::startcode::{BitReader, find_start_code};
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SC_SEQUENCE_HEADER: u8 = 0x0F;
const SC_ENTRY_POINT: u8 = 0x0E;
const SC_FRAME: u8 = 0x0D;

// Read the advanced-profile sequence header's `INTERLACE` flag (SMPTE 421M §6.1.1), bit 41
// after the start code. `None` for simple/main profile.
fn parse_vc1_interlace(sh: &[u8]) -> Option<bool> {
    // 48 bits; INTERLACE is bit index 41 from the MSB, so 6 from the LSB.
    Some((vc1_adv_seq_bits(sh, 6)? >> 6) & 1 == 1)
}

// First `n` (<= 8) de-escaped bytes after the start code of an advanced-profile (PROFILE == 3)
// sequence header, packed big-endian. VC-1 Annex-B may carry emulation-prevention bytes.
fn vc1_adv_seq_bits(sh: &[u8], n: usize) -> Option<u64> {
    if sh.len() <= 4 || (sh[4] >> 6) & 0x03 != 3 {
        return None;
    }
    let mut bits: u64 = 0;
    let mut got = 0;
    let mut zeros = 0u8;
    for &b in &sh[4..] {
        if zeros >= 2 && b == 0x03 {
            zeros = 0; // drop the emulation-prevention byte
            continue;
        }
        bits = (bits << 8) | b as u64;
        got += 1;
        if got == n {
            return Some(bits);
        }
        zeros = if b == 0x00 { zeros + 1 } else { 0 };
    }
    None
}

// Decode the advanced-profile **progressive** picture PTYPE VLC (SMPTE 421M §7.1.1.4). Only
// valid when the sequence is progressive — interlaced uses an FCM/FPTYPE code instead.
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

// Measure the coding type of an advanced-profile frame from its picture header. Decodes PTYPE
// only for a PROGRESSIVE sequence; declines (`None`) for interlaced/simple-main/unknown.
fn vc1_frame_coding_type(frame_rbsp: &[u8], seq_header: Option<&[u8]>) -> Option<CodingType> {
    if parse_vc1_interlace(seq_header?)? {
        return None; // interlaced: FCM/FPTYPE not decoded here
    }
    vc1_progressive_ptype(&mut BitReader::new(frame_rbsp))
}

/// What an advanced-profile picture predicts from, for decodability: the PTYPE VLC, or for an
/// interlaced sequence FCM then PTYPE or a field pair's FPTYPE (SMPTE 421M §7.1.1.4; FCM and
/// FPTYPE as ffmpeg `vc1.c` reads them).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Vc1Pic {
    /// I, or an I/I or I/P field pair.
    Intra,
    /// P, skipped, or a P/I or P/P field pair.
    Predicted,
    /// B, or a field pair holding a B field.
    Bi,
    /// BI (intra, never a reference), or a BI/BI field pair.
    IntraB,
}

fn vc1_picture(frame_rbsp: &[u8], seq_header: Option<&[u8]>) -> Option<Vc1Pic> {
    let interlace = parse_vc1_interlace(seq_header?)?;
    let mut br = BitReader::new(frame_rbsp);
    // FCM: 0 progressive, 10 frame-interlace, 11 field-interlace.
    if interlace && br.read_bit()? == 1 && br.read_bit()? == 1 {
        // FPTYPE: bit 2 = B/BI pair, bit 1 = first field P (or BI), bit 0 = second field.
        return Some(match br.read_bits(3)? {
            0 | 1 => Vc1Pic::Intra,
            2 | 3 => Vc1Pic::Predicted,
            7 => Vc1Pic::IntraB,
            _ => Vc1Pic::Bi,
        });
    }
    // PTYPE: 0 P, 10 B, 110 I, 1110 BI, 1111 skipped (P).
    let mut ones = 0;
    while ones < 4 && br.read_bit()? == 1 {
        ones += 1;
    }
    Some(match ones {
        1 => Vc1Pic::Bi,
        2 => Vc1Pic::Intra,
        3 => Vc1Pic::IntraB,
        _ => Vc1Pic::Predicted,
    })
}

pub struct Vc1Parser {
    // First-seen seq_header + entry_point seed MKV codecPrivate. A redefined
    // body must be emitted IN-BAND at each occurrence, and at every keyframe
    // when it differs from codecPrivate (SMPTE 421M requirement).
    seq_header: Option<Vec<u8>>,
    entry_point: Option<Vec<u8>>,
    // Currently-ACTIVE body of each type, distinct from the fixed codecPrivate
    // copies above. Strip/emit is decided against `cur_*`, not the first-seen
    // copy: switching BACK to the first-seen body is still a change to signal.
    cur_seq_header: Option<Vec<u8>>,
    cur_entry_point: Option<Vec<u8>>,
    width: u32,
    height: u32,
    /// Display-order PTS reconstruction, enabled only on the program-stream
    /// (HD-DVD EVO) path where the source stamps a PTS once per GOP. `None` on
    /// the BD/UHD transport path, which carries a per-frame PTS.
    reorder: Option<super::reorder::SparsePtsReorder>,
    /// Which pictures decode from the pictures this output holds.
    decodable: Decodable,
    /// What each frame held by `reorder` needs, in decode order.
    needs: std::collections::VecDeque<Need>,
    /// Whether an anchor followed the last entry-point picture: a B before one is leading.
    anchor_since_entry: bool,
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
            decodable: Decodable::new(true),
            needs: std::collections::VecDeque::new(),
            anchor_since_entry: false,
        }
    }

    /// Enable display-order PTS reconstruction for a program-stream source.
    /// No-op (leaves timestamps as parsed) for a transport-stream source.
    pub(crate) fn with_ps_reorder(mut self, enabled: bool) -> Self {
        if enabled {
            self.reorder = Some(super::reorder::SparsePtsReorder::new());
            // Reconstructed PTS are approximate: only a step past the leading window is a join.
            self.decodable = Decodable::new(false);
        }
        self
    }

    /// Route a finished frame through the PTS reorderer when enabled, then keep it
    /// only if it decodes from what this output holds.
    fn finish(
        &mut self,
        explicit: Option<i64>,
        dts: Option<i64>,
        frame: Frame,
        need: Need,
    ) -> Vec<Frame> {
        match self.reorder.as_mut() {
            Some(r) => {
                self.needs.push_back(need);
                let out = r.push(explicit, frame);
                self.decide(out)
            }
            None => self
                .decodable
                .admit(frame, need, explicit, dts)
                .into_iter()
                .collect(),
        }
    }

    // Decide frames the reorderer released (decode order, display PTS assigned).
    fn decide(&mut self, frames: Vec<Frame>) -> Vec<Frame> {
        frames
            .into_iter()
            .filter_map(|f| {
                let need = self.needs.pop_front().unwrap_or(Need::Nothing);
                let pts = Some(f.pts_ns);
                self.decodable.admit(f, need, pts, None)
            })
            .collect()
    }
}

// Handle a seq_header or entry_point start-code unit (Annex B raw bytes). Strips the first-seen
// (codecPrivate-seeding) and redundant copies; emits only a genuine change vs the active body.
fn handle_header(
    first: &mut Option<Vec<u8>>,
    cur: &mut Option<Vec<u8>>,
    unit: &[u8],
) -> Option<Vec<u8>> {
    let is_first = first.is_none();
    if is_first {
        first.replace(unit.to_vec()); // seeds codecPrivate; stripped here
    }
    let changed = cur.as_deref() != Some(unit);
    if changed {
        *cur = Some(unit.to_vec());
    }
    // Strip the seeding occurrence and any unit that doesn't change the active header.
    if is_first || !changed {
        return None;
    }
    Some(unit.to_vec())
}

impl CodecParser for Vc1Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // MKV block timecodes are PRESENTATION timestamps; use PTS, not DTS —
        // DTS presents B-frames in decode order (judder, broken seeking).
        // Fall back to DTS only if PTS is absent.
        let explicit_pts = pes.pts.or(pes.dts).map(pts_to_ns);
        let dts = pes.pts.and(pes.dts).map(pts_to_ns);
        let ts_ns = explicit_pts.unwrap_or(0);
        let mut has_seq_header = false;
        let mut has_entry_point = false;
        // BROKEN_LINK and CLOSED_ENTRY: the entry-point header's first two bits (§6.2.1).
        let mut entry_flags: Option<(bool, bool)> = None;
        let mut frame_start: Option<usize> = None;
        // Track whether this AU carried a redefined (in-band) copy of each
        // header type, in separate temporaries so the keyframe prefix can be
        // assembled in canonical SMPTE 421M order regardless of scan order.
        let mut redefined_seq: Option<Vec<u8>> = None;
        let mut redefined_ep: Option<Vec<u8>> = None;

        // Scan for start codes (00 00 01 XX), reusing the shared SIMD-backed
        // scanner rather than a hand-rolled byte-by-byte loop.
        let data = &pes.data;
        let mut i = 0;
        while let Some(pos) = find_start_code(data, i) {
            if pos + 3 >= data.len() {
                break; // start code with no following type byte — nothing to read
            }
            let sc_type = data[pos + 3];
            match sc_type {
                SC_SEQUENCE_HEADER => {
                    let end = find_start_code(data, pos + 4).unwrap_or(data.len());
                    let sh = &data[pos..end];
                    // Try to parse resolution from advanced profile sequence header
                    if self.seq_header.is_none()
                        && let Some((w, h)) = parse_vc1_resolution(sh) {
                            self.width = w;
                            self.height = h;
                        }
                    if let Some(v) = handle_header(&mut self.seq_header, &mut self.cur_seq_header, sh)
                    {
                        redefined_seq = Some(v);
                    }
                    has_seq_header = true;
                }
                SC_ENTRY_POINT => {
                    let end = find_start_code(data, pos + 4).unwrap_or(data.len());
                    let b = data.get(pos + 4).copied().unwrap_or(0);
                    entry_flags = Some((b & 0x80 != 0, b & 0x40 != 0));
                    if let Some(v) = handle_header(
                        &mut self.entry_point,
                        &mut self.cur_entry_point,
                        &data[pos..end],
                    ) {
                        redefined_ep = Some(v);
                    }
                    has_entry_point = true;
                }
                SC_FRAME
                    // Frame data starts at this start code
                    if frame_start.is_none() => {
                        frame_start = Some(pos);
                    }
                _ => {}
            }
            i = pos + 4;
        }

        // Keyframe = this PES contains a sequence header (I-frame indicator in BD)
        let keyframe = has_seq_header;

        // Build the in-band prefix in canonical SMPTE 421M order — seq_header
        // (0x0F) then entry_point (0x0E) — using each header's redefined body if
        // changed else the active one, to avoid the scan-order-dependent [ep, seq] hazard.
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
                // No frame start code: if this PES carried only parameter sets
                // (already captured into codecPrivate above), drop it rather than
                // pass through as a bogus keyframe (mirrors H.264/HEVC).
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

        // An entry-point picture opens a segment; a B before the segment's next anchor is
        // leading (with CLOSED_ENTRY 0 it may predict from the anchor before the entry point).
        let picture = frame_start
            .and_then(|fs| vc1_picture(data.get(fs + 4..)?, self.cur_seq_header.as_deref()));
        let need = match picture {
            None => Need::Nothing,
            Some(_) if keyframe || entry_flags.is_some() => {
                self.anchor_since_entry = false;
                let (broken, closed) = entry_flags.unwrap_or((false, false));
                Need::Rap { closed, broken }
            }
            Some(Vc1Pic::Intra) => {
                self.anchor_since_entry = true;
                Need::Intra
            }
            Some(Vc1Pic::Predicted) => {
                self.anchor_since_entry = true;
                Need::Anchor
            }
            Some(Vc1Pic::Bi) if self.anchor_since_entry => Need::Trailing,
            Some(Vc1Pic::Bi) => Need::Leading,
            Some(Vc1Pic::IntraB) => Need::Nothing,
        };

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
        self.finish(explicit_pts, dts, frame, need)
    }

    fn flush(&mut self) -> Vec<Frame> {
        match self.reorder.as_mut() {
            Some(r) => {
                let out = r.flush();
                self.decide(out)
            }
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

// Parse width and height from a VC-1 advanced profile sequence header (00 00 01 0F...). Coded
// dimensions are 12-bit fields.
fn parse_vc1_resolution(sh: &[u8]) -> Option<(u32, u32)> {
    // Advanced profile layout (SMPTE 421M, from sh[4]): 16 bits of PROFILE..POSTPROCFLAG, then
    // MAX_CODED_WIDTH(12)+HEIGHT(12) = 40 bits = 5 bytes. Simple/Main carry no resolution.
    let bits = vc1_adv_seq_bits(sh, 5)?;
    // bits holds 40 significant bits: [16 leading][WIDTH:12][HEIGHT:12], so
    // WIDTH starts 12 bits from the LSB end and HEIGHT occupies the low 12 (shift 0).
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

#[cfg(test)]
#[path = "vc1_tests.rs"]
mod tests;
