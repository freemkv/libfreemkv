//! H.264 (AVC) elementary stream parser.
//!
//! Extracts SPS and PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IDR slices).
//! Each PES packet = one access unit = one frame.

use super::coding::{CodingType, PictureInfo};
use super::startcode::{BitReader, find_start_code, skip_start_code};
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// H.264 NAL unit types we care about.
const NAL_SLICE_NON_IDR: u8 = 1;
const NAL_SLICE_IDR: u8 = 5;
const NAL_SPS: u8 = 7;
const NAL_PPS: u8 = 8;
const NAL_AUD: u8 = 9;

/// Map an H.264 `slice_type` (Rec. ITU-T H.264 §7.4.3, Table 7-6) to a coding
/// type. Values 5..=9 repeat 0..=4 (the "all slices of this type" forms), so
/// `slice_type % 5`: 0 = P, 1 = B, 2 = I, 3 = SP (predicted → P), 4 = SI
/// (intra → I). Returns `None` for values outside 0..=9 (malformed header).
fn h264_slice_coding_type(slice_type: u32) -> Option<CodingType> {
    match slice_type {
        0..=9 => Some(match slice_type % 5 {
            0 | 3 => CodingType::P, // P, SP
            1 => CodingType::B,
            _ => CodingType::I, // 2 = I, 4 = SI
        }),
        _ => None,
    }
}

/// H.264 (AVC) Annex B → MKV codec parser: extracts SPS/PPS for the avcC
/// codecPrivate, detects IDR keyframes, and converts each PES access unit into
/// length-prefixed NAL units. Implements [`CodecParser`].
pub struct H264Parser {
    // First-seen SPS/PPS seed the MKV codecPrivate (avcC) — the only out-of-band
    // copy the player gets. BD H.264 repeats the parameter sets at every IDR;
    // a player re-applies the avcC copy at each keyframe. A stream may redefine
    // a parameter set mid-title under the SAME id with a different body. Any
    // occurrence whose body DIFFERS from the codecPrivate copy must therefore be
    // emitted IN-BAND at each point it appears so it overrides the re-applied
    // avcC set; otherwise those frames decode against the wrong parameter set.
    // (Same defect class as the HEVC PPS-redefinition bug — fixed identically.)
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    // Currently-ACTIVE body of each type (most recent the bitstream defined),
    // distinct from the fixed `sps`/`pps` codecPrivate copy. See the HEVC
    // parser for the full rationale: the strip/emit decision must be made
    // against the active set, and the active set must be re-asserted in-band at
    // every keyframe that doesn't carry it, or a streaming decoder reverts to
    // the stale avcC copy after a mid-title redefinition.
    cur_sps: Option<Vec<u8>>,
    cur_pps: Option<Vec<u8>>,
    /// Display-order PTS reconstruction, enabled only on the program-stream
    /// (HD-DVD EVO) path where the source stamps a PTS once per GOP. `None` on
    /// the BD/UHD transport path, which carries a per-frame PTS.
    reorder: Option<super::reorder::SparsePtsReorder>,
    /// MVC dependent-view (Blu-ray 3D right-eye) passthrough mode. When set, the
    /// parser does NOT strip SPS/PPS (nor re-assert at keyframes): every NAL —
    /// subset SPS (type 15), prefix (14), coded-slice-extension (20), PPS (8) —
    /// is length-prefixed in-band, so each emitted frame is a self-contained
    /// dependent access unit suitable for a Matroska `BlockAdditional`. The base
    /// view's avcC/param-set stripping is unchanged (separate parser instance).
    mvc_passthrough: bool,
}

impl Default for H264Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl H264Parser {
    /// Create a fresh H.264 parser with no parameter sets captured yet.
    pub fn new() -> Self {
        Self {
            sps: None,
            pps: None,
            cur_sps: None,
            cur_pps: None,
            reorder: None,
            mvc_passthrough: false,
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

    /// Enable MVC dependent-view passthrough (see the `mvc_passthrough` field):
    /// keep every parameter set in-band so each frame is a self-contained
    /// dependent access unit for a Matroska `BlockAdditional`. Used only for the
    /// Blu-ray 3D dependent (right-eye) stream.
    pub(crate) fn with_mvc_passthrough(mut self, enabled: bool) -> Self {
        self.mvc_passthrough = enabled;
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

/// Append `nal` to `out` as a 4-byte big-endian length prefix + body. A NAL
/// longer than `u32::MAX` can't be length-prefixed in the 4-byte field, so it
/// is skipped rather than mis-framed. Unreachable in practice (no AU > 4 GiB).
fn push_length_prefixed(out: &mut Vec<u8>, nal: &[u8]) {
    let Ok(len) = u32::try_from(nal.len()) else {
        return;
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(nal);
}

/// Handle an SPS/PPS NAL (mirrors the HEVC fix). The strip/emit decision is
/// made against the currently-ACTIVE set `cur`, NOT the codecPrivate copy
/// `first`: a streaming MKV decoder applies avcC once at init and thereafter
/// updates a parameter set only from an in-band NAL, so a switch BACK to the
/// first-seen body (== codecPrivate) is still a change the decoder must be told
/// about. Stripping on `== first` silently dropped that revert.
///
/// - First of its type → seeds codecPrivate; stripped (decoder gets it from avcC).
/// - Equal to the active set `cur` → redundant; stripped.
/// - Different from `cur` (a change in EITHER direction) → emitted in-band and
///   `cur` updated.
///
/// Returns `true` when the NAL was emitted in-band into `frame_data`.
fn handle_param_set(
    first: &mut Option<Vec<u8>>,
    cur: &mut Option<Vec<u8>>,
    nal: &[u8],
    frame_data: &mut Vec<u8>,
) -> bool {
    let is_first = first.is_none();
    if is_first {
        first.replace(nal.to_vec()); // seeds codecPrivate; stripped here
    }
    let changed = cur.as_deref() != Some(nal);
    if changed {
        *cur = Some(nal.to_vec());
    }
    if is_first || !changed {
        return false;
    }
    push_length_prefixed(frame_data, nal);
    true
}

/// Append the active parameter set `cur` to `prefix` (length-prefixed) so every
/// keyframe is SELF-CONTAINED: it carries the active SPS/PPS in-band ahead of
/// its slices. Skipped only when this access unit ALREADY carried the NAL in-band
/// (`emitted` — avoids a duplicate) or no active set exists yet.
///
/// Unconditional (not only when the active set differs from codecPrivate): a
/// streaming decoder applies the avcC param sets once at init, then relies on
/// in-band repetition. Some sources stop repeating a param set at later IDRs even
/// though its body is unchanged; if the decoder then drops it (a reset event),
/// nothing re-sends it and every subsequent slice fails (param-set id out of
/// range) until the next genuine change. Re-asserting at EVERY keyframe — what
/// compliant muxers do at every IDR — makes streaming decode self-healing.
/// Re-sending an identical param set is benign; cost is a few bytes per keyframe.
/// This strictly supersets the change-only re-assert, so the param-set-revert
/// fix is unaffected.
fn reassert_active(prefix: &mut Vec<u8>, cur: &Option<Vec<u8>>, emitted: bool) {
    if emitted {
        return;
    }
    let Some(active) = cur.as_deref() else {
        return;
    };
    push_length_prefixed(prefix, active);
}

impl CodecParser for H264Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // MKV block timecodes are PRESENTATION timestamps; frames are stored in
        // decode order and the player reorders by timecode. Use PTS, not DTS —
        // DTS presents B-frames in decode order (visible judder) and breaks
        // PTS-based seeking. Fall back to DTS only if PTS is absent.
        let explicit_pts = pes.pts.or(pes.dts).map(pts_to_ns);
        let pts_ns = explicit_pts.unwrap_or(0);

        // Single pass: detect IDR keyframes, seed/strip param sets, and convert
        // Annex B (start-code prefixed) NALUs to length-prefixed NALUs (MKV with
        // AVCDecoderConfigurationRecord expects a 4-byte length prefix per NAL).
        let mut keyframe = false;
        // Picture coding type, MEASURED from the first coded slice's header.
        let mut coding_type: Option<CodingType> = None;
        // Did this access unit already carry each param-set type in-band?
        let mut emitted_sps = false;
        let mut emitted_pps = false;
        // Pre-size: output is ~input bytes plus a few 4-byte NAL length prefixes.
        // The unsized Vec growth chain otherwise reallocs several times per
        // frame in the mux hot path (mirrors the HEVC parser).
        let mut frame_data = Vec::with_capacity(pes.data.len() + 64);

        // MVC dependent-view passthrough: keep ALL param sets in-band (the frame
        // is a self-contained BlockAdditional access unit), never strip/re-assert.
        let mvc = self.mvc_passthrough;

        for nal in NalIterator::new(&pes.data) {
            let nal_type = nal[0] & 0x1F;

            match nal_type {
                // Param sets: seed avcC, strip if unchanged vs the active set,
                // emit in-band on any change (incl. reverting to the avcC copy).
                // In MVC passthrough these fall through to the default arm so the
                // subset SPS / PPS stay in-band (self-contained dependent AU).
                NAL_SPS if !mvc => {
                    emitted_sps |=
                        handle_param_set(&mut self.sps, &mut self.cur_sps, nal, &mut frame_data)
                }
                NAL_PPS if !mvc => {
                    emitted_pps |=
                        handle_param_set(&mut self.pps, &mut self.cur_pps, nal, &mut frame_data)
                }
                // Access unit delimiters: drop. Intentional and spec-correct —
                // Matroska H.264 frame data omits AUDs (the container delimits
                // access units), so keeping them in-band is redundant. Mirrors
                // the HEVC parser.
                NAL_AUD => {}
                _ => {
                    if nal_type == NAL_SLICE_IDR {
                        keyframe = true;
                    }
                    // Measure the coding type from the FIRST coded slice's header
                    // (H.264 §7.3.3: first_mb_in_slice ue(v), then slice_type
                    // ue(v)). Populates PictureInfo so a consumer reads a MEASURED
                    // I/P/B, never a keyframe-only guess. Only the first slice of
                    // the access unit is read; `nal[1..]` is the RBSP after the
                    // 1-byte NAL header (slice_type is too early for an
                    // emulation-prevention byte to intervene).
                    if (nal_type == NAL_SLICE_NON_IDR || nal_type == NAL_SLICE_IDR)
                        && coding_type.is_none()
                    {
                        let mut br = BitReader::new(&nal[1..]);
                        if let (Some(_first_mb), Some(slice_type)) = (br.read_ue(), br.read_ue()) {
                            coding_type = h264_slice_coding_type(slice_type);
                        }
                    }
                    // A NAL longer than u32::MAX can't be length-prefixed in the
                    // 4-byte field; skip it rather than mis-frame the output.
                    // Unreachable in practice (no real AU is >4 GiB).
                    let Ok(len) = u32::try_from(nal.len()) else {
                        continue;
                    };
                    frame_data.extend_from_slice(&len.to_be_bytes());
                    frame_data.extend_from_slice(nal);
                }
            }
        }

        if frame_data.is_empty() {
            return Vec::new();
        }

        // Every keyframe is self-contained: re-assert the active SPS/PPS in-band
        // ahead of the slices (even when unchanged vs codecPrivate) so a decoder
        // that dropped the set at a reset recovers, and a stale avcC re-apply
        // can't revert it. Skipped per-type only when this AU already carried it.
        if keyframe && !mvc {
            let mut prefix = Vec::new();
            reassert_active(&mut prefix, &self.cur_sps, emitted_sps);
            reassert_active(&mut prefix, &self.cur_pps, emitted_pps);
            if !prefix.is_empty() {
                prefix.extend_from_slice(&frame_data);
                frame_data = prefix;
            }
        }

        let frame = Frame {
            // Coding-type only: H.264 field order is not decoded here, so
            // `field_order()` stays `None` — honestly absent, never guessed.
            coding: coding_type.map(PictureInfo::coding_type_only),
            source: pes.source,
            pts_ns,
            keyframe,
            // One access unit per PES (BD-TS aligns AUs to PES), so the gap
            // signal maps straight onto this frame.
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
        // Build AVCDecoderConfigurationRecord from SPS + PPS
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        if sps.len() < 4 {
            return None;
        }

        // avcC encodes each NAL's length in a 16-bit field. A param set larger
        // than 65535 bytes would truncate the length while the full bytes are
        // appended → mis-framed record. Refuse rather than emit a corrupt avcC
        // (param sets this large are non-conforming anyway).
        if sps.len() > 0xFFFF || pps.len() > 0xFFFF {
            return None;
        }

        // AVCDecoderConfigurationRecord (ISO 14496-15):
        // configurationVersion = 1
        // AVCProfileIndication = SPS[1]
        // profile_compatibility = SPS[2]
        // AVCLevelIndication = SPS[3]
        // lengthSizeMinusOne = 3 (4-byte length prefix)
        // numOfSequenceParameterSets = 1
        // sequenceParameterSetLength = sps.len()
        // sequenceParameterSetNALUnit = sps
        // numOfPictureParameterSets = 1
        // pictureParameterSetLength = pps.len()
        // pictureParameterSetNALUnit = pps
        // [High Profile extension per ISO 14496-15 §5.3.3.1.2, when applicable]

        let mut record = vec![
            1,      // configurationVersion
            sps[1], // profile
            sps[2], // compatibility
            sps[3], // level
            0xFF,   // 6 bits reserved (111111) + 2 bits lengthSizeMinusOne (11 = 3)
            0xE1,   // 3 bits reserved (111) + 5 bits numSPS (1)
            (sps.len() >> 8) as u8,
            sps.len() as u8,
        ];
        record.extend_from_slice(sps);
        record.push(1); // numPPS
        record.push((pps.len() >> 8) as u8);
        record.push(pps.len() as u8);
        record.extend_from_slice(pps);

        // ISO 14496-15 §5.3.3.1.2: for High-Profile and the related
        // chroma/bit-depth-extended profiles the record has 4 trailing extension
        // bytes carrying chroma_format_idc and the luma/chroma bit depths. The
        // full set that mandates the extension is profile_idc ∈ {100, 110, 122,
        // 144, 244 (High 4:4:4 Predictive), 44, 83, 86, 118, 128, 138, 139, 134,
        // 135}. Older parsers expect the record to END after the PPS for
        // Baseline/Main/Extended — do NOT append for those (strict parsers
        // reject the extra bytes).
        let profile_idc = sps[1];
        const HIGH_PROFILES: [u8; 14] = [
            100, 110, 122, 144, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135,
        ];
        if HIGH_PROFILES.contains(&profile_idc) {
            if let Some((chroma_fmt, depth_luma, depth_chroma)) = parse_sps_high_profile_ext(sps) {
                // byte 0: 111111xx — reserved(6) + chroma_format_idc(2)
                record.push(0xFC | (chroma_fmt & 0x03));
                // byte 1: 11111xxx — reserved(5) + bit_depth_luma_minus8(3)
                record.push(0xF8 | (depth_luma & 0x07));
                // byte 2: 11111xxx — reserved(5) + bit_depth_chroma_minus8(3)
                record.push(0xF8 | (depth_chroma & 0x07));
                // byte 3: num_of_sequence_parameter_set_ext (0 = none)
                record.push(0x00);
            }
        }

        Some(record)
    }
}

/// Parse `(chroma_format_idc, bit_depth_luma_minus8, bit_depth_chroma_minus8)` from
/// a High-Profile SPS NAL (profile_idc ∈ `HIGH_PROFILES` — the 14 chroma/bit-depth
/// extended profiles `codec_private` invokes this for).
///
/// SPS RBSP layout (ITU-T H.264 §7.3.2.1.1) up to the fields we need:
///   byte 0        NAL header (already known to be type 7)
///   byte 1        profile_idc
///   byte 2        constraint_set_flags / reserved
///   byte 3        level_idc
///   ue(v)         seq_parameter_set_id
///   — High-profile branch —
///   ue(v)         chroma_format_idc
///   if chroma_format_idc == 3: u(1) separate_colour_plane_flag
///   ue(v)         bit_depth_luma_minus8
///   ue(v)         bit_depth_chroma_minus8
///
/// RBSP emulation-prevention bytes (0x00 0x00 0x03 → 0x00 0x00) are removed
/// before bit-parsing so the bit reader sees clean RBSP data.
///
/// Returns `None` if the SPS is too short or malformed (Exp-Golomb code
/// overflows 32 bits, leading-zero count > 31, etc.). The caller silently
/// omits the extension in that case.
fn parse_sps_high_profile_ext(sps: &[u8]) -> Option<(u8, u8, u8)> {
    // Strip emulation-prevention bytes: 00 00 03 xx → 00 00 xx (drop the 03).
    // We skip byte 0 (NAL header) and start the RBSP from byte 1.
    let rbsp: Vec<u8> = {
        let raw = &sps[1..]; // skip NAL header byte
        let mut out = Vec::with_capacity(raw.len());
        let mut i = 0;
        while i < raw.len() {
            if i + 2 < raw.len() && raw[i] == 0x00 && raw[i + 1] == 0x00 && raw[i + 2] == 0x03 {
                out.push(0x00);
                out.push(0x00);
                i += 3; // skip the 0x03 emulation-prevention byte
            } else {
                out.push(raw[i]);
                i += 1;
            }
        }
        out
    };

    // RBSP layout after stripping the NAL header byte:
    //   [0] profile_idc (already checked by caller)
    //   [1] constraint flags
    //   [2] level_idc
    //   [3..] seq_parameter_set_id ue(v), then High-Profile fields
    if rbsp.len() < 4 {
        return None;
    }

    // Bit reader over rbsp[3..] (skip profile/flags/level, already known).
    let mut reader = SpsReader::new(&rbsp[3..]);

    // seq_parameter_set_id — skip
    reader.read_ue()?;

    // chroma_format_idc
    let chroma_format_idc = reader.read_ue()?;

    // separate_colour_plane_flag (only when chroma_format_idc == 3)
    if chroma_format_idc == 3 {
        reader.read_bits(1)?; // skip separate_colour_plane_flag
    }

    // bit_depth_luma_minus8
    let bit_depth_luma_minus8 = reader.read_ue()?;
    // bit_depth_chroma_minus8
    let bit_depth_chroma_minus8 = reader.read_ue()?;

    // Clamp to the 2- and 3-bit fields in the avcC extension bytes.
    // Valid H.264 values are 0..=6; the spec guarantees ≤ 6, so no real
    // content should be truncated. Out-of-spec values are clamped rather
    // than rejected so a corrupt-but-decodable SPS still produces a
    // reasonable avcC.
    Some((
        (chroma_format_idc & 0x03) as u8,
        (bit_depth_luma_minus8 & 0x07) as u8,
        (bit_depth_chroma_minus8 & 0x07) as u8,
    ))
}

/// Minimal Exp-Golomb / fixed-width bit reader over a byte slice, for SPS parsing.
struct SpsReader<'a> {
    data: &'a [u8],
    /// Current byte index.
    byte: usize,
    /// Number of bits remaining in data[byte] (0 means fully consumed, advance).
    bits_left: u8,
}

impl<'a> SpsReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte: 0,
            bits_left: if data.is_empty() { 0 } else { 8 },
        }
    }

    /// Read one bit. Returns `None` when the slice is exhausted.
    fn read_bit(&mut self) -> Option<u8> {
        if self.bits_left == 0 {
            self.byte += 1;
            if self.byte >= self.data.len() {
                return None;
            }
            self.bits_left = 8;
        }
        self.bits_left -= 1;
        Some((self.data[self.byte] >> self.bits_left) & 1)
    }

    /// Read `n` bits (n ≤ 32) as a u32, MSB first. Returns `None` on
    /// end-of-data.
    fn read_bits(&mut self, n: u8) -> Option<u32> {
        let mut val = 0u32;
        for _ in 0..n {
            val = (val << 1) | (self.read_bit()? as u32);
        }
        Some(val)
    }

    /// Read one Exp-Golomb coded unsigned integer ue(v). Leading-zero count
    /// must not exceed 31 (a 63-bit code would overflow u32). Returns `None`
    /// on end-of-data or overflow.
    fn read_ue(&mut self) -> Option<u32> {
        let mut leading_zeros = 0u8;
        loop {
            let bit = self.read_bit()?;
            if bit == 1 {
                break;
            }
            leading_zeros += 1;
            if leading_zeros > 31 {
                return None; // malformed / non-conforming SPS
            }
        }
        if leading_zeros == 0 {
            return Some(0);
        }
        let suffix = self.read_bits(leading_zeros)?;
        Some((1u32 << leading_zeros) - 1 + suffix)
    }
}

/// Iterator over NAL units in Annex B byte stream.
/// Finds start codes (00 00 01 or 00 00 00 01) and yields the data between them.
struct NalIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> NalIterator<'a> {
    fn new(data: &'a [u8]) -> Self {
        // Skip to first start code
        let pos = find_start_code(data, 0).unwrap_or(data.len());
        Self { data, pos }
    }
}

impl<'a> Iterator for NalIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        // Loop (not tail-recursion) over empty NALs: a crafted/garbled Annex B
        // stream with many adjacent start codes (e.g. 00 00 01 00 00 01 ...)
        // yields empty NALs back-to-back; recursing once per empty NAL would
        // overflow the stack. `self.pos` advances to `nal_end` each iteration,
        // so the loop always terminates. Mirrors the HEVC parser's while-scan.
        loop {
            if self.pos >= self.data.len() {
                return None;
            }

            // Skip the start code at current position
            let nal_start = skip_start_code(self.data, self.pos)?;

            // Find next start code (or end of data)
            let nal_end = find_start_code(self.data, nal_start).unwrap_or(self.data.len());

            // Strip the leading zeros of the following start code. For a
            // conforming bitstream this is lossless: rbsp_trailing_bits() sets a
            // stop-one bit, so the final byte of any RBSP is never 0x00 — the only
            // trailing zeros here belong to the next 00 00 (00) 01 prefix, never to
            // the NAL's RBSP payload. (Mirrors the HEVC parser.)
            let mut end = nal_end;
            while end > nal_start && self.data[end - 1] == 0x00 {
                end -= 1;
            }

            self.pos = nal_end;

            if end > nal_start {
                return Some(&self.data[nal_start..end]);
            }
            // Empty NAL — continue scanning instead of recursing.
        }
    }
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

    // --- parse SPS+PPS → codec_private ---

    #[test]
    fn parse_sps_pps() {
        let mut parser = H264Parser::new();

        // Build PES with SPS (type 7) + PPS (type 8) + IDR slice (type 5)
        // SPS NAL: 0x67 = 0_11_00111 (nal_type = 7), followed by profile/compat/level + payload
        // PPS NAL: 0x68 = 0_11_01000 (nal_type = 8)
        let mut data = Vec::new();
        // SPS: 00 00 01 [67 42 00 1E <payload>]
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x67); // SPS
        data.extend_from_slice(&[0x42, 0x00, 0x1E, 0xAB, 0xCD]); // profile=0x42, compat=0x00, level=0x1E
        // PPS: 00 00 01 [68 <payload>]
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x68); // PPS
        data.extend_from_slice(&[0xCE, 0x01]);
        // IDR slice: 00 00 01 [65 <payload>]
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65); // IDR
        data.extend_from_slice(&[0x88, 0x00, 0x10]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        // codec_private should now be available
        let cp = parser.codec_private();
        assert!(
            cp.is_some(),
            "codec_private should be Some after seeing SPS+PPS"
        );
        let cp = cp.unwrap();

        // AVCDecoderConfigurationRecord checks
        assert_eq!(cp[0], 1, "configurationVersion");
        assert_eq!(cp[1], 0x42, "profile from SPS[1]");
        assert_eq!(cp[2], 0x00, "compatibility from SPS[2]");
        assert_eq!(cp[3], 0x1E, "level from SPS[3]");
        assert_eq!(cp[4], 0xFF, "reserved + lengthSizeMinusOne=3");
        assert_eq!(cp[5], 0xE1, "reserved + numSPS=1");

        // Frames should have been produced
        assert_eq!(frames.len(), 1);
    }

    // Length-prefixed NAL bodies out of frame_data, and the H.264 PPS (type 8)
    // payloads among them.
    fn h264_nals_in(frame: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut i = 0;
        while i + 4 <= frame.len() {
            let len =
                u32::from_be_bytes([frame[i], frame[i + 1], frame[i + 2], frame[i + 3]]) as usize;
            i += 4;
            if i + len > frame.len() {
                break;
            }
            out.push(frame[i..i + len].to_vec());
            i += len;
        }
        out
    }
    fn h264_pps_bodies(nals: &[Vec<u8>]) -> Vec<Vec<u8>> {
        nals.iter()
            .filter(|n| !n.is_empty() && n[0] & 0x1F == 8)
            .map(|n| n[1..].to_vec())
            .collect()
    }
    fn h264_nal(t: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, t];
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn mvc_passthrough_keeps_param_sets_inband() {
        // A dependent-view access unit: subset SPS (NAL 15) + PPS (NAL 8) +
        // coded-slice-extension (NAL 20). No IDR (type 5), so keyframe stays
        // false and there is no keyframe re-assertion.
        let au = || {
            let mut d = Vec::new();
            d.extend_from_slice(&h264_nal(0x6F, &[0x80, 0x00, 0x33, 0xAA])); // subset SPS (15)
            d.extend_from_slice(&h264_nal(0x68, &[0xCE, 0x01])); // PPS (8)
            d.extend_from_slice(&h264_nal(0x74, &[0x11, 0x22])); // slice-ext (20)
            d
        };
        let nal_types =
            |f: &Frame| -> Vec<u8> { h264_nals_in(&f.data).iter().map(|n| n[0] & 0x1F).collect() };

        // Normal parser strips the PPS from a non-keyframe AU (it is captured for
        // the avcC and, without an IDR, never re-asserted in-band).
        let mut normal = H264Parser::new();
        let f = normal.parse(&make_pes(au(), Some(90000)));
        assert_eq!(f.len(), 1);
        assert!(
            !nal_types(&f[0]).contains(&8),
            "normal parser strips PPS from a non-keyframe AU: {:?}",
            nal_types(&f[0])
        );

        // Passthrough keeps EVERY parameter set in-band, so each dependent frame
        // is a self-contained access unit for a BlockAdditional.
        let mut pt = H264Parser::new().with_mvc_passthrough(true);
        let f = pt.parse(&make_pes(au(), Some(90000)));
        assert_eq!(f.len(), 1);
        let types = nal_types(&f[0]);
        assert!(types.contains(&15), "subset SPS kept in-band: {types:?}");
        assert!(
            types.contains(&8),
            "PPS kept in-band under passthrough: {types:?}"
        );
        assert!(types.contains(&20), "slice kept: {types:?}");
    }

    #[test]
    fn h264_populates_measured_coding_type_and_source() {
        use super::super::coding::CodingType;
        // Slice-header body = first_mb_in_slice=0 ('1') then slice_type ue(v):
        //   0x88 = '1 0001000' → slice_type 7 (I)
        //   0x98 = '1 00110..' → slice_type 5 (P)
        //   0x9C = '1 00111..' → slice_type 6 (B)
        let src = crate::pes::SourcePos::at_byte(8192);
        let parse = |nal_type: u8, body: u8| {
            let mut p = H264Parser::new();
            let mut pe = make_pes(h264_nal(nal_type, &[body]), Some(0));
            pe.source = Some(src);
            p.parse(&pe)
        };

        // IDR carrying an I-slice → keyframe + MEASURED I; source carried; H.264
        // field order is not decoded, so it is honestly absent (not guessed).
        let fi = parse(NAL_SLICE_IDR, 0x88);
        assert_eq!(fi.len(), 1);
        assert!(fi[0].keyframe, "IDR is a keyframe");
        let ci = fi[0].coding.expect("H.264 frame carries PictureInfo");
        assert_eq!(ci.coding_type(), CodingType::I, "slice_type 7 → I");
        assert!(
            ci.field_order().is_none(),
            "H.264 field order undecoded → None, never faked"
        );
        assert_eq!(
            fi[0].source.unwrap().byte,
            8192,
            "source provenance carried"
        );

        // Non-IDR P / B slices → MEASURED P / B, not keyframes.
        let fp = parse(NAL_SLICE_NON_IDR, 0x98);
        assert_eq!(
            fp[0].coding.unwrap().coding_type(),
            CodingType::P,
            "slice_type 5 → P"
        );
        assert!(!fp[0].keyframe);
        let fb = parse(NAL_SLICE_NON_IDR, 0x9C);
        assert_eq!(
            fb[0].coding.unwrap().coding_type(),
            CodingType::B,
            "slice_type 6 → B"
        );
    }

    /// End-to-end sparse-PTS reconstruction through the REAL parser + reorder:
    /// a program-stream source (`with_ps_reorder(true)`) that stamps a PTS only
    /// on each GOP's I-frame must yield distinct, display-ordered PTS for every
    /// frame — the property the mkv muxer needs so a decoder derives monotonic
    /// DTS. Without the reorder the non-anchor frames all collapse to one PTS.
    #[test]
    fn h264_ps_reorder_reconstructs_distinct_display_pts() {
        use super::super::coding::CodingType;
        // slice bodies: 0x88 → I (IDR), 0x98 → P, 0x9C → B (non-IDR).
        // Decode order of a classic single-B GOP: I P B P B.
        let gop = |anchor_pts: Option<i64>| {
            vec![
                (NAL_SLICE_IDR, 0x88u8, anchor_pts),
                (NAL_SLICE_NON_IDR, 0x98, None),
                (NAL_SLICE_NON_IDR, 0x9C, None),
                (NAL_SLICE_NON_IDR, 0x98, None),
                (NAL_SLICE_NON_IDR, 0x9C, None),
            ]
        };

        let feed = |reorder: bool| -> Vec<super::super::Frame> {
            let mut p = H264Parser::new().with_ps_reorder(reorder);
            let mut out = Vec::new();
            // Two GOPs; the second I carries an anchor 5 frames later (90 kHz:
            // 5 * 3750 = 18750 ticks) so the reorder can calibrate a duration.
            for (nal, body, pts) in gop(Some(0)).into_iter().chain(gop(Some(18750))) {
                out.extend(p.parse(&make_pes(h264_nal(nal, &[body]), pts)));
            }
            out.extend(p.flush());
            out
        };

        // With reorder ON: all 10 frames emitted, every PTS distinct.
        let recon = feed(true);
        assert_eq!(recon.len(), 10, "no frame dropped");
        let mut pts: Vec<i64> = recon.iter().map(|f| f.pts_ns).collect();
        let n = pts.len();
        pts.sort_unstable();
        pts.dedup();
        assert_eq!(
            pts.len(),
            n,
            "reconstructed PTS are all distinct (no DTS collision)"
        );

        // The GOP's first-displayed frame is the I; the B in decode position 2
        // must display BEFORE the P in decode position 1 (classic reorder).
        let g1 = &recon[0..5];
        assert_eq!(g1[0].coding.unwrap().coding_type(), CodingType::I);
        assert!(
            g1[2].pts_ns < g1[1].pts_ns,
            "B (decode idx 2) displays before its forward-anchor P (decode idx 1)"
        );
        assert_eq!(g1[0].pts_ns, 0, "GOP anchor locks the I to its true PTS");

        // With reorder OFF (transport-stream behaviour): the non-anchor frames
        // collapse to a single colliding PTS — the bug this fix removes.
        let raw = feed(false);
        let collisions = raw.iter().filter(|f| f.pts_ns == 0).count();
        assert!(
            collisions >= 8,
            "without reorder the sparse-PTS frames collide on 0 (got {collisions})"
        );
    }

    /// Regression (Fight Club bug, H.264 variant): PPS id 0 = body A (→ avcC),
    /// redefined to B, then switched BACK to A. A streaming decoder is on B; the
    /// revert to A == avcC must still be emitted in-band or the A-segment
    /// decodes against B.
    #[test]
    fn h264_emits_switch_back_to_codecprivate_pps() {
        let a = [0xA1u8, 0xA2];
        let b = [0xB1u8, 0xB2, 0xB3];
        let mut p = H264Parser::new();
        // AU1: SPS (seed avcC) + PPS-A (seed) + IDR.
        p.parse(&make_pes(
            [
                h264_nal(0x67, &[0x42, 0x00, 0x1E, 0xAB]),
                h264_nal(0x68, &a),
                h264_nal(0x65, &[1]),
            ]
            .concat(),
            Some(0),
        ));
        // AU2 IDR: redefine PPS to B → emitted in-band.
        let f2 = p.parse(&make_pes(
            [h264_nal(0x68, &b), h264_nal(0x65, &[2])].concat(),
            Some(1),
        ));
        assert!(
            h264_pps_bodies(&h264_nals_in(&f2[0].data))
                .iter()
                .any(|x| x == &b),
            "AU2 must carry redefined PPS-B in-band"
        );
        // AU3 IDR: back to A (== avcC) — must be emitted in-band (active was B).
        let f3 = p.parse(&make_pes(
            [h264_nal(0x68, &a), h264_nal(0x65, &[3])].concat(),
            Some(2),
        ));
        assert!(
            h264_pps_bodies(&h264_nals_in(&f3[0].data))
                .iter()
                .any(|x| x == &a),
            "switch back to avcC PPS-A must be emitted in-band"
        );
    }

    /// Regression: a bare IDR keyframe (source omits the PPS) after a mid-title
    /// redefinition must re-assert the active PPS in-band.
    #[test]
    fn h264_reasserts_active_pps_at_bare_keyframe() {
        let a = [0xA1u8, 0xA2];
        let b = [0xB1u8, 0xB2, 0xB3];
        let mut p = H264Parser::new();
        p.parse(&make_pes(
            [
                h264_nal(0x67, &[0x42, 0x00, 0x1E, 0xAB]),
                h264_nal(0x68, &a),
                h264_nal(0x65, &[1]),
            ]
            .concat(),
            Some(0),
        ));
        // Redefine to B at a keyframe.
        p.parse(&make_pes(
            [h264_nal(0x68, &b), h264_nal(0x65, &[2])].concat(),
            Some(1),
        ));
        // Bare IDR (no PPS): active B must be re-asserted; stale A must not be.
        let f3 = p.parse(&make_pes(h264_nal(0x65, &[3]), Some(2)));
        let got = h264_pps_bodies(&h264_nals_in(&f3[0].data));
        assert!(
            got.iter().any(|x| x == &b),
            "bare keyframe must re-assert active PPS-B"
        );
        assert!(
            !got.iter().any(|x| x == &a),
            "must not re-assert stale avcC PPS-A"
        );
    }

    #[test]
    fn codec_private_none_before_sps_pps() {
        let parser = H264Parser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- IDR keyframe detection ---

    #[test]
    fn parse_idr_keyframe() {
        let mut parser = H264Parser::new();

        // PES with IDR NAL (type 5 = 0x65)
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65); // IDR slice (nal_type = 5)
        data.extend_from_slice(&[0x88, 0x00, 0x10, 0x20]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(
            frames[0].keyframe,
            "IDR slice should be detected as keyframe"
        );
    }

    // --- non-IDR → not keyframe ---

    #[test]
    fn parse_non_idr() {
        let mut parser = H264Parser::new();

        // PES with non-IDR slice (type 1 = 0x61 or 0x41)
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41); // non-IDR coded slice (nal_type = 1)
        data.extend_from_slice(&[0x9A, 0x00, 0x10]);

        let pes = make_pes(data, Some(180000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "non-IDR slice should not be keyframe");
    }

    // --- length prefix conversion ---

    #[test]
    fn length_prefix_conversion() {
        let mut parser = H264Parser::new();

        // PES with a single non-IDR NAL
        let nal_payload = [0x41, 0xAA, 0xBB, 0xCC, 0xDD]; // type 1, 5 bytes
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&nal_payload);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        let frame_data = &frames[0].data;

        // Should start with 4-byte big-endian length prefix
        assert!(
            frame_data.len() >= 4,
            "frame data should have length prefix"
        );
        let length =
            u32::from_be_bytes([frame_data[0], frame_data[1], frame_data[2], frame_data[3]]);
        assert_eq!(
            length as usize,
            nal_payload.len(),
            "length prefix should match NAL size"
        );

        // Followed by the NAL data itself
        assert_eq!(&frame_data[4..], &nal_payload);

        // No start code (00 00 01) should appear in the output
        for i in 0..frame_data.len().saturating_sub(2) {
            let is_sc =
                frame_data[i] == 0x00 && frame_data[i + 1] == 0x00 && frame_data[i + 2] == 0x01;
            assert!(!is_sc, "output should not contain Annex B start codes");
        }
    }

    // --- AUD is stripped; SPS/PPS seed avcC and re-assert at the keyframe ---

    #[test]
    fn aud_stripped_param_sets_reasserted_at_keyframe() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        // AUD (type 9) — always dropped
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x09);
        data.push(0xF0);
        // SPS (type 7)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x67);
        data.extend_from_slice(&[0x42, 0x00, 0x1E, 0xAB]);
        // PPS (type 8)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x68);
        data.extend_from_slice(&[0xCE, 0x01]);
        // IDR (type 5)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65);
        data.extend_from_slice(&[0x88, 0x00]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);

        // SPS/PPS seed avcC...
        assert!(parser.codec_private().is_some(), "SPS/PPS seed avcC");
        // ...and because this is a keyframe, the active SPS/PPS are re-asserted
        // in-band ahead of the IDR so the keyframe is self-contained. AUD (9) is
        // always dropped. Frame data = SPS(7), PPS(8), IDR(5).
        let fd = &frames[0].data;
        let mut types = Vec::new();
        let mut o = 0;
        while o + 4 <= fd.len() {
            let len = u32::from_be_bytes([fd[o], fd[o + 1], fd[o + 2], fd[o + 3]]) as usize;
            o += 4;
            types.push(fd[o] & 0x1F);
            o += len;
        }
        assert_eq!(
            types,
            vec![7, 8, 5],
            "keyframe: SPS+PPS re-asserted ahead of IDR, AUD dropped"
        );
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41);
        data.extend_from_slice(&[0x00, 0x10]);

        // PTS = 90000 (1 second at 90kHz) → 1_000_000_000 ns
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // --- empty PES ---

    #[test]
    fn parse_empty_pes() {
        let mut parser = H264Parser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- PTS (presentation) used for the MKV block timecode, not DTS ---

    #[test]
    fn pts_preferred_over_dts() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41);
        data.extend_from_slice(&[0x00, 0x10]);

        let pes = PesPacket {
            source: None,
            pid: 0x1011,
            pts: Some(180000), // 2 seconds (presentation)
            dts: Some(90000),  // 1 second (decode)
            data,
            discontinuity: false,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        // PTS must be used — MKV block timecodes are presentation timestamps.
        assert_eq!(frames[0].pts_ns, 2_000_000_000);
    }

    // --- mid-title param-set redefinition emitted in-band ---

    /// Collect the NAL types from a length-prefixed frame_data buffer.
    fn frame_nal_types(fd: &[u8]) -> Vec<u8> {
        let mut types = Vec::new();
        let mut off = 0;
        while off + 4 <= fd.len() {
            let len = u32::from_be_bytes([fd[off], fd[off + 1], fd[off + 2], fd[off + 3]]) as usize;
            off += 4;
            if off + len > fd.len() {
                break;
            }
            types.push(fd[off] & 0x1F);
            off += len;
        }
        types
    }

    #[test]
    fn keyframes_self_contained_and_redefinition_emitted() {
        let mut parser = H264Parser::new();

        // AU 1: SPS(id0,bodyA) + PPS(id0,bodyA) + IDR. The param sets seed avcC,
        // and because this is a keyframe the active SPS/PPS are re-asserted
        // in-band ahead of the IDR (self-contained keyframe). Frame = SPS,PPS,IDR.
        let mut au1 = Vec::new();
        au1.extend_from_slice(&[0x00, 0x00, 0x01]);
        au1.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]); // SPS body A
        au1.extend_from_slice(&[0x00, 0x00, 0x01]);
        au1.extend_from_slice(&[0x68, 0x11]); // PPS body A
        au1.extend_from_slice(&[0x00, 0x00, 0x01]);
        au1.extend_from_slice(&[0x65, 0x10, 0x20]); // IDR
        let f1 = parser.parse(&make_pes(au1, Some(0)));
        assert_eq!(f1.len(), 1);
        assert_eq!(
            frame_nal_types(&f1[0].data),
            vec![7, 8, 5],
            "AU1 keyframe: SPS+PPS re-asserted ahead of IDR"
        );

        // AU 2: SPS identical to avcC (re-asserted unchanged at the keyframe),
        // PPS REDEFINED (same id, different body) → emitted in-band as a change.
        // Frame = SPS(re-asserted), PPS(redefined), IDR.
        let mut au2 = Vec::new();
        au2.extend_from_slice(&[0x00, 0x00, 0x01]);
        au2.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]); // SPS == body A
        au2.extend_from_slice(&[0x00, 0x00, 0x01]);
        au2.extend_from_slice(&[0x68, 0x22]); // PPS body B (redefinition)
        au2.extend_from_slice(&[0x00, 0x00, 0x01]);
        au2.extend_from_slice(&[0x65, 0x30, 0x40]); // IDR
        let f2 = parser.parse(&make_pes(au2, Some(90000)));
        assert_eq!(f2.len(), 1);
        let types = frame_nal_types(&f2[0].data);
        assert_eq!(types, vec![7, 8, 5], "got {types:?}");
        // Confirm the in-band PPS is the REDEFINED body B (0x22), not avcC's A.
        let mut o = 0;
        let mut pps_body = None;
        while o + 4 <= f2[0].data.len() {
            let len = u32::from_be_bytes([
                f2[0].data[o],
                f2[0].data[o + 1],
                f2[0].data[o + 2],
                f2[0].data[o + 3],
            ]) as usize;
            o += 4;
            if f2[0].data[o] & 0x1F == 8 {
                pps_body = Some(f2[0].data[o + 1]);
            }
            o += len;
        }
        assert_eq!(
            pps_body,
            Some(0x22),
            "in-band PPS must be the redefined body B"
        );
    }

    #[test]
    fn repeated_identical_param_sets_reasserted_each_keyframe() {
        let mut parser = H264Parser::new();
        let mut au = Vec::new();
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]);
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&[0x68, 0x11]);
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&[0x65, 0x10]);
        // Two identical AUs. Each is a keyframe, so each re-asserts the active
        // SPS/PPS in-band (self-contained keyframe) even though the bodies are
        // unchanged — a decoder that dropped them at a reset recovers at every
        // IDR. Frame = SPS, PPS, IDR.
        parser.parse(&make_pes(au.clone(), Some(0)));
        let f = parser.parse(&make_pes(au, Some(90000)));
        assert_eq!(
            frame_nal_types(&f[0].data),
            vec![7, 8, 5],
            "each keyframe re-asserts the active SPS/PPS in-band"
        );
    }

    #[test]
    fn many_empty_nals_do_not_overflow_stack() {
        // Regression: NalIterator::next must iterate, not recurse, over empty
        // NALs. A crafted Annex B stream of tens of thousands of adjacent start
        // codes (each producing an empty NAL) would blow the stack under the old
        // tail-recursive implementation. Iterating handles it in bounded stack.
        let mut data = Vec::new();
        // 50_000 back-to-back 3-byte start codes → 50_000 empty NALs.
        for _ in 0..50_000 {
            data.extend_from_slice(&[0x00, 0x00, 0x01]);
        }
        // One real NAL at the end so the iterator yields something.
        data.extend_from_slice(&[0x41, 0xAA, 0xBB]);

        let mut parser = H264Parser::new();
        let frames = parser.parse(&make_pes(data, Some(0)));
        // Exactly one populated frame; the empty NALs are skipped without
        // overflowing.
        assert_eq!(frames.len(), 1);
        let fd = &frames[0].data;
        let len = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]) as usize;
        assert_eq!(len, 3, "the single real NAL is length-prefixed");
        assert_eq!(fd[4], 0x41);
    }

    // --- avcC exact byte layout (ISO 14496-15 §5.2.4.1) ---

    #[test]
    fn avcc_exact_length_fields_and_payload() {
        // The AVCDecoderConfigurationRecord must encode SPS length and PPS length
        // as 16-bit big-endian fields, followed by the verbatim NAL bodies.
        // Uses a Main-Profile SPS (profile_idc=0x4D=77) so no High-Profile
        // extension bytes are appended — the test validates the fixed-header
        // layout only. High-Profile extension is covered by
        // avcc_high_profile_appends_extension_bytes.
        // SPS = 0x67,profile,compat,level + 2 payload bytes (6 bytes total).
        // PPS = 0x68 + 2 payload bytes (3 bytes total).
        let mut parser = H264Parser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&[0x67, 0x4D, 0x00, 0x28, 0xAB, 0xCD]); // SPS, 6 bytes, Main Profile (77)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&[0x68, 0xEE, 0x3C]); // PPS, 3 bytes
        // A slice so a frame is produced (not required for codec_private though).
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x11]);
        parser.parse(&make_pes(data, Some(0)));

        let cp = parser.codec_private().expect("avcC");
        // Fixed header.
        assert_eq!(cp[0], 1, "configurationVersion");
        assert_eq!(cp[1], 0x4D, "AVCProfileIndication = SPS[1]");
        assert_eq!(cp[2], 0x00, "profile_compatibility = SPS[2]");
        assert_eq!(cp[3], 0x28, "AVCLevelIndication = SPS[3]");
        assert_eq!(cp[4], 0xFF, "lengthSizeMinusOne nibble (4-byte prefix)");
        assert_eq!(cp[5], 0xE1, "numSPS = 1");
        // sequenceParameterSetLength (16-bit BE) = 6.
        assert_eq!(u16::from_be_bytes([cp[6], cp[7]]), 6, "SPS length field");
        // SPS body follows verbatim.
        assert_eq!(&cp[8..14], &[0x67, 0x4D, 0x00, 0x28, 0xAB, 0xCD]);
        // numPPS = 1.
        assert_eq!(cp[14], 1, "numPPS");
        // pictureParameterSetLength (16-bit BE) = 3.
        assert_eq!(u16::from_be_bytes([cp[15], cp[16]]), 3, "PPS length field");
        // PPS body verbatim.
        assert_eq!(&cp[17..20], &[0x68, 0xEE, 0x3C]);
        // Record length is exactly the sum of its parts — no extension bytes for Main Profile.
        assert_eq!(cp.len(), 20);
    }

    #[test]
    fn avcc_none_when_sps_shorter_than_four_bytes() {
        // codec_private reads SPS[1..=3] for profile/compat/level, so an SPS
        // shorter than 4 bytes can't form a valid avcC → None (guard
        // `sps.len() < 4`). A 3-byte SPS (header + 2 bytes) triggers it.
        let mut parser = H264Parser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x67, 0x42]); // SPS = 2 bytes
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]); // PPS
        parser.parse(&make_pes(data, Some(0)));
        assert!(
            parser.codec_private().is_none(),
            "SPS < 4 bytes must not yield an avcC"
        );
    }

    #[test]
    fn avcc_none_with_sps_but_no_pps() {
        // Both SPS and PPS are required. SPS only → None.
        let mut parser = H264Parser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAA]);
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x10]); // IDR, no PPS
        parser.parse(&make_pes(data, Some(0)));
        assert!(parser.codec_private().is_none());
    }

    // --- NAL type extraction: forbidden_zero_bit + nal_ref_idc are masked ---

    #[test]
    fn nal_type_masks_high_three_bits() {
        // nal_type = byte0 & 0x1F. The forbidden_zero_bit (bit 7) and
        // nal_ref_idc (bits 6-5) must not affect type detection. An IDR (type 5)
        // header is 0x65 (nal_ref_idc=3) or 0x25 (nal_ref_idc=1) — both type 5,
        // both keyframes.
        for idr_hdr in [0x65u8, 0x25, 0x05, 0x85] {
            let mut parser = H264Parser::new();
            let data = vec![0x00, 0x00, 0x01, idr_hdr, 0x10, 0x20];
            let f = parser.parse(&make_pes(data, Some(0)));
            assert_eq!(f.len(), 1);
            assert!(
                f[0].keyframe,
                "header {idr_hdr:#x} is NAL type 5 (IDR) → keyframe"
            );
        }
    }

    #[test]
    fn sps_recognized_regardless_of_ref_idc() {
        // SPS is type 7; header 0x67 (ref_idc 3) and 0x27 (ref_idc 1) are both
        // SPS and must seed codec_private identically.
        for sps_hdr in [0x67u8, 0x27] {
            let mut parser = H264Parser::new();
            let mut data = vec![0x00, 0x00, 0x01, sps_hdr, 0x42, 0x00, 0x1E, 0xAA];
            data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]); // PPS
            parser.parse(&make_pes(data, Some(0)));
            let cp = parser.codec_private().expect("avcC");
            assert_eq!(cp[1], 0x42, "profile from SPS[1] regardless of ref_idc");
        }
    }

    // --- 4-byte start code handling ---

    #[test]
    fn four_byte_start_code_parsed() {
        // A 4-byte start code (00 00 00 01) must be skipped correctly so the NAL
        // body begins at the right offset (skip_start_code returns pos+4).
        let mut parser = H264Parser::new();
        let data = vec![0x00, 0x00, 0x00, 0x01, 0x41, 0xAA, 0xBB];
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        let len = u32::from_be_bytes([f[0].data[0], f[0].data[1], f[0].data[2], f[0].data[3]]);
        // NAL = 0x41 0xAA 0xBB = 3 bytes (trailing 0xBB kept; not a zero).
        assert_eq!(len, 3);
        assert_eq!(&f[0].data[4..], &[0x41, 0xAA, 0xBB]);
    }

    #[test]
    fn trailing_zeros_of_next_start_code_stripped_from_nal() {
        // The byte(s) before a following 4-byte start code (00 00 00 01) are
        // leading zeros of that start code, not RBSP, and must be stripped from
        // the current NAL. Two NALs separated by a 4-byte start code: NAL 1 must
        // not absorb the extra 00.
        let mut parser = H264Parser::new();
        let mut data = vec![0x00, 0x00, 0x01, 0x41, 0xAA]; // NAL1 = 0x41 0xAA
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x41, 0xBB]); // 4-byte SC
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        // Walk length-prefixed NALs; first must be exactly 2 bytes (0x41 0xAA),
        // NOT 3 (it must not swallow the leading 0x00 of the next start code).
        let len1 = u32::from_be_bytes([f[0].data[0], f[0].data[1], f[0].data[2], f[0].data[3]]);
        assert_eq!(len1, 2, "NAL1 must not absorb the next start code's zeros");
        assert_eq!(&f[0].data[4..6], &[0x41, 0xAA]);
    }

    #[test]
    fn aud_dropped_but_following_slice_kept() {
        // AUD (type 9) is dropped from frame data; a following slice survives.
        let mut parser = H264Parser::new();
        let mut data = vec![0x00, 0x00, 0x01, 0x09, 0xF0]; // AUD
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x41, 0xAA, 0xBB]); // slice
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            frame_nal_types(&f[0].data),
            vec![1],
            "only the slice remains"
        );
    }

    #[test]
    fn param_set_only_pes_emits_no_frame() {
        // A PES carrying ONLY SPS+PPS (both stripped into avcC) has no in-band
        // NAL → frame_data empty → no frame emitted (mirrors HEVC/MPEG2/VC1).
        let mut parser = H264Parser::new();
        let mut data = vec![0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAA];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]);
        let f = parser.parse(&make_pes(data, Some(0)));
        assert!(f.is_empty(), "param-set-only PES emits no frame");
        // But the avcC is captured.
        assert!(parser.codec_private().is_some());
    }

    #[test]
    fn dts_fallback_when_pts_absent() {
        // PTS absent → DTS is used (or().map). pts.or(dts) per the comment.
        let mut parser = H264Parser::new();
        let pes = PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: Some(90000),
            data: vec![0x00, 0x00, 0x01, 0x41, 0x10],
            discontinuity: false,
        };
        let f = parser.parse(&pes);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "falls back to DTS");
    }

    #[test]
    fn no_pts_no_dts_defaults_zero() {
        let mut parser = H264Parser::new();
        let pes = PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: None,
            data: vec![0x00, 0x00, 0x01, 0x41, 0x10],
            discontinuity: false,
        };
        let f = parser.parse(&pes);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 0);
    }

    #[test]
    fn no_start_code_emits_nothing() {
        // A PES with no Annex B start code yields no NAL → no frame (NalIterator
        // starts at data.len()).
        let mut parser = H264Parser::new();
        let f = parser.parse(&make_pes(vec![0x41, 0xAA, 0xBB, 0xCC], Some(0)));
        assert!(f.is_empty(), "no start code → no NAL → no frame");
    }

    #[test]
    fn avcc_oversized_param_set_returns_none() {
        // A param set > 65535 bytes can't be length-encoded in avcC's 16-bit
        // field; codec_private must refuse rather than emit a truncated record.
        let mut parser = H264Parser::new();
        let mut data = Vec::new();
        // Oversized SPS (header byte 0x67 + 70000 filler bytes).
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x67);
        data.extend_from_slice(&vec![0x11u8; 70_000]);
        // PPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&[0x68, 0x11]);
        parser.parse(&make_pes(data, Some(0)));
        assert!(
            parser.codec_private().is_none(),
            "oversized SPS must not produce a truncated avcC"
        );
    }

    // --- High Profile avcC extension (ISO 14496-15 §5.3.3.1.2) ---

    /// Build a minimal High-Profile SPS RBSP with the fields needed for the
    /// avcC extension. The SPS bytes (NAL-header included) are:
    ///   [0x67]  NAL header (type=7, ref_idc=3)
    ///   [profile_idc] [constraint_flags] [level_idc]
    ///   ue(v) seq_parameter_set_id = 0  → 1 bit: 0b1
    ///   ue(v) chroma_format_idc         → depends on value
    ///   if chroma_format_idc==3: u(1) separate_colour_plane_flag
    ///   ue(v) bit_depth_luma_minus8
    ///   ue(v) bit_depth_chroma_minus8
    ///
    /// All ue(v) values <= 6 fit within 3 leading zeros + 3 suffix bits (7 bits
    /// total): prefix = leading_zeros + stop-1 bit, suffix = leading_zeros bits.
    /// For small values (0..=2), the unary prefix + code is short enough to
    /// pack manually with a simple bit-packing helper.
    fn build_high_profile_sps(
        profile_idc: u8,
        chroma_format_idc: u32,
        bit_depth_luma_minus8: u32,
        bit_depth_chroma_minus8: u32,
    ) -> Vec<u8> {
        // Bit-pack the ue(v) fields into a byte buffer after the fixed header.
        // We append bits MSB-first into a growing Vec<u8>.
        struct BitWriter {
            buf: Vec<u8>,
            cur: u8,
            bits: u8, // bits accumulated in `cur` (0..8)
        }
        impl BitWriter {
            fn new() -> Self {
                Self {
                    buf: Vec::new(),
                    cur: 0,
                    bits: 0,
                }
            }
            fn push_bit(&mut self, bit: u8) {
                self.cur = (self.cur << 1) | (bit & 1);
                self.bits += 1;
                if self.bits == 8 {
                    self.buf.push(self.cur);
                    self.cur = 0;
                    self.bits = 0;
                }
            }
            fn write_ue(&mut self, val: u32) {
                // Exp-Golomb encode: find k such that 2^k - 1 <= val, then
                // k leading zeros + 1 stop + k-bit suffix.
                if val == 0 {
                    self.push_bit(1);
                    return;
                }
                let code = val + 1; // code = val + 1, k = floor(log2(code))
                let k = 31 - code.leading_zeros();
                for _ in 0..k {
                    self.push_bit(0);
                } // k leading zeros
                self.push_bit(1); // stop bit
                for i in (0..k).rev() {
                    self.push_bit(((code >> i) & 1) as u8);
                }
            }
            fn finish(mut self) -> Vec<u8> {
                // Flush partial byte (padding with zeros on the right — RBSP
                // trailing bits pattern, sufficient for our test payload).
                if self.bits > 0 {
                    self.cur <<= 8 - self.bits;
                    self.buf.push(self.cur);
                }
                self.buf
            }
        }

        let mut w = BitWriter::new();
        w.write_ue(0); // seq_parameter_set_id = 0
        w.write_ue(chroma_format_idc);
        if chroma_format_idc == 3 {
            w.push_bit(0); // separate_colour_plane_flag = 0
        }
        w.write_ue(bit_depth_luma_minus8);
        w.write_ue(bit_depth_chroma_minus8);
        let payload = w.finish();

        let mut sps = vec![
            0x67, // NAL header (type=7)
            profile_idc,
            0x00, // constraint flags
            0x28, // level_idc = 4.0
        ];
        sps.extend_from_slice(&payload);
        sps
    }

    fn feed_sps_pps(parser: &mut H264Parser, sps_bytes: &[u8]) {
        // Feed a PES containing: custom SPS + a minimal PPS + an IDR slice.
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(sps_bytes);
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0xCE, 0x01]); // PPS
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x88]); // IDR
        parser.parse(&make_pes(data, Some(0)));
    }

    /// ISO 14496-15 §5.3.3.1.2 regression: a High-Profile SPS (profile_idc=100)
    /// must produce an avcC with the 4 extension bytes (chroma_format_idc,
    /// bit_depth_luma_minus8, bit_depth_chroma_minus8, num_sps_ext=0).
    #[test]
    fn avcc_high_profile_appends_extension_bytes() {
        // profile_idc=100 (High), chroma_format_idc=1 (4:2:0), depths both 0.
        let sps = build_high_profile_sps(100, 1, 0, 0);
        let mut parser = H264Parser::new();
        feed_sps_pps(&mut parser, &sps);

        let cp = parser.codec_private().expect("avcC must be present");

        // Walk to the end of the fixed record to locate the extension bytes.
        // Fixed header: 6 bytes. SPS length field: 2 bytes. SPS body. numPPS: 1.
        // PPS length: 2. PPS body (0x68, 0xCE, 0x01 = 3 bytes).
        // Fixed tail offset = 6 + 2 + sps.len() + 1 + 2 + 3 = sps.len() + 14.
        let ext_off = sps.len() + 14;
        assert!(
            cp.len() == ext_off + 4,
            "High-Profile avcC must have exactly 4 extension bytes (len={}, expected {})",
            cp.len(),
            ext_off + 4
        );

        // Byte 0: 111111xx — upper 6 bits reserved (0b111111), lower 2 = chroma_format_idc=1.
        assert_eq!(
            cp[ext_off] & 0xFC,
            0xFC,
            "extension byte 0: reserved bits must be 111111xx"
        );
        assert_eq!(cp[ext_off] & 0x03, 1, "chroma_format_idc must be 1 (4:2:0)");
        // Byte 1: 11111xxx — upper 5 bits reserved, lower 3 = bit_depth_luma_minus8=0.
        assert_eq!(
            cp[ext_off + 1] & 0xF8,
            0xF8,
            "extension byte 1: reserved bits must be 11111xxx"
        );
        assert_eq!(cp[ext_off + 1] & 0x07, 0, "bit_depth_luma_minus8 must be 0");
        // Byte 2: 11111xxx — upper 5 bits reserved, lower 3 = bit_depth_chroma_minus8=0.
        assert_eq!(
            cp[ext_off + 2] & 0xF8,
            0xF8,
            "extension byte 2: reserved bits must be 11111xxx"
        );
        assert_eq!(
            cp[ext_off + 2] & 0x07,
            0,
            "bit_depth_chroma_minus8 must be 0"
        );
        // Byte 3: num_of_sequence_parameter_set_ext = 0.
        assert_eq!(
            cp[ext_off + 3],
            0,
            "num_of_sequence_parameter_set_ext must be 0"
        );
    }

    /// ISO 14496-15 §5.3.3.1.2 regression: a High-Profile SPS with non-zero
    /// chroma_format_idc and bit depths carries those values correctly in the
    /// extension bytes.
    #[test]
    fn avcc_high_profile_extension_carries_correct_values() {
        // profile_idc=100, chroma_format_idc=3 (4:4:4), depth_luma=2, depth_chroma=2.
        let sps = build_high_profile_sps(100, 3, 2, 2);
        let mut parser = H264Parser::new();
        feed_sps_pps(&mut parser, &sps);

        let cp = parser.codec_private().expect("avcC");
        let ext_off = sps.len() + 14;

        assert_eq!(cp[ext_off] & 0x03, 3, "chroma_format_idc must be 3 (4:4:4)");
        assert_eq!(cp[ext_off + 1] & 0x07, 2, "bit_depth_luma_minus8 must be 2");
        assert_eq!(
            cp[ext_off + 2] & 0x07,
            2,
            "bit_depth_chroma_minus8 must be 2"
        );
        assert_eq!(
            cp[ext_off + 3],
            0,
            "num_of_sequence_parameter_set_ext must be 0"
        );
    }

    /// ISO 14496-15 §5.3.3.1.2 regression: profile_idc=244 (High 4:4:4
    /// Predictive) ALSO mandates the chroma/bit-depth extension. It was missing
    /// from HIGH_PROFILES, so a 244 stream took the Baseline/Main path and
    /// emitted an avcC with NO extension bytes — non-conforming, and strict
    /// parsers then assume 8-bit 4:2:0. The extension must be appended.
    #[test]
    fn avcc_profile_244_appends_extension_bytes() {
        // profile_idc=244, chroma_format_idc=3 (4:4:4), depths both 4 (12-bit).
        let sps = build_high_profile_sps(244, 3, 4, 4);
        let mut parser = H264Parser::new();
        feed_sps_pps(&mut parser, &sps);

        let cp = parser.codec_private().expect("avcC must be present");
        let ext_off = sps.len() + 14;
        assert_eq!(
            cp.len(),
            ext_off + 4,
            "profile 244 avcC must have the 4 extension bytes (len={}, expected {})",
            cp.len(),
            ext_off + 4
        );
        assert_eq!(cp[ext_off] & 0x03, 3, "chroma_format_idc must be 3 (4:4:4)");
        assert_eq!(cp[ext_off + 1] & 0x07, 4, "bit_depth_luma_minus8 must be 4");
        assert_eq!(
            cp[ext_off + 2] & 0x07,
            4,
            "bit_depth_chroma_minus8 must be 4"
        );
    }

    /// ISO 14496-15 §5.3.3.1.2 regression: a Main-Profile SPS (profile_idc=77)
    /// must NOT have the extension bytes — strict parsers reject trailing bytes
    /// for Baseline/Main/Extended profiles.
    #[test]
    fn avcc_main_profile_no_extension_bytes() {
        // profile_idc=77 (Main). No High-Profile branch in the SPS RBSP,
        // so we build a simpler SPS: NAL header + profile/compat/level + a
        // ue(v) seq_parameter_set_id=0 + remaining RBSP (can be trivial).
        let sps = vec![
            0x67, // NAL header (type=7)
            77,   // profile_idc = Main
            0x40, // constraint flags
            0x28, // level_idc
            // seq_parameter_set_id=0 → ue(v) = 0b1 (1 bit).  Pack into a byte:
            // bit pattern: 1000_0000 (stop bit in MSB, rest don't-care)
            0x80,
        ];
        let mut parser = H264Parser::new();
        feed_sps_pps(&mut parser, &sps);

        let cp = parser.codec_private().expect("avcC must be present");
        // Fixed record: 6 + 2 + sps.len() + 1 + 2 + 3 = sps.len() + 14.
        let expected_len = sps.len() + 14;
        assert_eq!(
            cp.len(),
            expected_len,
            "Main-Profile avcC must NOT have extension bytes (len={}, expected {})",
            cp.len(),
            expected_len
        );
    }
}
