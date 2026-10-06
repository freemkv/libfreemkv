//! H.264 (AVC) elementary stream parser.
//!
//! Extracts SPS and PPS NAL units for MKV codecPrivate.
//! Detects keyframes: IDR slices, and open-GOP intra-coded access units (the
//! non-IDR recovery points BD titles use — see the intra promotion in `parse`).
//! Each PES packet = one access unit = one frame.

use super::coding::{CodingType, PictureInfo};
use super::decodable::{Decodable, Need};
use super::startcode::{BitReader, find_start_code, skip_start_code};
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// H.264 NAL unit types we care about.
const NAL_SLICE_NON_IDR: u8 = 1;
const NAL_SLICE_IDR: u8 = 5;
const NAL_SPS: u8 = 7;
const NAL_PPS: u8 = 8;
const NAL_AUD: u8 = 9;

// Map an H.264 slice_type (§7.4.3, Table 7-6) to a coding type via slice_type % 5 (values 5..=9
// repeat 0..=4).
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
    // First-seen SPS/PPS seed the MKV codecPrivate (avcC). A stream may redefine
    // a set mid-title under the same id with a different body; that occurrence
    // must be emitted in-band, or it decodes against the stale avcC copy.
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    // Currently-active body of each type, distinct from the fixed `sps`/`pps`
    // codecPrivate copy. Must be re-asserted in-band at every keyframe that
    // doesn't carry it, or a decoder reverts to the stale avcC copy.
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
    /// Which pictures decode from the pictures this output holds (not the MVC dependent
    /// view, whose access units follow their base picture's fate).
    decodable: Decodable,
    /// What each frame held by `reorder` needs, in decode order.
    needs: std::collections::VecDeque<Need>,
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
            decodable: Decodable::new(false),
            needs: std::collections::VecDeque::new(),
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

    // Enable MVC dependent-view passthrough (see `mvc_passthrough` field): keep
    // every param set in-band so each frame is self-contained. Blu-ray 3D only.
    pub(crate) fn with_mvc_passthrough(mut self, enabled: bool) -> Self {
        self.mvc_passthrough = enabled;
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

// Bytes reserved at the front of every assembled access unit so the keyframe
// param-set re-assert (SPS+PPS) can splice in without reallocating. Oversized
// sets just reallocate once — correctness is unaffected. Mirrors HEVC parser.
const PARAM_REASSERT_HEADROOM: usize = 1024;

// Per-thread count of keyframe re-asserts that had to reallocate. Test-only:
// proves the splice stays in-place rather than reasoning about it. See
// `keyframe_param_reassert_does_not_reallocate_the_frame`. Mirrors HEVC.
#[cfg(test)]
thread_local! {
    static PARAM_REASSERT_REALLOCS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// Copy the leading bytes of an EBSP with emulation-prevention bytes removed.
fn unescape_ebsp_prefix(ebsp: &[u8]) -> Vec<u8> {
    const PREFIX_OCTETS: usize = 16;
    unescape_ebsp(ebsp, PREFIX_OCTETS)
}

// Copy `ebsp` with emulation-prevention bytes removed (cumulative zero-run rule, ITU-T H.264
// §7.3.1), stopping after `max_octets` output bytes.
fn unescape_ebsp(ebsp: &[u8], max_octets: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(max_octets.min(ebsp.len()));
    let mut zeros = 0usize;
    for &b in ebsp {
        if out.len() == max_octets {
            break;
        }
        // Drop the escape octet itself, but only in the 00 00 03 position.
        if zeros >= 2 && b == 0x03 {
            zeros = 0;
            continue;
        }
        zeros = if b == 0 { zeros + 1 } else { 0 };
        out.push(b);
    }
    out
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

// Handle an SPS/PPS NAL: strip/emit decision is against the ACTIVE set `cur`, not codecPrivate
// `first`, so a switch back to the first-seen body is still told to the decoder. Returns true
// when emitted in-band.
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

// Append the active parameter set `cur` to `prefix` (length-prefixed) so every keyframe is
// self-contained. Unconditional (not only on change) so a decoder that silently dropped a param
// set is self-healing.
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

        // MKV block timecodes are presentation timestamps (decode order stored,
        // player reorders). Use PTS not DTS: DTS presents B-frames in decode
        // order (visible judder) and breaks PTS-based seeking; fall back only if absent.
        let explicit_pts = pes.pts.or(pes.dts).map(pts_to_ns);
        let dts = pes.pts.and(pes.dts).map(pts_to_ns);
        let pts_ns = explicit_pts.unwrap_or(0);

        // Single pass: detect IDR keyframes, seed/strip param sets, and convert
        // Annex B (start-code prefixed) NALUs to length-prefixed NALUs (MKV with
        // AVCDecoderConfigurationRecord expects a 4-byte length prefix per NAL).
        let mut keyframe = false;
        // Picture coding type, MEASURED from the first coded slice's header.
        let mut coding_type: Option<CodingType> = None;
        // Open-GOP promotion needs every slice of the FIRST picture intra. A later slice with
        // first_mb_in_slice == 0 starts the next picture (the P second field of a 1080i
        // anchor), which doesn't veto it. `saw_vcl` guards a param-set-only access unit.
        let mut all_vcl_intra = true;
        let mut saw_vcl = false;
        let mut first_pic_done = false;
        // In-band redefinitions, held aside so they lead the AU in SPS, PPS order:
        // a PPS is parsed against the SPS before it, so a re-asserted PPS must
        // never precede a redefined SPS.
        let mut inband_sps = Vec::new();
        let mut inband_pps = Vec::new();
        // Pre-sized to input length plus PARAM_REASSERT_HEADROOM so the mux hot
        // path avoids repeated reallocs and the keyframe param-set re-assert
        // below can be spliced in front without reallocating (see that site).
        let mut frame_data = Vec::with_capacity(pes.data.len() + 64 + PARAM_REASSERT_HEADROOM);

        // MVC dependent-view passthrough: keep ALL param sets in-band (the frame
        // is a self-contained BlockAdditional access unit), never strip/re-assert.
        let mvc = self.mvc_passthrough;

        for nal in NalIterator::new(&pes.data) {
            let nal_type = nal[0] & 0x1F;

            match nal_type {
                // Param sets: seed avcC, strip if unchanged vs active set, emit
                // in-band on any change. In MVC passthrough these fall through to
                // the default arm so subset SPS/PPS stay in-band (self-contained AU).
                NAL_SPS if !mvc => {
                    handle_param_set(&mut self.sps, &mut self.cur_sps, nal, &mut inband_sps);
                }
                NAL_PPS if !mvc => {
                    handle_param_set(&mut self.pps, &mut self.cur_pps, nal, &mut inband_pps);
                }
                // Access unit delimiters: drop. Matroska H.264 frame data omits
                // AUDs (the container delimits access units), so keeping them
                // in-band is redundant. Mirrors the HEVC parser.
                NAL_AUD => {}
                _ => {
                    if nal_type == NAL_SLICE_IDR {
                        keyframe = true;
                    }
                    // Slice header (§7.3.3: first_mb_in_slice, slice_type, both ue(v)), read
                    // after unescaping EBSP (§7.3.1). The first slice sets `coding_type`; each
                    // slice of the first picture feeds `all_vcl_intra`.
                    let is_slice = nal_type == NAL_SLICE_NON_IDR || nal_type == NAL_SLICE_IDR;
                    // Once the first picture is over (or already non-intra with its coding
                    // type known), no later slice can change the outcome: skip the parse.
                    let settled = first_pic_done || (coding_type.is_some() && !all_vcl_intra);
                    if is_slice && !settled {
                        let header = unescape_ebsp_prefix(&nal[1..]);
                        let mut br = BitReader::new(&header);
                        match (br.read_ue(), br.read_ue()) {
                            (Some(0), _) if saw_vcl => first_pic_done = true,
                            (Some(_first_mb), Some(slice_type)) => {
                                let ct = h264_slice_coding_type(slice_type);
                                if coding_type.is_none() {
                                    coding_type = ct;
                                }
                                if ct != Some(CodingType::I) {
                                    all_vcl_intra = false;
                                }
                            }
                            // An unparseable slice header cannot be proven intra.
                            _ => all_vcl_intra = false,
                        }
                    }
                    saw_vcl |= is_slice;
                    push_length_prefixed(&mut frame_data, nal);
                }
            }
        }

        if frame_data.is_empty() && inband_sps.is_empty() && inband_pps.is_empty() {
            return Vec::new();
        }

        // Open-GOP anchor heuristic (recovery_point SEI §D.2.8 is authoritative, not read):
        // promote when every slice of the first picture is intra, a following P field
        // allowed, base view only. Else the resync gate can drop frames to EOF.
        let idr = keyframe;
        if saw_vcl && all_vcl_intra && !mvc {
            keyframe = true;
        }
        // A B displayed before its random-access picture is a leading picture; an IDR has none.
        let need = match coding_type {
            _ if mvc || !saw_vcl => Need::Nothing,
            _ if keyframe => Need::Rap {
                closed: idr,
                broken: false,
            },
            Some(CodingType::B) => Need::ByPts,
            _ => Need::Anchor,
        };

        // Every keyframe is self-contained: re-assert active SPS/PPS in-band so a
        // decoder that dropped the set recovers, and a stale avcC re-apply can't
        // revert it. A type this AU redefined goes in-band instead, in its place.
        {
            let reassert = keyframe && !mvc;
            let mut prefix = Vec::with_capacity(PARAM_REASSERT_HEADROOM);
            for (inband, cur) in [(&inband_sps, &self.cur_sps), (&inband_pps, &self.cur_pps)] {
                if !inband.is_empty() {
                    prefix.extend_from_slice(inband);
                } else if reassert {
                    reassert_active(&mut prefix, cur, false);
                }
            }
            if !prefix.is_empty() {
                // Splice prefix into frame_data in place, sized via
                // PARAM_REASSERT_HEADROOM to avoid the extra whole-frame alloc+copy
                // per keyframe this used to cost. Mirrors the HEVC parser's fix.
                #[cfg(test)]
                let cap_before = frame_data.capacity();
                frame_data.splice(0..0, prefix);
                #[cfg(test)]
                if frame_data.capacity() != cap_before {
                    PARAM_REASSERT_REALLOCS.with(|c| c.set(c.get() + 1));
                }
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
        // Build AVCDecoderConfigurationRecord from SPS + PPS
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        if sps.len() < 4 {
            return None;
        }

        // avcC encodes each NAL's length in a 16-bit field; a param set over
        // 65535 bytes would truncate the length while full bytes are appended,
        // mis-framing the record. Refuse rather than emit a corrupt avcC.
        if sps.len() > 0xFFFF || pps.len() > 0xFFFF {
            return None;
        }

        // AVCDecoderConfigurationRecord (ISO 14496-15): fields built below are
        // configurationVersion, profile/compatibility/level from SPS[1..4],
        // lengthSizeMinusOne=3, then one SPS and one PPS, length-prefixed.

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

        // ISO 14496-15 §5.3.3.1.2: High-Profile and related chroma/bit-depth
        // profiles get 4 trailing extension bytes (chroma_format_idc, luma/chroma
        // bit depths). Do NOT append for Baseline/Main/Extended: strict parsers reject them.
        let profile_idc = sps[1];
        const HIGH_PROFILES: [u8; 14] = [
            100, 110, 122, 144, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135,
        ];
        if HIGH_PROFILES.contains(&profile_idc)
            && let Some((chroma_fmt, depth_luma, depth_chroma)) = parse_sps_high_profile_ext(sps)
        {
            // byte 0: 111111xx — reserved(6) + chroma_format_idc(2)
            record.push(0xFC | (chroma_fmt & 0x03));
            // byte 1: 11111xxx — reserved(5) + bit_depth_luma_minus8(3)
            record.push(0xF8 | (depth_luma & 0x07));
            // byte 2: 11111xxx — reserved(5) + bit_depth_chroma_minus8(3)
            record.push(0xF8 | (depth_chroma & 0x07));
            // byte 3: num_of_sequence_parameter_set_ext (0 = none)
            record.push(0x00);
        }

        Some(record)
    }
}

// Parse (chroma_format_idc, bit_depth_luma_minus8, bit_depth_chroma_minus8) from a High-Profile
// SPS NAL (ITU-T H.264 §7.3.2.1.1). Returns None if the SPS is too short/malformed.
fn parse_sps_high_profile_ext(sps: &[u8]) -> Option<(u8, u8, u8)> {
    // Strip emulation-prevention bytes (00 00 03 xx -> 00 00 xx), skipping the
    // NAL header. Shares `unescape_ebsp` with the slice-header prefix reader —
    // see its doc comment for why a second, re-derived copy used to disagree.
    let raw = &sps[1..]; // skip NAL header byte
    let rbsp: Vec<u8> = unescape_ebsp(raw, raw.len());

    // RBSP layout after stripping the NAL header byte: [0] profile_idc (already
    // checked by caller), [1] constraint flags, [2] level_idc, [3..]
    // seq_parameter_set_id ue(v) then High-Profile fields.
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

    // Clamp to the 2- and 3-bit avcC extension fields. Valid H.264 values are
    // 0..=6 so no real content is truncated; out-of-spec values are clamped
    // rather than rejected so a corrupt-but-decodable SPS still yields an avcC.
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
    /// Number of bits remaining in `data[byte]` (0 means fully consumed, advance).
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

impl SpsReader<'_> {
    /// Read one signed Exp-Golomb integer se(v) (H.264 §9.1.1: code_num k maps to
    /// `(−1)^(k+1)·Ceil(k÷2)`). `None` on end-of-data or a malformed code.
    fn read_se(&mut self) -> Option<i64> {
        let k = self.read_ue()? as i64;
        Some(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
    }
}

/// SPS fields the sink-side DTS deriver ([`crate::mux::decode_ts`]) needs: the
/// reorder depth R, the slice-header layout, and the VUI field period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpsDtsInfo {
    /// Reorder depth R, per the design's §2.3 fallback chain (see [`parse_sps_dts_info`]).
    pub reorder: u32,
    /// `log2_max_frame_num_minus4 + 4`: the `frame_num` width in a slice header.
    pub log2_max_frame_num: u32,
    /// `frame_mbs_only_flag`: when 0, slice headers carry `field_pic_flag`.
    pub frame_mbs_only: bool,
    /// `separate_colour_plane_flag`: when 1, slice headers carry `colour_plane_id`.
    pub separate_colour_plane: bool,
    /// The nominal frame period, two VUI clock ticks (`2·num_units_in_tick ÷
    /// time_scale`), in 90 kHz ticks when `timing_info_present_flag` is set.
    pub frame_period_ticks: Option<i64>,
}

// Profiles whose SPS carries chroma_format_idc, bit depths and scaling lists (H.264 §7.3.2.1.1).
const SPS_CHROMA_PROFILES: [u32; 14] = [
    100, 110, 122, 144, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135,
];

// Intra profiles when constraint_set3_flag = 1: no reordering (design §2.3 step 2, H.264 E.2.1 [I]).
const INTRA_PROFILES: [u32; 6] = [44, 86, 100, 110, 122, 244];

// Upper bound of H.264 MaxDpbFrames, and the inference for an unknown level (design §2.3).
const MAX_DPB_FRAMES: u32 = 16;

/// Parse the DTS-relevant fields of an H.264 SPS NAL (1-byte header included):
/// emulation prevention removed, then SPS → VUI → `hrd_parameters` (NAL and VCL)
/// → `bitstream_restriction` (H.264 §7.3.2.1.1, E.1.1, E.1.2 \[I\]). R is, in order:
/// `max_num_reorder_frames`; 0 for the intra profiles with `constraint_set3_flag`;
/// 0 for `pic_order_cnt_type = 2`; else MaxDpbFrames inferred from the level
/// (E.2.1 \[I\]). `None` when the SPS is cut short before the VUI flag.
pub(crate) fn parse_sps_dts_info(sps_nal: &[u8]) -> Option<SpsDtsInfo> {
    let raw = sps_nal.get(1..)?;
    let rbsp = unescape_ebsp(raw, raw.len());
    let mut r = SpsReader::new(&rbsp);
    let profile_idc = r.read_bits(8)?;
    let constraint_flags = r.read_bits(8)?;
    let level_idc = r.read_bits(8)?;
    // constraint_set3_flag is the 4th flag bit (constraint_set0 is the MSB).
    let constraint_set3 = constraint_flags & 0x10 != 0;
    r.read_ue()?; // seq_parameter_set_id
    let mut separate_colour_plane = false;
    if SPS_CHROMA_PROFILES.contains(&profile_idc) {
        let chroma_format_idc = r.read_ue()?;
        if chroma_format_idc == 3 {
            separate_colour_plane = r.read_bit()? == 1;
        }
        r.read_ue()?; // bit_depth_luma_minus8
        r.read_ue()?; // bit_depth_chroma_minus8
        r.read_bit()?; // qpprime_y_zero_transform_bypass_flag
        if r.read_bit()? == 1 {
            // seq_scaling_matrix_present_flag: 8 lists, or 12 for 4:4:4.
            let lists = if chroma_format_idc != 3 { 8 } else { 12 };
            for i in 0..lists {
                if r.read_bit()? == 1 {
                    skip_scaling_list(&mut r, if i < 6 { 16 } else { 64 })?;
                }
            }
        }
    }
    let log2_max_frame_num = r.read_ue()?.saturating_add(4);
    if log2_max_frame_num > 16 {
        return None; // log2_max_frame_num_minus4 is 0..=12
    }
    let pic_order_cnt_type = r.read_ue()?;
    match pic_order_cnt_type {
        0 => {
            r.read_ue()?; // log2_max_pic_order_cnt_lsb_minus4
        }
        1 => {
            r.read_bit()?; // delta_pic_order_always_zero_flag
            r.read_se()?; // offset_for_non_ref_pic
            r.read_se()?; // offset_for_top_to_bottom_field
            let cycle = r.read_ue()?; // num_ref_frames_in_pic_order_cnt_cycle, 0..=255
            if cycle > 255 {
                return None;
            }
            for _ in 0..cycle {
                r.read_se()?; // offset_for_ref_frame[i]
            }
        }
        _ => {}
    }
    r.read_ue()?; // max_num_ref_frames
    r.read_bit()?; // gaps_in_frame_num_value_allowed_flag
    let width_mbs = r.read_ue()?.saturating_add(1);
    let height_map_units = r.read_ue()?.saturating_add(1);
    let frame_mbs_only = r.read_bit()? == 1;
    if !frame_mbs_only {
        r.read_bit()?; // mb_adaptive_frame_field_flag
    }
    r.read_bit()?; // direct_8x8_inference_flag
    if r.read_bit()? == 1 {
        // frame_cropping_flag: four ue(v) offsets.
        for _ in 0..4 {
            r.read_ue()?;
        }
    }
    let vui_present = r.read_bit()? == 1;
    // A cut-short VUI keeps what it reached; a missing reorder count falls back
    // to the level inference, which is never below max_num_reorder_frames.
    let mut vui = Vui::default();
    if vui_present {
        let _ = parse_vui(&mut r, &mut vui);
    }
    let frame_height_mbs = (2 - frame_mbs_only as u32).saturating_mul(height_map_units);
    let reorder = if let Some(n) = vui.max_num_reorder_frames {
        n
    } else if constraint_set3 && INTRA_PROFILES.contains(&profile_idc) {
        0
    } else if pic_order_cnt_type == 2 {
        // POC type 2: output order is decode order.
        0
    } else {
        max_dpb_frames(
            profile_idc,
            constraint_set3,
            level_idc,
            width_mbs,
            frame_height_mbs,
        )
    };
    Some(SpsDtsInfo {
        reorder,
        log2_max_frame_num,
        frame_mbs_only,
        separate_colour_plane,
        frame_period_ticks: vui.frame_period_ticks,
    })
}

/// VUI values [`parse_sps_dts_info`] keeps.
#[derive(Default)]
struct Vui {
    frame_period_ticks: Option<i64>,
    max_num_reorder_frames: Option<u32>,
}

// scaling_list(sizeOfScalingList) (H.264 §7.3.2.1.1.1): only delta_scale se(v) is coded.
fn skip_scaling_list(r: &mut SpsReader, size: usize) -> Option<()> {
    let mut last_scale: i64 = 8;
    let mut next_scale: i64 = 8;
    for _ in 0..size {
        if next_scale != 0 {
            let delta_scale = r.read_se()?;
            next_scale = (last_scale + delta_scale + 256).rem_euclid(256);
        }
        if next_scale != 0 {
            last_scale = next_scale;
        }
    }
    Some(())
}

// vui_parameters() (H.264 E.1.1 [I]), filling `out` as fields are reached.
fn parse_vui(r: &mut SpsReader, out: &mut Vui) -> Option<()> {
    if r.read_bit()? == 1 {
        // aspect_ratio_info_present_flag: aspect_ratio_idc u(8); 255 = Extended_SAR.
        if r.read_bits(8)? == 255 {
            r.read_bits(16)?; // sar_width
            r.read_bits(16)?; // sar_height
        }
    }
    if r.read_bit()? == 1 {
        r.read_bit()?; // overscan_info_present_flag → overscan_appropriate_flag
    }
    if r.read_bit()? == 1 {
        // video_signal_type_present_flag: video_format u(3), video_full_range_flag u(1).
        r.read_bits(4)?;
        if r.read_bit()? == 1 {
            r.read_bits(24)?; // colour_primaries, transfer_characteristics, matrix_coefficients
        }
    }
    if r.read_bit()? == 1 {
        r.read_ue()?; // chroma_sample_loc_type_top_field
        r.read_ue()?; // chroma_sample_loc_type_bottom_field
    }
    if r.read_bit()? == 1 {
        // timing_info_present_flag
        let num_units_in_tick = r.read_bits(32)? as i64;
        let time_scale = r.read_bits(32)? as i64;
        r.read_bit()?; // fixed_frame_rate_flag
        if num_units_in_tick > 0 && time_scale > 0 {
            out.frame_period_ticks =
                Some((2 * num_units_in_tick * 90_000 + time_scale / 2) / time_scale);
        }
    }
    let nal_hrd = r.read_bit()? == 1;
    if nal_hrd {
        skip_hrd_parameters(r)?;
    }
    let vcl_hrd = r.read_bit()? == 1;
    if vcl_hrd {
        skip_hrd_parameters(r)?;
    }
    if nal_hrd || vcl_hrd {
        r.read_bit()?; // low_delay_hrd_flag
    }
    r.read_bit()?; // pic_struct_present_flag
    if r.read_bit()? == 1 {
        // bitstream_restriction_flag
        r.read_bit()?; // motion_vectors_over_pic_boundaries_flag
        r.read_ue()?; // max_bytes_per_pic_denom
        r.read_ue()?; // max_bits_per_mb_denom
        r.read_ue()?; // log2_max_mv_length_horizontal
        r.read_ue()?; // log2_max_mv_length_vertical
        let max_num_reorder_frames = r.read_ue()?;
        r.read_ue()?; // max_dec_frame_buffering
        out.max_num_reorder_frames = Some(max_num_reorder_frames);
    }
    Some(())
}

// hrd_parameters() (H.264 E.1.2 [I]), skipped field by field.
fn skip_hrd_parameters(r: &mut SpsReader) -> Option<()> {
    let cpb_cnt_minus1 = r.read_ue()?;
    if cpb_cnt_minus1 > 31 {
        return None; // cpb_cnt_minus1 is 0..=31
    }
    r.read_bits(8)?; // bit_rate_scale u(4), cpb_size_scale u(4)
    for _ in 0..=cpb_cnt_minus1 {
        r.read_ue()?; // bit_rate_value_minus1
        r.read_ue()?; // cpb_size_value_minus1
        r.read_bit()?; // cbr_flag
    }
    // initial_cpb_removal_delay_length_minus1, cpb_removal_delay_length_minus1,
    // dpb_output_delay_length_minus1, time_offset_length: u(5) each.
    r.read_bits(20)?;
    Some(())
}

// MaxDpbFrames = Min(MaxDpbMbs / (PicWidthInMbs · FrameHeightInMbs), 16) (H.264 A.3.1,
// Table A-1 [I]). Level 1b is level_idc 11 + constraint_set3 on Baseline/Main/Extended,
// or level_idc 9. An unknown level infers 16.
fn max_dpb_frames(profile_idc: u32, set3: bool, level_idc: u32, w_mbs: u32, h_mbs: u32) -> u32 {
    let level_1b =
        level_idc == 9 || (level_idc == 11 && set3 && matches!(profile_idc, 66 | 77 | 88));
    let max_dpb_mbs: u32 = match level_idc {
        _ if level_1b => 396,
        10 => 396,
        11 => 900,
        12 | 13 | 20 => 2376,
        21 => 4752,
        22 | 30 => 8100,
        31 => 18000,
        32 => 20480,
        40 | 41 => 32768,
        42 => 34816,
        50 => 110400,
        51 | 52 => 184320,
        60..=62 => 696320,
        _ => return MAX_DPB_FRAMES,
    };
    let frame_mbs = w_mbs.saturating_mul(h_mbs);
    if frame_mbs == 0 {
        return MAX_DPB_FRAMES;
    }
    (max_dpb_mbs / frame_mbs).min(MAX_DPB_FRAMES)
}

/// First-slice-header fields that decide field pairing (H.264 §7.3.3 \[I\]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SliceFieldInfo {
    /// `first_mb_in_slice`: 0 starts a new picture.
    pub first_mb: u32,
    /// `frame_num`, `log2_max_frame_num` bits wide.
    pub frame_num: u32,
    /// `Some(bottom_field_flag)` for a field picture, `None` for a frame.
    pub field: Option<bool>,
}

/// Read `first_mb_in_slice`, `frame_num` and `field_pic_flag`/`bottom_field_flag`
/// from a coded slice NAL (1-byte header included), after emulation prevention is
/// removed. `sps` supplies the header layout; `None` when the header is cut short.
pub(crate) fn parse_slice_field_info(nal: &[u8], sps: &SpsDtsInfo) -> Option<SliceFieldInfo> {
    // Everything read here fits well inside 32 RBSP octets.
    const HEADER_OCTETS: usize = 32;
    let raw = nal.get(1..)?;
    let hdr = unescape_ebsp(raw, HEADER_OCTETS);
    let mut r = SpsReader::new(&hdr);
    let first_mb = r.read_ue()?;
    r.read_ue()?; // slice_type
    r.read_ue()?; // pic_parameter_set_id
    if sps.separate_colour_plane {
        r.read_bits(2)?; // colour_plane_id
    }
    let frame_num = r.read_bits(sps.log2_max_frame_num as u8)?;
    let field = if !sps.frame_mbs_only && r.read_bit()? == 1 {
        Some(r.read_bit()? == 1) // bottom_field_flag
    } else {
        None
    };
    Some(SliceFieldInfo {
        first_mb,
        frame_num,
        field,
    })
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
        // Loop (not tail-recursion): a garbled Annex B stream with many adjacent
        // start codes yields empty NALs back-to-back, and recursing once per one
        // would overflow the stack. Mirrors the HEVC parser's while-scan.
        loop {
            if self.pos >= self.data.len() {
                return None;
            }

            // Skip the start code at current position
            let nal_start = skip_start_code(self.data, self.pos)?;

            // Find next start code (or end of data)
            let nal_end = find_start_code(self.data, nal_start).unwrap_or(self.data.len());

            // Strip leading zeros of the following start code: lossless since
            // rbsp_trailing_bits() sets a stop-one bit, so an RBSP's final byte
            // is never 0x00 — these zeros belong to the next prefix. Mirrors HEVC.
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
#[path = "h264_tests.rs"]
mod tests;
