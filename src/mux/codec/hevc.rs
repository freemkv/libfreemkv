//! HEVC (H.265) elementary stream parser.
//!
//! Extracts VPS, SPS, PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IRAP pictures: IDR, CRA, BLA).
//! Each PES packet = one access unit = one frame.

use super::coding::{CodingType, PictureInfo};
use super::decodable::{Decodable, Need};
use super::startcode::{BitReader, find_start_code, skip_start_code};
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

// HEVC NAL unit types
const NAL_VPS: u8 = 32;
const NAL_SPS: u8 = 33;
const NAL_PPS: u8 = 34;
const NAL_AUD: u8 = 35;
// SEI (H.265 Table 7-1): prefix (39) precedes its picture, suffix (40) follows.
// HDR10 static metadata is carried in prefix SEI on UHD streams; both types are
// scanned for the HDR10 payloads below but pass through to frame data unchanged.
const NAL_SEI_PREFIX: u8 = 39;
const NAL_SEI_SUFFIX: u8 = 40;

// HEVC SEI payload types (H.265 Annex D.2) carrying HDR10 static metadata:
// Mastering Display Colour Volume (D.2.28) = 137, Content Light Level Info (D.2.35) = 144.
const SEI_MASTERING_DISPLAY_COLOUR_VOLUME: u32 = 137;
const SEI_CONTENT_LIGHT_LEVEL_INFO: u32 = 144;
// Dolby Vision RPU NALs (type 62) pass through: only VPS/SPS/PPS/AUD are filtered.
// IRAP types (keyframes): BLA, IDR, CRA
const NAL_BLA_W_LP: u8 = 16;
const NAL_BLA_N_LP: u8 = 18;
const NAL_IDR_W_RADL: u8 = 19;
const NAL_IDR_N_LP: u8 = 20;
// Leading pictures (H.265 Table 7-1): RADL decode from their IRAP alone, RASL also
// reference pictures before it.
const NAL_RADL_N: u8 = 6;
const NAL_RADL_R: u8 = 7;
const NAL_RASL_N: u8 = 8;
const NAL_RASL_R: u8 = 9;
const NAL_RSV_IRAP_VCL23: u8 = 23;
// CRA_NUT (Clean Random Access). A CRA at a splice carries RASL pictures
// referencing frames before the splice, gone after concatenation ("Could not
// find ref with POC N"). Remedy: rewrite to BLA so NoRaslOutput discards them cleanly.
const NAL_CRA_NUT: u8 = 21;
/// Highest VCL (coded-slice) NAL type. Rec. ITU-T H.265 Table 7-1: types 0..=31
/// are VCL, 32..=63 non-VCL. A coded slice carries a `slice_type`.
const NAL_VCL_MAX: u8 = 31;

// `num_extra_slice_header_bits` from a HEVC PPS NAL (H.265 §7.3.2.3); `None` if the PPS is too
// short to parse.
fn hevc_num_extra_slice_header_bits(pps_nal: &[u8]) -> Option<u32> {
    let mut br = BitReader::new(pps_nal.get(2..)?);
    br.read_ue()?; // pps_pic_parameter_set_id
    br.read_ue()?; // pps_seq_parameter_set_id
    br.skip_bits(2)?; // dependent_slice_segments_enabled_flag, output_flag_present_flag
    let n = br.read_bits(3)?;
    Some(n)
}

/// Map a HEVC `slice_type` (H.265 §7.4.7.1, Table 7-7) to a coding type:
/// 0 = B, 1 = P, 2 = I. `None` for any other value (malformed header).
fn hevc_slice_coding_type(slice_type: u32) -> Option<CodingType> {
    match slice_type {
        0 => Some(CodingType::B),
        1 => Some(CodingType::P),
        2 => Some(CodingType::I),
        _ => None,
    }
}

// Number of PPS ids a stream may use: `pps_pic_parameter_set_id` is 0..=63 (H.265 §7.4.3.3).
const HEVC_MAX_PPS_COUNT: usize = 64;

// The `pps_pic_parameter_set_id` (first ue(v)) of a PPS NAL — the key under which each PPS is
// stored so a slice's referenced PPS is resolved by its own id. `None` if the PPS is too short.
fn hevc_pps_id(pps_nal: &[u8]) -> Option<u32> {
    BitReader::new(pps_nal.get(2..)?).read_ue()
}

/// Measures the coding type from the FIRST coded slice of an access unit (H.265 §7.3.6.1).
/// `None` for a non-first slice or on truncation — never a guess. `resolve_num_extra` maps the
/// slice's OWN `slice_pic_parameter_set_id` to that PPS's `num_extra_slice_header_bits`, so the
/// `slice_type` bit offset is taken from the PPS the slice references — not merely the
/// last-active one.
fn hevc_first_slice_coding_type(
    nal: &[u8],
    nal_type: u8,
    resolve_num_extra: impl Fn(u32) -> Option<u32>,
) -> Option<CodingType> {
    let mut br = BitReader::new(nal.get(2..)?); // RBSP after the 2-byte NAL header
    if br.read_bit()? != 1 {
        return None; // not the first slice segment of the picture
    }
    if (NAL_BLA_W_LP..=NAL_RSV_IRAP_VCL23).contains(&nal_type) {
        br.skip_bits(1)?; // no_output_of_prior_pics_flag (IRAP only)
    }
    let pps_id = br.read_ue()?; // slice_pic_parameter_set_id
    // Resolve num_extra from the PPS this slice REFERENCES; a stream with
    // multiple PPS can point a slice at one whose num_extra differs, which would
    // shift the slice_type offset if the last-active PPS were assumed instead.
    let num_extra = resolve_num_extra(pps_id)?;
    // First slice → no slice_segment_address and dependent_slice_segment_flag is
    // 0, so slice_type follows the reserved bits directly.
    br.skip_bits(num_extra)?; // slice_reserved_flag[i]
    hevc_slice_coding_type(br.read_ue()?)
}

/// HEVC (H.265) Annex B → MKV codec parser: extracts VPS/SPS/PPS for the hvcC
/// codecPrivate, detects IRAP keyframes, and converts each PES access unit into
/// length-prefixed NAL units. Implements [`CodecParser`].
pub struct HevcParser {
    // First-seen param set of each type seeds the MKV codecPrivate (hvcC); a
    // player re-applies it at every keyframe. A body that differs from this
    // copy (mid-title redefinition) must be emitted in-band, or CABAC desyncs.
    vps: Option<Vec<u8>>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    // Currently-active param-set body of each type, distinct from the fixed
    // `vps/sps/pps` codecPrivate copy above. A player re-applies codecPrivate at
    // every keyframe, so a mid-title redefinition must be re-emitted in-band. See `parse`.
    cur_vps: Option<Vec<u8>>,
    cur_sps: Option<Vec<u8>>,
    cur_pps: Option<Vec<u8>>,
    /// `num_extra_slice_header_bits` of each PPS, indexed by `pps_pic_parameter_set_id`, so a
    /// slice's coding type uses the PPS it REFERENCES, not the last-active one. Fixed size:
    /// ids are 0..=63 (H.265 §7.4.3.3); an out-of-range id is ignored.
    pps_num_extra: [Option<u32>; HEVC_MAX_PPS_COUNT],
    // Splice-aware CRA→BLA rewrite for a non-seamless BD clip boundary (first
    // CRA_NUT -> BLA_W_LP so NoRaslOutput discards dangling RASL). Armed by the public
    // `mark_clip_boundary` hook (no in-tree caller); a CRA at a detected join is rewritten too.
    pending_clip_boundary: bool,
    /// Which pictures decode from the pictures this output holds; also detects a join (the
    /// PTS timeline moves at an IRAP).
    decodable: Decodable,
    /// What each frame held by `reorder` needs, in decode order.
    needs: std::collections::VecDeque<Need>,
    // HDR10 static metadata from prefix/suffix SEI: mastering display (137) and
    // content light level (144), captured independently and sticky (first wins).
    // `hdr10()` needs the mastering SEI; content light is optional (some UHD discs omit it).
    sei_mastering: Option<MasteringDisplay>,
    sei_content_light: Option<ContentLightLevel>,
    /// Display-order PTS reconstruction, enabled only on the program-stream
    /// path where the source stamps a PTS once per GOP. `None` on the BD/UHD
    /// transport path (the common HEVC case), which carries a per-frame PTS.
    reorder: Option<super::reorder::SparsePtsReorder>,
}

/// Mastering Display Colour Volume payload (Rec. ITU-T H.265 D.2.28),
/// payloadType 137. Raw SEI integer values — chromaticity in 0.00002 units,
/// luminance in 0.0001 cd/m² units. SEI primary order is c=0 G, c=1 B, c=2 R.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MasteringDisplay {
    display_primaries_x: [u16; 3],
    display_primaries_y: [u16; 3],
    white_point_x: u16,
    white_point_y: u16,
    max_display_mastering_luminance: u32,
    min_display_mastering_luminance: u32,
}

/// Content Light Level Information payload (Rec. ITU-T H.265 D.2.35),
/// payloadType 144. Both values are cd/m² integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ContentLightLevel {
    max_content_light_level: u16,
    max_pic_average_light_level: u16,
}

// Bytes reserved at the front of every assembled access unit so the keyframe
// param-set re-assert (VPS+SPS+PPS, typically well under 1 KiB on BD/UHD
// streams) can splice in without reallocating. Oversized sets just reallocate once.
const PARAM_REASSERT_HEADROOM: usize = 1024;

// Per-thread count of keyframe re-asserts that had to reallocate. Test-only:
// proves the splice stays in-place rather than reasoning about it. See
// `keyframe_param_reassert_does_not_reallocate_the_frame`.
#[cfg(test)]
thread_local! {
    static PARAM_REASSERT_REALLOCS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// Test-only: forces a framing desync just before the parse() self-check guard
// so its drop branch is exercised end-to-end (frame_data is length-prefixed by
// construction, so no real input desyncs it). See parse_drops_desynced_access_unit.
#[cfg(test)]
thread_local! {
    static FORCE_FRAMING_DESYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // Count of access units the self-check dropped; real input must never move it.
    static GUARD_DROPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Default for HevcParser {
    fn default() -> Self {
        Self::new()
    }
}

impl HevcParser {
    /// Create a fresh HEVC parser with no parameter sets captured yet.
    pub fn new() -> Self {
        Self {
            vps: None,
            sps: None,
            pps: None,
            cur_vps: None,
            cur_sps: None,
            cur_pps: None,
            pps_num_extra: [None; HEVC_MAX_PPS_COUNT],
            pending_clip_boundary: false,
            decodable: Decodable::new(true),
            needs: std::collections::VecDeque::new(),
            sei_mastering: None,
            sei_content_light: None,
            reorder: None,
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

    // Mastering-display SEI as Hdr10Metadata, with content light when that SEI was seen too.
    // `None` until the mastering SEI arrives; a missing content-light SEI is never zero-filled.
    fn hdr10(&self) -> Option<crate::mux::codec::Hdr10Metadata> {
        let m = self.sei_mastering?;
        let c = self.sei_content_light;
        Some(crate::mux::codec::Hdr10Metadata {
            display_primaries_x: m.display_primaries_x,
            display_primaries_y: m.display_primaries_y,
            white_point_x: m.white_point_x,
            white_point_y: m.white_point_y,
            max_display_mastering_luminance: m.max_display_mastering_luminance,
            min_display_mastering_luminance: m.min_display_mastering_luminance,
            max_content_light_level: c.map(|c| c.max_content_light_level),
            max_pic_average_light_level: c.map(|c| c.max_pic_average_light_level),
        })
    }

    // Scans an SEI NAL for the two HDR10 payload types, capturing each the FIRST time it
    // appears.
    fn scan_sei(&mut self, nal: &[u8]) {
        // Both HDR10 messages are sticky (first wins); once both are captured, skip the
        // walk entirely — an HDR10 UHD stream carries a prefix SEI per AU.
        if self.sei_mastering.is_some() && self.sei_content_light.is_some() {
            return;
        }
        let Some(raw) = nal.get(2..) else {
            return;
        };
        // Unescape on the fly: only a payload being captured is copied, so a stream
        // lacking one HDR10 message costs no per-AU RBSP copy.
        let mut rbsp = RbspBytes::new(raw);
        // payloadType/payloadSize use ff-extension coding. A clean end of the RBSP ends
        // the walk; a trailing 0x80 is a bogus type whose size read fails.
        while let Some(payload_type) = read_sei_ff_value(&mut rbsp) {
            let Some(payload_size) = read_sei_ff_value(&mut rbsp) else {
                break;
            };
            let payload_size = payload_size as usize;
            let wanted = match payload_type {
                SEI_MASTERING_DISPLAY_COLOUR_VOLUME => self.sei_mastering.is_none(),
                SEI_CONTENT_LIGHT_LEVEL_INFO => self.sei_content_light.is_none(),
                _ => false,
            };
            if !wanted {
                if rbsp.by_ref().take(payload_size).count() < payload_size {
                    break; // truncated payload — stop scanning
                }
                continue;
            }
            let payload: Vec<u8> = rbsp.by_ref().take(payload_size).collect();
            if payload.len() < payload_size {
                break;
            }
            if payload_type == SEI_MASTERING_DISPLAY_COLOUR_VOLUME {
                self.sei_mastering = parse_mastering_display(&payload);
            } else {
                self.sei_content_light = parse_content_light_level(&payload);
            }
        }
    }

    /// Mark that the NEXT IRAP this parser sees begins a NON-SEAMLESS BD clip
    /// join (MPLS `connection_condition` 0x01 on a PlayItem after the first). The first
    /// CRA at/after this point is rewritten CRA_NUT (21) → BLA_W_LP (16) so a linear
    /// decoder sets NoRaslOutput, and its RASL leading pictures are dropped.
    ///
    /// MUST NOT be called for connection_condition 0x05/0x06 (seamless), for the first
    /// PlayItem, or within a single-clip title.
    pub fn mark_clip_boundary(&mut self) {
        self.pending_clip_boundary = true;
    }
}

// Handles a VPS/SPS/PPS NAL: strip it (decoder already has it) or emit it in-band, tracking the
// active body. Decision MUST be against `cur`, not the codecPrivate copy `first`
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
    // Strip the seeding occurrence (decoder gets it from hvcC) and any NAL that
    // doesn't change the active set. Emit only a genuine change.
    if is_first || !changed {
        return false;
    }
    // A NAL longer than u32::MAX can't be length-prefixed in the 4-byte field;
    // skip it rather than mis-frame the output. Unreachable in practice (no
    // real access unit is >4 GiB).
    let Ok(len) = u32::try_from(nal.len()) else {
        return false;
    };
    frame_data.extend_from_slice(&len.to_be_bytes());
    frame_data.extend_from_slice(nal);
    true
}

// Appends the active parameter set `cur` to `prefix` so every keyframe is self-contained.
// Unconditional (not just on divergence from codecPrivate) so a streaming decoder self-heals.
fn reassert_active(prefix: &mut Vec<u8>, cur: &Option<Vec<u8>>, emitted: bool) {
    if emitted {
        return;
    }
    let Some(active) = cur.as_deref() else {
        return;
    };
    push_length_prefixed(prefix, active);
}

// Width of the NAL length prefix this parser writes (4-byte BE, `u32::to_be_bytes`).
// MUST equal hvcC `lengthSizeMinusOne + 1` (byte 21, low bits = 3 => 4); a mismatch
// is the framing desync a demuxer reports as "Invalid NAL unit size".
const LENGTH_PREFIX_SIZE: usize = 4;

// Appends `nal` as a 4-byte BE length prefix + body. Skipped (not
// mis-framed) if `nal.len()` overflows u32 — unreachable in practice.
fn push_length_prefixed(out: &mut Vec<u8>, nal: &[u8]) {
    let Ok(len) = u32::try_from(nal.len()) else {
        return;
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(nal);
}

/// Walk a length-prefixed NAL buffer (`[u32-BE len][body]` records) and confirm the records
/// EXACTLY tile it: every declared length fits, none is zero, and the last body ends precisely
/// at the buffer end with no trailing bytes. Our `frame_data` is length-prefixed by
/// construction, so this is a self-consistency guard — a `false` means a framing desync that a
/// downstream demuxer would report as "Invalid NAL unit size (N>M)", and such an access unit
/// must be dropped rather than emitted.
fn length_prefix_tiles(data: &[u8]) -> bool {
    let mut pos = 0usize;
    while pos < data.len() {
        // The full length field must be present.
        if pos + LENGTH_PREFIX_SIZE > data.len() {
            return false;
        }
        let len =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        // A zero-length record is a structurally invalid (empty) NALU.
        if len == 0 {
            return false;
        }
        pos += LENGTH_PREFIX_SIZE;
        // The declared body must fit within the remaining buffer.
        match pos.checked_add(len) {
            Some(end) if end <= data.len() => pos = end,
            _ => return false,
        }
    }
    // Exact tiling: the walk must land precisely on the buffer end.
    pos == data.len()
}

impl CodecParser for HevcParser {
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

        // A join (mpls connection_condition isn't plumbed through the mux pipeline): an
        // IRAP whose PTS does not continue the timeline. Its RASL reference another clip.
        let joined = self.decodable.joined(explicit_pts, dts);

        let data = &pes.data;
        let mut keyframe = false;
        // NAL type of the AU's IRAP slice, else its first coded slice: what it needs to decode.
        let mut vcl_type: Option<u8> = None;
        // Set once the first CRA of a non-seamless join is seen in this AU.
        let mut bla_au = false;
        // Picture coding type, MEASURED from the first coded slice's header.
        let mut coding_type: Option<CodingType> = None;
        // This AU's in-band redefinitions, held aside to lead the AU in VPS, SPS, PPS
        // order: a re-asserted PPS ahead of a redefined SPS is dropped by the decoder.
        let mut inband_vps = Vec::new();
        let mut inband_sps = Vec::new();
        let mut inband_pps = Vec::new();
        // Pre-sized to input length plus PARAM_REASSERT_HEADROOM: UHD frames are
        // 150-300 KB and an unsized Vec otherwise reallocs 5-7x per frame; the
        // headroom also lets the keyframe param-set re-assert splice in without reallocating.
        let mut frame_data = Vec::with_capacity(data.len() + 64 + PARAM_REASSERT_HEADROOM);

        // Single-pass NAL scan: extract params, detect keyframes, build length-prefixed output
        let mut pos = 0;
        while let Some(sc_pos) = find_start_code(data, pos) {
            if let Some(nal_start) = skip_start_code(data, sc_pos) {
                let next = find_start_code(data, nal_start).unwrap_or(data.len());
                // Strip leading zeros of the following start code: lossless
                // since rbsp_trailing_bits() sets a stop-one bit, so an RBSP's
                // final byte is never 0x00 — these zeros belong to the next prefix.
                let mut end = next;
                while end > nal_start && data[end - 1] == 0x00 {
                    end -= 1;
                }

                // Skip empty NALs: when the trailing-zero strip reduces `end`
                // back to `nal_start` (e.g. adjacent start codes, or a zero-filled
                // bad sector), emitting a 0-length NALU would be structurally invalid.
                if nal_start < data.len() && end > nal_start {
                    // HEVC NAL header: 2 bytes. Type is bits 1-6 of first byte.
                    let nal_type = (data[nal_start] >> 1) & 0x3F;

                    // Measure coding type from the first coded slice (VCL NAL
                    // 0..=31), only once the active PPS is known so the bit
                    // offset to `slice_type` is exact; else decline (`None`), never guess.
                    if nal_type <= NAL_VCL_MAX && !keyframe {
                        vcl_type.get_or_insert(nal_type);
                    }
                    if coding_type.is_none() && nal_type <= NAL_VCL_MAX {
                        // Resolve num_extra from the PPS the slice REFERENCES (by
                        // its slice_pic_parameter_set_id), not the last-active PPS.
                        let pps_num_extra = &self.pps_num_extra;
                        coding_type = hevc_first_slice_coding_type(
                            &data[nal_start..end],
                            nal_type,
                            |pps_id| *pps_num_extra.get(usize::try_from(pps_id).ok()?)?,
                        );
                    }

                    match nal_type {
                        NAL_VPS => {
                            handle_param_set(
                                &mut self.vps,
                                &mut self.cur_vps,
                                &data[nal_start..end],
                                &mut inband_vps,
                            );
                        }
                        NAL_SPS => {
                            handle_param_set(
                                &mut self.sps,
                                &mut self.cur_sps,
                                &data[nal_start..end],
                                &mut inband_sps,
                            );
                        }
                        NAL_PPS => {
                            // Store the PPS under its own id so a later slice's
                            // coding type resolves num_extra from the PPS it
                            // references, not merely the last-active one.
                            let pps = &data[nal_start..end];
                            if let Some(slot) = hevc_pps_id(pps)
                                .and_then(|id| usize::try_from(id).ok())
                                .and_then(|id| self.pps_num_extra.get_mut(id))
                            {
                                *slot = hevc_num_extra_slice_header_bits(pps);
                            }
                            handle_param_set(
                                &mut self.pps,
                                &mut self.cur_pps,
                                &data[nal_start..end],
                                &mut inband_pps,
                            );
                        }
                        // Drop Access Unit Delimiters: Matroska HEVC frame data
                        // omits AUDs (the container delimits access units), so
                        // carrying them in-band is redundant. H.264 does the same.
                        NAL_AUD => {}
                        t if (NAL_BLA_W_LP..=NAL_RSV_IRAP_VCL23).contains(&t) => {
                            keyframe = true;
                            vcl_type = Some(t);
                            // Splice-aware CRA→BLA: a CRA at a join or a marked boundary
                            // becomes BLA_W_LP so NoRaslOutput drops RASL. Any IRAP clears
                            // the mark; only CRA is rewritten.
                            if (self.pending_clip_boundary || joined) && t == NAL_CRA_NUT {
                                bla_au = true;
                            }
                            self.pending_clip_boundary = false;
                            if bla_au && t == NAL_CRA_NUT {
                                // Rewrite EVERY CRA slice of this picture (7.4.2.4.4:
                                // one type per picture). NAL type is bits 1-6 of
                                // byte 0: byte = (byte & 0x81) | (type << 1).
                                let mut rewritten = data[nal_start..end].to_vec();
                                rewritten[0] = (rewritten[0] & 0x81) | (NAL_BLA_W_LP << 1);
                                push_length_prefixed(&mut frame_data, &rewritten);
                            } else {
                                push_length_prefixed(&mut frame_data, &data[nal_start..end]);
                            }
                        }
                        NAL_SEI_PREFIX | NAL_SEI_SUFFIX => {
                            // Observe HDR10 static metadata (mastering display /
                            // content light level) but pass the SEI through
                            // unchanged — scanning is non-destructive.
                            self.scan_sei(&data[nal_start..end]);
                            push_length_prefixed(&mut frame_data, &data[nal_start..end]);
                        }
                        _ => {
                            // All other NAL types (slices, DV RPU, etc.) pass through
                            push_length_prefixed(&mut frame_data, &data[nal_start..end]);
                        }
                    }
                }
                pos = next;
            } else {
                break;
            }
        }

        if frame_data.is_empty()
            && inband_vps.is_empty()
            && inband_sps.is_empty()
            && inband_pps.is_empty()
        {
            return Vec::new();
        }

        // A player re-applies hvcC param sets at every keyframe; if the active
        // set was redefined mid-title and the source stopped repeating it, the
        // reversion desyncs CABAC. Re-assert at every keyframe; a redefined type goes instead.
        {
            let mut prefix = Vec::with_capacity(PARAM_REASSERT_HEADROOM);
            for (inband, cur) in [
                (&inband_vps, &self.cur_vps),
                (&inband_sps, &self.cur_sps),
                (&inband_pps, &self.cur_pps),
            ] {
                if !inband.is_empty() {
                    prefix.extend_from_slice(inband);
                } else if keyframe {
                    reassert_active(&mut prefix, cur, false);
                }
            }
            if !prefix.is_empty() {
                // Splice prefix into frame_data in place via PARAM_REASSERT_HEADROOM,
                // avoiding the extra whole-frame alloc+copy per keyframe the old
                // extend+reassign cost — ~7,200 keyframes, 14-28 GB/title on a 2h UHD.
                #[cfg(test)]
                let cap_before = frame_data.capacity();
                frame_data.splice(0..0, prefix);
                #[cfg(test)]
                if frame_data.capacity() != cap_before {
                    PARAM_REASSERT_REALLOCS.with(|c| c.set(c.get() + 1));
                }
            }
        }

        // Test-only seam: append a stray byte so the guard below sees a desync,
        // exercising its drop branch end-to-end (no real input can desync here).
        #[cfg(test)]
        if FORCE_FRAMING_DESYNC.with(|c| c.get()) {
            frame_data.push(0xFF);
        }

        // Defense in depth (issue #52): frame_data is length-prefixed by
        // construction, but a desynced buffer would surface as "Invalid NAL unit
        // size". Drop it (log src) rather than emit a mis-framed access unit.
        if !length_prefix_tiles(&frame_data) {
            #[cfg(test)]
            GUARD_DROPS.with(|c| c.set(c.get() + 1));
            tracing::warn!(
                target: "freemkv::mux::hevc",
                src = ?pes.source,
                frame_len = frame_data.len(),
                keyframe,
                "HEVC length-prefix self-check failed; dropping desynced access unit \
                 (would surface downstream as \"Invalid NAL unit size\")"
            );
            return Vec::new();
        }

        // RASL reference pictures before their IRAP (dropped unless the output holds them;
        // always after a BLA); RADL only their IRAP.
        let need = match vcl_type {
            None => Need::Nothing,
            Some(NAL_BLA_W_LP..=NAL_BLA_N_LP) => Need::Rap {
                closed: false,
                broken: true,
            },
            Some(NAL_IDR_W_RADL | NAL_IDR_N_LP) => Need::Rap {
                closed: true,
                broken: false,
            },
            Some(NAL_CRA_NUT..=NAL_RSV_IRAP_VCL23) => Need::Rap {
                closed: false,
                broken: bla_au,
            },
            Some(NAL_RADL_N | NAL_RADL_R) => Need::Nothing,
            Some(NAL_RASL_N | NAL_RASL_R) => Need::Leading,
            Some(_) => Need::Anchor,
        };

        // HDR10 static metadata is stamped onto every frame's PictureInfo once
        // the mastering SEI is seen (at the first IRAP), riding the deferred-muxer path (reads it
        // from the first coded picture before the track header). `None` for SDR tracks.
        let hdr10 = self.hdr10();
        let frame = Frame {
            // Coding-type only: HEVC field order (pic_struct, from a pic_timing
            // SEI) is not decoded here, so field_order() stays None — honestly
            // absent, never guessed. HDR10 metadata is attached when measured.
            coding: coding_type
                .map(PictureInfo::coding_type_only)
                .map(|p| p.with_hdr10(hdr10)),
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
        // HEVCDecoderConfigurationRecord (ISO 14496-15)
        let vps = self.vps.as_ref()?;
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        // hvcC encodes each NAL's length as a 16-bit field; a param set over
        // 65535 bytes would truncate the length while full bytes are appended,
        // mis-framing the record. Refuse rather than emit a corrupt hvcC.
        if vps.len() > 0xFFFF || sps.len() > 0xFFFF || pps.len() > 0xFFFF {
            return None;
        }

        // Build a conforming HEVCDecoderConfigurationRecord: fixed header
        // (configurationVersion, profile_tier_level fields, parallelism, parsed
        // chroma/bit depths) followed by numOfArrays length-prefixed NAL arrays.
        let mut record = Vec::new();

        // The stored SPS NAL is [2-byte NAL header][SPS RBSP]. profile_tier_level
        // fields must be read off the emulation-prevention-STRIPPED RBSP — a
        // `00 00 03` in the first ~15 bytes would shift byte indices and corrupt the PTL.
        let ptl: Vec<u8> = if sps.len() > 2 {
            strip_emulation_prevention(&sps[2..])
        } else {
            Vec::new()
        };
        let ptl_at = |i: usize| -> u8 { ptl.get(i).copied().unwrap_or(0) };
        record.push(1); // configurationVersion
        // general_profile_space + general_tier_flag + general_profile_idc
        record.push(ptl_at(1));
        // general_profile_compatibility_flags (4 bytes) — RBSP bytes 2..6
        for i in 2..6 {
            record.push(ptl_at(i));
        }
        // general_constraint_indicator_flags (6 bytes) — RBSP bytes 6..12
        for i in 6..12 {
            record.push(ptl_at(i));
        }
        // general_level_idc — RBSP byte 12
        record.push(ptl_at(12));
        // min_spatial_segmentation_idc (4 + 12 bits)
        record.extend_from_slice(&[0xF0, 0x00]);
        // parallelismType (6 + 2 bits)
        record.push(0xFC);
        // chromaFormat / bit depths — parse real values from the SPS RBSP; a
        // hardcoded 8-bit 4:2:0 is wrong for 10-bit Main 10 UHD (nearly all UHD
        // content). Fall back to it only if the SPS can't be parsed.
        let chroma = parse_sps_chroma(sps).unwrap_or_else(|| {
            tracing::warn!(
                target: "freemkv::mux::hevc",
                "HEVC SPS unparseable; hvcC declares 8-bit 4:2:0"
            );
            SpsChroma {
                chroma_format_idc: 1,
                bit_depth_luma_minus8: 0,
                bit_depth_chroma_minus8: 0,
                max_sub_layers_minus1: 0,
                temporal_id_nesting_flag: 0,
                max_num_reorder_pics: None,
                picture_period_ticks: None,
            }
        });
        // chromaFormat (6 reserved bits set + 2-bit chroma_format_idc)
        record.push(0xFC | (chroma.chroma_format_idc & 0x03));
        // bitDepthLumaMinus8 (5 reserved bits set + 3-bit value)
        record.push(0xF8 | (chroma.bit_depth_luma_minus8 & 0x07));
        // bitDepthChromaMinus8 (5 reserved bits set + 3-bit value)
        record.push(0xF8 | (chroma.bit_depth_chroma_minus8 & 0x07));
        // avgFrameRate
        record.extend_from_slice(&[0, 0]);
        // Byte 21 packs constantFrameRate=0, numTemporalLayers (u3),
        // temporalIdNested, lengthSizeMinusOne=3 (ISO 14496-15). sub_layers+1 can
        // be 8, which saturates to 7 (not wraps to 0 via &0x07) for the u(3) field.
        let num_temporal_layers = chroma.max_sub_layers_minus1.saturating_add(1).min(7) & 0x07;
        let temporal_id_nested = chroma.temporal_id_nesting_flag & 0x01;
        record.push((num_temporal_layers << 3) | (temporal_id_nested << 2) | 0x03);
        // numOfArrays
        record.push(3); // VPS, SPS, PPS

        // VPS array
        record.push(0x20 | (NAL_VPS & 0x3F)); // array_completeness (0) + NAL type
        record.extend_from_slice(&[0, 1]); // numNalus = 1
        record.push((vps.len() >> 8) as u8);
        record.push(vps.len() as u8);
        record.extend_from_slice(vps);

        // SPS array
        // array_completeness = 0: param sets are also re-asserted in-band.
        record.push(0x20 | (NAL_SPS & 0x3F));
        record.extend_from_slice(&[0, 1]);
        record.push((sps.len() >> 8) as u8);
        record.push(sps.len() as u8);
        record.extend_from_slice(sps);

        // PPS array
        record.push(0x20 | (NAL_PPS & 0x3F));
        record.extend_from_slice(&[0, 1]);
        record.push((pps.len() >> 8) as u8);
        record.push(pps.len() as u8);
        record.extend_from_slice(pps);

        Some(record)
    }
}

/// chroma_format_idc + bit depths parsed from an HEVC SPS RBSP, for the hvcC
/// fixed header. Without these the record falsely advertised 8-bit 4:2:0, wrong
/// for 10-bit Main 10 UHD (essentially all UHD content).
struct SpsChroma {
    /// chroma_format_idc: 0 mono, 1 4:2:0, 2 4:2:2, 3 4:4:4.
    chroma_format_idc: u8,
    bit_depth_luma_minus8: u8,
    bit_depth_chroma_minus8: u8,
    /// sps_max_sub_layers_minus1 (u3): numTemporalLayers = this + 1 for hvcC.
    max_sub_layers_minus1: u8,
    /// sps_temporal_id_nesting_flag (u1) for hvcC temporalIdNested.
    temporal_id_nesting_flag: u8,
    /// `sps_max_num_reorder_pics[sps_max_sub_layers_minus1]` (H.265 §7.3.2.2), the
    /// reorder depth R the sink-side DTS deriver needs. `None` when the SPS is cut
    /// short before the ordering-info loop.
    max_num_reorder_pics: Option<u32>,
    /// VUI `num_units_in_tick ÷ time_scale` (one picture period) in 90 kHz ticks, when
    /// `vui_timing_info_present_flag` is set.
    picture_period_ticks: Option<i64>,
}

/// Reorder depth R of an HEVC SPS NAL (2-byte header included):
/// `sps_max_num_reorder_pics[sps_max_sub_layers_minus1]` (H.265 §7.3.2.2 \[I\]).
#[cfg(test)]
pub(crate) fn parse_sps_reorder(sps: &[u8]) -> Option<u32> {
    parse_sps_chroma(sps)?.max_num_reorder_pics
}

/// Reorder depth R and the VUI picture period (90 kHz ticks) of an HEVC SPS NAL.
pub(crate) fn parse_sps_dts(sps: &[u8]) -> Option<(u32, Option<i64>)> {
    let c = parse_sps_chroma(sps)?;
    Some((c.max_num_reorder_pics?, c.picture_period_ticks))
}

// Per-thread count of `strip_emulation_prevention` calls: the function
// allocates+copies a whole RBSP, so this measures cost rather than reasoning
// about it. Thread-local (not atomic) since `cargo test` runs concurrently.
#[cfg(test)]
thread_local! {
    static RBSP_COPIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Strip HEVC/H.264 emulation-prevention bytes (00 00 03 → 00 00) from a NAL
/// RBSP so a bit reader sees the true coded values.
fn strip_emulation_prevention(rbsp: &[u8]) -> Vec<u8> {
    #[cfg(test)]
    RBSP_COPIES.with(|c| c.set(c.get() + 1));
    let mut out = Vec::with_capacity(rbsp.len());
    let mut zeros = 0usize;
    for &b in rbsp {
        if zeros >= 2 && b == 0x03 {
            // Drop the emulation-prevention byte; reset the run.
            zeros = 0;
            continue;
        }
        out.push(b);
        if b == 0x00 {
            zeros += 1;
        } else {
            zeros = 0;
        }
    }
    out
}

// RBSP bytes of an EBSP with emulation-prevention bytes dropped, yielded without copying.
// Same rule as `strip_emulation_prevention`.
struct RbspBytes<'a> {
    src: &'a [u8],
    pos: usize,
    zeros: usize,
}

impl<'a> RbspBytes<'a> {
    fn new(src: &'a [u8]) -> Self {
        Self {
            src,
            pos: 0,
            zeros: 0,
        }
    }
}

impl Iterator for RbspBytes<'_> {
    type Item = u8;
    fn next(&mut self) -> Option<u8> {
        loop {
            let b = *self.src.get(self.pos)?;
            self.pos += 1;
            if self.zeros >= 2 && b == 0x03 {
                self.zeros = 0;
                continue;
            }
            self.zeros = if b == 0x00 { self.zeros + 1 } else { 0 };
            return Some(b);
        }
    }
}

// Reads an SEI payloadType/payloadSize value (H.265 D.2 ff-extension coding:
// a run of 0xFF bytes plus one final byte < 0xFF). `None` at end-of-buffer.
fn read_sei_ff_value(rbsp: &mut impl Iterator<Item = u8>) -> Option<u32> {
    let mut value: u32 = 0;
    loop {
        let b = rbsp.next()?;
        value = value.checked_add(b as u32)?;
        if b != 0xFF {
            return Some(value);
        }
    }
}

// Parse the 24-byte big-endian Mastering Display Colour Volume SEI (H.265 D.2.28).
// Return None for a truncated payload.
fn parse_mastering_display(p: &[u8]) -> Option<MasteringDisplay> {
    if p.len() < 24 {
        return None;
    }
    let u16_at = |off: usize| u16::from_be_bytes([p[off], p[off + 1]]);
    let u32_at = |off: usize| u32::from_be_bytes([p[off], p[off + 1], p[off + 2], p[off + 3]]);
    Some(MasteringDisplay {
        display_primaries_x: [u16_at(0), u16_at(4), u16_at(8)],
        display_primaries_y: [u16_at(2), u16_at(6), u16_at(10)],
        white_point_x: u16_at(12),
        white_point_y: u16_at(14),
        max_display_mastering_luminance: u32_at(16),
        min_display_mastering_luminance: u32_at(20),
    })
}

// Parses a Content Light Level Info SEI payload (H.265 D.2.35): 4 bytes BE,
// max_content_light_level u(16) then max_pic_average_light_level u(16).
// `None` if shorter than 4 bytes.
fn parse_content_light_level(p: &[u8]) -> Option<ContentLightLevel> {
    if p.len() < 4 {
        return None;
    }
    Some(ContentLightLevel {
        max_content_light_level: u16::from_be_bytes([p[0], p[1]]),
        max_pic_average_light_level: u16::from_be_bytes([p[2], p[3]]),
    })
}

// Parses chroma_format_idc and bit depths from a stored SPS NAL. Handles
// emulation-prevention and sub-layer profile_tier_level. `None` if too short
// or malformed (caller falls back to the 8-bit 4:2:0 default).
fn parse_sps_chroma(sps: &[u8]) -> Option<SpsChroma> {
    if sps.len() < 3 {
        return None;
    }
    // RBSP begins after the 2-byte HEVC NAL header.
    let rbsp = strip_emulation_prevention(&sps[2..]);
    let mut r = BitReader::new(&rbsp);

    // sps_video_parameter_set_id u(4)
    r.skip_bits(4)?;
    // sps_max_sub_layers_minus1 u(3)
    let max_sub_layers_minus1 = r.read_bits(3)?;
    // sps_temporal_id_nesting_flag u(1)
    let temporal_id_nesting_flag = r.read_bit()?;

    // profile_tier_level( 1, sps_max_sub_layers_minus1 )
    parse_profile_tier_level(&mut r, max_sub_layers_minus1)?;

    // sps_seq_parameter_set_id ue(v)
    r.read_ue()?;
    // chroma_format_idc ue(v)
    let chroma_format_idc = u8::try_from(r.read_ue()?).ok().filter(|&c| c <= 3)?;
    if chroma_format_idc == 3 {
        // separate_colour_plane_flag u(1)
        r.skip_bits(1)?;
    }
    // pic_width_in_luma_samples ue(v), pic_height_in_luma_samples ue(v)
    r.read_ue()?;
    r.read_ue()?;
    // conformance_window_flag u(1) + 4× ue(v) if set
    if r.read_bit()? == 1 {
        r.read_ue()?;
        r.read_ue()?;
        r.read_ue()?;
        r.read_ue()?;
    }
    // bit_depth_luma_minus8 ue(v), bit_depth_chroma_minus8 ue(v)
    let bit_depth_luma_minus8 = u8::try_from(r.read_ue()?).ok().filter(|&d| d <= 8)?;
    let bit_depth_chroma_minus8 = u8::try_from(r.read_ue()?).ok().filter(|&d| d <= 8)?;
    // The ordering-info tail is optional to the hvcC caller: a cut-short SPS keeps
    // its chroma fields and only loses R.
    let (max_num_reorder_pics, picture_period_ticks) =
        match parse_sps_ordering_tail(&mut r, max_sub_layers_minus1) {
            Some((reorder, poc_lsb_bits)) => {
                (Some(reorder), parse_sps_vui_timing(&mut r, poc_lsb_bits))
            }
            None => (None, None),
        };

    Some(SpsChroma {
        chroma_format_idc,
        bit_depth_luma_minus8,
        bit_depth_chroma_minus8,
        max_sub_layers_minus1: max_sub_layers_minus1 as u8,
        temporal_id_nesting_flag: temporal_id_nesting_flag as u8,
        max_num_reorder_pics,
        picture_period_ticks,
    })
}

// H.265 §7.3.2.2 after bit_depth_chroma_minus8: log2_max_pic_order_cnt_lsb_minus4, then
// the sub-layer ordering loop (i = present ? 0 : max). Returns the reorder value at
// i = max and log2_max_pic_order_cnt_lsb.
fn parse_sps_ordering_tail(r: &mut BitReader, max_sub_layers_minus1: u32) -> Option<(u32, u32)> {
    // log2_max_pic_order_cnt_lsb_minus4 ue(v), 0..=12
    let poc_lsb_bits = r.read_ue()?.checked_add(4).filter(|&b| b <= 16)?;
    // sps_sub_layer_ordering_info_present_flag u(1)
    let first = if r.read_bit()? == 1 {
        0
    } else {
        max_sub_layers_minus1
    };
    let mut reorder = 0;
    for _ in first..=max_sub_layers_minus1 {
        // sps_max_dec_pic_buffering_minus1, sps_max_num_reorder_pics,
        // sps_max_latency_increase_plus1: all ue(v).
        r.read_ue()?;
        reorder = r.read_ue()?;
        r.read_ue()?;
    }
    Some((reorder, poc_lsb_bits))
}

// se(v) over the shared bit reader (H.265 §9.2): code_num k → (−1)^(k+1)·Ceil(k÷2).
fn read_se(r: &mut BitReader) -> Option<i64> {
    let k = r.read_ue()? as i64;
    Some(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
}

// H.265 §7.3.2.2 from log2_min_luma_coding_block_size_minus3 to the VUI's
// vui_timing_info (E.2.1), walking scaling_list_data, PCM, st_ref_pic_set and the
// long-term set. Returns num_units_in_tick ÷ time_scale in 90 kHz ticks (rounded).
fn parse_sps_vui_timing(r: &mut BitReader, poc_lsb_bits: u32) -> Option<i64> {
    // log2_min_luma_coding_block_size_minus3 … max_transform_hierarchy_depth_intra: 6 ue(v)
    for _ in 0..6 {
        r.read_ue()?;
    }
    // scaling_list_enabled_flag, then sps_scaling_list_data_present_flag
    if r.read_bit()? == 1 && r.read_bit()? == 1 {
        skip_scaling_list_data(r)?;
    }
    r.skip_bits(2)?; // amp_enabled_flag, sample_adaptive_offset_enabled_flag
    if r.read_bit()? == 1 {
        // pcm_enabled_flag: two u(4) bit depths, two ue(v) sizes, loop-filter flag.
        r.skip_bits(8)?;
        r.read_ue()?;
        r.read_ue()?;
        r.skip_bits(1)?;
    }
    let num_sets = r.read_ue()?; // num_short_term_ref_pic_sets, 0..=64
    if num_sets > 64 {
        return None;
    }
    let mut num_delta_pocs: Vec<u32> = Vec::with_capacity(num_sets as usize);
    for idx in 0..num_sets as usize {
        let n = skip_st_ref_pic_set(r, idx, &num_delta_pocs)?;
        num_delta_pocs.push(n);
    }
    if r.read_bit()? == 1 {
        // long_term_ref_pics_present_flag: lt_ref_pic_poc_lsb_sps u(v) + used flag each.
        let n = r.read_ue()?;
        if n > 32 {
            return None;
        }
        for _ in 0..n {
            r.skip_bits(poc_lsb_bits + 1)?;
        }
    }
    r.skip_bits(2)?; // sps_temporal_mvp_enabled_flag, strong_intra_smoothing_enabled_flag
    if r.read_bit()? == 0 {
        return None; // vui_parameters_present_flag
    }
    if r.read_bit()? == 1 && r.read_bits(8)? == 255 {
        r.skip_bits(32)?; // aspect_ratio_idc = Extended_SAR: sar_width, sar_height
    }
    if r.read_bit()? == 1 {
        r.skip_bits(1)?; // overscan_appropriate_flag
    }
    if r.read_bit()? == 1 {
        // video_format u(3), video_full_range_flag u(1), colour_description_present_flag
        r.skip_bits(4)?;
        if r.read_bit()? == 1 {
            r.skip_bits(24)?;
        }
    }
    if r.read_bit()? == 1 {
        r.read_ue()?; // chroma_sample_loc_type_top_field
        r.read_ue()?; // chroma_sample_loc_type_bottom_field
    }
    // neutral_chroma_indication_flag, field_seq_flag, frame_field_info_present_flag
    r.skip_bits(3)?;
    if r.read_bit()? == 1 {
        for _ in 0..4 {
            r.read_ue()?; // default display window offsets
        }
    }
    if r.read_bit()? == 0 {
        return None; // vui_timing_info_present_flag
    }
    let num_units_in_tick = r.read_bits(32)? as i64;
    let time_scale = r.read_bits(32)? as i64;
    (num_units_in_tick > 0 && time_scale > 0)
        .then(|| (num_units_in_tick * 90_000 + time_scale / 2) / time_scale)
}

// scaling_list_data() (H.265 §7.3.4): only its length matters here.
fn skip_scaling_list_data(r: &mut BitReader) -> Option<()> {
    for size_id in 0..4u32 {
        let step = if size_id == 3 { 3 } else { 1 };
        for _ in (0..6).step_by(step) {
            if r.read_bit()? == 0 {
                r.read_ue()?; // scaling_list_pred_matrix_id_delta
            } else {
                let coefs = 64.min(1u32 << (4 + (size_id << 1)));
                if size_id > 1 {
                    read_se(r)?; // scaling_list_dc_coef_minus8
                }
                for _ in 0..coefs {
                    read_se(r)?; // scaling_list_delta_coef
                }
            }
        }
    }
    Some(())
}

// st_ref_pic_set(idx) in the SPS (H.265 §7.3.7): returns its NumDeltaPocs. An
// inter-predicted set references set idx − 1 (delta_idx_minus1 is slice-header only).
fn skip_st_ref_pic_set(r: &mut BitReader, idx: usize, num_delta_pocs: &[u32]) -> Option<u32> {
    if idx != 0 && r.read_bit()? == 1 {
        r.skip_bits(1)?; // delta_rps_sign
        r.read_ue()?; // abs_delta_rps_minus1
        let mut n = 0;
        for _ in 0..=num_delta_pocs[idx - 1] {
            // used_by_curr_pic_flag; use_delta_flag only when not used (inferred 1).
            let used = r.read_bit()? == 1;
            if used || r.read_bit()? == 1 {
                n += 1;
            }
        }
        return Some(n);
    }
    let negative = r.read_ue()?;
    let positive = r.read_ue()?;
    if negative > 16 || positive > 16 {
        return None;
    }
    for _ in 0..negative + positive {
        r.read_ue()?; // delta_poc_s{0,1}_minus1
        r.skip_bits(1)?; // used_by_curr_pic_s{0,1}_flag
    }
    Some(negative + positive)
}

/// Consume a profile_tier_level(profilePresentFlag=1, maxNumSubLayersMinus1)
/// structure from the bit reader (HEVC 7.3.3).
fn parse_profile_tier_level(r: &mut BitReader, max_sub_layers_minus1: u32) -> Option<()> {
    // general PTL fixed layout (HEVC 7.3.3): profile_space/tier/profile_idc (8) +
    // compatibility_flags (32) + constraint-flags/reserved (48) + level_idc (8)
    // = 96 bits = 12 bytes. Skip 96 bits.
    r.skip_bits(96)?;

    if max_sub_layers_minus1 > 0 {
        // sub_layer_profile_present_flag[i] u(1) + sub_layer_level_present_flag[i]
        // u(1), for i in 0..max_sub_layers_minus1.
        let mut profile_present = [false; 8];
        let mut level_present = [false; 8];
        for i in 0..max_sub_layers_minus1 as usize {
            profile_present[i] = r.read_bit()? == 1;
            level_present[i] = r.read_bit()? == 1;
        }
        // reserved_zero_2bits for i in max_sub_layers_minus1..8
        for _ in max_sub_layers_minus1..8 {
            r.skip_bits(2)?;
        }
        for i in 0..max_sub_layers_minus1 as usize {
            if profile_present[i] {
                // sub_layer profile block: 8 + 32 + 48 = 88 bits.
                r.skip_bits(88)?;
            }
            if level_present[i] {
                // sub_layer_level_idc u(8)
                r.skip_bits(8)?;
            }
        }
    }
    Some(())
}

#[cfg(test)]
#[path = "hevc_tests.rs"]
mod tests;
