//! HEVC (H.265) elementary stream parser.
//!
//! Extracts VPS, SPS, PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IRAP pictures: IDR, CRA, BLA).
//! Each PES packet = one access unit = one frame.

use super::coding::{CodingType, PictureInfo};
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
    // CRA_NUT -> BLA_W_LP so NoRaslOutput discards dangling RASL). Armed by PTS-backstep
    // auto-detect in `parse`, or by the public `mark_clip_boundary` hook (no in-tree caller).
    pending_clip_boundary: bool,
    // Highest PES PTS seen, on a monotonic 64-bit timeline (33-bit PTS unwrapped
    // across 2^33 wraps — see `pts_wrap_offset`). Auto-detects a non-seamless clip
    // boundary when the caller never plumbs one in (see `BACKSTEP_TICKS`).
    high_pts: Option<i64>,
    // Accumulated 2^33-tick offset unwrapping raw PES PTS onto the monotonic
    // `high_pts` timeline. Without it, a 33-bit PTS wrap (~26.5h) looks like a
    // backward clip reset and false-arms the CRA→BLA rewrite, corrupting valid RASL.
    pts_wrap_offset: i64,
    // HDR10 static metadata from prefix/suffix SEI: mastering display (137) and
    // content light level (144), captured independently and sticky (first wins).
    // `hdr10()` combines both only when present; SDR streams stay `None`, never fabricated.
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

// A backward PES-PTS step over this (90kHz ticks) marks a non-seamless BD clip
// boundary: each .m2ts clip has its own PTS base, resetting far more than any
// B-frame reorder window. Mirrors `DISCONTINUITY_BACKSTEP_NS` in `mux/timeline.rs`.
const BACKSTEP_TICKS: i64 = 270_000;

// Enforced, not just described: 90kHz ticks -> ns is × 100_000 / 9, so this must
// equal `DISCONTINUITY_BACKSTEP_NS` exactly. Changing either constant alone fails the build.
const _: () = assert!(
    BACKSTEP_TICKS * 100_000 / 9 == crate::mux::timeline::DISCONTINUITY_BACKSTEP_NS,
    "HEVC BACKSTEP_TICKS must mirror mux::timeline::DISCONTINUITY_BACKSTEP_NS"
);

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

// 33-bit 90kHz PTS wraps at 2^33 ticks (~26.5h). A backward step of ~2^33 is a
// wraparound (unwrap, add 2^33) not a clip reset (arbitrary sub-2^33 backward
// step); accepting steps within `PTS_WRAP_PERIOD`/2 of a full period separates the two cases.
const PTS_WRAP_PERIOD: i64 = 1 << 33;

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
            high_pts: None,
            pts_wrap_offset: 0,
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

    // Combines mastering-display + content-light SEI into Hdr10Metadata, or `None` until BOTH
    // are seen — never a half-populated (confidently-wrong) HDR10 record.
    fn hdr10(&self) -> Option<crate::mux::codec::Hdr10Metadata> {
        let m = self.sei_mastering?;
        let c = self.sei_content_light?;
        Some(crate::mux::codec::Hdr10Metadata {
            display_primaries_x: m.display_primaries_x,
            display_primaries_y: m.display_primaries_y,
            white_point_x: m.white_point_x,
            white_point_y: m.white_point_y,
            max_display_mastering_luminance: m.max_display_mastering_luminance,
            min_display_mastering_luminance: m.min_display_mastering_luminance,
            max_content_light_level: c.max_content_light_level,
            max_pic_average_light_level: c.max_pic_average_light_level,
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
    /// join (MPLS `connection_condition` 0x05 or 0x06). The first CRA at/after
    /// this point is rewritten CRA_NUT (21) → BLA_W_LP (16) so a linear decoder
    /// sets NoRaslOutput and discards the now-dangling RASL leading pictures.
    ///
    /// MUST be called ONLY for connection_condition 0x05/0x06 — never for 0x01 (seamless/first
    /// item) or within a single-clip title.
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
        let pts_ns = explicit_pts.unwrap_or(0);

        // Auto-detect a non-seamless clip boundary: mpls connection_condition
        // isn't plumbed through the (threaded) mux pipeline, so a PTS backstep
        // beyond `BACKSTEP_TICKS` on the unwrapped/high-water timeline arms the rewrite.
        if let Some(raw_pts) = pes.pts {
            // Unwrap onto the monotonic timeline: if the offset-adjusted value
            // dropped ~2^33 below the high-water, the counter wrapped — add
            // another period and re-check, rather than treat it as a clip reset.
            let mut unwrapped = raw_pts + self.pts_wrap_offset;
            if let Some(high) = self.high_pts
                && high - unwrapped > PTS_WRAP_PERIOD / 2
            {
                self.pts_wrap_offset += PTS_WRAP_PERIOD;
                unwrapped += PTS_WRAP_PERIOD;
            }
            match self.high_pts {
                Some(high) if unwrapped < high - BACKSTEP_TICKS => {
                    self.pending_clip_boundary = true;
                    self.high_pts = Some(unwrapped);
                }
                Some(high) => self.high_pts = Some(high.max(unwrapped)),
                None => self.high_pts = Some(unwrapped),
            }
        }

        let data = &pes.data;
        let mut keyframe = false;
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
                            // Splice-aware CRA→BLA: the first CRA_NUT after a
                            // non-seamless boundary becomes BLA_W_LP so NoRaslOutput
                            // drops RASL. Any IRAP clears the flag; only CRA is rewritten.
                            if self.pending_clip_boundary && t == NAL_CRA_NUT {
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

        // HDR10 static metadata is stamped onto every frame's PictureInfo once
        // both SEI messages are seen, riding the deferred-muxer path (reads it
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
        self.finish(explicit_pts, frame)
    }

    fn flush(&mut self) -> Vec<Frame> {
        match self.reorder.as_mut() {
            Some(r) => r.flush(),
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

    /// Build an HEVC NAL header (2 bytes). Type is bits 1-6 of first byte.
    /// Format: forbidden(1) | type(6) | layer_id_high(1) || layer_id_low(5) | tid(3)
    fn hevc_nal_header(nal_type: u8) -> [u8; 2] {
        [(nal_type & 0x3F) << 1, 0x01] // tid=1
    }

    /// Encode an SEI message body: payloadType + payloadSize (ff-extension) +
    /// payload bytes. Values < 255 take a single byte each (the common case).
    fn sei_message(payload_type: u32, payload: &[u8]) -> Vec<u8> {
        fn ff_encode(mut v: u32) -> Vec<u8> {
            let mut out = Vec::new();
            while v >= 255 {
                out.push(0xFF);
                v -= 255;
            }
            out.push(v as u8);
            out
        }
        let mut m = ff_encode(payload_type);
        m.extend(ff_encode(payload.len() as u32));
        m.extend_from_slice(payload);
        m
    }

    /// Build a 24-byte Mastering Display Colour Volume payload (D.2.28) from raw
    /// SEI integers. SEI primary order is G(0), B(1), R(2).
    fn mastering_payload(
        prim_x: [u16; 3],
        prim_y: [u16; 3],
        wp_x: u16,
        wp_y: u16,
        max_lum: u32,
        min_lum: u32,
    ) -> Vec<u8> {
        let mut p = Vec::new();
        for c in 0..3 {
            p.extend_from_slice(&prim_x[c].to_be_bytes());
            p.extend_from_slice(&prim_y[c].to_be_bytes());
        }
        p.extend_from_slice(&wp_x.to_be_bytes());
        p.extend_from_slice(&wp_y.to_be_bytes());
        p.extend_from_slice(&max_lum.to_be_bytes());
        p.extend_from_slice(&min_lum.to_be_bytes());
        p
    }

    /// Build a 4-byte Content Light Level Info payload (D.2.35).
    fn cll_payload(maxcll: u16, maxfall: u16) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&maxcll.to_be_bytes());
        p.extend_from_slice(&maxfall.to_be_bytes());
        p
    }

    /// Insert HEVC emulation-prevention bytes: any `00 00` followed by a byte
    /// ≤ 0x03 gets a `0x03` inserted (Rec. ITU-T H.265 §7.4.2). A real bitstream
    /// is always EP-coded; the parser strips it back out.
    fn emulation_prevent(rbsp: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut zeros = 0;
        for &b in rbsp {
            if zeros >= 2 && b <= 0x03 {
                out.push(0x03);
                zeros = 0;
            }
            out.push(b);
            if b == 0 {
                zeros += 1;
            } else {
                zeros = 0;
            }
        }
        out
    }

    // Wraps SEI messages in a prefix-SEI NAL (type 39) after a start code, emulation-prevented
    // like a real encoder.
    fn sei_nal(messages: &[Vec<u8>]) -> Vec<u8> {
        let mut rbsp = Vec::new();
        for m in messages {
            rbsp.extend_from_slice(m);
        }
        let mut v = vec![0x00, 0x00, 0x01];
        v.extend_from_slice(&hevc_nal_header(NAL_SEI_PREFIX));
        v.extend_from_slice(&emulation_prevent(&rbsp));
        v.push(0x80); // rbsp_trailing_bits
        v
    }

    /// Both HDR10 SEI messages in one access unit → the parser surfaces a fully
    /// populated Hdr10Metadata with the EXACT raw SEI integers (scaling is the
    /// muxer's job, asserted separately in mkv.rs). DCI-P3 D65 reference values.
    #[test]
    fn hevc_parses_hdr10_sei_with_exact_raw_values() {
        // BT.2020 primaries (SEI order G, B, R) and D65 white point, as a typical
        // UHD master would signal. Luminance: 1000 cd/m² max (×10000 = 10_000_000),
        // 0.0001 cd/m² min (= 1).
        let prim_x = [8500u16, 6550, 35400]; // G, B, R
        let prim_y = [39850u16, 2300, 14600];
        let (wp_x, wp_y) = (15635u16, 16450);
        let (max_lum, min_lum) = (10_000_000u32, 1u32);
        let (maxcll, maxfall) = (1000u16, 400u16);

        let pps = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(NAL_PPS));
            v.push(0xC0); // num_extra_slice_header_bits 0
            v
        };
        let idr = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(19)); // IDR_W_RADL
            v.push(0xEC); // first_slice, slice_type I
            v
        };

        let mut data = pps;
        data.extend_from_slice(&sei_nal(&[
            sei_message(
                SEI_MASTERING_DISPLAY_COLOUR_VOLUME,
                &mastering_payload(prim_x, prim_y, wp_x, wp_y, max_lum, min_lum),
            ),
            sei_message(SEI_CONTENT_LIGHT_LEVEL_INFO, &cll_payload(maxcll, maxfall)),
        ]));
        data.extend_from_slice(&idr);

        let mut parser = HevcParser::new();
        let frames = parser.parse(&make_pes(data, Some(0)));
        let h = frames[0]
            .coding
            .expect("HEVC frame carries PictureInfo")
            .hdr10()
            .expect("both HDR10 SEI present → metadata surfaced");

        assert_eq!(h.display_primaries_x, prim_x, "primary X raw (G,B,R)");
        assert_eq!(h.display_primaries_y, prim_y, "primary Y raw (G,B,R)");
        assert_eq!(h.white_point_x, wp_x);
        assert_eq!(h.white_point_y, wp_y);
        assert_eq!(h.max_display_mastering_luminance, max_lum);
        assert_eq!(h.min_display_mastering_luminance, min_lum);
        assert_eq!(h.max_content_light_level, maxcll);
        assert_eq!(h.max_pic_average_light_level, maxfall);
    }

    // The FIRST mastering-display / content-light SEI wins; later repeats (or a corrupt splice)
    // must not overwrite it.
    #[test]
    fn hevc_hdr10_sei_keeps_the_first_value_and_ignores_later_repeats() {
        let pps = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(NAL_PPS));
            v.push(0xC0);
            v
        };
        let idr = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(19));
            v.push(0xEC);
            v
        };
        let mastering_au = |max_lum: u32| {
            let mut data = pps.clone();
            data.extend_from_slice(&sei_nal(&[sei_message(
                SEI_MASTERING_DISPLAY_COLOUR_VOLUME,
                &mastering_payload([1, 2, 3], [4, 5, 6], 7, 8, max_lum, 10),
            )]));
            data.extend_from_slice(&idr);
            data
        };
        let cll_au = |maxcll: u16, maxfall: u16| {
            let mut data = pps.clone();
            data.extend_from_slice(&sei_nal(&[sei_message(
                SEI_CONTENT_LIGHT_LEVEL_INFO,
                &cll_payload(maxcll, maxfall),
            )]));
            data.extend_from_slice(&idr);
            data
        };

        let mut mastering_only = HevcParser::new();
        mastering_only.parse(&make_pes(mastering_au(10_000_000), Some(0)));
        mastering_only.parse(&make_pes(mastering_au(1), Some(3750)));
        assert_eq!(
            mastering_only
                .sei_mastering
                .map(|m| m.max_display_mastering_luminance),
            Some(10_000_000),
            "the SECOND AU's mastering-luminance must be ignored, not adopted"
        );

        let mut cll_only = HevcParser::new();
        cll_only.parse(&make_pes(cll_au(1000, 400), Some(0)));
        cll_only.parse(&make_pes(cll_au(9999, 9999), Some(3750)));
        assert_eq!(
            cll_only.sei_content_light,
            Some(ContentLightLevel {
                max_content_light_level: 1000,
                max_pic_average_light_level: 400
            }),
            "the SECOND AU's content-light numbers must be ignored, not adopted"
        );
    }

    // MEASURED: `scan_sei` makes no RBSP copy once both HDR10 messages are captured.
    #[test]
    fn scan_sei_stops_copying_once_both_hdr10_messages_are_captured() {
        let pps = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(NAL_PPS));
            v.push(0xC0);
            v
        };
        let idr = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(19));
            v.push(0xEC);
            v
        };
        // Every AU carries BOTH HDR10 SEI messages, as a real HDR10 stream does.
        let au = || {
            let mut data = pps.clone();
            data.extend_from_slice(&sei_nal(&[
                sei_message(
                    SEI_MASTERING_DISPLAY_COLOUR_VOLUME,
                    &mastering_payload([1, 2, 3], [4, 5, 6], 7, 8, 9, 10),
                ),
                sei_message(SEI_CONTENT_LIGHT_LEVEL_INFO, &cll_payload(1000, 400)),
            ]));
            data.extend_from_slice(&idr);
            data
        };

        let mut parser = HevcParser::new();
        // First AU captures both messages.
        parser.parse(&make_pes(au(), Some(0)));
        assert!(
            parser.sei_mastering.is_some() && parser.sei_content_light.is_some(),
            "first AU must capture both HDR10 messages"
        );
        // Now measure the next 50 AUs, whose SEI scan is a guaranteed no-op.
        RBSP_COPIES.with(|c| c.set(0));
        for i in 0..50 {
            parser.parse(&make_pes(au(), Some(3750 * (i + 1))));
        }
        let copies = RBSP_COPIES.with(|c| c.get());
        assert_eq!(
            copies, 0,
            "SEI RBSP must not be copied once both HDR10 messages are captured; \
             {copies} copies over 50 access units"
        );
        // And the captured metadata is still surfaced on those later frames.
        let f = parser.parse(&make_pes(au(), Some(3750 * 51)));
        assert!(
            f[0].coding.unwrap().hdr10().is_some(),
            "the sticky HDR10 metadata must still ride every later frame"
        );
    }

    // A stream carrying only ONE HDR10 message never completes the pair, so the SEI
    // scan runs every AU; it must still not copy the RBSP each time.
    #[test]
    fn scan_sei_does_not_copy_when_one_hdr10_message_is_absent() {
        let mut au = nal_bytes(NAL_PPS, &[0xC0]);
        au.extend_from_slice(&sei_nal(&[sei_message(
            SEI_MASTERING_DISPLAY_COLOUR_VOLUME,
            &mastering_payload([1, 2, 3], [4, 5, 6], 7, 8, 9, 10),
        )]));
        au.extend_from_slice(&nal_bytes(19, &[0xEC]));
        let mut parser = HevcParser::new();
        parser.parse(&make_pes(au.clone(), Some(0)));
        assert!(parser.sei_mastering.is_some() && parser.sei_content_light.is_none());
        RBSP_COPIES.with(|c| c.set(0));
        for i in 0..50 {
            parser.parse(&make_pes(au.clone(), Some(3750 * (i + 1))));
        }
        assert_eq!(RBSP_COPIES.with(|c| c.get()), 0);
    }

    /// Only the mastering-display SEI (no content-light SEI) → metadata is NOT
    /// surfaced. HDR10 requires BOTH; a half-populated record is never emitted.
    #[test]
    fn hevc_requires_both_hdr10_sei_messages() {
        let pps = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(NAL_PPS));
            v.push(0xC0);
            v
        };
        let idr = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(19));
            v.push(0xEC); // IDR: first_slice + no_output + pps_id 0 + slice_type I
            v
        };
        let mut data = pps;
        data.extend_from_slice(&sei_nal(&[sei_message(
            SEI_MASTERING_DISPLAY_COLOUR_VOLUME,
            &mastering_payload([1, 2, 3], [4, 5, 6], 7, 8, 9, 10),
        )]));
        data.extend_from_slice(&idr);

        let mut parser = HevcParser::new();
        let frames = parser.parse(&make_pes(data, Some(0)));
        assert!(
            frames[0].coding.unwrap().hdr10().is_none(),
            "mastering-only stream must NOT surface HDR10 (content-light absent)"
        );
    }

    /// An SDR stream with no HDR10 SEI at all leaves hdr10() None — never faked.
    #[test]
    fn hevc_sdr_stream_has_no_hdr10() {
        let pps = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(NAL_PPS));
            v.push(0xC0);
            v
        };
        let idr = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(19));
            v.push(0xEC); // IDR: first_slice + no_output + pps_id 0 + slice_type I
            v
        };
        let mut data = pps;
        data.extend_from_slice(&idr);
        let mut parser = HevcParser::new();
        let frames = parser.parse(&make_pes(data, Some(0)));
        assert!(
            frames[0].coding.unwrap().hdr10().is_none(),
            "SDR / no-SEI stream must surface no HDR10 metadata"
        );
    }

    // HDR10 SEI parse must de-emulate (00 00 03) before reading fields, or
    // every field after an emulation byte shifts by one. Builds a mastering
    // payload with a forced 00 00 03 and checks the decode still matches.
    #[test]
    fn hevc_hdr10_sei_de_emulates() {
        // prim_x[0]=0x0000, prim_y[0]=0x0002: raw payload starts 00 00 00 02, so
        // a conforming encoder inserts emulation-prevention 0x03 -> 00 00 03 00 02.
        // The parser must strip that 03 before reading, or every later field shifts.
        let prim_x = [0u16, 6550, 35400];
        let prim_y = [2u16, 2300, 14600];
        let payload = mastering_payload(prim_x, prim_y, 15635, 16450, 10_000_000, 1);

        // Manually emulate: insert 0x03 after each 00 00 followed by a byte ≤ 0x03,
        // the way a conforming HEVC encoder would in the RBSP.
        let mut emulated = Vec::new();
        let mut zeros = 0;
        for &b in &payload {
            if zeros >= 2 && b <= 0x03 {
                emulated.push(0x03);
                zeros = 0;
            }
            emulated.push(b);
            if b == 0 {
                zeros += 1;
            } else {
                zeros = 0;
            }
        }
        assert!(
            emulated.len() > payload.len(),
            "test must actually insert an emulation byte"
        );

        let mut nal = vec![0x00, 0x00, 0x01];
        nal.extend_from_slice(&hevc_nal_header(NAL_SEI_PREFIX));
        nal.push(137); // payloadType
        nal.push(24); // payloadSize = ORIGINAL (un-emulated) byte count
        nal.extend_from_slice(&emulated);
        nal.push(0x80);

        // Pair with a content-light SEI so hdr10() can combine.
        let mut clnal = vec![0x00, 0x00, 0x01];
        clnal.extend_from_slice(&hevc_nal_header(NAL_SEI_PREFIX));
        clnal.push(144);
        clnal.push(4);
        clnal.extend_from_slice(&cll_payload(1000, 400));
        clnal.push(0x80);

        let pps = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(NAL_PPS));
            v.push(0xC0);
            v
        };
        let idr = {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(19));
            v.push(0xEC); // IDR: first_slice + no_output + pps_id 0 + slice_type I
            v
        };
        let mut data = pps;
        data.extend_from_slice(&nal);
        data.extend_from_slice(&clnal);
        data.extend_from_slice(&idr);

        let mut parser = HevcParser::new();
        let frames = parser.parse(&make_pes(data, Some(0)));
        let h = frames[0].coding.unwrap().hdr10().unwrap();
        assert_eq!(
            h.display_primaries_x, prim_x,
            "de-emulated payload must decode to original primary X (00 00 03 stripped)"
        );
        assert_eq!(h.display_primaries_y, prim_y);
        assert_eq!(h.max_display_mastering_luminance, 10_000_000);
    }

    // Every earlier fixture used num_extra_slice_header_bits == 0, so a parser that ignored the
    // field entirely agreed with all of them.
    #[test]
    // The underscores here mark BITFIELD boundaries (e.g. 5-bit then 3-bit),
    // not thousands-style digit groups — regrouping them uniformly would
    // satisfy the lint by destroying the only thing they encode.
    #[allow(clippy::unusual_byte_groupings)]
    fn nonzero_num_extra_slice_header_bits_shifts_the_slice_type_offset() {
        use super::super::coding::CodingType;

        // PPS body bits: pps_id ue=0 ('1'), sps_id ue=0 ('1'),
        // dependent_slice_segments_enabled_flag 0, output_flag_present_flag 0,
        // num_extra_slice_header_bits u(3).
        let pps_body = |num_extra: u8| 0b1100_0000u8 | (num_extra << 1);
        assert_eq!(pps_body(0), 0xC0, "matches the existing zero-extra fixture");
        assert_eq!(pps_body(3), 0xC6);

        let pps_nal = |num_extra: u8| {
            let mut v = hevc_nal_header(NAL_PPS).to_vec();
            v.push(pps_body(num_extra));
            v
        };
        for n in 0..8u8 {
            assert_eq!(
                hevc_num_extra_slice_header_bits(&pps_nal(n)),
                Some(n as u32),
                "PPS must yield the value it encodes, for every u(3) code point"
            );
        }
        // A PPS truncated to just its 2-byte NAL header carries no field to read,
        // so the answer is absent — never a defaulted zero.
        assert_eq!(
            hevc_num_extra_slice_header_bits(&hevc_nal_header(NAL_PPS)),
            None
        );

        // Slice header for TRAIL_R (type 1): first_slice=1, pps_id ue=0 ('1'),
        // then THREE reserved bits = 101 (deliberately nonzero, so skipping vs
        // reading them disagrees), then slice_type ue(v)='011' -> 2 (I).
        let slice_body = 0b1_1_101_011u8;
        assert_eq!(slice_body, 0xEB);

        let nal = |t: u8, body: u8| {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.push(body);
            v
        };
        let mut data = nal(NAL_PPS, pps_body(3));
        data.extend_from_slice(&nal(1, slice_body));
        let frames = HevcParser::new().parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].coding.expect("PictureInfo").coding_type(),
            CodingType::I,
            "with num_extra=3 the reserved bits are skipped and slice_type reads 2 (I)"
        );

        // Control: the SAME slice bytes under a PPS declaring num_extra=0 land on
        // a different slice_type — proving the PPS field, not the slice bytes,
        // decides the offset.
        let mut data0 = nal(NAL_PPS, pps_body(0));
        data0.extend_from_slice(&nal(1, slice_body));
        let frames0 = HevcParser::new().parse(&make_pes(data0, Some(0)));
        assert_eq!(
            frames0[0].coding.expect("PictureInfo").coding_type(),
            CodingType::B,
            "num_extra=0 reads slice_type from bit 2 instead → 0 (B)"
        );
    }

    /// A slice must be measured with the num_extra_slice_header_bits of the PPS
    /// it REFERENCES (slice_pic_parameter_set_id), not the last-active PPS. Two
    /// PPS with different num_extra: the slice points at the FIRST while the
    /// second is active. Assuming the active PPS reads slice_type at the wrong
    /// offset (None here); resolving by the slice's own pps id reads it right.
    #[test]
    #[allow(clippy::unusual_byte_groupings)]
    fn slice_coding_type_uses_the_pps_the_slice_references_not_the_active_one() {
        use super::super::coding::CodingType;

        // PPS id=0, num_extra=0 → RBSP byte 0xC0 (pps_id '1', sps_id '1', 00, 000).
        let pps0 = 0xC0u8;
        // PPS id=1, num_extra=3 → RBSP 0101_0001 1000_0000: pps_id ue '010'=1,
        // sps_id '1'=0, 00, num_extra '011'=3.
        let pps1 = [0x51u8, 0x80];
        // Raw NAL (header + body, NO start code) for the direct-parse helpers.
        let raw_pps = |body: &[u8]| {
            let mut v = hevc_nal_header(NAL_PPS).to_vec();
            v.extend_from_slice(body);
            v
        };
        assert_eq!(hevc_num_extra_slice_header_bits(&raw_pps(&[pps0])), Some(0));
        assert_eq!(hevc_num_extra_slice_header_bits(&raw_pps(&pps1)), Some(3));
        assert_eq!(hevc_pps_id(&raw_pps(&[pps0])), Some(0));
        assert_eq!(hevc_pps_id(&raw_pps(&pps1)), Some(1));

        // TRAIL_R slice referencing pps_id=0: first_slice=1, pps_id '1'=0,
        // slice_type ue '011'=2 (I) directly (num_extra of PPS 0 is 0). 0xD8.
        let slice = 0xD8u8;

        // AU order: PPS0, PPS1 (so cur_pps = PPS1, num_extra=3), then the slice
        // that references PPS0 (num_extra=0).
        let mut data = nal_bytes(NAL_PPS, &[pps0]);
        data.extend_from_slice(&nal_bytes(NAL_PPS, &pps1));
        data.extend_from_slice(&nal_bytes(1, &[slice]));

        let frames = HevcParser::new().parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].coding.expect("PictureInfo").coding_type(),
            CodingType::I,
            "resolved via the slice's own pps id (PPS0, num_extra=0) → slice_type 2 (I); \
             assuming the active PPS1 (num_extra=3) would misread the offset"
        );
    }

    // PPS ids are 0..=63 (H.265 7.4.3.3): an out-of-range id is never stored, so a slice
    // naming it declines a coding type; id 63 still resolves.
    #[test]
    #[allow(clippy::unusual_byte_groupings)]
    fn pps_id_outside_spec_range_is_ignored() {
        use super::super::coding::CodingType;
        // PPS id=64: ue '0000001000001', sps_id '1', flags '00', num_extra '000', stop '1'.
        let pps64 = [0b0000_0010, 0b0000_1100, 0b0001_0000];
        // TRAIL_R slice: first '1', pps_id ue(64), slice_type ue '011' (I), stop '1'.
        let slice64 = [0b1000_0001, 0b0000_0101, 0b1100_0000];
        let mut data = nal_bytes(NAL_PPS, &pps64);
        data.extend_from_slice(&nal_bytes(1, &slice64));
        let frames = HevcParser::new().parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].coding.map(|c| c.coding_type()),
            None,
            "pps id 64 is out of range"
        );

        // PPS id=63: ue '0000001000000', sps_id '1', flags '00', num_extra '000', stop '1'.
        let pps63 = [0b0000_0010, 0b0000_0100, 0b0001_0000];
        // Slice: first '1', pps_id ue(63), slice_type '011', stop '1'.
        let slice63 = [0b1000_0001, 0b0000_0001, 0b1100_0000];
        assert_eq!(hevc_pps_id(&[0x44, 0x01, pps63[0], pps63[1]]), Some(63));
        let mut data = nal_bytes(NAL_PPS, &pps63);
        data.extend_from_slice(&nal_bytes(1, &slice63));
        let frames = HevcParser::new().parse(&make_pes(data, Some(0)));
        assert_eq!(
            frames[0].coding.map(|c| c.coding_type()),
            Some(CodingType::I)
        );
    }

    /// Build an Annex-B NAL (start code + 2-byte header + body bytes).
    fn nal_bytes(nal_type: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01];
        v.extend_from_slice(&hevc_nal_header(nal_type));
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn hevc_populates_measured_coding_type_and_source() {
        use super::super::coding::CodingType;
        // PPS 0xC0: all fields 0 so slice_type follows pps_id directly. Slice
        // body (TRAIL_R type 1) = first_slice=1, pps_id=0, slice_type:
        // 0xD8->I(2); 0xD0->P(1); 0xE0->B(0).
        let nal = |t: u8, body: u8| {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.push(body);
            v
        };
        let src = crate::pes::SourcePos::at_byte(16384);
        let run = |slice_body: u8| {
            let mut p = HevcParser::new();
            let mut data = nal(NAL_PPS, 0xC0); // active PPS first (sets num_extra)
            data.extend_from_slice(&nal(1, slice_body)); // then the coded slice
            let mut pe = make_pes(data, Some(0));
            pe.source = Some(src);
            p.parse(&pe)
        };

        let fi = run(0xD8);
        assert_eq!(fi.len(), 1);
        let ci = fi[0].coding.expect("HEVC frame carries PictureInfo");
        assert_eq!(ci.coding_type(), CodingType::I, "slice_type 2 → I");
        assert!(
            ci.field_order().is_none(),
            "HEVC field order undecoded → None, never faked"
        );
        assert_eq!(
            fi[0].source.unwrap().byte,
            16384,
            "source provenance carried"
        );
        assert_eq!(
            run(0xD0)[0].coding.unwrap().coding_type(),
            CodingType::P,
            "slice_type 1 → P"
        );
        assert_eq!(
            run(0xE0)[0].coding.unwrap().coding_type(),
            CodingType::B,
            "slice_type 0 → B"
        );

        // No PPS seen → num_extra is unknown, so slice_type is NOT guessed; the
        // coding stays None (honestly absent) rather than risk a wrong offset.
        let mut p = HevcParser::new();
        let bare = p.parse(&make_pes(nal(1, 0xD8), Some(0)));
        assert!(
            bare[0].coding.is_none(),
            "no active PPS → coding omitted, never a guessed type"
        );
    }

    // An IRAP slice header carries no_output_of_prior_pics_flag before the PPS id; skipping it
    // is what lands slice_type on the right bit, whatever the flag's value.
    #[test]
    fn irap_slices_report_their_coding_type() {
        use super::super::coding::CodingType;
        // first_slice=1, no_output=X, pps_id ue '1'=0, slice_type ue '011'=2 (I), pad.
        for (nal_type, body) in [(19, 0xECu8), (19, 0xAC), (21, 0xEC), (21, 0xAC)] {
            let mut data = nal_bytes(NAL_PPS, &[0xC0]);
            data.extend_from_slice(&nal_bytes(nal_type, &[body]));
            let frames = HevcParser::new().parse(&make_pes(data, Some(0)));
            assert_eq!(
                frames[0].coding.expect("PictureInfo").coding_type(),
                CodingType::I,
                "NAL {nal_type} body {body:#x}"
            );
        }
    }

    // --- VPS+SPS+PPS → codec_private ---

    #[test]
    fn parse_vps_sps_pps() {
        let mut parser = HevcParser::new();

        let mut data = Vec::new();
        // VPS (type 32)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let vps_hdr = hevc_nal_header(32);
        data.extend_from_slice(&vps_hdr);
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // VPS payload

        // SPS (type 33)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let sps_hdr = hevc_nal_header(33);
        data.extend_from_slice(&sps_hdr);
        data.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
        ]); // SPS payload (>12 bytes for level)

        // PPS (type 34)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let pps_hdr = hevc_nal_header(34);
        data.extend_from_slice(&pps_hdr);
        data.extend_from_slice(&[0xDD, 0xEE]); // PPS payload

        // IRAP slice (type 19 = IDR_W_RADL) so a frame is emitted
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let idr_hdr = hevc_nal_header(19);
        data.extend_from_slice(&idr_hdr);
        data.extend_from_slice(&[0x10, 0x20, 0x30]);

        let pes = make_pes(data, Some(90000));
        let _frames = parser.parse(&pes);

        let cp = parser.codec_private();
        assert!(
            cp.is_some(),
            "codec_private should be Some after VPS+SPS+PPS"
        );

        let cp = cp.unwrap();
        // configurationVersion = 1
        assert_eq!(cp[0], 1);
        // numOfArrays = 3 (VPS, SPS, PPS)
        assert_eq!(cp[22], 3);
        // Should be longer than the minimal header (23 bytes) + array entries
        assert!(
            cp.len() > 23,
            "codec_private should contain VPS+SPS+PPS data"
        );
    }

    // Regression (UHD banded corruption): a bare keyframe after a mid-title PPS redefinition
    // must re-assert the active PPS in-band.
    #[test]
    fn reasserts_active_pps_at_bare_keyframe() {
        fn nal(t: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.extend_from_slice(body);
            v
        }
        // Split length-prefixed frame_data back into NAL bodies.
        fn nals_in(frame: &[u8]) -> Vec<Vec<u8>> {
            let mut out = Vec::new();
            let mut i = 0;
            while i + 4 <= frame.len() {
                let len = u32::from_be_bytes([frame[i], frame[i + 1], frame[i + 2], frame[i + 3]])
                    as usize;
                i += 4;
                if i + len > frame.len() {
                    break;
                }
                out.push(frame[i..i + len].to_vec());
                i += len;
            }
            out
        }
        let pps_of = |nals: &[Vec<u8>]| -> Vec<Vec<u8>> {
            nals.iter()
                .filter(|n| n.len() >= 2 && (n[0] >> 1) & 0x3F == 34)
                .map(|n| n[2..].to_vec())
                .collect()
        };
        let sps_body = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
        ];
        let pps_a = [0xA1u8, 0xA2];
        let pps_b = [0xB1u8, 0xB2, 0xB3];

        let mut parser = HevcParser::new();

        // AU1: seeds codecPrivate with VPS/SPS/PPS-A (all stripped in-band).
        let au1 = [
            nal(32, &[0xAA]),
            nal(33, &sps_body),
            nal(34, &pps_a),
            nal(19, &[0x10]),
        ]
        .concat();
        parser.parse(&make_pes(au1, Some(0)));

        // AU2: keyframe redefines PPS id 0 to body B → emitted in-band.
        let au2 = [nal(34, &pps_b), nal(19, &[0x11])].concat();
        let f2 = parser.parse(&make_pes(au2, Some(3600)));
        assert!(
            pps_of(&nals_in(&f2[0].data)).iter().any(|b| b == &pps_b),
            "AU2 must carry the redefined PPS-B in-band"
        );

        // AU3: BARE keyframe, source omits the PPS. The active set (B) must be
        // re-asserted, and the stale codecPrivate A must NOT be injected.
        let au3 = nal(19, &[0x12]);
        let f3 = parser.parse(&make_pes(au3, Some(7200)));
        let got = pps_of(&nals_in(&f3[0].data));
        assert!(
            got.iter().any(|b| b == &pps_b),
            "bare keyframe must re-assert the active PPS-B in-band, got {got:?}"
        );
        assert!(
            !got.iter().any(|b| b == &pps_a),
            "must not re-assert the stale codecPrivate PPS-A"
        );

        // AU4: switch the active set BACK to A (== codecPrivate) via an in-band
        // redefinition (a real change from B → emitted).
        let au4 = [nal(34, &pps_a), nal(19, &[0x13])].concat();
        parser.parse(&make_pes(au4, Some(10800)));
        // AU5: bare keyframe, active set now EQUALS codecPrivate. Must still be
        // re-asserted in-band — a decoder that dropped PPS id 0 at a CRA reset
        // can only recover from an in-band copy, though no genuine change occurred.
        let au5 = nal(19, &[0x14]);
        let f5 = parser.parse(&make_pes(au5, Some(14400)));
        assert!(
            pps_of(&nals_in(&f5[0].data)).iter().any(|b| b == &pps_a),
            "bare keyframe must re-assert the active PPS even when == codecPrivate"
        );
    }

    // MEASURED: the keyframe param-set re-assert must splice into the already-assembled access
    // unit IN PLACE, never a fresh full-size buffer.
    #[test]
    fn keyframe_param_reassert_does_not_reallocate_the_frame() {
        fn nal(t: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.extend_from_slice(body);
            v
        }
        let sps_body = [0x01u8; 24];
        let pps_body = [0xA1u8, 0xA2, 0xA3];
        let mut parser = HevcParser::new();
        // AU1 seeds the active VPS/SPS/PPS.
        let au1 = [
            nal(32, &[0xAA; 12]),
            nal(33, &sps_body),
            nal(34, &pps_body),
            nal(19, &[0x10; 4096]),
        ]
        .concat();
        parser.parse(&make_pes(au1, Some(0)));

        // A run of BARE keyframes (source omits the parameter sets), each of which
        // takes the re-assert path. Payload sized like a real coded picture so a
        // reallocation would be the expensive one.
        PARAM_REASSERT_REALLOCS.with(|c| c.set(0));
        for i in 0..30i64 {
            let au = nal(19, &vec![0x11u8; 300_000]);
            let f = parser.parse(&make_pes(au, Some(3600 * (i + 1))));
            // The re-assert really happened (otherwise the count is vacuously 0).
            assert!(
                f[0].data.len() > 300_000,
                "keyframe {i} must carry the re-asserted parameter sets"
            );
        }
        let reallocs = PARAM_REASSERT_REALLOCS.with(|c| c.get());
        assert_eq!(
            reallocs, 0,
            "the parameter-set splice must fit in the reserved headroom; \
             {reallocs} of 30 keyframes reallocated the whole frame"
        );
    }

    // Regression (a UHD title, the real bug): switching PPS id 0 back to its codecPrivate body
    // must still be emitted in-band.
    #[test]
    fn emits_switch_back_to_codecprivate_pps() {
        fn nal(t: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.extend_from_slice(body);
            v
        }
        fn nals_in(frame: &[u8]) -> Vec<Vec<u8>> {
            let mut out = Vec::new();
            let mut i = 0;
            while i + 4 <= frame.len() {
                let len = u32::from_be_bytes([frame[i], frame[i + 1], frame[i + 2], frame[i + 3]])
                    as usize;
                i += 4;
                if i + len > frame.len() {
                    break;
                }
                out.push(frame[i..i + len].to_vec());
                i += len;
            }
            out
        }
        let pps_body = |nals: &[Vec<u8>]| -> Vec<Vec<u8>> {
            nals.iter()
                .filter(|n| n.len() >= 2 && (n[0] >> 1) & 0x3F == 34)
                .map(|n| n[2..].to_vec())
                .collect()
        };
        let sps = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
        ];
        let a = [0xA1u8, 0xA2];
        let b = [0xB1u8, 0xB2, 0xB3];
        let mut parser = HevcParser::new();

        // AU1: seeds codecPrivate with PPS-A.
        parser.parse(&make_pes(
            [nal(32, &[0xAA]), nal(33, &sps), nal(34, &a), nal(19, &[1])].concat(),
            Some(0),
        ));
        // AU2 keyframe: redefine to B → emitted in-band.
        parser.parse(&make_pes([nal(34, &b), nal(19, &[2])].concat(), Some(3600)));
        // AU3 keyframe: source sends A again (== codecPrivate). Must be emitted
        // in-band because the active set was B.
        let f3 = parser.parse(&make_pes([nal(34, &a), nal(19, &[3])].concat(), Some(7200)));
        assert!(
            pps_body(&nals_in(&f3[0].data)).iter().any(|p| p == &a),
            "switch back to codecPrivate PPS-A must be emitted in-band"
        );
        // AU4 keyframe: A again, now == active AND == codecPrivate. Still
        // re-asserted in-band (self-contained-keyframe rule) so a decoder that
        // dropped PPS id 0 at this IRAP recovers.
        let f4 = parser.parse(&make_pes(
            [nal(34, &a), nal(19, &[4])].concat(),
            Some(10800),
        ));
        assert!(
            pps_body(&nals_in(&f4[0].data)).iter().any(|p| p == &a),
            "active PPS must be re-asserted at every keyframe (self-contained), even when == codecPrivate"
        );
    }

    #[test]
    fn hvcc_profile_tier_level_offsets() {
        // The hvcC fixed header must read profile_tier_level from the SPS RBSP,
        // not the NAL header. Stored SPS = [2-byte NAL header][RBSP]; layout is
        // byte-aligned sps[2..15] per the labeled array below.
        let mut parser = HevcParser::new();

        // Distinct, recognizable values for each field.
        let sps_rbsp: [u8; 13] = [
            0xAB, // sps[2]  (vps_id etc.) — must NOT leak into profile fields
            0x21, // sps[3]  profile byte: space=0, tier=0, profile_idc=1
            0x60, 0x00, 0x00, 0x00, // sps[4..8] compat flags
            0x90, 0x00, 0x00, 0x00, 0x00, 0x00, // sps[8..14] constraint flags
            0x7B, // sps[14] level_idc = 123
        ];

        let mut data = Vec::new();
        // VPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        // SPS — 2-byte header + the structured RBSP above
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&sps_rbsp);
        // PPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD, 0xEE]);

        let pes = make_pes(data, Some(0));
        parser.parse(&pes);

        let cp = parser
            .codec_private()
            .expect("codec_private should be Some");

        // record[0] = configurationVersion
        assert_eq!(cp[0], 1, "configurationVersion");
        // record[1] = general_profile_space+tier+profile_idc  <- sps[3]
        assert_eq!(
            cp[1], 0x21,
            "profile byte must come from SPS RBSP, not NAL hdr"
        );
        // record[2..6] = general_profile_compatibility_flags  <- sps[4..8]
        assert_eq!(&cp[2..6], &[0x60, 0x00, 0x00, 0x00], "compatibility flags");
        // record[6..12] = general_constraint_indicator_flags  <- sps[8..14]
        assert_eq!(
            &cp[6..12],
            &[0x90, 0x00, 0x00, 0x00, 0x00, 0x00],
            "constraint flags"
        );
        // record[12] = general_level_idc  <- sps[14]
        assert_eq!(cp[12], 0x7B, "level_idc must come from sps[14]");
    }

    #[test]
    fn hvcc_short_sps_does_not_panic() {
        // A truncated SPS must still produce a fixed header without panicking
        // and zero-pad the missing profile/level bytes.
        let mut parser = HevcParser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA]);
        // SPS with only 3 RBSP bytes (stored len = 5): forces every guard path
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0x11, 0x22, 0x33]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD]);

        let pes = make_pes(data, Some(0));
        parser.parse(&pes);

        let cp = parser
            .codec_private()
            .expect("codec_private should be Some");
        // sps stored = [hdr0, hdr1, 0x11, 0x22, 0x33], len 5.
        // profile byte = sps[3] = 0x22; everything past sps[4]=0x33 is absent.
        assert_eq!(cp[0], 1);
        assert_eq!(cp[1], 0x22, "profile byte = sps[3]");
        // compat flags: only sps[4]=0x33 present, rest zero-padded.
        assert_eq!(&cp[2..6], &[0x33, 0x00, 0x00, 0x00]);
        // constraint flags: none present, all zero.
        assert_eq!(&cp[6..12], &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        // level_idc: absent, zero.
        assert_eq!(cp[12], 0x00);
    }

    #[test]
    fn codec_private_none_before_params() {
        let parser = HevcParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn codec_private_none_missing_pps() {
        let mut parser = HevcParser::new();

        // Only VPS + SPS, no PPS
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA, 0xBB]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        // Add a slice so parse doesn't return empty
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(1)); // TRAIL_R
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(0));
        parser.parse(&pes);
        assert!(
            parser.codec_private().is_none(),
            "should be None without PPS"
        );
    }

    // --- IRAP keyframe detection ---

    #[test]
    fn parse_irap_keyframe_idr_w_radl() {
        let mut parser = HevcParser::new();

        let mut data = Vec::new();
        // IDR_W_RADL = type 19
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(19));
        data.extend_from_slice(&[0x10, 0x20, 0x30]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(
            frames[0].keyframe,
            "IDR_W_RADL (type 19) should be keyframe"
        );
    }

    #[test]
    fn parse_irap_keyframe_bla() {
        let mut parser = HevcParser::new();

        // BLA_W_LP = type 16
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(16));
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe, "BLA_W_LP (type 16) should be keyframe");
    }

    #[test]
    fn parse_irap_keyframe_cra() {
        let mut parser = HevcParser::new();

        // CRA_NUT = type 21
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(21));
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe, "CRA (type 21) should be keyframe");
    }

    #[test]
    fn parse_irap_type_23() {
        let mut parser = HevcParser::new();

        // RSV_IRAP_VCL23 = type 23 (upper boundary)
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(23));
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe, "type 23 should be keyframe");
    }

    // --- splice-aware CRA→BLA rewrite (non-seamless clip boundary) ---

    /// Split length-prefixed frame_data into NAL bodies (4-byte BE length + NAL).
    fn nals_of(frame: &[u8]) -> Vec<Vec<u8>> {
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

    fn nal_type_of(nal: &[u8]) -> u8 {
        (nal[0] >> 1) & 0x3F
    }

    /// Build a standalone CRA (type 21) access unit.
    fn cra_au(payload: &[u8]) -> Vec<u8> {
        let mut d = vec![0x00, 0x00, 0x01];
        d.extend_from_slice(&hevc_nal_header(21));
        d.extend_from_slice(payload);
        d
    }

    /// Every slice NAL of the spliced CRA picture must get the same BLA type.
    #[test]
    fn multi_slice_cra_at_boundary_all_rewritten() {
        let mut parser = HevcParser::new();
        parser.mark_clip_boundary();
        let mut au = cra_au(&[0x10, 0x20]);
        au.extend_from_slice(&cra_au(&[0x30, 0x40]));
        let frames = parser.parse(&make_pes(au, Some(0)));
        let types: Vec<u8> = nals_of(&frames[0].data)
            .iter()
            .map(|n| nal_type_of(n))
            .collect();
        assert_eq!(types, vec![NAL_BLA_W_LP, NAL_BLA_W_LP]);
    }

    /// Test 1: a CRA at a MARKED non-seamless boundary is rewritten to BLA_W_LP.
    #[test]
    fn cra_at_marked_boundary_rewritten_to_bla() {
        let mut parser = HevcParser::new();
        parser.mark_clip_boundary();
        let frames = parser.parse(&make_pes(cra_au(&[0x10, 0x20, 0x30]), Some(0)));
        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe, "rewritten BLA is still a keyframe");
        let nals = nals_of(&frames[0].data);
        assert_eq!(nals.len(), 1);
        assert_eq!(
            nal_type_of(&nals[0]),
            NAL_BLA_W_LP,
            "marked-boundary CRA must be rewritten to BLA_W_LP (16)"
        );
        // The forbidden_zero_bit + layer-id-high (bit 0) and the rest of byte 0,
        // and all payload bytes, are otherwise untouched.
        assert_eq!(nals[0][0] & 0x81, hevc_nal_header(21)[0] & 0x81);
        assert_eq!(&nals[0][2..], &[0x10, 0x20, 0x30]);
        // The flag is one-shot: a SECOND CRA (no new marker) is left as CRA.
        let f2 = parser.parse(&make_pes(cra_au(&[0x40]), Some(90000)));
        assert_eq!(
            nal_type_of(&nals_of(&f2[0].data)[0]),
            NAL_CRA_NUT,
            "only the first CRA after a boundary is rewritten"
        );
    }

    /// A seamless clip join restarts CC (flagged as a gap) with continuous PTS; the next CRA's
    /// RASL stay decodable, so it must not become BLA (the decoder would drop them).
    #[test]
    fn cra_after_a_seamless_join_is_left_a_cra() {
        let mut parser = HevcParser::new();
        let mut join = make_pes(vec![0, 0, 1, 0x02, 0x01, 0x80], Some(0));
        join.discontinuity = true;
        parser.parse(&join);
        let f = parser.parse(&make_pes(cra_au(&[0x10]), Some(3000)));
        assert_eq!(
            nal_type_of(&nals_of(&f[0].data)[0]),
            NAL_CRA_NUT,
            "a CRA at a seamless join must stay a CRA"
        );
    }

    /// Whether `frames` hand a decoder a RASL picture it must decode: one that
    /// follows a CRA (not a BLA, whose RASL a decoder discards) in decode order.
    fn emits_decodable_rasl_after_cra(frames: &[Frame]) -> bool {
        let mut last_irap = None;
        for f in frames {
            let t = nal_type_of(&nals_of(&f.data)[0]);
            match t {
                16..=21 => last_irap = Some(t),
                8 | 9 if last_irap == Some(NAL_CRA_NUT) => return true,
                _ => {}
            }
        }
        false
    }

    #[test]
    #[ignore = "defect: a non-seamless join whose PTS jumps FORWARD is not detected, so its CRA stays CRA and its RASL decode against the previous clip"]
    fn a_forward_pts_join_onto_a_cra_does_not_hand_its_rasl_to_the_decoder() {
        // Clip 1: CRA + trailing pictures at ~0 s. Clip 2 starts 100 s later in
        // the source clock (e.g. a PlayItem that skips part of a clip, or a clip
        // whose STC runs ahead) with a CRA whose RASL reference a picture of
        // the skipped material.
        let mut p = HevcParser::new();
        let mut frames = Vec::new();
        frames.extend(p.parse(&make_pes(cra_au(&[0x01]), Some(0))));
        frames.extend(p.parse(&make_pes(nal_bytes(1, &[0xD0]), Some(3750))));
        frames.extend(p.parse(&make_pes(nal_bytes(1, &[0xD0]), Some(7500))));
        let t = 100 * 90_000;
        let clip2 = frames.len();
        frames.extend(p.parse(&make_pes(cra_au(&[0x02]), Some(t))));
        frames.extend(p.parse(&make_pes(nal_bytes(9, &[0xE0]), Some(t - 7500))));
        frames.extend(p.parse(&make_pes(nal_bytes(8, &[0xE0]), Some(t - 3750))));
        assert!(
            !emits_decodable_rasl_after_cra(&frames[clip2..]),
            "clip 2's RASL reference a picture this output does not hold"
        );
    }

    #[test]
    fn rasl_after_a_seamless_join_cra_stay_decodable() {
        // Characterises the seamless case: PTS continuous across the join, so the
        // RASL reference the previous clip's tail, which the output holds.
        let mut p = HevcParser::new();
        let mut frames = Vec::new();
        frames.extend(p.parse(&make_pes(cra_au(&[0x01]), Some(0))));
        frames.extend(p.parse(&make_pes(nal_bytes(1, &[0xD0]), Some(3750))));
        frames.extend(p.parse(&make_pes(cra_au(&[0x02]), Some(4 * 3750))));
        frames.extend(p.parse(&make_pes(nal_bytes(9, &[0xE0]), Some(2 * 3750))));
        assert!(
            emits_decodable_rasl_after_cra(&frames),
            "a seamless join keeps its CRA and RASL"
        );
    }

    #[test]
    #[ignore = "defect: after a gap the ResyncGate resumes on a CRA and passes its RASL, whose reference the gate dropped"]
    fn a_gap_resync_onto_a_cra_does_not_hand_its_rasl_to_the_decoder() {
        // IDR, a TRAIL_R that follows lost data, then a CRA (same clock) whose RASL
        // reference the dropped TRAIL_R. The CRA is not first in the bitstream, so
        // a decoder decodes the RASL.
        let mut p = HevcParser::new();
        let mut lost = make_pes(nal_bytes(1, &[0xD0]), Some(3750));
        lost.discontinuity = true;
        let pes = [
            make_pes(nal_bytes(19, &[0x80]), Some(0)),
            lost,
            make_pes(cra_au(&[0x02]), Some(4 * 3750)),
            make_pes(nal_bytes(9, &[0xE0]), Some(2 * 3750)),
            make_pes(nal_bytes(8, &[0xE0]), Some(3 * 3750)),
        ];
        let mut gate = crate::mux::resync::ResyncGate::new();
        let frames: Vec<Frame> = pes
            .iter()
            .flat_map(|x| p.parse(x))
            .filter(|f| gate.admit(true, f.discontinuity, f.keyframe))
            .collect();
        assert!(
            !emits_decodable_rasl_after_cra(&frames),
            "the RASL reference the picture the gate dropped"
        );
    }

    #[test]
    #[ignore = "defect: pictures before the stream's first IRAP are emitted with no reference"]
    fn pictures_before_the_first_irap_are_dropped() {
        let mut p = HevcParser::new();
        let mut frames = Vec::new();
        frames.extend(p.parse(&make_pes(nal_bytes(1, &[0xD0]), Some(0))));
        frames.extend(p.parse(&make_pes(cra_au(&[0x01]), Some(3750))));
        assert!(frames[0].keyframe, "the first emitted picture is the IRAP");
    }

    /// Test 2: a CRA with NO boundary marker is left unchanged (CRA stays CRA).
    #[test]
    fn cra_without_boundary_unchanged() {
        let mut parser = HevcParser::new();
        let frames = parser.parse(&make_pes(cra_au(&[0x10, 0x20]), Some(0)));
        let nals = nals_of(&frames[0].data);
        assert_eq!(
            nal_type_of(&nals[0]),
            NAL_CRA_NUT,
            "an unmarked CRA must remain a CRA"
        );
    }

    // Regression (UHD Dolby Vision title): must AUTO-DETECT a non-seamless clip boundary from a
    // backward PES-PTS reset, no `mark_clip_boundary` call.
    #[test]
    fn cra_at_auto_detected_pts_backstep_rewritten_to_bla() {
        let mut parser = HevcParser::new();
        // Clip 1: a CRA then a few trailing frames advancing the PTS watermark.
        // PTS in 90 kHz ticks: 0, then ~1 h into the clip.
        let one_hour = 90_000i64 * 3600;
        parser.parse(&make_pes(cra_au(&[0x01]), Some(0)));
        parser.parse(&make_pes(cra_au(&[0x02]), Some(one_hour)));
        // In-clip B-frame dip: PTS steps back a few frames (< BACKSTEP_TICKS).
        // Must NOT be mistaken for a clip boundary — this CRA stays CRA.
        let dip = parser.parse(&make_pes(cra_au(&[0x03]), Some(one_hour - 3 * 3750)));
        assert_eq!(
            nal_type_of(&nals_of(&dip[0].data)[0]),
            NAL_CRA_NUT,
            "a sub-threshold B-frame PTS dip must not trigger the rewrite"
        );
        // Clip 2 splice: PES PTS resets to a new clip base far below the
        // watermark (> BACKSTEP_TICKS backward). The opening CRA is rewritten.
        let splice = parser.parse(&make_pes(cra_au(&[0x04]), Some(0)));
        assert_eq!(
            nal_type_of(&nals_of(&splice[0].data)[0]),
            NAL_BLA_W_LP,
            "the splice CRA after a backward PTS reset must become BLA_W_LP"
        );
        // One-shot: the NEXT clip-2 CRA (PTS advancing again) stays CRA.
        let next = parser.parse(&make_pes(cra_au(&[0x05]), Some(90_000)));
        assert_eq!(
            nal_type_of(&nals_of(&next[0].data)[0]),
            NAL_CRA_NUT,
            "only the first CRA after the boundary is rewritten"
        );
    }

    // Regression (rc.5.2 audit #1): a SINGLE clip crossing the 2^33 PTS wrap must not be
    // mistaken for a non-seamless clip join.
    #[test]
    fn cra_after_33bit_pts_wrap_not_rewritten() {
        let mut parser = HevcParser::new();
        let period = 1i64 << 33;
        // Single clip, PTS climbing toward the 33-bit wrap. Start just below 2^33.
        let near_wrap = period - 90_000; // ~1 s before the wrap point
        parser.parse(&make_pes(cra_au(&[0x01]), Some(near_wrap)));
        parser.parse(&make_pes(cra_au(&[0x02]), Some(near_wrap + 3750)));
        // The counter wraps: raw PTS resets to a small value, but this is the
        // SAME continuous clip, one frame later. A naive raw comparison sees a
        // ~2^33 backward step and false-arms the boundary.
        let wrapped = parser.parse(&make_pes(cra_au(&[0x03]), Some(7500)));
        assert_eq!(
            nal_type_of(&nals_of(&wrapped[0].data)[0]),
            NAL_CRA_NUT,
            "a CRA whose PTS merely wrapped 2^33->0 must stay CRA, not become BLA"
        );
        // Continue past the wrap: PTS keeps climbing from the new low base; still
        // one continuous clip, the CRA after must remain CRA.
        let after = parser.parse(&make_pes(cra_au(&[0x04]), Some(11250)));
        assert_eq!(
            nal_type_of(&nals_of(&after[0].data)[0]),
            NAL_CRA_NUT,
            "post-wrap in-clip CRA must stay CRA"
        );
    }

    // The wrap-vs-backstep test above doesn't distinguish `-` from a hand-flipped `+`; this
    // uses PTS ~3e9 where they diverge.
    #[test]
    fn cra_splice_detected_at_large_pts_magnitude_not_masked_by_wrap_logic() {
        let mut parser = HevcParser::new();
        // Clip 1: two ordinary forward-progressing frames at ~3e9 ticks
        // (order 2^32, comfortably below PTS_WRAP_PERIOD/2 = 2^32 exactly,
        // and far from the actual 2^33 wrap point).
        let clip1_base = 3_000_000_000i64;
        parser.parse(&make_pes(cra_au(&[0x01]), Some(clip1_base)));
        let dip = parser.parse(&make_pes(cra_au(&[0x02]), Some(clip1_base + 3750)));
        assert_eq!(
            nal_type_of(&nals_of(&dip[0].data)[0]),
            NAL_CRA_NUT,
            "ordinary forward progression at large PTS magnitude must not itself \
             be mistaken for anything"
        );
        // Clip 2 splice: PES PTS resets to a small new-clip base — a genuine,
        // large (~3e9-tick) backward step that is NOT a 2^33 wrap (the
        // backward delta here is far short of PTS_WRAP_PERIOD/2).
        let splice = parser.parse(&make_pes(cra_au(&[0x03]), Some(500)));
        assert_eq!(
            nal_type_of(&nals_of(&splice[0].data)[0]),
            NAL_BLA_W_LP,
            "a genuine large backward PTS reset at this magnitude must still be \
             detected as a clip splice and rewrite the CRA to BLA_W_LP"
        );
        assert_eq!(
            splice[0].pts_ns,
            pts_to_ns(500),
            "the emitted PTS must be the raw splice-clip PTS, unaffected by the \
             internal unwrap bookkeeping"
        );
    }

    // Non-CRA NALs are never rewritten even when a boundary IS marked.
    #[test]
    fn non_cra_nals_never_rewritten_at_boundary() {
        // IDR boundary: marker set, but the first IRAP is an IDR → no rewrite,
        // and the marker is consumed so a later CRA is untouched.
        let mut parser = HevcParser::new();
        parser.mark_clip_boundary();
        let mut idr = vec![0x00, 0x00, 0x01];
        idr.extend_from_slice(&hevc_nal_header(19)); // IDR_W_RADL
        idr.extend_from_slice(&[0x10]);
        let f = parser.parse(&make_pes(idr, Some(0)));
        assert_eq!(
            nal_type_of(&nals_of(&f[0].data)[0]),
            19,
            "IDR at a marked boundary must stay IDR"
        );
        // Marker was consumed by the IDR: a following CRA is NOT rewritten.
        let f2 = parser.parse(&make_pes(cra_au(&[0x20]), Some(90000)));
        assert_eq!(
            nal_type_of(&nals_of(&f2[0].data)[0]),
            NAL_CRA_NUT,
            "the IDR consumed the boundary marker; later CRA stays CRA"
        );

        // RASL leading pictures (types 8/9) preceding the splice CRA must not be
        // touched and must not consume the marker — only the CRA itself does.
        let mut parser = HevcParser::new();
        parser.mark_clip_boundary();
        let mut au = vec![0x00, 0x00, 0x01];
        au.extend_from_slice(&hevc_nal_header(8)); // RASL_N
        au.extend_from_slice(&[0xAA]);
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&hevc_nal_header(9)); // RASL_R
        au.extend_from_slice(&[0xBB]);
        au.extend_from_slice(&cra_au(&[0xCC])); // CRA after the RASLs
        let f = parser.parse(&make_pes(au, Some(0)));
        let nals = nals_of(&f[0].data);
        let types: Vec<u8> = nals.iter().map(|n| nal_type_of(n)).collect();
        assert_eq!(
            types,
            vec![8, 9, NAL_BLA_W_LP],
            "RASLs pass through untouched; the CRA (after them) becomes BLA"
        );
    }

    // A stream with NO boundary marker is BYTE-IDENTICAL to pre-feature behaviour (UHD-safety
    // guarantee).
    #[test]
    fn no_boundary_marker_is_byte_identical() {
        let build = || {
            let mut d = Vec::new();
            // AU0: VPS/SPS/PPS + CRA keyframe.
            d.extend_from_slice(&[0x00, 0x00, 0x01]);
            d.extend_from_slice(&hevc_nal_header(32));
            d.extend_from_slice(&[0xAA]);
            d.extend_from_slice(&[0x00, 0x00, 0x01]);
            d.extend_from_slice(&hevc_nal_header(33));
            d.extend_from_slice(&[0xBB, 0xCC, 0xDD]);
            d.extend_from_slice(&[0x00, 0x00, 0x01]);
            d.extend_from_slice(&hevc_nal_header(34));
            d.extend_from_slice(&[0xEE]);
            d.extend_from_slice(&cra_au(&[0x11, 0x22]));
            d
        };
        // Reference parser: the rewrite field exists but is never marked, so its
        // output is exactly pre-feature behaviour — compared against a second
        // never-marked run and the invariant that CRA is emitted as-is (type 21).
        let mut a = HevcParser::new();
        let mut b = HevcParser::new();
        let fa = a.parse(&make_pes(build(), Some(0)));
        let fb = b.parse(&make_pes(build(), Some(0)));
        assert_eq!(fa.len(), 1);
        assert_eq!(fa[0].data, fb[0].data, "never-marked output must be stable");
        // And the CRA was NOT converted (type 21 still present, no BLA).
        let types: Vec<u8> = nals_of(&fa[0].data)
            .iter()
            .map(|n| nal_type_of(n))
            .collect();
        assert!(
            types.contains(&NAL_CRA_NUT) && !types.contains(&NAL_BLA_W_LP),
            "unmarked stream must keep its CRA (no BLA), got {types:?}"
        );

        // Feed a second AU (a CRA) to the same unmarked parser: still a CRA.
        // Param sets are re-asserted ahead of the keyframe, so locate the CRA
        // among the emitted NALs rather than assuming it is first.
        let f2 = a.parse(&make_pes(cra_au(&[0x33]), Some(90000)));
        let t2: Vec<u8> = nals_of(&f2[0].data)
            .iter()
            .map(|n| nal_type_of(n))
            .collect();
        assert!(
            t2.contains(&NAL_CRA_NUT) && !t2.contains(&NAL_BLA_W_LP),
            "unmarked mid-stream CRA must never become BLA, got {t2:?}"
        );
    }

    // A SEAMLESS boundary is expressed by NOT calling `mark_clip_boundary`, so a CRA across a
    // seamless join stays unchanged.
    #[test]
    fn seamless_boundary_no_rewrite() {
        // Simulate two clips joined seamlessly: the caller does NOT mark, so the
        // second clip's opening CRA stays a CRA.
        let mut parser = HevcParser::new();
        // Clip 1 ends with a CRA (no marker — mid-content).
        let f1 = parser.parse(&make_pes(cra_au(&[0x01]), Some(0)));
        assert_eq!(nal_type_of(&nals_of(&f1[0].data)[0]), NAL_CRA_NUT);
        // Seamless join: caller deliberately does NOT call mark_clip_boundary().
        // Clip 2 opens with a CRA → must remain a CRA.
        let f2 = parser.parse(&make_pes(cra_au(&[0x02]), Some(90000)));
        assert_eq!(
            nal_type_of(&nals_of(&f2[0].data)[0]),
            NAL_CRA_NUT,
            "a seamless join (no marker) must never rewrite the CRA"
        );
    }

    // --- non-IRAP (trailing) → not keyframe ---

    #[test]
    fn parse_trailing_not_keyframe() {
        let mut parser = HevcParser::new();

        // TRAIL_R = type 1
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(1));
        data.extend_from_slice(&[0x10, 0x20, 0x30]);

        let pes = make_pes(data, Some(180000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(
            !frames[0].keyframe,
            "TRAIL_R (type 1) should not be keyframe"
        );
    }

    #[test]
    fn parse_tsa_not_keyframe() {
        let mut parser = HevcParser::new();

        // TSA_N = type 2
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(2));
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "TSA_N (type 2) should not be keyframe");
    }

    // --- VPS/SPS/PPS stripped from frame data ---

    #[test]
    fn param_sets_seed_codecprivate_and_reassert_at_keyframe() {
        let mut parser = HevcParser::new();

        let mut data = Vec::new();
        // VPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA]);
        // SPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0xBB]);
        // PPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xCC]);
        // IDR slice
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let idr_hdr = hevc_nal_header(19);
        data.extend_from_slice(&idr_hdr);
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);

        // The param sets seed codecPrivate (hvcC).
        assert!(
            parser.codec_private().is_some(),
            "VPS/SPS/PPS must seed codecPrivate"
        );

        // Because this is a keyframe, the active VPS/SPS/PPS are ALSO re-asserted
        // in-band ahead of the IDR so the keyframe is self-contained. Frame data
        // = VPS, SPS, PPS, IDR (4 length-prefixed NALs, in that order).
        let fd = &frames[0].data;
        let mut types = Vec::new();
        let mut o = 0;
        while o + 4 <= fd.len() {
            let len = u32::from_be_bytes([fd[o], fd[o + 1], fd[o + 2], fd[o + 3]]) as usize;
            o += 4;
            types.push((fd[o] >> 1) & 0x3F);
            o += len;
        }
        assert_eq!(
            types,
            vec![32, 33, 34, 19],
            "keyframe must re-assert VPS/SPS/PPS in-band ahead of the IDR slice"
        );
    }

    // --- parameter-set redefinition (mid-title redefinition bug) ---

    // An IRAP whose SPS changed but whose PPS did not: the re-asserted PPS must follow
    // the new SPS, or the decoder parses it against the old one and drops it.
    #[test]
    fn reasserted_pps_follows_a_redefined_sps() {
        let mut parser = HevcParser::new();
        let nal = |t: u8, body: u8| {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.extend_from_slice(&[body, body]);
            v
        };
        let au = |sps: u8| {
            let mut d = nal(32, 0x11); // VPS
            d.extend(nal(33, sps)); // SPS
            d.extend(nal(34, 0x33)); // PPS
            d.extend(nal(19, 0x44)); // IDR_W_RADL
            d
        };
        let types = |fd: &[u8]| {
            let (mut t, mut o) = (Vec::new(), 0usize);
            while o + 4 <= fd.len() {
                let len = u32::from_be_bytes([fd[o], fd[o + 1], fd[o + 2], fd[o + 3]]) as usize;
                o += 4;
                t.push((fd[o] >> 1) & 0x3F);
                o += len;
            }
            t
        };
        parser.parse(&make_pes(au(0x22), Some(0)));
        let f = parser.parse(&make_pes(au(0x23), Some(1)));
        assert_eq!(types(&f[0].data), vec![32, 33, 34, 19]);
    }

    // A parameter set REDEFINED mid-stream must be emitted INLINE so the decoder re-activates
    // it.
    #[test]
    fn redefined_pps_emitted_inline() {
        let mut parser = HevcParser::new();
        let pps = |body: u8| {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(34)); // PPS
            v.extend_from_slice(&[body, body]);
            v
        };
        let slice = || {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(1)); // TRAIL_R
            v.extend_from_slice(&[0x10, 0x20]);
            v
        };
        // count PPS (type 34) NALs in length-prefixed frame data
        let count_pps = |fd: &[u8]| {
            let (mut n, mut o) = (0usize, 0usize);
            while o + 4 <= fd.len() {
                let len = u32::from_be_bytes([fd[o], fd[o + 1], fd[o + 2], fd[o + 3]]) as usize;
                o += 4;
                if o < fd.len() && (fd[o] >> 1) & 0x3F == 34 {
                    n += 1;
                }
                o += len;
            }
            n
        };

        // PES1: first PPS-A → seeds codecPrivate, stripped from frame.
        let mut d = pps(0xAA);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(0)));
        assert_eq!(count_pps(&f[0].data), 0, "first PPS goes to codecPrivate");

        // PES2: PPS-B (redefinition, different body) → emitted INLINE.
        let mut d = pps(0xBB);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(1)));
        assert_eq!(count_pps(&f[0].data), 1, "redefined PPS must be inline");

        // PES3: PPS-B repeated on a non-keyframe slice — B is already active, so
        // this carries no change and is stripped. Re-assertion for hvcC-reapplying
        // players is handled by `reassert_active` at keyframes, not TRAIL_R slices.
        let mut d = pps(0xBB);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(2)));
        assert_eq!(
            count_pps(&f[0].data),
            0,
            "PPS equal to the active set carries no change → stripped"
        );

        // PES4: back to PPS-A. Active set is B, so switching to A is a real
        // change and must be emitted in-band — a streaming decoder sitting on B
        // would never revert otherwise (the old `== codecPrivate -> strip` rule dropped this).
        let mut d = pps(0xAA);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(3)));
        assert_eq!(
            count_pps(&f[0].data),
            1,
            "switch back to the codecPrivate body is a change → emitted in-band"
        );
    }

    // --- empty NAL between adjacent start codes is skipped ---

    #[test]
    fn empty_nal_between_start_codes_emits_no_bare_prefix() {
        // `00 00 01 00 00 01 <real NAL>`: two adjacent start codes leave the
        // in-between NAL empty after the trailing-zero strip. It must be skipped,
        // not written as a bare 0x00000000 length prefix (malformed to a decoder).
        let mut parser = HevcParser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]); // start code, empty NAL
        data.extend_from_slice(&[0x00, 0x00, 0x01]); // next start code
        data.extend_from_slice(&hevc_nal_header(1)); // TRAIL_R
        data.extend_from_slice(&[0x10, 0x20]);

        let frames = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        let fd = &frames[0].data;
        // Exactly one length-prefixed NAL — no zero-length entry.
        let len = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]) as usize;
        assert!(len > 0, "no bare zero-length prefix emitted");
        assert_eq!(len + 4, fd.len(), "exactly one NAL in frame data");
    }

    // The trailing-zero strip on the LAST NAL (no start code follows, `next`
    // falls back to `data.len()`) must not walk `end` out of bounds. Models
    // a damaged/zero-padded trailing sector on disc.
    #[test]
    fn trailing_zero_strip_on_last_nal_does_not_run_past_the_buffer() {
        let mut parser = HevcParser::new();
        let mut data = vec![0x00, 0x00, 0x01];
        data.extend_from_slice(&hevc_nal_header(NAL_AUD));
        data.push(0x00); // damaged/zero-padded trailing byte, no start code follows
        let frames = parser.parse(&make_pes(data, Some(0)));
        // AUD is dropped and the payload is otherwise empty once the padding
        // is stripped, so there is nothing to emit — the assertion that
        // matters is that `parse` returned at all instead of panicking.
        assert!(frames.is_empty());
    }

    // --- empty PES ---

    #[test]
    fn parse_empty_pes() {
        let mut parser = HevcParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion() {
        let mut parser = HevcParser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(1));
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // --- PTS (presentation), not DTS, drives the MKV block timecode ---
    // Regression for B-frame presentation: writing DTS as the block timecode
    // presents frames in decode order (visible judder) and breaks seeking.

    #[test]
    fn pts_preferred_over_dts() {
        let mut parser = HevcParser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(1)); // TRAIL_R slice
        data.extend_from_slice(&[0x10, 0x20]);

        let pes = PesPacket {
            source: None,
            pid: 0x1011,
            pts: Some(180000), // 2 s (presentation)
            dts: Some(90000),  // 1 s (decode)
            data,
            discontinuity: false,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].pts_ns, 2_000_000_000,
            "block timecode must be PTS"
        );
    }

    // --- Dolby Vision enhancement layer ---

    #[test]
    fn dv_rpu_nal_preserved() {
        // Dolby Vision enhancement layer streams contain RPU (Reference Processing
        // Unit) metadata as NAL type 62 (UNSPEC62). The HEVC parser must pass these
        // through to the frame data — only VPS/SPS/PPS/AUD are stripped.
        let mut parser = HevcParser::new();

        let mut data = Vec::new();

        // VPS (type 32) — should be stripped from frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA, 0xBB]);

        // SPS (type 33) — should be stripped from frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);

        // PPS (type 34) — should be stripped from frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD, 0xEE]);

        // IDR_W_RADL slice (type 19) — should appear in frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let idr_hdr = hevc_nal_header(19);
        data.extend_from_slice(&idr_hdr);
        data.extend_from_slice(&[0x10, 0x20, 0x30]);

        // Dolby Vision RPU (type 62 = UNSPEC62) — MUST appear in frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        let rpu_hdr = hevc_nal_header(62);
        data.extend_from_slice(&rpu_hdr);
        let rpu_payload = [0xF0, 0xF1, 0xF2, 0xF3, 0xF4];
        data.extend_from_slice(&rpu_payload);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1, "should produce one frame");
        assert!(frames[0].keyframe, "IDR should mark keyframe");

        // Verify the frame data contains both the IDR NAL and the RPU NAL.
        // Frame data is length-prefixed NALUs (4-byte big-endian length + NAL bytes).
        let fd = &frames[0].data;

        // Walk the length-prefixed NALUs and collect their types
        let mut nal_types = Vec::new();
        let mut offset = 0;
        while offset + 4 <= fd.len() {
            let length =
                u32::from_be_bytes([fd[offset], fd[offset + 1], fd[offset + 2], fd[offset + 3]])
                    as usize;
            offset += 4;
            assert!(offset + length <= fd.len(), "NAL length exceeds frame data");
            let nal_type = (fd[offset] >> 1) & 0x3F;
            nal_types.push(nal_type);
            offset += length;
        }

        assert!(
            nal_types.contains(&19),
            "frame data must contain IDR NAL (type 19), got: {:?}",
            nal_types
        );
        assert!(
            nal_types.contains(&62),
            "frame data must contain Dolby Vision RPU NAL (type 62), got: {:?}",
            nal_types
        );
        // Self-contained keyframe: VPS/SPS/PPS re-assert ahead of the IDR, giving
        // VPS,SPS,PPS,IDR,RPU. The RPU (type 62) is preserved (never stripped) —
        // only duplicate-suppression of unchanged param sets is lifted at keyframes.
        assert_eq!(
            nal_types,
            vec![32, 33, 34, 19, 62],
            "keyframe carries re-asserted param sets + IDR + preserved RPU, got: {:?}",
            nal_types
        );

        // Verify RPU payload is intact
        let mut offset = 0;
        while offset + 4 <= fd.len() {
            let length =
                u32::from_be_bytes([fd[offset], fd[offset + 1], fd[offset + 2], fd[offset + 3]])
                    as usize;
            offset += 4;
            let nal_type = (fd[offset] >> 1) & 0x3F;
            if nal_type == 62 {
                // NAL = 2-byte header + payload
                let nal_payload = &fd[offset + 2..offset + length];
                assert_eq!(
                    nal_payload, &rpu_payload,
                    "RPU payload must be preserved verbatim"
                );
            }
            offset += length;
        }
    }

    // --- hvcC chroma / bit-depth from SPS ---

    /// MSB-first bit writer for building a test SPS RBSP.
    struct BitWriter {
        bytes: Vec<u8>,
        nbits: usize,
    }
    impl BitWriter {
        fn new() -> Self {
            Self {
                bytes: Vec::new(),
                nbits: 0,
            }
        }
        fn put_bit(&mut self, b: u32) {
            if self.nbits.is_multiple_of(8) {
                self.bytes.push(0);
            }
            if b & 1 != 0 {
                let i = self.nbits / 8;
                let shift = 7 - (self.nbits % 8);
                self.bytes[i] |= 1 << shift;
            }
            self.nbits += 1;
        }
        fn put_bits(&mut self, v: u32, n: u32) {
            for i in (0..n).rev() {
                self.put_bit((v >> i) & 1);
            }
        }
        fn put_ue(&mut self, v: u32) {
            let val = v + 1;
            let bits = 32 - val.leading_zeros();
            for _ in 0..bits - 1 {
                self.put_bit(0);
            }
            for i in (0..bits).rev() {
                self.put_bit((val >> i) & 1);
            }
        }
    }

    /// Build a stored SPS NAL ([2-byte header][RBSP]) with the given
    /// chroma_format_idc and bit depths, max_sub_layers_minus1 = 0.
    fn make_sps_with_chroma(chroma_idc: u32, bd_luma_m8: u32, bd_chroma_m8: u32) -> Vec<u8> {
        let mut w = BitWriter::new();
        w.put_bits(0, 4); // sps_video_parameter_set_id
        w.put_bits(0, 3); // sps_max_sub_layers_minus1 = 0
        w.put_bit(1); // sps_temporal_id_nesting_flag
        // general profile_tier_level: 96 bits (12 bytes) of zeros is fine here.
        for _ in 0..96 {
            w.put_bit(0);
        }
        w.put_ue(0); // sps_seq_parameter_set_id
        w.put_ue(chroma_idc); // chroma_format_idc
        if chroma_idc == 3 {
            w.put_bit(0); // separate_colour_plane_flag
        }
        w.put_ue(3840); // pic_width_in_luma_samples
        w.put_ue(2160); // pic_height_in_luma_samples
        w.put_bit(0); // conformance_window_flag = 0
        w.put_ue(bd_luma_m8); // bit_depth_luma_minus8
        w.put_ue(bd_chroma_m8); // bit_depth_chroma_minus8

        let mut sps = hevc_nal_header(33).to_vec();
        sps.extend_from_slice(&w.bytes);
        sps
    }

    #[test]
    fn sps_chroma_rejects_out_of_range_values() {
        assert!(parse_sps_chroma(&make_sps_with_chroma(259, 0, 0)).is_none());
        assert!(parse_sps_chroma(&make_sps_with_chroma(1, 259, 0)).is_none());
    }

    fn codec_private_from_sps(sps_nal: &[u8]) -> Vec<u8> {
        let mut parser = HevcParser::new();
        // VPS + the given SPS + PPS, all length-prefixed in one PES.
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA, 0xBB]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(sps_nal);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD, 0xEE]);
        parser.parse(&make_pes(data, Some(0)));
        parser.codec_private().expect("codec_private")
    }

    #[test]
    fn hvcc_emits_10bit_420_from_sps() {
        // Main 10 UHD: chroma_format_idc=1 (4:2:0), bit depths = 10 (minus8 = 2).
        let sps = make_sps_with_chroma(1, 2, 2);
        let cp = codec_private_from_sps(&sps);
        // chromaFormat at cp[16], bit depths at cp[17]/cp[18].
        assert_eq!(cp[16], 0xFC | 1, "chroma_format_idc = 1 (4:2:0)");
        assert_eq!(cp[17], 0xF8 | 2, "bit_depth_luma_minus8 = 2 (10-bit)");
        assert_eq!(cp[18], 0xF8 | 2, "bit_depth_chroma_minus8 = 2 (10-bit)");
    }

    #[test]
    fn hvcc_emits_8bit_420_from_sps() {
        // 8-bit 4:2:0 must still report correctly (not a regression).
        let sps = make_sps_with_chroma(1, 0, 0);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 1);
        assert_eq!(cp[17], 0xF8);
        assert_eq!(cp[18], 0xF8);
    }

    #[test]
    fn hvcc_emits_444_12bit_from_sps() {
        // 4:4:4 (idc=3) with 12-bit depth (minus8 = 4).
        let sps = make_sps_with_chroma(3, 4, 4);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 3, "chroma_format_idc = 3 (4:4:4)");
        assert_eq!(cp[17], 0xF8 | 4, "bit_depth_luma_minus8 = 4 (12-bit)");
        assert_eq!(cp[18], 0xF8 | 4);
    }

    #[test]
    fn hvcc_byte21_from_sps_temporal_layers() {
        // make_sps_with_chroma sets sub_layers_minus1=0, nesting_flag=1, so byte
        // 21 = numTemporalLayers=1, temporalIdNested=1, lengthSizeMinusOne=3:
        // (1<<3)|(1<<2)|3 = 0x0F.
        let sps = make_sps_with_chroma(1, 2, 2);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(
            cp[21], 0x0F,
            "byte 21: numTemporalLayers=1, temporalIdNested=1, lengthSizeMinusOne=3"
        );
    }

    // ── issue #52 (Fix A): length-prefix self-consistency ───────────────────

    #[test]
    fn hvcc_length_size_matches_push_length_prefixed_width() {
        // Regression: hvcC byte-21 lengthSizeMinusOne+1 (low 2 bits) MUST equal the
        // prefix width push_length_prefixed writes (LENGTH_PREFIX_SIZE); a divergence
        // is the framing desync reported as "Invalid NAL unit size".
        let sps = make_sps_with_chroma(1, 2, 2);
        let cp = codec_private_from_sps(&sps);
        let length_size_minus_one = (cp[21] & 0x03) as usize;
        assert_eq!(
            length_size_minus_one + 1,
            LENGTH_PREFIX_SIZE,
            "hvcC lengthSizeMinusOne+1 must equal the NAL length-prefix width"
        );
        assert_eq!(LENGTH_PREFIX_SIZE, 4, "HEVC length prefix is 4-byte BE");
    }

    #[test]
    fn length_prefix_tiles_accepts_well_formed_records() {
        // Two records: len 3 + len 2, back to back, tiling the buffer exactly.
        let mut buf = Vec::new();
        push_length_prefixed(&mut buf, &[0xAA, 0xBB, 0xCC]);
        push_length_prefixed(&mut buf, &[0x11, 0x22]);
        assert!(length_prefix_tiles(&buf));
        assert!(length_prefix_tiles(&[]), "an empty buffer trivially tiles");
    }

    #[test]
    fn length_prefix_tiles_rejects_a_desynced_frame() {
        // A declared length that OVERRUNS the buffer — the shape that surfaces as
        // "Invalid NAL unit size (N>M)". Declared 0x1000_0001 bytes, only a few
        // present.
        let overrun = [0x10, 0x00, 0x00, 0x01, 0xAA, 0xBB, 0xCC];
        assert!(
            !length_prefix_tiles(&overrun),
            "a length that overruns the buffer must be rejected"
        );
        // Trailing garbage past the last complete record.
        let mut trailing = Vec::new();
        push_length_prefixed(&mut trailing, &[0xAA, 0xBB]);
        trailing.push(0x99); // one stray byte, not a full length field
        assert!(
            !length_prefix_tiles(&trailing),
            "trailing bytes that don't form a record must be rejected"
        );
        // A zero-length record (empty NALU) is structurally invalid.
        let zero_len = [0x00, 0x00, 0x00, 0x00];
        assert!(
            !length_prefix_tiles(&zero_len),
            "a zero-length record must be rejected"
        );
        // A truncated length prefix (fewer than 4 bytes at the tail).
        assert!(
            !length_prefix_tiles(&[0x00, 0x00]),
            "a truncated length prefix must be rejected"
        );
    }

    #[test]
    fn parsed_annex_b_access_unit_tiles_exactly_and_is_emitted() {
        // Positive path: a normal Annex-B AU (VPS + SPS + PPS + IDR slice) parses
        // to a frame whose length-prefixed data tiles EXACTLY, so the guard emits
        // it (never dropped).
        let mut parser = HevcParser::new();
        let sps = make_sps_with_chroma(1, 2, 2);
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32)); // VPS
        data.extend_from_slice(&[0xAA, 0xBB]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&sps); // SPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34)); // PPS
        data.extend_from_slice(&[0xDD, 0xEE]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(19)); // IDR_W_RADL slice → keyframe
        data.extend_from_slice(&[0x10, 0x20, 0x30]);

        let frames = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1, "the access unit is emitted, not dropped");
        assert!(
            length_prefix_tiles(&frames[0].data),
            "an emitted HEVC frame's records must tile exactly"
        );
    }

    // The build a normal Annex-B AU (VPS + SPS + PPS + IDR) as a helper for the
    // end-to-end guard test below.
    #[cfg(test)]
    fn annex_b_access_unit() -> Vec<u8> {
        let sps = make_sps_with_chroma(1, 2, 2);
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32)); // VPS
        data.extend_from_slice(&[0xAA, 0xBB]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&sps); // SPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34)); // PPS
        data.extend_from_slice(&[0xDD, 0xEE]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(19)); // IDR_W_RADL slice → keyframe
        data.extend_from_slice(&[0x10, 0x20, 0x30]);
        data
    }

    // Issue #52: the guard's WIRING in parse() — a desynced access unit is
    // dropped (empty emit), a well-formed one is emitted. Disconnecting the guard
    // from parse() would let the desynced case through and fail this test.
    #[test]
    fn parse_drops_desynced_access_unit() {
        // Well-formed: emitted.
        let mut parser = HevcParser::new();
        let frames = parser.parse(&make_pes(annex_b_access_unit(), Some(0)));
        assert_eq!(frames.len(), 1, "a well-formed access unit is emitted");

        // Same AU, framing forced out of sync: the guard drops it.
        GUARD_DROPS.with(|c| c.set(0));
        FORCE_FRAMING_DESYNC.with(|c| c.set(true));
        let mut parser2 = HevcParser::new();
        let dropped = parser2.parse(&make_pes(annex_b_access_unit(), Some(0)));
        FORCE_FRAMING_DESYNC.with(|c| c.set(false));
        assert!(
            dropped.is_empty(),
            "a desynced access unit is dropped by parse(), not emitted"
        );
        assert_eq!(GUARD_DROPS.with(|c| c.get()), 1);
    }

    // The hook above is the only way to fire the guard: over arbitrary Annex-B input
    // (random NAL types, zero runs, param-set churn, empty NALs) every emitted frame
    // tiles and the guard never drops, so it stays pure defense in depth.
    #[test]
    fn guard_never_fires_on_arbitrary_annex_b_input() {
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        GUARD_DROPS.with(|c| c.set(0));
        let mut parser = HevcParser::new();
        for au in 0..2000 {
            let mut data = Vec::new();
            for _ in 0..(next() % 6) {
                data.extend_from_slice(if next() % 2 == 0 {
                    &[0, 0, 1]
                } else {
                    &[0, 0, 0, 1]
                });
                let r = next();
                let nal_type =
                    [NAL_VPS, NAL_SPS, NAL_PPS, NAL_AUD, 1, 19, 21, 39][(r % 8) as usize];
                data.extend_from_slice(&hevc_nal_header(nal_type));
                for _ in 0..(next() % 12) {
                    data.push([0x00, 0x03, 0xFF, (next() & 0xFF) as u8][(next() % 4) as usize]);
                }
            }
            for f in parser.parse(&make_pes(data, Some(au * 3750))) {
                assert!(length_prefix_tiles(&f.data), "au {au} does not tile");
            }
        }
        assert_eq!(
            GUARD_DROPS.with(|c| c.get()),
            0,
            "the guard fired on real input"
        );
    }

    #[test]
    fn hvcc_handles_emulation_prevention_in_sps() {
        // Insert emulation-prevention bytes (00 00 03) after the parsed fields
        // in a 10-bit 4:2:0 SPS RBSP tail and confirm the chroma/bit-depth
        // parse still lands right — the strip must not corrupt earlier bits.
        let mut sps = make_sps_with_chroma(1, 2, 2);
        // Append a benign 00 00 03 sequence to the RBSP.
        sps.extend_from_slice(&[0x00, 0x00, 0x03, 0x00]);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 1);
        assert_eq!(cp[17], 0xF8 | 2);
        assert_eq!(cp[18], 0xF8 | 2);
    }

    // PTL and the SPS fields are read off the emulation-prevention-STRIPPED RBSP; a real
    // UHD SPS carries `00 00 03` inside general_constraint_indicator_flags.
    #[test]
    fn hvcc_reads_ptl_and_chroma_through_emulation_prevention_in_the_ptl() {
        let sps = make_sps_with_chroma(1, 2, 2);
        let mut rbsp = sps[2..].to_vec();
        rbsp[1] = 0x21; // profile
        rbsp[2] = 0x60; // compatibility flags 0x60000000
        rbsp[6] = 0x90; // constraint flags 0x900000000000
        rbsp[12] = 0x7B; // level_idc
        let escaped = emulation_prevent(&rbsp);
        assert!(escaped.len() > rbsp.len(), "the zero runs were escaped");
        let mut stored = sps[..2].to_vec();
        stored.extend_from_slice(&escaped);

        assert_eq!(parse_sps_chroma(&stored).unwrap().chroma_format_idc, 1);
        let cp = codec_private_from_sps(&stored);
        assert_eq!(cp[1], 0x21);
        assert_eq!(&cp[2..6], &[0x60, 0, 0, 0]);
        assert_eq!(&cp[6..12], &[0x90, 0, 0, 0, 0, 0]);
        assert_eq!(cp[12], 0x7B);
        assert_eq!(cp[16], 0xFC | 1);
        assert_eq!(cp[17], 0xF8 | 2);
        assert_eq!(cp[18], 0xF8 | 2);
    }

    // The gap flag on a PES must reach the frame (both emit paths), or the resync gate never
    // arms for HEVC.
    #[test]
    fn pes_discontinuity_propagates_to_frame() {
        for reorder in [false, true] {
            for flag in [true, false] {
                let mut pes = make_pes(cra_au(&[0x10]), Some(0));
                pes.discontinuity = flag;
                let mut parser = HevcParser::new().with_ps_reorder(reorder);
                let mut frames = parser.parse(&pes);
                frames.extend(parser.flush());
                assert_eq!(frames.len(), 1, "reorder {reorder}");
                assert_eq!(frames[0].discontinuity, flag, "reorder {reorder}");
            }
        }
    }

    // Other SEI messages sharing the NAL (or an HDR10 message already captured) are skipped by
    // their declared size, so the wanted message behind them is still found.
    #[test]
    fn hdr10_sei_is_found_behind_other_and_already_captured_messages() {
        let idr = nal_bytes(19, &[0xEC]);
        let pps = nal_bytes(NAL_PPS, &[0xC0]);
        let mastering = |max_lum| {
            sei_message(
                SEI_MASTERING_DISPLAY_COLOUR_VOLUME,
                &mastering_payload([1, 2, 3], [4, 5, 6], 7, 8, max_lum, 10),
            )
        };
        let cll = sei_message(SEI_CONTENT_LIGHT_LEVEL_INFO, &cll_payload(1000, 400));
        let other = |n: usize| sei_message(1, &vec![0x5A; n]);

        let mut one_nal = HevcParser::new();
        let mut au = pps.clone();
        au.extend_from_slice(&sei_nal(&[
            other(5),
            mastering(10_000_000),
            other(3),
            cll.clone(),
        ]));
        au.extend_from_slice(&idr);
        one_nal.parse(&make_pes(au, Some(0)));
        assert_eq!(
            one_nal
                .sei_mastering
                .map(|m| m.max_display_mastering_luminance),
            Some(10_000_000)
        );
        assert!(one_nal.sei_content_light.is_some());

        let mut later = HevcParser::new();
        let mut first = pps.clone();
        first.extend_from_slice(&sei_nal(&[mastering(10_000_000)]));
        first.extend_from_slice(&idr);
        later.parse(&make_pes(first, Some(0)));
        assert!(later.sei_content_light.is_none());
        let mut second = pps.clone();
        second.extend_from_slice(&sei_nal(&[mastering(1), other(4), cll.clone()]));
        second.extend_from_slice(&idr);
        later.parse(&make_pes(second, Some(3750)));
        assert_eq!(
            later
                .sei_mastering
                .map(|m| m.max_display_mastering_luminance),
            Some(10_000_000),
            "the repeat is skipped, not adopted"
        );
        assert!(
            later.sei_content_light.is_some(),
            "content light behind a skipped repeat"
        );
    }

    #[test]
    fn hdr10_sei_in_a_suffix_nal_is_captured() {
        let mut sei = sei_nal(&[sei_message(
            SEI_CONTENT_LIGHT_LEVEL_INFO,
            &cll_payload(1000, 400),
        )]);
        sei[3] = NAL_SEI_SUFFIX << 1;
        let mut au = nal_bytes(NAL_PPS, &[0xC0]);
        au.extend_from_slice(&nal_bytes(19, &[0xEC]));
        au.extend_from_slice(&sei);
        let mut parser = HevcParser::new();
        parser.parse(&make_pes(au, Some(0)));
        assert!(parser.sei_content_light.is_some());
    }

    // Only the first slice segment of a picture carries the slice_type this reads; a later
    // segment's bits would otherwise be taken for one.
    #[test]
    fn a_non_first_slice_segment_reports_no_coding_type() {
        for nal_type in [1u8, 19] {
            let nal = nal_bytes(nal_type, &[0x6C, 0x00]);
            // 0x6C = first_slice_segment_in_pic_flag 0, then bits that would decode as an
            // I slice (no_output_of_prior_pics 1, pps id 0, slice_type 2) if the flag were ignored.
            assert_eq!(
                hevc_first_slice_coding_type(&nal[3..], nal_type, |_| Some(0)),
                None,
                "NAL {nal_type}"
            );
        }
    }

    #[test]
    fn hvcc_oversized_vps_or_pps_returns_none() {
        for oversized in [32u8, 34] {
            let mut data = Vec::new();
            for t in [32u8, 33, 34] {
                data.extend_from_slice(&[0x00, 0x00, 0x01]);
                data.extend_from_slice(&hevc_nal_header(t));
                let n = if t == oversized { 70_000 } else { 2 };
                data.extend_from_slice(&vec![0x11u8; n]);
            }
            let mut parser = HevcParser::new();
            parser.parse(&make_pes(data, Some(0)));
            assert!(
                parser.codec_private().is_none(),
                "oversized NAL type {oversized} must not produce a truncated hvcC"
            );
        }
    }

    // A bare IRAP re-asserts the ACTIVE parameter sets, not the first-seen hvcC copy: a player
    // re-applies hvcC at every keyframe, so reverting would undo a mid-title redefinition.
    #[test]
    fn a_bare_keyframe_reasserts_the_active_param_sets_not_the_hvcc_copy() {
        let au = |sets: Option<u8>, slice: Vec<u8>| {
            let mut d = Vec::new();
            if let Some(b) = sets {
                d.extend_from_slice(&nal_bytes(32, &[0xA0, b]));
                d.extend_from_slice(&nal_bytes(33, &[0xB0, b]));
                d.extend_from_slice(&nal_bytes(NAL_PPS, &[0xC0 | b]));
            }
            d.extend_from_slice(&slice);
            d
        };
        let bodies = |fd: &[u8]| -> Vec<(u8, u8)> {
            let mut out = Vec::new();
            let mut off = 0;
            while off + 4 <= fd.len() {
                let len =
                    u32::from_be_bytes([fd[off], fd[off + 1], fd[off + 2], fd[off + 3]]) as usize;
                out.push(((fd[off + 4] >> 1) & 0x3F, fd[off + 4 + len - 1]));
                off += 4 + len;
            }
            out
        };
        let mut parser = HevcParser::new();
        parser.parse(&make_pes(au(Some(1), nal_bytes(19, &[0xEC])), Some(0)));
        // Redefinition of all three sets on a non-keyframe access unit.
        let f = parser.parse(&make_pes(au(Some(2), nal_bytes(1, &[0xD0])), Some(3000)));
        assert_eq!(bodies(&f[0].data).len(), 4);
        let f = parser.parse(&make_pes(au(None, nal_bytes(19, &[0xEC])), Some(6000)));
        assert_eq!(
            bodies(&f[0].data),
            vec![(32, 2), (33, 2), (34, 0xC2), (19, 0xEC)],
            "VPS, SPS, PPS are the redefined ones, in hierarchy order"
        );
    }

    // --- BitReader unit tests (exp-Golomb + bit reads) ---

    #[test]
    fn bitreader_read_bits_msb_first() {
        // 0b1011_0010 read 4 bits → 0b1011 = 11, then 4 → 0b0010 = 2.
        let mut r = BitReader::new(&[0b1011_0010]);
        assert_eq!(r.read_bits(4), Some(11));
        assert_eq!(r.read_bits(4), Some(2));
        // Past end → None.
        assert_eq!(r.read_bit(), None);
    }

    #[test]
    fn bitreader_ue_golomb_values() {
        // Exp-Golomb ue(v) (H.264/HEVC §9.1): codeNum 0="1", 1="010", 2="011".
        // Packed "1 010 011" = byte 0b1010_0110: read ue -> 0, then 1, then 2.
        let mut r = BitReader::new(&[0b1010_0110]);
        assert_eq!(r.read_ue(), Some(0));
        assert_eq!(r.read_ue(), Some(1));
        assert_eq!(r.read_ue(), Some(2));
    }

    #[test]
    fn bitreader_ue_large_value() {
        // codeNum 4 = "00101". Byte 0b0010_1000 → ue = 4.
        let mut r = BitReader::new(&[0b0010_1000]);
        assert_eq!(r.read_ue(), Some(4));
    }

    #[test]
    fn bitreader_ue_runaway_zeros_bounded() {
        // A corrupt all-zero stream has unbounded leading zeros; read_ue caps at
        // 31 zeros and returns None rather than looping/overflowing.
        let zeros = [0u8; 8]; // 64 zero bits
        let mut r = BitReader::new(&zeros);
        assert_eq!(r.read_ue(), None, "runaway zero-run is bounded → None");
    }

    #[test]
    fn bitreader_skip_bits_past_end_is_none() {
        let mut r = BitReader::new(&[0xFF]);
        assert_eq!(r.skip_bits(8), Some(()));
        assert_eq!(r.skip_bits(1), None, "skipping past the buffer end → None");
    }

    // --- strip_emulation_prevention (00 00 03 → 00 00) ---

    #[test]
    fn strip_ep_removes_third_byte_after_two_zeros() {
        // 00 00 03 XX → 00 00 XX. The 0x03 is removed only after exactly two
        // zeros. (H.264/HEVC §7.4.)
        assert_eq!(
            strip_emulation_prevention(&[0x00, 0x00, 0x03, 0x42]),
            vec![0x00, 0x00, 0x42]
        );
    }

    #[test]
    fn strip_ep_leaves_03_after_single_zero() {
        // A 0x03 preceded by only ONE zero is real data, not an EP byte.
        assert_eq!(
            strip_emulation_prevention(&[0x00, 0x03, 0x42]),
            vec![0x00, 0x03, 0x42]
        );
    }

    #[test]
    fn strip_ep_handles_consecutive_sequences() {
        // 00 00 03 00 00 03 → 00 00 00 00. After dropping the first 0x03 the run
        // resets to 0, so the next two zeros re-arm and drop the second 0x03.
        assert_eq!(
            strip_emulation_prevention(&[0x00, 0x00, 0x03, 0x00, 0x00, 0x03]),
            vec![0x00, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn strip_ep_03_not_dropped_when_not_preceded_by_zeros() {
        // 0x03 after non-zero bytes is kept verbatim.
        assert_eq!(
            strip_emulation_prevention(&[0xAA, 0xBB, 0x03, 0xCC]),
            vec![0xAA, 0xBB, 0x03, 0xCC]
        );
    }

    // --- parse_sps_chroma: chroma_format_idc edge values ---

    #[test]
    fn hvcc_chroma_monochrome_idc0() {
        // chroma_format_idc = 0 (monochrome). bit depths 8-bit (minus8=0).
        let sps = make_sps_with_chroma(0, 0, 0);
        let cp = codec_private_from_sps(&sps);
        // chromaFormat byte = 0xFC (6 reserved bits) | chroma_format_idc(0) = 0xFC.
        assert_eq!(cp[16], 0xFC, "chroma_format_idc = 0 (monochrome)");
    }

    #[test]
    fn hvcc_chroma_422_idc2() {
        // chroma_format_idc = 2 (4:2:2), 10-bit.
        let sps = make_sps_with_chroma(2, 2, 2);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 2, "chroma_format_idc = 2 (4:2:2)");
        assert_eq!(cp[17], 0xF8 | 2);
    }

    #[test]
    fn hvcc_asymmetric_bit_depths() {
        // luma and chroma bit depths can differ; both must be parsed
        // independently. luma minus8 = 2 (10-bit), chroma minus8 = 4 (12-bit).
        let sps = make_sps_with_chroma(1, 2, 4);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[17], 0xF8 | 2, "bit_depth_luma_minus8 = 2");
        assert_eq!(cp[18], 0xF8 | 4, "bit_depth_chroma_minus8 = 4");
    }

    // Builds a stored SPS NAL with sub-layers + a conformance window, so the parser must skip
    // both before reaching bit depths.
    fn make_sps_full(
        chroma_idc: u32,
        bd_luma_m8: u32,
        bd_chroma_m8: u32,
        max_sub_layers_minus1: u32,
        conformance_window: bool,
    ) -> Vec<u8> {
        let flags = vec![(true, true); max_sub_layers_minus1 as usize];
        make_sps_sublayers(
            chroma_idc,
            bd_luma_m8,
            bd_chroma_m8,
            &flags,
            conformance_window,
        )
    }

    // As `make_sps_full`, with each sub-layer's (profile_present, level_present) flags chosen.
    fn make_sps_sublayers(
        chroma_idc: u32,
        bd_luma_m8: u32,
        bd_chroma_m8: u32,
        sub_layer_flags: &[(bool, bool)],
        conformance_window: bool,
    ) -> Vec<u8> {
        let max_sub_layers_minus1 = sub_layer_flags.len() as u32;
        let mut w = BitWriter::new();
        w.put_bits(0, 4); // sps_video_parameter_set_id
        w.put_bits(max_sub_layers_minus1, 3);
        w.put_bit(1); // sps_temporal_id_nesting_flag
        // general profile_tier_level: 96 bits.
        for _ in 0..96 {
            w.put_bit(0);
        }
        // Sub-layer flags + sub-layer PTL when max_sub_layers_minus1 > 0.
        if max_sub_layers_minus1 > 0 {
            for &(profile, level) in sub_layer_flags {
                // sub_layer_profile_present_flag, sub_layer_level_present_flag.
                w.put_bit(profile as u32);
                w.put_bit(level as u32);
            }
            if max_sub_layers_minus1 < 8 {
                for _ in max_sub_layers_minus1..8 {
                    w.put_bits(0, 2); // reserved_zero_2bits
                }
            }
            for &(profile, level) in sub_layer_flags {
                if profile {
                    for _ in 0..88 {
                        w.put_bit(0); // sub-layer profile block
                    }
                }
                if level {
                    w.put_bits(0, 8); // sub_layer_level_idc
                }
            }
        }
        w.put_ue(0); // sps_seq_parameter_set_id
        w.put_ue(chroma_idc);
        if chroma_idc == 3 {
            w.put_bit(0); // separate_colour_plane_flag
        }
        w.put_ue(3840);
        w.put_ue(2160);
        if conformance_window {
            w.put_bit(1); // conformance_window_flag
            w.put_ue(0); // conf_win_left_offset
            w.put_ue(0); // conf_win_right_offset
            w.put_ue(0); // conf_win_top_offset
            w.put_ue(0); // conf_win_bottom_offset
        } else {
            w.put_bit(0);
        }
        w.put_ue(bd_luma_m8);
        w.put_ue(bd_chroma_m8);

        let mut sps = hevc_nal_header(33).to_vec();
        sps.extend_from_slice(&w.bytes);
        sps
    }

    #[test]
    fn hvcc_parses_chroma_through_sublayer_ptl() {
        // With max_sub_layers_minus1=2 the parser must consume the sub-layer
        // present-flag bits, reserved bits, and two sub-layer PTL blocks before
        // reaching chroma/bit-depth fields, or a wrong skip mis-reads them.
        let sps = make_sps_full(1, 2, 2, 2, false);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 1, "4:2:0 after sub-layer PTL skip");
        assert_eq!(cp[17], 0xF8 | 2, "10-bit luma after sub-layer PTL skip");
        assert_eq!(cp[18], 0xF8 | 2);
        // byte 21: numTemporalLayers = max_sub_layers_minus1 + 1 = 3.
        assert_eq!(
            cp[21],
            (3 << 3) | (1 << 2) | 0x03,
            "numTemporalLayers = 3, temporalIdNested = 1, lengthSizeMinusOne = 3"
        );
    }

    // Each sub-layer's profile block (88 bits) and level byte (8 bits) are skipped on their OWN
    // flag, so any mix of the two flags must still land on the bit depths.
    #[test]
    fn hvcc_parses_chroma_through_sublayers_with_mixed_ptl_flags() {
        let cases: [&[(bool, bool)]; 5] = [
            &[(true, false)],
            &[(false, true)],
            &[(false, false)],
            &[(true, false), (false, true)],
            &[(false, true), (true, false), (false, false)],
        ];
        for flags in cases {
            let cp = codec_private_from_sps(&make_sps_sublayers(1, 4, 2, flags, false));
            assert_eq!(cp[16], 0xFC | 1, "chroma with flags {flags:?}");
            assert_eq!(cp[17], 0xF8 | 4, "luma depth with flags {flags:?}");
            assert_eq!(cp[18], 0xF8 | 2, "chroma depth with flags {flags:?}");
        }
    }

    #[test]
    fn hvcc_parses_chroma_through_conformance_window() {
        // conformance_window_flag = 1 inserts 4 ue(v) fields the parser must skip
        // before the bit depths. A correct skip lands on the right depths.
        let sps = make_sps_full(1, 2, 2, 0, true);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(
            cp[17],
            0xF8 | 2,
            "10-bit luma after conformance-window skip"
        );
        assert_eq!(cp[18], 0xF8 | 2);
    }

    #[test]
    fn hvcc_parses_444_with_separate_colour_plane() {
        // chroma_format_idc = 3 (4:4:4) inserts separate_colour_plane_flag (1
        // bit) that the parser must consume before pic dimensions. 12-bit.
        let sps = make_sps_full(3, 4, 4, 0, false);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 3, "4:4:4");
        assert_eq!(cp[17], 0xF8 | 4, "12-bit luma");
    }

    // --- hvcC array structure (VPS/SPS/PPS arrays) ---

    #[test]
    fn hvcc_array_headers_and_lengths() {
        // After the fixed header the record holds three arrays, each
        // (0x20 | nal_type), numNalus(=1, u16-BE), nalLength(u16), NAL bytes
        // (ISO/IEC 14496-15 §8.3.3.1). Verify the SPS array encodes correctly.
        let mut parser = HevcParser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xA0, 0xA1, 0xA2]); // VPS, 5 bytes total
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]); // SPS, 11 bytes
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xC0, 0xC1]); // PPS, 4 bytes
        parser.parse(&make_pes(data, Some(0)));
        let cp = parser.codec_private().expect("hvcC");

        // numOfArrays at index 22.
        assert_eq!(cp[22], 3);
        // VPS array begins at 23. array header byte = 0x20 | 32 = 0x40.
        let mut o = 23;
        assert_eq!(cp[o], 0x20 | 32, "VPS array nal_type byte");
        assert_eq!(
            u16::from_be_bytes([cp[o + 1], cp[o + 2]]),
            1,
            "numNalus VPS"
        );
        let vps_len = u16::from_be_bytes([cp[o + 3], cp[o + 4]]) as usize;
        assert_eq!(vps_len, 5, "VPS NAL length = 2 hdr + 3 payload");
        // skip to SPS array.
        o += 5 + vps_len;
        assert_eq!(cp[o], 0x20 | 33, "SPS array nal_type byte");
        let sps_len = u16::from_be_bytes([cp[o + 3], cp[o + 4]]) as usize;
        assert_eq!(sps_len, 11, "SPS NAL length = 2 hdr + 9 payload");
        o += 5 + sps_len;
        assert_eq!(cp[o], 0x20 | 34, "PPS array nal_type byte");
        let pps_len = u16::from_be_bytes([cp[o + 3], cp[o + 4]]) as usize;
        assert_eq!(pps_len, 4, "PPS NAL length = 2 hdr + 2 payload");
    }

    #[test]
    fn hvcc_none_missing_vps() {
        // VPS is required for hvcC; SPS + PPS only → None.
        let mut parser = HevcParser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD, 0xEE]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(1)); // slice
        data.extend_from_slice(&[0x10, 0x20]);
        parser.parse(&make_pes(data, Some(0)));
        assert!(parser.codec_private().is_none(), "no VPS → None");
    }

    // --- IRAP keyframe boundary values ---

    #[test]
    fn type_15_just_below_irap_not_keyframe() {
        // Type 15 (RASL_R) is one below the IRAP range and must NOT be a keyframe.
        let mut parser = HevcParser::new();
        let mut data = vec![0x00, 0x00, 0x01];
        data.extend_from_slice(&hevc_nal_header(15));
        data.extend_from_slice(&[0x10, 0x20]);
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert!(!f[0].keyframe, "type 15 is below the IRAP range");
    }

    #[test]
    fn type_24_just_above_irap_not_keyframe() {
        // Type 24 (RSV_VCL24) is one above the IRAP range (..=23) → not keyframe.
        let mut parser = HevcParser::new();
        let mut data = vec![0x00, 0x00, 0x01];
        data.extend_from_slice(&hevc_nal_header(24));
        data.extend_from_slice(&[0x10, 0x20]);
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert!(!f[0].keyframe, "type 24 is above the IRAP range");
    }

    #[test]
    fn hevc_nal_type_extraction_masks_correctly() {
        // HEVC NAL type = (byte0 >> 1) & 0x3F. forbidden_zero_bit (bit 7) and the
        // low layer-id bit (bit 0) must not affect type: hevc_nal_header(19) =
        // [0x26, 0x01]; with the forbidden bit set (0xA6) it's still type 19.
        let mut parser = HevcParser::new();
        let data = vec![0x00, 0x00, 0x01, 0xA6, 0x01, 0x10, 0x20]; // 0xA6>>1&0x3F = 19
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert!(
            f[0].keyframe,
            "0xA6 decodes to NAL type 19 (IDR) → keyframe"
        );
    }

    #[test]
    fn hevc_dts_fallback_when_pts_absent() {
        let mut parser = HevcParser::new();
        let pes = PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: Some(90000),
            data: {
                let mut d = vec![0x00, 0x00, 0x01];
                d.extend_from_slice(&hevc_nal_header(1));
                d.extend_from_slice(&[0x10, 0x20]);
                d
            },
            discontinuity: false,
        };
        let f = parser.parse(&pes);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "falls back to DTS");
    }

    #[test]
    fn parse_sps_chroma_too_short_returns_none() {
        // An SPS shorter than 3 bytes can't carry the 2-byte NAL header + RBSP →
        // parse_sps_chroma returns None (caller falls back to 8-bit 4:2:0).
        assert!(parse_sps_chroma(&[0x42]).is_none());
        assert!(parse_sps_chroma(&[0x42, 0x01]).is_none());
    }

    /// A mastering-display SEI payload one byte short of the fixed 24-byte
    /// layout must be rejected, not read out of bounds. This is the guard a
    /// crafted/truncated SEI on a damaged disc hits directly.
    #[test]
    fn parse_mastering_display_one_byte_short_is_none() {
        assert!(parse_mastering_display(&[0u8; 23]).is_none());
        assert!(parse_mastering_display(&[0u8; 24]).is_some());
    }

    /// Same guard, content-light-level's 4-byte layout.
    #[test]
    fn parse_content_light_level_one_byte_short_is_none() {
        assert!(parse_content_light_level(&[0u8; 3]).is_none());
        assert!(parse_content_light_level(&[0u8; 4]).is_some());
    }

    #[test]
    fn hvcc_falls_back_to_8bit_420_on_unparseable_sps() {
        // An SPS whose RBSP is truncated mid-parse (can't reach the bit depths)
        // must fall back to the 8-bit 4:2:0 default, not panic. A 3-byte stored
        // SPS (header + 1 RBSP byte) can't complete the PTL skip.
        let mut parser = HevcParser::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA, 0xBB]);
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&[0x00]); // 1 RBSP byte — unparseable
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD]);
        parser.parse(&make_pes(data, Some(0)));
        let cp = parser.codec_private().expect("hvcC");
        assert_eq!(cp[16], 0xFC | 1, "fallback chroma_format_idc = 1 (4:2:0)");
        assert_eq!(cp[17], 0xF8, "fallback 8-bit luma");
        assert_eq!(cp[18], 0xF8, "fallback 8-bit chroma");
    }

    #[test]
    fn hvcc_oversized_param_set_returns_none() {
        // A param set larger than 65535 bytes cannot be length-encoded in hvcC's
        // 16-bit field; codec_private must refuse rather than emit a truncated,
        // mis-framed record.
        let mut parser = HevcParser::new();
        let mut data = Vec::new();
        // VPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&[0xAA, 0xBB]);
        // Oversized SPS: header + 70000 bytes of payload (avoid 00 00 0x runs by
        // using 0x11 filler so it stays one NAL).
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&vec![0x11u8; 70_000]);
        // PPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&[0xDD, 0xEE]);
        parser.parse(&make_pes(data, Some(0)));
        assert!(
            parser.codec_private().is_none(),
            "oversized param set must not produce a (truncated) hvcC"
        );
    }

    // Every other fixture's VPS/SPS/PPS is under 256 bytes, so a `>>` -> `<<` mutation in the
    // 16-bit array-length write is unobservable there. Uses 300+ byte NALs to catch it.
    #[test]
    fn hvcc_array_length_round_trips_above_256_bytes() {
        let mut parser = HevcParser::new();
        let mut data = Vec::new();
        // VPS: 2-byte NAL header + 300 filler bytes -> NAL length 302.
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(32));
        data.extend_from_slice(&vec![0x11u8; 300]);
        // SPS: same size.
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(33));
        data.extend_from_slice(&vec![0x11u8; 300]);
        // PPS: same size.
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&hevc_nal_header(34));
        data.extend_from_slice(&vec![0x11u8; 300]);
        parser.parse(&make_pes(data, Some(0)));
        let cp = parser.codec_private().expect("hvcC");

        let expected_nal_len = 2 + 300; // NAL header + payload
        let mut o = 23; // past the 23-byte fixed header
        assert_eq!(cp[o], 0x20 | 32, "VPS array nal_type byte");
        let vps_len = u16::from_be_bytes([cp[o + 3], cp[o + 4]]) as usize;
        assert_eq!(
            vps_len, expected_nal_len,
            "VPS length must round-trip above 256 bytes"
        );
        o += 5 + vps_len;

        assert_eq!(cp[o], 0x20 | 33, "SPS array nal_type byte");
        let sps_len = u16::from_be_bytes([cp[o + 3], cp[o + 4]]) as usize;
        assert_eq!(
            sps_len, expected_nal_len,
            "SPS length must round-trip above 256 bytes"
        );
        o += 5 + sps_len;

        assert_eq!(cp[o], 0x20 | 34, "PPS array nal_type byte");
        let pps_len = u16::from_be_bytes([cp[o + 3], cp[o + 4]]) as usize;
        assert_eq!(
            pps_len, expected_nal_len,
            "PPS length must round-trip above 256 bytes"
        );
    }

    // `with_ps_reorder(true)` must INSTALL the reorderer, and `flush()` must drain its real
    // buffered frames at EOF.
    #[test]
    fn hevc_ps_reorder_is_installed_and_flush_drains_its_real_frames() {
        const NAL_IDR_W_RADL: u8 = 19; // IRAP → keyframe / GOP anchor
        const NAL_TRAIL_R: u8 = 1; // non-IRAP coded slice
        let nal = |t: u8, body: u8| {
            let mut v = vec![0x00, 0x00, 0x01];
            v.extend_from_slice(&hevc_nal_header(t));
            v.push(body);
            v
        };
        // One access unit = active PPS + one coded slice, in one PES.
        let au = |t: u8, body: u8| {
            let mut d = nal(NAL_PPS, 0xC0);
            d.extend_from_slice(&nal(t, body));
            d
        };
        // Decode order of a classic single-B GOP: I P B P B.
        let gop = |anchor: Option<i64>| {
            vec![
                (au(NAL_IDR_W_RADL, 0xAC), anchor), // I, IRAP anchor
                (au(NAL_TRAIL_R, 0xD0), None),      // P
                (au(NAL_TRAIL_R, 0xE0), None),      // B
                (au(NAL_TRAIL_R, 0xD0), None),      // P
                (au(NAL_TRAIL_R, 0xE0), None),      // B
            ]
        };

        let feed = |reorder: bool| -> (Vec<Frame>, Vec<Frame>) {
            let mut p = HevcParser::new().with_ps_reorder(reorder);
            let mut during = Vec::new();
            // Two GOPs; the second anchor is 5 frames later (90 kHz: 5 × 3750).
            for (data, pts) in gop(Some(0)).into_iter().chain(gop(Some(18_750))) {
                during.extend(p.parse(&make_pes(data, pts)));
            }
            let tail = p.flush();
            (during, tail)
        };

        let (during, tail) = feed(true);
        assert!(
            !tail.is_empty(),
            "the reorderer holds frames back; flush must release them"
        );
        for f in &tail {
            assert!(
                !f.data.is_empty(),
                "a flushed frame carries real coded bytes, never a manufactured empty one"
            );
        }
        let all: Vec<&Frame> = during.iter().chain(tail.iter()).collect();
        assert_eq!(all.len(), 10, "every access unit is emitted exactly once");
        let mut pts: Vec<i64> = all.iter().map(|f| f.pts_ns).collect();
        let n = pts.len();
        pts.sort_unstable();
        pts.dedup();
        assert_eq!(
            pts.len(),
            n,
            "reconstructed PTS are all distinct (no DTS collision)"
        );

        // With reorder OFF the sparse-PTS frames collapse onto the anchor's
        // timestamp and nothing is buffered, so flush is empty — the discriminator
        // proving `with_ps_reorder(true)` actually changed behaviour.
        let (raw_during, raw_tail) = feed(false);
        assert!(
            raw_tail.is_empty(),
            "no reorderer installed → nothing buffered at EOF"
        );
        assert_eq!(raw_during.len(), 10);
        let collisions = raw_during.iter().filter(|f| f.pts_ns == 0).count();
        assert!(
            collisions >= 4,
            "without reorder the sparse-PTS frames collide on 0 (got {collisions})"
        );
    }
}
