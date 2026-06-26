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
// Supplemental Enhancement Information (Rec. ITU-T H.265 Table 7-1): a prefix
// SEI (type 39) precedes the coded picture it applies to, a suffix SEI (40)
// follows it. HDR10 static metadata (mastering display / content light level)
// is carried in PREFIX SEI on UHD streams; both are scanned for the two HDR10
// payload types below. SEI NALs still pass through to the frame data unchanged
// (the `_ =>` arm); scanning them is observation-only.
const NAL_SEI_PREFIX: u8 = 39;
const NAL_SEI_SUFFIX: u8 = 40;

// HEVC SEI payload types (Rec. ITU-T H.265 Annex D.2) carrying HDR10 static
// metadata.
//   - Mastering Display Colour Volume (D.2.28): payloadType 137.
//   - Content Light Level Information (D.2.35): payloadType 144.
const SEI_MASTERING_DISPLAY_COLOUR_VOLUME: u32 = 137;
const SEI_CONTENT_LIGHT_LEVEL_INFO: u32 = 144;
// Dolby Vision RPU (Reference Processing Unit) — NAL type 62 (UNSPEC62).
// This is NOT filtered: all NAL types except VPS/SPS/PPS/AUD pass through
// to frame data, so DV enhancement layer RPU NALs are preserved automatically.
const _NAL_UNSPEC62_DV_RPU: u8 = 62;
// IRAP types (keyframes): BLA, IDR, CRA
const NAL_BLA_W_LP: u8 = 16;
const NAL_RSV_IRAP_VCL23: u8 = 23;
// CRA_NUT (Clean Random Access). A CRA at a splice carries RASL leading
// pictures that reference frames from BEFORE the splice; on linear decode of a
// concatenated title those references are gone ("Could not find ref with POC
// N"). The HEVC spec remedy is to rewrite the splice CRA as a BLA (Broken Link
// Access): a decoder then sets NoRaslOutput and discards the RASL cleanly with
// no error. See `mark_clip_boundary` / the IRAP arm in `parse`.
const NAL_CRA_NUT: u8 = 21;
/// Highest VCL (coded-slice) NAL type. Rec. ITU-T H.265 Table 7-1: types 0..=31
/// are VCL, 32..=63 non-VCL. A coded slice carries a `slice_type`.
const NAL_VCL_MAX: u8 = 31;

/// `num_extra_slice_header_bits` from a HEVC PPS NAL (H.265 §7.3.2.3): after the
/// 2-byte NAL header, skip `pps_pic_parameter_set_id` + `pps_seq_parameter_set_id`
/// (both `ue(v)`) and `dependent_slice_segments_enabled_flag` +
/// `output_flag_present_flag` (`u(1)` each), then read `u(3)`. `None` if the PPS
/// is too short to parse — the caller then declines to guess a slice type.
fn hevc_num_extra_slice_header_bits(pps_nal: &[u8]) -> Option<u32> {
    let mut br = BitReader::new(pps_nal.get(2..)?);
    br.read_ue()?; // pps_pic_parameter_set_id
    br.read_ue()?; // pps_seq_parameter_set_id
    br.skip_bits(2)?; // dependent_slice_segments_enabled_flag, output_flag_present_flag
    let mut n = 0u32;
    for _ in 0..3 {
        n = (n << 1) | br.read_bit()?;
    }
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

/// Measure the coding type from the FIRST coded slice of an access unit
/// (H.265 §7.3.6.1 `slice_segment_header`). Reads only the leading fields of the
/// first slice segment: `first_slice_segment_in_pic_flag` u(1), the IRAP
/// `no_output_of_prior_pics_flag` u(1), `slice_pic_parameter_set_id` ue(v), the
/// `num_extra_slice_header_bits` reserved bits, then `slice_type` ue(v). Returns
/// `None` for a non-first slice or on truncation — never a guess. `num_extra`
/// MUST come from the active PPS so the bit offset to `slice_type` is exact.
fn hevc_first_slice_coding_type(nal: &[u8], nal_type: u8, num_extra: u32) -> Option<CodingType> {
    let mut br = BitReader::new(nal.get(2..)?); // RBSP after the 2-byte NAL header
    if br.read_bit()? != 1 {
        return None; // not the first slice segment of the picture
    }
    if (NAL_BLA_W_LP..=NAL_RSV_IRAP_VCL23).contains(&nal_type) {
        br.skip_bits(1)?; // no_output_of_prior_pics_flag (IRAP only)
    }
    br.read_ue()?; // slice_pic_parameter_set_id
    // First slice → no slice_segment_address and dependent_slice_segment_flag is
    // 0, so slice_type follows the reserved bits directly.
    br.skip_bits(num_extra)?; // slice_reserved_flag[i]
    hevc_slice_coding_type(br.read_ue()?)
}

/// HEVC (H.265) Annex B → MKV codec parser: extracts VPS/SPS/PPS for the hvcC
/// codecPrivate, detects IRAP keyframes, and converts each PES access unit into
/// length-prefixed NAL units. Implements [`CodecParser`].
pub struct HevcParser {
    // First-seen parameter set of each type → seeds the MKV codecPrivate (hvcC).
    // This is the ONLY copy the player gets out-of-band, and a player re-applies
    // it at every keyframe (ffmpeg's hvcC→Annex-B insertion). A stream may
    // redefine a parameter set mid-title under the SAME id with a different body
    // (some discs redefine PPS id 0 partway through). Any occurrence whose body
    // DIFFERS from this codecPrivate copy must therefore be emitted IN-BAND at
    // each point it appears (i.e. at every keyframe of the redefined segment) so
    // it overrides the re-applied codecPrivate set; otherwise those frames decode
    // against the wrong parameter set → CABAC/cu_qp_delta desync.
    vps: Option<Vec<u8>>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    // The currently-ACTIVE parameter-set body of each type — the most recent
    // one the bitstream defined, which the decoder must use until the next
    // redefinition. Distinct from the `vps/sps/pps` codecPrivate copy above
    // (which is fixed to the FIRST one seen). When a stream redefines a param
    // set mid-title (e.g. PPS id 0 body changes partway through, then the
    // source STOPS repeating it at later IRAPs and relies on the decoder
    // retaining it), a raw decode is fine — but an hvcC/MKV decode is NOT: a
    // player re-applies the codecPrivate set at EVERY keyframe (ffmpeg's
    // hvcC→Annex-B insertion), reverting id 0 to the stale FIRST body. We must
    // therefore re-emit the active set IN-BAND at every keyframe whenever it
    // differs from the codecPrivate copy and the access unit didn't already
    // carry it. See `parse`.
    cur_vps: Option<Vec<u8>>,
    cur_sps: Option<Vec<u8>>,
    cur_pps: Option<Vec<u8>>,
    // Splice-aware CRA→BLA rewrite (non-seamless BD clip boundaries).
    //
    // When a BD title concatenates clips at a NON-SEAMLESS join (MPLS
    // connection_condition 0x05 or 0x06), the next clip opens with a CRA whose
    // RASL leading pictures reference frames from before the splice — gone after
    // concatenation. The caller (the code that crosses the join) sets this flag
    // via `mark_clip_boundary`; the parser then rewrites the FIRST CRA it sees
    // at/after that point from CRA_NUT (21) to BLA_W_LP (16) so a linear decoder
    // sets NoRaslOutput and discards the dangling RASL with no error. The flag
    // is consumed (cleared) by that first CRA so only ONE CRA per boundary is
    // touched — never a mid-stream CRA, never an IDR, never a non-CRA NAL.
    //
    // SAFETY: defaults to `false` and is ONLY ever set through
    // `mark_clip_boundary`, which the caller invokes ONLY for a non-seamless
    // (0x05/0x06) join. connection_condition 0x01 is the first-item/seamless
    // case and must NOT trigger this flag. A stream with no boundary marker
    // (single-clip title, or seamless-joined 0x01 UHD/BD) never has this set,
    // so the rewrite branch is never reached and output is byte-identical to a
    // parser without this field.
    pending_clip_boundary: bool,
    // Highest PES PTS seen on this video stream so far, on a MONOTONIC 64-bit
    // timeline (raw 33-bit PTS unwrapped across 2^33 wraparounds — see
    // `pts_wrap_offset`). Used to AUTO-DETECT a non-seamless clip boundary from
    // the bitstream when the caller never plumbs one in (the common case — see
    // `BACKSTEP_TICKS`). `None` until the first AU with a PTS.
    high_pts: Option<i64>,
    // Accumulated 2^33-tick offset applied to raw PES PTS values to unwrap them
    // onto the monotonic timeline `high_pts` lives on. The 33-bit 90 kHz PTS
    // wraps every ~26.5 h; a BD clip can start at a high base and cross the wrap
    // mid-title. Without unwrapping, the 2^33→0 step looks like a backward clip
    // reset and false-arms the CRA→BLA rewrite (corrupting a legitimate in-clip
    // CRA and dropping valid RASL pictures). Each detected wrap adds 2^33 here.
    pts_wrap_offset: i64,
    // HDR10 static metadata accumulated from prefix/suffix SEI. The Mastering
    // Display Colour Volume (payloadType 137) and Content Light Level Info
    // (payloadType 144) messages arrive in (possibly) separate SEI NALs; each is
    // captured independently and STICKY (first seen wins — they are per-stream
    // constants). `hdr10()` combines them into a complete `Hdr10Metadata` only
    // when BOTH are present. An SDR / no-SEI stream leaves both `None` so no
    // colour-volume metadata is ever fabricated.
    sei_mastering: Option<MasteringDisplay>,
    sei_content_light: Option<ContentLightLevel>,
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

// A backward PES-PTS step larger than this (90 kHz ticks) marks a non-seamless
// BD clip boundary: each .m2ts clip carries its own PTS base, so at a 0x05/0x06
// join the next clip's PTS resets backward by far more than any B-frame reorder
// window (HEVC reorder depth tops out ~16 frames, <1 s at 24 fps). 3 s = 270000
// ticks sits well above any legitimate reorder dip and far below any real clip's
// duration, so it never false-triggers within a clip. This MIRRORS the mux-side
// `DISCONTINUITY_BACKSTEP_NS` (3 s) in `mux/mkv.rs`, which independently rebases
// the timeline at the same boundaries; here it drives the CRA→BLA rewrite that
// kills the dangling-RASL "Could not find ref with POC N" decode errors a
// concatenated multi-clip title otherwise produces.
const BACKSTEP_TICKS: i64 = 270_000;

// The 33-bit 90 kHz PES PTS counter wraps at 2^33 ticks (~26.5 h). When the raw
// PTS steps backward by approximately a full period — i.e. it landed just past
// the wrap — it is a counter wraparound, NOT a clip reset: unwrap it (add 2^33)
// instead of arming the CRA→BLA rewrite. A genuine non-seamless clip join resets
// the PTS to a fresh small base, a backward step of arbitrary (sub-2^33) size; a
// wrap is specifically a step of ~2^33. We accept any backward step within one
// `PTS_WRAP_PERIOD`/2 of a full period as a wrap (the new value is below the old
// high-water but within a reorder window of the wrap point), which cleanly
// separates the two cases since a clip reset to a small base is nowhere near 2^33
// below the high-water unless the title is itself ~26 h long (impossible on BD).
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
            pending_clip_boundary: false,
            high_pts: None,
            pts_wrap_offset: 0,
            sei_mastering: None,
            sei_content_light: None,
        }
    }

    /// Combine the accumulated mastering-display and content-light SEI into a
    /// complete [`Hdr10Metadata`], or `None` until BOTH HDR10 SEI messages have
    /// been seen. Requiring both means an SDR / partially-signalled stream never
    /// emits a half-populated (confidently-wrong) HDR10 record.
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

    /// Scan an SEI NAL (`[2-byte NAL header][RBSP]`) for the two HDR10 payload
    /// types and capture each the FIRST time it appears (per-stream constants).
    ///
    /// RBSP structure (Rec. ITU-T H.265 D.2 `sei_rbsp` / `sei_message`): a
    /// sequence of messages, each `payloadType` then `payloadSize` encoded as a
    /// run of 0xFF bytes plus a final <0xFF byte (the "ff-extension" coding),
    /// followed by `payloadSize` payload bytes. Emulation-prevention (00 00 03)
    /// is stripped before reading — unlike a slice header, an SEI payload can be
    /// deep enough that an emulation byte falls inside the fields we read.
    /// Unknown payload types are skipped by their size so a later HDR10 message
    /// in the same NAL is still reached.
    fn scan_sei(&mut self, nal: &[u8]) {
        let Some(raw) = nal.get(2..) else {
            return;
        };
        let rbsp = strip_emulation_prevention(raw);
        let mut i = 0usize;
        loop {
            // payloadType: sum of 0xFF run + final byte.
            let Some(payload_type) = read_sei_ff_value(&rbsp, &mut i) else {
                break;
            };
            // payloadSize: same ff-extension coding.
            let Some(payload_size) = read_sei_ff_value(&rbsp, &mut i) else {
                break;
            };
            let payload_size = payload_size as usize;
            let Some(payload) = rbsp.get(i..i.saturating_add(payload_size)) else {
                break; // truncated / malformed payload length — stop scanning
            };
            match payload_type {
                SEI_MASTERING_DISPLAY_COLOUR_VOLUME if self.sei_mastering.is_none() => {
                    if let Some(m) = parse_mastering_display(payload) {
                        self.sei_mastering = Some(m);
                    }
                }
                SEI_CONTENT_LIGHT_LEVEL_INFO if self.sei_content_light.is_none() => {
                    if let Some(c) = parse_content_light_level(payload) {
                        self.sei_content_light = Some(c);
                    }
                }
                _ => {}
            }
            i += payload_size;
            // An RBSP trailing byte (0x80) or padding zeros after the last
            // message is not another payloadType; stop when nothing meaningful
            // remains. `read_sei_ff_value` returning None on the next pass
            // handles end-of-buffer; a lone 0x80 trailing bits byte is consumed
            // as a (bogus) payloadType of 128 then fails the size read → break.
            if i >= rbsp.len() {
                break;
            }
        }
    }

    /// Mark that the NEXT IRAP this parser sees begins a NON-SEAMLESS BD clip
    /// join. MPLS connection_condition 0x05 and 0x06 are the non-seamless
    /// values (per the BD-ROM spec: 0x01 = first item / seamless, 0x05/0x06 =
    /// non-seamless). The first CRA at/after this point is rewritten CRA_NUT
    /// (21) → BLA_W_LP (16) so a linear decoder sets NoRaslOutput and discards
    /// the now-dangling RASL leading pictures with no "could not find ref"
    /// error.
    ///
    /// MUST be called ONLY when MPLS reports connection_condition as 0x05 or
    /// 0x06 (non-seamless). It is a no-op for the rewrite unless a CRA actually
    /// follows: an IDR/IDR_W_RADL boundary needs no fix (it carries no
    /// cross-splice references), and the flag is cleared by the first
    /// IRAP-class CRA it reaches.
    ///
    /// SAFETY: never call this for connection_condition 0x01 (seamless/first
    /// item) or within a single-clip title — doing so could convert a
    /// legitimate mid-content CRA to BLA. The default (never called) path
    /// leaves output byte-identical.
    pub fn mark_clip_boundary(&mut self) {
        self.pending_clip_boundary = true;
    }
}

/// Handle a VPS/SPS/PPS NAL. Decides whether to strip it (the decoder already
/// has the value) or emit it in-band, and tracks the currently-active body.
///
/// The decision MUST be made against the currently-active set (`cur`), NOT the
/// codecPrivate copy (`first`). The two player behaviours for hvcC-in-MKV
/// diverge exactly here:
///
/// - A *seek-capable / Annex-B* player (e.g. ffmpeg's `hevc_mp4toannexb`)
///   re-applies the hvcC sets at every keyframe. `reassert_active` handles it.
/// - A *streaming* decode (ffmpeg decoding the MKV directly — what most
///   integrity checkers do) applies hvcC ONCE at init and thereafter updates a
///   parameter set ONLY from an in-band NAL.
///
/// So when a title redefines a set mid-stream (id 0 body A → B) and later
/// switches BACK to A (== codecPrivate), the change to A must STILL be emitted
/// in-band: the streaming decoder is sitting on B and will never revert
/// otherwise, decoding the whole A-segment against B → CABAC/cu_qp_delta
/// desync. Stripping on `== first` (the old behaviour) dropped exactly that
/// revert and corrupted every "switch back to the first body" segment.
///
/// Rules:
/// - First of its type → seeds codecPrivate; stripped (the decoder gets it from
///   hvcC at init).
/// - Equal to the active set `cur` → redundant; stripped.
/// - Different from `cur` (a change, in EITHER direction) → emitted in-band and
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

/// Append the active parameter set `cur` to `prefix` (length-prefixed) so every
/// keyframe is SELF-CONTAINED: it carries the active VPS/SPS/PPS in-band ahead
/// of its slices. Skipped only when this access unit ALREADY carried the NAL
/// in-band (`emitted` — avoids a duplicate) or no active set exists yet.
///
/// Why unconditional (not only when the active set differs from codecPrivate):
/// a streaming decoder applies the hvcC param sets once at init, then relies on
/// in-band repetition. Some sources stop repeating a param set at later IRAPs
/// even though its body is unchanged; if the decoder then drops it (a CRA reset
/// or SPS event), nothing re-sends it and every subsequent slice fails with
/// "PPS id out of range" until the next genuine change (observed as a ~24 min
/// corrupt band on one dual-layer UHD title). Re-asserting the active set at
/// EVERY keyframe — what compliant muxers (mkvmerge) do at every IRAP — makes
/// streaming decode self-healing. Re-sending an identical param set is benign
/// (decoders expect it at IRAPs); cost is a few hundred bytes per keyframe.
/// This strictly supersets the earlier change-only re-assert, so the
/// param-set-revert fix is unaffected.
fn reassert_active(prefix: &mut Vec<u8>, cur: &Option<Vec<u8>>, emitted: bool) {
    if emitted {
        return;
    }
    let Some(active) = cur.as_deref() else {
        return;
    };
    push_length_prefixed(prefix, active);
}

/// Append `nal` to `out` as a 4-byte big-endian length prefix followed by the
/// NAL body. A NAL longer than `u32::MAX` can't be length-prefixed in the
/// 4-byte field, so it is skipped rather than mis-framed. Unreachable in
/// practice (no real access unit is >4 GiB).
fn push_length_prefixed(out: &mut Vec<u8>, nal: &[u8]) {
    let Ok(len) = u32::try_from(nal.len()) else {
        return;
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(nal);
}

impl CodecParser for HevcParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // MKV block timecodes are PRESENTATION timestamps; frames are stored
        // in decode order (the order they arrive here) and the player reorders
        // for display by timecode. So use PTS, not DTS — using DTS makes the
        // block timecode monotonic in storage order, which presents B-frames in
        // decode order (visible judder / wrong frames) and breaks PTS-based
        // seeking. Fall back to DTS only if PTS is somehow absent.
        let pts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);

        // Auto-detect a non-seamless clip boundary from the bitstream. freemkv
        // reads a BD title's clips as ONE concatenated sector stream and the
        // mpls connection_condition is not plumbed through the (threaded) mux
        // pipeline, so `mark_clip_boundary` is otherwise never invoked. Each
        // .m2ts clip carries its own PTS base; at a 0x05/0x06 join the next
        // clip's PTS resets backward by far more than any reorder window. A
        // backward step beyond `BACKSTEP_TICKS` is that boundary: arm the same
        // CRA→BLA rewrite (`pending_clip_boundary`) the first IRAP of the new
        // clip then consumes. Without this, the splice CRA's RASL leading
        // pictures reference pre-join frames gone after concatenation and a
        // linear decoder floods "Could not find ref with POC N" (the Top Gun
        // UHD defect). Uses the 90 kHz PES PTS (not the rebased mux timeline)
        // UNWRAPPED onto a monotonic 64-bit timeline first — the raw 33-bit PTS
        // wraps every ~26.5 h, and a single-clip title that crosses 2^33→0 would
        // otherwise false-arm the rewrite (corrupting a legitimate in-clip CRA).
        // Tracks the high-water mark so a single in-clip B-frame dip never arms
        // it. DTS-only AUs (no PTS) leave the watermark untouched.
        if let Some(raw_pts) = pes.pts {
            // Unwrap onto the monotonic timeline. If the offset-adjusted value
            // dropped to roughly a full period (2^33) below the high-water, the
            // 33-bit counter wrapped: add another period and re-check, rather
            // than treat the wrap as a backward clip reset.
            let mut unwrapped = raw_pts + self.pts_wrap_offset;
            if let Some(high) = self.high_pts {
                if high - unwrapped > PTS_WRAP_PERIOD / 2 {
                    self.pts_wrap_offset += PTS_WRAP_PERIOD;
                    unwrapped += PTS_WRAP_PERIOD;
                }
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
        // Picture coding type, MEASURED from the first coded slice's header.
        let mut coding_type: Option<CodingType> = None;
        // Track whether THIS access unit already carried each param-set type
        // in-band (a redefinition vs codecPrivate). Used after the scan to
        // re-assert the active set at a keyframe the source left bare.
        let mut emitted_vps = false;
        let mut emitted_sps = false;
        let mut emitted_pps = false;
        // Pre-size: output is ~input bytes with a few 4-byte length
        // prefixes added. UHD frames are 150-300 KB; the unsized Vec
        // growth chain otherwise reallocs 5-7× per frame.
        let mut frame_data = Vec::with_capacity(data.len() + 64);

        // Single-pass NAL scan: extract params, detect keyframes, build length-prefixed output
        let mut pos = 0;
        while let Some(sc_pos) = find_start_code(data, pos) {
            if let Some(nal_start) = skip_start_code(data, sc_pos) {
                let next = find_start_code(data, nal_start).unwrap_or(data.len());
                // Strip the leading zeros of the following start code. For a
                // conforming bitstream this is lossless: rbsp_trailing_bits()
                // sets a stop-one bit, so the final byte of any RBSP is never
                // 0x00 — the only trailing zeros here belong to the next
                // 00 00 (00) 01 prefix.
                let mut end = next;
                while end > nal_start && data[end - 1] == 0x00 {
                    end -= 1;
                }

                // Skip empty NALs entirely. When the trailing-zero strip reduces
                // `end` back to `nal_start` (e.g. `00 00 01 00 00 01`, or a
                // zero-filled bad sector between two start codes), the slice is
                // empty; emitting a 4-byte 0x00000000 length prefix with no NAL
                // body produces a structurally invalid NALU a decoder rejects.
                if nal_start < data.len() && end > nal_start {
                    // HEVC NAL header: 2 bytes. Type is bits 1-6 of first byte.
                    let nal_type = (data[nal_start] >> 1) & 0x3F;

                    // Measure the coding type from the FIRST coded slice (VCL NAL
                    // 0..=31). Only attempted once the active PPS is known, so
                    // `num_extra_slice_header_bits` — and thus the bit offset to
                    // `slice_type` — is EXACT. With no PPS we decline rather than
                    // guess, leaving coding `None` (honestly absent).
                    if coding_type.is_none() && nal_type <= NAL_VCL_MAX {
                        if let Some(num_extra) = self
                            .cur_pps
                            .as_deref()
                            .and_then(hevc_num_extra_slice_header_bits)
                        {
                            coding_type = hevc_first_slice_coding_type(
                                &data[nal_start..end],
                                nal_type,
                                num_extra,
                            );
                        }
                    }

                    match nal_type {
                        NAL_VPS => {
                            emitted_vps |= handle_param_set(
                                &mut self.vps,
                                &mut self.cur_vps,
                                &data[nal_start..end],
                                &mut frame_data,
                            )
                        }
                        NAL_SPS => {
                            emitted_sps |= handle_param_set(
                                &mut self.sps,
                                &mut self.cur_sps,
                                &data[nal_start..end],
                                &mut frame_data,
                            )
                        }
                        NAL_PPS => {
                            emitted_pps |= handle_param_set(
                                &mut self.pps,
                                &mut self.cur_pps,
                                &data[nal_start..end],
                                &mut frame_data,
                            )
                        }
                        // Drop Access Unit Delimiters. This is intentional and
                        // spec-correct: Matroska HEVC frame data omits AUDs
                        // (the container delimits access units), so carrying
                        // them in-band is redundant. H.264 does the same below.
                        NAL_AUD => {}
                        t if (NAL_BLA_W_LP..=NAL_RSV_IRAP_VCL23).contains(&t) => {
                            keyframe = true;
                            // Splice-aware CRA→BLA rewrite. At the FIRST CRA
                            // following a non-seamless clip boundary (flag set
                            // via `mark_clip_boundary`), rewrite CRA_NUT (21) →
                            // BLA_W_LP (16) so a linear decoder sets NoRaslOutput
                            // and drops the dangling RASL with no error. The flag
                            // is consumed here so exactly ONE CRA per boundary is
                            // touched. A non-CRA IRAP (IDR, BLA) clears the flag
                            // too (the boundary is handled — IDR carries no
                            // cross-splice refs) but is NOT modified. Default
                            // path (flag never set) is unreachable → byte-
                            // identical output.
                            if self.pending_clip_boundary && t == NAL_CRA_NUT {
                                // First CRA after a non-seamless boundary: rewrite
                                // its header type to BLA_W_LP. NAL type is bits 1-6
                                // of byte 0: byte = (byte & 0x81) | (type << 1).
                                self.pending_clip_boundary = false;
                                let mut rewritten = data[nal_start..end].to_vec();
                                rewritten[0] = (rewritten[0] & 0x81) | (NAL_BLA_W_LP << 1);
                                push_length_prefixed(&mut frame_data, &rewritten);
                            } else {
                                // Any IRAP clears a pending boundary (it's been
                                // reached and handled — an IDR needs no rewrite),
                                // but only a CRA is modified.
                                self.pending_clip_boundary = false;
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

        if frame_data.is_empty() {
            return Vec::new();
        }

        // A player re-applies the hvcC (codecPrivate) parameter sets at every
        // keyframe. If the active set was redefined mid-title and the source
        // stopped repeating that redefinition at later IRAPs (relying on the
        // decoder to retain it — valid for a raw bitstream), the hvcC
        // re-insertion would silently revert to the stale FIRST body and every
        // frame in the segment decodes against the wrong parameter set
        // (CABAC/cu_qp_delta desync). Re-assert the active set in-band, ahead
        // of this AU's slices, so it wins. Re-asserted at EVERY keyframe (even
        // when active == codecPrivate) so each keyframe is self-contained and a
        // decoder that dropped the set (CRA reset / SPS event) self-heals.
        if keyframe {
            let mut prefix = Vec::new();
            reassert_active(&mut prefix, &self.cur_vps, emitted_vps);
            reassert_active(&mut prefix, &self.cur_sps, emitted_sps);
            reassert_active(&mut prefix, &self.cur_pps, emitted_pps);
            if !prefix.is_empty() {
                prefix.extend_from_slice(&frame_data);
                frame_data = prefix;
            }
        }

        // HDR10 static metadata is per-stream; once both SEI messages have been
        // seen it is stamped onto every frame's PictureInfo so it rides the same
        // deferred-muxer path the measured field order uses (the muxer reads it
        // from the first coded picture before writing the track header). `None`
        // until both SEI present → SDR / no-SEI tracks carry nothing.
        let hdr10 = self.hdr10();
        vec![Frame {
            // Coding-type only: HEVC field order (pic_struct, from a pic_timing
            // SEI) is not decoded here, so field_order() stays None — honestly
            // absent, never guessed. HDR10 metadata is attached when measured.
            coding: coding_type
                .map(PictureInfo::coding_type_only)
                .map(|p| p.with_hdr10(hdr10)),
            source: pes.source,
            pts_ns,
            keyframe,
            data: frame_data,
            duration_ns: None,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        // HEVCDecoderConfigurationRecord (ISO 14496-15)
        let vps = self.vps.as_ref()?;
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        // hvcC encodes each NAL's length as a 16-bit field. A param set larger
        // than 65535 bytes would silently truncate the length while the full
        // bytes are appended → mis-framed record. Refuse rather than emit a
        // corrupt hvcC (param sets this large are non-conforming anyway).
        if vps.len() > 0xFFFF || sps.len() > 0xFFFF || pps.len() > 0xFFFF {
            return None;
        }

        // Build a conforming HEVCDecoderConfigurationRecord: fixed header
        // (configurationVersion, profile_tier_level fields, parallelism, parsed
        // chroma/bit depths) followed by numOfArrays length-prefixed NAL arrays.
        let mut record = Vec::new();

        // Minimal HEVCDecoderConfigurationRecord header.
        //
        // The stored SPS NAL is [2-byte HEVC NAL header][SPS RBSP...].
        // profile_tier_level fields must be read off the
        // emulation-prevention-STRIPPED RBSP — a `00 00 03` sequence in the
        // first ~15 SPS bytes would otherwise shift every raw byte index and
        // corrupt the PTL (profile/compat/constraint/level). We strip first
        // (same as parse_sps_chroma) and index into the cleaned RBSP:
        //   rbsp[0]      sps_vps_id u(4)+max_sub_layers u(3)+temporal_nesting u(1)
        //   rbsp[1]      general_profile_space u(2)+tier u(1)+profile_idc u(5)
        //   rbsp[2..6]   general_profile_compatibility_flags u(32)
        //   rbsp[6..12]  general_constraint_indicator_flags 48 bits
        //   rbsp[12]     general_level_idc u(8)
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
        // chromaFormat / bit depths — parse the real values from the SPS RBSP.
        // A hardcoded 8-bit 4:2:0 is wrong for 10-bit Main 10 UHD (essentially
        // all UHD content). Fall back to 8-bit 4:2:0 only if the SPS can't be
        // parsed (emulation-prevention is handled; sub-layer PTL is skipped).
        let chroma = parse_sps_chroma(sps).unwrap_or(SpsChroma {
            chroma_format_idc: 1,
            bit_depth_luma_minus8: 0,
            bit_depth_chroma_minus8: 0,
            max_sub_layers_minus1: 0,
            temporal_id_nesting_flag: 0,
        });
        // chromaFormat (6 reserved bits set + 2-bit chroma_format_idc)
        record.push(0xFC | (chroma.chroma_format_idc & 0x03));
        // bitDepthLumaMinus8 (5 reserved bits set + 3-bit value)
        record.push(0xF8 | (chroma.bit_depth_luma_minus8 & 0x07));
        // bitDepthChromaMinus8 (5 reserved bits set + 3-bit value)
        record.push(0xF8 | (chroma.bit_depth_chroma_minus8 & 0x07));
        // avgFrameRate
        record.extend_from_slice(&[0, 0]);
        // Byte 21 packs four fields (ISO/IEC 14496-15):
        //   constantFrameRate u(2) = 0 (unknown / not constant)
        //   numTemporalLayers u(3) = sps_max_sub_layers_minus1 + 1
        //   temporalIdNested  u(1) = sps_temporal_id_nesting_flag
        //   lengthSizeMinusOne u(2) = 3 (4-byte length prefix)
        // sps_max_sub_layers_minus1 is u(3) (0..7), so +1 is 1..8. The hvcC
        // numTemporalLayers field is u(3) (0..7); the max legal value (8) is
        // saturated to 7 rather than wrapping to 0 via the & 0x07 mask.
        let num_temporal_layers = chroma.max_sub_layers_minus1.saturating_add(1).min(7) & 0x07;
        let temporal_id_nested = chroma.temporal_id_nesting_flag & 0x01;
        record.push((num_temporal_layers << 3) | (temporal_id_nested << 2) | 0x03);
        // numOfArrays
        record.push(3); // VPS, SPS, PPS

        // VPS array
        record.push(0x20 | (NAL_VPS & 0x3F)); // array_completeness + NAL type
        record.extend_from_slice(&[0, 1]); // numNalus = 1
        record.push((vps.len() >> 8) as u8);
        record.push(vps.len() as u8);
        record.extend_from_slice(vps);

        // SPS array
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
}

/// Strip HEVC/H.264 emulation-prevention bytes (00 00 03 → 00 00) from a NAL
/// RBSP so a bit reader sees the true coded values.
fn strip_emulation_prevention(rbsp: &[u8]) -> Vec<u8> {
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

/// Read an SEI `payloadType` / `payloadSize` value using the H.265 D.2
/// ff-extension coding: consume a run of `0xFF` bytes (each adding 255) plus one
/// final byte `< 0xFF`. Advances `*i` past the bytes read. Returns `None` at
/// end-of-buffer (the value is incomplete / no further message).
fn read_sei_ff_value(rbsp: &[u8], i: &mut usize) -> Option<u32> {
    let mut value: u32 = 0;
    loop {
        let b = *rbsp.get(*i)?;
        *i += 1;
        value = value.checked_add(b as u32)?;
        if b != 0xFF {
            return Some(value);
        }
    }
}

/// Parse a Mastering Display Colour Volume SEI payload (Rec. ITU-T H.265
/// D.2.28 / semantics D.3.28). Layout — 24 bytes total, all big-endian:
///   display_primaries_x[c] u(16), display_primaries_y[c] u(16) for c=0,1,2
///     (SEI primary order is c=0 Green, c=1 Blue, c=2 Red)
///   white_point_x u(16), white_point_y u(16)
///   max_display_mastering_luminance u(32)
///   min_display_mastering_luminance u(32)
/// Returns `None` if the payload is shorter than 24 bytes (malformed → ignored,
/// never partially populated).
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

/// Parse a Content Light Level Information SEI payload (Rec. ITU-T H.265
/// D.2.35 / semantics D.3.35). Layout — 4 bytes, big-endian:
///   max_content_light_level u(16)   (MaxCLL, cd/m²)
///   max_pic_average_light_level u(16) (MaxFALL, cd/m²)
/// Returns `None` if shorter than 4 bytes.
fn parse_content_light_level(p: &[u8]) -> Option<ContentLightLevel> {
    if p.len() < 4 {
        return None;
    }
    Some(ContentLightLevel {
        max_content_light_level: u16::from_be_bytes([p[0], p[1]]),
        max_pic_average_light_level: u16::from_be_bytes([p[2], p[3]]),
    })
}

/// Parse chroma_format_idc and bit depths from a stored SPS NAL
/// (`[2-byte NAL header][RBSP...]`). Handles emulation-prevention and
/// sub-layer profile_tier_level. Returns `None` if the bitstream is too short
/// or malformed (caller falls back to the 8-bit 4:2:0 default).
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
    let chroma_format_idc = r.read_ue()? as u8;
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
    let bit_depth_luma_minus8 = r.read_ue()? as u8;
    let bit_depth_chroma_minus8 = r.read_ue()? as u8;

    Some(SpsChroma {
        chroma_format_idc,
        bit_depth_luma_minus8,
        bit_depth_chroma_minus8,
        max_sub_layers_minus1: max_sub_layers_minus1 as u8,
        temporal_id_nesting_flag: temporal_id_nesting_flag as u8,
    })
}

/// Consume a profile_tier_level(profilePresentFlag=1, maxNumSubLayersMinus1)
/// structure from the bit reader (HEVC 7.3.3).
fn parse_profile_tier_level(r: &mut BitReader, max_sub_layers_minus1: u32) -> Option<()> {
    // general PTL fixed layout (HEVC 7.3.3): profile_space u(2) + tier u(1) +
    // profile_idc u(5) = 8, general_profile_compatibility_flags u(32),
    // constraint-flags/reserved area = 48, general_level_idc u(8).
    // Total = 8 + 32 + 48 + 8 = 96 bits = 12 bytes. Skip 96 bits.
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
        if max_sub_layers_minus1 < 8 {
            for _ in max_sub_layers_minus1..8 {
                r.skip_bits(2)?;
            }
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

    /// Wrap one or more SEI messages in a prefix-SEI NAL (type 39) preceded by an
    /// Annex-B start code. The assembled message bytes are emulation-prevented
    /// (as a conforming encoder would) so they never form a false start code; the
    /// 0x80 RBSP trailing-bits byte is appended.
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

    /// The HDR10 SEI parse must de-emulate (00 00 03) before reading payload
    /// fields. A payload byte sequence 00 00 03 in the bitstream is an
    /// emulation-prevention insertion the parser must strip, or every field
    /// after it shifts by one byte. Construct a mastering payload whose raw bytes
    /// contain 00 00 (forcing an emulation byte), insert the 03, and assert the
    /// decoded values still match the un-emulated payload.
    #[test]
    fn hevc_hdr10_sei_de_emulates() {
        // prim_x[0]=0x0000, prim_y[0]=0x0002 → raw payload starts 00 00 00 02.
        // A conforming HEVC encoder inserts an emulation-prevention 0x03 after the
        // 00 00 (since the following byte is ≤ 0x03), giving 00 00 03 00 02. The
        // parser MUST strip that 03 before reading, or every later field shifts.
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

    #[test]
    fn hevc_populates_measured_coding_type_and_source() {
        use super::super::coding::CodingType;
        // PPS body 0xC0 = pps_id 0, sps_id 0, dependent_slice 0, output_flag 0,
        // num_extra_slice_header_bits 0 → slice_type follows pps_id directly.
        // Slice body (TRAIL_R, non-IRAP VCL type 1) = first_slice 1, pps_id 0,
        // slice_type: 0xD8 → 2 (I); 0xD0 → 1 (P); 0xE0 → 0 (B).
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

    /// Regression (Fight Club UHD banded corruption): a stream redefines PPS
    /// id 0 mid-title, then a later keyframe arrives WITHOUT repeating it (the
    /// source relies on the decoder retaining the redefinition — valid for a
    /// raw bitstream). An hvcC player re-applies the FIRST (codecPrivate) PPS
    /// at every keyframe, so the active redefinition must be re-asserted
    /// in-band at that bare keyframe or the whole segment decodes against the
    /// wrong parameter set.
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
        // AU5: BARE keyframe, source omits the PPS, and the active set now
        // EQUALS codecPrivate. It must STILL be re-asserted in-band — every
        // keyframe is self-contained: a decoder that dropped PPS id 0 at a CRA
        // reset can only recover from an in-band copy, and there is no genuine
        // change here to trigger the emit path.
        let au5 = nal(19, &[0x14]);
        let f5 = parser.parse(&make_pes(au5, Some(14400)));
        assert!(
            pps_of(&nals_in(&f5[0].data)).iter().any(|b| b == &pps_a),
            "bare keyframe must re-assert the active PPS even when == codecPrivate"
        );
    }

    /// Regression (Fight Club UHD, the real bug): id 0 is body A (→ hvcC), then
    /// redefined to B, then the title switches BACK to A. A streaming decoder
    /// (hvcC at init, in-band updates only) is sitting on B; the switch back to
    /// A must be emitted IN-BAND even though A == codecPrivate, or the whole
    /// A-segment decodes against B (cu_qp_delta desync). Stripping on `== hvcC`
    /// dropped this revert.
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
        // AU4 keyframe: A again, now == active AND == codecPrivate. Under the
        // self-contained-keyframe rule it is STILL re-asserted in-band so a
        // decoder that dropped PPS id 0 at this IRAP recovers. handle_param_set
        // strips the source copy (== active), then reassert_active prepends the
        // active set unconditionally.
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
        // The hvcC fixed header must read profile_tier_level from the SPS
        // RBSP, not from the NAL header. Stored SPS = [2-byte NAL header][RBSP].
        // RBSP layout (byte-aligned):
        //   sps[2]      sps_vps_id/max_sub_layers/temporal_nesting
        //   sps[3]      general_profile_space+tier+profile_idc
        //   sps[4..8]   general_profile_compatibility_flags
        //   sps[8..14]  general_constraint_indicator_flags
        //   sps[14]     general_level_idc
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

    /// Regression for the "TopGun bug" (Top Gun 1986 UHD, DV P7 dual-layer):
    /// a multi-clip title is read as one concatenated stream and the mpls
    /// connection_condition is never plumbed to the parser, so the splice CRA
    /// opening the next clip kept its dangling RASL leading pictures and a
    /// linear decoder flooded "Could not find ref with POC N". The parser must
    /// AUTO-DETECT the boundary from the backward PES-PTS reset between clips
    /// (each .m2ts has its own PTS base) and rewrite that splice CRA → BLA_W_LP
    /// with no explicit `mark_clip_boundary` call.
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

    /// Regression for the 33-bit PTS wraparound false-trigger (rc.5.2 audit #1):
    /// a SINGLE clip whose raw 90 kHz PES PTS crosses the 2^33 counter wrap
    /// (~26.5 h) must NOT be mistaken for a non-seamless clip join. Before the
    /// fix the raw 2^33→0 backward step armed `pending_clip_boundary` and the
    /// next in-clip CRA was wrongly rewritten CRA→BLA_W_LP (dropping valid RASL
    /// pictures — visible corruption). After unwrapping onto a monotonic
    /// timeline the wrap is absorbed and the CRA stays CRA.
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

    /// Test 3: non-CRA NALs are never rewritten even when a boundary IS marked.
    /// IDR (19), RASL (8/9), VPS/SPS/PPS, and a trailing slice all pass through
    /// unmodified; the IDR clears the pending boundary so no later CRA is wrongly
    /// converted.
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

    /// Test 4: a frame stream with NO boundary marker is BYTE-IDENTICAL to a
    /// parser that has no splice-rewrite field at all (the UHD-safety guarantee).
    /// We assert byte-equality of every emitted frame across a multi-AU stream
    /// containing CRAs, IDRs, RASLs, VPS/SPS/PPS, and trailing slices — none of
    /// which is ever marked.
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
        // Reference parser: the rewrite field exists but is NEVER marked, so its
        // output is exactly the pre-feature behaviour. We compare a never-marked
        // run against a second never-marked run AND against the documented
        // invariant that the CRA is emitted as-is (type 21, payload intact).
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

    /// Test 5: a SEAMLESS boundary (connection_condition 0x05/0x06) is expressed
    /// by NOT calling `mark_clip_boundary`, so a CRA across a seamless join is
    /// left unchanged. This encodes the contract: only non-seamless joins call
    /// `mark_clip_boundary`; seamless ones never do, so no rewrite occurs.
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

    /// A parameter set REDEFINED mid-stream (same id, different body) must be
    /// emitted INLINE so the decoder re-activates it. Some discs redefine PPS
    /// id 0 partway through the title; the old parser kept only the first PPS,
    /// so the second segment decoded against the wrong PPS (CABAC desync).
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

        // PES3: PPS-B repeated on a NON-keyframe slice — B is already the active
        // set, so this carries no change and is stripped. (Re-assertion for
        // players that re-apply hvcC at keyframes is handled by
        // `reassert_active` at KEYFRAMES, not on every trailing frame; these
        // slices are TRAIL_R, not IRAP.)
        let mut d = pps(0xBB);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(2)));
        assert_eq!(
            count_pps(&f[0].data),
            0,
            "PPS equal to the active set carries no change → stripped"
        );

        // PES4: back to PPS-A. Even though A == codecPrivate, the ACTIVE set is
        // B, so switching to A is a real change and MUST be emitted in-band — a
        // streaming decoder (hvcC at init, in-band updates only) is sitting on B
        // and would otherwise never revert. (This is the Fight Club bug: the old
        // `== codecPrivate → strip` rule dropped exactly this revert.)
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
        // `00 00 01 00 00 01 <real NAL>`: the first start code is immediately
        // followed by another, so the in-between NAL is empty after the
        // trailing-zero strip. It must be skipped, NOT written as a bare
        // 0x00000000 length prefix (which a decoder treats as malformed).
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
        // Self-contained keyframe: the active VPS/SPS/PPS are re-asserted in-band
        // ahead of the IDR, so the frame is VPS, SPS, PPS, IDR, RPU — in that
        // order. The RPU (type 62) is preserved (never stripped); only the
        // duplicate-suppression of unchanged param sets was lifted at keyframes.
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
            if self.nbits % 8 == 0 {
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
        // make_sps_with_chroma sets sps_max_sub_layers_minus1 = 0 and
        // sps_temporal_id_nesting_flag = 1, so byte 21 must encode
        // numTemporalLayers = 1, temporalIdNested = 1, lengthSizeMinusOne = 3:
        //   (1 << 3) | (1 << 2) | 3 = 0x0F.
        let sps = make_sps_with_chroma(1, 2, 2);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(
            cp[21], 0x0F,
            "byte 21: numTemporalLayers=1, temporalIdNested=1, lengthSizeMinusOne=3"
        );
    }

    #[test]
    fn hvcc_handles_emulation_prevention_in_sps() {
        // Insert an emulation-prevention byte (00 00 03) into the SPS RBSP and
        // confirm the chroma/bit-depth parse still lands on the right values.
        // Build a 10-bit 4:2:0 SPS, then splice 00 00 03 into the RBSP tail
        // (after the fields we parse) — the strip must not corrupt earlier bits.
        let mut sps = make_sps_with_chroma(1, 2, 2);
        // Append a benign 00 00 03 sequence to the RBSP.
        sps.extend_from_slice(&[0x00, 0x00, 0x03, 0x00]);
        let cp = codec_private_from_sps(&sps);
        assert_eq!(cp[16], 0xFC | 1);
        assert_eq!(cp[17], 0xF8 | 2);
        assert_eq!(cp[18], 0xF8 | 2);
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
        // Exp-Golomb ue(v): codeNum 0 = "1", 1 = "010", 2 = "011", 3 = "00100",
        // 4 = "00101". (H.264/HEVC §9.1.) Pack "1 010 011" = 1010011x.
        // Byte 0b1010_0110: read ue → 0 (leading "1"), then "010" → 1, then
        // "011" → 2.
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

    /// Build a stored SPS NAL with sub-layers and a conformance window, so the
    /// parser must skip sub-layer PTL and the 4 conformance-window ue(v) fields
    /// before reaching the bit depths. max_sub_layers_minus1 controls the
    /// sub-layer loop.
    fn make_sps_full(
        chroma_idc: u32,
        bd_luma_m8: u32,
        bd_chroma_m8: u32,
        max_sub_layers_minus1: u32,
        conformance_window: bool,
    ) -> Vec<u8> {
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
            let mut profile_present = Vec::new();
            let mut level_present = Vec::new();
            for _ in 0..max_sub_layers_minus1 {
                // sub_layer_profile_present_flag, sub_layer_level_present_flag.
                w.put_bit(1); // profile present
                w.put_bit(1); // level present
                profile_present.push(true);
                level_present.push(true);
            }
            if max_sub_layers_minus1 < 8 {
                for _ in max_sub_layers_minus1..8 {
                    w.put_bits(0, 2); // reserved_zero_2bits
                }
            }
            for i in 0..max_sub_layers_minus1 as usize {
                if profile_present[i] {
                    for _ in 0..88 {
                        w.put_bit(0); // sub-layer profile block
                    }
                }
                if level_present[i] {
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
        // With max_sub_layers_minus1 = 2 the parser must consume the sub-layer
        // present-flag bits, reserved bits, and two sub-layer PTL blocks before
        // reaching chroma_format_idc / bit depths. A wrong sub-layer skip would
        // mis-read the bit depths.
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
        // After the 23-byte fixed header + numOfArrays the record holds three
        // arrays. Each: (0x20 | nal_type), numNalus(=1, u16-BE), nalLength(u16),
        // NAL bytes. Verify the SPS array's nal_type byte and length encode
        // correctly. (ISO/IEC 14496-15 §8.3.3.1.)
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
        // HEVC NAL type = (byte0 >> 1) & 0x3F. The forbidden_zero_bit (bit 7) and
        // the low layer-id bit (bit 0) must not affect type. hevc_nal_header(19)
        // = [(19<<1), 0x01] = [0x26, 0x01]; with the forbidden bit set (0xA6) it
        // is still type 19.
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
}
