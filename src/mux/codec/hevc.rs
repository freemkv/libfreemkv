//! HEVC (H.265) elementary stream parser.
//!
//! Extracts VPS, SPS, PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IRAP pictures: IDR, CRA, BLA).
//! Each PES packet = one access unit = one frame.

use super::h264::{find_start_code, skip_start_code};
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

// HEVC NAL unit types
const NAL_VPS: u8 = 32;
const NAL_SPS: u8 = 33;
const NAL_PPS: u8 = 34;
const NAL_AUD: u8 = 35;
// Dolby Vision RPU (Reference Processing Unit) — NAL type 62 (UNSPEC62).
// This is NOT filtered: all NAL types except VPS/SPS/PPS/AUD pass through
// to frame data, so DV enhancement layer RPU NALs are preserved automatically.
const _NAL_UNSPEC62_DV_RPU: u8 = 62;
// IRAP types (keyframes): BLA, IDR, CRA
const NAL_BLA_W_LP: u8 = 16;
const NAL_RSV_IRAP_VCL23: u8 = 23;

pub struct HevcParser {
    // First-seen parameter set of each type → seeds the MKV codecPrivate (hvcC).
    // This is the ONLY copy the player gets out-of-band, and a player re-applies
    // it at every keyframe (ffmpeg's hvcC→Annex-B insertion). A stream may
    // redefine a parameter set mid-title under the SAME id with a different body
    // (Fight Club redefines PPS id 0 partway through). Any occurrence whose body
    // DIFFERS from this codecPrivate copy must therefore be emitted IN-BAND at
    // each point it appears (i.e. at every keyframe of the redefined segment) so
    // it overrides the re-applied codecPrivate set; otherwise those frames decode
    // against the wrong parameter set → CABAC/cu_qp_delta desync.
    vps: Option<Vec<u8>>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

impl Default for HevcParser {
    fn default() -> Self {
        Self::new()
    }
}

impl HevcParser {
    pub fn new() -> Self {
        Self {
            vps: None,
            sps: None,
            pps: None,
        }
    }
}

/// Handle a VPS/SPS/PPS NAL.
///
/// - First of its type → seeds codecPrivate (`first`); stripped from frame data
///   (the player gets it from hvcC).
/// - Identical to the codecPrivate copy → stripped (the player already re-applies
///   it from hvcC at each keyframe; BD streams repeat param sets at every IRAP).
/// - DIFFERENT body from the codecPrivate copy (a mid-title redefinition of the
///   same id) → emitted IN-BAND (length-prefixed) at EVERY occurrence, so it
///   overrides the hvcC copy the player re-applies at each keyframe. Emitting it
///   only once is not enough — the next keyframe's hvcC re-insertion would revert
///   it. This matches what a conforming muxer produces and fixes the Fight Club
///   PPS-id-0 redefinition.
fn handle_param_set(first: &mut Option<Vec<u8>>, nal: &[u8], frame_data: &mut Vec<u8>) {
    match first {
        None => {
            first.replace(nal.to_vec()); // seeds codecPrivate; stripped here
        }
        Some(f) if f.as_slice() == nal => {} // == codecPrivate → player has it
        Some(_) => {
            // Differs from codecPrivate → emit in-band so it wins at this AU.
            frame_data.extend_from_slice(&(nal.len() as u32).to_be_bytes());
            frame_data.extend_from_slice(nal);
        }
    }
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
        let data = &pes.data;
        let mut keyframe = false;
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

                if nal_start < data.len() {
                    // HEVC NAL header: 2 bytes. Type is bits 1-6 of first byte.
                    let nal_type = (data[nal_start] >> 1) & 0x3F;

                    match nal_type {
                        NAL_VPS => {
                            handle_param_set(&mut self.vps, &data[nal_start..end], &mut frame_data)
                        }
                        NAL_SPS => {
                            handle_param_set(&mut self.sps, &data[nal_start..end], &mut frame_data)
                        }
                        NAL_PPS => {
                            handle_param_set(&mut self.pps, &data[nal_start..end], &mut frame_data)
                        }
                        NAL_AUD => {} // Skip access unit delimiters
                        t if (NAL_BLA_W_LP..=NAL_RSV_IRAP_VCL23).contains(&t) => {
                            keyframe = true;
                            let nal = &data[nal_start..end];
                            frame_data.extend_from_slice(&(nal.len() as u32).to_be_bytes());
                            frame_data.extend_from_slice(nal);
                        }
                        _ => {
                            // All other NAL types (slices, SEI, DV RPU, etc.) pass through
                            let nal = &data[nal_start..end];
                            frame_data.extend_from_slice(&(nal.len() as u32).to_be_bytes());
                            frame_data.extend_from_slice(nal);
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

        vec![Frame {
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

        // Simplified: store as arrays in Annex B format
        // Full HEVCDecoderConfigurationRecord is complex — for now, concatenate
        let mut record = Vec::new();

        // Minimal HEVCDecoderConfigurationRecord header.
        //
        // The stored SPS NAL is [2-byte HEVC NAL header][SPS RBSP...].
        // The RBSP begins at sps[2]; profile_tier_level() begins one byte
        // later, after sps_video_parameter_set_id u(4) +
        // sps_max_sub_layers_minus1 u(3) + sps_temporal_id_nesting_flag u(1)
        // (= sps[2], a full byte). So the profile_tier_level fields are:
        //   sps[3]      general_profile_space u(2)+tier u(1)+profile_idc u(5)
        //   sps[4..8]   general_profile_compatibility_flags u(32)
        //   sps[8..14]  general_constraint_indicator_flags 48 bits
        //   sps[14]     general_level_idc u(8)
        // (Byte-aligned read; emulation-prevention bytes within the first
        // 15 SPS bytes are not handled — extremely rare and matches the
        // pre-existing simplification.)
        record.push(1); // configurationVersion
        // general_profile_space + general_tier_flag + general_profile_idc
        record.push(if sps.len() > 3 { sps[3] } else { 0 });
        // general_profile_compatibility_flags (4 bytes) — SPS bytes 4..8
        if sps.len() > 7 {
            record.extend_from_slice(&sps[4..8]);
        } else {
            let avail = sps.len().saturating_sub(4).min(4);
            record.extend_from_slice(&sps[sps.len().min(4)..sps.len().min(8)]);
            record.extend_from_slice(&vec![0u8; 4 - avail]);
        }
        // general_constraint_indicator_flags (6 bytes) — SPS bytes 8..14
        if sps.len() > 13 {
            record.extend_from_slice(&sps[8..14]);
        } else {
            let avail = sps.len().saturating_sub(8).min(6);
            record.extend_from_slice(&sps[sps.len().min(8)..sps.len().min(14)]);
            record.extend_from_slice(&vec![0u8; 6 - avail]);
        }
        // general_level_idc — SPS byte 14
        record.push(if sps.len() > 14 { sps[14] } else { 0 });
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
        });
        // chromaFormat (6 reserved bits set + 2-bit chroma_format_idc)
        record.push(0xFC | (chroma.chroma_format_idc & 0x03));
        // bitDepthLumaMinus8 (5 reserved bits set + 3-bit value)
        record.push(0xF8 | (chroma.bit_depth_luma_minus8 & 0x07));
        // bitDepthChromaMinus8 (5 reserved bits set + 3-bit value)
        record.push(0xF8 | (chroma.bit_depth_chroma_minus8 & 0x07));
        // avgFrameRate
        record.extend_from_slice(&[0, 0]);
        // constantFrameRate + numTemporalLayers + temporalIdNested + lengthSizeMinusOne
        record.push(0x03); // lengthSizeMinusOne = 3 (4 bytes)
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
}

/// Minimal MSB-first bit reader over a byte slice.
struct BitReader<'a> {
    data: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit_pos: 0 }
    }

    fn read_bit(&mut self) -> Option<u32> {
        let byte = self.bit_pos / 8;
        if byte >= self.data.len() {
            return None;
        }
        let shift = 7 - (self.bit_pos % 8);
        self.bit_pos += 1;
        Some(((self.data[byte] >> shift) & 1) as u32)
    }

    fn read_bits(&mut self, n: u32) -> Option<u32> {
        let mut v = 0u32;
        for _ in 0..n {
            v = (v << 1) | self.read_bit()?;
        }
        Some(v)
    }

    fn skip_bits(&mut self, n: u32) -> Option<()> {
        for _ in 0..n {
            self.read_bit()?;
        }
        Some(())
    }

    /// Exp-Golomb unsigned, ue(v). Bounded leading-zero count to avoid runaway
    /// on corrupt input.
    fn read_ue(&mut self) -> Option<u32> {
        let mut zeros = 0u32;
        while self.read_bit()? == 0 {
            zeros += 1;
            if zeros > 31 {
                return None;
            }
        }
        if zeros == 0 {
            return Some(0);
        }
        let rest = self.read_bits(zeros)?;
        Some((1u32 << zeros) - 1 + rest)
    }
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
    r.skip_bits(1)?;

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
    })
}

/// Consume a profile_tier_level(profilePresentFlag=1, maxNumSubLayersMinus1)
/// structure from the bit reader (HEVC 7.3.3).
fn parse_profile_tier_level(r: &mut BitReader, max_sub_layers_minus1: u32) -> Option<()> {
    // general: profile_space u(2) + tier u(1) + profile_idc u(5) = 8 bits,
    // profile_compatibility_flags u(32), 4× constraint/flags + reserved = 44
    // bits, general_inbld/reserved = 1 bit (total constraint area 48 bits),
    // general_level_idc u(8). 88 bits = 11 bytes... but the spec packs the
    // general PTL as 8 + 32 + 48 + 8 = 96 bits = 12 bytes. Skip 96 bits.
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
    fn param_sets_stripped_from_frame() {
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

        // Frame data should only have the IDR NAL (length-prefixed)
        let fd = &frames[0].data;
        let length = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]);
        // IDR NAL = 2 bytes header + 2 bytes payload = 4 bytes
        assert_eq!(
            length as usize + 4,
            fd.len(),
            "frame should contain exactly one length-prefixed NAL"
        );
    }

    // --- parameter-set redefinition (Fight Club bug) ---

    /// A parameter set REDEFINED mid-stream (same id, different body) must be
    /// emitted INLINE so the decoder re-activates it. Fight Club redefines PPS
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
                let len =
                    u32::from_be_bytes([fd[o], fd[o + 1], fd[o + 2], fd[o + 3]]) as usize;
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

        // PES3: PPS-B repeated — still differs from codecPrivate(A), so emitted
        // AGAIN. Every keyframe of the redefined segment must carry it, because
        // the player re-applies the hvcC (codecPrivate) copy at each keyframe;
        // emitting once would be reverted at the next keyframe.
        let mut d = pps(0xBB);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(2)));
        assert_eq!(count_pps(&f[0].data), 1, "redefined PPS re-emitted every occurrence");

        // PES4: back to PPS-A (== codecPrivate) → stripped (hvcC supplies it).
        let mut d = pps(0xAA);
        d.extend(slice());
        let f = parser.parse(&make_pes(d, Some(3)));
        assert_eq!(count_pps(&f[0].data), 0, "occurrence equal to codecPrivate stripped");
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
        assert_eq!(
            nal_types.len(),
            2,
            "frame data should have exactly 2 NALs (IDR + RPU), got: {:?}",
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
