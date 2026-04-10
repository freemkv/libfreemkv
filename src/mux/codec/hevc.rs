//! HEVC (H.265) elementary stream parser.
//!
//! Extracts VPS, SPS, PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IRAP pictures: IDR, CRA, BLA).
//! Each PES packet = one access unit = one frame.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};
use super::h264::{find_start_code, skip_start_code};

// HEVC NAL unit types
const NAL_VPS: u8 = 32;
const NAL_SPS: u8 = 33;
const NAL_PPS: u8 = 34;
const NAL_AUD: u8 = 35;
// IRAP types (keyframes): BLA, IDR, CRA
const NAL_BLA_W_LP: u8 = 16;
const NAL_RSV_IRAP_VCL23: u8 = 23;

pub struct HevcParser {
    vps: Option<Vec<u8>>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

impl HevcParser {
    pub fn new() -> Self {
        Self { vps: None, sps: None, pps: None }
    }
}

impl CodecParser for HevcParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        let data = &pes.data;
        let mut keyframe = false;

        // Scan NAL units
        let mut pos = 0;
        while let Some(sc_pos) = find_start_code(data, pos) {
            if let Some(nal_start) = skip_start_code(data, sc_pos) {
                let next = find_start_code(data, nal_start).unwrap_or(data.len());
                let mut end = next;
                while end > nal_start && data[end - 1] == 0x00 { end -= 1; }

                if nal_start < data.len() {
                    // HEVC NAL header: 2 bytes. Type is bits 1-6 of first byte.
                    let nal_type = (data[nal_start] >> 1) & 0x3F;

                    match nal_type {
                        NAL_VPS => self.vps = Some(data[nal_start..end].to_vec()),
                        NAL_SPS => self.sps = Some(data[nal_start..end].to_vec()),
                        NAL_PPS => self.pps = Some(data[nal_start..end].to_vec()),
                        t if t >= NAL_BLA_W_LP && t <= NAL_RSV_IRAP_VCL23 => {
                            keyframe = true;
                        }
                        _ => {}
                    }
                }
                pos = next;
            } else {
                break;
            }
        }

        // Convert Annex B to length-prefixed NALUs.
        // Skip VPS/SPS/PPS/AUD — they're in codecPrivate.
        let mut frame_data = Vec::new();
        let mut pos = 0;
        while let Some(sc_pos) = find_start_code(&pes.data, pos) {
            if let Some(nal_start) = skip_start_code(&pes.data, sc_pos) {
                let next = find_start_code(&pes.data, nal_start).unwrap_or(pes.data.len());
                let mut end = next;
                while end > nal_start && pes.data[end - 1] == 0x00 { end -= 1; }

                if nal_start < pes.data.len() {
                    let nal_type = (pes.data[nal_start] >> 1) & 0x3F;
                    // Skip parameter sets and AUD
                    if nal_type != NAL_VPS && nal_type != NAL_SPS && nal_type != NAL_PPS && nal_type != NAL_AUD {
                        let nal = &pes.data[nal_start..end];
                        let len = nal.len() as u32;
                        frame_data.extend_from_slice(&len.to_be_bytes());
                        frame_data.extend_from_slice(nal);
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
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        // HEVCDecoderConfigurationRecord (ISO 14496-15)
        let vps = self.vps.as_ref()?;
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        // Simplified: store as arrays in Annex B format
        // Full HEVCDecoderConfigurationRecord is complex — for now, concatenate
        let mut record = Vec::new();

        // Minimal HEVCDecoderConfigurationRecord header
        record.push(1);  // configurationVersion
        // General profile space, tier flag, profile IDC from SPS
        if sps.len() > 3 {
            record.push(sps[1]); // general_profile_space + general_tier_flag + general_profile_idc
        } else {
            record.push(0);
        }
        // general_profile_compatibility_flags (4 bytes)
        record.extend_from_slice(&[0, 0, 0, 0]);
        // general_constraint_indicator_flags (6 bytes)
        record.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
        // general_level_idc
        record.push(if sps.len() > 12 { sps[12] } else { 0 });
        // min_spatial_segmentation_idc (4 + 12 bits)
        record.extend_from_slice(&[0xF0, 0x00]);
        // parallelismType (6 + 2 bits)
        record.push(0xFC);
        // chromaFormat (6 + 2 bits)
        record.push(0xFC | 1); // 4:2:0
        // bitDepthLumaMinus8 (5 + 3 bits)
        record.push(0xF8);
        // bitDepthChromaMinus8 (5 + 3 bits)
        record.push(0xF8);
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
