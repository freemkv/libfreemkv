//! H.264 (AVC) elementary stream parser.
//!
//! Extracts SPS and PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IDR slices).
//! Each PES packet = one access unit = one frame.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// H.264 NAL unit types we care about.
const NAL_SLICE: u8 = 1;
const NAL_SLICE_IDR: u8 = 5;
const NAL_SEI: u8 = 6;
const NAL_SPS: u8 = 7;
const NAL_PPS: u8 = 8;
const NAL_AUD: u8 = 9;

pub struct H264Parser {
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

impl H264Parser {
    pub fn new() -> Self {
        Self { sps: None, pps: None }
    }
}

impl CodecParser for H264Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        // Scan NAL units for SPS, PPS, and IDR detection
        let mut keyframe = false;
        let mut frame_data = Vec::new();

        for nal in NalIterator::new(&pes.data) {
            let nal_type = nal[0] & 0x1F;

            match nal_type {
                NAL_SPS => {
                    self.sps = Some(nal.to_vec());
                }
                NAL_PPS => {
                    self.pps = Some(nal.to_vec());
                }
                NAL_SLICE_IDR => {
                    keyframe = true;
                }
                _ => {}
            }
        }

        // Convert Annex B (start code prefixed) to length-prefixed NALUs.
        // MKV with AVCDecoderConfigurationRecord expects 4-byte length prefix per NALU.
        // Skip SPS/PPS/AUD NALUs — they're in codecPrivate, not in frame data.
        for nal in NalIterator::new(&pes.data) {
            let nal_type = nal[0] & 0x1F;
            // Skip parameter sets and access unit delimiters
            if nal_type == NAL_SPS || nal_type == NAL_PPS || nal_type == NAL_AUD {
                continue;
            }
            // 4-byte big-endian length prefix
            let len = nal.len() as u32;
            frame_data.extend_from_slice(&len.to_be_bytes());
            frame_data.extend_from_slice(nal);
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
        // Build AVCDecoderConfigurationRecord from SPS + PPS
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

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

        let mut record = Vec::new();
        record.push(1); // configurationVersion
        record.push(sps[1]); // profile
        record.push(sps[2]); // compatibility
        record.push(sps[3]); // level
        record.push(0xFF); // 6 bits reserved (111111) + 2 bits lengthSizeMinusOne (11 = 3)
        record.push(0xE1); // 3 bits reserved (111) + 5 bits numSPS (1)
        record.push((sps.len() >> 8) as u8);
        record.push(sps.len() as u8);
        record.extend_from_slice(sps);
        record.push(1); // numPPS
        record.push((pps.len() >> 8) as u8);
        record.push(pps.len() as u8);
        record.extend_from_slice(pps);

        Some(record)
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
        if self.pos >= self.data.len() {
            return None;
        }

        // Skip the start code at current position
        let nal_start = skip_start_code(self.data, self.pos)?;

        // Find next start code (or end of data)
        let nal_end = find_start_code(self.data, nal_start).unwrap_or(self.data.len());

        // Remove trailing zeros (part of next start code's zero prefix)
        let mut end = nal_end;
        while end > nal_start && self.data[end - 1] == 0x00 {
            end -= 1;
        }

        self.pos = nal_end;

        if end > nal_start {
            Some(&self.data[nal_start..end])
        } else {
            self.next()
        }
    }
}

/// Find the position of the next start code (00 00 01) at or after `from`.
pub fn find_start_code(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < from + 3 {
        return None;
    }
    for i in from..data.len() - 2 {
        if data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01 {
            return Some(i);
        }
    }
    None
}

/// Skip past the start code at position `pos`, returning the first byte after it.
pub fn skip_start_code(data: &[u8], pos: usize) -> Option<usize> {
    if pos + 2 >= data.len() {
        return None;
    }
    if data[pos] == 0x00 && data[pos + 1] == 0x00 {
        if pos + 3 < data.len() && data[pos + 2] == 0x00 && data[pos + 3] == 0x01 {
            return Some(pos + 4); // 4-byte start code
        }
        if data[pos + 2] == 0x01 {
            return Some(pos + 3); // 3-byte start code
        }
    }
    None
}
