//! H.264 (AVC) elementary stream parser.
//!
//! Extracts SPS and PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IDR slices).
//! Each PES packet = one access unit = one frame.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

/// H.264 NAL unit types we care about.
const NAL_SLICE_IDR: u8 = 5;
const NAL_SPS: u8 = 7;
const NAL_PPS: u8 = 8;
const NAL_AUD: u8 = 9;

pub struct H264Parser {
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

impl Default for H264Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl H264Parser {
    pub fn new() -> Self {
        Self {
            sps: None,
            pps: None,
        }
    }
}

impl CodecParser for H264Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Use DTS when available (monotonic for B-frame content), fall back to PTS
        let pts_ns = pes.dts.or(pes.pts).map(pts_to_ns).unwrap_or(0);

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

        if sps.len() < 4 {
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
    // Range excludes last 2 bytes since we read 3 bytes at each position.
    // data.len()-2 as exclusive upper bound means last checked index is data.len()-3,
    // which accesses data[len-3], data[len-2], data[len-1] — all valid.
    (from..data.len() - 2).find(|&i| data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01)
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

    // --- find_start_code tests ---

    #[test]
    fn find_start_code_3byte() {
        let data = [0x00, 0x00, 0x01, 0x65];
        assert_eq!(find_start_code(&data, 0), Some(0));
    }

    #[test]
    fn find_start_code_4byte() {
        let data = [0x00, 0x00, 0x00, 0x01, 0x65];
        // find_start_code looks for 00 00 01 pattern, which starts at offset 1 in a 4-byte start code
        assert_eq!(find_start_code(&data, 0), Some(1));
    }

    #[test]
    fn find_start_code_offset() {
        let data = [0xFF, 0xFF, 0x00, 0x00, 0x01, 0x09];
        assert_eq!(find_start_code(&data, 0), Some(2));
    }

    #[test]
    fn find_start_code_none() {
        let data = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
    }

    #[test]
    fn find_start_code_too_short() {
        let data = [0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
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

    // --- SPS/PPS/AUD are stripped from frame data ---

    #[test]
    fn sps_pps_aud_stripped_from_frame_data() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        // AUD (type 9)
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
        // IDR (type 5) - only this should appear in frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65);
        data.extend_from_slice(&[0x88, 0x00]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);

        // Frame data should only contain the IDR NAL (length-prefixed)
        let fd = &frames[0].data;
        let length = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]);
        // IDR NAL is 0x65, 0x88 (trailing 0x00 is stripped as potential start code prefix)
        assert_eq!(length, 2);
        assert_eq!(fd[4], 0x65); // IDR NAL type byte
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

    // --- DTS preferred over PTS when present ---

    #[test]
    fn dts_preferred_over_pts() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41);
        data.extend_from_slice(&[0x00, 0x10]);

        let pes = PesPacket {
            pid: 0x1011,
            pts: Some(180000), // 2 seconds
            dts: Some(90000),  // 1 second
            data,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        // DTS should be used, not PTS
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }
}
