//! Dolby TrueHD / Atmos elementary stream parser.
//!
//! TrueHD major sync: 0xF8726FBA at a 4-byte aligned position.
//! Access units consist of a major sync followed by minor syncs.
//! An embedded AC3 core is in substream 0 for backward compatibility.
//! All access units are keyframes.
//! Each PES packet = one access unit.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

pub struct TrueHdParser;

impl TrueHdParser {
    pub fn new() -> Self { Self }
}

impl CodecParser for TrueHdParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame { pts_ns, keyframe: true, data: pes.data.clone() }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket { pid: 0x1100, pts, dts: None, data }
    }

    #[test]
    fn parse_basic_frame() {
        let mut parser = TrueHdParser::new();
        // TrueHD major sync: F8 72 6F BA (at 4-byte aligned position) + payload
        let data = vec![0xF8, 0x72, 0x6F, 0xBA, 0x01, 0x02, 0x03, 0x04];
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn all_keyframes() {
        let mut parser = TrueHdParser::new();
        for i in 0..3 {
            let data = vec![0xF8, 0x72, 0x6F, 0xBA, i];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "TrueHD frame should always be keyframe");
        }
    }

    #[test]
    fn codec_private_none() {
        let parser = TrueHdParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = TrueHdParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }
}
