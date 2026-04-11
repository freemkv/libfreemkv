//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension follows the core frame.
//! All frames are keyframes (no inter-frame dependencies).
//! Each PES packet = one frame.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

pub struct DtsParser;

impl Default for DtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DtsParser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame {
            pts_ns,
            keyframe: true,
            data: pes.data.clone(),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1100,
            pts,
            dts: None,
            data,
        }
    }

    #[test]
    fn parse_basic_frame() {
        let mut parser = DtsParser::new();
        // DTS core syncword: 7F FE 80 01 + payload
        let data = vec![0x7F, 0xFE, 0x80, 0x01, 0xAA, 0xBB, 0xCC];
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn all_keyframes() {
        let mut parser = DtsParser::new();
        for i in 0..3 {
            let data = vec![0x7F, 0xFE, 0x80, 0x01, i];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "DTS frame should always be keyframe");
        }
    }

    #[test]
    fn codec_private_none() {
        let parser = DtsParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = DtsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn no_pts() {
        let mut parser = DtsParser::new();
        let pes = make_pes(vec![0x7F, 0xFE, 0x80, 0x01], None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
    }
}
