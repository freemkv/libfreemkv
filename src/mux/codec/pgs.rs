//! HDMV PGS (Presentation Graphics Stream) subtitle parser.
//!
//! PGS segments: PCS, WDS, PDS, ODS, END.
//! Each PES packet contains one or more segments.
//! All segments are keyframes (no inter-segment dependencies).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

pub struct PgsParser;

impl Default for PgsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PgsParser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for PgsParser {
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
            pid: 0x1200,
            pts,
            dts: None,
            data,
        }
    }

    #[test]
    fn parse_basic_segment() {
        let mut parser = PgsParser::new();
        // PGS segment data (PCS = presentation composition segment)
        let data = vec![0x16, 0x00, 0x00, 0x11, 0x01, 0x02, 0x03];
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn all_keyframes() {
        let mut parser = PgsParser::new();
        for i in 0..3 {
            let data = vec![0x16, 0x00, i];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "PGS segment should always be keyframe");
        }
    }

    #[test]
    fn codec_private_none() {
        let parser = PgsParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = PgsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }
}
