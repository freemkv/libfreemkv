//! DVD bitmap subtitle (VobSub) parser.
//!
//! DVD subtitles are carried in PS private stream 1 with sub-stream IDs 0x20-0x3F.
//! Each subtitle display set may span multiple PES packets, but at the MKV level
//! we pass through the raw VobSub packets as-is — the container wraps them.
//!
//! For MKV: codec ID "S_VOBSUB".
//! All frames are keyframes (each is a complete bitmap).

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

pub struct DvdSubParser;

impl Default for DvdSubParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DvdSubParser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for DvdSubParser {
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
    fn passthrough_data() {
        let mut parser = DvdSubParser::new();
        let sub_data = vec![0x00, 0x0A, 0x00, 0x08, 0x01, 0xFF, 0x02, 0x03, 0x04, 0x05];
        let pes = make_pes(sub_data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, sub_data, "VobSub data should pass through unmodified");
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn always_keyframe() {
        let mut parser = DvdSubParser::new();
        for i in 0..3u8 {
            let data = vec![0x00, i, 0x00, i + 1];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "DVD subtitle frames should always be keyframes");
        }
    }

    #[test]
    fn empty_pes_returns_no_frames() {
        let mut parser = DvdSubParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn codec_private_none() {
        let parser = DvdSubParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn no_pts_defaults_to_zero() {
        let mut parser = DvdSubParser::new();
        let pes = make_pes(vec![0x01, 0x02], None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
    }
}
