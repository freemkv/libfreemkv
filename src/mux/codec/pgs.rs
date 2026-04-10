//! HDMV PGS (Presentation Graphics Stream) subtitle parser.
//!
//! PGS segments: PCS, WDS, PDS, ODS, END.
//! Each PES packet contains one or more segments.
//! All segments are keyframes (no inter-segment dependencies).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

pub struct PgsParser;

impl PgsParser {
    pub fn new() -> Self { Self }
}

impl CodecParser for PgsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame { pts_ns, keyframe: true, data: pes.data.clone() }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> { None }
}
