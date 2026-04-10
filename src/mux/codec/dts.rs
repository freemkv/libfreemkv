//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension follows the core frame.
//! All frames are keyframes (no inter-frame dependencies).
//! Each PES packet = one frame.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

pub struct DtsParser;

impl DtsParser {
    pub fn new() -> Self { Self }
}

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame { pts_ns, keyframe: true, data: pes.data.clone() }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> { None }
}
