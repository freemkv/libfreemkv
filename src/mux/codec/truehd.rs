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
