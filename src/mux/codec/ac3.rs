//! AC3 (Dolby Digital) / EAC3 (Dolby Digital Plus) frame parser.
//!
//! AC3 frames are self-contained and always start with syncword 0x0B77.
//! Each PES packet typically contains exactly one AC3 frame.
//! All AC3 frames are effectively keyframes (no inter-frame dependencies).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

pub struct Ac3Parser;

impl Ac3Parser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for Ac3Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        // AC3: each PES = one frame, always a keyframe
        vec![Frame {
            pts_ns,
            keyframe: true,
            data: pes.data.clone(),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        // AC3 doesn't need codecPrivate in MKV
        None
    }
}
