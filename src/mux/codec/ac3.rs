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
        if pes.data.len() < 2 {
            return Vec::new();
        }

        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        // Find AC3 syncword (0x0B77) — skip any garbage before it
        let data = &pes.data;
        let start = find_ac3_sync(data).unwrap_or(0);

        vec![Frame {
            pts_ns,
            keyframe: true,
            data: data[start..].to_vec(),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Find AC3 syncword (0x0B77) in data.
fn find_ac3_sync(data: &[u8]) -> Option<usize> {
    for i in 0..data.len().saturating_sub(1) {
        if data[i] == 0x0B && data[i + 1] == 0x77 {
            return Some(i);
        }
    }
    None
}
