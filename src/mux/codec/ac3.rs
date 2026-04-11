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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket { pid: 0x1100, pts, dts: None, data }
    }

    // --- syncword detection ---

    #[test]
    fn find_ac3_sync_at_start() {
        let data = [0x0B, 0x77, 0x01, 0x02, 0x03];
        assert_eq!(find_ac3_sync(&data), Some(0));
    }

    #[test]
    fn find_ac3_sync_with_garbage_prefix() {
        let data = [0xFF, 0xFE, 0x0B, 0x77, 0x01, 0x02];
        assert_eq!(find_ac3_sync(&data), Some(2));
    }

    #[test]
    fn find_ac3_sync_none() {
        let data = [0x0B, 0x78, 0x00, 0x00];
        assert_eq!(find_ac3_sync(&data), None);
    }

    #[test]
    fn find_ac3_sync_empty() {
        let data: [u8; 0] = [];
        assert_eq!(find_ac3_sync(&data), None);
    }

    // --- parse syncword → frame extracted ---

    #[test]
    fn parse_syncword() {
        let mut parser = Ac3Parser::new();

        // AC3 frame starting with syncword
        let data = vec![0x0B, 0x77, 0x44, 0x55, 0x66, 0x77, 0x88];
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn parse_syncword_with_garbage_prefix() {
        let mut parser = Ac3Parser::new();

        // Garbage bytes before syncword
        let data = vec![0xFF, 0xFE, 0x0B, 0x77, 0x44, 0x55];
        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        // Data should start from the syncword
        assert_eq!(frames[0].data[0], 0x0B);
        assert_eq!(frames[0].data[1], 0x77);
        assert_eq!(frames[0].data.len(), 4); // syncword + 2 payload bytes
    }

    // --- all frames are keyframes ---

    #[test]
    fn all_keyframes() {
        let mut parser = Ac3Parser::new();

        for i in 0..5 {
            let data = vec![0x0B, 0x77, 0x00, i];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "AC3 frame {} should be a keyframe", i);
        }
    }

    // --- codec_private is None ---

    #[test]
    fn codec_private_none() {
        let parser = Ac3Parser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- empty / too-short PES ---

    #[test]
    fn parse_empty_pes() {
        let mut parser = Ac3Parser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    #[test]
    fn parse_single_byte_pes() {
        let mut parser = Ac3Parser::new();
        let pes = make_pes(vec![0x0B], Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion() {
        let mut parser = Ac3Parser::new();
        let data = vec![0x0B, 0x77, 0x00, 0x01];
        // 45000 ticks = 0.5 seconds → 500_000_000 ns
        let pes = make_pes(data, Some(45000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 500_000_000);
    }

    // --- None PTS ---

    #[test]
    fn no_pts() {
        let mut parser = Ac3Parser::new();
        let data = vec![0x0B, 0x77, 0x00, 0x01];
        let pes = make_pes(data, None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
    }
}
