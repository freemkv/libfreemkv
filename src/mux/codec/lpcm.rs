//! BD/DVD LPCM (Linear PCM) audio parser.
//!
//! BD LPCM PES packets have a 4-byte header:
//!   Bytes 0-1: audio frame number
//!   Byte 2:    reserved
//!   Byte 3:    quantization (bits 7-6), sample rate (bits 5-4), channel assignment (bits 3-0)
//!
//! DVD LPCM (private stream 1, sub-stream 0xA0-0xA7) has a 3-byte header.
//!
//! The raw PCM data follows the header. No framing is needed — each PES
//! payload minus its header is one complete audio frame.
//!
//! For MKV: codec ID "A_PCM/INT/BIG" (BD) or "A_PCM/INT/LIT" (DVD).
//! All frames are keyframes (uncompressed audio).

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

/// BD LPCM header size in bytes.
const BD_LPCM_HEADER_SIZE: usize = 4;

pub struct LpcmParser;

impl Default for LpcmParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LpcmParser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for LpcmParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        // Skip the BD LPCM header (4 bytes).
        // If the PES is too short to contain header + data, return nothing.
        if pes.data.len() <= BD_LPCM_HEADER_SIZE {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame {
            pts_ns,
            keyframe: true,
            data: pes.data[BD_LPCM_HEADER_SIZE..].to_vec(),
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
    fn header_skip_extracts_pcm_data() {
        let mut parser = LpcmParser::new();
        // 4-byte LPCM header + 6 bytes of PCM data
        let header = vec![0x00, 0x01, 0x00, 0b1001_0001]; // frame#=1, quant=24bit, rate=48k, ch=1
        let pcm_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let mut pes_data = header;
        pes_data.extend_from_slice(&pcm_data);

        let pes = make_pes(pes_data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, pcm_data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000); // 90000 ticks = 1 second
    }

    #[test]
    fn always_keyframe() {
        let mut parser = LpcmParser::new();
        for i in 0..5u8 {
            let data = vec![0x00, 0x00, 0x00, 0x00, i, i + 1];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "LPCM frames should always be keyframes");
        }
    }

    #[test]
    fn empty_pes_returns_no_frames() {
        let mut parser = LpcmParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn header_only_pes_returns_no_frames() {
        let mut parser = LpcmParser::new();
        // Exactly 4 bytes = header only, no PCM data
        let pes = make_pes(vec![0x00, 0x01, 0x00, 0x00], Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn codec_private_none() {
        let parser = LpcmParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn pts_conversion() {
        let mut parser = LpcmParser::new();
        // PTS = 0 should give pts_ns = 0
        let pes = make_pes(vec![0; 8], Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames[0].pts_ns, 0);

        // No PTS should default to 0
        let pes_no_pts = make_pes(vec![0; 8], None);
        let frames = parser.parse(&pes_no_pts);
        assert_eq!(frames[0].pts_ns, 0);
    }
}
