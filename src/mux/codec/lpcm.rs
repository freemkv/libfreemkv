//! BD/DVD LPCM (Linear PCM) audio parser.
//!
//! BD LPCM PES packets (TS stream type 0x80) carry a 4-byte header on the
//! elementary-stream payload:
//!   Bytes 0-1: audio frame number
//!   Byte 2:    reserved
//!   Byte 3:    quantization (bits 7-6), sample rate (bits 5-4), channel assignment (bits 3-0)
//! This header is part of the ES payload, so the BD parser must strip it.
//!
//! DVD LPCM lives in private stream 1 (sub-stream 0xA0-0xA7). Its 7-byte
//! private sub-header (sub_id + frames + first-access-unit-ptr(2) + emphasis +
//! quant/freq + channels) is stripped by `PsDemuxer` while demuxing the
//! Program Stream. By the time a DVD LPCM `PesPacket` reaches this parser its
//! `data` is already raw PCM, so the parser must NOT strip any further bytes —
//! doing so drops one sample pair per PES and drifts the audio.
//!
//! The two origins are distinguished by the `strip_header` flag: BD = strip,
//! DVD = leave intact. The raw PCM data is otherwise one complete audio frame
//! per PES; no framing is needed.
//!
//! For MKV: both BD and DVD LPCM map to codec ID "A_PCM/INT/BIG" (big-endian).
//! DVD-Video LPCM is big-endian per the DVD-Video spec, and `mkv.rs` emits
//! "A_PCM/INT/BIG" unconditionally for `Codec::Lpcm` — there is no DVD/BD branch
//! and no "A_PCM/INT/LIT" path, so no byte-swap or alternate codec ID applies.
//! All frames are keyframes (uncompressed audio).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// BD LPCM header size in bytes (present on BD-TS LPCM, absent on DVD-PS LPCM
/// because `PsDemuxer` already stripped the private sub-header).
const BD_LPCM_HEADER_SIZE: usize = 4;

pub struct LpcmParser {
    /// Whether to strip the 4-byte BD LPCM header from each PES payload.
    ///
    /// `true` for BD-TS LPCM (header still present), `false` for DVD-PS LPCM
    /// (header already removed by `PsDemuxer`).
    strip_header: bool,
}

impl Default for LpcmParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LpcmParser {
    /// BD-TS LPCM parser: strips the 4-byte BD LPCM header from each PES.
    pub fn new() -> Self {
        Self { strip_header: true }
    }

    /// DVD-PS LPCM parser: `PsDemuxer` already stripped the private sub-header,
    /// so the payload is raw PCM and no further bytes are removed.
    pub fn new_dvd() -> Self {
        Self {
            strip_header: false,
        }
    }
}

impl CodecParser for LpcmParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let offset = if self.strip_header {
            BD_LPCM_HEADER_SIZE
        } else {
            0
        };
        // If the PES is too short to contain header + data, return nothing.
        if pes.data.len() <= offset {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame {
            pts_ns,
            keyframe: true,
            data: pes.data[offset..].to_vec(),
            duration_ns: None,
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
    fn bd_lpcm_strips_4_byte_header() {
        // BD-TS LPCM: the 4-byte BD header is part of the ES payload and must
        // be stripped, leaving exactly the PCM bytes.
        let mut parser = LpcmParser::new();
        let header = vec![0x00, 0x01, 0x00, 0b1001_0001];
        let pcm = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let mut data = header;
        data.extend_from_slice(&pcm);

        let frames = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, pcm, "BD must strip exactly 4 header bytes");
    }

    #[test]
    fn dvd_lpcm_preserves_all_pcm_bytes() {
        // DVD-PS LPCM: PsDemuxer already removed the 7-byte private sub-header,
        // so the payload handed to this parser is raw PCM. The DVD parser must
        // NOT strip any further bytes — applying the BD 4-byte strip to a DVD
        // payload would drop one sample pair per PES and progressively drift
        // the audio.
        let mut parser = LpcmParser::new_dvd();
        let pcm = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0x01, 0x02];
        let frames = parser.parse(&make_pes(pcm.clone(), Some(90000)));

        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].data, pcm,
            "DVD must preserve every PCM byte — no second strip"
        );
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn dvd_lpcm_emits_short_payload_bd_would_drop() {
        // A 4-byte raw-PCM DVD payload (1 sample pair at 16-bit stereo). The BD
        // parser drops <= 4 bytes as "header only"; the DVD parser must emit it.
        let mut bd = LpcmParser::new();
        let mut dvd = LpcmParser::new_dvd();
        let pcm = vec![0xAA, 0xBB, 0xCC, 0xDD];

        assert!(
            bd.parse(&make_pes(pcm.clone(), Some(0))).is_empty(),
            "BD treats 4 bytes as header-only"
        );
        let frames = dvd.parse(&make_pes(pcm.clone(), Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, pcm);
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

    // --- BD strip offset boundary ---

    #[test]
    fn bd_five_bytes_yields_one_pcm_byte() {
        // BD strips exactly BD_LPCM_HEADER_SIZE (4). The guard is
        // `data.len() <= offset` (drop), so 5 bytes → 1 PCM byte emitted, not 0.
        let mut parser = LpcmParser::new();
        let f = parser.parse(&make_pes(vec![0x00, 0x01, 0x00, 0x91, 0xAB], Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, vec![0xAB], "5 BD bytes → 1 PCM byte");
    }

    #[test]
    fn bd_three_bytes_dropped() {
        // Fewer than the 4-byte header → dropped, no panic / no underflow slice.
        let mut parser = LpcmParser::new();
        assert!(
            parser
                .parse(&make_pes(vec![0x00, 0x01, 0x00], Some(0)))
                .is_empty()
        );
    }

    // --- DVD strips nothing ---

    #[test]
    fn dvd_one_byte_payload_emitted() {
        // DVD offset is 0, so even a single byte is real PCM and must be emitted
        // (`len <= 0` is false for len 1).
        let mut parser = LpcmParser::new_dvd();
        let f = parser.parse(&make_pes(vec![0xAB], Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, vec![0xAB]);
    }

    #[test]
    fn dvd_empty_payload_dropped() {
        // DVD with an empty payload: `len <= 0` (0 <= 0) → dropped.
        let mut parser = LpcmParser::new_dvd();
        assert!(parser.parse(&make_pes(Vec::new(), Some(0))).is_empty());
    }

    #[test]
    fn bd_default_constructor_strips_header() {
        // Default::default() must build the BD (strip) variant, matching new().
        let mut parser = LpcmParser::default();
        let header = vec![0x00, 0x01, 0x00, 0x91];
        let pcm = vec![0x11, 0x22, 0x33, 0x44];
        let mut data = header;
        data.extend_from_slice(&pcm);
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f[0].data, pcm, "default = BD variant, strips 4 bytes");
    }

    #[test]
    fn lpcm_no_pts_defaults_zero_dvd() {
        // DVD variant with no PTS → pts_ns 0 (unwrap_or(0)).
        let mut parser = LpcmParser::new_dvd();
        let f = parser.parse(&make_pes(vec![0xAA, 0xBB], None));
        assert_eq!(f[0].pts_ns, 0);
    }
}
