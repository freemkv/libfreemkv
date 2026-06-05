//! Elementary stream codec parsers.
//!
//! Each parser takes PES packets and produces frames suitable for MKV muxing.
//! Responsibilities:
//! - Find frame boundaries
//! - Extract codec initialization data (SPS/PPS, etc.)
//! - Determine keyframe status
//! - Convert PTS from 90kHz to nanoseconds

pub mod ac3;
pub mod dts;
pub mod dvdsub;
pub mod h264;
pub mod hevc;
pub mod lpcm;
pub mod mpeg2;
pub mod pgs;
pub mod truehd;
pub mod vc1;

use super::ts::PesPacket;
use crate::disc::Codec;

/// A single frame ready for MKV muxing.
pub struct Frame {
    /// Presentation timestamp in nanoseconds.
    pub pts_ns: i64,
    /// Whether this is a keyframe (used for cue points).
    pub keyframe: bool,
    /// Frame data (elementary stream bytes).
    pub data: Vec<u8>,
    /// Optional duration in nanoseconds — only set by parsers that
    /// can compute one (currently PGS, which pairs a display PCS
    /// with the following empty PCS). When `Some`, the MKV muxer
    /// emits a `BlockGroup` with `BlockDuration` instead of a
    /// `SimpleBlock`; without it players guess the display interval
    /// (subtitles linger past their end-time).
    pub duration_ns: Option<u64>,
}

/// Convert 90kHz PTS to nanoseconds (round to nearest).
pub fn pts_to_ns(pts: i64) -> i64 {
    // pts * 1_000_000_000 / 90_000 = pts * 100_000 / 9
    // Add half-divisor for rounding: (pts * 100_000 + 4) / 9
    (pts * 100_000 + 4) / 9
}

/// Trait for codec-specific elementary stream parsers.
pub trait CodecParser: Send {
    /// Parse a PES packet into zero or more frames.
    /// Most codecs: one PES = one frame.
    /// Some (TrueHD): multiple access units per PES.
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame>;

    /// Drain any access unit still buffered after the last PES.
    ///
    /// Parsers that buffer across PES boundaries to assemble a complete
    /// access unit (e.g. DTS-HD, whose extension substreams arrive in
    /// separate PES packets) hold the final unit until they can prove it's
    /// complete. At end-of-stream there is no following packet to prove it,
    /// so the demuxer calls `flush()` once after the last PES to emit it.
    /// Default: nothing buffered, no tail.
    fn flush(&mut self) -> Vec<Frame> {
        Vec::new()
    }

    /// Get codec initialization data (e.g., SPS+PPS for H.264).
    /// Returns None until enough data has been seen.
    fn codec_private(&self) -> Option<Vec<u8>>;
}

/// Passthrough parser — treats each PES as one frame, no parsing.
/// Used for codecs where PES = frame (AC3, DTS, PGS).
pub struct PassthroughParser {
    keyframe: bool,
}

impl PassthroughParser {
    pub fn new(always_keyframe: bool) -> Self {
        Self {
            keyframe: always_keyframe,
        }
    }
}

impl CodecParser for PassthroughParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame {
            pts_ns,
            keyframe: self.keyframe,
            data: pes.data.clone(),
            duration_ns: None,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Create the appropriate parser for a codec, with optional codec private data.
///
/// For DvdSub, `codec_data` should be the pre-formatted VobSub .idx palette header.
///
/// `is_dvd_ps` selects the DVD program-stream variant where it matters: DVD
/// LPCM arrives with its private sub-header already stripped by the
/// `PsDemuxer`, so the LPCM parser must NOT strip the 4-byte BD LPCM header
/// again (that would drop one PCM sample pair per PES → progressive drift).
pub fn parser_for_codec(
    codec: Codec,
    codec_data: Option<Vec<u8>>,
    is_dvd_ps: bool,
) -> Box<dyn CodecParser> {
    match codec {
        Codec::H264 => Box::new(h264::H264Parser::new()),
        Codec::Hevc => Box::new(hevc::HevcParser::new()),
        Codec::Mpeg2 => Box::new(mpeg2::Mpeg2Parser::new()),
        Codec::Vc1 => Box::new(vc1::Vc1Parser::new()),
        Codec::Ac3 | Codec::Ac3Plus => Box::new(ac3::Ac3Parser::new()),
        Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => Box::new(dts::DtsParser::new()),
        Codec::TrueHd => Box::new(truehd::TrueHdParser::new()),
        Codec::Pgs => Box::new(pgs::PgsParser::new()),
        Codec::Lpcm if is_dvd_ps => Box::new(lpcm::LpcmParser::new_dvd()),
        Codec::Lpcm => Box::new(lpcm::LpcmParser::new()),
        Codec::DvdSub => Box::new(dvdsub::DvdSubParser::new(codec_data)),
        _ => Box::new(PassthroughParser::new(true)),
    }
}
