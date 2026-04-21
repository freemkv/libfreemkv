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
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Create the appropriate parser for a codec, with optional codec private data.
///
/// For DvdSub, `codec_data` should be the pre-formatted VobSub .idx palette header.
pub fn parser_for_codec(codec: Codec, codec_data: Option<Vec<u8>>) -> Box<dyn CodecParser> {
    match codec {
        Codec::H264 => Box::new(h264::H264Parser::new()),
        Codec::Hevc => Box::new(hevc::HevcParser::new()),
        Codec::Mpeg2 => Box::new(mpeg2::Mpeg2Parser::new()),
        Codec::Vc1 => Box::new(vc1::Vc1Parser::new()),
        Codec::Ac3 | Codec::Ac3Plus => Box::new(ac3::Ac3Parser::new()),
        Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => Box::new(dts::DtsParser::new()),
        Codec::TrueHd => Box::new(truehd::TrueHdParser::new()),
        Codec::Pgs => Box::new(pgs::PgsParser::new()),
        Codec::Lpcm => Box::new(lpcm::LpcmParser::new()),
        Codec::DvdSub => Box::new(dvdsub::DvdSubParser::new(codec_data)),
        _ => Box::new(PassthroughParser::new(true)),
    }
}
