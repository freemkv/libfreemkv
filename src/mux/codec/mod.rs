//! Elementary stream codec parsers.
//!
//! Each parser takes PES packets and produces frames suitable for MKV muxing.
//! Responsibilities:
//! - Find frame boundaries
//! - Extract codec initialization data (SPS/PPS, etc.)
//! - Determine keyframe status
//! - Convert PTS from 90kHz to nanoseconds

/// AC-3 / E-AC-3 (Dolby Digital / Digital Plus) elementary-stream parser.
pub mod ac3;
/// Codec-agnostic per-picture coding carrier (`PictureInfo` + accessors).
pub mod coding;
/// DTS / DTS-HD elementary-stream parser.
pub mod dts;
/// DVD bitmap subtitle (VobSub) parser.
pub mod dvdsub;
/// H.264 (AVC) Annex-B elementary-stream parser.
pub mod h264;
/// HEVC (H.265) Annex-B elementary-stream parser.
pub mod hevc;
/// BD/DVD LPCM (Linear PCM) audio parser.
pub mod lpcm;
/// MPEG-2 Video elementary-stream parser.
pub mod mpeg2;
/// HDMV PGS (Presentation Graphics Stream) subtitle parser.
pub mod pgs;
/// Shared MPEG/Annex-B start-code scanning helpers.
pub(crate) mod startcode;
/// Dolby TrueHD / Atmos elementary-stream parser.
pub mod truehd;
/// VC-1 (SMPTE 421M) elementary-stream parser.
pub mod vc1;

pub use coding::{FieldOrder, PictureInfo};

use super::ts::PesPacket;
use crate::disc::Codec;

/// A single frame ready for MKV muxing.
#[derive(Default)]
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
    /// Codec-agnostic per-picture coding info, set by the video parsers that
    /// decode it (MPEG-2 fully; H.264/HEVC/VC-1 coding-type only); `None` for
    /// audio/subtitle frames. Carried additively through the highway and
    /// forwarded onto [`crate::pes::PesFrame::coding`] so the muxer can read
    /// field order / pulldown off the frame instead of assuming it. Default
    /// `None` keeps non-video frames paying nothing.
    pub coding: Option<PictureInfo>,
    /// Source position of this frame's first byte, carried from the demux seam
    /// (where each PES is stamped) through the parser. `None` for synthetic
    /// sources / parsers that don't track it. Forwarded onto
    /// [`crate::pes::PesFrame::source`].
    pub source: Option<crate::pes::SourcePos>,
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
///
/// Used for the audio codecs that have no dedicated parser and whose PES
/// boundaries already line up with frame boundaries (Aac, Mp2, Mp3, Flac,
/// Opus). AC3/DTS/TrueHD have their own parsers; PGS/DvdSub have their own
/// subtitle parsers. Video codecs must NOT use the all-keyframe form of this
/// parser — see `parser_for_codec`.
pub struct PassthroughParser {
    keyframe: bool,
}

impl PassthroughParser {
    /// Create a passthrough parser. Pass `true` for codecs where every PES is
    /// independently decodable (audio / subtitle keyframes), `false` for the
    /// video fallback where no frame-boundary or keyframe detection occurs.
    pub fn new(always_keyframe: bool) -> Self {
        Self {
            keyframe: always_keyframe,
        }
    }
}

impl CodecParser for PassthroughParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let pts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);
        vec![Frame {
            coding: None,
            source: None,
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
        // Video codecs with no dedicated parser. There is no frame-boundary
        // detection here, so a PES carrying multiple access units is emitted as
        // one oversized block — but marking every frame a keyframe (as the
        // audio passthrough does) would explode Cues density and mislead
        // seeking. Use the non-keyframe passthrough and warn that framing is
        // approximate. Mpeg1/Av1 are real Codec variants without a parser yet.
        Codec::Mpeg1 | Codec::Av1 => {
            tracing::warn!(
                target: "mux",
                "no dedicated parser for video codec {:?}; using non-keyframe passthrough (frame boundaries/keyframes not detected)",
                codec
            );
            Box::new(PassthroughParser::new(false))
        }
        // Remaining audio-only codecs (Aac, Mp2, Mp3, Flac, Opus) where PES =
        // frame: all-keyframe passthrough is correct. Subtitle/Unknown also land
        // here; keyframe flag is irrelevant for them.
        Codec::Aac | Codec::Mp2 | Codec::Mp3 | Codec::Flac | Codec::Opus => {
            Box::new(PassthroughParser::new(true))
        }
        Codec::Srt | Codec::Ssa | Codec::Unknown(_) => Box::new(PassthroughParser::new(true)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pes(pts: Option<i64>, data: Vec<u8>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1011,
            pts,
            dts: None,
            data,
        }
    }

    #[test]
    fn unhandled_video_codecs_use_non_keyframe_passthrough() {
        // Mpeg1/Av1 have no dedicated parser. They must NOT be marked
        // all-keyframe (that would explode Cues density and mislead seeking);
        // the non-keyframe passthrough is the safe fallback.
        for codec in [Codec::Mpeg1, Codec::Av1] {
            let mut parser = parser_for_codec(codec, None, false);
            let frames = parser.parse(&pes(Some(9000), vec![0xDE, 0xAD, 0xBE, 0xEF]));
            assert_eq!(frames.len(), 1, "{codec:?}");
            assert!(
                !frames[0].keyframe,
                "{codec:?} must not be flagged keyframe by the fallback parser"
            );
            assert_eq!(frames[0].data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        }
    }

    #[test]
    fn unhandled_audio_codecs_use_keyframe_passthrough() {
        // PES = frame audio codecs: every frame is independently decodable, so
        // all-keyframe passthrough is correct.
        for codec in [Codec::Aac, Codec::Mp2, Codec::Mp3, Codec::Flac, Codec::Opus] {
            let mut parser = parser_for_codec(codec, None, false);
            let frames = parser.parse(&pes(Some(0), vec![0x01, 0x02]));
            assert_eq!(frames.len(), 1, "{codec:?}");
            assert!(frames[0].keyframe, "{codec:?} should be keyframe");
        }
    }
}
