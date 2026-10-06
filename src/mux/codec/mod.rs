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

pub mod adts;
mod audio_frames;
/// Codec-agnostic per-picture coding carrier (`PictureInfo` + accessors).
pub mod coding;
pub(crate) mod crc;
/// Which pictures decode from the pictures the output holds (start, join, gap).
pub(crate) mod decodable;
pub(crate) mod dropgate;
/// DTS / DTS-HD elementary-stream parser.
pub mod dts;
/// DVD bitmap subtitle (VobSub) parser.
pub mod dvdsub;

pub mod flac;
/// H.264 (AVC) Annex-B elementary-stream parser.
pub mod h264;
/// HEVC (H.265) Annex-B elementary-stream parser.
pub mod hevc;
/// BD/DVD LPCM (Linear PCM) audio parser.
pub mod lpcm;
/// MPEG-2 Video elementary-stream parser.
pub mod mpeg2;

pub(crate) mod mp2_channels;
pub mod mpegaudio;
/// HDMV PGS (Presentation Graphics Stream) subtitle parser.
pub mod pgs;

/// One accumulation buffer for parsers that assemble access units across PES
/// packets, so a unit's timestamp and its source offset always come from the
/// packet that carried its first byte -- and from the SAME packet.
pub(crate) mod pesbuf;
/// Display-order PTS reconstruction for sparse-PTS program-stream video.
pub(crate) mod reorder;
/// Shared MPEG/Annex-B start-code scanning helpers.
pub(crate) mod startcode;
/// Dolby TrueHD / Atmos elementary-stream parser.
pub mod truehd;
/// VC-1 (SMPTE 421M) elementary-stream parser.
pub mod vc1;

pub use coding::{FieldOrder, Hdr10Metadata, PictureInfo};

use super::ts::PesPacket;
use crate::disc::Codec;

/// A single frame ready for MKV muxing.
#[derive(Default)]
pub struct Frame {
    /// Presentation timestamp in nanoseconds.
    pub pts_ns: i64,
    /// Whether this is a keyframe (used for cue points).
    pub keyframe: bool,
    /// This frame is the FIRST coded picture after a concealed/lost gap: its data begins after
    /// packets the demuxer never received (an undecryptable unit concealed as NULL-TS upstream,
    /// or a continuity break in a damaged source). Inter-coded video frames carrying this flag
    /// reference data that is gone, so the consumer's `ResyncGate` arms here and drops forward
    /// to the next keyframe. Default `false`; only ever set on the degraded/conceal path.
    pub discontinuity: bool,
    /// Frame data (elementary stream bytes).
    pub data: Vec<u8>,
    /// Optional duration in nanoseconds — only set by parsers that
    /// can compute one (PGS pairs a display PCS with the following empty PCS;
    /// AC-3, DTS, ADTS/MPEG audio, MPEG-2 and the reorder path also set it). When `Some`, the MKV muxer
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

/// Convert nanoseconds to 90 kHz ticks, rounding to nearest (half up, also below 0):
/// the one ns → tick helper of the mux sinks (design §2.3, J16/J20). It inverts
/// [`pts_to_ns`] exactly, and does not saturate: only `ticks − origin` does.
pub(crate) fn ns_to_ticks(ns: i64) -> i64 {
    // |ns − p·100 000/9| ≤ 0.5 for a pts_to_ns value, a tick error ≤ 4.5·10⁻⁵ < 0.5.
    ns.saturating_mul(9)
        .saturating_add(50_000)
        .div_euclid(100_000)
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

    /// Frames whose in-band config differs from the kept first config.
    fn config_changes(&self) -> u64 {
        0
    }
}

/// Passthrough parser — treats each PES as one frame, no parsing.
///
/// Used for Opus (and any audio codec with no dedicated parser) whose PES
/// boundaries already line up with frame boundaries. AC3/E-AC3, DTS, TrueHD,
/// AAC(ADTS), MP2/MP3 and FLAC now have their own gating parsers; PGS/DvdSub
/// have their own subtitle parsers. Video codecs must NOT use the all-keyframe
/// form of this parser — see `parser_for_codec`.
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
        // Passthrough emits exactly one frame per PES with no cross-PES buffering,
        // so the PES's discontinuity maps directly onto this frame. (Buffering
        // parsers must instead defer the flag to the next emitted frame.)
        vec![Frame {
            coding: None,
            source: pesbuf::PesFacts::of(pes).source,
            pts_ns,
            keyframe: self.keyframe,
            discontinuity: pes.discontinuity,
            data: pes.data.clone(),
            duration_ns: None,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Create the appropriate parser for a codec, with optional codec private
/// data.
///
/// For DvdSub, `codec_data` should be the pre-formatted VobSub .idx palette
/// header. `is_dvd_ps` selects the DVD program-stream variant where it
/// matters: DVD LPCM arrives with its private sub-header already stripped by
/// the `PsDemuxer`, so the LPCM parser must NOT strip the 4-byte BD LPCM
/// header again (that would drop one PCM sample pair per PES → progressive
/// drift).
pub fn parser_for_codec(
    codec: Codec,
    codec_data: Option<Vec<u8>>,
    is_dvd_ps: bool,
) -> Box<dyn CodecParser> {
    match codec {
        // `is_dvd_ps` marks a program-stream source (DVD VOB / HD-DVD EVO), whose
        // video is timestamped only at GOP granularity: H.264/HEVC/VC-1 reconstruct
        // a display-order PTS per frame there; on BD/UHD (per-frame PTS) they don't.
        Codec::H264 => Box::new(h264::H264Parser::new().with_ps_reorder(is_dvd_ps)),
        Codec::Hevc => Box::new(hevc::HevcParser::new().with_ps_reorder(is_dvd_ps)),
        // 11172-2 is the 13818-2 syntax without extension start codes (mpg-output-design v5
        // §4 step 1), so MPEG-1 video is framed per picture by the same parser.
        Codec::Mpeg2 | Codec::Mpeg1 => Box::new(mpeg2::Mpeg2Parser::new()),
        Codec::Vc1 => Box::new(vc1::Vc1Parser::new().with_ps_reorder(is_dvd_ps)),
        Codec::Ac3 | Codec::Ac3Plus => Box::new(ac3::Ac3Parser::new()),
        Codec::Flac => Box::new(flac::FlacParser::new()),
        Codec::Mp2 | Codec::Mp3 => Box::new(mpegaudio::MpegAudioParser::new()),
        Codec::Aac => Box::new(adts::AdtsParser::new()),
        Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => Box::new(dts::DtsParser::new()),
        Codec::TrueHd => Box::new(truehd::TrueHdParser::new()),
        Codec::Pgs => Box::new(pgs::PgsParser::new()),
        Codec::Lpcm if is_dvd_ps => Box::new(lpcm::LpcmParser::new_dvd()),
        Codec::Lpcm => Box::new(lpcm::LpcmParser::new()),
        Codec::DvdSub => Box::new(dvdsub::DvdSubParser::new(codec_data)),
        // Video with no dedicated parser (AV1 is real, just unparsed): a multi-AU PES
        // becomes one oversized block. Non-keyframe passthrough, not all-keyframe (that
        // would explode Cues density); warn that framing is approximate.
        Codec::Av1 => {
            tracing::warn!(
                target: "mux",
                "no dedicated parser for video codec {:?}; using non-keyframe passthrough (frame boundaries/keyframes not detected)",
                codec
            );
            Box::new(PassthroughParser::new(false))
        }
        // Opus (PES = frame): all-keyframe passthrough is correct. Subtitle/Unknown
        // also land here; the keyframe flag is irrelevant for them. (Aac/Mp2/Mp3/Flac
        // have dedicated parsers dispatched earlier in the match.)
        Codec::Opus => Box::new(PassthroughParser::new(true)),
        Codec::Srt | Codec::Ssa | Codec::Unknown(_) => Box::new(PassthroughParser::new(true)),
    }
}

/// Parser for a DVD MPEG-2 multichannel extension track
/// ([`crate::disc::AudioStream::is_mp2_extension`]): one frame per PES, whole. Its payload is
/// ISO/IEC 13818-3 `ext_frame()`s ("ext_syncword - A 12 bit string '0111 1111 1111'",
/// §2.5.2.10), not Layer II frames, so the Layer II parser would split it at false syncs.
pub(crate) fn parser_for_mp2_extension() -> Box<dyn CodecParser> {
    Box::new(PassthroughParser::new(true))
}

/// Build the codec parser for a Blu-ray 3D **MVC dependent (right-eye)** video
/// stream. Same codec space as the base view (H.264), but in param-set
/// passthrough mode so each emitted frame is a self-contained dependent access
/// unit for a Matroska `BlockAdditional`. Non-H.264 (unexpected) falls back to
/// the ordinary parser.
pub fn parser_for_mvc_dependent(codec: Codec, is_dvd_ps: bool) -> Box<dyn CodecParser> {
    match codec {
        Codec::H264 => Box::new(
            h264::H264Parser::new()
                .with_ps_reorder(is_dvd_ps)
                .with_mvc_passthrough(true),
        ),
        _ => parser_for_codec(codec, None, is_dvd_ps),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "mod_provenance_guard_tests.rs"]
mod provenance_guard;
