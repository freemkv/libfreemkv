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
mod tests {
    use super::*;

    // Design §4 step 1: "MPEG-1 video is `Codec::Mpeg1`, which is routed to `Mpeg2Parser`
    // (the 11172-2 syntax is the 13818-2 syntax without extension start codes)"; the parser
    // must accept a sequence header with no sequence_extension and frame each picture.
    #[test]
    fn mpeg1_video_is_framed_by_the_mpeg2_parser() {
        use crate::mux::decode_ts::test_es::{mpeg1_seq, mpeg2_pic};
        let mut es = mpeg1_seq(3);
        es.extend(mpeg2_pic(1, 3));
        es.extend(mpeg2_pic(2, 3));
        es.extend(mpeg2_pic(3, 3));
        let pes = crate::mux::ts::PesPacket {
            source: None,
            pid: 0xE0,
            pts: Some(9_000),
            dts: None,
            data: es,
            discontinuity: false,
        };
        let mut p = parser_for_codec(Codec::Mpeg1, None, true);
        let mut frames = p.parse(&pes);
        frames.extend(p.flush());
        assert_eq!(frames.len(), 3, "one frame per picture, not one per PES");
        assert!(frames[0].keyframe, "the I picture is a keyframe");
        assert_eq!(p.codec_private(), Some(mpeg1_seq(3)));
    }

    // Per design J16/J20 (MPG3-5, MPG4-2); do not change without a spec citation proving
    // otherwise. Every 90 kHz tick survives pts_to_ns → ns_to_ticks exactly, including
    // every residue mod 9 (100 000 ≡ 1 mod 9) and negative ticks.
    #[test]
    fn disc_ticks_round_trip_through_ns_exactly() {
        let sweep = (-200_000i64..200_000)
            .chain((0..9).map(|r| (1i64 << 33) - 9 + r))
            .chain((0..9).map(|r| -(1i64 << 33) + r))
            .chain([i64::from(u32::MAX), 95_443_717_i64 * 9 + 4]);
        for p in sweep {
            assert_eq!(
                ns_to_ticks(pts_to_ns(p)),
                p,
                "tick {p} (residue {})",
                p.rem_euclid(9)
            );
        }
    }

    // Round half up on the signed line: no saturation inside the helper (MPG4-2), so a
    // negative absolute PTS keeps its distance from the origin.
    #[test]
    fn ns_to_ticks_rounds_to_nearest_signed() {
        assert_eq!(ns_to_ticks(0), 0);
        assert_eq!(ns_to_ticks(5_555), 0, "0.49995 tick");
        assert_eq!(ns_to_ticks(5_556), 1, "0.50004 tick");
        assert_eq!(ns_to_ticks(-5_555), 0, "−0.49995 tick");
        assert_eq!(ns_to_ticks(-5_556), -1, "−0.50004 tick");
        assert_eq!(ns_to_ticks(-40_000_000), -3_600);
        assert_eq!(ns_to_ticks(-80_000_000), -7_200);
        assert_eq!(
            ns_to_ticks(i64::MIN),
            i64::MIN
                .saturating_mul(9)
                .saturating_add(50_000)
                .div_euclid(100_000)
        );
    }

    fn pes(pts: Option<i64>, data: Vec<u8>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1011,
            pts,
            dts: None,
            data,
            discontinuity: false,
        }
    }

    #[test]
    fn unhandled_video_codecs_use_non_keyframe_passthrough() {
        // Av1 has no dedicated parser. It must NOT be marked all-keyframe (that would
        // explode Cues density and mislead seeking); the non-keyframe passthrough is the
        // safe fallback. (MPEG-1 has the MPEG-2 parser: mpg-output-design v5 §4 step 1.)
        let codec = Codec::Av1;
        let mut parser = parser_for_codec(codec, None, false);
        let frames = parser.parse(&pes(Some(9000), vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(frames.len(), 1, "{codec:?}");
        assert!(
            !frames[0].keyframe,
            "{codec:?} must not be flagged keyframe by the fallback parser"
        );
        assert_eq!(frames[0].data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    // codec_private() feeds MKV CodecPrivate (RFC 9559 §5.1.4.1.24): Some vs None are NOT
    // interchangeable.
    #[test]
    fn parsers_that_derive_no_config_report_absent_never_an_empty_codec_private() {
        // Parsers with no config extraction, each fed a REAL frame so the gate
        // keeps it (a rejected frame proves nothing). AAC derives an ASC, so
        // it is tested separately below.
        let cases: [(Codec, Vec<u8>); 4] = [
            // MPEG-1 Layer II, 44.1 kHz, 128 kbit/s, stereo.
            (Codec::Mp2, vec![0xFF, 0xFD, 0x70, 0x00, 0x00, 0x00]),
            // MPEG-1 Layer III, 44.1 kHz, 128 kbit/s, stereo.
            (Codec::Mp3, vec![0xFF, 0xFB, 0x90, 0x00, 0x00, 0x00]),
            // Not a FLAC frame sync, so the gate passes it through unvalidated —
            // still the keep path, and still no configuration derived.
            (Codec::Flac, vec![0x01, 0x02, 0x03, 0x04]),
            // Opus rides the all-keyframe passthrough parser.
            (Codec::Opus, vec![0x78, 0x01, 0x02, 0x03]),
        ];

        for (codec, payload) in cases {
            let mut parser = parser_for_codec(codec, None, false);
            assert_eq!(
                parser.codec_private(),
                None,
                "{codec:?}: no config before any frame"
            );
            parser.parse(&pes(Some(0), payload.clone()));
            parser.parse(&pes(Some(90_000), payload));
            assert_eq!(
                parser.codec_private(),
                None,
                "{codec:?}: this parser derives no config, so it must report the \
                 CodecPrivate as ABSENT — an empty or invented Some would be \
                 written into the track header as if it were real"
            );
            // Flushing at end of stream must not conjure one either.
            parser.flush();
            assert_eq!(parser.codec_private(), None, "{codec:?}: after flush");
        }
    }

    // A valid ADTS frame (AAC-LC, 44.1 kHz, stereo, frame_length 7) must yield
    // its AudioSpecificConfig as soon as the frame is parsed.
    #[test]
    fn aac_parser_derives_audio_specific_config_from_first_frame() {
        let mut parser = parser_for_codec(Codec::Aac, None, false);
        assert_eq!(parser.codec_private(), None);
        let frames = parser.parse(&pes(
            Some(0),
            vec![0xFF, 0xF1, 0x50, 0x80, 0x00, 0xFF, 0xFC],
        ));
        assert_eq!(frames.len(), 1, "the frame is valid, not dropped");
        assert_eq!(parser.codec_private(), Some(vec![0x12, 0x10]));
    }

    #[test]
    fn audio_codecs_emit_keyframe_frames() {
        // PES = frame audio: every frame is independently decodable → keyframe.
        // Aac/Mp2/Mp3/Flac go through their dedicated gating parsers (which pass a
        // non-sync/too-short payload straight through); Opus uses PassthroughParser.
        for codec in [Codec::Aac, Codec::Mp2, Codec::Mp3, Codec::Flac, Codec::Opus] {
            let mut parser = parser_for_codec(codec, None, false);
            let frames = parser.parse(&pes(Some(0), vec![0x01, 0x02]));
            assert_eq!(frames.len(), 1, "{codec:?}");
            assert!(frames[0].keyframe, "{codec:?} should be keyframe");
        }
    }
}

#[cfg(test)]
mod provenance_guard {
    //! Every emitted frame must carry the source byte offset of the packet it came from. That
    //! invariant held only for video for as long as it existed: several audio/subtitle parsers
    //! built frames with `source: None`, so a multi-clip title fell back to inferring the clip
    //! from timestamps — which made branched titles run minutes long. This is a brace-balanced
    //! scan of the tree, as a test.

    /// Every parser module's source, checked for a `Frame` built without a
    /// source. Paired with `every_codec_module_is_covered` below, which fails
    /// if a module is added and not listed here.
    const PARSER_SOURCES: &[(&str, &str)] = &[
        ("mod.rs", include_str!("mod.rs")),
        ("ac3.rs", include_str!("ac3.rs")),
        ("adts.rs", include_str!("adts.rs")),
        ("audio_frames.rs", include_str!("audio_frames.rs")),
        ("dts.rs", include_str!("dts.rs")),
        ("dvdsub.rs", include_str!("dvdsub.rs")),
        ("flac.rs", include_str!("flac.rs")),
        ("h264.rs", include_str!("h264.rs")),
        ("hevc.rs", include_str!("hevc.rs")),
        ("lpcm.rs", include_str!("lpcm.rs")),
        ("mpeg2.rs", include_str!("mpeg2.rs")),
        ("mpegaudio.rs", include_str!("mpegaudio.rs")),
        ("pgs.rs", include_str!("pgs.rs")),
        ("truehd.rs", include_str!("truehd.rs")),
        ("vc1.rs", include_str!("vc1.rs")),
    ];

    /// Modules that declare no `Frame` and so need no entry above.
    const NON_PARSER_MODULES: &[&str] = &[
        "coding",
        "crc",
        "decodable",
        "dropgate",
        "mp2_channels",
        "pesbuf",
        "reorder",
        "startcode",
    ];

    // Strip line comments before scanning: a comment merely mentioning `source: None` isn't a
    // construction.
    fn code_only(src: &str) -> String {
        // The guard itself constructs no Frame; it only talks about them, in
        // prose and in string literals. Scanning its own body matches both.
        let src = match src.find("mod provenance_guard") {
            Some(i) => &src[..i],
            None => src,
        };
        src.lines()
            .map(|l| match l.find("//") {
                // Not inside a string literal: no quote before the slashes.
                Some(i) if !l[..i].contains('"') => &l[..i],
                _ => l,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn frame_literals(src: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let b = src.as_bytes();
        let mut i = 0;
        while let Some(p) = src[i..].find("Frame {") {
            let start = i + p;
            // Skip longer names ending in `Frame {` (e.g. `PesFrame {`).
            if start > 0 && (b[start - 1].is_ascii_alphanumeric() || b[start - 1] == b'_') {
                i = start + "Frame {".len();
                continue;
            }
            let open = start + src[start..].find('{').unwrap();
            let (mut depth, mut k) = (0usize, open);
            while k < b.len() {
                match b[k] {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                k += 1;
            }
            out.push(&src[start..(k + 1).min(src.len())]);
            i = k + 1;
        }
        out
    }

    #[test]
    fn no_parser_emits_a_frame_without_provenance() {
        let mut offenders = Vec::new();
        for (name, src) in PARSER_SOURCES {
            let src = &code_only(src);
            for blk in frame_literals(src) {
                // Only codec `Frame` literals are scanned (not `PesFrame`, whose
                // `source: None` fixtures are fine). Two loss spellings: explicit
                // `source: None`, or omitting it (Frame derives Default, so `..` yields None).
                let explicit_none = blk.contains("source: None");
                let no_source_field = !blk.contains("source:") && !blk.contains("source,");
                if explicit_none || no_source_field {
                    let line = src[..src.find(blk).unwrap_or(0)].lines().count() + 1;
                    let how = if explicit_none {
                        "source: None"
                    } else {
                        "no source field (Default fills in None)"
                    };
                    offenders.push(format!("{name}:{line} ({how})"));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "these parsers build a Frame with no source byte offset, so a \
             multi-clip title cannot place their tracks by provenance and \
             falls back to inferring the clip from timestamps: {offenders:?}"
        );
    }

    /// A parser added without an entry in `PARSER_SOURCES` would be silently
    /// unchecked, which is exactly how this class of gap persists.
    #[test]
    fn every_codec_module_is_covered() {
        let modules: Vec<&str> = include_str!("mod.rs")
            .lines()
            .filter_map(|l| {
                let l = l.trim();
                let rest = ["pub mod ", "pub(crate) mod ", "mod "]
                    .iter()
                    .find_map(|p| l.strip_prefix(p))?;
                rest.strip_suffix(';')
            })
            .collect();
        let listed: Vec<&str> = PARSER_SOURCES
            .iter()
            .map(|(n, _)| n.trim_end_matches(".rs"))
            .collect();
        let unchecked: Vec<&&str> = modules
            .iter()
            .filter(|m| !listed.contains(m) && !NON_PARSER_MODULES.contains(m))
            .collect();
        assert!(
            unchecked.is_empty(),
            "codec module(s) not covered by the provenance guard — add them to \
             PARSER_SOURCES (or to NON_PARSER_MODULES if they emit no Frame): \
             {unchecked:?}"
        );
    }
}
