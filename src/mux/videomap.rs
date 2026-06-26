//! `VideoMap` — freemkv's reusable, pure-data per-picture video index ("the
//! FVI object").
//!
//! A [`VideoMap`] is a header (per-title video facts + provenance root) plus an
//! ordered list of per-picture records. Each record carries the per-picture
//! coding truth ([`PictureInfo`], off `frame.coding`) and the byte-exact source
//! provenance ([`SourcePos`], off `frame.source`) that the highway already
//! stamps — this module never re-parses the elementary stream.
//!
//! It is a STANDALONE PRIMITIVE, deliberately decoupled from any one sink:
//! - The `fvi://` sink ([`crate::mux::fvi_sink`]) owns a `VideoMap`, appends
//!   each video [`PesFrame`], and serializes it.
//! - The same `VideoMap` can later be populated as a side-channel during ANY
//!   mux (e.g. `iso → mkv` while ALSO emitting a `.fvi` sidecar), and reused
//!   for seek-indexing, recovery loss-mapping, and diagnostics.
//!
//! `VideoMap` is PURE DATA — it knows no output format. The on-disk shape is the
//! freemkv FVI format, whose normative spec is `docs/FVI_FORMAT.md` (ships
//! publicly with libfreemkv); the `fvi://` sink does the serialization. A
//! different output format would be a DIFFERENT sink reusing this same model,
//! not a pluggable encoder here.

use crate::disc::{ColorSpace, DiscTitle, FrameRate, Stream as DiscStream, VideoStream};
use crate::mux::codec::PictureInfo;
use crate::mux::codec::coding::{CodingType, FieldOrder};
use crate::pes::{PesFrame, SourcePos};

// ── Format constants (cite docs/FVI_FORMAT.md) ───────────────────────────────

/// Value of the header `"format"` member — the FVI document signature
/// (`docs/FVI_FORMAT.md` §6). Identifies a stream as a freemkv video index.
pub const FVI_FORMAT: &str = "freemkv/video-index";

/// Value of the header `"fvi_version"` member — the FVI document format version
/// (`docs/FVI_FORMAT.md` §6, §11). This spec defines `1`.
pub const FVI_VERSION: u32 = 1;

/// Producing tool tag for the header `"generator"` member
/// (`docs/FVI_FORMAT.md` §6).
pub const FVI_GENERATOR: &str = concat!("freemkv/", env!("CARGO_PKG_VERSION"));

/// Header `"timescale"` for all `pts`/`dts` ticks (`docs/FVI_FORMAT.md` §10).
/// The highway carries presentation timestamps in nanoseconds, so the timescale
/// is `1_000_000_000` ticks per second.
pub const FVI_TIMESCALE: u64 = 1_000_000_000;

/// Bytes per `src.sector` unit (`docs/FVI_FORMAT.md` §6.2, §9). The highway's
/// [`SourcePos`] counts 2048-byte logical sectors.
pub const FVI_SECTOR_SIZE: u32 = 2048;

// ── Logical model (serialization-independent) ────────────────────────────────

/// Source-stream colour description (CICP code points), header-level
/// (`docs/FVI_FORMAT.md` §6.1 `colour`).
///
/// Each field is the ITU-T H.273 / ISO 23091-2 code point for the title's
/// primary video, derived from the disc's [`ColorSpace`]. `full_range` is the
/// video-range flag (`false` = limited / TV range, the disc norm).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Colour {
    pub primaries: u8,
    pub transfer: u8,
    pub matrix: u8,
    pub full_range: bool,
}

impl Colour {
    /// Map the title's [`ColorSpace`] to CICP code points. Unknown colorimetry
    /// maps to code point 2 ("unspecified"), the CICP convention.
    pub fn from_color_space(cs: ColorSpace) -> Self {
        // (primaries, transfer, matrix) per ITU-T H.273.
        let (p, t, m) = match cs {
            ColorSpace::Bt709 => (1, 1, 1),
            ColorSpace::Bt2020 => (9, 14, 9), // BT.2020 NCL
            ColorSpace::Bt470bg => (5, 5, 5),
            ColorSpace::Smpte170m => (6, 6, 6),
            ColorSpace::Unknown => (2, 2, 2), // unspecified
        };
        Self {
            primaries: p,
            transfer: t,
            matrix: m,
            // Disc video is limited-range; full-range is not signalled at this
            // layer, so report the disc norm.
            full_range: false,
        }
    }
}

/// Scan type for the header `stream.scan` member (`docs/FVI_FORMAT.md` §6.1).
/// `"mbaff"` is reachable only for codecs that signal it; MPEG-2 / disc video
/// resolves to `progressive` / `interlaced`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scan {
    Progressive,
    Interlaced,
}

impl Scan {
    pub fn as_str(self) -> &'static str {
        match self {
            Scan::Progressive => "progressive",
            Scan::Interlaced => "interlaced",
        }
    }
}

/// Source `medium` for the header `source.medium` member
/// (`docs/FVI_FORMAT.md` §6.2). Describes the physical/logical input the index
/// was built from. The bare resolver path has no input-URL context, so it
/// defaults to [`Medium::File`]; the CLI follow-up passes the real medium.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Medium {
    Disc,
    Iso,
    #[default]
    File,
    Stream,
}

impl Medium {
    pub fn as_str(self) -> &'static str {
        match self {
            Medium::Disc => "disc",
            Medium::Iso => "iso",
            Medium::File => "file",
            Medium::Stream => "stream",
        }
    }
}

/// Provenance root for the header (`docs/FVI_FORMAT.md` §6.2 `source`).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SourceInfo {
    /// Input medium.
    pub medium: Medium,
    /// Source path / label (may be empty).
    pub path: String,
    /// 0-based title / program number the index was built from.
    pub title: usize,
    /// Playlist / PGC identifier, if known (empty → omitted).
    pub playlist: String,
    /// Disc volume identifier, if read (empty → omitted).
    pub volume_id: String,
}

/// Per-title video facts for the header `stream` object
/// (`docs/FVI_FORMAT.md` §6.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamInfo {
    /// Registered codec id (Appendix B), e.g. `"mpeg2video"`, `"hevc"`.
    pub codec: &'static str,
    /// Coded luma dimensions in pixels.
    pub width: u32,
    pub height: u32,
    /// Display aspect ratio as `(num, den)`.
    pub dar: (u32, u32),
    /// Nominal frame rate as an exact rational `(num, den)`.
    pub frame_rate: (u32, u32),
    /// Scan type.
    pub scan: Scan,
    /// Source colour (CICP code points).
    pub colour: Colour,
}

/// The header row: per-title facts (`docs/FVI_FORMAT.md` §6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapHeader {
    /// The indexed elementary stream.
    pub stream: StreamInfo,
    /// Provenance root.
    pub source: SourceInfo,
    /// Total pictures if known at header time; `None` when streaming (omitted).
    pub picture_count: Option<u64>,
}

/// Map the disc's `Codec` to a registered FVI codec id
/// (`docs/FVI_FORMAT.md` Appendix B). The disc-info `Codec::id` strings differ
/// (`"mpeg2"`/`"mpeg1"`); FVI uses the bitstream names.
fn fvi_codec_id(codec: crate::disc::Codec) -> &'static str {
    use crate::disc::Codec;
    match codec {
        Codec::Mpeg2 => "mpeg2video",
        Codec::Mpeg1 => "mpeg1video",
        Codec::H264 => "h264",
        Codec::Hevc => "hevc",
        Codec::Vc1 => "vc1",
        // Not in the registry yet; carry the disc-info id so the field is still
        // a stable, machine-readable token (readers ignore unknown codecs).
        other => other.id(),
    }
}

impl MapHeader {
    /// Assemble the header from the title's primary video stream + the supplied
    /// provenance (`source`). Without a video stream there is nothing to index;
    /// this returns neutral stream defaults so the header still serializes (the
    /// record stream will be empty) — a malformed / audio-only title does not
    /// panic.
    pub fn from_title(title: &DiscTitle, source: SourceInfo) -> Self {
        let video: Option<&VideoStream> = title.streams.iter().find_map(|s| match s {
            DiscStream::Video(v) => Some(v),
            _ => None,
        });

        let stream = match video {
            Some(v) => {
                let (width, height) = v.resolution.pixels();
                StreamInfo {
                    codec: fvi_codec_id(v.codec),
                    width,
                    height,
                    dar: display_aspect_ratio(v, width, height),
                    frame_rate: v.frame_rate.as_fraction(),
                    scan: if v.resolution.is_interlaced() {
                        Scan::Interlaced
                    } else {
                        Scan::Progressive
                    },
                    colour: Colour::from_color_space(v.color_space),
                }
            }
            None => StreamInfo {
                codec: "unknown",
                width: 0,
                height: 0,
                dar: (0, 1),
                frame_rate: (0, 1),
                scan: Scan::Progressive,
                colour: Colour::from_color_space(ColorSpace::Unknown),
            },
        };

        Self {
            stream,
            source,
            picture_count: None,
        }
    }
}

/// Display aspect ratio as `(num, den)`. Anamorphic titles carry an explicit
/// `display_aspect`; square-pixel titles use the coded pixel dimensions.
fn display_aspect_ratio(v: &VideoStream, w: u32, h: u32) -> (u32, u32) {
    match v.display_aspect {
        Some((a, b)) if b != 0 => (a, b),
        _ if h != 0 => (w, h),
        _ => (0, 1),
    }
}

/// The title's nominal frame rate as a fraction — the single mapping site reused
/// by the header builder. (Retained as the canonical accessor.)
#[allow(dead_code)]
fn frame_rate_fraction(fr: FrameRate) -> (u32, u32) {
    fr.as_fraction()
}

/// One per-picture index record, distilled from a video [`PesFrame`]
/// (`docs/FVI_FORMAT.md` §7).
///
/// `coding` is the codec-agnostic per-picture truth ([`PictureInfo`], set by
/// EVERY video parser that decodes coding — MPEG-2 fully, H.264/HEVC/VC-1 as
/// coding-type-only); `source` is the byte-exact provenance. Both are optional:
/// an audio / synthetic / provenance-absent frame yields a record whose
/// coding-derived members are omitted and whose `src` is the spec-defined null.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PictureRecord {
    /// Coded-order index, 0-based, contiguous.
    pub n: u64,
    /// Codec-agnostic per-picture coding info, if present. Set by every video
    /// parser that decodes coding (MPEG-2 fully; H.264/HEVC/VC-1 carry
    /// coding-type only); `None` for audio/subtitle/synthetic frames.
    pub coding: Option<PictureInfo>,
    /// Random-access / keyframe flag carried for EVERY codec on the frame
    /// (`PesFrame::keyframe`): IDR/IRAP for HEVC/H.264, the I-picture flag for
    /// MPEG-2/VC-1. Drives the codec-agnostic `key` member.
    pub keyframe: bool,
    /// Presentation timestamp in `timescale` ticks (nanoseconds).
    pub pts_ns: Option<i64>,
    /// Byte-exact source provenance, if present.
    pub source: Option<SourcePos>,
}

/// Record `type` label (`docs/FVI_FORMAT.md` §7), codec-agnostic.
///
/// When `coding` is present (any video codec — every parser now fills it), the
/// agnostic coding type is reported from [`PictureInfo::coding_type`]:
/// `CodingType::{I,P,B}` → `"I"`/`"P"`/`"B"`. When `coding` is absent
/// (audio / synthetic frames), the type degrades to the I-vs-non-I distinction
/// the frame's keyframe flag still carries: `keyframe` → "I", otherwise "P".
pub fn type_label(coding: Option<PictureInfo>, keyframe: bool) -> &'static str {
    match coding {
        Some(c) => match c.coding_type() {
            CodingType::I => "I",
            CodingType::P => "P",
            CodingType::B => "B",
        },
        // No PictureInfo: the highway still gives a keyframe flag.
        None => {
            if keyframe {
                "I"
            } else {
                "P"
            }
        }
    }
}

/// Field-display-order label for the optional `field_order` member
/// (`docs/FVI_FORMAT.md` §7.1, Matroska element 0x9D), or `None` when the codec
/// did not measure it (signal absent / coding-type-only codec). `None` is an
/// HONEST absence — the writer OMITS the member rather than guessing a default.
pub fn field_order_label(coding: Option<PictureInfo>) -> Option<&'static str> {
    match coding?.field_order()? {
        FieldOrder::Tff => Some("tff"),
        FieldOrder::Bff => Some("bff"),
        FieldOrder::Progressive => Some("progressive"),
    }
}

/// Whether a picture is a random-access point for the `key` member
/// (`docs/FVI_FORMAT.md` §7), codec-agnostic.
///
/// For EVERY codec the frame's own `keyframe` flag IS the random-access signal:
/// IDR/IRAP for HEVC/H.264, the I-picture flag for MPEG-2/VC-1 — authored by
/// each codec's parser through the highway. The codec-agnostic [`PictureInfo`]
/// carries NO GOP-closure (no `closed_gop`/`gop_start`), so we DO NOT claim the
/// stricter open-GOP clean-RAP precision; `key` is the parser-flagged
/// decode-restart point (an intra picture). This is the honest limitation
/// documented in `docs/FVI_FORMAT.md`.
pub fn is_random_access(coding: Option<PictureInfo>, keyframe: bool) -> bool {
    // For a video frame `coding.keyframe()` == an intra (I) picture, which is
    // exactly the highway's `frame.keyframe`; use the frame flag uniformly.
    let _ = coding;
    keyframe
}

/// The reusable video index: a header plus an ordered list of per-picture
/// records. PURE DATA — serialization lives in the sink that consumes it.
#[derive(Clone, Debug)]
pub struct VideoMap {
    header: MapHeader,
    records: Vec<PictureRecord>,
}

impl VideoMap {
    /// Create an empty map with the header assembled from `title`'s primary
    /// video stream + the supplied provenance.
    pub fn new(title: &DiscTitle, source: SourceInfo) -> Self {
        Self {
            header: MapHeader::from_title(title, source),
            records: Vec::new(),
        }
    }

    /// The header row.
    pub fn header(&self) -> &MapHeader {
        &self.header
    }

    /// The per-picture records, in coded/arrival order.
    pub fn records(&self) -> &[PictureRecord] {
        &self.records
    }

    /// Append one video frame as the next picture record, pulling the coding
    /// truth from `frame.coding` and the provenance from `frame.source`. The
    /// record index `n` is the current record count (coded order). Returns the
    /// record just appended.
    pub fn append_frame(&mut self, frame: &PesFrame) -> &PictureRecord {
        let rec = PictureRecord {
            n: self.records.len() as u64,
            coding: frame.coding,
            keyframe: frame.keyframe,
            // pts is carried as ns; the highway always sets a presentation time
            // (0 at start), so emit it. A future source genuinely lacking a PTS
            // would set None and the writer omits the member.
            pts_ns: Some(frame.pts),
            source: frame.source,
        };
        self.records.push(rec);
        self.records.last().expect("just pushed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        Codec, ColorSpace, ContentFormat, FrameRate, HdrFormat, Resolution, VideoStream,
    };

    fn video_title(codec: Codec, res: Resolution, fr: FrameRate, cs: ColorSpace) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams = vec![DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec,
            resolution: res,
            frame_rate: fr,
            hdr: HdrFormat::Sdr,
            color_space: cs,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })];
        t.content_format = ContentFormat::BdTs;
        t
    }

    fn src(medium: Medium, path: &str, title: usize) -> SourceInfo {
        SourceInfo {
            medium,
            path: path.to_string(),
            title,
            ..Default::default()
        }
    }

    fn vframe(coding: Option<PictureInfo>, pts: i64, source: Option<SourcePos>) -> PesFrame {
        let keyframe = coding.map(|c| c.keyframe()).unwrap_or(false);
        PesFrame {
            track: 0,
            pts,
            keyframe,
            data: vec![0u8; 4],
            duration_ns: None,
            source,
            coding,
        }
    }

    use crate::mux::codec::coding::Mpeg2Coding;

    /// An interlaced (tff) MPEG-2 frame picture of the given coding type.
    fn mpeg2_pic(ct: CodingType) -> PictureInfo {
        PictureInfo::mpeg2(
            ct,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                progressive_frame: false,
                progressive_sequence: false,
                frame_picture: true,
            },
        )
    }

    /// A canonical I-picture fixture (interlaced frame).
    fn i_picture() -> PictureInfo {
        mpeg2_pic(CodingType::I)
    }

    #[test]
    fn colour_maps_cicp_code_points() {
        assert_eq!(
            Colour::from_color_space(ColorSpace::Bt709),
            Colour {
                primaries: 1,
                transfer: 1,
                matrix: 1,
                full_range: false
            }
        );
        assert_eq!(
            Colour::from_color_space(ColorSpace::Bt2020),
            Colour {
                primaries: 9,
                transfer: 14,
                matrix: 9,
                full_range: false
            }
        );
        assert_eq!(Colour::from_color_space(ColorSpace::Unknown).primaries, 2);
    }

    #[test]
    fn type_label_full_and_codec_agnostic_fallback() {
        // coding present: full I/P/B from the agnostic coding_type().
        let mk = |ct| Some(mpeg2_pic(ct));
        assert_eq!(type_label(mk(CodingType::I), false), "I");
        assert_eq!(type_label(mk(CodingType::P), false), "P");
        assert_eq!(type_label(mk(CodingType::B), false), "B");
        // coding-type-only codec still reports its type.
        assert_eq!(
            type_label(Some(PictureInfo::coding_type_only(CodingType::B)), false),
            "B"
        );
        // No coding (audio/synthetic): degrade to I-vs-non-I from keyframe.
        assert_eq!(type_label(None, true), "I");
        assert_eq!(type_label(None, false), "P");
    }

    #[test]
    fn field_order_label_omitted_when_unmeasured() {
        // MPEG-2 interlaced tff frame → "tff".
        assert_eq!(field_order_label(Some(i_picture())), Some("tff"));
        // Progressive frame → "progressive".
        let prog = PictureInfo::mpeg2(
            CodingType::I,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                progressive_frame: true,
                progressive_sequence: false,
                frame_picture: true,
            },
        );
        assert_eq!(field_order_label(Some(prog)), Some("progressive"));
        // Coding-type-only codec did not measure field order → None (omitted).
        assert_eq!(
            field_order_label(Some(PictureInfo::coding_type_only(CodingType::I))),
            None
        );
        // No coding at all → None.
        assert_eq!(field_order_label(None), None);
    }

    #[test]
    fn is_random_access_codec_agnostic() {
        // For EVERY codec the frame keyframe flag IS the random-access signal.
        assert!(is_random_access(Some(i_picture()), true));
        // An I-picture whose frame flag is clear is NOT promoted — `key` follows
        // the frame's keyframe flag, never fabricated GOP-closure.
        assert!(!is_random_access(Some(i_picture()), false));
        // P/B with the flag clear → never.
        assert!(!is_random_access(Some(mpeg2_pic(CodingType::P)), false));
        // No coding: the frame keyframe flag IS the RAP signal.
        assert!(is_random_access(None, true));
        assert!(!is_random_access(None, false));
    }

    #[test]
    fn fvi_codec_ids_use_bitstream_names() {
        assert_eq!(fvi_codec_id(Codec::Mpeg2), "mpeg2video");
        assert_eq!(fvi_codec_id(Codec::Mpeg1), "mpeg1video");
        assert_eq!(fvi_codec_id(Codec::H264), "h264");
        assert_eq!(fvi_codec_id(Codec::Hevc), "hevc");
        assert_eq!(fvi_codec_id(Codec::Vc1), "vc1");
    }

    #[test]
    fn header_from_title_pulls_video_facts() {
        let t = video_title(
            Codec::Mpeg2,
            Resolution::R576i,
            FrameRate::F25,
            ColorSpace::Bt470bg,
        );
        let h = MapHeader::from_title(&t, src(Medium::Iso, "iso://x.iso", 2));
        assert_eq!(h.stream.codec, "mpeg2video");
        assert_eq!((h.stream.width, h.stream.height), (720, 576));
        assert_eq!(h.stream.dar, (720, 576)); // square-pixel fallback
        assert_eq!(h.stream.frame_rate, (25, 1));
        assert_eq!(h.stream.scan, Scan::Interlaced);
        assert_eq!(h.stream.colour.matrix, 5);
        assert_eq!(h.source.path, "iso://x.iso");
        assert_eq!(h.source.title, 2);
        assert_eq!(h.source.medium, Medium::Iso);
    }

    #[test]
    fn header_audio_only_title_is_neutral_not_panic() {
        let t = DiscTitle::empty();
        let h = MapHeader::from_title(&t, SourceInfo::default());
        assert_eq!(h.stream.codec, "unknown");
        assert_eq!((h.stream.width, h.stream.height), (0, 0));
    }

    #[test]
    fn append_frame_numbers_records_in_order() {
        let t = video_title(
            Codec::Mpeg2,
            Resolution::R1080p,
            FrameRate::F23_976,
            ColorSpace::Bt709,
        );
        let mut map = VideoMap::new(&t, SourceInfo::default());
        map.append_frame(&vframe(
            Some(i_picture()),
            0,
            Some(SourcePos::at_byte(2048)),
        ));
        map.append_frame(&vframe(
            Some(mpeg2_pic(CodingType::B)),
            42,
            Some(SourcePos::at_byte(4096)),
        ));
        assert_eq!(map.records().len(), 2);
        assert_eq!(map.records()[0].n, 0);
        assert_eq!(map.records()[1].n, 1);
        assert_eq!(map.records()[0].source.unwrap().sector, 1);
        assert_eq!(map.records()[1].pts_ns, Some(42));
    }
}
