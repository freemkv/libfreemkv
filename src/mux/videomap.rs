//! `VideoMap` — freemkv's reusable, pure-data per-picture video index ("the
//! FVI object").
//!
//! A [`VideoMap`] is a header (per-title video facts + provenance root) plus an
//! ordered list of per-picture records: coding truth
//! ([`PictureInfo`]) plus source provenance ([`SourcePos`]).
//!
//! `VideoMap` is PURE DATA — it knows no output format; the `fvi://` sink does the
//! serialization to the on-disk FVI format.

use crate::disc::{ColorSpace, DiscTitle, Resolution, Stream as DiscStream, VideoStream};
use crate::mux::codec::PictureInfo;
use crate::mux::codec::coding::{CodingType, FieldOrder};
use crate::pes::{PesFrame, SourcePos};

// ── Format constants  ───────────────────────────────.

/// Value of the header `"format"` member — the FVI document signature. Identifies a stream as a
/// freemkv video index.
pub const FVI_FORMAT: &str = "freemkv/video-index";

/// Value of the header `"fvi_version"` member — the FVI document format version. This spec
/// defines `1`.
pub const FVI_VERSION: u32 = 1;

/// Producing tool tag for the header `"generator"` member.
pub const FVI_GENERATOR: &str = concat!("freemkv/", env!("FREEMKV_VERSION"), env!("GIT_SUFFIX"));

/// Header `"timescale"` for all `pts`/`dts` ticks. The highway carries presentation timestamps
/// in nanoseconds, so the timescale is `1_000_000_000` ticks per second.
pub const FVI_TIMESCALE: u64 = 1_000_000_000;

/// Bytes per `src.sector` unit. The highway's [`SourcePos`] counts 2048-byte logical sectors.
pub const FVI_SECTOR_SIZE: u32 = crate::consts::SECTOR_BYTES as u32;

// ── Logical model (serialization-independent) ────────────────────────────────

/// Source-stream colour description (CICP code points), header-level.
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
    /// Derive the FVI CICP code points from a full [`VideoStream`], using the
    /// SAME precedence as the MKV muxer ([`crate::mux::mkv::cicp_for_video`]):
    /// measured CICP (authoritative) → coarse `color_space` enum + HDR-driven
    /// transfer override. This is what the sidecar must use so it never reports
    /// an SDR transfer (14) for an HDR10 BT.2020 title while the MKV container
    /// reports PQ (16) — the two sinks of one title must agree.
    pub fn from_video(v: &VideoStream) -> Self {
        let (matrix, transfer, primaries, range) = crate::mux::mkv::cicp_for_video(v);
        Self {
            primaries,
            transfer,
            matrix,
            // Matroska/MeasuredCicp Range: 1 = limited (disc norm), 2 = full.
            full_range: range == 2,
        }
    }

    /// Map the title's [`ColorSpace`] alone to CICP code points (no HDR/measured
    /// context). Retained for the no-video header fallback and unit coverage;
    /// the title path uses [`Colour::from_video`]. Unknown colorimetry maps to
    /// code point 2 ("unspecified"), the CICP convention.
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

/// Scan type for the header `stream.scan` member. `"mbaff"` is reachable only for codecs that
/// signal it; MPEG-2 / disc video resolves to `progressive` / `interlaced`.
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

/// Source `medium` for the header `source.medium` member. Describes the physical/logical input
/// the index was built from — never the destination the index is written to. The driver derives
/// it from the `MuxSource` arm; [`Medium::File`] is the default only for a caller that declares
/// no provenance at all.
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

/// Provenance root for the header.
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

/// Per-title video facts for the header `stream` object.
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

/// The header row: per-title facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapHeader {
    /// The indexed elementary stream.
    pub stream: StreamInfo,
    /// Provenance root.
    pub source: SourceInfo,
    /// Total pictures if known at header time; `None` when streaming (omitted).
    pub picture_count: Option<u64>,
}

/// Map the disc's `Codec` to a registered FVI codec id. The disc-info `Codec::id` strings
/// differ (`"mpeg2"`/`"mpeg1"`); FVI uses the bitstream names.
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
                // Informational map; absent dimensions report as 0.
                let (width, height) = v.resolution.pixels().unwrap_or((0, 0));
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
                    colour: Colour::from_video(v),
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
/// `display_aspect`; without one, SD is never square-pixel so its aspect is
/// unknown `(0, 1)`, and HD uses the coded pixel dimensions.
fn display_aspect_ratio(v: &VideoStream, w: u32, h: u32) -> (u32, u32) {
    let sd = matches!(
        v.resolution,
        Resolution::R480i | Resolution::R480p | Resolution::R576i | Resolution::R576p
    );
    match v.display_aspect {
        Some((a, b)) if b != 0 => (a, b),
        _ if sd => (0, 1),
        _ if h != 0 => (w, h),
        _ => (0, 1),
    }
}

/// One per-picture index record, distilled from a video [`PesFrame`].
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

/// Record `type` label, codec-agnostic.
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

/// Field-display-order label for the optional `field_order` member, or `None` when the codec
/// did not measure it (signal absent / coding-type-only codec). `None` is an HONEST absence —
/// the writer OMITS the member rather than guessing a default.
pub fn field_order_label(coding: Option<PictureInfo>) -> Option<&'static str> {
    match coding?.field_order()? {
        FieldOrder::Tff => Some("tff"),
        FieldOrder::Bff => Some("bff"),
        FieldOrder::Progressive => Some("progressive"),
    }
}

/// Whether a picture is a random-access point for the `key` member, codec-agnostic.
///
/// For EVERY codec the frame's own `keyframe` flag IS the random-access signal: IDR/IRAP for
/// HEVC/H.264, the I-picture flag for MPEG-2/VC-1 — authored by each codec's parser through the
/// highway. `key` is the parser-flagged decode-restart point (an intra picture), not the
/// stricter open-GOP clean-RAP precision.
pub fn is_random_access(coding: Option<PictureInfo>, keyframe: bool) -> bool {
    // `PictureInfo` carries no GOP-closure (`closed_gop`/`gop_start`), so we
    // don't claim clean-RAP precision here; frame-flag alone is sufficient
    // since `coding.keyframe()` always agrees with `frame.keyframe`.
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
#[path = "videomap_tests.rs"]
mod tests;
