//! Matroska (MKV) muxer.
//!
//! Writes EBML header, Segment with tracks, clusters, and cues.
//! Designed for streaming writes: clusters are written as data arrives,
//! cues and seek head are finalized at the end.

use super::ebml;
// Clip-boundary timeline-continuity corrector — shared verbatim with the
// `demux://` sink.
use super::timeline::TimelineContinuity;
use crate::disc::{
    AudioStream, Chapter, Codec, CodecKind, ColorSpace, HdrFormat, SubtitleStream, VideoStream,
};
// Production code reaches resolutions only through `VideoStream::resolution`;
// the fixtures below name the variants directly.
#[cfg(test)]
use crate::disc::Resolution;
use std::io::{self, Seek, Write};

// ── CICP colour codes (ITU-T H.273) ──────────────────────────────────────────
// Matroska's Colour element (RFC 9559) carries Matrix/Transfer/Primaries verbatim as
// ITU-T H.273 CICP code-points; named constants keep each value traceable to the spec table.

/// ColourPrimaries = 1 (BT.709 / sRGB) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT709: u8 = 1;
/// ColourPrimaries = 5 (BT.470 System B/G — PAL/SECAM SD) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT470BG: u8 = 5;
/// ColourPrimaries = 6 (BT.601-525 / SMPTE 170M — NTSC SD) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT601_525: u8 = 6;
/// ColourPrimaries = 9 (BT.2020 / BT.2100) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT2020: u8 = 9;
/// ColourPrimaries = 2 ("unspecified" — colorimetry unknown) — ITU-T H.273
/// Table 2.
const CICP_PRIMARIES_UNSPECIFIED: u8 = 2;

/// TransferCharacteristics = 1 (BT.709) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT709: u8 = 1;
/// TransferCharacteristics = 5 (BT.470 System B/G) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT470BG: u8 = 5;
/// TransferCharacteristics = 6 (BT.601-525 / SMPTE 170M) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT601_525: u8 = 6;
/// TransferCharacteristics = 14 (BT.2020 10-bit, SDR) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT2020_10: u8 = 14;
/// TransferCharacteristics = 16 (SMPTE ST 2084 / PQ — HDR10/HDR10+/DV) — ITU-T
/// H.273 Table 3.
const CICP_TRANSFER_PQ: u8 = 16;
/// TransferCharacteristics = 18 (ARIB STD-B67 / Hybrid Log-Gamma) — ITU-T H.273
/// Table 3.
const CICP_TRANSFER_HLG: u8 = 18;
/// TransferCharacteristics = 2 ("unspecified" — transfer unknown) — ITU-T H.273
/// Table 3.
const CICP_TRANSFER_UNSPECIFIED: u8 = 2;

/// MatrixCoefficients = 1 (BT.709) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT709: u8 = 1;
/// MatrixCoefficients = 5 (BT.470 System B/G) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT470BG: u8 = 5;
/// MatrixCoefficients = 6 (BT.601-525 / SMPTE 170M) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT601_525: u8 = 6;
/// MatrixCoefficients = 9 (BT.2020 non-constant luminance) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT2020NC: u8 = 9;
/// MatrixCoefficients = 2 ("unspecified" — matrix unknown) — ITU-T H.273 Table 4.
const CICP_MATRIX_UNSPECIFIED: u8 = 2;

/// Matroska Colour/Range = 1 (broadcast / studio-swing "limited" range). RFC
/// 9559 Range element. (0 = unspecified, 2 = full.)
const COLOUR_RANGE_LIMITED: u8 = 1;

/// BlockAddIDType "dvcC" — the DOVIDecoderConfigurationRecord fourcc, big-endian
/// ASCII 'd''v''c''C'. Matroska BlockAdditionMapping/BlockAddIDType for a Dolby
/// Vision configuration record (RFC 9559 + Dolby Vision-in-Matroska spec).
const BLOCK_ADD_ID_TYPE_DVCC: u64 = 0x6476_6343;

// BlockAddIDType "mvcC" — MVCDecoderConfigurationRecord fourcc, big-endian ASCII 'm''v''c''C'.
const BLOCK_ADD_ID_TYPE_MVCC: u64 = 0x6D76_6343;

/// BlockAddIDValue for the MVC mapping — the value each per-frame `BlockAddID`
/// references (RFC 9559 requires ≥ 2; 1 is the default plain BlockAdditional).
const BLOCK_ADD_ID_VALUE_MVC: u64 = 2;

// Build an MVCDecoderConfigurationRecord (BlockAddIDExtraData for the mvcC mapping). `None` if
// either param set is absent, too short, or too long.
fn mvc_decoder_config_record(subset_sps: &[u8], pps: &[u8]) -> Option<Vec<u8>> {
    if subset_sps.len() < 4 || subset_sps.len() > 0xFFFF || pps.is_empty() || pps.len() > 0xFFFF {
        return None;
    }
    let mut record = vec![
        1,             // configurationVersion
        subset_sps[1], // AVCProfileIndication (whole MVC stream, from subset SPS)
        subset_sps[2], // profile_compatibility
        subset_sps[3], // AVCLevelIndication
        // complete_representation(1)=1 | explicit_au_track(1)=0 |
        // reserved '1111'(4) | lengthSizeMinusOne(2)=3(11) → 1_0_1111_11 = 0xBF.
        0xBF,
        // reserved '0'(1) | numOfSequenceParameterSets(7)=1 → 0_0000001 = 0x01.
        // NB: distinct from the AVC record's byte[5] (reserved(3)|numSPS(5)).
        0x01,
        (subset_sps.len() >> 8) as u8,
        subset_sps.len() as u8,
    ];
    record.extend_from_slice(subset_sps);
    record.push(1); // numOfPictureParameterSets
    record.push((pps.len() >> 8) as u8);
    record.push(pps.len() as u8);
    record.extend_from_slice(pps);
    Some(record)
}

// Build the CodecPrivate for an MVC base track: avcc + mvcC extension block (Matroska Codec
// Specifications §4.3.9).
fn mvc_codec_private(avcc: &[u8], record: &[u8]) -> Vec<u8> {
    let ext_size = (4 + record.len()) as u32; // "mvcC" (4) + record; = block size − 4
    let mut out = Vec::with_capacity(avcc.len() + 8 + record.len());
    out.extend_from_slice(avcc);
    out.extend_from_slice(&ext_size.to_be_bytes());
    out.extend_from_slice(b"mvcC");
    out.extend_from_slice(record);
    out
}

// Resolve a video stream's CICP colour code points (matrix, transfer, primaries, range) with a
// single precedence shared by every sink so they never drift.
pub(crate) fn cicp_for_video(v: &VideoStream) -> (u8, u8, u8, u8) {
    if let Some(c) = v.measured_cicp {
        return (c.matrix, c.transfer, c.primaries, c.range);
    }
    let (m, t, p, r) = match v.color_space {
        // SDR BT.2020 (UHD MPLS dynamic_range 0); the HDR override below selects PQ/HLG.
        ColorSpace::Bt2020 => (
            CICP_MATRIX_BT2020NC,
            CICP_TRANSFER_BT2020_10,
            CICP_PRIMARIES_BT2020,
            COLOUR_RANGE_LIMITED,
        ),
        ColorSpace::Bt709 => (
            CICP_MATRIX_BT709,
            CICP_TRANSFER_BT709,
            CICP_PRIMARIES_BT709,
            COLOUR_RANGE_LIMITED,
        ),
        // PAL SD: BT.470 System B/G matrix/transfer/primaries.
        ColorSpace::Bt470bg => (
            CICP_MATRIX_BT470BG,
            CICP_TRANSFER_BT470BG,
            CICP_PRIMARIES_BT470BG,
            COLOUR_RANGE_LIMITED,
        ),
        // NTSC SD: SMPTE 170M / BT.601-525.
        ColorSpace::Smpte170m => (
            CICP_MATRIX_BT601_525,
            CICP_TRANSFER_BT601_525,
            CICP_PRIMARIES_BT601_525,
            COLOUR_RANGE_LIMITED,
        ),
        // Unknown colorimetry → CICP "unspecified" (2) for matrix/transfer/primaries,
        // limited range (disc norm); MKV and the FVI sidecar both emit 2 so the two
        // sinks of one title agree (matches `Colour::from_color_space`'s Unknown mapping).
        ColorSpace::Unknown => (
            CICP_MATRIX_UNSPECIFIED,
            CICP_TRANSFER_UNSPECIFIED,
            CICP_PRIMARIES_UNSPECIFIED,
            COLOUR_RANGE_LIMITED,
        ),
    };
    // Override the transfer for HDR signalled by the HdrFormat (the coarse enum
    // can't express PQ/HLG). Only applies on the enum fallback; a measured CICP
    // already carries the real transfer and returned above.
    let t = match v.hdr {
        HdrFormat::Hdr10 | HdrFormat::Hdr10Plus | HdrFormat::DolbyVision => CICP_TRANSFER_PQ,
        HdrFormat::Hlg => CICP_TRANSFER_HLG,
        _ => t,
    };
    (m, t, p, r)
}

/// MKV track definition (built from disc stream metadata).
pub struct MkvTrack {
    pub track_type: u64, // 1=video, 2=audio, 17=subtitle
    pub codec_id: &'static str,
    pub language: String,
    pub name: String, // Track name / label (e.g. "English (Lossless)")
    pub codec_private: Option<Vec<u8>>,
    pub is_default: bool,
    pub is_forced: bool,
    // Video-specific
    pub pixel_width: u32,
    pub pixel_height: u32,
    pub default_duration_ns: u64, // nanoseconds per frame (0 = unknown)
    pub display_width: u32,       // display aspect ratio width (0 = same as pixel)
    pub display_height: u32,      // display aspect ratio height (0 = same as pixel)
    // HDR colour metadata
    pub colour_matrix: u8,    // MatrixCoefficients (9=bt2020nc)
    pub colour_transfer: u8,  // TransferCharacteristics (16=smpte2084/PQ)
    pub colour_primaries: u8, // Primaries (9=bt2020)
    pub colour_range: u8,     // Range (1=tv/limited)
    // Scan type. `interlaced` drives FlagInterlaced (0x9A): true → 1
    // (interlaced), false → 2 (progressive). `field_order` (0x9D) is only
    // meaningful when interlaced; `FIELD_ORDER_UNDETERMINED` omits it.
    pub interlaced: bool,
    pub field_order: u8,
    /// DefaultDecodedFieldDuration (ns per field) for interlaced video — half
    /// the frame `default_duration_ns`. 0 = omit (progressive / unknown).
    pub field_duration_ns: u64,
    // Audio-specific
    pub sample_rate: f64,
    pub channels: u8,
    pub bit_depth: u8,
    // Dolby Vision: the dvcC (DOVIDecoderConfigurationRecord) for the DV layer,
    // emitted as a BlockAdditionMapping. `None` for non-DV tracks.
    pub dv_config: Option<Vec<u8>>,
    /// HDR10 static metadata measured from the bitstream (HEVC SEI), or `None`
    /// when the stream carried no HDR10 SEI. Set from the first coded picture's
    /// `PictureInfo` at muxer activation (the same deferred path FieldOrder
    /// uses), NOT at construction — the SEI is only known once the elementary
    /// stream is parsed. When `Some`, the serializer emits MasteringMetadata +
    /// MaxCLL/MaxFALL inside Colour; when `None` they are omitted entirely.
    pub hdr10: Option<crate::mux::codec::Hdr10Metadata>,
    /// Blu-ray 3D (MVC): the dependent (right-eye) view's `(subset_sps, pps)`
    /// NAL units, from which the serializer builds the `mvcC`
    /// MVCDecoderConfigurationRecord (ISO/IEC 14496-15 §7.6.2) for the track's
    /// BlockAdditionMapping. `None` for non-3D tracks. Set at muxer activation
    /// from the dependent stream's parameter sets, never at construction. When
    /// `Some`, the per-frame dependent view rides as a `BlockAdditional`. (No
    /// `StereoMode`: RFC 9559 assigns none to MVC-in-BlockAdditional.)
    pub mvc_params: Option<(Vec<u8>, Vec<u8>)>,
}

/// Build a DOVIDecoderConfigurationRecord (dvcC) — 24 bytes — for the Matroska
/// BlockAdditionMapping, with RPU and EL present. `bl_present` is false for a track
/// carrying only EL + RPU (the disc's dual-PID Profile 7 secondary stream, muxed as its
/// own track beside the base layer), per Dolby's MPEG-2 TS spec §7.2.2.
pub fn dolby_vision_config(profile: u8, level: u8, bl_present: bool, bl_compat_id: u8) -> Vec<u8> {
    let mut v = vec![0u8; 24];
    v[0] = 1; // dv_version_major
    v[1] = 0; // dv_version_minor
    // profile(7) | level(6) | rpu_present(1) | el_present(1) | bl_present(1)
    v[2] = ((profile & 0x7F) << 1) | ((level >> 5) & 0x01);
    v[3] = ((level & 0x1F) << 3) | (1 << 2) | (1 << 1) | u8::from(bl_present);
    v[4] = (bl_compat_id & 0x0F) << 4;
    // v[5..24] reserved = 0
    v
}

// Dolby Vision level for a disc Profile 7 title: the base layer is always 3840x2160 (the
// DV-tagged stream is often the 1080p EL), so the level follows the frame rate alone
// (Dolby Vision Profiles and Levels: 6 = 2160p24, 7 = p30, 8 = p48, 9 = p60).
fn dv_level_2160p(num: u32, den: u32) -> u8 {
    let (num, den) = (u64::from(num), u64::from(den.max(1)));
    if num == 0 {
        return 6;
    }
    [24, 30, 48]
        .iter()
        .position(|&max| num <= max * den)
        .map_or(9, |i| 6 + i as u8)
}

/// SEI chromaticity unit (Rec. ITU-T H.265 D.3.28): `display_primaries_*` and
/// `white_point_*` are in increments of 0.00002. Matroska chromaticity elements
/// are floats in the [0, 1] range, so the conversion is `value * 0.00002`.
const HDR10_CHROMATICITY_UNIT: f64 = 0.00002;
/// SEI luminance unit (Rec. ITU-T H.265 D.3.28): `max/min_display_mastering_
/// luminance` are in increments of 0.0001 cd/m². Matroska Luminance elements are
/// floats in cd/m², so the conversion is `value * 0.0001`.
const HDR10_LUMINANCE_UNIT: f64 = 0.0001;

// Emit the HDR10 static-metadata children of Colour: MasteringMetadata + MaxCLL/MaxFALL. Called
// only when measured from bitstream SEI.
fn write_hdr10<W: Write + Seek>(w: &mut W, h: &crate::mux::codec::Hdr10Metadata) -> io::Result<()> {
    let chroma = |v: u16| -> f64 { v as f64 * HDR10_CHROMATICITY_UNIT };
    let lum = |v: u32| -> f64 { v as f64 * HDR10_LUMINANCE_UNIT };

    let mm_pos = ebml::start_master(w, ebml::MASTERING_METADATA)?;
    // SEI index 2 = Red, 0 = Green, 1 = Blue.
    ebml::write_float(
        w,
        ebml::PRIMARY_R_CHROMATICITY_X,
        chroma(h.display_primaries_x[2]),
    )?;
    ebml::write_float(
        w,
        ebml::PRIMARY_R_CHROMATICITY_Y,
        chroma(h.display_primaries_y[2]),
    )?;
    ebml::write_float(
        w,
        ebml::PRIMARY_G_CHROMATICITY_X,
        chroma(h.display_primaries_x[0]),
    )?;
    ebml::write_float(
        w,
        ebml::PRIMARY_G_CHROMATICITY_Y,
        chroma(h.display_primaries_y[0]),
    )?;
    ebml::write_float(
        w,
        ebml::PRIMARY_B_CHROMATICITY_X,
        chroma(h.display_primaries_x[1]),
    )?;
    ebml::write_float(
        w,
        ebml::PRIMARY_B_CHROMATICITY_Y,
        chroma(h.display_primaries_y[1]),
    )?;
    ebml::write_float(w, ebml::WHITE_POINT_CHROMATICITY_X, chroma(h.white_point_x))?;
    ebml::write_float(w, ebml::WHITE_POINT_CHROMATICITY_Y, chroma(h.white_point_y))?;
    ebml::write_float(
        w,
        ebml::LUMINANCE_MAX,
        lum(h.max_display_mastering_luminance),
    )?;
    ebml::write_float(
        w,
        ebml::LUMINANCE_MIN,
        lum(h.min_display_mastering_luminance),
    )?;
    ebml::end_master(w, mm_pos)?;

    // Optional, independent Colour children (RFC 9559): absent SEI → element omitted.
    if let Some(cll) = h.max_content_light_level {
        ebml::write_uint(w, ebml::MAX_CLL, cll as u64)?;
    }
    if let Some(fall) = h.max_pic_average_light_level {
        ebml::write_uint(w, ebml::MAX_FALL, fall as u64)?;
    }
    Ok(())
}

// RFC 9559 §12 Language is ISO 639-2; empty means "no language stated" → `und`. Single decision
// point so every scanner doesn't repeat the default.
fn language_or_und(lang: &str) -> String {
    if lang.is_empty() {
        "und".to_string()
    } else {
        lang.to_string()
    }
}

/// Matroska CodecID for `codec` carried as a `kind` track, or `None` when Matroska has no ID
/// for it here (text subtitles, whose payload is not in Matroska block form; `Unknown`; a codec
/// of another kind). Exhaustive, so a new `Codec` variant must be mapped or refused here.
fn codec_id(codec: Codec, kind: CodecKind) -> Option<&'static str> {
    // `A_DTS` is the sole registered ID for the whole DTS family; players tell core/HD-HRA/
    // HD-MA apart from the bitstream. Unregistered `A_DTS/MA`/`A_DTS/HR` break strict parsers.
    let id = match codec {
        Codec::H264 => ebml::CODEC_H264,
        Codec::Hevc => ebml::CODEC_HEVC,
        Codec::Vc1 => ebml::CODEC_VC1,
        Codec::Mpeg2 => ebml::CODEC_MPEG2,
        Codec::Mpeg1 => ebml::CODEC_MPEG1,
        Codec::Av1 => ebml::CODEC_AV1,
        Codec::Ac3 => ebml::CODEC_AC3,
        Codec::Ac3Plus => ebml::CODEC_EAC3,
        Codec::TrueHd => ebml::CODEC_TRUEHD,
        Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => ebml::CODEC_DTS,
        Codec::Lpcm => ebml::CODEC_PCM_BE,
        Codec::Aac => ebml::CODEC_AAC,
        Codec::Mp2 => ebml::CODEC_MP2,
        Codec::Mp3 => ebml::CODEC_MP3,
        Codec::Flac => ebml::CODEC_FLAC,
        Codec::Opus => ebml::CODEC_OPUS,
        Codec::Pgs => ebml::CODEC_PGS,
        Codec::DvdSub => ebml::CODEC_VOBSUB,
        Codec::Srt | Codec::Ssa | Codec::Unknown(_) => return None,
    };
    (codec.kind() == kind).then_some(id)
}

/// Whether `s` has a Matroska CodecID, i.e. whether `mkv://` carries it.
pub(crate) fn is_mappable(s: &crate::disc::Stream) -> bool {
    match s {
        crate::disc::Stream::Video(v) => codec_id(v.codec, CodecKind::Video).is_some(),
        crate::disc::Stream::Audio(a) => codec_id(a.codec, CodecKind::Audio).is_some(),
        crate::disc::Stream::Subtitle(t) => codec_id(t.codec, CodecKind::Subtitle).is_some(),
    }
}

// Test shorthands for [`MkvTrack::from_stream`] on a stream known to be mappable.
#[cfg(test)]
impl MkvTrack {
    pub fn video(v: &VideoStream) -> Self {
        Self::try_video(v).expect("mappable video codec")
    }
    pub fn audio(a: &AudioStream) -> Self {
        Self::try_audio(a).expect("mappable audio codec")
    }
    pub fn subtitle(s: &SubtitleStream) -> Self {
        Self::try_subtitle(s).expect("mappable subtitle codec")
    }
}

impl MkvTrack {
    /// Build the track for a title stream, or `None` when its codec has no Matroska CodecID
    /// (the stream is then left out rather than declared under another codec's ID).
    pub(crate) fn from_stream(s: &crate::disc::Stream) -> Option<Self> {
        match s {
            crate::disc::Stream::Video(v) => Self::try_video(v),
            crate::disc::Stream::Audio(a) => Self::try_audio(a),
            crate::disc::Stream::Subtitle(t) => Self::try_subtitle(t),
        }
    }

    // Video track: language `und`, colour from `cicp_for_video`, and a dvcC
    // BlockAdditionMapping when `hdr == DolbyVision`.
    fn try_video(v: &VideoStream) -> Option<Self> {
        let codec_id = codec_id(v.codec, CodecKind::Video)?;
        // Unknown resolution -> `pixels()` reports (0, 0) (no default is fabricated),
        // and the writer omits the optional PixelWidth/PixelHeight
        // on 0 per RFC 9559 5.1.4.1.28-29.
        let (w, h) = v.resolution.pixels().unwrap_or((0, 0));
        let (num, den) = v.frame_rate.as_fraction();
        let default_duration_ns = if num > 0 {
            (1_000_000_000u64 * den as u64) / num as u64
        } else {
            0
        };
        // CICP (matrix, transfer, primaries, range) — ITU-T H.273 code points.
        // Derived by the single shared resolver so every sink (this muxer, the
        // FVI sidecar in `videomap.rs`) reports identical code points.
        let (matrix, transfer, primaries, range) = cicp_for_video(v);
        // HD/UHD/BD is square-pixel so display == pixel. DVD (720x480/576) is
        // anamorphic — coded pixels aren't square, so derive width from the DAR
        // (e.g. 720x576 16:9 -> 1024x576) or players show 5:4/3:2 instead of 16:9.
        let (display_width, display_height) = match v.display_aspect {
            // u64: a caller-supplied ratio must not overflow; an unrepresentable width keeps (w, h).
            Some((an, ad)) if an > 0 && ad > 0 && h > 0 => {
                let dw = (u64::from(h) * u64::from(an) + u64::from(ad) / 2) / u64::from(ad);
                u32::try_from(dw).map_or((w, h), |dw| (dw, h))
            }
            _ => (w, h),
        };
        Some(Self {
            track_type: ebml::TRACK_TYPE_VIDEO,
            codec_id,
            language: "und".into(),
            name: v.label.clone(),
            codec_private: None,
            is_default: !v.secondary,
            is_forced: false,
            pixel_width: w,
            pixel_height: h,
            default_duration_ns,
            display_width,
            display_height,
            colour_matrix: matrix,
            colour_transfer: transfer,
            colour_primaries: primaries,
            colour_range: range,
            interlaced: v.resolution.is_interlaced(),
            // FieldOrder is a bitstream property the IFO/MPLS scan can't know, so it
            // defaults to UNDETERMINED here; `MkvStream` sets the MEASURED value from
            // the first coded picture. Still UNDETERMINED at mux time = logged, never faked.
            field_order: ebml::FIELD_ORDER_UNDETERMINED,
            // DefaultDecodedFieldDuration DELIBERATELY NOT emitted (0 suppresses it):
            // setting it made Windows Explorer report half fps / "Variable" instead of
            // "Constant" (a known-good rip omits it); the ES already signals interlace.
            field_duration_ns: 0,
            sample_rate: 0.0,
            channels: 0,
            bit_depth: 0,
            // The DV layer (hdr=DolbyVision) carries the dvcC. A secondary DV track is the
            // disc's EL + RPU stream beside the HDR10 base track: no BL, and the BL's
            // compatibility id (6, UHD BD HDR10 base) as Dolby TS §7.2.2 and mkvmerge give.
            dv_config: match (v.hdr, v.secondary) {
                (HdrFormat::DolbyVision, true) => {
                    Some(dolby_vision_config(7, dv_level_2160p(num, den), false, 6))
                }
                (HdrFormat::DolbyVision, false) => {
                    Some(dolby_vision_config(7, dv_level_2160p(num, den), true, 0))
                }
                _ => None,
            },
            // HDR10 static metadata is measured from the HEVC SEI at mux time, not known
            // at construction; the mux stream sets it before the header is written
            // (same deferred path FieldOrder uses). `None` here -> omitted unless seen.
            hdr10: None,
            mvc_params: None,
        })
    }

    // Audio track; every DTS family member maps to the single registered `A_DTS`.
    fn try_audio(a: &AudioStream) -> Option<Self> {
        let codec_id = codec_id(a.codec, CodecKind::Audio)?;
        // Unknown sample rate/channels -> accessors return 0, so the serializer omits
        // SamplingFrequency/Channels rather than writing a fabricated 48kHz/6ch value.
        let sr = a.sample_rate.hz();
        let ch = a.channels.count();

        let name = a.label.clone();

        Some(Self {
            track_type: ebml::TRACK_TYPE_AUDIO,
            codec_id,
            language: language_or_und(&a.language),
            name,
            codec_private: None,
            is_default: !a.secondary,
            is_forced: false,
            pixel_width: 0,
            pixel_height: 0,
            default_duration_ns: 0,
            display_width: 0,
            display_height: 0,
            colour_matrix: 0,
            colour_transfer: 0,
            colour_primaries: 0,
            colour_range: 0,
            interlaced: false,
            field_order: ebml::FIELD_ORDER_UNDETERMINED,
            field_duration_ns: 0,
            sample_rate: sr,
            channels: ch,
            // Matroska requires BitDepth for `A_PCM/INT/*`. 24 unless the parser's
            // codec_private says the source was 16-bit (set by `MkvStream`).
            bit_depth: if a.codec == Codec::Lpcm { 24 } else { 0 },
            dv_config: None,
            hdr10: None,
            mvc_params: None,
        })
    }

    // Subtitle track (PGS or VobSub); `codec_data` (the VobSub `.idx` palette header) becomes
    // the CodecPrivate and the forced flag is propagated.
    fn try_subtitle(s: &SubtitleStream) -> Option<Self> {
        let codec_id = codec_id(s.codec, CodecKind::Subtitle)?;
        Some(Self {
            track_type: ebml::TRACK_TYPE_SUBTITLE,
            codec_id,
            language: language_or_und(&s.language),
            name: String::new(),
            codec_private: s.codec_data.clone(),
            is_default: false,
            is_forced: s.forced,
            pixel_width: 0,
            pixel_height: 0,
            default_duration_ns: 0,
            display_width: 0,
            display_height: 0,
            colour_matrix: 0,
            colour_transfer: 0,
            colour_primaries: 0,
            colour_range: 0,
            interlaced: false,
            field_order: ebml::FIELD_ORDER_UNDETERMINED,
            field_duration_ns: 0,
            sample_rate: 0.0,
            channels: 0,
            bit_depth: 0,
            dv_config: None,
            hdr10: None,
            mvc_params: None,
        })
    }
}

/// Cue point for seeking.
struct CuePoint {
    timestamp_ticks: i64, // TimestampScale ticks
    track: usize,
    cluster_pos: u64, // relative to Segment start
}

/// SeekHead entry that needs its 8-byte SeekPosition back-patched after Cues are written.
struct SeekPositionFixup {
    target_id: u32,
    value_offset: u64, // absolute file offset of the 8-byte SeekPosition value
}

/// MKV muxer. Call write_frame() for each frame, then finish() at the end.
pub struct MkvMuxer<W: Write + Seek> {
    writer: W,
    segment_start: u64,
    cluster_open: bool,
    cluster_pos: u64,
    cluster_size_pos: u64,
    cluster_ts_ticks: i64,
    /// Reusable scratch buffer for assembling ONE BlockGroup before it is written with a single
    /// `write_all`; building it in memory means its size is known before it reaches the file,
    /// so no `start_master`/ `end_master` back-patch seek is needed. Kept on the muxer so the
    /// allocation is made once, not per frame.
    block_group_buf: Vec<u8>,
    base_pts_ticks: Option<i64>,
    /// Last block timecode (TimestampScale ticks, relative to base_pts) written
    /// PER TRACK, to enforce strictly-monotonic per-track timestamps —
    /// players and decoders reject non-monotonic DTS, and some audio PES PTS land on
    /// the same tick (or tick back one from rounding).
    last_pts_ticks: std::collections::HashMap<usize, i64>,
    /// Per-track-index flag: true if the track is video. The strictly-monotonic
    /// block-timestamp nudge must be skipped for EVERY video track, not just
    /// track 0 — a title can carry a second video track (e.g. a Dolby Vision
    /// enhancement layer at index 1) whose B-frame PTS is just as legitimately
    /// non-monotonic. Keying the exemption on track type (not index) keeps that
    /// EL's true PTS instead of clobbering it to prev+1ms.
    track_is_video: Vec<bool>,
    /// Per-track-index flag: true if the track is a subtitle track
    /// (`track_type == 17`). A subtitle block must NEVER be written as a bare
    /// `SimpleBlock`: subtitle tracks carry no `DefaultDuration`, so without a
    /// `BlockDuration` a demuxer has nothing to bound the cue and ffmpeg reports
    /// "Timestamps are unset in a packet for stream N" (issue #52). Keyed on
    /// track type (mirroring `track_is_video`) so the invariant holds for every
    /// subtitle track — including a sparse forced-narrative second track.
    track_is_subtitle: Vec<bool>,
    /// Per-track-index flag: true for an `S_VOBSUB` track, whose SPU carries its
    /// own display duration.
    track_is_vobsub: Vec<bool>,
    /// Index of the PRIMARY video track — the first track whose type is video.
    /// This (not the literal index 0) is the clip-boundary epoch driver: the
    /// M2TS/PMT path orders streams by PMT declaration order and may list an
    /// audio ES before the video ES, so `streams[0]` is not guaranteed to be
    /// the primary video. `None` when the title has no video track (no track
    /// drives epochs).
    primary_video_track: Option<usize>,
    /// Per-track: whether the track declared an `mvcC` BlockAdditionMapping (its
    /// `mvc_params` was set at activation). A `BlockAdditional` with BlockAddID=2
    /// is only conforming when the track carries the matching mapping, so
    /// `write_frame`'s additional is dropped for a track without it (e.g. the
    /// dependent view's parameter sets were never captured before activation).
    track_has_mvc_mapping: Vec<bool>,
    /// Cross-clip timeline-continuity corrector (clip-boundary PTS rebasing).
    continuity: TimelineContinuity,
    cues: Vec<CuePoint>,
    frame_count: u64,
    /// Frames handed to `write_frame` that were dropped because no cluster was open yet (opens
    /// only on a keyframe from the primary video track). The ALL-dropped case is surfaced by
    /// `finish()` via `frame_count == 0`, not this counter; a PARTIAL drop is normal and is
    /// only logged, not an error — `finish()` is this field's only reader, so keep that log.
    dropped_pre_cluster: u64,
    /// Ticks the timeline origin sits before the first video keyframe.
    origin_lead_ticks: i64,
    /// Per VobSub track: file offset of an open-ended block's 3-byte
    /// BlockDuration value, and that block's timestamp in ticks.
    vobsub_open_end: std::collections::HashMap<usize, (u64, i64)>,
    seek_fixups: Vec<SeekPositionFixup>,
    /// Absolute file offset of the CUES SeekHead entry (a fixed 21-byte Seek
    /// element). When `finish()` writes no Cues element (zero cue points), this
    /// entry is overwritten with a Void so the SeekHead carries no pointer to a
    /// non-existent / wrong element.
    cues_seek_entry_pos: Option<u64>,
    info_offset: u64,
    tracks_offset: u64,
    chapters_offset: Option<u64>,
    /// Total payload bytes muxed PER TRACK (index = track_idx). Used to emit a
    /// per-track `BPS` statistics tag (bytes*8/duration) at finalize so Windows
    /// shows a bitrate for every track, not just CBR audio.
    track_bytes: Vec<u64>,
    /// Track UIDs in track order (parallels `track_bytes`), for the BPS Targets.
    track_uids: Vec<u64>,
    /// Segment duration in seconds (from `Info`), for the BPS denominator.
    duration_secs: f64,
    /// Byte offset of the DURATION element's 8-byte payload when it was written
    /// as a patch-later placeholder — the source supplied no duration (e.g.
    /// HD-DVD, whose `.MAP` timemaps are not parsed). `None` when a real duration
    /// was written up-front. Back-patched at `finish()` from the muxed timeline.
    duration_patch_pos: Option<u64>,
    /// Highest block timestamp (TimestampScale ticks) written across all tracks —
    /// the muxed runtime, used to back-patch the DURATION placeholder.
    max_block_ticks: i64,
    /// Timestamp (TimestampScale ticks) of the last video keyframe written on the
    /// video track. A non-keyframe frame written as a BlockGroup needs a
    /// `ReferenceBlock` so players don't mistake it for a keyframe, and it must
    /// reference a keyframe on its OWN track.
    ///
    /// Per track, not global: the ReferenceBlock is emitted for ANY video track,
    /// so a single global value made a secondary video track's non-keyframe point
    /// at a keyframe on a different track (or at 0, a self-reference, when the
    /// primary had not produced one yet). Indexed by `track_idx`.
    last_video_keyframe_ticks: Vec<Option<i64>>,
    /// Per-AC-3-audio-track channel-correction state. The DVD IFO audio nibble
    /// is unreliable, so the channel count written in the track header is
    /// corrected from the first frame's bitstream (AC-3 `acmod`, MPEG audio Layer II header
    /// and `mc_header`). Each entry records the file offset of the 1-byte Channels value
    /// (to patch in place) and the IFO-claimed count (to warn on disagreement);
    /// `corrected` flips once patched so we only act on the first frame.
    channel_fixups: std::collections::HashMap<usize, ChannelFixup>,
    /// Deferred PGS forced-subtitle detection. A PGS subtitle track reserves a
    /// 1-byte `FlagForced` value up-front; as its display sets are written, the
    /// track is judged forced iff it displayed at least one subtitle and EVERY
    /// display set carried the HDMV `forced_on_flag` (a dedicated forced/narrative
    /// track). At `finish()` the reserved byte is promoted to 1 for such tracks —
    /// so forced subs are flagged even on discs without vendor label metadata.
    /// A vendor forced flag is cleared only under the cross-track guard in
    /// `finish()` (see `super::codec::pgs::demotable`).
    pgs_forced_fixups: std::collections::HashMap<usize, PgsForcedFixup>,
    /// Deferred `FlagInterlaced` correction. A video track's scan type is written
    /// up-front from the FIRST coded picture, but MPEG-2 `progressive_frame` is a
    /// PER-picture flag (§6.3.10): the first picture (a progressive leader/logo on
    /// an interlaced feature, or an interlaced leader on a progressive one) can
    /// misrepresent the whole title. Each entry records the `FlagInterlaced` byte
    /// offset and the `FieldOrder` element's span, then counts progressive vs
    /// interlaced pictures across the WHOLE track; at `finish()` the byte is
    /// rewritten to the MAJORITY scan (and a now-wrong FieldOrder Void'd).
    flag_interlaced_fixups: std::collections::HashMap<usize, FlagInterlacedFixup>,
    /// `--log-level 3` opening-frame capture: the first ~100 coded frames per
    /// track are written (raw) to a `<output>.opening.bin` side file with a
    /// per-frame summary logged, so an opening-GOP / menu / mid-GOP-open issue is
    /// diagnosable from a future log without the disc. `None` on normal runs
    /// (diag off) — the muxer pays nothing.
    opening_capture: Option<crate::diag::OpeningCapture>,
    /// Per track: file offset of a Void reserved for a CodecPrivate that was not
    /// known at header time (AAC), filled by [`Self::set_codec_private`].
    codec_private_reserves: std::collections::HashMap<usize, u64>,
    /// Per MPEG-2 video track: the DefaultDuration measured from the kept frames.
    frame_rate_fixups: std::collections::HashMap<usize, FrameRateFixup>,
}

// Deferred DefaultDuration correction for an MPEG-2 video track: the header holds the
// sequence rate (29.97 for NTSC), but soft-telecined film is 23.976 coded frames a second.
// `finish()` rewrites it when the kept frames' PTS span measures another standard rate.
struct FrameRateFixup {
    /// Absolute file offset of the DefaultDuration value, and its byte length.
    value_offset: u64,
    value_len: usize,
    /// The value written up-front, in ns.
    initial_ns: u64,
    frames: u64,
    min_pts_ns: i64,
    max_pts_ns: i64,
}

// Fewest kept frames whose PTS span is taken as a measured frame period.
const FRAME_RATE_MIN_FRAMES: u64 = 120;

// The standard frame period (ns) within 0.5% of `measured`, if any.
fn standard_frame_period_ns(measured: f64) -> Option<u64> {
    use crate::disc::FrameRate;
    [
        FrameRate::F23_976,
        FrameRate::F24,
        FrameRate::F25,
        FrameRate::F29_97,
        FrameRate::F30,
        FrameRate::F50,
        FrameRate::F59_94,
        FrameRate::F60,
    ]
    .iter()
    .map(|r| {
        let (num, den) = r.as_fraction();
        (1_000_000_000u64 * den as u64) / num as u64
    })
    .find(|&ns| (measured - ns as f64).abs() <= ns as f64 * 0.005)
}

// Bytes of the shortest big-endian encoding `ebml::write_uint` gives `v`.
fn uint_len(v: u64) -> usize {
    (8 - (v.leading_zeros() / 8) as usize).max(1)
}

// Bytes reserved for a late AAC CodecPrivate: ID (2) + size (1-2) + an ASC of
// up to 12 bytes, all inside one Void so the Tracks size never changes.
const CODEC_PRIVATE_RESERVE: usize = 16;

/// Deferred Channels correction: the track header's `Channels` byte is written up-front from
/// the (unreliable) IFO count; on the first frame the bitstream can describe, it is rewritten
/// from what the stored stream actually carries.
struct ChannelFixup {
    /// Absolute file offset of the 1-byte Channels value in the Tracks element.
    value_offset: u64,
    /// Channel count the IFO claimed (already written at `value_offset`).
    claimed: u8,
    /// True once the first frame has been parsed and the value finalised.
    corrected: bool,
    /// Which bitstream describes the count.
    source: ChannelSource,
}

enum ChannelSource {
    /// AC-3 BSI `acmod` + `lfeon`.
    Ac3,
    /// MPEG audio header `mode`, plus a CRC-verified 13818-3 `mc_header` (§2.5.3.1).
    // Not gated on IFO coding mode 3: mode 2 is "MPEG-1 or MPEG-2 without extension bit stream"
    // (EP0867877A2), i.e. ext '0' multichannel; §2.5.3.1's mandatory CRC-check is the gate.
    MpegLayerII(super::codec::mp2_channels::ChannelTracker),
}

/// Deferred PGS forced-subtitle detection state for one PGS subtitle track.
struct PgsForcedFixup {
    /// Absolute file offset of the 1-byte `FlagForced` value in the Tracks element.
    value_offset: u64,
    /// The value written up-front — the scan/vendor-label flag. Kept so
    /// `finish()` can tell a promotion from a demotion and rewrite only the byte
    /// that actually changes.
    initial_forced: bool,
    /// Shared forced-narrative classifier fed the track's display sets. The same
    /// type drives the `info`-time forced probe, so both classify identically.
    tracker: super::codec::pgs::ForcedTracker,
}

// Bytes of a FieldOrder element with a 1-byte value (ID 0x9D, size, value).
const FIELD_ORDER_LEN: usize = 3;

// Deferred FlagInterlaced correction state for one video track: every
// picture's `progressive()` verdict is tallied and the header byte is
// corrected to the majority at `finish()`.
struct FlagInterlacedFixup {
    /// Absolute file offset of the 1-byte `FlagInterlaced` value in the Tracks element.
    value_offset: u64,
    /// `(offset, len)` of the `FieldOrder` element, or of a same-size Void reserved for it
    /// when no order was known up-front. Void'd on a demotion; filled on a promotion.
    field_order_span: Option<(u64, u64)>,
    /// A FieldOrder (not the reserve) was written up-front.
    field_order_written: bool,
    /// The value written up-front (`true` = interlaced), so `finish()` rewrites
    /// only when the majority actually disagrees.
    initial_interlaced: bool,
    /// Pictures measured progressive / interlaced across the whole track. Pictures
    /// with no scan signal (e.g. H.264/HEVC `CodingTypeOnly`) count toward neither.
    progressive_pics: u64,
    interlaced_pics: u64,
    /// Interlaced pictures by measured field order.
    tff_pics: u64,
    bff_pics: u64,
}

/// One `finish()`-time FlagInterlaced correction, resolved from the whole-stream
/// scan majority.
struct InterlacedRewrite {
    /// File offset of the 1-byte FlagInterlaced value to overwrite.
    value_offset: u64,
    /// The corrected scan (`true` = interlaced) the majority resolved to.
    final_interlaced: bool,
    /// `(offset, len)` of the up-front FieldOrder element, Void'd on a demotion.
    field_order_span: Option<(u64, u64)>,
    /// Majority field order to write into the reserved span (interlaced, none up-front).
    field_order: Option<u8>,
    /// FlagInterlaced differs from the value written up-front.
    changed: bool,
}

// TimestampScale: nanoseconds per Matroska timestamp tick. 0.1 ms (100_000 ns) avoids
// collisions the classic 1 ms scale causes for B-frame-reordered video and TrueHD's 0.833 ms
// AUs.
const TIMESTAMP_SCALE_NS: i64 = 100_000;

// BlockDuration for a durationless PGS block (issue #52): a PGS cue ends at the
// next display set, so a long bound only caps a cue that is already replaced.
const PGS_FALLBACK_BLOCK_DURATION_NS: i64 = 30_000_000_000;

// A VobSub SPU with no stop command ends at the next SPU on its track (patched
// in place), or after this cap. 100_000 ticks keeps the field 3 bytes wide.
const VOBSUB_OPEN_END_TICKS: u64 = 100_000;

// VobSub SP_DCSQ delays count 1024 ticks of the 90 kHz clock.
const SPU_DELAY_NS: u64 = 1024 * 1_000_000_000 / 90_000;

// Display time of a VobSub SPU: the delay of its first STP_DSP (0x02) command,
// walking the SP_DCSQ chain. `None` when the SPU has no stop or is malformed.
fn vobsub_display_ns(spu: &[u8]) -> Option<u64> {
    let be16 = |at: usize| -> Option<usize> {
        Some(u16::from_be_bytes([*spu.get(at)?, *spu.get(at + 1)?]) as usize)
    };
    let mut dcsq = be16(2)?;
    // Each DCSQ is at least 5 bytes, so a well-formed chain has bounded length.
    for _ in 0..spu.len() / 5 + 1 {
        let delay = be16(dcsq)? as u64;
        let next = be16(dcsq + 2)?;
        let mut at = dcsq + 4;
        loop {
            let args = match *spu.get(at)? {
                0x02 => return Some(delay * SPU_DELAY_NS),
                0x00 | 0x01 => 0,
                0x03 | 0x04 => 2,
                0x05 => 6,
                0x06 => 4,
                0x07 => be16(at + 1)?,
                _ => break,
            };
            at += 1 + args;
        }
        if next == dcsq {
            return None;
        }
        dcsq = next;
    }
    None
}

// Nominal new-cluster interval (2 s) in TimestampScale ticks; a keyframe only opens a new
// cluster once this much has elapsed.
const CLUSTER_DURATION_TICKS: i64 = 2_000 * 1_000_000 / TIMESTAMP_SCALE_NS;

// Maximum block-relative timestamp in the signed 16-bit SimpleBlock/Block field; a frame
// outside this forces a new cluster so `as i16` never wraps.
const MAX_BLOCK_REL: i64 = i16::MAX as i64;

/// Nanoseconds to timestamp ticks, at least 1 so a duration never truncates to 0.
fn ns_to_ticks(ns: u64) -> u64 {
    (ns as i64 / TIMESTAMP_SCALE_NS).max(1) as u64
}
/// Minimum block-relative timestamp expressible in the signed 16-bit field.
const MIN_BLOCK_REL: i64 = i16::MIN as i64;

// Force a per-track block timestamp to be strictly later than `prev` (the track's last
// timestamp, `None` for the first frame); never moves it earlier.
fn monotonic_ts(prev: Option<i64>, pts_ticks: i64) -> i64 {
    match prev {
        Some(p) => pts_ticks.max(p.saturating_add(1)),
        None => pts_ticks,
    }
}

// Per-track block timestamp: monotonic nudge applies to AUDIO/SUBTITLE only; video is UNCHANGED
// (B-frame PTS is legitimately non-monotonic), keyed on `is_video` not track index.
fn block_ts(is_video: bool, prev: Option<i64>, pts_ticks: i64) -> i64 {
    if is_video {
        pts_ticks
    } else {
        monotonic_ts(prev, pts_ticks)
    }
}

// Encode a Matroska track number as an EBML VINT into a stack buffer (no heap alloc; hot path).
// 1/2/3-byte widths per VINT marker bit; handled in RELEASE (not just debug_assert).
fn track_vint(track_num: usize) -> ([u8; 3], usize) {
    if track_num < 0x80 {
        ([(track_num as u8) | 0x80, 0, 0], 1)
    } else if track_num < 0x4000 {
        ([0x40 | ((track_num >> 8) as u8), track_num as u8, 0], 2)
    } else {
        debug_assert!(
            track_num < 0x20_0000,
            "track number {track_num} exceeds the 21-bit 3-byte EBML VINT range"
        );
        (
            [
                0x20 | ((track_num >> 16) as u8),
                (track_num >> 8) as u8,
                track_num as u8,
            ],
            3,
        )
    }
}

impl<W: Write + Seek> MkvMuxer<W> {
    /// Create a new MKV muxer: writes EBML header, Segment start, Info, Tracks, Chapters.
    #[cfg(test)]
    pub fn new(
        writer: W,
        tracks: &[MkvTrack],
        title: Option<&str>,
        duration_secs: f64,
        chapters: &[Chapter],
    ) -> io::Result<Self> {
        Self::new_with_timing(writer, tracks, &[], title, duration_secs, chapters)
    }

    pub(crate) fn new_with_timing(
        mut writer: W,
        tracks: &[MkvTrack],
        timings: &[crate::pes::TrackTiming],
        title: Option<&str>,
        duration_secs: f64,
        chapters: &[Chapter],
    ) -> io::Result<Self> {
        // EBML Header
        let ebml_pos = ebml::start_master(&mut writer, ebml::EBML)?;
        ebml::write_uint(&mut writer, ebml::EBML_VERSION, 1)?;
        ebml::write_uint(&mut writer, ebml::EBML_READ_VERSION, 1)?;
        ebml::write_uint(&mut writer, ebml::EBML_MAX_ID_LENGTH, 4)?;
        ebml::write_uint(&mut writer, ebml::EBML_MAX_SIZE_LENGTH, 8)?;
        ebml::write_string(&mut writer, ebml::EBML_DOC_TYPE, "matroska")?;
        ebml::write_uint(&mut writer, ebml::EBML_DOC_TYPE_VERSION, 4)?;
        ebml::write_uint(&mut writer, ebml::EBML_DOC_TYPE_READ_VERSION, 2)?;
        ebml::end_master(&mut writer, ebml_pos)?;

        // Segment (unknown size — we'll write cues at the end)
        ebml::write_id(&mut writer, ebml::SEGMENT)?;
        ebml::write_unknown_size(&mut writer)?;
        let segment_start = writer.stream_position()?;

        // SeekHead with fixed-width SeekPosition placeholders. Order: Info, Tracks, [Chapters], Cues.
        let mut seek_fixups: Vec<SeekPositionFixup> = Vec::new();
        let seekhead_pos = ebml::start_master(&mut writer, ebml::SEEK_HEAD)?;
        let mut targets: Vec<u32> = vec![ebml::INFO, ebml::TRACKS];
        if !chapters.is_empty() {
            targets.push(ebml::CHAPTERS);
        }
        targets.push(ebml::CUES);
        let seek_id_be = (ebml::SEEK as u16).to_be_bytes();
        let seek_inner_id_be = (ebml::SEEK_ID as u16).to_be_bytes();
        let seek_pos_id_be = (ebml::SEEK_POSITION as u16).to_be_bytes();
        // Offset of the CUES Seek entry, so that if no Cues element is written (zero
        // cue points) it can be overwritten with a Void at finish() instead of a
        // SeekHead pointer resolving to whatever element happens to land there.
        let mut cues_seek_entry_pos: Option<u64> = None;
        for target_id in &targets {
            let entry_pos = writer.stream_position()?;
            if *target_id == ebml::CUES {
                cues_seek_entry_pos = Some(entry_pos);
            }
            writer.write_all(&[seek_id_be[0], seek_id_be[1], 0x92])?;
            writer.write_all(&[seek_inner_id_be[0], seek_inner_id_be[1], 0x84])?;
            writer.write_all(&target_id.to_be_bytes())?;
            writer.write_all(&[seek_pos_id_be[0], seek_pos_id_be[1], 0x88])?;
            let value_offset = writer.stream_position()?;
            writer.write_all(&[0u8; 8])?;
            seek_fixups.push(SeekPositionFixup {
                target_id: *target_id,
                value_offset,
            });
        }
        ebml::end_master(&mut writer, seekhead_pos)?;

        // Info
        let info_start = writer.stream_position()?;
        let info_offset = info_start - segment_start;
        let info_pos = ebml::start_master(&mut writer, ebml::INFO)?;
        ebml::write_uint(
            &mut writer,
            ebml::TIMESTAMP_SCALE,
            TIMESTAMP_SCALE_NS as u64,
        )?;
        let duration_patch_pos = if duration_secs > 0.0 {
            // Duration is expressed in TimestampScale ticks (not ms).
            let duration_ticks = duration_secs * 1_000_000_000.0 / TIMESTAMP_SCALE_NS as f64;
            ebml::write_float(&mut writer, ebml::DURATION, duration_ticks)?;
            None
        } else {
            // No declared duration (e.g. HD-DVD `.MAP` timemaps aren't parsed): reserve
            // a DURATION placeholder and back-patch it at finish() from the muxed
            // timeline. Payload sits 3 bytes in (2-byte ID `0x4489` + 1-byte size).
            let pos = writer.stream_position()?;
            ebml::write_float(&mut writer, ebml::DURATION, 0.0)?;
            Some(pos + 3)
        };
        // Stamp the freemkv version so any muxed file is traceable to the build
        // that produced it (surfaced as a media analyzer's "Writing
        // application"/"library" field).
        ebml::write_string(&mut writer, ebml::MUXING_APP, crate::MUX_APP)?;
        ebml::write_string(&mut writer, ebml::WRITING_APP, crate::MUX_APP)?;
        if let Some(t) = title {
            ebml::write_string(&mut writer, ebml::TITLE, t)?;
        }
        ebml::end_master(&mut writer, info_pos)?;

        // Tracks
        let tracks_start = writer.stream_position()?;
        let tracks_offset = tracks_start - segment_start;
        let tracks_pos = ebml::start_master(&mut writer, ebml::TRACKS)?;
        let mut track_uids: Vec<u64> = Vec::with_capacity(tracks.len());
        let mut channel_fixups: std::collections::HashMap<usize, ChannelFixup> =
            std::collections::HashMap::new();
        let mut pgs_forced_fixups: std::collections::HashMap<usize, PgsForcedFixup> =
            std::collections::HashMap::new();
        let mut codec_private_reserves: std::collections::HashMap<usize, u64> =
            std::collections::HashMap::new();
        let mut flag_interlaced_fixups: std::collections::HashMap<usize, FlagInterlacedFixup> =
            std::collections::HashMap::new();
        let mut frame_rate_fixups: std::collections::HashMap<usize, FrameRateFixup> =
            std::collections::HashMap::new();
        // Per track: whether it emitted a conforming `mvcC` BlockAdditionMapping.
        // Filled below from the SAME built record that drives the CodecPrivate
        // mvcC extension, so the three MVC signals never diverge.
        let mut track_has_mvc_mapping: Vec<bool> = Vec::with_capacity(tracks.len());
        for (i, track) in tracks.iter().enumerate() {
            let track_uid = (i + 1) as u64 | 0x100_0000;
            track_uids.push(track_uid);
            // Build the MVC (Blu-ray 3D) MVCDecoderConfigurationRecord ONCE per track;
            // `None` for non-3D or malformed params. Single source of truth for the
            // CodecPrivate mvcC extension, BlockAdditionMapping, and conformance flag.
            let mvc_record = track
                .mvc_params
                .as_ref()
                .and_then(|(sps, pps)| mvc_decoder_config_record(sps, pps));
            track_has_mvc_mapping.push(mvc_record.is_some());
            let entry_pos = ebml::start_master(&mut writer, ebml::TRACK_ENTRY)?;
            ebml::write_uint(&mut writer, ebml::TRACK_NUMBER, (i + 1) as u64)?;
            ebml::write_uint(&mut writer, ebml::TRACK_UID, track_uid)?;
            ebml::write_uint(&mut writer, ebml::TRACK_TYPE, track.track_type)?;
            if let Some(timing) = timings.get(i) {
                if timing.codec_delay_ns > 0 {
                    ebml::write_uint(&mut writer, ebml::CODEC_DELAY, timing.codec_delay_ns)?;
                }
                if timing.seek_preroll_ns > 0 {
                    ebml::write_uint(&mut writer, ebml::SEEK_PRE_ROLL, timing.seek_preroll_ns)?;
                }
            }

            if mvc_record.is_some() {
                ebml::write_uint(
                    &mut writer,
                    ebml::MAX_BLOCK_ADDITION_ID,
                    BLOCK_ADD_ID_VALUE_MVC,
                )?;
            }
            ebml::write_uint(&mut writer, ebml::FLAG_LACING, 0)?;
            ebml::write_string(&mut writer, ebml::CODEC_ID, track.codec_id)?;
            ebml::write_string(&mut writer, ebml::LANGUAGE, &track.language)?;
            if !track.name.is_empty() {
                ebml::write_string(&mut writer, ebml::TRACK_NAME, &track.name)?;
            }

            if !track.is_default {
                ebml::write_uint(&mut writer, ebml::FLAG_DEFAULT, 0)?;
            }
            if track.track_type == ebml::TRACK_TYPE_SUBTITLE && track.codec_id == ebml::CODEC_PGS {
                // Reserve a 1-byte FlagForced (initial = scan/vendor flag) and record
                // its offset, so PGS content can promote it to 1 at finish() if the
                // track proves forced narrative — same patchable-byte idiom as Channels.
                ebml::write_id(&mut writer, ebml::FLAG_FORCED)?;
                ebml::write_size(&mut writer, 1)?;
                let value_offset = writer.stream_position()?;
                writer.write_all(&[track.is_forced as u8])?;
                pgs_forced_fixups.insert(
                    i,
                    PgsForcedFixup {
                        value_offset,
                        initial_forced: track.is_forced,
                        tracker: super::codec::pgs::ForcedTracker::new(),
                    },
                );
            } else if track.is_forced {
                ebml::write_uint(&mut writer, ebml::FLAG_FORCED, 1)?;
            }

            // A_PCM defines no CodecPrivate; the LPCM parser's is BD re-mux metadata.
            if let Some(ref cp) = track.codec_private
                && track.codec_id != ebml::CODEC_PCM_BE
            {
                match mvc_record.as_ref() {
                    // MVC (Blu-ray 3D) base track: CodecPrivate = base-view avcC + `mvcC`
                    // extension, the track-level signal decoders read to recognise the
                    // stereoscopic track (dependent view rides in BlockAdditional below).
                    Some(record) => {
                        let cp_mvc = mvc_codec_private(cp, record);
                        ebml::write_binary(&mut writer, ebml::CODEC_PRIVATE, &cp_mvc)?;
                    }
                    // Non-MVC (2D/UHD/audio/…): write codec_private verbatim, unchanged.
                    None => ebml::write_binary(&mut writer, ebml::CODEC_PRIVATE, cp)?,
                }
            } else if track.codec_id == ebml::CODEC_AAC {
                codec_private_reserves.insert(i, writer.stream_position()?);
                writer.write_all(&ebml::void_element(CODEC_PRIVATE_RESERVE)?)?;
            }
            // Pre-0.13's deferred codecPrivate path was removed as dead code.

            // DefaultDuration — frame duration in nanoseconds
            if track.default_duration_ns > 0 {
                ebml::write_uint(
                    &mut writer,
                    ebml::DEFAULT_DURATION,
                    track.default_duration_ns,
                )?;
                if track.track_type == ebml::TRACK_TYPE_VIDEO && track.codec_id == ebml::CODEC_MPEG2
                {
                    let value_len = uint_len(track.default_duration_ns);
                    frame_rate_fixups.insert(
                        i,
                        FrameRateFixup {
                            value_offset: writer.stream_position()? - value_len as u64,
                            value_len,
                            initial_ns: track.default_duration_ns,
                            frames: 0,
                            min_pts_ns: i64::MAX,
                            max_pts_ns: i64::MIN,
                        },
                    );
                }
            }

            // DefaultDecodedFieldDuration: production video always passes 0 here
            // (emitting it made Windows Explorer report half fps / VFR, see
            // `MkvTrack::video`); the guard still emits a valid element for other callers.
            if track.track_type == ebml::TRACK_TYPE_VIDEO
                && track.interlaced
                && track.field_duration_ns > 0
            {
                ebml::write_uint(
                    &mut writer,
                    ebml::DEFAULT_DECODED_FIELD_DURATION,
                    track.field_duration_ns,
                )?;
            }

            // Video-specific
            if track.track_type == ebml::TRACK_TYPE_VIDEO && track.pixel_width > 0 {
                let vid_pos = ebml::start_master(&mut writer, ebml::VIDEO)?;
                ebml::write_uint(&mut writer, ebml::PIXEL_WIDTH, track.pixel_width as u64)?;
                ebml::write_uint(&mut writer, ebml::PIXEL_HEIGHT, track.pixel_height as u64)?;
                // FlagInterlaced (1=interlaced, 2=progressive) and FieldOrder are
                // written by hand (id+size+value) so their file offsets can be
                // captured for the whole-stream majority correction at finish().
                ebml::write_id(&mut writer, ebml::FLAG_INTERLACED)?;
                ebml::write_size(&mut writer, 1)?;
                let flag_offset = writer.stream_position()?;
                writer.write_all(&[if track.interlaced {
                    ebml::INTERLACED_INTERLACED as u8
                } else {
                    ebml::INTERLACED_PROGRESSIVE as u8
                }])?;
                let field_order_written =
                    track.interlaced && track.field_order != ebml::FIELD_ORDER_UNDETERMINED;
                let start = writer.stream_position()?;
                if field_order_written {
                    // `track.field_order` was set correctly before construction (the mux
                    // stream reads the first coded picture's measured field order).
                    ebml::write_uint(&mut writer, ebml::FIELD_ORDER, track.field_order as u64)?;
                } else if track.codec_id == ebml::CODEC_MPEG2 {
                    // Only MPEG-2 pictures measure a field order: reserve its size for finish().
                    writer.write_all(&ebml::void_element(FIELD_ORDER_LEN)?)?;
                }
                let end = writer.stream_position()?;
                let field_order_span = (end > start).then_some((start, end - start));
                flag_interlaced_fixups.insert(
                    i,
                    FlagInterlacedFixup {
                        value_offset: flag_offset,
                        field_order_span,
                        field_order_written,
                        initial_interlaced: track.interlaced,
                        progressive_pics: 0,
                        interlaced_pics: 0,
                        tff_pics: 0,
                        bff_pics: 0,
                    },
                );
                if track.display_width > 0 && track.display_height > 0 {
                    ebml::write_uint(&mut writer, ebml::DISPLAY_WIDTH, track.display_width as u64)?;
                    ebml::write_uint(
                        &mut writer,
                        ebml::DISPLAY_HEIGHT,
                        track.display_height as u64,
                    )?;
                }
                // Colour metadata (HDR). Open the Colour master when the track
                // carries CICP signalling OR measured HDR10 static metadata.
                if track.colour_matrix > 0 || track.colour_transfer > 0 || track.hdr10.is_some() {
                    let col_pos = ebml::start_master(&mut writer, ebml::COLOUR)?;
                    ebml::write_uint(
                        &mut writer,
                        ebml::MATRIX_COEFFICIENTS,
                        track.colour_matrix as u64,
                    )?;
                    ebml::write_uint(
                        &mut writer,
                        ebml::TRANSFER_CHARACTERISTICS,
                        track.colour_transfer as u64,
                    )?;
                    ebml::write_uint(&mut writer, ebml::PRIMARIES, track.colour_primaries as u64)?;
                    ebml::write_uint(&mut writer, ebml::RANGE, track.colour_range as u64)?;
                    // HDR10 static metadata — emitted ONLY when measured from the
                    // bitstream SEI (never fabricated for SDR).
                    if let Some(h) = track.hdr10 {
                        write_hdr10(&mut writer, &h)?;
                    }
                    ebml::end_master(&mut writer, col_pos)?;
                }
                ebml::end_master(&mut writer, vid_pos)?;
            }

            // Blu-ray 3D (MVC) signaling: BlockAdditionMapping carries the mvcC record
            // so players recognise the dependent (right-eye) view riding as a
            // per-frame BlockAdditional under this mapping (BlockAddIDValue = 2).
            match (mvc_record.as_ref(), track.mvc_params.as_ref()) {
                (Some(record), _) => {
                    let map_pos = ebml::start_master(&mut writer, ebml::BLOCK_ADDITION_MAPPING)?;
                    ebml::write_uint(
                        &mut writer,
                        ebml::BLOCK_ADD_ID_VALUE,
                        BLOCK_ADD_ID_VALUE_MVC,
                    )?;
                    ebml::write_uint(&mut writer, ebml::BLOCK_ADD_ID_TYPE, BLOCK_ADD_ID_TYPE_MVCC)?;
                    ebml::write_binary(&mut writer, ebml::BLOCK_ADD_ID_EXTRA_DATA, record)?;
                    ebml::end_master(&mut writer, map_pos)?;
                }
                // `mvc_params` present but the record failed to build (malformed params):
                // no mapping, `track_has_mvc_mapping` is already `false`, so
                // BlockAdditionals are dropped — file stays conforming, no orphan BlockAddID.
                (None, Some((s, p))) => {
                    tracing::warn!(
                        target: "mux",
                        "MVC track: could not build MVCDecoderConfigurationRecord from the \
                         dependent view's parameter sets (subset_sps={} B, pps={} B); \
                         emitting no mvcC mapping — the 3D pairing will not be signalled.",
                        s.len(),
                        p.len(),
                    );
                }
                (None, None) => {}
            }

            // Dolby Vision signaling — BlockAdditionMapping is a child of the
            // TrackEntry (sibling of Video). Carries the dvcC so players /
            // analyzers recognise the track as Dolby Vision.
            if let Some(ref dvcc) = track.dv_config {
                let map_pos = ebml::start_master(&mut writer, ebml::BLOCK_ADDITION_MAPPING)?;
                // BlockAddIDType = "dvcC" fourcc (DOVIDecoderConfigurationRecord).
                ebml::write_uint(&mut writer, ebml::BLOCK_ADD_ID_TYPE, BLOCK_ADD_ID_TYPE_DVCC)?;
                ebml::write_binary(&mut writer, ebml::BLOCK_ADD_ID_EXTRA_DATA, dvcc)?;
                ebml::end_master(&mut writer, map_pos)?;
            }

            // Audio-specific
            if track.track_type == ebml::TRACK_TYPE_AUDIO && track.sample_rate > 0.0 {
                let aud_pos = ebml::start_master(&mut writer, ebml::AUDIO)?;
                ebml::write_float(&mut writer, ebml::SAMPLING_FREQUENCY, track.sample_rate)?;
                // Omit Channels when unknown (0) — Matroska defaults it to 1
                // rather than us fabricating a 6-channel count.
                if track.channels > 0 {
                    // Capture the offset of the 1-byte Channels value so an AC-3 or MP2 track
                    // can correct it from its bitstream (the IFO count is unreliable); written
                    // explicitly so the in-place single-byte rewrite stays valid.
                    ebml::write_id(&mut writer, ebml::CHANNELS)?;
                    ebml::write_size(&mut writer, 1)?;
                    let value_offset = writer.stream_position()?;
                    writer.write_all(&[track.channels])?;
                    let source = match track.codec_id {
                        ebml::CODEC_AC3 => Some(ChannelSource::Ac3),
                        ebml::CODEC_MP2 => Some(ChannelSource::MpegLayerII(Default::default())),
                        _ => None,
                    };
                    if let Some(source) = source {
                        channel_fixups.insert(
                            i,
                            ChannelFixup {
                                value_offset,
                                claimed: track.channels,
                                corrected: false,
                                source,
                            },
                        );
                    }
                }
                if track.bit_depth > 0 {
                    ebml::write_uint(&mut writer, ebml::BIT_DEPTH, track.bit_depth as u64)?;
                }
                ebml::end_master(&mut writer, aud_pos)?;
            }

            ebml::end_master(&mut writer, entry_pos)?;
        }
        ebml::end_master(&mut writer, tracks_pos)?;

        // Chapters
        let mut chapters_offset: Option<u64> = None;
        if !chapters.is_empty() {
            let chapters_start = writer.stream_position()?;
            chapters_offset = Some(chapters_start - segment_start);
            let chapters_pos = ebml::start_master(&mut writer, ebml::CHAPTERS)?;
            let edition_pos = ebml::start_master(&mut writer, ebml::EDITION_ENTRY)?;
            for (i, ch) in chapters.iter().enumerate() {
                let atom_pos = ebml::start_master(&mut writer, ebml::CHAPTER_ATOM)?;
                ebml::write_uint(&mut writer, ebml::CHAPTER_UID, (i + 1) as u64)?;
                let time_ns = (ch.time_secs * 1_000_000_000.0) as u64;
                ebml::write_uint(&mut writer, ebml::CHAPTER_TIME_START, time_ns)?;
                let display_pos = ebml::start_master(&mut writer, ebml::CHAPTER_DISPLAY)?;
                ebml::write_string(&mut writer, ebml::CHAP_STRING, &ch.name)?;
                ebml::write_string(&mut writer, ebml::CHAP_LANGUAGE, "und")?;
                ebml::end_master(&mut writer, display_pos)?;
                ebml::end_master(&mut writer, atom_pos)?;
            }
            ebml::end_master(&mut writer, edition_pos)?;
            ebml::end_master(&mut writer, chapters_pos)?;
        }

        Ok(Self {
            writer,
            segment_start,
            cluster_open: false,
            cluster_pos: 0,
            cluster_size_pos: 0,
            block_group_buf: Vec::new(),
            cluster_ts_ticks: 0,
            base_pts_ticks: None,
            last_pts_ticks: std::collections::HashMap::new(),
            track_is_video: tracks
                .iter()
                .map(|t| t.track_type == ebml::TRACK_TYPE_VIDEO)
                .collect(),
            track_is_subtitle: tracks
                .iter()
                .map(|t| t.track_type == ebml::TRACK_TYPE_SUBTITLE)
                .collect(),
            track_is_vobsub: tracks
                .iter()
                .map(|t| t.codec_id == ebml::CODEC_VOBSUB)
                .collect(),
            primary_video_track: tracks
                .iter()
                .position(|t| t.track_type == ebml::TRACK_TYPE_VIDEO),
            track_has_mvc_mapping,
            continuity: TimelineContinuity::new(),
            cues: Vec::new(),
            frame_count: 0,
            dropped_pre_cluster: 0,
            origin_lead_ticks: 0,
            vobsub_open_end: std::collections::HashMap::new(),
            seek_fixups,
            cues_seek_entry_pos,
            info_offset,
            tracks_offset,
            chapters_offset,
            track_bytes: vec![0u64; tracks.len()],
            track_uids,
            duration_secs,
            duration_patch_pos,
            max_block_ticks: 0,
            last_video_keyframe_ticks: vec![None; tracks.len()],
            channel_fixups,
            pgs_forced_fixups,
            flag_interlaced_fixups,
            opening_capture: None,
            codec_private_reserves,
            frame_rate_fixups,
        })
    }

    /// Fill a CodecPrivate reserved at header time. `Ok(false)` when the track
    /// had no reservation (already set, or not AAC) or `cp` does not fit.
    pub(crate) fn set_codec_private(&mut self, track: usize, cp: &[u8]) -> io::Result<bool> {
        let Some(&pos) = self.codec_private_reserves.get(&track) else {
            return Ok(false);
        };
        let mut rest = match CODEC_PRIVATE_RESERVE.checked_sub(3 + cp.len()) {
            Some(r) => r,
            None => return Ok(false),
        };
        let mut el = Vec::with_capacity(CODEC_PRIVATE_RESERVE);
        ebml::write_id(&mut el, ebml::CODEC_PRIVATE)?;
        // A 1-byte remainder cannot hold a Void, so widen the size VINT instead.
        if rest == 1 {
            el.extend_from_slice(&[0x40, cp.len() as u8]);
            rest = 0;
        } else {
            ebml::write_size(&mut el, cp.len() as u64)?;
        }
        el.extend_from_slice(cp);
        if rest >= 2 {
            el.extend_from_slice(&ebml::void_element(rest)?);
        }
        let here = self.writer.stream_position()?;
        self.writer.seek(std::io::SeekFrom::Start(pos))?;
        self.writer.write_all(&el)?;
        self.writer.seek(std::io::SeekFrom::Start(here))?;
        self.codec_private_reserves.remove(&track);
        Ok(true)
    }

    // Overwrite a 3-byte big-endian value at `pos`, then return to the end.
    fn patch_u24(&mut self, pos: u64, val: u64) -> io::Result<()> {
        let end = self.writer.stream_position()?;
        self.writer.seek(io::SeekFrom::Start(pos))?;
        self.writer.write_all(&val.to_be_bytes()[5..])?;
        self.writer.seek(io::SeekFrom::Start(end))?;
        Ok(())
    }

    /// Put the timeline origin `lead_ns` before the first video keyframe (the
    /// earliest audio/video frame), and count `dropped` frames that lay before it.
    pub(crate) fn set_origin_lead_ns(&mut self, lead_ns: i64, dropped: u64) {
        self.origin_lead_ticks = lead_ns.max(0) / TIMESTAMP_SCALE_NS;
        self.dropped_pre_cluster += dropped;
    }

    /// Attach an opening-frame capture (`--log-level 3`). The capture writes the
    /// first ~100 coded frames per track to `<output>.opening.bin` and logs a
    /// per-frame summary, so opening-GOP / menu issues are diagnosable from a
    /// log + side file without the disc. `None` is a no-op (normal runs).
    pub(crate) fn set_opening_capture(&mut self, capture: Option<crate::diag::OpeningCapture>) {
        self.opening_capture = capture;
    }
    /// Drive seam correction from the title's PlayItem marks instead of inferring it from PTS
    /// jumps. Each clip is placed at the sum of the earlier clips' durations, so output runs
    /// exactly as long as the playlist says. No-op for fewer than two clips or without usable
    /// marks — DVD, HD-DVD and file sources keep the inference path.
    pub(crate) fn set_clips(
        &mut self,
        clips: &[crate::disc::Clip],
        content_format: crate::disc::ContentFormat,
    ) {
        self.continuity = TimelineContinuity::with_clips(clips, content_format);
    }

    /// Write a single frame. `duration_ns = Some` emits a `BlockGroup` with
    /// `BlockDuration` (e.g. PGS subtitles, so the on-screen bitmap is
    /// removed at the right time); otherwise a plain `SimpleBlock`.
    ///
    /// TEST-ONLY wrapper over [`MkvMuxer::write_frame_at`] — production
    /// calls `write_frame_at_with_padding` to preserve container trimming.
    #[cfg(test)]
    pub fn write_frame(
        &mut self,
        track_idx: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
        duration_ns: Option<u64>,
        block_additional: Option<&[u8]>,
    ) -> io::Result<()> {
        self.write_frame_at(
            track_idx,
            pts_ns,
            keyframe,
            data,
            duration_ns,
            block_additional,
            None,
            None,
        )
    }

    /// `write_frame` with the frame's SOURCE BYTE OFFSET, used to identify
    /// which clip it came from under a seam plan (ambiguous by timestamp
    /// alone inside an overlap). Pass `frame.source.map(|s| s.byte)`.
    ///
    /// `block_additional`, when `Some`, is a Matroska `BlockAdditional` (BlockAddID=2) for
    /// Blu-ray 3D MVC dependent-view data; such a frame is always a `BlockGroup` with a
    /// `ReferenceBlock` when not a keyframe. `None` for non-3D.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub fn write_frame_at(
        &mut self,
        track_idx: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
        duration_ns: Option<u64>,
        block_additional: Option<&[u8]>,
        src_byte: Option<u64>,
        scan: Option<super::codec::FieldOrder>,
    ) -> io::Result<()> {
        use super::codec::{FieldOrder, PictureInfo, coding::CodingType, coding::Mpeg2Coding};
        // A P frame picture carrying `scan`.
        let coding = scan.map(|o| {
            PictureInfo::mpeg2(
                CodingType::P,
                Mpeg2Coding {
                    top_field_first: o == FieldOrder::Tff,
                    repeat_first_field: false,
                    progressive_frame: o == FieldOrder::Progressive,
                    progressive_sequence: false,
                    frame_picture: true,
                },
            )
        });
        self.write_frame_at_with_padding(
            track_idx,
            pts_ns,
            keyframe,
            data,
            duration_ns,
            block_additional,
            src_byte,
            coding,
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_frame_at_with_padding(
        &mut self,
        track_idx: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
        duration_ns: Option<u64>,
        block_additional: Option<&[u8]>,
        src_byte: Option<u64>,
        coding: Option<super::codec::PictureInfo>,
        discard_padding_ns: i64,
    ) -> io::Result<()> {
        // This picture's measured scan type, tallied for the FlagInterlaced majority.
        let scan = coding.as_ref().and_then(|c| c.field_order());
        // --log-level 3: capture the first ~100 coded frames per track to the side file
        // BEFORE any timeline mangling, with the parser's own PTS, so an opening-GOP
        // issue is reconstructable offline. No-op/no alloc normally (capture = `None`).
        if let Some(cap) = self.opening_capture.as_mut() {
            cap.record(track_idx, pts_ns, keyframe, data);
        }

        // A block for an undeclared TrackNumber would make the file inconsistent.
        if track_idx >= self.track_uids.len() {
            return Err(crate::error::Error::MuxTrackRange {
                track: track_idx,
                tracks: self.track_uids.len(),
            }
            .into());
        }

        // Is this a video track? Used for the monotonic block-timestamp nudge
        // below, which must exempt EVERY video track (incl. a Dolby Vision EL).
        let is_video = self.track_is_video.get(track_idx).copied().unwrap_or(false);

        // Clip-boundary epochs are driven by the PRIMARY video track only (not literal
        // index 0 — M2TS/PMT can list audio first): a Dolby Vision EL's overlapping PTS
        // would false-trigger a reset every GOP, inflating a 1-clip title to ~7h.
        let drives_epoch = Some(track_idx) == self.primary_video_track;

        // Map the raw PES PTS onto the continuous timeline FIRST: source PTS jumps
        // backward at a clip boundary, so rebasing avoids non-monotonic timestamps.
        // `None` = outside clip IN/OUT marks; dropping avoids duplicate content at a join.
        let Some(pts_ns) = self.continuity.map_picture(
            pts_ns,
            drives_epoch,
            track_idx,
            is_video,
            src_byte,
            is_video.then_some(super::timeline::SeamPic { keyframe, coding }),
        ) else {
            return Ok(());
        };
        // Tally this KEPT picture's measured scan type for the whole-stream
        // FlagInterlaced majority (dropped-by-mark frames must not count). A
        // picture with no scan signal (`None`) counts toward neither.
        if let Some(scan) = scan
            && let Some(fixup) = self.flag_interlaced_fixups.get_mut(&track_idx)
        {
            use super::codec::FieldOrder;
            match scan {
                FieldOrder::Progressive => fixup.progressive_pics += 1,
                FieldOrder::Tff => {
                    fixup.interlaced_pics += 1;
                    fixup.tff_pics += 1;
                }
                FieldOrder::Bff => {
                    fixup.interlaced_pics += 1;
                    fixup.bff_pics += 1;
                }
            }
        }
        if let Some(f) = self.frame_rate_fixups.get_mut(&track_idx) {
            f.frames += 1;
            f.min_pts_ns = f.min_pts_ns.min(pts_ns);
            f.max_pts_ns = f.max_pts_ns.max(pts_ns);
        }
        let raw_ticks = pts_ns / TIMESTAMP_SCALE_NS;

        // Clusters normally open on a video keyframe so Cues resolve to a seekable
        // IDR. For an audio/subtitle-only title (no video track) fall back to track
        // 0's keyframes, or no cluster would ever open and every frame would drop.
        let cluster_driver = self.primary_video_track.unwrap_or(0);
        let is_video_key = keyframe && track_idx == cluster_driver;

        // Base is the first *kept* keyframe, NOT the first frame seen: with B-frame
        // reordering the first frame seen can have a higher PTS, which would make
        // later timestamps negative and wrap to ~u64::MAX on the `as u64` cast.
        let base = match self.base_pts_ticks {
            Some(b) => b,
            None => {
                if !is_video_key {
                    // No cluster can open yet (clusters start on a track-0
                    // keyframe). Drop this frame as before, but count it so an
                    // all-dropped run surfaces as an error at finish().
                    self.dropped_pre_cluster += 1;
                    return Ok(());
                }
                let base = raw_ticks - self.origin_lead_ticks;
                self.base_pts_ticks = Some(base);
                base
            }
        };
        // Floor at 0: a frame earlier than base (pre-keyframe audio, or a stream
        // discontinuity back-jump) would compute negative and wrap to ~u64::MAX on
        // the `as u64` write; clamp to t=0 instead of corrupting the timeline.
        let pts_ticks = (raw_ticks - base).max(0);

        // Strictly-monotonic timestamps — AUDIO/SUBTITLE ONLY: nudge a truncated/
        // backward PES tick to prev+1 (inaudible). VIDEO is EXEMPT: B-frame PTS is
        // legitimately non-monotonic and forcing it makes decoders reject the DTS.
        let pts_ticks = block_ts(
            is_video,
            self.last_pts_ticks.get(&track_idx).copied(),
            pts_ticks,
        );

        let needs_new_cluster = !self.cluster_open
            || (is_video_key && (pts_ticks - self.cluster_ts_ticks) >= CLUSTER_DURATION_TICKS);

        if needs_new_cluster {
            if !is_video_key {
                // A cluster is open but this non-keyframe wants a fresh one only
                // because !cluster_open is false here — so this branch is the
                // "no cluster open and not a keyframe" case. Drop and count.
                if !self.cluster_open {
                    self.dropped_pre_cluster += 1;
                }
                return Ok(());
            }
            // The first keyframe sits `origin_lead_ticks` after the earliest sample: open
            // its cluster as early as a block offset allows, so the earlier samples
            // replayed after it land in the same cluster and clusters stay ascending.
            let open_ts = if self.last_pts_ticks.is_empty() {
                (pts_ticks - MAX_BLOCK_REL).clamp(0, pts_ticks)
            } else {
                pts_ticks
            };
            self.start_cluster(open_ts)?;
            self.cues.push(CuePoint {
                timestamp_ticks: pts_ticks,
                track: track_idx + 1,
                cluster_pos: self.cluster_pos - self.segment_start,
            });
        } else {
            let rel = pts_ticks - self.cluster_ts_ticks;
            if !(MIN_BLOCK_REL..=MAX_BLOCK_REL).contains(&rel) {
                // Block-relative timestamp is signed 16-bit (~±3.27s at 0.1ms scale); a
                // long GOP/audio stretch or PTS back-jump can wrap it, so force a fresh
                // cluster, with a Cue entry if this is a keyframe.
                self.start_cluster(pts_ticks)?;
                if keyframe {
                    self.cues.push(CuePoint {
                        timestamp_ticks: pts_ticks,
                        track: track_idx + 1,
                        cluster_pos: self.cluster_pos - self.segment_start,
                    });
                }
            }
        }

        // Committed to writing this frame — record its (monotonic) timestamp so
        // the next block on this track is forced strictly later.
        self.last_pts_ticks.insert(track_idx, pts_ticks);
        // Track the highest block END (start + duration when known), not just start,
        // so a back-patched Segment Duration (for a missing source duration) covers
        // the final frame's full presentation instead of understating the runtime.
        let block_end_ticks = pts_ticks + duration_ns.map_or(0, |d| ns_to_ticks(d) as i64);
        self.max_block_ticks = self.max_block_ticks.max(block_end_ticks);

        let relative_ts = (pts_ticks - self.cluster_ts_ticks) as i16;
        let duration_ticks = duration_ns.map(ns_to_ticks);
        // Defense in depth (issue #52): a SUBTITLE block must never be a bare
        // SimpleBlock (no DefaultDuration => unbounded cue, ffmpeg "Timestamps are
        // unset"). Substitute a minimum fallback so it takes the BlockGroup arm.
        let is_subtitle = self
            .track_is_subtitle
            .get(track_idx)
            .copied()
            .unwrap_or(false);
        let is_vobsub = self
            .track_is_vobsub
            .get(track_idx)
            .copied()
            .unwrap_or(false);
        if is_vobsub && let Some((pos, start)) = self.vobsub_open_end.remove(&track_idx) {
            let ticks = ((pts_ticks - start).max(1) as u64).min(VOBSUB_OPEN_END_TICKS);
            self.patch_u24(pos, ticks)?;
        }
        let mut vobsub_open = false;
        let duration_ticks = match duration_ticks {
            Some(dt) => Some(dt),
            None if is_vobsub => match vobsub_display_ns(data).filter(|&ns| ns > 0) {
                Some(ns) => Some(ns_to_ticks(ns)),
                None => {
                    vobsub_open = true;
                    Some(VOBSUB_OPEN_END_TICKS)
                }
            },
            None if is_subtitle => {
                Some((PGS_FALLBACK_BLOCK_DURATION_NS / TIMESTAMP_SCALE_NS) as u64)
            }
            None => None,
        };
        // A BlockAdditional with BlockAddID=2 conforms only when the track declared
        // the matching mvcC mapping; if not (dependent-view params never captured
        // before the header was written), drop it and emit a plain block instead.
        let block_additional = match block_additional {
            Some(a)
                if self
                    .track_has_mvc_mapping
                    .get(track_idx)
                    .copied()
                    .unwrap_or(false) =>
            {
                Some(a)
            }
            Some(_) => None,
            None => None,
        };
        // Offset of the referenced keyframe for a non-keyframe BlockGroup: inside a
        // BlockGroup the SimpleBlock keyframe bit is reserved/0, so keyframe-ness is
        // carried only by presence of a ReferenceBlock (gated to video; falls back to 0).
        let reference = if keyframe || !is_video {
            None
        } else {
            Some(
                self.last_video_keyframe_ticks
                    .get(track_idx)
                    .copied()
                    .flatten()
                    .map(|kf| kf - pts_ticks)
                    .unwrap_or(0),
            )
        };
        if discard_padding_ns != 0 {
            let mut buf = std::mem::take(&mut self.block_group_buf);
            buf.clear();
            let result = Self::build_block_group(
                &mut buf,
                track_idx + 1,
                relative_ts,
                data,
                reference,
                duration_ticks,
                block_additional,
                discard_padding_ns,
            )
            .and_then(|()| self.writer.write_all(&buf));
            self.block_group_buf = buf;
            result?;
        } else {
            match block_additional {
                // MVC: base view Block + dependent-view BlockAdditional, always a BlockGroup;
                // non-keyframe base frames get a ReferenceBlock so a player never treats
                // a P/B frame as a seek point.
                Some(additional) => {
                    self.write_block_group_mvc(
                        track_idx + 1,
                        relative_ts,
                        data,
                        additional,
                        reference,
                        duration_ticks,
                    )?;
                }
                None => match duration_ticks {
                    // BlockDuration present (PGS subtitles, AC-3 audio, and EVERY
                    // MPEG-2 video frame) → BlockGroup.
                    Some(dt) => {
                        self.write_block_group(track_idx + 1, relative_ts, data, reference, dt)?;
                    }
                    None => {
                        self.write_simple_block(track_idx + 1, relative_ts, keyframe, data)?;
                    }
                },
            }
        }
        // BlockDuration is the group's last element: its 3 value bytes end the write.
        if vobsub_open {
            let end = self.writer.stream_position()?;
            self.vobsub_open_end.insert(track_idx, (end - 3, pts_ticks));
        }
        // Recorded per track (not a single global slot) so a later non-keyframe
        // references a keyframe on its OWN track — a shared slot produced cross-track
        // references on a multi-video-track title (MVC base+EL, or a two-angle disc).
        if keyframe
            && is_video
            && let Some(slot) = self.last_video_keyframe_ticks.get_mut(track_idx)
        {
            *slot = Some(pts_ticks);
        }
        self.frame_count += 1;

        // Per-track byte total for the finalize-time BPS statistics tag.
        if let Some(b) = self.track_bytes.get_mut(track_idx) {
            *b += data.len() as u64;
        }

        // Correct Channels from the first frame the bitstream describes: the DVD IFO count is
        // unreliable (5.1 on a 2.0 AC-3 stream; an MP2 program count on a stereo base layer).
        // Byte width is unchanged, so the patch is a single-byte in-place rewrite.
        if let Some(fixup) = self.channel_fixups.get_mut(&track_idx)
            && !fixup.corrected
        {
            let actual = match &mut fixup.source {
                ChannelSource::Ac3 => super::codec::ac3::acmod_channels(data).filter(|&c| c > 0),
                // A frame that proves nothing leaves the IFO value in place; the tracker settles
                // on the first verified frame or after its bounded look (never on a failure).
                ChannelSource::MpegLayerII(t) => t.observe(data),
            };
            // None: frame too short or no valid header; keep the IFO value, retry next frame.
            if let Some(actual) = actual {
                fixup.corrected = true;
                let (offset, claimed) = (fixup.value_offset, fixup.claimed);
                self.patch_channels(track_idx, offset, claimed, actual)?;
            }
        }

        // Accumulate PGS forced-subtitle state; `finish()` promotes FlagForced only
        // for a track that displayed subtitles and had every one forced.
        if let Some(fixup) = self.pgs_forced_fixups.get_mut(&track_idx) {
            fixup.tracker.observe(data);
        }

        Ok(())
    }

    // Apply deferred in-place byte patches in `finish()`; offsets are disjoint
    // fixed-size regions reserved up-front, so nothing shifts. Shared by the
    // PGS-forced and FlagInterlaced fixups.
    fn patch_bytes(&mut self, patches: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if patches.is_empty() {
            return Ok(());
        }
        let here = self.writer.stream_position()?;
        for (off, bytes) in patches {
            self.writer.seek(std::io::SeekFrom::Start(*off))?;
            self.writer.write_all(bytes)?;
        }
        self.writer.seek(std::io::SeekFrom::Start(here))?;
        Ok(())
    }

    /// Finish the MKV file: write Cues element.
    ///
    /// A cluster only opens on a keyframe from the PRIMARY VIDEO TRACK, not necessarily index 0
    /// (see `cluster_driver`). The caller must deliver a keyframe on that track
    /// before/alongside other-track data, or every `write_frame` is silently dropped; `finish`
    /// then returns `Error::MkvInvalid` rather than emit a structurally valid but empty MKV.
    // Rewrites a track's 1-byte Channels value in place when the bitstream disagrees.
    fn patch_channels(
        &mut self,
        track: usize,
        offset: u64,
        claimed: u8,
        actual: u8,
    ) -> io::Result<()> {
        if let Some(ChannelFixup {
            source: ChannelSource::MpegLayerII(t),
            ..
        }) = self.channel_fixups.get(&track)
        {
            let (frames, last) = t.evidence();
            let ext = t.extension_signalled();
            tracing::debug!(
                target: "freemkv::diag",
                "tag=mp2.channels track={track} declared={claimed} stored={actual} frames={frames} extension_signalled={ext} last_fallback={last:?}",
            );
        }
        if actual == claimed {
            return Ok(());
        }
        tracing::warn!(
            target: "mux",
            "audio track {track}: IFO claimed {claimed} channels but the stored bitstream carries {actual}; trusting the bitstream",
        );
        let here = self.writer.stream_position()?;
        self.writer.seek(std::io::SeekFrom::Start(offset))?;
        self.writer.write_all(&[actual])?;
        self.writer.seek(std::io::SeekFrom::Start(here))?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> io::Result<()> {
        // An MP2 track shorter than the tracker's look settles on its base count now.
        let pending: Vec<(usize, u64, u8, u8)> = self
            .channel_fixups
            .iter_mut()
            .filter(|(_, f)| !f.corrected)
            .filter_map(|(&i, f)| match &mut f.source {
                ChannelSource::MpegLayerII(t) => {
                    t.finish().map(|n| (i, f.value_offset, f.claimed, n))
                }
                ChannelSource::Ac3 => None,
            })
            .collect();
        for (track, offset, claimed, actual) in pending {
            self.patch_channels(track, offset, claimed, actual)?;
        }
        // Order matters: a title the seam plan dropped ENTIRELY also has zero frames,
        // and `MkvInvalid` is classified as a skippable empty nav/menu stub — so that
        // case must be decided FIRST, or a real feature silently drops at exit 0.
        if self.frame_count == 0 && self.continuity.dropped_total() > 0 {
            return Err(crate::error::Error::SinkWroteNothing.into());
        }
        if self.frame_count == 0 {
            return Err(crate::error::Error::MkvInvalid.into());
        }
        // Partial pre-cluster drops don't fail the mux (leading audio ahead of the
        // first video IDR is normal), but must leave a record — this counter was
        // write-only, so frames silently vanished while the run reported success.
        if self.dropped_pre_cluster > 0 {
            tracing::warn!(
                target: "mux",
                dropped = self.dropped_pre_cluster,
                frames_written = self.frame_count,
                driver_track = self.primary_video_track.unwrap_or(0),
                "frames were discarded before the first cluster opened (no keyframe on the cluster-driving track had arrived yet); they are absent from the output"
            );
        }
        // Frames excluded by the playlist's clip marks are dropped on purpose (a join
        // legitimately discards material a disc stores twice), but the count must not
        // be write-only — same reasoning as the pre-cluster counter above.
        let seam_dropped = self.continuity.dropped_total();
        // Counting isn't bounding: if marks don't line up with the PES clock, the plan
        // can discard most of a title, and the only other gate (zero-frame check) is a
        // skippable stub — a title emitting seconds of a feature must not exit 0.
        if seam_dropped > self.frame_count {
            return Err(crate::error::Error::SeamPlanDroppedMost {
                dropped: seam_dropped,
                written: self.frame_count,
            }
            .into());
        }
        if seam_dropped > 0 {
            tracing::info!(
                target: "mux",
                dropped = seam_dropped,
                frames_written = self.frame_count,
                "frames outside the playlist's clip marks were dropped at clip joins"
            );
        }
        // The source declared no duration up-front (DURATION was reserved as a
        // placeholder). Derive the real runtime from the muxed timeline so the
        // Segment declares it — and so the BPS tags below can be computed.
        if self.duration_patch_pos.is_some() && self.max_block_ticks > 0 {
            self.duration_secs =
                self.max_block_ticks as f64 * TIMESTAMP_SCALE_NS as f64 / 1_000_000_000.0;
        }
        // Close final cluster
        self.end_cluster()?;

        // Correct FlagForced for PGS tracks from what the mux saw (in-place rewrite).
        // PROMOTE (0->1) a track whose displays all carry `forced_on_flag`. DEMOTE
        // (1->0) a vendor-labelled track that contradicts it, per [`super::codec::pgs::demotable`].
        let disc_uses_forced_flag = self
            .pgs_forced_fixups
            .values()
            .any(|f| f.tracker.facts().forced_displays > 0);
        let busiest = self
            .pgs_forced_fixups
            .values()
            .map(|f| f.tracker.facts().displays)
            .max()
            .unwrap_or(0);
        let rewrites: Vec<(u64, u8)> = self
            .pgs_forced_fixups
            .values()
            .filter_map(|f| {
                if f.tracker.is_forced() && !f.initial_forced {
                    return Some((f.value_offset, 1u8));
                }
                if f.initial_forced
                    && !f.tracker.is_forced()
                    && super::codec::pgs::demotable(
                        f.tracker.facts(),
                        disc_uses_forced_flag,
                        busiest,
                    )
                {
                    tracing::info!(
                        target: "mux",
                        displays = f.tracker.facts().displays,
                        forced_displays = f.tracker.facts().forced_displays,
                        busiest,
                        "PGS track labelled forced showed no forced display sets on a disc that uses the flag; clearing FlagForced"
                    );
                    return Some((f.value_offset, 0u8));
                }
                None
            })
            .collect();
        self.patch_bytes(
            &rewrites
                .into_iter()
                .map(|(off, value)| (off, vec![value]))
                .collect::<Vec<_>>(),
        )?;

        // Correct an MPEG-2 track's DefaultDuration from its measured frame period.
        let rate_patches: Vec<(u64, Vec<u8>)> = self
            .frame_rate_fixups
            .values()
            .filter(|f| f.frames >= FRAME_RATE_MIN_FRAMES)
            .filter_map(|f| {
                let measured = (f.max_pts_ns - f.min_pts_ns) as f64 / (f.frames - 1) as f64;
                let ns = standard_frame_period_ns(measured)?;
                (ns != f.initial_ns && uint_len(ns) == f.value_len)
                    .then(|| (f.value_offset, ns.to_be_bytes()[8 - f.value_len..].to_vec()))
            })
            .collect();
        self.patch_bytes(&rate_patches)?;

        // Correct FlagInterlaced from the WHOLE-stream scan majority: the header
        // value came from only the FIRST coded picture. Rewrite the byte to the
        // dominant scan; on a demotion to progressive, Void the stale FieldOrder.
        let interlaced_rewrites: Vec<InterlacedRewrite> = self
            .flag_interlaced_fixups
            .values()
            .filter_map(|f| {
                // No scan signal at all (e.g. H.264/HEVC): keep the provisional value.
                if f.progressive_pics == 0 && f.interlaced_pics == 0 {
                    return None;
                }
                // Interlaced only on a strict majority — a tie stays progressive so
                // a mostly-progressive title is not needlessly deinterlaced.
                let final_interlaced = f.interlaced_pics > f.progressive_pics;
                // Interlaced with no order written up-front: fill the reserve (strict majority).
                let field_order = (final_interlaced && !f.field_order_written)
                    .then(|| match f.tff_pics.cmp(&f.bff_pics) {
                        std::cmp::Ordering::Greater => Some(ebml::FIELD_ORDER_TFF),
                        std::cmp::Ordering::Less => Some(ebml::FIELD_ORDER_BFF),
                        std::cmp::Ordering::Equal => None,
                    })
                    .flatten();
                if final_interlaced == f.initial_interlaced && field_order.is_none() {
                    return None; // the first picture already matched the majority
                }
                Some(InterlacedRewrite {
                    value_offset: f.value_offset,
                    final_interlaced,
                    field_order_span: f.field_order_span,
                    field_order,
                    changed: final_interlaced != f.initial_interlaced,
                })
            })
            .collect();
        let mut interlaced_patches: Vec<(u64, Vec<u8>)> = Vec::new();
        for rw in &interlaced_rewrites {
            let value = if rw.final_interlaced {
                ebml::INTERLACED_INTERLACED
            } else {
                ebml::INTERLACED_PROGRESSIVE
            } as u8;
            interlaced_patches.push((rw.value_offset, vec![value]));
            if let (Some(order), Some((fo_off, fo_len))) = (rw.field_order, rw.field_order_span) {
                let mut el = Vec::new();
                ebml::write_uint(&mut el, ebml::FIELD_ORDER, u64::from(order))?;
                if el.len() as u64 == fo_len {
                    interlaced_patches.push((fo_off, el));
                }
            }
            // Demotion to progressive: the up-front FieldOrder element is now
            // contradictory — overwrite it with a same-length Void so no later
            // element shifts.
            if !rw.final_interlaced
                && let Some((fo_off, fo_len)) = rw.field_order_span
            {
                interlaced_patches.push((fo_off, ebml::void_element(fo_len as usize)?));
            }
            if rw.changed {
                tracing::warn!(
                    target: "mux",
                    interlaced = rw.final_interlaced,
                    "FlagInterlaced corrected from the whole-stream scan majority; the first coded picture did not represent the title"
                );
            }
        }
        self.patch_bytes(&interlaced_patches)?;

        // Write Cues
        let cues_start = self.writer.stream_position()?;
        let cues_offset = cues_start - self.segment_start;
        let have_cues = !self.cues.is_empty();
        if !self.cues.is_empty() {
            let cues_pos = ebml::start_master(&mut self.writer, ebml::CUES)?;
            for cue in &self.cues {
                let cp_pos = ebml::start_master(&mut self.writer, ebml::CUE_POINT)?;
                ebml::write_uint(&mut self.writer, ebml::CUE_TIME, cue.timestamp_ticks as u64)?;
                let ctp_pos = ebml::start_master(&mut self.writer, ebml::CUE_TRACK_POSITIONS)?;
                ebml::write_uint(&mut self.writer, ebml::CUE_TRACK, cue.track as u64)?;
                ebml::write_uint(
                    &mut self.writer,
                    ebml::CUE_CLUSTER_POSITION,
                    cue.cluster_pos,
                )?;
                ebml::end_master(&mut self.writer, ctp_pos)?;
                ebml::end_master(&mut self.writer, cp_pos)?;
            }
            ebml::end_master(&mut self.writer, cues_pos)?;
        }

        // Per-track BPS statistics tags (mkvmerge convention): a reader that shows
        // container `BPS` (Windows Explorer) rather than computing it from stream
        // size gets a bitrate for EVERY track this way, not just CBR audio.
        self.write_bps_tags()?;

        // Back-patch SeekHead SeekPosition values now offsets are known. If no Cues
        // were written, skip the CUES fixup (else it'd point at whatever now sits at
        // `cues_offset` — Tags/EOF) and Void the whole entry below instead.
        for fixup in &self.seek_fixups {
            if fixup.target_id == ebml::CUES && !have_cues {
                continue;
            }
            let offset = match fixup.target_id {
                ebml::INFO => self.info_offset,
                ebml::TRACKS => self.tracks_offset,
                // The CHAPTERS entry is reserved only with chapters, which set the offset.
                ebml::CHAPTERS => match self.chapters_offset {
                    Some(off) => off,
                    None => return Err(crate::error::Error::MkvUnencodable.into()),
                },
                ebml::CUES => cues_offset,
                // The SeekHead reserves only the targets above.
                _ => return Err(crate::error::Error::MkvUnencodable.into()),
            };
            self.writer
                .seek(std::io::SeekFrom::Start(fixup.value_offset))?;
            self.writer.write_all(&offset.to_be_bytes())?;
        }
        // Back-patch the DURATION placeholder (source supplied no duration) with
        // the real runtime from the muxed timeline. The CUES-void and seek-to-end
        // below re-seek absolutely, so no position restore is needed here.
        if let Some(pos) = self.duration_patch_pos {
            if self.max_block_ticks > 0 {
                self.writer.seek(std::io::SeekFrom::Start(pos))?;
                self.writer
                    .write_all(&(self.max_block_ticks as f64).to_be_bytes())?;
            } else {
                // Timeline never advanced past tick 0 (degenerate single-frame recovery):
                // can't derive a runtime, so DON'T leave DURATION=0.0 (reads as corrupt).
                // Void the whole 11-byte element instead; `pos` is payload start (+3), back up 3.
                self.writer.seek(std::io::SeekFrom::Start(pos - 3))?;
                ebml::write_id(&mut self.writer, ebml::VOID)?;
                ebml::write_size(&mut self.writer, 9)?; // 11 - 1 (Void id) - 1 (size)
                self.writer.write_all(&[0u8; 9])?;
            }
        }

        // Neutralise the unused CUES Seek entry: it's a fixed 21-byte Seek master, and
        // a 1-byte Void ID + 1-byte size VINT covering the remaining 19 bytes occupies
        // exactly 21 bytes too, overwriting it in place without shifting later elements.
        if !have_cues && let Some(entry_pos) = self.cues_seek_entry_pos {
            self.writer.seek(std::io::SeekFrom::Start(entry_pos))?;
            ebml::write_id(&mut self.writer, ebml::VOID)?;
            // 19 = 21-byte entry minus the Void ID (1) and size (1) bytes.
            ebml::write_size(&mut self.writer, 19)?;
            self.writer.write_all(&[0u8; 19])?;
        }
        self.writer.seek(std::io::SeekFrom::End(0))?;

        self.writer.flush()?;
        Ok(())
    }

    // Write a Tags master with a per-track BPS SimpleTag (bytes*8/duration),
    // mirroring mkvmerge so readers like Windows Explorer show a bitrate.
    // No-op when duration is unknown or no track carried any bytes.
    fn write_bps_tags(&mut self) -> io::Result<()> {
        if self.duration_secs <= 0.0 {
            return Ok(());
        }
        if self.track_bytes.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let tags_pos = ebml::start_master(&mut self.writer, ebml::TAGS)?;
        // Snapshot to avoid borrowing self across the writer borrow.
        let entries: Vec<(u64, u64)> = self
            .track_uids
            .iter()
            .zip(self.track_bytes.iter())
            .map(|(&uid, &bytes)| (uid, bytes))
            .collect();
        for (uid, bytes) in entries {
            if bytes == 0 {
                continue;
            }
            // bits per second = bytes * 8 / duration_secs, rounded to nearest.
            let bps = ((bytes as f64) * 8.0 / self.duration_secs).round() as u64;
            let tag_pos = ebml::start_master(&mut self.writer, ebml::TAG)?;
            // Targets → TagTrackUID (this tag applies to one track).
            let targets_pos = ebml::start_master(&mut self.writer, ebml::TARGETS)?;
            ebml::write_uint(&mut self.writer, ebml::TAG_TRACK_UID, uid)?;
            ebml::end_master(&mut self.writer, targets_pos)?;
            // SimpleTag(TagName="BPS", TagString="<bps>").
            let st_pos = ebml::start_master(&mut self.writer, ebml::SIMPLE_TAG)?;
            ebml::write_string(&mut self.writer, ebml::TAG_NAME, "BPS")?;
            ebml::write_string(&mut self.writer, ebml::TAG_STRING, &bps.to_string())?;
            ebml::end_master(&mut self.writer, st_pos)?;
            ebml::end_master(&mut self.writer, tag_pos)?;
        }
        ebml::end_master(&mut self.writer, tags_pos)?;
        Ok(())
    }

    fn start_cluster(&mut self, ts_ticks: i64) -> io::Result<()> {
        // Close previous cluster if open
        if self.cluster_open {
            self.end_cluster()?;
        }
        self.cluster_pos = self.writer.stream_position()?;
        self.cluster_size_pos = ebml::start_master(&mut self.writer, ebml::CLUSTER)?;
        ebml::write_uint(&mut self.writer, ebml::CLUSTER_TIMESTAMP, ts_ticks as u64)?;
        self.cluster_ts_ticks = ts_ticks;
        self.cluster_open = true;
        Ok(())
    }

    fn end_cluster(&mut self) -> io::Result<()> {
        if self.cluster_open {
            ebml::end_master(&mut self.writer, self.cluster_size_pos)?;
            self.cluster_open = false;
        }
        Ok(())
    }

    fn write_simple_block(
        &mut self,
        track_num: usize,
        relative_ts: i16,
        keyframe: bool,
        data: &[u8],
    ) -> io::Result<()> {
        // SimpleBlock: [track_number VINT] [relative_ts i16] [flags u8] [data]
        let (tv, tv_len) = track_vint(track_num);
        let track_vint = &tv[..tv_len];

        let flags: u8 = if keyframe { 0x80 } else { 0x00 };

        let block_size = track_vint.len() + 2 + 1 + data.len(); // vint + ts(2) + flags(1) + data
        ebml::write_id(&mut self.writer, ebml::SIMPLE_BLOCK)?;
        ebml::write_size(&mut self.writer, block_size as u64)?;
        self.writer.write_all(track_vint)?;
        self.writer.write_all(&relative_ts.to_be_bytes())?;
        self.writer.write_all(&[flags])?;
        self.writer.write_all(data)?;

        Ok(())
    }

    // Write a BlockGroup (Block + BlockDuration, plus ReferenceBlock when not a keyframe). Not
    // subtitle-only: every MPEG-2 I/P/B frame arrives here.
    fn write_block_group(
        &mut self,
        track_num: usize,
        relative_ts: i16,
        data: &[u8],
        reference: Option<i64>,
        duration_ticks: u64,
    ) -> io::Result<()> {
        let mut buf = std::mem::take(&mut self.block_group_buf);
        buf.clear();
        let res = Self::build_block_group(
            &mut buf,
            track_num,
            relative_ts,
            data,
            reference,
            Some(duration_ticks),
            None,
            0,
        )
        .and_then(|()| self.writer.write_all(&buf));
        // Hand the (now grown) scratch buffer back so the next frame reuses the
        // allocation, on the error path too.
        self.block_group_buf = buf;
        res
    }

    // Assemble a complete BlockGroup into `buf` (one write_all at the call
    // site, no seek). `additional` = Some(dependent AU) for MVC, appending
    // BlockAdditions > BlockMore { BlockAddID=2, BlockAdditional }.
    #[allow(clippy::too_many_arguments)]
    fn build_block_group(
        buf: &mut Vec<u8>,
        track_num: usize,
        relative_ts: i16,
        data: &[u8],
        reference: Option<i64>,
        duration_ticks: Option<u64>,
        additional: Option<&[u8]>,
        discard_padding_ns: i64,
    ) -> io::Result<()> {
        let (tv, tv_len) = track_vint(track_num);
        let track_vint = &tv[..tv_len];
        // The 0x80 Keyframe flag is SimpleBlock-only; inside a BlockGroup Block
        // it is reserved and MUST be 0 — keyframe-ness is signalled by the
        // presence/absence of ReferenceBlock.
        let flags: u8 = 0x00;
        let block_size = track_vint.len() + 2 + 1 + data.len();

        let bg_pos = ebml::start_master_buf(buf, ebml::BLOCK_GROUP)?;
        ebml::write_id(buf, ebml::BLOCK)?;
        ebml::write_size(buf, block_size as u64)?;
        buf.extend_from_slice(track_vint);
        buf.extend_from_slice(&relative_ts.to_be_bytes());
        buf.push(flags);
        buf.extend_from_slice(data);
        if discard_padding_ns != 0 {
            ebml::write_int(buf, ebml::DISCARD_PADDING, discard_padding_ns)?;
        }
        if let Some(dt) = duration_ticks {
            ebml::write_uint(buf, ebml::BLOCK_DURATION, dt)?;
        }
        if let Some(ref_off) = reference {
            ebml::write_int(buf, ebml::REFERENCE_BLOCK, ref_off)?;
        }
        if let Some(additional) = additional {
            let adds_pos = ebml::start_master_buf(buf, ebml::BLOCK_ADDITIONS)?;
            let more_pos = ebml::start_master_buf(buf, ebml::BLOCK_MORE)?;
            ebml::write_uint(buf, ebml::BLOCK_ADD_ID, BLOCK_ADD_ID_VALUE_MVC)?;
            ebml::write_binary(buf, ebml::BLOCK_ADDITIONAL, additional)?;
            ebml::end_master_buf(buf, more_pos)?;
            ebml::end_master_buf(buf, adds_pos)?;
        }
        ebml::end_master_buf(buf, bg_pos)?;
        Ok(())
    }

    // Write a BlockGroup: base view Block + MVC dependent AU as a BlockAdditional
    // (BlockAddID=2) per the track's mvcC mapping.
    fn write_block_group_mvc(
        &mut self,
        track_num: usize,
        relative_ts: i16,
        data: &[u8],
        additional: &[u8],
        reference: Option<i64>,
        duration_ticks: Option<u64>,
    ) -> io::Result<()> {
        let mut buf = std::mem::take(&mut self.block_group_buf);
        buf.clear();
        let res = Self::build_block_group(
            &mut buf,
            track_num,
            relative_ts,
            data,
            reference,
            duration_ticks,
            Some(additional),
            0,
        )
        .and_then(|()| self.writer.write_all(&buf));
        self.block_group_buf = buf;
        res
    }
}

// ============================================================ Helpers.
#[cfg(test)]
#[path = "mkv_tests.rs"]
mod tests;
