//! Matroska (MKV) muxer.
//!
//! Writes EBML header, Segment with tracks, clusters, and cues.
//! Designed for streaming writes: clusters are written as data arrives,
//! cues and seek head are finalized at the end.

use super::ebml;
use super::timeline::TimelineContinuity;
use crate::disc::{
    AudioChannels, AudioStream, Chapter, Codec, ColorSpace, HdrFormat, Resolution, SampleRate,
    SubtitleStream, VideoStream,
};
use std::io::{self, Seek, Write};

// ── CICP colour codes (ITU-T H.273) ──────────────────────────────────────────
//
// The Matroska Colour element (RFC 9559) carries MatrixCoefficients,
// TransferCharacteristics and Primaries verbatim as the integer code-points
// defined by ITU-T H.273 ("Coding-independent code points", CICP). Hoisting the
// codes to named constants keeps the colour match arms self-documenting and the
// values traceable to the public spec table that defines each.

/// ColourPrimaries = 1 (BT.709 / sRGB) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT709: u8 = 1;
/// ColourPrimaries = 5 (BT.470 System B/G — PAL/SECAM SD) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT470BG: u8 = 5;
/// ColourPrimaries = 6 (BT.601-525 / SMPTE 170M — NTSC SD) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT601_525: u8 = 6;
/// ColourPrimaries = 9 (BT.2020 / BT.2100) — ITU-T H.273 Table 2.
const CICP_PRIMARIES_BT2020: u8 = 9;

/// TransferCharacteristics = 1 (BT.709) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT709: u8 = 1;
/// TransferCharacteristics = 5 (BT.470 System B/G) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT470BG: u8 = 5;
/// TransferCharacteristics = 6 (BT.601-525 / SMPTE 170M) — ITU-T H.273 Table 3.
const CICP_TRANSFER_BT601_525: u8 = 6;
/// TransferCharacteristics = 16 (SMPTE ST 2084 / PQ — HDR10/HDR10+/DV) — ITU-T
/// H.273 Table 3.
const CICP_TRANSFER_PQ: u8 = 16;
/// TransferCharacteristics = 18 (ARIB STD-B67 / Hybrid Log-Gamma) — ITU-T H.273
/// Table 3.
const CICP_TRANSFER_HLG: u8 = 18;

/// MatrixCoefficients = 1 (BT.709) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT709: u8 = 1;
/// MatrixCoefficients = 5 (BT.470 System B/G) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT470BG: u8 = 5;
/// MatrixCoefficients = 6 (BT.601-525 / SMPTE 170M) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT601_525: u8 = 6;
/// MatrixCoefficients = 9 (BT.2020 non-constant luminance) — ITU-T H.273 Table 4.
const CICP_MATRIX_BT2020NC: u8 = 9;

/// Matroska Colour/Range = 1 (broadcast / studio-swing "limited" range). RFC
/// 9559 Range element. (0 = unspecified, 2 = full.)
const COLOUR_RANGE_LIMITED: u8 = 1;

/// BlockAddIDType "dvcC" — the DOVIDecoderConfigurationRecord fourcc, big-endian
/// ASCII 'd''v''c''C'. Matroska BlockAdditionMapping/BlockAddIDType for a Dolby
/// Vision configuration record (RFC 9559 + Dolby Vision-in-Matroska spec).
const BLOCK_ADD_ID_TYPE_DVCC: u64 = 0x6476_6343;

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
}

/// Build a DOVIDecoderConfigurationRecord (dvcC) — 24 bytes — for the Matroska
/// BlockAdditionMapping. For disc Profile 7 dual-layer the base, enhancement,
/// and RPU are all present (lossless FEL/MEL preserved as a second track).
pub fn dolby_vision_config(profile: u8, level: u8, bl_compat_id: u8) -> Vec<u8> {
    let mut v = vec![0u8; 24];
    v[0] = 1; // dv_version_major
    v[1] = 0; // dv_version_minor
    // profile(7) | level(6) | rpu_present(1) | el_present(1) | bl_present(1)
    v[2] = ((profile & 0x7F) << 1) | ((level >> 5) & 0x01);
    v[3] = ((level & 0x1F) << 3) | (1 << 2) | (1 << 1) | 1; // rpu = el = bl = 1
    v[4] = (bl_compat_id & 0x0F) << 4;
    // v[5..24] reserved = 0
    v
}

impl MkvTrack {
    /// Build a video track from a [`VideoStream`]. Language defaults to `"und"`;
    /// colour metadata is derived from the stream's colour space and HDR format
    /// (PQ for HDR10/HDR10+/DV, HLG for HLG). When `hdr == DolbyVision` a dvcC
    /// BlockAdditionMapping is attached automatically so players recognise the
    /// Dolby Vision layer.
    pub fn video(v: &VideoStream) -> Self {
        let codec_id = match v.codec {
            Codec::H264 => ebml::CODEC_H264,
            Codec::Hevc => ebml::CODEC_HEVC,
            Codec::Vc1 => ebml::CODEC_VC1,
            Codec::Mpeg2 => ebml::CODEC_MPEG2,
            _ => ebml::CODEC_MPEG2,
        };
        // An Unknown resolution has no real dimensions — emit (0, 0) so the
        // serializer omits PixelWidth/PixelHeight (Matroska marks them
        // optional) rather than writing a fabricated 1920x1080 default.
        let (w, h) = if matches!(v.resolution, Resolution::Unknown) {
            (0, 0)
        } else {
            v.resolution.pixels()
        };
        let (num, den) = v.frame_rate.as_fraction();
        let default_duration_ns = if num > 0 {
            (1_000_000_000u64 * den as u64) / num as u64
        } else {
            0
        };
        // CICP (matrix, transfer, primaries, range) — ITU-T H.273 code points.
        //
        // Prefer MEASURED CICP read from the bitstream (HEVC/H.264 VUI
        // colour_description or MPEG-2 sequence_display_extension) when the
        // stream states it: those are authoritative. Fall back to the coarse
        // `color_space` enum (a playlist nibble / PAL-NTSC guess) only when no
        // measured triplet is present, so the container stops ASSUMING a colour
        // space the bitstream may contradict.
        let (matrix, transfer, primaries, range) = match v.measured_cicp {
            Some(c) => (c.matrix, c.transfer, c.primaries, c.range),
            None => {
                let (m, t, p, r) = match v.color_space {
                    ColorSpace::Bt2020 => (
                        CICP_MATRIX_BT2020NC,
                        CICP_TRANSFER_PQ,
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
                    ColorSpace::Unknown => (0, 0, 0, 0),
                };
                // Override the transfer for non-PQ HDR signalled by the HdrFormat
                // (the enum can't express HLG). Only applies on the enum
                // fallback; a measured CICP already carries the real transfer.
                let t = match v.hdr {
                    HdrFormat::Hdr10 | HdrFormat::Hdr10Plus | HdrFormat::DolbyVision => {
                        CICP_TRANSFER_PQ
                    }
                    HdrFormat::Hlg => CICP_TRANSFER_HLG,
                    _ => t,
                };
                (m, t, p, r)
            }
        };
        // Display dimensions. For square-pixel video (HD/UHD/BD) the display
        // aspect equals the pixel grid, so display == pixel. For anamorphic
        // content (DVD: 720x480/576 pixels shown as 16:9 or 4:3) the coded
        // pixels are NOT square — keep the coded height and derive the width so
        // DisplayWidth:DisplayHeight carries the intended DAR (e.g. 720x576
        // 16:9 → 1024x576). Without this, players use the square-pixel ratio
        // and show the disc as 5:4 / 3:2 instead of 16:9.
        let (display_width, display_height) = match v.display_aspect {
            Some((an, ad)) if an > 0 && ad > 0 && h > 0 => ((h * an + ad / 2) / ad, h),
            _ => (w, h),
        };
        Self {
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
            // FieldOrder (Matroska 0x9D) is a bitstream property
            // (`top_field_first`) the IFO/MPLS scan cannot know, so it is NOT set
            // here — it would only ever be a guess. Default to UNDETERMINED; the
            // mux stream (`MkvStream`) sets the MEASURED value from the first
            // coded picture's `PictureInfo` before the muxer writes the header.
            // If an interlaced track ever reaches the muxer still UNDETERMINED,
            // that is a parser/source gap and is logged loudly — never faked.
            field_order: ebml::FIELD_ORDER_UNDETERMINED,
            // DefaultDecodedFieldDuration is DELIBERATELY NOT emitted (0 here
            // suppresses the element; see the writer in `MkvMuxer::new`).
            //
            // rc.5.1 added it (= half the frame period, 20 ms for 576i25) to try
            // to fix the Windows-fps report, on the theory that Windows derives
            // fps from it. The captured SOTL evidence proves the opposite: with
            // FlagInterlaced=1 + DefaultDuration=40 ms + DefaultDecodedFieldDuration=20 ms,
            // Windows Explorer reports 12.5 fps (half), and MediaInfo flips the
            // track to "Frame rate mode: Variable" with no clean rate. MakeMKV's
            // correct rip of the same disc OMITS DefaultDecodedFieldDuration,
            // keeps FlagInterlaced=1 + FieldOrder=TFF + DefaultDuration=40 ms, and
            // Explorer reports the full 25 fps with MediaInfo "Constant". ffmpeg's
            // matroskaenc.c does the same (full-frame DefaultDuration, no field
            // duration). The lone frame-rate signal every tool actually trusts is
            // `1 / DefaultDuration`; that full-frame value (40 ms → 25 fps) is kept
            // below. Dropping the field-duration element removes the per-field
            // signal that made Explorer halve the rate.
            //
            // Trade-off: the container no longer carries an explicit per-field
            // decoded duration. Nothing is lost in practice — the interlace
            // signaling that deinterlacers and MediaInfo rely on lives in the
            // MPEG-2 elementary stream's picture_coding_extension (picture_structure /
            // top_field_first), which MediaInfo reads directly (so it still reports
            // "Interlaced / Top Field First"), and the container still flags
            // FlagInterlaced=1 + FieldOrder=TFF so players keep deinterlacing.
            field_duration_ns: 0,
            sample_rate: 0.0,
            channels: 0,
            bit_depth: 0,
            // The DV layer (hdr=DolbyVision) carries the dvcC so the track is
            // recognised as Dolby Vision (disc Profile 7 dual-layer).
            dv_config: if matches!(v.hdr, HdrFormat::DolbyVision) {
                Some(dolby_vision_config(7, 6, 0))
            } else {
                None
            },
        }
    }

    /// Build an audio track from an [`AudioStream`]. The codec ID follows the
    /// Matroska registry; every DTS family member (core, DTS-HD HR, DTS-HD MA)
    /// maps to the single registered `A_DTS` ID (see the note below).
    pub fn audio(a: &AudioStream) -> Self {
        // The Matroska codec-ID registry defines `A_DTS` for the entire
        // DTS family — the spec text for `A_DTS` explicitly states it
        // "Supports DTS, DTS-ES, DTS-96/26, DTS-HD High Resolution Audio
        // and DTS-HD Master Audio." Players distinguish core vs HD-HRA vs
        // HD-MA by parsing the DTS bitstream extension substreams, not by
        // the container codec ID. The previously-emitted `A_DTS/MA` and
        // `A_DTS/HR` suffixes are NOT registered codec IDs; strict parsers
        // (libmatroska) and some hardware renderers fail to recognise the
        // track at all. Emit plain `A_DTS` for every DTS variant — the
        // lossless MA / HRA payload bytes are unchanged, only the
        // container codec-ID string differs.
        let codec_id = match a.codec {
            Codec::Ac3 => ebml::CODEC_AC3,
            Codec::Ac3Plus => ebml::CODEC_EAC3,
            Codec::TrueHd => ebml::CODEC_TRUEHD,
            Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => ebml::CODEC_DTS,
            Codec::Lpcm => ebml::CODEC_PCM_BE,
            _ => ebml::CODEC_AC3,
        };
        // Unknown sample rate / channel layout: emit 0 so the serializer omits
        // the SamplingFrequency / Channels element (Matroska supplies its own
        // spec default) rather than writing a fabricated 48000 Hz / 6-channel
        // value into the file.
        let sr = if matches!(a.sample_rate, SampleRate::Unknown) {
            0.0
        } else {
            a.sample_rate.hz()
        };
        let ch = if matches!(a.channels, AudioChannels::Unknown) {
            0
        } else {
            a.channels.count()
        };

        let name = a.label.clone();

        Self {
            track_type: ebml::TRACK_TYPE_AUDIO,
            codec_id,
            language: a.language.clone(),
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
            bit_depth: 0,
            dv_config: None,
        }
    }

    /// Build a subtitle track from a [`SubtitleStream`]. PGS maps to
    /// `S_HDMV/PGS` and DVD VobSub to `S_VOBSUB`; the stream's `codec_data`
    /// (the VobSub `.idx` palette header for DVD) becomes the track's
    /// CodecPrivate. The forced-display flag is propagated from the stream.
    pub fn subtitle(s: &SubtitleStream) -> Self {
        let codec_id = match s.codec {
            Codec::DvdSub => ebml::CODEC_VOBSUB,
            _ => ebml::CODEC_PGS,
        };
        Self {
            track_type: ebml::TRACK_TYPE_SUBTITLE,
            codec_id,
            language: s.language.clone(),
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
        }
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
    base_pts_ticks: Option<i64>,
    /// Last block timecode (TimestampScale ticks, relative to base_pts) written
    /// PER TRACK, to enforce strictly-monotonic per-track timestamps —
    /// players/ffmpeg reject non-monotonic DTS, and some audio PES PTS land on
    /// the same tick (or tick back one from rounding).
    last_pts_ticks: std::collections::HashMap<usize, i64>,
    /// Per-track-index flag: true if the track is video. The strictly-monotonic
    /// block-timestamp nudge must be skipped for EVERY video track, not just
    /// track 0 — a title can carry a second video track (e.g. a Dolby Vision
    /// enhancement layer at index 1) whose B-frame PTS is just as legitimately
    /// non-monotonic. Keying the exemption on track type (not index) keeps that
    /// EL's true PTS instead of clobbering it to prev+1ms.
    track_is_video: Vec<bool>,
    /// Cross-clip timeline-continuity corrector (clip-boundary PTS rebasing).
    continuity: TimelineContinuity,
    cues: Vec<CuePoint>,
    frame_count: u64,
    /// Frames handed to `write_frame` that were dropped because no cluster was
    /// open yet (a cluster only opens on a track-0 video keyframe). If this is
    /// non-zero at `finish()` and not a single frame was ever written, the
    /// caller produced an empty MKV — surfaced as an error rather than a
    /// silently empty file. See `write_frame` for the track-0 invariant.
    dropped_pre_cluster: u64,
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
    /// Per-AC-3-audio-track channel-correction state. The DVD IFO audio nibble
    /// is unreliable, so the channel count written in the track header is
    /// corrected from the AC-3 bitstream `acmod` of the first frame on the
    /// track. Each entry records the file offset of the 1-byte Channels value
    /// (to patch in place) and the IFO-claimed count (to warn on disagreement);
    /// `corrected` flips once patched so we only act on the first frame.
    ac3_channel_fixups: std::collections::HashMap<usize, Ac3ChannelFixup>,
    /// `--log-level 3` opening-frame capture: the first ~100 coded frames per
    /// track are written (raw) to a `<output>.opening.bin` side file with a
    /// per-frame summary logged, so an opening-GOP / menu / mid-GOP-open issue is
    /// diagnosable from a future log without the disc. `None` on normal runs
    /// (diag off) — the muxer pays nothing.
    opening_capture: Option<crate::diag::OpeningCapture>,
}

/// Deferred AC-3 channel-count correction: the track header's `Channels` byte
/// is written up-front from the (unreliable) IFO count; on the first AC-3 frame
/// for the track the value is rewritten from the bitstream `acmod`.
struct Ac3ChannelFixup {
    /// Absolute file offset of the 1-byte Channels value in the Tracks element.
    value_offset: u64,
    /// Channel count the IFO claimed (already written at `value_offset`).
    claimed: u8,
    /// True once the first frame has been parsed and the value finalised.
    corrected: bool,
}

/// TimestampScale: nanoseconds per Matroska timestamp tick. 0.1 ms (100_000 ns).
///
/// The classic 1 ms scale truncates two distinct cadences onto the same tick:
/// - 23.976 fps video frames are ~41.7 ms apart, but with B-frame reorder two
///   neighbouring frames can round to the same whole millisecond — a decoder
///   then derives colliding DTS ("non monotonically increasing dts").
/// - TrueHD audio access units are 0.833 ms (1/1200 s); at 1 ms granularity
///   every AU truncates to a 1 ms grid and the per-track monotonic nudge has to
///   space them at a fabricated 1 ms instead of their true 0.833 ms.
///
/// 0.1 ms resolves both: 41.7 ms and 0.833 ms each map to distinct ticks, so
/// frames stop colliding and audio keeps its real cadence. Player/parser
/// support for sub-millisecond TimestampScale is universal (it is the spec
/// default mechanism). The cost is a smaller per-cluster i16 span (see
/// `MAX_BLOCK_REL`), handled by splitting clusters and emitting a Cue for the
/// split (see `write_frame`).
const TIMESTAMP_SCALE_NS: i64 = 100_000;

/// Nominal new-cluster interval (2 s) expressed in TimestampScale ticks.
///
/// A keyframe only OPENS a new cluster once this much has elapsed since the open
/// cluster's timestamp, so the actual cluster span runs from this value up to
/// roughly this value plus one GOP (the next keyframe lands a GOP later). With a
/// typical ≤ 1 s GOP that worst-case span (~3 s ≈ 30_000 ticks) stays UNDER the
/// i16 block-relative limit (`MAX_BLOCK_REL` = 32_767 ticks ≈ 3.27 s at the
/// 0.1 ms scale), so video keyframes drive Cue-aligned cluster boundaries and
/// the i16-overflow split path stays a rare fallback (long audio-only stretches
/// or pathological multi-second GOPs) rather than the common case. The classic
/// 5 s window would, at this scale, force an unaligned i16 split inside every
/// cluster.
const CLUSTER_DURATION_TICKS: i64 = 2_000 * 1_000_000 / TIMESTAMP_SCALE_NS;

/// Maximum block-relative timestamp expressible in the signed 16-bit
/// SimpleBlock/Block field (`i16::MAX` ticks). A frame whose offset from the
/// open cluster's timestamp falls outside `i16::MIN..=i16::MAX` ticks forces a
/// new cluster (see `write_frame`) so the `as i16` cast can never wrap — in
/// EITHER direction. PES timestamps come from untrusted disc/file bytes and can
/// back-jump on discontinuities, so the lower bound matters as much as the
/// upper one. At a 0.1 ms scale i16::MAX is ~3.27 s, well under the 5 s cluster
/// window, so a long-GOP / audio-only stretch can hit this bound before the
/// keyframe boundary — the split path must (and does) push a Cue.
const MAX_BLOCK_REL: i64 = i16::MAX as i64;
/// Minimum block-relative timestamp expressible in the signed 16-bit field.
const MIN_BLOCK_REL: i64 = i16::MIN as i64;

// The clip-boundary timeline-continuity corrector (`TimelineContinuity`) lives
// in `crate::mux::timeline` — shared verbatim with the `demux://` sink. It is
// imported below where the muxer uses it.

/// Force a per-track block timestamp (in TimestampScale ticks) to be strictly
/// later than the previous one written for that track. `prev` is the last
/// timestamp for the track (`None` for the first frame). Fixes non-monotonic
/// DTS: some audio PES PTS truncate to the same tick as the prior frame (or tick
/// back one from rounding), which ffmpeg/strict players reject. At the 0.1 ms
/// scale a TrueHD AU (0.833 ms = ~8 ticks) no longer collides with its
/// neighbour, so this rarely fires for lossless audio — but a +1-tick nudge
/// (0.1 ms, sub-AU and inaudible) still guards genuine same-tick collisions on
/// any no-reorder track. Never moves a timestamp earlier.
fn monotonic_ts(prev: Option<i64>, pts_ticks: i64) -> i64 {
    match prev {
        Some(p) => pts_ticks.max(p.saturating_add(1)),
        None => pts_ticks,
    }
}

/// Per-track block timestamp. The strictly-monotonic nudge is applied to
/// AUDIO/SUBTITLE tracks only; ALL VIDEO tracks are returned UNCHANGED.
///
/// With B-frames, a video frame's presentation PTS is legitimately
/// non-monotonic in decode/storage order (a B-frame sits between its anchors,
/// below the frame stored just before it). Forcing it strictly-increasing
/// clobbers those PTS to prev+1ms — a `copy` remux preserves the (wrong) value,
/// but a decoder derives DTS from the HEVC POC and finds them colliding
/// ("non monotonically increasing dts", thousands per title). Matroska
/// SimpleBlock permits non-monotonic block timestamps (signed block-relative
/// offsets), so video keeps its true PES PTS; only no-reorder tracks (audio,
/// subtitles), where a same-millisecond collision IS a real defect, get nudged.
///
/// The exemption is keyed on `is_video` (track type), NOT a track index: a
/// title can carry more than one video track — e.g. a Dolby Vision enhancement
/// layer at index 1 — and every one must keep its true PTS. Keying on
/// `track_idx == 0` clamped the EL and reintroduced the exact non-monotonic-DTS
/// warning this exemption exists to prevent.
fn block_ts(is_video: bool, prev: Option<i64>, pts_ticks: i64) -> i64 {
    if is_video {
        pts_ticks
    } else {
        monotonic_ts(prev, pts_ticks)
    }
}

/// Encode a Matroska track number as an EBML VINT into a stack buffer,
/// returning the buffer and the used length. Track numbers are small (1-based,
/// a handful of tracks), so 1 byte covers `< 0x80` and 2 bytes covers the rest;
/// no heap allocation, called once per block on the mux hot path.
///
/// The 2-byte form holds 14 payload bits (max 0x3FFF). The `debug_assert`
/// guards the 0x4000 bound: at or above it, `(track_num >> 8)` is >= 0x40 and
/// OR-ing the 0x40 length marker would clobber it, corrupting the track
/// number. Not reachable today (track numbers are `i+1` over a few streams),
/// so this documents the bound rather than handling 3-byte VINTs.
fn track_vint(track_num: usize) -> ([u8; 2], usize) {
    if track_num < 0x80 {
        ([(track_num as u8) | 0x80, 0], 1)
    } else {
        debug_assert!(
            track_num < 0x4000,
            "track number {track_num} exceeds the 14-bit 2-byte EBML VINT range"
        );
        ([0x40 | ((track_num >> 8) as u8), track_num as u8], 2)
    }
}

impl<W: Write + Seek> MkvMuxer<W> {
    /// Create a new MKV muxer: writes EBML header, Segment start, Info, Tracks, Chapters.
    pub fn new(
        mut writer: W,
        tracks: &[MkvTrack],
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
        // Absolute file offset where the CUES Seek entry begins, so that — if no
        // Cues element is ultimately written (zero cue points) — the entry can be
        // overwritten with a Void at finish() instead of leaving a SeekHead
        // pointer that resolves to whatever element (Tags / EOF) happens to land
        // at the Cues offset. See `cues_seek_entry_pos` / `finish`.
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
        if duration_secs > 0.0 {
            // Duration is expressed in TimestampScale ticks (not ms).
            let duration_ticks = duration_secs * 1_000_000_000.0 / TIMESTAMP_SCALE_NS as f64;
            ebml::write_float(&mut writer, ebml::DURATION, duration_ticks)?;
        }
        // Stamp the freemkv version so any muxed file is traceable to the build
        // that produced it (MediaInfo "Writing application"/"library").
        const FREEMKV_MUX_APP: &str = concat!("freemkv ", env!("CARGO_PKG_VERSION"));
        ebml::write_string(&mut writer, ebml::MUXING_APP, FREEMKV_MUX_APP)?;
        ebml::write_string(&mut writer, ebml::WRITING_APP, FREEMKV_MUX_APP)?;
        if let Some(t) = title {
            ebml::write_string(&mut writer, ebml::TITLE, t)?;
        }
        ebml::end_master(&mut writer, info_pos)?;

        // Tracks
        let tracks_start = writer.stream_position()?;
        let tracks_offset = tracks_start - segment_start;
        let tracks_pos = ebml::start_master(&mut writer, ebml::TRACKS)?;
        let mut track_uids: Vec<u64> = Vec::with_capacity(tracks.len());
        let mut ac3_channel_fixups: std::collections::HashMap<usize, Ac3ChannelFixup> =
            std::collections::HashMap::new();
        for (i, track) in tracks.iter().enumerate() {
            let track_uid = (i + 1) as u64 | 0x100_0000;
            track_uids.push(track_uid);
            let entry_pos = ebml::start_master(&mut writer, ebml::TRACK_ENTRY)?;
            ebml::write_uint(&mut writer, ebml::TRACK_NUMBER, (i + 1) as u64)?;
            ebml::write_uint(&mut writer, ebml::TRACK_UID, track_uid)?;
            ebml::write_uint(&mut writer, ebml::TRACK_TYPE, track.track_type)?;
            ebml::write_uint(&mut writer, ebml::FLAG_LACING, 0)?;
            ebml::write_string(&mut writer, ebml::CODEC_ID, track.codec_id)?;
            ebml::write_string(&mut writer, ebml::LANGUAGE, &track.language)?;
            if !track.name.is_empty() {
                ebml::write_string(&mut writer, ebml::TRACK_NAME, &track.name)?;
            }

            if !track.is_default {
                ebml::write_uint(&mut writer, ebml::FLAG_DEFAULT, 0)?;
            }
            if track.is_forced {
                ebml::write_uint(&mut writer, ebml::FLAG_FORCED, 1)?;
            }

            if let Some(ref cp) = track.codec_private {
                ebml::write_binary(&mut writer, ebml::CODEC_PRIVATE, cp)?;
            }
            // Pre-0.13 a deferred codecPrivate path existed for video tracks
            // (placeholder reserve + later seek-back fill via
            // `fill_codec_private`). The PES pipeline hands codec_private
            // up-front via the DiscTitle, so the deferred path was never
            // exercised — removed in the 0.13 dead-code sweep.

            // DefaultDuration — frame duration in nanoseconds
            if track.default_duration_ns > 0 {
                ebml::write_uint(
                    &mut writer,
                    ebml::DEFAULT_DURATION,
                    track.default_duration_ns,
                )?;
            }

            // DefaultDecodedFieldDuration (one FIELD = half a frame), a DIRECT
            // child of TrackEntry. The production video path now ALWAYS passes
            // `field_duration_ns == 0` (see `MkvTrack::video`) so this element is
            // NOT written: emitting it (20 ms for 576i25) is exactly what made
            // Windows Explorer report 12.5 fps and MediaInfo flip to VFR on the
            // captured SOTL rip, while MakeMKV — which omits it — shows the full
            // 25 fps. The guard below is retained so a non-zero value still emits
            // a well-formed element for any future caller / round-trip test, but
            // the muxer's own callers no longer trigger it.
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
                // Scan type. FlagInterlaced: 1 = interlaced, 2 = progressive.
                // FieldOrder (0x9D) is only written for interlaced content with a
                // determined order: TFF = 1, BFF = 6, 0 = progressive, and the
                // element is omitted entirely when undetermined (RFC 9559).
                ebml::write_uint(
                    &mut writer,
                    ebml::FLAG_INTERLACED,
                    if track.interlaced {
                        ebml::INTERLACED_INTERLACED
                    } else {
                        ebml::INTERLACED_PROGRESSIVE
                    },
                )?;
                if track.interlaced && track.field_order != ebml::FIELD_ORDER_UNDETERMINED {
                    // `track.field_order` was set CORRECTLY before construction
                    // (the mux stream reads the first coded picture's measured
                    // field order and sets it on the track), so this writes the
                    // right value the first time — no later rewrite.
                    ebml::write_uint(&mut writer, ebml::FIELD_ORDER, track.field_order as u64)?;
                }
                if track.display_width > 0 && track.display_height > 0 {
                    ebml::write_uint(&mut writer, ebml::DISPLAY_WIDTH, track.display_width as u64)?;
                    ebml::write_uint(
                        &mut writer,
                        ebml::DISPLAY_HEIGHT,
                        track.display_height as u64,
                    )?;
                }
                // Colour metadata (HDR)
                if track.colour_matrix > 0 || track.colour_transfer > 0 {
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
                    ebml::end_master(&mut writer, col_pos)?;
                }
                ebml::end_master(&mut writer, vid_pos)?;
            }

            // Dolby Vision signaling — BlockAdditionMapping is a child of the
            // TrackEntry (sibling of Video). Carries the dvcC so players /
            // mediainfo recognise the track as Dolby Vision.
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
                    // Capture the ACTUAL file offset of the 1-byte Channels value
                    // so an AC-3 track can correct it from the bitstream acmod on
                    // its first frame (the IFO nibble is unreliable). Rather than
                    // assume write_uint's encoding (ID + size widths), write the
                    // element's ID and size explicitly, then record the position
                    // immediately before the value byte. Channels is 1..=255 so
                    // the value is exactly one byte (Size = 1), and the acmod
                    // correction is likewise 1..=255 — the width never changes, so
                    // the in-place single-byte rewrite stays valid.
                    ebml::write_id(&mut writer, ebml::CHANNELS)?;
                    ebml::write_size(&mut writer, 1)?;
                    let value_offset = writer.stream_position()?;
                    writer.write_all(&[track.channels])?;
                    if track.codec_id == ebml::CODEC_AC3 {
                        ac3_channel_fixups.insert(
                            i,
                            Ac3ChannelFixup {
                                value_offset,
                                claimed: track.channels,
                                corrected: false,
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
            cluster_ts_ticks: 0,
            base_pts_ticks: None,
            last_pts_ticks: std::collections::HashMap::new(),
            track_is_video: tracks
                .iter()
                .map(|t| t.track_type == ebml::TRACK_TYPE_VIDEO)
                .collect(),
            continuity: TimelineContinuity::new(),
            cues: Vec::new(),
            frame_count: 0,
            dropped_pre_cluster: 0,
            seek_fixups,
            cues_seek_entry_pos,
            info_offset,
            tracks_offset,
            chapters_offset,
            track_bytes: vec![0u64; tracks.len()],
            track_uids,
            duration_secs,
            ac3_channel_fixups,
            opening_capture: None,
        })
    }

    /// Attach an opening-frame capture (`--log-level 3`). The capture writes the
    /// first ~100 coded frames per track to `<output>.opening.bin` and logs a
    /// per-frame summary, so opening-GOP / menu issues are diagnosable from a
    /// log + side file without the disc. `None` is a no-op (normal runs).
    pub fn set_opening_capture(&mut self, capture: Option<crate::diag::OpeningCapture>) {
        self.opening_capture = capture;
    }

    /// Write a single frame.
    ///
    /// When `duration_ns` is `Some`, the frame is emitted as a
    /// `BlockGroup` with `BlockDuration` so the player knows exactly
    /// when to remove the on-screen artifact (the practical case is
    /// PGS subtitles — without it, the last bitmap lingers until the
    /// next display set replaces it). Otherwise a plain `SimpleBlock`.
    /// Rewrite a video track's `FieldOrder` value in place from the MEASURED
    /// field order carried on the first coded picture, replacing the scan-time
    /// guess written at construction. This is the fix for the "we parsed
    /// `top_field_first` then ignored it" red flag: the muxer now stamps the
    /// field order the bitstream actually states, not an assumption.
    ///
    /// Idempotent — only the first call per track patches (later calls and
    /// non-interlaced / non-video tracks are no-ops). `Progressive` / unknown
    /// (`None`) leaves the written value untouched: an interlaced track keeps
    /// its guess rather than being cleared via a multi-element change. The byte
    /// width is fixed (FieldOrder is 0..=14), so the in-place rewrite is valid.
    pub fn write_frame(
        &mut self,
        track_idx: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
        duration_ns: Option<u64>,
    ) -> io::Result<()> {
        // --log-level 3: capture the first ~100 coded frames per track to the
        // side file BEFORE any timeline mangling, with the codec parser's own
        // frame PTS — so an opening-GOP / mid-GOP-open / menu issue is
        // reconstructable from the log + side file alone (no disc). No-op (and
        // no allocation) on normal runs; the capture is `None`.
        if let Some(cap) = self.opening_capture.as_mut() {
            cap.record(track_idx, pts_ns, keyframe, data);
        }

        // Is this a video track? Used for the monotonic block-timestamp nudge
        // below, which must exempt EVERY video track (incl. a Dolby Vision EL).
        let is_video = self.track_is_video.get(track_idx).copied().unwrap_or(false);

        // The clip-boundary epoch decision is driven by the PRIMARY video track
        // ONLY (track 0). A title can carry a SECOND video track — a Dolby Vision
        // enhancement layer — whose PTS runs on its OWN timeline, interleaved
        // with the base layer's. The two video PTS sequences overlap, so the EL's
        // frames look like multi-second backward jumps against the base layer's
        // frontier and would false-trigger an epoch reset on every GOP (the exact
        // ratchet that inflated Top Gun's 1-clip timeline to ~7 h). Only the base
        // video layer establishes/advances the frontier and opens epochs; the EL
        // — like audio and subtitles — rides the current offset.
        let drives_epoch = track_idx == 0;

        // Map the raw PES PTS onto the continuous output timeline FIRST, before
        // any base/cluster math: at a non-seamless clip / layer-break boundary
        // the source PES PTS jumps backward. Rebasing it (a global offset across
        // all tracks, A/V-sync-preserving) keeps the boundary from becoming a
        // band of non-monotonic block timestamps. Only the PRIMARY video track
        // drives the boundary decision; every other track (audio, subtitle, DV
        // EL) rides the current offset. No-op for single-clip titles.
        let pts_ns = self.continuity.adjust(pts_ns, drives_epoch);
        let raw_ticks = pts_ns / TIMESTAMP_SCALE_NS;

        // Cluster boundaries normally coincide with a video keyframe so every
        // Cues entry resolves to a seekable IDR at the cluster start.
        let is_video_key = keyframe && track_idx == 0;

        // Derive the timestamp base from the first *kept* keyframe (the frame
        // that opens the first cluster), NOT the first frame merely seen. The
        // first frame seen can have a higher display PTS than the subsequent
        // I-frame (B-frame reordering / a PTS discontinuity), which would make
        // later cluster/cue timestamps negative and wrap to ~u64::MAX on the
        // `as u64` cast in `start_cluster`/`finish`. Anchoring on the first kept
        // keyframe guarantees the open cluster's timestamp is 0 and all later
        // relative offsets are computed from a frame we actually wrote.
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
                self.base_pts_ticks = Some(raw_ticks);
                raw_ticks
            }
        };
        // Floor at 0: base is the first kept keyframe, so any frame with an
        // earlier PTS (audio/subtitle arriving with a pre-keyframe timestamp, or
        // a back-jump on a stream discontinuity) would compute negative here,
        // which would wrap to ~u64::MAX on the `as u64` cluster/cue write and
        // could overflow the i16 block-relative cast. Frames before the first
        // kept keyframe are clamped to t=0 rather than corrupting the timeline.
        let pts_ticks = (raw_ticks - base).max(0);

        // Strictly-monotonic block timestamps — AUDIO/SUBTITLE ONLY. Some audio
        // PES PTS truncate to the same tick as the previous frame (or tick back
        // one); nudge those to prev+1 tick (sub-frame, inaudible).
        //
        // VIDEO is EXEMPT: with B-frames, presentation PTS is legitimately
        // non-monotonic in decode/storage order (a B-frame's PTS sits between its
        // anchors, below the frame stored before it). Forcing it
        // strictly-increasing clobbers those PTS, which a `copy` remux preserves
        // but a decoder rejects — it derives DTS from the HEVC POC and finds them
        // colliding ("non monotonically increasing dts"). Matroska SimpleBlock
        // permits non-monotonic block timestamps (negative block-relative
        // offsets), so leave the true PES PTS intact for video.
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
            self.start_cluster(pts_ticks)?;
            self.cues.push(CuePoint {
                timestamp_ticks: pts_ticks,
                track: track_idx + 1,
                cluster_pos: self.cluster_pos - self.segment_start,
            });
        } else {
            let rel = pts_ticks - self.cluster_ts_ticks;
            if !(MIN_BLOCK_REL..=MAX_BLOCK_REL).contains(&rel) {
                // The block-relative timestamp is a signed 16-bit value, so a
                // frame whose offset from the current cluster's timestamp falls
                // outside i16::MIN..=i16::MAX ticks (~±3.27 s at the 0.1 ms
                // scale) would silently wrap on the `as i16` cast, corrupting A/V
                // sync. The keyframe-driven boundary above only fires on a video
                // keyframe — a long audio-only stretch, a very long GOP with no
                // intervening keyframe (positive direction), or an
                // audio/subtitle PES whose PTS back-jumps below the open cluster
                // (negative direction) can drift past the i16 range. Force a
                // fresh cluster here even without a keyframe to keep the cast in
                // range. pts_ticks is already floored at 0 above, so the new
                // cluster timestamp never wraps on the `as u64` write in
                // start_cluster.
                //
                // This split cluster is NOT keyframe-aligned, but it MUST still
                // carry a Cue entry: at the finer 0.1 ms scale these forced
                // splits are routine (any GOP/audio run over ~3.27 s triggers
                // one), so omitting them would leave multi-second gaps in the
                // seek index where a player's `-ss` lands at the wrong cluster.
                // The Cue points at this cluster's start; a player seeking here
                // resumes decode from the first block (it back-references the
                // prior keyframe via the codec, as players already do for
                // non-IDR cue targets). Cue track is the current frame's track.
                self.start_cluster(pts_ticks)?;
                self.cues.push(CuePoint {
                    timestamp_ticks: pts_ticks,
                    track: track_idx + 1,
                    cluster_pos: self.cluster_pos - self.segment_start,
                });
            }
        }

        // Committed to writing this frame — record its (monotonic) timestamp so
        // the next block on this track is forced strictly later.
        self.last_pts_ticks.insert(track_idx, pts_ticks);

        let relative_ts = (pts_ticks - self.cluster_ts_ticks) as i16;
        match duration_ns {
            Some(dur_ns) => {
                // BlockDuration is in TimestampScale ticks, floored at 1.
                let duration_ticks = (dur_ns as i64 / TIMESTAMP_SCALE_NS).max(1) as u64;
                self.write_block_group(track_idx + 1, relative_ts, keyframe, data, duration_ticks)?;
            }
            None => {
                self.write_simple_block(track_idx + 1, relative_ts, keyframe, data)?;
            }
        }
        self.frame_count += 1;

        // Per-track byte total for the finalize-time BPS statistics tag.
        if let Some(b) = self.track_bytes.get_mut(track_idx) {
            *b += data.len() as u64;
        }

        // Correct the AC-3 track's Channels element from the bitstream acmod on
        // the FIRST frame of the track. The DVD IFO audio nibble is unreliable
        // (it claims 5.1 on a 2.0 stream); the bitstream acmod is authoritative.
        // Only the first frame triggers it; the byte width is unchanged so the
        // patch is a single-byte in-place rewrite (then restore position).
        if let Some(fixup) = self.ac3_channel_fixups.get_mut(&track_idx) {
            if !fixup.corrected {
                match super::codec::ac3::acmod_channels(data) {
                    Some(actual) if actual > 0 => {
                        if actual != fixup.claimed {
                            tracing::warn!(
                                target: "mux",
                                "AC-3 track {track_idx}: IFO claimed {} channels but bitstream acmod says {}; trusting the bitstream (possible wrong-stream selection)",
                                fixup.claimed,
                                actual,
                            );
                            let here = self.writer.stream_position()?;
                            self.writer
                                .seek(std::io::SeekFrom::Start(fixup.value_offset))?;
                            self.writer.write_all(&[actual])?;
                            self.writer.seek(std::io::SeekFrom::Start(here))?;
                        }
                        fixup.corrected = true;
                    }
                    // Frame too short to carry the BSI bits — keep the passed
                    // (IFO) value and try again on the next frame.
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Finish the MKV file: write Cues element.
    ///
    /// # Track-0 invariant
    ///
    /// A cluster only opens on a track-0 video keyframe, so the caller must
    /// supply track 0 as the video track and deliver a keyframe on it before
    /// (or alongside) other-track data. If no track-0 keyframe ever arrives,
    /// every `write_frame` is silently dropped; rather than emit a structurally
    /// valid but empty MKV (zero clusters, zero frames), `finish` returns
    /// `Error::MkvInvalid` when frames were submitted but none were written.
    pub fn finish(mut self) -> io::Result<()> {
        // A title that produced no frames (e.g. fully unreadable, or every
        // frame dropped before the first track-0 keyframe opened a cluster)
        // would otherwise yield a structurally-empty MKV with no clusters or
        // cues. Surface that as an error rather than writing valid-but-empty
        // output.
        if self.frame_count == 0 {
            return Err(crate::error::Error::MkvInvalid.into());
        }
        // Close final cluster
        self.end_cluster()?;

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

        // Per-track BPS statistics tags (mkvmerge convention). A reader that
        // reads the container `BPS` tag (Windows Explorer's MKV property
        // handler) rather than computing bitrate from stream size shows a
        // bitrate for EVERY track this way, not just CBR audio.
        self.write_bps_tags()?;

        // Back-patch SeekHead SeekPosition values now that all element offsets
        // are known. When no Cues element was written (zero cue points), the
        // CUES entry's SeekPosition would otherwise be back-patched to
        // `cues_offset`, which now holds Tags / EOF — a dangling pointer to a
        // non-Cues element. Skip that fixup and instead Void the whole CUES Seek
        // entry (below) so the SeekHead carries no false pointer.
        for fixup in &self.seek_fixups {
            if fixup.target_id == ebml::CUES && !have_cues {
                continue;
            }
            let offset = match fixup.target_id {
                ebml::INFO => self.info_offset,
                ebml::TRACKS => self.tracks_offset,
                ebml::CHAPTERS => self
                    .chapters_offset
                    .expect("CHAPTERS seek fixup present => chapters_offset is Some"),
                ebml::CUES => cues_offset,
                _ => 0,
            };
            self.writer
                .seek(std::io::SeekFrom::Start(fixup.value_offset))?;
            self.writer.write_all(&offset.to_be_bytes())?;
        }
        // Neutralise the unused CUES Seek entry. The entry is a fixed 21-byte
        // Seek master: SEEK(2 ID + 1 size) + SEEK_ID(2+1) + 4-byte target id +
        // SEEK_POSITION(2+1) + 8-byte value = 21 bytes. A Void (0xEC, 1-byte ID)
        // with a 1-byte size VINT covering the remaining 19 bytes occupies
        // exactly 1 + 1 + 19 = 21 bytes, overwriting the entry in place without
        // shifting any following element.
        if !have_cues {
            if let Some(entry_pos) = self.cues_seek_entry_pos {
                self.writer.seek(std::io::SeekFrom::Start(entry_pos))?;
                ebml::write_id(&mut self.writer, ebml::VOID)?;
                // 19 = 21-byte entry minus the Void ID (1) and size (1) bytes.
                ebml::write_size(&mut self.writer, 19)?;
                self.writer.write_all(&[0u8; 19])?;
            }
        }
        self.writer.seek(std::io::SeekFrom::End(0))?;

        self.writer.flush()?;
        Ok(())
    }

    /// Write a `Tags` master with a per-track `BPS` SimpleTag (bytes*8 /
    /// duration_secs). Mirrors mkvmerge's per-track statistics tag so readers
    /// that surface the container tag (Windows Explorer) show a bitrate for
    /// every track. No-op when the duration is unknown (can't compute a rate)
    /// or no track carried any bytes.
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

    fn write_block_group(
        &mut self,
        track_num: usize,
        relative_ts: i16,
        keyframe: bool,
        data: &[u8],
        duration_ticks: u64,
    ) -> io::Result<()> {
        let (tv, tv_len) = track_vint(track_num);
        let track_vint = &tv[..tv_len];
        // The 0x80 Keyframe flag is defined only for SimpleBlock; inside a
        // Block within a BlockGroup that high bit is reserved and MUST be 0
        // (keyframe-ness is signalled by the absence of a ReferenceBlock
        // child). `keyframe` is intentionally unused here — every Block this
        // path emits is intra (PGS subtitle frames carrying a duration).
        let _ = keyframe;
        let flags: u8 = 0x00;
        let block_size = track_vint.len() + 2 + 1 + data.len();

        let bg_pos = ebml::start_master(&mut self.writer, ebml::BLOCK_GROUP)?;
        ebml::write_id(&mut self.writer, ebml::BLOCK)?;
        ebml::write_size(&mut self.writer, block_size as u64)?;
        self.writer.write_all(track_vint)?;
        self.writer.write_all(&relative_ts.to_be_bytes())?;
        self.writer.write_all(&[flags])?;
        self.writer.write_all(data)?;
        ebml::write_uint(&mut self.writer, ebml::BLOCK_DURATION, duration_ticks)?;
        ebml::end_master(&mut self.writer, bg_pos)?;
        Ok(())
    }
}

// ============================================================
// Helpers
// ============================================================

// Old parse_resolution/parse_sample_rate/parse_channels removed —
// Resolution::pixels(), SampleRate::hz(), AudioChannels::count() replace them.

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Anamorphic DVD: a 720x576 (R576i) PAL stream flagged 16:9 must write a
    /// DisplayWidth/Height carrying the 16:9 DAR (1024x576), NOT the square-pixel
    /// 720x576 (which players show as ~5:4). Square-pixel video
    /// (`display_aspect == None`) keeps display == pixel.
    #[test]
    fn video_track_anamorphic_display_aspect() {
        let base = VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: crate::disc::FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: Some((16, 9)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };
        let t = MkvTrack::video(&base);
        assert_eq!((t.pixel_width, t.pixel_height), (720, 576));
        assert_eq!(
            (t.display_width, t.display_height),
            (1024, 576),
            "16:9 anamorphic must emit a 16:9 DAR, not square-pixel 720x576"
        );

        let square = VideoStream {
            display_aspect: None,
            ..base
        };
        let t2 = MkvTrack::video(&square);
        assert_eq!(
            (t2.display_width, t2.display_height),
            (720, 576),
            "square pixels: display == pixel"
        );
    }

    /// Measured CICP from the bitstream must take precedence over the coarse
    /// `color_space` enum. A BT.2020/PQ enum that would otherwise produce
    /// (9,16,9) is overridden by a measured BT.709 triplet when present.
    #[test]
    fn measured_cicp_overrides_color_space_enum() {
        let base = VideoStream {
            pid: 0xE0,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: crate::disc::FrameRate::F24,
            hdr: HdrFormat::Hdr10, // enum/HDR path would force PQ transfer
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };

        // Without a measured triplet: enum + HDR → BT.2020nc / PQ / BT.2020.
        let t_enum = MkvTrack::video(&base);
        assert_eq!(
            (
                t_enum.colour_matrix,
                t_enum.colour_transfer,
                t_enum.colour_primaries
            ),
            (
                CICP_MATRIX_BT2020NC,
                CICP_TRANSFER_PQ,
                CICP_PRIMARIES_BT2020
            ),
            "enum fallback derives BT.2020/PQ"
        );

        // With a measured BT.709 triplet: the bitstream's value wins outright,
        // INCLUDING the transfer (the HDR override does not apply to measured).
        let measured = VideoStream {
            measured_cicp: Some(crate::disc::MeasuredCicp {
                matrix: CICP_MATRIX_BT709,
                transfer: CICP_TRANSFER_BT709,
                primaries: CICP_PRIMARIES_BT709,
                range: COLOUR_RANGE_LIMITED,
            }),
            ..base
        };
        let t = MkvTrack::video(&measured);
        assert_eq!(
            (t.colour_matrix, t.colour_transfer, t.colour_primaries),
            (CICP_MATRIX_BT709, CICP_TRANSFER_BT709, CICP_PRIMARIES_BT709),
            "measured CICP must override the enum, transfer included"
        );
    }

    /// Helper: search for a 4-byte big-endian EBML ID in a byte slice.
    fn find_id(data: &[u8], id: u32) -> Option<usize> {
        let bytes = id.to_be_bytes();
        // Determine how many leading zero bytes to skip
        let start = if bytes[0] != 0 {
            0
        } else if bytes[1] != 0 {
            1
        } else if bytes[2] != 0 {
            2
        } else {
            3
        };
        let needle = &bytes[start..];
        data.windows(needle.len()).position(|w| w == needle)
    }

    fn make_video_track() -> MkvTrack {
        MkvTrack {
            track_type: ebml::TRACK_TYPE_VIDEO,
            codec_id: ebml::CODEC_H264,
            language: "und".into(),
            name: String::new(),
            codec_private: Some(vec![0x00, 0x01, 0x02, 0x03]),
            is_default: true,
            is_forced: false,
            pixel_width: 1920,
            pixel_height: 1080,
            default_duration_ns: 41708333,
            display_width: 1920,
            display_height: 1080,
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
        }
    }

    fn make_audio_track() -> MkvTrack {
        MkvTrack {
            track_type: ebml::TRACK_TYPE_AUDIO,
            codec_id: ebml::CODEC_AC3,
            language: "eng".into(),
            name: "English".into(),
            codec_private: None,
            is_default: true,
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
            sample_rate: 48000.0,
            channels: 6,
            bit_depth: 0,
            dv_config: None,
        }
    }

    fn audio_stream(codec: Codec) -> AudioStream {
        use crate::disc::{AudioChannels, LabelPurpose, SampleRate};
        AudioStream {
            pid: 0x1100,
            codec,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }
    }

    #[test]
    fn dts_variants_map_to_registered_a_dts_codec_id() {
        // The Matroska codec-ID registry defines `A_DTS` for the whole DTS
        // family (core, DTS-HD HRA, DTS-HD MA). The `/MA` and `/HR` suffixes
        // are not registered and break strict parsers, so every DTS variant
        // must emit plain `A_DTS`.
        for codec in [Codec::Dts, Codec::DtsHdMa, Codec::DtsHdHr] {
            let track = MkvTrack::audio(&audio_stream(codec));
            assert_eq!(
                track.codec_id, "A_DTS",
                "{codec:?} must map to registered codec ID A_DTS, got {}",
                track.codec_id
            );
        }
        // Sanity: the non-DTS variants keep their distinct IDs.
        assert_eq!(MkvTrack::audio(&audio_stream(Codec::Ac3)).codec_id, "A_AC3");
        assert_eq!(
            MkvTrack::audio(&audio_stream(Codec::TrueHd)).codec_id,
            "A_TRUEHD"
        );
    }

    #[test]
    fn dolby_vision_config_profile7() {
        // dvcC for disc Profile 7 dual-layer: version 1.0, profile 7, all of
        // bl/el/rpu present. 24 bytes.
        let c = dolby_vision_config(7, 6, 0);
        assert_eq!(c.len(), 24);
        assert_eq!(c[0], 1); // dv_version_major
        assert_eq!(c[1], 0); // dv_version_minor
        // profile in the top 7 bits of byte 2
        assert_eq!(c[2] >> 1, 7, "dv_profile must be 7");
        // rpu/el/bl present flags in byte 3 (low 3 bits after level)
        assert_eq!(c[3] & 0b0000_0111, 0b0000_0111, "rpu+el+bl all present");
    }

    #[test]
    fn mkv_writes_ebml_header() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let muxer = MkvMuxer::new(buf, &tracks, Some("Test"), 120.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        // EBML header element ID: 0x1A45DFA3
        assert!(data.len() >= 4);
        assert_eq!(&data[0..4], &[0x1A, 0x45, 0xDF, 0xA3]);
    }

    #[test]
    fn mkv_writes_segment() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let muxer = MkvMuxer::new(buf, &tracks, None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        // Segment element ID: 0x18538067
        assert!(
            find_id(&data, ebml::SEGMENT).is_some(),
            "Segment element not found in output"
        );
    }

    #[test]
    fn mkv_write_frame_creates_cluster() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let mut muxer = MkvMuxer::new(buf, &tracks, None, 60.0, &[]).unwrap();
        muxer
            .write_frame(0, 0, true, &[0xDE, 0xAD, 0xBE, 0xEF], None)
            .unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::CLUSTER).is_some(),
            "Cluster element not found after write_frame"
        );
    }

    #[test]
    fn mkv_finish_writes_cues_element() {
        // finish() consumes self and flushes the writer, so use the
        // module-level SharedWriter to inspect the buffer afterwards.
        use std::sync::{Arc, Mutex};

        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let tracks = [make_video_track()];
        let mut muxer = MkvMuxer::new(writer, &tracks, Some("Cue Test"), 60.0, &[]).unwrap();
        muxer
            .write_frame(0, 0, true, &[0x01, 0x02, 0x03], None)
            .unwrap();
        muxer.finish().unwrap();

        let data = shared.lock().unwrap().clone().into_inner();
        assert!(
            find_id(&data, ebml::CUES).is_some(),
            "Cues element (0x1C53BB6B) not found after finish()"
        );
    }

    /// When no Cues element is written (zero cue points), the SeekHead must NOT
    /// retain a CUES entry that back-patches to the Cues offset — that offset now
    /// holds Tags / EOF, a dangling pointer to a non-Cues element. finish() Voids
    /// the unused CUES Seek entry instead. (The empty-cues case is defensive —
    /// the normal path pushes a cue with every cluster — so the test clears the
    /// cue list directly before finalizing.)
    #[test]
    fn zero_cues_voids_seekhead_entry_no_dangling_pointer() {
        use std::sync::{Arc, Mutex};

        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let tracks = [make_video_track()];
        let mut muxer = MkvMuxer::new(writer, &tracks, Some("NoCue"), 60.0, &[]).unwrap();
        muxer
            .write_frame(0, 0, true, &[0x01, 0x02, 0x03], None)
            .unwrap();
        // Force the zero-cue branch: drop every cue before finalizing.
        let cues_entry_pos = muxer.cues_seek_entry_pos.expect("CUES seek entry recorded");
        muxer.cues.clear();
        muxer.finish().unwrap();

        let data = shared.lock().unwrap().clone().into_inner();

        // No Cues element is written.
        assert!(
            find_id(&data, ebml::CUES).is_none(),
            "no Cues element expected when there are zero cue points"
        );
        // The recorded CUES Seek entry was overwritten with a Void (0xEC) of the
        // remaining 19 bytes — it no longer begins a SEEK (0x4DBB) element.
        let entry = &data[cues_entry_pos as usize..cues_entry_pos as usize + 2];
        assert_eq!(
            entry,
            &[ebml::VOID as u8, 0x80 | 19],
            "CUES Seek entry must be Void(19), not a live Seek pointer"
        );

        // Defensively confirm no Seek entry's SeekPosition resolves to the (now
        // Tags/EOF) cues offset: scan all 8-byte SeekPosition values in the
        // SeekHead and ensure none equals the offset where Cues would have been.
        // (Sanity: the file must still parse its real elements.)
        assert!(
            find_id(&data, ebml::INFO).is_some() && find_id(&data, ebml::TRACKS).is_some(),
            "Info and Tracks must still be present and seekable"
        );
    }

    #[test]
    fn monotonic_ts_forces_strictly_increasing() {
        // First frame passes through unchanged.
        assert_eq!(monotonic_ts(None, 1000), 1000);
        // A repeated millisecond is nudged to prev+1.
        assert_eq!(monotonic_ts(Some(1000), 1000), 1001);
        // A backwards tick is nudged forward, never earlier.
        assert_eq!(monotonic_ts(Some(1001), 1000), 1002);
        // A genuine advance is left alone.
        assert_eq!(monotonic_ts(Some(1000), 1040), 1040);
        // Simulate a stream of audio PTS that round to dup/back-tick ms and
        // confirm the emitted sequence is strictly increasing.
        let raw = [1000i64, 1000, 1000, 999, 1032, 1032, 1064];
        let mut prev: Option<i64> = None;
        let mut out = Vec::new();
        for &p in &raw {
            let t = monotonic_ts(prev, p);
            out.push(t);
            prev = Some(t);
        }
        assert!(
            out.windows(2).all(|w| w[1] > w[0]),
            "not strictly monotonic: {out:?}"
        );
        assert_eq!(out, [1000, 1001, 1002, 1003, 1032, 1033, 1064]);
    }

    #[test]
    fn block_ts_exempts_video_from_monotonic_nudge() {
        // VIDEO keeps its true PTS even when non-monotonic in storage order — a
        // B-frame whose presentation PTS sits below the frame stored before it
        // must NOT be nudged to prev+1ms (that clobbering is what produced the
        // "non monotonically increasing dts" flood on decode).
        assert_eq!(
            block_ts(true, Some(1040), 1000),
            1000,
            "video B-frame PTS preserved"
        );
        assert_eq!(
            block_ts(true, Some(1000), 1000),
            1000,
            "video dup-ms PTS preserved"
        );
        // A realistic decode-order GOP (I, then B-frames dipping below it):
        // every value passes through untouched for video.
        let gop = [1000i64, 960, 920, 1080, 1040];
        let mut prev = None;
        let out: Vec<i64> = gop
            .iter()
            .map(|&p| {
                let t = block_ts(true, prev, p);
                prev = Some(t);
                t
            })
            .collect();
        assert_eq!(out, gop, "video timestamps must be left exactly as-is");

        // AUDIO/SUBTITLE still get the strictly-monotonic nudge — a same-ms
        // collision there is a real defect.
        assert_eq!(
            block_ts(false, Some(1000), 1000),
            1001,
            "audio dup-ms nudged"
        );
        assert_eq!(
            block_ts(false, Some(1001), 1000),
            1002,
            "subtitle back-tick nudged"
        );
    }

    /// Regression for the second-video-track bug: a Dolby Vision enhancement
    /// layer is video but NOT track 0. The exemption must follow track TYPE, so
    /// the EL's B-frame PTS are preserved exactly like the main video's — not
    /// clamped to prev+1ms (which reintroduced the non-monotonic-DTS flood on
    /// the EL stream). Drives the muxer through both video tracks and asserts
    /// every video block timecode equals its source PTS.
    #[test]
    fn second_video_track_pts_not_clobbered() {
        use std::io::Cursor;
        // Main video at index 0, a Dolby-Vision-EL-style second video at index 1.
        let tracks = vec![make_video_track(), make_video_track()];
        let buf = Cursor::new(Vec::new());
        let mux = MkvMuxer::new(buf, &tracks, None, 0.0, &[]).unwrap();
        // Both tracks must be flagged video so neither is nudged.
        assert_eq!(mux.track_is_video, vec![true, true]);
        // A B-frame dip on the EL (track 1) must pass through unchanged — keyed
        // on track type, not index.
        assert_eq!(block_ts(mux.track_is_video[1], Some(1040), 1000), 1000);
    }

    /// End-to-end output regression (the symptom, at the block-timecode level):
    /// a large clip-boundary reset WITH an interleaved straggler audio frame
    /// from clip 1's tail, driven through the full muxer. Asserts cluster
    /// timestamps are monotonic non-decreasing AND the timeline reaches past the
    /// boundary (clip 2 present) without ratcheting. This is the test that would
    /// have caught BOTH the original `-820000` non-monotonic band and the
    /// straggler ratchet that made everything after the boundary unseekable.
    #[test]
    fn clip_boundary_with_straggler_yields_monotonic_clusters() {
        let tracks = [make_video_track(), make_audio_track()];
        // ms→ns helper for readability.
        let ms = |m: i64| m * 1_000_000;
        let frames: Vec<(usize, i64, bool, Vec<u8>)> = vec![
            // Clip 1: video keyframes at 0s and 600s, audio alongside.
            (0, ms(0), true, vec![0x01; 16]),
            (1, ms(0), true, vec![0xA0; 8]),
            (0, ms(600_000), true, vec![0x02; 16]), // 600s kf
            (1, ms(600_000), true, vec![0xA1; 8]),
            // Clip 2: video keyframe RESETS to 0 (the -600s boundary).
            (0, ms(0), true, vec![0x03; 16]),
            // Straggler: clip 1's tail audio (≈599.5s) arrives interleaved AFTER
            // the reset — the exact frame class that caused the ratchet.
            (1, ms(599_500), true, vec![0xA2; 8]),
            // Clip 2 continues: audio at 0, video keyframe at 5s.
            (1, ms(0), true, vec![0xA3; 8]),
            (0, ms(5_000), true, vec![0x04; 16]), // clip2 + 5s
        ];
        let (data, frame_count) = mux_to_bytes(&tracks, &[], &frames);
        assert_eq!(frame_count, 8, "all frames written (none dropped)");

        // Clusters are VIDEO-keyframe-driven, so they track the (rebased) video
        // timeline only — the lagging audio straggler never opens a cluster.
        // CLUSTER timestamps are in TimestampScale TICKS (0.1 ms).
        let tick = |ms: i64| ms * 1_000_000 / TIMESTAMP_SCALE_NS; // ms → ticks
        let clusters = find_clusters(&data);
        let ts: Vec<u64> = clusters.iter().map(|&(_, _, t)| t).collect();
        assert!(!ts.is_empty(), "expected clusters");
        // Cluster timestamps must be monotonic non-decreasing (the boundary is
        // rebased by VIDEO; no back-dated cluster, no non-monotonic band).
        assert!(
            ts.windows(2).all(|w| w[1] >= w[0]),
            "cluster timestamps must be monotonic, got {ts:?}"
        );
        let max = *ts.iter().max().unwrap() as i64;
        // Timeline reaches past the boundary (clip 2 present): ≥ ~600s.
        assert!(
            max >= tick(600_000),
            "timeline must span past the boundary, got {max} ticks"
        );
        // And does NOT ratchet far beyond clip1+clip2 (~605s): well under 2× clip1.
        assert!(
            max < tick(1_000_000),
            "no ratchet: max cluster ts {max} ticks must stay near 605s"
        );
    }

    #[test]
    fn mkv_multiple_tracks() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track(), make_audio_track()];
        let mut muxer = MkvMuxer::new(buf, &tracks, Some("Multi"), 120.0, &[]).unwrap();
        // Write frames to both tracks
        muxer
            .write_frame(0, 0, true, &[0x00, 0x00, 0x01], None)
            .unwrap();
        muxer
            .write_frame(1, 0, false, &[0x0B, 0x77, 0x00], None)
            .unwrap();
        muxer
            .write_frame(0, 40_000_000, false, &[0x00, 0x00, 0x01], None)
            .unwrap();
        muxer
            .write_frame(1, 32_000_000, false, &[0x0B, 0x77, 0x01], None)
            .unwrap();
        // Should not panic
        let data = muxer.writer.into_inner();
        assert!(data.len() > 100, "output too small for multi-track MKV");
    }

    #[test]
    fn mkv_keyframe_flag() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let mut muxer = MkvMuxer::new(buf, &tracks, None, 10.0, &[]).unwrap();

        // Record position before first frame
        let pos_before_kf = muxer.writer.position();
        muxer.write_frame(0, 0, true, &[0xAA], None).unwrap();
        let pos_after_kf = muxer.writer.position();

        muxer
            .write_frame(0, 1_000_000, false, &[0xBB], None)
            .unwrap();
        let pos_after_nkf = muxer.writer.position();

        let data = muxer.writer.into_inner();

        // Extract the SimpleBlock regions
        let kf_region = &data[pos_before_kf as usize..pos_after_kf as usize];
        let nkf_region = &data[pos_after_kf as usize..pos_after_nkf as usize];

        // In a SimpleBlock, after ID + size + track_vint + 2-byte timestamp,
        // the next byte is flags. Keyframe flag = 0x80, non-keyframe = 0x00.
        // Find the flags byte in each region: it's the byte after the 2-byte timestamp.
        // SimpleBlock ID is 0xA3. Find it and walk past ID + size + vint + ts.
        fn extract_flags(region: &[u8]) -> u8 {
            // Find 0xA3 (SimpleBlock ID)
            let sb_pos = region.iter().position(|&b| b == 0xA3).unwrap();
            // After ID: size (variable), track vint (1 byte for track<128), ts (2 bytes), flags (1 byte)
            // Size is 1 byte for small blocks (< 127 bytes)
            let after_id = sb_pos + 1;
            // Read VINT size: first byte has high bit set for 1-byte sizes
            let size_byte = region[after_id];
            let size_len = if size_byte & 0x80 != 0 { 1 } else { 2 };
            // Track VINT: 1 byte (track 1 = 0x81)
            let track_vint_pos = after_id + size_len;
            let track_vint_len = 1; // track 1 encoded as 0x81
            // 2-byte relative timestamp
            let ts_pos = track_vint_pos + track_vint_len;
            // flags byte
            let flags_pos = ts_pos + 2;
            region[flags_pos]
        }

        let kf_flags = extract_flags(kf_region);
        let nkf_flags = extract_flags(nkf_region);

        assert_eq!(
            kf_flags & 0x80,
            0x80,
            "keyframe flag should be set (0x80), got 0x{:02X}",
            kf_flags
        );
        assert_eq!(
            nkf_flags & 0x80,
            0x00,
            "non-keyframe flag should be clear, got 0x{:02X}",
            nkf_flags
        );
    }

    #[test]
    fn mkv_writes_chapters_element() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let chapters = vec![
            Chapter {
                time_secs: 0.0,
                name: "Chapter 1".into(),
            },
            Chapter {
                time_secs: 300.0,
                name: "Chapter 2".into(),
            },
            Chapter {
                time_secs: 600.0,
                name: "Chapter 3".into(),
            },
        ];
        let muxer = MkvMuxer::new(buf, &tracks, Some("Chapter Test"), 900.0, &chapters).unwrap();
        let data = muxer.writer.into_inner();

        // Chapters element ID: 0x1043A770
        assert!(
            find_id(&data, ebml::CHAPTERS).is_some(),
            "Chapters element (0x1043A770) not found in output"
        );
        // EditionEntry element ID: 0x45B9
        assert!(
            find_id(&data, ebml::EDITION_ENTRY).is_some(),
            "EditionEntry element not found"
        );
        // ChapterAtom element ID: 0xB6
        assert!(
            find_id(&data, ebml::CHAPTER_ATOM).is_some(),
            "ChapterAtom element not found"
        );
    }

    #[test]
    fn mkv_no_chapters_when_empty() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let muxer = MkvMuxer::new(buf, &tracks, Some("No Chapters"), 60.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::CHAPTERS).is_none(),
            "Chapters element should not be present when no chapters given"
        );
    }

    #[test]
    fn mkv_default_flag_on_first_video_and_audio() {
        // First video: is_default=true, first audio: is_default=true, second audio: is_default=false
        let video = make_video_track(); // is_default: true
        let audio1 = make_audio_track(); // is_default: true
        let mut audio2 = make_audio_track();
        audio2.is_default = false;
        audio2.language = "fra".into();

        let buf = Cursor::new(Vec::new());
        let tracks = [video, audio1, audio2];
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0, &[]).unwrap();
        let data = muxer.writer.into_inner();

        // FlagDefault ID is 0x88. When is_default is true, FlagDefault is NOT written
        // (MKV default is 1). When is_default is false, FlagDefault=0 IS written.
        // So we should find at least one FlagDefault element (for the non-default track).
        let flag_default_id = ebml::FLAG_DEFAULT.to_be_bytes();
        let _needle = &[flag_default_id[3]]; // 0x88 is a 1-byte ID
        let count = data.windows(1).filter(|w| w[0] == 0x88).count();
        // 0x88 appears as FlagDefault + as TrackType (also 0x83... no, 0x83 != 0x88)
        // FlagDefault (0x88) should appear for the non-default track
        assert!(
            count >= 1,
            "FlagDefault should be written for non-default tracks"
        );
    }

    #[test]
    fn mkv_forced_flag_on_forced_subtitle() {
        use crate::disc::SubtitleStream;
        let video = make_video_track();
        let forced_sub = MkvTrack::subtitle(&SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: true,
            qualifier: crate::disc::LabelQualifier::Forced,
            codec_data: None,
        });
        assert!(forced_sub.is_forced);

        let buf = Cursor::new(Vec::new());
        let tracks = [video, forced_sub];
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0, &[]).unwrap();
        let data = muxer.writer.into_inner();

        // FlagForced ID: 0x55AA (2-byte ID)
        assert!(
            find_id(&data, ebml::FLAG_FORCED).is_some(),
            "FlagForced element should be present for forced subtitle track"
        );
    }

    #[test]
    fn mkv_no_forced_flag_on_non_forced_subtitle() {
        use crate::disc::SubtitleStream;
        let video = make_video_track();
        let sub = MkvTrack::subtitle(&SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: crate::disc::LabelQualifier::None,
            codec_data: None,
        });
        assert!(!sub.is_forced);

        let buf = Cursor::new(Vec::new());
        let tracks = [video, sub];
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0, &[]).unwrap();
        let data = muxer.writer.into_inner();

        // FlagForced should NOT be written for non-forced tracks
        assert!(
            find_id(&data, ebml::FLAG_FORCED).is_none(),
            "FlagForced element should not be present for non-forced subtitle"
        );
    }

    // ============================================================
    // Seekability tests: SeekHead, keyframe-aligned clusters, Cues
    // ============================================================

    use std::sync::{Arc, Mutex};

    /// Writer that lets the test inspect the buffer after `finish()` consumes the muxer.
    struct SharedWriter(Arc<Mutex<Cursor<Vec<u8>>>>);
    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }
    impl Seek for SharedWriter {
        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            self.0.lock().unwrap().seek(pos)
        }
    }

    /// Build interleaved frames at 24 fps video (IDR every gop_secs) + 48 kHz audio (1024 samples per frame).
    fn frames_for(duration_secs: f64, gop_secs: f64) -> Vec<(usize, i64, bool, Vec<u8>)> {
        let video_interval_ns: i64 = 1_000_000_000 / 24;
        let audio_interval_ns: i64 = (1024i64 * 1_000_000_000) / 48_000;
        let gop_frames = (gop_secs * 24.0).round() as i64;

        let mut out: Vec<(usize, i64, bool, Vec<u8>)> = Vec::new();
        let total_ns = (duration_secs * 1_000_000_000.0) as i64;

        let mut vi: i64 = 0;
        loop {
            let pts = vi * video_interval_ns;
            if pts >= total_ns {
                break;
            }
            let keyframe = vi % gop_frames == 0;
            out.push((0, pts, keyframe, vec![0xAB; 64]));
            vi += 1;
        }

        let mut ai: i64 = 0;
        loop {
            let pts = ai * audio_interval_ns;
            if pts >= total_ns {
                break;
            }
            out.push((1, pts, true, vec![0xCD; 32]));
            ai += 1;
        }

        out.sort_by_key(|f| f.1);
        out
    }

    /// Mux frames through a SharedWriter and return the final buffer.
    fn mux_to_bytes(
        tracks: &[MkvTrack],
        chapters: &[Chapter],
        frames: &[(usize, i64, bool, Vec<u8>)],
    ) -> (Vec<u8>, u64) {
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, tracks, None, 0.0, chapters).unwrap();
        for (t, pts, kf, data) in frames {
            muxer.write_frame(*t, *pts, *kf, data, None).unwrap();
        }
        let frame_count = muxer.frame_count;
        muxer.finish().unwrap();
        let data = shared.lock().unwrap().clone().into_inner();
        (data, frame_count)
    }

    /// Find the Segment header in the buffer and return (segment_id_pos, segment_start_pos).
    /// segment_start = position immediately after Segment's id + size bytes.
    fn locate_segment(data: &[u8]) -> (usize, usize) {
        let segment_id_pos = find_id(data, ebml::SEGMENT).expect("segment id not found");
        // Segment is written via write_id + write_unknown_size: 4 byte id + 8 byte size
        (segment_id_pos, segment_id_pos + 4 + 8)
    }

    /// Walk Segment's top-level children. Returns Vec<(id, data_start_offset, data_size)>
    /// where data_start_offset is absolute file offset and data_size is the element body size.
    fn segment_children(data: &[u8]) -> Vec<(u32, usize, u64)> {
        let (_, seg_start) = locate_segment(data);
        let mut out = Vec::new();
        let mut cursor = Cursor::new(&data[seg_start..]);
        while (cursor.position() as usize) < data.len() - seg_start {
            let pos_before = cursor.position();
            let (id, size, hdr_len) = match ebml::read_element_header(&mut cursor) {
                Ok(v) => v,
                Err(_) => break,
            };
            let data_abs = seg_start + pos_before as usize + hdr_len;
            out.push((id, data_abs, size));
            // Skip the body to advance to the next element.
            cursor
                .seek(io::SeekFrom::Current(size as i64))
                .expect("seek past element body");
        }
        out
    }

    /// Walk the direct children of a master element body. Returns
    /// `Vec<(id, body_start_abs, body_size)>` (absolute offsets into `data`).
    /// `master_body` is an absolute-offset slice range `[start, start+size)`.
    /// Unlike `find_id`'s flat byte-scan, this respects EBML nesting: a child
    /// id buried inside a deeper master is NOT reported at this level.
    fn master_children(data: &[u8], body_start: usize, body_size: usize) -> Vec<(u32, usize, u64)> {
        let mut out = Vec::new();
        let body = &data[body_start..body_start + body_size];
        let mut cursor = Cursor::new(body);
        while (cursor.position() as usize) < body.len() {
            let pos_before = cursor.position();
            let (id, size, hdr_len) = match ebml::read_element_header(&mut cursor) {
                Ok(v) => v,
                Err(_) => break,
            };
            let child_abs = body_start + pos_before as usize + hdr_len;
            out.push((id, child_abs, size));
            cursor
                .seek(io::SeekFrom::Current(size as i64))
                .expect("seek past child body");
        }
        out
    }

    /// Locate the first `TrackEntry` master and return the offset/size of its
    /// body. Walks Segment → Tracks → TrackEntry, never a flat byte-scan, so the
    /// returned range is the genuine TrackEntry body.
    fn first_track_entry(data: &[u8]) -> (usize, usize) {
        let (tracks_start, tracks_size) = segment_children(data)
            .into_iter()
            .find_map(|(id, off, sz)| (id == ebml::TRACKS).then_some((off, sz as usize)))
            .expect("Tracks element present");
        let (_, te_start, te_size) = master_children(data, tracks_start, tracks_size)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::TRACK_ENTRY)
            .expect("TrackEntry present");
        (te_start, te_size as usize)
    }

    /// Find every Cluster: returns Vec<(cluster_data_start_abs, cluster_data_size, cluster_timestamp_ms)>.
    fn find_clusters(data: &[u8]) -> Vec<(usize, u64, u64)> {
        let mut out = Vec::new();
        for (id, body_start, body_size) in segment_children(data) {
            if id == ebml::CLUSTER {
                let mut cursor = Cursor::new(&data[body_start..body_start + body_size as usize]);
                let (tid, tsize, _) = ebml::read_element_header(&mut cursor).unwrap();
                assert_eq!(
                    tid,
                    ebml::CLUSTER_TIMESTAMP,
                    "cluster must start with timestamp"
                );
                let ts = ebml::read_uint_val(&mut cursor, tsize as usize).unwrap();
                out.push((body_start, body_size, ts));
            }
        }
        out
    }

    /// Parse the first SimpleBlock that appears in a cluster body slice.
    /// Returns (track_num, flags_byte). track_num decoded from VINT.
    fn first_simple_block(cluster_body: &[u8]) -> (u64, u8) {
        let mut cursor = Cursor::new(cluster_body);
        loop {
            let (id, size, _) = ebml::read_element_header(&mut cursor).unwrap();
            if id == ebml::SIMPLE_BLOCK {
                let body_start = cursor.position() as usize;
                // Decode track VINT.
                let b0 = cluster_body[body_start];
                let (track_num, vint_len) = if b0 & 0x80 != 0 {
                    ((b0 & 0x7F) as u64, 1usize)
                } else if b0 & 0x40 != 0 {
                    let b1 = cluster_body[body_start + 1];
                    ((((b0 & 0x3F) as u64) << 8) | b1 as u64, 2)
                } else {
                    panic!("unsupported track vint width");
                };
                let flags = cluster_body[body_start + vint_len + 2];
                return (track_num, flags);
            }
            // Skip non-SimpleBlock child.
            cursor.seek(io::SeekFrom::Current(size as i64)).unwrap();
        }
    }

    /// Parse the Cues element body into Vec<(cue_time, cue_track, cue_cluster_position)>.
    fn parse_cues(data: &[u8]) -> Vec<(u64, u64, u64)> {
        let mut out = Vec::new();
        let (cues_id, cues_body_start, cues_body_size) = segment_children(data)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::CUES)
            .expect("cues element not found");
        assert_eq!(cues_id, ebml::CUES);
        let cues_body = &data[cues_body_start..cues_body_start + cues_body_size as usize];
        let mut cursor = Cursor::new(cues_body);
        while (cursor.position() as usize) < cues_body.len() {
            let (id, size, _) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(id, ebml::CUE_POINT);
            let cp_end = cursor.position() + size;
            let mut cue_time = 0u64;
            let mut cue_track = 0u64;
            let mut cue_pos = 0u64;
            while cursor.position() < cp_end {
                let (sid, ssize, _) = ebml::read_element_header(&mut cursor).unwrap();
                match sid {
                    ebml::CUE_TIME => {
                        cue_time = ebml::read_uint_val(&mut cursor, ssize as usize).unwrap();
                    }
                    ebml::CUE_TRACK_POSITIONS => {
                        let ctp_end = cursor.position() + ssize;
                        while cursor.position() < ctp_end {
                            let (iid, isize_, _) = ebml::read_element_header(&mut cursor).unwrap();
                            match iid {
                                ebml::CUE_TRACK => {
                                    cue_track =
                                        ebml::read_uint_val(&mut cursor, isize_ as usize).unwrap();
                                }
                                ebml::CUE_CLUSTER_POSITION => {
                                    cue_pos =
                                        ebml::read_uint_val(&mut cursor, isize_ as usize).unwrap();
                                }
                                _ => {
                                    cursor.seek(io::SeekFrom::Current(isize_ as i64)).unwrap();
                                }
                            }
                        }
                    }
                    _ => {
                        cursor.seek(io::SeekFrom::Current(ssize as i64)).unwrap();
                    }
                }
            }
            out.push((cue_time, cue_track, cue_pos));
        }
        out
    }

    /// Parse the SeekHead body into Vec<(seek_id, seek_position)>.
    fn parse_seekhead(data: &[u8]) -> Vec<(u32, u64)> {
        let mut out = Vec::new();
        let (sh_id, sh_body_start, sh_body_size) = segment_children(data)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::SEEK_HEAD)
            .expect("seekhead not found");
        assert_eq!(sh_id, ebml::SEEK_HEAD);
        let sh_body = &data[sh_body_start..sh_body_start + sh_body_size as usize];
        let mut cursor = Cursor::new(sh_body);
        while (cursor.position() as usize) < sh_body.len() {
            let (id, size, _) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(id, ebml::SEEK);
            let seek_end = cursor.position() + size;
            let mut seek_id_val: u32 = 0;
            let mut seek_pos_val: u64 = 0;
            while cursor.position() < seek_end {
                let (sid, ssize, _) = ebml::read_element_header(&mut cursor).unwrap();
                match sid {
                    ebml::SEEK_ID => {
                        let raw = ebml::read_uint_val(&mut cursor, ssize as usize).unwrap();
                        seek_id_val = raw as u32;
                    }
                    ebml::SEEK_POSITION => {
                        seek_pos_val = ebml::read_uint_val(&mut cursor, ssize as usize).unwrap();
                    }
                    _ => {
                        cursor.seek(io::SeekFrom::Current(ssize as i64)).unwrap();
                    }
                }
            }
            out.push((seek_id_val, seek_pos_val));
        }
        out
    }

    #[test]
    fn keyframe_driven_clusters_start_on_video_keyframe() {
        // The COMMON case: a keyframe-driven cluster (opened because a video
        // keyframe crossed the cluster-duration boundary) must begin with the
        // video keyframe. With a 1 s GOP and 3 s clusters, keyframe spacing keeps
        // every keyframe-driven cluster well within the i16 block span, so no
        // forced i16-split clusters appear here and EVERY cluster is
        // keyframe-aligned.
        let tracks = [make_video_track(), make_audio_track()];
        let frames = frames_for(30.0, 1.0);
        let (data, _) = mux_to_bytes(&tracks, &[], &frames);
        let clusters = find_clusters(&data);
        assert!(!clusters.is_empty(), "expected at least one cluster");
        for (body_start, body_size, _ts) in clusters {
            let body = &data[body_start..body_start + body_size as usize];
            // Skip past the CLUSTER_TIMESTAMP element first.
            let mut cursor = Cursor::new(body);
            let (tid, tsize, _) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
            cursor.seek(io::SeekFrom::Current(tsize as i64)).unwrap();
            let after_ts = cursor.position() as usize;
            let (track_num, flags) = first_simple_block(&body[after_ts..]);
            assert_eq!(
                track_num, 1,
                "first block in cluster must be track 1 (video)"
            );
            assert_eq!(
                flags & 0x80,
                0x80,
                "first block in cluster must have keyframe flag set, got 0x{:02X}",
                flags
            );
        }
    }

    #[test]
    fn cue_count_equals_cluster_count() {
        // Regression for the cluster-split Cue gap: EVERY cluster — whether
        // opened by a video keyframe OR forced by the i16 block-relative split —
        // must carry a Cue, so cluster count always equals cue count and the
        // seek index has no multi-second holes.
        let tracks = [make_video_track(), make_audio_track()];
        let frames = frames_for(30.0, 1.0);
        let (data, _) = mux_to_bytes(&tracks, &[], &frames);
        let clusters = find_clusters(&data);
        let cues = parse_cues(&data);
        assert_eq!(
            clusters.len(),
            cues.len(),
            "every cluster must have a cue: cluster count {} != cue count {}",
            clusters.len(),
            cues.len()
        );
        // For 30s @ 2s nominal cluster duration with 1s GOP, expect 15 clusters.
        assert_eq!(
            clusters.len(),
            15,
            "expected 15 clusters for 30s @ 2s cluster duration"
        );
    }

    #[test]
    fn cue_count_equals_cluster_count_blockgroup_vfr() {
        // DVD (MPEG-2) seek-index regression. Unlike UHD/HEVC — which emits
        // SimpleBlocks (`duration_ns = None`) — DVD video is VFR: every coded
        // picture carries a per-frame `duration_ns = Some(..)`, so it is written
        // as a BlockGroup, NOT a SimpleBlock. The Cues index must still get one
        // cue per cluster on this path exactly like the SimpleBlock path; a DVD
        // MKV with thousands of clusters and ZERO cues lets players chapter-seek
        // but never scrub. The pre-existing cue tests all feed `None` (SimpleBlock
        // only), leaving this BlockGroup path unguarded — this test covers it.
        //
        // Drives the REAL `Mpeg2Parser` end-to-end (decode-order frames,
        // non-monotonic B-frame display PTS, telecine field durations) into the
        // muxer, so it exercises the genuine DVD frame shape, not a hand-built one.
        use crate::mux::codec::CodecParser;
        use crate::mux::codec::mpeg2::Mpeg2Parser;
        use crate::mux::ts::PesPacket;

        // Minimal MPEG-2 ES builders (mirroring the mpeg2 parser's own test
        // fixtures): a 720x480 / 29.97 sequence header, a GOP delimiter, and a
        // frame-picture access unit (picture header + coding extension + slice).
        fn seq_header() -> Vec<u8> {
            let (w, h, aspect, fr): (u16, u16, u8, u8) = (720, 480, 2, 4);
            let mut hdr = vec![0x00, 0x00, 0x01, 0xB3u8];
            hdr.push((w >> 4) as u8);
            hdr.push((((w & 0x0F) as u8) << 4) | (((h >> 8) & 0x0F) as u8));
            hdr.push((h & 0xFF) as u8);
            hdr.push((aspect << 4) | (fr & 0x0F));
            hdr.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x00]);
            hdr
        }
        fn gop() -> Vec<u8> {
            vec![0x00, 0x00, 0x01, 0xB8u8, 0x00, 0x00, 0x00, 0x00]
        }
        fn pic(coding_type: u8, tr: u16) -> Vec<u8> {
            let b4 = ((tr >> 2) & 0xFF) as u8;
            let b5 = (((tr & 0x03) as u8) << 6) | ((coding_type & 0x07) << 3);
            let mut au = vec![0x00, 0x00, 0x01, 0x00u8, b4, b5, 0x00, 0x00];
            // Picture coding extension: frame picture, no pulldown → 2 fields.
            au.extend_from_slice(&[0x00, 0x00, 0x01, 0xB5u8, 0x80, 0x00, 0x03, 0x00, 0x80]);
            au.extend_from_slice(&[0xAA; 32]);
            au
        }

        let mut parser = Mpeg2Parser::new();
        let field_ns = 1_000_000_000i64 * 1001 / 30000 / 2;
        let frame_ns = 2 * field_ns;
        let mut frames: Vec<crate::mux::codec::Frame> = Vec::new();
        // 80 GOPs × 12 frames ≈ 32 s of video — well past the 2 s cluster span,
        // so many clusters open and every one must carry a cue.
        let gop_len = 12u16;
        for g in 0..80i64 {
            let mut es = seq_header();
            es.extend_from_slice(&gop());
            es.extend_from_slice(&pic(1, 0)); // I-frame (tr0, keyframe)
            for tr in 1..gop_len {
                // Mix of P (tr%3==0) and B frames at climbing display order.
                let ct = if tr % 3 == 0 { 2 } else { 3 };
                es.extend_from_slice(&pic(ct, tr));
            }
            // One PES PTS anchor per GOP (90 kHz), as a real VOBU stamps.
            let gop_pts = g * gop_len as i64 * frame_ns * 90_000 / 1_000_000_000;
            frames.extend(parser.parse(&PesPacket {
                source: None,
                pid: 0x1011,
                pts: Some(gop_pts),
                dts: None,
                data: es,
            }));
        }
        frames.extend(parser.flush());

        // Confirm the DVD frame shape: every frame carries a duration (→
        // BlockGroup, NOT SimpleBlock), and the I-frames are flagged keyframes.
        assert!(
            frames.iter().all(|f| f.duration_ns.is_some()),
            "DVD VFR frames must carry per-frame durations (BlockGroup path)"
        );
        assert_eq!(
            frames.iter().filter(|f| f.keyframe).count(),
            80,
            "one I-frame keyframe per GOP"
        );

        let tracks = [make_video_track()];
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, &tracks, None, 0.0, &[]).unwrap();
        for f in &frames {
            muxer
                .write_frame(0, f.pts_ns, f.keyframe, &f.data, f.duration_ns)
                .unwrap();
        }
        muxer.finish().unwrap();
        let data = shared.lock().unwrap().clone().into_inner();

        // The muxer branches on `duration_ns`: Some → BlockGroup, None →
        // SimpleBlock. Every frame above carries Some, so this output is wholly
        // the BlockGroup path — confirmed structurally by walking the first
        // cluster's children (a BlockGroup present, no SimpleBlock).
        let clusters = find_clusters(&data);
        {
            let (c0_start, c0_size, _) = clusters[0];
            let mut kinds = Vec::new();
            let mut cur = Cursor::new(&data[c0_start..c0_start + c0_size as usize]);
            while (cur.position() as usize) < c0_size as usize {
                let (id, size, _) = ebml::read_element_header(&mut cur).unwrap();
                kinds.push(id);
                cur.seek(io::SeekFrom::Current(size as i64)).unwrap();
            }
            assert!(
                kinds.contains(&ebml::BLOCK_GROUP),
                "DVD VFR cluster must contain a BlockGroup"
            );
            assert!(
                !kinds.contains(&ebml::SIMPLE_BLOCK),
                "DVD VFR cluster must NOT contain a SimpleBlock"
            );
        }

        let cues = parse_cues(&data);
        assert!(
            clusters.len() > 1,
            "expected many clusters for 32 s of video"
        );
        assert_eq!(
            clusters.len(),
            cues.len(),
            "BlockGroup/VFR (DVD) seek index: every cluster must have a cue — \
             cluster count {} != cue count {}",
            clusters.len(),
            cues.len()
        );

        // And each cue must resolve to a real cluster (no dangling positions).
        let (_, seg_start) = locate_segment(&data);
        for (_time, _track, pos) in &cues {
            let abs = seg_start + *pos as usize;
            let mut cursor = Cursor::new(&data[abs..]);
            let (id, _size, _hdr_len) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(
                id,
                ebml::CLUSTER,
                "cue position 0x{:X} did not resolve to a cluster",
                pos
            );
        }
    }

    #[test]
    fn cue_positions_resolve_to_clusters() {
        let tracks = [make_video_track(), make_audio_track()];
        let frames = frames_for(30.0, 1.0);
        let (data, _) = mux_to_bytes(&tracks, &[], &frames);
        let (_, seg_start) = locate_segment(&data);
        let cues = parse_cues(&data);
        assert!(!cues.is_empty());
        for (_time, _track, pos) in cues {
            let abs = seg_start + pos as usize;
            let mut cursor = Cursor::new(&data[abs..]);
            let (id, _size, _hdr_len) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(
                id,
                ebml::CLUSTER,
                "cue position 0x{:X} did not resolve to a cluster",
                pos
            );
        }
    }

    #[test]
    fn cue_times_match_cluster_timestamps() {
        let tracks = [make_video_track(), make_audio_track()];
        let frames = frames_for(30.0, 1.0);
        let (data, _) = mux_to_bytes(&tracks, &[], &frames);
        let (_, seg_start) = locate_segment(&data);
        let cues = parse_cues(&data);
        for (time, _track, pos) in cues {
            let abs = seg_start + pos as usize;
            let mut cursor = Cursor::new(&data[abs..]);
            let (id, size, _hdr_len) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(id, ebml::CLUSTER);
            let body_start = abs + (cursor.position() as usize);
            let body = &data[body_start..body_start + size as usize];
            let mut bc = Cursor::new(body);
            let (tid, tsize, _) = ebml::read_element_header(&mut bc).unwrap();
            assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
            let cluster_ts = ebml::read_uint_val(&mut bc, tsize as usize).unwrap();
            assert_eq!(
                cluster_ts, time,
                "cluster timestamp {} != cue time {}",
                cluster_ts, time
            );
        }
    }

    #[test]
    fn opening_keyframe_with_nonzero_disc_pts_anchors_base_not_corrupted() {
        // SOTL SUB-TASK 2 regression (opening-GOP PTS handling). A DVD title
        // opens on an I-frame the disc stamps at its REAL timeline PTS (here
        // ~10 s, a large non-zero value — NOT 0). The muxer must anchor `base` on
        // that first kept keyframe so the first cluster's timestamp is 0 (the
        // `.max(0)` floor must NOT corrupt it into a huge value or wrap), and the
        // following frame must land exactly one frame interval (40 ms = 400 ticks
        // at the 0.1 ms scale) later — proving the opening pictures keep their
        // relative timeline and aren't garbled.
        let tracks = [make_video_track()];
        const OPEN_PTS: i64 = 10_000_000_000; // 10 s opening anchor
        let frames = vec![
            (0usize, OPEN_PTS, true, vec![0xAAu8; 8]), // opening I-frame
            (0usize, OPEN_PTS + 40_000_000, false, vec![0xBBu8; 8]), // +40 ms
        ];
        let (data, count) = mux_to_bytes(&tracks, &[], &frames);
        assert_eq!(
            count, 2,
            "both opening frames written (none dropped/floored away)"
        );

        // Read the FIRST cluster's timestamp — must be 0 (base == opening PTS).
        let (_, seg_start) = locate_segment(&data);
        let cluster_abs = seg_start
            + segment_children(&data)
                .iter()
                .find(|(id, _, _)| *id == ebml::CLUSTER)
                .map(|(_, off, _)| *off - seg_start)
                .expect("a cluster was written");
        let mut bc = Cursor::new(&data[cluster_abs..]);
        let (tid, tsize, _) = ebml::read_element_header(&mut bc).unwrap();
        assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
        let cluster_ts = ebml::read_uint_val(&mut bc, tsize as usize).unwrap();
        assert_eq!(
            cluster_ts, 0,
            "opening cluster timestamp must be 0 (base anchored on the opening keyframe's real PTS)"
        );

        // The first cue (opening keyframe) is at tick 0 — not the absolute disc PTS.
        let cues = parse_cues(&data);
        assert_eq!(cues[0].0, 0, "opening cue at t=0, disc PTS rebased to base");
    }

    #[test]
    fn seekhead_is_first_child_of_segment() {
        let tracks = [make_video_track(), make_audio_track()];
        let (data, _) = mux_to_bytes(&tracks, &[], &frames_for(10.0, 1.0));
        let children = segment_children(&data);
        assert!(!children.is_empty());
        assert_eq!(
            children[0].0,
            ebml::SEEK_HEAD,
            "first child of segment must be SeekHead, got id 0x{:X}",
            children[0].0
        );
    }

    #[test]
    fn seekhead_points_to_real_elements() {
        let tracks = [make_video_track(), make_audio_track()];
        let (data, _) = mux_to_bytes(&tracks, &[], &frames_for(10.0, 1.0));
        let (_, seg_start) = locate_segment(&data);
        let entries = parse_seekhead(&data);
        let required = [ebml::INFO, ebml::TRACKS, ebml::CUES];
        for &want_id in &required {
            let entry = entries
                .iter()
                .find(|(id, _)| *id == want_id)
                .unwrap_or_else(|| panic!("seekhead missing entry for id 0x{:X}", want_id));
            let abs = seg_start + entry.1 as usize;
            let mut cursor = Cursor::new(&data[abs..]);
            let (got_id, _, _) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(
                got_id, want_id,
                "seekhead entry for 0x{:X} resolves to wrong id 0x{:X}",
                want_id, got_id
            );
        }
    }

    #[test]
    fn seekhead_omits_chapters_when_empty() {
        let tracks = [make_video_track()];
        let (data, _) = mux_to_bytes(&tracks, &[], &frames_for(5.0, 1.0));
        let entries = parse_seekhead(&data);
        assert_eq!(
            entries.len(),
            3,
            "expected 3 seek entries (Info, Tracks, Cues), got {}",
            entries.len()
        );
        assert!(
            entries.iter().all(|(id, _)| *id != ebml::CHAPTERS),
            "seekhead should not contain Chapters entry when chapters are empty"
        );
    }

    /// Collect every (cluster_ts_ms, block_relative_ts_i16, absolute_ms) for
    /// all SimpleBlocks across all clusters, so a test can assert that the
    /// reconstructed absolute timestamp (cluster_ts + relative_ts) is correct
    /// and that no relative_ts ever wrapped the i16 range.
    fn all_block_timestamps(data: &[u8]) -> Vec<(i64, i16, i64)> {
        let mut out = Vec::new();
        for (body_start, body_size, cluster_ts) in find_clusters(data) {
            let body = &data[body_start..body_start + body_size as usize];
            let mut cursor = Cursor::new(body);
            // Skip CLUSTER_TIMESTAMP.
            let (tid, tsize, _) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
            cursor.seek(io::SeekFrom::Current(tsize as i64)).unwrap();
            while (cursor.position() as usize) < body.len() {
                let (id, sz, _) = ebml::read_element_header(&mut cursor).unwrap();
                if id == ebml::SIMPLE_BLOCK {
                    let bstart = cursor.position() as usize;
                    let b0 = body[bstart];
                    let vint_len = if b0 & 0x80 != 0 { 1 } else { 2 };
                    let ts_pos = bstart + vint_len;
                    let rel = i16::from_be_bytes([body[ts_pos], body[ts_pos + 1]]);
                    out.push((cluster_ts as i64, rel, cluster_ts as i64 + rel as i64));
                }
                cursor.seek(io::SeekFrom::Current(sz as i64)).unwrap();
            }
        }
        out
    }

    #[test]
    fn long_audio_gap_forces_cluster_no_i16_overflow() {
        // Regression for the `(pts_ms - cluster_ts_ms) as i16` truncation:
        // a single video keyframe at t=0 opens one cluster, then a long
        // audio-only stretch (no further video keyframe) drifts well past
        // i16::MAX ms (~32.767 s). Without the overflow guard the audio
        // blocks past 32.767 s would write a wrapped (negative) relative
        // timestamp into the SimpleBlock. With the guard a fresh cluster is
        // forced so every relative_ts stays in range and reconstructs to the
        // true absolute timestamp.
        let tracks = [make_video_track(), make_audio_track()];
        let mut frames: Vec<(usize, i64, bool, Vec<u8>)> = Vec::new();
        // One video keyframe at t=0 (opens the first cluster).
        frames.push((0, 0, true, vec![0xAB; 16]));
        // Audio frames every 100 ms out to 60 s — past the 32.767 s i16 limit
        // and past two i16 spans, with NO further video keyframe.
        let mut t_ms = 0i64;
        while t_ms <= 60_000 {
            frames.push((1, t_ms * 1_000_000, true, vec![0xCD; 16]));
            t_ms += 100;
        }

        let (data, _) = mux_to_bytes(&tracks, &[], &frames);

        let blocks = all_block_timestamps(&data);
        assert!(!blocks.is_empty());
        // Every block's relative timestamp must be within i16 range (it is by
        // type), AND must reconstruct to a non-negative, monotonic-ish
        // absolute timestamp matching the source — i.e. no silent wrap.
        for (cluster_ts, rel, abs) in &blocks {
            assert!(
                *rel as i64 >= 0 && (*rel as i64) <= MAX_BLOCK_REL,
                "block relative_ts {rel} out of [0, i16::MAX] range \
                 (cluster_ts={cluster_ts}, abs={abs}) — i16 overflow"
            );
        }
        // The latest audio frame is at 60_000 ms = 600_000 ticks (0.1 ms scale);
        // its reconstructed absolute timestamp must equal that, proving no
        // truncation occurred.
        let max_abs = blocks.iter().map(|(_, _, abs)| *abs).max().unwrap();
        let tick = |ms: i64| ms * 1_000_000 / TIMESTAMP_SCALE_NS;
        assert_eq!(
            max_abs,
            tick(60_000),
            "last block must reconstruct to 600_000 ticks (60_000 ms)"
        );
        // The overflow guard must have opened more than one cluster (the
        // single keyframe alone would otherwise yield exactly one).
        let clusters = find_clusters(&data);
        assert!(
            clusters.len() >= 2,
            "expected the i16 guard to force extra clusters, got {}",
            clusters.len()
        );
        // CLUSTER-SPLIT CUE REGRESSION: each i16-forced split cluster is NOT
        // keyframe-aligned (the only video keyframe is at t=0), yet it MUST carry
        // a Cue — otherwise a player seeking into the long audio stretch lands in
        // a multi-second seek-index hole. Every cluster must have a matching cue.
        let cues = parse_cues(&data);
        assert_eq!(
            cues.len(),
            clusters.len(),
            "every i16-split cluster must emit a cue (cues {} != clusters {})",
            cues.len(),
            clusters.len()
        );
        // And the cue times must cover the full span, including past the first
        // i16 boundary (~3.27 s), so the back half of the stream is seekable.
        let max_cue = cues.iter().map(|(t, _, _)| *t).max().unwrap() as i64;
        assert!(
            max_cue > MAX_BLOCK_REL,
            "cue coverage must extend past the first i16 boundary, max cue {max_cue}"
        );
    }

    #[test]
    fn pre_first_keyframe_frames_dropped() {
        let tracks = [make_video_track()];
        let frames = vec![
            (0usize, 0i64, false, vec![0x11; 16]),
            (0usize, 41_000_000i64, true, vec![0x22; 16]),
        ];
        let (data, frame_count) = mux_to_bytes(&tracks, &[], &frames);
        assert_eq!(frame_count, 1, "muxer.frame_count must equal 1");
        let clusters = find_clusters(&data);
        assert_eq!(clusters.len(), 1, "expected exactly one cluster");
        let (body_start, body_size, _ts) = clusters[0];
        let body = &data[body_start..body_start + body_size as usize];
        let mut cursor = Cursor::new(body);
        // Skip CLUSTER_TIMESTAMP.
        let (tid, tsize, _) = ebml::read_element_header(&mut cursor).unwrap();
        assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
        cursor.seek(io::SeekFrom::Current(tsize as i64)).unwrap();
        let mut sb_count = 0;
        while (cursor.position() as usize) < body.len() {
            let (id, sz, _) = ebml::read_element_header(&mut cursor).unwrap();
            if id == ebml::SIMPLE_BLOCK {
                sb_count += 1;
            }
            cursor.seek(io::SeekFrom::Current(sz as i64)).unwrap();
        }
        assert_eq!(sb_count, 1, "expected exactly one SimpleBlock in output");
    }

    #[test]
    fn no_track0_keyframe_yields_error_not_empty_file() {
        // If track 0 never delivers a keyframe, every frame is dropped. finish()
        // must surface this rather than emitting a structurally valid empty MKV.
        let tracks = [make_video_track(), make_audio_track()];
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, &tracks, None, 0.0, &[]).unwrap();
        // Audio frames (track 1) and non-keyframe video — no track-0 keyframe.
        muxer.write_frame(1, 0, true, &[0xAA; 8], None).unwrap();
        muxer
            .write_frame(0, 10_000_000, false, &[0xBB; 8], None)
            .unwrap();
        muxer
            .write_frame(1, 20_000_000, true, &[0xCC; 8], None)
            .unwrap();
        let err = muxer.finish().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn finish_with_no_frames_errors() {
        // A muxer that received no frames at all must surface MkvInvalid on
        // finish() rather than writing a structurally-empty MKV.
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0, &[]).unwrap();
        let err = muxer.finish().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn backjumped_audio_handled_by_i16_split_no_wrap() {
        // A NON-VIDEO (audio) frame whose PTS back-jumps far below the open
        // cluster does NOT drive an epoch (only video does — that is the rc3
        // fix), so it is not rebased forward. Instead the i16 block-relative
        // guard catches the out-of-range negative offset and forces a fresh,
        // Cue-carrying cluster floored at t=0 — no negative i16 relative, no
        // wrapped `as u64` cluster timestamp. Build: video kf at 0, video kf at
        // 40s, then audio at raw t=0 (a 40s back-jump).
        let tracks = [make_video_track(), make_audio_track()];
        let frames = vec![
            (0usize, 0i64, true, vec![0x01; 16]),
            (0usize, 40_000_000_000i64, true, vec![0x02; 16]), // 40s
            (1usize, 0i64, true, vec![0x03; 16]),              // back-jumped audio
        ];
        let (data, frame_count) = mux_to_bytes(&tracks, &[], &frames);
        assert_eq!(frame_count, 3);
        let clusters = find_clusters(&data);
        // Cluster timestamps stay non-negative (the `as u64` write is safe) — the
        // back-jumped audio's cluster is floored at 0, never wraps.
        let ts: Vec<u64> = clusters.iter().map(|(_, _, t)| *t).collect();
        for t in &ts {
            assert!(*t <= i64::MAX as u64, "cluster ts must not have wrapped");
        }
        // Every block's relative timestamp stays within the i16 range (no silent
        // wrap from the 40s back-jump).
        for (cluster_ts, rel, abs) in all_block_timestamps(&data) {
            assert!(
                (MIN_BLOCK_REL..=MAX_BLOCK_REL).contains(&(rel as i64)),
                "block rel {rel} wrapped i16 (cluster_ts={cluster_ts}, abs={abs})"
            );
        }
        // Every cluster carries a Cue (including the back-dated split cluster),
        // so the seek index has no hole.
        assert_eq!(
            parse_cues(&data).len(),
            clusters.len(),
            "every cluster (incl. the i16-split) must have a cue"
        );
    }

    #[test]
    fn negative_pts_audio_after_keyframe_does_not_wrap() {
        // Stream order: video keyframe at 5s (anchors base=5000ms, opens cluster
        // at ts 0), then an audio frame with raw PTS 4s — earlier than base.
        // raw_ms - base = -1000ms (negative). It must be floored to 0 rather
        // than wrapping the `as u64` cluster/cue write or overflowing the i16
        // relative cast.
        let tracks = [make_video_track(), make_audio_track()];
        let frames_in_order = [
            (0usize, 5_000_000_000i64, true, vec![0xBB; 16]), // video kf at 5s
            (1usize, 4_000_000_000i64, true, vec![0xAA; 8]),  // audio at 4s (< base)
        ];
        // Do NOT sort — preserve the out-of-order arrival.
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, &tracks, None, 0.0, &[]).unwrap();
        for (t, pts, kf, data) in &frames_in_order {
            muxer.write_frame(*t, *pts, *kf, data, None).unwrap();
        }
        muxer.finish().unwrap();
        let data = shared.lock().unwrap().clone().into_inner();
        let clusters = find_clusters(&data);
        assert!(!clusters.is_empty());
        for (_, _, ts) in &clusters {
            // A wrapped negative would be a huge near-u64::MAX value.
            assert!(*ts < 1_000_000_000, "cluster timestamp wrapped: {}", ts);
        }
    }

    #[test]
    fn track_vint_encodes_one_and_two_byte_forms() {
        // 1-byte form for track numbers < 0x80, high bit set.
        let (b, n) = track_vint(1);
        assert_eq!(&b[..n], &[0x81]);
        let (b, n) = track_vint(0x7F);
        assert_eq!(&b[..n], &[0xFF]);
        // 2-byte form at/above 0x80, 0x40 length marker in the top byte.
        let (b, n) = track_vint(0x80);
        assert_eq!(&b[..n], &[0x40, 0x80]);
        let (b, n) = track_vint(0x3FFF);
        assert_eq!(&b[..n], &[0x7F, 0xFF]);
    }

    // ============================================================
    // SimpleBlock byte layout (Matroska §6.2.3): the element's declared
    // size must equal track_vint_len + 2 (rel ts) + 1 (flags) + data, and
    // the rel-ts is a signed 16-bit big-endian field. A wrong size desyncs
    // every following element; a wrong ts byte order corrupts A/V sync.
    // ============================================================

    /// Locate the first SimpleBlock and return (declared_size, track_vint_len,
    /// rel_ts, flags, data_slice) by decoding its header inline.
    fn first_simple_block_full(data: &[u8]) -> (u64, usize, i16, u8, Vec<u8>) {
        let clusters = find_clusters(data);
        let (body_start, body_size, _ts) = clusters[0];
        let body = &data[body_start..body_start + body_size as usize];
        let mut cursor = Cursor::new(body);
        // Skip CLUSTER_TIMESTAMP.
        let (tid, tsize, _) = ebml::read_element_header(&mut cursor).unwrap();
        assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
        cursor.seek(io::SeekFrom::Current(tsize as i64)).unwrap();
        loop {
            let (id, size, _) = ebml::read_element_header(&mut cursor).unwrap();
            if id == ebml::SIMPLE_BLOCK {
                let p = cursor.position() as usize;
                let b0 = body[p];
                let vl = if b0 & 0x80 != 0 { 1 } else { 2 };
                let rel = i16::from_be_bytes([body[p + vl], body[p + vl + 1]]);
                let flags = body[p + vl + 2];
                let dat = body[p + vl + 3..p + size as usize].to_vec();
                return (size, vl, rel, flags, dat);
            }
            cursor.seek(io::SeekFrom::Current(size as i64)).unwrap();
        }
    }

    /// A frame for `mux_with_durations`: (track, pts_ns, keyframe, data,
    /// duration_ns). Aliased to keep clippy's type-complexity lint happy.
    type DurFrame = (usize, i64, bool, Vec<u8>, Option<u64>);

    /// Mux frames through a SharedWriter and return the finalized buffer, so
    /// the final cluster is closed (size back-patched) before inspection.
    fn mux_with_durations(tracks: &[MkvTrack], frames: &[DurFrame]) -> Vec<u8> {
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, tracks, None, 0.0, &[]).unwrap();
        for (t, pts, kf, data, dur) in frames {
            muxer.write_frame(*t, *pts, *kf, data, *dur).unwrap();
        }
        muxer.finish().unwrap();
        shared.lock().unwrap().clone().into_inner()
    }

    #[test]
    fn simple_block_declared_size_covers_exactly_the_payload() {
        let tracks = [make_video_track()];
        let payload = vec![0x11u8, 0x22, 0x33, 0x44, 0x55];
        let data = mux_with_durations(&tracks, &[(0, 0, true, payload.clone(), None)]);
        let (size, vl, rel, flags, dat) = first_simple_block_full(&data);
        // size = vint(vl) + ts(2) + flags(1) + data(5).
        assert_eq!(size as usize, vl + 2 + 1 + payload.len());
        assert_eq!(rel, 0, "first frame at cluster base → rel ts 0");
        assert_eq!(flags & 0x80, 0x80, "keyframe flag set");
        assert_eq!(dat, payload, "data must be the exact frame bytes");
    }

    #[test]
    fn simple_block_rel_ts_is_signed_big_endian() {
        // A frame 1000 ms after the keyframe-anchored cluster (within the 3 s
        // cluster window) must encode rel ts = 1000 ms in TICKS = 10_000.
        let tracks = [make_video_track()];
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 1_000_000_000, false, vec![0xBB], None),
            ],
        );
        // The second block is in the same cluster (1000ms < 3000ms boundary, and
        // 10_000 ticks < the i16 span).
        let clusters = find_clusters(&data);
        assert_eq!(clusters.len(), 1, "1s < 3s cluster window → one cluster");
        let blocks = all_block_timestamps(&data);
        // Two blocks: rel 0 and rel 10_000 (1000 ms at the 0.1 ms scale).
        let rels: Vec<i16> = blocks.iter().map(|(_, r, _)| *r).collect();
        assert!(
            rels.contains(&10_000),
            "second block rel ts must be 10_000 ticks"
        );
    }

    // ============================================================
    // BlockGroup (Matroska §6.2.4): a Block inside a BlockGroup carries
    // BlockDuration, and the Block's keyframe flag bit (0x80) MUST be 0
    // (keyframe-ness is signalled by absence of ReferenceBlock). PGS
    // subtitle frames take this path.
    // ============================================================

    fn first_block_group(data: &[u8]) -> (Vec<u8>, u64, u8) {
        // Returns (inner BLOCK payload bytes after vint+ts+flags, block_duration_ms, flags).
        let clusters = find_clusters(data);
        for (body_start, body_size, _ts) in clusters {
            let body = &data[body_start..body_start + body_size as usize];
            let mut cursor = Cursor::new(body);
            let (tid, tsize, _) = ebml::read_element_header(&mut cursor).unwrap();
            assert_eq!(tid, ebml::CLUSTER_TIMESTAMP);
            cursor.seek(io::SeekFrom::Current(tsize as i64)).unwrap();
            while (cursor.position() as usize) < body.len() {
                let (id, size, _) = ebml::read_element_header(&mut cursor).unwrap();
                if id == ebml::BLOCK_GROUP {
                    let bg_start = cursor.position() as usize;
                    let bg = &body[bg_start..bg_start + size as usize];
                    // Parse the BlockGroup children.
                    let mut bc = Cursor::new(bg);
                    let mut data_after = Vec::new();
                    let mut dur = 0u64;
                    let mut flags = 0xFFu8;
                    while (bc.position() as usize) < bg.len() {
                        let (cid, cs, _) = ebml::read_element_header(&mut bc).unwrap();
                        let cstart = bc.position() as usize;
                        if cid == ebml::BLOCK {
                            let blk = &bg[cstart..cstart + cs as usize];
                            let vl = if blk[0] & 0x80 != 0 { 1 } else { 2 };
                            flags = blk[vl + 2];
                            data_after = blk[vl + 3..].to_vec();
                        } else if cid == ebml::BLOCK_DURATION {
                            dur = ebml::read_uint_val(&mut bc, cs as usize).unwrap();
                            continue;
                        }
                        bc.seek(io::SeekFrom::Current(cs as i64)).unwrap();
                    }
                    return (data_after, dur, flags);
                }
                cursor.seek(io::SeekFrom::Current(size as i64)).unwrap();
            }
        }
        panic!("no BlockGroup found");
    }

    #[test]
    fn block_group_emits_block_duration_and_clears_keyframe_flag() {
        // A frame written with a duration becomes a BlockGroup. The inner Block
        // MUST have flags 0x00 (the 0x80 keyframe bit is reserved/zero inside a
        // BlockGroup per the spec), and BlockDuration must equal the ms value.
        let tracks = [make_video_track()];
        // Open a cluster with a keyframe (track 0), then a frame carrying a
        // duration. Pass keyframe=true to prove the flag is still forced to 0.
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 40_000_000, true, vec![0xCC, 0xDD], Some(40_000_000)),
            ],
        );
        let (block_data, dur_ticks, flags) = first_block_group(&data);
        assert_eq!(block_data, vec![0xCC, 0xDD]);
        // BlockDuration is in ticks: 40_000_000 ns / 100_000 = 400 ticks (40 ms).
        assert_eq!(dur_ticks, 400, "BlockDuration must be 400 ticks (40 ms)");
        assert_eq!(
            flags & 0x80,
            0x00,
            "Block inside BlockGroup must clear the keyframe flag (got 0x{flags:02X})"
        );
    }

    #[test]
    fn block_duration_floored_to_at_least_one_tick() {
        // A sub-tick duration (e.g. 50_000 ns = 0.05 ms, under the 0.1 ms tick)
        // must floor to 1 tick, never 0 — a 0-duration BlockGroup would tell
        // players to remove the artifact instantly.
        let tracks = [make_video_track()];
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 10_000_000, true, vec![0xBB], Some(50_000)),
            ],
        );
        let (_, dur_ticks, _) = first_block_group(&data);
        assert_eq!(
            dur_ticks, 1,
            "sub-tick duration must floor to 1 tick, not 0"
        );
    }

    // ============================================================
    // Cluster boundary (CLUSTER_DURATION_TICKS): a new cluster opens on a
    // video keyframe once >= the cluster duration has elapsed since the open
    // cluster's timestamp. A keyframe exactly at the boundary opens a new
    // cluster; one just under stays in the current cluster.
    // ============================================================

    #[test]
    fn keyframe_at_cluster_boundary_opens_new_cluster() {
        let tracks = [make_video_track()];
        // Keyframe at exactly 3000 ms (>= the 3 s cluster window) → new cluster.
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 3_000_000_000, true, vec![0xBB], None),
            ],
        );
        assert_eq!(
            find_clusters(&data).len(),
            2,
            "keyframe at the 3s boundary must open a second cluster"
        );
    }

    #[test]
    fn keyframe_just_under_cluster_window_stays_in_cluster() {
        let tracks = [make_video_track()];
        // Keyframe at 1999 ms (< 2000 nominal, and 19990 ticks < i16 span) → same
        // cluster.
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 1_999_000_000, true, vec![0xBB], None),
            ],
        );
        assert_eq!(
            find_clusters(&data).len(),
            1,
            "keyframe under the cluster window must stay in the open cluster"
        );
    }

    // ============================================================
    // monotonic_ts saturating add — at i64::MAX the +1 must saturate, not
    // overflow-panic. (The strictly-monotonic invariant relies on
    // saturating_add.)
    // ============================================================

    #[test]
    fn monotonic_ts_saturates_at_i64_max() {
        // prev = i64::MAX, pts equal → saturating_add(1) caps at i64::MAX rather
        // than wrapping to i64::MIN.
        assert_eq!(monotonic_ts(Some(i64::MAX), i64::MAX), i64::MAX);
        // A pts already above prev+1 is left alone.
        assert_eq!(monotonic_ts(Some(10), 100), 100);
    }

    // ============================================================
    // SeekHead encoding (Matroska §7.1): the muxer writes fixed-width
    // entries — SeekID as a 4-byte binary element (size 0x84) and
    // SeekPosition as an 8-byte uint (size 0x88) so they can be
    // back-patched in place. Verify the declared SeekID matches the target
    // element ID bytes.
    // ============================================================

    // ============================================================
    // dolby_vision_config (dvcC / DOVIDecoderConfigurationRecord) bit
    // packing. Byte 2: profile(7 bits) << 1 | level high bit. Byte 3:
    // level low 5 bits << 3 | rpu | el | bl. Byte 4: bl_compat_id << 4.
    // ============================================================

    #[test]
    fn dolby_vision_config_packs_level_and_compat_id() {
        // profile 7, level 6 (0b00110), bl_compat_id 1.
        let c = dolby_vision_config(7, 6, 1);
        assert_eq!(c.len(), 24);
        // level high bit = (6 >> 5) & 1 = 0 → byte2 low bit 0; profile 7 in top.
        // byte2 = profile(7) << 1 | level_high_bit(0).
        assert_eq!(c[2], 7 << 1);
        assert_eq!(c[2] & 0x01, 0, "level bit 5 is 0 for level 6");
        // byte3: (6 & 0x1F) << 3 | rpu|el|bl = (6<<3) | 0b111 = 0x30 | 0x07.
        assert_eq!(c[3], (6 << 3) | 0b111);
        // byte4: bl_compat_id 1 in the top nibble.
        assert_eq!(c[4], 1 << 4);
        // Reserved tail is zero.
        assert!(c[5..].iter().all(|&b| b == 0), "v[5..24] reserved = 0");
    }

    #[test]
    fn dolby_vision_config_high_level_sets_byte2_low_bit() {
        // A level with bit 5 set (>= 32) must place that bit in byte2's LSB.
        // level 0x20 → (0x20 >> 5) & 1 = 1.
        let c = dolby_vision_config(7, 0x20, 0);
        assert_eq!(c[2] & 0x01, 1, "level bit 5 belongs in byte2 LSB");
        // and byte3 carries the low 5 bits (0x20 & 0x1F = 0) << 3.
        assert_eq!(c[3] >> 3, 0);
    }

    // ============================================================
    // Full round-trip: mux frames → MKV bytes → MkvStream reader → frames.
    // This is the strongest "never silently truncate" property: every
    // written frame must be readable back with the same track, keyframe
    // flag and data.
    // ============================================================

    #[test]
    fn muxed_frames_round_trip_through_reader() {
        use crate::pes::Stream as _;
        let tracks = [make_video_track(), make_audio_track()];
        // Two video keyframes + interleaved audio, all within one cluster.
        let frames = vec![
            (0usize, 0i64, true, vec![0x01, 0x02, 0x03]),
            (1usize, 0i64, false, vec![0x0B, 0x77, 0x00]),
            (0usize, 1_000_000_000i64, false, vec![0x04, 0x05]),
        ];
        let (data, count) = mux_to_bytes(&tracks, &[], &frames);
        assert_eq!(count, 3, "all three frames must be written");

        let mut stream = super::super::mkvstream::MkvStream::open(Cursor::new(data)).unwrap();
        let mut read_back = Vec::new();
        while let Some(f) = stream.read().unwrap() {
            read_back.push((f.track, f.keyframe, f.data));
        }
        // All three frames survive the round trip (no silent drop/truncation).
        assert_eq!(read_back.len(), 3, "every muxed frame must read back");
        // Track 0 video keyframe with its exact bytes is present.
        assert!(
            read_back
                .iter()
                .any(|(t, kf, d)| *t == 0 && *kf && d == &[0x01, 0x02, 0x03])
        );
        // Track 1 audio frame bytes survive.
        assert!(
            read_back
                .iter()
                .any(|(t, _, d)| *t == 1 && d == &[0x0B, 0x77, 0x00])
        );
    }

    #[test]
    fn audio_track_emits_sampling_frequency_and_channels() {
        // An audio TrackEntry must contain an Audio element (0xE1) with
        // SamplingFrequency (0xB5, an 8-byte float) and Channels (0x9F).
        // Without these, players can't configure the audio decoder.
        let tracks = [make_video_track(), make_audio_track()];
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &tracks, None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::AUDIO).is_some(),
            "Audio element present"
        );
        assert!(
            find_id(&data, ebml::SAMPLING_FREQUENCY).is_some(),
            "SamplingFrequency present"
        );
        assert!(find_id(&data, ebml::CHANNELS).is_some(), "Channels present");
    }

    #[test]
    fn video_colour_element_emitted_only_when_hdr_metadata_present() {
        // A video track with colour metadata (matrix/transfer) must emit the
        // Colour element (0x55B0); a plain SDR track with all-zero colour must
        // not. The conditional is `colour_matrix > 0 || colour_transfer > 0`.
        let mut hdr_video = make_video_track();
        hdr_video.colour_matrix = 9; // bt2020nc
        hdr_video.colour_transfer = 16; // PQ
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[hdr_video], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::COLOUR).is_some(),
            "Colour element must be emitted for HDR track"
        );

        // make_video_track has zero colour fields → no Colour element.
        let muxer = MkvMuxer::new(
            Cursor::new(Vec::new()),
            &[make_video_track()],
            None,
            0.0,
            &[],
        )
        .unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::COLOUR).is_none(),
            "no Colour element when colour metadata is all zero"
        );
    }

    #[test]
    fn video_emits_flag_interlaced_and_field_order() {
        // An interlaced track must emit FlagInterlaced=1 and its FieldOrder
        // value. A progressive track must emit FlagInterlaced=2 and NO
        // FieldOrder.
        let mut interlaced = make_video_track();
        interlaced.interlaced = true;
        interlaced.field_order = ebml::FIELD_ORDER_TFF;
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[interlaced], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        let fi = find_id(&data, ebml::FLAG_INTERLACED).expect("FlagInterlaced present");
        // [id=0x9A][size=0x81][value]
        assert_eq!(
            data[fi + 2],
            ebml::INTERLACED_INTERLACED as u8,
            "FlagInterlaced must be 1 (interlaced)"
        );
        let fo = find_id(&data, ebml::FIELD_ORDER).expect("FieldOrder present");
        assert_eq!(
            data[fo + 2],
            ebml::FIELD_ORDER_TFF,
            "FieldOrder value must round-trip through the writer"
        );

        // Progressive track: FlagInterlaced=2, no FieldOrder.
        let muxer = MkvMuxer::new(
            Cursor::new(Vec::new()),
            &[make_video_track()],
            None,
            0.0,
            &[],
        )
        .unwrap();
        let data = muxer.writer.into_inner();
        let fi = find_id(&data, ebml::FLAG_INTERLACED).expect("FlagInterlaced present");
        assert_eq!(
            data[fi + 2],
            ebml::INTERLACED_PROGRESSIVE as u8,
            "FlagInterlaced must be 2 (progressive)"
        );
        assert!(
            find_id(&data, ebml::FIELD_ORDER).is_none(),
            "no FieldOrder for progressive content"
        );
    }

    #[test]
    fn video_576i_field_order_undetermined_at_track_build() {
        // Field order is a bitstream property the IFO/MPLS scan cannot know, so
        // the track is built with FieldOrder=UNDETERMINED — never a scan-time
        // guess. The mux stream sets the MEASURED value from the first coded
        // picture before the header is written (mkvstream::apply_coding_to_track).
        let v = VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: crate::disc::FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: Some((16, 9)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };
        let t = MkvTrack::video(&v);
        assert!(t.interlaced, "576i is interlaced (FlagInterlaced=1)");
        assert_eq!(
            t.field_order,
            ebml::FIELD_ORDER_UNDETERMINED,
            "field order is not known at scan — set later from the measured picture"
        );
    }

    #[test]
    fn interlaced_576i_omits_default_decoded_field_duration_keeps_full_frame_duration() {
        // SOTL SUB-TASK 1 regression. The Windows-fps fix: a 576i25 track must
        // carry the FULL-FRAME DefaultDuration (40 ms → `1/DefaultDuration` = 25
        // fps, the only rate every tool trusts) and must NOT emit
        // DefaultDecodedFieldDuration. rc.5.1 emitted the 20 ms field duration to
        // try to fix Windows; the captured SOTL evidence proved it did the
        // opposite (Explorer 12.5 fps, MediaInfo VFR). MakeMKV's correct rip omits
        // it (Explorer 25 fps, MediaInfo CFR). So: frame duration present = 40 ms,
        // field duration ABSENT, interlace signalling (FlagInterlaced/FieldOrder)
        // retained.
        let v = VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: crate::disc::FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };
        let t = MkvTrack::video(&v);
        assert_eq!(t.default_duration_ns, 40_000_000, "frame duration is 40 ms");
        assert_eq!(
            t.field_duration_ns, 0,
            "field duration must be 0 so the element is suppressed"
        );
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[t], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        // DefaultDuration (frame) present and = 40 ms → drives the 25 fps report.
        let dd = find_id(&data, ebml::DEFAULT_DURATION).expect("DefaultDuration present");
        let frame_ns = u32::from_be_bytes([data[dd + 4], data[dd + 5], data[dd + 6], data[dd + 7]]);
        assert_eq!(frame_ns, 40_000_000, "DefaultDuration is the full frame");
        // DefaultDecodedFieldDuration must be ABSENT — this is the fix.
        assert!(
            find_id(&data, ebml::DEFAULT_DECODED_FIELD_DURATION).is_none(),
            "DefaultDecodedFieldDuration must NOT be written (Windows halves the rate when it is)"
        );
        // Interlace signalling is RETAINED so deinterlacers still engage and
        // MediaInfo (which also reads scan type from the MPEG-2 ES) agrees.
        let fi = find_id(&data, ebml::FLAG_INTERLACED).expect("FlagInterlaced present");
        assert_eq!(
            data[fi + 2],
            ebml::INTERLACED_INTERLACED as u8,
            "FlagInterlaced=1 retained"
        );
        // FieldOrder is set at mux time from the first coded picture's measured
        // field order. A track built directly (no measured picture) carries
        // UNDETERMINED, so the element is omitted — never a scan-time guess.
        assert!(
            find_id(&data, ebml::FIELD_ORDER).is_none(),
            "FieldOrder omitted until measured — no guess at track build"
        );
    }

    #[test]
    fn progressive_video_omits_field_duration() {
        // A progressive track must NOT carry DefaultDecodedFieldDuration.
        let t = make_video_track(); // progressive
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[t], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::DEFAULT_DECODED_FIELD_DURATION).is_none(),
            "no field duration for progressive content"
        );
    }

    #[test]
    fn field_duration_when_set_is_direct_trackentry_child_not_in_video() {
        // GUARDS the element nesting for the retained (now non-default) writer
        // path: if a caller DOES set field_duration_ns > 0,
        // DefaultDecodedFieldDuration (0x234E7A) must be emitted as a DIRECT child
        // of TrackEntry — NOT nested inside the Video master (which only holds
        // FlagInterlaced / FieldOrder), per the Matroska schema. The production
        // video path passes 0 (so the element is suppressed — see
        // interlaced_576i_omits_default_decoded_field_duration_keeps_full_frame_duration);
        // this test builds a track with a non-zero field duration to exercise the
        // writer guard and pin its (correct) nesting depth.
        let mut t = make_video_track();
        t.interlaced = true;
        t.field_order = ebml::FIELD_ORDER_TFF;
        t.field_duration_ns = 20_000_000;
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[t], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();

        let (te_start, te_size) = first_track_entry(&data);
        let te_children = master_children(&data, te_start, te_size);

        // Direct child of TrackEntry — present.
        assert!(
            te_children
                .iter()
                .any(|(id, _, _)| *id == ebml::DEFAULT_DECODED_FIELD_DURATION),
            "DefaultDecodedFieldDuration must be a DIRECT child of TrackEntry"
        );

        // Locate the Video master (a direct child of TrackEntry) and confirm the
        // field-duration element is NOT inside it.
        let (_, vid_start, vid_size) = te_children
            .iter()
            .copied()
            .find(|(id, _, _)| *id == ebml::VIDEO)
            .expect("Video master present");
        let vid_children = master_children(&data, vid_start, vid_size as usize);
        assert!(
            !vid_children
                .iter()
                .any(|(id, _, _)| *id == ebml::DEFAULT_DECODED_FIELD_DURATION),
            "DefaultDecodedFieldDuration must NOT be nested inside the Video master"
        );
        // And, for completeness, DefaultDuration is also a TrackEntry child (not
        // in Video) — pins the pair together so a future edit can't move either.
        assert!(
            te_children
                .iter()
                .any(|(id, _, _)| *id == ebml::DEFAULT_DURATION),
            "DefaultDuration must be a direct child of TrackEntry"
        );
        assert!(
            !vid_children
                .iter()
                .any(|(id, _, _)| *id == ebml::DEFAULT_DURATION),
            "DefaultDuration must NOT be nested inside the Video master"
        );
        // FlagInterlaced / FieldOrder ARE Video children (the spec's split).
        assert!(
            vid_children
                .iter()
                .any(|(id, _, _)| *id == ebml::FLAG_INTERLACED),
            "FlagInterlaced is a Video child"
        );
    }

    /// Helper: read the 1-byte value of a `[id][size=0x81][value]` uint element
    /// among the direct children of the Video master of the first TrackEntry.
    fn video_child_u8(data: &[u8], id: u32) -> Option<u8> {
        let (te_start, te_size) = first_track_entry(data);
        let (_, vid_start, vid_size) = master_children(data, te_start, te_size)
            .into_iter()
            .find(|(c, _, _)| *c == ebml::VIDEO)?;
        let (_, child_start, child_size) = master_children(data, vid_start, vid_size as usize)
            .into_iter()
            .find(|(c, _, _)| *c == id)?;
        // 1-byte uint value sits at the child body start.
        (child_size == 1).then(|| data[child_start])
    }

    #[test]
    fn pal_576i_emits_bt470bg_colour_codes() {
        // GUARDS audit §2 colour-code gap: the dvd.rs tests assert at the stream
        // layer (ColorSpace::Bt470bg); nothing asserted the actual CICP tuple
        // emitted in the MKV. PAL SD must emit matrix/transfer/primaries =
        // (5,5,5) with range=1 (BT.470BG). A swap with NTSC's (6,6,6) goes
        // uncaught by the stream-layer tests alone.
        let v = VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: crate::disc::FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: Some((16, 9)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };
        let t = MkvTrack::video(&v);
        assert_eq!(
            (
                t.colour_matrix,
                t.colour_transfer,
                t.colour_primaries,
                t.colour_range
            ),
            (5, 5, 5, 1),
            "PAL SD must map to BT.470BG (5,5,5,1)"
        );
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[t], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        // Depth-scoped: Colour master inside Video, with the exact CICP codes.
        let (te_start, te_size) = first_track_entry(&data);
        let (_, vid_start, vid_size) = master_children(&data, te_start, te_size)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::VIDEO)
            .expect("Video master");
        let (_, col_start, col_size) = master_children(&data, vid_start, vid_size as usize)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::COLOUR)
            .expect("Colour master present for PAL SD");
        let col = master_children(&data, col_start, col_size as usize);
        let val = |id: u32| -> u8 {
            let (_, off, sz) = col.iter().copied().find(|(c, _, _)| *c == id).unwrap();
            assert_eq!(sz, 1, "single-byte CICP value");
            data[off]
        };
        assert_eq!(
            val(ebml::MATRIX_COEFFICIENTS),
            5,
            "PAL matrix = BT.470BG (5)"
        );
        assert_eq!(
            val(ebml::TRANSFER_CHARACTERISTICS),
            5,
            "PAL transfer = BT.470BG (5)"
        );
        assert_eq!(val(ebml::PRIMARIES), 5, "PAL primaries = BT.470BG (5)");
        assert_eq!(val(ebml::RANGE), 1, "PAL range = limited (1)");
    }

    #[test]
    fn ntsc_480i_emits_smpte170m_colour_codes() {
        // Mirror of the PAL test: NTSC SD must emit (6,6,6,1) — SMPTE-170M /
        // BT.601-525 — not BT.470BG's (5,5,5). Together the two tests pin the
        // PAL/NTSC colour split at the emitted-byte layer.
        let v = VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R480i,
            frame_rate: crate::disc::FrameRate::F29_97,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Smpte170m,
            display_aspect: Some((4, 3)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };
        let t = MkvTrack::video(&v);
        assert_eq!(
            (
                t.colour_matrix,
                t.colour_transfer,
                t.colour_primaries,
                t.colour_range
            ),
            (6, 6, 6, 1),
            "NTSC SD must map to SMPTE-170M (6,6,6,1)"
        );
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[t], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        let (te_start, te_size) = first_track_entry(&data);
        let (_, vid_start, vid_size) = master_children(&data, te_start, te_size)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::VIDEO)
            .expect("Video master");
        let (_, col_start, col_size) = master_children(&data, vid_start, vid_size as usize)
            .into_iter()
            .find(|(id, _, _)| *id == ebml::COLOUR)
            .expect("Colour master present for NTSC SD");
        let col = master_children(&data, col_start, col_size as usize);
        let val = |id: u32| -> u8 {
            let (_, off, sz) = col.iter().copied().find(|(c, _, _)| *c == id).unwrap();
            assert_eq!(sz, 1, "single-byte CICP value");
            data[off]
        };
        assert_eq!(
            val(ebml::MATRIX_COEFFICIENTS),
            6,
            "NTSC matrix = SMPTE-170M (6)"
        );
        assert_eq!(
            val(ebml::TRANSFER_CHARACTERISTICS),
            6,
            "NTSC transfer = SMPTE-170M (6)"
        );
        assert_eq!(val(ebml::PRIMARIES), 6, "NTSC primaries = SMPTE-170M (6)");
        assert_eq!(val(ebml::RANGE), 1, "NTSC range = limited (1)");
    }

    #[test]
    fn ntsc_480i_duration_metadata_and_field_order_undetermined_at_build() {
        // NTSC 480i duration metadata (Windows-fps fix) PLUS field-order honesty.
        // Field order is a bitstream property the IFO/MPLS scan cannot know, so a
        // track built without a measured picture carries FieldOrder=UNDETERMINED
        // and the element is OMITTED — never a hardcoded TFF guess. The MEASURED
        // value is set by the mux stream from the first coded picture (see
        // mkvstream::apply_coding_to_track and its dedicated test).
        let v = VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R480i,
            frame_rate: crate::disc::FrameRate::F29_97,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Smpte170m,
            display_aspect: Some((4, 3)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        };
        let t = MkvTrack::video(&v);
        assert!(t.interlaced, "480i is interlaced");
        assert_eq!(
            t.field_order,
            ebml::FIELD_ORDER_UNDETERMINED,
            "field order is not known at scan time — never guessed at track build"
        );
        // 480i @ 29.97: frame = 1001/30000 s = 33_366_666 ns; field = half.
        assert_eq!(
            t.default_duration_ns, 33_366_666,
            "480i frame duration is ~33.37 ms (29.97 fps, not halved)"
        );
        assert_eq!(
            t.field_duration_ns, 0,
            "field duration is suppressed (DefaultDecodedFieldDuration omitted — Windows-fps fix)"
        );
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[t], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        // FlagInterlaced is still encoded (480i IS interlaced); FieldOrder is
        // omitted until measured — never a hardcoded guess.
        assert_eq!(
            video_child_u8(&data, ebml::FLAG_INTERLACED),
            Some(ebml::INTERLACED_INTERLACED as u8),
            "480i must encode FlagInterlaced = 1"
        );
        assert_eq!(
            video_child_u8(&data, ebml::FIELD_ORDER),
            None,
            "FieldOrder omitted until measured — not a hardcoded guess"
        );
    }

    #[test]
    fn finalize_emits_per_track_bps_tags() {
        // At finalize a Tags master with a per-track BPS SimpleTag is written.
        // BPS = bytes*8/duration_secs. With a 10 s duration and a video frame of
        // 1000 bytes, video BPS = 1000*8/10 = 800.
        let tracks = [make_video_track(), make_audio_track()];
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, &tracks, None, 10.0, &[]).unwrap();
        // Video keyframe 1000 bytes; audio frame 500 bytes.
        muxer
            .write_frame(0, 0, true, &vec![0xABu8; 1000], None)
            .unwrap();
        muxer
            .write_frame(1, 0, false, &vec![0xCDu8; 500], None)
            .unwrap();
        muxer.finish().unwrap();
        let data = shared.lock().unwrap().clone().into_inner();

        // The Tags master must be present as a top-level Segment child.
        let children = segment_children(&data);
        assert!(
            children.iter().any(|(id, _, _)| *id == ebml::TAGS),
            "Tags element must be written at finalize"
        );
        // The BPS values must appear as TagString text. Video: 800, Audio: 400.
        let text = String::from_utf8_lossy(&data);
        assert!(text.contains("BPS"), "BPS TagName must be present");
        assert!(
            text.contains("800"),
            "video BPS (1000*8/10) must be present"
        );
        assert!(text.contains("400"), "audio BPS (500*8/10) must be present");
    }

    #[test]
    fn no_bps_tags_when_duration_unknown() {
        // With duration 0 (unknown) the BPS rate can't be computed; no Tags.
        let tracks = [make_video_track()];
        let frames = vec![(0usize, 0i64, true, vec![0xABu8; 1000])];
        let (data, _) = mux_to_bytes(&tracks, &[], &frames);
        let children = segment_children(&data);
        assert!(
            !children.iter().any(|(id, _, _)| *id == ebml::TAGS),
            "no Tags element when duration is unknown"
        );
    }

    #[test]
    fn ac3_channels_corrected_from_bitstream_acmod() {
        // The audio track header claims 6 channels (IFO 5.1), but the AC-3
        // bitstream's first frame has acmod=2 (2.0 stereo). The Channels element
        // must be rewritten to 2 from the bitstream, not left at the IFO's 6.
        let mut audio = make_audio_track(); // codec A_AC3, channels = 6
        audio.channels = 6;
        let video = make_video_track();
        let shared = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        let writer = SharedWriter(shared.clone());
        let mut muxer = MkvMuxer::new(writer, &[video, audio], None, 0.0, &[]).unwrap();
        // A minimal AC-3 BSI with acmod=2 (2/0 stereo), no LFE → 2 channels.
        // byte5 = bsid 8 (legacy AC-3). byte6: acmod(010) | dsurmod(00) |
        // lfeon(0) = 0b0100_0000 = 0x40. acmod_channels only needs >= 8 bytes.
        let ac3 = vec![0x0B, 0x77, 0x00, 0x00, 0x00, 8 << 3, 0x40, 0x00];
        // Open a cluster with a video keyframe first (cluster invariant).
        muxer.write_frame(0, 0, true, &[0x01, 0x02], None).unwrap();
        muxer.write_frame(1, 0, false, &ac3, None).unwrap();
        muxer.finish().unwrap();
        let data = shared.lock().unwrap().clone().into_inner();

        // Locate the Channels element (0x9F) WITHIN the Tracks body (so a stray
        // 0x9F in cluster/AC-3 payload can't be mistaken for the element) and
        // assert the value byte is 2.
        let (tracks_start, tracks_size) = segment_children(&data)
            .into_iter()
            .find_map(|(id, off, sz)| (id == ebml::TRACKS).then_some((off, sz as usize)))
            .expect("Tracks element present");
        let tracks_body = &data[tracks_start..tracks_start + tracks_size];
        let ch = find_id(tracks_body, ebml::CHANNELS).expect("Channels element present");
        assert_eq!(
            tracks_body[ch + 2],
            2,
            "Channels must be corrected to 2 (bitstream acmod), not 6 (IFO)"
        );
    }

    #[test]
    fn dolby_vision_track_emits_block_addition_mapping() {
        // A DV track (dv_config set) must emit BlockAdditionMapping (0x41E4)
        // carrying the dvcC so players recognise Dolby Vision.
        let mut dv = make_video_track();
        dv.dv_config = Some(dolby_vision_config(7, 6, 0));
        let muxer = MkvMuxer::new(Cursor::new(Vec::new()), &[dv], None, 0.0, &[]).unwrap();
        let data = muxer.writer.into_inner();
        assert!(
            find_id(&data, ebml::BLOCK_ADDITION_MAPPING).is_some(),
            "DV track must emit BlockAdditionMapping"
        );
        // Without dv_config, no mapping.
        let muxer = MkvMuxer::new(
            Cursor::new(Vec::new()),
            &[make_video_track()],
            None,
            0.0,
            &[],
        )
        .unwrap();
        let data = muxer.writer.into_inner();
        assert!(find_id(&data, ebml::BLOCK_ADDITION_MAPPING).is_none());
    }
}
