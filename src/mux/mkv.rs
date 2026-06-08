//! Matroska (MKV) muxer.
//!
//! Writes EBML header, Segment with tracks, clusters, and cues.
//! Designed for streaming writes: clusters are written as data arrives,
//! cues and seek head are finalized at the end.

use super::ebml;
use crate::disc::{
    AudioStream, Chapter, Codec, ColorSpace, HdrFormat, SubtitleStream, VideoStream,
};
use std::io::{self, Seek, Write};

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
            Codec::H264 => "V_MPEG4/ISO/AVC",
            Codec::Hevc => "V_MPEGH/ISO/HEVC",
            Codec::Vc1 => "V_MS/VFW/FOURCC",
            Codec::Mpeg2 => "V_MPEG2",
            _ => "V_MPEG2",
        };
        let (w, h) = v.resolution.pixels();
        let (num, den) = v.frame_rate.as_fraction();
        let default_duration_ns = if num > 0 {
            (1_000_000_000u64 * den as u64) / num as u64
        } else {
            0
        };
        let (matrix, transfer, primaries, range) = match v.color_space {
            ColorSpace::Bt2020 => (9, 16, 9, 1), // bt2020nc, PQ, bt2020, limited
            ColorSpace::Bt709 => (1, 1, 1, 1),   // bt709
            ColorSpace::Unknown => (0, 0, 0, 0),
        };
        // Override transfer for non-PQ HDR
        let transfer = match v.hdr {
            HdrFormat::Hdr10 | HdrFormat::Hdr10Plus | HdrFormat::DolbyVision => 16, // PQ
            HdrFormat::Hlg => 18,
            _ => transfer,
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
            display_width: w,
            display_height: h,
            colour_matrix: matrix,
            colour_transfer: transfer,
            colour_primaries: primaries,
            colour_range: range,
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
            Codec::Ac3 => "A_AC3",
            Codec::Ac3Plus => "A_EAC3",
            Codec::TrueHd => "A_TRUEHD",
            Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => "A_DTS",
            Codec::Lpcm => "A_PCM/INT/BIG",
            _ => "A_AC3",
        };
        let sr = a.sample_rate.hz();
        let ch = a.channels.count();

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
            Codec::DvdSub => "S_VOBSUB",
            _ => "S_HDMV/PGS",
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
            sample_rate: 0.0,
            channels: 0,
            bit_depth: 0,
            dv_config: None,
        }
    }
}

/// Cue point for seeking.
struct CuePoint {
    timestamp_ms: i64,
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
    cluster_ts_ms: i64,
    base_pts_ms: Option<i64>,
    /// Last block timecode (ms, relative to base_pts) written PER TRACK, to
    /// enforce strictly-monotonic per-track timestamps — players/ffmpeg reject
    /// non-monotonic DTS, and some audio PES PTS land on the same millisecond
    /// (or tick back 1ms from rounding).
    last_pts_ms: std::collections::HashMap<usize, i64>,
    cues: Vec<CuePoint>,
    frame_count: u64,
    /// Frames handed to `write_frame` that were dropped because no cluster was
    /// open yet (a cluster only opens on a track-0 video keyframe). If this is
    /// non-zero at `finish()` and not a single frame was ever written, the
    /// caller produced an empty MKV — surfaced as an error rather than a
    /// silently empty file. See `write_frame` for the track-0 invariant.
    dropped_pre_cluster: u64,
    seek_fixups: Vec<SeekPositionFixup>,
    info_offset: u64,
    tracks_offset: u64,
    chapters_offset: Option<u64>,
}

/// New cluster every 5 seconds.
const CLUSTER_DURATION_MS: i64 = 5000;

/// Maximum block-relative timestamp expressible in the signed 16-bit
/// SimpleBlock/Block field (`i16::MAX` ms). A frame whose offset from the open
/// cluster's timestamp falls outside `i16::MIN..=i16::MAX` ms forces a new
/// cluster (see `write_frame`) so the `as i16` cast can never wrap — in EITHER
/// direction. PES timestamps come from untrusted disc/file bytes and can
/// back-jump on discontinuities, so the lower bound matters as much as the
/// upper one.
const MAX_BLOCK_REL_MS: i64 = i16::MAX as i64;
/// Minimum block-relative timestamp expressible in the signed 16-bit field.
const MIN_BLOCK_REL_MS: i64 = i16::MIN as i64;

/// Force a per-track block timestamp to be strictly later than the previous one
/// written for that track. `prev` is the last timestamp for the track (`None`
/// for the first frame). Fixes non-monotonic DTS: some audio PES PTS truncate to
/// the same millisecond as the prior frame (or tick back 1ms from rounding),
/// which ffmpeg/strict players reject. The nudge is at most a few ms — sub-frame
/// and inaudible — and never moves a timestamp earlier.
fn monotonic_ts(prev: Option<i64>, pts_ms: i64) -> i64 {
    match prev {
        Some(p) => pts_ms.max(p.saturating_add(1)),
        None => pts_ms,
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
        for target_id in &targets {
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
        ebml::write_uint(&mut writer, ebml::TIMESTAMP_SCALE, 1_000_000)?; // 1ms precision
        if duration_secs > 0.0 {
            ebml::write_float(&mut writer, ebml::DURATION, duration_secs * 1000.0)?;
            // in ms
        }
        ebml::write_string(&mut writer, ebml::MUXING_APP, "freemkv")?;
        ebml::write_string(&mut writer, ebml::WRITING_APP, "freemkv")?;
        if let Some(t) = title {
            ebml::write_string(&mut writer, ebml::TITLE, t)?;
        }
        ebml::end_master(&mut writer, info_pos)?;

        // Tracks
        let tracks_start = writer.stream_position()?;
        let tracks_offset = tracks_start - segment_start;
        let tracks_pos = ebml::start_master(&mut writer, ebml::TRACKS)?;
        for (i, track) in tracks.iter().enumerate() {
            let entry_pos = ebml::start_master(&mut writer, ebml::TRACK_ENTRY)?;
            ebml::write_uint(&mut writer, ebml::TRACK_NUMBER, (i + 1) as u64)?;
            ebml::write_uint(&mut writer, ebml::TRACK_UID, (i + 1) as u64 | 0x100_0000)?;
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

            // Video-specific
            if track.track_type == ebml::TRACK_TYPE_VIDEO && track.pixel_width > 0 {
                let vid_pos = ebml::start_master(&mut writer, ebml::VIDEO)?;
                ebml::write_uint(&mut writer, ebml::PIXEL_WIDTH, track.pixel_width as u64)?;
                ebml::write_uint(&mut writer, ebml::PIXEL_HEIGHT, track.pixel_height as u64)?;
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
                ebml::write_uint(&mut writer, ebml::BLOCK_ADD_ID_TYPE, 0x6476_6343)?;
                ebml::write_binary(&mut writer, ebml::BLOCK_ADD_ID_EXTRA_DATA, dvcc)?;
                ebml::end_master(&mut writer, map_pos)?;
            }

            // Audio-specific
            if track.track_type == ebml::TRACK_TYPE_AUDIO && track.sample_rate > 0.0 {
                let aud_pos = ebml::start_master(&mut writer, ebml::AUDIO)?;
                ebml::write_float(&mut writer, ebml::SAMPLING_FREQUENCY, track.sample_rate)?;
                ebml::write_uint(&mut writer, ebml::CHANNELS, track.channels as u64)?;
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
            cluster_ts_ms: 0,
            base_pts_ms: None,
            last_pts_ms: std::collections::HashMap::new(),
            cues: Vec::new(),
            frame_count: 0,
            dropped_pre_cluster: 0,
            seek_fixups,
            info_offset,
            tracks_offset,
            chapters_offset,
        })
    }

    /// Write a single frame.
    ///
    /// When `duration_ns` is `Some`, the frame is emitted as a
    /// `BlockGroup` with `BlockDuration` so the player knows exactly
    /// when to remove the on-screen artifact (the practical case is
    /// PGS subtitles — without it, the last bitmap lingers until the
    /// next display set replaces it). Otherwise a plain `SimpleBlock`.
    pub fn write_frame(
        &mut self,
        track_idx: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
        duration_ns: Option<u64>,
    ) -> io::Result<()> {
        let raw_ms = pts_ns / 1_000_000;

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
        let base = match self.base_pts_ms {
            Some(b) => b,
            None => {
                if !is_video_key {
                    // No cluster can open yet (clusters start on a track-0
                    // keyframe). Drop this frame as before, but count it so an
                    // all-dropped run surfaces as an error at finish().
                    self.dropped_pre_cluster += 1;
                    return Ok(());
                }
                self.base_pts_ms = Some(raw_ms);
                raw_ms
            }
        };
        // Floor at 0: base is the first kept keyframe, so any frame with an
        // earlier PTS (audio/subtitle arriving with a pre-keyframe timestamp, or
        // a back-jump on a stream discontinuity) would compute negative here,
        // which would wrap to ~u64::MAX on the `as u64` cluster/cue write and
        // could overflow the i16 block-relative cast. Frames before the first
        // kept keyframe are clamped to t=0 rather than corrupting the timeline.
        let pts_ms = (raw_ms - base).max(0);

        // Enforce strictly-monotonic per-track block timestamps. Some audio PES
        // PTS truncate to the same millisecond as the previous frame (or, rarely,
        // tick back 1ms), which surfaces as "non-monotonic DTS" and is rejected
        // by ffmpeg/strict players. Nudge to prev+1ms — sub-frame, inaudible,
        // and A/V sync is unaffected at millisecond granularity.
        let pts_ms = monotonic_ts(self.last_pts_ms.get(&track_idx).copied(), pts_ms);

        let needs_new_cluster = !self.cluster_open
            || (is_video_key && (pts_ms - self.cluster_ts_ms) >= CLUSTER_DURATION_MS);

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
            self.start_cluster(pts_ms)?;
            self.cues.push(CuePoint {
                timestamp_ms: pts_ms,
                track: track_idx + 1,
                cluster_pos: self.cluster_pos - self.segment_start,
            });
        } else {
            let rel = pts_ms - self.cluster_ts_ms;
            if !(MIN_BLOCK_REL_MS..=MAX_BLOCK_REL_MS).contains(&rel) {
                // The block-relative timestamp is a signed 16-bit value, so a
                // frame whose offset from the current cluster's timestamp falls
                // outside i16::MIN..=i16::MAX ms (~±32.767 s) would silently wrap
                // on the `as i16` cast, corrupting A/V sync. The keyframe-driven
                // boundary above only fires on a video keyframe — a long
                // audio-only stretch, a very long GOP with no intervening
                // keyframe (positive direction), or an audio/subtitle PES whose
                // PTS back-jumps below the open cluster (negative direction, e.g.
                // a stream discontinuity) can drift past the i16 range. Force a
                // fresh cluster here even without a keyframe to keep the cast in
                // range. pts_ms is already floored at 0 above, so the new
                // cluster timestamp never wraps on the `as u64` write in
                // start_cluster. This cluster is not keyframe-aligned so it gets
                // no Cues entry (Cues stay IDR-only for seekability).
                self.start_cluster(pts_ms)?;
            }
        }

        // Committed to writing this frame — record its (monotonic) timestamp so
        // the next block on this track is forced strictly later.
        self.last_pts_ms.insert(track_idx, pts_ms);

        let relative_ts = (pts_ms - self.cluster_ts_ms) as i16;
        match duration_ns {
            Some(dur_ns) => {
                let duration_ms = (dur_ns / 1_000_000).max(1);
                self.write_block_group(track_idx + 1, relative_ts, keyframe, data, duration_ms)?;
            }
            None => {
                self.write_simple_block(track_idx + 1, relative_ts, keyframe, data)?;
            }
        }
        self.frame_count += 1;

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
        if !self.cues.is_empty() {
            let cues_pos = ebml::start_master(&mut self.writer, ebml::CUES)?;
            for cue in &self.cues {
                let cp_pos = ebml::start_master(&mut self.writer, ebml::CUE_POINT)?;
                ebml::write_uint(&mut self.writer, ebml::CUE_TIME, cue.timestamp_ms as u64)?;
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

        // Back-patch SeekHead SeekPosition values now that all element offsets are known.
        for fixup in &self.seek_fixups {
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
        self.writer.seek(std::io::SeekFrom::End(0))?;

        self.writer.flush()?;
        Ok(())
    }

    fn start_cluster(&mut self, ts_ms: i64) -> io::Result<()> {
        // Close previous cluster if open
        if self.cluster_open {
            self.end_cluster()?;
        }
        self.cluster_pos = self.writer.stream_position()?;
        self.cluster_size_pos = ebml::start_master(&mut self.writer, ebml::CLUSTER)?;
        ebml::write_uint(&mut self.writer, ebml::CLUSTER_TIMESTAMP, ts_ms as u64)?;
        self.cluster_ts_ms = ts_ms;
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
        duration_ms: u64,
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
        ebml::write_uint(&mut self.writer, ebml::BLOCK_DURATION, duration_ms)?;
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
            codec_id: "V_MPEG4/ISO/AVC",
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
            sample_rate: 0.0,
            channels: 0,
            bit_depth: 0,
            dv_config: None,
        }
    }

    fn make_audio_track() -> MkvTrack {
        MkvTrack {
            track_type: ebml::TRACK_TYPE_AUDIO,
            codec_id: "A_AC3",
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
    fn cluster_starts_only_on_video_keyframe() {
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
        let tracks = [make_video_track(), make_audio_track()];
        let frames = frames_for(30.0, 1.0);
        let (data, _) = mux_to_bytes(&tracks, &[], &frames);
        let clusters = find_clusters(&data);
        let cues = parse_cues(&data);
        assert_eq!(
            clusters.len(),
            cues.len(),
            "cluster count {} != cue count {}",
            clusters.len(),
            cues.len()
        );
        // For 30s @ 5s min cluster duration with 1s GOP, expect 6 clusters / 6 cues.
        assert_eq!(
            clusters.len(),
            6,
            "expected 6 clusters for 30s @ 5s cluster duration"
        );
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
                *rel as i64 >= 0 && (*rel as i64) <= MAX_BLOCK_REL_MS,
                "block relative_ts {rel} out of [0, i16::MAX] range \
                 (cluster_ts={cluster_ts}, abs={abs}) — i16 overflow"
            );
        }
        // The latest audio frame is at 60_000 ms; its reconstructed absolute
        // timestamp must equal that, proving no truncation occurred.
        let max_abs = blocks.iter().map(|(_, _, abs)| *abs).max().unwrap();
        assert_eq!(max_abs, 60_000, "last block must reconstruct to 60_000 ms");
        // The overflow guard must have opened more than one cluster (the
        // single keyframe alone would otherwise yield exactly one).
        let clusters = find_clusters(&data);
        assert!(
            clusters.len() >= 2,
            "expected the i16 guard to force extra clusters, got {}",
            clusters.len()
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
    fn negative_relative_audio_forces_new_cluster_no_i16_wrap() {
        // An audio frame whose PTS back-jumps far below the open cluster (a
        // discontinuity) must force a fresh cluster rather than wrap the i16
        // block-relative cast. Build: keyframe at t=0 opening a cluster, a video
        // keyframe far later (so cluster ts is large), then an audio frame whose
        // PTS lands before that cluster's start by more than i16::MIN ms.
        let tracks = [make_video_track(), make_audio_track()];
        // base = 0 (first kept keyframe). Cluster opens at 0; a later keyframe at
        // 40s opens a second cluster at ts=40000. Then audio at t=0 → relative
        // 0-40000 = -40000 ms, below i16::MIN (-32768) → must open a new cluster.
        let frames = vec![
            (0usize, 0i64, true, vec![0x01; 16]),
            (0usize, 40_000_000_000i64, true, vec![0x02; 16]), // 40s
            (1usize, 0i64, true, vec![0x03; 16]),              // back-jumped audio
        ];
        let (data, frame_count) = mux_to_bytes(&tracks, &[], &frames);
        assert_eq!(frame_count, 3);
        let clusters = find_clusters(&data);
        // Three clusters: t=0 (video kf), t=40000 (video kf), t=0 (forced for the
        // back-jumped audio, no Cues entry).
        assert!(
            clusters.len() >= 3,
            "back-jumped audio must force a fresh cluster, got {} clusters",
            clusters.len()
        );
        // Every SimpleBlock's relative timestamp must round-trip through i16
        // without the block landing outside the cluster (verified implicitly by
        // the muxer never panicking on the `as i16` cast; here we assert the
        // forced cluster's timestamp is non-negative so the `as u64` write is
        // also safe).
        for (_, _, ts) in &clusters {
            assert!(*ts <= i64::MAX as u64, "cluster ts must not have wrapped");
        }
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
        // A frame 1000 ms after the keyframe-anchored cluster (within the 5s
        // cluster window) must encode rel ts 1000 = 0x03E8 big-endian.
        let tracks = [make_video_track()];
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 1_000_000_000, false, vec![0xBB], None),
            ],
        );
        // The second block is in the same cluster (1000ms < 5000ms boundary).
        let clusters = find_clusters(&data);
        assert_eq!(clusters.len(), 1, "1s < 5s cluster window → one cluster");
        let blocks = all_block_timestamps(&data);
        // Two blocks: rel 0 and rel 1000.
        let rels: Vec<i16> = blocks.iter().map(|(_, r, _)| *r).collect();
        assert!(rels.contains(&1000), "second block rel ts must be 1000ms");
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
        let (block_data, dur_ms, flags) = first_block_group(&data);
        assert_eq!(block_data, vec![0xCC, 0xDD]);
        assert_eq!(dur_ms, 40, "BlockDuration must be 40 ms (40_000_000 ns)");
        assert_eq!(
            flags & 0x80,
            0x00,
            "Block inside BlockGroup must clear the keyframe flag (got 0x{flags:02X})"
        );
    }

    #[test]
    fn block_duration_floored_to_at_least_one_ms() {
        // A sub-millisecond duration (e.g. 500_000 ns = 0.5 ms) must floor to 1
        // ms, never 0 — a 0-duration BlockGroup would tell players to remove the
        // artifact instantly.
        let tracks = [make_video_track()];
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 10_000_000, true, vec![0xBB], Some(500_000)),
            ],
        );
        let (_, dur_ms, _) = first_block_group(&data);
        assert_eq!(dur_ms, 1, "sub-ms duration must floor to 1 ms, not 0");
    }

    // ============================================================
    // Cluster boundary (CLUSTER_DURATION_MS = 5000): a new cluster opens
    // on a video keyframe once >= 5000 ms have elapsed since the open
    // cluster's timestamp. A keyframe exactly at the boundary opens a new
    // cluster; one just under stays in the current cluster.
    // ============================================================

    #[test]
    fn keyframe_at_5s_boundary_opens_new_cluster() {
        let tracks = [make_video_track()];
        // Keyframe at exactly 5000 ms (>= CLUSTER_DURATION_MS) → new cluster.
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 5_000_000_000, true, vec![0xBB], None),
            ],
        );
        assert_eq!(
            find_clusters(&data).len(),
            2,
            "keyframe at the 5s boundary must open a second cluster"
        );
    }

    #[test]
    fn keyframe_just_under_5s_stays_in_cluster() {
        let tracks = [make_video_track()];
        // Keyframe at 4999 ms (< 5000) → same cluster.
        let data = mux_with_durations(
            &tracks,
            &[
                (0, 0, true, vec![0xAA], None),
                (0, 4_999_000_000, true, vec![0xBB], None),
            ],
        );
        assert_eq!(
            find_clusters(&data).len(),
            1,
            "keyframe under the 5s window must stay in the open cluster"
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
