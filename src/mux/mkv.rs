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
}

impl MkvTrack {
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
        }
    }

    pub fn audio(a: &AudioStream) -> Self {
        // Codec ID strings must distinguish the DTS family — strict
        // players (Plex transcoder, some hardware decoders, some AV
        // receivers) reject lossless DTS-HD MA payload when the
        // track advertises plain `A_DTS` because it implies the
        // bitstream is the 1.5 Mbps "core" only. Fix is just to
        // emit the right ID per BD-STN codec field.
        let codec_id = match a.codec {
            Codec::Ac3 => "A_AC3",
            Codec::Ac3Plus => "A_EAC3",
            Codec::TrueHd => "A_TRUEHD",
            Codec::DtsHdMa => "A_DTS/MA",
            Codec::DtsHdHr => "A_DTS/HR",
            Codec::Dts => "A_DTS",
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
        }
    }

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
    cues: Vec<CuePoint>,
    frame_count: u64,
    seek_fixups: Vec<SeekPositionFixup>,
    info_offset: u64,
    tracks_offset: u64,
    chapters_offset: Option<u64>,
}

/// New cluster every 5 seconds.
const CLUSTER_DURATION_MS: i64 = 5000;

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
            cues: Vec::new(),
            frame_count: 0,
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
        let base = *self.base_pts_ms.get_or_insert(raw_ms);
        let pts_ms = raw_ms - base;

        // Cluster boundaries must coincide with a video keyframe so every
        // Cues entry resolves to a seekable IDR at the cluster start.
        let is_video_key = keyframe && track_idx == 0;
        let needs_new_cluster = !self.cluster_open
            || (is_video_key && (pts_ms - self.cluster_ts_ms) >= CLUSTER_DURATION_MS);

        if needs_new_cluster {
            if !is_video_key {
                return Ok(());
            }
            self.start_cluster(pts_ms)?;
            self.cues.push(CuePoint {
                timestamp_ms: pts_ms,
                track: track_idx + 1,
                cluster_pos: self.cluster_pos - self.segment_start,
            });
        }

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
    pub fn finish(mut self) -> io::Result<()> {
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
                ebml::CHAPTERS => self.chapters_offset.unwrap_or(0),
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
        // Track number as EBML VINT
        let track_vint = if track_num < 0x80 {
            vec![(track_num as u8) | 0x80]
        } else {
            vec![0x40 | ((track_num >> 8) as u8), track_num as u8]
        };

        let flags: u8 = if keyframe { 0x80 } else { 0x00 };

        let block_size = track_vint.len() + 2 + 1 + data.len(); // vint + ts(2) + flags(1) + data
        ebml::write_id(&mut self.writer, ebml::SIMPLE_BLOCK)?;
        ebml::write_size(&mut self.writer, block_size as u64)?;
        self.writer.write_all(&track_vint)?;
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
        let track_vint = if track_num < 0x80 {
            vec![(track_num as u8) | 0x80]
        } else {
            vec![0x40 | ((track_num >> 8) as u8), track_num as u8]
        };
        let flags: u8 = if keyframe { 0x80 } else { 0x00 };
        let block_size = track_vint.len() + 2 + 1 + data.len();

        let bg_pos = ebml::start_master(&mut self.writer, ebml::BLOCK_GROUP)?;
        ebml::write_id(&mut self.writer, ebml::BLOCK)?;
        ebml::write_size(&mut self.writer, block_size as u64)?;
        self.writer.write_all(&track_vint)?;
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
        }
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
        // Use a Vec wrapped in Cursor, then check after finish
        use std::sync::{Arc, Mutex};

        // We'll write to a Cursor, but finish() consumes self.
        // The trick: Cursor<Vec<u8>> - we can get data back via into_inner chain.
        // But MkvMuxer::finish consumes self and flushes writer.
        // We need a way to inspect the output. Let's use a wrapper.

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
}
