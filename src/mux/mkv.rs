//! Matroska (MKV) muxer.
//!
//! Writes EBML header, Segment with tracks, clusters, and cues.
//! Designed for streaming writes: clusters are written as data arrives,
//! cues and seek head are finalized at the end.

use super::ebml;
use crate::disc::{AudioStream, Codec, SubtitleStream, VideoStream};
use std::io::{self, Seek, SeekFrom, Write};

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
        let (w, h) = parse_resolution(&v.resolution);
        Self {
            track_type: ebml::TRACK_TYPE_VIDEO,
            codec_id,
            language: "und".into(),
            name: v.label.clone(),
            codec_private: None, // filled later by parser
            is_default: !v.secondary,
            is_forced: false,
            pixel_width: w,
            pixel_height: h,
            sample_rate: 0.0,
            channels: 0,
            bit_depth: 0,
        }
    }

    pub fn audio(a: &AudioStream) -> Self {
        let codec_id = match a.codec {
            Codec::Ac3 => "A_AC3",
            Codec::Ac3Plus => "A_EAC3",
            Codec::TrueHd => "A_TRUEHD",
            Codec::DtsHdMa | Codec::DtsHdHr | Codec::Dts => "A_DTS",
            Codec::Lpcm => "A_PCM/INT/BIG",
            _ => "A_AC3",
        };
        let sr = parse_sample_rate(&a.sample_rate);
        let ch = parse_channels(&a.channels);
        Self {
            track_type: ebml::TRACK_TYPE_AUDIO,
            codec_id,
            language: a.language.clone(),
            name: a.label.clone(),
            codec_private: None,
            is_default: !a.secondary,
            is_forced: false,
            pixel_width: 0,
            pixel_height: 0,
            sample_rate: sr,
            channels: ch,
            bit_depth: 0,
        }
    }

    pub fn subtitle(s: &SubtitleStream) -> Self {
        Self {
            track_type: ebml::TRACK_TYPE_SUBTITLE,
            codec_id: "S_HDMV/PGS",
            language: s.language.clone(),
            name: String::new(),
            codec_private: None,
            is_default: false,
            is_forced: s.forced,
            pixel_width: 0,
            pixel_height: 0,
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

/// MKV muxer. Call write_frame() for each frame, then finish() at the end.
pub struct MkvMuxer<W: Write + Seek> {
    writer: W,
    segment_start: u64,
    cluster_open: bool,
    cluster_pos: u64,
    cluster_size_pos: u64,
    cluster_ts_ms: i64,
    cues: Vec<CuePoint>,
    frame_count: u64,
    /// File positions of codecPrivate placeholders (track_idx → offset, max_size).
    /// Used to seek back and fill in SPS/PPS after first keyframe.
    codec_private_slots: Vec<Option<(u64, usize)>>,
    codec_private_filled: Vec<bool>,
}

/// New cluster every 5 seconds.
const CLUSTER_DURATION_MS: i64 = 5000;

impl<W: Write + Seek> MkvMuxer<W> {
    /// Create a new MKV muxer: writes EBML header, Segment start, Info, Tracks.
    pub fn new(
        mut writer: W,
        tracks: &[MkvTrack],
        title: Option<&str>,
        duration_secs: f64,
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

        // Info
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
        let mut codec_private_slots: Vec<Option<(u64, usize)>> = Vec::new();
        let mut codec_private_filled: Vec<bool> = Vec::new();
        let tracks_pos = ebml::start_master(&mut writer, ebml::TRACKS)?;
        for (i, track) in tracks.iter().enumerate() {
            let entry_pos = ebml::start_master(&mut writer, ebml::TRACK_ENTRY)?;
            ebml::write_uint(&mut writer, ebml::TRACK_NUMBER, (i + 1) as u64)?;
            ebml::write_uint(&mut writer, ebml::TRACK_UID, (i + 1) as u64)?;
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
                codec_private_slots.push(None); // already filled
                codec_private_filled.push(true);
            } else if track.track_type == ebml::TRACK_TYPE_VIDEO {
                // Reserve space for codecPrivate — will be filled after first keyframe
                // Reserve 256 bytes (enough for SPS+PPS or VPS+SPS+PPS)
                let cp_pos = writer.stream_position()?;
                let placeholder = vec![0u8; 256];
                ebml::write_binary(&mut writer, ebml::CODEC_PRIVATE, &placeholder)?;
                codec_private_slots.push(Some((cp_pos, 256)));
                codec_private_filled.push(false);
            } else {
                codec_private_slots.push(None);
                codec_private_filled.push(true);
            }

            // Video-specific
            if track.track_type == ebml::TRACK_TYPE_VIDEO && track.pixel_width > 0 {
                let vid_pos = ebml::start_master(&mut writer, ebml::VIDEO)?;
                ebml::write_uint(&mut writer, ebml::PIXEL_WIDTH, track.pixel_width as u64)?;
                ebml::write_uint(&mut writer, ebml::PIXEL_HEIGHT, track.pixel_height as u64)?;
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

        Ok(Self {
            writer,
            segment_start,
            cluster_open: false,
            cluster_pos: 0,
            cluster_size_pos: 0,
            cluster_ts_ms: 0,
            cues: Vec::new(),
            frame_count: 0,
            codec_private_slots,
            codec_private_filled,
        })
    }

    /// Write a single frame.
    pub fn write_frame(
        &mut self,
        track_idx: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
    ) -> io::Result<()> {
        let pts_ms = pts_ns / 1_000_000;

        // Start new cluster if needed
        if !self.cluster_open || (pts_ms - self.cluster_ts_ms) >= CLUSTER_DURATION_MS {
            if self.cluster_open {
                // Close current cluster (it's a master with unknown size — we use known size)
                // Actually, for streaming we keep clusters open-ended. Just start a new one.
            }
            self.start_cluster(pts_ms)?;

            // Add cue point at cluster start for keyframes (video track 0)
            if keyframe && track_idx == 0 {
                self.cues.push(CuePoint {
                    timestamp_ms: pts_ms,
                    track: track_idx + 1,
                    cluster_pos: self.cluster_pos - self.segment_start,
                });
            }
        }

        // Write SimpleBlock
        let relative_ts = (pts_ms - self.cluster_ts_ms) as i16;
        self.write_simple_block(track_idx + 1, relative_ts, keyframe, data)?;
        self.frame_count += 1;

        Ok(())
    }

    /// Finish the MKV file: write Cues element.
    pub fn finish(mut self) -> io::Result<()> {
        // Close final cluster
        self.end_cluster()?;

        // Write Cues
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

        self.writer.flush()?;
        Ok(())
    }

    /// Fill in a deferred codecPrivate for a track.
    /// Seeks back to the placeholder, writes the actual data, restores position.
    pub fn fill_codec_private(&mut self, track_idx: usize, data: &[u8]) -> io::Result<()> {
        if track_idx >= self.codec_private_filled.len() || self.codec_private_filled[track_idx] {
            return Ok(());
        }
        if let Some((pos, max_size)) = self.codec_private_slots[track_idx] {
            if data.len() > max_size {
                // Data too large for reserved space — can't fill in place
                // This shouldn't happen with 256 bytes reserved
                return Ok(());
            }
            let current = self.writer.stream_position()?;
            self.writer.seek(SeekFrom::Start(pos))?;
            // Rewrite: element ID + size + data + zero-pad remainder
            let mut padded = data.to_vec();
            padded.resize(max_size, 0);
            ebml::write_binary(&mut self.writer, ebml::CODEC_PRIVATE, &padded)?;
            self.writer.seek(SeekFrom::Start(current))?;
            self.codec_private_filled[track_idx] = true;
        }
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
}

// ============================================================
// Helpers
// ============================================================

fn parse_resolution(s: &str) -> (u32, u32) {
    if s.contains("2160") {
        (3840, 2160)
    } else if s.contains("1080") {
        (1920, 1080)
    } else if s.contains("720") {
        (1280, 720)
    } else if s.contains("576") {
        (720, 576)
    } else if s.contains("480") {
        (720, 480)
    } else {
        (1920, 1080)
    }
}

fn parse_sample_rate(s: &str) -> f64 {
    if s.contains("96") {
        96000.0
    } else if s.contains("192") {
        192000.0
    } else {
        48000.0
    }
}

fn parse_channels(s: &str) -> u8 {
    if s.contains("7.1") {
        8
    } else if s.contains("5.1") {
        6
    } else if s.contains("stereo") || s.contains("2.0") {
        2
    } else if s.contains("mono") {
        1
    } else {
        6
    }
}
