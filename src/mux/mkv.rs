//! Matroska (MKV) muxer.
//!
//! Writes EBML header, Segment with tracks, clusters, and cues.
//! Designed for streaming writes: clusters are written as data arrives,
//! cues and seek head are finalized at the end.

use super::ebml;
use crate::disc::{AudioStream, Chapter, Codec, SubtitleStream, VideoStream};
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
    /// Create a new MKV muxer: writes EBML header, Segment start, Info, Tracks, Chapters.
    pub fn new(
        writer: W,
        tracks: &[MkvTrack],
        title: Option<&str>,
        duration_secs: f64,
    ) -> io::Result<Self> {
        Self::new_with_chapters(writer, tracks, title, duration_secs, &[])
    }

    /// Create a new MKV muxer with chapters: writes EBML header, Segment start, Info, Tracks, Chapters.
    pub fn new_with_chapters(
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

        // Chapters
        if !chapters.is_empty() {
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
    if s.contains("192") {
        192000.0
    } else if s.contains("96") {
        96000.0
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
            sample_rate: 48000.0,
            channels: 6,
            bit_depth: 0,
        }
    }

    #[test]
    fn mkv_writes_ebml_header() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let muxer = MkvMuxer::new(buf, &tracks, Some("Test"), 120.0).unwrap();
        let data = muxer.writer.into_inner();
        // EBML header element ID: 0x1A45DFA3
        assert!(data.len() >= 4);
        assert_eq!(&data[0..4], &[0x1A, 0x45, 0xDF, 0xA3]);
    }

    #[test]
    fn mkv_writes_segment() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let muxer = MkvMuxer::new(buf, &tracks, None, 0.0).unwrap();
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
        let mut muxer = MkvMuxer::new(buf, &tracks, None, 60.0).unwrap();
        muxer
            .write_frame(0, 0, true, &[0xDE, 0xAD, 0xBE, 0xEF])
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
        let mut muxer = MkvMuxer::new(writer, &tracks, Some("Cue Test"), 60.0).unwrap();
        muxer.write_frame(0, 0, true, &[0x01, 0x02, 0x03]).unwrap();
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
        let mut muxer = MkvMuxer::new(buf, &tracks, Some("Multi"), 120.0).unwrap();
        // Write frames to both tracks
        muxer.write_frame(0, 0, true, &[0x00, 0x00, 0x01]).unwrap();
        muxer.write_frame(1, 0, false, &[0x0B, 0x77, 0x00]).unwrap();
        muxer
            .write_frame(0, 40_000_000, false, &[0x00, 0x00, 0x01])
            .unwrap();
        muxer
            .write_frame(1, 32_000_000, false, &[0x0B, 0x77, 0x01])
            .unwrap();
        // Should not panic
        let data = muxer.writer.into_inner();
        assert!(data.len() > 100, "output too small for multi-track MKV");
    }

    #[test]
    fn mkv_keyframe_flag() {
        let buf = Cursor::new(Vec::new());
        let tracks = [make_video_track()];
        let mut muxer = MkvMuxer::new(buf, &tracks, None, 10.0).unwrap();

        // Record position before first frame
        let pos_before_kf = muxer.writer.position();
        muxer.write_frame(0, 0, true, &[0xAA]).unwrap();
        let pos_after_kf = muxer.writer.position();

        muxer.write_frame(0, 1_000_000, false, &[0xBB]).unwrap();
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
        let muxer =
            MkvMuxer::new_with_chapters(buf, &tracks, Some("Chapter Test"), 900.0, &chapters)
                .unwrap();
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
        let muxer = MkvMuxer::new(buf, &tracks, Some("No Chapters"), 60.0).unwrap();
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
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0).unwrap();
        let data = muxer.writer.into_inner();

        // FlagDefault ID is 0x88. When is_default is true, FlagDefault is NOT written
        // (MKV default is 1). When is_default is false, FlagDefault=0 IS written.
        // So we should find at least one FlagDefault element (for the non-default track).
        let flag_default_id = ebml::FLAG_DEFAULT.to_be_bytes();
        let needle = &[flag_default_id[3]]; // 0x88 is a 1-byte ID
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
            codec_data: None,
        });
        assert!(forced_sub.is_forced);

        let buf = Cursor::new(Vec::new());
        let tracks = [video, forced_sub];
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0).unwrap();
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
            codec_data: None,
        });
        assert!(!sub.is_forced);

        let buf = Cursor::new(Vec::new());
        let tracks = [video, sub];
        let muxer = MkvMuxer::new(buf, &tracks, None, 60.0).unwrap();
        let data = muxer.writer.into_inner();

        // FlagForced should NOT be written for non-forced tracks
        assert!(
            find_id(&data, ebml::FLAG_FORCED).is_none(),
            "FlagForced element should not be present for non-forced subtitle"
        );
    }
}
