//! MPLS playlist parser — Blu-ray movie playlists.
//!
//! Each .mpls file in BDMV/PLAYLIST/ defines a title.
//! Contains play items (clips) with in/out timestamps,
//! stream info (video, audio, subtitle tracks).
//!
//! Reference: https://github.com/lw/BluRay/wiki/MPLS

use crate::error::{Error, Result};

/// Parsed MPLS playlist.
#[derive(Debug)]
pub struct Playlist {
    /// MPLS version (e.g. "0200" or "0300")
    pub version: String,
    /// Play items in playback order
    pub play_items: Vec<PlayItem>,
    /// Streams from the first play item's STN table
    pub streams: Vec<StreamEntry>,
}

/// A play item — one clip reference with in/out times.
#[derive(Debug)]
pub struct PlayItem {
    /// Clip filename without extension (e.g. "00001")
    pub clip_id: String,
    /// In-time in 45kHz ticks
    pub in_time: u32,
    /// Out-time in 45kHz ticks
    pub out_time: u32,
    /// Connection condition (1=seamless, 5/6=non-seamless)
    pub connection_condition: u8,
}

/// A stream entry from the STN table.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Stream category: 1=video, 2=audio, 3=PG subtitle, 4=IG, 5=secondary audio, 6=secondary video, 7=DV EL
    pub stream_type: u8,
    /// MPEG-TS PID
    pub pid: u16,
    /// Coding type (0x24=HEVC, 0x1B=H264, 0x83=TrueHD, etc.)
    pub coding_type: u8,
    /// Video format (1=480i, 4=1080i, 5=720p, 6=1080p, 8=2160p)
    pub video_format: u8,
    /// Video frame rate (1=23.976, 2=24, 3=25, 4=29.97, 6=50, 7=59.94)
    pub video_rate: u8,
    /// Audio channel layout (1=mono, 3=stereo, 6=5.1, 12=7.1)
    pub audio_format: u8,
    /// Audio sample rate (1=48kHz, 4=96kHz, 5=192kHz)
    pub audio_rate: u8,
    /// ISO 639-2 language code (e.g. "eng")
    pub language: String,
    /// HDR dynamic range (0=SDR, 1=HDR10, 2=Dolby Vision)
    pub dynamic_range: u8,
    /// Color space (0=unknown, 1=BT.709, 2=BT.2020)
    pub color_space: u8,
    /// Whether this is a secondary stream (commentary, PiP, DV EL)
    pub secondary: bool,
}

/// Parse an MPLS file from raw bytes.
pub fn parse(data: &[u8]) -> Result<Playlist> {
    if data.len() < 40 {
        return Err(Error::DiscError { detail: "MPLS too short".into() });
    }
    if &data[0..4] != b"MPLS" {
        return Err(Error::DiscError { detail: "not an MPLS file".into() });
    }

    let version = String::from_utf8_lossy(&data[4..8]).to_string();
    let playlist_start = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;

    if playlist_start + 10 > data.len() {
        return Err(Error::DiscError { detail: "MPLS playlist offset out of range".into() });
    }

    let pl = &data[playlist_start..];
    let num_play_items = u16::from_be_bytes([pl[6], pl[7]]) as usize;

    let mut play_items = Vec::with_capacity(num_play_items);
    let mut streams = Vec::new();
    let mut pos = 10;

    for item_idx in 0..num_play_items {
        if pos + 2 > pl.len() { break; }
        let item_length = u16::from_be_bytes([pl[pos], pl[pos + 1]]) as usize;
        if pos + 2 + item_length > pl.len() { break; }

        let item = &pl[pos + 2..pos + 2 + item_length];
        if item.len() < 20 { pos += 2 + item_length; continue; }

        let clip_id = String::from_utf8_lossy(&item[0..5]).to_string();
        let connection_condition = item[9] & 0x0F;
        let in_time = u32::from_be_bytes([item[12], item[13], item[14], item[15]]);
        let out_time = u32::from_be_bytes([item[16], item[17], item[18], item[19]]);

        // Parse STN table from the first play item
        // PlayItem layout after out_time:
        //   [20:28] UO_mask_table (8 bytes)
        //   [28]    misc flags (1 byte)
        //   [29]    still_mode (1 byte)
        //   [30:32] still_time (2 bytes)
        //   [32:]   STN_table
        const STN_OFFSET: usize = 32;
        if item_idx == 0 && item.len() > STN_OFFSET + 16 {
            // STN header: length(2) + reserved(2) + counts(8) + reserved(4) = 16 bytes
            let n_video = item[STN_OFFSET + 4] as usize;
            let n_audio = item[STN_OFFSET + 5] as usize;
            let n_pg = item[STN_OFFSET + 6] as usize;
            let n_ig = item[STN_OFFSET + 7] as usize;
            let n_sec_audio = item[STN_OFFSET + 8] as usize;
            let n_sec_video = item[STN_OFFSET + 9] as usize;
            let _n_pip_pg = item[STN_OFFSET + 10] as usize;
            let n_dv = item[STN_OFFSET + 11] as usize;

            let mut spos = STN_OFFSET + 16;

            // Primary video
            for _ in 0..n_video {
                if let Some((entry, next)) = parse_stream_entry(item, spos, 1) {
                    streams.push(entry);
                    spos = next;
                } else { break; }
            }
            // Primary audio
            for _ in 0..n_audio {
                if let Some((entry, next)) = parse_stream_entry(item, spos, 2) {
                    streams.push(entry);
                    spos = next;
                } else { break; }
            }
            // PG subtitles
            for _ in 0..n_pg {
                if let Some((entry, next)) = parse_stream_entry(item, spos, 3) {
                    streams.push(entry);
                    spos = next;
                } else { break; }
            }
            // IG (skip but advance)
            for _ in 0..n_ig {
                if let Some((_, next)) = parse_stream_entry(item, spos, 4) {
                    spos = next;
                } else { break; }
            }
            // Secondary audio
            for _ in 0..n_sec_audio {
                if let Some((mut entry, next)) = parse_stream_entry(item, spos, 2) {
                    entry.stream_type = 5;
                    entry.secondary = true;
                    streams.push(entry);
                    // Skip extra ref bytes: num_refs(1) + reserved(1) + refs + padding
                    if next < item.len() {
                        let n_refs = item[next] as usize;
                        spos = next + 2 + n_refs + (n_refs % 2);
                    } else { spos = next; }
                } else { break; }
            }
            // Secondary video (PiP)
            for _ in 0..n_sec_video {
                if let Some((mut entry, next)) = parse_stream_entry(item, spos, 1) {
                    entry.stream_type = 6;
                    entry.secondary = true;
                    streams.push(entry);
                    // Skip extra ref bytes (audio refs + PG refs)
                    if next + 2 < item.len() {
                        let n_arefs = item[next] as usize;
                        let after_arefs = next + 2 + n_arefs + (n_arefs % 2);
                        if after_arefs < item.len() {
                            let n_prefs = item[after_arefs] as usize;
                            spos = after_arefs + 2 + n_prefs + (n_prefs % 2);
                        } else { spos = after_arefs; }
                    } else { spos = next; }
                } else { break; }
            }
            // Dolby Vision enhancement layer
            for _ in 0..n_dv {
                if let Some((mut entry, next)) = parse_stream_entry(item, spos, 1) {
                    entry.stream_type = 7;
                    entry.secondary = true;
                    streams.push(entry);
                    spos = next;
                } else { break; }
            }
        }

        play_items.push(PlayItem {
            clip_id,
            in_time,
            out_time,
            connection_condition,
        });

        pos += 2 + item_length;
    }

    Ok(Playlist {
        version,
        play_items,
        streams,
    })
}

/// Parse one stream entry from the STN table.
/// Returns (StreamEntry, next position) or None.
fn parse_stream_entry(item: &[u8], pos: usize, stream_type: u8) -> Option<(StreamEntry, usize)> {
    if pos + 2 > item.len() { return None; }

    // Stream entry: length(1) + data
    let se_len = item[pos] as usize;
    let se_end = pos + 1 + se_len;
    if se_end > item.len() { return None; }

    // PID from stream entry (type 0x01 = PlayItem stream: PID at bytes 2-3)
    let pid = if item[pos + 1] == 0x01 && pos + 4 <= item.len() {
        u16::from_be_bytes([item[pos + 2], item[pos + 3]])
    } else {
        0
    };

    // Stream attributes: length(1) + coding_type(1) + format-specific data
    if se_end + 2 > item.len() { return None; }
    let sa_len = item[se_end] as usize;
    let sa_end = se_end + 1 + sa_len;
    if sa_end > item.len() || sa_len < 1 { return None; }

    let sa = &item[se_end + 1..se_end + 1 + sa_len];
    let coding_type = sa[0];

    let mut video_format = 0u8;
    let mut video_rate = 0u8;
    let mut audio_format = 0u8;
    let mut audio_rate = 0u8;
    let mut dynamic_range = 0u8;
    let mut color_space_val = 0u8;
    let mut language = String::new();

    match stream_type {
        1 => {
            // Video: coding_type(1) + format_rate(1) + [hdr_info(1) if HEVC]
            if sa.len() >= 2 {
                video_format = (sa[1] >> 4) & 0x0F;
                video_rate = sa[1] & 0x0F;
            }
            if coding_type == 0x24 && sa.len() > 2 {
                dynamic_range = (sa[2] >> 4) & 0x0F;
                color_space_val = sa[2] & 0x0F;
            }
        }
        2 => {
            // Audio: coding_type(1) + format_rate(1) + language(3)
            if sa.len() >= 2 {
                audio_format = (sa[1] >> 4) & 0x0F;
                audio_rate = sa[1] & 0x0F;
            }
            if sa.len() >= 5 {
                language = String::from_utf8_lossy(&sa[2..5]).to_string();
            }
        }
        3 | 4 => {
            // PG/IG: coding_type(1) + language(3)
            if sa.len() >= 4 {
                language = String::from_utf8_lossy(&sa[1..4]).to_string();
            }
        }
        5 => {
            // Secondary audio: same as primary audio
            if sa.len() >= 2 {
                audio_format = (sa[1] >> 4) & 0x0F;
                audio_rate = sa[1] & 0x0F;
            }
            if sa.len() >= 5 {
                language = String::from_utf8_lossy(&sa[2..5]).to_string();
            }
        }
        6 | 7 => {
            // Secondary video: same as primary video
            if sa.len() >= 2 {
                video_format = (sa[1] >> 4) & 0x0F;
                video_rate = sa[1] & 0x0F;
            }
            if coding_type == 0x24 && sa.len() > 2 {
                dynamic_range = (sa[2] >> 4) & 0x0F;
                color_space_val = sa[2] & 0x0F;
            }
        }
        _ => {}
    }

    Some((StreamEntry {
        stream_type,
        pid,
        coding_type,
        video_format,
        video_rate,
        audio_format,
        audio_rate,
        language,
        dynamic_range,
        color_space: color_space_val,
        secondary: false,
    }, sa_end))
}
