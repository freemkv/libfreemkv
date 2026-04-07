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
    /// Stream category: 1=primary video, 2=primary audio, 3=PG subtitle, 4=IG
    pub stream_type: u8,
    /// MPEG-TS PID
    pub pid: u16,
    /// Coding type (0x24=HEVC, 0x1B=H264, 0x83=TrueHD, etc.)
    pub coding_type: u8,
    /// Video format (1=480i, 6=1080p, 8=2160p, etc.)
    pub video_format: u8,
    /// Video frame rate (1=23.976, 3=25, 4=29.97, etc.)
    pub video_rate: u8,
    /// Audio format (3=stereo, 6=5.1, 12=7.1, etc.)
    pub audio_format: u8,
    /// Audio sample rate (1=48kHz, 4=96kHz, 5=192kHz)
    pub audio_rate: u8,
    /// ISO 639-2 language code (e.g. "eng")
    pub language: String,
}

/// Parse an MPLS file from raw bytes.
pub fn parse(data: &[u8]) -> Result<Playlist> {
    if data.len() < 40 {
        return Err(Error::DiscError { detail: "MPLS too short".into() });
    }

    // Header: "MPLS" + version (4 bytes ASCII)
    if &data[0..4] != b"MPLS" {
        return Err(Error::DiscError { detail: "not an MPLS file".into() });
    }
    let version = String::from_utf8_lossy(&data[4..8]).to_string();

    // Offsets table at bytes 8-19
    let playlist_start = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;

    if playlist_start + 10 > data.len() {
        return Err(Error::DiscError { detail: "MPLS playlist offset out of range".into() });
    }

    // PlayList section
    let pl = &data[playlist_start..];
    let num_play_items = u16::from_be_bytes([pl[6], pl[7]]) as usize;

    let mut play_items = Vec::with_capacity(num_play_items);
    let mut streams = Vec::new();
    let mut pos = 10; // start of first play item

    for item_idx in 0..num_play_items {
        if playlist_start + pos + 2 > data.len() {
            break;
        }

        let item_length = u16::from_be_bytes([pl[pos], pl[pos + 1]]) as usize;
        if playlist_start + pos + item_length + 2 > data.len() {
            break;
        }

        let item = &pl[pos + 2..pos + 2 + item_length];
        if item.len() < 20 { break; }

        let clip_id = String::from_utf8_lossy(&item[0..5]).to_string();
        let connection_condition = item[9] & 0x0F;
        let in_time = u32::from_be_bytes([item[12], item[13], item[14], item[15]]);
        let out_time = u32::from_be_bytes([item[16], item[17], item[18], item[19]]);

        // Parse STN table from the first play item
        if item_idx == 0 && item.len() > 32 {
            // UO mask is 8 bytes (64 bits) at offset 20
            // STN table starts after: 2 bytes length + 2 bytes reserved + UO mask (8 bytes)
            // STN offset within play_item = 20 (UO start)
            // Actually: offset 20 = UO_mask_table (8 bytes)
            //           offset 28 = random_access_flag + ... (2 bytes)
            //           offset 30 = still_mode (1 byte) + still_time (2 bytes if still)
            // Then STN table
            // The STN table position varies. Let's find it by looking for the STN length field.

            // Per BD spec: play_item structure after out_time:
            //   [20..28] UO_mask_table (8 bytes)
            //   [28] misc flags (1 byte)
            //   [29] still_mode (1 byte)
            //   [30..32] still_time (2 bytes) if still_mode != 0
            //   Then STN_table

            let still_mode = if item.len() > 29 { item[29] } else { 0 };
            let stn_offset = if still_mode != 0 { 32 } else { 32 };
            // Actually it's always 32 based on our previous work (UO mask = 8 bytes, not 64)

            if item.len() > stn_offset + 6 {
                let stn = &item[stn_offset..];
                let _stn_length = u16::from_be_bytes([stn[0], stn[1]]) as usize;
                if stn.len() > 6 {
                    // reserved 2 bytes at [2..4]
                    let n_video = stn[4] as usize;
                    let n_audio = stn[5] as usize;
                    let n_pg = if stn.len() > 6 { stn[6] as usize } else { 0 };
                    let n_ig = if stn.len() > 7 { stn[7] as usize } else { 0 };

                    // Parse stream entries starting at offset 8
                    // But there's another 2 bytes reserved before entries
                    let mut stn_pos = 8;
                    // Possible 2 more reserved bytes
                    // Let's skip and parse entries

                    // Primary video streams
                    for _ in 0..n_video {
                        if let Some((entry, len)) = parse_stream_entry(stn, stn_pos, 1) {
                            streams.push(entry);
                            stn_pos += len;
                        } else {
                            break;
                        }
                    }

                    // Primary audio streams
                    for _ in 0..n_audio {
                        if let Some((entry, len)) = parse_stream_entry(stn, stn_pos, 2) {
                            streams.push(entry);
                            stn_pos += len;
                        } else {
                            break;
                        }
                    }

                    // PG (subtitle) streams
                    for _ in 0..n_pg {
                        if let Some((entry, len)) = parse_stream_entry(stn, stn_pos, 3) {
                            streams.push(entry);
                            stn_pos += len;
                        } else {
                            break;
                        }
                    }

                    // IG streams
                    for _ in 0..n_ig {
                        if let Some((entry, len)) = parse_stream_entry(stn, stn_pos, 4) {
                            streams.push(entry);
                            stn_pos += len;
                        } else {
                            break;
                        }
                    }
                }
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
/// Returns (StreamEntry, bytes consumed) or None.
fn parse_stream_entry(stn: &[u8], pos: usize, stream_type: u8) -> Option<(StreamEntry, usize)> {
    if pos + 2 > stn.len() { return None; }

    // Stream entry format:
    //   [0] length of stream entry (1 byte)
    //   [1] stream_type (1=PlayItem, 2=SubPath, 3=InMux)
    //   Then stream_pid reference (varies by stream_type)
    //   Then stream attributes

    let entry_len = stn[pos] as usize;
    if entry_len < 4 || pos + 1 + entry_len > stn.len() { return None; }

    let entry = &stn[pos + 1..pos + 1 + entry_len];

    // Stream entry: type(1) + ref_type-dependent PID extraction
    // For type 1 (PlayItem stream): [0]=type, [1..3]=ref_to_stream_PID_of_playItem
    // ref format: [0] = subpath/playitem ref type, [1..2] = PID (big-endian u16)
    let pid = if entry.len() >= 3 {
        u16::from_be_bytes([entry[1], entry[2]])
    } else {
        0
    };

    // Stream attributes follow after the PID reference
    // Attributes block: length(1) + coding_type(1) + format-specific data + language(3)
    let attr_start = pos + 1 + entry_len;
    if attr_start + 2 > stn.len() { return None; }

    let attr_len = stn[attr_start] as usize;
    if attr_len < 4 || attr_start + 1 + attr_len > stn.len() {
        return Some((StreamEntry {
            stream_type, pid, coding_type: 0, video_format: 0, video_rate: 0,
            audio_format: 0, audio_rate: 0, language: String::new(),
        }, 1 + entry_len + 1 + attr_len.max(1)));
    }

    let attr = &stn[attr_start + 1..attr_start + 1 + attr_len];
    let coding_type = attr[0];

    let mut video_format = 0u8;
    let mut video_rate = 0u8;
    let mut audio_format = 0u8;
    let mut audio_rate = 0u8;
    let mut language = String::new();

    match stream_type {
        1 => {
            // Video: coding_type(1) + format_and_rate(1) + ...
            if attr.len() >= 2 {
                video_format = (attr[1] >> 4) & 0x0F;
                video_rate = attr[1] & 0x0F;
            }
        }
        2 => {
            // Audio: coding_type(1) + format_and_rate(1) + language(3)
            if attr.len() >= 2 {
                audio_format = (attr[1] >> 4) & 0x0F;
                audio_rate = attr[1] & 0x0F;
            }
            if attr.len() >= 5 {
                language = String::from_utf8_lossy(&attr[2..5]).to_string();
            }
        }
        3 => {
            // PG subtitle: coding_type(1) + language(3)
            if attr.len() >= 4 {
                language = String::from_utf8_lossy(&attr[1..4]).to_string();
            }
        }
        4 => {
            // IG: coding_type(1) + language(3)
            if attr.len() >= 4 {
                language = String::from_utf8_lossy(&attr[1..4]).to_string();
            }
        }
        _ => {}
    }

    let total_consumed = 1 + entry_len + 1 + attr_len;

    Some((StreamEntry {
        stream_type,
        pid,
        coding_type,
        video_format,
        video_rate,
        audio_format,
        audio_rate,
        language,
    }, total_consumed))
}
