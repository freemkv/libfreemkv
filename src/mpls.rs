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
    /// Number of video streams
    pub video_stream_count: u16,
    /// Number of audio streams
    pub audio_stream_count: u16,
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
    let _playlist_mark_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;

    if playlist_start + 10 > data.len() {
        return Err(Error::DiscError { detail: "MPLS playlist offset out of range".into() });
    }

    // PlayList section
    let pl = &data[playlist_start..];
    let _pl_length = u32::from_be_bytes([pl[0], pl[1], pl[2], pl[3]]) as usize;
    // pl[4..6] reserved
    let num_play_items = u16::from_be_bytes([pl[6], pl[7]]) as usize;
    let _num_sub_paths = u16::from_be_bytes([pl[8], pl[9]]) as usize;

    let mut play_items = Vec::with_capacity(num_play_items);
    let mut pos = 10; // start of first play item

    let mut video_streams: u16 = 0;
    let mut audio_streams: u16 = 0;

    for _ in 0..num_play_items {
        if playlist_start + pos + 2 > data.len() {
            break;
        }

        let item_length = u16::from_be_bytes([pl[pos], pl[pos + 1]]) as usize;
        if playlist_start + pos + item_length + 2 > data.len() {
            break;
        }

        let item = &pl[pos + 2..pos + 2 + item_length];

        // Clip ID: 5 bytes ASCII at offset 0 (e.g. "00001")
        let clip_id = String::from_utf8_lossy(&item[0..5]).to_string();
        // item[5..9] = codec ID ("M2TS")
        // item[9] = connection condition (bits)
        let connection_condition = item[9] & 0x0F;
        // item[10] = ref to STC_id
        // item[12..16] = IN_time
        let in_time = u32::from_be_bytes([item[12], item[13], item[14], item[15]]);
        // item[16..20] = OUT_time
        let out_time = u32::from_be_bytes([item[16], item[17], item[18], item[19]]);

        // STN table follows at offset 20 within the play item
        if item.len() > 22 {
            let stn_length = u16::from_be_bytes([item[20], item[21]]) as usize;
            if stn_length > 4 && item.len() > 24 {
                // Number of primary video/audio entries
                let n_video = item[24] as u16;
                let n_audio = item[25] as u16;
                if video_streams == 0 {
                    video_streams = n_video;
                    audio_streams = n_audio;
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
        video_stream_count: video_streams,
        audio_stream_count: audio_streams,
    })
}
