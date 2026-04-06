//! Disc structure — titles, clips, and sector ranges.
//!
//! Reads the BDMV directory structure from a disc to enumerate titles.
//! Each title is a playlist (MPLS) containing one or more clips,
//! each clip mapping to a range of sectors (LBAs) on disc.

use crate::error::{Error, Result};
use crate::drive::DriveSession;
use crate::udf;
use crate::mpls;
use crate::clpi;

/// A disc title (one MPLS playlist).
#[derive(Debug, Clone)]
pub struct Title {
    /// Playlist number (e.g. 800 for 00800.mpls)
    pub playlist_id: u16,
    /// Playlist filename (e.g. "00800.mpls")
    pub filename: String,
    /// Duration in 45kHz ticks
    pub duration_ticks: u64,
    /// Duration formatted as human-readable string
    pub duration: String,
    /// Total size in bytes (sum of all clip extents × 2048)
    pub size_bytes: u64,
    /// Clips in playback order
    pub clips: Vec<Clip>,
    /// Number of video streams
    pub video_streams: u16,
    /// Number of audio streams
    pub audio_streams: u16,
}

/// A clip within a title.
#[derive(Debug, Clone)]
pub struct Clip {
    /// Clip filename (e.g. "00001")
    pub clip_id: String,
    /// In-time (45kHz ticks)
    pub in_time: u32,
    /// Out-time (45kHz ticks)
    pub out_time: u32,
    /// Sector ranges on disc for this clip
    pub extents: Vec<Extent>,
}

/// A contiguous range of sectors on disc.
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    /// Starting LBA
    pub start_lba: u32,
    /// Number of sectors
    pub sector_count: u32,
}

impl Title {
    /// Duration in seconds.
    pub fn duration_secs(&self) -> f64 {
        self.duration_ticks as f64 / 45000.0
    }

    /// Total sectors across all clips.
    pub fn total_sectors(&self) -> u64 {
        self.clips.iter()
            .flat_map(|c| &c.extents)
            .map(|e| e.sector_count as u64)
            .sum()
    }

    /// Format duration as "Xh Ym" or "Xm Ys".
    fn format_duration(ticks: u64) -> String {
        let secs = ticks / 45000;
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        if hours > 0 {
            format!("{}h {:02}m", hours, mins)
        } else {
            format!("{}m {:02}s", mins, secs % 60)
        }
    }
}

/// Scan a disc and return all titles sorted by duration (longest first).
pub fn scan_titles(session: &mut DriveSession) -> Result<Vec<Title>> {
    // Read disc capacity
    let capacity = read_capacity(session)?;

    // Read UDF filesystem to find BDMV directory
    let udf_fs = udf::read_filesystem(session)?;

    // Find all playlist files
    let playlist_dir = udf_fs.find_dir("/BDMV/PLAYLIST")
        .ok_or_else(|| Error::DiscError { detail: "BDMV/PLAYLIST not found".into() })?;

    let mut titles = Vec::new();

    for entry in &playlist_dir.entries {
        if !entry.name.ends_with(".mpls") {
            continue;
        }

        // Read the MPLS file
        let mpls_data = udf_fs.read_file(session, &format!("/BDMV/PLAYLIST/{}", entry.name))?;
        let playlist = match mpls::parse(&mpls_data) {
            Ok(p) => p,
            Err(_) => continue, // skip malformed playlists
        };

        // For each play item, read the corresponding CLPI to get sector extents
        let mut clips = Vec::new();
        let mut total_ticks: u64 = 0;
        let mut total_size: u64 = 0;

        for item in &playlist.play_items {
            let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", item.clip_id);
            let clpi_data = match udf_fs.read_file(session, &clpi_path) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let clip_info = match clpi::parse(&clpi_data) {
                Ok(c) => c,
                Err(_) => continue,
            };

            // Map timestamps to sector extents
            let extents = clip_info.get_extents(item.in_time, item.out_time);
            let clip_sectors: u64 = extents.iter().map(|e| e.sector_count as u64).sum();

            total_ticks += (item.out_time - item.in_time) as u64;
            total_size += clip_sectors * 2048;

            clips.push(Clip {
                clip_id: item.clip_id.clone(),
                in_time: item.in_time,
                out_time: item.out_time,
                extents,
            });
        }

        if clips.is_empty() {
            continue;
        }

        // Parse playlist ID from filename
        let playlist_id = entry.name.trim_end_matches(".mpls")
            .parse::<u16>().unwrap_or(0);

        titles.push(Title {
            playlist_id,
            filename: entry.name.clone(),
            duration_ticks: total_ticks,
            duration: Title::format_duration(total_ticks),
            size_bytes: total_size,
            clips,
            video_streams: playlist.video_stream_count,
            audio_streams: playlist.audio_stream_count,
        });
    }

    // Sort by duration, longest first
    titles.sort_by(|a, b| b.duration_ticks.cmp(&a.duration_ticks));

    Ok(titles)
}

/// Read disc capacity (total sectors).
fn read_capacity(session: &mut DriveSession) -> Result<u32> {
    let cdb = [0x25, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    let mut buf = [0u8; 8];
    session.scsi_execute(&cdb, crate::scsi::DataDirection::FromDevice, &mut buf, 5000)?;
    let lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    Ok(lba)
}
