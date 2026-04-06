//! Disc structure — scan titles, streams, and sector ranges from a Blu-ray disc.
//!
//! This is the high-level API for disc content. The CLI calls this,
//! never parses MPLS/CLPI/UDF directly.
//!
//! Usage:
//!   let disc = Disc::scan(&mut session)?;
//!   for title in disc.titles() { ... }
//!   for stream in title.streams() { ... }

use crate::error::{Error, Result};
use crate::drive::DriveSession;
use crate::udf;
use crate::mpls;
use crate::clpi;

// ─── Public types ───────────────────────────────────────────────────────────

/// A scanned Blu-ray disc.
#[derive(Debug)]
pub struct Disc {
    /// Disc capacity in sectors
    pub capacity_sectors: u32,
    /// Titles sorted by duration (longest first), then playlist name
    pub titles: Vec<Title>,
}

/// A title (one MPLS playlist).
#[derive(Debug, Clone)]
pub struct Title {
    /// Playlist filename (e.g. "00800.mpls")
    pub playlist: String,
    /// Playlist number (e.g. 800)
    pub playlist_id: u16,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Number of clips
    pub clip_count: usize,
    /// All streams (video, audio, subtitle, etc.)
    pub streams: Vec<Stream>,
    /// Sector extents for ripping (clip LBA ranges)
    pub extents: Vec<Extent>,
}

/// A stream within a title.
#[derive(Debug, Clone)]
pub struct Stream {
    /// Stream type
    pub kind: StreamKind,
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec
    pub codec: Codec,
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Video resolution (e.g. "2160p", "1080p")
    pub resolution: String,
    /// Frame rate (e.g. "23.976")
    pub frame_rate: String,
    /// Channel layout (e.g. "5.1", "7.1", "stereo")
    pub channels: String,
    /// Sample rate (e.g. "48kHz")
    pub sample_rate: String,
    /// HDR format
    pub hdr: HdrFormat,
    /// Color space
    pub color_space: ColorSpace,
    /// Whether this is a secondary/enhancement stream
    pub secondary: bool,
    /// Extra label (e.g. "Dolby Vision EL")
    pub label: String,
}

/// Stream type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StreamKind {
    Video,
    Audio,
    Subtitle,
}

/// Video/audio codec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Codec {
    // Video
    Hevc,
    H264,
    Vc1,
    Mpeg2,
    // Audio
    TrueHd,
    DtsHdMa,
    DtsHdHr,
    Dts,
    Ac3,
    Ac3Plus,
    Lpcm,
    // Subtitle
    Pgs,
    // Unknown
    Unknown(u8),
}

/// HDR format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HdrFormat {
    Sdr,
    Hdr10,
    DolbyVision,
}

/// Color space.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColorSpace {
    Bt709,
    Bt2020,
    Unknown,
}

/// A contiguous range of sectors on disc.
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    pub start_lba: u32,
    pub sector_count: u32,
}

// ─── Display helpers ────────────────────────────────────────────────────────

impl Codec {
    pub fn name(&self) -> &'static str {
        match self {
            Codec::Hevc => "HEVC",
            Codec::H264 => "H.264",
            Codec::Vc1 => "VC-1",
            Codec::Mpeg2 => "MPEG-2",
            Codec::TrueHd => "TrueHD",
            Codec::DtsHdMa => "DTS-HD MA",
            Codec::DtsHdHr => "DTS-HD HR",
            Codec::Dts => "DTS",
            Codec::Ac3 => "AC-3",
            Codec::Ac3Plus => "AC-3+",
            Codec::Lpcm => "LPCM",
            Codec::Pgs => "PGS",
            Codec::Unknown(_) => "Unknown",
        }
    }

    fn from_coding_type(ct: u8) -> Self {
        match ct {
            0x24 => Codec::Hevc,
            0x1B => Codec::H264,
            0xEA => Codec::Vc1,
            0x02 => Codec::Mpeg2,
            0x83 => Codec::TrueHd,
            0x86 => Codec::DtsHdMa,
            0x85 => Codec::DtsHdHr,
            0x82 => Codec::Dts,
            0x81 => Codec::Ac3,
            0x84 | 0xA1 => Codec::Ac3Plus,
            0x80 => Codec::Lpcm,
            0xA2 => Codec::DtsHdHr,
            0x90 | 0x91 => Codec::Pgs,
            ct => Codec::Unknown(ct),
        }
    }
}

impl HdrFormat {
    pub fn name(&self) -> &'static str {
        match self {
            HdrFormat::Sdr => "SDR",
            HdrFormat::Hdr10 => "HDR10",
            HdrFormat::DolbyVision => "Dolby Vision",
        }
    }
}

impl ColorSpace {
    pub fn name(&self) -> &'static str {
        match self {
            ColorSpace::Bt709 => "BT.709",
            ColorSpace::Bt2020 => "BT.2020",
            ColorSpace::Unknown => "",
        }
    }
}

impl Title {
    /// Duration formatted as "Xh Ym"
    pub fn duration_display(&self) -> String {
        let hrs = (self.duration_secs / 3600.0) as u32;
        let mins = ((self.duration_secs % 3600.0) / 60.0) as u32;
        format!("{}h {:02}m", hrs, mins)
    }

    /// Size in GB
    pub fn size_gb(&self) -> f64 {
        self.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Total sectors across all extents
    pub fn total_sectors(&self) -> u64 {
        self.extents.iter().map(|e| e.sector_count as u64).sum()
    }
}

impl Stream {
    /// Human-readable one-line description.
    pub fn display(&self) -> String {
        match self.kind {
            StreamKind::Video => {
                let mut parts = vec![self.codec.name().to_string()];
                if !self.resolution.is_empty() { parts.push(self.resolution.clone()); }
                if !self.frame_rate.is_empty() { parts.push(format!("{}fps", self.frame_rate)); }
                if self.hdr != HdrFormat::Sdr { parts.push(self.hdr.name().to_string()); }
                if self.color_space != ColorSpace::Unknown && self.color_space != ColorSpace::Bt709 {
                    parts.push(self.color_space.name().to_string());
                }
                if self.secondary { parts.push(format!("[{}]", self.label)); }
                parts.join(" ")
            }
            StreamKind::Audio => {
                let mut parts = vec![self.codec.name().to_string()];
                if !self.channels.is_empty() { parts.push(self.channels.clone()); }
                if !self.sample_rate.is_empty() { parts.push(self.sample_rate.clone()); }
                if !self.language.is_empty() { parts.push(format!("({})", self.language)); }
                if self.secondary { parts.push("[secondary]".to_string()); }
                parts.join(" ")
            }
            StreamKind::Subtitle => {
                let mut parts = vec![self.codec.name().to_string()];
                if !self.language.is_empty() { parts.push(format!("({})", self.language)); }
                parts.join(" ")
            }
        }
    }

    /// Kind as a display string
    pub fn kind_name(&self) -> &'static str {
        match self.kind {
            StreamKind::Video => "Video",
            StreamKind::Audio => "Audio",
            StreamKind::Subtitle => "Subtitle",
        }
    }
}

// ─── Disc scanning ──────────────────────────────────────────────────────────

// Placeholder — the actual implementation will be wired in
// when the CLI's disc_info.rs parsing is migrated here.
// For now, the CLI does its own parsing.

impl Disc {
    /// Disc capacity in GB
    pub fn capacity_gb(&self) -> f64 {
        self.capacity_sectors as f64 * 2048.0 / (1024.0 * 1024.0 * 1024.0)
    }
}
