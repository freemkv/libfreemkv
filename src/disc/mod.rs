//! Disc structure -- scan titles, streams, and sector ranges from a Blu-ray disc.
//!
//! This is the high-level API for disc content. The CLI calls this,
//! never parses MPLS/CLPI/UDF directly.
//!
//! Usage:
//!   let disc = Disc::scan(&mut session)?;
//!   for title in disc.titles() { ... }
//!   for stream in title.streams() { ... }

mod bluray;
mod dvd;
mod encrypt;

use crate::drive::Drive;
use crate::error::{Error, Result};
use crate::sector::SectorReader;
use crate::udf;

use encrypt::HandshakeResult;

// ─── Public types ───────────────────────────────────────────────────────────

/// A scanned Blu-ray disc.
#[derive(Debug)]
pub struct Disc {
    /// UDF Volume Identifier from Primary Volume Descriptor (always present)
    pub volume_id: String,
    /// Disc title from META/DL/bdmt_eng.xml (None if disc has no metadata)
    pub meta_title: Option<String>,
    /// Disc format (BD, UHD, DVD)
    pub format: DiscFormat,
    /// Disc capacity in sectors
    pub capacity_sectors: u32,
    /// Disc capacity in bytes
    pub capacity_bytes: u64,
    /// Number of layers (1 = single, 2 = dual)
    pub layers: u8,
    /// Titles sorted by duration (longest first), then playlist name
    pub titles: Vec<DiscTitle>,
    /// Disc region
    pub region: DiscRegion,
    /// AACS state -- None if disc is unencrypted or keys unavailable
    pub aacs: Option<AacsState>,
    /// CSS state -- None if not a CSS-encrypted DVD
    pub css: Option<crate::css::CssState>,
    /// Whether this disc requires decryption (AACS or CSS)
    pub encrypted: bool,
    /// Content format (BD transport stream vs DVD program stream)
    pub content_format: ContentFormat,
}

/// Content format — determines how sectors are interpreted downstream.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContentFormat {
    /// Blu-ray BD Transport Stream (192-byte packets)
    BdTs,
    /// DVD MPEG-2 Program Stream (VOB)
    MpegPs,
}

/// Disc format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiscFormat {
    /// 4K UHD Blu-ray (HEVC 2160p)
    Uhd,
    /// Standard Blu-ray (1080p/1080i)
    BluRay,
    /// DVD
    Dvd,
    /// Unknown
    Unknown,
}

/// Disc playback region.
#[derive(Debug, Clone, PartialEq)]
pub enum DiscRegion {
    /// Region-free (all UHD discs, some BD/DVD)
    Free,
    /// Blu-ray regions (A/B/C or combination)
    BluRay(Vec<BdRegion>),
    /// DVD regions (1-8 or combination)
    Dvd(Vec<u8>),
}

/// Blu-ray region codes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BdRegion {
    /// Region A/1 -- Americas, East Asia (Japan, Korea, Southeast Asia)
    A,
    /// Region B/2 -- Europe, Africa, Australia, Middle East
    B,
    /// Region C/3 -- Central/South Asia, China, Russia
    C,
}

/// A title (one MPLS playlist).
#[derive(Debug, Clone)]
pub struct DiscTitle {
    /// Playlist filename (e.g. "00800.mpls")
    pub playlist: String,
    /// Playlist number (e.g. 800)
    pub playlist_id: u16,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Clip references in playback order
    pub clips: Vec<Clip>,
    /// All streams (video, audio, subtitle, etc.)
    pub streams: Vec<Stream>,
    /// Chapter points
    pub chapters: Vec<Chapter>,
    /// Sector extents for ripping (clip LBA ranges)
    pub extents: Vec<Extent>,
    /// Content format for this title
    pub content_format: ContentFormat,
    /// Codec initialization data per stream (SPS/PPS, etc).
    /// Index matches `streams`. None for streams without codec init data.
    pub codec_privates: Vec<Option<Vec<u8>>>,
}

/// A clip reference within a title.
#[derive(Debug, Clone)]
pub struct Clip {
    /// Clip filename without extension (e.g. "00001")
    pub clip_id: String,
    /// In-time in 45kHz ticks
    pub in_time: u32,
    /// Out-time in 45kHz ticks
    pub out_time: u32,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Source packet count (from CLPI, 0 if unavailable)
    pub source_packets: u32,
}

/// A stream within a title.
#[derive(Debug, Clone)]
pub enum Stream {
    Video(VideoStream),
    Audio(AudioStream),
    Subtitle(SubtitleStream),
}

/// A video stream.
#[derive(Debug, Clone)]
pub struct VideoStream {
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec (HEVC, H.264, VC-1, MPEG-2)
    pub codec: Codec,
    /// Resolution
    pub resolution: Resolution,
    /// Frame rate
    pub frame_rate: FrameRate,
    /// HDR format
    pub hdr: HdrFormat,
    /// Color space
    pub color_space: ColorSpace,
    /// Whether this is a secondary stream (PiP, Dolby Vision EL)
    pub secondary: bool,
    /// Extra label (e.g. "Dolby Vision EL")
    pub label: String,
}

/// An audio stream.
#[derive(Debug, Clone)]
pub struct AudioStream {
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec (TrueHD, DTS-HD MA, DD, LPCM, etc.)
    pub codec: Codec,
    /// Channel layout
    pub channels: AudioChannels,
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Sample rate
    pub sample_rate: SampleRate,
    /// Whether this is a secondary stream (commentary)
    pub secondary: bool,
    /// Extra label
    pub label: String,
}

/// A subtitle stream.
#[derive(Debug, Clone)]
pub struct SubtitleStream {
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec (PGS)
    pub codec: Codec,
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Whether this is a forced subtitle
    pub forced: bool,
    /// Pre-formatted codec private data (e.g. VobSub .idx palette header)
    pub codec_data: Option<Vec<u8>>,
}

/// Video/audio codec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Codec {
    // Video
    Hevc,
    H264,
    Vc1,
    Mpeg2,
    Mpeg1,
    Av1,
    // Audio
    TrueHd,
    DtsHdMa,
    DtsHdHr,
    Dts,
    Ac3,
    Ac3Plus,
    Lpcm,
    Aac,
    Mp2,
    Mp3,
    Flac,
    Opus,
    // Subtitle
    Pgs,
    DvdSub,
    Srt,
    Ssa,
    // Unknown
    Unknown(u8),
}

/// Video resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// 480i (720x480 interlaced) — NTSC DVD
    R480i,
    /// 480p (720x480 progressive)
    R480p,
    /// 576i (720x576 interlaced) — PAL DVD
    R576i,
    /// 576p (720x576 progressive)
    R576p,
    /// 720p (1280x720 progressive) — some Blu-rays
    R720p,
    /// 1080i (1920x1080 interlaced) — broadcast, some BD
    R1080i,
    /// 1080p (1920x1080 progressive) — standard Blu-ray
    R1080p,
    /// 2160p (3840x2160 progressive) — 4K UHD Blu-ray
    R2160p,
    /// 4320p (7680x4320 progressive) — 8K, future-proof
    R4320p,
    /// Unknown resolution
    Unknown,
}

/// Video frame rate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameRate {
    /// 23.976 fps — film-based BD/UHD (NTSC pulldown)
    F23_976,
    /// 24.000 fps — true film rate
    F24,
    /// 25.000 fps — PAL standard
    F25,
    /// 29.970 fps — NTSC standard
    F29_97,
    /// 30.000 fps
    F30,
    /// 50.000 fps — PAL high frame rate
    F50,
    /// 59.940 fps — NTSC high frame rate
    F59_94,
    /// 60.000 fps
    F60,
    /// Unknown frame rate
    Unknown,
}

/// Audio channel layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioChannels {
    /// 1.0 mono
    Mono,
    /// 2.0 stereo
    Stereo,
    /// 2.1 (stereo + LFE)
    Stereo21,
    /// 4.0 quadraphonic
    Quad,
    /// 5.0 surround (no LFE)
    Surround50,
    /// 5.1 surround — standard BD/DVD surround
    Surround51,
    /// 6.1 surround (DTS-ES, Dolby EX)
    Surround61,
    /// 7.1 surround — UHD Atmos beds, DTS:X
    Surround71,
    /// Unknown channel layout
    Unknown,
}

/// Audio sample rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleRate {
    /// 44.1 kHz — CD audio (rare on disc)
    S44_1,
    /// 48 kHz — standard BD/DVD/UHD audio
    S48,
    /// 96 kHz — high-res BD audio
    S96,
    /// 192 kHz — highest BD audio (LPCM)
    S192,
    /// 48/96 kHz combo (secondary audio resampled)
    S48_96,
    /// 48/192 kHz combo (secondary audio resampled)
    S48_192,
    /// Unknown sample rate
    Unknown,
}

/// HDR format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HdrFormat {
    Sdr,
    Hdr10,
    Hdr10Plus,
    DolbyVision,
    Hlg,
}

/// Color space.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColorSpace {
    Bt709,
    Bt2020,
    Unknown,
}

/// A chapter point within a title.
#[derive(Debug, Clone)]
pub struct Chapter {
    /// Chapter start time in seconds
    pub time_secs: f64,
    /// Chapter name (e.g. "Chapter 1", "Chapter 2")
    pub name: String,
}

/// A contiguous range of sectors on disc.
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    pub start_lba: u32,
    pub sector_count: u32,
}

// ─── Display helpers ────────────────────────────────────────────────────────

impl Codec {
    /// Human-readable display name.
    pub fn name(&self) -> &'static str {
        for (_, name, v) in Self::ALL_CODECS {
            if v == self {
                return name;
            }
        }
        "Unknown"
    }

    /// Compact identifier for serialization (lowercase, no spaces).
    pub fn id(&self) -> &'static str {
        for (id, _, v) in Self::ALL_CODECS {
            if v == self {
                return id;
            }
        }
        "unknown"
    }

    const ALL_CODECS: &[(&'static str, &'static str, Codec)] = &[
        ("hevc", "HEVC", Codec::Hevc),
        ("h264", "H.264", Codec::H264),
        ("vc1", "VC-1", Codec::Vc1),
        ("mpeg2", "MPEG-2", Codec::Mpeg2),
        ("mpeg1", "MPEG-1", Codec::Mpeg1),
        ("av1", "AV1", Codec::Av1),
        ("truehd", "TrueHD", Codec::TrueHd),
        ("dtshd_ma", "DTS-HD MA", Codec::DtsHdMa),
        ("dtshd_hr", "DTS-HD HR", Codec::DtsHdHr),
        ("dts", "DTS", Codec::Dts),
        ("ac3", "AC-3", Codec::Ac3),
        ("eac3", "EAC-3", Codec::Ac3Plus),
        ("lpcm", "LPCM", Codec::Lpcm),
        ("aac", "AAC", Codec::Aac),
        ("mp2", "MP2", Codec::Mp2),
        ("mp3", "MP3", Codec::Mp3),
        ("flac", "FLAC", Codec::Flac),
        ("opus", "Opus", Codec::Opus),
        ("pgs", "PGS", Codec::Pgs),
        ("dvdsub", "DVD Subtitle", Codec::DvdSub),
        ("srt", "SRT", Codec::Srt),
        ("ssa", "SSA", Codec::Ssa),
    ];

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

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl Resolution {
    /// Parse from MPLS video_format byte.
    pub fn from_video_format(vf: u8) -> Self {
        match vf {
            1 => Resolution::R480i,
            2 => Resolution::R576i,
            3 => Resolution::R480p,
            4 => Resolution::R1080i,
            5 => Resolution::R720p,
            6 => Resolution::R1080p,
            7 => Resolution::R576p,
            8 => Resolution::R2160p,
            _ => Resolution::Unknown,
        }
    }

    /// Pixel dimensions (width, height).
    pub fn pixels(&self) -> (u32, u32) {
        match self {
            Resolution::R480i | Resolution::R480p => (720, 480),
            Resolution::R576i | Resolution::R576p => (720, 576),
            Resolution::R720p => (1280, 720),
            Resolution::R1080i | Resolution::R1080p => (1920, 1080),
            Resolution::R2160p => (3840, 2160),
            Resolution::R4320p => (7680, 4320),
            Resolution::Unknown => (1920, 1080),
        }
    }

    /// True if this is a UHD (4K+) resolution.
    pub fn is_uhd(&self) -> bool {
        matches!(self, Resolution::R2160p | Resolution::R4320p)
    }

    /// True if this is an HD (720p+) resolution.
    pub fn is_hd(&self) -> bool {
        !matches!(
            self,
            Resolution::R480i
                | Resolution::R480p
                | Resolution::R576i
                | Resolution::R576p
                | Resolution::Unknown
        )
    }

    /// True if this is an SD (480/576) resolution.
    pub fn is_sd(&self) -> bool {
        matches!(
            self,
            Resolution::R480i | Resolution::R480p | Resolution::R576i | Resolution::R576p
        )
    }

    /// Parse from pixel height (e.g. from MKV track).
    pub fn from_height(h: u32) -> Self {
        match h {
            0..=480 => Resolution::R480p,
            481..=576 => Resolution::R576p,
            577..=720 => Resolution::R720p,
            721..=1080 => Resolution::R1080p,
            1081..=2160 => Resolution::R2160p,
            _ => Resolution::R4320p,
        }
    }
}

// Display for Resolution is generated by enum_str! macro

impl FrameRate {
    /// Parse from MPLS video_rate byte.
    pub fn from_video_rate(vr: u8) -> Self {
        match vr {
            1 => FrameRate::F23_976,
            2 => FrameRate::F24,
            3 => FrameRate::F25,
            4 => FrameRate::F29_97,
            5 => FrameRate::F30,
            6 => FrameRate::F50,
            7 => FrameRate::F59_94,
            8 => FrameRate::F60,
            _ => FrameRate::Unknown,
        }
    }

    /// Frame rate as (numerator, denominator) for precise representation.
    pub fn as_fraction(&self) -> (u32, u32) {
        match self {
            FrameRate::F23_976 => (24000, 1001),
            FrameRate::F24 => (24, 1),
            FrameRate::F25 => (25, 1),
            FrameRate::F29_97 => (30000, 1001),
            FrameRate::F30 => (30, 1),
            FrameRate::F50 => (50, 1),
            FrameRate::F59_94 => (60000, 1001),
            FrameRate::F60 => (60, 1),
            FrameRate::Unknown => (0, 1),
        }
    }
}

// Display for FrameRate is generated by enum_str! macro

impl AudioChannels {
    /// Parse from MPLS audio_format byte.
    pub fn from_audio_format(af: u8) -> Self {
        match af {
            1 => AudioChannels::Mono,
            3 => AudioChannels::Stereo,
            6 => AudioChannels::Surround51,
            12 => AudioChannels::Surround71,
            _ if af > 0 => AudioChannels::Unknown,
            _ => AudioChannels::Unknown,
        }
    }

    /// Channel count as a number.
    pub fn count(&self) -> u8 {
        match self {
            AudioChannels::Mono => 1,
            AudioChannels::Stereo => 2,
            AudioChannels::Stereo21 => 3,
            AudioChannels::Quad => 4,
            AudioChannels::Surround50 => 5,
            AudioChannels::Surround51 => 6,
            AudioChannels::Surround61 => 7,
            AudioChannels::Surround71 => 8,
            AudioChannels::Unknown => 6,
        }
    }

    /// Parse from channel count number.
    pub fn from_count(n: u8) -> Self {
        match n {
            1 => AudioChannels::Mono,
            2 => AudioChannels::Stereo,
            3 => AudioChannels::Stereo21,
            4 => AudioChannels::Quad,
            5 => AudioChannels::Surround50,
            6 => AudioChannels::Surround51,
            7 => AudioChannels::Surround61,
            8 => AudioChannels::Surround71,
            _ => AudioChannels::Unknown,
        }
    }
}

// Display for AudioChannels is generated by enum_str! macro

impl SampleRate {
    /// Parse from MPLS audio_rate byte.
    pub fn from_audio_rate(ar: u8) -> Self {
        match ar {
            1 => SampleRate::S48,
            4 => SampleRate::S96,
            5 => SampleRate::S192,
            12 => SampleRate::S48_192,
            14 => SampleRate::S48_96,
            _ => SampleRate::Unknown,
        }
    }

    /// Sample rate in Hz (primary rate for combo rates).
    pub fn hz(&self) -> f64 {
        match self {
            SampleRate::S44_1 => 44100.0,
            SampleRate::S48 | SampleRate::S48_96 | SampleRate::S48_192 => 48000.0,
            SampleRate::S96 => 96000.0,
            SampleRate::S192 => 192000.0,
            SampleRate::Unknown => 48000.0,
        }
    }

    /// Parse from Hz value.
    pub fn from_hz(hz: u32) -> Self {
        match hz {
            44100 => SampleRate::S44_1,
            48000 => SampleRate::S48,
            96000 => SampleRate::S96,
            192000 => SampleRate::S192,
            _ => SampleRate::Unknown,
        }
    }
}

// Display for SampleRate is generated by enum_str! macro

impl HdrFormat {
    pub fn name(&self) -> &'static str {
        match self {
            HdrFormat::Sdr => "SDR",
            HdrFormat::Hdr10 => "HDR10",
            HdrFormat::Hdr10Plus => "HDR10+",
            HdrFormat::DolbyVision => "Dolby Vision",
            HdrFormat::Hlg => "HLG",
        }
    }

    const ALL_HDR: &[(&'static str, HdrFormat)] = &[
        ("sdr", HdrFormat::Sdr),
        ("hdr10", HdrFormat::Hdr10),
        ("hdr10+", HdrFormat::Hdr10Plus),
        ("dv", HdrFormat::DolbyVision),
        ("hlg", HdrFormat::Hlg),
    ];

    /// Compact identifier for serialization.
    pub fn id(&self) -> &'static str {
        for (id, v) in Self::ALL_HDR {
            if v == self {
                return id;
            }
        }
        "sdr"
    }
}

impl std::fmt::Display for HdrFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
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

impl std::fmt::Display for ColorSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

// ─── FromStr impls — single source of truth via ALL_* arrays ───────────────
//
// Each enum defines a const array of (str, variant) pairs. Display, FromStr,
// and id() all derive from this one table — no string appears twice.

macro_rules! enum_str {
    ($name:ident, $default:expr, [ $( ($s:expr, $v:expr) ),* $(,)? ]) => {
        impl $name {
            const ALL: &[(&'static str, $name)] = &[ $( ($s, $v), )* ];
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for (s, v) in $name::ALL {
                    if v == self { return f.write_str(s); }
                }
                f.write_str("")
            }
        }
        impl std::str::FromStr for $name {
            type Err = ();
            fn from_str(s: &str) -> std::result::Result<Self, ()> {
                for (k, v) in $name::ALL {
                    if *k == s { return Ok(*v); }
                }
                Ok($default)
            }
        }
    };
}

enum_str!(
    Resolution,
    Resolution::Unknown,
    [
        ("480i", Resolution::R480i),
        ("480p", Resolution::R480p),
        ("576i", Resolution::R576i),
        ("576p", Resolution::R576p),
        ("720p", Resolution::R720p),
        ("1080i", Resolution::R1080i),
        ("1080p", Resolution::R1080p),
        ("2160p", Resolution::R2160p),
        ("4320p", Resolution::R4320p),
    ]
);

enum_str!(
    FrameRate,
    FrameRate::Unknown,
    [
        ("23.976", FrameRate::F23_976),
        ("24", FrameRate::F24),
        ("25", FrameRate::F25),
        ("29.97", FrameRate::F29_97),
        ("30", FrameRate::F30),
        ("50", FrameRate::F50),
        ("59.94", FrameRate::F59_94),
        ("60", FrameRate::F60),
    ]
);

enum_str!(
    AudioChannels,
    AudioChannels::Unknown,
    [
        ("mono", AudioChannels::Mono),
        ("stereo", AudioChannels::Stereo),
        ("2.1", AudioChannels::Stereo21),
        ("4.0", AudioChannels::Quad),
        ("5.0", AudioChannels::Surround50),
        ("5.1", AudioChannels::Surround51),
        ("6.1", AudioChannels::Surround61),
        ("7.1", AudioChannels::Surround71),
    ]
);

enum_str!(
    SampleRate,
    SampleRate::Unknown,
    [
        ("44.1kHz", SampleRate::S44_1),
        ("48kHz", SampleRate::S48),
        ("96kHz", SampleRate::S96),
        ("192kHz", SampleRate::S192),
        ("48/96kHz", SampleRate::S48_96),
        ("48/192kHz", SampleRate::S48_192),
    ]
);

impl std::str::FromStr for Codec {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, ()> {
        for (id, _, v) in Codec::ALL_CODECS {
            if *id == s {
                return Ok(*v);
            }
        }
        Ok(Codec::Unknown(0))
    }
}

impl std::str::FromStr for HdrFormat {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, ()> {
        for (id, v) in HdrFormat::ALL_HDR {
            if *id == s {
                return Ok(*v);
            }
        }
        // Also accept display names
        for (_id, v) in HdrFormat::ALL_HDR {
            if HdrFormat::name(v) == s {
                return Ok(*v);
            }
        }
        Ok(HdrFormat::Sdr)
    }
}

impl DiscTitle {
    /// Empty DiscTitle with no streams.
    pub fn empty() -> Self {
        Self {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    /// Duration formatted as "Xh Ym"
    pub fn duration_display(&self) -> String {
        let hrs = (self.duration_secs / 3600.0) as u32;
        let mins = ((self.duration_secs % 3600.0) / 60.0) as u32;
        format!("{hrs}h {mins:02}m")
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

// ─── Encryption ─────────────────────────────────────────────────────────────

/// AACS decryption state for a disc.
#[derive(Debug)]
pub struct AacsState {
    /// AACS version (1 or 2)
    pub version: u8,
    /// Whether bus encryption is enabled (always true for AACS 2.0 / UHD)
    pub bus_encryption: bool,
    /// MKB version from disc (e.g. 68, 77)
    pub mkb_version: Option<u32>,
    /// Disc hash (SHA1 of Unit_Key_RO.inf) -- hex string with 0x prefix
    pub disc_hash: String,
    /// How keys were resolved
    pub key_source: KeySource,
    /// Volume Unique Key (16 bytes)
    pub vuk: [u8; 16],
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Read data key for AACS 2.0 bus decryption -- None for AACS 1.0
    pub read_data_key: Option<[u8; 16]>,
    /// Volume ID (16 bytes) -- from SCSI handshake
    pub volume_id: [u8; 16],
}

/// How AACS keys were resolved.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeySource {
    /// VUK found directly in KEYDB by disc hash
    KeyDb,
    /// Media key + Volume ID from KEYDB → derived VUK
    KeyDbDerived,
    /// MKB + processing keys → media key → VUK
    ProcessingKey,
    /// MKB + device keys → subset-difference tree → VUK
    DeviceKey,
}

impl KeySource {
    pub fn name(&self) -> &'static str {
        match self {
            KeySource::KeyDb => "KEYDB",
            KeySource::KeyDbDerived => "KEYDB (derived)",
            KeySource::ProcessingKey => "MKB + processing key",
            KeySource::DeviceKey => "MKB + device key",
        }
    }
}

// ─── Disc scanning ──────────────────────────────────────────────────────────

/// Standard KEYDB.cfg search locations (compatible with libaacs).
const KEYDB_SEARCH_PATHS: &[&str] = &[
    ".config/aacs/KEYDB.cfg", // relative to $HOME
];
const KEYDB_SYSTEM_PATH: &str = "/etc/aacs/KEYDB.cfg";

/// Options for disc scanning.
#[derive(Default)]
pub struct ScanOptions {
    /// Path to KEYDB.cfg for AACS key lookup.
    /// If None, searches standard locations ($HOME/.config/aacs/ and /etc/aacs/).
    pub keydb_path: Option<std::path::PathBuf>,
}

impl ScanOptions {
    /// Create options with a specific KEYDB path.
    pub fn with_keydb(path: impl Into<std::path::PathBuf>) -> Self {
        ScanOptions {
            keydb_path: Some(path.into()),
        }
    }

    /// Resolve KEYDB path: explicit path first, then standard locations.
    fn resolve_keydb(&self) -> Option<std::path::PathBuf> {
        if let Some(p) = &self.keydb_path {
            if p.exists() {
                return Some(p.clone());
            }
        }
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            for relative in KEYDB_SEARCH_PATHS {
                let p = std::path::PathBuf::from(&home).join(relative);
                if p.exists() {
                    return Some(p);
                }
            }
        }
        let p = std::path::PathBuf::from(KEYDB_SYSTEM_PATH);
        if p.exists() {
            return Some(p);
        }
        None
    }
}

impl Disc {
    /// Disc capacity in GB
    pub fn capacity_gb(&self) -> f64 {
        self.capacity_sectors as f64 * 2048.0 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Scan a disc -- parse filesystem, playlists, streams, and set up AACS decryption.
    ///
    /// This is the main entry point. After scan(), the Disc is ready:
    ///   - titles are populated with streams
    ///   - AACS keys are derived (if KEYDB available)
    ///   - content can be read and decrypted transparently
    ///
    /// Scan a disc. One pipeline, one order:
    ///   1. Read capacity
    ///   2. Read UDF filesystem
    ///   3. Resolve AACS keys (all via UDF, no SCSI commands)
    ///   4. Parse playlists + streams
    ///   5. Apply labels
    ///
    /// The session must be open and unlocked (Drive::open handles this).
    /// All disc reads use standard READ(10) via UDF -- no vendor SCSI commands.
    pub fn scan(session: &mut Drive, opts: &ScanOptions) -> Result<Self> {
        // READ CAPACITY may fail in LibreDrive mode — proceed with 0 and estimate later
        let capacity = Self::read_capacity(session).unwrap_or(0);
        let handshake = Self::do_handshake(session, opts);
        Self::scan_with(session, capacity, handshake, opts)
    }

    /// Scan a disc image (ISO or any SectorReader). No SCSI, no handshake.
    /// AACS resolution uses KEYDB VUK lookup only.
    pub fn scan_image(
        reader: &mut dyn SectorReader,
        capacity: u32,
        opts: &ScanOptions,
    ) -> Result<Self> {
        Self::scan_with(reader, capacity, None, opts)
    }

    /// Core scan pipeline — works with any SectorReader.
    fn scan_with(
        reader: &mut dyn SectorReader,
        capacity: u32,
        handshake: Option<HandshakeResult>,
        opts: &ScanOptions,
    ) -> Result<Self> {
        // 1. UDF filesystem
        let udf_fs = udf::read_filesystem(reader)?;

        // 2. Resolve encryption (AACS, CSS, or none)
        let encrypted =
            udf_fs.find_dir("/AACS").is_some() || udf_fs.find_dir("/BDMV/AACS").is_some();

        let aacs = if encrypted {
            if let Some(keydb_path) = opts.resolve_keydb() {
                Self::resolve_encryption(&udf_fs, reader, &keydb_path, handshake.as_ref()).ok()
            } else {
                None
            }
        } else {
            None
        };

        // 3. Titles — BD (MPLS playlists) or DVD (IFO title sets)
        let (mut titles, content_format) = if udf_fs.find_dir("/BDMV").is_some() {
            (
                Self::scan_bluray_titles(reader, &udf_fs),
                ContentFormat::BdTs,
            )
        } else if udf_fs.find_dir("/VIDEO_TS").is_some() {
            (
                Self::scan_dvd_titles(reader, &udf_fs),
                ContentFormat::MpegPs,
            )
        } else {
            (Vec::new(), ContentFormat::BdTs)
        };
        titles.sort_by(|a, b| {
            b.duration_secs
                .partial_cmp(&a.duration_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 4. Metadata + labels
        let meta_title = Self::read_meta_title(reader, &udf_fs);
        crate::labels::apply(reader, &udf_fs, &mut titles);

        // 5. Derive format, layers, region
        let format = Self::detect_format(&titles);
        let layers = if capacity > 24_000_000 { 2 } else { 1 };
        let region = DiscRegion::Free;

        // 6. CSS detection for DVDs
        let css = if content_format == ContentFormat::MpegPs && !titles.is_empty() {
            crate::css::crack_key(reader, &titles[0].extents)
        } else {
            None
        };
        let encrypted = encrypted || css.is_some();

        Ok(Disc {
            volume_id: udf_fs.volume_id.clone(),
            meta_title,
            format,
            capacity_sectors: capacity,
            capacity_bytes: capacity as u64 * 2048,
            layers,
            titles,
            region,
            aacs,
            css,
            encrypted,
            content_format,
        })
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Detect disc format from the main title's video streams.
    fn detect_format(titles: &[DiscTitle]) -> DiscFormat {
        for title in titles.iter().take(3) {
            for stream in &title.streams {
                if let Stream::Video(v) = stream {
                    if v.resolution.is_uhd() {
                        return DiscFormat::Uhd;
                    }
                    if v.resolution.is_hd() {
                        return DiscFormat::BluRay;
                    }
                    if v.resolution.is_sd() {
                        return DiscFormat::Dvd;
                    }
                }
            }
        }
        DiscFormat::Unknown
    }

    fn read_capacity(session: &mut Drive) -> Result<u32> {
        let cdb = [
            crate::scsi::SCSI_READ_CAPACITY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut buf = [0u8; 8];
        session.scsi_execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        let lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Ok(lba + 1)
    }
}

impl Disc {
    /// Get the resolved decryption keys for this disc.
    /// Used by disc-to-ISO and other full-disc operations.
    pub fn decrypt_keys(&self) -> crate::decrypt::DecryptKeys {
        if let Some(ref aacs) = self.aacs {
            crate::decrypt::DecryptKeys::Aacs {
                unit_keys: aacs.unit_keys.clone(),
                read_data_key: aacs.read_data_key,
            }
        } else if let Some(ref css) = self.css {
            crate::decrypt::DecryptKeys::Css {
                title_key: css.title_key,
            }
        } else {
            crate::decrypt::DecryptKeys::None
        }
    }

    /// Raw sector copy — write the entire disc image to a file.
    ///
    /// This is NOT a stream operation. It copies sectors 0→capacity byte-for-byte,
    /// producing a valid ISO/UDF image. The disc's filesystem structure is preserved.
    ///
    /// If `decrypt` is true and keys are available, sectors are decrypted on the fly.
    /// If `resume` is true and the file already exists, resumes from the last safe position.
    ///
    /// `on_progress` is called periodically with (bytes_done, total_bytes).
    pub fn copy(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        decrypt: bool,
        resume: bool,
        on_progress: Option<&dyn Fn(u64, u64)>,
    ) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let total_bytes = self.capacity_sectors as u64 * 2048;
        let keys = if decrypt { self.decrypt_keys() } else { crate::decrypt::DecryptKeys::None };

        // Resume: check existing file
        let (start_lba, file) = if resume {
            match std::fs::metadata(path) {
                Ok(meta) if meta.len() > 0 => {
                    let safe_sectors = (meta.len() / 2048).saturating_sub(5) as u32;
                    let mut f = std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(|e| Error::IoError { source: e })?;
                    let resume_pos = safe_sectors as u64 * 2048;
                    f.set_len(resume_pos)
                        .map_err(|e| Error::IoError { source: e })?;
                    f.seek(SeekFrom::End(0))
                        .map_err(|e| Error::IoError { source: e })?;
                    (safe_sectors, f)
                }
                _ => {
                    let f = std::fs::File::create(path)
                        .map_err(|e| Error::IoError { source: e })?;
                    (0u32, f)
                }
            }
        } else {
            let f = std::fs::File::create(path)
                .map_err(|e| Error::IoError { source: e })?;
            (0u32, f)
        };

        let mut writer = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        let batch: u16 = 64; // 128 KB per read
        let mut lba = start_lba;
        let mut bytes_done = start_lba as u64 * 2048;
        let mut buf = vec![0u8; batch as usize * 2048];

        while lba < self.capacity_sectors {
            let remaining = self.capacity_sectors - lba;
            let count = remaining.min(batch as u32) as u16;
            let bytes = count as usize * 2048;

            reader
                .read_sectors(lba, count, &mut buf[..bytes])
                .map_err(|e| Error::IoError {
                    source: std::io::Error::other(e.to_string()),
                })?;

            // Decrypt if requested
            if decrypt {
                crate::decrypt::decrypt_sectors(&mut buf[..bytes], &keys, 0)?;
            }

            writer
                .write_all(&buf[..bytes])
                .map_err(|e| Error::IoError { source: e })?;

            lba += count as u32;
            bytes_done += bytes as u64;

            if let Some(ref cb) = on_progress {
                cb(bytes_done, total_bytes);
            }
        }

        writer.flush().map_err(|e| Error::IoError { source: e })?;
        Ok(())
    }
}

const MAX_BATCH_SECTORS: u16 = 510;
const DEFAULT_BATCH_SECTORS: u16 = 60;
const MIN_BATCH_SECTORS: u16 = 3;

/// Detect the maximum transfer size in sectors for a device.
/// Reads /sys/block/<dev>/queue/max_hw_sectors_kb on Linux.
/// For sg devices, resolves the corresponding block device via sysfs.
/// Returns a value aligned to 3 sectors (one aligned unit).
pub fn detect_max_batch_sectors(device_path: &str) -> u16 {
    let dev_name = device_path.rsplit('/').next().unwrap_or("");
    if dev_name.is_empty() {
        return DEFAULT_BATCH_SECTORS;
    }

    // For sg devices, find the corresponding block device name
    let block_name = if dev_name.starts_with("sg") {
        let block_dir = format!("/sys/class/scsi_generic/{dev_name}/device/block");
        std::fs::read_dir(&block_dir)
            .ok()
            .and_then(|mut entries| entries.next())
            .and_then(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
    } else {
        Some(dev_name.to_string())
    };

    if let Some(bname) = block_name {
        let sysfs_path = format!("/sys/block/{bname}/queue/max_hw_sectors_kb");
        if let Ok(content) = std::fs::read_to_string(&sysfs_path) {
            if let Ok(kb) = content.trim().parse::<u32>() {
                // Convert KB to sectors (1 sector = 2 KB = 2048 bytes)
                let sectors = (kb / 2) as u16;
                // Align down to 3 (one aligned unit)
                let aligned = (sectors / 3) * 3;
                if aligned >= MIN_BATCH_SECTORS {
                    return aligned.min(MAX_BATCH_SECTORS);
                }
            }
        }
    }
    // Fallback: safe default well under typical kernel limits
    DEFAULT_BATCH_SECTORS
}

// ─── Format helpers ────────────────────────────────────────────────────────

// Old format_* functions replaced by Resolution/FrameRate/AudioChannels/SampleRate enums

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a DiscTitle with a single video stream at the given resolution.
    fn title_with_video(codec: Codec, resolution: Resolution) -> DiscTitle {
        DiscTitle {
            playlist: "00800.mpls".into(),
            playlist_id: 800,
            duration_secs: 7200.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: vec![Stream::Video(VideoStream {
                pid: 0x1011,
                codec,
                resolution,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: String::new(),
            })],
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    #[test]
    fn detect_format_uhd() {
        let titles = vec![title_with_video(Codec::Hevc, Resolution::R2160p)];
        assert_eq!(Disc::detect_format(&titles), DiscFormat::Uhd);
    }

    #[test]
    fn detect_format_bluray() {
        let titles = vec![title_with_video(Codec::H264, Resolution::R1080p)];
        assert_eq!(Disc::detect_format(&titles), DiscFormat::BluRay);
    }

    #[test]
    fn detect_format_dvd() {
        let titles = vec![title_with_video(Codec::Mpeg2, Resolution::R480i)];
        assert_eq!(Disc::detect_format(&titles), DiscFormat::Dvd);
    }

    #[test]
    fn detect_format_empty() {
        let titles: Vec<DiscTitle> = Vec::new();
        assert_eq!(Disc::detect_format(&titles), DiscFormat::Unknown);
    }

    #[test]
    fn content_format_default_bdts() {
        let t = title_with_video(Codec::H264, Resolution::R1080p);
        assert_eq!(t.content_format, ContentFormat::BdTs);
    }

    #[test]
    fn content_format_dvd_mpegps() {
        let t = DiscTitle {
            content_format: ContentFormat::MpegPs,
            ..title_with_video(Codec::Mpeg2, Resolution::R480i)
        };
        assert_eq!(t.content_format, ContentFormat::MpegPs);
    }

    #[test]
    fn disc_capacity_gb() {
        // Single-layer BD-25: ~12,219,392 sectors
        let disc = Disc {
            volume_id: String::new(),
            meta_title: None,
            format: DiscFormat::BluRay,
            capacity_sectors: 12_219_392,
            capacity_bytes: 12_219_392u64 * 2048,
            layers: 1,
            titles: Vec::new(),
            region: DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: false,
            content_format: ContentFormat::BdTs,
        };
        let gb = disc.capacity_gb();
        // 12,219,392 * 2048 / 1073741824 = ~23.3 GB
        assert!((gb - 23.3).abs() < 0.1, "expected ~23.3 GB, got {}", gb);

        // Zero sectors
        let disc_zero = Disc {
            capacity_sectors: 0,
            capacity_bytes: 0,
            ..disc
        };
        assert_eq!(disc_zero.capacity_gb(), 0.0);
    }

    #[test]
    fn disc_title_duration_display_edge_cases() {
        let mut t = DiscTitle::empty();

        // 0 seconds
        t.duration_secs = 0.0;
        assert_eq!(t.duration_display(), "0h 00m");

        // 1 second
        t.duration_secs = 1.0;
        assert_eq!(t.duration_display(), "0h 00m");

        // 59 minutes
        t.duration_secs = 59.0 * 60.0;
        assert_eq!(t.duration_display(), "0h 59m");

        // 24 hours
        t.duration_secs = 24.0 * 3600.0;
        assert_eq!(t.duration_display(), "24h 00m");
    }
}
