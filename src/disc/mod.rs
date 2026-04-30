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
pub mod mapfile;

use crate::drive::{Drive, extract_scsi_context};
use crate::error::{Error, Result};
use crate::sector::SectorReader;
use crate::udf;

use encrypt::HandshakeResult;

// Re-export label classification enums alongside AudioStream / SubtitleStream
// so the public surface keeps the structured metadata together. Callers map
// these to display text in their own locale.
pub use crate::labels::{LabelPurpose, LabelQualifier};

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
    /// Stream purpose (commentary / descriptive / score / IME / normal).
    /// Callers translate this to display text in their own locale.
    pub purpose: LabelPurpose,
    /// Codec / variant text (e.g. "Dolby TrueHD 5.1", "(US)").
    /// NEVER contains English purpose words — see `purpose` for that.
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
    /// Subtitle qualifier (SDH / descriptive service / forced / none).
    /// Callers translate this to display text in their own locale.
    pub qualifier: LabelQualifier,
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
    ".config/aacs/KEYDB.cfg",    // libaacs standard path
    ".config/freemkv/keydb.cfg", // freemkv download path
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

/// Quick disc identification — name, format, capacity. No title/stream parsing.
#[derive(Debug)]
pub struct DiscId {
    /// UDF Volume Identifier (always present, e.g. "V_FOR_VENDETTA")
    pub volume_id: String,
    /// Disc title from META/DL/bdmt_eng.xml (e.g. "V for Vendetta")
    pub meta_title: Option<String>,
    /// Disc format (BD, UHD, DVD) — UHD vs BD requires full scan to confirm
    pub format: DiscFormat,
    /// Disc capacity in sectors
    pub capacity_sectors: u32,
    /// Whether AACS directory exists (disc is likely encrypted)
    pub encrypted: bool,
    /// Number of layers
    pub layers: u8,
}

impl DiscId {
    /// Best available name: meta_title, then formatted volume_id.
    pub fn name(&self) -> &str {
        self.meta_title.as_deref().unwrap_or(&self.volume_id)
    }
}

impl Disc {
    /// Fast disc identification — reads only UDF metadata for name and format.
    /// No AACS handshake, no playlist parsing, no CLPI, no labels.
    /// Typically completes in 2-3 seconds on USB drives.
    pub fn identify(session: &mut Drive) -> Result<DiscId> {
        let (capacity, mut buffered, udf_fs) = Self::read_udf(session)?;

        let meta_title = Self::read_meta_title(&mut buffered, &udf_fs);
        let format = if udf_fs.find_dir("/BDMV").is_some() {
            DiscFormat::BluRay // full scan distinguishes UHD vs BD
        } else if udf_fs.find_dir("/VIDEO_TS").is_some() {
            DiscFormat::Dvd
        } else {
            DiscFormat::Unknown
        };
        let encrypted =
            udf_fs.find_dir("/AACS").is_some() || udf_fs.find_dir("/BDMV/AACS").is_some();
        let layers = if capacity > 24_000_000 { 2 } else { 1 };

        Ok(DiscId {
            volume_id: udf_fs.volume_id,
            meta_title,
            format,
            capacity_sectors: capacity,
            encrypted,
            layers,
        })
    }

    /// Disc capacity in GB
    pub fn capacity_gb(&self) -> f64 {
        self.capacity_sectors as f64 * 2048.0 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Read UDF filesystem and set up buffered reader with metadata prefetched.
    /// Shared setup for both identify() and scan().
    fn read_udf(session: &mut Drive) -> Result<(u32, udf::BufferedSectorReader<'_>, udf::UdfFs)> {
        let capacity = Self::read_capacity(session).unwrap_or(0);
        let batch = detect_max_batch_sectors(session.device_path());
        let mut buffered = udf::BufferedSectorReader::new(session, batch);
        let udf_fs = udf::read_filesystem(&mut buffered)?;
        buffered.prefetch(udf_fs.metadata_start(), udf_fs.metadata_sectors());
        Ok((capacity, buffered, udf_fs))
    }

    /// Scan a disc -- parse filesystem, playlists, streams, and set up AACS decryption.
    ///
    /// This is the main entry point. After scan(), the Disc is ready:
    ///   - titles are populated with streams
    ///   - AACS keys are derived (if KEYDB available)
    ///   - content can be read and decrypted transparently
    ///
    /// Scan a disc. One pipeline, one order:
    ///   1. Read capacity + UDF filesystem
    ///   2. AACS handshake + key resolution
    ///   3. Parse playlists + streams
    ///   4. Apply labels
    ///
    /// The session must be open and unlocked (Drive::open handles this).
    /// All disc reads use standard READ(10) via UDF -- no vendor SCSI commands.
    pub fn scan(session: &mut Drive, opts: &ScanOptions) -> Result<Self> {
        // AACS handshake (Blu-ray/UHD)
        let handshake = Self::do_handshake(session, opts);

        // Request max read speed — removes riplock on DVD
        // (BD/UHD speed is set by firmware init, but DVD needs explicit SET CD SPEED)
        session.set_speed(0xFFFF);

        // Read UDF filesystem with buffered sector reader
        let (capacity, mut buffered, udf_fs) = Self::read_udf(session)?;

        // Pre-read all small file sectors (AACS, MPLS, CLPI, META, *.bdmv).
        // Without this, each read_file() triggers individual SCSI commands at 500ms each.
        if let Ok(ranges) = udf_fs.metadata_sector_ranges(&mut buffered) {
            buffered.prefetch_ranges(&ranges);
        }

        let mut disc = Self::scan_with(&mut buffered, capacity, handshake, opts, udf_fs)?;

        // CSS key extraction for DVDs (bus auth → disc key → title key).
        // Must be a single auth session — can't call authenticate() separately.
        if disc.css.is_none()
            && disc.content_format == ContentFormat::MpegPs
            && !disc.titles.is_empty()
        {
            let lba = disc.titles[0].extents.iter().find_map(|ext| {
                let mut buf = vec![0u8; 2048];
                if session
                    .read_sectors(ext.start_lba, 1, &mut buf, true)
                    .is_ok()
                    && crate::css::is_scrambled(&buf)
                {
                    return Some(ext.start_lba);
                }
                None
            });

            if let Some(lba) = lba {
                if let Ok(title_key) =
                    crate::css::auth::authenticate_and_read_title_key(session, lba)
                {
                    disc.css = Some(crate::css::CssState { title_key });
                    disc.encrypted = true;
                }
            }
        }

        Ok(disc)
    }

    /// Scan a disc image (ISO or any SectorReader). No SCSI, no handshake.
    /// AACS resolution uses KEYDB VUK lookup only.
    pub fn scan_image(
        reader: &mut dyn SectorReader,
        capacity: u32,
        opts: &ScanOptions,
    ) -> Result<Self> {
        let udf_fs = udf::read_filesystem(reader)?;
        Self::scan_with(reader, capacity, None, opts, udf_fs)
    }

    /// Core scan pipeline — works with any SectorReader.
    fn scan_with(
        reader: &mut dyn SectorReader,
        capacity: u32,
        handshake: Option<HandshakeResult>,
        opts: &ScanOptions,
        udf_fs: udf::UdfFs,
    ) -> Result<Self> {
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
        crate::labels::fill_defaults(&mut titles);

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
    /// NOT a stream operation. Copies sectors 0→capacity byte-for-byte producing
    /// a valid ISO/UDF image. Records progress in a ddrescue-format mapfile at
    /// `path + ".mapfile"` — flushed every block for crash-safe resume.
    ///
    /// # Options
    /// - **default** (skip_on_error=false): uses full drive recovery (may take
    ///   minutes per bad sector), aborts on error. Mapfile is produced as a
    ///   side-effect.
    /// - **skip_on_error**: zero-fill bad blocks in the ISO, mark them NonTrimmed
    ///   in the mapfile, and continue. Reads in `batch`-sector chunks (32 sectors
    ///   = 1 BD ECC block by default). Failed blocks are marked NonTrimmed for
    ///   recovery by `Disc::patch`. No single-sector reads in this pass —
    ///   Pass 1 is pure ECC-block sweep.
    /// - **resume**: if the mapfile exists, resume from its state — only
    ///   `non-tried` ranges are read. Without `resume`, a fresh mapfile is
    ///   written and the ISO recreated from scratch.
    pub fn copy(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &CopyOptions,
    ) -> Result<CopyResult> {
        use std::io::{Seek, SeekFrom, Write};

        let total_bytes = self.capacity_sectors as u64 * 2048;
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        // Mapfile: load if resuming, else wipe + recreate.
        let mapfile_path = mapfile_path_for(path);
        if !opts.resume {
            let _ = std::fs::remove_file(&mapfile_path);
        }
        let mut map =
            mapfile::Mapfile::open_or_create(&mapfile_path, total_bytes, concat!("libfreemkv v", env!("CARGO_PKG_VERSION")))
                .map_err(|e| Error::IoError { source: e })?;

        // ISO file: if resuming and mapfile has Finished ranges, open existing;
        // otherwise create fresh and pre-size to total_bytes (sparse holes for
        // non-tried regions).
        let file = if opts.resume
            && std::fs::metadata(path)
                .map(|m| m.len() > 0)
                .unwrap_or(false)
        {
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| Error::IoError { source: e })?
        } else {
            let f = std::fs::File::create(path).map_err(|e| Error::IoError { source: e })?;
            f.set_len(total_bytes)
                .map_err(|e| Error::IoError { source: e })?;
            f
        };

        let mut file = file;
        let batch: u16 = match opts.batch_sectors {
            Some(b) => b,
            None if opts.skip_on_error => 32,
            None => DEFAULT_BATCH_SECTORS,
        };

        let mut buf = vec![0u8; batch as usize * 2048];
        let mut bytes_done = 0u64;
        let mut halt_requested = false;
        let copy_t0 = std::time::Instant::now();
        let mut iter_count: u64 = 0;
        let mut read_ok_count: u64 = 0;
        let mut read_err_count: u64 = 0;
        let mut last_log_iter: u64 = 0;
        let mut not_ready_retries: u32 = 0;
        const NOT_READY_MAX_RETRIES: u32 = 3;
        let mut bridge_degradation_count: u32 = 0;
        const BRIDGE_DEGRADATION_MAX: u32 = 5;
        const BRIDGE_DEGRADATION_COOLDOWN_SECS: u64 = 10;
        const DAMAGE_WINDOW: usize = 50;
        const DAMAGE_THRESHOLD_PCT: usize = 25;
        const JUMP_SECTORS_FACTOR: u64 = 256;
        let mut damage_window: Vec<bool> = Vec::with_capacity(DAMAGE_WINDOW);
        let mut jump_multiplier: u64 = 1;
        let mut consecutive_good: u64 = 0;
        tracing::trace!(
            target: "freemkv::disc",
            phase = "copy_start",
            total_bytes,
            batch,
            skip_on_error = opts.skip_on_error,
            "Disc::copy entered"
        );

        'outer: loop {
            let regions_to_do = map.ranges_with(&[
                mapfile::SectorStatus::NonTried,
                mapfile::SectorStatus::NonTrimmed,
                mapfile::SectorStatus::NonScraped,
            ]);
            tracing::trace!(
                target: "freemkv::disc",
                phase = "outer_loop",
                regions_remaining = regions_to_do.len(),
                "Disc::copy outer iter"
            );
            if regions_to_do.is_empty() {
                break;
            }
            let Some((region_pos, region_size)) = map.next_with(0, mapfile::SectorStatus::NonTried)
            else {
                break;
            };
            let region_end = region_pos + region_size;
            let mut pos = region_pos;
            tracing::trace!(
                target: "freemkv::disc",
                phase = "region_enter",
                region_pos,
                region_size,
                region_end,
                "entering NonTried region"
            );

            while pos < region_end {
                if let Some(ref h) = opts.halt {
                    if h.load(std::sync::atomic::Ordering::Relaxed) {
                        halt_requested = true;
                        break 'outer;
                    }
                }

                let block_bytes = (region_end - pos).min(batch as u64 * 2048);
                let block_lba = (pos / 2048) as u32;
                let block_count = (block_bytes / 2048) as u16;
                let recovery = !opts.skip_on_error;

                let read_result = reader.read_sectors(
                    block_lba,
                    block_count,
                    &mut buf[..block_bytes as usize],
                    recovery,
                );

                let mut did_skip_ahead = false;

                if read_result.is_ok() {
                    read_ok_count += 1;
                    damage_window.push(true);
                    if damage_window.len() > DAMAGE_WINDOW {
                        damage_window.remove(0);
                    }
                    consecutive_good += 1;
                    if consecutive_good >= DAMAGE_WINDOW as u64 {
                        jump_multiplier = 1;
                    }
                    bridge_degradation_count = 0;

                    if opts.decrypt {
                        crate::decrypt::decrypt_sectors(
                            &mut buf[..block_bytes as usize],
                            &keys,
                            0,
                        )?;
                    }
                    file.seek(SeekFrom::Start(pos))
                        .map_err(|e| Error::IoError { source: e })?;
                    file.write_all(&buf[..block_bytes as usize])
                        .map_err(|e| Error::IoError { source: e })?;
                    map.record(pos, block_bytes, mapfile::SectorStatus::Finished)
                        .map_err(|e| Error::IoError { source: e })?;
                    bytes_done = bytes_done.saturating_add(block_bytes);
                } else if !opts.skip_on_error {
                    let (status, sense) = read_result
                        .as_ref()
                        .err()
                        .map(extract_scsi_context)
                        .unwrap_or((0, None));
                    return Err(Error::DiscRead {
                        sector: block_lba as u64,
                        status: Some(status),
                        sense,
                    });
                } else {
                    let err = read_result.err().unwrap();
                    read_err_count += 1;
                    consecutive_good = 0;

                    if err.is_scsi_transport_failure() {
                        tracing::warn!(
                            target: "freemkv::disc",
                            phase = "transport_failure",
                            lba = block_lba,
                            error = %err,
                            "transport failure (bridge crash); aborting copy — caller should USB reset + resume"
                        );
                        return Err(err);
                    }

                    if err.is_bridge_degradation() {
                        if bridge_degradation_count < BRIDGE_DEGRADATION_MAX {
                            bridge_degradation_count += 1;
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "bridge_degradation",
                                lba = block_lba,
                                degradation_count = bridge_degradation_count,
                                error = %err,
                                "bridge degradation; cooling down 10s"
                            );
                            std::thread::sleep(std::time::Duration::from_secs(BRIDGE_DEGRADATION_COOLDOWN_SECS));
                            continue;
                        }
                        tracing::warn!(
                            target: "freemkv::disc",
                            phase = "bridge_degradation_exhausted",
                            lba = block_lba,
                            "bridge degradation retries exhausted; treating as bad sector"
                        );
                    }

                    let sense = err.scsi_sense();
                    let sense_key = sense.map(|s| s.sense_key).unwrap_or(0);
                    let asc = sense.map(|s| s.asc).unwrap_or(0);
                    let ascq = sense.map(|s| s.ascq).unwrap_or(0);

                    if sense_key == crate::scsi::SENSE_KEY_NOT_READY && not_ready_retries < NOT_READY_MAX_RETRIES {
                        not_ready_retries += 1;
                        tracing::warn!(
                            target: "freemkv::disc",
                            phase = "not_ready_pause",
                            lba = block_lba,
                            sense_key,
                            asc,
                            ascq,
                            retry = not_ready_retries,
                            "NOT READY; pausing 3s then retrying"
                        );
                        std::thread::sleep(std::time::Duration::from_secs(3));
                        continue;
                    }
                    not_ready_retries = 0;

                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "skip_ecc_block",
                        lba = block_lba,
                        sectors = block_count,
                        sense_key,
                        asc,
                        ascq,
                        error = %err,
                        "ECC block failed; marking NonTrimmed"
                    );
                    let zero = vec![0u8; block_bytes as usize];
                    file.seek(SeekFrom::Start(pos))
                        .map_err(|e| Error::IoError { source: e })?;
                    file.write_all(&zero)
                        .map_err(|e| Error::IoError { source: e })?;
                    map.record(pos, block_bytes, mapfile::SectorStatus::NonTrimmed)
                        .map_err(|e| Error::IoError { source: e })?;
                    bytes_done = bytes_done.saturating_add(block_bytes);

                    damage_window.push(false);
                    if damage_window.len() > DAMAGE_WINDOW {
                        damage_window.remove(0);
                    }

                    let bad_count = damage_window.iter().filter(|&&b| !b).count();
                    if damage_window.len() >= DAMAGE_WINDOW
                        && bad_count * 100 / damage_window.len() >= DAMAGE_THRESHOLD_PCT
                    {
                        let jump_sectors = JUMP_SECTORS_FACTOR * batch as u64 * jump_multiplier;
                        let jump_lba = ((pos / 2048) + jump_sectors) as u32;
                        let region_end_lba = (region_end / 2048) as u32;
                        if jump_lba < region_end_lba {
                            let jump_pos = jump_lba as u64 * 2048;
                            let gap_start = pos + block_bytes;
                            let gap_bytes = jump_pos.saturating_sub(gap_start);
                            let jump_mb = gap_bytes / 1_048_576;
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "damage_jump",
                                from_lba = block_lba,
                                to_lba = jump_lba,
                                jump_mb,
                                bad_pct = bad_count * 100 / damage_window.len(),
                                multiplier = jump_multiplier,
                                "25%+ failures in last 50 blocks; jumping ahead"
                            );
                            if gap_bytes > 0 {
                                let zero = vec![0u8; 65536];
                                let mut filled: u64 = 0;
                                while filled < gap_bytes {
                                    let chunk = (gap_bytes - filled).min(zero.len() as u64);
                                    file.seek(SeekFrom::Start(gap_start + filled))
                                        .map_err(|e| Error::IoError { source: e })?;
                                    file.write_all(&zero[..chunk as usize])
                                        .map_err(|e| Error::IoError { source: e })?;
                                    filled += chunk;
                                }
                                map.record(gap_start, gap_bytes, mapfile::SectorStatus::NonTrimmed)
                                    .map_err(|e| Error::IoError { source: e })?;
                                bytes_done = bytes_done.saturating_add(gap_bytes);
                            }
                            pos = jump_pos;
                            jump_multiplier *= 2;
                            did_skip_ahead = true;
                        }
                    }
                }

                if !did_skip_ahead {
                    pos += block_bytes;
                }
                iter_count += 1;

                if iter_count - last_log_iter >= 100 {
                    last_log_iter = iter_count;
                    let stats = map.stats();
                    tracing::trace!(
                        target: "freemkv::disc",
                        phase = "iter_progress",
                        iter_count,
                        read_ok_count,
                        read_err_count,
                        pos,
                        region_end,
                        bytes_good = stats.bytes_good,
                        bytes_pending = stats.bytes_pending,
                        copy_elapsed_ms = copy_t0.elapsed().as_millis() as u64,
                        "Disc::copy inner iter"
                    );
                }

                if let Some(reporter) = opts.progress {
                    let stats = map.stats();
                    reporter.report(&crate::progress::PassProgress {
                        kind: crate::progress::PassKind::Sweep,
                        work_done: pos,
                        work_total: total_bytes,
                        bytes_good_total: stats.bytes_good,
                        bytes_bad_total: stats.bytes_unreadable + stats.bytes_retryable,
                        bytes_total_disc: total_bytes,
                    });
                }
            }
        }

        file.sync_all().map_err(|e| Error::IoError { source: e })?;
        let stats = map.stats();
        tracing::trace!(
            target: "freemkv::disc",
            phase = "copy_done",
            iter_count,
            read_ok_count,
            read_err_count,
            bytes_good = stats.bytes_good,
            bytes_pending = stats.bytes_pending,
            halted = halt_requested,
            copy_elapsed_ms = copy_t0.elapsed().as_millis() as u64,
            "Disc::copy returning"
        );
        Ok(CopyResult {
            bytes_total: total_bytes,
            bytes_good: stats.bytes_good,
            bytes_unreadable: stats.bytes_unreadable,
            bytes_pending: stats.bytes_pending,
            complete: stats.bytes_pending == 0 && !halt_requested,
            halted: halt_requested,
        })
    }
}

/// Options for `Disc::copy`. All fields default to the pre-v0.11.21 behavior
/// (recovery reads, abort on bad sector).
#[derive(Default)]
pub struct CopyOptions<'a> {
    pub decrypt: bool,
    pub resume: bool,
    pub batch_sectors: Option<u16>,
    pub skip_on_error: bool,
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

/// Result of `Disc::copy`. `complete=true` means every byte reached a terminal
/// state (Finished or Unreadable). `complete=false` means there's still pending
/// work (halt, abort, or non-tried ranges) that `Disc::patch` or a resumed
/// `Disc::copy` would continue.
#[derive(Debug, Clone, Copy)]
pub struct CopyResult {
    pub bytes_total: u64,
    pub bytes_good: u64,
    pub bytes_unreadable: u64,
    pub bytes_pending: u64,
    pub complete: bool,
    pub halted: bool,
}

/// Sidecar mapfile path for a given ISO path — `foo.iso` → `foo.iso.mapfile`.
pub fn mapfile_path_for(iso_path: &std::path::Path) -> std::path::PathBuf {
    let mut s = iso_path.as_os_str().to_os_string();
    s.push(".mapfile");
    std::path::PathBuf::from(s)
}

/// Options for `Disc::patch`. Idempotent — each call is one patch attempt.
#[derive(Default)]
pub struct PatchOptions<'a> {
    pub decrypt: bool,
    /// Sector-granularity block size for retries. Defaults to 1 sector (2 KB).
    pub block_sectors: Option<u16>,
    /// Use full drive-level recovery on each read (slow but thorough). Defaults
    /// to true — patch is the pass where we *want* the drive to try hard.
    pub full_recovery: bool,
    /// Walk bad ranges in reverse order, and within each range walk sectors
    /// from high to low LBA. Useful for drives that wedge after a forward
    /// read of a bad sector — approaching the post-bad-zone from end-of-disc
    /// reads good sectors before the drive sees a bad one.
    pub reverse: bool,
    /// Bail out early if this many consecutive read failures occur with zero
    /// successful reads in the same pass — i.e. the drive is wedged on the
    /// bad zone and won't recover during this attempt. `0` disables the
    /// guard (run to completion or halt).
    pub wedged_threshold: u64,
    /// Per-iteration progress reporter. See `CopyOptions::progress`.
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

/// Result of `Disc::patch` — how many bad bytes were recovered, plus
/// per-block counters for diagnosing why a pass made or didn't make progress.
#[derive(Debug, Clone, Copy)]
pub struct PatchResult {
    pub bytes_total: u64,
    pub bytes_good: u64,
    pub bytes_unreadable: u64,
    pub bytes_pending: u64,
    pub bytes_recovered_this_pass: u64,
    pub halted: bool,
    /// Total inner-loop iterations this pass (one per block attempted).
    pub blocks_attempted: u64,
    /// Reads that returned Ok and were promoted to `Finished`.
    pub blocks_read_ok: u64,
    /// Reads that returned Err and were marked `Unreadable`.
    pub blocks_read_failed: u64,
    /// Pass exited early because `wedged_threshold` consecutive failures
    /// occurred with zero successful reads — drive appears wedged on the
    /// bad zone for this pass.
    pub wedged_exit: bool,
}

impl Disc {
    /// Patch an existing ISO using its sidecar mapfile. Re-reads every range
    /// that's not yet `+` (Finished) and writes successful bytes into the ISO
    /// at their exact offsets. Updates mapfile entries as it goes.
    ///
    /// Idempotent — call repeatedly to apply more retry attempts. Stops early
    /// if a pass recovered zero bytes (no point continuing).
    pub fn patch(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &PatchOptions,
    ) -> Result<PatchResult> {
        use std::io::{Seek, SeekFrom, Write};

        let mapfile_path = mapfile_path_for(path);
        let mut map =
            mapfile::Mapfile::load(&mapfile_path).map_err(|e| Error::IoError { source: e })?;
        let total_bytes = map.total_size();
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| Error::IoError { source: e })?;

        let block_sectors = opts.block_sectors.unwrap_or(1);
        let recovery = opts.full_recovery;

        let bytes_good_before = map.stats().bytes_good;
        let mut halted = false;
        let mut wedged_exit = false;
        let mut blocks_attempted: u64 = 0;
        let mut blocks_read_ok: u64 = 0;
        let mut blocks_read_failed: u64 = 0;
        let mut consecutive_failures: u64 = 0;
        let mut unreadable_count: u64 = 0;
        let mut buf = vec![0u8; block_sectors as usize * 2048];

        // Collect bad ranges up front. Iterating while mutating is fragile;
        // each recorded change is persisted, so resume works even if we crash
        // mid-loop.
        let mut bad_ranges = map.ranges_with(&[
            mapfile::SectorStatus::NonTried,
            mapfile::SectorStatus::NonTrimmed,
            mapfile::SectorStatus::NonScraped,
            mapfile::SectorStatus::Unreadable,
        ]);
        // Reverse mode: walk ranges from highest LBA to lowest.
        if opts.reverse {
            bad_ranges.reverse();
        }
        // work_total = sum of all bad-range bytes. on_progress's third arg
        // is this value; the second arg (work_done) is incremented per block
        // attempted. UI consumers (autorip) compute pass_progress_pct =
        // work_done / work_total — true 0..100% per pass per RIP_DESIGN.md §16.
        let work_total: u64 = bad_ranges.iter().map(|(_, sz)| *sz).sum();
        let mut work_done: u64 = 0;
        tracing::trace!(
            target: "freemkv::disc",
            phase = "patch_start",
            block_sectors,
            recovery,
            reverse = opts.reverse,
            wedged_threshold = opts.wedged_threshold,
            num_ranges = bad_ranges.len(),
            work_total,
            "Disc::patch entered"
        );

        'outer: for (range_pos, range_size) in bad_ranges {
            let end = range_pos + range_size;
            // In reverse mode, walk this range from end - block_bytes back to range_pos.
            // Each iteration emits the block ending at `block_end` (so reads land on
            // increasing LBAs internally; we just choose blocks back-to-front).
            let mut block_end = if opts.reverse { end } else { range_pos };
            loop {
                if let Some(ref h) = opts.halt {
                    if h.load(std::sync::atomic::Ordering::Relaxed) {
                        halted = true;
                        break 'outer;
                    }
                }
                // Compute block boundaries based on direction.
                let (pos, block_bytes) = if opts.reverse {
                    if block_end <= range_pos {
                        break;
                    }
                    let span = (block_end - range_pos).min(block_sectors as u64 * 2048);
                    (block_end - span, span)
                } else {
                    if block_end >= end {
                        break;
                    }
                    let span = (end - block_end).min(block_sectors as u64 * 2048);
                    (block_end, span)
                };
                let lba = (pos / 2048) as u32;
                let count = (block_bytes / 2048) as u16;
                let bytes = count as usize * 2048;
                blocks_attempted += 1;
                let read_result = reader.read_sectors(lba, count, &mut buf[..bytes], recovery);
                // 0.13.23: parity with Disc::copy — bail immediately on a
                // non-marginal SCSI sense (transport failure, HARDWARE
                // ERROR, DATA PROTECT, UNIT ATTENTION, NOT READY,
                // ILLEGAL REQUEST, kernel IoError). The wedged_threshold
                // counter is a defense-in-depth backstop for ~50
                // consecutive marginal failures, but a single
                // non-marginal error already proves the drive can't
                // produce data this pass — don't waste 50× the timeout
                // budget proving it again.
                if let Err(ref e) = read_result {
                    if !e.is_marginal_read() {
                        let err = read_result.err().unwrap();
                        tracing::trace!(
                            target: "freemkv::disc",
                            phase = "patch_bail",
                            lba,
                            error = %err,
                            "patch read failed with non-marginal sense; bailing"
                        );
                        return Err(err);
                    }
                }
                let read_ok = read_result.is_ok();
                if read_ok {
                    blocks_read_ok += 1;
                    consecutive_failures = 0;
                    if opts.decrypt {
                        crate::decrypt::decrypt_sectors(&mut buf[..bytes], &keys, 0)?;
                    }
                    file.seek(SeekFrom::Start(pos))
                        .map_err(|e| Error::IoError { source: e })?;
                    file.write_all(&buf[..bytes])
                        .map_err(|e| Error::IoError { source: e })?;
                    map.record(pos, block_bytes, mapfile::SectorStatus::Finished)
                        .map_err(|e| Error::IoError { source: e })?;
                } else {
                    blocks_read_failed += 1;
                    consecutive_failures += 1;
                    unreadable_count += 1;
                    map.record(pos, block_bytes, mapfile::SectorStatus::Unreadable)
                        .map_err(|e| Error::IoError { source: e })?;
                }
                // Advance block_end in chosen direction.
                if opts.reverse {
                    block_end = block_end.saturating_sub(block_bytes);
                } else {
                    block_end += block_bytes;
                }

                // Wedged-drive early-exit: many consecutive failures with zero
                // recovered bytes this pass means the drive is stuck and won't
                // produce data this pass. Save the wallclock budget for productive
                // grinding; future passes (with smaller block size, reverse, or
                // after settle) may still recover.
                if opts.wedged_threshold > 0
                    && consecutive_failures >= opts.wedged_threshold
                    && blocks_read_ok == 0
                {
                    tracing::trace!(
                        target: "freemkv::disc",
                        phase = "patch_wedged_exit",
                        consecutive_failures,
                        blocks_read_failed,
                        "Disc::patch giving up — drive appears wedged"
                    );
                    wedged_exit = true;
                    break 'outer;
                }

                // Track work done in this pass for the per-pass progress bar.
                // Each block iterated counts as work, regardless of read
                // outcome — a failed retry is still progress through the
                // bad-range walk.
                work_done = work_done.saturating_add(block_bytes);

                if let Some(reporter) = opts.progress {
                    let s = map.stats();
                    let kind = if block_sectors == 1 {
                        crate::progress::PassKind::Scrape {
                            reverse: opts.reverse,
                        }
                    } else {
                        crate::progress::PassKind::Trim {
                            reverse: opts.reverse,
                        }
                    };
                    reporter.report(&crate::progress::PassProgress {
                        kind,
                        work_done,
                        work_total,
                        bytes_good_total: s.bytes_good,
                        bytes_bad_total: s.bytes_unreadable + s.bytes_retryable,
                        bytes_total_disc: total_bytes,
                    });
                }
            }
        }

        file.sync_all().map_err(|e| Error::IoError { source: e })?;
        let stats = map.stats();
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_done",
            blocks_attempted,
            blocks_read_ok,
            blocks_read_failed,
            unreadable_count,
            wedged_exit,
            halted,
            bytes_recovered = stats.bytes_good.saturating_sub(bytes_good_before),
            "Disc::patch returning"
        );
        Ok(PatchResult {
            bytes_total: total_bytes,
            bytes_good: stats.bytes_good,
            bytes_unreadable: stats.bytes_unreadable,
            bytes_pending: stats.bytes_pending,
            bytes_recovered_this_pass: stats.bytes_good.saturating_sub(bytes_good_before),
            halted,
            blocks_attempted,
            blocks_read_ok,
            blocks_read_failed,
            wedged_exit,
        })
    }
}

const MAX_BATCH_SECTORS: u16 = 510;
const DEFAULT_BATCH_SECTORS: u16 = 60;
const MIN_BATCH_SECTORS: u16 = 3;

pub fn ecc_sectors(format: DiscFormat) -> u16 {
    match format {
        DiscFormat::Uhd | DiscFormat::BluRay => 32,
        DiscFormat::Dvd => 16,
        DiscFormat::Unknown => 32,
    }
}

/// Coarse damage tier for a finished or in-progress rip. Maps the
/// observable signals (bad sector count + lost wallclock playback time)
/// onto a small discrete classification so UIs can render a colored badge
/// and operators can decide whether to rescan / replug / accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DamageSeverity {
    /// No bad sectors at all.
    Clean,
    /// 1–50 bad sectors AND <1 sec lost. Likely unnoticeable.
    Cosmetic,
    /// 51–500 sectors OR 1–30 sec lost. Visible artifacts possible.
    Moderate,
    /// 500+ sectors OR 30+ sec lost. Significant damage; consider rescan
    /// or different drive.
    Serious,
}

/// Classify damage severity from raw counters. `bad_sectors` is the
/// number of sectors marked unreadable (or NonTrimmed pending Pass 2);
/// `lost_ms` is the cumulative wallclock playback time those sectors
/// represent (computed from the title's bytes-per-sec).
pub fn classify_damage(bad_sectors: u64, lost_ms: f64) -> DamageSeverity {
    if bad_sectors == 0 {
        return DamageSeverity::Clean;
    }
    if bad_sectors >= 500 || lost_ms >= 30_000.0 {
        return DamageSeverity::Serious;
    }
    if bad_sectors >= 51 || lost_ms >= 1_000.0 {
        return DamageSeverity::Moderate;
    }
    DamageSeverity::Cosmetic
}

#[cfg(test)]
mod severity_tests {
    use super::*;
    #[test]
    fn clean_when_no_damage() {
        assert_eq!(classify_damage(0, 0.0), DamageSeverity::Clean);
    }
    #[test]
    fn cosmetic_for_a_handful() {
        assert_eq!(classify_damage(1, 5.0), DamageSeverity::Cosmetic);
        assert_eq!(classify_damage(50, 999.0), DamageSeverity::Cosmetic);
    }
    #[test]
    fn moderate_threshold_by_sectors() {
        assert_eq!(classify_damage(51, 0.0), DamageSeverity::Moderate);
    }
    #[test]
    fn moderate_threshold_by_time() {
        assert_eq!(classify_damage(10, 1_000.0), DamageSeverity::Moderate);
    }
    #[test]
    fn serious_threshold_by_sectors() {
        assert_eq!(classify_damage(500, 0.0), DamageSeverity::Serious);
    }
    #[test]
    fn serious_threshold_by_time() {
        assert_eq!(classify_damage(10, 30_000.0), DamageSeverity::Serious);
    }
}

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
