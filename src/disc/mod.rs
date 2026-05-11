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
mod patch;
pub mod read_error;
mod sweep;

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
    /// AACS resolution error when `encrypted` is true and `aacs` is None.
    /// Lets callers distinguish "no KEYDB found", "KEYDB failed to parse",
    /// "disc hash not in KEYDB", etc. None when AACS resolution wasn't
    /// attempted (unencrypted disc) or succeeded.
    pub aacs_error: Option<crate::error::Error>,
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

/// Calculate how many bytes of bad/unreadable data fall within a title's extents.
/// `pub(crate)` so autorip can use it for main-movie lost_ms computation.
pub fn bytes_bad_in_title(title: &DiscTitle, bad_ranges: &[(u64, u64)]) -> u64 {
    if bad_ranges.is_empty() || title.extents.is_empty() {
        return 0;
    }
    let t_start = title.extents.first().map(|e| (e.start_lba as u64) * 2048);
    let t_end = title
        .extents
        .last()
        .map(|e| ((e.start_lba as u64) + (e.sector_count as u64)) * 2048);
    let (Some(ts), Some(te)) = (t_start, t_end) else {
        return 0;
    };
    bad_ranges
        .iter()
        .map(|(pos, size)| {
            let r_start = *pos;
            let r_end = *pos + *size;
            let overlap_start = r_start.max(ts);
            let overlap_end = r_end.min(te);
            overlap_end.saturating_sub(overlap_start)
        })
        .sum()
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

        let (aacs, aacs_error) = if encrypted {
            match opts.resolve_keydb() {
                Some(keydb_path) => {
                    match Self::resolve_encryption(&udf_fs, reader, &keydb_path, handshake.as_ref())
                    {
                        Ok(state) => (Some(state), None),
                        Err(e) => {
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "scan_aacs_resolve_failed",
                                error_code = e.code(),
                                keydb = %keydb_path.display(),
                                handshake_ok = handshake.is_some(),
                                "AACS key resolution failed"
                            );
                            (None, Some(e))
                        }
                    }
                }
                None => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "scan_aacs_no_keydb",
                        "encrypted disc but no KEYDB found in search paths"
                    );
                    // Reuse KeydbLoad with sentinel path — adding a new Error
                    // variant would be a breaking change for downstream
                    // exhaustive matches. The path string makes the cause
                    // unambiguous to autorip's message switch.
                    (
                        None,
                        Some(crate::error::Error::KeydbLoad {
                            path: String::from("<no keydb in search paths>"),
                        }),
                    )
                }
            }
        } else {
            (None, None)
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
        // Title ordering: titles[0] should be the canonical main feature.
        //
        // Naive "longest duration first" misranks branching UHDs (see
        // `canonical_title_order` for the full rationale). Sort the
        // titles so the consumer-side `-t 1` / autorip's main-feature
        // picker / `disc.titles.first()` all converge on the actual
        // movie instead of the virtual play-all composite.
        let capacity_bytes = capacity as u64 * 2048;
        titles.sort_by(|a, b| Self::canonical_title_order(a, b, capacity_bytes));

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
            aacs_error,
            content_format,
        })
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Detect disc format from the main title's video streams.
    /// Total ordering used to sort `Disc::titles` so `titles[0]` is the
    /// canonical main feature.
    ///
    /// **Why not just sort by duration descending?** Branching UHDs
    /// (and some BD authoring) ship a "play-all" virtual playlist that
    /// references the same source clips multiple times for seamless
    /// alternate-angle / alternate-ending playback. Those playlists
    /// report an inflated `duration_secs` (often 4+ hours) and an
    /// inflated `size_bytes` greater than the disc's physical
    /// capacity. Example seen in the wild — *The Amateur (2025)* UHD,
    /// 58.5 GB BD-100 disc:
    ///
    /// | Title | Playlist     | Duration | Size    | Clips |
    /// |-------|--------------|----------|---------|-------|
    /// |   1   | 00020.mpls   | 4h 13m   | 92.4 GB |  253  |
    /// |   2   | 00800.mpls   | 2h 02m   | 57.2 GB |    1  |
    ///
    /// Title 1's 92.4 GB cannot fit on a 58.5 GB disc unless the same
    /// clip data is referenced multiple times — proof it's a virtual
    /// composite. A duration-only sort would put it at `titles[0]`,
    /// so `freemkv -t 1`, `disc.titles.first()`, and autorip's
    /// main-feature picker all grab the 4-hour composite instead of
    /// the 2-hour movie that actually matches TMDB.
    ///
    /// **Sort priority (titles[0] = most likely main feature):**
    /// 1. Real titles (`size_bytes ≤ capacity_bytes`) before virtual
    ///    composites. The capacity check is a hard "physically
    ///    possible data on this disc" gate.
    /// 2. Among real titles, fewer clips first. A 1-clip playlist is
    ///    the canonical main feature; multi-clip playlists are either
    ///    chapter-stitched (small count) or virtual composites
    ///    (large count). Fewer wins.
    /// 3. Tiebreak on longer duration first.
    ///
    /// **Effect on non-branching discs:** unchanged — the main movie
    /// is already the longest 1-clip title.
    /// **Effect on branching UHDs:** the virtual play-all playlist is
    /// pushed to the back, the actual movie surfaces at index 0.
    pub fn canonical_title_order(
        a: &DiscTitle,
        b: &DiscTitle,
        capacity_bytes: u64,
    ) -> std::cmp::Ordering {
        let a_oversize = a.size_bytes > capacity_bytes;
        let b_oversize = b.size_bytes > capacity_bytes;
        a_oversize
            .cmp(&b_oversize)
            .then_with(|| a.clips.len().cmp(&b.clips.len()))
            .then_with(|| b.duration_secs.total_cmp(&a.duration_secs))
    }

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

    /// Copy disc sectors to an ISO image file.
    ///
    /// NOT a stream operation. Copies sectors byte-for-byte producing a valid
    /// ISO/UDF image. Records progress in a ddrescue-format mapfile at
    /// `path + ".mapfile"` — flushed every block for crash-safe resume.
    ///
    /// Auto-detects the pass based on mapfile state:
    /// - **No mapfile** → Pass 1 (sweep): sequential read of the entire disc,
    ///   ECC-aligned batches, damage-jump on contiguous failures, marks bad
    ///   blocks as NonTrimmed. No drive-level recovery — fast.
    /// - **Mapfile with bad ranges** → Pass N (patch): re-reads only bad ranges
    ///   sector-by-sector with full drive-level recovery. Marks recovered
    ///   sectors as Finished, failed as Unreadable (terminal).
    /// - **Mapfile clean** → no-op: all sectors are Finished.
    ///
    /// Without `multipass`: aborts on the first read error (legacy single-pass).
    pub fn copy(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &CopyOptions,
    ) -> Result<CopyResult> {
        if opts.multipass {
            let mf_path = self.mapfile_for(path);
            if mf_path.exists() {
                let map =
                    mapfile::Mapfile::load(&mf_path).map_err(|e| Error::IoError { source: e })?;
                let stats = map.stats();
                let disc_size = self.capacity_bytes;
                let covers_disc = map.total_size() == disc_size;
                let bad_bytes = stats.bytes_pending + stats.bytes_unreadable;
                tracing::info!(
                    "copy dispatch: disc={} map={} covers={} good={} nontried={} pending={} unreadable={}",
                    disc_size,
                    map.total_size(),
                    covers_disc,
                    stats.bytes_good,
                    stats.bytes_nontried,
                    stats.bytes_pending,
                    stats.bytes_unreadable,
                );
                if covers_disc && bad_bytes == 0 {
                    return Ok(CopyResult {
                        bytes_total: disc_size,
                        bytes_good: stats.bytes_good,
                        bytes_unreadable: stats.bytes_unreadable,
                        bytes_pending: 0,
                        recovered_this_pass: 0,
                        complete: true,
                        halted: false,
                    });
                }
                if !covers_disc {
                    tracing::info!("copy dispatch: → sweep (covers_disc={})", covers_disc,);
                    return self.sweep_internal(reader, path, opts, true);
                }
                if stats.bytes_retryable > 0 {
                    tracing::info!(
                        "copy dispatch: → patch (retryable={})",
                        stats.bytes_retryable,
                    );
                    return self.patch_internal(reader, path, opts);
                }
                tracing::info!("copy dispatch: → sweep (resume)");
                return self.sweep_internal(reader, path, opts, true);
            }
        }
        self.sweep_internal(reader, path, opts, false)
    }

    fn sweep_internal(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &CopyOptions,
        resume: bool,
    ) -> Result<CopyResult> {
        let sweep_opts = SweepOptions {
            decrypt: opts.decrypt,
            resume,
            batch_sectors: None,
            skip_on_error: opts.multipass,
            progress: opts.progress,
            halt: opts.halt.clone(),
        };
        self.sweep(reader, path, &sweep_opts)
    }

    fn patch_internal(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &CopyOptions,
    ) -> Result<CopyResult> {
        let patch_opts = PatchOptions {
            decrypt: opts.decrypt,
            // 0.18.13: adaptive batching. patch() reads at 32 sectors
            // when the drive is healthy, drops to 1 on failure to
            // probe each sector individually, then climbs back after
            // 16 consecutive clean singles. Walks NonTrimmed regions
            // ~32x faster in clean stretches without sacrificing any
            // per-sector recovery quality — the drop-to-1 retry from
            // the same position guarantees every sector in a failed
            // batch is individually probed. See Disc::patch body.
            block_sectors: Some(32),
            full_recovery: true,
            reverse: true,
            wedged_threshold: 50,
            progress: opts.progress,
            halt: opts.halt.clone(),
        };
        let pr = self.patch(reader, path, &patch_opts)?;
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_done",
            blocks_attempted = pr.blocks_attempted,
            blocks_read_ok = pr.blocks_read_ok,
            blocks_read_failed = pr.blocks_read_failed,
            bytes_recovered = pr.bytes_recovered_this_pass,
            halted = pr.halted,
            wedged_exit = pr.wedged_exit,
            "Patch completed"
        );
        Ok(CopyResult {
            bytes_total: pr.bytes_total,
            bytes_good: pr.bytes_good,
            bytes_unreadable: pr.bytes_unreadable,
            bytes_pending: pr.bytes_pending,
            recovered_this_pass: pr.bytes_recovered_this_pass,
            complete: pr.bytes_pending == 0,
            halted: pr.halted,
        })
    }

    /// Pass 1 of a multipass rip: walk the disc forward, write
    /// every readable sector into `path`, and record the result
    /// in the sidecar mapfile. With `skip_on_error: true`, a bad
    /// sector zero-fills + marks `NonTrimmed` and the sweep keeps
    /// going (jumping ahead through dense damage); without it,
    /// the first read failure aborts.
    ///
    /// 0.18: this is one of the two flat verbs the library exposes
    /// for rip orchestration. Multipass + retry decisions are the
    /// caller's job — see [`PatchOptions`] for the retry primitive.
    pub fn sweep(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &SweepOptions,
    ) -> Result<CopyResult> {
        use crate::io::{DEFAULT_PIPELINE_DEPTH, Pipeline};
        use crate::sector::{DecryptingSectorSource, SectorSource};
        use sweep::{ProgressSnapshot, SweepSink, WorkItem, try_recv_progress};

        let total_bytes = self.capacity_sectors as u64 * 2048;
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        // Wrap the producer-side reader once so every read_sectors call
        // yields plaintext. `DecryptKeys::None` makes the decorator a
        // pass-through, so the wrapping is cheap when --raw / unencrypted
        // discs are being swept and we keep the pipeline shape uniform.
        // Replaces the inline `decrypt::decrypt_sectors` calls that used
        // to live in this loop and in the bisect inner loop below.
        let mut reader = DecryptingSectorSource::new(reader, keys);
        let reader = &mut reader;

        // Mapfile: load if resuming, else wipe + recreate.
        let mapfile_path = self.mapfile_for(path);
        if !opts.resume {
            let _ = std::fs::remove_file(&mapfile_path);
        }
        let map = mapfile::Mapfile::open_or_create(
            &mapfile_path,
            total_bytes,
            concat!("libfreemkv v", env!("CARGO_PKG_VERSION")),
        )
        .map_err(|e| Error::IoError { source: e })?;

        // ISO file: if resuming and mapfile has Finished ranges, open existing;
        // otherwise create fresh and pre-size to total_bytes (sparse holes for
        // non-tried regions).
        let is_regular = std::fs::metadata(path)
            .map(|m| m.file_type().is_file())
            .unwrap_or(false);
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
            if is_regular {
                f.set_len(total_bytes)
                    .map_err(|e| Error::IoError { source: e })?;
            }
            f
        };

        // Wrap the raw `File` in our bounded-cache `WritebackFile`
        // (drains dirty pages continuously instead of bursting; see
        // `crate::io`). The `WritebackFile` moves into the consumer
        // thread.
        let file = crate::io::WritebackFile::new(file).map_err(|e| Error::IoError { source: e })?;
        let batch: u16 = match opts.batch_sectors {
            Some(b) => b,
            None if opts.skip_on_error => ecc_sectors(self.format),
            None => DEFAULT_BATCH_SECTORS_OPTICAL,
        };

        // Pre-compute the list of NonTried regions before handing the
        // mapfile to the consumer thread. Each region is processed by
        // the producer in order; the consumer mutates the mapfile per
        // work-item. Any regions left as NonTrimmed/Unreadable after
        // sweep finishes are the patch pass's job.
        let regions: Vec<(u64, u64)> = map.ranges_with(&[mapfile::SectorStatus::NonTried]);

        // Spawn the consumer. It owns WritebackFile + Mapfile; the producer
        // (this thread) keeps `reader`, `read_ctx`, halt + set_speed.
        // The thread name is preserved from the 0.17.x sweep_pipeline so it
        // stays identifiable in stack traces / `top -H`.
        let (sink, prog_rx) = SweepSink::new(file, map, is_regular);
        let pipe: Pipeline<WorkItem, sweep::ConsumerSummary> =
            Pipeline::spawn_named("freemkv-sweep-consumer", DEFAULT_PIPELINE_DEPTH, sink)?;

        // Translate `Pipeline::send` failure (consumer gone) into the
        // same `Error` shape the 0.17.x `send_or_abort` produced, so
        // the producer-error semantics are unchanged.
        fn consumer_gone() -> Error {
            Error::IoError {
                source: std::io::Error::other("sweep consumer terminated unexpectedly"),
            }
        }

        let mut buf = vec![0u8; batch as usize * 2048];
        let mut bytes_done = 0u64;
        let mut halt_requested = false;
        let copy_t0 = std::time::Instant::now();
        let mut iter_count: u64 = 0;
        let mut read_ok_count: u64 = 0;
        let mut read_err_count: u64 = 0;
        let mut last_log_iter: u64 = 0;
        let mut read_ctx = read_error::ReadCtx::for_sweep(batch);
        let mut in_damage_zone = false;
        const DAMAGE_ZONE_EXIT_THRESHOLD: u64 = 16;
        let mut cached_snapshot: Option<ProgressSnapshot> = None;
        let mut producer_err: Option<Error> = None;

        tracing::trace!(
            target: "freemkv::disc",
            phase = "copy_start",
            total_bytes,
            batch,
            skip_on_error = opts.skip_on_error,
            regions = regions.len(),
            "Disc::sweep entered (producer/consumer)"
        );

        'outer: for (region_pos, region_size) in regions {
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

                match read_result {
                    Ok(_) => {
                        read_ok_count += 1;
                        read_ctx.on_success();

                        if read_ctx.consecutive_good >= DAMAGE_ZONE_EXIT_THRESHOLD {
                            read_ctx.jump_multiplier = 1;
                            if in_damage_zone {
                                in_damage_zone = false;
                                reader.set_speed(0xFFFF);
                                tracing::debug!(
                                    target: "freemkv::disc",
                                    phase = "damage_exit",
                                    lba = block_lba,
                                    "Exited damage zone; restoring max read speed"
                                );
                            }
                        }
                        read_ctx.bridge_degradation_count = 0;

                        // Plaintext: the wrapped reader (DecryptingSectorSource)
                        // applied AACS / CSS in-place during read_sectors above.
                        // The consumer thread sees decrypted bytes; the
                        // pre-0.18 inline decrypt_sectors call lived here.

                        // Move the batch into the channel via fresh
                        // owned Vec. The producer's `buf` is reused
                        // for the next read.
                        let send_buf = buf[..block_bytes as usize].to_vec();
                        if pipe.send(WorkItem::Good { pos, buf: send_buf }).is_err() {
                            producer_err = Some(consumer_gone());
                            break 'outer;
                        }
                        bytes_done = bytes_done.saturating_add(block_bytes);
                        pos += block_bytes;
                    }
                    Err(err) if !opts.skip_on_error => {
                        let (status, sense) = extract_scsi_context(&err);
                        producer_err = Some(Error::DiscRead {
                            sector: block_lba as u64,
                            status: Some(status),
                            sense,
                        });
                        break 'outer;
                    }
                    Err(err) => {
                        read_err_count += 1;
                        let action = read_error::handle_read_error(&err, &mut read_ctx);

                        match action {
                            read_error::ReadAction::Retry { pause_secs } => {
                                sleep_secs_or_halt(pause_secs, opts.halt.as_ref());
                            }
                            read_error::ReadAction::Bisect => {
                                read_ctx.bisecting = true;
                                let saved_batch = read_ctx.batch;
                                read_ctx.batch = 1;
                                let mut bisect_aborted = false;
                                for sector_offset in 0..block_count {
                                    if let Some(ref h) = opts.halt {
                                        if h.load(std::sync::atomic::Ordering::Relaxed) {
                                            halt_requested = true;
                                            bisect_aborted = true;
                                            break;
                                        }
                                    }
                                    let sector_lba = block_lba + (sector_offset as u32);
                                    let mut sector_buf = [0u8; 2048];
                                    let write_pos = pos + (sector_offset as u64 * 2048);
                                    match reader.read_sectors(
                                        sector_lba,
                                        1,
                                        &mut sector_buf[..],
                                        true,
                                    ) {
                                        Ok(_) => {
                                            read_ctx.on_success();
                                            // Plaintext via the wrapping
                                            // DecryptingSectorSource — same
                                            // decrypt path the batch read takes.
                                            if pipe
                                                .send(WorkItem::BisectGood {
                                                    pos: write_pos,
                                                    buf: Box::new(sector_buf),
                                                })
                                                .is_err()
                                            {
                                                producer_err = Some(consumer_gone());
                                                bisect_aborted = true;
                                                break;
                                            }
                                        }
                                        Err(inner_err) => {
                                            let _ = read_error::handle_read_error(
                                                &inner_err,
                                                &mut read_ctx,
                                            );
                                            if pipe
                                                .send(WorkItem::BisectBad { pos: write_pos })
                                                .is_err()
                                            {
                                                producer_err = Some(consumer_gone());
                                                bisect_aborted = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                read_ctx.bisecting = false;
                                read_ctx.batch = saved_batch;
                                if bisect_aborted {
                                    break 'outer;
                                }
                                bytes_done = bytes_done.saturating_add(block_bytes);
                                pos += block_bytes;
                            }
                            read_error::ReadAction::SkipBlock { pause_secs } => {
                                if pipe
                                    .send(WorkItem::SkipFill {
                                        pos,
                                        len: block_bytes,
                                    })
                                    .is_err()
                                {
                                    producer_err = Some(consumer_gone());
                                    break 'outer;
                                }
                                bytes_done = bytes_done.saturating_add(block_bytes);
                                sleep_secs_or_halt(pause_secs, opts.halt.as_ref());
                                pos += block_bytes;
                            }
                            read_error::ReadAction::JumpAhead {
                                sectors,
                                pause_secs,
                            } => {
                                if pipe
                                    .send(WorkItem::SkipFill {
                                        pos,
                                        len: block_bytes,
                                    })
                                    .is_err()
                                {
                                    producer_err = Some(consumer_gone());
                                    break 'outer;
                                }
                                bytes_done = bytes_done.saturating_add(block_bytes);

                                if !in_damage_zone {
                                    in_damage_zone = true;
                                    reader.set_speed(0x0000);
                                    tracing::debug!(
                                        target: "freemkv::disc",
                                        phase = "damage_enter",
                                        lba = block_lba,
                                        "Entered damage zone; dropping to minimum read speed"
                                    );
                                }

                                let jump_pos = (pos + block_bytes + sectors * 2048).min(region_end);
                                let gap_start = pos + block_bytes;
                                let gap_bytes = jump_pos.saturating_sub(gap_start);
                                if gap_bytes > 0 {
                                    if pipe
                                        .send(WorkItem::GapFill {
                                            pos: gap_start,
                                            len: gap_bytes,
                                        })
                                        .is_err()
                                    {
                                        producer_err = Some(consumer_gone());
                                        break 'outer;
                                    }
                                    bytes_done = bytes_done.saturating_add(gap_bytes);
                                }
                                tracing::warn!(
                                    target: "freemkv::disc",
                                    phase = "damage_jump",
                                    from_lba = block_lba,
                                    to_lba = (jump_pos / 2048) as u32,
                                    jump_mb = gap_bytes / 1_048_576,
                                    "damage-jump"
                                );
                                pos = jump_pos;
                                sleep_secs_or_halt(pause_secs, opts.halt.as_ref());
                            }
                            read_error::ReadAction::AbortPass => {
                                let (status, sense) = extract_scsi_context(&err);
                                producer_err = Some(Error::DiscRead {
                                    sector: block_lba as u64,
                                    status: Some(status),
                                    sense,
                                });
                                break 'outer;
                            }
                        }
                    }
                }

                iter_count += 1;

                // Drain any consumer-side stats snapshot.
                if let Some(snap) = try_recv_progress(&prog_rx) {
                    cached_snapshot = Some(snap);
                }

                if iter_count - last_log_iter >= 100 {
                    last_log_iter = iter_count;
                    if let Some(ref snap) = cached_snapshot {
                        tracing::trace!(
                            target: "freemkv::disc",
                            phase = "iter_progress",
                            iter_count,
                            read_ok_count,
                            read_err_count,
                            pos,
                            region_end,
                            bytes_good = snap.stats.bytes_good,
                            bytes_pending = snap.stats.bytes_pending,
                            copy_elapsed_ms = copy_t0.elapsed().as_millis() as u64,
                            "Disc::sweep inner iter"
                        );
                    }
                    // Throttled stats refresh request — best-effort
                    // try_send so a busy consumer doesn't stall the
                    // producer; the cached snapshot stays current
                    // enough for one more iteration.
                    let _ = pipe.try_send(WorkItem::StatsRequest);
                }

                if let Some(reporter) = opts.progress {
                    // Use the latest consumer snapshot if we have
                    // one; otherwise synthesise a producer-side
                    // placeholder. On a fresh sweep, before the
                    // first stats round-trip lands, this means
                    // bytes_good ≈ bytes_done (producer's notion of
                    // good-so-far) and the bad-range list is empty —
                    // close enough for an early UI tick; the next
                    // real snapshot replaces it.
                    let main_title = self.titles.first();
                    let main_title_bad = match &cached_snapshot {
                        Some(snap) => self
                            .titles
                            .first()
                            .map(|t| bytes_bad_in_title(t, &snap.bad_ranges))
                            .unwrap_or(0),
                        None => 0,
                    };
                    // The consumer's snapshot is the source of truth for
                    // bytes_unreadable / bytes_pending (the producer doesn't
                    // see them), but its bytes_good lags producer-side
                    // `bytes_done` whenever the consumer is behind on draining
                    // the work channel. Take the max so the user-visible
                    // counter never regresses below what the producer has
                    // already sent — Anomaly B in the 0.18.1 prod test was
                    // this regression: a stale early snapshot pinned the
                    // display to 0 GB while bytes_done was already advancing.
                    let (bytes_good, bytes_unreadable, bytes_pending) = match &cached_snapshot {
                        Some(snap) => (
                            snap.stats.bytes_good.max(bytes_done),
                            snap.stats.bytes_unreadable,
                            snap.stats.bytes_pending,
                        ),
                        None => (bytes_done, 0u64, total_bytes.saturating_sub(bytes_done)),
                    };
                    let pp = crate::progress::PassProgress {
                        kind: crate::progress::PassKind::Sweep,
                        work_done: pos,
                        work_total: total_bytes,
                        bytes_good_total: bytes_good,
                        bytes_unreadable_total: bytes_unreadable,
                        bytes_pending_total: bytes_pending,
                        bytes_total_disc: total_bytes,
                        disc_duration_secs: main_title.map(|t| t.duration_secs),
                        bytes_bad_in_main_title: main_title_bad,
                        main_title_duration_secs: main_title.map(|t| t.duration_secs),
                        main_title_size_bytes: main_title.map(|t| t.size_bytes),
                    };
                    if !reporter.report(&pp) {
                        halt_requested = true;
                        break 'outer;
                    }
                }
            }
        }

        // Producer side is done. Drop the channel and let the
        // consumer drain whatever's still in flight, then run its
        // close() (drain writeback, fsync, mapfile.flush) and return
        // the final stats. On consumer panic `pipe.finish` returns
        // the wrapped panic message via Error::IoError — same shape
        // the previous `consumer_handle.join().map_err(...)` produced.
        let summary = pipe.finish();

        // Producer-side error wins over consumer-side (the read failure
        // is what motivated quitting; the consumer's flush error, if
        // any, is downstream).
        if let Some(e) = producer_err {
            // Drop the consumer's result if we already have a producer
            // error, but propagate consumer-panic on top of nothing
            // since that's strictly informative.
            let _ = summary;
            return Err(e);
        }
        let summary = summary?;

        let stats = summary.stats;
        tracing::debug!(
            target: "freemkv::disc",
            phase = "sweep_done",
            iter_count,
            read_ok_count,
            read_err_count,
            bytes_good = stats.bytes_good,
            bytes_pending = stats.bytes_pending,
            halted = halt_requested,
            copy_elapsed_ms = copy_t0.elapsed().as_millis() as u64,
            "Disc::sweep returning"
        );

        // End-of-pass diagnostic summary (added 2026-05-10 alongside
        // the per-error timing instrumentation in read_error.rs).
        // One INFO line per sweep that lets a post-mortem analyst tell
        // at a glance how much damage the disc + drive saw, without
        // grepping through the per-error WARN log. The PassSummary
        // counters come from `ReadCtx`'s accumulated state.
        let pass_sum = read_ctx.pass_summary();
        tracing::info!(
            target: "freemkv::disc",
            phase = "pass1_summary",
            total_reads_ok = pass_sum.total_reads_ok,
            total_errors = pass_sum.total_errors,
            zones_entered = pass_sum.zones_entered,
            jumps_taken = pass_sum.jumps_taken,
            bytes_good = stats.bytes_good,
            bytes_pending = stats.bytes_pending,
            copy_elapsed_ms = copy_t0.elapsed().as_millis() as u64,
            "Pass 1 complete"
        );
        Ok(CopyResult {
            bytes_total: total_bytes,
            bytes_good: stats.bytes_good,
            bytes_unreadable: stats.bytes_unreadable,
            bytes_pending: stats.bytes_pending,
            recovered_this_pass: 0,
            complete: stats.bytes_pending == 0 && !halt_requested,
            halted: halt_requested,
        })
    }
}

#[derive(Default)]
pub struct CopyOptions<'a> {
    pub decrypt: bool,
    pub multipass: bool,
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

#[derive(Debug, Clone, Copy)]
pub struct CopyResult {
    pub bytes_total: u64,
    pub bytes_good: u64,
    pub bytes_unreadable: u64,
    pub bytes_pending: u64,
    pub recovered_this_pass: u64,
    pub complete: bool,
    pub halted: bool,
}

/// Options for [`Disc::sweep`] (Pass 1 / forward sequential pass).
pub struct SweepOptions<'a> {
    pub decrypt: bool,
    pub resume: bool,
    pub batch_sectors: Option<u16>,
    pub skip_on_error: bool,
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

/// Options for [`Disc::patch`] (Pass N retry pass over bad ranges).
pub struct PatchOptions<'a> {
    pub decrypt: bool,
    pub block_sectors: Option<u16>,
    pub full_recovery: bool,
    pub reverse: bool,
    pub wedged_threshold: u64,
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

/// Result returned by [`Disc::patch`].
pub struct PatchOutcome {
    pub bytes_total: u64,
    pub bytes_good: u64,
    pub bytes_unreadable: u64,
    pub bytes_pending: u64,
    pub bytes_recovered_this_pass: u64,
    pub halted: bool,
    pub blocks_attempted: u64,
    pub blocks_read_ok: u64,
    pub blocks_read_failed: u64,
    pub wedged_exit: bool,
    pub wedged_threshold: u64,
}

/// Sleep `secs` seconds, but break early if `halt` flips to true.
/// Used by Pass 1's wedge-avoidance inter-error pause so halt
/// remains responsive regardless of how long the pause is.
/// Polling granularity 100 ms — bounded latency on halt regardless
/// of pause length.
pub(crate) fn sleep_secs_or_halt(
    secs: u64,
    halt: Option<&std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    if secs == 0 {
        return;
    }
    let Some(h) = halt else {
        std::thread::sleep(std::time::Duration::from_secs(secs));
        return;
    };
    let total = std::time::Duration::from_secs(secs);
    let slice = std::time::Duration::from_millis(100);
    let start = std::time::Instant::now();
    while start.elapsed() < total {
        if h.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let remaining = total.saturating_sub(start.elapsed());
        std::thread::sleep(remaining.min(slice));
    }
}

pub fn mapfile_path_for(iso_path: &std::path::Path) -> std::path::PathBuf {
    let mut s = iso_path.as_os_str().to_os_string();
    s.push(".mapfile");
    std::path::PathBuf::from(s)
}

impl Disc {
    /// Path to the mapfile for a given output path.
    ///
    /// For `/dev/null` output, returns `/tmp/{volume_id_or_title}.mapfile`.
    /// For regular files, returns `{path}.mapfile`.
    pub fn mapfile_for(&self, path: &std::path::Path) -> std::path::PathBuf {
        if path.as_os_str() == "/dev/null" {
            let name: String = self
                .meta_title
                .as_deref()
                .unwrap_or(&self.volume_id)
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            std::path::PathBuf::from(format!("/tmp/{name}.mapfile"))
        } else {
            mapfile_path_for(path)
        }
    }
}

impl Disc {
    /// Bytes of bad/unreadable data in a title's extents, from a mapfile.
    ///
    /// Consumers (CLI, autorip) call this after a rip pass to determine
    /// how much damage affects a particular title — useful for showing
    /// "42s lost (12s in main movie)" in the UI.
    pub fn bytes_bad_in_title(&self, mapfile_path: &std::path::Path, title: &DiscTitle) -> u64 {
        let map = match mapfile::Mapfile::load(mapfile_path) {
            Ok(m) => m,
            Err(_) => return 0,
        };
        let bad_ranges = map.ranges_with(&[
            mapfile::SectorStatus::NonTrimmed,
            mapfile::SectorStatus::Unreadable,
            mapfile::SectorStatus::NonScraped,
            mapfile::SectorStatus::NonTried,
        ]);
        bytes_bad_in_title(title, &bad_ranges)
    }

    /// Pass 2..N of a multipass rip: re-read the bad ranges
    /// recorded in the sidecar mapfile and try to recover them.
    /// With `reverse: true` (the default for the recovery walker),
    /// the bad-range walk runs end-to-start so escalating skips
    /// converge on the actual bad sub-zones inside any
    /// `NonTrimmed` block. Returns a [`PatchOutcome`] with
    /// recovered byte counts and wedge-detection signals.
    ///
    /// 0.18: paired with [`Disc::sweep`] as the library's other flat
    /// rip-phase verb. Caller drives the retry loop and the
    /// sweep-vs-patch dispatch.
    pub fn patch(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &PatchOptions,
    ) -> Result<PatchOutcome> {
        use crate::io::pipeline::{Pipeline, WRITE_THROUGH_DEPTH};
        use crate::sector::{DecryptingSectorSource, SectorSource};
        use patch::{PatchItem, PatchSink};

        const BRIDGE_DEGRADATION_PAUSE_SECS: u64 = 10;
        const POST_FAILURE_PAUSE_SECS: u64 = 1;
        const CONSECUTIVE_FAIL_LONG_PAUSE: u64 = 5;
        const CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD: u64 = 10;

        fn skip_sectors_for_probe(idx: usize) -> u64 {
            let base = PASSN_SKIP_SECTORS_BASE as i64;
            let escalation = (idx * 3) as i64;
            let shifted = if escalation < 64 {
                base << escalation
            } else {
                base
            };
            shifted.min(PASSN_SKIP_SECTORS_CAP as i64) as u64
        }

        let mapfile_path = self.mapfile_for(path);
        let map =
            mapfile::Mapfile::load(&mapfile_path).map_err(|e| Error::IoError { source: e })?;
        let total_bytes = map.total_size();
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        // Wrap the producer-side reader once so every read_sectors
        // call (the main recovery read, the backtrack read, and the
        // non-NOT_READY retry read) yields plaintext. Replaces three
        // inline decrypt_sectors call sites that all keyed off the
        // same `keys`. `DecryptKeys::None` keeps the unencrypted /
        // --raw path a pass-through.
        let mut reader = DecryptingSectorSource::new(reader, keys);
        let reader = &mut reader;

        let is_regular = std::fs::metadata(path)
            .map(|m| m.file_type().is_file())
            .unwrap_or(false);

        // Snapshot fields we need from the mapfile *before* it moves into
        // the consumer thread: bytes_good baseline, total entries, the
        // initial `bad_ranges` work list, and the start-of-patch
        // diagnostic dump. The shared state (`shared`) republishes these
        // throughout the pass; the consumer owns the live `Mapfile`.
        let bytes_good_before = map.stats().bytes_good;
        let bytes_good_start = bytes_good_before;
        let initial_stats = map.stats();
        let initial_entries: Vec<_> = map.entries().to_vec();
        // Every retry pass acts on every non-Finished range. Including
        // Unreadable means a sector that failed in pass N gets a fresh
        // shot in pass N+1 — drive state evolves, the same read can
        // succeed later. Each pass owns its own jumps/skips; if pass 5
        // jumps over the same zone as pass 2, fine.
        let mut bad_ranges = map.ranges_with(&[
            mapfile::SectorStatus::NonTrimmed,
            mapfile::SectorStatus::NonScraped,
            mapfile::SectorStatus::Unreadable,
        ]);
        if opts.reverse {
            bad_ranges.reverse();
        }
        let work_total: u64 = bad_ranges.iter().map(|(_, sz)| *sz).sum();

        // Spawn the consumer. The `WritebackFile` (same bounded-cache
        // wrapper sweep uses, so patch's recovery writes — sparse but
        // can be many across a damaged region — get the burst-flush
        // protection on slow / NFS-backed staging) and the `Mapfile`
        // both move into the sink. We hold an `Arc<Mutex<…>>` snapshot
        // the sink republishes after every record so producer-side
        // stall guards / progress callbacks can read consumer side-
        // effects.
        let (sink, shared) = PatchSink::new(path, map, is_regular)?;
        // Why: WRITE_THROUGH_DEPTH (=1) — patch reads ONE sector per
        // recovery decision and the producer's stall / damage-window
        // logic checks consumer-published stats inline. Sweep's
        // DEFAULT_PIPELINE_DEPTH (=4) would let several sectors of
        // recovered bytes queue up between producer decisions and
        // writes, which conflicts with the per-sector lockstep this
        // loop was written against.
        let pipe = Pipeline::<PatchItem, _>::spawn(WRITE_THROUGH_DEPTH, sink)?;

        // Send a `PatchItem` and translate a `SendError` (consumer
        // thread died / panicked) into a useful library error so the
        // caller can propagate cleanly. Mirrors `sweep_pipeline.rs`'s
        // `send_or_abort`.
        let send_or_abort = |pipe: &Pipeline<PatchItem, _>, item: PatchItem| -> Result<()> {
            pipe.send(item).map_err(|_| Error::IoError {
                source: std::io::Error::other("patch consumer terminated unexpectedly"),
            })
        };

        // Snapshot helper for producer-side stats reads. Holds the
        // mutex briefly; we never read across operations so a fresh
        // snapshot per call is fine.
        let read_shared = |shared: &std::sync::Mutex<patch::SharedPatchState>| -> (
            mapfile::MapStats,
            Vec<(u64, u64)>,
        ) {
            let g = shared.lock().expect("PatchSink shared state mutex poisoned");
            (g.stats, g.bad_ranges.clone())
        };

        // Log ISO file size at patch start for write monitoring
        if let Ok(metadata) = std::fs::metadata(path) {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_iso_size_start",
                iso_bytes = metadata.len(),
                "ISO file size at patch start"
            );
        }

        // Adaptive batching: read at `current_batch`, drop to 1 on
        // batch-read failure, climb back to `initial_batch` after
        // ADAPTIVE_UPSCALE_THRESHOLD consecutive single-sector successes.
        // Rationale: dense damage scattered through a NonTrimmed range
        // is rare — most "bad ranges" in pass N have lots of good
        // sectors that swept-by-default landed inside. Batch reads
        // walk those at ~32x the speed of singles, dropping to 1
        // only when the drive actually returns an error. Guarantees:
        //   - no good sector is ever marked NonTrimmed because it
        //     was bundled in a failed batch — failed batches are
        //     "split decisions", not recorded failures
        //   - drop-to-1 retries the SAME starting position, so every
        //     sector in the failed batch is individually probed
        let initial_batch = opts.block_sectors.unwrap_or(1);
        let mut current_batch: u16 = initial_batch;
        let mut consecutive_singles_ok: u32 = 0;
        const ADAPTIVE_UPSCALE_THRESHOLD: u32 = 16;
        let recovery = opts.full_recovery;

        let mut halted = false;
        let mut wedged_exit = false;
        let mut blocks_attempted: u64 = 0;
        let mut blocks_read_ok: u64 = 0;
        let mut blocks_read_failed: u64 = 0;
        // Reset to 0 at the start of every range; declared without init
        // because the per-range reset (below) always runs before any read.
        let mut consecutive_failures: u64;
        let mut unreadable_count: u64 = 0;
        let mut bytes_good_last = bytes_good_before;
        let mut stall_start = std::time::Instant::now();
        let mut range_start;
        let mut range_bytes_good;
        const STALL_SECS: u64 = 3600;
        // Per-range budget = sectors_in_range × SECONDS_PER_SECTOR, capped
        // at RANGE_BUDGET_CAP. Replaces the old flat 180 s/range — that
        // was unfair to medium ranges (a 51-sector range got the same
        // 180 s as a 1-sector range, so multi-sector ranges couldn't
        // even attempt every sector inside their budget) and pointlessly
        // generous to single-sector ranges (180 s when ~5 s would do).
        // The cap keeps catastrophic ranges (10s of MB) bounded so they
        // can't consume the entire patch run; multi-pass orchestration
        // raises the cap on later passes for the genuinely-stuck ones.
        // Empirical per-failed-sector cost on direct-SATA BU40N (2026-05-08):
        // ~3 s SCSI READ failure + ~15 s sr0 pread fallback (kernel sr_mod
        // does ~5 internal retries) ≈ 18-25 s total. SECONDS_PER_SECTOR=25
        // lets a small range fully sample within budget instead of bailing
        // after one slow read. Previous value of 5 was too tight: a
        // 3-sector range got 15 s budget but the first failed read alone
        // took ~20 s, so the watchdog fired before sector 2 could be tried.
        const SECONDS_PER_SECTOR: u64 = 25;
        const RANGE_BUDGET_CAP_SECS: u64 = 1800;
        const MAX_SKIPS_PER_RANGE: u32 = 10;
        let mut skip_count: u32;
        let mut buf = vec![0u8; initial_batch as usize * 2048];

        // Pass 2 uses smaller sectors (1 vs 32) but same damage detection logic
        const PASSN_DAMAGE_WINDOW: usize = 16;
        // Reduced from 12% to 6% for BU40N encrypted UHD discs.
        // Lower threshold means patch tries harder before skipping ahead,
        // giving more sectors a chance to be recovered on marginal media.
        const PASSN_DAMAGE_THRESHOLD_PCT: usize = 6;
        // Reduced base from 64 to 32 sectors (64 KB) for BU40N encrypted UHD.
        // Smaller initial skips give patch more chances to recover marginal data
        // before jumping far ahead in the range. Escalation still works up to cap.
        const PASSN_SKIP_SECTORS_BASE: u64 = 32;
        const PASSN_SKIP_SECTORS_CAP: u64 = 4096;
        const PASSN_ESCALATION_RESET_GOOD: u32 = 4;
        let mut damage_window: Vec<bool> = Vec::with_capacity(PASSN_DAMAGE_WINDOW);
        let mut consecutive_skips_without_recovery: u32;
        let mut consecutive_good_since_skip: u32;
        let mut last_skip_from: Option<u64> = None;

        reader.set_speed(0x0000);

        // Log ALL mapfile entries for diagnostic purposes
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_mapfile_snapshot",
            total_entries = initial_entries.len(),
            bytes_good_before,
            bytes_retryable = initial_stats.bytes_retryable,
            bytes_unreadable = initial_stats.bytes_unreadable,
            bytes_nontried = initial_stats.bytes_nontried,
            "Mapfile state snapshot at patch start"
        );

        // Log first 10 and last 10 entries for inspection
        if !initial_entries.is_empty() {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_mapfile_entries_start",
                num_to_log = (initial_entries.len().min(10)) as u32,
                "First 10 entries"
            );
            for entry in initial_entries.iter().take(10) {
                tracing::debug!(
                    target: "freemkv::disc",
                    phase = "patch_mapfile_entry_start",
                    pos_hex = format!("0x{:09x}", entry.pos),
                    size_mb = entry.size as f64 / 1_048_576.0,
                    status_char = entry.status.to_char() as u8 as i32,
                    "Mapfile entry"
                );
            }
        }
        if initial_entries.len() > 10 {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_mapfile_entries_end",
                num_to_log = (initial_entries.len().min(10)) as u32,
                "Last 10 entries"
            );
            for entry in initial_entries.iter().skip(initial_entries.len() - 10) {
                tracing::debug!(
                    target: "freemkv::disc",
                    phase = "patch_mapfile_entry_end",
                    pos_hex = format!("0x{:09x}", entry.pos),
                    size_mb = entry.size as f64 / 1_048_576.0,
                    status_char = format!("{}", entry.status.to_char()),
                    "Mapfile entry"
                );
            }
        }

        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_bad_ranges",
            num_ranges = bad_ranges.len(),
            work_total,
            reverse_mode = opts.reverse,
            "Bad ranges for patch"
        );
        let mut work_done: u64 = 0;
        tracing::info!(
            target: "freemkv::disc",
            phase = "patch_start",
            block_sectors = initial_batch,
            recovery,
            reverse = opts.reverse,
            wedged_threshold = opts.wedged_threshold,
            num_ranges = bad_ranges.len(),
            work_total,
            bytes_good_start,
            "Disc::patch entered"
        );

        'outer: for (range_idx, (range_pos, range_size)) in bad_ranges.iter().enumerate() {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_range_start",
                range_index = range_idx,
                num_total_ranges = bad_ranges.len(),
                range_lba = *range_pos / 2048,
                range_size_mb = *range_size as f64 / 1_048_576.0,
                "Starting patch range"
            );
            let end = *range_pos + *range_size;
            let mut block_end = if opts.reverse { end } else { *range_pos };
            damage_window.clear();
            consecutive_skips_without_recovery = 0;
            consecutive_good_since_skip = 0;
            range_start = std::time::Instant::now();
            range_bytes_good = bytes_good_before;
            skip_count = 0;
            // Reset consecutive_failures at each range boundary. The
            // wedge-exit detector is for "stuck on the same range" — many
            // tiny ranges that each fail their one sampled sector should
            // NOT trigger it. Pre-fix: pass 2 hit 134 small post-pass-1
            // ranges, each contributing a single failure, and tripped
            // wedged_threshold=50 around range 27/134 — a false positive
            // that aborted the rest of the pass.
            consecutive_failures = 0;
            let range_sectors = *range_size / 2048;
            let range_budget_secs = (range_sectors * SECONDS_PER_SECTOR).min(RANGE_BUDGET_CAP_SECS);
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch_range_budget",
                range_lba = *range_pos / 2048,
                range_sectors,
                range_budget_secs,
                "Per-range time budget computed"
            );
            loop {
                if let Some(ref h) = opts.halt {
                    if h.load(std::sync::atomic::Ordering::Relaxed) {
                        halted = true;
                        break 'outer;
                    }
                }

                // Per-range watchdog: budget = range_sectors × 5 s, capped
                // at RANGE_BUDGET_CAP_SECS. Tiny ranges exit fast (1-sector
                // range = 5 s budget); medium ranges get proportional time
                // (51-sector range = 255 s); huge ranges still bounded by
                // the cap so they can't monopolise pass 1.
                //
                // Both the absolute-elapsed and no-progress checks share
                // the same per-range budget. The progress check resets
                // range_start on every byte gained, so a steadily-recovering
                // range can run as long as it makes progress.
                if range_start.elapsed().as_secs() > range_budget_secs {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "patch_range_timeout",
                        range_lba = range_pos / 2048,
                        range_sectors,
                        elapsed_secs = range_start.elapsed().as_secs(),
                        budget_secs = range_budget_secs,
                        bytes_recovered = range_bytes_good.saturating_sub(bytes_good_before),
                        "Range timeout - moving to next range"
                    );
                    break;
                }

                let bytes_good_now = read_shared(&shared).0.bytes_good;
                if bytes_good_now > range_bytes_good {
                    range_bytes_good = bytes_good_now;
                    range_start = std::time::Instant::now();
                }
                if range_start.elapsed().as_secs() > range_budget_secs {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "patch_range_stall",
                        range_lba = range_pos / 2048,
                        range_sectors,
                        elapsed_secs = range_start.elapsed().as_secs(),
                        budget_secs = range_budget_secs,
                        bytes_recovered = range_bytes_good.saturating_sub(bytes_good_before),
                        "Range stalled - moving to next range"
                    );
                    break;
                }

                // Test 3: Skip count - max 10 skips per range
                if skip_count >= MAX_SKIPS_PER_RANGE {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "patch_skip_limit",
                        range_lba = range_pos / 2048,
                        skip_count,
                        "Skip limit reached - leaving remaining bytes NonTrimmed for next pass",
                    );
                    // CRITICAL: don't mark sectors we NEVER ATTEMPTED as
                    // Unreadable. Only sectors we actually read+failed get
                    // the terminal `-` status. Sectors we jumped over are
                    // hopeful — the drive may read them on a later pass
                    // when state has evolved (cache, mechanical settle).
                    // 2026-05-07 dd-as-oracle test confirmed ~36% of
                    // patch-marked Unreadable sectors are actually readable.
                    let unmarked_bytes = block_end.saturating_sub(*range_pos);
                    if opts.reverse {
                        send_or_abort(
                            &pipe,
                            PatchItem::NonTrimmed {
                                pos: *range_pos,
                                len: unmarked_bytes,
                            },
                        )?;
                    } else {
                        let remaining_start = *range_pos + (end - block_end);
                        if remaining_start < end {
                            send_or_abort(
                                &pipe,
                                PatchItem::NonTrimmed {
                                    pos: remaining_start,
                                    len: end - remaining_start,
                                },
                            )?;
                        }
                    }
                    // Continue to next range (break inner loop only)
                    break;
                }
                let (pos, block_bytes) = if opts.reverse {
                    if block_end <= *range_pos {
                        break;
                    }
                    let span = (block_end - *range_pos).min(current_batch as u64 * 2048);
                    (block_end - span, span)
                } else {
                    if block_end >= end {
                        break;
                    }
                    let span = (end - block_end).min(current_batch as u64 * 2048);
                    (block_end, span)
                };
                let lba = (pos / 2048) as u32;
                let count = (block_bytes / 2048) as u16;
                let bytes = count as usize * 2048;
                blocks_attempted += 1;

                tracing::debug!(
                    target: "freemkv::disc",
                    phase = "patch_read_start",
                    lba,
                    count,
                    bytes,
                    attempt_num = blocks_attempted,
                    range_index = range_idx,
                    pos_byte = pos,
                    "Starting sector read"
                );

                // Cache priming: before reading the target sector, do
                // a few single-sector reads at LBAs immediately preceding
                // it. The drive's read-ahead cache prefetches forward on
                // sequential reads — so by the time we ask for `lba` it
                // may already be cached, even if a cold read fails. Proven
                // 2026-05-07 with dd-as-oracle: 8/8 sectors recoverable
                // when primed vs 6/8 cold. Throwaway reads — we already
                // have those bytes Finished from a prior pass; failures
                // here don't update mapfile state.
                const CACHE_PRIME_SECTORS: u32 = 3;
                if lba >= CACHE_PRIME_SECTORS && count == 1 {
                    let mut prime_buf = [0u8; 2048];
                    for i in 0..CACHE_PRIME_SECTORS {
                        let prime_lba = lba - CACHE_PRIME_SECTORS + i;
                        // Best-effort; ignore errors. Recovery=false is
                        // intentional: a fast 1.5s timeout is fine because
                        // we don't need the data.
                        let _ = reader.read_sectors(prime_lba, 1, &mut prime_buf[..], false);
                    }
                }

                // Single-shot read. Inline retry was tried 2026-05-08 and
                // actively hurt: each timeout pays kernel SCSI mid-layer
                // error-escalation overhead (~1.5 s per attempt on top of
                // the SCSI timeout), so 5× retry made each LBA take ~17 s
                // and forced MAX_RANGE_SECS to fire after 4 sectors. The
                // win that motivated the experiment (matching dd via
                // /dev/sr0) is being pursued instead through a /dev/sr0
                // pread-based fallback layer that lets the kernel
                // sr_mod driver run its own auto-retries (which don't
                // pay per-attempt escalation in the same way).
                let read_start = std::time::Instant::now();
                let read_result = reader.read_sectors(lba, count, &mut buf[..bytes], recovery);
                let read_duration_ms = read_start.elapsed().as_millis();

                match read_result {
                    Ok(_) => {
                        blocks_read_ok += 1;
                        consecutive_failures = 0;
                        consecutive_good_since_skip += 1;
                        if consecutive_good_since_skip >= PASSN_ESCALATION_RESET_GOOD {
                            consecutive_skips_without_recovery = 0;
                        }
                        // Adaptive batching: track clean single-sector reads to
                        // decide when to climb back to `initial_batch`. A batch
                        // read succeeding (count > 1) tells us the drive is healthy
                        // but doesn't accumulate toward upscale — we got back to
                        // batch=1 because of a failure here, we need consistent
                        // health at the slow tempo before scaling up again.
                        if count == 1 && current_batch < initial_batch {
                            consecutive_singles_ok += 1;
                            if consecutive_singles_ok >= ADAPTIVE_UPSCALE_THRESHOLD {
                                tracing::info!(
                                    target: "freemkv::disc",
                                    phase = "patch_adaptive_upscale",
                                    from = current_batch,
                                    to = initial_batch,
                                    consecutive_singles_ok,
                                    lba,
                                    "adaptive batching: drive stable, climbing back to initial_batch"
                                );
                                current_batch = initial_batch;
                                consecutive_singles_ok = 0;
                            }
                        }
                        damage_window.push(true);
                        if damage_window.len() > PASSN_DAMAGE_WINDOW {
                            damage_window.remove(0);

                            tracing::info!(
                                target: "freemkv::disc",
                                phase = "patch_read_ok",
                                lba,
                                count,
                                bytes,
                                blocks_read_ok,
                                consecutive_failures,
                                read_duration_ms,
                                range_idx,
                                pos,
                                "Read succeeded"
                            );
                        }
                        // Plaintext: DecryptingSectorSource applied AACS / CSS
                        // in-place during the read_sectors call above. The
                        // pre-0.18 inline decrypt_sectors call lived here.
                        let write_start = std::time::Instant::now();
                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "patch_write_start",
                            pos,
                            bytes,
                            "Starting ISO write"
                        );
                        // Hand the recovered bytes off to the consumer:
                        // seek + write + mapfile.record(Finished) all
                        // happen on the consumer thread, so the producer
                        // can immediately move on to the next read while
                        // these bytes are being committed.
                        send_or_abort(
                            &pipe,
                            PatchItem::Recovered {
                                pos,
                                buf: buf[..bytes].to_vec(),
                            },
                        )?;
                        let write_duration_ms = write_start.elapsed().as_millis();
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_write_ok",
                            pos,
                            bytes,
                            write_duration_ms,
                            "ISO write succeeded"
                        );
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_mapfile_record_ok",
                            pos,
                            block_bytes,
                            "Mapfile record dispatched"
                        );

                        // Stall guard: watch bytes_good (real progress),
                        // not pos (advances on skips). With the consumer
                        // running in its own thread, this read can lag
                        // by up to one item; the watchdog operates at
                        // STALL_SECS=3600 granularity so single-item lag
                        // is irrelevant.
                        let bytes_good_now = read_shared(&shared).0.bytes_good;
                        if bytes_good_now > bytes_good_last {
                            stall_start = std::time::Instant::now();
                            bytes_good_last = bytes_good_now;
                        }
                        if stall_start.elapsed() > std::time::Duration::from_secs(STALL_SECS) {
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "patch_stall",
                                elapsed_secs = stall_start.elapsed().as_secs(),
                                bytes_good = bytes_good_now,
                                bytes_good_start,
                                "Patch stalled - no recovery for {}s, exiting pass",
                                STALL_SECS
                            );
                            wedged_exit = true;
                            break 'outer;
                        }

                        if let Some(skip_from) = last_skip_from.take() {
                            let backtrack_start = block_end;
                            let backtrack_end = skip_from;
                            if opts.reverse && backtrack_start < backtrack_end {
                                tracing::info!(
                                    target: "freemkv::disc",
                                    phase = "patch_backtrack_start",
                                    from_lba = pos,
                                    to_lba = backtrack_end / 2048,
                                    "recovered after skip; backtracking into gap"
                                );
                                let mut bt_pos = backtrack_start;
                                while bt_pos < backtrack_end {
                                    let span =
                                        // Backtrack always at count=1: this path
                                        // fills a gap that the main loop's damage-
                                        // window skip jumped over. Using batched
                                        // reads here would lump good sectors into
                                        // NonTrimmed marks when the gap contains
                                        // even one bad sector. Backtrack is rare
                                        // enough that the per-sector cost is fine.
                                        (backtrack_end - bt_pos).min(2048);
                                    let bt_lba = (bt_pos / 2048) as u32;
                                    let bt_count = (span / 2048) as u16;
                                    let bt_bytes = bt_count as usize * 2048;
                                    match reader.read_sectors(
                                        bt_lba,
                                        bt_count,
                                        &mut buf[..bt_bytes],
                                        recovery,
                                    ) {
                                        Ok(_) => {
                                            blocks_read_ok += 1;
                                            // Plaintext via DecryptingSectorSource
                                            // wrapping; same path the main read
                                            // takes above.
                                            send_or_abort(
                                                &pipe,
                                                PatchItem::Recovered {
                                                    pos: bt_pos,
                                                    buf: buf[..bt_bytes].to_vec(),
                                                },
                                            )?;
                                        }
                                        Err(_err) => {
                                            blocks_read_failed += 1;
                                            // Leave NonTrimmed (not Unreadable) so a later
                                            // pass gets another shot. Per the project goal
                                            // — "recover 100% of readable data" — and the
                                            // multi-pass design's promise: bytes stay
                                            // Good-or-Maybe across passes; promotion to
                                            // Unreadable is the orchestrator's job at
                                            // end-of-recovery (final retry pass complete).
                                            // Reference: 2026-05-11 design call.
                                            send_or_abort(
                                                &pipe,
                                                PatchItem::NonTrimmed {
                                                    pos: bt_pos,
                                                    len: span,
                                                },
                                            )?;
                                            tracing::info!(
                                                target: "freemkv::disc",
                                                phase = "patch_backtrack_stop",
                                                lba = bt_lba,
                                                "backtrack hit damage; stopping"
                                            );
                                            break;
                                        }
                                    }
                                    work_done = work_done.saturating_add(span);
                                    bt_pos += span;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        // Adaptive batching split decision: a batch-read
                        // failure (count > 1) is NOT a recorded failure.
                        // We don't yet know which sector in the batch was
                        // actually bad — could be one, could be many.
                        // Drop to count=1 and retry the SAME starting
                        // position so every sector gets individually
                        // probed. Cursor stays put; loop continues.
                        // Invariants: no good sector ever gets lumped
                        // into a NonTrimmed mark, no spurious
                        // consecutive_failures (which drives wedge
                        // detection), no damage_window pollution from
                        // batch-level signals.
                        if count > 1 {
                            tracing::info!(
                                target: "freemkv::disc",
                                phase = "patch_adaptive_split",
                                lba,
                                count,
                                from_batch = current_batch,
                                err_code = err.code(),
                                "adaptive batching: batch read failed, dropping to count=1 to probe individually"
                            );
                            current_batch = 1;
                            consecutive_singles_ok = 0;
                            continue;
                        }

                        blocks_read_failed += 1;
                        consecutive_failures += 1;
                        consecutive_good_since_skip = 0;
                        consecutive_singles_ok = 0;
                        unreadable_count += 1;

                        tracing::warn!(
                            target: "freemkv::disc",
                            phase = "patch_read_err",
                            lba,
                            count,
                            bytes,
                            blocks_read_failed,
                            consecutive_failures,
                            read_duration_ms,
                            error_code = err.code(),
                            range_idx,
                            pos,
                            "Read failed"
                        );

                        // Check if this is a NOT_READY error that should be retried
                        let sense = err.scsi_sense();

                        // ASC values indicating temporary drive unresponsiveness:
                        // 0x02 = medium not present, 0x03 = becoming ready, 0x04 = initialization required
                        let is_not_ready_retryable = sense
                            .map(|s| {
                                s.sense_key == 0x02
                                    && (s.asc == 0x02 || s.asc == 0x03 || s.asc == 0x04)
                            })
                            .unwrap_or(false);

                        // For retryable NOT_READY errors, pause longer and don't mark as Unreadable yet
                        if is_not_ready_retryable {
                            tracing::info!(
                                target: "freemkv::disc",
                                phase = "patch_not_ready_retry",
                                lba,
                                consecutive_failures,
                                err_asc = sense.map(|s| s.asc as u32).unwrap_or(0),
                                "NOT_READY with ASC=0x03/0x04; pausing for drive recovery before retry"
                            );

                            // Extended pause for NOT_READY - let drive complete internal mechanical recovery
                            let pause_secs = 15u64;
                            tracing::debug!(
                                target: "freemkv::disc",
                                phase = "patch_not_ready_pause",
                                lba,
                                consecutive_failures,
                                pause_secs,
                                "Waiting for drive to become ready"
                            );
                            std::thread::sleep(std::time::Duration::from_secs(pause_secs));

                            // Don't mark as Unreadable yet - will retry on next iteration
                            damage_window.push(false);
                            if damage_window.len() > PASSN_DAMAGE_WINDOW {
                                damage_window.remove(0);
                            }
                            continue;
                        }

                        // For non-NOT_READY errors (MEDIUM ERROR, ABORTED COMMAND, etc.),
                        // try additional retries before marking Unreadable. This is especially
                        // important for encrypted UHD discs where decryption failures can
                        // manifest as read errors that succeed on retry.
                        let mut retry_count = 0;
                        const MAX_NON_NOT_READY_RETRIES: u64 = 3;
                        let should_retry = opts.decrypt && retry_count < MAX_NON_NOT_READY_RETRIES;

                        if should_retry {
                            tracing::info!(
                                target: "freemkv::disc",
                                phase = "patch_non_not_ready_retry",
                                lba,
                                err_code = err.code(),
                                retry = retry_count + 1,
                                max_retries = MAX_NON_NOT_READY_RETRIES,
                                "Non-NOT_READY error on encrypted disc; retrying"
                            );

                            // Exponential backoff: 2s, 4s, 8s before final Unreadable mark
                            let pause_secs = (1u64 << retry_count).min(8);
                            std::thread::sleep(std::time::Duration::from_secs(pause_secs));
                            retry_count += 1;

                            // Retry the read
                            match reader.read_sectors(lba, count, &mut buf[..bytes], recovery) {
                                Ok(_) => {
                                    blocks_read_ok += 1;
                                    consecutive_failures = 0;
                                    consecutive_good_since_skip += 1;
                                    if consecutive_good_since_skip >= PASSN_ESCALATION_RESET_GOOD {
                                        consecutive_skips_without_recovery = 0;
                                    }
                                    damage_window.push(true);
                                    if damage_window.len() > PASSN_DAMAGE_WINDOW {
                                        damage_window.remove(0);
                                    }

                                    tracing::info!(
                                        target: "freemkv::disc",
                                        phase = "patch_retry_success",
                                        lba,
                                        retry_count,
                                        "Retry succeeded after non-NOT_READY error"
                                    );

                                    // Plaintext via DecryptingSectorSource;
                                    // same path the original read takes.
                                    let write_start = std::time::Instant::now();
                                    tracing::debug!(
                                        target: "freemkv::disc",
                                        phase = "patch_write_start",
                                        pos,
                                        bytes,
                                        "Starting ISO write"
                                    );
                                    send_or_abort(
                                        &pipe,
                                        PatchItem::Recovered {
                                            pos,
                                            buf: buf[..bytes].to_vec(),
                                        },
                                    )?;
                                    let write_duration_ms = write_start.elapsed().as_millis();
                                    tracing::info!(
                                        target: "freemkv::disc",
                                        phase = "patch_write_ok",
                                        pos,
                                        bytes,
                                        write_duration_ms,
                                        "ISO write succeeded"
                                    );
                                    tracing::info!(
                                        target: "freemkv::disc",
                                        phase = "patch_mapfile_record_ok",
                                        pos,
                                        block_bytes,
                                        "Mapfile record dispatched"
                                    );

                                    // Stall guard after successful retry
                                    let bytes_good_now = read_shared(&shared).0.bytes_good;
                                    if bytes_good_now > bytes_good_last {
                                        stall_start = std::time::Instant::now();
                                        bytes_good_last = bytes_good_now;
                                    }
                                    continue;
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        target: "freemkv::disc",
                                        phase = "patch_retry_failed",
                                        lba,
                                        retry_count,
                                        "Retry failed after non-NOT_READY error"
                                    );
                                }
                            }
                        }

                        // All retries exhausted IN THIS PASS — leave NonTrimmed
                        // so a subsequent pass gets another shot. Bytes stay
                        // Good-or-Maybe across passes; only the orchestrator
                        // (autorip) promotes still-NonTrimmed → Unreadable
                        // after the FINAL retry pass completes. Reference:
                        // 2026-05-11 design call ("good or maybe until all
                        // passes are done, then it's gone"). Pre-fix the
                        // patch loop marked Unreadable here, which gave up
                        // on sectors that a later pass might have recovered
                        // (drive reads are stochastic — same sector that
                        // fails 10x in Pass 2 might succeed on attempt 1 in
                        // Pass 3 after the drive state has shifted).
                        send_or_abort(
                            &pipe,
                            PatchItem::NonTrimmed {
                                pos,
                                len: block_bytes,
                            },
                        )?;

                        damage_window.push(false);
                        if damage_window.len() > PASSN_DAMAGE_WINDOW {
                            damage_window.remove(0);
                        }

                        // Stall guard: check on failures too, not just successes
                        let bytes_good_now = read_shared(&shared).0.bytes_good;
                        if bytes_good_now > bytes_good_last {
                            stall_start = std::time::Instant::now();
                            bytes_good_last = bytes_good_now;
                        }
                        if stall_start.elapsed() > std::time::Duration::from_secs(STALL_SECS) {
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "patch_stall",
                                elapsed_secs = stall_start.elapsed().as_secs(),
                                consecutive_failures,
                                bytes_good = bytes_good_now,
                                bytes_good_start,
                                "Patch stalled - no recovery for {}s, exiting pass",
                                STALL_SECS
                            );
                            wedged_exit = true;
                            break 'outer;
                        }

                        // Log every 10 failures or when approaching wedged threshold
                        if consecutive_failures % 10 == 0
                            || consecutive_failures >= opts.wedged_threshold
                        {
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "patch_failure_count",
                                lba,
                                consecutive_failures,
                                wedged_threshold = opts.wedged_threshold,
                                "Failure count"
                            );
                        }

                        // Probe good sectors to differentiate wedge vs bad sector
                        if consecutive_failures >= 3 && consecutive_failures % 5 == 0 {
                            let probe_offsets: [u64; 3] =
                                [0, skip_sectors_for_probe(1), skip_sectors_for_probe(2)];
                            let mut probes_ok = 0;

                            for (probe_idx, &offset) in probe_offsets.iter().enumerate() {
                                if offset >= block_bytes
                                    || (offset == 0 && consecutive_failures < 5)
                                {
                                    continue;
                                }

                                let probe_pos = pos + offset;
                                let probe_lba = (probe_pos / 2048) as u32;
                                let probe_count = 1u16;
                                let mut probe_buf = [0u8; 2048];

                                match reader.read_sectors(
                                    probe_lba,
                                    probe_count,
                                    &mut probe_buf[..],
                                    recovery,
                                ) {
                                    Ok(_) => {
                                        probes_ok += 1;
                                        tracing::debug!(
                                            target: "freemkv::disc",
                                            phase = "patch_probe_ok",
                                            lba = probe_lba,
                                            offset_from_current = offset,
                                            probe_idx,
                                            "Probe read succeeded — drive responsive"
                                        );
                                    }
                                    Err(_) => {
                                        tracing::debug!(
                                            target: "freemkv::disc",
                                            phase = "patch_probe_err",
                                            lba = probe_lba,
                                            offset_from_current = offset,
                                            probe_idx,
                                            "Probe read failed"
                                        );
                                    }
                                }
                            }

                            if probes_ok > 0 {
                                tracing::info!(
                                    target: "freemkv::disc",
                                    phase = "patch_drive_responsive",
                                    consecutive_failures,
                                    probes_ok,
                                    total_probes = 3,
                                    lba,
                                    range_idx,
                                    "Drive responsive — bad sector cluster, not wedged"
                                );
                            } else if probes_ok == 0 && consecutive_failures >= 10 {
                                // Heuristic suspicion of wedge — NOT the
                                // confirmed wedge_transition log that fires
                                // when the SCSI sense family flips into
                                // Hardware/IllegalRequest. This log just
                                // says "the local zone is fully bad" which
                                // could mean a real wedge OR a fully-bad
                                // cluster on a non-wedged drive. The
                                // wedge_skip handler in read_error.rs is
                                // what actually decides + acts.
                                tracing::warn!(
                                    target: "freemkv::disc",
                                    phase = "patch_zone_fully_bad",
                                    consecutive_failures,
                                    lba,
                                    range_idx,
                                    "patch zone fully bad (10+ failures, all probes failed); \
                                     not a wedge unless read_error.rs's wedge_transition also fires"
                                );
                            }
                        }

                        // Pair with the earlier NonTrimmed dispatch — same
                        // bytes, same state. Pre-2026-05-11 this was a
                        // second Unreadable mark; now it's NonTrimmed for
                        // the same reason: cross-pass retry survival.
                        send_or_abort(
                            &pipe,
                            PatchItem::NonTrimmed {
                                pos,
                                len: block_bytes,
                            },
                        )?;
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_mapfile_record_nontrimmed",
                            pos,
                            block_bytes,
                            consecutive_failures,
                            "Mapfile record dispatched as NonTrimmed (retry next pass)"
                        );

                        let pause_secs = if err.is_bridge_degradation() {
                            tracing::debug!(
                                target: "freemkv::disc",
                                phase = "patch_bridge_degradation",
                                lba,
                                consecutive_failures,
                                error = %err,
                                "bridge degradation; cooling down"
                            );
                            BRIDGE_DEGRADATION_PAUSE_SECS
                        } else if consecutive_failures >= CONSECUTIVE_FAIL_LONG_PAUSE_THRESHOLD {
                            CONSECUTIVE_FAIL_LONG_PAUSE
                        } else {
                            POST_FAILURE_PAUSE_SECS
                        };

                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "patch_post_failure_pause",
                            lba,
                            consecutive_failures,
                            pause_secs,
                            "breathing room after failure"
                        );
                        std::thread::sleep(std::time::Duration::from_secs(pause_secs));
                    }
                }

                let bad_count = damage_window.iter().filter(|&&b| !b).count();
                let mut did_skip = false;
                if damage_window.len() >= PASSN_DAMAGE_WINDOW
                    && bad_count * 100 / damage_window.len() >= PASSN_DAMAGE_THRESHOLD_PCT
                {
                    // Size-aware cap: never skip more than 1/4 of the
                    // remaining bad range. A 100-sector bad range is
                    // really 25-bad + 50-good + 25-bad in disguise; a
                    // hardcoded MB-scale skip would leap over the
                    // entire thing and miss the good middle. Capping
                    // at range_remaining/4 forces convergence on the
                    // actual bad sub-zones.
                    let range_remaining_bytes = if opts.reverse {
                        block_end.saturating_sub(*range_pos)
                    } else {
                        end.saturating_sub(block_end)
                    };
                    let range_remaining_sectors = range_remaining_bytes / 2048;
                    let range_quarter = (range_remaining_sectors / 4).max(1);
                    let escalated = (PASSN_SKIP_SECTORS_BASE << consecutive_skips_without_recovery)
                        .min(PASSN_SKIP_SECTORS_CAP);
                    let skip_sectors = escalated.min(range_quarter);
                    let skip_bytes = skip_sectors * 2048;
                    let new_block_end = if opts.reverse {
                        block_end.saturating_sub(skip_bytes).max(*range_pos)
                    } else {
                        (block_end + skip_bytes).min(end)
                    };
                    if new_block_end != block_end {
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_damage_skip",
                            from_lba = lba,
                            skip_sectors,
                            escalation = consecutive_skips_without_recovery,
                            bad_pct = bad_count * 100 / damage_window.len(),
                            "damage cluster detected; skipping within range"
                        );
                        let gap_bytes = if opts.reverse {
                            block_end.saturating_sub(new_block_end)
                        } else {
                            new_block_end.saturating_sub(block_end)
                        };
                        work_done = work_done.saturating_add(gap_bytes);
                        last_skip_from = Some(block_end);
                        block_end = new_block_end;
                        consecutive_skips_without_recovery += 1;
                        skip_count += 1;
                        did_skip = true;
                    }
                }

                if !did_skip {
                    if opts.reverse {
                        block_end = block_end.saturating_sub(block_bytes);
                    } else {
                        block_end += block_bytes;
                    }
                }

                if opts.wedged_threshold > 0 && consecutive_failures >= opts.wedged_threshold {
                    // Only exit wedged after attempting multiple ranges with zero recovery.
                    // Single-range terminal failures should not abort the entire pass.
                    let multi_range_attempted = range_idx > 0;
                    if multi_range_attempted {
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_wedged_exit",
                            consecutive_failures,
                            blocks_read_failed,
                            blocks_read_ok,
                            range_index = range_idx,
                            total_ranges = bad_ranges.len(),
                            "Disc::patch giving up — drive appears wedged after multiple ranges"
                        );
                        wedged_exit = true;
                        break 'outer;
                    }
                }

                work_done = work_done.saturating_add(block_bytes);

                if let Some(reporter) = opts.progress {
                    let (s, bad_ranges_now) = read_shared(&shared);
                    let kind = if initial_batch == 1 {
                        crate::progress::PassKind::Scrape {
                            reverse: opts.reverse,
                        }
                    } else {
                        crate::progress::PassKind::Trim {
                            reverse: opts.reverse,
                        }
                    };
                    let main_title_bad = self
                        .titles
                        .first()
                        .map(|t| bytes_bad_in_title(t, &bad_ranges_now))
                        .unwrap_or(0);
                    let main_title = self.titles.first();
                    let pp = crate::progress::PassProgress {
                        kind,
                        work_done,
                        work_total,
                        bytes_good_total: s.bytes_good,
                        bytes_unreadable_total: s.bytes_unreadable,
                        bytes_pending_total: s.bytes_pending,
                        bytes_total_disc: total_bytes,
                        disc_duration_secs: main_title.map(|t| t.duration_secs),
                        bytes_bad_in_main_title: main_title_bad,
                        main_title_duration_secs: main_title.map(|t| t.duration_secs),
                        main_title_size_bytes: main_title.map(|t| t.size_bytes),
                    };
                    if !reporter.report(&pp) {
                        halted = true;
                        break 'outer;
                    }
                }
            }
        }

        // Drain the consumer thread: drop tx, wait for `close` to run
        // sync_all + mapfile.flush, then take the final stats from the
        // sink's summary. `close` failing on a regular-file sync_all is
        // surfaced here as `Error::IoError`, matching pre-split
        // behaviour.
        let summary = pipe.finish()?;
        let stats = summary.stats;

        // Log final ISO file size for write verification
        if let Ok(metadata) = std::fs::metadata(path) {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_iso_size_end",
                iso_bytes = metadata.len(),
                bytes_recovered = stats.bytes_good.saturating_sub(bytes_good_before),
                "ISO file size at patch end"
            );
        }

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
            final_bytes_good = stats.bytes_good,
            final_bytes_unreadable = stats.bytes_unreadable,
            final_bytes_pending = stats.bytes_pending,
            total_ranges_processed = bad_ranges.len(),
            "Disc::patch returning"
        );
        Ok(PatchOutcome {
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
            wedged_threshold: opts.wedged_threshold,
        })
    }
}

const MAX_BATCH_SECTORS: u16 = 510;
const DEFAULT_BATCH_SECTORS_OPTICAL: u16 = 60;
const DEFAULT_BATCH_SECTORS_BLOCK: u16 = 8192;
const MIN_BATCH_SECTORS: u16 = 3;

pub(crate) fn ecc_sectors(format: DiscFormat) -> u16 {
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
pub fn detect_max_batch_sectors(device_path: &str) -> u16 {
    let dev_name = device_path.rsplit('/').next().unwrap_or("");
    if dev_name.is_empty() {
        return DEFAULT_BATCH_SECTORS_OPTICAL;
    }

    // Check if optical drive (0x05 = CD/DVD)
   let is_optical = (|| -> bool {
        use std::path::Path;
        let scsi_device_dir = format!("/sys/class/scsi_device/");
        if let Ok(entries) = std::fs::read_dir(&scsi_device_dir) {
            for entry in entries.flatten() {
                let device_type_path = entry.path().join("device/type");
                if Path::new(&device_type_path).exists() {
                    if let Ok(content) = std::fs::read_to_string(&device_type_path) {
                        // Type 0x05 (decimal 5) = CD/DVD drive
                        if content.trim().parse::<u32>() == Ok(5) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    })();

    if is_optical {
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
     DEFAULT_BATCH_SECTORS_OPTICAL
    } else {
        DEFAULT_BATCH_SECTORS_BLOCK
    }
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

    /// Build a DiscTitle with full control over the fields the title
    /// sorter cares about. Used by the canonical-title-order tests.
    fn title_with(
        playlist: &str,
        duration_secs: f64,
        size_bytes: u64,
        n_clips: usize,
    ) -> DiscTitle {
        let mut t = title_with_video(Codec::Hevc, Resolution::R2160p);
        t.playlist = playlist.into();
        t.duration_secs = duration_secs;
        t.size_bytes = size_bytes;
        t.clips = (0..n_clips)
            .map(|i| Clip {
                clip_id: format!("{i:05}"),
                in_time: 0,
                out_time: 1,
                duration_secs: 1.0,
                source_packets: 0,
            })
            .collect();
        t
    }

    /// Regression for branching-UHD title ordering. Mirrors the live
    /// observed *The Amateur (2025)* layout: a 4h13m / 92.4 GB / 253-clip
    /// virtual play-all playlist alongside the real 2h02m / 57.2 GB /
    /// 1-clip main feature. Disc capacity 58.5 GB. After sorting,
    /// titles[0] must be the main feature, not the virtual composite.
    #[test]
    fn canonical_order_pushes_oversize_play_all_behind_real_main() {
        const CAPACITY: u64 = 58_500_000_000; // 58.5 GB
        let mut titles = vec![
            // Title 1 in the raw MPLS order — virtual play-all
            title_with(
                "00020.mpls",
                4.0 * 3600.0 + 13.0 * 60.0,
                92_400_000_000,
                253,
            ),
            // Title 2 — actual movie
            title_with("00800.mpls", 2.0 * 3600.0 + 2.0 * 60.0, 57_200_000_000, 1),
        ];
        titles.sort_by(|a, b| Disc::canonical_title_order(a, b, CAPACITY));
        assert_eq!(
            titles[0].playlist, "00800.mpls",
            "main feature should land at index 0"
        );
        assert_eq!(
            titles[1].playlist, "00020.mpls",
            "virtual play-all should be pushed back"
        );
    }

    /// Non-branching disc: longest 1-clip title is the movie. Sort
    /// must not change behaviour — the existing "duration descending"
    /// expectation holds when no titles overflow capacity.
    #[test]
    fn canonical_order_preserves_natural_ranking_on_normal_disc() {
        const CAPACITY: u64 = 60_000_000_000;
        let mut titles = vec![
            title_with("00100.mpls", 600.0, 5_000_000_000, 1), // 10 min menu
            title_with("00800.mpls", 7320.0, 55_000_000_000, 1), // 2h02m main feature
            title_with("00200.mpls", 1800.0, 2_000_000_000, 1), // 30 min extra
        ];
        titles.sort_by(|a, b| Disc::canonical_title_order(a, b, CAPACITY));
        assert_eq!(
            titles[0].playlist, "00800.mpls",
            "longest valid title still wins"
        );
        assert_eq!(titles[1].playlist, "00200.mpls");
        assert_eq!(titles[2].playlist, "00100.mpls");
    }

    /// Tiebreak: equal duration + equal capacity-validity → fewer
    /// clips wins. A chapter-stitched 3-clip movie should beat a
    /// 50-clip virtual composite of the same duration.
    #[test]
    fn canonical_order_fewer_clips_wins_tiebreak() {
        const CAPACITY: u64 = 100_000_000_000;
        let mut titles = vec![
            title_with("00050.mpls", 7200.0, 50_000_000_000, 50),
            title_with("00800.mpls", 7200.0, 50_000_000_000, 3),
        ];
        titles.sort_by(|a, b| Disc::canonical_title_order(a, b, CAPACITY));
        assert_eq!(titles[0].playlist, "00800.mpls");
        assert_eq!(titles[1].playlist, "00050.mpls");
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
            aacs_error: None,
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

    struct MockReader {
        total_sectors: u32,
        bad_sectors: std::collections::HashSet<u32>,
    }

    impl crate::sector::SectorReader for MockReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let n = count as usize * 2048;
            for i in 0..count {
                if self.bad_sectors.contains(&(lba + i as u32)) {
                    return Err(crate::error::Error::DiscRead {
                        sector: (lba + i as u32) as u64,
                        status: Some(0x02),
                        sense: Some(crate::scsi::ScsiSense {
                            sense_key: 0x02,
                            asc: 0x04,
                            ascq: 0x3E,
                        }),
                    });
                }
            }
            buf[..n].fill(0xAA);
            Ok(n)
        }

        fn capacity(&self) -> u32 {
            self.total_sectors
        }
    }

    fn make_test_disc(sectors: u32, name: &str) -> Disc {
        Disc {
            volume_id: name.into(),
            meta_title: Some(name.into()),
            format: DiscFormat::Uhd,
            capacity_sectors: sectors,
            capacity_bytes: sectors as u64 * 2048,
            layers: 1,
            titles: Vec::new(),
            region: DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: false,
            aacs_error: None,
            content_format: ContentFormat::BdTs,
        }
    }

    #[test]
    fn sweep_to_dev_null_no_enodev() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        let sectors: u32 = 1000;
        let bad: std::collections::HashSet<u32> = [500u32, 501, 502].into_iter().collect();
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: bad,
        };
        let disc = make_test_disc(sectors, "T1");
        let opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
        };
        let result = disc.copy(&mut reader, &iso_path, &opts);
        assert!(
            result.is_ok(),
            "sweep to regular file should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn sweep_to_dev_null_real() {
        let _cleanup = CleanupGuard(std::path::PathBuf::from("/tmp/T2.mapfile"));
        let sectors: u32 = 1000;
        let bad: std::collections::HashSet<u32> = [500u32, 501, 502].into_iter().collect();
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: bad,
        };
        let disc = make_test_disc(sectors, "T2");
        let opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
        };
        let result = disc.copy(&mut reader, std::path::Path::new("/dev/null"), &opts);
        assert!(
            result.is_ok(),
            "sweep to /dev/null should not fail with ENODEV: {:?}",
            result.err()
        );
    }

    struct CleanupGuard(std::path::PathBuf);
    impl Drop for CleanupGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn sweep_dev_null_full_good() {
        let _cleanup = CleanupGuard(std::path::PathBuf::from("/tmp/T3.mapfile"));
        let sectors: u32 = 2000;
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let disc = make_test_disc(sectors, "T3");
        let opts = CopyOptions {
            decrypt: false,
            multipass: false,
            progress: None,
            halt: None,
        };
        let result = disc.copy(&mut reader, std::path::Path::new("/dev/null"), &opts);
        assert!(
            result.is_ok(),
            "full-good sweep to /dev/null should succeed: {:?}",
            result.err()
        );
        let r = result.unwrap();
        assert!(r.complete, "should be complete");
        assert_eq!(r.bytes_good, sectors as u64 * 2048);
    }

    #[test]
    fn patch_dev_null_after_sweep() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        let sectors: u32 = 500;
        let bad: std::collections::HashSet<u32> = [100u32, 200, 300].into_iter().collect();
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: bad.clone(),
        };
        let disc = make_test_disc(sectors, "T4");

        let sweep_opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
        };
        let sweep_result = disc.copy(&mut reader, &iso_path, &sweep_opts);
        assert!(
            sweep_result.is_ok(),
            "sweep should succeed: {:?}",
            sweep_result.err()
        );

        let mut reader2 = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let patch_opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
        };
        let patch_result = disc.copy(&mut reader2, &iso_path, &patch_opts);
        assert!(
            patch_result.is_ok(),
            "patch should succeed: {:?}",
            patch_result.err()
        );
        let pr = patch_result.unwrap();
        assert!(
            pr.complete,
            "patch should complete: bytes_pending={}",
            pr.bytes_pending
        );
    }

    #[test]
    fn patch_dev_null_direct() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        let sectors: u32 = 500;
        let bad: std::collections::HashSet<u32> = [100u32, 200, 300].into_iter().collect();
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: bad.clone(),
        };
        let disc = make_test_disc(sectors, "T5");

        let sweep_opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
        };
        let _sweep_result = disc.copy(&mut reader, &iso_path, &sweep_opts).unwrap();

        let mut reader2 = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let patch_opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
        };
        let patch_result = disc.copy(&mut reader2, std::path::Path::new("/dev/null"), &patch_opts);
        assert!(
            patch_result.is_ok(),
            "patch to /dev/null should succeed: {:?}",
            patch_result.err()
        );
    }

    /// Synthetic regression test for the 0.18 SweepSink + Pipeline
    /// migration. ~100 batches of clean reads (6000 sectors at the
    /// default 60-sector single-pass batch size); verifies all bytes
    /// land in the ISO and the consumer's final stats match the input.
    /// The throughput regression check (vs 0.17.13) is a separate
    /// manual / live-drive concern; here we only assert correctness.
    #[test]
    fn sweep_pipeline_full_good_100_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        // 6000 sectors / 60-sector default batch = exactly 100
        // produce/consume cycles through the pipeline.
        let sectors: u32 = 6000;
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let disc = make_test_disc(sectors, "TPipeline100");
        let opts = CopyOptions {
            decrypt: false,
            multipass: false,
            progress: None,
            halt: None,
        };
        let result = disc.copy(&mut reader, &iso_path, &opts);
        let r = result.expect("100-batch clean sweep should succeed");
        assert!(r.complete, "complete=true expected");
        assert!(!r.halted, "halted=false expected");
        assert_eq!(
            r.bytes_good,
            sectors as u64 * 2048,
            "all sectors must be marked good after a 100% clean sweep"
        );
        assert_eq!(
            r.bytes_pending, 0,
            "no pending bytes expected after a clean sweep"
        );
        // The ISO file must end up the right size — the consumer
        // wrote everything before fsync.
        let meta = std::fs::metadata(&iso_path).unwrap();
        assert_eq!(meta.len(), sectors as u64 * 2048);
    }
}
