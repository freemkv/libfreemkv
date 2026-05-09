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
pub mod read_error;

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
        let patch_opts = PatchOpts {
            decrypt: opts.decrypt,
            block_sectors: Some(1),
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

    fn sweep(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &SweepOptions,
    ) -> Result<CopyResult> {
        use std::io::{Seek, SeekFrom, Write};

        let total_bytes = self.capacity_sectors as u64 * 2048;
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        // Mapfile: load if resuming, else wipe + recreate.
        let mapfile_path = self.mapfile_for(path);
        if !opts.resume {
            let _ = std::fs::remove_file(&mapfile_path);
        }
        let mut map = mapfile::Mapfile::open_or_create(
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

        // Wrap the raw `File` in our bounded-cache writer so the
        // kernel's writeback queue drains continuously instead of
        // accumulating hundreds of MB of dirty pages and then bursting
        // a flush that blocks app writes (see `crate::io`).
        let mut file = crate::io::Writer::new(file).map_err(|e| Error::IoError { source: e })?;
        let batch: u16 = match opts.batch_sectors {
            Some(b) => b,
            None if opts.skip_on_error => ecc_sectors(self.format),
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
        // ALL read state lives in one place. The single error-handling
        // entry point (`read_error::handle_read_error`) owns the
        // counters, retry budgets, and damage-window updates.
        let mut read_ctx = read_error::ReadCtx::for_sweep(batch);
        // Speed control derives from damage-zone state. We track the
        // transition locally so we only call set_speed on edges, not
        // every iteration.
        let mut in_damage_zone = false;
        const DAMAGE_ZONE_EXIT_THRESHOLD: u64 = 16;
        tracing::trace!(
            target: "freemkv::disc",
            phase = "copy_start",
            total_bytes,
            batch,
            skip_on_error = opts.skip_on_error,
            "Disc::copy entered"
        );

        'outer: loop {
            // Every pass retries every non-Finished range. Includes
            // Unreadable so each pass gets its own shot at sectors prior
            // passes gave up on — drive state may have changed (cooled
            // down, bridge stabilized, etc.). Mapfile is binary in
            // intent: Finished or not-yet-good.
            let regions_to_do = map.ranges_with(&[
                mapfile::SectorStatus::NonTried,
                mapfile::SectorStatus::NonTrimmed,
                mapfile::SectorStatus::NonScraped,
                mapfile::SectorStatus::Unreadable,
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

                match read_result {
                    Ok(_) => {
                        // === SUCCESS PATH ===
                        read_ok_count += 1;
                        read_ctx.on_success();

                        // Damage-zone exit: after enough consecutive good
                        // reads, restore max speed and reset jump multiplier.
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
                        pos += block_bytes;
                    }
                    Err(err) if !opts.skip_on_error => {
                        // Caller asked us not to skip. Surface the error verbatim.
                        let (status, sense) = extract_scsi_context(&err);
                        return Err(Error::DiscRead {
                            sector: block_lba as u64,
                            status: Some(status),
                            sense,
                        });
                    }
                    Err(err) => {
                        // === ERROR PATH — single source of truth ===
                        // ALL errors flow through handle_read_error. New
                        // error class = one new arm in that function.
                        // Logging, counter updates, retry budgets all live
                        // there. The dispatch below is purely the I/O side
                        // of each action.
                        read_err_count += 1;
                        let action = read_error::handle_read_error(&err, &mut read_ctx);

                        match action {
                            read_error::ReadAction::Retry { pause_secs } => {
                                if pause_secs > 0 {
                                    std::thread::sleep(std::time::Duration::from_secs(pause_secs));
                                }
                                // Don't advance pos — same LBA next iteration.
                            }
                            read_error::ReadAction::Bisect => {
                                // Re-issue the failed batch as single-sector reads.
                                // ctx.bisecting=true so the inner failures don't
                                // recursively request another bisect.
                                read_ctx.bisecting = true;
                                let saved_batch = read_ctx.batch;
                                read_ctx.batch = 1;
                                for sector_offset in 0..block_count {
                                    if let Some(ref h) = opts.halt {
                                        if h.load(std::sync::atomic::Ordering::Relaxed) {
                                            halt_requested = true;
                                            read_ctx.bisecting = false;
                                            read_ctx.batch = saved_batch;
                                            break 'outer;
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
                                            file.seek(SeekFrom::Start(write_pos))
                                                .map_err(|e| Error::IoError { source: e })?;
                                            file.write_all(&sector_buf)
                                                .map_err(|e| Error::IoError { source: e })?;
                                            map.record(
                                                write_pos,
                                                2048,
                                                mapfile::SectorStatus::Finished,
                                            )
                                            .map_err(|e| Error::IoError { source: e })?;
                                        }
                                        Err(inner_err) => {
                                            // Inner failure goes through the same handler — it'll
                                            // see bisecting=true and won't recurse. We only honour
                                            // the SkipBlock action here (the bisect by definition
                                            // can't return another Bisect, and JumpAhead inside a
                                            // single-sector retry doesn't make sense).
                                            let _ = read_error::handle_read_error(
                                                &inner_err,
                                                &mut read_ctx,
                                            );
                                            let zero = [0u8; 2048];
                                            file.seek(SeekFrom::Start(write_pos))
                                                .map_err(|e| Error::IoError { source: e })?;
                                            file.write_all(&zero)
                                                .map_err(|e| Error::IoError { source: e })?;
                                            map.record(
                                                write_pos,
                                                2048,
                                                mapfile::SectorStatus::NonTrimmed,
                                            )
                                            .map_err(|e| Error::IoError { source: e })?;
                                        }
                                    }
                                }
                                read_ctx.bisecting = false;
                                read_ctx.batch = saved_batch;
                                bytes_done = bytes_done.saturating_add(block_bytes);
                                pos += block_bytes;
                            }
                            read_error::ReadAction::SkipBlock { pause_secs } => {
                                let zero = vec![0u8; block_bytes as usize];
                                file.seek(SeekFrom::Start(pos))
                                    .map_err(|e| Error::IoError { source: e })?;
                                file.write_all(&zero)
                                    .map_err(|e| Error::IoError { source: e })?;
                                map.record(pos, block_bytes, mapfile::SectorStatus::NonTrimmed)
                                    .map_err(|e| Error::IoError { source: e })?;
                                bytes_done = bytes_done.saturating_add(block_bytes);
                                if pause_secs > 0 {
                                    std::thread::sleep(std::time::Duration::from_secs(pause_secs));
                                }
                                pos += block_bytes;
                            }
                            read_error::ReadAction::JumpAhead {
                                sectors,
                                pause_secs,
                            } => {
                                // Mark the failed batch + the gap up to jump_pos NonTrimmed.
                                let zero_batch = vec![0u8; block_bytes as usize];
                                file.seek(SeekFrom::Start(pos))
                                    .map_err(|e| Error::IoError { source: e })?;
                                file.write_all(&zero_batch)
                                    .map_err(|e| Error::IoError { source: e })?;
                                map.record(pos, block_bytes, mapfile::SectorStatus::NonTrimmed)
                                    .map_err(|e| Error::IoError { source: e })?;
                                bytes_done = bytes_done.saturating_add(block_bytes);

                                // Damage-zone enter: drop to minimum read speed.
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
                                    let zero_gap = vec![0u8; 65536];
                                    let mut filled: u64 = 0;
                                    while filled < gap_bytes {
                                        let chunk = (gap_bytes - filled).min(zero_gap.len() as u64);
                                        file.seek(SeekFrom::Start(gap_start + filled))
                                            .map_err(|e| Error::IoError { source: e })?;
                                        file.write_all(&zero_gap[..chunk as usize])
                                            .map_err(|e| Error::IoError { source: e })?;
                                        filled += chunk;
                                    }
                                    map.record(
                                        gap_start,
                                        gap_bytes,
                                        mapfile::SectorStatus::NonTrimmed,
                                    )
                                    .map_err(|e| Error::IoError { source: e })?;
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
                                if pause_secs > 0 {
                                    std::thread::sleep(std::time::Duration::from_secs(pause_secs));
                                }
                            }
                            read_error::ReadAction::AbortPass => {
                                let (status, sense) = extract_scsi_context(&err);
                                return Err(Error::DiscRead {
                                    sector: block_lba as u64,
                                    status: Some(status),
                                    sense,
                                });
                            }
                        }
                    }
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
                    let bad_ranges = map.ranges_with(&[
                        mapfile::SectorStatus::NonTrimmed,
                        mapfile::SectorStatus::Unreadable,
                        mapfile::SectorStatus::NonScraped,
                        mapfile::SectorStatus::NonTried,
                    ]);
                    let main_title_bad = self
                        .titles
                        .first()
                        .map(|t| bytes_bad_in_title(t, &bad_ranges))
                        .unwrap_or(0);
                    let main_title = self.titles.first();
                    let pp = crate::progress::PassProgress {
                        kind: crate::progress::PassKind::Sweep,
                        work_done: pos,
                        work_total: total_bytes,
                        bytes_good_total: stats.bytes_good,
                        bytes_unreadable_total: stats.bytes_unreadable,
                        bytes_pending_total: stats.bytes_pending,
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

        tracing::debug!(
            target: "freemkv::disc",
            phase = "sweep_sync",
            file_len = file.metadata().map(|m| m.len()).unwrap_or(0),
            "sweep: calling sync_all"
        );
        if let Err(e) = file.sync_all() {
            if is_regular {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "sweep_sync_failed",
                    error = %e,
                    os_error = e.raw_os_error(),
                    error_kind = ?e.kind(),
                    "sweep: sync_all failed"
                );
                return Err(Error::IoError { source: e });
            }
            tracing::debug!(
                target: "freemkv::disc",
                phase = "sweep_sync_skipped",
                error = %e,
                "sweep: sync_all failed for non-regular file; ignoring"
            );
        }
        let stats = map.stats();
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

pub(crate) struct SweepOptions<'a> {
    pub decrypt: bool,
    pub resume: bool,
    pub batch_sectors: Option<u16>,
    pub skip_on_error: bool,
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

pub(crate) struct PatchOpts<'a> {
    pub decrypt: bool,
    pub block_sectors: Option<u16>,
    pub full_recovery: bool,
    pub reverse: bool,
    pub wedged_threshold: u64,
    pub progress: Option<&'a dyn crate::progress::Progress>,
    pub halt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

#[allow(dead_code)]
pub(crate) struct PatchOutcome {
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

    fn patch(
        &self,
        reader: &mut dyn SectorReader,
        path: &std::path::Path,
        opts: &PatchOpts,
    ) -> Result<PatchOutcome> {
        use std::io::{Seek, SeekFrom, Write};

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
        let mut map =
            mapfile::Mapfile::load(&mapfile_path).map_err(|e| Error::IoError { source: e })?;
        let total_bytes = map.total_size();
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };

        let is_regular = std::fs::metadata(path)
            .map(|m| m.file_type().is_file())
            .unwrap_or(false);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| Error::IoError { source: e })?;

        // Log ISO file size at patch start for write monitoring
        if let Ok(metadata) = std::fs::metadata(path) {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_iso_size_start",
                iso_bytes = metadata.len(),
                "ISO file size at patch start"
            );
        }

        let block_sectors = opts.block_sectors.unwrap_or(1);
        let recovery = opts.full_recovery;

        let bytes_good_before = map.stats().bytes_good;
        let bytes_good_start = bytes_good_before;
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
        let mut buf = vec![0u8; block_sectors as usize * 2048];

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
            total_entries = map.entries().len(),
            bytes_good_before,
            bytes_retryable = map.stats().bytes_retryable,
            bytes_unreadable = map.stats().bytes_unreadable,
            bytes_nontried = map.stats().bytes_nontried,
            "Mapfile state snapshot at patch start"
        );

        // Log first 10 and last 10 entries for inspection
        let entries = map.entries();
        if !entries.is_empty() {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_mapfile_entries_start",
                num_to_log = (entries.len().min(10)) as u32,
                "First 10 entries"
            );
            for entry in entries.iter().take(10) {
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
        if entries.len() > 10 {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_mapfile_entries_end",
                num_to_log = (entries.len().min(10)) as u32,
                "Last 10 entries"
            );
            for entry in entries.iter().skip(entries.len() - 10) {
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
            block_sectors,
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

                let bytes_good_now = map.stats().bytes_good;
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
                        map.record(
                            *range_pos,
                            unmarked_bytes,
                            mapfile::SectorStatus::NonTrimmed,
                        )
                        .map_err(|e| Error::IoError { source: e })?;
                    } else {
                        let remaining_start = *range_pos + (end - block_end);
                        if remaining_start < end {
                            map.record(
                                remaining_start,
                                end - remaining_start,
                                mapfile::SectorStatus::NonTrimmed,
                            )
                            .map_err(|e| Error::IoError { source: e })?;
                        }
                    }
                    // Continue to next range (break inner loop only)
                    break;
                }
                let (pos, block_bytes) = if opts.reverse {
                    if block_end <= *range_pos {
                        break;
                    }
                    let span = (block_end - *range_pos).min(block_sectors as u64 * 2048);
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
                        if opts.decrypt {
                            crate::decrypt::decrypt_sectors(&mut buf[..bytes], &keys, 0)?;
                        }
                        let write_start = std::time::Instant::now();
                        file.seek(SeekFrom::Start(pos))
                            .map_err(|e| Error::IoError { source: e })?;
                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "patch_write_start",
                            pos,
                            bytes,
                            "Starting ISO write"
                        );
                        file.write_all(&buf[..bytes])
                            .map_err(|e| Error::IoError { source: e })?;
                        let write_duration_ms = write_start.elapsed().as_millis();
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_write_ok",
                            pos,
                            bytes,
                            write_duration_ms,
                            "ISO write succeeded"
                        );
                        let mapfile_record_start = std::time::Instant::now();
                        map.record(pos, block_bytes, mapfile::SectorStatus::Finished)
                            .map_err(|e| Error::IoError { source: e })?;
                        let mapfile_record_duration_ms = mapfile_record_start.elapsed().as_millis();
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_mapfile_record_ok",
                            pos,
                            block_bytes,
                            mapfile_record_duration_ms,
                            "Mapfile record written"
                        );

                        // Stall guard: watch bytes_good (real progress), not pos (advances on skips)
                        let bytes_good_now = map.stats().bytes_good;
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
                                        (backtrack_end - bt_pos).min(block_sectors as u64 * 2048);
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
                                            if opts.decrypt {
                                                crate::decrypt::decrypt_sectors(
                                                    &mut buf[..bt_bytes],
                                                    &keys,
                                                    0,
                                                )?;
                                            }
                                            file.seek(SeekFrom::Start(bt_pos))
                                                .map_err(|e| Error::IoError { source: e })?;
                                            file.write_all(&buf[..bt_bytes])
                                                .map_err(|e| Error::IoError { source: e })?;
                                            map.record(
                                                bt_pos,
                                                span,
                                                mapfile::SectorStatus::Finished,
                                            )
                                            .map_err(|e| Error::IoError { source: e })?;
                                        }
                                        Err(_err) => {
                                            blocks_read_failed += 1;
                                            map.record(
                                                bt_pos,
                                                span,
                                                mapfile::SectorStatus::Unreadable,
                                            )
                                            .map_err(|e| Error::IoError { source: e })?;
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
                        blocks_read_failed += 1;
                        consecutive_failures += 1;
                        consecutive_good_since_skip = 0;
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

                                    if opts.decrypt {
                                        crate::decrypt::decrypt_sectors(
                                            &mut buf[..bytes],
                                            &keys,
                                            0,
                                        )?;
                                    }
                                    let write_start = std::time::Instant::now();
                                    file.seek(SeekFrom::Start(pos))
                                        .map_err(|e| Error::IoError { source: e })?;
                                    tracing::debug!(
                                        target: "freemkv::disc",
                                        phase = "patch_write_start",
                                        pos,
                                        bytes,
                                        "Starting ISO write"
                                    );
                                    file.write_all(&buf[..bytes])
                                        .map_err(|e| Error::IoError { source: e })?;
                                    let write_duration_ms = write_start.elapsed().as_millis();
                                    tracing::info!(
                                        target: "freemkv::disc",
                                        phase = "patch_write_ok",
                                        pos,
                                        bytes,
                                        write_duration_ms,
                                        "ISO write succeeded"
                                    );
                                    let mapfile_record_start = std::time::Instant::now();
                                    map.record(pos, block_bytes, mapfile::SectorStatus::Finished)
                                        .map_err(|e| Error::IoError { source: e })?;
                                    let mapfile_record_duration_ms =
                                        mapfile_record_start.elapsed().as_millis();
                                    tracing::info!(
                                        target: "freemkv::disc",
                                        phase = "patch_mapfile_record_ok",
                                        pos,
                                        block_bytes,
                                        mapfile_record_duration_ms,
                                        "Mapfile record written"
                                    );

                                    // Stall guard after successful retry
                                    let bytes_good_now = map.stats().bytes_good;
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

                        // All retries exhausted - mark as Unreadable
                        map.record(pos, block_bytes, mapfile::SectorStatus::Unreadable)
                            .map_err(|e| Error::IoError { source: e })?;

                        damage_window.push(false);
                        if damage_window.len() > PASSN_DAMAGE_WINDOW {
                            damage_window.remove(0);
                        }

                        // Stall guard: check on failures too, not just successes
                        let bytes_good_now = map.stats().bytes_good;
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
                                tracing::warn!(
                                    target: "freemkv::disc",
                                    phase = "patch_potential_wedge",
                                    consecutive_failures,
                                    lba,
                                    range_idx,
                                    "All probes failed — possible wedge condition"
                                );
                            }
                        }

                        // Log mapfile record for Unreadable status
                        let mapfile_record_start = std::time::Instant::now();
                        map.record(pos, block_bytes, mapfile::SectorStatus::Unreadable)
                            .map_err(|e| Error::IoError { source: e })?;
                        let mapfile_record_duration_ms = mapfile_record_start.elapsed().as_millis();
                        tracing::info!(
                            target: "freemkv::disc",
                            phase = "patch_mapfile_record_unreadable",
                            pos,
                            block_bytes,
                            consecutive_failures,
                            mapfile_record_duration_ms,
                            "Mapfile record written as Unreadable"
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
                    let bad_ranges = map.ranges_with(&[
                        mapfile::SectorStatus::NonTrimmed,
                        mapfile::SectorStatus::Unreadable,
                        mapfile::SectorStatus::NonScraped,
                        mapfile::SectorStatus::NonTried,
                    ]);
                    let main_title_bad = self
                        .titles
                        .first()
                        .map(|t| bytes_bad_in_title(t, &bad_ranges))
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

        if let Err(e) = file.sync_all() {
            if is_regular {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "patch_sync_failed",
                    error = %e,
                    os_error = e.raw_os_error(),
                    error_kind = ?e.kind(),
                    "patch: sync_all failed"
                );
                return Err(Error::IoError { source: e });
            }
            tracing::debug!(
                target: "freemkv::disc",
                phase = "patch_sync_skipped",
                error = %e,
                "patch: sync_all failed for non-regular file; ignoring"
            );
        }

        // Log final ISO file size for write verification
        if let Ok(metadata) = std::fs::metadata(path) {
            tracing::info!(
                target: "freemkv::disc",
                phase = "patch_iso_size_end",
                iso_bytes = metadata.len(),
                bytes_recovered = map.stats().bytes_good.saturating_sub(bytes_good_before),
                "ISO file size at patch end"
            );
        }

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
const DEFAULT_BATCH_SECTORS: u16 = 60;
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
}
