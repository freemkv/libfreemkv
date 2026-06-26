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
pub mod dvd_audio_probe;
mod encrypt;
mod extract;
pub mod mapfile;
mod patch;
pub mod read_error;
mod sweep;

use crate::drive::{Drive, extract_scsi_context};
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf;

use encrypt::HandshakeResult;

// Re-export label classification enums alongside AudioStream / SubtitleStream
// so the public surface keeps the structured metadata together. Callers map
// these to display text in their own locale.
pub use crate::labels::{LabelPurpose, LabelQualifier};
pub use extract::{ExtractOptions, ExtractResult, FileResult};

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
    /// CSS crack failure: `Some(Error::CssKeyMissing)` when the scan SAW
    /// scrambled sectors but could NOT recover a title key (the
    /// known-plaintext attack found no crackable crib, or the scrambled
    /// region was unreadable). `css` is `None` in that case — but the disc is
    /// genuinely encrypted, so callers MUST surface this hard error rather
    /// than treat `css.is_none()` as "unencrypted" and mux scrambled MPEG as
    /// plaintext garbage. `None` when no scrambled sector was seen (genuinely
    /// unencrypted) or a key was recovered (`css.is_some()`). The CSS analogue
    /// of [`Self::aacs_error`].
    pub css_error: Option<crate::error::Error>,
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
    /// Intended display aspect ratio as `(num, den)` when the coded pixels are
    /// **anamorphic** (display shape ≠ pixel grid) — e.g. DVD 720x576 shown as
    /// 16:9 → `Some((16, 9))`. `None` means square pixels: the display aspect
    /// equals the pixel dimensions (HD/UHD, BD). Consumed by the MKV muxer to
    /// write DisplayWidth/DisplayHeight; passthrough muxers (TS/M2TS) ignore it
    /// because the aspect already lives in the elementary stream.
    pub display_aspect: Option<(u32, u32)>,
    /// Whether this is a secondary stream (PiP, Dolby Vision EL)
    pub secondary: bool,
    /// Extra label (e.g. "Dolby Vision EL")
    pub label: String,
    /// CICP colour signalling (matrix, transfer, primaries, full_range) MEASURED
    /// from the bitstream — HEVC/H.264 VUI `colour_description` or MPEG-2
    /// `sequence_display_extension`. `Some(...)` takes precedence over the
    /// coarse `color_space` enum (a playlist nibble / PAL-NTSC guess); `None`
    /// means the bitstream did not state it, so the enum-derived triplet is used.
    /// Codes are ITU-T H.273 (CICP); `range` is 1 = limited/TV, 2 = full.
    pub measured_cicp: Option<MeasuredCicp>,
}

/// Measured CICP colour signalling read directly from a video elementary stream
/// (ITU-T H.273). Preferred over the coarse [`ColorSpace`] enum when present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasuredCicp {
    /// MatrixCoefficients (ITU-T H.273 Table 4).
    pub matrix: u8,
    /// TransferCharacteristics (ITU-T H.273 Table 3).
    pub transfer: u8,
    /// ColourPrimaries (ITU-T H.273 Table 2).
    pub primaries: u8,
    /// Range: 1 = limited (studio/TV), 2 = full. Matroska Colour/Range values.
    pub range: u8,
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
    /// 88.2 kHz — 44.1 kHz-family high-res TrueHD (music BD)
    S88_2,
    /// 96 kHz — high-res BD audio
    S96,
    /// 176.4 kHz — 44.1 kHz-family high-res TrueHD (music BD)
    S176_4,
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
    /// SD PAL/576-line colorimetry (ITU-R BT.470 System B/G — primaries 5,
    /// transfer 5, matrix 5). DVDs are SD, not HD: stamping BT.709 mis-tags
    /// their colour.
    Bt470bg,
    /// SD NTSC/480-line colorimetry (SMPTE 170M / BT.601-525 — primaries 6,
    /// transfer 6, matrix 6).
    Smpte170m,
    Unknown,
}

/// A chapter point within a title.
#[derive(Debug, Clone)]
pub struct Chapter {
    /// Chapter start time in seconds
    pub time_secs: f64,
    /// Chapter name — a bare 1-based index ("1", "2", …). The library
    /// emits no localized prose; consuming apps prepend any "Chapter "
    /// prefix in the user's language.
    pub name: String,
}

/// Default chapter name for the 0-based chapter index `i`: the bare
/// 1-based ordinal as a string. Keeps chapter labelling language-neutral
/// (apps localize) and gives BD and DVD a single source of truth.
pub(crate) fn chapter_name(i: usize) -> String {
    (i + 1).to_string()
}

/// A contiguous range of sectors on disc.
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    pub start_lba: u32,
    pub sector_count: u32,
}

/// Correct a title's TrueHD audio-stream metadata by probing the first
/// decrypted access units — channel count, real sample rate, and Atmos
/// detection in a single major-sync read. The MPLS descriptors declare the BASE
/// layout (often 5.1 / a container-guessed rate) even for a 7.1/Atmos TrueHD
/// track; the truth is in the MLP major sync. `reader` must yield DECRYPTED
/// sectors (the m2ts is AACS-encrypted, so this can only run at mux time, not
/// scan). Reads a bounded window of the title's first extent.
///
/// Corrections, each individually guarded so a malformed field never writes a
/// wrong header:
/// - **Channels**: from the presentation channel masks (as before).
/// - **Sample rate**: from the whitelisted rate nibble; left untouched on an
///   unknown rate or no major sync.
/// - **Atmos**: when a 4th substream is detected AND the stream still carries
///   the basic descriptor label, the label is promoted to the Atmos form;
///   richer editorial labels (e.g. an existing "Dolby Atmos") are left intact.
pub(crate) fn correct_truehd_channels(reader: &mut dyn SectorSource, title: &mut DiscTitle) {
    use crate::mux::codec::truehd::{
        truehd_channels, truehd_sample_rate_hz, truehd_sync_info_from_stream,
    };

    let pids: Vec<u16> = title
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Audio(a) if matches!(a.codec, Codec::TrueHd) => Some(a.pid),
            _ => None,
        })
        .collect();
    if pids.is_empty() {
        return;
    }
    let Some(ext) = title.extents.first() else {
        return;
    };
    // Bounded probe: up to 8 MiB from the start of the title — enough for the
    // first interleaved TrueHD major sync of each stream.
    const PROBE_SECTORS: u32 = 4096;
    let n = ext.sector_count.min(PROBE_SECTORS) as u16;
    if n == 0 {
        return;
    }
    let mut buf = vec![0u8; n as usize * 2048];
    // Anchor the AACS unit-alignment gate to the title's encrypted-region start
    // before probing. Without this a `DecryptingSectorSource` falls back to an
    // absolute `start_lba % 3` gate; a non-3-aligned `ext.start_lba` then trips
    // DecryptFailed on the very first probe read, so the TrueHD channel count is
    // never corrected and Atmos / 7.1 is silently understated as 5.1. No-op for
    // CSS / unencrypted sources (set_unit_base default is a no-op).
    reader.set_unit_base(ext.start_lba);
    if reader
        .read_sectors(ext.start_lba, n, &mut buf, true)
        .is_err()
    {
        return;
    }

    let mut demux = crate::mux::ts::TsDemuxer::new(&pids);
    let mut payloads: std::collections::HashMap<u16, Vec<u8>> = std::collections::HashMap::new();
    for pes in demux.feed(&buf).into_iter().chain(demux.flush()) {
        payloads
            .entry(pes.pid)
            .or_default()
            .extend_from_slice(&pes.data);
    }

    for s in title.streams.iter_mut() {
        let Stream::Audio(a) = s else { continue };
        if !matches!(a.codec, Codec::TrueHd) {
            continue;
        }
        let Some(payload) = payloads.get(&a.pid) else {
            continue;
        };
        // One major-sync read yields channels, sample rate and the Atmos signal.
        let Some(info) = truehd_sync_info_from_stream(payload) else {
            continue;
        };

        // Whether the label is still the plain descriptor (no richer editorial
        // label). Captured against the CURRENT channels before any correction so
        // a label promotion only happens when nothing editorial is present.
        let was_basic =
            a.label == crate::labels::generate_audio_label(&a.codec, &a.channels, a.secondary);

        // (1) Channels — only when the major sync resolves a different layout.
        if let Some(count) = truehd_channels(info.format_info) {
            let new_ch = AudioChannels::from_count(count);
            if new_ch != AudioChannels::Unknown && new_ch != a.channels {
                a.channels = new_ch;
            }
        }

        // (2) Sample rate — whitelisted rates only; an unknown nibble or a rate
        // that maps to no enum variant leaves the container value untouched
        // (never write a wrong SamplingFrequency).
        if let Some(hz) = truehd_sample_rate_hz(info.format_info) {
            let new_sr = SampleRate::from_hz(hz);
            if new_sr != SampleRate::Unknown && new_sr != a.sample_rate {
                a.sample_rate = new_sr;
            }
        }

        // (3) Label — refresh to the corrected channels; promote to the Atmos
        // form only when the stream carried the basic descriptor (no editorial
        // Atmos already) AND a 4th substream was positively detected.
        if was_basic {
            a.label = if info.is_atmos == Some(true) {
                crate::labels::generate_audio_label_atmos(&a.codec, &a.channels, a.secondary)
            } else {
                crate::labels::generate_audio_label(&a.codec, &a.channels, a.secondary)
            };
        }
    }
}

/// Calculate how many bytes of bad/unreadable data fall within a title's extents.
/// `pub(crate)` so autorip can use it for main-movie lost_ms computation.
pub fn bytes_bad_in_title(title: &DiscTitle, bad_ranges: &[(u64, u64)]) -> u64 {
    if bad_ranges.is_empty() || title.extents.is_empty() {
        return 0;
    }
    // Overlap each bad range against every extent individually. A single
    // bounding box (first extent start → last extent end) would count
    // bad sectors in inter-extent gaps (other titles' data, BDMV
    // metadata) as bad bytes in this title, over-counting lost_ms for
    // titles with non-contiguous clips.
    let mut total: u64 = 0;
    for ext in &title.extents {
        let es = (ext.start_lba as u64) * 2048;
        let ee = ((ext.start_lba as u64) + (ext.sector_count as u64)) * 2048;
        for (pos, size) in bad_ranges {
            let r_start = *pos;
            let r_end = pos.saturating_add(*size);
            let overlap_start = r_start.max(es);
            let overlap_end = r_end.min(ee);
            total = total.saturating_add(overlap_end.saturating_sub(overlap_start));
        }
    }
    total
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

    pub(crate) fn from_coding_type(ct: u8) -> Self {
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
            // 0x86 (primary) / 0xA2 (secondary) are the DTS-HD MA
            // lossless pair, parallel to 0x81/0xA1 for AC-3. 0xA2 is
            // lossless MA, not lossy HR.
            0xA2 => Codec::DtsHdMa,
            // 0x90 = Presentation Graphics (PG / subtitles). 0x91 = Interactive
            // Graphics (IG / menus) and 0x92 = Text subtitles are distinct HDMV
            // coding types and are NOT PG subtitle streams; only 0x90 maps to
            // Pgs. IG (0x91) falls through to Unknown so the PMT/STN walker drops
            // it rather than surfacing a bogus PGS subtitle track for a menu ES.
            0x90 => Codec::Pgs,
            ct => Codec::Unknown(ct),
        }
    }

    /// Broad stream category for a codec. Used by demuxers to decide
    /// whether a PMT/STN entry becomes a video, audio, or subtitle
    /// `Stream` without duplicating per-codec knowledge.
    pub fn kind(&self) -> CodecKind {
        match self {
            Codec::Hevc | Codec::H264 | Codec::Vc1 | Codec::Mpeg2 | Codec::Mpeg1 | Codec::Av1 => {
                CodecKind::Video
            }
            Codec::TrueHd
            | Codec::DtsHdMa
            | Codec::DtsHdHr
            | Codec::Dts
            | Codec::Ac3
            | Codec::Ac3Plus
            | Codec::Lpcm
            | Codec::Aac
            | Codec::Mp2
            | Codec::Mp3
            | Codec::Flac
            | Codec::Opus => CodecKind::Audio,
            Codec::Pgs | Codec::DvdSub | Codec::Srt | Codec::Ssa => CodecKind::Subtitle,
            Codec::Unknown(_) => CodecKind::Unknown,
        }
    }
}

/// Broad category of a [`Codec`] — video / audio / subtitle / unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecKind {
    Video,
    Audio,
    Subtitle,
    Unknown,
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
            other => {
                tracing::warn!(video_format = other, "unknown MPLS video_format byte");
                Resolution::Unknown
            }
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

    /// True if this is an interlaced resolution (the `R*i` variants).
    pub fn is_interlaced(&self) -> bool {
        matches!(
            self,
            Resolution::R480i | Resolution::R576i | Resolution::R1080i
        )
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
            other => {
                tracing::warn!(video_rate = other, "unknown MPLS video_rate byte");
                FrameRate::Unknown
            }
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
            other => {
                tracing::warn!(audio_format = other, "unknown MPLS audio_format byte");
                AudioChannels::Unknown
            }
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
            other => {
                tracing::warn!(audio_rate = other, "unknown MPLS audio_rate byte");
                SampleRate::Unknown
            }
        }
    }

    /// Sample rate in Hz (primary rate for combo rates).
    pub fn hz(&self) -> f64 {
        match self {
            SampleRate::S44_1 => 44100.0,
            SampleRate::S48 | SampleRate::S48_96 | SampleRate::S48_192 => 48000.0,
            SampleRate::S88_2 => 88200.0,
            SampleRate::S96 => 96000.0,
            SampleRate::S176_4 => 176400.0,
            SampleRate::S192 => 192000.0,
            SampleRate::Unknown => 48000.0,
        }
    }

    /// Parse from Hz value.
    pub fn from_hz(hz: u32) -> Self {
        match hz {
            44100 => SampleRate::S44_1,
            48000 => SampleRate::S48,
            88200 => SampleRate::S88_2,
            96000 => SampleRate::S96,
            176400 => SampleRate::S176_4,
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
            ColorSpace::Bt470bg => "BT.470BG",
            ColorSpace::Smpte170m => "SMPTE 170M",
            ColorSpace::Unknown => "",
        }
    }

    const ALL_CS: &[(&'static str, ColorSpace)] = &[
        ("bt709", ColorSpace::Bt709),
        ("bt2020", ColorSpace::Bt2020),
        ("bt470bg", ColorSpace::Bt470bg),
        ("smpte170m", ColorSpace::Smpte170m),
        ("unknown", ColorSpace::Unknown),
    ];

    /// Compact identifier for serialization (round-trips via `FromStr`).
    pub fn id(&self) -> &'static str {
        for (id, v) in Self::ALL_CS {
            if v == self {
                return id;
            }
        }
        "unknown"
    }
}

impl std::fmt::Display for ColorSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for ColorSpace {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, ()> {
        for (id, v) in ColorSpace::ALL_CS {
            if *id == s {
                return Ok(*v);
            }
        }
        // Also accept display names (e.g. "BT.2020").
        for (_id, v) in ColorSpace::ALL_CS {
            if ColorSpace::name(v) == s {
                return Ok(*v);
            }
        }
        Ok(ColorSpace::Unknown)
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
                // The only variant not in ALL is the Unknown fallback (kept out
                // of ALL so FromStr("unknown") round-trips to it via $default
                // without ALL gaining a duplicate key). Display it visibly as
                // "unknown" rather than an empty string, which produced blank
                // metadata in labels and logs.
                f.write_str("unknown")
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
        ("88.2kHz", SampleRate::S88_2),
        ("96kHz", SampleRate::S96),
        ("176.4kHz", SampleRate::S176_4),
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
        // An unrecognised string is an error, not silently SDR. ("sdr"/"SDR"
        // already matched above.) Callers that want SDR-on-unknown opt in
        // explicitly with `.unwrap_or(HdrFormat::Sdr)` (e.g. mux/meta.rs).
        Err(())
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
    pub key_source: KeyOrigin,
    /// Volume Unique Key (16 bytes). `None` when keys were resolved
    /// via the [`KeyOrigin::KeyDbUnitKeys`] path — that source delivers
    /// pre-decrypted unit keys without a VUK to derive them from.
    pub vuk: Option<[u8; 16]>,
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Read data key for AACS 2.0 bus decryption -- None for AACS 1.0
    pub read_data_key: Option<[u8; 16]>,
    /// Volume ID (16 bytes) -- from SCSI handshake
    pub volume_id: [u8; 16],
    /// Raw `Unit_Key_RO.inf` bytes (encrypted unit keys + CPS map). Stashed at
    /// scan so an external resolver (key-resolver) can derive the unit keys
    /// from a VUK without re-reading the disc. Empty when not captured.
    pub uk_ro: Vec<u8>,
    /// Raw MKB bytes (`MKB_RO.inf`). Stashed at scan so an external resolver can
    /// walk it (device/processing key → media key). Empty when not captured.
    pub mkb: Vec<u8>,
}

/// How AACS keys were resolved. Variants are ordered root-of-trust →
/// per-disc-leaf, matching the resolver's path-try order: the resolver
/// attempts derivation from the strongest input it has first and falls
/// back toward pre-computed per-disc material.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyOrigin {
    /// MKB + device keys → subset-difference tree → VUK
    DeviceKey,
    /// MKB + processing keys → media key → VUK
    ProcessingKey,
    /// Media key + Volume ID from KEYDB → derived VUK
    KeyDbDerived,
    /// VUK found directly in KEYDB by disc hash
    KeyDb,
    /// Pre-decrypted unit keys taken directly from KEYDB by disc hash.
    /// No VUK present in the entry — `AacsState::vuk` is `None`.
    KeyDbUnitKeys,
    /// Unit key supplied directly by the caller (the external Unit Key path).
    /// No keydb, no derivation — `AacsState::vuk` is `None`.
    ExternalUk,
}

impl KeyOrigin {
    pub fn name(&self) -> &'static str {
        match self {
            KeyOrigin::DeviceKey => "MKB + device key",
            KeyOrigin::ProcessingKey => "MKB + processing key",
            KeyOrigin::KeyDbDerived => "KEYDB (derived)",
            KeyOrigin::KeyDb => "KEYDB",
            KeyOrigin::KeyDbUnitKeys => "KEYDB (unit keys)",
            KeyOrigin::ExternalUk => "external UK",
        }
    }
}

// ─── Disc scanning ──────────────────────────────────────────────────────────

/// AACS host credentials for the live-drive authenticated handshake.
///
/// Optional and source-agnostic: an ISO scan has no handshake at all, and a
/// live drive without supplied credentials simply skips cert auth. The
/// caller supplies the host cert(s) from wherever it likes — today the keydb's
/// `host_certs()`, tomorrow a cert file or built-in. Decoupled from the key
/// source: a locked drive needs the cert to unlock even when the decryption key
/// comes from an online service.
#[derive(Default, Clone)]
pub struct DriveCredentials {
    /// Host certificate(s) + private key(s) for the SCSI AACS handshake.
    pub host_certs: Vec<crate::aacs::HostCert>,
}

/// Options for disc scanning.
///
/// libfreemkv is lookup-free — it resolves no keys. The caller resolves a key
/// out-of-band (a key source) and applies it via [`Disc::decrypt_with`]. The
/// only scan input is the optional drive credentials for the live-drive
/// authenticated handshake.
#[derive(Default)]
pub struct ScanOptions {
    /// Host credentials for the live-drive AACS handshake. `None` for ISO
    /// scans, or a live drive where cert auth should be skipped.
    ///
    /// Host certs may ALSO be supplied through [`Self::key_sources`]: the
    /// handshake unifies certs from both, so the app can pass its already-built
    /// keysource layer rather than (or in addition to) pre-extracting certs into
    /// `DriveCredentials`. Either route is keysource-served — certs are never
    /// compiled into the library.
    pub credentials: Option<DriveCredentials>,
    /// The application's key-source layer. The handshake collects host certs
    /// across these (via [`crate::KeySource::host_certs`]) for the OEM/AACS
    /// cert-auth route, unioned with [`Self::credentials`]. Empty by default —
    /// an ISO scan supplies none, and a live-drive caller that pre-extracted
    /// certs into `credentials` may leave it empty too. The library still
    /// resolves NO keys from these at scan time; they are consulted only for
    /// their host certs here (key *resolution* stays out-of-band via
    /// `Disc::decrypt_with`).
    pub key_sources: Vec<Box<dyn crate::KeySource>>,
    /// Optional cooperative-cancellation token. When set, long scan-time
    /// loops (notably the CSS known-plaintext crack, which can scan up to
    /// 50_000 sectors on a live DVD) poll it and bail out cleanly so a
    /// scan-phase watchdog or operator Stop is never stuck behind a hang.
    pub halt: Option<crate::halt::Halt>,
}

/// Quick disc identification — name, format, capacity. No title/stream parsing.
#[derive(Debug)]
pub struct DiscId {
    /// UDF Volume Identifier (always present, e.g. "SAMPLE_FILM")
    pub volume_id: String,
    /// Disc title from META/DL/bdmt_eng.xml (e.g. "Sample Film")
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
        let capacity = Self::read_capacity(session).unwrap_or_else(|e| {
            // A READ CAPACITY failure (transient drive spin-up, SCSI error)
            // must not be silently treated as a 0-sector disc: capacity=0
            // skews the layer heuristic (always 1 layer) and title ordering.
            // Recovery is unchanged (we still proceed with 0), but surface it.
            tracing::warn!(
                target: "freemkv::scan",
                error = %e,
                "READ CAPACITY failed; treating disc capacity as 0 sectors (layer count and title ordering may be wrong)"
            );
            0
        });
        let batch = detect_max_batch_sectors(session.device_path());
        let mut buffered = udf::BufferedSectorReader::new(session, batch);
        let udf_fs = udf::read_filesystem(&mut buffered)?;
        buffered.prefetch(udf_fs.metadata_start(), udf_fs.metadata_sectors());
        Ok((capacity, buffered, udf_fs))
    }

    /// Scan a disc — parse filesystem, playlists, streams, and set up
    /// AACS decryption. This is the main entry point; after `scan()` the
    /// Disc is ready (titles populated with streams, AACS inputs
    /// captured, content readable and decryptable transparently).
    ///
    /// One pipeline, one order:
    ///   1. Read capacity + UDF filesystem
    ///   2. AACS handshake + key resolution
    ///   3. Parse playlists + streams
    ///   4. Apply labels
    ///
    /// The session must be open and unlocked (`Drive::open` handles this).
    /// All disc reads use standard READ(10) via UDF — no vendor SCSI commands.
    pub fn scan(session: &mut Drive, opts: &ScanOptions) -> Result<Self> {
        // AACS handshake (Blu-ray/UHD). Acquires the Volume ID via the
        // cert-based mutual-auth handshake (the OEM route); drive unlock
        // itself runs separately behind the pluggable `Unlocker` seam.
        // AACS is Blu-ray/UHD only. A DVD uses CSS — skip the AACS handshake
        // entirely (the drive already classified the disc as DVD at init), so a
        // DVD never issues AACS OEM-VID / cert SCSI against the drive before the
        // CSS bus-auth runs.
        let (handshake, handshake_error) = if session.disc_is_dvd() {
            (None, None)
        } else {
            tracing::info!(target: "freemkv::scan", "phase: AACS handshake");
            Self::do_handshake(session, opts)
        };
        tracing::info!(target: "freemkv::scan", handshake = handshake.is_some(), "phase: handshake done");

        // Request max read speed — removes riplock on DVD
        // (BD/UHD speed is set by firmware init, but DVD needs explicit SET CD SPEED)
        session.set_speed(0xFFFF);

        // Read UDF filesystem with buffered sector reader
        tracing::info!(target: "freemkv::scan", "phase: reading UDF filesystem");
        let (capacity, mut buffered, udf_fs) = Self::read_udf(session)?;
        tracing::info!(target: "freemkv::scan", capacity, "phase: UDF read");

        // Pre-read all small file sectors (AACS, MPLS, CLPI, META, *.bdmv).
        // Without this, each read_file() triggers individual SCSI commands at 500ms each.
        if let Ok(ranges) = udf_fs.metadata_sector_ranges(&mut buffered) {
            buffered.prefetch_ranges(&ranges);
        }

        tracing::info!(target: "freemkv::scan", "phase: parsing titles/streams");
        let mut disc = Self::scan_with(
            &mut buffered,
            capacity,
            handshake,
            handshake_error,
            opts,
            udf_fs,
        )?;
        tracing::info!(target: "freemkv::scan", titles = disc.titles.len(), format = ?disc.content_format, "phase: titles parsed");

        // CSS key extraction for DVDs (bus auth → disc key → title key).
        // Must be a single auth session — can't call authenticate() separately.
        // Route through the DRM dispatcher: probe a title sector, detect
        // CSS if scrambled, then load via the SCSI auth path.
        // We already know this is a DVD (MPEG-PS program stream), so drive the
        // CSS handshake DIRECTLY off the main title's first content sector. We
        // must NOT first read a scrambled sector to "detect" CSS: a drive that
        // enforces CSS (e.g. the BU40N) rejects an UNauthenticated read of a
        // scrambled sector with sense 05/6F/03 ("read of scrambled sector
        // without authentication"), so a detect-then-auth ordering dead-locks —
        // detection needs the read, the read needs auth, auth needs detection.
        // The handshake is itself the detector: on a non-CSS (unencrypted) DVD
        // the disc-key read fails, `resolve` returns None, and the disc is left
        // in the clear. This block is DVD-only (MPEG-PS); BD/UHD (MPEG-TS) goes
        // through the AACS handshake above and never reaches here.
        if disc.css.is_none()
            && disc.content_format == ContentFormat::MpegPs
            && !disc.titles.is_empty()
        {
            // CSS title keys are per-VTS, and ONLY the scrambled movie content
            // carries a non-zero key. Menu / VMG / logo cells (often the
            // low-LBA first extent) return a ZERO title key over REPORT KEY —
            // accepting that would leave the whole feature un-descrambled
            // (raw scrambled bytes passed through as "clear"). So build
            // candidate LBAs from the MAIN feature (largest title), LARGEST
            // extent first (the movie body is the biggest scrambled chunk),
            // and accept the first auth that yields a NON-ZERO title key. A
            // genuinely unencrypted DVD returns zero for every candidate →
            // disc stays in the clear. This block is DVD-only (MPEG-PS);
            // BD/UHD (MPEG-TS) used the AACS handshake above and never reach here.
            // Main feature = the largest title; its extents, largest (the movie
            // body) first — that's where the scrambled content with a recoverable
            // title key lives.
            let main_extents = match disc
                .titles
                .iter()
                .filter(|t| !t.extents.is_empty())
                .max_by_key(|t| t.extents.iter().map(|e| e.sector_count as u64).sum::<u64>())
            {
                Some(t) => {
                    let mut v = t.extents.clone();
                    v.sort_by(|a, b| b.sector_count.cmp(&a.sector_count));
                    v
                }
                None => Vec::new(),
            };
            tracing::info!(target: "freemkv::scan", extents = main_extents.len(), "phase: CSS — main feature located");
            if let Some(unlock_lba) = main_extents.first().map(|e| e.start_lba) {
                tracing::info!(target: "freemkv::scan", unlock_lba, "phase: CSS — bus-auth unlock");
                // Unlock the drive's CSS read gating. A CSS-enforcing drive (the
                // BU40N) refuses to return scrambled sectors until a CSS bus-auth
                // handshake has run for the title; we run it here purely for that
                // unlock and IGNORE the key it derives (the disc-key crack is
                // unreliable). The real descramble key is recovered from the
                // scrambled movie data itself via the known-plaintext attack — no
                // player keys, no disc-key crack, no REPORT-KEY-derived title key.
                if let Err(e) = crate::css::auth::unlock_css_reads(session, unlock_lba) {
                    tracing::warn!(
                        target: "freemkv::scan",
                        error_code = e.code(),
                        "CSS bus-auth unlock failed; scrambled sectors may be unavailable"
                    );
                }
                // Size the crack's batch reads to THIS drive's per-command max
                // (DVD ≈ 16; the USB bridge may be lower) — an over-large
                // READ(10) fails outright and would scan nothing.
                let crack_batch = detect_max_batch_sectors(session.device_path());
                tracing::info!(target: "freemkv::scan", crack_batch, "phase: CSS — known-plaintext crack");
                let crack_t0 = std::time::Instant::now();
                let crack_result = crate::css::crack_key_outcome(
                    session,
                    &main_extents,
                    crack_batch,
                    opts.halt.as_ref(),
                );
                tracing::info!(
                    target: "freemkv::scan",
                    elapsed_ms = crack_t0.elapsed().as_millis() as u64,
                    outcome = ?crack_result,
                    "phase: CSS — crack done"
                );
                match crack_result {
                    crate::css::CrackOutcome::Cracked(state) => {
                        tracing::debug!(target: "freemkv::disc", "dvd css: title key recovered via known-plaintext crack");
                        disc.css = Some(state);
                        disc.encrypted = true;
                    }
                    crate::css::CrackOutcome::ScrambledUncracked => {
                        // Scrambled sectors WERE seen but no key could be
                        // recovered — the content is encrypted-but-uncrackable.
                        // Record a hard error so callers fail loudly instead of
                        // muxing scrambled MPEG as plaintext garbage at exit 0.
                        tracing::warn!(target: "freemkv::disc", "dvd css: scrambled sectors seen but no title key cracked");
                        disc.encrypted = true;
                        disc.css_error = Some(crate::error::Error::CssKeyMissing);
                    }
                    crate::css::CrackOutcome::Unencrypted => {
                        tracing::debug!(target: "freemkv::disc", "dvd css: no scrambled sector seen (genuinely unencrypted)");
                    }
                }
            }
        }

        tracing::info!(target: "freemkv::scan", css = disc.css.is_some(), "phase: scan complete");
        Ok(disc)
    }

    /// Scan a disc image (ISO or any SectorSource). No SCSI, no handshake.
    /// AACS resolution uses KEYDB VUK lookup only.
    pub fn scan_image(
        reader: &mut dyn SectorSource,
        capacity: u32,
        opts: &ScanOptions,
    ) -> Result<Self> {
        let udf_fs = udf::read_filesystem(reader)?;
        let mut disc = Self::scan_with(reader, capacity, None, None, opts, udf_fs)?;

        // CSS for a raw (still-scrambled) DVD image: recover the title key from
        // the scrambled movie data itself (known-plaintext attack), same as the
        // live-drive path — but with no SCSI auth/unlock (an image is already
        // readable). This lets the CLI mux a RAW CSS ISO, not only a
        // pre-decrypted one. A pre-decrypted image has its scramble flags clear,
        // so `crack_key` finds no crackable sector and the disc stays in the
        // clear. AACS images go through KEYDB VUK lookup, not here.
        if disc.css.is_none()
            && disc.content_format == ContentFormat::MpegPs
            && !disc.titles.is_empty()
        {
            let main_extents = match disc
                .titles
                .iter()
                .filter(|t| !t.extents.is_empty())
                .max_by_key(|t| t.extents.iter().map(|e| e.sector_count as u64).sum::<u64>())
            {
                Some(t) => {
                    let mut v = t.extents.clone();
                    v.sort_by(|a, b| b.sector_count.cmp(&a.sector_count));
                    v
                }
                None => Vec::new(),
            };
            if !main_extents.is_empty() {
                // Image reads aren't drive-batch-limited; use a generous batch.
                match crate::css::crack_key_outcome(reader, &main_extents, 32, None) {
                    crate::css::CrackOutcome::Cracked(state) => {
                        tracing::info!(target: "freemkv::scan", "image css: title key recovered via known-plaintext crack");
                        disc.css = Some(state);
                        disc.encrypted = true;
                    }
                    crate::css::CrackOutcome::ScrambledUncracked => {
                        // Scrambled image data with no recoverable key — a hard
                        // failure, surfaced so the mux path doesn't pass scrambled
                        // MPEG through as plaintext (garbage at exit 0).
                        tracing::warn!(target: "freemkv::scan", "image css: scrambled sectors seen but no title key cracked");
                        disc.encrypted = true;
                        disc.css_error = Some(crate::error::Error::CssKeyMissing);
                    }
                    crate::css::CrackOutcome::Unencrypted => {}
                }
            }
        }

        Ok(disc)
    }

    /// Read a disc's AACS key-input files from a sector source: returns
    /// `(Unit_Key_RO.inf, MKB)` raw bytes. Shared body for
    /// [`Disc::read_aacs_inputs`] (ISO) and
    /// [`Disc::read_aacs_inputs_from_drive`] (live drive).
    ///
    /// Prefers MKB_RO, falls back to MKB_RW, then TRIMS to the real
    /// record length. Both files are allocated to a fixed ~128 MiB and
    /// zero-padded, so reading either ships up to ~124 MiB of nothing —
    /// trim to the record stream so callers send/store a few MB, not
    /// 128 MiB.
    pub(crate) fn read_aacs_inputs_from_reader(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let inf = udf_fs
            .read_file(reader, "/AACS/Unit_Key_RO.inf")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/DUPLICATE/Unit_Key_RO.inf"))
            .map_err(|_| Error::AacsNoKeys)?;
        let mkb = Self::read_mkb_content(reader, udf_fs)?;
        Ok((inf, mkb))
    }

    /// Read the AACS MKB's real record stream — NOT its zero padding.
    ///
    /// `MKB_RO.inf` / `MKB_RW.inf` are allocated to a fixed ~128 MiB and
    /// zero-padded; the actual record stream is a few MiB. We read a bounded
    /// prefix, find the record-stream length via [`crate::aacs::mkb_content_len`]
    /// and return exactly that, growing the prefix if the records run past it.
    /// This avoids reading 100+ MiB of padding on every scan AND avoids the
    /// `read_file` `MAX_FILE_BYTES` cap that (since 0.31.0) rejected the padded
    /// 128 MiB MKB outright — which made `read_aacs_inputs` fail and autorip
    /// report "could not read this disc's key files" without ever contacting
    /// the keyserver.
    fn read_mkb_content(reader: &mut dyn SectorSource, udf_fs: &udf::UdfFs) -> Result<Vec<u8>> {
        const START_BYTES: usize = 16 * 1024 * 1024;
        const MAX_BYTES: usize = 64 * 1024 * 1024;
        let mut want = START_BYTES;
        loop {
            let buf = udf_fs
                .read_file_prefix(reader, "/AACS/MKB_RO.inf", want)
                .or_else(|_| udf_fs.read_file_prefix(reader, "/AACS/MKB_RW.inf", want))
                .map_err(|_| Error::AacsNoKeys)?;
            let n = crate::aacs::mkb_content_len(&buf);
            // `n` strictly inside `buf` => the record walk reached the padding
            // boundary (full content captured). `buf` shorter than `want` =>
            // the whole file is already read. Otherwise the records may run
            // past the prefix — grow and retry, bounded by MAX_BYTES.
            if (n > 0 && n < buf.len()) || buf.len() < want || want >= MAX_BYTES {
                return Ok(crate::aacs::trim_mkb(buf));
            }
            want = (want * 2).min(MAX_BYTES);
        }
    }

    /// Read a disc's AACS key-input files from an ISO image: returns
    /// `(Unit_Key_RO.inf, MKB)` raw bytes. For callers that resolve a Unit Key
    /// out-of-band: obtain the key however you like, then apply it via
    /// [`Disc::decrypt_with`]. libfreemkv never makes a network call.
    pub fn read_aacs_inputs(iso_path: &std::path::Path) -> Result<(Vec<u8>, Vec<u8>)> {
        // Preserve the underlying open error (`Error::IoError`, E5000, carrying
        // the OS errno) instead of collapsing ENOENT/EPERM/etc. into
        // `Error::AacsNoKeys` (E7000). A missing or unreadable ISO is an I/O
        // fault, not a key-resolution failure; callers that dispatch on
        // `.code()` must be able to tell the two apart.
        let mut reader = crate::io::file_sector_source::FileSectorSource::open(iso_path)?;
        let udf_fs = udf::read_filesystem(&mut reader)?;
        Self::read_aacs_inputs_from_reader(&mut reader, &udf_fs)
    }

    /// Same as [`Disc::read_aacs_inputs`] but reads from a live drive. The
    /// out-of-band Unit Key path fetches the disc's key files from the drive,
    /// resolves a key from them however it likes, then applies it via
    /// [`Disc::decrypt_with`]. These files are plaintext UDF metadata — no
    /// AACS handshake or keys are required to read them.
    pub fn read_aacs_inputs_from_drive(drive: &mut Drive) -> Result<(Vec<u8>, Vec<u8>)> {
        let (_, mut reader, udf_fs) = Self::read_udf(drive)?;
        Self::read_aacs_inputs_from_reader(&mut reader, &udf_fs)
    }

    /// Core scan pipeline — works with any SectorSource.
    ///
    /// `handshake_error` is plumbed from `do_handshake` so failures
    /// (cert rejected, raw-read unsupported, VID read failed) are
    /// preserved as `disc.aacs_error` for callers to render. When key
    /// resolution succeeds despite the handshake failure (built-in
    /// keys + disc-hash lookup hit) the error is dropped.
    fn scan_with(
        reader: &mut dyn SectorSource,
        capacity: u32,
        handshake: Option<HandshakeResult>,
        handshake_error: Option<Error>,
        _opts: &ScanOptions,
        udf_fs: udf::UdfFs,
    ) -> Result<Self> {
        let scan_with_t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::scan", phase = "scan_with", "begin");
        // 2. Resolve encryption (AACS, CSS, or none)
        let encrypted =
            udf_fs.find_dir("/AACS").is_some() || udf_fs.find_dir("/BDMV/AACS").is_some();

        let (aacs, aacs_error) = if !encrypted {
            (None, None)
        } else {
            // Lookup-free: capture the disc's AACS inputs (MKB, VID,
            // Unit_Key_RO.inf) but resolve NO key. The caller resolves a Key
            // from a key source and applies it via `Disc::decrypt_with`. The
            // disc reports "encrypted, no keys" until then.
            match Self::resolve_vid_only(&udf_fs, reader, handshake.as_ref()) {
                Ok(state) => (Some(state), None),
                // A handshake failure (no VID) is more actionable than the
                // generic capture error, so surface it when present.
                Err(e) => (None, Some(handshake_error.unwrap_or(e))),
            }
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

        // 6. CSS detection for DVDs.
        //    Detection from a single probe sector would miss
        //    DVDs whose first sector is unscrambled, so the crack path
        //    scans extents internally and bottoms out at None on
        //    unencrypted media.
        // CSS for a live-drive DVD is resolved by the drive-authentication
        // path in `Disc::scan` (which has `&mut Drive`), AFTER this function
        // returns. We deliberately do NOT run the reader-based crack path here:
        // it is non-functional against this crate's descrambler (always returns
        // None — see `css::crack_key`), and on a CSS-protected disc it would scan up
        // to 50,000 scrambled sectors one-by-one, each rejected by the drive
        // with sense 05/6F/03 ("read of scrambled sector without
        // authentication") — roughly an hour of failing reads before the real
        // auth path ever runs. Leave `css` unresolved here.
        let css = None;
        let encrypted = encrypted || css.is_some();

        tracing::info!(
            target: "freemkv::scan",
            phase = "scan_with",
            titles = titles.len(),
            encrypted,
            elapsed_ms = scan_with_t0.elapsed().as_millis() as u64,
            "end"
        );
        let disc = Disc {
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
            // CSS crack runs AFTER scan_with returns (in `scan` / `scan_image`),
            // which set this when they observe scrambled-but-uncracked content.
            css_error: None,
            content_format,
        };

        // Structured scan diagnostic block (--log-level 3). Emits the
        // per-title / per-stream / decision / AACS rows under the
        // `freemkv::diag` target; a no-op unless that target is enabled.
        // (DVD per-cell category rows are emitted earlier from the IFO scan,
        // before the per-cell detail is lowered away.)
        crate::diag::dump_disc(&disc);

        Ok(disc)
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
            // Same length + clip count = the same feature authored as multiple
            // playlists (a full-audio main vs an audio-reduced twin, e.g. Fight
            // Club's 00800 [DTS-HD MA + 13 tracks] vs 00004 [stereo AC-3 only]).
            // Prefer the richer audio so we never rip a stereo-only variant over
            // the lossless-multichannel main feature.
            .then_with(|| Self::audio_richness(b).cmp(&Self::audio_richness(a)))
    }

    /// Audio-richness rank for `canonical_title_order`'s same-length tiebreak.
    /// Higher is better: `(any lossless track, best channel count, audio count)`.
    fn audio_richness(t: &DiscTitle) -> (u8, u8, usize) {
        let mut lossless = 0u8;
        let mut max_ch = 0u8;
        let mut count = 0usize;
        for s in &t.streams {
            if let Stream::Audio(a) = s {
                count += 1;
                if matches!(
                    a.codec,
                    Codec::TrueHd | Codec::DtsHdMa | Codec::DtsHdHr | Codec::Lpcm | Codec::Flac
                ) {
                    lossless = 1;
                }
                let ch = match a.channels {
                    AudioChannels::Surround71 => 8,
                    AudioChannels::Surround61 => 7,
                    AudioChannels::Surround51 => 6,
                    AudioChannels::Surround50 => 5,
                    AudioChannels::Quad => 4,
                    AudioChannels::Stereo21 => 3,
                    AudioChannels::Stereo => 2,
                    AudioChannels::Mono => 1,
                    AudioChannels::Unknown => 0,
                };
                max_ch = max_ch.max(ch);
            }
        }
        (lossless, max_ch, count)
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
        // `last_lba + 1` = sector count. Guard the 0xFFFF_FFFF sentinel
        // (capacity exceeds 32 bits) so it surfaces as an error instead of
        // wrapping to 0 in release — mirrors the public `decode_read_capacity`.
        lba.checked_add(1)
            .ok_or(crate::error::Error::DiscCapacityOverflow)
    }
}

/// A decryption key handed to libfreemkv by the caller.
///
/// libfreemkv is **lookup-free**: it never reads a keydb, never talks to a key
/// server, never searches paths. The application resolves a key from whatever
/// source it likes (a local keydb, an online key service, a mapfile cache) and
/// hands it in here; libfreemkv uses it to decrypt, deriving any remaining
/// AACS-chain steps it can from disc-read inputs (MKB / VID / `Unit_Key_RO.inf`).
///
/// `#[non_exhaustive]`: AACS is a derivation chain
/// (`DK →(MKB)→ MK →(VID)→ VK →(Unit_Key_RO)→ UK`). Each variant is an entry
/// point at one level of that chain; [`Disc::decrypt_with`] derives down from
/// it to the per-CPS unit keys. New levels can be added without breaking
/// callers.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Key {
    /// Device key(s) (AACS DK, positioned). libfreemkv walks the MKB
    /// (subset-difference tree) to find the one that applies → media key →
    /// VUK → unit keys. A source hands in its FULL device-key set, because
    /// choosing which one applies *is* the MKB walk (derivation), and all
    /// derivation lives here — never in a source.
    Device(Vec<crate::aacs::types::DeviceKey>),
    /// Processing key(s) (AACS PK). libfreemkv applies each against the MKB
    /// → media key → VUK → unit keys.
    Processing(Vec<[u8; 16]>),
    /// Media key candidate(s) (Km). A source hands its full pool because an MK
    /// is MKB-scoped (shared across a pressing/MKB family) — picking the one
    /// that applies is `km_verifies` against this disc's MKB, which is
    /// derivation, so it lives here. libfreemkv verifies, then derives the VUK
    /// via the Volume ID and the per-CPS-unit keys.
    Media(Vec<[u8; 16]>),
    /// Volume Unique Key (VK / VUK). libfreemkv decrypts `Unit_Key_RO.inf`
    /// into the per-CPS-unit keys. NOT terminal — the chain continues to the
    /// unit keys.
    Volume([u8; 16]),
    /// Final per-CPS-unit AACS keys (`(cps_unit, 16-byte key)`). A key source
    /// (keydb / key server) resolved these, or they were cached in the mapfile
    /// at sweep; libfreemkv decrypts directly with no further derivation. This
    /// is the terminal level every other variant derives down into.
    Unit(Vec<(u32, [u8; 16])>),
}

/// True if `unit_keys` covers EVERY supplied scrambled content `sample` — the
/// validation gate for [`Disc::decrypt_with`]. Conservative: a sample that is
/// not AACS-scrambled proves nothing, and with no scrambled sample at all there
/// is nothing to disprove against, so it returns `true` (accept).
///
/// It returns `false` when ANY scrambled sample cannot be restored to clear
/// MPEG-TS by ANY unit key in the set. That covers two distinct failure shapes:
///   1. a wholly wrong key (a keydb VK that does not match this disc) — no
///      sample decrypts; and
///   2. a *partially* applicable key set on a multi-CPS-unit disc — the resolved
///      keys cover CPS unit 0 but not CPS unit 1. Accepting on the first sample
///      that decrypts (the old behaviour) would commit such a set, after which
///      CPS-unit-1 sectors pass through as raw encrypted bytes into the ISO/MKV
///      with no error surfaced anywhere. Requiring every scrambled sample to
///      decrypt rejects the incomplete set so the caller falls through to the
///      next candidate (and ultimately surfaces a key error rather than silently
///      writing ciphertext).
///
/// Reuses the ecosystem's single `is_aacs_scrambled` predicate and the full
/// (bus + AACS) unit decrypt, so it agrees with the actual mux decrypt.
fn aligned_unit_keys_validate(
    unit_keys: &[(u32, [u8; 16])],
    read_data_key: Option<&[u8; 16]>,
    samples: &[Vec<u8>],
) -> bool {
    use crate::aacs::decrypt::{ALIGNED_UNIT_LEN, decrypt_unit_full, is_aacs_scrambled};
    let scrambled: Vec<&[u8]> = samples
        .iter()
        .map(|s| s.as_slice())
        .filter(|s| s.len() >= ALIGNED_UNIT_LEN && is_aacs_scrambled(s))
        .collect();
    if scrambled.is_empty() {
        return true; // nothing to disprove against — accept
    }
    if unit_keys.is_empty() {
        return false;
    }
    let mut probe = vec![0u8; ALIGNED_UNIT_LEN];
    let total = (scrambled.len() as u64) * (unit_keys.len() as u64);
    let mut tried = 0u64;
    let mut hb = crate::progress::Heartbeat::new("scan_key_trial");
    // Every scrambled sample must be covered by SOME unit key. A single sample
    // that no key descrambles means the key set is incomplete (wrong key, or a
    // CPS unit left uncovered) — reject so the wrong/partial set never commits.
    for sample in scrambled {
        let mut covered = false;
        for (_, k) in unit_keys {
            // Pure-CPU inner loop: only consult the clock every 256 trials.
            hb.tick_cpu(tried, total);
            tried += 1;
            probe.copy_from_slice(&sample[..ALIGNED_UNIT_LEN]);
            if decrypt_unit_full(&mut probe, k, read_data_key) {
                covered = true;
                break;
            }
        }
        if !covered {
            return false;
        }
    }
    true
}

impl Disc {
    /// Get the resolved decryption keys for this disc.
    /// Used by disc-to-ISO and other full-disc operations.
    pub fn decrypt_keys(&self) -> crate::decrypt::DecryptKeys {
        if let Some(ref aacs) = self.aacs {
            // An AACS state with NO unit keys is "encrypted, no keys" — e.g.
            // the VID-only state from out-of-band resolution before a Unit Key
            // is supplied. Report None so callers treat it as missing keys
            // (not a usable, empty key set).
            if aacs.unit_keys.is_empty() {
                return crate::decrypt::DecryptKeys::None;
            }
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

    /// The 40-hex AACS disc id (SHA1 of `Unit_Key_RO.inf`, no `0x` prefix), or
    /// empty when this disc has no captured AACS state. Used to name the disc in
    /// a [`Error::NoDiscKey`] so the application can tell the user which disc to
    /// add to the keydb.
    pub fn aacs_disc_hash(&self) -> String {
        self.aacs
            .as_ref()
            .map(|a| a.disc_hash.trim_start_matches("0x").to_string())
            .unwrap_or_default()
    }

    /// The system-wide decrypt correctness gate.
    ///
    /// Returns `Ok(())` when it is safe to proceed with a copy or mux, and a
    /// clear typed error when decryption is **needed but unavailable** — the
    /// case that would otherwise write ciphertext (disc→ISO) or feed the demux
    /// undecryptable bytes (mux) and exit 0. Every copy/mux entry point calls
    /// this **after key resolution and before any source-data processing
    /// begins**, so the verdict is identical everywhere and the failure is a
    /// pre-flight one (no partial output).
    ///
    /// The verdict, in order:
    /// - `raw == true` → `Ok(())`. `--raw` intentionally skips decryption and
    ///   needs no key (the caller wants an encrypted image).
    /// - `self.css_error.is_some()` → `Err(Error::CssKeyMissing)`. The scan saw
    ///   scrambled CSS sectors but recovered no title key (`self.css` is `None`
    ///   yet the content IS encrypted). Treating `css.is_none()` as
    ///   "unencrypted" would mux scrambled MPEG as plaintext garbage.
    /// - AACS-encrypted (`self.aacs.is_some()`) with no usable key
    ///   (`decrypt_keys()` is `None`) → `Err(Error::NoDiscKey { .. })`, naming
    ///   the disc by hash.
    /// - CSS-encrypted (`self.css.is_some()`) with no usable key →
    ///   `Err(Error::CssKeyMissing)`. (The disc-wide `decrypt_keys()` yields
    ///   `Css{..}` whenever `css.is_some()`, so this is defensive; the live
    ///   multi-VTS case is gated by [`Self::ensure_decryptable_keys`].)
    /// - otherwise → `Ok(())`. A genuinely unencrypted disc has `None` keys
    ///   legitimately, and a CSS disc whose keyless crack succeeded has a key.
    pub fn ensure_decryptable(&self, raw: bool) -> Result<()> {
        self.ensure_decryptable_keys(raw, &self.decrypt_keys())
    }

    /// [`Self::ensure_decryptable`] against a caller-resolved key set, for the
    /// per-title path. A multi-VTS CSS DVD resolves its key with
    /// [`Self::decrypt_keys_for_title`] (which can return `None` when the chosen
    /// title's VTS could not be re-cracked even though the disc-wide
    /// `decrypt_keys()` is `Css{..}`); the gate must judge THAT key, not the
    /// disc-wide one. The "is the source encrypted?" question is answered by the
    /// scan-captured disc state (`css_error`/`aacs`/`css`), never by the keys —
    /// so an unencrypted disc (no AACS/CSS state) never false-errors regardless
    /// of `keys`.
    pub fn ensure_decryptable_keys(
        &self,
        raw: bool,
        keys: &crate::decrypt::DecryptKeys,
    ) -> Result<()> {
        // --raw skips decryption entirely: never error, even on an encrypted
        // disc with no key (the user asked for the encrypted image).
        if raw {
            return Ok(());
        }
        // Scrambled-but-uncracked CSS: the disc is encrypted but `css` is None,
        // so the key check below can't see it. Surface the recorded hard error.
        if self.css_error.is_some() {
            return Err(Error::CssKeyMissing);
        }
        // Decryption is needed iff the disc carries cipher state. A no-key
        // verdict on a non-encrypted disc is impossible here (the disc has no
        // AACS/CSS state), so a genuinely unencrypted disc never errors.
        let needs_key = matches!(keys, crate::decrypt::DecryptKeys::None);
        if needs_key {
            if self.aacs.is_some() {
                // E7017 vs E7022 split: when key resolution had derivation
                // material (device / processing keys) but no Volume ID to derive
                // the unit key, the captured `aacs_error` is `AacsVidUnavailable`
                // — report THAT (the fix is recovering the VID, not adding keys),
                // not the generic `NoDiscKey`. Any other (or absent) reason →
                // `NoDiscKey` naming the disc by hash, unchanged.
                if matches!(self.aacs_error, Some(Error::AacsVidUnavailable)) {
                    return Err(Error::AacsVidUnavailable);
                }
                return Err(Error::NoDiscKey {
                    disc_hash: self.aacs_disc_hash(),
                });
            }
            if self.css.is_some() {
                return Err(Error::CssKeyMissing);
            }
        }
        Ok(())
    }

    /// Resolve decryption keys for muxing a *specific* title.
    ///
    /// CSS title keys are per-VTS. The scan cracks one key (from the main
    /// feature, title 0 for autorip). Applying it to a title that lives in
    /// a *different* VTS would silently descramble with the wrong key
    /// (garbage output). When the requested title's extents don't overlap
    /// the span the cracked key came from, re-crack the key from this
    /// title's own extents using `reader`. AACS / unencrypted / single-VTS
    /// paths are identical to [`Self::decrypt_keys`].
    ///
    /// `batch_sectors` sizes the crack's batched reads (file-safe value for
    /// an ISO; `detect_max_batch_sectors` for a live drive).
    pub fn decrypt_keys_for_title(
        &self,
        idx: usize,
        reader: &mut dyn SectorSource,
        batch_sectors: u16,
    ) -> crate::decrypt::DecryptKeys {
        self.decrypt_keys_for_title_checked(idx, reader, batch_sectors)
            .0
    }

    /// [`Self::decrypt_keys_for_title`] plus the per-title encryption verdict the
    /// gate needs to AVOID A FALSE ERROR on a genuinely-clear extra title.
    ///
    /// On a multi-VTS CSS DVD that ALSO carries a clear, unencrypted stub title
    /// (a 0.5 s menu loop, an FBI-warning nav title) living in its own VTS, the
    /// re-crack over that stub's extents finds NO scrambled sector and recovers
    /// no key. The bare `decrypt_keys_for_title` collapses that to
    /// `DecryptKeys::None`, indistinguishable from "scrambled but uncrackable",
    /// so [`Self::ensure_decryptable_keys`] (which fails whenever `css.is_some()`
    /// and the key is `None`) wrongly raised `E7023` for a title that needs no
    /// key at all. That is the false error the multi-title mux must never emit.
    ///
    /// This variant runs the re-crack via [`crate::css::crack_key_outcome`] and
    /// returns `title_is_clear == true` when the title's own extents showed NO
    /// scrambling (`CrackOutcome::Unencrypted`) — the gate then treats that title
    /// as needing no key and passes it cleanly. A title that genuinely IS
    /// scrambled but uncrackable returns `(None, false)` and still hard-fails.
    /// The returned bool pairs with [`Self::ensure_title_decryptable`].
    pub fn decrypt_keys_for_title_checked(
        &self,
        idx: usize,
        reader: &mut dyn SectorSource,
        batch_sectors: u16,
    ) -> (crate::decrypt::DecryptKeys, bool) {
        let css = match self.css {
            Some(ref c) => c,
            None => return (self.decrypt_keys(), false),
        };
        let title = match self.titles.get(idx) {
            Some(t) if !t.extents.is_empty() => t,
            // No extents to crack from — fall back to the disc-wide key.
            _ => return (self.decrypt_keys(), false),
        };
        // If the title overlaps the span the existing key was cracked from,
        // it's the same VTS — the cracked key applies. `crack_span: None`
        // (unknown provenance) is also treated as "applies".
        let overlaps = match css.crack_span {
            None => true,
            Some((cs, ce)) => title.extents.iter().any(|e| {
                let ts = e.start_lba;
                let te = e.start_lba.saturating_add(e.sector_count);
                ts < ce && cs < te
            }),
        };
        if overlaps {
            return (self.decrypt_keys(), false);
        }
        // Different VTS: re-crack from this title's extents, largest first
        // (the movie body is the biggest scrambled chunk — same heuristic
        // the scan uses). The disc-wide key provably does NOT apply here
        // (crack_span is Some and this title doesn't overlap it). Use
        // `crack_key_outcome` (not the bare `crack_key`) so we can tell a
        // genuinely-clear title (`Unencrypted` — no scrambled sector in its
        // own extents) apart from a scrambled-but-uncrackable one:
        //   - Cracked            → the title's own key.
        //   - Unencrypted        → (None, title_is_clear=true): this extra title
        //                          needs no key; the gate must NOT raise E7023.
        //   - ScrambledUncracked → (None, false): genuinely encrypted but no key
        //                          → a real hard failure, still surfaced.
        // The disc-wide fallback is reserved for the unknown-provenance case
        // (crack_span == None), already handled above via overlaps == true.
        let mut extents = title.extents.clone();
        extents.sort_by(|a, b| b.sector_count.cmp(&a.sector_count));
        match crate::css::crack_key_outcome(reader, &extents, batch_sectors, None) {
            crate::css::CrackOutcome::Cracked(state) => (
                crate::decrypt::DecryptKeys::Css {
                    title_key: state.title_key,
                },
                false,
            ),
            // No scrambled sector in THIS title's extents: it is genuinely clear.
            // Signal `title_is_clear` so the per-title gate passes it without a
            // key — NO FALSE E7023 for an unencrypted extra title.
            crate::css::CrackOutcome::Unencrypted => (crate::decrypt::DecryptKeys::None, true),
            // Scrambled sectors seen but no key recovered: a genuine hard failure.
            crate::css::CrackOutcome::ScrambledUncracked => {
                (crate::decrypt::DecryptKeys::None, false)
            }
        }
    }

    /// Per-title decrypt gate that honours the `title_is_clear` verdict from
    /// [`Self::decrypt_keys_for_title_checked`].
    ///
    /// Identical to [`Self::ensure_decryptable_keys`] EXCEPT it does not raise
    /// `E7023` when the chosen title proved genuinely clear (`title_is_clear`):
    /// a multi-VTS CSS disc can carry an unencrypted stub title in its own VTS,
    /// and that title needs no key. The disc-wide `css.is_some()` is true, so the
    /// plain gate would false-error; this one passes the clear title through.
    /// A scrambled-but-uncrackable title (`title_is_clear == false`, key `None`)
    /// still hard-fails exactly as before.
    pub fn ensure_title_decryptable(
        &self,
        raw: bool,
        keys: &crate::decrypt::DecryptKeys,
        title_is_clear: bool,
    ) -> Result<()> {
        if raw {
            return Ok(());
        }
        // A title proven clear by its own re-crack (no scrambled sector in its
        // extents) needs no key even though the disc is CSS — pass it. The
        // disc-wide `css_error` is deliberately NOT consulted here: it reflects
        // the MAIN feature's crack, not this clear extra title.
        if title_is_clear && !keys.is_encrypted() {
            return Ok(());
        }
        self.ensure_decryptable_keys(raw, keys)
    }

    /// Inject pre-resolved AACS unit keys into a scanned disc — the deferred-mux
    /// / resume path. The keys come from the mapfile's `# freemkv-uk:` header
    /// (persisted at sweep time when the disc was keyed), so the mux decrypts
    /// directly with NO key-service round-trip. Populates `self.aacs.unit_keys`
    /// so [`decrypt_keys`] returns them and marks the source `ExternalUk`.
    ///
    /// If the scan built no AACS state (`self.aacs == None`) — which happens
    /// when the keydb was absent at scan time (`scan_aacs_no_keydb` →
    /// `aacs_error = KeydbLoad`) — this synthesizes a minimal `ExternalUk`
    /// state for an encrypted AACS disc. A Unit Key is the FINAL per-title
    /// decryption key; the keydb is only needed to *derive* it, and that
    /// derivation already happened at sweep (the UK is in the mapfile). So a UK
    /// alone is sufficient to decrypt the on-disk ISO — AACS 2.0 bus decryption
    /// was applied by the drive at read time, so `read_data_key` is unused for
    /// file-backed mux. Without this, a keyed disc swept without a keydb would
    /// recover its UK yet still report E8005 (no usable `decrypt_keys`) at
    /// remux. No-op for an unencrypted or CSS (DVD) disc.
    pub fn inject_unit_keys(&mut self, keys: Vec<(u32, [u8; 16])>) {
        if let Some(aacs) = self.aacs.as_mut() {
            aacs.unit_keys = keys;
            aacs.key_source = KeyOrigin::ExternalUk;
        } else if self.encrypted && self.css.is_none() {
            self.aacs = Some(AacsState {
                version: if self.format == DiscFormat::Uhd { 2 } else { 1 },
                bus_encryption: self.format == DiscFormat::Uhd,
                mkb_version: None,
                disc_hash: String::new(),
                key_source: KeyOrigin::ExternalUk,
                vuk: None,
                unit_keys: keys,
                read_data_key: None,
                volume_id: [0u8; 16],
                uk_ro: Vec::new(),
                mkb: Vec::new(),
            });
            // The prior resolution error (e.g. KeydbLoad) is now moot — we have
            // the decryption key. Clear it so callers don't treat the disc as
            // keyless on the stale error.
            self.aacs_error = None;
        }
    }

    /// The public AACS inputs for this disc, for a [`crate::KeySource`] to look
    /// a key up. `None` when the disc carries no AACS state (unencrypted, CSS,
    /// or AACS inputs not captured at scan). Contains no secrets — just disc
    /// identity plus the on-disc AACS structures.
    pub fn inputs(&self) -> Option<crate::keysource::DiscInputs> {
        self.aacs.as_ref().map(|a| crate::keysource::DiscInputs {
            disc_hash: a.disc_hash.clone(),
            volume_id: a.volume_id,
            mkb: a.mkb.clone(),
            unit_key_ro: a.uk_ro.clone(),
            // Content samples need the disc reader, which scan does not retain;
            // the caller fills these for sources that validate against ciphertext.
            samples: Vec::new(),
            // Human title: prefer the UDF/ISO volume identifier, fall back to the
            // BDMV display name. Identity only — a key service may catalog it.
            volume_label: {
                let v = self.volume_id.trim();
                if v.is_empty() {
                    self.meta_title.clone()
                } else {
                    Some(v.to_string())
                }
            },
        })
    }

    /// Apply a caller-resolved [`Key`] so [`Self::decrypt_keys`] yields usable
    /// decryption state. **Lookup-free**: no keydb, no network — the caller
    /// (an application, via a key source) does all resolution and hands the key
    /// in here. For [`Key::Unit`] this is the deferred-mux / resume path: the
    /// unit keys came from a key source or the mapfile cache, and libfreemkv
    /// decrypts directly (see [`Self::inject_unit_keys`]).
    /// `samples` are encrypted on-disc aligned units (each 6144 bytes), supplied
    /// by the caller for content validation. A wrong key — a keydb VK that does
    /// not match this disc, a stale UK — can still *derive* a non-empty (garbage)
    /// unit-key set, so before the key touches disc state we confirm it actually
    /// de-scrambles real ciphertext. This is the single home of key validation:
    /// every caller loops a key source's candidates through `decrypt_with` and a
    /// rejected key (`Err(AacsKeyRejected)`) transparently falls through to the
    /// next. Pass `&[]` when no content sample is available (resume / mapfile
    /// cache) — validation is then skipped and the key is applied as-is.
    pub fn decrypt_with(&mut self, key: Key, samples: &[Vec<u8>]) -> Result<()> {
        // The AACS 2.x bus key, needed to de-scramble a sample for validation;
        // captured before any mutable borrow. None for AACS 1.0 and file-backed
        // ISO units (bus encryption was already removed at read time).
        let read_data_key = self.aacs.as_ref().and_then(|a| a.read_data_key);

        // Resolve the supplied key DOWN to candidate unit keys WITHOUT
        // committing them, so a wrong higher-level key can be rejected before it
        // poisons disc state.
        let (candidate_unit_keys, candidate_vuk) = if let Key::Unit(keys) = key {
            // Terminal — the source / mapfile already holds the final UKs.
            (keys, None)
        } else {
            // Every higher level derives DOWN to the unit keys, reusing the
            // version-dispatched resolver — the single home for all AACS
            // derivation (1.0 / 2.0 / 2.1 / 2.x). It needs the AACS inputs
            // (Unit_Key_RO.inf, MKB, VID) stashed on the disc at scan time.
            let aacs = self.aacs.as_ref().ok_or(crate::error::Error::AacsNoKeys)?;
            if aacs.uk_ro.is_empty() {
                // Scan captured no Unit_Key_RO.inf — nothing to derive into.
                return Err(crate::error::Error::AacsNoKeys);
            }

            // Map the supplied Key -> raw material. The source handed in material
            // at exactly one level; choosing/applying it is the resolver's job.
            let mut supplied = crate::aacs::provider::SuppliedKey {
                device_keys: Vec::new(),
                processing_keys: Vec::new(),
                media_keys: Vec::new(),
                disc_entry: None,
            };
            match key {
                Key::Device(dks) => supplied.device_keys = dks,
                Key::Processing(pks) => supplied.processing_keys = pks,
                Key::Media(mks) => supplied.media_keys = mks,
                Key::Volume(vuk) => {
                    supplied.disc_entry = Some(crate::aacs::DiscEntry {
                        disc_hash: aacs.disc_hash.clone(),
                        title: String::new(),
                        media_key: None,
                        disc_id: None,
                        vuk: Some(vuk),
                        unit_keys: Vec::new(),
                    });
                }
                Key::Unit(_) => unreachable!("Key::Unit handled above"),
            }

            // Snapshot inputs (releases the &self borrow before the &mut below).
            let volume_id = aacs.volume_id;
            let mkb = aacs.mkb.clone();
            let uk_ro = aacs.uk_ro.clone();
            let version_u8 = aacs.version;

            let provider_refs: [&dyn crate::aacs::KeyProvider; 1] = [&supplied];
            let ctx = crate::aacs::ResolveContext {
                unit_key_ro: &uk_ro,
                content_cert: None,
                volume_id: &volume_id,
                providers: &provider_refs,
                mkb: if mkb.is_empty() { None } else { Some(&mkb) },
            };

            // Version dispatch — V10 uses the classical resolver at 48-byte
            // stride; V20/V21 share the 64-byte stride, so try the classical V20
            // paths first and fall back to the 2.1 variant chain. The
            // reason-preserving wrapper threads the no-key cause out so the
            // decrypt gate can report E7017 (had derivation material but no VID)
            // vs E7022 (no usable material) instead of a flat AacsKeyRejected.
            let resolved = crate::aacs::resolve_keys_with_reason(&ctx, version_u8)
                .map_err(|_reason| crate::error::Error::AacsKeyRejected)?;

            if resolved.unit_keys.is_empty() {
                return Err(crate::error::Error::AacsKeyRejected);
            }
            (resolved.unit_keys, resolved.vuk)
        };

        // VALIDATE against real ciphertext. Conservative: reject only when a
        // supplied sample is AACS-scrambled and NO candidate unit key can
        // de-scramble it. With no samples (or only clear ones) there is nothing
        // to disprove against, so the key is accepted as-is — keeping the
        // sample-less paths (resume / mapfile cache) byte-for-byte unchanged.
        if !aligned_unit_keys_validate(&candidate_unit_keys, read_data_key.as_ref(), samples) {
            return Err(crate::error::Error::AacsKeyRejected);
        }

        // Commit — only now does the key touch disc state.
        match self.aacs.as_mut() {
            Some(a) => {
                a.unit_keys = candidate_unit_keys;
                if candidate_vuk.is_some() {
                    a.vuk = candidate_vuk;
                }
                a.key_source = KeyOrigin::ExternalUk;
            }
            // A Unit key for an AACS disc whose scan built no state (keyless
            // scan, no keydb): synthesize a minimal ExternalUk state.
            None => self.inject_unit_keys(candidate_unit_keys),
        }
        // A prior scan-time resolution error (e.g. keyless scan) is now moot.
        self.aacs_error = None;
        Ok(())
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
        reader: &mut dyn SectorSource,
        path: &std::path::Path,
        opts: &CopyOptions,
    ) -> Result<CopyResult> {
        // Pre-flight decrypt gate. A decrypting copy (`opts.decrypt == true`,
        // i.e. NOT `--raw`) of an encrypted disc with no usable key would wrap
        // the reader in a pass-through `DecryptingSectorSource` and write
        // ciphertext to the ISO, then return `Ok` (bytes_good > 0) — a silent
        // garbage success at exit 0. Refuse here, BEFORE any sweep/patch reads a
        // single sector, so the failure is pre-flight and no partial ISO is
        // written. `opts.decrypt == false` is `--raw`: the gate is a no-op (the
        // user wants the encrypted image), and an unencrypted disc passes too.
        self.ensure_decryptable(!opts.decrypt)?;
        // Mapfile-driven resume dispatch. This runs for BOTH plain and
        // `--multipass` copies: an interrupted plain `disc:// → iso://` writes
        // a per-block-flushed mapfile (crash-safe), and re-issuing the SAME
        // command must pick up where it stopped rather than re-sweep from
        // sector 0 (the help/CLI examples promise "auto-resumes if
        // interrupted"). The ONLY multipass-specific behaviour is the patch
        // (Pass N) dispatch on retryable bytes — plain mode has no patch pass,
        // so it returns a terminal result there instead.
        let mf_path = self.mapfile_for(path);
        if mf_path.exists() {
            let map = mapfile::Mapfile::load(&mf_path).map_err(|e| Error::IoError { source: e })?;
            let stats = map.stats();
            let disc_size = self.capacity_bytes;
            let covers_disc = map.total_size() == disc_size;
            let bad_bytes = stats.bytes_pending + stats.bytes_unreadable;
            tracing::info!(
                "copy dispatch: disc={} map={} covers={} multipass={} good={} nontried={} pending={} unreadable={}",
                disc_size,
                map.total_size(),
                covers_disc,
                opts.multipass,
                stats.bytes_good,
                stats.bytes_nontried,
                stats.bytes_pending,
                stats.bytes_unreadable,
            );
            if covers_disc && bad_bytes == 0 && stats.bytes_nontried == 0 {
                // Every sector is Finished — a prior copy completed. Re-issuing
                // the command is a no-op (don't re-sweep a finished ISO).
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
                // Mapfile capacity != disc capacity. Force a full (non-
                // resume) sweep on ANY mismatch so [0, disc_size) is covered
                // as one fresh region (the non-resume path also set_len's the
                // ISO to the full capacity).
                //
                // UNDER-cover (map.total_size() < disc_size): a resume sweep
                // builds its region list only from the mapfile's NonTried
                // entries and would silently never read the tail
                // [map.total_size(), disc_size) — abandoning readable data
                // and the ISO's tail.
                //
                // OVER-cover (map.total_size() > disc_size): a resume sweep's
                // NonTried regions extend past the disc; `reader.read_sectors`
                // would then read LBAs beyond capacity (the promised
                // capacity clamp was never actually applied). A fresh sweep
                // sized to the real disc avoids reading past the end.
                tracing::info!(
                    "copy dispatch: → sweep (covers_disc=false, resume=false, map={}, disc={})",
                    map.total_size(),
                    disc_size,
                );
                return self.sweep_internal(reader, path, opts, false);
            }
            // NonTried bytes mean a prior sweep was halted mid-way (Ctrl-C /
            // crash) and the mapfile still has un-attempted ranges (the un-swept
            // tail). The sweep pass's job is to read those — route to a resume
            // sweep FIRST, even when retryable bytes also exist. Checking
            // retryable before this (and routing straight to patch) would
            // silently abandon the un-swept tail: patch only revisits the
            // mapfile's bad ranges, never the NonTried ones. The retry
            // (patch) passes run after, driven separately by the caller's
            // pass loop, and pick up the retryable bytes the sweep leaves.
            // This is the plain-copy resume path too: a clean disc interrupted
            // by Ctrl-C leaves exactly this state (NonTried tail), so a re-run
            // resumes the sweep instead of restarting from sector 0.
            if stats.bytes_nontried > 0 {
                tracing::info!(
                    "copy dispatch: → sweep resume (covers_disc=true, \
                     nontried={}, retryable={})",
                    stats.bytes_nontried,
                    stats.bytes_retryable,
                );
                return self.sweep_internal(reader, path, opts, true);
            }
            // From here covers_disc=true and nontried=0: the whole disc was
            // attempted. Only the retry/patch decision differs by mode.
            if opts.multipass {
                if stats.bytes_retryable > 0 {
                    tracing::info!(
                        "copy dispatch: → patch (retryable={})",
                        stats.bytes_retryable,
                    );
                    return self.patch_internal(reader, path, opts);
                }
                // Fallthrough: covers_disc=true, nontried=0, retryable=0.
                // All sectors were attempted; any remaining bad bytes are
                // already Unreadable. A resume sweep would visit zero new
                // sectors and patch has nothing retryable — return the
                // terminal result immediately.
                tracing::info!(
                    "copy dispatch: all bad sectors already Unreadable \
                     (retryable=0, nontried=0) — returning terminal result",
                );
                return Ok(CopyResult {
                    bytes_total: disc_size,
                    bytes_good: stats.bytes_good,
                    bytes_unreadable: stats.bytes_unreadable,
                    bytes_pending: 0,
                    recovered_this_pass: 0,
                    complete: false,
                    halted: false,
                });
            }
            // Plain (non-multipass) copy: there is no patch pass and the sweep
            // aborts on the first read error, so a fully-attempted mapfile with
            // bad bytes is terminal. Re-running must NOT restart from sector 0
            // (that re-reads the whole disc and re-hits the same bad sector);
            // return the terminal result so the caller surfaces the failure.
            // (`complete` is true only when no bad bytes remain.)
            tracing::info!(
                "copy dispatch: plain copy, disc fully attempted (bad={}) — terminal result",
                bad_bytes,
            );
            return Ok(CopyResult {
                bytes_total: disc_size,
                bytes_good: stats.bytes_good,
                bytes_unreadable: stats.bytes_unreadable,
                bytes_pending: stats.bytes_pending,
                recovered_this_pass: 0,
                complete: bad_bytes == 0,
                halted: false,
            });
        }
        self.sweep_internal(reader, path, opts, false)
    }

    fn sweep_internal(
        &self,
        reader: &mut dyn SectorSource,
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
            vid: opts.vid,
            unit_keys: opts.unit_keys.clone(),
        };
        self.sweep(reader, path, &sweep_opts)
    }

    fn patch_internal(
        &self,
        reader: &mut dyn SectorSource,
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
    /// This is one of the two flat verbs the library exposes
    /// for rip orchestration. Multipass + retry decisions are the
    /// caller's job — see [`PatchOptions`] for the retry primitive.
    pub fn sweep(
        &self,
        reader: &mut dyn SectorSource,
        path: &std::path::Path,
        opts: &SweepOptions,
    ) -> Result<CopyResult> {
        use crate::io::{DEFAULT_PIPELINE_DEPTH, Pipeline};
        use crate::sector::{DecryptingSectorSource, SectorSource};
        use sweep::{ProgressSnapshot, SweepSink, WorkItem, try_recv_progress};

        // Pre-flight decrypt gate (also enforced in `copy`; re-checked here so a
        // direct `sweep` caller can't bypass it). A decrypting sweep of an
        // encrypted disc with no usable key would write ciphertext to the ISO at
        // exit 0; refuse before reading any sector. No-op for `--raw`
        // (`opts.decrypt == false`) and unencrypted discs.
        self.ensure_decryptable(!opts.decrypt)?;

        let total_bytes = self.capacity_sectors as u64 * 2048;
        let keys = if opts.decrypt {
            self.decrypt_keys()
        } else {
            crate::decrypt::DecryptKeys::None
        };
        // Captured before `keys` moves into the decorator below. A decrypting
        // AACS-keyed sweep needs unit-aligned (3-sector) batch sizing + region
        // read-starts (see the batch computation further down).
        let decrypt_is_aacs = matches!(keys, crate::decrypt::DecryptKeys::Aacs { .. });

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
        // covers_disc reconciliation. A resume against a mapfile whose total
        // size != the real disc size is unsafe — exactly the case copy()'s
        // dispatch forces to a fresh sweep (see Disc::copy). Under-cover
        // (map < disc) abandons the disc tail [map.total_size(), disc);
        // over-cover (map > disc) reads LBAs past capacity. When sweep() is
        // called directly (not via copy()), apply the same downgrade: drop the
        // stale mapfile and sweep [0, total_bytes) fresh.
        let mut resume = opts.resume;
        if resume && mapfile_path.exists() {
            match mapfile::Mapfile::load(&mapfile_path) {
                Ok(existing) => {
                    if existing.total_size() != total_bytes {
                        tracing::info!(
                            "sweep: mapfile total_size {} != disc {}; forcing fresh sweep",
                            existing.total_size(),
                            total_bytes,
                        );
                        resume = false;
                    } else {
                        // Inconsistent-resume guard. The mapfile claims prior
                        // progress (some range past NonTried) but the ISO is
                        // missing or zero-length — the ISO was deleted or
                        // truncated while the mapfile survived (reachable via
                        // autorip ResumeMode::Require). The producer only builds
                        // work from NonTried ranges, so any Finished range would
                        // never be re-read and would stay ZERO in the fresh ISO,
                        // silently holed. Downgrade to a fresh full sweep (mirror
                        // the total_size-mismatch case) so the rip self-heals.
                        let iso_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                        let claims_progress =
                            existing.stats().bytes_pending != existing.total_size();
                        if iso_len == 0 && claims_progress {
                            tracing::info!(
                                "sweep: mapfile claims prior progress (pending {} of {}) but ISO is missing/zero-length; forcing fresh sweep",
                                existing.stats().bytes_pending,
                                existing.total_size(),
                            );
                            resume = false;
                        }
                    }
                }
                Err(_) => {
                    // The mapfile exists but is corrupt / unparseable. Proceeding
                    // with resume=true would hand a garbage (or empty) mapfile to
                    // open_or_create and silently skip already-Finished ranges or
                    // mis-track progress. Downgrade to a fresh sweep — consistent
                    // with the total_size-mismatch branch above — so the `!resume`
                    // path below drops the corrupt mapfile and the rip restarts
                    // clean.
                    tracing::info!(
                        "sweep: mapfile at {} is corrupt/unparseable; forcing fresh sweep",
                        mapfile_path.display(),
                    );
                    resume = false;
                }
            }
        }
        if !resume {
            // A fresh sweep MUST start from an empty mapfile. If the stale file
            // can't be removed, open_or_create would load it and the new disc
            // would inherit the old Finished ranges → silently zero-filled ISO.
            // ENOENT is fine (nothing to remove); any other error aborts.
            match std::fs::remove_file(&mapfile_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::IoError { source: e }),
            }
        }
        let mut map = mapfile::Mapfile::open_or_create(
            &mapfile_path,
            total_bytes,
            concat!("libfreemkv v", env!("CARGO_PKG_VERSION")),
        )
        .map_err(|e| Error::IoError { source: e })?;

        // Persist the disc's decryption state into the mapfile header so it
        // survives to deferred-mux / resume. ddrescue-safe (comment lines);
        // does not touch the ISO payload. KEYS XOR VID: a keyed disc writes its
        // unit keys (the final answer — deferred-mux decrypts directly, no key
        // service); an unresolved disc writes only the VID (the retry marker).
        if !opts.unit_keys.is_empty() {
            map.set_unit_keys(&opts.unit_keys);
        } else if let Some(vid) = opts.vid {
            map.set_vid(vid);
        }

        // ISO file: if resuming and mapfile has Finished ranges, open existing;
        // otherwise create fresh and pre-size to total_bytes (sparse holes for
        // non-tried regions).
        let is_regular = std::fs::metadata(path)
            .map(|m| m.file_type().is_file())
            .unwrap_or(false);
        let file = if resume
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
        let mut batch: u16 = match opts.batch_sectors {
            Some(b) => b,
            None if opts.skip_on_error => ecc_sectors(self.format),
            None => DEFAULT_BATCH_SECTORS_OPTICAL,
        };

        // AACS unit alignment for a DECRYPTING sweep. AACS aligned units are 3
        // sectors (6144 bytes); `decrypt_sectors` anchors units at buffer offset
        // 0, so every read handed to the decrypting reader MUST start on a unit
        // boundary AND span a whole number of units — otherwise units straddle
        // batch/region boundaries and decrypt under the wrong CBC/unit alignment
        // (the verify-gate then leaves content encrypted or aborts DecryptFailed).
        //
        // ecc_sectors() is 32 for UHD/BD, which is NOT a multiple of 3, so the
        // default batch would start every batch-after-the-first mid-unit. Round
        // the batch UP to the next multiple of 3 (32 → 33) when this sweep both
        // decrypts and is AACS-keyed. Region read-starts are aligned DOWN to a
        // unit boundary in the loop below; a fresh sweep starts at LBA 0 (already
        // aligned), so alignment only bites on resume NonTried regions.
        const UNIT_SECTORS: u16 = (crate::aacs::ALIGNED_UNIT_LEN / 2048) as u16; // 3
        if decrypt_is_aacs && batch % UNIT_SECTORS != 0 {
            batch = batch.saturating_add(UNIT_SECTORS - (batch % UNIT_SECTORS));
        }

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

        // Translate `Pipeline::send` failure (consumer gone) into a
        // numeric library error so the producer-error semantics are
        // unchanged but no English leaks into an io::Error.
        fn consumer_gone() -> Error {
            Error::PipelineConsumerGone
        }

        let mut buf = vec![0u8; batch as usize * 2048];
        let mut bytes_done = 0u64;
        let mut halt_requested = false;
        let copy_t0 = std::time::Instant::now();
        tracing::info!(
            target: "freemkv::scan",
            phase = "sweep",
            total_bytes,
            skip_on_error = opts.skip_on_error,
            resume,
            "begin"
        );
        let mut iter_count: u64 = 0;
        let mut read_ok_count: u64 = 0;
        let mut read_err_count: u64 = 0;
        let mut last_log_iter: u64 = 0;
        // Sweep heartbeat: fire every 5s OR every 100 iterations, whichever
        // comes first, so a slow-but-alive sweep on a marginal disc keeps
        // emitting "no silent hang" liveness even between the 100-iter marks.
        let mut last_log_time = std::time::Instant::now();
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

        // Request the drive's max read speed for the whole sweep — removes
        // riplock. BD/UHD get their speed from the firmware unlock/init, but a
        // DVD skips that path (the stock-mode gate, `Drive::disc_is_dvd`), so
        // without this explicit SET CD SPEED a DVD rip sweeps at the drive's
        // default (riplocked) speed. The damage-recovery branch below also
        // re-asserts max speed after slowing on bad sectors; this sets it once
        // up front so a clean disc never pays the riplock penalty.
        reader.set_speed(0xFFFF);

        'outer: for (region_pos, region_size) in regions {
            let region_end = region_pos + region_size;
            // AACS unit alignment: anchor the region's read cursor DOWN to the
            // nearest 6144-byte unit boundary so the decrypting reader never gets
            // a buffer that starts mid-unit. Re-reading the few already-covered
            // head sectors is idempotent (they re-decrypt identically and the
            // consumer overwrites the same ISO offsets / mapfile ranges). A fresh
            // sweep's NonTried region starts at 0, already unit-aligned; this only
            // shifts resume regions that begin mid-unit.
            let mut pos = if decrypt_is_aacs {
                let unit_bytes = crate::aacs::ALIGNED_UNIT_LEN as u64;
                region_pos - (region_pos % unit_bytes)
            } else {
                region_pos
            };
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
                        // bridge_degradation_count is reset inside on_success()
                        // (called above); no separate reset needed here.

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
                                            let inner_action = read_error::handle_read_error(
                                                &inner_err,
                                                &mut read_ctx,
                                            );
                                            match inner_action {
                                                read_error::ReadAction::Retry { pause_secs } => {
                                                    // Transient (NOT_READY / bridge
                                                    // degradation): honour the
                                                    // cooldown pause, then mark
                                                    // BisectBad and move on. We
                                                    // are already inside a
                                                    // single-sector retry; a
                                                    // second bisect would be
                                                    // nonsensical (ctx.bisecting
                                                    // is true, so handle_read_error
                                                    // can't return Bisect).
                                                    sleep_secs_or_halt(
                                                        pause_secs,
                                                        opts.halt.as_ref(),
                                                    );
                                                }
                                                read_error::ReadAction::AbortPass => {
                                                    // Transport failure or
                                                    // wedge-abort threshold
                                                    // reached: stop immediately.
                                                    let (status, sense) =
                                                        extract_scsi_context(&inner_err);
                                                    producer_err = Some(Error::DiscRead {
                                                        sector: sector_lba as u64,
                                                        status: Some(status),
                                                        sense,
                                                    });
                                                    bisect_aborted = true;
                                                    break;
                                                }
                                                // JumpAhead / SkipBlock: honour
                                                // any indicated pause; the
                                                // bisect-inner loop's job is just
                                                // to classify this specific sector,
                                                // so we still mark BisectBad and
                                                // continue to the next sector.
                                                read_error::ReadAction::JumpAhead {
                                                    pause_secs,
                                                    ..
                                                }
                                                | read_error::ReadAction::SkipBlock {
                                                    pause_secs,
                                                } => {
                                                    sleep_secs_or_halt(
                                                        pause_secs,
                                                        opts.halt.as_ref(),
                                                    );
                                                }
                                                // Bisect cannot recurse: ctx.bisecting
                                                // is true so handle_read_error will
                                                // never return Bisect here.
                                                read_error::ReadAction::Bisect => {}
                                            }
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

                let time_due = last_log_time.elapsed() >= std::time::Duration::from_secs(5);
                if iter_count - last_log_iter >= 100 || time_due {
                    last_log_iter = iter_count;
                    last_log_time = std::time::Instant::now();
                    // Promoted trace -> debug ("no silent hangs"): the sweep
                    // heartbeat must be visible at the standard debug level, not
                    // only the trace firehose. Carries lba/pos/region_end and
                    // bytes_good when a consumer snapshot is available.
                    let lba = (pos / 2048) as u32;
                    if let Some(ref snap) = cached_snapshot {
                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "iter_progress",
                            iter_count,
                            read_ok_count,
                            read_err_count,
                            lba,
                            pos,
                            region_end,
                            bytes_good = snap.stats.bytes_good,
                            bytes_pending = snap.stats.bytes_pending,
                            copy_elapsed_ms = copy_t0.elapsed().as_millis() as u64,
                            "Disc::sweep inner iter"
                        );
                    } else {
                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "iter_progress",
                            iter_count,
                            read_ok_count,
                            read_err_count,
                            lba,
                            pos,
                            region_end,
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
                    let (bytes_good, bytes_unreadable, bytes_pending, bytes_retryable) =
                        match &cached_snapshot {
                            Some(snap) => (
                                snap.stats.bytes_good.max(bytes_done),
                                snap.stats.bytes_unreadable,
                                snap.stats.bytes_pending,
                                snap.stats.bytes_retryable,
                            ),
                            None => (
                                bytes_done,
                                0u64,
                                total_bytes.saturating_sub(bytes_done),
                                0u64,
                            ),
                        };
                    let pp = crate::progress::PassProgress {
                        kind: crate::progress::PassKind::Sweep,
                        work_done: pos,
                        work_total: total_bytes,
                        bytes_good_total: bytes_good,
                        bytes_unreadable_total: bytes_unreadable,
                        bytes_pending_total: bytes_pending,
                        bytes_retryable_total: bytes_retryable,
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
    /// AACS Volume ID (16 bytes) to persist into the mapfile during
    /// Pass 1 so it survives to deferred-mux / resume. `None` for
    /// unencrypted / non-AACS discs. Caller wires this from
    /// `Disc::aacs.volume_id`.
    ///
    /// Persisted ONLY when `unit_keys` is empty (the disc didn't resolve a
    /// key): the VID is the "still unresolved, retry-able" marker.
    pub vid: Option<[u8; 16]>,
    /// Resolved AACS unit keys `(CPS unit, key)` to persist into the mapfile
    /// during Pass 1. When non-empty these are written (the final answer, so
    /// deferred-mux/resume decrypts directly) and the VID is NOT — keys XOR VID.
    /// Caller wires this from `Disc::aacs.unit_keys`.
    pub unit_keys: Vec<(u32, [u8; 16])>,
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
    /// AACS Volume ID (16 bytes) persisted into the mapfile when the
    /// sweep creates / opens it. `None` for unencrypted discs. Written ONLY
    /// when `unit_keys` is empty (keys XOR VID — the VID is the retry marker).
    pub vid: Option<[u8; 16]>,
    /// Resolved AACS unit keys persisted into the mapfile when the sweep
    /// creates / opens it. When non-empty these win over `vid`.
    pub unit_keys: Vec<(u32, [u8; 16])>,
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

/// Mapfile path for a regular output file: appends `.mapfile` to the
/// output path. For `/dev/null` (benchmark) output use
/// [`Disc::mapfile_for`], which special-cases it to a temp-dir path
/// derived from the disc title.
pub fn mapfile_path_for(iso_path: &std::path::Path) -> std::path::PathBuf {
    let mut s = iso_path.as_os_str().to_os_string();
    s.push(".mapfile");
    std::path::PathBuf::from(s)
}

impl Disc {
    /// Path to the mapfile for a given output path.
    ///
    /// For `/dev/null` output, returns
    /// `{temp_dir}/{volume_id_or_title}.mapfile` (temp dir is
    /// `TMPDIR`-aware and cross-platform). For regular files, returns
    /// `{path}.mapfile`.
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
            std::env::temp_dir().join(format!("{name}.mapfile"))
        } else {
            mapfile_path_for(path)
        }
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

/// Whether the Linux-sysfs transfer-size probe applies to this device path.
///
/// The probe reads `/sys/block/<name>/...` / `/sys/class/scsi_generic/<name>/...`,
/// which only exist on Linux and only for `/`-delimited node paths. A Windows
/// `\\.\CdRom0` / `\\.\D:` path has no forward slash and no sysfs node, so the
/// probe cannot run and the caller must fall back to the optical default.
fn sysfs_batch_probe_supported(device_path: &str) -> bool {
    cfg!(target_os = "linux") && device_path.contains('/')
}

/// Detect the maximum transfer size in sectors for a device.
pub fn detect_max_batch_sectors(device_path: &str) -> u16 {
    // The sysfs probe below is Linux-only. Non-sysfs platforms (Windows in
    // particular) use `\\.\`-form device paths (e.g. `\\.\CdRom0`, `\\.\D:`)
    // that have no forward slash, so the Linux name-parsing below would treat
    // the whole path as the device name, find no `/sys` node, and fall through
    // to the block default (8192 sectors = 16 MiB) — far over the optical cap.
    // Every device we open on a non-sysfs platform here is an optical drive,
    // so return the optical default directly.
    if !sysfs_batch_probe_supported(device_path) {
        return DEFAULT_BATCH_SECTORS_OPTICAL;
    }

    let dev_name = device_path.rsplit('/').next().unwrap_or("");
    if dev_name.is_empty() {
        return DEFAULT_BATCH_SECTORS_OPTICAL;
    }

    // Check whether THIS device (not any device on the host) is an
    // optical drive: read the SCSI peripheral type of the target node
    // only. Type 0x05 (decimal 5) = CD/DVD. A previous version scanned
    // every /sys/class/scsi_device entry and returned true if any was
    // optical, misclassifying a block device as optical on a host that
    // also has an optical drive.
    let is_optical = {
        // For an sg node the type lives at scsi_generic/<sg>/device/type;
        // for a block node (sr0/sdX) at /sys/block/<name>/device/type.
        let type_path = if dev_name.starts_with("sg") {
            format!("/sys/class/scsi_generic/{dev_name}/device/type")
        } else {
            format!("/sys/block/{dev_name}/device/type")
        };
        std::fs::read_to_string(&type_path)
            .ok()
            .map(|c| c.trim().parse::<u32>() == Ok(5))
            .unwrap_or(false)
    };

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
                    let sectors = (kb / 2).min(u16::MAX as u32) as u16;
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

    /// A Windows-form optical device path (`\\.\CdRom0`, `\\.\D:`) must never
    /// fall through to the block default (8192 sectors = 16 MiB, well over the
    /// optical 510-sector cap). It has no forward slash, so the Linux-sysfs
    /// name parse cannot apply; the detector must return the optical default.
    #[test]
    fn windows_device_path_uses_optical_default() {
        for path in ["\\\\.\\CdRom0", "\\\\.\\CdRom15", "\\\\.\\D:", "\\\\.\\E:"] {
            let batch = detect_max_batch_sectors(path);
            assert_eq!(
                batch, DEFAULT_BATCH_SECTORS_OPTICAL,
                "windows path {path:?} must map to the optical default, got {batch}"
            );
            assert!(
                batch <= MAX_BATCH_SECTORS,
                "windows path {path:?} batch {batch} exceeds optical cap {MAX_BATCH_SECTORS}"
            );
        }
    }

    /// `read_aacs_inputs` on a missing/unreadable ISO must surface the real
    /// I/O fault (`E_IO_ERROR`, 5000) carrying the OS errno — NOT `AacsNoKeys`
    /// (7000). Collapsing ENOENT into a key error makes callers that dispatch
    /// on `.code()` tell the user "no keys / check your KEYDB" when the actual
    /// problem is that the ISO file does not exist.
    #[test]
    fn read_aacs_inputs_missing_iso_is_io_error_not_no_keys() {
        let missing = std::path::Path::new("/nonexistent/freemkv/does-not-exist.iso");
        let err = Disc::read_aacs_inputs(missing).expect_err("opening a nonexistent ISO must fail");
        assert_eq!(
            err.code(),
            crate::error::E_IO_ERROR,
            "missing ISO must map to E_IO_ERROR (5000), got {} ({err:?})",
            err.code()
        );
        assert_ne!(
            err.code(),
            crate::error::E_AACS_NO_KEYS,
            "missing ISO must not be reported as AacsNoKeys (7000)"
        );
    }

    /// The sysfs probe only applies on Linux and only to `/`-delimited node
    /// paths. A backslash-form path is never sysfs-probeable on any platform.
    #[test]
    fn windows_path_not_sysfs_probeable() {
        assert!(!sysfs_batch_probe_supported("\\\\.\\CdRom0"));
        assert!(!sysfs_batch_probe_supported("\\\\.\\D:"));
    }

    /// AACS unit-alignment of the DECRYPTING multipass sweep. AACS aligned units
    /// are 3 sectors (6144 bytes); `decrypt_sectors` anchors units at buffer
    /// offset 0, so the sweep MUST (a) round its per-batch sector count UP to a
    /// multiple of 3 and (b) align each NonTried region's read cursor DOWN to a
    /// unit boundary — otherwise batches after the first start mid-unit and every
    /// unit decrypts under the wrong CBC/unit alignment.
    ///
    /// This mirrors the exact arithmetic the sweep loop uses (the full path needs
    /// a live AACS `Disc`, out of reach in a unit test). The decorator-level
    /// reject for an unaligned start LBA is covered end-to-end in
    /// `sector::decrypting::tests::aacs_unaligned_start_lba_rejected`.
    #[test]
    fn aacs_sweep_batch_and_region_are_unit_aligned() {
        const UNIT_SECTORS: u16 = (crate::aacs::ALIGNED_UNIT_LEN / 2048) as u16; // 3
        let unit_bytes = crate::aacs::ALIGNED_UNIT_LEN as u64; // 6144

        // (a) Batch rounding: ecc_sectors() for UHD/BD is 32, not a multiple of 3.
        // The decrypting-AACS path rounds it up to the next multiple of 3 (33).
        for format in [DiscFormat::Uhd, DiscFormat::BluRay] {
            let mut batch = ecc_sectors(format);
            assert_eq!(batch, 32);
            if batch % UNIT_SECTORS != 0 {
                batch = batch.saturating_add(UNIT_SECTORS - (batch % UNIT_SECTORS));
            }
            assert_eq!(batch, 33, "batch must round 32 -> 33 (a multiple of 3)");
            assert_eq!(batch % UNIT_SECTORS, 0);
            // Every full batch read is then a whole number of 6144-byte units.
            assert_eq!((batch as u64 * 2048) % unit_bytes, 0);
        }

        // (b) Region-start down-alignment. A resume NonTried region can begin
        // mid-unit; aligning the read cursor DOWN to the nearest unit boundary
        // makes block_lba % 3 == 0 for the first (and thus every) batch read.
        // Re-reading the few head sectors is idempotent.
        for region_pos in [0u64, 2048, 4096, 6144, 8192, 65536, 67_584] {
            let pos = region_pos - (region_pos % unit_bytes);
            assert_eq!(pos % unit_bytes, 0, "aligned cursor must be unit-aligned");
            assert!(pos <= region_pos, "alignment only moves the cursor down");
            // block_lba derived as pos/2048 must be a multiple of 3 sectors.
            assert_eq!((pos / 2048) % UNIT_SECTORS as u64, 0);
        }
        // An already-aligned region (fresh sweep starts at 0) is unchanged.
        assert_eq!(0u64 - (0u64 % unit_bytes), 0);
        assert_eq!(6144u64 - (6144u64 % unit_bytes), 6144);
    }

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
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
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

    /// Contract pin (owner-flagged): `freemkv -t 1` ALWAYS selects the main
    /// feature. The CLI's `-t 1` maps to `titles[0]`, and the title list is
    /// ordered by `canonical_title_order` (main feature first), so `titles[0]`
    /// IS the movie. Anything but the main feature at index 0 is a
    /// title-ordering bug, not a remux problem. DVD-shaped fixture (DVD-9
    /// capacity; a 1h49m main feature alongside a menu loop and a short extra).
    #[test]
    fn title_index_0_is_main_feature_dvd_the_dash_t_1_contract() {
        const DVD9: u64 = 7_900_000_000; // dual-layer DVD
        let mut titles = vec![
            title_with("VTS_01_menu", 120.0, 200_000_000, 1), // 2m menu/setup loop
            title_with("VTS_02_main", 6540.0, 6_300_000_000, 1), // 1h49m main feature
            title_with("VTS_03_extra", 900.0, 800_000_000, 1), // 15m extra
        ];
        titles.sort_by(|a, b| Disc::canonical_title_order(a, b, DVD9));
        assert_eq!(
            titles[0].playlist, "VTS_02_main",
            "titles[0] (== what `freemkv -t 1` selects) must be the DVD main feature"
        );
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
            css_error: None,
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

    impl crate::sector::SectorSource for MockReader {
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

        fn capacity_sectors(&self) -> u32 {
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
            css_error: None,
            content_format: ContentFormat::BdTs,
        }
    }

    #[test]
    fn inject_unit_keys_synthesizes_aacs_state_when_scan_built_none() {
        // Regression (E8005 deferred-mux loop): a keyed AACS disc swept WITHOUT a
        // keydb scans to aacs=None + aacs_error=KeydbLoad, but its UK is persisted
        // in the mapfile. At remux the UK is recovered and injected — that MUST
        // yield usable decrypt keys. Before the fix, inject_unit_keys no-op'd
        // (no aacs to mutate), decrypt_keys stayed None, and the mux deferred
        // forever with "No keys available (E8005)" despite holding the UK.
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs_error = Some(crate::error::Error::KeydbLoad {
            path: "<no keydb in search paths>".into(),
        });
        assert!(
            matches!(disc.decrypt_keys(), crate::decrypt::DecryptKeys::None),
            "precondition: encrypted disc with no aacs state => no decrypt keys"
        );

        let uk = vec![(0u32, [0x11u8; 16])];
        disc.inject_unit_keys(uk.clone());

        match disc.decrypt_keys() {
            crate::decrypt::DecryptKeys::Aacs {
                unit_keys,
                read_data_key,
            } => {
                assert_eq!(unit_keys, uk, "injected UK must be the decrypt key");
                assert_eq!(read_data_key, None, "ISO mux needs no bus key");
            }
            _ => panic!("expected Aacs decrypt keys after injecting a UK"),
        }
        assert!(
            disc.aacs_error.is_none(),
            "stale KeydbLoad must be cleared once a UK is in hand"
        );
        assert_eq!(
            disc.aacs.as_ref().unwrap().key_source,
            KeyOrigin::ExternalUk
        );
    }

    /// Build an AacsState carrying the given unit keys (other fields are inert
    /// defaults — these tests only exercise the unit-key/decrypt-keys plumbing).
    fn aacs_with(unit_keys: Vec<(u32, [u8; 16])>) -> AacsState {
        AacsState {
            version: 2,
            bus_encryption: true,
            mkb_version: None,
            disc_hash: String::new(),
            key_source: KeyOrigin::DeviceKey,
            vuk: None,
            unit_keys,
            read_data_key: None,
            volume_id: [0u8; 16],
            uk_ro: Vec::new(),
            mkb: Vec::new(),
        }
    }

    // ── ensure_decryptable: the system-wide decrypt verdict matrix ──────────
    //
    // This is the single gate every copy/mux entry point calls. The cases below
    // are the full truth table: only "decryption needed AND unavailable AND not
    // --raw" may error; every legit non-error case (raw / unencrypted / a
    // resolved key) must proceed.

    fn css_state() -> crate::css::CssState {
        crate::css::CssState {
            title_key: [0u8; 5],
            crack_span: None,
        }
    }

    /// AACS-encrypted disc, decryption requested, no unit key resolved → the
    /// gate must fail with NoDiscKey (this is the headline bug: a pass-through
    /// `DecryptingSectorSource` would otherwise write ciphertext at exit 0).
    #[test]
    fn ensure_decryptable_aacs_no_key_errors() {
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(Vec::new())); // present but no unit keys → None
        assert!(matches!(
            disc.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));
        let err = disc
            .ensure_decryptable(false)
            .expect_err("AACS disc, no key, !raw must error");
        assert_eq!(
            err.code(),
            crate::error::Error::NoDiscKey {
                disc_hash: String::new()
            }
            .code()
        );
    }

    /// E7017 vs E7022 split (rc.6 WS1). When key resolution HAD derivation
    /// material (device / processing keys) but no Volume ID was available to
    /// derive the unit key, the captured `aacs_error` is `AacsVidUnavailable`
    /// — the gate must surface THAT (E7017), not the generic `NoDiscKey`
    /// (E7022). When there was no usable key material at all, the reason is
    /// absent and the gate keeps `NoDiscKey` (E7022). Both branches proven here.
    #[test]
    fn ensure_decryptable_aacs_vid_unavailable_vs_no_key() {
        // Branch 1 — derivation material present, but no VID: E7017.
        // The resolver classifies a device-keys-but-zero-VID context as
        // `VidUnavailable`; that reason rides on `aacs_error`.
        let supplied = crate::aacs::provider::SuppliedKey {
            device_keys: vec![crate::aacs::DeviceKey {
                key: [0x11; 16],
                node: 1,
                uv: 1,
                u_mask_shift: 0,
            }],
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: None,
        };
        let provider_refs: [&dyn crate::aacs::KeyProvider; 1] = [&supplied];
        // A minimal but parseable Unit_Key_RO.inf (uk_pos=32, zero unit keys)
        // so resolution proceeds to the path-try logic and fails for lack of a
        // VID — not because the .inf failed to parse.
        let mut uk_ro = vec![0u8; 40];
        uk_ro[0..4].copy_from_slice(&32u32.to_be_bytes()); // uk_pos = 32
        // num_unit_keys = 0 (BE16) at uk_pos -> parses to an empty key file.
        let ctx = crate::aacs::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16], // the "no VID" sentinel
            providers: &provider_refs,
            mkb: None,
        };
        assert_eq!(
            crate::aacs::resolve_keys_with_reason(&ctx, 2).err(),
            Some(crate::aacs::ResolveFailure::VidUnavailable),
            "device keys + zero VID must classify as VidUnavailable"
        );

        let mut disc_e7017 = make_test_disc(1000, "UHD");
        disc_e7017.encrypted = true;
        disc_e7017.aacs = Some(aacs_with(Vec::new())); // present but no unit keys
        disc_e7017.aacs_error = Some(crate::error::Error::AacsVidUnavailable);
        let err = disc_e7017
            .ensure_decryptable(false)
            .expect_err("AACS disc, material-but-no-VID, !raw must error");
        assert_eq!(
            err.code(),
            crate::error::Error::AacsVidUnavailable.code(),
            "material-but-no-VID must surface E7017 (AacsVidUnavailable), not E7022"
        );

        // Branch 2 — no key material at all: classified NoMaterial, gate E7022.
        let supplied_none = crate::aacs::provider::SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: None,
        };
        let provider_refs_none: [&dyn crate::aacs::KeyProvider; 1] = [&supplied_none];
        let ctx_none = crate::aacs::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers: &provider_refs_none,
            mkb: None,
        };
        assert_eq!(
            crate::aacs::resolve_keys_with_reason(&ctx_none, 2).err(),
            Some(crate::aacs::ResolveFailure::NoMaterial),
            "no key material must classify as NoMaterial"
        );

        let mut disc_e7022 = make_test_disc(1000, "UHD");
        disc_e7022.encrypted = true;
        disc_e7022.aacs = Some(aacs_with(Vec::new()));
        disc_e7022.aacs_error = None; // no reason captured → generic no-key
        let err = disc_e7022
            .ensure_decryptable(false)
            .expect_err("AACS disc, no material, !raw must error");
        assert_eq!(
            err.code(),
            crate::error::Error::NoDiscKey {
                disc_hash: String::new()
            }
            .code(),
            "no-material must keep E7022 (NoDiscKey)"
        );
    }

    /// Same AACS-no-key disc under `--raw` (raw=true) must PROCEED — the user
    /// asked for the encrypted image and needs no key.
    #[test]
    fn ensure_decryptable_aacs_no_key_raw_proceeds() {
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(Vec::new()));
        assert!(disc.ensure_decryptable(true).is_ok(), "--raw must proceed");
    }

    /// AACS disc WITH a resolved unit key → proceed (decrypt_keys is Aacs).
    #[test]
    fn ensure_decryptable_aacs_with_key_proceeds() {
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(vec![(0, [0x11u8; 16])]));
        assert!(disc.ensure_decryptable(false).is_ok());
    }

    /// A genuinely unencrypted disc has `None` keys legitimately — the gate must
    /// NOT false-error. This is the "is the source encrypted?" guard: the answer
    /// is the scan-captured disc state, not the keys.
    #[test]
    fn ensure_decryptable_unencrypted_proceeds() {
        let disc = make_test_disc(1000, "BD"); // aacs/css/css_error all None
        assert!(matches!(
            disc.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));
        assert!(
            disc.ensure_decryptable(false).is_ok(),
            "unencrypted disc with None keys must proceed, not false-error"
        );
    }

    /// CSS scrambled-but-uncracked (the keyless crack failed): `css` is None but
    /// `css_error` is Some — the disc IS encrypted. The gate must fail with
    /// CssKeyMissing rather than read `css.is_none()` as "unencrypted".
    #[test]
    fn ensure_decryptable_css_error_errors() {
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css_error = Some(crate::error::Error::CssKeyMissing);
        let err = disc
            .ensure_decryptable(false)
            .expect_err("scrambled-but-uncracked CSS must error");
        assert_eq!(err.code(), crate::error::Error::CssKeyMissing.code());
        // --raw is exempt.
        assert!(disc.ensure_decryptable(true).is_ok());
    }

    /// CSS-keyless-crack SUCCESS: `css` is Some with a title key → proceed.
    #[test]
    fn ensure_decryptable_css_with_key_proceeds() {
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css = Some(css_state());
        assert!(disc.ensure_decryptable(false).is_ok());
    }

    /// Per-title gate: a multi-VTS CSS disc whose chosen title's VTS could not
    /// be re-cracked yields `DecryptKeys::None` even though the disc-wide
    /// `decrypt_keys()` is `Css{..}`. `ensure_decryptable_keys` judges the
    /// per-title key and must fail with CssKeyMissing.
    #[test]
    fn ensure_decryptable_keys_css_per_title_none_errors() {
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css = Some(css_state());
        let err = disc
            .ensure_decryptable_keys(false, &crate::decrypt::DecryptKeys::None)
            .expect_err("CSS disc, per-title key None, !raw must error");
        assert_eq!(err.code(), crate::error::Error::CssKeyMissing.code());
        // The same None key under --raw proceeds.
        assert!(
            disc.ensure_decryptable_keys(true, &crate::decrypt::DecryptKeys::None)
                .is_ok()
        );
    }

    /// `ensure_decryptable_keys` must never false-error an UNENCRYPTED disc no
    /// matter the key argument (the verdict keys off disc state, not keys).
    #[test]
    fn ensure_decryptable_keys_unencrypted_never_errors() {
        let disc = make_test_disc(1000, "BD");
        assert!(
            disc.ensure_decryptable_keys(false, &crate::decrypt::DecryptKeys::None)
                .is_ok()
        );
    }

    // ── Fix 2/3: a genuinely-clear extra title on a CSS disc never E7023s ──────

    /// Reader that serves clear (unscrambled) sectors for one extent range and
    /// CSS-locked errors elsewhere — enough to drive `decrypt_keys_for_title_
    /// checked`'s per-title re-crack to `Unencrypted` for a clear stub.
    struct ClearStubReader {
        clear_range: (u32, u32),
    }
    impl crate::sector::SectorSource for ClearStubReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let n = count as usize * 2048;
            buf[..n].fill(0); // clear sectors: scramble flag never set
            let _ = self.clear_range;
            Ok(n)
        }
        fn capacity_sectors(&self) -> u32 {
            self.clear_range.1
        }
    }

    /// Build a multi-VTS CSS disc: `css` cracked from the main feature's span
    /// `[main_lba, main_end)`, plus a clear stub title living in a DISJOINT VTS.
    fn css_disc_with_clear_stub() -> (Disc, usize) {
        let mut disc = make_test_disc(100_000, "DVD");
        disc.encrypted = true;
        disc.css = Some(crate::css::CssState {
            title_key: [0u8; 5],
            crack_span: Some((0, 1000)), // main feature VTS span
        });
        // Title 0: the main feature, overlaps the cracked span.
        let mut feature = title_with_video(Codec::Mpeg2, Resolution::R480i);
        feature.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1000,
        }];
        // Title 1: a tiny CLEAR stub in its own VTS, disjoint from the span.
        let mut stub = title_with_video(Codec::Mpeg2, Resolution::R480i);
        stub.extents = vec![Extent {
            start_lba: 50_000,
            sector_count: 7, // a 7-sector menu stub
        }];
        disc.titles = vec![feature, stub];
        (disc, 1) // stub is title index 1
    }

    /// THE Fix 2/3 regression: on a multi-VTS CSS DVD, a genuinely-clear extra
    /// title (an unencrypted menu stub in its own VTS) must resolve to
    /// `title_is_clear = true` with `None` keys, and `ensure_title_decryptable`
    /// must PASS it — no false E7023. The old `decrypt_keys_for_title` +
    /// `ensure_decryptable_keys` pair raised CssKeyMissing here because the
    /// re-crack of the clear stub returned `None`, indistinguishable from a
    /// scrambled-uncracked title.
    #[test]
    fn clear_stub_title_on_css_disc_is_not_a_key_failure() {
        let (disc, stub_idx) = css_disc_with_clear_stub();
        let mut reader = ClearStubReader {
            clear_range: (0, 100_000),
        };
        let (keys, title_is_clear) = disc.decrypt_keys_for_title_checked(stub_idx, &mut reader, 8);
        assert!(
            !keys.is_encrypted(),
            "a clear stub needs no key (got encrypted keys)"
        );
        assert!(
            title_is_clear,
            "the stub's own extents show no scrambling → title_is_clear must be true"
        );
        // The gate must PASS the clear stub — NO false E7023.
        assert!(
            disc.ensure_title_decryptable(false, &keys, title_is_clear)
                .is_ok(),
            "a genuinely clear extra title must never raise E7023"
        );
    }

    /// Counterpart guard: a scrambled-but-uncrackable title (`title_is_clear ==
    /// false`, `None` keys) on a CSS disc must STILL hard-fail with CssKeyMissing.
    /// Fix 2/3 must not weaken the genuine encrypted-but-uncrackable case.
    #[test]
    fn scrambled_uncracked_title_still_hard_fails() {
        let (disc, _) = css_disc_with_clear_stub();
        let err = disc
            .ensure_title_decryptable(false, &crate::decrypt::DecryptKeys::None, false)
            .expect_err("scrambled-uncracked title (title_is_clear=false) must error");
        assert_eq!(err.code(), crate::error::Error::CssKeyMissing.code());
        // --raw is exempt even for a scrambled-uncracked title.
        assert!(
            disc.ensure_title_decryptable(true, &crate::decrypt::DecryptKeys::None, false)
                .is_ok()
        );
    }

    #[test]
    fn decrypt_keys_none_when_aacs_present_but_unit_keys_empty() {
        // VID-only state (resolved but no Unit Key yet) must read as None, not
        // an empty-but-usable key set — callers treat it as "keys missing".
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(Vec::new()));
        assert!(matches!(
            disc.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));
    }

    #[test]
    fn decrypt_with_replaces_existing_aacs_unit_keys_and_marks_external() {
        // When scan DID build an AACS state, decrypt_with must overwrite its
        // unit keys (not append) and mark the source ExternalUk.
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(vec![(0, [0x01; 16])]));
        let new = vec![(0u32, [0x77u8; 16]), (1, [0x88; 16])];
        disc.decrypt_with(Key::Unit(new.clone()), &[]).unwrap();
        match disc.decrypt_keys() {
            crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => {
                assert_eq!(unit_keys, new, "must replace, preserving every CPS unit");
            }
            _ => panic!("expected Aacs decrypt keys"),
        }
        assert_eq!(
            disc.aacs.as_ref().unwrap().key_source,
            KeyOrigin::ExternalUk
        );
    }

    /// Build a minimal valid `Unit_Key_RO.inf` carrying the given encrypted
    /// unit keys at the V20 (64-byte) stride. Header is inert (no titles); only
    /// the key-storage area matters for `parse_unit_key_ro`.
    fn uk_ro_v20(enc_keys: &[[u8; 16]]) -> Vec<u8> {
        let uk_pos = 32usize;
        let keys_start = uk_pos + 48;
        let stride = 64usize;
        let mut data = vec![0u8; keys_start + enc_keys.len().max(1) * stride];
        data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        data[uk_pos..uk_pos + 2].copy_from_slice(&(enc_keys.len() as u16).to_be_bytes());
        for (i, k) in enc_keys.iter().enumerate() {
            let off = keys_start + i * stride;
            data[off..off + 16].copy_from_slice(k);
        }
        data
    }

    #[test]
    fn decrypt_with_volume_derives_per_cps_unit_keys() {
        // A Volume key (VUK) is NOT terminal — the lib must decrypt
        // Unit_Key_RO.inf into ONE unit key per CPS unit. Oracle = the lib's
        // own decrypt_unit_key, so this pins the derive-down WIRING (Volume →
        // per-CPS Unit), not the cipher.
        let vuk = [0x5au8; 16];
        let enc0 = [0x12u8; 16];
        let enc1 = [0x34u8; 16];
        let exp0 = crate::aacs::decrypt_unit_key(&vuk, &enc0);
        let exp1 = crate::aacs::decrypt_unit_key(&vuk, &enc1);

        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        let mut a = aacs_with(Vec::new());
        a.uk_ro = uk_ro_v20(&[enc0, enc1]);
        disc.aacs = Some(a);

        disc.decrypt_with(Key::Volume(vuk), &[]).unwrap();
        match disc.decrypt_keys() {
            crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => {
                assert_eq!(
                    unit_keys,
                    vec![(1u32, exp0), (2u32, exp1)],
                    "VUK must decrypt EACH CPS unit's encrypted key (does not stop at VK)"
                );
            }
            _ => panic!("expected Aacs decrypt keys after Volume-key derive-down"),
        }
        assert_eq!(
            disc.aacs.as_ref().unwrap().key_source,
            KeyOrigin::ExternalUk
        );
    }

    #[test]
    fn decrypt_with_higher_key_without_inputs_errors() {
        // A non-Unit key needs the AACS inputs (Unit_Key_RO.inf) stashed at
        // scan. Without them the lib cannot derive — surfaces AacsNoKeys, not a
        // panic and not a silent keyless "success".
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(Vec::new())); // uk_ro empty
        assert!(matches!(
            disc.decrypt_with(Key::Volume([0x11u8; 16]), &[])
                .unwrap_err(),
            crate::error::Error::AacsNoKeys
        ));

        // No AACS state at all → same.
        let mut disc2 = make_test_disc(1000, "UHD");
        disc2.encrypted = true;
        assert!(matches!(
            disc2
                .decrypt_with(Key::Media(vec![[0x22u8; 16]]), &[])
                .unwrap_err(),
            crate::error::Error::AacsNoKeys
        ));
    }

    #[test]
    fn decrypt_with_volume_yielding_no_units_is_rejected() {
        // A key that produces zero unit keys (here: an empty key-storage area)
        // is a rejection, not a silent empty success.
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        let mut a = aacs_with(Vec::new());
        a.uk_ro = uk_ro_v20(&[]); // num_uk = 0
        disc.aacs = Some(a);
        assert!(matches!(
            disc.decrypt_with(Key::Volume([0x11u8; 16]), &[])
                .unwrap_err(),
            crate::error::Error::AacsKeyRejected
        ));
    }

    #[test]
    fn decrypt_with_unit_key_yields_decrypt_keys() {
        // The public lookup-free entry point: hand libfreemkv a Key::Unit and
        // decrypt_keys() must return usable AACS state (same path as the
        // deferred-mux resume — autorip resolves the UK and passes it in).
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        let uk = vec![(0u32, [0x44u8; 16])];
        disc.decrypt_with(Key::Unit(uk.clone()), &[]).unwrap();
        match disc.decrypt_keys() {
            crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => {
                assert_eq!(unit_keys, uk);
            }
            _ => panic!("expected Aacs decrypt keys after decrypt_with(Key::Unit)"),
        }
    }

    #[test]
    fn unit_key_validation_gates_on_real_ciphertext() {
        use crate::aacs::decrypt::{ALIGNED_UNIT_LEN, is_aacs_scrambled};

        // No samples -> nothing to disprove against -> accept (sample-less paths
        // like resume / mapfile must be unaffected).
        assert!(super::aligned_unit_keys_validate(
            &[(0, [0x11u8; 16])],
            None,
            &[]
        ));

        // A clear unit (TS syncs intact) is not scrambled -> proves nothing ->
        // accept even with an arbitrary key.
        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            clear[off] = 0x47;
            off += 192;
        }
        assert!(!is_aacs_scrambled(&clear));
        assert!(super::aligned_unit_keys_validate(
            &[(0, [0x11u8; 16])],
            None,
            &[clear.clone()]
        ));

        // A genuinely scrambled unit the RIGHT key restores to clear TS.
        let uk = [0x5au8; 16];
        let enc = encrypt_unit_for_test(&clear, &uk);
        assert!(
            is_aacs_scrambled(&enc),
            "encrypted unit must read scrambled"
        );

        // Right key -> de-scrambles -> accept (NO false reject of a good key).
        assert!(super::aligned_unit_keys_validate(
            &[(7, uk)],
            None,
            &[enc.clone()]
        ));
        // Wrong key -> cannot de-scramble a scrambled sample -> reject.
        assert!(!super::aligned_unit_keys_validate(
            &[(7, [0x00u8; 16])],
            None,
            &[enc.clone()]
        ));
        // Empty key set against a scrambled sample -> reject.
        assert!(!super::aligned_unit_keys_validate(&[], None, &[enc]));
    }

    #[test]
    fn unit_key_validation_rejects_partial_cps_unit_coverage() {
        // Regression: a multi-CPS-unit disc. CPS unit 0's body is scrambled
        // under uk0; CPS unit 1's body under uk1. A resolved key set that
        // covers only CPS unit 0 used to pass validation (the old gate accepted
        // on the FIRST sample any key decrypted), committing an incomplete set —
        // CPS-unit-1 sectors then passed through as raw encrypted bytes into the
        // ISO/MKV with no error surfaced. The gate must now reject a key set
        // that leaves any scrambled sample uncovered.
        use crate::aacs::decrypt::{ALIGNED_UNIT_LEN, is_aacs_scrambled};

        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            clear[off] = 0x47;
            off += 192;
        }

        let uk0 = [0x11u8; 16];
        let uk1 = [0x22u8; 16];
        let sample0 = encrypt_unit_for_test(&clear, &uk0); // CPS unit 0 body
        let sample1 = encrypt_unit_for_test(&clear, &uk1); // CPS unit 1 body
        assert!(is_aacs_scrambled(&sample0));
        assert!(is_aacs_scrambled(&sample1));

        let samples = vec![sample0.clone(), sample1.clone()];

        // Partial key set (CPS unit 0 only) against samples from BOTH units ->
        // reject. This is the bug fix: previously this returned true.
        assert!(!super::aligned_unit_keys_validate(
            &[(0, uk0)],
            None,
            &samples
        ));

        // Complete key set (both CPS units) -> accept.
        assert!(super::aligned_unit_keys_validate(
            &[(0, uk0), (1, uk1)],
            None,
            &samples
        ));

        // Order-independent: covering key present anywhere in the set is fine.
        assert!(super::aligned_unit_keys_validate(
            &[(1, uk1), (0, uk0)],
            None,
            &samples
        ));
    }

    /// Inverse of `decrypt_unit` for one 6144-byte unit: produce on-disc
    /// ciphertext that `decrypt_unit(uk)` restores to `clear`. Mirrors the AACS
    /// unit algorithm — ECB-derive the per-unit key, then AES-CBC encrypt the
    /// body with the fixed AACS IV.
    fn encrypt_unit_for_test(clear: &[u8], uk: &[u8; 16]) -> Vec<u8> {
        use crate::aacs::decrypt::{AACS_IV, ALIGNED_UNIT_LEN};
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
        let mut unit = clear[..ALIGNED_UNIT_LEN].to_vec();
        let mut header = [0u8; 16];
        header.copy_from_slice(&unit[..16]);
        let cipher = Aes128::new(GenericArray::from_slice(uk));
        let mut blk = GenericArray::clone_from_slice(&header);
        cipher.encrypt_block(&mut blk);
        let mut dk = [0u8; 16];
        for i in 0..16 {
            dk[i] = blk[i] ^ header[i];
        }
        let bc = Aes128::new(GenericArray::from_slice(&dk));
        let mut prev = AACS_IV;
        let mut i = 16;
        while i + 16 <= ALIGNED_UNIT_LEN {
            let mut b = [0u8; 16];
            for j in 0..16 {
                b[j] = unit[i + j] ^ prev[j];
            }
            let mut g = GenericArray::clone_from_slice(&b);
            bc.encrypt_block(&mut g);
            for j in 0..16 {
                unit[i + j] = g[j];
            }
            prev.copy_from_slice(&unit[i..i + 16]);
            i += 16;
        }
        unit
    }

    #[test]
    fn inject_unit_keys_is_noop_without_aacs_on_unencrypted_or_css() {
        // Unencrypted disc: nothing to inject into, stays None.
        let mut plain = make_test_disc(1000, "PLAIN");
        plain.inject_unit_keys(vec![(0, [0x22; 16])]);
        assert!(plain.aacs.is_none());
        assert!(matches!(
            plain.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));

        // Encrypted CSS (DVD): an AACS UK must NOT synthesize an AACS state.
        let mut dvd = make_test_disc(1000, "DVD");
        dvd.format = DiscFormat::Dvd;
        dvd.encrypted = true;
        dvd.css = Some(crate::css::CssState {
            title_key: [0u8; 5],
            crack_span: None,
        });
        dvd.inject_unit_keys(vec![(0, [0x33; 16])]);
        assert!(dvd.aacs.is_none(), "CSS disc must not gain an AACS state");
    }

    /// Records the LBAs read; returns all-zero (unscrambled) sectors so any
    /// re-crack attempt finds no key and falls back, while we observe WHETHER
    /// the title's extents were read at all.
    struct RecordingSource {
        reads: std::cell::RefCell<Vec<u32>>,
    }
    impl SectorSource for RecordingSource {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.reads.borrow_mut().push(lba);
            let n = (count as usize * 2048).min(buf.len());
            for b in buf[..n].iter_mut() {
                *b = 0;
            }
            Ok(n)
        }
    }

    fn css_disc_with_two_vts() -> Disc {
        // Title 0 (cracked VTS) at LBA 100..200; title 1 (other VTS) at
        // 5000..5100. The cracked key's span is title 0's extents.
        let mut t0 = title_with_video(Codec::Mpeg2, Resolution::R480p);
        t0.extents = vec![Extent {
            start_lba: 100,
            sector_count: 100,
        }];
        let mut t1 = title_with_video(Codec::Mpeg2, Resolution::R480p);
        t1.playlist = "00801.mpls".into();
        t1.extents = vec![Extent {
            start_lba: 5000,
            sector_count: 100,
        }];
        let mut disc = make_test_disc(6000, "DVD");
        disc.format = DiscFormat::Dvd;
        disc.content_format = ContentFormat::MpegPs;
        disc.encrypted = true;
        disc.titles = vec![t0, t1];
        disc.css = Some(crate::css::CssState {
            title_key: [0xAB; 5],
            crack_span: Some((100, 200)),
        });
        disc
    }

    /// Regression (multi-VTS CSS): a title that OVERLAPS the cracked span is
    /// the same VTS — the existing key is reused and the reader is NOT touched.
    #[test]
    fn decrypt_keys_for_title_reuses_key_for_same_vts() {
        let disc = css_disc_with_two_vts();
        let mut src = RecordingSource {
            reads: std::cell::RefCell::new(Vec::new()),
        };
        match disc.decrypt_keys_for_title(0, &mut src, 16) {
            crate::decrypt::DecryptKeys::Css { title_key } => {
                assert_eq!(title_key, [0xAB; 5], "same-VTS title reuses cracked key");
            }
            _ => panic!("expected Css keys for same-VTS title"),
        }
        assert!(
            src.reads.borrow().is_empty(),
            "an overlapping title must not trigger a re-crack read"
        );
    }

    /// Regression (multi-VTS CSS): a title in a DIFFERENT VTS (no overlap with
    /// the cracked span) must re-crack from its OWN extents — verified by the
    /// reader being driven over that title's LBA range (5000..). The fixture
    /// yields unscrambled sectors so the re-crack finds NO key; the fix
    /// requires this to be a HARD failure (`DecryptKeys::None`), NOT a silent
    /// fall-back to the known-wrong-VTS disc-wide key (which would descramble
    /// to garbage). Both the read-attempt and the None result are asserted.
    #[test]
    fn decrypt_keys_for_title_recracks_for_other_vts() {
        let disc = css_disc_with_two_vts();
        let mut src = RecordingSource {
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let keys = disc.decrypt_keys_for_title(1, &mut src, 16);
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None),
            "a re-crack miss in a provably-different VTS must be a hard failure (None), \
             not the wrong-VTS disc-wide key"
        );
        let reads = src.reads.borrow();
        assert!(
            !reads.is_empty(),
            "a non-overlapping title must trigger a re-crack read"
        );
        assert!(
            reads.iter().all(|&lba| lba >= 5000),
            "re-crack must read title 1's own extents (>=5000), got {reads:?}"
        );
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
            vid: None,
            unit_keys: Vec::new(),
        };
        let result = disc.copy(&mut reader, &iso_path, &opts);
        assert!(
            result.is_ok(),
            "sweep to regular file should succeed: {:?}",
            result.err()
        );
    }

    /// disc→ISO correctness gate (the headline bug, at the copy entry point):
    /// a DECRYPTING copy (`decrypt: true`, i.e. not --raw) of an AACS disc with
    /// no resolved key must ERROR before reading any sector — never write
    /// ciphertext to the ISO and return Ok. Asserts the error code is NoDiscKey
    /// AND that no non-empty ISO was produced.
    #[test]
    fn copy_decrypting_aacs_no_key_errors_and_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("garbage.iso");
        let sectors: u32 = 999; // 3-aligned for AACS units
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let mut disc = make_test_disc(sectors, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(Vec::new())); // encrypted, no unit key → None
        let opts = CopyOptions {
            decrypt: true, // NOT --raw → decryption is required
            multipass: false,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        let err = disc
            .copy(&mut reader, &iso_path, &opts)
            .expect_err("decrypting copy of AACS-no-key disc must error pre-flight");
        assert_eq!(
            err.code(),
            crate::error::Error::NoDiscKey {
                disc_hash: String::new()
            }
            .code(),
            "must surface NoDiscKey, not silently write ciphertext"
        );
        // No partial/garbage ISO: the gate fired before the sweep opened/sized
        // the file, so either the file doesn't exist or it's empty.
        let produced = std::fs::metadata(&iso_path).map(|m| m.len()).unwrap_or(0);
        assert_eq!(produced, 0, "no ciphertext ISO may be written");
    }

    /// The same disc under `--raw` (`decrypt: false`) must PROCEED: the gate is
    /// a no-op for raw, the sweep runs as a pass-through and writes the
    /// encrypted image the user asked for. Proves the gate doesn't over-fire.
    #[test]
    fn copy_raw_aacs_no_key_proceeds() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("raw.iso");
        let sectors: u32 = 999;
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let mut disc = make_test_disc(sectors, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_with(Vec::new()));
        let opts = CopyOptions {
            decrypt: false, // --raw: no decryption, no key needed
            multipass: false,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        assert!(
            disc.copy(&mut reader, &iso_path, &opts).is_ok(),
            "--raw copy of an encrypted disc must proceed (encrypted image is the goal)"
        );
    }

    #[test]
    fn sweep_to_dev_null_real() {
        let sectors: u32 = 1000;
        let bad: std::collections::HashSet<u32> = [500u32, 501, 502].into_iter().collect();
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: bad,
        };
        let disc = make_test_disc(sectors, "T2");
        let _cleanup = CleanupGuard(disc.mapfile_for(std::path::Path::new("/dev/null")));
        let opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        let result = disc.copy(&mut reader, std::path::Path::new("/dev/null"), &opts);
        assert!(
            result.is_ok(),
            "sweep to /dev/null should not fail with ENODEV: {:?}",
            result.err()
        );
    }

    /// End-to-end Pass-1 sweep against a synthetic `MockReader` with an injected
    /// bad-sector region, asserting the RESULTING MAPFILE — the thing the sweep
    /// loop and damage-jump exist to produce. Drives the real `Disc::sweep` (no
    /// live drive, per the project's "synthetic fixtures only" rule) and checks:
    ///   * the leading good region is marked Finished,
    ///   * the bad region (and the skip-ahead gap the damage-jump zero-fills) is
    ///     marked NonTrimmed,
    ///   * the damage-jump actually engaged — the NonTrimmed span is far larger
    ///     than the single failed ECC batch, which only happens if Pass-1 jumped
    ///     ahead (JUMP_BASE_SECTORS×batch) and zero-filled the gap as NonTrimmed,
    ///   * the mapfile covers the whole disc with no overlap, and good+retryable
    ///     accounting matches.
    ///
    /// Note: this exercises the real cooldown/pause pacing, so it spends a few
    /// seconds of wall time on the single zone-entry pause (same cost the
    /// existing `sweep_to_dev_null_real` already pays) — but unlike that test it
    /// asserts the actual recovery bookkeeping, not just `is_ok()`.
    #[test]
    fn sweep_marks_bad_region_nontrimmed_and_engages_damage_jump() {
        use crate::disc::mapfile::{Mapfile, SectorStatus};

        let sectors: u32 = 1000;
        // One bad sector at LBA 320 fails the entire ECC batch [320,352).
        // batch=32 for UHD, so [0,320) = 10 clean batches before the failure.
        let bad: std::collections::HashSet<u32> = [320u32].into_iter().collect();
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: bad,
        };
        let disc = make_test_disc(sectors, "DJ");
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("dj.iso");
        let opts = SweepOptions {
            decrypt: false,
            resume: false,
            batch_sectors: None, // → ecc batch (32) for UHD
            skip_on_error: true, // multipass → damage-jump engaged
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        disc.sweep(&mut reader, &iso_path, &opts).expect("sweep");

        let mf = Mapfile::load(&disc.mapfile_for(&iso_path)).expect("load mapfile");
        let good = mf.ranges_with(&[SectorStatus::Finished]);
        let bad_ranges = mf.ranges_with(&[SectorStatus::NonTrimmed]);
        let disc_bytes = sectors as u64 * 2048;
        const SEC: u64 = crate::consts::SECTOR_BYTES as u64;

        // The first failing batch starts at LBA 320; everything before it read
        // cleanly and must be Finished.
        let good_bytes: u64 = good.iter().map(|(_, sz)| sz).sum();
        assert!(
            good_bytes > 0,
            "leading clean region must be marked Finished"
        );
        assert!(
            good.iter().all(|(pos, sz)| pos + sz <= 320 * SEC),
            "all Finished bytes must lie before the bad batch at LBA 320; got {good:?}"
        );
        // The clean lead is the 10 batches [0,320) = 320 sectors.
        assert_eq!(
            good_bytes,
            320 * SEC,
            "exactly the 320 clean sectors before the failure are Finished"
        );

        // The bad region must be NonTrimmed and must START at the failed batch.
        assert!(
            !bad_ranges.is_empty(),
            "the failed batch must produce a NonTrimmed range"
        );
        let bad_bytes: u64 = bad_ranges.iter().map(|(_, sz)| sz).sum();
        let (first_bad_pos, _) = bad_ranges[0];
        assert_eq!(
            first_bad_pos,
            320 * SEC,
            "NonTrimmed must begin at the failed ECC batch (LBA 320)"
        );

        // Damage-jump proof: a single ECC batch is 32 sectors. If only the failed
        // batch were marked, NonTrimmed would be ~32 sectors. The fast-jump
        // (JUMP_BASE_SECTORS=1024 × batch=32) overshoots this 1000-sector disc, so
        // the entire tail from the failure to EOF is zero-filled NonTrimmed — far
        // more than one batch. That can ONLY happen if the jump engaged.
        assert!(
            bad_bytes > 32 * SEC,
            "NonTrimmed span ({} sectors) must exceed a single ECC batch — proves \
             the damage-jump skipped ahead and zero-filled the gap",
            bad_bytes / SEC
        );
        // Specifically: the jump overshoots EOF, so the whole tail [320,1000) is
        // NonTrimmed.
        assert_eq!(
            bad_bytes,
            (sectors as u64 - 320) * SEC,
            "the damage-jump overshoots EOF → the entire tail is NonTrimmed"
        );

        // Whole-disc coverage with no gaps/overlap: Finished + NonTrimmed = disc.
        assert_eq!(
            good_bytes + bad_bytes,
            disc_bytes,
            "Finished + NonTrimmed must cover the whole disc exactly"
        );
        // Stats agree with the range view.
        let stats = mf.stats();
        assert_eq!(stats.bytes_good, good_bytes, "stats.bytes_good vs ranges");
        assert_eq!(
            stats.bytes_retryable, bad_bytes,
            "NonTrimmed counts as retryable in stats"
        );
        assert!(
            stats.bytes_unreadable == 0,
            "Pass-1 never promotes to Unreadable (that's a later pass's job)"
        );
    }

    /// Regression (finding 6): sweep() resume against a mapfile whose
    /// total_size != the real disc size must DOWNGRADE to a fresh full sweep
    /// covering [0, capacity), not reuse the stale mapfile (which would
    /// abandon the disc tail or read past capacity). Mirrors copy()'s
    /// covers_disc reconciliation for the direct-sweep entry point.
    #[test]
    fn sweep_resume_downgrades_on_size_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("mismatch.iso");

        // First sweep: a small disc → mapfile sized to small_sectors.
        let small_sectors: u32 = 500;
        let mut small_reader = MockReader {
            total_sectors: small_sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let small_disc = make_test_disc(small_sectors, "SMALL");
        let opts0 = SweepOptions {
            decrypt: false,
            resume: false,
            batch_sectors: None,
            skip_on_error: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        small_disc
            .sweep(&mut small_reader, &iso_path, &opts0)
            .expect("initial small sweep");
        let mf = small_disc.mapfile_for(&iso_path);
        assert_eq!(
            mapfile::Mapfile::load(&mf).unwrap().total_size(),
            small_sectors as u64 * 2048,
            "precondition: mapfile reflects the small disc"
        );

        // Now a LARGER disc resumes against that stale (under-cover) mapfile.
        // The reconciliation must force a fresh full sweep of the big disc.
        let big_sectors: u32 = 2000;
        let mut big_reader = MockReader {
            total_sectors: big_sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let big_disc = make_test_disc(big_sectors, "BIG");
        let opts_resume = SweepOptions {
            resume: true,
            ..opts0
        };
        let result = big_disc
            .sweep(&mut big_reader, &iso_path, &opts_resume)
            .expect("resume sweep on mismatched mapfile");

        assert_eq!(
            result.bytes_total,
            big_sectors as u64 * 2048,
            "fresh sweep must be sized to the real (big) disc"
        );
        assert_eq!(
            result.bytes_good,
            big_sectors as u64 * 2048,
            "the whole big disc (incl. the tail beyond the stale mapfile) must be swept"
        );
        assert_eq!(
            mapfile::Mapfile::load(&mf).unwrap().total_size(),
            big_sectors as u64 * 2048,
            "mapfile must be re-created at the real disc size, not the stale one"
        );
    }

    /// Regression (resume/mapfile consistency, MED): a resume sweep against a
    /// mapfile that claims prior progress (Finished ranges) while the ISO is
    /// missing/zero-length must DOWNGRADE to a fresh full sweep — NOT reuse the
    /// stale mapfile. The producer only builds work from NonTried ranges, so a
    /// reused mapfile would leave every Finished range unread and ZERO in the
    /// new ISO (a silent hole). Reachable via autorip ResumeMode::Require when
    /// the ISO was deleted/truncated but the mapfile survived. The fresh-sweep
    /// downgrade self-heals: all ranges are re-read and the ISO is fully
    /// populated.
    #[test]
    fn sweep_resume_downgrades_on_zero_iso_with_progress_mapfile() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("zeroed.iso");

        let sectors: u32 = 500;
        let total_bytes = sectors as u64 * 2048;
        let disc = make_test_disc(sectors, "ZEROED");

        // First sweep: clean disc → ISO fully written, mapfile all-Finished.
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let opts0 = SweepOptions {
            decrypt: false,
            resume: false,
            batch_sectors: None,
            skip_on_error: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        disc.sweep(&mut reader, &iso_path, &opts0)
            .expect("initial clean sweep");
        let mf = disc.mapfile_for(&iso_path);
        let loaded = mapfile::Mapfile::load(&mf).unwrap();
        assert_eq!(
            loaded.stats().bytes_pending,
            0,
            "precondition: a clean sweep leaves no pending (all Finished) ranges"
        );

        // Truncate the ISO to zero length while the progress-claiming mapfile
        // survives — exactly the inconsistent-resume case.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&iso_path)
            .expect("truncate ISO to zero");
        assert_eq!(
            std::fs::metadata(&iso_path).unwrap().len(),
            0,
            "precondition: ISO is zero-length"
        );

        // Resume sweep: must downgrade to a fresh FULL sweep, re-reading every
        // range (including the formerly-Finished ones).
        let mut reader2 = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let opts_resume = SweepOptions {
            resume: true,
            ..opts0
        };
        let result = disc
            .sweep(&mut reader2, &iso_path, &opts_resume)
            .expect("resume sweep on zero-length ISO");

        // A holed resume would re-read nothing (no NonTried ranges) → bytes_good
        // == 0 and a zero ISO. The downgrade re-reads the whole disc.
        assert_eq!(
            result.bytes_good, total_bytes,
            "downgrade must re-read the whole disc, not skip Finished ranges"
        );
        assert_eq!(
            std::fs::metadata(&iso_path).unwrap().len(),
            total_bytes,
            "ISO must be re-sized + fully written, not left zero/holed"
        );

        // The ISO must actually contain the swept data (0xAA) at LBA 0 — proof
        // the formerly-Finished head range was re-read, not left as a hole.
        let iso = std::fs::read(&iso_path).unwrap();
        assert_eq!(
            &iso[..2048],
            &[0xAAu8; 2048][..],
            "head sector must hold re-read data, not a zero hole"
        );
    }

    /// Regression (resume reconciliation, MED follow-on): a resume sweep against
    /// a CORRUPT / unparseable mapfile must DOWNGRADE to a fresh full sweep —
    /// not proceed with resume=true (which would hand a garbage/empty mapfile to
    /// open_or_create and silently skip ranges). The `load()` Err arm sets
    /// resume=false; the `!resume` path then drops the corrupt mapfile and the
    /// rip restarts clean. Consistent with the total_size-mismatch downgrade.
    #[test]
    fn sweep_resume_downgrades_on_corrupt_mapfile() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("corrupt.iso");

        let sectors: u32 = 500;
        let total_bytes = sectors as u64 * 2048;
        let disc = make_test_disc(sectors, "CORRUPT");
        let mf = disc.mapfile_for(&iso_path);

        // Write a non-empty ISO so the zero-length-ISO guard is NOT what triggers
        // the downgrade — we want the corrupt-mapfile path specifically.
        std::fs::write(&iso_path, vec![0u8; total_bytes as usize]).unwrap();
        // Plant a corrupt mapfile: garbage bytes that Mapfile::load can't parse.
        std::fs::write(&mf, b"this is not a valid ddrescue mapfile\nxxxx\n").unwrap();
        assert!(
            mapfile::Mapfile::load(&mf).is_err(),
            "precondition: the planted mapfile must be unparseable"
        );

        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let opts = SweepOptions {
            decrypt: false,
            resume: true,
            batch_sectors: None,
            skip_on_error: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        let result = disc
            .sweep(&mut reader, &iso_path, &opts)
            .expect("resume sweep on corrupt mapfile");

        // The downgrade must re-sweep the whole disc from a fresh mapfile.
        assert_eq!(
            result.bytes_good, total_bytes,
            "corrupt-mapfile resume must downgrade to a fresh full sweep"
        );
        let reloaded = mapfile::Mapfile::load(&mf)
            .expect("a valid mapfile must have been written by the fresh sweep");
        assert_eq!(
            reloaded.total_size(),
            total_bytes,
            "mapfile must be re-created at the real disc size"
        );
        assert_eq!(
            reloaded.stats().bytes_pending,
            0,
            "the fresh sweep must leave all ranges Finished"
        );
    }

    /// Regression: a fresh (non-resume) sweep MUST abort if the stale mapfile
    /// cannot be removed, rather than swallowing the error and letting
    /// `open_or_create` load the stale file (which would make the new disc
    /// inherit old Finished ranges → silently zero-filled ISO). We force the
    /// remove to fail with a non-ENOENT error by placing a NON-EMPTY DIRECTORY
    /// at the mapfile path (`remove_file` on a dir fails, and a non-empty dir
    /// can't be ENOENT).
    #[test]
    fn sweep_fresh_aborts_when_stale_mapfile_unremovable() {
        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("blocked.iso");

        let sectors: u32 = 500;
        let disc = make_test_disc(sectors, "BLOCKED");
        let mf = disc.mapfile_for(&iso_path);
        // Put a non-empty directory where the mapfile would live.
        std::fs::create_dir_all(&mf).unwrap();
        std::fs::write(mf.join("occupant"), b"x").unwrap();

        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let opts = SweepOptions {
            decrypt: false,
            resume: false,
            batch_sectors: None,
            skip_on_error: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        let result = disc.sweep(&mut reader, &iso_path, &opts);
        assert!(
            result.is_err(),
            "fresh sweep must abort when the stale mapfile cannot be removed"
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
        let sectors: u32 = 2000;
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };
        let disc = make_test_disc(sectors, "T3");
        let _cleanup = CleanupGuard(disc.mapfile_for(std::path::Path::new("/dev/null")));
        let opts = CopyOptions {
            decrypt: false,
            multipass: false,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
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

    /// Finding #6 regression: on resume, copy() must NOT abandon the un-swept
    /// NonTried tail when retryable (NonTrimmed) bytes also remain. The mapfile
    /// covers the disc and has BOTH a NonTrimmed (retryable) range and a
    /// NonTried tail; dispatch must route to a resume sweep first so the tail is
    /// actually read. Before the fix, `bytes_retryable > 0` short-circuited to
    /// patch and the NonTried tail was silently left unread.
    #[test]
    fn resume_sweeps_nontried_tail_even_with_retryable_present() {
        use crate::disc::mapfile::{Mapfile, SectorStatus};
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};

        // Reader that records every LBA it is asked to read.
        struct TrackingReader {
            total_sectors: u32,
            reads: Arc<Mutex<HashSet<u32>>>,
        }
        impl crate::sector::SectorSource for TrackingReader {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> crate::error::Result<usize> {
                {
                    let mut r = self.reads.lock().unwrap();
                    for i in 0..count as u32 {
                        r.insert(lba + i);
                    }
                }
                let n = count as usize * 2048;
                buf[..n].fill(0xAA);
                Ok(n)
            }
            fn capacity_sectors(&self) -> u32 {
                self.total_sectors
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        let sectors: u32 = 200;
        let disc = make_test_disc(sectors, "T6Tail");

        // Pre-build a mapfile covering the whole disc:
        //   [0..100)   Finished
        //   [100..150) NonTrimmed (retryable)
        //   [150..200) NonTried   (un-swept tail)
        let mf_path = disc.mapfile_for(&iso_path);
        {
            let mut mf = Mapfile::create(&mf_path, sectors as u64 * 2048, "test").unwrap();
            mf.record(0, 100 * 2048, SectorStatus::Finished).unwrap();
            mf.record(100 * 2048, 50 * 2048, SectorStatus::NonTrimmed)
                .unwrap();
            // [150..200) stays NonTried from create()'s initial region.
            mf.flush().unwrap();

            // Sanity on the constructed state.
            let st = mf.stats();
            assert!(st.bytes_nontried > 0, "must have a NonTried tail");
            assert!(st.bytes_retryable > 0, "must have retryable bytes too");
            assert_eq!(mf.total_size(), sectors as u64 * 2048);
        }
        // The ISO file must exist for the sweep to write into.
        std::fs::write(&iso_path, vec![0u8; sectors as usize * 2048]).unwrap();

        let reads = Arc::new(Mutex::new(HashSet::new()));
        let mut reader = TrackingReader {
            total_sectors: sectors,
            reads: reads.clone(),
        };
        let opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        let result = disc.copy(&mut reader, &iso_path, &opts);
        assert!(result.is_ok(), "resume copy failed: {:?}", result.err());

        // The un-swept tail [150..200) MUST have been read by the resume sweep.
        let got = reads.lock().unwrap();
        let tail_read = (150u32..200).any(|lba| got.contains(&lba));
        assert!(
            tail_read,
            "resume must sweep the NonTried tail; tail sectors were never read"
        );
    }

    /// Regression (rc.6 user fix): a PLAIN (non-`--multipass`) `disc:// → iso://`
    /// copy interrupted by Ctrl-C must RESUME from where it stopped when the
    /// SAME command is re-issued — not restart from sector 0. The CLI help and
    /// `rip_iso` examples promise "auto-resumes if interrupted". Before the fix
    /// the whole mapfile-resume dispatch in `Disc::copy` was gated behind
    /// `if opts.multipass`, so a plain copy always called
    /// `sweep_internal(resume=false)`, which wiped the mapfile + ISO and swept
    /// the disc again from LBA 0.
    ///
    /// Simulate an interrupted plain sweep: a mapfile that covers the disc with
    /// a Finished prefix [0..100) and a NonTried tail [100..200). A plain re-run
    /// must read ONLY the tail (resume) and leave the prefix untouched.
    #[test]
    fn plain_copy_resumes_nontried_tail_after_interrupt() {
        use crate::disc::mapfile::{Mapfile, SectorStatus};
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};

        // Reader that records every LBA it is asked to read.
        struct TrackingReader {
            total_sectors: u32,
            reads: Arc<Mutex<HashSet<u32>>>,
        }
        impl crate::sector::SectorSource for TrackingReader {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> crate::error::Result<usize> {
                {
                    let mut r = self.reads.lock().unwrap();
                    for i in 0..count as u32 {
                        r.insert(lba + i);
                    }
                }
                let n = count as usize * 2048;
                buf[..n].fill(0xAA);
                Ok(n)
            }
            fn capacity_sectors(&self) -> u32 {
                self.total_sectors
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        let sectors: u32 = 200;
        let disc = make_test_disc(sectors, "PlainResume");

        // Pre-build a mapfile mimicking an interrupted plain sweep:
        //   [0..100)   Finished  (already written before Ctrl-C)
        //   [100..200) NonTried  (un-swept tail)
        let mf_path = disc.mapfile_for(&iso_path);
        {
            let mut mf = Mapfile::create(&mf_path, sectors as u64 * 2048, "test").unwrap();
            mf.record(0, 100 * 2048, SectorStatus::Finished).unwrap();
            // [100..200) stays NonTried from create()'s initial region.
            mf.flush().unwrap();

            let st = mf.stats();
            assert!(st.bytes_nontried > 0, "must have a NonTried tail");
            assert_eq!(st.bytes_retryable, 0, "plain interrupt leaves no retryable");
            assert_eq!(mf.total_size(), sectors as u64 * 2048);
        }
        // The ISO file must already exist (it was being written before the
        // interrupt) so the resume opens it rather than recreating it.
        std::fs::write(&iso_path, vec![0u8; sectors as usize * 2048]).unwrap();

        let reads = Arc::new(Mutex::new(HashSet::new()));
        let mut reader = TrackingReader {
            total_sectors: sectors,
            reads: reads.clone(),
        };
        // PLAIN copy — multipass: false. This is the path the bug broke.
        let opts = CopyOptions {
            decrypt: false,
            multipass: false,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };
        let result = disc.copy(&mut reader, &iso_path, &opts);
        assert!(
            result.is_ok(),
            "plain resume copy failed: {:?}",
            result.err()
        );

        let got = reads.lock().unwrap();
        // The NonTried tail [100..200) MUST have been read by the resume sweep.
        let tail_read = (100u32..200).any(|lba| got.contains(&lba));
        assert!(
            tail_read,
            "plain copy must resume-sweep the NonTried tail; tail sectors were never read"
        );
        // The Finished prefix [0..100) must NOT be re-read — that would mean a
        // restart-from-zero (the bug), not a resume.
        let prefix_reread = (0u32..100).any(|lba| got.contains(&lba));
        assert!(
            !prefix_reread,
            "plain copy must NOT re-read the already-Finished prefix (it restarted from sector 0)"
        );

        // The mapfile must now be fully Finished (disc fully swept on resume).
        let reloaded = Mapfile::load(&mf_path).unwrap();
        assert_eq!(
            reloaded.stats().bytes_nontried,
            0,
            "resume sweep must clear the NonTried tail"
        );
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
            vid: None,
            unit_keys: Vec::new(),
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
            vid: None,
            unit_keys: Vec::new(),
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
            vid: None,
            unit_keys: Vec::new(),
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
            vid: None,
            unit_keys: Vec::new(),
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
            vid: None,
            unit_keys: Vec::new(),
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

    /// bytes_bad_in_title must overlap per-extent, not against a single
    /// bounding box: a bad range in the gap between two extents of the
    /// same title must NOT be counted.
    #[test]
    fn bytes_bad_in_title_ignores_inter_extent_gap() {
        let mut title = title_with_video(Codec::Hevc, Resolution::R2160p);
        // Two extents: sectors [0,10) and [100,110). Gap = [10,100).
        title.extents = vec![
            Extent {
                start_lba: 0,
                sector_count: 10,
            },
            Extent {
                start_lba: 100,
                sector_count: 10,
            },
        ];
        // A bad range entirely inside the gap (sector 50 == byte 50*2048).
        let gap = vec![(50 * 2048, 2048)];
        assert_eq!(
            bytes_bad_in_title(&title, &gap),
            0,
            "bad bytes in the inter-extent gap must not be counted"
        );
        // A bad range overlapping the first extent counts.
        let in_first = vec![(0, 4096)];
        assert_eq!(bytes_bad_in_title(&title, &in_first), 4096);
        // A bad range spanning both extents plus the gap counts only the
        // bytes that fall inside the two extents (10 + 10 sectors).
        let spanning = vec![(0, 110 * 2048)];
        assert_eq!(bytes_bad_in_title(&title, &spanning), 20 * 2048);
    }

    /// 0xA2 is secondary DTS-HD MA (lossless), not lossy HR.
    #[test]
    fn coding_type_a2_is_dts_hd_ma() {
        assert_eq!(Codec::from_coding_type(0xA2), Codec::DtsHdMa);
        assert_eq!(Codec::from_coding_type(0x86), Codec::DtsHdMa);
    }

    /// HDMV coding_type 0x90 = Presentation Graphics (PG / subtitles) → Pgs,
    /// but 0x91 = Interactive Graphics (IG / menus) is NOT a subtitle stream.
    /// It must NOT map to Pgs (whose kind() is Subtitle), else a menu ES would
    /// surface as a bogus PGS subtitle track. 0x91 falls through to Unknown so
    /// the PMT/STN walker drops it.
    #[test]
    fn coding_type_ig_0x91_is_not_pgs_subtitle() {
        assert_eq!(Codec::from_coding_type(0x90), Codec::Pgs);
        assert_eq!(Codec::from_coding_type(0x90).kind(), CodecKind::Subtitle);
        // IG must not be a PGS subtitle.
        assert_eq!(Codec::from_coding_type(0x91), Codec::Unknown(0x91));
        assert_ne!(Codec::from_coding_type(0x91).kind(), CodecKind::Subtitle);
    }

    /// chapter_name emits a bare 1-based ordinal (no localized prose).
    #[test]
    fn chapter_name_is_bare_ordinal() {
        assert_eq!(chapter_name(0), "1");
        assert_eq!(chapter_name(41), "42");
    }

    // ── Regression tests for bisect inner-loop ReadAction dispatch ───────────
    //
    // Before the fix the bisect inner loop discarded the ReadAction returned by
    // handle_read_error:
    //
    //   let _ = read_error::handle_read_error(&inner_err, &mut read_ctx);
    //
    // Consequences:
    //   (a) Retry{pause_secs} — cooldown skipped; sector immediately marked
    //       BisectBad, hammering a degraded drive (violates Hard Rule #2).
    //   (b) AbortPass — ignored; loop kept issuing reads against a crashed drive.
    //
    // The fix replaces the discard with a match.  The tests below prove the
    // required ReadAction values are produced by handle_read_error in the
    // bisect-inner context (bisecting=true, batch=1), so that any regression
    // to `let _ = ...` would break real behaviour on the tested error paths.

    /// NOT_READY inside a bisect must return Retry, not SkipBlock.
    /// If the inner loop discarded the action the 3-second cooldown would be
    /// skipped, hammering the drive during a transient NOT_READY condition.
    #[test]
    fn bisect_inner_not_ready_returns_retry_with_pause() {
        use crate::disc::read_error::{ReadAction, ReadCtx, handle_read_error};
        use crate::error::Error;
        use crate::scsi::ScsiSense;

        let not_ready_err = Error::DiscRead {
            sector: 500,
            status: Some(crate::scsi::SCSI_STATUS_CHECK_CONDITION),
            sense: Some(ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_NOT_READY,
                asc: 0x04,
                ascq: 0x00, // not 0x3E — generic NOT_READY, not bridge degradation
            }),
        };

        let mut ctx = ReadCtx::for_patch(1);
        ctx.bisecting = true; // simulate being inside the bisect inner loop

        let action = handle_read_error(&not_ready_err, &mut ctx);
        match action {
            ReadAction::Retry { pause_secs } => {
                assert!(
                    pause_secs > 0,
                    "NOT_READY retry must carry a non-zero pause; got {pause_secs}s"
                );
            }
            other => panic!(
                "bisect inner NOT_READY must return Retry{{pause_secs}}, got {other:?}; \
                 a discard (`let _ = ...`) would skip this pause and hammer the drive"
            ),
        }
    }

    /// A transport failure inside a bisect must return AbortPass.
    /// If the inner loop discarded the action the loop would continue
    /// issuing reads against a crashed bridge, producing spurious BisectBad
    /// entries and potentially looping until the batch is exhausted.
    #[test]
    fn bisect_inner_transport_failure_returns_abort_pass() {
        use crate::disc::read_error::{ReadAction, ReadCtx, handle_read_error};
        use crate::error::Error;

        let transport_err = Error::DiscRead {
            sector: 500,
            status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
            sense: None,
        };

        let mut ctx = ReadCtx::for_patch(1);
        ctx.bisecting = true;

        let action = handle_read_error(&transport_err, &mut ctx);
        assert_eq!(
            action,
            ReadAction::AbortPass,
            "bisect inner transport failure must return AbortPass; \
             a discard (`let _ = ...`) would silently keep looping against a crashed drive"
        );
    }

    /// After enough consecutive wedge errors with bisecting=true the handler
    /// must eventually return AbortPass.  Before the fix, the inner loop
    /// discarded the returned action and kept issuing reads against a permanently
    /// wedged drive at full rate.
    ///
    /// The threshold is 16 consecutive wedges (WEDGE_ABORT_THRESHOLD in
    /// read_error.rs); we drive 20 iterations to give the assertion headroom
    /// without hard-coding the internal constant here.
    #[test]
    fn bisect_inner_wedge_abort_threshold_reached_returns_abort_pass() {
        use crate::disc::read_error::{ReadAction, ReadCtx, handle_read_error};
        use crate::error::Error;
        use crate::scsi::ScsiSense;

        let hardware_err = || Error::DiscRead {
            sector: 500,
            status: Some(crate::scsi::SCSI_STATUS_CHECK_CONDITION),
            sense: Some(ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_HARDWARE_ERROR,
                asc: 0x44,
                ascq: 0x00,
            }),
        };

        let mut ctx = ReadCtx::for_patch(1);
        ctx.bisecting = true;

        let mut aborted = false;
        for _ in 0..20 {
            let action = handle_read_error(&hardware_err(), &mut ctx);
            if action == ReadAction::AbortPass {
                aborted = true;
                break;
            }
        }
        assert!(
            aborted,
            "bisect inner wedge loop must reach AbortPass after consecutive hardware errors; \
             a discard (`let _ = ...`) would loop forever on a bricked drive"
        );
    }

    /// Regression: copy() dispatch with covers_disc=true, retryable=0, nontried>0 must
    /// route to sweep_internal(resume=true) so the unread NonTried ranges are actually
    /// read rather than silently abandoned.
    ///
    /// Before the fix the fallthrough returned a terminal CopyResult immediately,
    /// leaving the NonTried sectors unread.
    #[test]
    fn copy_dispatch_routes_to_sweep_when_nontried_gt_zero() {
        use crate::disc::mapfile::{self, SectorStatus};

        let tmp = tempfile::tempdir().unwrap();
        let iso_path = tmp.path().join("test.iso");
        let sectors: u32 = 200;
        let disc = make_test_disc(sectors, "DispatchNonTried");
        let disc_size = sectors as u64 * 2048;

        // Synthesise a mapfile that covers the disc (total_size == disc_size) with:
        //   - [0, half_bytes): Finished
        //   - [half_bytes, disc_size): NonTried
        // This gives covers_disc=true, bytes_retryable=0, bytes_nontried>0.
        let mf_path = disc.mapfile_for(&iso_path);
        let half_bytes = disc_size / 2;
        {
            let mut map =
                mapfile::Mapfile::create(&mf_path, disc_size, "test").expect("create mapfile");
            map.record(0, half_bytes, SectorStatus::Finished)
                .expect("record Finished");
            map.flush().expect("flush");
        }

        // Create an ISO file pre-sized to the full disc size so the resume
        // sweep can open it and write the NonTried regions at their offsets.
        // (len > 0 selects the resume-open branch; full pre-size avoids
        // short-seek writes past EOF.)
        {
            let f = std::fs::File::create(&iso_path).expect("create iso");
            f.set_len(disc_size).expect("pre-size iso");
        }

        // All sectors are readable in this reader.
        let mut reader = MockReader {
            total_sectors: sectors,
            bad_sectors: std::collections::HashSet::new(),
        };

        let opts = CopyOptions {
            decrypt: false,
            multipass: true,
            progress: None,
            halt: None,
            vid: None,
            unit_keys: Vec::new(),
        };

        let result = disc.copy(&mut reader, &iso_path, &opts);
        assert!(
            result.is_ok(),
            "copy with nontried>0 should succeed: {:?}",
            result.err()
        );
        let r = result.unwrap();
        // The sweep must have read the NonTried half — bytes_good should be
        // the whole disc, not just the already-Finished half.
        assert_eq!(
            r.bytes_good, disc_size,
            "all sectors must be good after resume sweep reads the NonTried half \
             (before fix: terminal returned with bytes_good={}, skipping {} NonTried bytes)",
            half_bytes, half_bytes
        );
    }
}
