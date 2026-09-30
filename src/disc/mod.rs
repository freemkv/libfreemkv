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
pub(crate) mod dvd_audio_probe;
mod encrypt;
pub(crate) use encrypt::handshake_class_error;
mod extract;
mod hddvd;
pub(crate) mod pgs_forced_probe;
pub mod profile;
#[cfg(test)]
mod scan_order_tests;

use crate::drive::Drive;
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf;

// Re-export label classification enums alongside AudioStream / SubtitleStream
// so the public surface keeps the structured metadata together. Callers map
// these to display text in their own locale.
pub use crate::labels::{LabelPurpose, LabelQualifier};
pub use extract::{ExtractOptions, ExtractResult, FileResult};
pub use profile::{AudioTrack, DiscProfile, SubtitleTrack, TitleProfile, VideoTrack};

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
    /// scrambled sectors but could NOT recover a title key. `css` is `None`
    /// in that case — but the disc is genuinely encrypted, so callers MUST
    /// surface this hard error rather than treat `css.is_none()` as
    /// "unencrypted" and mux scrambled MPEG as plaintext garbage. `None` when
    /// no key was needed or one was recovered. Records the MAIN feature's
    /// crack (whole-disc signal): gates convert it to `Error::CssNoDiscKey`,
    /// not the per-title `Error::CssKeyMissing` this field itself carries.
    pub css_error: Option<crate::error::Error>,
    /// Content format (BD transport stream vs DVD program stream)
    pub content_format: ContentFormat,
}

/// Content format — determines how sectors are interpreted downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentFormat {
    /// Blu-ray BD Transport Stream (192-byte packets)
    BdTs,
    /// MPEG-2 Program Stream — DVD (`.vob`) and HD-DVD (`.evo`). For AACS content
    /// this selects the PS-aware encrypted-flag / structural checks.
    MpegPs,
}

/// Disc format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiscFormat {
    /// 4K UHD Blu-ray (HEVC 2160p)
    Uhd,
    /// UHD Blu-ray with AACS 2.1 FMTS content — the main feature is a `.fmts`
    /// clip (M2TS transport stream plus interleaved forensic variant segments).
    /// A BD-tree disc (enumerated by [`Disc::scan_bluray_titles`]); distinct
    /// from [`DiscFormat::Uhd`] only in the container + AACS generation.
    Fmts,
    /// Standard Blu-ray (1080p/1080i)
    BluRay,
    /// HD-DVD — `HVDVD_TS/` tree with `.evo` (Enhanced VOB, MPEG program stream)
    /// clips. A tree-level peer of DVD/BD, enumerated by its own scanner.
    HdDvd,
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
    /// Byte range this clip's stream occupies within the TITLE'S FEED — the
    /// concatenation of the title's extents, in the order the mux reads them.
    /// `None` when it could not be determined. This is PROVENANCE: during an
    /// overlap, two clips' mark ranges can share a timestamp, so a frame's
    /// source byte offset (`PesFrame::source`) — which falls in exactly one
    /// clip's span — is used for assignment instead of timestamps alone.
    pub feed_span: Option<(u64, u64)>,
}

/// Per-title classification for [`Disc::main_feature_order`]. All three fields
/// are GLOBAL properties (they depend on the whole title list, not on a pairwise
/// comparison), so they are precomputed once by [`Disc::rank_titles`] and
/// threaded into the comparator.
#[derive(Debug, Clone, Copy, Default)]
pub struct TitleRank {
    /// The disc's own HDMV navigation (played like a real player by the
    /// [`crate::bdnav`] VM) reaches this video-bearing title as the feature —
    /// the authoritative `nav-feature` signal, above `authoring`.
    pub nav: bool,
    /// The disc's authoring names this (video-bearing, non-composite,
    /// duration-corroborated) title as the feature.
    pub authoring: bool,
    /// This title is a play-all / wrapper composite of another substantial
    /// video title (see the `standalone` key in [`Disc::MAIN_FEATURE_ORDER_KEYS`]).
    pub composite: bool,
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

/// Label marking a video stream as the Blu-ray 3D **MVC dependent (right-eye)
/// view** — the paired substream of the AVC base view. Set by the BD scan
/// (`bluray.rs`) and recognised by the mux path (`resolve.rs` builds its parser
/// in param-set-preserving mode; `mkvstream.rs` folds it into the base track as
/// per-frame `BlockAdditional`). Single source of truth for the contract.
pub const MVC_DEPENDENT_LABEL: &str = "MVC dependent view (3D right eye)";

impl VideoStream {
    /// Whether this video stream is the MVC dependent (right-eye) view — the
    /// 3D substream that the muxer merges into the base track as a per-frame
    /// `BlockAdditional` rather than emitting as an independent track.
    pub fn is_mvc_dependent(&self) -> bool {
        self.label == MVC_DEPENDENT_LABEL
    }
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

/// Label marking an [`AudioStream`] as a DVD MPEG-2 multichannel extension bit stream (PES
/// `0xD0|n`): the ISO/IEC 13818-3 remainder of the surround that only means something next to
/// its base MPEG audio track (`0xC0|n`). A marker like [`MVC_DEPENDENT_LABEL`], not display text.
pub const MP2_EXTENSION_LABEL: &str = "MPEG-2 multichannel extension";

impl AudioStream {
    /// Whether this is a DVD MPEG-2 multichannel extension track ([`MP2_EXTENSION_LABEL`] on
    /// PID `0xD0..=0xD7`), which a sink either writes next to its base or lists as excluded.
    /// Its base is PID `0xC0 | (pid & 7)`.
    pub fn is_mp2_extension(&self) -> bool {
        self.label == MP2_EXTENSION_LABEL && matches!(self.pid, 0xD0..=0xD7)
    }
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
    /// 3.0 (L C R, or L R S), no LFE
    Surround30,
    /// 3.1 (3.0 + LFE)
    Surround31,
    /// 4.0 quadraphonic
    Quad,
    /// 4.1 (4.0 + LFE)
    Surround41,
    /// 5.0 surround (no LFE)
    Surround50,
    /// 5.1 surround — standard BD/DVD surround
    Surround51,
    /// 6.0 surround (no LFE)
    Surround60,
    /// 6.1 surround (DTS-ES, Dolby EX)
    Surround61,
    /// 7.0 surround (no LFE)
    Surround70,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    pub start_lba: u32,
    pub sector_count: u32,
}

// THE ONE definition of "structurally AACS-encrypted" — shared by the fast identify and the
// full scan so they can't silently desync. Structural, not cryptographic; HD DVD's `X!` dir
// is found by the same discovery the key-file reads use.
pub(crate) fn aacs_dir_present(udf_fs: &crate::udf::UdfFs) -> bool {
    udf_fs.find_dir("/AACS").is_some()
        || udf_fs.find_dir("/BDMV/AACS").is_some()
        || crate::aacs::find_hddvd_aacs_dir(udf_fs).is_some()
}

// Title-ranking heuristic thresholds. Each gates a DISTINCT decision — several share a value
// (0.5) by coincidence, so they are named and tuned separately.
/// Nav candidate: a real-video title qualifies only if it runs at least this
/// fraction of the longest probable-video title's duration.
const NAV_CANDIDATE_MIN_DURATION_FRAC: f64 = 0.5;
/// Payload floor: a promoted title must hold at least this fraction of the
/// largest video-bearing title's byte size, else a tiny playback-valid branch
/// could win nav/authoring.
const FEATURE_PAYLOAD_MIN_SIZE_FRAC: f64 = 0.10;
/// Composite/wrapper gate: candidate wrapper Q must be at least this fraction of
/// the outer title P's byte size to count as a plausible wrapper.
const WRAPPER_MIN_SIZE_FRAC: f64 = 0.5;
/// Seamless-branch guard (issue #45): Q is a wrapper if it is a much SHORTER cut,
/// i.e. its duration is below this fraction of P's duration.
const SEAMLESS_SHORTER_CUT_DURATION_FRAC: f64 = 0.85;
/// Seamless-branch guard (issue #45): Q is a wrapper if it is nearly P's SIZE, i.e.
/// its byte size is at least this fraction of P's (a bumper, not a real branch).
const SEAMLESS_BUMPER_SIZE_FRAC: f64 = 0.90;
/// Authoring hint: honoured only when the title runs at least this fraction of the
/// longest probable-video title's duration.
const AUTHORING_MIN_DURATION_FRAC: f64 = 0.5;
/// Seamless-branch guard (issue #45): the minimum number of chapter marks a title
/// must carry to look like a COMPLETE feature presentation (a real feature has a
/// full chapter table; a bare body/branch subset carries one mark or none).
const MIN_FEATURE_CHAPTERS: usize = 2;
/// Seamless-branch guard (issue #45): a complete feature's chapter marks span at
/// least this fraction of its own runtime (start-to-last), distinguishing a real
/// chapter table from a couple of clustered marks.
const CHAPTER_SPAN_MIN_FRAC: f64 = 0.5;

/// Issue #45: does `q` look like a COMPLETE feature presentation — a real chapter table (≥
/// [`MIN_FEATURE_CHAPTERS`] marks) spanning ≥ [`CHAPTER_SPAN_MIN_FRAC`] of its runtime? A bare
/// body subset (Alita's 00703 = one mark) fails; a genuine feature (Alita's 00800 = 37 marks)
/// passes. Gates the composite bumper limb so a chaptered feature is not demoted below its own
/// un-chaptered body subset. This is the ABSOLUTE-threshold form; to switch to the RELATIVE
/// form (only if a hoard sweep finds an SM3-style decoy whose chaptered member is the SUPERSET)
/// replace the call site with `p.chapters.len() <= q.chapters.len()`.
fn is_complete_feature_presentation(q: &DiscTitle) -> bool {
    q.chapters.len() >= MIN_FEATURE_CHAPTERS
        && q.duration_secs > 0.0
        && (q.chapters.last().map(|c| c.time_secs).unwrap_or(0.0)
            - q.chapters.first().map(|c| c.time_secs).unwrap_or(0.0))
            >= CHAPTER_SPAN_MIN_FRAC * q.duration_secs
}

/// Union a set of extents into sorted, merged, disjoint `(start_lba,
/// sector_count)` ranges — the pure, testable core of
/// [`Disc::encrypted_content_ranges`]. Reuses [`crate::udf::merge_ranges`].
fn merged_extents<'a>(extents: impl Iterator<Item = &'a Extent>) -> Vec<(u32, u32)> {
    let mut ranges: Vec<(u32, u32)> = extents.map(|e| (e.start_lba, e.sector_count)).collect();
    ranges.sort_by_key(|r| r.0);
    crate::udf::merge_ranges(&ranges)
}

// Per-file stream extents in file order; a `None` start is an unrecorded hole.
type StreamFiles = Vec<Vec<(Option<u32>, u32)>>;

// The /BDMV/STREAM walk: every locatable file's extents, plus the files that are not.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct StreamScan {
    pub(crate) files: StreamFiles,
    pub(crate) unmapped: Vec<crate::sector::bus_removal::UnmappedStreamFile>,
}

// File Entry re-reads: at most this many per file, and per scan in all (a count, not a
// timeout: a drive in its fast-fail state answers every read at once).
const FE_REREADS_PER_FILE: u32 = 2;
const FE_REREADS_PER_SCAN: u32 = 4;
// The engine's FAIL_PAUSE_SECS: let the drive settle before each re-read.
const FE_REREAD_PAUSE: std::time::Duration = std::time::Duration::from_secs(5);

// The re-read allowance and pause for one /BDMV/STREAM walk; `halt` ends a pause early.
pub(crate) struct FeRereads {
    left: u32,
    pause: std::time::Duration,
    halt: Option<crate::halt::Halt>,
}

impl FeRereads {
    pub(crate) fn new(halt: Option<crate::halt::Halt>) -> Self {
        Self::with_pause(halt, FE_REREAD_PAUSE)
    }

    fn with_pause(halt: Option<crate::halt::Halt>, pause: std::time::Duration) -> Self {
        Self {
            left: FE_REREADS_PER_SCAN,
            pause,
            halt,
        }
    }

    // A Stop during the pause is `Halted`; with no token nothing can cancel it.
    fn pause(&self) -> Result<()> {
        match &self.halt {
            Some(h) => h.wait(self.pause),
            None => crate::halt::Halt::new().wait(self.pause),
        }
    }
}

// Sends every read as a recovery read (FUA on re-reads, past the drive cache) and notes a
// failed read and whether its sense is the wedge family, so a parse failure is not re-read.
struct RecoveryReads<'a> {
    inner: &'a mut dyn SectorSource,
    fua: bool,
    read_failed: bool,
    wedged: bool,
}

impl<'a> RecoveryReads<'a> {
    fn new(inner: &'a mut dyn SectorSource, fua: bool) -> Self {
        Self {
            inner,
            fua,
            read_failed: false,
            wedged: false,
        }
    }
}

impl SectorSource for RecoveryReads<'_> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }
    fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], _r: bool) -> Result<usize> {
        let got = self.inner.read_sectors_fua(lba, n, buf, true, self.fua);
        if let Err(e) = &got
            && !matches!(e, Error::Halted)
        {
            self.read_failed = true;
            self.wedged |= e.scsi_sense().is_some_and(|s| {
                crate::scsi::SenseFamily::from_sense_key(s.sense_key).is_wedge_family()
            });
        }
        got
    }
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
    }
}

// A read fault a re-read may clear: not a dead transport, a gone source or an image's end.
fn rereadable(e: &Error) -> bool {
    !(e.is_scsi_transport_failure()
        || e.is_source_terminated()
        || matches!(e, Error::Halted | Error::ImageEndsBeforeRead { .. }))
}

// A File Entry's extents. A media read fault gets up to FE_REREADS_PER_FILE recovery+FUA
// re-reads from the scan's budget, each after a Stop-aware pause; wedge-family sense
// (HARDWARE ERROR / ILLEGAL REQUEST, the BU40N fast-fail state) gets none.
fn file_entry_extents(
    reader: &mut dyn SectorSource,
    udf_fs: &udf::UdfFs,
    icb: u32,
    rereads: &mut FeRereads,
) -> Result<Vec<udf::AbsExtent>> {
    let mut first = RecoveryReads::new(reader, false);
    let mut err = match udf_fs.extents_abs_at(&mut first, icb) {
        Ok(exts) => return Ok(exts),
        Err(e) if !first.read_failed || first.wedged || !rereadable(&e) => return Err(e),
        Err(e) => e,
    };
    for _ in 0..FE_REREADS_PER_FILE {
        if rereads.left == 0 {
            break;
        }
        rereads.left -= 1;
        tracing::info!(target: "freemkv::scan", icb, error = %err, "File Entry read failed; re-reading");
        rereads.pause()?;
        let mut again = RecoveryReads::new(reader, true);
        match udf_fs.extents_abs_at(&mut again, icb) {
            Ok(exts) => return Ok(exts),
            Err(e) if !again.read_failed || again.wedged || !rereadable(&e) => return Err(e),
            Err(e) => err = e,
        }
    }
    Err(err)
}

// The whole-disc bus map: every stream file, plus title extents no stream file
// covers as content of unknown unit alignment (always de-bussed).
fn bus_map(files: StreamFiles, titles: &[DiscTitle]) -> crate::sector::bus_removal::BusMap {
    let unknown: Vec<(u32, u32)> = titles
        .iter()
        .flat_map(|t| &t.extents)
        .map(|e| (e.start_lba, e.sector_count))
        .collect();
    crate::sector::bus_removal::BusMap::new(files, &unknown)
}

// Corrects a title's TrueHD channels/sample-rate/Atmos by probing the first decrypted major
// sync (`reader` must yield DECRYPTED sectors: mux time, not scan).
pub(crate) fn correct_truehd_channels(reader: &mut dyn SectorSource, title: &mut DiscTitle) {
    use crate::mux::codec::truehd::{
        truehd_channels, truehd_lfe, truehd_sample_rate_hz, truehd_sync_info_from_stream,
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
    // Anchor the AACS unit-alignment gate to the title's start before probing;
    // otherwise DecryptingSectorSource's absolute `start_lba % 3` gate trips
    // DecryptFailed and TrueHD channels never get corrected. No-op for CSS/unencrypted.
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
            let lfe = truehd_lfe(info.format_info);
            let new_ch = match AudioChannels::from_layout(count.saturating_sub(lfe), lfe) {
                AudioChannels::Unknown => AudioChannels::from_count(count),
                named => named,
            };
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
    // Overlap each bad range against every extent individually — a single
    // bounding box would count inter-extent gaps (other titles' data, BDMV
    // metadata) as bad bytes, over-counting lost_ms for non-contiguous titles.
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

// Byte offset of `lba` within `title`'s extents (concatenated in order), or
// `None` if outside every extent — maps a disc LBA into the title's virtual
// contiguous stream. (Moved from autorip — clients must not re-derive it.)
fn byte_offset_in_title(lba: u32, title: &DiscTitle) -> Option<u64> {
    use crate::consts::SECTOR_BYTES_U64;
    let mut cumulative = 0u64;
    for ext in &title.extents {
        // Saturating like other extent-end computations (ECMA-167 LBAs are
        // 32-bit); a malformed extent near u32::MAX would otherwise panic in
        // debug or wrap to a tiny end LBA in release, hiding the true offset.
        let ext_end = ext.start_lba.saturating_add(ext.sector_count);
        if lba >= ext.start_lba && lba < ext_end {
            return Some(cumulative + (lba - ext.start_lba) as u64 * SECTOR_BYTES_U64);
        }
        cumulative += ext.sector_count as u64 * SECTOR_BYTES_U64;
    }
    None
}

/// The 1-based chapter index + movie-time offset a byte position within a title
/// falls in, or `None` if the title has no size/chapters. Pure helper for the
/// range→chapter/time annotation the progress drilldown ([`locate_ranges`])
/// renders — also used by autorip's done-card range annotation. (Formerly lived
/// in the removed standalone sector-verify module.)
pub fn chapter_at_offset(
    chapters: &[Chapter],
    byte_offset: u64,
    duration_secs: f64,
    total_bytes: u64,
) -> Option<(usize, f64)> {
    if total_bytes == 0 || chapters.is_empty() {
        return None;
    }
    let time_secs = byte_offset as f64 / total_bytes as f64 * duration_secs;
    let mut chapter_idx = 0;
    for (i, ch) in chapters.iter().enumerate() {
        if ch.time_secs <= time_secs {
            chapter_idx = i;
        } else {
            break;
        }
    }
    Some((chapter_idx + 1, time_secs))
}

/// The 1-based chapter + movie-time offset an LBA falls in, or `(None, None)`
/// if it isn't inside the title.
fn range_chapter(lba: u32, title: &DiscTitle) -> (Option<u32>, Option<f64>) {
    if let Some(byte_offset) = byte_offset_in_title(lba, title)
        && let Some((ch, t)) = chapter_at_offset(
            &title.chapters,
            byte_offset,
            title.duration_secs,
            title.size_bytes,
        )
    {
        return (Some(ch as u32), Some(t));
    }
    (None, None)
}

/// Annotate raw bad byte-ranges with chapter + movie time, producing the
/// rendered drilldown ([`crate::progress::LocatedProgress`]) a client draws.
/// `raw` is the mapfile's `(byte_pos, byte_len)` set for whichever statuses
/// the caller cares about (the live "Maybe" set, or terminal `Unreadable`).
/// Sorted largest-movie-time first and capped at 50; `truncated` reports the
/// overflow. `bps` (title bytes/sec) is derived from the title so callers
/// don't thread it. Single place range→chapter/time annotation happens.
pub fn locate_ranges(raw: &[(u64, u64)], title: &DiscTitle) -> crate::progress::LocatedProgress {
    use crate::consts::{MILLIS_PER_SEC, SECTOR_BYTES_U64};
    use crate::progress::{LocatedProgress, LocatedRange};
    const MAX_LOCATED: usize = 50;
    let bps = if title.duration_secs > 0.0 {
        title.size_bytes as f64 / title.duration_secs
    } else {
        0.0
    };
    let num_ranges = raw.len() as u32;
    let mut ranges: Vec<LocatedRange> = raw
        .iter()
        .map(|(pos, size)| {
            let lba = pos / SECTOR_BYTES_U64;
            let count = (size / SECTOR_BYTES_U64) as u32;
            let duration_ms = if bps > 0.0 {
                (*size as f64) / bps * MILLIS_PER_SEC
            } else {
                0.0
            };
            let (chapter, time_offset_secs) = range_chapter(lba as u32, title);
            LocatedRange {
                lba,
                count,
                duration_ms,
                chapter,
                time_offset_secs,
            }
        })
        .collect();
    ranges.sort_by(|a, b| {
        b.duration_ms
            .partial_cmp(&a.duration_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let largest_gap_ms = ranges.first().map(|r| r.duration_ms).unwrap_or(0.0);
    let truncated = ranges.len().saturating_sub(MAX_LOCATED) as u32;
    ranges.truncate(MAX_LOCATED);
    // At-risk movie time = duration of the ranges that intersect the title
    // extents (the others are menus/extras → no movie impact).
    let main_at_risk_ms = if bps > 0.0 {
        bytes_bad_in_title(title, raw) as f64 * MILLIS_PER_SEC / bps
    } else {
        0.0
    };
    LocatedProgress {
        ranges,
        num_ranges,
        truncated,
        main_at_risk_ms,
        largest_gap_ms,
    }
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
        use crate::consts::coding_type as c;
        match ct {
            c::HEVC => Codec::Hevc,
            // 0x1B base-view AVC and 0x20 MVC dependent-view (Blu-ray 3D right
            // eye) are both H.264; mapping 0x20 to video lets the PMT scan
            // enumerate the dependent view as its own SSIF stream — the basis of 3D.
            c::H264 | c::H264_MVC => Codec::H264,
            c::VC1 => Codec::Vc1,
            c::MPEG2_VIDEO => Codec::Mpeg2,
            c::TRUEHD => Codec::TrueHd,
            c::DTS_HD_MA => Codec::DtsHdMa,
            c::DTS_HD_HR => Codec::DtsHdHr,
            c::DTS => Codec::Dts,
            c::AC3 => Codec::Ac3,
            c::AC3_PLUS | c::AC3_PLUS_SECONDARY => Codec::Ac3Plus,
            c::LPCM => Codec::Lpcm,
            // 0xA2 is the SECONDARY DTS-HD stream: DTS Express / DTS-HD LBR, a
            // LOSSY low-bitrate extension (parallel to 0xA1 secondary E-AC-3), NOT
            // DTS-HD MA (0x86); mapping it as lossless would misreport quality.
            c::DTS_HD_SECONDARY => Codec::DtsHdHr,
            // PG (0x90) = Presentation Graphics (subtitles). IG (0x91, menus) and
            // TEXT_SUBTITLE (0x92) are distinct types and fall through to Unknown
            // so the PMT/STN walker drops them instead of faking a PGS track.
            c::PG => Codec::Pgs,
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

    /// Pixel dimensions (width, height). `None` when the resolution is
    /// [`Resolution::Unknown`] — "no dimensions", not a guess. SD frames are
    /// the DVD-Video coded pictures (BT.601 525/60, 625/50 active area); HD
    /// frames are the BD-ROM part 3 video formats; 3840x2160 is UHD BD.
    /// `Unknown` deliberately does NOT fabricate a plausible 1920x1080 or
    /// `(0, 0)`: every caller must positively check the variant, and each
    /// makes a DIFFERENT decision on `None` (Matroska/VobSub omit the field,
    /// MP4 must refuse per ISO/IEC 14496-12's mandatory width/height).
    pub fn pixels(&self) -> Option<(u32, u32)> {
        match self {
            Resolution::R480i | Resolution::R480p => Some((720, 480)),
            Resolution::R576i | Resolution::R576p => Some((720, 576)),
            Resolution::R720p => Some((1280, 720)),
            Resolution::R1080i | Resolution::R1080p => Some((1920, 1080)),
            Resolution::R2160p => Some((3840, 2160)),
            Resolution::R4320p => Some((7680, 4320)),
            Resolution::Unknown => None,
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
            AudioChannels::Stereo21 | AudioChannels::Surround30 => 3,
            AudioChannels::Quad | AudioChannels::Surround31 => 4,
            AudioChannels::Surround50 | AudioChannels::Surround41 => 5,
            AudioChannels::Surround51 | AudioChannels::Surround60 => 6,
            AudioChannels::Surround61 | AudioChannels::Surround70 => 7,
            AudioChannels::Surround71 => 8,
            // 0, not a plausible guess like 6 — a fake count once let the json://
            // sink report a confident 5.1 for audio its own fields called "unknown".
            // 0 is what Matroska/sinks already coerce Unknown to, and is obviously wrong.
            AudioChannels::Unknown => 0,
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

    /// Exact layout from full-range (heights included) and LFE channel counts,
    /// the `N.M` label; `Unknown` when no variant names it.
    pub(crate) fn from_layout(full: u8, lfe: u8) -> Self {
        match (full, lfe) {
            (1, 0) => AudioChannels::Mono,
            (2, 0) => AudioChannels::Stereo,
            (2, 1) => AudioChannels::Stereo21,
            (3, 0) => AudioChannels::Surround30,
            (3, 1) => AudioChannels::Surround31,
            (4, 0) => AudioChannels::Quad,
            (4, 1) => AudioChannels::Surround41,
            (5, 0) => AudioChannels::Surround50,
            (5, 1) => AudioChannels::Surround51,
            (6, 0) => AudioChannels::Surround60,
            (6, 1) => AudioChannels::Surround61,
            (7, 0) => AudioChannels::Surround70,
            (7, 1) => AudioChannels::Surround71,
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
            // 0.0, not 48000.0 — see AudioChannels::count. A fabricated rate is
            // indistinguishable from a real one; a zero is not.
            SampleRate::Unknown => 0.0,
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
// Each enum defines a const array of (str, variant) pairs; Display, FromStr,
// and id() all derive from this one table, so no string appears twice.

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
                // Unknown is kept out of ALL so FromStr("unknown") round-trips via
                // $default without a duplicate key; display it visibly rather than
                // an empty string, which produced blank metadata in labels/logs.
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
        ("3.0", AudioChannels::Surround30),
        ("3.1", AudioChannels::Surround31),
        ("4.0", AudioChannels::Quad),
        ("4.1", AudioChannels::Surround41),
        ("5.0", AudioChannels::Surround50),
        ("5.1", AudioChannels::Surround51),
        ("6.0", AudioChannels::Surround60),
        ("6.1", AudioChannels::Surround61),
        ("7.0", AudioChannels::Surround70),
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

    /// The title's audio streams, in declared order. Cleaner than matching on
    /// the [`Stream`] enum for the common "iterate the audio tracks" case
    /// (stream selection, the desktop UI's info panel, disc-info listing).
    pub fn audio_streams(&self) -> impl Iterator<Item = &AudioStream> {
        self.streams.iter().filter_map(|s| match s {
            Stream::Audio(a) => Some(a),
            _ => None,
        })
    }

    /// The title's subtitle streams, in declared order.
    pub fn subtitle_streams(&self) -> impl Iterator<Item = &SubtitleStream> {
        self.streams.iter().filter_map(|s| match s {
            Stream::Subtitle(s) => Some(s),
            _ => None,
        })
    }

    /// The title's video streams, in declared order (usually one; two for a
    /// Blu-ray 3D MVC title — the base view plus the dependent view).
    pub fn video_streams(&self) -> impl Iterator<Item = &VideoStream> {
        self.streams.iter().filter_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
    }

    /// Whether this title carries at least one primary video stream. A title
    /// with no video can never mux a feature — a playlist-obfuscation "decoy"
    /// (long/large but streamless) is the canonical case — so main-feature
    /// selection demotes it below every title that has video
    /// (see [`Disc::main_feature_order`]).
    pub fn has_video(&self) -> bool {
        self.video_streams().next().is_some()
    }

    /// Whether this title PLAUSIBLY carries feature video content — a real
    /// primary video stream, OR a size/duration profile only video explains
    /// (at least 2 GB and 5 minutes). Deliberately MORE permissive than
    /// [`Self::has_video`] (only ever ADMITS a title the stricter gate would
    /// reject) so a bumper-led feature isn't wrongly disqualified, while a
    /// clip-less pure-metadata decoy is still rejected. Used as the
    /// `has-video` key by [`Disc::main_feature_order`].
    pub fn has_probable_video(&self) -> bool {
        self.has_video()
            || (!self.clips.is_empty()
                && self.size_bytes >= 2_000_000_000
                && self.duration_secs >= 300.0)
    }
}

// ─── Encryption ─────────────────────────────────────────────────────────────

/// AACS decryption state for a disc.
pub struct AacsState {
    /// AACS version (1 or 2)
    pub version: u8,
    /// Whether bus encryption is enabled (always true for AACS 2.0 / UHD)
    pub bus_encryption: bool,
    /// MKB version from disc (e.g. 68, 77)
    pub mkb_version: Option<u32>,
    /// Disc hash (SHA1 of Unit_Key_RO.inf) -- hex string with 0x prefix
    pub disc_hash: String,
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

// Redacting `Debug`: `AacsState` is public via `Disc.aacs` and carries VUK /
// unit keys / bus key / volume id / raw .inf + MKB. Print only non-secret shape;
// redact every key/secret field. Guarded by `aacs_state_and_key_debug_are_redacted`.
impl std::fmt::Debug for AacsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AacsState")
            .field("version", &self.version)
            .field("bus_encryption", &self.bus_encryption)
            .field("mkb_version", &self.mkb_version)
            .field("disc_hash", &self.disc_hash)
            .field("volume_id", &"<redacted>")
            .field("uk_ro_len", &self.uk_ro.len())
            .field("mkb_len", &self.mkb.len())
            .finish()
    }
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

// No `KeyOrigin::name()`: the library holds ZERO user-facing English. Callers
// map variants to display text (see freemkv's `disc_info::key_origin_label`).

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
    pub host_certs: Vec<crate::aacs::types::HostCert>,
}

/// Options for disc scanning.
///
/// libfreemkv is lookup-free — it resolves no keys. The caller resolves a key
/// out-of-band through [`ResolvedKeySet::resolve`](crate::keys::ResolvedKeySet::resolve). The
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
    /// `ResolvedKeySet::resolve`).
    pub key_sources: Vec<Box<dyn crate::KeySource>>,
    /// Optional cooperative-cancellation token. When set, long scan-time
    /// loops (notably the CSS known-plaintext crack, which can scan up to
    /// 50_000 sectors on a live DVD) poll it and bail out cleanly so a
    /// scan-phase watchdog or operator Stop is never stuck behind a hang.
    pub halt: Option<crate::halt::Halt>,
    /// Read the PGS subtitle streams during the scan to detect forced-narrative
    /// tracks from their content (the `forced_on_flag`), matching what the mux
    /// derives during a rip. OFF by default — it reads the clip's PGS content,
    /// which is slow, so only callers that want authoritative forced flags in the
    /// scanned title (e.g. `freemkv info`) opt in. The rip path leaves it off:
    /// the muxer detects forced during muxing without a second read.
    pub probe_forced_subtitles: bool,
    /// The caller copies sectors raw and never decrypts or accepts keys (a raw
    /// disc→ISO copy). A live AACS disc whose `Unit_Key_RO.inf` is unreadable then
    /// still scans, with [`Error::AacsKeyFileUnreadable`] recorded and every key refused;
    /// otherwise `Disc::scan` returns that error.
    pub raw_copy: bool,
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
        // Authoritative here — the same MKB-driven detector the full scan uses
        // (no titles needed: BD/UHD/FMTS come from the MKB generation). It no
        // longer defaults to BluRay or defers UHD/FMTS to the full scan.
        let format = Self::detect_disc_format(&mut buffered, &udf_fs, &[]);
        let encrypted = aacs_dir_present(&udf_fs);
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
    fn read_udf(
        session: &mut Drive,
    ) -> Result<(u32, udf::BufferedSectorReader<'_, Drive>, udf::UdfFs)> {
        // READ CAPACITY is the sole authoritative whole-disc size (the UDF partition
        // is a subset — 288 sectors short on a real BD, truncating the backup anchor).
        // Its sporadic failures are ridden out by `read_capacity_retrying` (0 on hard fail).
        let capacity = Self::read_capacity_retrying(session)?;
        let (buffered, udf_fs) = Self::open_udf(session)?;
        Ok((capacity, buffered, udf_fs))
    }

    // UDF filesystem + metadata-prefetched reader, without the READ CAPACITY step.
    fn open_udf(session: &mut Drive) -> Result<(udf::BufferedSectorReader<'_, Drive>, udf::UdfFs)> {
        let batch = detect_max_batch_sectors(session.device_path());
        let mut buffered = udf::BufferedSectorReader::new(session, batch);
        let udf_fs = udf::read_filesystem(&mut buffered)?;
        buffered.prefetch(udf_fs.metadata_start(), udf_fs.metadata_sectors())?;
        Ok((buffered, udf_fs))
    }

    /// Number of READ CAPACITY attempts before giving up. The command answers
    /// reliably on a healthy drive (measured 5/5 on the target); the field
    /// failure this rides out — GOOD-then-empty / host_status set, no sense,
    /// intermittent, succeeds on retry — is the USB/UAS transport + spin-up
    /// timing signature. Six attempts covers a cold spin-up plus one USB
    /// re-enumeration; needing more means a bad disc or drive, not a blip.
    const READ_CAPACITY_ATTEMPTS: u32 = 6;

    /// First backoff after a failed READ CAPACITY. Backoff grows exponentially
    /// (double each miss) up to [`Self::READ_CAPACITY_BACKOFF_CAP`]. The floor is
    /// well above one revolution — an optical drive can only usefully retry
    /// about once per rotation, so sub-100ms hammering buys nothing.
    const READ_CAPACITY_BACKOFF_START: std::time::Duration = std::time::Duration::from_millis(200);

    /// Cap on the exponential backoff between READ CAPACITY attempts. With six
    /// attempts the sleeps run 200/400/800/1600/2000 ms — a ~5s total budget,
    /// comfortably inside a cold spin-up + one re-enumeration.
    const READ_CAPACITY_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(2);

    /// READ CAPACITY with retry: the hardware sector count, or 0 once retries are
    /// exhausted or the drive refuses deterministically (`read_capacity_is_permanent`).
    /// It is the only authoritative whole-disc size (a UDF partition size would
    /// truncate an image), and its field failures are sporadic, so they are ridden out
    /// with exponential backoff. A 0 keeps scan/identify/MKV lenient; imaging turns it
    /// into [`Error::EmptyImage`] via `image_read_sectors`. A Stop ([`Error::Halted`],
    /// including one during a backoff sleep) is returned, never retried.
    fn read_capacity_retrying(session: &mut Drive) -> Result<u32> {
        Self::read_capacity_retrying_with(session, |d, t| d.pause(t))
    }

    // `sleep` is the backoff wait (injectable for tests); it returns Halted on a Stop.
    fn read_capacity_retrying_with(
        session: &mut Drive,
        mut sleep: impl FnMut(&Drive, std::time::Duration) -> Result<()>,
    ) -> Result<u32> {
        let mut backoff = Self::READ_CAPACITY_BACKOFF_START;
        for attempt in 1..=Self::READ_CAPACITY_ATTEMPTS {
            match Self::read_capacity(session) {
                Ok(sectors) => return Ok(sectors),
                Err(Error::Halted) => return Err(Error::Halted),
                Err(e)
                    if attempt == Self::READ_CAPACITY_ATTEMPTS
                        || Self::read_capacity_is_permanent(&e) =>
                {
                    let permanent = Self::read_capacity_is_permanent(&e);
                    tracing::warn!(
                        target: "freemkv::scan",
                        error = %e,
                        attempts = attempt,
                        reason = if permanent { "permanent" } else { "retries_exhausted" },
                        "READ CAPACITY gave up; disc capacity unavailable (image/ISO output disabled; scan/identify/MKV read by extent and continue)"
                    );
                    return Ok(0);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::scan",
                        error = %e,
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        "READ CAPACITY failed; retrying after backoff"
                    );
                    sleep(session, backoff)?;
                    backoff = (backoff * 2).min(Self::READ_CAPACITY_BACKOFF_CAP);
                }
            }
        }
        Ok(0)
    }

    // Retrying cannot change these: the drive rejects the command, or has no medium.
    fn read_capacity_is_permanent(e: &Error) -> bool {
        e.scsi_sense().is_some_and(|s| {
            s.sense_key == crate::scsi::SENSE_KEY_ILLEGAL_REQUEST
                || (s.sense_key == crate::scsi::SENSE_KEY_NOT_READY && s.asc == 0x3A)
        })
    }

    /// Scan a disc — parse filesystem, playlists, streams, and capture AACS
    /// inputs; the session must be open. Order: (1) DVD only: CSS bus-auth, (2) read
    /// capacity + UDF, (3) the AACS files (`Unit_Key_RO.inf`, content cert, MKB) with
    /// plain READs, (4) the AACS handshake if AACS, (5) titles + labels.
    ///
    /// A live AACS disc whose `Unit_Key_RO.inf` cannot be read fails with
    /// [`Error::AacsKeyFileUnreadable`] before any AACS command (see
    /// [`ScanOptions::raw_copy`]). A dead bus during a handshake aborts the scan.
    /// `opts.halt` is the scan's op token unless the drive has one attached (§2.2).
    pub fn scan(session: &mut Drive, opts: &ScanOptions) -> Result<Self> {
        let mut session = session.alias(opts.halt.as_ref());
        let disc = Self::scan_live(&mut session, opts)?;
        // LS6: a Stop after the last CDB still ends the scan; no `Disc` is returned.
        session.check_token()?;
        Ok(disc)
    }

    fn scan_live(session: &mut Drive, opts: &ScanOptions) -> Result<Self> {
        let dvd = session.disc_is_dvd();
        // Max read speed; removes riplock on DVD.
        session.set_speed(0xFFFF);
        // CSS bus-auth before any read, else the UDF prefetch hits scrambled VOB extents.
        if dvd {
            Self::css_bus_step(session)?;
        }

        tracing::info!(target: "freemkv::scan", "phase: reading UDF filesystem");
        let (capacity, mut buffered, udf_fs) = Self::read_udf(session)?;
        tracing::info!(target: "freemkv::scan", capacity, "phase: UDF read");
        // Pre-read small files (AACS, MPLS, CLPI, META, *.bdmv): one command each otherwise.
        match udf_fs.metadata_sector_ranges(&mut buffered) {
            Ok(ranges) => buffered.prefetch_ranges(&ranges)?,
            Err(Error::Halted) => return Err(Error::Halted),
            Err(_) => {} // prefetch is optional
        }

        let aacs = if aacs_dir_present(&udf_fs) {
            // A DVD with /AACS gets no handshake, so its key file keeps the image rule.
            let from = if dvd {
                encrypt::CaptureFrom::Image
            } else {
                encrypt::CaptureFrom::Live {
                    raw_copy: opts.raw_copy,
                }
            };
            let cap = encrypt::capture(&mut buffered, &udf_fs, from)?;
            let bus = if !dvd {
                tracing::info!(target: "freemkv::scan", "phase: AACS handshake");
                encrypt::aacs_bus_step(buffered.inner_mut(), opts)?
            } else if buffered.inner_mut().unlocker_name().is_some() {
                encrypt::BusOutcome::FirmwareUnlocked
            } else {
                encrypt::BusOutcome::FileOrIso
            };
            Some((cap, bus))
        } else {
            None
        };
        Self::live_finish(buffered, capacity, udf_fs, aacs, opts)
    }

    // `finish` over the live reader, then the drive's bus wiring from the bus outcome.
    fn live_finish(
        mut buffered: udf::BufferedSectorReader<'_, Drive>,
        capacity: u32,
        udf_fs: udf::UdfFs,
        aacs: Option<(encrypt::AacsCapture, encrypt::BusOutcome)>,
        opts: &ScanOptions,
    ) -> Result<Self> {
        let bus_key = aacs.as_ref().and_then(|(c, b)| encrypt::bus_key(c, b));
        // The op token: the drive's attached token (the `opts.halt` alias or the caller's).
        let op = buffered.inner_mut().token().cloned();
        let rereads = FeRereads::new(op.clone());
        let streams = Self::bus_stream_files(&mut buffered, &udf_fs, bus_key.is_some(), rereads)?;

        tracing::info!(target: "freemkv::scan", "phase: parsing titles/streams");
        let disc = Self::finish(&mut buffered, capacity, udf_fs, aacs, opts, op.as_ref())?;
        tracing::info!(target: "freemkv::scan", titles = disc.titles.len(), format = ?disc.content_format, "phase: titles parsed");

        // The SINGLE de-bus point, wired before the caller samples keys or muxes (the
        // metadata reads above ran under Passthrough).
        let session = buffered.into_inner();
        let map = bus_map(streams.files, &disc.titles).with_unmapped(streams.unmapped);
        Self::wire_bus_removal(session, bus_key, map);

        // No CSS key recovery at scan time: DVD CSS keys are re-cracked keylessly at read time.
        tracing::info!(target: "freemkv::scan", format = ?disc.format, titles = disc.titles.len(), "phase: scan complete");
        Ok(disc)
    }

    // CSS bus-auth for a live DVD. A dead bus aborts like `Drive::init`; a Stop is `Halted`.
    fn css_bus_step(session: &mut Drive) -> Result<()> {
        tracing::info!(target: "freemkv::scan", "phase: CSS — bus-auth unlock (pre-scan)");
        let drive_id = session.drive_id.clone();
        let (_, res) = encrypt::bus_step_guard(session, |s| {
            crate::unlock_bridge::run_bus(s, &drive_id, freemkv_unlock::DiscKind::Css, &[])
        })?;
        match res {
            Ok(Some(_)) => {}
            Ok(None) => tracing::warn!(
                target: "freemkv::scan",
                "CSS bus-auth unlock declined; scrambled sectors may be unavailable"
            ),
            Err(freemkv_unlock::UnlockError::Transport) => {
                return Err(crate::unlock_bridge::unlock_transport_error());
            }
            Err(e) => tracing::warn!(
                target: "freemkv::scan",
                outcome = ?e,
                "CSS bus-auth unlock did not apply; scrambled sectors may be unavailable"
            ),
        }
        Ok(())
    }

    /// The AACS stream files' sectors (every file under `/BDMV/STREAM`: m2ts,
    /// SSIF, fmts) as sorted, merged `(start_lba, sector_count)` ranges — the
    /// whole-disc encrypted-content map, unlike the title-only
    /// [`Self::encrypted_content_ranges`]. Reads the UDF tree from `reader`.
    ///
    /// Fails closed with [`Error::BusStreamUnmapped`], naming them, when any stream
    /// file's extents cannot be read: a map that silently omits one is not this map.
    pub fn stream_content_ranges(reader: &mut dyn SectorSource) -> Result<Vec<(u32, u32)>> {
        let udf_fs = udf::read_filesystem(reader)?;
        let scan = Self::stream_file_extents(reader, &udf_fs, FeRereads::new(None))?;
        crate::sector::bus_removal::unmapped_error(&scan.unmapped)?;
        Ok(crate::sector::bus_removal::BusMap::new(scan.files, &[]).covered_ranges())
    }

    /// The sectors a staged image for an MKV rip of `titles` (indices into
    /// [`Self::titles`]) needs: everything outside `/BDMV/STREAM` (UDF structures,
    /// nav, AACS files) plus those titles' extents, sorted and merged. Reads the UDF
    /// tree from `reader`.
    ///
    /// None of it is bus-encrypted once read through a scanned drive: nav and UDF carry
    /// BEF=0 (AACS BD Pre-recorded 0.953 §3.7), and every title extent is in the bus
    /// map. So the image holds no bus-encrypted byte even when a stream file is unmapped.
    /// Per spec; do not change without a spec citation proving otherwise.
    pub fn mkv_staging_ranges(
        &self,
        reader: &mut dyn SectorSource,
        titles: &[usize],
    ) -> Result<Vec<(u32, u32)>> {
        let udf_fs = udf::read_filesystem(reader)?;
        let mut ranges = udf_fs.non_stream_ranges(reader)?;
        for &i in titles {
            let t = self.titles.get(i).ok_or(Error::DiscTitleRange {
                index: i,
                count: self.titles.len(),
            })?;
            ranges.extend(t.extents.iter().map(|e| (e.start_lba, e.sector_count)));
        }
        ranges.retain(|&(_, n)| n > 0);
        ranges.sort_by_key(|r| r.0);
        Ok(crate::udf::merge_ranges(&ranges))
    }

    // The stream files a live scan's bus map needs. `debus` = the cert route installs a
    // host-key de-bus stage; otherwise the map de-busses nothing, so skip the tree walk.
    fn bus_stream_files(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        debus: bool,
        rereads: FeRereads,
    ) -> Result<StreamScan> {
        if !debus {
            return Ok(StreamScan::default());
        }
        Self::stream_file_extents(reader, udf_fs, rereads)
    }

    /// Extents, in file order, of every file under /BDMV/STREAM (the AACS Clip AV
    /// stream files); an unrecorded extent is a `None` hole that keeps file offsets.
    ///
    /// A file whose extents cannot be read is recorded in `unmapped` (its sectors stay
    /// bus-encrypted, so whole-disc images must refuse it) and the walk goes on; a Stop
    /// returns [`Error::Halted`]. An embedded file (§3.10.1) holds no Aligned Unit: skipped.
    /// Per spec; do not change without a spec citation proving otherwise.
    pub(crate) fn stream_file_extents(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        mut rereads: FeRereads,
    ) -> Result<StreamScan> {
        let mut out = StreamScan::default();
        let mut stack: Vec<(&udf::DirEntry, String)> = udf_fs
            .find_dir("/BDMV/STREAM")
            .map(|d| (d, "/BDMV/STREAM".to_string()))
            .into_iter()
            .collect();
        while let Some((dir, dir_path)) = stack.pop() {
            for e in &dir.entries {
                let path = format!("{dir_path}/{}", e.name);
                if e.is_dir {
                    stack.push((e, path));
                    continue;
                }
                match file_entry_extents(reader, udf_fs, e.meta_lba, &mut rereads) {
                    Ok(exts) => out.files.push(
                        exts.iter()
                            .filter(|x| x.len > 0)
                            .map(|x| {
                                let n = (x.len as u64).div_ceil(2048) as u32;
                                (x.recorded.then_some(x.lba), n)
                            })
                            .collect(),
                    ),
                    // §3.10.1: "The total size of an Aligned Unit is 6144 bytes, which is
                    // equal to the size of 3 logical sectors." Embedded data holds none.
                    Err(Error::UdfEmbeddedData) => tracing::debug!(
                        target: "freemkv::scan",
                        file = %path,
                        "embedded stream file: no Aligned Unit to de-bus"
                    ),
                    // An operator Stop, not a disc fault: end the walk, record nothing.
                    Err(Error::Halted) => return Err(Error::Halted),
                    // §3.7 Note: "PC Host shall decrypt bus-encrypted Clip AV stream file";
                    // unlocated, this file cannot be, so image outputs must refuse it.
                    Err(err) => {
                        tracing::warn!(
                            target: "freemkv::scan",
                            file = %path,
                            code = err.code(),
                            error = %err,
                            "stream file's File Entry unreadable; its sectors cannot be de-bussed"
                        );
                        let lost = crate::sector::bus_removal::UnmappedStreamFile::new;
                        out.unmapped.push(lost(path, e.meta_lba, &err));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Wire the SINGLE AACS bus-removal de-bus point onto `session` from a
    /// completed handshake. The cert-route Read Data Key (`bus_key`, `None` on
    /// the firmware/vendor route) picks the `BusStage`; the stream-file bus map
    /// gates it. ALWAYS install the map even when empty: no map means "de-bus
    /// every sector", an empty map means "de-bus none" — so a host-key disc with
    /// no stream files fails SAFE (untouched) instead of de-bussing clear UDF/nav
    /// bytes to garbage.
    pub(crate) fn wire_bus_removal(
        session: &mut Drive,
        bus_key: Option<[u8; 16]>,
        map: crate::sector::bus_removal::BusMap,
    ) {
        session.set_bus_stage(crate::sector::bus_removal::BusStage::from_read_data_key(
            bus_key,
        ));
        session.set_bus_map(std::sync::Arc::new(map));
    }

    // The extents an image-time CSS crack scans, in the crate's CANONICAL order (main feature's
    // own extents, playback order) — do not re-derive this inline.
    fn image_crack_extents(titles: &[DiscTitle]) -> &[Extent] {
        // Prefer the first title with BOTH video and extents (the main feature)
        // so the CSS crack scans the movie, not a streamless decoy. Falls back
        // to the first title with extents when none has video.
        titles
            .iter()
            .find(|t| t.has_probable_video() && !t.extents.is_empty())
            .or_else(|| titles.iter().find(|t| !t.extents.is_empty()))
            .map(|t| t.extents.as_slice())
            .unwrap_or(&[])
    }

    /// Scan a disc image (ISO or any SectorSource). No SCSI, no handshake.
    /// AACS resolution uses KEYDB VUK lookup only.
    pub fn scan_image(
        reader: &mut dyn SectorSource,
        capacity: u32,
        opts: &ScanOptions,
    ) -> Result<Self> {
        let udf_fs = udf::read_filesystem(reader)?;
        let mut disc = Self::scan_fs(reader, capacity, opts, udf_fs)?;

        // CSS for a raw (still-scrambled) DVD image: recover the title key via
        // known-plaintext crack (no SCSI auth needed). Gated on `DiscFormat::Dvd`,
        // not `MpegPs`, since HD-DVD `.evo` images are also MPEG-PS but AACS.
        if disc.css.is_none() && disc.format == DiscFormat::Dvd && !disc.titles.is_empty() {
            // Copied out so the crack's `disc.css`/`disc.encrypted` writes below
            // don't collide with a live borrow of `disc.titles`. Order is exactly
            // what `image_crack_extents` returns — never re-sorted here.
            let main_extents = Self::image_crack_extents(&disc.titles).to_vec();
            if !main_extents.is_empty() {
                // Image reads aren't drive-batch-limited; use a generous batch.
                match crate::css::crack_key_outcome(reader, &main_extents, 32, opts.halt.as_ref()) {
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
                    crate::css::CrackOutcome::Halted => return Err(Error::Halted),
                    // No verdict, but the image stays listable: the per-title crack
                    // (the mux always re-cracks) reports the read fault for that title.
                    crate::css::CrackOutcome::Unreadable(e) => {
                        tracing::warn!(target: "freemkv::scan", code = e.code(), "image css: crack extent unreadable");
                    }
                }
            }
        }

        Ok(disc)
    }

    // Reads a disc's AACS key-input files: (Unit_Key_RO.inf, MKB) raw bytes. Shared body for
    // read_aacs_inputs (ISO) / read_aacs_inputs_from_drive (live drive).
    pub(crate) fn read_aacs_inputs_from_reader(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Result<(Vec<u8>, Vec<u8>, u8)> {
        let inf = crate::aacs::read_first(
            &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::UnitKey),
            |p| udf_fs.read_file(reader, p),
        )?;
        let mkb = Self::read_mkb_content(reader, udf_fs)?;
        let version = Self::read_aacs_version(reader, udf_fs);
        Ok((inf, mkb, version))
    }

    // AACS major version from the content certificate; drives the Unit_Key_RO.inf parse stride.
    // Defaults to UHD (V20, 64-byte stride) when unreadable.
    fn read_aacs_version(reader: &mut dyn SectorSource, udf_fs: &udf::UdfFs) -> u8 {
        match crate::aacs::read_first(
            &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::ContentCert),
            |p| udf_fs.read_file(reader, p),
        )
        .ok()
        .as_deref()
        .and_then(crate::aacs::inf::parse_content_cert)
        {
            Some(c) => c.version.major(),
            None => {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "scan_aacs_version",
                    "no readable AACS content certificate; defaulting to the V20/UHD \
                     Unit_Key_RO stride (a VUK-from-server path would otherwise mis-stride)"
                );
                crate::aacs::mkb::AACS_MAJOR_UHD
            }
        }
    }

    // Reads the AACS MKB's real record stream — NOT its ~128 MiB zero padding. Reads a bounded,
    // growing prefix instead, avoiding both the padding read and the read_file MAX_FILE_BYTES
    // cap.
    fn read_mkb_content(reader: &mut dyn SectorSource, udf_fs: &udf::UdfFs) -> Result<Vec<u8>> {
        const START_BYTES: usize = 16 * 1024 * 1024;
        const MAX_BYTES: usize = 64 * 1024 * 1024;
        let mut want = START_BYTES;
        loop {
            let buf = crate::aacs::read_first(
                &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::Mkb),
                |p| udf_fs.read_file_prefix(reader, p, want),
            )?;
            let n = crate::aacs::mkb::mkb_content_len(&buf);
            // `n` strictly inside `buf` or `buf` shorter than `want` means the
            // whole content is captured; otherwise records may run past the
            // prefix — grow and retry, bounded by MAX_BYTES.
            if (n > 0 && n < buf.len()) || buf.len() < want || want >= MAX_BYTES {
                return Ok(crate::aacs::mkb::trim_mkb(buf));
            }
            want = (want * 2).min(MAX_BYTES);
        }
    }

    /// Read a disc's AACS key-input files from an ISO image: returns
    /// `(Unit_Key_RO.inf, MKB, aacs_major_version)`. For callers that resolve a
    /// Unit Key out-of-band: the key reaches a read only through
    /// [`ResolvedKeySet`](crate::keys::ResolvedKeySet). libfreemkv never makes a network call.
    pub fn read_aacs_inputs(iso_path: &std::path::Path) -> Result<(Vec<u8>, Vec<u8>, u8)> {
        // Preserve the underlying open error (E5000) instead of collapsing
        // ENOENT/EPERM into `Error::AacsNoKeys` (E7000): a missing ISO is an
        // I/O fault, not a key-resolution failure, and `.code()` callers must differ.
        let mut reader = crate::io::file_sector_source::FileSectorSource::open(iso_path)?;
        let udf_fs = udf::read_filesystem(&mut reader)?;
        Self::read_aacs_inputs_from_reader(&mut reader, &udf_fs)
    }

    /// Same as [`Disc::read_aacs_inputs`] but over an extracted disc FOLDER.
    ///
    /// `dir://` is an image-level source: `dirimage` synthesizes a real UDF
    /// volume over the folder, so the AACS inputs are read by exactly the same
    /// reader path an ISO uses. Without this the online key fetch was silently
    /// unavailable for folders — the CLI's fetch helper matched only `Iso` and
    /// returned None for a folder, so the same disc that fetched its key fine
    /// as an ISO failed as an extracted directory. A sink is a sink: any input
    /// has to work with any output, and that includes the key path.
    pub fn read_aacs_inputs_from_dir(dir: &std::path::Path) -> Result<(Vec<u8>, Vec<u8>, u8)> {
        let mut reader = crate::dirimage::DirImage::open(dir)?;
        let udf_fs = udf::read_filesystem(&mut reader)?;
        Self::read_aacs_inputs_from_reader(&mut reader, &udf_fs)
    }

    /// Read a disc's **structure metadata** for diagnostics — `(relative_path,
    /// bytes)` for each present index file that drives title enumeration and
    /// main-feature selection: `BDMV/index.bdmv`, `MovieObject.bdmv`,
    /// `PLAYLIST/*.mpls`, `CLIPINF/*.clpi`, `BDJO/*.bdjo`, `META/DL/*.xml`, DVD
    /// `VIDEO_TS/*.IFO`. No audio/video essence and no AACS keys are read, so the
    /// result is safe to share in a bug report (a few hundred KB); missing files
    /// are skipped. Capped at 8192 files / 64 MiB; unsafe disc names are dropped; a
    /// Stop returns [`Error::Halted`]. Powers `freemkv info … --share` (issue #45).
    pub fn read_structure_files(reader: &mut dyn SectorSource) -> Result<Vec<(String, Vec<u8>)>> {
        // Aggregate caps: a crafted image can list thousands of large files.
        const MAX_FILES: usize = 8192;
        const MAX_BYTES: u64 = 64 * 1024 * 1024;

        let udf_fs = udf::read_filesystem(reader)?;

        // `(rel, declared size, ICB)` of the plain-named files in `dir` matching `pred`, sorted.
        // Of case variants keep the first in directory order: the one a read resolves to,
        // and the only one a case-insensitive host can hold.
        let list = |dir: &str, pred: &dyn Fn(&str) -> bool| -> Vec<(String, u64, u32)> {
            let mut seen = std::collections::HashSet::new();
            let mut v: Vec<(String, u64, u32)> = udf_fs
                .find_dir(&format!("/{dir}"))
                .map(|d| {
                    d.entries
                        .iter()
                        .filter(|e| !e.is_dir && is_plain_file_name(&e.name) && pred(&e.name))
                        .filter(|e| seen.insert(e.name.to_ascii_lowercase()))
                        .map(|e| (format!("{dir}/{}", e.name), e.size, e.meta_lba))
                        .collect()
                })
                .unwrap_or_default();
            v.sort();
            v
        };

        // Nav files are bundled under their canonical names, whatever the disc casing.
        let mut wanted = Vec::new();
        for top in ["index.bdmv", "MovieObject.bdmv"] {
            if let Some((_, size, icb)) =
                list("BDMV", &|n: &str| n.eq_ignore_ascii_case(top)).first()
            {
                wanted.push((format!("BDMV/{top}"), *size, *icb));
            }
        }
        for (dir, ext) in [
            ("BDMV/PLAYLIST", ".mpls"),
            ("BDMV/CLIPINF", ".clpi"),
            ("BDMV/BDJO", ".bdjo"),
            ("BDMV/META/DL", ".xml"),
            ("VIDEO_TS", ".ifo"),
        ] {
            wanted.extend(list(dir, &|n: &str| n.to_ascii_lowercase().ends_with(ext)));
        }

        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        let mut total: u64 = 0;
        let mut omitted: usize = 0;
        let wanted_count = wanted.len();
        for (i, (rel, size, icb)) in wanted.into_iter().enumerate() {
            if out.len() >= MAX_FILES {
                omitted += wanted_count - i;
                break;
            }
            if total.saturating_add(size) > MAX_BYTES {
                omitted += 1;
                continue;
            }
            // Bounded by the declared size, so a lying ICB cannot exceed the budget.
            let read = FileReadAhead::new(reader, &udf_fs, icb).and_then(|mut ra| {
                udf_fs.read_file_prefix(&mut ra, &format!("/{rel}"), size as usize)
            });
            match read {
                Ok(bytes) => {
                    total += bytes.len() as u64;
                    out.push((rel, bytes));
                }
                Err(Error::Halted) => return Err(Error::Halted),
                Err(_) => {} // unreadable file: skipped, as when absent
            }
        }
        if omitted > 0 {
            tracing::warn!(target: "freemkv::scan", omitted, files = out.len(), bytes = total, "structure bundle caps reached; files omitted");
        }
        Ok(out)
    }

    /// Same as [`Disc::read_aacs_inputs`] but reads from a live drive. The
    /// out-of-band Unit Key path fetches the disc's key files from the drive,
    /// resolves a key from them however it likes, through
    /// [`ResolvedKeySet`](crate::keys::ResolvedKeySet). These files are plaintext UDF metadata — no
    /// AACS handshake or keys are required to read them.
    pub fn read_aacs_inputs_from_drive(drive: &mut Drive) -> Result<(Vec<u8>, Vec<u8>, u8)> {
        // No READ CAPACITY: the key files do not need the disc size.
        let (mut reader, udf_fs) = Self::open_udf(drive)?;
        Self::read_aacs_inputs_from_reader(&mut reader, &udf_fs)
    }

    // Image scan body (no SCSI): capture the AACS files, then `finish`. A missing
    // `Unit_Key_RO.inf` is recorded, not fatal (decrypted folders, deferred mux).
    fn scan_fs(
        reader: &mut dyn SectorSource,
        capacity: u32,
        opts: &ScanOptions,
        udf_fs: udf::UdfFs,
    ) -> Result<Self> {
        let aacs = if aacs_dir_present(&udf_fs) {
            let cap = encrypt::capture(reader, &udf_fs, encrypt::CaptureFrom::Image)?;
            Some((cap, encrypt::BusOutcome::FileOrIso))
        } else {
            None
        };
        Self::finish(reader, capacity, udf_fs, aacs, opts, opts.halt.as_ref())
    }

    // Everything after the AACS bus step, for live and image scans alike: the AACS
    // verdict, titles, labels, format. `aacs` is `None` for a disc with no AACS dir.
    fn finish(
        reader: &mut dyn SectorSource,
        capacity: u32,
        udf_fs: udf::UdfFs,
        aacs: Option<(encrypt::AacsCapture, encrypt::BusOutcome)>,
        opts: &ScanOptions,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<Self> {
        let scan_with_t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::scan", phase = "scan_with", "begin");
        let encrypted = aacs.is_some();
        // Lookup-free: the state carries the disc's AACS inputs but no key; the caller
        // resolves one into a `ResolvedKeySet`.
        let (aacs, aacs_error) = match aacs {
            Some((cap, bus)) => encrypt::resolve_aacs(cap, &bus),
            None => (None, None),
        };

        // 3. Titles + container — dispatched by on-disc tree (HD-DVD/DVD peers,
        // FMTS shares the BD tree; FORMAT is a separate axis derived below). DVD
        // resolves its main feature via First-Play nav (issue #40) as `nav_feature`.
        let mut dvd_nav_feature: Option<u16> = None;
        let (mut titles, content_format) = if udf_fs.find_dir("/BDMV").is_some() {
            (
                Self::scan_bluray_titles(reader, &udf_fs, halt)?,
                ContentFormat::BdTs,
            )
        } else if udf_fs.find_dir("/HVDVD_TS").is_some() {
            (
                Self::scan_hddvd_titles(reader, &udf_fs, halt)?,
                ContentFormat::MpegPs,
            )
        } else if udf_fs.find_dir("/VIDEO_TS").is_some() {
            let (dvd_titles, nav) = Self::scan_dvd_titles(reader, &udf_fs, halt)?;
            dvd_nav_feature = nav;
            (dvd_titles, ContentFormat::MpegPs)
        } else {
            (Vec::new(), ContentFormat::BdTs)
        };
        // Title ordering: titles[0] should be the canonical main feature. Naive
        // "longest duration first" misranks branching UHDs (see `canonical_title_order`);
        // sort so `-t 1` / autorip / `.first()` converge on the real movie.
        let capacity_bytes = capacity as u64 * 2048;
        titles.sort_by(|a, b| Self::canonical_title_order(a, b, capacity_bytes));

        // 4. Metadata + labels
        let meta_title = Self::read_meta_title(reader, &udf_fs);
        let feature_hint = crate::labels::apply(reader, &udf_fs, &mut titles);

        // Authoritative signal: run the disc's own HDMV First-Play nav (`bdnav` VM)
        // to see which playlist it plays; BDMV only, `None` on BD-J/non-convergence,
        // restricted to non-trivial video titles so a logo/pre-roll isn't picked.
        let nav_feature = if udf_fs.find_dir("/BDMV").is_some() {
            let longest_video_dur = titles
                .iter()
                .filter(|t| t.has_probable_video())
                .map(|t| t.duration_secs)
                .fold(0.0_f64, f64::max);
            // Candidates: real-video titles at least half as long as the longest
            // probable-video title; the `<= 0.0` arm admits all when no duration
            // is known, so a bare tree still yields a candidate set for the nav.
            let candidates: std::collections::HashSet<u16> = titles
                .iter()
                .filter(|t| {
                    t.has_video()
                        && (longest_video_dur <= 0.0
                            || t.duration_secs
                                >= NAV_CANDIDATE_MIN_DURATION_FRAC * longest_video_dur)
                })
                .map(|t| t.playlist_id)
                .collect();
            crate::bdnav::resolve_feature(reader, &udf_fs, |id| candidates.contains(&id))
        } else {
            // DVD: the First-Play nav pick resolved during `scan_dvd_titles`.
            // Restrict it to a video-bearing title, the same guard the BD path
            // applies, so a nav result pointing at a streamless entry cannot win.
            dvd_nav_feature
                .filter(|id| titles.iter().any(|t| t.playlist_id == *id && t.has_video()))
        };

        // Authoritative main-feature ordering, refined once labels have run:
        // `nav_feature` wins outright, else a play-all composite is demoted and
        // `feature_hint` promoted; the sort above runs first for label anchoring.
        Self::sort_titles_by_main_feature(
            &mut titles,
            capacity_bytes,
            feature_hint.as_ref(),
            nav_feature,
        );

        // Optional content-based forced-subtitle detection. `info` opts in so its
        // forced flags match the muxer's rip-time result (shared PGS classifier);
        // rip leaves it off since the muxer detects forced without a second read.
        if opts.probe_forced_subtitles {
            Self::probe_forced_subtitles_for_bdts_titles(reader, &mut titles, halt);
        }
        crate::labels::fill_defaults(&mut titles);

        // 5. Format (AACS MKB generation → BD/UHD/FMTS; tree → HD-DVD/DVD) and
        //    layers. Region coding is not decoded yet — every disc reports
        //    Region-free (correct for UHD; a stub until BD/DVD region detection).
        let format = Self::detect_disc_format(reader, &udf_fs, &titles);
        let layers = if capacity > 24_000_000 { 2 } else { 1 };
        let region = DiscRegion::Free;

        // 6. CSS detection for DVDs is deferred to `Disc::scan`'s drive-auth path
        // (has `&mut Drive`), run after this returns — the reader-based crack path
        // is NOT run here: on a CSS disc it would fail ~50,000 sectors one-by-one.
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

        // Structured scan diagnostic block (--log-level 3): emits per-title/stream/
        // decision/AACS rows under `freemkv::diag`, a no-op unless enabled. (DVD
        // per-cell rows are emitted earlier, before per-cell detail is lowered away.)
        crate::diag::dump_disc(&disc);

        Ok(disc)
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Total ordering used to sort `Disc::titles` so `titles[0]` is the canonical main feature.
    /// Sort priority: (1) real titles (`size_bytes <= capacity_bytes`, skipped when capacity is
    /// UNKNOWN i.e. 0) before virtual "play-all" composites; (2) among real titles, LARGEST
    /// physical size first; (3) tiebreak on longer duration. Not a plain duration sort: a
    /// branching UHD's play-all can report an inflated duration/size exceeding the disc's
    /// physical capacity. These are the [`Self::canonical_title_order`] sort keys.
    pub const CANONICAL_TITLE_ORDER_KEYS: &'static [&'static str] = &[
        "fits-disc",
        "largest-size",
        "longest",
        "richest-audio",
        "more-video",
        "more-subs",
        "lowest-playlist-id",
    ];

    // Content-based forced-subtitle detection, restricted to `BdTs` titles — gates on STREAM
    // CONTAINER not disc-tree format (only BdTs carries PES-wrapped PGS).
    fn probe_forced_subtitles_for_bdts_titles(
        reader: &mut dyn SectorSource,
        titles: &mut [DiscTitle],
        halt: Option<&crate::halt::Halt>,
    ) {
        // One cache across every title: playlists overwhelmingly reference the
        // same handful of clips, so without memoisation the same extents are
        // re-read from the drive once per playlist — 30-150 times on a Blu-ray.
        let mut cache = pgs_forced_probe::ForcedProbeCache::new();
        for title in titles.iter_mut() {
            if title.content_format == ContentFormat::BdTs {
                pgs_forced_probe::probe_and_set_forced(reader, title, &mut cache, halt);
            }
        }
    }

    pub fn canonical_title_order(
        a: &DiscTitle,
        b: &DiscTitle,
        capacity_bytes: u64,
    ) -> std::cmp::Ordering {
        // A title bigger than the whole disc is a "play-all" composite (size
        // double-counts shared clips) — demote it below real titles. `capacity_bytes
        // == 0` means UNKNOWN (READ CAPACITY failed), so the gate stays inert then.
        let capacity_known = capacity_bytes > 0;
        let a_oversize = capacity_known && a.size_bytes > capacity_bytes;
        let b_oversize = capacity_known && b.size_bytes > capacity_bytes;
        a_oversize
            .cmp(&b_oversize)
            // PRIMARY: largest physical size = the main feature — robust where
            // duration/clip-count are not, since a decoy play-all runs long but
            // is tiny. Validated on 23 discs against the old clip-count key.
            .then_with(|| b.size_bytes.cmp(&a.size_bytes))
            // Tiebreak for equal-size twins: longer duration, then richer audio
            // (a full-audio main vs a stereo-only twin) — prefer lossless-multichannel.
            .then_with(|| b.duration_secs.total_cmp(&a.duration_secs))
            .then_with(|| Self::audio_richness(b).cmp(&Self::audio_richness(a)))
            // Issue #45: before the lowest-id key, prefer the STREAM-RICHER sibling on
            // the axes audio_richness misses (extra video = Dolby Vision EL; extra
            // subtitle). True siblings match both and fall through to lowest-id.
            .then_with(|| b.video_streams().count().cmp(&a.video_streams().count()))
            .then_with(|| {
                b.subtitle_streams()
                    .count()
                    .cmp(&a.subtitle_streams().count())
            })
            // Final determinism (issue #45): among equal-size seamless siblings
            // (Alita 00800-00808, same body + different logo) pick the LOWEST
            // playlist_id. Inert where an earlier key separates the pair.
            .then_with(|| a.playlist_id.cmp(&b.playlist_id))
    }

    /// The keys [`Self::main_feature_order`] layers ON TOP of
    /// [`Self::CANONICAL_TITLE_ORDER_KEYS`], highest priority first, as diagnostic tokens:
    /// `nav-feature` (disc's own HDMV nav pick, authoritative), `authoring-feature` (disc's
    /// menu-designated feature), `standalone` (demotes a play-all/wrapper COMPOSITE below the
    /// real title it wraps), `has-video` (demotes titles with no plausible video, see
    /// [`DiscTitle::has_probable_video`]). Kept beside the comparator.
    pub const MAIN_FEATURE_ORDER_KEYS: &'static [&'static str] = &[
        "nav-feature",
        "authoring-feature",
        "standalone",
        "has-video",
    ];

    /// Precompute the [`TitleRank`] for every title, in the same index order. Composite
    /// detection: title `P` is a composite when some OTHER title `Q` has a non-empty clip-id
    /// set that is a PROPER SUBSET of `P`'s, `Q` has plausible video, `Q` accounts for at least
    /// half of `P`'s declared size, AND `P` has a WRAPPER shape (issue #45): either `Q` is a
    /// much shorter cut or `Q` is nearly `P`'s size-equal. Capacity-independent (`capacity` may
    /// be `0`/unknown).
    pub fn rank_titles(
        titles: &[DiscTitle],
        hint: Option<&crate::labels::FeaturePlaylistHint>,
        nav_feature: Option<u16>,
    ) -> Vec<TitleRank> {
        let clip_sets: Vec<std::collections::BTreeSet<&str>> = titles
            .iter()
            .map(|t| t.clips.iter().map(|c| c.clip_id.as_str()).collect())
            .collect();
        let longest_video_dur = titles
            .iter()
            .filter(|t| t.has_probable_video())
            .map(|t| t.duration_secs)
            .fold(0.0_f64, f64::max);
        // Largest video-bearing title's byte size, a plausibility floor: a promoted
        // title must hold a meaningful fraction of it, else a tiny but playback-valid
        // seamless-branch playlist could win nav/authoring and get ripped instead.
        let max_video_size = titles
            .iter()
            .filter(|t| t.has_probable_video())
            .map(|t| t.size_bytes)
            .max()
            .unwrap_or(0);
        let carries_feature_payload = |t: &DiscTitle| {
            max_video_size == 0
                || t.size_bytes as f64 >= FEATURE_PAYLOAD_MIN_SIZE_FRAC * max_video_size as f64
        };
        // The composite scan below is O(n^2 * clip-set-size); title/clip counts are
        // untrusted, so a crafted image could stall it. Bound the total pair work;
        // above budget, skip composite classification and use has-video/canonical order.
        let max_clips = clip_sets.iter().map(|s| s.len()).max().unwrap_or(0);
        let composite_scan_ok = titles
            .len()
            .saturating_mul(titles.len())
            .saturating_mul(max_clips.max(1))
            <= 50_000_000;
        titles
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let pi = &clip_sets[i];
                let composite = composite_scan_ok
                    && !pi.is_empty()
                    && p.size_bytes > 0
                    && titles.iter().enumerate().any(|(j, q)| {
                        j != i && {
                            let qj = &clip_sets[j];
                            // Cheap gates (length, video, size) before the O(len)
                            // `is_subset` traversal, so the expensive check runs
                            // only on genuinely plausible wrapper candidates.
                            !qj.is_empty()
                                && qj.len() < pi.len()
                                && q.has_probable_video()
                                && q.size_bytes as f64 >= WRAPPER_MIN_SIZE_FRAC * p.size_bytes as f64
                                // Seamless-branch guard (issue #45): wrapper only if Q is
                                // a much SHORTER cut (concat) OR nearly P's SIZE AND a
                                // complete feature (bumper) — see is_complete_feature_presentation.
                                && (q.duration_secs
                                    < SEAMLESS_SHORTER_CUT_DURATION_FRAC * p.duration_secs
                                    || (q.size_bytes as f64
                                        >= SEAMLESS_BUMPER_SIZE_FRAC * p.size_bytes as f64
                                        && is_complete_feature_presentation(q)))
                                && qj.is_subset(pi)
                        }
                    });
                // Authoring hint honoured only when the title has REAL video
                // (strict `has_video`, not `has_probable_video`), is NOT itself a
                // composite, and runs at least half the longest title (audit R4).
                let authoring = !composite
                    && hint.is_some_and(|h| h.matches(p.playlist_id, &p.playlist))
                    && p.has_video()
                    && (longest_video_dur <= 0.0
                        || p.duration_secs >= AUTHORING_MIN_DURATION_FRAC * longest_video_dur)
                    && carries_feature_payload(p);
                // Nav result is video- and payload-gated like the authoring hint,
                // so a streamless or low-byte branch/DV presentation cannot win
                // even if the disc's own navigation resolves to it.
                let nav = nav_feature == Some(p.playlist_id)
                    && p.has_video()
                    && carries_feature_payload(p);
                TitleRank {
                    nav,
                    authoring,
                    composite,
                }
            })
            .collect()
    }

    /// Pairwise order over titles for MAIN-FEATURE selection, given each title's
    /// precomputed [`TitleRank`]: authoring-designated feature first, then a
    /// standalone title ahead of any composite that wraps it, then plausible
    /// video ahead of none, then the physical [`Self::canonical_title_order`]
    /// keys. [`Self::sort_titles_by_main_feature`] drives it so `titles[0]` is
    /// the selected main feature.
    pub fn main_feature_order(
        a: &DiscTitle,
        b: &DiscTitle,
        capacity_bytes: u64,
        ra: &TitleRank,
        rb: &TitleRank,
    ) -> std::cmp::Ordering {
        rb.nav
            .cmp(&ra.nav)
            .then_with(|| rb.authoring.cmp(&ra.authoring))
            .then_with(|| ra.composite.cmp(&rb.composite))
            .then_with(|| b.has_probable_video().cmp(&a.has_probable_video()))
            .then_with(|| Self::canonical_title_order(a, b, capacity_bytes))
    }

    /// Sort `titles` so `titles[0]` is the main feature: precompute the global
    /// [`TitleRank`]s, then order by [`Self::main_feature_order`]. Used by
    /// `scan_with` (after labels, so the authoring hint is available) and by the
    /// selection tests.
    pub fn sort_titles_by_main_feature(
        titles: &mut Vec<DiscTitle>,
        capacity_bytes: u64,
        hint: Option<&crate::labels::FeaturePlaylistHint>,
        nav_feature: Option<u16>,
    ) {
        let ranks = Self::rank_titles(titles, hint, nav_feature);
        let mut decorated: Vec<(TitleRank, DiscTitle)> =
            ranks.into_iter().zip(std::mem::take(titles)).collect();
        decorated
            .sort_by(|(ra, a), (rb, b)| Self::main_feature_order(a, b, capacity_bytes, ra, rb));
        *titles = decorated.into_iter().map(|(_, t)| t).collect();
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
                max_ch = max_ch.max(a.channels.count());
            }
        }
        (lossless, max_ch, count)
    }

    // The disc format, from tree (HD-DVD/DVD are tree-level peers) and AACS
    // MKB generation (within BDMV/, the MKB Type record decides BD/UHD/FMTS).
    // Unencrypted/MKB-less BD falls back to resolution, never below BluRay.
    fn detect_disc_format(
        reader: &mut dyn SectorSource,
        udf_fs: &crate::udf::UdfFs,
        titles: &[DiscTitle],
    ) -> DiscFormat {
        use crate::aacs::mkb::{AacsVersion, mkb_type};
        // Tree priority MUST match the title-scan dispatch (BDMV → HVDVD_TS →
        // VIDEO_TS): otherwise a disc carrying two trees would be classified as
        // one format but enumerated as another (e.g. BD titles tagged HdDvd).
        if udf_fs.find_dir("/BDMV").is_some() {
            // Only the Type-and-Version record (first record) is needed.
            if let Ok(mkb) = udf_fs.read_file_prefix(reader, "/AACS/MKB_RO.inf", 64) {
                match mkb_type(&mkb).map(|t| t.generation()) {
                    Some(AacsVersion::V21) => return DiscFormat::Fmts,
                    Some(AacsVersion::V20) => return DiscFormat::Uhd,
                    Some(AacsVersion::V10) => return DiscFormat::BluRay,
                    None => {}
                }
            }
            // Unencrypted/unreadable MKB: refine by resolution, but a BD-tree disc
            // is never below Blu-ray — `detect_format` can return Dvd for an SD
            // bonus title, which must not mis-tag a BDMV disc (mis-sizes the ECC sweep).
            return match Self::detect_format(titles) {
                DiscFormat::Uhd => DiscFormat::Uhd,
                _ => DiscFormat::BluRay,
            };
        }
        if udf_fs.find_dir("/HVDVD_TS").is_some() {
            return DiscFormat::HdDvd;
        }
        if udf_fs.find_dir("/VIDEO_TS").is_some() {
            return DiscFormat::Dvd;
        }
        DiscFormat::Unknown
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
        let result = session.exec(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        // Share the decoder with `Drive::capacity` rather than re-deriving it — a
        // hand-rolled copy previously dropped the short-transfer check, so a drive
        // answering GOOD with an empty data phase decoded to a 1-sector disc.
        crate::drive::decode_read_capacity(&buf, result.bytes_transferred)
    }
}

/// A decryption key handed to libfreemkv by the caller. libfreemkv is
/// **lookup-free**: it never reads a keydb, talks to a key server, or
/// searches paths — the application resolves a key however it likes and
/// hands it in here; libfreemkv derives any remaining AACS-chain steps from
/// disc-read inputs (MKB / VID / `Unit_Key_RO.inf`). `#[non_exhaustive]`:
/// AACS is a derivation chain (`DK →(MKB)→ MK →(VID)→ VK →(Unit_Key_RO)→
/// UK`); each variant enters at one level, and the library
/// derives down from it — new levels can be added without breaking callers.
#[derive(Clone)]
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

// Redacting `Debug`: `Key` is a key-transport type;
// every variant carries raw key material. Print only variant name and count —
// never bytes. Guarded by `aacs_state_and_key_debug_are_redacted`.
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Device(v) => write!(f, "Key::Device(<{} redacted>)", v.len()),
            Key::Processing(v) => write!(f, "Key::Processing(<{} redacted>)", v.len()),
            Key::Media(v) => write!(f, "Key::Media(<{} redacted>)", v.len()),
            Key::Volume(_) => f.write_str("Key::Volume(<redacted>)"),
            Key::Unit(v) => write!(f, "Key::Unit(<{} redacted>)", v.len()),
        }
    }
}

// Read-ahead confined to one file's recorded extents: a single-sector read inside an
// extent fetches up to a batch of THAT extent only (never adjacent essence). Other
// single reads (the ICB) are memoised; the caller's `recovery` flag is forwarded.
struct FileReadAhead<'a> {
    inner: &'a mut dyn SectorSource,
    extents: Vec<(u32, u32)>,
    cache: Vec<u8>,
    cache_lba: u32,
    cached: u32,
    // After a failed batch, read the rest of the file sector by sector.
    batching: bool,
    last: Option<(u32, Box<[u8; 2048]>)>,
}

impl<'a> FileReadAhead<'a> {
    const BATCH: u32 = DEFAULT_BATCH_SECTORS_OPTICAL as u32;

    fn new(inner: &'a mut dyn SectorSource, fs: &udf::UdfFs, icb: u32) -> Result<Self> {
        let mut ra = Self {
            inner,
            extents: Vec::new(),
            cache: Vec::new(),
            cache_lba: 0,
            cached: 0,
            batching: true,
            last: None,
        };
        // No extent list (e.g. ICB-embedded data) just means no read-ahead.
        ra.extents = match fs.extents_abs_at(&mut ra, icb) {
            Ok(v) => v
                .into_iter()
                .filter(|e| e.recorded)
                .map(|e| (e.lba, (e.len as u64).div_ceil(2048) as u32))
                .collect(),
            Err(Error::Halted) => return Err(Error::Halted),
            Err(_) => Vec::new(),
        };
        Ok(ra)
    }

    fn read_one(&mut self, lba: u32, buf: &mut [u8], recovery: bool) -> Result<usize> {
        if let Some((at, data)) = &self.last
            && *at == lba
        {
            buf[..2048].copy_from_slice(&data[..]);
            return Ok(2048);
        }
        self.inner.read_sectors(lba, 1, buf, recovery)?;
        let mut data = Box::new([0u8; 2048]);
        data.copy_from_slice(&buf[..2048]);
        self.last = Some((lba, data));
        Ok(2048)
    }
}

impl SectorSource for FileReadAhead<'_> {
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        if count != 1 || buf.len() < 2048 {
            return self.inner.read_sectors(lba, count, buf, recovery);
        }
        if lba >= self.cache_lba && lba - self.cache_lba < self.cached {
            let off = (lba - self.cache_lba) as usize * 2048;
            buf[..2048].copy_from_slice(&self.cache[off..off + 2048]);
            return Ok(2048);
        }
        let left = self
            .extents
            .iter()
            .find(|&&(start, n)| lba >= start && lba - start < n)
            .map(|&(start, n)| n - (lba - start));
        let want = if self.batching {
            left.unwrap_or(1).min(Self::BATCH)
        } else {
            1
        };
        self.cached = 0;
        if want <= 1 {
            return self.read_one(lba, buf, recovery);
        }
        self.cache.resize(want as usize * 2048, 0);
        match self
            .inner
            .read_sectors(lba, want as u16, &mut self.cache, recovery)
        {
            Ok(_) => {
                self.cache_lba = lba;
                self.cached = want;
                buf[..2048].copy_from_slice(&self.cache[..2048]);
                Ok(2048)
            }
            Err(Error::Halted) => Err(Error::Halted),
            // A bad sector later in the batch must not fail this one: retry it alone.
            Err(_) => {
                self.batching = false;
                self.read_one(lba, buf, recovery)
            }
        }
    }
}

// A disc-supplied name safe to use as one path component on any host (incl. Windows).
fn is_plain_file_name(name: &str) -> bool {
    if name.is_empty() || name.ends_with('.') || name.ends_with(' ') {
        return false; // also rejects "." and ".."
    }
    if name.chars().any(|c| {
        matches!(c, '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*') || c.is_control()
    }) {
        return false;
    }
    // Windows reserved device names, matched on the stem with trailing spaces ignored.
    let stem = name.split('.').next().unwrap_or(name).trim_end_matches(' ');
    let stem = stem.to_ascii_uppercase();
    let numbered = |p: &str| {
        stem.strip_prefix(p).is_some_and(|d| {
            let mut c = d.chars();
            matches!(
                (c.next(), c.next()),
                (Some('0'..='9' | '\u{B9}' | '\u{B2}' | '\u{B3}'), None)
            )
        })
    };
    !(matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || numbered("COM")
        || numbered("LPT"))
}

impl Disc {
    /// Sector count for a whole-disc image read (`disc:// → iso://` / raw
    /// image), validated non-zero.
    ///
    /// `capacity_sectors` is 0 only when READ CAPACITY failed on every retry
    /// (see `read_capacity_retrying`). Imaging paths size their
    /// read domain through this accessor so that empty case is a hard
    /// [`Error::EmptyImage`], not a silent 0-byte ISO reported as success;
    /// scan/identify/MKV paths read the field directly and stay lenient.
    pub fn image_read_sectors(&self) -> Result<u32> {
        if self.capacity_sectors == 0 {
            return Err(Error::EmptyImage);
        }
        Ok(self.capacity_sectors)
    }

    /// The disc's own decryption keys: CSS, or `None`. AACS keys come only from a
    /// [`ResolvedKeySet`](crate::keys::ResolvedKeySet) (KU §2.2), so an AACS disc is `None`.
    pub(crate) fn decrypt_keys(&self) -> crate::decrypt::DecryptKeys {
        if self.aacs.is_some() {
            crate::decrypt::DecryptKeys::None
        } else if let Some(ref css) = self.css {
            crate::decrypt::DecryptKeys::Css {
                title_key: css.title_key,
            }
        } else {
            crate::decrypt::DecryptKeys::None
        }
    }

    /// The scanned titles' encrypted content as a sorted, merged, disjoint set
    /// of `(start_lba, sector_count)` ranges — the union of every title's
    /// stream extents. Covers kept titles ONLY: stream files no title plays
    /// are absent, so this is not a whole-disc map (the live drive's bus gate
    /// also covers every `/BDMV/STREAM` file). Empty when no titles parsed.
    pub fn encrypted_content_ranges(&self) -> Vec<(u32, u32)> {
        merged_extents(self.titles.iter().flat_map(|t| &t.extents))
    }

    /// `n_decl` (KU design §2.3 step 3): the CPS units the disc's title-key file
    /// DECLARES, parsed by [`crate::aacs::inf::parse_title_keys`] (BD/UHD
    /// `Unit_Key_RO.inf` or HD DVD VTKF). `None` when absent or unparseable (fails closed).
    pub(crate) fn declared_cps_units(&self) -> Option<usize> {
        use crate::aacs::mkb::AacsVersion;
        let aacs = self.aacs.as_ref()?;
        let version = if aacs.version >= 2 {
            AacsVersion::V20
        } else {
            AacsVersion::V10
        };
        // KS-14 [BD] §3.9.3: "Num_of_CPS_Unit field (16 bits) indicates the number of CPS
        // Units on the disc" — the declared count, never the number of keys held. K-8: an
        // HD DVD VTKF counts too, through `parse_title_keys` (KS-27, evidence).
        let ukf = crate::aacs::inf::parse_title_keys(&aacs.uk_ro, version)?;
        Some(ukf.encrypted_keys.len())
    }

    // The 40-hex AACS disc id (SHA1 of Unit_Key_RO.inf, no 0x prefix), or
    // empty when uncaptured. Names the disc in an Error::NoDiscKey.
    pub(crate) fn aacs_disc_hash(&self) -> String {
        self.aacs
            .as_ref()
            .map(|a| crate::hex::strip_hex_prefix(&a.disc_hash).to_string())
            .unwrap_or_default()
    }

    /// The unlocker matrix for this scanned disc on `drive`: each REGISTERED
    /// unlocker's name + whether it actually **did work this rip** — i.e. ran
    /// and accomplished its job, NOT merely "matched the disc kind".
    /// Registry-driven names so the CLI and autorip render an identical
    /// report. On a drive taken by the firmware bus-unlock route, the AACS
    /// host-cert unlocker never runs — it "matched" but did nothing, so
    /// `did-work` reports `AACS: no`, and a *stock* drive on the cert route
    /// instead reports `LD: no, AACS: yes` — the real diagnostic.
    pub fn unlocker_matrix(&self, drive: &crate::Drive) -> Vec<(&'static str, bool)> {
        // The firmware unlocker that ran (recorded on init): "freemkv", "LD", or "Renesas" —
        // mutually exclusive per drive.
        let prep = drive.unlocker_name();
        // Every firmware / vendor-CDB route (freemkv Raw Read, LD, Renesas/Pioneer vendor open)
        // removes bus encryption AT THE DRIVE — clear content, no host cert.
        let fw_removed_bus = matches!(prep, Some("freemkv") | Some("LD") | Some("Renesas"));
        crate::unlock_bridge::unlocker_names()
            .into_iter()
            .map(|name| {
                let did_work = match name {
                    // Each firmware unlocker did work iff it was the one that ran.
                    "freemkv" => prep == Some("freemkv"),
                    "LD" => prep == Some("LD"),
                    "Renesas" => prep == Some("Renesas"),
                    // The AACS host-cert route did work only when NO firmware route
                    // ran, its handshake yielded a VID, and no handshake error stands.
                    "AACS" => {
                        self.aacs.as_ref().is_some_and(|a| a.volume_id != [0u8; 16])
                            && !fw_removed_bus
                            && self
                                .aacs_error
                                .as_ref()
                                .is_none_or(|e| encrypt::handshake_class_error(e).is_none())
                    }
                    // DVD read-unlock (CSS bus-auth) runs for EVERY DVD during scan
                    // and isn't tracked separately, so this reports the MEDIUM
                    // engaged, not a per-rip success bit — failures surface downstream.
                    "DVD" => self.format == DiscFormat::Dvd,
                    // A newly-registered unlocker with no runtime signal wired
                    // here yet: report `no` rather than guess.
                    _ => false,
                };
                (name, did_work)
            })
            .collect()
    }

    /// The public AACS inputs for this disc, for a [`crate::KeySource`] to look
    /// a key up. `None` when the disc carries no AACS state (unencrypted, CSS,
    /// or AACS inputs not captured at scan). Contains no secrets — just disc
    /// identity plus the on-disc AACS structures.
    pub fn inputs(&self) -> Option<crate::keysource::DiscInputs> {
        self.aacs.as_ref().map(|a| crate::keysource::DiscInputs {
            disc_hash: a.disc_hash.clone(),
            volume_id: a.volume_id,
            version: a.version,
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

    /// [`Self::inputs`] with `samples` filled: up to `n` encrypted units read via
    /// `reader` from the main feature, so a key source's answer can be validated
    /// against real ciphertext. `None` for a disc with no AACS inputs.
    pub fn inputs_with_samples(
        &self,
        reader: &mut dyn SectorSource,
        n: usize,
    ) -> Option<crate::keysource::DiscInputs> {
        let mut inputs = self.inputs()?;
        inputs.samples = self.content_samples(reader, n);
        Some(inputs)
    }

    // Encrypted sample units from the main feature (see `main_title`).
    pub(crate) fn content_samples(&self, reader: &mut dyn SectorSource, n: usize) -> Vec<Vec<u8>> {
        self.main_title()
            .map(|t| crate::keysource::read_encrypted_units(reader, t, n))
            .unwrap_or_default()
    }

    // The LARGEST title that HAS video (skips size-inflated streamless decoys on
    // some obfuscated UHDs); the largest title outright when none has video.
    pub(crate) fn main_title(&self) -> Option<&DiscTitle> {
        self.titles
            .iter()
            .filter(|t| t.has_probable_video())
            .max_by_key(|t| t.size_bytes)
            .or_else(|| self.titles.iter().max_by_key(|t| t.size_bytes))
    }
}

// Mapfile path for a regular output file: appends `.mapfile` to the output
// path. For `/dev/null` (benchmark) output use `Disc::mapfile_for` instead.
pub(crate) fn mapfile_path_for(iso_path: &std::path::Path) -> std::path::PathBuf {
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

// Whether the Linux-sysfs transfer-size probe applies to this device path.
// The probe reads /sys/block or /sys/class/scsi_generic, Linux-only and only
// for `/`-delimited paths; a Windows `\\.\CdRom0` path has neither.
fn sysfs_batch_probe_supported(device_path: &str) -> bool {
    cfg!(target_os = "linux") && device_path.contains('/')
}

/// Detect the maximum transfer size in sectors for a device.
pub fn detect_max_batch_sectors(device_path: &str) -> u16 {
    // The sysfs probe below is Linux-only; Windows-style `\\.\CdRom0` paths
    // have no forward slash, so the parsing below would find no `/sys` node
    // and fall through to the block default (16 MiB), far over the optical cap.
    if !sysfs_batch_probe_supported(device_path) {
        return DEFAULT_BATCH_SECTORS_OPTICAL;
    }

    let dev_name = device_path.rsplit('/').next().unwrap_or("");
    if dev_name.is_empty() {
        return DEFAULT_BATCH_SECTORS_OPTICAL;
    }

    // Check whether THIS device (not any device on the host) is optical: read
    // the SCSI type of the target node only (0x05 = CD/DVD). A previous version
    // scanned every scsi_device entry, misclassifying a block device as optical.
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
            if let Ok(content) = std::fs::read_to_string(&sysfs_path)
                && let Ok(kb) = content.trim().parse::<u32>()
            {
                // Convert KB to sectors (1 sector = 2 KB = 2048 bytes)
                let sectors = (kb / 2).min(u16::MAX as u32) as u16;
                // Align down to 3 (one aligned unit)
                let aligned = (sectors / 3) * 3;
                if aligned >= MIN_BATCH_SECTORS {
                    return aligned.min(MAX_BATCH_SECTORS);
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

    fn mp2_track(pid: u16, label: &str) -> AudioStream {
        AudioStream {
            pid,
            codec: Codec::Mp2,
            channels: AudioChannels::Unknown,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: label.into(),
        }
    }

    /// The extension marker needs both the label and a DVD extension PID `0xD0|n` (US5987417
    /// "1100 0***b or 1101 0***b"): a base or BD track that happens to carry the text is not one.
    #[test]
    fn mp2_extension_needs_the_label_and_an_extension_pid() {
        for pid in 0xD0..=0xD7 {
            assert!(
                mp2_track(pid, MP2_EXTENSION_LABEL).is_mp2_extension(),
                "{pid:#x}"
            );
            assert!(!mp2_track(pid, "").is_mp2_extension(), "{pid:#x}");
        }
        for pid in [0x00C0, 0x00C7, 0x00CF, 0x00D8, 0x1100, 0xBD80] {
            assert!(
                !mp2_track(pid, MP2_EXTENSION_LABEL).is_mp2_extension(),
                "{pid:#x}"
            );
        }
    }

    // ── image-time CSS crack: canonical extent ordering (finding 1) ─────────

    /// Build a title with the given extents (start_lba, sector_count) and a
    /// declared `size_bytes` (used by `canonical_title_order`'s capacity gate).
    fn title_with_extents(size_bytes: u64, extents: &[(u32, u32)]) -> DiscTitle {
        DiscTitle {
            size_bytes,
            extents: extents
                .iter()
                .map(|&(start_lba, sector_count)| Extent {
                    start_lba,
                    sector_count,
                })
                .collect(),
            content_format: ContentFormat::MpegPs,
            ..DiscTitle::empty()
        }
    }

    // The image-time CSS crack must scan extents in natural PLAYBACK order
    // (largest-cell-first is the 1.5.1 garbage bug). Fixture's LARGEST cell
    // is physically LAST, so the two orderings yield different LBA sequences.
    #[test]
    fn image_crack_extents_are_playback_order_not_largest_cell_first() {
        // A real CSS DVD: a short clear front matter cell (logo/rating card)
        // precedes the big scrambled feature body.
        let title = title_with_extents(0, &[(1_000, 16), (1_016, 512), (2_000, 40_960)]);
        let got = Disc::image_crack_extents(std::slice::from_ref(&title));
        let lbas: Vec<u32> = got.iter().map(|e| e.start_lba).collect();
        assert_eq!(
            lbas,
            vec![1_000, 1_016, 2_000],
            "extents must be handed to the crack in playback order"
        );
    }

    // Image-time crack must use the canonically-ordered titles[0], NOT a
    // locally re-derived "most sectors" pick: an oversize play-all composite
    // has the most sectors but is demoted to the back by canonical order.
    #[test]
    fn image_crack_extents_follow_canonical_title_order_not_sector_count() {
        // titles[0] = the real feature (canonical order already applied).
        let feature = title_with_extents(2_000_000_000, &[(5_000, 100_000)]);
        // Demoted play-all composite: MORE total sectors than the feature.
        let play_all = title_with_extents(9_000_000_000, &[(5_000, 100_000), (5_000, 100_000)]);
        let titles = [feature, play_all];
        let got = Disc::image_crack_extents(&titles);
        assert_eq!(
            got.len(),
            1,
            "the crack must use the canonical main feature's single extent"
        );
        assert_eq!(got[0].sector_count, 100_000);
    }

    // ── scan_image's CSS-crack gate: DVD-only, never AACS/HD-DVD ────────────

    /// Minimal VIDEO_TS.IFO (VMG): one title, VTS 1, title 1. Layout mirrors
    /// `disc::dvd`'s own IFO builders (magic@0, TT_SRPT ptr@0xC4).
    fn dvd_vmg_bytes() -> Vec<u8> {
        let tt_srpt_sector = 1u32;
        let mut d = vec![0u8; 2 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VMG");
        d[0xC4..0xC8].copy_from_slice(&tt_srpt_sector.to_be_bytes());
        let base = tt_srpt_sector as usize * 2048;
        d[base..base + 2].copy_from_slice(&1u16.to_be_bytes()); // num_titles = 1
        let e = base + 8;
        d[e + 2..e + 4].copy_from_slice(&1u16.to_be_bytes()); // chapters
        d[e + 6] = 1; // vts
        d[e + 7] = 1; // vts_title
        d
    }

    /// Minimal VTS_01_0.IFO: one PGC, one cell `[first, last]`, NTSC/4:3, no
    /// audio/subs. Layout mirrors `disc::dvd`'s own IFO builders.
    fn dvd_vts_bytes(vob_start: u32, first_sector: u32, last_sector: u32) -> Vec<u8> {
        let pgcit_sector = 2u32;
        let mut d = vec![0u8; 4 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VTS");
        d[0xC4..0xC8].copy_from_slice(&vob_start.to_be_bytes()); // vtstt_vobs
        d[0xCC..0xD0].copy_from_slice(&pgcit_sector.to_be_bytes());
        d[0x202..0x204].copy_from_slice(&0u16.to_be_bytes()); // num_audio
        d[0x254..0x256].copy_from_slice(&0u16.to_be_bytes()); // num_subs

        let pg = pgcit_sector as usize * 2048;
        d[pg..pg + 2].copy_from_slice(&1u16.to_be_bytes()); // num_pgcs = 1
        let pgc_rel: u32 = 0x100;
        d[pg + 8 + 4..pg + 8 + 8].copy_from_slice(&pgc_rel.to_be_bytes());
        let pgc = pg + pgc_rel as usize;
        d[pgc + 0x02] = 1; // nr_of_programs
        d[pgc + 0x03] = 1; // nr_of_cells
        d[pgc + 0x06] = 0x30; // BCD 30s duration
        d[pgc + 0x07] = 0b0100_0000; // 25fps rate bits
        let cell_tbl_rel: u16 = 0xF0;
        let pgm_map_rel: u16 = 0xEC;
        d[pgc + 0xE6..pgc + 0xE8].copy_from_slice(&pgm_map_rel.to_be_bytes());
        d[pgc + 0xE8..pgc + 0xEA].copy_from_slice(&cell_tbl_rel.to_be_bytes());
        d[pgc + pgm_map_rel as usize] = 1; // program 0 -> cell 1
        let cell_base = pgc + cell_tbl_rel as usize;
        d[cell_base + 8..cell_base + 12].copy_from_slice(&first_sector.to_be_bytes());
        d[cell_base + 20..cell_base + 24].copy_from_slice(&last_sector.to_be_bytes());
        d
    }

    /// A still-scrambled CSS DVD image must come back from `scan_image` with
    /// `css.is_some()` and `encrypted == true` — the known-plaintext crack must
    /// actually run against a `DiscFormat::Dvd` image with titles.
    #[test]
    fn scan_image_scrambled_css_dvd_is_cracked_and_marked_encrypted() {
        use crate::udf::fixture::*;

        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let crackable = crackable_css_sector(&title_key).to_vec();

        let vmg = dvd_vmg_bytes();
        // vob_start = 1000, single cell sector [10, 10] -> 1-sector extent.
        let vts = dvd_vts_bytes(1000, 10, 10);

        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "VIDEO_TS".into(),
                icb_lba: 50,
                dir_data_lba: 51,
                files: vec![
                    file_with("VIDEO_TS.IFO", 60, 5000, vmg, true),
                    file_with("VTS_01_0.IFO", 62, 6000, vts, true),
                ],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        // IFO absolute LBA = PART_START(2000) + data_lba(6000) = 8000; extent
        // absolute LBA = 8000 + vob_start(1000) + first_sector(10) = 9010.
        disc.put_bytes(9010, &crackable);

        let disc_scan = Disc::scan_image(&mut disc, 500_000, &ScanOptions::default())
            .expect("scan_image must succeed on this synthetic DVD image");
        assert_eq!(disc_scan.format, DiscFormat::Dvd, "sanity: image is DVD");
        assert!(!disc_scan.titles.is_empty(), "sanity: a title was parsed");
        assert!(
            disc_scan.css.is_some(),
            "a scrambled CSS DVD image must be cracked, not left in the clear"
        );
        assert!(
            disc_scan.encrypted,
            "a cracked CSS DVD must be reported encrypted"
        );
    }

    // An operator Stop during scan_image's CSS crack must end the scan as Halted —
    // the token in `opts.halt` has to reach the crack, not a hardcoded `None`.
    #[test]
    fn scan_image_css_crack_honours_the_halt_token() {
        use crate::udf::fixture::*;
        // Cancels the token on the first read inside the crack extent.
        struct StopInVob<'a> {
            inner: &'a mut MemDisc,
            halt: crate::halt::Halt,
        }
        impl SectorSource for StopInVob<'_> {
            fn capacity_sectors(&self) -> u32 {
                self.inner.capacity_sectors()
            }
            fn read_sectors(&mut self, lba: u32, c: u16, b: &mut [u8], r: bool) -> Result<usize> {
                if lba >= 9010 {
                    self.halt.cancel();
                }
                self.inner.read_sectors(lba, c, b, r)
            }
        }
        let vts = dvd_vts_bytes(1000, 10, 200); // a ~190-sector clear extent
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "VIDEO_TS".into(),
                icb_lba: 50,
                dir_data_lba: 51,
                files: vec![
                    file_with("VIDEO_TS.IFO", 60, 5000, dvd_vmg_bytes(), true),
                    file_with("VTS_01_0.IFO", 62, 6000, vts, true),
                ],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let halt = crate::halt::Halt::new();
        let opts = ScanOptions {
            halt: Some(halt.clone()),
            ..ScanOptions::default()
        };
        let mut src = StopInVob {
            inner: &mut disc,
            halt,
        };
        let err = Disc::scan_image(&mut src, 500_000, &opts).expect_err("stopped mid-crack");
        assert_eq!(err.code(), Error::Halted.code());
    }

    // A truncated DVD ISO whose feature lies past EOF: the scan still lists it, and
    // the per-title crack reports the read fault, never a missing CSS key.
    #[test]
    fn scan_image_unreadable_crack_extent_reports_the_read_error() {
        use crate::udf::fixture::*;
        struct Truncated<'a>(&'a mut MemDisc);
        impl SectorSource for Truncated<'_> {
            fn read_sectors(&mut self, lba: u32, c: u16, b: &mut [u8], r: bool) -> Result<usize> {
                if lba >= 9010 {
                    return Err(Error::DiscRead {
                        sector: lba as u64,
                        status: None,
                        sense: None,
                    });
                }
                self.0.read_sectors(lba, c, b, r)
            }
        }
        let vts = dvd_vts_bytes(1000, 10, 200);
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "VIDEO_TS".into(),
                icb_lba: 50,
                dir_data_lba: 51,
                files: vec![
                    file_with("VIDEO_TS.IFO", 60, 5000, dvd_vmg_bytes(), true),
                    file_with("VTS_01_0.IFO", 62, 6000, vts, true),
                ],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let mut src = Truncated(&mut disc);
        let scanned = Disc::scan_image(&mut src, 500_000, &ScanOptions::default())
            .expect("an unreadable crack extent must not make the image unlistable");
        assert!(scanned.css.is_none() && scanned.css_error.is_none());
        let title = &scanned.titles[0];
        let read_fault = Error::DiscRead {
            sector: title.extents[0].start_lba as u64,
            status: None,
            sense: None,
        };
        let mut keys = crate::decrypt::DecryptKeys::None;
        let err = crate::css::resolve_dvd_title_key(
            &mut src,
            &title.extents,
            &mut keys,
            32,
            title.content_format,
            false,
            None,
        )
        .expect_err("the per-title crack reports the fault");
        assert_eq!(
            err.to_string(),
            std::io::Error::from(read_fault).to_string()
        );
    }

    // An HD-DVD image is also MPEG-PS but must NEVER enter the CSS crack.
    // The HD-DVD clip's own extent carries a genuinely crackable CSS sector,
    // so if the gate wrongly let the crack run, `disc.css` would be `Some`.
    #[test]
    fn scan_image_hddvd_never_enters_css_crack() {
        use crate::udf::fixture::*;

        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let crackable = crackable_css_sector(&title_key).to_vec();

        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![file_with("FEATURE.EVO", 30, 9000, crackable, true)],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);

        let disc_scan = Disc::scan_image(&mut disc, 500_000, &ScanOptions::default())
            .expect("scan_image must succeed on this synthetic HD-DVD image");
        assert_eq!(
            disc_scan.format,
            DiscFormat::HdDvd,
            "sanity: image is HD-DVD, not DVD"
        );
        assert!(!disc_scan.titles.is_empty(), "sanity: a title was parsed");
        assert!(
            disc_scan.css.is_none(),
            "the CSS crack must never run against a non-DVD (HD-DVD/AACS) image, \
             even when its content happens to be CSS-crackable"
        );
    }

    // ── scan_with's forced-subtitle probe: BdTs-only gate (finding 10) ──────

    /// A `PGS` subtitle title with the given `content_format`, one extent
    /// starting at `start_lba`, for the `probe_forced_subtitles_for_bdts_titles`
    /// container gate.
    fn pgs_title(content_format: ContentFormat, start_lba: u32) -> DiscTitle {
        DiscTitle {
            content_format,
            streams: vec![Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: false,
                qualifier: LabelQualifier::None,
                codec_data: None,
            })],
            extents: vec![Extent {
                start_lba,
                sector_count: 4,
            }],
            ..DiscTitle::empty()
        }
    }

    /// Records every LBA read — used to prove WHICH title's extents were
    /// actually touched, not merely that some read happened.
    struct ForcedProbeSpyReader {
        lbas: std::cell::RefCell<Vec<u32>>,
    }
    impl SectorSource for ForcedProbeSpyReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.lbas.borrow_mut().push(lba);
            let n = (count as usize * 2048).min(buf.len());
            buf[..n].fill(0);
            Ok(n)
        }
    }

    // Probe gates on content_format == BdTs, NOT on whether a title declares
    // a PGS stream. The BdTs title (LBA 0) must be probed; the MpegPs title
    // (LBA 5000) must never be touched.
    #[test]
    fn probe_forced_subtitles_only_touches_bdts_titles() {
        let mut titles = vec![
            pgs_title(ContentFormat::BdTs, 0),
            pgs_title(ContentFormat::MpegPs, 5000),
        ];
        let mut reader = ForcedProbeSpyReader {
            lbas: std::cell::RefCell::new(Vec::new()),
        };
        Disc::probe_forced_subtitles_for_bdts_titles(&mut reader, &mut titles, None);
        let lbas = reader.lbas.borrow();
        assert!(
            !lbas.is_empty(),
            "the BdTs title must be probed (a read must occur)"
        );
        assert!(
            lbas.iter().all(|&l| l < 5000),
            "the MpegPs title's extent (LBA 5000) must never be read: {lbas:?}"
        );
    }

    // ── scan_with's capacity_bytes feeds canonical_title_order (finding 10) ─
    // Two HD-DVD .evo clips (each its own title) with the given DECLARED byte
    // sizes; scan_with's capacity ranking depends on ICB-declared size only.
    fn hddvd_two_clip_disc(
        main_bytes: u32,
        other_bytes: u32,
    ) -> (crate::udf::fixture::MemDisc, udf::UdfFs) {
        use crate::udf::fixture::*;
        let files = vec![
            file("MAIN.EVO", 100, 5_000, main_bytes as u64, true),
            file("OTHER.EVO", 101, 50_000, other_bytes as u64, true),
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        (disc, udf)
    }

    // A cancelled Halt must stop the HD-DVD title scan (bounded but big, so a
    // Stop that only takes effect after the enumerator returns is no Stop at
    // all), and must not report a half-enumerated disc as a successful scan.
    #[test]
    fn scan_with_cancelled_halt_stops_the_hddvd_title_scan() {
        use crate::udf::fixture::PART_START;

        /// Counts reads that land on CLIP DATA (at or past the first clip's
        /// data extent) — i.e. the per-clip stream probing, the expensive part
        /// of the scan. Everything below that is filesystem metadata.
        struct CountingReader<'a> {
            inner: &'a mut crate::udf::fixture::MemDisc,
            clip_reads: usize,
        }
        impl SectorSource for CountingReader<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                recovery: bool,
            ) -> Result<usize> {
                if lba >= PART_START + 5_000 {
                    self.clip_reads += 1;
                }
                self.inner.read_sectors(lba, count, buf, recovery)
            }
        }

        let (mut disc, udf) = hddvd_two_clip_disc(3_000_000, 5_000_000);
        let halt = crate::halt::Halt::new();
        halt.cancel();
        let opts = ScanOptions {
            halt: Some(halt),
            ..Default::default()
        };
        let mut reader = CountingReader {
            inner: &mut disc,
            clip_reads: 0,
        };
        let res = Disc::scan_fs(&mut reader, 3_997_952, &opts, udf);
        let clip_reads = reader.clip_reads;

        assert!(
            matches!(res, Err(Error::Halted)),
            "a cancelled scan must say so, not return a partial title list as \
             a completed scan; got {:?}",
            res.map(|d| d.titles.len())
        );
        assert_eq!(
            clip_reads, 0,
            "cancellation must be observed before the per-clip stream probes, \
             not after all of them"
        );
    }

    // scan_with must hand ScanOptions::halt to the BLU-RAY enumerator: other tests call
    // scan_bluray_titles directly, so this wiring was otherwise uncovered.
    #[test]
    fn scan_with_passes_the_halt_flag_to_the_bluray_enumerator() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "BDMV".into(),
                icb_lba: 12,
                dir_data_lba: 13,
                files: Vec::new(),
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        // Sanity: the same disc scans clean when nothing is cancelled, so a
        // pass below cannot be some unrelated failure wearing Halted.
        assert!(
            Disc::scan_fs(&mut disc, 500_000, &ScanOptions::default(), udf).is_ok(),
            "fixture must scan successfully when not cancelled"
        );

        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let halt = crate::halt::Halt::new();
        halt.cancel();
        let opts = ScanOptions {
            halt: Some(halt),
            ..Default::default()
        };
        let res = Disc::scan_fs(&mut disc, 500_000, &opts, udf);
        assert!(
            matches!(res, Err(Error::Halted)),
            "a cancelled BD scan must say so; returning a title list built \
             after Stop reports a truncated enumeration as a completed one. \
             Got {:?}",
            res.map(|d| d.titles.len())
        );
    }

    /// The DVD half of the same wiring, and the same reasoning.
    ///
    /// Mutation: `Self::scan_dvd_titles(reader, &udf_fs, None)?` fails here.
    #[test]
    fn scan_with_passes_the_halt_flag_to_the_dvd_enumerator() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "VIDEO_TS".into(),
                icb_lba: 50,
                dir_data_lba: 51,
                files: vec![
                    file_with("VIDEO_TS.IFO", 60, 5000, dvd_vmg_bytes(), true),
                    file_with("VTS_01_0.IFO", 62, 6000, dvd_vts_bytes(1000, 10, 10), true),
                ],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        assert!(
            Disc::scan_fs(&mut disc, 500_000, &ScanOptions::default(), udf).is_ok(),
            "fixture must scan successfully when not cancelled"
        );

        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let halt = crate::halt::Halt::new();
        halt.cancel();
        let opts = ScanOptions {
            halt: Some(halt),
            ..Default::default()
        };
        let res = Disc::scan_fs(&mut disc, 500_000, &opts, udf);
        assert!(
            matches!(res, Err(Error::Halted)),
            "a cancelled DVD scan must say so, not hand back whatever it had \
             enumerated so far as a finished scan. Got {:?}",
            res.map(|d| d.titles.len())
        );
    }

    // A Stop on a LIVE DRIVE never touches ScanOptions::halt (Drive fails every SCSI command
    // with Error::Halted itself); the HD-DVD enumerator must not swallow that into a successful
    // scan.
    #[test]
    fn halted_reads_do_not_report_the_hddvd_scan_as_successful() {
        use crate::udf::fixture::PART_START;

        /// Fails clip-data reads the way a live drive does once Stop is
        /// pressed; filesystem metadata below the first clip still resolves,
        /// so the scan gets far enough to enumerate titles.
        struct HaltingReader<'a> {
            inner: &'a mut crate::udf::fixture::MemDisc,
        }
        impl SectorSource for HaltingReader<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                recovery: bool,
            ) -> Result<usize> {
                if lba >= PART_START + 5_000 {
                    return Err(Error::Halted);
                }
                self.inner.read_sectors(lba, count, buf, recovery)
            }
        }

        let (mut disc, udf) = hddvd_two_clip_disc(3_000_000, 5_000_000);
        let mut reader = HaltingReader { inner: &mut disc };
        let res = Disc::scan_fs(&mut reader, 3_997_952, &ScanOptions::default(), udf);
        assert!(
            matches!(res, Err(Error::Halted)),
            "reads cancelled by the drive's own halt flag must surface as a \
             cancelled scan, not as titles that merely look stream-less; got \
             {:?}",
            res.map(|d| d
                .titles
                .iter()
                .map(|t| (t.playlist.clone(), t.streams.len()))
                .collect::<Vec<_>>())
        );
    }

    // capacity_bytes = capacity * 2048 feeds canonical_title_order's oversize threshold; chosen
    // so a `*` -> `+` mutation flips titles[0], not merely a wrong numeric threshold.
    #[test]
    fn scan_with_capacity_bytes_uses_multiplication_not_addition() {
        const CAPACITY_SECTORS: u32 = 3_997_952; // *2048 = 8_187_805_696; +2048 = 4_000_000
        let (mut disc, udf) = hddvd_two_clip_disc(3_000_000, 5_000_000);
        let scanned = Disc::scan_fs(&mut disc, CAPACITY_SECTORS, &ScanOptions::default(), udf)
            .expect("scan_with must succeed on this synthetic HD-DVD image");
        assert_eq!(
            scanned.titles.first().map(|t| t.playlist.as_str()),
            Some("OTHER.EVO"),
            "with the real *2048 capacity both titles are non-oversize, so the \
             bigger clip (OTHER, 5_000_000) must rank first: {:?}",
            scanned
                .titles
                .iter()
                .map(|t| &t.playlist)
                .collect::<Vec<_>>()
        );
    }

    // Same mechanism as above, tuned to catch a `*` -> `/` mutation instead.
    #[test]
    fn scan_with_capacity_bytes_uses_multiplication_not_division() {
        const CAPACITY_SECTORS: u32 = 204_800_000; // *2048 = huge; /2048 = 100_000
        let (mut disc, udf) = hddvd_two_clip_disc(1_000, 5_000_000);
        let scanned = Disc::scan_fs(&mut disc, CAPACITY_SECTORS, &ScanOptions::default(), udf)
            .expect("scan_with must succeed on this synthetic HD-DVD image");
        assert_eq!(
            scanned.titles.first().map(|t| t.playlist.as_str()),
            Some("OTHER.EVO"),
            "with the real *2048 capacity both titles are non-oversize, so the \
             bigger clip (OTHER, 5_000_000) must rank first: {:?}",
            scanned
                .titles
                .iter()
                .map(|t| &t.playlist)
                .collect::<Vec<_>>()
        );
    }

    // ── Unknown must not fabricate a plausible value (finding 2) ────────────
    // Resolution::Unknown has no dimensions, so pixels() must report none — not (0, 0) or a
    // plausible 1920x1080.
    #[test]
    fn unknown_resolution_reports_no_pixel_dimensions() {
        assert_eq!(
            Resolution::Unknown.pixels(),
            None,
            "an unknown resolution must report no dimensions, not a fabricated default"
        );
        assert_eq!(Resolution::R480i.pixels(), Some((720, 480)));
        assert_eq!(Resolution::R480p.pixels(), Some((720, 480)));
        assert_eq!(Resolution::R576i.pixels(), Some((720, 576)));
        assert_eq!(Resolution::R576p.pixels(), Some((720, 576)));
        assert_eq!(Resolution::R720p.pixels(), Some((1280, 720)));
        assert_eq!(Resolution::R1080i.pixels(), Some((1920, 1080)));
        assert_eq!(Resolution::R1080p.pixels(), Some((1920, 1080)));
        assert_eq!(Resolution::R2160p.pixels(), Some((3840, 2160)));
        assert_eq!(Resolution::R4320p.pixels(), Some((7680, 4320)));
    }

    // Every Unknown variant with a numeric accessor must report "nothing",
    // never a plausible default. FrameRate::Unknown reports 0/1 (not 0 fps)
    // because callers divide by the numerator.
    #[test]
    fn no_unknown_variant_fabricates_a_numeric_value() {
        assert_eq!(
            Resolution::Unknown.pixels(),
            None,
            "the strongest form of this rule: not even a zero pair, which reads              as a usable value and was serialised into an MP4 as a 0x0 track"
        );
        assert_eq!(FrameRate::Unknown.as_fraction(), (0, 1));
        assert_eq!(AudioChannels::Unknown.count(), 0);
        assert_eq!(SampleRate::Unknown.hz(), 0.0);
        // Not numeric, but the same rule: the token names the unknown, it does
        // not name a plausible colorimetry.
        assert_eq!(ColorSpace::Unknown.id(), "unknown");
    }

    // ── BD-ROM stream_coding_type 0xA2 (finding 3) ──────────────────────────
    // 0xA2 is SECONDARY DTS-HD (lossy LBR), NOT DTS-HD Master Audio (0x86).
    // Literals, not consts::coding_type names (renaming can't pass vacuously).
    #[test]
    fn secondary_dts_hd_0xa2_is_lossy_not_master_audio() {
        assert_ne!(
            Codec::from_coding_type(0xA2),
            Codec::DtsHdMa,
            "0xA2 is the lossy secondary DTS-HD stream, not lossless Master Audio"
        );
        assert_eq!(Codec::from_coding_type(0xA2), Codec::DtsHdHr);
        // The lossless primary keeps its own code, unchanged.
        assert_eq!(Codec::from_coding_type(0x86), Codec::DtsHdMa);
        // ...and both remain audio, so the STN/PMT walker still enumerates them.
        assert_eq!(Codec::from_coding_type(0xA2).kind(), CodecKind::Audio);
    }

    // ── extent-end arithmetic saturates (finding 5) ─────────────────────────
    // byte_offset_in_title must saturate its extent-end arithmetic: a
    // malformed extent near u32::MAX otherwise panics in debug or wraps.
    #[test]
    fn byte_offset_in_title_saturates_the_extent_end() {
        let title = title_with_extents(0, &[(u32::MAX - 10, 100)]);
        // 9 sectors past the extent start; 2048 is the ECMA-167 / UDF logical
        // sector size, so the byte offset is 9 * 2048.
        let got = byte_offset_in_title(u32::MAX - 1, &title);
        assert_eq!(got, Some(18_432));
        // The saturated end is u32::MAX (exclusive), so the very last
        // addressable LBA is still inside the extent.
        assert_eq!(
            byte_offset_in_title(u32::MAX - 10, &title),
            Some(0),
            "the extent start itself maps to offset 0"
        );
    }

    /// `AacsState` (public via `Disc.aacs`) and `Key` (the key-transport enum)
    /// must never print raw key bytes on `{:?}`. Sentinel 213 (0xD5); non-secret
    /// fields below are not 213.
    #[test]
    fn aacs_state_and_key_debug_are_redacted() {
        let st = AacsState {
            version: 2,
            bus_encryption: true,
            mkb_version: Some(77),
            disc_hash: "0xAA".into(),
            volume_id: [0xD5; 16],
            uk_ro: vec![1, 2, 3],
            mkb: vec![4, 5, 6],
        };
        let d = format!("{st:?}");
        assert!(!d.contains("213"), "AacsState leaked key bytes: {d}");
        assert!(d.contains("redacted"), "AacsState missing marker: {d}");

        for k in [
            Key::Unit(vec![(1, [0xD5; 16])]),
            Key::Volume([0xD5; 16]),
            Key::Processing(vec![[0xD5; 16]]),
            Key::Media(vec![[0xD5; 16]]),
        ] {
            let d = format!("{k:?}");
            assert!(!d.contains("213"), "Key leaked bytes: {d}");
            assert!(d.contains("redacted"), "Key missing marker: {d}");
        }
    }

    // ── encrypted-content map (`merged_extents` core) ────────────────────────

    fn ext(start_lba: u32, sector_count: u32) -> Extent {
        Extent {
            start_lba,
            sector_count,
        }
    }

    #[test]
    fn merged_extents_empty_is_empty() {
        assert_eq!(merged_extents([].iter()), Vec::<(u32, u32)>::new());
    }

    #[test]
    fn merged_extents_single() {
        assert_eq!(merged_extents([ext(100, 50)].iter()), vec![(100, 50)]);
    }

    /// Out-of-order extents from several titles, with an OVERLAP, an ADJACENT
    /// pair, and a DISJOINT one, must come back sorted + merged + disjoint.
    #[test]
    fn merged_extents_unions_sorts_and_merges() {
        // [300,310) ; [100,150) ; [150,200) adjacent→merges with prev ;
        // [120,160) overlaps [100,150)&[150,200) ; [500,505) disjoint.
        let v = [
            ext(300, 10),
            ext(100, 50),
            ext(150, 50),
            ext(120, 40),
            ext(500, 5),
        ];
        assert_eq!(
            merged_extents(v.iter()),
            vec![(100, 100), (300, 10), (500, 5)],
            "[100,200) merged, [300,310), [500,505)"
        );
    }

    /// The same clip referenced by two titles (identical extents) de-duplicates
    /// to a single range — no double-counting of shared content.
    #[test]
    fn merged_extents_dedups_shared_clip() {
        let v = [ext(100, 50), ext(100, 50)];
        assert_eq!(merged_extents(v.iter()), vec![(100, 50)]);
    }

    // A Windows-form path (\\.\CdRom0, \\.\D:) must never fall through to
    // the block default (8192 sectors, over the optical 510-sector cap): it
    // has no forward slash, so the Linux-sysfs name parse cannot apply.
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

    // read_aacs_inputs on a missing ISO must surface E_IO_ERROR (5000), NOT
    // AacsNoKeys (7000) — else callers dispatching on .code() would tell the
    // user "check your KEYDB" when the ISO simply doesn't exist.
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

    #[test]
    fn locate_ranges_at_risk_in_vs_out_of_feature() {
        // Honest-"Maybe" behaviour: in-feature damage counts as movie time at risk;
        // out-of-feature damage is still located but reads 0:00.
        // bps = size_bytes / duration_secs = 4096 B/s → 4096 B == 1000 ms.
        let mut title = title_with_video(Codec::Hevc, Resolution::R2160p);
        title.duration_secs = 100.0;
        title.size_bytes = 409_600;
        // Feature extent = sectors [10,110) → bytes [20480, 225280).
        title.extents = vec![Extent {
            start_lba: 10,
            sector_count: 100,
        }];

        // In-feature pending range: 4096 B == 1000 ms of movie at risk.
        let in_feat = locate_ranges(&[(40_960, 4096)], &title);
        assert_eq!(in_feat.num_ranges, 1);
        assert!(
            (in_feat.main_at_risk_ms - 1000.0).abs() < 1.0,
            "in-feature damage must count as at-risk movie time, got {}",
            in_feat.main_at_risk_ms
        );

        // Out-of-feature range: located, but zero movie time at risk.
        let out_feat = locate_ranges(&[(2_000_000, 100_000)], &title);
        assert_eq!(out_feat.num_ranges, 1, "still a located range");
        assert_eq!(
            out_feat.main_at_risk_ms, 0.0,
            "out-of-feature damage must not read as movie loss"
        );
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
                feed_span: None,
                clip_id: format!("{i:05}"),
                in_time: 0,
                out_time: 1,
                duration_secs: 1.0,
                source_packets: 0,
            })
            .collect();
        t
    }

    // Regression for branching-UHD title ordering: mirrors the observed
    // *The Amateur (2025)* layout (4h13m/92.4GB/253-clip play-all vs the
    // real 2h02m/57.2GB/1-clip feature, 58.5GB disc).
    #[test]
    fn canonical_order_pushes_oversize_play_all_behind_real_main() {
        const CAPACITY: u64 = 58_500_000_000; // 58.5 GB
        let mut titles = [
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

    /// Non-branching disc: largest title is the movie. With realistic sizes
    /// (bytes track duration for same-codec content) size-first yields the same
    /// ranking as duration — biggest/longest feature, then extra, then menu.
    #[test]
    fn canonical_order_preserves_natural_ranking_on_normal_disc() {
        const CAPACITY: u64 = 60_000_000_000;
        let mut titles = [
            title_with("00100.mpls", 600.0, 500_000_000, 1), // 10 min menu (small)
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

    // Contract pin (owner-flagged): `freemkv -t 1` maps to titles[0], which
    // canonical_title_order orders main-feature-first, so titles[0] IS the
    // movie. DVD-9 fixture: 1h49m feature alongside a menu loop and extra.
    #[test]
    fn title_index_0_is_main_feature_dvd_the_dash_t_1_contract() {
        const DVD9: u64 = 7_900_000_000; // dual-layer DVD
        let mut titles = [
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

    /// An AACS MKB Type-and-Version record (0x10) carrying `raw_type` — the only
    /// record [`Disc::detect_disc_format`] reads to decide BD/UHD/FMTS.
    fn mkb_type_record(raw_type: u32) -> Vec<u8> {
        let mut v = vec![0x10, 0x00, 0x00, 0x0c]; // record type 0x10, rec_len 12
        v.extend_from_slice(&raw_type.to_be_bytes()); // MKBType @ body offset 0
        v.extend_from_slice(&0u32.to_be_bytes()); // version @ body offset 4
        v
    }

    /// FORMAT derives from the AACS MKB generation, not the tree or filesystem:
    /// 2.1 → FMTS, 2.0 → UHD, 1.0 → BD — all from the MKB Type record.
    #[test]
    fn detect_format_from_mkb_generation() {
        use crate::udf::fixture::*;
        for (raw, expected) in [
            (0x4815_1003u32, DiscFormat::Fmts),
            (0x4814_1003u32, DiscFormat::Uhd),
            (0x0004_1003u32, DiscFormat::BluRay),
        ] {
            let mut disc = MemDisc::new();
            let root = DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: Vec::new(),
                subdirs: vec![
                    DirSpec {
                        name: "BDMV".into(),
                        icb_lba: 12,
                        dir_data_lba: 13,
                        files: Vec::new(),
                        subdirs: vec![],
                    },
                    DirSpec {
                        name: "AACS".into(),
                        icb_lba: 14,
                        dir_data_lba: 15,
                        files: vec![file_with(
                            "MKB_RO.inf",
                            16,
                            5000,
                            mkb_type_record(raw),
                            true,
                        )],
                        subdirs: vec![],
                    },
                ],
            };
            build_udf_skeleton(&mut disc, 10);
            lay_dir(&mut disc, &root);
            let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
            assert_eq!(
                Disc::detect_disc_format(&mut disc, &udf, &[]),
                expected,
                "MKB type {raw:#010x}"
            );
        }
    }

    /// HD-DVD is a tree-level format — recognized from `HVDVD_TS/`, no MKB.
    #[test]
    fn detect_format_hddvd_from_tree() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        assert_eq!(
            Disc::detect_disc_format(&mut disc, &udf, &[]),
            DiscFormat::HdDvd
        );
    }

    // read_mkb_content must GROW its bounded prefix when the real record stream runs longer.
    // Run inside a watchdog: some growth-step mutants spin forever re-reading a zero-length
    // prefix.
    #[test]
    fn read_mkb_content_grows_prefix_past_16mib_when_records_run_longer() {
        use crate::udf::fixture::*;

        const REC_LEN: usize = 1024 * 1024; // 1 MiB, header included
        const N_RECORDS: usize = 20; // 20 MiB of real record stream
        const TOTAL: usize = N_RECORDS * REC_LEN;

        let mut mkb = Vec::with_capacity(TOTAL + 4);
        for _ in 0..N_RECORDS {
            mkb.push(0x04); // REC_SUBSET_DIFFERENCE — any non-zero, non-terminator type
            let len = REC_LEN as u32;
            mkb.push((len >> 16) as u8);
            mkb.push((len >> 8) as u8);
            mkb.push(len as u8);
            mkb.resize(mkb.len() + (REC_LEN - 4), 0xAA);
        }
        mkb.extend_from_slice(&[0, 0, 0, 0]); // explicit end-of-records marker
        assert_eq!(mkb.len(), TOTAL + 4);

        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "AACS".into(),
                icb_lba: 12,
                dir_data_lba: 13,
                files: vec![file_with("MKB_RO.inf", 14, 1000, mkb, true)],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut disc = disc;
            let r = Disc::read_mkb_content(&mut disc, &udf).map(|v| v.len());
            let _ = tx.send(r);
        });
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect(
                "read_mkb_content did not terminate — the prefix-growth loop \
                 spun forever instead of converging on the record-stream length",
            )
            .expect("read_mkb_content failed");
        assert_eq!(
            got, TOTAL,
            "must recover the full 20 MiB record stream, not the 16 MiB starting prefix"
        );
    }

    // ── identify(): AACS-directory encrypted gate (finding 6) ──────────────
    // A ScsiTransport serving a synthetic UDF image through real READ(10),
    // so Disc::identify is exercised end-to-end, not mocked itself.
    struct MemDiscDrive {
        mem: crate::udf::fixture::MemDisc,
        last_lba: u32,
    }
    impl crate::scsi::ScsiTransport for MemDiscDrive {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: crate::scsi::DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<crate::scsi::ScsiResult> {
            match cdb.first().copied() {
                Some(op) if op == crate::scsi::SCSI_READ_CAPACITY => {
                    data[0..4].copy_from_slice(&self.last_lba.to_be_bytes());
                    data[4..8].copy_from_slice(&2048u32.to_be_bytes());
                    Ok(crate::scsi::ScsiResult {
                        status: 0,
                        bytes_transferred: 8,
                        sense: [0u8; 32],
                    })
                }
                Some(op) if op == crate::scsi::SCSI_READ_10 => {
                    let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
                    let count = u16::from_be_bytes([cdb[7], cdb[8]]);
                    let n = self.mem.read_sectors(lba, count, data, false)?;
                    Ok(crate::scsi::ScsiResult {
                        status: 0,
                        bytes_transferred: n,
                        sense: [0u8; 32],
                    })
                }
                _ => Ok(crate::scsi::ScsiResult {
                    status: 0,
                    bytes_transferred: 0,
                    sense: [0u8; 32],
                }),
            }
        }
    }

    /// Build a synthetic disc with (or without) a root `/AACS` directory
    /// and/or a nested `/BDMV/AACS` directory, then run the real
    /// `Disc::identify` end-to-end through a mocked `Drive`.
    fn identify_with_aacs_dirs(has_aacs: bool, has_bdmv_aacs: bool) -> DiscId {
        use crate::udf::fixture::*;
        let mut subdirs = Vec::new();
        if has_aacs {
            subdirs.push(DirSpec {
                name: "AACS".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: Vec::new(),
            });
        }
        if has_bdmv_aacs {
            subdirs.push(DirSpec {
                name: "BDMV".into(),
                icb_lba: 30,
                dir_data_lba: 31,
                files: Vec::new(),
                subdirs: vec![DirSpec {
                    name: "AACS".into(),
                    icb_lba: 32,
                    dir_data_lba: 33,
                    files: Vec::new(),
                    subdirs: Vec::new(),
                }],
            });
        }
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs,
        };
        let mut mem = MemDisc::new();
        build_udf_skeleton(&mut mem, 10);
        lay_dir(&mut mem, &root);
        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(MemDiscDrive {
            mem,
            last_lba: 99_999,
        }));
        Disc::identify(&mut drive).expect("identify must succeed on this synthetic image")
    }

    /// A root `/AACS` directory alone (the near-universal retail BD/UHD
    /// shape) must report `encrypted == true`.
    #[test]
    fn identify_reports_encrypted_for_aacs_dir_alone() {
        let id = identify_with_aacs_dirs(true, false);
        assert!(id.encrypted, "/AACS alone must report encrypted");
    }

    // A nested /BDMV/AACS alone must ALSO report encrypted == true —
    // degrading the `||` to `&&` would report almost every retail BD as
    // unencrypted, since real discs rarely carry both paths at once.
    #[test]
    fn identify_reports_encrypted_for_bdmv_aacs_dir_alone() {
        let id = identify_with_aacs_dirs(false, true);
        assert!(id.encrypted, "/BDMV/AACS alone must report encrypted");
    }

    /// Neither AACS path present must report `encrypted == false`.
    #[test]
    fn identify_reports_unencrypted_when_no_aacs_dir_exists() {
        let id = identify_with_aacs_dirs(false, false);
        assert!(
            !id.encrypted,
            "no AACS directory at all must report unencrypted"
        );
    }

    // Title selection is by largest physical size, NOT clip count/duration: a
    // 57 GB/11-clip feature must outrank a 1-clip bonus reel and a
    // long-but-tiny decoy play-all (91 reused clips, 1h31m, 0.4 GB).
    #[test]
    fn canonical_title_order_picks_largest_feature() {
        fn title_sized(size_bytes: u64, duration_secs: f64, n_clips: usize) -> DiscTitle {
            DiscTitle {
                playlist: String::new(),
                playlist_id: 0,
                duration_secs,
                size_bytes,
                clips: (0..n_clips)
                    .map(|i| Clip {
                        feed_span: None,
                        clip_id: format!("{i:05}"),
                        in_time: 0,
                        out_time: 0,
                        duration_secs: 0.0,
                        source_packets: 0,
                    })
                    .collect(),
                streams: Vec::new(),
                chapters: Vec::new(),
                extents: Vec::new(),
                content_format: ContentFormat::BdTs,
                codec_privates: Vec::new(),
            }
        }
        let capacity = 66_000_000_000u64;
        let feature = title_sized(57_000_000_000, 7860.0, 11); // 2h11m, 11 chapters
        let bonus = title_sized(1_200_000_000, 600.0, 1); // 10m, 1 clip
        let decoy = title_sized(400_000_000, 5460.0, 91); // 1h31m but tiny (reused)
        let mut v = [bonus, decoy, feature];
        v.sort_by(|a, b| Disc::canonical_title_order(a, b, capacity));
        assert_eq!(
            v[0].size_bytes, 57_000_000_000,
            "the largest real title is the main feature"
        );
    }

    // Issue #45: equal size/duration/audio siblings differing only on video-count or
    // subtitle-count must pick the stream-richer playlist (not lowest id); truly
    // identical siblings still fall through to lowest-id determinism.
    #[test]
    fn canonical_title_order_prefers_stream_richer_sibling_issue_45() {
        let cap = 100_000_000_000u64;
        let base = |id: u16| DiscTitle {
            playlist_id: id,
            size_bytes: 57_000_000_000,
            duration_secs: 8000.0,
            ..title_with_video(Codec::Hevc, Resolution::R2160p)
        };
        // Higher-id sibling carries an extra VIDEO stream (a Dolby Vision EL).
        let mut richer_v = base(801);
        richer_v.streams.push(Stream::Video(VideoStream {
            pid: 0x1015,
            codec: Codec::Hevc,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: true,
            label: String::new(),
            measured_cicp: None,
        }));
        let mut vv = [base(40), richer_v];
        vv.sort_by(|a, b| Disc::canonical_title_order(a, b, cap));
        assert_eq!(
            vv[0].playlist_id, 801,
            "the video-richer sibling (Dolby Vision EL) must win over the lowest id"
        );

        // Higher-id sibling carries an extra SUBTITLE track.
        let mut richer_s = base(801);
        richer_s.streams.push(Stream::Subtitle(SubtitleStream {
            pid: 0x12a4,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        }));
        let mut ss = [base(40), richer_s];
        ss.sort_by(|a, b| Disc::canonical_title_order(a, b, cap));
        assert_eq!(
            ss[0].playlist_id, 801,
            "the subtitle-richer sibling must win over the lowest id"
        );

        // Truly identical siblings → lowest-id determinism preserved.
        let mut ii = [base(808), base(800)];
        ii.sort_by(|a, b| Disc::canonical_title_order(a, b, cap));
        assert_eq!(
            ii[0].playlist_id, 800,
            "identical seamless siblings still pick the lowest id"
        );
    }

    // L088: canonical_title_order's final tiebreak (playlist_id) is a real sort
    // key, same as the other 6 — the const naming them must not omit it.
    #[test]
    fn canonical_title_order_keys_names_every_sort_key_including_the_tiebreak() {
        assert_eq!(
            Disc::CANONICAL_TITLE_ORDER_KEYS.last(),
            Some(&"lowest-playlist-id"),
            "the const must name the comparator's final tiebreak key, not leave \
             callers to append it by hand"
        );
    }

    /// A BD title carrying video, `id`, `dur`/`size`, and the given clip ids.
    fn bd_title(playlist: &str, id: u16, dur: f64, size: u64, clip_ids: &[&str]) -> DiscTitle {
        DiscTitle {
            playlist: playlist.into(),
            playlist_id: id,
            duration_secs: dur,
            size_bytes: size,
            clips: clip_ids
                .iter()
                .map(|c| Clip {
                    feed_span: None,
                    clip_id: (*c).into(),
                    in_time: 0,
                    out_time: 1,
                    duration_secs: 1.0,
                    source_packets: 0,
                })
                .collect(),
            ..title_with_video(Codec::Hevc, Resolution::R2160p)
        }
    }

    /// A BD title as [`bd_title`], plus a chapter table of `n_chapters` marks
    /// evenly spanning `dur` (first at 0, last at `dur`) — models a COMPLETE
    /// feature presentation for the issue #45 seamless-branch guard. `n_chapters`
    /// must be ≥ 2 for the marks to span any runtime.
    fn bd_title_chaptered(
        playlist: &str,
        id: u16,
        dur: f64,
        size: u64,
        clip_ids: &[&str],
        n_chapters: usize,
    ) -> DiscTitle {
        let chapters = (0..n_chapters)
            .map(|i| Chapter {
                time_secs: if n_chapters <= 1 {
                    0.0
                } else {
                    i as f64 * dur / (n_chapters - 1) as f64
                },
                name: (i + 1).to_string(),
            })
            .collect();
        DiscTitle {
            chapters,
            ..bd_title(playlist, id, dur, size, clip_ids)
        }
    }

    /// A BD title with `n_chapters` marks clustered in the first `span_secs` of a
    /// `dur`-second runtime (first at 0, last at `span_secs`) — lets a test drive
    /// the CHAPTER_SPAN_MIN_FRAC boundary independently of chapter count.
    fn bd_title_chaptered_span(
        playlist: &str,
        id: u16,
        dur: f64,
        size: u64,
        clip_ids: &[&str],
        n_chapters: usize,
        span_secs: f64,
    ) -> DiscTitle {
        let chapters = (0..n_chapters)
            .map(|i| Chapter {
                time_secs: if n_chapters <= 1 {
                    0.0
                } else {
                    i as f64 * span_secs / (n_chapters - 1) as f64
                },
                name: (i + 1).to_string(),
            })
            .collect();
        DiscTitle {
            chapters,
            ..bd_title(playlist, id, dur, size, clip_ids)
        }
    }

    // Issue #45 boundary: marks that cluster in the first ~20% of runtime span
    // < CHAPTER_SPAN_MIN_FRAC of duration, so they are NOT a complete feature.
    #[test]
    fn is_complete_feature_presentation_false_when_marks_cluster_early() {
        // 5 marks over the first 20% (span 0.2 < 0.5 of duration).
        let clustered =
            bd_title_chaptered_span("00001.mpls", 1, 1000.0, 1_000_000, &["00001"], 5, 200.0);
        assert!(
            !is_complete_feature_presentation(&clustered),
            "marks spanning < 0.5 of runtime are not a complete feature"
        );
    }

    // Issue #45 boundary: marks spanning exactly CHAPTER_SPAN_MIN_FRAC of the
    // runtime (with >= MIN_FEATURE_CHAPTERS marks) DO look like a real feature.
    #[test]
    fn is_complete_feature_presentation_true_at_span_threshold() {
        // 2 marks (== MIN_FEATURE_CHAPTERS) spanning exactly 0.5 of duration.
        let at_threshold =
            bd_title_chaptered_span("00001.mpls", 1, 1000.0, 1_000_000, &["00001"], 2, 500.0);
        assert!(
            is_complete_feature_presentation(&at_threshold),
            "marks spanning >= 0.5 of runtime with enough marks are a complete feature"
        );
    }

    /// Same, but with an empty STN stream list — models a title whose FIRST
    /// PlayItem is a non-video bumper (`has_video()` is false; `streams` reflect
    /// PlayItem 0 only).
    fn bd_title_no_stn_video(
        playlist: &str,
        id: u16,
        dur: f64,
        size: u64,
        clip_ids: &[&str],
    ) -> DiscTitle {
        DiscTitle {
            streams: Vec::new(),
            ..bd_title(playlist, id, dur, size, clip_ids)
        }
    }

    // Regression for a real UHD decoy hang: 00245.mpls is a WRAPPER COMPOSITE
    // (bumper+feature+outro), LONGEST/LARGEST; demoted STRUCTURALLY by the chapter
    // table (issue #45) — wrapped feature 00001 is chaptered, the decoy is not.
    #[test]
    fn main_feature_order_demotes_wrapper_composite_decoy() {
        let decoy = bd_title_no_stn_video(
            "00245.mpls",
            245,
            8350.3,
            78_427_502_592,
            &["00339", "00001", "00336"],
        );
        let feature = bd_title_chaptered("00001.mpls", 1, 8038.4, 76_525_627_392, &["00001"], 12);
        let extra = bd_title("00248.mpls", 248, 794.2, 60_000_000_000, &["00248"]);
        let capacity = 88_000_000_000u64;

        // Pre-gate: the physical comparator ranks the larger/longer decoy FIRST.
        assert_eq!(
            Disc::canonical_title_order(&decoy, &feature, capacity),
            std::cmp::Ordering::Less,
            "pre-gate physical order would pick the wrapper composite decoy"
        );
        // The decoy is classified a composite; the feature is standalone.
        let ranks = Disc::rank_titles(&[decoy.clone(), feature.clone()], None, None);
        assert!(ranks[0].composite, "the wrapper decoy is a composite");
        assert!(!ranks[1].composite, "the feature it wraps is standalone");

        let mut titles = vec![decoy, feature, extra];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "the standalone feature is selected as titles[0]"
        );
        assert_eq!(
            titles[2].playlist_id, 245,
            "the wrapper composite decoy is demoted to the back"
        );
    }

    // The O(n^2 * clips) composite scan is work-bounded: a pathological
    // title/clip count must skip it (degrade to has-video + canonical
    // order); at a normal count the wrapper is still detected.
    #[test]
    fn composite_scan_is_work_bounded() {
        let decoy = bd_title(
            "00245.mpls",
            245,
            8350.0,
            78_000_000_000,
            &["dead", "00001"],
        );
        // The wrapped feature carries the real chapter table (issue #45), so the
        // bumper limb correctly detects 00245 as its wrapper composite.
        let feature = bd_title_chaptered("00001.mpls", 1, 8000.0, 76_000_000_000, &["00001"], 12);
        let small = vec![decoy.clone(), feature.clone()];
        assert!(
            Disc::rank_titles(&small, None, None)[0].composite,
            "at a normal title count the wrapper is detected as composite"
        );
        // n^2 * max_clips over the budget (6000^2 * 2 = 7.2e7 > 5e7): the scan is
        // skipped and nothing is flagged composite.
        let mut many = vec![decoy, feature];
        while many.len() < 6000 {
            let i = many.len() as u16;
            many.push(bd_title("00099.mpls", i, 100.0, 1_000_000, &["x"]));
        }
        assert!(
            !Disc::rank_titles(&many, None, None)[0].composite,
            "above the work budget the composite scan is skipped, not run"
        );
    }

    // Audit T2: a composite that ALSO carries video (old has-video gate
    // couldn't see this) is still demoted below the standalone feature it
    // wraps, by clip-set containment alone.
    #[test]
    fn main_feature_order_demotes_video_composite() {
        let composite = bd_title(
            "00245.mpls",
            245,
            8350.3,
            78_427_502_592,
            &["00001", "00336"],
        );
        // The wrapped feature carries the real chapter table (issue #45); the
        // video-bearing wrapper carries none, so it stays demoted.
        let feature = bd_title_chaptered("00001.mpls", 1, 8038.4, 76_525_627_392, &["00001"], 12);
        let capacity = 88_000_000_000u64;

        let mut titles = vec![composite, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "a video-bearing composite must still lose to the standalone feature it contains"
        );
    }

    // Audit T3: capacity unknown (0, READ CAPACITY failed) disables the
    // fits-disc oversize gate, so an oversize play-all composite would win
    // on size; clip-set composite detection demotes it regardless.
    #[test]
    fn main_feature_order_demotes_oversize_composite_when_capacity_unknown() {
        let playall = bd_title(
            "00099.mpls",
            99,
            15_180.0,
            92_400_000_000,
            &["00800", "00801", "00802", "00803"],
        );
        let feature = bd_title("00800.mpls", 800, 7320.0, 57_200_000_000, &["00800"]);
        let capacity = 0u64; // unknown → oversize gate inert

        let mut titles = vec![playall, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 800,
            "the standalone feature wins even when the oversize gate is disabled"
        );
    }

    /// Issue #45 (SAFETY): the REAL Alita BD-J numbers. Feature 00800 (4 clips,
    /// 7317 s, 58.61 GB, 37 chapter marks) is a SUPERSET of its single-clip body
    /// 00703 ([00688], 6951 s, 56.88 GB, 1 mark). The body is 0.97 of the feature's
    /// size, so the size-only bumper limb fires (0.97 ≥ 0.90) while the body runs
    /// over 0.85 of the feature so the concat limb does NOT — the OLD gate demoted
    /// 00800 and ripped the body. The chapter-table guard keeps 00800: its body
    /// subset carries one mark (< MIN_FEATURE_CHAPTERS), not a complete feature.
    #[test]
    fn seamless_branch_feature_not_demoted_issue_45() {
        // 00800: real feature = body + intro/credits/seamless branch segments,
        // with a full 37-mark chapter table.
        let feature = bd_title_chaptered(
            "00800.mpls",
            800,
            7317.0,
            58_610_000_000,
            &["00687", "00688", "00674", "00689"],
            37,
        );
        // 00703: single body clip, a PROPER subset, 6951 s (~0.95 of 00800's
        // duration → concat limb inert) and 56.88 GB (~0.97 of its size → old
        // bumper limb would fire) — one chapter mark, not a feature.
        let decoy = bd_title_chaptered("00703.mpls", 703, 6951.0, 56_880_000_000, &["00688"], 1);
        let capacity = 88_000_000_000u64;

        // The body subset is a proper subset with ≥90 % size — the OLD gate
        // flagged 00800 composite on that alone. It must no longer be flagged.
        let ranks = Disc::rank_titles(&[feature.clone(), decoy.clone()], None, None);
        assert!(
            !ranks[0].composite,
            "the seamless-branch feature must not be flagged a wrapper composite"
        );
        assert!(!ranks[1].composite, "the shorter body subset is standalone");

        let mut titles = vec![decoy, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 800,
            "the full seamless-branch feature is selected, not the shorter body decoy"
        );
    }

    /// The seamless-branch guard must NOT weaken the real play-all case: a
    /// concat wrapper (00099, four distinct parts, ~4h13 = their SUM) whose body
    /// subset is much SHORTER (00800, one part, ~2h02, <half the runtime) is
    /// still flagged composite and demoted below that standalone part.
    #[test]
    fn true_play_all_concat_wrapper_still_demoted() {
        let playall = bd_title(
            "00099.mpls",
            99,
            15_180.0, // ~4h13 — the SUM of its four parts
            92_400_000_000,
            &["00800", "00801", "00802", "00803"],
        );
        // The real feature = one part, ~2h02, less than half the wrapper's runtime.
        let feature = bd_title("00800.mpls", 800, 7320.0, 57_200_000_000, &["00800"]);
        let capacity = 100_000_000_000u64;

        let ranks = Disc::rank_titles(&[playall.clone(), feature.clone()], None, None);
        assert!(
            ranks[0].composite,
            "a concat play-all whose subset is much shorter is still a composite"
        );
        assert!(!ranks[1].composite, "the standalone feature part is not");

        let mut titles = vec![playall, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 800,
            "the standalone feature wins; the concat play-all is demoted"
        );
    }

    /// Issue #45 (Alita BD-J): the disc has NO authoritative static playlist
    /// chain (First-Play is a BD-J Xlet, index title's playlist has an empty
    /// TableOfAccessiblePlayLists), so selection is a pure structural failsafe.
    /// The chaptered feature 00800 is a SUPERSET of its un-chaptered body subset
    /// 00703 that is 0.97 of its size — the size-only bumper limb would demote
    /// it. The chapter-table guard keeps 00800 (its body carries one mark).
    #[test]
    fn alita_bdj_body_subset_does_not_demote_chaptered_feature_issue_45() {
        let feature = bd_title_chaptered(
            "00800.mpls",
            800,
            7317.0,
            58_610_000_000,
            &["00687", "00688", "00674", "00689"],
            37,
        );
        let body = bd_title_chaptered("00703.mpls", 703, 6951.0, 56_880_000_000, &["00688"], 1);
        let capacity = 88_000_000_000u64;

        let ranks = Disc::rank_titles(&[feature.clone(), body.clone()], None, None);
        assert!(
            !ranks[0].composite,
            "the chaptered feature must not be flagged a wrapper composite of its body"
        );

        // No hint, no nav — the BD-J disc gives neither; selection is structural.
        let mut titles = vec![body, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 800,
            "the chaptered feature is selected, not its un-chaptered body subset"
        );
    }

    /// Issue #45 (secondary determinism): the nine feature variants 00800-00808
    /// share the body clip 00688 and differ only by opening logo, so they are
    /// equal in size/duration/audio — the lowest-playlist_id final tie-break
    /// deterministically picks 00800. All nine rank above the un-chaptered body
    /// subset 00703, and none is demoted as a composite of it.
    #[test]
    fn alita_full_family_ranks_above_body() {
        let mut titles: Vec<DiscTitle> = (0..9)
            .map(|k| {
                let id = 800 + k as u16;
                bd_title_chaptered(
                    &format!("008{k:02}.mpls"),
                    id,
                    7317.0,
                    58_610_000_000,
                    &[&format!("logo{k}"), "00688"],
                    37,
                )
            })
            .collect();
        // The bare body: a proper subset of every family member, one chapter mark,
        // 0.97 of their size (would trip the old bumper limb).
        titles.push(bd_title_chaptered(
            "00703.mpls",
            703,
            6951.0,
            56_880_000_000,
            &["00688"],
            1,
        ));
        let capacity = 88_000_000_000u64;

        let ranks = Disc::rank_titles(&titles, None, None);
        assert!(
            ranks.iter().all(|r| !r.composite),
            "no equal-size seamless sibling nor its body subset is a composite"
        );

        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 800,
            "the lowest-id feature variant is the deterministic pick among equals"
        );
        assert_eq!(
            titles.last().unwrap().playlist_id,
            703,
            "the smaller body subset ranks below the whole feature family"
        );
    }

    /// Issue #45 (SM3 topology, opposite of Alita): the real chaptered feature
    /// 00001 is the SUBSET; the decoy 00245 = [bumper][feature][outro] is the
    /// SUPERSET. Because the subset IS a complete feature presentation (real
    /// chapter table), the bumper limb still fires and the wrapper stays demoted.
    #[test]
    fn bumper_wrapper_with_chaptered_subset_still_demoted() {
        let wrapper = bd_title(
            "00245.mpls",
            245,
            8350.0,
            78_000_000_000,
            &["00339", "00001", "00336"],
        );
        // 0.98 of the wrapper's size (bumper limb) and a full chapter table.
        let feature = bd_title_chaptered("00001.mpls", 1, 8038.0, 76_500_000_000, &["00001"], 24);
        let capacity = 88_000_000_000u64;

        let ranks = Disc::rank_titles(&[wrapper.clone(), feature.clone()], None, None);
        assert!(
            ranks[0].composite,
            "a bumper wrapper of a CHAPTERED feature subset is still a composite"
        );
        assert!(
            !ranks[1].composite,
            "the chaptered feature subset is standalone"
        );

        let mut titles = vec![wrapper, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "the chaptered feature wins; its bumper wrapper is demoted"
        );
    }

    /// Issue #45: when the bumper-size subset carries NO chapter table it is not
    /// a complete feature, so the bumper limb does not fire and the SUPERSET is
    /// kept — the exact mechanism that saves Alita's 00800 over its body 00703.
    #[test]
    fn un_chaptered_bumper_subset_keeps_superset() {
        // Superset feature, chaptered.
        let superset = bd_title_chaptered(
            "00800.mpls",
            800,
            7317.0,
            58_610_000_000,
            &["00687", "00688", "00674", "00689"],
            30,
        );
        // Subset: 0.97 of the superset's size (bumper limb size), but NO chapters.
        let subset = bd_title("00703.mpls", 703, 6951.0, 56_880_000_000, &["00688"]);
        assert!(
            subset.chapters.is_empty(),
            "the body subset carries no marks"
        );
        let capacity = 88_000_000_000u64;

        let ranks = Disc::rank_titles(&[superset.clone(), subset.clone()], None, None);
        assert!(
            !ranks[0].composite,
            "an un-chaptered bumper-size subset must not demote its superset"
        );

        let mut titles = vec![subset, superset];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 800,
            "the superset feature is kept, not the un-chaptered body subset"
        );
    }

    /// The disc's own authoring-designated feature wins even when it is NOT the
    /// longest/largest title — a size-independent signal a decoy cannot forge.
    #[test]
    fn main_feature_order_honours_authoring_hint() {
        let hint = crate::labels::FeaturePlaylistHint {
            playlist_id: Some(222),
            filename: Some("00222.mpls".into()),
        };
        let authored = bd_title("00222.mpls", 222, 6000.0, 40_000_000_000, &["00222"]);
        let generic = bd_title("00001.mpls", 1, 8000.0, 80_000_000_000, &["00001"]);
        let capacity = 100_000_000_000u64;

        // No hint: the larger generic title wins on size.
        let mut no_hint = vec![authored.clone(), generic.clone()];
        Disc::sort_titles_by_main_feature(&mut no_hint, capacity, None, None);
        assert_eq!(
            no_hint[0].playlist_id, 1,
            "without a hint the larger generic title wins"
        );

        // With the hint: the authored feature wins outright despite being smaller.
        let mut titles = vec![generic, authored];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, Some(&hint), None);
        assert_eq!(
            titles[0].playlist_id, 222,
            "the authoring hint promotes the designated feature above the larger title"
        );
    }

    // The disc's own HDMV navigation pick (nav_feature, resolved by bdnav
    // playing First-Play) wins outright — above the authoring hint and
    // physical size order — when the nav-resolved title has video.
    #[test]
    fn nav_feature_promotes_navigated_title_over_larger() {
        let navigated = bd_title("00222.mpls", 222, 6000.0, 40_000_000_000, &["00222"]);
        let generic = bd_title("00001.mpls", 1, 8000.0, 80_000_000_000, &["00001"]);
        let capacity = 100_000_000_000u64;

        // Nav resolves 222 → it wins despite the generic title being larger/longer.
        let mut titles = vec![generic.clone(), navigated.clone()];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, Some(222));
        assert_eq!(
            titles[0].playlist_id, 222,
            "the disc's own navigation pick wins outright"
        );

        // Sanity: without a nav result, the larger generic title wins.
        let mut titles = vec![generic, navigated];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(titles[0].playlist_id, 1);
    }

    /// A nav result pointing at a STREAMLESS title is ignored (the `nav-feature`
    /// key is video-gated exactly like the authoring hint), so a mis-resolve can
    /// never select a decoy; selection falls through to the normal order.
    #[test]
    fn nav_feature_ignored_when_target_is_streamless() {
        // The SM3-shaped decoy: streamless wrapper composite of the feature's clip.
        let streamless = bd_title_no_stn_video(
            "00245.mpls",
            245,
            8350.0,
            78_000_000_000,
            &["00339", "00001", "00336"],
        );
        // The wrapped feature carries the real chapter table (issue #45), so the
        // streamless wrapper 00245 is detected as a composite and demoted.
        let feature = bd_title_chaptered("00001.mpls", 1, 8000.0, 76_000_000_000, &["00001"], 12);
        let capacity = 88_000_000_000u64;

        let mut titles = vec![streamless, feature];
        // Nav (wrongly) points at the streamless 245 — must be ignored.
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, Some(245));
        assert_eq!(
            titles[0].playlist_id, 1,
            "a streamless nav target is ignored (video-gated), so the real feature wins"
        );
    }

    // Regression: the disc navigates to a Dolby-Vision seamless-branch
    // playlist (tiny payload, real video) beside a 91.5 GB feature. Nav pick
    // must ALSO clear the payload floor, falling through to size ranking.
    #[test]
    fn nav_feature_ignored_when_target_is_a_low_payload_branch() {
        let branch = bd_title("00001.mpls", 1, 7500.0, 400_000_000, &["b01", "b02", "b03"]);
        let feature = bd_title("00002.mpls", 2, 7680.0, 91_500_000_000, &["00002"]);
        let capacity = 100_000_000_000u64;

        assert!(
            branch.has_probable_video(),
            "the DV branch has video streams (only its payload is tiny)"
        );
        let mut titles = vec![branch, feature];
        // Nav resolves the branch (1) like a real player — must be payload-gated.
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, Some(1));
        assert_eq!(
            titles[0].playlist_id, 2,
            "the 91.5 GB feature wins; the 0.4 GB nav-picked branch is not a rip target"
        );
    }

    /// A hint pointing at a STREAMLESS entry must not resurrect the decoy bug:
    /// the authoring preference requires the designated title to have real video
    /// streams, and a clip-less streamless title fails the `has-video` floor too.
    #[test]
    fn authoring_hint_never_promotes_a_streamless_title() {
        let hint = crate::labels::FeaturePlaylistHint {
            playlist_id: Some(245),
            filename: None,
        };
        let mut streamless = DiscTitle::empty();
        streamless.playlist = "00245.mpls".into();
        streamless.playlist_id = 245;
        streamless.duration_secs = 9000.0;
        streamless.size_bytes = 90_000_000_000;
        let feature = bd_title("00001.mpls", 1, 8000.0, 76_000_000_000, &["00001"]);
        let capacity = 100_000_000_000u64;

        let mut titles = vec![streamless, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, Some(&hint), None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "a hint at a streamless title must not override the has-video floor"
        );
    }

    /// Audit T4: an authoring hint pointing at a SHORT video branch/trailer must
    /// not override the real feature — the hint is corroborated by duration.
    #[test]
    fn authoring_hint_not_honoured_for_short_branch() {
        let hint = crate::labels::FeaturePlaylistHint {
            playlist_id: Some(250),
            filename: None,
        };
        let feature = bd_title("00001.mpls", 1, 8038.4, 76_000_000_000, &["00001"]);
        let branch = bd_title("00250.mpls", 250, 561.0, 5_000_000_000, &["00250"]);
        let capacity = 100_000_000_000u64;

        let mut titles = vec![branch, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, Some(&hint), None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "a hint at a 9-minute branch must not beat the 2h13m feature"
        );
    }

    /// Audit T6: a REAL feature whose first PlayItem is a non-video bumper
    /// (so its first-PlayItem STN reports v=0) must NOT be disqualified — the
    /// permissive `has-video` floor keeps it, and the smaller bonus does not win.
    #[test]
    fn feature_with_non_video_first_playitem_not_disqualified() {
        let feature =
            bd_title_no_stn_video("00001.mpls", 1, 8038.4, 76_000_000_000, &["00339", "00001"]);
        let bonus = bd_title("00050.mpls", 50, 600.0, 5_000_000_000, &["00050"]);
        let capacity = 88_000_000_000u64;

        assert!(
            !feature.has_video() && feature.has_probable_video(),
            "the bumper-lead feature has no first-PlayItem video but IS probable video"
        );
        let mut titles = vec![bonus, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "the bumper-lead feature is selected, not the small bonus"
        );
    }

    // Audit T7: when every title is streamless, the pick is still
    // deterministic (largest) and the sort/image_crack_extents/diag pick
    // all name the same title (audit R9 single-source-of-truth).
    #[test]
    fn all_streamless_fallback_is_deterministic_and_consistent() {
        let mut a = DiscTitle::empty();
        a.playlist = "00010.mpls".into();
        a.playlist_id = 10;
        a.duration_secs = 600.0;
        a.size_bytes = 500_000_000;
        a.extents = vec![Extent {
            start_lba: 10,
            sector_count: 100,
        }];
        let mut b = DiscTitle::empty();
        b.playlist = "00020.mpls".into();
        b.playlist_id = 20;
        b.duration_secs = 400.0;
        b.size_bytes = 300_000_000;
        b.extents = vec![Extent {
            start_lba: 200,
            sector_count: 50,
        }];
        let capacity = 50_000_000_000u64;

        let mut titles = vec![b, a];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 10,
            "the largest title is the deterministic fallback pick"
        );
        // image_crack_extents must agree with titles[0] (R9).
        assert_eq!(
            Disc::image_crack_extents(&titles),
            titles[0].extents.as_slice(),
            "image-crack extents must be the selected title's (single source of truth)"
        );
    }

    /// Equal-clip TWINS (seamless-branching siblings) are NOT composites — a
    /// composite requires a PROPER subset — so both stay eligible and the
    /// physical tiebreak (here size) decides.
    #[test]
    fn equal_clip_twins_are_not_composite() {
        let t0 = bd_title("00800.mpls", 800, 7200.0, 57_000_000_000, &["00001"]);
        let t1 = bd_title("00801.mpls", 801, 7200.0, 56_000_000_000, &["00001"]);
        let ranks = Disc::rank_titles(&[t0, t1], None, None);
        assert!(
            !ranks[0].composite && !ranks[1].composite,
            "identical-clip twins must not be flagged composite"
        );
    }

    /// A video-only title (no audio tracks) is a valid feature — the gate keys
    /// on video, not audio — so it is not disqualified.
    #[test]
    fn video_only_title_is_probable_video() {
        let t = bd_title("00001.mpls", 1, 7200.0, 60_000_000_000, &["00001"]);
        assert!(t.video_streams().next().is_some());
        assert!(t.streams.iter().all(|s| !matches!(s, Stream::Audio(_))));
        assert!(t.has_probable_video());
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
    fn image_read_sectors_rejects_zero_capacity() {
        // A non-zero capacity passes through unchanged — the imaging read
        // domain is the disc's sector count.
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
        assert_eq!(disc.image_read_sectors().expect("nonzero"), 12_219_392);

        // capacity_sectors == 0 means READ CAPACITY failed and was swallowed to
        // 0 during the scan. Imaging must hard-fail here, not size a 0-byte read
        // domain and write an empty ISO that reports success.
        let disc_zero = Disc {
            capacity_sectors: 0,
            capacity_bytes: 0,
            ..disc
        };
        assert!(
            matches!(disc_zero.image_read_sectors(), Err(Error::EmptyImage)),
            "zero capacity must be EmptyImage, got {:?}",
            disc_zero.image_read_sectors()
        );
    }

    #[test]
    fn read_capacity_retrying_rides_out_a_transient_failure() {
        use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};

        // Fails the first `fail_first` attempts with an empty data phase (which
        // decodes to Error::DiscCapacityMalformed), then answers a healthy READ
        // CAPACITY. Models the sporadic field failure the retry loop exists for.
        struct FlakyCapacity {
            calls: u32,
            fail_first: u32,
            last_lba: u32,
        }
        impl ScsiTransport for FlakyCapacity {
            fn execute(
                &mut self,
                cdb: &[u8],
                _dir: DataDirection,
                buf: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                // Only model READ CAPACITY; any other opcode (e.g. the ALLOW
                // MEDIUM REMOVAL that Drive::drop issues) trivially succeeds.
                if cdb[0] != crate::scsi::SCSI_READ_CAPACITY {
                    return Ok(ScsiResult {
                        status: 0,
                        sense: [0u8; 32],
                        bytes_transferred: 0,
                    });
                }
                self.calls += 1;
                if self.calls <= self.fail_first {
                    // Empty data phase → Error::DiscCapacityMalformed.
                    return Ok(ScsiResult {
                        status: 0,
                        sense: [0u8; 32],
                        bytes_transferred: 0,
                    });
                }
                buf[0..4].copy_from_slice(&self.last_lba.to_be_bytes());
                Ok(ScsiResult {
                    status: 0,
                    sense: [0u8; 32],
                    bytes_transferred: 8,
                })
            }
        }

        // Fails on the first two attempts, succeeds on the third: the retry
        // yields the hardware value, not the swallowed 0. (last_lba + 1.)
        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(FlakyCapacity {
            calls: 0,
            fail_first: 2,
            last_lba: 9_997_279,
        }));
        assert_eq!(
            Disc::read_capacity_retrying(&mut drive).expect("capacity"),
            9_997_280
        );
    }

    #[test]
    fn read_capacity_retrying_gives_up_as_zero_and_that_blocks_imaging() {
        use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};

        // Every attempt answers GOOD with an empty data phase (malformed): the
        // retry exhausts and returns 0 rather than a bogus value.
        struct AlwaysEmpty;
        impl ScsiTransport for AlwaysEmpty {
            fn execute(
                &mut self,
                _cdb: &[u8],
                _dir: DataDirection,
                _buf: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                Ok(ScsiResult {
                    status: 0,
                    sense: [0u8; 32],
                    bytes_transferred: 0,
                })
            }
        }

        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(AlwaysEmpty));
        let capacity = Disc::read_capacity_retrying(&mut drive).expect("not halted");
        assert_eq!(
            capacity, 0,
            "exhausted retries must yield 0, not a bad value"
        );

        // A capacity of 0 leaves the shared scan/identify/MKV path lenient but
        // hard-fails image output: image_read_sectors -> EmptyImage.
        let disc = Disc {
            volume_id: String::new(),
            meta_title: None,
            format: DiscFormat::BluRay,
            capacity_sectors: capacity,
            capacity_bytes: capacity as u64 * 2048,
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
        assert!(
            matches!(disc.image_read_sectors(), Err(Error::EmptyImage)),
            "capacity 0 must block imaging with EmptyImage, got {:?}",
            disc.image_read_sectors()
        );
    }

    // READ CAPACITY transport that counts attempts and answers every one with `err`.
    struct CountingCapacityFail {
        calls: std::sync::Arc<std::sync::atomic::AtomicU32>,
        err: fn() -> Error,
    }
    impl crate::scsi::ScsiTransport for CountingCapacityFail {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: crate::scsi::DataDirection,
            _buf: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<crate::scsi::ScsiResult> {
            if cdb[0] == crate::scsi::SCSI_READ_CAPACITY {
                self.calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err((self.err)());
            }
            Ok(crate::scsi::ScsiResult {
                status: 0,
                sense: [0u8; 32],
                bytes_transferred: 0,
            })
        }
    }

    fn capacity_fail_drive(
        err: fn() -> Error,
    ) -> (Drive, std::sync::Arc<std::sync::atomic::AtomicU32>) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let drive = Drive::from_transport_for_test(Box::new(CountingCapacityFail {
            calls: calls.clone(),
            err,
        }));
        (drive, calls)
    }

    // A Stop requested before the scan must not be ridden out as a flaky READ CAPACITY.
    #[test]
    fn read_udf_honours_halt_before_read_capacity() {
        let (mut drive, calls) = capacity_fail_drive(|| Error::ScsiError {
            opcode: crate::scsi::SCSI_READ_CAPACITY,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
        drive.halt();
        let t0 = std::time::Instant::now();
        let res = Disc::read_udf(&mut drive).map(|_| ());
        assert!(matches!(res, Err(Error::Halted)), "got {res:?}");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a halted drive must not issue READ CAPACITY"
        );
        assert!(t0.elapsed() < std::time::Duration::from_secs(1));
    }

    // ILLEGAL REQUEST is deterministic: retrying it only burns the backoff budget.
    #[test]
    fn read_capacity_does_not_retry_illegal_request() {
        let (mut drive, calls) = capacity_fail_drive(|| Error::ScsiError {
            opcode: crate::scsi::SCSI_READ_CAPACITY,
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_ILLEGAL_REQUEST,
                asc: 0x20,
                ascq: 0,
            }),
        });
        let _ = Disc::read_udf(&mut drive).map(|_| ());
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    // NOT READY / MEDIUM NOT PRESENT (asc 0x3A) will not clear within the retry budget.
    #[test]
    fn read_capacity_does_not_retry_medium_not_present() {
        let (mut drive, calls) = capacity_fail_drive(|| Error::ScsiError {
            opcode: crate::scsi::SCSI_READ_CAPACITY,
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_NOT_READY,
                asc: 0x3A,
                ascq: 0,
            }),
        });
        let _ = Disc::read_udf(&mut drive).map(|_| ());
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    // A Stop that lands during the backoff sleep ends the retry loop as Halted.
    #[test]
    fn read_capacity_stop_during_backoff_is_halted() {
        let (mut drive, calls) = capacity_fail_drive(|| Error::ScsiError {
            opcode: crate::scsi::SCSI_READ_CAPACITY,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
        let halt = drive.halt_flag();
        let stopper = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            halt.store(true, std::sync::atomic::Ordering::Relaxed);
        });
        let t0 = std::time::Instant::now();
        let res = Disc::read_udf(&mut drive).map(|_| ());
        let _ = stopper.join();
        assert!(matches!(res, Err(Error::Halted)), "got {res:?}");
        // The Stop can land before or after the second attempt on a loaded runner.
        assert!(calls.load(std::sync::atomic::Ordering::Relaxed) <= 2);
        assert!(t0.elapsed() < std::time::Duration::from_secs(2));
    }

    // Deterministic: a Stop raised as the backoff sleep begins ends the loop before the
    // next attempt.
    #[test]
    fn read_capacity_stop_before_backoff_prevents_next_attempt() {
        let (mut drive, calls) = capacity_fail_drive(|| Error::ScsiError {
            opcode: crate::scsi::SCSI_READ_CAPACITY,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
        let res = Disc::read_capacity_retrying_with(&mut drive, |d, t| {
            d.halt();
            d.pause(t)
        });
        assert!(matches!(res, Err(Error::Halted)), "got {res:?}");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    // The key files do not need the disc size: no READ CAPACITY (and no retry budget).
    #[test]
    fn read_aacs_inputs_from_drive_issues_no_read_capacity() {
        let (mut drive, calls) = capacity_fail_drive(|| Error::ScsiError {
            opcode: crate::scsi::SCSI_READ_CAPACITY,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
        let _ = Disc::read_aacs_inputs_from_drive(&mut drive);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    // In-memory BDMV tree for read_structure_files: `bdmv` files at /BDMV, plus
    // PLAYLIST / CLIPINF / BDJO subdirectories.
    fn structure_image(
        bdmv: Vec<udf::fixture::FileSpec>,
        playlist: Vec<udf::fixture::FileSpec>,
        clipinf: Vec<udf::fixture::FileSpec>,
        bdjo: Vec<udf::fixture::FileSpec>,
    ) -> udf::fixture::MemDisc {
        use udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, lay_dir};
        let sub = |name: &str, icb: u32, files| DirSpec {
            name: name.into(),
            icb_lba: icb,
            dir_data_lba: icb + 1,
            files,
            subdirs: vec![],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: vec![],
            subdirs: vec![DirSpec {
                name: "BDMV".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: bdmv,
                subdirs: vec![
                    sub("PLAYLIST", 30, playlist),
                    sub("CLIPINF", 40, clipinf),
                    sub("BDJO", 50, bdjo),
                ],
            }],
        };
        let mut disc = MemDisc::new();
        lay_dir(&mut disc, &root);
        build_udf_skeleton(&mut disc, 10);
        disc
    }

    // Counts inner read commands, to prove per-file reads are coalesced.
    struct CountingSource<'a> {
        inner: &'a mut udf::fixture::MemDisc,
        calls: usize,
    }
    impl SectorSource for CountingSource<'_> {
        fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            self.calls += 1;
            self.inner.read_sectors(lba, count, buf, r)
        }
    }

    // A 64-sector CLPI must not cost 64 single-sector commands.
    #[test]
    fn read_structure_files_batches_file_reads() {
        use udf::fixture::file_with;
        let mut disc = structure_image(
            vec![],
            vec![],
            vec![file_with(
                "00000.clpi",
                100,
                1000,
                vec![9; 64 * 2048],
                false,
            )],
            vec![],
        );
        let mut baseline = CountingSource {
            inner: &mut disc,
            calls: 0,
        };
        udf::read_filesystem(&mut baseline).expect("fs");
        let fs_calls = baseline.calls;
        let mut src = CountingSource {
            inner: &mut disc,
            calls: 0,
        };
        let files = Disc::read_structure_files(&mut src).expect("structure");
        assert_eq!(files.len(), 1);
        let file_calls = src.calls.saturating_sub(fs_calls);
        assert!(
            file_calls < 16,
            "{file_calls} read commands for a 64-sector file"
        );
    }

    // Read-ahead must stay inside the file: sectors right after a small structure file
    // are essence (possibly scrambled) and must never be requested.
    #[test]
    fn read_structure_files_read_ahead_stays_inside_the_file() {
        use udf::fixture::{PART_START, file_with};
        struct Fenced<'a> {
            inner: &'a mut udf::fixture::MemDisc,
            touched_fence: usize,
        }
        impl SectorSource for Fenced<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                r: bool,
            ) -> Result<usize> {
                let fence = PART_START + 1003..PART_START + 1100;
                if lba < fence.end && lba + count as u32 > fence.start {
                    self.touched_fence += 1;
                    return Err(Error::DiscRead {
                        sector: lba as u64,
                        status: None,
                        sense: None,
                    });
                }
                self.inner.read_sectors(lba, count, buf, r)
            }
        }
        let mut disc = structure_image(
            vec![],
            vec![],
            vec![file_with("00000.clpi", 100, 1000, vec![5; 3 * 2048], false)],
            vec![],
        );
        let mut src = Fenced {
            inner: &mut disc,
            touched_fence: 0,
        };
        let files = Disc::read_structure_files(&mut src).expect("structure");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].1.len(), 3 * 2048);
        assert_eq!(
            src.touched_fence, 0,
            "read-ahead spilled past the file extent"
        );
    }

    // The read-ahead forwards the caller's recovery flag instead of forcing it on.
    #[test]
    fn file_read_ahead_forwards_recovery_flag() {
        use udf::fixture::{PART_START, file_with};
        struct Flags<'a> {
            inner: &'a mut udf::fixture::MemDisc,
            forced: bool,
        }
        impl SectorSource for Flags<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                r: bool,
            ) -> Result<usize> {
                // Only file data; the ICB lookup is udf's own read.
                self.forced |= r && lba >= udf::fixture::PART_START + 1000;
                self.inner.read_sectors(lba, count, buf, r)
            }
        }
        let mut disc = structure_image(
            vec![],
            vec![],
            vec![file_with("00000.clpi", 100, 1000, vec![5; 3 * 2048], false)],
            vec![],
        );
        let fs = udf::read_filesystem(&mut disc).expect("fs");
        let icb = fs.find_dir("/BDMV/CLIPINF").expect("dir").entries[0].meta_lba;
        let mut src = Flags {
            inner: &mut disc,
            forced: false,
        };
        let mut ra = FileReadAhead::new(&mut src, &fs, icb).expect("extents");
        let mut buf = [0u8; 2048];
        ra.read_sectors(PART_START + 1000, 1, &mut buf, false)
            .expect("read");
        drop(ra);
        assert!(!src.forced, "recovery was forced on");
        assert_eq!(buf[0], 5);
    }

    // An ICB-embedded (AD type 3) structure file is bundled like any other.
    #[test]
    fn read_structure_files_keeps_embedded_data_files() {
        use udf::fixture::{PART_START, file_with};
        let payload = b"INDX0200-embedded".to_vec();
        let mut disc = structure_image(
            vec![file_with("index.bdmv", 100, 1000, payload.clone(), false)],
            vec![],
            vec![],
            vec![],
        );
        // Rewrite the file's ICB as an Extended File Entry with embedded data.
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        icb[34..36].copy_from_slice(&3u16.to_le_bytes());
        icb[56..64].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        icb[212..216].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        icb[216..216 + payload.len()].copy_from_slice(&payload);
        disc.put_bytes(PART_START + 100, &icb);
        let files = Disc::read_structure_files(&mut disc).expect("structure");
        assert_eq!(files, vec![("BDMV/index.bdmv".to_string(), payload)]);
    }

    // Once a batch inside a file fails, the rest of that file is read sector by
    // sector: no repeated multi-sector reads over the bad area.
    #[test]
    fn read_structure_files_stops_batching_after_a_failed_batch() {
        use udf::fixture::{PART_START, file_with};
        struct BadSector<'a> {
            inner: &'a mut udf::fixture::MemDisc,
            failed_batches: usize,
        }
        impl SectorSource for BadSector<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                r: bool,
            ) -> Result<usize> {
                let bad = PART_START + 1008;
                if lba <= bad && lba + count as u32 > bad {
                    if count > 1 {
                        self.failed_batches += 1;
                    }
                    return Err(Error::DiscRead {
                        sector: bad as u64,
                        status: None,
                        sense: None,
                    });
                }
                self.inner.read_sectors(lba, count, buf, r)
            }
        }
        let mut disc = structure_image(
            vec![],
            vec![],
            vec![file_with(
                "00000.clpi",
                100,
                1000,
                vec![5; 16 * 2048],
                false,
            )],
            vec![],
        );
        let mut src = BadSector {
            inner: &mut disc,
            failed_batches: 0,
        };
        let files = Disc::read_structure_files(&mut src).expect("structure");
        assert!(files.is_empty(), "the unreadable file is skipped");
        assert_eq!(
            src.failed_batches, 1,
            "batching must stop after the first failure"
        );
    }

    // Each file's ICB is read from the source once, not once per UDF lookup.
    #[test]
    fn read_structure_files_reads_each_icb_once() {
        use udf::fixture::{PART_START, file_with};
        struct IcbCount<'a> {
            inner: &'a mut udf::fixture::MemDisc,
            icb_reads: usize,
        }
        impl SectorSource for IcbCount<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                r: bool,
            ) -> Result<usize> {
                let icb = PART_START + 100;
                if lba <= icb && lba + count as u32 > icb {
                    self.icb_reads += 1;
                }
                self.inner.read_sectors(lba, count, buf, r)
            }
        }
        let mut disc = structure_image(
            vec![],
            vec![],
            vec![file_with("00000.clpi", 100, 1000, vec![5; 2048], false)],
            vec![],
        );
        let mut base = IcbCount {
            inner: &mut disc,
            icb_reads: 0,
        };
        udf::read_filesystem(&mut base).expect("fs");
        let parse_reads = base.icb_reads;
        let mut src = IcbCount {
            inner: &mut disc,
            icb_reads: 0,
        };
        Disc::read_structure_files(&mut src).expect("structure");
        assert_eq!(src.icb_reads - parse_reads, 1);
    }

    fn structure_names(disc: &mut udf::fixture::MemDisc) -> Vec<String> {
        Disc::read_structure_files(disc)
            .expect("structure")
            .into_iter()
            .map(|(p, _)| p)
            .collect()
    }

    // Disc FID names are untrusted; the caller joins them under a profile dir.
    #[test]
    fn read_structure_files_drops_unsafe_names() {
        use udf::fixture::file_with;
        let mut disc = structure_image(
            vec![],
            vec![
                file_with("00000.mpls", 100, 1000, vec![1; 10], false),
                file_with("..\\..\\evil.mpls", 101, 1001, vec![2; 10], false),
                file_with("a/b.mpls", 102, 1002, vec![3; 10], false),
                file_with("CON.mpls", 103, 1003, vec![4; 10], false),
                file_with("a?b.mpls", 104, 1004, vec![5; 10], false),
            ],
            vec![],
            vec![],
        );
        assert_eq!(structure_names(&mut disc), vec!["BDMV/PLAYLIST/00000.mpls"]);
    }

    #[test]
    fn plain_file_name_rejects_windows_invalid_names() {
        for bad in [
            "",
            ".",
            "..",
            "a/b",
            "a\\b",
            "a:b",
            "a<b",
            "a>b",
            "a\"b",
            "a|b",
            "a?b",
            "a*b",
            "a\u{1}b",
            "CON",
            "con.mpls",
            "PRN.x",
            "AUX",
            "NUL.clpi",
            "COM1.mpls",
            "lpt9.xml",
            "x.mpls.",
            "x.mpls ",
        ] {
            assert!(!is_plain_file_name(bad), "{bad:?} must be rejected");
        }
        for bad in [
            "CON .mpls",
            "COM\u{B9}.mpls",
            "lpt\u{B3}.x",
            "CONIN$.xml",
            "conout$",
            "COM0.mpls",
            "LPT0",
        ] {
            assert!(!is_plain_file_name(bad), "{bad:?} must be rejected");
        }
        for good in [
            "COMX.mpls",
            "00000.mpls",
            "index.bdmv",
            "CONSOLE.xml",
            "LPT10.x",
        ] {
            assert!(is_plain_file_name(good), "{good:?} must be accepted");
        }
    }

    // Hitting the byte budget skips that file only; later, smaller files survive.
    #[test]
    fn read_structure_files_byte_cap_skips_only_the_oversized_file() {
        use udf::fixture::{file, file_with};
        const MIB: u64 = 1024 * 1024;
        let mut disc = structure_image(
            vec![],
            vec![],
            vec![
                file("00000.clpi", 100, 10_000, 40 * MIB, false),
                file("00001.clpi", 101, 10_000, 40 * MIB, false),
            ],
            vec![file_with("00000.bdjo", 102, 1000, vec![7; 10], false)],
        );
        let files = Disc::read_structure_files(&mut disc).expect("structure");
        let total: usize = files.iter().map(|(_, b)| b.len()).sum();
        assert!(total <= 64 * MIB as usize, "bundle not capped: {total}");
        let names: Vec<&str> = files.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(
            names,
            vec!["BDMV/CLIPINF/00000.clpi", "BDMV/BDJO/00000.bdjo"]
        );
    }

    // The bundled bytes come from the variant that is actually read (first in directory
    // order), with that variant's own size — not a sibling's.
    #[test]
    fn read_structure_files_case_variants_use_the_read_variants_size() {
        use udf::fixture::file_with;
        let mut disc = structure_image(
            vec![
                file_with("index.bdmv", 100, 1000, vec![1; 20], false),
                file_with("INDEX.BDMV", 101, 1001, vec![2; 10], false),
            ],
            vec![
                file_with("00000.mpls", 102, 1002, vec![3; 20], false),
                file_with("00000.MPLS", 103, 1003, vec![4; 10], false),
            ],
            vec![],
            vec![],
        );
        let files = Disc::read_structure_files(&mut disc).expect("structure");
        let got: Vec<(&str, usize)> = files.iter().map(|(p, b)| (p.as_str(), b.len())).collect();
        assert_eq!(
            got,
            vec![("BDMV/index.bdmv", 20), ("BDMV/PLAYLIST/00000.mpls", 20)]
        );
    }

    // Nav files use canonical names whatever the disc casing, and case variants
    // (which collide on case-insensitive hosts) are bundled once.
    #[test]
    fn read_structure_files_canonical_nav_names_and_case_dedupe() {
        use udf::fixture::file_with;
        let mut disc = structure_image(
            vec![
                file_with("INDEX.BDMV", 100, 1000, vec![1; 10], false),
                file_with("movieobject.bdmv", 101, 1001, vec![2; 10], false),
            ],
            vec![
                file_with("00000.MPLS", 102, 1002, vec![3; 10], false),
                file_with("00000.mpls", 103, 1003, vec![4; 10], false),
            ],
            vec![],
            vec![],
        );
        assert_eq!(
            structure_names(&mut disc),
            vec![
                "BDMV/index.bdmv",
                "BDMV/MovieObject.bdmv",
                "BDMV/PLAYLIST/00000.MPLS"
            ]
        );
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

    /// An AACS state with every capture field inert (no keys live on `AacsState`, KU §2.2).
    fn aacs_empty() -> AacsState {
        AacsState {
            version: 2,
            bus_encryption: true,
            mkb_version: None,
            disc_hash: String::new(),
            volume_id: [0u8; 16],
            uk_ro: Vec::new(),
            mkb: Vec::new(),
        }
    }

    // The disc-wide decrypt gate with no key set (`keys::disc_gate`). Only "decryption
    // needed AND unavailable AND not --raw" may error.

    fn gate(disc: &Disc, raw: bool) -> Result<()> {
        crate::keys::disc_gate(disc, raw)
    }

    /// AACS disc with no key set → NoDiscKey (a pass-through reader would otherwise
    /// write ciphertext at exit 0). An AACS disc's own keys are always `None`.
    #[test]
    fn disc_gate_aacs_no_key_errors() {
        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_empty());
        assert!(matches!(
            disc.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));
        let err = gate(&disc, false).expect_err("AACS disc, no key, !raw must error");
        assert_eq!(
            err.code(),
            crate::error::Error::NoDiscKey {
                disc_hash: String::new()
            }
            .code()
        );
        assert!(gate(&disc, true).is_ok(), "--raw must proceed");
    }

    // E7017 vs E7022: with derivation material but no VID the scan records
    // AacsVidUnavailable, and the gate surfaces E7017, not the generic E7022.
    #[test]
    fn disc_gate_aacs_vid_unavailable_vs_no_key() {
        let supplied = crate::aacs::provider::SuppliedKey {
            device_keys: vec![crate::aacs::types::DeviceKey {
                key: [0x11; 16],
                node: 1,
                uv: 1,
                u_mask_shift: 0,
            }],
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: None,
        };
        let provider_refs: [&dyn crate::aacs::provider::KeyProvider; 1] = [&supplied];
        // A parseable Unit_Key_RO.inf (uk_pos=32, zero unit keys), so resolution
        // fails for lack of a VID, not because the .inf failed to parse.
        let mut uk_ro = vec![0u8; 40];
        uk_ro[0..4].copy_from_slice(&32u32.to_be_bytes());
        let ctx = crate::aacs::resolve::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers: &provider_refs,
            mkb: None,
        };
        assert_eq!(
            crate::aacs::resolve::resolve_keys_with_reason(&ctx, 2).err(),
            Some(crate::aacs::resolve::ResolveFailure::VidUnavailable),
            "device keys + zero VID must classify as VidUnavailable"
        );

        let mut disc = make_test_disc(1000, "UHD");
        disc.encrypted = true;
        disc.aacs = Some(aacs_empty());
        disc.aacs_error = Some(crate::error::Error::AacsVidUnavailable);
        assert_eq!(
            gate(&disc, false).expect_err("must error").code(),
            crate::error::Error::AacsVidUnavailable.code(),
            "material-but-no-VID must surface E7017, not E7022"
        );
        disc.aacs_error = None;
        assert_eq!(
            gate(&disc, false).expect_err("must error").code(),
            crate::error::Error::NoDiscKey {
                disc_hash: String::new()
            }
            .code(),
            "no reason captured keeps E7022"
        );
    }

    // A key SOURCE that could not answer must not be reported as "this disc has no key"
    // (E7022 for both once sent an operator hunting for a VUK through hours of 502s).
    #[test]
    fn key_source_failure_is_not_reported_as_a_missing_disc_key() {
        let miss_code = crate::error::Error::NoDiscKey {
            disc_hash: String::new(),
        }
        .code();
        let failures = [
            (
                crate::error::Error::KeyServiceUnavailable,
                crate::error::E_KEY_SERVICE_UNAVAILABLE,
            ),
            (
                crate::error::Error::KeyServiceUnauthorized,
                crate::error::E_KEY_SERVICE_UNAUTHORIZED,
            ),
            (
                crate::error::Error::KeyServiceRateLimited,
                crate::error::E_KEY_SERVICE_RATE_LIMITED,
            ),
        ];
        for (reason, want) in failures {
            let mut down = make_test_disc(1000, "UHD");
            down.encrypted = true;
            down.aacs = Some(aacs_empty());
            down.aacs_error = Some(reason);
            let code = gate(&down, false).expect_err("no key must error").code();
            assert_eq!(code, want, "the gate must surface the SOURCE's code");
            assert_ne!(code, miss_code);
        }
    }

    /// A genuinely unencrypted disc has `None` keys legitimately — the gate keys off
    /// the scan-captured disc state, never the keys, so it must not false-error.
    #[test]
    fn disc_gate_unencrypted_proceeds() {
        let disc = make_test_disc(1000, "BD");
        assert!(gate(&disc, false).is_ok());
    }

    // CSS scrambled-but-uncracked (css None, css_error Some): the DISC-LEVEL
    // CssNoDiscKey, never the per-title skippable CssKeyMissing.
    #[test]
    fn disc_gate_css_error_is_disc_level() {
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css_error = Some(crate::error::Error::CssKeyMissing);
        let err = gate(&disc, false).expect_err("scrambled-but-uncracked CSS must error");
        assert_eq!(err.code(), crate::error::Error::CssNoDiscKey.code());
        let wide: std::io::Error = err.into();
        assert!(crate::error::is_disc_level_no_key(&wide));
        assert!(!crate::error::is_skippable_title_stub(&wide));
        assert!(gate(&disc, true).is_ok(), "--raw is exempt");
    }

    /// CSS-keyless-crack SUCCESS: `css` holds a title key → proceed.
    #[test]
    fn disc_gate_css_with_key_proceeds() {
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css = Some(crate::css::CssState {
            title_key: [0u8; 5],
            crack_span: None,
        });
        assert!(gate(&disc, false).is_ok());
    }

    /// Build a crackable scrambled CSS sector (a periodic run in the
    /// clear header continuing past 0x80), mirroring the css-module fixture.
    fn crackable_css_sector(title_key: &[u8; 5]) -> [u8; 2048] {
        const RUN_START: usize = 0x59;
        const PERIOD: usize = 8;
        let mut sec = [0u8; 2048];
        sec[0x00..0x04].copy_from_slice(&crate::css::PACK_START);
        sec[4] = 0x44; // '01': a 13818-1 pack
        sec[0x14] = 0x10; // scramble flag
        for (i, b) in sec.iter_mut().enumerate().skip(RUN_START) {
            *b = (0xA0u8.wrapping_add((i % PERIOD) as u8)) ^ 0x5A;
        }
        crate::css::lfsr::scramble_sector(title_key, &mut sec);
        sec
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

    // (Former coding_type_a2_is_dts_hd_ma wrongly asserted 0xA2 is Master
    // Audio; see secondary_dts_hd_0xa2_is_lossy_not_master_audio instead.)
    // HDMV 0x91 (IG/menus) must NOT map to Pgs (0x90); falls to Unknown.
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

    // ── correct_truehd_channels ──────────────────────────────────────────
    // Records every read_sectors call and serves a fixed zero-padded
    // buffer, for probing early-return guards and round-trip tests below.
    struct ThdSpyReader {
        calls: std::cell::RefCell<Vec<(u32, u16)>>,
        data: Vec<u8>,
    }
    impl SectorSource for ThdSpyReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.calls.borrow_mut().push((lba, count));
            let n = self.data.len().min(buf.len());
            buf[..n].copy_from_slice(&self.data[..n]);
            for b in buf[n..].iter_mut() {
                *b = 0;
            }
            Ok(buf.len())
        }
    }

    /// One 192-byte BD-TS PES packet on `pid` carrying `es` as its raw
    /// elementary payload. Minimal PES header (no PTS/DTS) — this probe
    /// reads and demuxes+flushes in one shot, so no timestamp is needed.
    fn thd_bd_pes(pid: u16, es: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; 192];
        pkt[4] = 0x47; // TS sync
        pkt[5] = 0x40 | ((pid >> 8) & 0x1F) as u8; // PUSI + PID hi
        pkt[6] = (pid & 0xFF) as u8; // PID lo
        pkt[7] = 0x10; // adaptation = payload-only, cc = 0
        let p = 8;
        pkt[p] = 0x00;
        pkt[p + 1] = 0x00;
        pkt[p + 2] = 0x01;
        pkt[p + 3] = 0xBD; // private_stream_1
        pkt[p + 4] = 0x00;
        pkt[p + 5] = 0x00;
        pkt[p + 6] = 0x80; // flags1 marker bits
        pkt[p + 7] = 0x00; // flags2: no PTS/DTS
        pkt[p + 8] = 0x00; // PES_header_data_length = 0
        let es_off = p + 9;
        let n = es.len().min(192 - es_off);
        pkt[es_off..es_off + n].copy_from_slice(&es[..n]);
        pkt
    }

    /// A synthetic TrueHD major-sync access unit: 2 junk bytes, the
    /// 0xF8726FBA sync, `format_info`, then padding through the
    /// num_substreams byte (sync offset + 16) so Atmos detection can read it.
    fn thd_major_sync_es(format_info: u32, num_substreams: u8) -> Vec<u8> {
        let mut es = vec![0u8; 24];
        es[0] = 0xAA;
        es[1] = 0xBB;
        es[2..6].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
        es[6..10].copy_from_slice(&format_info.to_be_bytes());
        es[2 + 16] = num_substreams << 4;
        es
    }

    fn truehd_audio_stream(pid: u16, channels: AudioChannels, sample_rate: SampleRate) -> Stream {
        Stream::Audio(AudioStream {
            pid,
            codec: Codec::TrueHd,
            channels,
            language: "eng".into(),
            sample_rate,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: crate::labels::generate_audio_label(&Codec::TrueHd, &channels, false),
        })
    }

    // No TrueHd stream -> the pid list is empty and the probe must return
    // before touching the reader. Mutation guard: flipping the codec match
    // to `true` would sweep this stream's pid into the probe list too.
    #[test]
    fn correct_truehd_channels_skips_probe_when_no_truehd_stream() {
        let mut title = DiscTitle::empty();
        title.streams = vec![Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })];
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 10,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: Vec::new(),
        };
        correct_truehd_channels(&mut reader, &mut title);
        assert!(
            reader.calls.borrow().is_empty(),
            "no TrueHD stream present → the reader must never be touched: {:?}",
            reader.calls.borrow()
        );
    }

    // A TrueHd stream IS present -> the probe must read the title's first
    // extent. Mutation guard: flipping the codec match to `false` would
    // empty the pid list even here, so the probe never reads.
    #[test]
    fn correct_truehd_channels_reads_when_truehd_stream_present() {
        let mut title = DiscTitle::empty();
        title.streams = vec![truehd_audio_stream(
            0x1100,
            AudioChannels::Surround51,
            SampleRate::S48,
        )];
        title.extents = vec![Extent {
            start_lba: 7,
            sector_count: 10,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: Vec::new(),
        };
        correct_truehd_channels(&mut reader, &mut title);
        assert!(
            !reader.calls.borrow().is_empty(),
            "a TrueHD stream present must drive a probe read"
        );
    }

    // The bounded-probe sector count is ext.sector_count.min(4096); a ZERO
    // sector extent means nothing to read, so the probe must return before
    // calling into the reader. Mutation guard: `n == 0` flipped to `!= 0`.
    #[test]
    fn correct_truehd_channels_skips_read_on_zero_sector_extent() {
        let mut title = DiscTitle::empty();
        title.streams = vec![truehd_audio_stream(
            0x1100,
            AudioChannels::Surround51,
            SampleRate::S48,
        )];
        title.extents = vec![Extent {
            start_lba: 7,
            sector_count: 0,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: Vec::new(),
        };
        correct_truehd_channels(&mut reader, &mut title);
        assert!(
            reader.calls.borrow().is_empty(),
            "a zero-sector extent must never trigger a read: {:?}",
            reader.calls.borrow()
        );
    }

    // Full round-trip: a major sync carrying 7.1/96kHz/Atmos, probed through
    // a container-declared 5.1/48kHz basic-descriptor stream. All three
    // corrections must land and the label promote to Atmos. See docs.
    #[test]
    fn correct_truehd_channels_full_correction_and_atmos_promotion() {
        let pid = 0x1100u16;
        // format_info: top nibble 0x1 -> 96 kHz; low 13 bits 0x1F -> 7.1 (8ch).
        let format_info = (0x1u32 << 28) | 0x1F;
        let es = thd_major_sync_es(format_info, 4); // num_substreams=4 -> Atmos
        let ts = thd_bd_pes(pid, &es);
        let mut title = DiscTitle::empty();
        title.streams = vec![truehd_audio_stream(
            pid,
            AudioChannels::Surround51, // base 5.1 the MPLS descriptor understates
            SampleRate::S48,           // base 48 kHz the container guessed
        )];
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: ts,
        };
        correct_truehd_channels(&mut reader, &mut title);
        let Stream::Audio(a) = &title.streams[0] else {
            panic!("stream type must be preserved")
        };
        assert_eq!(
            a.channels,
            AudioChannels::Surround71,
            "the 8ch major-sync presentation must correct the understated 5.1"
        );
        assert_eq!(
            a.sample_rate,
            SampleRate::S96,
            "the whitelisted 0x1 rate nibble must correct the guessed 48 kHz"
        );
        assert_eq!(
            a.label,
            crate::labels::generate_audio_label_atmos(
                &Codec::TrueHd,
                &AudioChannels::Surround71,
                false
            ),
            "basic descriptor + detected Atmos substream must promote the label"
        );
    }

    // A major sync whose presentation masks map to Unknown (no real
    // channel-count meaning) must leave the container's channel count
    // untouched, not overwrite a known-good value with Unknown.
    #[test]
    fn correct_truehd_channels_leaves_channels_when_count_unmapped() {
        let pid = 0x1100u16;
        // All 13 8ch bits set -> 20 channels: no N.M variant, from_count(20)
        // -> Unknown. Rate nibble 0x0 -> 48 kHz (matches the container, so
        // this test isolates the channels guard from the rate guard).
        let format_info = 0x1FFF;
        let es = thd_major_sync_es(format_info, 0); // not Atmos
        let ts = thd_bd_pes(pid, &es);
        let mut title = DiscTitle::empty();
        title.streams = vec![truehd_audio_stream(
            pid,
            AudioChannels::Surround51,
            SampleRate::S48,
        )];
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: ts,
        };
        correct_truehd_channels(&mut reader, &mut title);
        let Stream::Audio(a) = &title.streams[0] else {
            panic!("stream type must be preserved")
        };
        assert_eq!(
            a.channels,
            AudioChannels::Surround51,
            "an unmapped (Unknown) major-sync channel count must not overwrite a known container value"
        );
    }

    // A rate nibble not in the six whitelisted rates must leave the
    // container's sample rate untouched, mirroring the channels guard
    // ("never write a wrong SamplingFrequency").
    #[test]
    fn correct_truehd_channels_leaves_sample_rate_when_nibble_unrecognized() {
        let pid = 0x1100u16;
        let format_info = 0x3u32 << 28; // unrecognised rate nibble; ch8/ch6 masks = 0
        let es = thd_major_sync_es(format_info, 0);
        let ts = thd_bd_pes(pid, &es);
        let mut title = DiscTitle::empty();
        title.streams = vec![truehd_audio_stream(
            pid,
            AudioChannels::Surround51,
            SampleRate::S48,
        )];
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: ts,
        };
        correct_truehd_channels(&mut reader, &mut title);
        let Stream::Audio(a) = &title.streams[0] else {
            panic!("stream type must be preserved")
        };
        assert_eq!(
            a.sample_rate,
            SampleRate::S48,
            "an unrecognised major-sync rate nibble must not overwrite the container's sample rate"
        );
    }

    fn truehd_corrected(format_info: u32) -> AudioStream {
        let pid = 0x1100u16;
        let ts = thd_bd_pes(pid, &thd_major_sync_es(format_info, 0));
        let mut title = DiscTitle::empty();
        title.streams = vec![truehd_audio_stream(
            pid,
            AudioChannels::Surround51,
            SampleRate::S48,
        )];
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1,
        }];
        let mut reader = ThdSpyReader {
            calls: std::cell::RefCell::new(Vec::new()),
            data: ts,
        };
        correct_truehd_channels(&mut reader, &mut title);
        match title.streams.remove(0) {
            Stream::Audio(a) => a,
            other => panic!("stream type must be preserved, got {other:?}"),
        }
    }

    // The TrueHD mask's LFE bit decides N.M; summing it into a count mislabelled
    // 3.0 as "2.1", 7.0 as "6.1", etc.
    #[test]
    fn correct_truehd_channels_keeps_lfe_split_from_the_mask() {
        for (mask, want) in [
            (0x03u32, "3.0"),
            (0x07, "3.1"),
            (0x0D, "4.1"),
            (0x8B, "6.0"),
            (0x4B, "7.0"),
            (0x0F, "5.1"),
            (0x4F, "7.1"),
            (0x1F, "7.1"), // 5.1.2: heights flatten to full-range
        ] {
            let a = truehd_corrected(mask);
            assert_eq!(a.channels.to_string(), want, "mask {mask:#x}");
            assert_eq!(a.label, format!("Dolby TrueHD {want}"), "mask {mask:#x}");
        }
    }

    // Known limitation: unnamed layouts (5.2, 8.0 no-LFE, LFE-only) fall back to
    // the count-based name; here 5.2 reads "6.1" but Channels stays 7, not 6.
    #[test]
    fn correct_truehd_channels_counts_unnameable_layout() {
        let a = truehd_corrected(0x100F);
        assert_eq!(a.channels.count(), 7);
        assert_eq!(a.channels.to_string(), "6.1");
    }

    // bytes_bad_in_title empty-input guard: the `||`-to-`&&` mutant here is
    // EQUIVALENT (pure short-circuit), so it's intentionally untested.

    // ── byte_offset_in_title ──────────────────────────────────────────────

    fn title_with_size(size_bytes: u64, extents: Vec<Extent>) -> DiscTitle {
        DiscTitle {
            size_bytes,
            extents,
            ..DiscTitle::empty()
        }
    }

    // A multi-extent title where the target LBA lands in the SECOND extent — exercises both the
    // first extent's boundary check and the running `cumulative` byte total added on the way
    // past it.
    #[test]
    fn byte_offset_in_title_accumulates_across_extents() {
        let title = title_with_size(
            0,
            vec![
                Extent {
                    start_lba: 100,
                    sector_count: 10,
                }, // LBAs 100..110, 20_480 bytes
                Extent {
                    start_lba: 200,
                    sector_count: 10,
                }, // LBAs 200..210
            ],
        );
        // lba 205 is 5 sectors into the SECOND extent.
        let got = byte_offset_in_title(205, &title);
        assert_eq!(
            got,
            Some(20_480 + 5 * 2048),
            "offset must be the first extent's full byte length plus the \
             position within the second extent, not a first-extent mismatch"
        );
    }

    /// An extent's end is EXCLUSIVE (`start_lba + sector_count`): the LBA one
    /// past the last sector of an extent belongs to no extent (or the next
    /// one), never this one. Kills `lba < ext_end` flipped to `<=`.
    #[test]
    fn byte_offset_in_title_extent_end_is_exclusive() {
        let title = title_with_size(
            0,
            vec![Extent {
                start_lba: 100,
                sector_count: 10, // covers LBAs 100..110
            }],
        );
        assert_eq!(
            byte_offset_in_title(110, &title),
            None,
            "LBA 110 is one past this extent's last sector (109) and must not resolve inside it"
        );
        assert_eq!(
            byte_offset_in_title(109, &title),
            Some(9 * 2048),
            "sanity: the extent's actual last sector still resolves"
        );
    }

    // ── chapter_at_offset ─────────────────────────────────────────────────

    fn three_chapters() -> Vec<Chapter> {
        vec![
            Chapter {
                time_secs: 0.0,
                name: "1".into(),
            },
            Chapter {
                time_secs: 50.0,
                name: "2".into(),
            },
            Chapter {
                time_secs: 100.0,
                name: "3".into(),
            },
        ]
    }

    // Concrete end-to-end arithmetic: byte_offset 60/100 of a 100s title
    // lands at t=60s, chapter index 1 (0-based) -> 1-based chapter 2. Kills
    // every arithmetic-operator and fixed-tuple mutant. See docs.
    #[test]
    fn chapter_at_offset_concrete_arithmetic() {
        let chapters = three_chapters();
        let got = chapter_at_offset(&chapters, 60, 100.0, 100);
        assert_eq!(
            got,
            Some((2, 60.0)),
            "byte 60/100 of a 100s title = t=60s = chapter 2 (1-based)"
        );
    }

    // total_bytes == 0 must short-circuit to None regardless of chapters.
    // Kills `total_bytes == 0` flipped to `!=`, and (with the next test)
    // the `||` flipped to `&&`.
    #[test]
    fn chapter_at_offset_zero_total_bytes_is_none() {
        let chapters = three_chapters();
        assert_eq!(
            chapter_at_offset(&chapters, 10, 100.0, 0),
            None,
            "a title with no declared size has no byte-fraction to place a chapter at"
        );
    }

    // No chapters declared -> None, even with a valid nonzero title size.
    // Kills the `||` flipped to `&&` (would fall through the guard and
    // return a bogus Some((1, ..)) from the then-empty scan loop).
    #[test]
    fn chapter_at_offset_no_chapters_is_none() {
        assert_eq!(
            chapter_at_offset(&[], 10, 100.0, 100),
            None,
            "a title with no chapters has nothing to report a chapter index against"
        );
    }

    // ── range_chapter ─────────────────────────────────────────────────────

    fn title_for_range_chapter() -> DiscTitle {
        DiscTitle {
            duration_secs: 100.0,
            size_bytes: 204_800, // 100 sectors * 2048
            chapters: three_chapters(),
            extents: vec![Extent {
                start_lba: 1_000,
                sector_count: 100,
            }],
            ..DiscTitle::empty()
        }
    }

    // Concrete positive case chaining byte_offset_in_title + chapter_at_offset:
    // lba 1060 -> t=60s -> chapter 2. This exact tuple kills every
    // fixed-tuple whole-function replacement mutant.
    #[test]
    fn range_chapter_concrete_positive_case() {
        let title = title_for_range_chapter();
        assert_eq!(range_chapter(1_060, &title), (Some(2), Some(60.0)));
    }

    /// An LBA outside every extent resolves to `(None, None)`.
    #[test]
    fn range_chapter_outside_extents_is_none() {
        let title = title_for_range_chapter();
        assert_eq!(range_chapter(5_000, &title), (None, None));
    }

    // ── locate_ranges ─────────────────────────────────────────────────────
    // Isolates the per-range lba/count sector-arithmetic from every
    // bps-dependent branch (duration_secs negative forces bps == 0.0).
    #[test]
    fn locate_ranges_lba_and_count_are_sector_quotients() {
        let title = title_with_size(0, vec![]);
        let mut title = title;
        title.duration_secs = -1.0;
        let result = locate_ranges(&[(5_000, 6_000)], &title);
        assert_eq!(result.ranges.len(), 1);
        assert_eq!(result.ranges[0].lba, 2, "5000 / 2048 = 2");
        assert_eq!(result.ranges[0].count, 2, "6000 / 2048 = 2");
    }

    // Concrete positive-bps arithmetic for duration_ms and main_at_risk_ms:
    // bps = 2048 B/s exactly, a 4096-byte range = 2000 ms, entirely inside
    // the title's only extent. Kills bps/size arithmetic mutants; see docs.
    #[test]
    fn locate_ranges_positive_bps_duration_and_at_risk() {
        let title = title_with_size(
            204_800,
            vec![Extent {
                start_lba: 0,
                sector_count: 100,
            }],
        );
        let mut title = title;
        title.duration_secs = 100.0;
        let result = locate_ranges(&[(0, 4096)], &title);
        assert_eq!(result.ranges.len(), 1);
        assert_eq!(
            result.ranges[0].duration_ms, 2000.0,
            "4096 B / 2048 B/s * 1000 = 2000 ms"
        );
        assert_eq!(result.largest_gap_ms, 2000.0);
        assert_eq!(
            result.main_at_risk_ms, 2000.0,
            "the range is entirely inside the title's extent"
        );
    }

    // bps computed exactly 0.0: duration_ms/main_at_risk_ms must stay 0.0,
    // never inf/NaN. Kills `bps > 0.0` flipped to `>=` at both sites (the
    // boundary exactly zero would wrongly take the division branch).
    #[test]
    fn locate_ranges_zero_bps_stays_zero_not_infinite() {
        let title = title_with_size(
            204_800,
            vec![Extent {
                start_lba: 0,
                sector_count: 100,
            }],
        );
        // duration_secs left at DiscTitle::empty()'s default 0.0.
        let result = locate_ranges(&[(0, 4096)], &title);
        assert_eq!(result.ranges[0].duration_ms, 0.0);
        assert_eq!(result.main_at_risk_ms, 0.0);
    }

    // NOTE: the `>`-to-`>=` mutant seeding bps is EQUIVALENT (re-checked).
    // ── Codec::name/Display: linear lookup keyed by `==` against
    // ALL_CODECS; a non-first entry catches a mutated `==`->`!=`.
    #[test]
    fn codec_name_lookup_and_unknown_fallback() {
        assert_eq!(Codec::Hevc.name(), "HEVC");
        assert_eq!(Codec::TrueHd.name(), "TrueHD");
        assert_eq!(
            Codec::Unknown(0xAB).name(),
            "Unknown",
            "a coding type outside the table falls back to the literal \"Unknown\""
        );
    }

    /// `Display` must forward to `name()`, not silently emit nothing.
    #[test]
    fn codec_display_forwards_to_name() {
        assert_eq!(format!("{}", Codec::TrueHd), "TrueHD");
    }

    // ── Resolution::is_sd / from_height ──────────────────────────────────

    #[test]
    fn resolution_is_sd_matches_sd_variants_only() {
        assert!(Resolution::R480i.is_sd());
        assert!(Resolution::R480p.is_sd());
        assert!(Resolution::R576i.is_sd());
        assert!(Resolution::R576p.is_sd());
        assert!(!Resolution::R720p.is_sd());
        assert!(!Resolution::R1080p.is_sd());
        assert!(!Resolution::Unknown.is_sd());
    }

    // Every from_height bucket boundary — deleting any match arm makes its
    // heights fall through to the NEXT surviving arm, so each pair (top of
    // one bucket, bottom of the next) pins the arm to its own boundary.
    #[test]
    fn resolution_from_height_bucket_boundaries() {
        assert_eq!(Resolution::from_height(0), Resolution::R480p);
        assert_eq!(Resolution::from_height(480), Resolution::R480p);
        assert_eq!(Resolution::from_height(481), Resolution::R576p);
        assert_eq!(Resolution::from_height(576), Resolution::R576p);
        assert_eq!(Resolution::from_height(577), Resolution::R720p);
        assert_eq!(Resolution::from_height(720), Resolution::R720p);
        assert_eq!(Resolution::from_height(721), Resolution::R1080p);
        assert_eq!(Resolution::from_height(1080), Resolution::R1080p);
        assert_eq!(Resolution::from_height(1081), Resolution::R2160p);
        assert_eq!(Resolution::from_height(2160), Resolution::R2160p);
        assert_eq!(Resolution::from_height(2161), Resolution::R4320p);
    }

    // ── AudioChannels::from_count ─────────────────────────────────────────

    /// Every mapped count 1..=8, plus an out-of-range fallback. Deleting any
    /// one match arm makes that count fall through to `_ => Unknown`.
    #[test]
    fn audio_channels_from_count_every_mapped_value() {
        assert_eq!(AudioChannels::from_count(1), AudioChannels::Mono);
        assert_eq!(AudioChannels::from_count(2), AudioChannels::Stereo);
        assert_eq!(AudioChannels::from_count(3), AudioChannels::Stereo21);
        assert_eq!(AudioChannels::from_count(4), AudioChannels::Quad);
        assert_eq!(AudioChannels::from_count(5), AudioChannels::Surround50);
        assert_eq!(AudioChannels::from_count(6), AudioChannels::Surround51);
        assert_eq!(AudioChannels::from_count(7), AudioChannels::Surround61);
        assert_eq!(AudioChannels::from_count(8), AudioChannels::Surround71);
        assert_eq!(AudioChannels::from_count(0), AudioChannels::Unknown);
        assert_eq!(AudioChannels::from_count(9), AudioChannels::Unknown);
    }

    // FMKV metadata / json:// carry the Display string; every layout must round-trip.
    #[test]
    fn audio_channels_layout_strings_round_trip() {
        for s in ["3.0", "3.1", "4.1", "6.0", "7.0", "2.1", "5.1", "7.1"] {
            let parsed: AudioChannels = s.parse().unwrap();
            assert_eq!(parsed.to_string(), s);
        }
    }

    #[test]
    fn audio_channels_from_layout_names_or_unknown() {
        for full in 0..=8u8 {
            for lfe in 0..=2u8 {
                let c = AudioChannels::from_layout(full, lfe);
                if c == AudioChannels::Unknown {
                    continue;
                }
                assert_eq!(c.count(), full + lfe, "{full}.{lfe}");
                assert_eq!(
                    c.to_string(),
                    format!("{full}.{lfe}")
                        .replace("1.0", "mono")
                        .replace("2.0", "stereo")
                );
            }
        }
        assert_eq!(AudioChannels::from_layout(3, 0), AudioChannels::Surround30);
        assert_eq!(AudioChannels::from_layout(7, 0), AudioChannels::Surround70);
        assert_eq!(AudioChannels::from_layout(5, 2), AudioChannels::Unknown);
        assert_eq!(AudioChannels::from_layout(1, 1), AudioChannels::Unknown);
        assert_eq!(AudioChannels::from_layout(8, 0), AudioChannels::Unknown);
    }

    // ── SampleRate::from_hz ───────────────────────────────────────────────
    // Every rate the enum can represent, in Hz. Combo rates (S48_96,
    // S48_192) have no Hz spelling — 48000 must map back to plain S48 only.
    #[test]
    fn sample_rate_from_hz_every_mapped_rate() {
        assert_eq!(SampleRate::from_hz(44_100), SampleRate::S44_1);
        assert_eq!(SampleRate::from_hz(48_000), SampleRate::S48);
        assert_eq!(SampleRate::from_hz(88_200), SampleRate::S88_2);
        assert_eq!(SampleRate::from_hz(96_000), SampleRate::S96);
        assert_eq!(SampleRate::from_hz(176_400), SampleRate::S176_4);
        assert_eq!(SampleRate::from_hz(192_000), SampleRate::S192);
        assert_eq!(SampleRate::from_hz(0), SampleRate::Unknown);
        assert_eq!(SampleRate::from_hz(32_000), SampleRate::Unknown);
    }

    // from_hz must invert hz() for every rate with a single Hz value (all
    // but the two combo rates). Round-tripping keeps this honest if a rate
    // is ever added.
    #[test]
    fn sample_rate_from_hz_inverts_hz_for_single_rate_variants() {
        for r in [
            SampleRate::S44_1,
            SampleRate::S48,
            SampleRate::S88_2,
            SampleRate::S96,
            SampleRate::S176_4,
            SampleRate::S192,
        ] {
            assert_eq!(
                SampleRate::from_hz(r.hz() as u32),
                r,
                "from_hz must round-trip {r:?}"
            );
        }
    }

    // ── HdrFormat / ColorSpace: name, Display, FromStr ────────────────────

    /// `Display` must forward to `name()`, not emit an empty string: these
    /// strings reach Matroska track names and the JSON sink, where a blank
    /// HDR field is indistinguishable from "no HDR metadata".
    #[test]
    fn hdr_format_display_forwards_to_name() {
        assert_eq!(format!("{}", HdrFormat::Hdr10Plus), "HDR10+");
        assert_eq!(format!("{}", HdrFormat::DolbyVision), "Dolby Vision");
        assert_eq!(format!("{}", HdrFormat::Hlg), HdrFormat::Hlg.name());
    }

    // FromStr accepts human display names as well as compact ids, via a
    // second linear scan keyed on `name(v) == s`; every probe here is a
    // display name (not its own id) and not the first table entry.
    #[test]
    fn hdr_format_from_str_resolves_display_names() {
        assert_eq!(
            "Dolby Vision".parse::<HdrFormat>(),
            Ok(HdrFormat::DolbyVision)
        );
        assert_eq!("HDR10+".parse::<HdrFormat>(), Ok(HdrFormat::Hdr10Plus));
        assert_eq!("HLG".parse::<HdrFormat>(), Ok(HdrFormat::Hlg));
        // An unrecognised string is an error, never a silent SDR.
        assert_eq!("not-an-hdr-format".parse::<HdrFormat>(), Err(()));
    }

    /// `ColorSpace::name` is the ITU-R designation used in track metadata.
    /// `Unknown` is the one variant with no designation: it names the empty
    /// string so nothing prints a fabricated colour space.
    #[test]
    fn color_space_name_is_the_itu_designation() {
        assert_eq!(ColorSpace::Bt709.name(), "BT.709");
        assert_eq!(ColorSpace::Bt2020.name(), "BT.2020");
        assert_eq!(ColorSpace::Bt470bg.name(), "BT.470BG");
        assert_eq!(ColorSpace::Smpte170m.name(), "SMPTE 170M");
        assert!(ColorSpace::Unknown.name().is_empty());
    }

    /// `Display` must forward to `name()`.
    #[test]
    fn color_space_display_forwards_to_name() {
        assert_eq!(format!("{}", ColorSpace::Bt2020), "BT.2020");
        assert_eq!(
            format!("{}", ColorSpace::Smpte170m),
            ColorSpace::Smpte170m.name()
        );
    }

    /// Same second-scan property as `HdrFormat`: display names resolve, and
    /// they resolve to THEIR OWN variant. `ColorSpace` has no error case — an
    /// unrecognised string is `Unknown`, not `Err`.
    #[test]
    fn color_space_from_str_resolves_display_names() {
        assert_eq!("BT.2020".parse::<ColorSpace>(), Ok(ColorSpace::Bt2020));
        assert_eq!("BT.470BG".parse::<ColorSpace>(), Ok(ColorSpace::Bt470bg));
        assert_eq!(
            "SMPTE 170M".parse::<ColorSpace>(),
            Ok(ColorSpace::Smpte170m)
        );
        assert_eq!("bt2020".parse::<ColorSpace>(), Ok(ColorSpace::Bt2020));
        assert_eq!("nonsense".parse::<ColorSpace>(), Ok(ColorSpace::Unknown));
    }

    // ── DiscTitle stream filters ──────────────────────────────────────────
    // A title whose stream list interleaves all three kinds; each accessor
    // must yield exactly its own kind, in declared order.
    #[test]
    fn disc_title_stream_filters_select_their_own_kind_in_order() {
        let mut title = DiscTitle::empty();
        title.streams = vec![
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: false,
                qualifier: LabelQualifier::None,
                codec_data: None,
            }),
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Hevc,
                resolution: Resolution::R2160p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Hdr10,
                color_space: ColorSpace::Bt2020,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            }),
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::TrueHd,
                channels: AudioChannels::Surround71,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: LabelPurpose::Normal,
                label: String::new(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1101,
                codec: Codec::Ac3,
                channels: AudioChannels::Stereo,
                language: "fra".into(),
                sample_rate: SampleRate::S48,
                secondary: true,
                purpose: LabelPurpose::Commentary,
                label: String::new(),
            }),
            // Blu-ray 3D dependent view: a second video stream.
            Stream::Video(VideoStream {
                pid: 0x1012,
                codec: Codec::H264,
                resolution: Resolution::R1080p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                display_aspect: None,
                secondary: true,
                label: String::new(),
                measured_cicp: None,
            }),
        ];

        let audio: Vec<u16> = title.audio_streams().map(|a| a.pid).collect();
        assert_eq!(
            audio,
            vec![0x1100, 0x1101],
            "audio_streams must yield both audio PIDs in declared order"
        );
        let subs: Vec<u16> = title.subtitle_streams().map(|s| s.pid).collect();
        assert_eq!(subs, vec![0x1200]);
        let video: Vec<u16> = title.video_streams().map(|v| v.pid).collect();
        assert_eq!(
            video,
            vec![0x1011, 0x1012],
            "video_streams must yield the base view then the dependent view"
        );
        // The three filters partition the stream list: nothing is dropped and
        // nothing is counted twice.
        assert_eq!(audio.len() + subs.len() + video.len(), title.streams.len());
        // Each accessor's payload is the real stream, not a placeholder.
        assert_eq!(
            title.audio_streams().next().unwrap().channels,
            AudioChannels::Surround71
        );
        assert_eq!(
            title.video_streams().next().unwrap().resolution,
            Resolution::R2160p
        );
        assert_eq!(title.subtitle_streams().next().unwrap().language, "eng");
    }

    // ── DiscId::name ──────────────────────────────────────────────────────
    // The disc's best available name: the META/DL bdmt_*.xml title when
    // present, otherwise the UDF Volume Identifier.
    #[test]
    fn disc_id_name_prefers_meta_title_then_volume_id() {
        let with_meta = DiscId {
            volume_id: "SAMPLE_FILM".to_string(),
            meta_title: Some("Sample Film".to_string()),
            format: DiscFormat::BluRay,
            capacity_sectors: 0,
            encrypted: false,
            layers: 1,
        };
        assert_eq!(with_meta.name(), with_meta.meta_title.as_deref().unwrap());
        let without_meta = DiscId {
            meta_title: None,
            ..with_meta
        };
        assert_eq!(without_meta.name(), without_meta.volume_id);
    }

    // ── canonical_title_order: the capacity gate is STRICTLY greater-than ──
    // The gate is size_bytes <= capacity_bytes: a title whose size EXACTLY
    // equals capacity is REAL and must outrank an oversize composite.
    #[test]
    fn canonical_order_capacity_gate_admits_a_title_that_exactly_fills_the_disc() {
        use std::cmp::Ordering;
        const CAP: u64 = 50_000_000_000;
        // Exactly fills the disc — physically possible, therefore real.
        let exact = title_with("00800.mpls", 7_200.0, CAP, 1);
        // Twice the disc: cannot exist unless clips are double-counted.
        let huge = title_with("00020.mpls", 15_000.0, CAP * 2, 253);
        // A smaller real title.
        let smaller = title_with("00200.mpls", 3_600.0, CAP / 2, 1);

        // exact (real) before huge (composite), whichever way round it is asked.
        assert_eq!(
            Disc::canonical_title_order(&exact, &huge, CAP),
            Ordering::Less,
            "a title that exactly fills the disc is real and outranks the oversize composite"
        );
        assert_eq!(
            Disc::canonical_title_order(&huge, &exact, CAP),
            Ordering::Greater,
            "the oversize composite is demoted behind the exactly-fitting real title"
        );
        // Both real: the LARGER real title wins. `exact` is the larger, so it
        // must still be treated as real when it is the RIGHT-hand argument.
        assert_eq!(
            Disc::canonical_title_order(&smaller, &exact, CAP),
            Ordering::Greater,
            "the exactly-fitting title is real on the right-hand side too, and it is larger"
        );
        assert_eq!(
            Disc::canonical_title_order(&exact, &smaller, CAP),
            Ordering::Less
        );
    }

    // capacity_bytes == 0 means UNKNOWN (READ CAPACITY failed): a literal
    // gate reading inverts on 0, wrongly letting an empty title win
    // titles[0]. The gate must be INERT when capacity is unknown.
    #[test]
    fn canonical_order_unknown_capacity_does_not_demote_every_real_title() {
        const UNKNOWN: u64 = 0; // READ CAPACITY failed
        let feature = title_with("00800.mpls", 7_320.0, 57_200_000_000, 1);
        // A playlist whose CLPI files are missing/unparseable: no declared size.
        let sizeless = title_with("00001.mpls", 120.0, 0, 1);
        let mut titles = [sizeless, feature];
        titles.sort_by(|a, b| Disc::canonical_title_order(a, b, UNKNOWN));
        assert_eq!(
            titles[0].playlist, "00800.mpls",
            "with an UNKNOWN capacity the real feature must still sort first; \
             a size-0 title must not be promoted ahead of it"
        );
        assert_eq!(titles[1].playlist, "00001.mpls");
    }

    // Control: making the gate inert on UNKNOWN capacity must not make it
    // dead — with a KNOWN capacity an oversize composite is still demoted.
    #[test]
    fn canonical_order_known_capacity_still_demotes_a_genuinely_oversize_title() {
        use std::cmp::Ordering;
        const CAP: u64 = 58_500_000_000;
        let composite = title_with("00020.mpls", 15_180.0, 92_400_000_000, 253);
        let real = title_with("00800.mpls", 7_320.0, 57_200_000_000, 1);
        assert_eq!(
            Disc::canonical_title_order(&real, &composite, CAP),
            Ordering::Less,
            "a known capacity must still demote the oversize composite"
        );
        assert_eq!(
            Disc::canonical_title_order(&composite, &real, CAP),
            Ordering::Greater,
            "…in either argument order"
        );
    }

    // ── audio_richness: the same-size / same-duration tiebreak ─────────────

    /// A title carrying the given audio tracks, with size and duration fixed so
    /// every comparison below falls through to the audio-richness tiebreak.
    fn title_with_audio(audio: &[(Codec, AudioChannels)]) -> DiscTitle {
        DiscTitle {
            size_bytes: 40_000_000_000,
            duration_secs: 7_200.0,
            streams: audio
                .iter()
                .enumerate()
                .map(|(i, &(codec, channels))| {
                    Stream::Audio(AudioStream {
                        pid: 0x1100 + i as u16,
                        codec,
                        channels,
                        language: "eng".into(),
                        sample_rate: SampleRate::S48,
                        secondary: false,
                        purpose: LabelPurpose::Normal,
                        label: String::new(),
                    })
                })
                .collect(),
            ..DiscTitle::empty()
        }
    }

    // Equal-size/duration siblings are separated by audio richness (any
    // lossless, best channel count, track count). Each assertion varies ONE
    // component; the final pair is identical and must compare Equal.
    #[test]
    fn canonical_order_breaks_equal_size_ties_on_audio_richness() {
        use std::cmp::Ordering;
        const CAP: u64 = 50_000_000_000;

        // (1) lossless beats lossy at the same channel count and track count.
        let lossless = title_with_audio(&[(Codec::DtsHdMa, AudioChannels::Stereo)]);
        let lossy = title_with_audio(&[(Codec::Ac3, AudioChannels::Stereo)]);
        assert_eq!(
            Disc::canonical_title_order(&lossless, &lossy, CAP),
            Ordering::Less,
            "a lossless track outranks a lossy one"
        );
        assert_eq!(
            Disc::canonical_title_order(&lossy, &lossless, CAP),
            Ordering::Greater
        );

        // (2) more channels wins when both are lossy and single-track.
        let surround = title_with_audio(&[(Codec::Ac3, AudioChannels::Surround51)]);
        assert_eq!(
            Disc::canonical_title_order(&surround, &lossy, CAP),
            Ordering::Less,
            "5.1 outranks stereo at the same losslessness"
        );

        // (3) more tracks wins when losslessness and channel count are equal.
        let two_tracks = title_with_audio(&[
            (Codec::Ac3, AudioChannels::Stereo),
            (Codec::Ac3, AudioChannels::Stereo),
        ]);
        assert_eq!(
            Disc::canonical_title_order(&two_tracks, &lossy, CAP),
            Ordering::Less,
            "the title with more audio tracks is the richer one"
        );

        // (4) identical audio really is a tie.
        let same = title_with_audio(&[(Codec::Ac3, AudioChannels::Stereo)]);
        assert_eq!(
            Disc::canonical_title_order(&same, &lossy, CAP),
            Ordering::Equal,
            "identical titles must compare Equal — the tiebreak is a real comparison, not a constant"
        );
    }

    // ── detect_disc_format: the MKB-less BDMV fallback ────────────────────
    // No readable MKB falls back to video resolution: UHD PROMOTES to
    // DiscFormat::Uhd, everything else is clamped UP to BluRay (never DVD).
    #[test]
    fn mkb_less_bdmv_disc_is_promoted_to_uhd_by_resolution_and_clamped_up_otherwise() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            // BDMV only — no /AACS, so there is no MKB Type record to read.
            subdirs: vec![DirSpec {
                name: "BDMV".into(),
                icb_lba: 12,
                dir_data_lba: 13,
                files: Vec::new(),
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let uhd = [title_with_video(Codec::Hevc, Resolution::R2160p)];
        assert_eq!(
            Disc::detect_disc_format(&mut disc, &udf, &uhd),
            DiscFormat::Uhd,
            "a 2160p BDMV disc with no MKB is a UHD"
        );
        let hd = [title_with_video(Codec::H264, Resolution::R1080p)];
        assert_eq!(
            Disc::detect_disc_format(&mut disc, &udf, &hd),
            DiscFormat::BluRay
        );
        let sd = [title_with_video(Codec::Mpeg2, Resolution::R480i)];
        assert_eq!(
            Disc::detect_disc_format(&mut disc, &udf, &sd),
            DiscFormat::BluRay,
            "an SD title on a BDMV disc must never downgrade the disc to DVD"
        );
    }

    // ── whole-disc bus-removal gate ───────────────────────────────────────
    // Verbatim quotes, AACS Blu-ray Disc Pre-recorded Book, Final Rev 0.953 (subscript 1₂ as 1b).
    // The registered quotes (`crate::spec`, checked against tests/spec_quotes.txt).
    const SPEC_BD_3_7_BEF: &str = crate::spec::keys::KS_18_BUS_ENCRYPTION_FLAG.text;
    const SPEC_BD_3_7_NOTE: &str = "AACS BD Pre-recorded Book 0.953 §3.7 (Note): \"PC Host \
        shall decrypt bus-encrypted Clip AV stream file and hand it over to the application.\"";
    // BD tree: two m2ts (only one in a title), an SSIF, a clear index.bdmv, and
    // an AACS content cert whose byte 1 carries the BEE flag.
    fn bus_fixture(cert_byte1: u8) -> (crate::udf::fixture::MemDisc, udf::UdfFs) {
        let mut cert = vec![0u8; 32];
        cert[0] = 0x10;
        cert[1] = cert_byte1;
        bus_fixture_with(Some(cert), Vec::new())
    }

    // `cert` None = no content cert on disc; `m2ts2` = 00002.m2ts bytes (6 sectors).
    fn bus_fixture_with(
        cert: Option<Vec<u8>>,
        m2ts2: Vec<u8>,
    ) -> (crate::udf::fixture::MemDisc, udf::UdfFs) {
        use crate::udf::fixture::*;
        let mut m2 = file_with("00002.m2ts", 41, 2_000, m2ts2, true);
        m2.size = 6 * 2048;
        let aacs_files = cert
            .map(|c| vec![file_with("Content000.cer", 44, 600, c, true)])
            .unwrap_or_default();
        let stream = DirSpec {
            name: "STREAM".into(),
            icb_lba: 30,
            dir_data_lba: 31,
            files: vec![file("00001.m2ts", 40, 1_000, 3 * 2048, true), m2],
            subdirs: vec![DirSpec {
                name: "SSIF".into(),
                icb_lba: 32,
                dir_data_lba: 33,
                files: vec![file("00003.ssif", 42, 3_000, 3 * 2048, true)],
                subdirs: vec![],
            }],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "BDMV".into(),
                    icb_lba: 20,
                    dir_data_lba: 21,
                    files: vec![file("index.bdmv", 43, 500, 2048, true)],
                    subdirs: vec![stream],
                },
                DirSpec {
                    name: "AACS".into(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: aacs_files,
                    subdirs: vec![],
                },
            ],
        };
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        (disc, udf)
    }

    // Regression (v1.7.0 de-bussed every encrypted unit): the cert-route bus gate
    // must cover EVERY stream file, not just kept titles, else dir:// / ISO / sweep
    // leave non-title clips bus-encrypted. Clear nav (index.bdmv) stays outside.
    #[test]
    fn bus_content_ranges_cover_every_stream_file_not_just_titles() {
        use crate::udf::fixture::PART_START;
        let (mut mem, udf) = bus_fixture(0x80);
        let mut feature = DiscTitle::empty();
        feature.extents = vec![ext(PART_START + 1_000, 3)];
        let stream = Disc::stream_file_extents(&mut mem, &udf, no_pause()).unwrap();
        assert_eq!(
            bus_map(stream.files, &[feature]).covered_ranges(),
            vec![
                (PART_START + 1_000, 3),
                (PART_START + 2_000, 6),
                (PART_START + 3_000, 3)
            ],
            "non-title m2ts and SSIF must be de-bussed too; nav stays clear"
        );
    }

    // The public whole-disc stream map: every /BDMV/STREAM file, nav excluded.
    #[test]
    fn stream_content_ranges_lists_every_stream_file() {
        use crate::udf::fixture::PART_START;
        let (mut mem, _) = bus_fixture(0x80);
        assert_eq!(
            Disc::stream_content_ranges(&mut mem).unwrap(),
            vec![
                (PART_START + 1_000, 3),
                (PART_START + 2_000, 6),
                (PART_START + 3_000, 3)
            ]
        );
    }

    // Zero 00002.m2ts's File Entry (tag 0) after the tree was read: its extents are unreadable.
    fn corrupt_m2ts2_icb(mem: &mut crate::udf::fixture::MemDisc) {
        mem.put_bytes(crate::udf::fixture::PART_START + 41, &[0u8; 2048]);
    }

    // A reader that fails one LBA with `err` and serves the rest from `inner`.
    struct FailAt {
        inner: crate::udf::fixture::MemDisc,
        lba: u32,
        err: fn() -> Error,
    }

    impl SectorSource for FailAt {
        fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            if (lba..lba + n as u32).contains(&self.lba) {
                return Err((self.err)());
            }
            self.inner.read_sectors(lba, n, buf, r)
        }
    }

    const SPEC_BD_3_7_NOT_STREAM: &str = "AACS BD Pre-recorded Book 0.953 §3.7: \"the BEF \
        shall be set to 0b for the sectors that do not correspond to Clip AV stream files under \
        \\BDMV\\STREAM directory.\"";
    const SPEC_BD_3_7_OTHERWISE: &str = "AACS BD Pre-recorded Book 0.953 §3.7: \"If the BEE \
        flag in the Content Certificate is set to 1b, the BEF shall be set to 1b [...]. \
        Otherwise, the BEF shall be set to 0b.\"";
    const SPEC_BD_8_1_3: &str = "AACS BD Pre-recorded Book 0.953 §8.1.3: \"When the Clip AV \
        stream files are bus-encrypted as defined in Secion 3.7 of this specification, the \
        corresponding Stereoscopic Interleaved files are also bus-encrypted.\"";
    const SPEC_BD_3_10_1: &str = crate::spec::keys::KS_2_ALIGNED_UNIT.text;

    fn unmapped_paths(scan: &StreamScan) -> Vec<&str> {
        scan.unmapped.iter().map(|u| u.path.as_str()).collect()
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: "PC Host shall decrypt bus-encrypted Clip AV stream file".
    #[test]
    fn stream_content_ranges_fails_closed_naming_the_unreadable_stream_file() {
        let (mut mem, _) = bus_fixture(0x80);
        corrupt_m2ts2_icb(&mut mem);
        match Disc::stream_content_ranges(&mut mem) {
            Err(e @ Error::BusStreamUnmapped { .. }) => assert_eq!(
                e.to_string(),
                format!(
                    "E{}: /BDMV/STREAM/00002.m2ts (E{}: {})",
                    crate::error::E_BUS_STREAM_UNMAPPED,
                    crate::error::E_DISC_READ,
                    crate::udf::fixture::PART_START + 41
                ),
                "{SPEC_BD_3_7_NOTE}"
            ),
            other => {
                panic!("{SPEC_BD_3_7_NOTE}: a dropped file would ship bus-encrypted: {other:?}")
            }
        }
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: an unlocated file is recorded (by path + cause), not dropped.
    #[test]
    fn stream_file_extents_records_the_unreadable_file_and_maps_the_rest() {
        use crate::udf::fixture::PART_START;
        let (mut mem, udf) = bus_fixture(0x80);
        corrupt_m2ts2_icb(&mut mem);
        let mut scan =
            Disc::stream_file_extents(&mut mem, &udf, no_pause()).expect(SPEC_BD_3_7_NOTE);
        assert_eq!(
            unmapped_paths(&scan),
            ["/BDMV/STREAM/00002.m2ts"],
            "{SPEC_BD_3_7_NOTE}"
        );
        let u = &scan.unmapped[0];
        assert_eq!(u.icb, 41);
        let fe_read = Error::DiscRead {
            sector: (PART_START + 41) as u64,
            status: None,
            sense: None,
        };
        assert_eq!(u.cause, fe_read.to_string(), "names which read failed");
        scan.files.sort();
        assert_eq!(
            scan.files,
            vec![
                vec![(Some(PART_START + 1_000), 3)],
                vec![(Some(PART_START + 3_000), 3)],
            ],
            "{SPEC_BD_3_7_BEF}: the other files stay mapped"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_BEF`]: every Clip AV stream file is in scope, whichever one fails.
    #[test]
    fn stream_file_extents_records_any_unreadable_stream_file() {
        use crate::udf::fixture::PART_START;
        for (icb, name) in [(40u32, "00001.m2ts"), (41, "00002.m2ts")] {
            let (mut mem, udf) = bus_fixture(0x80);
            mem.put_bytes(PART_START + icb, &[0u8; 2048]);
            let scan =
                Disc::stream_file_extents(&mut mem, &udf, no_pause()).expect(SPEC_BD_3_7_BEF);
            let want = format!("/BDMV/STREAM/{name}");
            assert_eq!(unmapped_paths(&scan), [want.as_str()], "{SPEC_BD_3_7_BEF}");
            assert_eq!(scan.files.len(), 2, "{SPEC_BD_3_7_BEF}");
        }
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_8_1_3`]: an SSIF file is bus-encrypted too, so it is recorded as well.
    #[test]
    fn stream_file_extents_records_an_unreadable_ssif() {
        use crate::udf::fixture::PART_START;
        let (mut mem, udf) = bus_fixture(0x80);
        mem.put_bytes(PART_START + 42, &[0u8; 2048]);
        let scan = Disc::stream_file_extents(&mut mem, &udf, no_pause()).expect(SPEC_BD_8_1_3);
        assert_eq!(
            unmapped_paths(&scan),
            ["/BDMV/STREAM/SSIF/00003.ssif"],
            "{SPEC_BD_8_1_3}"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: a drive read fault on a File Entry is recorded with its cause.
    #[test]
    fn stream_file_extents_records_a_drive_read_fault() {
        use crate::udf::fixture::PART_START;
        let (mem, udf) = bus_fixture(0x80);
        let mut r = FailAt {
            inner: mem,
            lba: PART_START + 40,
            err: || Error::DiscRead {
                sector: (PART_START + 40) as u64,
                status: None,
                sense: None,
            },
        };
        let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect(SPEC_BD_3_7_NOTE);
        assert_eq!(
            unmapped_paths(&scan),
            ["/BDMV/STREAM/00001.m2ts"],
            "{SPEC_BD_3_7_NOTE}"
        );
        let fault = Error::DiscRead {
            sector: (PART_START + 40) as u64,
            status: None,
            sense: None,
        };
        assert_eq!(scan.unmapped[0].cause, fault.to_string());
    }

    // Fails reads of each of `lbas` with `err` `left` times (u32::MAX = always), logging
    // every read of them as `(lba, recovery, fua)`.
    struct Flaky {
        inner: crate::udf::fixture::MemDisc,
        lbas: Vec<u32>,
        left: u32,
        err: fn() -> Error,
        reads: Vec<(u32, bool, bool)>,
    }

    impl SectorSource for Flaky {
        fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            self.read_sectors_fua(lba, n, buf, r, false)
        }
        fn read_sectors_fua(
            &mut self,
            lba: u32,
            n: u16,
            buf: &mut [u8],
            r: bool,
            fua: bool,
        ) -> Result<usize> {
            if let Some(&hit) = self
                .lbas
                .iter()
                .find(|&&l| (lba..lba + n as u32).contains(&l))
            {
                self.reads.push((hit, r, fua));
                if self.left > 0 {
                    self.left = self.left.saturating_sub(u32::from(self.left != u32::MAX));
                    return Err((self.err)());
                }
            }
            self.inner.read_sectors(lba, n, buf, r)
        }
    }

    fn sense_error(sense_key: u8) -> Error {
        Error::DiscRead {
            sector: 0,
            status: Some(0x02),
            sense: Some(crate::scsi::ScsiSense {
                sense_key,
                asc: 0x11,
                ascq: 0x00,
            }),
        }
    }

    fn medium_error() -> Error {
        sense_error(0x03)
    }

    // Faults on the File Entries (ICB `icbs`) of the bus fixture's stream files.
    fn flaky(icbs: &[u32], left: u32, err: fn() -> Error) -> (Flaky, udf::UdfFs) {
        let (inner, udf) = bus_fixture(0x80);
        let lbas = icbs
            .iter()
            .map(|i| crate::udf::fixture::PART_START + i)
            .collect();
        let reads = Vec::new();
        (
            Flaky {
                inner,
                lbas,
                left,
                err,
                reads,
            },
            udf,
        )
    }

    fn flaky_m2ts1(left: u32, err: fn() -> Error) -> (Flaky, udf::UdfFs) {
        flaky(&[40], left, err)
    }

    // The scan's re-read allowance with no pause (the 5 s pause is exercised on its own).
    fn no_pause() -> FeRereads {
        FeRereads::with_pause(None, std::time::Duration::ZERO)
    }

    // A transient File Entry fault is recovered on re-read 1 (recovery + FUA), not recorded.
    #[test]
    fn a_transient_file_entry_fault_is_recovered_on_the_first_reread() {
        let (mut r, udf) = flaky_m2ts1(1, medium_error);
        let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect("scan");
        assert!(
            scan.unmapped.is_empty(),
            "{SPEC_BD_3_7_BEF}: recovered file stays mapped"
        );
        assert_eq!(scan.files.len(), 3);
        let icb = crate::udf::fixture::PART_START + 40;
        assert_eq!(
            r.reads,
            [(icb, true, false), (icb, true, true)],
            "read, then FUA re-read"
        );
    }

    // A count, not a timeout: at most FE_REREADS_PER_FILE re-reads, then the file is recorded.
    #[test]
    fn a_persistent_file_entry_fault_gets_two_rereads_then_is_recorded() {
        let (mut r, udf) = flaky_m2ts1(u32::MAX, medium_error);
        let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect("scan");
        assert_eq!(unmapped_paths(&scan), ["/BDMV/STREAM/00001.m2ts"]);
        assert_eq!(r.reads.len(), 3, "the read, then 2 re-reads");
        assert!(r.reads[1..].iter().all(|&(_, rec, fua)| rec && fua));
    }

    // HARDWARE ERROR / ILLEGAL REQUEST is the BU40N fast-fail (wedge) state: another read
    // only digs it deeper, so the file is recorded with no re-read.
    #[test]
    fn wedge_family_sense_gets_no_reread() {
        for err in [(|| sense_error(0x04)) as fn() -> Error, || {
            sense_error(0x05)
        }] {
            let (mut r, udf) = flaky_m2ts1(u32::MAX, err);
            let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect("scan");
            assert_eq!(unmapped_paths(&scan), ["/BDMV/STREAM/00001.m2ts"]);
            assert_eq!(r.reads.len(), 1, "{:?}", err());
        }
    }

    // The re-read budget is per scan: three bad File Entries share FE_REREADS_PER_SCAN.
    #[test]
    fn the_reread_budget_is_per_scan_not_per_file() {
        let (mut r, udf) = flaky(&[40, 41, 42], u32::MAX, medium_error);
        let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect("scan");
        assert_eq!(scan.unmapped.len(), 3);
        let rereads = r.reads.iter().filter(|&&(_, _, fua)| fua).count();
        assert_eq!(rereads, FE_REREADS_PER_SCAN as usize, "{:?}", r.reads);
        assert_eq!(r.reads.len(), 3 + FE_REREADS_PER_SCAN as usize);
    }

    // A Stop during the pause before a re-read ends the walk as Halted, without the re-read.
    #[test]
    fn a_stop_during_the_reread_pause_is_halted() {
        let (mut r, udf) = flaky_m2ts1(u32::MAX, medium_error);
        let halt = crate::halt::Halt::new();
        let stop = halt.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            stop.cancel();
        });
        let pause = std::time::Duration::from_secs(30);
        let t0 = std::time::Instant::now();
        let got = Disc::stream_file_extents(&mut r, &udf, FeRereads::with_pause(Some(halt), pause));
        t.join().unwrap();
        assert!(matches!(got, Err(Error::Halted)), "{got:?}");
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(5),
            "the pause ends on Stop"
        );
        assert_eq!(r.reads.len(), 1, "no re-read after the Stop");
    }

    // §5.8 "Use `Halt::wait` / `pause` instead" of `thread::sleep`: with or without a
    // token the re-read pause is a `Halt::wait`, so a NoBlockingScope refuses it.
    #[cfg(debug_assertions)]
    #[test]
    fn the_reread_pause_is_a_halt_wait() {
        for halt in [Some(crate::halt::Halt::new()), None] {
            let rereads = FeRereads::with_pause(halt, std::time::Duration::from_millis(1));
            let _scope = crate::halt::diag::NoBlockingScope::enter();
            let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rereads.pause()));
            let msg = match got {
                Err(p) => p.downcast_ref::<String>().cloned().unwrap_or_default(),
                Ok(r) => format!("returned {r:?}"),
            };
            assert_eq!(msg, "Halt::wait (sleep) under NoBlockingScope");
        }
    }

    // No token: nothing can end the pause early, so it is waited out in full.
    #[test]
    fn the_reread_pause_without_a_token_is_waited_out() {
        let pause = std::time::Duration::from_millis(60);
        let t0 = std::time::Instant::now();
        assert!(FeRereads::with_pause(None, pause).pause().is_ok());
        assert!(t0.elapsed() >= pause, "{:?}", t0.elapsed());
    }

    #[test]
    fn the_reread_policy_is_two_per_file_four_per_scan_after_5s() {
        assert_eq!((FE_REREADS_PER_FILE, FE_REREADS_PER_SCAN), (2, 4));
        assert_eq!(FE_REREAD_PAUSE, std::time::Duration::from_secs(5));
        assert_eq!(FeRereads::new(None).pause, FE_REREAD_PAUSE);
        assert_eq!(FeRereads::new(None).left, FE_REREADS_PER_SCAN);
    }

    // A dead transport is not a media fault: no re-read, recorded at once.
    #[test]
    fn a_transport_failure_is_not_reread() {
        let transport = || Error::DiscRead {
            sector: 0,
            status: Some(0xFF),
            sense: None,
        };
        let (mut r, udf) = flaky_m2ts1(u32::MAX, transport);
        let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect("scan");
        assert_eq!(unmapped_paths(&scan), ["/BDMV/STREAM/00001.m2ts"]);
        assert_eq!(r.reads.len(), 1);
    }

    // A Stop during a read ends the walk as Halted, not a recorded file.
    #[test]
    fn a_stop_during_a_file_entry_read_is_halted() {
        let (mut r, udf) = flaky_m2ts1(u32::MAX, || Error::Halted);
        let got = Disc::stream_file_extents(&mut r, &udf, no_pause());
        assert!(matches!(got, Err(Error::Halted)), "{got:?}");
        assert_eq!(r.reads.len(), 1, "Halted stops at once, no re-read");
    }

    // A File Entry that reads but does not parse gets no re-read (the bytes will not change).
    #[test]
    fn a_file_entry_that_reads_but_does_not_parse_is_not_reread() {
        let (mut r, udf) = flaky_m2ts1(0, medium_error);
        break_m2ts1_ads(&mut r.inner);
        let scan = Disc::stream_file_extents(&mut r, &udf, no_pause()).expect("scan");
        assert_eq!(unmapped_paths(&scan), ["/BDMV/STREAM/00001.m2ts"]);
        assert_eq!(r.reads.len(), 1);
    }

    // A leaf serving reads from a MemDisc while reporting a fixed unmapped list.
    struct MemReports(
        crate::udf::fixture::MemDisc,
        Vec<crate::sector::bus_removal::UnmappedStreamFile>,
    );
    impl SectorSource for MemReports {
        fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            self.0.read_sectors(lba, n, buf, r)
        }
        fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
            &self.1
        }
    }

    // FileReadAhead wraps the raw reader during title parse; it must relay the list.
    #[test]
    fn file_read_ahead_forwards_unmapped_stream_files() {
        use crate::sector::bus_removal::test_support::{m2ts1, unmapped_paths};
        let (mem, udf) = bus_fixture(0x80);
        let mut inner = MemReports(mem, vec![m2ts1()]);
        let ra = FileReadAhead::new(&mut inner, &udf, 40).expect("icb 40 is 00001.m2ts");
        assert_eq!(unmapped_paths(&ra), ["/BDMV/STREAM/00001.m2ts"]);
    }

    fn covers(ranges: &[(u32, u32)], lba: u32) -> bool {
        ranges.iter().any(|&(s, n)| lba >= s && lba - s < n)
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOT_STREAM`]: the MKV staging scope is nav + UDF + the chosen
    /// titles' extents; no other stream file's data is read.
    #[test]
    fn mkv_staging_ranges_are_non_stream_sectors_plus_the_chosen_titles() {
        use crate::udf::fixture::PART_START;
        let (mut mem, _) = bus_fixture(0x80);
        let mut disc = make_test_disc(10_000, "UHD");
        let mut t = DiscTitle::empty();
        t.extents = vec![ext(PART_START + 2_000, 6)];
        disc.titles = vec![DiscTitle::empty(), t];
        let r = disc.mkv_staging_ranges(&mut mem, &[1]).expect("scope");
        for lba in [0, 256, PART_START - 1, PART_START] {
            assert!(
                covers(&r, lba),
                "volume structures {lba}: {SPEC_BD_3_7_NOT_STREAM}"
            );
        }
        for icb in [10, 20, 22, 30, 32, 40, 41, 42, 43, 44] {
            assert!(covers(&r, PART_START + icb), "File Entry {icb}");
        }
        for lba in [500, 600] {
            assert!(covers(&r, PART_START + lba), "nav/AACS data {lba}");
        }
        for lba in 2_000..2_006 {
            assert!(
                covers(&r, PART_START + lba),
                "chosen title {lba}: {SPEC_BD_3_7_BEF}"
            );
        }
        for lba in (1_000..1_003).chain(3_000..3_003) {
            assert!(!covers(&r, PART_START + lba), "unchosen stream data {lba}");
        }
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: an unlocatable stream file never enters the staging scope,
    /// so a staged image holds none of its (bus-encrypted) data.
    #[test]
    fn mkv_staging_ranges_leave_out_an_unmapped_stream_file() {
        use crate::udf::fixture::PART_START;
        let (mut mem, _) = bus_fixture(0x80);
        break_m2ts1_ads(&mut mem);
        let mut disc = make_test_disc(10_000, "UHD");
        let mut t = DiscTitle::empty();
        t.extents = vec![ext(PART_START + 2_000, 6)];
        disc.titles = vec![t];
        let r = disc.mkv_staging_ranges(&mut mem, &[0]).expect("scope");
        assert!(covers(&r, PART_START + 40), "its File Entry is metadata");
        for lba in 1_000..1_003 {
            assert!(!covers(&r, PART_START + lba), "{SPEC_BD_3_7_NOTE}");
        }
    }

    #[test]
    fn mkv_staging_ranges_reject_an_unknown_title() {
        let (mut mem, _) = bus_fixture(0x80);
        let disc = make_test_disc(10_000, "UHD");
        let err = disc.mkv_staging_ranges(&mut mem, &[3]).unwrap_err();
        assert!(
            matches!(err, Error::DiscTitleRange { index: 3, count: 0 }),
            "{err:?}"
        );
    }

    // A Stop during the stream-file walk surfaces as Halted, never as a recorded bad file.
    #[test]
    fn stream_file_extents_propagates_halted() {
        use crate::udf::fixture::PART_START;
        for lba in [PART_START + 40, PART_START + 41] {
            let (mem, udf) = bus_fixture(0x80);
            let mut r = FailAt {
                inner: mem,
                lba,
                err: || Error::Halted,
            };
            let got = Disc::stream_file_extents(&mut r, &udf, no_pause());
            assert!(matches!(got, Err(Error::Halted)), "ICB {lba}: {got:?}");
        }
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_10_1`]: an embedded (< 1 sector) file holds no Aligned Unit, so it is skipped.
    #[test]
    fn stream_file_extents_skips_an_embedded_stream_file() {
        use crate::udf::fixture::{PART_START, build_file_icb};
        let (mut mem, udf) = bus_fixture(0x80);
        let mut icb = build_file_icb(100, 2_000, false);
        icb[34] = 3; // ECMA-167 4/14.6.8 AD type 3 = embedded; l_ad stays nonzero
        mem.put_bytes(PART_START + 41, &icb);
        let scan = Disc::stream_file_extents(&mut mem, &udf, no_pause()).expect(SPEC_BD_3_10_1);
        assert!(scan.unmapped.is_empty(), "{SPEC_BD_3_10_1}");
        let map = crate::sector::bus_removal::BusMap::new(scan.files, &[]);
        assert_eq!(
            map.covered_ranges(),
            vec![(PART_START + 1_000, 3), (PART_START + 3_000, 3)],
            "{SPEC_BD_3_10_1}"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOT_STREAM`]: nav files are never bus-encrypted, so their faults are not ours.
    #[test]
    fn stream_file_extents_ignores_unreadable_files_outside_bdmv_stream() {
        use crate::udf::fixture::PART_START;
        let (mut mem, udf) = bus_fixture(0x80);
        mem.put_bytes(PART_START + 43, &[0u8; 2048]); // index.bdmv File Entry
        let scan =
            Disc::stream_file_extents(&mut mem, &udf, no_pause()).expect(SPEC_BD_3_7_NOT_STREAM);
        assert!(scan.unmapped.is_empty(), "{SPEC_BD_3_7_NOT_STREAM}");
        assert_eq!(scan.files.len(), 3, "{SPEC_BD_3_7_NOT_STREAM}");
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_BEF`]: every readable stream file (m2ts + SSIF) is mapped, in file order.
    #[test]
    fn stream_file_extents_maps_every_readable_stream_file() {
        use crate::udf::fixture::PART_START;
        let (mut mem, udf) = bus_fixture(0x80);
        let mut scan =
            Disc::stream_file_extents(&mut mem, &udf, no_pause()).expect(SPEC_BD_3_7_BEF);
        assert!(scan.unmapped.is_empty(), "{SPEC_BD_3_7_BEF}");
        scan.files.sort();
        assert_eq!(
            scan.files,
            vec![
                vec![(Some(PART_START + 1_000), 3)],
                vec![(Some(PART_START + 2_000), 6)],
                vec![(Some(PART_START + 3_000), 3)],
            ],
            "{SPEC_BD_3_7_BEF}"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: the live scan's host-key walk records, not fails, a bad file.
    #[test]
    fn live_bus_map_records_an_unreadable_file_when_host_key_debus_is_on() {
        let (mut mem, udf) = bus_fixture(0x80);
        corrupt_m2ts2_icb(&mut mem);
        let scan =
            Disc::bus_stream_files(&mut mem, &udf, true, no_pause()).expect(SPEC_BD_3_7_NOTE);
        assert_eq!(
            unmapped_paths(&scan),
            ["/BDMV/STREAM/00002.m2ts"],
            "{SPEC_BD_3_7_NOTE}"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_OTHERWISE`]: no host-key stage means nothing to de-bus, so no walk to fail.
    #[test]
    fn live_bus_map_needs_no_stream_files_without_host_key_debus() {
        let (mut mem, udf) = bus_fixture(0x00);
        corrupt_m2ts2_icb(&mut mem);
        assert_eq!(
            Disc::bus_stream_files(&mut mem, &udf, false, no_pause()).expect(SPEC_BD_3_7_OTHERWISE),
            StreamScan::default(),
            "{SPEC_BD_3_7_OTHERWISE}"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_BEF`]: with every file readable, the live map carries them all.
    #[test]
    fn live_bus_map_carries_every_stream_file_when_readable() {
        use crate::udf::fixture::PART_START;
        let (mut mem, udf) = bus_fixture(0x80);
        let scan = Disc::bus_stream_files(&mut mem, &udf, true, no_pause()).expect(SPEC_BD_3_7_BEF);
        assert!(scan.unmapped.is_empty(), "{SPEC_BD_3_7_BEF}");
        assert_eq!(
            bus_map(scan.files, &[]).covered_ranges(),
            vec![
                (PART_START + 1_000, 3),
                (PART_START + 2_000, 6),
                (PART_START + 3_000, 3)
            ],
            "{SPEC_BD_3_7_BEF}"
        );
    }

    // The de-bus key a live scan picks for `rdk` over this disc's captured content cert.
    fn bus_key_for_disc(
        mem: &mut crate::udf::fixture::MemDisc,
        udf: &udf::UdfFs,
        rdk: Option<[u8; 16]>,
    ) -> Option<[u8; 16]> {
        let from = encrypt::CaptureFrom::Live { raw_copy: true };
        let cap = encrypt::capture(mem, udf, from).expect("capture");
        let bus = encrypt::BusOutcome::Handshake(encrypt::HandshakeResult {
            volume_id: [0x11; 16],
            read_data_key: rdk,
            read_data_key_err: None,
            drive_unlocked: false,
        });
        encrypt::bus_key(&cap, &bus)
    }

    // libaacs gates bus decrypt on the content cert BEE flag (`bee && bec`): a
    // BEE=0 disc must not be de-bussed even when the drive served a Read Data Key.
    #[test]
    fn bus_key_dropped_when_content_cert_bee_is_clear() {
        let rdk = Some([0x5Au8; 16]);
        let (mut mem, udf) = bus_fixture(0x00);
        assert_eq!(
            bus_key_for_disc(&mut mem, &udf, rdk),
            None,
            "BEE=0 content cert must force Passthrough"
        );
        let (mut mem, udf) = bus_fixture(0x80);
        assert_eq!(
            bus_key_for_disc(&mut mem, &udf, rdk),
            rdk,
            "BEE=1 keeps the cert-route Read Data Key"
        );
    }

    // Keep-key contract: no cert, a too-short cert, or an unknown cert type gives
    // no BEE verdict, so the drive-served Read Data Key is kept.
    #[test]
    fn bus_key_kept_when_content_cert_absent_or_unparseable() {
        let rdk = Some([0x5Au8; 16]);
        let (mut mem, udf) = bus_fixture_with(None, Vec::new());
        assert_eq!(bus_key_for_disc(&mut mem, &udf, rdk), rdk, "no cert");
        let (mut mem, udf) = bus_fixture_with(Some(vec![0x10, 0x00, 0x00]), Vec::new());
        assert_eq!(bus_key_for_disc(&mut mem, &udf, rdk), rdk, "short cert");
        let mut odd = vec![0u8; 32];
        odd[0] = 0x55;
        let (mut mem, udf) = bus_fixture_with(Some(odd), Vec::new());
        assert_eq!(bus_key_for_disc(&mut mem, &udf, rdk), rdk, "unknown type");
        assert_eq!(
            bus_key_for_disc(&mut mem, &udf, None),
            None,
            "no key stays none"
        );
    }

    // A mock drive serving a MemDisc over READ(10) / READ CAPACITY; every other
    // CDB answers zeros.
    struct MemTransport(crate::udf::fixture::MemDisc);
    impl crate::scsi::ScsiTransport for MemTransport {
        fn execute(
            &mut self,
            cdb: &[u8],
            _direction: crate::scsi::DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<crate::scsi::ScsiResult> {
            data.fill(0);
            match cdb[0] {
                crate::scsi::SCSI_READ_10 => {
                    let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
                    let count = u16::from_be_bytes([cdb[7], cdb[8]]);
                    self.0.read_sectors(lba, count, data, false)?;
                }
                crate::scsi::SCSI_READ_CAPACITY => {
                    data[..4].copy_from_slice(&9_999u32.to_be_bytes());
                    data[4..8].copy_from_slice(&2048u32.to_be_bytes());
                }
                _ => {}
            }
            Ok(crate::scsi::ScsiResult {
                status: 0,
                bytes_transferred: data.len(),
                sense: [0u8; 32],
            })
        }
    }

    // scan() wiring end to end (post-handshake): the handshake's Read Data Key and
    // the content cert's BEE flag pick the drive's bus stage, and the bus map covers
    // a stream file no title plays. Reverting either scan() line goes red.
    #[test]
    fn scan_wires_handshake_key_and_stream_map_onto_the_drive() {
        use crate::sector::SectorSource;
        use crate::udf::fixture::PART_START;
        let rdk = [0x6Du8; 16];
        let mut clear: Vec<u8> = (0..6 * 2048).map(|i| (i * 13 % 251) as u8).collect();
        clear[0] |= 0xC0;
        clear[3 * 2048] |= 0xC0;
        let mut wire = clear.clone();
        crate::aacs::content::encrypt_bus(&mut wire[..3 * 2048], &rdk);
        crate::aacs::content::encrypt_bus(&mut wire[3 * 2048..], &rdk);
        let handshake = || encrypt::HandshakeResult {
            volume_id: [0x11; 16],
            read_data_key: Some(rdk),
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let lba = PART_START + 2_000;
        for (bee, want) in [(0x80u8, &clear), (0x00u8, &wire)] {
            let mut cert = vec![0u8; 32];
            cert[0] = 0x10;
            cert[1] = bee;
            let (mem, _) = bus_fixture_with(Some(cert), wire.clone());
            let mut d = Drive::from_transport_for_test(Box::new(MemTransport(mem)));
            let (capacity, mut buffered, udf) = Disc::read_udf(&mut d).expect("udf");
            let from = encrypt::CaptureFrom::Live { raw_copy: true };
            let cap = encrypt::capture(&mut buffered, &udf, from).expect("capture");
            let bus = encrypt::BusOutcome::Handshake(handshake());
            Disc::live_finish(
                buffered,
                capacity,
                udf,
                Some((cap, bus)),
                &ScanOptions::default(),
            )
            .expect("scan");
            let mut got = vec![0u8; 6 * 2048];
            d.read_sectors(lba, 6, &mut got, false).unwrap();
            assert_eq!(&got, want, "BEE byte {bee:#04x}: non-title m2ts bus state");
        }
    }

    // 00001.m2ts's File Entry keeps its info_length but its l_ad overruns the ICB, so the tree
    // lists it at full size while its extents are unreadable (ECMA-167 4/14.17 L_AD).
    fn break_m2ts1_ads(mem: &mut crate::udf::fixture::MemDisc) {
        use crate::udf::fixture::{PART_START, build_file_icb};
        let mut icb = build_file_icb(3 * 2048, 1_000, true);
        icb[212..216].copy_from_slice(&0xFFFF_0000u32.to_le_bytes());
        mem.put_bytes(PART_START + 40, &icb);
    }

    // A live cert-route scan over `mem` (BEE=1 cert, `m2ts2` = 00002.m2ts wire bytes).
    fn live_bus_scan(
        mem: crate::udf::fixture::MemDisc,
        rdk: Option<[u8; 16]>,
    ) -> Result<(Drive, Disc)> {
        let mut d = Drive::from_transport_for_test(Box::new(MemTransport(mem)));
        let (capacity, mut buffered, udf) = Disc::read_udf(&mut d)?;
        let cap = encrypt::capture(
            &mut buffered,
            &udf,
            encrypt::CaptureFrom::Live { raw_copy: true },
        )?;
        let bus = encrypt::BusOutcome::Handshake(encrypt::HandshakeResult {
            volume_id: [0x11; 16],
            read_data_key: rdk,
            read_data_key_err: None,
            drive_unlocked: false,
        });
        let disc = Disc::live_finish(
            buffered,
            capacity,
            udf,
            Some((cap, bus)),
            &ScanOptions::default(),
        )?;
        Ok((d, disc))
    }

    // 00002.m2ts clear bytes (CPI set on both units) and their bus-encrypted wire form.
    fn m2ts2_clear_and_wire(rdk: &[u8; 16]) -> (Vec<u8>, Vec<u8>) {
        let mut clear: Vec<u8> = (0..6 * 2048).map(|i| (i * 7 % 253) as u8).collect();
        clear[0] |= 0xC0;
        clear[3 * 2048] |= 0xC0;
        let mut wire = clear.clone();
        crate::aacs::content::encrypt_bus(&mut wire[..3 * 2048], rdk);
        crate::aacs::content::encrypt_bus(&mut wire[3 * 2048..], rdk);
        (clear, wire)
    }

    fn bee_cert() -> Vec<u8> {
        let mut cert = vec![0u8; 32];
        cert[0] = 0x10;
        cert[1] = 0x80;
        cert
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: one unlocatable clip must not sink the scan; titles/MKV
    /// still run and every locatable stream file is still de-bussed on read.
    #[test]
    fn live_scan_proceeds_with_an_unreadable_stream_file() {
        use crate::sector::SectorSource;
        use crate::udf::fixture::PART_START;
        let rdk = [0x6Du8; 16];
        let (clear, wire) = m2ts2_clear_and_wire(&rdk);
        let (mut mem, _) = bus_fixture_with(Some(bee_cert()), wire);
        break_m2ts1_ads(&mut mem);
        let (mut d, _disc) = live_bus_scan(mem, Some(rdk)).expect(SPEC_BD_3_7_NOTE);
        let unmapped: Vec<&str> = d
            .unmapped_stream_files()
            .iter()
            .map(|u| u.path.as_str())
            .collect();
        assert_eq!(unmapped, ["/BDMV/STREAM/00001.m2ts"], "{SPEC_BD_3_7_NOTE}");
        let mut got = vec![0u8; 6 * 2048];
        d.read_sectors(PART_START + 2_000, 6, &mut got, false)
            .unwrap();
        assert_eq!(
            got, clear,
            "{SPEC_BD_3_7_BEF}: a locatable file is still de-bussed"
        );
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: an image would carry the unlocated file still bus-encrypted,
    /// so iso:// / sweep refuse up front, naming it.
    #[test]
    fn image_read_refuses_a_live_scan_with_an_unmapped_stream_file() {
        let rdk = [0x6Du8; 16];
        let (_, wire) = m2ts2_clear_and_wire(&rdk);
        let (mut mem, _) = bus_fixture_with(Some(bee_cert()), wire);
        break_m2ts1_ads(&mut mem);
        let (d, _) = live_bus_scan(mem, Some(rdk)).expect("scan");
        let err =
            crate::sector::bus_removal::ensure_image_debussable(&d).expect_err(SPEC_BD_3_7_NOTE);
        assert_eq!(
            err.code(),
            crate::error::E_BUS_STREAM_UNMAPPED,
            "{SPEC_BD_3_7_NOTE}"
        );
        let want = format!(
            "E6021: /BDMV/STREAM/00001.m2ts (E6000: {})",
            crate::udf::fixture::PART_START + 40
        );
        assert_eq!(err.to_string(), want, "{SPEC_BD_3_7_NOTE}: path and cause");
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_BEF`]: every stream file located, so the image path is open.
    #[test]
    fn image_read_allows_a_live_scan_that_located_every_stream_file() {
        let rdk = [0x6Du8; 16];
        let (_, wire) = m2ts2_clear_and_wire(&rdk);
        let (mem, _) = bus_fixture_with(Some(bee_cert()), wire);
        let (d, _) = live_bus_scan(mem, Some(rdk)).expect("scan");
        assert!(d.unmapped_stream_files().is_empty(), "{SPEC_BD_3_7_BEF}");
        crate::sector::bus_removal::ensure_image_debussable(&d).expect(SPEC_BD_3_7_BEF);
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_OTHERWISE`]: no host-key stage leaves nothing bus-encrypted to refuse.
    #[test]
    fn image_read_allows_an_unreadable_stream_file_without_host_key_debus() {
        let rdk = [0x6Du8; 16];
        let (_, wire) = m2ts2_clear_and_wire(&rdk);
        let (mut mem, _) = bus_fixture_with(Some(bee_cert()), wire);
        break_m2ts1_ads(&mut mem);
        let (d, _) = live_bus_scan(mem, None).expect("scan");
        crate::sector::bus_removal::ensure_image_debussable(&d).expect(SPEC_BD_3_7_OTHERWISE);
    }

    /// Per spec; do not change without a spec citation proving otherwise.
    /// [`SPEC_BD_3_7_NOTE`]: dir:// extracts every other file (de-bussed) and counts the
    /// unlocated one lost whole, never writing its bus-encrypted bytes.
    #[test]
    fn dir_extract_marks_the_unmapped_stream_file_lost_and_extracts_the_rest() {
        let rdk = [0x6Du8; 16];
        let (clear, wire) = m2ts2_clear_and_wire(&rdk);
        let (mut mem, _) = bus_fixture_with(Some(bee_cert()), wire);
        break_m2ts1_ads(&mut mem);
        let (mut d, disc) = live_bus_scan(mem, Some(rdk)).expect("scan");
        let dest = tempfile::tempdir().unwrap();
        let res = disc
            .extract_tree(&mut d, dest.path(), &ExtractOptions::default())
            .expect(SPEC_BD_3_7_NOTE);
        let stream = dest.path().join("BDMV/STREAM");
        assert_eq!(
            std::fs::read(stream.join("00002.m2ts")).unwrap(),
            clear,
            "{SPEC_BD_3_7_NOTE}"
        );
        assert!(stream.join("SSIF/00003.ssif").is_file());
        assert!(dest.path().join("BDMV/index.bdmv").is_file());
        assert!(!stream.join("00001.m2ts").exists(), "{SPEC_BD_3_7_NOTE}");
        assert!(
            !stream.join("00001.m2ts.partial").exists(),
            "{SPEC_BD_3_7_NOTE}"
        );
        let lost: Vec<_> = res.files.iter().filter(|f| !f.complete).collect();
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].path, std::path::Path::new("BDMV/STREAM/00001.m2ts"));
        assert_eq!(
            (lost[0].bytes_good, lost[0].bytes_unreadable),
            (0, 3 * 2048)
        );
        assert_eq!(res.bytes_lost(), 3 * 2048, "existing damage accounting");
        assert!(!res.complete && !res.halted);
    }

    // ── encrypted_content_ranges ──────────────────────────────────────────
    // The content map is the UNION of every title's extents, merged into a
    // disjoint set. Fixture has OVERLAP, ADJACENT, and DISJOINT, out of order.
    #[test]
    fn encrypted_content_ranges_unions_sorts_and_merges_every_titles_extents() {
        let mut disc = make_test_disc(200_000, "BD");
        let mut feature = DiscTitle::empty();
        feature.extents = vec![ext(1_000, 100), ext(5_000, 50)];
        let mut play_all = DiscTitle::empty();
        // [1050,1250) overlaps the feature's [1000,1100); [1250,1260) is
        // exactly adjacent to it.
        play_all.extents = vec![ext(1_050, 200), ext(1_250, 10)];
        disc.titles = vec![play_all, feature];

        assert_eq!(
            disc.encrypted_content_ranges(),
            vec![(1_000, 260), (5_000, 50)],
            "the encrypted-content map is the merged, disjoint union of every title's extents"
        );

        // No parsed titles => no content gate at all (callers fall back).
        let unscanned = make_test_disc(200_000, "BD");
        assert!(
            unscanned.encrypted_content_ranges().is_empty(),
            "a disc with no titles declares no encrypted content"
        );
    }

    // ── aacs_disc_hash ────────────────────────────────────────────────────
    // Names the disc in Error::NoDiscKey: the disc's own SHA-1 of
    // Unit_Key_RO.inf, stripped to bare 40-hex. No AACS state reports nothing.
    #[test]
    fn aacs_disc_hash_is_the_captured_hash_without_its_0x_prefix() {
        const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";
        let mut disc = make_test_disc(1_000, "UHD");
        assert!(
            disc.aacs_disc_hash().is_empty(),
            "no AACS state => no disc to name"
        );
        disc.aacs = Some(AacsState {
            disc_hash: format!("0x{SHA1}"),
            ..aacs_empty()
        });
        assert_eq!(disc.aacs_disc_hash(), SHA1);
        // Already bare (no prefix) passes through unchanged, never re-stripped.
        disc.aacs = Some(AacsState {
            disc_hash: SHA1.to_string(),
            ..aacs_empty()
        });
        assert_eq!(disc.aacs_disc_hash(), SHA1);
    }

    // ── mapfile paths ─────────────────────────────────────────────────────

    /// The mapfile sits BESIDE the output as `<output>.mapfile`: the suffix is
    /// appended to the whole path, never substituted for the extension (which
    /// would make `movie.iso` and `movie.mkv` share one mapfile).
    #[test]
    fn mapfile_path_for_appends_the_suffix_to_the_whole_output_path() {
        assert_eq!(
            mapfile_path_for(std::path::Path::new("/tmp/rip/movie.iso")),
            std::path::PathBuf::from("/tmp/rip/movie.iso.mapfile")
        );
        assert_eq!(
            mapfile_path_for(std::path::Path::new("/tmp/rip/movie")),
            std::path::PathBuf::from("/tmp/rip/movie.mapfile")
        );
    }

    /// Regular output: `Disc::mapfile_for` is the plain `<path>.mapfile` rule.
    #[test]
    fn mapfile_for_regular_output_is_the_output_path_plus_suffix() {
        let disc = make_test_disc(1_000, "SOME_DISC");
        assert_eq!(
            disc.mapfile_for(std::path::Path::new("/tmp/rip/movie.iso")),
            std::path::PathBuf::from("/tmp/rip/movie.iso.mapfile")
        );
    }

    // /dev/null output cannot host a sibling mapfile, so it's named from the
    // disc and placed in the temp dir, sanitized to [A-Za-z0-9-_] since it's
    // used verbatim as a filename.
    #[test]
    fn mapfile_for_dev_null_sanitizes_the_disc_name_into_a_temp_path() {
        let mut disc = make_test_disc(1_000, "VOLUME_ID");
        // Keeps: alphanumeric, '-', '_'. Replaces: space, '!', non-ASCII.
        disc.meta_title = Some("A-B_c1 d!é".into());
        assert_eq!(
            disc.mapfile_for(std::path::Path::new("/dev/null")),
            std::env::temp_dir().join("A-B_c1_d__.mapfile")
        );
        // The UDF volume id is the fallback when the disc carries no META/DL
        // title.
        disc.meta_title = None;
        assert_eq!(
            disc.mapfile_for(std::path::Path::new("/dev/null")),
            std::env::temp_dir().join("VOLUME_ID.mapfile")
        );
    }

    // A drive answering READ CAPACITY(10) GOOD with an empty data phase
    // leaves the buffer zero, decoding blind to a bogus "1 sector" disc.
    // read_capacity must reject it.
    #[test]
    fn read_capacity_rejects_a_short_transfer_instead_of_reporting_one_sector() {
        use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};

        /// GOOD status, no sense, and *nothing written* to `buf`.
        struct EmptyDataPhase;
        impl ScsiTransport for EmptyDataPhase {
            fn execute(
                &mut self,
                _cdb: &[u8],
                _dir: DataDirection,
                _buf: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                Ok(ScsiResult {
                    status: 0,
                    sense: [0u8; 32],
                    bytes_transferred: 0,
                })
            }
        }

        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(EmptyDataPhase));
        assert!(matches!(
            Disc::read_capacity(&mut drive),
            Err(crate::error::Error::DiscCapacityMalformed)
        ));
    }
}
