//! Disc structure -- scan titles, streams, and sector ranges from a Blu-ray disc.
//!
//! This is the high-level API for disc content. The CLI calls this,
//! never parses MPLS/CLPI/UDF directly.
//!
//! Usage:
//!   let disc = Disc::scan(&mut drive, &ScanOptions::default())?;
//!   for title in &disc.titles { ... }
//!   for stream in &title.streams { ... }

mod bluray;
mod dvd;
pub(crate) mod dvd_audio_probe;
pub(crate) mod dvd_forced_probe;
mod encrypt;
pub(crate) use encrypt::handshake_class_error;
mod extract;
mod hddvd;
pub(crate) mod pgs_forced_probe;
pub mod profile;
#[cfg(test)]
pub(crate) mod scan_order_tests;

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
    /// Titles in main-feature order: `titles[0]` is the selected main feature (nav or
    /// authoring pick first, else largest physical size), not necessarily the longest
    pub titles: Vec<DiscTitle>,
    /// Disc region: a DVD's VMG region mask, [`DiscRegion::Free`] for UHD (region-free by
    /// spec), and [`DiscRegion::Unknown`] for Blu-ray and HD DVD, whose region check is
    /// the disc's own program code rather than a static field.
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
    /// MPEG-2 Program Stream: HD-DVD (`.evo`) and plain program-stream files. For AACS
    /// content this selects the PS-aware encrypted-flag / structural checks.
    MpegPs,
    /// The program stream of a DVD-Video title (`.vob`), set only by the DVD scan: the same
    /// container as [`Self::MpegPs`], plus what is DVD's alone (CSS, DVD navigation packs,
    /// extents that are IFO cells). No other source gets the DVD-only stages.
    DvdPs,
}

impl ContentFormat {
    /// Whether the sectors hold an MPEG-2 program stream (DVD or not).
    pub fn is_program_stream(self) -> bool {
        matches!(self, Self::MpegPs | Self::DvdPs)
    }
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
    /// DVD regions the disc plays in (1-8 or combination); empty when the disc's
    /// region mask prohibits every region.
    Dvd(Vec<u8>),
    /// Not recorded anywhere a scan can read: a Blu-ray or HD DVD enforces region in
    /// its own navigation program (BD checks player register PSR20), not a static field.
    Unknown,
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
    /// Chapter name — the disc's own name when it carries one (DVD text
    /// data), else a bare 1-based index ("1", "2", …). The library emits
    /// no localized prose; apps prepend any "Chapter " prefix to ordinals.
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

// THE ONE definition of "structurally AACS-encrypted", shared by identify and the full scan.
// Root `AACS/` or the HD DVD dir the key reads use; never `/BDMV/AACS`, which no key reader
// (nor libaacs) reads and no disc has.
pub(crate) fn aacs_dir_present(udf_fs: &crate::udf::UdfFs) -> bool {
    udf_fs.find_dir("/AACS").is_some() || crate::aacs::find_hddvd_aacs_dir(udf_fs).is_some()
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

// Test-only: corrects a title's TrueHD channels/sample-rate/Atmos by probing the first decrypted
// major sync. The production path is `correct_truehd_stream`, driven from mux/driver.rs.
#[cfg(test)]
pub(crate) fn correct_truehd_channels(reader: &mut dyn SectorSource, title: &mut DiscTitle) {
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
        correct_truehd_stream(a, payload);
    }
}

/// Correct one TrueHD track from its stream bytes (the playlist understates 7.1/Atmos as
/// 5.1): channels, sample rate and label from the first major sync in `payload`. Returns
/// `false` when `payload` holds no major sync yet.
pub(crate) fn correct_truehd_stream(a: &mut AudioStream, payload: &[u8]) -> bool {
    use crate::mux::codec::truehd::{
        truehd_channels, truehd_lfe, truehd_sample_rate_hz, truehd_sync_info_from_stream,
    };
    // One major-sync read yields channels, sample rate and the Atmos signal.
    let Some(info) = truehd_sync_info_from_stream(payload) else {
        return false;
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
    true
}

/// Calculate how many bytes of bad/unreadable data fall within a title's extents.
/// Public so autorip can use it for main-movie lost_ms computation.
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
            6 => FrameRate::F50,
            7 => FrameRate::F59_94,
            // 5 and 8 are unassigned (libbluray, MediaInfo, tsMuxer); a UHD menu clip
            // carries 8, which is not a statement of 60 fps.
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
            // 12 is the stereo + multichannel combo; the layout is not knowable here.
            12 => AudioChannels::Unknown,
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
/// out-of-band through [`KeyRing::acquire`](crate::keys::KeyRing::acquire). The
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
    /// `KeyRing::resolve`).
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
    /// The caller copies sectors raw (a raw disc→ISO copy). No effect on the scan since
    /// an unreadable `Unit_Key_RO.inf` never fails it; kept for source compatibility.
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

// Whether a live scan parses the file at `path` (absolute, any case), so the metadata
// prefetch bulk-loads it. On a BD tree only what the scan reads: the nav and clip files,
// BD-J objects, jar archives and the config files the labels read beside them, META XML,
// and the AACS files the capture reads first. Not /BDMV/BACKUP, BD-J image assets,
// /AACS/DUPLICATE or the other AACS files: a fallback read of one still works, unprefetched.
// Any other tree (DVD, HD DVD) keeps every file.
pub(crate) fn scan_parses(bd: bool, path: &str) -> bool {
    if !bd {
        return true;
    }
    let p = path.to_ascii_uppercase();
    let in_dir = |dir: &str| {
        p.strip_prefix(dir)
            .and_then(|rest| rest.strip_prefix('/'))
            .filter(|name| !name.contains('/'))
    };
    if let Some(name) = in_dir("/BDMV") {
        return matches!(name, "INDEX.BDMV" | "MOVIEOBJECT.BDMV");
    }
    if ["/BDMV/PLAYLIST", "/BDMV/CLIPINF", "/BDMV/BDJO"]
        .iter()
        .any(|d| in_dir(d).is_some())
    {
        return true;
    }
    if p.starts_with("/BDMV/META/") {
        return p.ends_with(".XML");
    }
    if let Some(name) = in_dir("/BDMV/JAR") {
        return name.ends_with(".JAR");
    }
    if let Some(rest) = p.strip_prefix("/BDMV/JAR/") {
        // A jar subdirectory's own files: only those a label reads.
        return rest.split_once('/').is_some_and(|(_, name)| {
            !name.contains('/')
                && crate::labels::JAR_DIR_FILES
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(name))
        });
    }
    [
        crate::aacs::PATH_UNIT_KEY_RO,
        crate::aacs::PATH_CONTENT_CERT,
        crate::aacs::PATH_CONTENT_CERT_ALT,
        crate::aacs::PATH_MKB_RO,
    ]
    .iter()
    .any(|f| f.eq_ignore_ascii_case(path))
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
        let encrypted = aacs_dir_present(&udf_fs)
            && (format != DiscFormat::HdDvd
                || hddvd::hddvd_content_scrambled(&mut buffered, &udf_fs) != Some(false));
        let layers = Self::layers_for(format, capacity);

        Ok(DiscId {
            volume_id: udf_fs.volume_id,
            meta_title,
            format,
            capacity_sectors: capacity,
            encrypted,
            layers,
        })
    }

    // Layer count inferred from capacity against the format's per-layer size.
    fn layers_for(format: DiscFormat, capacity: u32) -> u8 {
        // Upper sector bounds of (1, 2) layers; above the second is 3 (BD-100, HD DVD TL).
        let (one, two) = match format {
            DiscFormat::Dvd => (2_400_000, u32::MAX),
            DiscFormat::HdDvd => (8_000_000, 16_000_000),
            DiscFormat::BluRay | DiscFormat::Uhd | DiscFormat::Fmts => (12_500_000, 40_000_000),
            DiscFormat::Unknown => (12_500_000, u32::MAX),
        };
        if capacity <= one {
            1
        } else if capacity <= two {
            2
        } else {
            3
        }
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
    /// A live AACS disc whose `Unit_Key_RO.inf` cannot be read is warned and scanned on,
    /// with [`Error::AacsKeyFileUnreadable`] recorded and every key refused. A dead bus
    /// during a handshake aborts the scan.
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
        session.set_speed(Drive::SPEED_MAX_KBPS);
        // CSS bus-auth before any read, else the UDF prefetch hits scrambled VOB extents.
        if dvd {
            Self::css_bus_step(session)?;
        }

        tracing::info!(target: "freemkv::scan", "phase: reading UDF filesystem");
        let (capacity, mut buffered, udf_fs) = Self::read_udf(session)?;
        tracing::info!(target: "freemkv::scan", capacity, "phase: UDF read");
        // Pre-read the small files the scan parses (AACS, MPLS, CLPI, META, *.bdmv): one
        // command each otherwise.
        let bd = udf_fs.find_dir("/BDMV").is_some();
        match udf_fs.metadata_sector_ranges_for(&mut buffered, &|p| scan_parses(bd, p)) {
            Ok(ranges) => buffered.prefetch_ranges(&ranges)?,
            Err(Error::Halted) => return Err(Error::Halted),
            Err(_) => {} // prefetch is optional
        }

        let aacs = if aacs_dir_present(&udf_fs) {
            // A DVD with /AACS gets no handshake, so its key file keeps the image rule.
            let from = if dvd {
                encrypt::CaptureFrom::Image
            } else {
                encrypt::CaptureFrom::Live
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
        let version = Self::read_aacs_version(reader, udf_fs, &mkb)?;
        Ok((inf, mkb, version))
    }

    // AACS major version driving the Unit_Key_RO.inf parse stride: the content certificate,
    // then the MKB type, then index.bdmv, through the shared `resolve_aacs_version`. `Err` only
    // for a Stop.
    fn read_aacs_version(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        mkb: &[u8],
    ) -> Result<u8> {
        let cert = crate::aacs::read_first(
            &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::ContentCert),
            |p| udf_fs.read_file(reader, p),
        );
        if matches!(cert, Err(Error::Halted)) {
            return Err(Error::Halted);
        }
        let cert_major = cert
            .ok()
            .as_deref()
            .and_then(crate::aacs::inf::parse_content_cert)
            .map(|c| c.version.major());
        if cert_major.is_none() {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "scan_aacs_version",
                "no readable AACS content certificate; Unit_Key_RO stride from the MKB type \
                 or index.bdmv"
            );
        }
        let index = encrypt::index_is_uhd(udf_fs, reader)?;
        Ok(crate::aacs::mkb::resolve_aacs_version(cert_major, mkb, index).major())
    }

    // Reads the AACS MKB's real record stream — NOT its ~128 MiB zero padding. Reads a bounded
    // prefix (4 MiB holds a real MKB, ~4 MB on a UHD) and walks the record headers: a stream
    // the prefix cuts is re-read with room for the record it cuts, never returned short. Also
    // avoids the read_file MAX_FILE_BYTES cap.
    fn read_mkb_content(reader: &mut dyn SectorSource, udf_fs: &udf::UdfFs) -> Result<Vec<u8>> {
        const START_BYTES: usize = 4 * 1024 * 1024;
        const MAX_BYTES: usize = 64 * 1024 * 1024;
        let mut want = START_BYTES;
        loop {
            let buf = crate::aacs::read_first(
                &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::Mkb),
                |p| udf_fs.read_file_prefix(reader, p, want),
            )?;
            // The stream ends inside `buf`, or `buf` is the whole file, or the cap is
            // reached: done. An unparseable first record (end 0) grows as before.
            let need = match crate::aacs::mkb::mkb_prefix_end(&buf) {
                Ok(n) if n > 0 => None,
                Ok(_) => Some(0),
                Err(need) => Some(need),
            };
            let Some(need) = need.filter(|_| buf.len() >= want && want < MAX_BYTES) else {
                // The prefix buffer is sized for the padded file; release the unused capacity.
                let mut mkb = crate::aacs::mkb::trim_mkb(buf);
                mkb.shrink_to_fit();
                return Ok(mkb);
            };
            want = (want * 2).max(need).min(MAX_BYTES);
        }
    }

    /// Read a disc's AACS key-input files from an ISO image: returns
    /// `(Unit_Key_RO.inf, MKB, aacs_major_version)`. For callers that resolve a
    /// Unit Key out-of-band: the key reaches a read only through
    /// [`KeyRing`](crate::keys::KeyRing). libfreemkv never makes a network call.
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
                Err(e) => {
                    tracing::warn!(target: "freemkv::scan", file = %rel, code = e.code(), "structure file unreadable; skipped");
                }
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
    /// [`KeyRing`](crate::keys::KeyRing). These files are plaintext UDF metadata — no
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
        let mut encrypted = aacs.is_some();
        // Lookup-free: the state carries the disc's AACS inputs but no key; the caller
        // resolves one into a `KeyRing`.
        let (mut aacs, mut aacs_error) = match aacs {
            Some((cap, bus)) => encrypt::resolve_aacs(cap, &bus),
            None => (None, None),
        };

        // 3. Titles + container — dispatched by on-disc tree (HD-DVD/DVD peers,
        // FMTS shares the BD tree; FORMAT is a separate axis derived below). DVD
        // resolves its main feature via First-Play nav (issue #40) as `nav_feature`.
        let mut dvd_nav_feature: Option<u16> = None;
        let mut dvd_region: Option<DiscRegion> = None;
        let (mut titles, content_format) = if udf_fs.find_dir("/BDMV").is_some() {
            (
                Self::scan_bluray_titles(reader, &udf_fs, halt)?,
                ContentFormat::BdTs,
            )
        } else if udf_fs.find_dir("/HVDVD_TS").is_some() {
            // An AACS directory over EVOs whose packs are all unscrambled is a decrypted rip.
            if encrypted && hddvd::hddvd_content_scrambled(reader, &udf_fs) == Some(false) {
                tracing::info!(
                    target: "freemkv::scan",
                    "HD DVD carries an AACS directory but its EVO packs are clear: not encrypted"
                );
                (encrypted, aacs, aacs_error) = (false, None, None);
            }
            (
                Self::scan_hddvd_titles(reader, &udf_fs, halt)?,
                ContentFormat::MpegPs,
            )
        } else if udf_fs.find_dir("/VIDEO_TS").is_some() {
            let (dvd_titles, nav, region) = Self::scan_dvd_titles(reader, &udf_fs, halt)?;
            dvd_nav_feature = nav;
            dvd_region = Some(region);
            (dvd_titles, ContentFormat::DvdPs)
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
            Self::probe_forced_subtitles_for_dvd_titles(reader, &mut titles, halt);
            // The probes swallow a Stop; surface it as the live scan does.
            if let Some(h) = halt {
                h.check()?;
            }
        }
        crate::labels::fill_defaults(&mut titles);

        // 5. Format (AACS MKB generation → BD/UHD/FMTS; tree → HD-DVD/DVD), layers and
        //    region: the DVD's VMG mask, region-free UHD, otherwise not statically recorded.
        let format = Self::detect_disc_format(reader, &udf_fs, &titles);
        let layers = Self::layers_for(format, capacity);
        let region = match (dvd_region, format) {
            (Some(region), _) => region,
            (None, DiscFormat::Uhd | DiscFormat::Fmts) => DiscRegion::Free,
            (None, _) => DiscRegion::Unknown,
        };

        // 6. CSS: `scan_image` cracks the title key after this returns and `scan_live` only
        // runs bus-auth. The reader-based crack is NOT run here: on a CSS disc it would
        // fail ~50,000 sectors one-by-one.
        let css = None;

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

    // Content-based forced-subtitle detection for DVD VobSub titles (FSTA_DSP), the DVD
    // counterpart of the PGS probe above; it only adds `forced`, never clears the IFO's.
    fn probe_forced_subtitles_for_dvd_titles(
        reader: &mut dyn SectorSource,
        titles: &mut [DiscTitle],
        halt: Option<&crate::halt::Halt>,
    ) {
        let mut cache = dvd_forced_probe::DvdForcedProbeCache::default();
        for title in titles.iter_mut() {
            if title.content_format == ContentFormat::DvdPs {
                dvd_forced_probe::probe_and_set_forced(reader, title, &mut cache, halt);
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
            let mkb_paths = crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::Mkb);
            match crate::aacs::read_first(&mkb_paths, |p| udf_fs.read_file_prefix(reader, p, 64)) {
                Ok(mkb) => match mkb_type(&mkb).map(|t| t.generation()) {
                    Some(AacsVersion::V21) => return DiscFormat::Fmts,
                    Some(AacsVersion::V20) => return DiscFormat::Uhd,
                    Some(AacsVersion::V10) => return DiscFormat::BluRay,
                    None => {}
                },
                // No MKB at all is an unencrypted disc, not a fault.
                Err(Error::AacsNoKeys) => {}
                Err(e) => tracing::warn!(
                    target: "freemkv::scan",
                    error_code = e.code(),
                    "MKB prefix unreadable; classifying the disc by resolution"
                ),
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
        session.read_capacity()
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
    // Matched on the stem with trailing spaces ignored.
    let stem = name.split('.').next().unwrap_or(name);
    !crate::io::tree_sink::is_windows_reserved(stem)
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
    /// [`KeyRing`](crate::keys::KeyRing) (KU §2.2), so an AACS disc is `None`.
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

// Disc-authored names can be arbitrarily long; the sanitised name stays well under NAME_MAX.
const NULL_MAPFILE_NAME_MAX_CHARS: usize = 128;

impl Disc {
    /// Path to the mapfile for a given output path.
    ///
    /// For the null device ([`crate::io::null_device`]), returns `{dir}/{volume_id_or_title}.mapfile`
    /// where `{dir}` is a directory this process created under the temp dir
    /// (owner-only on Unix, unpredictable name, stable for the process). For
    /// regular files, returns `{path}.mapfile`. The null-device directory and the mapfile in it
    /// outlive the process; removing them is the caller's job.
    pub fn mapfile_for(&self, path: &std::path::Path) -> std::path::PathBuf {
        if crate::io::is_null_device(path) {
            let name: String = self
                .meta_title
                .as_deref()
                .unwrap_or(&self.volume_id)
                .chars()
                .take(NULL_MAPFILE_NAME_MAX_CHARS)
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            null_mapfile_dir().join(format!("{name}.mapfile"))
        } else {
            mapfile_path_for(path)
        }
    }
}

// Private per-process dir for `/dev/null` mapfiles. Created fresh (never
// reused) so another user cannot pre-create, plant or symlink the path.
fn null_mapfile_dir() -> std::path::PathBuf {
    static DIR: std::sync::Mutex<Option<std::path::PathBuf>> = std::sync::Mutex::new(None);
    let mut dir = DIR
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(d) = dir.as_ref() {
        return d.clone();
    }
    let mut last = std::path::PathBuf::new();
    for _ in 0..8 {
        last = std::env::temp_dir().join(format!(
            "freemkv-{}-{:016x}",
            std::process::id(),
            random_u64()
        ));
        match create_private_dir(&last) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            _ => break,
        }
    }
    // Remembered even if not created, so every call agrees; a mapfile
    // under a dir that failed to create then fails to open.
    *dir = Some(last.clone());
    last
}

fn create_private_dir(path: &std::path::Path) -> std::io::Result<()> {
    let mut b = std::fs::DirBuilder::new();
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
    b.create(path)
}

// OS-seeded, per-call random value from std's SipHash keys.
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish()
}

const MAX_BATCH_SECTORS: u16 = 510;
pub(crate) const DEFAULT_BATCH_SECTORS_OPTICAL: u16 = 60;
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

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
