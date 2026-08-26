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
mod extract;
mod hddvd;
pub(crate) mod pgs_forced_probe;
pub mod profile;

use crate::drive::Drive;
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf;

use encrypt::HandshakeResult;

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
    /// scrambled sectors but could NOT recover a title key (the
    /// known-plaintext attack found no crackable crib, or the scrambled
    /// region was unreadable). `css` is `None` in that case — but the disc is
    /// genuinely encrypted, so callers MUST surface this hard error rather
    /// than treat `css.is_none()` as "unencrypted" and mux scrambled MPEG as
    /// plaintext garbage. `None` when no scrambled sector was seen (genuinely
    /// unencrypted) or a key was recovered (`css.is_some()`). The CSS analogue
    /// of [`Self::aacs_error`].
    ///
    /// This records the MAIN feature's crack, so it is a WHOLE-DISC signal: the
    /// gates convert it into [`crate::error::Error::CssNoDiscKey`] (disc-level,
    /// `error::is_disc_level_no_key`), not the per-title
    /// [`crate::error::Error::CssKeyMissing`] the field itself carries as its
    /// recorded reason.
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
    /// `None` when it could not be determined.
    ///
    /// This is PROVENANCE, and it is what makes clip assignment a lookup
    /// instead of a guess. During an overlap two clips' mark ranges both
    /// contain the same timestamp, so no rule over timestamps alone can say
    /// which clip a frame came from — four audit rounds each fixed one such
    /// rule and broke another. A frame carries its source byte offset
    /// (`PesFrame::source`), and that offset falls in exactly one clip's span.
    pub feed_span: Option<(u64, u64)>,
}

/// Per-title classification for [`Disc::main_feature_order`]. Both fields are
/// GLOBAL properties (they depend on the whole title list, not on a pairwise
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    pub start_lba: u32,
    pub sector_count: u32,
}

/// Union a set of extents into sorted, merged, disjoint `(start_lba,
/// sector_count)` ranges — the pure, testable core of
/// [`Disc::encrypted_content_ranges`]. Reuses [`crate::udf::merge_ranges`].
/// Is this disc structurally AACS-encrypted — i.e. does it carry an AACS
/// directory?
///
/// THE ONE definition of that question. `Disc::identify` (the fast path, which
/// only reads the filesystem) and `Disc::scan_with` (the full scan) both need
/// it, and both used to spell the same two `find_dir` calls out by hand. They
/// agreed today; nothing made them agree tomorrow. Adding a third AACS
/// location, or excluding an empty placeholder directory, to one copy and not
/// the other would silently desync the fast identify from the full scan — the
/// same disc reported encrypted by one and clear by the other.
///
/// This is STRUCTURAL, not cryptographic: it says the tree looks like an
/// encrypted disc, not that any sector actually is. A folder copied verbatim
/// from a decrypted disc keeps its `AACS/` and answers true here (see
/// `session::scan_dir`, which corrects for exactly that).
pub(crate) fn aacs_dir_present(udf_fs: &crate::udf::UdfFs) -> bool {
    udf_fs.find_dir("/AACS").is_some() || udf_fs.find_dir("/BDMV/AACS").is_some()
}

fn merged_extents<'a>(extents: impl Iterator<Item = &'a Extent>) -> Vec<(u32, u32)> {
    let mut ranges: Vec<(u32, u32)> = extents.map(|e| (e.start_lba, e.sector_count)).collect();
    ranges.sort_by_key(|r| r.0);
    crate::udf::merge_ranges(&ranges)
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

/// Merge per-title AACS key ranges into the sorted, disjoint set the whole-disc map
/// needs ([`crate::decrypt::AacsKeyMap::entry_for`] requires disjoint ranges).
/// Titles that share a clip resolve the SAME physical span (same LBAs → same CPS
/// unit → same key). When a later range overlaps a kept one that carries the SAME
/// key index and phase, the two are UNIONED (end extended to the max) — this covers
/// both the exact-duplicate (shared clip) case and any partial overlap without ever
/// dropping coverage, so no encrypted LBA is left in no range (which would pass
/// through as ciphertext). A real disc never produces two DIFFERENT keys for one
/// LBA; if that malformed case ever appeared, the later range is dropped to keep the
/// set disjoint rather than extend one key over another key's LBAs.
fn merge_content_key_ranges(
    mut ranges: Vec<(u32, u32, usize, crate::decrypt::Phase)>,
) -> Vec<(u32, u32, usize, crate::decrypt::Phase)> {
    ranges.sort_by_key(|&(s, _, _, _)| s);
    let mut merged: Vec<(u32, u32, usize, crate::decrypt::Phase)> = Vec::new();
    for r in ranges {
        match merged.last_mut() {
            // Overlaps the previous kept range.
            Some(last) if r.0 < last.1 => {
                // Same key + phase → union (coverage-preserving); a genuine
                // different-key overlap (malformed disc) is dropped to stay disjoint.
                if r.2 == last.2 && r.3 == last.3 {
                    last.1 = last.1.max(r.1);
                }
            }
            // Disjoint or exactly adjacent → keep as its own range.
            _ => merged.push(r),
        }
    }
    merged
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

/// Byte offset of `lba` within `title`'s extents (concatenated in order), or
/// `None` if the LBA falls outside every extent. The title is a virtual
/// contiguous stream; this maps a disc LBA into that stream so a chapter/time
/// lookup can place it. (Moved from autorip — clients must not re-derive it.)
fn byte_offset_in_title(lba: u32, title: &DiscTitle) -> Option<u64> {
    use crate::consts::SECTOR_BYTES_U64;
    let mut cumulative = 0u64;
    for ext in &title.extents {
        // Saturating, like every other extent-end computation in the crate
        // (`bytes_bad_in_title`, `crack_key_scan`'s crack span,
        // `DiscStream::fill_extents`). ECMA-167 logical block numbers are
        // 32-bit, so a malformed UDF/IFO extent near the top of that space
        // makes a plain `+` overflow — a debug-build PANIC inside a library,
        // and a release-build wrap to a tiny end LBA that silently reports the
        // offset as outside the title.
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
/// `raw` is the mapfile's `(byte_pos, byte_len)` set for whichever statuses the
/// caller cares about (the live "Maybe" set during a patch, or terminal
/// `Unreadable` for the verdict). The list is sorted largest-movie-time first
/// and capped at 50; `truncated` reports the overflow. `bps` (title bytes/sec)
/// is derived from the title so callers don't thread it.
///
/// This is the single place range→chapter/time annotation happens; autorip used
/// to own it and read the mapfile to do so. Now the library computes it from
/// its in-memory mapfile + title, and the client renders the result verbatim.
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
            // eye) are both H.264. Mapping 0x20 to video is what makes the PMT
            // scan enumerate the dependent view as a second H.264 stream (its
            // own PID in the SSIF) instead of dropping it — the basis of 3D.
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
            // 0xA2 is the SECONDARY DTS-HD audio stream (Blu-ray Disc
            // Read-Only Format part 3, stream_coding_type table): DTS Express /
            // DTS-HD LBR, a LOSSY low-bitrate extension carried alongside the
            // primary track for picture-in-picture and BD-J mixing — exactly
            // parallel to 0xA1 (secondary E-AC-3) on the Dolby side. It is NOT
            // DTS-HD Master Audio, which has its own primary code 0x86; mapping
            // it there advertised a lossless track for lossy content, so the
            // muxer's stream metadata and every label derived from it claimed a
            // quality the bitstream does not carry. The crate has no distinct
            // DTS Express variant, so it is represented by the LOSSY DTS-HD
            // member.
            c::DTS_HD_SECONDARY => Codec::DtsHdHr,
            // PG (0x90) = Presentation Graphics (subtitles). IG (0x91, menus)
            // and TEXT_SUBTITLE (0x92) are distinct HDMV coding types and are
            // NOT PG subtitle streams; only PG maps to Pgs. IG falls through to
            // Unknown so the PMT/STN walker drops it rather than surfacing a
            // bogus PGS subtitle track for a menu ES.
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

    /// Pixel dimensions (width, height). `(0, 0)` when the resolution is
    /// [`Resolution::Unknown`] — "no dimensions", not a guess.
    ///
    /// The SD frames are the DVD-Video coded pictures (ITU-R BT.601 525/60 and
    /// 625/50 active area); the HD frames are the Blu-ray Disc Read-Only Format
    /// part 3 video formats; 3840x2160 is the UHD BD frame.
    ///
    /// `Unknown` deliberately does NOT fabricate a plausible 1920x1080. This is
    /// the same trap already closed on [`AudioChannels::count`] (which used to
    /// return 6) and [`SampleRate::hz`] (48000.0): a plausible wrong answer is
    /// indistinguishable from a real one at every call site, so it makes each
    /// caller responsible for remembering to check the variant first — and the
    /// `json://` sink had already walked into exactly that, reporting confident
    /// dimensions for a stream whose neighbouring `resolution` field said
    /// "unknown". A zero is unmistakable, and every caller in tree already
    /// handles it: the Matroska sink omits the optional PixelWidth/PixelHeight,
    /// the VobSub `.idx` writer omits its `size:` line, and no caller divides by
    /// either dimension.
    /// `None` when the resolution never resolved.
    ///
    /// Returns an `Option` rather than a sentinel because every caller has to
    /// make a DIFFERENT decision and the compiler is the only thing that
    /// reliably makes them: Matroska omits the optional PixelWidth/PixelHeight
    /// elements, the metadata sinks report the dimensions as absent, and MP4
    /// cannot do either — ISO/IEC 14496-12 makes width and height MANDATORY in
    /// both `tkhd` (8.3.2) and VisualSampleEntry (12.1.3), so it must refuse.
    ///
    /// This returned `(0, 0)` before, and that is the shape the sentinel
    /// creates: `(0, 0)` looks like a usable pair, so the MP4 sink stored it
    /// and serialised a 0x0 video track — a structurally complete file that no
    /// player can render, written with no error. It returned a fabricated
    /// 1920x1080 before that, which was wrong but at least playable, so the
    /// sink had never needed a guard and the absence of one was invisible.
    ///
    /// A doc comment listing which callers are safe cannot hold this: one did
    /// not check, and a third kept its own duplicate `Unknown` test long after
    /// the accessor took the job over.
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
            AudioChannels::Stereo21 => 3,
            AudioChannels::Quad => 4,
            AudioChannels::Surround50 => 5,
            AudioChannels::Surround51 => 6,
            AudioChannels::Surround61 => 7,
            AudioChannels::Surround71 => 8,
            // 0, not 6. An unknown layout has no channel count, and returning a
            // plausible one made every caller responsible for remembering to
            // check the variant first — a trap, and one this crate walked into:
            // the json:// sink reported a confident 5.1 for audio its own
            // neighbouring fields called "unknown". 0 is the value Matroska and
            // the sinks already coerce Unknown to, and unlike 6 it is obviously
            // wrong if it ever reaches output.
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
    /// (at least 2 GB and 5 minutes).
    ///
    /// The exposed `streams` come from the title's FIRST PlayItem only, so a
    /// feature whose first PlayItem is a non-video bumper reports
    /// `has_video() == false` and would be wrongly disqualified (audit R6).
    /// This predicate is deliberately MORE permissive than [`Self::has_video`]
    /// — it only ever ADMITS a title the stricter gate would reject — so it
    /// cannot reopen the decoy hole (a size-inflated streamless decoy is demoted
    /// STRUCTURALLY by composite detection in [`Disc::rank_titles`], not by this
    /// gate), while it still rejects tiny streamless menu/metadata playlists.
    /// It is the `has-video` key used by [`Disc::main_feature_order`]. The size
    /// fallback requires at least one clip, so a clip-less pure-metadata decoy
    /// (which cannot be a real bumper-lead feature — that has the feature clip)
    /// is still rejected.
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

// Redacting `Debug`: `AacsState` is crate-root re-exported and reachable via the
// public `Disc.aacs` field; it carries VUK / unit keys / read-data (bus) key /
// volume id / raw .inf + MKB. Print only non-secret shape; redact every
// key/secret field. Guarded by `aacs_state_and_key_debug_are_redacted`.
impl std::fmt::Debug for AacsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AacsState")
            .field("version", &self.version)
            .field("bus_encryption", &self.bus_encryption)
            .field("mkb_version", &self.mkb_version)
            .field("disc_hash", &self.disc_hash)
            .field("key_source", &self.key_source)
            .field("vuk", &self.vuk.map(|_| "<redacted>"))
            .field("unit_keys_len", &self.unit_keys.len())
            .field("read_data_key", &self.read_data_key.map(|_| "<redacted>"))
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

// No `KeyOrigin::name()`: the library holds ZERO user-facing English (CLAUDE.md).
// `KeyOrigin` is a typed enum; applications map its variants to display text
// (see freemkv's `disc_info::key_origin_label`).

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
    /// Read the PGS subtitle streams during the scan to detect forced-narrative
    /// tracks from their content (the `forced_on_flag`), matching what the mux
    /// derives during a rip. OFF by default — it reads the clip's PGS content,
    /// which is slow, so only callers that want authoritative forced flags in the
    /// scanned title (e.g. `freemkv info`) opt in. The rip path leaves it off:
    /// the muxer detects forced during muxing without a second read.
    pub probe_forced_subtitles: bool,
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
        // (BD/UHD speed is set by drive unlock/init, but DVD needs explicit SET CD SPEED)
        session.set_speed(0xFFFF);

        // CSS bus-auth unlock — run BEFORE any scrambled-sector read.
        // On a CSS-enforcing drive (e.g. the BU40N) the UDF metadata prefetch
        // below reaches small/menu VOB extents that are themselves CSS-scrambled;
        // without the bus-auth handshake first, each of those reads is rejected
        // with sense 05/6F/03 ("read of scrambled sector without authentication")
        // after a full drive round-trip — ~13s of pure waste on a real disc, and
        // the prefetch caches nothing. The handshake needs no title info (it takes
        // no extents), is self-guarding to DVD media, and is non-fatal on failure,
        // so run it as soon as the drive has classified the disc as a DVD. The
        // per-VTS title key is NOT recovered here (or anywhere in scan) — it is
        // cracked keylessly at read/mux time via `css::resolve_dvd_title_key`;
        // this only unlocks the drive's read gating so those reads can happen.
        if session.disc_is_dvd() {
            tracing::info!(target: "freemkv::scan", "phase: CSS — bus-auth unlock (pre-scan)");
            let drive_id = session.drive_id.clone();
            let (_, css_unlock_res) = crate::unlock_bridge::run_bus(
                session.scsi_mut(),
                &drive_id,
                freemkv_unlock::DiscKind::Css,
                &[],
            );
            if let Err(e) = css_unlock_res {
                tracing::warn!(
                    target: "freemkv::scan",
                    outcome = ?e,
                    "CSS bus-auth unlock did not apply; scrambled sectors may be unavailable"
                );
            }
        }

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
        let disc = Self::scan_with(
            &mut buffered,
            capacity,
            handshake,
            handshake_error,
            opts,
            udf_fs,
        )?;
        tracing::info!(target: "freemkv::scan", titles = disc.titles.len(), format = ?disc.content_format, "phase: titles parsed");

        // No CSS key recovery at scan time. DVD CSS keys are per-VTS/per-title,
        // and the descramble path re-cracks each title's key keylessly from that
        // title's own extents at the moment its sectors are read/decrypted — so a
        // single up-front "disc key" is meaningless (it's not valid for the other
        // titles and every title re-derives its key at read time anyway). The
        // scan's only CSS responsibility is the bus-auth unlock above, which opens
        // the drive's read gating so the sweep can read scrambled sectors at all.
        // Detection is likewise moot: encrypted or not, a DVD muxes identically —
        // the read-time descrambler cracks a title key if the sectors are
        // scrambled and is a no-op if they are clear.

        // `disc.css` is intentionally never set at scan time now (per-title CSS
        // keys are cracked at read/mux time), so log the format the scan actually
        // determined rather than a key state that is always `None` here.
        tracing::info!(target: "freemkv::scan", format = ?disc.format, titles = disc.titles.len(), "phase: scan complete");
        Ok(disc)
    }

    /// The extents an image-time CSS crack scans, in the crate's CANONICAL
    /// order: the main feature's own extents, in natural playback order.
    ///
    /// Both halves are deferred to logic the crate already owns rather than
    /// re-derived here:
    /// - WHICH title — `scan_with` has already sorted `titles` with
    ///   [`Self::canonical_title_order`], so the canonical main feature is the
    ///   first title that actually has extents. No local pick.
    /// - WHAT ORDER — playback order, i.e. the extent vector untouched, exactly
    ///   as [`Self::decrypt_keys_for_title`] hands `&title.extents` to
    ///   `css::crack_key_outcome`.
    ///
    /// **Why this function exists (do not let the copy grow back):**
    /// `Disc::scan_image` used to re-implement both halves inline — it picked
    /// the title with the largest total sector count, then re-sorted that
    /// title's extents LARGEST-CELL-FIRST before handing them to
    /// `crack_key_outcome`. Both had drifted from the canonical rules:
    ///
    /// - Largest-cell-first is the 1.5.1 garbage bug. A CSS DVD's biggest cell
    ///   opens with a long CLEAR run, and the crack's 50_000-sector budget is
    ///   shared across the whole extent list, so starting there can exhaust the
    ///   budget without ever reaching a scrambled sector — the crack reports
    ///   `Unencrypted` and the mux emits scrambled MPEG as plaintext. Playback
    ///   order reaches the scrambled feature body after only the small clear
    ///   front matter that precedes it on a real disc (CSS: the title key is
    ///   recovered from the scrambled data itself, so the scan must actually
    ///   MEET scrambled data).
    /// - The sector-count pick ignores `canonical_title_order`'s capacity gate,
    ///   so it selects the oversize "play-all" composite (whose declared cells
    ///   double-count data shared with other playlists) instead of the real
    ///   feature — i.e. a DIFFERENT title, and on a multi-VTS disc a different
    ///   VTS, whose CSS title key does not descramble the feature at all.
    ///
    /// The result: the same disc cracked as an ISO could disagree with the same
    /// disc cracked from the drive, which is precisely what the duplicate was
    /// free to do. Keep the derivation here, shared, so it cannot recur.
    fn image_crack_extents(titles: &[DiscTitle]) -> &[Extent] {
        // Prefer the first title that has BOTH video and extents — the main
        // feature after `main_feature_order` — so the CSS crack scans the movie,
        // not a streamless decoy that happens to carry extents. Falls back to the
        // first title with extents (unchanged behaviour) when none has video.
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
        let mut disc = Self::scan_with(reader, capacity, None, None, opts, udf_fs)?;

        // CSS for a raw (still-scrambled) DVD image: recover the title key from
        // the scrambled movie data itself (known-plaintext attack), same as the
        // live-drive path — but with no SCSI auth/unlock (an image is already
        // readable). This lets the CLI mux a RAW CSS ISO, not only a
        // pre-decrypted one. A pre-decrypted image has its scramble flags clear,
        // so `crack_key` finds no crackable sector and the disc stays in the
        // clear. AACS images go through KEYDB VUK lookup, not here.
        //
        // Gate on `DiscFormat::Dvd`, NOT `content_format == MpegPs`: HD-DVD
        // `.evo` images are ALSO MPEG-PS but are AACS, not CSS — they must not
        // enter the CSS crack path. A CSS DVD's IFO (which defines the titles
        // this branch reads) is unscrambled, so `detect_format` reliably sets
        // `Dvd` from the SD-resolution titles even on a still-scrambled image.
        if disc.css.is_none() && disc.format == DiscFormat::Dvd && !disc.titles.is_empty() {
            // Copied out so the crack's `disc.css` / `disc.encrypted` writes
            // below don't collide with a live borrow of `disc.titles`. The
            // ORDER is whatever `image_crack_extents` returns — never re-sorted
            // here (see that function's docs: the local re-sort was the defect).
            let main_extents = Self::image_crack_extents(&disc.titles).to_vec();
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
    ) -> Result<(Vec<u8>, Vec<u8>, u8)> {
        let inf = crate::aacs::read_first(
            &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::UnitKey),
            |p| udf_fs.read_file(reader, p),
        )?;
        let mkb = Self::read_mkb_content(reader, udf_fs)?;
        let version = Self::read_aacs_version(reader, udf_fs);
        Ok((inf, mkb, version))
    }

    /// AACS major version ([`crate::aacs::mkb::AACS_MAJOR_BD`] /
    /// [`crate::aacs::mkb::AACS_MAJOR_UHD`]) from the content certificate. Drives the
    /// `Unit_Key_RO.inf` parse stride (48-byte V10 vs 64-byte V20/V21), so the
    /// out-of-band key-fetch path parses `enc_title_keys` at the right stride (a
    /// server VUK then derives the correct unit keys).
    ///
    /// When no content certificate is readable/parseable, defaults to **UHD
    /// (V20, 64-byte stride)** — the conservative choice the pre-1.2.0 fetch path
    /// hardcoded — and logs it: a wrong stride here folds a server VUK against
    /// mis-strided title keys (silent wrong unit keys), so a missing cert must
    /// not quietly pick the V10 stride for a UHD disc.
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

    /// Read the AACS MKB's real record stream — NOT its zero padding.
    ///
    /// `MKB_RO.inf` / `MKB_RW.inf` are allocated to a fixed ~128 MiB and
    /// zero-padded; the actual record stream is a few MiB. We read a bounded
    /// prefix, find the record-stream length via [`crate::aacs::mkb::mkb_content_len`]
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
            let buf = crate::aacs::read_first(
                &crate::aacs::role_paths(udf_fs, crate::aacs::AacsRole::Mkb),
                |p| udf_fs.read_file_prefix(reader, p, want),
            )?;
            let n = crate::aacs::mkb::mkb_content_len(&buf);
            // `n` strictly inside `buf` => the record walk reached the padding
            // boundary (full content captured). `buf` shorter than `want` =>
            // the whole file is already read. Otherwise the records may run
            // past the prefix — grow and retry, bounded by MAX_BYTES.
            if (n > 0 && n < buf.len()) || buf.len() < want || want >= MAX_BYTES {
                return Ok(crate::aacs::mkb::trim_mkb(buf));
            }
            want = (want * 2).min(MAX_BYTES);
        }
    }

    /// Read a disc's AACS key-input files from an ISO image: returns
    /// `(Unit_Key_RO.inf, MKB, aacs_major_version)`. For callers that resolve a
    /// Unit Key out-of-band: obtain the key however you like, then apply it via
    /// [`Disc::decrypt_with`]. libfreemkv never makes a network call.
    pub fn read_aacs_inputs(iso_path: &std::path::Path) -> Result<(Vec<u8>, Vec<u8>, u8)> {
        // Preserve the underlying open error (`Error::IoError`, E5000, carrying
        // the OS errno) instead of collapsing ENOENT/EPERM/etc. into
        // `Error::AacsNoKeys` (E7000). A missing or unreadable ISO is an I/O
        // fault, not a key-resolution failure; callers that dispatch on
        // `.code()` must be able to tell the two apart.
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

    /// Same as [`Disc::read_aacs_inputs`] but reads from a live drive. The
    /// out-of-band Unit Key path fetches the disc's key files from the drive,
    /// resolves a key from them however it likes, then applies it via
    /// [`Disc::decrypt_with`]. These files are plaintext UDF metadata — no
    /// AACS handshake or keys are required to read them.
    pub fn read_aacs_inputs_from_drive(drive: &mut Drive) -> Result<(Vec<u8>, Vec<u8>, u8)> {
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
        opts: &ScanOptions,
        udf_fs: udf::UdfFs,
    ) -> Result<Self> {
        let scan_with_t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::scan", phase = "scan_with", "begin");
        // 2. Resolve encryption (AACS, CSS, or none)
        let encrypted = aacs_dir_present(&udf_fs);

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

        // 3. Titles + container — dispatched by on-disc tree. HD-DVD and DVD are
        //    tree-level peers, each with its own enumerator; FMTS shares the BD
        //    tree (a `.fmts` stream variant). Disc FORMAT is a separate axis
        //    derived below from the AACS MKB generation, not the tree.
        // The DVD scanner resolves its own main feature by following the disc's
        // First-Play navigation (issue #40); it hands that title's `playlist_id`
        // back here to use as the `nav_feature` ranking signal, mirroring the BD
        // path's `bdnav::resolve_feature`. `None` for every other tree.
        let mut dvd_nav_feature: Option<u16> = None;
        let (mut titles, content_format) = if udf_fs.find_dir("/BDMV").is_some() {
            (
                Self::scan_bluray_titles(reader, &udf_fs, opts.halt.as_ref())?,
                ContentFormat::BdTs,
            )
        } else if udf_fs.find_dir("/HVDVD_TS").is_some() {
            (
                Self::scan_hddvd_titles(reader, &udf_fs, opts.halt.as_ref())?,
                ContentFormat::MpegPs,
            )
        } else if udf_fs.find_dir("/VIDEO_TS").is_some() {
            let (dvd_titles, nav) = Self::scan_dvd_titles(reader, &udf_fs, opts.halt.as_ref())?;
            dvd_nav_feature = nav;
            (dvd_titles, ContentFormat::MpegPs)
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
        let feature_hint = crate::labels::apply(reader, &udf_fs, &mut titles);

        // The authoritative signal: run the disc's own HDMV navigation the way a
        // real player would (First-Play in the `bdnav` VM) and see which playlist
        // it plays as the feature. Only for a BDMV tree; the VM abstains on BD-J
        // discs and any non-convergence, returning `None`. Restrict the accepted
        // result to video-bearing, non-trivial titles (duration within half of
        // the longest video title's) so a short logo/pre-roll `PlayPlayList`
        // cannot be mistaken for the feature.
        let nav_feature = if udf_fs.find_dir("/BDMV").is_some() {
            let longest_video_dur = titles
                .iter()
                .filter(|t| t.has_probable_video())
                .map(|t| t.duration_secs)
                .fold(0.0_f64, f64::max);
            let candidates: std::collections::HashSet<u16> = titles
                .iter()
                .filter(|t| {
                    t.has_video()
                        && (longest_video_dur <= 0.0 || t.duration_secs >= 0.5 * longest_video_dur)
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

        // Authoritative main-feature ordering. The physical `canonical_title_order`
        // sort above is refined once labels have run: the disc's own navigation
        // pick (`nav_feature`) wins outright when present; otherwise a play-all /
        // wrapper COMPOSITE (e.g. a real UHD title's decoy playlist `00245` =
        // `[bumper][00001 feature clip][outro]`) is demoted below the standalone
        // title it wraps, a title with no plausible video is dropped, and a
        // feature the disc's own menu authoring designates (`feature_hint`) is
        // promoted. After this, `titles[0]` is the selected main feature. The
        // `canonical_title_order` sort is kept first because label ANCHORING
        // (`labels::apply`, above) reads titles in duration-descending order.
        Self::sort_titles_by_main_feature(
            &mut titles,
            capacity_bytes,
            feature_hint.as_ref(),
            nav_feature,
        );

        // Optional content-based forced-subtitle detection. `info` opts in so its
        // forced flags match what the muxer derives during a rip (both use the
        // one shared PGS classifier); the rip path leaves it off — the muxer
        // detects forced while muxing, without a second read of the clip.
        if opts.probe_forced_subtitles {
            Self::probe_forced_subtitles_for_bdts_titles(reader, &mut titles, opts.halt.as_ref());
        }
        crate::labels::fill_defaults(&mut titles);

        // 5. Format (AACS MKB generation → BD/UHD/FMTS; tree → HD-DVD/DVD) and
        //    layers. Region coding is not yet decoded from the disc — every disc
        //    reports Region-free for now (correct for all UHD; a stub for
        //    region-locked BD/DVD until region detection is implemented).
        let format = Self::detect_disc_format(reader, &udf_fs, &titles);
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
    ///    possible data on this disc" gate. `capacity_bytes == 0` means
    ///    the capacity is UNKNOWN (READ CAPACITY failed), so the gate is
    ///    skipped entirely rather than demoting every real title.
    /// 2. Among real titles, LARGEST physical size first — the main
    ///    feature is the biggest real title on the disc. (This replaced
    ///    the old clip-count ordering, which mis-ranked heavily
    ///    chapter-per-clip discs.)
    /// 3. Tiebreak on longer duration first.
    ///
    /// **Effect on non-branching discs:** unchanged — the main movie
    /// is already the longest 1-clip title.
    /// **Effect on branching UHDs:** the virtual play-all playlist is
    /// pushed to the back, the actual movie surfaces at index 0.
    /// The sort keys [`Self::canonical_title_order`] applies, in priority
    /// order, as diagnostic-facing tokens.
    ///
    /// Defined HERE, immediately beside the comparator, so a diagnostic can
    /// NAME the ordering instead of restating it. The `freemkv::diag`
    /// main-feature decision row used to carry its own hand-written copy of
    /// this list, and it drifted: it still advertised a `fewest-clips` key long
    /// after the comparator replaced clip-count with largest-physical-size, so
    /// the `--log-level 3` bug-report log explained freemkv's top-level pick
    /// with a rule freemkv does not apply. Any change to the keys below must
    /// change this list in the same edit.
    pub const CANONICAL_TITLE_ORDER_KEYS: &'static [&'static str] =
        &["fits-disc", "largest-size", "longest", "richest-audio"];

    /// Content-based forced-subtitle detection, restricted to `BdTs` titles —
    /// gates on the STREAM CONTAINER, not the disc-tree format, because a
    /// disc can carry non-`BdTs` titles even under `BDMV/` scanning. Only
    /// `BdTs` titles carry PES-wrapped PGS the shared classifier
    /// (`pgs_forced_probe`) understands; running it against an HD-DVD/DVD
    /// (`MpegPs`) title's extents would demux the wrong container.
    ///
    /// Pulled out of `scan_with` as its own callable predicate: the decision
    /// is otherwise reachable only by driving the full scan pipeline (tree
    /// dispatch → title parsing → this loop), so a test could not pin the
    /// gate without also authoring an entire synthetic disc image.
    fn probe_forced_subtitles_for_bdts_titles(
        reader: &mut dyn SectorSource,
        titles: &mut [DiscTitle],
        halt: Option<&crate::halt::Halt>,
    ) {
        // One cache across every title: a disc's playlists overwhelmingly
        // reference the same handful of clips (main feature, play-all,
        // seamless-branch variants), so without memoisation the same physical
        // extents are re-read from the drive once per playlist — 30-150 times
        // on a typical Blu-ray.
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
        // A title bigger than the whole disc is a "play-all" composite artifact
        // (its declared size double-counts clips shared with other playlists) —
        // demote it below any real single title.
        //
        // `capacity_bytes == 0` means the capacity is UNKNOWN, not that the
        // disc holds nothing: `read_udf` substitutes 0 when READ CAPACITY
        // fails and scans on regardless. Applied literally the gate would
        // INVERT there — every real title (`size_bytes > 0`) would be
        // "oversize" and demoted, while a CLPI-less `size_bytes == 0` title
        // would not, landing at `titles[0]` ahead of the feature. With no
        // capacity to compare against, the gate is inert and the size /
        // duration / audio keys decide the order on their own.
        let capacity_known = capacity_bytes > 0;
        let a_oversize = capacity_known && a.size_bytes > capacity_bytes;
        let b_oversize = capacity_known && b.size_bytes > capacity_bytes;
        a_oversize
            .cmp(&b_oversize)
            // PRIMARY: largest physical size = the main feature. Robust where
            // duration and clip-count are not — a decoy "play-all" playlist runs
            // long (e.g. 1h31m) but is tiny (0.4 GB of reused/junk clips), and the
            // real feature is often chaptered into MANY clips (one per chapter),
            // which the old clip-count-ascending key wrongly demoted below 1-clip
            // bonus reels. Validated across 23 UHD/BD discs — fixes several
            // heavily-chaptered features (each ranked ~#13–36 before the fix),
            // no regressions on the 19 already correct.
            .then_with(|| b.size_bytes.cmp(&a.size_bytes))
            // Tiebreak for equal-size twins: longer duration, then richer audio —
            // the same feature authored as sibling playlists (a full-audio main
            // vs an audio-reduced twin: 00800 [DTS-HD MA + 13 tracks] vs
            // 00004 [stereo AC-3 only]). Prefer lossless-multichannel.
            .then_with(|| b.duration_secs.total_cmp(&a.duration_secs))
            .then_with(|| Self::audio_richness(b).cmp(&Self::audio_richness(a)))
    }

    /// The keys [`Self::main_feature_order`] layers ON TOP of
    /// [`Self::CANONICAL_TITLE_ORDER_KEYS`], highest priority first, as
    /// diagnostic tokens. Kept beside the comparator so the `freemkv::diag`
    /// decision row can NAME the ordering instead of restating it (same
    /// anti-drift contract as `CANONICAL_TITLE_ORDER_KEYS`).
    ///
    /// - `nav-feature`: the title the disc's own HDMV navigation plays as the
    ///   feature, resolved by running First-Play in the [`crate::bdnav`] VM the
    ///   way a real player would. This is the authoritative, size-independent
    ///   pick; it is video-gated (a mis-resolve cannot select a streamless
    ///   playlist) and simply absent when the VM abstains (BD-J discs, or
    ///   non-convergence), in which case the lower keys decide.
    /// - `authoring-feature`: a title the disc's own menu authoring designates
    ///   as the feature (has video, is not itself a composite, and is within a
    ///   sane fraction of the longest title's duration) outranks everything
    ///   else — a size-independent signal an obfuscation decoy cannot forge.
    /// - `standalone`: a title that is a play-all / wrapper COMPOSITE — its clip
    ///   set properly contains another substantial video title's whole clip set
    ///   (a real UHD title's decoy playlist `00245` = `[bumper][00001 feature clip][outro]`
    ///   case) — is demoted below the standalone title it wraps. This is the
    ///   STRUCTURAL anti-decoy signal; it does not depend on stream counts,
    ///   physical size, or disc capacity, so it survives where those are spoofed
    ///   or unknown.
    /// - `has-video`: a title with no plausible video content (see
    ///   [`DiscTitle::has_probable_video`]) is demoted below every title that
    ///   has some — a floor that drops streamless menu/metadata playlists.
    pub const MAIN_FEATURE_ORDER_KEYS: &'static [&'static str] = &[
        "nav-feature",
        "authoring-feature",
        "standalone",
        "has-video",
    ];

    /// Precompute the [`TitleRank`] for every title, in the same index order.
    ///
    /// Composite detection: title `P` is a composite when some OTHER title `Q`
    /// has a non-empty clip-id set that is a PROPER SUBSET of `P`'s
    /// (`Q ⊊ P` — `P` re-uses all of `Q`'s clips plus extra), `Q` has plausible
    /// video, and `Q` accounts for at least half of `P`'s declared size (so `Q`
    /// is a real feature `P` merely wraps/aggregates, not an incidental short
    /// clip). This catches both the wrapper decoy (`00245 ⊋ 00001`) and the
    /// oversize play-all — independent of capacity, which the `fits-disc` gate
    /// needs and which is `0`/unknown on drives that fail READ CAPACITY.
    ///
    /// Known residual: a real feature that has a separate "resume from the
    /// middle" branch playlist reusing most of its clips could be mis-flagged;
    /// this is rare on BD/UHD, and the (higher-precedence) nav and authoring
    /// signals override it when present. The corpus title-selection gate is the
    /// backstop across the disc hoard.
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
        // Largest video-bearing title's byte size, used as a plausibility floor
        // below. A nav/authoring signal can legitimately resolve a seamless-branch
        // or Dolby-Vision presentation playlist that a player navigates to but
        // that carries almost no unique payload (e.g. a 0.4 GB / 102-clip branch
        // sitting beside the 91.5 GB single-clip feature). Those are correct for
        // playback but wrong as a RIP target, so a promoted title must also hold a
        // meaningful fraction of this size; otherwise it falls through to the
        // composite/size ranking, which recovers the substantial feature.
        let max_video_size = titles
            .iter()
            .filter(|t| t.has_probable_video())
            .map(|t| t.size_bytes)
            .max()
            .unwrap_or(0);
        let carries_feature_payload = |t: &DiscTitle| {
            max_video_size == 0 || t.size_bytes as f64 >= 0.10 * max_video_size as f64
        };
        // The composite scan below is O(n^2 · clip-set-size). BOTH the title count
        // and the per-title clip count are untrusted disc metadata (playlist count
        // and PlayItem count — the latter a u16 the mpls parser does not otherwise
        // cap), so a crafted image can inflate either dimension into a multi-second
        // uncancellable stall. Bound the total pair work to a sub-second budget;
        // above it, skip composite classification (every real disc is orders of
        // magnitude under it) and let the has-video + canonical tiers order the
        // titles. Bounding the PRODUCT covers both the many-tiny-titles and the
        // few-clip-heavy-titles attack shapes.
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
                                && q.size_bytes as f64 >= 0.5 * p.size_bytes as f64
                                && qj.is_subset(pi)
                        }
                    });
                // The authoring hint is honoured only when the hinted title has
                // REAL video streams (strict `has_video`, not the permissive
                // `has_probable_video` floor — so a hint at a large streamless
                // entry cannot resurrect the decoy), is NOT itself a composite (a
                // mis-authored id pointing at a wrapper can't win), and runs at
                // least half as long as the longest title (audit R4 — a hint at a
                // short trailer/branch must not override the real 2h feature).
                let authoring = !composite
                    && hint.is_some_and(|h| h.matches(p.playlist_id, &p.playlist))
                    && p.has_video()
                    && (longest_video_dur <= 0.0 || p.duration_secs >= 0.5 * longest_video_dur)
                    && carries_feature_payload(p);
                // The nav result is video-gated exactly like the authoring hint,
                // so a mis-resolved playlist that happens to be streamless can
                // never win (and thus cannot reopen the decoy hole). It is also
                // payload-gated: a player may navigate to a low-byte branch/DV
                // presentation, but that is never the rip target.
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

    /// The disc format, from the two on-disc axes:
    ///   * **tree** → HD-DVD (`HVDVD_TS/`) and DVD (`VIDEO_TS/`) are tree-level
    ///     peers with their own enumerators;
    ///   * **AACS MKB generation** → within the BD tree (`BDMV/`), the MKB Type
    ///     record decides BD (1.0) / UHD (2.0) / FMTS (2.1). This is the
    ///     authoritative, cheap signal (the Type record is the first bytes of
    ///     `MKB_RO.inf`) — reusing [`crate::aacs::mkb::mkb_type`] /
    ///     [`crate::aacs::mkb::MkbType::generation`], not a filesystem heuristic.
    ///
    /// An unencrypted / MKB-less BD tree falls back to video resolution (still a
    /// BD-tree disc, so never below [`DiscFormat::BluRay`]).
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
            // Unencrypted / unreadable MKB: refine by resolution, but a BD-tree
            // disc is never below Blu-ray — only UHD can promote it. detect_format
            // is a general resolution classifier that can return Dvd for an SD
            // bonus/menu title, which must NOT tag a BDMV disc as DVD (that
            // mis-sizes the ECC-block sweep). Clamp anything but UHD up to BluRay.
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
        let result = session.scsi_execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        // Share the decoder with `Drive::capacity` rather than re-deriving it.
        // A hand-rolled copy here previously dropped the short-transfer check:
        // a drive answering GOOD with an empty data phase leaves `buf` zeroed,
        // which decodes to a 1-sector disc instead of an error.
        crate::drive::decode_read_capacity(&buf, result.bytes_transferred)
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

// Redacting `Debug`: `Key` is crate-root re-exported and is the key-transport
// type crossing `Disc::decrypt_with`; every variant carries raw key material.
// Print only the variant name and count — never bytes. Guarded by
// `aacs_state_and_key_debug_are_redacted`.
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
/// Reuses the ecosystem's single `is_clean` content-clarity predicate and the
/// full (bus + AACS) unit decrypt, so it agrees with the actual mux decrypt.
fn aligned_unit_keys_validate(
    unit_keys: &[(u32, [u8; 16])],
    read_data_key: Option<&[u8; 16]>,
    samples: &[Vec<u8>],
    format: ContentFormat,
) -> bool {
    use crate::aacs::content::{
        ALIGNED_UNIT_LEN, aacs_unit_needs_decrypt, decrypt_bus, decrypt_unit, is_clean,
    };
    let scrambled: Vec<&[u8]> = samples
        .iter()
        .map(|s| s.as_slice())
        .filter(|s| aacs_unit_needs_decrypt(s, format))
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
            // bus layer (AACS 2.0) first, then the CPS unit key, then the structural
            // proof — the composed form of the old `decrypt_unit_full`.
            if let Some(rdk) = read_data_key {
                decrypt_bus(&mut probe, rdk);
            }
            decrypt_unit(&mut probe, k);
            if is_clean(&probe, format) {
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
                format: self.content_format,
            }
        } else if let Some(ref css) = self.css {
            crate::decrypt::DecryptKeys::Css {
                title_key: css.title_key,
            }
        } else {
            crate::decrypt::DecryptKeys::None
        }
    }

    /// Resolve a WHOLE-DISC AACS key map for a decrypting sweep (`disc:// → iso://`):
    /// the union of every title's proactive key map ([`crate::mux::resolve_mux_key_map`]),
    /// so a sequential read of the entire disc decrypts each content unit with its
    /// mapped key and passes clear filesystem/nav sectors (in no range) through.
    /// Fails loud (via the per-title resolve) if any content unit's key is missing.
    /// `keys` is mutated as fetched keys are banked; the merged ranges are disjoint
    /// (titles that share a clip resolve the same span — the duplicate is dropped).
    pub fn resolve_content_key_map(
        &self,
        reader: &mut dyn SectorSource,
        keys: &mut crate::decrypt::DecryptKeys,
        fetch: Option<&crate::sector::KeyFetch>,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<crate::decrypt::AacsKeyMap> {
        let mut ranges: Vec<(u32, u32, usize, crate::decrypt::Phase)> = Vec::new();
        // ONE per-disc memo across every title. Without it every playlist re-derives
        // the same disc-wide facts off the drive:
        //
        // * the multi-CPS "which held key opens this extent" decision — 8 random
        //   6144-byte reads per extent, ~200 ms of seek apiece on a stock BD drive.
        //   A disc's playlists overwhelmingly reference the same handful of clips
        //   (main feature, play-all, per-chapter and seamless-branch variants), so
        //   this recomputed the same index from byte-identical input;
        // * the UDF walk + `/AACS/IndividualSegment.tbl` read that decides whether
        //   the disc is FMTS at all — ~35 single-sector reads at low LBAs, reached
        //   from a head the previous title's content sampling left deep in the
        //   content area, so a full-stroke seek out and back per playlist. This
        //   runs on EVERY disc, FMTS or not;
        // * on an FMTS (AACS 2.1) disc, the forensic anchor probe, the per-index
        //   phase probe AND the key-service round trip that returns the disc's
        //   index-key set. That last one is the key-server storm: one round trip
        //   per playlist for one disc-wide answer.
        //
        // The FMTS memos are what makes the multi-CPS memo reachable at all on an
        // FMTS disc — that path returns its finished map before the extent loop.
        let mut cache = crate::mux::resolve::DiscKeyCache::new();
        for title in &self.titles {
            let map = crate::mux::resolve::resolve_mux_key_map_cached(
                reader,
                title,
                keys,
                fetch,
                self.content_format,
                halt,
                &mut cache,
            )?;
            ranges.extend_from_slice(map.ranges());
        }
        Ok(crate::decrypt::AacsKeyMap::from_ranges_phased(
            merge_content_key_ranges(ranges),
        ))
    }

    /// The disc's AACS-encrypted content as a sorted, merged, disjoint set of
    /// `(start_lba, sector_count)` ranges — the union of every title's m2ts
    /// stream extents.
    ///
    /// This is the authoritative "which sectors are encrypted" map for a
    /// whole-disc read. AACS only encrypts the m2ts AV streams, so a sector is
    /// encrypted content **iff** it falls inside one of these ranges; everything
    /// else (UDF filesystem, BDMV nav, PLAYLIST/CLIPINF) is always clear.
    ///
    /// The in-read decrypt-verify gate (`DecryptingSectorSource`) uses this so it
    /// never consults the TS-sync content check about
    /// non-content bytes — filesystem data has no TS sync and would otherwise be
    /// mistaken for ciphertext (the first-2-GB false-positive this fixes).
    ///
    /// Empty when the disc has no parsed titles (CSS / unencrypted / unscanned);
    /// callers treat an empty map as "no content gate" and fall back accordingly.
    pub fn encrypted_content_ranges(&self) -> Vec<(u32, u32)> {
        merged_extents(self.titles.iter().flat_map(|t| &t.extents))
    }

    /// The 40-hex AACS disc id (SHA1 of `Unit_Key_RO.inf`, no `0x` prefix), or
    /// empty when this disc has no captured AACS state. Used to name the disc in
    /// a [`Error::NoDiscKey`] so the application can tell the user which disc to
    /// add to the keydb.
    pub(crate) fn aacs_disc_hash(&self) -> String {
        self.aacs
            .as_ref()
            .map(|a| crate::hex::strip_hex_prefix(&a.disc_hash).to_string())
            .unwrap_or_default()
    }

    /// The unlocker matrix for this scanned disc on `drive`: each REGISTERED
    /// unlocker's name + whether it actually **did work this rip** — i.e. ran and
    /// accomplished its job, NOT merely "matched the disc kind". Registry-driven
    /// names (no hardcoding) so the CLI and autorip render an identical, always-
    /// current report; the per-unlocker runtime signal is computed here because
    /// the library owns unlock semantics AND the disc/drive state.
    ///
    /// The distinction matters: on a drive taken by the firmware bus-unlock
    /// route, that route removes the AACS bus and reads the VID, so the AACS
    /// host-cert unlocker never runs — it "matched" (AACS disc) but did nothing.
    /// `did-work` reports that honestly (`AACS: no`), and on a *stock* drive that
    /// fell back to the cert route it reports `MT1959: no, AACS: yes` — the real
    /// diagnostic.
    pub fn unlocker_matrix(&self, drive: &crate::Drive) -> Vec<(&'static str, bool)> {
        // The drive-prep unlocker that actually ran (recorded on init):
        // "MT1959" (drive-firmware bus-unlock route) or "Renesas" (cert route) —
        // mutually exclusive per drive.
        let prep = drive.unlocker_name();
        // Only the firmware bus-unlock route removes AACS bus encryption AT THE
        // DRIVE; the Renesas route unlocks features but leaves the bus to the cert.
        let ld_removed_bus = prep == Some("MT1959");
        crate::unlock_bridge::unlocker_names()
            .into_iter()
            .map(|name| {
                let did_work = match name {
                    // Each firmware unlocker did work iff it was the one that ran.
                    "MT1959" => ld_removed_bus,
                    "Renesas" => prep == Some("Renesas"),
                    // The AACS host-cert route removed the bus ONLY when the
                    // firmware didn't (stock or Renesas drive) AND AACS state was
                    // actually obtained.
                    "AACS" => self.aacs.is_some() && !ld_removed_bus,
                    // The DVD read-unlock (CSS bus-auth) is issued during the scan
                    // for EVERY DVD to clear the drive's scrambled-read barrier;
                    // unlike the firmware/AACS arms above (whose success is already
                    // captured in disc state), the bus-auth outcome is not tracked
                    // separately, so this arm reports the MEDIUM — "the DVD
                    // read-unlock path engaged" — rather than a per-rip success
                    // bit. That is intentional: the CSS descramble is keyless and
                    // handled at mux time, and a genuine bus-auth failure surfaces
                    // downstream as a read/crack error, not here. (Formerly this
                    // reported `self.css.is_some()` — "a crack recovered a key" —
                    // which under-reported: an encrypted DVD that reads and muxes
                    // fine showed "no".)
                    "DVD" => self.format == DiscFormat::Dvd,
                    // A newly-registered unlocker with no runtime signal wired
                    // here yet: report `no` rather than guess.
                    _ => false,
                };
                (name, did_work)
            })
            .collect()
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
    /// - `self.css_error.is_some()` → `Err(Error::CssNoDiscKey)`. The scan saw
    ///   scrambled CSS sectors but recovered no title key (`self.css` is `None`
    ///   yet the content IS encrypted). Treating `css.is_none()` as
    ///   "unencrypted" would mux scrambled MPEG as plaintext garbage. A
    ///   DISC-LEVEL verdict (`error::is_disc_level_no_key`) — the main feature's
    ///   crack failed, so every title fails the same way and the rip loop must
    ///   stop rather than skip each title in turn.
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
        // so the key check below can't see it. `css_error` records the MAIN
        // feature's crack, so this is a WHOLE-DISC verdict — every title would
        // fail identically — and it is raised as `CssNoDiscKey` (disc-level,
        // `error::is_disc_level_no_key`), never as the per-title
        // `CssKeyMissing` (`error::is_skippable_title_stub`). Raised as the
        // latter, an undecryptable disc made the rip loop iterate all N titles
        // logging "title skipped, it was an empty stub" and exit 0.
        if self.css_error.is_some() {
            return Err(Error::CssNoDiscKey);
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
                // not the generic `NoDiscKey`. Any reason NOT listed below (or an
                // absent one) → `NoDiscKey` naming the disc by hash, unchanged.
                //
                // Same split, second axis: when a key SOURCE could not answer
                // (unreachable / 5xx / bad token / rate-limited) resolution
                // stamps that reason here, and it must surface as ITSELF.
                // `NoDiscKey` asserts every source answered and none holds a key
                // — the opposite of what happened — and reporting a seven-hour
                // 502 outage that way sent operators looking for a VUK when the
                // right action was to wait.
                match self.aacs_error {
                    Some(Error::AacsVidUnavailable) => return Err(Error::AacsVidUnavailable),
                    Some(Error::KeyServiceUnavailable) => {
                        return Err(Error::KeyServiceUnavailable);
                    }
                    Some(Error::KeyServiceUnauthorized) => {
                        return Err(Error::KeyServiceUnauthorized);
                    }
                    Some(Error::KeyServiceRateLimited) => {
                        return Err(Error::KeyServiceRateLimited);
                    }
                    _ => {}
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
    /// For a **DVD** the CSS title key MUST be recovered before descrambling: a
    /// scrambled sector without a recoverable crib cannot self-crack, and CSS leaves
    /// the pack/PES header clear, so a sector left un-descrambled would mux as a
    /// structurally-valid but corrupt PES packet with no loss reported. Two ways
    /// to get it:
    ///
    /// - **Fast path** — the scan already cracked a key whose LBA span
    ///   ([`crate::css::CssState::crack_span`]) covers this title's VTS: reuse it.
    ///   No re-read, and on a live drive no second CSS bus-auth round-trip. CSS
    ///   title keys are per-VTS, so an overlapping span is the same key.
    /// - **Crack** — when up-front detection missed (`self.css == None`) or the
    ///   title lives in a different VTS: crack the key from this title's OWN
    ///   extents in a SINGLE scan, in natural PLAYBACK ORDER (never largest-cell-
    ///   first, which starved the crack in a big cell's clear prefix — the 1.5.1
    ///   bug). Playback order reaches the scrambled feature body after only the
    ///   small clear front matter (logo / rating card) that precedes it. One
    ///   scan = one CSS-locked early-bail, so a locked title is not re-hammered
    ///   per cell against a live drive (hard rule #2); its 50k-sector budget is
    ///   the same accepted bound the disc-wide scan uses.
    ///
    /// Crucially the crack path does NOT gate on `self.css`: a detection miss can
    /// never route the mux into raw passthrough of scrambled sectors.
    ///
    /// Outcomes (`batch_sectors` sizes the crack's batched reads):
    /// - `(Css{title_key}, false)` — descramble with the recovered/reused key.
    /// - `(None, true)` — a genuinely-clear title needs no key; the gate passes it.
    /// - `(None, false)` — scrambled but no key recoverable → hard failure via
    ///   [`Self::ensure_title_decryptable`], never a silent garbage mux.
    ///
    /// Non-DVD schemes (AACS / FMTS / genuinely unencrypted) return
    /// [`Self::decrypt_keys`] unchanged.
    pub fn decrypt_keys_for_title(
        &self,
        idx: usize,
        reader: &mut dyn SectorSource,
        batch_sectors: u16,
    ) -> (crate::decrypt::DecryptKeys, bool) {
        // Non-DVD (AACS / FMTS / genuinely unencrypted): disc-wide keys, unchanged.
        if self.format != DiscFormat::Dvd {
            return (self.decrypt_keys(), false);
        }
        let title = match self.titles.get(idx) {
            Some(t) if !t.extents.is_empty() => t,
            // No extents to crack from: nothing scrambled to worry about, so mark
            // it clear (`true`). Returning `false` here would let the gate's DVD
            // "None keys + not clear = scrambled-uncracked" rule wrongly hard-fail
            // a genuinely-unencrypted DVD title that has no extents.
            _ => return (self.decrypt_keys(), true),
        };
        // Fast path: reuse the scan's cracked key if its span covers this title's
        // VTS. `crack_span: None` (unknown provenance) is treated as covering.
        if let Some(css) = self.css.as_ref() {
            let covers = match css.crack_span {
                None => true,
                Some((cs, ce)) => title
                    .extents
                    .iter()
                    .any(|e| e.start_lba < ce && cs < e.start_lba.saturating_add(e.sector_count)),
            };
            if covers {
                return (
                    crate::decrypt::DecryptKeys::Css {
                        title_key: css.title_key,
                    },
                    false,
                );
            }
        }
        // Detection miss or a different VTS: crack this title's key from its OWN
        // extents in a SINGLE scan, in natural PLAYBACK ORDER (never largest-cell-
        // first — that was the 1.5.1 garbage bug, where a big cell's clear prefix
        // starved the crack). Playback order reaches the scrambled feature body
        // after only the (small) clear front matter that precedes it on a real
        // disc. This is ONE crack_key_outcome call, exactly like the disc-wide
        // scan: its single CSS-locked early-bail runs at most once, so a locked/
        // uncrackable title is NOT re-hammered per cell against the live drive
        // (hard rule #2). The crack's 50k-sector budget bounds a fully-clear title,
        // the same accepted bound the disc-wide scan uses.
        match crate::css::crack_key_outcome(reader, &title.extents, batch_sectors, None) {
            crate::css::CrackOutcome::Cracked(state) => (
                crate::decrypt::DecryptKeys::Css {
                    title_key: state.title_key,
                },
                false,
            ),
            // Scrambled but no key recoverable → hard failure.
            crate::css::CrackOutcome::ScrambledUncracked => {
                (crate::decrypt::DecryptKeys::None, false)
            }
            // No scrambled sector anywhere in the whole title → genuinely clear.
            crate::css::CrackOutcome::Unencrypted => (crate::decrypt::DecryptKeys::None, true),
        }
    }

    /// Per-title decrypt gate that honours the `title_is_clear` verdict from
    /// [`Self::decrypt_keys_for_title`].
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
        // A DVD title that cracked to no key and is NOT clear is scrambled-but-
        // uncrackable (`decrypt_keys_for_title` → `ScrambledUncracked`). Hard-fail
        // here directly: `ensure_decryptable_keys` gates CSS on `self.css.is_some()`
        // (the scan's disc-wide detection), which can be `None` when that up-front
        // detection missed — exactly the case the per-title crack exists to catch.
        // Without this, an uncrackable DVD title would fall through to `Ok` and mux
        // scrambled sectors as corrupt PES at exit 0.
        //
        // `CssKeyMissing` (per-title, `error::is_skippable_title_stub`) is the
        // RIGHT code here and must stay: this is one title of a multi-VTS disc,
        // and a sibling title in another VTS may still crack its own key, so an
        // all-titles rip skips this one and finishes the rest. The whole-disc
        // failure — nothing on the disc cracked — is the gate above's
        // `CssNoDiscKey`.
        if self.format == DiscFormat::Dvd && !title_is_clear && !keys.is_encrypted() {
            return Err(Error::CssKeyMissing);
        }
        // A usable per-title key was resolved (a freshly-cracked CSS key, or AACS
        // unit keys) — the title IS decryptable, so pass it WITHOUT consulting the
        // disc-wide gate. `ensure_decryptable_keys` hard-fails (disc-level,
        // `CssNoDiscKey`) on `self.css_error`
        // unconditionally, which reflects the MAIN feature's crack: a bonus title
        // in a different VTS that just cracked its own key must not be blocked by
        // the main title having failed.
        if keys.is_encrypted() {
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
    pub(crate) fn inject_unit_keys(&mut self, keys: Vec<(u32, [u8; 16])>) {
        if let Some(aacs) = self.aacs.as_mut() {
            aacs.unit_keys = keys;
            aacs.key_source = KeyOrigin::ExternalUk;
        } else if self.encrypted && self.css.is_none() {
            // FMTS is AACS 2.1, a UHD-family (bus-encrypted) format — not BD.
            let uhd_family = matches!(self.format, DiscFormat::Uhd | DiscFormat::Fmts);
            self.aacs = Some(AacsState {
                version: if uhd_family {
                    crate::aacs::mkb::AACS_MAJOR_UHD
                } else {
                    crate::aacs::mkb::AACS_MAJOR_BD
                },
                bus_encryption: uhd_family,
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
                    supplied.disc_entry = Some(crate::aacs::types::DiscEntry {
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

            let provider_refs: [&dyn crate::aacs::provider::KeyProvider; 1] = [&supplied];
            let ctx = crate::aacs::resolve::ResolveContext {
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
            let resolved = crate::aacs::resolve::resolve_keys_with_reason(&ctx, version_u8)
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
        if !aligned_unit_keys_validate(
            &candidate_unit_keys,
            read_data_key.as_ref(),
            samples,
            self.content_format,
        ) {
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
}

/// Mapfile path for a regular output file: appends `.mapfile` to the
/// output path. For `/dev/null` (benchmark) output use
/// [`Disc::mapfile_for`], which special-cases it to a temp-dir path
/// derived from the disc title.
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

    /// The image-time CSS crack must scan the main feature's extents in natural
    /// PLAYBACK order — the same order `decrypt_keys_for_title` (the canonical
    /// per-title crack) uses. Largest-cell-first is the 1.5.1 garbage bug: a big
    /// cell's long clear prefix starves the 50k-sector budget before the crack
    /// ever reaches a scrambled sector.
    ///
    /// This test distinguishes the two orderings: the fixture's LARGEST cell is
    /// physically LAST, so largest-cell-first yields a strictly different LBA
    /// sequence than playback order.
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

    /// The main feature for the image-time crack is the canonically-ordered
    /// `titles[0]` (`scan_with` has already applied `canonical_title_order`),
    /// NOT a locally re-derived "most sectors" pick. The capacity gate is the
    /// difference: an oversize play-all composite double-counts shared cells, so
    /// it has the most sectors while being demoted to the back by the canonical
    /// order. Cracking from its extents is cracking from the wrong title.
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

    /// An HD-DVD image is also MPEG-PS, but must NEVER enter the CSS
    /// known-plaintext crack — its `.evo` payload can never satisfy the CSS
    /// attack, and running it wastes a 50_000-sector scan budget in the real
    /// case. Prove the gate actually keeps the crack away: the HD-DVD clip's
    /// own extent carries a genuinely crackable CSS sector, so if the
    /// gate wrongly let the crack run, `disc.css` would come back `Some`.
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

    /// The forced-subtitle probe gates on `content_format == BdTs`, NOT on
    /// whether a title happens to declare a PGS stream: an HD-DVD/DVD
    /// (`MpegPs`) title's PES container is not what the shared PGS classifier
    /// understands, so it must never be probed even if it carries a PGS pid.
    /// The BdTs title (at LBA 0) must be probed; the MpegPs title (at LBA
    /// 5000) must never be touched.
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

    /// Two HD-DVD `.evo` clips (no VTI/playlist, so each is its own title —
    /// see `scan_hddvd_titles`), with the given DECLARED byte sizes. No real
    /// content is written; `scan_with`'s capacity-threshold ranking depends
    /// only on the ICB-declared size, not the bytes at the extent.
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

    /// A cancelled [`crate::halt::Halt`] must stop the HD-DVD title scan.
    ///
    /// The scan is bounded but big — up to `MAX_HDDVD_CLIPS` clips, each
    /// costing an ICB resolve plus a 16 MiB `EVO_PROBE_SECTORS` stream probe —
    /// so on a live drive an operator Stop that only takes effect after the
    /// whole enumerator returns is no Stop at all. `ScanOptions::halt` is
    /// already honoured by the CSS crack and the forced-subtitle probe; the
    /// title enumerator must honour it too.
    ///
    /// It must also not report a HALF-ENUMERATED disc as a successful scan:
    /// a truncated title list is indistinguishable from a disc that genuinely
    /// holds fewer titles.
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
        let res = Disc::scan_with(&mut reader, 3_997_952, None, None, &opts, udf);
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

    /// `scan_with` must hand `ScanOptions::halt` to the BLU-RAY enumerator.
    ///
    /// Every BD and DVD cancellation test in this crate calls
    /// `scan_bluray_titles` / `scan_dvd_titles` DIRECTLY, so the three-line
    /// wiring in `scan_with` that connects the operator's Stop to them was
    /// covered by nothing at all: replacing `opts.halt.as_ref()` with `None`
    /// on either branch left the whole suite green while a Stop during a BD
    /// or DVD scan silently did nothing. Only the HD-DVD branch was pinned.
    ///
    /// The fixture is deliberately the smallest disc that takes the BD branch
    /// — a bare `/BDMV` with no PLAYLIST — because the point under test is the
    /// ARGUMENT, not the enumerator's own (separately tested) halt polling.
    /// Nothing between `scan_with`'s entry and the enumerator reads the flag
    /// (the disc is unencrypted, so the AACS path is skipped), so a green here
    /// can only mean the flag arrived.
    ///
    /// Mutation: `Self::scan_bluray_titles(reader, &udf_fs, None)?` fails here.
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
            Disc::scan_with(&mut disc, 500_000, None, None, &ScanOptions::default(), udf).is_ok(),
            "fixture must scan successfully when not cancelled"
        );

        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let halt = crate::halt::Halt::new();
        halt.cancel();
        let opts = ScanOptions {
            halt: Some(halt),
            ..Default::default()
        };
        let res = Disc::scan_with(&mut disc, 500_000, None, None, &opts, udf);
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
            Disc::scan_with(&mut disc, 500_000, None, None, &ScanOptions::default(), udf).is_ok(),
            "fixture must scan successfully when not cancelled"
        );

        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let halt = crate::halt::Halt::new();
        halt.cancel();
        let opts = ScanOptions {
            halt: Some(halt),
            ..Default::default()
        };
        let res = Disc::scan_with(&mut disc, 500_000, None, None, &opts, udf);
        assert!(
            matches!(res, Err(Error::Halted)),
            "a cancelled DVD scan must say so, not hand back whatever it had \
             enumerated so far as a finished scan. Got {:?}",
            res.map(|d| d.titles.len())
        );
    }

    /// A Stop on a LIVE DRIVE never touches `ScanOptions::halt`: `Drive` has
    /// its own flag and `checked_exec` fails every SCSI command with
    /// [`Error::Halted`] once it is set. The HD-DVD enumerator must not
    /// swallow that into a successful scan.
    ///
    /// Measured before this was fixed: the scan returned `Ok` with both
    /// titles present and ZERO streams on each — a cancelled scan wearing the
    /// shape of a disc whose clips carry no video or audio. Downstream that is
    /// a title list to cache, display and rip from.
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
        let res = Disc::scan_with(
            &mut reader,
            3_997_952,
            None,
            None,
            &ScanOptions::default(),
            udf,
        );
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

    /// `scan_with`'s `capacity_bytes = capacity as u64 * 2048` feeds
    /// `canonical_title_order`'s "bigger than the whole disc = play-all
    /// composite" threshold. Chosen so `capacity * 2048` clears both titles
    /// (neither is oversize, so the bigger one — OTHER — ranks first), but
    /// `capacity + 2048` lands BETWEEN the two sizes: MAIN stays non-oversize
    /// while OTHER flips to oversize and gets demoted, changing `titles[0]`.
    /// A `*` → `+` mutation is caught by this ranking flip, not merely a
    /// wrong numeric threshold value.
    #[test]
    fn scan_with_capacity_bytes_uses_multiplication_not_addition() {
        const CAPACITY_SECTORS: u32 = 3_997_952; // *2048 = 8_187_805_696; +2048 = 4_000_000
        let (mut disc, udf) = hddvd_two_clip_disc(3_000_000, 5_000_000);
        let scanned = Disc::scan_with(
            &mut disc,
            CAPACITY_SECTORS,
            None,
            None,
            &ScanOptions::default(),
            udf,
        )
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

    /// Same mechanism as above, tuned to catch a `*` → `/` mutation instead:
    /// `capacity * 2048` clears both titles, but `capacity / 2048` collapses
    /// to a threshold far below MAIN's declared size while still leaving
    /// MAIN under it, so only OTHER flips to oversize.
    #[test]
    fn scan_with_capacity_bytes_uses_multiplication_not_division() {
        const CAPACITY_SECTORS: u32 = 204_800_000; // *2048 = huge; /2048 = 100_000
        let (mut disc, udf) = hddvd_two_clip_disc(1_000, 5_000_000);
        let scanned = Disc::scan_with(
            &mut disc,
            CAPACITY_SECTORS,
            None,
            None,
            &ScanOptions::default(),
            udf,
        )
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

    /// `Resolution::Unknown` has no dimensions, so `pixels()` must report none
    /// (0, 0) — the same treatment `AudioChannels::count()` and
    /// `SampleRate::hz()` already give their `Unknown` variants. Returning a
    /// plausible 1920x1080 is indistinguishable from a real 1080p title, so a
    /// sink (the `json://` one already did this for audio) reports confident
    /// dimensions for video its own neighbouring `resolution` field calls
    /// "unknown".
    ///
    /// The real variants are asserted against spec literals, not against
    /// `pixels()` itself: 720x480 / 720x576 are the DVD-Video coded frames
    /// (ITU-R BT.601 525/60 and 625/50 active area), 1920x1080 and 1280x720 are
    /// the Blu-ray Disc Read-Only Format part 3 HD frames, 3840x2160 the UHD
    /// BD frame.
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

    /// Every `Unknown` variant that exposes a numeric accessor must report
    /// "nothing", never a plausible default — the sibling sweep the previous
    /// round skipped. `FrameRate::Unknown` reports the 0/1 null fraction rather
    /// than 0 fps, because callers divide by the numerator.
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

    /// Blu-ray Disc Read-Only Format part 3, `stream_coding_type` table: 0xA2 is
    /// the SECONDARY DTS-HD audio stream — DTS Express / DTS-HD LBR, a LOSSY
    /// low-bitrate codec carried alongside the primary track for
    /// picture-in-picture and BD-J mixing. It is NOT DTS-HD Master Audio (0x86),
    /// which is the lossless primary code.
    ///
    /// Literals, not the `consts::coding_type` names, so renaming or re-valuing
    /// a constant cannot make this pass vacuously.
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

    /// `byte_offset_in_title` must compute its extent end with saturating
    /// arithmetic, like every other extent-end computation in the crate. A
    /// malformed UDF/IFO extent near the top of the 32-bit LBA space (ECMA-167
    /// logical block numbers are 32-bit) otherwise overflows: a debug build
    /// PANICS inside a library, and a release build wraps to a tiny end LBA so
    /// the range test silently fails and the offset comes back `None`.
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
            key_source: KeyOrigin::ExternalUk,
            vuk: Some([0xD5; 16]),
            unit_keys: vec![(1, [0xD5; 16])],
            read_data_key: Some([0xD5; 16]),
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

    // ── merge_content_key_ranges (whole-disc AACS map assembly) ────────────────
    use crate::decrypt::Phase;

    /// Ranges from different titles are sorted by start LBA and kept disjoint.
    #[test]
    fn merge_key_ranges_sorts_and_keeps_disjoint() {
        let v = vec![
            (500u32, 600u32, 1usize, Phase::All),
            (100, 200, 0, Phase::All),
            (300, 400, 2, Phase::All),
        ];
        assert_eq!(
            merge_content_key_ranges(v),
            vec![
                (100, 200, 0, Phase::All),
                (300, 400, 2, Phase::All),
                (500, 600, 1, Phase::All),
            ]
        );
    }

    /// A clip shared by two titles resolves the SAME span twice; the duplicate is
    /// dropped so `entry_for` sees a disjoint set (one key for the span).
    #[test]
    fn merge_key_ranges_dedups_shared_clip_span() {
        let v = vec![
            (100u32, 300u32, 0usize, Phase::All),
            (100, 300, 0, Phase::All),
        ];
        assert_eq!(merge_content_key_ranges(v), vec![(100, 300, 0, Phase::All)]);
    }

    /// A later range that partially overlaps a kept one carrying the SAME key is
    /// UNIONED, not dropped — the tail (400..500) must stay covered, or those
    /// encrypted LBAs would fall in no range and pass through as ciphertext.
    #[test]
    fn merge_key_ranges_unions_same_key_overlap() {
        let v = vec![
            (100u32, 400u32, 0usize, Phase::All),
            (200, 500, 0, Phase::All),
        ];
        assert_eq!(merge_content_key_ranges(v), vec![(100, 500, 0, Phase::All)]);
    }

    /// A different-key partial overlap (malformed disc) is dropped rather than
    /// unioned, so one unit key is never stretched over another key's LBAs; the set
    /// stays disjoint for `entry_for`.
    #[test]
    fn merge_key_ranges_drops_conflicting_key_overlap() {
        let v = vec![
            (100u32, 400u32, 0usize, Phase::All),
            (200, 500, 1, Phase::All),
        ];
        assert_eq!(merge_content_key_ranges(v), vec![(100, 400, 0, Phase::All)]);
    }

    /// Adjacent (touching) ranges are BOTH kept — `r.0 >= prev_end` holds when the
    /// next starts exactly at the previous end, so no coverage is lost.
    #[test]
    fn merge_key_ranges_keeps_adjacent() {
        let v = vec![
            (100u32, 200u32, 0usize, Phase::All),
            (200, 300, 1, Phase::All),
        ];
        assert_eq!(
            merge_content_key_ranges(v),
            vec![(100, 200, 0, Phase::All), (200, 300, 1, Phase::All)]
        );
    }

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
        // The honest-"Maybe" behaviour the rendered drilldown must preserve:
        // in-feature damage counts as movie time at risk; out-of-feature damage
        // is still located but reads 0:00. (Ported from autorip's old
        // RipProgress::from_map tests when that logic moved into the library.)
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

    /// Regression for branching-UHD title ordering. Mirrors the live
    /// observed *The Amateur (2025)* layout: a 4h13m / 92.4 GB / 253-clip
    /// virtual play-all playlist alongside the real 2h02m / 57.2 GB /
    /// 1-clip main feature. Disc capacity 58.5 GB. After sorting,
    /// titles[0] must be the main feature, not the virtual composite.
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

    /// Contract pin (owner-flagged): `freemkv -t 1` ALWAYS selects the main
    /// feature. The CLI's `-t 1` maps to `titles[0]`, and the title list is
    /// ordered by `canonical_title_order` (main feature first), so `titles[0]`
    /// IS the movie. Anything but the main feature at index 0 is a
    /// title-ordering bug, not a remux problem. DVD-shaped fixture (DVD-9
    /// capacity; a 1h49m main feature alongside a menu loop and a short extra).
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

    /// `read_mkb_content` reads a bounded 16 MiB starting prefix and must GROW
    /// it when the MKB's real record stream runs past that prefix — otherwise
    /// the caller silently gets a truncated MKB. This fixture's record stream
    /// is deliberately built so the first 16 records land EXACTLY on the 16
    /// MiB boundary (so the 16 MiB prefix alone parses as a clean, complete
    /// record stream — the case a naive "n < buf.len() means done" check would
    /// wrongly accept) and 4 more MiB of records follow, terminated by the
    /// explicit `00 000000` end marker. Only a caller that actually grows past
    /// 16 MiB recovers the true 20 MiB content length.
    ///
    /// Run inside a watchdog thread: some mutants of the growth step (e.g.
    /// `want / 2` instead of `want * 2`) walk `want` to 0 and spin forever
    /// re-reading a zero-length prefix, which must fail this test rather than
    /// hang the suite.
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

    /// A `ScsiTransport` that serves a synthetic UDF image (built with the
    /// shared `udf::fixture` helpers) through real READ CAPACITY(10) /
    /// READ(10) commands, so `Disc::identify` is exercised end-to-end through
    /// a real `Drive` rather than mocking `identify` itself — proving the
    /// AACS-directory check is actually what the caller uses, not just that
    /// `UdfFs::find_dir` works in isolation.
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

    /// A nested `/BDMV/AACS` directory alone must ALSO report
    /// `encrypted == true` — dropping this side of the `||` (degrading it to
    /// `&&`) would report virtually every retail BD as unencrypted, since
    /// real discs almost never carry both paths at once.
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

    /// Title selection is by largest physical size, NOT clip count or duration.
    /// Real-disc shape: a 57 GB / 11-clip feature must outrank both a
    /// small 1-clip bonus reel and a long-but-tiny decoy "play-all" (91 reused
    /// clips, 1h31m, 0.4 GB). The old clip-count-ascending key put the bonus t1.
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

    /// Regression for a real UHD title's decoy-playlist hang: the decoy `00245.mpls` is a
    /// WRAPPER COMPOSITE — `[00339 2s bumper][00001 the whole feature clip][00336
    /// outro]` — so its first-PlayItem STN reports v=0, and it is the LONGEST and
    /// LARGEST title (8350s / 78.4 GB vs the feature's 8038s / 76.5 GB). Muxing
    /// its zero frames produced E6008 forever. It must be demoted STRUCTURALLY
    /// (its clip set properly contains the feature's), not by the fragile
    /// first-PlayItem stream count — this is what makes the fix robust to the
    /// video-bearing-composite variant (see `main_feature_order_demotes_video_composite`).
    #[test]
    fn main_feature_order_demotes_wrapper_composite_decoy() {
        let decoy = bd_title_no_stn_video(
            "00245.mpls",
            245,
            8350.3,
            78_427_502_592,
            &["00339", "00001", "00336"],
        );
        let feature = bd_title("00001.mpls", 1, 8038.4, 76_525_627_392, &["00001"]);
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

    /// The O(n^2 · clips) composite scan is work-bounded: a crafted disc with a
    /// pathological title/clip count must skip the scan (degrade to has-video +
    /// canonical order) instead of stalling uncancellably. At a normal count the
    /// wrapper is still detected.
    #[test]
    fn composite_scan_is_work_bounded() {
        let decoy = bd_title(
            "00245.mpls",
            245,
            8350.0,
            78_000_000_000,
            &["dead", "00001"],
        );
        let feature = bd_title("00001.mpls", 1, 8000.0, 76_000_000_000, &["00001"]);
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

    /// Audit T2: a composite that ALSO carries video (its first PlayItem is the
    /// feature clip, so v>0) — the case the old `has-video` gate could not see —
    /// is still demoted below the standalone feature it wraps, by clip-set
    /// containment alone.
    #[test]
    fn main_feature_order_demotes_video_composite() {
        let composite = bd_title(
            "00245.mpls",
            245,
            8350.3,
            78_427_502_592,
            &["00001", "00336"],
        );
        let feature = bd_title("00001.mpls", 1, 8038.4, 76_525_627_392, &["00001"]);
        let capacity = 88_000_000_000u64;

        let mut titles = vec![composite, feature];
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, None);
        assert_eq!(
            titles[0].playlist_id, 1,
            "a video-bearing composite must still lose to the standalone feature it contains"
        );
    }

    /// Audit T3: capacity unknown (`0` — READ CAPACITY failed) disables the
    /// `fits-disc` oversize gate, so a video-bearing OVERSIZE play-all composite
    /// would win on size. Clip-set composite detection demotes it regardless of
    /// capacity.
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

    /// The disc's own HDMV navigation pick (`nav_feature`, resolved by the
    /// `bdnav` VM playing First-Play like a real player) wins outright — above
    /// the authoring hint and the physical size order — when the nav-resolved
    /// title has video.
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
        let feature = bd_title("00001.mpls", 1, 8000.0, 76_000_000_000, &["00001"]);
        let capacity = 88_000_000_000u64;

        let mut titles = vec![streamless, feature];
        // Nav (wrongly) points at the streamless 245 — must be ignored.
        Disc::sort_titles_by_main_feature(&mut titles, capacity, None, Some(245));
        assert_eq!(
            titles[0].playlist_id, 1,
            "a streamless nav target is ignored (video-gated), so the real feature wins"
        );
    }

    /// Regression (a real UHD title): the disc navigates to a Dolby-Vision
    /// seamless-branch playlist (`00001` = 102 tiny clips, 0.4 GB) that has real
    /// video streams but carries almost no payload, sitting beside the 91.5 GB
    /// single-clip feature (`00002`). A player plays the branch; a RIP wants the
    /// feature. The nav pick is video-gated but must ALSO clear the payload floor,
    /// so it falls through to size ranking and the substantial feature wins.
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

    /// Audit T7: when every title is streamless (whole-disc parse failure), the
    /// pick is still deterministic (largest) and the three pickers agree —
    /// `titles[0]` after the sort, `image_crack_extents`, and the diag `pick`
    /// (also `titles[0]`) name the same title (audit R9 single-source-of-truth).
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
                ..
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

    #[test]
    fn inject_unit_keys_labels_fmts_as_uhd_family() {
        // FMTS is AACS 2.1 — a UHD-family, bus-encrypted format. Injecting a UK
        // on an FMTS disc must synthesize the UHD version + bus encryption, not
        // mislabel it AACS 1.0 / bus-off (which would break FMTS decryption on
        // the mapfile-recovered-UK path).
        let mut disc = make_test_disc(1000, "FMTS");
        disc.format = DiscFormat::Fmts;
        disc.encrypted = true;
        disc.inject_unit_keys(vec![(0u32, [0x22u8; 16])]);
        let aacs = disc.aacs.as_ref().expect("aacs state synthesized");
        assert_eq!(
            aacs.version,
            crate::aacs::mkb::AACS_MAJOR_UHD,
            "FMTS is AACS 2.x (UHD major), not BD"
        );
        assert!(aacs.bus_encryption, "FMTS is bus-encrypted like UHD");
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
        // A minimal but parseable Unit_Key_RO.inf (uk_pos=32, zero unit keys)
        // so resolution proceeds to the path-try logic and fails for lack of a
        // VID — not because the .inf failed to parse.
        let mut uk_ro = vec![0u8; 40];
        uk_ro[0..4].copy_from_slice(&32u32.to_be_bytes()); // uk_pos = 32
        // num_unit_keys = 0 (BE16) at uk_pos -> parses to an empty key file.
        let ctx = crate::aacs::resolve::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16], // the "no VID" sentinel
            providers: &provider_refs,
            mkb: None,
        };
        assert_eq!(
            crate::aacs::resolve::resolve_keys_with_reason(&ctx, 2).err(),
            Some(crate::aacs::resolve::ResolveFailure::VidUnavailable),
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
        let provider_refs_none: [&dyn crate::aacs::provider::KeyProvider; 1] = [&supplied_none];
        let ctx_none = crate::aacs::resolve::ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers: &provider_refs_none,
            mkb: None,
        };
        assert_eq!(
            crate::aacs::resolve::resolve_keys_with_reason(&ctx_none, 2).err(),
            Some(crate::aacs::resolve::ResolveFailure::NoMaterial),
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

    /// THE defect: a key SOURCE that could not answer must not be reported as
    /// "this disc has no key".
    ///
    /// Drives the real chain end to end — `KeySource` → `resolve_and_apply_traced`
    /// → `Disc::aacs_error` → `ensure_decryptable` — twice over the SAME disc, and
    /// asserts the two operator-visible verdicts differ:
    ///
    /// * source returns `Err(KeyServiceUnavailable)` (the HTTP-502 outage) → E7028
    ///   "the key service could not answer; retry later",
    /// * source returns `Ok(vec![])` (the service answered, no entry) → E7022
    ///   "no key source has a decryption key for this disc".
    ///
    /// Before the fix both arms produced E7022, which is what sent an operator
    /// hunting for a VUK through seven hours of 502s. The credential and
    /// rate-limit verdicts are asserted alongside, since each is a different
    /// operator action (fix the token / back off).
    #[test]
    fn key_source_failure_is_not_reported_as_a_missing_disc_key() {
        use crate::keysource::{KeySource, ResolveCtx, resolve_and_apply_traced};

        /// A source that fails the way a down / hostile key service fails.
        struct FailingSource(fn() -> crate::error::Error);
        impl KeySource for FailingSource {
            fn get_unit_keys(
                &self,
                _ctx: &dyn ResolveCtx,
            ) -> std::result::Result<Vec<crate::aacs::types::UnitKey>, crate::error::Error>
            {
                Err((self.0)())
            }
            fn label(&self) -> &'static str {
                "online"
            }
        }
        /// A source that ANSWERS and holds nothing — the genuine miss.
        struct AnsweredNoEntry;
        impl KeySource for AnsweredNoEntry {
            fn get_unit_keys(
                &self,
                _ctx: &dyn ResolveCtx,
            ) -> std::result::Result<Vec<crate::aacs::types::UnitKey>, crate::error::Error>
            {
                Ok(Vec::new())
            }
            fn label(&self) -> &'static str {
                "online"
            }
        }

        let inputs = crate::keysource::DiscInputs {
            disc_hash: "0x422EB".into(),
            volume_id: [0u8; 16],
            version: crate::aacs::mkb::AACS_MAJOR_UHD,
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        let encrypted_aacs_disc = || {
            let mut d = make_test_disc(1000, "UHD");
            d.encrypted = true;
            d.aacs = Some(aacs_with(Vec::new())); // AACS state, no unit keys
            d
        };

        // The genuine miss — the service answered, nothing for this disc.
        let mut answered = encrypted_aacs_disc();
        let sources: Vec<Box<dyn KeySource>> = vec![Box::new(AnsweredNoEntry)];
        let (ok, trace) = resolve_and_apply_traced(&sources, &inputs, &mut answered);
        assert!(!ok);
        assert_eq!(
            trace.keys[0].path,
            vec![crate::aacs::trace::KeyNode::NoEntry],
            "a source that ANSWERED and holds nothing is the one true `NoEntry`"
        );
        assert!(
            answered.aacs_error.is_none(),
            "a genuine miss stamps no failure reason on the disc"
        );
        let miss_code = answered
            .ensure_decryptable(false)
            .expect_err("no key, !raw must error")
            .code();
        assert_eq!(
            miss_code,
            crate::error::Error::NoDiscKey {
                disc_hash: String::new()
            }
            .code(),
            "a genuine miss keeps E7022 — that wording is correct for it"
        );

        // Each way the service can FAIL to answer, and the code it must produce.
        type MakeError = fn() -> crate::error::Error;
        let failures: &[(MakeError, u16)] = &[
            (
                || crate::error::Error::KeyServiceUnavailable,
                crate::error::E_KEY_SERVICE_UNAVAILABLE,
            ),
            (
                || crate::error::Error::KeyServiceUnauthorized,
                crate::error::E_KEY_SERVICE_UNAUTHORIZED,
            ),
            (
                || crate::error::Error::KeyServiceRateLimited,
                crate::error::E_KEY_SERVICE_RATE_LIMITED,
            ),
        ];
        for (make, want) in failures {
            let mut down = encrypted_aacs_disc();
            let sources: Vec<Box<dyn KeySource>> = vec![Box::new(FailingSource(*make))];
            let (ok, trace) = resolve_and_apply_traced(&sources, &inputs, &mut down);
            assert!(!ok);
            assert!(
                trace.keys[0].path.is_empty(),
                "a source that could not ANSWER must not claim `no entry` in the trace"
            );
            assert_eq!(
                down.aacs_error.as_ref().map(crate::error::Error::code),
                Some(*want),
                "the source's failure reason must reach the disc"
            );
            let code = down
                .ensure_decryptable(false)
                .expect_err("no key, !raw must error")
                .code();
            assert_eq!(code, *want, "the gate must surface the SOURCE's code");
            assert_ne!(
                code, miss_code,
                "a source that could not answer must NOT render as \"this disc has no key\""
            );
        }
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
    /// `css_error` is Some — the disc IS encrypted. The gate must fail rather
    /// than read `css.is_none()` as "unencrypted", and with the DISC-LEVEL
    /// `CssNoDiscKey` (not the per-title, skippable `CssKeyMissing`): `css_error`
    /// reflects the main feature's crack, so every title fails identically.
    #[test]
    fn ensure_decryptable_css_error_errors() {
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css_error = Some(crate::error::Error::CssKeyMissing);
        let err = disc
            .ensure_decryptable(false)
            .expect_err("scrambled-but-uncracked CSS must error");
        assert_eq!(err.code(), crate::error::Error::CssNoDiscKey.code());
        // --raw is exempt.
        assert!(disc.ensure_decryptable(true).is_ok());
    }

    /// The two CSS no-key conditions are NOT the same verdict and must classify
    /// oppositely through the public predicates:
    ///
    /// - **disc-wide** — `css_error` is set: the MAIN feature's crack failed, so
    ///   every title of this disc fails identically. Must be
    ///   [`crate::error::is_disc_level_no_key`] (the rip loop fail-fasts) and
    ///   must NOT be [`crate::error::is_skippable_title_stub`]. While both
    ///   conditions shared `E_CSS_KEY_MISSING`, an uncrackable CSS disc iterated
    ///   all N titles logging "title skipped" and exited 0 — a total failure
    ///   reported as success.
    /// - **per-title** — one title's own re-crack failed on a multi-VTS disc
    ///   (`title_is_clear == false`, no key): skipping it and finishing the rest
    ///   is correct policy, so it must STAY skippable and must NOT be disc-level.
    ///
    /// Pinned in both directions so a future change cannot silently flip either.
    #[test]
    fn css_disc_wide_no_key_is_disc_level_while_per_title_stays_skippable() {
        // Disc-wide: the scan saw scrambled sectors and recovered no key.
        let mut disc = make_test_disc(1000, "DVD");
        disc.encrypted = true;
        disc.css_error = Some(crate::error::Error::CssKeyMissing);
        let wide: std::io::Error = disc
            .ensure_decryptable(false)
            .expect_err("scrambled-but-uncracked CSS disc must error")
            .into();
        assert!(
            crate::error::is_disc_level_no_key(&wide),
            "a whole-disc CSS crack failure must classify as disc-level: {wide}"
        );
        assert!(
            !crate::error::is_skippable_title_stub(&wide),
            "a whole-disc CSS crack failure must NOT be a skippable title stub: {wide}"
        );

        // Per-title: this title's VTS could not be re-cracked; the rest of the
        // disc may still rip.
        let (stub_disc, _) = css_disc_with_clear_stub();
        let per_title: std::io::Error = stub_disc
            .ensure_title_decryptable(false, &crate::decrypt::DecryptKeys::None, false)
            .expect_err("scrambled-uncracked title must error")
            .into();
        assert!(
            crate::error::is_skippable_title_stub(&per_title),
            "a per-title CSS re-crack failure must stay skippable: {per_title}"
        );
        assert!(
            !crate::error::is_disc_level_no_key(&per_title),
            "a per-title CSS re-crack failure must NOT stop the whole rip: {per_title}"
        );
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
    /// CSS-locked errors elsewhere — enough to drive `decrypt_keys_for_title`'s
    /// per-title re-crack to `Unencrypted` for a clear stub.
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
        disc.format = DiscFormat::Dvd; // make_test_disc defaults to Uhd
        disc.content_format = ContentFormat::MpegPs;
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

    /// A genuinely-clear extra title (an unencrypted menu stub in its own VTS)
    /// on a CSS DVD must mux without a false E7023. The stub lives in a DISJOINT
    /// VTS (its extents don't overlap the scan's `crack_span`), so
    /// `decrypt_keys_for_title` takes the crack path over the stub's own extents;
    /// the reader serves only clear sectors, so the crack returns `Unencrypted`
    /// → `(None, title_is_clear=true)`. The gate must then PASS the title with no
    /// key — no false E7023.
    #[test]
    fn clear_stub_title_on_css_disc_is_not_a_key_failure() {
        let (disc, stub_idx) = css_disc_with_clear_stub();
        assert_eq!(
            disc.format,
            DiscFormat::Dvd,
            "fixture must exercise the DVD path"
        );
        let mut reader = ClearStubReader {
            clear_range: (0, 100_000),
        };
        let (keys, title_is_clear) = disc.decrypt_keys_for_title(stub_idx, &mut reader, 8);
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None),
            "a clear stub in a disjoint VTS cracks to no key"
        );
        assert!(title_is_clear, "the stub's own extents show no scrambling");
        // The gate must PASS a clear title — NO false E7023.
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
        let exp0 = crate::aacs::derive::decrypt_unit_key(&vuk, &enc0);
        let exp1 = crate::aacs::derive::decrypt_unit_key(&vuk, &enc1);

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
        use crate::aacs::content::ALIGNED_UNIT_LEN;

        // No samples -> nothing to disprove against -> accept (sample-less paths
        // like resume / mapfile must be unaffected).
        assert!(super::aligned_unit_keys_validate(
            &[(0, [0x11u8; 16])],
            None,
            &[],
            ContentFormat::BdTs
        ));

        // A clear unit (TS syncs intact) is not scrambled -> proves nothing ->
        // accept even with an arbitrary key.
        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            clear[off] = 0x47;
            off += 192;
        }
        assert!(crate::aacs::content::is_clean(
            &clear,
            crate::disc::ContentFormat::BdTs
        ));
        assert!(super::aligned_unit_keys_validate(
            &[(0, [0x11u8; 16])],
            None,
            &[clear.clone()],
            ContentFormat::BdTs
        ));

        // A genuinely scrambled unit the RIGHT key restores to clear TS.
        let uk = [0x5au8; 16];
        let enc = encrypt_unit_for_test(&clear, &uk);
        assert!(
            !crate::aacs::content::is_clean(&enc, crate::disc::ContentFormat::BdTs),
            "encrypted unit must read scrambled"
        );

        // Right key -> de-scrambles -> accept (NO false reject of a good key).
        assert!(super::aligned_unit_keys_validate(
            &[(7, uk)],
            None,
            std::slice::from_ref(&enc),
            ContentFormat::BdTs
        ));
        // Wrong key -> cannot de-scramble a scrambled sample -> reject.
        assert!(!super::aligned_unit_keys_validate(
            &[(7, [0x00u8; 16])],
            None,
            std::slice::from_ref(&enc),
            ContentFormat::BdTs
        ));
        // Empty key set against a scrambled sample -> reject.
        assert!(!super::aligned_unit_keys_validate(
            &[],
            None,
            &[enc],
            ContentFormat::BdTs
        ));
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
        use crate::aacs::content::ALIGNED_UNIT_LEN;

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
        assert!(!crate::aacs::content::is_clean(
            &sample0,
            crate::disc::ContentFormat::BdTs
        ));
        assert!(!crate::aacs::content::is_clean(
            &sample1,
            crate::disc::ContentFormat::BdTs
        ));

        let samples = vec![sample0.clone(), sample1.clone()];

        // Partial key set (CPS unit 0 only) against samples from BOTH units ->
        // reject. This is the bug fix: previously this returned true.
        assert!(!super::aligned_unit_keys_validate(
            &[(0, uk0)],
            None,
            &samples,
            ContentFormat::BdTs
        ));

        // Complete key set (both CPS units) -> accept.
        assert!(super::aligned_unit_keys_validate(
            &[(0, uk0), (1, uk1)],
            None,
            &samples,
            ContentFormat::BdTs
        ));

        // Order-independent: covering key present anywhere in the set is fine.
        assert!(super::aligned_unit_keys_validate(
            &[(1, uk1), (0, uk0)],
            None,
            &samples,
            ContentFormat::BdTs
        ));
    }

    /// Inverse of `decrypt_unit` for one 6144-byte unit: produce on-disc
    /// ciphertext that `decrypt_unit(uk)` restores to `clear`. Mirrors the AACS
    /// unit algorithm — ECB-derive the per-unit key, then AES-CBC encrypt the
    /// body with the fixed AACS IV.
    fn encrypt_unit_for_test(clear: &[u8], uk: &[u8; 16]) -> Vec<u8> {
        use crate::aacs::content::ALIGNED_UNIT_LEN;
        use crate::aacs::crypto::AACS_IV;
        use aes::Aes128;
        use aes::cipher::{Array, BlockCipherEncrypt, KeyInit};
        let mut unit = clear[..ALIGNED_UNIT_LEN].to_vec();
        // Flag the unit encrypted (CPI bits on byte 0) before key derivation so
        // the recovered plaintext header matches and `decrypt_unit`'s CPI gate
        // attempts the decrypt.
        unit[0] |= 0xC0;
        let mut header = [0u8; 16];
        header.copy_from_slice(&unit[..16]);
        let cipher = Aes128::new(&(*uk).into());
        let mut blk: Array<u8, _> = header.into();
        cipher.encrypt_block(&mut blk);
        let mut dk = [0u8; 16];
        for i in 0..16 {
            dk[i] = blk[i] ^ header[i];
        }
        let bc = Aes128::new(&dk.into());
        let mut prev = AACS_IV;
        let mut i = 16;
        while i + 16 <= ALIGNED_UNIT_LEN {
            let mut b = [0u8; 16];
            for j in 0..16 {
                b[j] = unit[i + j] ^ prev[j];
            }
            let mut g: Array<u8, _> = b.into();
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

    /// Build a crackable scrambled CSS sector (a periodic run in the
    /// clear header continuing past 0x80), mirroring the css-module fixture.
    fn crackable_css_sector(title_key: &[u8; 5]) -> [u8; 2048] {
        const RUN_START: usize = 0x59;
        const PERIOD: usize = 8;
        let mut sec = [0u8; 2048];
        sec[0x00..0x04].copy_from_slice(&crate::css::PACK_START);
        sec[0x14] = 0x10; // scramble flag
        for (i, b) in sec.iter_mut().enumerate().skip(RUN_START) {
            *b = (0xA0u8.wrapping_add((i % PERIOD) as u8)) ^ 0x5A;
        }
        crate::css::lfsr::scramble_sector(title_key, &mut sec);
        sec
    }

    /// A reader that serves crackable CSS sectors for LBAs in `scrambled`
    /// (half-open), all-zero (clear) elsewhere — records every LBA read.
    struct CssMapReader {
        key: [u8; 5],
        scrambled: (u32, u32),
        reads: std::cell::RefCell<Vec<u32>>,
    }
    impl SectorSource for CssMapReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.reads.borrow_mut().push(lba);
            let n = (count as usize * 2048).min(buf.len());
            for s in 0..(n / 2048) {
                let this = lba + s as u32;
                let dst = &mut buf[s * 2048..(s + 1) * 2048];
                if this >= self.scrambled.0 && this < self.scrambled.1 {
                    dst.copy_from_slice(&crackable_css_sector(&self.key));
                } else {
                    dst.fill(0);
                }
            }
            Ok(n)
        }
    }

    fn css_dvd_with_extents(extents: Vec<Extent>) -> Disc {
        let mut disc = make_test_disc(200_000, "DVD");
        disc.format = DiscFormat::Dvd;
        disc.content_format = ContentFormat::MpegPs;
        disc.encrypted = true;
        let mut t = title_with_video(Codec::Mpeg2, Resolution::R480p);
        t.extents = extents;
        disc.titles = vec![t];
        disc
    }

    /// `decrypt_keys_for_title` cracks a scrambled DVD title's key from the
    /// title's OWN extents and hands the mux the validated key — the seed the
    /// descramble needs, since a crib-less sector cannot self-crack and CSS leaves
    /// the pack/PES header clear (an un-seeded mux would emit corrupt PES).
    #[test]
    fn decrypt_keys_for_title_cracks_the_titles_key() {
        let key = [0x11, 0x22, 0x33, 0x44, 0x55];
        let disc = css_dvd_with_extents(vec![Extent {
            start_lba: 100,
            sector_count: 64,
        }]);
        let mut src = CssMapReader {
            key,
            scrambled: (100, 164),
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let (keys, title_is_clear) = disc.decrypt_keys_for_title(0, &mut src, 16);
        assert!(!title_is_clear, "a scrambled title is not clear");
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => {
                assert_eq!(title_key, key, "must crack the title's own key")
            }
            _ => panic!("expected Css{{key}} for a scrambled DVD title"),
        }
    }

    /// REGRESSION (the 1.5.1 garbage bug): the crack scans extents in PLAYBACK
    /// ORDER, never largest-cell-first. A title whose LARGEST cell opens with a
    /// long unscrambled run must still crack its key from the smaller,
    /// scrambled-early cell that plays first — largest-first would exhaust the
    /// crack budget in the clear giant and wrongly report the title unencrypted,
    /// which the mux would pass through as scrambled garbage.
    #[test]
    fn decrypt_keys_for_title_scans_playback_order_not_largest_first() {
        let key = [0xDE, 0xAD, 0xBE, 0xEF, 0x01];
        let disc = css_dvd_with_extents(vec![
            // Plays FIRST: small, scrambled from its start.
            Extent {
                start_lba: 100,
                sector_count: 32,
            },
            // A CLEAR cell far larger than the crack budget (would starve a
            // largest-first scan before it reached the scrambled cell above).
            Extent {
                start_lba: 10_000,
                sector_count: 100_000,
            },
        ]);
        let mut src = CssMapReader {
            key,
            scrambled: (100, 132),
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let (keys, _) = disc.decrypt_keys_for_title(0, &mut src, 16);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => assert_eq!(
                title_key, key,
                "must crack from the scrambled cell that plays first, not miss it behind the clear giant"
            ),
            _ => panic!("largest-first regression: the title was read as unencrypted"),
        }
        assert!(
            src.reads.borrow().iter().all(|&l| l < 10_000),
            "the key is found in the first (scrambled) cell — the clear giant must never be scanned: {:?}",
            src.reads.borrow()
        );
    }

    /// A reader whose every read is CSS-locked (`05/6F/03`) — a genuinely
    /// encrypted DVD whose sectors can't be authenticated/cracked.
    struct LockedReader;
    impl SectorSource for LockedReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            Err(Error::DiscRead {
                sector: lba as u64,
                status: Some(2),
                sense: Some(crate::scsi::ScsiSense {
                    sense_key: 0x05,
                    asc: 0x6F,
                    ascq: 0x03,
                }),
            })
        }
    }

    /// End-to-end: a scrambled-but-uncrackable DVD title with NO up-front
    /// detection (`self.css == None`) drives `decrypt_keys_for_title` to
    /// `(None, false)`, and the gate MUST hard-fail (CssKeyMissing) rather than
    /// pass it to the muxer — the silent-garbage case the per-title crack catches.
    #[test]
    fn decrypt_keys_for_title_scrambled_uncracked_dvd_hard_fails_even_without_detection() {
        let disc = css_dvd_with_extents(vec![Extent {
            start_lba: 100,
            sector_count: 8,
        }]);
        assert!(disc.css.is_none(), "fixture: no up-front detection");
        let mut reader = LockedReader;
        let (keys, title_is_clear) = disc.decrypt_keys_for_title(0, &mut reader, 8);
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None) && !title_is_clear,
            "a locked/uncrackable scrambled title resolves to (None, false)"
        );
        let err = disc
            .ensure_title_decryptable(false, &keys, title_is_clear)
            .expect_err("scrambled-uncracked DVD title must hard-fail without detection");
        assert_eq!(err.code(), crate::error::Error::CssKeyMissing.code());
    }

    /// Fast path: when the scan already cracked a key whose `crack_span` COVERS
    /// this title's VTS, `decrypt_keys_for_title` reuses it and never touches the
    /// reader (no redundant crack, no second bus-auth on a live drive).
    #[test]
    fn decrypt_keys_for_title_reuses_covered_scan_key_without_reading() {
        let disc = css_disc_with_two_vts(); // css=[0xAB;5], crack_span=(100,200)
        let mut src = RecordingSource {
            reads: std::cell::RefCell::new(Vec::new()),
        };
        // Title 0's extents (100..200) overlap the cracked span → reuse.
        let (keys, clear) = disc.decrypt_keys_for_title(0, &mut src, 16);
        assert!(!clear);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => {
                assert_eq!(title_key, [0xAB; 5], "reuse the scan's cracked key")
            }
            _ => panic!("expected the reused Css key"),
        }
        assert!(
            src.reads.borrow().is_empty(),
            "a covered title must NOT re-read/re-crack: {:?}",
            src.reads.borrow()
        );
    }

    /// A title in a DIFFERENT VTS (extents disjoint from `crack_span`) does NOT
    /// reuse the scan key — it cracks its own key from its own extents.
    #[test]
    fn decrypt_keys_for_title_cracks_other_vts_on_no_overlap() {
        let key = [0x77, 0x66, 0x55, 0x44, 0x33];
        let disc = css_disc_with_two_vts(); // title 1 lives at 5000.., span=(100,200)
        let mut src = CssMapReader {
            key,
            scrambled: (5000, 5100),
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let (keys, _) = disc.decrypt_keys_for_title(1, &mut src, 16);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => assert_eq!(
                title_key, key,
                "a disjoint-VTS title cracks its OWN key, not the reused scan key"
            ),
            _ => panic!("expected a freshly-cracked Css key for the other VTS"),
        }
        assert!(
            src.reads.borrow().iter().all(|&l| l >= 5000),
            "must crack from title 1's own extents (>=5000): {:?}",
            src.reads.borrow()
        );
    }

    /// A title whose (realistic) clear front matter — studio logo / rating card —
    /// plays FIRST, then the scrambled feature, still cracks: the single
    /// playback-order scan reads through the small clear prefix and reaches the
    /// scrambled body within its budget. (A clear prefix LARGER than the ~100 MB
    /// crack budget would starve — the accepted bounded-budget limit, identical to
    /// the disc-wide scan; not producible by real DVD front matter.)
    #[test]
    fn decrypt_keys_for_title_cracks_feature_after_clear_front_matter() {
        let key = [0xCA, 0xFE, 0xBA, 0xBE, 0x02];
        // css=None so the crack path runs. ~10 MB of clear front matter plays
        // first (well under the crack budget), then the scrambled feature.
        let disc = css_dvd_with_extents(vec![
            Extent {
                start_lba: 10_000,
                sector_count: 5_000,
            }, // clear front matter (~10 MB), plays first
            Extent {
                start_lba: 100,
                sector_count: 2_000,
            }, // scrambled feature body
        ]);
        let mut src = CssMapReader {
            key,
            scrambled: (100, 2_100),
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let (keys, _) = disc.decrypt_keys_for_title(0, &mut src, 16);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => assert_eq!(
                title_key, key,
                "must crack the scrambled feature after reading through clear front matter"
            ),
            _ => panic!("clear front matter wrongly starved the crack"),
        }
    }

    /// Scrambling that begins well INTO a cell (after a clear prefix), not at its
    /// start, must still be cracked: the single playback-order scan reads through
    /// the clear prefix and reaches the scrambled body within its budget — never a
    /// silent "clear" verdict that would mux the scrambled tail as corrupt PES.
    #[test]
    fn decrypt_keys_for_title_cracks_scrambling_after_a_clear_prefix_in_one_cell() {
        let key = [0x0D, 0xEE, 0x40, 0x00, 0x05];
        // One cell: clear for the first 9000 sectors, then scrambled (well within
        // the crack budget). css=None so the crack path runs.
        let disc = css_dvd_with_extents(vec![Extent {
            start_lba: 100,
            sector_count: 20_000,
        }]);
        let mut src = CssMapReader {
            key,
            scrambled: (100 + 9_000, 100 + 20_000),
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let (keys, _) = disc.decrypt_keys_for_title(0, &mut src, 16);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => assert_eq!(
                title_key, key,
                "the scan must crack scrambling that starts past a clear prefix"
            ),
            _ => panic!("in-cell-deep scrambling was misread as clear (silent-garbage direction)"),
        }
    }

    /// A DVD title with EMPTY extents (an angle/PGC placeholder with no cells)
    /// resolves to `(decrypt_keys(), true)` — clear, no key needed — and the gate
    /// must PASS it. Returning `false` here would trip the DVD scrambled-uncracked
    /// rule and wrongly hard-fail a genuinely-clear empty title.
    #[test]
    fn decrypt_keys_for_title_empty_extents_is_clear_not_hard_fail() {
        let mut disc = css_dvd_with_extents(vec![Extent {
            start_lba: 100,
            sector_count: 8,
        }]);
        disc.titles
            .push(title_with_video(Codec::Mpeg2, Resolution::R480p)); // idx 1: no extents
        let mut reader = LockedReader;
        let (keys, title_is_clear) = disc.decrypt_keys_for_title(1, &mut reader, 8);
        assert!(
            title_is_clear,
            "an empty-extents title is clear (nothing to descramble)"
        );
        assert!(
            disc.ensure_title_decryptable(false, &keys, title_is_clear)
                .is_ok(),
            "an empty-extents DVD title must not hard-fail"
        );
    }

    /// A bonus title that cracked its OWN valid key must NOT be blocked by the
    /// disc-wide `css_error` set when the MAIN feature's scan failed. A usable
    /// per-title key means the title is decryptable regardless of another title's
    /// failure. (Regression for the audit r5 css_error-over-valid-key finding.)
    #[test]
    fn ensure_title_decryptable_valid_key_ignores_disc_wide_css_error() {
        let mut disc = css_dvd_with_extents(vec![Extent {
            start_lba: 100,
            sector_count: 8,
        }]);
        disc.css_error = Some(crate::error::Error::CssKeyMissing); // main feature failed
        let keys = crate::decrypt::DecryptKeys::Css {
            title_key: [0x42; 5], // this bonus title cracked its own key
        };
        assert!(
            disc.ensure_title_decryptable(false, &keys, false).is_ok(),
            "a title with its own valid CSS key must pass despite disc-wide css_error"
        );
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

    // (The former `coding_type_a2_is_dts_hd_ma` asserted the DEFECT — that
    // BD-ROM Part 3 code 0xA2 is lossless Master Audio. It is the lossy
    // secondary stream; see `secondary_dts_hd_0xa2_is_lossy_not_master_audio`,
    // which now covers both 0xA2 and the 0x86 primary.)

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

    // ── correct_truehd_channels ──────────────────────────────────────────

    /// Records every `read_sectors` call and serves a fixed byte buffer
    /// (zero-padded to the requested size) — probes
    /// `correct_truehd_channels`'s early-return guards (empty pid list, `n ==
    /// 0`) without needing real TrueHD content, and carries real synthetic
    /// TrueHD bytes for the full round-trip tests below.
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

    /// No TrueHd stream in the title → the pid list is empty and the probe
    /// must return before ever touching the reader (extent/read-count guards
    /// are irrelevant once there's nothing to probe for). Mutation guard:
    /// `matches!(a.codec, Codec::TrueHd)` flipped to `true` would sweep this
    /// title's non-TrueHD stream's pid into the probe list too, and it would
    /// read the (spied) source.
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

    /// A TrueHd stream IS present → the probe must proceed past the pid-list
    /// guard and actually read the title's first extent. Mutation guard:
    /// `matches!(a.codec, Codec::TrueHd)` flipped to `false` would empty the
    /// pid list even here and the probe would return before ever reading.
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

    /// The bounded-probe sector count is `ext.sector_count.min(4096)`; when the
    /// extent has ZERO sectors that count is zero and there is nothing to read
    /// — the probe must return before calling into the reader. Mutation guard:
    /// `n == 0` flipped to `n != 0` inverts this so a zero-sector extent
    /// wrongly falls through to a (zero-length) read.
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

    /// Full round-trip: a real major sync carrying a 7.1 (8ch) presentation, a
    /// whitelisted 96 kHz rate nibble, and an Atmos substream count, probed
    /// through a container-declared 5.1/48 kHz basic-descriptor stream. All
    /// three corrections must land, and the label must be promoted to the
    /// Atmos form (the stream still carried the basic, non-editorial label).
    /// Kills the was_basic `==`, the channels/rate `!=`/`&&` guards' "already
    /// correct" branch, the `!matches!` per-stream skip, the `is_atmos ==
    /// Some(true)` branch, and the whole-function no-op mutant.
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

    /// A major sync whose 8ch/6ch presentation masks are BOTH set to values
    /// with no real channel-count meaning (all 13 8ch bits, summing to 20) —
    /// `AudioChannels::from_count` maps that to `Unknown`. The correction must
    /// leave the container's channel count untouched rather than overwrite a
    /// known-good value with `Unknown`. Kills the `new_ch != Unknown` guard's
    /// `==` and `&&`-to-`||` mutants (both would let an unmapped count
    /// clobber a valid `a.channels`).
    #[test]
    fn correct_truehd_channels_leaves_channels_when_count_unmapped() {
        let pid = 0x1100u16;
        // All 13 8ch bits set -> truehd_channels sums to 20 -> from_count(20)
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

    /// A major sync whose rate nibble (`format_info` bits 31..28) is `0x3` —
    /// not one of the six whitelisted rates `truehd_sample_rate_hz` recognises
    /// — must leave the container's sample rate untouched, mirroring the
    /// channels guard above ("never write a wrong SamplingFrequency"). Both
    /// channel-assignment masks are zeroed so `truehd_channels` returns `None`
    /// too, isolating this to the sample-rate guard alone.
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

    // ── bytes_bad_in_title: empty-input guard ────────────────────────────

    // NOTE: `bad_ranges.is_empty() || title.extents.is_empty()` (mod.rs:628) —
    // the `||`-to-`&&` mutant is EQUIVALENT here, not tested: the guard is a
    // pure short-circuit. Whichever operand is empty, the corresponding loop
    // (the outer `for ext in &title.extents` or the inner `for (pos, size) in
    // bad_ranges`) simply iterates zero times and `total` stays its initial
    // 0 — the early return changes nothing observable. See report.

    // ── byte_offset_in_title ──────────────────────────────────────────────

    fn title_with_size(size_bytes: u64, extents: Vec<Extent>) -> DiscTitle {
        DiscTitle {
            size_bytes,
            extents,
            ..DiscTitle::empty()
        }
    }

    /// A multi-extent title where the target LBA lands in the SECOND extent.
    /// Exercises both the boundary check for the FIRST (non-matching) extent
    /// and the running `cumulative` byte total added on the way past it.
    /// Kills: `lba >= start && lba < end` flipped to `||` (the first extent's
    /// disjunction would trivially match almost any lba and return the wrong,
    /// too-early offset); `cumulative +=` flipped to `*=` (cumulative is
    /// seeded at 0, so `*=` freezes it at 0 forever); and `sector_count *
    /// SECTOR_BYTES_U64` flipped to `+` or `/` (wrong per-extent byte length
    /// folded into cumulative).
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

    /// Concrete end-to-end arithmetic check: byte_offset 60/100 of a 100s
    /// title lands at t=60s, which is chapter index 1 (0-based, the last
    /// chapter whose start <= 60) → 1-based chapter 2. Kills: the `/`
    /// (time-fraction) flipped to `%` or `*`; the `*` (duration scale)
    /// flipped to `+`; the `<=` chapter-scan comparison flipped to `>`; the
    /// final `chapter_idx + 1` flipped to `-` or `*`; and every
    /// whole-function fixed-tuple replacement (none produce `(2, 60.0)`).
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

    /// `total_bytes == 0` must short-circuit to `None` (no title size to
    /// compute a fraction against) regardless of whether chapters exist.
    /// Kills `total_bytes == 0` flipped to `!=`, and (combined with the next
    /// test) the `||` flipped to `&&`.
    #[test]
    fn chapter_at_offset_zero_total_bytes_is_none() {
        let chapters = three_chapters();
        assert_eq!(
            chapter_at_offset(&chapters, 10, 100.0, 0),
            None,
            "a title with no declared size has no byte-fraction to place a chapter at"
        );
    }

    /// No chapters declared → `None`, even with a perfectly valid nonzero
    /// title size. Kills the `||` flipped to `&&` (which would let this case
    /// fall through the guard and return a bogus `Some((1, ..))` from the
    /// then-empty scan loop).
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

    /// Concrete positive case chaining `byte_offset_in_title` +
    /// `chapter_at_offset`: lba 1060 is 60 sectors (122_880 bytes) into the
    /// extent, 60% of the 204_800-byte title → t=60s → chapter 2. This exact
    /// non-default tuple kills every fixed-tuple whole-function replacement
    /// mutant (`(None, None)`, `(Some(0), ..)`, `(Some(1), ..)`, etc. — none
    /// equal `(Some(2), Some(60.0))`).
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

    /// Isolates the per-range `lba`/`count` sector-arithmetic from every
    /// bps-dependent branch (duration_secs is negative, so `bps` is 0.0
    /// under both the real `>` and any surviving `>=`/`==`/`<` mutant at that
    /// same guard). Non-power-coincidental `pos`/`size` so `/` vs `%` vs `*`
    /// all disagree with the expected quotient. Kills `pos / SECTOR_BYTES_U64`
    /// and `size / SECTOR_BYTES_U64` each flipped to `%` or `*`.
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

    /// Concrete positive-`bps` arithmetic for both `duration_ms` (per-range)
    /// and `main_at_risk_ms` (title-wide): bps = 204_800 B / 100 s = 2048
    /// B/s exactly. A 4096-byte range = 2 sectors = 2000 ms at that rate,
    /// and it's entirely inside the title's only extent so all of it counts
    /// toward `main_at_risk_ms` too. Kills `bps > 0.0` flipped to `==`/`<`
    /// (both would take the `else 0.0` branch here, wrongly reporting 0);
    /// `(*size as f64) / bps` flipped to `%` or `*`; and the trailing
    /// `* MILLIS_PER_SEC` flipped to `+` or `/`.
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

    /// `bps` computed exactly `0.0` (duration_secs == 0.0): both the
    /// per-range `duration_ms` and title-wide `main_at_risk_ms` must stay
    /// `0.0`, never a divide-by-zero `inf`/`NaN`. Kills `bps > 0.0` flipped
    /// to `>=` at BOTH sites (mod.rs:743 and mod.rs:768) — with the boundary
    /// exactly zero, `>=` wrongly takes the division branch and produces
    /// `inf` instead of the real code's `0.0`.
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

    // NOTE: mod.rs:732 (`title.duration_secs > 0.0` seeding `bps`) — the
    // `>`-to-`>=` mutant is EQUIVALENT. `bps` is only ever consumed behind
    // its own `bps > 0.0` re-check at both use sites (mod.rs:743, 768); at
    // the exact boundary (`duration_secs == 0.0`) the mutant instead computes
    // `bps = size_bytes / 0.0` (`inf` or `NaN` since `size_bytes >= 0`), and
    // `inf > 0.0` / `NaN > 0.0` are both `false` at the re-check — so the
    // final output (`0.0` via the `else` branch) is identical either way. No
    // reachable input makes this observable.

    // ── Codec::name / Display ────────────────────────────────────────────

    /// `name()` is a linear lookup keyed by `==` against `ALL_CODECS`; picking
    /// a codec that is NOT the first table entry means a mutated `==`→`!=`
    /// returns the (wrong) first entry's name instead. `Unknown` isn't in the
    /// table at all, exercising the post-loop fallback.
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

    /// Every from_height bucket boundary — deleting any one match arm makes
    /// its heights fall through to the NEXT surviving arm (a strictly
    /// different variant), so each of these pairs (top of one bucket, bottom
    /// of the next) pins the arm to its own boundary.
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

    // ── SampleRate::from_hz ───────────────────────────────────────────────

    /// Every rate the enum can represent, expressed in Hz. Deleting any one
    /// match arm sends that rate to `_ => Unknown`, so a rate-by-rate check
    /// pins each arm independently. The two combo rates (`S48_96`,
    /// `S48_192`) deliberately have no Hz spelling — `hz()` collapses them
    /// onto 48000, so 48000 must map back to the plain `S48` and nothing else.
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

    /// `from_hz` must invert `hz()` for every rate that has a single Hz
    /// value (i.e. all but the two combo rates, whose `hz()` reports their
    /// primary 48 kHz). Round-tripping rather than restating the table keeps
    /// this honest if a rate is ever added.
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

    /// `FromStr` accepts the human display names as well as the compact ids,
    /// via a second linear scan keyed on `name(v) == s`. Every probe here is
    /// a display name that is NOT its own id, so it can only be resolved by
    /// that second scan — and none of them is the first table entry, so a
    /// scan whose comparison is inverted returns the wrong (first) variant
    /// rather than the right one.
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

    /// A title whose stream list interleaves all three kinds. Each accessor
    /// must yield exactly its own kind, in declared order — an accessor that
    /// yields nothing (or drops its match arm) would leave stream selection
    /// and the info panel with no tracks at all.
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

    /// The disc's best available name: the META/DL `bdmt_*.xml` title when the
    /// disc carries one, otherwise the UDF Volume Identifier. Never a constant
    /// and never empty when either source has content — this string names the
    /// output file and the rip's directory.
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

    /// The "physically possible on this disc" gate is `size_bytes <=
    /// capacity_bytes` (see [`Disc::canonical_title_order`]'s contract:
    /// *"Real titles (`size_bytes ≤ capacity_bytes`) before virtual
    /// composites"*). A title whose declared size EXACTLY equals the disc
    /// capacity fits — a full-disc single-layer authoring, no double-counted
    /// clips — so it is a REAL title and must outrank the oversize composite,
    /// never be demoted alongside it.
    ///
    /// Asserted on the comparator directly (both argument orders), not on a
    /// sort: an inconsistent comparator produces an implementation-defined
    /// permutation, which would make a sort-based assertion prove nothing.
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

    /// `capacity_bytes == 0` means the disc capacity is UNKNOWN — `read_udf`
    /// substitutes 0 when READ CAPACITY fails (transient spin-up, SCSI error)
    /// and proceeds with the scan. It does NOT mean "the disc holds nothing".
    ///
    /// With a literal reading of the gate, 0 inverts it: EVERY real title is
    /// `size_bytes > 0` and therefore "oversize", while a title with no CLPI
    /// (`size_bytes == 0`) is not — so the empty title sorts to `titles[0]`
    /// ahead of the feature and `freemkv -t 1` rips garbage. The gate must be
    /// INERT when the capacity is unknown.
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

    /// Control for [`canonical_order_unknown_capacity_does_not_demote_every_real_title`]:
    /// making the gate inert on an UNKNOWN capacity must not make it dead. With
    /// a KNOWN capacity a genuinely oversize play-all composite is still demoted
    /// below a smaller real title — even though "largest size first" would
    /// otherwise rank it first. Asserted on the comparator in both argument
    /// orders so an inconsistent comparator cannot pass.
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

    /// Equal-size, equal-duration sibling playlists (the same feature authored
    /// twice — a full-audio main and an audio-reduced twin) are separated by
    /// audio richness, ranked `(any lossless, best channel count, track count)`
    /// with richer first. Each assertion below varies exactly ONE component of
    /// that key and holds the other two equal, so each component is pinned
    /// independently; the final pair is identical in all three and must compare
    /// Equal, so "richer first" is not satisfied by a comparator that simply
    /// never reports a tie.
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

    /// A BDMV-tree disc with NO readable `/AACS/MKB_RO.inf` (unencrypted, or an
    /// unreadable MKB) has no AACS generation to classify by, so the format
    /// falls back to video resolution. Two rules apply there:
    ///   * UHD resolution PROMOTES the disc to [`DiscFormat::Uhd`] — the ECC
    ///     block sweep is sized off this, so losing the promotion mis-sizes it;
    ///   * everything else is clamped UP to [`DiscFormat::BluRay`], because a
    ///     BD-tree disc is never a DVD even when an SD bonus title is scanned
    ///     first.
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

    // ── encrypted_content_ranges ──────────────────────────────────────────

    /// The authoritative "which sectors are AACS content" map is the UNION of
    /// every title's extents, sorted and merged into a disjoint set. Titles
    /// routinely share clips (a play-all playlist references the feature's
    /// clips), so the raw per-title extents overlap and arrive in playlist
    /// order, not LBA order. The fixture below carries all three shapes at
    /// once — an OVERLAP across two titles, an ADJACENT pair, and a DISJOINT
    /// region — supplied out of order.
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

    /// The disc hash names the disc in an [`Error::NoDiscKey`] so the caller can
    /// tell the user which keydb entry to add — it must be the disc's OWN
    /// captured SHA-1 of `Unit_Key_RO.inf`, in the bare 40-hex form the keydb is
    /// keyed on (the stored field carries a `0x` prefix). A disc with no AACS
    /// state has no hash to report, and reports nothing rather than a
    /// placeholder that would send the user hunting a non-existent entry.
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
            ..aacs_with(Vec::new())
        });
        assert_eq!(disc.aacs_disc_hash(), SHA1);
        // Already bare (no prefix) passes through unchanged, never re-stripped.
        disc.aacs = Some(AacsState {
            disc_hash: SHA1.to_string(),
            ..aacs_with(Vec::new())
        });
        assert_eq!(disc.aacs_disc_hash(), SHA1);
    }

    // ── decrypt_keys_for_title: CSS crack-span reuse is half-open ─────────

    /// A CSS title key is per-VTS: reusing the scan's cracked key for a title
    /// that lives OUTSIDE the cracked span descrambles that title with the wrong
    /// key, and the mux emits garbage at exit 0. `crack_span` is documented as
    /// the half-open LBA span `[start, end)`, so overlap is
    /// `extent.start < span.end && span.start < extent.end` — both comparisons
    /// STRICT. A title that merely ABUTS the span (ends exactly where it begins,
    /// or begins exactly where it ends) shares no sector with it and must NOT
    /// reuse the key.
    ///
    /// The reader serves only clear (all-zero) sectors, so a title that falls
    /// through to its own crack is reported unencrypted — distinguishable from
    /// the reused-key answer both in the key and in the is-clear flag.
    #[test]
    fn decrypt_keys_for_title_css_span_reuse_is_half_open() {
        const KEY: [u8; 5] = [0xA1, 0xB2, 0xC3, 0xD4, 0xE5];
        let mut disc = make_test_disc(200_000, "DVD");
        disc.format = DiscFormat::Dvd;
        disc.content_format = ContentFormat::MpegPs;
        disc.encrypted = true;
        // Key cracked from sectors [250, 300).
        disc.css = Some(crate::css::CssState {
            title_key: KEY,
            crack_span: Some((250, 300)),
        });
        let mk = |extents: &[(u32, u32)]| {
            let mut t = title_with_video(Codec::Mpeg2, Resolution::R480p);
            t.extents = extents
                .iter()
                .map(|&(start_lba, sector_count)| Extent {
                    start_lba,
                    sector_count,
                })
                .collect();
            t
        };
        disc.titles = vec![
            // 0: [200,250) — ends exactly where the span begins.
            mk(&[(200, 50)]),
            // 1: [300,350) — begins exactly where the span ends.
            mk(&[(300, 50)]),
            // 2: [260,270) — genuinely inside the span.
            mk(&[(260, 10)]),
            // 3: no extents at all.
            mk(&[]),
        ];
        let mut clear = CssMapReader {
            key: KEY,
            scrambled: (0, 0),
            reads: std::cell::RefCell::new(Vec::new()),
        };

        for idx in [0usize, 1] {
            let (keys, title_is_clear) = disc.decrypt_keys_for_title(idx, &mut clear, 16);
            assert!(
                matches!(keys, crate::decrypt::DecryptKeys::None),
                "title {idx} only ABUTS the crack span — it shares no sector with it, so the \
                 per-VTS key must not be reused"
            );
            assert!(
                title_is_clear,
                "title {idx} re-cracks from its own (clear) extents and is reported unencrypted"
            );
        }

        let (keys, title_is_clear) = disc.decrypt_keys_for_title(2, &mut clear, 16);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => assert_eq!(
                title_key, KEY,
                "a title INSIDE the crack span reuses the scan's key"
            ),
            _ => panic!("expected the reused CSS key for an overlapping title"),
        }
        assert!(!title_is_clear);

        // A title with NO extents has nothing to crack from: it short-circuits
        // to the disc-wide keys and is marked clear, so the decrypt gate's
        // "None keys + not clear" rule cannot hard-fail it.
        let (keys, title_is_clear) = disc.decrypt_keys_for_title(3, &mut clear, 16);
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key } => assert_eq!(title_key, KEY),
            _ => panic!("an extent-less title must return the disc-wide keys"),
        }
        assert!(
            title_is_clear,
            "an extent-less title is clear — nothing scrambled to worry about"
        );
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

    /// `/dev/null` output (the benchmark sink) cannot host a sibling mapfile, so
    /// the mapfile is named from the disc and placed in the temp dir. The name
    /// is sanitized to `[A-Za-z0-9-_]` — every other character, including the
    /// spaces and punctuation that appear in real META/DL titles and the
    /// non-ASCII ones, becomes `_` — because this string is used verbatim as a
    /// filename.
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

    /// A drive that answers READ CAPACITY (10) with GOOD status but an empty
    /// data phase leaves the caller's buffer zero-initialised. Decoding that
    /// blind yields `last_lba = 0` -> a "1 sector" disc, which reads as a
    /// successful capacity probe of an absurdly small disc rather than as the
    /// malformed response it is. `Disc::read_capacity` must reject it.
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
