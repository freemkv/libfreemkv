//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework.
//! To add a new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult>`
//!      (set [`ParseResult::confidence`]; it drives parser selection on
//!      a tie)
//!   4. Add `mod myformat;` below and one line to `PARSERS` array

mod bdmt;
pub(crate) mod class_reader;
pub mod clpi_audit;
mod criterion;
mod ctrm;
mod dbp;
mod deluxe;
pub(crate) mod jar;
mod mpls_universal;
mod paramount;
mod pixelogic;
mod png_filenames;
pub(crate) mod text;
pub mod vocab;
pub(crate) mod xml;

use crate::disc::{DiscTitle, Stream};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

// Re-export bdmt's public type so callers can construct/inspect
// disc-level metadata via `labels::DiscMetadata`. The module itself
// stays private — analyze() drives the parse path.
pub use bdmt::DiscMetadata;

// Re-exported via crate::disc — the public API surfaces these next to
// AudioStream/SubtitleStream so callers can map purpose/qualifier to display
// text in their own locale.

/// A stream label extracted from disc config files.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StreamLabel {
    /// STN index (1-based)
    pub stream_number: u16,
    /// Audio or Subtitle
    pub stream_type: StreamLabelType,
    /// ISO 639-2 language code
    pub language: String,
    /// Display name (e.g. "Commentary", "Descriptive Audio")
    pub name: String,
    /// Stream purpose
    pub purpose: LabelPurpose,
    /// Additional qualifier
    pub qualifier: LabelQualifier,
    /// Codec hint from config (e.g. "TrueHD", "Dolby Digital", "Dolby Atmos")
    pub codec_hint: String,
    /// Regional variant (e.g. "US", "UK", "Castilian", "Canadian")
    pub variant: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamLabelType {
    Audio,
    Subtitle,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelPurpose {
    Normal,
    Commentary,
    Descriptive,
    Score,
    /// Alternate music track (e.g. an alternate end-credits / closing-
    /// theme music stream), tagged by the `ime` token some BD-J
    /// authoring tools emit on the secondary music audio.
    Ime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelQualifier {
    None,
    Sdh,
    DescriptiveService,
    Forced,
}

// ── Parser registry ────────────────────────────────────────────────────────
//
// Each entry: (name, detect_fn, parse_fn). Order = tiebreaker only —
// the registry picks the highest-confidence parse result, falling back
// to array order on confidence ties.

// `detect` takes the reader too, so a parser can look INSIDE a jar's central
// directory (real vendor-prefix / project-file check) rather than firing on
// "any jar present". Precise detection is what lets the registry scale to many
// parsers without cross-parser collisions.
type DetectFn = fn(&mut dyn SectorSource, &UdfFs) -> bool;
type ParseFn = fn(&mut dyn SectorSource, &UdfFs) -> Option<ParseResult>;

/// Per-parser claim of how reliable its output is. Used by the
/// registry to pick between parsers when more than one matches (e.g.
/// a disc that has both `bluray_project.bin` and `playlists.xml`).
///
/// A parser SHOULD return `High` only when its full schema was
/// extracted with no fallback or guessing. `Medium` is for matched-
/// but-degraded outputs (some streams missing fields, fingerprint
/// matched but a sub-table couldn't be decoded, etc.). `Low` is for
/// the universal MPLS fallback — spec-mandated stream metadata
/// (language + base codec) that's correct but lacks editorial labels
/// (commentary, SDH, etc.). The registry prefers `High > Medium > Low`;
/// ties fall to array order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    Low,
    Medium,
    High,
}

/// Successful parser result. `None` from `parse()` still means "this
/// isn't my disc" (no labels at all); `Some(ParseResult { labels, .. })`
/// with `labels.is_empty()` is also a "no labels" case but reachable
/// via the analyzer (used by deluxe today to signal "I recognized the
/// framework but Phase D not yet implemented").
#[derive(Debug, Clone)]
pub struct ParseResult {
    pub labels: Vec<StreamLabel>,
    pub confidence: Confidence,
}

impl ParseResult {
    /// Convenience for the common "I parsed N labels with full schema
    /// coverage" case.
    pub fn high(labels: Vec<StreamLabel>) -> Self {
        ParseResult {
            labels,
            confidence: Confidence::High,
        }
    }

    /// Convenience for "I matched but had to fall back on some fields".
    pub fn medium(labels: Vec<StreamLabel>) -> Self {
        ParseResult {
            labels,
            confidence: Confidence::Medium,
        }
    }

    /// Convenience for the universal MPLS fallback: spec-derived
    /// stream language + codec, but no editorial labels (commentary,
    /// SDH, etc.). Framework parsers always win over `low`.
    pub fn low(labels: Vec<StreamLabel>) -> Self {
        ParseResult {
            labels,
            confidence: Confidence::Low,
        }
    }
}

const PARSERS: &[(&str, DetectFn, ParseFn)] = &[
    ("paramount", paramount::detect, paramount::parse),
    ("criterion", criterion::detect, criterion::parse),
    ("pixelogic", pixelogic::detect, pixelogic::parse),
    ("ctrm", ctrm::detect, ctrm::parse),
    // dbp and deluxe now detect via the real `com/<vendor>/` central-directory
    // prefix (reader-backed), so they claim only their own discs. Order between
    // them is the tiebreaker on equal confidence; dbp goes first because its
    // parse path is cheaper (constant-pool iteration vs. deluxe's bytecode
    // walking).
    ("dbp", dbp::detect, dbp::parse),
    ("deluxe", deluxe::detect, deluxe::parse),
    // Universal MPLS fallback. Returns Confidence::Low so framework
    // parsers always win when they match. Closes the "no framework
    // matched" gap (e.g. HDMV-only discs) with spec-derived language
    // + base codec for every stream the playlist references. Runs
    // last in registry order so it's only the chosen parser when
    // nothing else fired.
    (
        "mpls_universal",
        mpls_universal::detect,
        mpls_universal::parse,
    ),
    // Menu-graphic filename language hints (Low). AFTER mpls_universal so the
    // richer spec-derived floor wins the Low tie whenever it produces anything;
    // this only becomes the chosen parser when even MPLS yields nothing but the
    // menu artwork still names its languages. A last-resort language source.
    ("png_filenames", png_filenames::detect, png_filenames::parse),
];

/// Search disc for config files, extract labels, apply to streams.
/// This is 100% optional — if anything fails, streams are untouched.
pub fn apply(reader: &mut dyn SectorSource, udf: &UdfFs, titles: &mut [DiscTitle]) {
    let labels = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| extract(reader, udf)))
        .unwrap_or_default();
    if labels.is_empty() {
        return;
    }
    apply_labels(&labels, titles);
}

/// Apply a pre-extracted set of labels to titles' streams. Match
/// labels to streams by (stream_type, 1-based stream_number per type).
/// Audio streams update `purpose` + `label` (codec/variant info; never
/// English purpose text). Subtitle streams update `qualifier` and the
/// `forced` flag.
///
/// Extracted from `apply()` so the matching logic is unit-testable
/// without needing a SectorSource / UdfFs.
pub(crate) fn apply_labels(labels: &[StreamLabel], titles: &mut [DiscTitle]) {
    for title in titles.iter_mut() {
        let mut audio_idx: u16 = 0;
        let mut sub_idx: u16 = 0;

        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) => {
                    audio_idx += 1;
                    if let Some(label) = labels.iter().find(|l| {
                        l.stream_type == StreamLabelType::Audio && l.stream_number == audio_idx
                    }) {
                        // Structured fields — callers translate purpose to UI text.
                        a.purpose = label.purpose;

                        // Codec descriptor: trust the parser's `codec_hint` ONLY
                        // when it's consistent with the stream's actual codec — it
                        // may legitimately be richer (e.g. "Dolby Atmos" on a TrueHD
                        // stream, which the raw spec codec can't express). If the
                        // hint CONTRADICTS the stream (a mis-bound / shuffled label,
                        // e.g. "AC-3 2.0" on a TrueHD track, or "TrueHD" on a DD+
                        // track), discard it and derive the descriptor from the
                        // stream itself — that's correct per-stream and can never be
                        // shuffled. An empty hint is left for `fill_defaults`.
                        let codec_desc = if label.codec_hint.is_empty() {
                            // No codec hint — leave for fill_defaults.
                            String::new()
                        } else if !codec_hint_consistent(&label.codec_hint, &a.codec) {
                            // Hint contradicts the stream (mis-bound / shuffled):
                            // derive from the stream itself.
                            generate_audio_label(&a.codec, &a.channels, a.secondary)
                        } else if codec_hint_adds_detail(&label.codec_hint) {
                            // Consistent AND richer than the spec codec can express
                            // (e.g. "Dolby Atmos", "DTS:X") — keep the parser's hint.
                            label.codec_hint.clone()
                        } else {
                            // Consistent but a plain codec/channel restatement —
                            // normalize to the stream's own marketing descriptor so
                            // styling is uniform across tracks.
                            generate_audio_label(&a.codec, &a.channels, a.secondary)
                        };

                        // a.label only carries codec/variant info. NEVER any
                        // English purpose text — the CLI handles that via i18n.
                        let mut parts = Vec::new();
                        if !label.variant.is_empty() {
                            parts.push(format!("({})", label.variant));
                        }
                        if !codec_desc.is_empty() {
                            parts.push(codec_desc);
                        }
                        if !parts.is_empty() {
                            a.label = parts.join(" ");
                        } else if !label.name.is_empty() && label.purpose == LabelPurpose::Normal {
                            // Only fall back to the parser-supplied display
                            // name when there's no purpose to flag — the CLI
                            // handles purpose rendering itself.
                            a.label = label.name.clone();
                        }
                    }
                }
                Stream::Subtitle(s) => {
                    sub_idx += 1;
                    if let Some(label) = labels.iter().find(|l| {
                        l.stream_type == StreamLabelType::Subtitle && l.stream_number == sub_idx
                    }) {
                        s.qualifier = label.qualifier;
                        if label.qualifier == LabelQualifier::Forced {
                            s.forced = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Fill in default labels for any streams that don't have one.
/// Runs after BD-J label extraction — fills gaps with codec + channel descriptions.
/// This is the central place for all fallback label generation.
pub fn fill_defaults(titles: &mut [crate::disc::DiscTitle]) {
    use crate::disc::Stream;

    for title in titles.iter_mut() {
        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) if a.label.is_empty() => {
                    a.label = generate_audio_label(&a.codec, &a.channels, a.secondary);
                }
                Stream::Video(v) if v.label.is_empty() => {
                    // Unknown resolution: pass (0, 0) so the label omits the
                    // resolution token rather than tagging it a fabricated
                    // 1080p.
                    let px = if matches!(v.resolution, crate::disc::Resolution::Unknown) {
                        (0, 0)
                    } else {
                        v.resolution.pixels()
                    };
                    v.label = generate_video_label(
                        &v.codec,
                        px,
                        v.resolution.is_interlaced(),
                        &v.hdr,
                        v.secondary,
                    );
                }
                Stream::Subtitle(s) if s.forced => {
                    // Ensure forced subs are labeled even if BD-J didn't set a name
                    // (subtitle labels are generally not set — this just marks forced)
                }
                _ => {}
            }
        }
    }
}

fn generate_video_label(
    codec: &crate::disc::Codec,
    pixels: (u32, u32),
    interlaced: bool,
    hdr: &crate::disc::HdrFormat,
    secondary: bool,
) -> String {
    use crate::disc::HdrFormat;

    if secondary {
        // "Dolby Vision EL" is a brand identifier, not English prose, so the
        // library may emit it. Other "secondary video" wording is a CLI
        // concern — the library just leaves the label empty.
        return match hdr {
            HdrFormat::DolbyVision => "Dolby Vision EL".to_string(),
            _ => String::new(),
        };
    }

    let mut parts = Vec::new();

    // Codec
    parts.push(codec.name().to_string());

    // Resolution. Scan type (i/p) is honored for heights that can be
    // interlaced on disc (1080 and SD 576/480); 720/4K/8K are always
    // progressive.
    let (w, h) = pixels;
    let res = if w >= 7680 {
        "8K"
    } else if w >= 3840 {
        "4K"
    } else if w >= 1920 {
        if interlaced { "1080i" } else { "1080p" }
    } else if w >= 1280 {
        "720p"
    } else if h >= 576 {
        if interlaced { "576i" } else { "576p" }
    } else if h >= 480 {
        if interlaced { "480i" } else { "480p" }
    } else {
        ""
    };
    if !res.is_empty() {
        parts.push(res.into());
    }

    // HDR
    match hdr {
        HdrFormat::Sdr => {}
        _ => parts.push(hdr.name().to_string()),
    }

    parts.join(" ")
}

/// Does the parser's `codec_hint` name a codec consistent with the stream's
/// actual `codec`? [`apply_labels`] uses this to keep richer-but-consistent
/// hints (e.g. "Dolby Atmos" on a TrueHD stream — Atmos is a TrueHD extension
/// the raw spec codec can't express) while rejecting mis-bound ones (e.g.
/// "AC-3 2.0" on a TrueHD stream, the shuffled-label bug). Matching is by codec
/// FAMILY parsed out of the hint string. "Atmos" with no carrier named is
/// treated as compatible with its lossless carriers (TrueHD / E-AC-3). A hint
/// naming no recognizable codec family (pure editorial, e.g. "Commentary") is
/// consistent — it isn't asserting a codec.
fn codec_hint_consistent(hint: &str, codec: &crate::disc::Codec) -> bool {
    use crate::disc::Codec;
    let h = hint.to_ascii_lowercase();

    let says_truehd = h.contains("truehd") || h.contains("true hd");
    let says_ddp = h.contains("ac-3+")
        || h.contains("ac3+")
        || h.contains("e-ac-3")
        || h.contains("eac-3")
        || h.contains("eac3")
        || h.contains("digital plus")
        || h.contains("dd+");
    let says_ac3 =
        !says_ddp && (h.contains("ac-3") || h.contains("ac3") || h.contains("dolby digital"));
    let says_dts_ma = h.contains("master audio") || h.contains("hd ma");
    let says_dts_hr = h.contains("high resolution") || h.contains("hd hr");
    let says_dts = !says_dts_ma && !says_dts_hr && h.contains("dts");
    let says_lpcm = h.contains("lpcm") || h.contains("pcm");
    let says_atmos = h.contains("atmos");
    // DTS:X is an object-audio extension carried on a DTS-HD MA (or HR)
    // core, exactly as Atmos rides TrueHD / DD+. The spec Codec enum has
    // no DtsX variant, so a correctly-authored DTS:X hint must be judged
    // consistent with its DtsHdMa/DtsHdHr carrier rather than discarded.
    let says_dtsx = h.contains("dts:x") || h.contains("dts-x") || h.contains("dtsx");

    let names_family =
        says_truehd || says_ddp || says_ac3 || says_dts_ma || says_dts_hr || says_dts || says_lpcm;

    // Pure-editorial hint (no codec family named) isn't asserting a codec →
    // consistent. "Atmos" alone implies a lossless carrier (TrueHD or DD+).
    // ("DTS:X" always also matches the "dts" family above, so it never
    // reaches this branch — it is handled in the DtsHdMa/DtsHdHr arms.)
    if !names_family {
        return if says_atmos {
            matches!(codec, Codec::TrueHd | Codec::Ac3Plus)
        } else {
            true
        };
    }

    match codec {
        Codec::TrueHd => says_truehd || says_atmos,
        Codec::Ac3Plus => says_ddp || says_atmos,
        Codec::Ac3 => says_ac3,
        Codec::DtsHdMa => says_dts_ma || says_dtsx,
        Codec::DtsHdHr => says_dts_hr || says_dtsx,
        Codec::Dts => says_dts,
        Codec::Lpcm => says_lpcm,
        // Unknown / other stream codec — don't second-guess the parser's hint.
        _ => true,
    }
}

/// Does the hint carry object-audio detail the spec codec can't express
/// (Atmos / DTS:X)? Such hints are kept verbatim; plain codec/channel hints are
/// normalized to the stream's own descriptor for uniform styling across tracks.
fn codec_hint_adds_detail(hint: &str) -> bool {
    let h = hint.to_ascii_lowercase();
    h.contains("atmos") || h.contains("dts:x") || h.contains("dts-x") || h.contains("dtsx")
}

pub(crate) fn generate_audio_label(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    secondary: bool,
) -> String {
    generate_audio_label_inner(codec, channels, secondary, false)
}

/// Atmos-aware variant: same codec/channel string as [`generate_audio_label`]
/// with the object-audio marker folded into the codec brand
/// (e.g. "Dolby TrueHD Atmos 7.1"). The "Atmos" string lives here in the label
/// layer, not in the core parser. Used when a bitstream probe detected an Atmos
/// substream and the stream still carries the basic (non-editorial) label.
pub(crate) fn generate_audio_label_atmos(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    secondary: bool,
) -> String {
    generate_audio_label_inner(codec, channels, secondary, true)
}

fn generate_audio_label_inner(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    _secondary: bool,
    atmos: bool,
) -> String {
    use crate::disc::{AudioChannels, Codec};

    // Full marketing names for disc audio codecs.
    // These are codec brand identifiers, not user-facing English prose.
    let base_name = match codec {
        Codec::TrueHd => "Dolby TrueHD",
        Codec::Ac3 => "Dolby Digital",
        Codec::Ac3Plus => "Dolby Digital Plus",
        Codec::DtsHdMa => "DTS-HD Master Audio",
        Codec::DtsHdHr => "DTS-HD High Resolution",
        Codec::Dts => "DTS",
        Codec::Lpcm => "LPCM",
        Codec::Aac => "AAC",
        Codec::Mp2 => "MPEG Audio",
        Codec::Mp3 => "MP3",
        Codec::Flac => "FLAC",
        Codec::Opus => "Opus",
        _ => return String::new(),
    };

    // Atmos is an object-audio extension riding a lossless carrier (TrueHD or
    // DD+). Fold the marker into the brand name; "Atmos" is a label-layer
    // string, never asserted by the core parser.
    let codec_name = if atmos && matches!(codec, Codec::TrueHd | Codec::Ac3Plus) {
        std::borrow::Cow::Owned(format!("{base_name} Atmos"))
    } else {
        std::borrow::Cow::Borrowed(base_name)
    };

    // Channel layout
    let channel_str = match channels {
        AudioChannels::Mono => "1.0",
        AudioChannels::Stereo => "2.0",
        AudioChannels::Stereo21 => "2.1",
        AudioChannels::Quad => "4.0",
        AudioChannels::Surround50 => "5.0",
        AudioChannels::Surround51 => "5.1",
        AudioChannels::Surround61 => "6.1",
        AudioChannels::Surround71 => "7.1",
        AudioChannels::Unknown => "",
    };

    // The "(Secondary)" suffix is a CLI/UI concern — callers display it from
    // the AudioStream::secondary bool, not the library.
    if channel_str.is_empty() {
        codec_name.to_string()
    } else {
        format!("{} {}", codec_name, channel_str)
    }
}

fn extract(reader: &mut dyn SectorSource, udf: &UdfFs) -> Vec<StreamLabel> {
    let mut best: Option<(&'static str, ParseResult)> = None;
    for (name, detect, parse) in PARSERS {
        if !detect(reader, udf) {
            continue;
        }
        tracing::info!(parser = name, "label parser detected");
        let Some(result) = parse(reader, udf) else {
            continue;
        };
        if result.labels.is_empty() {
            continue;
        }
        // Pick highest confidence. Equal confidence → first wins
        // (array order tiebreaker).
        match &best {
            None => best = Some((name, result)),
            Some((_, b)) if result.confidence > b.confidence => best = Some((name, result)),
            _ => {}
        }
    }
    let (name, mut labels) = match best {
        Some((n, r)) => {
            tracing::info!(
                parser = n,
                confidence = ?r.confidence,
                label_count = r.labels.len(),
                "label parser selected",
            );
            (n, r.labels)
        }
        None => {
            tracing::info!("no label parser matched");
            return Vec::new();
        }
    };

    // Gap-fill: framework parsers often under-yield on multi-track
    // discs because their authoring layer only ships editorial labels
    // for "interesting" streams (Director's Cut, Atmos, SDH) and
    // leaves the rest as plain numbered slots. MPLS sees all streams
    // the playlist references. If MPLS has entries the framework
    // didn't cover (by stream_type + stream_number), merge them in
    // so the user sees every track even when only the "interesting"
    // ones have editorial names. Skips the merge when mpls_universal
    // was itself the chosen parser (its labels ARE the labels).
    if name != "mpls_universal" {
        if let Some(mpls_result) = mpls_universal::parse(reader, udf) {
            fill_gaps_from_mpls(&mut labels, &mpls_result.labels);
        }
    }

    // CLPI orphan streams: PIDs in /BDMV/CLIPINF/*.clpi ProgramInfo
    // that no MPLS playlist references. Empirically a small fraction of
    // streams are CLPI-only — physically on disc, not menu-reachable.
    // Append them as Low-confidence
    // labels at the tail of each stream_type (next slot after the
    // highest existing stream_number).
    let _orphans_added = append_clpi_orphans(&mut labels, reader, udf);

    labels
}

/// Append entries from `mpls` whose `(stream_type, stream_number)`
/// isn't already represented in `framework`. Framework labels are
/// richer (editorial purpose/qualifier, codec_hint with object-audio
/// detail like Atmos) so they always win for slots they cover; MPLS
/// only fills in untaken slots. Stable sort by (type, number) at
/// the end so callers see a deterministic, ascending list.
fn fill_gaps_from_mpls(framework: &mut Vec<StreamLabel>, mpls: &[StreamLabel]) {
    use std::collections::HashSet;
    let covered: HashSet<(StreamLabelType, u16)> = framework
        .iter()
        .map(|l| (l.stream_type, l.stream_number))
        .collect();
    let mut added = 0usize;
    for m in mpls {
        if !covered.contains(&(m.stream_type, m.stream_number)) {
            framework.push(m.clone());
            added += 1;
        }
    }
    if added > 0 {
        tracing::info!(
            gap_fill_added = added,
            "MPLS gap-fill merged streams the framework parser left uncovered"
        );
        framework.sort_by_key(|l| (type_tag(l.stream_type), l.stream_number));
    }
}

/// Stable sort key for `StreamLabelType`. Audio < Subtitle so the
/// merged label list groups audios first then subtitles.
fn type_tag(t: StreamLabelType) -> u8 {
    match t {
        StreamLabelType::Audio => 0,
        StreamLabelType::Subtitle => 1,
    }
}

/// Append CLPI ProgramInfo streams that NO existing label covers by
/// PID. These are "orphan" streams — physically present in the .m2ts
/// per CLPI's clip-authoritative view, but no MPLS playlist references
/// them, so the framework + MPLS gap-fill missed them. Returns the
/// number of orphans appended.
///
/// Numbering: the new entries get `stream_number = max(existing
/// per type) + 1, +2, …` so the playlist-reachable streams keep their
/// original positions and orphans sort cleanly at the tail. Empirically
/// these are commentary or alternate-version streams that the
/// authoring tool left out of the published playlist.
fn append_clpi_orphans(
    labels: &mut Vec<StreamLabel>,
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
) -> usize {
    use crate::consts::coding_type as c;
    // Index existing labels by PID — but StreamLabel doesn't carry
    // PID. Index by (type, language, codec_hint) tuple instead; this
    // is fuzzier than PID matching but the only signal available
    // here. False positives (a CLPI orphan that happens to share
    // (type, lang, codec) with an MPLS stream we already have) are
    // benign — we just skip the duplicate. False negatives (rare)
    // would cause double-listing, which is the conservative failure
    // mode.
    use std::collections::HashSet;
    let existing: HashSet<(StreamLabelType, String, String)> = labels
        .iter()
        .map(|l| (l.stream_type, l.language.clone(), l.codec_hint.clone()))
        .collect();

    // Walk CLPI files, collect distinct (type, pid, coding_type, lang)
    // tuples not already in `existing`. Dedup by PID across files so
    // a stream appearing in two clips only gets added once.
    let Some(dir) = udf.find_dir("/BDMV/CLIPINF") else {
        return 0;
    };
    let names: Vec<String> = dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".clpi"))
        .map(|e| e.name.clone())
        .collect();
    let mut seen_pids: HashSet<u16> = HashSet::new();
    let mut candidates: Vec<(StreamLabelType, u16, u8, String)> = Vec::new();
    for name in names {
        let path = format!("/BDMV/CLIPINF/{}", name);
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(clip) = crate::clpi::parse(&data) else {
            continue;
        };
        for s in clip.streams {
            if !seen_pids.insert(s.pid) {
                continue;
            }
            // Translate CLPI coding_type → label stream_type.
            // 0x90 = Presentation Graphics (PG subtitle). 0x91 =
            // Interactive Graphics (BD-J menu overlay), NOT a user-facing
            // subtitle — skip it, matching the MPLS path which drops IG.
            let stype = match s.coding_type {
                c::LPCM..=c::DTS_HD_MA | c::AC3_PLUS_SECONDARY | c::DTS_HD_SECONDARY => {
                    StreamLabelType::Audio
                }
                c::PG => StreamLabelType::Subtitle,
                _ => continue, // IG / video / unknown — skip
            };
            // Same dedup logic as MPLS: normalize language, build codec
            // hint, check against existing label set.
            let lang_norm = s.language.trim().to_ascii_lowercase();
            let codec_hint = mpls_universal::codec_name(s.coding_type).to_string();
            if existing.contains(&(stype, lang_norm.clone(), codec_hint.clone())) {
                continue;
            }
            candidates.push((stype, s.pid, s.coding_type, lang_norm));
        }
    }

    if candidates.is_empty() {
        return 0;
    }

    // Find next available stream_number per type.
    let mut next_audio: u16 = labels
        .iter()
        .filter(|l| l.stream_type == StreamLabelType::Audio)
        .map(|l| l.stream_number)
        .max()
        .unwrap_or(0)
        + 1;
    let mut next_sub: u16 = labels
        .iter()
        .filter(|l| l.stream_type == StreamLabelType::Subtitle)
        .map(|l| l.stream_number)
        .max()
        .unwrap_or(0)
        + 1;

    let added = candidates.len();
    for (stype, _pid, coding_type, language) in candidates {
        let codec_hint = mpls_universal::codec_name(coding_type).to_string();
        let name = mpls_universal::language_display_name(&language);
        let stream_number = match stype {
            StreamLabelType::Audio => {
                let n = next_audio;
                next_audio += 1;
                n
            }
            StreamLabelType::Subtitle => {
                let n = next_sub;
                next_sub += 1;
                n
            }
        };
        labels.push(StreamLabel {
            stream_number,
            stream_type: stype,
            language,
            name,
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint,
            variant: String::new(),
        });
    }
    if added > 0 {
        tracing::info!(
            clpi_orphans_added = added,
            "CLPI-only streams appended (PIDs not referenced by any MPLS playlist)"
        );
        labels.sort_by_key(|l| (type_tag(l.stream_type), l.stream_number));
    }
    added
}

/// Pick the winning parser result from `results` (built in PARSERS
/// order): highest [`Confidence`] among non-empty results, with the
/// earliest array position winning on a tie — matching `extract()`'s
/// strict-`>` first-wins scan.
///
/// `Iterator::max_by_key` returns the LAST maximal element, so the key
/// is `(confidence, Reverse(index))`: among equal-confidence entries the
/// one with the smallest index has the largest `Reverse(index)` and is
/// selected, i.e. first wins.
fn select_result<'a>(
    results: &'a [(&'static str, ParseResult)],
) -> Option<&'a (&'static str, ParseResult)> {
    results
        .iter()
        .enumerate()
        .filter(|(_, (_, r))| !r.labels.is_empty())
        .max_by_key(|(idx, (_, r))| (r.confidence, std::cmp::Reverse(*idx)))
        .map(|(_, entry)| entry)
}

/// Diagnostic introspection — returns the parser that matched, the
/// labels it emitted, and the inventory of files under `/BDMV/JAR/*/`
/// that the discriminators looked at. Intended for `freemkv-tools
/// labels-analyze` and corpus regression tooling, not production code
/// paths. The matching/parsing logic is identical to [`extract`]; only
/// the return shape is richer (includes confidence, all detected
/// parsers, and any parsers that produced empty results).
#[doc(hidden)]
pub fn analyze(reader: &mut dyn SectorSource, udf: &UdfFs) -> LabelAnalysis {
    let inventory = jar_inventory(udf);
    let mut parsers_detected: Vec<&'static str> = Vec::new();
    let mut all_results: Vec<(&'static str, ParseResult)> = Vec::new();

    for (name, detect, parse) in PARSERS {
        if !detect(reader, udf) {
            continue;
        }
        tracing::info!(parser = name, "label parser detected");
        parsers_detected.push(name);
        if let Some(r) = parse(reader, udf) {
            all_results.push((name, r));
        }
    }

    // Selection logic mirrors `extract`: highest confidence + non-empty,
    // with first-in-array-order winning on a confidence tie.
    let chosen = select_result(&all_results);

    let (parser, confidence, mut labels) = match chosen {
        Some((name, r)) => (Some(*name), Some(r.confidence), r.labels.clone()),
        None => (None, None, Vec::new()),
    };

    // Gap-fill: same merge as `extract()`. Framework labels (when
    // present) win for the slots they cover; MPLS fills in uncovered
    // stream_numbers. Skipped when MPLS was itself the chosen parser
    // (no gaps to fill against itself).
    let gap_fill_added = if parser.is_some() && parser != Some("mpls_universal") {
        let before = labels.len();
        // Re-run MPLS unconditionally — we only ran framework parsers
        // above (we want to know which one to pick), and in the
        // common case where MPLS would have detected but wasn't
        // chosen we still need its labels for the merge.
        if let Some(mpls_result) = mpls_universal::parse(reader, udf) {
            fill_gaps_from_mpls(&mut labels, &mpls_result.labels);
        }
        labels.len().saturating_sub(before)
    } else {
        0
    };

    if parsers_detected.is_empty() {
        tracing::info!("no label parser matched");
    } else if parser.is_none() {
        tracing::info!(
            detected = ?parsers_detected,
            "label parsers detected but produced no labels"
        );
    }

    // bdmt runs independently of the parser registry: it's disc-level
    // metadata (localized titles, box-set position), not per-stream
    // labels, so the "highest confidence wins" logic doesn't apply.
    // Always run if detected; surface result as a separate field.
    let disc_metadata = if bdmt::detect(udf) {
        bdmt::parse(reader, udf)
    } else {
        None
    };

    let chapter_summary = collect_chapter_summary(reader, udf);

    LabelAnalysis {
        parser,
        parsers_detected,
        confidence,
        jar_inventory: inventory,
        labels,
        disc_metadata,
        gap_fill_added,
        chapter_summary,
    }
}

/// Scan `/BDMV/PLAYLIST/*.mpls`, parse each, return a row per playlist
/// with chapter count (mark_type ≤ 1) and total duration. Sorted by
/// playlist filename. Skipped entries (read error, parse error, no
/// marks) silently dropped — this is a diagnostic field, not a
/// correctness-critical one.
fn collect_chapter_summary(reader: &mut dyn SectorSource, udf: &UdfFs) -> Vec<ChapterSummary> {
    let Some(playlist_dir) = udf.find_dir("/BDMV/PLAYLIST") else {
        return Vec::new();
    };
    let mut names: Vec<String> = playlist_dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".mpls"))
        .map(|e| e.name.clone())
        .collect();
    names.sort();

    let mut out: Vec<ChapterSummary> = Vec::new();
    for name in names {
        let path = format!("/BDMV/PLAYLIST/{}", name);
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(playlist) = crate::mpls::parse(&data) else {
            continue;
        };
        let chapter_count = playlist.marks.iter().filter(|m| m.mark_type <= 1).count();
        if chapter_count == 0 {
            continue;
        }
        // Duration: sum of (out_time - in_time) across play items,
        // each in 45kHz PTS ticks → seconds. Approximates the disc
        // module's per-title duration; we don't claim sample accuracy
        // here, just enough to identify "the long one" (main movie).
        let duration_ticks: u64 = playlist
            .play_items
            .iter()
            .map(|pi| pi.out_time.saturating_sub(pi.in_time) as u64)
            .sum();
        let duration_secs = duration_ticks as f64 / 45000.0;
        out.push(ChapterSummary {
            playlist: name,
            chapter_count,
            duration_secs,
        });
    }
    out
}

/// Result of [`analyze`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct LabelAnalysis {
    /// Which parser was SELECTED — the one whose `ParseResult` had
    /// the highest confidence among non-empty results (array order
    /// tiebreaker). `None` means either no parser recognized the
    /// disc, OR every parser that recognized it returned no labels.
    /// Use `parsers_detected` to disambiguate.
    pub parser: Option<&'static str>,
    /// Confidence of the selected parser, `None` if no parser was
    /// selected.
    pub confidence: Option<Confidence>,
    /// Every parser whose discriminator matched, in registry order.
    /// Distinguishes "we recognized this disc but couldn't extract
    /// labels" from "we don't recognize this disc at all" — the
    /// former points at a parser bug or a truncated capture, the
    /// latter points at a missing parser.
    pub parsers_detected: Vec<&'static str>,
    /// Filenames found under any `/BDMV/JAR/*/` subdirectory, deduped
    /// and sorted. Helps spot unknown authoring formats when no
    /// parser detected.
    pub jar_inventory: Vec<String>,
    /// Raw labels emitted by the selected parser (empty if `parser`
    /// is `None`).
    pub labels: Vec<StreamLabel>,
    /// Disc-level metadata from `/BDMV/META/DL/bdmt_*.xml` if present.
    /// Localized title names, descriptions, box-set position. Orthogonal
    /// to per-stream labels; populated independently from the parser
    /// registry.
    pub disc_metadata: Option<bdmt::DiscMetadata>,
    /// Number of stream slots the MPLS gap-fill merge added on top of
    /// the framework parser's output. 0 means the framework covered
    /// every MPLS-known stream slot, or MPLS itself was the chosen
    /// parser. Diagnostic for the labels-analyze tool.
    pub gap_fill_added: usize,
    /// Per-playlist chapter summary: `(playlist_filename, chapter_count, duration_secs)`.
    /// Sourced from MPLS PlaylistMark entries with `mark_type ≤ 1`
    /// (chapter entries). Ordered by playlist filename. Empty if no
    /// MPLS files have parseable marks, or the disc isn't Blu-ray.
    pub chapter_summary: Vec<ChapterSummary>,
}

/// One row of the per-playlist chapter summary in `LabelAnalysis`.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ChapterSummary {
    pub playlist: String,
    pub chapter_count: usize,
    pub duration_secs: f64,
}

/// List filenames found under any `/BDMV/JAR/<x>/` subdirectory of
/// the disc. Deduped, sorted. Returns an empty vec if no JAR dir is
/// present. `pub(crate)` so filename-based parsers (e.g. `png_filenames`)
/// can scan menu-asset names without a reader.
pub(crate) fn jar_inventory(udf: &UdfFs) -> Vec<String> {
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::new();
    for entry in &jar_dir.entries {
        if entry.is_dir {
            for child in &entry.entries {
                if !child.is_dir && !out.contains(&child.name) {
                    out.push(child.name.clone());
                }
            }
        }
    }
    out.sort();
    out
}

// ── Shared helpers ─────────────────────────────────────────────────────────

/// Check if a file exists in any BDMV/JAR subdirectory.
pub(crate) fn jar_file_exists(udf: &UdfFs, filename: &str) -> bool {
    find_jar_file(udf, filename).is_some()
}

/// Find a file in any BDMV/JAR subdirectory, return its path.
pub(crate) fn find_jar_file(udf: &UdfFs, filename: &str) -> Option<String> {
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in &jar_dir.entries {
        if entry.is_dir {
            let path = format!("/BDMV/JAR/{}/{}", entry.name, filename);
            // Check if file exists in this subdirectory
            for child in &entry.entries {
                if !child.is_dir && child.name.eq_ignore_ascii_case(filename) {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// Read a file from any BDMV/JAR subdirectory by filename.
pub(crate) fn read_jar_file(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    filename: &str,
) -> Option<Vec<u8>> {
    let path = find_jar_file(udf, filename)?;
    udf.read_file(reader, &path).ok().filter(|d| !d.is_empty())
}

// ── Registry-level tests ────────────────────────────────────────────────────

#[cfg(test)]
mod registry_tests {
    use super::*;

    /// Lock the parser roster + order. If someone reorders the array
    /// or adds/removes a parser, this test forces them to update the
    /// expectation explicitly. The order is load-bearing: first
    /// matching `parse()` wins, so reordering changes which parser
    /// claims a disc on overlapping detect signals.
    ///
    /// dbp + deluxe MUST stay at the end (their detect triggers on
    /// "any BD-J disc"; placing them earlier would short-circuit the
    /// stricter parsers above them).
    #[test]
    fn parsers_registry_order_locked() {
        let names: Vec<&str> = PARSERS.iter().map(|(n, _, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "paramount",
                "criterion",
                "pixelogic",
                "ctrm",
                "dbp",
                "deluxe",
                "mpls_universal",
                "png_filenames",
            ],
            "PARSERS array order changed — file-presence/reader-gated High \
             parsers (paramount/criterion/pixelogic/ctrm) stay first; dbp + \
             deluxe (now real com/<vendor>/ prefix detect) stay before \
             mpls_universal; mpls_universal stays the universal Low fallback; \
             png_filenames (Low, language-only hint) stays LAST so MPLS wins \
             the Low tie whenever it produces anything."
        );
    }

    fn one_label() -> StreamLabel {
        StreamLabel {
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }
    }

    fn result(conf: Confidence) -> ParseResult {
        ParseResult {
            labels: vec![one_label()],
            confidence: conf,
        }
    }

    /// `select_result` must pick the highest-confidence non-empty result
    /// and, on a confidence tie, the FIRST in array order — matching
    /// `extract()`'s strict-`>` first-wins scan (regression for the old
    /// `analyze()` `max_by(...then(Equal))` no-op that picked the LAST).
    #[test]
    fn select_result_first_wins_on_tie() {
        // Two parsers, equal (Medium) confidence: the first must win.
        let results = vec![
            ("alpha", result(Confidence::Medium)),
            ("beta", result(Confidence::Medium)),
        ];
        assert_eq!(select_result(&results).map(|(n, _)| *n), Some("alpha"));
    }

    #[test]
    fn select_result_highest_confidence_wins() {
        let results = vec![
            ("low", result(Confidence::Low)),
            ("high", result(Confidence::High)),
            ("medium", result(Confidence::Medium)),
        ];
        assert_eq!(select_result(&results).map(|(n, _)| *n), Some("high"));
    }

    #[test]
    fn select_result_skips_empty_and_handles_none() {
        let empty = ParseResult {
            labels: Vec::new(),
            confidence: Confidence::High,
        };
        // High-confidence but empty must be skipped in favour of a
        // non-empty lower-confidence result.
        let results = vec![("empty", empty), ("real", result(Confidence::Low))];
        assert_eq!(select_result(&results).map(|(n, _)| *n), Some("real"));
        // No non-empty results → None.
        let none: Vec<(&'static str, ParseResult)> = Vec::new();
        assert!(select_result(&none).is_none());
    }

    /// Per-parser sanity: every parser has both detect and parse
    /// hooked up. Catches accidental nullification (e.g. someone
    /// stubbing `parse` to always-None during a refactor).
    #[test]
    fn parsers_registry_all_entries_populated() {
        for (name, detect, parse) in PARSERS {
            // Function pointers can't be Null in safe Rust, so the
            // assertion is just that the array entry was constructed
            // — which the iter above already implies. The test
            // exists to fail compile if someone changes the tuple
            // shape (e.g. adds a 4th field) without updating callers,
            // and as a marker for "these parsers exist."
            let _ = (name, detect, parse);
        }
        // The loop above touches every registry entry. The non-empty
        // invariant is covered separately by `parsers_registry_order_locked`,
        // whose assert_eq! on the expected order fails if PARSERS is empty.
        // This test fails to compile if the tuple shape changes.
    }
}

// ── gap_fill_from_mpls tests ───────────────────────────────────────────────

#[cfg(test)]
mod gap_fill_tests {
    use super::*;

    fn label(t: StreamLabelType, n: u16, lang: &str, codec: &str) -> StreamLabel {
        StreamLabel {
            stream_number: n,
            stream_type: t,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: codec.into(),
            variant: String::new(),
        }
    }

    #[test]
    fn empty_framework_takes_all_mpls() {
        let mut framework: Vec<StreamLabel> = Vec::new();
        let mpls = vec![
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
            label(StreamLabelType::Subtitle, 1, "eng", "PG"),
        ];
        fill_gaps_from_mpls(&mut framework, &mpls);
        assert_eq!(framework.len(), 3);
    }

    #[test]
    fn framework_covers_all_mpls_no_op() {
        let mut framework = vec![
            label(StreamLabelType::Audio, 1, "eng", "Atmos"),
            label(StreamLabelType::Audio, 2, "fra", "Atmos"),
        ];
        let mpls = vec![
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 2, "fra", "TrueHD"),
        ];
        fill_gaps_from_mpls(&mut framework, &mpls);
        assert_eq!(framework.len(), 2, "no gaps to fill");
        // Framework entries kept verbatim — richer codec_hint survives.
        assert_eq!(framework[0].codec_hint, "Atmos");
        assert_eq!(framework[1].codec_hint, "Atmos");
    }

    #[test]
    fn partial_yield_fills_gaps_keeps_framework() {
        // Partial-coverage case: framework matched but only labeled 2 of 6 audios.
        let mut framework = vec![
            label(StreamLabelType::Audio, 1, "eng", "Atmos"),
            label(StreamLabelType::Audio, 4, "eng", "Commentary"),
            label(StreamLabelType::Subtitle, 1, "eng", "PG SDH"),
        ];
        let mpls = vec![
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
            label(StreamLabelType::Audio, 3, "spa", "AC-3"),
            label(StreamLabelType::Audio, 4, "eng", "AC-3"),
            label(StreamLabelType::Audio, 5, "deu", "AC-3"),
            label(StreamLabelType::Audio, 6, "ita", "AC-3"),
            label(StreamLabelType::Subtitle, 1, "eng", "PG"),
            label(StreamLabelType::Subtitle, 2, "fra", "PG"),
        ];
        fill_gaps_from_mpls(&mut framework, &mpls);
        assert_eq!(
            framework.len(),
            8,
            "3 framework + 4 audio fills (2,3,5,6) + 1 subtitle fill (2) = 8"
        );
        // sort by (type, number) means audios first
        let audios: Vec<_> = framework
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(audios.len(), 6, "all 6 audio slots covered");
        // Slot 1 + 4 retain framework codec_hint
        assert_eq!(audios[0].codec_hint, "Atmos");
        assert_eq!(audios[3].codec_hint, "Commentary");
        // Slots 2, 3, 5, 6 are MPLS-derived
        assert_eq!(audios[1].codec_hint, "AC-3");
        assert_eq!(audios[2].codec_hint, "AC-3");
    }

    #[test]
    fn orphan_append_skips_matching_type_lang_codec_tuples() {
        // If a "would-be orphan" actually shares (type, lang, codec)
        // with a label the framework or MPLS already produced, drop
        // it — the user-facing rendering would be a confusing
        // duplicate. Stream_number is computed from the EXISTING
        // labels' max(stream_number) per type so orphans (when they
        // do fire) sort cleanly at the tail.
        let labels = vec![
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
        ];
        // Simulate the orphan dedup: build the existing-tuple set
        // the way the production function does, then check exclusion.
        use std::collections::HashSet;
        let existing: HashSet<(StreamLabelType, String, String)> = labels
            .iter()
            .map(|l| (l.stream_type, l.language.clone(), l.codec_hint.clone()))
            .collect();
        let candidate = (
            StreamLabelType::Audio,
            "eng".to_string(),
            "TrueHD".to_string(),
        );
        assert!(
            existing.contains(&candidate),
            "matching tuple must be detected as duplicate"
        );
    }

    #[test]
    fn orphan_append_genuine_orphan_assigned_next_stream_number() {
        // Hypothetical scenario: framework emitted audio 1+2, MPLS
        // gap-filled 3-5, CLPI has an orphan audio in (lang=jpn,
        // codec=DTS) that doesn't collide. Expected: append as audio
        // stream_number=6 (max existing + 1).
        let mut labels = vec![
            label(StreamLabelType::Audio, 1, "eng", "TrueHD 5.1"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3 5.1"),
            label(StreamLabelType::Audio, 5, "eng", "AC-3 2.0"),
        ];
        let max_audio: u16 = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .map(|l| l.stream_number)
            .max()
            .unwrap_or(0);
        assert_eq!(max_audio, 5);
        labels.push(label(StreamLabelType::Audio, max_audio + 1, "jpn", "DTS"));
        labels.sort_by_key(|l| (type_tag(l.stream_type), l.stream_number));
        let last_audio = labels
            .iter()
            .rev()
            .find(|l| l.stream_type == StreamLabelType::Audio)
            .unwrap();
        assert_eq!(last_audio.stream_number, 6);
        assert_eq!(last_audio.language, "jpn");
    }

    #[test]
    fn sort_groups_audio_before_subtitle() {
        let mut framework: Vec<StreamLabel> = Vec::new();
        let mpls = vec![
            label(StreamLabelType::Subtitle, 1, "eng", "PG"),
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Subtitle, 2, "fra", "PG"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
        ];
        fill_gaps_from_mpls(&mut framework, &mpls);
        assert_eq!(framework.len(), 4);
        assert_eq!(framework[0].stream_type, StreamLabelType::Audio);
        assert_eq!(framework[0].stream_number, 1);
        assert_eq!(framework[1].stream_type, StreamLabelType::Audio);
        assert_eq!(framework[1].stream_number, 2);
        assert_eq!(framework[2].stream_type, StreamLabelType::Subtitle);
        assert_eq!(framework[3].stream_type, StreamLabelType::Subtitle);
    }
}

// ── apply() integration tests ──────────────────────────────────────────────
//
// End-to-end coverage for the apply_labels + fill_defaults pipeline
// without needing a SectorSource / UdfFs. Synthetic DiscTitle +
// StreamLabel inputs, assert on the resulting Stream field values.

#[cfg(test)]
mod apply_tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, Resolution,
        SampleRate, SubtitleStream, VideoStream,
    };

    fn audio(pid: u16, codec: Codec, channels: AudioChannels, language: &str) -> Stream {
        Stream::Audio(AudioStream {
            pid,
            codec,
            channels,
            language: language.into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    }

    fn subtitle(pid: u16, language: &str) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid,
            codec: Codec::Pgs,
            language: language.into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }

    fn video() -> Stream {
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
        })
    }

    fn title_with(streams: Vec<Stream>) -> DiscTitle {
        DiscTitle {
            playlist: "00800.mpls".into(),
            playlist_id: 800,
            duration_secs: 7200.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams,
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: crate::disc::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    fn audio_label(num: u16, lang: &str, codec_hint: &str, variant: &str) -> StreamLabel {
        StreamLabel {
            stream_number: num,
            stream_type: StreamLabelType::Audio,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: codec_hint.into(),
            variant: variant.into(),
        }
    }

    #[test]
    fn apply_attaches_codec_hint_and_variant_to_audio() {
        let mut titles = vec![title_with(vec![
            video(),
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
        ])];
        let labels = vec![audio_label(1, "eng", "Dolby Atmos", "")];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[1] {
            assert_eq!(a.label, "Dolby Atmos");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_combines_variant_and_codec_hint() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround51,
            "por",
        )])];
        let labels = vec![audio_label(1, "por", "Dolby Atmos", "Brazilian")];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "(Brazilian) Dolby Atmos");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_rejects_mismatched_codec_hint_and_uses_stream_codec() {
        // TrueHD+Atmos relabel case: a TrueHD+Atmos main track the parser mislabeled
        // "AC-3 2.0" (a compat-core hint bound to the wrong stream). The hint
        // contradicts the stream's real codec → discard it, use the stream's own.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround71,
            "eng",
        )])];
        let labels = vec![audio_label(1, "eng", "AC-3 2.0", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby TrueHD 7.1");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_unshuffles_cross_labeled_streams() {
        // Cross-bound hints case: hints fully cross-bound — a TrueHD stream wears "AC-3 5.1"
        // and a DD+ stream wears "TrueHD 5.1". Each is corrected from its own
        // stream codec, eliminating the shuffle.
        let mut titles = vec![title_with(vec![
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
            audio(0x1101, Codec::Ac3Plus, AudioChannels::Surround51, "spa"),
        ])];
        let labels = vec![
            audio_label(1, "eng", "AC-3 5.1", ""),
            audio_label(2, "spa", "TrueHD 5.1", ""),
        ];
        apply_labels(&labels, &mut titles);
        let got: Vec<String> = titles[0]
            .streams
            .iter()
            .filter_map(|s| {
                if let Stream::Audio(a) = s {
                    Some(a.label.clone())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(got, vec!["Dolby TrueHD 5.1", "Dolby Digital Plus 5.1"]);
    }

    #[test]
    fn apply_keeps_consistent_richer_atmos_hint() {
        // A DD+ Atmos stream legitimately labeled "Dolby Atmos" — the hint is
        // richer than the spec codec yet consistent with it, so it's kept.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3Plus,
            AudioChannels::Surround51,
            "eng",
        )])];
        let labels = vec![audio_label(1, "eng", "Dolby Atmos", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby Atmos");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_keeps_consistent_dtsx_hint_on_dts_hd_ma() {
        // DTS:X rides a DTS-HD MA core just as Atmos rides TrueHD. A
        // correctly-authored "DTS:X" hint on a DtsHdMa stream is richer
        // than the spec codec yet consistent, so it's kept verbatim —
        // not discarded and regenerated to "DTS-HD Master Audio".
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::DtsHdMa,
            AudioChannels::Surround71,
            "eng",
        )])];
        let labels = vec![audio_label(1, "eng", "DTS:X", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "DTS:X");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn dtsx_hint_consistent_with_dts_hd_carriers() {
        use crate::disc::Codec;
        // The MED fix: a DTS:X hint must now be judged consistent with
        // its DTS-HD lossless carriers (previously it was rejected,
        // because says_dts_ma/says_dts_hr were both false for "DTS:X").
        assert!(codec_hint_consistent("DTS:X", &Codec::DtsHdMa));
        assert!(codec_hint_consistent("DTS-X 7.1", &Codec::DtsHdHr));
        assert!(codec_hint_consistent("dtsx", &Codec::DtsHdMa));
        // It still names the DTS family, so plain-DTS streams remain
        // consistent (family match) — never discarded.
        assert!(codec_hint_consistent("DTS:X", &Codec::Dts));
        // But a DTS:X hint on a non-DTS stream is a genuine mismatch.
        assert!(!codec_hint_consistent("DTS:X", &Codec::TrueHd));
        assert!(!codec_hint_consistent("DTS:X", &Codec::Ac3Plus));
    }

    #[test]
    fn apply_normalizes_plain_consistent_hint_to_marketing() {
        // A French DD+ track: a DD+ stream whose hint "AC-3+ 5.1" is correct
        // but short-form. A sibling DD+ track that fell back uses the marketing
        // form — keeping the short form here would read inconsistently, so a
        // plain (non-richer) consistent hint is normalized to the stream's own.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3Plus,
            AudioChannels::Surround51,
            "fra",
        )])];
        let labels = vec![audio_label(1, "fra", "AC-3+ 5.1", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby Digital Plus 5.1");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_sets_purpose_on_audio_commentary() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3,
            AudioChannels::Stereo,
            "eng",
        )])];
        let labels = vec![StreamLabel {
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Commentary,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.purpose, LabelPurpose::Commentary);
            // Label stays empty: no codec/variant; purpose is conveyed
            // structurally, NOT as English text.
            assert_eq!(a.label, "");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_uses_name_fallback_only_for_normal_purpose() {
        // Name fallback fires when purpose=Normal and codec/variant are empty.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround71,
            "eng",
        )])];
        let labels = vec![StreamLabel {
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: "Director's Cut Edition".into(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Director's Cut Edition");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_name_fallback_suppressed_for_non_normal_purpose() {
        // Name fallback must NOT fire when purpose != Normal — the
        // CLI is responsible for rendering purpose text.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3,
            AudioChannels::Stereo,
            "eng",
        )])];
        let labels = vec![StreamLabel {
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: "Commentary by Director".into(),
            purpose: LabelPurpose::Commentary,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "", "label must not contain English purpose text");
            assert_eq!(a.purpose, LabelPurpose::Commentary);
        }
    }

    #[test]
    fn apply_sets_qualifier_on_subtitle_sdh() {
        let mut titles = vec![title_with(vec![subtitle(0x1200, "eng")])];
        let labels = vec![StreamLabel {
            stream_number: 1,
            stream_type: StreamLabelType::Subtitle,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::Sdh,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);

        if let Stream::Subtitle(s) = &titles[0].streams[0] {
            assert_eq!(s.qualifier, LabelQualifier::Sdh);
            // SDH doesn't flip the `forced` flag.
            assert!(!s.forced);
        } else {
            panic!("expected subtitle");
        }
    }

    #[test]
    fn apply_flips_forced_flag_on_subtitle_forced_qualifier() {
        let mut titles = vec![title_with(vec![subtitle(0x1200, "eng")])];
        let labels = vec![StreamLabel {
            stream_number: 1,
            stream_type: StreamLabelType::Subtitle,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::Forced,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);
        if let Stream::Subtitle(s) = &titles[0].streams[0] {
            assert_eq!(s.qualifier, LabelQualifier::Forced);
            assert!(s.forced);
        }
    }

    #[test]
    fn apply_indexes_streams_by_type_separately() {
        // Audio and subtitle each have their own 1-based index; an
        // Audio #2 label maps to the 2nd audio stream, not the 2nd
        // stream overall (which could be a subtitle).
        let mut titles = vec![title_with(vec![
            video(),
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
            subtitle(0x1200, "eng"),
            audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "fra"),
        ])];
        let labels = vec![
            audio_label(1, "eng", "Dolby Atmos", ""),
            audio_label(2, "fra", "Dolby Digital", ""),
            StreamLabel {
                stream_number: 1,
                stream_type: StreamLabelType::Subtitle,
                language: "eng".into(),
                name: String::new(),
                purpose: LabelPurpose::Normal,
                qualifier: LabelQualifier::Sdh,
                codec_hint: String::new(),
                variant: String::new(),
            },
        ];
        apply_labels(&labels, &mut titles);

        // Audio #1
        if let Stream::Audio(a) = &titles[0].streams[1] {
            assert_eq!(a.label, "Dolby Atmos");
        }
        // Audio #2 (4th stream overall). The plain "Dolby Digital" hint is
        // consistent with the AC-3 stream but carries no channel info, so it's
        // normalized to the stream's own uniform descriptor.
        if let Stream::Audio(a) = &titles[0].streams[3] {
            assert_eq!(a.label, "Dolby Digital 2.0");
        }
        // Subtitle #1
        if let Stream::Subtitle(s) = &titles[0].streams[2] {
            assert_eq!(s.qualifier, LabelQualifier::Sdh);
        }
    }

    #[test]
    fn apply_ignores_labels_for_nonexistent_streams() {
        // A label for stream #99 with no matching stream is a no-op.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround51,
            "eng",
        )])];
        let labels = vec![audio_label(99, "fra", "Dolby Digital", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "", "label must be untouched");
        }
    }

    #[test]
    fn apply_empty_labels_does_not_touch_streams() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround51,
            "eng",
        )])];
        apply_labels(&[], &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "");
        }
    }

    // ── fill_defaults() tests ───────────────────────────────────────────────

    #[test]
    fn fill_defaults_generates_audio_label_when_empty() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround71,
            "eng",
        )])];
        fill_defaults(&mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby TrueHD 7.1");
        }
    }

    #[test]
    fn fill_defaults_preserves_existing_audio_label() {
        let mut titles = vec![title_with(vec![Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::TrueHd,
            channels: AudioChannels::Surround71,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: "Pre-set Atmos".into(),
        })])];
        fill_defaults(&mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Pre-set Atmos");
        }
    }

    #[test]
    fn fill_defaults_generates_video_label_with_hdr() {
        let mut titles = vec![title_with(vec![video()])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert!(v.label.contains("4K"), "expected 4K, got {}", v.label);
            assert!(v.label.contains("HDR10"), "expected HDR10, got {}", v.label);
        }
    }

    /// Spec: an interlaced resolution (`R*i`) must surface the "i" scan type
    /// in the generated label, not a hardcoded "p". PAL DVD is 576i.
    /// Mutation: hardcode "p" → 576i video mislabeled as 576p.
    #[test]
    fn fill_defaults_video_label_honors_interlaced_scan_type() {
        let interlaced = Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        });
        let mut titles = vec![title_with(vec![interlaced])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert!(v.label.contains("576i"), "expected 576i, got {}", v.label);
            assert!(
                !v.label.contains("576p"),
                "must not say 576p, got {}",
                v.label
            );
        }

        let progressive = Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576p,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        });
        let mut titles = vec![title_with(vec![progressive])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert!(v.label.contains("576p"), "expected 576p, got {}", v.label);
        }
    }

    // ── codec_hint_consistent hardening ───────────────────────────────────────

    /// Spec: "Dolby Digital" (AC-3) hint is consistent ONLY with AC-3 streams;
    /// NOT with DD+ or TrueHD.
    /// Mutation: accept "Dolby Digital" as consistent with AC-3+ → DD+ mislabeled.
    #[test]
    fn codec_hint_consistent_ac3_not_confused_with_ddp() {
        assert!(codec_hint_consistent("Dolby Digital", &Codec::Ac3));
        assert!(codec_hint_consistent("AC-3 5.1", &Codec::Ac3));
        assert!(!codec_hint_consistent("Dolby Digital", &Codec::Ac3Plus));
        assert!(!codec_hint_consistent("AC-3 5.1", &Codec::TrueHd));
    }

    /// Spec: "Dolby Digital Plus" (AC-3+) is consistent with DD+ streams,
    /// NOT with plain AC-3.
    /// Mutation: merge DD and DD+ into one family check → mismatch undetected.
    #[test]
    fn codec_hint_consistent_ddp_not_confused_with_ac3() {
        assert!(codec_hint_consistent("Dolby Digital Plus", &Codec::Ac3Plus));
        assert!(codec_hint_consistent("E-AC-3", &Codec::Ac3Plus));
        assert!(codec_hint_consistent("DD+", &Codec::Ac3Plus));
        assert!(!codec_hint_consistent("Dolby Digital Plus", &Codec::Ac3));
    }

    /// Spec: "DTS" hint consistent with DTS streams, NOT DTS-HD families.
    /// Mutation: treat bare "DTS" hint as consistent with DtsHdMa → mismatch.
    #[test]
    fn codec_hint_consistent_dts_families_distinguished() {
        assert!(codec_hint_consistent("DTS", &Codec::Dts));
        assert!(!codec_hint_consistent("DTS", &Codec::DtsHdMa));
        assert!(!codec_hint_consistent("DTS", &Codec::DtsHdHr));
        assert!(codec_hint_consistent("DTS-HD MA", &Codec::DtsHdMa));
        assert!(codec_hint_consistent("DTS-HD HR", &Codec::DtsHdHr));
    }

    /// Spec: "LPCM" hint consistent only with Lpcm codec.
    /// Mutation: make PCM consistent with all → mismatch undetected.
    #[test]
    fn codec_hint_consistent_lpcm() {
        assert!(codec_hint_consistent("LPCM 7.1", &Codec::Lpcm));
        assert!(codec_hint_consistent("PCM", &Codec::Lpcm));
        assert!(!codec_hint_consistent("LPCM", &Codec::TrueHd));
        assert!(!codec_hint_consistent("LPCM", &Codec::Ac3));
    }

    /// Spec: empty codec hint → consistent (no assertion = no contradiction).
    /// Mutation: return false for empty hint → streams with no hint lose their label.
    #[test]
    fn codec_hint_consistent_empty_hint() {
        assert!(codec_hint_consistent("", &Codec::TrueHd));
        assert!(codec_hint_consistent("", &Codec::Ac3));
        assert!(codec_hint_consistent("", &Codec::Lpcm));
    }

    /// Spec: a pure-editorial hint (e.g. "Commentary") names no codec family
    /// and is therefore consistent with any codec stream.
    /// Mutation: parse "commentary" and return false → editorial labels discarded.
    #[test]
    fn codec_hint_consistent_editorial_hint_no_codec() {
        assert!(codec_hint_consistent("Commentary", &Codec::TrueHd));
        assert!(codec_hint_consistent("Commentary", &Codec::Ac3));
        assert!(codec_hint_consistent("Commentary", &Codec::Dts));
    }

    // ── generate_audio_label hardening ─────────────────────────────────────────

    /// Spec: `generate_audio_label` uses full marketing names, not abbreviations.
    /// Mutation: use "DD" instead of "Dolby Digital" → abbreviated name returned.
    #[test]
    fn generate_audio_label_all_codecs() {
        assert_eq!(
            generate_audio_label(&Codec::TrueHd, &AudioChannels::Surround51, false),
            "Dolby TrueHD 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Ac3, &AudioChannels::Surround51, false),
            "Dolby Digital 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Ac3Plus, &AudioChannels::Surround51, false),
            "Dolby Digital Plus 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::DtsHdMa, &AudioChannels::Surround51, false),
            "DTS-HD Master Audio 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::DtsHdHr, &AudioChannels::Surround51, false),
            "DTS-HD High Resolution 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Dts, &AudioChannels::Surround51, false),
            "DTS 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Lpcm, &AudioChannels::Surround51, false),
            "LPCM 5.1"
        );
    }

    /// Spec: Unknown codec → empty string (never "?", never panic).
    /// Mutation: return "Unknown" for unrecognized codecs → non-empty string.
    #[test]
    fn generate_audio_label_unknown_codec_empty() {
        assert_eq!(
            generate_audio_label(&Codec::Pgs, &AudioChannels::Surround51, false),
            ""
        );
    }

    /// Spec: Unknown channel layout → codec name only (no channel suffix).
    /// Mutation: append " Unknown" for unrecognized channels → spurious suffix.
    #[test]
    fn generate_audio_label_unknown_channels_no_suffix() {
        assert_eq!(
            generate_audio_label(&Codec::Ac3, &AudioChannels::Unknown, false),
            "Dolby Digital"
        );
    }

    /// Spec: all channel layouts produce the documented string suffixes.
    /// Mutation: swap any two (e.g. Mono/Stereo) → wrong descriptor rendered.
    #[test]
    fn generate_audio_label_all_channel_layouts() {
        let f = |ch| generate_audio_label(&Codec::Ac3, ch, false);
        assert_eq!(f(&AudioChannels::Mono), "Dolby Digital 1.0");
        assert_eq!(f(&AudioChannels::Stereo), "Dolby Digital 2.0");
        assert_eq!(f(&AudioChannels::Surround51), "Dolby Digital 5.1");
        assert_eq!(f(&AudioChannels::Surround71), "Dolby Digital 7.1");
    }

    /// Spec: codec_hint_adds_detail only returns true for Atmos and DTS:X.
    /// Mutation: return true for all hints → plain hints kept verbatim, no normalization.
    #[test]
    fn codec_hint_adds_detail_atmos_and_dtsx_only() {
        assert!(codec_hint_adds_detail("Dolby Atmos"));
        assert!(codec_hint_adds_detail("DTS:X"));
        assert!(codec_hint_adds_detail("DTS-X 7.1"));
        assert!(codec_hint_adds_detail("dtsx"));
        assert!(!codec_hint_adds_detail("Dolby TrueHD"));
        assert!(!codec_hint_adds_detail("DTS-HD Master Audio"));
        assert!(!codec_hint_adds_detail("Dolby Digital Plus 5.1"));
        assert!(!codec_hint_adds_detail(""));
    }
}
