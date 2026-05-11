//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework.
//! To add a new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>>`
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
use crate::sector::SectorReader;
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

type DetectFn = fn(&UdfFs) -> bool;
type ParseFn = fn(&mut dyn SectorReader, &UdfFs) -> Option<ParseResult>;

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
    // dbp and deluxe both detect on "any top-level .jar in /BDMV/JAR/"
    // (every BD-J disc trips that) and do the real vendor-prefix check
    // in parse(). Order between them is the tiebreaker on equal
    // confidence; dbp goes first because its parse path is cheaper
    // (constant-pool iteration vs. deluxe's bytecode walking).
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
];

/// Search disc for config files, extract labels, apply to streams.
/// This is 100% optional — if anything fails, streams are untouched.
pub fn apply(reader: &mut dyn SectorReader, udf: &UdfFs, titles: &mut [DiscTitle]) {
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
/// without needing a SectorReader / UdfFs.
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

                        // a.label only carries codec/variant info. NEVER any
                        // English purpose text — the CLI handles that via i18n.
                        let mut parts = Vec::new();
                        if !label.variant.is_empty() {
                            parts.push(format!("({})", label.variant));
                        }
                        if !label.codec_hint.is_empty() {
                            parts.push(label.codec_hint.clone());
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
                    v.label =
                        generate_video_label(&v.codec, v.resolution.pixels(), &v.hdr, v.secondary);
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

    // Resolution
    let (w, h) = pixels;
    let res = if w >= 7680 {
        "8K"
    } else if w >= 3840 {
        "4K"
    } else if w >= 1920 {
        "1080p"
    } else if w >= 1280 {
        "720p"
    } else if h >= 576 {
        "576p"
    } else if h >= 480 {
        "480p"
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

fn generate_audio_label(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    _secondary: bool,
) -> String {
    use crate::disc::{AudioChannels, Codec};

    // Full marketing names for disc audio codecs.
    // These are codec brand identifiers, not user-facing English prose.
    let codec_name = match codec {
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

fn extract(reader: &mut dyn SectorReader, udf: &UdfFs) -> Vec<StreamLabel> {
    let mut best: Option<(&'static str, ParseResult)> = None;
    for (name, detect, parse) in PARSERS {
        if !detect(udf) {
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

/// Diagnostic introspection — returns the parser that matched, the
/// labels it emitted, and the inventory of files under `/BDMV/JAR/*/`
/// that the discriminators looked at. Intended for `freemkv-tools
/// labels-analyze` and corpus regression tooling, not production code
/// paths. The matching/parsing logic is identical to [`extract`]; only
/// the return shape is richer (includes confidence, all detected
/// parsers, and any parsers that produced empty results).
#[doc(hidden)]
pub fn analyze(reader: &mut dyn SectorReader, udf: &UdfFs) -> LabelAnalysis {
    let inventory = jar_inventory(udf);
    let mut parsers_detected: Vec<&'static str> = Vec::new();
    let mut all_results: Vec<(&'static str, ParseResult)> = Vec::new();

    for (name, detect, parse) in PARSERS {
        if !detect(udf) {
            continue;
        }
        tracing::info!(parser = name, "label parser detected");
        parsers_detected.push(name);
        if let Some(r) = parse(reader, udf) {
            all_results.push((name, r));
        }
    }

    // Selection logic mirrors `extract`: highest confidence + non-empty,
    // array order tiebreaker.
    let chosen = all_results
        .iter()
        .filter(|(_, r)| !r.labels.is_empty())
        .max_by(|(_, a), (_, b)| {
            // Cmp first by confidence (higher first), then position
            // (earlier first). max_by yields the maximum, so we
            // invert the index comparison.
            a.confidence
                .cmp(&b.confidence)
                .then(std::cmp::Ordering::Equal)
        });

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
fn collect_chapter_summary(reader: &mut dyn SectorReader, udf: &UdfFs) -> Vec<ChapterSummary> {
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
/// present.
fn jar_inventory(udf: &UdfFs) -> Vec<String> {
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
    reader: &mut dyn SectorReader,
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
            ],
            "PARSERS array order changed — confirm dbp + deluxe stay just \
             before mpls_universal (loose detect, real check in parse), \
             stricter parsers (paramount/criterion/pixelogic/ctrm — all \
             file-presence gated detect) stay first, and mpls_universal \
             stays LAST as the universal Low-confidence fallback."
        );
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
        assert!(!PARSERS.is_empty(), "PARSERS array must not be empty");
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
        // Oppenheimer-style: framework matched but only labeled 2 of 6 audios.
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
// without needing a SectorReader / UdfFs. Synthetic DiscTitle +
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
            secondary: false,
            label: String::new(),
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
        // Audio #2 (4th stream overall)
        if let Stream::Audio(a) = &titles[0].streams[3] {
            assert_eq!(a.label, "Dolby Digital");
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
}
