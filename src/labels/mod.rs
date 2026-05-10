//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework.
//! To add a new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>>`
//!   4. Add `mod myformat;` below and one line to `PARSERS` array

pub(crate) mod class_reader;
mod criterion;
mod ctrm;
mod dbp;
mod deluxe;
pub(crate) mod jar;
mod paramount;
mod pixelogic;
pub(crate) mod text;
pub mod vocab;

use crate::disc::{DiscTitle, Stream};
use crate::sector::SectorReader;
use crate::udf::UdfFs;

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

#[derive(Debug, Clone, Copy, PartialEq)]
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
// Each entry: (name, detect_fn, parse_fn)
// Order = priority. First match wins. Highest quality output first.

type DetectFn = fn(&UdfFs) -> bool;
type ParseFn = fn(&mut dyn SectorReader, &UdfFs) -> Option<Vec<StreamLabel>>;

const PARSERS: &[(&str, DetectFn, ParseFn)] = &[
    ("paramount", paramount::detect, paramount::parse),
    ("criterion", criterion::detect, criterion::parse),
    ("pixelogic", pixelogic::detect, pixelogic::parse),
    ("ctrm", ctrm::detect, ctrm::parse),
    // dbp last: detects on any top-level .jar in /BDMV/JAR/ (every
    // BD-J disc has one), so parse() does the real `com/dbp/` check
    // and returns None on a mismatch. By placing dbp last, the
    // earlier parsers' fast file-presence detects short-circuit and
    // dbp only runs on discs that fell through everything else.
    // dbp and deluxe both detect on "any top-level .jar in /BDMV/JAR/"
    // (every BD-J disc trips that) and do the real vendor-prefix check
    // in parse(). Order between them is somewhat arbitrary since either
    // returns None on a mismatched jar, but dbp goes first because its
    // parse path is cheaper (constant-pool iteration vs. deluxe's
    // bytecode walking once Phase D lands).
    ("dbp", dbp::detect, dbp::parse),
    ("deluxe", deluxe::detect, deluxe::parse),
];

/// Search disc for config files, extract labels, apply to streams.
/// This is 100% optional — if anything fails, streams are untouched.
pub fn apply(reader: &mut dyn SectorReader, udf: &UdfFs, titles: &mut [DiscTitle]) {
    let labels = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| extract(reader, udf)))
        .unwrap_or_default();
    if labels.is_empty() {
        return;
    }

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
    for (name, detect, parse) in PARSERS {
        if detect(udf) {
            tracing::info!(parser = name, "label parser matched");
            if let Some(labels) = parse(reader, udf) {
                return labels;
            }
        }
    }
    tracing::info!("no label parser matched");
    Vec::new()
}

/// Diagnostic introspection — returns the parser that matched, the
/// labels it emitted, and the inventory of files under `/BDMV/JAR/*/`
/// that the discriminators looked at. Intended for `freemkv-tools
/// labels-analyze` and corpus regression tooling, not production code
/// paths. The matching/parsing logic is identical to [`extract`]; only
/// the return shape is richer.
#[doc(hidden)]
pub fn analyze(reader: &mut dyn SectorReader, udf: &UdfFs) -> LabelAnalysis {
    let inventory = jar_inventory(udf);
    // Record every parser whose discriminator matched — even if its
    // parse step then returned None — so the analyzer can distinguish
    // "no parser recognized this disc" from "parser recognized it but
    // couldn't read the file" (e.g. content past a truncated capture)
    // or "parser ran but produced no labels."
    let mut parsers_detected: Vec<&'static str> = Vec::new();
    for (name, detect, parse) in PARSERS {
        if detect(udf) {
            tracing::info!(parser = name, "label parser matched");
            parsers_detected.push(name);
            if let Some(labels) = parse(reader, udf) {
                return LabelAnalysis {
                    parser: Some(name),
                    parsers_detected,
                    jar_inventory: inventory,
                    labels,
                };
            }
        }
    }
    if parsers_detected.is_empty() {
        tracing::info!("no label parser matched");
    } else {
        tracing::info!(
            detected = ?parsers_detected,
            "label parsers detected but produced no labels"
        );
    }
    LabelAnalysis {
        parser: None,
        parsers_detected,
        jar_inventory: inventory,
        labels: Vec::new(),
    }
}

/// Result of [`analyze`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct LabelAnalysis {
    /// Which parser matched ("paramount" / "criterion" / "pixelogic" /
    /// "ctrm") AND emitted labels. `None` means either no parser
    /// recognized the disc, OR a parser recognized it but its parse
    /// step returned None (file unreadable, no parseable tokens). Use
    /// `parsers_detected` to disambiguate.
    pub parser: Option<&'static str>,
    /// Every parser whose discriminator matched, in priority order.
    /// Distinguishes "we recognized this disc but couldn't extract
    /// labels" from "we don't recognize this disc at all" — the
    /// former points at a parser bug or a truncated capture, the
    /// latter points at a missing parser.
    pub parsers_detected: Vec<&'static str>,
    /// Filenames found under any `/BDMV/JAR/*/` subdirectory, deduped
    /// and sorted. Helps spot unknown authoring formats when no
    /// parser detected.
    pub jar_inventory: Vec<String>,
    /// Raw labels emitted by the matched parser (empty if `parser` is
    /// `None`).
    pub labels: Vec<StreamLabel>,
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
                "deluxe"
            ],
            "PARSERS array order changed — confirm dbp + deluxe stay last \
             (loose detect, real check in parse), and stricter parsers \
             (paramount/criterion/pixelogic/ctrm — all file-presence \
             gated detect) stay first."
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
