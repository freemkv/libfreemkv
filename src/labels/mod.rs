//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework.
//! To add a new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(session: &mut DriveSession, udf: &UdfFs) -> Option<Vec<StreamLabel>>`
//!   4. Add `mod myformat;` below and one line to `PARSERS` array

mod paramount;
mod criterion;
mod pixelogic;
mod ctrm;
pub mod vocab;

use crate::drive::DriveSession;
use crate::udf::UdfFs;
use crate::disc::{DiscTitle, Stream};

/// A stream label extracted from disc config files.
#[derive(Debug, Clone)]
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
type ParseFn = fn(&mut DriveSession, &UdfFs) -> Option<Vec<StreamLabel>>;

const PARSERS: &[(&str, DetectFn, ParseFn)] = &[
    ("paramount",  paramount::detect,  paramount::parse),
    ("criterion",  criterion::detect,  criterion::parse),
    ("pixelogic",  pixelogic::detect,  pixelogic::parse),
    ("ctrm",       ctrm::detect,       ctrm::parse),
    // ("deluxe",  deluxe::detect,     deluxe::parse),  // TODO: bytecode parser
];

/// Search disc for config files, extract labels, apply to streams.
/// This is 100% optional — if anything fails, streams are untouched.
pub fn apply(session: &mut DriveSession, udf: &UdfFs, titles: &mut [DiscTitle]) {
    let labels = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        extract(session, udf)
    })).unwrap_or_default();
    if labels.is_empty() { return; }

    for title in titles.iter_mut() {
        let mut audio_idx: u16 = 0;
        let mut sub_idx: u16 = 0;

        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) => {
                    audio_idx += 1;
                    if let Some(label) = labels.iter().find(|l|
                        l.stream_type == StreamLabelType::Audio && l.stream_number == audio_idx
                    ) {
                        let mut parts = Vec::new();
                        match label.purpose {
                            LabelPurpose::Commentary => parts.push("Commentary".to_string()),
                            LabelPurpose::Descriptive => parts.push("Descriptive Audio".to_string()),
                            LabelPurpose::Score => parts.push("Score".to_string()),
                            LabelPurpose::Ime => parts.push("IME".to_string()),
                            LabelPurpose::Normal => {}
                        }
                        if !label.variant.is_empty() {
                            parts.push(format!("({})", label.variant));
                        }
                        if !label.codec_hint.is_empty() {
                            parts.push(label.codec_hint.clone());
                        }
                        if !parts.is_empty() {
                            a.label = parts.join(" ");
                        } else if !label.name.is_empty() {
                            a.label = label.name.clone();
                        }
                    }
                }
                Stream::Subtitle(s) => {
                    sub_idx += 1;
                    if let Some(label) = labels.iter().find(|l|
                        l.stream_type == StreamLabelType::Subtitle && l.stream_number == sub_idx
                    ) {
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

fn extract(session: &mut DriveSession, udf: &UdfFs) -> Vec<StreamLabel> {
    for (_name, detect, parse) in PARSERS {
        if detect(udf) {
            if let Some(labels) = parse(session, udf) {
                return labels;
            }
        }
    }
    Vec::new()
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
pub(crate) fn read_jar_file(session: &mut DriveSession, udf: &UdfFs, filename: &str) -> Option<Vec<u8>> {
    let path = find_jar_file(udf, filename)?;
    udf.read_file(session, &path).ok().filter(|d| !d.is_empty())
}
