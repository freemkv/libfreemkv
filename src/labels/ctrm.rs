//! Warner CTRM — `menu_base.prop` and/or `language_streams.txt`
//!
//! Two sub-formats from the same framework. A disc may have one or both.
//! When both exist, language_streams.txt provides structured types while
//! menu_base.prop provides stream number → button name mapping.

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::{BTreeMap, HashMap, HashSet};

// Untrusted-input caps: labels per language_streams, distinct menu_base prefixes,
// and properties kept per prefix.
const MAX_CTRM_LABELS: usize = 4096;
const MAX_PROPS_PER_PREFIX: usize = 64;

/// Cheap signature check: a CTRM disc ships `menu_base.prop` and/or
/// `language_streams.txt` inside a `/BDMV/JAR/*` archive.
pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "menu_base.prop")
        || super::jar_file_exists(udf, "language_streams.txt")
}

/// Full extraction: parses `language_streams.txt` (structured types) and
/// `menu_base.prop` (stream numbers + button names), merging when both
/// are present. Returns `None` when neither file is present/parseable or
/// no labels result.
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    // Try language_streams.txt first (richer structured data)
    let ls_labels = parse_language_streams(reader, udf);

    // Try menu_base.prop (stream numbers + key names)
    let mb_labels = parse_menu_base(reader, udf);

    // If we have both, merge: language_streams for structure, menu_base for names
    let labels = match (ls_labels, mb_labels) {
        (Some(ls), Some(mb)) => merge(ls, mb),
        (Some(ls), None) => ls,
        (None, Some(mb)) => mb,
        (None, None) => return None,
    };
    if labels.is_empty() {
        return None;
    }
    // High confidence: both language_streams.txt and menu_base.prop
    // are structured key-value formats with documented types.
    Some(ParseResult::high(labels))
}

fn merge(ls: Vec<StreamLabel>, mb: Vec<StreamLabel>) -> Vec<StreamLabel> {
    // language_streams has better type/purpose data, menu_base has button names
    // Match by stream number + type, take name from menu_base (first match wins).
    let mut mb_first: HashMap<(StreamLabelType, u16), &StreamLabel> = HashMap::new();
    for m in &mb {
        mb_first
            .entry((m.stream_type, m.stream_number))
            .or_insert(m);
    }
    let mut result = ls;
    for label in &mut result {
        if let Some(mb_match) = mb_first.get(&(label.stream_type, label.stream_number))
            && label.name.is_empty()
            && !mb_match.name.is_empty()
        {
            label.name = mb_match.name.clone();
        }
    }
    // Append menu_base-only streams (present in mb but not in ls by
    // (stream_type, stream_number)); language_streams is authoritative for
    // type/purpose but not necessarily a superset of menu_base.
    let mut seen: HashSet<(StreamLabelType, u16)> = result
        .iter()
        .map(|l| (l.stream_type, l.stream_number))
        .collect();
    for mb_label in mb {
        if seen.insert((mb_label.stream_type, mb_label.stream_number)) {
            result.push(mb_label);
        }
    }
    result
}

// Prefix denotes a commentary group if it has a `commentary`/`comm`
// segment (split on `_`) — not a substring scan, which used to
// over-match `common_*`/`community_*`.
fn prefix_is_commentary(prefix: &str) -> bool {
    prefix
        .split('_')
        .any(|seg| seg.eq_ignore_ascii_case("commentary") || seg.eq_ignore_ascii_case("comm"))
}

// ── language_streams.txt parser ────────────────────────────────────────────

fn parse_language_streams(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "language_streams.txt")?;
    let text = std::str::from_utf8(&data).ok()?;
    let labels = parse_language_streams_text(text);
    if labels.is_empty() {
        return None;
    }
    Some(labels)
}

// Parses language_streams.txt body into labels. Split from
// parse_language_streams (I/O + UTF-8 decode only) so tests below
// exercise this production code, not a hand-copied duplicate.
fn parse_language_streams_text(text: &str) -> Vec<StreamLabel> {
    let mut labels = Vec::new();

    for line in text.lines() {
        if labels.len() >= MAX_CTRM_LABELS {
            break;
        }
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 4 {
            continue;
        }

        let type_str = parts[1];
        // STN indices are 1-based; apply_labels pre-increments from 0 and
        // never matches a 0, so a 0 here would emit a dead label. Skip it
        // (matching the `n > 0` guard in parse_menu_base).
        let stream_num: u16 = match parts[2].parse() {
            Ok(n) if n > 0 => n,
            _ => continue,
        };
        let language = parts[3].to_string();
        let variant = if parts.len() > 4 {
            parts[4].to_string()
        } else {
            String::new()
        };

        let (stream_type, purpose, qualifier) = match type_str {
            "audio_production" => (
                StreamLabelType::Audio,
                LabelPurpose::Normal,
                LabelQualifier::None,
            ),
            "audio_commentary" => (
                StreamLabelType::Audio,
                LabelPurpose::Commentary,
                LabelQualifier::None,
            ),
            "audio_ime" => (
                StreamLabelType::Audio,
                LabelPurpose::Ime,
                LabelQualifier::None,
            ),
            "subtitle_production" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Normal,
                LabelQualifier::None,
            ),
            "subtitle_commentary" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Commentary,
                LabelQualifier::None,
            ),
            "subtitle_narrative" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Normal,
                LabelQualifier::Forced,
            ),
            "subtitle_dual" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Normal,
                LabelQualifier::None,
            ),
            "subtitle_bonus" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Normal,
                LabelQualifier::None,
            ),
            "subtitle_ime" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Ime,
                LabelQualifier::None,
            ),
            "subtitle_ime_narrative" => (
                StreamLabelType::Subtitle,
                LabelPurpose::Ime,
                LabelQualifier::Forced,
            ),
            _ => continue,
        };

        // Classify variant code
        let mut codec_hint = String::new();
        let mut variant_code = String::new();
        let mut final_purpose = purpose;

        if !variant.is_empty() {
            match variant.as_str() {
                // Purpose variants
                "eda" => final_purpose = LabelPurpose::Descriptive,
                // Dialect variants — pass through raw code from disc
                "csp" | "cs" | "lsp" | "ls" | "cf" | "pf" | "bp" | "pp" => {
                    variant_code = variant.clone();
                }
                // Everything else: defer to vocab::codec as the single source of
                // codec-name truth — a known token gets its canonical name,
                // otherwise it's stored as-is.
                _ => codec_hint = vocab::codec(&variant).to_string(),
            }
        }

        labels.push(StreamLabel {
            stream_id: None,
            stream_number: stream_num,
            stream_type,
            language,
            name: String::new(),
            purpose: final_purpose,
            qualifier,
            codec_hint,
            variant: variant_code,
        });
    }

    labels
}

// ── menu_base.prop parser ──────────────────────────────────────────────────

fn parse_menu_base(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "menu_base.prop")?;
    let text = std::str::from_utf8(&data).ok()?;
    let labels = parse_menu_base_text(text);
    if labels.is_empty() {
        return None;
    }
    Some(labels)
}

// Parses menu_base.prop body into labels. Split from parse_menu_base
// (I/O + UTF-8 decode only) so tests exercise real parsing logic.
// Returns labels sorted by (type, number).
fn parse_menu_base_text(text: &str) -> Vec<StreamLabel> {
    // Parse key=value, group by prefix
    // BTreeMap: deterministic prefix order so equal (type, number) ties sort stably.
    let mut entries: BTreeMap<String, HashMap<String, String>> = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let eq_pos = match line.find('=') {
            Some(p) => p,
            None => continue,
        };
        let full_key = line[..eq_pos].trim();
        let value = line[eq_pos + 1..].trim_start();

        if let Some(dot_pos) = full_key.rfind('.') {
            let prefix = full_key[..dot_pos].to_string();
            let key = full_key[dot_pos + 1..].to_string();
            if !entries.contains_key(&prefix) && entries.len() >= MAX_CTRM_LABELS {
                continue;
            }
            let props = entries.entry(prefix).or_default();
            if props.len() < MAX_PROPS_PER_PREFIX || props.contains_key(&key) {
                props.insert(key, value.to_string());
            }
        }
    }

    let mut labels = Vec::new();

    for (prefix, props) in &entries {
        // Audio: has "streamNumber" or "audioStream" and audio-related class
        let is_audio = props
            .get("class")
            .is_some_and(|c| c.contains("AudioButton"))
            || prefix.starts_with("audio_");
        let is_subtitle = props
            .get("class")
            .is_some_and(|c| c.contains("SubtitleButton"))
            || prefix.starts_with("subtitle_");

        let stream_num_str = props
            .get("streamNumber")
            .or_else(|| props.get("audioStream"))
            .or_else(|| props.get("subtitleStream"));

        let stream_num: u16 = match stream_num_str.and_then(|s| s.parse().ok()) {
            Some(n) if n > 0 => n,
            _ => continue,
        };

        if !is_audio && !is_subtitle {
            continue;
        }

        // Resolve the stream type FIRST: when an entry trips both flags
        // (e.g. an `audio_` prefix with a class containing
        // "SubtitleButton"), audio wins the type.
        let stream_type = if is_audio {
            StreamLabelType::Audio
        } else {
            StreamLabelType::Subtitle
        };

        let name = props.get("name").cloned().unwrap_or_default();

        // Ask vocab first (word-boundary matched, avoiding the "Commenter"
        // false positive of the old `name.contains("comment")`), then fall
        // back to the structural prefix check (`audio_commentary.foo`-style).
        let purpose = match vocab::purpose(&name) {
            LabelPurpose::Normal if prefix_is_commentary(prefix) => LabelPurpose::Commentary,
            p => p,
        };

        // Qualifier (SDH/Forced) is a subtitle-only concept. Gate on the
        // RESOLVED type, not the raw is_subtitle flag, so an entry that
        // resolved to Audio never carries a subtitle qualifier.
        let qualifier = if stream_type == StreamLabelType::Subtitle {
            vocab::qualifier(&name)
        } else {
            LabelQualifier::None
        };

        // Try to extract language from audioLanguage/subtitleLanguage prop
        let language = props
            .get("audioLanguage")
            .or_else(|| props.get("subtitleLanguage"))
            .cloned()
            .unwrap_or_default();

        labels.push(StreamLabel {
            stream_id: None,
            stream_number: stream_num,
            stream_type,
            language,
            name,
            purpose,
            qualifier,
            codec_hint: String::new(),
            variant: String::new(),
        });
    }

    labels.sort_by_key(|l| (l.stream_type as u8, l.stream_number));
    labels
}

#[cfg(test)]
#[path = "ctrm_tests.rs"]
mod tests;
