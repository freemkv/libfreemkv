//! Warner CTRM — `menu_base.prop` and/or `language_streams.txt`
//!
//! Two sub-formats from the same framework. A disc may have one or both.
//! When both exist, language_streams.txt provides structured types while
//! menu_base.prop provides stream number → button name mapping.

use super::{LabelPurpose, LabelQualifier, StreamLabel, StreamLabelType, vocab};
use crate::sector::SectorReader;
use crate::udf::UdfFs;
use std::collections::HashMap;

pub fn detect(udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "menu_base.prop")
        || super::jar_file_exists(udf, "language_streams.txt")
}

pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    // Try language_streams.txt first (richer structured data)
    let ls_labels = parse_language_streams(reader, udf);

    // Try menu_base.prop (stream numbers + key names)
    let mb_labels = parse_menu_base(reader, udf);

    // If we have both, merge: language_streams for structure, menu_base for names
    match (ls_labels, mb_labels) {
        (Some(ls), Some(mb)) => Some(merge(ls, mb)),
        (Some(ls), None) => Some(ls),
        (None, Some(mb)) => Some(mb),
        (None, None) => None,
    }
}

fn merge(ls: Vec<StreamLabel>, mb: Vec<StreamLabel>) -> Vec<StreamLabel> {
    // language_streams has better type/purpose data, menu_base has button names
    // Match by stream number + type, take name from menu_base
    let mut result = ls;
    for label in &mut result {
        if let Some(mb_match) = mb
            .iter()
            .find(|m| m.stream_type == label.stream_type && m.stream_number == label.stream_number)
        {
            if label.name.is_empty() && !mb_match.name.is_empty() {
                label.name = mb_match.name.clone();
            }
        }
    }
    result
}

// ── language_streams.txt parser ────────────────────────────────────────────

fn parse_language_streams(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "language_streams.txt")?;
    let text = std::str::from_utf8(&data).ok()?;

    let mut labels = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 4 {
            continue;
        }

        let type_str = parts[1];
        let stream_num: u16 = match parts[2].parse() {
            Ok(n) => n,
            Err(_) => continue,
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
                // Codec variants — use shared label vocab
                "atmos" | "MLP" | "AC3" | "DTS" | "DDL" => {
                    codec_hint = vocab::codec(&variant).to_string();
                }
                // Purpose variants
                "eda" => final_purpose = LabelPurpose::Descriptive,
                // Dialect variants — pass through raw code from disc
                "csp" | "cs" | "lsp" | "ls" | "cf" | "pf" | "bp" | "pp" => {
                    variant_code = variant.clone();
                }
                // Unknown — store as-is in codec_hint
                _ => codec_hint = variant.clone(),
            }
        }

        labels.push(StreamLabel {
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

    if labels.is_empty() {
        return None;
    }
    Some(labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal menu_base.prop text and run `parse_menu_base`'s
    /// inner logic via a temporary closure. This isolates the prop
    /// parsing without needing a SectorReader.
    fn parse_props(text: &str) -> Vec<StreamLabel> {
        // Mirror the inner loop of parse_menu_base exactly. Kept
        // separate so the test doesn't need disc fixtures.
        use std::collections::HashMap;
        let mut entries: HashMap<String, HashMap<String, String>> = HashMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some(eq_pos) = line.find('=') else {
                continue;
            };
            let full_key = &line[..eq_pos];
            let value = &line[eq_pos + 1..];
            if let Some(dot_pos) = full_key.rfind('.') {
                entries
                    .entry(full_key[..dot_pos].to_string())
                    .or_default()
                    .insert(full_key[dot_pos + 1..].to_string(), value.to_string());
            }
        }
        let mut labels = Vec::new();
        for (prefix, props) in &entries {
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
            let name = props.get("name").cloned().unwrap_or_default();
            let purpose = match vocab::purpose(&name) {
                LabelPurpose::Normal if prefix.contains("comm") => LabelPurpose::Commentary,
                p => p,
            };
            let qualifier = if is_subtitle {
                vocab::qualifier(&name)
            } else {
                LabelQualifier::None
            };
            let stream_type = if is_audio {
                StreamLabelType::Audio
            } else {
                StreamLabelType::Subtitle
            };
            let language = props
                .get("audioLanguage")
                .or_else(|| props.get("subtitleLanguage"))
                .cloned()
                .unwrap_or_default();
            labels.push(StreamLabel {
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

    #[test]
    fn commentary_via_name() {
        let labels = parse_props(
            "audio_1.class=AudioButton\n\
             audio_1.streamNumber=2\n\
             audio_1.name=Director's Commentary\n\
             audio_1.audioLanguage=eng\n",
        );
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].purpose, LabelPurpose::Commentary);
        assert_eq!(labels[0].language, "eng");
    }

    #[test]
    fn commentary_via_prefix_when_name_silent() {
        let labels = parse_props(
            "audio_commentary_1.class=AudioButton\n\
             audio_commentary_1.streamNumber=2\n\
             audio_commentary_1.name=Track 2\n\
             audio_commentary_1.audioLanguage=eng\n",
        );
        assert_eq!(labels[0].purpose, LabelPurpose::Commentary);
    }

    #[test]
    fn commenter_does_not_false_match_commentary() {
        // Regression for the pre-refactor `name.contains("comment")`
        // bug: this would wrongly classify a "Commenter Pro" track as
        // Commentary. vocab::purpose enforces a word boundary.
        let labels = parse_props(
            "audio_1.class=AudioButton\n\
             audio_1.streamNumber=2\n\
             audio_1.name=Commenter Pro Track\n\
             audio_1.audioLanguage=eng\n",
        );
        assert_eq!(labels[0].purpose, LabelPurpose::Normal);
    }

    #[test]
    fn descriptive_via_name() {
        let labels = parse_props(
            "audio_1.class=AudioButton\n\
             audio_1.streamNumber=3\n\
             audio_1.name=English Descriptive Audio\n",
        );
        assert_eq!(labels[0].purpose, LabelPurpose::Descriptive);
    }

    #[test]
    fn sdh_only_on_subtitles() {
        // SDH applied to a subtitle stream.
        let labels = parse_props(
            "subtitle_1.class=SubtitleButton\n\
             subtitle_1.streamNumber=4\n\
             subtitle_1.name=English SDH\n",
        );
        assert_eq!(labels[0].qualifier, LabelQualifier::Sdh);
    }

    #[test]
    fn sdh_not_applied_to_audio_stream_even_if_name_contains_sdh() {
        // Audio streams should not pick up SDH (it's a subtitle
        // concept). Edge case: badly-authored name happens to include
        // "SDH" — we don't propagate it to audio metadata.
        let labels = parse_props(
            "audio_1.class=AudioButton\n\
             audio_1.streamNumber=5\n\
             audio_1.name=English SDH (track?)\n",
        );
        assert_eq!(labels[0].qualifier, LabelQualifier::None);
    }
}

// ── menu_base.prop parser ──────────────────────────────────────────────────

fn parse_menu_base(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "menu_base.prop")?;
    let text = std::str::from_utf8(&data).ok()?;

    // Parse key=value, group by prefix
    let mut entries: HashMap<String, HashMap<String, String>> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let eq_pos = match line.find('=') {
            Some(p) => p,
            None => continue,
        };
        let full_key = &line[..eq_pos];
        let value = &line[eq_pos + 1..];

        if let Some(dot_pos) = full_key.rfind('.') {
            let prefix = full_key[..dot_pos].to_string();
            let key = full_key[dot_pos + 1..].to_string();
            entries
                .entry(prefix)
                .or_default()
                .insert(key, value.to_string());
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

        let name = props.get("name").cloned().unwrap_or_default();

        // Purpose: ask vocab first (word-boundary matched — avoids the
        // "Commenter" false positive the prior `name.contains("comment")`
        // had). Then fall back to the structural prefix check
        // (`audio_commentary.foo`-style keys group commentary streams
        // regardless of display name).
        let purpose = match vocab::purpose(&name) {
            LabelPurpose::Normal if prefix.contains("comm") => LabelPurpose::Commentary,
            p => p,
        };

        // Qualifier: only apply to subtitles (SDH is a subtitle concept).
        let qualifier = if is_subtitle {
            vocab::qualifier(&name)
        } else {
            LabelQualifier::None
        };

        let stream_type = if is_audio {
            StreamLabelType::Audio
        } else {
            StreamLabelType::Subtitle
        };

        // Try to extract language from audioLanguage/subtitleLanguage prop
        let language = props
            .get("audioLanguage")
            .or_else(|| props.get("subtitleLanguage"))
            .cloned()
            .unwrap_or_default();

        labels.push(StreamLabel {
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

    if labels.is_empty() {
        return None;
    }
    labels.sort_by_key(|l| (l.stream_type as u8, l.stream_number));
    Some(labels)
}
