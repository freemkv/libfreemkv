//! Parser for `menu_base.prop` — Warner CTRM properties format.
//!
//! Found at: `BDMV/JAR/*/menu_base.prop`
//!
//! Format:
//! ```text
//! audio_en.name=AudioEnglish
//! audio_en.audioStream=1
//! audio_en.audioLanguage=eng
//! subtitle_en_sdh.name=SubtitleEnglishSDH
//! subtitle_en_sdh.subtitleStream=1
//! subtitle_en_sdh.subtitleLanguage=eng
//! ```

use crate::drive::DriveSession;
use crate::udf::{UdfFs, DirEntry};
use super::{StreamLabel, StreamLabelType, LabelPurpose, LabelQualifier};
use std::collections::HashMap;

pub fn parse(session: &mut DriveSession, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    let data = find_and_read(session, udf, jar_dir, "menu_base.prop")?;
    let text = std::str::from_utf8(&data).ok()?;

    // Parse into key=value map grouped by prefix
    // e.g. "audio_en.name" → prefix="audio_en", key="name", value="AudioEnglish"
    let mut entries: HashMap<String, HashMap<String, String>> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let eq_pos = match line.find('=') {
            Some(p) => p,
            None => continue,
        };
        let full_key = &line[..eq_pos];
        let value = &line[eq_pos + 1..];

        // Split on last dot: "audio_en.audioStream" → ("audio_en", "audioStream")
        if let Some(dot_pos) = full_key.rfind('.') {
            let prefix = full_key[..dot_pos].to_string();
            let key = full_key[dot_pos + 1..].to_string();
            entries.entry(prefix).or_default().insert(key, value.to_string());
        }
    }

    let mut labels = Vec::new();

    for (prefix, props) in &entries {
        // Audio entries have "audioStream" and "audioLanguage"
        if let (Some(stream_str), Some(language)) = (props.get("audioStream"), props.get("audioLanguage")) {
            let stream_num: u16 = match stream_str.parse() {
                Ok(n) if n > 0 => n,
                _ => continue,
            };
            let name = props.get("name").cloned().unwrap_or_default();

            // Detect purpose from name
            let name_lower = name.to_lowercase();
            let purpose = if name_lower.contains("commentary") || prefix.contains("comm") {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };

            // Detect codec hint from name
            let codec_hint = if name_lower.contains("dolby") || name_lower.contains("hd") {
                "lossless".to_string()
            } else {
                String::new()
            };

            labels.push(StreamLabel {
                stream_number: stream_num,
                stream_type: StreamLabelType::Audio,
                language: language.clone(),
                name,
                purpose,
                qualifier: LabelQualifier::None,
                codec_hint,
                region: String::new(),
            });
        }

        // Subtitle entries have "subtitleStream" and "subtitleLanguage"
        if let (Some(stream_str), Some(language)) = (props.get("subtitleStream"), props.get("subtitleLanguage")) {
            let stream_num: u16 = match stream_str.parse() {
                Ok(n) if n > 0 => n,
                _ => continue,
            };
            let name = props.get("name").cloned().unwrap_or_default();

            let name_lower = name.to_lowercase();
            let qualifier = if name_lower.contains("sdh") {
                LabelQualifier::Sdh
            } else {
                LabelQualifier::None
            };

            labels.push(StreamLabel {
                stream_number: stream_num,
                stream_type: StreamLabelType::Subtitle,
                language: language.clone(),
                name,
                purpose: LabelPurpose::Normal,
                qualifier,
                codec_hint: String::new(),
                region: String::new(),
            });
        }
    }

    if labels.is_empty() { return None; }
    // Sort by type then stream number
    labels.sort_by_key(|l| (l.stream_type as u8, l.stream_number));
    Some(labels)
}

fn find_and_read(session: &mut DriveSession, udf: &UdfFs, parent: &DirEntry, filename: &str) -> Option<Vec<u8>> {
    for entry in &parent.entries {
        if entry.is_dir {
            let sub_path = format!("/BDMV/JAR/{}/{}", entry.name, filename);
            if let Ok(data) = udf.read_file(session, &sub_path) {
                if !data.is_empty() { return Some(data); }
            }
        }
    }
    None
}
