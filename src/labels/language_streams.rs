//! Parser for `language_streams.txt` — Warner CTRM CSV format.
//!
//! Found at: `BDMV/JAR/*/language_streams.txt`
//!
//! Format:
//! ```text
//! playlist_id, type, stream_num, language, variant
//! 100, audio_production, 1, eng, atmos
//! 100, subtitle_narrative, 7, fra,
//! ```

use crate::drive::DriveSession;
use crate::udf::{UdfFs, DirEntry};
use super::{StreamLabel, StreamLabelType, LabelPurpose, LabelQualifier};

pub fn parse(session: &mut DriveSession, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    // Search BDMV/JAR/*/ for language_streams.txt
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    let data = find_and_read(session, udf, jar_dir, "language_streams.txt")?;
    let text = std::str::from_utf8(&data).ok()?;

    let mut labels = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }

        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 4 { continue; }

        let _playlist_id = parts[0]; // could filter by playlist later
        let type_str = parts[1];
        let stream_num: u16 = match parts[2].parse() {
            Ok(n) => n,
            Err(_) => continue,
        };
        let language = parts[3].to_string();
        let variant = if parts.len() > 4 { parts[4].to_string() } else { String::new() };

        let (stream_type, purpose, qualifier) = match type_str {
            "audio_production" => (StreamLabelType::Audio, LabelPurpose::Normal, LabelQualifier::None),
            "audio_commentary" => (StreamLabelType::Audio, LabelPurpose::Commentary, LabelQualifier::None),
            "audio_ime" => (StreamLabelType::Audio, LabelPurpose::Ime, LabelQualifier::None),
            "subtitle_production" => (StreamLabelType::Subtitle, LabelPurpose::Normal, LabelQualifier::None),
            "subtitle_commentary" => (StreamLabelType::Subtitle, LabelPurpose::Commentary, LabelQualifier::None),
            "subtitle_narrative" => (StreamLabelType::Subtitle, LabelPurpose::Normal, LabelQualifier::Forced),
            "subtitle_dual" => (StreamLabelType::Subtitle, LabelPurpose::Normal, LabelQualifier::None),
            "subtitle_bonus" => (StreamLabelType::Subtitle, LabelPurpose::Normal, LabelQualifier::None),
            "subtitle_ime" => (StreamLabelType::Subtitle, LabelPurpose::Ime, LabelQualifier::None),
            "subtitle_ime_narrative" => (StreamLabelType::Subtitle, LabelPurpose::Ime, LabelQualifier::Forced),
            _ => continue,
        };

        // Skip feature_override_default lines
        if type_str == "feature_override_default" { continue; }

        let codec_hint = variant.clone(); // "atmos", "eda", etc.

        labels.push(StreamLabel {
            stream_number: stream_num,
            stream_type,
            language,
            name: String::new(),
            purpose,
            qualifier,
            codec_hint,
            region: String::new(),
        });
    }

    if labels.is_empty() { return None; }
    Some(labels)
}

/// Search subdirectories of a parent for a file by name.
fn find_and_read(session: &mut DriveSession, udf: &UdfFs, parent: &DirEntry, filename: &str) -> Option<Vec<u8>> {
    for entry in &parent.entries {
        if entry.is_dir {
            let sub_path = format!("/BDMV/JAR/{}/{}", entry.name, filename);
            if let Ok(data) = udf.read_file(session, &sub_path) {
                if !data.is_empty() {
                    return Some(data);
                }
            }
        }
    }
    None
}
