//! Stream label extraction from BD-J disc files.
//!
//! Searches the disc UDF filesystem for known config files that contain
//! stream labels (language, purpose, codec, forced flags). Four formats
//! supported, tried in order. If found, labels are applied directly
//! to the title streams. If not found, streams keep MPLS data as-is.

mod language_streams;
mod menu_base;
mod stream_properties;
mod bluray_project;

use crate::drive::DriveSession;
use crate::udf::UdfFs;
use crate::disc::{Title, Stream};

/// A stream label extracted from disc config files.
#[derive(Debug, Clone)]
pub struct StreamLabel {
    /// STN index (1-based)
    pub stream_number: u16,
    /// Audio or Subtitle
    pub stream_type: StreamLabelType,
    /// ISO 639-2 language code
    pub language: String,
    /// Display name (e.g. "AudioEnglishDolby", "English Dolby Atmos")
    pub name: String,
    /// Stream purpose
    pub purpose: LabelPurpose,
    /// Additional qualifier
    pub qualifier: LabelQualifier,
    /// Codec hint from config (e.g. "MLP", "AC3", "atmos")
    pub codec_hint: String,
    /// Regional variant (e.g. "US", "UK", "CF", "CS")
    pub region: String,
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

/// Search disc for config files, extract labels, apply to streams.
/// If no config files found, streams are left unchanged.
pub fn apply(session: &mut DriveSession, udf: &UdfFs, titles: &mut [Title]) {
    let labels = extract(session, udf);
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
                        if !label.region.is_empty() {
                            parts.push(format!("({})", label.region));
                        }
                        if !label.codec_hint.is_empty()
                            && !matches!(label.codec_hint.as_str(), "MLP" | "AC3" | "DTS")
                        {
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
    if let Some(labels) = language_streams::parse(session, udf) { return labels; }
    if let Some(labels) = menu_base::parse(session, udf) { return labels; }
    if let Some(labels) = stream_properties::parse(session, udf) { return labels; }
    if let Some(labels) = bluray_project::parse(session, udf) { return labels; }
    Vec::new()
}
