//! Stream label extraction from BD-J disc files.
//!
//! Searches the disc UDF filesystem for known config files that contain
//! stream labels (language, purpose, codec, forced flags). Four formats
//! supported, tried in order:
//!
//! 1. `language_streams.txt` — Warner CTRM CSV format
//! 2. `menu_base.prop` — Warner CTRM properties format
//! 3. `streamproperties.xml` + `playbackconfig.xml` — Criterion XML format
//! 4. `bluray_project.bin` — Pixelogic binary format

mod language_streams;
mod menu_base;
mod stream_properties;
mod bluray_project;

use crate::drive::DriveSession;
use crate::udf::UdfFs;

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
    /// Normal dialogue track
    Normal,
    /// Audio commentary
    Commentary,
    /// Descriptive audio (visually impaired)
    Descriptive,
    /// Music score only
    Score,
    /// In-movie experience
    Ime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelQualifier {
    None,
    /// Subtitles for deaf and hard of hearing
    Sdh,
    /// Descriptive service
    DescriptiveService,
    /// Forced/narrative subtitle
    Forced,
}

/// Try all parsers in order, return first successful result.
pub fn extract(session: &mut DriveSession, udf: &UdfFs) -> Vec<StreamLabel> {
    // Try each format in order
    if let Some(labels) = language_streams::parse(session, udf) {
        return labels;
    }
    if let Some(labels) = menu_base::parse(session, udf) {
        return labels;
    }
    if let Some(labels) = stream_properties::parse(session, udf) {
        return labels;
    }
    if let Some(labels) = bluray_project::parse(session, udf) {
        return labels;
    }
    Vec::new()
}
