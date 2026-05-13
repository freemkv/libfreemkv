//! Paramount/onQ — `playlists.xml`
//!
//! Richest structured format. Complete language lists with forced flags
//! and commentary indices per playlist, all in XML attributes.
//!
//! ```xml
//! <playlist name="Feature" id="00222"
//!   aud="eng,deu,spa,spa,fra"
//!   sub="eng,eng,zho,ces,dan"
//!   forced_sub="0,0,0,1,0"
//!   aud_com1_idx="10"
//!   sub_com1_idx="23,24,25" />
//! ```

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

pub fn detect(udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "playlists.xml")
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let data = super::read_jar_file(reader, udf, "playlists.xml")?;
    let text = std::str::from_utf8(&data).ok()?;

    // Find the feature playlist — longest duration or name="Feature"
    let feature = find_feature_playlist(text)?;

    let mut labels = Vec::new();

    // Parse audio streams
    if let Some(aud) = xml::attr(&feature, "aud") {
        let com_idx = xml::attr(&feature, "aud_com1_idx").and_then(|s| s.parse::<usize>().ok());

        for (i, lang) in aud.split(',').enumerate() {
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }
            let purpose = if com_idx == Some(i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };
            labels.push(StreamLabel {
                stream_number: (i + 1) as u16,
                stream_type: StreamLabelType::Audio,
                language: lang.to_string(),
                name: String::new(),
                purpose,
                qualifier: LabelQualifier::None,
                codec_hint: String::new(),
                variant: String::new(),
            });
        }
    }

    // Parse subtitle streams
    if let Some(sub) = xml::attr(&feature, "sub") {
        let forced: Vec<bool> = xml::attr(&feature, "forced_sub")
            .map(|s| s.split(',').map(|f| f.trim() == "1").collect())
            .unwrap_or_default();

        let com_indices: Vec<usize> = xml::attr(&feature, "sub_com1_idx")
            .map(|s| s.split(',').filter_map(|i| i.trim().parse().ok()).collect())
            .unwrap_or_default();

        for (i, lang) in sub.split(',').enumerate() {
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }

            let purpose = if com_indices.contains(&i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };

            let qualifier = if forced.get(i).copied().unwrap_or(false) {
                LabelQualifier::Forced
            } else {
                LabelQualifier::None
            };

            labels.push(StreamLabel {
                stream_number: (i + 1) as u16,
                stream_type: StreamLabelType::Subtitle,
                language: lang.to_string(),
                name: String::new(),
                purpose,
                qualifier,
                codec_hint: String::new(),
                variant: String::new(),
            });
        }
    }

    if labels.is_empty() {
        return None;
    }
    // High confidence: paramount's playlists.xml is fully structured
    // and we extract every documented field.
    Some(ParseResult::high(labels))
}

/// Find the feature playlist element (the one with the most audio tracks).
fn find_feature_playlist(text: &str) -> Option<String> {
    let mut best: Option<String> = None;
    let mut best_aud_count = 0;
    let mut from = 0;

    while let Some((start, end)) = xml::find_element(text, "playlist", from) {
        let element = &text[start..end];

        // Prefer name="Feature" explicitly.
        if let Some(name) = xml::attr(element, "name") {
            if name.eq_ignore_ascii_case("Feature") {
                return Some(element.to_string());
            }
        }

        // Otherwise pick the one with the most audio streams.
        if let Some(aud) = xml::attr(element, "aud") {
            let count = aud.split(',').count();
            if count > best_aud_count {
                best_aud_count = count;
                best = Some(element.to_string());
            }
        }

        from = end;
    }
    best
}
