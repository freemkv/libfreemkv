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

    let labels = labels_from_feature(&feature);

    if labels.is_empty() {
        return None;
    }
    // High confidence: paramount's playlists.xml is fully structured
    // and we extract every documented field.
    Some(ParseResult::high(labels))
}

/// Build the stream labels from a single `<playlist .../>` feature
/// element. Split out from `parse` so the per-type numbering and
/// commentary/forced-index logic is unit-testable without a
/// `SectorSource`/`UdfFs`.
fn labels_from_feature(feature: &str) -> Vec<StreamLabel> {
    let mut labels = Vec::new();

    // Parse audio streams
    if let Some(aud) = xml::attr(feature, "aud") {
        // aud_com1_idx is a trimmed, comma-separated list of CSV positions
        // (some authoring tools emit whitespace, and multiple commentary
        // tracks are possible) — symmetric with sub_com1_idx below.
        let com_indices: Vec<usize> = xml::attr(feature, "aud_com1_idx")
            .map(|s| s.split(',').filter_map(|i| i.trim().parse().ok()).collect())
            .unwrap_or_default();

        // stream_number must match apply_labels' monotonic 1-based
        // per-type counter, which increments once per *real* stream — so
        // it counts only non-empty slots, not the raw CSV index. The
        // commentary index comparison stays on the raw CSV index `i`,
        // since aud_com1_idx is positional against the original CSV.
        let mut audio_num: u16 = 0;
        for (i, lang) in aud.split(',').enumerate() {
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }
            let purpose = if com_indices.contains(&i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };
            audio_num = audio_num.saturating_add(1);
            labels.push(StreamLabel {
                stream_number: audio_num,
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
    if let Some(sub) = xml::attr(feature, "sub") {
        let forced: Vec<bool> = xml::attr(feature, "forced_sub")
            .map(|s| s.split(',').map(|f| f.trim() == "1").collect())
            .unwrap_or_default();

        let com_indices: Vec<usize> = xml::attr(feature, "sub_com1_idx")
            .map(|s| s.split(',').filter_map(|i| i.trim().parse().ok()).collect())
            .unwrap_or_default();

        // As with audio: count only non-empty slots for stream_number,
        // but keep com/forced lookups on the raw CSV index `i`.
        let mut sub_num: u16 = 0;
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

            sub_num = sub_num.saturating_add(1);
            labels.push(StreamLabel {
                stream_number: sub_num,
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

    labels
}

/// Find the feature playlist element (the one with the most non-empty
/// audio slots).
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

        // Otherwise pick the one with the most audio streams. Count only
        // non-empty slots so a malformed `aud=",,,,,"` can't outscore a
        // legitimate feature.
        if let Some(aud) = xml::attr(element, "aud") {
            let count = aud.split(',').filter(|s| !s.trim().is_empty()).count();
            if count > best_aud_count {
                best_aud_count = count;
                best = Some(element.to_string());
            }
        }

        from = end;
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audio(labels: &[StreamLabel]) -> Vec<&StreamLabel> {
        labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect()
    }

    fn subs(labels: &[StreamLabel]) -> Vec<&StreamLabel> {
        labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .collect()
    }

    #[test]
    fn empty_middle_slot_does_not_inflate_stream_number() {
        // aud="eng,,fra": the empty middle slot is skipped, and the
        // second real stream (fra) must be numbered 2, matching
        // apply_labels' monotonic counter — not 3 (its raw CSV index).
        let feature = r#"<playlist name="Feature" aud="eng,,fra" />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].language, "eng");
        assert_eq!(a[0].stream_number, 1);
        assert_eq!(a[1].language, "fra");
        assert_eq!(a[1].stream_number, 2);
    }

    #[test]
    fn aud_com1_idx_trimmed_and_multivalue() {
        // Whitespace around the index, and a multi-value list, must both
        // resolve. com index is positional against the raw CSV, so with
        // an empty slot at position 1, " 2 " marks the 'fra' track
        // (CSV index 2) as commentary.
        let feature = r#"<playlist aud="eng,,fra" aud_com1_idx=" 2 " />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert_eq!(a.len(), 2);
        assert_eq!(a[1].language, "fra");
        assert_eq!(a[1].purpose, LabelPurpose::Commentary);
        assert_eq!(a[0].purpose, LabelPurpose::Normal);
    }

    #[test]
    fn forced_sub_aligns_with_raw_csv_index() {
        // sub="eng,eng,zho,ces" forced_sub="0,0,0,1": the forced flag is
        // positional on the raw CSV, so 'ces' (index 3) is forced; its
        // stream_number is its non-empty position (4 here, no gaps).
        let feature = r#"<playlist sub="eng,eng,zho,ces" forced_sub="0,0,0,1" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 4);
        assert_eq!(s[3].language, "ces");
        assert_eq!(s[3].qualifier, LabelQualifier::Forced);
        assert_eq!(s[3].stream_number, 4);
    }

    #[test]
    fn find_feature_skips_empty_audio_slot_playlist() {
        // A playlist of all-empty audio slots must not outscore a real
        // two-language feature.
        let xml = r#"
            <playlist name="Junk" aud=",,,,," />
            <playlist name="Movie" aud="eng,fra" />
        "#;
        let feature = find_feature_playlist(xml).expect("a feature is found");
        assert!(feature.contains(r#"name="Movie""#));
    }
}
