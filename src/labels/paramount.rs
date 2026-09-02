//! Paramount/onQ — `playlists.xml`. Richest structured format: complete
//! language lists with forced flags and commentary indices per playlist,
//! all in XML attributes.
//!
//! NOT A SPECIFICATION: `/BDMV/JAR/` is application-defined space; every
//! field meaning here was derived by measuring real discs — see
//! docs/paramount.md for the derivation. The disc's own `forced_on_flag`
//! is the only authoritative forced signal.
//!
//! ```xml
//! <playlist name="Feature" id="00222"
//!   aud="eng,deu,spa,spa,fra"
//!   sub="eng,eng,zho,ces,dan"
//!   forced_sub="0,0,0,1,3"
//!   aud_com1_idx="10"
//!   sub_com1_idx="23,24,25" />
//! ```
//!
//! `forced_sub` is an ENUMERATION, not a boolean — see [`ForcedSub`].

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashSet;

pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
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

    // Surface the feature playlist's identity so title selection can prefer
    // it over a size-inflated decoy. The id is 5-digit zero-padded on BD
    // (matching `NNNNN.mpls`); keep only digits to resist a stray quote/space.
    let feature_playlist = super::xml::attr(&feature, "id").and_then(|id| {
        let digits: String = id.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            return None;
        }
        Some(super::FeaturePlaylistHint {
            playlist_id: digits.parse::<u16>().ok(),
            filename: Some(format!("{digits}.mpls")),
        })
    });

    // High confidence: this format is fully structured and we extract
    // every field whose meaning the corpus establishes. "Documented" would
    // be the wrong word — see the module note; nothing about it is.
    let mut result = ParseResult::high(labels);
    result.feature_playlist = feature_playlist;
    Some(result)
}

// One cell of the `forced_sub` CSV. Reads like a boolean but is an
// enumeration; see docs/paramount.md for the corpus measurements behind
// each value. Unrecognised cells map to `None` — the conservative direction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ForcedSub {
    /// No forced-narrative content, or an unrecognised cell.
    None,
    /// A full dialogue track that also carries forced-narrative segments.
    ContainsForcedSegments,
    /// A dedicated forced-narrative track.
    ForcedNarrative,
}

// The highest CSV cell position that can ever be addressed — caps both the
// VALUE set size and the WORK done per cell. See docs/paramount.md for why
// those are two different bounds. Real authoring never approaches it.
const MAX_COM_INDICES: usize = u16::MAX as usize;

// Parse a `*_com1_idx` attribute into the set the labelling loops query.
// Extracted so the bound is independently testable — see docs/paramount.md
// (`com_indices` / `forced_subs`) for why a label-level test cannot see it.
fn com_indices(attr: Option<String>) -> HashSet<usize> {
    attr.map(|s| {
        s.split(',')
            .take(MAX_COM_INDICES)
            .filter_map(|i| i.trim().parse().ok())
            .filter(|&i| i < MAX_COM_INDICES)
            .collect()
    })
    .unwrap_or_default()
}

// Parse `forced_sub` into the cell list the subtitle loop queries. Bounded
// by POSITION rather than value (nothing to filter on a classification) —
// see docs/paramount.md (`com_indices` / `forced_subs`) for why.
fn forced_subs(attr: Option<String>) -> Vec<ForcedSub> {
    attr.map(|s| {
        s.split(',')
            .take(MAX_COM_INDICES)
            .map(forced_sub_cell)
            .collect()
    })
    .unwrap_or_default()
}

fn forced_sub_cell(cell: &str) -> ForcedSub {
    match cell.trim() {
        "1" => ForcedSub::ContainsForcedSegments,
        "2" | "3" => ForcedSub::ForcedNarrative,
        _ => ForcedSub::None,
    }
}

// Build the stream labels from a single `<playlist .../>` feature element.
// Split out from `parse` so numbering/commentary/forced-index logic is
// unit-testable without a `SectorSource`/`UdfFs`.
fn labels_from_feature(feature: &str) -> Vec<StreamLabel> {
    let mut labels = Vec::new();

    // Parse audio streams
    if let Some(aud) = xml::attr(feature, "aud") {
        // aud_com1_idx: trimmed CSV positions, symmetric with sub_com1_idx.
        // HashSet not Vec: parsed from an attacker-controlled attribute and
        // only membership-tested; a Vec would make large inputs quadratic.
        let com_indices = com_indices(xml::attr(feature, "aud_com1_idx"));

        // The CSV *is* the STN list, so `stream_number` is each cell's 1-based
        // position, not a counter over labeled cells (empty cells still occupy
        // their slot). `u16::try_from`: stop rather than wrap onto `u16::MAX`.
        for (i, lang) in aud.split(',').enumerate() {
            let Ok(stream_number) = u16::try_from(i + 1) else {
                break;
            };
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }
            let purpose = if com_indices.contains(&i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };
            labels.push(StreamLabel {
                stream_id: None,
                stream_number,
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
        let forced = forced_subs(xml::attr(feature, "forced_sub"));

        // HashSet for the same reason as the audio side above: unbounded
        // parsed input, membership-only use, linear scan once per stream.
        let com_indices = com_indices(xml::attr(feature, "sub_com1_idx"));

        // As with audio: the cell position IS the STN slot, indexed by
        // `forced_sub`/`sub_com1_idx`, so an empty cell must not renumber
        // the rest — else a forced marker lands on the wrong subtitle track.
        for (i, lang) in sub.split(',').enumerate() {
            let Ok(stream_number) = u16::try_from(i + 1) else {
                break;
            };
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }

            let purpose = if com_indices.contains(&i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };

            // Only a DEDICATED forced-narrative slot earns the forced flag.
            // A cell marking a full track as merely containing forced segments
            // is dropped, not weakened into a forced label (see [`ForcedSub`]).
            let qualifier = match forced.get(i).copied().unwrap_or(ForcedSub::None) {
                ForcedSub::ForcedNarrative => LabelQualifier::Forced,
                ForcedSub::ContainsForcedSegments | ForcedSub::None => LabelQualifier::None,
            };

            labels.push(StreamLabel {
                stream_id: None,
                stream_number,
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
        if let Some(name) = xml::attr(element, "name")
            && name.eq_ignore_ascii_case("Feature")
        {
            return Some(element.to_string());
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

    // Immunity pin, section-boundary half — see docs/paramount.md.
    // Mutation: hand `labels_from_feature` the document instead of the
    // element, or let an unterminated element run to EOF.
    #[test]
    fn a_playlists_stream_list_cannot_run_into_the_next_playlist() {
        let doc = r#"
            <playlist name="Feature" aud="eng,fra" sub="eng,spa" forced_sub="0,1"/>
            <playlist name="Bonus" aud="deu,ita,jpn" sub="deu,ita,jpn"/>
        "#;
        let feature = find_feature_playlist(doc).expect("feature playlist found");
        let labels = labels_from_feature(&feature);
        let got: Vec<(StreamLabelType, u16, &str)> = labels
            .iter()
            .map(|l| (l.stream_type, l.stream_number, l.language.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                (StreamLabelType::Audio, 1, "eng"),
                (StreamLabelType::Audio, 2, "fra"),
                (StreamLabelType::Subtitle, 1, "eng"),
                (StreamLabelType::Subtitle, 2, "spa"),
            ],
            "the CSV's own cells are the whole stream list"
        );

        // Same document with the feature element left unterminated.
        let unterminated = r#"
            <playlist name="Feature" aud="eng,fra">
            <playlist name="Bonus" aud="deu,ita,jpn"/>
        "#;
        assert!(
            find_feature_playlist(unterminated).is_none(),
            "a missing element boundary truncates the walk, never extends it"
        );
    }

    // Replaces a flaky wall-clock test; see docs/paramount.md for why.
    // Guards that bounding the parse did not change what a legitimate
    // playlist MEANS (real indices `0,2,4` must still resolve).
    #[test]
    fn bounding_the_parse_does_not_change_a_legitimate_playlist() {
        // Three real indices, then far more entries than can address a cell.
        const OVERSIZED: usize = MAX_COM_INDICES + 10_000;
        let mut feature = String::from(r#"<playlist name="Feature" sub=""#);
        feature.push_str(&"eng,".repeat(8));
        feature.pop();
        feature.push_str(r#"" sub_com1_idx="0,2,4,"#);
        feature.push_str(&"9999999,".repeat(OVERSIZED));
        feature.pop();
        feature.push_str(r#"" />"#);

        let labels = labels_from_feature(&feature);

        // The fixture's real indices still decide the purposes: bounding the
        // parse must not change what a legitimate playlist means.
        assert_eq!(labels.len(), 8);
        assert_eq!(labels[0].purpose, LabelPurpose::Commentary);
        assert_eq!(labels[1].purpose, LabelPurpose::Normal);
        assert_eq!(labels[2].purpose, LabelPurpose::Commentary);
        assert_eq!(labels[3].purpose, LabelPurpose::Normal);
        assert_eq!(labels[4].purpose, LabelPurpose::Commentary);
    }

    // The set REFUSES unaddressable indices. DISTINCT values on purpose — a
    // `HashSet` collapses repeats, so only distinct entries can prove the
    // filter exists; see docs/paramount.md for the full rationale.
    #[test]
    fn distinct_unaddressable_indices_are_refused_not_stored() {
        let hostile: String = (MAX_COM_INDICES..MAX_COM_INDICES + 50_000)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let set = com_indices(Some(hostile));
        assert!(
            set.is_empty(),
            "kept {} unaddressable indices — the parse is still unbounded",
            set.len()
        );
        // The addressable ones are still kept.
        assert_eq!(com_indices(Some("0,2,4".to_string())).len(), 3);
    }

    // `forced_sub` is bounded too, and read through `forced_subs` rather
    // than the labels for the same reason as the tests above — see
    // docs/paramount.md.
    #[test]
    fn forced_sub_cells_past_the_last_addressable_one_are_not_parsed() {
        let hostile = "0,".repeat(MAX_COM_INDICES + 50_000);
        let cells = forced_subs(Some(hostile));
        assert_eq!(
            cells.len(),
            MAX_COM_INDICES,
            "parsed {} cells — the forced_sub parse is still unbounded",
            cells.len()
        );
    }

    /// Bounding it must not change what a legitimate playlist means: the
    /// cells that CAN address a stream still classify exactly as before.
    #[test]
    fn bounding_forced_sub_leaves_the_addressable_cells_alone() {
        let cells = forced_subs(Some("0,1,3,2".to_string()));
        assert_eq!(
            cells,
            vec![
                ForcedSub::None,
                ForcedSub::ContainsForcedSegments,
                ForcedSub::ForcedNarrative,
                ForcedSub::ForcedNarrative,
            ]
        );
    }

    // An index that cannot address any cell is dropped rather than STORED.
    // Asserted through `com_indices`, not the labels — see docs/paramount.md
    // for why a label-level assertion here could not fail.
    #[test]
    fn an_index_that_cannot_address_any_cell_is_not_retained() {
        let set = com_indices(Some(format!(
            "1,{},{}",
            MAX_COM_INDICES,
            MAX_COM_INDICES + 1
        )));
        assert_eq!(
            set.len(),
            1,
            "only the addressable index belongs in the set, got {set:?}"
        );
        assert!(set.contains(&1));
    }

    /// Headroom: the BD STN_table admits at most 32 PG streams per playlist,
    /// and a real `sub_com1_idx` lists a handful of commentary tracks. The set
    /// must behave identically to the old scan on real-shaped input.
    #[test]
    fn commentary_indices_still_match_on_real_shaped_input() {
        let feature = r#"<playlist name="Feature" sub="eng,eng,zho,ces,dan" sub_com1_idx="1,3" />"#;
        let labels = labels_from_feature(feature);
        let purposes: Vec<LabelPurpose> = labels.iter().map(|l| l.purpose).collect();
        assert_eq!(
            purposes,
            vec![
                LabelPurpose::Normal,
                LabelPurpose::Commentary,
                LabelPurpose::Normal,
                LabelPurpose::Commentary,
                LabelPurpose::Normal,
            ]
        );
    }

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

    // An empty CSV cell carries nothing to label but still OCCUPIES its STN
    // slot, so it must not renumber the slots behind it — see
    // docs/paramount.md for how the old renumbering misplaced a forced flag.
    #[test]
    fn empty_csv_slot_still_occupies_its_stn_slot() {
        // Audio: slot 2 is empty; `fra` is STN slot 3 and is the commentary
        // the vendor pointed at with the 0-based CSV index 2.
        let feature = r#"<playlist name="Feature" aud="eng,,fra" aud_com1_idx="2" />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert_eq!(a.len(), 2, "the empty slot carries no label");
        assert_eq!(a[0].language, "eng");
        assert_eq!(a[0].stream_number, 1);
        assert_eq!(a[1].language, "fra");
        assert_eq!(
            a[1].stream_number, 3,
            "an empty CSV cell occupies STN slot 2, so `fra` is slot 3"
        );
        assert_eq!(a[1].purpose, LabelPurpose::Commentary);

        // Subtitles: same shape, and the consequence is a misplaced forced
        // flag. `forced_sub` index 2 is the forced-narrative track; with the
        // empty slot renumbered away it would be written onto STN slot 2.
        let feature = r#"<playlist name="Feature" sub="eng,,fra" forced_sub="0,0,3" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].language, "eng");
        assert_eq!(s[0].stream_number, 1);
        assert_eq!(s[0].qualifier, LabelQualifier::None);
        assert_eq!(s[1].language, "fra");
        assert_eq!(
            s[1].stream_number, 3,
            "the forced marker belongs to STN slot 3, not slot 2"
        );
        assert_eq!(s[1].qualifier, LabelQualifier::Forced);
    }

    #[test]
    fn empty_middle_slot_carries_no_label_but_keeps_its_slot() {
        // aud="eng,,fra": the empty middle cell yields no label — there is
        // nothing to label — but it still owns STN slot 2, so `fra` is slot
        // 3. (This test previously asserted 2, pinning the renumbering bug.)
        let feature = r#"<playlist name="Feature" aud="eng,,fra" />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].language, "eng");
        assert_eq!(a[0].stream_number, 1);
        assert_eq!(a[1].language, "fra");
        assert_eq!(a[1].stream_number, 3);
    }

    #[test]
    fn aud_com1_idx_trimmed_and_multivalue() {
        // Whitespace and multi-value lists must both resolve; com index is
        // positional against the raw CSV, so with an empty slot at position 1,
        // " 2 " marks 'fra' (CSV index 2, STN slot 3) as commentary.
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
        // sub="eng,eng,zho,ces" forced_sub="0,0,0,3": the forced marker is
        // positional on the raw CSV, so 'ces' (index 3) is forced; its
        // stream_number is its 1-based cell position, 4.
        let feature = r#"<playlist sub="eng,eng,zho,ces" forced_sub="0,0,0,3" />"#;
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

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: `name="Feature"` (case-insensitive) wins immediately.
    /// Mutation: use case-sensitive equality → "feature" (lowercase) not found.
    #[test]
    fn find_feature_name_match_case_insensitive() {
        let xml = r#"<playlist name="feature" aud="eng" />"#;
        let feature = find_feature_playlist(xml).expect("found");
        assert!(feature.contains("eng"));
    }

    /// Spec: when no name="Feature" present, most audio slots wins.
    /// Mutation: use first playlist instead of max-audio-count → wrong playlist chosen.
    #[test]
    fn find_feature_selects_most_audio_streams() {
        let xml = r#"
            <playlist name="Preview" aud="eng" />
            <playlist name="MainMovie" aud="eng,fra,spa,deu" />
            <playlist name="Short" aud="eng,fra" />
        "#;
        let feature = find_feature_playlist(xml).expect("found");
        assert!(feature.contains(r#"name="MainMovie""#));
    }

    // Spec: stream_number is the cell's own 1-based CSV position, since
    // empty cells are slots too. Mutation: count only non-empty cells →
    // every label behind an empty cell shifts one slot forward.
    #[test]
    fn audio_stream_numbering_uses_raw_csv_slot_position() {
        let feature = r#"<playlist name="Feature" aud="eng,,fra,,spa" />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert_eq!(a.len(), 3);
        assert_eq!(a[0].language, "eng");
        assert_eq!(a[0].stream_number, 1);
        assert_eq!(a[1].language, "fra");
        assert_eq!(a[1].stream_number, 3);
        assert_eq!(a[2].language, "spa");
        assert_eq!(a[2].stream_number, 5);
    }

    /// Spec: forced subtitle at the last position with gaps in between.
    /// raw CSV index 4 means the last subtitle (5th entry) is forced.
    /// Mutation: use stream_number (dense) instead of raw index → wrong subtitle forced.
    #[test]
    fn forced_sub_uses_raw_csv_index_with_gaps() {
        // sub="eng,,fra,,spa" forced_sub="0,0,0,0,3"
        // raw CSV index 4 = "spa", i.e. STN slot 5.
        let feature = r#"<playlist name="Feature" sub="eng,,fra,,spa" forced_sub="0,0,0,0,3" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].language, "eng");
        assert_eq!(s[0].qualifier, LabelQualifier::None);
        assert_eq!(s[1].language, "fra");
        assert_eq!(s[1].qualifier, LabelQualifier::None);
        assert_eq!(s[2].language, "spa");
        assert_eq!(s[2].qualifier, LabelQualifier::Forced);
        assert_eq!(s[2].stream_number, 5);
    }

    // Spec: aud_com1_idx is positional against the raw CSV, so an empty
    // gap before the index does not shift what's labeled commentary.
    // Mutation: use stream_number instead of raw CSV index.
    #[test]
    fn audio_commentary_index_raw_csv_position() {
        // aud="eng,,fra,spa" aud_com1_idx="2" → CSV index 2 = "fra",
        // which is STN slot 3.
        let feature = r#"<playlist name="Feature" aud="eng,,fra,spa" aud_com1_idx="2" />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert_eq!(a.len(), 3);
        assert_eq!(a[1].language, "fra");
        assert_eq!(a[1].stream_number, 3);
        assert_eq!(a[1].purpose, LabelPurpose::Commentary);
        assert_eq!(a[0].purpose, LabelPurpose::Normal);
        assert_eq!(a[2].purpose, LabelPurpose::Normal);
    }

    /// Spec: sub_com1_idx can be a comma-separated list with multiple values.
    /// Mutation: only parse the first value → multi-commentary subtitles missed.
    #[test]
    fn subtitle_commentary_multiple_indices() {
        let feature = r#"<playlist name="Feature" sub="eng,fra,spa,deu" sub_com1_idx="2,3" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 4);
        assert_eq!(s[0].purpose, LabelPurpose::Normal);
        assert_eq!(s[1].purpose, LabelPurpose::Normal);
        assert_eq!(s[2].purpose, LabelPurpose::Commentary); // index 2
        assert_eq!(s[3].purpose, LabelPurpose::Commentary); // index 3
    }

    /// Spec: an absent `aud` attribute means no audio labels are emitted.
    /// Mutation: default aud to "*" instead of None → spurious labels generated.
    #[test]
    fn feature_without_aud_attr_yields_no_audio_labels() {
        // Only subtitle data; no aud= attribute.
        let feature = r#"<playlist name="Feature" sub="eng,fra" />"#;
        let labels = labels_from_feature(feature);
        let a = audio(&labels);
        assert!(a.is_empty(), "no audio labels when aud is absent");
        let s = subs(&labels);
        assert_eq!(s.len(), 2);
    }

    /// Spec: an absent `sub` attribute means no subtitle labels are emitted.
    /// Mutation: default sub to "*" → spurious labels generated.
    #[test]
    fn feature_without_sub_attr_yields_no_subtitle_labels() {
        let feature = r#"<playlist name="Feature" aud="eng" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert!(s.is_empty(), "no subtitle labels when sub is absent");
    }

    /// Spec: audio stream_number is the cell's 1-based position and never
    /// wraps; past the u16 space the parser stops emitting.
    /// Mutation: cast `i + 1` to u16 → stream numbers wrap to 0, skipping apply.
    #[test]
    fn audio_stream_number_never_wraps() {
        // 65535 tracks is impossible on a real disc but must not panic/produce 0.
        // 300 slots is sufficient to exercise the number-assignment logic
        // via labels_from_feature without building the full 65535-entry CSV.
        let aud: String = (0..300).map(|_| "eng").collect::<Vec<_>>().join(",");
        let feature = format!(r#"<playlist name="Feature" aud="{}" />"#, aud);
        let labels = labels_from_feature(&feature);
        assert_eq!(labels.len(), 300);
        // Numbers must be strictly increasing, never 0.
        let mut last = 0u16;
        for l in &labels {
            if let Some(t) = l.stream_number.checked_sub(last) {
                assert!(t > 0, "stream_number must be strictly increasing");
            }
            last = l.stream_number;
        }
        assert_eq!(last, 300);
    }

    /// Spec: a `forced_sub` cell with surrounding whitespace still classifies.
    /// Mutation: drop the `trim()` → " 3 " falls through to the unrecognised
    /// arm and the disc's forced-narrative track loses its label.
    #[test]
    fn forced_sub_cells_are_trimmed_before_classification() {
        let feature = r#"<playlist name="Feature" sub="eng,fra,spa" forced_sub="0, 3 , 1 " />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s[0].qualifier, LabelQualifier::None);
        assert_eq!(s[1].qualifier, LabelQualifier::Forced);
        assert_eq!(s[2].qualifier, LabelQualifier::None);
    }

    // `forced_sub` is an enumeration; `1` means "full dialogue track that
    // also carries forced signs", NOT "this track is forced" — see
    // docs/paramount.md. Mutation: `"1" => ForcedNarrative` (old reading).
    #[test]
    fn a_contains_forced_segments_cell_is_not_a_forced_track() {
        let feature = r#"<playlist name="Feature" sub="eng,ces,deu" forced_sub="0,1,1" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 3);
        assert!(
            s.iter().all(|l| l.qualifier == LabelQualifier::None),
            "a `1` marks a full track containing forced signs, not a forced track"
        );
    }

    // `2` and `3` are the cells that DO name a dedicated forced-narrative
    // track, and the old boolean reading discarded both — see
    // docs/paramount.md. Mutation: drop either arm of the `"2" | "3"` match.
    #[test]
    fn a_dedicated_forced_narrative_cell_is_a_forced_track() {
        // The measured shape: full tracks first, their forced companions in
        // trailing slots of the same languages.
        let feature =
            r#"<playlist name="Feature" sub="eng,cat,jpn,cat,jpn" forced_sub="0,0,0,2,3" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 5);
        assert_eq!(s[1].qualifier, LabelQualifier::None, "the full cat track");
        assert_eq!(s[2].qualifier, LabelQualifier::None, "the full jpn track");
        assert_eq!(s[3].qualifier, LabelQualifier::Forced, "cat forced slot");
        assert_eq!(s[3].stream_number, 4);
        assert_eq!(s[4].qualifier, LabelQualifier::Forced, "jpn forced slot");
        assert_eq!(s[4].stream_number, 5);
    }

    // An unrecognised cell must fall to NOT forced — asserting forced is
    // the expensive mistake (see docs/paramount.md). Mutation:
    // `_ => ForcedNarrative`, or treating "any non-zero" as forced.
    #[test]
    fn an_unrecognised_forced_sub_cell_is_not_forced() {
        let feature = r#"<playlist name="Feature" sub="eng,fra,spa,ita" forced_sub="4,x,,-1" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s.len(), 4);
        assert!(s.iter().all(|l| l.qualifier == LabelQualifier::None));
        // ...and so must a cell the CSV simply does not reach.
        let feature = r#"<playlist name="Feature" sub="eng,fra" forced_sub="0" />"#;
        let labels = labels_from_feature(feature);
        assert_eq!(subs(&labels)[1].qualifier, LabelQualifier::None);
    }

    /// Spec: `find_feature_playlist` returns None when XML has no `<playlist>` elements.
    /// Mutation: return a default struct instead of None → downstream code mislabels.
    #[test]
    fn find_feature_returns_none_on_empty_xml() {
        assert!(find_feature_playlist("").is_none());
        assert!(find_feature_playlist("<root />").is_none());
    }

    // Spec: on a tie in audio-slot count, the FIRST playlist wins (see
    // docs/paramount.md). Mutation: `count > best_aud_count` ->
    // `count >= best_aud_count` lets a later tie silently displace it.
    #[test]
    fn find_feature_first_wins_on_audio_count_tie() {
        let xml = r#"
            <playlist name="A" aud="eng,fra" />
            <playlist name="B" aud="deu,spa" />
        "#;
        let feature = find_feature_playlist(xml).expect("a feature is found");
        assert!(
            feature.contains(r#"name="A""#),
            "first playlist must win a tie, got: {feature}"
        );
    }
}
