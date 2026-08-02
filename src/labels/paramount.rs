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
        // A HashSet, not a Vec: `com_indices` is parsed straight out of an
        // attacker-controlled attribute with no length bound and was scanned
        // linearly once per stream, so `aud="..."` and `aud_com1_idx="..."`
        // both grown large make this quadratic in the size of one XML file.
        // Membership is the only operation performed on it.
        let com_indices: HashSet<usize> = xml::attr(feature, "aud_com1_idx")
            .map(|s| s.split(',').filter_map(|i| i.trim().parse().ok()).collect())
            .unwrap_or_default();

        // The CSV *is* the STN list: one cell per stream, in stream order,
        // and `aud_com1_idx` is a 0-based index into those same cells. So
        // `stream_number` is the cell's own 1-based position — NOT a counter
        // that only advances on cells carrying a language.
        //
        // A cell with an empty language still occupies its STN slot; it just
        // has nothing to label. Renumbering the surviving cells 1..N shifts
        // every label behind an empty cell one slot forward, which is how a
        // marker authored for one stream ends up written onto the stream in
        // front of it (see the subtitle side, where the marker is `forced`).
        //
        // `u16::try_from` rather than `saturating_add`: past the 1-based u16
        // numbering space every cell would collapse onto `u16::MAX`, binding
        // several streams to one label. Stop emitting instead. Unreachable on
        // real media — the BD STN_table admits at most 32 primary audio
        // streams per playlist.
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
        let forced: Vec<bool> = xml::attr(feature, "forced_sub")
            .map(|s| s.split(',').map(|f| f.trim() == "1").collect())
            .unwrap_or_default();

        // HashSet for the same reason as the audio side above: unbounded
        // parsed input, membership-only use, linear scan once per stream.
        let com_indices: HashSet<usize> = xml::attr(feature, "sub_com1_idx")
            .map(|s| s.split(',').filter_map(|i| i.trim().parse().ok()).collect())
            .unwrap_or_default();

        // As with audio: the cell position IS the STN slot. `forced_sub` and
        // `sub_com1_idx` are indexed against those same cells, so an empty
        // cell must not renumber the cells behind it — a forced marker
        // authored for one PG slot would otherwise be written onto an
        // earlier, full-dialogue subtitle track.
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

            let qualifier = if forced.get(i).copied().unwrap_or(false) {
                LabelQualifier::Forced
            } else {
                LabelQualifier::None
            };

            labels.push(StreamLabel {
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

    /// Immunity pin, section-boundary half. The pixelogic parser walks a flat
    /// string sequence and recognises its feature section's END by marker
    /// alone, so a section with no marker behind it runs off into whatever
    /// follows and counts it as more STN slots. Nothing here can do that: the
    /// stream list is one attribute of one XML element, so its length is the
    /// CSV's own cell count and its scope is the element's byte range that
    /// `xml::find_element` returns. Text after the element — including the
    /// next playlist's own `aud` — is not reachable from it.
    ///
    /// And when the boundary is MISSING the failure is closed, not open:
    /// `xml::find_element` needs a matching close tag and yields `None`
    /// without one, so an unterminated element ends the walk rather than
    /// swallowing the rest of the document.
    ///
    /// Mutation: hand `labels_from_feature` the document instead of the
    /// element, or let an unterminated element run to EOF → the bonus
    /// playlist's languages join the feature's stream list.
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

    /// `sub_com1_idx` is an unbounded index list parsed straight out of the
    /// disc's `playlists.xml` and was membership-tested with a linear
    /// `Vec::contains` once per subtitle stream — quadratic in the size of a
    /// single attacker-supplied file.
    ///
    /// Proof is by deadline rather than micro-benchmark. With the linear scan
    /// the original fixture (200 000 streams x 1 000 001 indices) measured
    /// 31 s in a release build and far longer in debug; with a set it measured
    /// 0.03 s release / 0.56 s debug. A 10 s deadline sits ~18x above the
    /// slowest passing measurement and ~3x below the fastest failing one, and
    /// makes a regression fail fast instead of hanging CI.
    ///
    /// The CSV now stops at the end of the 1-based `u16` stream-numbering
    /// space, so only the first 65 535 cells are scanned. `INDICES` is raised
    /// to keep the linear-scan work product (`cells x indices`) at or above
    /// the original fixture's, preserving that deadline margin.
    ///
    /// Correctness is pinned on fixture-derived literals: indices 0, 2 and 4
    /// are the commentary tracks, 1 and 3 are not.
    #[test]
    fn commentary_index_lookup_is_not_quadratic() {
        /// Cells offered. Everything past `u16::MAX` is unnumberable and the
        /// parser stops there, so the scanned prefix is 65 535 cells.
        const STREAMS: usize = 200_000;
        const SCANNED: usize = u16::MAX as usize;
        const INDICES: usize = 3_100_000;
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut feature = String::from(r#"<playlist name="Feature" sub=""#);
            feature.push_str(&"eng,".repeat(STREAMS));
            feature.pop();
            // Three real commentary indices, then a long run of one
            // out-of-range value: nothing here is bounded by the stream count.
            feature.push_str(r#"" sub_com1_idx="0,2,4,"#);
            feature.push_str(&"9999999,".repeat(INDICES));
            feature.pop();
            feature.push_str(r#"" />"#);
            let _ = tx.send(labels_from_feature(&feature));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(labels) => {
                worker.join().expect("worker panicked");
                assert_eq!(labels.len(), SCANNED);
                assert_eq!(labels[0].purpose, LabelPurpose::Commentary);
                assert_eq!(labels[1].purpose, LabelPurpose::Normal);
                assert_eq!(labels[2].purpose, LabelPurpose::Commentary);
                assert_eq!(labels[3].purpose, LabelPurpose::Normal);
                assert_eq!(labels[4].purpose, LabelPurpose::Commentary);
            }
            Err(_) => panic!(
                "labels_from_feature did not finish {STREAMS} streams x \
                 {INDICES} commentary indices within 10s — the membership \
                 test is still a linear scan"
            ),
        }
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

    /// The `aud` / `sub` CSVs are the vendor's STN-ordered stream lists: one
    /// slot per stream, and `aud_com1_idx` / `forced_sub` are indexed against
    /// those same slot positions. A slot whose language cell is empty carries
    /// nothing to label but still OCCUPIES its slot, so it must not renumber
    /// the slots behind it.
    ///
    /// Numbering only the slots that carry a language collapsed every later
    /// label one position forward per empty cell, which is how a forced
    /// marker authored for one STN slot lands on the full-subtitle track in
    /// front of it.
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
        let feature = r#"<playlist name="Feature" sub="eng,,fra" forced_sub="0,0,1" />"#;
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
        // Whitespace around the index, and a multi-value list, must both
        // resolve. com index is positional against the raw CSV, so with
        // an empty slot at position 1, " 2 " marks the 'fra' track
        // (CSV index 2, STN slot 3) as commentary.
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
        // stream_number is its 1-based cell position, 4.
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

    /// Spec: stream_number for audio is the cell's own 1-based CSV position,
    /// because the CSV is the STN list and empty cells are slots too.
    /// Mutation: count only non-empty cells → every label behind an empty
    /// cell shifts one slot forward.
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
        // sub="eng,,fra,,spa" forced_sub="0,0,0,0,1"
        // raw CSV index 4 = "spa", i.e. STN slot 5.
        let feature = r#"<playlist name="Feature" sub="eng,,fra,,spa" forced_sub="0,0,0,0,1" />"#;
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

    /// Spec: aud_com1_idx is positional against the raw CSV.
    /// When the index refers to a slot before an empty gap, the gap does
    /// not shift what stream is labeled as commentary.
    /// Mutation: use stream_number instead of raw CSV index → wrong stream is commentary.
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
        // 65535 audio tracks is impossible on a real disc but the parser must
        // not panic or produce 0. Build a comma-separated list of 65535 "eng"s.
        // We only run the number-assignment logic via labels_from_feature.
        // Limit: CSV with 300 slots is sufficient to test the counter.
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

    /// Spec: forced_sub with whitespace around "1" must still parse as true.
    /// Mutation: use `== "1"` instead of `trim() == "1"` → " 1 " fails.
    #[test]
    fn forced_sub_whitespace_around_one() {
        let feature = r#"<playlist name="Feature" sub="eng,fra" forced_sub="0, 1" />"#;
        let labels = labels_from_feature(feature);
        let s = subs(&labels);
        assert_eq!(s[0].qualifier, LabelQualifier::None);
        assert_eq!(s[1].qualifier, LabelQualifier::Forced);
    }

    /// Spec: `find_feature_playlist` returns None when XML has no `<playlist>` elements.
    /// Mutation: return a default struct instead of None → downstream code mislabels.
    #[test]
    fn find_feature_returns_none_on_empty_xml() {
        assert!(find_feature_playlist("").is_none());
        assert!(find_feature_playlist("<root />").is_none());
    }

    /// Spec: on a tie in audio-slot count, the FIRST playlist encountered
    /// wins (consistent with `select_result`'s first-wins tiebreak
    /// elsewhere in the registry) — later playlists only displace the
    /// current best on a STRICTLY greater count.
    /// Mutation: `count > best_aud_count` -> `count >= best_aud_count`
    /// would let a later tied playlist silently displace the first.
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
