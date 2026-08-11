//! Paramount/onQ — `playlists.xml`
//!
//! Richest structured format. Complete language lists with forced flags
//! and commentary indices per playlist, all in XML attributes.
//!
//! NOT A SPECIFICATION. `/BDMV/JAR/` is application-defined space, so this
//! file is one authoring house's internal metadata that happens to ship on
//! the pressing. There is nothing to look up: every field meaning here was
//! derived by measuring real discs and cross-checking against per-display-set
//! content. Treat an unfamiliar value as unknown rather than guessing — the
//! disc's own `forced_on_flag` is the only authoritative forced signal.
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
    // High confidence: this format is fully structured and we extract
    // every field whose meaning the corpus establishes. "Documented" would
    // be the wrong word — see the module note; nothing about it is.
    Some(ParseResult::high(labels))
}

/// One cell of the `forced_sub` CSV.
///
/// The attribute reads like a boolean and was parsed as one (`cell == "1"` →
/// forced). It is not. Every image in the corpus carrying this vendor's
/// `playlists.xml` — seven distinct discs — uses four values, and decoding
/// three of those discs' feature subtitle tracks and counting every PGS
/// display set separates them into two populations two orders of magnitude
/// apart:
///
///   * `0` — a subtitle track with no forced-narrative content. On the two
///     discs measured that use the flag at all, not one `0` track carried a
///     single `forced_on_flag` display set.
///   * `1` — a FULL DIALOGUE track that additionally contains some
///     forced-narrative signs. On one measured disc, all nine `1` cells are
///     full tracks of 949-1411 display sets, eight of them carrying 5-14
///     flagged sets and the ninth none; that disc has no dedicated forced
///     track at all. On another, all seven `1` cells are full tracks of
///     1602-1651 display sets carrying 0-31 flagged sets. Reading `1` as
///     forced is what made one language present as two identical full
///     subtitle tracks with one of them flagged forced.
///   * `2` and `3` — a DEDICATED forced-narrative track. These take their own
///     trailing STN slots, one per localized language, duplicating a language
///     that already holds a full track earlier in the list. Measured: the two
///     `2` slots on one disc are 15 and 10 display sets, EVERY one flagged
///     forced, against ~1600 on that disc's full tracks; the four `3` slots on
///     another are 7, 14, 23 and 59 display sets against 1216-2655. What
///     distinguishes `2` from `3` the corpus does not reveal — both sit in the
///     same trailing position, both measure the same shape, and one disc uses
///     each for a different language — so both map alike.
///
/// So the old reading was wrong in BOTH directions: it flagged full dialogue
/// tracks forced, and it discarded the cells that name the real forced tracks.
///
/// The `1` case is deliberately NOT carried through as a weaker "contains
/// forced segments" hint. There is no qualifier for that, and the asymmetry
/// argues against inventing one here: a wrong forced flag on a 30 MB dialogue
/// track is the user-visible defect, while a missing hint costs nothing.
///
/// An unrecognised cell maps to [`ForcedSub::None`] — the conservative
/// direction, since asserting forced is the expensive mistake.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ForcedSub {
    /// No forced-narrative content, or an unrecognised cell.
    None,
    /// A full dialogue track that also carries forced-narrative segments.
    ContainsForcedSegments,
    /// A dedicated forced-narrative track.
    ForcedNarrative,
}

/// The most `*_com1_idx` entries worth parsing from one playlist.
///
/// These indices address CSV cell positions, and a cell is only addressable
/// while its 1-based number fits a `u16` — the loops below `break` at
/// `u16::try_from(i + 1)`. So an index at or beyond `u16::MAX` can never match
/// a cell, and more than that many entries cannot describe anything new.
///
/// The bound is what makes the parse safe on hostile input, not merely fast.
/// The `HashSet` that replaced a linear scan fixed the LOOKUP cost, but the
/// set is still built from an attribute with no length limit: a disc
/// declaring half a billion indices allocates half a billion entries before
/// any lookup happens. Real authoring is nowhere near this — the BD STN table
/// admits at most 32 streams per playlist — so nothing legitimate is lost.
const MAX_COM_INDICES: usize = u16::MAX as usize;

/// Parse a `*_com1_idx` attribute into the set the labelling loops query.
///
/// Extracted so the BOUND is observable. Asserting it through
/// `labels_from_feature` is not possible: a `HashSet` collapses repeated
/// values, and an out-of-range index changes no label either way, so such a
/// test passes whether or not the cap exists — an assertion that cannot fail.
/// Returning the set lets a test hand in tens of thousands of DISTINCT
/// unaddressable indices and see them refused.
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

fn forced_sub_cell(cell: &str) -> ForcedSub {
    match cell.trim() {
        "1" => ForcedSub::ContainsForcedSegments,
        "2" | "3" => ForcedSub::ForcedNarrative,
        _ => ForcedSub::None,
    }
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
        let com_indices = com_indices(xml::attr(feature, "aud_com1_idx"));

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
        let forced: Vec<ForcedSub> = xml::attr(feature, "forced_sub")
            .map(|s| s.split(',').map(forced_sub_cell).collect())
            .unwrap_or_default();

        // HashSet for the same reason as the audio side above: unbounded
        // parsed input, membership-only use, linear scan once per stream.
        let com_indices = com_indices(xml::attr(feature, "sub_com1_idx"));

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

    /// `sub_com1_idx` is parsed straight out of the disc's `playlists.xml`,
    /// which is attacker-controlled and has no length bound of its own.
    ///
    /// This replaces a WALL-CLOCK test. That one built a 200 000 x 1 000 001
    /// fixture and failed if it took over 10 s, to prove the membership test
    /// was a set rather than a linear scan. Measured on the machine that
    /// wrote this: 1.62 s alone, and OVER 10 s — a real failure — when the
    /// suite's other 3 347 tests were running concurrently. A 6x margin
    /// against a shared CPU is not a margin; it is a CI failure that looks
    /// like a flake and gets re-run until it passes.
    ///
    /// It also measured the wrong thing. Making the lookup O(1) bounded the
    /// QUERY, not the PARSE: the set was still built from every entry the
    /// disc declared, so a hostile playlist could still force an unbounded
    /// allocation before any lookup happened. `MAX_COM_INDICES` bounds that,
    /// and this test asserts the bound directly — an equality check with no
    /// clock in it, which cannot flake under any load.
    #[test]
    fn a_hostile_commentary_index_list_is_bounded_not_merely_fast() {
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

    /// The set REFUSES unaddressable indices, so a hostile playlist cannot
    /// inflate it. DISTINCT values on purpose: a `HashSet` collapses repeats,
    /// so a million copies of one index costs one entry and would prove
    /// nothing. Fifty thousand distinct out-of-range indices cost fifty
    /// thousand entries without the filter, and none with it — so this test
    /// goes red if the bound is removed, which the label-level assertions
    /// below cannot do.
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

    /// An index that cannot address any cell is dropped rather than stored.
    ///
    /// `u16::MAX` and beyond can never match, because the labelling loop
    /// stops at `u16::try_from(i + 1)`. Keeping such entries would let a disc
    /// inflate the set with values that can never be looked up — the
    /// allocation half of the same defect.
    #[test]
    fn an_index_that_cannot_address_a_cell_is_not_retained() {
        let feature = format!(
            r#"<playlist name="Feature" sub="eng,eng" sub_com1_idx="1,{},{}" />"#,
            MAX_COM_INDICES,
            MAX_COM_INDICES + 1
        );
        let labels = labels_from_feature(&feature);
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].purpose, LabelPurpose::Normal);
        assert_eq!(
            labels[1].purpose,
            LabelPurpose::Commentary,
            "the addressable index must still be honoured"
        );
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

    /// `forced_sub` is an enumeration, and `1` is its "full dialogue track that
    /// also carries forced signs" value — NOT "this track is forced".
    ///
    /// Measured on a disc whose feature declares nine `1` cells among 32
    /// subtitle slots: all nine are full dialogue tracks of 949-1411 display
    /// sets, and the disc has no dedicated forced track at all. Reading `1` as
    /// forced is what produced two identical full subtitle tracks for one
    /// language with one of them flagged forced.
    ///
    /// Nothing downstream can undo this on the discs that need it most:
    /// `mux::codec::pgs::demotable` may only clear a vendor forced label where
    /// some track on the disc demonstrably sets `forced_on_flag`, and measured
    /// discs using this label format never set it.
    ///
    /// Mutation: `"1" => ForcedNarrative` (the old reading) → red.
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

    /// `2` and `3` are the cells that DO name a dedicated forced-narrative
    /// track, and the old boolean reading discarded both.
    ///
    /// Measured: these cells occupy their own trailing STN slots, one per
    /// localized language, duplicating a language that already holds a full
    /// track earlier in the list. On one measured disc the four `3` slots carry
    /// 7, 14, 23 and 59 display sets against 1216-2655 on the full tracks they
    /// duplicate — and not one display set anywhere on that disc carries
    /// `forced_on_flag`, so neither the scan probe nor the muxer can promote
    /// them from content. The vendor cell is the only evidence there is.
    ///
    /// Mutation: drop either arm of the `"2" | "3"` match → red.
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

    /// An unrecognised cell must fall to NOT forced. Asserting forced is the
    /// expensive mistake (a full dialogue track a player then burns on screen),
    /// so an unknown value from a future authoring revision must not be able to
    /// make that claim.
    ///
    /// Mutation: `_ => ForcedNarrative`, or treating "any non-zero" as forced.
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
