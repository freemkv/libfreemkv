//! Warner CTRM — `menu_base.prop` and/or `language_streams.txt`
//!
//! Two sub-formats from the same framework. A disc may have one or both.
//! When both exist, language_streams.txt provides structured types while
//! menu_base.prop provides stream number → button name mapping.

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashMap;

/// Cheap signature check: a CTRM disc ships `menu_base.prop` and/or
/// `language_streams.txt` inside a `/BDMV/JAR/*` archive.
pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "menu_base.prop")
        || super::jar_file_exists(udf, "language_streams.txt")
}

/// Full extraction: parses `language_streams.txt` (structured types) and
/// `menu_base.prop` (stream numbers + button names), merging when both
/// are present. Returns `None` when neither file is present/parseable or
/// no labels result.
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    // Try language_streams.txt first (richer structured data)
    let ls_labels = parse_language_streams(reader, udf);

    // Try menu_base.prop (stream numbers + key names)
    let mb_labels = parse_menu_base(reader, udf);

    // If we have both, merge: language_streams for structure, menu_base for names
    let labels = match (ls_labels, mb_labels) {
        (Some(ls), Some(mb)) => merge(ls, mb),
        (Some(ls), None) => ls,
        (None, Some(mb)) => mb,
        (None, None) => return None,
    };
    if labels.is_empty() {
        return None;
    }
    // High confidence: both language_streams.txt and menu_base.prop
    // are structured key-value formats with documented types.
    Some(ParseResult::high(labels))
}

fn merge(ls: Vec<StreamLabel>, mb: Vec<StreamLabel>) -> Vec<StreamLabel> {
    // language_streams has better type/purpose data, menu_base has button names
    // Match by stream number + type, take name from menu_base
    let mut result = ls;
    for label in &mut result {
        if let Some(mb_match) = mb
            .iter()
            .find(|m| m.stream_type == label.stream_type && m.stream_number == label.stream_number)
            && label.name.is_empty()
            && !mb_match.name.is_empty()
        {
            label.name = mb_match.name.clone();
        }
    }
    // Append menu_base-only streams (present in mb but not in ls by
    // (stream_type, stream_number)); language_streams is authoritative for
    // type/purpose but not necessarily a superset of menu_base.
    for mb_label in mb {
        let already = result.iter().any(|l| {
            l.stream_type == mb_label.stream_type && l.stream_number == mb_label.stream_number
        });
        if !already {
            result.push(mb_label);
        }
    }
    result
}

// Prefix denotes a commentary group if it has a `commentary`/`comm`
// segment (split on `_`) — not a substring scan, which used to
// over-match `common_*`/`community_*`.
fn prefix_is_commentary(prefix: &str) -> bool {
    prefix
        .split('_')
        .any(|seg| seg.eq_ignore_ascii_case("commentary") || seg.eq_ignore_ascii_case("comm"))
}

// ── language_streams.txt parser ────────────────────────────────────────────

fn parse_language_streams(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "language_streams.txt")?;
    let text = std::str::from_utf8(&data).ok()?;
    let labels = parse_language_streams_text(text);
    if labels.is_empty() {
        return None;
    }
    Some(labels)
}

// Parses language_streams.txt body into labels. Split from
// parse_language_streams (I/O + UTF-8 decode only) so tests below
// exercise this production code, not a hand-copied duplicate.
fn parse_language_streams_text(text: &str) -> Vec<StreamLabel> {
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
        // STN indices are 1-based; apply_labels pre-increments from 0 and
        // never matches a 0, so a 0 here would emit a dead label. Skip it
        // (matching the `n > 0` guard in parse_menu_base).
        let stream_num: u16 = match parts[2].parse() {
            Ok(n) if n > 0 => n,
            _ => continue,
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
                // Purpose variants
                "eda" => final_purpose = LabelPurpose::Descriptive,
                // Dialect variants — pass through raw code from disc
                "csp" | "cs" | "lsp" | "ls" | "cf" | "pf" | "bp" | "pp" => {
                    variant_code = variant.clone();
                }
                // Everything else: defer to vocab::codec as the single source of
                // codec-name truth — a known token gets its canonical name,
                // otherwise it's stored as-is.
                _ => codec_hint = vocab::codec(&variant).to_string(),
            }
        }

        labels.push(StreamLabel {
            stream_id: None,
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

    labels
}

// NOTE: `parse_menu_base` / `parse_menu_base_text` structurally belong above this
// module but are left below (lint allowed) — a ~120-line move is safer as its
// own focused change.
#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;

    /// Run the real shipping parser ([`parse_menu_base_text`]) on a
    /// menu_base.prop body so tests exercise production code directly.
    fn parse_props(text: &str) -> Vec<StreamLabel> {
        parse_menu_base_text(text)
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

    #[test]
    fn dual_flag_entry_resolves_to_audio_with_no_subtitle_qualifier() {
        // An entry tripping BOTH flags (audio_ prefix sets is_audio, class
        // "SubtitleButton" sets is_subtitle): audio wins the type, and the
        // subtitle qualifier (SDH) must NOT carry onto the Audio label.
        let labels = parse_props(
            "audio_1.class=SubtitleButton\n\
             audio_1.streamNumber=6\n\
             audio_1.name=English SDH\n",
        );
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
        assert_eq!(labels[0].qualifier, LabelQualifier::None);
    }

    // Spec: lines are skipped when is_empty() || starts_with('#') —
    // either alone suffices. Mutation `||`->`&&` would let a
    // commented-out key=value line fall through and get parsed.
    #[test]
    fn menu_base_comment_line_with_equals_is_still_skipped() {
        let labels = parse_props(
            "#audio_1.class=AudioButton\n\
             #audio_1.streamNumber=9\n\
             #audio_1.name=Should Not Appear\n\
             audio_2.class=AudioButton\n\
             audio_2.streamNumber=1\n\
             audio_2.name=Real Track\n",
        );
        assert_eq!(labels.len(), 1, "commented-out entry must not be parsed");
        assert_eq!(labels[0].name, "Real Track");
    }

    // Spec: streamNumber must be strictly positive (0 = "no STN
    // entry"), matching the language_streams `n > 0` guard. Mutation
    // `n>0`->`n>=0` emits a dead label apply_labels never matches.
    #[test]
    fn menu_base_zero_stream_number_skipped() {
        let labels = parse_props(
            "audio_1.class=AudioButton\n\
             audio_1.streamNumber=0\n\
             audio_1.name=Disabled Slot\n",
        );
        assert!(
            labels.is_empty(),
            "streamNumber=0 must be skipped, got {labels:?}"
        );
    }

    // Spec: is_subtitle = class contains SubtitleButton ||
    // prefix.starts_with("subtitle_") — either alone suffices.
    // Mutation `||`->`&&` drops entries with a non-`subtitle_` prefix.
    #[test]
    fn menu_base_subtitle_class_alone_is_sufficient() {
        let labels = parse_props(
            "menuBtn7.class=SubtitleButton\n\
             menuBtn7.streamNumber=1\n\
             menuBtn7.name=English SDH\n",
        );
        assert_eq!(
            labels.len(),
            1,
            "class=SubtitleButton alone must classify as subtitle, not be dropped"
        );
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
    }

    #[test]
    fn prefix_commentary_segment_match_not_substring() {
        // Genuine commentary group segments match.
        assert!(prefix_is_commentary("audio_commentary"));
        assert!(prefix_is_commentary("audio_commentary_1"));
        assert!(prefix_is_commentary("comm"));
        // Substring-only prefixes must NOT match (the over-match bug).
        assert!(!prefix_is_commentary("common"));
        assert!(!prefix_is_commentary("audio_common_1"));
        assert!(!prefix_is_commentary("community"));
        assert!(!prefix_is_commentary("audio_1"));
    }

    fn lbl(t: StreamLabelType, n: u16, name: &str) -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: n,
            stream_type: t,
            language: String::new(),
            name: name.to_string(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }
    }

    // Spec: merge matches mb entries by (type AND number) together —
    // either alone isn't a unique key. Mutation `&&`->`||` lets
    // .find() (first match) pick the right type but wrong number.
    #[test]
    fn merge_matches_mb_entry_by_type_and_number_together() {
        // ls wants audio #2 (empty name, so it will borrow from mb).
        let ls = vec![lbl(StreamLabelType::Audio, 2, "")];
        // mb's FIRST audio entry is #1 (wrong number); its #2 entry (the
        // real match) comes second.
        let mb = vec![
            lbl(StreamLabelType::Audio, 1, "Wrong Number Match"),
            lbl(StreamLabelType::Audio, 2, "Correct Match"),
        ];
        let merged = merge(ls, mb);
        assert_eq!(
            merged.len(),
            2,
            "mb's own audio #1 must also survive as its own entry"
        );
        let a2 = merged
            .iter()
            .find(|l| l.stream_type == StreamLabelType::Audio && l.stream_number == 2)
            .unwrap();
        assert_eq!(
            a2.name, "Correct Match",
            "must match mb by (type AND number), not type or number alone"
        );
    }

    #[test]
    fn merge_preserves_menu_base_only_streams() {
        // language_streams covers audio 1; menu_base has audio 1 (name)
        // AND a menu_base-only audio 2. The merge must keep audio 2 —
        // the both-files path previously dropped it.
        let ls = vec![lbl(StreamLabelType::Audio, 1, "")];
        let mb = vec![
            lbl(StreamLabelType::Audio, 1, "Main"),
            lbl(StreamLabelType::Audio, 2, "Commentary"),
        ];
        let merged = merge(ls, mb);
        assert_eq!(merged.len(), 2, "menu_base-only stream must survive");
        // ls audio 1 takes its name from mb.
        let a1 = merged
            .iter()
            .find(|l| l.stream_type == StreamLabelType::Audio && l.stream_number == 1)
            .unwrap();
        assert_eq!(a1.name, "Main");
        // mb-only audio 2 is appended.
        assert!(
            merged
                .iter()
                .any(|l| l.stream_number == 2 && l.name == "Commentary")
        );
    }

    // ── Additional hardening tests: language_streams.txt parser ──────────────

    /// Spec: `audio_production` line → Audio / Normal / no qualifier.
    /// Mutation: misparse `audio_production` as subtitle → Audio fails assertion.
    #[test]
    fn ls_audio_production_parsed() {
        let labels = parse_language_streams_text("id1,audio_production,1,eng\n");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
        assert_eq!(labels[0].purpose, LabelPurpose::Normal);
        assert_eq!(labels[0].qualifier, LabelQualifier::None);
        assert_eq!(labels[0].language, "eng");
        assert_eq!(labels[0].stream_number, 1);
    }

    /// Spec: `audio_commentary` line → Audio / Commentary.
    /// Mutation: change purpose to Normal → commentary track not flagged.
    #[test]
    fn ls_audio_commentary_parsed() {
        let labels = parse_language_streams_text("id2,audio_commentary,3,eng\n");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
        assert_eq!(labels[0].purpose, LabelPurpose::Commentary);
    }

    /// Spec: `audio_ime` → Audio / Ime (secondary music track).
    /// Mutation: remove Ime variant → purpose stays Normal.
    #[test]
    fn ls_audio_ime_parsed() {
        let labels = parse_language_streams_text("id3,audio_ime,2,jpn\n");
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
        assert_eq!(labels[0].purpose, LabelPurpose::Ime);
    }

    /// Spec: `subtitle_narrative` → Subtitle / Forced qualifier (forced narrative).
    /// Mutation: don't set Forced on narrative → forced flag not propagated.
    #[test]
    fn ls_subtitle_narrative_is_forced() {
        let labels = parse_language_streams_text("id4,subtitle_narrative,1,eng\n");
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(labels[0].qualifier, LabelQualifier::Forced);
    }

    // See docs/ctrm.md — immunity pin: unlike `paramount`'s forced_sub
    // flag, forced-ness is the row's stream-kind token, so a full
    // dialogue track can never acquire it (Forced qualifier misuse).
    #[test]
    fn a_full_subtitle_track_kind_can_never_carry_the_forced_qualifier() {
        // Every subtitle kind in the vocabulary, one row each.
        let text = "id1,subtitle_production,1,eng\n\
                    id2,subtitle_commentary,2,eng\n\
                    id3,subtitle_dual,3,eng\n\
                    id4,subtitle_bonus,4,eng\n\
                    id5,subtitle_ime,5,kor\n\
                    id6,subtitle_narrative,6,eng\n\
                    id7,subtitle_ime_narrative,7,kor\n";
        let labels = parse_language_streams_text(text);
        let forced: Vec<&str> = labels
            .iter()
            .filter(|l| l.qualifier == LabelQualifier::Forced)
            .map(|l| l.language.as_str())
            .collect();
        assert_eq!(
            forced.len(),
            2,
            "only the two narrative kinds are forced, got {forced:?}"
        );
        // The full dialogue kind specifically.
        let production = parse_language_streams_text("id,subtitle_production,1,eng\n");
        assert_eq!(production[0].qualifier, LabelQualifier::None);
    }

    /// Spec: `subtitle_commentary` → Subtitle / Commentary.
    /// Mutation: treat as Normal → subtitle commentary not flagged.
    #[test]
    fn ls_subtitle_commentary_parsed() {
        let labels = parse_language_streams_text("id5,subtitle_commentary,4,eng\n");
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(labels[0].purpose, LabelPurpose::Commentary);
    }

    /// Spec: `subtitle_ime_narrative` → Subtitle / Ime / Forced.
    /// Mutation: miss Forced → forced subtitles not identified.
    #[test]
    fn ls_subtitle_ime_narrative_is_ime_and_forced() {
        let labels = parse_language_streams_text("id6,subtitle_ime_narrative,2,kor\n");
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(labels[0].purpose, LabelPurpose::Ime);
        assert_eq!(labels[0].qualifier, LabelQualifier::Forced);
    }

    /// Spec: stream_num=0 is SKIPPED (0 means "no STN entry"; apply_labels
    /// starts from 1). Mutation: allow 0 → dead label emitted, never matched.
    #[test]
    fn ls_zero_stream_num_skipped() {
        let labels = parse_language_streams_text("id,audio_production,0,eng\n");
        assert!(labels.is_empty(), "stream_num=0 must be skipped");
    }

    /// Spec: a non-numeric stream_num is skipped (malformed disc).
    /// Mutation: parse as 0 → dead label.
    #[test]
    fn ls_non_numeric_stream_num_skipped() {
        let labels = parse_language_streams_text("id,audio_production,N/A,eng\n");
        assert!(labels.is_empty());
    }

    /// Spec: an unrecognized type token is skipped.
    /// Mutation: emit Unknown stream label → wrong type label appears.
    #[test]
    fn ls_unknown_type_skipped() {
        let labels = parse_language_streams_text("id,audio_bonus_extended,1,eng\n");
        assert!(labels.is_empty());
    }

    // See docs/ctrm.md — immunity pin: stream number comes from field
    // 3, so a dropped row never shifts later rows (unlike a
    // positional counter). Mutation: number via a running counter.
    #[test]
    fn ls_stream_numbers_come_from_the_row_not_a_counter() {
        let labels = parse_language_streams_text(
            "id,audio_production,4,eng\n\
             id,audio_bonus_extended,5,eng\n\
             id,audio_production,0,fra\n\
             id,audio_production,7,fra\n\
             id,subtitle_production\n\
             id,subtitle_narrative,9,deu\n",
        );
        let nums: Vec<(StreamLabelType, u16)> = labels
            .iter()
            .map(|l| (l.stream_type, l.stream_number))
            .collect();
        assert_eq!(
            nums,
            vec![
                (StreamLabelType::Audio, 4),
                (StreamLabelType::Audio, 7),
                (StreamLabelType::Subtitle, 9),
            ],
            "an unusable row drops out without shifting the numbering"
        );
        assert_eq!(labels[2].qualifier, LabelQualifier::Forced);
    }

    // Immunity pin: menu_base numbers come from the entry's own
    // streamNumber, so skipped entries don't renumber survivors.
    // Mutation: number by iteration order → survivors collapse to 1/2.
    #[test]
    fn menu_base_stream_numbers_come_from_the_entry_not_a_counter() {
        let labels = parse_props(
            "#audio_0.class=AudioButton\n\
             #audio_0.streamNumber=1\n\
             audio_1.class=AudioButton\n\
             audio_1.streamNumber=0\n\
             audio_2.class=AudioButton\n\
             audio_2.streamNumber=6\n\
             other_1.class=SomeOtherButton\n\
             other_1.streamNumber=2\n\
             subtitle_1.class=SubtitleButton\n\
             subtitle_1.streamNumber=11\n",
        );
        let nums: Vec<(StreamLabelType, u16)> = labels
            .iter()
            .map(|l| (l.stream_type, l.stream_number))
            .collect();
        assert_eq!(
            nums,
            vec![(StreamLabelType::Audio, 6), (StreamLabelType::Subtitle, 11),],
            "skipped entries must not renumber the ones that survive"
        );
    }

    /// Spec: `eda` variant → `Descriptive` purpose.
    /// Mutation: miss the `eda` branch → purpose stays Normal.
    #[test]
    fn ls_eda_variant_sets_descriptive() {
        let labels = parse_language_streams_text("id,audio_production,2,eng,eda\n");
        assert_eq!(labels[0].purpose, LabelPurpose::Descriptive);
    }

    /// Spec: dialect variant codes (`bp`, `csp`, etc.) pass through as variant_code.
    /// Mutation: store as codec_hint → variant field empty on BP stream.
    #[test]
    fn ls_bp_variant_is_dialect_code() {
        let labels = parse_language_streams_text("id,audio_production,1,por,bp\n");
        assert_eq!(labels[0].variant, "bp");
        assert_eq!(labels[0].codec_hint, "");
    }

    /// Spec: codec token from the 5th column → codec_hint via vocab::codec.
    /// Mutation: skip vocab lookup → raw token stored instead of canonical name.
    #[test]
    fn ls_codec_token_passed_to_vocab() {
        let labels = parse_language_streams_text("id,audio_production,1,eng,MLP\n");
        // "MLP" maps to "TrueHD" via vocab::codec.
        assert_eq!(labels[0].codec_hint, "TrueHD");
    }

    /// Spec: lines with fewer than 4 CSV fields are silently skipped.
    /// Mutation: parse short lines anyway → panic or garbage label emitted.
    #[test]
    fn ls_too_few_fields_skipped() {
        let labels = parse_language_streams_text("id,audio_production,1\n");
        assert!(labels.is_empty());
    }

    /// Spec: comment lines (starting with #) are skipped.
    /// Mutation: remove `starts_with('#')` guard → comment parsed as stream.
    #[test]
    fn ls_comment_lines_skipped() {
        let labels =
            parse_language_streams_text("# this is a comment\nid,audio_production,1,eng\n");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].language, "eng");
    }

    // Spec: skip test is is_empty()||starts_with('#') — either alone
    // skips. A commented-out CSV-shaped line must never produce a
    // label. Mutation `||`->`&&` lets it fall through to the CSV parser.
    #[test]
    fn ls_comment_line_with_csv_shape_is_still_skipped() {
        let labels =
            parse_language_streams_text("#id,audio_production,1,eng\nid2,audio_production,2,fra\n");
        assert_eq!(
            labels.len(),
            1,
            "the commented-out CSV-shaped line must not parse"
        );
        assert_eq!(labels[0].language, "fra");
    }

    /// Spec: `subtitle_dual` is a recognized subtitle type (Normal/no
    /// qualifier). Mutation: delete this match arm → falls to the
    /// catch-all `_ => continue`, silently dropping the stream.
    #[test]
    fn ls_subtitle_dual_parsed() {
        let labels = parse_language_streams_text("id,subtitle_dual,1,eng\n");
        assert_eq!(labels.len(), 1, "subtitle_dual must produce a label");
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(labels[0].purpose, LabelPurpose::Normal);
        assert_eq!(labels[0].qualifier, LabelQualifier::None);
    }

    /// Spec: `subtitle_bonus` is a recognized subtitle type (Normal/no
    /// qualifier). Mutation: delete this match arm → dropped as unknown.
    #[test]
    fn ls_subtitle_bonus_parsed() {
        let labels = parse_language_streams_text("id,subtitle_bonus,2,eng\n");
        assert_eq!(labels.len(), 1, "subtitle_bonus must produce a label");
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(labels[0].purpose, LabelPurpose::Normal);
    }

    /// Spec: `subtitle_ime` maps to Subtitle/Ime (no Forced qualifier,
    /// unlike `subtitle_ime_narrative`).
    /// Mutation: delete this match arm → dropped as unknown.
    #[test]
    fn ls_subtitle_ime_parsed() {
        let labels = parse_language_streams_text("id,subtitle_ime,3,jpn\n");
        assert_eq!(labels.len(), 1, "subtitle_ime must produce a label");
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(labels[0].purpose, LabelPurpose::Ime);
        assert_eq!(labels[0].qualifier, LabelQualifier::None);
    }

    /// Spec: multiple valid lines produce multiple labels.
    /// Mutation: stop after first label → only 1 label returned.
    #[test]
    fn ls_multiple_lines_produce_multiple_labels() {
        let text = "id1,audio_production,1,eng\nid2,audio_commentary,2,eng\nid3,subtitle_production,1,eng\n";
        let labels = parse_language_streams_text(text);
        assert_eq!(labels.len(), 3);
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .collect();
        assert_eq!(audio.len(), 2);
        assert_eq!(subs.len(), 1);
    }

    // Spec: rejects "community_" (pre-fix bug: bare contains("comm")
    // substring-matched it). Now only whole-segment comm/commentary
    // match. Mutation: contains("comm") → community_1 wrongly matches.
    #[test]
    fn prefix_is_commentary_rejects_community_prefix() {
        assert!(!prefix_is_commentary("community_1"));
        assert!(!prefix_is_commentary("community"));
        assert!(!prefix_is_commentary("recommit_1"));
    }

    /// Spec: prefix_is_commentary matches "comm" as a standalone segment.
    /// Mutation: require "commentary" specifically → bare "comm" prefix fails.
    #[test]
    fn prefix_is_commentary_matches_bare_comm_segment() {
        assert!(prefix_is_commentary("comm"));
        assert!(prefix_is_commentary("audio_comm"));
        assert!(prefix_is_commentary("comm_track_1"));
    }
}

// ── menu_base.prop parser ──────────────────────────────────────────────────

fn parse_menu_base(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "menu_base.prop")?;
    let text = std::str::from_utf8(&data).ok()?;
    let labels = parse_menu_base_text(text);
    if labels.is_empty() {
        return None;
    }
    Some(labels)
}

// Parses menu_base.prop body into labels. Split from parse_menu_base
// (I/O + UTF-8 decode only) so tests exercise real parsing logic.
// Returns labels sorted by (type, number).
fn parse_menu_base_text(text: &str) -> Vec<StreamLabel> {
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

        // Resolve the stream type FIRST: when an entry trips both flags
        // (e.g. an `audio_` prefix with a class containing
        // "SubtitleButton"), audio wins the type.
        let stream_type = if is_audio {
            StreamLabelType::Audio
        } else {
            StreamLabelType::Subtitle
        };

        let name = props.get("name").cloned().unwrap_or_default();

        // Ask vocab first (word-boundary matched, avoiding the "Commenter"
        // false positive of the old `name.contains("comment")`), then fall
        // back to the structural prefix check (`audio_commentary.foo`-style).
        let purpose = match vocab::purpose(&name) {
            LabelPurpose::Normal if prefix_is_commentary(prefix) => LabelPurpose::Commentary,
            p => p,
        };

        // Qualifier (SDH/Forced) is a subtitle-only concept. Gate on the
        // RESOLVED type, not the raw is_subtitle flag, so an entry that
        // resolved to Audio never carries a subtitle qualifier.
        let qualifier = if stream_type == StreamLabelType::Subtitle {
            vocab::qualifier(&name)
        } else {
            LabelQualifier::None
        };

        // Try to extract language from audioLanguage/subtitleLanguage prop
        let language = props
            .get("audioLanguage")
            .or_else(|| props.get("subtitleLanguage"))
            .cloned()
            .unwrap_or_default();

        labels.push(StreamLabel {
            stream_id: None,
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
