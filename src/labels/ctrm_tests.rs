use super::*;

/// Run the real shipping parser ([`parse_menu_base_text`]) on a
/// menu_base.prop body so tests exercise production code directly.
fn parse_props(text: &str) -> Vec<StreamLabel> {
    parse_menu_base_text(text)
}

#[test]
fn menu_base_tie_order_is_deterministic_by_prefix() {
    // Inserted in reverse; many prefixes so hash order can't match by luck.
    let mut text = String::new();
    for i in (0..64).rev() {
        text.push_str(&format!(
            "p{i:02}.class=AudioButton\np{i:02}.streamNumber=2\np{i:02}.name=N{i:02}\n"
        ));
    }
    let names: Vec<String> = parse_props(&text).into_iter().map(|l| l.name).collect();
    let want: Vec<String> = (0..64).map(|i| format!("N{i:02}")).collect();
    assert_eq!(names, want);
}

#[test]
fn menu_base_spaces_around_separator_are_accepted() {
    let labels = parse_props("audio_1.streamNumber = 3\naudio_1.name = Foo\n");
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].stream_number, 3);
    assert_eq!(labels[0].name, "Foo");
}

#[test]
fn parsers_cap_retained_entries() {
    let mut mb = String::new();
    let mut ls = String::new();
    for i in 0..(MAX_CTRM_LABELS + 100) {
        mb.push_str(&format!("audio_{i}.streamNumber=1\n"));
        ls.push_str("x,audio_production,1,eng\n");
    }
    assert!(parse_menu_base_text(&mb).len() <= MAX_CTRM_LABELS);
    assert_eq!(parse_language_streams_text(&ls).len(), MAX_CTRM_LABELS);
}

#[test]
fn menu_base_sorts_by_type_then_number_across_prefixes() {
    let labels = parse_props(
        "a_sub.class=SubtitleButton\na_sub.streamNumber=1\n\
             b_aud.class=AudioButton\nb_aud.streamNumber=1\n\
             c_aud.class=AudioButton\nc_aud.streamNumber=2\n\
             d_aud.class=AudioButton\nd_aud.streamNumber=1\nd_aud.name=Low\n",
    );
    let key: Vec<(StreamLabelType, u16)> = labels
        .iter()
        .map(|l| (l.stream_type, l.stream_number))
        .collect();
    assert_eq!(
        key,
        vec![
            (StreamLabelType::Audio, 1),
            (StreamLabelType::Audio, 1),
            (StreamLabelType::Audio, 2),
            (StreamLabelType::Subtitle, 1),
        ]
    );
}

#[test]
fn menu_base_caps_properties_per_prefix() {
    let mut text = String::from("audio_1.streamNumber=1\n");
    for i in 0..(MAX_PROPS_PER_PREFIX + 50) {
        text.push_str(&format!("audio_1.junk{i}=x\n"));
    }
    // Past the cap: a new key is dropped, an already-retained key is still updated.
    text.push_str("audio_1.late=x\naudio_1.name=Late\naudio_1.streamNumber=2\n");
    let labels = parse_props(&text);
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].stream_number, 2);
    assert_eq!(labels[0].name, "");
}

#[test]
fn menu_base_accepts_alternate_stream_and_language_keys() {
    let labels = parse_props(
        "audio_1.audioStream=4\n\
             subtitle_1.subtitleStream=5\nsubtitle_1.subtitleLanguage=fra\n",
    );
    assert_eq!(labels.len(), 2);
    assert_eq!(labels[0].stream_number, 4);
    assert_eq!(labels[1].stream_number, 5);
    assert_eq!(labels[1].language, "fra");
}

#[test]
fn merge_fills_names_and_appends_only_missing() {
    let mk = |n: u16, name: &str| StreamLabel {
        stream_id: None,
        stream_number: n,
        stream_type: StreamLabelType::Audio,
        language: String::new(),
        name: name.into(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: String::new(),
        variant: String::new(),
    };
    let out = merge(vec![mk(1, "")], vec![mk(1, "A"), mk(1, "B"), mk(2, "C")]);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].name, "A");
    assert_eq!(out[1].name, "C");
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
    let labels = parse_language_streams_text("# this is a comment\nid,audio_production,1,eng\n");
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
    let text =
        "id1,audio_production,1,eng\nid2,audio_commentary,2,eng\nid3,subtitle_production,1,eng\n";
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
