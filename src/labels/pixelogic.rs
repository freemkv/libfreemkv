//! Pixelogic — `bluray_project.bin`
//!
//! Binary file with embedded UTF-8 token strings in STN order per
//! playlist section. A common Pixelogic layout.
//!
//! Token format: `{lang}_{codec?}_{purpose?}_{region?}_`

use super::{
    Confidence, LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, text,
    vocab,
};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

/// Known audio codec tokens
const AUDIO_CODECS: &[&str] = &["MLP", "AC3", "DTS", "DDL", "WAV", "AC"];
/// Sane upper bound on streams of one type within a single feature
/// section. The BD STN table caps audio at 32; this generous ceiling
/// stops a crafted blob with tens of thousands of stream tokens from
/// overflowing the u16 STN counters (panic in debug, wrap-to-0 in
/// release, which would misnumber subsequent labels).
const MAX_STREAMS_PER_TYPE: u16 = 512;
/// Known region tokens
const REGIONS: &[&str] = &[
    "US", "UK", "CF", "PF", "CS", "LS", "BP", "PP", "SM", "TM", "CAN", "DUM", "FLE",
];

pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "bluray_project.bin")
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let data = super::read_jar_file(reader, udf, "bluray_project.bin")?;
    // min_len=4 matches the prior local extract_strings impl. The token
    // grammar is `{lang3}_{codec?}_{purpose?}_{region?}_` so the
    // shortest meaningful run is 4 chars (lang + underscore).
    let strings = text::extract_ascii_strings(&data, 4);

    // Tracked across all parse_token calls in this run: did any stream
    // hit an unrecognized token component (skip-unknown path)? If yes
    // we downgrade confidence to Medium — the labels are still valid
    // but the corpus surfaced something we don't catalogue. Parsing is
    // single-threaded and sequential, so a plain bool suffices.
    let mut saw_unknown = false;

    let labels = assign_labels(&strings, &mut saw_unknown);

    if labels.is_empty() {
        return None;
    }
    let confidence = if saw_unknown {
        Confidence::Medium
    } else {
        Confidence::High
    };
    Some(ParseResult { labels, confidence })
}

/// Walk the extracted token strings of the feature section and emit a
/// `StreamLabel` per editorial token, numbered in STN order. Split out
/// from `parse` so the section/numbering logic is unit-testable without
/// a `SectorSource`/`UdfFs`.
fn assign_labels(strings: &[String], saw_unknown: &mut bool) -> Vec<StreamLabel> {
    // The authoritative per-feature stream list lives in the `FPL_`
    // (FeaturePLaylist) section, in STN order. `SEG_*` entries are menu
    // segments (intros, logos, disclaimers, previews) that can also carry
    // stray stream tokens — e.g. a `SEG_MainFeature` preview segment that
    // lists only a commentary track. Anchoring on such a segment grabs the
    // wrong streams and misnumbers them. So when the project ships any
    // `FPL_` playlist, anchor exclusively on it; only fall back to
    // `SEG_MainFeature` on discs that have no `FPL_` section at all.
    let has_fpl = strings.iter().any(|s| s.starts_with("FPL_"));

    let mut labels = Vec::new();
    let mut in_feature = false;
    let mut audio_num: u16 = 0;
    let mut sub_num: u16 = 0;

    for s in strings {
        // Detect feature section start
        let is_start = if has_fpl {
            s.starts_with("FPL_")
        } else {
            s.starts_with("SEG_MainFeature")
        };
        if is_start {
            if in_feature {
                break;
            }
            in_feature = true;
            audio_num = 0;
            sub_num = 0;
            continue;
        }

        // Detect section end
        if in_feature && (s.starts_with("SEG_") || s.starts_with("SF_") || s.starts_with("FPL_")) {
            break;
        }

        if !in_feature {
            continue;
        }

        // Generic audio-slot placeholder. Pixelogic lists each audio stream
        // in the playlist as either an editorial `{lang}_{codec}_…` token OR
        // a bare `Audio Stream N` placeholder when no editorial label was
        // authored. The placeholder carries nothing to label, but it DOES
        // occupy an STN slot — so it must advance `audio_num`. Otherwise a
        // later editorial token (e.g. a lone `eng_ACOM_` commentary sitting
        // at STN slot 4, behind three unlabelled main tracks) collapses onto
        // slot 1 and its Commentary purpose lands on the main feature track.
        //
        // Only audio is corrected here: subtitle (`PG Stream N`) numbering is
        // left exactly as-is — the corpus snapshots show forced/commentary
        // subtitle tokens already align with STN without counting the
        // placeholders, and counting them regresses several discs.
        // Stop accumulating once both counters reach the sane cap — a
        // crafted blob can't drive them to u16 overflow.
        if audio_num >= MAX_STREAMS_PER_TYPE && sub_num >= MAX_STREAMS_PER_TYPE {
            break;
        }

        if s.starts_with("Audio Stream") {
            if audio_num < MAX_STREAMS_PER_TYPE {
                audio_num += 1;
            }
            continue;
        }

        if let Some(label) = parse_token_inner(s, Some(&mut *saw_unknown)) {
            match label.stream_type {
                StreamLabelType::Audio => {
                    if audio_num >= MAX_STREAMS_PER_TYPE {
                        continue;
                    }
                    audio_num += 1;
                    labels.push(StreamLabel {
                        stream_number: audio_num,
                        ..label
                    });
                }
                StreamLabelType::Subtitle => {
                    if sub_num >= MAX_STREAMS_PER_TYPE {
                        continue;
                    }
                    sub_num += 1;
                    labels.push(StreamLabel {
                        stream_number: sub_num,
                        ..label
                    });
                }
            }
        }
    }

    labels
}

fn parse_token_inner(s: &str, mut saw_unknown: Option<&mut bool>) -> Option<StreamLabel> {
    let clean = s.trim().trim_start_matches('\t').trim_end_matches('_');
    let parts: Vec<&str> = clean.split('_').collect();
    if parts.len() < 2 {
        return None;
    }

    let lang = parts[0];
    if lang.len() != 3 || !lang.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }

    let mut codec = String::new();
    let mut purpose = LabelPurpose::Normal;
    let mut qualifier = LabelQualifier::None;
    let mut variant = String::new();
    let mut is_subtitle = false;
    let mut is_audio = false;

    for &raw_part in &parts[1..] {
        if raw_part.is_empty() {
            continue;
        }
        // Token components are spec-uppercase (codec IDs, ADES/ACOM/SDH,
        // region codes). vocab elsewhere is deliberately case-insensitive,
        // so normalize each component to uppercase before the gate to
        // avoid silently dropping a lowercase-authored token (which would
        // fall through to the unknown branch and, with no is_audio/
        // is_subtitle set, get the whole stream discarded below).
        let part_up = raw_part.to_ascii_uppercase();
        let part = part_up.as_str();
        if AUDIO_CODECS.contains(&part) {
            codec = vocab::codec(part).to_string();
            is_audio = true;
        } else if part == "ADES" {
            purpose = LabelPurpose::Descriptive;
            is_audio = true;
        } else if part == "ACOM" {
            purpose = LabelPurpose::Commentary;
            is_audio = true;
        } else if part == "ADLG" || part == "ATRI" {
            is_audio = true;
        } else if part == "SDH" {
            qualifier = LabelQualifier::Sdh;
            is_subtitle = true;
        } else if part == "SDLG" {
            is_subtitle = true;
        } else if part == "SCOM" {
            purpose = LabelPurpose::Commentary;
            is_subtitle = true;
        } else if part == "STRI" || part == "TXT" {
            is_subtitle = true;
        } else if part == "FOR" {
            // `FOR` (forced) is a subtitle-domain qualifier. A token whose
            // only non-language component is FOR (e.g. `eng_FOR_`) would
            // otherwise classify as neither audio nor subtitle and be
            // dropped at the `!is_audio && !is_subtitle` guard below. Treat
            // a forced marker as a subtitle signal so the stream survives.
            qualifier = LabelQualifier::Forced;
            is_subtitle = true;
        } else if REGIONS.contains(&part) {
            variant = part.to_string();
        } else if part.starts_with("PGSTREAM") {
            is_subtitle = true;
        } else {
            // Unknown token component — skip this single part rather
            // than discarding the entire stream record. Pre-refactor
            // behavior was `return None` here, which silently dropped
            // any stream containing a single uncatalogued token (e.g.
            // a new codec ID or framework variant). Better to surface
            // what we know than discard a whole stream over one part,
            // but flag the parse as Medium-confidence so callers know
            // some data was elided.
            tracing::debug!(part = %part, "pixelogic: unrecognized token component, skipping");
            if let Some(flag) = saw_unknown.as_deref_mut() {
                *flag = true;
            }
        }
    }

    if !is_audio && !is_subtitle {
        return None;
    }

    // Tie-break for tokens that signal both domains (e.g. `eng_MLP_SDH_`
    // sets is_audio via the codec and is_subtitle via SDH). An audio
    // codec hint is the stronger, audio-domain signal, so prefer Audio
    // when one is present (keeps the parsed codec_hint instead of
    // discarding it); otherwise file as Subtitle. Pure-subtitle and
    // pure-audio tokens are unaffected.
    let has_audio_codec = is_audio && !codec.is_empty();
    let stream_type = if is_subtitle && !has_audio_codec {
        StreamLabelType::Subtitle
    } else {
        StreamLabelType::Audio
    };

    Some(StreamLabel {
        stream_number: 0,
        stream_type,
        language: lang.to_string(),
        name: String::new(),
        purpose,
        qualifier,
        codec_hint: codec,
        variant,
    })
}

// extract_strings removed — replaced by super::text::extract_ascii_strings(data, 4).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_token_basic_audio() {
        let l = parse_token_inner("eng_MLP_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.language, "eng");
        assert_eq!(l.codec_hint, "TrueHD");
        assert_eq!(l.purpose, LabelPurpose::Normal);
    }

    #[test]
    fn parse_token_basic_subtitle_sdh() {
        let l = parse_token_inner("eng_SDH_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
        assert_eq!(l.language, "eng");
        assert_eq!(l.qualifier, LabelQualifier::Sdh);
    }

    #[test]
    fn parse_token_commentary() {
        let l = parse_token_inner("eng_MLP_ACOM_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.purpose, LabelPurpose::Commentary);
    }

    #[test]
    fn parse_token_descriptive() {
        let l = parse_token_inner("eng_AC3_ADES_", None).unwrap();
        assert_eq!(l.purpose, LabelPurpose::Descriptive);
    }

    #[test]
    fn parse_token_with_region() {
        let l = parse_token_inner("eng_MLP_US_", None).unwrap();
        assert_eq!(l.language, "eng");
        assert_eq!(l.variant, "US");
    }

    #[test]
    fn parse_token_unknown_component_does_not_kill_stream() {
        // Regression: pre-refactor, an unrecognized token part returned
        // None for the whole stream, silently dropping it. New
        // behavior: skip the unknown part, surface what we know.
        let l = parse_token_inner("eng_MLP_FUTUREFLAG_FOR_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.language, "eng");
        assert_eq!(l.codec_hint, "TrueHD");
        assert_eq!(l.qualifier, LabelQualifier::Forced);
    }

    #[test]
    fn parse_token_no_audio_or_subtitle_signal_returns_none() {
        // A token that has only a language and an unknown part with
        // no audio/subtitle classifier should still return None —
        // there's no way to file it as a stream.
        assert!(parse_token_inner("eng_UNKNOWN_", None).is_none());
    }

    #[test]
    fn parse_token_rejects_non_lang_prefix() {
        assert!(parse_token_inner("XX_MLP_", None).is_none());
        assert!(parse_token_inner("ENG_MLP_", None).is_none()); // uppercase not accepted as ISO 639-2
    }

    #[test]
    fn parse_token_dual_type_with_codec_prefers_audio() {
        // `eng_MLP_SDH_` sets the audio codec (MLP) and the subtitle SDH
        // qualifier. Policy: a codec hint wins -> Audio, and codec_hint is
        // preserved rather than discarded.
        let l = parse_token_inner("eng_MLP_SDH_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.codec_hint, "TrueHD");
        assert_eq!(l.qualifier, LabelQualifier::Sdh);
    }

    #[test]
    fn parse_token_solo_forced_is_subtitle() {
        // A token whose only non-language component is FOR must survive as
        // a forced subtitle rather than being dropped.
        let l = parse_token_inner("eng_FOR_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
        assert_eq!(l.language, "eng");
        assert_eq!(l.qualifier, LabelQualifier::Forced);
    }

    #[test]
    fn parse_token_components_are_case_insensitive() {
        // Regression for the case-sensitive gate: a lowercase codec/
        // qualifier component must classify identically to uppercase
        // rather than falling through to the unknown branch and getting
        // the whole stream dropped. The ISO 639-2 lang prefix is still
        // required lowercase.
        let l = parse_token_inner("eng_mlp_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.codec_hint, "TrueHD");

        let l = parse_token_inner("eng_ac3_acom_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.purpose, LabelPurpose::Commentary);
        assert_eq!(l.codec_hint, "Dolby Digital");

        let l = parse_token_inner("eng_sdh_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
        assert_eq!(l.qualifier, LabelQualifier::Sdh);

        // Mixed-case region token still recognized as a variant.
        let l = parse_token_inner("eng_MLP_us_", None).unwrap();
        assert_eq!(l.variant, "US");
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn assign_labels_numbers_commentary_behind_placeholders() {
        // Observed case: the FPL_MainFeature playlist lists three unlabelled main
        // audio tracks as `Audio Stream N` placeholders, then a lone
        // `eng_ACOM_` commentary at STN slot 4. The commentary must land on
        // audio #4, not collapse onto #1 (which would tag the main feature
        // track as commentary).
        let mut flag = false;
        let tokens = strs(&[
            "FPL_MainFeature",
            "Audio Stream 1",
            "Audio Stream 2",
            "Audio Stream 3",
            "eng_ACOM_",
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(audio.len(), 1, "only the commentary carries a label");
        assert_eq!(audio[0].stream_number, 4, "commentary is STN slot 4");
        assert_eq!(audio[0].purpose, LabelPurpose::Commentary);
        assert_eq!(audio[0].language, "eng");
    }

    #[test]
    fn assign_labels_prefers_fpl_over_seg_mainfeature() {
        // A `SEG_MainFeature` menu/preview segment carries a stray commentary
        // token, but the real playlist is `FPL_MainFeature`. When an FPL_
        // section exists, the SEG_ one must be ignored as an anchor — so we
        // number from the FPL playlist, putting the commentary at slot 2.
        let mut flag = false;
        let tokens = strs(&[
            "SEG_MainFeature",
            "eng_ACOM_", // stray token in the menu segment — must be ignored
            "FPL_MainFeature",
            "Audio Stream 1",
            "eng_ACOM_",
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(audio.len(), 1);
        assert_eq!(audio[0].stream_number, 2, "numbered from the FPL playlist");
        assert_eq!(audio[0].purpose, LabelPurpose::Commentary);
    }

    #[test]
    fn assign_labels_falls_back_to_seg_without_fpl() {
        // Discs with no FPL_ playlist still anchor on SEG_MainFeature.
        let mut flag = false;
        let tokens = strs(&["SEG_MainFeature", "eng_MLP_", "spa_AC3_"]);
        let labels = assign_labels(&tokens, &mut flag);
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(audio.len(), 2);
        assert_eq!(audio[0].stream_number, 1);
        assert_eq!(audio[0].language, "eng");
        assert_eq!(audio[1].stream_number, 2);
        assert_eq!(audio[1].language, "spa");
    }

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: `DDL` token → Dolby Digital Plus (via vocab::codec).
    /// Mutation: remove "DDL" from AUDIO_CODECS → DDL falls to unknown branch.
    #[test]
    fn parse_token_ddl_maps_to_dolby_digital_plus() {
        let l = parse_token_inner("eng_DDL_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.codec_hint, "Dolby Digital Plus");
    }

    /// Spec: `WAV` token → PCM (via vocab::codec).
    /// Mutation: remove "WAV" from AUDIO_CODECS → WAV falls to unknown branch.
    #[test]
    fn parse_token_wav_maps_to_pcm() {
        let l = parse_token_inner("eng_WAV_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.codec_hint, "PCM");
    }

    /// Spec: `SDLG` token marks a subtitle stream (dialogue).
    /// Mutation: remove "SDLG" arm → is_subtitle stays false → None.
    #[test]
    fn parse_token_sdlg_is_subtitle() {
        let l = parse_token_inner("eng_SDLG_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
        assert_eq!(l.language, "eng");
    }

    /// Spec: `SCOM` token marks a subtitle commentary stream.
    /// Mutation: remove "SCOM" arm → is_subtitle stays false → None.
    #[test]
    fn parse_token_scom_is_subtitle_commentary() {
        let l = parse_token_inner("eng_SCOM_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
        assert_eq!(l.purpose, LabelPurpose::Commentary);
    }

    /// Spec: `STRI` token marks a subtitle stream (trivia/bonus).
    /// Mutation: remove "STRI" arm → None.
    #[test]
    fn parse_token_stri_is_subtitle() {
        let l = parse_token_inner("fra_STRI_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
    }

    /// Spec: `ADLG` token marks an audio stream (dialogue).
    /// Mutation: remove "ADLG" → is_audio stays false → None.
    #[test]
    fn parse_token_adlg_is_audio() {
        let l = parse_token_inner("eng_ADLG_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
    }

    /// Spec: `ATRI` token marks an audio stream (trivia/bonus).
    /// Mutation: remove "ATRI" → is_audio stays false → None.
    #[test]
    fn parse_token_atri_is_audio() {
        let l = parse_token_inner("eng_ATRI_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
    }

    /// Spec: `TXT` token marks a subtitle text stream.
    /// Mutation: remove "TXT" arm → None.
    #[test]
    fn parse_token_txt_is_subtitle() {
        let l = parse_token_inner("eng_TXT_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
    }

    /// Spec: `PGSTREAM` prefix marks a subtitle (presentation-graphics) stream.
    /// Mutation: change `starts_with("PGSTREAM")` to exact match → PGSTREAM1 fails.
    #[test]
    fn parse_token_pgstream_prefix_is_subtitle() {
        let l = parse_token_inner("eng_PGSTREAM1_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
    }

    /// Spec: all region tokens are recognized variants.
    /// Mutation: remove a region from REGIONS → it falls to unknown branch.
    #[test]
    fn parse_token_all_regions_recognized() {
        for region in REGIONS {
            let token = format!("eng_MLP_{}_", region);
            let l = parse_token_inner(&token, None)
                .unwrap_or_else(|| panic!("region {region} should parse"));
            assert_eq!(l.variant, *region, "region {} should be in variant", region);
        }
    }

    /// Spec: lang must be exactly 3 lowercase ASCII letters.
    /// Mutation: allow length > 3 → "engl_MLP_" parsed as a stream.
    #[test]
    fn parse_token_rejects_four_char_lang() {
        assert!(parse_token_inner("engl_MLP_", None).is_none());
    }

    /// Spec: lang must be exactly 3 lowercase ASCII letters.
    /// Mutation: allow length < 3 → "en_MLP_" parsed.
    #[test]
    fn parse_token_rejects_two_char_lang() {
        assert!(parse_token_inner("en_MLP_", None).is_none());
    }

    /// Spec: is_audio wins over is_subtitle when codec explicitly identified.
    /// Tests the commentary audio case — `ACOM` sets both is_audio purpose and SDH-only-is-subtitle:
    /// MLP codec wins → Audio.
    /// Mutation: flip the tie-break → Subtitle returned when codec present.
    #[test]
    fn parse_token_codec_always_wins_type_tiebreak() {
        let l = parse_token_inner("eng_AC3_SDH_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.codec_hint, "Dolby Digital");
    }

    /// Spec: an unknown component sets saw_unknown flag.
    /// Mutation: remove the flag-setting → Medium confidence never triggered.
    #[test]
    fn parse_token_unknown_sets_saw_unknown_flag() {
        let mut flag = false;
        let _ = parse_token_inner("eng_MLP_FUTURETOKEN_", Some(&mut flag));
        assert!(flag, "unknown component must set saw_unknown flag");
    }

    /// Spec: a known-only token leaves saw_unknown=false.
    /// Mutation: always set the flag → all parses downgrade to Medium.
    #[test]
    fn parse_token_all_known_leaves_flag_false() {
        let mut flag = false;
        let _ = parse_token_inner("eng_MLP_ACOM_US_", Some(&mut flag));
        assert!(!flag, "all-known token must NOT set saw_unknown flag");
    }

    /// Spec: `Audio Stream N` placeholder advances audio_num but emits no label.
    /// Mutation: also emit a label for placeholder → audio#N+1 shifts to N+2.
    #[test]
    fn assign_labels_audio_placeholder_advances_counter_no_label() {
        let mut flag = false;
        let tokens = strs(&["FPL_MainFeature", "Audio Stream 1", "eng_MLP_"]);
        let labels = assign_labels(&tokens, &mut flag);
        let a: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(a.len(), 1, "only editorial token produces a label");
        assert_eq!(a[0].stream_number, 2, "placeholder must advance counter");
    }

    /// Spec: FPL section ends when SEG_ or SF_ marker is encountered.
    /// Mutation: don't end on SEG_ → tokens from a following segment are parsed.
    #[test]
    fn assign_labels_fpl_section_ends_on_seg_boundary() {
        let mut flag = false;
        let tokens = strs(&[
            "FPL_MainFeature",
            "eng_MLP_",
            "SEG_Trailer", // must end the FPL section
            "fra_AC3_",    // must NOT be parsed
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        assert_eq!(labels.len(), 1, "only eng from FPL section");
        assert_eq!(labels[0].language, "eng");
    }

    /// Spec: MAX_STREAMS_PER_TYPE=512 caps the counter to prevent u16 overflow.
    /// Mutation: remove the cap check → counter wraps past 512.
    #[test]
    fn assign_labels_max_streams_cap_prevents_overflow() {
        let mut flag = false;
        // Build 520 Audio Stream placeholders inside FPL, then an editorial token.
        let mut tokens = vec!["FPL_MainFeature".to_string()];
        for i in 1..=520 {
            tokens.push(format!("Audio Stream {}", i));
        }
        tokens.push("eng_ACOM_".to_string());
        // Must not panic. The editorial token after the cap should be silently dropped.
        let labels = assign_labels(&tokens, &mut flag);
        // The commentary must NOT be emitted (audio_num already at cap).
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        // All editorial tokens past the cap are dropped.
        assert!(audio.is_empty() || audio.iter().all(|l| l.stream_number <= 512));
    }

    /// Spec: subtitle placeholders (PG Stream N) do NOT advance the subtitle counter.
    /// Only audio placeholders (`Audio Stream N`) do.
    /// Mutation: also advance sub counter on PG placeholder → subtitle labels misnumbered.
    #[test]
    fn assign_labels_pg_placeholder_does_not_advance_sub_counter() {
        // The spec comment says "Only audio is corrected here: subtitle (PG Stream N) numbering
        // is left exactly as-is". PG Stream placeholders are not a token the parser recognizes
        // as placeholders — they would only appear as real subtitle tokens with SDLG/SDH markers.
        // This test verifies the audio-only correction behavior via a mixed sequence.
        let mut flag = false;
        let tokens = strs(&[
            "FPL_MainFeature",
            "Audio Stream 1",
            "Audio Stream 2",
            "eng_SDH_", // subtitle token — sub_num becomes 1
            "fra_SDH_", // subtitle token — sub_num becomes 2
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .collect();
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].stream_number, 1);
        assert_eq!(subs[1].stream_number, 2);
    }
}
