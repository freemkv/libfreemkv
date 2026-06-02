//! Pixelogic — `bluray_project.bin`
//!
//! Binary file with embedded UTF-8 token strings in STN order per
//! playlist section. Most common format (5/10 test discs).
//!
//! Token format: `{lang}_{codec?}_{purpose?}_{region?}_`

use super::{
    Confidence, LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, text,
    vocab,
};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::sync::atomic::{AtomicBool, Ordering};

/// Known audio codec tokens
const AUDIO_CODECS: &[&str] = &["MLP", "AC3", "DTS", "DDL", "WAV", "AC"];
/// Known region tokens
const REGIONS: &[&str] = &[
    "US", "UK", "CF", "PF", "CS", "LS", "BP", "PP", "SM", "TM", "CAN", "DUM", "FLE",
];

pub fn detect(udf: &UdfFs) -> bool {
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
    // but the corpus surfaced something we don't catalogue.
    let saw_unknown = AtomicBool::new(false);

    let labels = assign_labels(&strings, &saw_unknown);

    if labels.is_empty() {
        return None;
    }
    let confidence = if saw_unknown.load(Ordering::Relaxed) {
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
fn assign_labels(strings: &[String], saw_unknown: &AtomicBool) -> Vec<StreamLabel> {
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
        if s.starts_with("Audio Stream") {
            audio_num += 1;
            continue;
        }

        if let Some(label) = parse_token_inner(s, Some(saw_unknown)) {
            match label.stream_type {
                StreamLabelType::Audio => {
                    audio_num += 1;
                    labels.push(StreamLabel {
                        stream_number: audio_num,
                        ..label
                    });
                }
                StreamLabelType::Subtitle => {
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

fn parse_token_inner(s: &str, saw_unknown: Option<&AtomicBool>) -> Option<StreamLabel> {
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

    for &part in &parts[1..] {
        if part.is_empty() {
            continue;
        }
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
            qualifier = LabelQualifier::Forced;
        } else if REGIONS.contains(&part) {
            variant = part.to_string();
        } else if part.starts_with("PGStream") {
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
            if let Some(flag) = saw_unknown {
                flag.store(true, Ordering::Relaxed);
            }
        }
    }

    if !is_audio && !is_subtitle {
        return None;
    }

    let stream_type = if is_subtitle {
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
        let flag = AtomicBool::new(false);
        let tokens = strs(&[
            "FPL_MainFeature",
            "Audio Stream 1",
            "Audio Stream 2",
            "Audio Stream 3",
            "eng_ACOM_",
        ]);
        let labels = assign_labels(&tokens, &flag);
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
        let flag = AtomicBool::new(false);
        let tokens = strs(&[
            "SEG_MainFeature",
            "eng_ACOM_", // stray token in the menu segment — must be ignored
            "FPL_MainFeature",
            "Audio Stream 1",
            "eng_ACOM_",
        ]);
        let labels = assign_labels(&tokens, &flag);
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
        let flag = AtomicBool::new(false);
        let tokens = strs(&["SEG_MainFeature", "eng_MLP_", "spa_AC3_"]);
        let labels = assign_labels(&tokens, &flag);
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
}
