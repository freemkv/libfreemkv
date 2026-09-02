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
// Ceiling on streams of one type per feature section: stops a crafted blob
// with tens of thousands of tokens from overflowing the u16 STN counters.
// See docs/pixelogic.md — MAX_STREAMS_PER_TYPE.
const MAX_STREAMS_PER_TYPE: u16 = 512;
// Ceiling on distinct video-slot entries one section may list before the
// walk gives up on it (used to find the next section's start).
// See docs/pixelogic.md — MAX_VIDEO_SLOTS.
const MAX_VIDEO_SLOTS: usize = 33;
/// Known region tokens
const REGIONS: &[&str] = &[
    "US", "UK", "CF", "PF", "CS", "LS", "BP", "PP", "SM", "TM", "CAN", "DUM", "FLE",
];
// Cap on distinct uncatalogued token components retained for the
// end-of-parse report; past the cap they're still counted, not named.
// See docs/pixelogic.md — MAX_REPORTED_UNKNOWN.
const MAX_REPORTED_UNKNOWN: usize = 16;
/// Longest retained form of a single uncatalogued component. Components come
/// from disc bytes and can be arbitrarily long; truncation is by CHARS, not
/// bytes, so a multi-byte sequence can never be split (which would panic).
const MAX_UNKNOWN_LEN: usize = 32;

// Collects the uncatalogued token components one parse ran into, reported
// ONCE at the end rather than silently or per-occurrence.
// See docs/pixelogic.md — UnknownParts.
#[derive(Debug, Default)]
struct UnknownParts {
    /// Distinct components, deduplicated and ordered for a stable log line.
    /// Bounded by [`MAX_REPORTED_UNKNOWN`].
    seen: std::collections::BTreeSet<String>,
    /// Total occurrences, including ones past the retention cap.
    total: usize,
}

impl UnknownParts {
    fn record(&mut self, part: &str) {
        self.total = self.total.saturating_add(1);
        if self.seen.len() >= MAX_REPORTED_UNKNOWN {
            return;
        }
        // Char-wise truncation: `part` is uppercased disc text, not
        // guaranteed ASCII, and slicing by byte offset could split a
        // multi-byte char and panic.
        self.seen
            .insert(part.chars().take(MAX_UNKNOWN_LEN).collect());
    }

    fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Emit the single end-of-parse report, if this parse hit anything.
    fn report(&self) {
        if self.is_empty() {
            return;
        }
        let names: Vec<&str> = self.seen.iter().map(String::as_str).collect();
        tracing::warn!(
            components = ?names,
            distinct = self.seen.len(),
            occurrences = self.total,
            truncated = self.seen.len() >= MAX_REPORTED_UNKNOWN,
            "pixelogic: uncatalogued token components in this disc's label blob; \
             any editorial meaning they carry (forced / SDH / commentary / dub) \
             was NOT applied to the affected streams"
        );
    }
}

pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "bluray_project.bin")
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let data = super::read_jar_file(reader, udf, "bluray_project.bin")?;
    // min_len=4 matches the prior local extract_strings impl. The token
    // grammar is `{lang3}_{codec?}_{purpose?}_{region?}_` so the
    // shortest meaningful run is 4 chars (lang + underscore).
    let strings = text::extract_ascii_strings(&data, 4);

    // Collects uncatalogued token components; if any, confidence downgrades to Medium
    // and they're reported once (see `UnknownParts`). Sequential parse, so a plain
    // owned collector suffices.
    let mut unknown = UnknownParts::default();

    let labels = assign_labels(&strings, &mut unknown);

    // Reported even when the parse yields nothing: "we recognized the format,
    // couldn't classify its components, and produced no labels" is precisely
    // the case worth surfacing.
    unknown.report();

    if labels.is_empty() {
        return None;
    }
    let confidence = if unknown.is_empty() {
        Confidence::High
    } else {
        Confidence::Medium
    };
    Some(ParseResult {
        labels,
        confidence,
        feature_playlist: None,
    })
}

// Walk the feature section's token strings, emitting a `StreamLabel` per
// editorial token in STN order. Split out from `parse` for unit-testing
// without a `SectorSource`/`UdfFs`.
fn assign_labels(strings: &[String], unknown: &mut UnknownParts) -> Vec<StreamLabel> {
    // The authoritative per-feature stream list lives in `FPL_` (FeaturePlaylist), in
    // STN order. `SEG_*` menu segments can carry stray tokens, so anchor on `FPL_` when
    // present; fall back to `SEG_MainFeature` only if no `FPL_` section exists.
    let has_fpl = strings.iter().any(|s| s.starts_with("FPL_"));

    let mut labels = Vec::new();
    let mut in_feature = false;
    let mut audio_num: u16 = 0;
    let mut sub_num: u16 = 0;
    // Which stream list the section is currently enumerating. Sections run
    // video → audio → PG, so audio is the correct start, and it only matters
    // for slots whose own type is unknowable (see the loop body).
    let mut domain = StreamLabelType::Audio;
    // The video slots this section has listed so far, in order. Used only to
    // recognise where the section ENDS — see the `Video Stream` arm below.
    let mut video_slots: Vec<&str> = Vec::new();

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
            domain = StreamLabelType::Audio;
            video_slots.clear();
            continue;
        }

        // Detect section end
        if in_feature && (s.starts_with("SEG_") || s.starts_with("SF_") || s.starts_with("FPL_")) {
            break;
        }

        if !in_feature {
            continue;
        }

        // Second section-end signal (the only one some discs give): every section's stream
        // list OPENS with its video slots, so a `Video Stream N` repeating one this section
        // listed marks the next section's start. Guards against swallowing trailing cards.
        if s.starts_with("Video Stream") {
            if video_slots.contains(&s.as_str()) || video_slots.len() >= MAX_VIDEO_SLOTS {
                break;
            }
            video_slots.push(s);
            continue;
        }

        // Stop accumulating once both counters reach the sane cap — a
        // crafted blob can't drive them to u16 overflow.
        if audio_num >= MAX_STREAMS_PER_TYPE && sub_num >= MAX_STREAMS_PER_TYPE {
            break;
        }

        // Every stream-list entry occupies one STN slot (editorial token, bare placeholder,
        // or unclassifiable token) and ALL must advance the per-type counter, else labels
        // renumber onto wrong streams. Unclassifiable slots follow `domain` (video→audio→PG).
        if let Some(kind) = placeholder_kind(s) {
            domain = kind;
            match kind {
                StreamLabelType::Audio => {
                    if audio_num < MAX_STREAMS_PER_TYPE {
                        audio_num += 1;
                    }
                }
                StreamLabelType::Subtitle => {
                    if sub_num < MAX_STREAMS_PER_TYPE {
                        sub_num += 1;
                    }
                }
            }
            continue;
        }

        if let Some(label) = parse_token_inner(s, Some(&mut *unknown)) {
            domain = label.stream_type;
            match label.stream_type {
                StreamLabelType::Audio => {
                    if audio_num >= MAX_STREAMS_PER_TYPE {
                        continue;
                    }
                    audio_num += 1;
                    labels.push(StreamLabel {
                        stream_id: None,
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
                        stream_id: None,
                        stream_number: sub_num,
                        ..label
                    });
                }
            }
        } else if is_stream_token(s) {
            // Token-shaped but unclassifiable: no label, but the slot is real.
            match domain {
                StreamLabelType::Audio => {
                    if audio_num < MAX_STREAMS_PER_TYPE {
                        audio_num += 1;
                    }
                }
                StreamLabelType::Subtitle => {
                    if sub_num < MAX_STREAMS_PER_TYPE {
                        sub_num += 1;
                    }
                }
            }
        }
    }

    labels
}

// The bare `Audio Stream N` / `PG Stream N` slot placeholders, and which
// list they belong to. `None` for anything else (`AR_…`, `Video Stream N`).
// See docs/pixelogic.md — placeholder_kind.
fn placeholder_kind(s: &str) -> Option<StreamLabelType> {
    if s.starts_with("Audio Stream") {
        Some(StreamLabelType::Audio)
    } else if s.starts_with("PG Stream") {
        Some(StreamLabelType::Subtitle)
    } else {
        None
    }
}

// Whether a string has the shape of a pixelogic stream token —
// `{lang3}_{component}…` — the same gate `parse_token_inner` applies.
// See docs/pixelogic.md — is_stream_token.
fn is_stream_token(s: &str) -> bool {
    let clean = s.trim().trim_start_matches('\t').trim_end_matches('_');
    let mut parts = clean.split('_');
    let Some(lang) = parts.next() else {
        return false;
    };
    if parts.next().is_none() {
        return false;
    }
    lang.len() == 3 && lang.chars().all(|c| c.is_ascii_lowercase())
}

fn parse_token_inner(s: &str, mut unknown: Option<&mut UnknownParts>) -> Option<StreamLabel> {
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
        // Token components are spec-uppercase (codec IDs, ADES/ACOM/SDH, region codes).
        // Normalize each to uppercase before the gate so a lowercase-authored token isn't
        // silently dropped through the unknown branch (no is_audio/is_subtitle set).
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
            // `FOR` (forced) is a subtitle-domain qualifier. Treat it as a subtitle signal
            // so a token whose only non-language component is FOR (e.g. `eng_FOR_`) isn't
            // dropped at the `!is_audio && !is_subtitle` guard below.
            qualifier = LabelQualifier::Forced;
            is_subtitle = true;
        } else if part == "DUB" {
            // `DUB` = forced-narrative subtitle for a language's dubbed audio (same class as
            // `*_TXT_FOR_`). Token-local, NOT in `vocab::qualifier` (English "dub" = dubbed
            // AUDIO). Like FOR, a subtitle-domain signal so the guard keeps the stream.
            qualifier = LabelQualifier::Forced;
            is_subtitle = true;
        } else if REGIONS.contains(&part) {
            variant = part.to_string();
        } else if part.starts_with("PGSTREAM") {
            is_subtitle = true;
        } else {
            // Unknown token component — skip this single part rather than discarding the
            // whole stream record (pre-refactor `return None` dropped streams over one
            // uncatalogued token). Recorded so the parse downgrades to Medium confidence.
            tracing::debug!(part = ?part, "pixelogic: unrecognized token component, skipping");
            if let Some(acc) = unknown.as_deref_mut() {
                acc.record(part);
            }
        }
    }

    if !is_audio && !is_subtitle {
        return None;
    }

    // Tie-break for tokens signalling both domains (e.g. `eng_MLP_SDH_`: is_audio via codec,
    // is_subtitle via SDH). An audio codec is the stronger signal, so prefer Audio when
    // present (keeps codec_hint); otherwise Subtitle. Pure tokens are unaffected.
    let has_audio_codec = is_audio && !codec.is_empty();
    let stream_type = if is_subtitle && !has_audio_codec {
        StreamLabelType::Subtitle
    } else {
        StreamLabelType::Audio
    };

    Some(StreamLabel {
        stream_id: None,
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

    // Immunity pin against the `paramount`-parser defect where a "forced"
    // marker on a FULL dialogue track's slot got read as "track is forced".
    // See docs/pixelogic.md — a_full_subtitle_token_is_never_forced....
    #[test]
    fn a_full_subtitle_token_is_never_forced_without_its_own_forced_component() {
        for token in [
            "eng_SDLG_",
            "eng_TXT_",
            "eng_SDH_",
            "eng_STRI_",
            "eng_SCOM_",
        ] {
            let l = parse_token_inner(token, None)
                .unwrap_or_else(|| panic!("{token} must classify as a subtitle"));
            assert_eq!(l.stream_type, StreamLabelType::Subtitle);
            assert_ne!(
                l.qualifier,
                LabelQualifier::Forced,
                "{token} carries no forced component and must not be forced"
            );
        }
        // And a language's forced pass is a SEPARATE slot from its full track,
        // never a marker applied to the full track's slot.
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "PG Stream 1",
            "eng_SDLG_",    // PG slot 2 — the full dialogue track
            "eng_TXT_FOR_", // PG slot 3 — its forced-narrative companion
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .map(|l| (l.stream_number, l.qualifier))
            .collect();
        assert_eq!(
            subs,
            vec![(2, LabelQualifier::None), (3, LabelQualifier::Forced),],
            "the forced marker belongs to its own slot, not to the full track's"
        );
    }

    #[test]
    fn parse_token_components_are_case_insensitive() {
        // Regression for the case-sensitive gate: a lowercase codec/qualifier component must
        // classify identically to uppercase, not fall through to the unknown branch and drop
        // the stream. The ISO 639-2 lang prefix is still required lowercase.
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
        // Observed case: FPL_MainFeature lists three unlabelled `Audio Stream N`
        // placeholders, then a lone `eng_ACOM_` commentary at STN slot 4. It must land on
        // audio #4, not collapse onto #1 (which would tag the main track as commentary).
        let mut flag = UnknownParts::default();
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

    // Real UHD feature shape: unlabelled/region-only PG slots must still
    // occupy STN slots so trailing forced tokens number correctly.
    // See docs/pixelogic.md — assign_labels_numbers_subtitles_by_stn_slot....
    #[test]
    fn assign_labels_numbers_subtitles_by_stn_slot_not_by_parsed_token() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "SEG_MainFeature",
            "Video Stream 1",
            "AR_169",
            // Audio list: 9 STN slots.
            "Audio Stream 1",
            "eng_ADES_",
            "fra_CF_",
            "fra_PF_",
            "Audio Stream 5",
            "Audio Stream 6",
            "spa_CS_",
            "Audio Stream 8",
            "spa_LS_",
            // PG list: 18 STN slots, in order.
            "PG Stream 1",
            "eng_SDH_",
            "fra_CF_",
            "fra_PF_",
            "PG Stream 5",
            "PG Stream 6",
            "spa_CS_",
            "PG Stream 8",
            "PG Stream 9",
            "spa_LS_",
            "eng_TXT_FOR_",
            "fra_CF_TXT_FOR_",
            "fra_PF_TXT_FOR_",
            "deu_TXT_FOR_",
            "ita_TXT_FOR_",
            "spa_CS_TXT_FOR_",
            "jpn_DUB_",
            "spa_LS_TXT_FOR_",
        ]);
        let labels = assign_labels(&tokens, &mut flag);

        let sdh: Vec<_> = labels
            .iter()
            .filter(|l| l.qualifier == LabelQualifier::Sdh)
            .map(|l| l.stream_number)
            .collect();
        assert_eq!(sdh, vec![2], "eng_SDH_ is PG STN slot 2");

        let forced: Vec<_> = labels
            .iter()
            .filter(|l| l.qualifier == LabelQualifier::Forced)
            .map(|l| l.stream_number)
            .collect();
        assert_eq!(
            forced,
            vec![11, 12, 13, 14, 15, 16, 17, 18],
            "every forced-narrative token in the run sits at PG STN slots 11-18"
        );

        // The lone audio label is unaffected: `eng_ADES_` is audio STN slot 2.
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .map(|l| l.stream_number)
            .collect();
        assert_eq!(audio, vec![2], "eng_ADES_ is audio STN slot 2");
    }

    #[test]
    fn assign_labels_prefers_fpl_over_seg_mainfeature() {
        // A `SEG_MainFeature` segment carries a stray commentary token, but the real
        // playlist is `FPL_MainFeature`. When an FPL_ section exists the SEG_ one is ignored
        // as anchor, so numbering comes from the FPL playlist (commentary at slot 2).
        let mut flag = UnknownParts::default();
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
        let mut flag = UnknownParts::default();
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

    // Spec: an explicit codec token (`AC3`) wins the Audio/Subtitle tiebreak
    // over a bare `SDH` signal. Mutation: flip the tie-break to Subtitle.
    #[test]
    fn parse_token_codec_always_wins_type_tiebreak() {
        let l = parse_token_inner("eng_AC3_SDH_", None).unwrap();
        assert_eq!(l.stream_type, StreamLabelType::Audio);
        assert_eq!(l.codec_hint, "Dolby Digital");
    }

    /// Spec: an unknown component is recorded by name, not merely flagged.
    /// Mutation: drop the `record` call → Medium confidence never triggered
    /// and the end-of-parse report never names the gap.
    #[test]
    fn parse_token_unknown_is_recorded_by_name() {
        let mut acc = UnknownParts::default();
        let _ = parse_token_inner("eng_MLP_FUTURETOKEN_", Some(&mut acc));
        assert!(!acc.is_empty(), "unknown component must be recorded");
        assert!(
            acc.seen.contains("FUTURETOKEN"),
            "the report must name the component, got {:?}",
            acc.seen
        );
    }

    /// Spec: a known-only token records nothing.
    /// Mutation: always record → all parses downgrade to Medium and every
    /// normal disc emits the warn.
    #[test]
    fn parse_token_all_known_records_nothing() {
        let mut acc = UnknownParts::default();
        let _ = parse_token_inner("eng_MLP_ACOM_US_", Some(&mut acc));
        assert!(acc.is_empty(), "all-known token must record nothing");
    }

    /// Spec: `Audio Stream N` placeholder advances audio_num but emits no label.
    /// Mutation: also emit a label for placeholder → audio#N+1 shifts to N+2.
    #[test]
    fn assign_labels_audio_placeholder_advances_counter_no_label() {
        let mut flag = UnknownParts::default();
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
        let mut flag = UnknownParts::default();
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
        let mut flag = UnknownParts::default();
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

    // Isolates the `SF_` alternative in the end-of-section check from
    // `SEG_`/`FPL_`, which a mutated `||` -> `&&` couldn't otherwise catch.
    // See docs/pixelogic.md — assign_labels_fpl_section_ends_on_sf_boundary.
    #[test]
    fn assign_labels_fpl_section_ends_on_sf_boundary() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "eng_MLP_",
            "SF_Something", // must end the FPL section
            "fra_AC3_",     // must NOT be parsed
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        assert_eq!(labels.len(), 1, "only eng from FPL section");
        assert_eq!(labels[0].language, "eng");
    }

    // The feature section also ends where the NEXT section's own video slot
    // repeats — the only boundary when the feature playlist is the last
    // named section (real corpus disc; see docs/pixelogic.md for the shape).
    #[test]
    fn assign_labels_section_ends_at_the_next_sections_video_slot() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "Video Stream 1",
            "AR_169",
            // Audio list: 2 STN slots.
            "Audio Stream 1",
            "eng_ADES_",
            // PG list: 2 STN slots.
            "PG Stream 1",
            "eng_SDH_",
            // End of the feature's list. No named section follows — the next
            // section is a notice card, announced only by its own video slot.
            "deu_Warning",
            "Video Stream 1",
            "AR_169",
            "fra_ND",
            "Video Stream 1",
            "AR_169",
            "eng_AC",
        ]);
        let labels = assign_labels(&tokens, &mut flag);

        let got: Vec<(StreamLabelType, u16, &str)> = labels
            .iter()
            .map(|l| (l.stream_type, l.stream_number, l.language.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                (StreamLabelType::Audio, 2, "eng"),
                (StreamLabelType::Subtitle, 2, "eng"),
            ],
            "only the feature playlist's own slots are labelled"
        );
        // A card's name is emitted BEFORE its section's video slot, so a forward-only walk
        // counts the first one while still nominally inside the feature section. That lone
        // tail entry can't renumber any label, only cost High confidence. Pinned, not hidden.
        assert_eq!(
            flag.seen.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["WARNING"],
            "only the card sitting between the last slot and the boundary leaks"
        );
    }

    // Companion: the boundary is a REPEATED video slot, not any video slot —
    // a section may legitimately list a secondary video stream.
    // Mutation: break on the first `Video Stream` seen → commentary lost.
    #[test]
    fn assign_labels_keeps_a_sections_distinct_video_slots() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "Video Stream 1",
            "Video Stream 2",
            "AR_169",
            "Audio Stream 1",
            "eng_ACOM_",
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(audio.len(), 1);
        assert_eq!(audio[0].stream_number, 2, "audio list is not cut short");
        assert_eq!(audio[0].purpose, LabelPurpose::Commentary);
    }

    // The video-slot memo is bounded by MAX_VIDEO_SLOTS since it's built
    // from disc bytes. Mutation: drop the length guard → unbounded memo.
    #[test]
    fn assign_labels_video_slot_memo_is_bounded() {
        let mut flag = UnknownParts::default();
        let mut tokens = vec!["FPL_MainFeature".to_string()];
        for i in 1..=(MAX_VIDEO_SLOTS + 50) {
            tokens.push(format!("Video Stream {i}"));
        }
        tokens.push("eng_ACOM_".to_string());
        let labels = assign_labels(&tokens, &mut flag);
        assert!(
            labels.is_empty(),
            "the walk stops once the video-slot memo is full"
        );
    }

    // The two per-type caps are independent: the loop only stops once BOTH
    // reach MAX_STREAMS_PER_TYPE. Mutation: `&&` -> `||` stops on either.
    // See docs/pixelogic.md — assign_labels_audio_cap_alone....
    #[test]
    fn assign_labels_audio_cap_alone_does_not_stop_subtitle_processing() {
        let mut flag = UnknownParts::default();
        let mut tokens = vec!["FPL_MainFeature".to_string()];
        for i in 1..=(MAX_STREAMS_PER_TYPE as usize) {
            tokens.push(format!("Audio Stream {}", i));
        }
        // Subtitle counter is still 0 here — well under the cap.
        tokens.push("eng_SDH_".to_string());
        let labels = assign_labels(&tokens, &mut flag);
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .collect();
        assert_eq!(
            subs.len(),
            1,
            "a subtitle stream after the audio cap (but under the subtitle \
             cap) must still be labeled"
        );
    }

    // Companion: subtitle saturated, audio under cap — audio must still
    // process. Isolates the first `>=` operand from the second.
    // See docs/pixelogic.md — assign_labels_subtitle_cap_alone....
    #[test]
    fn assign_labels_subtitle_cap_alone_does_not_stop_audio_processing() {
        let mut flag = UnknownParts::default();
        let mut tokens = vec!["FPL_MainFeature".to_string()];
        for _ in 1..=(MAX_STREAMS_PER_TYPE as usize) {
            tokens.push("eng_SDH_".to_string());
        }
        // Audio counter is still 0 here — well under the cap.
        tokens.push("fra_MLP_".to_string());
        let labels = assign_labels(&tokens, &mut flag);
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        assert_eq!(
            audio.len(),
            1,
            "an audio stream after the subtitle cap (but under the audio \
             cap) must still be labeled"
        );
    }

    // `PG Stream N` occupies a PG STN slot and advances the subtitle
    // counter, independent of audio. Mutation: skip PG placeholders →
    // later subtitle labels shift down.
    #[test]
    fn assign_labels_pg_placeholder_advances_sub_counter_only() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "Audio Stream 1",
            "Audio Stream 2",
            "eng_MLP_",    // audio token — audio_num becomes 3
            "PG Stream 1", // unlabelled PG slot 1
            "PG Stream 2", // unlabelled PG slot 2
            "eng_SDH_",    // subtitle token — PG STN slot 3
            "fra_SDH_",    // subtitle token — PG STN slot 4
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .map(|l| l.stream_number)
            .collect();
        assert_eq!(subs, vec![3, 4], "PG placeholders occupy PG STN slots");
        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .map(|l| l.stream_number)
            .collect();
        assert_eq!(audio, vec![3], "PG placeholders must not touch audio");
    }

    // A token-shaped but unclassifiable entry (`fra_CF_`, `jpn_ZZQ_`) still
    // occupies an STN slot in the current list.
    // Mutation: skip unclassifiable tokens → later labels shift down.
    #[test]
    fn assign_labels_unclassifiable_token_still_occupies_a_slot() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "PG Stream 1", // switches the domain to PG, slot 1
            "fra_CF_",     // region-only: no label, but PG slot 2
            "jpn_ZZQ_",    // uncatalogued: no label, but PG slot 3
            "eng_SDH_",    // PG slot 4
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .map(|l| l.stream_number)
            .collect();
        assert_eq!(subs, vec![4]);
    }

    // `DUB` is the subtitle-domain forced-narrative marker, same class as
    // `TXT_FOR`. Mutation: drop the `DUB` arm → token falls to unknown
    // branch and the whole stream is silently dropped.
    #[test]
    fn parse_token_dub_is_a_forced_narrative_subtitle() {
        let l = parse_token_inner("jpn_DUB_", None).expect("DUB must classify as a subtitle");
        assert_eq!(l.stream_type, StreamLabelType::Subtitle);
        assert_eq!(l.language, "jpn");
        assert_eq!(l.qualifier, LabelQualifier::Forced);
        assert_eq!(l.purpose, LabelPurpose::Normal);
        // Case-insensitive like every other component.
        let l = parse_token_inner("jpn_dub_", None).expect("lowercase DUB must classify too");
        assert_eq!(l.qualifier, LabelQualifier::Forced);
    }

    // `DUB` is catalogued, so it must not be reported as uncatalogued
    // (disc keeps High confidence). Mutation: leave DUB unknown → Medium.
    #[test]
    fn parse_token_dub_is_not_reported_as_uncatalogued() {
        let mut acc = UnknownParts::default();
        let _ = parse_token_inner("jpn_DUB_", Some(&mut acc));
        assert!(acc.is_empty(), "DUB is catalogued, got {:?}", acc.seen);
    }

    // A `DUB` slot inside a run of `*_TXT_FOR_` siblings yields a forced
    // label on its own slot, contiguous with its neighbours.
    // Mutation: classify DUB as audio → PG run gains a hole.
    #[test]
    fn assign_labels_dub_slot_is_forced_in_a_forced_run() {
        let mut flag = UnknownParts::default();
        let tokens = strs(&[
            "FPL_MainFeature",
            "PG Stream 1",
            "eng_TXT_FOR_", // PG slot 2
            "fra_CF_TXT_FOR_",
            "jpn_DUB_", // PG slot 4 — same class, different spelling
            "spa_TXT_FOR_",
        ]);
        let labels = assign_labels(&tokens, &mut flag);
        let forced: Vec<_> = labels
            .iter()
            .filter(|l| l.qualifier == LabelQualifier::Forced)
            .map(|l| (l.stream_type, l.stream_number, l.language.as_str()))
            .collect();
        assert_eq!(
            forced,
            vec![
                (StreamLabelType::Subtitle, 2, "eng"),
                (StreamLabelType::Subtitle, 3, "fra"),
                (StreamLabelType::Subtitle, 4, "jpn"),
                (StreamLabelType::Subtitle, 5, "spa"),
            ]
        );
        assert!(flag.is_empty(), "no uncatalogued components in this run");
    }

    // `UnknownParts` deduplicates, so the SAME component on dozens of
    // entries reports once. Mutation: use a Vec → `distinct` grows.
    #[test]
    fn unknown_parts_dedups_but_counts_every_occurrence() {
        let mut acc = UnknownParts::default();
        for _ in 0..50 {
            acc.record("ZZQ");
        }
        acc.record("QQZ");
        assert_eq!(acc.seen.len(), 2, "two distinct components");
        assert_eq!(acc.total, 51, "every occurrence counted");
    }

    /// Spec: the retained set is bounded — a crafted blob with thousands of
    /// distinct components must not grow it without bound, and must not panic.
    /// Mutation: drop the cap check → unbounded memory from disc bytes.
    #[test]
    fn unknown_parts_retention_is_bounded() {
        let mut acc = UnknownParts::default();
        for i in 0..10_000 {
            acc.record(&format!("PART{}", i));
        }
        assert_eq!(acc.seen.len(), MAX_REPORTED_UNKNOWN);
        assert_eq!(acc.total, 10_000, "occurrences still counted past the cap");
    }

    /// Spec: an over-long component is truncated by CHARS, so a multi-byte
    /// sequence is never split. Byte-offset truncation would panic here.
    /// Mutation: `part[..MAX_UNKNOWN_LEN].to_string()` → panics mid-char.
    #[test]
    fn unknown_parts_truncates_on_char_boundaries() {
        let mut acc = UnknownParts::default();
        let long: String = "é".repeat(500); // 2 bytes per char
        acc.record(&long);
        let stored = acc.seen.iter().next().expect("recorded");
        assert_eq!(stored.chars().count(), MAX_UNKNOWN_LEN);
        // Round-trips as valid UTF-8 — no split code point.
        assert!(stored.chars().all(|c| c == 'é'));
    }

    // Per-language notice/disclaimer clip names merely COLLIDE with the
    // token shape and must stay uncatalogued, collapsing into ONE report.
    // See docs/pixelogic.md — unknown_parts_collapses_a_wall....
    #[test]
    fn unknown_parts_collapses_a_wall_of_segment_name_collisions() {
        let mut acc = UnknownParts::default();
        for lang in ["ara", "bul", "ces", "dan", "deu", "ell"] {
            let _ = lang;
            acc.record("ND");
            acc.record("WARNING");
        }
        assert_eq!(acc.seen.len(), 2, "one entry per distinct component");
        assert_eq!(acc.total, 12);
    }

    /// Spec: `is_stream_token` accepts exactly what `parse_token_inner`'s own
    /// entry gate accepts — `{lang3}_{component}…` — so the section's
    /// `Video Stream 1` / `AR_169` entries never consume a stream slot.
    #[test]
    fn is_stream_token_matches_the_parser_entry_gate() {
        assert!(is_stream_token("eng_SDH_"));
        assert!(is_stream_token("fra_CF_"));
        assert!(is_stream_token("jpn_DUB_"));
        assert!(!is_stream_token("eng"), "a bare language is not a token");
        assert!(!is_stream_token("AR_169"), "AR is not a 3-letter language");
        assert!(!is_stream_token("Video Stream 1"));
        assert!(!is_stream_token("entry-markEa9"));
    }
}
