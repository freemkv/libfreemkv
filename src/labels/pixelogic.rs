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
// Ceiling on streams of one type per feature section: stops a crafted blob with tens of
// thousands of tokens from overflowing the u16 STN counters.
const MAX_STREAMS_PER_TYPE: u16 = 512;
// Ceiling on distinct video-slot entries one section may list before the walk gives up on it
// (used to find the next section's start).
const MAX_VIDEO_SLOTS: usize = 33;
/// Known region tokens
const REGIONS: &[&str] = &[
    "US", "UK", "CF", "PF", "CS", "LS", "BP", "PP", "SM", "TM", "CAN", "DUM", "FLE",
];
// Cap on distinct uncatalogued token components retained for the end-of-parse report; past the
// cap they're still counted, not named.
const MAX_REPORTED_UNKNOWN: usize = 16;
/// Longest retained form of a single uncatalogued component. Components come
/// from disc bytes and can be arbitrarily long; truncation is by CHARS, not
/// bytes, so a multi-byte sequence can never be split (which would panic).
const MAX_UNKNOWN_LEN: usize = 32;

// Collects the uncatalogued token components one parse ran into, reported ONCE at the end
// rather than silently or per-occurrence.
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
    // The token grammar is `{lang3}_{codec?}_{purpose?}_{region?}_` so the
    // shortest meaningful run is 4 chars (lang + underscore). The extractor caps run count (untrusted blob).
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

// The bare `Audio Stream N` / `PG Stream N` slot placeholders, and which list they belong to.
// `None` for anything else (`AR_…`, `Video Stream N`).
fn placeholder_kind(s: &str) -> Option<StreamLabelType> {
    if s.starts_with("Audio Stream") {
        Some(StreamLabelType::Audio)
    } else if s.starts_with("PG Stream") {
        Some(StreamLabelType::Subtitle)
    } else {
        None
    }
}

// Whether a string has the shape of a pixelogic stream token — `{lang3}_{component}…` — the
// same gate `parse_token_inner` applies.
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

#[cfg(test)]
#[path = "pixelogic_tests.rs"]
mod tests;
