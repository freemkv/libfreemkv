//! "dbp" framework — Magnolia Pictures BD-J authoring shop (per
//! `bd-live.magpictures.com` referenced in the disc's
//! `com/dbp/bluray.MenuXlet.perm`). Detected on UHD discs whose
//! `/BDMV/JAR/<x>.jar` (top-level, not in a subdir) contains
//! `com/dbp/` package paths.
//!
//! Stream labels live as plain ASCII strings inside compiled `.class`
//! files in the jar — a quirk of the menu-rendering layer encoding
//! its TextField positions and content as constant strings the
//! Java compiler retained in the class string pool. Format observed
//! in the corpus (Civil War UHD, 2024):
//!
//! ```text
//! LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,...
//! RTextField,Audio2,English Descriptive Audio,Fontstrip_Composite,...
//! HTextField,Subtitle1,English SDH,Fontstrip_Composite,...
//! ATextField,Subtitle0,None,Fontstrip_Composite,...
//! ```
//!
//! The single uppercase letter before `TextField` is string-pool
//! prefix noise — the parser anchors on `TextField,` regardless of
//! what precedes it. `Subtitle0` is the disable-subtitles menu
//! button and is skipped (not a real subtitle stream).
//!
//! ## Implementation
//!
//! v2 (2026-05-10): rewritten on top of [`super::class_reader`] —
//! iterates `CpInfo::Utf8` constant-pool entries instead of raw byte
//! scanning each class file. Equivalent label coverage (the literal
//! `TextField,...` strings live in the CP as Utf8 entries), but
//! structurally cleaner: no false-positive risk from method bytecode
//! or attribute names happening to contain `TextField,`. Language /
//! purpose / qualifier classification moved to [`super::vocab`] so all
//! Java-parser families share one source of truth.

use super::class_reader::CpInfo;
use super::{ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorReader;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

/// dbp detect can't peek inside a jar without a SectorReader (the
/// trait function only takes `&UdfFs`), so we trigger on the cheap
/// signal "any top-level .jar in /BDMV/JAR/." That fires on every
/// BD-J disc, but parse() does the real `com/dbp/` check and
/// returns None on a mismatch — so this parser only ever consumes
/// time on discs that fell through every earlier parser.
pub fn detect(udf: &UdfFs) -> bool {
    jar::has_any_top_level_jar(udf)
}

pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<ParseResult> {
    jar::for_each_jar(reader, udf, |_entry_name, archive| {
        if !jar::has_path_prefix(archive, "com/dbp/") {
            return None;
        }
        let labels = scan_jar(archive);
        if labels.is_empty() {
            None
        } else {
            // High confidence: TextField,Audio1,... is a stable anchor
            // pattern + vocab routes language/purpose/qualifier.
            Some(ParseResult::high(labels))
        }
    })
}

fn scan_jar(archive: &mut jar::Jar) -> Vec<StreamLabel> {
    // BTreeMap so we keep the highest-numbered (last-written) label
    // for each stream slot deterministic across runs. The same
    // TextField,Audio1,... string can appear in multiple classes
    // (button-state variants, localization fallbacks). Last write
    // wins — they should all agree, but the structure is defensive.
    let mut audios: BTreeMap<u16, String> = BTreeMap::new();
    let mut subs: BTreeMap<u16, String> = BTreeMap::new();

    jar::for_each_class(archive, |_class_name, class| {
        for (_idx, cp) in class.constant_pool.iter() {
            if let CpInfo::Utf8(s) = cp {
                collect_textfield(s, &mut audios, &mut subs);
            }
        }
    });

    let mut out = Vec::new();
    for (num, label) in audios {
        out.push(make_label(num, label, StreamLabelType::Audio));
    }
    for (num, label) in subs {
        out.push(make_label(num, label, StreamLabelType::Subtitle));
    }
    out
}

fn collect_textfield(
    s: &str,
    audios: &mut BTreeMap<u16, String>,
    subs: &mut BTreeMap<u16, String>,
) {
    // Anchor on "TextField," — the prefix character before it varies
    // (string-pool ordering inside compiled Java) and is irrelevant.
    let Some(idx) = s.find("TextField,") else {
        return;
    };
    let after = &s[idx + "TextField,".len()..];
    let mut parts = after.splitn(3, ',');
    let kind_n = parts.next().unwrap_or("").trim();
    let label = parts.next().unwrap_or("").trim();
    if label.is_empty() {
        return;
    }
    if let Some(rest) = kind_n.strip_prefix("Audio") {
        if let Ok(n) = rest.parse::<u16>() {
            audios.insert(n, label.to_string());
        }
    } else if let Some(rest) = kind_n.strip_prefix("Subtitle") {
        if let Ok(n) = rest.parse::<u16>() {
            // Subtitle0 is conventionally the "None / Off" disable
            // button, not an actual subtitle stream.
            if n > 0 {
                subs.insert(n, label.to_string());
            }
        }
    }
}

fn make_label(num: u16, label: String, stream_type: StreamLabelType) -> StreamLabel {
    let lang_info = vocab::lang(&label);
    let language = lang_info.map(|l| l.code).unwrap_or("").to_string();
    let variant = lang_info.map(|l| l.variant).unwrap_or("").to_string();
    let qualifier = vocab::qualifier(&label);
    let purpose = vocab::purpose(&label);
    StreamLabel {
        stream_number: num,
        stream_type,
        language,
        name: label,
        purpose,
        qualifier,
        codec_hint: String::new(),
        variant,
    }
}

#[cfg(test)]
mod tests {
    use super::super::{LabelPurpose, LabelQualifier};
    use super::*;

    #[test]
    fn collect_extracts_audio_and_subtitle_indices() {
        let mut audios = BTreeMap::new();
        let mut subs = BTreeMap::new();
        let lines = [
            "LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,296,763,275,25,left",
            "RTextField,Audio2,English Descriptive Audio,Fontstrip_Composite,296,803,275,25,left",
            "RTextField,Audio3,Spanish 5.1 Dolby Digital,Fontstrip_Composite,296,843,275,25,left",
            "ATextField,Subtitle0,None,Fontstrip_Composite,1312,843,275,25,left",
            "HTextField,Subtitle1,English SDH,Fontstrip_Composite,1312,763,275,25,left",
            "DTextField,Subtitle2,Spanish,Fontstrip_Composite,1312,803,275,25,left",
        ];
        for s in &lines {
            collect_textfield(s, &mut audios, &mut subs);
        }
        assert_eq!(audios.len(), 3);
        assert_eq!(audios[&1], "English Dolby Atmos");
        assert_eq!(audios[&2], "English Descriptive Audio");
        assert_eq!(audios[&3], "Spanish 5.1 Dolby Digital");
        // Subtitle0 ("None") is skipped — disable button, not a stream.
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[&1], "English SDH");
        assert_eq!(subs[&2], "Spanish");
    }

    #[test]
    fn collect_ignores_non_textfield_strings() {
        let mut audios = BTreeMap::new();
        let mut subs = BTreeMap::new();
        for s in [
            "GraphicButton,SU_Audio",
            "AudioMenu",
            "CommentaryMenuAlternateScenes",
            "PrimaryAudioControl",
        ] {
            collect_textfield(s, &mut audios, &mut subs);
        }
        assert!(audios.is_empty());
        assert!(subs.is_empty());
    }

    #[test]
    fn make_label_routes_via_vocab() {
        let l = make_label(1, "English SDH".to_string(), StreamLabelType::Subtitle);
        assert_eq!(l.language, "eng");
        assert_eq!(l.qualifier, LabelQualifier::Sdh);
        assert_eq!(l.purpose, LabelPurpose::Normal);
    }

    #[test]
    fn make_label_descriptive_audio() {
        let l = make_label(
            2,
            "English Descriptive Audio".to_string(),
            StreamLabelType::Audio,
        );
        assert_eq!(l.language, "eng");
        assert_eq!(l.purpose, LabelPurpose::Descriptive);
    }

    #[test]
    fn make_label_commentary() {
        let l = make_label(
            3,
            "English Director's Commentary".to_string(),
            StreamLabelType::Audio,
        );
        assert_eq!(l.language, "eng");
        assert_eq!(l.purpose, LabelPurpose::Commentary);
    }

    #[test]
    fn make_label_compound_languages_populate_variant() {
        let brazilian = make_label(1, "Brazilian Portuguese 5.1".into(), StreamLabelType::Audio);
        assert_eq!(brazilian.language, "por");
        assert_eq!(brazilian.variant, "Brazilian");

        let castilian = make_label(1, "Castilian Spanish".into(), StreamLabelType::Audio);
        assert_eq!(castilian.language, "spa");
        assert_eq!(castilian.variant, "Castilian");

        let canadian = make_label(
            1,
            "Canadian French Dolby Digital".into(),
            StreamLabelType::Audio,
        );
        assert_eq!(canadian.language, "fra");
        assert_eq!(canadian.variant, "Canadian");
    }

    #[test]
    fn make_label_bare_language_has_empty_variant() {
        let l = make_label(1, "English Dolby Atmos".into(), StreamLabelType::Audio);
        assert_eq!(l.language, "eng");
        assert_eq!(l.variant, "");
    }

    #[test]
    fn make_label_unknown_language_is_empty() {
        // vocab::lang returns None — make_label converts both fields to "".
        let l = make_label(1, "Klingon Dolby Atmos".into(), StreamLabelType::Audio);
        assert_eq!(l.language, "");
        assert_eq!(l.variant, "");
    }

    #[test]
    fn make_label_rnib_descriptive_service() {
        let l = make_label(1, "English RNIB".into(), StreamLabelType::Subtitle);
        assert_eq!(l.language, "eng");
        assert_eq!(l.qualifier, LabelQualifier::DescriptiveService);
    }
}
