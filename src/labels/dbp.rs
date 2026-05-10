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
//! Per `freemkv-private/memory/feedback_label_data_rules.md`: this
//! parser knows its own format, so we map human-readable language
//! names ("English", "Spanish", "Canadian French", ...) to ISO 639-2
//! codes locally. The full disc-authored display string is preserved
//! in the label `name` field — consumers display it raw without
//! freemkv guessing further structure.

use super::{LabelPurpose, LabelQualifier, StreamLabel, StreamLabelType};
use crate::sector::SectorReader;
use crate::udf::UdfFs;

/// dbp detect can't peek inside a jar without a SectorReader (the
/// trait function only takes `&UdfFs`), so we trigger on the cheap
/// signal "any top-level .jar in /BDMV/JAR/." That fires on every
/// BD-J disc, but parse() does the real `com/dbp/` check and
/// returns None on a mismatch — so this parser only ever consumes
/// time on discs that fell through every earlier parser. The
/// parse-side mismatch is bounded (read one .jar, list central
/// directory, walk class strings).
pub fn detect(udf: &UdfFs) -> bool {
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return false;
    };
    jar_dir
        .entries
        .iter()
        .any(|e| !e.is_dir && e.name.to_lowercase().ends_with(".jar"))
}

pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in &jar_dir.entries {
        if entry.is_dir {
            continue;
        }
        if !entry.name.to_lowercase().ends_with(".jar") {
            continue;
        }
        let path = format!("/BDMV/JAR/{}", entry.name);
        let Ok(bytes) = udf.read_file(reader, &path) else {
            continue;
        };
        let cursor = std::io::Cursor::new(&bytes);
        let Ok(mut archive) = zip::ZipArchive::new(cursor) else {
            continue;
        };
        if !archive_has_dbp(&mut archive) {
            continue;
        }
        let labels = scan_jar(&mut archive);
        if !labels.is_empty() {
            return Some(labels);
        }
    }
    None
}

fn archive_has_dbp<R: std::io::Read + std::io::Seek>(archive: &mut zip::ZipArchive<R>) -> bool {
    for i in 0..archive.len() {
        if let Ok(f) = archive.by_index(i) {
            if f.name().starts_with("com/dbp/") {
                return true;
            }
        }
    }
    false
}

fn scan_jar<R: std::io::Read + std::io::Seek>(
    archive: &mut zip::ZipArchive<R>,
) -> Vec<StreamLabel> {
    use std::collections::BTreeMap;
    // BTreeMap so we keep the highest-numbered (last-written) label
    // for each stream slot deterministic across runs. Entries are
    // collected from string-pool fragments scattered across hundreds
    // of obfuscated .class files; the same TextField,Audio1,...
    // string can appear in multiple classes (button-state variants,
    // localization fallbacks). Last write wins — they should all
    // agree on the label text, but the structure is defensive.
    let mut audios: BTreeMap<u16, String> = BTreeMap::new();
    let mut subs: BTreeMap<u16, String> = BTreeMap::new();

    for i in 0..archive.len() {
        let Ok(mut f) = archive.by_index(i) else {
            continue;
        };
        if !f.name().ends_with(".class") {
            continue;
        }
        let mut buf = Vec::new();
        if std::io::Read::read_to_end(&mut f, &mut buf).is_err() {
            continue;
        }
        for s in extract_printable(&buf) {
            collect_textfield(&s, &mut audios, &mut subs);
        }
    }

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
    audios: &mut std::collections::BTreeMap<u16, String>,
    subs: &mut std::collections::BTreeMap<u16, String>,
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
    let (language, qualifier, purpose) = parse_attributes(&label);
    StreamLabel {
        stream_number: num,
        stream_type,
        language,
        name: label,
        purpose,
        qualifier,
        codec_hint: String::new(),
        variant: String::new(),
    }
}

fn parse_attributes(label: &str) -> (String, LabelQualifier, LabelPurpose) {
    let lower = label.to_lowercase();
    let language = detect_language(&lower);
    let qualifier = if lower.contains("sdh") {
        LabelQualifier::Sdh
    } else if lower.contains("descriptive service") || lower.contains(" rnib") {
        LabelQualifier::DescriptiveService
    } else if lower.contains("forced") {
        LabelQualifier::Forced
    } else {
        LabelQualifier::None
    };
    let purpose = if lower.contains("commentary") {
        LabelPurpose::Commentary
    } else if lower.contains("descriptive") || lower.contains("audio description") {
        LabelPurpose::Descriptive
    } else {
        LabelPurpose::Normal
    };
    (language, qualifier, purpose)
}

/// Map English-language label tokens to ISO 639-2 codes. Keep this
/// list conservative — only common tokens we've actually observed
/// or that have a canonical mapping. Returns "" when the token
/// isn't recognized; the consumer falls back to fill_defaults reading
/// MPLS spec language codes.
fn detect_language(lower: &str) -> String {
    // Compound tokens first (multi-word language names).
    for (needle, code) in [
        ("brazilian portuguese", "por"),
        ("euro portuguese", "por"),
        ("castilian spanish", "spa"),
        ("latin american spanish", "spa"),
        ("canadian french", "fra"),
        ("parisian french", "fra"),
        ("australian english", "eng"),
        ("austrailian english", "eng"), // disc-corpus typo, keep matching
    ] {
        if lower.contains(needle) {
            return code.to_string();
        }
    }
    // Then bare tokens. Order matters where one is prefix of another.
    for (needle, code) in [
        ("english", "eng"),
        ("spanish", "spa"),
        ("french", "fra"),
        ("german", "deu"),
        ("italian", "ita"),
        ("japanese", "jpn"),
        ("chinese", "zho"),
        ("portuguese", "por"),
        ("polish", "pol"),
        ("czech", "ces"),
        ("hungarian", "hun"),
        ("dutch", "nld"),
        ("korean", "kor"),
        ("arabic", "ara"),
        ("hindi", "hin"),
        ("turkish", "tur"),
        ("thai", "tha"),
        ("swedish", "swe"),
        ("norwegian", "nor"),
        ("danish", "dan"),
        ("finnish", "fin"),
        ("hebrew", "heb"),
        ("russian", "rus"),
    ] {
        if lower.split_whitespace().next() == Some(needle)
            || lower.split_whitespace().any(|w| w == needle)
        {
            return code.to_string();
        }
    }
    String::new()
}

fn extract_printable(data: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for &b in data {
        if (0x20..0x7f).contains(&b) {
            current.push(b as char);
        } else {
            if current.len() >= 5 {
                out.push(current.clone());
            }
            current.clear();
        }
    }
    if current.len() >= 5 {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_extracts_audio_and_subtitle_indices() {
        let mut audios = std::collections::BTreeMap::new();
        let mut subs = std::collections::BTreeMap::new();
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
        let mut audios = std::collections::BTreeMap::new();
        let mut subs = std::collections::BTreeMap::new();
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
    fn parse_attributes_recognizes_sdh() {
        let (lang, qual, purp) = parse_attributes("English SDH");
        assert_eq!(lang, "eng");
        assert_eq!(qual, LabelQualifier::Sdh);
        assert_eq!(purp, LabelPurpose::Normal);
    }

    #[test]
    fn parse_attributes_recognizes_descriptive_audio() {
        let (lang, qual, purp) = parse_attributes("English Descriptive Audio");
        assert_eq!(lang, "eng");
        assert_eq!(qual, LabelQualifier::None);
        assert_eq!(purp, LabelPurpose::Descriptive);
    }

    #[test]
    fn parse_attributes_recognizes_commentary() {
        let (lang, qual, purp) = parse_attributes("English Director's Commentary");
        assert_eq!(lang, "eng");
        assert_eq!(qual, LabelQualifier::None);
        assert_eq!(purp, LabelPurpose::Commentary);
    }

    #[test]
    fn parse_attributes_recognizes_compound_languages() {
        assert_eq!(parse_attributes("Brazilian Portuguese 5.1").0, "por");
        assert_eq!(parse_attributes("Castilian Spanish").0, "spa");
        assert_eq!(parse_attributes("Canadian French Dolby Digital").0, "fra");
        assert_eq!(parse_attributes("Latin American Spanish").0, "spa");
    }

    #[test]
    fn parse_attributes_returns_empty_for_unknown_language() {
        // Don't guess. Per the rules-of-engagement.
        assert_eq!(parse_attributes("Klingon Dolby Atmos").0, "");
    }

    #[test]
    fn parse_attributes_recognizes_rnib_descriptive_service() {
        let (lang, qual, _) = parse_attributes("English RNIB");
        assert_eq!(lang, "eng");
        assert_eq!(qual, LabelQualifier::DescriptiveService);
    }
}
