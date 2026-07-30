//! "dbp" framework — a BD-J authoring framework identified by
//! `com/dbp/` package paths in a top-level `/BDMV/JAR/<x>.jar` (not in
//! a subdir). Seen on UHD discs.
//!
//! Stream labels live as plain ASCII strings inside compiled `.class`
//! files in the jar — a quirk of the menu-rendering layer encoding
//! its TextField positions and content as constant strings the
//! Java compiler retained in the class string pool. Observed format:
//!
//! ```text
//! LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,...
//! RTextField,Audio2,English Descriptive Audio,Fontstrip_Composite,...
//! HTextField,Subtitle1,English SDH,Fontstrip_Composite,...
//! ATextField,Subtitle0,None,Fontstrip_Composite,...
//! ```
//!
//! The parser ignores any prefix before the first `TextField,`
//! occurrence — whatever string-pool ordering placed ahead of it is
//! irrelevant. `Subtitle0` is the disable-subtitles menu button and is
//! skipped (not a real subtitle stream).
//!
//! ## Implementation
//!
//! Iterates `CpInfo::Utf8` constant-pool entries rather than raw
//! byte-scanning each class file. Equivalent label coverage (the literal
//! `TextField,...` strings live in the CP as Utf8 entries) with no
//! false-positive risk from method bytecode or attribute names that
//! happen to contain `TextField,`. Language / purpose / qualifier
//! classification lives in [`super::vocab`] so all Java-parser families
//! share one source of truth.

use super::class_reader::CpInfo;
use super::{ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

/// The real dbp signal is the `com/dbp/` package prefix inside a top-level
/// jar's central directory. With a reader in `detect`, we check that directly
/// (a cheap central-directory scan, no class decode) so this parser claims
/// only dbp discs instead of firing on every BD-J disc. `parse()` repeats the
/// check as belt-and-suspenders.
pub fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    jar::for_each_jar(reader, udf, |_entry, archive| {
        jar::has_path_prefix(archive, "com/dbp/").then_some(())
    })
    .is_some()
}

/// Scan every top-level `/BDMV/JAR/*.jar` for the dbp framework and
/// extract its stream labels. Returns `None` if no jar carries a
/// `com/dbp/` package path or none yields any labels.
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
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

/// Cap on the bytes retained for one stream label.
///
/// The label is an owned copy of a slice of a `CONSTANT_Utf8_info` entry,
/// whose `length` field is a `u16` (JVMS §4.4.7) — so a single crafted
/// constant contributes up to 65535 bytes, and the `u16` stream-number
/// keyspace admits 65536 of them per type.
///
/// Headroom: real dbp menu labels are short display names — "English Dolby
/// Atmos" (19 bytes), "Spanish 5.1 Dolby Digital" (25). The longest plausible
/// retail string ("Portuguese (Brazilian) 5.1 Dolby Digital Plus") is 45
/// bytes. 256 leaves >5x headroom over that, and any string past it is menu
/// geometry or padding, never a language name — `vocab::lang` would not
/// resolve it anyway.
const MAX_LABEL_BYTES: usize = 256;

/// Cap on retained stream slots per type.
///
/// The keys come from `parse::<u16>()` on disc bytes, so all 65536 slots per
/// type are reachable; paired with [`MAX_LABEL_BYTES`] this bounds the whole
/// scan at 2 x 512 x 256 bytes.
///
/// Headroom: the BD STN_table admits at most 32 primary audio and 32 PG
/// streams per playlist, and dbp emits one menu TextField per stream. 512
/// leaves 16x headroom over the spec maximum.
const MAX_LABELS_PER_TYPE: usize = 512;

/// Record `label` for stream `n`, honouring the retention caps. Existing
/// slots are still overwritten at the cap so the documented last-write-wins
/// behaviour is preserved; only NEW slots are refused.
fn retain_label(map: &mut BTreeMap<u16, String>, n: u16, label: &str) {
    if label.len() > MAX_LABEL_BYTES {
        return;
    }
    if map.len() >= MAX_LABELS_PER_TYPE && !map.contains_key(&n) {
        return;
    }
    map.insert(n, label.to_string());
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
            retain_label(audios, n, label);
        }
    } else if let Some(rest) = kind_n.strip_prefix("Subtitle")
        && let Ok(n) = rest.parse::<u16>()
    {
        // Subtitle0 is conventionally the "None / Off" disable
        // button, not an actual subtitle stream.
        if n > 0 {
            retain_label(subs, n, label);
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

    /// A `CONSTANT_Utf8_info` carries a `u16` length (JVMS §4.4.7), so one
    /// crafted constant contributes up to 65535 bytes and the `u16` stream
    /// keyspace admits 65536 slots per type — ~4 GiB of retained `String` per
    /// map from a jar that is orders of magnitude smaller.
    ///
    /// Boundary literals, not the constant: a 256-byte label is kept, 257 and
    /// the JVMS maximum 65535 are refused.
    #[test]
    fn oversized_labels_are_not_retained() {
        let mut audios = BTreeMap::new();
        let mut subs = BTreeMap::new();

        collect_textfield(
            &format!("XTextField,Audio1,{},rest", "A".repeat(256)),
            &mut audios,
            &mut subs,
        );
        assert_eq!(
            audios.get(&1).map(String::len),
            Some(256),
            "a 256-byte label must still be retained"
        );

        collect_textfield(
            &format!("XTextField,Audio2,{},rest", "A".repeat(257)),
            &mut audios,
            &mut subs,
        );
        assert!(!audios.contains_key(&2), "a 257-byte label must be refused");

        collect_textfield(
            &format!("XTextField,Subtitle1,{},rest", "B".repeat(65_535)),
            &mut audios,
            &mut subs,
        );
        assert!(
            !subs.contains_key(&1),
            "a JVMS-maximum 65535-byte Utf8 label must be refused"
        );
    }

    /// The stream-slot keyspace is the full `u16` on both maps. Offer 600
    /// distinct audio slots; exactly 512 are retained.
    #[test]
    fn retained_stream_slots_are_capped_per_type() {
        let mut audios = BTreeMap::new();
        let mut subs = BTreeMap::new();
        for n in 1..=600u16 {
            collect_textfield(
                &format!("XTextField,Audio{n},English,rest"),
                &mut audios,
                &mut subs,
            );
        }
        assert_eq!(
            audios.len(),
            512,
            "600 audio slots offered, {} retained — the slot count is unbounded",
            audios.len()
        );
    }

    /// Reaching the slot cap must not break the documented last-write-wins
    /// behaviour for slots already held.
    #[test]
    fn existing_slot_is_still_overwritten_at_the_cap() {
        let mut audios = BTreeMap::new();
        let mut subs = BTreeMap::new();
        for n in 1..=600u16 {
            collect_textfield(
                &format!("XTextField,Audio{n},English,rest"),
                &mut audios,
                &mut subs,
            );
        }
        collect_textfield("XTextField,Audio1,Spanish,rest", &mut audios, &mut subs);
        assert_eq!(audios.get(&1).map(String::as_str), Some("Spanish"));
    }

    /// Headroom: the longest plausible retail label must survive untouched.
    #[test]
    fn longest_realistic_label_survives_the_cap() {
        let mut audios = BTreeMap::new();
        let mut subs = BTreeMap::new();
        let real = "Portuguese (Brazilian) 5.1 Dolby Digital Plus";
        assert_eq!(real.len(), 45, "fixture length changed");
        collect_textfield(
            &format!("XTextField,Audio1,{real},Fontstrip_Composite,296,763"),
            &mut audios,
            &mut subs,
        );
        assert_eq!(audios.get(&1).map(String::as_str), Some(real));
    }

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
