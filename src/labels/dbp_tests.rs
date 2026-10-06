use super::super::{LabelPurpose, LabelQualifier};
use super::*;

// A budget no test fixture can exhaust.
fn unbounded() -> u64 {
    u64::MAX
}
use std::io::{Cursor, Write as _};

// Minimal, structurally valid `.class` file (JVMS §4.1) with only the
// given `Utf8` constant-pool entries — no fields/methods/attributes,
// since `scan_jar` only reads the constant pool.
fn build_class(utf8_entries: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&0xCAFEBABEu32.to_be_bytes()); // magic
    out.extend_from_slice(&0u16.to_be_bytes()); // minor_version
    out.extend_from_slice(&52u16.to_be_bytes()); // major_version (Java 8)
    out.extend_from_slice(&((utf8_entries.len() + 1) as u16).to_be_bytes()); // cp_count
    for s in utf8_entries {
        out.push(1); // CONSTANT_Utf8 tag
        out.extend_from_slice(&(s.len() as u16).to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    out.extend_from_slice(&0u16.to_be_bytes()); // access_flags
    out.extend_from_slice(&0u16.to_be_bytes()); // this_class
    out.extend_from_slice(&0u16.to_be_bytes()); // super_class
    out.extend_from_slice(&0u16.to_be_bytes()); // interfaces_count
    out.extend_from_slice(&0u16.to_be_bytes()); // fields_count
    out.extend_from_slice(&0u16.to_be_bytes()); // methods_count
    out.extend_from_slice(&0u16.to_be_bytes()); // attributes_count
    out
}

/// Zip `entries` (name -> bytes) into an in-memory, Stored (uncompressed)
/// `jar::Jar` via the `zip` crate's own writer — a real archive, not a
/// hand-rolled central directory.
fn build_jar_bytes(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, data) in entries {
            writer.start_file(*name, opts).expect("start_file");
            writer.write_all(&data[..]).expect("write class bytes");
        }
        writer.finish().expect("finish zip");
    }
    buf
}

fn build_jar(entries: &[(&str, Vec<u8>)]) -> jar::Jar {
    zip::ZipArchive::new(Cursor::new(build_jar_bytes(entries))).expect("valid zip")
}

// An in-memory disc holding one `/BDMV/JAR/<name>` jar per entry.
fn jar_disc(jars: &[(&str, Vec<u8>)]) -> (crate::udf::fixture::MemDisc, UdfFs) {
    use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
    let specs = jars
        .iter()
        .enumerate()
        .map(|(i, (name, bytes))| {
            let n = i as u32;
            file_with(name, 100 + n, 2000 + n * 4, bytes.clone(), true)
        })
        .collect();
    let dir = |name: &str, icb, data, files, subdirs| DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: data,
        files,
        subdirs,
    };
    let jar = dir("JAR", 52, 53, specs, vec![]);
    let bdmv = dir("BDMV", 12, 13, vec![], vec![jar]);
    let root = dir("", 10, 11, vec![], vec![bdmv]);
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

// The com/dbp/ prefix is the only thing keeping this parser (ahead of
// deluxe in PARSERS) off other BD-J discs that carry a TextField string.
#[test]
fn detect_and_parse_require_the_dbp_package_prefix() {
    let class =
        || build_class(&["LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,296,763"]);
    let (mut other, udf) = jar_disc(&[(
        "00000.jar",
        build_jar_bytes(&[("com/other/Menu.class", class())]),
    )]);
    assert!(!detect(&mut other, &udf));
    assert!(parse(&mut other, &udf).is_none());

    let (mut dbp, udf) = jar_disc(&[(
        "00000.jar",
        build_jar_bytes(&[("com/dbp/Menu.class", class())]),
    )]);
    assert!(detect(&mut dbp, &udf));
    assert_eq!(parse(&mut dbp, &udf).expect("labels").labels.len(), 1);
}

// Wires the class sweep + collect_textfield + make_label into the real
// per-jar scan (unit tests above only cover the pure pieces). Mutation
// pin: catches scan_jar's body being replaced with `vec![]`.
#[test]
fn scan_jar_extracts_labels_from_real_class_entries() {
    let class_bytes = build_class(&[
        "com/dbp/Whatever", // unrelated string — must be ignored
        "LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,296,763",
        "HTextField,Subtitle1,English SDH,Fontstrip_Composite,1312,763",
        "ATextField,Subtitle0,None,Fontstrip_Composite,1312,843", // disable button, skipped
    ]);
    let mut archive = build_jar(&[("com/dbp/Menu.class", class_bytes)]);

    let labels = scan_jar(&mut archive, &mut unbounded());

    assert_eq!(
        labels.len(),
        2,
        "expected one audio + one real subtitle label"
    );
    let audio = labels
        .iter()
        .find(|l| l.stream_type == StreamLabelType::Audio)
        .expect("audio label present");
    assert_eq!(audio.stream_number, 1);
    assert_eq!(audio.language, "eng");

    let sub = labels
        .iter()
        .find(|l| l.stream_type == StreamLabelType::Subtitle)
        .expect("subtitle label present");
    assert_eq!(sub.stream_number, 1);
    assert_eq!(sub.qualifier, LabelQualifier::Sdh);
}

// One inflate budget covers every jar a parse sweeps: what the first jar
// spends is gone for the second.
#[test]
fn scan_jar_budget_is_shared_across_jars() {
    let class = build_class(&["LTextField,Audio1,English,Fontstrip_Composite,296,763"]);
    let len = class.len() as u64;
    let mut first = build_jar(&[("com/dbp/A.class", class.clone())]);
    let mut second = build_jar(&[("com/dbp/B.class", class)]);
    let mut budget = len + len / 2;
    assert_eq!(scan_jar(&mut first, &mut budget).len(), 1);
    assert_eq!(budget, len / 2);
    assert!(scan_jar(&mut second, &mut budget).is_empty());
    assert_eq!(budget, 0);
}

// Immunity pin: stream numbers come from the AudioN/SubtitleN token, not
// iteration order, so gaps/skipped entries don't shift later labels.
// Mutation: numbering by iteration order would rebind Audio4/Subtitle3.
#[test]
fn stream_numbers_come_from_the_token_not_iteration_order() {
    let class_bytes = build_class(&[
        "LTextField,Audio1,English Dolby Atmos,Fontstrip_Composite,296,763",
        // Slots 2 and 3 have no menu TextField authored.
        "LTextField,Audio4,French 5.1 Dolby Digital,Fontstrip_Composite,296,803",
        // Not a stream: the disable-subtitles button.
        "ATextField,Subtitle0,None,Fontstrip_Composite,1312,843",
        // Unparseable slot token — dropped, and must shift nothing.
        "HTextField,SubtitleX,German,Fontstrip_Composite,1312,883",
        "HTextField,Subtitle3,English SDH,Fontstrip_Composite,1312,763",
    ]);
    let mut archive = build_jar(&[("com/dbp/Menu.class", class_bytes)]);

    let labels = scan_jar(&mut archive, &mut unbounded());
    let nums: Vec<(StreamLabelType, u16)> = labels
        .iter()
        .map(|l| (l.stream_type, l.stream_number))
        .collect();
    assert_eq!(
        nums,
        vec![
            (StreamLabelType::Audio, 1),
            (StreamLabelType::Audio, 4),
            (StreamLabelType::Subtitle, 3),
        ],
        "unlabelled and unusable slots leave the authored numbers alone"
    );
}

// CONSTANT_Utf8_info's u16 length lets one crafted constant retain up to
// 65535 bytes across 65536 slots (~4 GiB) without MAX_LABEL_BYTES.
// Boundary check: 256 bytes kept, 257 and the JVMS max 65535 refused.
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
fn collect_skips_entries_with_an_empty_or_missing_label() {
    let mut audios = BTreeMap::new();
    let mut subs = BTreeMap::new();
    for s in [
        "XTextField,Audio1,,rest",
        "XTextField,Audio2",
        "XTextField,Subtitle1, ,rest",
    ] {
        collect_textfield(s, &mut audios, &mut subs);
    }
    assert!(audios.is_empty() && subs.is_empty());
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

#[test]
fn audio_zero_is_not_retained() {
    let (mut a, mut s) = (BTreeMap::new(), BTreeMap::new());
    collect_textfield("xTextField,Audio0,English,y", &mut a, &mut s);
    assert!(a.is_empty(), "Audio0 is NO_STN_SLOT and can never bind");
    collect_textfield("xTextField,Audio1,English,y", &mut a, &mut s);
    assert_eq!(a.len(), 1);
}
