use super::*;

/// A vendor label: a slot in one stream table, naming no stream.
fn label(t: StreamLabelType, n: u16, lang: &str, codec: &str) -> StreamLabel {
    StreamLabel {
        stream_id: None,
        stream_number: n,
        stream_type: t,
        language: lang.into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: codec.into(),
        variant: String::new(),
    }
}

/// A derived label: names the stream it describes.
fn derived(
    t: StreamLabelType,
    clip: &str,
    pid: u16,
    n: u16,
    lang: &str,
    codec: &str,
) -> StreamLabel {
    StreamLabel {
        stream_id: Some(StreamId {
            clip_id: clip.into(),
            pid,
        }),
        ..label(t, n, lang, codec)
    }
}

#[test]
fn empty_framework_takes_all_mpls() {
    let mut framework: Vec<StreamLabel> = Vec::new();
    let mpls = vec![
        derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
        derived(StreamLabelType::Audio, "00001", 0x1101, 2, "fra", "AC-3"),
        derived(StreamLabelType::Subtitle, "00001", 0x1200, 1, "eng", "PG"),
    ];
    merge_mpls_floor(&mut framework, &mpls);
    assert_eq!(framework.len(), 3);
}

// Spec: merge suppresses a floor entry only when the list already NAMES that stream — a
// shared slot number is not the same fact.
#[test]
fn suppression_is_by_named_stream_not_by_slot_number() {
    // Framework claims audio slots 1 and 2 but names no stream.
    let mut framework = vec![
        label(StreamLabelType::Audio, 1, "eng", "Atmos"),
        label(StreamLabelType::Audio, 2, "fra", "Atmos"),
    ];
    let mpls = vec![
        derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
        derived(StreamLabelType::Audio, "00001", 0x1101, 2, "fra", "AC-3"),
    ];
    merge_mpls_floor(&mut framework, &mpls);
    assert_eq!(
        framework.len(),
        4,
        "slot collision is not stream identity: both floor entries survive"
    );
    // The framework's richer hints are untouched — they simply are no
    // longer competing with the floor for a number.
    let vendor: Vec<&str> = framework
        .iter()
        .filter(|l| l.stream_id.is_none())
        .map(|l| l.codec_hint.as_str())
        .collect();
    assert_eq!(vendor, vec!["Atmos", "Atmos"]);
}

/// A floor entry for a stream the framework already named is redundant and
/// is dropped; the framework's own (richer) label for that stream stays.
#[test]
fn floor_entry_for_an_already_named_stream_is_dropped() {
    let mut framework = vec![derived(
        StreamLabelType::Audio,
        "00001",
        0x1100,
        1,
        "eng",
        "Atmos",
    )];
    let mpls = vec![
        derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
        derived(StreamLabelType::Audio, "00001", 0x1101, 2, "fra", "AC-3"),
    ];
    merge_mpls_floor(&mut framework, &mpls);
    assert_eq!(framework.len(), 2);
    assert_eq!(framework[0].codec_hint, "Atmos", "framework label survives");
    assert_eq!(framework[1].codec_hint, "AC-3");
}

/// Partial-yield case: the framework labelled 2 of 6 audios and 1 of 2
/// subtitles. The floor supplies all eight streams; the framework's two
/// editorial labels are kept alongside, to be preferred at bind time.
#[test]
fn partial_yield_keeps_framework_and_adds_the_whole_floor() {
    let mut framework = vec![
        label(StreamLabelType::Audio, 1, "eng", "Atmos"),
        label(StreamLabelType::Audio, 4, "eng", "Commentary"),
        label(StreamLabelType::Subtitle, 1, "eng", "PG SDH"),
    ];
    let mut mpls = Vec::new();
    for (i, lang) in ["eng", "fra", "spa", "eng", "deu", "ita"]
        .iter()
        .enumerate()
    {
        mpls.push(derived(
            StreamLabelType::Audio,
            "00001",
            0x1100 + i as u16,
            (i + 1) as u16,
            lang,
            "AC-3",
        ));
    }
    for (i, lang) in ["eng", "fra"].iter().enumerate() {
        mpls.push(derived(
            StreamLabelType::Subtitle,
            "00001",
            0x1200 + i as u16,
            (i + 1) as u16,
            lang,
            "PG",
        ));
    }
    merge_mpls_floor(&mut framework, &mpls);
    assert_eq!(framework.len(), 3 + 8, "3 framework + all 8 floor streams");
    let vendor_hints: Vec<&str> = framework
        .iter()
        .filter(|l| l.stream_id.is_none())
        .map(|l| l.codec_hint.as_str())
        .collect();
    assert_eq!(vendor_hints, vec!["Atmos", "Commentary", "PG SDH"]);
}

/// Sort order is presentation only: audios first, vendor slots ahead of the
/// PID-named labels, each group in its own ascending order.
#[test]
fn sort_groups_audio_before_subtitle_and_vendor_before_named() {
    let mut labels = vec![
        derived(StreamLabelType::Subtitle, "00001", 0x1200, 1, "eng", "PG"),
        label(StreamLabelType::Subtitle, 1, "eng", "SDH"),
        derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
        label(StreamLabelType::Audio, 1, "eng", "Atmos"),
    ];
    sort_labels(&mut labels);
    let shape: Vec<(StreamLabelType, bool)> = labels
        .iter()
        .map(|l| (l.stream_type, l.stream_id.is_some()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (StreamLabelType::Audio, false),
            (StreamLabelType::Audio, true),
            (StreamLabelType::Subtitle, false),
            (StreamLabelType::Subtitle, true),
        ]
    );
}
