use super::*;

fn info(id: &str, t: StreamLabelType) -> StreamInfo {
    StreamInfo {
        id: id.into(),
        stream_type: t,
        language: "eng".into(),
        variant: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
    }
}

#[test]
fn fallback_numbers_dense_when_map_empty() {
    let infos = vec![
        info("a0", StreamLabelType::Audio),
        info("a1", StreamLabelType::Audio),
        info("s0", StreamLabelType::Subtitle),
    ];
    let nums =
        assign_stream_numbers(&infos, &HashMap::new()).expect("numbering space not exhausted");
    // Per-type 1-based: audio 1,2 ; subtitle 1.
    assert_eq!(nums, vec![1, 2, 1]);
}

// Immunity pin: an unusable/malformed element still occupies its slot (never dropped) and a
// close-less element can only shorten the list, never extend it.
#[test]
fn an_unterminated_stream_element_shortens_the_list_it_cannot_extend_it() {
    let sp = concat!(
        "<AudioStreamInfos><ID>a0</ID><LangInfoID>ENG</LangInfoID></AudioStreamInfos>",
        // No `</AudioStreamInfos>` for this one.
        "<AudioStreamInfos><ID>a1</ID><LangInfoID>FRA</LangInfoID>",
        "<AudioStreamInfos><ID>a2</ID><LangInfoID>DEU</LangInfoID></AudioStreamInfos>",
    );
    let infos = parse_stream_infos(sp);
    assert_eq!(
        infos.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        vec!["a0", "a1"],
        "the close-less element absorbs the one behind it — two slots, not \
             three, and never four"
    );
    assert_eq!(infos[1].language, "fra", "and keeps its own leading fields");

    // With no close tag anywhere behind it, the element is not returned at
    // all and the walk ends — the tail of the document never becomes a
    // stream list.
    let no_close = "<AudioStreamInfos><ID>a0</ID><LangInfoID>ENG</LangInfoID>";
    assert!(parse_stream_infos(no_close).is_empty());
}

#[test]
fn unusable_stream_element_still_occupies_its_position() {
    let sp = r#"
            <AudioStreamInfos><ID>a0</ID><LangInfoID>ENG_US</LangInfoID></AudioStreamInfos>
            <AudioStreamInfos></AudioStreamInfos>
            <AudioStreamInfos><ID>a2</ID><LangInfoID>FRA</LangInfoID><Content>COMMENTARY</Content></AudioStreamInfos>
            <SubtitleStreamInfos><ID>s0</ID><LangInfoID></LangInfoID><Qualifier>WAT</Qualifier></SubtitleStreamInfos>
            <SubtitleStreamInfos><ID>s1</ID><LangInfoID>ENG</LangInfoID><Qualifier>SDH</Qualifier></SubtitleStreamInfos>
        "#;
    let infos = parse_stream_infos(sp);
    assert_eq!(infos.len(), 5, "every element yields a StreamInfo");
    let nums =
        assign_stream_numbers(&infos, &HashMap::new()).expect("numbering space not exhausted");
    assert_eq!(
        nums,
        vec![1, 2, 3, 1, 2],
        "the blank element owns audio slot 2, so the commentary is slot 3"
    );
    assert_eq!(infos[2].purpose, LabelPurpose::Commentary);
    assert_eq!(infos[4].qualifier, LabelQualifier::Sdh);
}

#[test]
fn fallback_does_not_collide_with_partial_map() {
    // Map claims audio "a1" -> 1. The unmapped audio "a0" must NOT
    // also get 1 (the pre-fix bug); it must skip to 2.
    let mut map = HashMap::new();
    map.insert("a1".to_string(), 1u16);
    let infos = vec![
        info("a0", StreamLabelType::Audio), // unmapped → fallback
        info("a1", StreamLabelType::Audio), // mapped → 1
        info("a2", StreamLabelType::Audio), // unmapped → fallback
    ];
    let nums = assign_stream_numbers(&infos, &map).expect("numbering space not exhausted");
    // a0 skips the taken 1 → 2; a1 keeps 1; a2 → 3. All distinct.
    assert_eq!(nums, vec![2, 1, 3]);
    let mut sorted = nums.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 3, "stream numbers must be unique");
}

/// Duplicate stream numbers in playbackconfig: two distinct StreamInfo_IDs
/// mapped to the SAME number is contradictory disc data. The first keeps the
/// number; the second is DROPPED (NO_STN_SLOT) rather than bound to a
/// synthesized slot no data supports — no label beats a wrong label.
#[test]
fn duplicate_mapped_numbers_drop_the_second_stream() {
    let mut map = HashMap::new();
    map.insert("a0".to_string(), 1u16);
    map.insert("a1".to_string(), 1u16); // duplicate claim of 1
    let infos = vec![
        info("a0", StreamLabelType::Audio),
        info("a1", StreamLabelType::Audio),
        info("a2", StreamLabelType::Audio), // unmapped → fallback
    ];
    let nums = assign_stream_numbers(&infos, &map).expect("numbering space not exhausted");
    assert_eq!(nums, vec![1, super::super::NO_STN_SLOT, 2]);
}

#[test]
fn map_fully_drives_numbers_when_complete() {
    let mut map = HashMap::new();
    map.insert("a0".to_string(), 5u16);
    map.insert("a1".to_string(), 9u16);
    let infos = vec![
        info("a0", StreamLabelType::Audio),
        info("a1", StreamLabelType::Audio),
    ];
    assert_eq!(
        assign_stream_numbers(&infos, &map).expect("numbering space not exhausted"),
        vec![5, 9]
    );
}

// ── parse(): the playbackconfig.xml -> stream_map wiring ────────────────

// Lay /BDMV/JAR/00000/<files> onto an in-memory disc and open its filesystem.
fn jar_disc(files: &[(&str, &str)]) -> (crate::udf::fixture::MemDisc, UdfFs) {
    use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
    let specs = files
        .iter()
        .enumerate()
        .map(|(i, (name, body))| {
            let n = i as u32;
            file_with(name, 100 + n, 2000 + n * 4, body.as_bytes().to_vec(), true)
        })
        .collect();
    let dir = |name: &str, icb, data, files, subdirs| DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: data,
        files,
        subdirs,
    };
    let sub = dir("00000", 54, 55, specs, vec![]);
    let jar = dir("JAR", 52, 53, vec![], vec![sub]);
    let bdmv = dir("BDMV", 12, 13, vec![], vec![jar]);
    let root = dir("", 10, 11, vec![], vec![bdmv]);
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

// Dropping the playbackconfig read (or passing an empty map) would number the
// streams by fallback order and bind names to the wrong streams.
#[test]
fn parse_numbers_streams_from_playbackconfig() {
    let sp = "<AudioStreamInfos><ID>a0</ID><LangInfoID>ENG</LangInfoID></AudioStreamInfos>\
                  <AudioStreamInfos><ID>a1</ID><LangInfoID>FRA</LangInfoID></AudioStreamInfos>";
    let pc = "<AudioStreams><StreamID>2</StreamID><StreamInfo_ID>a0</StreamInfo_ID></AudioStreams>\
                  <AudioStreams><StreamID>1</StreamID><StreamInfo_ID>a1</StreamInfo_ID></AudioStreams>";
    let (mut disc, udf) = jar_disc(&[("streamproperties.xml", sp), ("playbackconfig.xml", pc)]);
    let got = parse(&mut disc, &udf).expect("labels");
    let by_lang: Vec<(&str, u16)> = got
        .labels
        .iter()
        .map(|l| (l.language.as_str(), l.stream_number))
        .collect();
    assert_eq!(by_lang, [("eng", 2), ("fra", 1)]);
}

// Two streams claiming the same number: the second is dropped, not emitted with the
// NO_STN_SLOT sentinel.
#[test]
fn parse_drops_a_stream_with_a_contradictory_number_claim() {
    let sp = "<AudioStreamInfos><ID>a0</ID><LangInfoID>ENG</LangInfoID></AudioStreamInfos>\
                  <AudioStreamInfos><ID>a1</ID><LangInfoID>FRA</LangInfoID></AudioStreamInfos>";
    let pc = "<AudioStreams><StreamID>2</StreamID><StreamInfo_ID>a0</StreamInfo_ID></AudioStreams>\
                  <AudioStreams><StreamID>2</StreamID><StreamInfo_ID>a1</StreamInfo_ID></AudioStreams>";
    let (mut disc, udf) = jar_disc(&[("streamproperties.xml", sp), ("playbackconfig.xml", pc)]);
    let got = parse(&mut disc, &udf).expect("labels");
    let kept: Vec<(&str, u16)> = got
        .labels
        .iter()
        .map(|l| (l.language.as_str(), l.stream_number))
        .collect();
    assert_eq!(kept, [("eng", 2)]);
}

// ── Additional hardening tests ─────────────────────────────────────────

/// Spec: audio and subtitle counters are INDEPENDENT — audio fallback counter
/// must not affect subtitle numbering and vice versa.
/// Mutation: use a single shared counter → subtitle gets wrong numbers.
#[test]
fn audio_and_subtitle_counters_are_independent() {
    let infos = vec![
        info("a0", StreamLabelType::Audio),
        info("s0", StreamLabelType::Subtitle),
        info("a1", StreamLabelType::Audio),
        info("s1", StreamLabelType::Subtitle),
    ];
    let nums =
        assign_stream_numbers(&infos, &HashMap::new()).expect("numbering space not exhausted");
    // Audio: 1, 2; Subtitle: 1, 2 — each counter resets at 1 per type.
    assert_eq!(nums[0], 1); // audio 1
    assert_eq!(nums[1], 1); // subtitle 1
    assert_eq!(nums[2], 2); // audio 2
    assert_eq!(nums[3], 2); // subtitle 2
}

/// Spec: a map value of 0 is unmatchable (apply_labels is 1-based), so
/// assign_stream_numbers must treat it as unmapped and synthesize a
/// real 1-based number rather than emit an orphan 0.
#[test]
fn map_zero_stream_num_is_synthesized_not_emitted() {
    let mut map = HashMap::new();
    map.insert("a0".to_string(), 0u16); // 0 must not be treated as a claim
    let infos = vec![info("a0", StreamLabelType::Audio)];
    let nums = assign_stream_numbers(&infos, &map).expect("numbering space not exhausted");
    // 0 is treated as unmapped → the fallback counter assigns 1.
    assert_eq!(nums[0], 1);
}

/// A stream genuinely mapped to 1 plus another stream whose map value is 0
/// must NOT both land on 1: the 0-stream is synthesized past the claimed 1.
#[test]
fn map_zero_does_not_collide_with_a_real_stream_one() {
    let mut map = HashMap::new();
    map.insert("real".to_string(), 1u16);
    map.insert("bad".to_string(), 0u16);
    let infos = vec![
        info("real", StreamLabelType::Audio),
        info("bad", StreamLabelType::Audio),
    ];
    let nums = assign_stream_numbers(&infos, &map).expect("numbering space not exhausted");
    assert_eq!(nums[0], 1); // the genuinely-mapped stream keeps 1
    assert_eq!(nums[1], 2); // the 0-stream is synthesized to the next free slot
}

/// Spec: collision-avoidance works across audio AND subtitle independently.
/// Subtitle map claiming #2 must not affect audio fallback counter.
/// Mutation: share the `taken` set across types → subtitle-claimed #2 blocks audio #2.
#[test]
fn taken_sets_are_per_type_not_global() {
    // Audio: a0 unmapped. Subtitle: s0 mapped to 2.
    let mut map = HashMap::new();
    map.insert("s0".to_string(), 2u16);
    let infos = vec![
        info("a0", StreamLabelType::Audio),    // fallback
        info("s0", StreamLabelType::Subtitle), // mapped → 2
    ];
    let nums = assign_stream_numbers(&infos, &map).expect("numbering space not exhausted");
    // Audio fallback for a0 → 1 (subtitle's taken-2 doesn't block it).
    assert_eq!(nums[0], 1);
    assert_eq!(nums[1], 2);
}

// Crafted input drives the fallback counter to the top of the u16 space; must TERMINATE
// (not hang) and fail closed with None. Run on a worker thread with a deadline.
#[test]
fn exhausted_numbering_terminates_instead_of_looping() {
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        // One mapped audio stream claims the last number in the space.
        let mut map = HashMap::new();
        map.insert("claims_max".to_string(), u16::MAX);
        let mut infos = vec![info("claims_max", StreamLabelType::Audio)];
        // Enough unmapped audio streams to walk the counter to the top.
        for i in 0..=(u16::MAX as u32) {
            infos.push(info(&format!("u{i}"), StreamLabelType::Audio));
        }
        let _ = tx.send(assign_stream_numbers(&infos, &map));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(20)) {
        Ok(result) => {
            worker.join().expect("worker panicked");
            assert!(
                result.is_none(),
                "an exhausted 1-based u16 numbering space must fail the parse, \
                     not emit colliding or wrapped stream numbers"
            );
        }
        Err(_) => panic!(
            "assign_stream_numbers did not terminate within 20s — \
                 non-terminating skip loop on crafted stream_map"
        ),
    }
}

/// The whole 1-based u16 space must remain usable: 65535 unmapped audio streams get 65535
/// distinct numbers with no panic and no wrap.
#[test]
fn full_u16_numbering_space_is_usable_and_unique() {
    let infos: Vec<StreamInfo> = (0..65_535u32)
        .map(|i| info(&format!("a{i}"), StreamLabelType::Audio))
        .collect();
    let nums = assign_stream_numbers(&infos, &HashMap::new()).expect("space is not exhausted");
    assert_eq!(nums.len(), 65_535);
    assert_eq!(nums[0], 1);
    assert_eq!(nums[65_534], 65_535);
    let mut sorted = nums.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 65_535, "stream numbers must all be distinct");
}

/// Spec: a partially-mapped playlist with many claimed numbers must still
/// synthesize past every claim without panicking or colliding.
/// Mutation: drop the skip loop → the fallback reuses a claimed number.
#[test]
fn fallback_skips_a_dense_block_of_claimed_numbers() {
    // 500 claimed numbers force the fallback counter past a dense block.
    let mut map = HashMap::new();
    for n in 1u16..=500 {
        map.insert(format!("taken_{}", n), n);
    }
    // Add 500 infos that are all mapped, plus 1 unmapped.
    let mut infos: Vec<StreamInfo> = (1u16..=500)
        .map(|n| StreamInfo {
            id: format!("taken_{}", n),
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            variant: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
        })
        .collect();
    infos.push(StreamInfo {
        id: "unmapped".into(),
        stream_type: StreamLabelType::Audio,
        language: "eng".into(),
        variant: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
    });
    // This must not panic.
    let nums = assign_stream_numbers(&infos, &map).expect("numbering space not exhausted");
    assert_eq!(nums.len(), 501);
    // The unmapped entry skips every claimed number: exactly the next free one.
    assert_eq!(nums[500], 501);
}

/// Spec: parse_stream_infos extracts COMMENTARY purpose from the Content element.
/// Mutation: change equality check from `eq_ignore_ascii_case("COMMENTARY")` →
/// only exact uppercase match → lowercase "commentary" fails.
#[test]
fn parse_stream_infos_commentary_case_insensitive() {
    let xml = r#"<root>
          <AudioStreamInfos>
            <ID>a1</ID>
            <LangInfoID>eng</LangInfoID>
            <Content>commentary</Content>
            <Qualifier></Qualifier>
          </AudioStreamInfos>
        </root>"#;
    let infos = parse_stream_infos(xml);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].purpose, LabelPurpose::Commentary);
}

/// Spec: LangInfoID with underscore splits into language + variant.
/// e.g. "por_BP" → language="por", variant="BP".
/// Mutation: don't split on underscore → full "por_BP" used as language code.
#[test]
fn parse_stream_infos_lang_variant_split() {
    let xml = r#"<root>
          <AudioStreamInfos>
            <ID>a1</ID>
            <LangInfoID>por_BP</LangInfoID>
            <Content>Normal</Content>
            <Qualifier></Qualifier>
          </AudioStreamInfos>
        </root>"#;
    let infos = parse_stream_infos(xml);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].language, "por");
    assert_eq!(infos[0].variant, "BP");
}

/// Spec: Qualifier=SDH maps to LabelQualifier::Sdh.
/// Mutation: change match arm from "SDH" to "Sdh" → no case-insensitive match.
#[test]
fn parse_stream_infos_qualifier_sdh_case_insensitive() {
    let xml = r#"<root>
          <SubtitleStreamInfos>
            <ID>s1</ID>
            <LangInfoID>eng</LangInfoID>
            <Content>Normal</Content>
            <Qualifier>sdh</Qualifier>
          </SubtitleStreamInfos>
        </root>"#;
    let infos = parse_stream_infos(xml);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].qualifier, LabelQualifier::Sdh);
}

/// Spec: Qualifier=DS maps to LabelQualifier::DescriptiveService.
/// Mutation: remove "DS" arm → DescriptiveService never returned.
#[test]
fn parse_stream_infos_qualifier_descriptive_service() {
    let xml = r#"<root>
          <AudioStreamInfos>
            <ID>a1</ID>
            <LangInfoID>eng</LangInfoID>
            <Content>Normal</Content>
            <Qualifier>DS</Qualifier>
          </AudioStreamInfos>
        </root>"#;
    let infos = parse_stream_infos(xml);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].qualifier, LabelQualifier::DescriptiveService);
}

/// Spec: playbackconfig.xml zero StreamID is filtered.
/// Mutation: remove `stream_num != 0` guard → 0 stored in map.
#[test]
fn parse_playback_config_zero_stream_id_skipped() {
    let xml = r#"<root>
          <AudioStreams>
            <StreamID>0</StreamID>
            <StreamInfo_ID>bad_id</StreamInfo_ID>
          </AudioStreams>
          <AudioStreams>
            <StreamID>2</StreamID>
            <StreamInfo_ID>good_id</StreamInfo_ID>
          </AudioStreams>
        </root>"#;
    let mut map = HashMap::new();
    parse_playback_config(xml, &mut map);
    assert!(!map.contains_key("bad_id"), "zero StreamID must be skipped");
    assert_eq!(map.get("good_id").copied(), Some(2));
}

/// Spec: SubtitlesStreams entries are parsed by parse_playback_config.
/// Mutation: only iterate AudioStreams → subtitle mappings dropped.
#[test]
fn parse_playback_config_subtitle_streams_parsed() {
    let xml = r#"<root>
          <SubtitlesStreams>
            <StreamID>3</StreamID>
            <StreamInfo_ID>sub1</StreamInfo_ID>
          </SubtitlesStreams>
        </root>"#;
    let mut map = HashMap::new();
    parse_playback_config(xml, &mut map);
    assert_eq!(map.get("sub1").copied(), Some(3));
}

/// Spec: `LangInfoID` values are lowercased so they match
/// `apply_labels`' lookup (an uppercase "ENG" must parse as "eng").
#[test]
fn parse_stream_infos_language_lowercased() {
    // LangInfoID values must be lowercased so they match apply_labels' lookup.
    let xml = r#"<root>
          <AudioStreamInfos>
            <ID>a1</ID>
            <LangInfoID>ENG</LangInfoID>
            <Content>Normal</Content>
            <Qualifier></Qualifier>
          </AudioStreamInfos>
        </root>"#;
    let infos = parse_stream_infos(xml);
    assert_eq!(infos[0].language, "eng");
}
