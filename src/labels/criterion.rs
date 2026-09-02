//! Criterion Collection — `streamproperties.xml` + `playbackconfig.xml`
//!
//! Clean structured XML with Content/Qualifier per stream and
//! stream number mapping via playbackconfig.
//!
//! When `playbackconfig.xml` is absent or maps only some streams,
//! unmapped streams get 1-based-per-type stream numbers synthesized in
//! `streamproperties.xml` order, skipping any number already claimed by
//! the map so synthesized and mapped numbers never collide. See
//! [`assign_stream_numbers`].

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashMap;

/// Cheap signature check: a Criterion disc ships `streamproperties.xml`
/// inside a `/BDMV/JAR/*` archive.
pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "streamproperties.xml")
}

/// Parse `streamproperties.xml` (+ optional `playbackconfig.xml`) into
/// per-stream labels. Returns `None` if `streamproperties.xml` is
/// absent/unparseable or yields no streams. Stream numbering follows
/// the contract documented at module level (see
/// [`assign_stream_numbers`]).
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let sp_data = super::read_jar_file(reader, udf, "streamproperties.xml")?;
    let sp_text = std::str::from_utf8(&sp_data).ok()?;

    let stream_infos = parse_stream_infos(sp_text);
    if stream_infos.is_empty() {
        return None;
    }

    // Stream number mapping from playbackconfig.xml
    let mut stream_map: HashMap<String, u16> = HashMap::new();
    if let Some(pc_data) = super::read_jar_file(reader, udf, "playbackconfig.xml")
        && let Ok(pc_text) = std::str::from_utf8(&pc_data)
    {
        parse_playback_config(pc_text, &mut stream_map);
    }

    let stream_nums = assign_stream_numbers(&stream_infos, &stream_map)?;

    let mut labels = Vec::new();
    for (info, &stream_num) in stream_infos.iter().zip(stream_nums.iter()) {
        labels.push(StreamLabel {
            stream_id: None,
            stream_number: stream_num,
            stream_type: info.stream_type,
            language: info.language.clone(),
            name: String::new(),
            purpose: info.purpose,
            qualifier: info.qualifier,
            codec_hint: String::new(),
            variant: info.variant.clone(),
        });
    }

    if labels.is_empty() {
        return None;
    }
    // High confidence: streamproperties.xml is fully structured.
    Some(ParseResult::high(labels))
}

// Assign a 1-based stream number per StreamInfo. Map-assigned numbers win;
// unmapped streams get the next free per-type number, skipping map claims so
// the two domains never collide. See docs/criterion.md — assign_stream_numbers.
fn assign_stream_numbers(
    infos: &[StreamInfo],
    stream_map: &HashMap<String, u16>,
) -> Option<Vec<u16>> {
    /// One past the last assignable stream number, as a `u32` so the
    /// counters can step off the end of the `u16` domain without wrapping.
    const NUMBER_SPACE_END: u32 = u16::MAX as u32 + 1;

    // Numbers already claimed by the map, per type. A map value of 0 is NOT a
    // claim (apply_labels binds 1-based numbers, so 0 is unmatchable); treat it
    // as unmapped so the stream gets a real number instead of colliding with 1.
    let mut taken_audio: Vec<u16> = Vec::new();
    let mut taken_sub: Vec<u16> = Vec::new();
    for info in infos {
        if let Some(&n) = stream_map.get(&info.id) {
            if n == 0 {
                continue;
            }
            match info.stream_type {
                StreamLabelType::Audio => taken_audio.push(n),
                StreamLabelType::Subtitle => taken_sub.push(n),
            }
        }
    }

    let mut audio_idx: u32 = 1;
    let mut sub_idx: u32 = 1;
    let mut out = Vec::with_capacity(infos.len());
    for info in infos {
        let n = match stream_map.get(&info.id).copied() {
            Some(n) if n != 0 => n,
            _ => {
                let (idx, taken) = match info.stream_type {
                    StreamLabelType::Audio => (&mut audio_idx, &taken_audio),
                    StreamLabelType::Subtitle => (&mut sub_idx, &taken_sub),
                };
                // Advance past any number already claimed via the map. The
                // counter strictly increases and NUMBER_SPACE_END is fixed, so
                // this terminates in at most 65535 steps for any input.
                while *idx < NUMBER_SPACE_END && taken.contains(&(*idx as u16)) {
                    *idx += 1;
                }
                if *idx >= NUMBER_SPACE_END {
                    // Numbering space exhausted. Emitting anything here would
                    // either wrap to 0 (unmatchable) or duplicate a number
                    // already bound to a different stream, so the parse fails.
                    tracing::warn!(
                        streams = infos.len(),
                        "criterion: 1-based u16 stream-number space exhausted; \
                         refusing to synthesize a colliding stream number"
                    );
                    return None;
                }
                let n = *idx as u16;
                *idx += 1;
                n
            }
        };
        out.push(n);
    }
    Some(out)
}

struct StreamInfo {
    id: String,
    stream_type: StreamLabelType,
    language: String,
    variant: String,
    purpose: LabelPurpose,
    qualifier: LabelQualifier,
}

fn parse_stream_infos(text: &str) -> Vec<StreamInfo> {
    let mut infos = Vec::new();

    for (tag_name, stream_type) in [
        ("AudioStreamInfos", StreamLabelType::Audio),
        ("SubtitleStreamInfos", StreamLabelType::Subtitle),
    ] {
        let mut from = 0;
        while let Some((start, end)) = xml::find_element(text, tag_name, from) {
            let block = &text[start..end];
            let id = xml::text(block, "ID").unwrap_or_default();
            let lang_id = xml::text(block, "LangInfoID").unwrap_or_default();
            let content = xml::text(block, "Content").unwrap_or_default();
            let qualifier_str = xml::text(block, "Qualifier").unwrap_or_default();

            let (language, variant) = if lang_id.contains('_') {
                let parts: Vec<&str> = lang_id.splitn(2, '_').collect();
                (parts[0].to_lowercase(), parts[1].to_string())
            } else {
                (lang_id.to_lowercase(), String::new())
            };

            let purpose = if content.eq_ignore_ascii_case("COMMENTARY") {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };

            let qualifier = match qualifier_str.to_ascii_uppercase().as_str() {
                "SDH" => LabelQualifier::Sdh,
                "DS" => LabelQualifier::DescriptiveService,
                _ => LabelQualifier::None,
            };

            infos.push(StreamInfo {
                id,
                stream_type,
                language,
                variant,
                purpose,
                qualifier,
            });
            from = end;
        }
    }
    infos
}

fn parse_playback_config(text: &str, map: &mut HashMap<String, u16>) {
    for tag_name in ["AudioStreams", "SubtitlesStreams"] {
        let mut from = 0;
        while let Some((start, end)) = xml::find_element(text, tag_name, from) {
            let block = &text[start..end];
            if let (Some(stream_id_str), Some(info_id)) = (
                xml::text(block, "StreamID"),
                xml::text(block, "StreamInfo_ID"),
            ) && let Ok(stream_num) = stream_id_str.parse::<u16>()
            {
                // Stream numbers are 1-based per the apply_labels
                // contract; a mapped 0 is unmatchable and silently
                // drops the label. Skip it rather than store it.
                if stream_num != 0 {
                    map.insert(info_id, stream_num);
                }
            }
            from = end;
        }
    }
}

#[cfg(test)]
mod tests {
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

    // Immunity pin: an unusable/malformed element still occupies its slot
    // (never dropped) and a close-less element can only shorten the list,
    // never extend it. See docs/criterion.md — immunity pin, section-boundary half.
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

    // Crafted input drives the fallback counter to the top of the u16 space;
    // must TERMINATE (not hang) and fail closed with None. Run on a worker
    // thread with a deadline. See docs/criterion.md — exhausted_numbering_terminates_instead_of_looping.
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

    /// The whole 1-based u16 space must remain usable: 65535 unmapped audio
    /// streams get 65535 distinct numbers with no panic and no wrap. See
    /// docs/criterion.md — full_u16_numbering_space_is_usable_and_unique.
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
        // Force the counter past u16::MAX by pre-taking all values 1..=u16::MAX.
        // Doing that for real would be slow; instead inject u16::MAX into taken.
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
        // The last (unmapped) entry's number must be > 500 (skipped all taken).
        assert!(nums[500] > 500);
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
}
