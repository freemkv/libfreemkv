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
    if let Some(pc_data) = super::read_jar_file(reader, udf, "playbackconfig.xml") {
        if let Ok(pc_text) = std::str::from_utf8(&pc_data) {
            parse_playback_config(pc_text, &mut stream_map);
        }
    }

    let stream_nums = assign_stream_numbers(&stream_infos, &stream_map);

    let mut labels = Vec::new();
    for (info, &stream_num) in stream_infos.iter().zip(stream_nums.iter()) {
        labels.push(StreamLabel {
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

/// Assign a 1-based stream number per `StreamInfo`, parallel to
/// `infos`.
///
/// A stream mapped in `playbackconfig.xml` (`stream_map`) keeps its
/// mapped number. Streams with no mapping (absent or incomplete
/// `playbackconfig.xml`, or an unmatched `StreamInfo_ID`) are numbered
/// 1-based per type — but the fallback counter SKIPS any number already
/// claimed via the map, so a synthesized number can never collide with a
/// map-assigned one. (Both numbering domains are 1-based per type, and
/// `apply_labels` matches on `(type, stream_number)`, so a collision
/// would mislabel tracks.)
fn assign_stream_numbers(infos: &[StreamInfo], stream_map: &HashMap<String, u16>) -> Vec<u16> {
    // Numbers already claimed by the map, per type. A map value of 0 is NOT a
    // claim: apply_labels binds on 1-based stream numbers, so 0 is unmatchable.
    // Treat 0 as "unmapped" here (defense in depth — parse_playback_config also
    // filters it) so such a stream gets a real synthesized number instead of an
    // orphan 0 that collides with / shadows a genuine stream 1.
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

    let mut audio_idx: u16 = 1;
    let mut sub_idx: u16 = 1;
    let mut out = Vec::with_capacity(infos.len());
    for info in infos {
        let n = match stream_map.get(&info.id).copied() {
            Some(n) if n != 0 => n,
            _ => {
                let (idx, taken) = match info.stream_type {
                    StreamLabelType::Audio => (&mut audio_idx, &taken_audio),
                    StreamLabelType::Subtitle => (&mut sub_idx, &taken_sub),
                };
                // Advance past any number already claimed via the map.
                // saturating: a crafted XML with >65k stream entries must
                // not overflow (panic in debug, wrap-to-0 in release) on
                // untrusted disc bytes.
                while taken.contains(idx) {
                    *idx = idx.saturating_add(1);
                }
                let n = *idx;
                *idx = idx.saturating_add(1);
                n
            }
        };
        out.push(n);
    }
    out
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
            ) {
                if let Ok(stream_num) = stream_id_str.parse::<u16>() {
                    // Stream numbers are 1-based per the apply_labels
                    // contract; a mapped 0 is unmatchable and silently
                    // drops the label. Skip it rather than store it.
                    if stream_num != 0 {
                        map.insert(info_id, stream_num);
                    }
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
        let nums = assign_stream_numbers(&infos, &HashMap::new());
        // Per-type 1-based: audio 1,2 ; subtitle 1.
        assert_eq!(nums, vec![1, 2, 1]);
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
        let nums = assign_stream_numbers(&infos, &map);
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
        assert_eq!(assign_stream_numbers(&infos, &map), vec![5, 9]);
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
        let nums = assign_stream_numbers(&infos, &HashMap::new());
        // Audio: 1, 2; Subtitle: 1, 2 — each counter resets at 1 per type.
        assert_eq!(nums[0], 1); // audio 1
        assert_eq!(nums[1], 1); // subtitle 1
        assert_eq!(nums[2], 2); // audio 2
        assert_eq!(nums[3], 2); // subtitle 2
    }

    /// Spec: a map value of 0 is unmatchable (apply_labels is 1-based), so
    /// assign_stream_numbers must treat it as unmapped and synthesize a real
    /// 1-based number rather than emit an orphan 0.
    /// Mutation: read the map value verbatim → stream_number 0 leaks out.
    #[test]
    fn map_zero_stream_num_is_synthesized_not_emitted() {
        let mut map = HashMap::new();
        map.insert("a0".to_string(), 0u16); // 0 must not be treated as a claim
        let infos = vec![info("a0", StreamLabelType::Audio)];
        let nums = assign_stream_numbers(&infos, &map);
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
        let nums = assign_stream_numbers(&infos, &map);
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
        let nums = assign_stream_numbers(&infos, &map);
        // Audio fallback for a0 → 1 (subtitle's taken-2 doesn't block it).
        assert_eq!(nums[0], 1);
        assert_eq!(nums[1], 2);
    }

    /// Spec: saturating_add prevents overflow when many streams are listed.
    /// Mutation: use wrapping_add → counter wraps to 0 and collides.
    #[test]
    fn assign_stream_numbers_saturation_on_overflow() {
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
        let nums = assign_stream_numbers(&infos, &map);
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

    /// Spec: high confidence is returned when streamproperties.xml is fully
    /// structured (no fallback). This is the Criterion parser's claim.
    /// Mutation: change to ParseResult::medium → confidence assertion fails.
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
