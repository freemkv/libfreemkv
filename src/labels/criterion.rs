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
pub fn detect(udf: &UdfFs) -> bool {
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
    // Numbers already claimed by the map, per type.
    let mut taken_audio: Vec<u16> = Vec::new();
    let mut taken_sub: Vec<u16> = Vec::new();
    for info in infos {
        if let Some(&n) = stream_map.get(&info.id) {
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
            Some(n) => n,
            None => {
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
}
