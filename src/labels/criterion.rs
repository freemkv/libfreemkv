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
use std::collections::{HashMap, HashSet};

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
        if stream_num == super::NO_STN_SLOT {
            continue; // dropped: contradictory stream-number map
        }
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

// Assign a 1-based stream number per StreamInfo. Map-assigned numbers win; unmapped streams get
// the next free per-type number, skipping map claims so the two domains never collide.
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
    let mut taken_audio: HashSet<u16> = HashSet::new();
    let mut taken_sub: HashSet<u16> = HashSet::new();
    for info in infos {
        if let Some(&n) = stream_map.get(&info.id) {
            if n == 0 {
                continue;
            }
            match info.stream_type {
                StreamLabelType::Audio => taken_audio.insert(n),
                StreamLabelType::Subtitle => taken_sub.insert(n),
            };
        }
    }

    let mut audio_idx: u32 = 1;
    let mut sub_idx: u32 = 1;
    // Numbers already EMITTED per type (HashSet: O(1) skip across the full
    // 65535-stream space). A duplicate map claim yields NO_STN_SLOT (dropped).
    let mut used_audio: HashSet<u16> = HashSet::new();
    let mut used_sub: HashSet<u16> = HashSet::new();
    let mut out = Vec::with_capacity(infos.len());
    for info in infos {
        let (idx, taken, used) = match info.stream_type {
            StreamLabelType::Audio => (&mut audio_idx, &taken_audio, &mut used_audio),
            StreamLabelType::Subtitle => (&mut sub_idx, &taken_sub, &mut used_sub),
        };
        // A map value of 0 is unmatchable (apply_labels is 1-based); treat it as
        // unmapped. A mapped number already claimed by an earlier stream of the
        // same type is contradictory and is dropped (NO_STN_SLOT).
        let mapped = stream_map.get(&info.id).copied().filter(|&n| n != 0);
        let n = match mapped {
            Some(n) if !used.contains(&n) => n,
            // Contradictory map (a second stream claims a used number): drop it.
            Some(_) => {
                out.push(super::NO_STN_SLOT);
                continue;
            }
            None => {
                // Advance past any number already claimed via the map OR already
                // emitted (dedup). The counter strictly increases and NUMBER_SPACE_END
                // is fixed, so this terminates in at most 65535 steps for any input.
                while *idx < NUMBER_SPACE_END
                    && (taken.contains(&(*idx as u16)) || used.contains(&(*idx as u16)))
                {
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
        used.insert(n);
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
#[path = "criterion_tests.rs"]
mod tests;
