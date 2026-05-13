//! Criterion Collection — `streamproperties.xml` + `playbackconfig.xml`
//!
//! Clean structured XML with Content/Qualifier per stream and
//! stream number mapping via playbackconfig.

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashMap;

pub fn detect(udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "streamproperties.xml")
}

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

    let mut labels = Vec::new();
    let mut audio_idx: u16 = 1;
    let mut sub_idx: u16 = 1;

    for info in &stream_infos {
        let stream_num =
            stream_map
                .get(&info.id)
                .copied()
                .unwrap_or_else(|| match info.stream_type {
                    StreamLabelType::Audio => {
                        let n = audio_idx;
                        audio_idx += 1;
                        n
                    }
                    StreamLabelType::Subtitle => {
                        let n = sub_idx;
                        sub_idx += 1;
                        n
                    }
                });

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
                    map.insert(info_id, stream_num);
                }
            }
            from = end;
        }
    }
}
