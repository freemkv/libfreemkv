//! Criterion Collection — `streamproperties.xml` + `playbackconfig.xml`
//!
//! Clean structured XML with Content/Qualifier per stream and
//! stream number mapping via playbackconfig.

use crate::sector::SectorReader;
use crate::udf::UdfFs;
use super::{StreamLabel, StreamLabelType, LabelPurpose, LabelQualifier};
use std::collections::HashMap;

pub fn detect(udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "streamproperties.xml")
}

pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let sp_data = super::read_jar_file(reader, udf, "streamproperties.xml")?;
    let sp_text = std::str::from_utf8(&sp_data).ok()?;

    let stream_infos = parse_stream_infos(sp_text);
    if stream_infos.is_empty() { return None; }

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
        let stream_num = stream_map.get(&info.id).copied().unwrap_or_else(|| {
            match info.stream_type {
                StreamLabelType::Audio => { let n = audio_idx; audio_idx += 1; n }
                StreamLabelType::Subtitle => { let n = sub_idx; sub_idx += 1; n }
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

    if labels.is_empty() { return None; }
    Some(labels)
}

struct StreamInfo {
    id: String,
    stream_type: StreamLabelType,
    language: String,
    variant: String,
    purpose: LabelPurpose,
    qualifier: LabelQualifier,
}

fn parse_stream_infos(xml: &str) -> Vec<StreamInfo> {
    let mut infos = Vec::new();
    let mut pos = 0;

    while pos < xml.len() {
        let (tag, stream_type) = if let Some(p) = xml[pos..].find("<AudioStreamInfos>") {
            (p + pos, StreamLabelType::Audio)
        } else if let Some(p) = xml[pos..].find("<SubtitleStreamInfos>") {
            (p + pos, StreamLabelType::Subtitle)
        } else {
            break;
        };

        let end_tag = match stream_type {
            StreamLabelType::Audio => "</AudioStreamInfos>",
            StreamLabelType::Subtitle => "</SubtitleStreamInfos>",
        };

        let block_end = match xml[tag..].find(end_tag) {
            Some(p) => tag + p + end_tag.len(),
            None => break,
        };

        let block = &xml[tag..block_end];
        let id = extract_tag(block, "ID").unwrap_or_default();
        let lang_id = extract_tag(block, "LangInfoID").unwrap_or_default();
        let content = extract_tag(block, "Content").unwrap_or_default();
        let qualifier_str = extract_tag(block, "Qualifier").unwrap_or_default();

        let (language, variant) = if lang_id.contains('_') {
            let parts: Vec<&str> = lang_id.splitn(2, '_').collect();
            (parts[0].to_lowercase(), parts[1].to_string())
        } else {
            (lang_id.to_lowercase(), String::new())
        };

        let purpose = match content.as_str() {
            "COMMENTARY" => LabelPurpose::Commentary,
            _ => LabelPurpose::Normal,
        };

        let qualifier = match qualifier_str.as_str() {
            "SDH" => LabelQualifier::Sdh,
            "DS" => LabelQualifier::DescriptiveService,
            _ => LabelQualifier::None,
        };

        infos.push(StreamInfo { id, stream_type, language, variant, purpose, qualifier });
        pos = block_end;
    }
    infos
}

fn parse_playback_config(xml: &str, map: &mut HashMap<String, u16>) {
    let mut pos = 0;
    while pos < xml.len() {
        let tag_start = if let Some(p) = xml[pos..].find("<AudioStreams>") {
            Some(p + pos)
        } else if let Some(p) = xml[pos..].find("<SubtitlesStreams>") {
            Some(p + pos)
        } else {
            None
        };

        let tag_start = match tag_start {
            Some(p) => p,
            None => break,
        };

        let block_end = xml[tag_start..].find("</AudioStreams>")
            .or_else(|| xml[tag_start..].find("</SubtitlesStreams>"))
            .map(|p| tag_start + p + 20)
            .unwrap_or(xml.len());

        let block = &xml[tag_start..block_end];

        if let (Some(stream_id_str), Some(info_id)) = (extract_tag(block, "StreamID"), extract_tag(block, "StreamInfo_ID")) {
            if let Ok(stream_num) = stream_id_str.parse::<u16>() {
                map.insert(info_id, stream_num);
            }
        }

        pos = block_end;
    }
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}
