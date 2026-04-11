//! Pixelogic — `bluray_project.bin`
//!
//! Binary file with embedded UTF-8 token strings in STN order per
//! playlist section. Most common format (5/10 test discs).
//!
//! Token format: `{lang}_{codec?}_{purpose?}_{region?}_`

use crate::sector::SectorReader;
use crate::udf::UdfFs;
use super::{StreamLabel, StreamLabelType, LabelPurpose, LabelQualifier, vocab};

/// Known audio codec tokens
const AUDIO_CODECS: &[&str] = &["MLP", "AC3", "DTS", "DDL", "WAV", "AC"];
/// Known region tokens
const REGIONS: &[&str] = &["US", "UK", "CF", "PF", "CS", "LS", "BP", "PP", "SM", "TM", "CAN", "DUM", "FLE"];

pub fn detect(udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "bluray_project.bin")
}

pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let data = super::read_jar_file(reader, udf, "bluray_project.bin")?;
    let strings = extract_strings(&data);

    let mut labels = Vec::new();
    let mut in_feature = false;
    let mut audio_num: u16 = 0;
    let mut sub_num: u16 = 0;

    for s in &strings {
        // Detect feature section start
        if s.starts_with("FPL_") || s.starts_with("SEG_MainFeature") {
            if in_feature { break; }
            in_feature = true;
            audio_num = 0;
            sub_num = 0;
            continue;
        }

        // Detect section end
        if in_feature && (s.starts_with("SEG_") || s.starts_with("SF_") || s.starts_with("FPL_")) {
            break;
        }

        if !in_feature { continue; }

        if let Some(label) = parse_token(s) {
            match label.stream_type {
                StreamLabelType::Audio => {
                    audio_num += 1;
                    labels.push(StreamLabel { stream_number: audio_num, ..label });
                }
                StreamLabelType::Subtitle => {
                    sub_num += 1;
                    labels.push(StreamLabel { stream_number: sub_num, ..label });
                }
            }
        }
    }

    if labels.is_empty() { return None; }
    Some(labels)
}

fn parse_token(s: &str) -> Option<StreamLabel> {
    let clean = s.trim().trim_start_matches('\t').trim_end_matches('_');
    let parts: Vec<&str> = clean.split('_').collect();
    if parts.len() < 2 { return None; }

    let lang = parts[0];
    if lang.len() != 3 || !lang.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }

    let mut codec = String::new();
    let mut purpose = LabelPurpose::Normal;
    let mut qualifier = LabelQualifier::None;
    let mut variant = String::new();
    let mut is_subtitle = false;
    let mut is_audio = false;

    for &part in &parts[1..] {
        if part.is_empty() { continue; }
        if AUDIO_CODECS.contains(&part) { codec = vocab::codec(part).to_string(); is_audio = true; }
        else if part == "ADES" { purpose = LabelPurpose::Descriptive; is_audio = true; }
        else if part == "ACOM" { purpose = LabelPurpose::Commentary; is_audio = true; }
        else if part == "ADLG" { is_audio = true; }
        else if part == "ATRI" { is_audio = true; }
        else if part == "SDH" { qualifier = LabelQualifier::Sdh; is_subtitle = true; }
        else if part == "SDLG" { is_subtitle = true; }
        else if part == "SCOM" { purpose = LabelPurpose::Commentary; is_subtitle = true; }
        else if part == "STRI" { is_subtitle = true; }
        else if part == "TXT" { is_subtitle = true; }
        else if part == "FOR" { qualifier = LabelQualifier::Forced; }
        else if REGIONS.contains(&part) { variant = part.to_string(); }
        else if part.starts_with("PGStream") { is_subtitle = true; }
        else { return None; }
    }

    if !is_audio && !is_subtitle { return None; }

    let stream_type = if is_subtitle { StreamLabelType::Subtitle } else { StreamLabelType::Audio };

    Some(StreamLabel {
        stream_number: 0,
        stream_type,
        language: lang.to_string(),
        name: String::new(),
        purpose,
        qualifier,
        codec_hint: codec,
        variant,
    })
}

fn extract_strings(data: &[u8]) -> Vec<String> {
    let mut strings = Vec::new();
    let mut current = String::new();

    for &b in data {
        if b >= 0x20 && b < 0x7f {
            current.push(b as char);
        } else {
            if current.len() > 3 {
                strings.push(current.clone());
            }
            current.clear();
        }
    }
    if current.len() > 3 {
        strings.push(current);
    }
    strings
}
