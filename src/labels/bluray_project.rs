//! Parser for `bluray_project.bin` — Pixelogic binary format.
//!
//! Found at: `BDMV/JAR/*/bluray_project.bin`
//!
//! Binary file with embedded UTF-8 strings. Stream tokens appear in STN order
//! under playlist sections. Tokens follow the format:
//!   {lang}_{purpose?}_{codec?}_{region?}_
//!
//! Examples: eng_MLP_, eng_US_ADES_, eng_SDH_, fra_TXT_FOR_

use crate::drive::DriveSession;
use crate::udf::{UdfFs, DirEntry};
use super::{StreamLabel, StreamLabelType, LabelPurpose, LabelQualifier};

/// Known audio codec tokens
const AUDIO_CODECS: &[&str] = &["MLP", "AC3", "DTS", "DDL", "WAV"];
/// Known audio purpose tokens
const AUDIO_PURPOSES: &[&str] = &["ADLG", "ACOM", "ADES", "ATRI"];
/// Known subtitle types
const SUB_TYPES: &[&str] = &["SDH", "SDLG", "SCOM", "STRI"];
/// Known region tokens
const REGIONS: &[&str] = &["US", "UK", "CF", "PF", "CS", "LS", "BP", "PP", "SM", "TM", "CAN", "DUM", "FLE"];

pub fn parse(session: &mut DriveSession, udf: &UdfFs) -> Option<Vec<StreamLabel>> {
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    let data = find_and_read(session, udf, jar_dir, "bluray_project.bin")?;

    // Extract all strings from the binary
    let strings = extract_strings(&data);

    // Find the main feature section — look for "FPL_MainFeature" or "SEG_MainFeature"
    // then collect audio and subtitle tokens that follow
    let mut labels = Vec::new();
    let mut in_feature = false;
    let mut audio_num: u16 = 0;
    let mut sub_num: u16 = 0;

    for s in &strings {
        // Detect feature section start
        if s.starts_with("FPL_") || s.starts_with("SEG_MainFeature") {
            if in_feature {
                // Already found one — this is a second copy, skip
                break;
            }
            in_feature = true;
            audio_num = 0;
            sub_num = 0;
            continue;
        }

        // Detect section end (next playlist starts)
        if in_feature && (s.starts_with("SEG_") || s.starts_with("SF_") || s.starts_with("FPL_")) {
            break;
        }

        if !in_feature { continue; }

        // Try to parse as a stream token
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

/// Parse a token string like "eng_MLP_" or "eng_US_ADES_" into a StreamLabel.
fn parse_token(s: &str) -> Option<StreamLabel> {
    let clean = s.trim_end_matches('_');
    let parts: Vec<&str> = clean.split('_').collect();
    if parts.len() < 2 { return None; }

    // First part must be 3-letter lowercase language code
    let lang = parts[0];
    if lang.len() != 3 || !lang.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }

    let mut codec = String::new();
    let mut purpose = LabelPurpose::Normal;
    let mut qualifier = LabelQualifier::None;
    let mut region = String::new();
    let mut is_subtitle = false;
    let mut is_audio = false;

    for &part in &parts[1..] {
        if part.is_empty() { continue; }
        if AUDIO_CODECS.contains(&part) { codec = part.to_string(); is_audio = true; }
        else if part == "ADES" { purpose = LabelPurpose::Descriptive; is_audio = true; }
        else if part == "ACOM" { purpose = LabelPurpose::Commentary; is_audio = true; }
        else if part == "ADLG" { purpose = LabelPurpose::Normal; is_audio = true; }
        else if part == "ATRI" { purpose = LabelPurpose::Normal; is_audio = true; }
        else if part == "SDH" { qualifier = LabelQualifier::Sdh; is_subtitle = true; }
        else if part == "SDLG" { is_subtitle = true; }
        else if part == "SCOM" { purpose = LabelPurpose::Commentary; is_subtitle = true; }
        else if part == "STRI" { is_subtitle = true; }
        else if part == "TXT" { is_subtitle = true; }
        else if part == "FOR" { qualifier = LabelQualifier::Forced; }
        else if REGIONS.contains(&part) { region = part.to_string(); }
        else if part.starts_with("PGStream") { is_subtitle = true; }
        else { return None; } // Unknown token — not a stream ID
    }

    if !is_audio && !is_subtitle { return None; }

    let stream_type = if is_subtitle { StreamLabelType::Subtitle } else { StreamLabelType::Audio };

    Some(StreamLabel {
        stream_number: 0, // caller sets this
        stream_type,
        language: lang.to_string(),
        name: s.to_string(),
        purpose,
        qualifier,
        codec_hint: codec,
        region,
    })
}

/// Extract all printable strings > 3 chars from binary data.
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

fn find_and_read(session: &mut DriveSession, udf: &UdfFs, parent: &DirEntry, filename: &str) -> Option<Vec<u8>> {
    for entry in &parent.entries {
        if entry.is_dir {
            let sub_path = format!("/BDMV/JAR/{}/{}", entry.name, filename);
            if let Ok(data) = udf.read_file(session, &sub_path) {
                if !data.is_empty() { return Some(data); }
            }
        }
    }
    None
}
