//! BD-J JAR parser — extract audio/subtitle track labels from disc menus.
//!
//! Blu-ray discs with BD-J menus store the menu application as Java JAR files
//! in BDMV/JAR/. These contain .class files with string constants that label
//! audio and subtitle tracks.
//!
//! Multiple label formats exist across studios:
//!   - Label format: "eng_MLP_", "fra_AudioStream3" (Warner UHD, e.g. Barbie, Dune)
//!   - TextField format: "TextField,Audio1,English Dolby Atmos,..." (A24/Lionsgate, e.g. Civil War)
//!
//! Labels match to streams by STN index — the Nth audio label in the JAR
//! corresponds to the Nth audio stream in the MPLS STN table. This is defined
//! by the BD-J API where stream numbers = STN indices (1-based).

use std::io::Read;

/// Labels extracted from a BD-J JAR file.
#[derive(Debug, Default)]
pub struct JarLabels {
    /// Audio track labels in STN order
    pub audio: Vec<TrackLabel>,
    /// Subtitle track labels in STN order
    pub subtitle: Vec<TrackLabel>,
    /// Playlist purpose labels (MAIN_FEATURE, FORCED_TRAILER, etc.)
    pub playlists: Vec<String>,
}

/// A parsed track label from the JAR.
#[derive(Debug, Clone)]
pub struct TrackLabel {
    /// Human-readable description (e.g. "English Dolby Atmos", "TrueHD", "Descriptive Audio (US)")
    pub description: String,
    /// ISO 639-2 language code if available (e.g. "eng", "fra")
    pub language: String,
    /// Codec hint if available (e.g. "MLP"=TrueHD, "AC3"=DD, "DTS")
    pub codec_hint: String,
    /// The raw string from the class file
    pub raw: String,
}

/// Extract track labels from a JAR file (raw ZIP bytes).
///
/// Tries multiple format parsers. Returns None if no labels found.
pub fn extract_labels(jar_data: &[u8]) -> Option<JarLabels> {
    let strings = extract_all_jar_strings(jar_data)?;

    // Try each format parser in order
    try_textfield_format(&strings)
        .or_else(|| try_label_format(&strings))
        .or_else(|| try_playlist_only(&strings))
}

// ── Format 1: TextField (A24/Lionsgate) ─────────────────────────────────────
//
// Pattern: "TextField,Audio{N},{description},..."
//          "TextField,Subtitle{N},{description},..."
// Found in: Civil War, and similar A24/Lionsgate discs

fn try_textfield_format(strings: &[String]) -> Option<JarLabels> {
    let mut audio: Vec<(u32, String, String)> = Vec::new(); // (index, description, raw)
    let mut subtitle: Vec<(u32, String, String)> = Vec::new();
    let mut playlists = Vec::new();

    for s in strings {
        if s.starts_with("TextField,Audio") {
            // TextField,Audio{N},{description},...
            let parts: Vec<&str> = s.splitn(4, ',').collect();
            if parts.len() >= 3 {
                let key = parts[1]; // "Audio1", "Audio2", etc.
                let desc = parts[2].to_string();
                if let Some(num_str) = key.strip_prefix("Audio") {
                    if let Ok(idx) = num_str.parse::<u32>() {
                        if !desc.is_empty() {
                            audio.push((idx, desc, s.clone()));
                        }
                    }
                }
            }
        } else if s.starts_with("TextField,Subtitle") {
            let parts: Vec<&str> = s.splitn(4, ',').collect();
            if parts.len() >= 3 {
                let key = parts[1];
                let desc = parts[2].to_string();
                if let Some(num_str) = key.strip_prefix("Subtitle") {
                    if let Ok(idx) = num_str.parse::<u32>() {
                        if !desc.eq_ignore_ascii_case("None") && !desc.is_empty() {
                            subtitle.push((idx, desc, s.clone()));
                        }
                    }
                }
            }
        }

        collect_playlist_label(s, &mut playlists);
    }

    if audio.is_empty() && subtitle.is_empty() {
        return None;
    }

    // Sort by index to ensure STN order
    audio.sort_by_key(|a| a.0);
    subtitle.sort_by_key(|s| s.0);

    Some(JarLabels {
        audio: audio.into_iter().map(|(_, desc, raw)| TrackLabel {
            description: desc, language: String::new(), codec_hint: String::new(), raw,
        }).collect(),
        subtitle: subtitle.into_iter().map(|(_, desc, raw)| TrackLabel {
            description: desc, language: String::new(), codec_hint: String::new(), raw,
        }).collect(),
        playlists,
    })
}

// ── Format 2: Label strings (Warner UHD) ────────────────────────────────────
//
// Pattern: "eng_MLP_", "fra_AudioStream3", "dan_PGStream4"
// Found in: Barbie, Dune Part Two, and similar Warner UHD discs

fn try_label_format(strings: &[String]) -> Option<JarLabels> {
    let mut audio = Vec::new();
    let mut subtitle = Vec::new();
    let mut playlists = Vec::new();

    for s in strings {
        if let Some(label) = parse_label_string(s) {
            if label.is_audio && !audio.iter().any(|a: &TrackLabel| a.raw == label.raw) {
                audio.push(TrackLabel {
                    description: label.description, language: label.language,
                    codec_hint: label.codec_hint, raw: label.raw,
                });
            } else if label.is_subtitle && !subtitle.iter().any(|a: &TrackLabel| a.raw == label.raw) {
                subtitle.push(TrackLabel {
                    description: label.description, language: label.language,
                    codec_hint: label.codec_hint, raw: label.raw,
                });
            }
        }

        collect_playlist_label(s, &mut playlists);
    }

    if audio.is_empty() && subtitle.is_empty() {
        return None;
    }

    Some(JarLabels { audio, subtitle, playlists })
}

struct ParsedLabel {
    description: String,
    language: String,
    codec_hint: String,
    raw: String,
    is_audio: bool,
    is_subtitle: bool,
}

fn parse_label_string(s: &str) -> Option<ParsedLabel> {
    let clean = s.trim_end_matches('_');
    let parts: Vec<&str> = clean.splitn(3, '_').collect();
    if parts.len() < 2 { return None; }

    let language = parts[0];
    if language.len() < 2 || language.len() > 3 || !language.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }

    let hint = parts[1];
    let variant = if parts.len() > 2 { parts[2] } else { "" };

    let (description, codec_hint, is_audio, is_subtitle) = match hint {
        "MLP" => ("TrueHD".to_string(), "MLP".to_string(), true, false),
        "AC3" => {
            let d = if variant.is_empty() { "compatibility".to_string() } else { variant.to_string() };
            (d, "AC3".to_string(), true, false)
        }
        "DTS" => ("DTS".to_string(), "DTS".to_string(), true, false),
        "LPCM" => ("LPCM".to_string(), "LPCM".to_string(), true, false),
        "ADES" => {
            let d = if variant.is_empty() { "Descriptive Audio".to_string() }
                    else { format!("Descriptive Audio ({})", variant) };
            (d, "ADES".to_string(), true, false)
        }
        h if h.starts_with("AudioStream") => (String::new(), String::new(), true, false),
        h if h.starts_with("PGStream") => (String::new(), String::new(), false, true),
        _ => return None,
    };

    Some(ParsedLabel {
        description, language: language.to_string(), codec_hint,
        raw: s.to_string(), is_audio, is_subtitle,
    })
}

// ── Format 3: Playlist-only ─────────────────────────────────────────────────
//
// Some JARs have no track labels but do have playlist purpose markers.

fn try_playlist_only(strings: &[String]) -> Option<JarLabels> {
    let mut playlists = Vec::new();
    for s in strings {
        collect_playlist_label(s, &mut playlists);
    }
    if playlists.is_empty() { return None; }
    Some(JarLabels { audio: Vec::new(), subtitle: Vec::new(), playlists })
}

// ── Common helpers ──────────────────────────────────────────────────────────

fn collect_playlist_label(s: &str, playlists: &mut Vec<String>) {
    if matches!(s.as_ref(),
        "MAIN_FEATURE" | "MAIN_FEATURE_INTRO" |
        "FORCED_TRAILER" | "INTL_FORCED_TRAILER" |
        "commentary_extras" | "extras"
    ) {
        if !playlists.iter().any(|p| p == s) {
            playlists.push(s.to_string());
        }
    }
}

/// Extract all UTF-8 string constants from all .class files in a JAR.
fn extract_all_jar_strings(jar_data: &[u8]) -> Option<Vec<String>> {
    let cursor = std::io::Cursor::new(jar_data);
    let mut archive = zip::ZipArchive::new(cursor).ok()?;

    let mut all_strings = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        if !file.name().ends_with(".class") { continue; }

        let mut data = Vec::new();
        file.read_to_end(&mut data).ok()?;

        extract_class_strings(&data, &mut all_strings);
    }

    if all_strings.is_empty() { return None; }
    Some(all_strings)
}

/// Extract UTF-8 string constants from a Java .class file's constant pool.
fn extract_class_strings(data: &[u8], out: &mut Vec<String>) {
    if data.len() < 10 || &data[0..4] != &[0xCA, 0xFE, 0xBA, 0xBE] {
        return;
    }

    let cp_count = ((data[8] as u16) << 8 | data[9] as u16) as usize;
    let mut pos = 10;
    let mut entry = 1;

    while entry < cp_count && pos < data.len() {
        let tag = data[pos];
        pos += 1;

        match tag {
            // CONSTANT_Utf8
            1 => {
                if pos + 2 > data.len() { break; }
                let len = ((data[pos] as usize) << 8) | data[pos + 1] as usize;
                pos += 2;
                if pos + len > data.len() { break; }

                if let Ok(s) = std::str::from_utf8(&data[pos..pos + len]) {
                    if s.len() >= 3 && s.len() <= 500 {
                        out.push(s.to_string());
                    }
                }
                pos += len;
            }
            3 | 4 => { pos += 4; }
            5 | 6 => { pos += 8; entry += 1; }
            7 | 8 | 16 => { pos += 2; }
            9 | 10 | 11 | 12 | 18 => { pos += 4; }
            15 => { pos += 3; }
            _ => { break; }
        }
        entry += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_textfield_audio() {
        let strings = vec![
            "TextField,Audio1,English Dolby Atmos,Font,296,763,275,25,left".to_string(),
            "TextField,Audio2,English Descriptive Audio,Font,296,803,275,25,left".to_string(),
            "TextField,Audio3,Spanish 5.1 Dolby Digital,Font,296,843,275,25,left".to_string(),
        ];
        let labels = try_textfield_format(&strings).unwrap();
        assert_eq!(labels.audio.len(), 3);
        assert_eq!(labels.audio[0].description, "English Dolby Atmos");
        assert_eq!(labels.audio[1].description, "English Descriptive Audio");
        assert_eq!(labels.audio[2].description, "Spanish 5.1 Dolby Digital");
    }

    #[test]
    fn test_textfield_subtitle_skips_none() {
        let strings = vec![
            "TextField,Subtitle0,None,Font,1312,843,275,25,left".to_string(),
            "TextField,Subtitle1,English SDH,Font,1312,763,275,25,left".to_string(),
            "TextField,Subtitle2,Spanish,Font,1312,803,275,25,left".to_string(),
        ];
        let labels = try_textfield_format(&strings).unwrap();
        assert_eq!(labels.subtitle.len(), 2);
        assert_eq!(labels.subtitle[0].description, "English SDH");
        assert_eq!(labels.subtitle[1].description, "Spanish");
    }

    #[test]
    fn test_label_format_audio() {
        let strings = vec![
            "eng_MLP_".to_string(),
            "eng_ADES_US_".to_string(),
            "fra_AudioStream3".to_string(),
        ];
        let labels = try_label_format(&strings).unwrap();
        assert_eq!(labels.audio.len(), 3);
        assert_eq!(labels.audio[0].description, "TrueHD");
        assert_eq!(labels.audio[1].description, "Descriptive Audio (US)");
    }

    #[test]
    fn test_label_format_subtitle() {
        let strings = vec!["dan_PGStream4".to_string()];
        let labels = try_label_format(&strings).unwrap();
        assert_eq!(labels.subtitle.len(), 1);
    }

    #[test]
    fn test_no_labels() {
        let strings = vec!["java/lang/Object".to_string(), "toString".to_string()];
        assert!(try_textfield_format(&strings).is_none());
        assert!(try_label_format(&strings).is_none());
    }
}
