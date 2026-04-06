//! BD-J JAR parser — extract audio/subtitle track labels from disc menus.
//!
//! Blu-ray discs with BD-J menus store the menu application as Java JAR files
//! in BDMV/JAR/. These contain .class files with string constants that label
//! audio and subtitle tracks (e.g. "eng_ADES_US_" = Descriptive Audio US).
//!
//! with no way to tell them apart (e.g. 3x "AC-3 5.1 English").
//!
//! The JAR is a ZIP file. Each .class file has a Java constant pool containing
//! UTF-8 string literals. We scan for patterns like:
//!   {lang}_{codec}_{variant}_   → audio track labels
//!   {lang}_PGStream{n}          → subtitle track labels
//!   MAIN_FEATURE                → playlist identification
//!   FORCED_TRAILER              → forced content identification
//!
//! This is best-effort: if the JAR doesn't contain labels or the format
//! is unexpected, we return empty results. The caller falls back to
//! showing streams without labels.

use std::io::Read;

/// Labels extracted from a BD-J JAR file.
#[derive(Debug, Default)]
pub struct JarLabels {
    /// Audio track labels in STN order: (language, codec_hint, variant, raw_label)
    pub audio: Vec<TrackLabel>,
    /// Subtitle track labels: (language, stream_index, raw_label)
    pub subtitle: Vec<TrackLabel>,
    /// Playlist purpose labels (MAIN_FEATURE, FORCED_TRAILER, etc.)
    pub playlists: Vec<String>,
}

/// A parsed track label from the JAR.
#[derive(Debug, Clone)]
pub struct TrackLabel {
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Codec or type hint (e.g. "MLP", "AC3", "ADES", "PGStream")
    pub hint: String,
    /// Variant or region (e.g. "US", "UK", "3", "4")
    pub variant: String,
    /// Human-readable description derived from the label
    pub description: String,
    /// The raw string from the class file
    pub raw: String,
}

impl TrackLabel {
    /// Parse a label string like "eng_ADES_US_" or "dan_PGStream4"
    fn parse(s: &str) -> Option<Self> {
        let clean = s.trim_end_matches('_');
        let parts: Vec<&str> = clean.splitn(3, '_').collect();
        if parts.len() < 2 {
            return None;
        }

        let language = parts[0].to_string();
        // Language should be 2-3 lowercase letters
        if language.len() < 2 || language.len() > 3 || !language.chars().all(|c| c.is_ascii_lowercase()) {
            return None;
        }

        let hint = parts[1].to_string();
        let variant = if parts.len() > 2 { parts[2].to_string() } else { String::new() };

        let description = match hint.as_str() {
            "MLP" => "TrueHD".to_string(),
            "AC3" => {
                if variant.is_empty() { "compatibility".to_string() }
                else { variant.clone() }
            }
            "DTS" => "DTS".to_string(),
            "LPCM" => "LPCM".to_string(),
            "ADES" => {
                if variant.is_empty() { "Descriptive Audio".to_string() }
                else { format!("Descriptive Audio ({})", variant) }
            }
            h if h.starts_with("AudioStream") => String::new(), // generic, no extra info
            h if h.starts_with("PGStream") => String::new(),    // generic subtitle
            _ => String::new(),
        };

        Some(TrackLabel {
            language,
            hint,
            variant,
            description,
            raw: s.to_string(),
        })
    }

    /// Is this an audio track label?
    fn is_audio(&self) -> bool {
        matches!(self.hint.as_str(), "MLP" | "AC3" | "DTS" | "LPCM" | "ADES")
            || self.hint.starts_with("AudioStream")
    }

    /// Is this a subtitle track label?
    fn is_subtitle(&self) -> bool {
        self.hint.starts_with("PGStream")
    }
}

/// Extract track labels from a JAR file (raw ZIP bytes).
///
/// Returns None if the JAR can't be parsed or contains no labels.
/// This is best-effort — failure is not an error.
pub fn extract_labels(jar_data: &[u8]) -> Option<JarLabels> {
    let cursor = std::io::Cursor::new(jar_data);
    let mut archive = zip::ZipArchive::new(cursor).ok()?;

    let mut all_audio = Vec::new();
    let mut all_subtitle = Vec::new();
    let mut all_playlists = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        if !file.name().ends_with(".class") {
            continue;
        }

        let mut data = Vec::new();
        file.read_to_end(&mut data).ok()?;

        let strings = extract_class_strings(&data);

        for s in &strings {
            // Track labels: {lang}_{type}_{variant}_
            if let Some(label) = TrackLabel::parse(s) {
                if label.is_audio() && !all_audio.iter().any(|a: &TrackLabel| a.raw == label.raw) {
                    all_audio.push(label);
                } else if label.is_subtitle() && !all_subtitle.iter().any(|a: &TrackLabel| a.raw == label.raw) {
                    all_subtitle.push(label);
                }
            }

            // Playlist purpose labels
            if matches!(s.as_str(),
                "MAIN_FEATURE" | "MAIN_FEATURE_INTRO" |
                "FORCED_TRAILER" | "INTL_FORCED_TRAILER" |
                "commentary_extras" | "extras"
            ) {
                if !all_playlists.contains(s) {
                    all_playlists.push(s.clone());
                }
            }
        }
    }

    if all_audio.is_empty() && all_subtitle.is_empty() {
        return None;
    }

    Some(JarLabels {
        audio: all_audio,
        subtitle: all_subtitle,
        playlists: all_playlists,
    })
}

/// Extract all UTF-8 string constants from a Java .class file's constant pool.
///
/// Java class file format:
///   [0:4]   magic (0xCAFEBABE)
///   [4:6]   minor version
///   [6:8]   major version
///   [8:10]  constant_pool_count
///   [10:]   constant_pool entries
///
/// CONSTANT_Utf8 (tag=1): u8 tag + u16 length + bytes
/// We only extract these — they contain all string literals.
fn extract_class_strings(data: &[u8]) -> Vec<String> {
    let mut strings = Vec::new();

    // Verify Java class magic
    if data.len() < 10 || &data[0..4] != &[0xCA, 0xFE, 0xBA, 0xBE] {
        return strings;
    }

    let cp_count = ((data[8] as u16) << 8 | data[9] as u16) as usize;
    let mut pos = 10;

    // Parse constant pool entries
    let mut entry = 1; // constant pool is 1-indexed
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
                    // Only keep strings that look like track labels
                    // Must contain underscore and be reasonable length
                    if s.len() >= 5 && s.len() <= 100 && s.contains('_') {
                        strings.push(s.to_string());
                    }
                }
                pos += len;
            }
            // CONSTANT_Integer, CONSTANT_Float
            3 | 4 => { pos += 4; }
            // CONSTANT_Long, CONSTANT_Double (take 2 entries)
            5 | 6 => { pos += 8; entry += 1; }
            // CONSTANT_Class, CONSTANT_String, CONSTANT_MethodType
            7 | 8 | 16 => { pos += 2; }
            // CONSTANT_Fieldref, CONSTANT_Methodref, CONSTANT_InterfaceMethodref,
            // CONSTANT_NameAndType, CONSTANT_InvokeDynamic
            9 | 10 | 11 | 12 | 18 => { pos += 4; }
            // CONSTANT_MethodHandle
            15 => { pos += 3; }
            // Unknown tag — can't continue parsing safely
            _ => { break; }
        }
        entry += 1;
    }

    strings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_audio_labels() {
        let l = TrackLabel::parse("eng_MLP_").unwrap();
        assert_eq!(l.language, "eng");
        assert_eq!(l.hint, "MLP");
        assert_eq!(l.description, "TrueHD");
        assert!(l.is_audio());

        let l = TrackLabel::parse("eng_ADES_US_").unwrap();
        assert_eq!(l.language, "eng");
        assert_eq!(l.hint, "ADES");
        assert_eq!(l.variant, "US");
        assert_eq!(l.description, "Descriptive Audio (US)");
        assert!(l.is_audio());

        let l = TrackLabel::parse("fra_AudioStream3").unwrap();
        assert_eq!(l.language, "fra");
        assert!(l.is_audio());
    }

    #[test]
    fn test_parse_subtitle_labels() {
        let l = TrackLabel::parse("dan_PGStream4").unwrap();
        assert_eq!(l.language, "dan");
        assert!(l.is_subtitle());
    }

    #[test]
    fn test_reject_non_labels() {
        assert!(TrackLabel::parse("substring").is_none());
        assert!(TrackLabel::parse("equals").is_none());
        assert!(TrackLabel::parse("Code").is_none());
    }
}
