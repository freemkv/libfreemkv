//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework.
//! To add a new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<Vec<StreamLabel>>`
//!   4. Add `mod myformat;` below and one line to `PARSERS` array

mod criterion;
mod ctrm;
mod paramount;
mod pixelogic;
pub mod vocab;

use crate::disc::{DiscTitle, Stream};
use crate::sector::SectorReader;
use crate::udf::UdfFs;

// Re-exported via crate::disc — the public API surfaces these next to
// AudioStream/SubtitleStream so callers can map purpose/qualifier to display
// text in their own locale.

/// A stream label extracted from disc config files.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StreamLabel {
    /// STN index (1-based)
    pub stream_number: u16,
    /// Audio or Subtitle
    pub stream_type: StreamLabelType,
    /// ISO 639-2 language code
    pub language: String,
    /// Display name (e.g. "Commentary", "Descriptive Audio")
    pub name: String,
    /// Stream purpose
    pub purpose: LabelPurpose,
    /// Additional qualifier
    pub qualifier: LabelQualifier,
    /// Codec hint from config (e.g. "TrueHD", "Dolby Digital", "Dolby Atmos")
    pub codec_hint: String,
    /// Regional variant (e.g. "US", "UK", "Castilian", "Canadian")
    pub variant: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StreamLabelType {
    Audio,
    Subtitle,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelPurpose {
    Normal,
    Commentary,
    Descriptive,
    Score,
    Ime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelQualifier {
    None,
    Sdh,
    DescriptiveService,
    Forced,
}

// ── Parser registry ────────────────────────────────────────────────────────
//
// Each entry: (name, detect_fn, parse_fn)
// Order = priority. First match wins. Highest quality output first.

type DetectFn = fn(&UdfFs) -> bool;
type ParseFn = fn(&mut dyn SectorReader, &UdfFs) -> Option<Vec<StreamLabel>>;

const PARSERS: &[(&str, DetectFn, ParseFn)] = &[
    ("paramount", paramount::detect, paramount::parse),
    ("criterion", criterion::detect, criterion::parse),
    ("pixelogic", pixelogic::detect, pixelogic::parse),
    ("ctrm", ctrm::detect, ctrm::parse),
    // ("deluxe",  deluxe::detect,     deluxe::parse),  // TODO: bytecode parser
];

/// Search disc for config files, extract labels, apply to streams.
/// This is 100% optional — if anything fails, streams are untouched.
pub fn apply(reader: &mut dyn SectorReader, udf: &UdfFs, titles: &mut [DiscTitle]) {
    let labels = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| extract(reader, udf)))
        .unwrap_or_default();
    if labels.is_empty() {
        return;
    }

    for title in titles.iter_mut() {
        let mut audio_idx: u16 = 0;
        let mut sub_idx: u16 = 0;

        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) => {
                    audio_idx += 1;
                    if let Some(label) = labels.iter().find(|l| {
                        l.stream_type == StreamLabelType::Audio && l.stream_number == audio_idx
                    }) {
                        // Structured fields — callers translate purpose to UI text.
                        a.purpose = label.purpose;

                        // a.label only carries codec/variant info. NEVER any
                        // English purpose text — the CLI handles that via i18n.
                        let mut parts = Vec::new();
                        if !label.variant.is_empty() {
                            parts.push(format!("({})", label.variant));
                        }
                        if !label.codec_hint.is_empty() {
                            parts.push(label.codec_hint.clone());
                        }
                        if !parts.is_empty() {
                            a.label = parts.join(" ");
                        } else if !label.name.is_empty() && label.purpose == LabelPurpose::Normal {
                            // Only fall back to the parser-supplied display
                            // name when there's no purpose to flag — the CLI
                            // handles purpose rendering itself.
                            a.label = label.name.clone();
                        }
                    }
                }
                Stream::Subtitle(s) => {
                    sub_idx += 1;
                    if let Some(label) = labels.iter().find(|l| {
                        l.stream_type == StreamLabelType::Subtitle && l.stream_number == sub_idx
                    }) {
                        s.qualifier = label.qualifier;
                        if label.qualifier == LabelQualifier::Forced {
                            s.forced = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Fill in default labels for any streams that don't have one.
/// Runs after BD-J label extraction — fills gaps with codec + channel descriptions.
/// This is the central place for all fallback label generation.
pub fn fill_defaults(titles: &mut [crate::disc::DiscTitle]) {
    use crate::disc::Stream;

    for title in titles.iter_mut() {
        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) if a.label.is_empty() => {
                    a.label = generate_audio_label(&a.codec, &a.channels, a.secondary);
                }
                Stream::Video(v) if v.label.is_empty() => {
                    v.label =
                        generate_video_label(&v.codec, v.resolution.pixels(), &v.hdr, v.secondary);
                }
                Stream::Subtitle(s) if s.forced => {
                    // Ensure forced subs are labeled even if BD-J didn't set a name
                    // (subtitle labels are generally not set — this just marks forced)
                }
                _ => {}
            }
        }
    }
}

fn generate_video_label(
    codec: &crate::disc::Codec,
    pixels: (u32, u32),
    hdr: &crate::disc::HdrFormat,
    secondary: bool,
) -> String {
    use crate::disc::HdrFormat;

    if secondary {
        // "Dolby Vision EL" is a brand identifier, not English prose, so the
        // library may emit it. Other "secondary video" wording is a CLI
        // concern — the library just leaves the label empty.
        return match hdr {
            HdrFormat::DolbyVision => "Dolby Vision EL".to_string(),
            _ => String::new(),
        };
    }

    let mut parts = Vec::new();

    // Codec
    parts.push(codec.name().to_string());

    // Resolution
    let (w, h) = pixels;
    let res = if w >= 7680 {
        "8K"
    } else if w >= 3840 {
        "4K"
    } else if w >= 1920 {
        "1080p"
    } else if w >= 1280 {
        "720p"
    } else if h >= 576 {
        "576p"
    } else if h >= 480 {
        "480p"
    } else {
        ""
    };
    if !res.is_empty() {
        parts.push(res.into());
    }

    // HDR
    match hdr {
        HdrFormat::Sdr => {}
        _ => parts.push(hdr.name().to_string()),
    }

    parts.join(" ")
}

fn generate_audio_label(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    _secondary: bool,
) -> String {
    use crate::disc::{AudioChannels, Codec};

    // Full marketing names for disc audio codecs.
    // These are codec brand identifiers, not user-facing English prose.
    let codec_name = match codec {
        Codec::TrueHd => "Dolby TrueHD",
        Codec::Ac3 => "Dolby Digital",
        Codec::Ac3Plus => "Dolby Digital Plus",
        Codec::DtsHdMa => "DTS-HD Master Audio",
        Codec::DtsHdHr => "DTS-HD High Resolution",
        Codec::Dts => "DTS",
        Codec::Lpcm => "LPCM",
        Codec::Aac => "AAC",
        Codec::Mp2 => "MPEG Audio",
        Codec::Mp3 => "MP3",
        Codec::Flac => "FLAC",
        Codec::Opus => "Opus",
        _ => return String::new(),
    };

    // Channel layout
    let channel_str = match channels {
        AudioChannels::Mono => "1.0",
        AudioChannels::Stereo => "2.0",
        AudioChannels::Stereo21 => "2.1",
        AudioChannels::Quad => "4.0",
        AudioChannels::Surround50 => "5.0",
        AudioChannels::Surround51 => "5.1",
        AudioChannels::Surround61 => "6.1",
        AudioChannels::Surround71 => "7.1",
        AudioChannels::Unknown => "",
    };

    // The "(Secondary)" suffix is a CLI/UI concern — callers display it from
    // the AudioStream::secondary bool, not the library.
    if channel_str.is_empty() {
        codec_name.to_string()
    } else {
        format!("{} {}", codec_name, channel_str)
    }
}

fn extract(reader: &mut dyn SectorReader, udf: &UdfFs) -> Vec<StreamLabel> {
    for (_name, detect, parse) in PARSERS {
        if detect(udf) {
            if let Some(labels) = parse(reader, udf) {
                return labels;
            }
        }
    }
    Vec::new()
}

// ── Shared helpers ─────────────────────────────────────────────────────────

/// Check if a file exists in any BDMV/JAR subdirectory.
pub(crate) fn jar_file_exists(udf: &UdfFs, filename: &str) -> bool {
    find_jar_file(udf, filename).is_some()
}

/// Find a file in any BDMV/JAR subdirectory, return its path.
pub(crate) fn find_jar_file(udf: &UdfFs, filename: &str) -> Option<String> {
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in &jar_dir.entries {
        if entry.is_dir {
            let path = format!("/BDMV/JAR/{}/{}", entry.name, filename);
            // Check if file exists in this subdirectory
            for child in &entry.entries {
                if !child.is_dir && child.name.eq_ignore_ascii_case(filename) {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// Read a file from any BDMV/JAR subdirectory by filename.
pub(crate) fn read_jar_file(
    reader: &mut dyn SectorReader,
    udf: &UdfFs,
    filename: &str,
) -> Option<Vec<u8>> {
    let path = find_jar_file(udf, filename)?;
    udf.read_file(reader, &path).ok().filter(|d| !d.is_empty())
}
