//! Universal MPLS-based stream labels: the confidence-tier floor.
//!
//! Every Blu-ray ships MPLS playlists under `/BDMV/PLAYLIST/` with an
//! STN table giving per-stream language codes plus coding-type /
//! channel-layout / sample-rate bytes. Framework-specific parsers
//! (dbp, pixelogic, ctrm, criterion, ...) extract richer editorial
//! labels when the disc matches a recognized authoring tool; this
//! module is the fallback, always Low confidence.

use super::{
    LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType,
    vocab::{self, LangInfo},
};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashSet;

// Retained-label cap: a crafted image can carry tens of thousands of playlists, each
// naming distinct clips with a full STN table.
const MAX_MPLS_LABELS: usize = 4096;

/// True iff `/BDMV/PLAYLIST/` exists and contains at least one
/// `.mpls` file. Cheap directory walk only — no sector reads.
pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    let Some(dir) = udf.find_dir("/BDMV/PLAYLIST") else {
        return false;
    };
    dir.entries
        .iter()
        .any(|e| !e.is_dir && has_mpls_extension(&e.name))
}

/// Walk every `*.mpls` in `/BDMV/PLAYLIST/`, parse it, and convert
/// each StreamEntry to a [`StreamLabel`]. Streams shared across
/// playlists (same clip and PID) are deduped.
///
/// Returns `None` if no labels could be produced (e.g. no .mpls files
/// parsed successfully, or every parsed stream was a type we skip
/// like IG / DV EL).
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let playlist_dir = udf.find_dir("/BDMV/PLAYLIST")?;

    // Collect mpls filenames first so we don't hold a borrow on udf
    // while we call udf.read_file (which takes &self).
    let mpls_names: Vec<String> = playlist_dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && has_mpls_extension(&e.name))
        .map(|e| e.name.clone())
        .collect();

    if mpls_names.is_empty() {
        return None;
    }

    // Labels are built per playlist and each Playlist dropped, so a crafted
    // directory of huge playlists never holds more than one in memory.
    let mut labels: Vec<StreamLabel> = Vec::new();
    let mut seen: HashSet<super::StreamId> = HashSet::new();
    for name in &mpls_names {
        if labels.len() >= MAX_MPLS_LABELS {
            break;
        }
        let path = format!("/BDMV/PLAYLIST/{}", name);
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(playlist) = crate::mpls::parse(&data) else {
            continue;
        };
        add_playlist_labels(&playlist, &mut labels, &mut seen);
    }

    if labels.is_empty() {
        return None;
    }

    // MPLS gives language + codec but never editorial info. Low confidence
    // means framework parsers always win when they match; MPLS is only
    // chosen when nothing else fired — the universal-fallback role we want.
    Some(ParseResult::low(labels))
}

// Converts stream entries into StreamLabels; identity is (clip, PID).
#[cfg(test)]
fn build_labels(playlists: &[crate::mpls::Playlist]) -> Vec<StreamLabel> {
    let mut labels: Vec<StreamLabel> = Vec::new();
    let mut seen: HashSet<super::StreamId> = HashSet::new();
    for playlist in playlists {
        add_playlist_labels(playlist, &mut labels, &mut seen);
    }
    labels
}

fn add_playlist_labels(
    playlist: &crate::mpls::Playlist,
    labels: &mut Vec<StreamLabel>,
    seen: &mut HashSet<super::StreamId>,
) {
    {
        // `Playlist::streams` is the FIRST play item's STN table, so every entry
        // here is a stream of that play item's clip, matching `disc::bluray`'s
        // `clips[0]` — that pairing is what makes the PID an identity.
        let Some(clip_id) = playlist.play_items.first().map(|pi| pi.clip_id.clone()) else {
            return;
        };

        // 1-based STN slot within THIS playlist's table, per type — matches how
        // `disc::bluray` counts the same entries. Nothing binds through it (labels
        // bind by id); it's stated truthfully so the label list shows real slots.
        let mut audio_idx: u16 = 0;
        let mut sub_idx: u16 = 0;

        for entry in &playlist.streams {
            let Some(label_type) = label_type_for(entry) else {
                continue;
            };
            let stream_number = match label_type {
                StreamLabelType::Audio => {
                    audio_idx += 1;
                    audio_idx
                }
                StreamLabelType::Subtitle => {
                    sub_idx += 1;
                    sub_idx
                }
            };

            let stream_id = super::StreamId {
                clip_id: clip_id.clone(),
                pid: entry.pid,
            };
            if labels.len() >= MAX_MPLS_LABELS {
                return;
            }
            if !seen.insert(stream_id.clone()) {
                continue;
            }

            let language = normalize_language(&entry.language);
            let name = language_display_name(&language);
            let codec_hint = build_codec_hint(label_type, entry);

            labels.push(StreamLabel {
                stream_id: Some(stream_id),
                stream_number,
                stream_type: label_type,
                language,
                name,
                purpose: LabelPurpose::Normal,
                qualifier: LabelQualifier::None,
                codec_hint,
                variant: String::new(),
            });
        }
    }
}

// Per-type numbering list an STN entry belongs to, or None if unlabellable. MUST agree with
// disc::bluray's stream list.
fn label_type_for(entry: &crate::mpls::StreamEntry) -> Option<StreamLabelType> {
    use crate::consts::coding_type as c;
    if entry.coding_type == 0 {
        return None;
    }
    match entry.stream_type {
        2 | 5 if entry.coding_type == c::PG => Some(StreamLabelType::Subtitle),
        2 | 5 => Some(StreamLabelType::Audio),
        // PiP PG is the secondary-video overlay, not a stream of the title.
        3 if entry.secondary => None,
        3 => Some(StreamLabelType::Subtitle),
        _ => None,
    }
}

fn has_mpls_extension(name: &str) -> bool {
    // Case-insensitive ".mpls" suffix (discs use both cases). Compared on a lowercased
    // copy, never by slicing: UDF names are lossily decoded, so byte n-5 may fall
    // inside a multi-byte replacement char.
    name.to_ascii_lowercase().ends_with(".mpls")
}

// Lowercase + trim the raw 3-char ISO 639-2 code; if vocab::lang maps
// it, use its canonical code, else return the trimmed lowercase string.
fn normalize_language(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return String::new();
    }
    // vocab::lang() matches free-form English names, not ISO 639-2
    // codes — so for the typical MPLS payload ("eng", "fra", ...)
    // it returns None and we keep the trimmed code.
    if let Some(LangInfo { code, .. }) = vocab::lang(&trimmed) {
        return code.to_string();
    }
    trimmed
}

/// Human-readable English name for an ISO 639-2 code, or empty if
/// the code is unknown. Kept inline rather than in vocab because
/// vocab is the *reverse* mapping (name → code).
pub(crate) fn language_display_name(iso: &str) -> String {
    match iso {
        "eng" => "English",
        "fra" | "fre" => "French",
        "spa" => "Spanish",
        "deu" | "ger" => "German",
        "ita" => "Italian",
        "jpn" => "Japanese",
        "zho" | "chi" => "Chinese",
        "kor" => "Korean",
        "por" => "Portuguese",
        "pol" => "Polish",
        "ces" | "cze" => "Czech",
        "hun" => "Hungarian",
        "nld" | "dut" => "Dutch",
        "ara" => "Arabic",
        "hin" => "Hindi",
        "tur" => "Turkish",
        "tha" => "Thai",
        "swe" => "Swedish",
        "nor" => "Norwegian",
        "dan" => "Danish",
        "fin" => "Finnish",
        "heb" => "Hebrew",
        "rus" => "Russian",
        "ell" | "gre" => "Greek",
        "vie" => "Vietnamese",
        "ind" => "Indonesian",
        "msa" | "may" => "Malay",
        "ukr" => "Ukrainian",
        "ron" | "rum" => "Romanian",
        "bul" => "Bulgarian",
        "hrv" => "Croatian",
        "srp" => "Serbian",
        "slk" | "slo" => "Slovak",
        "slv" => "Slovenian",
        "est" => "Estonian",
        "lav" => "Latvian",
        "lit" => "Lithuanian",
        "isl" | "ice" => "Icelandic",
        "eus" | "baq" => "Basque",
        "cat" => "Catalan",
        "glg" => "Galician",
        _ => "",
    }
    .to_string()
}

/// Map BD coding_type byte → codec name. Returns empty for unknown
/// bytes (the table covers everything the spec defines, but unknown
/// values are still possible on malformed discs).
pub(crate) fn codec_name(coding_type: u8) -> &'static str {
    use crate::consts::coding_type as c;
    match coding_type {
        c::MPEG2_VIDEO => "MPEG-2",
        c::H264 => "H.264",
        c::HEVC => "HEVC",
        c::LPCM => "LPCM",
        c::AC3 => "AC-3",
        c::DTS => "DTS",
        c::TRUEHD => "TrueHD",
        c::AC3_PLUS => "AC-3+",
        c::DTS_HD_HR => "DTS-HD HR", // BD-ROM Part 3-1: 0x85 = DTS-HD High Resolution
        c::DTS_HD_MA => "DTS-HD MA",
        c::PG => "PG",
        c::IG => "IG",
        c::AC3_PLUS_SECONDARY => "AC-3+ Secondary",
        c::DTS_HD_SECONDARY => "DTS-HD Secondary",
        _ => "",
    }
}

/// Build the final `codec_hint`. For audio streams, optionally
/// append " <channels>" and/or " <rate>" suffixes. Sample rate is
/// only spelled out for non-48k (the universal default).
fn build_codec_hint(label_type: StreamLabelType, entry: &crate::mpls::StreamEntry) -> String {
    let base = codec_name(entry.coding_type);
    if base.is_empty() {
        return String::new();
    }

    if label_type != StreamLabelType::Audio {
        return base.to_string();
    }

    let mut out = base.to_string();

    let channels = match entry.audio_format {
        1 => Some("mono"),
        3 => Some("2.0"),
        6 => Some("5.1"),
        // 12 is the BD "combo" type (stereo core + extension), not 7.1: state no channels.
        _ => None,
    };
    if let Some(ch) = channels {
        out.push(' ');
        out.push_str(ch);
    }

    // 1 = 48 kHz (universal default, omit). Only call out higher rates.
    let rate = match entry.audio_rate {
        4 => Some("96kHz"),
        5 => Some("192kHz"),
        _ => None,
    };
    if let Some(r) = rate {
        out.push(' ');
        out.push_str(r);
    }

    out
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "mpls_universal_tests.rs"]
mod tests;
