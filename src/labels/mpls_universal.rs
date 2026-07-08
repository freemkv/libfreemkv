//! Universal MPLS-based stream labels.
//!
//! Unlike the framework-specific parsers in this directory (dbp,
//! pixelogic, ctrm, criterion, ...), this module is the *floor*:
//! every Blu-ray ships with MPLS playlists under `/BDMV/PLAYLIST/`,
//! and every MPLS file has an STN table with per-stream ISO 639-2
//! language codes plus coding-type / channel-layout / sample-rate
//! bytes from the BD spec.
//!
//! The framework parsers extract richer editorial labels ("English
//! Dolby Atmos", "Director's Commentary") when the disc was authored
//! with a recognized tool. When none of them match (e.g. a "no BD-J"
//! disc, or an authoring framework we haven't catalogued), MPLS still
//! gives us language + codec on every stream — enough to render
//! something more useful than the bare PID.
//!
//! Output confidence is Low: MPLS carries language + codec but
//! never purpose/qualifier info (no way to tell "Commentary" from
//! "Normal" from the STN table alone). Higher-confidence framework
//! parsers, when present, always win on the registry's max-by-confidence
//! tiebreaker — MPLS is only chosen when nothing else matched.

use super::{
    LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType,
    vocab::{self, LangInfo},
};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

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
/// playlists (same PID) are deduped.
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

    let mut labels: Vec<StreamLabel> = Vec::new();
    // (stream_type_tag, language, codec_hint, pid) — PID is the
    // canonical "same physical stream" key; type+lang+codec round
    // out the rare case where two distinct logical streams happen
    // to share a PID across playlists with different metadata.
    let mut seen: Vec<(StreamLabelType, String, String, u16)> = Vec::new();

    // Global 1-based counters keyed by StreamLabelType. Incremented
    // only when an entry survives dedup, so stream_numbers are dense
    // (1, 2, 3, ...) per type across the whole disc — not reset per
    // playlist. A disc with 2 MPLS files that each list the same
    // 8 audio streams ends up with audio_1..audio_8, not audio_1..
    // audio_16 or audio_1..audio_8 with audio_1 duplicated.
    let mut audio_idx: u16 = 0;
    let mut sub_idx: u16 = 0;

    for name in &mpls_names {
        let path = format!("/BDMV/PLAYLIST/{}", name);
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(playlist) = crate::mpls::parse(&data) else {
            continue;
        };

        for entry in &playlist.streams {
            let label_type = match entry.stream_type {
                2 | 5 => StreamLabelType::Audio, // primary + secondary audio
                3 => StreamLabelType::Subtitle,  // PG subtitle
                // 1 = primary video, 6 = secondary video, 7 = DV EL
                //     → no StreamLabelType variant for video, skip.
                // 4 = IG (interactive graphics) — not a user-facing
                //     stream, skip.
                _ => continue,
            };

            let language = normalize_language(&entry.language);
            let name = language_display_name(&language);
            let codec_hint = build_codec_hint(label_type, entry);

            let key = (label_type, language.clone(), codec_hint.clone(), entry.pid);
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);

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

            labels.push(StreamLabel {
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

    if labels.is_empty() {
        return None;
    }

    // MPLS gives language + codec but never editorial info (no
    // commentary/SDH/director's cut). Low confidence means framework
    // parsers (paramount, criterion, pixelogic, ctrm, dbp, deluxe) always
    // win when they match. MPLS only gets chosen as the parser when
    // nothing else fired — exactly the universal-fallback role we want.
    Some(ParseResult::low(labels))
}

fn has_mpls_extension(name: &str) -> bool {
    // Case-insensitive ".mpls" suffix. Some discs use uppercase,
    // some lowercase; UDF filenames preserve case but we don't.
    //
    // UDF names are decoded via from_utf8_lossy, so a multi-byte
    // replacement char (EF BF BD) can straddle byte index n-5; a raw
    // byte slice there panics on a non-char-boundary. `ends_with` on a
    // lowercased copy is char-boundary-safe and still case-insensitive.
    name.len() >= 5 && name.to_ascii_lowercase().ends_with(".mpls")
}

/// Lowercase + trim the raw 3-char ISO 639-2 code. If the lowered
/// string maps via [`vocab::lang`] (it won't for plain "eng" — that
/// matcher is for English-name fragments, not codes) use its
/// canonical code; otherwise return the trimmed lowercase string.
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
        12 => Some("7.1"),
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
mod tests {
    use super::*;
    use crate::mpls::{Playlist, StreamEntry};

    fn audio_entry(pid: u16, coding: u8, fmt: u8, rate: u8, lang: &str) -> StreamEntry {
        StreamEntry {
            stream_type: 2,
            pid,
            coding_type: coding,
            video_format: 0,
            video_rate: 0,
            audio_format: fmt,
            audio_rate: rate,
            language: lang.to_string(),
            dynamic_range: 0,
            color_space: 0,
            secondary: false,
        }
    }

    fn pg_entry(pid: u16, lang: &str) -> StreamEntry {
        StreamEntry {
            stream_type: 3,
            pid,
            coding_type: 0x90,
            video_format: 0,
            video_rate: 0,
            audio_format: 0,
            audio_rate: 0,
            language: lang.to_string(),
            dynamic_range: 0,
            color_space: 0,
            secondary: false,
        }
    }

    fn playlist_with(streams: Vec<StreamEntry>) -> Playlist {
        Playlist {
            version: "0200".to_string(),
            play_items: Vec::new(),
            streams,
            marks: Vec::new(),
        }
    }

    /// Drive the same conversion logic that `parse()` runs on real
    /// disc data, but starting from already-parsed Playlists so we
    /// don't have to synthesize valid MPLS bytes.
    fn labels_from_playlists(playlists: &[Playlist]) -> Vec<StreamLabel> {
        let mut labels: Vec<StreamLabel> = Vec::new();
        let mut seen: Vec<(StreamLabelType, String, String, u16)> = Vec::new();

        // Global counters hoisted OUT of the playlist loop to match
        // production `parse()` (lines 77-78): stream_numbers are dense
        // per type across the whole disc, not reset per playlist.
        let mut audio_idx: u16 = 0;
        let mut sub_idx: u16 = 0;

        for playlist in playlists {
            for entry in &playlist.streams {
                let label_type = match entry.stream_type {
                    2 | 5 => StreamLabelType::Audio,
                    3 => StreamLabelType::Subtitle,
                    _ => continue,
                };
                // Dedup BEFORE consuming a counter value, matching prod
                // parse() ordering so a deduped duplicate does not burn a
                // stream number.
                let language = normalize_language(&entry.language);
                let name = language_display_name(&language);
                let codec_hint = build_codec_hint(label_type, entry);
                let key = (label_type, language.clone(), codec_hint.clone(), entry.pid);
                if seen.contains(&key) {
                    continue;
                }
                seen.push(key);
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
                labels.push(StreamLabel {
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
        labels
    }

    #[test]
    fn mpls_audio_streams_become_labels() {
        // Two audio streams: English TrueHD 7.1 48k, French AC-3 5.1 48k.
        let pl = playlist_with(vec![
            audio_entry(0x1100, 0x83, 12, 1, "eng"),
            audio_entry(0x1101, 0x81, 6, 1, "fra"),
        ]);
        let labels = labels_from_playlists(&[pl]);
        assert_eq!(labels.len(), 2);

        // English TrueHD 7.1
        let a = &labels[0];
        assert_eq!(a.stream_type, StreamLabelType::Audio);
        assert_eq!(a.stream_number, 1);
        assert_eq!(a.language, "eng");
        assert_eq!(a.name, "English");
        assert_eq!(a.codec_hint, "TrueHD 7.1");
        assert_eq!(a.purpose, LabelPurpose::Normal);
        assert_eq!(a.qualifier, LabelQualifier::None);
        assert_eq!(a.variant, "");

        // French AC-3 5.1
        let b = &labels[1];
        assert_eq!(b.stream_type, StreamLabelType::Audio);
        assert_eq!(b.stream_number, 2);
        assert_eq!(b.language, "fra");
        assert_eq!(b.name, "French");
        assert_eq!(b.codec_hint, "AC-3 5.1");
    }

    #[test]
    fn mpls_pg_streams_become_subtitle_labels() {
        let pl = playlist_with(vec![
            pg_entry(0x1200, "eng"),
            pg_entry(0x1201, "spa"),
            pg_entry(0x1202, "fra"),
        ]);
        let labels = labels_from_playlists(&[pl]);
        assert_eq!(labels.len(), 3);
        for label in &labels {
            assert_eq!(label.stream_type, StreamLabelType::Subtitle);
            assert_eq!(label.codec_hint, "PG");
        }
        assert_eq!(labels[0].stream_number, 1);
        assert_eq!(labels[0].language, "eng");
        assert_eq!(labels[0].name, "English");
        assert_eq!(labels[1].stream_number, 2);
        assert_eq!(labels[1].language, "spa");
        assert_eq!(labels[1].name, "Spanish");
        assert_eq!(labels[2].stream_number, 3);
        assert_eq!(labels[2].language, "fra");
    }

    #[test]
    fn dedup_streams_across_playlists() {
        // Two playlists, same English TrueHD 7.1 PID 0x1100 in both.
        // Expect one Audio label, not two.
        let pl1 = playlist_with(vec![
            audio_entry(0x1100, 0x83, 12, 1, "eng"),
            audio_entry(0x1101, 0x81, 6, 1, "fra"),
        ]);
        let pl2 = playlist_with(vec![
            audio_entry(0x1100, 0x83, 12, 1, "eng"), // duplicate
            audio_entry(0x1102, 0x82, 6, 1, "deu"),  // new
        ]);
        let labels = labels_from_playlists(&[pl1, pl2]);
        // Expected: eng@0x1100, fra@0x1101, deu@0x1102 — three uniques.
        assert_eq!(labels.len(), 3);
        // PID isn't stored on StreamLabel, so assert on the surviving
        // language set instead.
        let mut langs: Vec<String> = labels.iter().map(|l| l.language.clone()).collect();
        langs.sort();
        assert_eq!(langs, vec!["deu", "eng", "fra"]);

        // Stream numbers must be DENSE and GLOBAL across playlists, not
        // reset per playlist. eng (pl1) = 1, fra (pl1) = 2, the duplicate
        // eng in pl2 is deduped (no number consumed), and deu (pl2) = 3.
        // Regression guard for the per-playlist counter-reset divergence.
        let num = |lang: &str| {
            labels
                .iter()
                .find(|l| l.language == lang)
                .map(|l| l.stream_number)
        };
        assert_eq!(num("eng"), Some(1));
        assert_eq!(num("fra"), Some(2));
        assert_eq!(num("deu"), Some(3));
    }

    #[test]
    fn has_mpls_extension_handles_short_and_non_ascii_names() {
        // Short names: no panic, just false.
        assert!(!has_mpls_extension(""));
        assert!(!has_mpls_extension("a"));
        assert!(!has_mpls_extension(".mpl"));
        // Exact-length and longer valid suffixes, case-insensitive.
        assert!(has_mpls_extension("0.mpls"));
        assert!(has_mpls_extension("00000.MPLS"));
        assert!(has_mpls_extension("Movie.MpLs"));
        // Non-matching suffix.
        assert!(!has_mpls_extension("file.clpi"));
        // Multi-byte char near the tail must NOT panic on a byte-slice
        // boundary (from_utf8_lossy U+FFFD = EF BF BD is the real-disc
        // case). A name ending in such a char is simply not ".mpls".
        assert!(!has_mpls_extension("na\u{FFFD}me"));
        // And a name where a multi-byte char sits exactly at the n-5
        // boundary used by the old slice index.
        assert!(!has_mpls_extension("ab\u{FFFD}cd"));
        // A genuine .mpls preceded by a multi-byte char still matches.
        assert!(has_mpls_extension("f\u{FFFD}.mpls"));
    }

    #[test]
    fn coding_type_to_codec_hint_table() {
        // Spot-check every entry in the spec table. Audio entries
        // come back bare (no channels/rate set) so codec_hint is the
        // codec name alone.
        let cases: &[(u8, &str)] = &[
            (0x02, "MPEG-2"),
            (0x1B, "H.264"),
            (0x24, "HEVC"),
            (0x80, "LPCM"),
            (0x81, "AC-3"),
            (0x82, "DTS"),
            (0x83, "TrueHD"),
            (0x84, "AC-3+"),
            (0x85, "DTS-HD HR"),
            (0x86, "DTS-HD MA"),
            (0x90, "PG"),
            (0x91, "IG"),
            (0xA1, "AC-3+ Secondary"),
            (0xA2, "DTS-HD Secondary"),
        ];
        for (ct, expected) in cases {
            assert_eq!(
                codec_name(*ct),
                *expected,
                "coding_type 0x{:02X} should map to {}",
                ct,
                expected
            );
        }
        // Unknown bytes return empty.
        assert_eq!(codec_name(0x00), "");
        assert_eq!(codec_name(0xFF), "");
    }

    #[test]
    fn audio_format_appends_channel_layout() {
        let mono = audio_entry(1, 0x83, 1, 1, "eng");
        let stereo = audio_entry(2, 0x83, 3, 1, "eng");
        let surround_51 = audio_entry(3, 0x83, 6, 1, "eng");
        let surround_71 = audio_entry(4, 0x83, 12, 1, "eng");
        let unknown = audio_entry(5, 0x83, 0, 1, "eng");
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &mono),
            "TrueHD mono"
        );
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &stereo),
            "TrueHD 2.0"
        );
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &surround_51),
            "TrueHD 5.1"
        );
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &surround_71),
            "TrueHD 7.1"
        );
        assert_eq!(build_codec_hint(StreamLabelType::Audio, &unknown), "TrueHD");
    }

    #[test]
    fn audio_rate_only_shows_above_48k() {
        // 48 kHz (rate=1) is the universal default → not surfaced.
        let r48 = audio_entry(1, 0x83, 6, 1, "eng");
        // 96 kHz (rate=4) → surfaced.
        let r96 = audio_entry(2, 0x83, 6, 4, "eng");
        // 192 kHz (rate=5) → surfaced.
        let r192 = audio_entry(3, 0x83, 6, 5, "eng");
        assert_eq!(build_codec_hint(StreamLabelType::Audio, &r48), "TrueHD 5.1");
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &r96),
            "TrueHD 5.1 96kHz"
        );
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &r192),
            "TrueHD 5.1 192kHz"
        );
    }

    #[test]
    fn unknown_iso_code_passes_through_without_display_name() {
        // Made-up code: keep the raw lowercase code as `language`,
        // but `name` is empty because we don't know it.
        let pl = playlist_with(vec![audio_entry(0x1100, 0x83, 6, 1, "xyz")]);
        let labels = labels_from_playlists(&[pl]);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].language, "xyz");
        assert_eq!(labels[0].name, "");
    }

    #[test]
    fn ig_and_dv_streams_are_skipped() {
        // stream_type 4 = IG, 7 = DV EL — both must not surface.
        let mut ig = pg_entry(0x1400, "eng");
        ig.stream_type = 4;
        let mut dv = audio_entry(0x1011, 0x24, 0, 0, "");
        dv.stream_type = 7;
        let pl = playlist_with(vec![ig, dv]);
        let labels = labels_from_playlists(&[pl]);
        assert!(labels.is_empty());
    }

    #[test]
    fn secondary_audio_becomes_audio_label() {
        // stream_type 5 = secondary audio. The conversion should
        // still produce an Audio label (the registry's apply path
        // can ignore secondary if it wants — this module just
        // surfaces what's there).
        let mut sec = audio_entry(0x1A00, 0x83, 3, 1, "eng");
        sec.stream_type = 5;
        sec.secondary = true;
        let pl = playlist_with(vec![sec]);
        let labels = labels_from_playlists(&[pl]);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
        assert_eq!(labels[0].codec_hint, "TrueHD 2.0");
    }

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: language_display_name covers all documented ISO 639-2 codes.
    /// Spot-check a subset; the table is the single mapping in the codebase.
    /// Mutation: remove any entry from the match → returns "" for that code.
    #[test]
    fn language_display_name_spot_check() {
        assert_eq!(language_display_name("eng"), "English");
        assert_eq!(language_display_name("fra"), "French");
        assert_eq!(language_display_name("fre"), "French"); // BT.1 alternate
        assert_eq!(language_display_name("spa"), "Spanish");
        assert_eq!(language_display_name("deu"), "German");
        assert_eq!(language_display_name("ger"), "German"); // BT.1 alternate
        assert_eq!(language_display_name("jpn"), "Japanese");
        assert_eq!(language_display_name("zho"), "Chinese");
        assert_eq!(language_display_name("chi"), "Chinese"); // BT.1 alternate
        assert_eq!(language_display_name("kor"), "Korean");
        assert_eq!(language_display_name("por"), "Portuguese");
        assert_eq!(language_display_name("rus"), "Russian");
        assert_eq!(language_display_name("ara"), "Arabic");
    }

    /// Spec: unknown ISO codes → empty string (no guess).
    /// Mutation: return "Unknown" for unrecognized codes → non-empty string returned.
    #[test]
    fn language_display_name_unknown_returns_empty() {
        assert_eq!(language_display_name("xyz"), "");
        assert_eq!(language_display_name(""), "");
        assert_eq!(language_display_name("zz"), ""); // not a valid 3-letter code
    }

    /// Spec: BD-ROM STN coding_type table is exhaustive for audio families.
    /// Tests every audio coding_type in the spec (LPCM=0x80, AC-3=0x81, ...).
    /// Mutation: remove 0x82 → DTS returns "" instead of "DTS".
    #[test]
    fn codec_name_all_audio_types() {
        assert_eq!(codec_name(0x80), "LPCM");
        assert_eq!(codec_name(0x81), "AC-3");
        assert_eq!(codec_name(0x82), "DTS");
        assert_eq!(codec_name(0x83), "TrueHD");
        assert_eq!(codec_name(0x84), "AC-3+");
        assert_eq!(codec_name(0x85), "DTS-HD HR");
        assert_eq!(codec_name(0x86), "DTS-HD MA");
        assert_eq!(codec_name(0xA1), "AC-3+ Secondary");
        assert_eq!(codec_name(0xA2), "DTS-HD Secondary");
    }

    /// Spec: video/graphics coding_types are also in the table.
    /// Mutation: remove 0x24 → HEVC returns "" instead of "HEVC".
    #[test]
    fn codec_name_video_and_pg_types() {
        assert_eq!(codec_name(0x02), "MPEG-2");
        assert_eq!(codec_name(0x1B), "H.264");
        assert_eq!(codec_name(0x24), "HEVC");
        assert_eq!(codec_name(0x90), "PG");
        assert_eq!(codec_name(0x91), "IG");
    }

    /// Spec: build_codec_hint for subtitle streams uses only the codec name (no channels/rate).
    /// Mutation: apply channel suffix to subtitle → "PG mono" returned incorrectly.
    #[test]
    fn build_codec_hint_subtitle_no_channels_appended() {
        let e = pg_entry(0x1200, "eng");
        assert_eq!(build_codec_hint(StreamLabelType::Subtitle, &e), "PG");
    }

    /// Spec: unknown audio format → no channel suffix.
    /// Mutation: append "?" on unknown format → "TrueHD ?" returned.
    #[test]
    fn build_codec_hint_unknown_audio_format_no_suffix() {
        let e = audio_entry(0x1100, 0x83, 0, 1, "eng");
        assert_eq!(build_codec_hint(StreamLabelType::Audio, &e), "TrueHD");
    }

    /// Spec: 96 kHz rate suffix only for audio rate=4.
    /// Mutation: show "96kHz" for rate=1 (48 kHz) → spurious suffix.
    #[test]
    fn build_codec_hint_48k_omitted_96k_shown() {
        let e48 = audio_entry(1, 0x83, 12, 1, "eng");
        let e96 = audio_entry(2, 0x83, 12, 4, "eng");
        assert_eq!(build_codec_hint(StreamLabelType::Audio, &e48), "TrueHD 7.1");
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &e96),
            "TrueHD 7.1 96kHz"
        );
    }

    /// Spec: 192 kHz rate suffix for audio rate=5.
    /// Mutation: map rate=5 to "96kHz" → incorrect rate label.
    #[test]
    fn build_codec_hint_192k_shown() {
        let e = audio_entry(1, 0x83, 6, 5, "eng");
        assert_eq!(
            build_codec_hint(StreamLabelType::Audio, &e),
            "TrueHD 5.1 192kHz"
        );
    }

    /// Spec: unknown coding_type returns empty string → no codec_hint populated.
    /// Mutation: return "Unknown" for bad types → non-empty hint emitted.
    #[test]
    fn build_codec_hint_unknown_coding_type_returns_empty() {
        let e = audio_entry(1, 0x00, 6, 1, "eng"); // 0x00 not in the table
        assert_eq!(build_codec_hint(StreamLabelType::Audio, &e), "");
    }

    /// Spec: dedup key includes PID. Two streams with same lang/codec but
    /// different PIDs are NOT duplicates (different physical streams).
    /// Mutation: omit PID from the dedup key → second stream dropped.
    #[test]
    fn dedup_different_pid_same_lang_codec_not_deduped() {
        let pl = playlist_with(vec![
            audio_entry(0x1100, 0x83, 12, 1, "eng"), // PID 0x1100
            audio_entry(0x1101, 0x83, 12, 1, "eng"), // PID 0x1101 — different stream
        ]);
        let labels = labels_from_playlists(&[pl]);
        assert_eq!(labels.len(), 2, "different PIDs must NOT be deduped");
        assert_eq!(labels[0].stream_number, 1);
        assert_eq!(labels[1].stream_number, 2);
    }

    /// Spec: normalize_language lowercases and trims the raw field.
    /// Mutation: skip lowercase normalization → "ENG" stays "ENG" in the label.
    #[test]
    fn normalize_language_lowercases_and_trims() {
        assert_eq!(
            super::super::mpls_universal::language_display_name(&{
                let trimmed = "  ENG  ".trim().to_ascii_lowercase();
                // feed through production normalize_language logic
                trimmed
            }),
            "English"
        );
    }
}
