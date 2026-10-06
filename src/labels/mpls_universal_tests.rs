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
        hdr_plus: false,
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
        hdr_plus: false,
        secondary: false,
    }
}

// A playlist over clip "00001" with one play item, so each label gets the `(clip, PID)`
// identity it is bound by.
fn playlist_with(streams: Vec<StreamEntry>) -> Playlist {
    playlist_on("00001", streams)
}

fn playlist_on(clip_id: &str, streams: Vec<StreamEntry>) -> Playlist {
    Playlist {
        version: "0200".to_string(),
        play_items: vec![crate::mpls::PlayItem {
            clip_id: clip_id.to_string(),
            in_time: 0,
            out_time: 0,
            connection_condition: 1,
        }],
        streams,
        marks: Vec::new(),
    }
}

// Drives the real build_labels (what parse() calls) from parsed Playlists so mutations
// there are actually caught here.
fn labels_from_playlists(playlists: &[Playlist]) -> Vec<StreamLabel> {
    build_labels(playlists)
}

// Minimal MPLS: one play item on `clip` whose STN lists one primary audio stream `pid`.
fn mpls_bytes(clip: &[u8; 5], pid: u16) -> Vec<u8> {
    let mut item = Vec::new();
    item.extend_from_slice(clip);
    item.extend_from_slice(b"M2TS");
    item.extend_from_slice(&[0u8; 3]);
    item.extend_from_slice(&0u32.to_be_bytes());
    item.extend_from_slice(&(7000u32 * 45000).to_be_bytes());
    item.extend_from_slice(&[0u8; 12]); // UO mask, misc, still
    item.extend_from_slice(&[0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    item.extend_from_slice(&[3, 0x01]);
    item.extend_from_slice(&pid.to_be_bytes());
    item.extend_from_slice(&[5, 0x81, 0x61]);
    item.extend_from_slice(b"eng");
    let mut pl = vec![0u8; 6];
    pl.extend_from_slice(&1u16.to_be_bytes());
    pl.extend_from_slice(&[0u8; 2]);
    pl.extend_from_slice(&(item.len() as u16).to_be_bytes());
    pl.extend_from_slice(&item);
    let pl_len = (pl.len() - 4) as u32;
    pl[0..4].copy_from_slice(&pl_len.to_be_bytes());
    let mut buf = b"MPLS0200".to_vec();
    buf.extend_from_slice(&40u32.to_be_bytes());
    buf.extend_from_slice(&[0u8; 28]);
    buf.extend_from_slice(&pl);
    buf
}

// The same stream named by two playlists yields one label; an unparseable playlist
// between them is skipped, not fatal.
#[test]
fn parse_dedups_across_playlists_and_skips_a_bad_one() {
    use crate::udf::fixture::*;
    let files = vec![
        file_with("00800.mpls", 32, 8200, mpls_bytes(b"00001", 0x1100), false),
        file_with("00801.mpls", 33, 8300, b"not an mpls".to_vec(), false),
        file_with("00802.mpls", 34, 8400, mpls_bytes(b"00001", 0x1100), false),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "PLAYLIST".to_string(),
                icb_lba: 30,
                dir_data_lba: 31,
                files,
                subdirs: vec![],
            }],
        }],
    };
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    let got = parse(&mut disc, &udf).expect("labels");
    assert_eq!(got.labels.len(), 1);
}

#[test]
fn playlist_without_play_items_yields_no_labels() {
    let mut pl = playlist_with(vec![audio_entry(0x1100, 0x81, 6, 1, "eng")]);
    pl.play_items.clear();
    assert!(labels_from_playlists(&[pl]).is_empty());
}

#[test]
fn pip_pg_streams_are_not_labelled_and_take_no_slot() {
    let mut pip = pg_entry(0x1B00, "eng");
    pip.secondary = true;
    let pl = playlist_with(vec![pg_entry(0x1200, "eng"), pip, pg_entry(0x1201, "fra")]);
    let labels = labels_from_playlists(&[pl]);
    let got: Vec<(u16, u16)> = labels
        .iter()
        .map(|l| (l.stream_id.as_ref().unwrap().pid, l.stream_number))
        .collect();
    assert_eq!(got, [(0x1200, 1), (0x1201, 2)]);
}

#[test]
fn retained_labels_are_capped() {
    let playlists: Vec<Playlist> = (0..MAX_MPLS_LABELS + 50)
        .map(|i| {
            playlist_on(
                &format!("{i:05}"),
                vec![audio_entry(0x1100, 0x81, 6, 1, "eng")],
            )
        })
        .collect();
    assert_eq!(labels_from_playlists(&playlists).len(), MAX_MPLS_LABELS);
}

#[test]
fn mpls_audio_streams_become_labels() {
    // Two audio streams: English TrueHD combo 48k, French AC-3 5.1 48k.
    let pl = playlist_with(vec![
        audio_entry(0x1100, 0x83, 12, 1, "eng"),
        audio_entry(0x1101, 0x81, 6, 1, "fra"),
    ]);
    let labels = labels_from_playlists(&[pl]);
    assert_eq!(labels.len(), 2);

    // English TrueHD
    let a = &labels[0];
    assert_eq!(a.stream_type, StreamLabelType::Audio);
    assert_eq!(a.stream_number, 1);
    assert_eq!(a.language, "eng");
    assert_eq!(a.name, "English");
    assert_eq!(a.codec_hint, "TrueHD");
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

// disc::bluray drops coding_type==0 (STN padding) from the stream list that stream_number
// binds against, so it must not be counted here.
#[test]
fn padding_stn_entry_does_not_consume_a_label_slot() {
    let pl = playlist_with(vec![
        audio_entry(0x1100, 0x83, 12, 1, "eng"),
        // coding_type 0: STN padding. Not a stream.
        audio_entry(0x1101, 0x00, 0, 0, ""),
        audio_entry(0x1102, 0x81, 6, 1, "fra"),
    ]);
    let labels = labels_from_playlists(&[pl]);
    assert_eq!(labels.len(), 2, "the padding slot yields no label");
    assert_eq!(labels[0].language, "eng");
    assert_eq!(labels[0].stream_number, 1);
    assert_eq!(labels[1].language, "fra");
    assert_eq!(
        labels[1].stream_number, 2,
        "padding is absent from the title's stream list, so `fra` is \
             audio stream 2"
    );
}

// A PG coding_type in an audio STN slot is a real, documented shape; disc::bluray
// classifies it Subtitle, so this module must match.
#[test]
fn pg_coding_type_in_an_audio_slot_counts_as_a_subtitle() {
    let mut misplaced = audio_entry(0x1200, 0x90, 0, 0, "spa");
    misplaced.stream_type = 2;
    let pl = playlist_with(vec![
        audio_entry(0x1100, 0x83, 12, 1, "eng"),
        misplaced,
        audio_entry(0x1101, 0x81, 6, 1, "fra"),
        pg_entry(0x1201, "deu"),
    ]);
    let labels = labels_from_playlists(&[pl]);

    let audio: Vec<_> = labels
        .iter()
        .filter(|l| l.stream_type == StreamLabelType::Audio)
        .map(|l| (l.language.as_str(), l.stream_number))
        .collect();
    assert_eq!(
        audio,
        vec![("eng", 1), ("fra", 2)],
        "the PG entry is not an audio stream and must not number one"
    );

    let sub: Vec<_> = labels
        .iter()
        .filter(|l| l.stream_type == StreamLabelType::Subtitle)
        .map(|l| (l.language.as_str(), l.stream_number))
        .collect();
    assert_eq!(
        sub,
        vec![("spa", 1), ("deu", 2)],
        "it is subtitle stream 1, ahead of the PG-slot entry"
    );
}

/// Two playlists over the SAME clip that both list PID 0x1100: one
/// physical stream, so one label. Identity is `(clip, PID)`, and each
/// label states the STN slot it holds in its own playlist.
#[test]
fn one_label_per_stream_across_playlists_on_the_same_clip() {
    let pl1 = playlist_on(
        "00001",
        vec![
            audio_entry(0x1100, 0x83, 12, 1, "eng"),
            audio_entry(0x1101, 0x81, 6, 1, "fra"),
        ],
    );
    let pl2 = playlist_on(
        "00001",
        vec![
            audio_entry(0x1100, 0x83, 12, 1, "eng"), // same stream
            audio_entry(0x1102, 0x82, 6, 1, "deu"),  // new
        ],
    );
    let labels = labels_from_playlists(&[pl1, pl2]);
    assert_eq!(
        labels.len(),
        3,
        "eng/fra/deu — the duplicate eng is one stream"
    );

    let id = |lang: &str| {
        labels
            .iter()
            .find(|l| l.language == lang)
            .and_then(|l| l.stream_id.clone())
            .map(|i| (i.clip_id, i.pid))
    };
    assert_eq!(id("eng"), Some(("00001".into(), 0x1100)));
    assert_eq!(id("fra"), Some(("00001".into(), 0x1101)));
    assert_eq!(id("deu"), Some(("00001".into(), 0x1102)));

    // `stream_number` is the entry's slot in ITS OWN playlist's STN table —
    // deu is pl2's second audio, so 2, not "third distinct stream on disc".
    // (Used to be a dense disc-global counter fed to a binder expecting a slot.)
    let num = |lang: &str| {
        labels
            .iter()
            .find(|l| l.language == lang)
            .map(|l| l.stream_number)
    };
    assert_eq!(num("eng"), Some(1));
    assert_eq!(num("fra"), Some(2));
    assert_eq!(num("deu"), Some(2), "pl2's second audio slot");
}

// Same PID in two DIFFERENT clips is two streams — a PID is only unique within one clip,
// not deduped across clips.
#[test]
fn same_pid_in_two_clips_is_two_streams() {
    let pl1 = playlist_on("00001", vec![audio_entry(0x1100, 0x83, 12, 1, "eng")]);
    let pl2 = playlist_on("00002", vec![audio_entry(0x1100, 0x83, 12, 1, "eng")]);
    let labels = labels_from_playlists(&[pl1, pl2]);
    assert_eq!(labels.len(), 2, "different clips: two distinct streams");
    let clips: Vec<String> = labels
        .iter()
        .filter_map(|l| l.stream_id.as_ref().map(|i| i.clip_id.clone()))
        .collect();
    assert_eq!(clips, vec!["00001", "00002"]);
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
        "TrueHD",
        "12 is the combo type, not 7.1: no channel suffix"
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
    // stream_type 5 = secondary audio; conversion still produces an Audio
    // label (the registry's apply path can ignore secondary if it wants).
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
    assert_eq!(build_codec_hint(StreamLabelType::Audio, &e48), "TrueHD");
    assert_eq!(
        build_codec_hint(StreamLabelType::Audio, &e96),
        "TrueHD 96kHz"
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
    assert_eq!(normalize_language("  ENG  "), "eng");
    assert_eq!(normalize_language("   "), "");
    // An unknown code keeps its trimmed lowercase form.
    assert_eq!(normalize_language(" XYZ "), "xyz");
}
