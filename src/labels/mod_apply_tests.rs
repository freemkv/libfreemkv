use super::*;
use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, Resolution, SampleRate,
    SubtitleStream, VideoStream,
};

fn audio(pid: u16, codec: Codec, channels: AudioChannels, language: &str) -> Stream {
    Stream::Audio(AudioStream {
        pid,
        codec,
        channels,
        language: language.into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })
}

fn subtitle(pid: u16, language: &str) -> Stream {
    Stream::Subtitle(SubtitleStream {
        pid,
        codec: Codec::Pgs,
        language: language.into(),
        forced: false,
        qualifier: LabelQualifier::None,
        codec_data: None,
    })
}

fn video() -> Stream {
    Stream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R2160p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Hdr10,
        color_space: ColorSpace::Bt2020,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })
}

fn title_with(streams: Vec<Stream>) -> DiscTitle {
    DiscTitle {
        playlist: "00800.mpls".into(),
        playlist_id: 800,
        duration_secs: 7200.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams,
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: crate::disc::ContentFormat::BdTs,
        codec_privates: Vec::new(),
    }
}

/// A title that plays one named clip — the shape that matters for
/// cross-playlist binding, where two playlists cover the same clip.
fn title_on_clip(playlist: &str, clip_id: &str, streams: Vec<Stream>) -> DiscTitle {
    DiscTitle {
        playlist: playlist.into(),
        clips: vec![crate::disc::Clip {
            feed_span: None,
            clip_id: clip_id.into(),
            in_time: 0,
            out_time: 0,
            duration_secs: 7200.0,
            source_packets: 0,
        }],
        ..title_with(streams)
    }
}

/// Sentinel embedded in the crafted playlist name below. The capture keeps
/// ONLY fields whose rendered form contains it, so the assertion cannot be
/// fooled by another test's log output.
const LOG_INJECTION_SENTINEL: &str = "FMKV-LOG-INJECTION-PROBE";

// A crafted.mpls filename must be logged via `?` (Debug), not `%` (Display) — Display
// writes control/ANSI bytes verbatim (CWE-117).
#[test]
fn a_disc_derived_playlist_name_is_escaped_in_the_log_not_written_verbatim() {
    // A name whose bytes would clear the line and repaint it.
    let evil = format!("\u{1b}[2K\u{1b}[31m{LOG_INJECTION_SENTINEL}\u{7}\u{1b}[0m.mpls");

    let labels = vec![
        sub_label(1, "eng", LabelQualifier::None),
        sub_label(2, "spa", LabelQualifier::None),
        sub_label(3, "fra", LabelQualifier::None),
    ];
    let mut titles = vec![title_on_clip(
        &evil,
        "00294",
        vec![
            subtitle(0x12A0, "eng"),
            subtitle(0x12A1, "spa"),
            subtitle(0x12A2, "fra"),
        ],
    )];
    // `testlog::capture` installs the crate's ONE global `tracing` subscriber,
    // routing events to a thread-local sink; `tracing` caches interest
    // globally, so a per-test subscriber would poison other tests' callsites.
    let ((), events) = crate::testlog::capture(|| {
        apply_labels(&labels, &mut titles);
    });
    let playlist: Vec<&str> = events
        .iter()
        .filter(|e| e.target.starts_with("libfreemkv::labels"))
        .filter_map(|e| e.field("playlist"))
        .filter(|v| v.contains(LOG_INJECTION_SENTINEL))
        .collect();
    assert!(
        !playlist.is_empty(),
        "the anchoring event must actually have fired, or this test proves \
             nothing; captured: {events:?}"
    );
    for rendered in playlist {
        assert!(
            !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
            "a disc-controlled playlist name reached the log with its raw \
                 control bytes intact: {rendered:?}"
        );
        assert!(
            rendered.contains(LOG_INJECTION_SENTINEL),
            "the name must still be legible once escaped: {rendered:?}"
        );
    }
}

fn sub_label(num: u16, lang: &str, qualifier: LabelQualifier) -> StreamLabel {
    StreamLabel {
        stream_id: None,
        stream_number: num,
        stream_type: StreamLabelType::Subtitle,
        language: lang.into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier,
        codec_hint: String::new(),
        variant: String::new(),
    }
}

/// `(pid, forced, qualifier)` for every subtitle of a title.
fn sub_state(title: &DiscTitle) -> Vec<(u16, bool, LabelQualifier)> {
    title
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(s) => Some((s.pid, s.forced, s.qualifier)),
            _ => None,
        })
        .collect()
}

fn audio_label(num: u16, lang: &str, codec_hint: &str, variant: &str) -> StreamLabel {
    StreamLabel {
        stream_id: None,
        stream_number: num,
        stream_type: StreamLabelType::Audio,
        language: lang.into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: codec_hint.into(),
        variant: variant.into(),
    }
}

/// A DVD MPEG-2 extension track is not a listed stream: it takes no vendor slot and keeps
/// its marker label, so slot 2 names the next real audio track.
#[test]
fn apply_skips_mp2_extension_tracks() {
    let mut ext = audio(0x00D0, Codec::Mp2, AudioChannels::Unknown, "eng");
    if let Stream::Audio(a) = &mut ext {
        a.label = crate::disc::MP2_EXTENSION_LABEL.into();
    }
    let mut titles = vec![title_with(vec![
        video(),
        audio(0x00C0, Codec::Mp2, AudioChannels::Stereo, "eng"),
        ext,
        audio(0x00C1, Codec::Mp2, AudioChannels::Stereo, "eng"),
    ])];
    let labels = vec![audio_label(2, "eng", "", "(Commentary mix)")];
    apply_labels(&labels, &mut titles);
    let labels_of: Vec<String> = titles[0]
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Audio(a) => Some(a.label.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(labels_of[1], crate::disc::MP2_EXTENSION_LABEL);
    assert!(labels_of[2].contains("Commentary mix"), "{labels_of:?}");
    assert!(!labels_of[0].contains("Commentary mix"), "{labels_of:?}");
}

#[test]
fn apply_attaches_codec_hint_and_variant_to_audio() {
    let mut titles = vec![title_with(vec![
        video(),
        audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
    ])];
    let labels = vec![audio_label(1, "eng", "Dolby Atmos", "")];
    apply_labels(&labels, &mut titles);

    if let Stream::Audio(a) = &titles[0].streams[1] {
        assert_eq!(a.label, "Dolby Atmos");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_combines_variant_and_codec_hint() {
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::TrueHd,
        AudioChannels::Surround51,
        "por",
    )])];
    let labels = vec![audio_label(1, "por", "Dolby Atmos", "Brazilian")];
    apply_labels(&labels, &mut titles);

    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "(Brazilian) Dolby Atmos");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_rejects_mismatched_codec_hint_and_uses_stream_codec() {
    // TrueHD+Atmos relabel case: a TrueHD+Atmos main track the parser mislabeled
    // "AC-3 2.0" (a compat-core hint bound to the wrong stream). The hint
    // contradicts the stream's real codec → discard it, use the stream's own.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::TrueHd,
        AudioChannels::Surround71,
        "eng",
    )])];
    let labels = vec![audio_label(1, "eng", "AC-3 2.0", "")];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "Dolby TrueHD 7.1");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_unshuffles_cross_labeled_streams() {
    // Cross-bound hints case: hints fully cross-bound — a TrueHD stream wears "AC-3 5.1"
    // and a DD+ stream wears "TrueHD 5.1". Each is corrected from its own
    // stream codec, eliminating the shuffle.
    let mut titles = vec![title_with(vec![
        audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
        audio(0x1101, Codec::Ac3Plus, AudioChannels::Surround51, "spa"),
    ])];
    let labels = vec![
        audio_label(1, "eng", "AC-3 5.1", ""),
        audio_label(2, "spa", "TrueHD 5.1", ""),
    ];
    apply_labels(&labels, &mut titles);
    let got: Vec<String> = titles[0]
        .streams
        .iter()
        .filter_map(|s| {
            if let Stream::Audio(a) = s {
                Some(a.label.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(got, vec!["Dolby TrueHD 5.1", "Dolby Digital Plus 5.1"]);
}

#[test]
fn apply_keeps_consistent_richer_atmos_hint() {
    // A DD+ Atmos stream legitimately labeled "Dolby Atmos" — the hint is
    // richer than the spec codec yet consistent with it, so it's kept.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::Ac3Plus,
        AudioChannels::Surround51,
        "eng",
    )])];
    let labels = vec![audio_label(1, "eng", "Dolby Atmos", "")];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "Dolby Atmos");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_keeps_consistent_dtsx_hint_on_dts_hd_ma() {
    // DTS:X rides a DTS-HD MA core just as Atmos rides TrueHD. A correctly
    // authored "DTS:X" hint on a DtsHdMa stream is richer than the spec
    // codec yet consistent, so it's kept verbatim, not regenerated.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::DtsHdMa,
        AudioChannels::Surround71,
        "eng",
    )])];
    let labels = vec![audio_label(1, "eng", "DTS:X", "")];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "DTS:X");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn dtsx_hint_consistent_with_dts_hd_carriers() {
    use crate::disc::Codec;
    // The MED fix: a DTS:X hint must now be judged consistent with
    // its DTS-HD lossless carriers (previously it was rejected,
    // because says_dts_ma/says_dts_hr were both false for "DTS:X").
    assert!(codec_hint_consistent("DTS:X", &Codec::DtsHdMa));
    assert!(codec_hint_consistent("DTS-X 7.1", &Codec::DtsHdHr));
    assert!(codec_hint_consistent("dtsx", &Codec::DtsHdMa));
    // It still names the DTS family, so plain-DTS streams remain
    // consistent (family match) — never discarded.
    assert!(codec_hint_consistent("DTS:X", &Codec::Dts));
    // But a DTS:X hint on a non-DTS stream is a genuine mismatch.
    assert!(!codec_hint_consistent("DTS:X", &Codec::TrueHd));
    assert!(!codec_hint_consistent("DTS:X", &Codec::Ac3Plus));
}

#[test]
fn apply_normalizes_plain_consistent_hint_to_marketing() {
    // A DD+ stream whose hint "AC-3+ 5.1" is correct but short-form; a sibling
    // fallback track uses the marketing form, so a plain (non-richer)
    // consistent hint is normalized to the stream's own to stay consistent.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::Ac3Plus,
        AudioChannels::Surround51,
        "fra",
    )])];
    let labels = vec![audio_label(1, "fra", "AC-3+ 5.1", "")];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "Dolby Digital Plus 5.1");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_sets_purpose_on_audio_commentary() {
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::Ac3,
        AudioChannels::Stereo,
        "eng",
    )])];
    let labels = vec![StreamLabel {
        stream_id: None,
        stream_number: 1,
        stream_type: StreamLabelType::Audio,
        language: "eng".into(),
        name: String::new(),
        purpose: LabelPurpose::Commentary,
        qualifier: LabelQualifier::None,
        codec_hint: String::new(),
        variant: String::new(),
    }];
    apply_labels(&labels, &mut titles);

    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.purpose, LabelPurpose::Commentary);
        // Label stays empty: no codec/variant; purpose is conveyed
        // structurally, NOT as English text.
        assert_eq!(a.label, "");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_uses_name_fallback_only_for_normal_purpose() {
    // Name fallback fires when purpose=Normal and codec/variant are empty.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::TrueHd,
        AudioChannels::Surround71,
        "eng",
    )])];
    let labels = vec![StreamLabel {
        stream_id: None,
        stream_number: 1,
        stream_type: StreamLabelType::Audio,
        language: "eng".into(),
        name: "Director's Cut Edition".into(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: String::new(),
        variant: String::new(),
    }];
    apply_labels(&labels, &mut titles);

    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "Director's Cut Edition");
    } else {
        panic!("expected audio stream");
    }
}

#[test]
fn apply_name_fallback_suppressed_for_non_normal_purpose() {
    // Name fallback must NOT fire when purpose != Normal — the
    // CLI is responsible for rendering purpose text.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::Ac3,
        AudioChannels::Stereo,
        "eng",
    )])];
    let labels = vec![StreamLabel {
        stream_id: None,
        stream_number: 1,
        stream_type: StreamLabelType::Audio,
        language: "eng".into(),
        name: "Commentary by Director".into(),
        purpose: LabelPurpose::Commentary,
        qualifier: LabelQualifier::None,
        codec_hint: String::new(),
        variant: String::new(),
    }];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "", "label must not contain English purpose text");
        assert_eq!(a.purpose, LabelPurpose::Commentary);
    }
}

#[test]
fn apply_sets_qualifier_on_subtitle_sdh() {
    let mut titles = vec![title_with(vec![subtitle(0x1200, "eng")])];
    let labels = vec![StreamLabel {
        stream_id: None,
        stream_number: 1,
        stream_type: StreamLabelType::Subtitle,
        language: "eng".into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::Sdh,
        codec_hint: String::new(),
        variant: String::new(),
    }];
    apply_labels(&labels, &mut titles);

    if let Stream::Subtitle(s) = &titles[0].streams[0] {
        assert_eq!(s.qualifier, LabelQualifier::Sdh);
        // SDH doesn't flip the `forced` flag.
        assert!(!s.forced);
    } else {
        panic!("expected subtitle");
    }
}

#[test]
fn apply_flips_forced_flag_on_subtitle_forced_qualifier() {
    let mut titles = vec![title_with(vec![subtitle(0x1200, "eng")])];
    let labels = vec![StreamLabel {
        stream_id: None,
        stream_number: 1,
        stream_type: StreamLabelType::Subtitle,
        language: "eng".into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::Forced,
        codec_hint: String::new(),
        variant: String::new(),
    }];
    apply_labels(&labels, &mut titles);
    if let Stream::Subtitle(s) = &titles[0].streams[0] {
        assert_eq!(s.qualifier, LabelQualifier::Forced);
        assert!(s.forced);
    }
}

#[test]
fn apply_indexes_streams_by_type_separately() {
    // Audio and subtitle each have their own 1-based index; an
    // Audio #2 label maps to the 2nd audio stream, not the 2nd
    // stream overall (which could be a subtitle).
    let mut titles = vec![title_with(vec![
        video(),
        audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
        subtitle(0x1200, "eng"),
        audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "fra"),
    ])];
    let labels = vec![
        audio_label(1, "eng", "Dolby Atmos", ""),
        audio_label(2, "fra", "Dolby Digital", ""),
        StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Subtitle,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::Sdh,
            codec_hint: String::new(),
            variant: String::new(),
        },
    ];
    apply_labels(&labels, &mut titles);

    // Audio #1
    if let Stream::Audio(a) = &titles[0].streams[1] {
        assert_eq!(a.label, "Dolby Atmos");
    }
    // Audio #2 (4th stream overall). The plain "Dolby Digital" hint is
    // consistent with the AC-3 stream but carries no channel info, so it's
    // normalized to the stream's own uniform descriptor.
    if let Stream::Audio(a) = &titles[0].streams[3] {
        assert_eq!(a.label, "Dolby Digital 2.0");
    }
    // Subtitle #1
    if let Stream::Subtitle(s) = &titles[0].streams[2] {
        assert_eq!(s.qualifier, LabelQualifier::Sdh);
    }
}

#[test]
fn apply_ignores_labels_for_nonexistent_streams() {
    // A label for stream #99 with no matching stream is a no-op.
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::TrueHd,
        AudioChannels::Surround51,
        "eng",
    )])];
    let labels = vec![audio_label(99, "fra", "Dolby Digital", "")];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "", "label must be untouched");
    }
}

// Cross-playlist mis-binding this module's two-tier binding stops: ordinal numbering would
// put Forced on different PIDs per sibling playlist.
#[test]
fn forced_label_follows_the_pid_not_the_ordinal_across_sibling_playlists() {
    // Six slots: three plain, a commentary subtitle, then two forced.
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::None),
        sub_label(2, "spa", LabelQualifier::None),
        sub_label(3, "fra", LabelQualifier::None),
        sub_label(4, "eng", LabelQualifier::None),
        sub_label(5, "spa", LabelQualifier::Forced),
        sub_label(6, "fra", LabelQualifier::Forced),
    ];
    // The default rip target enumerates a seventh stream (0x12A4, a full
    // English track) in the middle; its sibling does not.
    let mut titles = vec![
        title_on_clip(
            "00801.mpls",
            "00294",
            vec![
                subtitle(0x12A0, "eng"),
                subtitle(0x12A1, "spa"),
                subtitle(0x12A2, "fra"),
                subtitle(0x12A3, "eng"),
                subtitle(0x12A4, "eng"),
                subtitle(0x12A5, "spa"),
                subtitle(0x12A6, "fra"),
            ],
        ),
        title_on_clip(
            "00040.mpls",
            "00294",
            vec![
                subtitle(0x12A0, "eng"),
                subtitle(0x12A1, "spa"),
                subtitle(0x12A2, "fra"),
                subtitle(0x12A3, "eng"),
                subtitle(0x12A5, "spa"),
                subtitle(0x12A6, "fra"),
            ],
        ),
    ];
    apply_labels(&labels, &mut titles);

    assert_eq!(
        sub_state(&titles[0]),
        vec![
            (0x12A0, false, LabelQualifier::None),
            (0x12A1, false, LabelQualifier::None),
            (0x12A2, false, LabelQualifier::None),
            (0x12A3, false, LabelQualifier::None),
            // The extra stream the label list never described: untouched,
            // and above all NOT forced.
            (0x12A4, false, LabelQualifier::None),
            (0x12A5, true, LabelQualifier::Forced),
            (0x12A6, true, LabelQualifier::Forced),
        ],
    );
    // Same two PIDs forced in the sibling — the flags now agree.
    assert_eq!(
        sub_state(&titles[1]),
        vec![
            (0x12A0, false, LabelQualifier::None),
            (0x12A1, false, LabelQualifier::None),
            (0x12A2, false, LabelQualifier::None),
            (0x12A3, false, LabelQualifier::None),
            (0x12A5, true, LabelQualifier::Forced),
            (0x12A6, true, LabelQualifier::Forced),
        ],
    );
}

// No anchor: ordinal fallback drops a label whose language contradicts the stream —
// unlabelled beats mislabelled `forced`.
#[test]
fn ordinal_binding_drops_a_subtitle_label_that_contradicts_the_stream_language() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::None),
        sub_label(2, "fra", LabelQualifier::Forced),
    ];
    let mut titles = vec![title_with(vec![
        subtitle(0x12A0, "eng"),
        subtitle(0x12A1, "spa"),
    ])];
    apply_labels(&labels, &mut titles);
    assert_eq!(
        sub_state(&titles[0]),
        vec![
            (0x12A0, false, LabelQualifier::None),
            (0x12A1, false, LabelQualifier::None),
        ],
    );
}

/// The same guard on the audio side: a label whose language contradicts
/// the stream is not applied, so a shifted list cannot move `Commentary`
/// onto a main dialogue track.
#[test]
fn ordinal_binding_drops_an_audio_label_that_contradicts_the_stream_language() {
    let mut titles = vec![title_with(vec![
        audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
        audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "spa"),
    ])];
    let labels = vec![
        audio_label(1, "eng", "", ""),
        StreamLabel {
            stream_id: None,
            stream_number: 2,
            stream_type: StreamLabelType::Audio,
            language: "fra".into(),
            name: String::new(),
            purpose: LabelPurpose::Commentary,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        },
    ];
    apply_labels(&labels, &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[1] {
        assert_eq!(a.purpose, LabelPurpose::Normal);
    } else {
        panic!("expected audio stream");
    }
}

// A single agreeing stream is not evidence: every disc has some
// one-audio clip whose language matches label #1, so it must not anchor.
#[test]
fn a_single_stream_title_cannot_anchor_the_label_list() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::None),
        sub_label(2, "spa", LabelQualifier::Forced),
    ];
    let titles = vec![title_on_clip(
        "01241.mpls",
        "00294",
        vec![subtitle(0x12A4, "eng")],
    )];
    assert_eq!(
        find_anchor(&labels, &titles, StreamLabelType::Subtitle),
        None,
    );
}

#[test]
fn apply_empty_labels_does_not_touch_streams() {
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::TrueHd,
        AudioChannels::Surround51,
        "eng",
    )])];
    apply_labels(&[], &mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "");
    }
}

// ── fill_defaults() tests ───────────────────────────────────────────────

#[test]
fn fill_defaults_generates_audio_label_when_empty() {
    let mut titles = vec![title_with(vec![audio(
        0x1100,
        Codec::TrueHd,
        AudioChannels::Surround71,
        "eng",
    )])];
    fill_defaults(&mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "Dolby TrueHD 7.1");
    }
}

#[test]
fn fill_defaults_preserves_existing_audio_label() {
    let mut titles = vec![title_with(vec![Stream::Audio(AudioStream {
        pid: 0x1100,
        codec: Codec::TrueHd,
        channels: AudioChannels::Surround71,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: "Pre-set Atmos".into(),
    })])];
    fill_defaults(&mut titles);
    if let Stream::Audio(a) = &titles[0].streams[0] {
        assert_eq!(a.label, "Pre-set Atmos");
    }
}

// Spec: fill_defaults must not clobber a pre-set video label
// (mirrors the audio contract above; mutate the is_empty() guard to
// `true` to see this go red).
#[test]
fn fill_defaults_preserves_existing_video_label() {
    let mut titles = vec![title_with(vec![Stream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R2160p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Hdr10,
        color_space: ColorSpace::Bt2020,
        display_aspect: None,
        secondary: false,
        label: "Pre-set 4K HDR".into(),
        measured_cicp: None,
    })])];
    fill_defaults(&mut titles);
    if let Stream::Video(v) = &titles[0].streams[0] {
        assert_eq!(v.label, "Pre-set 4K HDR");
    } else {
        panic!("expected video stream");
    }
}

#[test]
fn fill_defaults_generates_video_label_with_hdr() {
    let mut titles = vec![title_with(vec![video()])];
    fill_defaults(&mut titles);
    if let Stream::Video(v) = &titles[0].streams[0] {
        assert!(v.label.contains("4K"), "expected 4K, got {}", v.label);
        assert!(v.label.contains("HDR10"), "expected HDR10, got {}", v.label);
    }
}

/// Spec: an interlaced resolution (`R*i`) must surface the "i" scan type
/// in the generated label, not a hardcoded "p". PAL DVD is 576i.
/// Mutation: hardcode "p" → 576i video mislabeled as 576p.
#[test]
fn fill_defaults_video_label_honors_interlaced_scan_type() {
    let interlaced = Stream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Mpeg2,
        resolution: Resolution::R576i,
        frame_rate: FrameRate::F25,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt470bg,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    });
    let mut titles = vec![title_with(vec![interlaced])];
    fill_defaults(&mut titles);
    if let Stream::Video(v) = &titles[0].streams[0] {
        assert!(v.label.contains("576i"), "expected 576i, got {}", v.label);
        assert!(
            !v.label.contains("576p"),
            "must not say 576p, got {}",
            v.label
        );
    }

    let progressive = Stream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Mpeg2,
        resolution: Resolution::R576p,
        frame_rate: FrameRate::F25,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt470bg,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    });
    let mut titles = vec![title_with(vec![progressive])];
    fill_defaults(&mut titles);
    if let Stream::Video(v) = &titles[0].streams[0] {
        assert!(v.label.contains("576p"), "expected 576p, got {}", v.label);
    }
}

// ── codec_hint_consistent hardening ───────────────────────────────────────

/// Spec: "Dolby Digital" (AC-3) hint is consistent ONLY with AC-3 streams;
/// NOT with DD+ or TrueHD.
/// Mutation: accept "Dolby Digital" as consistent with AC-3+ → DD+ mislabeled.
#[test]
fn codec_hint_consistent_ac3_not_confused_with_ddp() {
    assert!(codec_hint_consistent("Dolby Digital", &Codec::Ac3));
    assert!(codec_hint_consistent("AC-3 5.1", &Codec::Ac3));
    assert!(!codec_hint_consistent("Dolby Digital", &Codec::Ac3Plus));
    assert!(!codec_hint_consistent("AC-3 5.1", &Codec::TrueHd));
}

/// Spec: "Dolby Digital Plus" (AC-3+) is consistent with DD+ streams,
/// NOT with plain AC-3.
/// Mutation: merge DD and DD+ into one family check → mismatch undetected.
#[test]
fn codec_hint_consistent_ddp_not_confused_with_ac3() {
    assert!(codec_hint_consistent("Dolby Digital Plus", &Codec::Ac3Plus));
    assert!(codec_hint_consistent("E-AC-3", &Codec::Ac3Plus));
    assert!(codec_hint_consistent("DD+", &Codec::Ac3Plus));
    assert!(!codec_hint_consistent("Dolby Digital Plus", &Codec::Ac3));
}

/// Spec: "DTS" hint consistent with DTS streams, NOT DTS-HD families.
/// Mutation: treat bare "DTS" hint as consistent with DtsHdMa → mismatch.
#[test]
fn codec_hint_consistent_dts_families_distinguished() {
    assert!(codec_hint_consistent("DTS", &Codec::Dts));
    assert!(!codec_hint_consistent("DTS", &Codec::DtsHdMa));
    assert!(!codec_hint_consistent("DTS", &Codec::DtsHdHr));
    assert!(codec_hint_consistent("DTS-HD MA", &Codec::DtsHdMa));
    assert!(codec_hint_consistent("DTS-HD HR", &Codec::DtsHdHr));
}

/// Spec: "LPCM" hint consistent only with Lpcm codec.
/// Mutation: make PCM consistent with all → mismatch undetected.
#[test]
fn codec_hint_consistent_lpcm() {
    assert!(codec_hint_consistent("LPCM 7.1", &Codec::Lpcm));
    assert!(codec_hint_consistent("PCM", &Codec::Lpcm));
    assert!(!codec_hint_consistent("LPCM", &Codec::TrueHd));
    assert!(!codec_hint_consistent("LPCM", &Codec::Ac3));
}

/// Spec: empty codec hint → consistent (no assertion = no contradiction).
/// Mutation: return false for empty hint → streams with no hint lose their label.
#[test]
fn codec_hint_consistent_empty_hint() {
    assert!(codec_hint_consistent("", &Codec::TrueHd));
    assert!(codec_hint_consistent("", &Codec::Ac3));
    assert!(codec_hint_consistent("", &Codec::Lpcm));
}

/// Spec: a pure-editorial hint (e.g. "Commentary") names no codec family
/// and is therefore consistent with any codec stream.
/// Mutation: parse "commentary" and return false → editorial labels discarded.
#[test]
fn codec_hint_consistent_editorial_hint_no_codec() {
    assert!(codec_hint_consistent("Commentary", &Codec::TrueHd));
    assert!(codec_hint_consistent("Commentary", &Codec::Ac3));
    assert!(codec_hint_consistent("Commentary", &Codec::Dts));
}

// ── generate_audio_label hardening ─────────────────────────────────────────

/// Spec: `generate_audio_label` uses full marketing names, not abbreviations.
/// Mutation: use "DD" instead of "Dolby Digital" → abbreviated name returned.
#[test]
fn generate_audio_label_all_codecs() {
    assert_eq!(
        generate_audio_label(&Codec::TrueHd, &AudioChannels::Surround51, false),
        "Dolby TrueHD 5.1"
    );
    assert_eq!(
        generate_audio_label(&Codec::Ac3, &AudioChannels::Surround51, false),
        "Dolby Digital 5.1"
    );
    assert_eq!(
        generate_audio_label(&Codec::Ac3Plus, &AudioChannels::Surround51, false),
        "Dolby Digital Plus 5.1"
    );
    assert_eq!(
        generate_audio_label(&Codec::DtsHdMa, &AudioChannels::Surround51, false),
        "DTS-HD Master Audio 5.1"
    );
    assert_eq!(
        generate_audio_label(&Codec::DtsHdHr, &AudioChannels::Surround51, false),
        "DTS-HD High Resolution 5.1"
    );
    assert_eq!(
        generate_audio_label(&Codec::Dts, &AudioChannels::Surround51, false),
        "DTS 5.1"
    );
    assert_eq!(
        generate_audio_label(&Codec::Lpcm, &AudioChannels::Surround51, false),
        "LPCM 5.1"
    );
}

/// Spec: Unknown codec → empty string (never "?", never panic).
/// Mutation: return "Unknown" for unrecognized codecs → non-empty string.
#[test]
fn generate_audio_label_unknown_codec_empty() {
    assert_eq!(
        generate_audio_label(&Codec::Pgs, &AudioChannels::Surround51, false),
        ""
    );
}

/// Spec: Unknown channel layout → codec name only (no channel suffix).
/// Mutation: append " Unknown" for unrecognized channels → spurious suffix.
#[test]
fn generate_audio_label_unknown_channels_no_suffix() {
    assert_eq!(
        generate_audio_label(&Codec::Ac3, &AudioChannels::Unknown, false),
        "Dolby Digital"
    );
}

/// Spec: all channel layouts produce the documented string suffixes.
/// Mutation: swap any two (e.g. Mono/Stereo) → wrong descriptor rendered.
#[test]
fn generate_audio_label_all_channel_layouts() {
    let f = |ch| generate_audio_label(&Codec::Ac3, ch, false);
    assert_eq!(f(&AudioChannels::Mono), "Dolby Digital 1.0");
    assert_eq!(f(&AudioChannels::Stereo), "Dolby Digital 2.0");
    assert_eq!(f(&AudioChannels::Surround51), "Dolby Digital 5.1");
    assert_eq!(f(&AudioChannels::Surround71), "Dolby Digital 7.1");
}

/// Spec: codec_hint_adds_detail only returns true for Atmos and DTS:X.
/// Mutation: return true for all hints → plain hints kept verbatim, no normalization.
#[test]
fn codec_hint_adds_detail_atmos_and_dtsx_only() {
    assert!(codec_hint_adds_detail("Dolby Atmos"));
    assert!(codec_hint_adds_detail("DTS:X"));
    assert!(codec_hint_adds_detail("DTS-X 7.1"));
    assert!(codec_hint_adds_detail("dtsx"));
    assert!(!codec_hint_adds_detail("Dolby TrueHD"));
    assert!(!codec_hint_adds_detail("DTS-HD Master Audio"));
    assert!(!codec_hint_adds_detail("Dolby Digital Plus 5.1"));
    assert!(!codec_hint_adds_detail(""));
}

// ── generate_video_label hardening: secondary Dolby Vision stream gets "Dolby Vision EL",
// every other HDR format on a secondary stream gets no label.
#[test]
fn generate_video_label_secondary_dolby_vision_el() {
    assert_eq!(
        generate_video_label(
            &Codec::Hevc,
            (3840, 2160),
            false,
            &HdrFormat::DolbyVision,
            true
        ),
        "Dolby Vision EL"
    );
    // Every other HDR format on a secondary stream: empty, not text.
    assert_eq!(
        generate_video_label(&Codec::Hevc, (3840, 2160), false, &HdrFormat::Hdr10, true),
        ""
    );
}

// Spec: 480 lines is the SD floor — height exactly 480 must get the "480p"/"480i" token,
// not the empty-resolution case.
#[test]
fn generate_video_label_480_boundary() {
    let label = generate_video_label(&Codec::Mpeg2, (0, 480), false, &HdrFormat::Sdr, false);
    assert!(
        label.contains("480p"),
        "h == 480 must resolve to 480p, got {label:?}"
    );
}

// Spec: SDR is the unmarked default — must never appear as a token (only non-SDR formats
// get an explicit tag).
#[test]
fn generate_video_label_sdr_produces_no_hdr_token() {
    assert_eq!(
        generate_video_label(&Codec::Hevc, (1920, 1080), false, &HdrFormat::Sdr, false),
        "HEVC 1080p"
    );
}

// ── generate_audio_label_atmos ───────────────────────────────────────

// Spec: the Atmos-aware variant folds "Atmos" into the codec brand for TrueHD/DD+ carriers.
#[test]
fn generate_audio_label_atmos_folds_brand() {
    assert_eq!(
        generate_audio_label_atmos(&Codec::TrueHd, &AudioChannels::Surround71, false),
        "Dolby TrueHD Atmos 7.1"
    );
    assert_eq!(
        generate_audio_label_atmos(&Codec::Ac3Plus, &AudioChannels::Surround51, false),
        "Dolby Digital Plus Atmos 5.1"
    );
}

// Spec: every disc-audio codec has a full marketing name, including lossy PC-container
// codecs.
#[test]
fn generate_audio_label_covers_pc_container_codecs() {
    assert_eq!(
        generate_audio_label(&Codec::Aac, &AudioChannels::Stereo, false),
        "AAC 2.0"
    );
    assert_eq!(
        generate_audio_label(&Codec::Mp2, &AudioChannels::Stereo, false),
        "MPEG Audio 2.0"
    );
    assert_eq!(
        generate_audio_label(&Codec::Mp3, &AudioChannels::Stereo, false),
        "MP3 2.0"
    );
    assert_eq!(
        generate_audio_label(&Codec::Flac, &AudioChannels::Stereo, false),
        "FLAC 2.0"
    );
    assert_eq!(
        generate_audio_label(&Codec::Opus, &AudioChannels::Stereo, false),
        "Opus 2.0"
    );
}

// ── codec_hint_consistent: chained-OR boundary hardening. Each test
// below isolates ONE `||` synonym clause so weakening it to `&&`
// changes the verdict. Isolates the "true hd" (space form) synonym.
#[test]
fn codec_hint_consistent_truehd_space_synonym() {
    assert!(codec_hint_consistent("True HD 7.1", &Codec::TrueHd));
    assert!(!codec_hint_consistent("True HD 7.1", &Codec::Ac3));
}

// Isolates the "ac3+" (no-hyphen) synonym in says_ddp; must not fall
// through to the plain-AC3 says_ac3 check.
#[test]
fn codec_hint_consistent_ddp_ac3_plus_no_hyphen_synonym() {
    assert!(codec_hint_consistent("AC3+ 5.1", &Codec::Ac3Plus));
    assert!(!codec_hint_consistent("AC3+ 5.1", &Codec::Ac3));
}

// Isolates the "eac3" synonym in says_ddp, last clause before
// "digital plus"/"dd+".
#[test]
fn codec_hint_consistent_ddp_eac3_synonym() {
    assert!(codec_hint_consistent("EAC3 5.1", &Codec::Ac3Plus));
    assert!(!codec_hint_consistent("EAC3 5.1", &Codec::Ac3));
}

// Isolates the "pcm" (no "lpcm") synonym in says_lpcm.
#[test]
fn codec_hint_consistent_lpcm_bare_pcm_synonym() {
    assert!(codec_hint_consistent("PCM", &Codec::Lpcm));
    assert!(!codec_hint_consistent("PCM", &Codec::Ac3));
}

// Isolates says_dts_ma || says_dts_hr inside names_family.
#[test]
fn codec_hint_consistent_names_family_dts_ma_alone() {
    assert!(!codec_hint_consistent("Master Audio", &Codec::Ac3));
    assert!(codec_hint_consistent("Master Audio", &Codec::DtsHdMa));
}

// Isolates the Codec::TrueHd atmos arm: Atmos with a family that is not the other carrier.
#[test]
fn codec_hint_consistent_truehd_arm_atmos_alone() {
    assert!(codec_hint_consistent("LPCM Atmos", &Codec::TrueHd));
}

// Spec: Codec::Dts is consistent ONLY when says_dts is true, not via any other named
// family.
#[test]
fn codec_hint_consistent_dts_arm_not_bypassed() {
    assert!(!codec_hint_consistent("Dolby Digital", &Codec::Dts));
}

// Spec: Codec::Lpcm is consistent ONLY when says_lpcm is true (same
// bypass failure mode as the Dts arm above).
#[test]
fn codec_hint_consistent_lpcm_arm_not_bypassed() {
    assert!(!codec_hint_consistent("Dolby Digital", &Codec::Lpcm));
}

// ── provenance: which labels may be reached by counting ────────────────

/// An MPLS/CLPI-derived label naming clip `clip`, PID `pid`, sitting at
/// slot `num` of its own playlist's table.
fn derived_sub(clip: &str, pid: u16, num: u16, lang: &str) -> StreamLabel {
    StreamLabel {
        stream_id: Some(StreamId {
            clip_id: clip.into(),
            pid,
        }),
        ..sub_label(num, lang, LabelQualifier::None)
    }
}

fn derived_audio(clip: &str, pid: u16, num: u16, lang: &str, codec: &str) -> StreamLabel {
    StreamLabel {
        stream_id: Some(StreamId {
            clip_id: clip.into(),
            pid,
        }),
        ..audio_label(num, lang, codec, "")
    }
}

// Spec: a label that names its own stream is never reachable through the slot lookup
// (different coordinate systems).
#[test]
fn slot_lookup_never_returns_a_label_that_names_its_own_stream() {
    let labels = vec![
        derived_sub("00002", 0x1200, 1, "fra"),
        sub_label(1, "eng", LabelQualifier::Sdh),
    ];
    let found = label_at(&labels, StreamLabelType::Subtitle, 1).expect("slot 1 is vendor's");
    assert_eq!(found.language, "eng");
    assert_eq!(found.qualifier, LabelQualifier::Sdh);
}

// Spec: the derived floor does not vote on which title anchors the vendor list, and cannot
// VETO the title that does.
#[test]
fn the_derived_floor_cannot_veto_the_anchor() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::Sdh),
        sub_label(2, "fra", LabelQualifier::Forced),
        derived_sub("00050", 0x1202, 3, "spa"),
    ];
    let titles = vec![title_on_clip(
        "00800.mpls",
        "00800",
        vec![
            subtitle(0x12A0, "eng"),
            subtitle(0x12A1, "fra"),
            subtitle(0x12A2, "deu"),
        ],
    )];
    assert_eq!(
        find_anchor(&labels, &titles, StreamLabelType::Subtitle),
        Some(0),
        "the feature reproduces every slot the VENDOR named"
    );
}

// Spec: among titles the vendor list admits, the one that CONFIRMS more of it wins —
// silence is not evidence, size alone must not win.
#[test]
fn the_anchor_is_the_title_that_confirms_most_of_the_list() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::Sdh),
        sub_label(2, "fra", LabelQualifier::Forced),
    ];
    let titles = vec![
        // Longer, but says nothing: compatible with anything, confirms none.
        title_on_clip(
            "00050.mpls",
            "00050",
            vec![
                subtitle(0x1200, ""),
                subtitle(0x1201, ""),
                subtitle(0x1202, ""),
            ],
        ),
        // Shorter, but reproduces the list.
        title_on_clip(
            "00800.mpls",
            "00800",
            vec![subtitle(0x12A0, "eng"), subtitle(0x12A1, "fra")],
        ),
    ];
    assert_eq!(
        find_anchor(&labels, &titles, StreamLabelType::Subtitle),
        Some(1)
    );
}

// Spec: a vendor qualifier does not leak via ordinal fallback onto a different physical
// stream (same ordinal, different clip).
#[test]
fn a_vendor_qualifier_does_not_leak_onto_a_featurettes_own_stream() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::Sdh),
        sub_label(2, "fra", LabelQualifier::None),
        // The floor names the featurette's single subtitle.
        derived_sub("00076", 0x1200, 1, "eng"),
    ];
    let mut titles = vec![
        title_on_clip(
            "00800.mpls",
            "00082",
            vec![subtitle(0x12A0, "eng"), subtitle(0x12A1, "fra")],
        ),
        title_on_clip("00451.mpls", "00076", vec![subtitle(0x1200, "eng")]),
    ];
    apply_labels(&labels, &mut titles);
    assert_eq!(
        sub_state(&titles[0]),
        vec![
            (0x12A0, false, LabelQualifier::Sdh),
            (0x12A1, false, LabelQualifier::None)
        ],
        "the feature IS the table the list describes and keeps its SDH"
    );
    assert_eq!(
        sub_state(&titles[1]),
        vec![(0x1200, false, LabelQualifier::None)],
        "the featurette's own stream is not the feature's subtitle 1"
    );
}

/// A title that plays SEVERAL clips in order — the shape the anchor's
/// PID facts are harvested from.
fn title_on_clips(playlist: &str, clip_ids: &[&str], streams: Vec<Stream>) -> DiscTitle {
    DiscTitle {
        playlist: playlist.into(),
        clips: clip_ids
            .iter()
            .map(|id| crate::disc::Clip {
                feed_span: None,
                clip_id: (*id).into(),
                in_time: 0,
                out_time: 0,
                duration_secs: 3600.0,
                source_packets: 0,
            })
            .collect(),
        ..title_with(streams)
    }
}

// Spec: the anchor proves a (clip, PID) fact only for the clip its stream table was READ
// FROM (the first play item), never every clip it plays.
#[test]
fn an_anchor_proves_pids_only_for_the_clip_its_table_came_from() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::Sdh),
        sub_label(2, "fra", LabelQualifier::None),
    ];
    let mut titles = vec![
        // The anchor: its stream table is clip 00082's (the first play
        // item); it merely CONTINUES into 00090.
        title_on_clips(
            "00800.mpls",
            &["00082", "00090"],
            vec![subtitle(0x1200, "eng"), subtitle(0x1201, "fra")],
        ),
        // Sibling playlist over the anchor's SECOND clip; reuses PID 0x1200
        // (PIDs are only unique within a clip) but is Spanish, so the
        // anchor's English SDH slot doesn't describe it.
        title_on_clips("00451.mpls", &["00090"], vec![subtitle(0x1200, "spa")]),
    ];
    apply_labels(&labels, &mut titles);

    assert_eq!(
        sub_state(&titles[0]),
        vec![
            (0x1200, false, LabelQualifier::Sdh),
            (0x1201, false, LabelQualifier::None)
        ],
        "the anchor itself keeps the qualifiers the list states for it"
    );
    assert_eq!(
        sub_state(&titles[1]),
        vec![(0x1200, false, LabelQualifier::None)],
        "a PID in a clip the anchor's table never described is not that \
             table's stream 1"
    );
}

// Spec: an anchor PID fact reaches a title only through that title's FIRST clip (its
// stream table's clip), not any later clip it merely plays.
#[test]
fn an_anchor_pid_fact_does_not_reach_a_title_through_a_later_clip() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::Sdh),
        sub_label(2, "fra", LabelQualifier::None),
    ];
    let mut titles = vec![
        title_on_clips(
            "00800.mpls",
            &["00082", "00090"],
            vec![subtitle(0x1200, "eng"), subtitle(0x1201, "fra")],
        ),
        // Own table is 00050's; it only continues into the anchor's clip 00082.
        title_on_clips(
            "00451.mpls",
            &["00050", "00082"],
            vec![subtitle(0x1200, "spa")],
        ),
    ];
    apply_labels(&labels, &mut titles);
    assert_eq!(
        sub_state(&titles[1]),
        vec![(0x1200, false, LabelQualifier::None)]
    );
}

// Spec: a label list whose languages are all empty confirms nothing, so it anchors to no title.
#[test]
fn an_all_empty_language_list_anchors_to_no_title() {
    let labels = vec![
        sub_label(1, "", LabelQualifier::None),
        sub_label(2, "", LabelQualifier::None),
    ];
    let titles = vec![
        title_on_clips(
            "00800.mpls",
            &["00001"],
            vec![subtitle(0x1200, "eng"), subtitle(0x1201, "fra")],
        ),
        title_on_clips(
            "00801.mpls",
            &["00002"],
            vec![
                subtitle(0x1200, "eng"),
                subtitle(0x1201, "fra"),
                subtitle(0x1202, "spa"),
            ],
        ),
    ];
    assert_eq!(
        find_anchor(&labels, &titles, StreamLabelType::Subtitle),
        None
    );
}

// Spec: ISO 639-2/B and /T spellings of one language agree.
#[test]
fn language_b_and_t_codes_agree() {
    assert!(languages_compatible("fre", "fra"));
    assert!(languages_agree("ger", "deu"));
    assert!(languages_agree("CHI", "zho"));
    assert!(!languages_agree("fre", "deu"));
    assert!(!languages_compatible("fre", "deu"));
}

// Spec: an Atmos hint that names the OTHER carrier is a mis-bind.
#[test]
fn atmos_hint_naming_the_other_carrier_is_inconsistent() {
    assert!(!codec_hint_consistent(
        "Dolby Digital Plus Atmos",
        &Codec::TrueHd
    ));
    assert!(codec_hint_consistent(
        "Dolby Digital Plus Atmos",
        &Codec::Ac3Plus
    ));
    assert!(!codec_hint_consistent(
        "Dolby TrueHD Atmos",
        &Codec::Ac3Plus
    ));
    assert!(codec_hint_consistent("Dolby TrueHD Atmos", &Codec::TrueHd));
    assert!(codec_hint_consistent("Atmos", &Codec::TrueHd));
}

// Spec: a vendor codec/variant claim does not follow the ordinal onto a bonus clip that
// carries a different codec.
#[test]
fn a_vendor_codec_claim_does_not_follow_the_ordinal_onto_a_bonus_clip() {
    // This framework states no codec_hint and puts its descriptor in `name`,
    // which `apply_labels` falls back to verbatim, so the codec-consistency
    // guard never runs and cannot catch the mis-binding.
    let feature_audio = StreamLabel {
        name: "English Dolby Atmos".into(),
        ..audio_label(1, "eng", "", "")
    };
    let labels = vec![
        feature_audio,
        audio_label(2, "eng", "", ""),
        derived_audio("00020", 0x1100, 1, "eng", "AC-3"),
    ];
    let mut titles = vec![
        title_on_clip(
            "00001.mpls",
            "00000",
            vec![
                audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
                audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "eng"),
            ],
        ),
        title_on_clip(
            "00301.mpls",
            "00020",
            vec![audio(0x1100, Codec::Ac3, AudioChannels::Stereo, "eng")],
        ),
    ];
    apply_labels(&labels, &mut titles);
    let label_of = |t: &DiscTitle, i: usize| match &t.streams[i] {
        Stream::Audio(a) => a.label.clone(),
        _ => unreachable!(),
    };
    assert_eq!(
        label_of(&titles[0], 0),
        "English Dolby Atmos",
        "the feature keeps the descriptor its own list states"
    );
    assert_eq!(
        label_of(&titles[1], 0),
        "Dolby Digital 2.0",
        "the bonus clip describes the stream it actually carries"
    );
}

// Spec: a title whose stream count is smaller than the vendor list's highest slot cannot be
// the table that list describes.
#[test]
fn a_title_shorter_than_the_vendor_list_cannot_anchor_it() {
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::None),
        sub_label(2, "fra", LabelQualifier::None),
        sub_label(3, "deu", LabelQualifier::Forced),
    ];
    let titles = vec![
        // Two streams, both confirming their slot: the strongest evidence
        // on offer, and still not a table with a third slot in it.
        title_on_clip(
            "00050.mpls",
            "00050",
            vec![subtitle(0x1200, "eng"), subtitle(0x1201, "fra")],
        ),
        // Three streams, only the first stating a language — weaker
        // evidence, but it is the only shape the list can be describing.
        title_on_clip(
            "00800.mpls",
            "00800",
            vec![
                subtitle(0x12A0, "eng"),
                subtitle(0x12A1, ""),
                subtitle(0x12A2, ""),
            ],
        ),
    ];
    assert_eq!(
        find_anchor(&labels, &titles, StreamLabelType::Subtitle),
        Some(1),
        "only the three-stream title can hold a list whose top slot is 3"
    );
}

// Spec: a slot the vendor list never names constrains nothing (these blobs under-yield by
// design).
#[test]
fn an_unnamed_slot_does_not_disqualify_a_title() {
    // The vendor names slots 1 and 3 only; slot 2 is its silence.
    let labels = vec![
        sub_label(1, "eng", LabelQualifier::Sdh),
        sub_label(3, "fra", LabelQualifier::Forced),
    ];
    let mut titles = vec![title_on_clip(
        "00800.mpls",
        "00800",
        vec![
            subtitle(0x12A0, "eng"),
            subtitle(0x12A1, "deu"),
            subtitle(0x12A2, "fra"),
        ],
    )];
    assert_eq!(
        find_anchor(&labels, &titles, StreamLabelType::Subtitle),
        Some(0)
    );
    apply_labels(&labels, &mut titles);
    assert_eq!(
        sub_state(&titles[0]),
        vec![
            (0x12A0, false, LabelQualifier::Sdh),
            (0x12A1, false, LabelQualifier::None),
            (0x12A2, true, LabelQualifier::Forced),
        ]
    );
}
