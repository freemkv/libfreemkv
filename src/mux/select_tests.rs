use super::*;
use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, LabelPurpose,
    LabelQualifier, Resolution, SampleRate, SubtitleStream, VideoStream,
};

fn video(pid: u16) -> Stream {
    Stream::Video(VideoStream {
        pid,
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
fn audio(pid: u16, lang: &str) -> Stream {
    Stream::Audio(AudioStream {
        pid,
        codec: Codec::TrueHd,
        channels: AudioChannels::Stereo,
        language: lang.into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })
}
fn subtitle(pid: u16, lang: &str) -> Stream {
    Stream::Subtitle(SubtitleStream {
        pid,
        codec: Codec::Pgs,
        language: lang.into(),
        forced: false,
        qualifier: LabelQualifier::None,
        codec_data: None,
    })
}

// video + 3 audio (eng/spa/fra) + 2 subs (eng/spa).
fn title() -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams = vec![
        video(0x1011),
        audio(0x1100, "eng"),
        audio(0x1101, "spa"),
        audio(0x1102, "fra"),
        subtitle(0x1200, "eng"),
        subtitle(0x1201, "spa"),
    ];
    t
}

fn pids(t: &DiscTitle) -> Vec<u16> {
    t.streams
        .iter()
        .map(|s| match s {
            Stream::Video(v) => v.pid,
            Stream::Audio(a) => a.pid,
            Stream::Subtitle(s) => s.pid,
        })
        .collect()
}

#[test]
fn apply_all_is_identity_and_untouched() {
    let sel = StreamSelection::default();
    assert!(sel.is_all());
    let mut t = title();
    let before = pids(&t);
    sel.apply(&mut t).unwrap();
    assert_eq!(pids(&t), before, "All/All must not change the stream list");
}

#[test]
fn apply_only_retains_listed_audio_pids_in_declared_order() {
    // Keep eng+fra audio (skip spa); leave subtitles alone.
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x1100, 0x1102]),
        subtitle: PidFilter::All,
    };
    let mut t = title();
    sel.apply(&mut t).unwrap();
    assert_eq!(
        pids(&t),
        vec![0x1011, 0x1100, 0x1102, 0x1200, 0x1201],
        "video + eng/fra audio (order preserved) + both subs"
    );
}

#[test]
fn apply_only_empty_yields_video_only() {
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![]),
        subtitle: PidFilter::Only(vec![]),
    };
    let mut t = title();
    sel.apply(&mut t).unwrap();
    assert_eq!(pids(&t), vec![0x1011], "only the video stream survives");
}

#[test]
fn apply_subtitle_filter_does_not_touch_audio() {
    let sel = StreamSelection {
        audio: PidFilter::All,
        subtitle: PidFilter::Only(vec![0x1200]),
    };
    let mut t = title();
    sel.apply(&mut t).unwrap();
    assert_eq!(
        pids(&t),
        vec![0x1011, 0x1100, 0x1101, 0x1102, 0x1200],
        "all audio kept, only eng subtitle kept"
    );
}

#[test]
fn apply_unknown_pid_errors_and_leaves_title_untouched() {
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x9999]),
        subtitle: PidFilter::All,
    };
    let mut t = title();
    let before = pids(&t);
    let err = sel.apply(&mut t).unwrap_err();
    assert!(matches!(err, Error::SelectionPidUnknown { pid: 0x9999 }));
    assert_eq!(pids(&t), before, "title unmodified on error");
}

#[test]
fn apply_prunes_codec_privates_in_lockstep_when_populated() {
    // A caller that pre-filled codec_privates parallel to streams: pruning
    // must keep the two vecs aligned.
    let mut t = title();
    t.codec_privates = vec![
        Some(vec![0xAA]), // video 0x1011
        Some(vec![0x11]), // audio 0x1100 eng
        Some(vec![0x22]), // audio 0x1101 spa
        Some(vec![0x33]), // audio 0x1102 fra
        None,             // sub 0x1200
        None,             // sub 0x1201
    ];
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x1100]),
        subtitle: PidFilter::Only(vec![]),
    };
    sel.apply(&mut t).unwrap();
    assert_eq!(pids(&t), vec![0x1011, 0x1100]);
    assert_eq!(
        t.codec_privates,
        vec![Some(vec![0xAA]), Some(vec![0x11])],
        "codec_privates pruned to match the retained streams, in order"
    );
}

// codec_privates longer than streams must still prune in lockstep by index (regression: a
// trailing extra entry once skipped the prune, misattaching codec-private to the wrong
// track).
#[test]
fn apply_prunes_codec_privates_even_when_length_does_not_match_streams() {
    let mut t = title();
    t.streams.truncate(4); // video + eng + spa + fra audio
    t.codec_privates = vec![
        Some(vec![0xAA]), // video 0x1011
        Some(vec![0x11]), // audio 0x1100 eng
        Some(vec![0x22]), // audio 0x1101 spa
        Some(vec![0x33]), // audio 0x1102 fra
        Some(vec![0xEE]), // trailing extra — describes no declared stream
    ];
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x1102]),
        subtitle: PidFilter::All,
    };
    sel.apply(&mut t).unwrap();

    assert_eq!(pids(&t), vec![0x1011, 0x1102], "video + fra audio");
    assert_eq!(
        t.codec_privates,
        vec![Some(vec![0xAA]), Some(vec![0x33])],
        "the retained fra track must keep ITS OWN codec_private, and the \
             trailing entry that describes no stream must not survive the prune"
    );
    assert_eq!(
        t.codec_privates.len(),
        t.streams.len(),
        "the two positional vecs must be aligned after apply()"
    );
}
// A PID in the wrong class's filter must fail loud, not silently vanish (validation once
// scanned both classes, letting it pass then get dropped by `keeps`).
#[test]
fn a_pid_listed_in_the_wrong_class_filter_is_rejected() {
    let mut t = title();
    let before = t.streams.len();

    // 0x1200 is a SUBTITLE pid, listed here in the AUDIO filter.
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x1200]),
        subtitle: PidFilter::All,
    };
    assert!(
        sel.apply(&mut t).is_err(),
        "a subtitle PID in the audio filter must be rejected"
    );
    assert_eq!(
        t.streams.len(),
        before,
        "a rejected selection must not prune"
    );

    // And the mirror case: an audio pid listed in the subtitle filter.
    let sel = StreamSelection {
        audio: PidFilter::All,
        subtitle: PidFilter::Only(vec![0x1100]),
    };
    assert!(
        sel.apply(&mut t).is_err(),
        "an audio PID in the subtitle filter must be rejected"
    );

    // Sanity: each PID in its OWN class still validates.
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x1100]),
        subtitle: PidFilter::Only(vec![0x1200]),
    };
    assert!(
        sel.apply(&mut t).is_ok(),
        "correctly-classed PIDs must apply"
    );
}

// DVD title: video, MP2 base 0xC0 + its extension 0xD0, MP2 0xC1.
fn mp2_ext_title() -> DiscTitle {
    let mp2 = |pid: u16, label: &str| {
        Stream::Audio(AudioStream {
            pid,
            codec: Codec::Mp2,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: label.into(),
        })
    };
    let mut t = DiscTitle::empty();
    t.streams = vec![
        video(0x00E0),
        mp2(0x00C0, ""),
        mp2(0x00D0, crate::disc::MP2_EXTENSION_LABEL),
        mp2(0x00C1, ""),
    ];
    t
}

/// An MPEG-2 extension track (`0xD0|n`) is kept iff its base (`0xC0|n`) is, whatever the
/// filter lists: it holds "a remainder of the multichannel ... information" (13818-3 2nd ed.
/// §2.5.2.13), meaningless alone. Per spec; do not change without a spec citation otherwise.
#[test]
fn an_mp2_extension_follows_its_base_whatever_the_filter_lists() {
    let keep = |audio: Vec<u16>| {
        let mut t = mp2_ext_title();
        StreamSelection {
            audio: PidFilter::Only(audio),
            subtitle: PidFilter::All,
        }
        .apply(&mut t)
        .unwrap();
        pids(&t)
    };
    assert_eq!(keep(vec![0x00C0]), vec![0x00E0, 0x00C0, 0x00D0]);
    assert_eq!(keep(vec![0x00C1]), vec![0x00E0, 0x00C1]);
    assert_eq!(keep(vec![0x00C0, 0x00D0]), vec![0x00E0, 0x00C0, 0x00D0]);
}

/// Listing an extension PID without its base pulls the base in (logged), never a silent
/// drop of what was asked for: the extension is "a remainder" of the base's multichannel
/// information (13818-3 2nd ed. §2.5.2.13), useless alone.
#[test]
fn an_mp2_extension_listed_alone_pulls_its_base_in() {
    let (kept, ev) = crate::testlog::capture(|| {
        let mut t = mp2_ext_title();
        StreamSelection {
            audio: PidFilter::Only(vec![0x00D0, 0x00C1]),
            subtitle: PidFilter::All,
        }
        .apply(&mut t)
        .unwrap();
        pids(&t)
    });
    assert_eq!(kept, vec![0x00E0, 0x00C0, 0x00D0, 0x00C1]);
    assert!(
        ev.iter()
            .any(|e| e.message().contains("0xc0") && e.message().contains("0xd0")),
        "the added base is logged"
    );
}

// The pairing is per index: extension 0xD1 belongs to base 0xC1, never to 0xC0.
#[test]
fn an_mp2_extension_pairs_with_the_base_of_its_own_index() {
    let mp2 = |pid: u16, label: &str| {
        Stream::Audio(AudioStream {
            pid,
            codec: Codec::Mp2,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: label.into(),
        })
    };
    let keep = |audio: Vec<u16>| {
        let mut t = DiscTitle::empty();
        t.streams = vec![
            video(0x00E0),
            mp2(0x00C0, ""),
            mp2(0x00C1, ""),
            mp2(0x00D1, crate::disc::MP2_EXTENSION_LABEL),
        ];
        StreamSelection {
            audio: PidFilter::Only(audio),
            subtitle: PidFilter::All,
        }
        .apply(&mut t)
        .unwrap();
        pids(&t)
    };
    assert_eq!(keep(vec![0x00C1]), vec![0x00E0, 0x00C1, 0x00D1]);
    assert_eq!(keep(vec![0x00C0]), vec![0x00E0, 0x00C0]);
    assert_eq!(keep(vec![0x00D1]), vec![0x00E0, 0x00C1, 0x00D1]);
}
