use super::*;
use crate::disc::{
    AudioChannels, AudioStream, ColorSpace, FrameRate, HdrFormat, LabelPurpose, LabelQualifier,
    Resolution, SampleRate, SubtitleStream, VideoStream,
};
use crate::mux::resolve::parse_url;

fn video(codec: Codec) -> Stream {
    Stream::Video(VideoStream {
        pid: 0x1011,
        codec,
        resolution: Resolution::R1080p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt709,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })
}
fn audio(pid: u16, codec: Codec, rate: SampleRate, label: &str) -> Stream {
    Stream::Audio(AudioStream {
        pid,
        codec,
        channels: AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: rate,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: label.into(),
    })
}
fn pgs() -> Stream {
    Stream::Subtitle(SubtitleStream {
        pid: 0x1200,
        codec: Codec::Pgs,
        language: "eng".into(),
        forced: false,
        qualifier: LabelQualifier::None,
        codec_data: None,
    })
}
fn title(streams: Vec<Stream>) -> DiscTitle {
    DiscTitle {
        codec_privates: vec![None; streams.len()],
        streams,
        ..DiscTitle::empty()
    }
}
fn ext() -> Stream {
    audio(
        0x00D0,
        Codec::Mp2,
        SampleRate::S48,
        crate::disc::MP2_EXTENSION_LABEL,
    )
}

/// Every scheme `output()` can open, as a destination URL.
const OUTPUTS: &[&str] = &[
    "mkv:///o/x.mkv",
    "mp4:///o/x.mp4",
    "m2ts:///o/x.m2ts",
    "demux:///o/",
    "video:///o/",
    "audio:///o/",
    "sub:///o/",
    "network://127.0.0.1:9",
    "stdio://",
    "null://",
    "json:///o/x.json",
    "chapters:///o/x.xml",
    "fvi:///o/x.fvi",
];

// J23: "the generic pre-mux fit report leaves out declared-only extension tracks".
// Per design; do not change without a design citation proving otherwise.
#[test]
fn a_declared_only_mp2_extension_is_never_planned_out() {
    let t = title(vec![
        video(Codec::Mpeg2),
        audio(0x00C0, Codec::Mp2, SampleRate::S48, ""),
        ext(),
    ]);
    for url in OUTPUTS {
        let r = fit_report(&parse_url(url), &t);
        assert!(
            r.skipped.iter().all(|&(i, _)| i != 2) && !r.included.contains(&2),
            "{url}: a declared-only extension is in neither half: {r:?}"
        );
    }
}

#[test]
fn every_other_stream_is_either_included_or_skipped_exactly_once() {
    let t = title(vec![
        video(Codec::Hevc),
        audio(0x1100, Codec::TrueHd, SampleRate::S48, ""),
        audio(0x1101, Codec::Ac3, SampleRate::S48, ""),
        pgs(),
    ]);
    let class_sinks = ["video:///o/", "audio:///o/", "sub:///o/"];
    for url in OUTPUTS.iter().filter(|u| !class_sinks.contains(u)) {
        let r = fit_report(&parse_url(url), &t);
        let mut seen: Vec<usize> = r.included.clone();
        seen.extend(r.skipped.iter().map(|&(i, _)| i));
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3], "{url}: {r:?}");
    }
}

#[test]
fn mp4_is_the_mp4_plan_through_the_wrapper() {
    let t = title(vec![
        video(Codec::Hevc),
        audio(0x1100, Codec::TrueHd, SampleRate::S48, ""),
        audio(0x1101, Codec::Ac3, SampleRate::S48, ""),
        pgs(),
    ]);
    let mp4 = super::super::mp4::fit_report(&t);
    let r = fit_report(&parse_url("mp4:///o/x.mp4"), &t);
    assert_eq!(r.included, mp4.included);
    let want: Vec<(usize, SkipReason)> = mp4.skipped.iter().map(|&(i, s)| (i, s.into())).collect();
    assert_eq!(r.skipped, want);
    assert_eq!(
        r.skipped,
        vec![
            (1, SkipReason::UnmappableAudio),
            (3, SkipReason::BitmapSubtitle)
        ]
    );
}

// m2ts refuses LPCM that BD LPCM cannot carry at create (`M2tsStream::create`); the
// plan predicts it with the same predicate.
#[test]
fn m2ts_plans_out_lpcm_bd_lpcm_cannot_carry() {
    let t = title(vec![
        video(Codec::H264),
        audio(0x1100, Codec::Lpcm, SampleRate::S44_1, ""),
        audio(0x1101, Codec::Lpcm, SampleRate::S48, ""),
    ]);
    let r = fit_report(&parse_url("m2ts:///o/x.m2ts"), &t);
    assert_eq!(r.included, vec![0, 2]);
    assert_eq!(r.skipped, vec![(1, SkipReason::UnmappableAudio)]);
}

// AAC needs an ADTS-signallable ASC to be re-framed for the TS (HE-AAC signals its core).
#[test]
fn m2ts_plans_out_aac_adts_cannot_signal() {
    let mut t = title(vec![
        video(Codec::H264),
        audio(0x1100, Codec::Aac, SampleRate::S48, ""),
        audio(0x1101, Codec::Aac, SampleRate::S48, ""),
        audio(0x1102, Codec::Aac, SampleRate::S48, ""),
        audio(0x1103, Codec::Aac, SampleRate::S48, ""),
    ]);
    // LC, HE-AAC, no ASC, and LC with 960-sample frames (frameLengthFlag).
    t.codec_privates = vec![
        None,
        Some(vec![0x11, 0x90]),
        Some(vec![0x2B, 0x92, 0x08]),
        None,
        Some(vec![0x11, 0x94]),
    ];
    let r = fit_report(&parse_url("m2ts:///o/x.m2ts"), &t);
    assert_eq!(r.included, vec![0, 1, 2]);
    let skip = SkipReason::UnmappableAudio;
    assert_eq!(r.skipped, vec![(3, skip), (4, skip)]);
}

// A class sink writes only its class by the user's choice: the rest is not a loss,
// so it is neither included nor skipped.
#[test]
fn class_sinks_include_only_their_class_and_skip_nothing() {
    let t = title(vec![
        video(Codec::Hevc),
        audio(0x1100, Codec::TrueHd, SampleRate::S48, ""),
        pgs(),
    ]);
    for (url, want) in [("video:///o/", 0), ("audio:///o/", 1), ("sub:///o/", 2)] {
        let r = fit_report(&parse_url(url), &t);
        assert_eq!(r.included, vec![want], "{url}");
        assert!(r.skipped.is_empty(), "{url}");
    }
}

#[test]
fn mkv_and_the_wire_carry_everything() {
    let t = title(vec![
        video(Codec::Vc1),
        audio(0x1100, Codec::Lpcm, SampleRate::S44_1, ""),
        pgs(),
    ]);
    for url in ["mkv:///o/x.mkv", "network://127.0.0.1:9", "stdio://"] {
        let r = fit_report(&parse_url(url), &t);
        assert_eq!(r.included, vec![0, 1, 2], "{url}");
        assert!(r.skipped.is_empty(), "{url}");
    }
}

// A codec with no Matroska CodecID (text subs from an FMKV header, Unknown from a
// read-back) is planned out for mkv://, never declared under another codec's ID.
#[test]
fn mkv_plans_out_codecs_matroska_cannot_name() {
    let mut srt = pgs();
    if let Stream::Subtitle(s) = &mut srt {
        s.codec = Codec::Srt;
    }
    let t = title(vec![
        video(Codec::Unknown(0)),
        audio(0x1100, Codec::Unknown(0), SampleRate::S48, ""),
        audio(0x1101, Codec::Ac3, SampleRate::S48, ""),
        srt,
        pgs(),
    ]);
    let r = fit_report(&parse_url("mkv:///o/x.mkv"), &t);
    assert_eq!(r.included, vec![2, 4]);
    assert_eq!(
        r.skipped,
        vec![
            (0, SkipReason::UnmappableVideo),
            (1, SkipReason::UnmappableAudio),
            (3, SkipReason::UnmappableSubtitle),
        ]
    );
}
