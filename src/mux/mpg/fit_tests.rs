use super::*;
use crate::disc::{
    AudioChannels, AudioStream, ColorSpace, FrameRate, HdrFormat, LabelPurpose, LabelQualifier,
    Resolution, SampleRate, SubtitleStream, VideoStream,
};

fn video(codec: Codec) -> Stream {
    Stream::Video(VideoStream {
        pid: 0xE0,
        codec,
        resolution: Resolution::R480i,
        frame_rate: FrameRate::F29_97,
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
fn sub(pid: u16, codec: Codec) -> Stream {
    Stream::Subtitle(SubtitleStream {
        pid,
        codec,
        language: "eng".into(),
        forced: false,
        qualifier: LabelQualifier::None,
        codec_data: None,
    })
}
fn title(streams: Vec<Stream>) -> DiscTitle {
    DiscTitle {
        streams,
        ..DiscTitle::empty()
    }
}

#[test]
fn a_dvd_title_keeps_its_source_ids() {
    let t = title(vec![
        video(Codec::Mpeg2),
        audio(0x00C1, Codec::Mp2, SampleRate::S48, ""),
        audio(
            0x00D1,
            Codec::Mp2,
            SampleRate::S48,
            crate::disc::MP2_EXTENSION_LABEL,
        ),
        audio(0xBD81, Codec::Ac3, SampleRate::S48, ""),
        audio(0xBD89, Codec::Dts, SampleRate::S48, ""),
        audio(0xBDA2, Codec::Lpcm, SampleRate::S48, ""),
        sub(0x0025, Codec::DvdSub),
    ]);
    let p = plan(&t);
    assert_eq!(p.report.included, vec![0, 1, 2, 3, 4, 5, 6]);
    assert!(p.report.skipped.is_empty());
    assert_eq!(p.carriage[1], Some(Carriage::MpegAudio { stream_id: 0xC1 }));
    assert_eq!(
        p.carriage[2],
        Some(Carriage::Mp2Extension {
            stream_id: 0xD1,
            base: 1
        })
    );
    assert!(matches!(
        p.carriage[3],
        Some(Carriage::Private {
            sub_id: 0x81,
            kind: PrivateKind::Ac3
        })
    ));
    assert!(matches!(
        p.carriage[4],
        Some(Carriage::Private {
            sub_id: 0x89,
            kind: PrivateKind::Dts
        })
    ));
    assert!(matches!(
        p.carriage[5],
        Some(Carriage::Private {
            sub_id: 0xA2,
            kind: PrivateKind::Lpcm {
                channels: 2,
                rate: 48_000
            }
        })
    ));
    assert!(matches!(
        p.carriage[6],
        Some(Carriage::Private {
            sub_id: 0x25,
            kind: PrivateKind::Subpicture
        })
    ));
}

// J24 (coordinator JUDGEMENT): H.264/HEVC/VC-1 go through the excluded-track note until
// the 2013+ H.222.0 text is sourced (F8). Per design; do not change without a citation.
#[test]
fn j24_newer_video_and_e_ac3_are_excluded_with_a_reason() {
    for codec in [Codec::H264, Codec::Hevc, Codec::Vc1, Codec::Av1] {
        let p = plan(&title(vec![video(codec)]));
        assert_eq!(
            p.report.skipped,
            vec![(0, SkipReason::UnmappableVideo)],
            "{codec:?}"
        );
    }
    let t = title(vec![
        video(Codec::Mpeg2),
        audio(0x1100, Codec::Ac3Plus, SampleRate::S48, ""),
        audio(0x1101, Codec::TrueHd, SampleRate::S48, ""),
        audio(0x1102, Codec::Lpcm, SampleRate::S192, ""),
        sub(0x1200, Codec::Pgs),
        sub(0x1201, Codec::Srt),
        video(Codec::Mpeg2),
    ]);
    assert_eq!(
        plan(&t).report.skipped,
        vec![
            (1, SkipReason::UnmappableAudio),
            (2, SkipReason::UnmappableAudio),
            (3, SkipReason::UnmappableAudio),
            (4, SkipReason::BitmapSubtitle),
            (5, SkipReason::UnmappableSubtitle),
            (6, SkipReason::SecondaryVideo),
        ]
    );
}

// Design §2.2: "Overflow → NoStreamId"; MPEG audio "0xC0–0xC7 … 8", AC-3 "8 each".
#[test]
fn a_full_range_is_no_stream_id() {
    let mut s = vec![video(Codec::Mpeg2)];
    s.extend((0..9).map(|i| audio(0x1100 + i, Codec::Ac3, SampleRate::S48, "")));
    s.extend((0..9).map(|i| audio(0x1200 + i, Codec::Mp2, SampleRate::S48, "")));
    let p = plan(&title(s));
    assert_eq!(
        p.report.skipped,
        vec![(9, SkipReason::NoStreamId), (18, SkipReason::NoStreamId)]
    );
    assert_eq!(
        p.carriage[1],
        Some(Carriage::Private {
            sub_id: 0x80,
            kind: PrivateKind::Ac3
        })
    );
    assert_eq!(
        p.carriage[10],
        Some(Carriage::MpegAudio { stream_id: 0xC0 })
    );
}

// Pass 1 keeps a source sub-id only inside the codec's own range and only once: an AC-3
// track under a DTS-range id is re-allocated, and a duplicate id takes the lowest free.
#[test]
fn source_sub_ids_out_of_range_or_duplicated_are_reallocated() {
    let t = title(vec![
        video(Codec::Mpeg2),
        audio(0xBD89, Codec::Ac3, SampleRate::S48, ""),
        audio(0xBD81, Codec::Ac3, SampleRate::S48, ""),
        audio(0xBD81, Codec::Ac3, SampleRate::S48, ""),
    ]);
    let p = plan(&t);
    let ac3 = |sub_id| {
        Some(Carriage::Private {
            sub_id,
            kind: PrivateKind::Ac3,
        })
    };
    assert_eq!(p.carriage[1], ac3(0x80), "0x89 is DTS range, not kept");
    assert_eq!(p.carriage[2], ac3(0x81), "first 0x81 keeps its id");
    assert_eq!(
        p.carriage[3],
        ac3(0x82),
        "duplicate 0x81 takes the lowest free id"
    );
}

// J23: a declared extension whose base is not carried is in neither half (seen-only).
#[test]
fn an_extension_without_a_carried_base_is_not_planned_out() {
    let t = title(vec![
        video(Codec::Mpeg2),
        audio(
            0x00D3,
            Codec::Mp2,
            SampleRate::S48,
            crate::disc::MP2_EXTENSION_LABEL,
        ),
    ]);
    let p = plan(&t);
    assert_eq!(p.report.included, vec![0]);
    assert!(p.report.skipped.is_empty());
}

#[test]
fn allocated_ids_avoid_kept_dvd_ids() {
    let t = title(vec![
        video(Codec::Mpeg2),
        audio(0x1100, Codec::Mp2, SampleRate::S48, ""),
        audio(0x00C0, Codec::Mp2, SampleRate::S48, ""),
    ]);
    let p = plan(&t);
    assert_eq!(p.carriage[1], Some(Carriage::MpegAudio { stream_id: 0xC1 }));
    assert_eq!(p.carriage[2], Some(Carriage::MpegAudio { stream_id: 0xC0 }));
}
