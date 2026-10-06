use super::*;
use crate::disc::Chapter;

fn chaps() -> Vec<Chapter> {
    vec![
        Chapter {
            time_secs: 0.0,
            name: "1".into(),
        },
        Chapter {
            time_secs: 62.5,
            name: "2".into(),
        },
    ]
}

fn cue_times(chapters: &[(f64, &str)]) -> Vec<String> {
    let chapters: Vec<_> = chapters
        .iter()
        .map(|&(t, n)| Chapter {
            time_secs: t,
            name: n.into(),
        })
        .collect();
    chapters_vtt(&chapters)
        .lines()
        .filter(|l| l.contains(" --> "))
        .map(str::to_string)
        .collect()
}

#[test]
fn a_vtt_cue_always_ends_after_it_starts() {
    assert_eq!(
        cue_times(&[(10.0, "1"), (10.0, "2")]),
        [
            "00:00:10.000 --> 00:00:11.000",
            "00:00:10.000 --> 00:00:11.000"
        ]
    );
    assert_eq!(
        cue_times(&[(12.0, "1"), (5.0, "2"), (20.0, "3")]),
        [
            "00:00:12.000 --> 00:00:13.000",
            "00:00:05.000 --> 00:00:20.000",
            "00:00:20.000 --> 00:00:21.000"
        ]
    );
}

#[test]
fn a_vtt_chapter_name_cannot_forge_a_cue() {
    let vtt = chapters_vtt(&[Chapter {
        time_secs: 0.0,
        name: "a & <b>\n\n9\n00:00:01.000 --> 00:00:02.000\nforged".into(),
    }]);
    assert_eq!(vtt.matches(" --> ").count(), 1, "{vtt}");
    assert!(vtt.contains("a &amp; &lt;b&gt;"), "{vtt}");
}

#[test]
fn vtt_times_carry_hours_minutes_and_seconds() {
    assert_eq!(vtt_time(3725.5), "01:02:05.500");
    assert_eq!(vtt_time(36_000.0), "10:00:00.000");
}

// json:// marks the MVC dependent view and only that.
#[test]
fn json_marks_the_mvc_dependent_view() {
    use crate::disc::{
        Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, MVC_DEPENDENT_LABEL, Resolution,
        Stream as DiscStream, VideoStream,
    };
    let video = |label: &str| {
        DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: label.into(),
            measured_cicp: None,
        })
    };
    let mut t = DiscTitle::empty();
    t.streams = vec![video(""), video(MVC_DEPENDENT_LABEL)];
    let v = title_json(&t);
    assert_eq!(v["video"][0]["mvc"], false);
    assert_eq!(v["video"][1]["mvc"], true);
}

#[test]
fn chapters_format_selected_by_extension() {
    let xml = chapters_content(&chaps(), Some("xml"));
    assert!(xml.contains("<Chapters>"), "xml chosen for .xml");
    let ogm = chapters_content(&chaps(), Some("txt"));
    assert!(ogm.contains("CHAPTER01="), "ogm chosen for .txt");
    let vtt = chapters_content(&chaps(), Some("vtt"));
    assert!(
        vtt.starts_with("WEBVTT") && vtt.contains("00:01:02.500"),
        "vtt chosen for .vtt, with cue timing"
    );
    // Unknown / missing extension defaults to XML.
    assert!(chapters_content(&chaps(), None).contains("<Chapters>"));
}

#[test]
fn title_json_carries_streams_and_chapters() {
    use crate::disc::{AudioChannels, AudioStream, Codec, DiscTitle};
    use crate::disc::{LabelPurpose, SampleRate, Stream as DiscStream};
    let mut t = DiscTitle::empty();
    t.playlist = "MAIN".into();
    t.chapters = chaps();
    t.streams = vec![DiscStream::Audio(AudioStream {
        pid: 0x1100,
        codec: Codec::TrueHd,
        channels: AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })];
    let v = title_json(&t);
    assert_eq!(v["playlist"], "MAIN");
    // Normalized profile schema: streams split into typed arrays.
    let a = &v["audio"][0];
    assert_eq!(a["codec"], "truehd");
    assert_eq!(a["language"], "eng");
    assert_eq!(a["channels"], "stereo");
    // Editorial purpose is decomposed into booleans.
    assert!(!a["commentary"].as_bool().unwrap());
    assert!(!a["descriptive"].as_bool().unwrap());
    // First non-secondary audio is the hoisted default.
    assert!(a["default"].as_bool().unwrap());
    // Chapter COUNT from the profile; full list under chapter_marks.
    assert_eq!(v["chapters"], 2);
    assert_eq!(v["chapter_marks"][1]["n"], 2);
    assert_eq!(v["chapter_marks"][1]["start_secs"], 62.5);
    assert_eq!(v["chapter_marks"][1]["name"], "2");
}

/// An audio stream whose channel layout is genuinely unknown reports the
/// honest `"unknown"` string (the normalized profile carries the layout label,
/// not a fabricated numeric channel count).
#[test]
fn unknown_audio_layout_reports_unknown_channels() {
    use crate::disc::{AudioChannels, AudioStream, Codec, DiscTitle};
    use crate::disc::{LabelPurpose, SampleRate, Stream as DiscStream};
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStream::Audio(AudioStream {
        pid: 0x1100,
        codec: Codec::DtsHdMa,
        channels: AudioChannels::Unknown,
        language: "eng".into(),
        sample_rate: SampleRate::Unknown,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })];
    let v = title_json(&t);
    let a = &v["audio"][0];
    // Unknown layout must serialize as the string "unknown" — never a
    // fabricated numeric layout. (The schema has no separate channel_count
    // field; `channels` is the only channel signal, so this is the guard.)
    assert_eq!(a["channels"], "unknown");
}

#[test]
fn video_json_carries_resolution_and_hdr() {
    use crate::disc::Codec;
    use crate::disc::{
        ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStream, VideoStream,
    };
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStream::Video(VideoStream {
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
    })];
    let vid = &title_json(&t)["video"][0];
    assert_eq!(vid["codec"], "hevc");
    assert_eq!(vid["resolution"], "2160p");
    assert_eq!(vid["frame_rate"], "23.976");
    assert_eq!(vid["hdr"], "hdr10");
    assert!(vid["default"].as_bool().unwrap());
}

/// The json:// document must carry the per-stream detail the normalized
/// `TitleProfile` drops: video pid/color_space/measured_cicp/aspect/w/h/
/// interlaced/mvc, audio pid/secondary/sample_rate/purpose, subtitle pid/
/// descriptive_service. Before the fix these were absent (profile had no such
/// fields), so each index read would be `Null` and every assertion below fail.
#[test]
fn json_restores_dropped_stream_fields_from_title() {
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat,
        LabelPurpose, LabelQualifier, MeasuredCicp, Resolution, SampleRate, Stream as DiscStream,
        SubtitleStream, VideoStream,
    };
    use serde_json::json;
    let mut t = DiscTitle::empty();
    t.streams = vec![
        DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i, // interlaced, 720x576
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: Some((16, 9)),
            secondary: false,
            label: String::new(),
            measured_cicp: Some(MeasuredCicp {
                matrix: 5,
                transfer: 6,
                primaries: 5,
                range: 1,
            }),
        }),
        DiscStream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: true,
            purpose: LabelPurpose::Commentary,
            label: String::new(),
        }),
        DiscStream::Subtitle(SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: LabelQualifier::DescriptiveService,
            codec_data: None,
        }),
    ];
    let v = title_json(&t);

    let vid = &v["video"][0];
    assert_eq!(vid["pid"], 0x1011);
    assert_eq!(vid["color_space"], "bt470bg");
    assert_eq!(vid["display_aspect"], json!([16, 9]));
    assert_eq!(vid["measured_cicp"]["matrix"], 5);
    assert_eq!(vid["measured_cicp"]["transfer"], 6);
    assert_eq!(vid["measured_cicp"]["primaries"], 5);
    assert_eq!(vid["measured_cicp"]["range"], 1);
    assert_eq!(vid["width"], 720);
    assert_eq!(vid["height"], 576);
    assert_eq!(vid["interlaced"], true);
    assert_eq!(vid["mvc"], false);

    let a = &v["audio"][0];
    assert_eq!(a["pid"], 0x1100);
    assert_eq!(a["secondary"], true);
    assert_eq!(a["sample_rate"], 48_000);
    assert_eq!(a["sample_rates"], serde_json::json!([48_000]));
    assert_eq!(a["purpose"], "commentary");

    let s = &v["subtitles"][0];
    assert_eq!(s["pid"], 0x1200);
    assert_eq!(s["descriptive_service"], true);
}

// A document missing the per-kind arrays must not panic the enrichment.
#[test]
fn enriching_a_document_without_stream_arrays_does_not_panic() {
    let mut t = DiscTitle::empty();
    t.streams = vec![crate::disc::Stream::Audio(crate::disc::AudioStream {
        pid: 0x1100,
        codec: crate::disc::Codec::Ac3,
        channels: crate::disc::AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: crate::disc::SampleRate::Unknown,
        secondary: false,
        purpose: crate::disc::LabelPurpose::Normal,
        label: String::new(),
    })];
    let mut doc = serde_json::json!({});
    enrich_streams(&mut doc, &t);
    assert_eq!(
        title_json(&t)["audio"][0]["sample_rate"],
        serde_json::Value::Null
    );
}

#[test]
fn combo_sample_rates_keep_both_rates() {
    use crate::disc::SampleRate;
    assert_eq!(sample_rates(SampleRate::S48_96), vec![48_000, 96_000]);
    assert_eq!(sample_rates(SampleRate::S48_192), vec![48_000, 192_000]);
    assert!(sample_rates(SampleRate::Unknown).is_empty());
}

/// Two streams of each kind, interleaved: every per-kind cursor must advance,
/// or the second stream's fields land on the first's entry.
#[test]
fn json_per_kind_cursors_and_title_fields() {
    use crate::disc::{
        AudioChannels, AudioStream, Clip, Codec, ColorSpace, ContentFormat, DiscTitle, FrameRate,
        HdrFormat, LabelPurpose, LabelQualifier, Resolution, SampleRate, Stream as DiscStream,
        SubtitleStream, VideoStream,
    };
    let vid = |pid| {
        DiscStream::Video(VideoStream {
            pid,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    };
    let aud = |pid| {
        DiscStream::Audio(AudioStream {
            pid,
            codec: Codec::Ac3,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    };
    let sub = |pid| {
        DiscStream::Subtitle(SubtitleStream {
            pid,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    };
    let mut t = DiscTitle::empty();
    t.streams = vec![
        aud(0x1100),
        vid(0x1011),
        aud(0x1101),
        sub(0x1200),
        sub(0x1201),
        vid(0x1012),
    ];
    t.content_format = ContentFormat::BdTs;
    t.playlist_id = 801;
    t.clips = vec![Clip {
        feed_span: None,
        clip_id: "00007".into(),
        in_time: 0,
        out_time: 45_000,
        duration_secs: 1.0,
        source_packets: 42,
    }];
    let v = title_json(&t);
    for (kind, pids) in [
        ("video", [0x1011, 0x1012]),
        ("audio", [0x1100, 0x1101]),
        ("subtitles", [0x1200, 0x1201]),
    ] {
        assert_eq!(v[kind][0]["pid"], pids[0], "{kind}[0]");
        assert_eq!(v[kind][1]["pid"], pids[1], "{kind}[1]");
    }
    assert_eq!(v["format"], "BdTs");
    assert_eq!(v["playlist_id"], 801);
    assert_eq!(v["clips"][0]["clip_id"], "00007");
    assert_eq!(v["clips"][0]["source_packets"], 42);
    assert_eq!(v["clips"][0]["duration_secs"], 1.0);
}

/// create() output read back: the extension selects the chapter format and
/// the JSON file holds the title document.
#[test]
fn chapters_and_json_create_write_readable_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = DiscTitle::empty();
    t.chapters = chaps();
    t.playlist = "MAIN".into();
    for (name, marker) in [
        ("c.txt", "CHAPTER01="),
        ("c.VTT", "WEBVTT"),
        ("c.xml", "<Chapters>"),
    ] {
        let path = dir.path().join(name);
        ChaptersSink::create(&path, &t).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains(marker), "{name}: {text}");
    }
    let path = dir.path().join("t.json");
    JsonSink::create(&path, &t).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(v["playlist"], "MAIN");
    assert_eq!(v["chapter_marks"][1]["n"], 2);
}
