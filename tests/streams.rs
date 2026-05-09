//! Integration tests for the PES stream pipeline.

// 0.18 trait split: this suite still drives the deprecated `pes::Stream`
// trait directly. It will be migrated to `FrameSource`/`FrameSink` in the
// follow-up that ports concrete impls.
#![allow(deprecated)]

use libfreemkv::mux::meta::M2tsMeta;
use libfreemkv::pes::Stream as PesStream;
use libfreemkv::*;
use std::io::{Cursor, Write};

fn sample_disc_title() -> DiscTitle {
    DiscTitle {
        playlist: "Test Movie".into(),
        playlist_id: 0,
        duration_secs: 7200.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Hevc,
                resolution: Resolution::R2160p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Hdr10,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: "Main".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::TrueHd,
                channels: AudioChannels::Surround71,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: libfreemkv::LabelPurpose::Normal,
                label: "English Atmos".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1101,
                codec: Codec::Ac3,
                channels: AudioChannels::Surround51,
                language: "fra".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: libfreemkv::LabelPurpose::Normal,
                label: "French".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: false,
                qualifier: libfreemkv::LabelQualifier::None,
                codec_data: None,
            }),
        ],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    }
}

// ── URL parsing ────────────────────────────────────────────────

#[test]
fn parse_url_disc() {
    let u = parse_url("disc://");
    assert_eq!(u.scheme(), "disc");
    assert_eq!(u.path_str(), "");
}

#[test]
fn parse_url_disc_device() {
    let u = parse_url("disc:///dev/sg4");
    assert_eq!(u.scheme(), "disc");
    assert_eq!(u.path_str(), "/dev/sg4");
}

#[test]
fn parse_url_mkv() {
    let u = parse_url("mkv://Dune.mkv");
    assert_eq!(u.scheme(), "mkv");
    assert_eq!(u.path_str(), "Dune.mkv");
}

#[test]
fn parse_url_network() {
    let u = parse_url("network://10.1.7.11:9000");
    assert_eq!(u.scheme(), "network");
    assert_eq!(u.path_str(), "10.1.7.11:9000");
}

#[test]
fn parse_url_bare_path_rejected() {
    let u = parse_url("Dune.mkv");
    assert_eq!(u.scheme(), "unknown");
}

#[test]
fn parse_url_null() {
    let u = parse_url("null://");
    assert_eq!(u.scheme(), "null");
    assert_eq!(u.path_str(), "");
}

#[test]
fn parse_url_m2ts_with_path() {
    let u = parse_url("m2ts:///tmp/Dune.m2ts");
    assert_eq!(u.scheme(), "m2ts");
    assert_eq!(u.path_str(), "/tmp/Dune.m2ts");
}

#[test]
fn parse_url_m2ts_relative() {
    let u = parse_url("m2ts://Dune.m2ts");
    assert_eq!(u.scheme(), "m2ts");
    assert_eq!(u.path_str(), "Dune.m2ts");
}

#[test]
fn open_input_bare_path_errors() {
    let result = libfreemkv::input("Dune.mkv", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        msg.contains("not a valid stream URL") || msg.contains("E9002"),
        "got: {}",
        msg
    );
}

#[test]
fn open_output_bare_path_errors() {
    let dt = sample_disc_title();
    let result = libfreemkv::output("Dune.mkv", &dt);
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        msg.contains("not a valid stream URL") || msg.contains("E9002"),
        "got: {}",
        msg
    );
}

#[test]
fn open_input_m2ts_empty_path_errors() {
    let result = libfreemkv::input("m2ts://", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        msg.contains("requires a file path") || msg.contains("E9003"),
        "got: {}",
        msg
    );
}

#[test]
fn open_output_null_input_errors() {
    let result = libfreemkv::input("null://", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        msg.contains("write-only") || msg.contains("E9001"),
        "got: {}",
        msg
    );
}

#[test]
fn open_output_disc_errors() {
    let dt = sample_disc_title();
    let result = libfreemkv::output("disc://", &dt);
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        msg.contains("read-only") || msg.contains("E9000"),
        "got: {}",
        msg
    );
}

#[test]
fn open_input_network_no_port_errors() {
    let result = libfreemkv::input("network://10.0.0.1", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        msg.contains("PES pipeline") || msg.contains("missing port") || msg.contains("E9004"),
        "got: {}",
        msg
    );
}

#[test]
fn parse_url_stdio() {
    let u = parse_url("stdio://");
    assert_eq!(u.scheme(), "stdio");
    assert_eq!(u.path_str(), "");
}

// ── M2TS metadata roundtrip ───────────────────────────────────

#[test]
fn m2ts_meta_roundtrip() {
    let dt = sample_disc_title();
    let meta = M2tsMeta::from_title(&dt);
    let restored = meta.to_title();

    assert_eq!(restored.playlist, dt.playlist);
    assert_eq!(restored.duration_secs, dt.duration_secs);
    assert_eq!(restored.streams.len(), dt.streams.len());

    // Check video
    if let Stream::Video(v) = &restored.streams[0] {
        assert_eq!(v.codec, Codec::Hevc);
        assert_eq!(v.resolution, Resolution::R2160p);
        assert_eq!(v.label, "Main");
    } else {
        panic!("expected video");
    }

    // Check audio
    if let Stream::Audio(a) = &restored.streams[1] {
        assert_eq!(a.codec, Codec::TrueHd);
        assert_eq!(a.language, "eng");
        assert_eq!(a.label, "English Atmos");
    } else {
        panic!("expected audio");
    }

    // Check subtitle
    if let Stream::Subtitle(s) = &restored.streams[3] {
        assert_eq!(s.language, "eng");
        assert!(!s.forced);
    } else {
        panic!("expected subtitle");
    }
}

// ── M2TS header write + read ──────────────────────────────────

#[test]
fn m2ts_header_write_read() {
    let dt = sample_disc_title();
    let meta = M2tsMeta::from_title(&dt);

    // Write header to buffer
    let mut buf = Vec::new();
    libfreemkv::mux::meta::write_header(&mut buf, &meta).unwrap();

    // Verify magic
    assert_eq!(&buf[..4], b"FMKV");

    // Verify 192-byte alignment
    assert_eq!(buf.len() % 192, 0);

    // Read it back
    let mut cursor = Cursor::new(&buf);
    let read_back = libfreemkv::mux::meta::read_header(&mut cursor)
        .unwrap()
        .unwrap();
    assert_eq!(read_back.title, "Test Movie");
    assert_eq!(read_back.duration, 7200.0);
    assert_eq!(read_back.streams.len(), 4);

    // Cursor should be at end of header (192-aligned)
    assert_eq!(cursor.position() as usize, buf.len());
}

// ── M2tsStream write + read ───────────────────────────────────

#[test]
fn m2ts_stream_write_read() {
    let dt = sample_disc_title();

    // Write PES frames through M2tsStream to a Cursor
    let output = Cursor::new(Vec::new());
    let mut stream = M2tsStream::create(output, &dt).unwrap();

    // Write some PES frames
    for i in 0..5u8 {
        let frame = libfreemkv::pes::PesFrame {
            track: 0,
            pts: i as i64 * 1_000_000,
            keyframe: i == 0,
            data: vec![i; 100],
        };
        PesStream::write(&mut stream, &frame).unwrap();
    }
    PesStream::finish(&mut stream).unwrap();

    // Verify the info is correct
    let info = PesStream::info(&stream);
    assert_eq!(info.streams.len(), 4);
    assert_eq!(info.duration_secs, 7200.0);
}

// ── M2tsStream PES frame roundtrip ───────────────────────────

#[test]
fn m2ts_pes_frame_roundtrip() {
    // PesFrame serialize/deserialize roundtrip
    let frame = libfreemkv::pes::PesFrame {
        track: 2,
        pts: 1_234_567_890,
        keyframe: true,
        data: vec![0xDE; 200],
    };

    let mut buf = Vec::new();
    frame.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf);
    let restored = libfreemkv::pes::PesFrame::deserialize(&mut cursor)
        .unwrap()
        .unwrap();

    assert_eq!(restored.track, frame.track);
    assert_eq!(restored.pts, frame.pts);
    assert_eq!(restored.keyframe, frame.keyframe);
    assert_eq!(restored.data, frame.data);
}

// ── PES Stream trait ─────────────────────────────────────────

#[test]
fn m2ts_implements_pes_stream() {
    let dt = sample_disc_title();
    let output = Cursor::new(Vec::new());
    let stream = M2tsStream::create(output, &dt).unwrap();

    let boxed: Box<dyn PesStream> = Box::new(stream);
    let meta = boxed.info();
    assert_eq!(meta.streams.len(), 4);
}

#[test]
fn m2ts_read_returns_error_on_write_stream() {
    let dt = sample_disc_title();
    let output = Cursor::new(Vec::new());
    let mut stream = M2tsStream::create(output, &dt).unwrap();
    assert!(PesStream::read(&mut stream).is_err());
}

// ── DiscTitle::empty ──────────────────────────────────────────

#[test]
fn disc_title_empty() {
    let dt = DiscTitle::empty();
    assert_eq!(dt.streams.len(), 0);
    assert_eq!(dt.duration_secs, 0.0);
    assert!(dt.playlist.is_empty());
}

// ── Meta codec roundtrip ─────────────────────────────────────

#[test]
fn meta_codec_roundtrip() {
    // Test that all codec types survive from_title -> to_title
    let codecs_video = &[Codec::Hevc, Codec::H264, Codec::Vc1, Codec::Mpeg2];
    let codecs_audio = &[
        Codec::Ac3,
        Codec::Ac3Plus,
        Codec::TrueHd,
        Codec::DtsHdMa,
        Codec::DtsHdHr,
        Codec::Dts,
        Codec::Lpcm,
    ];
    let codecs_sub = &[Codec::Pgs];

    let mut streams = Vec::new();
    for (i, &codec) in codecs_video.iter().enumerate() {
        streams.push(Stream::Video(VideoStream {
            pid: (0x1011 + i) as u16,
            codec,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            secondary: false,
            label: String::new(),
        }));
    }
    for (i, &codec) in codecs_audio.iter().enumerate() {
        streams.push(Stream::Audio(AudioStream {
            pid: (0x1100 + i) as u16,
            codec,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: libfreemkv::LabelPurpose::Normal,
            label: String::new(),
        }));
    }
    for (i, &codec) in codecs_sub.iter().enumerate() {
        streams.push(Stream::Subtitle(SubtitleStream {
            pid: (0x1200 + i) as u16,
            codec,
            language: "eng".into(),
            forced: false,
            qualifier: libfreemkv::LabelQualifier::None,
            codec_data: None,
        }));
    }

    let dt = DiscTitle {
        playlist: "Codec Test".into(),
        playlist_id: 0,
        duration_secs: 100.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams,
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    let meta = M2tsMeta::from_title(&dt);
    let restored = meta.to_title();

    assert_eq!(restored.streams.len(), dt.streams.len());
    for (orig, rest) in dt.streams.iter().zip(restored.streams.iter()) {
        match (orig, rest) {
            (Stream::Video(o), Stream::Video(r)) => {
                assert_eq!(o.codec, r.codec, "video codec mismatch")
            }
            (Stream::Audio(o), Stream::Audio(r)) => {
                assert_eq!(o.codec, r.codec, "audio codec mismatch")
            }
            (Stream::Subtitle(o), Stream::Subtitle(r)) => {
                assert_eq!(o.codec, r.codec, "subtitle codec mismatch")
            }
            _ => panic!("stream type mismatch"),
        }
    }
}

#[test]
fn meta_empty_streams() {
    let dt = DiscTitle {
        playlist: "Empty".into(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: Vec::new(),
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    let meta = M2tsMeta::from_title(&dt);
    assert_eq!(meta.streams.len(), 0);
    let restored = meta.to_title();
    assert_eq!(restored.streams.len(), 0);
    assert_eq!(restored.playlist, "Empty");
}

#[test]
fn meta_all_stream_types() {
    let dt = DiscTitle {
        playlist: "Full".into(),
        playlist_id: 0,
        duration_secs: 3600.0,
        size_bytes: 0,
        clips: Vec::new(),
        chapters: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
        streams: vec![
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Hevc,
                resolution: Resolution::R2160p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Hdr10,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: "Primary".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::TrueHd,
                channels: AudioChannels::Surround71,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: libfreemkv::LabelPurpose::Normal,
                label: "Primary Audio".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "fra".into(),
                forced: true,
                qualifier: libfreemkv::LabelQualifier::None,
                codec_data: None,
            }),
            Stream::Audio(AudioStream {
                pid: 0x1110,
                codec: Codec::Ac3,
                channels: AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: true,
                purpose: libfreemkv::LabelPurpose::Normal,
                label: "Commentary".into(),
            }),
        ],
        extents: Vec::new(),
    };

    let meta = M2tsMeta::from_title(&dt);
    let restored = meta.to_title();

    assert_eq!(restored.streams.len(), 4);

    // Video preserved
    if let Stream::Video(v) = &restored.streams[0] {
        assert_eq!(v.codec, Codec::Hevc);
        assert_eq!(v.resolution, Resolution::R2160p);
        assert_eq!(v.label, "Primary");
        assert!(!v.secondary);
    } else {
        panic!("expected video");
    }

    // Primary audio preserved
    if let Stream::Audio(a) = &restored.streams[1] {
        assert_eq!(a.codec, Codec::TrueHd);
        assert_eq!(a.channels, AudioChannels::Surround71);
        assert!(!a.secondary);
    } else {
        panic!("expected audio");
    }

    // Subtitle preserved (forced flag)
    if let Stream::Subtitle(s) = &restored.streams[2] {
        assert_eq!(s.language, "fra");
        assert!(s.forced);
    } else {
        panic!("expected subtitle");
    }

    // Secondary audio preserved
    if let Stream::Audio(a) = &restored.streams[3] {
        assert_eq!(a.codec, Codec::Ac3);
        assert!(a.secondary);
        assert_eq!(a.label, "Commentary");
    } else {
        panic!("expected secondary audio");
    }
}

// ── MkvStream tests ──────────────────────────────────────────

#[test]
fn mkvstream_write_finish() {
    let dt = sample_disc_title();
    let writer: Box<dyn libfreemkv::mux::WriteSeek + Send> = Box::new(Cursor::new(Vec::new()));
    let mut stream = MkvStream::create(writer, &dt).unwrap();

    // Write some fake PES frames (they won't produce valid MKV content
    // since there is no real codec data, but it should not panic)
    for i in 0..20u8 {
        let frame = libfreemkv::pes::PesFrame {
            track: 0,
            pts: i as i64 * 1_000_000,
            keyframe: i == 0,
            data: vec![i; 100],
        };
        PesStream::write(&mut stream, &frame).unwrap();
    }

    // finish should not panic even without valid codec data
    PesStream::finish(&mut stream).unwrap();
}

#[test]
fn mkvstream_meta_sets_title() {
    let dt = sample_disc_title();
    let writer: Box<dyn libfreemkv::mux::WriteSeek + Send> = Box::new(Cursor::new(Vec::new()));
    let stream = MkvStream::create(writer, &dt).unwrap();

    let info = PesStream::info(&stream);
    assert_eq!(info.playlist, "Test Movie");
    assert_eq!(info.duration_secs, 7200.0);
    assert_eq!(info.streams.len(), 4);
}

// ── MkvStream additional tests ──────────────────────────────

#[test]
fn mkvstream_roundtrip_bdts() {
    // Write BD-TS packets through MkvStream and verify the pipeline works.
    // Without real codec headers (SPS/PPS), the muxer stays in scanning phase.
    // With a title that has no video streams (audio-only), codec scanning is
    // skipped and the muxer enters streaming mode immediately, producing EBML output.

    let dt = DiscTitle {
        playlist: "Audio Only".into(),
        playlist_id: 0,
        duration_secs: 60.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: libfreemkv::LabelPurpose::Normal,
            label: "English".into(),
        })],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    let writer: Box<dyn libfreemkv::mux::WriteSeek + Send> = Box::new(Cursor::new(Vec::new()));
    let mut stream = MkvStream::create(writer, &dt).unwrap();

    // Write PES frames targeting the audio track
    for i in 0..10u8 {
        let frame = libfreemkv::pes::PesFrame {
            track: 0,
            pts: i as i64 * 1_000_000,
            keyframe: true,
            data: vec![i; 100],
        };
        PesStream::write(&mut stream, &frame).unwrap();
    }

    PesStream::finish(&mut stream).unwrap();

    // Verify the info is correct
    let info = PesStream::info(&stream);
    assert_eq!(info.streams.len(), 1);
    assert_eq!(info.playlist, "Audio Only");
}

#[test]
fn mkvstream_meta_preserves_all_streams() {
    let dt = DiscTitle {
        playlist: "Stream Test".into(),
        playlist_id: 0,
        duration_secs: 3600.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::H264,
                resolution: Resolution::R1080p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: "Main Video".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::Ac3,
                channels: AudioChannels::Surround51,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: libfreemkv::LabelPurpose::Normal,
                label: "English".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1101,
                codec: Codec::DtsHdMa,
                channels: AudioChannels::Surround71,
                language: "fra".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: libfreemkv::LabelPurpose::Normal,
                label: "French".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: false,
                qualifier: libfreemkv::LabelQualifier::None,
                codec_data: None,
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1201,
                codec: Codec::Pgs,
                language: "fra".into(),
                forced: true,
                qualifier: libfreemkv::LabelQualifier::None,
                codec_data: None,
            }),
        ],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    let writer: Box<dyn libfreemkv::mux::WriteSeek + Send> = Box::new(Cursor::new(Vec::new()));
    let stream = MkvStream::create(writer, &dt).unwrap();

    let info = PesStream::info(&stream);
    assert_eq!(info.streams.len(), 5, "all 5 streams should be preserved");
    assert_eq!(info.playlist, "Stream Test");
    assert_eq!(info.duration_secs, 3600.0);

    // Verify stream types preserved in order
    assert!(matches!(&info.streams[0], Stream::Video(_)));
    assert!(matches!(&info.streams[1], Stream::Audio(_)));
    assert!(matches!(&info.streams[2], Stream::Audio(_)));
    assert!(matches!(&info.streams[3], Stream::Subtitle(_)));
    assert!(matches!(&info.streams[4], Stream::Subtitle(_)));

    // Check specific attributes
    if let Stream::Video(v) = &info.streams[0] {
        assert_eq!(v.codec, Codec::H264);
    }
    if let Stream::Audio(a) = &info.streams[1] {
        assert_eq!(a.language, "eng");
    }
    if let Stream::Audio(a) = &info.streams[2] {
        assert_eq!(a.codec, Codec::DtsHdMa);
        assert_eq!(a.language, "fra");
    }
    if let Stream::Subtitle(s) = &info.streams[3] {
        assert!(!s.forced);
    }
    if let Stream::Subtitle(s) = &info.streams[4] {
        assert!(s.forced);
    }
}

// ── H2: End-to-end MKV mux test ─────────────────────────────

#[test]
fn mkvstream_e2e_h264_produces_valid_mkv() {
    // Construct a DiscTitle with one H.264 video stream
    let dt = DiscTitle {
        playlist: "H264 Test".into(),
        playlist_id: 0,
        duration_secs: 10.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            secondary: false,
            label: "Main".into(),
        })],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    // Build elementary stream data: start codes + NALs
    let mut es_data = Vec::new();

    // SPS NAL (type 7): minimal valid SPS
    es_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // start code
    es_data.push(0x67); // NAL type 7 (SPS), nal_ref_idc=3
    es_data.extend_from_slice(&[
        0x42, 0xC0, 0x1E, // profile=66, constraint_set0=1, level=30
        0xD9, 0x00, 0xA0, 0x47, 0xFE, 0x88, // minimal SPS rbsp
    ]);

    // PPS NAL (type 8): minimal valid PPS
    es_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // start code
    es_data.push(0x68); // NAL type 8 (PPS), nal_ref_idc=3
    es_data.extend_from_slice(&[0xCE, 0x38, 0x80]); // minimal PPS rbsp

    // IDR NAL (type 5): keyframe
    es_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // start code
    es_data.push(0x65); // NAL type 5 (IDR), nal_ref_idc=3
    es_data.extend_from_slice(&[0x88, 0x84, 0x00, 0x21, 0xFF, 0xFE, 0xF6, 0xE2]);
    es_data.extend_from_slice(&[0x00; 64]);

    // Non-IDR slice data (NAL type 1)
    let mut es_data2 = Vec::new();
    es_data2.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    es_data2.push(0x41); // NAL type 1 (non-IDR)
    es_data2.extend_from_slice(&[0x9A, 0x00, 0x10, 0x20]);
    es_data2.extend_from_slice(&[0x00; 32]);

    // Feed through MkvStream using a shared writer to inspect the output bytes.
    let output2 = std::sync::Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));

    struct SharedWriter(std::sync::Arc<std::sync::Mutex<Cursor<Vec<u8>>>>);
    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }
    impl std::io::Seek for SharedWriter {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
            self.0.lock().unwrap().seek(pos)
        }
    }

    let writer: Box<dyn libfreemkv::mux::WriteSeek + Send> =
        Box::new(SharedWriter(output2.clone()));
    let mut stream2 = MkvStream::create(writer, &dt).unwrap();

    // Write the ES data (SPS+PPS+IDR) as a keyframe PES frame.
    let frame1 = libfreemkv::pes::PesFrame {
        track: 0,
        pts: 1_000_000_000, // 1 second in ns
        keyframe: true,
        data: es_data,
    };
    PesStream::write(&mut stream2, &frame1).unwrap();

    // Write a second non-IDR frame
    let frame2 = libfreemkv::pes::PesFrame {
        track: 0,
        pts: 1_041_700_000, // ~1 frame later in ns
        keyframe: false,
        data: es_data2,
    };
    PesStream::write(&mut stream2, &frame2).unwrap();
    PesStream::finish(&mut stream2).unwrap();

    let data = output2.lock().unwrap().clone().into_inner();

    // Verify output starts with EBML magic (0x1A45DFA3)
    assert!(
        data.len() >= 4,
        "MKV output too small: {} bytes",
        data.len()
    );
    assert_eq!(
        &data[0..4],
        &[0x1A, 0x45, 0xDF, 0xA3],
        "output should start with EBML magic"
    );

    // Verify output contains a Tracks element (0x1654AE6B)
    let tracks_needle = [0x16, 0x54, 0xAE, 0x6B];
    let has_tracks = data.windows(4).any(|w| w == tracks_needle);
    assert!(has_tracks, "output should contain Tracks element");

    // Note: With the PES stream API, codec_privates are populated from the
    // DiscTitle at creation time, not extracted from ES data during write.
    // This test verifies the muxer produces valid EBML structure.
}
