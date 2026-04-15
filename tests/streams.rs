//! Integration tests for the IOStream pipeline.

use libfreemkv::mux::meta::M2tsMeta;
use libfreemkv::*;
use std::io::{Cursor, Read, Write};

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
                label: "English Atmos".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1101,
                codec: Codec::Ac3,
                channels: AudioChannels::Surround51,
                language: "fra".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                label: "French".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: false,
                codec_data: None,
            }),
        ],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
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
    let result = libfreemkv::open_input("Dune.mkv", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("not a valid stream URL"), "got: {}", msg);
}

#[test]
fn open_output_bare_path_errors() {
    let dt = sample_disc_title();
    let result = libfreemkv::open_output("Dune.mkv", &dt);
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("not a valid stream URL"), "got: {}", msg);
}

#[test]
fn open_input_m2ts_empty_path_errors() {
    let result = libfreemkv::open_input("m2ts://", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("requires a file path"), "got: {}", msg);
}

#[test]
fn open_output_null_input_errors() {
    let result = libfreemkv::open_input("null://", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("write-only"), "got: {}", msg);
}

#[test]
fn open_output_disc_errors() {
    let dt = sample_disc_title();
    let result = libfreemkv::open_output("disc://", &dt);
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("read-only"), "got: {}", msg);
}

#[test]
fn open_input_network_no_port_errors() {
    let result = libfreemkv::open_input("network://10.0.0.1", &libfreemkv::InputOptions::default());
    assert!(result.is_err());
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("missing port"), "got: {}", msg);
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

    // Build fake BD-TS packets
    let mut ts_data = Vec::new();
    for i in 0..10u8 {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        pkt[5] = 0x10;
        pkt[6] = 0x11;
        pkt[7] = 0x10;
        pkt[8] = i;
        ts_data.extend_from_slice(&pkt);
    }

    // Write through M2tsStream to a Cursor
    let output = Cursor::new(Vec::new());
    let mut stream = M2tsStream::new(output).meta(&dt);
    stream.write_all(&ts_data).unwrap();
    stream.finish().unwrap();

    // M2tsStream consumed the cursor — we need the inner data.
    // For this test, write to a shared buffer instead.
    // Use a second pass: write header + data manually to verify read side.
    let mut encoded = Vec::new();
    let meta = M2tsMeta::from_title(&dt);
    libfreemkv::mux::meta::write_header(&mut encoded, &meta).unwrap();
    encoded.extend_from_slice(&ts_data);

    // Read back
    let cursor = Cursor::new(encoded);
    let mut stream = M2tsStream::open(cursor).unwrap();
    let info = stream.info();
    assert_eq!(info.streams.len(), 4);
    assert_eq!(info.duration_secs, 7200.0);

    // Read BD-TS data
    let mut read_buf = vec![0u8; 192 * 10];
    let mut total = 0;
    loop {
        match stream.read(&mut read_buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(_) => break,
        }
    }

    assert_eq!(total, 192 * 10);
    for i in 0..10u8 {
        assert_eq!(read_buf[i as usize * 192 + 4], 0x47);
        assert_eq!(read_buf[i as usize * 192 + 8], i);
    }
}

// ── M2tsStream passthrough identity ───────────────────────────

#[test]
fn m2ts_passthrough_preserves_data() {
    let dt = sample_disc_title();

    // Create original BD-TS data
    let mut original = Vec::new();
    for i in 0..100u8 {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        pkt[5] = (i % 3) << 4;
        pkt[6] = i;
        for (j, byte) in pkt.iter_mut().enumerate().skip(8) {
            *byte = i.wrapping_add(j as u8);
        }
        original.extend_from_slice(&pkt);
    }

    // Build encoded: header + original data
    let mut encoded = Vec::new();
    let meta = M2tsMeta::from_title(&dt);
    libfreemkv::mux::meta::write_header(&mut encoded, &meta).unwrap();
    encoded.extend_from_slice(&original);

    // Read back through M2tsStream
    let cursor = Cursor::new(encoded);
    let mut stream = M2tsStream::open(cursor).unwrap();
    let mut decoded = vec![0u8; original.len()];
    let mut total = 0;
    loop {
        match stream.read(&mut decoded[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(_) => break,
        }
    }

    // BD-TS data must be byte-identical
    assert_eq!(total, original.len());
    assert_eq!(decoded, original);
}

// ── IOStream trait ────────────────────────────────────────────

#[test]
fn m2ts_implements_iostream() {
    let dt = sample_disc_title();
    let output = Cursor::new(Vec::new());
    let stream = M2tsStream::new(output).meta(&dt);

    let mut boxed: Box<dyn IOStream> = Box::new(stream);
    let meta = boxed.info();
    assert_eq!(meta.streams.len(), 4);

    let pkt = [0u8; 192];
    boxed.write_all(&pkt).unwrap();
    boxed.finish().unwrap();
}

#[test]
fn m2ts_read_returns_error_on_write_stream() {
    let output = Cursor::new(Vec::new());
    let stream = M2tsStream::new(output);
    let mut boxed: Box<dyn IOStream> = Box::new(stream);
    let mut buf = [0u8; 10];
    assert!(boxed.read(&mut buf).is_err());
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
            label: String::new(),
        }));
    }
    for (i, &codec) in codecs_sub.iter().enumerate() {
        streams.push(Stream::Subtitle(SubtitleStream {
            pid: (0x1200 + i) as u16,
            codec,
            language: "eng".into(),
            forced: false,
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
                label: "Primary Audio".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "fra".into(),
                forced: true,
                codec_data: None,
            }),
            Stream::Audio(AudioStream {
                pid: 0x1110,
                codec: Codec::Ac3,
                channels: AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: true,
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
    let output = Cursor::new(Vec::new());
    let dt = sample_disc_title();
    let mut stream = MkvStream::new(output).meta(&dt).max_buffer(1024 * 1024);

    // Write some fake BD-TS packets (they won't produce valid MKV frames
    // since there is no real codec data, but it should not panic)
    for i in 0..20u8 {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        // PID 0x1011 (video)
        pkt[5] = 0x10;
        pkt[6] = 0x11;
        pkt[7] = 0x10;
        pkt[8] = i;
        stream.write_all(&pkt).unwrap();
    }

    // finish should not panic even without valid codec data
    stream.finish().unwrap();
}

#[test]
fn mkvstream_meta_sets_title() {
    let output = Cursor::new(Vec::new());
    let dt = sample_disc_title();
    let stream = MkvStream::new(output).meta(&dt);

    let info = stream.info();
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
            label: "English".into(),
        })],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
    };

    let output = Cursor::new(Vec::new());
    let mut stream = MkvStream::new(output).meta(&dt).max_buffer(1024 * 1024);

    // Write BD-TS packets targeting the audio PID 0x1100
    for i in 0..10u8 {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        // PID 0x1100
        pkt[5] = 0x11;
        pkt[6] = 0x00;
        pkt[7] = 0x10;
        pkt[8] = i;
        stream.write_all(&pkt).unwrap();
    }

    stream.finish().unwrap();

    // Verify the info is correct
    let info = stream.info();
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
                label: "English".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1101,
                codec: Codec::DtsHdMa,
                channels: AudioChannels::Surround71,
                language: "fra".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                label: "French".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: false,
                codec_data: None,
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1201,
                codec: Codec::Pgs,
                language: "fra".into(),
                forced: true,
                codec_data: None,
            }),
        ],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
    };

    let output = Cursor::new(Vec::new());
    let stream = MkvStream::new(output).meta(&dt);

    let info = stream.info();
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
    };

    // Build synthetic BD-TS packets containing valid H.264 NALs
    // We need PES headers wrapping: SPS (NAL type 7), PPS (NAL type 8), IDR (NAL type 5)
    let mut ts_data = Vec::new();

    // Build elementary stream data: start codes + NALs
    let mut es_data = Vec::new();

    // SPS NAL (type 7): minimal valid SPS
    es_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // start code
    es_data.push(0x67); // NAL type 7 (SPS), nal_ref_idc=3
                        // Minimal SPS payload: profile_idc=66 (Baseline), constraint flags, level_idc=30
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
                        // Some IDR slice data
    es_data.extend_from_slice(&[0x88, 0x84, 0x00, 0x21, 0xFF, 0xFE, 0xF6, 0xE2]);
    // Pad to reasonable size
    es_data.extend_from_slice(&[0x00; 64]);

    // Wrap in PES header with PTS
    let pts: i64 = 90000; // 1 second in 90kHz ticks
    let pts_bytes = encode_pts_test(pts);
    let pes_header_len = 9 + 5; // basic PES header (9) + PTS (5)
    let pes_length = (3 + 5 + es_data.len()) as u16; // flags(3) + PTS(5) + ES data

    let mut pes = Vec::new();
    pes.extend_from_slice(&[0x00, 0x00, 0x01, 0xE0]); // PES start code + video stream_id
    pes.extend_from_slice(&pes_length.to_be_bytes()); // PES packet length
    pes.extend_from_slice(&[0x80, 0x80, 0x05]); // flags: PTS present, header_data_len=5
    pes.extend_from_slice(&pts_bytes);
    pes.extend_from_slice(&es_data);

    // Wrap PES in 192-byte BD-TS packets
    let pid: u16 = 0x1011;
    let mut pes_offset = 0;
    let mut pusi = true;
    let mut cc: u8 = 0;

    while pes_offset < pes.len() {
        let mut pkt = [0u8; 192];
        // 4-byte TP_extra_header (zeros)
        pkt[4] = 0x47; // sync byte
        pkt[5] = (pid >> 8) as u8 & 0x1F;
        if pusi {
            pkt[5] |= 0x40; // PUSI
            pusi = false;
        }
        pkt[6] = pid as u8;
        pkt[7] = 0x10 | (cc & 0x0F); // payload only + continuity counter
        cc = cc.wrapping_add(1);

        let space = 184;
        let rem = pes.len() - pes_offset;
        let n = rem.min(space);

        if n < space {
            // Need adaptation field for padding
            let pad = space - n;
            pkt[7] = 0x30 | (cc.wrapping_sub(1) & 0x0F); // AF + payload
            pkt[8] = (pad - 1) as u8; // adaptation_field_length
            if pad > 1 {
                pkt[9] = 0x00; // flags
            }
            for byte in pkt.iter_mut().take(8 + pad).skip(10) {
                *byte = 0xFF;
            }
            pkt[8 + pad..8 + pad + n].copy_from_slice(&pes[pes_offset..pes_offset + n]);
        } else {
            pkt[8..8 + n].copy_from_slice(&pes[pes_offset..pes_offset + n]);
        }

        ts_data.extend_from_slice(&pkt);
        pes_offset += n;
    }

    // Build a second PES (access unit) to trigger the first PES to be output
    // by the TS demuxer (it needs a new PUSI to emit the previous PES).
    let pts2: i64 = 90000 + 3753; // ~1 frame later
    let pts2_bytes = encode_pts_test(pts2);
    let mut es_data2 = Vec::new();
    // Just a non-IDR slice (NAL type 1)
    es_data2.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    es_data2.push(0x41); // NAL type 1 (non-IDR)
    es_data2.extend_from_slice(&[0x9A, 0x00, 0x10, 0x20]);
    es_data2.extend_from_slice(&[0x00; 32]);

    let pes2_length = (3 + 5 + es_data2.len()) as u16;
    let mut pes2 = Vec::new();
    pes2.extend_from_slice(&[0x00, 0x00, 0x01, 0xE0]);
    pes2.extend_from_slice(&pes2_length.to_be_bytes());
    pes2.extend_from_slice(&[0x80, 0x80, 0x05]);
    pes2.extend_from_slice(&pts2_bytes);
    pes2.extend_from_slice(&es_data2);

    // Wrap second PES in BD-TS packets
    let mut pes2_offset = 0;
    let mut pusi2 = true;
    while pes2_offset < pes2.len() {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        pkt[5] = (pid >> 8) as u8 & 0x1F;
        if pusi2 {
            pkt[5] |= 0x40;
            pusi2 = false;
        }
        pkt[6] = pid as u8;
        pkt[7] = 0x10 | (cc & 0x0F);
        cc = cc.wrapping_add(1);

        let space = 184;
        let rem = pes2.len() - pes2_offset;
        let n = rem.min(space);

        if n < space {
            let pad = space - n;
            pkt[7] = 0x30 | (cc.wrapping_sub(1) & 0x0F);
            pkt[8] = (pad - 1) as u8;
            if pad > 1 {
                pkt[9] = 0x00;
            }
            for byte in pkt.iter_mut().take(8 + pad).skip(10) {
                *byte = 0xFF;
            }
            pkt[8 + pad..8 + pad + n].copy_from_slice(&pes2[pes2_offset..pes2_offset + n]);
        } else {
            pkt[8..8 + n].copy_from_slice(&pes2[pes2_offset..pes2_offset + n]);
        }

        ts_data.extend_from_slice(&pkt);
        pes2_offset += n;
    }

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

    let writer = SharedWriter(output2.clone());
    let mut stream2 = MkvStream::new(writer).meta(&dt).max_buffer(1024 * 1024);
    stream2.write_all(&ts_data).unwrap();
    stream2.finish().unwrap();

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

    // Verify codecPrivate is non-empty (not all zeros)
    // CodecPrivate element ID is 0x63A2
    let cp_needle = [0x63, 0xA2];
    let cp_pos = data.windows(2).position(|w| w == cp_needle);
    if let Some(pos) = cp_pos {
        // After the ID, there's a size VINT, then the data
        let after_id = pos + 2;
        if after_id < data.len() {
            // Read VINT size
            let size_byte = data[after_id];
            let (cp_size, cp_data_start) = if size_byte & 0x80 != 0 {
                ((size_byte & 0x7F) as usize, after_id + 1)
            } else if size_byte & 0x40 != 0 && after_id + 1 < data.len() {
                (
                    (((size_byte & 0x3F) as usize) << 8) | data[after_id + 1] as usize,
                    after_id + 2,
                )
            } else {
                (0, after_id + 1)
            };
            if cp_size > 0 && cp_data_start + cp_size <= data.len() {
                let cp_data = &data[cp_data_start..cp_data_start + cp_size];
                let all_zeros = cp_data.iter().all(|&b| b == 0);
                assert!(
                    !all_zeros,
                    "codecPrivate should not be all zeros (SPS/PPS should be filled)"
                );
            }
        }
    }
}

fn encode_pts_test(pts: i64) -> [u8; 5] {
    let p = pts as u64;
    [
        0x21 | ((p >> 29) & 0x0E) as u8,
        ((p >> 22) & 0xFF) as u8,
        0x01 | ((p >> 14) & 0xFE) as u8,
        ((p >> 7) & 0xFF) as u8,
        0x01 | ((p << 1) & 0xFE) as u8,
    ]
}
