//! Integration tests for the IOStream pipeline.

use std::io::{Cursor, Read, Write, Seek, SeekFrom};
use libfreemkv::*;
use libfreemkv::mux::meta::M2tsMeta;

fn sample_disc_title() -> DiscTitle {
    DiscTitle {
        playlist: "Test Movie".into(),
        playlist_id: 0,
        duration_secs: 7200.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![
            Stream::Video(VideoStream {
                pid: 0x1011, codec: Codec::Hevc,
                resolution: "2160p".into(), frame_rate: "23.976".into(),
                hdr: HdrFormat::Hdr10, color_space: ColorSpace::Bt709,
                secondary: false, label: "Main".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1100, codec: Codec::TrueHd,
                channels: "7.1".into(), language: "eng".into(),
                sample_rate: "48kHz".into(), secondary: false,
                label: "English Atmos".into(),
            }),
            Stream::Audio(AudioStream {
                pid: 0x1101, codec: Codec::Ac3,
                channels: "5.1".into(), language: "fra".into(),
                sample_rate: "48kHz".into(), secondary: false,
                label: "French".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200, codec: Codec::Pgs,
                language: "eng".into(), forced: false,
            }),
        ],
        extents: Vec::new(),
    }
}

// ── URL parsing ────────────────────────────────────────────────

#[test]
fn parse_url_disc() {
    let u = parse_url("disc://");
    assert_eq!(u.scheme, "disc");
    assert_eq!(u.path, "");
}

#[test]
fn parse_url_disc_device() {
    let u = parse_url("disc:///dev/sg4");
    assert_eq!(u.scheme, "disc");
    assert_eq!(u.path, "/dev/sg4");
}

#[test]
fn parse_url_mkv() {
    let u = parse_url("mkv://Dune.mkv");
    assert_eq!(u.scheme, "mkv");
    assert_eq!(u.path, "Dune.mkv");
}

#[test]
fn parse_url_network() {
    let u = parse_url("network://10.1.7.11:9000");
    assert_eq!(u.scheme, "network");
    assert_eq!(u.path, "10.1.7.11:9000");
}

#[test]
fn parse_url_bare_mkv() {
    let u = parse_url("Dune.mkv");
    assert_eq!(u.scheme, "mkv");
    assert_eq!(u.path, "Dune.mkv");
}

#[test]
fn parse_url_bare_m2ts() {
    let u = parse_url("Dune.m2ts");
    assert_eq!(u.scheme, "m2ts");
    assert_eq!(u.path, "Dune.m2ts");
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
        assert_eq!(v.resolution, "2160p");
        assert_eq!(v.label, "Main");
    } else { panic!("expected video"); }

    // Check audio
    if let Stream::Audio(a) = &restored.streams[1] {
        assert_eq!(a.codec, Codec::TrueHd);
        assert_eq!(a.language, "eng");
        assert_eq!(a.label, "English Atmos");
    } else { panic!("expected audio"); }

    // Check subtitle
    if let Stream::Subtitle(s) = &restored.streams[3] {
        assert_eq!(s.language, "eng");
        assert!(!s.forced);
    } else { panic!("expected subtitle"); }
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
    let read_back = libfreemkv::mux::meta::read_header(&mut cursor).unwrap().unwrap();
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
        for j in 8..192 { pkt[j] = i.wrapping_add(j as u8); }
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
