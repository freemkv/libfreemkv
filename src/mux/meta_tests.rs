use super::*;
use crate::disc::{Codec, FrameRate, Resolution};

fn video_title(hdr: HdrFormat, cs: ColorSpace) -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams.push(Stream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R2160p,
        frame_rate: FrameRate::F23_976,
        hdr,
        color_space: cs,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    }));
    t
}

fn round_trip_color_space(title: &DiscTitle) -> ColorSpace {
    let meta = M2tsMeta::from_title(title);
    let back = meta.to_title();
    match &back.streams[0] {
        Stream::Video(v) => v.color_space,
        _ => panic!("expected video stream"),
    }
}

#[test]
fn color_space_round_trips_bt2020() {
    // The regression: HDR10 / BT.2020 must survive from_title → to_title,
    // not collapse to the hardcoded BT.709.
    let cs = round_trip_color_space(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
    assert_eq!(cs, ColorSpace::Bt2020, "BT.2020 must round-trip");
}

#[test]
fn color_space_round_trips_bt709() {
    let cs = round_trip_color_space(&video_title(HdrFormat::Sdr, ColorSpace::Bt709));
    assert_eq!(cs, ColorSpace::Bt709);
}

#[test]
fn color_space_serialized_in_json() {
    let meta = M2tsMeta::from_title(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
    let json = serde_json::to_string(&meta).unwrap();
    assert!(
        json.contains("bt2020"),
        "color_space must be serialized: {json}"
    );
}

#[test]
fn legacy_metadata_without_color_space_derives_from_hdr() {
    // Pre-0.30.7 JSON has no color_space field. to_title must derive the
    // color space from the HDR format so HDR color metadata is preserved.
    let json = r#"{
            "v": 1,
            "title": "x",
            "duration": 0.0,
            "streams": [
                {"type":"video","pid":4113,"codec":"hevc","resolution":"2160p",
                 "frame_rate":"23.976","hdr":"hdr10","label":"","secondary":false}
            ]
        }"#;
    let meta: M2tsMeta = serde_json::from_str(json).unwrap();
    let back = meta.to_title();
    match &back.streams[0] {
        Stream::Video(v) => {
            assert_eq!(v.hdr, HdrFormat::Hdr10);
            assert_eq!(
                v.color_space,
                ColorSpace::Bt2020,
                "HDR10 must derive BT.2020 when color_space absent"
            );
        }
        _ => panic!("expected video stream"),
    }
}

#[test]
fn read_header_empty_is_none_not_error() {
    // No bytes at all → clean EOF on the magic read → Ok(None), the
    // "no FMKV header, fall back" signal.
    let empty: &[u8] = &[];
    let mut cursor = io::Cursor::new(empty);
    let got = read_header(&mut cursor).expect("clean EOF must be Ok(None)");
    assert!(got.is_none());
}

#[test]
fn read_header_propagates_non_eof_error() {
    // A reader that fails with a non-EOF error must surface that
    // error, not be swallowed as Ok(None).
    struct BrokenReader;
    impl Read for BrokenReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }
    }
    let mut r = BrokenReader;
    let err = read_header(&mut r).expect_err("broken pipe must propagate");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn write_header_then_read_header_round_trips() {
    let title = video_title(HdrFormat::Hdr10, ColorSpace::Bt2020);
    let meta = M2tsMeta::from_title(&title);
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).expect("write");
    let mut cursor = io::Cursor::new(&buf);
    let back = read_header(&mut cursor)
        .expect("read")
        .expect("header present");
    assert_eq!(back.streams.len(), 1);
    // Header is padded to a 192-byte boundary; the cursor must land
    // exactly there so the following BD-TS data stays aligned.
    assert_eq!(cursor.position() as usize % BD_SOURCE_PACKET_BYTES, 0);
}

// Every stream field the receiver acts on survives an FMKV hop (network://, stdio://, m2ts://).
#[test]
fn stream_fields_round_trip_through_the_header() {
    use crate::disc::{
        AudioChannels, AudioStream, LabelPurpose, LabelQualifier, MVC_DEPENDENT_LABEL, SampleRate,
        SubtitleStream,
    };
    let mut t = video_title(HdrFormat::Hdr10, ColorSpace::Bt2020);
    if let Stream::Video(v) = &mut t.streams[0] {
        v.label = MVC_DEPENDENT_LABEL.into();
        v.secondary = true;
    }
    for (i, purpose) in [
        LabelPurpose::Commentary,
        LabelPurpose::Descriptive,
        LabelPurpose::Score,
        LabelPurpose::Ime,
    ]
    .into_iter()
    .enumerate()
    {
        t.streams.push(Stream::Audio(AudioStream {
            pid: 0x1100 + i as u16,
            codec: Codec::Dts,
            channels: AudioChannels::Surround71,
            language: "fra".into(),
            sample_rate: SampleRate::S96,
            secondary: true,
            purpose,
            label: format!("audio {i}"),
        }));
    }
    for qualifier in [
        LabelQualifier::Sdh,
        LabelQualifier::DescriptiveService,
        LabelQualifier::Forced,
    ] {
        t.streams.push(Stream::Subtitle(SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "deu".into(),
            forced: true,
            qualifier,
            codec_data: None,
        }));
    }
    let back = M2tsMeta::from_title(&t).to_title();
    let Stream::Video(v) = &back.streams[0] else {
        panic!("video first")
    };
    assert!(v.is_mvc_dependent());
    let dbg = |s: &crate::disc::Stream| format!("{s:?}");
    let (want, got): (Vec<_>, Vec<_>) = t
        .streams
        .iter()
        .zip(&back.streams)
        .map(|(a, b)| (dbg(a), dbg(b)))
        .unzip();
    assert_eq!(got, want);
}

// The wire track is a u8: a header declaring more than 256 streams is refused.
#[test]
fn a_header_with_more_than_256_streams_is_refused() {
    let title_of = |n: usize| {
        let mut t = DiscTitle::empty();
        for pid in 0..n {
            t.streams
                .push(Stream::Subtitle(crate::disc::SubtitleStream {
                    pid: pid as u16,
                    codec: Codec::Pgs,
                    language: String::new(),
                    forced: false,
                    qualifier: crate::disc::LabelQualifier::None,
                    codec_data: None,
                }));
        }
        t
    };
    let read = |n| {
        let mut buf = Vec::new();
        write_header(&mut buf, &M2tsMeta::from_title(&title_of(n))).unwrap();
        read_header(&mut io::Cursor::new(buf))
    };
    assert!(read(256).unwrap().is_some());
    assert!(read(257).is_err());
}

#[test]
fn audio_and_subtitle_codec_private_round_trip() {
    use crate::disc::{AudioChannels, AudioStream, LabelPurpose, SampleRate, SubtitleStream};
    let mut t = DiscTitle::empty();
    t.streams.push(Stream::Audio(AudioStream {
        pid: 0x1100,
        codec: Codec::Dts,
        channels: AudioChannels::Surround51,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    }));
    t.streams.push(Stream::Subtitle(SubtitleStream {
        pid: 0x1200,
        codec: Codec::DvdSub,
        language: "eng".into(),
        forced: false,
        qualifier: crate::disc::LabelQualifier::None,
        codec_data: None,
    }));
    // codec_privates: index 0 = audio init data, index 1 = subtitle init data.
    t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC]), Some(vec![0x01, 0x02])];

    let meta = M2tsMeta::from_title(&t);
    // Must serialize for both audio and subtitle (not just video).
    let cps = meta.codec_privates();
    assert_eq!(cps[0].as_deref(), Some(&[0xAA, 0xBB, 0xCC][..]));
    assert_eq!(cps[1].as_deref(), Some(&[0x01, 0x02][..]));

    // And to_title restores the subtitle codec_data from the header.
    let back = meta.to_title();
    match &back.streams[1] {
        Stream::Subtitle(s) => {
            assert_eq!(s.codec_data.as_deref(), Some(&[0x01, 0x02][..]))
        }
        _ => panic!("expected subtitle stream"),
    }
    // The round-tripped title also carries all codec_privates.
    assert_eq!(
        back.codec_privates[0].as_deref(),
        Some(&[0xAA, 0xBB, 0xCC][..])
    );
    assert_eq!(back.codec_privates[1].as_deref(), Some(&[0x01, 0x02][..]));
}

#[test]
fn newer_version_header_rejected() {
    // A header tagged with a version above SUPPORTED_VERSION must be
    // refused, not silently parsed as v1.
    let meta = M2tsMeta::from_title(&video_title(HdrFormat::Sdr, ColorSpace::Bt709));
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    buf[VERSION_BYTE] = SUPPORTED_VERSION + 1; // bump version byte
    let mut cur = io::Cursor::new(buf);
    let err = read_header(&mut cur).unwrap_err();
    // NoMetadata (E9008) maps to InvalidInput.
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn empty_stream_is_clean_none_but_partial_magic_errors() {
    // Zero bytes → no header (Ok(None)).
    let mut empty = io::Cursor::new(Vec::<u8>::new());
    assert!(read_header(&mut empty).unwrap().is_none());

    // Begins with 'F' (MAGIC[0]) then truncates → error, not None.
    let mut partial = io::Cursor::new(vec![b'F', b'M', b'K']);
    assert!(read_header(&mut partial).is_err());

    // Does not begin with the FMKV magic at all → Ok(None) (headerless).
    let mut other = io::Cursor::new(vec![0x47u8; 16]);
    assert!(read_header(&mut other).unwrap().is_none());
}

#[test]
fn header_round_trips_through_write_read() {
    let meta = M2tsMeta::from_title(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    let mut cur = io::Cursor::new(buf);
    let back = read_header(&mut cur).unwrap().expect("header present");
    assert_eq!(back.streams.len(), 1);
}

#[test]
fn legacy_sdr_without_color_space_derives_bt709() {
    let json = r#"{
            "v": 1,
            "title": "x",
            "duration": 0.0,
            "streams": [
                {"type":"video","pid":4113,"codec":"h264","resolution":"1080p",
                 "frame_rate":"24","hdr":"sdr","label":"","secondary":false}
            ]
        }"#;
    let meta: M2tsMeta = serde_json::from_str(json).unwrap();
    let back = meta.to_title();
    match &back.streams[0] {
        Stream::Video(v) => assert_eq!(v.color_space, ColorSpace::Bt709),
        _ => panic!("expected video stream"),
    }
}

// Header byte-layout invariants. Format: [8B magic][4B json_len BE][JSON]
// [pad to 192B]. MUST end on a 192-byte (BD-TS packet) boundary so following
// TS stays aligned (tools resync on 0x47); wrong padding misaligns the payload.

#[test]
fn magic_bytes_exact_layout() {
    // The magic is "FMKV" + reserved 0x00 + version 0x01 + 2 reserved.
    // The version byte lives at index 5. A regression that shifted the
    // version byte would make every header read the wrong version.
    assert_eq!(&MAGIC[0..4], b"FMKV");
    assert_eq!(MAGIC[VERSION_BYTE], 1, "the base (v1) magic");
    assert_eq!(VERSION_BYTE, 5);
    assert_eq!(MAGIC.len(), 8);
}

#[test]
fn write_header_pads_to_192_byte_boundary() {
    // The total written length must always be a multiple of BD_SOURCE_PACKET_BYTES
    // (192). Test a range of JSON sizes by varying stream count.
    for n_streams in 0..6 {
        let mut t = DiscTitle::empty();
        for _ in 0..n_streams {
            t.streams.push(Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Hevc,
                resolution: Resolution::R2160p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Hdr10,
                color_space: ColorSpace::Bt2020,
                display_aspect: None,
                secondary: false,
                label: "x".into(),
                measured_cicp: None,
            }));
        }
        let meta = M2tsMeta::from_title(&t);
        let mut buf = Vec::new();
        write_header(&mut buf, &meta).unwrap();
        assert_eq!(
            buf.len() % BD_SOURCE_PACKET_BYTES,
            0,
            "header for {n_streams} streams (len {}) not 192-aligned",
            buf.len()
        );
        // The declared json_len (bytes 8..12, big-endian) must equal the
        // actual JSON byte length embedded.
        let json_len = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        let json_bytes = &buf[12..12 + json_len];
        // Round-trips as valid JSON for M2tsMeta.
        let parsed: M2tsMeta = serde_json::from_slice(json_bytes).unwrap();
        assert_eq!(parsed.streams.len(), n_streams);
    }
}

#[test]
fn json_length_field_is_big_endian() {
    // The 4-byte length is stored big-endian (most-significant byte first).
    // read_header decodes it the same way; a little-endian regression would
    // request a wildly wrong JSON length.
    let meta = M2tsMeta::from_title(&video_title(HdrFormat::Sdr, ColorSpace::Bt709));
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    let json_len_be = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
    // Reconstruct the JSON object directly and confirm the length matches.
    let json = serde_json::to_vec(&meta).unwrap();
    assert_eq!(json_len_be, json.len());
}

#[test]
fn oversized_json_len_field_rejected_not_allocated() {
    // A header whose json_len field claims > 10 MiB must be rejected
    // (NoMetadata → InvalidInput) BEFORE the reader allocates a 10 MiB+
    // buffer for untrusted input.
    let mut buf = Vec::new();
    buf.extend_from_slice(&MAGIC);
    let huge = (10 * 1024 * 1024 + 1) as u32;
    buf.extend_from_slice(&huge.to_be_bytes());
    // No JSON body needed — the size check fires first.
    let mut cur = io::Cursor::new(buf);
    let err = read_header(&mut cur).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn truncated_json_body_errors_not_panics() {
    // magic + a json_len of 100 but no body → read_exact must surface a
    // UnexpectedEof error, never panic or return a half-filled meta.
    let mut buf = Vec::new();
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&100u32.to_be_bytes());
    // supply only 10 of the promised 100 JSON bytes.
    buf.extend_from_slice(&[b'{'; 10]);
    let mut cur = io::Cursor::new(buf);
    let err = read_header(&mut cur).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn malformed_json_body_is_no_metadata() {
    // Valid magic + valid length but the JSON itself is garbage → the
    // parse must fail with the numeric NoMetadata code, not panic and not
    // leak serde's English error into the io::Error.
    let bad = b"not json at all!"; // 16 bytes
    let mut buf = Vec::new();
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&(bad.len() as u32).to_be_bytes());
    buf.extend_from_slice(bad);
    let mut cur = io::Cursor::new(buf);
    let err = read_header(&mut cur).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput); // NoMetadata
}

#[test]
fn second_magic_byte_mismatch_is_none_not_error() {
    // First byte matches MAGIC[0] ('F') so we read 7 more, but the 4-byte
    // magic differs from "FMKV": per the reader contract this is "not FMKV"
    // → Ok(None), not an error. (Only a truncated read after 'F' errors.)
    let mut buf = vec![b'F', b'X', b'X', b'X', 0, 0, 0, 0];
    // pad so the 8-byte magic read succeeds.
    buf.extend_from_slice(&[0u8; 8]);
    let mut cur = io::Cursor::new(buf);
    let got = read_header(&mut cur).unwrap();
    assert!(got.is_none(), "non-FMKV 4-byte magic must be Ok(None)");
}

#[test]
fn read_header_consumes_exactly_one_packet_boundary() {
    // After read_header the reader must sit exactly at a 192-byte boundary
    // with none of the following data consumed. Append a sentinel TS sync
    // byte (0x47) after the header and confirm it is the very next byte.
    let meta = M2tsMeta::from_title(&video_title(HdrFormat::Hdr10, ColorSpace::Bt2020));
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    let header_len = buf.len();
    buf.push(0x47); // TS sync byte follows the header
    let mut cur = io::Cursor::new(buf);
    read_header(&mut cur).unwrap().expect("header present");
    assert_eq!(cur.position() as usize, header_len);
    assert_eq!(header_len % BD_SOURCE_PACKET_BYTES, 0);
    let mut next = [0u8; 1];
    use std::io::Read as _;
    cur.read_exact(&mut next).unwrap();
    assert_eq!(next[0], 0x47, "byte after header must be the TS sync byte");
}

#[test]
fn invalid_base64_codec_private_decodes_to_none() {
    // decode_codec_private treats invalid base64 as absent (None) rather
    // than failing the whole metadata parse — a corrupt init blob must not
    // sink an otherwise-good header.
    assert_eq!(decode_codec_private(&None), None);
    assert_eq!(
        decode_codec_private(&Some("!!!not base64!!!".to_string())),
        None
    );
    // Valid base64 round-trips to the raw bytes.
    use base64::Engine;
    let enc = base64::engine::general_purpose::STANDARD.encode([0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(
        decode_codec_private(&Some(enc)),
        Some(vec![0xDE, 0xAD, 0xBE, 0xEF])
    );
}

#[test]
fn video_codec_private_round_trips_through_header() {
    // A video stream's HEVCDecoderConfigurationRecord must survive from_title
    // → write_header → read_header → codec_privates(); otherwise an FMKV-driven
    // remux loses the hvcC and the MKV video track is undecodable.
    let mut t = video_title(HdrFormat::Hdr10, ColorSpace::Bt2020);
    t.codec_privates = vec![Some(vec![0x01, 0x02, 0x20, 0x00])]; // fake hvcC
    let meta = M2tsMeta::from_title(&t);
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    let mut cur = io::Cursor::new(buf);
    let back = read_header(&mut cur).unwrap().expect("header present");
    assert_eq!(
        back.codec_privates()[0].as_deref(),
        Some(&[0x01, 0x02, 0x20, 0x00][..]),
        "video codec_private (hvcC) must round-trip through the header"
    );
}

#[test]
fn duration_and_title_round_trip() {
    // Title string and duration must survive the JSON round-trip — these
    // populate the MKV Info element on remux.
    let mut t = video_title(HdrFormat::Sdr, ColorSpace::Bt709);
    t.playlist = "The Movie".into();
    t.duration_secs = 7384.5;
    let meta = M2tsMeta::from_title(&t);
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    let mut cur = io::Cursor::new(buf);
    let back = read_header(&mut cur).unwrap().unwrap().to_title();
    assert_eq!(back.playlist, "The Movie");
    assert_eq!(back.duration_secs, 7384.5);
}

// Timing switches the header to v2 (older readers refuse it cleanly); no
// timing keeps the v1 wire byte-identical.
#[test]
fn timing_round_trips_and_selects_the_header_version() {
    let t = video_title(HdrFormat::Sdr, ColorSpace::Bt709);
    let timing = crate::pes::TrackTiming {
        codec_delay_ns: 1,
        seek_preroll_ns: 2,
    };
    for (timings, version) in [(vec![], 1u8), (vec![timing], 2)] {
        let mut buf = Vec::new();
        write_header(&mut buf, &M2tsMeta::from_title(&t).with_timings(&timings)).unwrap();
        assert_eq!(buf[VERSION_BYTE], version);
        let back = read_header(&mut io::Cursor::new(&buf)).unwrap().unwrap();
        assert_eq!(back.frame_padding, version == 2);
        assert_eq!(back.timing(0), timings.first().copied().unwrap_or_default());
    }
}

// Non-default pid / frame_rate / hdr / sample_rate must survive the header.
#[test]
fn pid_frame_rate_hdr_and_sample_rate_round_trip() {
    use crate::disc::{AudioChannels, AudioStream, LabelPurpose, SampleRate};
    let mut t = video_title(HdrFormat::DolbyVision, ColorSpace::Bt2020);
    if let Stream::Video(v) = &mut t.streams[0] {
        v.pid = 0x1012;
        v.frame_rate = FrameRate::F59_94;
    }
    t.streams.push(Stream::Audio(AudioStream {
        pid: 0x1101,
        codec: Codec::Dts,
        channels: AudioChannels::Surround51,
        language: "eng".into(),
        sample_rate: SampleRate::S96,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    }));
    let mut buf = Vec::new();
    write_header(&mut buf, &M2tsMeta::from_title(&t)).unwrap();
    let back = read_header(&mut io::Cursor::new(buf))
        .unwrap()
        .unwrap()
        .to_title();
    match &back.streams[0] {
        Stream::Video(v) => {
            assert_eq!(v.pid, 0x1012);
            assert_eq!(v.frame_rate, FrameRate::F59_94);
            assert_eq!(v.hdr, HdrFormat::DolbyVision);
        }
        _ => panic!("expected video stream"),
    }
    match &back.streams[1] {
        Stream::Audio(a) => {
            assert_eq!(a.pid, 0x1101);
            assert_eq!(a.sample_rate, SampleRate::S96);
        }
        _ => panic!("expected audio stream"),
    }
}

// Timing on a later track with default tracks before it: the index is
// taken before the default filter, so it must not shift onto track 0/1.
#[test]
fn timing_keeps_its_track_index_past_default_tracks() {
    let t = video_title(HdrFormat::Sdr, ColorSpace::Bt709);
    let timing = crate::pes::TrackTiming {
        codec_delay_ns: 6_500_000,
        seek_preroll_ns: 80_000_000,
    };
    let timings = [
        crate::pes::TrackTiming::default(),
        crate::pes::TrackTiming::default(),
        timing,
    ];
    let mut buf = Vec::new();
    write_header(&mut buf, &M2tsMeta::from_title(&t).with_timings(&timings)).unwrap();
    let back = read_header(&mut io::Cursor::new(&buf)).unwrap().unwrap();
    assert_eq!(back.timing(2), timing);
    assert_eq!(back.timing(0), crate::pes::TrackTiming::default());
    assert_eq!(back.timing(1), crate::pes::TrackTiming::default());
}

#[test]
fn read_header_caps_untrusted_timings() {
    let t = video_title(HdrFormat::Sdr, ColorSpace::Bt709);
    let mut meta = M2tsMeta::from_title(&t);
    meta.timings = (0..5000)
        .map(|track| MetaTiming {
            track,
            codec_delay_ns: 1,
            seek_preroll_ns: 1,
        })
        .collect();
    let mut buf = Vec::new();
    write_header(&mut buf, &meta).unwrap();
    let back = read_header(&mut io::Cursor::new(&buf)).unwrap().unwrap();
    assert!(back.timings.len() <= 256);
}

// Everything a receiver's muxer uses must survive the FMKV header.
fn full_title() -> DiscTitle {
    let mut t = video_title(HdrFormat::Sdr, ColorSpace::Bt709);
    if let Stream::Video(v) = &mut t.streams[0] {
        v.display_aspect = Some((16, 9));
        v.measured_cicp = Some(crate::disc::MeasuredCicp {
            matrix: 6,
            transfer: 6,
            primaries: 5,
            range: 1,
        });
    }
    t.streams.push(Stream::Audio(AudioStream {
        pid: 0x80,
        codec: Codec::Ac3,
        channels: crate::disc::AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: crate::disc::SampleRate::S48,
        secondary: false,
        purpose: crate::disc::LabelPurpose::Commentary,
        label: String::new(),
    }));
    t.streams.push(Stream::Subtitle(SubtitleStream {
        pid: 0x20,
        codec: Codec::DvdSub,
        language: "eng".into(),
        forced: false,
        qualifier: crate::disc::LabelQualifier::Sdh,
        codec_data: None,
    }));
    t.chapters = vec![
        crate::disc::Chapter {
            time_secs: 0.0,
            name: "1".into(),
        },
        crate::disc::Chapter {
            time_secs: 312.5,
            name: "2".into(),
        },
    ];
    t.content_format = crate::disc::ContentFormat::MpegPs;
    t
}

fn over_the_wire(t: &DiscTitle) -> DiscTitle {
    let mut buf = Vec::new();
    write_header(&mut buf, &M2tsMeta::from_title(t)).unwrap();
    read_header(&mut io::Cursor::new(buf))
        .unwrap()
        .expect("header")
        .to_title()
}

#[test]
fn display_shape_colour_labels_chapters_and_format_survive_the_header() {
    let back = over_the_wire(&full_title());
    let Stream::Video(v) = &back.streams[0] else {
        panic!("video first")
    };
    assert_eq!(v.display_aspect, Some((16, 9)));
    assert_eq!(
        v.measured_cicp
            .map(|c| (c.matrix, c.transfer, c.primaries, c.range)),
        Some((6, 6, 5, 1))
    );
    let Stream::Audio(a) = &back.streams[1] else {
        panic!("audio second")
    };
    assert_eq!(a.purpose, crate::disc::LabelPurpose::Commentary);
    let Stream::Subtitle(s) = &back.streams[2] else {
        panic!("subtitle third")
    };
    assert_eq!(s.qualifier, crate::disc::LabelQualifier::Sdh);
    let marks: Vec<_> = back
        .chapters
        .iter()
        .map(|c| (c.time_secs, c.name.as_str()))
        .collect();
    assert_eq!(marks, [(0.0, "1"), (312.5, "2")]);
    assert_eq!(back.content_format, crate::disc::ContentFormat::MpegPs);
}

#[test]
fn a_header_without_the_optional_fields_still_reads_with_the_old_defaults() {
    let old = r#"{"v":1,"title":"t","duration":1.0,"streams":[
            {"type":"video","pid":4113,"codec":"hevc"},
            {"type":"audio","pid":128,"codec":"ac3"},
            {"type":"subtitle","pid":32,"codec":"pgs"}]}"#;
    let t = serde_json::from_str::<M2tsMeta>(old).unwrap().to_title();
    let Stream::Video(v) = &t.streams[0] else {
        panic!("video")
    };
    assert_eq!((v.display_aspect, v.measured_cicp), (None, None));
    assert!(t.chapters.is_empty());
    assert_eq!(t.content_format, crate::disc::ContentFormat::BdTs);
}

#[test]
fn unknown_fields_from_a_newer_writer_are_ignored() {
    let newer = r#"{"v":1,"title":"t","duration":1.0,"future":[1],"streams":[
            {"type":"audio","pid":128,"codec":"ac3","future":{"x":1}}]}"#;
    assert_eq!(
        serde_json::from_str::<M2tsMeta>(newer)
            .unwrap()
            .streams
            .len(),
        1
    );
}

#[test]
fn a_non_finite_chapter_time_does_not_break_the_header() {
    let mut t = full_title();
    t.chapters[1].time_secs = f64::NAN;
    let back = over_the_wire(&t);
    assert_eq!(back.chapters.len(), 1);
}
