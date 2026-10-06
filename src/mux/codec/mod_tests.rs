use super::*;

// Design §4 step 1: "MPEG-1 video is `Codec::Mpeg1`, which is routed to `Mpeg2Parser`
// (the 11172-2 syntax is the 13818-2 syntax without extension start codes)"; the parser
// must accept a sequence header with no sequence_extension and frame each picture.
#[test]
fn mpeg1_video_is_framed_by_the_mpeg2_parser() {
    use crate::mux::decode_ts::test_es::{mpeg1_seq, mpeg2_pic};
    let mut es = mpeg1_seq(3);
    es.extend(mpeg2_pic(1, 3));
    es.extend(mpeg2_pic(2, 3));
    es.extend(mpeg2_pic(3, 3));
    let pes = crate::mux::ts::PesPacket {
        source: None,
        pid: 0xE0,
        pts: Some(9_000),
        dts: None,
        data: es,
        discontinuity: false,
    };
    let mut p = parser_for_codec(Codec::Mpeg1, None, true);
    let mut frames = p.parse(&pes);
    frames.extend(p.flush());
    assert_eq!(frames.len(), 3, "one frame per picture, not one per PES");
    assert!(frames[0].keyframe, "the I picture is a keyframe");
    assert_eq!(p.codec_private(), Some(mpeg1_seq(3)));
}

// Per design J16/J20 (MPG3-5, MPG4-2); do not change without a spec citation proving
// otherwise. Every 90 kHz tick survives pts_to_ns → ns_to_ticks exactly, including
// every residue mod 9 (100 000 ≡ 1 mod 9) and negative ticks.
#[test]
fn disc_ticks_round_trip_through_ns_exactly() {
    let sweep = (-200_000i64..200_000)
        .chain((0..9).map(|r| (1i64 << 33) - 9 + r))
        .chain((0..9).map(|r| -(1i64 << 33) + r))
        .chain([i64::from(u32::MAX), 95_443_717_i64 * 9 + 4]);
    for p in sweep {
        assert_eq!(
            ns_to_ticks(pts_to_ns(p)),
            p,
            "tick {p} (residue {})",
            p.rem_euclid(9)
        );
    }
}

// Round half up on the signed line: no saturation inside the helper (MPG4-2), so a
// negative absolute PTS keeps its distance from the origin.
#[test]
fn ns_to_ticks_rounds_to_nearest_signed() {
    assert_eq!(ns_to_ticks(0), 0);
    assert_eq!(ns_to_ticks(5_555), 0, "0.49995 tick");
    assert_eq!(ns_to_ticks(5_556), 1, "0.50004 tick");
    assert_eq!(ns_to_ticks(-5_555), 0, "−0.49995 tick");
    assert_eq!(ns_to_ticks(-5_556), -1, "−0.50004 tick");
    assert_eq!(ns_to_ticks(-40_000_000), -3_600);
    assert_eq!(ns_to_ticks(-80_000_000), -7_200);
    assert_eq!(
        ns_to_ticks(i64::MIN),
        i64::MIN
            .saturating_mul(9)
            .saturating_add(50_000)
            .div_euclid(100_000)
    );
}

fn pes(pts: Option<i64>, data: Vec<u8>) -> PesPacket {
    PesPacket {
        source: None,
        pid: 0x1011,
        pts,
        dts: None,
        data,
        discontinuity: false,
    }
}

#[test]
fn unhandled_video_codecs_use_non_keyframe_passthrough() {
    // Av1 has no dedicated parser. It must NOT be marked all-keyframe (that would
    // explode Cues density and mislead seeking); the non-keyframe passthrough is the
    // safe fallback. (MPEG-1 has the MPEG-2 parser: mpg-output-design v5 §4 step 1.)
    let codec = Codec::Av1;
    let mut parser = parser_for_codec(codec, None, false);
    let frames = parser.parse(&pes(Some(9000), vec![0xDE, 0xAD, 0xBE, 0xEF]));
    assert_eq!(frames.len(), 1, "{codec:?}");
    assert!(
        !frames[0].keyframe,
        "{codec:?} must not be flagged keyframe by the fallback parser"
    );
    assert_eq!(frames[0].data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

// codec_private() feeds MKV CodecPrivate (RFC 9559 §5.1.4.1.24): Some vs None are NOT
// interchangeable.
#[test]
fn parsers_that_derive_no_config_report_absent_never_an_empty_codec_private() {
    // Parsers with no config extraction, each fed a REAL frame so the gate
    // keeps it (a rejected frame proves nothing). AAC derives an ASC, so
    // it is tested separately below.
    let cases: [(Codec, Vec<u8>); 4] = [
        // MPEG-1 Layer II, 44.1 kHz, 128 kbit/s, stereo.
        (Codec::Mp2, vec![0xFF, 0xFD, 0x70, 0x00, 0x00, 0x00]),
        // MPEG-1 Layer III, 44.1 kHz, 128 kbit/s, stereo.
        (Codec::Mp3, vec![0xFF, 0xFB, 0x90, 0x00, 0x00, 0x00]),
        // Not a FLAC frame sync, so the gate passes it through unvalidated —
        // still the keep path, and still no configuration derived.
        (Codec::Flac, vec![0x01, 0x02, 0x03, 0x04]),
        // Opus rides the all-keyframe passthrough parser.
        (Codec::Opus, vec![0x78, 0x01, 0x02, 0x03]),
    ];

    for (codec, payload) in cases {
        let mut parser = parser_for_codec(codec, None, false);
        assert_eq!(
            parser.codec_private(),
            None,
            "{codec:?}: no config before any frame"
        );
        parser.parse(&pes(Some(0), payload.clone()));
        parser.parse(&pes(Some(90_000), payload));
        assert_eq!(
            parser.codec_private(),
            None,
            "{codec:?}: this parser derives no config, so it must report the \
                 CodecPrivate as ABSENT — an empty or invented Some would be \
                 written into the track header as if it were real"
        );
        // Flushing at end of stream must not conjure one either.
        parser.flush();
        assert_eq!(parser.codec_private(), None, "{codec:?}: after flush");
    }
}

// A valid ADTS frame (AAC-LC, 44.1 kHz, stereo, frame_length 7) must yield
// its AudioSpecificConfig as soon as the frame is parsed.
#[test]
fn aac_parser_derives_audio_specific_config_from_first_frame() {
    let mut parser = parser_for_codec(Codec::Aac, None, false);
    assert_eq!(parser.codec_private(), None);
    let frames = parser.parse(&pes(
        Some(0),
        vec![0xFF, 0xF1, 0x50, 0x80, 0x00, 0xFF, 0xFC],
    ));
    assert_eq!(frames.len(), 1, "the frame is valid, not dropped");
    assert_eq!(parser.codec_private(), Some(vec![0x12, 0x10]));
}

#[test]
fn audio_codecs_emit_keyframe_frames() {
    // PES = frame audio: every frame is independently decodable → keyframe.
    // Aac/Mp2/Mp3/Flac go through their dedicated gating parsers (which pass a
    // non-sync/too-short payload straight through); Opus uses PassthroughParser.
    for codec in [Codec::Aac, Codec::Mp2, Codec::Mp3, Codec::Flac, Codec::Opus] {
        let mut parser = parser_for_codec(codec, None, false);
        let frames = parser.parse(&pes(Some(0), vec![0x01, 0x02]));
        assert_eq!(frames.len(), 1, "{codec:?}");
        assert!(frames[0].keyframe, "{codec:?} should be keyframe");
    }
}
