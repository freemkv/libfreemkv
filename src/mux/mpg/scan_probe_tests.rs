use super::*;

// A sequence header for `w`x`h` with aspect code 1 and frame-rate code `rate`, then a
// sequence extension whose progressive_sequence bit is `progressive`.
fn video(w: u32, h: u32, rate: u8, progressive: bool) -> Vec<u8> {
    vec![
        0,
        0,
        1,
        0xB3,
        (w >> 4) as u8,
        ((w & 0xF) << 4) as u8 | (h >> 8) as u8,
        h as u8,
        0x10 | rate,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0xB5,
        0x14,
        if progressive { 0x8A } else { 0x82 },
    ]
}

#[test]
fn frame_rate_codes_map_in_order() {
    let want = [
        FrameRate::F23_976,
        FrameRate::F24,
        FrameRate::F25,
        FrameRate::F29_97,
        FrameRate::F30,
        FrameRate::F50,
        FrameRate::F59_94,
        FrameRate::F60,
    ];
    for (i, rate) in want.into_iter().enumerate() {
        let es = video(720, 576, i as u8 + 1, false);
        assert_eq!(
            probe_video(&es, Some(0x02)).unwrap().2,
            rate,
            "code {}",
            i + 1
        );
    }
    let es = video(720, 576, 0, false);
    assert_eq!(probe_video(&es, Some(0x02)).unwrap().2, FrameRate::Unknown);
}

#[test]
fn scan_type_follows_the_progressive_bit() {
    let res = |h, p| probe_video(&video(720, h, 3, p), Some(0x02)).unwrap().1;
    assert_eq!(res(480, false), Resolution::R480i);
    assert_eq!(res(576, false), Resolution::R576i);
    assert_eq!(res(576, true), Resolution::R576p);
    assert_eq!(res(1080, false), Resolution::R1080i);
    assert_eq!(res(1080, true), Resolution::R1080p);
}

#[test]
fn mpeg_audio_rate_and_layer_come_from_the_header() {
    let probe = |b1: u8, b2: u8| {
        let (codec, _, rate) = probe_mpeg_audio(&[0xFF, b1, b2, 0xC4, 0, 0]);
        (codec, rate)
    };
    // MPEG-1 (ID bit set): 44.1, 48, 32 kHz; Layer II is 0xFD, Layer III 0xFB.
    assert_eq!(probe(0xFD, 0x00), (Codec::Mp2, SampleRate::S44_1));
    assert_eq!(probe(0xFD, 0x04), (Codec::Mp2, SampleRate::S48));
    assert_eq!(probe(0xFD, 0x08), (Codec::Mp2, SampleRate::Unknown));
    assert_eq!(probe(0xFB, 0x00), (Codec::Mp3, SampleRate::S44_1));
    assert_eq!(probe(0xFB, 0x04), (Codec::Mp3, SampleRate::S48));
    // LSF (ID bit clear) never reads the MPEG-1 table.
    assert_eq!(probe(0xF5, 0x00), (Codec::Mp2, SampleRate::Unknown));
    assert_eq!(probe(0xF5, 0x04), (Codec::Mp2, SampleRate::Unknown));
}

#[test]
fn lpcm_rate_and_channels_come_from_the_header() {
    let probe = |b: u8| probe_lpcm(&[0, b, 0]);
    assert_eq!(probe(0x01), (AudioChannels::Stereo, SampleRate::S48));
    assert_eq!(probe(0x11), (AudioChannels::Stereo, SampleRate::S96));
    assert_eq!(probe(0x21), (AudioChannels::Stereo, SampleRate::S44_1));
    assert_eq!(probe(0x31), (AudioChannels::Stereo, SampleRate::Unknown));
}

// A crafted map whose declared lengths overrun the packet is refused, never indexed.
#[test]
fn a_map_with_overrunning_lengths_is_refused() {
    let ok = pack::psm(
        &[],
        &[pack::PsmEntry {
            stream_type: 0x02,
            stream_id: 0xE0,
            descriptors: vec![],
        }],
    )
    .unwrap();
    assert!(parse_map(&ok).is_some());
    let refuse = |at: usize, v: u8| {
        let mut m = ok.clone();
        m[at] = v;
        let n = m.len() - 4;
        let crc = pack::crc32(&m[..n]);
        m[n..].copy_from_slice(&crc.to_be_bytes());
        assert!(parse_map(&m).is_none(), "byte {at} = {v}");
    };
    // program_stream_info_length, elementary_stream_map_length, then an entry's
    // info length: each past the end of the packet.
    refuse(9, 0xF0);
    refuse(11, 0xF0);
    let info = usize::from(u16::from_be_bytes([ok[8], ok[9]]));
    refuse(12 + info + 3, 0xF0);
}
