use super::*;

/// Build a synthetic TT_SRPT at offset 0: a declared `u16` title count
/// followed by `entries` 12-byte records of
/// `(num_chapters, vts_number, vts_title_num)`.
fn tt_srpt_bytes(declared: u16, entries: &[(u16, u8, u8)]) -> Vec<u8> {
    let mut data = vec![0u8; 8 + entries.len() * 12];
    data[0..2].copy_from_slice(&declared.to_be_bytes());
    for (i, &(chapters, vts, title)) in entries.iter().enumerate() {
        let base = 8 + i * 12;
        data[base + 2..base + 4].copy_from_slice(&chapters.to_be_bytes());
        data[base + 6] = vts;
        data[base + 7] = title;
    }
    data
}

#[test]
fn tt_srpt_title_count_is_capped_and_deduplicated() {
    // The TT_SRPT count is an untrusted u16. Entries must be DISTINCT: identical
    // entries would let dedup alone collapse them to one title, so the cap would
    // never be exercised — vary both u8 fields for ~65025 distinct pairs.
    let entries: Vec<(u16, u8, u8)> = (0..u16::MAX)
        .map(|i| (5u16, (i % 255) as u8 + 1, ((i / 255) % 255) as u8 + 1))
        .collect();
    let data = tt_srpt_bytes(u16::MAX, &entries);
    let map = parse_tt_srpt(&data, 0).expect("well-formed TT_SRPT");
    let total: usize = map.values().map(|v| v.len()).sum();
    // Asserted against the DVD-Video max as a LITERAL, not MAX_TT_SRPT_TITLES:
    // comparing to the constant under test would pass vacuously if that constant
    // were raised. 99 is the format's own ceiling, pinning the cap to the spec.
    assert!(
        total <= 99,
        "TT_SRPT produced {total} titles from a crafted 65535-entry table; \
             DVD-Video allows at most 99"
    );
    // De-duplication, on its own fixture: the same VTS title named 65535
    // times is one title, not many. Kept separate from the cap assertion
    // above so neither guard can mask the other.
    let dupes = vec![(5u16, 1u8, 1u8); u16::MAX as usize];
    let dup_map = parse_tt_srpt(&tt_srpt_bytes(u16::MAX, &dupes), 0).expect("well-formed TT_SRPT");
    assert_eq!(
        dup_map.get(&1).map(Vec::len),
        Some(1),
        "duplicate (vts_number, vts_title_num) entries must collapse"
    );
}

#[test]
fn tt_srpt_admits_a_full_99_title_disc() {
    // The DVD-Video maximum must still parse in full: 99 distinct titles
    // spread over two title sets.
    let entries: Vec<(u16, u8, u8)> = (0..99u8)
        .map(|i| (3u16, if i < 50 { 1 } else { 2 }, i + 1))
        .collect();
    let data = tt_srpt_bytes(99, &entries);
    let map = parse_tt_srpt(&data, 0).expect("well-formed TT_SRPT");
    let total: usize = map.values().map(|v| v.len()).sum();
    assert_eq!(total, 99, "a full 99-title disc must survive the cap");
    assert_eq!(map.get(&1).map(Vec::len), Some(50));
    assert_eq!(map.get(&2).map(Vec::len), Some(49));
}

#[test]
fn bcd_to_secs_basic() {
    // 1 hour, 23 minutes, 45 seconds, 0 frames at 25fps
    let bcd = [0x01, 0x23, 0x45, 0b01_000000];
    let secs = bcd_to_secs(&bcd);
    let expected = 1.0 * 3600.0 + 23.0 * 60.0 + 45.0;
    assert!((secs - expected).abs() < 0.01, "got {}", secs);
}

#[test]
fn bcd_to_secs_with_frames() {
    // 0 hours, 1 minute, 30 seconds, 15 frames of NTSC timecode.
    let bcd = [0x00, 0x01, 0x30, 0b11_010101];
    let secs = bcd_to_secs(&bcd);
    // 0b010101 = 0x15 BCD = 15 frames. Timecode 00:01:30:15 = 2715 frames at
    // 1001/30000 s each = 90.5905 s (NOT 90.5 s — NTSC runs 0.1% slow).
    let expected = 2715.0 * 1001.0 / 30000.0;
    assert!((secs - expected).abs() < 1e-9, "got {}", secs);
    assert!((secs - 90.5905).abs() < 1e-9, "got {}", secs);
}

/// The NTSC 0.1% pull-down must be applied to the whole timecode, not just
/// the frame field. Regression test for issue freemkv#25: a title's chapter marks
/// drifted ~3.6 s per hour because H:M:S was read as literal seconds.
#[test]
fn bcd_ntsc_timecode_is_not_literal_seconds() {
    // One hour of NTSC timecode = 3603.6 s of real time.
    let one_hour = [0x01, 0x00, 0x00, 0b11_000000];
    assert!((bcd_to_secs(&one_hour) - 3603.6).abs() < 1e-6);
    // 00:01:02:00 -> 1860 frames -> 62.062 s (the issue freemkv#25 chapter 2 value).
    let ch2 = [0x00, 0x01, 0x02, 0b11_000000];
    assert!((bcd_to_secs(&ch2) - 62.062).abs() < 1e-9);
    // A 20-minute NTSC episode cell is 00:19:58:24 = 35964 frames.
    let ep = [0x00, 0x19, 0x58, 0b11_100100];
    assert_eq!(bcd_to_frames(&ep).unwrap().0, 35964);
    assert!((bcd_to_secs(&ep) - 1199.9988).abs() < 1e-6);
}

/// PAL timecode is exact (25 frames = 1.000 s), so the fix must leave every
/// PAL value bit-identical.
#[test]
fn bcd_pal_unaffected_by_ntsc_fix() {
    let pal = [0x01, 0x23, 0x45, 0b01_000000];
    assert!((bcd_to_secs(&pal) - (3600.0 + 23.0 * 60.0 + 45.0)).abs() < 1e-9);
    let pal_frames = [0x00, 0x00, 0x10, 0b01_010010]; // 10 s + 12 frames
    assert!((bcd_to_secs(&pal_frames) - 10.48).abs() < 1e-9);
    let r = DvdRate::from_flag(0x01).unwrap();
    assert_eq!((r.nominal_fps, r.frame_num, r.frame_den), (25, 1, 25));
}

#[test]
fn dvd_rate_from_flag_rejects_reserved() {
    assert!(DvdRate::from_flag(0x00).is_none());
    assert!(DvdRate::from_flag(0x02).is_none());
    let n = DvdRate::from_flag(0x03).unwrap();
    assert_eq!((n.nominal_fps, n.frame_num, n.frame_den), (30, 1001, 30000));
}

#[test]
fn bcd_to_frames_none_on_short_or_unknown_rate() {
    assert!(bcd_to_frames(&[0x00, 0x00]).is_none());
    assert!(bcd_to_frames(&[0x00, 0x00, 0x00, 0b00_000000]).is_none());
    assert!(bcd_to_frames(&[0x00, 0x00, 0x00, 0b10_000000]).is_none());
}

#[test]
fn bcd_to_secs_zero() {
    let bcd = [0x00, 0x00, 0x00, 0x00];
    assert_eq!(bcd_to_secs(&bcd), 0.0);
}

#[test]
fn bcd_to_secs_short_input() {
    assert_eq!(bcd_to_secs(&[0x01, 0x02]), 0.0);
    assert_eq!(bcd_to_secs(&[]), 0.0);
}

#[test]
fn bcd_to_secs_invalid_bcd_digits() {
    // 0xFF has hi=15, lo=15 — both > 9, should return 0 for that byte
    let bcd = [0xFF, 0x01, 0x02, 0b01_000000];
    let secs = bcd_to_secs(&bcd);
    // hours=0 (invalid), minutes=1, seconds=2
    let expected = 0.0 + 60.0 + 2.0;
    assert!((secs - expected).abs() < 0.01, "got {}", secs);
}

#[test]
fn bcd_byte_valid() {
    assert_eq!(bcd_byte(0x00), 0);
    assert_eq!(bcd_byte(0x09), 9);
    assert_eq!(bcd_byte(0x10), 10);
    assert_eq!(bcd_byte(0x59), 59);
    assert_eq!(bcd_byte(0x99), 99);
}

#[test]
fn bcd_byte_invalid() {
    assert_eq!(bcd_byte(0xAA), 0);
    assert_eq!(bcd_byte(0x0F), 0);
    assert_eq!(bcd_byte(0xF0), 0);
}

#[test]
fn be_helpers_bounds_check() {
    let data = [0x00, 0x01, 0x02];
    assert!(be_u16(&data, 0).is_ok());
    assert!(be_u16(&data, 1).is_ok());
    assert!(be_u16(&data, 2).is_err()); // only 1 byte left
    assert!(be_u32(&data, 0).is_err()); // only 3 bytes
}

/// mpucoder PGC_SPST_CTL: "Stream number for 4:3", "for wide", "for letterbox", "for
/// pan&scan" in bytes 0-3. This is per spec; do not change without a spec citation proving
/// otherwise.
#[test]
fn subpicture_stream_id_selects_by_aspect_and_presence() {
    let ctl = 0x9FE1_0203; // reserved bits set in bytes 0-1 must be masked off
    // vm_get_subp_stream "/* 4:3 */": ">> 24) & 0x1f".
    assert_eq!(subpicture_stream_id(ctl, DvdAspect::R4x3), Some(0x3F));
    // "mode == 0 - widescreen": ">> 16) & 0x1f".
    assert_eq!(subpicture_stream_id(ctl, DvdAspect::R16x9), Some(0x21));
    // ifo_print: "if(pgc->subp_control[i] & 0x80000000) { /* The 'is present' bit */".
    assert_eq!(subpicture_stream_id(0x0001_0203, DvdAspect::R16x9), None);
    assert_eq!(
        subpicture_stream_id(0x8000_0000, DvdAspect::R4x3),
        Some(0x20)
    );
}

/// This is per spec; do not change without a spec citation proving otherwise.
#[test]
fn audio_stream_number_reads_present_bit_and_low_three_bits() {
    // vm_get_audio_stream: "streamN = ((vm->state).pgc->audio_control[audioN] >> 8) & 0x07;"
    assert_eq!(audio_stream_number(0x8000), Some(0));
    assert_eq!(audio_stream_number(0xFFFF), Some(7));
    assert_eq!(audio_stream_number(0x8A00), Some(2));
    // ifo_print: "if(pgc->audio_control[i] & 0x8000) { /* The 'is present' bit */".
    assert_eq!(audio_stream_number(0x7FFF), None);
    // VLC ps.h "0x80 -> 0x87" (AC-3), "0x88 -> 0x8f" (DTS); mpucoder LPCM "1010 0***b".
    assert_eq!(audio_sub_stream_id(Codec::Ac3, 3), Some(0x83));
    assert_eq!(audio_sub_stream_id(Codec::Dts, 3), Some(0x8B));
    assert_eq!(audio_sub_stream_id(Codec::Lpcm, 3), Some(0xA3));
    // mpucoder PES: MPEG audio is "0xC0 - 0xDF", not a private stream 1 sub-stream.
    assert_eq!(audio_sub_stream_id(Codec::Mp2, 3), None);
}

// mpucoder PGC: "000C PGC_AST_CTL 8*2 Audio Stream Control".
#[test]
fn pgc_parses_ast_ctl_at_0x0c() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x0B] = 0xFF; // last byte of the PGC's prohibited-user-ops field
    pgc[0x0C..0x0E].copy_from_slice(&0x8100u16.to_be_bytes());
    pgc[0x1A..0x1C].copy_from_slice(&0x8700u16.to_be_bytes());
    pgc[0x1C] = 0xFF; // first byte of SPST_CTL; must not bleed in
    let t = parse_pgc(&pgc, 0, 1).unwrap();
    assert_eq!(t.ast_ctl, [0x8100, 0, 0, 0, 0, 0, 0, 0x8700]);
}

// mpucoder PGC: "001C PGC_SPST_CTL 32*4 Subpicture Stream Control".
#[test]
fn pgc_parses_spst_ctl_at_0x1c() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x1B] = 0xFF; // last byte of PGC_AST_CTL; must not bleed in
    pgc[0x1C..0x20].copy_from_slice(&0x8003_0405u32.to_be_bytes());
    pgc[0x98..0x9C].copy_from_slice(&0x8000_1F00u32.to_be_bytes());
    let t = parse_pgc(&pgc, 0, 1).unwrap();
    assert_eq!(t.spst_ctl[0], 0x8003_0405);
    assert_eq!(t.spst_ctl[31], 0x8000_1F00);
    assert!(t.spst_ctl[1..31].iter().all(|&c| c == 0));
}

#[test]
fn pgc_parses_duration_from_correct_offset() {
    // Build a minimal PGC: 0xEA bytes minimum
    // PGC layout: 0x02 = nr_programs, 0x03 = nr_cells, 0x04-0x07 = BCD time
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 1; // 1 program
    pgc[0x03] = 2; // 2 cells
    // 1h 59m 30s at 29.97fps, 0 frames
    pgc[0x04] = 0x01; // hours BCD
    pgc[0x05] = 0x59; // minutes BCD
    pgc[0x06] = 0x30; // seconds BCD
    pgc[0x07] = 0b11_000000; // 29.97fps, 0 frames
    // Cell playback info offset at PGC+0xE8
    let cell_offset: u16 = 0xEA; // right after minimum header
    pgc[0xE8] = (cell_offset >> 8) as u8;
    pgc[0xE9] = cell_offset as u8;
    // Add 2 cells (24 bytes each)
    pgc.resize(pgc.len() + 48, 0);
    // Cell 0: sectors 100-200
    let co = 0xEA;
    pgc[co + 8] = 0;
    pgc[co + 9] = 0;
    pgc[co + 10] = 0;
    pgc[co + 11] = 100; // first sector
    pgc[co + 20] = 0;
    pgc[co + 21] = 0;
    pgc[co + 22] = 0;
    pgc[co + 23] = 200; // last sector
    // Cell 1: sectors 300-400
    let co = 0xEA + 24;
    pgc[co + 8] = 0;
    pgc[co + 9] = 0;
    pgc[co + 10] = 1;
    pgc[co + 11] = 44; // first sector = 300
    pgc[co + 20] = 0;
    pgc[co + 21] = 0;
    pgc[co + 22] = 1;
    pgc[co + 23] = 144; // last sector = 400

    let title = parse_pgc(&pgc, 0, 5).unwrap();
    // 01:59:30:00 is NTSC *timecode*, so 7170 timecode seconds = 215100
    // frames, and the real running time is 215100 * 1001/30000 = 7177.17 s.
    // Reading the BCD as literal seconds (7170.0) was the issue freemkv#25 bug.
    let expected = 7170.0 * 30.0 * 1001.0 / 30000.0;
    assert!(
        (title.duration_secs - expected).abs() < 1e-6,
        "expected ~{expected}s, got {}s",
        title.duration_secs
    );
    assert!((title.duration_secs - 7177.17).abs() < 1e-6);
    assert_eq!(title.chapters, 5);
    assert_eq!(title.cells.len(), 2);
    assert_eq!(title.cells[0].first_sector, 100);
    assert_eq!(title.cells[0].last_sector, 200);
    assert_eq!(title.cells[1].first_sector, 300);
    assert_eq!(title.cells[1].last_sector, 400);
}

#[test]
fn video_attr_parsing() {
    let mut data = vec![0u8; 0x204];
    data[0x200] = v_atr_byte(VIDEO_FORMAT_NTSC, ASPECT_16X9);
    let attr = parse_video_attr(&data).unwrap();
    assert_eq!(attr.standard, TvSystem::Ntsc);
    assert_eq!(attr.aspect, DvdAspect::R16x9);
    assert_eq!(attr.resolution, Resolution::R480i);
    assert_eq!(attr.codec, Codec::Mpeg2);
}

#[test]
fn video_attr_pal() {
    let mut data = vec![0u8; 0x204];
    data[0x200] = v_atr_byte(VIDEO_FORMAT_PAL, ASPECT_4X3);
    let attr = parse_video_attr(&data).unwrap();
    assert_eq!(attr.standard, TvSystem::Pal);
    assert_eq!(attr.aspect, DvdAspect::R4x3);
    assert_eq!(attr.resolution, Resolution::R576i);
}

// Regression: PAL 16:9 anamorphic disc (Silence of the Lambs UK SKU). Old
// code read TV system from bits 1-0 (permitted_df) and reported NTSC/480i
// — only NTSC discs, where the wrong bits coincide on 0, were tested.
#[test]
fn video_attr_pal_16x9_anamorphic() {
    let mut data = vec![0u8; 0x204];
    data[0x200] = v_atr_byte(VIDEO_FORMAT_PAL, ASPECT_16X9);
    let attr = parse_video_attr(&data).unwrap();
    assert_eq!(attr.standard, TvSystem::Pal);
    assert_eq!(attr.aspect, DvdAspect::R16x9);
    assert_eq!(attr.resolution, Resolution::R576i);
}

// ABSOLUTE-BYTE pin: other tests build the byte via v_atr_byte(...), which shares the
// parser's shift constants, so a co-edit could hide a bug. This feeds parse_video_attr
// HARDCODED real-layout bytes instead.
#[test]
fn video_attr_absolute_bytes_pin_real_layout() {
    // (byte @0x200, expected standard, expected aspect, expected resolution):
    // PAL 16:9=0x1C, PAL 4:3=0x10, NTSC 16:9=0x0C, NTSC 4:3=0x00. A real disc
    // also sets mpeg_version=01 in bits 7-6 (|0x40), which the parser must ignore.
    let cases: &[(u8, TvSystem, DvdAspect, Resolution)] = &[
        (0x1C, TvSystem::Pal, DvdAspect::R16x9, Resolution::R576i),
        (0x10, TvSystem::Pal, DvdAspect::R4x3, Resolution::R576i),
        (0x0C, TvSystem::Ntsc, DvdAspect::R16x9, Resolution::R480i),
        (0x00, TvSystem::Ntsc, DvdAspect::R4x3, Resolution::R480i),
        // mpeg_version=2 (MPEG-2) in bits 7-6 must not perturb the read.
        (0x5C, TvSystem::Pal, DvdAspect::R16x9, Resolution::R576i),
    ];
    for &(b0, std, aspect, res) in cases {
        let mut data = vec![0u8; 0x204];
        data[0x200] = b0;
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, std, "byte {b0:#04x} → standard");
        assert_eq!(attr.aspect, aspect, "byte {b0:#04x} → aspect");
        assert_eq!(attr.resolution, res, "byte {b0:#04x} → resolution");
    }
    // Anti-bug anchor: the original bug read TV system from bits 1-0
    // (permitted_df). A byte with low bits set but format=NTSC (0x03, df=11)
    // must stay NTSC here, proving the low bits are ignored.
    let mut df = vec![0u8; 0x204];
    df[0x200] = 0x03; // format=NTSC(00), df=11
    assert_eq!(
        parse_video_attr(&df).unwrap().standard,
        TvSystem::Ntsc,
        "permitted_df bits (1-0) must NOT be read as the TV system"
    );
}

#[test]
fn audio_attr_parsing() {
    let mut data = vec![0u8; 16];
    // AC3 (coding=0), 48kHz (rate=0), 6 channels (stored as 5)
    // b0: bits 7-5=000(AC3), bits 4-3=00(48k) => 0x00
    data[0] = 0x00;
    // b1: bits 2-0=101 (channels-1=5) => 0x05
    data[1] = 0x05;
    // on-disc language "en" (ISO 639-1) -> parsed as ISO 639-2 "eng"
    data[2] = b'e';
    data[3] = b'n';

    let attr = parse_audio_attr(&data, 0).unwrap();
    assert_eq!(attr.codec, Codec::Ac3);
    assert_eq!(attr.sample_rate, 48000);
    assert_eq!(attr.channels, 6);
    assert_eq!(attr.language, "eng");
}

// Positional fallback: codec base | position, the wire ids the demux routes on.
#[test]
fn mixed_codec_positional_pids_are_distinct() {
    let codecs = [Codec::Ac3, Codec::Dts, Codec::Lpcm, Codec::Ac3, Codec::Mp2];
    let pids: Vec<Option<u16>> = (0u8..).zip(codecs).map(|(n, c)| audio_pid(c, n)).collect();
    assert_eq!(
        pids,
        [0xBD80, 0xBD89, 0xBDA2, 0xBD83, 0x00C4].map(Some).to_vec()
    );
}

// Regression (The Punisher 2004): DTS at physical stream 1 is sub-id 0x89 (0x88|1), not
// the old per-codec 0x88, which broke demux routing and muxed it silent.
#[test]
fn dts_after_ac3_uses_physical_stream_number() {
    assert_eq!(audio_pid(Codec::Dts, 1), Some(0xBD89));
}

// MPEG audio is its own PES id 0xC0|n, not a private_stream_1 sub-id; unknown codecs
// have no route.
#[test]
fn mp2_audio_routes_to_its_pes_stream_id() {
    assert_eq!(audio_pid(Codec::Mp2, 0), Some(0x00C0));
    assert_eq!(audio_pid(Codec::Mp2, 7), Some(0x00C7));
    assert_eq!(audio_pid(Codec::Unknown(1), 0), None);
}

/// mpucoder IFO coding mode "2 Mpeg-1, 3 Mpeg-2ext"; EP0867877A2 "010b MPEG-1 or MPEG-2
/// without extension bit stream", "011b MPEG-2 with extension bitstream".
#[test]
fn coding_mode_3_is_mpeg2_with_extension_and_mode_2_is_not() {
    let attr = |b0: u8| parse_audio_attr(&[b0, 0x05, b'e', b'n', 0, 0, 0, 0], 0).unwrap();
    assert_eq!((attr(0x60).codec, attr(0x60).mpeg_ext), (Codec::Mp2, true));
    assert_eq!((attr(0x40).codec, attr(0x40).mpeg_ext), (Codec::Mp2, false));
    assert!(
        !attr(0x00).mpeg_ext,
        "AC-3 (mode 0) carries no MPEG extension"
    );
}

#[test]
fn every_audio_coding_mode_maps_to_an_audio_codec() {
    // An audio attribute block must never yield a codec whose kind() is Video.
    // Mode 2 (MPEG-1 audio Layer II) mapped to Codec::Mpeg1 — the MPEG-1 VIDEO
    // variant — so a DVD MPEG-audio stream was classified as video downstream.
    for (mode, want) in [
        (0u8, Codec::Ac3),
        (2, Codec::Mp2),
        (3, Codec::Mp2),
        (4, Codec::Lpcm),
        (6, Codec::Dts),
    ] {
        let mut data = vec![0u8; 16];
        data[0] = mode << 5;
        data[2] = b'e';
        data[3] = b'n';
        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.codec, want, "coding_mode {mode} must map to {want:?}");
        assert_eq!(
            attr.codec.kind(),
            crate::disc::CodecKind::Audio,
            "coding_mode {mode} produced {:?}, whose kind is not Audio",
            attr.codec
        );
    }
}

#[test]
fn audio_attr_dts() {
    let mut data = vec![0u8; 16];
    // DTS (coding=6), 96kHz (rate=1, byte1 bits 5-4), 2 channels (stored as 1)
    // b0: bits 7-5=110(DTS) => 0b110_00000 = 0xC0
    data[0] = 0xC0;
    // b1: bits 5-4=01(96k), bits 2-0=001(channels-1=1) => 0b00_01_0_001 = 0x11
    data[1] = 0x11;
    data[2] = b'f';
    data[3] = b'r';

    let attr = parse_audio_attr(&data, 0).unwrap();
    assert_eq!(attr.codec, Codec::Dts);
    assert_eq!(attr.sample_rate, 96000);
    assert_eq!(attr.channels, 2);
    assert_eq!(attr.language, "fra");
}

// Added hardening tests, grounded in the DVD-Video IFO spec
// (http://dvd.sourceforge.net).

/// BCD frame-rate flag: bits 7-6 of byte[3]. 0b01 = 25fps (PAL),
/// 0b11 = 29.97fps (NTSC). 0b00/0b10 are "unknown" → frames ignored.
/// Verify the 25fps branch contributes frames correctly.
#[test]
fn bcd_25fps_frame_contribution() {
    // 0h 0m 0s, 12 frames at 25fps → 12/25 = 0.48s.
    let bcd = [0x00, 0x00, 0x00, 0b01_010010]; // frame BCD 0x12 = 12
    let secs = bcd_to_secs(&bcd);
    assert!((secs - 12.0 / 25.0).abs() < 1e-9, "got {secs}");
}

/// BCD rate_flag 0b00 (and 0b10) → fps 0.0 → frame count ignored
/// entirely (only H/M/S counted). Source: `_ => 0.0` arm.
#[test]
fn bcd_unknown_rate_ignores_frames() {
    // 0h 1m 0s with frame bits set but rate_flag 0b00.
    let bcd = [0x00, 0x01, 0x00, 0b00_011001]; // frames present, rate unknown
    let secs = bcd_to_secs(&bcd);
    assert!((secs - 60.0).abs() < 0.001, "got {secs}");
    // rate_flag 0b10 also unknown.
    let bcd2 = [0x00, 0x01, 0x00, 0b10_011001];
    assert!((bcd_to_secs(&bcd2) - 60.0).abs() < 0.001);
}

/// BCD frame count is the LOW 6 bits of byte[3] (bits 5-0), decoded as
/// BCD. The 2 high bits (rate flag) must not leak into the frame value.
/// 0b11_100101: rate=29.97, frame BCD = 0x25 = 25 frames.
#[test]
fn bcd_frame_count_masks_rate_bits() {
    let bcd = [0x00, 0x00, 0x00, 0b11_100101]; // 0x25 BCD = 25 frames
    assert_eq!(bcd_to_frames(&bcd).unwrap().0, 25);
    let secs = bcd_to_secs(&bcd);
    assert!((secs - 25.0 * 1001.0 / 30000.0).abs() < 1e-9, "got {secs}");
}

/// BCD hours can exceed 12 (long titles): 0x12 BCD = 12 → but test a
/// value where hi/lo are both valid digits, e.g. 0x10 = 10 hours.
/// Ensures hours aren't capped or treated as hex.
#[test]
fn bcd_double_digit_hours() {
    let bcd = [0x10, 0x00, 0x00, 0x00]; // 10 hours BCD
    let secs = bcd_to_secs(&bcd);
    assert!((secs - 10.0 * 3600.0).abs() < 0.01, "got {secs}");
}

/// sub_slice uses saturating_add so an offset near usize::MAX cannot
/// wrap and bypass the bounds check. Must return Err, not panic/OOB.
#[test]
fn sub_slice_no_overflow_wrap() {
    let data = [0u8; 8];
    assert!(sub_slice(&data, usize::MAX, 4).is_err());
    assert!(sub_slice(&data, 4, 4).is_ok());
    assert!(sub_slice(&data, 5, 4).is_err()); // 5+4 > 8
}

/// byte_at returns Err for an out-of-range index (uses .get()).
#[test]
fn byte_at_out_of_range() {
    let data = [0xAA, 0xBB];
    assert_eq!(byte_at(&data, 0).unwrap(), 0xAA);
    assert_eq!(byte_at(&data, 1).unwrap(), 0xBB);
    assert!(byte_at(&data, 2).is_err());
}

/// A reserved video_format value (2/3) falls into the NTSC default.
#[test]
fn video_attr_reserved_standard_defaults_ntsc() {
    let mut data = vec![0u8; 0x204];
    // A reserved value is anything past PAL (2 or 3).
    data[0x200] = v_atr_byte(VIDEO_FORMAT_PAL + 1, ASPECT_4X3);
    let attr = parse_video_attr(&data).unwrap();
    assert_eq!(attr.standard, TvSystem::Ntsc);
    assert_eq!(attr.resolution, Resolution::R480i);
}

/// A reserved display_aspect value (1/2) falls into the 4:3 default.
#[test]
fn video_attr_reserved_aspect_defaults_4_3() {
    let mut data = vec![0u8; 0x204];
    // A reserved aspect value is between 4:3 (0) and 16:9 (3).
    data[0x200] = v_atr_byte(VIDEO_FORMAT_NTSC, ASPECT_4X3 + 1);
    let attr = parse_video_attr(&data).unwrap();
    assert_eq!(attr.aspect, DvdAspect::R4x3);
}

/// Audio coding_mode (b0>>5 & 0x07): 0=AC3, 2=MPEG1, 3=MP2, 4=LPCM,
/// 6=DTS; everything else → Unknown(mode). Verify LPCM (4) and an
/// unknown mode (1) map per the spec table.
#[test]
fn audio_attr_lpcm_and_unknown_coding() {
    let mut data = vec![0u8; 8];
    // LPCM: coding=4 → b0 bits 7-5 = 0b100 → 0x80
    data[0] = 0x80;
    data[2] = b'e';
    data[3] = b'n';
    let attr = parse_audio_attr(&data, 0).unwrap();
    assert_eq!(attr.codec, Codec::Lpcm);

    // coding=1 (reserved/unknown) → Unknown(1)
    let mut data2 = vec![0u8; 8];
    data2[0] = 0b001_00000; // coding=1
    let attr2 = parse_audio_attr(&data2, 0).unwrap();
    assert_eq!(attr2.codec, Codec::Unknown(1));
}

// Audio language bytes [offset+2..+4] both 0x00: unspecified, maps to
// valid ISO 639-2 "und", not an empty string (illegal Matroska element).
#[test]
fn audio_attr_zero_language_becomes_und() {
    let mut data = vec![0u8; 8];
    data[0] = 0x00;
    data[2] = 0x00;
    data[3] = 0x00;
    let attr = parse_audio_attr(&data, 0).unwrap();
    assert_eq!(attr.language, "und");
}

/// Audio sample_rate flag (byte 1 bits 5-4): 0=48kHz, 1=96kHz, 2/3 reserved -> 48kHz.
#[test]
fn audio_attr_reserved_rate_defaults_48k() {
    for (b1, want) in [(0b0000_0000u8, 48000), (0b0001_0000, 96000)] {
        let mut data = vec![0u8; 8];
        data[1] = b1;
        assert_eq!(parse_audio_attr(&data, 0).unwrap().sample_rate, want);
    }
    for b1 in [0b0010_0000u8, 0b0011_0000] {
        let mut data = vec![0u8; 8];
        data[1] = b1;
        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.sample_rate, 48000, "b1={b1:#010b}");
    }
}

/// The code extension is byte 5 of both attribute blocks (libdvdread audio_attr_t /
/// subp_attr_t: lang_code, lang_extension, code_extension); byte 4 is not it.
#[test]
fn attr_code_extension_is_byte_5() {
    let mut audio = vec![0u8; 8];
    audio[4] = 0x7F;
    audio[5] = 3;
    assert_eq!(parse_audio_attr(&audio, 0).unwrap().code_extension, 3);
    let mut sub = vec![0u8; 6];
    sub[4] = 0x7F;
    sub[5] = 9;
    assert_eq!(parse_subtitle_attr(&sub, 0).unwrap().code_extension, 9);
}

/// Subtitle language is at [offset+2..+4]. Verify a valid 2-letter code
/// and the all-zero → empty case.
#[test]
fn subtitle_attr_language() {
    let mut data = vec![0u8; 6];
    data[2] = b'd';
    data[3] = b'e';
    let attr = parse_subtitle_attr(&data, 0).unwrap();
    assert_eq!(attr.language, "deu");

    let zero = vec![0u8; 6];
    let attr2 = parse_subtitle_attr(&zero, 0).unwrap();
    assert_eq!(attr2.language, "und");
}

/// parse_pgc requires `pgc_offset + 0xEA <= data.len()` (needs the cell
/// playback offset at 0xE8). A PGC shorter than 0xEA → IfoParse error,
/// not panic.
#[test]
fn pgc_too_short_errs() {
    let pgc = vec![0u8; 0xE9]; // one byte short of 0xEA
    assert!(parse_pgc(&pgc, 0, 1).is_err());
}

/// parse_pgc cell loop stops when a cell record runs past the buffer
/// (`co + 24 > data.len()` → break), parsing only complete cells.
/// Declare 3 cells but supply bytes for 2.
#[test]
fn pgc_truncated_cell_table_stops() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 1;
    pgc[0x03] = 3; // claims 3 cells
    pgc[0xE8] = 0x00;
    pgc[0xE9] = 0xEA;
    // Only room for 2 full cells (48 bytes).
    pgc.resize(0xEA + 48, 0);
    pgc[0xEA + 8..0xEA + 12].copy_from_slice(&10u32.to_be_bytes());
    pgc[0xEA + 24 + 8..0xEA + 24 + 12].copy_from_slice(&20u32.to_be_bytes());
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    // Only 2 cells parsed; the 3rd had no bytes.
    assert_eq!(title.cells.len(), 2);
    assert_eq!(title.cells[0].first_sector, 10);
    assert_eq!(title.cells[1].first_sector, 20);
}

/// parse_pgc palette: at PGC+0xA4, 16 colors × 4 bytes [pad, Y, Cr, Cb].
/// A palette with at least one non-zero Y/Cr/Cb is returned as Some;
/// an all-zero palette returns None (source filters empty palettes).
#[test]
fn pgc_palette_present_and_empty() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x03] = 0; // no cells
    // Set color 0's Y byte (offset 0xA4 + 1) non-zero.
    pgc[0xA4 + 1] = 0x80;
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    let pal = title.palette.expect("non-empty palette should be Some");
    assert_eq!(pal.len(), 16);
    assert_eq!(pal[0], [0x00, 0x80, 0x00, 0x00]);

    // All-zero palette → None.
    let mut pgc2 = vec![0u8; 0xEA];
    pgc2[0x03] = 0;
    let title2 = parse_pgc(&pgc2, 0, 1).unwrap();
    assert!(title2.palette.is_none());
}

/// The palette byte order is [padding, Y, Cr, Cb] — chroma is Cr BEFORE Cb
/// — and the DvdSub consumer reads it the same way. Pin both together with
/// distinct Cr/Cb so a future byte-swap in either place fails here.
#[test]
fn pgc_palette_chroma_order_is_cr_then_cb_matching_the_consumer() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x03] = 0; // no cells
    // Color 0: Y=0x40, Cr=0x11, Cb=0x22 — distinct so an order swap shows.
    pgc[0xA4] = 0x00; // padding
    pgc[0xA4 + 1] = 0x40; // Y
    pgc[0xA4 + 2] = 0x11; // Cr
    pgc[0xA4 + 3] = 0x22; // Cb
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    let pal = title.palette.expect("non-empty palette");
    assert_eq!(
        pal[0],
        [0x00, 0x40, 0x11, 0x22],
        "stored as [pad, Y, Cr, Cb]"
    );
    // Consumer lockstep, checked against colour meaning rather than itself:
    // a CLUT entry with high Cr (byte 2) and neutral Cb must render red.
    let red = crate::mux::codec::dvdsub::ycbcr_to_rgb(&[0x00, 0x80, 0xF0, 0x80]);
    assert!(
        red[0] > 0xE0 && red[2] < 0x90,
        "byte 2 must be read as Cr: {red:?}"
    );
}

/// parse_pgc palette layout: each color is [padding, Y, Cr, Cb] and the
/// "non-empty" test ignores the padding byte (index 0). A palette whose
/// ONLY non-zero bytes are padding must still be treated as empty (None).
#[test]
fn pgc_palette_padding_only_is_empty() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x03] = 0;
    // Set padding byte (index 0) of color 0 non-zero, but Y/Cr/Cb zero.
    pgc[0xA4] = 0xFF;
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    assert!(
        title.palette.is_none(),
        "padding-only palette must be treated as empty"
    );
}

// chapter_time[p] = sum of cell durations before program p's first cell
// (program map at PGC+0xE6, 1-based cell numbers). 2-program, 3-cell case:
// program 0 at cell 1 (t=0), program 1 at cell 3 (t=dur(cell0)+dur(cell1)).
#[test]
fn pgc_chapter_times_from_program_map() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 2; // nr_programs = 2
    pgc[0x03] = 3; // nr_cells = 3
    // program map offset at 0xE6 (u16 BE)
    let pgm_off: u16 = 0xEA;
    pgc[0xE6] = (pgm_off >> 8) as u8;
    pgc[0xE7] = pgm_off as u8;
    // cell playback offset at 0xE8
    let cell_off: u16 = 0xEA + 2; // after the 2-byte program map
    pgc[0xE8] = (cell_off >> 8) as u8;
    pgc[0xE9] = cell_off as u8;

    // Layout: [0xEA..0xEC] = program map (2 bytes), then 3 cells × 24.
    pgc.resize(cell_off as usize + 3 * 24, 0);
    // Program map: program0 first cell = 1, program1 first cell = 3.
    pgc[0xEA] = 1;
    pgc[0xEB] = 3;
    // Cell durations: cell0 = 5s, cell1 = 7s, cell2 = 9s (BCD seconds).
    let cb = cell_off as usize;
    pgc[cb + 6] = 0x05; // cell0 sec
    pgc[cb + 24 + 6] = 0x07; // cell1 sec
    pgc[cb + 48 + 6] = 0x09; // cell2 sec

    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert_eq!(title.chapter_times.len(), 2);
    // Program 0 → before cell 1 → 0s.
    assert!((title.chapter_times[0] - 0.0).abs() < 0.01);
    // Program 1 → before cell 3 → dur(cell0)+dur(cell1) = 5+7 = 12s.
    assert!(
        (title.chapter_times[1] - 12.0).abs() < 0.01,
        "got {}",
        title.chapter_times[1]
    );
}

// An angle block plays one angle, so its other angles' cells add no time before a chapter:
// cell 1 is angle 1 (first of block), cell 2 angle 2 (last of block), both 7 s.
#[test]
fn pgc_chapter_times_count_an_angle_block_once() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 2;
    pgc[0x03] = 4;
    let pgm_off: u16 = 0xEA;
    pgc[0xE6..0xE8].copy_from_slice(&pgm_off.to_be_bytes());
    let cell_off: u16 = 0xEA + 2;
    pgc[0xE8..0xEA].copy_from_slice(&cell_off.to_be_bytes());
    pgc.resize(cell_off as usize + 4 * 24, 0);
    pgc[0xEA] = 1;
    pgc[0xEB] = 4;
    let cb = cell_off as usize;
    pgc[cb + 6] = 0x05;
    pgc[cb + 24] = 0x50; // first cell of an angle block
    pgc[cb + 24 + 6] = 0x07;
    pgc[cb + 48] = 0xD0; // last cell of the same angle block
    pgc[cb + 48 + 6] = 0x07;
    pgc[cb + 72 + 6] = 0x09;
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert!(
        (title.chapter_times[1] - 12.0).abs() < 0.01,
        "got {}",
        title.chapter_times[1]
    );
}

// Regression for freemkv#25 (NTSC chapter drift): real cell table vs reference chapter
// marks. Drift (0.1%, proportional to elapsed time) is why a short synthetic fixture
// wouldn't catch it.
#[test]
fn pgc_chapter_times_ntsc_no_pulldown_drift() {
    // (minutes, seconds, frames) of NTSC non-drop-frame timecode per cell.
    let cells: [(u8, u8, u8); 13] = [
        (1, 2, 0),
        (4, 44, 12),
        (1, 32, 11),
        (13, 42, 1),
        (0, 59, 28),
        (0, 31, 8),
        (1, 2, 0),
        (19, 58, 24),
        (0, 59, 28),
        (0, 31, 8),
        (1, 2, 0),
        (19, 58, 26),
        (0, 59, 28),
    ];
    // Program map: 14 chapters, one per cell boundary (1-based first cell).
    let pgm: [u8; 14] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];

    fn bcd(v: u8) -> u8 {
        ((v / 10) << 4) | (v % 10)
    }

    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = pgm.len() as u8;
    pgc[0x03] = cells.len() as u8;
    let pgm_off: u16 = 0xEA;
    pgc[0xE6] = (pgm_off >> 8) as u8;
    pgc[0xE7] = pgm_off as u8;
    let cell_off: u16 = pgm_off + pgm.len() as u16;
    pgc[0xE8] = (cell_off >> 8) as u8;
    pgc[0xE9] = cell_off as u8;
    pgc.resize(cell_off as usize + cells.len() * 24, 0);
    pgc[pgm_off as usize..pgm_off as usize + pgm.len()].copy_from_slice(&pgm);
    for (i, &(m, s, f)) in cells.iter().enumerate() {
        let co = cell_off as usize + i * 24;
        pgc[co + 4] = 0x00; // hours
        pgc[co + 5] = bcd(m);
        pgc[co + 6] = bcd(s);
        pgc[co + 7] = 0xC0 | bcd(f); // rate flag 0b11 = NTSC
    }

    let title = parse_pgc(&pgc, 0, pgm.len() as u16).unwrap();
    // Reference / source truth, in seconds.
    let expected = [
        0.0,
        62.062,
        346.7464,
        439.205_433_333,
        1262.0608,
        1_322.054_066_666,
        1353.352,
        1415.414,
        2615.4128,
        2_675.406_066_666,
        2706.704,
        2768.766,
        3_968.831_533_333,
        4028.8248,
    ];
    assert_eq!(title.chapter_times.len(), expected.len());
    for (i, (&got, &want)) in title.chapter_times.iter().zip(&expected).enumerate() {
        assert!(
            (got - want).abs() < 1e-6,
            "chapter {} drifted: got {got}, want {want}",
            i + 1
        );
    }
    // Every mark must sit on an exact 30000/1001 frame boundary.
    for (i, &t) in title.chapter_times.iter().enumerate() {
        let frames = t * 30000.0 / 1001.0;
        assert!(
            (frames - frames.round()).abs() < 1e-6,
            "chapter {} not frame-aligned: {t}",
            i + 1
        );
    }
}

/// parse_pgc duration: when the PGC-level BCD time is NON-zero it is
/// used directly and NOT overwritten by cell-sum recomputation
/// (the recompute only fires when duration_secs == 0.0).
#[test]
fn pgc_nonzero_duration_not_recomputed() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 1;
    pgc[0x03] = 1;
    // PGC-level time = 1m 0s at 25fps.
    pgc[0x05] = 0x01; // minutes BCD 1
    pgc[0x07] = 0b01_000000; // 25fps, 0 frames
    pgc[0xE8] = 0x00;
    pgc[0xE9] = 0xEA;
    pgc.resize(0xEA + 24, 0);
    // Give the cell a bogus huge duration that must be IGNORED.
    pgc[0xEA + 6] = 0x59; // 59s — would change result if recomputed
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    assert!(
        (title.duration_secs - 60.0).abs() < 0.01,
        "PGC-level 60s must win, got {}",
        title.duration_secs
    );
}

/// parse_pgc with cell_playback_offset == 0 must produce NO cells (the
/// `cell_playback_offset > 0 && num_cells > 0` guard). Even with
/// nr_cells set, a zero offset means the table is absent.
#[test]
fn pgc_zero_cell_offset_no_cells() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x03] = 5; // claims 5 cells
    // cell_playback_offset (0xE8) left 0.
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    assert!(title.cells.is_empty());
}

// ─────────────────────────────────────────────────────────────────────
// Cell-category decode + bug-4 leading-cell filter.
// ─────────────────────────────────────────────────────────────────────

fn cell(first: u32, last: u32, category: u8) -> DvdCell {
    DvdCell {
        first_sector: first,
        last_sector: last,
        category,
        duration_secs: 0.0,
    }
}

/// CellCategory decodes the DVD-Video cell-category byte-0 bitfields: block_mode (7-6),
/// block_type (5-4), seamless_play (3), interleaved (2),
/// stc_discontinuity (1), seamless_angle (0).
#[test]
fn cell_category_decode_bits() {
    // 0x00 → plain feature, nothing set.
    let c = CellCategory::decode(0x00);
    assert_eq!(c.block_mode, 0);
    assert_eq!(c.block_type, 0);
    assert!(!c.seamless_play);
    assert!(!c.interleaved);
    assert!(c.is_plain_feature());
    assert!(!c.is_secondary_block_piece());

    // block_mode=1 (first cell of block), block_type=1 (angle block):
    // 0b0101_0000 = 0x50. This is the angle we KEEP — not secondary.
    let c = CellCategory::decode(0b0101_0000);
    assert_eq!(c.block_mode, 1);
    assert_eq!(c.block_type, 1);
    assert!(!c.is_plain_feature());
    assert!(!c.is_secondary_block_piece());

    // block_mode=2 (in block) / 3 (last of block) of an angle block
    // (block_type=1) → secondary.
    assert!(CellCategory::decode(0b1001_0000).is_secondary_block_piece());
    assert!(CellCategory::decode(0b1101_0000).is_secondary_block_piece());
    // First cell of the block (block_mode=1) is NEVER secondary.
    assert!(!CellCategory::decode(0b0101_0000).is_secondary_block_piece());

    // The low flags (seamless_play bit3, interleaved bit2, stc bit1,
    // seamless_angle bit0) on an otherwise-plain cell must NOT make it
    // secondary — they don't mark non-feature content.
    let c = CellCategory::decode(0b0000_1111);
    assert!(c.seamless_play);
    assert!(c.interleaved);
    assert!(c.stc_discontinuity);
    assert!(c.seamless_angle);
    assert!(c.is_plain_feature());
    assert!(!c.is_secondary_block_piece());
}

/// A normal single-angle feature (every cell category 0x00) is never
/// filtered: feature_start_cell == 0, feature_cells == all cells. This is
/// the plain-single-angle-feature case — the filter must be a no-op.
#[test]
fn feature_filter_noop_on_plain_feature() {
    let t = DvdTitle {
        chapters: 3,
        duration_secs: 6780.0,
        cells: vec![
            cell(0, 99, 0x00),
            cell(100, 199, 0x00),
            cell(200, 299, 0x00),
        ],
        chapter_times: vec![0.0, 100.0, 200.0],
        palette: None,
        ast_ctl: [0; 8],
        spst_ctl: [0; 32],
        vts_title_num: 0,
        pgcs: 1,
        ptt_programs: None,
    };
    assert_eq!(t.feature_start_cell(), 0);
    assert_eq!(t.feature_cells().len(), 3);
}

/// A leading interleaved/angle-block sub-cell (category marks a secondary
/// block piece) is dropped; the scan stops at the first plain cell and
/// keeps the rest.
#[test]
fn feature_filter_drops_leading_secondary_block_cells() {
    let t = DvdTitle {
        chapters: 2,
        duration_secs: 100.0,
        cells: vec![
            cell(0, 9, 0b1001_0000),   // in-block cell of angle block → drop
            cell(10, 19, 0b1101_0000), // last cell of angle block → drop
            cell(20, 119, 0x00),       // feature starts here
            cell(120, 219, 0x00),
        ],
        chapter_times: vec![0.0, 50.0],
        palette: None,
        ast_ctl: [0; 8],
        spst_ctl: [0; 32],
        vts_title_num: 0,
        pgcs: 1,
        ptt_programs: None,
    };
    assert_eq!(t.feature_start_cell(), 2);
    let fc = t.feature_cells();
    assert_eq!(fc.len(), 2);
    assert_eq!(fc[0].first_sector, 20);
}

// Conservative guard: if EVERY cell looks like a secondary block piece,
// the filter refuses to drop them all — returns 0, keeps every cell.
#[test]
fn feature_filter_never_empties_title() {
    let t = DvdTitle {
        chapters: 1,
        duration_secs: 100.0,
        cells: vec![cell(0, 9, 0b1001_0000), cell(10, 19, 0b1101_0000)],
        chapter_times: vec![0.0],
        palette: None,
        ast_ctl: [0; 8],
        spst_ctl: [0; 32],
        vts_title_num: 0,
        pgcs: 1,
        ptt_programs: None,
    };
    assert_eq!(t.feature_start_cell(), 0);
    assert_eq!(t.feature_cells().len(), 2);
}

/// An empty title (no cells) returns 0 and an empty slice — no panic.
#[test]
fn feature_filter_empty_cells() {
    let t = DvdTitle {
        chapters: 0,
        duration_secs: 0.0,
        cells: vec![],
        chapter_times: vec![],
        palette: None,
        ast_ctl: [0; 8],
        spst_ctl: [0; 32],
        vts_title_num: 0,
        pgcs: 1,
        ptt_programs: None,
    };
    assert_eq!(t.feature_start_cell(), 0);
    assert!(t.feature_cells().is_empty());
}

/// parse_pgc populates the new `category` + `duration_secs` cell fields
/// from `cell_playback + 0` and the BCD time at `cell_playback + 4`.
#[test]
fn pgc_reads_cell_category_and_duration() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 1;
    pgc[0x03] = 2; // 2 cells
    pgc[0xE8] = 0x00;
    pgc[0xE9] = 0xEA;
    pgc.resize(0xEA + 48, 0);
    // Cell 0: category byte = 0x90 (in-block cell of angle block), 5s BCD.
    pgc[0xEA] = 0x90;
    pgc[0xEA + 6] = 0x05;
    pgc[0xEA + 8..0xEA + 12].copy_from_slice(&10u32.to_be_bytes());
    // Cell 1: category 0x00 (plain feature), 7s BCD.
    pgc[0xEA + 24] = 0x00;
    pgc[0xEA + 24 + 6] = 0x07;
    pgc[0xEA + 24 + 8..0xEA + 24 + 12].copy_from_slice(&20u32.to_be_bytes());
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert_eq!(title.cells[0].category, 0x90);
    assert!((title.cells[0].duration_secs - 5.0).abs() < 0.01);
    assert_eq!(title.cells[1].category, 0x00);
    assert!((title.cells[1].duration_secs - 7.0).abs() < 0.01);
    // The leading secondary-block cell is filtered out of the feature.
    assert_eq!(title.feature_start_cell(), 1);
}

// Regression: a crafted IFO whose program-map byte names a first_cell index larger than the
// actual cell count must NOT panic (it used to, slicing cell_durations out of bounds).
#[test]
fn pgc_program_map_oob_cell_index_no_panic() {
    let mut pgc = vec![0u8; 0xEA];
    pgc[0x02] = 1; // nr_programs = 1
    pgc[0x03] = 1; // nr_cells = 1

    // program map offset at PGC+0xE6 (u16 BE) → right after the header
    let pgm_off: u16 = 0xEA;
    pgc[0xE6] = (pgm_off >> 8) as u8;
    pgc[0xE7] = pgm_off as u8;

    // cell playback offset at PGC+0xE8 → after the 1-byte program map
    let cell_off: u16 = 0xEA + 1;
    pgc[0xE8] = (cell_off >> 8) as u8;
    pgc[0xE9] = cell_off as u8;

    // Allocate space: 1 program-map byte + 1 cell × 24 bytes
    pgc.resize(cell_off as usize + 24, 0);

    // Craft: program 0's first_cell = 0xFF (255) — far past the 1 real cell
    pgc[0xEA] = 0xFF;

    // Cell 0 duration = 10s (BCD seconds byte at cell_base + 6)
    pgc[cell_off as usize + 6] = 0x10; // BCD 0x10 = 10 seconds

    // Must return Ok; must not panic.
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    // With first_cell=255, end = min(254, 1) = 1, so chapter_times[0] = dur(cell0) = 10s.
    assert_eq!(title.chapter_times.len(), 1);
    assert!(
        (title.chapter_times[0] - 10.0).abs() < 0.01,
        "got {}",
        title.chapter_times[0]
    );
}

// Cell-category bit isolation: each low flag (bit3 seamless_play, bit2
// interleaved, bit1 stc_discontinuity, bit0 seamless_angle) comes from its
// OWN bit — setting one must leave the other three clear.
#[test]
fn cell_category_low_flags_are_bit_isolated() {
    let c = CellCategory::decode(0x00);
    assert_eq!(
        [
            c.seamless_play,
            c.interleaved,
            c.stc_discontinuity,
            c.seamless_angle
        ],
        [false, false, false, false],
        "category 0x00 sets no flag"
    );
    for bit in 0..4u8 {
        let c = CellCategory::decode(1u8 << bit);
        assert_eq!(
            [
                c.seamless_play,
                c.interleaved,
                c.stc_discontinuity,
                c.seamless_angle
            ],
            [bit == 3, bit == 2, bit == 1, bit == 0],
            "only bit {bit} may be set"
        );
    }
}

/// `is_plain_feature` requires BOTH block_mode and block_type to be zero.
/// A cell inside a block is not plain feature content even when the other
/// field happens to be zero.
#[test]
fn is_plain_feature_requires_both_block_fields_zero() {
    assert!(CellCategory::decode(0x00).is_plain_feature());
    // block_type = 1, block_mode = 0
    assert!(!CellCategory::decode(0b0001_0000).is_plain_feature());
    // block_mode = 1, block_type = 0
    assert!(!CellCategory::decode(0b0100_0000).is_plain_feature());
    assert!(!CellCategory::decode(0b0101_0000).is_plain_feature());
}

/// be_u32 reads four CONSECUTIVE big-endian bytes from `offset`.
/// All four values are distinct so a repeated or skipped index shows up.
#[test]
fn be_u32_reads_four_consecutive_be_bytes() {
    let data = [0xFFu8, 0x01, 0x02, 0x03, 0x04, 0xFF];
    assert_eq!(be_u32(&data, 1).unwrap(), 0x0102_0304);
}

// ── malformed language codes ─────────────────────────────────────────

// Only lowercase a-z is taken verbatim, else ASCII-alphanumeric salvage.
// Exercises parse_raw_dvd_lang_bytes, separately from ISO 639-1->2 mapping.
#[test]
fn language_code_rejects_non_lowercase_bytes() {
    // (byte0, byte1, expected raw salvage)
    let cases: [(u8, u8, &str); 8] = [
        (b'e', b'n', "en"), // both in range → verbatim
        (0x21, b'n', "n"),  // '!' is below 'a'
        (0x7B, b'n', "n"),  // '{' is above 'z'
        (b'e', 0x21, "e"),  // second byte below 'a'
        (b'e', 0x7B, "e"),  // second byte above 'z'
        (b'E', 0x00, "E"),  // only the second byte is zero
        (0x00, b'E', "E"),  // only the first byte is zero
        (0x00, 0x00, ""),   // both zero → unset
    ];
    for (b0, b1, want) in cases {
        assert_eq!(
            parse_raw_dvd_lang_bytes(&[b0, b1]),
            want,
            "raw language salvage for ({b0:#04x}, {b1:#04x})"
        );
    }
}

// Full pipeline: parse_audio_attr/parse_subtitle_attr on top of the raw
// salvage. Valid ISO 639-1 maps to 639-2; anything unmapped (empty or a
// malformed leftover letter) degrades to "und", never an illegal value.
#[test]
fn dvd_two_letter_and_malformed_language_becomes_iso639_2() {
    // (byte0, byte1, expected final language)
    let cases: [(u8, u8, &str); 4] = [
        (b'e', b'n', "eng"), // valid ISO 639-1 → mapped
        (0x21, b'n', "und"), // malformed → salvage "n", unmapped → und
        (b'E', 0x00, "und"), // malformed → salvage "E", unmapped → und
        (0x00, 0x00, "und"), // unspecified → und
    ];
    for (b0, b1, want) in cases {
        let mut audio = vec![0u8; 8];
        audio[2] = b0;
        audio[3] = b1;
        assert_eq!(
            parse_audio_attr(&audio, 0).unwrap().language,
            want,
            "audio language for ({b0:#04x}, {b1:#04x})"
        );

        let mut sub = vec![0u8; 6];
        sub[2] = b0;
        sub[3] = b1;
        assert_eq!(
            parse_subtitle_attr(&sub, 0).unwrap().language,
            want,
            "subtitle language for ({b0:#04x}, {b1:#04x})"
        );
    }
}

// ── PGC fixtures ─────────────────────────────────────────────────────

// Builds a standalone PGC at offset 0: header, program map (1-based first cell number per
// program), 24-byte cell table.
fn build_pgc(
    pgc_time: [u8; 4],
    cells: &[(u8, [u8; 4], u32, u32)],
    programs: &[u8],
    palette: Option<[[u8; 4]; 16]>,
) -> Vec<u8> {
    let mut d = vec![0u8; 0xEA];
    d[0x02] = programs.len() as u8;
    d[0x03] = cells.len() as u8;
    d[0x04..0x08].copy_from_slice(&pgc_time);
    if let Some(p) = palette {
        for (i, c) in p.iter().enumerate() {
            d[0xA4 + i * 4..0xA4 + i * 4 + 4].copy_from_slice(c);
        }
    }
    let pgm_off = 0xEAusize;
    let cell_off = pgm_off + programs.len();
    if !programs.is_empty() {
        d[0xE6..0xE8].copy_from_slice(&(pgm_off as u16).to_be_bytes());
    }
    if !cells.is_empty() {
        d[0xE8..0xEA].copy_from_slice(&(cell_off as u16).to_be_bytes());
    }
    d.extend_from_slice(programs);
    for &(cat, time, first, last) in cells {
        let mut c = vec![0u8; 24];
        c[0] = cat;
        c[4..8].copy_from_slice(&time);
        c[8..12].copy_from_slice(&first.to_be_bytes());
        c[20..24].copy_from_slice(&last.to_be_bytes());
        d.extend_from_slice(&c);
    }
    d
}

// BCD playback time of `secs` TIMECODE seconds (<60) at the NTSC rate.
// Timecode, not real time: `secs` seconds = `secs*30` frames, lasting
// `secs*1.001` real seconds, so e.g. bcd_secs(10) is 10.01s not 10s.
fn bcd_secs(secs: u8) -> [u8; 4] {
    assert!(secs < 60);
    [0, 0, ((secs / 10) << 4) | (secs % 10), 0b11_000000]
}

/// When the PGC-level playback time is zero, the duration is recomputed
/// as the SUM of every cell's own BCD time, each read from its own
/// 24-byte cell playback record at +4.
#[test]
fn pgc_zero_duration_recomputed_as_sum_of_cell_times() {
    let pgc = build_pgc(
        [0, 0, 0, 0],
        &[
            (0x00, bcd_secs(10), 100, 199),
            (0x00, bcd_secs(20), 200, 299),
            (0x00, bcd_secs(31), 300, 399),
        ],
        &[],
        None,
    );
    let title = parse_pgc(&pgc, 0, 3).unwrap();
    assert_eq!(title.cells.len(), 3);
    assert_eq!(title.cells[0].duration_secs, 10.01);
    assert_eq!(title.cells[1].duration_secs, 20.02);
    assert_eq!(title.cells[2].duration_secs, 31.031);
    assert!(
        (title.duration_secs - 61.061).abs() < 1e-9,
        "recomputed duration must be the sum of the distinct cell times, got {}",
        title.duration_secs
    );
}

// Chapter times: program's byte is the 1-based first-cell number; chapter
// time is the sum of durations of every cell BEFORE it. Distinct cell
// durations mean a misread program byte or mis-strided table shows up.
#[test]
fn pgc_chapter_times_sum_preceding_cell_durations() {
    let pgc = build_pgc(
        bcd_secs(59),
        &[
            (0x00, bcd_secs(10), 0, 9),
            (0x00, bcd_secs(20), 10, 19),
            (0x00, bcd_secs(31), 20, 29),
        ],
        &[1, 2, 3],
        None,
    );
    let title = parse_pgc(&pgc, 0, 3).unwrap();
    assert_eq!(title.chapter_times, vec![0.0, 10.01, 30.03]);
}

/// A program map offset of 0 means there is no program map, so no chapter
/// times may be produced — the PGC header must not be read as one. Same
/// for a cell playback offset of 0.
#[test]
fn pgc_absent_program_map_or_cell_table_yields_no_chapter_times() {
    // programs declared, but pgm_map_offset patched to 0
    let mut pgc = build_pgc(
        bcd_secs(30),
        &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
        &[1, 2],
        None,
    );
    pgc[0xE6..0xE8].copy_from_slice(&0u16.to_be_bytes());
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert!(
        title.chapter_times.is_empty(),
        "no program map → no chapter times, got {:?}",
        title.chapter_times
    );

    // program map present, but cell_playback_offset patched to 0
    let mut pgc = build_pgc(
        bcd_secs(30),
        &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
        &[1, 2],
        None,
    );
    pgc[0xE8..0xEA].copy_from_slice(&0u16.to_be_bytes());
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert!(title.cells.is_empty());
    assert!(
        title.chapter_times.is_empty(),
        "no cell table → no chapter times, got {:?}",
        title.chapter_times
    );
}

/// A declared cell count larger than the cell table actually holds must
/// not read past the end while collecting durations for the chapter-time
/// calculation; the missing cells contribute zero.
#[test]
fn pgc_cell_count_overshoot_does_not_read_past_end() {
    let mut pgc = build_pgc(
        bcd_secs(30),
        &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
        &[1, 2],
        None,
    );
    pgc[0x03] = 8; // declare 8 cells; only 2 records exist
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert_eq!(title.cells.len(), 2, "only the readable cells are kept");
    // Program 2 starts at cell 2, so its time is cell 0's duration.
    assert_eq!(title.chapter_times, vec![0.0, 10.01]);
}

// A cell cut off mid-record is not in `cells`, so it adds no time to a chapter mark.
#[test]
fn pgc_chapter_times_ignore_a_cell_cut_off_mid_record() {
    let mut pgc = build_pgc(
        bcd_secs(59),
        &[
            (0x00, bcd_secs(10), 0, 9),
            (0x00, bcd_secs(20), 10, 19),
            (0x00, bcd_secs(31), 20, 29),
        ],
        &[1, 3],
        None,
    );
    pgc.truncate(0xEA + 2 + 24 + 12);
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert_eq!(title.cells.len(), 1);
    assert_eq!(title.chapter_times, vec![0.0, 10.01]);
}

// A program map running past the end of the data must stop at the buffer
// end. Fixture map begins at PGC+0xEA, buffer holds 50 bytes from there,
// so a declared 255 programs must yield exactly 50 chapter times.
#[test]
fn pgc_program_map_past_end_stops() {
    let mut pgc = build_pgc(
        bcd_secs(30),
        &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
        &[1, 2],
        None,
    );
    pgc[0x02] = 255; // declare 255 programs
    let available = pgc.len() - 0xEA;
    assert_eq!(available, 50, "fixture: 2 map bytes + 2 cells of 24");
    let title = parse_pgc(&pgc, 0, 2).unwrap();
    assert_eq!(
        title.chapter_times.len(),
        available,
        "the program map walk must stop exactly at the buffer end"
    );
    assert_eq!(title.chapter_times[0], 0.0);
    assert_eq!(title.chapter_times[1], 10.01);
}

/// The subtitle palette is 16 entries of 4 bytes at PGC+0xA4, each
/// `[padding, Y, Cr, Cb]`. Every byte of every entry is distinct here, so
/// a wrong stride, a wrong base or a shifted component shows up.
#[test]
// The loop variable is the DOMAIN VALUE under test (a palette entry number),
// not a collection cursor — it's what the assertion message names, so
// `.iter().enumerate()` would rename the thing under test and read worse.
#[allow(clippy::needless_range_loop)]
fn pgc_palette_entries_read_at_correct_stride() {
    let mut pal = [[0u8; 4]; 16];
    for (i, c) in pal.iter_mut().enumerate() {
        let b = i as u8;
        *c = [0x10 + b, 0x30 + b, 0x50 + b, 0x70 + b];
    }
    let pgc = build_pgc(bcd_secs(30), &[], &[], Some(pal));
    let title = parse_pgc(&pgc, 0, 1).unwrap();
    let got = title.palette.expect("palette present");
    assert_eq!(got.len(), 16);
    for i in 0..16 {
        let b = i as u8;
        assert_eq!(
            got[i],
            [0x10 + b, 0x30 + b, 0x50 + b, 0x70 + b],
            "palette entry {i}"
        );
    }
}

/// A palette is "present" when ANY of Y, Cr or Cb is non-zero in ANY
/// entry — a single non-zero chroma component is enough. Only the
/// padding byte [0] is ignored.
#[test]
fn pgc_palette_present_on_any_single_nonzero_component() {
    for comp in 1..4usize {
        let mut pal = [[0u8; 4]; 16];
        pal[7][comp] = 0x40; // exactly one non-zero component, in one entry
        let pgc = build_pgc(bcd_secs(30), &[], &[], Some(pal));
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        assert!(
            title.palette.is_some(),
            "component {comp} alone must mark the palette present"
        );
    }
    // Only the padding byte set → still empty.
    let mut pal = [[0u8; 4]; 16];
    for c in pal.iter_mut() {
        c[0] = 0xFF;
    }
    let pgc = build_pgc(bcd_secs(30), &[], &[], Some(pal));
    assert!(parse_pgc(&pgc, 0, 1).unwrap().palette.is_none());
}

// ── PGCIT ────────────────────────────────────────────────────────────

// Builds a VTS_PGCIT at offset 0: header, one 8-byte VTS_PGCI_SRP per PGC, then `pgcs`
// appended after the SRP table.
fn build_pgcit(pgcs: &[Vec<u8>]) -> Vec<u8> {
    let mut d = vec![0u8; 8 + pgcs.len() * 8];
    d[0..2].copy_from_slice(&(pgcs.len() as u16).to_be_bytes());
    let mut off = d.len();
    for (i, p) in pgcs.iter().enumerate() {
        let e = 8 + i * 8;
        d[e + 4..e + 8].copy_from_slice(&(off as u32).to_be_bytes());
        off += p.len();
    }
    for p in pgcs {
        d.extend_from_slice(p);
    }
    d
}

// Each VTS_PGCI_SRP is 8 bytes; PGC start address is the entry's own
// second word, so title N must resolve to PGC N. Distinct durations
// catch an entry read at the wrong stride or from the wrong entry.
#[test]
fn pgcit_entry_stride_selects_the_right_pgc() {
    let pgc0 = build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None);
    let pgc1 = build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None);
    let data = build_pgcit(&[pgc0, pgc1]);

    // vts_title_num is 1-based: title 2 → PGC index 1.
    let titles = parse_pgcit(&data, 0, 0, &[(5, 2)]).unwrap();
    assert_eq!(titles.len(), 1);
    assert_eq!(titles[0].duration_secs, 22.022);
    assert_eq!(titles[0].cells[0].first_sector, 50);

    let titles = parse_pgcit(&data, 0, 0, &[(5, 1)]).unwrap();
    assert_eq!(titles.len(), 1);
    assert_eq!(titles[0].duration_secs, 11.011);
    assert_eq!(titles[0].cells[0].first_sector, 0);

    // Both titles, in order.
    let titles = parse_pgcit(&data, 0, 0, &[(5, 1), (7, 2)]).unwrap();
    assert_eq!(titles.len(), 2);
    assert_eq!(titles[0].duration_secs, 11.011);
    assert_eq!(titles[1].duration_secs, 22.022);
}

/// A VTS_PGCIT whose 8-byte header ends exactly at the end of the data is
/// a complete header with no SRP entries: an empty title list, not an
/// error. Below 8 bytes the header itself is truncated → IfoParse.
#[test]
fn pgcit_header_boundary() {
    let data = vec![0u8; 8];
    let titles = parse_pgcit(&data, 0, 0, &[(5, 1)]).expect("complete header parses");
    assert!(titles.is_empty());
    for len in 0..8usize {
        assert!(
            parse_pgcit(&vec![0u8; len], 0, 0, &[(5, 1)]).is_err(),
            "len={len}"
        );
    }
}

/// An SRP entry that runs past the end of the data is SKIPPED, leaving
/// an empty title list — a truncated entry table must not turn into a
/// parse error for the whole PGCIT.
#[test]
fn pgcit_entry_past_end_is_skipped_not_an_error() {
    // 8-byte header declaring 3 PGCs, then only 12 bytes of table: the
    // entry for PGC index 2 (offset 24..32) is entirely past the end.
    let mut data = vec![0u8; 20];
    data[0..2].copy_from_slice(&3u16.to_be_bytes());
    let titles = parse_pgcit(&data, 0, 0, &[(5, 3)])
        .expect("a truncated SRP entry is skipped, not an error");
    assert!(titles.is_empty());
}

// TT_SRP entry is 12 bytes; chapter count is the 16-bit field at +2, and
// each entry's own value must carry through. Distinct counts per entry
// catch a read from a neighbouring offset.
#[test]
fn tt_srpt_chapter_count_read_from_entry_offset_2() {
    let data = tt_srpt_bytes(3, &[(7, 1, 1), (13, 1, 2), (0x0102, 2, 1)]);
    let map = parse_tt_srpt(&data, 0).unwrap();
    assert_eq!(map[&1], vec![(7u16, 1u8), (13, 2)]);
    assert_eq!(map[&2], vec![(0x0102u16, 1u8)]);
}

// ── TT_SRPT: the title table (`parse_tt_srpt` / `parse_vmg`) ────────────
// This table says which titles a DVD has and had no tests until 1.6.2, despite
// two real-disc 1.6.1 bugs that silently mis-enumerated titles through it.

/// Build a VMG with a TT_SRPT at `tt_srpt_sector` declaring `entries`
/// of `(num_chapters, vts_number, vts_title_num)`.
fn vmg_with_tt_srpt(tt_srpt_sector: u32, entries: &[(u16, u8, u8)]) -> Vec<u8> {
    let off = tt_srpt_sector as usize * crate::consts::SECTOR_BYTES;
    let mut v = vec![0u8; off + 8 + entries.len() * 12 + 16];
    v[0..12].copy_from_slice(b"DVDVIDEO-VMG");
    v[0xC4..0xC8].copy_from_slice(&tt_srpt_sector.to_be_bytes());
    v[off..off + 2].copy_from_slice(&(entries.len() as u16).to_be_bytes());
    for (i, &(chapters, vts, title)) in entries.iter().enumerate() {
        let b = off + 8 + i * 12;
        v[b + 2..b + 4].copy_from_slice(&chapters.to_be_bytes());
        v[b + 6] = vts;
        v[b + 7] = title;
    }
    v
}

const TX_DISC: u8 = 0x01;
const TX_TITLE: u8 = 0x02;
const TX_CHAPTER: u8 = 0x04;

fn names(v: &[Option<&str>]) -> Vec<Option<String>> {
    v.iter().map(|n| n.map(str::to_string)).collect()
}

/// Text-data chapter names map onto TT_SRPT titles in order, keyed by
/// (vts, vts_title_num); the disc name and unknown item kinds are skipped.
#[test]
fn txtdt_chapter_names_map_to_tt_srpt_titles() {
    let vmg = vmg_with_tt_srpt(1, &[(3, 1, 1), (2, 2, 1)]);
    let txtdt = build_txtdt_mg(
        TXTDT_CHARSET_ISO8859_1,
        &[
            (TX_DISC, b"disc1"),
            (0x08, b"authoring tag"),
            (TX_TITLE, b"feature"),
            (TX_CHAPTER, b"1_opening"),
            (TX_CHAPTER, b" 1_show "),
            (TX_CHAPTER, b"END"),
            (TX_TITLE, b"extra"),
            (TX_CHAPTER, b"Caf\xe9"),
            (TX_CHAPTER, b"Chapter 2"),
        ],
    );
    let vmg = with_txtdt(vmg, &txtdt);
    let map = parse_txtdt_chapter_names(&vmg, SECTOR_BYTES);
    assert_eq!(map.len(), 2);
    assert_eq!(
        map[&(1, 1)],
        names(&[Some("1_opening"), Some("1_show"), Some("END")])
    );
    assert_eq!(map[&(2, 1)], names(&[Some("Café"), Some("Chapter 2")]));
}

/// Empty, control-bearing, overlong or wrong-charset names are `None` (numeric
/// fallback); a title whose name count differs from its chapters is dropped.
#[test]
fn txtdt_garbled_names_fall_back() {
    let long = vec![b'x'; TXTDT_MAX_NAME + 1];
    let vmg = vmg_with_tt_srpt(1, &[(5, 1, 1), (3, 1, 2), (1, 1, 3)]);
    let items: &[(u8, &[u8])] = &[
        (TX_TITLE, b"t1"),
        (TX_CHAPTER, b""),
        (TX_CHAPTER, b"bell\x07"),
        (TX_CHAPTER, b"c1\x85"),
        (TX_CHAPTER, &long),
        (TX_CHAPTER, b"ok"),
        (TX_TITLE, b"t2"),
        (TX_CHAPTER, b"only one of three"),
        (TX_TITLE, b"t3"),
        (TX_CHAPTER, b"   "),
    ];
    let latin1 = with_txtdt(vmg.clone(), &build_txtdt_mg(TXTDT_CHARSET_ISO8859_1, items));
    let map = parse_txtdt_chapter_names(&latin1, SECTOR_BYTES);
    assert_eq!(map.len(), 1, "t2 mismatched, t3 has no usable name");
    assert_eq!(map[&(1, 1)], names(&[None, None, None, None, Some("ok")]));

    // Shift JIS (0x12) is not decoded: only plain ASCII survives.
    let sjis = with_txtdt(
        vmg_with_tt_srpt(1, &[(2, 1, 1)]),
        &build_txtdt_mg(
            0x12,
            &[
                (TX_TITLE, b"t"),
                (TX_CHAPTER, b"\x83e"),
                (TX_CHAPTER, b"ascii"),
            ],
        ),
    );
    let map = parse_txtdt_chapter_names(&sjis, SECTOR_BYTES);
    assert_eq!(map[&(1, 1)], names(&[None, Some("ascii")]));
}

/// Every item of a full table pointing into one long unterminated run (the item table
/// itself, crafted free of TAB and NUL) costs one bounded look, not a scan to the end.
#[test]
fn txtdt_unterminated_names_are_read_bounded() {
    let items = usize::from(u16::MAX / 2);
    let base = TXTDT_MG_HDR + 8 + TXTDT_LU_ITEMS;
    let mut d = vec![0xFFu8; base + 4 + items * 8 + 1024 * 1024];
    d[..TXTDT_MG_HDR + 8].fill(0x01);
    d[TXTDT_MG_HDR + 4..TXTDT_MG_HDR + 8]
        .copy_from_slice(&((TXTDT_MG_HDR + 8) as u32).to_be_bytes());
    let last = (d.len() - 1) as u32;
    d[0x10..0x14].copy_from_slice(&last.to_be_bytes());
    for i in 0..items {
        let item = base + 4 + i * 8;
        d[item] = if i == 0 { TX_TITLE } else { TX_CHAPTER };
        d[item + 6..item + 8].copy_from_slice(&0x0404u16.to_be_bytes());
    }
    let vmg = with_txtdt(vmg_with_tt_srpt(1, &[(2, 1, 1)]), &d);
    let started = std::time::Instant::now();
    let titles = txtdt_titles(&vmg).expect("table");
    assert_eq!(titles[0].len(), items - 1);
    assert!(
        titles[0].iter().all(Option::is_none),
        "overlong names are no names"
    );
    let took = started.elapsed();
    assert!(took < std::time::Duration::from_secs(5), "took {took:?}");
}

/// No text data, bad pointers, bad lengths and truncation yield no names and
/// never read out of bounds or panic.
#[test]
fn txtdt_bad_offsets_and_lengths_are_bounded() {
    let vmg = vmg_with_tt_srpt(1, &[(2, 1, 1)]);
    let txtdt = build_txtdt_mg(
        TXTDT_CHARSET_ISO646,
        &[(TX_TITLE, b"t"), (TX_CHAPTER, b"a"), (TX_CHAPTER, b"b")],
    );
    let good = with_txtdt(vmg.clone(), &txtdt);
    let mg = be_u32(&good, TXTDT_MG_PTR).unwrap() as usize * SECTOR_BYTES;
    assert_eq!(parse_txtdt_chapter_names(&good, SECTOR_BYTES).len(), 1);
    assert!(parse_txtdt_chapter_names(&vmg, SECTOR_BYTES).is_empty());

    let put32 = |at: usize, v: u32| {
        let mut d = good.clone();
        d[at..at + 4].copy_from_slice(&v.to_be_bytes());
        d
    };
    let lu = mg + TXTDT_MG_HDR + 8;
    let base = lu + TXTDT_LU_ITEMS;
    let cases = [
        ("mg past end", put32(TXTDT_MG_PTR, 1000)),
        ("mg huge", put32(TXTDT_MG_PTR, u32::MAX)),
        ("last byte short", put32(mg + 0x10, 0x20)),
        ("last byte huge", put32(mg + 0x10, u32::MAX)),
        ("no units", put32(mg + 0x0C, 0)),
        ("unit huge", put32(mg + TXTDT_MG_HDR + 4, u32::MAX)),
        ("item count huge", put32(base, 0xFFFF_0000)),
        (
            "string offset past end",
            put32(base + 4 + 8 + 4, 0x0000_FFFF),
        ),
    ];
    for (name, d) in cases {
        let map = parse_txtdt_chapter_names(&d, SECTOR_BYTES);
        if let Some(n) = map.get(&(1, 1)) {
            assert_eq!(n.len(), 2, "{name}");
        }
    }
    assert!(
        parse_txtdt_chapter_names(&put32(TXTDT_MG_PTR, 0), SECTOR_BYTES).is_empty(),
        "a zero pointer means no text data"
    );
    // Every truncation and every single-byte corruption of the table.
    for len in 0..good.len() {
        parse_txtdt_chapter_names(&good[..len], SECTOR_BYTES);
    }
    for at in mg..good.len() {
        let mut d = good.clone();
        d[at] = 0xFF;
        parse_txtdt_chapter_names(&d, SECTOR_BYTES);
    }
}

#[test]
fn tt_srpt_groups_titles_by_their_title_set() {
    // Three titles: two in VTS 1, one in VTS 2 — the ordinary shape.
    let vmg = vmg_with_tt_srpt(1, &[(12, 1, 1), (5, 1, 2), (20, 2, 1)]);
    let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
    assert_eq!(map.len(), 2, "two title sets");
    assert_eq!(
        map[&1],
        vec![(12, 1), (5, 2)],
        "VTS 1 keeps both titles in order"
    );
    assert_eq!(map[&2], vec![(20, 1)], "VTS 2 keeps its one");
}

// Pointer at 0xC4 is a SECTOR offset from VIDEO_TS.IFO start, not a byte
// offset. Reading as bytes lands 1/2048th of the way in — a zero-filled
// region on most discs — reporting no titles rather than failing loudly.
#[test]
fn tt_srpt_pointer_is_a_sector_offset_not_a_byte_offset() {
    for sector in [1u32, 2, 5] {
        let vmg = vmg_with_tt_srpt(sector, &[(3, 1, 1)]);
        let at_sector = parse_tt_srpt(&vmg, sector as usize * crate::consts::SECTOR_BYTES)
            .expect("reading at the sector offset works");
        assert_eq!(at_sector[&1], vec![(3, 1)], "sector {sector}");
        // Reading the same pointer as a BYTE offset lands 1/2048th of the way
        // in and yields a different table without erroring — silently wrong
        // titles, which is how dropped titles looked like a scan difference.
        let as_bytes = parse_tt_srpt(&vmg, sector as usize).expect("does not error");
        assert_ne!(
            as_bytes, at_sector,
            "sector {sector}: a byte-offset read must not coincidentally \
                 agree with the correct one, or this test proves nothing"
        );
    }
}

/// A VTS number of 0 is not a title set; entries carrying it are padding.
#[test]
fn tt_srpt_skips_entries_with_no_title_set() {
    let vmg = vmg_with_tt_srpt(1, &[(9, 0, 1), (4, 3, 1)]);
    let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
    assert_eq!(map.len(), 1, "the vts=0 entry is dropped");
    assert_eq!(map[&3], vec![(4, 1)]);
}

/// The same (title set, title) twice is one title, not two. Real discs
/// carry duplicate TT_SRPT rows.
#[test]
fn tt_srpt_collapses_a_duplicate_title() {
    let vmg = vmg_with_tt_srpt(1, &[(7, 2, 1), (7, 2, 1), (8, 2, 2)]);
    let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
    assert_eq!(
        map[&2],
        vec![(7, 1), (8, 2)],
        "the repeat is dropped, the distinct one kept"
    );
}

/// A truncated table parses what is there rather than failing the whole
/// disc — one unreadable row should not cost a user every other title.
#[test]
fn tt_srpt_truncated_keeps_the_entries_it_can_read() {
    let mut vmg = vmg_with_tt_srpt(1, &[(1, 1, 1), (2, 2, 1), (3, 3, 1)]);
    vmg.truncate(crate::consts::SECTOR_BYTES + 8 + 12 + 6); // 1 whole entry + a fragment
    let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
    assert_eq!(map.len(), 1, "the complete entry survives");
    assert_eq!(map[&1], vec![(1, 1)]);
}

/// The count is an untrusted u16 off the disc. DVD-Video caps a disc at 99
/// titles, and each entry re-parses a PGC into a full DvdTitle — so an
/// unclamped 65535 is a ~540 MB allocation from a ~800 KB crafted file.
#[test]
fn tt_srpt_clamps_an_absurd_declared_title_count() {
    // The fixture must actually CONTAIN more entries than the cap, or the walk
    // stops when the buffer runs out and the clamp is never exercised — an
    // earlier version declared u16::MAX over a 2KB fixture and passed vacuously.
    const PRESENT: usize = MAX_TT_SRPT_TITLES + 40;
    let entries: Vec<(u16, u8, u8)> = (0..PRESENT)
        .map(|i| (1u16, 1u8, (i % 250 + 1) as u8))
        .collect();
    let mut vmg = vmg_with_tt_srpt(1, &entries);
    let off = crate::consts::SECTOR_BYTES;
    vmg[off..off + 2].copy_from_slice(&u16::MAX.to_be_bytes());

    let map = parse_tt_srpt(&vmg, off).expect("parse");
    let total: usize = map.values().map(|v| v.len()).sum();
    assert!(
        total <= MAX_TT_SRPT_TITLES,
        "a declared count of u16::MAX over {PRESENT} real entries must be \
             clamped to {MAX_TT_SRPT_TITLES}, got {total}"
    );
}

// ── Review-round tests: PTT_SRPT, angles, chapter rates, VMG/VTS ─────

/// VTS_TTN is 1-based: an entry carrying title number 0 would alias title 1.
#[test]
fn tt_srpt_skips_title_number_zero() {
    let vmg = vmg_with_tt_srpt(1, &[(9, 1, 0), (4, 1, 1)]);
    let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
    assert_eq!(map[&1], vec![(4, 1)], "the title-number-0 entry is dropped");
}

// PTT_SRPT bytes: header, one u32 offset per title, then one (pgcn, pgn) entry per title.
fn build_ptt_srpt(pgcn_per_title: &[u16]) -> Vec<u8> {
    let n = pgcn_per_title.len();
    let total = 8 + n * 4 + n * 4;
    let mut d = vec![0u8; total];
    d[0..2].copy_from_slice(&(n as u16).to_be_bytes());
    d[4..8].copy_from_slice(&((total - 1) as u32).to_be_bytes());
    for (i, &pgcn) in pgcn_per_title.iter().enumerate() {
        let entry = 8 + n * 4 + i * 4;
        d[8 + i * 4..12 + i * 4].copy_from_slice(&(entry as u32).to_be_bytes());
        d[entry..entry + 2].copy_from_slice(&pgcn.to_be_bytes());
        d[entry + 2..entry + 4].copy_from_slice(&1u16.to_be_bytes());
    }
    d
}

// The title number and its entry offset are both bounded by the table: bytes beyond
// it are never read as a PGCN. The table sits at byte 8 (offset 0 means absent).
#[test]
fn ptt_srpt_pgc_index_stays_inside_the_table() {
    let put = |d: &mut Vec<u8>, at: usize, v: &[u8]| d[8 + at..8 + at + v.len()].copy_from_slice(v);
    let mut d = vec![0u8; 8 + 24];
    put(&mut d, 0, &1u16.to_be_bytes());
    put(&mut d, 4, &23u32.to_be_bytes());
    put(&mut d, 8, &16u32.to_be_bytes());
    put(&mut d, 12, &20u32.to_be_bytes());
    put(&mut d, 16, &3u16.to_be_bytes());
    put(&mut d, 20, &9u16.to_be_bytes());
    assert_eq!(ptt_srpt_pgc_index(&d, 8, 1), Some(2));
    assert_eq!(
        ptt_srpt_pgc_index(&d, 8, 2),
        None,
        "title 2 is past the count"
    );
    // The declared table ends before the entry.
    put(&mut d, 4, &15u32.to_be_bytes());
    assert_eq!(
        ptt_srpt_pgc_index(&d, 8, 1),
        None,
        "entry outside the table"
    );
}

#[test]
fn ptt_srpt_counts_the_pgcs_a_title_spans() {
    // Title 1: one PTT per PGC (1, 2, 3); title 2: two PTTs in PGC 4.
    let entries: [(u16, u16); 5] = [(1, 1), (2, 1), (3, 1), (4, 1), (4, 2)];
    let total = 8 + 2 * 4 + entries.len() * 4;
    let mut d = vec![0u8; total];
    d[0..2].copy_from_slice(&2u16.to_be_bytes());
    d[4..8].copy_from_slice(&((total - 1) as u32).to_be_bytes());
    d[8..12].copy_from_slice(&16u32.to_be_bytes());
    d[12..16].copy_from_slice(&28u32.to_be_bytes());
    for (i, (pgcn, pgn)) in entries.iter().enumerate() {
        d[16 + i * 4..18 + i * 4].copy_from_slice(&pgcn.to_be_bytes());
        d[18 + i * 4..20 + i * 4].copy_from_slice(&pgn.to_be_bytes());
    }
    assert_eq!(ptt_srpt_pgc_count(&d, 0, 1), 0);
    let mut buf = vec![0u8; 4];
    buf.extend_from_slice(&d);
    assert_eq!(ptt_srpt_pgc_count(&buf, 4, 1), 3);
    assert_eq!(ptt_srpt_pgc_count(&buf, 4, 2), 1);
    assert_eq!(ptt_srpt_pgc_count(&buf, 4, 3), 0);
}

/// TTN -> PGC goes through VTS_PTT_SRPT, not "TTN N is PGCIT entry N"; the PGCIT
/// sits at a NON-ZERO offset so the pgcit base term is exercised, and the real TTN
/// is stamped on each title (disc/dvd.rs joins on it).
#[test]
fn pgcit_maps_title_numbers_through_ptt_srpt() {
    let pgc0 = build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None);
    let pgc1 = build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None);
    let pgcit = build_pgcit(&[pgc0, pgc1]);
    let pgcit_at = 512usize;
    let mut data = vec![0u8; pgcit_at];
    data.extend_from_slice(&pgcit);
    let ptt_at = data.len();
    // Title 1 -> PGC 2, title 2 -> PGC 1, title 3 -> PGC 2 (past... TTN > num_pgcs).
    data.extend_from_slice(&build_ptt_srpt(&[2, 1, 2]));

    let titles = parse_pgcit(&data, pgcit_at, ptt_at, &[(5, 1), (6, 2), (7, 3)]).unwrap();
    let got: Vec<(u8, f64)> = titles
        .iter()
        .map(|t| (t.vts_title_num, t.duration_secs))
        .collect();
    assert_eq!(got, vec![(1, 22.022), (2, 11.011), (3, 22.022)]);

    // No PTT_SRPT (offset 0), or a PGCN of 0: fall back to the 1:1 assumption.
    let titles = parse_pgcit(&data, pgcit_at, 0, &[(5, 1)]).unwrap();
    assert_eq!(titles[0].duration_secs, 11.011);
    let mut bad = data.clone();
    let entry = ptt_at + 8 + 3 * 4; // title 1's (pgcn, pgn)
    bad[entry..entry + 2].copy_from_slice(&0u16.to_be_bytes());
    let titles = parse_pgcit(&bad, pgcit_at, ptt_at, &[(5, 1)]).unwrap();
    assert_eq!(titles[0].duration_secs, 11.011);
}

/// A PTT_SRPT PGCN past the table falls back to the 1:1 mapping, not a dropped title.
#[test]
fn pgcit_ptt_pgcn_past_table_falls_back_to_one_to_one() {
    let pgc0 = build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None);
    let pgc1 = build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None);
    let pgcit = build_pgcit(&[pgc0, pgc1]);
    let pgcit_at = 512usize;
    let mut data = vec![0u8; pgcit_at];
    data.extend_from_slice(&pgcit);
    let ptt_at = data.len();
    data.extend_from_slice(&build_ptt_srpt(&[9, 9]));
    let titles = parse_pgcit(&data, pgcit_at, ptt_at, &[(5, 1), (6, 2)]).unwrap();
    let got: Vec<f64> = titles.iter().map(|t| t.duration_secs).collect();
    assert_eq!(got, vec![11.011, 22.022]);
}

/// A title number past num_pgcs is dropped, even when an in-buffer 8-byte
/// "entry" happens to sit at that index.
#[test]
fn pgcit_title_past_num_pgcs_is_dropped() {
    let pgc0 = build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None);
    let pgc1 = build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None);
    let mut data = build_pgcit(&[pgc0.clone(), pgc0.clone()]);
    // Entry index 2 lands inside PGC 0's bytes; make it point at a valid PGC.
    let pgc1_at = data.len();
    data.extend_from_slice(&pgc1);
    data[28..32].copy_from_slice(&(pgc1_at as u32).to_be_bytes());
    let titles = parse_pgcit(&data, 0, 0, &[(5, 3)]).unwrap();
    assert!(titles.is_empty(), "TTN 3 of a 2-PGC table must be omitted");
}

/// Cells with no rate flag fall back to literal seconds; in a mixed-rate PGC the
/// first rate seen wins for every frame count.
#[test]
fn pgc_chapter_times_unknown_and_mixed_rates() {
    let unknown = |secs: u8| [0, 0, secs, 0];
    let pgc = build_pgc(
        bcd_secs(59),
        &[
            (0x00, unknown(0x10), 0, 9),
            (0x00, bcd_secs(20), 10, 19),
            (0x00, unknown(0x05), 20, 29),
        ],
        &[1, 2, 3],
        None,
    );
    let t = parse_pgc(&pgc, 0, 3).unwrap();
    let want = [0.0, 10.0, 30.02];
    for (g, w) in t.chapter_times.iter().zip(want) {
        assert!(
            (g - w).abs() < 1e-9,
            "unknown-rate chapters {:?}",
            t.chapter_times
        );
    }
    // PAL (25 fps) cell first, then NTSC cells: everything is read at the PAL rate.
    let pal10 = [0, 0, 0x10, 0b01_000000];
    let pgc = build_pgc(
        bcd_secs(59),
        &[
            (0x00, pal10, 0, 9),
            (0x00, bcd_secs(20), 10, 19),
            (0x00, bcd_secs(10), 20, 29),
        ],
        &[1, 3],
        None,
    );
    let t = parse_pgc(&pgc, 0, 2).unwrap();
    // (250 + 600) frames at 1/25 s.
    assert!(
        (t.chapter_times[1] - 34.0).abs() < 1e-9,
        "{:?}",
        t.chapter_times
    );
}

// ── VMG / VTS through a real UDF image ───────────────────────────────

use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};

// A VTS_XX_0.IFO: header at sector 0, PGCIT at sector 2, optional PTT_SRPT at sector 3.
fn build_vts_ifo(audio: u16, subs: u16, pgcs: &[Vec<u8>], ptt: Option<&[u16]>) -> Vec<u8> {
    let mut d = vec![0u8; 4 * 2048];
    d[0..12].copy_from_slice(VTS_MAGIC);
    d[0xC4..0xC8].copy_from_slice(&7u32.to_be_bytes()); // vtstt_vobs
    d[0xCC..0xD0].copy_from_slice(&2u32.to_be_bytes()); // PGCIT sector
    d[0x202..0x204].copy_from_slice(&audio.to_be_bytes());
    d[0x254..0x256].copy_from_slice(&subs.to_be_bytes());
    let pgcit = build_pgcit(pgcs);
    d[2 * 2048..2 * 2048 + pgcit.len()].copy_from_slice(&pgcit);
    if let Some(p) = ptt {
        d[0xC8..0xCC].copy_from_slice(&3u32.to_be_bytes());
        let t = build_ptt_srpt(p);
        d[3 * 2048..3 * 2048 + t.len()].copy_from_slice(&t);
    }
    d
}

fn video_ts_disc(files: Vec<(&str, Vec<u8>)>) -> (MemDisc, UdfFs) {
    let files = files
        .into_iter()
        .enumerate()
        .map(|(i, (n, c))| file_with(n, 30 + i as u32, 5000 + 100 * i as u32, c, false))
        .collect();
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".into(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

fn one_pgc() -> Vec<Vec<u8>> {
    vec![build_pgc(
        bcd_secs(22),
        &[(0x00, bcd_secs(22), 50, 59)],
        &[],
        None,
    )]
}

/// The 8 audio / 32 subtitle stream caps, the ptt pointer at 0xC8 and the vob
/// start rebase, all through parse_vts.
#[test]
fn parse_vts_caps_streams_and_reads_ptt_and_vob_start() {
    let two = vec![
        build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None),
        build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None),
    ];
    let ifo = build_vts_ifo(0xFFFF, 0xFFFF, &two, Some(&[2]));
    let (mut disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", ifo)]);
    let ts = parse_vts(&mut disc, &udf, 1, &[(5, 1)]).expect("vts parses");
    assert_eq!(ts.audio_streams.len(), 8);
    assert_eq!(ts.subtitle_streams.len(), 32);
    let lba = udf
        .file_start_lba(&mut disc, "/VIDEO_TS/VTS_01_0.IFO")
        .unwrap();
    assert_eq!(ts.vob_start_sector, lba + 7);
    assert_eq!(ts.titles[0].duration_secs, 22.022, "TTN 1 -> PGC 2 via PTT");
}

fn bup_fixture() -> (Vec<u8>, Vec<u8>) {
    let two = vec![
        build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None),
        build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None),
    ];
    let good = build_vts_ifo(0, 0, &two, None);
    let mut bad = good.clone();
    bad[0] = b'X';
    (bad, good)
}

/// A bad IFO falls back to the BUP, but the title VOBS stay anchored on the IFO's LBA.
#[test]
fn parse_vts_falls_back_to_the_bup_and_anchors_on_the_ifo() {
    let (bad, good) = bup_fixture();
    let (mut disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", bad), ("VTS_01_0.BUP", good)]);
    let ts = parse_vts(&mut disc, &udf, 1, &[(1, 1), (1, 2)]).expect("BUP used");
    assert_eq!(ts.titles.len(), 2);
    let lba = udf
        .file_start_lba(&mut disc, "/VIDEO_TS/VTS_01_0.IFO")
        .unwrap();
    assert_eq!(ts.vob_start_sector, lba + 7);
}

/// An IFO that parses but drops a damaged PGC defers to a BUP that keeps it.
#[test]
fn parse_vts_prefers_a_bup_that_keeps_more_titles() {
    let (_, good) = bup_fixture();
    let mut damaged = good.clone();
    // Second PGC entry's offset: point it past the end of the table.
    damaged[2 * 2048 + 8 + 8 + 4..2 * 2048 + 8 + 8 + 8]
        .copy_from_slice(&0x00ff_ffffu32.to_be_bytes());
    let (mut disc, udf) = video_ts_disc(vec![
        ("VTS_01_0.IFO", damaged.clone()),
        ("VTS_01_0.BUP", good),
    ]);
    let ts = parse_vts(&mut disc, &udf, 1, &[(1, 1), (1, 2)]).expect("vts");
    assert_eq!(ts.titles.len(), 2, "BUP kept both titles");
    // With no usable BUP the IFO's partial result stands.
    let (mut disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", damaged)]);
    let ts = parse_vts(&mut disc, &udf, 1, &[(1, 1), (1, 2)]).expect("vts");
    assert_eq!(ts.titles.len(), 1);
}

/// A stop during the BUP attempt is a stop, not a reason to report the IFO's error.
#[test]
fn parse_vts_propagates_a_halt_during_the_bup_attempt() {
    struct HaltAt(MemDisc, u32);
    impl SectorSource for HaltAt {
        fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            if lba == self.1 {
                return Err(Error::Halted);
            }
            self.0.read_sectors(lba, n, buf, r)
        }
    }
    let (bad, good) = bup_fixture();
    let (mut disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", bad), ("VTS_01_0.BUP", good)]);
    let bup_lba = udf
        .file_start_lba(&mut disc, "/VIDEO_TS/VTS_01_0.BUP")
        .unwrap();
    let r = parse_vts(&mut HaltAt(disc, bup_lba), &udf, 1, &[(1, 1)]);
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
}

#[test]
fn parse_vts_rejects_bad_magic_and_short_files() {
    let mut bad_magic = build_vts_ifo(0, 0, &one_pgc(), None);
    bad_magic[0] = b'X';
    let short = bad_magic[..0x100].to_vec();
    let mut short_ok_magic = short.clone();
    short_ok_magic[0..12].copy_from_slice(VTS_MAGIC);
    let (mut disc, udf) = video_ts_disc(vec![
        ("VTS_01_0.IFO", bad_magic),
        ("VTS_02_0.IFO", short_ok_magic),
    ]);
    for n in [1u8, 2] {
        let r = parse_vts(&mut disc, &udf, n, &[(5, 1)]);
        assert!(matches!(r, Err(Error::IfoParse)), "vts {n}: {r:?}");
    }
}

fn vmg_at_sector(sector: u32, entries: &[(u16, u8, u8)]) -> Vec<u8> {
    vmg_with_tt_srpt(sector, entries)
}

/// parse_vmg_with reads the TT_SRPT pointer at 0xC4 as a SECTOR offset, guards
/// magic/length/bounds, and skips a title set whose IFO is missing.
#[test]
fn parse_vmg_with_reads_sector_pointer_and_skips_failed_title_sets() {
    let vts = build_vts_ifo(1, 1, &one_pgc(), None);
    let (mut disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", vts)]);
    // Sector 3 (not 1): a byte-offset or wrong-field read would find nothing.
    let vmg = vmg_at_sector(3, &[(5, 1, 1), (5, 2, 1)]);
    let info = parse_vmg_with(&mut disc, &udf, Some(&vmg)).expect("vmg parses");
    assert_eq!(info.title_sets.len(), 1, "VTS 2 has no IFO: skipped");
    assert_eq!(info.region_mask, 0, "no region bits set");
    let mut coded = vmg.clone();
    coded[0x22] = 0x11; // VMG_CATEGORY's first byte is not the mask
    coded[0x23] = 0xFE;
    let info = parse_vmg_with(&mut disc, &udf, Some(&coded)).expect("vmg parses");
    assert_eq!(info.region_mask, 0xFE);
    assert_eq!(info.title_sets[0].vts_number, 1);
    assert_eq!(info.title_sets[0].titles[0].vts_title_num, 1);

    let mut bad_magic = vmg.clone();
    bad_magic[0] = b'X';
    let short = vmg[..0xC7].to_vec();
    let mut oob = vmg.clone();
    oob[0xC4..0xC8].copy_from_slice(&1000u32.to_be_bytes());
    let mut huge = vmg.clone();
    huge[0xC4..0xC8].copy_from_slice(&u32::MAX.to_be_bytes());
    for (name, v) in [
        ("magic", bad_magic),
        ("short", short),
        ("oob", oob),
        ("huge", huge),
    ] {
        let r = parse_vmg_with(&mut disc, &udf, Some(&v));
        assert!(matches!(r, Err(Error::IfoParse)), "{name}: {r:?}");
    }
}

/// parse_vmg_with carries the text-data chapter names into DvdInfo.
#[test]
fn parse_vmg_with_reads_txtdt_chapter_names() {
    let vts = build_vts_ifo(1, 1, &one_pgc(), None);
    let (mut disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", vts)]);
    let txtdt = build_txtdt_mg(
        TXTDT_CHARSET_ISO646,
        &[(TX_TITLE, b"main"), (TX_CHAPTER, b"intro")],
    );
    let vmg = with_txtdt(vmg_at_sector(3, &[(1, 1, 1)]), &txtdt);
    let info = parse_vmg_with(&mut disc, &udf, Some(&vmg)).expect("vmg parses");
    assert_eq!(info.chapter_names[&(1, 1)], names(&[Some("intro")]));
}

// Reader that reports a halt once armed.
struct HaltingDisc {
    inner: MemDisc,
    halted: bool,
}
impl SectorSource for HaltingDisc {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        if self.halted {
            return Err(Error::Halted);
        }
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

/// A halted drive is not a placeholder title set: Halted propagates instead of
/// silently truncating the scan.
#[test]
fn parse_vmg_with_propagates_halted() {
    let vts = build_vts_ifo(0, 0, &one_pgc(), None);
    let (disc, udf) = video_ts_disc(vec![("VTS_01_0.IFO", vts)]);
    let mut reader = HaltingDisc {
        inner: disc,
        halted: true,
    };
    let vmg = vmg_at_sector(1, &[(5, 1, 1)]);
    let r = parse_vmg_with(&mut reader, &udf, Some(&vmg));
    assert!(matches!(r, Err(Error::Halted)), "got {r:?}");
}
