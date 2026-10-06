use super::*;
use crate::mux::ts::PesPacket;

fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
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
fn vc1_populates_measured_coding_type_and_source() {
    use super::super::coding::CodingType;
    // Advanced-profile sequence header: 00 00 01 0F, PROFILE=3 (0xC0), then
    // zeros so INTERLACE (bit 41) = 0 → progressive.
    let seq_prog = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0];
    // Frame: 00 00 01 0D then the PTYPE VLC as the first RBSP bits:
    //   0xC0 = '110' → I; 0x00 = '0' → P; 0x80 = '10' → B.
    let frame = |ptype: u8| vec![0x00, 0x00, 0x01, SC_FRAME, ptype];
    let src = crate::pes::SourcePos::at_byte(2048);
    let mut p = Vc1Parser::new();

    // I-frame carrying the seq header → keyframe, sets the active seq header.
    let mut pe = make_pes([seq_prog.clone(), frame(0xC0)].concat(), Some(0));
    pe.source = Some(src);
    let fi = p.parse(&pe);
    assert_eq!(fi.len(), 1);
    assert!(fi[0].keyframe, "seq header present → keyframe");
    let ci = fi[0].coding.expect("VC-1 frame carries PictureInfo");
    assert_eq!(ci.coding_type(), CodingType::I, "PTYPE 110 → I");
    assert!(
        ci.field_order().is_none(),
        "VC-1 field order undecoded → None, never faked"
    );
    assert_eq!(
        fi[0].source.unwrap().byte,
        2048,
        "source provenance carried"
    );

    // P / B frames (no seq header; the active progressive seq header
    // persists) → measured P / B, not keyframes.
    let fp = p.parse(&make_pes(frame(0x00), Some(0)));
    assert!(!fp[0].keyframe);
    assert_eq!(
        fp[0].coding.unwrap().coding_type(),
        CodingType::P,
        "PTYPE 0 → P"
    );
    let fb = p.parse(&make_pes(frame(0x80), Some(0)));
    assert_eq!(
        fb[0].coding.unwrap().coding_type(),
        CodingType::B,
        "PTYPE 10 → B"
    );
}

#[test]
fn vc1_interlaced_declines_coding_type_never_guesses() {
    // Interlaced sequence (INTERLACE bit 41 = 1): FCM/FPTYPE precede PTYPE
    // and are NOT decoded here, so the coding type is honestly omitted
    // rather than read at the wrong bit offset.
    let seq_int = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0x40];
    let frame = vec![0x00, 0x00, 0x01, SC_FRAME, 0xC0];
    let mut p = Vc1Parser::new();
    let f = p.parse(&make_pes([seq_int, frame].concat(), Some(0)));
    assert!(
        f[0].coding.is_none(),
        "interlaced VC-1 → coding omitted, never a guessed type"
    );
}

// `with_ps_reorder(true)` (HD-DVD EVO) must INSTALL the reorderer, and `flush()` must drain
// the frames it still holds at EOF; without it nothing is buffered.
#[test]
fn vc1_ps_reorder_is_installed_and_flush_drains_its_frames() {
    // Progressive advanced-profile header, so PTYPE is measured (I '110', P '0', B '10').
    let seq = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0];
    let pic = |ptype: u8| vec![0x00, 0x00, 0x01, SC_FRAME, ptype];
    let gop = |anchor: i64| {
        vec![
            (([&seq[..], &pic(0xC0)]).concat(), Some(anchor)), // I: keyframe anchor
            (pic(0x00), None),                                 // P
            (pic(0x80), None),                                 // B
            (pic(0x00), None),                                 // P
            (pic(0x80), None),                                 // B
        ]
    };
    let feed = |reorder: bool| -> (Vec<Frame>, Vec<Frame>) {
        let mut p = Vc1Parser::new().with_ps_reorder(reorder);
        let mut during = Vec::new();
        // Two GOPs; the second anchor is 5 frames later (90 kHz: 5 x 3750).
        for (data, pts) in gop(0).into_iter().chain(gop(18_750)) {
            during.extend(p.parse(&make_pes(data, pts)));
        }
        let tail = p.flush();
        (during, tail)
    };

    let (during, tail) = feed(true);
    assert!(
        !tail.is_empty(),
        "the reorderer holds frames; flush releases them"
    );
    assert!(tail.iter().all(|f| !f.data.is_empty()), "real coded bytes");
    let mut pts: Vec<i64> = during.iter().chain(&tail).map(|f| f.pts_ns).collect();
    assert_eq!(pts.len(), 10, "every access unit is emitted exactly once");
    pts.sort_unstable();
    pts.dedup();
    assert_eq!(pts.len(), 10, "reconstructed PTS are all distinct");

    // Off: nothing is buffered, and the sparse-PTS frames collide on the anchor's 0.
    let (raw_during, raw_tail) = feed(false);
    assert!(raw_tail.is_empty(), "no reorderer installed");
    assert_eq!(raw_during.len(), 10);
    assert!(raw_during.iter().filter(|f| f.pts_ns == 0).count() >= 4);
}

// The PES PTS anchors the reconstructed display timeline: with anchors 5 frames apart the
// frame duration calibrates to a fifth of the spacing, and each GOP's frames follow.
#[test]
fn vc1_ps_reorder_stamps_display_order_pts_from_the_pes_anchors() {
    let seq = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0];
    let pic = |ptype: u8| vec![0x00, 0x00, 0x01, SC_FRAME, ptype];
    let mut p = Vc1Parser::new().with_ps_reorder(true);
    let mut frames = Vec::new();
    for anchor in [0i64, 18_750] {
        for (k, ptype) in [0xC0u8, 0x00, 0x80, 0x00, 0x80].into_iter().enumerate() {
            let data = if k == 0 {
                [&seq[..], &pic(ptype)].concat()
            } else {
                pic(ptype)
            };
            frames.extend(p.parse(&make_pes(data, (k == 0).then_some(anchor))));
        }
    }
    frames.extend(p.flush());
    let d = 208_333_333 / 5;
    let gop = |origin: i64| [0, 2, 1, 4, 3].map(|i| origin + i * d);
    let want: Vec<i64> = gop(0).into_iter().chain(gop(208_333_333)).collect();
    let got: Vec<i64> = frames.iter().map(|f| f.pts_ns).collect();
    assert_eq!(got, want);
}

// An advanced-profile sequence header may carry `00 00 03` emulation prevention; the
// resolution and INTERLACE bit are read from the de-escaped bytes.
#[test]
fn sequence_header_fields_are_read_through_emulation_prevention() {
    // De-escaped: PROFILE 3 byte, 00, then W-1 = 0 (12 bits), H-1 = 31 (12 bits),
    // then INTERLACE set at bit 41. Bytes 1..=3 are 00 00 00, so an encoder escapes them.
    let sh = [
        0x00,
        0x00,
        0x01,
        SC_SEQUENCE_HEADER,
        0xC0,
        0x00,
        0x00,
        0x03,
        0x00,
        0x1F,
        0x40,
    ];
    assert_eq!(parse_vc1_resolution(&sh), Some((2, 64)));
    assert_eq!(parse_vc1_interlace(&sh), Some(true));
}

#[test]
fn a_pes_ending_in_a_bare_start_code_prefix_passes_through_without_panicking() {
    let data = vec![0xAA, 0x00, 0x00, 0x01];
    let f = Vc1Parser::new().parse(&make_pes(data.clone(), Some(0)));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].data, data);
}

/// Build a VC-1 PES with sequence header + entry point + frame start code.
fn build_vc1_iframe_pes() -> Vec<u8> {
    let mut data = Vec::new();
    // Sequence header: 00 00 01 0F + payload
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
    // Entry point: 00 00 01 0E + payload
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_ENTRY_POINT]);
    data.extend_from_slice(&[0x11, 0x22, 0x33, 0x44]);
    // Frame: 00 00 01 0D + payload
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data.extend_from_slice(&[0x55, 0x66, 0x77, 0x88, 0x99]);
    data
}

// --- Pictures a decoder cannot reconstruct from the stream's own data ---

/// A progressive advanced-profile entry-point I-picture AU (sequence header,
/// entry-point header with the given BROKEN_LINK / CLOSED_ENTRY, frame)
/// followed, in decode order, by B B P (SMPTE 421M §6.2.1: with CLOSED_ENTRY
/// 0 the Bs after the entry-point I may predict from the anchor before it).
fn entry_gop(broken_link: bool, closed_entry: bool) -> Vec<Vec<u8>> {
    let seq = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0];
    let ep0 = (u8::from(broken_link) << 7) | (u8::from(closed_entry) << 6);
    let ep = [0x00, 0x00, 0x01, SC_ENTRY_POINT, ep0, 0x00];
    let pic = |ptype: u8| vec![0x00, 0x00, 0x01, SC_FRAME, ptype];
    vec![
        [&seq[..], &ep[..], &pic(0xC0)].concat(),
        pic(0x80),
        pic(0x80),
        pic(0x00),
    ]
}

fn coding_types(aus: Vec<Vec<u8>>) -> Vec<CodingType> {
    let mut p = Vc1Parser::new();
    aus.into_iter()
        .flat_map(|au| p.parse(&make_pes(au, Some(0))))
        .map(|f| f.coding.unwrap().coding_type())
        .collect()
}

#[test]
fn an_open_entry_point_at_the_stream_start_drops_its_leading_b_pictures() {
    assert_eq!(
        coding_types(entry_gop(false, false)),
        [CodingType::I, CodingType::P]
    );
}

#[test]
fn interlaced_pictures_are_judged_from_fcm_and_fptype() {
    // INTERLACE = 1. FCM '11' + FPTYPE: '000' I/I, '011' P/P, '100' B/B, '111' BI/BI;
    // FCM '10' (frame interlace) + PTYPE '110' I; FCM '0' + PTYPE '10' B.
    let seq_int = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0x40];
    let pic = |b: u8| vc1_picture(&[b], Some(&seq_int));
    assert_eq!(pic(0b1100_0000), Some(Vc1Pic::Intra));
    assert_eq!(pic(0b1101_1000), Some(Vc1Pic::Predicted));
    assert_eq!(pic(0b1110_0000), Some(Vc1Pic::Bi));
    assert_eq!(pic(0b1111_1000), Some(Vc1Pic::IntraB));
    assert_eq!(pic(0b1011_0000), Some(Vc1Pic::Intra));
    assert_eq!(pic(0b0100_0000), Some(Vc1Pic::Bi));
}

#[test]
fn an_interlaced_open_entry_point_at_the_stream_start_drops_its_leading_b_field_pairs() {
    let seq = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0, 0, 0, 0, 0x40];
    let ep = [0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x00, 0x00];
    let pic = |b: u8| vec![0x00, 0x00, 0x01, SC_FRAME, b];
    let aus = [
        [&seq[..], &ep[..], &pic(0b1100_1000)].concat(), // I/P field pair
        pic(0b1110_0000),                                // B/B, leading
        pic(0b1101_1000),                                // P/P
        pic(0b1110_0000),                                // B/B, trailing
    ];
    let mut p = Vc1Parser::new();
    let n: Vec<usize> = aus
        .into_iter()
        .map(|au| p.parse(&make_pes(au, Some(0))).len())
        .collect();
    assert_eq!(n, [1, 0, 1, 1]);
}

#[test]
fn a_closed_entry_point_at_the_stream_start_keeps_every_picture() {
    assert_eq!(
        coding_types(entry_gop(false, true)),
        [CodingType::I, CodingType::B, CodingType::B, CodingType::P]
    );
}

#[test]
fn a_broken_link_drops_the_leading_b_pictures_mid_stream() {
    let mut aus = entry_gop(false, true);
    aus.extend(entry_gop(true, false));
    assert_eq!(
        coding_types(aus),
        [
            CodingType::I,
            CodingType::B,
            CodingType::B,
            CodingType::P,
            CodingType::I,
            CodingType::P
        ]
    );
}

#[test]
fn a_gap_resync_onto_an_open_entry_point_emits_no_leading_b_pictures() {
    let mut aus = entry_gop(false, true);
    aus.extend(entry_gop(false, false));
    let mut p = Vc1Parser::new();
    let mut gate = crate::mux::resync::ResyncGate::new();
    let types: Vec<CodingType> = aus
        .into_iter()
        .enumerate()
        .flat_map(|(i, au)| {
            let mut pes = make_pes(au, Some(0));
            // The first entry segment's P follows lost data.
            pes.discontinuity = i == 3;
            p.parse(&pes)
        })
        .filter(|f| gate.admit(true, f.discontinuity, f.keyframe))
        .map(|f| f.coding.unwrap().coding_type())
        .collect();
    assert_eq!(
        types,
        [
            CodingType::I,
            CodingType::B,
            CodingType::B,
            CodingType::I,
            CodingType::P
        ],
        "the second entry's Bs reference the P the gate dropped"
    );
}

// --- sequence header detection ---

#[test]
fn parse_sequence_header() {
    let mut parser = Vc1Parser::new();

    let data = build_vc1_iframe_pes();
    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    // Sequence header present → keyframe
    assert!(
        frames[0].keyframe,
        "PES with sequence header should be keyframe"
    );
    // seq_header should be stored internally
    assert!(parser.seq_header.is_some());
}

#[test]
fn parse_entry_point() {
    let mut parser = Vc1Parser::new();

    let data = build_vc1_iframe_pes();
    let pes = make_pes(data, Some(0));
    parser.parse(&pes);

    assert!(parser.entry_point.is_some());
}

// --- codec_private is BITMAPINFOHEADER (40+ bytes) ---

#[test]
fn codec_private_bitmapinfoheader() {
    let mut parser = Vc1Parser::new();

    let data = build_vc1_iframe_pes();
    let pes = make_pes(data, Some(0));
    parser.parse(&pes);

    let cp = parser.codec_private();
    assert!(
        cp.is_some(),
        "codec_private should be Some after seq header + entry point"
    );

    let cp = cp.unwrap();
    // BITMAPINFOHEADER is 40 bytes + extra data
    assert!(
        cp.len() >= 40,
        "codec_private should be at least 40 bytes (BITMAPINFOHEADER)"
    );

    // biSize (first 4 bytes, little-endian) should equal total length
    let bi_size = u32::from_le_bytes([cp[0], cp[1], cp[2], cp[3]]);
    assert_eq!(
        bi_size as usize,
        cp.len(),
        "biSize should match total codec_private length"
    );

    // biCompression = "WVC1" at offset 16
    assert_eq!(&cp[16..20], b"WVC1", "FOURCC should be WVC1");

    // biWidth at offset 4 (little-endian u32) = 1920
    let width = u32::from_le_bytes([cp[4], cp[5], cp[6], cp[7]]);
    assert_eq!(width, 1920);

    // biHeight at offset 8 (little-endian u32) = 1080
    let height = u32::from_le_bytes([cp[8], cp[9], cp[10], cp[11]]);
    assert_eq!(height, 1080);
}

#[test]
fn codec_private_none_before_data() {
    let parser = Vc1Parser::new();
    assert!(parser.codec_private().is_none());
}

#[test]
fn codec_private_none_missing_entry_point() {
    let mut parser = Vc1Parser::new();

    // Only sequence header, no entry point
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    data.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data.extend_from_slice(&[0x55, 0x66]);

    let pes = make_pes(data, Some(0));
    parser.parse(&pes);

    assert!(
        parser.codec_private().is_none(),
        "should be None without entry point"
    );
}

// --- frame without sequence header → not keyframe ---

#[test]
fn parse_non_keyframe() {
    let mut parser = Vc1Parser::new();

    // PES with only a frame start code (no sequence header)
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data.extend_from_slice(&[0x55, 0x66, 0x77]);

    let pes = make_pes(data, Some(180000));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    assert!(
        !frames[0].keyframe,
        "frame without sequence header should not be keyframe"
    );
}

// --- frame data starts from frame start code ---

#[test]
fn frame_data_starts_at_frame_sc() {
    let mut parser = Vc1Parser::new();

    let data = build_vc1_iframe_pes();
    let pes = make_pes(data.clone(), Some(0));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    // Seq+entry seed codecPrivate on first occurrence, but as a keyframe (RAP)
    // they're re-asserted in-band, so frame data STARTS with the seq_header
    // start code, then the SC_FRAME picture data.
    let fd = &frames[0].data;
    assert!(fd.len() >= 4);
    assert_eq!(&fd[0..4], &[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    let frame_sc = fd
        .windows(4)
        .position(|w| w == [0x00, 0x00, 0x01, SC_FRAME]);
    assert!(
        frame_sc.is_some(),
        "SC_FRAME picture data must follow the re-asserted headers"
    );
}

// --- parameter-set-only PES (seq header + entry point, no frame SC) ---

#[test]
fn param_set_only_pes_emits_no_frame() {
    let mut parser = Vc1Parser::new();

    // Sequence header + entry point, but NO frame start code (0x0D).
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_ENTRY_POINT]);
    data.extend_from_slice(&[0x11, 0x22, 0x33, 0x44]);

    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);

    // No coded picture → no frame emitted (parameter bytes must not be
    // passed through as a bogus keyframe).
    assert!(
        frames.is_empty(),
        "parameter-set-only PES should not emit a frame"
    );
    // But codecPrivate is still captured.
    assert!(parser.seq_header.is_some());
    assert!(parser.entry_point.is_some());
    assert!(parser.codec_private().is_some());

    // A following frame-bearing PES still emits its picture.
    let mut data2 = Vec::new();
    data2.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data2.extend_from_slice(&[0x55, 0x66, 0x77]);
    let frames2 = parser.parse(&make_pes(data2, Some(180000)));
    assert_eq!(frames2.len(), 1);
    assert_eq!(&frames2[0].data[0..4], &[0x00, 0x00, 0x01, SC_FRAME]);
}

// --- empty PES ---

#[test]
fn parse_empty_pes() {
    let mut parser = Vc1Parser::new();
    let pes = make_pes(Vec::new(), Some(0));
    let frames = parser.parse(&pes);
    assert!(frames.is_empty());
}

// --- PTS conversion ---

#[test]
fn pts_conversion() {
    let mut parser = Vc1Parser::new();

    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data.extend_from_slice(&[0x55, 0x66]);

    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

// --- PTS (presentation) used for the MKV block timecode, not DTS ---

#[test]
fn pts_preferred_over_dts() {
    let mut parser = Vc1Parser::new();

    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data.extend_from_slice(&[0x55, 0x66]);

    let pes = PesPacket {
        source: None,
        pid: 0x1011,
        pts: Some(180000), // presentation
        dts: Some(90000),  // decode
        data,
        discontinuity: false,
    };
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    // PTS must be used — MKV block timecodes are presentation timestamps.
    assert_eq!(frames[0].pts_ns, 2_000_000_000);
}

// --- advanced-profile resolution parsing (bit-offset regression) --- Builds a seq header
// encoding width/height (PROFILE=3 + coded W/H fields).
fn make_ap_seq_header(width: u32, height: u32) -> Vec<u8> {
    let coded_w = (width / 2) - 1;
    let coded_h = (height / 2) - 1;
    // Accumulate 40 bits MSB-first: 16 leading bits then 12+12.
    let mut acc: u64 = 0;
    let mut nbits = 0u32;
    let put = |val: u64, n: u32, acc: &mut u64, nbits: &mut u32| {
        *acc = (*acc << n) | (val & ((1u64 << n) - 1));
        *nbits += n;
    };
    // PROFILE = 3 (advanced), then 14 more leading bits (all zero here).
    put(0b11, 2, &mut acc, &mut nbits);
    put(0, 14, &mut acc, &mut nbits); // level+colordiff+frmrtq+bitrtq+postproc
    put(coded_w as u64, 12, &mut acc, &mut nbits);
    put(coded_h as u64, 12, &mut acc, &mut nbits);
    // 40 bits → 5 bytes, MSB-first.
    let mut payload = Vec::with_capacity(5);
    for i in (0..5).rev() {
        payload.push(((acc >> (i * 8)) & 0xFF) as u8);
    }
    let mut sh = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER];
    sh.extend_from_slice(&payload);
    sh
}

#[test]
fn advanced_profile_resolution_uses_16bit_offset() {
    // Regression: parser skipped 11 bits (omitting BITRTQ_POSTPROC's 5) instead
    // of 16, reading width/height 5 bits early. Round-trip a non-default
    // 1280x720 to confirm the 16-bit pre-width offset.
    let mut parser = Vc1Parser::new();
    let mut data = make_ap_seq_header(1280, 720);
    // A frame so the parser emits and stores the header.
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME]);
    data.extend_from_slice(&[0x55, 0x66]);
    parser.parse(&make_pes(data, Some(0)));

    let cp = parser.codec_private();
    // codec_private needs an entry point too; resolution is in width/height
    // fields regardless. Read them off the parser via codec_private when
    // available, else assert the internal fields directly.
    assert_eq!(parser.width, 1280, "width parsed at the 16-bit offset");
    assert_eq!(parser.height, 720, "height parsed at the 16-bit offset");
    let _ = cp;
}

// --- codec_private extra data contains seq header + entry point ---

// --- parse_vc1_resolution: profile gating + bounds + de-escaping ---

#[test]
fn resolution_none_for_non_advanced_profile() {
    // Simple (profile 0) and Main (profile 2) don't carry resolution in the
    // sequence header → parse returns None and the parser keeps the 1920x1080
    // default. PROFILE is byte4 bits 7-6.
    for profile in [0u8, 1, 2] {
        let mut sh = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER];
        sh.push(profile << 6); // byte4: profile in top 2 bits
        sh.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(
            parse_vc1_resolution(&sh),
            None,
            "profile {profile} (not advanced) has no header resolution"
        );
    }
}

#[test]
fn resolution_too_short_returns_none() {
    // < 8 bytes can't carry the bit fields → None, no panic.
    let sh = vec![0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xC0, 0x00];
    assert_eq!(parse_vc1_resolution(&sh), None);
}

#[test]
fn resolution_round_trips_4k() {
    // Advanced profile 3840x2160: coded_w = 1920-1 = 1919, coded_h = 1080-1.
    let sh = make_ap_seq_header(3840, 2160);
    assert_eq!(parse_vc1_resolution(&sh), Some((3840, 2160)));
}

#[test]
fn resolution_max_encodable_is_8192_within_bound() {
    // MAX_CODED_WIDTH/HEIGHT are 12-bit (max 4095); decoded dim = (coded+1)*2,
    // so (4095+1)*2 = 8192 is the ceiling and exactly the `<= 8192` accept
    // bound (guard exists for corrupt input). 8192x8192 (coded=4095) must round-trip.
    let sh = make_ap_seq_header(8192, 8192);
    assert_eq!(parse_vc1_resolution(&sh), Some((8192, 8192)));
}

// --- codec_private BITMAPINFOHEADER field layout ---

#[test]
fn codec_private_bitmapinfoheader_fixed_fields() {
    // BITMAPINFOHEADER (40 bytes, little-endian). Verify the fixed fields:
    // biPlanes (u16 @ 12) = 1, biBitCount (u16 @ 14) = 24, biCompression
    // (@16) = "WVC1", and the five trailing u32 fields (@20..40) = 0.
    let mut parser = Vc1Parser::new();
    parser.parse(&make_pes(build_vc1_iframe_pes(), Some(0)));
    let cp = parser.codec_private().unwrap();
    assert_eq!(u16::from_le_bytes([cp[12], cp[13]]), 1, "biPlanes");
    assert_eq!(u16::from_le_bytes([cp[14], cp[15]]), 24, "biBitCount");
    assert_eq!(&cp[16..20], b"WVC1", "biCompression FOURCC");
    // biSizeImage, biXPelsPerMeter, biYPelsPerMeter, biClrUsed, biClrImportant.
    for (i, off) in (20..40).step_by(4).enumerate() {
        let v = u32::from_le_bytes([cp[off], cp[off + 1], cp[off + 2], cp[off + 3]]);
        assert_eq!(v, 0, "BITMAPINFOHEADER trailing field {i} must be 0");
    }
}

#[test]
fn codec_private_extra_data_is_seq_header_then_entry_point() {
    // The extra codec data after the 40-byte header is sequence header bytes
    // immediately followed by entry-point bytes, in that order. Build a
    // header whose seq/entry payloads are distinguishable.
    let mut parser = Vc1Parser::new();
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    data.extend_from_slice(&[0x11, 0x22, 0x33]);
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_ENTRY_POINT]);
    data.extend_from_slice(&[0x44, 0x55]);
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME, 0x66]);
    parser.parse(&make_pes(data, Some(0)));
    let cp = parser.codec_private().unwrap();
    let extra = &cp[40..];
    // seq header: 00 00 01 0F 11 22 33, then entry point: 00 00 01 0E 44 55.
    assert_eq!(
        extra,
        &[
            0x00,
            0x00,
            0x01,
            SC_SEQUENCE_HEADER,
            0x11,
            0x22,
            0x33,
            0x00,
            0x00,
            0x01,
            SC_ENTRY_POINT,
            0x44,
            0x55
        ],
        "extra = seq header then entry point, both Annex B"
    );
}

#[test]
fn codec_private_none_missing_sequence_header() {
    // Entry point alone (no sequence header) → None.
    let mut parser = Vc1Parser::new();
    let mut data = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0xAA, 0xBB];
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME, 0xCC]);
    parser.parse(&make_pes(data, Some(0)));
    assert!(parser.codec_private().is_none());
}

// --- frame start code: only the FIRST 0x0D anchors frame data ---

#[test]
fn frame_data_anchors_at_first_frame_sc_includes_later_codes() {
    // frame_start is set once (the first 0x0D). Frame data runs from there to
    // the end, INCLUDING any later start codes (e.g. slice/field codes). It
    // must not be re-anchored by a second 0x0D.
    let mut parser = Vc1Parser::new();
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, SC_FRAME, 0xAA]); // frame 1 SC
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x0B, 0xBB]); // slice code 0x0B
    let f = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(f.len(), 1);
    // Data begins at the first frame SC and includes everything after.
    assert_eq!(&f[0].data[0..4], &[0x00, 0x00, 0x01, SC_FRAME]);
    assert_eq!(f[0].data.len(), 10, "all bytes from first 0x0D to end kept");
}

#[test]
fn no_start_code_passthrough_as_picture() {
    // A PES with no start code at all (no seq header / entry point either) is
    // a genuine picture payload continuation → passed through whole, not a
    // keyframe.
    let mut parser = Vc1Parser::new();
    let data = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    let f = parser.parse(&make_pes(data.clone(), Some(0)));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].data, data, "passthrough whole");
    assert!(!f[0].keyframe);
}

#[test]
fn scan_finds_start_codes_past_a_long_zero_run_and_4_byte_form() {
    // Pin the tricky shapes the shared scanner must still handle: a long
    // zero run before the real start code, and a 4-byte `00 00 00 01`
    // start code (reported at the inner triple).
    let mut parser = Vc1Parser::new();
    let mut data = vec![0u8; 64]; // long zero run
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, SC_FRAME, 0xAB]); // 4-byte SC
    let f = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(f.len(), 1);
    assert_eq!(
        &f[0].data[0..5],
        &[0x00, 0x00, 0x01, SC_FRAME, 0xAB],
        "frame data starts at the inner 00 00 01 triple, not the outer zero"
    );
}

#[test]
fn entry_point_without_frame_or_seq_header_emits_no_frame() {
    // A PES with ONLY an entry point (no frame SC, no seq header) is a
    // parameter-set-only AU → no coded picture → no frame (has_entry_point
    // path of the None arm).
    let mut parser = Vc1Parser::new();
    let data = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0xAA, 0xBB];
    let f = parser.parse(&make_pes(data, Some(0)));
    assert!(f.is_empty(), "entry-point-only PES emits no frame");
    assert!(parser.entry_point.is_some(), "but entry point captured");
}

#[test]
fn vc1_dts_fallback_and_zero_default() {
    // PTS absent → DTS used; both absent → 0.
    let mut parser = Vc1Parser::new();
    let pes = PesPacket {
        source: None,
        pid: 0x1011,
        pts: None,
        dts: Some(90000),
        data: vec![0x00, 0x00, 0x01, SC_FRAME, 0x55],
        discontinuity: false,
    };
    let f = parser.parse(&pes);
    assert_eq!(f[0].pts_ns, 1_000_000_000, "DTS fallback");

    let mut parser2 = Vc1Parser::new();
    let pes2 = PesPacket {
        source: None,
        pid: 0x1011,
        pts: None,
        dts: None,
        data: vec![0x00, 0x00, 0x01, SC_FRAME, 0x55],
        discontinuity: false,
    };
    let f2 = parser2.parse(&pes2);
    assert_eq!(f2[0].pts_ns, 0, "no PTS/DTS → 0");
}

#[test]
fn codec_private_contains_extra_data() {
    let mut parser = Vc1Parser::new();

    let data = build_vc1_iframe_pes();
    let pes = make_pes(data, Some(0));
    parser.parse(&pes);

    let cp = parser.codec_private().unwrap();
    // After the 40-byte BITMAPINFOHEADER, we should have seq_header + entry_point data
    let extra = &cp[40..];
    assert!(
        !extra.is_empty(),
        "extra data after BITMAPINFOHEADER should not be empty"
    );
    // Extra data should start with the sequence header start code
    assert_eq!(&extra[0..4], &[0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
}

// --- regression: mid-stream entry_point A→B→A revert emitted in-band --- Reverted A must
// still be emitted IN-BAND (decoder already on B needs the explicit revert).
#[test]
fn vc1_emits_entry_point_revert_to_first_value() {
    let sh = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xAA, 0xBB];
    let ep_a = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x11, 0x22];
    let ep_b = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x33, 0x44, 0x55];
    let frame = [0x00, 0x00, 0x01, SC_FRAME, 0x77];

    let mut parser = Vc1Parser::new();

    // AU1: seeds codecPrivate with sh + ep_a. Both are first → stripped from frame.
    let au1: Vec<u8> = sh
        .iter()
        .chain(ep_a.iter())
        .chain(frame.iter())
        .cloned()
        .collect();
    let f1 = parser.parse(&make_pes(au1, Some(0)));
    assert_eq!(f1.len(), 1, "AU1 emits a frame");
    // seq+entry seed codecPrivate, but this is a keyframe (RAP) so the active
    // headers are re-asserted in-band (self-contained RAP) — ep_a present.
    assert!(
        contains_sc(&f1[0].data, SC_ENTRY_POINT),
        "AU1: keyframe re-asserts the active entry_point in-band"
    );
    assert!(
        f1[0].data.windows(ep_a.len()).any(|w| w == ep_a),
        "AU1 carries the active ep_a bytes in-band"
    );

    // AU2: entry_point redefined to B → must be emitted in-band.
    let au2: Vec<u8> = ep_b.iter().chain(frame.iter()).cloned().collect();
    let f2 = parser.parse(&make_pes(au2, Some(90000)));
    assert_eq!(f2.len(), 1, "AU2 emits a frame");
    assert!(
        contains_sc(&f2[0].data, SC_ENTRY_POINT),
        "AU2: redefined entry_point B must be in-band"
    );
    assert!(
        f2[0].data.windows(ep_b.len()).any(|w| w == ep_b),
        "AU2 must carry the ep_b bytes"
    );

    // AU3: entry_point reverts to A (== codecPrivate). Active was B; this is
    // a real change and must still be emitted in-band.
    let au3: Vec<u8> = ep_a.iter().chain(frame.iter()).cloned().collect();
    let f3 = parser.parse(&make_pes(au3, Some(180000)));
    assert_eq!(f3.len(), 1, "AU3 emits a frame");
    assert!(
        f3[0].data.windows(ep_a.len()).any(|w| w == ep_a),
        "AU3: revert to A (== codecPrivate) must be emitted in-band"
    );
}

/// Regression: a bare keyframe (no seq_header / entry_point in PES) after
/// a mid-title redefinition must re-assert the active headers in-band so
/// seek points carry valid decoder state (SMPTE 421M).
#[test]
fn vc1_reasserts_active_headers_at_bare_keyframe() {
    let sh_a = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xAA, 0xBB];
    let ep_a = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x11, 0x22];
    let ep_b = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x33, 0x44, 0x55];
    let frame = [0x00, 0x00, 0x01, SC_FRAME, 0x77];

    let mut parser = Vc1Parser::new();

    // AU1: seed codecPrivate.
    let au1: Vec<u8> = sh_a
        .iter()
        .chain(ep_a.iter())
        .chain(frame.iter())
        .cloned()
        .collect();
    parser.parse(&make_pes(au1, Some(0)));

    // AU2: redefine entry_point to B at a keyframe.
    let au2: Vec<u8> = sh_a
        .iter()
        .chain(ep_b.iter())
        .chain(frame.iter())
        .cloned()
        .collect();
    parser.parse(&make_pes(au2, Some(90000)));

    // AU3: bare keyframe — only SC_SEQUENCE_HEADER (keyframe signal) + SC_FRAME,
    // no entry_point. Active entry_point is B (differs from codecPrivate A);
    // must be re-asserted in-band so seeks into this frame don't revert to A.
    let au3: Vec<u8> = sh_a.iter().chain(frame.iter()).cloned().collect();
    let f3 = parser.parse(&make_pes(au3, Some(180000)));
    assert_eq!(f3.len(), 1, "AU3 emits a frame");
    assert!(
        f3[0].data.windows(ep_b.len()).any(|w| w == ep_b),
        "bare keyframe must re-assert active entry_point B in-band"
    );
    assert!(
        !f3[0].data.windows(ep_a.len()).any(|w| w == ep_a),
        "must not re-assert stale codecPrivate entry_point A"
    );
}

// Regression: keyframe with seq_header UNCHANGED but entry_point REDEFINED must still
// assemble prefix in seq-then-entry order (SMPTE 421M).
#[test]
fn vc1_keyframe_prefix_order_seq_unchanged_entry_redefined() {
    let sh = [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER, 0xAA, 0xBB, 0xCC];
    let ep_a = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x11, 0x22];
    let ep_b = vec![0x00, 0x00, 0x01, SC_ENTRY_POINT, 0x33, 0x44, 0x55];
    let frame = [0x00, 0x00, 0x01, SC_FRAME, 0x77];

    let mut parser = Vc1Parser::new();

    // AU1: seed codecPrivate (sh + ep_a, both first → stripped, then
    // re-asserted as active at keyframe in seq-then-entry order).
    let au1: Vec<u8> = sh
        .iter()
        .chain(ep_a.iter())
        .chain(frame.iter())
        .cloned()
        .collect();
    parser.parse(&make_pes(au1, Some(0)));

    // AU2: keyframe, seq_header UNCHANGED, entry_point REDEFINED to B. Bug
    // trigger: scan emits ep_b but strips sh, so the keyframe reassert must
    // prepend sh BEFORE ep_b, not after.
    let au2: Vec<u8> = sh
        .iter()
        .chain(ep_b.iter())
        .chain(frame.iter())
        .cloned()
        .collect();
    let f2 = parser.parse(&make_pes(au2, Some(90000)));
    assert_eq!(f2.len(), 1, "AU2 must emit a frame");

    // Find positions of seq_header and entry_point start codes in the output.
    let data = &f2[0].data;
    let seq_pos = data
        .windows(4)
        .position(|w| w == [0x00, 0x00, 0x01, SC_SEQUENCE_HEADER]);
    let ep_pos = data
        .windows(4)
        .position(|w| w == [0x00, 0x00, 0x01, SC_ENTRY_POINT]);
    assert!(
        seq_pos.is_some(),
        "seq_header must be present in the keyframe prefix"
    );
    assert!(
        ep_pos.is_some(),
        "entry_point must be present in the keyframe prefix"
    );
    assert!(
        seq_pos.unwrap() < ep_pos.unwrap(),
        "seq_header (pos {}) must precede entry_point (pos {}) — SMPTE 421M order",
        seq_pos.unwrap(),
        ep_pos.unwrap()
    );
    // The redefined entry_point body (ep_b) must appear, not the old ep_a.
    assert!(
        data.windows(ep_b.len()).any(|w| w == ep_b),
        "redefined ep_b must be present"
    );
    assert!(
        !data.windows(ep_a.len()).any(|w| w == ep_a),
        "stale ep_a must not be present"
    );
}

/// Helper: does `data` contain a start-code unit with the given type byte?
fn contains_sc(data: &[u8], sc_type: u8) -> bool {
    data.windows(4)
        .any(|w| w[0] == 0x00 && w[1] == 0x00 && w[2] == 0x01 && w[3] == sc_type)
}
