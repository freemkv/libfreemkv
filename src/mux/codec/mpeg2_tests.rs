use super::*;
use crate::mux::ts::PesPacket;

/// Build a picture coding extension (`00 00 01 B5`, ext-id 1000) carrying the
/// given pulldown flags, for parser tests.
fn pic_coding_ext(tff: u8, rff: u8, progressive_frame: u8, frame_picture: bool) -> Vec<u8> {
    let e0 = 0x80; // ext-id 1000, f_code high nibble 0
    let e1 = 0x00;
    let e2 = if frame_picture { 0x03 } else { 0x01 }; // picture_structure bits 1-0
    let e3 = (tff << 7) | (rff << 1);
    let e4 = progressive_frame << 7;
    vec![0x00, 0x00, 0x01, SEQ_EXT_CODE, e0, e1, e2, e3, e4]
}

#[test]
fn field_picture_order_follows_picture_structure() {
    use crate::mux::codec::coding::FieldOrder;
    let mk = |e2: u8| {
        let mut ext = pic_coding_ext(0, 0, 0, false);
        ext[6] = e2;
        let (tff, rff, pf, fp) = picture_coding_flags(&ext);
        PictureInfo::mpeg2(
            CodingType::I,
            Mpeg2Coding {
                top_field_first: tff,
                repeat_first_field: rff,
                progressive_frame: pf,
                progressive_sequence: false,
                frame_picture: fp,
            },
        )
        .field_order()
    };
    assert_eq!(mk(0x01), Some(FieldOrder::Tff));
    assert_eq!(mk(0x02), Some(FieldOrder::Bff));
}

#[test]
fn field_pair_second_field_inherits_first_order() {
    use crate::mux::codec::coding::FieldOrder;
    let mut p = Mpeg2Parser::new();
    let mut frames = Vec::new();
    let mut first = true;
    for (ct, e2) in [(1u8, 0x01u8), (2, 0x02), (1, 0x01), (2, 0x02)] {
        let mut au = Vec::new();
        if first {
            au.extend_from_slice(&make_seq_header(720, 576, 3, 3));
            first = false;
        }
        au.extend_from_slice(&make_picture_header(ct));
        let mut ext = pic_coding_ext(0, 0, 0, false);
        ext[6] = e2;
        au.extend_from_slice(&ext);
        frames.extend(p.parse(&PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: None,
            data: au,
            discontinuity: false,
        }));
    }
    frames.extend(p.flush());
    assert_eq!(frames.len(), 4);
    for f in &frames {
        assert_eq!(f.coding.unwrap().field_order(), Some(FieldOrder::Tff));
    }
}

#[test]
fn a_second_top_field_starts_a_new_pair_instead_of_partnering_the_first() {
    use crate::mux::codec::coding::FieldOrder;
    let mut p = Mpeg2Parser::new();
    let mut frames = Vec::new();
    // top, top, bottom: the bottom field pairs with the SECOND top field.
    for (i, (ct, e2)) in [(1u8, 0x01u8), (2, 0x01), (2, 0x02)]
        .into_iter()
        .enumerate()
    {
        let mut au = Vec::new();
        if i == 0 {
            au.extend_from_slice(&make_seq_header(720, 576, 3, 3));
        }
        au.extend_from_slice(&make_picture_header(ct));
        let mut ext = pic_coding_ext(0, 0, 0, false);
        ext[6] = e2;
        au.extend_from_slice(&ext);
        frames.extend(p.parse(&make_pes(au, None)));
    }
    frames.extend(p.flush());
    assert_eq!(frames.len(), 3);
    for f in &frames {
        assert_eq!(f.coding.unwrap().field_order(), Some(FieldOrder::Tff));
    }
}

#[test]
fn a_pts_jump_in_a_later_gop_relocks_the_timeline_origin() {
    // 25 fps = 40 ms. GOP 1 anchors at 0; GOP 2 carries a PES PTS of 10 s
    // (a clip boundary), so its frames follow it, not GOP 1's extrapolation.
    let mut p = Mpeg2Parser::new();
    let mut g1 = make_seq_header(720, 480, 3, 3);
    g1.extend_from_slice(&gop());
    g1.extend_from_slice(&make_picture_header_tr(1, 0));
    g1.extend_from_slice(&[0xAA; 10]);
    g1.extend_from_slice(&make_picture_header_tr(2, 1));
    g1.extend_from_slice(&[0xBB; 10]);
    let mut frames = p.parse(&make_pes(g1, Some(0)));
    let mut g2 = gop();
    g2.extend_from_slice(&make_picture_header_tr(1, 0));
    g2.extend_from_slice(&[0xCC; 10]);
    frames.extend(p.parse(&make_pes(g2, Some(10 * 90_000))));
    frames.extend(p.flush());
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[1].pts_ns, 40_000_000);
    assert_eq!(frames[2].pts_ns, 10_000_000_000);
}

#[test]
fn gop_header_resets_stale_lone_field() {
    use crate::mux::codec::coding::FieldOrder;
    let mut p = Mpeg2Parser::new();
    let mut frames = Vec::new();
    let feed = |p: &mut Mpeg2Parser, prefix: Vec<u8>, ct: u8, e2: u8| {
        let mut au = prefix;
        au.extend_from_slice(&make_picture_header(ct));
        let mut ext = pic_coding_ext(0, 0, 0, false);
        ext[6] = e2;
        au.extend_from_slice(&ext);
        p.parse(&PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: None,
            data: au,
            discontinuity: false,
        })
    };
    // Stray lone top field, then a new GOP coded BFF (bottom, top) x2.
    frames.extend(feed(&mut p, make_seq_header(720, 576, 3, 3), 1, 0x01));
    let gop = vec![0x00, 0x00, 0x01, GOP_CODE, 0, 0, 0, 0];
    frames.extend(feed(&mut p, gop, 1, 0x02));
    frames.extend(feed(&mut p, Vec::new(), 2, 0x01));
    frames.extend(feed(&mut p, Vec::new(), 2, 0x02));
    frames.extend(feed(&mut p, Vec::new(), 2, 0x01));
    frames.extend(p.flush());
    assert_eq!(frames.len(), 5);
    for f in &frames[1..] {
        assert_eq!(f.coding.unwrap().field_order(), Some(FieldOrder::Bff));
    }
}

#[test]
fn parser_populates_full_pictureinfo_and_source() {
    use crate::mux::codec::coding::FieldOrder;
    // Drive the REAL parser over I/P/B pictures exercising every PictureInfo
    // facet, each in its OWN PES with its OWN source stamp (realistic DVD
    // shape), asserting byte-exact provenance rides every frame.
    let mk_pes = |data: Vec<u8>, byte: u64| PesPacket {
        source: Some(crate::pes::SourcePos::at_byte(byte)),
        pid: 0x1011,
        pts: None,
        dts: None,
        data,
        discontinuity: false,
    };
    let mut p = Mpeg2Parser::new();
    let mut frames = Vec::new();
    // I-picture (with the seq header) @ source byte 0.
    let mut au = make_seq_header(720, 576, 3, 3); // interlaced 16:9 25fps
    au.extend_from_slice(&make_picture_header(1));
    au.extend_from_slice(&pic_coding_ext(1, 0, 0, true));
    frames.extend(p.parse(&mk_pes(au, 0)));
    // P-picture @ source byte 2048.
    let mut au = make_picture_header(2);
    au.extend_from_slice(&pic_coding_ext(0, 0, 0, true));
    frames.extend(p.parse(&mk_pes(au, 2048)));
    // B-picture @ source byte 4096.
    let mut au = make_picture_header(3);
    au.extend_from_slice(&pic_coding_ext(0, 1, 1, true));
    frames.extend(p.parse(&mk_pes(au, 4096)));
    frames.extend(p.flush());
    assert_eq!(frames.len(), 3, "three pictures → three frames");

    // Every frame carries PictureInfo and the SourcePos its PES stamped.
    for f in &frames {
        assert!(f.coding.is_some(), "every MPEG-2 frame carries PictureInfo");
        assert!(
            f.source.is_some(),
            "every frame carries SourcePos provenance"
        );
    }
    let frame = |t: CodingType| {
        frames
            .iter()
            .find(|f| f.coding.unwrap().coding_type() == t)
            .unwrap_or_else(|| panic!("no {t:?} frame"))
    };

    let i = frame(CodingType::I);
    assert_eq!(
        i.source.unwrap().byte,
        0,
        "I frame keeps its PES source @ 0"
    );
    let ic = i.coding.unwrap();
    assert!(ic.keyframe(), "I picture is a keyframe");
    assert_eq!(ic.field_order(), Some(FieldOrder::Tff), "tff=1 → TFF");
    assert_eq!(ic.nb_fields(), 2, "normal interlaced frame = 2 fields");
    assert_eq!(ic.progressive(), Some(false));

    let pp = frame(CodingType::P);
    assert_eq!(
        pp.source.unwrap().byte,
        2048,
        "P frame keeps its PES source"
    );
    let pc = pp.coding.unwrap();
    assert!(!pc.keyframe());
    assert_eq!(
        pc.field_order(),
        Some(FieldOrder::Bff),
        "tff=0 interlaced frame → BFF (the red-flag fix)"
    );
    assert_eq!(pc.nb_fields(), 2);

    let b = frame(CodingType::B);
    assert_eq!(b.source.unwrap().byte, 4096, "B frame keeps its PES source");
    let bc = b.coding.unwrap();
    assert!(!bc.keyframe());
    assert_eq!(
        bc.field_order(),
        Some(FieldOrder::Progressive),
        "progressive_frame → Progressive (no field order)"
    );
    assert_eq!(
        bc.nb_fields(),
        3,
        "rff + progressive_frame in interlaced seq → 2:3 pulldown = 3 fields"
    );
    assert_eq!(bc.progressive(), Some(true));
}

#[test]
fn progressive_sequence_parsed_from_seq_ext() {
    // Sequence extension: 00 00 01 B5, e0 ext-id 0001 (0x1_), e1 bit3 = progressive_sequence.
    assert!(parse_progressive_sequence(&[
        0,
        0,
        1,
        SEQ_EXT_CODE,
        0x10,
        0x08
    ]));
    assert!(!parse_progressive_sequence(&[
        0,
        0,
        1,
        SEQ_EXT_CODE,
        0x10,
        0x00
    ]));
    // No sequence extension at all → interlaced default (false).
    assert!(!parse_progressive_sequence(&[
        0,
        0,
        1,
        SEQ_HEADER_CODE,
        0,
        0
    ]));
}

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

/// Build a minimal MPEG-2 sequence header.
/// 00 00 01 B3 [h_size:12][v_size:12] [aspect:4][frame_rate:4] ...
fn make_seq_header(width: u16, height: u16, aspect: u8, frame_rate: u8) -> Vec<u8> {
    let mut hdr = vec![0x00, 0x00, 0x01, SEQ_HEADER_CODE];
    hdr.push((width >> 4) as u8);
    hdr.push(((width & 0x0F) as u8) << 4 | ((height >> 8) & 0x0F) as u8);
    hdr.push((height & 0xFF) as u8);
    hdr.push((aspect << 4) | (frame_rate & 0x0F));
    // Bit rate (18 bits) + marker + VBV buffer size (10 bits) etc — pad minimally.
    hdr.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x00]);
    hdr
}

/// Build a picture header with the given coding type.
fn make_picture_header(coding_type: u8) -> Vec<u8> {
    // 00 00 01 00 [temporal_ref:10][picture_coding_type:3][...]
    let byte5 = (coding_type & 0x07) << 3;
    vec![0x00, 0x00, 0x01, PICTURE_CODE, 0x00, byte5, 0x00, 0x00]
}

/// A GOP header start code (used as a clean access-unit delimiter in tests).
fn gop() -> Vec<u8> {
    vec![0x00, 0x00, 0x01, GOP_CODE, 0x00, 0x00, 0x00, 0x00]
}

/// Picture header carrying an explicit 10-bit temporal_reference.
fn make_picture_header_tr(coding_type: u8, tr: u16) -> Vec<u8> {
    let b4 = ((tr >> 2) & 0xFF) as u8;
    let b5 = (((tr & 0x03) as u8) << 6) | ((coding_type & 0x07) << 3);
    vec![0x00, 0x00, 0x01, PICTURE_CODE, b4, b5, 0x00, 0x00]
}

/// Collect every frame from a single PES followed by an EOF flush — the
/// common single-picture test shape (the final AU emits on flush()).
fn parse_then_flush(parser: &mut Mpeg2Parser, pes: &PesPacket) -> Vec<Frame> {
    let mut frames = parser.parse(pes);
    frames.extend(parser.flush());
    frames
}

/// Parse an opening I-picture, then `pes`; returns only `pes`'s frames. A
/// stream emits nothing before its first I-picture.
fn parse_after_i(parser: &mut Mpeg2Parser, pes: &PesPacket) -> Vec<Frame> {
    let mut i = make_picture_header(PICTURE_TYPE_I);
    i.extend_from_slice(&[0xEE; 8]);
    let mut frames = parser.parse(&make_pes(i, None));
    frames.extend(parse_then_flush(parser, pes));
    assert!(frames[0].keyframe, "the opening I-picture");
    frames.split_off(1)
}

// --- Pictures a decoder cannot reconstruct from the stream's own data ---

/// A GOP header with the given `closed_gop` / `broken_link` flags.
fn gop_flags(closed: bool, broken_link: bool) -> Vec<u8> {
    let b7 = (u8::from(closed) << 6) | (u8::from(broken_link) << 5);
    vec![0x00, 0x00, 0x01, GOP_CODE, 0x00, 0x00, 0x00, b7]
}

/// One frame picture as its own PES: `prefix` headers, a picture header,
/// a payload.
fn pic_pes(prefix: Vec<u8>, ct: u8, tr: u16, pts: Option<i64>) -> PesPacket {
    let mut au = prefix;
    au.extend_from_slice(&make_picture_header_tr(ct, tr));
    au.extend_from_slice(&[0xA5; 12]);
    make_pes(au, pts)
}

/// A 25 fps GOP in decode order `I2 B0 B1 P5 B3 B4`, the I at `pts` (90 kHz).
fn gop_ibbp(head: Vec<u8>, pts: Option<i64>) -> Vec<PesPacket> {
    vec![
        pic_pes(head, 1, 2, pts),
        pic_pes(Vec::new(), 3, 0, None),
        pic_pes(Vec::new(), 3, 1, None),
        pic_pes(Vec::new(), 2, 5, None),
        pic_pes(Vec::new(), 3, 3, None),
        pic_pes(Vec::new(), 3, 4, None),
    ]
}

fn run(pes: impl IntoIterator<Item = PesPacket>) -> Vec<Frame> {
    let mut p = Mpeg2Parser::new();
    let mut frames: Vec<Frame> = pes.into_iter().flat_map(|x| p.parse(&x)).collect();
    frames.extend(p.flush());
    frames
}

fn pts_ms(frames: &[Frame]) -> Vec<i64> {
    frames.iter().map(|f| f.pts_ns / 1_000_000).collect()
}

fn seq_gop(closed: bool, broken_link: bool) -> Vec<u8> {
    let mut h = make_seq_header(720, 576, 3, 3);
    h.extend_from_slice(&gop_flags(closed, broken_link));
    h
}

#[test]
fn an_open_gop_at_the_stream_start_drops_its_leading_b_pictures() {
    // B0 and B1 reference the anchor before this GOP, which the stream does
    // not hold. The second open GOP's leading Bs reference P5, which it does.
    let mut pes = gop_ibbp(seq_gop(false, false), Some(90_000));
    pes.extend(gop_ibbp(gop_flags(false, false), None));
    let f = run(pes);
    assert!(f[0].keyframe, "the first emitted picture is the I-picture");
    assert_eq!(
        pts_ms(&f),
        vec![1000, 1120, 1040, 1080, 1240, 1160, 1200, 1360, 1280, 1320],
        "I2 keeps its PES PTS; every kept frame keeps its display slot"
    );
    assert_eq!(f[0].duration_ns, Some(40_000_000));
}

#[test]
fn a_closed_gop_at_the_stream_start_keeps_every_picture() {
    let f = run(gop_ibbp(seq_gop(true, false), Some(90_000)));
    assert_eq!(pts_ms(&f), vec![1000, 920, 960, 1120, 1040, 1080]);
}

#[test]
fn pictures_before_the_first_i_picture_are_dropped() {
    let pes = vec![
        pic_pes(make_seq_header(720, 576, 3, 3), 2, 1, Some(90_000)),
        pic_pes(Vec::new(), 3, 0, None),
    ];
    let mut all = pes;
    all.extend(gop_ibbp(gop_flags(true, false), None));
    let f = run(all);
    assert_eq!(f.len(), 6, "only the closed GOP is emitted");
    assert!(f[0].keyframe);
    assert_eq!(
        f[0].pts_ns, 1_120_000_000,
        "the I keeps its slot after the dropped two"
    );
}

#[test]
fn a_broken_link_drops_the_leading_b_pictures_mid_stream() {
    let mut pes = gop_ibbp(seq_gop(true, false), Some(90_000));
    pes.extend(gop_ibbp(gop_flags(false, true), None));
    let f = run(pes);
    assert_eq!(f.len(), 10);
    assert_eq!(pts_ms(&f)[6..], [1240, 1360, 1280, 1320]);
}

#[test]
fn a_join_drops_the_leading_b_pictures_of_an_open_gop() {
    // The second GOP's PTS does not continue the first: a different clip, so
    // its leading Bs' forward reference is not the stream's last anchor.
    let mut pes = gop_ibbp(seq_gop(true, false), Some(90_000));
    pes.extend(gop_ibbp(gop_flags(false, false), Some(10 * 90_000)));
    let f = run(pes);
    assert_eq!(pts_ms(&f)[6..], [10_000, 10_120, 10_040, 10_080]);
}

#[test]
fn pes_opened_mid_picture_is_not_a_join() {
    // A muxer packing pictures back to back opens each PES
    // inside the previous picture, timed for the picture commencing in it. Read as
    // the previous picture's PTS, the open GOP's origin moved a frame: a false join.
    let mut pics = vec![
        pic_pes(seq_gop(true, false), 1, 0, None),
        pic_pes(Vec::new(), 2, 3, None),
        pic_pes(Vec::new(), 3, 1, None),
        pic_pes(Vec::new(), 3, 2, None),
    ];
    pics.extend(gop_ibbp(gop_flags(false, false), None));
    // Display slot of each picture, in decode order.
    let slots: Vec<i64> = [0, 3, 1, 2, 6, 4, 5, 9, 7, 8]
        .iter()
        .map(|&s| 90_000 + s * 3_600)
        .collect();
    let starts: Vec<usize> = pics
        .iter()
        .scan(0, |at, p| Some(std::mem::replace(at, *at + p.data.len())))
        .collect();
    let es: Vec<u8> = pics.into_iter().flat_map(|p| p.data).collect();
    // PES j opens 5 bytes into picture j - 1, so picture j commences inside it.
    let mut cuts: Vec<usize> = std::iter::once(0)
        .chain(starts.iter().map(|s| s + 5))
        .collect();
    cuts.push(es.len());
    let pes = cuts
        .windows(2)
        .enumerate()
        .map(|(j, w)| make_pes(es[w[0]..w[1]].to_vec(), slots.get(j).copied()));
    let f = run(pes);
    assert_eq!(
        pts_ms(&f),
        slots.iter().map(|s| s / 90).collect::<Vec<_>>(),
        "every picture kept, each in its display slot"
    );
}

#[test]
fn a_dropped_picture_passes_its_discontinuity_to_the_next_emitted_frame() {
    let mut pes = gop_ibbp(seq_gop(false, false), Some(90_000));
    pes[1].discontinuity = true;
    let f = run(pes);
    assert_eq!(f.len(), 4);
    assert!(!f[0].discontinuity, "the I precedes the dropped B");
    assert!(f[1].discontinuity, "P5 is the next emitted frame");
}

#[test]
fn a_field_coded_open_gop_drops_both_fields_of_each_leading_b() {
    // I/P field pair = one anchor frame, so the leading B field pairs still lack
    // their forward reference.
    let field = |head: Vec<u8>, ct: u8, tr: u16, e2: u8, pts: Option<i64>| {
        let mut au = head;
        au.extend_from_slice(&make_picture_header_tr(ct, tr));
        let mut ext = pic_coding_ext(0, 0, 0, false);
        ext[6] = e2;
        au.extend_from_slice(&ext);
        make_pes(au, pts)
    };
    let f = run(vec![
        field(seq_gop(false, false), 1, 1, 0x01, Some(90_000)),
        field(Vec::new(), 2, 1, 0x02, None),
        field(Vec::new(), 3, 0, 0x01, None),
        field(Vec::new(), 3, 0, 0x02, None),
        field(Vec::new(), 2, 2, 0x01, None),
        field(Vec::new(), 2, 2, 0x02, None),
    ]);
    let types: Vec<_> = f.iter().map(|x| x.coding.unwrap().coding_type()).collect();
    assert_eq!(
        types,
        [CodingType::I, CodingType::P, CodingType::P, CodingType::P]
    );
}

#[test]
fn a_gap_resync_onto_an_open_gop_emits_no_leading_b_pictures() {
    // GOP 1 (closed) loses data before P5: P5 and the Bs after it reference
    // the lost picture. GOP 2 is open and PTS-continuous (no join): its leading
    // B0/B1 reference GOP 1's P5, which the gate dropped.
    let mut pes = gop_ibbp(seq_gop(true, false), Some(90_000));
    pes[3].discontinuity = true;
    pes.extend(gop_ibbp(gop_flags(false, false), None));
    let mut gate = crate::mux::resync::ResyncGate::new();
    let out: Vec<Frame> = run(pes)
        .into_iter()
        .filter(|f| gate.admit(true, f.discontinuity, f.keyframe))
        .collect();
    let types: Vec<_> = out
        .iter()
        .map(|f| f.coding.unwrap().coding_type())
        .collect();
    // GOP 1: I2 B0 B1 kept; P5 B3 B4 dropped. GOP 2: I2 kept, B0 B1 lack P5
    // (dropped), P5 B3 B4 decode from GOP 2's own anchors.
    assert_eq!(
        types,
        [
            CodingType::I,
            CodingType::B,
            CodingType::B,
            CodingType::I,
            CodingType::P,
            CodingType::B,
            CodingType::B,
        ],
        "no emitted picture may reference one the gate dropped"
    );
}

// --- Sequence header parsing ---

#[test]
fn parse_sequence_header_resolution() {
    assert_eq!(
        parse_resolution(&make_seq_header(720, 480, 2, 4)),
        Some((720, 480))
    );
}

#[test]
fn parse_sequence_header_1920x1080() {
    assert_eq!(
        parse_resolution(&make_seq_header(1920, 1080, 3, 4)),
        Some((1920, 1080))
    );
}

#[test]
fn parse_sequence_header_frame_rate() {
    let hdr = make_seq_header(720, 480, 2, 4); // frame_rate_code 4 = 29.97
    assert_eq!(parse_frame_rate(&hdr), Some((30000, 1001)));
}

#[test]
fn parse_sequence_header_aspect_ratio() {
    let hdr = make_seq_header(720, 480, 3, 4); // aspect code 3 = 16:9
    assert_eq!(parse_aspect_ratio(&hdr), Some((16, 9)));
}

#[test]
fn parse_sequence_header_too_short() {
    let hdr = vec![0x00, 0x00, 0x01, SEQ_HEADER_CODE];
    assert!(parse_resolution(&hdr).is_none());
    assert!(parse_frame_rate(&hdr).is_none());
    assert!(parse_aspect_ratio(&hdr).is_none());
}

// --- I-frame detection ---

#[test]
fn detect_i_frame() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_picture_header(PICTURE_TYPE_I);
    data.extend_from_slice(&[0xFF; 16]);
    let frames = parse_then_flush(&mut parser, &make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 1);
    assert!(frames[0].keyframe, "I-frame should be detected as keyframe");
}

#[test]
fn detect_p_frame_not_keyframe() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_picture_header(2); // P-frame
    data.extend_from_slice(&[0xFF; 16]);
    let frames = parse_after_i(&mut parser, &make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 1);
    assert!(!frames[0].keyframe, "P-frame should not be keyframe");
}

#[test]
fn detect_b_frame_not_keyframe() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_picture_header(3); // B-frame
    data.extend_from_slice(&[0xFF; 16]);
    let frames = parse_after_i(&mut parser, &make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 1);
    assert!(!frames[0].keyframe, "B-frame should not be keyframe");
}

// --- The core fix: a picture split across many PES packets is ONE frame ---

#[test]
fn picture_fragmented_across_pes_is_reassembled_into_one_frame() {
    // A DVD coded picture spans multiple ~2 KB PES packets; only the first
    // carries a PTS. The parser must concatenate them into ONE access unit,
    // not emit one fragment per PES.
    let mut parser = Mpeg2Parser::new();

    let mut au = make_seq_header(720, 480, 3, 4);
    au.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    au.extend_from_slice(&vec![0xAA; 5000]); // slice data (no start codes)

    // Split the AU into 2 KB fragments across separate PES packets.
    let mut frames = Vec::new();
    for (i, chunk) in au.chunks(2000).enumerate() {
        let pts = if i == 0 { Some(90000) } else { None };
        frames.extend(parser.parse(&make_pes(chunk.to_vec(), pts)));
    }
    // No boundary yet → nothing emitted during parse().
    assert!(frames.is_empty(), "incomplete AU must not emit fragments");
    // Flush completes the trailing AU.
    frames.extend(parser.flush());

    assert_eq!(frames.len(), 1, "fragments reassembled into ONE frame");
    assert_eq!(frames[0].data, au, "frame is the whole picture, byte-exact");
    assert!(frames[0].keyframe);
    assert_eq!(
        frames[0].pts_ns, 1_000_000_000,
        "PTS from the first fragment"
    );
}

#[test]
fn two_pictures_in_one_gop_emit_both_on_flush() {
    // Two pictures with no GOP/sequence boundary between them are ONE GOP; the
    // VFR timeline needs the whole GOP (a P-frame's PTS depends on later
    // B-frames), so they buffer until close/EOF, then emit in DECODE order.
    let mut parser = Mpeg2Parser::new();

    let mut pic1 = make_picture_header(PICTURE_TYPE_I);
    pic1.extend_from_slice(&[0x11; 100]);
    let mut pic2 = make_picture_header(2); // P
    pic2.extend_from_slice(&[0x22; 100]);

    let mut stream = pic1.clone();
    stream.extend_from_slice(&pic2);

    let frames = parser.parse(&make_pes(stream, Some(0)));
    assert!(frames.is_empty(), "same GOP — buffered until flush");

    let frames = parser.flush();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, pic1);
    assert!(frames[0].keyframe);
    assert_eq!(frames[1].data, pic2);
    assert!(!frames[1].keyframe);
}

// B1 hole-2 regression: the concealed gap must land on the post-gap picture, not the
// previous one.
#[test]
fn discontinuity_offset_mark_stamps_post_gap_picture_not_previous() {
    let mut parser = Mpeg2Parser::new();
    let mut pic1 = make_picture_header(PICTURE_TYPE_I);
    pic1.extend_from_slice(&[0x11; 100]);
    let mut pic2 = make_picture_header(2); // P
    pic2.extend_from_slice(&[0x22; 100]);

    // pic1 on a clean PES; nothing emits (same GOP, buffered).
    assert!(parser.parse(&make_pes(pic1.clone(), Some(0))).is_empty());
    // pic2 on a PES flagged discontinuity (a concealed gap preceded it).
    // parse() of this PES completes pic1's AU (the PREVIOUS picture) — which
    // must stay clean — while pic2 keeps buffering.
    let pes2 = PesPacket {
        source: None,
        pid: 0x1011,
        pts: Some(90000),
        dts: None,
        data: pic2.clone(),
        discontinuity: true,
    };
    assert!(parser.parse(&pes2).is_empty(), "same GOP — still buffered");

    let frames = parser.flush();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, pic1);
    assert!(
        !frames[0].discontinuity,
        "the previous (pre-gap) I picture must NOT be flagged"
    );
    assert_eq!(frames[1].data, pic2);
    assert!(
        frames[1].discontinuity,
        "the post-gap P picture (the discontinuity PES's own AU) IS flagged"
    );
}

#[test]
fn picture_coding_extension_stays_with_its_picture() {
    // Regression for `ignoring pic cod ext after 0`: the picture coding
    // extension (00 00 01 B5) must remain in the SAME access unit as its
    // picture header, never split into the next block.
    let mut parser = Mpeg2Parser::new();

    let mut au = make_picture_header(PICTURE_TYPE_I);
    au.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x88, 0x00]); // pic coding ext
    au.extend_from_slice(&[0x00, 0x00, 0x01, 0x01]); // slice
    au.extend_from_slice(&[0x77; 50]);

    let frames = parse_then_flush(&mut parser, &make_pes(au.clone(), Some(0)));
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].data, au,
        "picture + coding extension + slice = one AU"
    );
}

// --- PTS association across fragments ---

#[test]
fn each_picture_gets_the_pts_of_the_pes_that_began_it() {
    // With no sequence header (no frame rate) the parser falls back to each
    // AU's own PES PTS. Both pictures are one GOP → emitted on flush in
    // decode order, each carrying the PTS of the PES that began it.
    let mut parser = Mpeg2Parser::new();

    let mut pic1 = make_picture_header(PICTURE_TYPE_I);
    pic1.extend_from_slice(&[0x11; 50]);
    let frames1 = parser.parse(&make_pes(pic1, Some(90000)));
    assert!(frames1.is_empty(), "buffered until flush");

    let mut pic2 = make_picture_header(2);
    pic2.extend_from_slice(&[0x22; 50]);
    let frames2 = parser.parse(&make_pes(pic2, Some(180000)));
    assert!(frames2.is_empty(), "same GOP — still buffered");

    let frames = parser.flush();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].pts_ns, 1_000_000_000, "pic1 → PTS 90000");
    assert_eq!(frames[1].pts_ns, 2_000_000_000, "pic2 → PTS 180000");
}

// --- sparse PTS reconstructed from temporal_reference + frame rate ---

#[test]
fn sparse_pts_interpolated_by_temporal_reference() {
    // DVD stamps a PTS only ~once per VOBU; frames between marks must be
    // timed by temporal_reference × frame interval, anchored to the real
    // PES PTS so audio stays in sync. Frame rate code 3 = 25 fps = 40 ms.
    let mut p = Mpeg2Parser::new();

    // GOP 1: seq + gop + I(TR0) carrying PES PTS 0 (the anchor).
    let mut a = make_seq_header(720, 480, 3, 3);
    a.extend_from_slice(&gop());
    a.extend_from_slice(&make_picture_header_tr(1, 0));
    a.extend_from_slice(&[0xAA; 20]);
    let mut frames = p.parse(&make_pes(a, Some(0)));
    assert!(
        frames.is_empty(),
        "first AU waits for the next picture boundary"
    );

    // TR1, no PES PTS → interpolate.
    let mut b1 = make_picture_header_tr(3, 1);
    b1.extend_from_slice(&[0xBB; 20]);
    frames.extend(p.parse(&make_pes(b1, None)));

    // TR2, no PES PTS → interpolate.
    let mut b2 = make_picture_header_tr(3, 2);
    b2.extend_from_slice(&[0xCC; 20]);
    frames.extend(p.parse(&make_pes(b2, None)));

    frames.extend(p.flush());
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].pts_ns, 0, "anchor frame uses its real PES PTS");
    assert_eq!(frames[1].pts_ns, 40_000_000, "TR1 → +1 frame interval");
    assert_eq!(frames[2].pts_ns, 80_000_000, "TR2 → +2 frame intervals");
    assert_eq!(frames[0].duration_ns, Some(40_000_000));
}

/// A frame-picture AU with a picture coding extension carrying pulldown
/// flags (progressive_frame=1, so rff=1 → 3 fields), for VFR timing tests.
fn make_pulldown_picture(coding_type: u8, tr: u16, rff: u8) -> Vec<u8> {
    let mut au = make_picture_header_tr(coding_type, tr);
    // 00 00 01 B5 | e0 ext-id 1000 | e1 | e2 frame-pic | e3 rff<<1 | e4 prog_frame
    au.extend_from_slice(&[
        0x00,
        0x00,
        0x01,
        SEQ_EXT_CODE,
        0x80,
        0x00,
        0x03,
        rff << 1,
        0x80,
    ]);
    au.extend_from_slice(&[0xAA; 16]);
    au
}

#[test]
fn telecine_pts_accumulates_by_field_durations_not_a_fixed_grid() {
    // NTSC film 29.97: a 2:3 frame (rff=1) occupies 3 fields, 2:2 occupies 2.
    // PTS must accumulate by ACTUAL field durations so the next frame starts
    // exactly when this one ends — closing the fixed-grid gap that judders.
    let mut p = Mpeg2Parser::new();
    let field = 1_000_000_000i64 * 1001 / 30000 / 2;

    let mut a = make_seq_header(720, 480, 2, 4);
    a.extend_from_slice(&gop());
    a.extend(make_pulldown_picture(1, 0, 1)); // I tr0, 3 fields, PES anchor 0
    a.extend(make_pulldown_picture(2, 1, 0)); // P tr1, 2 fields
    let mut frames = p.parse(&make_pes(a, Some(0)));
    frames.extend(p.flush());

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].pts_ns, 0, "I anchored to PES PTS 0");
    assert_eq!(
        frames[0].duration_ns,
        Some(3 * field as u64),
        "I = 3 fields"
    );
    assert_eq!(
        frames[1].pts_ns,
        3 * field,
        "P starts exactly at I-end (3 fields), not the 1/29.97 grid"
    );
    assert_eq!(
        frames[1].duration_ns,
        Some(2 * field as u64),
        "P = 2 fields"
    );
    assert!(frames[1].pts_ns > frames[0].pts_ns, "strictly monotonic");
}

#[test]
fn b_frames_emit_in_decode_order_with_lower_display_pts() {
    // Decode order I(tr0) P(tr2) B(tr1): emitted in DECODE order, but the
    // B-frame carries a LOWER (earlier) display PTS than the P that precedes
    // it in the stream — never reordered (reordering corrupts the picture).
    let mut p = Mpeg2Parser::new();
    let field = 1_000_000_000i64 * 1001 / 30000 / 2;

    let mut a = make_seq_header(720, 480, 2, 4);
    a.extend_from_slice(&gop());
    a.extend(make_pulldown_picture(1, 0, 0)); // I tr0 (displays 1st), PES anchor 0
    a.extend(make_pulldown_picture(2, 2, 0)); // P tr2 (displays 3rd)
    a.extend(make_pulldown_picture(3, 1, 0)); // B tr1 (displays 2nd)
    let mut frames = p.parse(&make_pes(a, Some(0)));
    frames.extend(p.flush());

    assert_eq!(frames.len(), 3);
    assert!(frames[0].keyframe, "decode order preserved: I first");
    assert_eq!(frames[0].pts_ns, 0, "I (tr0) displays 1st");
    assert_eq!(frames[1].pts_ns, 4 * field, "P (tr2) displays 3rd");
    assert_eq!(frames[2].pts_ns, 2 * field, "B (tr1) displays 2nd");
    assert!(
        frames[2].pts_ns < frames[1].pts_ns,
        "B emitted AFTER P (decode order) but displays BEFORE it (lower PTS)"
    );
}

#[test]
fn temporal_reference_resets_each_gop_via_gop_base() {
    // Across a GOP boundary, temporal_reference restarts at 0 but the
    // whole-stream display index must keep climbing (gop_base folds the
    // previous GOP's frame count). 25 fps = 40 ms.
    let mut p = Mpeg2Parser::new();

    // GOP 1: two pictures TR0 (anchor PTS 0), TR1.
    let mut g1 = make_seq_header(720, 480, 3, 3);
    g1.extend_from_slice(&gop());
    g1.extend_from_slice(&make_picture_header_tr(1, 0));
    g1.extend_from_slice(&[0xAA; 10]);
    g1.extend_from_slice(&make_picture_header_tr(2, 1));
    g1.extend_from_slice(&[0xBB; 10]);
    let mut frames = p.parse(&make_pes(g1, Some(0)));

    // GOP 2: new GOP header, picture TR0 again (no PES PTS).
    let mut g2 = gop();
    g2.extend_from_slice(&make_picture_header_tr(1, 0));
    g2.extend_from_slice(&[0xCC; 10]);
    frames.extend(p.parse(&make_pes(g2, None)));
    frames.extend(p.flush());

    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].pts_ns, 0); // GOP1 TR0
    assert_eq!(frames[1].pts_ns, 40_000_000); // GOP1 TR1
    // GOP2 TR0 → display index 2 (gop_base 2 + TR 0), NOT a reset to 0.
    assert_eq!(
        frames[2].pts_ns, 80_000_000,
        "gop_base keeps the clock climbing"
    );
}

#[test]
fn leading_frames_buffered_until_first_pts_anchor() {
    // A DVD title can open with a still-frame sequence whose PTS lands a few
    // frames in. Leading frames must be held and anchored to that real
    // timeline, never zero-stamped. 25fps=40ms; PTS (2s) arrives on picture 3.
    let mut p = Mpeg2Parser::new();

    let mut a = make_seq_header(720, 480, 3, 3);
    a.extend_from_slice(&gop());
    a.extend_from_slice(&make_picture_header_tr(1, 0));
    a.extend_from_slice(&[0xAA; 20]);
    let mut f = p.parse(&make_pes(a, None)); // no PTS → buffered

    let mut b1 = make_picture_header_tr(3, 1);
    b1.extend_from_slice(&[0xBB; 20]);
    f.extend(p.parse(&make_pes(b1, None))); // no PTS → buffered

    let mut b2 = make_picture_header_tr(3, 2);
    b2.extend_from_slice(&[0xCC; 20]);
    f.extend(p.parse(&make_pes(b2, Some(180000)))); // PTS 2 s → anchor + backfill
    f.extend(p.flush());

    assert_eq!(f.len(), 3);
    // Anchored to the real disc timeline, NOT a 0 base.
    assert_eq!(
        f[0].pts_ns,
        2_000_000_000 - 80_000_000,
        "leading frame back-anchored"
    );
    assert_eq!(f[1].pts_ns, 2_000_000_000 - 40_000_000);
    assert_eq!(
        f[2].pts_ns, 2_000_000_000,
        "anchor frame = its real PES PTS"
    );
    // Decode order preserved.
    assert!(f[0].keyframe);
}

#[test]
fn opening_au_keeps_disc_pts_and_opening_seq_header_no_zero_floor() {
    // Opening-GOP regression: a DVD VOBU opens with a seq header + I-frame
    // stamped at its REAL non-zero PTS. Emit that PTS (never floored to 0)
    // and capture THAT opening seq header as codec_private.
    let mut p = Mpeg2Parser::new();

    // Opening AU: seq header (the codecPrivate) + GOP + I-frame TR0 carrying
    // the disc's real opening PTS (2 s here, i.e. NOT zero). 25 fps PAL.
    let mut a = make_seq_header(720, 576, 3, 3); // 16:9, 25 fps
    a.extend_from_slice(&gop());
    a.extend_from_slice(&make_picture_header_tr(PICTURE_TYPE_I, 0));
    a.extend_from_slice(&[0xAA; 20]);
    let mut frames = p.parse(&make_pes(a, Some(180_000))); // PTS = 2 s (90 kHz)
    assert!(frames.is_empty(), "first AU waits for the next boundary");

    // Second picture (no PTS) closes the opening AU: the I-frame emits and
    // the opening sequence header is captured (headers-ready timing — the
    // consumer reads codec_private once the first AU drains).
    let mut b = make_picture_header_tr(3, 1);
    b.extend_from_slice(&[0xBB; 20]);
    frames.extend(p.parse(&make_pes(b, None)));

    // codec_private is the OPENING sequence header (read at headers-ready,
    // before any later AU could replace it).
    let cp = p
        .codec_private()
        .expect("opening seq header captured at headers-ready");
    assert_eq!(
        &cp[..4],
        &[0x00, 0x00, 0x01, SEQ_HEADER_CODE],
        "codec_private is the opening sequence header"
    );
    assert_eq!(p.resolution(), Some((720, 576)), "576i opening header");
    assert_eq!(p.frame_rate(), Some((25, 1)), "25 fps opening header");

    frames.extend(p.flush());

    assert_eq!(frames.len(), 2);
    assert!(frames[0].keyframe, "opening picture is the I-frame");
    assert_eq!(
        frames[0].pts_ns, 2_000_000_000,
        "opening I-frame keeps the disc's real PTS (2 s), NOT floored to 0"
    );
    assert_eq!(
        frames[1].pts_ns, 2_040_000_000,
        "next frame is one 40 ms interval later on the real timeline"
    );
}

// --- Sequence header → codec_private ---

#[test]
fn codec_private_from_sequence_header() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_seq_header(720, 480, 3, 4);
    data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    data.extend_from_slice(&[0xFF; 8]);
    let _ = parse_then_flush(&mut parser, &make_pes(data, Some(0)));

    let cp = parser
        .codec_private()
        .expect("codec_private after seq header");
    assert_eq!(&cp[..4], &[0x00, 0x00, 0x01, SEQ_HEADER_CODE]);
}

#[test]
fn codec_private_none_initially() {
    assert!(Mpeg2Parser::new().codec_private().is_none());
}

#[test]
fn codec_private_includes_extension_but_not_picture() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_seq_header(1920, 1080, 3, 4);
    // Sequence extension: 00 00 01 B5 [ext data]
    data.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x14, 0x8A, 0x00, 0x01]);
    data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    data.extend_from_slice(&[0xFF; 4]);

    let _ = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
    let cp = parser.codec_private().unwrap();
    assert!(
        cp.windows(4).any(|w| w == [0x00, 0x00, 0x01, SEQ_EXT_CODE]),
        "codec_private should include the sequence extension"
    );
    // It must stop before the picture header — extradata is seq header only.
    assert!(
        !cp.windows(4).any(|w| w == [0x00, 0x00, 0x01, PICTURE_CODE]),
        "codec_private must NOT include the picture start code"
    );
}

// --- seq-header keyframe flag must not leak into a P/B-frame ---

#[test]
fn seq_header_then_p_frame_is_not_keyframe() {
    // A PES carrying a sequence header followed by a P-frame must NOT be a
    // keyframe — keyframe-ness belongs to the coded picture.
    let mut parser = Mpeg2Parser::new();
    let mut data = make_seq_header(720, 480, 3, 4);
    data.extend_from_slice(&make_picture_header(2)); // P-frame
    data.extend_from_slice(&[0xFF; 16]);
    let frames = parse_after_i(&mut parser, &make_pes(data, Some(0)));
    assert_eq!(frames.len(), 1);
    assert!(
        !frames[0].keyframe,
        "seq-header + P-frame must not be a keyframe"
    );
    assert!(parser.codec_private().is_some());
}

#[test]
fn sequence_header_with_picture_is_keyframe() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_seq_header(720, 480, 3, 4);
    data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    data.extend_from_slice(&[0xFF; 16]);
    let frames = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
    assert_eq!(frames.len(), 1);
    assert!(frames[0].keyframe);
    assert!(parser.codec_private().is_some());
}

// --- a SECOND sequence header re-captures (title boundary) ---

#[test]
fn new_sequence_header_replaces_codec_private() {
    let mut parser = Mpeg2Parser::new();

    // AU A: 1920x1080 seq header + I picture, delimited by a following GOP.
    let mut a = make_seq_header(1920, 1080, 3, 4);
    a.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    a.extend_from_slice(&[0xAA; 20]);
    a.extend_from_slice(&gop()); // trailing GOP header starts the next GOP
    let _fa = parser.parse(&make_pes(a, Some(0)));
    // Header A is captured during parse (codec_private) even though its GOP
    // only emits once header B's picture closes it / on flush.
    assert_eq!(parser.resolution(), Some((1920, 1080)));

    // AU B: a NEW 720x480 seq header + I picture. Its extension/header must
    // replace the stored one rather than keeping stale 1920x1080.
    let mut b = make_seq_header(720, 480, 2, 4);
    b.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    b.extend_from_slice(&[0xBB; 20]);
    let _ = parse_then_flush(&mut parser, &make_pes(b, Some(3600)));
    assert_eq!(
        parser.resolution(),
        Some((720, 480)),
        "codec_private updated to header B"
    );
}

// --- PTS conversion ---

#[test]
fn pts_conversion_to_nanoseconds() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_picture_header(PICTURE_TYPE_I);
    data.extend_from_slice(&[0xFF; 4]);
    let frames = parse_then_flush(&mut parser, &make_pes(data, Some(90000)));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

#[test]
fn mpeg2_dts_fallback_and_zero() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_picture_header(PICTURE_TYPE_I);
    data.extend_from_slice(&[0xFF; 4]);
    let pes = PesPacket {
        source: None,
        pid: 0x1011,
        pts: None,
        dts: Some(90000),
        data,
        discontinuity: false,
    };
    let f = parse_then_flush(&mut parser, &pes);
    assert_eq!(f[0].pts_ns, 1_000_000_000, "DTS fallback");

    let mut parser2 = Mpeg2Parser::new();
    let mut data2 = make_picture_header(PICTURE_TYPE_I);
    data2.extend_from_slice(&[0xFF; 4]);
    let pes2 = PesPacket {
        source: None,
        pid: 0x1011,
        pts: None,
        dts: None,
        data: data2,
        discontinuity: false,
    };
    let f2 = parse_then_flush(&mut parser2, &pes2);
    assert_eq!(f2[0].pts_ns, 0, "no PTS/DTS → 0");
}

// --- Empty PES ---

#[test]
fn empty_pes_no_frames() {
    let mut parser = Mpeg2Parser::new();
    assert!(parser.parse(&make_pes(Vec::new(), Some(0))).is_empty());
}

// --- parameter-set-only stream: seq header, no picture → no frame ---

#[test]
fn sequence_header_only_emits_no_frame_and_captures_no_codec_private() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_seq_header(1920, 1080, 3, 4);
    data.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x14, 0x8A]);
    // No picture start code at all.
    let frames = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
    assert!(frames.is_empty(), "no coded picture → no frame");
    // A header-only AU returns before extract_seq_header: nothing is captured.
    assert!(parser.codec_private().is_none());
}

// --- buffer cap: corrupt stream with no second boundary is force-flushed ---

#[test]
fn oversized_au_without_boundary_is_force_flushed() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_picture_header(PICTURE_TYPE_I);
    // > MAX_AU_BUFFER of slice bytes with no following picture/seq/GOP.
    data.extend(std::iter::repeat_n(0xAA, MAX_AU_BUFFER + 1024));
    // The AU assembler force-completes the ~8 MiB AU (no boundary), and the
    // GOP byte cap (MAX_PENDING_BYTES) then force-flushes that oversized GOP
    // during parse rather than buffering it unbounded.
    let mut frames = parser.parse(&make_pes(data, Some(0)));
    frames.extend(parser.flush());
    assert_eq!(frames.len(), 1, "over-cap AU force-flushed, not dropped");
    assert!(frames[0].keyframe);
}

// Frame-count cap: a run of pictures with no GOP/sequence boundary is force-flushed
// during parse at MAX_PENDING_FRAMES, not held until EOF.
#[test]
fn gop_frame_cap_flushes_during_parse() {
    let mut parser = Mpeg2Parser::new();
    let mut data = Vec::new();
    for _ in 0..MAX_PENDING_FRAMES + 100 {
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xAA; 4]);
    }
    let out = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(out.len(), MAX_PENDING_FRAMES, "cap flushed one full run");
}

// Byte cap: few-but-huge pictures are force-flushed once the buffered bytes reach
// MAX_PENDING_BYTES, even far below the frame cap.
#[test]
fn gop_byte_cap_flushes_during_parse() {
    let mut parser = Mpeg2Parser::new();
    let mut data = Vec::new();
    for _ in 0..2 {
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend(std::iter::repeat_n(0xAA, MAX_PENDING_BYTES / 2));
    }
    data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    data.extend_from_slice(&[0xAA; 4]);
    let out = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(out.len(), 2, "byte cap flushed the two huge pictures");
}

// Parser-level timing for a progressive sequence: rff counts 4 fields (tff=0) or 6 (tff=1)
// only when the sequence extension's progressive_sequence is honoured.
#[test]
fn progressive_sequence_rff_durations_via_parser() {
    let mut parser = Mpeg2Parser::new();
    let field = 20_000_000u64; // 25 fps
    let mut au = make_seq_header(720, 576, 3, 3);
    au.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x10, 0x08]); // progressive_sequence
    au.extend_from_slice(&gop());
    for (ct, tr, tff) in [(1u8, 0u16, 0u8), (2, 1, 1)] {
        au.extend_from_slice(&make_picture_header_tr(ct, tr));
        au.extend_from_slice(&pic_coding_ext(tff, 1, 0, true));
        au.extend_from_slice(&[0xAA; 8]);
    }
    let f = parse_then_flush(&mut parser, &make_pes(au, Some(0)));
    assert_eq!(f.len(), 2);
    assert_eq!(f[0].duration_ns, Some(4 * field), "rff, tff=0 → 4 fields");
    assert_eq!(f[1].duration_ns, Some(6 * field), "rff, tff=1 → 6 fields");
    assert_eq!(f[1].pts_ns as u64, 4 * field);
}

// Parser-level timing for field pictures: each occupies one field period.
#[test]
fn field_pictures_occupy_one_field_via_parser() {
    let mut parser = Mpeg2Parser::new();
    let field = 20_000_000u64; // 25 fps
    let mut au = make_seq_header(720, 576, 3, 3);
    au.extend_from_slice(&gop());
    for (ct, e2) in [(1u8, 0x01u8), (2, 0x02)] {
        au.extend_from_slice(&make_picture_header(ct));
        let mut ext = pic_coding_ext(0, 0, 0, false);
        ext[6] = e2;
        au.extend_from_slice(&ext);
        au.extend_from_slice(&[0xAA; 8]);
    }
    let f = parse_then_flush(&mut parser, &make_pes(au, Some(0)));
    assert_eq!(f.len(), 2);
    assert_eq!(f[0].duration_ns, Some(field));
    assert_eq!(f[1].duration_ns, Some(field));
}

// --- parse_resolution: 12-bit field packing (ISO 13818-2 §6.2.2.1) ---

#[test]
fn resolution_packs_split_nibble_correctly() {
    let hdr = make_seq_header(0xABC, 0xDEF, 1, 1);
    assert_eq!(parse_resolution(&hdr), Some((0xABC, 0xDEF)));
}

#[test]
fn resolution_max_12bit() {
    let hdr = make_seq_header(4095, 4095, 1, 1);
    assert_eq!(parse_resolution(&hdr), Some((4095, 4095)));
}

#[test]
fn resolution_too_short_none() {
    assert_eq!(parse_resolution(&[0x00, 0x00, 0x01, 0xB3, 0x07]), None);
}

// --- parse_frame_rate: full table + reserved codes ---

#[test]
fn frame_rate_all_valid_codes() {
    let expect = [
        (24000u32, 1001u32),
        (24, 1),
        (25, 1),
        (30000, 1001),
        (30, 1),
        (50, 1),
        (60000, 1001),
        (60, 1),
    ];
    for (i, &want) in expect.iter().enumerate() {
        let code = (i + 1) as u8;
        let hdr = make_seq_header(720, 480, 1, code);
        assert_eq!(parse_frame_rate(&hdr), Some(want), "frame_rate_code {code}");
    }
}

#[test]
fn frame_rate_code_zero_forbidden_none() {
    assert_eq!(parse_frame_rate(&make_seq_header(720, 480, 1, 0)), None);
}

#[test]
fn frame_rate_code_out_of_range_none() {
    assert_eq!(parse_frame_rate(&make_seq_header(720, 480, 1, 0x0F)), None);
}

// --- parse_aspect_ratio: table + reserved codes ---

#[test]
fn aspect_ratio_all_valid_codes() {
    let expect = [(1u8, 1u8), (4, 3), (16, 9), (221, 100)];
    for (i, &want) in expect.iter().enumerate() {
        let code = (i + 1) as u8;
        let hdr = make_seq_header(720, 480, code, 4);
        assert_eq!(parse_aspect_ratio(&hdr), Some(want), "aspect code {code}");
    }
}

#[test]
fn aspect_ratio_code_zero_none() {
    assert_eq!(parse_aspect_ratio(&make_seq_header(720, 480, 0, 4)), None);
}

#[test]
fn aspect_ratio_code_out_of_range_none() {
    assert_eq!(
        parse_aspect_ratio(&make_seq_header(720, 480, 0x0F, 4)),
        None
    );
}

// --- picture_coding_type: byte position + bit field ---

#[test]
fn picture_coding_type_bits_5_3() {
    for (ct, is_kf) in [(1u8, true), (2, false), (3, false), (4, false)] {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(ct);
        data.extend_from_slice(&[0xFF; 8]);
        let f = parse_after_i(&mut parser, &make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].keyframe, is_kf, "picture_coding_type {ct}");
    }
}

#[test]
fn parser_resolution_method() {
    let mut parser = Mpeg2Parser::new();
    let mut data = make_seq_header(720, 576, 2, 3);
    data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
    data.extend_from_slice(&[0xFF; 4]);
    let _ = parse_then_flush(&mut parser, &make_pes(data, Some(0)));

    assert_eq!(parser.resolution(), Some((720, 576)));
    assert_eq!(parser.frame_rate(), Some((25, 1))); // frame_rate_code 3 = 25fps
    assert_eq!(parser.aspect_ratio(), Some((4, 3))); // aspect code 2 = 4:3
}
