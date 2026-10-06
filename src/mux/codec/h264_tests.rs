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

// Open-GOP resync fix: a non-IDR intra access unit must be flagged a keyframe so the B1
// resync gate can disarm on a BD open-GOP tail.
#[test]
fn open_gop_intra_access_unit_is_a_keyframe() {
    // Annex B AU: one non-IDR coded slice (NAL type 1, nal_ref_idc 3 -> 0x61)
    // whose header decodes first_mb_in_slice=0 (ue="1"), slice_type=7 (I,
    // ue="0001000"); packed into one byte: "1"+"0001000" = 0x88.
    let au = vec![0x00, 0x00, 0x01, 0x61, 0x88, 0x00];
    let mut parser = H264Parser::new();
    let frames = parser.parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1, "one PES access unit → one frame");
    assert_eq!(
        frames[0].coding.map(|c| c.coding_type()),
        Some(CodingType::I),
        "the slice header measures an I picture",
    );
    assert!(
        frames[0].keyframe,
        "an open-GOP intra access unit is a resync-safe keyframe even without an IDR",
    );
}

/// A parser that has emitted an IDR at PTS 0, so an inter picture after it decodes.
fn primed() -> H264Parser {
    let mut p = H264Parser::new();
    let idr = p.parse(&make_pes(vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00], Some(0)));
    assert!(idr[0].keyframe);
    p
}

// --- Pictures a decoder cannot reconstruct from the stream's own data ---

/// One non-IDR picture AU: `hdr` = NAL header byte, `body` = slice-header byte
/// (0x88 I, 0x98 P, 0x9C B), at `frame` 24 fps frame periods (90 kHz).
fn pic_au(hdr: u8, body: u8, frame: i64) -> PesPacket {
    make_pes(vec![0x00, 0x00, 0x01, hdr, body, 0x00], Some(frame * 3750))
}

/// An open GOP in decode order `I3 B1 B2 P6 B4 B5` (display frame numbers),
/// offset by `base` frames. B1/B2 precede the I in display order: leading
/// pictures predicted from the anchor BEFORE this GOP.
fn open_gop(base: i64) -> Vec<PesPacket> {
    vec![
        pic_au(0x61, 0x88, base + 3),
        pic_au(0x01, 0x9C, base + 1),
        pic_au(0x01, 0x9C, base + 2),
        pic_au(0x41, 0x98, base + 6),
        pic_au(0x01, 0x9C, base + 4),
        pic_au(0x01, 0x9C, base + 5),
    ]
}

/// Emitted frames' display frame numbers.
fn frame_nos(frames: &[Frame]) -> Vec<i64> {
    frames
        .iter()
        .map(|f| (f.pts_ns * 24 + 500_000_000) / 1_000_000_000)
        .collect()
}

#[test]
fn an_open_gop_at_the_stream_start_drops_its_leading_b_pictures() {
    let mut p = H264Parser::new();
    let f: Vec<Frame> = open_gop(0).iter().flat_map(|x| p.parse(x)).collect();
    assert_eq!(
        frame_nos(&f),
        [3, 6, 4, 5],
        "B1/B2 reference a picture the output does not hold"
    );
}

#[test]
fn a_join_onto_an_open_gop_drops_its_leading_b_pictures() {
    // Clip 1 ends on a complete open GOP; clip 2's PTS restarts far away, so
    // its leading B1/B2 reference a picture of the clip the playlist skipped.
    let mut p = H264Parser::new();
    let mut pes = vec![make_pes(vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00], Some(0))];
    pes.extend(open_gop(0).into_iter().skip(3));
    pes.extend(open_gop(10_000));
    let f: Vec<Frame> = pes.iter().flat_map(|x| p.parse(x)).collect();
    assert_eq!(
        frame_nos(&f)[4..],
        [10_003, 10_006, 10_004, 10_005],
        "clip 2's leading Bs reference clip 1's tail, not their own source"
    );
}

#[test]
fn pictures_before_the_first_random_access_picture_are_dropped() {
    let mut p = H264Parser::new();
    let mut pes = vec![pic_au(0x41, 0x98, 0), pic_au(0x01, 0x9C, 1)];
    pes.push(make_pes(
        vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00],
        Some(2 * 3750),
    ));
    let f: Vec<Frame> = pes.iter().flat_map(|x| p.parse(x)).collect();
    assert!(f[0].keyframe, "the first emitted picture is the IDR");
}

#[test]
fn a_gap_resync_onto_an_open_gop_emits_no_leading_b_pictures() {
    // IDR, then a P that follows lost data (and the B predicted from it), then
    // an open GOP continuing the same clock: its B1/B2 reference the dropped P.
    let mut p = H264Parser::new();
    let mut pes = vec![make_pes(
        vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00],
        Some(7 * 3750),
    )];
    let mut lost = pic_au(0x41, 0x98, 9);
    lost.discontinuity = true;
    pes.push(lost);
    pes.push(pic_au(0x01, 0x9C, 8));
    pes.extend(open_gop(10));
    let mut gate = crate::mux::resync::ResyncGate::new();
    let f: Vec<Frame> = pes
        .iter()
        .flat_map(|x| p.parse(x))
        .filter(|f| gate.admit(true, f.discontinuity, f.keyframe))
        .collect();
    assert_eq!(
        frame_nos(&f),
        [7, 13, 16, 14, 15],
        "B1/B2 reference the P the gate dropped"
    );
}

#[test]
fn a_gap_leaves_its_loss_to_the_gate() {
    // The P after the gap and the B predicted from it decode from the stream's own
    // anchors, so the parser passes them: the gate drops and counts them as loss.
    let mut p = H264Parser::new();
    let mut lost = pic_au(0x41, 0x98, 9);
    lost.discontinuity = true;
    let pes = [
        make_pes(vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00], Some(7 * 3750)),
        lost,
        pic_au(0x01, 0x9C, 8),
    ];
    let f: Vec<Frame> = pes.iter().flat_map(|x| p.parse(x)).collect();
    assert_eq!(frame_nos(&f), [7, 9, 8]);
    let mut gate = crate::mux::resync::ResyncGate::new();
    for x in &f {
        gate.admit(true, x.discontinuity, x.keyframe);
    }
    assert_eq!(gate.dropped_total(), 2);
}

#[test]
fn a_continuous_open_gop_keeps_its_leading_b_pictures() {
    // The leading B7/B8 predict from P6, which the output holds.
    let mut p = H264Parser::new();
    let mut pes = vec![make_pes(vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00], Some(0))];
    pes.extend(open_gop(0).into_iter().skip(3));
    pes.extend(open_gop(6));
    let f: Vec<Frame> = pes.iter().flat_map(|x| p.parse(x)).collect();
    assert_eq!(frame_nos(&f), [0, 6, 4, 5, 9, 7, 8, 12, 10, 11]);
}

/// `pic_au` carrying a DTS (`dts` frames) when it differs from its PTS.
fn pic_au_dts(hdr: u8, body: u8, frame: i64, dts: i64) -> PesPacket {
    let mut p = pic_au(hdr, body, frame);
    p.dts = (dts != frame).then_some(dts * 3750);
    p
}

// Decode order IDR P B B, then an open GOP I B B P whose I decodes `skip` frames late.
fn decode_clock_run(skip: i64) -> Vec<i64> {
    let s = skip;
    let pes = [
        pic_au_dts(0x65, 0x88, 1, 0),
        pic_au_dts(0x41, 0x98, 4, 1),
        pic_au_dts(0x01, 0x9C, 2, 2),
        pic_au_dts(0x01, 0x9C, 3, 3),
        pic_au_dts(0x61, 0x88, 7 + s, 4 + s),
        pic_au_dts(0x01, 0x9C, 5 + s, 5 + s),
        pic_au_dts(0x01, 0x9C, 6 + s, 6 + s),
        pic_au_dts(0x41, 0x98, 10 + s, 7 + s),
    ];
    let mut p = H264Parser::new();
    frame_nos(&pes.iter().flat_map(|x| p.parse(x)).collect::<Vec<_>>())
}

#[test]
fn the_decode_clock_tells_a_join_of_a_frame_or_two() {
    assert_eq!(decode_clock_run(0), [1, 4, 2, 3, 7, 5, 6, 10]);
    // Within the PTS-only window, but the decode clock skipped: another clip.
    assert_eq!(decode_clock_run(2), [1, 4, 2, 3, 9, 12]);
}

#[test]
fn a_closed_gop_title_keeps_every_picture() {
    let idr = |frame: i64| make_pes(vec![0x00, 0x00, 0x01, 0x65, 0x88, 0x00], Some(frame * 3750));
    let pes = [
        idr(0),
        pic_au(0x41, 0x98, 3),
        pic_au(0x01, 0x9C, 1),
        pic_au(0x01, 0x9C, 2),
        idr(4),
        pic_au(0x41, 0x98, 7),
        pic_au(0x01, 0x9C, 5),
        pic_au(0x01, 0x9C, 6),
    ];
    let mut p = H264Parser::new();
    let f: Vec<Frame> = pes.iter().flat_map(|x| p.parse(x)).collect();
    assert_eq!(frame_nos(&f), [0, 3, 1, 2, 4, 7, 5, 6]);
}

// The escaped slice header must be unescaped at the parse call site: a second slice with
// first_mb_in_slice=65535 (00 00 03 escape) is intra only when read unescaped.
#[test]
fn parse_unescapes_later_slice_header_before_reading_it() {
    let au = vec![
        0x00, 0x00, 0x01, 0x61, 0x88, 0x00, // slice 1: first_mb=0, I
        0x00, 0x00, 0x01, 0x61, 0x00, 0x00, 0x03, 0x80, 0x00, 0x30, // slice 2: 65535, I
    ];
    let mut parser = H264Parser::new();
    let frames = parser.parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].keyframe,
        "every slice reads intra only if the escape byte is removed"
    );
}

// The gap flag on a PES must reach the frame, or the resync gate never arms for H.264.
#[test]
fn pes_discontinuity_propagates_to_frame() {
    let au = vec![0x00, 0x00, 0x01, 0x61, 0x88, 0x00];
    for flag in [true, false] {
        let mut pes = make_pes(au.clone(), Some(0));
        pes.discontinuity = flag;
        let frames = H264Parser::new().parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].discontinuity, flag);
    }
}

// Guard the promotion's precision: a non-intra (P) AU must NOT become a
// keyframe, or resync would resume on inter-referenced dropped data.
#[test]
fn inter_coded_access_unit_is_not_a_keyframe() {
    // Same NAL type 1, but slice_type = 0 (P): ue "1", so with first_mb = 0
    // the byte is "1" + "1" padded = 0b1100_0000 = 0xC0.
    let au = vec![0x00, 0x00, 0x01, 0x61, 0xC0, 0x00];
    let mut parser = primed();
    let frames = parser.parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].coding.map(|c| c.coding_type()),
        Some(CodingType::P),
        "the slice header measures a P picture",
    );
    assert!(
        !frames[0].keyframe,
        "an inter-coded access unit must never be a keyframe",
    );
}

// A picture whose FIRST slice is I but whose LATER slice is P/B is not a
// clean random-access point: the open-GOP promotion must require EVERY slice
// of the picture to be intra, not trust the first slice alone.
#[test]
fn a_mixed_slice_picture_with_a_p_slice_is_not_promoted() {
    // One picture, two slices: I at first_mb 0 (0x88, slice_type=7), then P at
    // first_mb 1 (ue '010', slice_type '1', stop '1' -> 0x58). NAL type 1 -> 0x61.
    let au = vec![
        0x00, 0x00, 0x01, 0x61, 0x88, // I slice, first_mb 0
        0x00, 0x00, 0x01, 0x61, 0x58, // P slice, first_mb 1 (same picture)
        0x00,
    ];
    let mut parser = primed();
    let frames = parser.parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1, "one PES access unit → one frame");
    assert_eq!(
        frames[0].coding.map(|c| c.coding_type()),
        Some(CodingType::I),
        "the picture's coding type is still the first slice's (I)",
    );
    assert!(
        !frames[0].keyframe,
        "a picture with a P slice is not a random-access point and must not promote",
    );
}

// 1080i open-GOP anchor (heuristic, not spec-guaranteed; recovery_point SEI §D.2.8 is
// authoritative): an I first field with a P second field is promoted.
#[test]
fn field_coded_i_then_p_pair_is_a_keyframe() {
    // I field: slices at first_mb 0 (0x88) and 1 ('010'+'0001000'+stop -> 0x42 0x20);
    // P field: slices at first_mb 0 (0xC0) and 1 (0x58). NAL type 1 -> 0x61.
    let au = vec![
        0x00, 0x00, 0x01, 0x61, 0x88, // I field, first_mb 0
        0x00, 0x00, 0x01, 0x61, 0x42, 0x20, // I field, first_mb 1
        0x00, 0x00, 0x01, 0x61, 0xC0, // P field, first_mb 0
        0x00, 0x00, 0x01, 0x61, 0x58, // P field, first_mb 1
        0x00,
    ];
    let frames = H264Parser::new().parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].coding.map(|c| c.coding_type()),
        Some(CodingType::I)
    );
    assert!(
        frames[0].keyframe,
        "an I/P field pair is an open-GOP anchor"
    );
}

// A second-field slice whose slice_type is truncated still starts a new picture
// (first_mb 0), so it must not veto the I first field.
#[test]
fn truncated_second_field_slice_type_does_not_block_promotion() {
    // I field (0x88), then first_mb 0 with nothing after it (0x80: '1', then zeros).
    let au = vec![0x00, 0x00, 0x01, 0x61, 0x88, 0x00, 0x00, 0x01, 0x61, 0x80];
    let frames = H264Parser::new().parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].keyframe,
        "second-field header truncation is not a veto"
    );
}

// MVC dependent-view (Blu-ray 3D right-eye) passthrough: an intra dependent
// view is not an independent random-access anchor, so it must NOT be
// promoted to a keyframe even when every slice is intra.
#[test]
fn an_intra_mvc_dependent_view_is_not_promoted() {
    // Single non-IDR I slice, as in open_gop_intra_access_unit_is_a_keyframe.
    let au = vec![0x00, 0x00, 0x01, 0x61, 0x88, 0x00];
    let mut parser = H264Parser::new().with_mvc_passthrough(true);
    let frames = parser.parse(&make_pes(au, Some(0)));
    assert_eq!(frames.len(), 1);
    assert!(
        !frames[0].keyframe,
        "an MVC dependent view is never an independent anchor, intra or not",
    );
}

// The slice-header parse must remove emulation-prevention bytes first (ISO/IEC 14496-10
// §7.3.1/§7.4.1) before decoding ue(v) fields.
#[test]
fn slice_header_parse_removes_emulation_prevention_bytes() {
    // first_mb_in_slice=65535 needs 16 zero bits (ue(v)); combined with
    // slice_type=2 (I, ue="011") the bits pack to 00 00 80 00 30 RBSP,
    // and the leading 00 00 forces the encoder to escape it: 00 00 03 80 00 30.
    let rbsp = [0x00u8, 0x00, 0x80, 0x00, 0x30];
    let ebsp = [0x00u8, 0x00, 0x03, 0x80, 0x00, 0x30];

    // The un-escaped prefix must equal the original RBSP.
    assert_eq!(
        super::unescape_ebsp_prefix(&ebsp),
        rbsp,
        "the 0x03 after 00 00 must be dropped"
    );

    // And both must decode to the same two fields. Reading the EBSP raw is
    // what used to happen, and it does NOT agree.
    let unescaped = super::unescape_ebsp_prefix(&ebsp);
    let mut good = super::BitReader::new(&unescaped);
    let (first_mb, slice_type) = (good.read_ue(), good.read_ue());
    assert_eq!(first_mb, Some(65535), "first_mb_in_slice");
    assert_eq!(slice_type, Some(2), "slice_type must survive the escape");

    let mut raw = super::BitReader::new(&ebsp[..]);
    let (_, raw_slice_type) = (raw.read_ue(), raw.read_ue());
    assert_ne!(
        raw_slice_type,
        Some(2),
        "if the raw EBSP decoded correctly this test would prove nothing"
    );
}

/// A 0x03 that does NOT follow 00 00 is ordinary payload and must be kept,
/// and the sequence 00 00 03 03 keeps its second 0x03 (§7.4.1 resets the
/// zero run at the escape).
#[test]
fn unescape_keeps_an_0x03_that_is_not_an_escape() {
    assert_eq!(super::unescape_ebsp_prefix(&[0x03, 0x03]), vec![0x03, 0x03]);
    assert_eq!(
        super::unescape_ebsp_prefix(&[0x01, 0x00, 0x03]),
        vec![0x01, 0x00, 0x03]
    );
    assert_eq!(
        super::unescape_ebsp_prefix(&[0x00, 0x00, 0x03, 0x03]),
        vec![0x00, 0x00, 0x03]
    );
}

// Regression: `parse_sps_high_profile_ext` used to re-derive the emulation-prevention rule
// with its own (disagreeing) window scanner. Pin the shared `unescape_ebsp` behaviour.
#[test]
fn unescape_ebsp_drops_escape_after_a_run_of_three_zeros() {
    assert_eq!(
        super::unescape_ebsp(&[0x00, 0x00, 0x00, 0x03, 0x42], 5),
        vec![0x00, 0x00, 0x00, 0x42],
        "the 0x03 after a 3-zero run is an escape byte, not payload"
    );
}

// --- keyframe parameter-set re-assert: exact bytes + no whole-frame copy --- Must produce
// EXACTLY: active SPS, active PPS, then the AU's own NALs, each length-prefixed.
#[test]
fn keyframe_param_reassert_emits_exact_bytes() {
    fn annexb(nal: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01];
        v.extend_from_slice(nal);
        v
    }
    const SPS: [u8; 5] = [0x67, 0x42, 0x00, 0x1E, 0xAB];
    const PPS: [u8; 3] = [0x68, 0xCE, 0x01];
    const IDR1: [u8; 4] = [0x65, 0x88, 0x84, 0x21];
    const IDR2: [u8; 4] = [0x65, 0x88, 0x11, 0x22];

    let mut parser = H264Parser::new();

    // AU1: SPS + PPS + IDR. Both parameter sets are first-of-type, so they
    // seed avcC and are stripped from the in-band scan output; the keyframe
    // re-assert then splices them back in ahead of the slice.
    let au1 = [annexb(&SPS), annexb(&PPS), annexb(&IDR1)].concat();
    let f1 = parser.parse(&make_pes(au1, Some(0)));
    assert_eq!(
        f1[0].data,
        vec![
            0x00, 0x00, 0x00, 0x05, 0x67, 0x42, 0x00, 0x1E, 0xAB, // SPS
            0x00, 0x00, 0x00, 0x03, 0x68, 0xCE, 0x01, // PPS
            0x00, 0x00, 0x00, 0x04, 0x65, 0x88, 0x84, 0x21, // IDR slice
        ],
        "keyframe must emit length-prefixed SPS, PPS, then the slice"
    );

    // AU2: BARE keyframe — the source omits the parameter sets. The active
    // set must be re-asserted ahead of the slice, byte-identically.
    let f2 = parser.parse(&make_pes(annexb(&IDR2), Some(3600)));
    assert_eq!(
        f2[0].data,
        vec![
            0x00, 0x00, 0x00, 0x05, 0x67, 0x42, 0x00, 0x1E, 0xAB, // SPS
            0x00, 0x00, 0x00, 0x03, 0x68, 0xCE, 0x01, // PPS
            0x00, 0x00, 0x00, 0x04, 0x65, 0x88, 0x11, 0x22, // IDR slice
        ],
        "bare keyframe must re-assert the active SPS/PPS ahead of the slice"
    );
}

// MEASURED: the keyframe param-set re-assert must be spliced in place, not built as a fresh
// full-size buffer (avoids a whole-frame realloc+copy per keyframe). Mirrors HEVC's test of
// the same name.
#[test]
fn keyframe_param_reassert_does_not_reallocate_the_frame() {
    fn annexb(nal_header: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, nal_header];
        v.extend_from_slice(body);
        v
    }
    let mut parser = H264Parser::new();
    // AU1 seeds the active SPS/PPS. The SPS is deliberately ~200 bytes so the
    // re-assert prefix exceeds the incidental `+64` slack in `frame_data`'s
    // capacity — a tiny SPS would pass even with PARAM_REASSERT_HEADROOM zeroed.
    let mut big_sps = vec![0x42, 0x00, 0x1E];
    big_sps.extend(std::iter::repeat_n(0xAB, 200));
    let au1 = [
        annexb(0x67, &big_sps),
        annexb(0x68, &[0xCE, 0x01]),
        annexb(0x65, &[0x88; 4096]),
    ]
    .concat();
    parser.parse(&make_pes(au1, Some(0)));

    // A run of BARE keyframes (source omits the parameter sets), each of
    // which takes the re-assert path. Payload sized like a real coded
    // picture so a reallocation would be the expensive one.
    PARAM_REASSERT_REALLOCS.with(|c| c.set(0));
    for i in 0..30i64 {
        let au = annexb(0x65, &vec![0x88u8; 300_000]);
        let f = parser.parse(&make_pes(au, Some(3600 * (i + 1))));
        // The re-assert really happened (otherwise the count is vacuously 0):
        // length-prefixed slice + SPS (4+1+body) + PPS (4+1+2).
        assert_eq!(
            f[0].data.len(),
            (4 + 1 + 300_000) + (4 + 1 + big_sps.len()) + (4 + 1 + 2),
            "keyframe {i} must carry the re-asserted parameter sets"
        );
    }
    let reallocs = PARAM_REASSERT_REALLOCS.with(|c| c.get());
    assert_eq!(
        reallocs, 0,
        "the parameter-set splice must fit in the reserved headroom; \
             {reallocs} of 30 keyframes reallocated the whole frame"
    );
}

// --- parse SPS+PPS → codec_private ---

#[test]
fn parse_sps_pps() {
    let mut parser = H264Parser::new();

    // Build PES with SPS (type 7) + PPS (type 8) + IDR slice (type 5)
    // SPS NAL: 0x67 = 0_11_00111 (nal_type = 7), followed by profile/compat/level + payload
    // PPS NAL: 0x68 = 0_11_01000 (nal_type = 8)
    let mut data = Vec::new();
    // SPS: 00 00 01 [67 42 00 1E <payload>]
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x67); // SPS
    data.extend_from_slice(&[0x42, 0x00, 0x1E, 0xAB, 0xCD]); // profile=0x42, compat=0x00, level=0x1E
    // PPS: 00 00 01 [68 <payload>]
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x68); // PPS
    data.extend_from_slice(&[0xCE, 0x01]);
    // IDR slice: 00 00 01 [65 <payload>]
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x65); // IDR
    data.extend_from_slice(&[0x88, 0x00, 0x10]);

    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);

    // codec_private should now be available
    let cp = parser.codec_private();
    assert!(
        cp.is_some(),
        "codec_private should be Some after seeing SPS+PPS"
    );
    let cp = cp.unwrap();

    // AVCDecoderConfigurationRecord checks
    assert_eq!(cp[0], 1, "configurationVersion");
    assert_eq!(cp[1], 0x42, "profile from SPS[1]");
    assert_eq!(cp[2], 0x00, "compatibility from SPS[2]");
    assert_eq!(cp[3], 0x1E, "level from SPS[3]");
    assert_eq!(cp[4], 0xFF, "reserved + lengthSizeMinusOne=3");
    assert_eq!(cp[5], 0xE1, "reserved + numSPS=1");

    // Frames should have been produced
    assert_eq!(frames.len(), 1);
}

// Length-prefixed NAL bodies out of frame_data, and the H.264 PPS (type 8)
// payloads among them.
fn h264_nals_in(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= frame.len() {
        let len = u32::from_be_bytes([frame[i], frame[i + 1], frame[i + 2], frame[i + 3]]) as usize;
        i += 4;
        if i + len > frame.len() {
            break;
        }
        out.push(frame[i..i + len].to_vec());
        i += len;
    }
    out
}
fn h264_pps_bodies(nals: &[Vec<u8>]) -> Vec<Vec<u8>> {
    nals.iter()
        .filter(|n| !n.is_empty() && n[0] & 0x1F == 8)
        .map(|n| n[1..].to_vec())
        .collect()
}
fn h264_nal(t: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x00, 0x01, t];
    v.extend_from_slice(body);
    v
}

#[test]
fn mvc_passthrough_keeps_param_sets_inband() {
    // A dependent-view access unit: subset SPS (NAL 15) + PPS (NAL 8) +
    // coded-slice-extension (NAL 20). No IDR (type 5), so keyframe stays
    // false and there is no keyframe re-assertion.
    let au = || {
        let mut d = Vec::new();
        d.extend_from_slice(&h264_nal(0x6F, &[0x80, 0x00, 0x33, 0xAA])); // subset SPS (15)
        d.extend_from_slice(&h264_nal(0x68, &[0xCE, 0x01])); // PPS (8)
        d.extend_from_slice(&h264_nal(0x74, &[0x11, 0x22])); // slice-ext (20)
        d
    };
    let nal_types =
        |f: &Frame| -> Vec<u8> { h264_nals_in(&f.data).iter().map(|n| n[0] & 0x1F).collect() };

    // Normal parser strips the PPS from a non-keyframe AU (it is captured for
    // the avcC and, without an IDR, never re-asserted in-band).
    let mut normal = H264Parser::new();
    let f = normal.parse(&make_pes(au(), Some(90000)));
    assert_eq!(f.len(), 1);
    assert!(
        !nal_types(&f[0]).contains(&8),
        "normal parser strips PPS from a non-keyframe AU: {:?}",
        nal_types(&f[0])
    );

    // Passthrough keeps EVERY parameter set in-band, so each dependent frame
    // is a self-contained access unit for a BlockAdditional.
    let mut pt = H264Parser::new().with_mvc_passthrough(true);
    let f = pt.parse(&make_pes(au(), Some(90000)));
    assert_eq!(f.len(), 1);
    let types = nal_types(&f[0]);
    assert!(types.contains(&15), "subset SPS kept in-band: {types:?}");
    assert!(
        types.contains(&8),
        "PPS kept in-band under passthrough: {types:?}"
    );
    assert!(types.contains(&20), "slice kept: {types:?}");
}

#[test]
fn parser_for_mvc_dependent_h264_is_passthrough() {
    // The dependent-view stream must get a passthrough parser: a PPS in a
    // non-keyframe AU is kept in-band, not stripped like the base parser.
    let mut p = crate::mux::codec::parser_for_mvc_dependent(crate::disc::Codec::H264, false);
    let mut d = Vec::new();
    d.extend_from_slice(&h264_nal(0x68, &[0xCE, 0x01])); // PPS (8)
    d.extend_from_slice(&h264_nal(0x74, &[0x11, 0x22])); // slice-ext (20)
    let f = p.parse(&make_pes(d, Some(90000)));
    assert_eq!(f.len(), 1);
    let types: Vec<u8> = h264_nals_in(&f[0].data)
        .iter()
        .map(|n| n[0] & 0x1F)
        .collect();
    assert!(
        types.contains(&8),
        "dependent parser keeps PPS in-band (passthrough): {types:?}"
    );
}

#[test]
fn mvc_passthrough_with_idr_does_not_reassert_param_sets() {
    // With an IDR present (keyframe=true), passthrough must NOT re-assert the
    // param sets (the `keyframe && !mvc` guard), so SPS/PPS appear exactly
    // once — a duplicate would corrupt the dependent BlockAdditional.
    let mut p = H264Parser::new().with_mvc_passthrough(true);
    let mut d = Vec::new();
    d.extend_from_slice(&h264_nal(0x67, &[0x42, 0x00, 0x1E, 0x01])); // SPS (7)
    d.extend_from_slice(&h264_nal(0x68, &[0xCE, 0x01])); // PPS (8)
    d.extend_from_slice(&h264_nal(0x65, &[0x88, 0x00])); // IDR slice (5)
    let f = p.parse(&make_pes(d, Some(90000)));
    assert_eq!(f.len(), 1);
    let types: Vec<u8> = h264_nals_in(&f[0].data)
        .iter()
        .map(|n| n[0] & 0x1F)
        .collect();
    assert_eq!(
        types.iter().filter(|&&t| t == 7).count(),
        1,
        "exactly one SPS, no keyframe re-assert under passthrough: {types:?}"
    );
    assert_eq!(
        types.iter().filter(|&&t| t == 8).count(),
        1,
        "exactly one PPS, no keyframe re-assert under passthrough: {types:?}"
    );
}

#[test]
fn h264_populates_measured_coding_type_and_source() {
    use super::super::coding::CodingType;
    // Slice-header body = first_mb_in_slice=0 ('1') then slice_type ue(v):
    // 0x88='1 0001000'->I (7); 0x98='1 00110..'->P (5); 0x9C='1 00111..'->B (6).
    let src = crate::pes::SourcePos::at_byte(8192);
    let parse = |nal_type: u8, body: u8| {
        let mut p = primed();
        let mut pe = make_pes(h264_nal(nal_type, &[body]), Some(0));
        pe.source = Some(src);
        p.parse(&pe)
    };

    // IDR carrying an I-slice → keyframe + MEASURED I; source carried; H.264
    // field order is not decoded, so it is honestly absent (not guessed).
    let fi = parse(NAL_SLICE_IDR, 0x88);
    assert_eq!(fi.len(), 1);
    assert!(fi[0].keyframe, "IDR is a keyframe");
    let ci = fi[0].coding.expect("H.264 frame carries PictureInfo");
    assert_eq!(ci.coding_type(), CodingType::I, "slice_type 7 → I");
    assert!(
        ci.field_order().is_none(),
        "H.264 field order undecoded → None, never faked"
    );
    assert_eq!(
        fi[0].source.unwrap().byte,
        8192,
        "source provenance carried"
    );

    // Non-IDR P / B slices → MEASURED P / B, not keyframes.
    let fp = parse(NAL_SLICE_NON_IDR, 0x98);
    assert_eq!(
        fp[0].coding.unwrap().coding_type(),
        CodingType::P,
        "slice_type 5 → P"
    );
    assert!(!fp[0].keyframe);
    let fb = parse(NAL_SLICE_NON_IDR, 0x9C);
    assert_eq!(
        fb[0].coding.unwrap().coding_type(),
        CodingType::B,
        "slice_type 6 → B"
    );
}

// End-to-end sparse-PTS reconstruction: a program-stream source that
// stamps PTS only on each GOP's I-frame must yield distinct, display-
// ordered PTS for every frame. Without reorder, non-anchors collapse.
#[test]
fn h264_ps_reorder_reconstructs_distinct_display_pts() {
    use super::super::coding::CodingType;
    // slice bodies: 0x88 → I (IDR), 0x98 → P, 0x9C → B (non-IDR).
    // Decode order of a classic single-B GOP: I P B P B.
    let gop = |anchor_pts: Option<i64>| {
        vec![
            (NAL_SLICE_IDR, 0x88u8, anchor_pts),
            (NAL_SLICE_NON_IDR, 0x98, None),
            (NAL_SLICE_NON_IDR, 0x9C, None),
            (NAL_SLICE_NON_IDR, 0x98, None),
            (NAL_SLICE_NON_IDR, 0x9C, None),
        ]
    };

    let feed = |reorder: bool| -> Vec<super::super::Frame> {
        let mut p = H264Parser::new().with_ps_reorder(reorder);
        let mut out = Vec::new();
        // Two GOPs; the second I carries an anchor 5 frames later (90 kHz:
        // 5 * 3750 = 18750 ticks) so the reorder can calibrate a duration.
        for (nal, body, pts) in gop(Some(0)).into_iter().chain(gop(Some(18750))) {
            out.extend(p.parse(&make_pes(h264_nal(nal, &[body]), pts)));
        }
        out.extend(p.flush());
        out
    };

    // With reorder ON: all 10 frames emitted, every PTS distinct.
    let recon = feed(true);
    assert_eq!(recon.len(), 10, "no frame dropped");
    let mut pts: Vec<i64> = recon.iter().map(|f| f.pts_ns).collect();
    let n = pts.len();
    pts.sort_unstable();
    pts.dedup();
    assert_eq!(
        pts.len(),
        n,
        "reconstructed PTS are all distinct (no DTS collision)"
    );

    // The GOP's first-displayed frame is the I; the B in decode position 2
    // must display BEFORE the P in decode position 1 (classic reorder).
    let g1 = &recon[0..5];
    assert_eq!(g1[0].coding.unwrap().coding_type(), CodingType::I);
    assert!(
        g1[2].pts_ns < g1[1].pts_ns,
        "B (decode idx 2) displays before its forward-anchor P (decode idx 1)"
    );
    assert_eq!(g1[0].pts_ns, 0, "GOP anchor locks the I to its true PTS");

    // With reorder OFF (transport-stream behaviour): the non-anchor frames
    // collapse to a single colliding PTS — the bug this fix removes.
    let raw = feed(false);
    let collisions = raw.iter().filter(|f| f.pts_ns == 0).count();
    assert!(
        collisions >= 8,
        "without reorder the sparse-PTS frames collide on 0 (got {collisions})"
    );
}

// Regression (PPS revert bug, H.264 variant): PPS id 0 = body A (→ avcC),
// redefined to B, then switched back to A; the revert must still be
// emitted in-band or the A-segment decodes against stale B.
#[test]
fn h264_emits_switch_back_to_codecprivate_pps() {
    let a = [0xA1u8, 0xA2];
    let b = [0xB1u8, 0xB2, 0xB3];
    let mut p = H264Parser::new();
    // AU1: SPS (seed avcC) + PPS-A (seed) + IDR.
    p.parse(&make_pes(
        [
            h264_nal(0x67, &[0x42, 0x00, 0x1E, 0xAB]),
            h264_nal(0x68, &a),
            h264_nal(0x65, &[1]),
        ]
        .concat(),
        Some(0),
    ));
    // AU2 IDR: redefine PPS to B → emitted in-band.
    let f2 = p.parse(&make_pes(
        [h264_nal(0x68, &b), h264_nal(0x65, &[2])].concat(),
        Some(1),
    ));
    assert!(
        h264_pps_bodies(&h264_nals_in(&f2[0].data))
            .iter()
            .any(|x| x == &b),
        "AU2 must carry redefined PPS-B in-band"
    );
    // AU3 IDR: back to A (== avcC) — must be emitted in-band (active was B).
    let f3 = p.parse(&make_pes(
        [h264_nal(0x68, &a), h264_nal(0x65, &[3])].concat(),
        Some(2),
    ));
    assert!(
        h264_pps_bodies(&h264_nals_in(&f3[0].data))
            .iter()
            .any(|x| x == &a),
        "switch back to avcC PPS-A must be emitted in-band"
    );
}

/// Regression: a bare IDR keyframe (source omits the PPS) after a mid-title
/// redefinition must re-assert the active PPS in-band.
#[test]
fn h264_reasserts_active_pps_at_bare_keyframe() {
    let a = [0xA1u8, 0xA2];
    let b = [0xB1u8, 0xB2, 0xB3];
    let mut p = H264Parser::new();
    p.parse(&make_pes(
        [
            h264_nal(0x67, &[0x42, 0x00, 0x1E, 0xAB]),
            h264_nal(0x68, &a),
            h264_nal(0x65, &[1]),
        ]
        .concat(),
        Some(0),
    ));
    // Redefine to B at a keyframe.
    p.parse(&make_pes(
        [h264_nal(0x68, &b), h264_nal(0x65, &[2])].concat(),
        Some(1),
    ));
    // Bare IDR (no PPS): active B must be re-asserted; stale A must not be.
    let f3 = p.parse(&make_pes(h264_nal(0x65, &[3]), Some(2)));
    let got = h264_pps_bodies(&h264_nals_in(&f3[0].data));
    assert!(
        got.iter().any(|x| x == &b),
        "bare keyframe must re-assert active PPS-B"
    );
    assert!(
        !got.iter().any(|x| x == &a),
        "must not re-assert stale avcC PPS-A"
    );
}

#[test]
fn codec_private_none_before_sps_pps() {
    let parser = H264Parser::new();
    assert!(parser.codec_private().is_none());
}

// --- IDR keyframe detection ---

#[test]
fn parse_idr_keyframe() {
    let mut parser = H264Parser::new();

    // PES with IDR NAL (type 5 = 0x65)
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x65); // IDR slice (nal_type = 5)
    data.extend_from_slice(&[0x88, 0x00, 0x10, 0x20]);

    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].keyframe,
        "IDR slice should be detected as keyframe"
    );
}

// --- non-IDR → not keyframe ---

#[test]
fn parse_non_idr() {
    let mut parser = primed();

    // PES with non-IDR slice (type 1 = 0x61 or 0x41)
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x41); // non-IDR coded slice (nal_type = 1)
    data.extend_from_slice(&[0x9A, 0x00, 0x10]);

    let pes = make_pes(data, Some(180000));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    assert!(!frames[0].keyframe, "non-IDR slice should not be keyframe");
}

// --- length prefix conversion ---

#[test]
fn length_prefix_conversion() {
    let mut parser = primed();

    // PES with a single non-IDR NAL
    let nal_payload = [0x41, 0xAA, 0xBB, 0xCC, 0xDD]; // type 1, 5 bytes
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.extend_from_slice(&nal_payload);

    let pes = make_pes(data, Some(0));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    let frame_data = &frames[0].data;

    // Should start with 4-byte big-endian length prefix
    assert!(
        frame_data.len() >= 4,
        "frame data should have length prefix"
    );
    let length = u32::from_be_bytes([frame_data[0], frame_data[1], frame_data[2], frame_data[3]]);
    assert_eq!(
        length as usize,
        nal_payload.len(),
        "length prefix should match NAL size"
    );

    // Followed by the NAL data itself
    assert_eq!(&frame_data[4..], &nal_payload);

    // No start code (00 00 01) should appear in the output
    for i in 0..frame_data.len().saturating_sub(2) {
        let is_sc = frame_data[i] == 0x00 && frame_data[i + 1] == 0x00 && frame_data[i + 2] == 0x01;
        assert!(!is_sc, "output should not contain Annex B start codes");
    }
}

// --- AUD is stripped; SPS/PPS seed avcC and re-assert at the keyframe ---

#[test]
fn aud_stripped_param_sets_reasserted_at_keyframe() {
    let mut parser = H264Parser::new();

    let mut data = Vec::new();
    // AUD (type 9) — always dropped
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x09);
    data.push(0xF0);
    // SPS (type 7)
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x67);
    data.extend_from_slice(&[0x42, 0x00, 0x1E, 0xAB]);
    // PPS (type 8)
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x68);
    data.extend_from_slice(&[0xCE, 0x01]);
    // IDR (type 5)
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x65);
    data.extend_from_slice(&[0x88, 0x00]);

    let pes = make_pes(data, Some(0));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);

    // SPS/PPS seed avcC...
    assert!(parser.codec_private().is_some(), "SPS/PPS seed avcC");
    // ...and because this is a keyframe, the active SPS/PPS are re-asserted
    // in-band ahead of the IDR so the keyframe is self-contained. AUD (9) is
    // always dropped. Frame data = SPS(7), PPS(8), IDR(5).
    let fd = &frames[0].data;
    let mut types = Vec::new();
    let mut o = 0;
    while o + 4 <= fd.len() {
        let len = u32::from_be_bytes([fd[o], fd[o + 1], fd[o + 2], fd[o + 3]]) as usize;
        o += 4;
        types.push(fd[o] & 0x1F);
        o += len;
    }
    assert_eq!(
        types,
        vec![7, 8, 5],
        "keyframe: SPS+PPS re-asserted ahead of IDR, AUD dropped"
    );
}

// --- PTS conversion ---

#[test]
fn pts_conversion() {
    let mut parser = primed();

    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x41);
    data.extend_from_slice(&[0x00, 0x10]);

    // PTS = 90000 (1 second at 90kHz) → 1_000_000_000 ns
    let pes = make_pes(data, Some(90000));
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

// --- empty PES ---

#[test]
fn parse_empty_pes() {
    let mut parser = H264Parser::new();
    let pes = make_pes(Vec::new(), Some(0));
    let frames = parser.parse(&pes);
    assert!(frames.is_empty());
}

// --- PTS (presentation) used for the MKV block timecode, not DTS ---

#[test]
fn pts_preferred_over_dts() {
    let mut parser = primed();

    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x41);
    data.extend_from_slice(&[0x00, 0x10]);

    let pes = PesPacket {
        source: None,
        pid: 0x1011,
        pts: Some(180000), // 2 seconds (presentation)
        dts: Some(90000),  // 1 second (decode)
        data,
        discontinuity: false,
    };
    let frames = parser.parse(&pes);
    assert_eq!(frames.len(), 1);
    // PTS must be used — MKV block timecodes are presentation timestamps.
    assert_eq!(frames[0].pts_ns, 2_000_000_000);
}

// --- mid-title param-set redefinition emitted in-band ---

/// Collect the NAL types from a length-prefixed frame_data buffer.
fn frame_nal_types(fd: &[u8]) -> Vec<u8> {
    let mut types = Vec::new();
    let mut off = 0;
    while off + 4 <= fd.len() {
        let len = u32::from_be_bytes([fd[off], fd[off + 1], fd[off + 2], fd[off + 3]]) as usize;
        off += 4;
        if off + len > fd.len() {
            break;
        }
        types.push(fd[off] & 0x1F);
        off += len;
    }
    types
}

// The parameter sets a later bare keyframe re-asserts are the ACTIVE ones, not the first-seen
// avcC copy: a player re-applies avcC at every keyframe, so reverting would undo the change.
#[test]
fn a_bare_keyframe_reasserts_the_active_param_sets_not_the_avcc_copy() {
    let nals = |fd: &[u8]| -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut off = 0;
        while off + 4 <= fd.len() {
            let len = u32::from_be_bytes([fd[off], fd[off + 1], fd[off + 2], fd[off + 3]]) as usize;
            out.push(fd[off + 4..off + 4 + len].to_vec());
            off += 4 + len;
        }
        out
    };
    let au = |sps: Option<u8>, pps: Option<u8>, slice: &[u8]| {
        let mut d = Vec::new();
        if let Some(b) = sps {
            d.extend_from_slice(&[0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, b]);
        }
        if let Some(b) = pps {
            d.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, b]);
        }
        d.extend_from_slice(&[0x00, 0x00, 0x01]);
        d.extend_from_slice(slice);
        d
    };
    let mut parser = H264Parser::new();
    parser.parse(&make_pes(
        au(Some(0xAA), Some(0x11), &[0x65, 0x10]),
        Some(0),
    ));
    // Mid-title redefinition of both sets, on a non-keyframe access unit.
    let f = parser.parse(&make_pes(
        au(Some(0xBB), Some(0x22), &[0x41, 0x9A]),
        Some(3000),
    ));
    assert_eq!(frame_nal_types(&f[0].data), vec![7, 8, 1]);
    // A bare IDR: SPS then PPS, both the redefined bodies.
    let f = parser.parse(&make_pes(au(None, None, &[0x65, 0x10]), Some(6000)));
    let n = nals(&f[0].data);
    assert_eq!(frame_nal_types(&f[0].data), vec![7, 8, 5]);
    assert_eq!(n[0].last(), Some(&0xBB), "active SPS, not the avcC copy");
    assert_eq!(n[1].last(), Some(&0x22), "active PPS, not the avcC copy");
}

#[test]
fn slice_types_map_to_coding_types() {
    use CodingType::{B, I, P};
    let expect = [P, B, I, P, I, P, B, I, P, I];
    for (st, want) in expect.into_iter().enumerate() {
        assert_eq!(h264_slice_coding_type(st as u32), Some(want), "{st}");
    }
    assert_eq!(h264_slice_coding_type(10), None);
    assert_eq!(h264_slice_coding_type(u32::MAX), None);
}

#[test]
fn avcc_oversized_pps_returns_none() {
    let mut data = vec![0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAA];
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68]);
    data.extend_from_slice(&vec![0x11u8; 70_000]);
    let mut parser = H264Parser::new();
    parser.parse(&make_pes(data, Some(0)));
    assert!(
        parser.codec_private().is_none(),
        "oversized PPS must not produce a truncated avcC"
    );
}

// ISO 14496-15 §5.3.3.1.2: every profile whose SPS carries chroma_format_idc and bit depths
// gets the 4 avcC extension bytes; Baseline/Main/Extended must not.
#[test]
fn avcc_extension_bytes_follow_exactly_the_chroma_carrying_profiles() {
    for profile in [
        100u8, 110, 122, 144, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135,
    ] {
        let sps = build_high_profile_sps(profile, 1, 0, 0);
        let mut parser = H264Parser::new();
        feed_sps_pps(&mut parser, &sps);
        let cp = parser.codec_private().expect("avcC");
        assert_eq!(cp.len(), sps.len() + 14 + 4, "profile {profile}");
    }
    for profile in [66u8, 77, 88] {
        let sps = build_high_profile_sps(profile, 1, 0, 0);
        let mut parser = H264Parser::new();
        feed_sps_pps(&mut parser, &sps);
        let cp = parser.codec_private().expect("avcC");
        assert_eq!(cp.len(), sps.len() + 14, "profile {profile}");
    }
}

// An IDR whose SPS changed but whose PPS did not: the re-asserted PPS must follow
// the new SPS (§7.3.2.2: a PPS is parsed against its SPS).
#[test]
fn reasserted_pps_follows_a_redefined_sps() {
    let mut parser = H264Parser::new();
    let au = |sps: u8| {
        let mut d = vec![0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, sps];
        d.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]);
        d.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x10, 0x20]);
        d
    };
    parser.parse(&make_pes(au(0xAA), Some(0)));
    let f = parser.parse(&make_pes(au(0xAB), Some(90000)));
    assert_eq!(frame_nal_types(&f[0].data), vec![7, 8, 5]);
}

#[test]
fn keyframes_self_contained_and_redefinition_emitted() {
    let mut parser = H264Parser::new();

    // AU 1: SPS(id0,bodyA) + PPS(id0,bodyA) + IDR. The param sets seed avcC,
    // and because this is a keyframe the active SPS/PPS are re-asserted
    // in-band ahead of the IDR (self-contained keyframe). Frame = SPS,PPS,IDR.
    let mut au1 = Vec::new();
    au1.extend_from_slice(&[0x00, 0x00, 0x01]);
    au1.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]); // SPS body A
    au1.extend_from_slice(&[0x00, 0x00, 0x01]);
    au1.extend_from_slice(&[0x68, 0x11]); // PPS body A
    au1.extend_from_slice(&[0x00, 0x00, 0x01]);
    au1.extend_from_slice(&[0x65, 0x10, 0x20]); // IDR
    let f1 = parser.parse(&make_pes(au1, Some(0)));
    assert_eq!(f1.len(), 1);
    assert_eq!(
        frame_nal_types(&f1[0].data),
        vec![7, 8, 5],
        "AU1 keyframe: SPS+PPS re-asserted ahead of IDR"
    );

    // AU 2: SPS identical to avcC (re-asserted unchanged at the keyframe),
    // PPS REDEFINED (same id, different body) → emitted in-band as a change.
    // Frame = SPS(re-asserted), PPS(redefined), IDR.
    let mut au2 = Vec::new();
    au2.extend_from_slice(&[0x00, 0x00, 0x01]);
    au2.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]); // SPS == body A
    au2.extend_from_slice(&[0x00, 0x00, 0x01]);
    au2.extend_from_slice(&[0x68, 0x22]); // PPS body B (redefinition)
    au2.extend_from_slice(&[0x00, 0x00, 0x01]);
    au2.extend_from_slice(&[0x65, 0x30, 0x40]); // IDR
    let f2 = parser.parse(&make_pes(au2, Some(90000)));
    assert_eq!(f2.len(), 1);
    let types = frame_nal_types(&f2[0].data);
    assert_eq!(types, vec![7, 8, 5], "got {types:?}");
    // Confirm the in-band PPS is the REDEFINED body B (0x22), not avcC's A.
    let mut o = 0;
    let mut pps_body = None;
    while o + 4 <= f2[0].data.len() {
        let len = u32::from_be_bytes([
            f2[0].data[o],
            f2[0].data[o + 1],
            f2[0].data[o + 2],
            f2[0].data[o + 3],
        ]) as usize;
        o += 4;
        if f2[0].data[o] & 0x1F == 8 {
            pps_body = Some(f2[0].data[o + 1]);
        }
        o += len;
    }
    assert_eq!(
        pps_body,
        Some(0x22),
        "in-band PPS must be the redefined body B"
    );
}

#[test]
fn repeated_identical_param_sets_reasserted_each_keyframe() {
    let mut parser = H264Parser::new();
    let mut au = Vec::new();
    au.extend_from_slice(&[0x00, 0x00, 0x01]);
    au.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]);
    au.extend_from_slice(&[0x00, 0x00, 0x01]);
    au.extend_from_slice(&[0x68, 0x11]);
    au.extend_from_slice(&[0x00, 0x00, 0x01]);
    au.extend_from_slice(&[0x65, 0x10]);
    // Two identical AUs, each a keyframe: each re-asserts the active SPS/PPS
    // in-band even though unchanged, so a decoder that dropped them at a
    // reset recovers at every IDR. Frame = SPS, PPS, IDR.
    parser.parse(&make_pes(au.clone(), Some(0)));
    let f = parser.parse(&make_pes(au, Some(90000)));
    assert_eq!(
        frame_nal_types(&f[0].data),
        vec![7, 8, 5],
        "each keyframe re-asserts the active SPS/PPS in-band"
    );
}

#[test]
fn many_empty_nals_do_not_overflow_stack() {
    // Regression: NalIterator::next must iterate, not recurse, over empty
    // NALs, or tens of thousands of adjacent start codes (each an empty NAL)
    // would blow the stack under the old tail-recursive implementation.
    let mut data = Vec::new();
    // 50_000 back-to-back 3-byte start codes → 50_000 empty NALs.
    for _ in 0..50_000 {
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
    }
    // One real NAL at the end so the iterator yields something.
    data.extend_from_slice(&[0x41, 0xAA, 0xBB]);

    let mut parser = primed();
    let frames = parser.parse(&make_pes(data, Some(0)));
    // Exactly one populated frame; the empty NALs are skipped without
    // overflowing.
    assert_eq!(frames.len(), 1);
    let fd = &frames[0].data;
    let len = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]) as usize;
    assert_eq!(len, 3, "the single real NAL is length-prefixed");
    assert_eq!(fd[4], 0x41);
}

// --- avcC exact byte layout (ISO 14496-15 §5.2.4.1) ---

#[test]
fn avcc_exact_length_fields_and_payload() {
    // Validates the fixed AVCDecoderConfigurationRecord header/length-field
    // layout using a Main-Profile SPS (profile_idc=0x4D=77) so no High-Profile
    // extension bytes are appended (see avcc_high_profile_appends_extension_bytes).
    let mut parser = H264Parser::new();
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.extend_from_slice(&[0x67, 0x4D, 0x00, 0x28, 0xAB, 0xCD]); // SPS, 6 bytes, Main Profile (77)
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.extend_from_slice(&[0x68, 0xEE, 0x3C]); // PPS, 3 bytes
    // A slice so a frame is produced (not required for codec_private though).
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x11]);
    parser.parse(&make_pes(data, Some(0)));

    let cp = parser.codec_private().expect("avcC");
    // Fixed header.
    assert_eq!(cp[0], 1, "configurationVersion");
    assert_eq!(cp[1], 0x4D, "AVCProfileIndication = SPS[1]");
    assert_eq!(cp[2], 0x00, "profile_compatibility = SPS[2]");
    assert_eq!(cp[3], 0x28, "AVCLevelIndication = SPS[3]");
    assert_eq!(cp[4], 0xFF, "lengthSizeMinusOne nibble (4-byte prefix)");
    assert_eq!(cp[5], 0xE1, "numSPS = 1");
    // sequenceParameterSetLength (16-bit BE) = 6.
    assert_eq!(u16::from_be_bytes([cp[6], cp[7]]), 6, "SPS length field");
    // SPS body follows verbatim.
    assert_eq!(&cp[8..14], &[0x67, 0x4D, 0x00, 0x28, 0xAB, 0xCD]);
    // numPPS = 1.
    assert_eq!(cp[14], 1, "numPPS");
    // pictureParameterSetLength (16-bit BE) = 3.
    assert_eq!(u16::from_be_bytes([cp[15], cp[16]]), 3, "PPS length field");
    // PPS body verbatim.
    assert_eq!(&cp[17..20], &[0x68, 0xEE, 0x3C]);
    // Record length is exactly the sum of its parts — no extension bytes for Main Profile.
    assert_eq!(cp.len(), 20);
}

#[test]
fn avcc_none_when_sps_shorter_than_four_bytes() {
    // codec_private reads SPS[1..=3] for profile/compat/level, so an SPS
    // shorter than 4 bytes can't form a valid avcC → None (guard
    // `sps.len() < 4`). A 3-byte SPS (header + 2 bytes) triggers it.
    let mut parser = H264Parser::new();
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x67, 0x42]); // SPS = 2 bytes
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]); // PPS
    parser.parse(&make_pes(data, Some(0)));
    assert!(
        parser.codec_private().is_none(),
        "SPS < 4 bytes must not yield an avcC"
    );
}

#[test]
fn avcc_none_with_sps_but_no_pps() {
    // Both SPS and PPS are required. SPS only → None.
    let mut parser = H264Parser::new();
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAA]);
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x10]); // IDR, no PPS
    parser.parse(&make_pes(data, Some(0)));
    assert!(parser.codec_private().is_none());
}

// --- NAL type extraction: forbidden_zero_bit + nal_ref_idc are masked ---

#[test]
fn nal_type_masks_high_three_bits() {
    // nal_type = byte0 & 0x1F. forbidden_zero_bit (bit 7) and nal_ref_idc
    // (bits 6-5) must not affect type detection: 0x65 (ref_idc=3) and 0x25
    // (ref_idc=1) are both IDR (type 5), both keyframes.
    for idr_hdr in [0x65u8, 0x25, 0x05, 0x85] {
        let mut parser = H264Parser::new();
        let data = vec![0x00, 0x00, 0x01, idr_hdr, 0x10, 0x20];
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert!(
            f[0].keyframe,
            "header {idr_hdr:#x} is NAL type 5 (IDR) → keyframe"
        );
    }
}

#[test]
fn sps_recognized_regardless_of_ref_idc() {
    // SPS is type 7; header 0x67 (ref_idc 3) and 0x27 (ref_idc 1) are both
    // SPS and must seed codec_private identically.
    for sps_hdr in [0x67u8, 0x27] {
        let mut parser = H264Parser::new();
        let mut data = vec![0x00, 0x00, 0x01, sps_hdr, 0x42, 0x00, 0x1E, 0xAA];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]); // PPS
        parser.parse(&make_pes(data, Some(0)));
        let cp = parser.codec_private().expect("avcC");
        assert_eq!(cp[1], 0x42, "profile from SPS[1] regardless of ref_idc");
    }
}

// --- 4-byte start code handling ---

#[test]
fn four_byte_start_code_parsed() {
    // A 4-byte start code (00 00 00 01) must be skipped correctly so the NAL
    // body begins at the right offset (skip_start_code returns pos+4).
    let mut parser = primed();
    let data = vec![0x00, 0x00, 0x00, 0x01, 0x41, 0xAA, 0xBB];
    let f = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(f.len(), 1);
    let len = u32::from_be_bytes([f[0].data[0], f[0].data[1], f[0].data[2], f[0].data[3]]);
    // NAL = 0x41 0xAA 0xBB = 3 bytes (trailing 0xBB kept; not a zero).
    assert_eq!(len, 3);
    assert_eq!(&f[0].data[4..], &[0x41, 0xAA, 0xBB]);
}

#[test]
fn trailing_zeros_of_next_start_code_stripped_from_nal() {
    // The byte(s) before a following 4-byte start code (00 00 00 01) are
    // leading zeros of that start code, not RBSP, and must be stripped from
    // the current NAL — NAL 1 must not absorb the extra 00.
    let mut parser = primed();
    let mut data = vec![0x00, 0x00, 0x01, 0x41, 0xAA]; // NAL1 = 0x41 0xAA
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x41, 0xBB]); // 4-byte SC
    let f = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(f.len(), 1);
    // Walk length-prefixed NALs; first must be exactly 2 bytes (0x41 0xAA),
    // NOT 3 (it must not swallow the leading 0x00 of the next start code).
    let len1 = u32::from_be_bytes([f[0].data[0], f[0].data[1], f[0].data[2], f[0].data[3]]);
    assert_eq!(len1, 2, "NAL1 must not absorb the next start code's zeros");
    assert_eq!(&f[0].data[4..6], &[0x41, 0xAA]);
}

#[test]
fn aud_dropped_but_following_slice_kept() {
    // AUD (type 9) is dropped from frame data; a following slice survives.
    let mut parser = primed();
    let mut data = vec![0x00, 0x00, 0x01, 0x09, 0xF0]; // AUD
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x41, 0xAA, 0xBB]); // slice
    let f = parser.parse(&make_pes(data, Some(0)));
    assert_eq!(f.len(), 1);
    assert_eq!(
        frame_nal_types(&f[0].data),
        vec![1],
        "only the slice remains"
    );
}

#[test]
fn param_set_only_pes_emits_no_frame() {
    // A PES carrying ONLY SPS+PPS (both stripped into avcC) has no in-band
    // NAL → frame_data empty → no frame emitted (mirrors HEVC/MPEG2/VC1).
    let mut parser = H264Parser::new();
    let mut data = vec![0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAA];
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0x11]);
    let f = parser.parse(&make_pes(data, Some(0)));
    assert!(f.is_empty(), "param-set-only PES emits no frame");
    // But the avcC is captured.
    assert!(parser.codec_private().is_some());
}

#[test]
fn dts_fallback_when_pts_absent() {
    // PTS absent → DTS is used (or().map). pts.or(dts) per the comment.
    let mut parser = primed();
    let pes = PesPacket {
        source: None,
        pid: 0x1011,
        pts: None,
        dts: Some(90000),
        data: vec![0x00, 0x00, 0x01, 0x41, 0x10],
        discontinuity: false,
    };
    let f = parser.parse(&pes);
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].pts_ns, 1_000_000_000, "falls back to DTS");
}

#[test]
fn no_pts_no_dts_defaults_zero() {
    let mut parser = primed();
    let pes = PesPacket {
        source: None,
        pid: 0x1011,
        pts: None,
        dts: None,
        data: vec![0x00, 0x00, 0x01, 0x41, 0x10],
        discontinuity: false,
    };
    let f = parser.parse(&pes);
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].pts_ns, 0);
}

#[test]
fn no_start_code_emits_nothing() {
    // A PES with no Annex B start code yields no NAL → no frame (NalIterator
    // starts at data.len()).
    let mut parser = H264Parser::new();
    let f = parser.parse(&make_pes(vec![0x41, 0xAA, 0xBB, 0xCC], Some(0)));
    assert!(f.is_empty(), "no start code → no NAL → no frame");
}

#[test]
fn avcc_oversized_param_set_returns_none() {
    // A param set > 65535 bytes can't be length-encoded in avcC's 16-bit
    // field; codec_private must refuse rather than emit a truncated record.
    let mut parser = H264Parser::new();
    let mut data = Vec::new();
    // Oversized SPS (header byte 0x67 + 70000 filler bytes).
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.push(0x67);
    data.extend_from_slice(&vec![0x11u8; 70_000]);
    // PPS
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.extend_from_slice(&[0x68, 0x11]);
    parser.parse(&make_pes(data, Some(0)));
    assert!(
        parser.codec_private().is_none(),
        "oversized SPS must not produce a truncated avcC"
    );
}

// --- High Profile avcC extension (ISO 14496-15 §5.3.3.1.2) ---
// Build a minimal High-Profile SPS RBSP: NAL header + profile/constraint/
// level + ue(v) fields through bit_depth_chroma_minus8, bit-packed manually.
fn build_high_profile_sps(
    profile_idc: u8,
    chroma_format_idc: u32,
    bit_depth_luma_minus8: u32,
    bit_depth_chroma_minus8: u32,
) -> Vec<u8> {
    // Bit-pack the ue(v) fields into a byte buffer after the fixed header.
    // We append bits MSB-first into a growing Vec<u8>.
    struct BitWriter {
        buf: Vec<u8>,
        cur: u8,
        bits: u8, // bits accumulated in `cur` (0..8)
    }
    impl BitWriter {
        fn new() -> Self {
            Self {
                buf: Vec::new(),
                cur: 0,
                bits: 0,
            }
        }
        fn push_bit(&mut self, bit: u8) {
            self.cur = (self.cur << 1) | (bit & 1);
            self.bits += 1;
            if self.bits == 8 {
                self.buf.push(self.cur);
                self.cur = 0;
                self.bits = 0;
            }
        }
        fn write_ue(&mut self, val: u32) {
            // Exp-Golomb encode: find k such that 2^k - 1 <= val, then
            // k leading zeros + 1 stop + k-bit suffix.
            if val == 0 {
                self.push_bit(1);
                return;
            }
            let code = val + 1; // code = val + 1, k = floor(log2(code))
            let k = 31 - code.leading_zeros();
            for _ in 0..k {
                self.push_bit(0);
            } // k leading zeros
            self.push_bit(1); // stop bit
            for i in (0..k).rev() {
                self.push_bit(((code >> i) & 1) as u8);
            }
        }
        fn finish(mut self) -> Vec<u8> {
            // Flush partial byte (padding with zeros on the right — RBSP
            // trailing bits pattern, sufficient for our test payload).
            if self.bits > 0 {
                self.cur <<= 8 - self.bits;
                self.buf.push(self.cur);
            }
            self.buf
        }
    }

    let mut w = BitWriter::new();
    w.write_ue(0); // seq_parameter_set_id = 0
    w.write_ue(chroma_format_idc);
    if chroma_format_idc == 3 {
        w.push_bit(0); // separate_colour_plane_flag = 0
    }
    w.write_ue(bit_depth_luma_minus8);
    w.write_ue(bit_depth_chroma_minus8);
    let payload = w.finish();

    let mut sps = vec![
        0x67, // NAL header (type=7)
        profile_idc,
        0x00, // constraint flags
        0x28, // level_idc = 4.0
    ];
    sps.extend_from_slice(&payload);
    sps
}

#[test]
fn dts_info_walks_chroma_block_for_profile_144() {
    // sps_id 0, chroma 1, depths 0/0, no bypass/scaling, log2_max_frame_num_minus4 0,
    // poc type 2, 1 ref, no gaps, 1x1 MB, frame_mbs_only, direct8x8, no crop/VUI, stop.
    let bits = "1010110010110100111100 1".replace(' ', "");
    let mut bytes = vec![0x67, 144, 0x00, 0x28];
    let padded = format!("{bits:0<32}");
    for c in padded.as_bytes().chunks(8) {
        bytes.push(u8::from_str_radix(std::str::from_utf8(c).unwrap(), 2).unwrap());
    }
    let info = parse_sps_dts_info(&bytes).expect("sps parses");
    assert_eq!(info.log2_max_frame_num, 4);
}

fn feed_sps_pps(parser: &mut H264Parser, sps_bytes: &[u8]) {
    // Feed a PES containing: custom SPS + a minimal PPS + an IDR slice.
    let mut data = Vec::new();
    data.extend_from_slice(&[0x00, 0x00, 0x01]);
    data.extend_from_slice(sps_bytes);
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x68, 0xCE, 0x01]); // PPS
    data.extend_from_slice(&[0x00, 0x00, 0x01, 0x65, 0x88]); // IDR
    parser.parse(&make_pes(data, Some(0)));
}

/// ISO 14496-15 §5.3.3.1.2 regression: a High-Profile SPS (profile_idc=100)
/// must produce an avcC with the 4 extension bytes (chroma_format_idc,
/// bit_depth_luma_minus8, bit_depth_chroma_minus8, num_sps_ext=0).
#[test]
fn avcc_high_profile_appends_extension_bytes() {
    // profile_idc=100 (High), chroma_format_idc=1 (4:2:0), depths both 0.
    let sps = build_high_profile_sps(100, 1, 0, 0);
    let mut parser = H264Parser::new();
    feed_sps_pps(&mut parser, &sps);

    let cp = parser.codec_private().expect("avcC must be present");

    // Locate extension bytes past the fixed record: 6-byte header + 2-byte
    // SPS length + SPS body + 1-byte numPPS + 2-byte PPS length + 3-byte PPS
    // body (0x68,0xCE,0x01) = sps.len() + 14.
    let ext_off = sps.len() + 14;
    assert!(
        cp.len() == ext_off + 4,
        "High-Profile avcC must have exactly 4 extension bytes (len={}, expected {})",
        cp.len(),
        ext_off + 4
    );

    // Byte 0: 111111xx — upper 6 bits reserved (0b111111), lower 2 = chroma_format_idc=1.
    assert_eq!(
        cp[ext_off] & 0xFC,
        0xFC,
        "extension byte 0: reserved bits must be 111111xx"
    );
    assert_eq!(cp[ext_off] & 0x03, 1, "chroma_format_idc must be 1 (4:2:0)");
    // Byte 1: 11111xxx — upper 5 bits reserved, lower 3 = bit_depth_luma_minus8=0.
    assert_eq!(
        cp[ext_off + 1] & 0xF8,
        0xF8,
        "extension byte 1: reserved bits must be 11111xxx"
    );
    assert_eq!(cp[ext_off + 1] & 0x07, 0, "bit_depth_luma_minus8 must be 0");
    // Byte 2: 11111xxx — upper 5 bits reserved, lower 3 = bit_depth_chroma_minus8=0.
    assert_eq!(
        cp[ext_off + 2] & 0xF8,
        0xF8,
        "extension byte 2: reserved bits must be 11111xxx"
    );
    assert_eq!(
        cp[ext_off + 2] & 0x07,
        0,
        "bit_depth_chroma_minus8 must be 0"
    );
    // Byte 3: num_of_sequence_parameter_set_ext = 0.
    assert_eq!(
        cp[ext_off + 3],
        0,
        "num_of_sequence_parameter_set_ext must be 0"
    );
}

/// ISO 14496-15 §5.3.3.1.2 regression: a High-Profile SPS with non-zero
/// chroma_format_idc and bit depths carries those values correctly in the
/// extension bytes.
#[test]
fn avcc_high_profile_extension_carries_correct_values() {
    // profile_idc=100, chroma_format_idc=3 (4:4:4), depth_luma=2, depth_chroma=5
    // (unequal, so a luma/chroma swap shows).
    let sps = build_high_profile_sps(100, 3, 2, 5);
    let mut parser = H264Parser::new();
    feed_sps_pps(&mut parser, &sps);

    let cp = parser.codec_private().expect("avcC");
    let ext_off = sps.len() + 14;

    assert_eq!(cp[ext_off] & 0x03, 3, "chroma_format_idc must be 3 (4:4:4)");
    assert_eq!(cp[ext_off + 1] & 0x07, 2, "bit_depth_luma_minus8 must be 2");
    assert_eq!(
        cp[ext_off + 2] & 0x07,
        5,
        "bit_depth_chroma_minus8 must be 5"
    );
    assert_eq!(
        cp[ext_off + 3],
        0,
        "num_of_sequence_parameter_set_ext must be 0"
    );
}

// ISO 14496-15 §5.3.3.1.2 regression: profile_idc=244 (High 4:4:4 Predictive) also mandates
// the chroma/bit-depth extension; it was missing from HIGH_PROFILES.
#[test]
fn avcc_profile_244_appends_extension_bytes() {
    // profile_idc=244, chroma_format_idc=3 (4:4:4), depths both 4 (12-bit).
    let sps = build_high_profile_sps(244, 3, 4, 4);
    let mut parser = H264Parser::new();
    feed_sps_pps(&mut parser, &sps);

    let cp = parser.codec_private().expect("avcC must be present");
    let ext_off = sps.len() + 14;
    assert_eq!(
        cp.len(),
        ext_off + 4,
        "profile 244 avcC must have the 4 extension bytes (len={}, expected {})",
        cp.len(),
        ext_off + 4
    );
    assert_eq!(cp[ext_off] & 0x03, 3, "chroma_format_idc must be 3 (4:4:4)");
    assert_eq!(cp[ext_off + 1] & 0x07, 4, "bit_depth_luma_minus8 must be 4");
    assert_eq!(
        cp[ext_off + 2] & 0x07,
        4,
        "bit_depth_chroma_minus8 must be 4"
    );
}

/// ISO 14496-15 §5.3.3.1.2 regression: a Main-Profile SPS (profile_idc=77)
/// must NOT have the extension bytes — strict parsers reject trailing bytes
/// for Baseline/Main/Extended profiles.
#[test]
fn avcc_main_profile_no_extension_bytes() {
    // profile_idc=77 (Main). No High-Profile branch in the SPS RBSP,
    // so we build a simpler SPS: NAL header + profile/compat/level + a
    // ue(v) seq_parameter_set_id=0 + remaining RBSP (can be trivial).
    let sps = vec![
        0x67, // NAL header (type=7)
        77,   // profile_idc = Main
        0x40, // constraint flags
        0x28, // level_idc
        // seq_parameter_set_id=0 → ue(v) = 0b1 (1 bit).  Pack into a byte:
        // bit pattern: 1000_0000 (stop bit in MSB, rest don't-care)
        0x80,
    ];
    let mut parser = H264Parser::new();
    feed_sps_pps(&mut parser, &sps);

    let cp = parser.codec_private().expect("avcC must be present");
    // Fixed record: 6 + 2 + sps.len() + 1 + 2 + 3 = sps.len() + 14.
    let expected_len = sps.len() + 14;
    assert_eq!(
        cp.len(),
        expected_len,
        "Main-Profile avcC must NOT have extension bytes (len={}, expected {})",
        cp.len(),
        expected_len
    );
}

// `SpsReader::read_bits` shifts each new bit into the low end
// (`val << 1 | bit`); pins the direction against a `<<` -> `>>` typo.
#[test]
fn sps_reader_read_bits_builds_value_msb_first() {
    // 0b1011_0000 read 4 bits MSB-first -> 0b1011 = 11.
    let mut r = super::SpsReader::new(&[0b1011_0000]);
    assert_eq!(r.read_bits(4), Some(0b1011));
}

// `SpsReader::read_ue`'s truncation guard is `leading_zeros > 31`: the
// longest legal code must decode, not abort. Mirrors `BitReader` in
// `startcode.rs`; kept separate since `SpsReader` is unshared.
#[test]
fn sps_reader_read_ue_thirty_one_leading_zeros_is_still_valid() {
    let data = [0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
    let mut r = super::SpsReader::new(&data);
    assert_eq!(r.read_ue(), Some(u32::MAX >> 1));
}
