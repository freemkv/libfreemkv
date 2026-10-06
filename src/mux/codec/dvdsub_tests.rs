use super::*;
use crate::mux::ts::PesPacket;

fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
    PesPacket {
        source: None,
        pid: 0x1200,
        pts,
        dts: None,
        data,
        discontinuity: false,
    }
}

#[test]
fn post_gap_continuation_is_not_spliced_onto_pending_spu() {
    let mut parser = DvdSubParser::new(None);
    let head = vec![0x00, 0x08, 0xDE, 0xAD];
    assert!(parser.parse(&make_pes(head, Some(90000))).is_empty());
    let mut cont = make_pes(vec![1, 2, 3, 4], None);
    cont.discontinuity = true;
    assert!(parser.parse(&cont).is_empty(), "no spliced SPU emitted");
    assert!(parser.flush().is_empty());
}

#[test]
fn passthrough_data() {
    let mut parser = DvdSubParser::new(None);
    let sub_data = vec![0x00, 0x0A, 0x00, 0x08, 0x01, 0xFF, 0x02, 0x03, 0x04, 0x05];
    let pes = make_pes(sub_data.clone(), Some(90000));
    let frames = parser.parse(&pes);

    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].data, sub_data,
        "VobSub data should pass through unmodified"
    );
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

#[test]
fn always_keyframe() {
    let mut parser = DvdSubParser::new(None);
    for i in 0..3u8 {
        let data = vec![0x00, i, 0x00, i + 1];
        let pes = make_pes(data, Some(90000 * i as i64));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert!(
            frames[0].keyframe,
            "DVD subtitle frames should always be keyframes"
        );
    }
}

#[test]
fn empty_pes_returns_no_frames() {
    let mut parser = DvdSubParser::new(None);
    let pes = make_pes(Vec::new(), Some(0));
    assert!(parser.parse(&pes).is_empty());
}

#[test]
fn codec_private_none_by_default() {
    let parser = DvdSubParser::new(None);
    assert!(parser.codec_private().is_none());
}

#[test]
fn codec_private_returns_palette_when_set() {
    let palette_data = b"palette: 000000, ffffff\n".to_vec();
    let parser = DvdSubParser::new(Some(palette_data.clone()));
    let cp = parser.codec_private();
    assert!(cp.is_some());
    assert_eq!(cp.unwrap(), palette_data);
}

#[test]
fn no_pts_orphan_with_no_pending_is_dropped() {
    // Orphan (no pending, no PTS), even when "complete" on arrival: has
    // no real start time, so it's dropped, not emitted at pts 0 (L054).
    let mut parser = DvdSubParser::new(None);
    let pes = make_pes(vec![0x00, 0x02], None);
    let frames = parser.parse(&pes);
    assert!(frames.is_empty(), "orphan no-PTS PES is dropped");
    assert!(parser.pending.is_none());
}

#[test]
fn multi_pes_spu_reassembled() {
    let mut parser = DvdSubParser::new(None);
    // Declared SPU_size = 12 bytes total. First PES carries the 2 size
    // bytes + 4 payload bytes and the only PTS; the next two PESs are
    // continuations with PTS=0.
    let head = vec![0x00, 0x0C, 0xAA, 0xBB, 0xCC, 0xDD];
    let cont1 = vec![0x11, 0x22, 0x33];
    let cont2 = vec![0x44, 0x55, 0x66];

    let f = parser.parse(&make_pes(head.clone(), Some(90000)));
    assert!(f.is_empty(), "incomplete SPU should not emit yet");
    // Continuations carry NO PTS (None), per the PS demuxer.
    let f = parser.parse(&make_pes(cont1.clone(), None));
    assert!(f.is_empty(), "still incomplete");
    let frames = parser.parse(&make_pes(cont2.clone(), None));
    assert_eq!(frames.len(), 1, "completed SPU emits exactly one frame");

    // Reassembled bytes = head + cont1 + cont2, in order.
    let mut expected = head;
    expected.extend_from_slice(&cont1);
    expected.extend_from_slice(&cont2);
    assert_eq!(frames[0].data, expected);
    // PTS inherited from the head PES (1s = 1e9 ns), not the PTS=0 tails.
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
    assert!(frames[0].keyframe);
}

#[test]
fn flush_emits_truncated_trailing_spu() {
    let mut parser = DvdSubParser::new(None);
    // Declared 100 bytes but only 6 ever arrive before EOF.
    let head = vec![0x00, 0x64, 0xDE, 0xAD, 0xBE, 0xEF];
    let f = parser.parse(&make_pes(head.clone(), Some(90000)));
    assert!(f.is_empty(), "incomplete SPU should not emit during parse");
    let frames = parser.flush();
    assert_eq!(frames.len(), 1, "EOF flush emits the partial SPU");
    assert_eq!(frames[0].data, head);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

#[test]
fn real_pts_pes_force_emits_stale_pending_and_starts_new_spu() {
    // A lost continuation leaves an incomplete pending SPU. The NEXT real
    // subtitle arrives with its own PTS — it must force-emit the stuck unit
    // (truncated) and begin a fresh SPU, not be appended as a continuation.
    let mut parser = DvdSubParser::new(None);

    // SPU 1 declares 100 bytes but only 6 arrive; the continuation is lost.
    let head1 = vec![0x00, 0x64, 0xDE, 0xAD, 0xBE, 0xEF];
    assert!(
        parser
            .parse(&make_pes(head1.clone(), Some(90000)))
            .is_empty(),
        "SPU 1 incomplete, held pending"
    );

    // SPU 2 arrives with a real PTS — declares 4 bytes, fully present.
    let head2 = vec![0x00, 0x04, 0x11, 0x22];
    let frames = parser.parse(&make_pes(head2.clone(), Some(180000)));
    // First the truncated stale SPU 1, then complete SPU 2.
    assert_eq!(frames.len(), 2, "stale flushed + new emitted");
    assert_eq!(frames[0].data, head1, "stale SPU 1 emitted truncated");
    assert_eq!(frames[0].pts_ns, 1_000_000_000, "SPU 1 keeps its PTS");
    assert_eq!(frames[1].data, head2, "SPU 2 emitted fresh");
    assert_eq!(frames[1].pts_ns, 2_000_000_000, "SPU 2 keeps its own PTS");
}

#[test]
fn corrupt_oversized_size_recovers_on_next_real_pts() {
    // A corrupt SPU_size that real data never reaches must not swallow every
    // later subtitle. The next real-PTS PES resets pending and recovers the
    // track.
    let mut parser = DvdSubParser::new(None);

    // Declares 0xFFFF but only a few bytes ever arrive (corrupt size).
    let bad = vec![0xFF, 0xFF, 0x01, 0x02, 0x03];
    assert!(parser.parse(&make_pes(bad.clone(), Some(90000))).is_empty());
    // A no-PTS stray continuation appends (still stuck under the bad size).
    assert!(parser.parse(&make_pes(vec![0x04, 0x05], None)).is_empty());

    // Next real subtitle (PTS present) recovers: stale flushed + new SPU.
    let good = vec![0x00, 0x04, 0xAA, 0xBB];
    let frames = parser.parse(&make_pes(good.clone(), Some(270000)));
    assert_eq!(frames.len(), 2, "track recovers, not swallowed to EOF");
    assert_eq!(frames[1].data, good);
    assert_eq!(frames[1].pts_ns, 3_000_000_000);
}

#[test]
fn declared_size_below_two_passes_through_as_lone_frame() {
    // SPU_size includes its own 2-byte header, so a declared size < 2 is
    // malformed. It must pass through as a lone frame, not emit an oversized
    // unit or get stuck pending.
    let mut parser = DvdSubParser::new(None);
    let data = vec![0x00, 0x00, 0xAB, 0xCD]; // declared = 0
    let frames = parser.parse(&make_pes(data.clone(), Some(90000)));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, data, "passed through whole");
    assert!(parser.pending.is_none(), "no pending left open");
}

// ── YCbCr → RGB conversion tests ──────────────────────────────────────

#[test]
fn ycbcr_to_rgb_white() {
    // Studio-range white: Y=235 at neutral chroma expands to full white.
    let color = [0x00, 235, 128, 128];
    assert_eq!(ycbcr_to_rgb(&color), [255, 255, 255]);
}

#[test]
fn ycbcr_to_rgb_black() {
    // Studio-range black: Y=16 at neutral chroma is true black.
    let color = [0x00, 16, 128, 128];
    assert_eq!(ycbcr_to_rgb(&color), [0, 0, 0]);
}

#[test]
fn ycbcr_to_rgb_clamps_overflow() {
    // Y=255, Cr=255 → R would be 255 + 1.402*127 = ~433, should clamp to 255.
    // Cr is byte 2 in the on-disc [pad, Y, Cr, Cb] layout.
    let color = [0x00, 255, 255, 128];
    let [r, _g, _b] = ycbcr_to_rgb(&color);
    assert_eq!(r, 255);
}

#[test]
fn ycbcr_to_rgb_clamps_underflow() {
    // Y=0, Cr=0 → R = 0 + 1.402*(0-128) = -179, should clamp to 0.
    // Cr is byte 2 in the on-disc [pad, Y, Cr, Cb] layout.
    let color = [0x00, 0, 0, 128];
    let [r, _g, _b] = ycbcr_to_rgb(&color);
    assert_eq!(r, 0);
}

#[test]
fn ycbcr_to_rgb_red() {
    // Approximate red: Y=82, Cr=240, Cb=90 — on disc as [pad, Y, Cr, Cb].
    let color = [0x00, 82, 240, 90];
    let [r, g, b] = ycbcr_to_rgb(&color);
    assert!(r > 200, "R should be high for red, got {}", r);
    assert!(g < 30, "G should be low for red, got {}", g);
    assert!(b < 30, "B should be low for red, got {}", b);
}

// Real on-disc red entry (byte 2=Cr, byte 3=Cb); catches a chroma-byte swap.
// `_white`/`_black` can't catch this (Cb=Cr=128 is a no-op swap).
#[test]
fn ycbcr_to_rgb_reads_byte2_as_cr_and_byte3_as_cb() {
    // On-disc [pad, Y, Cr, Cb] for saturated red.
    let on_disc_red = [0x00u8, 76, 255, 85];
    let [r, g, b] = ycbcr_to_rgb(&on_disc_red);

    assert!(
        r > 200 && b < 60,
        "on-disc red [0,Y=76,Cr=255,Cb=85] must render red-dominant, \
             got R={r} G={g} B={b} (R and B swapped => byte 2/3 are transposed)"
    );
    assert_eq!([r, g, b], [255, 0, 0], "exact studio-range BT.601 red");

    // And the converse: a saturated BLUE on-disc entry (Y=29, Cr=107, Cb=255)
    // must not come out red.
    let on_disc_blue = [0x00u8, 29, 107, 255];
    let [r2, g2, b2] = ycbcr_to_rgb(&on_disc_blue);
    assert!(
        b2 > 200 && r2 < 60,
        "on-disc blue [0,Y=29,Cr=107,Cb=255] must render blue-dominant, \
             got R={r2} G={g2} B={b2}"
    );
}

// ── Palette formatting tests ──────────────────────────────────────────

#[test]
fn format_palette_basic() {
    // Two colors: black and white (at neutral chroma)
    let palette = vec![
        [0x00, 0, 128, 128],   // Y=0 → RGB (0,0,0)
        [0x00, 255, 128, 128], // Y=255 → RGB (255,255,255)
    ];
    let result = format_palette(&palette, 0, 0);
    let text = String::from_utf8(result).unwrap();
    assert!(
        text.starts_with("palette: "),
        "should start with 'palette: '"
    );
    assert!(text.ends_with('\n'), "should end with newline");
    // First color: 000000
    assert!(
        text.contains("000000"),
        "black should be 000000, got: {}",
        text
    );
    // Second color: ffffff
    assert!(
        text.contains("ffffff"),
        "white should be ffffff, got: {}",
        text
    );
}

#[test]
fn format_palette_16_colors() {
    let palette: Vec<[u8; 4]> = (0..16).map(|i| [0x00, (i * 16) as u8, 128, 128]).collect();
    let result = format_palette(&palette, 0, 0);
    let text = String::from_utf8(result).unwrap();
    // Should have exactly 15 commas (16 colors separated by ", ")
    let comma_count = text.matches(", ").count();
    assert_eq!(
        comma_count, 15,
        "16 colors should have 15 separators, got {}",
        comma_count
    );
}

#[test]
fn format_palette_hex_format() {
    // Y=126 at neutral chroma → R=G=B=128 → "808080"
    let palette = vec![[0x00, 126, 128, 128]];
    let result = format_palette(&palette, 0, 0);
    let text = String::from_utf8(result).unwrap();
    assert_eq!(text, "palette: 808080\n");
}

#[test]
fn format_palette_emits_size_line_before_palette() {
    // With non-zero dimensions the `.idx` `size:` line is prepended ahead of
    // the palette so players place/scale the VobSub bitmap (PAL 720x576).
    let palette = vec![[0x00, 126, 128, 128]];
    let result = format_palette(&palette, 720, 576);
    let text = String::from_utf8(result).unwrap();
    assert_eq!(
        text, "size: 720x576\npalette: 808080\n",
        "size: line must precede palette: line"
    );
}

#[test]
fn format_palette_omits_size_line_when_dimensions_unknown() {
    // 0 width/height (unknown resolution) omits the size line rather than
    // emitting a 0x0 frame; the palette line is still present.
    let palette = vec![[0x00, 126, 128, 128]];
    let result = format_palette(&palette, 0, 576);
    let text = String::from_utf8(result).unwrap();
    assert_eq!(text, "palette: 808080\n", "no size line when a dim is 0");
}

// --- SPU_size boundary: completes exactly at declared size ---

#[test]
fn spu_completes_exactly_at_declared_size() {
    // SPU_size is the total byte length including the 2-byte header. When the
    // accumulated bytes reach exactly the declared size, the unit emits.
    // Declared = 6, head carries all 6 → emits immediately on the head PES.
    let mut parser = DvdSubParser::new(None);
    let head = vec![0x00, 0x06, 0xAA, 0xBB, 0xCC, 0xDD]; // 6 bytes, declared 6
    let f = parser.parse(&make_pes(head.clone(), Some(90000)));
    assert_eq!(f.len(), 1, "complete-on-arrival SPU emits at once");
    assert_eq!(f[0].data, head);
    assert!(parser.pending.is_none(), "nothing left pending");
}

#[test]
fn spu_one_byte_short_waits_then_completes() {
    // Declared 7 but head has 6 → held; a 1-byte continuation completes it.
    let mut parser = DvdSubParser::new(None);
    let head = vec![0x00, 0x07, 0xAA, 0xBB, 0xCC, 0xDD]; // 6 of 7
    assert!(
        parser
            .parse(&make_pes(head.clone(), Some(90000)))
            .is_empty()
    );
    let f = parser.parse(&make_pes(vec![0xEE], None)); // continuation
    assert_eq!(f.len(), 1);
    let mut expect = head;
    expect.push(0xEE);
    assert_eq!(
        f[0].data, expect,
        "reassembled to exactly the declared size"
    );
}

#[test]
fn spu_overshoot_emits_all_buffered_bytes() {
    // If a continuation pushes the buffer PAST the declared size, the unit
    // still emits with all buffered bytes (>= size triggers emit). Declared
    // 5, head 4, continuation 4 → 8 buffered, emits all 8.
    let mut parser = DvdSubParser::new(None);
    let head = vec![0x00, 0x05, 0xAA, 0xBB]; // 4 of 5
    assert!(parser.parse(&make_pes(head, Some(90000))).is_empty());
    let f = parser.parse(&make_pes(vec![0xCC, 0xDD, 0xEE, 0xFF], None));
    assert_eq!(f.len(), 1);
    assert_eq!(
        f[0].data,
        vec![0x00, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
        "all buffered bytes emitted, not truncated to declared size"
    );
}

// --- MAX_SPU_BYTES bound ---

#[test]
fn head_pes_larger_than_max_spu_is_truncated() {
    // A head PES larger than MAX_SPU_BYTES (0xFFFF) is truncated to the cap
    // when buffered. Declared size in the first 2 bytes = 0xFFFF.
    let mut parser = DvdSubParser::new(None);
    let mut head = vec![0xFF, 0xFF]; // declared 0xFFFF
    head.extend(std::iter::repeat_n(0xAB, MAX_SPU_BYTES + 100));
    // The declared size 0xFFFF == buffered cap, so it completes at the cap.
    let f = parser.parse(&make_pes(head, Some(90000)));
    assert_eq!(f.len(), 1);
    assert_eq!(
        f[0].data.len(),
        MAX_SPU_BYTES,
        "head buffer truncated to MAX_SPU_BYTES"
    );
}

#[test]
fn continuation_appends_bounded_by_max_spu() {
    // Declared the largest size (0xFFFF), so the SPU completes exactly when the clamp
    // fills the buffer: an unclamped append would emit a longer frame.
    let mut parser = DvdSubParser::new(None);
    let mut head = vec![0xFF, 0xFF];
    head.extend(std::iter::repeat_n(0x11, 1000));
    assert!(parser.parse(&make_pes(head, Some(90000))).is_empty());
    let mut frames = Vec::new();
    for _ in 0..100 {
        frames.extend(parser.parse(&make_pes(vec![0x22u8; 2000], None)));
    }
    assert_eq!(frames.len(), 1, "the SPU completes once");
    assert_eq!(frames[0].data.len(), MAX_SPU_BYTES, "clamped to the cap");
}

// --- one-byte head: too short to carry SPU_size ---

#[test]
fn single_byte_head_passes_through_as_lone_frame() {
    // < 2 bytes can't carry the SPU_size field → passed through as a lone
    // frame, not stored pending.
    let mut parser = DvdSubParser::new(None);
    let f = parser.parse(&make_pes(vec![0xAB], Some(90000)));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].data, vec![0xAB]);
    assert!(parser.pending.is_none());
}

#[test]
fn declared_size_one_passes_through() {
    // declared = 1 < 2 (the 2-byte header itself) is malformed → lone frame.
    let mut parser = DvdSubParser::new(None);
    let data = vec![0x00, 0x01, 0xAB];
    let f = parser.parse(&make_pes(data.clone(), Some(90000)));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].data, data);
    assert!(parser.pending.is_none());
}

#[test]
fn no_pts_short_segment_without_pending_is_dropped() {
    // Too-short (< 2 bytes) orphan: dropped, not passed through at pts 0.
    // Matches PGS's drop of the same case (L054).
    let mut parser = DvdSubParser::new(None);
    let f = parser.parse(&make_pes(vec![0xAA], None));
    assert!(f.is_empty(), "orphan short no-PTS segment is dropped");
    assert!(parser.pending.is_none());
}

#[test]
fn no_pts_sized_segment_without_pending_is_dropped() {
    // No-PTS segment, no pending, valid SPU_size (>= 2): still an orphan
    // continuation with no start time — dropped, not turned into a fresh
    // pending SPU at pts 0 (the bug: a garbage bitmap at 00:00:00.0).
    let mut parser = DvdSubParser::new(None);
    let f = parser.parse(&make_pes(vec![0x00, 0x10, 0xAA], None));
    assert!(f.is_empty(), "orphan sized no-PTS segment is dropped");
    assert!(parser.pending.is_none(), "no pending SPU is started");
}

#[test]
fn flush_empty_when_nothing_pending() {
    let mut parser = DvdSubParser::new(None);
    assert!(parser.flush().is_empty());
}

// --- YCbCr → RGB green channel + neutral chroma ---

#[test]
fn ycbcr_blue_channel_clamps_high() {
    // B = 1.164*(Y-16) + 2.017*(Cb-128) ≈ 130 + 256 → clamp 255.
    // Cb is byte 3 in the on-disc [pad, Y, Cr, Cb] layout.
    let [_r, _g, b] = ycbcr_to_rgb(&[0x00, 128, 128, 255]);
    assert_eq!(b, 255, "blue clamps at 255");
}

#[test]
fn ycbcr_to_rgb_green_uses_both_chroma_terms() {
    // Y=128, Cr=100, Cb=90: G = 1.164*112 - 0.392*(-38) - 0.813*(-28) = 167.9.
    assert_eq!(ycbcr_to_rgb(&[0x00, 128, 100, 90])[1], 168);
    // Each chroma term alone: Cr moves G by -0.813/step, Cb by -0.392/step.
    assert_eq!(ycbcr_to_rgb(&[0x00, 128, 100, 128])[1], 153);
    assert_eq!(ycbcr_to_rgb(&[0x00, 128, 128, 90])[1], 145);
}

#[test]
fn format_palette_empty_is_just_prefix() {
    // An empty palette yields "palette: \n" (prefix + newline, no entries).
    let result = format_palette(&[], 0, 0);
    assert_eq!(String::from_utf8(result).unwrap(), "palette: \n");
}

#[test]
fn format_palette_pads_each_channel_to_two_hex_digits() {
    // Each RGB channel is formatted as exactly 2 hex digits (zero-padded).
    // Y=20,neutral → 5 → "050505" (each channel two digits).
    let result = format_palette(&[[0x00, 20, 128, 128]], 0, 0);
    assert_eq!(String::from_utf8(result).unwrap(), "palette: 050505\n");
}

// Text guard in codec/mod.rs can't see `source: facts.source` with no
// offset; only a runtime check proves the frame carries its real byte.
#[test]
fn an_emitted_frame_carries_the_packets_source_offset() {
    let mut parser = DvdSubParser::new(None);
    let mut pes = make_pes(
        vec![0x00, 0x0A, 0x00, 0x08, 0x01, 0xFF, 0x02, 0x03, 0x04, 0x05],
        Some(90_000),
    );
    pes.source = Some(crate::pes::SourcePos::at_byte(7_777));
    let frames = parser.parse(&pes);
    assert!(!frames.is_empty(), "the segment is emitted");
    assert_eq!(frames[0].source.map(|s| s.byte), Some(7_777));
}
