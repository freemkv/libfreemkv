//! Guard tests for the DTS deriver, per spec; do not change without a spec citation
//! proving otherwise. H.222.0 = ITU-T H.222.0 (02/2000), cited as h222:N.

use super::test_es::*;
use super::*;

/// h222:6425-6429 (§2.7.5), verbatim.
const SPEC_DTS_IFF: &str = "A decoding_timestamp (DTS) shall appear in a PES packet header if and \
only if the following two conditions are met: • a PTS is present in the PES packet header; \
• the decoding time differs from the presentation time.";

/// h222:2013-2016 (§2.4.2.4), verbatim.
const SPEC_REORDER_FROM_START: &str = "Care should be taken to use adequate re-ordering delay from \
the beginning of video elementary streams to meet the requirements of the entire stream. For \
example, a stream which initially has only I- and P-pictures but later includes B-pictures \
should include re-ordering delay starting at the beginning of the stream.";

/// h222:8835-8837 (Annex C), verbatim.
const SPEC_LOW_DELAY: &str = "In all cases where the decoding and presentation times are \
identical in the STD, i.e. all AAUs, B-picture VAUs, and I- and P-picture VAUs within low-delay \
video sequences, the DTS is not coded, as it would have the same value as the PTS.";

/// h222:2010-2012 (§2.4.2.4), verbatim.
const SPEC_B_NO_DELAY: &str = "For presentation units that do not require re-ordering delay, \
tpn(k) is equal to tdn(j) since the access units are decoded instantaneously; this is the case, \
for example, for B-frames.";

// Frame period at 24 fps and a base that keeps every start-up DTS above 0.
const F: i64 = 3_750;
const B: i64 = 90_000;

/// Push `(pts, es)` in decode order, then finish; the DTS per frame (`None` = PTS only).
fn run(d: &mut DtsDeriver, frames: &[(i64, Vec<u8>)]) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for (pts, es) in frames {
        d.push(*pts, es);
        while let Some(o) = d.pop() {
            out.push(o);
        }
    }
    d.finish();
    while let Some(o) = d.pop() {
        out.push(o);
    }
    assert_eq!(out.len(), frames.len(), "one DTS decision per pushed frame");
    out
}

/// §2.7.5 invariants: every written DTS is below its PTS (a DTS equal to the PTS is not
/// written) and written DTS strictly increase in decode order.
fn assert_conformant(frames: &[(i64, Vec<u8>)], dts: &[Option<i64>]) {
    let mut last = None;
    for ((pts, _), d) in frames.iter().zip(dts) {
        if let Some(d) = *d {
            assert!(
                d < *pts,
                "{SPEC_DTS_IFF}: DTS {d} must differ from and precede PTS {pts}"
            );
            assert!(
                last.is_none_or(|l| d > l),
                "written DTS must strictly increase: {last:?} then {d}"
            );
            last = Some(d);
        }
    }
}

/// A picture at 24 fps (frame_rate_code 2: one frame = `F`), a sequence header first when
/// `seq` is `Some(low_delay)`.
fn mpeg2_frame(seq: Option<bool>, coding: u8, structure: u8) -> Vec<u8> {
    mpeg2_frame_at(2, seq, coding, structure)
}

/// The same at 25 Hz (frame_rate_code 3: a field is 1800 ticks).
fn mpeg2_frame_25(seq: Option<bool>, coding: u8, structure: u8) -> Vec<u8> {
    mpeg2_frame_at(3, seq, coding, structure)
}

fn mpeg2_frame_at(frc: u8, seq: Option<bool>, coding: u8, structure: u8) -> Vec<u8> {
    let mut es = seq
        .map(|low_delay| mpeg2_seq(frc, low_delay))
        .unwrap_or_default();
    es.extend(mpeg2_pic(coding, structure));
    es
}

/// An H.264 SPS with `max_num_reorder_frames = reorder` and VUI timing stating 24 fps
/// (two 1/48 s clock ticks per frame = `F`).
fn sps_timed(reorder: u32) -> H264Sps {
    let mut sps = sps_with_reorder(reorder);
    if let Some(v) = sps.vui.as_mut() {
        v.timing = Some((1, 48));
    }
    sps
}

/// Frame pictures, decode order `order` (display indices), I first with a sequence header.
fn mpeg2_frames(order: &[i64], types: &[u8], low_delay: bool) -> Vec<(i64, Vec<u8>)> {
    order
        .iter()
        .zip(types)
        .enumerate()
        .map(|(i, (&d, &t))| (B + d * F, mpeg2_frame((i == 0).then_some(low_delay), t, 3)))
        .collect()
}

fn mpeg2() -> DtsDeriver {
    DtsDeriver::for_codec(Codec::Mpeg2, None)
}

// ── §2.7.5 worked cases (design §2.3) ──────────────────────────────────────────────
// Start-up T is the stated frame period: the reorder delay "is a multiple of the nominal
// picture period" (h222:2012-2013). These pin the design's worked values exactly.

#[test]
fn mpeg2_ibbp_b_pictures_are_pts_only_i_and_p_carry_dts() {
    // Decode I0 P3 B1 B2 P6 B4 B5, R = 1, T = one frame: DTS −1 0 1 2 3 (B PTS-only).
    let frames = mpeg2_frames(&[0, 3, 1, 2, 6, 4, 5], &[1, 2, 3, 3, 2, 3, 3], false);
    let dts = run(&mut mpeg2(), &frames);
    assert_eq!(
        dts,
        vec![
            Some(B - F),
            Some(B),
            None,
            None,
            Some(B + 3 * F),
            None,
            None
        ],
        "{SPEC_B_NO_DELAY}"
    );
    assert_conformant(&frames, &dts);
}

#[test]
fn mpeg2_field_pairs_across_ir_frames_are_one_unit_each() {
    // Decode I0t I0b P3t P3b B1t B1b B2t B2b, PTS in field periods 0 1 6 7 2 3 4 5 (25 Hz:
    // 1800 ticks). T = one frame = 2 fields: DTS −2 −1 0 1 2 3 4 5 (B fields PTS-only).
    let fp = 1_800;
    let pts = [0, 1, 6, 7, 2, 3, 4, 5];
    let types = [1, 1, 2, 2, 3, 3, 3, 3];
    let frames: Vec<_> = (0..8)
        .map(|i| {
            let seq = (i == 0).then_some(false);
            (
                B + pts[i] * fp,
                mpeg2_frame_25(seq, types[i], if i % 2 == 0 { 1 } else { 2 }),
            )
        })
        .collect();
    let mut d = mpeg2();
    let dts = run(&mut d, &frames);
    let f = |n: i64| Some(B + n * fp);
    assert_eq!(dts, vec![f(-2), f(-1), f(0), f(1), None, None, None, None]);
    assert_eq!(d.core.units, 4, "four field pairs are four units");
    assert_conformant(&frames, &dts);
}

#[test]
fn mpeg2_ir_frame_holding_both_fields_is_one_unit() {
    // mkvmerge layout: each IR frame carries the top and the bottom field picture.
    let order = [0, 3, 1, 2];
    let types = [1u8, 2, 3, 3];
    let frames: Vec<_> = order
        .iter()
        .zip(types)
        .enumerate()
        .map(|(i, (&o, t))| {
            let mut es = if i == 0 {
                mpeg2_seq(2, false)
            } else {
                Vec::new()
            };
            es.extend(mpeg2_pic(t, 1));
            es.extend(mpeg2_pic(t, 2));
            (B + o * F, es)
        })
        .collect();
    let mut d = mpeg2();
    let dts = run(&mut d, &frames);
    assert_eq!(d.core.units, 4, "one unit per two-field IR frame (MPG4-5)");
    assert_eq!(dts, vec![Some(B - F), Some(B), None, None]);
}

#[test]
fn mpeg2_cross_frame_pairing_only_within_one_field_period_plus_one_tick() {
    let fp = 1_800; // 25 Hz, frame_rate_code 3
    for (gap, units) in [(fp, 1), (fp + 1, 1), (fp + 2, 2), (2 * fp, 2)] {
        let mut d = mpeg2();
        d.push(B, &mpeg2_frame_25(Some(false), 1, 1));
        d.push(B + gap, &mpeg2_frame_25(None, 1, 2));
        assert_eq!(
            d.core.units, units,
            "second field {gap} ticks after the first"
        );
    }
    // Same parity never pairs.
    let mut d = mpeg2();
    d.push(B, &mpeg2_frame_25(Some(false), 1, 1));
    d.push(B + fp, &mpeg2_frame_25(None, 1, 1));
    assert_eq!(d.core.units, 2, "two top fields are two non-paired units");
}

#[test]
fn mpeg2_frame_period_includes_the_frame_rate_extension() {
    // frame_rate = value × (n + 1) ÷ (d + 1) (ISO/IEC 13818-2 §6.3.5): code 3 (25 Hz) with
    // n = 1, d = 0 is 50 Hz, a 1800-tick frame.
    let mut seq = mpeg2_seq(3, false);
    let last = seq.len() - 1;
    seq[last] |= 0b0010_0000; // frame_rate_extension_n = 1
    let mut t = None;
    scan_mpeg2(&seq, &mut t);
    assert_eq!(t, Some(1_800));
    scan_mpeg2(&mpeg2_seq(1, false), &mut t);
    assert_eq!(t, Some(3_754), "23.976 Hz: 3753.75 ticks, rounded");
}

#[test]
fn open_gop_leading_pictures_start_up_below_the_first_pts() {
    // Decode I2 B0 B1 P5 (R = 1): the leading B pictures precede the I in output.
    let frames = mpeg2_frames(&[2, 0, 1, 5], &[1, 3, 3, 2], false);
    let dts = run(&mut mpeg2(), &frames);
    assert_eq!(
        dts,
        vec![Some(B - F), None, None, Some(B + 2 * F)],
        "−1 0 1 2"
    );
    assert_conformant(&frames, &dts);
}

#[test]
fn low_delay_sequence_writes_pts_only() {
    let frames = mpeg2_frames(&[0, 1, 2, 3], &[1, 2, 2, 2], true);
    let mut d = mpeg2();
    let dts = run(&mut d, &frames);
    assert_eq!(dts, vec![None; 4], "{SPEC_LOW_DELAY}");
    assert_eq!(d.counters(), DtsCounters::default());
}

#[test]
fn mpeg2_ip_only_without_low_delay_gets_r1_dts_on_p_pictures() {
    // low_delay = 0 declares R = 1 even without B pictures (design §2.3 "What S0 changes").
    let frames = mpeg2_frames(&[0, 1, 2], &[1, 2, 2], false);
    let dts = run(&mut mpeg2(), &frames);
    assert_eq!(dts, vec![Some(B - F), Some(B), Some(B + F)]);
    assert_conformant(&frames, &dts);
}

#[test]
fn mpeg1_sequence_header_without_extension_is_r1() {
    let mut d = DtsDeriver::for_codec(Codec::Mpeg1, None);
    let mut i = mpeg1_seq(2);
    i.extend(mpeg2_pic(1, 3));
    let frames = vec![
        (B, i),
        (B + 3 * F, mpeg2_pic(2, 3)),
        (B + F, mpeg2_pic(3, 3)),
    ];
    let dts = run(&mut d, &frames);
    assert_eq!(dts, vec![Some(B - F), Some(B), None]);
}

// ── H.264 ──────────────────────────────────────────────────────────────────────

fn h264_frame(sps: &H264Sps, idr: bool, frame_num: u32) -> Vec<u8> {
    length_prefixed(&[h264_slice(sps, idr, 0, frame_num, None)])
}

#[test]
fn h264_b_pyramid_r2_matches_the_order_statistic() {
    // Decode I0 P4 B2 b1 b3 P8 B6 b5 b7, R = 2 from max_num_reorder_frames.
    let sps = sps_timed(2);
    let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps.nal())));
    let order = [0, 4, 2, 1, 3, 8, 6, 5, 7];
    let frames: Vec<_> = order
        .iter()
        .enumerate()
        .map(|(i, &o)| (B + o * F, h264_frame(&sps, i == 0, i as u32)))
        .collect();
    let dts = run(&mut d, &frames);
    let f = |n: i64| Some(B + n * F);
    assert_eq!(
        dts,
        vec![f(-2), f(-1), f(0), None, f(2), f(3), f(4), None, f(6)]
    );
    assert_eq!(d.counters(), DtsCounters::default());
    assert_conformant(&frames, &dts);
}

#[test]
fn without_a_stated_frame_rate_t_is_the_smallest_window_gap() {
    // No VUI timing: T falls back to the smallest positive gap of the sorted window
    // {0, 2, 4}, here 2 frames (then 1/24 s when there is none).
    let sps = sps_with_reorder(2);
    let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps.nal())));
    let frames: Vec<_> = [0i64, 4, 2]
        .into_iter()
        .enumerate()
        .map(|(i, o)| (B + o * F, h264_frame(&sps, i == 0, i as u32)))
        .collect();
    let f = |n: i64| Some(B + n * F);
    assert_eq!(run(&mut d, &frames), vec![f(-4), f(-2), f(0)]);
    assert_eq!(min_gap(&[B]), FALLBACK_FRAME_TICKS);
    assert_eq!(min_gap(&[B, B, B + 7]), 7);
}

#[test]
fn h264_paff_pairs_come_from_the_slice_header() {
    let sps = H264Sps {
        frame_mbs_only: false,
        height_map_units: 34,
        vui: Some(H264Vui {
            timing: Some((1, 50)),
            restriction: Some((1, 2)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let field =
        |idr, fnum, bottom| length_prefixed(&[h264_slice(&sps, idr, 0, fnum, Some(bottom))]);
    let mut d = DtsDeriver::for_codec(Codec::H264, None);
    let mut first = length_prefixed(&[sps.nal()]);
    first.extend(field(true, 0, false));
    // IDR top + non-IDR bottom, same frame_num, one field (1800) apart: one unit.
    d.push(B, &first);
    d.push(B + 1_800, &field(false, 0, true));
    assert_eq!(d.core.units, 1);
    // A different frame_num never pairs, nor does an IDR second field, nor equal parity.
    d.push(B + 7_200, &field(false, 1, false));
    d.push(B + 9_000, &field(false, 2, true));
    assert_eq!(d.core.units, 3, "frame_num 1 then 2: not a pair");
    d.push(B + 10_800, &field(true, 3, false));
    assert_eq!(d.core.units, 4, "IDR is always a first field");
    d.push(B + 12_600, &field(false, 3, false));
    assert_eq!(d.core.units, 5, "same parity: not a pair");
    // Both fields in one IR frame (h264.rs field-pair AU) are one unit.
    let mut pair = field(false, 4, true);
    pair.extend(length_prefixed(&[h264_slice(
        &sps,
        false,
        0,
        4,
        Some(false),
    )]));
    d.push(B + 14_400, &pair);
    assert_eq!(d.core.units, 6);
    assert!(
        d.core.open_field.is_none(),
        "a complete pair leaves no field open"
    );
}

#[test]
fn h264_field_coded_r_upgrade_allots_one_slot_per_field() {
    // 25 fps PAFF: clip 1 R = 0 (I/P), clip 2 IDR activates R = 2 (B-pyramid). The
    // re-start places 4 field slots in (DTS_last, S_new[0]); no guard firing.
    let clip = |reorder| H264Sps {
        frame_mbs_only: false,
        height_map_units: 34,
        vui: Some(H264Vui {
            timing: Some((1, 50)),
            restriction: Some((reorder, reorder + 1)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let (sps0, sps2) = (clip(0), clip(2));
    let (frame, field) = (3_600, 1_800);
    let mut frames = Vec::new();
    for u in 0..4i64 {
        for bottom in [false, true] {
            let mut es = if u == 0 && !bottom {
                length_prefixed(&[sps0.nal()])
            } else {
                Vec::new()
            };
            let slice = h264_slice(&sps0, u == 0 && !bottom, 0, u as u32, Some(bottom));
            es.extend(length_prefixed(&[slice]));
            frames.push((B + u * frame + bottom as i64 * field, es));
        }
    }
    let base2 = B + 4 * frame;
    for (i, o) in [0i64, 4, 2, 1, 3, 8, 6, 5, 7].into_iter().enumerate() {
        for bottom in [false, true] {
            let mut es = if i == 0 && !bottom {
                length_prefixed(&[sps2.nal()])
            } else {
                Vec::new()
            };
            let slice = h264_slice(&sps2, i == 0 && !bottom, 0, i as u32, Some(bottom));
            es.extend(length_prefixed(&[slice]));
            frames.push((base2 + o * frame + bottom as i64 * field, es));
        }
    }
    let mut d = DtsDeriver::for_codec(Codec::H264, None);
    let dts = run(&mut d, &frames);
    assert!(
        dts[..8].iter().all(Option::is_none),
        "clip 1 is R = 0: PTS only"
    );
    let dts_last = B + 3 * frame + field;
    let step = (base2 - dts_last) / 5;
    let slots: Vec<_> = (1..=4).map(|i| Some(dts_last + i * step)).collect();
    assert_eq!(
        &dts[8..12],
        &slots[..],
        "one slot per field in (DTS_last, S_new[0])"
    );
    assert_eq!(
        dts[12],
        Some(base2),
        "unit R takes the steady rule: S_new[0]"
    );
    assert_eq!(d.counters(), DtsCounters::default(), "no guard firing");
    assert_conformant(&frames, &dts);
}

#[test]
fn r_is_parsed_from_the_start_and_only_increases() {
    let _ = SPEC_REORDER_FROM_START;
    // R = 0 then R = 2 (IPPP, then IDR + B-pyramid): re-start spaced in (DTS_last, S_new[0]).
    let (sps0, sps2) = (sps_with_reorder(0), sps_with_reorder(2));
    let mut frames = Vec::new();
    for u in 0..3i64 {
        let mut es = if u == 0 {
            length_prefixed(&[sps0.nal()])
        } else {
            Vec::new()
        };
        es.extend(h264_frame(&sps0, u == 0, u as u32));
        frames.push((B + u * F, es));
    }
    let base2 = B + 3 * F;
    for (i, o) in [0i64, 4, 2, 1, 3].into_iter().enumerate() {
        let mut es = if i == 0 {
            length_prefixed(&[sps2.nal()])
        } else {
            Vec::new()
        };
        es.extend(h264_frame(&sps2, i == 0, i as u32));
        frames.push((base2 + o * F, es));
    }
    let mut d = DtsDeriver::for_codec(Codec::H264, None);
    let dts = run(&mut d, &frames);
    let last = B + 2 * F;
    let step = (base2 - last) / 3;
    assert_eq!(&dts[..3], &[None, None, None]);
    assert_eq!(&dts[3..5], &[Some(last + step), Some(last + 2 * step)]);
    assert_eq!(dts[5], Some(base2), "unit R_new: S_new[0]");
    assert_eq!(d.counters(), DtsCounters::default());
    assert_conformant(&frames, &dts);

    // R = 2 then R = 0: R stays 2, so an in-order clip keeps a two-frame reorder delay.
    let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps2.nal())));
    for u in 0..3i64 {
        d.push(B + u * F, &h264_frame(&sps2, u == 0, u as u32));
    }
    let mut es = length_prefixed(&[sps0.nal()]);
    es.extend(h264_frame(&sps0, true, 0));
    d.push(B + 3 * F, &es);
    d.push(B + 4 * F, &h264_frame(&sps0, false, 1));
    let out: Vec<_> = std::iter::from_fn(|| d.pop()).collect();
    assert_eq!(
        out[3..],
        [Some(B + F), Some(B + 2 * F)],
        "a smaller R_new is ignored"
    );
    assert_eq!(d.core.r, 2);
}

#[test]
fn no_parameter_set_yet_means_r0_and_no_hold() {
    // SPS-less H.264 (and HEVC): reordered PTS pass through PTS-only, uncounted, no window.
    for codec in [Codec::H264, Codec::Hevc] {
        let mut d = DtsDeriver::for_codec(codec, None);
        for (i, o) in [0i64, 3, 1, 2].into_iter().enumerate() {
            d.push(B + o * F, &length_prefixed(&[vec![0x41, i as u8, 0x80]]));
            assert!(!d.pending(), "no hold before a parameter set parses");
            assert_eq!(d.pop(), Some(None));
        }
        assert_eq!(d.counters(), DtsCounters::default());
    }
}

#[test]
fn first_parameter_set_after_written_units_runs_the_upgrade_rule() {
    // Two PTS-only units before the first SPS; the SPS (R = 1) re-starts, never rewinds.
    let sps = sps_with_reorder(1);
    let mut d = DtsDeriver::for_codec(Codec::H264, None);
    let bare = length_prefixed(&[vec![0x65, 0x88, 0x80]]);
    d.push(B, &bare);
    d.push(B + F, &bare);
    let mut es = length_prefixed(&[sps.nal()]);
    es.extend(h264_frame(&sps, true, 0));
    d.push(B + 3 * F, &es);
    assert!(d.pending(), "the first parse starts a hold");
    d.push(B + 2 * F, &h264_frame(&sps, false, 1));
    let out: Vec<_> = std::iter::from_fn(|| d.pop()).collect();
    let step = (B + 2 * F - (B + F)) / 2;
    assert_eq!(out, vec![None, None, Some(B + F + step), None]);
}

#[test]
fn h264_intra_profile_and_poc_type_2_are_r0() {
    for sps in [
        H264Sps {
            profile_idc: 110,
            constraint_flags: 0x10,
            ..Default::default()
        },
        H264Sps {
            poc_type: 2,
            ..Default::default()
        },
    ] {
        let info = h264::parse_sps_dts_info(&sps.nal()).expect("parses");
        assert_eq!(info.reorder, 0);
        let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps.nal())));
        d.push(B, &h264_frame(&sps, true, 0));
        assert!(!d.pending());
    }
}

#[test]
fn h264_e21_inference_from_the_level() {
    let r = |sps: H264Sps| h264::parse_sps_dts_info(&sps.nal()).unwrap().reorder;
    // 1920×1088 at 4.0: 32768 / (120·68) = 4; interlaced map units double the height.
    assert_eq!(r(H264Sps::default()), 4);
    let interlaced = H264Sps {
        frame_mbs_only: false,
        height_map_units: 34,
        ..Default::default()
    };
    assert_eq!(r(interlaced), 4);
    // SD 720×480 at 4.1: 32768 / 1350 = 24 → capped at 16.
    let sd = H264Sps {
        level_idc: 41,
        width_mbs: 45,
        height_map_units: 30,
        ..Default::default()
    };
    assert_eq!(r(sd), 16);
    // QCIF Baseline: level_idc 11 + constraint_set3 is 1b (396 → 4), else 1.1 (900 → 9).
    let qcif = |set3: bool| H264Sps {
        profile_idc: 66,
        constraint_flags: 0xC0 | if set3 { 0x10 } else { 0 },
        level_idc: 11,
        width_mbs: 11,
        height_map_units: 9,
        ..Default::default()
    };
    assert_eq!(r(qcif(true)), 4);
    assert_eq!(r(qcif(false)), 9);
    let unknown = H264Sps {
        level_idc: 99,
        ..Default::default()
    };
    assert_eq!(r(unknown), 16, "an unknown level infers 16");
}

#[test]
fn h264_sps_parser_walks_scaling_lists_poc1_hrd_and_emulation_prevention() {
    let vui = H264Vui {
        timing: Some((1, 50)),
        nal_hrd: true,
        vcl_hrd: true,
        restriction: Some((3, 4)),
    };
    for (scaling_lists, poc_type) in [(true, 0), (false, 1), (true, 1)] {
        let sps = H264Sps {
            scaling_lists,
            poc_type,
            vui: Some(vui.clone()),
            ..Default::default()
        };
        let nal = sps.nal();
        assert!(
            nal.windows(3).any(|w| w == [0, 0, 3]),
            "the VUI timing needs an EPB"
        );
        let info = h264::parse_sps_dts_info(&nal).expect("parses");
        assert_eq!(
            info.reorder, 3,
            "max_num_reorder_frames past HRD (lists {scaling_lists}, POC {poc_type})"
        );
        assert_eq!(info.frame_period_ticks, Some(3_600), "2 × 1/50 s");
    }
    // Cut short before the VUI flag: no parse. Inside the VUI: the level inference.
    let full = sps_with_reorder(1).nal();
    assert_eq!(h264::parse_sps_dts_info(&full[..5]), None);
    let cut = h264::parse_sps_dts_info(&full[..full.len() - 2]).expect("core parses");
    assert_eq!(cut.reorder, 4, "a cut-short VUI falls back to MaxDpbFrames");
}

// ── HEVC and VC-1 ────────────────────────────────────────────────────────────────

#[test]
fn hevc_r_is_sps_max_num_reorder_pics_at_the_highest_sub_layer() {
    assert_eq!(hevc::parse_sps_reorder(&hevc_sps(0, &[(4, 2, 0)])), Some(2));
    assert_eq!(
        hevc::parse_sps_reorder(&hevc_sps(2, &[(2, 0, 0), (3, 1, 0), (5, 3, 0)])),
        Some(3)
    );
    assert_eq!(
        hevc::parse_sps_reorder(&hevc_sps(2, &[(5, 4, 0)])),
        Some(4),
        "info_present = 0"
    );
}

#[test]
fn hevc_b_pyramid_uses_the_sps_reorder_depth() {
    // VUI timing 1/24 s per picture: T = F, the worked −2 −1 0 1 2.
    let sps = hevc_sps_vui(2, Some((1, 24)), false);
    let mut hvcc = vec![0u8; 22];
    hvcc[21] = 0x03; // lengthSizeMinusOne = 3: the IR's 4-octet NAL prefixes
    hvcc.extend_from_slice(&[1, 0x21, 0, 1]);
    hvcc.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    hvcc.extend_from_slice(&sps);
    let mut d = DtsDeriver::for_codec(Codec::Hevc, Some(&hvcc));
    let frames: Vec<_> = [0i64, 4, 2, 1, 3]
        .into_iter()
        .map(|o| (B + o * F, length_prefixed(&[hevc_slice(1)])))
        .collect();
    let dts = run(&mut d, &frames);
    let f = |n: i64| Some(B + n * F);
    assert_eq!(dts, vec![f(-2), f(-1), f(0), None, f(2)]);
}

#[test]
fn hevc_vui_timing_is_reached_through_scaling_lists_pcm_and_rps() {
    for scaling in [false, true] {
        let sps = hevc_sps_vui(3, Some((1001, 30_000)), scaling);
        assert_eq!(
            hevc::parse_sps_dts(&sps),
            Some((3, Some(3_003))),
            "29.97 Hz, scaling {scaling}"
        );
    }
    assert_eq!(
        hevc::parse_sps_dts(&hevc_sps_vui(1, None, true)),
        Some((1, None))
    );
    // An SPS cut short inside the VUI keeps R and loses only the period.
    let sps = hevc_sps_vui(2, Some((1, 24)), false);
    assert_eq!(hevc::parse_sps_dts(&sps[..sps.len() - 8]), Some((2, None)));
}

#[test]
fn vc1_sequence_header_is_r1() {
    let mut d = DtsDeriver::for_codec(Codec::Vc1, None);
    let frames = vec![
        (B, vec![0, 0, 1, 0x0F, 0xC0, 0, 0, 1, 0x0D, 0x12]),
        (B + 3 * F, vec![0, 0, 1, 0x0D, 0x34]),
        (B + F, vec![0, 0, 1, 0x0D, 0x56]),
    ];
    assert_eq!(run(&mut d, &frames), vec![Some(B - 3 * F), Some(B), None]);
}

#[test]
fn other_codecs_never_get_a_dts() {
    let mut d = DtsDeriver::for_codec(Codec::Av1, None);
    let frames = mpeg2_frames(&[0, 3, 1], &[1, 2, 3], false);
    assert_eq!(run(&mut d, &frames), vec![None; 3]);
}

// ── Order statistic, guard and caps (MPG3-2, MPG4-1) ─────────────────────────────

#[test]
fn vfr_pulldown_and_dropped_frames_stay_conformant() {
    // 3:2 pulldown PTS (field durations 3,2,3,2 of 1500 ticks), IBBP decode order, and the
    // same with B pictures dropped (a damage gap). The rule uses order only.
    let display: Vec<i64> = (0..13).map(|i| B + (i * 5 / 2) * 1_500).collect();
    let order = [0usize, 3, 1, 2, 6, 4, 5, 9, 7, 8, 12, 10, 11];
    let types = [1u8, 2, 3, 3, 2, 3, 3, 2, 3, 3, 2, 3, 3];
    let all: Vec<_> = order
        .iter()
        .zip(types)
        .enumerate()
        .map(|(i, (&o, t))| (display[o], mpeg2_frame((i == 0).then_some(false), t, 3)))
        .collect();
    let dropped: Vec<_> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| ![5, 8, 9].contains(i))
        .map(|(_, f)| f.clone())
        .collect();
    for frames in [all, dropped] {
        let mut d = mpeg2();
        let dts = run(&mut d, &frames);
        assert_conformant(&frames, &dts);
        assert_eq!(d.counters(), DtsCounters::default());
    }
}

#[test]
fn under_estimated_r_is_capped_at_pts_guarded_and_counted() {
    // R = 1 declared on an R = 2 B-pyramid: b1's statistic exceeds its PTS; the cap and the
    // guard leave it PTS only, and every written DTS still strictly increases.
    let sps = sps_timed(1);
    let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps.nal())));
    let frames: Vec<_> = [0i64, 4, 2, 1, 3, 8]
        .into_iter()
        .enumerate()
        .map(|(i, o)| (B + o * F, h264_frame(&sps, i == 0, i as u32)))
        .collect();
    let dts = run(&mut d, &frames);
    assert_eq!(
        dts,
        vec![Some(B - F), Some(B), None, None, None, Some(B + 4 * F)]
    );
    assert_eq!(d.counters().order_violations, 1, "b1 counted once");
    assert_conformant(&frames, &dts);

    // R = 0 declared (low_delay) on IBBP: statistic 0 3 3 3 capped to 0 3 1 2, guarded.
    let frames = mpeg2_frames(&[0, 3, 1, 2, 6], &[1, 2, 3, 3, 2], true);
    let mut d = mpeg2();
    let dts = run(&mut d, &frames);
    assert_eq!(dts, vec![None; 5], "R = 0 never writes a DTS");
    assert_eq!(d.counters().order_violations, 2, "B1 and B2 counted");
}

#[test]
fn start_up_dts_below_zero_is_clamped_and_counted() {
    let frames = mpeg2_frames(&[0, 3, 1], &[1, 2, 3], false);
    let h = F / 2;
    let shifted: Vec<_> = frames.into_iter().map(|(p, es)| (p - B + h, es)).collect();
    let mut d = mpeg2();
    let dts = run(&mut d, &shifted);
    assert_eq!(
        dts,
        vec![Some(0), Some(h), None],
        "I0 at F/2: F/2 − F clamps to 0"
    );
    assert_eq!(d.counters().order_violations, 1);
}

#[test]
fn cap_release_uses_the_eof_formula_and_the_guard_prevents_a_duplicate() {
    // Slideshow: R = 1, I0 at 0 s, the hold released by the cap, P1 at 5 s.
    let mut d = mpeg2();
    d.push(B, &mpeg2_frame(Some(false), 1, 3));
    assert!(d.pending());
    d.release_cap();
    assert_eq!(d.pop(), Some(None), "one held unit: S[0] − 0·T = PTS");
    d.push(B + 450_000, &mpeg2_frame(None, 2, 3));
    assert_eq!(
        d.pop(),
        Some(Some(B + 1)),
        "DTS_0 + 1, not a duplicate of DTS_0"
    );
    assert_eq!(
        d.counters(),
        DtsCounters {
            order_violations: 1,
            hold_overflow: 1
        }
    );
}

#[test]
fn eof_with_at_most_r_units_uses_the_start_up_formula_over_what_arrived() {
    let sps = sps_timed(2);
    let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps.nal())));
    let frames = vec![
        (B, h264_frame(&sps, true, 0)),
        (B + 4 * F, h264_frame(&sps, false, 1)),
    ];
    assert_eq!(run(&mut d, &frames), vec![Some(B - F), Some(B)]);
    assert_eq!(
        d.counters(),
        DtsCounters::default(),
        "EOF is not a cap release"
    );
}

#[test]
fn the_start_up_window_resolves_only_when_unit_r_arrives() {
    let sps = sps_timed(2);
    let mut d = DtsDeriver::for_codec(Codec::H264, Some(&avcc(&sps.nal())));
    d.push(B, &h264_frame(&sps, true, 0));
    d.push(B + 4 * F, &h264_frame(&sps, false, 1));
    assert!(
        d.pending() && d.pop().is_none(),
        "units 0..R−1 wait for unit R"
    );
    d.push(B + 2 * F, &h264_frame(&sps, false, 2));
    assert!(!d.pending());
    assert_eq!(d.pop(), Some(Some(B - 2 * F)));
}
