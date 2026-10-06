use super::*;
use crate::mux::codec::coding::PictureInfo;

fn frame(ctype: CodingType, keyframe: bool) -> Frame {
    Frame {
        keyframe,
        coding: Some(PictureInfo::coding_type_only(ctype)),
        ..Default::default()
    }
}

#[test]
fn display_indices_map_classic_gop() {
    use CodingType::*;
    // decode I P B P B  ->  display I B P B P
    let d = display_indices([I, P, B, P, B].into_iter());
    assert_eq!(d, vec![0, 2, 1, 4, 3]);
}

#[test]
fn display_indices_all_anchors_are_identity() {
    use CodingType::*;
    let d = display_indices([I, P, P, P].into_iter());
    assert_eq!(d, vec![0, 1, 2, 3]);
}

#[test]
fn reconstructs_monotonic_display_pts_from_one_anchor_per_gop() {
    use CodingType::*;
    // Two GOPs of 5 frames, decode order I P B P B, anchor PTS only on the
    // GOP's I (0 ns, then ~5-frames-later). Frame duration should calibrate
    // to the spacing/5 and every frame get a distinct increasing display PTS.
    let dur = 41_708_333i64;
    let mut r = SparsePtsReorder::new();
    let mut got: Vec<i64> = Vec::new();
    // GOP 1: anchor on the I at t=0.
    for (k, (ct, pts)) in [(I, Some(0i64)), (P, None), (B, None), (P, None), (B, None)]
        .into_iter()
        .enumerate()
    {
        let out = r.push(pts, frame(ct, k == 0));
        got.extend(out.iter().map(|f| f.pts_ns));
    }
    // GOP 2: anchor on the I at t = 5*dur (its true display time).
    for (k, (ct, pts)) in [
        (I, Some(5 * dur)),
        (P, None),
        (B, None),
        (P, None),
        (B, None),
    ]
    .into_iter()
    .enumerate()
    {
        let out = r.push(pts, frame(ct, k == 0));
        got.extend(out.iter().map(|f| f.pts_ns));
    }
    got.extend(r.flush().iter().map(|f| f.pts_ns));

    // Ten frames out, none dropped.
    assert_eq!(got.len(), 10, "all frames emitted");
    // The calibrated duration is (5*dur)/5 = dur.
    // GOP 1 decode order I P B P B -> display indices 0 2 1 4 3 -> PTS:
    assert_eq!(
        &got[0..5],
        &[0, 2 * dur, dur, 4 * dur, 3 * dur],
        "GOP1 display PTS in decode order"
    );
    // GOP 2 re-locks origin to 5*dur.
    assert_eq!(
        &got[5..10],
        &[5 * dur, 7 * dur, 6 * dur, 9 * dur, 8 * dur],
        "GOP2 display PTS continue monotonically per display order"
    );
}

#[test]
fn force_flushes_a_gop_that_exceeds_the_byte_cap() {
    use CodingType::*;
    // Few-but-huge access units with no keyframe must not accumulate past the
    // byte cap: a handful of ~MAX_GOP_BYTES/4-sized frames force-completes the
    // GOP well before the frame-count cap, bounding memory.
    let big = MAX_GOP_BYTES / 4 + 1;
    let mut r = SparsePtsReorder::new();
    let mut emitted = 0usize;
    // Enough huge frames to trigger several byte-cap completions (a GOP is
    // held one step for duration calibration, so the first emit lands after
    // the second cap fires) — well under the 600-frame count cap.
    for i in 0..16 {
        let mut f = frame(P, false);
        f.data = vec![0u8; big];
        emitted += r.push((i == 0).then_some(0), f).len();
    }
    assert!(
        emitted >= 1,
        "byte cap force-flushed (emitted {emitted}) before the frame-count cap"
    );
}

#[test]
fn force_flushes_a_gop_that_never_signals_a_keyframe() {
    use CodingType::*;
    // A stream that never flags a keyframe (open-GOP recovery points, or a
    // crafted/corrupt disc) must not buffer the whole title: the cap
    // force-completes GOPs so frames are emitted well before flush().
    let mut r = SparsePtsReorder::new();
    let mut emitted = 0usize;
    for i in 0..(MAX_GOP_FRAMES * 3) {
        let pts = (i == 0).then_some(0);
        emitted += r.push(pts, frame(P, false)).len();
    }
    assert!(
        emitted >= MAX_GOP_FRAMES,
        "cap force-flushed GOPs before EOF (emitted {emitted})"
    );
}

#[test]
fn calibration_accounts_for_open_gop_leading_bs() {
    use CodingType::*;
    // Closed GOP (I at display 0) then an open GOP whose I follows two leading Bs.
    let dur = 41_708_333i64;
    let mut r = SparsePtsReorder::new();
    let mut got: Vec<i64> = Vec::new();
    let gops = [
        ([I, P, P, P], 0i64),
        ([I, B, B, P], 6 * dur), // I displays after B B, at slot 6
    ];
    for (types, pts) in gops {
        for (k, ct) in types.into_iter().enumerate() {
            let p = (k == 0).then_some(pts);
            got.extend(r.push(p, frame(ct, k == 0)).iter().map(|f| f.pts_ns));
        }
    }
    got.extend(r.flush().iter().map(|f| f.pts_ns));
    assert_eq!(&got[..4], &[0, dur, 2 * dur, 3 * dur]);
}

// The slots between two anchors exclude the held GOP's frames displayed before its own anchor.
#[test]
fn calibration_discounts_the_held_gops_leading_display_offset() {
    use CodingType::*;
    let dur = 40_000_000i64;
    let mut r = SparsePtsReorder::new();
    let mut got: Vec<i64> = Vec::new();
    // Open first GOP, decode I B B P: the I displays third (slot 2), the P fourth.
    for (k, ct) in [I, B, B, P].into_iter().enumerate() {
        let p = (k == 0).then_some(2 * dur);
        got.extend(r.push(p, frame(ct, k == 0)).iter().map(|f| f.pts_ns));
    }
    // The next GOP's I is anchored two slots after the first anchor.
    for (k, ct) in [I, P].into_iter().enumerate() {
        let p = (k == 0).then_some(4 * dur);
        got.extend(r.push(p, frame(ct, k == 0)).iter().map(|f| f.pts_ns));
    }
    got.extend(r.flush().iter().map(|f| f.pts_ns));
    assert_eq!(&got[..4], &[2 * dur, 0, dur, 3 * dur]);
}

#[test]
fn no_pts_collisions_within_a_gop() {
    use CodingType::*;
    // Every frame distinct in DISPLAY order — the property the mkv muxer
    // needs so a decoder can derive monotonic DTS.
    let mut r = SparsePtsReorder::new();
    let mut all: Vec<i64> = Vec::new();
    for gop in 0..3 {
        for (k, ct) in [I, P, B, P, B].into_iter().enumerate() {
            let pts = (k == 0).then_some(gop as i64 * 5 * 41_708_333);
            all.extend(r.push(pts, frame(ct, k == 0)).iter().map(|f| f.pts_ns));
        }
    }
    all.extend(r.flush().iter().map(|f| f.pts_ns));
    let mut sorted = all.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), all.len(), "no two frames share a display PTS");
}

#[test]
fn unanchored_gops_continue_after_the_previous_gop() {
    use CodingType::*;
    // GOPs 1-2 carry anchors (calibrating dur); GOPs 3-4 carry none and must
    // continue at origin + count*dur, not collide.
    let dur = 40_000_000i64;
    let mut r = SparsePtsReorder::new();
    let mut got: Vec<i64> = Vec::new();
    for anchor in [Some(0i64), Some(3 * dur), None, None] {
        for (k, ct) in [I, P, P].into_iter().enumerate() {
            let p = if k == 0 { anchor } else { None };
            got.extend(r.push(p, frame(ct, k == 0)).iter().map(|f| f.pts_ns));
        }
    }
    got.extend(r.flush().iter().map(|f| f.pts_ns));
    let want: Vec<i64> = (0..12).map(|i| i * dur).collect();
    assert_eq!(got, want);
}

#[test]
fn single_anchor_uses_fallback_duration() {
    use CodingType::*;
    let mut r = SparsePtsReorder::new();
    let mut got = Vec::new();
    for (k, ct) in [I, P, P].into_iter().enumerate() {
        got.extend(r.push((k == 0).then_some(1_000), frame(ct, k == 0)));
    }
    got.extend(r.flush());
    let pts: Vec<i64> = got.iter().map(|f| f.pts_ns).collect();
    let d = FALLBACK_FRAME_DUR_NS;
    assert_eq!(pts, vec![1_000, 1_000 + d, 1_000 + 2 * d]);
    assert!(got.iter().all(|f| f.duration_ns == Some(d as u64)));
}
