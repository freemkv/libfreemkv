// `dropped` is per-run, `dropped_total` cumulative; only the latter can report loss on a
// gap that resolves.
#[test]
fn a_resolved_gap_still_reports_its_dropped_frames() {
    let mut g = ResyncGate::new();

    // Gap, then two inter-coded frames dropped, then a keyframe resyncs.
    assert!(
        !g.admit(true, true, false),
        "post-gap non-keyframe is dropped"
    );
    assert!(
        !g.admit(true, false, false),
        "still dropping until a keyframe"
    );
    assert!(
        g.admit(true, false, true),
        "the keyframe resyncs and is emitted"
    );

    assert_eq!(
        g.dropped_in_run(),
        0,
        "the per-run counter is zeroed by the resync — by design"
    );
    assert_eq!(
        g.dropped_total(),
        2,
        "but the frames are still gone, and the cumulative count must say so"
    );
    assert!(!g.is_armed(), "resynced");

    // A second gap accumulates rather than restarting.
    assert!(!g.admit(true, true, false));
    assert!(g.admit(true, false, true));
    assert_eq!(
        g.dropped_total(),
        3,
        "totals accumulate across runs; a title with several concealed gaps \
             must not report only the last one"
    );
}
use super::*;

#[test]
fn non_video_always_admits_even_on_discontinuity() {
    let mut g = ResyncGate::new();
    // Audio/subtitle: a gap drops only the (already TS-dropped) frame; every
    // frame the parser still emits is independent and must pass.
    assert!(g.admit(false, true, false));
    assert!(g.admit(false, true, false));
    assert!(!g.is_armed(), "non-video never arms");
}

#[test]
fn video_drops_inter_frames_until_next_keyframe() {
    let mut g = ResyncGate::new();
    // Clean run: everything admits.
    assert!(g.admit(true, false, true)); // IDR
    assert!(g.admit(true, false, false)); // P
    // Gap arrives on the next frame (a P referencing lost data) → drop it
    // and every inter frame until the next keyframe.
    assert!(!g.admit(true, true, false), "post-gap P dropped");
    assert!(
        g.is_armed(),
        "the gate reports itself armed WHILE it is dropping — this is what \
             the consumer reads to know the stream is in a resync hole"
    );
    assert!(!g.admit(true, false, false), "still dropping (no key yet)");
    assert!(g.is_armed(), "still armed mid-run");
    assert!(!g.admit(true, false, false));
    assert_eq!(g.dropped_in_run(), 3);
    // Next keyframe resyncs and is emitted.
    assert!(g.admit(true, false, true), "keyframe resumes the stream");
    assert!(!g.is_armed());
    assert_eq!(g.dropped_in_run(), 0);
    // Back to a clean run.
    assert!(g.admit(true, false, false), "post-resync P admits");
}

#[test]
fn discontinuity_landing_on_a_keyframe_emits_immediately() {
    let mut g = ResyncGate::new();
    // If the first surviving frame after the gap is itself an IRAP, there is
    // nothing to drop — it is self-contained.
    assert!(g.admit(true, true, true), "gap+keyframe emits, no drop");
    assert!(!g.is_armed());
    assert_eq!(g.dropped_in_run(), 0);
}
