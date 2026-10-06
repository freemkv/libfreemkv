use super::*;

#[test]
fn counts_kept_and_dropped() {
    let mut t = DropTally::new("test");
    t.record_kept();
    t.record_drop(0, 1000, 512, "bad");
    t.record_kept();
    assert_eq!(t.dropped_frames(), 1);
    assert_eq!(t.dropped_duration_ns(), 1000);
    assert!(!t.is_poisoned());
}

#[test]
fn a_negative_duration_is_counted_as_zero_lost_time() {
    let mut t = DropTally::new("test");
    t.record_drop(0, -5_000, 512, "bad");
    t.record_collateral_drop(0, -1, 512, "resync-forward");
    assert_eq!(t.dropped_frames(), 2);
    assert_eq!(t.dropped_duration_ns(), 0);
}

#[test]
fn poisons_after_min_aus_over_half_dropped() {
    let mut t = DropTally::new("test");
    // 199 AUs, all dropped: below the min-AU gate, must NOT poison yet.
    for _ in 0..199 {
        t.record_drop(0, 1000, 512, "bad");
    }
    assert!(!t.is_poisoned(), "below the 200-AU minimum, no verdict");
    // The 200th drop reaches the minimum with >50% dropped → poison.
    t.record_drop(0, 1000, 512, "bad");
    assert!(t.is_poisoned());
}

#[test]
fn does_not_poison_a_mostly_good_track() {
    let mut t = DropTally::new("test");
    // 400 AUs, 1 dropped: nowhere near 50%.
    t.record_drop(0, 1000, 512, "bad");
    for _ in 0..399 {
        t.record_kept();
    }
    assert!(!t.is_poisoned());
}

// Puts the kept count on the critical path (unlike the test above, which never exercises
// it): losing it would silently discard a healthy track.
#[test]
fn interleaved_keeps_are_in_the_poison_denominator() {
    let mut t = DropTally::new("test");
    // 2 kept per 1 dropped, well past the minimum-AU gate: a third of the
    // track is undecodable, which is bad but nowhere near the >50% threshold.
    for _ in 0..(TRACK_VERDICT_MIN_AUS * 3) {
        t.record_kept();
        t.record_kept();
        t.record_drop(0, 1000, 512, "bad");
        assert!(
            !t.is_poisoned(),
            "33% dropped must never poison, at any point in the run"
        );
    }
    assert_eq!(t.dropped_frames(), TRACK_VERDICT_MIN_AUS * 3);
}

/// Drop-gate mismatch: the two counters disagree. A MAJORITY of the track's
/// AUs are dropped (>50%), yet only a MINORITY are individually verified
/// undecodable — the rest are collateral. The poison gate keys on the
/// VERIFIED subset, so this track must survive. The collateral bulk is
/// recorded FIRST, so if the gate mistakenly judged on the raw `dropped`
/// count it would poison as soon as a verified drop pushed it past the
/// minimum; keying on `verified_dropped` it never does.
#[test]
fn a_verified_minority_amid_a_dropped_majority_does_not_poison() {
    let mut t = DropTally::new("test");
    let kept = TRACK_VERDICT_MIN_AUS; // 200 good
    let collateral = TRACK_VERDICT_MIN_AUS * 4; // 800 collateral-bad
    let verified = TRACK_VERDICT_MIN_AUS / 2; // 100 verified-bad
    for _ in 0..kept {
        t.record_kept();
    }
    for _ in 0..collateral {
        t.record_collateral_drop(0, 1000, 512, "resync-forward");
    }
    for _ in 0..verified {
        t.record_drop(0, 1000, 512, "crc");
        assert!(
            !t.is_poisoned(),
            "verified drops are a minority — the dropped majority must not poison"
        );
    }
    let total = kept + collateral + verified;
    assert!(
        t.dropped_frames() > total / 2,
        "a majority of AUs were dropped ({} of {total})",
        t.dropped_frames()
    );
    assert!(
        !t.is_poisoned(),
        "poison keys on the verified minority, not the dropped majority"
    );
}

#[test]
fn collateral_drops_never_poison_the_track() {
    // A TrueHD resync-forward run collaterally drops a long burst of AUs, but
    // none are individually undecodable — the whole-track verdict must stay
    // clean so one corruption event can't amplify into a false total loss.
    let mut t = DropTally::new("test");
    for _ in 0..(TRACK_VERDICT_MIN_AUS * 3) {
        t.record_collateral_drop(0, 1000, 512, "resync-forward");
    }
    assert!(t.dropped_frames() >= TRACK_VERDICT_MIN_AUS, "drops counted");
    assert!(!t.is_poisoned(), "collateral drops must not poison");
}
