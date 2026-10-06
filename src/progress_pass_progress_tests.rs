use super::*;

/// A zeroed sample. Each test sets only the fields its percentage reads, so
/// a failure names the field that mattered rather than drowning in a
/// thirteen-field literal.
fn sample() -> PassProgress {
    PassProgress {
        kind: PassKind::Sweep,
        work_done: 0,
        work_total: 0,
        bytes_good_total: 0,
        bytes_unreadable_total: 0,
        bytes_pending_total: 0,
        bytes_retryable_total: 0,
        bytes_total_disc: 0,
        disc_duration_secs: None,
        bytes_bad_in_main_title: 0,
        main_title_duration_secs: None,
        main_title_size_bytes: None,
        located: LocatedProgress::default(),
    }
}

// Pins the exact arithmetic (`done / total * 100`, not `done * total / 100`
// or `done / total + 100` — all "look like" a percentage).
#[test]
fn work_pct_is_done_over_total_scaled_to_a_hundred() {
    let p = PassProgress {
        work_done: 250,
        work_total: 1000,
        ..sample()
    };
    assert_eq!(p.work_pct(), 25.0);
}

/// Zero total is the divide-by-zero guard, and it must report COMPLETE, not
/// zero: a pass with no work to do has finished all of it. A UI that read
/// 0% here would sit at "0%" forever on an empty pass.
#[test]
fn work_pct_with_no_work_reports_complete() {
    assert_eq!(sample().work_pct(), 100.0);
}

/// The guard must fire on `total == 0` ONLY. With work present the real
/// arithmetic has to run — a guard inverted to `!=` would short-circuit
/// every real pass to 100% and divide by zero on the empty one.
#[test]
fn work_pct_guard_fires_only_on_zero_total() {
    let p = PassProgress {
        work_done: 1,
        work_total: 4,
        ..sample()
    };
    assert_eq!(p.work_pct(), 25.0, "a non-empty pass must not report 100%");
}

/// A transient overshoot clamps rather than reporting above 100%. Sector
/// counts briefly exceed the total when a pass re-reads, and a progress bar
/// fed 137% renders past its own end.
#[test]
fn work_pct_clamps_an_overshoot_to_a_hundred() {
    let p = PassProgress {
        work_done: 1370,
        work_total: 1000,
        ..sample()
    };
    assert_eq!(p.work_pct(), 100.0);
}

#[test]
fn good_pct_is_good_bytes_over_disc_size() {
    let p = PassProgress {
        bytes_good_total: 750,
        bytes_total_disc: 1000,
        ..sample()
    };
    assert_eq!(p.good_pct(), 75.0);
}

// Unknown disc size: good=100%, bad/pending=0% — the coherent
// "nothing known damaged" state, not three disagreeing percentages.
#[test]
fn good_pct_with_unknown_disc_size_reports_clean() {
    assert_eq!(sample().good_pct(), 100.0);
}

#[test]
fn good_pct_guard_fires_only_on_zero_disc_size() {
    let p = PassProgress {
        bytes_good_total: 1,
        bytes_total_disc: 2,
        ..sample()
    };
    assert_eq!(p.good_pct(), 50.0, "a sized disc must not report 100%");
}

#[test]
fn bad_pct_is_unreadable_bytes_over_disc_size() {
    let p = PassProgress {
        bytes_unreadable_total: 125,
        bytes_total_disc: 1000,
        ..sample()
    };
    assert_eq!(p.bad_pct(), 12.5);
}

/// Unknown disc size reports 0% bad — the opposite default from `good_pct`,
/// and deliberately so. Reporting 100% bad on an unsized disc would show a
/// fully-damaged disc the instant a rip started.
#[test]
fn bad_pct_with_unknown_disc_size_reports_none() {
    assert_eq!(sample().bad_pct(), 0.0);
}

#[test]
fn bad_pct_guard_fires_only_on_zero_disc_size() {
    let p = PassProgress {
        bytes_unreadable_total: 1,
        bytes_total_disc: 4,
        ..sample()
    };
    assert_eq!(p.bad_pct(), 25.0, "a sized disc must not report 0% bad");
}

#[test]
fn pending_pct_is_pending_bytes_over_disc_size() {
    let p = PassProgress {
        bytes_pending_total: 400,
        bytes_total_disc: 1000,
        ..sample()
    };
    assert_eq!(p.pending_pct(), 40.0);
}

#[test]
fn pending_pct_with_unknown_disc_size_reports_none() {
    assert_eq!(sample().pending_pct(), 0.0);
}

#[test]
fn pending_pct_guard_fires_only_on_zero_disc_size() {
    let p = PassProgress {
        bytes_pending_total: 3,
        bytes_total_disc: 4,
        ..sample()
    };
    assert_eq!(p.pending_pct(), 75.0, "a sized disc must not report 0%");
}

/// The three disc-relative percentages clamp an overshoot too, not just
/// `work_pct`. A counter can transiently exceed the disc size while a pass
/// re-reads a region, and a client fed 137% renders past the end of its bar.
#[test]
fn the_disc_percentages_clamp_an_overshoot_to_a_hundred() {
    let over = |f: fn(&PassProgress) -> f64, set: fn(&mut PassProgress)| {
        let mut p = PassProgress {
            bytes_total_disc: 1000,
            ..sample()
        };
        set(&mut p);
        f(&p)
    };
    assert_eq!(
        over(PassProgress::good_pct, |p| p.bytes_good_total = 5000),
        100.0
    );
    assert_eq!(
        over(PassProgress::bad_pct, |p| p.bytes_unreadable_total = 5000),
        100.0
    );
    assert_eq!(
        over(PassProgress::pending_pct, |p| p.bytes_pending_total = 5000),
        100.0
    );
}

// The three disc-relative percentages read distinct byte counters. Earlier
// tests set one counter at a time, so a swapped field would slip past them;
// this one sets all three to distinct values at once.
#[test]
fn the_disc_percentages_read_distinct_counters() {
    let p = PassProgress {
        bytes_good_total: 500,
        bytes_unreadable_total: 200,
        bytes_pending_total: 300,
        bytes_total_disc: 1000,
        ..sample()
    };
    assert_eq!(p.good_pct(), 50.0, "good_pct must read bytes_good_total");
    assert_eq!(
        p.bad_pct(),
        20.0,
        "bad_pct must read bytes_unreadable_total"
    );
    assert_eq!(
        p.pending_pct(),
        30.0,
        "pending_pct must read bytes_pending_total"
    );
}
