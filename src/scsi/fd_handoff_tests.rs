use super::*;

/// Fresh slot + liveness flag, as `SgIoTransport::open` builds them.
fn slot() -> (AtomicI32, AtomicBool) {
    (AtomicI32::new(EMPTY), AtomicBool::new(false))
}

// ── Drop-time tray unlock ──────────────────────────────────────────────

/// Drop sends ALLOW only for a transport that holds a PREVENT, only on an fd
/// it already owns (never an inline reopen of a possibly wedged node), and
/// not again after an ALLOW already died on the transport.
#[test]
fn drop_unlock_fd_uses_only_an_owned_fd_and_only_after_prevent() {
    assert_eq!(
        drop_unlock_fd(false, false, 5, None),
        None,
        "never PREVENTed"
    );
    assert_eq!(drop_unlock_fd(false, false, -1, Some(7)), None);
    assert_eq!(drop_unlock_fd(true, false, 5, None), Some(5));
    assert_eq!(drop_unlock_fd(true, false, 5, Some(7)), Some(5));
    assert_eq!(
        drop_unlock_fd(true, false, -1, Some(7)),
        Some(7),
        "recovered fd"
    );
    assert_eq!(
        drop_unlock_fd(true, false, -1, None),
        None,
        "no inline reopen"
    );
    assert_eq!(
        drop_unlock_fd(true, true, 5, None),
        None,
        "ALLOW hit a dead bus"
    );
}

/// A full recovery-thread cap means SG_IO calls are already stuck: Drop
/// closes at once rather than block on an inline ALLOW. Only a failed spawn
/// (checked by the caller) falls back to an inline ALLOW.
/// LD15 (GUARD + a new row): the transport's drop-ALLOW fires whenever PREVENT is
/// still held, including after the Drive's own ALLOW was answered with an error
/// (`prevent_held` stays set); only a dead-bus ALLOW or a full cap skips it.
#[test]
fn fd_handoff_drop_allow_after_failed_drive_allow() {
    // The Drive's ALLOW got a CHECK CONDITION: PREVENT still held, bus alive.
    assert_eq!(drop_unlock_fd(true, false, 5, None), Some(5));
    assert_eq!(drop_unlock_plan(Some(5), true), DropUnlock::Detached(5));
    // Cap full: skipped (the UI warns the tray may stay locked).
    assert_eq!(drop_unlock_plan(Some(5), false), DropUnlock::Skip);
    // The ALLOW died on the transport: never retried.
    assert_eq!(drop_unlock_fd(true, true, 5, None), None);
}

#[test]
fn drop_unlock_plan_skips_when_the_thread_cap_is_full() {
    assert_eq!(drop_unlock_plan(None, true), DropUnlock::Skip);
    assert_eq!(drop_unlock_plan(None, false), DropUnlock::Skip);
    assert_eq!(drop_unlock_plan(Some(5), true), DropUnlock::Detached(5));
    assert_eq!(drop_unlock_plan(Some(5), false), DropUnlock::Skip);
}

// ── Recovery-thread cap ────────────────────────────────────────────────

/// Past the cap nothing stays reserved (a leaked reservation would ratchet the
/// counter to the cap and force every later reopen inline), and a release
/// frees the slot again.
#[test]
fn recovery_slot_cap_gives_back_an_over_cap_reservation() {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    let counter = AtomicUsize::new(0);
    assert!(reserve_recovery_slot(&counter, 2));
    assert!(reserve_recovery_slot(&counter, 2));
    for _ in 0..5 {
        assert!(!reserve_recovery_slot(&counter, 2), "over the cap");
    }
    assert_eq!(counter.load(Relaxed), 2, "over-cap attempts must not leak");
    release_recovery_slot(&counter);
    assert!(
        reserve_recovery_slot(&counter, 2),
        "a released slot is reusable"
    );
}

/// Concurrent reservers never hold more than `max` slots at once.
#[test]
fn recovery_slot_cap_holds_under_contention() {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    const MAX: usize = 3;
    let counter = AtomicUsize::new(0);
    let held = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for _ in 0..8 {
            sc.spawn(|| {
                for _ in 0..2_000 {
                    if reserve_recovery_slot(&counter, MAX) {
                        let now = held.fetch_add(1, Relaxed) + 1;
                        peak.fetch_max(now, Relaxed);
                        held.fetch_sub(1, Relaxed);
                        release_recovery_slot(&counter);
                    }
                }
            });
        }
    });
    assert!(
        peak.load(Relaxed) <= MAX,
        "cap exceeded: {}",
        peak.load(Relaxed)
    );
    assert_eq!(counter.load(Relaxed), 0, "every reservation returned");
}

// ── Positive: the ordinary hand-off ────────────────────────────────────

/// The path taken on every successful recovery: the thread publishes, the
/// next `execute()` drains. Neither side is asked to close anything.
#[test]
fn published_fd_reaches_the_next_execute() {
    let (s, d) = slot();
    assert_eq!(
        publish_recovered_fd(&s, &d, 7),
        None,
        "publisher handed off"
    );
    assert_eq!(take_recovered_fd(&s), Some(7), "execute() picks it up");
    assert_eq!(take_recovered_fd(&s), None, "slot is empty afterwards");
}

/// Recovery completes, but the transport is dropped before another
/// `execute()` runs (the abort-on-wedge path). Teardown owns the close.
#[test]
fn teardown_claims_an_undrained_fd() {
    let (s, d) = slot();
    assert_eq!(publish_recovered_fd(&s, &d, 7), None);
    assert_eq!(claim_for_teardown(&s, &d), Some(7), "Drop closes it");
}

/// A small thing that the `EMPTY` sentinel exists to get right: `0` is a
/// perfectly legal descriptor and must not read as an empty slot.
#[test]
fn fd_zero_is_a_real_fd_not_an_empty_slot() {
    let (s, d) = slot();
    assert_eq!(publish_recovered_fd(&s, &d, 0), None);
    assert_eq!(take_recovered_fd(&s), Some(0));
}

// ── Negative: every way the fd could go unclosed or be closed twice ────

/// Teardown wins the race. It drains an empty slot and will never look
/// again, so the late publisher must get its own fd back — this is the
/// leak the `Acquire`/`Release` orderings could produce in practice.
#[test]
fn publish_after_teardown_returns_the_fd_to_the_publisher() {
    let (s, d) = slot();
    assert_eq!(claim_for_teardown(&s, &d), None, "nothing published yet");
    assert_eq!(
        publish_recovered_fd(&s, &d, 7),
        Some(7),
        "transport is gone — publisher must close its own fd"
    );
    assert_eq!(take_recovered_fd(&s), None, "and must not leave it behind");
}

/// Two recovery threads for one transport. The loser closes the fd it
/// opened; it must not overwrite the winner's, which nothing would then
/// close.
#[test]
fn losing_publisher_closes_its_own_fd_and_leaves_the_winners() {
    let (s, d) = slot();
    assert_eq!(publish_recovered_fd(&s, &d, 7), None, "winner");
    assert_eq!(publish_recovered_fd(&s, &d, 9), Some(9), "loser closes 9");
    assert_eq!(take_recovered_fd(&s), Some(7), "winner's fd survived");
}

/// Teardown after the fd has already been drained by `execute()`. The slot
/// is empty and teardown must claim nothing — returning the stale value
/// here would close an fd the transport is still using.
#[test]
fn teardown_after_drain_claims_nothing() {
    let (s, d) = slot();
    assert_eq!(publish_recovered_fd(&s, &d, 7), None);
    assert_eq!(take_recovered_fd(&s), Some(7));
    assert_eq!(claim_for_teardown(&s, &d), None, "no double close");
}

/// Teardown twice (belt and braces — `Drop` runs once, but the second
/// claim must be inert rather than re-yielding the fd).
#[test]
fn teardown_is_idempotent() {
    let (s, d) = slot();
    assert_eq!(publish_recovered_fd(&s, &d, 7), None);
    assert_eq!(claim_for_teardown(&s, &d), Some(7));
    assert_eq!(claim_for_teardown(&s, &d), None);
}

/// Empty slot, nothing published: both drains are no-ops.
#[test]
fn draining_an_empty_slot_yields_nothing() {
    let (s, d) = slot();
    assert_eq!(take_recovered_fd(&s), None);
    assert_eq!(claim_for_teardown(&s, &d), None);
}

/// Accounting over the orders the three operations can run in, one whole
/// operation at a time: for each, the fd is handed to exactly one caller.
///
/// Sequences, not interleavings — each function is the atomic unit here, so
/// this cannot express the case the ordering bug lives in (a teardown swap
/// landing between `publish_recovered_fd`'s CAS and its `dead` load). That
/// one is only reachable in the loom models below.
#[test]
fn every_sequence_closes_the_fd_exactly_once() {
    // Each sequence returns how many callers were handed the fd across the
    // whole run; the answer is always exactly one.
    fn exactly_one(order: &str, run: impl Fn() -> usize) {
        assert_eq!(run(), 1, "`{order}` must close fd 7 exactly once");
    }

    exactly_one("publish, drain", || {
        let (s, d) = slot();
        publish_recovered_fd(&s, &d, 7).is_some() as usize
            + take_recovered_fd(&s).is_some() as usize
    });
    exactly_one("publish, teardown", || {
        let (s, d) = slot();
        publish_recovered_fd(&s, &d, 7).is_some() as usize
            + claim_for_teardown(&s, &d).is_some() as usize
    });
    exactly_one("teardown, publish", || {
        let (s, d) = slot();
        claim_for_teardown(&s, &d).is_some() as usize
            + publish_recovered_fd(&s, &d, 7).is_some() as usize
    });
    exactly_one("drain, publish, teardown", || {
        let (s, d) = slot();
        take_recovered_fd(&s).is_some() as usize
            + publish_recovered_fd(&s, &d, 7).is_some() as usize
            + claim_for_teardown(&s, &d).is_some() as usize
    });
}

/// The real race, run for real: a teardown thread against a recovery
/// thread, many times over. This exercises the protocol under a genuine
/// scheduler — it does NOT prove the memory orderings (x86 and, in
/// practice, AArch64 will happily pass the weaker ones). The loom model
/// below is what covers those; this covers the ownership logic.
#[test]
fn concurrent_teardown_and_publish_close_each_fd_exactly_once() {
    use std::sync::Arc;

    for round in 0..2_000 {
        let s = Arc::new(AtomicI32::new(EMPTY));
        let d = Arc::new(AtomicBool::new(false));
        let fd = 7;

        let (s1, d1) = (Arc::clone(&s), Arc::clone(&d));
        let teardown = std::thread::spawn(move || claim_for_teardown(&s1, &d1));
        let (s2, d2) = (Arc::clone(&s), Arc::clone(&d));
        let recovery = std::thread::spawn(move || publish_recovered_fd(&s2, &d2, fd));

        let claimed = teardown.join().unwrap();
        let returned = recovery.join().unwrap();

        let closers = claimed.is_some() as usize + returned.is_some() as usize;
        assert_eq!(
            closers, 1,
            "round {round}: fd must be closed exactly once, not {closers} times \
                 (0 = leaked descriptor, 2 = close() of a reused fd number)"
        );
        assert_eq!(
            s.load(Ordering::Acquire),
            EMPTY,
            "round {round}: slot must not be left holding an fd nobody owns"
        );
    }
}
