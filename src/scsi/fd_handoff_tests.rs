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

// ── A command waiting on a pending reopen ──────────────────────────────

/// The BU40N field failure: the command right after a transport fault ran before
/// the reopen thread published, found no fd, and failed as "device not found".
#[test]
fn await_recovered_fd_waits_for_a_pending_reopen() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    let (s, d) = slot();
    let (s, d) = (Arc::new(s), Arc::new(d));
    let pending = Arc::new(AtomicUsize::new(1));
    let (s1, d1, p1) = (Arc::clone(&s), Arc::clone(&d), Arc::clone(&pending));
    let reopen = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(publish_recovered_fd(&s1, &d1, 9), None);
        p1.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    });
    let got = await_recovered_fd(&s, &pending, std::time::Duration::from_secs(5), None).unwrap();
    reopen.join().unwrap();
    assert_eq!(got, Some(9));
}

#[test]
fn await_recovered_fd_returns_at_once_with_no_reopen_pending() {
    let (s, _) = slot();
    let pending = std::sync::atomic::AtomicUsize::new(0);
    let t0 = std::time::Instant::now();
    assert_eq!(
        await_recovered_fd(&s, &pending, std::time::Duration::from_secs(5), None).unwrap(),
        None
    );
    assert!(t0.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn await_recovered_fd_takes_an_fd_published_by_a_finished_reopen() {
    let (s, d) = slot();
    assert_eq!(publish_recovered_fd(&s, &d, 4), None);
    let pending = std::sync::atomic::AtomicUsize::new(0);
    assert_eq!(
        await_recovered_fd(&s, &pending, std::time::Duration::ZERO, None).unwrap(),
        Some(4)
    );
}

#[test]
fn await_recovered_fd_gives_up_after_its_budget() {
    let (s, _) = slot();
    let pending = std::sync::atomic::AtomicUsize::new(1);
    let t0 = std::time::Instant::now();
    assert_eq!(
        await_recovered_fd(&s, &pending, std::time::Duration::from_millis(30), None).unwrap(),
        None
    );
    assert!(t0.elapsed() >= std::time::Duration::from_millis(30));
}

#[test]
fn recovery_stop_interrupts_pending_wait_without_dispatch_or_consuming_late_fd() {
    use crate::halt::{Halt, WAIT_SLICE};
    use std::sync::atomic::{AtomicUsize, Ordering::Acquire};
    use std::time::{Duration, Instant};
    let (s, d) = slot();
    let pending = AtomicUsize::new(1);
    let h = Halt::new();
    let dispatches = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (s, pending, token, dispatches) = (&s, &pending, &h, &dispatches);
        let waiter = scope.spawn(move || {
            started_tx.send(()).unwrap();
            let r = await_recovered_fd(s, pending, Duration::from_secs(5), Some(token));
            if matches!(r, Ok(Some(_))) {
                dispatches.fetch_add(1, Ordering::AcqRel);
            }
            r
        });
        started_rx.recv().unwrap();
        std::thread::sleep(WAIT_SLICE);
        let cancelled = Instant::now();
        h.cancel();
        let r = waiter.join().unwrap();
        assert!(matches!(r, Err(crate::Error::Halted)), "{r:?}");
        // Loose for a loaded runner, far below the 5 s wait an ignored cancel would run.
        assert!(
            cancelled.elapsed() < Duration::from_secs(1),
            "cancel took {:?}",
            cancelled.elapsed()
        );
    });
    assert_eq!(dispatches.load(Acquire), 0);
    assert_eq!(publish_recovered_fd(&s, &d, 9), None);
    assert_eq!(claim_for_teardown(&s, &d), Some(9));
    assert_eq!(claim_for_teardown(&s, &d), None);
}

#[test]
fn recovery_stop_leaves_an_already_published_fd_for_teardown() {
    let (s, d) = slot();
    let pending = std::sync::atomic::AtomicUsize::new(0);
    let h = crate::halt::Halt::new();
    assert_eq!(publish_recovered_fd(&s, &d, 9), None);
    h.cancel();
    let r = await_recovered_fd(&s, &pending, std::time::Duration::ZERO, Some(&h));
    assert!(matches!(r, Err(crate::Error::Halted)), "{r:?}");
    assert_eq!(claim_for_teardown(&s, &d), Some(9));
    assert_eq!(claim_for_teardown(&s, &d), None);
}

#[test]
fn recovery_stop_racing_publication_keeps_exactly_one_fd_owner() {
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    for _ in 0..100 {
        let (s, d) = slot();
        let pending = AtomicUsize::new(1);
        let h = crate::halt::Halt::new();
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            let (slot_ref, pending_ref, token, start2) = (&s, &pending, &h, &start);
            let waiter = scope.spawn(move || {
                start2.wait();
                await_recovered_fd(
                    slot_ref,
                    pending_ref,
                    std::time::Duration::from_secs(1),
                    Some(token),
                )
            });
            start.wait();
            h.cancel();
            let publisher_close = publish_recovered_fd(&s, &d, 9);
            pending.store(0, std::sync::atomic::Ordering::Release);
            let transport_close = match waiter.join().unwrap() {
                Ok(fd) => fd,
                Err(crate::Error::Halted) => None,
                r => panic!("unexpected wait result: {r:?}"),
            };
            let teardown_close = claim_for_teardown(&s, &d);
            assert_eq!(
                publisher_close.is_some() as usize
                    + transport_close.is_some() as usize
                    + teardown_close.is_some() as usize,
                1
            );
            assert_eq!(claim_for_teardown(&s, &d), None);
        });
    }
}

#[test]
fn recovery_stop_uncancelled_token_preserves_ready_timeout_and_no_pending() {
    use std::sync::atomic::{AtomicUsize, Ordering::Release};
    use std::time::{Duration, Instant};
    let (s, d) = slot();
    let pending = AtomicUsize::new(0);
    let halt = crate::halt::Halt::new();
    assert_eq!(publish_recovered_fd(&s, &d, 9), None);
    assert_eq!(
        await_recovered_fd(&s, &pending, Duration::ZERO, Some(&halt)).unwrap(),
        Some(9)
    );
    let start = Instant::now();
    assert_eq!(
        await_recovered_fd(&s, &pending, Duration::from_secs(5), Some(&halt)).unwrap(),
        None
    );
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "no pending reopen waits for no budget"
    );
    pending.store(1, Release);
    let start = Instant::now();
    assert_eq!(
        await_recovered_fd(&s, &pending, Duration::from_millis(30), Some(&halt)).unwrap(),
        None
    );
    assert!(start.elapsed() >= Duration::from_millis(30));
    assert_eq!(
        claim_for_teardown(&s, &d),
        None,
        "successful caller owns the fd"
    );
}
