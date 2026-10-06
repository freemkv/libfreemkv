use super::*;
use loom::sync::Arc;
use loom::sync::atomic::AtomicUsize;

/// `Drop` against a recovery thread: the execution this file exists for.
///
/// With `swap(.., Acquire)` in [`claim_for_teardown`], or with
/// `compare_exchange(.., Release, ..)` in [`publish_recovered_fd`] — either
/// one alone, not just both — loom finds an execution where teardown
/// drains the empty slot, the recovery thread's CAS then fills it, and the
/// `dead` load still reads `false`. Nobody closes the fd.
#[test]
fn loom_teardown_racing_publish_closes_the_fd_exactly_once() {
    loom::model(|| {
        let slot = Arc::new(AtomicI32::new(EMPTY));
        let dead = Arc::new(AtomicBool::new(false));
        let closes = Arc::new(AtomicUsize::new(0));

        let (s, d, c) = (slot.clone(), dead.clone(), closes.clone());
        let teardown = loom::thread::spawn(move || {
            if claim_for_teardown(&s, &d).is_some() {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });

        let (s, d, c) = (slot.clone(), dead.clone(), closes.clone());
        let recovery = loom::thread::spawn(move || {
            if publish_recovered_fd(&s, &d, 7).is_some() {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });

        teardown.join().unwrap();
        recovery.join().unwrap();

        assert_eq!(
            closes.load(Ordering::Relaxed),
            1,
            "fd must be closed exactly once (0 = leaked, 2 = double close)"
        );
        assert_eq!(slot.load(Ordering::Relaxed), EMPTY, "slot left non-empty");
    });
}

/// Two recovery threads against a teardown. `execute()` spawns a thread on
/// every transport-level failure and an earlier one may still be blocked in
/// `open()`, so this is reachable, and it is the case where a CAS lands
/// behind an intervening slot operation rather than reading the teardown
/// swap's value directly — the release-sequence half of the module docs.
///
/// Bounded rather than exhaustive: unbounded, this model runs for many
/// minutes, which is too slow for the CI step. Three preemptions is enough
/// to reach the leaking schedule — the old orderings fail this test.
#[test]
fn loom_two_publishers_racing_teardown_close_each_fd_exactly_once() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let slot = Arc::new(AtomicI32::new(EMPTY));
        let dead = Arc::new(AtomicBool::new(false));
        let closes = Arc::new(AtomicUsize::new(0));

        let publishers: Vec<_> = [7, 9]
            .into_iter()
            .map(|fd| {
                let (s, d, c) = (slot.clone(), dead.clone(), closes.clone());
                loom::thread::spawn(move || {
                    if publish_recovered_fd(&s, &d, fd).is_some() {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        let (s, d, c) = (slot.clone(), dead.clone(), closes.clone());
        let teardown = loom::thread::spawn(move || {
            if claim_for_teardown(&s, &d).is_some() {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });

        for p in publishers {
            p.join().unwrap();
        }
        teardown.join().unwrap();

        assert_eq!(
            closes.load(Ordering::Relaxed),
            2,
            "both fds must be closed, exactly once each"
        );
        assert_eq!(slot.load(Ordering::Relaxed), EMPTY, "slot left non-empty");
    });
}

/// The check on leaving [`take_recovered_fd`] at `Acquire` — i.e. with a
/// relaxed store half. A drain landing between the teardown swap and a
/// recovery CAS must not break the release sequence the edge rides on.
#[test]
fn loom_drain_does_not_break_the_release_sequence() {
    loom::model(|| {
        let slot = Arc::new(AtomicI32::new(EMPTY));
        let dead = Arc::new(AtomicBool::new(false));
        let closes = Arc::new(AtomicUsize::new(0));

        // execute() drains, then (later, same transport) Drop tears down.
        let (s, d, c) = (slot.clone(), dead.clone(), closes.clone());
        let transport = loom::thread::spawn(move || {
            if take_recovered_fd(&s).is_some() {
                c.fetch_add(1, Ordering::Relaxed);
            }
            if claim_for_teardown(&s, &d).is_some() {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });

        let (s, d, c) = (slot.clone(), dead.clone(), closes.clone());
        let recovery = loom::thread::spawn(move || {
            if publish_recovered_fd(&s, &d, 7).is_some() {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });

        transport.join().unwrap();
        recovery.join().unwrap();

        assert_eq!(
            closes.load(Ordering::Relaxed),
            1,
            "fd must be closed exactly once (0 = leaked, 2 = double close)"
        );
        assert_eq!(slot.load(Ordering::Relaxed), EMPTY, "slot left non-empty");
    });
}
