//! The fd hand-off between a Linux recovery thread and the transport that
//! spawned it.
//!
//! When `SgIoTransport::execute` sees a transport-level failure it abandons
//! its fd and spawns a detached thread to open a fresh one. That thread has no
//! reference back to the transport — the transport may have been dropped
//! before the `open()` returns — so the two sides rendezvous through a
//! single-slot mailbox: an `AtomicI32` holding an owned fd (`-1` = empty) and
//! an `AtomicBool` saying whether the transport is already gone.
//!
//! The invariant, and the only thing that matters here: **every fd that
//! reaches the slot is closed exactly once.** Closing zero times leaks a
//! descriptor; closing twice is worse than a leak, because a `close()` of an
//! fd number the process has since reused closes an unrelated file.
//!
//! # Why this lives in `scsi/`, not in `linux.rs`
//!
//! `linux.rs` is compiled on exactly one host, so nothing in it can be tested
//! on macOS or Windows CI — the same reason [`super::checked_cdb_len`] lives
//! in the parent module. The protocol below is plain atomics with no syscalls
//! in it, so it is compiled and tested on every platform even though Linux is
//! its only caller.
//!
//! # Memory ordering
//!
//! The subtle case is a `Drop` that races the recovery thread. The two sides
//! are ordered only through the slot, so the release edge has to be carried by
//! the slot's own operations:
//!
//! 1. [`claim_for_teardown`] publishes `dead = true` and then claims the slot.
//! 2. [`publish_recovered_fd`] fills the slot and then reads `dead`.
//!
//! Publish-first needs nothing special: teardown's swap is an RMW, so it reads
//! the last value in the slot's modification order — the fd — and closes it by
//! coherence alone. The dangerous order is teardown-first, where
//! `claim_for_teardown` finds the slot empty and returns, leaving the recovery
//! thread as the only one that can close the fd — and it will only do so if its
//! `dead` load observes the `true`.
//!
//! That observation is not free. Rust's atomics follow the C++20 rules
//! (`std::sync::atomic` module docs: "Rust atomics currently follow the same
//! rules as C++20 atomics, specifically the rules from the `intro.races`
//! section, without the 'consume' memory ordering"), and under those rules:
//!
//! - Every operation on the slot is a read-modify-write — `swap` or
//!   `compare_exchange`, never a plain store after construction. That matters,
//!   because "a release sequence headed by a release operation A on an atomic
//!   object M is a maximal contiguous sub-sequence of side effects in the
//!   modification order of M, where the first operation is A, and every
//!   subsequent operation is an atomic read-modify-write operation"
//!   ([intro.races], N4868). So once `claim_for_teardown`'s `swap` executes,
//!   *every* later slot operation is in its release sequence — including one
//!   that lands behind an intervening drain or a second recovery thread,
//!   which is why the argument does not depend on the CAS reading the `swap`'s
//!   value directly.
//! - Landing in that sequence is still not enough to synchronize with its
//!   head. That needs both halves: "an atomic operation A that performs a
//!   release operation on an atomic object M synchronizes with an atomic
//!   operation B that performs an acquire operation on M and takes its value
//!   from any side effect in the release sequence headed by A"
//!   ([atomics.order]/2, N4868).
//!
//! Both halves of that were missing:
//!
//! - `swap(-1, Acquire)` is not a release store. Per the [`Ordering::Acquire`]
//!   docs, "using this ordering for an operation that combines loads and
//!   stores leads to a `Relaxed` store operation" — so it heads no release
//!   sequence, and there was nothing for the CAS to synchronize with.
//! - `compare_exchange(.., Release, Relaxed)` is not an acquire load. Per the
//!   [`Ordering::Release`] docs, it "leads to a `Relaxed` load operation" — so
//!   even against a proper release store it would establish no edge.
//!
//! Upgrading only one side therefore fixes nothing; the loom models in this
//! file still report the leak with either half applied alone. Both are
//! `AcqRel` below.
//!
//! Concretely, the CAS in [`publish_recovered_fd`] carries both roles at once:
//! its release half publishes the `open()` to [`take_recovered_fd`], and its
//! acquire half is what lets the `dead` load that follows observe a teardown
//! that has already claimed the slot.
//!
//! [`take_recovered_fd`] is deliberately left at `Acquire`, which makes its
//! store half relaxed. That is harmless precisely because of the release-
//! sequence rule above: an RMW extends the sequence whatever its own ordering,
//! so a drain landing between `claim_for_teardown` and a later CAS does not
//! break the edge. It needs no release half of its own — it is called from
//! `execute()`, which cannot run concurrently with `Drop` (both need the
//! transport), so it never has anything of its own to publish to a teardown.
//! `loom_drain_does_not_break_the_release_sequence` is the check on that.
//!
//! `dead`'s `Release` store is belt-and-braces: it is sequenced before the
//! `AcqRel` swap, and the swap is what carries the edge. Downgrading it to
//! `Relaxed` leaves all three models passing.
//!
//! # Building the model
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --lib --no-default-features \
//!     --features scsi fd_handoff
//! ```
//!
//! Under those flags the protocol runs on loom's instrumented atomics, so the
//! models check the code the transport actually calls rather than a
//! transcription that could drift from it. The alias below is gated on `test`
//! as well as `loom`, because `loom` is a DEV-dependency and so is not in the
//! graph of a non-test target: `cfg(loom)` alone would break `cargo build`,
//! `cargo check` and `cargo clippy --all-targets` under those flags with an
//! unresolved-crate error. Non-test targets keep std's atomics.
//!
//! One consequence to know about: under `all(loom, test)`, `SgIoTransport`'s
//! atomics are loom's, and those panic outside a `loom::model`. No unit test
//! constructs a transport today — `raw_command` is a free-standing `fn` that
//! touches none — but the first one that does must live behind
//! `cfg(not(loom))`.

// See "Building the model" above for why this is gated on `test` too.
#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicI32, Ordering};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// The empty slot. Not a valid fd; `open()` never returns a negative value.
pub(crate) const EMPTY: i32 = -1;

/// Interpret a value taken out of the slot. Only a non-negative value is an fd
/// the caller now owns; `EMPTY` — or any other negative, which nothing can put
/// there — is nothing to close. Deliberately `>= 0` rather than `!= EMPTY`, to
/// keep the guard the call sites used before this protocol was factored out.
fn taken(value: i32) -> Option<i32> {
    (value >= 0).then_some(value)
}

/// Take whatever fd a recovery thread has published, transferring ownership to
/// the caller. `None` if no recovery has completed.
///
/// Called from `execute()` at the top of a command, which is why it does not
/// consult `dead`: a live `execute()` means the transport is not being torn
/// down. `Acquire` pairs with the `AcqRel` CAS in [`publish_recovered_fd`] so
/// the `open()` that produced the fd happens-before the caller uses it.
pub(crate) fn take_recovered_fd(slot: &AtomicI32) -> Option<i32> {
    taken(slot.swap(EMPTY, Ordering::Acquire))
}

/// Publish a freshly opened fd from a recovery thread. Returns the fd the
/// **caller** must now close, or `None` if it was handed off:
///
/// - The slot was already full — another recovery thread won, so we close
///   ours rather than overwrite (and leak) the winner's.
/// - The transport was torn down mid-`open()`; nothing will drain the slot,
///   so we re-claim through it rather than close `new_fd` directly, which
///   would double-close against a teardown swapping at the same moment.
///
/// Both orderings below are `AcqRel`; the module docs say why.
pub(crate) fn publish_recovered_fd(
    slot: &AtomicI32,
    dead: &AtomicBool,
    new_fd: i32,
) -> Option<i32> {
    // A negative `new_fd` would go in as the `EMPTY` sentinel, or as a value no
    // drain will hand back — silently lost either way. The caller checks
    // `open()` before getting here; this pins that as the contract.
    debug_assert!(new_fd >= 0, "only a real fd may enter the slot");
    if slot
        .compare_exchange(EMPTY, new_fd, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return Some(new_fd);
    }
    if dead.load(Ordering::Acquire) {
        return taken(slot.swap(EMPTY, Ordering::AcqRel));
    }
    None
}

/// Mark the transport dead and claim any fd still sitting in the slot,
/// transferring ownership to the caller. Called from `Drop`.
///
/// The `dead` store must be ordered before the claim: a recovery thread that
/// fills the slot after we have drained it has to see `dead == true`, or its
/// fd is never closed by anyone. `AcqRel` on the swap is what carries that
/// edge — see the module docs for why `Acquire` alone did not.
pub(crate) fn claim_for_teardown(slot: &AtomicI32, dead: &AtomicBool) -> Option<i32> {
    dead.store(true, Ordering::Release);
    taken(slot.swap(EMPTY, Ordering::AcqRel))
}

/// Ownership tests. These run on every platform's CI (the module has no Linux
/// in it) and drive each case directly rather than hoping a thread schedule
/// produces it. The assertion is always the same one: an fd that enters the
/// slot leaves it through exactly one caller.
///
/// They cover the ownership protocol, NOT the memory orderings — those compile
/// to the same instructions on x86_64, so these pass against the wrong ones.
/// [`loom_tests`] is what covers the orderings.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// Fresh slot + liveness flag, as `SgIoTransport::open` builds them.
    fn slot() -> (AtomicI32, AtomicBool) {
        (AtomicI32::new(EMPTY), AtomicBool::new(false))
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
}

/// Memory-ordering models. `cargo test` cannot observe a missing release edge:
/// the orderings compile to the same instructions on x86_64, and on AArch64 the
/// window is far too narrow to hit by chance. loom enumerates the executions
/// the C++20 model permits instead, and reports the stale `dead` load directly.
#[cfg(all(test, loom))]
mod loom_tests {
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
}
