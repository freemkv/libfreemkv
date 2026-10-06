//! Owned-fd handoff between Linux transport recovery and teardown.
//!
//! Every fd published to the atomic slot must be claimed and closed exactly once.
//! Teardown stores `dead = true` before its AcqRel swap; publication uses an
//! AcqRel CAS before checking `dead`. Both halves are required for the publisher
//! to observe teardown when it claims an empty slot after teardown has returned.
//! All slot updates are RMWs, so intervening drains preserve the release sequence.
//! The normal drain needs only Acquire because it cannot race its own transport's Drop.
//!
//! The protocol is platform-independent and tested with Loom's atomics:
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --lib --no-default-features --features scsi fd_handoff
//! ```
//! Under `all(loom, test)`, callers must execute inside `loom::model`.

// Gated on `test` too: a `--cfg loom` build of the library alone keeps std atomics.
#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicI32, Ordering};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// Reserve one of `max` recovery-thread slots on `counter`, atomically (no
/// load-then-add TOCTOU). `false` past the cap, with nothing left reserved.
pub(crate) fn reserve_recovery_slot(counter: &std::sync::atomic::AtomicUsize, max: usize) -> bool {
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    // CAS loop: a refused reserver never bumps the counter, so it can't starve a fitting one.
    let mut n = counter.load(Acquire);
    while n < max {
        match counter.compare_exchange_weak(n, n + 1, AcqRel, Acquire) {
            Ok(_) => return true,
            Err(now) => n = now,
        }
    }
    false
}

/// Give back a slot taken by [`reserve_recovery_slot`].
pub(crate) fn release_recovery_slot(counter: &std::sync::atomic::AtomicUsize) {
    counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
}

/// The fd `Drop` should send ALLOW MEDIUM REMOVAL on, if any: only when this
/// transport holds a PREVENT, only on an fd it already owns (the live one, else
/// a published recovery fd), and never after an ALLOW already hit a dead bus.
pub(crate) fn drop_unlock_fd(
    prevent_held: bool,
    allow_transport_failed: bool,
    fd: i32,
    recovered: Option<i32>,
) -> Option<i32> {
    if !prevent_held || allow_transport_failed {
        return None;
    }
    if fd >= 0 { Some(fd) } else { recovered }
}

/// How `Drop` sends its ALLOW MEDIUM REMOVAL.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DropUnlock {
    /// No PREVENT of ours to clear, or the thread cap is full (SG_IO already
    /// stuck): just close.
    Skip,
    /// On a capped detached thread: SG_IO on a wedged node can block for long.
    /// If the spawn itself fails, the caller sends ALLOW inline instead.
    Detached(i32),
}

/// Pick the [`DropUnlock`] for `unlock_fd` (from [`drop_unlock_fd`]), given
/// whether a recovery-thread slot was reserved.
pub(crate) fn drop_unlock_plan(unlock_fd: Option<i32>, slot: bool) -> DropUnlock {
    match unlock_fd {
        Some(fd) if slot => DropUnlock::Detached(fd),
        _ => DropUnlock::Skip,
    }
}

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
#[path = "fd_handoff_tests.rs"]
mod tests;

/// Memory-ordering models. `cargo test` cannot observe a missing release edge:
/// the orderings compile to the same instructions on x86_64, and on AArch64 the
/// window is far too narrow to hit by chance. loom enumerates the executions
/// the C++20 model permits instead, and reports the stale `dead` load directly.
#[cfg(all(test, loom))]
#[path = "fd_handoff_loom_tests.rs"]
mod loom_tests;
