//! One-bit cooperative cancellation flag.
//!
//! `Halt` is a clonable token wrapping `Arc<AtomicBool>`. Pass clones into
//! every long-running loop; the loop polls `is_cancelled()` and bails out
//! cleanly. Calling `cancel()` from any clone flips the shared flag, and
//! every other clone observes it on its next poll.
//!
//! Why: `Ordering::Relaxed` is sufficient on both load and store because
//! this flag is purely advisory — no other memory operations piggyback on
//! it for happens-before ordering. Callers that need to publish data
//! across threads do so via channels or other synchronization, not via
//! this bit.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Clonable, infallible cooperative-cancellation token.
///
/// Clones share the same underlying flag. `cancel()` is one-way; there is
/// no `reset()` by design — construct a fresh `Halt` for a fresh
/// operation.
///
/// Construct with [`Halt::new`] (or [`Halt::default`]). The `Default`
/// impl forwards to `new()` — both produce a fresh, uncancelled token.
/// The pair exists because clippy's `new_without_default` lint requires
/// `Default` whenever a public `new()` is present, even when the two
/// would do exactly the same thing.
#[derive(Clone, Debug)]
pub struct Halt(Arc<AtomicBool>);

impl Halt {
    /// Construct a fresh, uncancelled token.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Wrap an existing `Arc<AtomicBool>` as a `Halt`. A bridge for
    /// callers that already hold an `Arc<AtomicBool>` cancellation flag
    /// and want to adopt the token API without allocating a new flag.
    ///
    /// Cancelling either side flips the same bit — the wrapping `Halt`
    /// and the original `Arc` are two views over one shared flag.
    pub fn from_arc(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }

    /// Borrow the underlying `Arc<AtomicBool>`. The inverse of
    /// [`from_arc`](Self::from_arc): hand the shared flag to an API that
    /// still takes a raw `Arc<AtomicBool>` rather than a `Halt`.
    pub fn as_arc(&self) -> &Arc<AtomicBool> {
        &self.0
    }

    /// Flip the shared flag to cancelled. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Read the shared flag.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for Halt {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared poll interval for halt-aware loops.
///
/// `bounded_syscall` checks the cancellation flag and the deadline
/// every [`POLL_INTERVAL`] while blocked on a worker; the same
/// cadence governs `Pipeline::send_with_halt`'s `try_send` retry.
/// 250 ms is the sweet spot between responsiveness (operator presses
/// Stop, sees it take effect within ~quarter-second) and waste
/// (atomic load + clock read is cheap but not free at thousands of
/// hertz).
///
/// Centralised here so the half-dozen halt-polling loops across `io`
/// can't drift apart silently.
pub const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_is_not_cancelled() {
        let h = Halt::new();
        assert!(!h.is_cancelled());
    }

    #[test]
    fn cancel_flips_state() {
        let h = Halt::new();
        assert!(!h.is_cancelled());
        h.cancel();
        assert!(h.is_cancelled());
    }

    #[test]
    fn cancel_is_idempotent() {
        let h = Halt::new();
        h.cancel();
        h.cancel();
        assert!(h.is_cancelled());
    }

    #[test]
    fn clone_shares_state() {
        let original = Halt::new();
        let cloned = original.clone();
        assert!(!original.is_cancelled());
        assert!(!cloned.is_cancelled());

        // Cancel via the clone; the original observes it.
        cloned.cancel();
        assert!(original.is_cancelled());
        assert!(cloned.is_cancelled());
    }

    #[test]
    fn clone_shares_state_reverse_direction() {
        let original = Halt::new();
        let cloned = original.clone();

        // Cancel via the original; the clone observes it.
        original.cancel();
        assert!(cloned.is_cancelled());
    }

    #[test]
    fn clone_shares_state_across_threads() {
        let h = Halt::new();
        let h2 = h.clone();
        let handle = std::thread::spawn(move || {
            h2.cancel();
        });
        handle.join().unwrap();
        assert!(h.is_cancelled());
    }

    #[test]
    fn from_arc_shares_state() {
        // The 0.18 deprecation-window bridge: a Halt built from an
        // existing Arc<AtomicBool> must be a *view* over the same bit,
        // not a fresh copy. Cancelling either side flips both.
        let arc = Arc::new(AtomicBool::new(false));
        let halt = Halt::from_arc(arc.clone());
        assert!(!halt.is_cancelled());
        assert!(!arc.load(Ordering::Relaxed));

        // Cancel via the wrapping Halt; the original Arc observes it.
        halt.cancel();
        assert!(arc.load(Ordering::Relaxed));

        // Conversely: flip the Arc directly; the Halt view observes it.
        let arc2 = Arc::new(AtomicBool::new(false));
        let halt2 = Halt::from_arc(arc2.clone());
        arc2.store(true, Ordering::Relaxed);
        assert!(halt2.is_cancelled());
    }

    #[test]
    fn as_arc_returns_backing_flag() {
        // `as_arc()` must hand back the *same* Arc, not a clone of a
        // different bit. Verified by writing through the borrowed Arc
        // and observing through the Halt.
        let halt = Halt::new();
        let arc = halt.as_arc().clone();
        assert!(!halt.is_cancelled());
        arc.store(true, Ordering::Relaxed);
        assert!(halt.is_cancelled());
    }

    // ── New comprehensive tests ────────────────────────────────────────────────

    /// Default::default() produces a fresh, uncancelled token (same as new()).
    /// Spec: doc says "The Default impl forwards to new() — both produce a fresh,
    ///       uncancelled token."
    /// Mutation: having Default initialize to `cancelled=true` would break all
    ///           callers that rely on a default-constructed Halt being uncancelled.
    #[test]
    fn default_produces_uncancelled_token() {
        let h = Halt::default();
        assert!(
            !h.is_cancelled(),
            "Default::default() must produce uncancelled token"
        );
    }

    /// Multiple clones of the same Halt all observe a cancel from any one of them.
    /// Mutation: cloning the Arc by value (separate allocation) means clones don't share state.
    #[test]
    fn multiple_clones_all_share_same_flag() {
        let h0 = Halt::new();
        let h1 = h0.clone();
        let h2 = h0.clone();
        let h3 = h1.clone();
        // None cancelled yet.
        assert!(!h0.is_cancelled());
        assert!(!h1.is_cancelled());
        assert!(!h2.is_cancelled());
        assert!(!h3.is_cancelled());
        // Cancel via h2; all others must observe it.
        h2.cancel();
        assert!(h0.is_cancelled());
        assert!(h1.is_cancelled());
        assert!(h3.is_cancelled());
    }

    /// is_cancelled is non-destructive — reading the flag multiple times returns
    /// the same result.
    /// Mutation: using swap(false) instead of load would clear the flag on read.
    #[test]
    fn is_cancelled_is_non_destructive() {
        let h = Halt::new();
        h.cancel();
        assert!(h.is_cancelled());
        assert!(h.is_cancelled(), "second read must also return true");
        assert!(h.is_cancelled(), "third read must also return true");
    }

    /// from_arc followed by cancel(), then as_arc() load: the raw Arc must see the write.
    /// This is the round-trip that proves from_arc and as_arc are exact inverses.
    /// Mutation: from_arc doing `Arc::new(flag.load(...))` (copy not share) breaks this.
    #[test]
    fn from_arc_and_as_arc_are_inverses() {
        let original = Arc::new(AtomicBool::new(false));
        let halt = Halt::from_arc(original.clone());
        // Cancel via the Halt; read via the original Arc.
        halt.cancel();
        assert!(
            original.load(Ordering::Relaxed),
            "cancel() must be visible via the original Arc"
        );
        // The Arc retrieved by as_arc() must be the same one.
        let retrieved = halt.as_arc();
        assert!(
            std::ptr::eq(Arc::as_ptr(retrieved), Arc::as_ptr(&original)),
            "as_arc must return the same Arc pointer as was passed to from_arc"
        );
    }

    /// POLL_INTERVAL is 250ms — a specific value that the multi-thread halt
    /// loops depend on for responsiveness guarantees.
    /// Mutation: setting POLL_INTERVAL to 5s makes stop requests take 5s to notice.
    #[test]
    fn poll_interval_is_250ms() {
        assert_eq!(
            POLL_INTERVAL,
            std::time::Duration::from_millis(250),
            "POLL_INTERVAL must be 250ms for the guaranteed ~quarter-second cancel latency"
        );
    }

    /// cancel() then clone: the clone of an already-cancelled Halt starts cancelled.
    /// Mutation: cloning by re-reading the bool (not the Arc) would give a fresh false.
    #[test]
    fn clone_of_cancelled_halt_is_also_cancelled() {
        let h = Halt::new();
        h.cancel();
        let cloned = h.clone();
        assert!(
            cloned.is_cancelled(),
            "clone of a cancelled Halt must itself be cancelled"
        );
    }
}
