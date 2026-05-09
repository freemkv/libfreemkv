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

    /// Wrap an existing `Arc<AtomicBool>` as a `Halt`. Useful as a
    /// bridge during the 0.18 deprecation window: callers that already
    /// hold an `Arc<AtomicBool>` (e.g. `Drive::halt_flag()`, the
    /// deprecated `DiscStream::set_halt`) can adopt the new token API
    /// without changing the underlying flag.
    ///
    /// Cancelling either side flips the same bit — the wrapping `Halt`
    /// and the original `Arc` are two views over one shared flag.
    pub fn from_arc(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }

    /// Borrow the underlying `Arc<AtomicBool>`. Used at boundaries with
    /// pre-`Halt` APIs that still take an `Arc<AtomicBool>` directly
    /// (`CopyOptions::halt`, the deprecated `DiscStream::set_halt`).
    /// Round 3 deletes those boundaries and this accessor with them.
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
}
