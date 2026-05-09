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
#[derive(Clone, Debug, Default)]
pub struct Halt(Arc<AtomicBool>);

impl Halt {
    /// Construct a fresh, uncancelled token.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
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
    fn default_impl_is_uncancelled() {
        let h = Halt::default();
        assert!(!h.is_cancelled());
    }
}
