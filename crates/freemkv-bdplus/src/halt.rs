//! Cooperative cancellation independent of the embedding application.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// A shared, one-way cancellation flag for bounded engine execution.
#[derive(Clone, Debug, Default)]
pub struct Halt(Arc<AtomicBool>);

impl Halt {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
