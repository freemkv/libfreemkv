//! File I/O helpers that bound kernel cache pressure on big writes.
//!
//! `Writer` is a drop-in wrapper around `std::fs::File` for any call
//! site that performs large sequential writes (sweep, mux, etc.). It
//! implements `Write` and `Seek` so existing code paths can swap
//! `File` for `Writer` with no body changes. Internally it drives a
//! `WritebackPipeline` that, on Linux, drains dirty pages continuously
//! at 32 MB granularity to avoid the kernel's accumulate-then-burst
//! flush behaviour. macOS and Windows use a no-op pipeline — their
//! default cache policies have not been shown to exhibit the same
//! pathology for this access pattern.

mod writeback;
mod writer;

pub mod pipeline;

pub(crate) use writer::Writer;

// Re-exports for the 0.18 redesign. Currently flagged unused because
// no in-tree call site has been migrated yet (sweep is still on
// `disc/sweep_pipeline.rs`; patch and mux have no pipeline). The next
// 0.18 slice removes this allow as it wires up the first consumer.
#[allow(unused_imports)]
pub use pipeline::{Apply, DEFAULT_DEPTH, Pipeline, Sink};
