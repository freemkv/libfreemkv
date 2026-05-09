//! File I/O helpers that bound kernel cache pressure on big writes.
//!
//! `WritebackFile` is a drop-in wrapper around `std::fs::File` for any
//! call site that performs large sequential writes (sweep, patch, mux,
//! etc.). It implements `Write` and `Seek` so existing code paths can
//! swap `File` for `WritebackFile` with no body changes. Internally it
//! drives a `WritebackPipeline` that, on Linux, drains dirty pages
//! continuously at 32 MB granularity to avoid the kernel's
//! accumulate-then-burst flush behaviour. macOS and Windows use a
//! no-op pipeline — their default cache policies have not been shown
//! to exhibit the same pathology for this access pattern.
//!
//! `Pipeline` + `Sink` (0.18) is the generic producer/consumer primitive
//! used by sweep, patch, and mux to overlap reads with writes via a
//! bounded channel + dedicated consumer thread.

mod writeback;
mod writeback_file;

pub mod pipeline;

pub(crate) use writeback_file::WritebackFile;

// Re-exports for the 0.18 redesign. `Disc::patch` (0.18 round 2)
// uses `Pipeline` + `Sink` + `Flow` + `WRITE_THROUGH_DEPTH`. Sweep
// and mux still need to be migrated; until they are,
// `DEFAULT_PIPELINE_DEPTH` is unused outside tests.
#[allow(unused_imports)]
pub use pipeline::{DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, Sink, WRITE_THROUGH_DEPTH};
