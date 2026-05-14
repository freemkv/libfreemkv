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
//! `FileSectorSource` is the read-side dual — it implements
//! [`crate::sector::SectorSource`] for an ISO file with an internal
//! 32 MiB read-ahead buffer that amortises NFS round-trip latency
//! across thousands of sector reads.
//!
//! `Pipeline` + `Sink` (0.18) is the generic producer/consumer primitive
//! used by sweep, patch, and mux to overlap reads with writes via a
//! bounded channel + dedicated consumer thread.
//!
//! `byte_channel` is a byte-sized producer/consumer channel for the
//! mux pipeline, sized to absorb worst-case input read stalls (see
//! `freemkv-private/memory/project_buffering_architecture.md`).

pub(crate) mod bounded;
pub mod byte_channel;
pub mod file_sector_source;
pub mod sink;
mod writeback;
mod writeback_file;

pub mod pipeline;

pub(crate) use writeback_file::WritebackFile;

// Re-exports for the 0.18 redesign. Sweep + patch are both wired up
// (disc/sweep.rs, disc/patch.rs); mux migrates separately in autorip.
// `WRITE_THROUGH_DEPTH` is patch-specific and has no other in-tree
// caller — the targeted `#[allow]` keeps the re-export visible without
// dragging the rest of the module under `dead_code`.
#[allow(unused_imports)]
pub use pipeline::{
    DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, READ_PIPELINE_DEPTH, Sink, WRITE_PIPELINE_DEPTH,
    WRITE_THROUGH_DEPTH,
};
