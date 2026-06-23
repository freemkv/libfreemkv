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
//! [`crate::sector::SectorSource`] for an ISO file using direct
//! `pread`-equivalent calls so the kernel's own readahead policy runs
//! (which interleaves naturally with the concurrent writeback). It
//! pairs that with periodic `posix_fadvise(DONTNEED)` drops on the
//! consumed window so an 85 GB streaming ISO read doesn't fill the
//! page cache and starve the concurrent MKV write.
//!
//! `Pipeline` + `Sink` is the generic producer/consumer primitive
//! used by sweep, patch, and mux to overlap reads with writes via a
//! bounded channel + dedicated consumer thread.
//!
//! `byte_prefetcher` is the read-ahead producer feeding the mux
//! pipeline for `io::Read`-backed sources: a worker thread fills a
//! recycled pool of buffers and ships them through a channel, exposing
//! `BytePrefetcher` / `PrefetchShell`.

pub(crate) mod bounded;
pub mod byte_prefetcher;
pub mod file_sector_source;
pub mod fsync;
pub mod sink;
mod writeback;
mod writeback_file;

#[cfg(target_os = "macos")]
pub(crate) mod platform_macos;

pub mod pipeline;

pub(crate) use writeback_file::WritebackFile;

pub use pipeline::{
    DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, READ_PIPELINE_DEPTH, Sink, WRITE_PIPELINE_DEPTH,
    WRITE_THROUGH_DEPTH,
};
