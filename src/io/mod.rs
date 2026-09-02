//! File I/O helpers that bound kernel cache pressure on big writes.
//!
//! `WritebackFile` wraps `std::fs::File` (`Write` + `Seek`) for large
//! sequential writes. `FileSectorSource` is the read-side dual,
//! implementing [`crate::sector::SectorSource`] for ISO reads.
//! `Pipeline` + `Sink` overlaps reads with writes via a bounded
//! channel + consumer thread. `byte_prefetcher` is the read-ahead
//! producer feeding the mux pipeline for `io::Read`-backed sources.
//!
//! See docs/io-mod.md for rationale.

pub(crate) mod bounded;
pub mod byte_prefetcher;
pub mod file_sector_source;
pub mod fsync;
pub mod image_writer;
pub mod sink;
mod writeback;
mod writeback_file;

#[cfg(target_os = "macos")]
pub(crate) mod platform_macos;

pub mod pipeline;

pub use writeback_file::WritebackFile;

pub use pipeline::{
    DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH, WRITE_THROUGH_DEPTH,
};
