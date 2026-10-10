//! File I/O helpers that bound kernel cache pressure on big writes.
//!
//! `WritebackFile` wraps `std::fs::File` (`Write` + `Seek`) for large
//! sequential writes. `FileSectorSource` is the read-side dual,
//! implementing [`crate::sector::SectorSource`] for ISO reads.
//! `Pipeline` + `Sink` overlaps reads with writes via a bounded
//! channel + consumer thread.

pub mod artifact_lock;
pub mod block_sink;
pub(crate) mod bounded;
pub mod file_sector_source;
mod flush;
pub mod fsync;
pub mod image_writer;
pub mod publish;
mod writeback;
mod writeback_file;

#[cfg(target_os = "macos")]
pub(crate) mod platform_macos;

pub mod pipeline;
pub mod tree_sink;

pub use artifact_lock::ArtifactLock;
pub use block_sink::{
    BlockSink, IsoSink, NullBlockSink, is_null_device, null_device, open_block_sink,
};
pub use flush::{FlushProgress, durable_sync_file};
pub use tree_sink::{TreeSink, open_tree_sink};
pub use writeback_file::WritebackFile;

pub use pipeline::{
    DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH, WRITE_THROUGH_DEPTH,
};
