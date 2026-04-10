//! MKV muxing pipeline.
//!
//! Provides BD transport stream → MKV remuxing via composable streams.
//!
//! The main type is `MkvStream` — wraps any `Write + Seek` output,
//! receives raw BD-TS bytes via `write()`, outputs MKV.
//!
//! ```text
//! disc.rip(title, MkvStream::new(file, &title))
//! ```
//!
//! Components (for advanced use):
//! - `ts`: BD transport stream demuxer (192-byte packets → PES frames)
//! - `ebml`: EBML write primitives for Matroska container
//! - `mkv`: MKV muxer (tracks, clusters, blocks, cues)
//! - `codec`: Elementary stream parsers (frame boundaries, codec headers)

pub mod ebml;
pub mod ts;
pub mod mkv;
pub mod codec;
pub mod lookahead;
pub mod stream;

pub use stream::MkvStream;
