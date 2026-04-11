//! Stream-based I/O pipeline.
//!
//! All formats are streams. Two URLs, left reads, right writes:
//!
//! ```text
//! freemkv disc:// mkv://Dune.mkv
//! freemkv m2ts://Dune.m2ts mkv://Dune.mkv
//! freemkv disc:// network://10.1.7.11:9000
//! ```
//!
//! Streams implement `IOStream` for uniform handling:
//!
//! ```text
//! let mut input = open_input("disc://", &opts)?;
//! let mut output = open_output("mkv://Dune.mkv", input.info())?;
//! io::copy(&mut *input, &mut *output)?;
//! output.finish()?;
//! ```

pub mod ebml;
pub mod ts;
pub mod mkv;
pub mod codec;
pub mod lookahead;
pub mod meta;
mod m2ts;
mod mkvstream;
pub mod network;
pub mod disc;
pub mod null;
pub mod resolve;

pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
pub use network::NetworkStream;
pub use disc::{DiscStream, DiscOptions};
pub use null::NullStream;
pub use resolve::{open_input, open_output, parse_url, InputOptions, StreamUrl};

use std::io::{self, Read, Write, Seek};
use crate::disc::DiscTitle;

/// Common interface for all stream types.
///
/// A stream can be opened for reading or created for writing.
/// Calling the unsupported direction returns an error.
pub trait IOStream: Read + Write {
    /// Get stream metadata.
    fn info(&self) -> &DiscTitle;

    /// Finalize the stream (flush, write index/cues, close).
    fn finish(&mut self) -> io::Result<()>;
}

// Combined traits for internal trait objects.
pub(crate) trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

pub(crate) trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}
