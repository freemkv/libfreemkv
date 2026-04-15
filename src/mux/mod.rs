//! Stream-based I/O pipeline.
//!
//! All formats are streams. Two URLs, left reads, right writes:
//!
//! ```text
//! freemkv disc:// mkv://Dune.mkv
//! freemkv m2ts://Dune.m2ts mkv://Dune.mkv
//! freemkv disc:// network://10.1.7.11:9000
//! freemkv disc:// stdio://
//! freemkv stdio:// mkv://Dune.mkv
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

pub mod codec;
pub mod disc;
pub mod ebml;
pub mod iso;
mod isowriter;
pub mod lookahead;
mod m2ts;
pub mod meta;
pub mod mkv;
pub mod mkvout;
mod mkvstream;
pub mod network;
pub mod null;
pub mod ps;
pub mod resolve;
pub mod stdio;
pub mod ts;

pub use disc::{DiscOpenResult, DiscStream};
pub use iso::{IsoSectorReader, IsoStream};
pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
pub use network::NetworkStream;
pub use null::NullStream;
pub use resolve::{open_input, open_output, parse_url, InputOptions, StreamUrl};
pub use stdio::StdioStream;

use crate::disc::DiscTitle;
use std::io::{self, Read, Seek, Write};

/// Common interface for all stream types.
///
/// A stream can be opened for reading or created for writing.
/// Calling the unsupported direction returns an error.
pub trait IOStream: Read + Write {
    /// Get stream metadata.
    fn info(&self) -> &DiscTitle;

    /// Finalize the stream (flush, write index/cues, close).
    fn finish(&mut self) -> io::Result<()>;

    /// Total content size in bytes, if known. Used for progress display.
    fn total_bytes(&self) -> Option<u64> {
        None
    }

    /// Decryption keys for this stream. Default: no encryption.
    /// Overridden by DiscStream and IsoStream for AACS/CSS.
    fn keys(&self) -> crate::decrypt::DecryptKeys {
        crate::decrypt::DecryptKeys::None
    }
}

// Combined traits for internal trait objects.
pub(crate) trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

pub(crate) trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}
