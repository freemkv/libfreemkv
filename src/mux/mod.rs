//! Stream-based I/O pipeline.
//!
//! All formats are PES streams. Read from a format → PES frames.
//! Write PES frames → a format.
//!
//! ```text
//! let mut input = input("iso://Disc.iso", &opts)?;
//! let title = input.info().clone();
//! let mut output = output("mkv://Dune.mkv", &title)?;
//! while let Ok(Some(frame)) = input.read() {
//!     output.write(&frame)?;
//! }
//! output.finish()?;
//! ```
//!
//! For disc→ISO (raw sector copy), use `Disc::copy()` instead.

pub mod codec;
pub mod disc;
pub mod ebml;
pub mod iso;
pub mod mkv;
pub mod tsmux;
pub mod tsreader;
mod m2ts;
pub mod meta;
mod mkvstream;
pub mod network;
pub mod null;
pub mod ps;
pub mod resolve;
pub mod stdio;
pub mod ts;

pub use disc::DiscStream;
pub use iso::IsoSectorReader;
pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
pub use network::NetworkStream;
pub use null::NullStream;
pub use resolve::{input, output, parse_url, InputOptions, StreamUrl};
pub use stdio::StdioStream;

use std::io::{Seek, Write};

// WriteSeek — used internally by MKV muxer (container format requires seeking).
pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}
