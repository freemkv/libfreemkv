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

// Public modules — types here are intentionally part of the consumable API.
pub mod codec;
pub mod disc;
pub mod iso;
pub mod resolve;

// Internal modules — implementation details. Their *types* are re-exported
// where appropriate (`MkvStream`, `M2tsStream`, etc. surface from `lib.rs`),
// but the module paths themselves are not part of the API. Pre-0.13 these
// were `pub`, leaking low-level EBML primitives, TS muxer internals, and
// network/stdio implementations that no external caller had business
// reaching for.
pub(crate) mod ebml;
pub(crate) mod m2ts;
/// FMKV metadata header (used by `M2tsStream` / `NetworkStream` / `StdioStream`
/// to round-trip codec_privates that don't fit inside the underlying format).
/// Exposed for integration tests that exercise the wire format directly.
pub mod meta;

// ── Phase 3 sequential muxers ──────────────────────────────────────────────
//
// New container muxers that consume PES frames and write to a
// `SequentialSink`. They are NOT refactors of the existing `MkvStream` /
// `M2tsStream` (which round-trip via the legacy `Stream` trait + the
// BD-TS framing); they're sequential-only and target the Phase 2 sink
// split end-to-end.
pub mod fmp4;
pub mod hevc;
pub mod m2ts_mux;
pub(crate) mod mkv;
pub(crate) mod mkvstream;
pub(crate) mod network;
pub(crate) mod null;
pub(crate) mod ps;
pub(crate) mod stdio;
pub(crate) mod ts;
pub(crate) mod tsmux;

pub use disc::DiscStream;
pub use iso::IsoSectorReader;
pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
pub use network::NetworkStream;
pub use null::NullStream;
pub use resolve::{InputOptions, StreamUrl, input, output, parse_url};
pub use stdio::StdioStream;

use std::io::{Seek, Write};

/// Combined `Write + Seek` for sinks accepted by the MKV muxer.
///
/// Matroska's `SeekHead`, `Cues`, and `Cluster` size fields are written with
/// placeholder values during streaming and updated in-place at finalization,
/// so the output sink must support seeking. Provided as a single trait
/// alias so callers don't have to repeat `Write + Seek` everywhere; the
/// blanket impl below opts every `T: Write + Seek` in automatically
/// (`File`, `BufWriter<File>`, `Cursor<Vec<u8>>`).
pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}
