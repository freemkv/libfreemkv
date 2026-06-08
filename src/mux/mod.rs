//! Stream-based I/O pipeline.
//!
//! Two muxer families live here:
//!
//! 1. **Bidirectional PES streams** (`disc`, `mkv`, `m2ts`, `network`,
//!    `stdio`, `null`) implement the [`crate::pes::Stream`] interface:
//!    read a format → PES frames, or write PES frames → a format.
//! 2. **Write-only sequential-sink muxers** (`fmp4`, `hevc`,
//!    `m2ts_mux`) consume PES frames and write a container to a
//!    `SequentialSink`; they do not implement the read loop below.
//!
//! The bidirectional family is driven like this:
//!
//! ```text
//! let mut input = input("iso://Disc.iso", &opts)?;
//! let title = input.info().clone();
//! let mut output = output("mkv://Movie.mkv", &title)?;
//! while let Ok(Some(frame)) = input.read() {
//!     output.write(&frame)?;
//! }
//! output.finish()?;
//! ```
//!
//! For disc→ISO (raw sector copy), use `Disc::copy()` instead.

// Public modules — types here are intentionally part of the consumable API.
pub mod disc;
pub mod pipelined_stream;
pub mod resolve;

// Internal-only modules. Every reference is via `crate::mux::…` /
// `super::…` from inside the crate; nothing in the downstream crates or
// integration tests imports them and lib.rs re-exports nothing from
// them, so they are not part of the stable public API.
//
// `#[allow(dead_code)]`: narrowing these from `pub` to `pub(crate)`
// surfaces a handful of helpers/accessors that were only ever reachable
// as (unused) public API — e.g. the MPEG-2 resolution/frame-rate
// accessors and an alternate `DemuxThread` spawn path. They are kept as
// part of the parser/demux surface and covered by unit tests; allow the
// dead-code lint rather than delete still-relevant scaffolding.
#[allow(dead_code)]
pub(crate) mod codec;
#[allow(dead_code)]
pub(crate) mod demux_thread;

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

// ── Sequential-sink muxers ──────────────────────────────────────────────────
//
// Container muxers that consume PES frames and write to a
// `SequentialSink`. They are NOT the bidirectional `MkvStream` /
// `M2tsStream` (which round-trip via the `Stream` trait + BD-TS
// framing); these are write-only and sequential.
//
// `pub(crate)`: these have no external callers and are not re-exported
// from lib.rs. `fmp4` is an explicit STUB (`Fmp4Mux::write_video`
// accumulates and discards) — shipping it as `pub` would lock a
// half-built type into the v1.0 stability contract via the
// `libfreemkv::mux::fmp4::Fmp4Mux` path. `m2ts_mux` is the plain
// MPEG-TS sequential muxer and `hevc` is its Annex-B helper; both are
// staged scaffolding for the sink split and are not yet wired into a
// live pipeline (the production paths use `tsmux` / `mkv`).
// `#[allow(dead_code)]`: retained intentionally until the sink split
// lands; they are exercised by their own unit tests. If any becomes a
// public muxer, re-export its concrete type from lib.rs instead.
#[allow(dead_code)]
pub(crate) mod fmp4;
#[allow(dead_code)]
pub(crate) mod hevc;
#[allow(dead_code)]
pub(crate) mod m2ts_mux;
pub(crate) mod mkv;
pub(crate) mod mkvstream;
pub(crate) mod network;
pub(crate) mod null;
pub(crate) mod ps;
pub(crate) mod stdio;
pub(crate) mod ts;
pub(crate) mod tsmux;

pub use disc::DiscStream;
pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
pub use network::NetworkStream;
pub use null::NullStream;
pub use pipelined_stream::PipelinedPesStream;
pub use resolve::build_iso_pipeline;
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
