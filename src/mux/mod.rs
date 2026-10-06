//! The PES half of the pipeline: inputs implement [`crate::pes::PesSource`] (read a format →
//! PES frames), outputs [`crate::pes::PesSink`] (write PES frames → a format). Every input
//! scheme opens through [`open_source`] / [`input`], every output through [`output`].
//!
//! ```text
//! let mut input = input("iso://Disc.iso", &opts)?;
//! let title = input.info().clone();
//! let mut output = output("mkv://Movie.mkv", &title, None)?;
//! while let Ok(Some(frame)) = input.read() {
//!     output.write(&frame)?;
//! }
//! output.finish()?;
//! ```
//!
//! A whole-disc copy (`iso://`, `dir://`) is a block chain: see [`crate::io::open_block_sink`].

// Public modules — types here are intentionally part of the consumable API.
pub mod driver;
pub mod pipelined_stream;
pub mod resolve;
pub mod select;
mod selected;
pub mod source;

// Internal-only modules (referenced only via `crate::mux::…`; not public API).
// `#[allow(dead_code)]`: narrowing `pub`→`pub(crate)` surfaces helpers only
// reachable as unused public API — kept as tested scaffolding, not deleted.
pub(crate) mod au_assembly;
#[allow(dead_code)]
pub(crate) mod codec;
pub(crate) mod decode_ts;
pub(crate) mod demux_sink;
#[allow(dead_code)]
pub(crate) mod demux_thread;

// Internal modules — implementation details. Their *types* are re-exported where
// appropriate (`MkvStream`/`M2tsStream` from `lib.rs`), but the paths are not API.
// Pre-0.13 these were `pub`, leaking EBML/TS/network/stdio internals.
pub(crate) mod ebml;
/// `fvi://` sink — freemkv's native per-picture video index. A write-only PES sink that emits
/// one JSON-Lines record per coded picture; reuses the pure-data [`videomap`] model.
pub(crate) mod fvi_sink;
pub(crate) mod header_gate;
pub(crate) mod m2ts;
/// FMKV metadata header (used by `M2tsStream` / `NetworkStream` / `StdioStream`
/// to round-trip codec_privates that don't fit inside the underlying format).
/// Exposed for integration tests that exercise the wire format directly.
pub mod meta;
pub(crate) mod meta_sink;

pub(crate) mod fit;
pub(crate) mod hevc;
pub(crate) mod mkv;
pub(crate) mod mkvstream;
pub(crate) mod mp4;
pub(crate) mod mpg;
pub(crate) mod network;
pub(crate) mod null;
pub(crate) mod ps;
pub(crate) mod resync;
pub(crate) mod stdio;
// Shared clip-boundary timeline-continuity corrector (used by the MKV muxer
// and the `demux://` sink). Own `//!` docs live in timeline.rs; keep this a
// plain `//` so rustdoc resolves those links in the module's own scope.
pub(crate) mod timeline;
pub(crate) mod ts;
pub(crate) mod tsmux;
// Per-picture video index (FVI model) consumed by fvi_sink; pure data,
// serialization-independent.
#[allow(dead_code)]
pub(crate) mod videomap;

// `demux://`/`fvi://` sinks are built internally by `output()`; not public API.
// The provenance types ARE public: `output()` takes a `SourceInfo` so an `fvi://`
// destination records the INPUT it was built from (§6.2), not the file written.
pub use driver::{MuxOptions, MuxOutcome, mux_url, mux_with_keys};
pub use fit::{FitReport, SkipReason, fit_report};
pub use m2ts::M2tsStream;
pub use mkvstream::{
    MkvProbe, MkvProbeTrack, MkvStream, MkvTrackKind, parse_freemkv_version, probe_mkv,
    probe_mkv_with_cues,
};
pub use source::{ScannedTitle, Source, open_source};
pub use videomap::{Medium, SourceInfo};
// `Mp4Sink` is public so a caller driving the sink can ask `final_report()` what
// the finished file contains — the pre-mux `mp4_fit_report` is only a prediction,
// and two of its inclusions can still be dropped at `finish()`.
pub use mp4::{Mp4FitReport, Mp4Sink, Mp4SkipReason, fit_report as mp4_fit_report};
pub use mpg::MpgSink;
pub use network::{NetworkStream, is_blocked_ip};
pub use null::NullStream;
pub use pipelined_stream::PipelinedPesStream;
pub use resolve::{InputOptions, SinkCaps, StreamUrl, disc_root_of, input, output, parse_url};
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

#[cfg(test)]
mod fvi_pipeline_tests;
#[cfg(test)]
pub(crate) mod interop_tests;
#[cfg(test)]
pub(crate) mod parity_tests;
#[cfg(test)]
mod stage_tests;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
