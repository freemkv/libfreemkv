//! Stream-based I/O pipeline. Two muxer families live here:
//!
//! 1. **Bidirectional PES streams** (`disc`, `mkv`, `m2ts`, `network`, `stdio`, `null`) implement the [`crate::pes::Stream`] interface: read a format → PES frames, or write PES frames → a format.
//! 2. **Write-only sequential-sink muxers** (`fmp4`, `hevc`, `m2ts_mux`) consume PES frames and write a container to a `SequentialSink`; they do not implement the read loop below.
//!
//! The bidirectional family is driven like this:
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
//! For disc→ISO (raw sector copy), use `freemkv_engine::recovery::copy` instead.

// Public modules — types here are intentionally part of the consumable API.
pub mod disc;
pub mod driver;
pub mod pipelined_stream;
pub mod resolve;
pub mod select;

// Internal-only modules (referenced only via `crate::mux::…`; not public API).
// `#[allow(dead_code)]`: narrowing `pub`→`pub(crate)` surfaces helpers only
// reachable as unused public API — kept as tested scaffolding, not deleted.
pub(crate) mod au_assembly;
#[allow(dead_code)]
pub(crate) mod codec;
pub(crate) mod demux_sink;
#[allow(dead_code)]
pub(crate) mod demux_thread;

// Internal modules — implementation details. Their *types* are re-exported where
// appropriate (`MkvStream`/`M2tsStream` from `lib.rs`), but the paths are not API.
// Pre-0.13 these were `pub`, leaking EBML/TS/network/stdio internals.
pub(crate) mod ebml;
/// `fvi://` sink — freemkv's native per-picture video index (see
/// `docs/FVI_FORMAT.md`). A write-only PES sink that emits one JSON-Lines record
/// per coded picture; reuses the pure-data [`videomap`] model.
pub(crate) mod fvi_sink;
pub(crate) mod m2ts;
/// FMKV metadata header (used by `M2tsStream` / `NetworkStream` / `StdioStream`
/// to round-trip codec_privates that don't fit inside the underlying format).
/// Exposed for integration tests that exercise the wire format directly.
pub mod meta;
pub(crate) mod meta_sink;

// ── Sequential-sink muxers ── write-only PES → `SequentialSink`. `pub(crate)`+
// `allow(dead_code)`: `fmp4` is a STUB (pub would lock a half-built type into
// v1.0); `m2ts_mux`/`hevc` are sink-split scaffolding (production: `tsmux`/`mkv`).
#[allow(dead_code)]
pub(crate) mod fmp4;
#[allow(dead_code)]
pub(crate) mod hevc;
#[allow(dead_code)]
pub(crate) mod m2ts_mux;
pub(crate) mod mkv;
pub(crate) mod mkvstream;
pub(crate) mod mp4;
pub(crate) mod network;
pub(crate) mod null;
pub(crate) mod ps;
pub(crate) mod resync;
pub(crate) mod stdio;
/// Shared clip-boundary timeline-continuity corrector (used by the MKV muxer
/// and the `demux://` sink).
pub(crate) mod timeline;
pub(crate) mod ts;
pub(crate) mod tsmux;
// Per-picture video index (FVI model) consumed by fvi_sink; pure data, serialization-independent.
// See docs/videomap.md — why the VideoMap accumulator is allow(dead_code) for now.
#[allow(dead_code)]
pub(crate) mod videomap;

// `demux://`/`fvi://` sinks are built internally by `output()`; not public API.
// The provenance types ARE public: `output()` takes a `SourceInfo` so an `fvi://`
// destination records the INPUT it was built from (§6.2), not the file written.
pub use disc::DiscStream;
pub use driver::{MuxEvents, MuxInput, MuxOptions, MuxOutcome, mux_stream};
pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
pub use videomap::{Medium, SourceInfo};
// `Mp4Sink` is public so a caller driving the sink can ask `final_report()` what
// the finished file contains — the pre-mux `mp4_fit_report` is only a prediction,
// and two of its inclusions can still be dropped at `finish()`.
pub use mp4::{Mp4FitReport, Mp4Sink, Mp4SkipReason, fit_report as mp4_fit_report};
pub use network::NetworkStream;
pub use null::NullStream;
pub use pipelined_stream::PipelinedPesStream;
pub use resolve::build_iso_pipeline;
pub use resolve::resolve_mux_key_map;
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

#[cfg(test)]
mod tests {
    use super::resolve::{StreamUrl, parse_url};
    use std::path::PathBuf;

    // The scheme table is the public contract documented at the top of resolve.rs
    // (`scheme://path`). These tests pin the round-trip (parse_url →
    // scheme()/path_str()) against that table, not whatever the parser emits.

    #[test]
    fn scheme_names_match_the_documented_table() {
        // Each StreamUrl::scheme() must equal the scheme token that parses
        // back to it. A renamed/typo'd scheme string would break the
        // round-trip the resolver doc promises.
        assert_eq!(parse_url("disc://").scheme(), "disc");
        assert_eq!(parse_url("m2ts://f").scheme(), "m2ts");
        assert_eq!(parse_url("mkv://f").scheme(), "mkv");
        assert_eq!(parse_url("mp4://f").scheme(), "mp4");
        assert_eq!(parse_url("network://h:1").scheme(), "network");
        assert_eq!(parse_url("stdio://").scheme(), "stdio");
        assert_eq!(parse_url("iso://f").scheme(), "iso");
        assert_eq!(parse_url("null://").scheme(), "null");
        assert_eq!(parse_url("demux://out/").scheme(), "demux");
        assert_eq!(parse_url("video://out/").scheme(), "video");
        assert_eq!(parse_url("audio://out/").scheme(), "audio");
        assert_eq!(parse_url("sub://out/").scheme(), "sub");
        assert_eq!(parse_url("chapters://c.xml").scheme(), "chapters");
        assert_eq!(parse_url("json://t.json").scheme(), "json");
        assert_eq!(parse_url("bogus://x").scheme(), "unknown");
    }

    #[test]
    fn path_str_returns_the_path_component_for_file_schemes() {
        // For file-backed schemes path_str() must echo the exact path that
        // followed the `scheme://` prefix — the resolver later feeds this to
        // File::open, so a dropped/garbled component opens the wrong file.
        assert_eq!(parse_url("iso://Disc.iso").path_str(), "Disc.iso");
        assert_eq!(parse_url("m2ts:///abs/x.m2ts").path_str(), "/abs/x.m2ts");
        assert_eq!(parse_url("mkv://out.mkv").path_str(), "out.mkv");
    }

    #[test]
    fn path_str_returns_address_for_network() {
        // network:// path_str is the host:port address verbatim.
        assert_eq!(
            parse_url("network://203.0.113.5:9000").path_str(),
            "203.0.113.5:9000"
        );
    }

    #[test]
    fn path_str_empty_for_scheme_only_urls() {
        // disc:// (no device), stdio://, null:// carry no path; path_str()
        // must be empty so a caller doesn't treat trailing junk as a path.
        assert_eq!(parse_url("disc://").path_str(), "");
        assert_eq!(parse_url("stdio://").path_str(), "");
        assert_eq!(parse_url("null://").path_str(), "");
    }

    #[test]
    fn path_str_for_unknown_echoes_raw_input() {
        // Unknown URLs preserve the raw string so the caller can report the
        // exact offending input back to the user.
        assert_eq!(parse_url("plain/path").path_str(), "plain/path");
        assert_eq!(parse_url("ftp://x").path_str(), "ftp://x");
    }

    #[test]
    fn disc_url_with_device_carries_path() {
        // disc:///dev/sg1 → Disc{device: Some(/dev/sg1)}; path_str echoes it.
        let u = parse_url("disc:///dev/sg1");
        assert!(matches!(u, StreamUrl::Disc { device: Some(_) }));
        assert_eq!(u.path_str(), "/dev/sg1");
    }

    #[test]
    fn is_disc_source_only_for_disc_and_iso() {
        // is_disc_source gates the "raw sector copy" path. Per the doc table
        // only disc:// and iso:// are disc sources; mkv/m2ts/network/etc must
        // NOT be (they are container/stream formats, not raw sector media).
        assert!(parse_url("disc://").is_disc_source());
        assert!(parse_url("disc:///dev/sg1").is_disc_source());
        assert!(parse_url("iso://x.iso").is_disc_source());
        assert!(!parse_url("m2ts://x").is_disc_source());
        assert!(!parse_url("mkv://x").is_disc_source());
        assert!(!parse_url("network://h:1").is_disc_source());
        assert!(!parse_url("stdio://").is_disc_source());
        assert!(!parse_url("null://").is_disc_source());
        assert!(!parse_url("junk").is_disc_source());
    }

    #[test]
    fn null_and_stdio_with_trailing_path_are_unknown_not_silently_discarded() {
        // Doc + resolve.rs comment: null:// / stdio:// are scheme-only. A
        // trailing path is malformed and must fall through to Unknown rather
        // than be silently dropped (which would mask a caller typo).
        assert!(matches!(parse_url("null://x"), StreamUrl::Unknown { .. }));
        assert!(matches!(parse_url("stdio://x"), StreamUrl::Unknown { .. }));
        // The exact-prefix scheme-only forms still resolve.
        assert!(matches!(parse_url("null://"), StreamUrl::Null));
        assert!(matches!(parse_url("stdio://"), StreamUrl::Stdio));
    }

    #[test]
    fn bare_path_without_scheme_is_unknown() {
        // "Bare paths without a scheme are rejected." (resolve.rs doc.)
        assert!(matches!(parse_url("/dev/sg1"), StreamUrl::Unknown { .. }));
        assert!(matches!(parse_url("movie.mkv"), StreamUrl::Unknown { .. }));
        assert!(matches!(parse_url(""), StreamUrl::Unknown { .. }));
    }

    #[test]
    fn empty_iso_and_m2ts_paths_parse_but_keep_empty_pathbuf() {
        // `iso://` with no path parses to Iso{path:""} — parse_url does NOT
        // validate; validate_file_path (in input/output) is where the empty
        // path is rejected. Pinning this keeps the parse/validate split honest.
        assert!(
            matches!(parse_url("iso://"), StreamUrl::Iso { ref path } if path.as_os_str().is_empty())
        );
        assert!(
            matches!(parse_url("m2ts://"), StreamUrl::M2ts { ref path } if path.as_os_str().is_empty())
        );
    }

    #[test]
    fn first_matching_scheme_wins_no_double_prefix_confusion() {
        // A path component that itself looks like another scheme must be
        // treated as a path, not re-dispatched. iso://m2ts://x → Iso with
        // path "m2ts://x", because strip_prefix matches iso:// first.
        let u = parse_url("iso://m2ts://x");
        assert!(matches!(u, StreamUrl::Iso { ref path } if path == &PathBuf::from("m2ts://x")));
    }
}
