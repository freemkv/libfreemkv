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
pub(crate) mod au_assembly;
#[allow(dead_code)]
pub(crate) mod codec;
pub(crate) mod demux_sink;
#[allow(dead_code)]
pub(crate) mod demux_thread;

// Internal modules — implementation details. Their *types* are re-exported
// where appropriate (`MkvStream`, `M2tsStream`, etc. surface from `lib.rs`),
// but the module paths themselves are not part of the API. Pre-0.13 these
// were `pub`, leaking low-level EBML primitives, TS muxer internals, and
// network/stdio implementations that no external caller had business
// reaching for.
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
pub(crate) mod resync;
pub(crate) mod stdio;
/// Shared clip-boundary timeline-continuity corrector (used by the MKV muxer
/// and the `demux://` sink).
pub(crate) mod timeline;
pub(crate) mod ts;
pub(crate) mod tsmux;
/// Reusable, pure-data per-picture video index (the FVI logical model) consumed
/// by the [`fvi_sink`]. Serialization-independent. `#[allow(dead_code)]`: the
/// `VideoMap` accumulator is a standalone primitive staged for the side-channel
/// (mux-while-indexing) reuse described in its module doc; the `fvi://` sink
/// today builds `PictureRecord`s directly, so the accumulator is covered only by
/// its own unit tests until that wiring lands.
#[allow(dead_code)]
pub(crate) mod videomap;

// `demux://` and `fvi://` sinks are constructed internally by `output()` via the
// direct `super::demux_sink::` / `super::fvi_sink::` paths — no re-export needed,
// and no consumer names these types, so they are not public API.
pub use disc::DiscStream;
pub use m2ts::M2tsStream;
pub use mkvstream::MkvStream;
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

    // The scheme table is the public contract documented at the top of
    // resolve.rs: `scheme://path`. These tests pin the round-trip
    // (parse_url → scheme()/path_str()) against that table, not against
    // whatever the parser happens to emit.

    #[test]
    fn scheme_names_match_the_documented_table() {
        // Each StreamUrl::scheme() must equal the scheme token that parses
        // back to it. A renamed/typo'd scheme string would break the
        // round-trip the resolver doc promises.
        assert_eq!(parse_url("disc://").scheme(), "disc");
        assert_eq!(parse_url("m2ts://f").scheme(), "m2ts");
        assert_eq!(parse_url("mkv://f").scheme(), "mkv");
        assert_eq!(parse_url("network://h:1").scheme(), "network");
        assert_eq!(parse_url("stdio://").scheme(), "stdio");
        assert_eq!(parse_url("iso://f").scheme(), "iso");
        assert_eq!(parse_url("null://").scheme(), "null");
        assert_eq!(parse_url("demux://out/").scheme(), "demux");
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
