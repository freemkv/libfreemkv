//! libfreemkv -- Open source optical drive library for 4K UHD / Blu-ray / DVD.
//!
//! Handles drive access, disc structure parsing, AACS decryption, and raw
//! sector reading. Unlocking — removing bus encryption (firmware unlock, AACS
//! cert handshake, CSS bus-auth) — lives entirely in the `freemkv-unlock`
//! crate; libfreemkv consumes it privately and exposes none of it, so clients
//! are oblivious to unlockers (just as they are to the SCSI layer).
//!
//! # Quick Start
//!
//! ```no_run
//! use libfreemkv::{Drive, Disc, ScanOptions, find_drive};
//!
//! let mut drive = find_drive().expect("no optical drive found");
//! drive.wait_ready().unwrap();
//! drive.init().unwrap();
//! let disc = Disc::scan(&mut drive, &ScanOptions::default()).unwrap();
//!
//! for title in &disc.titles {
//!     println!("{} -- {} streams", title.duration_display(), title.streams.len());
//! }
//! ```
//!
//! Muxing to an output container runs through the PES pipeline. A live
//! `disc://` cannot be opened via [`input`] — it returns
//! [`Error::DiscUrlNotDirect`] by design (use `Drive` + `Disc::scan` +
//! `DiscStream::new` directly for a live drive). Any file-backed source
//! (`iso://`, `m2ts://`) opens through [`input`]:
//!
//! ```no_run
//! # fn run() -> std::io::Result<()> {
//! let opts = libfreemkv::InputOptions::default();
//! let mut input = libfreemkv::input("iso://disc.iso", &opts)?;
//! let title = input.info().clone();
//! let mut output = libfreemkv::output("mkv://Movie.mkv", &title, None)?;
//! // Propagate read errors instead of silently stopping on the first one.
//! while let Some(frame) = input.read()? {
//!     output.write(&frame)?;
//! }
//! output.finish()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! ```text
//! Drive           -- open, identify, unlock, read sectors
//!   ├── ScsiTransport    -- SG_IO (Linux), IOKit (macOS)
//!   ├── DriveId          -- INQUIRY + GET_CONFIG identification
//!   └── unlock_bridge    -- private seam to the `freemkv-unlock` crate
//!                           (firmware / AACS cert / CSS bus-auth unlockers)
//!
//! Disc                   -- scan titles, streams, AACS state
//!   ├── UDF reader       -- Blu-ray UDF 2.50 with metadata partitions
//!   ├── MPLS parser      -- playlists → titles + clips + STN streams
//!   ├── CLPI parser      -- clip info → EP map → sector extents
//!   ├── JAR parser       -- BD-J audio track labels
//!   └── AACS             -- encryption: key resolution + content decrypt
//!       ├── aacs         -- KEYDB, VUK, MKB, unit decrypt
//!       └── host_certs   -- collect host certs (cert handshake lives in freemkv-unlock)
//! ```
//!
//! # AACS Encryption
//!
//! Disc scanning automatically detects and handles AACS encryption.
//! If a KEYDB.cfg is available (via `ScanOptions` or standard paths),
//! the library resolves keys and decrypts content transparently.
//!
//! Supports AACS 1.0 (Blu-ray) and AACS 2.0 (UHD, with fallback).
//!
//! # Error Codes
//!
//! All errors are structured with numeric codes. No user-facing English
//! text -- applications format their own messages.
//!
//! | Range | Category |
//! |-------|----------|
//! | E1xxx | Device errors (not found, permission) |
//! | E2xxx | Profile errors (unsupported drive) |
//! | E3xxx | Unlock errors (failed, signature) |
//! | E4xxx | SCSI errors (command failed, timeout) |
//! | E5xxx | I/O errors |
//! | E6xxx | Disc format errors |
//! | E7xxx | AACS errors |
//! | E8xxx | Keydb errors (fetch, parse, load) |
//! | E9xxx | Stream / mux errors (URL, PES, pipeline) |

/// Single source of truth for every freemkv version surface.
///
/// `FREEMKV_VERSION` is the package version, overridable at build time via the
/// `FREEMKV_BUILD_LABEL` env (see `build.rs`); `GIT_SUFFIX` is the git short
/// hash. The CLI's `--version`, the MKV muxing/writing-application field, and
/// the FVI generator tag all derive from these two consts, so a binary reports
/// the exact same label it stamps into the files it produces — no split-brain
/// where an MKV claims one version and the binary another.
pub const VERSION_LABEL: &str = concat!(env!("FREEMKV_VERSION"), env!("GIT_SUFFIX"));

/// The muxing/writing-application string written into MKV output
/// (`"freemkv <version> (g<hash>)"`). Only the ripping tree muxes, so this is
/// gated with it — a transport-only build has no MKV writer to stamp.
#[cfg(feature = "rip")]
pub(crate) const MUX_APP: &str = concat!("freemkv ", env!("FREEMKV_VERSION"), env!("GIT_SUFFIX"));

// ─── Always compiled ────────────────────────────────────────────────────────
// `error` and `scsi` form the transport-only surface (`--features scsi`,
// `default-features = false`). `error` carries the SCSI error variants the
// transport layer returns; `scsi` is the SCSI passthrough layer itself. Neither
// pulls the ripping dependency tree. Everything below is gated behind `rip`.
pub mod error;
pub mod scsi;

// ─── Ripping tree (feature = "rip") ─────────────────────────────────────────
// Drive lifecycle, disc/UDF/MPLS/CLPI parsing, AACS/CSS decrypt, PES/mux
// pipeline, keydb, and the freemkv-unlock seam — plus their heavy deps (serde,
// zip, rayon, roxmltree, rand, crossbeam, memchr, …). Gated so a transport-only
// consumer compiles `scsi` alone.
#[cfg(feature = "rip")]
pub mod aacs;
#[cfg(feature = "rip")]
pub(crate) mod bdnav;
#[cfg(feature = "rip")]
pub(crate) mod clpi;
#[cfg(feature = "rip")]
pub mod consts;
#[cfg(feature = "rip")]
pub mod css;
#[cfg(feature = "rip")]
pub mod decrypt;
#[cfg(feature = "rip")]
pub mod diag;
#[cfg(feature = "rip")]
pub mod dirimage;
#[cfg(feature = "rip")]
pub mod disc;
#[cfg(feature = "rip")]
pub mod drive;
#[cfg(feature = "rip")]
pub(crate) mod dvdnav;
#[cfg(feature = "rip")]
pub mod event;
#[cfg(feature = "rip")]
pub mod halt;
#[cfg(all(test, feature = "rip"))]
mod harness;
#[cfg(feature = "rip")]
pub mod hex;
#[cfg(feature = "rip")]
pub(crate) mod identity;
#[cfg(feature = "rip")]
pub(crate) mod ifo;
#[cfg(feature = "rip")]
pub mod io;
#[cfg(feature = "rip")]
pub mod keysource;
#[cfg(feature = "rip")]
pub mod labels;
#[cfg(feature = "rip")]
pub(crate) mod mpls;
#[cfg(feature = "rip")]
pub mod mux;
#[cfg(feature = "rip")]
pub mod pes;
#[cfg(feature = "rip")]
pub(crate) mod platform;
#[cfg(feature = "rip")]
pub mod progress;
#[cfg(feature = "rip")]
pub mod sector;
#[cfg(feature = "rip")]
pub mod session;
#[cfg(all(test, feature = "rip"))]
pub(crate) mod testlog;
#[cfg(feature = "rip")]
pub(crate) mod udf;
#[cfg(feature = "rip")]
pub(crate) mod unlock_bridge;

// ─── Drive lifecycle ────────────────────────────────────────────────────────
// `Drive::open(path)` → `wait_ready()` → `init()` → `Disc::scan()`. `Drive` owns
// the SCSI session; `DriveCapture` etc. expose drive identity/profile data.
#[cfg(feature = "rip")]
pub use drive::capture::{
    CapturedFeature, DriveCapture, capture_drive_data, mask_bytes, mask_string,
};
#[cfg(feature = "rip")]
pub use drive::{Drive, DriveStatus, extract_scsi_context, find_drive};

// ─── Disc session (drive open + SCSI bring-up hoist) ─────────────────────────
// One entry point opens a drive, brings transport up, and forwards caller-built
// key material into `ScanOptions` (the library derives no certs; see `KeySpec`).
#[cfg(feature = "rip")]
pub use session::{
    DeviceTarget, DiscSession, KeySourceFactory, KeySpec, ResolvedKeys, resolve_keys_for, scan_dir,
    scan_iso,
};

// ─── Errors ─────────────────────────────────────────────────────────────────
// All fallible APIs return `Result<T, Error>`; `Error` is a typed enum with a
// numeric `code()` — no English text in the library; see `error.rs` for taxonomy.
pub use error::{
    Error, Result, error_code, is_disc_level_no_key, is_halt, is_skippable_title_stub,
};

// ─── Cooperative cancellation ───────────────────────────────────────────────
// One-bit cancellation token shared by every long-running loop (mux, and the
// engine's sweep/patch passes); clone cheaply, poll `is_cancelled()` in loops.
#[cfg(feature = "rip")]
pub use halt::Halt;

// Bounded producer/consumer primitive used by mux (and engine's sweep/patch) to
// overlap reads with writes via a consumer thread. `Sink::apply` defines
// per-item behavior; `Flow::Stop` ends the consumer cleanly, still calling `close()`.
#[cfg(feature = "rip")]
pub use io::pipeline::{
    DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH, WRITE_THROUGH_DEPTH,
};

// ─── Bounded-cache buffered file writer ─────────────────────────────────────
// Drop-in `std::fs::File` replacement for large sequential output (mux, extract,
// sweep, patch); drains dirty pages continuously. `pub` for freemkv-engine reuse.
#[cfg(feature = "rip")]
pub use io::WritebackFile;
/// Write an image-level source out as a sector image — what an `iso://`
/// DESTINATION means for any source that is not a physical drive. Drive sources
/// go through `freemkv_engine::copy`, which is the recovery path; see
/// [`io::image_writer`] for why the two are deliberately separate.
#[cfg(feature = "rip")]
pub use io::image_writer::write_image;

// ─── Drive events (low-level callbacks) ─────────────────────────────────────
#[cfg(feature = "rip")]
pub use event::{BatchSizeReason, Event, EventKind};
#[cfg(feature = "rip")]
pub use identity::DriveId;

// ─── Unlock seam ────────────────────────────────────────────────────────────
// Drive/disc unlocking (firmware, AACS cert, CSS bus-auth) lives entirely in
// `freemkv-unlock`; libfreemkv consumes it via `unlock_bridge` and exposes nothing.

// ─── Decryption (AACS / CSS) ────────────────────────────────────────────────
// `Disc::scan()` resolves keys onto `Disc`; `DiscStream::new(...)` consumes
// them directly. `decrypt_sectors()` is for raw sector buffers (ISO patching).
#[cfg(feature = "rip")]
pub use decrypt::{AacsKeyMap, DecryptKeys, decrypt_sectors, decrypt_threads, set_decrypt_threads};

// ─── Disc structure ─────────────────────────────────────────────────────────
// `Disc::scan()` fully populates `Disc`; `Disc::identify()` is a UDF-only fast path
// for name/format display. Codec enums are canonical; never compare display strings.
#[cfg(feature = "rip")]
pub use dirimage::DirImage;
#[cfg(feature = "rip")]
pub use disc::{
    AacsState, AudioChannels, AudioStream, Clip, Codec, ColorSpace, ContentFormat, Disc,
    DiscFormat, DiscId, DiscTitle, DriveCredentials, Extent, ExtractOptions, ExtractResult,
    FileResult, FrameRate, HdrFormat, Key, KeyOrigin, LabelPurpose, LabelQualifier, Resolution,
    SampleRate, ScanOptions, Stream, SubtitleStream, VideoStream,
};
#[cfg(feature = "rip")]
pub use keysource::{DiscInputs, KeySource, read_encrypted_units, resolve_and_apply};

// ─── Streams ────────────────────────────────────────────────────────────────
// All types implement `pes::Stream` (re-exported `PesStream` to avoid colliding
// with `disc::Stream`). Prefer `input()`/`output()` URL resolvers over direct construction.
#[cfg(feature = "rip")]
pub use pes::PesFrame;
#[cfg(feature = "rip")]
pub use pes::Stream as PesStream;

#[cfg(feature = "rip")]
pub use mux::DiscStream;
#[cfg(feature = "rip")]
pub use mux::M2tsStream;
#[cfg(feature = "rip")]
pub use mux::MkvStream;
#[cfg(feature = "rip")]
pub use mux::NetworkStream;
#[cfg(feature = "rip")]
pub use mux::NullStream;
#[cfg(feature = "rip")]
pub use mux::StdioStream;
#[cfg(feature = "rip")]
pub use mux::WriteSeek;
#[cfg(feature = "rip")]
pub use mux::{InputOptions, StreamUrl, input, output, parse_url};
#[cfg(feature = "rip")]
pub use mux::{Medium, SourceInfo};
#[cfg(feature = "rip")]
pub use mux::{Mp4FitReport, Mp4Sink, Mp4SkipReason, mp4_fit_report};

// ─── Lower-level surfaces ───────────────────────────────────────────────────
// `ScsiTransport` is the platform trait Drive uses, exposed for out-of-tree
// backends. `DecryptingSectorSource` wraps any `SectorSource` to decrypt (AACS/CSS).
#[cfg(feature = "rip")]
pub use mux::build_iso_pipeline;
#[cfg(feature = "rip")]
pub use mux::resolve_mux_key_map;
#[cfg(feature = "rip")]
pub use mux::select::{PidFilter, StreamSelection};
#[cfg(feature = "rip")]
pub use mux::{MuxEvents, MuxInput, MuxOptions, MuxOutcome, mux_stream};
pub use scsi::{DriveInfo, ScsiSense, ScsiTransport, SenseFamily, drive_has_disc, list_drives};
#[cfg(feature = "rip")]
pub use sector::{
    DecryptingSectorSource, FileSectorSource, KeyFetch, PrefetchedSectorSource, SectorSource,
};
#[cfg(feature = "rip")]
pub use udf::{UdfFs, read_filesystem};
