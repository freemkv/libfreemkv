//! libfreemkv -- Open source optical drive library for 4K UHD / Blu-ray / DVD.
//!
//! Handles drive access, disc structure parsing, AACS decryption, and raw
//! sector reading. 206 bundled drive profiles. No external files needed.
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
//!
//! // Stream via PES pipeline
//! let opts = libfreemkv::InputOptions::default();
//! let mut input = libfreemkv::input("disc://", &opts).unwrap();
//! let title = input.info().clone();
//! let mut output = libfreemkv::output("mkv://Movie.mkv", &title).unwrap();
//! while let Ok(Some(frame)) = input.read() {
//!     output.write(&frame).unwrap();
//! }
//! output.finish().unwrap();
//! ```
//!
//! # Architecture
//!
//! ```text
//! Drive           -- open, identify, unlock, read sectors
//!   ├── ScsiTransport    -- SG_IO (Linux), IOKit (macOS)
//!   ├── DriveProfile     -- per-drive unlock parameters (206 bundled)
//!   ├── DriveId          -- INQUIRY + GET_CONFIG identification
//!   └── Platform
//!       └── Mt1959       -- MediaTek unlock/read (Renesas planned)
//!
//! Disc                   -- scan titles, streams, AACS state
//!   ├── UDF reader       -- Blu-ray UDF 2.50 with metadata partitions
//!   ├── MPLS parser      -- playlists → titles + clips + STN streams
//!   ├── CLPI parser      -- clip info → EP map → sector extents
//!   ├── JAR parser       -- BD-J audio track labels
//!   └── AACS             -- encryption: key resolution + content decrypt
//!       ├── aacs         -- KEYDB, VUK, MKB, unit decrypt
//!       └── handshake    -- SCSI auth, ECDH, bus key
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

pub mod aacs;
pub(crate) mod clpi;
pub mod css;
pub mod decrypt;
pub mod disc;
pub mod drive;
pub mod error;
pub mod event;
pub mod halt;
pub(crate) mod identity;
pub(crate) mod ifo;
pub mod io;
pub mod keydb;
pub mod labels;
pub(crate) mod mpls;
pub mod mux;
pub mod pes;
pub(crate) mod platform;
pub mod profile;
pub mod progress;
pub mod scsi;
pub mod sector;
pub(crate) mod speed;
pub(crate) mod udf;
pub mod verify;

// Re-export verify types at the crate root for ergonomic imports.
pub use verify::{SectorRange, SectorStatus, VerifyResult, verify_title};

// ─── Drive lifecycle ────────────────────────────────────────────────────────
//
// `Drive::open(path)` → `wait_ready()` → `init()` → `Disc::scan()`. `Drive`
// owns the SCSI session; `DriveCapture` etc. let advanced callers introspect
// drive identity / profile data for sharing.
pub use drive::capture::{
    CapturedFeature, DriveCapture, capture_drive_data, mask_bytes, mask_string,
};
pub use drive::{Drive, DriveStatus, find_drive};

// ─── Errors ─────────────────────────────────────────────────────────────────
//
// All fallible APIs return `Result<T, Error>`. `Error` is a typed enum with a
// numeric `code()`; **no English text in the library** — applications map
// codes to localized messages. See `error.rs` for the full taxonomy.
pub use error::{Error, Result};

// ─── 0.18 primitives ────────────────────────────────────────────────────────
//
// One-bit cooperative cancellation token, shared by every long-running loop
// in libfreemkv (sweep, patch, mux). Replaces the ad-hoc `Arc<AtomicBool>`
// flags scattered across 0.17 (`DiscStream::set_halt`, autorip's
// `HALT_FLAGS` registry). Clone it cheaply; pass it by value into each
// component; poll `is_cancelled()` inside the loop body.
pub use halt::Halt;

// Generic bounded producer/consumer primitive used by sweep, patch, and
// mux to overlap reads with writes via a dedicated consumer thread.
// `Pipeline::spawn(name, depth, sink)` spawns a named consumer; `pipe.send(item)`
// pushes one item with back-pressure; `pipe.finish()` joins the
// consumer and surfaces its `close()` output. Callers implement `Sink`
// to define per-item behaviour and end-of-stream finalisation.
//
// `DEFAULT_PIPELINE_DEPTH` (=4) is for callers without specific needs;
// most should use READ_PIPELINE_DEPTH or WRITE_PIPELINE_DEPTH instead.
// Patch uses `WRITE_THROUGH_DEPTH` (=1). Returning `Flow::Stop` from
// `apply` ends the consumer cleanly (still calls `close()`).
pub use io::pipeline::{
    DEFAULT_PIPELINE_DEPTH, Flow, Pipeline, READ_PIPELINE_DEPTH, Sink, WRITE_PIPELINE_DEPTH,
    WRITE_THROUGH_DEPTH,
};

// ─── Drive events (low-level callbacks) ─────────────────────────────────────
pub use event::{Event, EventKind};
pub use identity::DriveId;
pub use profile::DriveProfile;
// Platform trait is pub(crate) — callers use Drive, not Platform directly.

// ─── Decryption (AACS / CSS) ────────────────────────────────────────────────
//
// `Disc::scan()` resolves keys and stores them on `Disc`; in most flows you
// don't touch `DecryptKeys` directly — `DiscStream::new(reader, title, keys, …)`
// accepts whatever `Disc::decrypt_keys()` returned. `decrypt_sectors()` is
// for callers that operate on raw sector buffers (e.g. ISO patching).
pub use decrypt::{DecryptKeys, decrypt_sectors};

// ─── Disc structure ─────────────────────────────────────────────────────────
//
// `Disc::scan()` produces a fully-populated `Disc` (titles, streams, AACS
// state). `Disc::identify()` is the fast path — UDF only, no playlist parse,
// for displaying disc name + format quickly while a full scan runs in the
// background. The codec / channel / resolution enums are the canonical
// structured representation; never compare against display strings.
// Note: `disc::Stream` here is the codec enum (audio / video / sub kind)
// — not the `pes::Stream` trait re-exported below as `PesStream`. Two
// different concepts, the same short name; the trait gets the `Pes`
// prefix at the crate root to keep both addressable.
pub use disc::{
    AacsState, AudioChannels, AudioStream, Clip, Codec, ColorSpace, ContentFormat, DamageSeverity,
    Disc, DiscFormat, DiscId, DiscTitle, Extent, FrameRate, HdrFormat, KeySource, LabelPurpose,
    LabelQualifier, PatchOptions, PatchOutcome, Resolution, SampleRate, ScanOptions, Stream,
    SubtitleStream, SweepOptions, VideoStream, classify_damage,
};

// ─── Streams ────────────────────────────────────────────────────────────────
//
// All stream types implement `pes::Stream` — read PES frames from a source,
// write PES frames to a sink. Pick the right type at construction:
//
// - `DiscStream` — physical drive or ISO (any `SectorSource`). Read-only.
// - `MkvStream`  — Matroska container. Read on `open()`, write on `create()`.
// - `M2tsStream` — Blu-ray Transport Stream. Read on `open()`, write on `create()`.
// - `NetworkStream` — TCP. Read on `listen()`, write on `connect()`.
// - `NullStream` — write-only black-hole sink. Useful for benchmarks.
// - `StdioStream` — pipe to/from stdin/stdout. Read or write.
//
// Most consumers use the URL resolvers (`input()` / `output()`) which pick
// the right type from a scheme:// URL. Direct construction is for callers
// that need to wire custom readers (e.g. autorip's drive-session reuse).
// The trait is re-exported as `PesStream` here to disambiguate from
// `disc::Stream` (the codec-kind enum re-exported above), which would
// otherwise collide at the crate root.
pub use pes::PesFrame;
pub use pes::Stream as PesStream;

pub use mux::DiscStream;
pub use mux::M2tsStream;
pub use mux::MkvStream;
pub use mux::NetworkStream;
pub use mux::NullStream;
pub use mux::StdioStream;
pub use mux::{InputOptions, StreamUrl, input, output, parse_url};

// ─── Lower-level surfaces ───────────────────────────────────────────────────
//
// `ScsiTransport` is the platform-abstraction trait Drive uses; expose for
// out-of-tree platform backends. `SectorSource` / `SectorSink` are the
// direction-typed read/write traits; `FileSectorSource` and `FileSectorSink`
// are the ISO-on-disk implementations. [`DecryptingSectorSource`] is the
// single decrypt-on-read decorator (AACS / CSS / none) — wrap any
// `SectorSource` to get plaintext sectors out.
pub use scsi::{DriveInfo, ScsiSense, ScsiTransport, drive_has_disc, list_drives};
pub use sector::{
    DecryptingSectorSource, FileSectorSink, FileSectorSource, SectorSink, SectorSource,
};
pub use speed::DriveSpeed;
pub use udf::{UdfFs, read_filesystem};
