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
pub(crate) mod identity;
pub(crate) mod ifo;
pub mod keydb;
pub(crate) mod labels;
pub(crate) mod mpls;
pub mod mux;
pub mod pes;
pub(crate) mod platform;
pub mod profile;
pub mod scsi;
pub mod sector;
pub(crate) mod speed;
pub(crate) mod udf;

pub use drive::capture::{
    capture_drive_data, mask_bytes, mask_string, CapturedFeature, DriveCapture,
};
pub use drive::{find_drive, find_drives, Drive, DriveStatus};
pub use error::{Error, Result};
pub use event::{Event, EventKind};
pub use identity::DriveId;
pub use profile::DriveProfile;
// Platform trait is pub(crate) -- callers use Drive, not Platform directly
pub use decrypt::{decrypt_sectors, DecryptKeys};
pub use disc::{
    AacsState, AudioChannels, AudioStream, Clip, Codec, ColorSpace, ContentFormat, Disc,
    DiscFormat, DiscId, DiscTitle, Extent, FrameRate, HdrFormat, KeySource, Resolution, SampleRate,
    ScanOptions, Stream, SubtitleStream, VideoStream,
};
pub use mux::DiscStream;
pub use mux::M2tsStream;
pub use mux::MkvStream;
pub use mux::NetworkStream;
pub use mux::NullStream;
pub use mux::StdioStream;
pub use mux::{input, output, parse_url, InputOptions, StreamUrl};
pub use scsi::ScsiTransport;
pub use sector::SectorReader;
pub use speed::DriveSpeed;
pub use udf::{read_filesystem, UdfFs};
