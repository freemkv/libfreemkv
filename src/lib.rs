//! libfreemkv -- Open source optical drive library for 4K UHD / Blu-ray / DVD.
//!
//! Handles drive access, disc structure parsing, AACS decryption, and raw
//! sector reading. 206 bundled drive profiles. No external files needed.
//!
//! # Quick Start
//!
//! ```no_run
//! use libfreemkv::{DriveSession, Disc, ScanOptions, find_drive};
//! use std::path::Path;
//!
//! let device = find_drive().expect("no optical drive found");
//! let mut session = DriveSession::open(Path::new(&device)).unwrap();
//! let disc = Disc::scan(&mut session, &ScanOptions::default()).unwrap();
//!
//! for title in &disc.titles {
//!     println!("{} -- {} streams", title.duration_display(), title.streams.len());
//! }
//!
//! // Read content (decrypted automatically if AACS keys available)
//! let mut reader = disc.open_title(&mut session, 0).unwrap();
//! while let Some(unit) = reader.read_unit().unwrap() {
//!     // 6144 bytes of decrypted content per unit
//! }
//! ```
//!
//! # Architecture
//!
//! ```text
//! DriveSession           -- open, identify, unlock, read sectors
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
pub(crate) mod platform;
pub(crate) mod profile;
pub mod scsi;
pub(crate) mod sector;
pub(crate) mod speed;
pub(crate) mod udf;

pub use drive::{find_drive, find_drives, resolve_device, DriveSession};
pub use drive::capture::{DriveCapture, CapturedFeature, capture_drive_data, mask_string, mask_bytes};
pub use error::{Error, Result};
pub use event::{Event, EventKind};
pub use identity::DriveId;
pub use profile::DriveProfile;
// Platform trait is pub(crate) -- callers use DriveSession, not Platform directly
pub use disc::{
    AacsState, AudioStream, Clip, Codec, ColorSpace, ContentFormat, ContentReader, Disc,
    DiscFormat, DiscTitle, Extent, HdrFormat, KeySource, ScanOptions, Stream, SubtitleStream,
    VideoStream,
};
pub use mux::DiscOptions;
pub use mux::DiscStream;
pub use mux::IOStream;
pub use mux::IsoStream;
pub use mux::M2tsStream;
pub use mux::MkvStream;
pub use mux::NetworkStream;
pub use mux::NullStream;
pub use mux::StdioStream;
pub use mux::{open_input, open_output, parse_url, InputOptions};
pub use scsi::ScsiTransport;
pub use sector::SectorReader;
pub use speed::DriveSpeed;
pub use udf::{read_filesystem, UdfFs};
