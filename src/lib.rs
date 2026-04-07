//! libfreemkv — Open source optical drive library for 4K UHD / Blu-ray / DVD.
//!
//! Handles drive access, disc structure parsing, AACS decryption, and raw
//! sector reading. 206 bundled drive profiles. No external files needed.
//!
//! # Quick Start
//!
//! ```no_run
//! use libfreemkv::{DriveSession, Disc, ScanOptions};
//! use std::path::Path;
//!
//! let mut session = DriveSession::open(Path::new("/dev/sr0")).unwrap();
//! let disc = Disc::scan(&mut session, &ScanOptions::default()).unwrap();
//!
//! for title in &disc.titles {
//!     println!("{} — {} streams", title.duration_display(), title.streams.len());
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
//! DriveSession           — open, identify, unlock, read sectors
//!   ├── ScsiTransport    — SG_IO (Linux), IOKit (macOS)
//!   ├── DriveProfile     — per-drive unlock parameters (206 bundled)
//!   ├── DriveId          — INQUIRY + GET_CONFIG identification
//!   └── Platform
//!       └── Mt1959       — MediaTek unlock/read (Renesas planned)
//!
//! Disc                   — scan titles, streams, AACS state
//!   ├── UDF reader       — Blu-ray UDF 2.50 with metadata partitions
//!   ├── MPLS parser      — playlists → titles + clips + STN streams
//!   ├── CLPI parser      — clip info → EP map → sector extents
//!   ├── JAR parser       — BD-J audio track labels
//!   └── AACS             — encryption: key resolution + content decrypt
//!       ├── aacs         — KEYDB, VUK, MKB, unit decrypt
//!       └── handshake    — SCSI auth, ECDH, bus key
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
//! text — applications format their own messages.
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

pub mod error;
pub mod scsi;
pub mod profile;
pub mod platform;
pub mod drive;
pub mod identity;
pub mod speed;
pub mod udf;
pub mod mpls;
pub mod clpi;
pub mod disc;
pub mod jar;
pub mod aacs;

pub use error::{Error, Result};
pub use drive::DriveSession;
pub use identity::DriveId;
pub use profile::{DriveProfile, Chipset};
pub use platform::{Platform, DriveStatus};
pub use scsi::ScsiTransport;
pub use speed::DriveSpeed;
pub use disc::{Disc, DiscFormat, Title, Clip, Stream, StreamKind, Codec, HdrFormat, ColorSpace,
               Extent, ContentReader, AacsState, KeySource, ScanOptions};
