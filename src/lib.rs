//! libfreemkv — Open source optical drive library for 4K UHD / Blu-ray / DVD.
//!
//! Drive access, disc format parsing, and raw sector reading in one library.
//! 206 bundled drive profiles. No external files, no configuration.
//!
//! # Drive Access
//!
//! ```no_run
//! use libfreemkv::DriveSession;
//! use std::path::Path;
//!
//! // Open drive — profiles are bundled, auto-identify
//! let mut session = DriveSession::open(Path::new("/dev/sr0")).unwrap();
//!
//! // Drive identity
//! println!("{} {}", session.drive_id.vendor_id.trim(), session.drive_id.product_id.trim());
//!
//! // Unlock and read raw sectors
//! session.unlock().unwrap();
//! session.calibrate().unwrap();
//! let mut buf = vec![0u8; 2048];
//! session.read_sectors(0, 1, &mut buf).unwrap();
//! ```
//!
//! # Disc Scanning
//!
//! ```no_run
//! # use libfreemkv::{DriveSession, Disc, Title, Stream, StreamKind};
//! # use std::path::Path;
//! # let mut session = DriveSession::open(Path::new("/dev/sr0")).unwrap();
//! // Scan disc structure — UDF filesystem, MPLS playlists, CLPI clip info
//! // (API in progress — Disc::scan() coming soon)
//!
//! // Each title has typed streams:
//! // stream.codec    → Codec::Hevc / Codec::TrueHd / Codec::Ac3 / Codec::Pgs
//! // stream.pid      → 0x1100
//! // stream.language  → "eng"
//! // stream.hdr      → HdrFormat::Hdr10 / HdrFormat::DolbyVision
//! ```
//!
//! # Architecture
//!
//! ```text
//! DriveSession          — open, identify, unlock, read sectors
//!   ├── ScsiTransport   — SG_IO (Linux), IOKit (macOS planned)
//!   ├── DriveProfile    — per-drive unlock parameters (206 bundled)
//!   ├── DriveId         — INQUIRY + GET_CONFIG 010C identification
//!   └── Platform
//!       └── Mt1959      — MediaTek unlock/read (Renesas planned)
//!
//! Disc                  — scan titles, streams, sector ranges
//!   ├── UDF reader      — Blu-ray UDF 2.50 with metadata partitions
//!   ├── MPLS parser     — playlists → titles + clips + STN streams
//!   └── CLPI parser     — clip info → EP map → sector extents
//! ```
//!
//! # Error Codes
//!
//! All errors are structured with numeric codes (E1000-E6000).
//! No user-facing English text — applications format their own messages.
//!
//! | Range | Category |
//! |-------|----------|
//! | E1xxx | Device errors (not found, permission) |
//! | E2xxx | Profile errors (unsupported drive, parse) |
//! | E3xxx | Unlock errors (failed, signature mismatch) |
//! | E4xxx | SCSI errors (command failed, timeout) |
//! | E5xxx | I/O errors |
//! | E6xxx | Disc format errors |

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

pub use error::{Error, Result};
pub use drive::DriveSession;
pub use identity::DriveId;
pub use profile::{DriveProfile, Chipset};
pub use platform::{Platform, DriveStatus};
pub use scsi::ScsiTransport;
pub use speed::DriveSpeed;
pub use disc::{Disc, Title, Stream, StreamKind, Codec, HdrFormat, ColorSpace, Extent};
