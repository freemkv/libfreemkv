//! libfreemkv — Open source raw disc access for optical drives.
//!
//! Provides SCSI/MMC commands to enable raw reading mode on compatible
//! Blu-ray drives, allowing direct sector access for disc archival
//! and backup purposes.
//!
//! # Architecture
//!
//! The library is data-driven. Drive-specific SCSI command sequences
//! are stored in profile files, not in code. Adding support for a new
//! drive requires only a profile contribution — no rebuild needed.
//!
//! ```text
//! DriveSession (high-level API)
//!   ├── Platform trait (per-chipset unlock logic)
//!   ├── DriveProfile (per-drive data from JSON profiles)
//!   └── ScsiTransport (SG_IO on Linux, IOKit on macOS)
//! ```
//!
//! # Quick Start
//!
//! ```no_run
//! use libfreemkv::DriveSession;
//! use std::path::Path;
//!
//! let mut session = DriveSession::open(
//!     Path::new("/dev/sr0"),
//!     Path::new("profiles/"),
//! ).unwrap();
//!
//! session.enable().unwrap();
//! session.calibrate().unwrap();
//!
//! let mut buf = vec![0u8; 2048];
//! let n = session.read_sectors(0, 1, &mut buf).unwrap();
//! ```

pub mod error;
pub mod scsi;
pub mod profile;
pub mod platform;
pub mod drive;
pub mod identity;
pub mod speed;

pub use error::{Error, Result};
pub use drive::DriveSession;
pub use identity::DriveId;
pub use profile::{DriveProfile, PlatformType};
pub use platform::{Platform, DriveStatus};
pub use scsi::ScsiTransport;
pub use speed::DriveSpeed;
