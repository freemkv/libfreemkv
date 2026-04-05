//! High-level drive session — the main API for consumers.
//!
//! Opens a drive, identifies it via standard SCSI commands,
//! matches it against the profile database, and provides
//! raw disc access methods.

use std::path::Path;
use crate::error::{Error, Result};
use crate::scsi::{SgIoTransport, ScsiTransport};
use crate::identity::DriveId;
use crate::profile::{self, DriveProfile, PlatformType};
use crate::platform::{Platform, DriveStatus};
use crate::platform::mt1959::Mt1959;

/// A complete drive session.
///
/// Handles: identify → match profile → create platform → execute commands.
pub struct DriveSession {
    scsi: Box<dyn ScsiTransport>,
    platform: Box<dyn Platform>,
    pub profile: DriveProfile,
    pub drive_id: DriveId,
}

impl DriveSession {
    /// Open a drive, identify it, and find the matching profile.
    /// Uses the bundled profile database — no external files needed.
    pub fn open(device: &Path) -> Result<Self> {
        let mut transport = SgIoTransport::open(device)?;
        let profiles = profile::load_bundled()?;

        // Identify drive via standard SCSI commands
        // SPC-4 §6.4 (INQUIRY) + MMC-6 §5.3.10 (Feature 010Ch)
        let drive_id = DriveId::from_drive(&mut transport)?;

        // Match drive to a profile by INQUIRY fields
        let profile = profile::find_by_drive_id(&profiles, &drive_id)
            .cloned()
            .ok_or_else(|| Error::UnsupportedDrive(format!("{}", drive_id)))?;

        if !profile.supported {
            return Err(Error::UnsupportedDrive(format!(
                "{} — status: {:?}", drive_id, profile.status
            )));
        }

        let platform: Box<dyn Platform> = match profile.platform {
            PlatformType::Mt1959A | PlatformType::Mt1959B => {
                Box::new(Mt1959::new(profile.clone()))
            }
            PlatformType::Pioneer => {
                return Err(Error::UnsupportedDrive("Pioneer not yet implemented".into()));
            }
        };

        Ok(DriveSession {
            scsi: Box::new(transport),
            platform,
            profile,
            drive_id,
        })
    }

    /// Open with an explicit profile (skip auto-detection).
    pub fn open_with_profile(device: &Path, profile: DriveProfile) -> Result<Self> {
        let mut transport = SgIoTransport::open(device)?;
        let drive_id = DriveId::from_drive(&mut transport)?;

        let platform: Box<dyn Platform> = match profile.platform {
            PlatformType::Mt1959A | PlatformType::Mt1959B => {
                Box::new(Mt1959::new(profile.clone()))
            }
            PlatformType::Pioneer => {
                return Err(Error::UnsupportedDrive("Pioneer not yet implemented".into()));
            }
        };

        Ok(DriveSession {
            scsi: Box::new(transport),
            platform,
            profile,
            drive_id,
        })
    }

    /// Activate raw disc access mode.
    pub fn unlock(&mut self) -> Result<()> {
        self.platform.unlock(self.scsi.as_mut())
    }

    /// Check if raw disc access mode is enabled.
    pub fn is_unlocked(&self) -> bool {
        self.platform.is_unlocked()
    }

    /// Read drive status and feature flags.
    pub fn status(&mut self) -> Result<DriveStatus> {
        self.platform.status(self.scsi.as_mut())
    }

    /// Read drive configuration block.
    pub fn read_config(&mut self) -> Result<Vec<u8>> {
        self.platform.read_config(self.scsi.as_mut())
    }

    /// Read hardware register.
    pub fn read_register(&mut self, index: u8) -> Result<[u8; 16]> {
        self.platform.read_register(self.scsi.as_mut(), index)
    }

    /// Calibrate read speed for the current disc.
    pub fn calibrate(&mut self) -> Result<()> {
        self.platform.calibrate(self.scsi.as_mut())
    }

    /// Read raw disc sectors.
    pub fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        self.platform.read_sectors(self.scsi.as_mut(), lba, count, buf)
    }

    /// Generic probe command.
    pub fn probe(&mut self, sub_cmd: u8, address: u32, length: u32) -> Result<Vec<u8>> {
        self.platform.probe(self.scsi.as_mut(), sub_cmd, address, length)
    }
}
