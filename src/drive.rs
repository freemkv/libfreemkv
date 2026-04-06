//! High-level drive session — the main API for consumers.
//!
//! Opens a drive, identifies it via standard SCSI commands,
//! matches it against the profile database, and provides
//! raw disc access methods.

use std::path::Path;
use crate::error::{Error, Result};
use crate::scsi::ScsiTransport;
use crate::identity::DriveId;
use crate::profile::{self, DriveProfile, Chipset};
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
        let mut transport = crate::scsi::open(device)?;
        let profiles = profile::load_bundled()?;

        // Identify drive via standard SCSI commands
        // SPC-4 §6.4 (INQUIRY) + MMC-6 §5.3.10 (Feature 010Ch)
        let drive_id = DriveId::from_drive(transport.as_mut())?;

        // Match drive to a profile by INQUIRY fields
        let profile = profile::find_by_drive_id(&profiles, &drive_id)
            .cloned()
            .ok_or_else(|| Error::UnsupportedDrive {
                vendor_id: drive_id.vendor_id.trim().to_string(),
                product_id: drive_id.product_id.trim().to_string(),
                product_revision: drive_id.product_revision.trim().to_string(),
            })?;

        let platform: Box<dyn Platform> = match profile.chipset {
            Chipset::MediaTek => {
                Box::new(Mt1959::new(profile.clone()))
            }
            Chipset::Renesas => {
                return Err(Error::UnsupportedDrive {
                    vendor_id: drive_id.vendor_id.trim().to_string(),
                    product_id: drive_id.product_id.trim().to_string(),
                    product_revision: "Renesas not yet implemented".to_string(),
                });
            }
        };

        Ok(DriveSession {
            scsi: transport,
            platform,
            profile,
            drive_id,
        })
    }

    /// Open with an explicit profile (skip auto-detection).
    pub fn open_with_profile(device: &Path, profile: DriveProfile) -> Result<Self> {
        let mut transport = crate::scsi::open(device)?;
        let drive_id = DriveId::from_drive(transport.as_mut())?;

        let platform: Box<dyn Platform> = match profile.chipset {
            Chipset::MediaTek => {
                Box::new(Mt1959::new(profile.clone()))
            }
            Chipset::Renesas => {
                return Err(Error::UnsupportedDrive {
                    vendor_id: drive_id.vendor_id.trim().to_string(),
                    product_id: drive_id.product_id.trim().to_string(),
                    product_revision: "Renesas not yet implemented".to_string(),
                });
            }
        };

        Ok(DriveSession {
            scsi: transport,
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

    /// Standard READ(10) — reads unencrypted sectors (UDF filesystem, etc).
    /// Does not require unlock. Use read_sectors() for raw/encrypted content.
    pub fn read_disc(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let cdb = [
            0x28, 0x00, // READ(10), no flags
            (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
            0x00,
            (count >> 8) as u8, count as u8,
            0x00,
        ];
        let result = self.scsi.as_mut().execute(
            &cdb, crate::scsi::DataDirection::FromDevice, buf, 30_000)?;
        Ok(result.bytes_transferred)
    }

    /// Send a raw SCSI CDB. Used by UDF reader and disc structure parsers.
    pub fn scsi_execute(&mut self, cdb: &[u8], direction: crate::scsi::DataDirection, buf: &mut [u8], timeout_ms: u32) -> Result<crate::scsi::ScsiResult> {
        self.scsi.as_mut().execute(cdb, direction, buf, timeout_ms)
    }
}
