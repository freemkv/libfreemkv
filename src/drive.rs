//! Drive session — open, identify, unlock, and read from optical drives.
//!
//! `DriveSession` is the entry point for all drive interaction. It handles
//! device identification, profile matching, platform-specific unlock, and
//! provides both raw sector reads and standard SCSI command execution.
//!
//! Two open modes:
//!   - `open()` — identify + unlock. Ready for reading immediately.
//!   - `open_no_unlock()` — identify only. Used for AACS authentication
//!     which must happen before the drive enters raw mode.

use std::path::Path;
use crate::error::{Error, Result};
use crate::scsi::ScsiTransport;
use crate::identity::DriveId;
use crate::profile::{self, DriveProfile, Chipset};
use crate::platform::{Platform, DriveStatus};
use crate::platform::mt1959::Mt1959;

/// A drive session with identification, platform, and SCSI transport.
///
/// Created via `DriveSession::open()` or `DriveSession::open_no_unlock()`.
/// All disc reading goes through this struct.
pub struct DriveSession {
    scsi: Box<dyn ScsiTransport>,
    platform: Box<dyn Platform>,
    pub profile: DriveProfile,
    pub drive_id: DriveId,
    device_path: String,
}

impl DriveSession {
    /// Open a drive, identify it, match a profile, and unlock for raw reads.
    ///
    /// This is the standard entry point. After `open()`, the drive is ready
    /// for sector reads, disc scanning, and content extraction.
    pub fn open(device: &Path) -> Result<Self> {
        let mut session = Self::open_no_unlock(device)?;
        session.wait_ready()?;
        let _ = session.unlock(); // silently ignore — unencrypted discs don't need it
        Ok(session)
    }

    /// Open a drive WITHOUT unlocking.
    ///
    /// Used when AACS authentication must happen before raw mode.
    /// The AACS SCSI handshake requires the drive's standard firmware
    /// state — unlocking puts the drive in vendor-specific raw mode
    /// which disables the AACS layer.
    pub fn open_no_unlock(device: &Path) -> Result<Self> {
        let mut transport = crate::scsi::open(device)?;
        let profiles = profile::load_bundled()?;
        let drive_id = DriveId::from_drive(transport.as_mut())?;

        let profile = profile::find_by_drive_id(&profiles, &drive_id)
            .cloned()
            .ok_or_else(|| Error::UnsupportedDrive {
                vendor_id: drive_id.vendor_id.trim().to_string(),
                product_id: drive_id.product_id.trim().to_string(),
                product_revision: drive_id.product_revision.trim().to_string(),
            })?;

        let platform = create_platform(&profile, &drive_id)?;

        Ok(DriveSession {
            scsi: transport,
            platform,
            profile,
            drive_id,
            device_path: device.to_string_lossy().to_string(),
        })
    }

    /// Open with an explicit profile, skipping auto-detection.
    pub fn open_with_profile(device: &Path, profile: DriveProfile) -> Result<Self> {
        let mut transport = crate::scsi::open(device)?;
        let drive_id = DriveId::from_drive(transport.as_mut())?;
        let platform = create_platform(&profile, &drive_id)?;

        Ok(DriveSession {
            scsi: transport,
            platform,
            profile,
            drive_id,
            device_path: device.to_string_lossy().to_string(),
        })
    }

    /// Wait for the drive to become ready (disc spun up).
    /// Polls TEST UNIT READY up to 30 seconds.
    pub fn wait_ready(&mut self) -> Result<()> {
        let tur = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // TEST UNIT READY
        for _ in 0..60 {
            let mut buf = [0u8; 0];
            if self.scsi.as_mut().execute(
                &tur, crate::scsi::DataDirection::None, &mut buf, 5000
            ).is_ok() {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        Err(Error::DeviceNotFound {
            path: format!("{}: drive not ready after 30s", self.device_path),
        })
    }

    /// Device path this session was opened on.
    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    /// Activate raw disc access mode (vendor-specific unlock).
    pub fn unlock(&mut self) -> Result<()> {
        self.platform.unlock(self.scsi.as_mut())
    }

    /// Check if raw disc access mode is active.
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

    /// Read raw disc sectors via platform-specific command.
    pub fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        self.platform.read_sectors(self.scsi.as_mut(), lba, count, buf)
    }

    /// Platform-specific probe command.
    pub fn probe(&mut self, sub_cmd: u8, address: u32, length: u32) -> Result<Vec<u8>> {
        self.platform.probe(self.scsi.as_mut(), sub_cmd, address, length)
    }

    /// Standard SCSI READ(10) for disc filesystem data (UDF, MPLS, CLPI).
    pub fn read_disc(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let cdb = [
            crate::scsi::SCSI_READ_10, 0x00,
            (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
            0x00,
            (count >> 8) as u8, count as u8,
            0x00,
        ];
        let result = self.scsi.as_mut().execute(
            &cdb, crate::scsi::DataDirection::FromDevice, buf, 5_000)?;
        Ok(result.bytes_transferred)
    }

    /// Execute a raw SCSI CDB. Used by parsers and AACS handshake.
    pub fn scsi_execute(
        &mut self,
        cdb: &[u8],
        direction: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        self.scsi.as_mut().execute(cdb, direction, buf, timeout_ms)
    }
}

/// Create the platform-specific driver for a given chipset.
fn create_platform(profile: &DriveProfile, drive_id: &DriveId) -> Result<Box<dyn Platform>> {
    match profile.chipset {
        Chipset::MediaTek => Ok(Box::new(Mt1959::new(profile.clone()))),
        Chipset::Renesas => Err(Error::UnsupportedDrive {
            vendor_id: drive_id.vendor_id.trim().to_string(),
            product_id: drive_id.product_id.trim().to_string(),
            product_revision: "Renesas not yet implemented".to_string(),
        }),
    }
}
