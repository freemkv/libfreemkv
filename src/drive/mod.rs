//! Drive session — open, identify, and read from optical drives.
//!
//! Three-step open:
//!   1. `open()` — open device, identify drive. Always OEM.
//!   2. `wait_ready()` — wait for disc to spin up. Call before reading.
//!   3. `init()` — activate custom firmware. Removes riplock.
//!   4. `probe_disc()` — probe disc surface. Drive learns optimal speeds.

pub mod capture;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

use crate::error::{Error, Result};
use crate::identity::DriveId;
use crate::platform::mt1959::Mt1959;
use crate::platform::PlatformDriver;
use crate::profile::{self, DriveProfile};
use crate::scsi::ScsiTransport;
use crate::sector::SectorReader;
use std::path::Path;

/// Physical state of the drive tray and disc.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DriveStatus {
    /// Tray is open
    TrayOpen,
    /// Tray closed, no disc
    NoDisc,
    /// Tray closed, disc present and ready
    DiscPresent,
    /// Drive is loading or spinning up
    NotReady,
    /// Could not determine status
    Unknown,
}

/// Optical disc drive session -- open, identify, unlock, and read.
pub struct DriveSession {
    scsi: Box<dyn ScsiTransport>,
    driver: Option<Box<dyn PlatformDriver>>,
    pub profile: Option<DriveProfile>,
    pub platform: Option<profile::Platform>,
    pub drive_id: DriveId,
    device_path: String,
}

impl DriveSession {
    pub fn open(device: &Path) -> Result<Self> {
        let mut transport = crate::scsi::open(device)?;
        let profiles = profile::load_bundled()?;
        let drive_id = DriveId::from_drive(transport.as_mut())?;

        let m = profile::find_by_drive_id(&profiles, &drive_id);
        let (driver, platform, profile) = match m {
            Some(m) => (
                create_driver(m.platform, &m.profile).ok(),
                Some(m.platform),
                Some(m.profile),
            ),
            None => (None, None, None),
        };

        Ok(DriveSession {
            scsi: transport,
            driver,
            platform,
            profile,
            drive_id,
            device_path: device.to_string_lossy().to_string(),
        })
    }

    /// Whether this drive has a known profile (unlock parameters available).
    pub fn has_profile(&self) -> bool {
        self.profile.is_some()
    }

    pub fn wait_ready(&mut self) -> Result<()> {
        let tur = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut tried_reset = false;

        for _ in 0..60 {
            let mut buf = [0u8; 0];
            match self.scsi.as_mut().execute(
                &tur,
                crate::scsi::DataDirection::None,
                &mut buf,
                5_000,
            ) {
                Ok(_) => return Ok(()),
                Err(Error::ScsiError { sense_key: 5, .. }) if !tried_reset => {
                    // Illegal Request on TUR — drive may be stuck from a previous session.
                    // Try reset() which attempts multiple recovery approaches.
                    tried_reset = true;
                    if self.reset().is_ok() {
                        return Ok(());
                    }
                    // If reset failed but disc is present, proceed anyway —
                    // the scan path will handle errors individually.
                    if self.drive_status() == DriveStatus::DiscPresent {
                        return Ok(());
                    }
                }
                Err(_) => {}
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        Err(Error::DeviceNotFound {
            path: format!("{}: drive not ready after 30s", self.device_path),
        })
    }

    /// Query the physical state of the drive — disc present, tray open, etc.
    /// Uses GET EVENT STATUS NOTIFICATION which works regardless of firmware state.
    pub fn drive_status(&mut self) -> DriveStatus {
        // GET EVENT STATUS NOTIFICATION: polled, media event class (0x10)
        let cdb = [0x4Au8, 0x01, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x08, 0x00];
        let mut buf = [0u8; 8];
        match self.scsi.as_mut().execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        ) {
            Ok(r) if r.bytes_transferred >= 6 => {
                let media_status = buf[5];
                // Bits 1-0: door/tray state
                // Bit 1: media present, Bit 0: tray open
                match media_status & 0x03 {
                    0x00 => DriveStatus::NoDisc,    // tray closed, no disc
                    0x01 => DriveStatus::TrayOpen,  // tray open
                    0x02 => DriveStatus::DiscPresent, // tray closed, disc present
                    0x03 => DriveStatus::DiscPresent, // tray closed, disc present
                    _ => DriveStatus::Unknown,
                }
            }
            _ => {
                // Fallback: try TUR
                let tur = [0x00u8, 0x00, 0x00, 0x00, 0x00, 0x00];
                let mut empty = [0u8; 0];
                match self.scsi.as_mut().execute(
                    &tur,
                    crate::scsi::DataDirection::None,
                    &mut empty,
                    5_000,
                ) {
                    Ok(_) => DriveStatus::DiscPresent,
                    Err(Error::ScsiError { sense_key: 2, .. }) => DriveStatus::NotReady,
                    Err(Error::ScsiError { sense_key: 6, .. }) => DriveStatus::NotReady, // UNIT ATTENTION
                    _ => DriveStatus::Unknown,
                }
            }
        }
    }

    /// Attempt to reset the drive to a clean state.
    /// Tries multiple approaches in order:
    /// 1. PREVENT ALLOW MEDIUM REMOVAL (allow) — clears command locks
    /// 2. START STOP UNIT (start) — restarts the disc
    /// 3. If the drive has a profile, re-init (firmware re-upload + unlock)
    pub fn reset(&mut self) -> Result<()> {
        let mut buf = [0u8; 0];

        // 1. Allow medium removal (clears any prevent lock)
        let allow = [0x1Eu8, 0x00, 0x00, 0x00, 0x00, 0x00];
        let _ = self.scsi.as_mut().execute(
            &allow, crate::scsi::DataDirection::None, &mut buf, 5_000,
        );

        // 2. START STOP UNIT: stop then start (forces disc re-spin)
        let stop = [0x1Bu8, 0x00, 0x00, 0x00, 0x00, 0x00]; // stop
        let _ = self.scsi.as_mut().execute(
            &stop, crate::scsi::DataDirection::None, &mut buf, 5_000,
        );
        std::thread::sleep(std::time::Duration::from_millis(500));
        let start = [0x1Bu8, 0x00, 0x00, 0x00, 0x01, 0x00]; // start
        let _ = self.scsi.as_mut().execute(
            &start, crate::scsi::DataDirection::None, &mut buf, 5_000,
        );
        std::thread::sleep(std::time::Duration::from_millis(2000));

        // 3. Check if TUR works now
        let tur = [0x00u8, 0x00, 0x00, 0x00, 0x00, 0x00];
        if self.scsi.as_mut().execute(
            &tur, crate::scsi::DataDirection::None, &mut buf, 5_000,
        ).is_ok() {
            return Ok(());
        }

        // 4. If still stuck and we have a profile, try re-init
        if self.driver.is_some() {
            self.init()?;
            std::thread::sleep(std::time::Duration::from_millis(1000));
            if self.scsi.as_mut().execute(
                &tur, crate::scsi::DataDirection::None, &mut buf, 5_000,
            ).is_ok() {
                return Ok(());
            }
        }

        Err(Error::DeviceNotFound {
            path: format!("{}: drive reset failed", self.device_path),
        })
    }

    pub fn platform_name(&self) -> &str {
        match self.platform {
            Some(ref p) => p.name(),
            None => "Unknown",
        }
    }

    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    /// Initialize drive — unlock + firmware upload.
    /// Optional. Adds features: removes riplock, enables UHD reads, speed control.
    pub fn init(&mut self) -> Result<()> {
        match self.driver {
            Some(ref mut d) => d.init(self.scsi.as_mut()),
            None => Err(Error::UnsupportedDrive {
                vendor_id: self.drive_id.vendor_id.trim().to_string(),
                product_id: self.drive_id.product_id.trim().to_string(),
                product_revision: self.drive_id.product_revision.trim().to_string(),
            }),
        }
    }

    /// Probe disc surface so the drive firmware learns optimal read speeds
    /// per region. After this the host reads at max speed and the drive
    /// manages zones internally.
    pub fn probe_disc(&mut self) -> Result<()> {
        match self.driver {
            Some(ref mut d) => d.probe_disc(self.scsi.as_mut()),
            None => Err(Error::UnsupportedDrive {
                vendor_id: self.drive_id.vendor_id.trim().to_string(),
                product_id: self.drive_id.product_id.trim().to_string(),
                product_revision: self.drive_id.product_revision.trim().to_string(),
            }),
        }
    }

    /// Query a specific GET CONFIGURATION feature by code.
    /// Returns the feature data (without the 8-byte header), or None if not available.
    pub fn get_config_feature(&mut self, feature_code: u16) -> Option<Vec<u8>> {
        let cdb = [
            crate::scsi::SCSI_GET_CONFIGURATION, 0x02,
            (feature_code >> 8) as u8, feature_code as u8,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];
        let mut buf = vec![0u8; 256];
        let r = self.scsi.as_mut()
            .execute(&cdb, crate::scsi::DataDirection::FromDevice, &mut buf, 5_000).ok()?;
        if r.bytes_transferred > 8 {
            Some(buf[8..r.bytes_transferred].to_vec())
        } else {
            None
        }
    }

    /// Read REPORT KEY RPC state (region playback control).
    pub fn report_key_rpc_state(&mut self) -> Option<Vec<u8>> {
        let cdb = [0xA4u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x08, 0x00];
        let mut buf = vec![0u8; 8];
        let r = self.scsi.as_mut()
            .execute(&cdb, crate::scsi::DataDirection::FromDevice, &mut buf, 5_000).ok()?;
        if r.bytes_transferred > 0 { Some(buf[..r.bytes_transferred].to_vec()) } else { None }
    }

    /// Read MODE SENSE page data.
    pub fn mode_sense_page(&mut self, page: u8) -> Option<Vec<u8>> {
        let cdb = [0x5Au8, 0x00, page, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFC, 0x00];
        let mut buf = vec![0u8; 252];
        let r = self.scsi.as_mut()
            .execute(&cdb, crate::scsi::DataDirection::FromDevice, &mut buf, 5_000).ok()?;
        if r.bytes_transferred > 0 { Some(buf[..r.bytes_transferred].to_vec()) } else { None }
    }

    /// Read vendor-specific READ BUFFER data.
    pub fn read_buffer(&mut self, mode: u8, buffer_id: u8, length: u16) -> Option<Vec<u8>> {
        let cdb = crate::scsi::build_read_buffer(mode, buffer_id, 0, length as u32);
        let mut buf = vec![0u8; length as usize];
        let r = self.scsi.as_mut()
            .execute(&cdb, crate::scsi::DataDirection::FromDevice, &mut buf, 5_000).ok()?;
        if r.bytes_transferred > 0 { Some(buf[..r.bytes_transferred].to_vec()) } else { None }
    }

    pub fn is_ready(&self) -> bool {
        match self.driver {
            Some(ref d) => d.is_ready(),
            None => false,
        }
    }

    pub fn read_disc(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let cdb = [
            crate::scsi::SCSI_READ_10,
            0x00,
            (lba >> 24) as u8,
            (lba >> 16) as u8,
            (lba >> 8) as u8,
            lba as u8,
            0x00,
            (count >> 8) as u8,
            count as u8,
            0x00,
        ];
        let result =
            self.scsi
                .as_mut()
                .execute(&cdb, crate::scsi::DataDirection::FromDevice, buf, 5_000)?;
        Ok(result.bytes_transferred)
    }

    pub fn read_content(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let cdb = [
            crate::scsi::SCSI_READ_10,
            0x00,
            (lba >> 24) as u8,
            (lba >> 16) as u8,
            (lba >> 8) as u8,
            lba as u8,
            0x00,
            (count >> 8) as u8,
            count as u8,
            0x00,
        ];
        let result = self.scsi.as_mut().execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            buf,
            30_000,
        )?;
        Ok(result.bytes_transferred)
    }

    pub fn set_speed(&mut self, speed_kbs: u16) {
        let cdb = crate::scsi::build_set_cd_speed(speed_kbs);
        let mut dummy = [0u8; 0];
        let _ = self.scsi_execute(&cdb, crate::scsi::DataDirection::None, &mut dummy, 5_000);
    }

    pub fn eject(&mut self) -> Result<()> {
        let allow_cdb = [0x1Eu8, 0, 0, 0, 0x00, 0];
        let mut buf = [0u8; 0];
        let _ = self.scsi.as_mut().execute(
            &allow_cdb,
            crate::scsi::DataDirection::None,
            &mut buf,
            5_000,
        );
        let eject_cdb = [0x1Bu8, 0, 0, 0, 0x02, 0];
        self.scsi.as_mut().execute(
            &eject_cdb,
            crate::scsi::DataDirection::None,
            &mut buf,
            30_000,
        )?;
        Ok(())
    }

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

impl SectorReader for DriveSession {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        self.read_disc(lba, count, buf)
    }
}

/// Find all optical drives connected to this system.
pub fn find_drives() -> Vec<(String, DriveId)> {
    #[cfg(target_os = "linux")]
    {
        linux::find_drives()
    }
    #[cfg(target_os = "macos")]
    {
        macos::find_drives()
    }
    #[cfg(windows)]
    {
        windows::find_drives()
    }
}

/// Find the first optical drive, returning its device path.
pub fn find_drive() -> Option<String> {
    find_drives().into_iter().next().map(|(path, _)| path)
}

/// Resolve a device path to its raw SCSI device, with optional warning message.
pub fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
    #[cfg(target_os = "linux")]
    {
        linux::resolve_device(path)
    }
    #[cfg(target_os = "macos")]
    {
        macos::resolve_device(path)
    }
    #[cfg(windows)]
    {
        windows::resolve_device(path)
    }
}

fn create_driver(
    platform: profile::Platform,
    profile: &DriveProfile,
) -> Result<Box<dyn PlatformDriver>> {
    match platform {
        profile::Platform::Mt1959A => Ok(Box::new(Mt1959::new(profile.clone(), false))),
        profile::Platform::Mt1959B => Ok(Box::new(Mt1959::new(profile.clone(), true))),
        profile::Platform::Renesas => Err(Error::UnsupportedDrive {
            vendor_id: profile.identity.vendor_id.trim().to_string(),
            product_id: String::new(),
            product_revision: "Renesas not yet implemented".to_string(),
        }),
    }
}
