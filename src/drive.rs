//! Drive session — open, identify, and read from optical drives.
//!
//! Three-step open:
//!   1. `open()` — open device, identify drive. Always OEM.
//!   2. `wait_ready()` — wait for disc to spin up. Call before reading.
//!   3. `init()` — activate custom firmware. Optional, caller decides.

use std::path::Path;
use crate::error::{Error, Result};
use crate::scsi::ScsiTransport;
use crate::identity::DriveId;
use crate::profile::{self, DriveProfile, ProfileMatch};
use crate::platform::PlatformDriver;
use crate::platform::mt1959::Mt1959;

pub struct DriveSession {
    scsi: Box<dyn ScsiTransport>,
    driver: Box<dyn PlatformDriver>,
    pub profile: DriveProfile,
    pub platform: profile::Platform,
    pub drive_id: DriveId,
    device_path: String,
}

impl DriveSession {
    pub fn open(device: &Path) -> Result<Self> {
        let mut transport = crate::scsi::open(device)?;
        let profiles = profile::load_bundled()?;
        let drive_id = DriveId::from_drive(transport.as_mut())?;

        let m = profile::find_by_drive_id(&profiles, &drive_id)
            .ok_or_else(|| Error::UnsupportedDrive {
                vendor_id: drive_id.vendor_id.trim().to_string(),
                product_id: drive_id.product_id.trim().to_string(),
                product_revision: drive_id.product_revision.trim().to_string(),
            })?;

        let driver = create_driver(m.platform, &m.profile)?;

        Ok(DriveSession {
            scsi: transport,
            driver,
            platform: m.platform,
            profile: m.profile,
            drive_id,
            device_path: device.to_string_lossy().to_string(),
        })
    }

    pub fn wait_ready(&mut self) -> Result<()> {
        let tur = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
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

    pub fn platform_name(&self) -> &str {
        self.platform.name()
    }

    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    pub fn init(&mut self) -> Result<()> {
        self.driver.init(self.scsi.as_mut())
    }

    pub fn is_ready(&self) -> bool {
        self.driver.is_ready()
    }

    pub fn set_read_speed(&mut self, lba: u32) -> Result<()> {
        self.driver.set_read_speed(self.scsi.as_mut(), lba)
    }

    pub fn read_disc(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let cdb = [
            crate::scsi::SCSI_READ_10, 0x00,
            (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
            0x00, (count >> 8) as u8, count as u8, 0x00,
        ];
        let result = self.scsi.as_mut().execute(
            &cdb, crate::scsi::DataDirection::FromDevice, buf, 5_000)?;
        Ok(result.bytes_transferred)
    }

    pub fn read_content(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let cdb = [
            crate::scsi::SCSI_READ_10, 0x00,
            (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
            0x00, (count >> 8) as u8, count as u8, 0x00,
        ];
        let result = self.scsi.as_mut().execute(
            &cdb, crate::scsi::DataDirection::FromDevice, buf, 30_000)?;
        Ok(result.bytes_transferred)
    }

    pub fn eject(&mut self) -> Result<()> {
        let allow_cdb = [0x1Eu8, 0, 0, 0, 0x00, 0];
        let mut buf = [0u8; 0];
        let _ = self.scsi.as_mut().execute(&allow_cdb, crate::scsi::DataDirection::None, &mut buf, 5_000);
        let eject_cdb = [0x1Bu8, 0, 0, 0, 0x02, 0];
        self.scsi.as_mut().execute(&eject_cdb, crate::scsi::DataDirection::None, &mut buf, 30_000)?;
        Ok(())
    }

    pub fn scsi_execute(
        &mut self, cdb: &[u8], direction: crate::scsi::DataDirection,
        buf: &mut [u8], timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        self.scsi.as_mut().execute(cdb, direction, buf, timeout_ms)
    }
}

pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    for i in 0..16 {
        let path = format!("/dev/sg{}", i);
        if !std::path::Path::new(&path).exists() { continue; }
        if let Ok(mut transport) = crate::scsi::open(std::path::Path::new(&path)) {
            if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                let profiles = match profile::load_bundled() {
                    Ok(p) => p, Err(_) => continue,
                };
                if profile::find_by_drive_id(&profiles, &id).is_some() {
                    drives.push((path, id));
                }
            }
        }
    }
    drives
}

pub fn find_drive() -> Option<String> {
    find_drives().into_iter().next().map(|(path, _)| path)
}

pub fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
    if path.contains("/sg") {
        if !std::path::Path::new(path).exists() {
            return Err(Error::DeviceNotFound { path: path.to_string() });
        }
        return Ok((path.to_string(), None));
    }
    if path.contains("/sr") {
        let mut sr_transport = crate::scsi::open(std::path::Path::new(path))?;
        let sr_id = DriveId::from_drive(sr_transport.as_mut())?;
        drop(sr_transport);
        for (sg_path, sg_id) in find_drives() {
            if sg_id.vendor_id == sr_id.vendor_id
                && sg_id.product_id == sr_id.product_id
                && sg_id.serial_number == sr_id.serial_number
            {
                let warning = format!(
                    "{} is a block device (sr) — using {} (sg) for raw access", path, sg_path
                );
                return Ok((sg_path, Some(warning)));
            }
        }
        return Ok((path.to_string(), Some(format!(
            "{} is a block device (sr) — no matching sg device found", path
        ))));
    }
    if !std::path::Path::new(path).exists() {
        return Err(Error::DeviceNotFound { path: path.to_string() });
    }
    Ok((path.to_string(), None))
}

fn create_driver(platform: profile::Platform, profile: &DriveProfile) -> Result<Box<dyn PlatformDriver>> {
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
