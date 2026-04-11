//! Drive session — open, identify, and read from optical drives.
//!
//! Three-step open:
//!   1. `open()` — open device, identify drive. Always OEM.
//!   2. `wait_ready()` — wait for disc to spin up. Call before reading.
//!   3. `init()` — activate custom firmware. Removes riplock.
//!   4. `probe_disc()` — probe disc surface. Drive learns optimal speeds.

#[cfg(unix)]
mod unix;
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
        for _ in 0..60 {
            let mut buf = [0u8; 0];
            if self
                .scsi
                .as_mut()
                .execute(&tur, crate::scsi::DataDirection::None, &mut buf, 5000)
                .is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        Err(Error::DeviceNotFound {
            path: format!("{}: drive not ready after 30s", self.device_path),
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

pub fn find_drives() -> Vec<(String, DriveId)> {
    #[cfg(unix)]
    {
        unix::find_drives()
    }
    #[cfg(windows)]
    {
        windows::find_drives()
    }
}

pub fn find_drive() -> Option<String> {
    find_drives().into_iter().next().map(|(path, _)| path)
}

pub fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
    #[cfg(unix)]
    {
        unix::resolve_device(path)
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
