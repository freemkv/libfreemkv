//! Drive session — open, identify, and read from optical drives.
//!
//! Three-step open:
//!   1. `open()` — open device, identify drive. Always OEM.
//!   2. `wait_ready()` — wait for disc to spin up. Call before reading.
//!   3. `init()` — activate custom firmware. Removes riplock.
//!   4. `probe_disc()` — probe disc surface. Drive learns optimal speeds.

pub mod capture;

// Per-platform discovery helpers (the `pub(crate)` `find_drives` /
// equivalents). Crate-public so `scsi/{linux,macos,windows}.rs` can
// reuse the existing enumeration logic when shaping `DriveInfo`.
#[cfg(target_os = "linux")]
pub(crate) mod linux;
#[cfg(target_os = "macos")]
pub(crate) mod macos;
#[cfg(windows)]
pub(crate) mod windows;

use crate::error::{Error, Result};
use crate::event::{Event, EventKind};
use crate::identity::DriveId;
use crate::platform::PlatformDriver;
use crate::platform::mt1959::Mt1959;
use crate::profile::{self, DriveProfile};
use crate::scsi::ScsiTransport;
use crate::sector::SectorReader;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

// SCSI opcodes used in drive control
const SCSI_TEST_UNIT_READY: u8 = 0x00;
const SCSI_START_STOP_UNIT: u8 = 0x1B;
const SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL: u8 = 0x1E;
const SCSI_GET_EVENT_STATUS: u8 = 0x4A;
const SCSI_MODE_SENSE: u8 = 0x5A;
const SCSI_REPORT_KEY: u8 = 0xA4;

/// Optical disc drive session -- open, identify, unlock, and read.
pub struct Drive {
    scsi: Box<dyn ScsiTransport>,
    driver: Option<Box<dyn PlatformDriver>>,
    pub profile: Option<DriveProfile>,
    pub platform: Option<profile::Platform>,
    pub drive_id: DriveId,
    device_path: String,
    /// Halt flag — when set, Drive::read() bails at the next check point.
    halt: Arc<AtomicBool>,
    /// Event handler — fires for read errors and library-level state changes.
    event_fn: Option<Box<dyn Fn(Event) + Send>>,
}

impl Drive {
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

        Ok(Drive {
            scsi: transport,
            driver,
            platform,
            profile,
            drive_id,
            device_path: device.to_string_lossy().to_string(),
            halt: Arc::new(AtomicBool::new(false)),
            event_fn: None,
        })
    }

    /// Get a clone of the halt flag. Set to true to interrupt Drive::read().
    pub fn halt_flag(&self) -> Arc<AtomicBool> {
        self.halt.clone()
    }

    /// Halt the drive — Drive::read() will bail at the next check point.
    pub fn halt(&self) {
        self.halt.store(true, Ordering::Relaxed);
    }

    /// Clear the halt flag for the next operation.
    pub fn clear_halt(&self) {
        self.halt.store(false, Ordering::Relaxed);
    }

    /// Set an event handler for read recovery events.
    pub fn on_event(&mut self, f: impl Fn(Event) + Send + 'static) {
        self.event_fn = Some(Box::new(f));
    }

    fn emit(&self, kind: EventKind) {
        if let Some(ref f) = self.event_fn {
            f(Event { kind });
        }
    }

    fn is_halted(&self) -> bool {
        self.halt.load(Ordering::Relaxed)
    }

    /// Halt-aware SCSI execute. Returns `Err(Halted)` if the flag is set
    /// before the command dispatches or by the time it completes. The only
    /// path to talk to the drive in the recovery hot loop; keeps Drive::read
    /// free of explicit halt checks.
    fn checked_exec(
        &mut self,
        cdb: &[u8],
        dir: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        if self.is_halted() {
            return Err(Error::Halted);
        }
        let r = self.scsi.as_mut().execute(cdb, dir, buf, timeout_ms)?;
        if self.is_halted() {
            return Err(Error::Halted);
        }
        Ok(r)
    }

    /// Close the drive cleanly. Unlocks tray, flushes SCSI state, closes fd.
    /// Also runs automatically on Drop as a safety net.
    pub fn close(self) {
        // cleanup() runs here via Drop
    }

    /// Shared cleanup — called by Drop (and thus by close).
    fn cleanup(&mut self) {
        self.unlock_tray();
    }

    // NOTE: Debug aid — remove after fd issue is resolved
    pub fn device_path_owned(&self) -> String {
        self.device_path.clone()
    }

    /// Whether this drive has a known profile (unlock parameters available).
    pub fn has_profile(&self) -> bool {
        self.profile.is_some()
    }

    /// Access the SCSI transport for direct commands (used by CSS/AACS auth).
    pub fn scsi_mut(&mut self) -> &mut dyn ScsiTransport {
        self.scsi.as_mut()
    }

    pub fn wait_ready(&mut self) -> Result<()> {
        let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];
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
        Err(Error::DeviceNotReady {
            path: self.device_path.clone(),
        })
    }

    /// Query the physical state of the drive — disc present, tray open, etc.
    /// Uses GET EVENT STATUS NOTIFICATION which works regardless of firmware state.
    pub fn drive_status(&mut self) -> DriveStatus {
        // GET EVENT STATUS NOTIFICATION: polled, media event class (0x10)
        let cdb = [
            SCSI_GET_EVENT_STATUS,
            0x01,
            0x00,
            0x00,
            0x10,
            0x00,
            0x00,
            0x00,
            0x08,
            0x00,
        ];
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
                    0x00 => DriveStatus::NoDisc,      // tray closed, no disc
                    0x01 => DriveStatus::TrayOpen,    // tray open
                    0x02 => DriveStatus::DiscPresent, // tray closed, disc present
                    0x03 => DriveStatus::DiscPresent, // tray closed, disc present
                    _ => DriveStatus::Unknown,
                }
            }
            _ => {
                // Fallback: try TUR
                let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];
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
    ///
    /// Escalates through increasingly aggressive recovery:
    /// 1. Unlock tray + stop/start — handles normal stuck states
    /// 2. Eject cycle — clears LibreDrive firmware stuck state (proven on BU40N)
    /// 3. Re-init — firmware re-upload if profile available
    ///
    /// Note: step 2 physically ejects the tray. On slimline drives the user
    /// must push it back in manually. Returns Ok(()) if TUR succeeds after
    /// any step, even if the drive reports "tray open" (that's a valid state).
    pub fn reset(&mut self) -> Result<()> {
        let mut buf = [0u8; 0];
        let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];

        // 1. Unlock + stop/start
        self.unlock_tray();
        let stop = [SCSI_START_STOP_UNIT, 0x00, 0x00, 0x00, 0x00, 0x00];
        let _ =
            self.scsi
                .as_mut()
                .execute(&stop, crate::scsi::DataDirection::None, &mut buf, 5_000);
        std::thread::sleep(std::time::Duration::from_millis(500));
        let start = [SCSI_START_STOP_UNIT, 0x00, 0x00, 0x00, 0x01, 0x00];
        let _ =
            self.scsi
                .as_mut()
                .execute(&start, crate::scsi::DataDirection::None, &mut buf, 5_000);
        std::thread::sleep(std::time::Duration::from_millis(2000));

        if self
            .scsi
            .as_mut()
            .execute(&tur, crate::scsi::DataDirection::None, &mut buf, 5_000)
            .is_ok()
        {
            return Ok(());
        }

        // 2. Eject cycle — clears MT1959 LibreDrive stuck state.
        // After eject, TUR returning "Not Ready — tray open" (sense key 2)
        // counts as success: the drive is functional, just needs disc reinserted.
        self.unlock_tray();
        let eject = [SCSI_START_STOP_UNIT, 0x00, 0x00, 0x00, 0x02, 0x00];
        let _ =
            self.scsi
                .as_mut()
                .execute(&eject, crate::scsi::DataDirection::None, &mut buf, 30_000);
        std::thread::sleep(std::time::Duration::from_millis(2000));

        match self
            .scsi
            .as_mut()
            .execute(&tur, crate::scsi::DataDirection::None, &mut buf, 5_000)
        {
            Ok(_) => return Ok(()),
            Err(Error::ScsiError { sense_key: 2, .. }) => return Ok(()), // tray open = valid
            _ => {}
        }

        // 3. If still stuck and we have a profile, try re-init
        if self.driver.is_some() {
            self.init()?;
            std::thread::sleep(std::time::Duration::from_millis(1000));
            match self.scsi.as_mut().execute(
                &tur,
                crate::scsi::DataDirection::None,
                &mut buf,
                5_000,
            ) {
                Ok(_) => return Ok(()),
                Err(Error::ScsiError { sense_key: 2, .. }) => return Ok(()),
                _ => {}
            }
        }

        Err(Error::DeviceResetFailed {
            path: self.device_path.clone(),
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
            crate::scsi::SCSI_GET_CONFIGURATION,
            0x02,
            (feature_code >> 8) as u8,
            feature_code as u8,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
            0x00,
        ];
        let mut buf = vec![0u8; 256];
        let r = self
            .scsi
            .as_mut()
            .execute(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        if r.bytes_transferred > 8 {
            Some(buf[8..r.bytes_transferred].to_vec())
        } else {
            None
        }
    }

    /// Read REPORT KEY RPC state (region playback control).
    pub fn report_key_rpc_state(&mut self) -> Option<Vec<u8>> {
        let cdb = [
            SCSI_REPORT_KEY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x08,
            0x08,
            0x00,
        ];
        let mut buf = vec![0u8; 8];
        let r = self
            .scsi
            .as_mut()
            .execute(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        if r.bytes_transferred > 0 {
            Some(buf[..r.bytes_transferred].to_vec())
        } else {
            None
        }
    }

    /// Read MODE SENSE page data.
    pub fn mode_sense_page(&mut self, page: u8) -> Option<Vec<u8>> {
        let cdb = [
            SCSI_MODE_SENSE,
            0x00,
            page,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0xFC,
            0x00,
        ];
        let mut buf = vec![0u8; 252];
        let r = self
            .scsi
            .as_mut()
            .execute(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        if r.bytes_transferred > 0 {
            Some(buf[..r.bytes_transferred].to_vec())
        } else {
            None
        }
    }

    /// Read vendor-specific READ BUFFER data.
    pub fn read_buffer(&mut self, mode: u8, buffer_id: u8, length: u16) -> Option<Vec<u8>> {
        let cdb = crate::scsi::build_read_buffer(mode, buffer_id, 0, length as u32);
        let mut buf = vec![0u8; length as usize];
        let r = self
            .scsi
            .as_mut()
            .execute(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        if r.bytes_transferred > 0 {
            Some(buf[..r.bytes_transferred].to_vec())
        } else {
            None
        }
    }

    pub fn is_ready(&self) -> bool {
        match self.driver {
            Some(ref d) => d.is_ready(),
            None => false,
        }
    }

    /// Read sectors from the disc. Single-shot — no inline retries, no
    /// SCSI reset.
    ///
    /// `recovery=true` bumps the per-CDB timeout to 30 s for the
    /// `Disc::patch` pass; `recovery=false` uses 1.5 s for `Disc::copy`'s
    /// fast skip-forward sweep. On any failure returns `Err(DiscRead)`
    /// immediately. The orchestration layer (`Disc::patch`'s outer loop
    /// for the patch pass, `DiscStream`'s adaptive batch halving for the
    /// stream path) handles retries.
    ///
    /// Inline retry phases (5× gentle + reset+reopen + 5× more) were
    /// removed in 0.13.6. Per
    /// `freemkv-private/postmortems/2026-04-25-stop-wedge-and-zero-kbs.md`,
    /// the inline reset on the LG BU40N (Initio bridge) wedged drive
    /// firmware without ever recovering a sector. The remaining recovery
    /// layers (Disc::patch multi-pass, DiscStream batch halving) do not
    /// touch the wedge-prone reset path.
    pub fn read(&mut self, lba: u32, count: u16, buf: &mut [u8], recovery: bool) -> Result<usize> {
        let timeout_ms = if recovery { 30_000 } else { 1_500 };
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

        match self.checked_exec(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            buf,
            timeout_ms,
        ) {
            Ok(result) => Ok(result.bytes_transferred),
            Err(Error::Halted) => Err(Error::Halted),
            Err(_) => Err(Error::DiscRead { sector: lba as u64 }),
        }
    }

    /// Read the disc capacity in sectors (2048 bytes each).
    pub fn read_capacity(&mut self) -> Result<u32> {
        let cdb = [
            crate::scsi::SCSI_READ_CAPACITY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut buf = [0u8; 8];
        self.scsi.as_mut().execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        let last_lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Ok(last_lba + 1)
    }

    pub fn set_speed(&mut self, speed_kbs: u16) {
        let cdb = crate::scsi::build_set_cd_speed(speed_kbs);
        let mut dummy = [0u8; 0];
        let _ = self.scsi_execute(&cdb, crate::scsi::DataDirection::None, &mut dummy, 5_000);
    }

    /// Lock the tray so the disc cannot be ejected during a rip.
    pub fn lock_tray(&mut self) {
        let prevent = [
            SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
        ];
        let mut buf = [0u8; 0];
        let _ =
            self.scsi
                .as_mut()
                .execute(&prevent, crate::scsi::DataDirection::None, &mut buf, 5_000);
    }

    /// Unlock the tray so the user can manually eject the disc.
    pub fn unlock_tray(&mut self) {
        let allow = [
            SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut buf = [0u8; 0];
        let _ =
            self.scsi
                .as_mut()
                .execute(&allow, crate::scsi::DataDirection::None, &mut buf, 5_000);
    }

    /// Eject the disc tray. Unlocks first, then ejects.
    pub fn eject(&mut self) -> Result<()> {
        self.unlock_tray();
        let eject_cdb = [SCSI_START_STOP_UNIT, 0, 0, 0, 0x02, 0];
        let mut buf = [0u8; 0];
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

impl Drop for Drive {
    fn drop(&mut self) {
        self.cleanup();
        // SgIoTransport::drop() runs next, calling libc::close(fd)
    }
}

impl SectorReader for Drive {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.read(lba, count, buf, recovery)
    }
}

/// Find all optical drives connected to this system.
/// Returns opened Drive objects ready for use.
pub fn find_drives() -> Vec<Drive> {
    discover_drives()
        .into_iter()
        .filter_map(|(path, _)| Drive::open(std::path::Path::new(&path)).ok())
        .collect()
}

/// Find the first optical drive.
/// Returns an opened Drive ready for use.
pub fn find_drive() -> Option<Drive> {
    find_drives().into_iter().next()
}

/// Halt-aware sleep primitive — wakes within ~100 ms of `halt` flipping
/// to true. Kept for the unit tests that cover the slicing behaviour;
/// production code paths no longer sleep on the recovery hot path
/// (recovery loop removed in 0.13.6).
#[cfg(test)]
fn sleep_until_halted(halt: &AtomicBool, total: std::time::Duration) -> Result<()> {
    const SLICE: std::time::Duration = std::time::Duration::from_millis(100);
    let deadline = std::time::Instant::now() + total;
    loop {
        if halt.load(Ordering::Relaxed) {
            return Err(Error::Halted);
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return Ok(());
        }
        let remaining = deadline - now;
        std::thread::sleep(remaining.min(SLICE));
    }
}

/// Internal: discover drive paths + IDs without opening full Drive objects.
fn discover_drives() -> Vec<(String, DriveId)> {
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

/// Resolve a device path to its raw SCSI device, with optional warning message.
#[allow(dead_code)]
pub(crate) fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
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
        profile::Platform::Renesas => Err(Error::PlatformNotImplemented {
            platform: "renesas".to_string(),
        }),
    }
}

#[cfg(test)]
mod halt_tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn sleep_until_halted_completes_when_not_halted() {
        let flag = AtomicBool::new(false);
        let t0 = Instant::now();
        let r = sleep_until_halted(&flag, Duration::from_millis(150));
        assert!(r.is_ok());
        assert!(t0.elapsed() >= Duration::from_millis(140));
    }

    #[test]
    fn sleep_until_halted_returns_immediately_if_preflagged() {
        let flag = AtomicBool::new(true);
        let t0 = Instant::now();
        let r = sleep_until_halted(&flag, Duration::from_secs(10));
        assert!(matches!(r, Err(Error::Halted)));
        // Must wake within one slice (100 ms) — the whole point of the
        // primitive is that a 30 s sleep doesn't block Stop.
        assert!(t0.elapsed() < Duration::from_millis(200));
    }

    #[test]
    fn sleep_until_halted_wakes_mid_sleep() {
        let flag = Arc::new(AtomicBool::new(false));
        let f2 = flag.clone();
        let t0 = Instant::now();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            f2.store(true, Ordering::Relaxed);
        });
        let r = sleep_until_halted(&flag, Duration::from_secs(10));
        assert!(matches!(r, Err(Error::Halted)));
        let waited = t0.elapsed();
        // Flag flipped at ~150 ms; we wake within one 100 ms slice → <300 ms.
        assert!(waited < Duration::from_millis(350), "waited {waited:?}");
        assert!(waited >= Duration::from_millis(140), "waited {waited:?}");
    }

    #[test]
    fn sleep_until_halted_zero_duration_is_noop_when_not_halted() {
        let flag = AtomicBool::new(false);
        let r = sleep_until_halted(&flag, Duration::ZERO);
        assert!(r.is_ok());
    }
}
