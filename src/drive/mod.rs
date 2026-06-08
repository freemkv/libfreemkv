//! Drive session — open, identify, and read from optical drives.
//!
//! A `Drive` is opened from a device path, identifies itself via INQUIRY,
//! optionally unlocks/initializes via a platform driver, and reads sectors.
//! `probe_disc()` primes the firmware's per-region speed table.

pub(crate) fn extract_scsi_context(e: &Error) -> (u8, Option<crate::scsi::ScsiSense>) {
    match e {
        Error::ScsiError { status, sense, .. } => (*status, *sense),
        Error::DiscRead { status, sense, .. } => (status.unwrap_or(0), *sense),
        _ => (0, None),
    }
}

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
use crate::event::Event;
use crate::identity::DriveId;
use crate::platform::PlatformDriver;
use crate::platform::mt1959::Mt1959;
use crate::profile::{self, DriveProfile};
use crate::scsi::ScsiTransport;
use crate::sector::SectorSource;
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
    /// Linux only: raw fd for the corresponding block device (`/dev/sr*`)
    /// used as a recovery fallback when SCSI READ via `/dev/sg*` returns
    /// an error. The kernel `sr_mod` driver auto-retries failed reads
    /// (~5× per command) — historically the reason `dd if=/dev/sr0`
    /// recovers ~50% of bad sectors that single-shot `SG_IO` READ
    /// misses on the same drive. `None` when the block device couldn't
    /// be resolved or opened (no fallback in that case; SCSI read
    /// errors propagate as before).
    #[cfg(target_os = "linux")]
    block_dev_fd: Option<std::os::unix::io::RawFd>,
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

        #[cfg(target_os = "linux")]
        let block_dev_fd = open_block_device_for_sg(device);

        Ok(Drive {
            scsi: transport,
            driver,
            platform,
            profile,
            drive_id,
            device_path: device.to_string_lossy().to_string(),
            halt: Arc::new(AtomicBool::new(false)),
            event_fn: None,
            #[cfg(target_os = "linux")]
            block_dev_fd,
        })
    }

    /// Test-only constructor: build a `Drive` over an arbitrary
    /// [`ScsiTransport`] (no profile, no platform driver, no block-device
    /// fallback) so command-builder/response-parser logic can be exercised
    /// against a scripted mock transport.
    #[cfg(test)]
    fn from_transport_for_test(scsi: Box<dyn ScsiTransport>) -> Self {
        Drive {
            scsi,
            driver: None,
            profile: None,
            platform: None,
            drive_id: DriveId {
                vendor_id: String::new(),
                product_id: String::new(),
                product_revision: String::new(),
                vendor_specific: String::new(),
                firmware_date: String::new(),
                serial_number: String::new(),
                raw_inquiry: Vec::new(),
                raw_gc_010c: Vec::new(),
            },
            device_path: "test".to_string(),
            halt: Arc::new(AtomicBool::new(false)),
            event_fn: None,
            #[cfg(target_os = "linux")]
            block_dev_fd: None,
        }
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

    /// Close the drive cleanly. Unlocks the tray and closes the fd.
    /// Also runs automatically on Drop as a safety net.
    pub fn close(self) {
        // cleanup() runs here via Drop
    }

    /// Shared cleanup — called by Drop (and thus by close).
    fn cleanup(&mut self) {
        self.unlock_tray();
    }

    /// Whether this drive has a known profile (unlock parameters available).
    pub fn has_profile(&self) -> bool {
        self.profile.is_some()
    }

    /// Borrow the matched drive profile, if any. Used by callers that
    /// need to issue per-drive OEM CDB templates (e.g. the OEM VID
    /// retrieval path in `disc::encrypt`).
    pub fn drive_profile(&self) -> Option<&DriveProfile> {
        self.profile.as_ref()
    }

    /// Access the SCSI transport for direct commands (used by CSS/AACS auth).
    pub fn scsi_mut(&mut self) -> &mut dyn ScsiTransport {
        self.scsi.as_mut()
    }

    pub fn wait_ready(&mut self) -> Result<()> {
        let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];

        for _ in 0..60 {
            let mut buf = [0u8; 0];
            if self
                .scsi
                .as_mut()
                .execute(&tur, crate::scsi::DataDirection::None, &mut buf, 5_000)
                .is_ok()
            {
                return Ok(());
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
                    0x01 => DriveStatus::TrayOpen,    // tray open, no media
                    0x02 => DriveStatus::DiscPresent, // tray closed, disc present
                    // 0x03 = tray-open bit AND media-present bit both set:
                    // a contradictory/transient state. Don't report it as
                    // ready — autorip must not start a rip on a drive that
                    // is still settling. Treat as tray-open.
                    0x03 => DriveStatus::TrayOpen,
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
                    Err(ref e)
                        if e.scsi_sense()
                            .is_some_and(|s| s.is_not_ready() || s.is_unit_attention()) =>
                    {
                        DriveStatus::NotReady
                    }
                    _ => DriveStatus::Unknown,
                }
            }
        }
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
        // Clamp the transport-reported count to the buffer length: a
        // misbehaving driver/bridge could report more bytes than the
        // buffer holds, which would panic the slice.
        let end = r.bytes_transferred.min(buf.len());
        if end > 8 {
            Some(buf[8..end].to_vec())
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
        let end = r.bytes_transferred.min(buf.len());
        if end > 0 {
            Some(buf[..end].to_vec())
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
        let end = r.bytes_transferred.min(buf.len());
        if end > 0 {
            Some(buf[..end].to_vec())
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
        let end = r.bytes_transferred.min(buf.len());
        if end > 0 {
            Some(buf[..end].to_vec())
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

    /// True if the drive is currently in the extended-access state.
    ///
    /// Detected by the platform driver during `init()` from the unlock
    /// response's mode markers. When true:
    ///   - SCSI READ_10 returns plaintext sectors (no AACS bus
    ///     encryption applied)
    ///   - VID retrieval works via the per-drive OEM CDB in
    ///     [`DriveProfile`] without the cert-based AACS handshake
    ///   - Disc-side Host Revocation List enforcement is effectively
    ///     bypassed by the alternate data path
    ///
    /// AACS layer code branches on this: if true, issue the OEM
    /// `read_vid_cdb` to retrieve VID directly; if false, fall back
    /// to the cert-based mutual-auth handshake.
    pub fn is_unlocked(&self) -> bool {
        match self.driver {
            Some(ref d) => d.is_unlocked(),
            None => false,
        }
    }

    /// Read sectors from the disc. Single-shot — no inline retries, no
    /// SCSI reset.
    ///
    /// `recovery=true` uses [`crate::scsi::READ_RECOVERY_TIMEOUT_MS`] (60 s,
    /// matches sg_dd) for the `Disc::patch` pass; `recovery=false` uses
    /// [`crate::scsi::READ_TIMEOUT_MS`] (10 s) for `Disc::copy`'s fast
    /// skip-forward sweep. Both budgets are generous enough that the drive
    /// can finish ECC recovery on a marginal sector — pre-0.13.21 this was
    /// 1.5 s on the fast path which forced the kernel mid-layer to time
    /// out and escalate while we waited anyway. On any failure returns
    /// `Err(DiscRead)` immediately; orchestration (`Disc::patch` multi-pass,
    /// `DiscStream` adaptive batch halving) handles retry policy.
    ///
    /// Inline retry phases (5× gentle + reset+reopen + 5× more) were
    /// removed in 0.13.6: on some USB-SATA bridges the inline reset wedged
    /// drive firmware without ever recovering a sector. The remaining
    /// recovery layers (Disc::patch multi-pass, DiscStream batch halving)
    /// do not touch the wedge-prone reset path.
    pub fn read(&mut self, lba: u32, count: u16, buf: &mut [u8], recovery: bool) -> Result<usize> {
        let timeout_ms = if recovery {
            crate::scsi::READ_RECOVERY_TIMEOUT_MS
        } else {
            crate::scsi::READ_TIMEOUT_MS
        };
        tracing::debug!(
            target: "freemkv::drive",
            lba,
            count,
            recovery,
            timeout_ms,
            "Drive::read enter"
        );
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
            Err(e) => {
                let (status, sense) = extract_scsi_context(&e);
                tracing::warn!(
                    target: "freemkv::drive",
                    lba,
                    count,
                    inner_error = %e,
                    scsi_status = status,
                    "Drive::read checked_exec failed"
                );

                // /dev/sr0 pread fallback (Linux only). The kernel
                // sr_mod driver auto-retries failed reads (~5× per
                // command). Empirically (BU40N + a UHD disc,
                // 2026-05-08) dd via /dev/sr0 recovers ~50% of bad
                // sectors that a single-shot SG_IO READ misses.
                #[cfg(target_os = "linux")]
                if recovery {
                    if let Some(fd) = self.block_dev_fd {
                        let len = count as usize * 2048;
                        if buf.len() >= len {
                            let offset = lba as i64 * 2048;
                            // Drop kernel cache for this region so we get
                            // a fresh device read, not stale page-cache
                            // data from a prior successful neighbour read.
                            let _ = unsafe {
                                libc::posix_fadvise(
                                    fd,
                                    offset,
                                    len as i64,
                                    libc::POSIX_FADV_DONTNEED,
                                )
                            };
                            let n = unsafe {
                                libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, len, offset)
                            };
                            if n == len as isize {
                                tracing::info!(
                                    target: "freemkv::drive",
                                    lba,
                                    count,
                                    bytes = len,
                                    "Drive::read recovered via /dev/sr0 pread fallback"
                                );
                                return Ok(len);
                            }
                            tracing::debug!(
                                target: "freemkv::drive",
                                lba,
                                count,
                                pread_ret = n as i64,
                                errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                                "/dev/sr0 pread fallback also failed"
                            );
                        }
                    }
                }

                Err(Error::DiscRead {
                    sector: lba as u64,
                    status: Some(status),
                    sense,
                })
            }
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
        let result = self.scsi.as_mut().execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        decode_read_capacity(&buf, result.bytes_transferred)
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
        #[cfg(target_os = "linux")]
        if let Some(fd) = self.block_dev_fd.take() {
            unsafe { libc::close(fd) };
        }
    }
}

/// Resolve a `/dev/sg*` path to the corresponding `/dev/sr*` block
/// device by walking sysfs, then open it for read (no `O_DIRECT` —
/// `posix_fadvise(POSIX_FADV_DONTNEED)` flushes the cache before each
/// pread, which avoids buffer-alignment requirements while still
/// forcing fresh device reads).
///
/// Returns `None` on any error (sysfs not present, no matching block
/// device, open failed). Callers treat that as "no fallback available"
/// and propagate the original SCSI READ error.
#[cfg(target_os = "linux")]
fn open_block_device_for_sg(sg_path: &Path) -> Option<std::os::unix::io::RawFd> {
    let basename = sg_path.file_name()?.to_str()?;
    if !basename.starts_with("sg") {
        return None;
    }
    let sysfs_dir = format!("/sys/class/scsi_generic/{}/device/block", basename);
    let entries = std::fs::read_dir(&sysfs_dir).ok()?;
    let block_name = entries
        .flatten()
        .find_map(|e| e.file_name().into_string().ok())?;
    let block_path = format!("/dev/{}", block_name);

    let mut bytes = block_path.as_bytes().to_vec();
    bytes.push(0);
    let fd = unsafe {
        libc::open(
            bytes.as_ptr() as *const libc::c_char,
            libc::O_RDONLY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        tracing::debug!(
            target: "freemkv::drive",
            sg = basename,
            block_path,
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "Failed to open block device for fallback; sr0 fallback disabled"
        );
        None
    } else {
        tracing::info!(
            target: "freemkv::drive",
            sg = basename,
            block_path,
            fd,
            "Opened /dev/sr* as recovery fallback for failed SCSI reads"
        );
        Some(fd)
    }
}

impl SectorSource for Drive {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.read(lba, count, buf, recovery)
    }

    fn set_speed(&mut self, kbs: u16) {
        Drive::set_speed(self, kbs);
    }
}

/// Find the first optical drive on this system and open it.
///
/// For just listing drives without opening (e.g. UI sidebar), use
/// `scsi::list_drives()` — that returns `DriveInfo` (path + identity)
/// without the cost of running every drive's profile + identity probe.
pub fn find_drive() -> Option<Drive> {
    discover_drives()
        .into_iter()
        .find_map(|(path, _)| Drive::open(std::path::Path::new(&path)).ok())
}

/// Decode a READ CAPACITY (10) response into a sector count.
///
/// A short transfer (`bytes_transferred < 4`, which would leave the high
/// bytes zero-initialised and decode to a bogus 1-sector disc) is rejected
/// as [`Error::DiscCapacityMalformed`]. The `0xFFFF_FFFF` "capacity exceeds
/// 32-bit" sentinel, whose `last_lba + 1` overflows `u32`, is reported as the
/// distinct [`Error::DiscCapacityOverflow`] so callers can tell an unusable
/// response apart from an over-large disc.
fn decode_read_capacity(buf: &[u8; 8], bytes_transferred: usize) -> Result<u32> {
    if bytes_transferred < 4 {
        return Err(Error::DiscCapacityMalformed);
    }
    let last_lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    last_lba.checked_add(1).ok_or(Error::DiscCapacityOverflow)
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

/// Structured outcome of [`resolve_device`] — a machine-readable signal
/// (no English prose) the application layer can render however it likes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceResolution {
    /// Path resolved directly to a SCSI-generic device; no substitution.
    Direct,
    /// A `/dev/sr*` block path was substituted with the matching
    /// `/dev/sg*` SCSI-generic device for raw access (Linux only).
    SrToSg,
    /// A `/dev/sr*` block path was given but no matching `/dev/sg*`
    /// device could be found; the original path is returned (Linux only).
    SrNoSgMatch,
}

/// Resolve a device path to its raw SCSI device. Returns the resolved
/// path plus a structured [`DeviceResolution`] signal describing whether
/// any substitution happened; the application layer maps that to UX text.
#[allow(dead_code)]
pub(crate) fn resolve_device(path: &str) -> Result<(String, DeviceResolution)> {
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

    #[test]
    fn read_capacity_short_transfer_is_rejected() {
        // bytes_transferred < 4 must NOT decode to capacity=1 from
        // zero-init bytes.
        let buf = [0u8; 8];
        assert!(matches!(
            decode_read_capacity(&buf, 0),
            Err(Error::DiscCapacityMalformed)
        ));
        assert!(matches!(
            decode_read_capacity(&buf, 3),
            Err(Error::DiscCapacityMalformed)
        ));
    }

    #[test]
    fn read_capacity_full_transfer_decodes_last_lba_plus_one() {
        // last_lba = 0x00012344 -> capacity 0x00012345.
        let buf = [0x00, 0x01, 0x23, 0x44, 0, 0, 0, 0];
        assert_eq!(decode_read_capacity(&buf, 8).unwrap(), 0x0001_2345);
    }

    #[test]
    fn read_capacity_overflow_is_rejected() {
        // last_lba = u32::MAX (the "capacity exceeds 32-bit" sentinel) -> +1
        // overflows; reported as the distinct DiscCapacityOverflow, not the
        // short-transfer DiscCapacityMalformed.
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];
        assert!(matches!(
            decode_read_capacity(&buf, 8),
            Err(Error::DiscCapacityOverflow)
        ));
    }
}

#[cfg(test)]
mod command_tests {
    use super::*;
    use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};

    /// Mock transport: returns a fixed data payload (copied into the
    /// caller's buffer, truncated to fit) on every `execute()`.
    struct FixedTransport {
        payload: Vec<u8>,
    }

    impl ScsiTransport for FixedTransport {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            let n = self.payload.len().min(data.len());
            data[..n].copy_from_slice(&self.payload[..n]);
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: n,
                sense: [0u8; 32],
            })
        }
    }

    fn drive_with(payload: Vec<u8>) -> Drive {
        Drive::from_transport_for_test(Box::new(FixedTransport { payload }))
    }

    #[test]
    fn read_capacity_normal_adds_one() {
        // last_lba = 0x0000_0063 (99) → capacity 100 sectors.
        let mut d = drive_with(vec![0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x08, 0x00]);
        assert_eq!(d.read_capacity().unwrap(), 100);
    }

    #[test]
    fn read_capacity_sentinel_does_not_overflow() {
        // last_lba = 0xFFFF_FFFF is the "capacity exceeds 32-bit" sentinel;
        // +1 would overflow. Must surface DiscCapacityOverflow, not panic
        // (debug) or wrap to 0 (release).
        let mut d = drive_with(vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x08, 0x00]);
        assert!(matches!(
            d.read_capacity(),
            Err(Error::DiscCapacityOverflow)
        ));
    }

    #[test]
    fn drive_status_tray_open_and_media_present_is_not_ready_to_rip() {
        // GET EVENT STATUS reply: byte 5 (media_status) low bits = 0b11
        // (tray-open AND media-present, contradictory). Must NOT report
        // DiscPresent. Buffer is 8 bytes; bytes_transferred >= 6.
        let mut buf = vec![0u8; 8];
        buf[5] = 0x03;
        let mut d = drive_with(buf);
        assert_eq!(d.drive_status(), DriveStatus::TrayOpen);
    }

    #[test]
    fn drive_status_disc_present_maps_correctly() {
        let mut buf = vec![0u8; 8];
        buf[5] = 0x02; // media present, tray closed
        let mut d = drive_with(buf);
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
    }
}
