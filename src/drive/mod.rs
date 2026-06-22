//! Drive session — open, identify, and read from optical drives.
//!
//! A `Drive` is opened from a device path, identifies itself via INQUIRY,
//! optionally unlocks/initializes via a registered [`crate::unlock::Unlocker`],
//! and reads sectors.

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
    /// Name of the [`crate::unlock::Unlocker`] that handled this drive at
    /// `init()`, if any matched. `None` means no unlocker matched and the
    /// drive runs in stock mode (host-cert AACS handshake carries discs).
    unlocker_name: Option<String>,
    /// True once `init()` has run (whether or not an unlocker matched).
    init_ran: bool,
    /// Lazily-computed registry-match name for `platform_name()`'s `&str`
    /// return before `init()` has run.
    matched_name_cache: std::sync::OnceLock<String>,
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
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "open", device = %device.display(), "begin");
        let mut transport = crate::scsi::open(device)?;
        let drive_id = DriveId::from_drive(transport.as_mut())?;
        tracing::info!(
            target: "freemkv::drive",
            phase = "open",
            device = %device.display(),
            vendor = %drive_id.vendor_id.trim(),
            product = %drive_id.product_id.trim(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end"
        );

        #[cfg(target_os = "linux")]
        let block_dev_fd = open_block_device_for_sg(device);

        Ok(Drive {
            scsi: transport,
            unlocker_name: None,
            init_ran: false,
            matched_name_cache: std::sync::OnceLock::new(),
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
            unlocker_name: None,
            init_ran: false,
            matched_name_cache: std::sync::OnceLock::new(),
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

    /// Whether a registered unlocker matches this drive (i.e. it can be
    /// firmware-unlocked). Queried against the unlock registry by identity;
    /// does not require `init()` to have run.
    pub fn has_profile(&self) -> bool {
        crate::unlock::matching_name(&self.drive_id).is_some()
    }

    /// Access the SCSI transport for direct commands (used by CSS/AACS auth).
    pub fn scsi_mut(&mut self) -> &mut dyn ScsiTransport {
        self.scsi.as_mut()
    }

    pub fn wait_ready(&mut self) -> Result<()> {
        let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "wait_ready", "begin");

        // The poll can take up to 30s (60 × 500ms). Heartbeat it so a slow
        // spin-up is visible as steady beats rather than a silent stall.
        let mut hb = crate::progress::Heartbeat::new("wait_ready");
        for attempt in 0..60u64 {
            hb.tick(attempt, 60);
            let mut buf = [0u8; 0];
            if self
                .scsi
                .as_mut()
                .execute(&tur, crate::scsi::DataDirection::None, &mut buf, 5_000)
                .is_ok()
            {
                tracing::info!(
                    target: "freemkv::drive",
                    phase = "wait_ready",
                    attempts = attempt + 1,
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "end"
                );
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        tracing::warn!(
            target: "freemkv::drive",
            phase = "wait_ready",
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "device never became ready"
        );
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

    /// Name of the unlocker handling this drive. After `init()` this is the
    /// unlocker that ran; before `init()` it reflects the registry match by
    /// identity. `"Unknown"` when no unlocker matches.
    pub fn platform_name(&self) -> &str {
        if let Some(ref n) = self.unlocker_name {
            return n;
        }
        // Cache the registry match so we can hand out a `&str` borrow.
        self.matched_name_cache.get_or_init(|| {
            crate::unlock::matching_name(&self.drive_id).unwrap_or_else(|| "Unknown".to_string())
        })
    }

    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    /// Current mounted-disc profile from the GET CONFIGURATION header
    /// (Current Profile, bytes 6-7). DVD family is `0x0010..=0x001F`, BD
    /// family `0x0040..=0x0043`. This is a stock MMC command — it works
    /// before (and without) any firmware unlock. `None` if unreadable.
    fn current_profile(&mut self) -> Option<u16> {
        let cdb = [
            crate::scsi::SCSI_GET_CONFIGURATION,
            0x00, // RT=0: header carries the Current Profile
            0x00,
            0x00, // starting feature 0
            0x00,
            0x00,
            0x00,
            0x00,
            0x08, // allocation length = 8 (header only)
            0x00,
        ];
        let mut buf = [0u8; 8];
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
        if r.bytes_transferred >= 8 {
            Some(((buf[6] as u16) << 8) | buf[7] as u16)
        } else {
            None
        }
    }

    /// True when the mounted disc is a DVD (profile family `0x0010..=0x001F`).
    fn disc_is_dvd(&mut self) -> bool {
        matches!(self.current_profile(), Some(p) if (0x0010..=0x001F).contains(&p))
    }

    /// Initialize drive — unlock + firmware upload.
    /// Optional. Adds features: removes riplock, enables UHD reads, speed control.
    ///
    /// The LibreDrive/OEM firmware unlock is required for BD/UHD (AACS) reads,
    /// but it puts the drive in an extended-access state where stock CSS
    /// authentication no longer works — so a CSS-protected DVD can't be read.
    /// For a DVD we therefore SKIP the unlock and run the drive in its normal
    /// stock mode; the DVD path then issues standard CSS commands, which a stock
    /// drive honors. BD/UHD and any non-DVD/unknown media keep today's behavior.
    pub fn init(&mut self) -> Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "init", "begin");
        if self.disc_is_dvd() {
            tracing::info!(target: "freemkv::drive", phase = "init", dvd = true, elapsed_ms = t0.elapsed().as_millis() as u64, "end (stock-mode DVD, no unlock)");
            self.init_ran = true;
            return Ok(());
        }
        // Walk the unlock registry: the first unlocker whose identity
        // matches runs; none matching leaves the drive in stock mode so the
        // host-cert AACS handshake (the OEM route) carries the disc.
        let r = crate::unlock::route_unlock(self.scsi.as_mut(), &self.drive_id);
        self.init_ran = true;
        let r = match r {
            Ok(Some(name)) => {
                self.unlocker_name = Some(name);
                // The matched unlocker may also be able to raise the drive to
                // its maximum read speed. Best-effort: a failure here must NOT
                // fail the rip — a slow drive still rips. Log and continue.
                if let Err(e) =
                    crate::unlock::unlocker_set_max_read_speed(self.scsi.as_mut(), &self.drive_id)
                {
                    tracing::warn!(
                        target: "freemkv::drive",
                        phase = "init",
                        error = ?e,
                        "unlocker set_max_read_speed failed; continuing at current speed"
                    );
                }
                Ok(())
            }
            // No unlocker matched: not an error — fall through to OEM route.
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };
        tracing::info!(
            target: "freemkv::drive",
            phase = "init",
            ok = r.is_ok(),
            unlocker = self.unlocker_name.as_deref().unwrap_or("none"),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end"
        );
        r
    }

    /// Probe disc surface so the drive firmware learns optimal read speeds
    /// per region. After this the host reads at max speed and the drive
    /// manages zones internally.
    pub fn probe_disc(&mut self) -> Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "probe_disc", "begin");
        // A DVD runs in stock mode (see `init`); skip the OEM/firmware-path
        // disc calibration, which only applies to the unlocked BD/UHD drive.
        if self.disc_is_dvd() {
            tracing::info!(target: "freemkv::drive", phase = "probe_disc", dvd = true, elapsed_ms = t0.elapsed().as_millis() as u64, "end (stock-mode DVD, no calibration)");
            return Ok(());
        }
        // Disc-speed calibration is firmware-specific and now lives inside
        // the unlocker's `unlock()` (run at `init()`). Nothing to do here.
        tracing::info!(
            target: "freemkv::drive",
            phase = "probe_disc",
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end (calibration handled by unlocker at init)"
        );
        Ok(())
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
        // Ready once init() has run and an unlocker handled the drive.
        self.init_ran && self.unlocker_name.is_some()
    }

    /// Whether libfreemkv should take the OEM extended-access read path.
    ///
    /// Whether a registered [`crate::unlock::Unlocker`] matches this drive.
    ///
    /// An unlocker unlocks *drive functionality* — firmware unlock, OEM VID
    /// retrieval, and other vendor capabilities. When one matches, libfreemkv
    /// routes both `unlock` and OEM VID through it (VID via the OEM path is
    /// decoupled from the host cert + HRL). This mirrors [`Self::has_profile`]
    /// — the honest signal is "a registered unlocker claims this drive" —
    /// rather than the old const `false`.
    pub fn is_unlocked(&self) -> bool {
        crate::unlock::matching_name(&self.drive_id).is_some()
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

        // Cap each CDB to the transport's max data-in transfer. A single
        // READ larger than the adapter limit fails outright on some
        // backends (notably Windows SPTI, where a 16 MiB read exceeds the
        // adapter MaximumTransferLength → DeviceIoControl fails → we'd
        // mis-read it as a transport failure and spam tiny-read fallbacks).
        // For the common small read (count <= max_sectors) this is a single
        // read_one call with no behavior change.
        let max_sectors = (self.scsi.max_transfer_bytes() / 2048).max(1) as u32;
        if count as u32 <= max_sectors {
            return self.read_one(lba, count, buf, timeout_ms, recovery);
        }

        // Large read: split into chunks of at most `max_sectors` sectors,
        // each a self-contained READ(10) with the same validation. Any
        // chunk error reports that chunk's LBA (more precise than the whole
        // request's base LBA).
        let mut done: u32 = 0;
        let mut total: usize = 0;
        let count = count as u32;
        while done < count {
            let chunk = (count - done).min(max_sectors);
            let cur_lba = lba + done;
            let byte_off = done as usize * 2048;
            let byte_len = chunk as usize * 2048;
            let slice = &mut buf[byte_off..byte_off + byte_len];
            let n = self.read_one(cur_lba, chunk as u16, slice, timeout_ms, recovery)?;
            total += n;
            done += chunk;
        }
        Ok(total)
    }

    /// Issue a single READ(10) for up to `count` sectors at `lba` into
    /// `buf`, with the recovery-timeout already resolved by the caller.
    /// This is the byte-identical single-shot read body that `read` calls
    /// (once for small reads, in a loop for reads larger than the
    /// transport's max transfer). On failure returns `Err(DiscRead)` with
    /// `sector = lba` (the failing chunk's LBA) and the preserved SCSI
    /// status/sense; a short transfer is treated as a failed read.
    fn read_one(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        timeout_ms: u32,
        // `recovery` only gates the Linux /dev/sr0 pread fallback below; on
        // other platforms it is intentionally unused.
        #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] recovery: bool,
    ) -> Result<usize> {
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
            Ok(result) if result.bytes_transferred == count as usize * 2048 => {
                Ok(result.bytes_transferred)
            }
            // A READ(10) that completes with GOOD status but a residual
            // underrun (bytes_transferred < requested) is a SHORT transfer:
            // the tail of `buf` still holds stale bytes from a prior read.
            // Committing those as recovered/Good is silent data corruption, so
            // treat a short transfer as a failed read — the caller marks the
            // range NonTrimmed and retries (a loud miss, never a silent commit).
            // The sector/file path enforces the same invariant in
            // sector/prefetched.rs; this is the live-drive counterpart.
            Ok(_) => Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            }),
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

/// Find an optical drive on this system and open it, **preferring a drive
/// that currently has media**.
///
/// On a multi-drive system (common on Windows, where an empty/not-ready
/// drive can enumerate first) returning the first drive blindly can pick a
/// drive with no disc, dooming the operation. So this opens each candidate
/// in enumeration order, queries [`Drive::drive_status`] (GET EVENT STATUS,
/// which works regardless of firmware state), and returns the first drive
/// reporting [`DriveStatus::DiscPresent`].
///
/// If no drive reports a disc — or `drive_status()` is unavailable/returns
/// `Unknown` everywhere (single-drive or quirky bridges) — it falls back to
/// the first drive that opened, preserving the historical behavior so those
/// setups don't regress.
///
/// For just listing drives without opening (e.g. UI sidebar), use
/// `scsi::list_drives()` — that returns `DriveInfo` (path + identity)
/// without the cost of running every drive's profile + identity probe.
pub fn find_drive() -> Option<Drive> {
    select_drive_with_media(
        discover_drives()
            .into_iter()
            .filter_map(|(path, _)| Drive::open(std::path::Path::new(&path)).ok()),
    )
}

/// Pick a drive from an iterator of opened drives, preferring one whose
/// [`Drive::drive_status`] reports [`DriveStatus::DiscPresent`]. Falls back
/// to the first drive yielded if none report a disc. Split out from
/// [`find_drive`] so the selection policy is unit-testable against fake
/// drives without touching real hardware.
fn select_drive_with_media(drives: impl Iterator<Item = Drive>) -> Option<Drive> {
    let mut fallback: Option<Drive> = None;
    for mut drive in drives {
        if drive.drive_status() == DriveStatus::DiscPresent {
            return Some(drive);
        }
        // Remember the first drive that opened as the no-media fallback so
        // single-drive / status-unavailable setups still get a drive.
        if fallback.is_none() {
            fallback = Some(drive);
        }
    }
    fallback
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

    /// `disc_is_dvd()` must match the DVD profile family (0x0010..=0x001F)
    /// and ONLY that family. A false positive on a BD/UHD profile (0x0040+)
    /// would skip the LibreDrive firmware unlock that UHD reads require; a
    /// false negative on a DVD would re-introduce the CSS read failure. The
    /// Current Profile is bytes 6-7 of the GET CONFIGURATION header.
    /// Mutation: widening the range to `..=0x0040` makes the BD-ROM assert
    /// fire; a failed/short GET CONFIGURATION must default to NOT-DVD so the
    /// unlock still runs.
    #[test]
    fn disc_is_dvd_matches_only_dvd_profile_family() {
        let probe = |profile: u16| {
            let mut hdr = vec![0u8; 8];
            hdr[6] = (profile >> 8) as u8;
            hdr[7] = profile as u8;
            drive_with(hdr).disc_is_dvd()
        };
        // DVD family → DVD (skip firmware unlock, run stock for CSS).
        assert!(probe(0x0010), "DVD-ROM");
        assert!(probe(0x0011), "DVD-R");
        assert!(probe(0x001B), "DVD+R DL");
        // BD/UHD family → NOT DVD (must keep today's unlock path).
        assert!(!probe(0x0040), "BD-ROM (UHD) must NOT be classed as DVD");
        assert!(!probe(0x0041), "BD-R");
        assert!(!probe(0x0008), "CD-ROM");
        assert!(!probe(0x0000), "no/unknown profile");
        // Short / failed GET CONFIGURATION → no Current Profile → NOT DVD,
        // so the firmware unlock still runs (safe default).
        assert!(
            !drive_with(vec![0u8; 4]).disc_is_dvd(),
            "short GET CONFIGURATION must default to not-DVD (unlock still runs)"
        );
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

    // ── Mocks for Drive::read single-shot semantics + CDB encoding ──

    use std::sync::{Arc, Mutex};

    /// Records the CDB of every execute() and returns a programmable
    /// outcome. Lets a test assert both the bytes sent to the drive and
    /// how the driver translates the transport result.
    struct RecordingTransport {
        last_cdb: Arc<Mutex<Vec<u8>>>,
        last_timeout: Arc<Mutex<u32>>,
        outcome: TransportOutcome,
    }
    enum TransportOutcome {
        /// Report this many bytes transferred (data left as-is).
        Ok(usize),
        /// Fail with a ScsiError carrying this status + optional sense.
        Scsi(u8, Option<crate::scsi::ScsiSense>),
    }
    impl ScsiTransport for RecordingTransport {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            timeout_ms: u32,
        ) -> Result<ScsiResult> {
            *self.last_cdb.lock().unwrap() = cdb.to_vec();
            *self.last_timeout.lock().unwrap() = timeout_ms;
            match self.outcome {
                TransportOutcome::Ok(n) => Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: n,
                    sense: [0u8; 32],
                }),
                TransportOutcome::Scsi(status, sense) => Err(Error::ScsiError {
                    opcode: cdb[0],
                    status,
                    sense,
                }),
            }
        }
    }

    fn recording(outcome: TransportOutcome) -> (Drive, Arc<Mutex<Vec<u8>>>, Arc<Mutex<u32>>) {
        let cdb = Arc::new(Mutex::new(Vec::new()));
        let to = Arc::new(Mutex::new(0u32));
        let t = RecordingTransport {
            last_cdb: cdb.clone(),
            last_timeout: to.clone(),
            outcome,
        };
        (Drive::from_transport_for_test(Box::new(t)), cdb, to)
    }

    #[test]
    fn read_builds_read10_cdb_with_be_lba_and_count() {
        // Drive::read issues READ(10) (0x28). LBA bytes 2..5 big-endian,
        // transfer length bytes 7..8 big-endian (MMC-6). No FUA on this
        // path (byte 1 == 0). Distinct nibbles catch a swapped shift.
        let (mut d, cdb, _to) = recording(TransportOutcome::Ok(4096));
        let mut buf = vec![0u8; 4096];
        let n = d.read(0x00AB_CDEF, 2, &mut buf, false).unwrap();
        assert_eq!(n, 4096, "returns transport bytes_transferred");
        let c = cdb.lock().unwrap();
        assert_eq!(c[0], crate::scsi::SCSI_READ_10);
        assert_eq!(c[1], 0x00, "Drive::read path sets no FUA");
        assert_eq!(&c[2..6], &[0x00, 0xAB, 0xCD, 0xEF], "LBA big-endian");
        assert_eq!(&c[7..9], &[0x00, 0x02], "transfer length big-endian");
    }

    #[test]
    fn read_recovery_flag_selects_60s_timeout() {
        // recovery=true must use READ_RECOVERY_TIMEOUT_MS (60 s); false
        // uses READ_TIMEOUT_MS (10 s). Doc: patch pass vs copy sweep.
        let (mut d, _cdb, to) = recording(TransportOutcome::Ok(2048));
        let mut buf = vec![0u8; 2048];
        d.read(0, 1, &mut buf, true).unwrap();
        assert_eq!(*to.lock().unwrap(), crate::scsi::READ_RECOVERY_TIMEOUT_MS);

        let (mut d2, _c2, to2) = recording(TransportOutcome::Ok(2048));
        d2.read(0, 1, &mut buf, false).unwrap();
        assert_eq!(*to2.lock().unwrap(), crate::scsi::READ_TIMEOUT_MS);
    }

    #[test]
    fn read_maps_scsi_error_to_discread_preserving_status_and_sense() {
        // On a non-Halted failure, Drive::read returns Error::DiscRead
        // with sector=lba and the transport's status+sense carried
        // through (extract_scsi_context). A 03/11/05 MEDIUM ERROR.
        let sense = crate::scsi::ScsiSense {
            sense_key: 3,
            asc: 0x11,
            ascq: 0x05,
        };
        let (mut d, _cdb, _to) = recording(TransportOutcome::Scsi(0x02, Some(sense)));
        let mut buf = vec![0u8; 2048];
        let err = d.read(0x1234, 1, &mut buf, false).unwrap_err();
        match err {
            Error::DiscRead {
                sector,
                status,
                sense: s,
            } => {
                assert_eq!(sector, 0x1234, "sector must be the requested LBA");
                assert_eq!(status, Some(0x02));
                assert_eq!(s, Some(sense), "sense triple preserved");
            }
            other => panic!("expected DiscRead, got {other:?}"),
        }
    }

    #[test]
    fn read_transport_failure_status_preserved_for_marginal_routing() {
        // Status 0xFF (TRANSPORT_FAILURE) with no sense must surface in
        // DiscRead.status so is_scsi_transport_failure() routes it.
        let (mut d, _cdb, _to) = recording(TransportOutcome::Scsi(
            crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            None,
        ));
        let mut buf = vec![0u8; 2048];
        let err = d.read(7, 1, &mut buf, false).unwrap_err();
        assert!(err.is_scsi_transport_failure());
        assert!(err.scsi_sense().is_none());
    }

    #[test]
    fn read_returns_halted_before_dispatch_without_touching_transport() {
        // When the halt flag is set, checked_exec returns Halted BEFORE
        // execute(); the error must be Halted (not DiscRead), so the
        // recovery loop distinguishes user-stop from a read failure.
        let (mut d, cdb, _to) = recording(TransportOutcome::Ok(2048));
        d.halt();
        let mut buf = vec![0u8; 2048];
        let err = d.read(0, 1, &mut buf, false).unwrap_err();
        assert!(matches!(err, Error::Halted));
        assert!(
            cdb.lock().unwrap().is_empty(),
            "transport execute must not run when pre-halted"
        );
    }

    #[test]
    fn clear_halt_reenables_reads() {
        // halt() then clear_halt() must allow reads again — the flag is
        // not sticky.
        let (mut d, _cdb, _to) = recording(TransportOutcome::Ok(2048));
        d.halt();
        d.clear_halt();
        let mut buf = vec![0u8; 2048];
        assert!(d.read(0, 1, &mut buf, false).is_ok());
    }

    #[test]
    fn read_does_not_truncate_reported_bytes() {
        // Single-shot contract: Drive::read returns exactly what the
        // transport reported, never a smaller count silently. Transport
        // says a full 32-sector batch (65536 bytes) succeeded.
        let (mut d, _cdb, _to) = recording(TransportOutcome::Ok(65536));
        let mut buf = vec![0u8; 65536];
        assert_eq!(d.read(0, 32, &mut buf, false).unwrap(), 65536);
    }

    // ── Drive::read chunking against a capped transport ─────────────

    /// Transport with a small `max_transfer_bytes` that records the LBA +
    /// transfer-length of every READ(10) CDB it sees, reports a full
    /// transfer for each, and can be told to fail the Nth read with a SCSI
    /// error. Lets a test assert the chunk decomposition and per-chunk
    /// error LBA.
    struct ChunkingTransport {
        max_bytes: usize,
        /// Recorded (lba, transfer_length_sectors) per READ(10).
        reads: Arc<Mutex<Vec<(u32, u16)>>>,
        /// If Some(i), the i-th READ(10) (0-based) fails with a SCSI error.
        fail_on: Option<usize>,
        seen: usize,
    }
    impl ScsiTransport for ChunkingTransport {
        fn max_transfer_bytes(&self) -> usize {
            self.max_bytes
        }
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            // Only track READ(10); ignore other CDBs (e.g. the 6-byte
            // PREVENT ALLOW MEDIUM REMOVAL the Drive sends on Drop).
            if cdb.first() != Some(&crate::scsi::SCSI_READ_10) || cdb.len() < 10 {
                return Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: data.len(),
                    sense: [0u8; 32],
                });
            }
            let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
            let count = u16::from_be_bytes([cdb[7], cdb[8]]);
            self.reads.lock().unwrap().push((lba, count));
            let idx = self.seen;
            self.seen += 1;
            if self.fail_on == Some(idx) {
                return Err(Error::ScsiError {
                    opcode: cdb[0],
                    status: 0x02,
                    sense: Some(crate::scsi::ScsiSense {
                        sense_key: 3,
                        asc: 0x11,
                        ascq: 0x05,
                    }),
                });
            }
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: data.len(),
                sense: [0u8; 32],
            })
        }
    }

    fn chunking(max_bytes: usize, fail_on: Option<usize>) -> (Drive, Arc<Mutex<Vec<(u32, u16)>>>) {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let t = ChunkingTransport {
            max_bytes,
            reads: reads.clone(),
            fail_on,
            seen: 0,
        };
        (Drive::from_transport_for_test(Box::new(t)), reads)
    }

    #[test]
    fn read_chunks_large_request_to_max_transfer() {
        // max_transfer = 4 sectors (4 * 2048 = 8192 bytes). A read of 10
        // sectors at LBA 0 must split into 3 READ(10) CDBs: (0,4), (4,4),
        // (8,2). The assembled buffer is the full 10*2048 bytes.
        let (mut d, reads) = chunking(4 * 2048, None);
        let mut buf = vec![0u8; 10 * 2048];
        let n = d.read(0, 10, &mut buf, false).unwrap();
        assert_eq!(n, 10 * 2048, "returns total bytes across all chunks");
        let r = reads.lock().unwrap();
        assert_eq!(
            *r,
            vec![(0, 4), (4, 4), (8, 2)],
            "must chunk into 4+4+2 sectors at advancing LBAs"
        );
    }

    #[test]
    fn read_chunk_failure_reports_failing_chunk_lba() {
        // Same 4-sector cap; fail the 2nd chunk (index 1), which covers
        // LBA 4. The error must be DiscRead with sector = 4 (the failing
        // chunk's LBA), NOT the request base LBA 0.
        let (mut d, reads) = chunking(4 * 2048, Some(1));
        let mut buf = vec![0u8; 10 * 2048];
        let err = d.read(0, 10, &mut buf, false).unwrap_err();
        match err {
            Error::DiscRead { sector, status, .. } => {
                assert_eq!(sector, 4, "failing chunk's LBA, not the request base");
                assert_eq!(status, Some(0x02));
            }
            other => panic!("expected DiscRead, got {other:?}"),
        }
        // Reads 0 (LBA 0) succeeded and 1 (LBA 4) failed; the loop stops on
        // the error so LBA 8 is never issued.
        let r = reads.lock().unwrap();
        assert_eq!(*r, vec![(0, 4), (4, 4)], "stops at the failing chunk");
    }

    #[test]
    fn read_small_request_is_single_unchunked_read() {
        // count <= max_sectors must take the single-read path unchanged: a
        // 3-sector read under a 4-sector cap is exactly one READ(10).
        let (mut d, reads) = chunking(4 * 2048, None);
        let mut buf = vec![0u8; 3 * 2048];
        assert_eq!(d.read(0, 3, &mut buf, false).unwrap(), 3 * 2048);
        assert_eq!(*reads.lock().unwrap(), vec![(0, 3)], "single CDB, no split");
    }

    // ── find_drive media-preference selection policy ────────────────

    /// Build a fake drive whose GET EVENT STATUS reply reports the given
    /// media_status byte (byte 5 of an 8-byte reply): 0x02 = DiscPresent,
    /// 0x00 = NoDisc, etc. Stands in for a real opened drive so the
    /// selection policy is testable without hardware.
    fn drive_with_media_byte(media_status: u8) -> Drive {
        let mut buf = vec![0u8; 8];
        buf[5] = media_status;
        drive_with(buf)
    }

    #[test]
    fn select_drive_prefers_drive_with_media() {
        // Drive #1 has no disc (0x00), drive #2 has a disc (0x02). The
        // selection must skip the empty first drive and pick the one with
        // media — the Windows multi-drive bug fix.
        let drives = vec![drive_with_media_byte(0x00), drive_with_media_byte(0x02)];
        let picked = select_drive_with_media(drives.into_iter()).expect("a drive");
        let mut picked = picked;
        assert_eq!(
            picked.drive_status(),
            DriveStatus::DiscPresent,
            "must pick the drive reporting DiscPresent, not the empty first drive"
        );
    }

    #[test]
    fn select_drive_falls_back_to_first_when_none_have_media() {
        // No drive reports a disc → fall back to the FIRST opened drive so
        // single-drive / quirky setups still get a drive (historical
        // behavior preserved). Tag drive #1 distinctly (TrayOpen 0x01) and
        // confirm it, not #2 (NoDisc 0x00), is returned.
        let drives = vec![drive_with_media_byte(0x01), drive_with_media_byte(0x00)];
        let mut picked = select_drive_with_media(drives.into_iter()).expect("a fallback drive");
        assert_eq!(
            picked.drive_status(),
            DriveStatus::TrayOpen,
            "fallback must be the first drive yielded"
        );
    }

    #[test]
    fn select_drive_none_when_no_drives() {
        // No candidates at all → None.
        let empty: Vec<Drive> = Vec::new();
        assert!(select_drive_with_media(empty.into_iter()).is_none());
    }

    // ── drive_status branch coverage (GET EVENT STATUS byte 5) ──────

    #[test]
    fn drive_status_no_disc_maps_correctly() {
        // media_status low bits 0b00 = tray closed, no disc.
        let mut buf = vec![0u8; 8];
        buf[5] = 0x00;
        let mut d = drive_with(buf);
        assert_eq!(d.drive_status(), DriveStatus::NoDisc);
    }

    #[test]
    fn drive_status_tray_open_maps_correctly() {
        // media_status low bits 0b01 = tray open, no media.
        let mut buf = vec![0u8; 8];
        buf[5] = 0x01;
        let mut d = drive_with(buf);
        assert_eq!(d.drive_status(), DriveStatus::TrayOpen);
    }

    #[test]
    fn drive_status_high_bits_in_media_status_ignored() {
        // Only the low 2 bits of byte 5 are the door/media state; upper
        // bits (NEA, etc.) must be masked. 0xFE has low bits 0b10 =
        // DiscPresent.
        let mut buf = vec![0u8; 8];
        buf[5] = 0xFE;
        let mut d = drive_with(buf);
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
    }

    #[test]
    fn drive_status_short_transfer_falls_back_to_tur() {
        // bytes_transferred < 6 means the GET EVENT reply is unusable;
        // the code falls back to a TUR. FixedTransport always returns
        // Ok, so the TUR "succeeds" → DiscPresent. (Buffer length 8 but
        // payload only 4 bytes → bytes_transferred = 4.)
        let mut d = drive_with(vec![0u8; 4]);
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
    }

    /// Transport that fails every command with a programmable error —
    /// drives the TUR-fallback NotReady/Unknown branches of drive_status.
    struct AlwaysErr {
        err: fn() -> Error,
    }
    impl ScsiTransport for AlwaysErr {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Err((self.err)())
        }
    }

    #[test]
    fn drive_status_tur_not_ready_sense_maps_not_ready() {
        // GET EVENT fails, fallback TUR fails with NOT READY sense →
        // DriveStatus::NotReady (drive spinning up). Doc: drive_status
        // fallback branch.
        let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
            err: || Error::ScsiError {
                opcode: 0,
                status: 0x02,
                sense: Some(crate::scsi::ScsiSense {
                    sense_key: 2, // NOT READY
                    asc: 0x04,
                    ascq: 0x01,
                }),
            },
        }));
        assert_eq!(d.drive_status(), DriveStatus::NotReady);
    }

    #[test]
    fn drive_status_tur_unit_attention_maps_not_ready() {
        // UNIT ATTENTION (media changed) on the fallback TUR also maps to
        // NotReady per the is_unit_attention() arm.
        let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
            err: || Error::ScsiError {
                opcode: 0,
                status: 0x02,
                sense: Some(crate::scsi::ScsiSense {
                    sense_key: 6, // UNIT ATTENTION
                    asc: 0x28,
                    ascq: 0x00,
                }),
            },
        }));
        assert_eq!(d.drive_status(), DriveStatus::NotReady);
    }

    #[test]
    fn drive_status_tur_other_error_maps_unknown() {
        // A fallback TUR failure that is neither NOT READY nor UNIT
        // ATTENTION (e.g. transport failure, no sense) → Unknown.
        let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
            err: || Error::ScsiError {
                opcode: 0,
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            },
        }));
        assert_eq!(d.drive_status(), DriveStatus::Unknown);
    }

    // ── get_config_feature: header-strip threshold + clamp ──────────

    #[test]
    fn get_config_feature_strips_8_byte_header() {
        // GET CONFIGURATION reply has an 8-byte Feature Header (MMC-6
        // §5.2.2). get_config_feature returns buf[8..end]. Provide a
        // 12-byte reply → returns the 4 payload bytes.
        let mut payload = vec![0u8; 8];
        payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let mut d = drive_with(payload);
        assert_eq!(
            d.get_config_feature(0x010D),
            Some(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn get_config_feature_at_exactly_8_bytes_returns_none() {
        // end == 8 means header only, no descriptor → None (the `end > 8`
        // guard). Boundary against an off-by-one that would return an
        // empty Vec instead of None.
        let mut d = drive_with(vec![0u8; 8]);
        assert_eq!(d.get_config_feature(0x0000), None);
    }

    // ── report_key / mode_sense / read_buffer empty-vs-some ─────────

    #[test]
    fn report_key_rpc_state_returns_transferred_prefix() {
        // Returns buf[..end] where end = bytes_transferred. An 8-byte
        // reply yields all 8 bytes.
        let mut d = drive_with(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(d.report_key_rpc_state(), Some(vec![1, 2, 3, 4, 5, 6, 7, 8]));
    }

    #[test]
    fn report_key_rpc_state_zero_transfer_returns_none() {
        // end == 0 → None (the `end > 0` guard), never Some(empty).
        let mut d = drive_with(vec![]);
        assert_eq!(d.report_key_rpc_state(), None);
    }

    #[test]
    fn mode_sense_zero_transfer_returns_none() {
        let mut d = drive_with(vec![]);
        assert_eq!(d.mode_sense_page(0x2A), None);
    }

    #[test]
    fn read_buffer_returns_prefix_and_clamps() {
        // read_buffer allocates `length` bytes; FixedTransport returns
        // min(payload, length). Request 16 with a 4-byte payload → 4 bytes.
        let mut d = drive_with(vec![9, 9, 9, 9]);
        assert_eq!(d.read_buffer(0x02, 0xF1, 16), Some(vec![9, 9, 9, 9]));
    }

    #[test]
    fn read_buffer_zero_transfer_returns_none() {
        let mut d = drive_with(vec![]);
        assert_eq!(d.read_buffer(0x02, 0xF1, 16), None);
    }

    // ── No-unlocker paths: init/probe succeed (OEM fallback) ────────

    #[test]
    fn init_without_unlocker_is_ok_oem_fallback() {
        // The test transport's identity matches no registered unlocker, so
        // route_unlock returns None. init() must succeed (leaving the drive
        // in stock mode for the host-cert handshake), not error — the OEM
        // route is the no-match fallback, not a failure.
        let mut d = drive_with(vec![]);
        assert!(
            d.init().is_ok(),
            "no-match init must succeed (OEM fallback)"
        );
        assert!(
            !d.is_ready(),
            "no unlocker ran → not in unlocked-ready state"
        );
    }

    #[test]
    fn probe_disc_without_unlocker_is_ok_noop() {
        // Disc-speed calibration moved into the unlocker (run at init).
        // With no unlocker, probe_disc is a successful no-op.
        let mut d = drive_with(vec![]);
        assert!(d.probe_disc().is_ok());
    }

    // ── decode_read_capacity additional boundaries ──────────────────

    #[test]
    fn read_capacity_exactly_4_bytes_decodes() {
        // bytes_transferred == 4 is the minimum that decodes (the guard
        // is `< 4`). last_lba in bytes 0..4 big-endian.
        let buf = [0x00, 0x00, 0x00, 0x05, 0, 0, 0, 0];
        assert_eq!(decode_read_capacity(&buf, 4).unwrap(), 6);
    }

    #[test]
    fn read_capacity_zero_last_lba_is_one_sector() {
        // last_lba 0 → capacity 1 (a single-sector medium), distinct from
        // the malformed/short-transfer rejection.
        let buf = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(decode_read_capacity(&buf, 8).unwrap(), 1);
    }
}
