//! Drive session — open, identify, and read from optical drives.
//!
//! A `Drive` is opened from a device path, identifies itself via INQUIRY,
//! optionally unlocks/initializes via the `freemkv-unlock` dispatch
//! (through [`crate::unlock_bridge`]), and reads sectors.

pub fn extract_scsi_context(e: &Error) -> (u8, Option<crate::scsi::ScsiSense>) {
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
/// Idle time the disc sits spun-down during [`Drive::spin_cycle`] before it's
/// spun back up — long enough for the mechanism's fast-fail wedge state to
/// clear. Validated at 5–6 s live.
const SPIN_DOWN_IDLE_SECS: u64 = 5;
/// Settle time after spin-up in [`Drive::spin_cycle`] before the caller reads
/// again, so the first post-cycle read doesn't hit a transient NOT_READY.
const SPIN_UP_SETTLE_SECS: u64 = 10;
const SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL: u8 = 0x1E;
const SCSI_GET_EVENT_STATUS: u8 = 0x4A;
const SCSI_MODE_SENSE: u8 = 0x5A;
const SCSI_MODE_SELECT: u8 = 0x55;
const SCSI_REPORT_KEY: u8 = 0xA4;

/// SBC/MMC Read-Write Error Recovery mode page (page code 0x01). We flip the
/// `PER` bit to make the drive REPORT a recovered read (via CHECK CONDITION +
/// sense key RECOVERED ERROR) instead of silently returning best-effort data as
/// GOOD status. On marginal/dirty media that silent-GOOD data can be
/// mis-corrected — a rip that "passed clean" but decoded with errors. With PER
/// on, freemkv sees the marginal read and re-reads it in Pass N (a loud miss,
/// never a silent commit). See `build_error_recovery_select_payload`.
const MODE_PAGE_ERROR_RECOVERY: u8 = 0x01;
/// Bit masks in the Read-Write Error Recovery flags byte (page byte 2).
const ERP_FLAG_TB: u8 = 0x20; // Transfer Block: still deliver the recovered data
const ERP_FLAG_PER: u8 = 0x04; // Post Error: report recovered errors
const ERP_FLAG_DTE: u8 = 0x02; // Data Terminate on Error: MUST be off (we want the data)
/// `Parameters Saveable` bit in a mode page's byte 0 — valid only on MODE SENSE;
/// must be cleared before echoing the page back in a MODE SELECT.
const MODE_PAGE_PS_BIT: u8 = 0x80;
/// MODE SENSE(10) parameter header length (bytes), preceding any block
/// descriptors and the mode pages.
const MODE10_HEADER_LEN: usize = 8;

/// Optical disc drive session -- open, identify, unlock, and read.
pub struct Drive {
    scsi: Box<dyn ScsiTransport>,
    /// Name of the unlocker that handled this drive at `init()`, if any matched.
    /// `None` means no unlocker matched and the drive runs in stock mode
    /// (host-cert AACS handshake carries discs).
    unlocker_name: Option<String>,
    /// The OEM Volume ID the matching unlocker returned from `unlock()` at
    /// `init()`, stashed for the AACS handshake phase (which reads it via
    /// [`Drive::oem_vid`] instead of a separate VID read). `None` when no
    /// unlocker matched or the matching unlocker produced no VID — the cert
    /// handshake then acquires the VID.
    oem_vid: Option<[u8; 16]>,
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
            oem_vid: None,
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
    pub(crate) fn from_transport_for_test(scsi: Box<dyn ScsiTransport>) -> Self {
        Drive {
            scsi,
            unlocker_name: None,
            oem_vid: None,
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

    /// Whether an unlocker claims this drive by identity (i.e. it can be
    /// unlocked at drive-prep). Queried via `freemkv-unlock`; does not require
    /// `init()` to have run.
    pub fn has_profile(&self) -> bool {
        crate::unlock_bridge::unlocker_name(&self.drive_id).is_some()
    }

    /// The name of the drive unlocker that ACTUALLY unlocked this drive at
    /// `init()`, or `None` if none applied (unsupported drive, or the unlock
    /// failed). Distinct from [`has_profile`](Self::has_profile), which reports
    /// only an identity match: this is the runtime outcome. Apps render it in the
    /// user-facing unlocker report.
    pub fn unlocker_name(&self) -> Option<&str> {
        self.unlocker_name.as_deref()
    }

    /// Access the SCSI transport for direct commands (used by CSS/AACS auth).
    pub fn scsi_mut(&mut self) -> &mut dyn ScsiTransport {
        self.scsi.as_mut()
    }

    /// The OEM Volume ID a matching unlocker returned at [`Drive::init`], if any.
    /// The AACS handshake uses this to skip the cert handshake when an unlocker
    /// already supplied the VID. `None` when no unlocker matched or it produced
    /// no VID.
    pub(crate) fn oem_vid(&self) -> Option<[u8; 16]> {
        self.oem_vid
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
        let reply = self.scsi.as_mut().execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        );

        // MMC-6 §6.7: byte 5 is a Media Status only when the Event Header
        // actually announces a Media Event Descriptor behind it. The header is
        // Event Descriptor Length (big-endian, bytes 0-1), then byte 2 = NEA
        // (bit 7) + Notification Class (bits 2-0), then byte 3 = Supported
        // Event Classes. With NEA set the drive is telling us there is NO event
        // to report and returns the header alone; with a different Notification
        // Class the descriptor that follows is not a media one and its second
        // byte means something else entirely.
        //
        // Decoding byte 5 unconditionally turns both of those into Media Status
        // 0 == "tray closed, no disc" — a drive with a disc loaded reported as
        // empty, from a reply that said nothing about the media at all. The
        // drive is untrusted input here, so an event-less or foreign-class reply
        // yields no media state and the TEST UNIT READY fallback answers
        // instead.
        const NEA: u8 = 0x80;
        const NOTIFICATION_CLASS_MASK: u8 = 0x07;
        const NOTIFICATION_CLASS_MEDIA: u8 = 0x04;
        // Bytes 2..7: the 2 remaining header bytes plus the 4-byte Media Event
        // Descriptor — the shortest reply in which byte 5 exists and is a Media
        // Status.
        const MIN_DESCRIPTOR_LENGTH: u16 = 6;

        let media_status = match reply {
            Ok(r) if r.bytes_transferred >= 6 => {
                let descriptor_len = u16::from_be_bytes([buf[0], buf[1]]);
                let class = buf[2] & NOTIFICATION_CLASS_MASK;
                if buf[2] & NEA == 0
                    && class == NOTIFICATION_CLASS_MEDIA
                    && descriptor_len >= MIN_DESCRIPTOR_LENGTH
                {
                    Some(buf[5])
                } else {
                    tracing::debug!(
                        target: "freemkv::drive",
                        nea = buf[2] & NEA != 0,
                        class,
                        descriptor_len,
                        "get event status carried no media event descriptor"
                    );
                    None
                }
            }
            _ => None,
        };

        match media_status {
            Some(media_status) => {
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
            None => {
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
    /// unlocker that ran; before `init()` it reflects the unlocker match by
    /// identity. `"Unknown"` when no unlocker matches.
    pub fn platform_name(&self) -> &str {
        if let Some(ref n) = self.unlocker_name {
            return n;
        }
        // Cache the unlocker match so we can hand out a `&str` borrow.
        self.matched_name_cache.get_or_init(|| {
            crate::unlock_bridge::unlocker_name(&self.drive_id)
                .map(str::to_string)
                .unwrap_or_else(|| "Unknown".to_string())
        })
    }

    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    /// Current mounted-disc profile from the GET CONFIGURATION header
    /// (Current Profile, bytes 6-7). DVD family is `0x0010..=0x001F`, BD
    /// family `0x0040..=0x0043`. This is a stock MMC command — it works
    /// before (and without) any drive unlock. `None` if unreadable.
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
    pub(crate) fn disc_is_dvd(&mut self) -> bool {
        matches!(self.current_profile(), Some(p) if (0x0010..=0x001F).contains(&p))
    }

    /// Initialize drive — drive-prep unlock + init.
    /// Optional. Adds features: removes riplock, enables UHD reads, speed control.
    ///
    /// The drive-prep (OEM) unlock is required for BD/UHD (AACS) reads,
    /// but it puts the drive in an extended-access state where stock CSS
    /// authentication no longer works — so a CSS-protected DVD can't be read.
    /// For a DVD we therefore SKIP the unlock and run the drive in its normal
    /// stock mode; the DVD path then issues standard CSS commands, which a stock
    /// drive honors. BD/UHD and any non-DVD/unknown media keep today's behavior.
    pub fn init(&mut self) -> Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "init", "begin");
        // Drive-prep runs for EVERY disc, DVD INCLUDED. The drive-level firmware
        // unlock lifts riplock and readies max read speed regardless of disc type
        // — drive features are disc-independent. At init the disc kind is not yet
        // probed (Unknown), so only the identity-keyed DRIVE unlocker matches
        // here; the AACS host-cert handshake and the CSS bus-auth handshake run
        // LATER, each gated on the actual disc kind, on TOP of the already-
        // unlocked drive. A genuine transport fault means the bus is dead — abort
        // init (the v1.1.0 invariant; `if let Ok` was silently swallowing it).
        // Every other error (NotApplicable / no match) is "nothing applied" —
        // fall through to stock mode. NOTE: the `if disc_is_dvd() { return }` skip
        // that used to sit here was the v1.0.0-rc.1 regression — it skipped the
        // drive-prep for DVD, leaving DVDs riplocked at stock speed.
        self.init_ran = true;
        let (matched, unlock_res) =
            crate::unlock_bridge::run_features(self.scsi.as_mut(), &self.drive_id);
        let r: Result<()> = match unlock_res {
            Ok(unlocked) => {
                // Record WHICH drive-prep unlocker actually ran — "LibreDrive"
                // (MediaTek) or "Renesas" — not the ld-only identity lookup, so a
                // Renesas drive reports itself honestly rather than as nothing.
                self.unlocker_name = Some(matched.to_string());
                // Stash the OEM Volume ID the unlocker returned for the AACS
                // handshake phase (do_handshake reads it via `oem_vid()`). A
                // drive-prep unlocker always carries a VID; guard anyway.
                if let Some(vid) = unlocked.vid {
                    self.oem_vid = Some(vid);
                }
                Ok(())
            }
            Err(freemkv_unlock::UnlockError::Transport) => Err(Error::ScsiError {
                opcode: 0,
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            }),
            Err(_) => Ok(()),
        };
        // Raise the drive to its maximum read speed — UNCONDITIONALLY whenever
        // the bus is alive (init didn't transport-fault), whether or not an
        // unlocker matched, and for ANY disc type (DVD now flows through here too,
        // so it gets max speed on the freshly firmware-unlocked drive instead of
        // the stock riplock). A stock-mode drive with no firmware unlocker still
        // wants max speed. Best-effort: a failure here must NOT fail the rip.
        if r.is_ok() {
            self.set_speed(Self::SPEED_MAX_KBPS);
            // Ask the drive to REPORT recovered/marginal reads rather than
            // silently commit best-effort data as GOOD (the dirty-disc
            // "passed-clean-but-decodes-with-errors" trap). Best-effort: a drive
            // that doesn't honor it just keeps its defaults — no regression, and
            // on a clean disc it changes nothing.
            self.enable_recovered_error_reporting();
        }
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
        // Disc-speed calibration is unlocker-specific and now lives inside
        // the unlocker's `unlock()` (run at `init()`, for every disc including
        // DVD). Nothing to do here — no disc-type branch.
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

    /// Ask the drive to REPORT recovered/marginal reads instead of silently
    /// returning best-effort data as GOOD status. MODE SENSE the Read-Write
    /// Error Recovery page, flip `PER` (and `TB` on / `DTE` off so we still get
    /// the data), and MODE SELECT it back — preserving the drive's own retry
    /// count and other bits.
    ///
    /// Best-effort: a drive that doesn't support the page, or rejects the SELECT,
    /// simply keeps its default behaviour — no regression, the rip proceeds. On a
    /// clean disc this changes nothing (no recovered errors fire); it only
    /// surfaces the marginal reads that a dirty disc would otherwise commit
    /// silently. Returns whether the page was successfully written.
    pub fn enable_recovered_error_reporting(&mut self) -> bool {
        let Some(sense) = self.mode_sense_page(MODE_PAGE_ERROR_RECOVERY) else {
            tracing::debug!(target: "freemkv::drive", "MODE SENSE error-recovery page unavailable; leaving drive defaults");
            return false;
        };
        let Some(payload) = build_error_recovery_select_payload(&sense) else {
            tracing::debug!(target: "freemkv::drive", "error-recovery page malformed/short; leaving drive defaults");
            return false;
        };
        // MODE SELECT(10): PF=1 (page format), parameter list length = payload.
        let len = payload.len() as u16;
        let cdb = [
            SCSI_MODE_SELECT,
            0x10, // PF=1, SP=0 (don't persist across power cycles)
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            (len >> 8) as u8,
            len as u8,
            0x00,
        ];
        let mut buf = payload;
        match self.checked_exec(&cdb, crate::scsi::DataDirection::ToDevice, &mut buf, 5_000) {
            Ok(_) => {
                tracing::info!(target: "freemkv::drive", phase = "error_recovery", "recovered-error reporting enabled (PER=1) — marginal reads will surface instead of committing silently");
                true
            }
            Err(e) => {
                tracing::debug!(target: "freemkv::drive", error = %e, "MODE SELECT error-recovery page rejected; leaving drive defaults");
                false
            }
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
    /// True when an unlocker claims this drive by identity. Such an unlocker
    /// unlocks *drive functionality* — drive unlock, OEM VID retrieval, and other
    /// vendor capabilities. When one matches, libfreemkv routes both `unlock` and
    /// OEM VID through it (VID via the OEM path is decoupled from the host cert +
    /// HRL). This mirrors [`Self::has_profile`] — the honest signal is "an
    /// unlocker claims this drive" — rather than the old const `false`.
    pub fn is_unlocked(&self) -> bool {
        crate::unlock_bridge::unlocker_name(&self.drive_id).is_some()
    }

    /// Read sectors from the disc. Single-shot — no inline retries, no
    /// SCSI reset.
    ///
    /// `recovery=true` uses [`crate::scsi::READ_RECOVERY_TIMEOUT_MS`] (60 s,
    /// matches sg_dd) for the `freemkv_engine::recovery::patch` pass; `recovery=false` uses
    /// [`crate::scsi::READ_TIMEOUT_MS`] (10 s) for `freemkv_engine::recovery::copy`'s fast
    /// skip-forward sweep. Both budgets are generous enough that the drive
    /// can finish ECC recovery on a marginal sector — pre-0.13.21 this was
    /// 1.5 s on the fast path which forced the kernel mid-layer to time
    /// out and escalate while we waited anyway. On any failure returns
    /// `Err(DiscRead)` immediately; orchestration (`freemkv_engine::recovery::patch` multi-pass,
    /// `DiscStream` adaptive batch halving) handles retry policy.
    ///
    /// Inline retry phases (5× gentle + reset+reopen + 5× more) were
    /// removed in 0.13.6: on some USB-SATA bridges the inline reset wedged
    /// drive firmware without ever recovering a sector. The remaining
    /// recovery layers (freemkv_engine::recovery::patch multi-pass, DiscStream batch halving)
    /// do not touch the wedge-prone reset path.
    pub fn read(&mut self, lba: u32, count: u16, buf: &mut [u8], recovery: bool) -> Result<usize> {
        // Bulk path: FUA off (the drive cache IS the streaming throughput).
        self.read_fua(lba, count, buf, recovery, false)
    }

    /// [`read`], but with an explicit Force Unit Access request: `fua = true`
    /// sets the READ(10) FUA bit so the drive re-fetches the medium instead of
    /// returning a cached copy — the Pass-N marginal-sector lever (see
    /// [`crate::sector::SectorSource::read_sectors_fua`]). The bulk sweep always
    /// passes `false`; only a per-sector recovery handler asks for FUA.
    ///
    /// [`read`]: Drive::read
    pub fn read_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        let timeout_ms = if recovery {
            crate::scsi::READ_RECOVERY_TIMEOUT_MS
        } else {
            crate::scsi::READ_TIMEOUT_MS
        };
        // TRACE, not DEBUG: this fires on every read (hundreds of thousands per
        // rip). At DEBUG it floods a bug-report log and buries the real events.
        tracing::trace!(
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
            return self.read_one(lba, count, buf, timeout_ms, recovery, fua);
        }

        // Large read: split into chunks of at most `max_sectors` sectors,
        // each a self-contained READ(10) with the same validation. Any
        // chunk error reports that chunk's LBA (more precise than the whole
        // request's base LBA).
        let count = count as u32;
        // Check the caller's buffer ONCE, up front. The chunk loop slices `buf`
        // by `count * 2048`; without this an undersized buffer PANICKED ('range
        // end index out of range') out of the public `read`/`read_fua`, while the
        // single-chunk path above tolerates the same undersized buffer and returns
        // `Err(DiscRead)` from `checked_exec`. Behaviour on an undersized buffer
        // must not depend on the transport's transfer limit.
        if buf.len() < count as usize * 2048 {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        // The whole range must be addressable: SBC-3 READ(10) carries a 32-bit
        // LOGICAL BLOCK ADDRESS, so a request whose last chunk crosses `u32::MAX`
        // has no valid CDB. `lba + done` below was unchecked — a debug panic out
        // of the public API, and in release a wrap to a low LBA that was read and
        // returned as if it were the requested one.
        if lba.checked_add(count.saturating_sub(1)).is_none() {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        let mut done: u32 = 0;
        let mut total: usize = 0;
        while done < count {
            let chunk = (count - done).min(max_sectors);
            let cur_lba = lba + done;
            let byte_off = done as usize * 2048;
            let byte_len = chunk as usize * 2048;
            let slice = &mut buf[byte_off..byte_off + byte_len];
            let n = self.read_one(cur_lba, chunk as u16, slice, timeout_ms, recovery, fua)?;
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
        // `recovery` gates only the Linux /dev/sr0 pread fallback below; on
        // other platforms it is intentionally unused.
        #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] recovery: bool,
        // FUA (Force Unit Access): when set, byte-1 bit 0x08 forces the drive to
        // re-fetch the medium past its cache.
        fua: bool,
    ) -> Result<usize> {
        // FUA is OFF on the bulk path — unconditionally forcing every READ(10)
        // past the cache disabled the drive's readahead/streaming cache on the
        // sequential sweep and collapsed throughput ~10x (UHD 15-25 → ~2 MB/s,
        // DVD → ~0.5 MB/s), disc-type-agnostic — the cache IS the streaming
        // throughput. It is set ONLY when a Pass-N recovery handler (FuaRetry)
        // asks for it per marginal-sector re-read, where cache-masking of a
        // stochastic sector actually matters (#55).
        let cdb = [
            crate::scsi::SCSI_READ_10,
            if fua { 0x08 } else { 0x00 },
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
                if recovery
                    && let Some(fd) = self.block_dev_fd
                    && buf.len() >= count as usize * 2048
                {
                    let len = count as usize * 2048;
                    let offset = lba as i64 * 2048;
                    // Drop kernel cache for this region so we get
                    // a fresh device read, not stale page-cache
                    // data from a prior successful neighbour read.
                    let _ = unsafe {
                        libc::posix_fadvise(fd, offset, len as i64, libc::POSIX_FADV_DONTNEED)
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

    /// SET CD SPEED "use the drive's maximum" sentinel (0xFFFF KB/s per MMC).
    pub const SPEED_MAX_KBPS: u16 = 0xFFFF;

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

    /// Soft power-cycle the drive mechanism WITHOUT ejecting: spin the disc
    /// down (`START STOP UNIT`, START=0, **LOEJ=0**) then back up (START=1).
    /// This clears the BU40N/Initio fast-fail *wedge* state that a run of
    /// `HARDWARE_ERROR` reads leaves the drive in — the non-eject equivalent of
    /// the power-cycle our notes say the wedge needs. The disc stays loaded (the
    /// BU40N is slot-loading; we NEVER eject to recover — a hands-on eject is a
    /// failure for an unattended service). Validated live 2026-07-01: took the
    /// drive from failing-every-read back to reading at MB/s.
    pub fn spin_cycle(&mut self) -> Result<()> {
        let stop = [SCSI_START_STOP_UNIT, 0, 0, 0, 0x00, 0]; // START=0, LOEJ=0 → spin down
        let start = [SCSI_START_STOP_UNIT, 0, 0, 0, 0x01, 0]; // START=1, LOEJ=0 → spin up
        let mut buf = [0u8; 0];
        self.scsi
            .as_mut()
            .execute(&stop, crate::scsi::DataDirection::None, &mut buf, 30_000)?;
        std::thread::sleep(std::time::Duration::from_secs(SPIN_DOWN_IDLE_SECS));
        self.scsi
            .as_mut()
            .execute(&start, crate::scsi::DataDirection::None, &mut buf, 30_000)?;
        std::thread::sleep(std::time::Duration::from_secs(SPIN_UP_SETTLE_SECS));
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

    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        self.read_fua(lba, count, buf, recovery, fua)
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

/// Turn a MODE SENSE(10) Read-Write Error Recovery page response into the
/// payload for a MODE SELECT(10) that enables recovered-error REPORTING —
/// preserving every other bit (notably the drive's own read-retry count).
///
/// Pure so the bit-twiddling is unit-tested without a drive. Steps:
/// - locate the page after the 8-byte header + block descriptors (bytes 6-7);
/// - verify it is page 0x01 with a flags byte present;
/// - in the flags byte: set `PER` (report) and `TB` (still deliver the data),
///   clear `DTE` (don't terminate the transfer on the recovered error);
/// - clear the page's `PS` bit (valid only on SENSE) and zero the header's
///   mode-data-length field (reserved on SELECT).
///
/// Returns `None` (caller leaves the drive at its defaults) when the response is
/// too short or isn't the error-recovery page — never panics on adversarial
/// bytes.
fn build_error_recovery_select_payload(sense: &[u8]) -> Option<Vec<u8>> {
    if sense.len() < MODE10_HEADER_LEN {
        return None;
    }
    let block_desc_len = u16::from_be_bytes([sense[6], sense[7]]) as usize;
    let page_off = MODE10_HEADER_LEN.checked_add(block_desc_len)?;
    // Need page byte 0 (code), byte 1 (length), byte 2 (flags).
    if page_off.checked_add(3)? > sense.len() {
        return None;
    }
    if sense[page_off] & 0x3F != MODE_PAGE_ERROR_RECOVERY {
        return None;
    }
    let mut payload = sense.to_vec();
    // Header: mode-data-length is reserved on SELECT — zero it.
    payload[0] = 0;
    payload[1] = 0;
    // Page byte 0: clear PS (SENSE-only).
    payload[page_off] &= !MODE_PAGE_PS_BIT;
    // Flags byte: PER on, TB on, DTE off. Retry count (next byte) untouched.
    payload[page_off + 2] |= ERP_FLAG_PER | ERP_FLAG_TB;
    payload[page_off + 2] &= !ERP_FLAG_DTE;
    Some(payload)
}

/// Decode a READ CAPACITY (10) response into a sector count.
///
/// A short transfer (`bytes_transferred < 4`, which would leave the high
/// bytes zero-initialised and decode to a bogus 1-sector disc) is rejected
/// as [`Error::DiscCapacityMalformed`]. The `0xFFFF_FFFF` "capacity exceeds
/// 32-bit" sentinel, whose `last_lba + 1` overflows `u32`, is reported as the
/// distinct [`Error::DiscCapacityOverflow`] so callers can tell an unusable
/// response apart from an over-large disc.
pub(crate) fn decode_read_capacity(buf: &[u8; 8], bytes_transferred: usize) -> Result<u32> {
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

    /// A minimal MODE SENSE(10) response carrying the Read-Write Error Recovery
    /// page (0x01) with the given flags byte and retry count, no block
    /// descriptors. `ps` sets the page's PS bit (SENSE-only), which the SELECT
    /// payload must clear.
    fn mode_sense_error_recovery(flags: u8, retry: u8, ps: bool) -> Vec<u8> {
        let mut v = vec![0u8; MODE10_HEADER_LEN + 12];
        // Header: nonzero mode-data-length (must be zeroed on SELECT); no block
        // descriptors.
        v[0] = 0x00;
        v[1] = 0x22;
        v[6] = 0x00;
        v[7] = 0x00; // block descriptor length = 0
        let po = MODE10_HEADER_LEN;
        v[po] = MODE_PAGE_ERROR_RECOVERY | if ps { MODE_PAGE_PS_BIT } else { 0 };
        v[po + 1] = 0x0A; // page length
        v[po + 2] = flags; // error-recovery flags
        v[po + 3] = retry; // read retry count
        v
    }

    #[test]
    fn error_recovery_payload_sets_per_tb_clears_dte_ps_preserves_retry() {
        // Start with PER off, DTE on, PS on, a specific retry count. The SELECT
        // payload must flip PER on, TB on, DTE off, clear PS, zero the header
        // mode-data-length, and leave the retry count untouched.
        let sense = mode_sense_error_recovery(ERP_FLAG_DTE, 0x2C, true);
        let out = build_error_recovery_select_payload(&sense).expect("valid page");
        let po = MODE10_HEADER_LEN;
        assert_eq!(out[0], 0, "header mode-data-length zeroed for SELECT");
        assert_eq!(out[1], 0);
        assert_eq!(out[po] & MODE_PAGE_PS_BIT, 0, "PS cleared for SELECT");
        assert_eq!(out[po] & 0x3F, MODE_PAGE_ERROR_RECOVERY, "still page 0x01");
        assert_eq!(out[po + 2] & ERP_FLAG_PER, ERP_FLAG_PER, "PER set");
        assert_eq!(
            out[po + 2] & ERP_FLAG_TB,
            ERP_FLAG_TB,
            "TB set (still get data)"
        );
        assert_eq!(
            out[po + 2] & ERP_FLAG_DTE,
            0,
            "DTE cleared (don't terminate)"
        );
        assert_eq!(out[po + 3], 0x2C, "read retry count preserved");
    }

    #[test]
    fn error_recovery_payload_honors_block_descriptor_offset() {
        // With an 8-byte block descriptor between header and page, the function
        // must locate the page at header+desc, not a fixed offset.
        let mut sense = vec![0u8; MODE10_HEADER_LEN + 8 + 12];
        sense[7] = 8; // block descriptor length
        let po = MODE10_HEADER_LEN + 8;
        sense[po] = MODE_PAGE_ERROR_RECOVERY;
        sense[po + 1] = 0x0A;
        sense[po + 2] = 0x00;
        let out = build_error_recovery_select_payload(&sense).expect("valid");
        assert_eq!(
            out[po + 2] & ERP_FLAG_PER,
            ERP_FLAG_PER,
            "PER set at the descriptor-offset page"
        );
    }

    #[test]
    fn error_recovery_payload_rejects_wrong_or_short_page() {
        // Wrong page code → None (leave drive at defaults).
        let mut wrong = mode_sense_error_recovery(0, 0, false);
        wrong[MODE10_HEADER_LEN] = 0x08; // page 0x08 (caching), not 0x01
        assert!(build_error_recovery_select_payload(&wrong).is_none());
        // Too short to hold the header → None, no panic.
        assert!(build_error_recovery_select_payload(&[0u8; 4]).is_none());
        // Header claims a block descriptor that runs off the buffer → None.
        let mut bad = mode_sense_error_recovery(0, 0, false);
        bad[7] = 0xF0; // descriptor length way past the buffer
        assert!(build_error_recovery_select_payload(&bad).is_none());
    }

    /// Boundary for the "does the buffer hold the full 3-byte page header
    /// (code/length/flags)?" guard: `page_off + 3 > sense.len()`. Build a
    /// buffer that is EXACTLY long enough (no slack) — `page_off + 3 ==
    /// sense.len()` — which must be accepted (`> ` is false), not rejected
    /// (a `>=` mutation would wrongly reject the last valid byte and return
    /// None even though every byte the function touches is in bounds).
    #[test]
    fn error_recovery_payload_accepts_exact_minimum_length() {
        // No block descriptors: page starts right after the 8-byte header.
        // Page needs exactly 3 bytes (code, length, flags) -> total 11.
        let mut sense = vec![0u8; MODE10_HEADER_LEN + 3];
        let po = MODE10_HEADER_LEN;
        sense[po] = MODE_PAGE_ERROR_RECOVERY;
        sense[po + 1] = 0x00; // page length field (unused by this function)
        sense[po + 2] = ERP_FLAG_DTE; // flags: DTE on, PER/TB off
        let out = build_error_recovery_select_payload(&sense)
            .expect("page_off + 3 == len is exactly enough room, must be accepted");
        assert_eq!(out[po + 2] & ERP_FLAG_PER, ERP_FLAG_PER, "PER set");
        assert_eq!(out[po + 2] & ERP_FLAG_DTE, 0, "DTE cleared");
    }

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
    /// would skip the drive unlock that UHD reads require; a
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
        // DVD family → DVD (skip drive unlock, run stock for CSS).
        assert!(probe(0x0010), "DVD-ROM");
        assert!(probe(0x0011), "DVD-R");
        assert!(probe(0x001B), "DVD+R DL");
        // BD/UHD family → NOT DVD (must keep today's unlock path).
        assert!(!probe(0x0040), "BD-ROM (UHD) must NOT be classed as DVD");
        assert!(!probe(0x0041), "BD-R");
        assert!(!probe(0x0008), "CD-ROM");
        assert!(!probe(0x0000), "no/unknown profile");
        // Short / failed GET CONFIGURATION → no Current Profile → NOT DVD,
        // so the drive unlock still runs (safe default).
        assert!(
            !drive_with(vec![0u8; 4]).disc_is_dvd(),
            "short GET CONFIGURATION must default to not-DVD (unlock still runs)"
        );
    }

    /// A conformant GET EVENT STATUS NOTIFICATION reply carrying one Media
    /// Event Descriptor (MMC-6 §6.7): Event Header — Event Descriptor Length
    /// (big-endian, bytes 0-1), then NEA (bit 7) + Notification Class (bits
    /// 2-0) in byte 2 and the Supported Event Class bitmap in byte 3 — followed
    /// by the 4-byte Media Event Descriptor whose byte 1 (reply byte 5) is the
    /// Media Status.
    fn media_event_reply(media_status: u8) -> Vec<u8> {
        let mut buf = vec![0u8; 8];
        buf[0..2].copy_from_slice(&6u16.to_be_bytes()); // 6 bytes follow
        buf[2] = 0x04; // NEA = 0, Notification Class 4 = Media
        buf[3] = 0x10; // Supported Event Classes: media
        buf[4] = 0x00; // Event Code: NoChg
        buf[5] = media_status;
        buf
    }

    #[test]
    fn drive_status_tray_open_and_media_present_is_not_ready_to_rip() {
        // Media Status low bits = 0b11 (tray-open AND media-present,
        // contradictory). Must NOT report DiscPresent.
        let mut d = drive_with(media_event_reply(0x03));
        assert_eq!(d.drive_status(), DriveStatus::TrayOpen);
    }

    #[test]
    fn drive_status_disc_present_maps_correctly() {
        // Media Status 0x02 = media present, tray closed.
        let mut d = drive_with(media_event_reply(0x02));
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
    }

    /// MMC-6 §6.7: byte 5 of the reply is a Media Status ONLY when the Event
    /// Header says a media event descriptor follows — NEA (byte 2 bit 7) clear
    /// AND Notification Class (byte 2 bits 2-0) == 4 (Media). A drive that
    /// answers with NEA set, or with a different class it chose to report, still
    /// returns 8 bytes; decoding byte 5 regardless reads a reserved/zero byte as
    /// Media Status 0 and reports NoDisc on a drive that has a disc loaded —
    /// the classic "works on my drive, not theirs" firmware split. The drive is
    /// untrusted input: an event-less reply carries no media state at all, so
    /// the status must come from the TEST UNIT READY fallback instead.
    ///
    /// This mock answers every command (including the fallback TUR) with
    /// success, so the fallback's verdict is `DiscPresent` — the point is that
    /// it is NOT the fabricated `NoDisc`.
    #[test]
    fn drive_status_rejects_a_reply_carrying_no_media_event_descriptor() {
        // NEA = 1: "No Event Available" — no descriptor was returned, so the
        // bytes after the header are not a Media Event Descriptor.
        let mut nea = media_event_reply(0x00);
        nea[2] = 0x80 | 0x04;
        let mut d = drive_with(nea);
        assert_ne!(
            d.drive_status(),
            DriveStatus::NoDisc,
            "NEA=1 means no event descriptor — byte 5 is not a Media Status"
        );
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);

        // Notification Class 1 (Operational Change), not 4 (Media): a real
        // descriptor, but of a class whose byte 5 means something else.
        let mut other_class = media_event_reply(0x00);
        other_class[2] = 0x01;
        let mut d = drive_with(other_class);
        assert_ne!(
            d.drive_status(),
            DriveStatus::NoDisc,
            "a non-Media notification class carries no media status"
        );
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);

        // Control: the same 8 bytes WITH a valid media event header really do
        // decode Media Status 0 as NoDisc, so the two asserts above are about
        // the header and not about byte 5.
        let mut d = drive_with(media_event_reply(0x00));
        assert_eq!(d.drive_status(), DriveStatus::NoDisc);
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

    /// A drive under test plus the handles that observe it: captured CDB bytes
    /// and the timeout counter.
    struct RecordingHarness {
        drive: Drive,
        cdb: Arc<Mutex<Vec<u8>>>,
        timeouts: Arc<Mutex<u32>>,
    }

    fn recording(outcome: TransportOutcome) -> RecordingHarness {
        let cdb = Arc::new(Mutex::new(Vec::new()));
        let to = Arc::new(Mutex::new(0u32));
        let t = RecordingTransport {
            last_cdb: cdb.clone(),
            last_timeout: to.clone(),
            outcome,
        };
        RecordingHarness {
            drive: Drive::from_transport_for_test(Box::new(t)),
            cdb,
            timeouts: to,
        }
    }

    #[test]
    fn read_builds_read10_cdb_with_be_lba_and_count() {
        // Drive::read issues READ(10) (0x28). LBA bytes 2..5 big-endian,
        // transfer length bytes 7..8 big-endian (MMC-6). FUA is DISABLED (byte 1
        // == 0x00) so the drive cache/readahead is allowed — forcing FUA on the
        // bulk sweep collapsed throughput ~10x. Distinct nibbles catch a swapped
        // shift.
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(4096));
        let mut buf = vec![0u8; 4096];
        let n = d.read(0x00AB_CDEF, 2, &mut buf, false).unwrap();
        assert_eq!(n, 4096, "returns transport bytes_transferred");
        let c = cdb.lock().unwrap();
        assert_eq!(c[0], crate::scsi::SCSI_READ_10);
        assert_eq!(
            c[1], 0x00,
            "FUA disabled — cache/readahead allowed on the bulk read path"
        );
        assert_eq!(&c[2..6], &[0x00, 0xAB, 0xCD, 0xEF], "LBA big-endian");
        assert_eq!(&c[7..9], &[0x00, 0x02], "transfer length big-endian");
    }

    #[test]
    fn read_fua_sets_the_force_unit_access_bit() {
        // The Pass-N FuaRetry lever: read_fua(.., fua=true) sets READ(10) byte-1
        // bit 0x08 so the drive re-fetches the medium past its cache; fua=false
        // leaves it clear (the bulk path).
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(2048));
        let mut buf = vec![0u8; 2048];
        d.read_fua(0, 1, &mut buf, false, true).unwrap();
        assert_eq!(
            cdb.lock().unwrap()[1],
            0x08,
            "FUA requested — byte-1 bit 0x08 set so the drive bypasses its cache"
        );
    }

    /// The existing CDB-encoding test (`read_builds_read10_cdb_with_be_lba_and_count`)
    /// uses LBA `0x00AB_CDEF` and count `2` — both of which have a ZERO top
    /// byte/top-byte-of-count, so a `(lba >> 24) as u8` or `(count >> 8) as
    /// u8` silently mutated to a left shift still yields `0x00` (any
    /// left-shift of at least 8 bits zeroes the low byte a `u8` cast keeps),
    /// and the assertion can't tell the two apart. Use values with a NONZERO
    /// top byte so a right-shift mutated to a left-shift is observable.
    #[test]
    fn read_cdb_shifts_are_not_masked_by_a_zero_top_byte() {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(300 * 2048));
        let mut buf = vec![0u8; 300 * 2048];
        // count = 300 (0x012C): count >> 8 == 0x01, nonzero — a `<<`
        // mutation would instead yield 0x00.
        d.read(0xAABB_CCDD, 300, &mut buf, false).unwrap();
        let c = cdb.lock().unwrap();
        assert_eq!(
            &c[2..6],
            &[0xAA, 0xBB, 0xCC, 0xDD],
            "LBA bytes, including the >>24 top byte, must be big-endian verbatim"
        );
        assert_eq!(
            &c[7..9],
            &[0x01, 0x2C],
            "count bytes, including the >>8 top byte"
        );
    }

    #[test]
    fn read_recovery_flag_selects_60s_timeout() {
        // recovery=true must use READ_RECOVERY_TIMEOUT_MS (60 s); false
        // uses READ_TIMEOUT_MS (10 s). Doc: patch pass vs copy sweep.
        let RecordingHarness {
            drive: mut d,
            cdb: _cdb,
            timeouts: to,
        } = recording(TransportOutcome::Ok(2048));
        let mut buf = vec![0u8; 2048];
        d.read(0, 1, &mut buf, true).unwrap();
        assert_eq!(*to.lock().unwrap(), crate::scsi::READ_RECOVERY_TIMEOUT_MS);

        let RecordingHarness {
            drive: mut d2,
            cdb: _c2,
            timeouts: to2,
        } = recording(TransportOutcome::Ok(2048));
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
        let RecordingHarness {
            drive: mut d,
            cdb: _cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Scsi(0x02, Some(sense)));
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
        let RecordingHarness {
            drive: mut d,
            cdb: _cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Scsi(
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
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(2048));
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
        let RecordingHarness {
            drive: mut d,
            cdb: _cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(2048));
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
        let RecordingHarness {
            drive: mut d,
            cdb: _cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(65536));
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

    /// A drive under test plus the handle recording each `(lba, count)` read.
    struct ChunkingHarness {
        drive: Drive,
        reads: Arc<Mutex<Vec<(u32, u16)>>>,
    }

    fn chunking(max_bytes: usize, fail_on: Option<usize>) -> ChunkingHarness {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let t = ChunkingTransport {
            max_bytes,
            reads: reads.clone(),
            fail_on,
            seen: 0,
        };
        ChunkingHarness {
            drive: Drive::from_transport_for_test(Box::new(t)),
            reads,
        }
    }

    /// An undersized caller buffer must be rejected identically whether the
    /// request fits in one transfer or has to be chunked.
    ///
    /// The chunk loop slices `buf` by `count * 2048`, so without the up-front
    /// length check this PANICKED with "range end index out of range" out of
    /// the public `read`/`read_fua` — while the single-chunk path tolerated the
    /// same buffer and returned `Err(DiscRead)`. Behaviour on a caller error
    /// must not depend on the drive's transfer limit, and a library inside a
    /// long-running service must not panic on it at all.
    ///
    /// Every other read test stays on the single-chunk path, so this guard was
    /// entirely unasserted and a mutation run flipped its arithmetic freely.
    #[test]
    fn an_undersized_buffer_errors_on_the_chunked_path_just_like_the_single_one() {
        // max_transfer = 4 sectors, so a 10-sector read must chunk.
        let mut h = chunking(4 * 2048, None);
        let mut small = vec![0u8; 4096]; // 2 sectors' worth for a 10-sector read

        let chunked = h.drive.read(0, 10, &mut small, false);
        assert!(
            matches!(chunked, Err(Error::DiscRead { .. })),
            "an undersized buffer on the chunked path must be an error, not a panic"
        );

        // The single-chunk path, same undersized buffer, same verdict.
        let single = h.drive.read(0, 3, &mut small, false);
        assert!(
            matches!(single, Err(Error::DiscRead { .. })),
            "the single-chunk path must agree"
        );

        // Exactly-sized still works, so the guard is not simply rejecting
        // everything on the chunked path.
        let mut exact = vec![0u8; 10 * 2048];
        assert!(h.drive.read(0, 10, &mut exact, false).is_ok());
    }

    #[test]
    fn read_chunks_large_request_to_max_transfer() {
        // max_transfer = 4 sectors (4 * 2048 = 8192 bytes). A read of 10
        // sectors at LBA 0 must split into 3 READ(10) CDBs: (0,4), (4,4),
        // (8,2). The assembled buffer is the full 10*2048 bytes.
        let ChunkingHarness {
            drive: mut d,
            reads,
        } = chunking(4 * 2048, None);
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
        let ChunkingHarness {
            drive: mut d,
            reads,
        } = chunking(4 * 2048, Some(1));
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
        let ChunkingHarness {
            drive: mut d,
            reads,
        } = chunking(4 * 2048, None);
        let mut buf = vec![0u8; 3 * 2048];
        assert_eq!(d.read(0, 3, &mut buf, false).unwrap(), 3 * 2048);
        assert_eq!(*reads.lock().unwrap(), vec![(0, 3)], "single CDB, no split");
    }

    /// Transport that fills whatever slice of `data` it's given with a
    /// marker byte derived from the CDB's LBA, so a test can verify BYTE
    /// POSITION, not just which (lba, count) pairs were issued.
    struct PlacementTransport;
    impl ScsiTransport for PlacementTransport {
        fn max_transfer_bytes(&self) -> usize {
            4 * 2048
        }
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            if cdb.first() != Some(&crate::scsi::SCSI_READ_10) || cdb.len() < 10 {
                return Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: data.len(),
                    sense: [0u8; 32],
                });
            }
            let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
            data.fill((lba + 1) as u8);
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: data.len(),
                sense: [0u8; 32],
            })
        }
    }

    /// The chunk loop computes each chunk's destination slice as
    /// `buf[done * 2048 .. done * 2048 + chunk * 2048]`. `reads.lock()`-style
    /// assertions on the (lba, count) pairs alone can't tell `done * 2048`
    /// apart from a corrupted `done + 2048` (chunk boundaries still line up
    /// on nice round numbers for small test LBAs, and neither mock transport
    /// above touches the buffer at all) — this test writes a distinct marker
    /// per chunk and checks it landed at the BYTE offset the request maps
    /// to, catching a `*` -> `+`/`/` mutation in the offset arithmetic that
    /// would silently misplace or overlap chunk data written from the
    /// physical drive into the caller's assembled buffer.
    #[test]
    fn read_chunks_write_into_correctly_offset_buffer_regions() {
        // max_transfer = 4 sectors (8192 bytes): a 10-sector read at LBA 0
        // splits into (lba=0,4), (lba=4,4), (lba=8,2).
        let mut d = Drive::from_transport_for_test(Box::new(PlacementTransport));
        let mut buf = vec![0u8; 10 * 2048];
        d.read(0, 10, &mut buf, false).unwrap();
        assert!(
            buf[0..8192].iter().all(|&b| b == 1),
            "chunk at LBA 0 (marker 1) must fill bytes [0, 8192)"
        );
        assert!(
            buf[8192..16384].iter().all(|&b| b == 5),
            "chunk at LBA 4 (marker 5) must fill bytes [8192, 16384), not overlap the first chunk"
        );
        assert!(
            buf[16384..20480].iter().all(|&b| b == 9),
            "chunk at LBA 8 (marker 9) must fill bytes [16384, 20480)"
        );
    }

    /// The multi-chunk path slices the caller's buffer by `count * 2048` with no
    /// length check, so an undersized `buf` PANICKED ('range end index out of
    /// range') out of the public `Drive::read` / `Drive::read_fua` — while the
    /// single-chunk path (`read_one` → `checked_exec`) tolerates the same
    /// undersized buffer and returns `Err(DiscRead)`. The public API's behaviour
    /// on an undersized buffer must not depend on the transport's transfer limit.
    #[test]
    fn undersized_buffer_multi_chunk_errors_not_panics() {
        let ChunkingHarness {
            drive: mut d,
            reads: _reads,
        } = chunking(4 * 2048, None);
        // count (10) > max_sectors (4) → the chunk loop; buf holds only 1 sector.
        let mut buf = vec![0u8; 2048];
        assert!(
            matches!(d.read(0, 10, &mut buf, false), Err(Error::DiscRead { .. })),
            "an undersized buffer must error, not panic"
        );
        // The single-chunk path with the SAME undersized buffer already errored;
        // the two paths must now agree.
        let mut buf = vec![0u8; 2048];
        assert!(
            matches!(d.read(0, 3, &mut buf, false), Err(Error::DiscRead { .. })),
            "single-chunk path errors on an undersized buffer (unchanged)"
        );
    }

    /// `Drive::read`'s chunk loop advanced the per-chunk LBA with an unchecked
    /// `lba + done`. SBC-3 READ(10) `LOGICAL BLOCK ADDRESS` is a 32-bit field, so
    /// a request whose last chunk crosses `u32::MAX` overflowed: debug panic out
    /// of the public API, release wrap to a low LBA silently read instead.
    #[test]
    fn chunk_lba_near_u32_max_errors_not_overflows() {
        let ChunkingHarness {
            drive: mut d,
            reads: _reads,
        } = chunking(4 * 2048, None);
        let mut buf = vec![0u8; 10 * 2048];
        // 0xFFFF_FFFE + 4 overflows on the second chunk.
        assert!(
            matches!(
                d.read(0xFFFF_FFFE, 10, &mut buf, false),
                Err(Error::DiscRead { .. })
            ),
            "an LBA range past u32::MAX must error, not overflow"
        );
    }

    // ── find_drive media-preference selection policy ────────────────

    /// Build a fake drive whose GET EVENT STATUS reply reports the given
    /// media_status byte (byte 5 of an 8-byte reply): 0x02 = DiscPresent,
    /// 0x00 = NoDisc, etc. Stands in for a real opened drive so the
    /// selection policy is testable without hardware.
    fn drive_with_media_byte(media_status: u8) -> Drive {
        drive_with(media_event_reply(media_status))
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
        let mut d = drive_with(media_event_reply(0x00));
        assert_eq!(d.drive_status(), DriveStatus::NoDisc);
    }

    #[test]
    fn drive_status_tray_open_maps_correctly() {
        // media_status low bits 0b01 = tray open, no media.
        let mut d = drive_with(media_event_reply(0x01));
        assert_eq!(d.drive_status(), DriveStatus::TrayOpen);
    }

    #[test]
    fn drive_status_high_bits_in_media_status_ignored() {
        // MMC-6 §6.7: only the low 2 bits of the Media Event Descriptor's
        // Media Status are the door/media state; the reserved upper bits must
        // be masked. 0xFE has low bits 0b10 = DiscPresent.
        let mut d = drive_with(media_event_reply(0xFE));
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
    }

    /// The `bytes_transferred >= 6` guard exists so byte 5 (Media Status) is
    /// only trusted when the transport actually delivered it. Craft a SHORT
    /// transfer (5 bytes) whose delivered prefix (bytes 0-2) still passes
    /// every OTHER check a look-ahead decode would make (descriptor_len=6,
    /// NEA clear, class=Media) — the only thing distinguishing "trust it"
    /// from "don't" is the byte count. Byte 5 itself was never delivered and
    /// is zero only because the local buffer was zero-initialised, not
    /// because the drive said so. Correct code falls back to the TUR (which
    /// this mock always answers OK) and reports DiscPresent; a guard
    /// weakened to unconditionally-true would decode the untransferred
    /// byte 5 as Media Status 0 and misreport NoDisc — a disc silently
    /// reported as absent from a reply that never said so.
    #[test]
    fn drive_status_rejects_media_status_from_an_undelivered_byte() {
        let short = vec![0x00, 0x06, 0x04, 0x00, 0x00]; // 5 bytes: descriptor_len=6, NEA=0, class=Media
        let mut d = drive_with(short);
        assert_eq!(
            d.drive_status(),
            DriveStatus::DiscPresent,
            "a transfer too short to include byte 5 must fall back to TUR, \
             not decode a media status the drive never sent"
        );
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

    #[test]
    fn get_config_feature_encodes_feature_code_be_in_cdb() {
        // GET CONFIGURATION (0x46), RT field byte 1 = 0x02 (report the
        // named feature), then the 16-bit feature code big-endian in
        // bytes 2..4. 0x010D has a nonzero high byte, so a swapped shift
        // (>> vs <<) or byte order bug asks the drive for the wrong
        // feature entirely.
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(0));
        let _ = d.get_config_feature(0x010D);
        let c = cdb.lock().unwrap();
        assert_eq!(
            &c[..4],
            &[crate::scsi::SCSI_GET_CONFIGURATION, 0x02, 0x01, 0x0D],
            "feature code must be big-endian in CDB bytes 2..4"
        );
    }

    // ── spin_cycle / wait_ready: recovery entry points (LOW finding 8) ──────

    /// Records EVERY CDB issued, in order — unlike `RecordingTransport`
    /// (used above), which only keeps the last one. Needed to assert a
    /// multi-command sequence like `spin_cycle`'s STOP-then-START.
    struct SequenceTransport {
        cdbs: Arc<Mutex<Vec<Vec<u8>>>>,
        ok: bool,
    }
    impl ScsiTransport for SequenceTransport {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            self.cdbs.lock().unwrap().push(cdb.to_vec());
            if self.ok {
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: 0,
                    sense: [0u8; 32],
                })
            } else {
                Err(Error::ScsiError {
                    opcode: cdb[0],
                    status: 2,
                    sense: None,
                })
            }
        }
    }

    /// The documented BU40N/Initio wedge recovery: spin the disc down then
    /// back up WITHOUT ejecting. Exactly two START STOP UNIT (0x1B) commands,
    /// in order — STOP (START=0) then START (START=1) — both with LOEJ=0
    /// (byte 4 bit 1 clear): a slot-loading BU40N must never eject during
    /// unattended recovery. Real time cost (~15s: the validated 5s spin-down
    /// idle + 10s spin-up settle) is accepted here rather than adding an
    /// injectable-sleep seam.
    #[test]
    fn spin_cycle_issues_stop_then_start_without_ejecting() {
        let cdbs = Arc::new(Mutex::new(Vec::new()));
        let t = SequenceTransport {
            cdbs: cdbs.clone(),
            ok: true,
        };
        let mut d = Drive::from_transport_for_test(Box::new(t));
        d.spin_cycle()
            .expect("spin_cycle must succeed when both SCSI commands succeed");
        let seq = cdbs.lock().unwrap();
        assert_eq!(
            seq.len(),
            2,
            "spin_cycle must issue exactly two commands: {seq:?}"
        );
        assert_eq!(seq[0][0], SCSI_START_STOP_UNIT);
        assert_eq!(seq[0][4], 0x00, "first command: START=0 (spin down)");
        assert_eq!(seq[1][0], SCSI_START_STOP_UNIT);
        assert_eq!(seq[1][4], 0x01, "second command: START=1 (spin up)");
        for (i, c) in seq.iter().enumerate() {
            assert_eq!(
                c[4] & 0x02,
                0,
                "LOEJ bit must be clear on command {i} — spin_cycle must never eject"
            );
        }
    }

    /// A drive that never answers TEST UNIT READY successfully must surface
    /// `Err(DeviceNotReady)`, not silently report ready. Real time cost
    /// accepted (60 x 500ms = ~30s) rather than adding an injectable-sleep
    /// seam for the poll backoff.
    #[test]
    fn wait_ready_returns_err_when_drive_never_becomes_ready() {
        struct NeverReady;
        impl ScsiTransport for NeverReady {
            fn execute(
                &mut self,
                cdb: &[u8],
                _dir: DataDirection,
                _data: &mut [u8],
                _timeout_ms: u32,
            ) -> Result<ScsiResult> {
                Err(Error::ScsiError {
                    opcode: cdb[0],
                    status: 2,
                    sense: None,
                })
            }
        }
        let mut d = Drive::from_transport_for_test(Box::new(NeverReady));
        let r = d.wait_ready();
        assert!(
            matches!(r, Err(Error::DeviceNotReady { .. })),
            "a drive that never answers TUR successfully must be DeviceNotReady, got {r:?}"
        );
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
    fn mode_sense_page_positive_transfer_returns_prefix() {
        // Guard is `end > 0`; without a positive-transfer case the guard
        // could be flipped to `end < 0` (always false for a usize) and
        // every call would silently return None.
        let mut d = drive_with(vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(d.mode_sense_page(0x01), Some(vec![0xAA, 0xBB, 0xCC]));
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

    // ── Tray/speed control CDBs (thin wrappers; verify they actually send) ──

    #[test]
    fn set_speed_sends_set_cd_speed_cdb_with_be_speed() {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(0));
        d.set_speed(0x1234);
        let c = cdb.lock().unwrap();
        assert_eq!(c[0], crate::scsi::SCSI_SET_CD_SPEED);
        assert_eq!(&c[2..4], &[0x12, 0x34], "read speed big-endian");
    }

    #[test]
    fn lock_tray_sends_prevent_with_removal_bit_set() {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(0));
        d.lock_tray();
        let c = cdb.lock().unwrap();
        assert_eq!(c[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
        assert_eq!(c[4], 0x01, "PREVENT bit set (locked)");
    }

    #[test]
    fn unlock_tray_sends_prevent_with_removal_bit_clear() {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(0));
        d.unlock_tray();
        let c = cdb.lock().unwrap();
        assert_eq!(c[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
        assert_eq!(c[4], 0x00, "PREVENT bit clear (unlocked)");
    }

    #[test]
    fn eject_unlocks_then_sends_start_stop_with_loej() {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(0));
        d.eject().unwrap();
        // The mock only records the LAST cdb; eject's own START STOP UNIT
        // (with LOEJ=1, byte 4 == 0x02) must be what's left recorded, not
        // the PREVENT/ALLOW from unlock_tray it calls first.
        let c = cdb.lock().unwrap();
        assert_eq!(c[0], SCSI_START_STOP_UNIT);
        assert_eq!(c[4], 0x02, "START=0, LOEJ=1 -> eject");
    }

    /// `SectorSource for Drive` must actually forward to `Drive`'s own
    /// methods, not silently become a no-op / stub return.
    #[test]
    fn sector_source_impl_forwards_to_drive_methods() {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Ok(2048));
        let mut buf = vec![0u8; 2048];
        let n = SectorSource::read_sectors(&mut d, 0, 1, &mut buf, false).unwrap();
        assert_eq!(n, 2048, "read_sectors must forward to Drive::read");
        assert_eq!(cdb.lock().unwrap()[0], crate::scsi::SCSI_READ_10);

        let n2 = SectorSource::read_sectors_fua(&mut d, 0, 1, &mut buf, false, true).unwrap();
        assert_eq!(n2, 2048, "read_sectors_fua must forward to Drive::read_fua");
        assert_eq!(
            cdb.lock().unwrap()[1],
            0x08,
            "fua=true must reach the CDB via the trait method"
        );

        SectorSource::set_speed(&mut d, 0xFFFF);
        assert_eq!(
            cdb.lock().unwrap()[0],
            crate::scsi::SCSI_SET_CD_SPEED,
            "SectorSource::set_speed must forward to Drive::set_speed"
        );
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
