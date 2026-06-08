//! SCSI/MMC command interface.
//!
//! Platform backends are in separate files:
//!   - `linux.rs` — SG_IO ioctl
//!   - `macos.rs` — IOKit SCSITaskDeviceInterface (exclusive access)
//!   - `windows.rs` — SPTI (SCSI Pass-Through Interface)

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

#[allow(unused_imports)]
use crate::error::{Error, Result};
use std::path::Path;

// ── SCSI opcodes (SPC-4, MMC-6) ────────────────────────────────────────────

/// SPC-4 TEST UNIT READY — six-byte CDB, no data transfer. Used by
/// [`drive_has_disc`] as the cheapest "is the drive responsive / does
/// it have media?" probe.
pub const SCSI_TEST_UNIT_READY: u8 = 0x00;
pub const SCSI_INQUIRY: u8 = 0x12;
pub const SCSI_READ_CAPACITY: u8 = 0x25;
pub const SCSI_READ_10: u8 = 0x28;
pub const SCSI_READ_BUFFER: u8 = 0x3C;
pub const SCSI_READ_TOC: u8 = 0x43;
pub const SCSI_GET_CONFIGURATION: u8 = 0x46;
pub const SCSI_SET_CD_SPEED: u8 = 0xBB;
pub const SCSI_SEND_KEY: u8 = 0xA3;
pub const SCSI_REPORT_KEY: u8 = 0xA4;
pub const SCSI_READ_12: u8 = 0xA8;
pub const SCSI_READ_DISC_STRUCTURE: u8 = 0xAD;

/// AACS key class for REPORT KEY / SEND KEY commands.
pub const AACS_KEY_CLASS: u8 = 0x02;

/// Timeout for TEST UNIT READY probes used by [`drive_has_disc`].
/// TUR is the cheapest SCSI op (no data transfer); 5 s is generous
/// for any healthy bus and short enough that a hung device can't stall
/// a poll-loop tick.
pub(crate) const TUR_TIMEOUT_MS: u32 = 5_000;

/// Timeout for content READ commands (READ_10 / READ_12) on the fast
/// path — the [`disc::Disc::copy`] sweep that bisects-on-failure.
///
/// 10 s is calibrated from live empirical data on an LG BU40N + Initio
/// 1618L bridge ripping a UHD with marginal sectors:
///
///   - Sustained sequential reads:    3 – 7 ms
///   - Cold-start seek + read:        up to ~1500 ms
///   - Successful ECC recovery:       1.6 – 2.6 sec
///   - Confirmed unreadable sector:   3.6 – 8.8 sec (kernel timeout)
///
/// 10 s catches every legitimate slow read with comfortable margin and
/// short-circuits truly bad sectors at ~10 s rather than letting the
/// kernel mid-layer escalate for 30 s+.
///
/// Pre-0.13.21 this was 1.5 s, which forced the kernel mid-layer to
/// time out *normal* reads (cold-start often takes ~1.5 s) and run its
/// full ABORT TASK / LUN RESET / BUS RESET escalation while userspace
/// kept submitting fresh reads. The Initio bridge couldn't drain the
/// resulting command queue and entered a wedge state that only physical
/// replug recovered — proven by the v0.13.18 + v0.13.20 live tests.
pub(crate) const READ_TIMEOUT_MS: u32 = 10_000;

/// Timeout for content READ commands on the recovery path —
/// [`disc::Disc::patch`]'s targeted retries on bad ranges. Matches
/// `sg_dd`'s 60 s ceiling: long enough that any sector the drive can
/// recover at all gets the time to do so, short enough that an
/// unresponsive bus is detected before the per-range watchdog fires.
///
/// In practice failed reads return in 1–4 s (the drive itself gives up
/// on uncorrectable ECC before the timeout); the 60 s value is a
/// safety ceiling, not a steady-state cost.
///
/// Historical note (2026-05-08): briefly lowered to 2 s with a 5×
/// inline retry loop in `Disc::patch` to mimic the kernel `sr_mod`
/// driver's auto-retry pattern. The synthetic logic worked but on the
/// live drive each "2 s" read paid ~1.5 s of kernel SCSI mid-layer
/// error escalation on top, so 5× retries took ~17 s per LBA and
/// triggered MAX_RANGE_SECS after 4 sectors — pushing recovery to
/// 0/22 ranges (worse than the 0/22 baseline of v0.17.3 single-shot
/// at 60 s, since that at least visited every range). Reverted; the
/// kernel-auto-retry approach is being pursued via a `/dev/sr0` pread
/// fallback instead.
pub(crate) const READ_RECOVERY_TIMEOUT_MS: u32 = 60_000;

// ── SCSI status bytes (SPC-4 §4.5.5) ────────────────────────────────────────

/// Status byte 0x00 — `GOOD`. Command completed successfully.
pub const SCSI_STATUS_GOOD: u8 = 0x00;
/// Status byte 0x02 — `CHECK CONDITION`. Drive completed the command
/// reply and attached sense data describing the failure.
pub const SCSI_STATUS_CHECK_CONDITION: u8 = 0x02;
/// libfreemkv-synthesised sentinel: the transport never delivered a
/// SCSI status byte (kernel timeout, USB bridge wedge, IOKit service
/// failure). Distinct from any drive-returned value. Carriers
/// [`Error::ScsiError`] with `sense = None`.
pub const SCSI_STATUS_TRANSPORT_FAILURE: u8 = 0xFF;

// ── SPC-4 sense keys (§4.5.6 Table 28) ─────────────────────────────────────
//
// Broad failure category returned in a CHECK CONDITION reply's sense data.
// Names match the SCSI spec; predicate methods on [`ScsiSense`] (e.g.
// `is_medium_error`, `is_unit_attention`) read more fluently than raw
// constant comparisons at call sites.

pub const SENSE_KEY_NO_SENSE: u8 = 0x00;
pub const SENSE_KEY_RECOVERED_ERROR: u8 = 0x01;
pub const SENSE_KEY_NOT_READY: u8 = 0x02;
pub const SENSE_KEY_MEDIUM_ERROR: u8 = 0x03;
pub const SENSE_KEY_HARDWARE_ERROR: u8 = 0x04;
pub const SENSE_KEY_ILLEGAL_REQUEST: u8 = 0x05;
pub const SENSE_KEY_UNIT_ATTENTION: u8 = 0x06;
pub const SENSE_KEY_DATA_PROTECT: u8 = 0x07;
pub const SENSE_KEY_BLANK_CHECK: u8 = 0x08;
pub const SENSE_KEY_ABORTED_COMMAND: u8 = 0x0B;

// ── Sense parsing ───────────────────────────────────────────────────────────

/// Decoded SPC-4 sense triple — the precise reason a SCSI command failed.
///
/// Returned by [`parse_sense`] and embedded inside [`Error::ScsiError`]
/// (`sense: Option<ScsiSense>`). Predicate methods (`is_medium_error`,
/// `is_unit_attention`, `is_marginal`, …) read more fluently at call
/// sites than raw `sense_key` comparisons.
///
/// `Default::default()` and the [`ScsiSense::NONE`] constant both
/// produce the all-zero "no sense info" triple. Per SPC-4 §4.5.3, an
/// empty sense buffer is reported as NO SENSE (key 0); use the constant
/// for explicit intent at construction sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScsiSense {
    /// Sense key — broad failure category (SPC-4 §4.5.6 Table 28).
    /// See the `SENSE_KEY_*` constants for named values.
    pub sense_key: u8,
    /// Additional Sense Code — narrows the cause within a sense key
    /// (SPC-4 §4.5.6 Table 29). E.g. `0x11` = UNRECOVERED READ ERROR.
    pub asc: u8,
    /// Additional Sense Code Qualifier — finest-grain disambiguation.
    /// E.g. `0x05` (with `asc=0x11`) = L-EC UNCORRECTABLE.
    pub ascq: u8,
}

impl ScsiSense {
    /// Sense reply with all-zero fields — explicit "no sense info"
    /// constructor for sites where `Default::default()` would be opaque.
    pub const NONE: ScsiSense = ScsiSense {
        sense_key: 0,
        asc: 0,
        ascq: 0,
    };

    /// `true` when the sense key indicates a *marginal-read* failure —
    /// the kind of error where the same read at smaller granularity
    /// (or a brief retry) sometimes succeeds:
    ///
    ///   - `MEDIUM ERROR` (3) — canonical bad-sector signal
    ///   - `NOT READY` (2) — on many drives (notably BU40N), this is the
    ///     dominant response for unreadable sectors (ASC 04/3E, 04/01, etc.)
    ///   - `ABORTED COMMAND` (B) — transient; retry usually works
    ///   - `RECOVERED ERROR` (1) / `NO SENSE` (0) — drive is healthy and
    ///     either recovered the data or has no specific fault to report
    ///
    /// `false` for HARDWARE ERROR, DATA PROTECT, UNIT ATTENTION,
    /// ILLEGAL REQUEST, BLANK CHECK, and any unknown key. Used
    /// by [`Error::is_marginal_read`] / `Disc::copy`'s hysteresis
    /// dispatch.
    pub fn is_marginal(&self) -> bool {
        matches!(
            self.sense_key,
            SENSE_KEY_NO_SENSE
                | SENSE_KEY_RECOVERED_ERROR
                | SENSE_KEY_NOT_READY
                | SENSE_KEY_MEDIUM_ERROR
                | SENSE_KEY_ABORTED_COMMAND
        )
    }

    /// `true` if `sense_key == MEDIUM ERROR (3)` — canonical "bad sector"
    /// signal from the drive.
    pub fn is_medium_error(&self) -> bool {
        self.sense_key == SENSE_KEY_MEDIUM_ERROR
    }

    /// `true` if `sense_key == HARDWARE ERROR (4)` — drive itself is
    /// failing. Not recoverable by retry.
    pub fn is_hardware_error(&self) -> bool {
        self.sense_key == SENSE_KEY_HARDWARE_ERROR
    }

    /// `true` if `sense_key == NOT READY (2)` — medium not present /
    /// drive becoming ready / etc.
    pub fn is_not_ready(&self) -> bool {
        self.sense_key == SENSE_KEY_NOT_READY
    }

    /// `true` if `sense_key == UNIT ATTENTION (6)` — disc/drive state
    /// changed since the prior command (media inserted/removed,
    /// power-on reset, parameters changed). Caller should rescan rather
    /// than retry the read.
    pub fn is_unit_attention(&self) -> bool {
        self.sense_key == SENSE_KEY_UNIT_ATTENTION
    }

    /// `true` if `sense_key == DATA PROTECT (7)` — read blocked by
    /// AACS / region / write-protect. Retry won't help.
    pub fn is_data_protect(&self) -> bool {
        self.sense_key == SENSE_KEY_DATA_PROTECT
    }

    /// `true` if `sense_key == ILLEGAL REQUEST (5)` — typically a bug
    /// in the CDB we sent (LBA out of range, reserved bit, etc.). Don't
    /// retry.
    pub fn is_illegal_request(&self) -> bool {
        self.sense_key == SENSE_KEY_ILLEGAL_REQUEST
    }

    /// `true` if `sense_key == ABORTED COMMAND (B)` — transient; one
    /// retry is usually safe.
    pub fn is_aborted_command(&self) -> bool {
        self.sense_key == SENSE_KEY_ABORTED_COMMAND
    }
}

/// Decode an SPC-4 sense buffer into the structured triple
/// `(sense_key, asc, ascq)`.
///
/// Handles both response-code formats SPC-4 mandates:
///
///   - **Descriptor format** (response code `0x72` / `0x73`):
///     - sense key = `sense[1] & 0x0F`
///     - asc = `sense[2]`
///     - ascq = `sense[3]`
///   - **Fixed format** (response code `0x70` / `0x71` and any unknown
///     code per SPC-4 §4.5.3):
///     - sense key = `sense[2] & 0x0F`
///     - asc = `sense[12]`
///     - ascq = `sense[13]`
///
/// `sb_len_wr` is the number of bytes the transport actually wrote into
/// `sense`. When the buffer is too short for the relevant fields we
/// return [`ScsiSense::NONE`] for the missing pieces rather than reading
/// uninitialised memory. The minimum useful sense reply is 4 bytes
/// (descriptor, to reach ASCQ at offset 3) or 14 bytes (fixed, to reach
/// ASC/ASCQ at offsets 12/13).
///
/// Pure function — same parse on every platform backend (Linux SG_IO,
/// macOS IOKit, Windows SPTI) so a regression here would silently
/// mis-route SCSI errors on all three OSes simultaneously.
pub(crate) fn parse_sense(sense: &[u8], sb_len_wr: u8) -> ScsiSense {
    let n = (sb_len_wr as usize).min(sense.len());
    if n < 3 {
        return ScsiSense::NONE;
    }
    let response_code = sense[0] & 0x7F;
    let descriptor = response_code == 0x72 || response_code == 0x73;
    if descriptor {
        // Descriptor format: key/asc/ascq are at fixed offsets 1/2/3.
        // n >= 3 is guaranteed by the early return above, so byte 2 is
        // always in bounds; only ascq (byte 3) needs a length check.
        let asc = sense[2];
        let ascq = if n >= 4 { sense[3] } else { 0 };
        ScsiSense {
            sense_key: sense[1] & 0x0F,
            asc,
            ascq,
        }
    } else {
        // Fixed format: key at byte 2, ASC/ASCQ at bytes 12/13.
        let asc = if n >= 13 { sense[12] } else { 0 };
        let ascq = if n >= 14 { sense[13] } else { 0 };
        ScsiSense {
            sense_key: sense[2] & 0x0F,
            asc,
            ascq,
        }
    }
}

// ── SG_IO driver_status bits ────────────────────────────────────────────────

/// `DRIVER_SENSE` (0x08) — bit set in `driver_status` to indicate that
/// sense data was attached to a CHECK CONDITION reply. **Not** a transport
/// failure on its own. Mask this off before deciding whether `driver_status`
/// represents a real bus/host problem.
///
/// Used by Linux SG_IO (`sg_io_hdr.driver_status`); macOS IOKit and
/// Windows SPTI carry the equivalent signal in different fields and
/// don't need the same masking — the misclassification was Linux-only.
#[cfg(target_os = "linux")]
pub(crate) const DRIVER_SENSE: u16 = 0x08;

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataDirection {
    None,
    FromDevice,
    ToDevice,
}

#[derive(Debug)]
pub struct ScsiResult {
    pub status: u8,
    pub bytes_transferred: usize,
    pub sense: [u8; 32],
}

/// Low-level SCSI transport — one implementation per platform.
pub trait ScsiTransport: Send {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult>;
}

// ── Platform-agnostic open / reset ──────────────────────────────────────────

/// Open a SCSI transport for the given device path.
/// Selects the right backend for the current platform.
pub fn open(device: &Path) -> Result<Box<dyn ScsiTransport>> {
    #[cfg(target_os = "linux")]
    {
        Ok(Box::new(linux::SgIoTransport::open(device)?))
    }

    #[cfg(target_os = "macos")]
    {
        Ok(Box::new(macos::MacScsiTransport::open(device)?))
    }

    #[cfg(target_os = "windows")]
    {
        Ok(Box::new(windows::SptiTransport::open(device)?))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        Err(Error::UnsupportedPlatform {
            target: std::env::consts::OS.to_string(),
        })
    }
}

// Note: a top-level `scsi::reset()` used to live here, wrapping a
// platform reset in a thread+recv_timeout so a kernel-wedged ioctl
// couldn't hang the caller. Removed in 0.13.6 along with the
// SG_SCSI_RESET / STOP+START UNIT escalation that needed it. The
// remaining platform reset (Linux: SgIoTransport::reset, available
// for explicit opt-in) does pure userspace state cleanup with bounded
// sleeps — no escape-hatch wrapper required.

// ── USB-layer recovery: rolled back in 0.13.4 ───────────────────────────────
//
// 0.13.1 – 0.13.3 exposed `scsi::usb_reset()` (`USBDEVFS_RESET` on Linux,
// `IOUSBDeviceInterface::ResetDevice` on macOS) and chained it into
// `drive_has_disc` recovery. Production testing on the LG BU40N USB BD-RE
// confirmed the USB stack resets succeed — dmesg logs
// `usb 3-2: reset high-speed USB device` and the device re-authorises —
// but the drive firmware below the USB bridge stays locked: LUN never
// re-enumerates, TUR still times out, the drive is unusable until
// physical unplug-replug or host reboot. Additional approaches tried
// and discarded: `authorized` 0→1 toggle, usb-storage driver
// unbind/rebind, forced SCSI host rescan, `STOP` + `START UNIT`.
//
// The APIs were removed so no caller can be misled into thinking a
// software-only recovery exists for this class of wedge. If a future
// hardware class surfaces where USB-layer recovery actually helps, the
// code should live here again, gated on a wedge signature — see git
// tag `v0.13.3` for the full implementation.

// ── Lightweight discovery + presence probes ─────────────────────────────────
//
// These two are the *only* hardware-touching APIs autorip + freemkv CLI use
// outside the rip path itself. They're intentionally cheap:
//
// - `list_drives()` is a one-shot enumeration: filesystem walk for sg/cdrom
//   nodes, type-5 filter, single INQUIRY per candidate. No firmware, no
//   reset-on-open, no init. Caller caches the result.
// - `drive_has_disc(path)` is a single TEST UNIT READY (six-byte CDB, no
//   data transfer) with internal wedge-recovery escalation. Callers in a
//   poll loop don't need any other primitive to detect "disc inserted /
//   removed" — and they never see the SCSI-vs-USB-reset escalation.
//
// `Drive::open` + `drive.init()` + `Disc::scan` remain heavy and on-demand;
// callers only invoke them once they've decided to actually rip / verify a
// specific drive.

/// One optical drive on the system. Returned by [`list_drives`]. The
/// fields are populated from a single INQUIRY at enumeration time —
/// no firmware reset, no init.
#[derive(Debug, Clone)]
pub struct DriveInfo {
    /// Platform device path: `/dev/sgN` (Linux), `/dev/diskN` (macOS),
    /// `\\.\CdRomN` (Windows).
    pub path: String,
    /// SCSI INQUIRY vendor identifier (e.g. `"HL-DT-ST"`).
    pub vendor: String,
    /// SCSI INQUIRY product identifier (e.g. `"BD-RE BU40N"`).
    pub model: String,
    /// SCSI INQUIRY firmware revision (e.g. `"1.04"`).
    pub firmware: String,
}

/// Enumerate optical drives present on the system.
///
/// **What it does**: per-platform sysfs / IOKit / setupapi walk for SCSI
/// devices, filtered to type 5 (CD/DVD/BD), with a single INQUIRY each
/// for vendor/model/firmware. No firmware reset, no `Drive::init`, no
/// disc scan. Suitable for an autorip-style poll loop or a CLI's
/// drive-list command.
///
/// **What it doesn't do**: probe disc presence (use [`drive_has_disc`]),
/// open a `Drive` for ripping (use [`crate::Drive::open`]), or load
/// drive profiles. Those are heavier operations callers invoke once
/// they've selected a drive.
pub fn list_drives() -> Vec<DriveInfo> {
    #[cfg(target_os = "linux")]
    {
        linux::list_drives()
    }

    #[cfg(target_os = "macos")]
    {
        macos::list_drives()
    }

    #[cfg(target_os = "windows")]
    {
        windows::list_drives()
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        Vec::new()
    }
}

/// True if the drive at `path` currently has a disc inserted.
///
/// Issues a single TEST UNIT READY (cheapest SCSI op, no data transfer).
/// Sense-key 2 ("not ready, medium not present") → `Ok(false)`; any
/// other ready/not-ready response → `Ok(true)` or interpreted ready
/// state. Suitable for poll-loop tick (~50 ms / drive on a healthy bus).
///
/// **No internal recovery.** A single TUR is issued; nothing else. When
/// the transport reports a wedged target (the `0xff` "no answer from the
/// device" pattern synthesised by the backend from a non-zero
/// `host_status` / `driver_status`), that failure surfaces directly to
/// the caller as `Err(Error::ScsiError)` with
/// `status == SCSI_STATUS_TRANSPORT_FAILURE (0xFF)` and `sense: None`. No
/// SCSI bus reset, no USB device reset, no retry is attempted in-library
/// (the USB-reset escalation was removed in 0.13.4 after it was shown to
/// deepen rather than clear the wedge). **No SCSI primitive is exposed to
/// outside crates** — autorip / freemkv CLI / bdemu use this single
/// function for the entire "is there a disc?" decision.
pub fn drive_has_disc(path: &Path) -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        linux::drive_has_disc(path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::drive_has_disc(path)
    }

    #[cfg(target_os = "windows")]
    {
        windows::drive_has_disc(path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = path;
        Err(Error::UnsupportedPlatform {
            target: std::env::consts::OS.to_string(),
        })
    }
}

// ── CDB builders (platform-agnostic) ────────────────────────────────────────

/// SCSI INQUIRY response.
#[derive(Debug, Clone)]
pub struct InquiryResult {
    pub vendor_id: String,
    pub model: String,
    pub firmware: String,
    pub raw: Vec<u8>,
}

/// Send INQUIRY and parse standard response fields.
pub fn inquiry(scsi: &mut dyn ScsiTransport) -> Result<InquiryResult> {
    let cdb = [SCSI_INQUIRY, 0x00, 0x00, 0x00, 0x60, 0x00];
    let mut buf = [0u8; 96];
    scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 5_000)?;

    Ok(InquiryResult {
        vendor_id: String::from_utf8_lossy(&buf[8..16]).trim().to_string(),
        model: String::from_utf8_lossy(&buf[16..32]).trim().to_string(),
        firmware: String::from_utf8_lossy(&buf[32..36]).trim().to_string(),
        raw: buf.to_vec(),
    })
}

/// Send GET CONFIGURATION for feature 0x010C (Firmware Information).
pub fn get_config_010c(scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
    let cdb = [
        SCSI_GET_CONFIGURATION,
        0x02,
        0x01,
        0x0C,
        0x00,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
    ];
    let mut buf = [0u8; 16];
    scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 5_000)?;
    Ok(buf.to_vec())
}

/// Build a READ BUFFER CDB.
pub fn build_read_buffer(mode: u8, buffer_id: u8, offset: u32, length: u32) -> [u8; 10] {
    [
        SCSI_READ_BUFFER,
        mode,
        buffer_id,
        (offset >> 16) as u8,
        (offset >> 8) as u8,
        offset as u8,
        (length >> 16) as u8,
        (length >> 8) as u8,
        length as u8,
        0x00,
    ]
}

/// Build a SET CD SPEED CDB.
pub fn build_set_cd_speed(read_speed: u16) -> [u8; 12] {
    [
        SCSI_SET_CD_SPEED,
        0x00,
        (read_speed >> 8) as u8,
        read_speed as u8,
        0xFF,
        0xFF,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
    ]
}

/// Build a READ(10) CDB with Force Unit Access (FUA) set — byte 1 bit 3
/// (0x08). FUA bypasses the drive cache and reads directly from the
/// medium. (Note: this is *not* a "raw" read; raw optical reads require
/// READ CD, opcode 0xBE.)
pub fn build_read10_fua(lba: u32, count: u16) -> [u8; 10] {
    [
        SCSI_READ_10,
        0x08,
        (lba >> 24) as u8,
        (lba >> 16) as u8,
        (lba >> 8) as u8,
        lba as u8,
        0x00,
        (count >> 8) as u8,
        count as u8,
        0x00,
    ]
}

#[cfg(test)]
mod parse_sense_tests {
    //! Unit tests for [`parse_sense`]. Covers both SPC-4 sense data
    //! formats (descriptor / fixed) and the short-buffer fallback. The
    //! same helper runs on every platform backend so a regression here
    //! would silently miscategorize SCSI errors on Linux, macOS, and
    //! Windows simultaneously.
    use super::parse_sense;
    fn parse_sense_key(sense: &[u8], sb_len_wr: u8) -> u8 {
        parse_sense(sense, sb_len_wr).sense_key
    }

    /// Helper: build a 32-byte sense buffer whose first three bytes are
    /// the given prefix; the rest are zeroes (sense data area).
    fn buf(b0: u8, b1: u8, b2: u8) -> [u8; 32] {
        let mut s = [0u8; 32];
        s[0] = b0;
        s[1] = b1;
        s[2] = b2;
        s
    }

    #[test]
    fn descriptor_format_72_picks_byte_1() {
        // Response code 0x72 (current, descriptor): sense key is the
        // low nibble of byte 1. Byte 2 here is 0x77 to prove it is NOT
        // the byte the parser reads.
        let s = buf(0x72, 0x05, 0x77); // ILLEGAL REQUEST
        assert_eq!(parse_sense_key(&s, 8), 5);
    }

    #[test]
    fn descriptor_format_73_picks_byte_1() {
        // Response code 0x73 (deferred, descriptor): same parse rule
        // as 0x72.
        let s = buf(0x73, 0x06, 0xFF); // UNIT ATTENTION
        assert_eq!(parse_sense_key(&s, 8), 6);
    }

    #[test]
    fn fixed_format_70_picks_byte_2() {
        // Response code 0x70 (current, fixed): sense key is the low
        // nibble of byte 2. Byte 1 is 0x77 to prove it is NOT read.
        let s = buf(0x70, 0x77, 0x05); // ILLEGAL REQUEST
        assert_eq!(parse_sense_key(&s, 18), 5);
    }

    #[test]
    fn fixed_format_71_picks_byte_2() {
        // Response code 0x71 (deferred, fixed): same parse as 0x70.
        let s = buf(0x71, 0x77, 0x02); // NOT READY
        assert_eq!(parse_sense_key(&s, 18), 2);
    }

    #[test]
    fn high_bit_in_byte_0_is_masked() {
        // SPC-4 sets the top bit of byte 0 ("INFORMATION VALID" / "VALID")
        // independently of the response code. parse_sense_key must mask
        // it off before classifying the format.
        let s = buf(0xF2, 0x05, 0x77);
        assert_eq!(
            parse_sense_key(&s, 8),
            5,
            "VALID-bit must not leak into format detection"
        );
        let s = buf(0xF0, 0x77, 0x02);
        assert_eq!(parse_sense_key(&s, 18), 2);
    }

    #[test]
    fn high_nibble_in_key_byte_is_masked() {
        // Sense key is byte_n & 0x0F (low nibble). Top nibble holds
        // FILEMARK / EOM / ILI / SDAT_OVFL flags, which must not bleed
        // into the key value.
        let s = buf(0x70, 0x00, 0xE5); // 0xE0 flags + key 5
        assert_eq!(parse_sense_key(&s, 18), 5);
    }

    #[test]
    fn sb_len_wr_zero_returns_no_sense() {
        // Transport set status non-zero but wrote zero sense bytes —
        // SPC-4 §4.5.3 says treat as NO SENSE (key 0).
        let s = buf(0x72, 0x05, 0x05);
        assert_eq!(parse_sense_key(&s, 0), 0);
    }

    #[test]
    fn sb_len_wr_below_three_returns_no_sense() {
        // Less than three bytes in the buffer means we can't safely
        // read either format byte 0 or key byte 2 — return 0.
        let s = buf(0x72, 0x05, 0x05);
        assert_eq!(parse_sense_key(&s, 1), 0);
        assert_eq!(parse_sense_key(&s, 2), 0);
    }

    #[test]
    fn slice_below_three_returns_no_sense() {
        // Defense-in-depth: even if a caller passes a too-short slice
        // with a falsely-large sb_len_wr, we don't panic and we return 0.
        let s = [0x72u8, 0x05];
        assert_eq!(parse_sense_key(&s, 8), 0);
    }

    #[test]
    fn unknown_response_code_falls_through_to_fixed() {
        // SPC-4 mandates implementations tolerate unknown response
        // codes and treat them as fixed format. Vendor-specific codes
        // in the 0x74..0x7E range surface here.
        let s = buf(0x7A, 0x77, 0x03); // MEDIUM ERROR via "fixed"
        assert_eq!(parse_sense_key(&s, 18), 3);
    }

    // ── Additional parse_sense coverage ─────────────────────────────

    /// Full 32-byte buffer to write arbitrary offsets into.
    fn buf32() -> [u8; 32] {
        [0u8; 32]
    }

    #[test]
    fn descriptor_format_reads_asc_byte2_ascq_byte3() {
        // SPC-4 §4.5.2.1 descriptor format: ASC at offset 2, ASCQ at
        // offset 3. Build 04/3E (NOT READY / logical unit not ready,
        // command in progress) — the BU40N bad-sector signature.
        let mut s = buf32();
        s[0] = 0x72;
        s[1] = 0x02; // NOT READY
        s[2] = 0x3E; // ASC
        s[3] = 0x01; // ASCQ
        let d = parse_sense(&s, 8);
        assert_eq!(d.sense_key, 2);
        assert_eq!(d.asc, 0x3E, "descriptor ASC is byte 2");
        assert_eq!(d.ascq, 0x01, "descriptor ASCQ is byte 3");
    }

    #[test]
    fn descriptor_format_key_nibble_masked() {
        // Descriptor byte 1 low nibble is the sense key. Even though the
        // upper nibble of byte 1 is reserved in descriptor format, the
        // parser masks &0x0F unconditionally; set the high nibble and
        // confirm it doesn't leak.
        let mut s = buf32();
        s[0] = 0x72;
        s[1] = 0xF3; // upper nibble garbage + key 3 (MEDIUM ERROR)
        s[2] = 0x11;
        s[3] = 0x05;
        let d = parse_sense(&s, 8);
        assert_eq!(d.sense_key, 3);
    }

    #[test]
    fn descriptor_n_exactly_3_ascq_defaults_zero() {
        // Descriptor needs byte 3 for ASCQ; with only 3 bytes written
        // the doc contract says ASCQ defaults to 0 rather than reading
        // uninitialised byte 3. ASC (byte 2) is still valid.
        let mut s = buf32();
        s[0] = 0x72;
        s[1] = 0x03;
        s[2] = 0x11;
        s[3] = 0x05; // present in buffer but n=3 must NOT read it
        let d = parse_sense(&s, 3);
        assert_eq!(d.sense_key, 3);
        assert_eq!(d.asc, 0x11);
        assert_eq!(d.ascq, 0, "n=3 must not reach descriptor ASCQ at offset 3");
    }

    #[test]
    fn fixed_format_full_reads_asc_byte12_ascq_byte13() {
        // SPC-4 §4.5.3 fixed format: key at byte 2, ASC at byte 12,
        // ASCQ at byte 13. Build 03/11/05 = MEDIUM ERROR / UNRECOVERED
        // READ ERROR / L-EC UNCORRECTABLE.
        let mut s = buf32();
        s[0] = 0x70;
        s[2] = 0x03;
        s[12] = 0x11;
        s[13] = 0x05;
        let d = parse_sense(&s, 18);
        assert_eq!(d.sense_key, 3);
        assert_eq!(d.asc, 0x11, "fixed ASC is byte 12");
        assert_eq!(d.ascq, 0x05, "fixed ASCQ is byte 13");
    }

    #[test]
    fn fixed_format_short_buffer_asc_ascq_default_zero() {
        // Fixed format needs n>=13 for ASC, n>=14 for ASCQ. A reply that
        // only has the key byte (e.g. an 8-byte sense reply, common from
        // some bridges) must yield asc=ascq=0, never read past the
        // written region. Sense key must still decode.
        let mut s = buf32();
        s[0] = 0x70;
        s[2] = 0x04; // HARDWARE ERROR
        s[12] = 0xAA; // present in array but n must gate it off
        s[13] = 0xBB;
        let d = parse_sense(&s, 8);
        assert_eq!(d.sense_key, 4);
        assert_eq!(d.asc, 0, "n=8 < 13: ASC must default 0");
        assert_eq!(d.ascq, 0, "n=8 < 14: ASCQ must default 0");
    }

    #[test]
    fn fixed_format_n13_reads_asc_but_not_ascq() {
        // Boundary: n==13 means bytes 0..12 inclusive are valid, so ASC
        // (byte 12) is readable but ASCQ (byte 13) is not. Exercises the
        // distinct n>=13 vs n>=14 guards.
        let mut s = buf32();
        s[0] = 0x70;
        s[2] = 0x03;
        s[12] = 0x11;
        s[13] = 0x05; // must NOT be read at n=13
        let d = parse_sense(&s, 13);
        assert_eq!(d.asc, 0x11, "n=13 reaches ASC at offset 12");
        assert_eq!(d.ascq, 0, "n=13 does not reach ASCQ at offset 13");
    }

    #[test]
    fn fixed_format_n14_reads_both() {
        // Boundary: n==14 is the minimum for a complete fixed ASC/ASCQ.
        let mut s = buf32();
        s[0] = 0x70;
        s[2] = 0x03;
        s[12] = 0x11;
        s[13] = 0x05;
        let d = parse_sense(&s, 14);
        assert_eq!(d.asc, 0x11);
        assert_eq!(d.ascq, 0x05, "n=14 reaches ASCQ at offset 13");
    }

    #[test]
    fn n_exactly_three_decodes_key_only() {
        // n==3 is the minimum that passes the n<3 early-return. For fixed
        // format the key (byte 2) is decodable; asc/ascq default to 0.
        let s = buf(0x70, 0x77, 0x06); // UNIT ATTENTION
        let d = parse_sense(&s, 3);
        assert_eq!(d.sense_key, 6);
        assert_eq!(d.asc, 0);
        assert_eq!(d.ascq, 0);
    }

    #[test]
    fn descriptor_high_bit_set_on_72_still_descriptor() {
        // 0xF2 = VALID bit | 0x72. After masking 0x7F the response code
        // is 0x72 (descriptor), so ASC/ASCQ come from bytes 2/3, not
        // 12/13. Put a fixed-format ASC at byte 12 to prove it's ignored.
        let mut s = buf32();
        s[0] = 0xF2;
        s[1] = 0x03;
        s[2] = 0x11; // descriptor ASC
        s[3] = 0x05;
        s[12] = 0x99; // would be ASC if mis-parsed as fixed
        let d = parse_sense(&s, 18);
        assert_eq!(d.asc, 0x11, "VALID-bit masking must keep descriptor parse");
    }

    #[test]
    fn empty_slice_returns_none() {
        // Defense-in-depth: zero-length slice with any sb_len_wr must not
        // panic and returns the all-zero triple.
        let s: [u8; 0] = [];
        let d = parse_sense(&s, 32);
        assert_eq!(d, super::ScsiSense::NONE);
    }
}

#[cfg(test)]
mod scsi_sense_predicate_tests {
    //! Classification of [`ScsiSense`] predicate methods against SPC-4
    //! §4.5.6 Table 28 sense keys. These drive `Disc::copy` hysteresis
    //! and `Disc::patch` routing; a misclassification here silently
    //! changes which sectors get retried vs. marked unreadable.
    use super::*;

    fn s(key: u8) -> ScsiSense {
        ScsiSense {
            sense_key: key,
            asc: 0,
            ascq: 0,
        }
    }

    #[test]
    fn is_marginal_matches_exactly_the_recoverable_keys() {
        // Doc contract: marginal == {NO SENSE(0), RECOVERED(1),
        // NOT READY(2), MEDIUM ERROR(3), ABORTED COMMAND(B)}.
        // Everything else is non-marginal. Walk every 4-bit key value.
        let marginal: [u8; 5] = [
            SENSE_KEY_NO_SENSE,
            SENSE_KEY_RECOVERED_ERROR,
            SENSE_KEY_NOT_READY,
            SENSE_KEY_MEDIUM_ERROR,
            SENSE_KEY_ABORTED_COMMAND,
        ];
        for key in 0u8..=0x0F {
            let expect = marginal.contains(&key);
            assert_eq!(
                s(key).is_marginal(),
                expect,
                "key {key:#x} marginal classification"
            );
        }
    }

    #[test]
    fn each_specific_predicate_is_exclusive() {
        // Each is_* predicate matches exactly its one key and no other.
        // Catches a copy-paste bug where e.g. is_not_ready compared the
        // wrong constant.
        let cases: &[(u8, fn(&ScsiSense) -> bool)] = &[
            (SENSE_KEY_MEDIUM_ERROR, ScsiSense::is_medium_error),
            (SENSE_KEY_HARDWARE_ERROR, ScsiSense::is_hardware_error),
            (SENSE_KEY_NOT_READY, ScsiSense::is_not_ready),
            (SENSE_KEY_UNIT_ATTENTION, ScsiSense::is_unit_attention),
            (SENSE_KEY_DATA_PROTECT, ScsiSense::is_data_protect),
            (SENSE_KEY_ILLEGAL_REQUEST, ScsiSense::is_illegal_request),
            (SENSE_KEY_ABORTED_COMMAND, ScsiSense::is_aborted_command),
        ];
        for &(key, pred) in cases {
            for other in 0u8..=0x0F {
                let got = pred(&s(other));
                assert_eq!(
                    got,
                    other == key,
                    "predicate for key {key:#x} fired on {other:#x}"
                );
            }
        }
    }

    #[test]
    fn none_constant_and_default_agree_and_are_no_sense() {
        // SPC-4 §4.5.3: empty sense reply is NO SENSE (key 0). Both the
        // NONE constant and Default must be the all-zero triple and be
        // classified marginal (NO SENSE is in the marginal set).
        assert_eq!(ScsiSense::NONE, ScsiSense::default());
        assert_eq!(ScsiSense::NONE.sense_key, SENSE_KEY_NO_SENSE);
        assert!(ScsiSense::NONE.is_marginal());
    }
}

#[cfg(test)]
mod cdb_builder_tests {
    //! CDB byte-layout tests grounded in MMC-6 / SPC-4 field definitions.
    //! A wrong shift or byte index silently sends a malformed command to
    //! the drive (wrong LBA, wrong length) — the 0.31.0 class of bug.
    use super::*;

    #[test]
    fn read10_fua_opcode_and_fua_bit() {
        // MMC-6 READ(10): byte 0 = opcode 0x28. FUA is byte 1 bit 3
        // (0x08) per SBC-3 §5.20. Doc explicitly sets FUA.
        let cdb = build_read10_fua(0, 1);
        assert_eq!(cdb[0], SCSI_READ_10);
        assert_eq!(cdb[0], 0x28);
        assert_eq!(cdb[1], 0x08, "FUA bit (byte1 bit3) must be set");
    }

    #[test]
    fn read10_fua_lba_big_endian_bytes_2_5() {
        // READ(10) LOGICAL BLOCK ADDRESS occupies bytes 2..5, big-endian
        // (MSB first). Use a value with all four bytes distinct so a
        // swapped shift is caught.
        let cdb = build_read10_fua(0x1122_3344, 0);
        assert_eq!(cdb[2], 0x11);
        assert_eq!(cdb[3], 0x22);
        assert_eq!(cdb[4], 0x33);
        assert_eq!(cdb[5], 0x44);
    }

    #[test]
    fn read10_fua_transfer_length_big_endian_bytes_7_8() {
        // READ(10) TRANSFER LENGTH is bytes 7..8 big-endian (number of
        // logical blocks). Byte 6 (group number) and byte 9 (control)
        // are zero.
        let cdb = build_read10_fua(0, 0xABCD);
        assert_eq!(cdb[6], 0x00, "byte 6 group number must be 0");
        assert_eq!(cdb[7], 0xAB, "transfer length MSB");
        assert_eq!(cdb[8], 0xCD, "transfer length LSB");
        assert_eq!(cdb[9], 0x00, "byte 9 control must be 0");
    }

    #[test]
    fn read10_fua_max_lba_and_count() {
        // u32::MAX LBA and u16::MAX count must encode without truncation
        // or panic (overflow on debug builds would be a bug).
        let cdb = build_read10_fua(u32::MAX, u16::MAX);
        assert_eq!(&cdb[2..6], &[0xFF, 0xFF, 0xFF, 0xFF]);
        assert_eq!(&cdb[7..9], &[0xFF, 0xFF]);
    }

    #[test]
    fn read_buffer_cdb_layout() {
        // MMC-6 READ BUFFER (0x3C): byte0 opcode, byte1 mode, byte2
        // buffer id, bytes 3..5 buffer offset (big-endian 24-bit),
        // bytes 6..8 allocation length (big-endian 24-bit), byte9 control.
        let cdb = build_read_buffer(0x02, 0xF1, 0x010203, 0x040506);
        assert_eq!(cdb[0], SCSI_READ_BUFFER);
        assert_eq!(cdb[1], 0x02, "mode");
        assert_eq!(cdb[2], 0xF1, "buffer id");
        assert_eq!(&cdb[3..6], &[0x01, 0x02, 0x03], "offset 24-bit BE");
        assert_eq!(&cdb[6..9], &[0x04, 0x05, 0x06], "length 24-bit BE");
        assert_eq!(cdb[9], 0x00, "control");
    }

    #[test]
    fn read_buffer_offset_truncates_to_24_bits_low() {
        // The CDB offset field is 24-bit; the builder takes the low three
        // bytes of the u32. A value with a non-zero top byte must encode
        // only the low 24 bits (matching the wire field width). This
        // documents the actual contract, not a guess.
        let cdb = build_read_buffer(0, 0, 0xFF01_0203, 0);
        assert_eq!(&cdb[3..6], &[0x01, 0x02, 0x03]);
    }

    #[test]
    fn set_cd_speed_cdb_layout() {
        // MMC-6 SET CD SPEED (0xBB): byte0 opcode, bytes 2..3 read speed
        // (big-endian kB/s), bytes 4..5 write speed = 0xFFFF (no change /
        // max). Use a distinct read speed to verify byte order.
        let cdb = build_set_cd_speed(0x1234);
        assert_eq!(cdb[0], SCSI_SET_CD_SPEED);
        assert_eq!(cdb[2], 0x12, "read speed MSB");
        assert_eq!(cdb[3], 0x34, "read speed LSB");
        assert_eq!(cdb[4], 0xFF, "write speed bytes set to 0xFFFF");
        assert_eq!(cdb[5], 0xFF);
    }

    #[test]
    fn set_cd_speed_zero_means_drive_default() {
        // read_speed 0 encodes as 0x0000 (MMC: "use drive default").
        let cdb = build_set_cd_speed(0);
        assert_eq!(cdb[2], 0x00);
        assert_eq!(cdb[3], 0x00);
    }
}

#[cfg(test)]
mod inquiry_tests {
    //! [`inquiry`] standard-INQUIRY field parsing (SPC-4 §6.4.2 Table 142):
    //!   - vendor identification: bytes 8..16 (8 ASCII chars)
    //!   - product identification: bytes 16..32 (16 ASCII chars)
    //!   - product revision level: bytes 32..36 (4 ASCII chars)
    //! Fields are space-padded ASCII; the parser trims surrounding
    //! whitespace.
    use super::*;

    /// Mock transport returning a scripted INQUIRY payload and recording
    /// the CDB it was handed.
    struct ScriptedTransport {
        payload: Vec<u8>,
        last_cdb: Vec<u8>,
    }
    impl ScsiTransport for ScriptedTransport {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            self.last_cdb = cdb.to_vec();
            let n = self.payload.len().min(data.len());
            data[..n].copy_from_slice(&self.payload[..n]);
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: n,
                sense: [0u8; 32],
            })
        }
    }

    fn inquiry_payload(vendor: &[u8], product: &[u8], rev: &[u8]) -> Vec<u8> {
        // SPC-4 §6.4.2: identifier fields are left-aligned ASCII, padded
        // with SPACE (0x20), not NUL — build the fixture that way so the
        // parser's trim() is exercised on real-shaped padding.
        let mut p = vec![0u8; 96];
        // peripheral device type 5 (CD/DVD) in byte 0 low 5 bits — not
        // parsed by inquiry() but realistic.
        p[0] = 0x05;
        for b in &mut p[8..36] {
            *b = b' ';
        }
        p[8..8 + vendor.len()].copy_from_slice(vendor);
        p[16..16 + product.len()].copy_from_slice(product);
        p[32..32 + rev.len()].copy_from_slice(rev);
        p
    }

    #[test]
    fn parses_vendor_product_revision_offsets() {
        // Real BU40N-style identity. Vendor "HL-DT-ST" (8 chars exactly),
        // product padded to 16, revision "1.04".
        let payload = inquiry_payload(b"HL-DT-ST", b"BD-RE BU40N     ", b"1.04");
        let mut t = ScriptedTransport {
            payload,
            last_cdb: vec![],
        };
        let r = inquiry(&mut t).unwrap();
        assert_eq!(r.vendor_id, "HL-DT-ST");
        assert_eq!(r.model, "BD-RE BU40N");
        assert_eq!(r.firmware, "1.04");
    }

    #[test]
    fn fields_are_independent_no_bleed_across_offset_boundaries() {
        // A wrong end-offset (e.g. vendor 8..17) would pull the first
        // product char into the vendor string. Use a vendor that fills
        // all 8 bytes and a product whose first byte is distinctive.
        let payload = inquiry_payload(b"VENDOR12", b"XPRODUCT", b"REV0");
        let mut t = ScriptedTransport {
            payload,
            last_cdb: vec![],
        };
        let r = inquiry(&mut t).unwrap();
        assert_eq!(r.vendor_id, "VENDOR12", "vendor must stop at byte 16");
        assert!(
            !r.vendor_id.contains('X'),
            "product byte must not bleed into vendor"
        );
        assert_eq!(r.model, "XPRODUCT");
    }

    #[test]
    fn whitespace_padded_fields_trimmed() {
        // SPC-4 pads identifiers with spaces; trim() removes them.
        let payload = inquiry_payload(b"  ABC   ", b"  MODEL X       ", b" R1 ");
        let mut t = ScriptedTransport {
            payload,
            last_cdb: vec![],
        };
        let r = inquiry(&mut t).unwrap();
        assert_eq!(r.vendor_id, "ABC");
        assert_eq!(r.model, "MODEL X");
        assert_eq!(r.firmware, "R1");
    }

    #[test]
    fn cdb_is_standard_inquiry_96_bytes() {
        // The CDB must be INQUIRY (0x12) with allocation length 0x60 (96)
        // in byte 4 — matching the 96-byte buffer the parser slices.
        let payload = inquiry_payload(b"V", b"M", b"R");
        let mut t = ScriptedTransport {
            payload,
            last_cdb: vec![],
        };
        let _ = inquiry(&mut t).unwrap();
        assert_eq!(t.last_cdb[0], SCSI_INQUIRY);
        assert_eq!(t.last_cdb[4], 0x60, "allocation length must be 96 bytes");
    }

    #[test]
    fn raw_response_preserved_full_96_bytes() {
        // raw must carry the entire 96-byte INQUIRY for downstream
        // identity capture/masking — not just the parsed fields.
        let payload = inquiry_payload(b"HL-DT-ST", b"BD-RE BU40N", b"1.04");
        let mut t = ScriptedTransport {
            payload,
            last_cdb: vec![],
        };
        let r = inquiry(&mut t).unwrap();
        assert_eq!(r.raw.len(), 96);
        assert_eq!(r.raw[0], 0x05, "peripheral device type byte preserved");
    }
}
