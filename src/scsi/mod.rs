//! SCSI/MMC command interface.
//!
//! Platform backends are in separate files:
//!   - `linux.rs` — SG_IO ioctl
//!   - `macos.rs` — IOKit SCSITaskDeviceInterface
//!   - `windows.rs` — SPTI (SCSI Pass-Through Interface)

#[cfg(target_os = "linux")]
mod linux;
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
/// kernel mid-layer escalate for 30 s+. See run log in
/// `freemkv-private/docs/TEST_PLAN.md` and the audit at
/// `freemkv-private/docs/audits/2026-04-26-scsi-architecture-research.md`.
///
/// Pre-0.13.21 this was 1.5 s, which forced the kernel mid-layer to
/// time out *normal* reads (cold-start often takes ~1.5 s) and run its
/// full ABORT TASK / LUN RESET / BUS RESET escalation while userspace
/// kept submitting fresh reads. The Initio bridge couldn't drain the
/// resulting command queue and entered a wedge state that only physical
/// replug recovered — proven by the v0.13.18 + v0.13.20 live tests.
pub(crate) const READ_TIMEOUT_MS: u32 = 10_000;

/// Timeout for content READ commands on the recovery path —
/// [`disc::Disc::patch`]'s targeted retries on bad ranges. Doubles
/// the fast-path budget so a sector that fails at 30 s gets one more
/// honest attempt. Matches sg_dd's default per-command timeout
/// (`DEF_TIMEOUT = 60000`).
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
    ///   - `ABORTED COMMAND` (B) — transient; retry usually works
    ///   - `RECOVERED ERROR` (1) / `NO SENSE` (0) — drive is healthy and
    ///     either recovered the data or has no specific fault to report
    ///
    /// `false` for HARDWARE ERROR, DATA PROTECT, UNIT ATTENTION, NOT
    /// READY, ILLEGAL REQUEST, BLANK CHECK, and any unknown key. Used
    /// by [`Error::is_marginal_read`] / `Disc::copy`'s hysteresis
    /// dispatch.
    pub fn is_marginal(&self) -> bool {
        matches!(
            self.sense_key,
            SENSE_KEY_NO_SENSE
                | SENSE_KEY_RECOVERED_ERROR
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
/// uninitialised memory. The minimum useful sense reply per SPC-4 is 8
/// bytes (descriptor) or 14 bytes (fixed, to reach ASC/ASCQ at offsets
/// 12/13).
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
        let asc = if n >= 3 { sense[2] } else { 0 };
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
// remaining platform reset (Linux: SgIoTransport::reset, called only
// from SgIoTransport::open) does pure userspace state cleanup with
// bounded sleeps — no escape-hatch wrapper required.

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
/// **Internal wedge recovery.** When the kernel's response indicates a
/// wedged target — the `0xff` status pattern that means "no answer from
/// the device" — this function transparently escalates: SCSI bus reset
/// → if still wedged → USB device reset (`USBDEVFS_RESET` on Linux) →
/// retry TUR. Callers never see wedge errors and never need to know
/// about the escalation; if even the recovery path can't get a response,
/// `Err(DeviceResetFailed)` surfaces. **No SCSI primitive is exposed to
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

/// Build a READ(10) CDB with the raw read flag.
pub fn build_read10_raw(lba: u32, count: u16) -> [u8; 10] {
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
    //! Unit tests for `parse_sense_key`. Covers both SPC-4 sense data
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
}
