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

// The Linux recovery-thread fd hand-off. Compiled on every platform though
// only `linux.rs` uses it, so its tests run everywhere — the same reasoning as
// `checked_cdb_len`. Rationale and memory ordering: the module's own docs.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) mod fd_handoff;

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

// Timeout for TEST UNIT READY probes (drive_has_disc): cheapest SCSI op,
// no data transfer, 5s is generous. Linux/Windows only — macOS answers
// from the IOKit registry with no SCSI command, so no timeout applies.
#[cfg_attr(target_os = "macos", allow(dead_code))]
pub(crate) const TUR_TIMEOUT_MS: u32 = 5_000;

// Timeout for content READ (READ_10/12) on the ripping fast path
// (`freemkv_engine::recovery::copy`). 10s, calibrated on an LG BU40N + Initio 1618L bridge.
#[cfg(feature = "rip")]
pub(crate) const READ_TIMEOUT_MS: u32 = 10_000;

// Timeout for content READ on the recovery path (`recovery::patch`'s targeted retries). Matches
// sg_dd's 60s ceiling; failed reads usually return in 1-4s.
#[cfg(feature = "rip")]
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

// ── CDB length validation ──────────────────────────────────────────────────

// Validate a CDB against a transport's field width; REJECT (never truncate) an over-length CDB.
pub(crate) fn checked_cdb_len(cdb: &[u8], max: usize) -> Result<u8> {
    if cdb.is_empty() || cdb.len() > max {
        return Err(Error::InvalidCdbLength {
            len: cdb.len(),
            max,
        });
    }
    // `max` is 16 on every backend, so the cast cannot truncate.
    Ok(cdb.len() as u8)
}

/// Timeout every backend applies when a caller passes `timeout_ms == 0`.
pub(crate) const DEFAULT_TIMEOUT_MS: u32 = 60_000;

// A caller's `timeout_ms`, with 0 (no timeout given) mapped to [`DEFAULT_TIMEOUT_MS`].
pub(crate) fn effective_timeout_ms(timeout_ms: u32) -> u32 {
    if timeout_ms == 0 {
        DEFAULT_TIMEOUT_MS
    } else {
        timeout_ms
    }
}

// SPC-4 defines sense data only for CHECK CONDITION; any other non-GOOD status (BUSY,
// RESERVATION CONFLICT, ...) carries none, so it must not become a fabricated 0/0/0 triple.
pub(crate) fn sense_for_status(status: u8, sense: &[u8], sb_len_wr: u8) -> Option<ScsiSense> {
    (status == SCSI_STATUS_CHECK_CONDITION).then(|| parse_sense(sense, sb_len_wr))
}

#[cfg(test)]
mod transport_arg_tests {
    use super::*;

    #[test]
    fn zero_timeout_means_one_default_everywhere() {
        assert_eq!(effective_timeout_ms(0), DEFAULT_TIMEOUT_MS);
        assert_eq!(effective_timeout_ms(1), 1);
        assert_eq!(effective_timeout_ms(12_345), 12_345);
        assert_eq!(spti_timeout_secs(0), 60);
    }

    #[test]
    fn only_check_condition_carries_sense() {
        let mut sense = [0u8; 32];
        sense[0] = 0x70;
        sense[2] = 0x05;
        sense[12] = 0x24;
        let s = sense_for_status(SCSI_STATUS_CHECK_CONDITION, &sense, 32).expect("CHECK CONDITION");
        assert_eq!((s.sense_key, s.asc), (0x05, 0x24));
        // BUSY (08h), RESERVATION CONFLICT (18h), TASK SET FULL (28h): no sense on the wire.
        for status in [0x08u8, 0x18, 0x28] {
            assert!(
                sense_for_status(status, &[0u8; 32], 0).is_none(),
                "{status:#x}"
            );
        }
    }
}

#[cfg(test)]
mod cdb_len_tests {
    use super::*;

    /// The maximum every backend declares (`K_MAX_CDB_SIZE`).
    const MAX: usize = 16;

    // Oversized CDB must be rejected, never truncated. This is the only
    // place the guard can be tested on every platform's CI, since
    // linux.rs/macos.rs/windows.rs each compile on one host only.
    #[test]
    fn oversized_cdb_is_rejected_not_truncated() {
        let cdb = [0u8; MAX + 1];
        match checked_cdb_len(&cdb, MAX) {
            Err(Error::InvalidCdbLength { len, max }) => {
                assert_eq!(len, MAX + 1);
                assert_eq!(max, MAX);
            }
            Err(other) => panic!("expected InvalidCdbLength, got {other:?}"),
            Ok(n) => panic!(
                "over-length CDB was accepted and truncated to {n} bytes — the drive \
                 would execute a DIFFERENT command than the caller asked for"
            ),
        }
    }

    /// A CDB exactly at the field width is legal and passes through untouched.
    #[test]
    fn max_length_cdb_is_accepted() {
        let cdb = [0u8; MAX];
        assert_eq!(checked_cdb_len(&cdb, MAX).ok(), Some(MAX as u8));
    }

    /// Every real CDB length (SPC-4 groups 0-5: 6, 10, 12, 16 bytes) is
    /// accepted and reported verbatim.
    #[test]
    fn in_range_cdb_lengths_pass_through_verbatim() {
        for len in [6usize, 10, 12, 16] {
            let cdb = vec![0u8; len];
            assert_eq!(
                checked_cdb_len(&cdb, MAX).ok(),
                Some(len as u8),
                "CDB of {len} bytes must be accepted verbatim"
            );
        }
    }

    // Empty CDB must be rejected by the shared helper; per-backend guards
    // previously missed it (macOS/Windows passed a zero-length descriptor
    // straight to the driver before this existed).
    #[test]
    fn empty_cdb_is_rejected_by_the_shared_helper() {
        match checked_cdb_len(&[], MAX) {
            Err(Error::InvalidCdbLength { len, max }) => {
                assert_eq!(len, 0);
                assert_eq!(max, MAX);
            }
            Err(other) => panic!("expected InvalidCdbLength, got {other:?}"),
            Ok(n) => panic!(
                "empty CDB accepted with length {n} — every backend would then \
                 index cdb[0] or issue a zero-length command descriptor"
            ),
        }
    }
}

// ── SPC-4 sense keys (§4.5.6 Table 28) ─────────────────────────────────────
// Names match the SCSI spec; predicate methods on [`ScsiSense`] read more
// fluently than raw constant comparisons at call sites.

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

/// Coarse classification of a SCSI sense key, for callers that need to
/// branch on "what kind of failure was this" without a `match` over every
/// `SENSE_KEY_*` constant. Pure hardware-fact translation — no retry policy
/// here; see the recovery/engine layer for what to DO about a given family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenseFamily {
    NotReady,
    Medium,
    Hardware,
    IllegalRequest,
    Other,
}

impl SenseFamily {
    pub fn from_sense_key(sense_key: u8) -> Self {
        match sense_key {
            SENSE_KEY_NOT_READY => SenseFamily::NotReady,
            SENSE_KEY_MEDIUM_ERROR => SenseFamily::Medium,
            SENSE_KEY_HARDWARE_ERROR => SenseFamily::Hardware,
            SENSE_KEY_ILLEGAL_REQUEST => SenseFamily::IllegalRequest,
            _ => SenseFamily::Other,
        }
    }

    /// True for the "wedge family" — some drives (e.g. the BU40N over a
    /// USB-SATA bridge) return Hardware or IllegalRequest sense once their
    /// firmware enters a fast-fail state after sustained bad-media reads.
    pub fn is_wedge_family(self) -> bool {
        matches!(self, SenseFamily::Hardware | SenseFamily::IllegalRequest)
    }
}

#[cfg(test)]
mod sense_family_tests {
    use super::*;

    #[test]
    fn classifies_each_named_sense_key() {
        assert_eq!(
            SenseFamily::from_sense_key(SENSE_KEY_NOT_READY),
            SenseFamily::NotReady
        );
        assert_eq!(
            SenseFamily::from_sense_key(SENSE_KEY_MEDIUM_ERROR),
            SenseFamily::Medium
        );
        assert_eq!(
            SenseFamily::from_sense_key(SENSE_KEY_HARDWARE_ERROR),
            SenseFamily::Hardware
        );
        assert_eq!(
            SenseFamily::from_sense_key(SENSE_KEY_ILLEGAL_REQUEST),
            SenseFamily::IllegalRequest
        );
        assert_eq!(
            SenseFamily::from_sense_key(SENSE_KEY_ABORTED_COMMAND),
            SenseFamily::Other
        );
    }

    #[test]
    fn wedge_family_is_hardware_and_illegal_request_only() {
        assert!(SenseFamily::Hardware.is_wedge_family());
        assert!(SenseFamily::IllegalRequest.is_wedge_family());
        assert!(!SenseFamily::Medium.is_wedge_family());
        assert!(!SenseFamily::NotReady.is_wedge_family());
        assert!(!SenseFamily::Other.is_wedge_family());
    }
}

// ── Sense parsing ───────────────────────────────────────────────────────────

/// Decoded SPC-4 sense triple — the precise reason a SCSI command failed.
///
/// Returned by [`parse_sense`] and embedded inside [`Error::ScsiError`]
/// (`sense: Option<ScsiSense>`). Predicate methods (`is_medium_error`,
/// `is_unit_attention`, `is_marginal`, …) read more fluently than raw
/// `sense_key` comparisons at call sites.
///
/// `Default::default()` and [`ScsiSense::NONE`] both produce the all-zero
/// "no sense info" triple (SPC-4 §4.5.3: empty sense buffer = NO SENSE).
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

    /// `true` when the sense key indicates a *marginal-read* failure — one where a retry or
    /// smaller-granularity read sometimes succeeds: MEDIUM ERROR, NOT READY (dominant on
    /// BU40N), ABORTED COMMAND, RECOVERED ERROR, or NO SENSE. `false` for HARDWARE ERROR, DATA
    /// PROTECT, UNIT ATTENTION, ILLEGAL REQUEST, BLANK CHECK, and unknown keys. Used by
    /// [`Error::is_marginal_read`] / the recovery-copy hysteresis dispatch.
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

    /// `true` for sense `05/6F/03` — MMC "READ OF SCRAMBLED SECTOR WITHOUT
    /// AUTHENTICATION". The drive is enforcing CSS and the bus-auth read gate
    /// is not (or no longer) open. Unlike a bare ILLEGAL REQUEST, this is
    /// positive proof the sector is CSS-scrambled — the CSS crack scan keys on
    /// it to distinguish "encrypted but locked" from "unreadable", and must
    /// never collapse it to "unencrypted".
    pub fn is_css_locked(&self) -> bool {
        self.sense_key == SENSE_KEY_ILLEGAL_REQUEST && self.asc == 0x6F && self.ascq == 0x03
    }

    /// `true` if `sense_key == ABORTED COMMAND (B)` — transient; one
    /// retry is usually safe.
    pub fn is_aborted_command(&self) -> bool {
        self.sense_key == SENSE_KEY_ABORTED_COMMAND
    }
}

// Decode an SPC-4 sense buffer into (sense_key, asc, ascq), handling both descriptor
// (0x72/0x73) and fixed (0x70/0x71) response-code formats; pure function shared by all three
// platform backends.
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

/// The sense-key specific progress indication in a sense buffer (§2.11), or `None`
/// when the sense key is not NOT READY / NO SENSE, SKSV is clear, or the buffer is
/// short. Fixed format (70h/71h): SKSV is byte 15 bit 7, the value bytes 16-17;
/// descriptor format (72h/73h): the sense-key specific descriptor, SKSV + bytes 5-6.
pub(crate) fn parse_sense_progress(sense: &[u8], sb_len_wr: u8) -> Option<u16> {
    // SS-1 Table 18: "NO SENSE or NOT READY | Progress indication"; other sense keys
    // give the field another meaning.
    let n = (sb_len_wr as usize).min(sense.len());
    let sense = &sense[..n];
    let progress_key = |k: u8| matches!(k & 0x0F, SENSE_KEY_NO_SENSE | SENSE_KEY_NOT_READY);
    match sense.first()? & 0x7F {
        0x70 | 0x71 => {
            // SS-1 Table 27: byte 15 "SKSV", bytes 15-17 "SENSE KEY SPECIFIC"; SKSV set
            // "indicates the SENSE KEY SPECIFIC field contains valid information".
            if !progress_key(*sense.get(2)?) || sense.get(15)? & 0x80 == 0 {
                return None;
            }
            Some(u16::from_be_bytes([*sense.get(16)?, *sense.get(17)?]))
        }
        0x72 | 0x73 => {
            if !progress_key(*sense.get(1)?) {
                return None;
            }
            // SS-2 Table 12/17: descriptors follow the 8-byte header; the sense key
            // specific descriptor is "DESCRIPTOR TYPE (02h)", "ADDITIONAL LENGTH (06h)".
            let end = (8 + *sense.get(7)? as usize).min(sense.len());
            let mut at = 8;
            while at + 2 <= end {
                let (kind, len) = (sense[at], sense[at + 1] as usize);
                if kind == 0x02 {
                    let d = sense.get(at..at + 2 + len)?;
                    if d.len() < 7 || d[4] & 0x80 == 0 {
                        return None;
                    }
                    return Some(u16::from_be_bytes([d[5], d[6]]));
                }
                at += 2 + len;
            }
            None
        }
        _ => None,
    }
}

// ── SG_IO driver_status bits ────────────────────────────────────────────────

// DRIVER_SENSE (0x08): SG_IO driver_status bit meaning sense data was
// attached to CHECK CONDITION — mask it off before treating driver_status as a real bus/host problem (Linux-only field).
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

    /// Maximum number of bytes the transport can carry in a single SCSI
    /// data-in transfer. A READ that requests more than this must be split
    /// into chunks by the caller ([`crate::Drive::read`]) — otherwise the
    /// transport fails the whole command.
    ///
    /// Default is a conservative 1 MiB, safe on every platform. Windows overrides with the
    /// adapter's real `MaximumTransferLength`
    fn max_transfer_bytes(&self) -> usize {
        1 << 20
    }

    /// The sense-key specific progress indication of the last command's sense data
    /// (a NOT READY or NO SENSE answer with SKSV set), as a fraction of 65 536.
    /// `wait_ready` counts a rising value as progress (stop design §2.11). Default:
    /// no progress reported.
    fn last_sense_progress(&self) -> Option<u16> {
        None
    }
}

// ── Platform-agnostic open / reset ──────────────────────────────────────────

/// Open a SCSI transport for the given device path.
/// Selects the right backend for the current platform.
pub fn open(device: &Path) -> Result<Box<dyn ScsiTransport>> {
    open_with(device, &crate::halt::Halt::new())
}

// `open` whose waits observe `halt`: the macOS shim's open waits are sliced and cancellable
// (stop design §2.9 M2); SG_IO and SPTI opens do not wait.
pub(crate) fn open_with(device: &Path, halt: &crate::halt::Halt) -> Result<Box<dyn ScsiTransport>> {
    #[cfg(not(target_os = "macos"))]
    let _ = halt;

    #[cfg(target_os = "linux")]
    {
        Ok(Box::new(linux::SgIoTransport::open(device)?))
    }

    #[cfg(target_os = "macos")]
    {
        Ok(Box::new(macos::MacScsiTransport::open(device, halt)?))
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

/// One optical drive on the system. Returned by [`list_drives`]. The
/// fields are populated from a single INQUIRY at enumeration time —
/// no firmware reset, no init.
#[derive(Debug, Clone)]
pub struct DriveInfo {
    /// Platform device path: `/dev/sgN` (Linux), `/dev/diskN` or an opaque
    /// `ioreg:<id>` selector for an empty optical drive (macOS), `\\.\CdRomN`
    /// (Windows).
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
/// devices, filtered to type 5 (CD/DVD/BD), with a single INQUIRY each for
/// vendor/model/firmware. No firmware reset, no `Drive::init`, no disc scan.
///
/// **What it doesn't do**: probe disc presence (use [`drive_has_disc`]) or
/// open a `Drive` for ripping (use [`crate::Drive::open`]) — those are
/// heavier operations invoked once a drive is selected.
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

/// Whether the drive at `path` holds a disc ([`DiscPresence`]).
///
/// Issues TEST UNIT READY (cheapest SCSI op, no data transfer), re-issued after
/// a UNIT ATTENTION (up to 4 times), and classifies the sense (derived from
/// MMC-6 Table F.3). On macOS the answer comes from IOKit and is only `Present`/`Absent`.
///
/// **No internal recovery.** A wedged target surfaces as `Err(Error::ScsiError)` with `status
/// == SCSI_STATUS_TRANSPORT_FAILURE` and `sense: None`, no bus/USB reset, no retry. Other
/// non-NOT-READY sense is also `Err`.
pub fn disc_presence(path: &Path) -> Result<DiscPresence> {
    #[cfg(target_os = "linux")]
    {
        linux::disc_presence(path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::disc_presence(path)
    }

    #[cfg(target_os = "windows")]
    {
        windows::disc_presence(path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = path;
        Err(Error::UnsupportedPlatform {
            target: std::env::consts::OS.to_string(),
        })
    }
}

/// True if the drive at `path` holds a disc: [`disc_presence`] with `Settling`
/// counted as a disc ([`DiscPresence::has_disc`]). Suitable for a poll-loop tick
/// (~50 ms / drive on a healthy bus).
pub fn drive_has_disc(path: &Path) -> Result<bool> {
    disc_presence(path).map(DiscPresence::has_disc)
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

// Timeout for the identification commands (INQUIRY, GET CONFIGURATION 010Ch).
const IDENTIFY_TIMEOUT_MS: u32 = 5_000;
// Standard INQUIRY allocation length, and the shortest reply that holds the identity fields
// (SPC-4 §6.4.2: firmware revision ends at byte 36).
const INQUIRY_ALLOC: usize = 96;
const INQUIRY_MIN_LEN: usize = 36;

/// Send INQUIRY and parse standard response fields. A reply shorter than the identity
/// fields is an error, never a blank identity.
pub fn inquiry(scsi: &mut dyn ScsiTransport) -> Result<InquiryResult> {
    let cdb = [SCSI_INQUIRY, 0x00, 0x00, 0x00, INQUIRY_ALLOC as u8, 0x00];
    let mut buf = [0u8; INQUIRY_ALLOC];
    let r = scsi.execute(
        &cdb,
        DataDirection::FromDevice,
        &mut buf,
        IDENTIFY_TIMEOUT_MS,
    )?;
    if r.bytes_transferred < INQUIRY_MIN_LEN {
        return Err(Error::IoError {
            source: std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
        });
    }

    Ok(InquiryResult {
        vendor_id: String::from_utf8_lossy(&buf[8..16]).trim().to_string(),
        model: String::from_utf8_lossy(&buf[16..32]).trim().to_string(),
        firmware: String::from_utf8_lossy(&buf[32..36]).trim().to_string(),
        raw: buf.to_vec(),
    })
}

/// Send GET CONFIGURATION for feature 0x010C (Firmware Information). Returns
/// the reply (8-byte header + descriptor) cut to what the drive actually sent.
pub fn get_config_010c(scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
    // 8-byte header + 20-byte 010Ch descriptor (MMC-6 §5.3.10).
    const ALLOC: u8 = 28;
    let cdb = [
        SCSI_GET_CONFIGURATION,
        0x02,
        0x01,
        0x0C,
        0x00,
        0x00,
        0x00,
        0x00,
        ALLOC,
        0x00,
    ];
    let mut buf = [0u8; ALLOC as usize];
    let r = scsi.execute(
        &cdb,
        DataDirection::FromDevice,
        &mut buf,
        IDENTIFY_TIMEOUT_MS,
    )?;
    Ok(buf[..gc_reply_len(&buf, r.bytes_transferred)].to_vec())
}

/// Valid length of a GET CONFIGURATION reply: the transfer count, clamped to the
/// buffer and to the header's Data Length + 4 (MMC-6 §5.3.1).
pub(crate) fn gc_reply_len(buf: &[u8], transferred: usize) -> usize {
    let end = transferred.min(buf.len());
    match buf.get(..4) {
        Some(h) if end >= 4 => {
            let data_len = u32::from_be_bytes([h[0], h[1], h[2], h[3]]) as usize;
            end.min(data_len.saturating_add(4))
        }
        _ => end,
    }
}

/// The descriptor for `feature` in a GET CONFIGURATION (RT=10b) reply, feature
/// header included, bounded by [`gc_reply_len`] and its Additional Length. `None`
/// when the drive answered header-only (feature absent) or with another feature.
#[cfg_attr(not(feature = "rip"), allow(dead_code))]
pub(crate) fn gc_feature_descriptor(buf: &[u8], transferred: usize, feature: u16) -> Option<&[u8]> {
    let reply = &buf[..gc_reply_len(buf, transferred)];
    let desc = reply.get(8..)?;
    if desc.len() < 4 || u16::from_be_bytes([desc[0], desc[1]]) != feature {
        return None;
    }
    Some(&desc[..desc.len().min(4 + usize::from(desc[3]))])
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

/// True if INQUIRY byte 0's peripheral device type (low 5 bits; the high 3
/// are the qualifier) is 05h, an MMC optical drive (SPC-4 §6.4.2).
#[cfg_attr(not(any(feature = "rip", target_os = "windows")), allow(dead_code))]
pub(crate) fn is_optical_peripheral(inquiry: &[u8]) -> bool {
    const PERIPHERAL_TYPE_MASK: u8 = 0x1F;
    const PERIPHERAL_TYPE_OPTICAL: u8 = 0x05;
    inquiry
        .first()
        .is_some_and(|b| b & PERIPHERAL_TYPE_MASK == PERIPHERAL_TYPE_OPTICAL)
}

/// PREVENT ALLOW MEDIUM REMOVAL (1Eh): `Some` of CDB byte 4 bits 1:0 (MMC-6:
/// bit 1 Persistent, bit 0 Prevent; SPC-4 §6.13), `None` for other commands.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn prevent_allow_request(cdb: &[u8]) -> Option<u8> {
    const PREVENT_ALLOW_MEDIUM_REMOVAL: u8 = 0x1E;
    match cdb {
        [PREVENT_ALLOW_MEDIUM_REMOVAL, _, _, _, b4, ..] => Some(b4 & 0b11),
        _ => None,
    }
}

/// Whether a drive holds a disc, from TEST UNIT READY sense. The split is our
/// reading of MMC-6 Table F.3 (derived, not spec text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DiscPresence {
    /// A medium is loaded: GOOD, or a NOT READY state that only exists with one
    /// (04/02, 04/04, 04/07, 04/08, 0Ch, 30h other than cleaning cartridges).
    Present,
    /// NOT READY 3Ah, MEDIUM NOT PRESENT (tray open or closed and empty), or a
    /// cleaning cartridge / cleaning failure (30/03, 30/07): nothing to rip.
    Absent,
    /// Not ready for a reason that does not settle presence: 04/01 (a mounted
    /// disc spinning up or changing Format-layer, MMC-6 §6.22.3), 04/00, 04/03,
    /// 04/09, 3Eh, or another NOT READY. Poll again.
    Settling,
}

impl DiscPresence {
    /// The [`drive_has_disc`] answer: `Settling` counts as a disc, so a poll loop
    /// never drops a session while a mounted disc re-spins.
    pub fn has_disc(self) -> bool {
        self != DiscPresence::Absent
    }
}

/// [`DiscPresence`] from TEST UNIT READY, `tur` issuing one TUR (Ok = GOOD).
/// UNIT ATTENTIONs are re-polled (up to 4); non-NOT-READY sense is `Err`.
#[cfg_attr(not(any(target_os = "linux", target_os = "windows")), allow(dead_code))]
pub(crate) fn tur_disc_presence(mut tur: impl FnMut() -> Result<()>) -> Result<DiscPresence> {
    const MAX_ATTENTION_RETRIES: u32 = 4;
    let mut retries = 0;
    loop {
        let Err(e) = tur() else {
            return Ok(DiscPresence::Present);
        };
        let Some(s) = e.scsi_sense().copied() else {
            return Err(e);
        };
        match s.sense_key {
            SENSE_KEY_NOT_READY => return Ok(not_ready_presence(s.asc, s.ascq)),
            SENSE_KEY_UNIT_ATTENTION if retries < MAX_ATTENTION_RETRIES => retries += 1,
            _ => return Err(e),
        }
    }
}

// NOT READY ASC/ASCQ -> presence, derived from MMC-6 Tables F.3 / F.10.
fn not_ready_presence(asc: u8, ascq: u8) -> DiscPresence {
    match (asc, ascq) {
        (0x3A, _) => DiscPresence::Absent,
        // Initializing cmd required, format / operation / long write in progress.
        (0x04, 0x02 | 0x04 | 0x07 | 0x08) => DiscPresence::Present,
        // Write error recovery needed, defects in error window.
        (0x0C, 0x07 | 0x0F) => DiscPresence::Present,
        // Cleaning cartridge installed / cleaning failure: no disc to rip.
        (0x30, 0x03 | 0x07) => DiscPresence::Absent,
        // Incompatible / unreadable medium installed.
        (0x30, _) => DiscPresence::Present,
        _ => DiscPresence::Settling,
    }
}

/// Whether a no-sysfs (unfiltered) sg node belongs in `list_drives`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub(crate) enum NodeProbe {
    /// INQUIRY says peripheral type 05h.
    Optical,
    /// INQUIRY says another peripheral type.
    NotOptical,
    /// Opened but INQUIRY failed, or open failed with e.g. EBUSY.
    Unresponsive,
    /// open() found no device (ENOENT, ENXIO, ENODEV).
    Absent,
    /// open() refused (EACCES, EPERM): nothing says it is optical.
    Denied,
}

/// [`NodeProbe`] for an sg node whose open(2) failed with `errno`.
#[cfg(unix)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn open_failure_probe(errno: Option<i32>) -> NodeProbe {
    match errno {
        Some(libc::ENOENT | libc::ENXIO | libc::ENODEV) => NodeProbe::Absent,
        Some(libc::EACCES | libc::EPERM) => NodeProbe::Denied,
        _ => NodeProbe::Unresponsive,
    }
}

/// Kept unless definitively not an optical drive or not there: a wedged or busy
/// drive must list as present (autorip: "unresponsive"), not as unplugged.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn keep_unfiltered_node(p: NodeProbe) -> bool {
    matches!(p, NodeProbe::Optical | NodeProbe::Unresponsive)
}

/// Upper bound on an adapter AlignmentMask (page alignment).
pub(crate) const MAX_ALIGNMENT_MASK: usize = 0xFFF;

/// Adapter-reported SPTI AlignmentMask made safe to size a bounce buffer with:
/// widened to the next 2^n-1 if malformed, capped at [`MAX_ALIGNMENT_MASK`].
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(crate) fn sanitize_alignment_mask(raw: u32) -> usize {
    let smeared = match raw.checked_ilog2() {
        Some(top) => u32::MAX >> (31 - top),
        None => 0,
    };
    (smeared as usize).min(MAX_ALIGNMENT_MASK)
}

// Round `p` up to satisfy an SPTI AlignmentMask (`(p + mask) & !mask`); mask=0 means no
// alignment requirement.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(crate) fn align_up(p: usize, mask: usize) -> usize {
    (p + mask) & !mask
}

/// SPTI TimeOutValue: whole seconds, rounded up, never wrapping; 0 ms is the default timeout.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(crate) fn spti_timeout_secs(timeout_ms: u32) -> u32 {
    effective_timeout_ms(timeout_ms).div_ceil(1000)
}

#[cfg(test)]
mod transport_helper_tests {
    use super::*;

    fn sense(sense_key: u8, asc: u8, ascq: u8) -> ScsiSense {
        ScsiSense {
            sense_key,
            asc,
            ascq,
        }
    }

    fn tur_err(sense_key: u8, asc: u8, ascq: u8) -> Result<()> {
        Err(Error::ScsiError {
            opcode: SCSI_TEST_UNIT_READY,
            status: SCSI_STATUS_CHECK_CONDITION,
            sense: Some(sense(sense_key, asc, ascq)),
        })
    }

    // Runs `tur_disc_presence` over scripted TUR outcomes; returns the verdict
    // and how many TURs it issued.
    fn presence(replies: Vec<Result<()>>) -> (Result<DiscPresence>, usize) {
        let mut replies = replies.into_iter();
        let mut calls = 0;
        let r = tur_disc_presence(|| {
            calls += 1;
            replies.next().unwrap_or(Ok(()))
        });
        (r, calls)
    }

    fn one(sense_key: u8, asc: u8, ascq: u8) -> Result<DiscPresence> {
        presence(vec![tur_err(sense_key, asc, ascq)]).0
    }

    // MMC-6 Table F.3: only 3Ah is MEDIUM NOT PRESENT. 04/01 is a mounted disc
    // spinning up or changing Format-layer (§6.22.3), so it is Settling, never
    // Absent; states that only exist with a medium loaded are Present.
    #[test]
    fn tur_disc_presence_follows_the_mmc6_readiness_table() {
        use DiscPresence::*;
        assert!(matches!(presence(vec![Ok(())]), (Ok(Present), 1)));
        for ascq in [0x00, 0x01, 0x02] {
            assert!(matches!(one(2, 0x3A, ascq), Ok(Absent)), "3A/{ascq:02x}");
        }
        for (asc, ascq) in [(0x04, 0x02), (0x04, 0x04), (0x04, 0x07), (0x04, 0x08)] {
            assert!(
                matches!(one(2, asc, ascq), Ok(Present)),
                "{asc:02x}/{ascq:02x}"
            );
        }
        for (asc, ascq) in [(0x0C, 0x07), (0x0C, 0x0F), (0x30, 0x00), (0x30, 0x02)] {
            assert!(
                matches!(one(2, asc, ascq), Ok(Present)),
                "{asc:02x}/{ascq:02x}"
            );
        }
        for (asc, ascq) in [(0x04, 0x00), (0x04, 0x01), (0x04, 0x03), (0x04, 0x09)] {
            assert!(
                matches!(one(2, asc, ascq), Ok(Settling)),
                "{asc:02x}/{ascq:02x}"
            );
        }
        assert!(matches!(one(2, 0x3E, 0x00), Ok(Settling)), "3E/00");
        // A cleaning cartridge (30/03) or cleaning failure (30/07) is nothing to
        // rip and does not resolve like a spin-up: no disc.
        for ascq in [0x03, 0x07] {
            assert!(matches!(one(2, 0x30, ascq), Ok(Absent)), "30/{ascq:02x}");
        }
        assert!(one(3, 0x11, 0).is_err(), "not a NOT READY key");
    }

    // A transport failure carries no sense: it must bubble up on the first
    // poll (a wedged drive is not "no disc"), never map to Absent or retry.
    #[test]
    fn tur_disc_presence_bubbles_a_no_sense_transport_failure() {
        let wedge = Err(Error::ScsiError {
            opcode: SCSI_TEST_UNIT_READY,
            status: SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
        let (r, calls) = presence(vec![wedge]);
        assert!(r.is_err(), "no-sense failure must be an error, got {r:?}");
        assert_eq!(calls, 1, "a wedge is not retried");
    }

    // A UNIT ATTENTION says nothing about the medium, and several can be queued
    // (06/29 reset, then 06/28 medium change): re-issue TUR up to 4 times.
    #[test]
    fn tur_disc_presence_retries_queued_unit_attentions() {
        use DiscPresence::*;
        let ua = |asc| tur_err(6, asc, 0);
        assert!(matches!(
            presence(vec![ua(0x28), tur_err(2, 0x3A, 0)]),
            (Ok(Absent), 2)
        ));
        assert!(matches!(
            presence(vec![ua(0x29), ua(0x28), Ok(())]),
            (Ok(Present), 3)
        ));
        assert!(matches!(
            presence(vec![ua(0x28), tur_err(2, 4, 1)]),
            (Ok(Settling), 2)
        ));
        let five = (0..5).map(|_| ua(0x28)).collect();
        assert!(
            matches!(presence(five), (Err(_), 5)),
            "attentions never clear"
        );
    }

    // drive_has_disc keeps its bool: Settling reads as "still there", so a poll
    // loop never tears down a session while a mounted disc re-spins.
    #[test]
    fn drive_has_disc_maps_settling_to_present() {
        assert!(DiscPresence::Present.has_disc());
        assert!(DiscPresence::Settling.has_disc());
        assert!(!DiscPresence::Absent.has_disc());
    }

    // Without sysfs, an sg node stays listed unless it is definitively not an
    // optical drive or not there: a wedged or busy drive must read as present
    // but unresponsive (autorip's "firmware unresponsive"), not as unplugged.
    #[test]
    fn unfiltered_sg_node_is_dropped_only_when_definitively_not_a_drive() {
        assert!(keep_unfiltered_node(NodeProbe::Optical));
        assert!(keep_unfiltered_node(NodeProbe::Unresponsive));
        assert!(!keep_unfiltered_node(NodeProbe::NotOptical));
        assert!(!keep_unfiltered_node(NodeProbe::Absent));
        // Without sysfs a non-root user sees EACCES on every root-owned sg node
        // (disks, tapes): unprovable as optical, so not listed.
        assert!(!keep_unfiltered_node(NodeProbe::Denied));
    }

    // How a failed open(2) of an unfiltered sg node is classified.
    #[cfg(unix)]
    #[test]
    fn unfiltered_open_errno_classification() {
        use NodeProbe::*;
        let probe = |e| open_failure_probe(Some(e));
        assert!(matches!(probe(libc::ENOENT), Absent));
        assert!(matches!(probe(libc::ENXIO), Absent));
        assert!(matches!(probe(libc::ENODEV), Absent));
        assert!(matches!(probe(libc::EACCES), Denied));
        assert!(matches!(probe(libc::EPERM), Denied));
        assert!(matches!(probe(libc::EBUSY), Unresponsive));
        assert!(matches!(open_failure_probe(None), Unresponsive));
    }

    // Drop unlocks the tray only for a transport that issued a PREVENT; the
    // CDB decode is what tracks that.
    #[test]
    fn prevent_allow_request_decodes_only_1eh() {
        // SPC-4 §6.13 PREVENT field, bits 1:0: 00 allow, 01 prevent,
        // 10 persistent allow, 11 persistent prevent. Upper bits are reserved.
        assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x00, 0]), Some(0b00));
        assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x01, 0]), Some(0b01));
        assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x02, 0]), Some(0b10));
        assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x03, 0]), Some(0b11));
        assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0xFD, 0]), Some(0b01));
        assert_eq!(prevent_allow_request(&[0x12, 0, 0, 0, 0x01, 0]), None);
        assert_eq!(prevent_allow_request(&[0x1E, 0, 0]), None, "short CDB");
    }

    // AlignmentMask is adapter-reported: it must be 2^n-1 and bounded, or a bogus
    // 0xFFFF_FFFF sizes every bounce buffer at data.len() + 4 GiB.
    #[test]
    fn alignment_mask_is_validated_and_capped() {
        assert_eq!(sanitize_alignment_mask(0), 0);
        assert_eq!(sanitize_alignment_mask(0x3), 0x3);
        assert_eq!(sanitize_alignment_mask(0x1FF), 0x1FF);
        assert_eq!(sanitize_alignment_mask(0x5), 0x7, "not 2^n-1: widen");
        assert_eq!(sanitize_alignment_mask(0xFFFF_FFFF), MAX_ALIGNMENT_MASK);
        assert_eq!(sanitize_alignment_mask(0x0010_0000), MAX_ALIGNMENT_MASK);
    }

    // SPTI's TimeOutValue is whole seconds, rounded up, never 0 — and a huge
    // timeout must saturate, not wrap to 1 s (or panic in debug).
    #[test]
    fn spti_timeout_rounds_up_without_overflow() {
        assert_eq!(spti_timeout_secs(0), 60);
        assert_eq!(spti_timeout_secs(1_500), 2);
        assert_eq!(spti_timeout_secs(5_000), 5);
        assert_eq!(spti_timeout_secs(u32::MAX), u32::MAX.div_ceil(1000));
    }

    // A GOOD reply that moved no data must not parse as a blank identity.
    #[test]
    fn inquiry_rejects_a_short_reply_and_parses_a_full_one() {
        struct Inq(usize);
        impl ScsiTransport for Inq {
            fn execute(
                &mut self,
                _cdb: &[u8],
                _dir: DataDirection,
                data: &mut [u8],
                _timeout_ms: u32,
            ) -> Result<ScsiResult> {
                let mut reply = [0u8; 36];
                reply[0] = 0x05;
                reply[8..32].fill(b' ');
                reply[8..16].copy_from_slice(b"HL-DT-ST");
                reply[16..27].copy_from_slice(b"BD-RE BU40N");
                reply[32..36].copy_from_slice(b"1.03");
                data[..36].copy_from_slice(&reply);
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: self.0,
                    sense: [0u8; 32],
                })
            }
        }
        assert!(inquiry(&mut Inq(0)).is_err(), "zero-byte reply");
        assert!(inquiry(&mut Inq(35)).is_err(), "truncated before firmware");
        let r = inquiry(&mut Inq(36)).unwrap();
        assert_eq!(
            (r.vendor_id.as_str(), r.model.as_str()),
            ("HL-DT-ST", "BD-RE BU40N")
        );
        assert_eq!(r.firmware, "1.03");
    }

    // Feature 010Ch is an 8-byte header plus a 20-byte descriptor (Additional
    // Length 10h): the allocation length must cover all 28 bytes.
    #[test]
    fn get_config_010c_returns_the_whole_descriptor() {
        struct Fw(Vec<u8>);
        impl ScsiTransport for Fw {
            fn execute(
                &mut self,
                cdb: &[u8],
                _dir: DataDirection,
                data: &mut [u8],
                _timeout_ms: u32,
            ) -> Result<ScsiResult> {
                let alloc = usize::from(u16::from_be_bytes([cdb[7], cdb[8]]));
                let mut reply = vec![0, 0, 0, 24, 0, 0, 0, 0, 0x01, 0x0C, 0x00, 0x10];
                reply.extend_from_slice(b"202101311259\0\0\0\0");
                let n = reply.len().min(alloc).min(data.len());
                data[..n].copy_from_slice(&reply[..n]);
                self.0 = cdb.to_vec();
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: n,
                    sense: [0u8; 32],
                })
            }
        }
        let mut t = Fw(Vec::new());
        let v = get_config_010c(&mut t).unwrap();
        assert_eq!(v.len(), 28, "header + full 010Ch descriptor");
        assert_eq!(&v[12..24], b"202101311259", "date through the minute");
    }
}

#[cfg(test)]
mod align_tests {
    use super::align_up;

    #[test]
    fn mask_zero_is_identity() {
        // AlignmentMask 0 (USB optical bridges) — no alignment required.
        for p in [0usize, 1, 2, 3, 7, 8, 13, 4096, 0x7fff_ffff] {
            assert_eq!(align_up(p, 0), p);
        }
    }

    #[test]
    fn already_aligned_is_unchanged() {
        // DWORD (mask 3): multiples of 4 are already aligned.
        assert_eq!(align_up(0, 3), 0);
        assert_eq!(align_up(4, 3), 4);
        assert_eq!(align_up(8, 3), 8);
        // 8-byte (mask 7): multiples of 8.
        assert_eq!(align_up(0, 7), 0);
        assert_eq!(align_up(16, 7), 16);
    }

    #[test]
    fn rounds_up_to_next_boundary() {
        // mask 1 (2-byte): odd → next even.
        assert_eq!(align_up(1, 1), 2);
        assert_eq!(align_up(3, 1), 4);
        // mask 3 (DWORD): 1,2,3 → 4; 5,6,7 → 8.
        assert_eq!(align_up(1, 3), 4);
        assert_eq!(align_up(2, 3), 4);
        assert_eq!(align_up(3, 3), 4);
        assert_eq!(align_up(5, 3), 8);
        // mask 7 (8-byte): 1..=7 → 8; 9 → 16.
        assert_eq!(align_up(1, 7), 8);
        assert_eq!(align_up(7, 7), 8);
        assert_eq!(align_up(9, 7), 16);
    }

    #[test]
    fn result_always_satisfies_mask() {
        for &mask in &[0usize, 1, 3, 7, 15, 31, 63] {
            for p in 0usize..256 {
                let a = align_up(p, mask);
                assert!(a >= p, "align_up({p},{mask})={a} went backwards");
                assert_eq!(a & mask, 0, "align_up({p},{mask})={a} not aligned");
                // Smallest such value: anything in (p-1-mask, a) would be < p
                // or unaligned; check a - p never exceeds mask.
                assert!(a - p <= mask, "align_up({p},{mask})={a} overshot");
            }
        }
    }
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
        // Byte 1's upper nibble is reserved in descriptor format; the
        // parser masks &0x0F unconditionally, so set garbage there and
        // confirm it doesn't leak into the decoded sense key.
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
        // Fixed format needs n>=13 for ASC, n>=14 for ASCQ. A short reply
        // (e.g. an 8-byte sense, common from some bridges) must yield
        // asc=ascq=0, never read past the written region.
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
    //! §4.5.6 Table 28 sense keys. These drive `freemkv_engine::recovery::copy` hysteresis
    //! and `freemkv_engine::recovery::patch` routing; a misclassification here silently
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
        type SenseCase = (u8, fn(&ScsiSense) -> bool);
        let cases: &[SenseCase] = &[
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

    // is_css_locked must require the exact 05/6F/03 triple (all three
    // fields ANDed, not ORed) — the CSS crack scan relies on it to
    // distinguish "encrypted but locked" from "unreadable".
    #[test]
    fn is_css_locked_requires_exact_key_asc_ascq_triple() {
        // The real signature: true.
        assert!(
            ScsiSense {
                sense_key: SENSE_KEY_ILLEGAL_REQUEST,
                asc: 0x6F,
                ascq: 0x03,
            }
            .is_css_locked()
        );
        // Right key, wrong ASC only -> must be false (rules out `||`
        // between key and asc, and rules out the `true` constant mutant).
        assert!(
            !ScsiSense {
                sense_key: SENSE_KEY_ILLEGAL_REQUEST,
                asc: 0x00,
                ascq: 0x03,
            }
            .is_css_locked()
        );
        // Right key, right ASC, wrong ASCQ -> must be false (rules out `||`
        // between asc and ascq).
        assert!(
            !ScsiSense {
                sense_key: SENSE_KEY_ILLEGAL_REQUEST,
                asc: 0x6F,
                ascq: 0x00,
            }
            .is_css_locked()
        );
        // Right ASC/ASCQ but wrong key (e.g. a bare ILLEGAL REQUEST with
        // unrelated ASC/ASCQ would already fail above; here flip the key
        // instead) -> must be false.
        assert!(
            !ScsiSense {
                sense_key: SENSE_KEY_MEDIUM_ERROR,
                asc: 0x6F,
                ascq: 0x03,
            }
            .is_css_locked()
        );
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
        // bytes of the u32, so a non-zero top byte must not leak into
        // the encoded field. Documents the actual wire contract.
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
    //!
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

#[cfg(test)]
mod sense_progress_tests {
    use super::*;
    use crate::spec::stop::{SS_1_SENSE_PROGRESS, SS_2_DESCRIPTOR_SENSE};

    // Fixed format (70h): sense key, ASC/ASCQ, and the SKSV + progress bytes 15-17.
    fn fixed(key: u8, sksv: bool, progress: u16) -> Vec<u8> {
        let mut s = vec![0u8; 18];
        s[0] = 0x70;
        s[2] = key;
        s[7] = 10;
        s[12] = 0x04;
        s[13] = 0x01;
        s[15] = if sksv { 0x80 } else { 0x00 };
        s[16..18].copy_from_slice(&progress.to_be_bytes());
        s
    }

    // Descriptor format (72h) with `before` other descriptors ahead of the
    // sense-key specific one (type 02h, length 06h).
    fn descriptor(key: u8, sksv: bool, progress: u16, before: usize) -> Vec<u8> {
        let mut s = vec![0x72, key, 0x04, 0x01, 0, 0, 0, 0];
        for _ in 0..before {
            s.extend_from_slice(&[0x00, 0x0A, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        }
        s.extend_from_slice(&[0x02, 0x06, 0, 0, if sksv { 0x80 } else { 0 }]);
        s.extend_from_slice(&progress.to_be_bytes());
        s.push(0);
        s[7] = (s.len() - 8) as u8;
        s
    }

    fn parse(s: &[u8]) -> Option<u16> {
        parse_sense_progress(s, s.len() as u8)
    }

    /// LD12e: fixed and descriptor formats; SKSV set/clear; NOT READY vs other keys;
    /// short buffers → `None`.
    #[test]
    fn parse_sense_progress_fixed_and_descriptor() {
        assert_eq!(
            parse(&fixed(SENSE_KEY_NOT_READY, true, 0x1234)),
            Some(0x1234)
        );
        assert_eq!(
            parse(&descriptor(SENSE_KEY_NOT_READY, true, 0x1234, 0)),
            Some(0x1234)
        );
        let f = fixed(SENSE_KEY_NOT_READY, true, 7);
        for n in 0..f.len() {
            assert_eq!(parse_sense_progress(&f, n as u8), None, "fixed, {n} bytes");
        }
        let d = descriptor(SENSE_KEY_NOT_READY, true, 7, 0);
        for n in 0..d.len() - 1 {
            assert_eq!(
                parse_sense_progress(&d, n as u8),
                None,
                "descriptor, {n} bytes"
            );
        }
    }

    /// SP1: corroborated by SS-1 (SKSV set to one "indicates the SENSE KEY SPECIFIC
    /// field contains valid information"): SKSV clear → `None`, whatever bytes 16-17 hold.
    #[test]
    fn progress_ignored_when_sksv_clear() {
        assert!(
            SS_1_SENSE_PROGRESS
                .text
                .contains("(SKSV) bit set to one indicates")
        );
        assert_eq!(parse(&fixed(SENSE_KEY_NOT_READY, false, 0xFFFF)), None);
        assert_eq!(
            parse(&descriptor(SENSE_KEY_NOT_READY, false, 0xFFFF, 0)),
            None
        );
    }

    /// SP2: corroborated by SS-1 Table 18 ("NO SENSE or NOT READY … Progress
    /// indication"): the same bytes under another sense key are not progress.
    #[test]
    fn progress_only_for_not_ready_and_no_sense() {
        assert!(
            SS_1_SENSE_PROGRESS
                .text
                .starts_with("NO SENSE or NOT READY")
        );
        assert_eq!(parse(&fixed(SENSE_KEY_NO_SENSE, true, 9)), Some(9));
        for key in [
            SENSE_KEY_MEDIUM_ERROR,
            SENSE_KEY_ILLEGAL_REQUEST,
            SENSE_KEY_UNIT_ATTENTION,
        ] {
            assert_eq!(parse(&fixed(key, true, 9)), None, "fixed key {key}");
            assert_eq!(
                parse(&descriptor(key, true, 9, 0)),
                None,
                "descriptor key {key}"
            );
        }
    }

    /// SP3: corroborated by SS-1 ("a numerator that has 65 536 (10000h) as its
    /// denominator"): big-endian, 0x8000 is half, 0xFFFF the maximum.
    #[test]
    fn progress_big_endian_fraction() {
        assert!(
            SS_1_SENSE_PROGRESS
                .text
                .contains("65 536 (10000h) as its denominator")
        );
        assert_eq!(
            parse(&fixed(SENSE_KEY_NOT_READY, true, 0x8000)),
            Some(32768)
        );
        assert_eq!(
            parse(&fixed(SENSE_KEY_NOT_READY, true, 0xFFFF)),
            Some(u16::MAX)
        );
        let mut s = fixed(SENSE_KEY_NOT_READY, true, 0);
        s[16] = 0x01;
        assert_eq!(parse(&s), Some(0x0100), "byte 16 is the MSB");
    }

    /// SP4: corroborated by SS-2: the sense key specific descriptor (type 02h) is
    /// found at any position in the descriptor list; a truncated one → `None`.
    #[test]
    fn descriptor_format_progress_descriptor() {
        assert!(
            SS_2_DESCRIPTOR_SENSE
                .text
                .contains("72h (current errors) and 73h")
        );
        for before in 0..3 {
            let d = descriptor(SENSE_KEY_NOT_READY, true, 0x0203, before);
            assert_eq!(parse(&d), Some(0x0203), "{before} descriptors ahead");
            let mut deferred = d.clone();
            deferred[0] = 0x73;
            assert_eq!(parse(&deferred), Some(0x0203));
        }
        let mut cut = descriptor(SENSE_KEY_NOT_READY, true, 0x0203, 1);
        cut.truncate(cut.len() - 3);
        assert_eq!(parse(&cut), None, "truncated descriptor");
    }
}
