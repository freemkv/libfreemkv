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
#[path = "mod_transport_arg_tests.rs"]
mod transport_arg_tests;

#[cfg(test)]
#[path = "mod_cdb_len_tests.rs"]
mod cdb_len_tests;

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
#[path = "mod_sense_family_tests.rs"]
mod sense_family_tests;

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
    /// Platform device path: `/dev/sgN` (Linux), a stable optical-service
    /// `ioreg:<id>` selector (macOS), `\\.\CdRomN`
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
    inquiry_alloc(scsi, INQUIRY_ALLOC)
}

/// INQUIRY with the standard 36-byte allocation, for bridges that reject longer ones.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn inquiry_standard(scsi: &mut dyn ScsiTransport) -> Result<InquiryResult> {
    inquiry_alloc(scsi, INQUIRY_MIN_LEN)
}

fn inquiry_alloc(scsi: &mut dyn ScsiTransport, alloc: usize) -> Result<InquiryResult> {
    let cdb = [SCSI_INQUIRY, 0x00, 0x00, 0x00, alloc as u8, 0x00];
    let mut buf = vec![0u8; alloc];
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
#[path = "mod_transport_helper_tests.rs"]
mod transport_helper_tests;

#[cfg(test)]
#[path = "mod_align_tests.rs"]
mod align_tests;

#[cfg(test)]
#[path = "mod_parse_sense_tests.rs"]
mod parse_sense_tests;

#[cfg(test)]
#[path = "mod_scsi_sense_predicate_tests.rs"]
mod scsi_sense_predicate_tests;

#[cfg(test)]
#[path = "mod_cdb_builder_tests.rs"]
mod cdb_builder_tests;

#[cfg(test)]
#[path = "mod_inquiry_tests.rs"]
mod inquiry_tests;

#[cfg(test)]
#[path = "mod_sense_progress_tests.rs"]
mod sense_progress_tests;
