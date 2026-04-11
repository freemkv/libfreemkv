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

pub const SCSI_INQUIRY: u8             = 0x12;
pub const SCSI_READ_CAPACITY: u8       = 0x25;
pub const SCSI_READ_10: u8             = 0x28;
pub const SCSI_READ_BUFFER: u8         = 0x3C;
pub const SCSI_READ_TOC: u8            = 0x43;
pub const SCSI_GET_CONFIGURATION: u8   = 0x46;
pub const SCSI_SET_CD_SPEED: u8        = 0xBB;
pub const SCSI_SEND_KEY: u8            = 0xA3;
pub const SCSI_REPORT_KEY: u8          = 0xA4;
pub const SCSI_READ_12: u8             = 0xA8;
pub const SCSI_READ_DISC_STRUCTURE: u8 = 0xAD;

/// AACS key class for REPORT KEY / SEND KEY commands.
pub const AACS_KEY_CLASS: u8 = 0x02;

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
pub trait ScsiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult>;

}

// ── Platform-agnostic open ──────────────────────────────────────────────────

/// Open a SCSI transport for the given device path.
/// Selects the right backend for the current platform.
pub fn open(device: &Path) -> Result<Box<dyn ScsiTransport>> {
    #[cfg(target_os = "linux")]
    { Ok(Box::new(linux::SgIoTransport::open(device)?)) }

    #[cfg(target_os = "macos")]
    { Ok(Box::new(macos::MacScsiTransport::open(device)?)) }

    #[cfg(target_os = "windows")]
    { Ok(Box::new(windows::SptiTransport::open(device)?)) }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    { Err(Error::DeviceNotFound { path: format!("{}: unsupported platform", device.display()) }) }
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
    let cdb = [SCSI_GET_CONFIGURATION, 0x02, 0x01, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00];
    let mut buf = [0u8; 16];
    scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 5_000)?;
    Ok(buf.to_vec())
}

/// Build a READ BUFFER CDB.
pub fn build_read_buffer(mode: u8, buffer_id: u8, offset: u32, length: u32) -> [u8; 10] {
    [
        SCSI_READ_BUFFER, mode, buffer_id,
        (offset >> 16) as u8, (offset >> 8) as u8, offset as u8,
        (length >> 16) as u8, (length >> 8) as u8, length as u8,
        0x00,
    ]
}

/// Build a SET CD SPEED CDB.
pub fn build_set_cd_speed(read_speed: u16) -> [u8; 12] {
    [
        SCSI_SET_CD_SPEED, 0x00,
        (read_speed >> 8) as u8, read_speed as u8,
        0xFF, 0xFF,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ]
}

/// Build a READ(10) CDB with the raw read flag.
pub fn build_read10_raw(lba: u32, count: u16) -> [u8; 10] {
    [
        SCSI_READ_10, 0x08,
        (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
        0x00,
        (count >> 8) as u8, count as u8,
        0x00,
    ]
}
