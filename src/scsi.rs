//! SCSI/MMC command interface.
//!
//! Platform backends:
//!   - Linux: SG_IO ioctl
//!   - macOS: IOKit SCSI passthrough (planned)
//!   - Windows: SPTI (planned)

use crate::error::{Error, Result};
use std::path::Path;

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

/// Low-level SCSI transport — implemented per platform.
pub trait ScsiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult>;
}

// ─── Linux: SG_IO ───────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
const SG_IO: libc::c_ulong = 0x2285;
#[cfg(target_os = "linux")]
const SG_DXFER_NONE: i32 = -1;
#[cfg(target_os = "linux")]
const SG_DXFER_TO_DEV: i32 = -2;
#[cfg(target_os = "linux")]
const SG_DXFER_FROM_DEV: i32 = -3;

#[cfg(target_os = "linux")]
#[repr(C)]
#[allow(non_camel_case_types)]
struct sg_io_hdr {
    interface_id: i32,
    dxfer_direction: i32,
    cmd_len: u8,
    mx_sb_len: u8,
    iovec_count: u16,
    dxfer_len: u32,
    dxferp: *mut u8,
    cmdp: *const u8,
    sbp: *mut u8,
    timeout: u32,
    flags: u32,
    pack_id: i32,
    usr_ptr: *mut libc::c_void,
    status: u8,
    masked_status: u8,
    msg_status: u8,
    sb_len_wr: u8,
    host_status: u16,
    driver_status: u16,
    resid: i32,
    duration: u32,
    info: u32,
}

#[cfg(target_os = "linux")]
pub struct SgIoTransport {
    fd: i32,
}

#[cfg(target_os = "linux")]
impl SgIoTransport {
    pub fn open(device: &Path) -> Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        let path_bytes = device.as_os_str().as_bytes();
        let mut c_path = Vec::with_capacity(path_bytes.len() + 1);
        c_path.extend_from_slice(path_bytes);
        c_path.push(0);

        let fd = unsafe { libc::open(c_path.as_ptr() as *const libc::c_char, libc::O_RDWR | libc::O_NONBLOCK) };
        if fd < 0 {
            return Err(Error::DeviceNotFound { path: device.display().to_string() });
        }
        Ok(SgIoTransport { fd })
    }
}

#[cfg(target_os = "linux")]
impl Drop for SgIoTransport {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd); }
    }
}

#[cfg(target_os = "linux")]
impl ScsiTransport for SgIoTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let mut sense = [0u8; 32];

        let dxfer_direction = match direction {
            DataDirection::None => SG_DXFER_NONE,
            DataDirection::FromDevice => SG_DXFER_FROM_DEV,
            DataDirection::ToDevice => SG_DXFER_TO_DEV,
        };

        let mut hdr: sg_io_hdr = unsafe { std::mem::zeroed() };
        hdr.interface_id = b'S' as i32;
        hdr.dxfer_direction = dxfer_direction;
        hdr.cmd_len = cdb.len() as u8;
        hdr.mx_sb_len = sense.len() as u8;
        hdr.dxfer_len = data.len() as u32;
        hdr.dxferp = data.as_mut_ptr();
        hdr.cmdp = cdb.as_ptr();
        hdr.sbp = sense.as_mut_ptr();
        hdr.timeout = timeout_ms;

        let ret = unsafe {
            libc::ioctl(self.fd, SG_IO, &mut hdr as *mut sg_io_hdr)
        };

        if ret < 0 {
            return Err(Error::IoError { source: std::io::Error::last_os_error() });
        }

        let bytes_transferred = (data.len() as i32 - hdr.resid) as usize;

        if hdr.status != 0 {
            let sense_key = if hdr.sb_len_wr > 2 { sense[2] & 0x0F } else { 0 };
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: hdr.status,
                sense_key,
            });
        }

        Ok(ScsiResult {
            status: hdr.status,
            bytes_transferred,
            sense,
        })
    }
}

// ─── macOS: IOKit (planned) ─────────────────────────────────────────────────

// TODO: IOKit MMC SCSI passthrough
// Use IOSCSIPeripheralDeviceType05 (MMC device nub)
// Send SCSITaskInterface commands via IOKit user client

// ─── Windows: SPTI (planned) ────────────────────────────────────────────────

// TODO: SCSI Pass Through Interface
// Use CreateFile on \\.\CdRomN
// Send IOCTL_SCSI_PASS_THROUGH_DIRECT

// ─── Platform-agnostic open ─────────────────────────────────────────────────

/// Open a SCSI transport for the given device path.
pub fn open(device: &Path) -> Result<Box<dyn ScsiTransport>> {
    #[cfg(target_os = "linux")]
    { Ok(Box::new(SgIoTransport::open(device)?)) }

    #[cfg(not(target_os = "linux"))]
    { Err(Error::DeviceNotFound { path: format!("{}: platform not yet supported (Linux only)", device.display()) }) }
}

// ─── CDB builders (platform-agnostic) ───────────────────────────────────────

/// SCSI INQUIRY response.
#[derive(Debug, Clone)]
pub struct InquiryResult {
    pub vendor_id: String,
    pub model: String,
    pub firmware: String,
    pub raw: Vec<u8>,
}

/// Send INQUIRY command and parse the standard response fields.
pub fn inquiry(scsi: &mut dyn ScsiTransport) -> Result<InquiryResult> {
    let cdb = [0x12, 0x00, 0x00, 0x00, 0x60, 0x00];
    let mut buf = [0u8; 96];
    scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 5_000)?;

    let vendor = String::from_utf8_lossy(&buf[8..16]).trim().to_string();
    let model = String::from_utf8_lossy(&buf[16..32]).trim().to_string();
    let firmware = String::from_utf8_lossy(&buf[32..36]).trim().to_string();

    Ok(InquiryResult {
        vendor_id: vendor,
        model,
        firmware,
        raw: buf.to_vec(),
    })
}

/// Send GET CONFIGURATION for feature 0x010C (Firmware Information).
pub fn get_config_010c(scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
    let cdb = [0x46, 0x02, 0x01, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00];
    let mut buf = [0u8; 16];
    scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 5_000)?;
    Ok(buf.to_vec())
}

/// Build a READ BUFFER (0x3C) CDB.
pub fn build_read_buffer(mode: u8, buffer_id: u8, offset: u32, length: u32) -> [u8; 10] {
    [
        0x3C, mode, buffer_id,
        (offset >> 16) as u8, (offset >> 8) as u8, offset as u8,
        (length >> 16) as u8, (length >> 8) as u8, length as u8,
        0x00,
    ]
}

/// Build a SET CD SPEED (0xBB) CDB.
pub fn build_set_cd_speed(read_speed: u16) -> [u8; 12] {
    [
        0xBB, 0x00,
        (read_speed >> 8) as u8, read_speed as u8,
        0xFF, 0xFF,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ]
}

/// Build a READ(10) CDB with the raw read flag (0x08).
pub fn build_read10_raw(lba: u32, count: u16) -> [u8; 10] {
    [
        0x28, 0x08,
        (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
        0x00,
        (count >> 8) as u8, count as u8,
        0x00,
    ]
}
