//! Windows SCSI transport via SPTI (SCSI Pass-Through Interface).
//!
//! Sends SCSI commands through DeviceIoControl with IOCTL_SCSI_PASS_THROUGH_DIRECT.
//! Accepts device paths like `D:`, `E:`, `\\.\CdRom0`, or `\\.\D:`.
//!
//! Requires administrator privileges for raw SCSI access.

use crate::error::{Error, Result};
use super::{DataDirection, ScsiResult, ScsiTransport};
use std::path::Path;

// ── Windows constants ──────────────────────────────────────────────────────

const IOCTL_SCSI_PASS_THROUGH_DIRECT: u32 = 0x4D014;
const SCSI_IOCTL_DATA_OUT: u8 = 0;
const SCSI_IOCTL_DATA_IN: u8 = 1;
const SCSI_IOCTL_DATA_UNSPECIFIED: u8 = 2;

const GENERIC_READ: u32 = 0x80000000;
const GENERIC_WRITE: u32 = 0x40000000;
const FILE_SHARE_READ: u32 = 0x00000001;
const FILE_SHARE_WRITE: u32 = 0x00000002;
const OPEN_EXISTING: u32 = 3;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x80;
const INVALID_HANDLE_VALUE: isize = -1;

const K_MAX_CDB_SIZE: usize = 16;
const K_SENSE_SIZE: usize = 32;

// ── SCSI_PASS_THROUGH_DIRECT structure ─────────────────────────────────────

#[repr(C)]
#[allow(non_snake_case)]
struct ScsiPassThroughDirect {
    Length: u16,
    ScsiStatus: u8,
    PathId: u8,
    TargetId: u8,
    Lun: u8,
    CdbLength: u8,
    SenseInfoLength: u8,
    DataIn: u8,
    _padding1: [u8; 3],
    DataTransferLength: u32,
    TimeOutValue: u32,
    DataBuffer: *mut u8,
    SenseInfoOffset: u32,
    Cdb: [u8; K_MAX_CDB_SIZE],
}

#[repr(C)]
struct SptwbDirect {
    spt: ScsiPassThroughDirect,
    sense: [u8; K_SENSE_SIZE],
}

// ── Windows FFI ────────────────────────────────────────────────────────────

extern "system" {
    fn CreateFileW(
        lpFileName: *const u16,
        dwDesiredAccess: u32,
        dwShareMode: u32,
        lpSecurityAttributes: *const std::ffi::c_void,
        dwCreationDisposition: u32,
        dwFlagsAndAttributes: u32,
        hTemplateFile: *const std::ffi::c_void,
    ) -> isize;

    fn CloseHandle(hObject: isize) -> i32;

    fn DeviceIoControl(
        hDevice: isize,
        dwIoControlCode: u32,
        lpInBuffer: *mut std::ffi::c_void,
        nInBufferSize: u32,
        lpOutBuffer: *mut std::ffi::c_void,
        nOutBufferSize: u32,
        lpBytesReturned: *mut u32,
        lpOverlapped: *mut std::ffi::c_void,
    ) -> i32;
}

// ── Transport implementation ───────────────────────────────────────────────

pub struct SptiTransport {
    handle: isize,
}

impl SptiTransport {
    pub fn open(device: &Path) -> Result<Self> {
        let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
            path: device.display().to_string(),
        })?;

        // Normalize device path to \\.\X: format
        let win_path = normalize_device_path(dev_str);
        let wide: Vec<u16> = win_path.encode_utf16().chain(std::iter::once(0)).collect();

        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null(),
            )
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(Error::DeviceNotFound {
                path: format!("{}: cannot open device (run as administrator)", dev_str),
            });
        }

        Ok(SptiTransport { handle })
    }
}

impl Drop for SptiTransport {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.handle); }
    }
}

impl ScsiTransport for SptiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let mut sptwb: SptwbDirect = unsafe { std::mem::zeroed() };

        let cdb_len = cdb.len().min(K_MAX_CDB_SIZE);
        sptwb.spt.Length = std::mem::size_of::<ScsiPassThroughDirect>() as u16;
        sptwb.spt.CdbLength = cdb_len as u8;
        sptwb.spt.SenseInfoLength = K_SENSE_SIZE as u8;
        sptwb.spt.DataIn = match direction {
            DataDirection::None => SCSI_IOCTL_DATA_UNSPECIFIED,
            DataDirection::FromDevice => SCSI_IOCTL_DATA_IN,
            DataDirection::ToDevice => SCSI_IOCTL_DATA_OUT,
        };
        sptwb.spt.DataTransferLength = data.len() as u32;
        sptwb.spt.TimeOutValue = (timeout_ms / 1000).max(1) as u32;
        sptwb.spt.DataBuffer = if data.is_empty() { std::ptr::null_mut() } else { data.as_mut_ptr() };
        sptwb.spt.SenseInfoOffset = std::mem::offset_of!(SptwbDirect, sense) as u32;
        sptwb.spt.Cdb[..cdb_len].copy_from_slice(&cdb[..cdb_len]);

        let buf_size = std::mem::size_of::<SptwbDirect>() as u32;
        let mut bytes_returned: u32 = 0;

        let ok = unsafe {
            DeviceIoControl(
                self.handle,
                IOCTL_SCSI_PASS_THROUGH_DIRECT,
                &mut sptwb as *mut _ as *mut std::ffi::c_void,
                buf_size,
                &mut sptwb as *mut _ as *mut std::ffi::c_void,
                buf_size,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };

        if ok == 0 {
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            });
        }

        if sptwb.spt.ScsiStatus != 0 {
            let sense_key = if sptwb.sense[2] != 0 { sptwb.sense[2] & 0x0F } else { 0 };
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: sptwb.spt.ScsiStatus,
                sense_key,
            });
        }

        let mut sense = [0u8; 32];
        sense.copy_from_slice(&sptwb.sense);

        Ok(ScsiResult {
            status: sptwb.spt.ScsiStatus,
            bytes_transferred: sptwb.spt.DataTransferLength as usize,
            sense,
        })
    }
}

