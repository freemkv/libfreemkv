//! macOS SCSI transport: IOKit SCSITaskDeviceInterface with exclusive access.
//!
//! Single dispatch path: **all** CDBs (INQUIRY, READ, REPORT KEY, etc.) go
//! through `SCSITaskDeviceInterface::ExecuteTaskSync` — 1:1 with the Linux
//! SG_IO backend. The C shim (`macos_shim.c`) handles:
//!
//! 1. `diskutil unmountDisk force` on the target device only
//! 2. Find `IOBDServices` matching the requested BSD name (walks IOKit
//!    registry: IOBDServices → IOBDBlockStorageDriver → IOMedia → BSD Name)
//! 3. Create `MMCDeviceInterface` → `SCSITaskDeviceInterface`
//! 4. `ObtainExclusiveAccess`
//! 5. Raw CDB dispatch via `CreateSCSITask` + `ExecuteTaskSync`
//!
//! Drive enumeration (`list_drives`) uses the IOKit registry directly via
//! `shim_list_drives` — no exclusive access, no SCSI commands, no unmounts.

use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use std::path::Path;

const K_SENSE_DATA_SIZE: usize = 32;

#[repr(C)]
#[derive(Copy, Clone)]
struct ShimDriveInfo {
    bsd_name: [u8; 32],
    vendor: [u8; 32],
    model: [u8; 48],
    firmware: [u8; 16],
}

unsafe extern "C" {
    fn shim_open_exclusive(bsd_name: *const u8) -> i32;
    fn shim_close();
    fn shim_execute(
        cdb: *const u8,
        cdb_len: u8,
        buf: *mut u8,
        buf_len: u32,
        data_in: i32,
        sense_out: *mut u8,
        sense_len: u32,
        task_status_out: *mut u8,
        transfer_count: *mut u64,
    ) -> i32;
    fn shim_list_drives(out: *mut ShimDriveInfo, max_entries: i32) -> i32;
}

pub struct MacScsiTransport {
    _bsd_name: String,
}

unsafe impl Send for MacScsiTransport {}

impl MacScsiTransport {
    pub fn open(device: &Path) -> Result<Self> {
        let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
            path: device.display().to_string(),
        })?;

        let bsd_name = if let Some(rest) = dev_str.strip_prefix("/dev/r") {
            rest
        } else if let Some(rest) = dev_str.strip_prefix("/dev/") {
            rest
        } else {
            dev_str
        };

        let mut bsd_c = bsd_name.as_bytes().to_vec();
        bsd_c.push(0);

        let rc = unsafe { shim_open_exclusive(bsd_c.as_ptr()) };
        if rc != 0 {
            return Err(Error::DeviceNotFound {
                path: bsd_name.to_string(),
            });
        }

        Ok(MacScsiTransport {
            _bsd_name: bsd_name.to_string(),
        })
    }
}

impl Drop for MacScsiTransport {
    fn drop(&mut self) {
        unsafe { shim_close() };
    }
}

impl ScsiTransport for MacScsiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let data_in = match direction {
            DataDirection::FromDevice => 1,
            DataDirection::ToDevice => 0,
            DataDirection::None => 0,
        };

        let mut sense = [0u8; K_SENSE_DATA_SIZE];
        let mut task_status: u8 = 0xFF;
        let mut transfer_count: u64 = 0;

        let kr = unsafe {
            shim_execute(
                cdb.as_ptr(),
                cdb.len() as u8,
                data.as_mut_ptr(),
                data.len() as u32,
                data_in,
                sense.as_mut_ptr(),
                K_SENSE_DATA_SIZE as u32,
                &mut task_status,
                &mut transfer_count,
            )
        };

        if kr != 0 {
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        if task_status != 0 {
            let parsed = super::parse_sense(&sense, K_SENSE_DATA_SIZE as u8);
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: task_status,
                sense: Some(parsed),
            });
        }

        Ok(ScsiResult {
            status: 0,
            bytes_transferred: transfer_count as usize,
            sense,
        })
    }
}

// ── Drive enumeration (registry-based, no exclusive access) ──────────────

pub(super) fn list_drives() -> Vec<super::DriveInfo> {
    let mut buf = [ShimDriveInfo {
        bsd_name: [0; 32],
        vendor: [0; 32],
        model: [0; 48],
        firmware: [0; 16],
    }; 8];

    let count = unsafe { shim_list_drives(buf.as_mut_ptr(), buf.len() as i32) };

    let mut out = Vec::new();
    for i in 0..(count as usize).min(buf.len()) {
        let info = &buf[i];
        let bsd_name = cstr_to_str(&info.bsd_name);
        if bsd_name.is_empty() {
            continue;
        }
        out.push(super::DriveInfo {
            path: format!("/dev/{bsd_name}"),
            vendor: cstr_to_str(&info.vendor).to_string(),
            model: cstr_to_str(&info.model).to_string(),
            firmware: cstr_to_str(&info.firmware).to_string(),
        });
    }
    out
}

fn cstr_to_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

pub(super) fn drive_has_disc(path: &Path) -> Result<bool> {
    let mut transport = MacScsiTransport::open(path)?;
    let cdb = [crate::scsi::SCSI_TEST_UNIT_READY, 0, 0, 0, 0, 0];
    let mut buf = [0u8; 0];
    match transport.execute(
        &cdb,
        crate::scsi::DataDirection::None,
        &mut buf,
        crate::scsi::TUR_TIMEOUT_MS,
    ) {
        Ok(_) => Ok(true),
        Err(ref e) if e.scsi_sense().is_some_and(|s| s.is_not_ready()) => Ok(false),
        Err(e) => Err(e),
    }
}
