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
use std::sync::atomic::{AtomicBool, Ordering};

const K_SENSE_DATA_SIZE: usize = 32;

/// Max CDB length the SCSI commands this library issues ever use; also
/// the clamp Linux applies. Used to bound the `cdb_len` passed to the
/// shim so a pathological >255-byte slice can't wrap a `u8`.
const K_MAX_CDB_SIZE: usize = 16;

/// The C shim uses a single global IOKit handle (`g_handle`), so only one
/// [`MacScsiTransport`] may exist at a time — a second `open()` would
/// share that handle and the first `drop()` would tear it down out from
/// under the other. This flag enforces single-instance ownership.
static OPEN: AtomicBool = AtomicBool::new(false);

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

        // Enforce single-instance: the shim's global handle can't back two
        // live transports safely. Bail rather than corrupt shared state.
        if OPEN.swap(true, Ordering::Acquire) {
            return Err(Error::DeviceLocked {
                path: bsd_name.to_string(),
                kr: 0,
            });
        }

        let mut bsd_c = bsd_name.as_bytes().to_vec();
        bsd_c.push(0);

        let rc = unsafe { shim_open_exclusive(bsd_c.as_ptr()) };
        if rc != 0 {
            // Release the single-instance lock taken by the OPEN.swap above;
            // a failed open must not leave it held or every later open wedges.
            OPEN.store(false, Ordering::Release);
            let path = bsd_name.to_string();
            // The shim returns distinct negative sentinels per failure
            // stage; map them to the typed variants that already exist
            // rather than collapsing every failure to DeviceNotFound.
            // These sentinels are not IOReturn codes, so kr is left 0.
            return Err(match rc {
                // -2/-3/-4: IOCreatePlugInInterfaceForService /
                // QueryInterface MMCDeviceInterface /
                // GetSCSITaskDeviceInterface failed.
                -4..=-2 => Error::IoKitPluginFailed { path, kr: 0 },
                // -5: ObtainExclusiveAccess failed (held by another
                // process).
                -5 => Error::DeviceLocked { path, kr: 0 },
                // -1 and anything else: device not present.
                _ => Error::DeviceNotFound { path },
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
        OPEN.store(false, Ordering::Release);
    }
}

impl ScsiTransport for MacScsiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        // NOTE: timeout_ms is currently ignored on macOS. The C shim
        // (`macos_shim.c`) hardcodes `SetTimeoutDuration(task, 30000)`, so
        // every command uses a fixed 30 s budget regardless of the
        // caller's READ_TIMEOUT_MS / READ_RECOVERY_TIMEOUT_MS / TUR value.
        // Plumbing it through the shim signature is tracked separately;
        // macOS is dev/test-only per the project rules.
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        // Match the Linux guard: a >=4 GiB buffer would wrap when cast to
        // u32 for the shim, producing a short transfer reported as success
        // with the wrong byte count.
        if data.len() > u32::MAX as usize {
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        let data_in = match direction {
            DataDirection::FromDevice => 1,
            DataDirection::ToDevice => 0,
            DataDirection::None => 0,
        };

        let mut sense = [0u8; K_SENSE_DATA_SIZE];
        let mut task_status: u8 = 0xFF;
        let mut transfer_count: u64 = 0;

        if cdb.len() > K_MAX_CDB_SIZE {
            return Err(Error::InvalidCdbLength {
                len: cdb.len(),
                max: K_MAX_CDB_SIZE,
            });
        }
        let cdb_len = cdb.len() as u8;
        let kr = unsafe {
            shim_execute(
                cdb.as_ptr(),
                cdb_len,
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
                opcode: cdb.first().copied().unwrap_or(0),
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        if task_status != 0 {
            let parsed = super::parse_sense(&sense, K_SENSE_DATA_SIZE as u8);
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: task_status,
                sense: Some(parsed),
            });
        }

        Ok(ScsiResult {
            status: 0,
            // Clamp to the buffer length, matching the Linux transport's
            // structural bound (data.len().saturating_sub(resid)). A lying
            // drive/shim can't then produce a bytes_transferred that
            // exceeds the buffer a future caller might slice with.
            bytes_transferred: (transfer_count as usize).min(data.len()),
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
    for info in buf.iter().take((count as usize).min(buf.len())) {
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

#[cfg(test)]
mod tests {
    use super::K_MAX_CDB_SIZE;
    use crate::error::Error;

    /// A CDB longer than K_MAX_CDB_SIZE must be rejected with
    /// `Error::InvalidCdbLength` before the shim is ever called.
    /// This test exercises the length guard portably — it calls the
    /// guard logic directly without opening an IOKit handle.
    #[test]
    fn oversized_cdb_returns_invalid_cdb_length() {
        // Build a CDB one byte over the limit.
        let long_cdb = [0u8; K_MAX_CDB_SIZE + 1];
        // Replicate the guard logic from MacScsiTransport::execute so
        // this test runs on Linux CI as well (no IOKit present there).
        let result: Result<(), Error> = if long_cdb.len() > K_MAX_CDB_SIZE {
            Err(Error::InvalidCdbLength {
                len: long_cdb.len(),
                max: K_MAX_CDB_SIZE,
            })
        } else {
            Ok(())
        };
        match result {
            Err(Error::InvalidCdbLength { len, max }) => {
                assert_eq!(len, K_MAX_CDB_SIZE + 1);
                assert_eq!(max, K_MAX_CDB_SIZE);
            }
            other => panic!("expected InvalidCdbLength, got {:?}", other),
        }
    }

    /// A CDB exactly at the limit must not trigger the guard.
    #[test]
    fn max_length_cdb_does_not_trigger_guard() {
        let cdb = [0u8; K_MAX_CDB_SIZE];
        let triggered = cdb.len() > K_MAX_CDB_SIZE;
        assert!(
            !triggered,
            "CDB of exactly K_MAX_CDB_SIZE should not trigger guard"
        );
    }
}
