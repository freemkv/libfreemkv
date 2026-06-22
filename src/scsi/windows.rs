//! Windows SCSI transport via SPTI (SCSI Pass-Through Interface).
//!
//! Sends SCSI commands through DeviceIoControl with IOCTL_SCSI_PASS_THROUGH_DIRECT.
//! Accepts device paths like `D:`, `E:`, `\\.\CdRom0`, or `\\.\D:`.
//!
//! Requires administrator privileges for raw SCSI access.

use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use std::path::Path;

// ── Windows constants ──────────────────────────────────────────────────────

const IOCTL_SCSI_PASS_THROUGH_DIRECT: u32 = 0x4D014;
/// IOCTL_STORAGE_QUERY_PROPERTY — CTL_CODE(IOCTL_STORAGE_BASE(0x2D),
/// 0x500, METHOD_BUFFERED(0), FILE_ANY_ACCESS(0)) = 0x002D1400.
const IOCTL_STORAGE_QUERY_PROPERTY: u32 = 0x002D1400;
/// STORAGE_PROPERTY_ID::StorageAdapterProperty.
const STORAGE_ADAPTER_PROPERTY: u32 = 1;
/// STORAGE_QUERY_TYPE::PropertyStandardQuery.
const PROPERTY_STANDARD_QUERY: u32 = 0;

/// Conservative fallback when the adapter MaximumTransferLength query
/// fails — 64 KiB is universally safe for SPTD on any Windows storage
/// stack. Also the floor we clamp a reported value up to.
const WINDOWS_MIN_TRANSFER_BYTES: usize = 64 * 1024;

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

// ── STORAGE_QUERY_PROPERTY structures (winioctl.h) ─────────────────────────

/// Input to IOCTL_STORAGE_QUERY_PROPERTY. Mirrors `STORAGE_PROPERTY_QUERY`:
/// `{ PropertyId: u32, QueryType: u32, AdditionalParameters: [u8; 1] }`.
#[repr(C)]
#[allow(non_snake_case)]
struct StoragePropertyQuery {
    PropertyId: u32,
    QueryType: u32,
    AdditionalParameters: [u8; 1],
}

/// Subset of `STORAGE_ADAPTER_DESCRIPTOR` (winioctl.h) up to and including
/// `MaximumTransferLength`. The real struct has more trailing fields, but
/// the driver fills the whole thing and we only read this prefix; reading a
/// truncated descriptor is the documented usage. Field layout (all the
/// leading fields are present so the offset of `MaximumTransferLength` is
/// correct):
///   Version, Size, MaximumTransferLength, MaximumPhysicalPages,
///   AlignmentMask: u32 …
#[repr(C)]
#[allow(non_snake_case)]
struct StorageAdapterDescriptor {
    Version: u32,
    Size: u32,
    MaximumTransferLength: u32,
    MaximumPhysicalPages: u32,
    AlignmentMask: u32,
    AdapterUsesPio: u8,
    AdapterScansDown: u8,
    CommandQueueing: u8,
    AcceleratedTransfer: u8,
    BusType: u8,
    BusMajorVersion: u16,
    BusMinorVersion: u16,
}

// ── Windows FFI ────────────────────────────────────────────────────────────

unsafe extern "system" {
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
    /// Adapter MaximumTransferLength in bytes, queried once at open via
    /// IOCTL_STORAGE_QUERY_PROPERTY and clamped to at least
    /// [`WINDOWS_MIN_TRANSFER_BYTES`]. A single READ larger than this fails
    /// `DeviceIoControl` outright, so [`crate::Drive::read`] chunks to it.
    max_transfer: usize,
}

// SptiTransport's only field is an isize HANDLE, so the compiler
// auto-derives BOTH Send and Sync. Exclusive use of the raw handle is
// enforced by `&mut self` on `execute()`, not by any absence of Sync.

/// Normalize a device path to Windows \\.\X: format.
///
/// NOTE: A near-identical `normalize_path` exists in `drive::windows`.
/// Both are kept because they live in separate `cfg(windows)` modules that
/// cannot easily share a helper without introducing cross-module coupling.
fn normalize_device_path(path: &str) -> String {
    if path.starts_with("\\\\.\\") {
        return path.to_string();
    }
    let trimmed = path.trim_end_matches('\\');
    if trimmed.len() == 2 && trimmed.as_bytes()[1] == b':' {
        return format!("\\\\.\\{}", trimmed);
    }
    if path.to_lowercase().starts_with("cdrom") {
        return format!("\\\\.\\{}", path);
    }
    format!("\\\\.\\{}", path)
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
            // Map last-os-error → Error variant; don't embed English hints
            // in the path field (the CLI handles localization).
            let err = std::io::Error::last_os_error();
            return Err(if err.kind() == std::io::ErrorKind::PermissionDenied {
                Error::DevicePermission {
                    path: dev_str.to_string(),
                }
            } else {
                Error::DeviceNotFound {
                    path: dev_str.to_string(),
                }
            });
        }

        let max_transfer = query_max_transfer_bytes(handle);

        Ok(SptiTransport {
            handle,
            max_transfer,
        })
    }

    /// Reset the drive to a known good state.
    /// Opens the device, sends IOCTL_STORAGE_RESET_DEVICE to reset
    /// the USB/SCSI bus, then closes. Same concept as SG_SCSI_RESET on Linux.
    pub fn reset(device: &Path) -> Result<()> {
        // CTL_CODE(IOCTL_STORAGE_BASE=0x2D, 0x0400, METHOD_BUFFERED=0,
        // FILE_READ_ACCESS|FILE_WRITE_ACCESS=3)
        //   = (0x2D<<16) | (3<<14) | (0x0400<<2) | 0 = 0x002DD000.
        // The earlier literal 0x002D1004 decoded to function 0x401 with the
        // access bits cleared — not IOCTL_STORAGE_RESET_DEVICE, so
        // DeviceIoControl would fail ERROR_INVALID_FUNCTION instead of resetting.
        const IOCTL_STORAGE_RESET_DEVICE: u32 = 0x002D_D000;

        let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
            path: device.display().to_string(),
        })?;
        let win_path = normalize_device_path(dev_str);
        let wide: Vec<u16> = win_path.encode_utf16().chain(std::iter::once(0)).collect();

        // Open
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
            return Ok(()); // can't open — skip reset, not fatal
        }

        // Send device reset
        let mut returned: u32 = 0;
        unsafe {
            DeviceIoControl(
                handle,
                IOCTL_STORAGE_RESET_DEVICE,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            );
        }

        // Close and wait for drive to settle
        unsafe { CloseHandle(handle) };
        std::thread::sleep(std::time::Duration::from_secs(2));
        Ok(())
    }
}

/// Enumerate optical drives on Windows via `find_drives()` (CdRom0..15
/// scan) and re-shape into `DriveInfo`. Existing implementation already
/// returns `(path, DriveId)`; mapped here to the public struct.
pub(super) fn list_drives() -> Vec<super::DriveInfo> {
    crate::drive::windows::find_drives()
        .into_iter()
        .map(|(path, id)| super::DriveInfo {
            path,
            vendor: id.vendor_id.trim().to_string(),
            model: id.product_id.trim().to_string(),
            firmware: id.product_revision.trim().to_string(),
        })
        .collect()
}

/// TEST UNIT READY probe on Windows. No in-library recovery — see the
/// Linux `drive_has_disc` doc block for the rationale.
pub(super) fn drive_has_disc(path: &Path) -> Result<bool> {
    let mut transport = SptiTransport::open(path)?;
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

impl Drop for SptiTransport {
    fn drop(&mut self) {
        if self.handle != INVALID_HANDLE_VALUE {
            unsafe {
                CloseHandle(self.handle);
            }
        }
    }
}

/// Query the storage adapter's `MaximumTransferLength` (bytes) via
/// IOCTL_STORAGE_QUERY_PROPERTY / StorageAdapterProperty. On any failure
/// (IOCTL failed, short reply, or a nonsensical zero) returns the
/// conservative [`WINDOWS_MIN_TRANSFER_BYTES`]; otherwise clamps the
/// reported value up to that floor. Never returns 0.
fn query_max_transfer_bytes(handle: isize) -> usize {
    if handle == INVALID_HANDLE_VALUE {
        return WINDOWS_MIN_TRANSFER_BYTES;
    }
    let query = StoragePropertyQuery {
        PropertyId: STORAGE_ADAPTER_PROPERTY,
        QueryType: PROPERTY_STANDARD_QUERY,
        AdditionalParameters: [0u8; 1],
    };
    let mut desc: StorageAdapterDescriptor = unsafe { std::mem::zeroed() };
    let mut bytes_returned: u32 = 0;
    let ok = unsafe {
        DeviceIoControl(
            handle,
            IOCTL_STORAGE_QUERY_PROPERTY,
            &query as *const _ as *mut std::ffi::c_void,
            std::mem::size_of::<StoragePropertyQuery>() as u32,
            &mut desc as *mut _ as *mut std::ffi::c_void,
            std::mem::size_of::<StorageAdapterDescriptor>() as u32,
            &mut bytes_returned,
            std::ptr::null_mut(),
        )
    };
    // MaximumTransferLength sits at offset 8; need at least that many bytes
    // written for the field to be valid.
    let valid = ok != 0
        && bytes_returned as usize
            >= std::mem::offset_of!(StorageAdapterDescriptor, MaximumTransferLength)
                + std::mem::size_of::<u32>();
    if !valid || desc.MaximumTransferLength == 0 {
        return WINDOWS_MIN_TRANSFER_BYTES;
    }
    (desc.MaximumTransferLength as usize).max(WINDOWS_MIN_TRANSFER_BYTES)
}

impl ScsiTransport for SptiTransport {
    fn max_transfer_bytes(&self) -> usize {
        self.max_transfer
    }

    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        // Zero the data buffer for reads to prevent returning uninitialized data
        // if the driver doesn't fully update DataTransferLength.
        if direction == DataDirection::FromDevice {
            data.fill(0);
        }

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
        // Match the macOS/Linux guard: a >=4 GiB buffer would wrap when cast to
        // u32 below, producing a short transfer reported as success with the
        // wrong byte count.
        if data.len() > u32::MAX as usize {
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }
        sptwb.spt.DataTransferLength = data.len() as u32;
        // Round up to the next whole second so a 1500ms request gets at
        // least 2s, not 1s. SPTI's TimeOutValue is u32 seconds with no
        // sub-second resolution; biasing toward "more time" is safer than
        // truncating (truncation broke 1500ms fast-reads on Drive::read).
        sptwb.spt.TimeOutValue = ((timeout_ms + 999) / 1000).max(1);
        sptwb.spt.DataBuffer = if data.is_empty() {
            std::ptr::null_mut()
        } else {
            data.as_mut_ptr()
        };
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
            // Driver-level failure (timeout, handle gone, etc.). Bubble
            // up; in-library handle recovery was removed in 0.13.20 along
            // with Linux's async fd-recovery and macOS's `try_recover` —
            // the kernel mid-layer already did its escalation by the time
            // DeviceIoControl returned, and re-issuing reset/reopen here
            // is at best redundant and at worst deepens the wedge. Caller
            // surfaces the failure to UX.
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        if sptwb.spt.ScsiStatus != 0 {
            // SPTI doesn't surface a "bytes written into sense buffer"
            // count separate from SenseInfoLength (input). Pass the full
            // K_SENSE_SIZE; parse_sense keys off byte 0's response code
            // to handle descriptor (0x72/0x73) vs fixed (0x70/0x71).
            //
            // 0.13.23: carry the full SPC-4 sense triple in
            // `Error::ScsiError::sense` so callers can route on
            // `ScsiSense::is_medium_error()` etc.
            let parsed = super::parse_sense(&sptwb.sense, K_SENSE_SIZE as u8);
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: sptwb.spt.ScsiStatus,
                sense: Some(parsed),
            });
        }

        let mut sense = [0u8; 32];
        sense.copy_from_slice(&sptwb.sense);

        Ok(ScsiResult {
            status: sptwb.spt.ScsiStatus,
            // Clamp to the caller's buffer length, matching Linux/macOS: a
            // driver that reports DataTransferLength > data.len() must never
            // let callers read past the buffer they handed in.
            bytes_transferred: (sptwb.spt.DataTransferLength as usize).min(data.len()),
            sense,
        })
    }
}
