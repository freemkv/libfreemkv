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
/// IOCTL_STORAGE_RESET_DEVICE (ntddstor.h) —
/// CTL_CODE(IOCTL_STORAGE_BASE=0x2D, 0x0401, METHOD_BUFFERED=0,
/// FILE_READ_ACCESS=1) = (0x2D<<16) | (1<<14) | (0x0401<<2) | 0
/// = 0x002D0000 | 0x4000 | 0x1004 = 0x002D5004.
/// Two earlier values were wrong: 0x002D1004 (function 0x401 but access
/// bits cleared) and 0x002DD000 (function 0x400 + R|W access — the
/// OBSOLETE RESET_BUS code class drivers reject). Both made
/// `DeviceIoControl` fail ERROR_INVALID_FUNCTION, silently skipping the
/// reset. See the value-regression test at the bottom of this module.
const IOCTL_STORAGE_RESET_DEVICE: u32 = 0x002D_5004;
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

// `#[repr(C, packed(4))]` mirrors ntddscsi.h's `#pragma pack(push, 4)` around
// SCSI_PASS_THROUGH_DIRECT. Without it, bare `#[repr(C)]` lets the compiler
// apply natural 8-byte alignment to the `DataBuffer` pointer on 64-bit hosts,
// inserting 4 implicit padding bytes after `TimeOutValue`. That shifts
// `DataBuffer` to offset 24 (SDK: 20), `SenseInfoOffset` to 32 (SDK: 28), and
// `Cdb` to 36 (SDK: 32), and grows the struct to 56 bytes (SDK: 48). The
// kernel driver reads the CDB and DataBuffer pointer at the SDK offsets, so a
// mismatched layout breaks every SPTI ioctl. See the layout regression test.
#[repr(C, packed(4))]
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

// Must carry the same `packed(4)` as `ScsiPassThroughDirect`, otherwise the
// trailing `sense` array would be repositioned and `offset_of!(SptwbDirect,
// sense)` (used for `SenseInfoOffset`) would point the driver at the wrong
// place to write sense data.
#[repr(C, packed(4))]
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
    // STORAGE_BUS_TYPE is an `int`-sized enum (4 bytes), not a byte. With the
    // four preceding `BOOLEAN`s filling offsets 20..24, `BusType` sits at
    // offset 24 and the two `USHORT` version fields follow at 28 and 30 —
    // matching winioctl.h. (Declaring this `u8` total-sized to 32 by luck but
    // pushed BusMajor/BusMinorVersion to offsets 26/28, so any reader of those
    // fields got garbage.)
    BusType: u32,
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

    fn GetLastError() -> u32;

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
    /// Adapter `AlignmentMask` (STORAGE_ADAPTER_DESCRIPTOR, ntddscsi.h /
    /// winioctl.h), queried alongside `max_transfer`. It is a *mask*: `0`
    /// (the common case on USB optical bridges) means the DataBuffer may
    /// sit at any address; `3` means DWORD-aligned, `7` 8-byte, etc. —
    /// always one less than the required alignment. SCSI/SAS HBAs report
    /// nonzero masks, and IOCTL_SCSI_PASS_THROUGH_DIRECT rejects a
    /// misaligned `DataBuffer` (DeviceIoControl fails → all reads return
    /// transport failure / status 0xFF). When set and the caller's buffer
    /// is misaligned, `execute()` bounces through an aligned scratch
    /// buffer (see there).
    alignment_mask: u32,
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

        let (max_transfer, alignment_mask) = query_adapter_descriptor(handle);

        Ok(SptiTransport {
            handle,
            max_transfer,
            alignment_mask,
        })
    }

    /// Reset the drive to a known good state.
    /// Opens the device, sends IOCTL_STORAGE_RESET_DEVICE to reset
    /// the USB/SCSI bus, then closes. Same concept as SG_SCSI_RESET on Linux.
    pub fn reset(device: &Path) -> Result<()> {
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

        // Send device reset. The result must be checked: a wrong/unsupported
        // IOCTL code fails with ERROR_INVALID_FUNCTION (0x1) and no-ops
        // silently — exactly the regression class the doc block above records
        // for the two earlier (incorrect) code values. Surface failures so a
        // non-functional reset is observable rather than masked by the
        // unconditional settle sleep below.
        let mut returned: u32 = 0;
        let ok = unsafe {
            DeviceIoControl(
                handle,
                IOCTL_STORAGE_RESET_DEVICE,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        let reset_ok = ok != 0;
        if reset_ok {
            tracing::debug!("IOCTL_STORAGE_RESET_DEVICE succeeded");
        } else {
            let err = unsafe { GetLastError() };
            // Not fatal — the caller treats reset as best-effort — but a
            // failing reset (especially ERROR_INVALID_FUNCTION = 1) means the
            // device was NOT reset, so there is nothing to settle and we must
            // not pay the settle-sleep penalty below.
            tracing::warn!(
                last_error = err,
                ioctl = format_args!("{IOCTL_STORAGE_RESET_DEVICE:#010x}"),
                "IOCTL_STORAGE_RESET_DEVICE failed; drive not reset"
            );
        }

        // Close the handle, then — only if the reset actually happened — wait
        // for the drive to settle. A failed IOCTL reset performed no reset, so
        // sleeping would burn 2 s for nothing.
        unsafe { CloseHandle(handle) };
        if reset_ok {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
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

/// Query the storage adapter descriptor via IOCTL_STORAGE_QUERY_PROPERTY /
/// StorageAdapterProperty and return `(max_transfer_bytes, alignment_mask)`.
///
/// `max_transfer_bytes`: the adapter's `MaximumTransferLength`. On any
/// failure (IOCTL failed, short reply, or a nonsensical zero) falls back to
/// the conservative [`WINDOWS_MIN_TRANSFER_BYTES`]; otherwise clamped up to
/// that floor. Never 0.
///
/// `alignment_mask`: the adapter's `AlignmentMask` (offset 16 in
/// STORAGE_ADAPTER_DESCRIPTOR). `0` means no alignment requirement (the
/// common case for USB optical bridges). A nonzero mask (SCSI/SAS HBAs)
/// forces `execute()` to bounce the DataBuffer through an aligned scratch
/// buffer. If the reply is too short to include `AlignmentMask`, returns
/// `0` (no requirement) — the safe default, since any address satisfies a
/// zero mask and the descriptor's leading fields are read first regardless.
fn query_adapter_descriptor(handle: isize) -> (usize, u32) {
    if handle == INVALID_HANDLE_VALUE {
        return (WINDOWS_MIN_TRANSFER_BYTES, 0);
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
    let max_valid = ok != 0
        && bytes_returned as usize
            >= std::mem::offset_of!(StorageAdapterDescriptor, MaximumTransferLength)
                + std::mem::size_of::<u32>();
    let max_transfer = if !max_valid || desc.MaximumTransferLength == 0 {
        WINDOWS_MIN_TRANSFER_BYTES
    } else {
        (desc.MaximumTransferLength as usize).max(WINDOWS_MIN_TRANSFER_BYTES)
    };

    // AlignmentMask sits at offset 16; only trust it if the reply is long
    // enough. Otherwise assume 0 (no alignment requirement).
    let align_valid = ok != 0
        && bytes_returned as usize
            >= std::mem::offset_of!(StorageAdapterDescriptor, AlignmentMask)
                + std::mem::size_of::<u32>();
    let alignment_mask = if align_valid { desc.AlignmentMask } else { 0 };

    (max_transfer, alignment_mask)
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

        // AlignmentMask bounce buffer.
        //
        // IOCTL_SCSI_PASS_THROUGH_DIRECT requires `DataBuffer` to satisfy
        // the adapter's `AlignmentMask` (`(ptr & mask) == 0`). On USB
        // optical bridges the mask is 0, so the caller's buffer is always
        // acceptable and we point straight at it (zero-copy fast path).
        // On SCSI/SAS HBAs the mask can be 3/7/… ; if the caller's buffer
        // happens to be misaligned the IOCTL fails outright (status 0xFF /
        // all reads fail). In that case we transfer through an aligned
        // scratch buffer: over-allocate by `mask` extra bytes so an aligned
        // base is guaranteed to exist inside it, align the base with
        // [`crate::scsi::align_up`], and use that as `DataBuffer`. For a
        // FROM-device transfer the result is copied back into `data` after
        // the IOCTL; for a TO-device transfer `data` is copied in before.
        //
        // `bounce` is kept alive for the whole `execute()` body so the
        // aligned pointer we hand the driver stays valid across the IOCTL.
        let mask = self.alignment_mask as usize;
        let needs_bounce =
            !data.is_empty() && mask != 0 && (data.as_mut_ptr() as usize) & mask != 0;
        let mut bounce: Vec<u8> = Vec::new();
        let data_ptr: *mut u8 = if data.is_empty() {
            std::ptr::null_mut()
        } else if needs_bounce {
            // Over-allocate by `mask` so an aligned start exists within.
            bounce = vec![0u8; data.len() + mask];
            let base = bounce.as_mut_ptr() as usize;
            let aligned = crate::scsi::align_up(base, mask);
            let aligned_ptr = aligned as *mut u8;
            // For writes (ToDevice) prime the aligned region with the
            // caller's payload before the IOCTL. (FromDevice copies back
            // after.)
            if direction == DataDirection::ToDevice {
                unsafe {
                    std::ptr::copy_nonoverlapping(data.as_ptr(), aligned_ptr, data.len());
                }
            }
            aligned_ptr
        } else {
            data.as_mut_ptr()
        };
        sptwb.spt.DataBuffer = data_ptr;
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

        // Clamp to the caller's buffer length, matching Linux/macOS: a
        // driver that reports DataTransferLength > data.len() must never let
        // callers read past the buffer they handed in.
        let transferred = (sptwb.spt.DataTransferLength as usize).min(data.len());

        // If we bounced a FROM-device read, copy the aligned scratch back
        // into the caller's buffer (only the bytes actually transferred).
        if needs_bounce && direction == DataDirection::FromDevice {
            let aligned_ptr = data_ptr; // points inside `bounce`
            unsafe {
                std::ptr::copy_nonoverlapping(aligned_ptr, data.as_mut_ptr(), transferred);
            }
        }
        // `bounce` is dropped here, after the last use of `data_ptr`.
        drop(bounce);

        let mut sense = [0u8; 32];
        sense.copy_from_slice(&sptwb.sense);

        Ok(ScsiResult {
            status: sptwb.spt.ScsiStatus,
            bytes_transferred: transferred,
            sense,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for `SptiTransport::reset()`. Two earlier IOCTL
    /// values (0x002D1004 and 0x002DD000) silently failed with
    /// ERROR_INVALID_FUNCTION while appearing to work — the reset no-oped
    /// but the unconditional settle sleep made it look successful. This
    /// recomputes IOCTL_STORAGE_RESET_DEVICE from the CTL_CODE formula
    /// independently of the hardcoded constant so a wrong value can't slip
    /// back in unnoticed.
    #[test]
    fn ioctl_storage_reset_device_value_is_correct() {
        // CTL_CODE(DeviceType, Function, Method, Access) =
        //   (DeviceType << 16) | (Access << 14) | (Function << 2) | Method
        const fn ctl_code(device_type: u32, function: u32, method: u32, access: u32) -> u32 {
            (device_type << 16) | (access << 14) | (function << 2) | method
        }
        const IOCTL_STORAGE_BASE: u32 = 0x2D;
        const METHOD_BUFFERED: u32 = 0;
        const FILE_READ_ACCESS: u32 = 1;
        let expected = ctl_code(
            IOCTL_STORAGE_BASE,
            0x0401,
            METHOD_BUFFERED,
            FILE_READ_ACCESS,
        );

        assert_eq!(
            IOCTL_STORAGE_RESET_DEVICE, expected,
            "IOCTL_STORAGE_RESET_DEVICE must equal CTL_CODE(0x2D, 0x0401, \
             METHOD_BUFFERED, FILE_READ_ACCESS); a wrong value fails \
             ERROR_INVALID_FUNCTION and silently no-ops the reset"
        );
        assert_eq!(IOCTL_STORAGE_RESET_DEVICE, 0x002D_5004);
        // The two historically wrong values must never reappear.
        assert_ne!(IOCTL_STORAGE_RESET_DEVICE, 0x002D_1004);
        assert_ne!(IOCTL_STORAGE_RESET_DEVICE, 0x002D_D000);
    }

    /// Regression guard for the `ScsiPassThroughDirect` layout. ntddscsi.h
    /// wraps SCSI_PASS_THROUGH_DIRECT in `#pragma pack(push, 4)`, forcing the
    /// PVOID `DataBuffer` to 4-byte alignment even on 64-bit hosts. With bare
    /// `#[repr(C)]` the compiler instead applies natural 8-byte pointer
    /// alignment, inserting 4 padding bytes after `TimeOutValue` — shifting
    /// `DataBuffer` to 24 (vs SDK 20), `SenseInfoOffset` to 32 (vs 28), `Cdb`
    /// to 36 (vs 32), and growing the struct to 56 bytes (vs 48). The kernel
    /// driver reads at the SDK offsets, so the wrong layout breaks every SPTI
    /// ioctl. `#[repr(C, packed(4))]` restores the SDK layout asserted here.
    #[test]
    fn scsi_pass_through_direct_matches_sdk_layout() {
        use std::mem::{offset_of, size_of};
        assert_eq!(offset_of!(ScsiPassThroughDirect, Length), 0);
        assert_eq!(offset_of!(ScsiPassThroughDirect, DataTransferLength), 12);
        assert_eq!(offset_of!(ScsiPassThroughDirect, TimeOutValue), 16);
        assert_eq!(offset_of!(ScsiPassThroughDirect, DataBuffer), 20);
        assert_eq!(offset_of!(ScsiPassThroughDirect, SenseInfoOffset), 28);
        assert_eq!(offset_of!(ScsiPassThroughDirect, Cdb), 32);
        assert_eq!(size_of::<ScsiPassThroughDirect>(), 48);
        // `sense` must immediately follow the 48-byte spt with no extra pad.
        assert_eq!(offset_of!(SptwbDirect, spt), 0);
        assert_eq!(offset_of!(SptwbDirect, sense), 48);
        assert_eq!(size_of::<SptwbDirect>(), 48 + K_SENSE_SIZE);
    }

    /// Regression guard for the `StorageAdapterDescriptor` layout. It must
    /// match `STORAGE_ADAPTER_DESCRIPTOR` (winioctl.h) field-for-field so a
    /// driver-filled buffer is interpreted at the correct offsets. `BusType`
    /// is `STORAGE_BUS_TYPE`, an `int`-sized (4-byte) enum, NOT a byte; a
    /// previous `u8` declaration kept the total size at 32 by coincidence but
    /// shifted `BusMajorVersion`/`BusMinorVersion` to offsets 26/28 (vs the
    /// SDK's 28/30), so any reader of those fields got wrong values.
    #[test]
    fn storage_adapter_descriptor_matches_sdk_layout() {
        use std::mem::{offset_of, size_of};
        assert_eq!(offset_of!(StorageAdapterDescriptor, Version), 0);
        assert_eq!(offset_of!(StorageAdapterDescriptor, Size), 4);
        assert_eq!(
            offset_of!(StorageAdapterDescriptor, MaximumTransferLength),
            8
        );
        assert_eq!(
            offset_of!(StorageAdapterDescriptor, MaximumPhysicalPages),
            12
        );
        assert_eq!(offset_of!(StorageAdapterDescriptor, AlignmentMask), 16);
        assert_eq!(offset_of!(StorageAdapterDescriptor, AdapterUsesPio), 20);
        assert_eq!(offset_of!(StorageAdapterDescriptor, AdapterScansDown), 21);
        assert_eq!(offset_of!(StorageAdapterDescriptor, CommandQueueing), 22);
        assert_eq!(
            offset_of!(StorageAdapterDescriptor, AcceleratedTransfer),
            23
        );
        // The fields that were misplaced by the old `u8` BusType.
        assert_eq!(offset_of!(StorageAdapterDescriptor, BusType), 24);
        assert_eq!(offset_of!(StorageAdapterDescriptor, BusMajorVersion), 28);
        assert_eq!(offset_of!(StorageAdapterDescriptor, BusMinorVersion), 30);
        assert_eq!(size_of::<StorageAdapterDescriptor>(), 32);
    }
}
