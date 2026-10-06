//! macOS SCSI transport: IOKit SCSITaskDeviceInterface with exclusive access.
//!
//! Single dispatch path: **all** CDBs go through `SCSITaskDeviceInterface::ExecuteTaskSync`,
//! 1:1 with the Linux SG_IO backend. The C shim (`macos_shim.c`) unmounts, opens, and obtains
//! exclusive access before dispatch.
//!
//! Drive enumeration and the media-presence probe use the IOKit registry directly and never
//! take exclusive access.

use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use crate::halt::Halt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

// Native calls run on the operation thread, retaining its diagnostic subscriber.
#[unsafe(no_mangle)]
extern "C" fn freemkv_macos_diagnostic(message: *const std::ffi::c_char) {
    if message.is_null() {
        return;
    }
    // Never unwind through the C ABI, including if a subscriber panics.
    let _ = std::panic::catch_unwind(|| {
        let message = unsafe { std::ffi::CStr::from_ptr(message) }.to_string_lossy();
        tracing::debug!(target: "freemkv::scsi::macos", %message, "macOS transport");
    });
}

const K_SENSE_DATA_SIZE: usize = 32;

/// Max CDB length the SCSI commands this library issues ever use. An
/// over-length CDB is rejected (never clamped) before the `cdb_len` reaches
/// the shim, so a pathological >255-byte slice can't wrap a `u8`.
const K_MAX_CDB_SIZE: usize = 16;

// The C shim uses a single global IOKit handle, so only one MacScsiTransport may exist at a
// time — a second open() would race the shared handle with the first drop().
static OPEN: AtomicBool = AtomicBool::new(false);

#[repr(C)]
#[derive(Copy, Clone)]
struct ShimDriveInfo {
    device_selector: [u8; 32],
    vendor: [u8; 32],
    model: [u8; 48],
    firmware: [u8; 16],
}

// shim_open_exclusive's result when the cancel byte was set (`SHIM_CANCELLED` in the shim).
const SHIM_CANCELLED: i32 = -6;

unsafe extern "C" {
    fn shim_open_exclusive(bsd_name: *const u8, cancel: *const u8) -> i32;
    fn shim_close();
    fn shim_last_open_kr() -> i32;
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
        timeout_ms: u32,
    ) -> i32;
    fn shim_list_drives(out: *mut ShimDriveInfo, max_entries: i32) -> i32;
    fn shim_media_present(bsd_name: *const u8) -> i32;
}

// Strip the /dev/ (or raw-device /dev/r) prefix off a device path,
// yielding the BSD name the shim's IOKit lookups take. Shared by
// MacScsiTransport::open and disc_presence so they can't disagree.
fn bsd_name_of(device: &Path) -> Result<&str> {
    let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
        path: device.display().to_string(),
    })?;
    Ok(dev_str
        .strip_prefix("/dev/r")
        .or_else(|| dev_str.strip_prefix("/dev/"))
        .unwrap_or(dev_str))
}

// The token's flag as the shim's `const volatile uint8_t *` cancel byte; valid while `halt` is.
fn cancel_byte(halt: &Halt) -> *const u8 {
    halt.as_arc().as_ptr().cast_const().cast()
}

fn device_path_for_selector(selector: &str) -> String {
    if selector.starts_with("ioreg:") {
        selector.to_string()
    } else {
        format!("/dev/{selector}")
    }
}

// Maps a shim_open_exclusive failure sentinel (negative rc, not an IOReturn) to its typed Error
// variant; `kr` is the IOReturn behind it. Standalone so the mapping can be unit-tested.
fn map_shim_open_error(rc: i32, path: String, kr: u32) -> Error {
    match rc {
        // The caller's token was cancelled during an open wait (§2.9 M2).
        SHIM_CANCELLED => Error::Halted,
        // -2/-3/-4: IOCreatePlugInInterfaceForService /
        // QueryInterface MMCDeviceInterface /
        // GetSCSITaskDeviceInterface failed.
        -4..=-2 => Error::IoKitPluginFailed { path, kr },
        // -5: ObtainExclusiveAccess failed (held by another
        // process).
        -5 => Error::DeviceLocked { path, kr },
        // -1 and anything else: device not present.
        _ => Error::DeviceNotFound { path },
    }
}

pub struct MacScsiTransport {
    /// The last command's sense-key specific progress indication (§2.11).
    last_progress: Option<u16>,
}

unsafe impl Send for MacScsiTransport {}

impl MacScsiTransport {
    /// Open `device`; every wait inside the shim's open ends within a slice of `halt`
    /// being cancelled, and a cancelled open is [`Error::Halted`] (stop design §2.9 M2).
    pub fn open(device: &Path, halt: &Halt) -> Result<Self> {
        let bsd_name = bsd_name_of(device)?;

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

        let rc = unsafe { shim_open_exclusive(bsd_c.as_ptr(), cancel_byte(halt)) };
        if rc != 0 {
            // Release the single-instance lock taken by the OPEN.swap above;
            // a failed open must not leave it held or every later open wedges.
            OPEN.store(false, Ordering::Release);
            let kr = unsafe { shim_last_open_kr() } as u32;
            return Err(map_shim_open_error(rc, bsd_name.to_string(), kr));
        }

        Ok(MacScsiTransport {
            last_progress: None,
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
    fn last_sense_progress(&self) -> Option<u16> {
        self.last_progress
    }

    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.last_progress = None;
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
            DataDirection::ToDevice | DataDirection::None => 0,
        };
        // The shim picks the transfer direction from the length: None never transfers.
        let buf_len = if direction == DataDirection::None {
            0
        } else {
            data.len() as u32
        };

        let mut sense = [0u8; K_SENSE_DATA_SIZE];
        let mut task_status: u8 = 0xFF;
        let mut transfer_count: u64 = 0;

        // Reject an over-length CDB rather than truncating it; the guard
        // lives in `scsi::checked_cdb_len`, shared with Linux and Windows
        // so it cannot drift per platform again.
        let cdb_len = super::checked_cdb_len(cdb, K_MAX_CDB_SIZE)?;
        let kr = unsafe {
            shim_execute(
                cdb.as_ptr(),
                cdb_len,
                data.as_mut_ptr(),
                buf_len,
                data_in,
                sense.as_mut_ptr(),
                K_SENSE_DATA_SIZE as u32,
                &mut task_status,
                &mut transfer_count,
                // SCSITaskLib.h SetTimeoutDuration: zero is "Wait Forever", so 0 becomes the default.
                super::effective_timeout_ms(timeout_ms),
            )
        };

        if kr != 0 {
            // Log the IOKit return before collapsing it — `execute()` used
            // to discard `kr` with no tracing, unlike Linux/Windows, leaving
            // resource contention and a real hardware wedge indistinguishable.
            tracing::warn!(
                target: "freemkv::scsi",
                opcode = cdb.first().copied().unwrap_or(0),
                kr,
                "shim_execute failed"
            );
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        if task_status != 0 {
            let parsed = super::sense_for_status(task_status, &sense, K_SENSE_DATA_SIZE as u8);
            self.last_progress =
                parsed.and_then(|_| super::parse_sense_progress(&sense, K_SENSE_DATA_SIZE as u8));
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: task_status,
                sense: parsed,
            });
        }

        Ok(ScsiResult {
            status: 0,
            // Clamp to the buffer length (matches Linux's structural bound)
            // so a lying drive/shim can't produce a bytes_transferred that
            // exceeds the buffer a future caller might slice with.
            bytes_transferred: (transfer_count as usize).min(data.len()),
            sense,
        })
    }
}

// ── Drive enumeration (registry-based, no exclusive access) ──────────────

pub(super) fn list_drives() -> Vec<super::DriveInfo> {
    let empty = ShimDriveInfo {
        device_selector: [0; 32],
        vendor: [0; 32],
        model: [0; 48],
        firmware: [0; 16],
    };
    // The shim stops at the buffer size; a full buffer means there may be more.
    let mut capacity = 16usize;
    loop {
        let mut buf = vec![empty; capacity];
        let count = unsafe { shim_list_drives(buf.as_mut_ptr(), buf.len() as i32) }.max(0) as usize;
        if count < capacity || capacity >= 4096 {
            return buf
                .iter()
                .take(count.min(capacity))
                .filter_map(drive_info_from_shim)
                .collect();
        }
        capacity *= 2;
    }
}

fn drive_info_from_shim(info: &ShimDriveInfo) -> Option<super::DriveInfo> {
    let selector = cstr_to_str(&info.device_selector);
    if selector.is_empty() {
        return None;
    }
    Some(super::DriveInfo {
        path: device_path_for_selector(selector),
        vendor: cstr_to_str(&info.vendor).to_string(),
        model: cstr_to_str(&info.model).to_string(),
        firmware: cstr_to_str(&info.firmware).to_string(),
    })
}

fn cstr_to_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

// Media-presence probe via the IOKit registry only — no exclusive access, no unmount, no SCSI
// command (open() force-unmounts the disc; a probe documented as side-effect-free must never do
// that).
pub(super) fn disc_presence(path: &Path) -> Result<super::DiscPresence> {
    let bsd_name = bsd_name_of(path)?;
    let mut bsd_c = bsd_name.as_bytes().to_vec();
    bsd_c.push(0);
    match unsafe { shim_media_present(bsd_c.as_ptr()) } {
        1 => Ok(super::DiscPresence::Present),
        0 => Ok(super::DiscPresence::Absent),
        // -1: IOKit itself is unavailable (IOMainPort / matching-dictionary
        // failure). That is not "no disc" — surface it rather than report a
        // false negative the caller would act on.
        _ => Err(Error::DeviceNotFound {
            path: bsd_name.to_string(),
        }),
    }
}

#[cfg(test)]
#[path = "macos_tests.rs"]
mod tests;
