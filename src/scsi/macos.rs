//! macOS SCSI transport via IOKit SCSITaskDeviceInterface.
//!
//! Sends SCSI commands to optical drives through IOKit's SCSI Architecture
//! Model family. Accepts BSD device paths like `/dev/disk2` or `/dev/rdisk2`.
//!
//! Requires exclusive access to the device — unmount the disc first:
//! `diskutil unmountDisk /dev/disk2`

use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use std::path::Path;

// ── IOKit / CoreFoundation type aliases ─────────────────────────────────────

type CFMutableDictionaryRef = *mut std::ffi::c_void;
type IOObject = u32;
type IOReturn = i32;
type MachPort = u32;

/// Opaque COM interface pointer — `*mut *mut VTable` (double-indirect).
/// IOKit plugins use COM-style vtables: the pointer points to a pointer
/// to the function table.
type ComRef = *mut *mut std::ffi::c_void;

const K_IO_RETURN_SUCCESS: IOReturn = 0;

// SCSI data transfer directions (SCSITaskLib.h)
const K_SCSI_DATA_TRANSFER_NO_DATA: u8 = 0;
const K_SCSI_DATA_TRANSFER_FROM_TARGET: u8 = 1;
const K_SCSI_DATA_TRANSFER_TO_TARGET: u8 = 2;

// SCSI task status values
const K_SCSI_TASK_STATUS_GOOD: u8 = 0x00;

const K_MAX_CDB_SIZE: usize = 16;
const K_SENSE_DATA_SIZE: usize = 32;

// ── IOKit plugin UUIDs ──────────────────────────────────────────────────────
// From IOKit/scsi/SCSITaskLib.h

/// kIOMMCDeviceUserClientTypeID — plugin type for MMC (optical) devices.
const K_IO_MMC_DEVICE_USER_CLIENT_TYPE_ID: [u8; 16] = [
    0x97, 0xAB, 0xCF, 0x5C, 0x45, 0x71, 0x11, 0xD6, 0xB6, 0xA0, 0x00, 0x30, 0x65, 0xA4, 0x7A, 0xEE,
];

/// kIOCFPlugInInterfaceID — base IOCFPlugin interface.
const K_IO_CFPLUGIN_INTERFACE_ID: [u8; 16] = [
    0xC2, 0x44, 0xE8, 0x58, 0x10, 0x9C, 0x11, 0xD4, 0x91, 0xD4, 0x00, 0x50, 0xE4, 0xC6, 0x42, 0x6F,
];

/// kIOSCSITaskDeviceInterfaceID — the interface we QueryInterface for.
const K_IO_SCSI_TASK_DEVICE_INTERFACE_ID: [u8; 16] = [
    0x61, 0x3E, 0x48, 0xB0, 0x30, 0x01, 0x11, 0xD6, 0xA4, 0xC0, 0x00, 0x0A, 0x27, 0x05, 0x28, 0x61,
];

// ── Scatter/gather element ──────────────────────────────────────────────────

#[repr(C)]
struct SCSITaskSGElement {
    address: u64,
    length: u64,
}

// ── External IOKit / CoreFoundation functions ───────────────────────────────

// Rust 2024: FFI blocks declaring extern fns must be `unsafe extern`.
unsafe extern "C" {
    fn IOMasterPort(bootstrap: u32, master: *mut MachPort) -> IOReturn;
    fn IOBSDNameMatching(
        master: MachPort,
        options: u32,
        bsd_name: *const u8,
    ) -> CFMutableDictionaryRef;
    fn IOServiceGetMatchingService(master: MachPort, matching: CFMutableDictionaryRef) -> IOObject;
    fn IOObjectRelease(object: IOObject) -> IOReturn;
    fn IORegistryEntryGetParentEntry(
        entry: IOObject,
        plane: *const u8,
        parent: *mut IOObject,
    ) -> IOReturn;
    fn IOObjectConformsTo(object: IOObject, class_name: *const u8) -> u8;
    fn IOCreatePlugInInterfaceForService(
        service: IOObject,
        plugin_type: *const [u8; 16],
        interface_type: *const [u8; 16],
        the_interface: *mut ComRef,
        the_score: *mut i32,
    ) -> IOReturn;
}

// ── COM vtable helpers ──────────────────────────────────────────────────────
//
// IOKit plugin interfaces use COM-style vtables. A ComRef is **vtable —
// dereferencing once gives the vtable pointer, then index into it for
// individual function pointers.
//
// All vtable indices verified against Apple open source:
// IOSCSIArchitectureModelFamily/UserClientLib/SCSITaskLib.h

/// Read a function pointer from a COM vtable at the given index.
///
/// # Safety
/// `iface` must be a valid COM interface pointer (*mut *mut c_void), and
/// `index` must be a valid vtable slot for the target type `T`.
unsafe fn vtable_fn<T>(iface: ComRef, index: usize) -> T {
    // Rust 2024: `unsafe fn` bodies are no longer implicitly unsafe.
    // Each unsafe op needs its own `unsafe { }` block.
    unsafe {
        let vtable = *iface as *const *const std::ffi::c_void;
        let fn_ptr = *vtable.add(index);
        std::mem::transmute_copy(&fn_ptr)
    }
}

/// Call Release (vtable index 3) on any COM interface.
fn com_release(iface: ComRef) {
    type Fn = unsafe extern "C" fn(ComRef) -> u32;
    unsafe {
        let f: Fn = vtable_fn(iface, 3);
        f(iface);
    }
}

// ── SCSITaskDeviceInterface vtable ──────────────────────────────────────────
//
// Index  Method
//   0    _reserved
//   1    QueryInterface
//   2    AddRef
//   3    Release
//   4    IsExclusiveAccessAvailable
//   5    AddCallbackDispatcherToRunLoop
//   6    RemoveCallbackDispatcherFromRunLoop
//   7    ObtainExclusiveAccess
//   8    ReleaseExclusiveAccess
//   9    CreateSCSITask

const VTIDX_OBTAIN_EXCLUSIVE: usize = 7;
const VTIDX_RELEASE_EXCLUSIVE: usize = 8;
const VTIDX_CREATE_TASK: usize = 9;

// ── SCSITaskInterface vtable ────────────────────────────────────────────────
//
// Index  Method
//   0    _reserved
//   1    QueryInterface
//   2    AddRef
//   3    Release
//   4    IsTaskActive
//   5    SetTaskAttribute
//   6    GetTaskAttribute
//   7    GetTaskState
//   8    SetCommandDescriptorBlock
//   9    GetCommandDescriptorBlockSize
//  10    GetCommandDescriptorBlock
//  11    SetScatterGatherEntries
//  12    SetTimeoutDuration
//  13    GetTimeoutDuration
//  14    SetTaskCompletionCallback
//  15    ExecuteTaskSync
//  16    ExecuteTaskAsync
//  17    AbortTask
//  18    GetSCSIServiceResponse
//  19    GetTaskStatus
//  20    GetRealizedDataTransferCount
//  21    GetAutoSenseData

const VTIDX_SET_CDB: usize = 8;
const VTIDX_SET_SG: usize = 11;
const VTIDX_SET_TIMEOUT: usize = 12;
const VTIDX_EXECUTE_SYNC: usize = 15;

// ── Transport implementation ────────────────────────────────────────────────

pub struct MacScsiTransport {
    device_iface: ComRef,
    exclusive: bool,
    /// BSD name (e.g. "disk2") retained for `try_recover()` after a
    /// task-level failure. Without it we can't re-call `find_scsi_service`.
    bsd_name: String,
}

// IOKit COM interface pointers are Mach port references — safe to send between threads.
unsafe impl Send for MacScsiTransport {}

impl MacScsiTransport {
    pub fn open(device: &Path) -> Result<Self> {
        let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
            path: device.display().to_string(),
        })?;

        // Strip /dev/ prefix to get BSD name (e.g. "disk2")
        let bsd_name = if let Some(rest) = dev_str.strip_prefix("/dev/r") {
            rest
        } else if let Some(rest) = dev_str.strip_prefix("/dev/") {
            rest
        } else {
            dev_str
        };

        let device_iface = Self::acquire_device_iface(bsd_name)?;

        Ok(MacScsiTransport {
            device_iface,
            exclusive: true,
            bsd_name: bsd_name.to_string(),
        })
    }

    /// Resolve the BSD name → IOKit SCSITaskDeviceInterface with exclusive
    /// access. Shared between `open()` and `try_recover()`. Returns the
    /// COM ref the caller must release.
    fn acquire_device_iface(bsd_name: &str) -> Result<ComRef> {
        let service = find_scsi_service(bsd_name)?;

        // Create IOKit plugin for the MMC device
        let mut plugin: ComRef = std::ptr::null_mut();
        let mut score: i32 = 0;
        let kr = unsafe {
            IOCreatePlugInInterfaceForService(
                service,
                &K_IO_MMC_DEVICE_USER_CLIENT_TYPE_ID,
                &K_IO_CFPLUGIN_INTERFACE_ID,
                &mut plugin,
                &mut score,
            )
        };
        unsafe { IOObjectRelease(service) };

        if kr != K_IO_RETURN_SUCCESS || plugin.is_null() {
            return Err(Error::IoKitPluginFailed {
                path: bsd_name.to_string(),
                kr: kr as u32,
            });
        }

        // QueryInterface for SCSITaskDeviceInterface
        let mut device_iface: ComRef = std::ptr::null_mut();
        let hr = unsafe {
            type QiFn = unsafe extern "C" fn(ComRef, *const [u8; 16], *mut ComRef) -> i32;
            let qi: QiFn = vtable_fn(plugin, 1);
            qi(
                plugin,
                &K_IO_SCSI_TASK_DEVICE_INTERFACE_ID,
                &mut device_iface,
            )
        };
        com_release(plugin);

        if hr != 0 || device_iface.is_null() {
            return Err(Error::ScsiInterfaceUnavailable {
                path: bsd_name.to_string(),
            });
        }

        // Obtain exclusive access
        let kr = unsafe {
            type Fn = unsafe extern "C" fn(ComRef) -> IOReturn;
            let f: Fn = vtable_fn(device_iface, VTIDX_OBTAIN_EXCLUSIVE);
            f(device_iface)
        };
        if kr != K_IO_RETURN_SUCCESS {
            com_release(device_iface);
            return Err(Error::DeviceLocked {
                path: bsd_name.to_string(),
                kr: kr as u32,
            });
        }

        Ok(device_iface)
    }

    /// Recover the IOKit interface after a task-level failure. Releases
    /// the current device_iface and re-acquires fresh state via
    /// `acquire_device_iface`. Same observable contract as the Linux fd
    /// recovery: after `try_recover()`, the next `execute()` either uses a
    /// fresh interface or returns `DeviceNotFound` if recovery failed.
    ///
    /// Synchronous because IOKit `RELEASE_EXCLUSIVE` + `com_release` don't
    /// block on in-flight CDBs the way Linux SG_IO `close` does.
    fn try_recover(&mut self) {
        if !self.device_iface.is_null() {
            if self.exclusive {
                unsafe {
                    type Fn = unsafe extern "C" fn(ComRef) -> IOReturn;
                    let f: Fn = vtable_fn(self.device_iface, VTIDX_RELEASE_EXCLUSIVE);
                    f(self.device_iface);
                }
                self.exclusive = false;
            }
            com_release(self.device_iface);
            self.device_iface = std::ptr::null_mut();
        }
        match Self::acquire_device_iface(&self.bsd_name) {
            Ok(new_iface) => {
                self.device_iface = new_iface;
                self.exclusive = true;
            }
            Err(_) => {
                // Leave device_iface null; next execute() returns
                // DeviceNotFound. Caller's retry path will reopen Drive.
                self.device_iface = std::ptr::null_mut();
                self.exclusive = false;
            }
        }
    }

    // `reset()` removed in 0.13.6 — see scsi/mod.rs for rationale.
}

/// Enumerate optical drives on macOS. Mirrors `drive::macos::find_drives`
/// (which iterates `/dev/disk0..15` + INQUIRY + filters peripheral
/// type 5). Same logic, exposed through the new `DriveInfo` shape so
/// callers — `list_drives()` in `scsi::mod` — never reach into
/// `crate::drive::macos`.
pub(super) fn list_drives() -> Vec<super::DriveInfo> {
    let mut out = Vec::new();
    for i in 0..K_DEV_DISK_MAX {
        let path = format!("/dev/disk{i}");
        if !std::path::Path::new(&path).exists() {
            continue;
        }
        let mut transport = match MacScsiTransport::open(std::path::Path::new(&path)) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let inquiry = match super::inquiry(&mut transport) {
            Ok(r) => r,
            Err(_) => continue,
        };
        // SCSI peripheral type field is the lower 5 bits of byte 0.
        if inquiry.raw.is_empty()
            || (inquiry.raw[K_INQUIRY_TYPE_BYTE] & K_INQUIRY_TYPE_MASK) != K_SCSI_TYPE_OPTICAL
        {
            continue;
        }
        out.push(super::DriveInfo {
            path,
            vendor: inquiry.vendor_id,
            model: inquiry.model,
            firmware: inquiry.firmware,
        });
    }
    out
}

/// Maximum BSD disk index probed during enumeration. macOS assigns
/// `/dev/diskN` sequentially per attached storage device; 16 covers
/// any realistic homelab.
const K_DEV_DISK_MAX: u8 = 16;

/// SCSI INQUIRY response: peripheral device type lives in byte 0,
/// lower 5 bits.
const K_INQUIRY_TYPE_BYTE: usize = 0;
const K_INQUIRY_TYPE_MASK: u8 = 0x1F;

/// SCSI peripheral type 5 = "CD-ROM device" (covers DVD, BD-ROM, BD-RE).
const K_SCSI_TYPE_OPTICAL: u8 = 0x05;

/// TEST UNIT READY probe on macOS. Any non-"not ready" error bubbles up
/// to the caller — in-library wedge recovery was rolled back in 0.13.4
/// after USB-layer resets failed to recover the LG BU40N on Linux; the
/// macOS impl mirrors that choice for symmetry. See the Linux
/// `drive_has_disc` in `scsi/linux.rs` for the full rationale.
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
        Err(Error::ScsiError { sense_key, .. }) if sense_key == K_SENSE_KEY_NOT_READY => Ok(false),
        Err(e) => Err(e),
    }
}

const K_SENSE_KEY_NOT_READY: u8 = 2;

impl Drop for MacScsiTransport {
    fn drop(&mut self) {
        if self.device_iface.is_null() {
            return;
        }
        if self.exclusive {
            unsafe {
                type Fn = unsafe extern "C" fn(ComRef) -> IOReturn;
                let f: Fn = vtable_fn(self.device_iface, VTIDX_RELEASE_EXCLUSIVE);
                f(self.device_iface);
            }
        }
        com_release(self.device_iface);
    }
}

impl ScsiTransport for MacScsiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        // Per RIP_DESIGN.md §15.1: parity with Linux/Windows recovery
        // contract. If a prior execute() invalidated the interface and
        // try_recover() also failed, fail fast.
        if self.device_iface.is_null() {
            return Err(Error::DeviceNotFound {
                path: self.bsd_name.clone(),
            });
        }

        // Create a SCSI task
        let task: ComRef = unsafe {
            type Fn = unsafe extern "C" fn(ComRef) -> ComRef;
            let f: Fn = vtable_fn(self.device_iface, VTIDX_CREATE_TASK);
            f(self.device_iface)
        };
        if task.is_null() {
            self.try_recover();
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            });
        }

        // Set CDB
        let mut cdb_padded = [0u8; K_MAX_CDB_SIZE];
        let cdb_len = cdb.len().min(K_MAX_CDB_SIZE);
        cdb_padded[..cdb_len].copy_from_slice(&cdb[..cdb_len]);
        unsafe {
            type Fn = unsafe extern "C" fn(ComRef, *const u8, u8) -> IOReturn;
            let f: Fn = vtable_fn(task, VTIDX_SET_CDB);
            f(task, cdb_padded.as_ptr(), cdb_len as u8);
        }

        // Set scatter/gather and transfer direction
        let iokit_dir = match direction {
            DataDirection::None => K_SCSI_DATA_TRANSFER_NO_DATA,
            DataDirection::FromDevice => K_SCSI_DATA_TRANSFER_FROM_TARGET,
            DataDirection::ToDevice => K_SCSI_DATA_TRANSFER_TO_TARGET,
        };

        if direction != DataDirection::None && !data.is_empty() {
            let sg = SCSITaskSGElement {
                address: data.as_mut_ptr() as u64,
                length: data.len() as u64,
            };
            unsafe {
                type Fn =
                    unsafe extern "C" fn(ComRef, *const SCSITaskSGElement, u8, u64, u8) -> IOReturn;
                let f: Fn = vtable_fn(task, VTIDX_SET_SG);
                f(task, &sg, 1, data.len() as u64, iokit_dir);
            }
        } else {
            unsafe {
                type Fn =
                    unsafe extern "C" fn(ComRef, *const SCSITaskSGElement, u8, u64, u8) -> IOReturn;
                let f: Fn = vtable_fn(task, VTIDX_SET_SG);
                f(task, std::ptr::null(), 0, 0, K_SCSI_DATA_TRANSFER_NO_DATA);
            }
        }

        // Set timeout (IOKit SCSITask takes milliseconds)
        unsafe {
            type Fn = unsafe extern "C" fn(ComRef, u32);
            let f: Fn = vtable_fn(task, VTIDX_SET_TIMEOUT);
            f(task, timeout_ms);
        }

        // Execute synchronously
        let mut sense = [0u8; K_SENSE_DATA_SIZE];
        let mut task_status: u32 = 0;
        let mut realized_count: u64 = 0;

        let kr = unsafe {
            type Fn = unsafe extern "C" fn(ComRef, *mut u8, *mut u32, *mut u64) -> IOReturn;
            let f: Fn = vtable_fn(task, VTIDX_EXECUTE_SYNC);
            f(
                task,
                sense.as_mut_ptr(),
                &mut task_status,
                &mut realized_count,
            )
        };

        com_release(task);

        if kr != K_IO_RETURN_SUCCESS {
            // Task-level failure (timeout / IOKit error). Recover the
            // interface so the caller's retry path can resume.
            self.try_recover();
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            });
        }

        if task_status != K_SCSI_TASK_STATUS_GOOD as u32 {
            let sense_key = if sense[2] != 0 { sense[2] & 0x0F } else { 0 };
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: task_status as u8,
                sense_key,
            });
        }

        Ok(ScsiResult {
            status: task_status as u8,
            bytes_transferred: realized_count as usize,
            sense,
        })
    }
}

// ── IOKit service discovery ─────────────────────────────────────────────────

/// BSD name → IOKit service for the SCSI device.
///
/// Walk: IOMedia (BSD name match) → parent chain → SCSIPeripheralDeviceNub.
///
/// All failure paths surface as `Error::DeviceNotFound { path: bsd_name }` —
/// the four internal stages (IOMasterPort / IOBSDNameMatching / IOMedia
/// lookup / walk_to_authoring_device) collapse into one observable error
/// because none of them are user-actionable individually. Pre-0.13 each
/// stage stuffed an English description into `path:` ("…IOMasterPort
/// failed", "…SCSITaskDeviceInterface not available", etc.) which broke
/// the library's "no English text" rule.
fn find_scsi_service(bsd_name: &str) -> Result<IOObject> {
    let not_found = || Error::DeviceNotFound {
        path: bsd_name.to_string(),
    };

    let mut master: MachPort = 0;
    let kr = unsafe { IOMasterPort(0, &mut master) };
    if kr != K_IO_RETURN_SUCCESS {
        return Err(not_found());
    }

    // IOBSDNameMatching creates a dictionary matching { "BSD Name" = bsd_name }
    let mut bsd_c = bsd_name.as_bytes().to_vec();
    bsd_c.push(0);
    let matching = unsafe { IOBSDNameMatching(master, 0, bsd_c.as_ptr()) };
    if matching.is_null() {
        return Err(not_found());
    }

    // Find the single IOMedia service (consumes the matching dict)
    let media = unsafe { IOServiceGetMatchingService(master, matching) };
    if media == 0 {
        return Err(not_found());
    }

    // Walk up the IOService plane to find the authoring device.
    // The chain is typically:
    //   IOMedia → IOPartitionScheme → IOMedia → IOBlockStorageDriver
    //   → IOSCSIPeripheralDeviceNub (this is what we want)
    //
    // We walk up until we find a service that IOCreatePlugInInterfaceForService
    // accepts with kIOMMCDeviceUserClientTypeID, or until we hit the root.
    let service = walk_to_authoring_device(media);
    unsafe { IOObjectRelease(media) };

    service.ok_or_else(not_found)
}

/// Walk up the IOService plane from an IOMedia to the SCSI authoring device.
fn walk_to_authoring_device(start: IOObject) -> Option<IOObject> {
    let mut current = start;
    // Retain start so we can release uniformly in the loop
    // (IORegistryEntryGetParentEntry retains the parent for us)

    // Target class names for authoring devices
    let target_classes: &[&[u8]] = &[
        b"IOSCSIPeripheralDeviceNub\0",
        b"IOBDBlockStorageDevice\0",
        b"IODVDBlockStorageDevice\0",
        b"IOCDBlockStorageDevice\0",
        b"IOBlockStorageDevice\0",
    ];

    // Walk up to 10 levels (more than enough)
    for _ in 0..10 {
        let mut parent: IOObject = 0;
        let kr = unsafe {
            IORegistryEntryGetParentEntry(current, c"IOService".as_ptr() as *const u8, &mut parent)
        };

        if current != start {
            unsafe { IOObjectRelease(current) };
        }

        if kr != K_IO_RETURN_SUCCESS || parent == 0 {
            return None;
        }

        // Check if this parent matches any of our target classes
        for class in target_classes {
            if unsafe { IOObjectConformsTo(parent, class.as_ptr()) } != 0 {
                return Some(parent);
            }
        }

        current = parent;
    }

    if current != start {
        unsafe { IOObjectRelease(current) };
    }
    None
}
