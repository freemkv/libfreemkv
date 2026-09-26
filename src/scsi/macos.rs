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
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

const K_SENSE_DATA_SIZE: usize = 32;

/// Max CDB length the SCSI commands this library issues ever use; also
/// the clamp Linux applies. Used to bound the `cdb_len` passed to the
/// shim so a pathological >255-byte slice can't wrap a `u8`.
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
    fn shim_media_present(bsd_name: *const u8) -> i32;
}

// Strip the /dev/ (or raw-device /dev/r) prefix off a device path,
// yielding the BSD name the shim's IOKit lookups take. Shared by
// MacScsiTransport::open and drive_has_disc so they can't disagree.
fn bsd_name_of(device: &Path) -> Result<&str> {
    let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
        path: device.display().to_string(),
    })?;
    Ok(dev_str
        .strip_prefix("/dev/r")
        .or_else(|| dev_str.strip_prefix("/dev/"))
        .unwrap_or(dev_str))
}

fn device_path_for_selector(selector: &str) -> String {
    if selector.starts_with("ioreg:") {
        selector.to_string()
    } else {
        format!("/dev/{selector}")
    }
}

// Maps a shim_open_exclusive failure sentinel (negative rc, not an IOReturn) to its typed Error
// variant, pulled out standalone so the mapping can be unit-tested.
fn map_shim_open_error(rc: i32, path: String) -> Error {
    match rc {
        // -2/-3/-4: IOCreatePlugInInterfaceForService /
        // QueryInterface MMCDeviceInterface /
        // GetSCSITaskDeviceInterface failed.
        -4..=-2 => Error::IoKitPluginFailed { path, kr: 0 },
        // -5: ObtainExclusiveAccess failed (held by another
        // process).
        -5 => Error::DeviceLocked { path, kr: 0 },
        // -1 and anything else: device not present.
        _ => Error::DeviceNotFound { path },
    }
}

pub struct MacScsiTransport {
    _bsd_name: String,
}

unsafe impl Send for MacScsiTransport {}

impl MacScsiTransport {
    pub fn open(device: &Path) -> Result<Self> {
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

        let rc = unsafe { shim_open_exclusive(bsd_c.as_ptr()) };
        if rc != 0 {
            // Release the single-instance lock taken by the OPEN.swap above;
            // a failed open must not leave it held or every later open wedges.
            OPEN.store(false, Ordering::Release);
            return Err(map_shim_open_error(rc, bsd_name.to_string()));
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
        // NOTE: timeout_ms is ignored on macOS — the C shim hardcodes a
        // fixed 30s `SetTimeoutDuration`, tracked separately since macOS
        // is dev/test-only per project rules.
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

        // Reject an over-length CDB rather than truncating it; the guard
        // lives in `scsi::checked_cdb_len`, shared with Linux and Windows
        // so it cannot drift per platform again.
        let cdb_len = super::checked_cdb_len(cdb, K_MAX_CDB_SIZE)?;
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
            let parsed = super::parse_sense(&sense, K_SENSE_DATA_SIZE as u8);
            return Err(Error::ScsiError {
                opcode: cdb.first().copied().unwrap_or(0),
                status: task_status,
                sense: Some(parsed),
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
    let mut buf = [ShimDriveInfo {
        device_selector: [0; 32],
        vendor: [0; 32],
        model: [0; 48],
        firmware: [0; 16],
    }; 8];

    let count = unsafe { shim_list_drives(buf.as_mut_ptr(), buf.len() as i32) };

    buf.iter()
        .take((count as usize).min(buf.len()))
        .filter_map(drive_info_from_shim)
        .collect()
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
pub(super) fn drive_has_disc(path: &Path) -> Result<bool> {
    let bsd_name = bsd_name_of(path)?;
    let mut bsd_c = bsd_name.as_bytes().to_vec();
    bsd_c.push(0);
    match unsafe { shim_media_present(bsd_c.as_ptr()) } {
        1 => Ok(true),
        0 => Ok(false),
        // -1: IOKit itself is unavailable (IOMainPort / matching-dictionary
        // failure). That is not "no disc" — surface it rather than report a
        // false negative the caller would act on.
        _ => Err(Error::DeviceNotFound {
            path: bsd_name.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        K_MAX_CDB_SIZE, OPEN, ShimDriveInfo, bsd_name_of, cstr_to_str, device_path_for_selector,
        drive_has_disc, drive_info_from_shim, map_shim_open_error,
    };
    use crate::error::Error;
    use std::path::Path;
    use std::sync::atomic::Ordering;

    #[test]
    fn bsd_name_strips_dev_and_raw_dev_prefixes() {
        assert_eq!(bsd_name_of(Path::new("/dev/disk4")).unwrap(), "disk4");
        assert_eq!(bsd_name_of(Path::new("/dev/rdisk4")).unwrap(), "disk4");
        assert_eq!(bsd_name_of(Path::new("disk4")).unwrap(), "disk4");
        assert_eq!(
            bsd_name_of(Path::new("ioreg:4294967295")).unwrap(),
            "ioreg:4294967295"
        );
    }

    #[test]
    fn list_path_preserves_registry_selector_and_uses_dev_for_bsd_name() {
        assert_eq!(device_path_for_selector("disk4"), "/dev/disk4");
        assert_eq!(device_path_for_selector("disk0"), "/dev/disk0");
        assert_eq!(
            device_path_for_selector("ioreg:4294967295"),
            "ioreg:4294967295"
        );
        // Registry IDs are uint64 values and the shim emits the complete
        // decimal value into its fixed-width selector field.
        assert_eq!(
            device_path_for_selector("ioreg:18446744073709551615"),
            "ioreg:18446744073709551615"
        );
    }

    fn shim_drive(selector: &[u8], vendor: &[u8], model: &[u8], firmware: &[u8]) -> ShimDriveInfo {
        let mut info = ShimDriveInfo {
            device_selector: [0; 32],
            vendor: [0; 32],
            model: [0; 48],
            firmware: [0; 16],
        };
        info.device_selector[..selector.len()].copy_from_slice(selector);
        info.vendor[..vendor.len()].copy_from_slice(vendor);
        info.model[..model.len()].copy_from_slice(model);
        info.firmware[..firmware.len()].copy_from_slice(firmware);
        info
    }

    #[test]
    fn shim_drive_record_maps_bsd_selector_and_identity_fields() {
        let info = shim_drive(b"disk4", b"HL-DT-ST", b"BD-RE BU40N", b"1.03");
        let mapped = drive_info_from_shim(&info).expect("non-empty selector");
        assert_eq!(mapped.path, "/dev/disk4");
        assert_eq!(mapped.vendor, "HL-DT-ST");
        assert_eq!(mapped.model, "BD-RE BU40N");
        assert_eq!(mapped.firmware, "1.03");
    }

    #[test]
    fn shim_drive_record_keeps_empty_tray_registry_selector() {
        let info = shim_drive(
            b"ioreg:18446744073709551615",
            b"HL-DT-ST",
            b"BD-RE BU40N",
            b"1.03",
        );
        let mapped = drive_info_from_shim(&info).expect("registry selector");
        assert_eq!(mapped.path, "ioreg:18446744073709551615");
        assert_eq!(mapped.vendor, "HL-DT-ST");
        assert_eq!(mapped.model, "BD-RE BU40N");
        assert_eq!(mapped.firmware, "1.03");
    }

    #[test]
    fn shim_drive_record_with_empty_selector_is_skipped() {
        let info = shim_drive(b"", b"HL-DT-ST", b"BD-RE BU40N", b"1.03");
        assert!(drive_info_from_shim(&info).is_none());
    }

    // Shim fixed-width fields are NUL-terminated C strings with garbage
    // after the terminator; cstr_to_str must stop at the first NUL, not
    // read the full width, and never panic on a non-UTF-8 tail.
    #[test]
    fn cstr_to_str_stops_at_first_nul() {
        let mut bytes = [0xAAu8; 8]; // 0xAA is not valid UTF-8 on its own
        bytes[..5].copy_from_slice(b"BU40N");
        bytes[5] = 0; // terminator; bytes[6..8] remain 0xAA "garbage"
        assert_eq!(cstr_to_str(&bytes), "BU40N");
    }

    #[test]
    fn cstr_to_str_no_nul_uses_whole_buffer() {
        let bytes = *b"HL-DT-ST";
        assert_eq!(cstr_to_str(&bytes), "HL-DT-ST");
    }

    #[test]
    fn cstr_to_str_invalid_utf8_returns_empty_not_panic() {
        let bytes = [0xFFu8, 0xFE, 0x00, 0x00];
        assert_eq!(cstr_to_str(&bytes), "");
    }

    // Regression test: drive_has_disc used to open a FULL exclusive transport (force-unmount +
    // 500ms sleep) on every poll tick. Checks it now answers Ok(false) fast with no lock held.
    #[test]
    fn presence_probe_does_not_open_a_transport() {
        let path = Path::new("/dev/freemkv-no-such-device");
        // Snapshot rather than assume `false`: OPEN is a process-global bit any
        // concurrent test's live transport can hold, so compare the delta — it
        // scopes the check to this call, can't flake, still catches a held lock.
        let open_before = OPEN.load(Ordering::Acquire);
        let t0 = std::time::Instant::now();
        let r = drive_has_disc(path);
        let elapsed = t0.elapsed();

        assert!(
            matches!(r, Ok(false)),
            "a device with no IOMedia must report absent media, got {r:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(250),
            "probe took {elapsed:?}: the transport path's unconditional 500 ms \
             post-unmount sleep means this budget can only be met without it"
        );
        assert_eq!(
            OPEN.load(Ordering::Acquire),
            open_before,
            "the probe must not change the exclusive-transport lock state"
        );
    }

    // Every negative shim_open_exclusive sentinel must map to its own typed
    // error, not collapse into DeviceNotFound: -2..=-4 (IOKit plugin chain)
    // and -5 (exclusive access held elsewhere) are the two at risk.
    #[test]
    fn map_shim_open_error_distinguishes_every_sentinel() {
        for rc in [-2, -3, -4] {
            match map_shim_open_error(rc, "disk4".into()) {
                Error::IoKitPluginFailed { path, kr } => {
                    assert_eq!(path, "disk4");
                    assert_eq!(kr, 0);
                }
                other => panic!("rc={rc}: expected IoKitPluginFailed, got {other:?}"),
            }
        }
        match map_shim_open_error(-5, "disk4".into()) {
            Error::DeviceLocked { path, kr } => {
                assert_eq!(path, "disk4");
                assert_eq!(kr, 0);
            }
            other => panic!("expected DeviceLocked, got {other:?}"),
        }
        for rc in [-1, -6, i32::MIN] {
            match map_shim_open_error(rc, "disk4".into()) {
                Error::DeviceNotFound { path } => assert_eq!(path, "disk4"),
                other => panic!("rc={rc}: expected DeviceNotFound, got {other:?}"),
            }
        }
    }

    // A CDB longer than K_MAX_CDB_SIZE must be rejected with
    // InvalidCdbLength before the shim is called; exercises the real
    // guard MacScsiTransport::execute uses, without opening an IOKit handle.
    #[test]
    fn oversized_cdb_returns_invalid_cdb_length() {
        // Build a CDB one byte over the limit.
        let long_cdb = [0u8; K_MAX_CDB_SIZE + 1];
        match crate::scsi::checked_cdb_len(&long_cdb, K_MAX_CDB_SIZE) {
            Err(Error::InvalidCdbLength { len, max }) => {
                assert_eq!(len, K_MAX_CDB_SIZE + 1);
                assert_eq!(max, K_MAX_CDB_SIZE);
            }
            other => panic!("expected InvalidCdbLength, got {other:?}"),
        }
    }

    /// A CDB exactly at the limit must not trigger the guard, and its length
    /// reaches the shim verbatim.
    #[test]
    fn max_length_cdb_does_not_trigger_guard() {
        let cdb = [0u8; K_MAX_CDB_SIZE];
        assert_eq!(
            crate::scsi::checked_cdb_len(&cdb, K_MAX_CDB_SIZE).ok(),
            Some(K_MAX_CDB_SIZE as u8),
            "CDB of exactly K_MAX_CDB_SIZE should not trigger guard"
        );
    }
}
