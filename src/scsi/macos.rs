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
//! The media-presence probe (`drive_has_disc`) does the same via
//! `shim_media_present`: steps 1-5 above are the *transport* open path, and a
//! probe documented as side-effect-free must not run any of them.

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
    fn shim_media_present(bsd_name: *const u8) -> i32;
}

/// Strip the `/dev/` (or raw-device `/dev/r`) prefix off a device path,
/// yielding the BSD name the shim's IOKit lookups take. Shared by
/// [`MacScsiTransport::open`] and [`drive_has_disc`] so the two cannot
/// disagree about what device they are talking about.
fn bsd_name_of(device: &Path) -> Result<&str> {
    let dev_str = device.to_str().ok_or_else(|| Error::DeviceNotFound {
        path: device.display().to_string(),
    })?;
    Ok(dev_str
        .strip_prefix("/dev/r")
        .or_else(|| dev_str.strip_prefix("/dev/"))
        .unwrap_or(dev_str))
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

        // Reject an over-length CDB rather than truncating it. The guard now
        // lives in `scsi::checked_cdb_len`, shared with the Linux and Windows
        // backends (which used to truncate here instead of erroring) so it
        // cannot drift per platform again.
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
            // Log the IOKit return before collapsing it. `open()` above decodes
            // the shim's sentinel into typed variants rather than flattening
            // every failure, but `execute()` discarded `kr` entirely — and this
            // file had no tracing at all, where the Linux and Windows backends
            // both log their execute failures. That left resource contention
            // (another process taking exclusive access mid-rip) and a real
            // hardware wedge indistinguishable, with no diagnostic trail on
            // either.
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

/// Media-presence probe, via the IOKit registry only.
///
/// [`crate::scsi::drive_has_disc`] is documented as the cheap, side-effect-free
/// "is there a disc?" question, suitable for a poll-loop tick. The Linux
/// backend honours that: `open(O_RDWR|O_NONBLOCK)` + one TEST UNIT READY, no
/// exclusive access, no unmount. The Windows backend likewise opens a shared
/// handle and issues one TUR.
///
/// macOS could not: `MacScsiTransport::open` is the FULL exclusive-transport
/// path, whose first act is `diskutil unmountDisk force` on the target device.
/// So the probe documented as side-effect-free force-unmounted the user's disc
/// — and on every poll tick, taking (and dropping) exclusive access each time.
///
/// The registry answers the same question with no side effect at all: the
/// IOStorageFamily publishes an IOMedia object for a removable device only
/// while media is present and removes it on eject, so a matching IOMedia is
/// exactly "a disc is in the drive". No SCSI command is issued, which is why
/// no timeout parameter is involved.
///
/// Trade-off, stated plainly: this reports what the OS has *enumerated*, so a
/// disc that is inserted but still spinning up (no IOMedia published yet) reads
/// as absent for the moment the enumeration takes — the same window in which a
/// TUR would answer "not ready" and this function's contract already maps to
/// `Ok(false)`.
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
    use super::{K_MAX_CDB_SIZE, OPEN, bsd_name_of, cstr_to_str, drive_has_disc};
    use crate::error::Error;
    use std::path::Path;
    use std::sync::atomic::Ordering;

    #[test]
    fn bsd_name_strips_dev_and_raw_dev_prefixes() {
        assert_eq!(bsd_name_of(Path::new("/dev/disk4")).unwrap(), "disk4");
        assert_eq!(bsd_name_of(Path::new("/dev/rdisk4")).unwrap(), "disk4");
        assert_eq!(bsd_name_of(Path::new("disk4")).unwrap(), "disk4");
    }

    /// The shim's fixed-width `[u8; N]` fields are C strings: NUL-terminated,
    /// with trailing bytes undefined/garbage past the terminator. `cstr_to_str`
    /// must stop at the first NUL, not read the full fixed width, and must
    /// never panic on a non-UTF-8 tail the shim could hand back.
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

    /// `drive_has_disc` is documented as a cheap, side-effect-free presence
    /// probe. It used to be implemented by constructing a FULL exclusive
    /// transport, whose first act is `diskutil unmountDisk force` on the target
    /// device followed by an unconditional `usleep(500000)` — so the probe
    /// force-unmounted the user's disc, on every poll tick.
    ///
    /// Two observables separate the registry probe from the transport open,
    /// neither of which needs an optical drive to be attached:
    ///
    /// 1. It ANSWERS. The transport path returned `Err(DeviceNotFound)` here;
    ///    the registry path reports "no media" as `Ok(false)`.
    /// 2. It is FAST. The transport path's `usleep(500000)` after the spawn is
    ///    unconditional, so it could not complete inside this budget even when
    ///    the spawn itself failed.
    #[test]
    fn presence_probe_does_not_open_a_transport() {
        let path = Path::new("/dev/freemkv-no-such-device");
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
        assert!(
            !OPEN.load(Ordering::Acquire),
            "the probe must not leave the exclusive-transport lock held"
        );
    }

    /// A CDB longer than K_MAX_CDB_SIZE must be rejected with
    /// `Error::InvalidCdbLength` before the shim is ever called. Exercises the
    /// real guard `MacScsiTransport::execute` uses, without opening an IOKit
    /// handle.
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
