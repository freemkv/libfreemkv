//! Linux SCSI transport via synchronous blocking SG_IO ioctl.
//!
//! `execute()` is one syscall: `ioctl(fd, SG_IO, &hdr)` blocks until the
//! kernel completes the command (success, error, or its own timeout). No
//! userspace abort or SG_SCSI_RESET: the kernel mid-layer runs its own error
//! ladder. On a transport-level failure the fd is closed and reopened (on
//! detached threads, see `fd_handoff`), so the fd is NOT stable across one.

use super::fd_handoff::{
    AtomicBool, AtomicI32, DropUnlock, claim_for_teardown, drop_unlock_fd, drop_unlock_plan,
    publish_recovered_fd, release_recovery_slot, reserve_recovery_slot, take_recovered_fd,
};
use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use std::path::Path;
use std::sync::Arc;

const SG_IO: u32 = 0x2285;
const SG_DXFER_NONE: i32 = -1;
const SG_DXFER_TO_DEV: i32 = -2;
const SG_DXFER_FROM_DEV: i32 = -3;
const SG_FLAG_Q_AT_HEAD: u32 = 0x10;

/// Width of `sg_io_hdr.cmdp` as far as SG_IO is concerned: `cmd_len` is a
/// single byte and every SPC-4/MMC command this crate issues is 6, 10, 12 or
/// 16 bytes. Matches `K_MAX_CDB_SIZE` in the macOS and Windows backends.
const K_MAX_CDB_SIZE: usize = 16;

/// SPC-4 PREVENT ALLOW MEDIUM REMOVAL (0x1E) with PREVENT=0. Sent by `Drop` so
/// the tray is not left locked; a six-byte group-0 CDB.
const ALLOW_MEDIUM_REMOVAL: [u8; 6] = [0x1E, 0, 0, 0, 0, 0];
/// PREVENT field values (CDB byte 4 bits 1:0) tracked for the Drop unlock.
const ALLOW: u8 = 0b00;
const PREVENT: u8 = 0b01;

/// Cap on detached fd-recovery threads outstanding at once (process-wide).
/// A sustained bridge wedge would otherwise spawn 2 threads per failed ioctl
/// with no bound; past the cap, recovery runs inline instead of spawning.
const MAX_RECOVERY_THREADS: usize = 8;
static RECOVERY_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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

// Compile-time validation: sg_io_hdr must match the kernel's layout.
// 88 bytes on 64-bit, 64 bytes on 32-bit (pointer-size dependent).
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<sg_io_hdr>() == 88);
#[cfg(target_pointer_width = "32")]
const _: () = assert!(std::mem::size_of::<sg_io_hdr>() == 64);

pub struct SgIoTransport {
    // Private: execute()/Drop close whatever these hold, so safe code outside
    // must never be able to plant a descriptor it does not own (I/O safety).
    fd: i32,
    device_path: std::path::PathBuf,
    /// Single-slot mailbox a background recovery thread publishes a freshly
    /// opened fd into. Drained by `execute()`, or claimed by `Drop` if the
    /// transport dies first. The protocol — and the memory ordering that makes
    /// it leak-free — lives in `super::fd_handoff`.
    fd_recovery: Arc<AtomicI32>,
    /// A PREVENT MEDIUM REMOVAL was issued and not yet cleared by an ALLOW;
    /// only then does `Drop` unlock the tray (enumeration probes never do).
    prevent_held: bool,
    /// The last ALLOW died on the transport: Drop must not retry it.
    allow_transport_failed: bool,
    /// Set to `true` by `Drop` before it claims the slot. A recovery thread
    /// that publishes after that point sees it and closes its own fd, since
    /// nothing will ever drain the slot again.
    dead: Arc<AtomicBool>,
    /// The last command's sense-key specific progress indication (§2.11).
    last_progress: Option<u16>,
}

impl SgIoTransport {
    /// Open a SCSI device for use.
    pub fn open(device: &Path) -> Result<Self> {
        Self::open_with_errno(device).map_err(|(e, _)| e)
    }

    // `open`, also handing back the failed open(2)'s errno.
    fn open_with_errno(device: &Path) -> std::result::Result<Self, (Error, Option<i32>)> {
        let device = Self::resolve_to_sg(device);
        let fd = Self::open_fd(&device);
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            return Err((Self::map_open_error(&err, &device), err.raw_os_error()));
        }
        Ok(SgIoTransport {
            fd,
            device_path: device,
            fd_recovery: Arc::new(AtomicI32::new(super::fd_handoff::EMPTY)),
            prevent_held: false,
            allow_transport_failed: false,
            dead: Arc::new(AtomicBool::new(false)),
            last_progress: None,
        })
    }

    // open(2) the sg node the way every path here does; negative on failure.
    fn open_fd(device: &Path) -> i32 {
        let c_path = Self::to_c_path(device);
        unsafe {
            libc::open(
                c_path.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        }
    }

    // `open_fd` for the reopen after a transport failure; a failure is logged.
    fn reopen_fd(device: &Path) -> i32 {
        let fd = Self::open_fd(device);
        if fd < 0 {
            tracing::warn!(
                target: "freemkv::scsi",
                errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                "reopen after transport failure failed"
            );
        }
        fd
    }

    // Map errno from a failed open(): permission-denied -> DevicePermission,
    // else DeviceNotFound. Path carried in the error; no English text (app
    // layer localizes).
    fn open_error<T>(device: &Path) -> Result<T> {
        Err(Self::map_open_error(
            &std::io::Error::last_os_error(),
            device,
        ))
    }

    fn map_open_error(err: &std::io::Error, device: &Path) -> Error {
        if err.kind() == std::io::ErrorKind::PermissionDenied {
            Error::DevicePermission {
                path: device.display().to_string(),
            }
        } else {
            Error::DeviceNotFound {
                path: device.display().to_string(),
            }
        }
    }

    /// Send a no-data SCSI command on a bare fd, for callers with no transport
    /// to route through `execute()`: `Drop`'s tray unlock and `disc_presence`.
    /// The CDB goes through the shared [`super::checked_cdb_len`] guard.
    fn raw_command(fd: i32, cdb: &[u8], timeout_ms: u32) -> Result<()> {
        let cmd_len = super::checked_cdb_len(cdb, K_MAX_CDB_SIZE)?;
        let mut sense = [0u8; 32];
        let mut hdr: sg_io_hdr = unsafe { std::mem::zeroed() };
        hdr.interface_id = b'S' as i32;
        hdr.dxfer_direction = SG_DXFER_NONE;
        hdr.cmd_len = cmd_len;
        hdr.mx_sb_len = sense.len() as u8;
        hdr.dxfer_len = 0;
        hdr.dxferp = std::ptr::null_mut();
        hdr.cmdp = cdb.as_ptr();
        hdr.sbp = sense.as_mut_ptr();
        hdr.timeout = timeout_ms;
        hdr.flags = SG_FLAG_Q_AT_HEAD;

        let ret = unsafe { libc::ioctl(fd, SG_IO as _, &mut hdr as *mut sg_io_hdr) };
        if ret < 0 {
            return Err(Error::IoError {
                source: std::io::Error::last_os_error(),
            });
        }
        // Mask DRIVER_SENSE (0x08): it only flags "sense data present", not
        // a failure — matches execute()'s driver_status_real handling so a
        // benign CHECK CONDITION isn't misread as a transport error.
        let driver_status_real = hdr.driver_status & !super::DRIVER_SENSE;
        if hdr.host_status != 0 || driver_status_real != 0 {
            // No SCSI status was delivered: same synthesised sentinel and same
            // `sense: None` that `execute()` reports for a transport wedge.
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }
        if hdr.status != 0 {
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: hdr.status,
                sense: Some(super::parse_sense(&sense, hdr.sb_len_wr)),
            });
        }
        Ok(())
    }

    fn to_c_path(device: &Path) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt;
        let path_bytes = device.as_os_str().as_bytes();
        let mut c_path = Vec::with_capacity(path_bytes.len() + 1);
        c_path.extend_from_slice(path_bytes);
        c_path.push(0);
        c_path
    }

    /// Resolve /dev/sr* -> /dev/sg* via sysfs. If already sg, returns as-is.
    /// Falls back to the original path if resolution fails.
    fn resolve_to_sg(device: &Path) -> std::path::PathBuf {
        let dev_name = match device.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => return device.to_path_buf(),
        };

        if dev_name.starts_with("sg") {
            return device.to_path_buf();
        }

        if dev_name.starts_with("sr") {
            let sg_dir = format!("/sys/class/block/{}/device/scsi_generic", dev_name);
            if let Ok(mut entries) = std::fs::read_dir(&sg_dir)
                && let Some(Ok(entry)) = entries.next()
            {
                let sg_name = entry.file_name();
                return std::path::PathBuf::from(format!("/dev/{}", sg_name.to_string_lossy()));
            }
        }

        device.to_path_buf()
    }
}

impl Drop for SgIoTransport {
    fn drop(&mut self) {
        // A failed execute() spawns a thread that opens a fresh fd into
        // fd_recovery, normally drained at the top of the next execute(). If
        // dropped first (abort-on-wedge), claim it here.
        let recovered = claim_for_teardown(&self.fd_recovery, &self.dead);
        let owned: Vec<i32> = [Some(self.fd), recovered]
            .into_iter()
            .flatten()
            .filter(|&f| f >= 0)
            .collect();
        let close_all = move |fds: &[i32]| {
            for &f in fds {
                unsafe { libc::close(f) };
            }
        };
        let unlock = drop_unlock_fd(
            self.prevent_held,
            self.allow_transport_failed,
            self.fd,
            recovered,
        );
        let unlock_inline = |fd: i32| {
            let _ = Self::raw_command(fd, &ALLOW_MEDIUM_REMOVAL, 3_000);
            close_all(&owned);
        };
        // Cap full = SG_IO calls already stuck: close without ALLOW rather than
        // block here on the kernel EH ladder. Only a failed spawn (a resource
        // limit, not a wedge) sends ALLOW inline.
        let slot =
            unlock.is_some() && reserve_recovery_slot(&RECOVERY_THREADS, MAX_RECOVERY_THREADS);
        match drop_unlock_plan(unlock, slot) {
            DropUnlock::Skip => close_all(&owned),
            DropUnlock::Detached(fd) => {
                let fds = owned.clone();
                let spawned = std::thread::Builder::new().spawn(move || {
                    let _ = Self::raw_command(fd, &ALLOW_MEDIUM_REMOVAL, 3_000);
                    close_all(&fds);
                    release_recovery_slot(&RECOVERY_THREADS);
                });
                // Spawn failed: the slot is ours to give back, and the lock still
                // needs clearing (closing the fd does not release PREVENT).
                if spawned.is_err() {
                    release_recovery_slot(&RECOVERY_THREADS);
                    unlock_inline(fd);
                }
            }
        }
    }
}

impl ScsiTransport for SgIoTransport {
    // Execute via one synchronous SG_IO ioctl; errors map to IoError/ScsiError.
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
        // Validate CDB length before indexing `cdb[0]`: `ScsiTransport` is
        // pub, so an external caller could pass an empty or over-length
        // CDB. Shared helper keeps the check identical across platforms.
        let cmd_len = super::checked_cdb_len(cdb, K_MAX_CDB_SIZE)?;
        self.last_progress = None;
        let exec_t0 = std::time::Instant::now();
        let opcode = cdb[0];
        // Held from the moment a PREVENT is attempted: its outcome may be unknown.
        let removal = super::prevent_allow_request(cdb);
        match removal {
            Some(PREVENT) => self.prevent_held = true,
            Some(ALLOW) => self.allow_transport_failed = false,
            _ => {}
        }
        tracing::trace!(
            target: "freemkv::scsi",
            phase = "enter",
            opcode = opcode,
            timeout_ms,
            data_len = data.len(),
            fd = self.fd,
            "SgIoTransport::execute"
        );

        // Check if a background recovery has produced a new fd.
        if let Some(recovered) = take_recovered_fd(&self.fd_recovery) {
            // Close the old fd if it's still valid.
            if self.fd >= 0 {
                unsafe { libc::close(self.fd) };
            }
            self.fd = recovered;
        } else if self.fd < 0 {
            return Err(Error::DeviceNotFound {
                path: self.device_path.display().to_string(),
            });
        }

        if data.len() > u32::MAX as usize {
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        let dxfer_direction = match direction {
            DataDirection::None => SG_DXFER_NONE,
            DataDirection::FromDevice => SG_DXFER_FROM_DEV,
            DataDirection::ToDevice => SG_DXFER_TO_DEV,
        };
        let mut sense = [0u8; 32];
        let mut hdr: sg_io_hdr = unsafe { std::mem::zeroed() };
        hdr.interface_id = b'S' as i32;
        hdr.dxfer_direction = dxfer_direction;
        hdr.cmd_len = cmd_len;
        hdr.mx_sb_len = sense.len() as u8;
        hdr.dxfer_len = data.len() as u32;
        hdr.dxferp = data.as_mut_ptr();
        hdr.cmdp = cdb.as_ptr();
        hdr.sbp = sense.as_mut_ptr();
        hdr.timeout = super::effective_timeout_ms(timeout_ms);
        hdr.flags = SG_FLAG_Q_AT_HEAD;

        // The single blocking syscall: returns on response, kernel timeout,
        // or after the kernel's own error-recovery escalation. <100ms on a
        // healthy read; up to `timeout_ms` on a hung drive (host_status set).
        let ret = unsafe { libc::ioctl(self.fd, SG_IO as _, &mut hdr as *mut sg_io_hdr) };
        let exec_elapsed_ms = exec_t0.elapsed().as_millis() as u64;

        if ret < 0 {
            let errno = std::io::Error::last_os_error();
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "ioctl_err",
                opcode = opcode,
                errno = errno.raw_os_error().unwrap_or(0),
                exec_elapsed_ms,
                "ioctl(SG_IO) returned <0"
            );
            return Err(Error::IoError { source: errno });
        }

        // Transport-level failure (timeout, bridge wedge, bus error);
        // status may be 0, so surface 0xFF for `drive_has_disc` to detect.
        // Mask DRIVER_SENSE (0x08) first — it only flags sense-present.
        let driver_status_real = hdr.driver_status & !super::DRIVER_SENSE;
        if hdr.host_status != 0 || driver_status_real != 0 {
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "transport_err",
                opcode = opcode,
                host_status = hdr.host_status,
                driver_status = hdr.driver_status,
                status = hdr.status,
                exec_elapsed_ms,
                "transport-level failure (timeout / bridge wedge)"
            );

            if removal == Some(ALLOW) {
                self.allow_transport_failed = true;
            }
            // Spawn recovery: close old fd, open new one in background.
            // This prevents the main thread from blocking on close() while
            // the kernel finishes the previous ioctl.
            let old_fd = self.fd;
            self.fd = -1;
            let path = self.device_path.clone();
            let recovery = self.fd_recovery.clone();
            let dead = self.dead.clone();

            // Cap outstanding recovery threads (past MAX a sustained wedge spawns
            // unbounded threads); the atomic reservation lives in `fd_handoff`. A
            // failed spawn (resource limit) gives the slot back and works inline.
            let closed_async = reserve_recovery_slot(&RECOVERY_THREADS, MAX_RECOVERY_THREADS) && {
                let spawned = std::thread::Builder::new().spawn(move || {
                    if old_fd >= 0 {
                        unsafe { libc::close(old_fd) };
                    }
                    release_recovery_slot(&RECOVERY_THREADS);
                });
                if spawned.is_err() {
                    release_recovery_slot(&RECOVERY_THREADS);
                }
                spawned.is_ok()
            };
            if !closed_async && old_fd >= 0 {
                unsafe { libc::close(old_fd) };
            }

            let reopened_async = reserve_recovery_slot(&RECOVERY_THREADS, MAX_RECOVERY_THREADS)
                && {
                    let thread_path = path.clone();
                    let spawned = std::thread::Builder::new().spawn(move || {
                        let new_fd = Self::reopen_fd(&thread_path);
                        if new_fd >= 0 {
                            // Hand the fd to the transport. Comes back to us only if
                            // nobody there will ever close it: another recovery thread
                            // won the slot, or Drop already tore the transport down.
                            if let Some(orphan) = publish_recovered_fd(&recovery, &dead, new_fd) {
                                unsafe { libc::close(orphan) };
                            }
                        }
                        release_recovery_slot(&RECOVERY_THREADS);
                    });
                    if spawned.is_err() {
                        release_recovery_slot(&RECOVERY_THREADS);
                    }
                    spawned.is_ok()
                };
            if !reopened_async {
                // Over the cap or no thread: reopen inline (blocking this call
                // briefly); a failure leaves fd < 0 (DeviceNotFound from then on).
                let new_fd = Self::reopen_fd(&path);
                if new_fd >= 0 {
                    self.fd = new_fd;
                }
            }

            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        // SCSI-level failure: non-zero status (typically CHECK CONDITION).
        // Parse the full SPC-4 sense triple so callers can route on
        // `ScsiSense::is_medium_error()` etc.
        if hdr.status != 0 {
            let parsed = super::sense_for_status(hdr.status, &sense, hdr.sb_len_wr);
            self.last_progress =
                parsed.and_then(|_| super::parse_sense_progress(&sense, hdr.sb_len_wr));
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "scsi_err",
                opcode = opcode,
                status = hdr.status,
                sense_key = parsed.map_or(0, |s| s.sense_key),
                asc = parsed.map_or(0, |s| s.asc),
                ascq = parsed.map_or(0, |s| s.ascq),
                exec_elapsed_ms,
                "SCSI status non-zero"
            );
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: hdr.status,
                sense: parsed,
            });
        }

        // Compute in usize so 2-4 GiB transfers don't wrap through an i32
        // cast and report a large read as ~0 bytes. Negative resid is
        // clamped to 0 before subtracting.
        if removal == Some(ALLOW) {
            self.prevent_held = false;
        }
        let resid = hdr.resid.max(0) as usize;
        let bytes_transferred = data.len().saturating_sub(resid);
        tracing::trace!(
            target: "freemkv::scsi",
            phase = "ok",
            opcode = opcode,
            bytes_transferred,
            exec_elapsed_ms,
            "execute() success"
        );
        Ok(ScsiResult {
            status: hdr.status,
            bytes_transferred,
            sense,
        })
    }
}

// Discovery + presence (Linux): `list_drives` walks sysfs type-5 nodes with an
// INQUIRY each; without sysfs it keeps every sg node `keep_unfiltered_node` keeps.
// `disc_presence` sends TEST UNIT READY; a wedge (0xFF, no sense) bubbles up.

/// SCSI peripheral type 5 = "CD-ROM device" (covers DVD, BD-ROM, BD-RE, etc.).
/// Stored in `/sys/class/scsi_generic/sgN/device/type` as ASCII decimal.
const SCSI_TYPE_OPTICAL: &str = "5";

/// Maximum sg index probed in the fallback path when sysfs is unavailable.
/// Linux assigns `/dev/sgN` sequentially per host adapter; 16 covers any
/// realistic homelab (typical PERC + USB optical = ≤8 nodes).
const SG_FALLBACK_MAX: u8 = 16;

pub(super) fn list_drives() -> Vec<super::DriveInfo> {
    let mut out = Vec::new();
    let (names, type_filtered) = enumerate_sg_names();
    for name in names {
        let path = format!("/dev/{name}");
        if !std::path::Path::new(&path).exists() {
            continue;
        }

        // Read sysfs-cached identity first: the kernel's own INQUIRY at
        // probe time, stashed under `.../sgN/device/`. Survives even when
        // the drive is wedged below the USB bridge and our INQUIRY times out.
        let (sysfs_vendor, sysfs_model, sysfs_firmware) = sysfs_identity(&name);

        // INQUIRY-only probe — open transport, run INQUIRY, drop. No
        // identify, no init, no firmware reset preamble's secondary
        // commands beyond what `SgIoTransport::open` already does.
        let probed = SgIoTransport::open_with_errno(std::path::Path::new(&path))
            .map(|mut t| super::inquiry(&mut t));
        if !type_filtered {
            let probe = match &probed {
                Ok(Ok(r)) if super::is_optical_peripheral(&r.raw) => super::NodeProbe::Optical,
                Ok(Ok(_)) => super::NodeProbe::NotOptical,
                Ok(Err(_)) => super::NodeProbe::Unresponsive,
                Err((_, errno)) => super::open_failure_probe(*errno),
            };
            if !super::keep_unfiltered_node(probe) {
                continue;
            }
        }
        let info = match probed {
            Ok(Ok(r)) => super::DriveInfo {
                path: path.clone(),
                vendor: pick_identity(r.vendor_id, &sysfs_vendor),
                model: pick_identity(r.model, &sysfs_model),
                firmware: pick_identity(r.firmware, &sysfs_firmware),
            },
            // Present but unresponsive (wedged, busy, or sysfs-typed but denied):
            // listed from what sysfs knows, so autorip reports it, not an unplug.
            _ => super::DriveInfo {
                path: path.clone(),
                vendor: sysfs_vendor,
                model: sysfs_model,
                firmware: sysfs_firmware,
            },
        };
        out.push(info);
    }
    out
}

/// Prefer the live INQUIRY answer over the sysfs-cached one, but fall
/// back to sysfs when the live answer is empty (wedge / bridge bug).
fn pick_identity(live: String, sysfs: &str) -> String {
    let trimmed = live.trim();
    if trimmed.is_empty() {
        sysfs.to_string()
    } else {
        live
    }
}

/// Read the kernel's cached INQUIRY identity strings for `sgN` from
/// `/sys/class/scsi_generic/sgN/device/{vendor,model,rev}`. Empty strings
/// when sysfs is unavailable (minimal container, non-Linux filesystem).
fn sysfs_identity(name: &str) -> (String, String, String) {
    let read = |field: &str| -> String {
        std::fs::read_to_string(format!("/sys/class/scsi_generic/{name}/device/{field}"))
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    };
    (read("vendor"), read("model"), read("rev"))
}

// `sg*` names via `/sys/class/scsi_generic/`, filtered to type 5 (optical), and
// whether that filter applied: false for the unfiltered `sg0..15` fallback when
// sysfs is unreadable. Sorted so caller iteration is deterministic.
pub(crate) fn enumerate_sg_names() -> (Vec<String>, bool) {
    let mut names = Vec::new();
    let mut type_filtered = true;
    if let Ok(entries) = std::fs::read_dir("/sys/class/scsi_generic") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with("sg") {
                continue;
            }
            let type_path = format!("/sys/class/scsi_generic/{name}/device/type");
            // By design: only type-5 (optical) sg nodes are collected. A
            // non-optical or unreadable `type` file (teardown race,
            // restricted sysfs) is silently skipped, not a fatal error.
            match std::fs::read_to_string(&type_path) {
                Ok(s) if s.trim() == SCSI_TYPE_OPTICAL => names.push(name),
                Ok(_) => {}  // not optical
                Err(_) => {} // type file unreadable
            }
        }
    } else {
        // Sysfs missing: brute-force probe, unfiltered — callers must check
        // the INQUIRY peripheral type themselves.
        type_filtered = false;
        for i in 0..SG_FALLBACK_MAX {
            let name = format!("sg{i}");
            if std::path::Path::new(&format!("/dev/{name}")).exists() {
                names.push(name);
            }
        }
    }
    names.sort();
    (names, type_filtered)
}

/// Send TEST UNIT READY directly — no transport, no reset, no side effects.
pub(super) fn disc_presence(path: &Path) -> Result<super::DiscPresence> {
    let device = SgIoTransport::resolve_to_sg(path);
    let fd = SgIoTransport::open_fd(&device);
    if fd < 0 {
        return SgIoTransport::open_error(&device);
    }
    let cdb = [crate::scsi::SCSI_TEST_UNIT_READY, 0, 0, 0, 0, 0];
    let r = super::tur_disc_presence(|| {
        SgIoTransport::raw_command(fd, &cdb, crate::scsi::TUR_TIMEOUT_MS)
    });
    unsafe { libc::close(fd) };
    r
}

// After a transport failure the fd is reopened in the background and the next
// execute() adopts it. Needs a real device: FREEMKV_TEST_SG_DEVICE (default sg2).
/// Open a block device read-only (no O_DIRECT). Negative on failure.
pub(crate) fn open_block_ro(path: &str) -> i32 {
    let mut bytes = path.as_bytes().to_vec();
    bytes.push(0);
    // SAFETY: bytes is NUL-terminated and outlives the call.
    unsafe {
        libc::open(
            bytes.as_ptr() as *const libc::c_char,
            libc::O_RDONLY | libc::O_CLOEXEC,
        )
    }
}

/// Close a block fd opened by [`open_block_ro`]; the caller must not reuse it.
pub(crate) fn close_block_fd(fd: i32) {
    // SAFETY: the caller owns fd and passes it here exactly once.
    unsafe { libc::close(fd) };
}

/// Drop the page cache for the range, then `pread` into `buf` at `offset`.
/// Returns the byte count, or a negative value on error (errno set).
pub(crate) fn pread_uncached(fd: i32, buf: &mut [u8], offset: i64) -> isize {
    // SAFETY: fd is a live block fd; fadvise passes no pointers.
    let _ = unsafe { libc::posix_fadvise(fd, offset, buf.len() as i64, libc::POSIX_FADV_DONTNEED) };
    // SAFETY: buf is a valid writable slice of buf.len() bytes.
    unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), offset) }
}

#[cfg(test)]
mod recovery_device_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    #[ignore]
    fn timeout_does_not_kill_transport() {
        let device =
            std::env::var("FREEMKV_TEST_SG_DEVICE").unwrap_or_else(|_| "/dev/sg2".to_string());
        let mut transport = SgIoTransport::open(Path::new(&device)).expect("open device");
        let fd_before = transport.fd;
        // READ(10) with a 1 ms timeout forces a kernel timeout.
        let cdb = [0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let mut data = vec![0u8; 2048];
        let err = transport
            .execute(&cdb, DataDirection::FromDevice, &mut data, 1)
            .expect_err("1 ms timeout must fail");
        assert!(err.is_scsi_transport_failure(), "got {err:?}");
        assert_eq!(transport.fd, -1, "fd handed to recovery");

        let mut published = false;
        for _ in 0..100 {
            if transport
                .fd_recovery
                .load(std::sync::atomic::Ordering::Acquire)
                >= 0
            {
                published = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(published, "recovery thread should have produced a new fd");

        let t0 = std::time::Instant::now();
        let r = transport.execute(&cdb, DataDirection::FromDevice, &mut data, 5_000);
        assert!(r.is_ok(), "recovered fd should work: {r:?}");
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
        assert_ne!(transport.fd, -1, "fd valid after recovery");
        assert_ne!(transport.fd, fd_before, "fd fresh after recovery");
    }
}

// ── CDB guard wiring ───────────────────────────────────────────────────────
#[cfg(test)]
mod raw_command_cdb_guard_tests {
    use super::*;

    /// `ioctl()` on this fails with `EBADF` before touching any device, so the
    /// tests below need no `/dev/sg*` and have no side effects — they run in
    /// ordinary CI. Any error that is NOT `InvalidCdbLength` therefore means
    /// the CDB cleared the guard and the syscall was reached.
    const NO_FD: i32 = -1;

    fn reached_the_ioctl(err: &Error) -> bool {
        !matches!(err, Error::InvalidCdbLength { .. })
    }

    // ── Negative: CDBs the guard must reject ──────────────────────────────

    /// The regression this module exists for. `raw_command` used to set
    /// `cmd_len = cdb.len().min(16)`, silently shortening an over-length CDB
    /// into a *different* SPC-4 command and issuing it. It must fail the
    /// caller instead, with the same error `execute()` gives.
    ///
    /// Before the fix this test fails by reaching the `ioctl` and returning
    /// `IoError(EBADF)` — the guard never ran.
    #[test]
    fn over_length_cdb_is_rejected_not_truncated() {
        let cdb = [0u8; K_MAX_CDB_SIZE + 1];
        match SgIoTransport::raw_command(NO_FD, &cdb, 3_000) {
            Err(Error::InvalidCdbLength { len, max }) => {
                assert_eq!(len, K_MAX_CDB_SIZE + 1);
                assert_eq!(max, K_MAX_CDB_SIZE);
            }
            other => panic!(
                "over-length CDB must be rejected with InvalidCdbLength, got {other:?} — \
                 a non-guard error means it was truncated to {K_MAX_CDB_SIZE} bytes and sent"
            ),
        }
    }

    /// Well past the field width, to show the guard is a bound and not a
    /// one-off check at `max + 1`.
    #[test]
    fn far_over_length_cdb_is_rejected() {
        let cdb = [0u8; 260];
        assert!(matches!(
            SgIoTransport::raw_command(NO_FD, &cdb, 3_000),
            Err(Error::InvalidCdbLength {
                len: 260,
                max: K_MAX_CDB_SIZE
            })
        ));
    }

    /// An empty CDB must be rejected rather than handed to the sg driver as a
    /// zero-length command descriptor, which under SPC-4 is not a command at
    /// all. (The pre-fix code did not panic on this — it never read the opcode
    /// — it just issued `cmd_len = 0`. The two error paths added here DO read
    /// `cdb[0]`, so the guard is now load-bearing for that too.)
    #[test]
    fn empty_cdb_is_rejected_before_the_opcode_is_read() {
        assert!(matches!(
            SgIoTransport::raw_command(NO_FD, &[], 3_000),
            Err(Error::InvalidCdbLength {
                len: 0,
                max: K_MAX_CDB_SIZE
            })
        ));
    }

    // ── Positive: CDBs the guard must let through ─────────────────────────

    /// The only CDB `raw_command` is called with in the crate. It must still
    /// reach the syscall — a guard that rejected this would silently stop
    /// unlocking the tray on `Drop`.
    #[test]
    fn drops_allow_medium_removal_still_reaches_the_ioctl() {
        let err = SgIoTransport::raw_command(NO_FD, &ALLOW_MEDIUM_REMOVAL, 3_000)
            .expect_err("EBADF on fd -1");
        assert!(
            reached_the_ioctl(&err),
            "the 6-byte CDB Drop sends must pass the guard, got {err:?}"
        );
    }

    /// Every real SPC-4 CDB length (groups 0-5: 6, 10, 12, 16), plus the
    /// shortest a caller could legally construct, plus the boundary case — a
    /// CDB of exactly `K_MAX_CDB_SIZE` must not be caught by an off-by-one.
    #[test]
    fn in_range_cdb_lengths_reach_the_ioctl() {
        for len in [1usize, 6, 10, 12, K_MAX_CDB_SIZE] {
            let cdb = vec![0x1Eu8; len];
            let err = SgIoTransport::raw_command(NO_FD, &cdb, 3_000).expect_err("EBADF on fd -1");
            assert!(
                reached_the_ioctl(&err),
                "a {len}-byte CDB is within the {K_MAX_CDB_SIZE}-byte field and must \
                 pass the guard, got {err:?}"
            );
        }
    }

    // ── fd-reopen path helpers (used by open / drive_has_disc / recovery) ──

    /// `to_c_path` must NUL-terminate exactly once and preserve the path bytes,
    /// or `libc::open` in the reopen path reads past the buffer or opens a
    /// truncated device name.
    #[test]
    fn to_c_path_nul_terminates_and_preserves_the_bytes() {
        let c = SgIoTransport::to_c_path(Path::new("/dev/sg3"));
        assert_eq!(c.last(), Some(&0u8), "must end in a NUL for libc::open");
        assert_eq!(&c[..c.len() - 1], b"/dev/sg3", "path bytes preserved");
        assert_eq!(
            c.iter().filter(|&&b| b == 0).count(),
            1,
            "exactly one NUL, at the end"
        );
    }

    /// `resolve_to_sg` passes an sg node through unchanged, and falls back to the
    /// original path when there is no filename or the node is neither sr nor sg —
    /// the branches that need no sysfs.
    #[test]
    fn resolve_to_sg_passes_sg_through_and_falls_back_otherwise() {
        assert_eq!(
            SgIoTransport::resolve_to_sg(Path::new("/dev/sg7")),
            Path::new("/dev/sg7"),
            "an sg node is already resolved"
        );
        assert_eq!(
            SgIoTransport::resolve_to_sg(Path::new("/")),
            Path::new("/"),
            "a path with no filename falls back to itself"
        );
        assert_eq!(
            SgIoTransport::resolve_to_sg(Path::new("/dev/foo")),
            Path::new("/dev/foo"),
            "a non-sr, non-sg node is returned unchanged"
        );
    }
}
