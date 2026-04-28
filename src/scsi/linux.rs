//! Linux SCSI transport via synchronous blocking SG_IO ioctl.
//!
//! `execute()` is one syscall: `ioctl(fd, SG_IO, &hdr)` blocks until the
//! kernel completes the command (success, error, or its own timeout).
//! No userspace abort, no fd close+reopen, no SG_SCSI_RESET escalation —
//! the kernel SCSI mid-layer's `scsi_eh.rst` ladder
//! (ABORT TASK → LUN RESET → BUS RESET → HOST RESET) runs internally
//! when `hdr.timeout` expires, and by the time the ioctl returns the
//! kernel has already done what it can.
//!
//! This matches what every reference project does: MakeMKV (8 s sync
//! ioctl), sg_dd (60 s sync ioctl), the kernel default for SCSI block
//! devices (30 s `/sys/.../timeout`). See
//! `freemkv-private/docs/audits/2026-04-26-scsi-architecture-research.md`
//! for the full primary-source audit.
//!
//! Pre-0.13.20 we ran an async `write() + poll(1.5s) + close-on-timeout +
//! bg reopen` pattern. That abandoned slow-but-alive commands faster than
//! the drive could drain its internal queue, deepening the wedge
//! pattern on the LG BU40N. Reverted in 0.13.20.

use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use std::path::Path;

const SG_IO: u32 = 0x2285;
const SG_DXFER_NONE: i32 = -1;
const SG_DXFER_TO_DEV: i32 = -2;
const SG_DXFER_FROM_DEV: i32 = -3;
const SG_FLAG_Q_AT_HEAD: u32 = 0x10;

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
    fd: i32,
    device_path: std::path::PathBuf,
    fd_recovery: std::sync::Arc<std::sync::atomic::AtomicI32>,
}

impl SgIoTransport {
    /// Open a SCSI device for use.
    pub fn open(device: &Path) -> Result<Self> {
        let device = Self::resolve_to_sg(device);
        let c_path = Self::to_c_path(&device);
        let fd = unsafe {
            libc::open(
                c_path.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Self::open_error(&device);
        }
        Ok(SgIoTransport {
            fd,
            device_path: device,
            fd_recovery: std::sync::Arc::new(std::sync::atomic::AtomicI32::new(-1)),
        })
    }

    /// Clean up kernel SG_IO state and unlock the tray. NOT a hardware
    /// reset — purely software cleanup before this process opens the
    /// device for real work.
    ///
    /// When a previous process is killed (SIGKILL) mid-SG_IO, the kernel
    /// may hold queued commands against the dead fd, and `Drop` never
    /// ran so the tray may still be locked via PREVENT MEDIUM REMOVAL.
    /// This routine handles both: open + close flushes the kernel SG
    /// queue (sg_release cancels commands tied to the fd), the 2 s sleep
    /// gives the kernel time to finish that cleanup, then a fresh fd
    /// sends ALLOW MEDIUM REMOVAL to clear any stale tray lock.
    ///
    /// We do NOT verify the drive with TUR or escalate to SG_SCSI_RESET /
    /// STOP+START UNIT. Both escalations were tried in 0.13.0–0.13.5
    /// against the LG BU40N (Initio USB-SATA bridge); both failed to
    /// recover wedged drives and made the wedge worse — see
    /// `freemkv-private/postmortems/2026-04-25-bu40n-wedge-recovery.md`.
    /// If the drive is genuinely unresponsive, the next workload command
    /// fails naturally and the caller surfaces a "physical reconnect
    /// required" prompt. Software has no path back from a wedged Initio
    /// bridge — only physical replug clears it.
    pub fn reset(device: &Path) -> Result<()> {
        let c_path = Self::to_c_path(device);

        // open + close — make the kernel cancel any SG_IO commands queued
        // against a previous fd that didn't close cleanly.
        let probe_fd = unsafe {
            libc::open(
                c_path.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if probe_fd >= 0 {
            unsafe { libc::close(probe_fd) };
        }

        // Let the kernel finish that cancellation before we reopen.
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Fresh fd just to send the unlock command, then close.
        let fd = unsafe {
            libc::open(
                c_path.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Self::open_error(device);
        }

        // ALLOW MEDIUM REMOVAL — clear any tray lock left by a killed
        // process whose Drop never ran. Best-effort; ignore result.
        let _ = Self::raw_command(fd, &[0x1E, 0, 0, 0, 0, 0], 3_000);

        unsafe { libc::close(fd) };
        Ok(())
    }

    fn open_error<T>(device: &Path) -> Result<T> {
        let err = std::io::Error::last_os_error();
        Err(if err.kind() == std::io::ErrorKind::PermissionDenied {
            Error::DevicePermission {
                path: device.display().to_string(),
            }
        } else {
            Error::DeviceNotFound {
                path: device.display().to_string(),
            }
        })
    }

    /// Send a raw SCSI command on an fd. Used by reset() before the
    /// transport is constructed.
    fn raw_command(fd: i32, cdb: &[u8], timeout_ms: u32) -> std::result::Result<(), ()> {
        let mut sense = [0u8; 32];
        let mut hdr: sg_io_hdr = unsafe { std::mem::zeroed() };
        hdr.interface_id = b'S' as i32;
        hdr.dxfer_direction = SG_DXFER_NONE;
        hdr.cmd_len = cdb.len().min(16) as u8;
        hdr.mx_sb_len = sense.len() as u8;
        hdr.dxfer_len = 0;
        hdr.dxferp = std::ptr::null_mut();
        hdr.cmdp = cdb.as_ptr();
        hdr.sbp = sense.as_mut_ptr();
        hdr.timeout = timeout_ms;
        hdr.flags = SG_FLAG_Q_AT_HEAD;

        let ret = unsafe { libc::ioctl(fd, SG_IO as _, &mut hdr as *mut sg_io_hdr) };
        if ret < 0 || hdr.status != 0 || hdr.host_status != 0 || hdr.driver_status != 0 {
            Err(())
        } else {
            Ok(())
        }
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
            if let Ok(mut entries) = std::fs::read_dir(&sg_dir) {
                if let Some(Ok(entry)) = entries.next() {
                    let sg_name = entry.file_name();
                    return std::path::PathBuf::from(format!("/dev/{}", sg_name.to_string_lossy()));
                }
            }
        }

        device.to_path_buf()
    }
}

impl Drop for SgIoTransport {
    fn drop(&mut self) {
        if self.fd >= 0 {
            // Unlock tray before closing — don't leave it locked.
            let _ = Self::raw_command(self.fd, &[0x1E, 0, 0, 0, 0, 0], 3_000);
            unsafe { libc::close(self.fd) };
        }
    }
}

impl ScsiTransport for SgIoTransport {
    /// Execute a SCSI command via synchronous blocking SG_IO.
    ///
    /// One syscall: `ioctl(fd, SG_IO, &hdr)`. The kernel honors
    /// `hdr.timeout` and runs its own ABORT TASK → LUN RESET → BUS
    /// RESET → HOST RESET escalation if the device times out (per
    /// `Documentation/scsi/scsi_eh.rst`). By the time this returns,
    /// the kernel has done its recovery work.
    ///
    /// Errors we surface to caller (any of these = command failed):
    ///
    ///   - ioctl returned -1 → `Error::IoError` (kernel-level failure)
    ///   - `hdr.host_status` != 0 OR `(hdr.driver_status & ~DRIVER_SENSE)` != 0
    ///     → `Error::ScsiError { status: 0xFF, sense_key: 0, asc: 0, ascq: 0 }`
    ///     (real transport-layer failure: kernel timeout, bridge wedge, bus error)
    ///   - `hdr.status` != 0 (typically `0x02` CHECK CONDITION) →
    ///     `Error::ScsiError { status, sense_key, asc, ascq }` carrying the
    ///     drive's full SPC-4 sense triple. Callers route on
    ///     `is_medium_error()`, `is_unit_attention()`, etc.
    ///
    /// Note: SG's `DRIVER_SENSE` (0x08) bit indicates *sense data is
    /// attached* — it's set on every CHECK CONDITION reply. It is **not**
    /// a transport failure; pre-0.13.23 we conflated it with one and
    /// silently lost every drive-reported error reason. The mask in the
    /// transport-error check below is the fix.
    ///
    /// Caller's `data` buffer is mutated only on success; partial
    /// transfers are reported via `bytes_transferred = data.len() - resid`.
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let exec_t0 = std::time::Instant::now();
        let opcode = cdb[0];
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
        let recovered = self
            .fd_recovery
            .swap(-1, std::sync::atomic::Ordering::Acquire);
        if recovered >= 0 {
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
        let cmd_len = cdb.len().min(16) as u8;

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
        hdr.timeout = timeout_ms;
        hdr.flags = SG_FLAG_Q_AT_HEAD;

        // The single blocking syscall. Returns when the device responds,
        // when the kernel's timeout fires, or when the kernel's error
        // recovery completes its escalation. On a healthy read this is
        // <100 ms; on a slow-recovery bad sector it can be tens of
        // seconds; on a hung drive it returns at `timeout_ms` with
        // `host_status` flagged.
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

        // Transport-level failure (kernel timeout, USB bridge wedge,
        // bus error). `hdr.status` may still be zero — the SCSI device
        // never got to send a status byte. Surface as 0xFF so callers
        // (e.g. `drive_has_disc`) can detect the wedge signature.
        //
        // 0.13.23: mask out `DRIVER_SENSE` (0x08) before treating
        // `driver_status` as a transport failure. That bit is set on
        // *every* CHECK CONDITION reply just to flag "sense data is
        // attached in `sbp`" — it's not an error of its own. Pre-fix
        // we collapsed every drive-reported error into a synthetic
        // 0xFF wedge signature and discarded the sense data, which
        // killed the rip's classification logic on damaged discs.
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

            // Spawn recovery: close old fd, open new one in background.
            // This prevents the main thread from blocking on close() while
            // the kernel finishes the previous ioctl.
            let old_fd = self.fd;
            self.fd = -1;
            let path = self.device_path.clone();
            let recovery = self.fd_recovery.clone();

            std::thread::spawn(move || {
                if old_fd >= 0 {
                    unsafe { libc::close(old_fd) };
                }
            });

            std::thread::spawn(move || {
                let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
                let new_fd = unsafe {
                    libc::open(
                        c_path.as_ptr() as *const libc::c_char,
                        libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
                    )
                };
                recovery.store(new_fd, std::sync::atomic::Ordering::Release);
            });

            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: super::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }

        // SCSI-level failure: device responded, returned non-zero status
        // (typically 0x02 CHECK CONDITION). Parse the full SPC-4 sense
        // triple so callers can route on `ScsiSense::is_medium_error()`
        // etc.
        if hdr.status != 0 {
            let parsed = super::parse_sense(&sense, hdr.sb_len_wr);
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "scsi_err",
                opcode = opcode,
                status = hdr.status,
                sense_key = parsed.sense_key,
                asc = parsed.asc,
                ascq = parsed.ascq,
                exec_elapsed_ms,
                "SCSI status non-zero"
            );
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: hdr.status,
                sense: Some(parsed),
            });
        }

        let bytes_transferred = (data.len() as i32).saturating_sub(hdr.resid).max(0) as usize;
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

// ── Lightweight discovery + presence (Linux) ────────────────────────────────
//
// `list_drives` walks `/sys/class/scsi_generic/`, filters to type-5 (CD/DVD/BD),
// and runs one INQUIRY each for vendor/model/firmware. Falls back to a
// `/dev/sg0..15` probe when sysfs is unreadable (minimal containers).
//
// `drive_has_disc` issues a single TEST UNIT READY. On the wedge signature
// (kernel returns status `0xff` with no sense — synthesised by `execute()`
// from a non-zero `host_status`) the error bubbles directly to the caller;
// no in-library reset escalation. See the rationale block on
// `drive_has_disc` below.

/// SCSI peripheral type 5 = "CD-ROM device" (covers DVD, BD-ROM, BD-RE, etc.).
/// Stored in `/sys/class/scsi_generic/sgN/device/type` as ASCII decimal.
const SCSI_TYPE_OPTICAL: &str = "5";

/// Maximum sg index probed in the fallback path when sysfs is unavailable.
/// Linux assigns `/dev/sgN` sequentially per host adapter; 16 covers any
/// realistic homelab (typical PERC + USB optical = ≤8 nodes).
const SG_FALLBACK_MAX: u8 = 16;

pub(super) fn list_drives() -> Vec<super::DriveInfo> {
    let mut out = Vec::new();
    let names = enumerate_sg_names();
    for name in names {
        let path = format!("/dev/{name}");
        if !std::path::Path::new(&path).exists() {
            continue;
        }

        // Read sysfs-cached identity first. The kernel runs its own INQUIRY
        // at device probe time and stashes vendor/model/rev under
        // `/sys/class/scsi_generic/sgN/device/`. Those values survive even
        // when the drive firmware is wedged below the USB bridge (our own
        // INQUIRY times out but sysfs still has the pre-wedge answer), so
        // the UI always has a human-readable identity to show.
        let (sysfs_vendor, sysfs_model, sysfs_firmware) = sysfs_identity(&name);

        // INQUIRY-only probe — open transport, run INQUIRY, drop. No
        // identify, no init, no firmware reset preamble's secondary
        // commands beyond what `SgIoTransport::open` already does.
        let info = match SgIoTransport::open(std::path::Path::new(&path)) {
            Ok(mut transport) => match super::inquiry(&mut transport) {
                Ok(r) => super::DriveInfo {
                    path: path.clone(),
                    vendor: pick_identity(r.vendor_id, &sysfs_vendor),
                    model: pick_identity(r.model, &sysfs_model),
                    firmware: pick_identity(r.firmware, &sysfs_firmware),
                },
                Err(_) => super::DriveInfo {
                    path: path.clone(),
                    vendor: sysfs_vendor,
                    model: sysfs_model,
                    firmware: sysfs_firmware,
                },
            },
            Err(_) => super::DriveInfo {
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

/// Enumerate `sg*` names via `/sys/class/scsi_generic/`, filtered to
/// SCSI peripheral type 5 (optical). Falls back to a `sg0..15` probe
/// when sysfs is unreadable. Returns names sorted lexically so caller
/// iteration is deterministic.
fn enumerate_sg_names() -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(entries) = std::fs::read_dir("/sys/class/scsi_generic") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with("sg") {
                continue;
            }
            let type_path = format!("/sys/class/scsi_generic/{name}/device/type");
            match std::fs::read_to_string(&type_path) {
                Ok(s) if s.trim() == SCSI_TYPE_OPTICAL => names.push(name),
                Ok(_) => {} // not optical
                Err(_) => {}
            }
        }
    } else {
        // Sysfs missing — fall back to a brute-force probe. The INQUIRY
        // step in `list_drives` filters non-optical responses naturally.
        for i in 0..SG_FALLBACK_MAX {
            let name = format!("sg{i}");
            if std::path::Path::new(&format!("/dev/{name}")).exists() {
                names.push(name);
            }
        }
    }
    names.sort();
    names
}

/// Send TEST UNIT READY directly — no transport, no reset, no side effects.
pub(super) fn drive_has_disc(path: &Path) -> Result<bool> {
    let device = SgIoTransport::resolve_to_sg(path);
    let c_path = SgIoTransport::to_c_path(&device);
    let fd = unsafe {
        libc::open(
            c_path.as_ptr() as *const libc::c_char,
            libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return SgIoTransport::open_error(&device);
    }

    let cdb = [crate::scsi::SCSI_TEST_UNIT_READY, 0, 0, 0, 0, 0];
    let mut sense = [0u8; 32];
    let mut hdr: sg_io_hdr = unsafe { std::mem::zeroed() };
    hdr.interface_id = b'S' as i32;
    hdr.dxfer_direction = SG_DXFER_NONE;
    hdr.cmd_len = cdb.len() as u8;
    hdr.mx_sb_len = sense.len() as u8;
    hdr.dxfer_len = 0;
    hdr.dxferp = std::ptr::null_mut();
    hdr.cmdp = cdb.as_ptr();
    hdr.sbp = sense.as_mut_ptr();
    hdr.timeout = crate::scsi::TUR_TIMEOUT_MS;
    hdr.flags = SG_FLAG_Q_AT_HEAD;

    let ret = unsafe { libc::ioctl(fd, SG_IO as _, &mut hdr as *mut sg_io_hdr) };
    unsafe { libc::close(fd) };

    if ret < 0 {
        return Err(Error::IoError {
            source: std::io::Error::last_os_error(),
        });
    }

    let driver_status_real = hdr.driver_status & !super::DRIVER_SENSE;
    if hdr.host_status != 0 || driver_status_real != 0 {
        return Err(Error::ScsiError {
            opcode: cdb[0],
            status: super::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
    }

    if hdr.status == 0 {
        return Ok(true);
    }

    let parsed = super::parse_sense(&sense, hdr.sb_len_wr);
    if parsed.is_not_ready() {
        Ok(false)
    } else {
        Err(Error::ScsiError {
            opcode: cdb[0],
            status: hdr.status,
            sense: Some(parsed),
        })
    }
}
