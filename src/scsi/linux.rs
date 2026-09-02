//! Linux SCSI transport via synchronous blocking SG_IO ioctl.
//!
//! `execute()` is one syscall: `ioctl(fd, SG_IO, &hdr)` blocks until the
//! kernel completes the command (success, error, or its own timeout).
//! No userspace abort, no fd close+reopen, no SG_SCSI_RESET escalation —
//! the kernel SCSI mid-layer's own error-handling ladder runs internally
//! when `hdr.timeout` expires.
//!
//! See docs/scsi-linux.md for the escalation ladder, the design
//! rationale, and the pre-0.13.20 async-poll design it replaced.

use super::{DataDirection, ScsiResult, ScsiTransport};
use crate::error::{Error, Result};
use std::path::Path;

const SG_IO: u32 = 0x2285;
const SG_DXFER_NONE: i32 = -1;
const SG_DXFER_TO_DEV: i32 = -2;
const SG_DXFER_FROM_DEV: i32 = -3;
const SG_FLAG_Q_AT_HEAD: u32 = 0x10;

/// Width of `sg_io_hdr.cmdp` as far as SG_IO is concerned: `cmd_len` is a
/// single byte and every SPC-4/MMC command this crate issues is 6, 10, 12 or
/// 16 bytes. Matches `K_MAX_CDB_SIZE` in the macOS and Windows backends.
const K_MAX_CDB_SIZE: usize = 16;

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
    pub fd: i32,
    device_path: std::path::PathBuf,
    pub fd_recovery: std::sync::Arc<std::sync::atomic::AtomicI32>,
    /// Set to `true` by `Drop` before the transport is torn down. The
    /// recovery thread checks this after a successful `compare_exchange`
    /// and closes `new_fd` itself when the transport is already gone,
    /// preventing an fd leak when Drop races the recovery thread.
    dead: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
            dead: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    // Map errno from a failed open(): permission-denied -> DevicePermission,
    // else DeviceNotFound. Path carried in the error; no English text (app
    // layer localizes).
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
        // Mask DRIVER_SENSE (0x08): it only flags "sense data present", not
        // a failure — matches execute()'s driver_status_real handling so a
        // benign CHECK CONDITION isn't misread as a transport error.
        let driver_status_real = hdr.driver_status & !super::DRIVER_SENSE;
        if ret < 0 || hdr.status != 0 || hdr.host_status != 0 || driver_status_real != 0 {
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
        if self.fd >= 0 {
            // Unlock tray before closing — don't leave it locked.
            let _ = Self::raw_command(self.fd, &[0x1E, 0, 0, 0, 0, 0], 3_000);
            unsafe { libc::close(self.fd) };
        }
        // Signal the recovery thread this transport is gone. Must be set
        // before the fd_recovery swap so it can't observe dead=false and
        // store into a slot Drop is no longer going to drain.
        self.dead.store(true, std::sync::atomic::Ordering::Release);
        // A failed execute() spawns a thread that opens a fresh fd into
        // fd_recovery, normally drained at the top of the next execute().
        // If dropped first (abort-on-wedge), claim and close it here.
        let recovered = self
            .fd_recovery
            .swap(-1, std::sync::atomic::Ordering::Acquire);
        if recovered >= 0 {
            unsafe { libc::close(recovered) };
        }
    }
}

impl ScsiTransport for SgIoTransport {
    // Execute via one synchronous SG_IO ioctl; errors map to IoError/ScsiError.
    // See docs/scsi-linux.md — error mapping, DRIVER_SENSE masking, and
    // partial-transfer notes.
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

            // Spawn recovery: close old fd, open new one in background.
            // This prevents the main thread from blocking on close() while
            // the kernel finishes the previous ioctl.
            let old_fd = self.fd;
            self.fd = -1;
            let path = self.device_path.clone();
            let recovery = self.fd_recovery.clone();
            let dead = self.dead.clone();

            std::thread::spawn(move || {
                if old_fd >= 0 {
                    unsafe { libc::close(old_fd) };
                }
            });

            std::thread::spawn(move || {
                // Don't unwrap: a device path with an interior NUL would
                // panic this detached thread (silently swallowed). Bail
                // and leave fd_recovery untouched instead.
                let c_path = match std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let new_fd = unsafe {
                    libc::open(
                        c_path.as_ptr() as *const libc::c_char,
                        libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
                    )
                };
                if new_fd < 0 {
                    return;
                }
                // Publish only into an empty (-1) slot. If two recovery
                // threads race, the loser closes its own fd rather than
                // overwriting (and leaking) the winner's.
                if recovery
                    .compare_exchange(
                        -1,
                        new_fd,
                        std::sync::atomic::Ordering::Release,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_err()
                {
                    // Another recovery thread already stored its fd; ours
                    // was not stored so it's our responsibility to close it.
                    unsafe { libc::close(new_fd) };
                    return;
                }
                // Check whether Drop raced us: if the transport is already
                // dead it won't drain fd_recovery, so swap to atomically
                // claim new_fd and close it; a -1 result means Drop won.
                if dead.load(std::sync::atomic::Ordering::Acquire) {
                    let claimed = recovery.swap(-1, std::sync::atomic::Ordering::AcqRel);
                    if claimed >= 0 {
                        unsafe { libc::close(claimed) };
                    }
                }
            });

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

        // Compute in usize so 2-4 GiB transfers don't wrap through an i32
        // cast and report a large read as ~0 bytes. Negative resid is
        // clamped to 0 before subtracting.
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

// Lightweight discovery + presence (Linux): `list_drives` walks sysfs
// type-5 nodes with an INQUIRY each. `drive_has_disc` sends TEST UNIT
// READY; on the wedge signature (status `0xff`, no sense) it bubbles up.

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

        // Read sysfs-cached identity first: the kernel's own INQUIRY at
        // probe time, stashed under `.../sgN/device/`. Survives even when
        // the drive is wedged below the USB bridge and our INQUIRY times out.
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

// Enumerate `sg*` names via `/sys/class/scsi_generic/`, filtered to type 5
// (optical). Falls back to a `sg0..15` probe when sysfs is unreadable.
// Names sorted lexically so caller iteration is deterministic.
fn enumerate_sg_names() -> Vec<String> {
    let mut names = Vec::new();
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
    // Capture the ioctl errno BEFORE close(): POSIX permits close() to
    // set errno (e.g. EIO on a flaky USB path), which would otherwise
    // clobber the ioctl failure reason reported below.
    let ioctl_err = std::io::Error::last_os_error();
    unsafe { libc::close(fd) };

    if ret < 0 {
        return Err(Error::IoError { source: ioctl_err });
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
