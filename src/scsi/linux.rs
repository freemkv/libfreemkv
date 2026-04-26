//! Linux SCSI transport via async sg write/poll/read.
//!
//! Uses the sg driver's asynchronous interface instead of the blocking
//! SG_IO ioctl. Commands are submitted via write(), waited on via
//! poll() with a hard timeout, and completed via read(). If poll()
//! times out, the fd is abandoned (closed in a background thread) and
//! a fresh fd is opened. This gives us true user-controlled timeouts
//! that the kernel's USB error recovery cannot override.

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
// 64 bytes on 64-bit, 44 bytes on 32-bit (pointer-size dependent).
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<sg_io_hdr>() == 88);
#[cfg(target_pointer_width = "32")]
const _: () = assert!(std::mem::size_of::<sg_io_hdr>() == 64);

pub struct SgIoTransport {
    fd: i32,
    device_path: std::path::PathBuf,
    /// Background-recovered fd. After a poll timeout `execute()` spawns a
    /// thread that closes `self.fd` and opens a fresh fd; the new fd is
    /// stored here. The next call to `execute()` swaps it into `self.fd`.
    /// `-1` means no recovery is ready (or the recovery open failed). See
    /// RIP_DESIGN.md §7 for the design rationale.
    fd_recovery: std::sync::Arc<std::sync::atomic::AtomicI32>,
}

// SgIoTransport's contained types (i32, PathBuf, Arc<AtomicI32>) are all
// Send; the auto-derived Send is intentional. Sync is NOT — callers must
// hold &mut for execute(), which the trait object dispatch enforces.

impl SgIoTransport {
    /// Open a SCSI device for use. Resets the drive first to ensure
    /// a known good state, then opens a fresh fd for commands.
    pub fn open(device: &Path) -> Result<Self> {
        let device = Self::resolve_to_sg(device);
        Self::reset(&device)?;
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
    /// freemkv-private/postmortems/2026-04-25-bu40n-wedge-recovery.md.
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
    /// transport is constructed. Uses synchronous SG_IO — fine for
    /// short commands (TUR, PREVENT MEDIUM REMOVAL, START/STOP).
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
        if ret < 0 || hdr.status != 0 {
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
            // Unlock tray before closing — don't leave it locked
            let _ = Self::raw_command(self.fd, &[0x1E, 0, 0, 0, 0, 0], 3_000);
            unsafe { libc::close(self.fd) };
        }
        // Drain any background-recovered fd so it doesn't leak.
        let recovered = self
            .fd_recovery
            .swap(-1, std::sync::atomic::Ordering::Acquire);
        if recovered >= 0 {
            unsafe { libc::close(recovered) };
        }
    }
}

impl ScsiTransport for SgIoTransport {
    /// Execute a SCSI command with an enforceable timeout.
    ///
    /// Uses the sg driver's async write/poll/read interface:
    /// 1. write() submits the command — returns immediately
    /// 2. poll() waits for completion — respects our timeout exactly
    /// 3. read() retrieves the result — copies data to caller's buffer
    ///
    /// If poll() times out, the pending command is abandoned: the old fd
    /// is closed in a background thread (may block while kernel finishes
    /// the USB transfer) and a fresh fd is opened. The caller sees a
    /// normal SCSI error and can retry.
    ///
    /// Without SG_FLAG_DIRECT_IO, the kernel uses internal buffers for
    /// DMA and copies to userspace during read(). On timeout (no read),
    /// the caller's buffer is untouched — safe to return immediately.
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

        // Recover from a prior timeout: if a background reopen produced a
        // fresh fd, swap it in. If recovery is still pending (-1), the
        // background thread hasn't finished — return DeviceNotFound and let
        // the caller's retry loop come back later.
        if self.fd < 0 {
            let recovered = self
                .fd_recovery
                .swap(-1, std::sync::atomic::Ordering::Acquire);
            if recovered >= 0 {
                tracing::trace!(
                    target: "freemkv::scsi",
                    phase = "recovery_swap_ok",
                    new_fd = recovered,
                    "fd_recovery delivered fresh fd"
                );
                self.fd = recovered;
            } else {
                tracing::trace!(
                    target: "freemkv::scsi",
                    phase = "recovery_pending",
                    elapsed_us = exec_t0.elapsed().as_micros() as u64,
                    "fd_recovery still pending → DeviceNotFound"
                );
                return Err(Error::DeviceNotFound {
                    path: self.device_path.display().to_string(),
                });
            }
        }

        let mut sense = [0u8; 32];

        let dxfer_direction = match direction {
            DataDirection::None => SG_DXFER_NONE,
            DataDirection::FromDevice => SG_DXFER_FROM_DEV,
            DataDirection::ToDevice => SG_DXFER_TO_DEV,
        };

        if data.len() > u32::MAX as usize {
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            });
        }

        let cmd_len = cdb.len().min(16) as u8;

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

        // Submit command asynchronously via write()
        let hdr_size = std::mem::size_of::<sg_io_hdr>();
        let write_t0 = std::time::Instant::now();
        let wr = unsafe {
            libc::write(
                self.fd,
                &hdr as *const sg_io_hdr as *const libc::c_void,
                hdr_size,
            )
        };
        let write_elapsed_us = write_t0.elapsed().as_micros() as u64;
        if wr < 0 {
            let errno = std::io::Error::last_os_error();
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "write_err",
                opcode = opcode,
                errno = errno.raw_os_error().unwrap_or(0),
                write_elapsed_us,
                "sg write() returned <0"
            );
            return Err(Error::IoError { source: errno });
        }
        tracing::trace!(
            target: "freemkv::scsi",
            phase = "write_ok",
            opcode = opcode,
            wr,
            write_elapsed_us,
            "sg write() submitted"
        );

        // Wait for completion with enforceable timeout.
        // Retry on EINTR (signal interrupted poll) with remaining time.
        let poll_t0 = std::time::Instant::now();
        let deadline = poll_t0 + std::time::Duration::from_millis(timeout_ms as u64);
        let pr = loop {
            let remaining = deadline
                .saturating_duration_since(std::time::Instant::now())
                .as_millis() as i32;
            if remaining <= 0 {
                break 0; // expired
            }
            let mut pfd = libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let ret = unsafe { libc::poll(&mut pfd, 1, remaining) };
            if ret >= 0 || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
            {
                break ret;
            }
        };
        let poll_elapsed_ms = poll_t0.elapsed().as_millis() as u64;
        tracing::trace!(
            target: "freemkv::scsi",
            phase = "poll_done",
            opcode = opcode,
            pr,
            poll_elapsed_ms,
            timeout_ms,
            "poll() returned"
        );

        if pr <= 0 {
            // Timeout (0) or fatal poll error (-1). Command is still pending
            // in the kernel. Per RIP_DESIGN.md §4(b)/§7: close + reopen run
            // in a background thread so the main thread is never blocked
            // beyond the poll() budget. The recovered fd is published to
            // `fd_recovery`; the next call to execute() picks it up.
            let old_fd = self.fd;
            self.fd = -1;
            let c_path = Self::to_c_path(&self.device_path);
            let recovery = self.fd_recovery.clone();
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "timeout_spawn_recovery",
                opcode = opcode,
                old_fd,
                exec_elapsed_ms = exec_t0.elapsed().as_millis() as u64,
                "poll timeout — spawning bg close+open"
            );
            std::thread::spawn(move || {
                // Close blocks until the kernel finishes/aborts the
                // abandoned command. Then we open a fresh fd. Both happen
                // off the main thread.
                let close_t0 = std::time::Instant::now();
                unsafe { libc::close(old_fd) };
                let close_ms = close_t0.elapsed().as_millis() as u64;
                let open_t0 = std::time::Instant::now();
                let new_fd = unsafe {
                    libc::open(
                        c_path.as_ptr() as *const libc::c_char,
                        libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
                    )
                };
                let open_ms = open_t0.elapsed().as_millis() as u64;
                tracing::trace!(
                    target: "freemkv::scsi",
                    phase = "bg_recovery_done",
                    old_fd,
                    new_fd,
                    close_ms,
                    open_ms,
                    "bg recovery thread completed close+open"
                );
                if new_fd >= 0 {
                    let prev = recovery.swap(new_fd, std::sync::atomic::Ordering::Release);
                    if prev >= 0 {
                        // Stale recovery fd from a prior unclaimed attempt;
                        // close it so it doesn't leak.
                        unsafe { libc::close(prev) };
                    }
                } else {
                    recovery.store(-1, std::sync::atomic::Ordering::Release);
                }
            });

            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            });
        }

        // Read response — copies data from kernel buffer to caller's buffer
        let read_t0 = std::time::Instant::now();
        let rd = unsafe {
            libc::read(
                self.fd,
                &mut hdr as *mut sg_io_hdr as *mut libc::c_void,
                hdr_size,
            )
        };
        let read_elapsed_us = read_t0.elapsed().as_micros() as u64;
        if rd < 0 {
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "read_err",
                opcode = opcode,
                read_elapsed_us,
                exec_elapsed_ms = exec_t0.elapsed().as_millis() as u64,
                "sg read() returned <0"
            );
            return Err(Error::IoError {
                source: std::io::Error::last_os_error(),
            });
        }

        let bytes_transferred = (data.len() as i32).saturating_sub(hdr.resid).max(0) as usize;

        if hdr.status != 0 {
            let sense_key = if hdr.sb_len_wr >= 3 {
                let response_code = sense[0] & 0x7F;
                if response_code == 0x72 || response_code == 0x73 {
                    // Descriptor format sense: sense key at byte 1
                    sense[1] & 0x0F
                } else {
                    // Fixed format sense (0x70/0x71): sense key at byte 2
                    sense[2] & 0x0F
                }
            } else {
                0
            };
            tracing::trace!(
                target: "freemkv::scsi",
                phase = "scsi_err",
                opcode = opcode,
                status = hdr.status,
                sense_key,
                exec_elapsed_ms = exec_t0.elapsed().as_millis() as u64,
                "SCSI status non-zero"
            );
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: hdr.status,
                sense_key,
            });
        }

        tracing::trace!(
            target: "freemkv::scsi",
            phase = "ok",
            opcode = opcode,
            bytes_transferred,
            exec_elapsed_ms = exec_t0.elapsed().as_millis() as u64,
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
// (kernel returns status `0xff` with no sense) it escalates: SCSI bus reset
// → if still wedged → USB device reset (`USBDEVFS_RESET`) → retry TUR.
// Callers never see the escalation; if it fails too, surface
// `DeviceResetFailed` so the caller can back off.

/// SCSI peripheral type 5 = "CD-ROM device" (covers DVD, BD-ROM, BD-RE, etc.).
/// Stored in `/sys/class/scsi_generic/sgN/device/type` as ASCII decimal.
const SCSI_TYPE_OPTICAL: &str = "5";

/// SCSI sense key 2 = "NOT READY". Sub-codes distinguish "medium not present"
/// (no disc) from other not-ready states (loading, etc.); for poll-loop
/// purposes any sense-key 2 means "no disc to act on".
const SENSE_KEY_NOT_READY: u8 = 2;

/// Maximum sg index probed in the fallback path when sysfs is unavailable.
/// Linux assigns `/dev/sgN` sequentially per host adapter; 16 covers any
/// realistic homelab (typical PERC + USB optical = ≤8 nodes).
const SG_FALLBACK_MAX: u8 = 16;

// SCSI INQUIRY field-offset constants previously lived here. They were
// used by an in-process SCSI INQUIRY parse path that 0.13.6 retired in
// favour of reading the kernel-cached sysfs identity (vendor/model/rev
// under /sys/class/scsi_generic/sgN/device/). Removed to keep clippy
// -D warnings clean.

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
        // commands beyond what `SgIoTransport::open` already does (one
        // SCSI bus reset on the kernel SG fd, ~2 s).
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
                Ok(_) => {}                 // not optical
                Err(_) => names.push(name), // sysfs unreadable — let INQUIRY decide
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

/// `drive_has_disc` = single TEST UNIT READY. Any error (including our
/// synthesised wedge signature, `ScsiError { status: 0xFF }`, when
/// `execute()` times out) bubbles straight up to the caller.
///
/// ## No in-library wedge recovery — and why
///
/// Versions 0.13.1 – 0.13.3 layered `scsi::reset()` + `scsi::usb_reset()`
/// (`USBDEVFS_RESET`) escalation inside `drive_has_disc`. Production
/// testing on the LG BU40N USB BD-RE showed all three userspace recovery
/// ladders succeed at the USB transport level (the kernel logs
/// `usb 3-2: reset high-speed USB device`, the device re-authorises
/// and re-attaches on a fresh `scsi_host`) **but the drive firmware
/// below the USB bridge stays locked** — no LUN ever enumerates, TUR
/// never succeeds, /dev/sg* never reappears. Physical power-cycle
/// (unplug-replug or host reboot) is the only recovery.
///
/// Methods tried and discarded:
///  - `SG_SCSI_RESET` (device-level SCSI bus reset)
///  - `STOP UNIT` / `START UNIT` CDB pair
///  - `USBDEVFS_RESET` ioctl on `/dev/bus/usb/BBB/DDD`
///  - `/sys/bus/usb/devices/<port>/authorized` 0→1 toggle
///  - `/sys/bus/usb/drivers/usb-storage/{unbind,bind}` driver rebind
///  - Forced `echo "- - -" > /sys/class/scsi_host/hostN/scan`
///
/// Rolled back in 0.13.4. Callers (autorip, CLI) surface the error
/// directly and prompt the user to physically reconnect the drive.
/// If a future hardware class is found where USB-layer recovery
/// actually works, the escalation belongs here, gated on the wedge
/// signature — see git tag `v0.13.3` for the full implementation.
pub(super) fn drive_has_disc(path: &Path) -> Result<bool> {
    let mut transport = SgIoTransport::open(path)?;
    let cdb = [crate::scsi::SCSI_TEST_UNIT_READY, 0, 0, 0, 0, 0];
    let mut buf = [0u8; 0];
    match transport.execute(
        &cdb,
        crate::scsi::DataDirection::None,
        &mut buf,
        crate::scsi::TUR_TIMEOUT_MS,
    ) {
        Ok(_) => Ok(true),
        Err(Error::ScsiError {
            sense_key: SENSE_KEY_NOT_READY,
            ..
        }) => Ok(false),
        Err(e) => Err(e),
    }
}
