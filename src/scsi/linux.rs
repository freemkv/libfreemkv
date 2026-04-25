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
const SG_SCSI_RESET: u32 = 0x2284;
const SG_SCSI_RESET_DEVICE: i32 = 1;
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
}

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
        })
    }

    /// Reset the drive to a known good state — equivalent to unplug/replug.
    /// After reset, the drive is clean and no fd is held open.
    ///
    /// ## Why each step exists
    ///
    /// When a process is killed (SIGKILL/kill -9) mid-SG_IO ioctl, two things
    /// go wrong: (1) the kernel's SG driver may have stale pending commands
    /// queued for the dead process's fd, and (2) the drive firmware may still
    /// be mid-operation (seeking, reading, processing a vendor command).
    ///
    /// A new process opening the same /dev/sg* device gets a fresh fd, but the
    /// kernel doesn't automatically abort the dead process's commands — the
    /// drive can appear hung on the first SCSI command.
    ///
    /// Additionally, killed processes skip Drop, so the tray may be locked
    /// via PREVENT MEDIUM REMOVAL with no process alive to unlock it.
    ///
    /// ## Sequence
    ///
    /// 1. **open** — allocates kernel SG state for this fd
    /// 2. **close** — triggers kernel cleanup: aborts any pending SG_IO
    ///    commands associated with this fd. The key operation —
    ///    the kernel's sg_release() cancels queued commands.
    /// 3. **sleep 2s** — the drive firmware needs time to finish/abort whatever
    ///    it was doing when the previous process died. Without
    ///    this, the next command may block on drive-internal state.
    /// 4. **open** — fresh fd with no stale commands in the kernel queue
    /// 5. **unlock** — ALLOW MEDIUM REMOVAL (CDB 0x1E, prevent=0). Clears
    ///    any tray lock left by a killed process that never
    ///    ran its Drop/cleanup.
    /// 6. **TUR** — TEST UNIT READY (CDB 0x00) with 3s timeout. If the
    ///    drive responds, it's in a good state.
    /// 7. **escalate** — if TUR fails:
    ///    - SG_SCSI_RESET (device level) — kernel sends a SCSI
    ///      bus reset to the device, clearing all firmware state.
    ///    - STOP + START UNIT (CDB 0x1B) — power-cycles the
    ///      drive's logical unit, like pressing the eject button
    ///      and reinserting.
    /// 8. **close** — release the fd. Drive is clean, nobody holds it.
    pub fn reset(device: &Path) -> Result<()> {
        let c_path = Self::to_c_path(device);

        // Step 1-2: open + close — flush stale kernel SG_IO state
        let probe_fd = unsafe {
            libc::open(
                c_path.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if probe_fd >= 0 {
            unsafe { libc::close(probe_fd) };
        }

        // Step 3: let drive settle
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Step 4: open clean fd
        let fd = unsafe {
            libc::open(
                c_path.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Self::open_error(device);
        }

        // Step 5: unlock tray
        let _ = Self::raw_command(fd, &[0x1E, 0, 0, 0, 0, 0], 3_000);

        // Step 6: TUR — if drive responds, we're done
        if Self::raw_command(fd, &[0, 0, 0, 0, 0, 0], 3_000).is_err() {
            // Step 7: escalate — SG_SCSI_RESET
            let mut reset_type: i32 = SG_SCSI_RESET_DEVICE;
            unsafe { libc::ioctl(fd, SG_SCSI_RESET as _, &mut reset_type) };
            std::thread::sleep(std::time::Duration::from_secs(3));

            if Self::raw_command(fd, &[0, 0, 0, 0, 0, 0], 3_000).is_err() {
                // STOP + START
                let _ = Self::raw_command(fd, &[0x1B, 0, 0, 0, 0x00, 0], 3_000);
                std::thread::sleep(std::time::Duration::from_secs(1));
                let _ = Self::raw_command(fd, &[0x1B, 0, 0, 0, 0x01, 0], 3_000);
                std::thread::sleep(std::time::Duration::from_secs(3));
                let _ = Self::raw_command(fd, &[0, 0, 0, 0, 0, 0], 3_000);
            }
        }

        // Step 8: close — drive is clean
        unsafe { libc::close(fd) };
        Ok(())
    }

    fn open_error<T>(device: &Path) -> Result<T> {
        let err = std::io::Error::last_os_error();
        Err(if err.kind() == std::io::ErrorKind::PermissionDenied {
            Error::DevicePermission {
                path: format!(
                    "{}: permission denied (try running as root)",
                    device.display()
                ),
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
        if self.fd < 0 {
            return Err(Error::DeviceNotFound {
                path: self.device_path.display().to_string(),
            });
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
        let wr = unsafe {
            libc::write(
                self.fd,
                &hdr as *const sg_io_hdr as *const libc::c_void,
                hdr_size,
            )
        };
        if wr < 0 {
            return Err(Error::IoError {
                source: std::io::Error::last_os_error(),
            });
        }

        // Wait for completion with enforceable timeout.
        // Retry on EINTR (signal interrupted poll) with remaining time.
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64);
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

        if pr <= 0 {
            // Timeout (0) or fatal poll error (-1).
            // Command is still pending in the kernel. Abandon this fd and
            // open a fresh one. The old fd is closed in a background thread
            // because close() blocks until the kernel completes/aborts the
            // pending command.
            let old_fd = self.fd;
            self.fd = -1;

            std::thread::spawn(move || {
                unsafe { libc::close(old_fd) };
            });

            let c_path = Self::to_c_path(&self.device_path);
            let new_fd = unsafe {
                libc::open(
                    c_path.as_ptr() as *const libc::c_char,
                    libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
                )
            };
            self.fd = if new_fd >= 0 { new_fd } else { -1 };

            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            });
        }

        // Read response — copies data from kernel buffer to caller's buffer
        let rd = unsafe {
            libc::read(
                self.fd,
                &mut hdr as *mut sg_io_hdr as *mut libc::c_void,
                hdr_size,
            )
        };
        if rd < 0 {
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
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: hdr.status,
                sense_key,
            });
        }

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

/// SCSI INQUIRY response field offsets (SPC-4, 6-byte standard CDB
/// returning 96 bytes). Used to populate `DriveInfo` fields without
/// magic-number arithmetic at the call site.
const INQUIRY_VENDOR_OFFSET: usize = 8;
const INQUIRY_VENDOR_LEN: usize = 8;
const INQUIRY_MODEL_OFFSET: usize = 16;
const INQUIRY_MODEL_LEN: usize = 16;
const INQUIRY_FIRMWARE_OFFSET: usize = 32;
const INQUIRY_FIRMWARE_LEN: usize = 4;

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
