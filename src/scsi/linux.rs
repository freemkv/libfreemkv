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
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms as u64);
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
