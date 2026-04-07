//! Linux SCSI transport via SG_IO ioctl.

use crate::error::{Error, Result};
use super::{ScsiTransport, ScsiResult, DataDirection};
use std::path::Path;

const SG_IO: u32 = 0x2285;
const SG_DXFER_NONE: i32 = -1;
const SG_DXFER_TO_DEV: i32 = -2;
const SG_DXFER_FROM_DEV: i32 = -3;

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

pub struct SgIoTransport {
    fd: i32,
}

impl SgIoTransport {
    pub fn open(device: &Path) -> Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        let path_bytes = device.as_os_str().as_bytes();
        let mut c_path = Vec::with_capacity(path_bytes.len() + 1);
        c_path.extend_from_slice(path_bytes);
        c_path.push(0);

        let fd = unsafe {
            libc::open(c_path.as_ptr() as *const libc::c_char, libc::O_RDWR | libc::O_NONBLOCK)
        };
        if fd < 0 {
            return Err(Error::DeviceNotFound { path: device.display().to_string() });
        }
        Ok(SgIoTransport { fd })
    }
}

impl Drop for SgIoTransport {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd); }
    }
}

impl ScsiTransport for SgIoTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let mut sense = [0u8; 32];

        let dxfer_direction = match direction {
            DataDirection::None => SG_DXFER_NONE,
            DataDirection::FromDevice => SG_DXFER_FROM_DEV,
            DataDirection::ToDevice => SG_DXFER_TO_DEV,
        };

        let mut hdr: sg_io_hdr = unsafe { std::mem::zeroed() };
        hdr.interface_id = b'S' as i32;
        hdr.dxfer_direction = dxfer_direction;
        hdr.cmd_len = cdb.len() as u8;
        hdr.mx_sb_len = sense.len() as u8;
        hdr.dxfer_len = data.len() as u32;
        hdr.dxferp = data.as_mut_ptr();
        hdr.cmdp = cdb.as_ptr();
        hdr.sbp = sense.as_mut_ptr();
        hdr.timeout = timeout_ms;

        let ret = unsafe {
            libc::ioctl(self.fd, SG_IO as _, &mut hdr as *mut sg_io_hdr)
        };

        if ret < 0 {
            return Err(Error::IoError { source: std::io::Error::last_os_error() });
        }

        let bytes_transferred = (data.len() as i32 - hdr.resid) as usize;

        if hdr.status != 0 {
            let sense_key = if hdr.sb_len_wr > 2 { sense[2] & 0x0F } else { 0 };
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
