//! macOS `statfs`-based filesystem type detection.
//!
//! macOS exposes a textual `f_fstypename` field (e.g. `"apfs"`, `"hfs"`,
//! `"nfs"`, `"smbfs"`) on its `statfs` struct, which is far more
//! reliable than chasing magic numbers. Anything starting with `"nfs"`
//! counts as NFS; anything else recognised maps to `Local`; otherwise
//! `Unknown`.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use super::FsType;

pub(super) fn detect_impl(path: &Path) -> FsType {
    let cpath = match CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return FsType::Unknown,
    };
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs(cpath.as_ptr(), &mut buf) };
    if rc != 0 {
        return FsType::Unknown;
    }
    // `f_fstypename` is a NUL-terminated C string of length MFSTYPENAMELEN.
    // SAFETY: libc guarantees the field is initialised to a NUL-terminated
    // string by a successful statfs.
    let name_ptr = buf.f_fstypename.as_ptr();
    let cstr = unsafe { std::ffi::CStr::from_ptr(name_ptr) };
    let name = cstr.to_bytes();
    if name.starts_with(b"nfs") {
        return FsType::Nfs;
    }
    // Recognised local types. SMB is not NFS but the buffering-policy
    // outcome on macOS is the same as for any other local FS — there's
    // no `WritebackFile` machinery to opt out of on this OS.
    match name {
        b"apfs" | b"hfs" | b"exfat" | b"msdos" | b"tmpfs" | b"smbfs" | b"webdav" => FsType::Local,
        _ => FsType::Unknown,
    }
}
