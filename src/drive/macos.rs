//! macOS drive discovery and device resolution.

use crate::error::{Error, Result};
use crate::identity::DriveId;

pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    for i in 0..16 {
        let path = format!("/dev/disk{}", i);
        if let Ok(mut transport) = crate::scsi::open(std::path::Path::new(&path)) {
            if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                if !id.raw_inquiry.is_empty() && (id.raw_inquiry[0] & 0x1F) == 0x05 {
                    drives.push((path, id));
                }
            }
        }
    }
    drives
}

pub fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
    // Accept /dev/diskN or /dev/rdiskN paths as-is
    if path.contains("/disk") || path.contains("/rdisk") {
        if !std::path::Path::new(path).exists() {
            return Err(Error::DeviceNotFound {
                path: path.to_string(),
            });
        }
        return Ok((path.to_string(), None));
    }
    if !std::path::Path::new(path).exists() {
        return Err(Error::DeviceNotFound {
            path: path.to_string(),
        });
    }
    Ok((path.to_string(), None))
}
