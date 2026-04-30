//! macOS drive discovery and device resolution.
//!
//! `find_drives` uses IOKit registry enumeration (via `scsi::list_drives`)
//! to discover optical drives without exclusive access or unmounts. Only
//! the returned paths are then opened for INQUIRY to build full `DriveId`.

use crate::error::{Error, Result};
use crate::identity::DriveId;

pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    let discovered = crate::scsi::list_drives();
    for info in discovered {
        let path = std::path::Path::new(&info.path);
        match crate::scsi::open(path) {
            Ok(mut transport) => {
                if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                    drives.push((info.path.clone(), id));
                }
            }
            Err(_) => {
                continue;
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
