//! Windows drive discovery and device resolution.

use crate::error::Result;
use crate::identity::DriveId;
use std::path::Path;

pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();

    // Try CdRom0..CdRom15
    for i in 0..16 {
        let path = format!("\\\\.\\CdRom{}", i);
        if let Ok(mut transport) = crate::scsi::open(Path::new(&path)) {
            if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                if !id.raw_inquiry.is_empty() && (id.raw_inquiry[0] & 0x1F) == 0x05 {
                    drives.push((path, id));
                }
            }
        }
    }

    // Also try drive letters if CdRom didn't find anything
    if drives.is_empty() {
        for letter in b'D'..=b'Z' {
            let path = format!("{}:", letter as char);
            if let Ok(mut transport) = crate::scsi::open(Path::new(&path)) {
                if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                    if !id.raw_inquiry.is_empty() && (id.raw_inquiry[0] & 0x1F) == 0x05 {
                        drives.push((path, id));
                    }
                }
            }
        }
    }

    drives
}

pub fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
    Ok((normalize_path(path), None))
}

/// Normalize a device path to Windows \\.\X: format.
///
/// Accepts: "D:", "D:\\", "\\.\D:", "\\.\CdRom0"
///
/// NOTE: A near-identical `normalize_device_path` exists in `scsi::windows`.
/// Both are kept because they live in separate `cfg(windows)` modules that
/// cannot easily share a helper without introducing cross-module coupling.
fn normalize_path(path: &str) -> String {
    if path.starts_with("\\\\.\\") {
        return path.to_string();
    }
    let trimmed = path.trim_end_matches('\\');
    if trimmed.len() == 2 && trimmed.as_bytes()[1] == b':' {
        return format!("\\\\.\\{}", trimmed);
    }
    if path.to_lowercase().starts_with("cdrom") {
        return format!("\\\\.\\{}", path);
    }
    format!("\\\\.\\{}", path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_drive_letter() {
        assert_eq!(normalize_path("D:"), "\\\\.\\D:");
        assert_eq!(normalize_path("E:\\"), "\\\\.\\E:");
    }

    #[test]
    fn normalize_already_prefixed() {
        assert_eq!(normalize_path("\\\\.\\D:"), "\\\\.\\D:");
        assert_eq!(normalize_path("\\\\.\\CdRom0"), "\\\\.\\CdRom0");
    }

    #[test]
    fn normalize_cdrom() {
        assert_eq!(normalize_path("CdRom0"), "\\\\.\\CdRom0");
    }
}
