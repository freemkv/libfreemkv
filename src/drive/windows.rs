//! Windows drive discovery and device resolution.

use crate::drive::DeviceResolution;
use crate::error::Result;
use crate::identity::DriveId;
use std::path::Path;

/// Discover optical drives. Probes `\\.\CdRom0..15` first; only if none
/// are found does it fall back to scanning drive letters `D..Z`. Each
/// candidate is opened, INQUIRY'd, and kept only if its peripheral device
/// type is optical (MMC, type 0x05). Returns normalized `\\.\` paths.
pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();

    // Try CdRom0..CdRom15
    for i in 0..16 {
        let path = format!("\\\\.\\CdRom{}", i);
        if let Ok(mut transport) = crate::scsi::open(Path::new(&path))
            && let Ok(id) = DriveId::from_drive(transport.as_mut())
            && id.is_optical()
        {
            drives.push((path, id));
        }
    }

    // Also try drive letters if CdRom didn't find anything
    if drives.is_empty() {
        for letter in b'D'..=b'Z' {
            let path = format!("{}:", letter as char);
            if let Ok(mut transport) = crate::scsi::open(Path::new(&path))
                && let Ok(id) = DriveId::from_drive(transport.as_mut())
                && id.is_optical()
            {
                // Normalize so returned paths are consistently in
                // \\.\ form regardless of which loop matched.
                drives.push((normalize_path(&path), id));
            }
        }
    }

    drives
}

/// Candidate drive paths for `find_drive`.
pub(crate) fn candidate_paths() -> Vec<String> {
    find_drives().into_iter().map(|(path, _)| path).collect()
}

/// Resolve a device path to its normalized Windows `\\.\` form. Windows
/// has no `sr`→`sg` symlink-target indirection, so resolution is purely a
/// path normalization and always reports [`DeviceResolution::Direct`].
pub fn resolve_device(path: &str) -> Result<(String, DeviceResolution)> {
    Ok((normalize_path(path), DeviceResolution::Direct))
}

// Normalize a device path ("D:", "D:\\", "\\.\D:", "\\.\CdRom0") to Windows \\.\X: format.
fn normalize_path(path: &str) -> String {
    if path.starts_with("\\\\.\\") {
        return path.to_string();
    }
    let trimmed = path.trim_end_matches('\\');
    if trimmed.len() == 2 && trimmed.as_bytes()[1] == b':' {
        return format!("\\\\.\\{}", trimmed);
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
