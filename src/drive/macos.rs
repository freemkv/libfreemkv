//! macOS drive discovery and device resolution.
//!
//! `find_drives` uses IOKit registry enumeration (via `scsi::list_drives`)
//! to discover optical drives without exclusive access or unmounts. Only
//! the returned paths are then opened for INQUIRY to build full `DriveId`.

use crate::drive::DeviceResolution;
use crate::error::{Error, Result};
use crate::identity::DriveId;

/// SCSI peripheral device type 5 = MMC / optical, in the low 5 bits of
/// INQUIRY byte 0.
const SCSI_PERIPHERAL_TYPE_OPTICAL: u8 = 0x05;

/// Discover optical drives via the IOKit registry (`scsi::list_drives`),
/// then open each candidate for INQUIRY to build a full `DriveId`.
///
/// Any drive where `scsi::open` or `DriveId::from_drive` fails, or whose
/// peripheral device type is not optical (MMC, type 0x05), is silently
/// skipped — the same MMC filter the Linux and Windows backends apply.
pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    let discovered = crate::scsi::list_drives();
    for info in discovered {
        let path = std::path::Path::new(&info.path);
        match crate::scsi::open(path) {
            Ok(mut transport) => {
                if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                    if !id.raw_inquiry.is_empty()
                        && (id.raw_inquiry[0] & 0x1F) == SCSI_PERIPHERAL_TYPE_OPTICAL
                    {
                        drives.push((info.path.clone(), id));
                    }
                }
            }
            Err(_) => {
                continue;
            }
        }
    }
    drives
}

/// Resolve a device path on macOS. There is no `sr`→`sg` style
/// substitution here (that is a Linux concern), so any existing path is
/// returned unchanged as [`DeviceResolution::Direct`]; the
/// [`DeviceResolution`] return exists for cross-platform signature parity.
pub fn resolve_device(path: &str) -> Result<(String, DeviceResolution)> {
    if !std::path::Path::new(path).exists() {
        return Err(Error::DeviceNotFound {
            path: path.to_string(),
        });
    }
    Ok((path.to_string(), DeviceResolution::Direct))
}
