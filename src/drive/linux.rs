//! Linux drive discovery and device resolution.

use crate::drive::DeviceResolution;
use crate::error::{Error, Result};
use crate::identity::DriveId;

/// Discover optical drives: `/dev/sg*` nodes sysfs reports as type 5 (all
/// nodes if sysfs is unreadable), each opened and kept only if its INQUIRY
/// peripheral device type is optical (MMC, type 0x05).
///
/// Devices where `scsi::open` or `DriveId::from_drive` fail are silently
/// skipped — that is intentional for enumeration (a busy or wedged node
/// shouldn't abort discovery of the others).
pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    for path in candidate_paths() {
        if let Ok(mut transport) = crate::scsi::open(std::path::Path::new(&path))
            && let Ok(id) = DriveId::from_drive(transport.as_mut())
            && id.is_optical()
        {
            drives.push((path, id));
        }
    }
    drives
}

/// Candidate optical `/dev/sg*` paths: from sysfs alone (nothing opened), or,
/// when sysfs is unreadable, nodes whose INQUIRY reports an optical device.
pub(crate) fn candidate_paths() -> Vec<String> {
    let (names, type_filtered) = crate::scsi::linux::enumerate_sg_names();
    let paths = names
        .into_iter()
        .map(|name| format!("/dev/{name}"))
        .filter(|path| std::path::Path::new(path).exists());
    optical_candidates(paths, type_filtered, |path| {
        crate::scsi::open(std::path::Path::new(path))
            .and_then(|mut t| crate::scsi::inquiry(t.as_mut()))
            .is_ok_and(|r| crate::scsi::is_optical_peripheral(&r.raw))
    })
}

// Keep `paths` as-is if sysfs already type-filtered them, else only those
// `is_optical` confirms.
fn optical_candidates(
    paths: impl Iterator<Item = String>,
    type_filtered: bool,
    mut is_optical: impl FnMut(&str) -> bool,
) -> Vec<String> {
    paths.filter(|p| type_filtered || is_optical(p)).collect()
}

/// Resolve a device path to its raw `/dev/sg*` SCSI-generic node.
///
/// - `/dev/sg*` paths pass through unchanged ([`DeviceResolution::Direct`]).
/// - `/dev/sr*` block paths are matched (by vendor/product/serial) to the
///   corresponding `/dev/sg*` node ([`DeviceResolution::SrToSg`]); if no
///   match is found the original path is returned with
///   [`DeviceResolution::SrNoSgMatch`].
/// - Any other existing path passes through as [`DeviceResolution::Direct`].
#[allow(dead_code)]
pub fn resolve_device(path: &str) -> Result<(String, DeviceResolution)> {
    if path.contains("/sg") {
        if !std::path::Path::new(path).exists() {
            return Err(Error::DeviceNotFound {
                path: path.to_string(),
            });
        }
        return Ok((path.to_string(), DeviceResolution::Direct));
    }
    if path.contains("/sr") {
        let mut sr_transport = crate::scsi::open(std::path::Path::new(path))?;
        let sr_id = DriveId::from_drive(sr_transport.as_mut())?;
        drop(sr_transport);
        for (sg_path, sg_id) in find_drives() {
            // Require a non-empty serial before treating vendor/product/serial as a unique
            // match: serial_number falls back to "" when GET CONFIGURATION 0108h is
            // unavailable (OEM drives), which would let same-model drives collide silently.
            if !sr_id.serial_number.is_empty()
                && sg_id.vendor_id == sr_id.vendor_id
                && sg_id.product_id == sr_id.product_id
                && sg_id.serial_number == sr_id.serial_number
            {
                return Ok((sg_path, DeviceResolution::SrToSg));
            }
        }
        return Ok((path.to_string(), DeviceResolution::SrNoSgMatch));
    }
    if !std::path::Path::new(path).exists() {
        return Err(Error::DeviceNotFound {
            path: path.to_string(),
        });
    }
    Ok((path.to_string(), DeviceResolution::Direct))
}

#[cfg(test)]
#[path = "linux_tests.rs"]
mod tests;
