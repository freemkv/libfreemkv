//! Linux drive discovery and device resolution.

use crate::drive::DeviceResolution;
use crate::error::{Error, Result};
use crate::identity::DriveId;

/// SCSI peripheral device type 5 = MMC / optical (CD/DVD/BD), held in the
/// low 5 bits of INQUIRY byte 0 (the high 3 bits are the peripheral
/// qualifier, masked off here).
const SCSI_PERIPHERAL_TYPE_OPTICAL: u8 = 0x05;

/// Discover optical drives by enumerating `/dev/sg*` SCSI-generic nodes,
/// opening each, running INQUIRY, and keeping only devices whose
/// peripheral device type is optical (MMC, type 0x05).
///
/// Devices where `scsi::open` or `DriveId::from_drive` fail are silently
/// skipped — that is intentional for enumeration (a busy or wedged node
/// shouldn't abort discovery of the others).
pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    for name in enumerate_sg_names() {
        let path = format!("/dev/{name}");
        if !std::path::Path::new(&path).exists() {
            continue;
        }
        if let Ok(mut transport) = crate::scsi::open(std::path::Path::new(&path)) {
            if let Ok(id) = DriveId::from_drive(transport.as_mut()) {
                if !id.raw_inquiry.is_empty()
                    && (id.raw_inquiry[0] & 0x1F) == SCSI_PERIPHERAL_TYPE_OPTICAL
                {
                    drives.push((path, id));
                }
            }
        }
    }
    drives
}

/// Enumerate `sg*` device names. Linux assigns `/dev/sgN` sequentially
/// across *all* SCSI-generic devices (disks, tape, HBAs, optical), so a
/// fixed `sg0..15` range can miss an optical drive on a host with many
/// targets. Prefer the exact present-device list from
/// `/sys/class/scsi_generic/`; fall back to a bounded `sg0..15` probe
/// only when sysfs is unreadable (minimal containers).
fn enumerate_sg_names() -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(entries) = std::fs::read_dir("/sys/class/scsi_generic") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("sg") {
                names.push(name);
            }
        }
    } else {
        for i in 0..16 {
            let name = format!("sg{i}");
            if std::path::Path::new(&format!("/dev/{name}")).exists() {
                names.push(name);
            }
        }
    }
    names.sort();
    names
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
            // Require a non-empty serial before treating vendor/product/
            // serial as a unique match. serial_number falls back to an
            // empty string when GET CONFIGURATION 0108h is unavailable
            // (common on OEM drives); two same-model drives would then
            // both compare equal and the first in enumeration order would
            // win silently, resolving sr1 to sr0's sg node. An empty
            // serial can't disambiguate, so fall through to the no-match
            // path instead.
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
