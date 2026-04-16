//! Linux drive discovery and device resolution.

use crate::error::{Error, Result};
use crate::identity::DriveId;

pub fn find_drives() -> Vec<(String, DriveId)> {
    let mut drives = Vec::new();
    for i in 0..16 {
        let path = format!("/dev/sg{i}");
        if !std::path::Path::new(&path).exists() {
            continue;
        }
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

#[allow(dead_code)]
pub fn resolve_device(path: &str) -> Result<(String, Option<String>)> {
    if path.contains("/sg") {
        if !std::path::Path::new(path).exists() {
            return Err(Error::DeviceNotFound {
                path: path.to_string(),
            });
        }
        return Ok((path.to_string(), None));
    }
    if path.contains("/sr") {
        let mut sr_transport = crate::scsi::open(std::path::Path::new(path))?;
        let sr_id = DriveId::from_drive(sr_transport.as_mut())?;
        drop(sr_transport);
        for (sg_path, sg_id) in find_drives() {
            if sg_id.vendor_id == sr_id.vendor_id
                && sg_id.product_id == sr_id.product_id
                && sg_id.serial_number == sr_id.serial_number
            {
                let warning =
                    format!("{path} is a block device (sr) — using {sg_path} (sg) for raw access");
                return Ok((sg_path, Some(warning)));
            }
        }
        return Ok((
            path.to_string(),
            Some(format!(
                "{path} is a block device (sr) — no matching sg device found"
            )),
        ));
    }
    if !std::path::Path::new(path).exists() {
        return Err(Error::DeviceNotFound {
            path: path.to_string(),
        });
    }
    Ok((path.to_string(), None))
}
