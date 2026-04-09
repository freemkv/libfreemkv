//! MT1959 variant A firmware upload.

use crate::error::Result;
use crate::scsi::{DataDirection, ScsiTransport};
use super::Mt1959;

pub(super) fn load_firmware(mt: &mut Mt1959, scsi: &mut dyn ScsiTransport) -> Result<()> {
    let firmware = &mt.profile.firmware;
    if firmware.is_empty() {
        return Err(crate::error::Error::UnlockFailed {
            detail: "no firmware in profile".into(),
        });
    }

    let len = firmware.len();
    let cdb = [
        0x3B, 0x06, 0x00,
        0x00, 0x00, 0x00,
        (len >> 16) as u8, (len >> 8) as u8, len as u8,
        0x00,
    ];
    let mut data = firmware.clone();
    scsi.execute(&cdb, DataDirection::ToDevice, &mut data, 30_000)?;

    // Verify (may fail, non-fatal)
    let verify_cdb = [0x3C, 0x01, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00];
    let mut verify_resp = [0u8; 4];
    let _ = scsi.execute(&verify_cdb, DataDirection::FromDevice, &mut verify_resp, 5_000);

    mt.do_unlock(scsi)?;
    mt.do_unlock(scsi)?;
    Ok(())
}
