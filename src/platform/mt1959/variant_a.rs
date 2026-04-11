//! MT1959 variant A firmware upload.
//!
//! WRITE_BUFFER (0x3B) → verify READ_BUFFER (0x45) → unlock × 2

use super::Mt1959;
use crate::error::Result;
use crate::scsi::{DataDirection, ScsiTransport};

const SCSI_WRITE_BUFFER: u8 = 0x3B;
const VERIFY_BUFFER_ID: u8 = 0x45;

pub(super) fn load_firmware(mt: &mut Mt1959, scsi: &mut dyn ScsiTransport) -> Result<()> {
    let firmware = &mt.profile.firmware;
    if firmware.is_empty() {
        return Err(crate::error::Error::UnlockFailed);
    }

    // Upload firmware via WRITE_BUFFER
    let len = firmware.len();
    let cdb = [
        SCSI_WRITE_BUFFER,
        0x06,
        0x00,
        0x00,
        0x00,
        0x00,
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        0x00,
    ];
    let mut data = firmware.clone();
    scsi.execute(&cdb, DataDirection::ToDevice, &mut data, 30_000)?;

    // Verify firmware loaded (non-fatal — different buffer_id 0x45)
    let verify_cdb = [
        super::SCSI_READ_BUFFER,
        super::MODE_A,
        VERIFY_BUFFER_ID,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        super::VALIDATE_RESPONSE_SIZE,
        0x00,
    ];
    let mut verify_resp = [0u8; super::VALIDATE_RESPONSE_SIZE as usize];
    let _ = scsi.execute(
        &verify_cdb,
        DataDirection::FromDevice,
        &mut verify_resp,
        5_000,
    );

    // Double unlock after firmware upload
    mt.do_unlock(scsi)?;
    mt.do_unlock(scsi)?;
    Ok(())
}
