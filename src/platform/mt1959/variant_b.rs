//! MT1959 variant B firmware upload.

use crate::error::Result;
use crate::scsi::{DataDirection, ScsiTransport};
use super::Mt1959;

const FIRMWARE_EXTRA: [u8; 16] = [0; 16];
const VERIFY_COMMAND: [u8; 10] = [0xF1, 0x01, 0x02, 0x00, 0x0D, 0x30, 0x01, 0xF3, 0xAD, 0x23];

pub(super) fn load_firmware(mt: &mut Mt1959, scsi: &mut dyn ScsiTransport) -> Result<()> {
    let firmware = &mt.profile.firmware;
    if firmware.is_empty() {
        return Err(crate::error::Error::UnlockFailed {
            detail: "no firmware in profile".into(),
        });
    }

    // Step 1: MODE SELECT with firmware payload
    let write_len = 0x9C0usize.min(firmware.len());
    let mode_select_cdb = [
        0x55, 0x10, 0x00,
        0x00, 0x00, 0x00,
        (write_len >> 16) as u8, (write_len >> 8) as u8, write_len as u8,
        0x00,
    ];
    let mut data = firmware[..write_len].to_vec();
    scsi.execute(&mode_select_cdb, DataDirection::ToDevice, &mut data, 30_000)?;

    // Step 2: Read firmware metadata
    let read_meta_cdb = [0x3C, 0x06, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x10, 0x00];
    let mut meta_resp = [0u8; 16];
    let _ = scsi.execute(&read_meta_cdb, DataDirection::FromDevice, &mut meta_resp, 5_000);

    // Step 3: Write extra firmware data
    let write2_cdb = [0x3B, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00];
    let mut data2 = FIRMWARE_EXTRA.to_vec();
    let _ = scsi.execute(&write2_cdb, DataDirection::ToDevice, &mut data2, 5_000);

    // Step 4: Vendor verify
    let mut dummy = [0u8; 0];
    let _ = scsi.execute(&VERIFY_COMMAND, DataDirection::None, &mut dummy, 5_000);

    // Step 5: Unlock retries
    for _attempt in 0..5 {
        if mt.do_unlock(scsi).is_ok() {
            let _ = mt.do_unlock(scsi);
            return Ok(());
        }
    }
    mt.do_unlock(scsi)?;
    Ok(())
}
