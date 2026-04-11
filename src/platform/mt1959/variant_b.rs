//! MT1959 variant B firmware upload.
//!
//! MODE SELECT (0x55) → read metadata → WRITE_BUFFER → vendor verify (0xF1) → unlock × 5+1

use super::Mt1959;
use crate::error::Result;
use crate::scsi::{DataDirection, ScsiTransport};

const SCSI_MODE_SELECT: u8 = 0x55;
const SCSI_WRITE_BUFFER: u8 = 0x3B;
const SCSI_READ_BUFFER: u8 = 0x3C;
const FIRMWARE_MAX_SIZE: usize = 0x9C0;
const FIRMWARE_EXTRA: [u8; 16] = [0; 16];
const VENDOR_VERIFY: [u8; 10] = [0xF1, 0x01, 0x02, 0x00, 0x0D, 0x30, 0x01, 0xF3, 0xAD, 0x23];

pub(super) fn load_firmware(mt: &mut Mt1959, scsi: &mut dyn ScsiTransport) -> Result<()> {
    let firmware = &mt.profile.firmware;
    if firmware.is_empty() {
        return Err(crate::error::Error::UnlockFailed);
    }

    // Step 1: Upload firmware via MODE SELECT
    let write_len = FIRMWARE_MAX_SIZE.min(firmware.len());
    let mode_select_cdb = [
        SCSI_MODE_SELECT,
        0x10,
        0x00,
        0x00,
        0x00,
        0x00,
        (write_len >> 16) as u8,
        (write_len >> 8) as u8,
        write_len as u8,
        0x00,
    ];
    let mut data = firmware[..write_len].to_vec();
    scsi.execute(&mode_select_cdb, DataDirection::ToDevice, &mut data, 30_000)?;

    // Step 2: Read firmware metadata (READ_BUFFER mode 6, offset 0x3000)
    let read_meta_cdb = [
        SCSI_READ_BUFFER,
        0x06,
        0x00,
        0x00,
        0x30,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
    ];
    let mut meta_resp = [0u8; 16];
    let _ = scsi.execute(
        &read_meta_cdb,
        DataDirection::FromDevice,
        &mut meta_resp,
        5_000,
    );

    // Step 3: Write extra firmware data (all zeros)
    let write_extra_cdb = [
        SCSI_WRITE_BUFFER,
        0x06,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
    ];
    let mut data2 = FIRMWARE_EXTRA.to_vec();
    let _ = scsi.execute(&write_extra_cdb, DataDirection::ToDevice, &mut data2, 5_000);

    // Step 4: Vendor verify (0xF1 — B-only, not standard SCSI)
    let mut dummy = [0u8; 0];
    let _ = scsi.execute(&VENDOR_VERIFY, DataDirection::None, &mut dummy, 5_000);

    // Step 5: Unlock retries (up to 5, then final attempt)
    for _attempt in 0..5 {
        if mt.do_unlock(scsi).is_ok() {
            let _ = mt.do_unlock(scsi);
            return Ok(());
        }
    }
    mt.do_unlock(scsi)?;
    Ok(())
}
