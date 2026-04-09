//! MT1959 platform — unlock, firmware upload, calibration, speed management.

use crate::error::{Error, Result};
use crate::profile::DriveProfile;
use crate::scsi::{self, DataDirection, ScsiTransport};
use super::PlatformDriver;

const UNLOCK_RESPONSE_SIZE: u8 = 64;

// Variant constants
const MODE_A: u8 = 0x01;
const MODE_B: u8 = 0x02;
const BUFFER_ID_A: u8 = 0x44;
const BUFFER_ID_B: u8 = 0x77;
const NOMINAL_SPEED_A: [u8; 12] = [0xBB, 0x00, 0x23, 0x28, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
const NOMINAL_SPEED_B: [u8; 12] = [0x00, 0x00, 0xBB, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
const FIRMWARE_EXTRA_B: [u8; 16] = [0; 16];
const VERIFY_COMMAND_B: [u8; 10] = [0xF1, 0x01, 0x02, 0x00, 0x0D, 0x30, 0x01, 0xF3, 0xAD, 0x23];

pub struct Mt1959 {
    profile: DriveProfile,
    mode: u8,
    buffer_id: u8,
    unlocked: bool,
    speed_table: [u16; 64],
    disc_sectors: u32,
    calibrated: bool,
    calibration_config: [u8; 4],
}

impl Mt1959 {
    pub fn new(profile: DriveProfile, is_variant_b: bool) -> Self {
        let (mode, buffer_id) = if is_variant_b {
            (MODE_B, BUFFER_ID_B)
        } else {
            (MODE_A, BUFFER_ID_A)
        };
        Mt1959 {
            profile,
            mode,
            buffer_id,
            unlocked: false,
            speed_table: [0u16; 64],
            disc_sectors: 0,
            calibrated: false,
            calibration_config: [0u8; 4],
        }
    }

    fn read_buffer_sub(&self, sub_cmd: u8, address: u16, length: u8) -> [u8; 10] {
        [
            0x3C, self.mode, self.buffer_id, sub_cmd,
            (address >> 8) as u8, address as u8,
            0x00, 0x00, length, 0x00,
        ]
    }

    fn read_buffer_probe(
        &self, scsi: &mut dyn ScsiTransport,
        sub_cmd: u8, address: u16, buf: &mut [u8], expected: usize,
    ) -> Result<usize> {
        let cdb = self.read_buffer_sub(sub_cmd, address, expected as u8);
        let result = scsi.execute(&cdb, DataDirection::FromDevice, buf, 5_000)?;
        if result.bytes_transferred != expected {
            return Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 });
        }
        Ok(result.bytes_transferred)
    }

    fn set_cd_speed_max(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(0xFFFF);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }

    fn set_cd_speed(&self, scsi: &mut dyn ScsiTransport, speed: u16) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(speed);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }

    fn do_unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
        let cdb = [
            0x3C, self.mode, self.buffer_id,
            0x00, 0x00, 0x00,
            0x00, 0x00, UNLOCK_RESPONSE_SIZE, 0x00,
        ];
        let mut response = vec![0u8; UNLOCK_RESPONSE_SIZE as usize];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        if response.len() >= 4 && response[0..4] != self.profile.signature {
            return Err(Error::SignatureMismatch {
                expected: self.profile.signature,
                got: response[0..4].try_into().unwrap_or([0; 4]),
            });
        }

        if response.len() >= 16 && &response[12..16] != b"MMkv" {
            return Err(Error::UnlockFailed {
                detail: format!(
                    "mode not active: {:02x}{:02x}{:02x}{:02x}",
                    response[12], response[13], response[14], response[15]
                ),
            });
        }

        self.unlocked = true;
        Ok(response)
    }

    fn validate(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        for _attempt in 0..5 {
            let cdb = [
                0x3C, self.mode, self.buffer_id,
                0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00,
            ];
            let mut resp = [0u8; 4];
            if scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000).is_ok() {
                return Ok(());
            }
        }
        Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 })
    }
}

impl PlatformDriver for Mt1959 {
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if self.unlocked && self.calibrated {
            return Ok(());
        }
        self.run_init(scsi)
    }

    fn set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()> {
        if !self.calibrated {
            return Ok(());
        }
        self.run_set_read_speed(scsi, lba)
    }

    fn is_ready(&self) -> bool {
        self.unlocked && self.calibrated
    }
}

impl Mt1959 {
    fn unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        self.do_unlock(scsi)?;
        Ok(())
    }

    fn load_firmware(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if self.profile.firmware.is_empty() {
            return Err(Error::UnlockFailed {
                detail: "no firmware in profile".into(),
            });
        }

        if self.mode == MODE_A {
            self.load_firmware_a(scsi)
        } else {
            self.load_firmware_b(scsi)
        }
    }

    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if !self.unlocked { self.do_unlock(scsi)?; }

        let cap_cdb = [0x25u8, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut cap_buf = [0u8; 8];
        if scsi.execute(&cap_cdb, DataDirection::FromDevice, &mut cap_buf, 5_000).is_ok() {
            self.disc_sectors = u32::from_be_bytes([cap_buf[0], cap_buf[1], cap_buf[2], cap_buf[3]]) + 1;
        }

        let init_addr: u16 = 0x0100; // TODO: detect disc type (0x0200 for UHD)
        let mut init_resp = [0u8; 4];
        let _ = self.read_buffer_probe(scsi, 0x12, init_addr, &mut init_resp, 4);

        self.validate(scsi)?;
        self.speed_table = [0u16; 64];

        let mut probe_buf = [0u8; 4];
        let _ = self.read_buffer_probe(scsi, 0x14, 0, &mut probe_buf, 4);
        let initial_speed = probe_buf[0];
        self.calibration_config[0] = probe_buf[0];
        self.calibration_config[1] = probe_buf[1];
        self.calibration_config[2] = probe_buf[2];

        let mut addr: u16 = 0;
        let mut prev_speed = initial_speed;
        while addr < 0x5800 {
            let mut resp = [0u8; 4];
            if self.read_buffer_probe(scsi, 0x14, addr, &mut resp, 4).is_err() {
                self.speed_table = [0u16; 64];
                self.calibration_config = [0u8; 4];
                return Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 });
            }
            if resp[0] != prev_speed {
                prev_speed = resp[0];
            }
            addr = addr.wrapping_add(0x100);
        }

        let mut addr: u32 = 0;
        let mut prev_speed: u8 = 0;
        while addr < 0x10000 {
            let mut resp = [0u8; 4];
            if self.read_buffer_probe(scsi, 0x14, addr as u16, &mut resp, 4).is_err() {
                break;
            }
            let speed = resp[0];
            if speed > prev_speed && speed > 0 {
                let idx = ((speed as usize) >> 1).saturating_sub(1);
                if idx < 64 && self.speed_table[idx] == 0 {
                    self.speed_table[idx] = addr as u16;
                }
            }
            prev_speed = speed;
            addr += 0x100;
        }
        self.calibration_config[3] = prev_speed;

        let _ = self.set_cd_speed_max(scsi);
        let nominal = if self.mode == MODE_A { &NOMINAL_SPEED_A } else { &NOMINAL_SPEED_B };
        let mut dummy = [0u8; 0];
        let _ = scsi.execute(nominal, DataDirection::None, &mut dummy, 5_000);
        let _ = self.set_cd_speed_max(scsi);

        self.calibrated = true;
        Ok(())
    }

    fn run_set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()> {
        if !self.calibrated {
            return Ok(());
        }

        let mut best_idx: usize = 0;
        let mut best_diff: u32 = 0x10000000;
        let mut found = false;

        for i in 0..64 {
            let entry = self.speed_table[i] as u32;
            if entry == 0 { continue; }
            let diff = if lba > entry { lba - entry } else { entry - lba };
            if diff < best_diff {
                best_diff = diff;
                best_idx = i;
                found = true;
            }
        }

        if !found {
            return Ok(());
        }

        let speed_val = self.speed_table[best_idx];
        let probe_addr = 0x0100 | (speed_val.swap_bytes() as u16);
        let mut probe_resp = [0u8; 4];
        let _ = self.read_buffer_probe(scsi, 0x14, probe_addr, &mut probe_resp, 4);

        let _ = self.set_cd_speed_max(scsi);
        let _ = self.set_cd_speed(scsi, speed_val);

        Ok(())
    }

    fn run_init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let mut unlocked = false;
        for _attempt in 0..6 {
            match self.unlock(scsi) {
                Ok(_) => { unlocked = true; break; }
                Err(Error::SignatureMismatch { .. }) => {
                    return Err(Error::UnlockFailed {
                        detail: "signature mismatch — wrong profile for this drive".into(),
                    });
                }
                Err(_) => {
                    if self.load_firmware(scsi).is_ok() {
                        unlocked = true;
                        break;
                    }
                }
            }
        }
        if !unlocked {
            return Err(Error::UnlockFailed {
                detail: "failed after 6 attempts".into(),
            });
        }

        let mut calibrated = false;
        for _attempt in 0..6 {
            if self.calibrate(scsi).is_ok() {
                calibrated = true;
                break;
            }
        }
        if !calibrated {
            return Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 });
        }

        Ok(())
    }
}

impl Mt1959 {
    fn load_firmware_a(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let firmware = &self.profile.firmware;
        let len = firmware.len();

        let cdb = [
            0x3B, 0x06, 0x00,
            0x00, 0x00, 0x00,
            (len >> 16) as u8, (len >> 8) as u8, len as u8,
            0x00,
        ];
        let mut data = firmware.clone();
        scsi.execute(&cdb, DataDirection::ToDevice, &mut data, 30_000)?;

        let verify_cdb = [0x3C, 0x01, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00];
        let mut verify_resp = [0u8; 4];
        let _ = scsi.execute(&verify_cdb, DataDirection::FromDevice, &mut verify_resp, 5_000);

        self.do_unlock(scsi)?;
        self.do_unlock(scsi)?;
        Ok(())
    }

    fn load_firmware_b(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let firmware = &self.profile.firmware;

        let write_len = 0x9C0usize.min(firmware.len());
        let mode_select_cdb = [
            0x55, 0x10, 0x00,
            0x00, 0x00, 0x00,
            (write_len >> 16) as u8, (write_len >> 8) as u8, write_len as u8,
            0x00,
        ];
        let mut data = firmware[..write_len].to_vec();
        scsi.execute(&mode_select_cdb, DataDirection::ToDevice, &mut data, 30_000)?;

        let read_meta_cdb = [0x3C, 0x06, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x10, 0x00];
        let mut meta_resp = [0u8; 16];
        let _ = scsi.execute(&read_meta_cdb, DataDirection::FromDevice, &mut meta_resp, 5_000);

        let write2_cdb = [0x3B, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00];
        let mut data2 = FIRMWARE_EXTRA_B.to_vec();
        let _ = scsi.execute(&write2_cdb, DataDirection::ToDevice, &mut data2, 5_000);

        let mut dummy = [0u8; 0];
        let _ = scsi.execute(&VERIFY_COMMAND_B, DataDirection::None, &mut dummy, 5_000);

        for _attempt in 0..5 {
            if self.do_unlock(scsi).is_ok() {
                let _ = self.do_unlock(scsi);
                return Ok(());
            }
        }
        self.do_unlock(scsi)?;
        Ok(())
    }
}
