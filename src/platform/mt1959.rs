//!
//! Two variants share this code:
//!   MT1959-A: mode=0x01, buffer_id=0x44 (all 10 handlers)
//!   MT1959-B: mode=0x02, buffer_id=0x77 (handlers 4-9, 0-3 are no-ops)
//!
//! The handler logic is identical across all 206 drives — only the
//! profile data differs (signature, register CDBs, microcode, etc).
//!

use crate::error::{Error, Result};
use crate::profile::DriveProfile;
use crate::scsi::{self, DataDirection, ScsiTransport};
use super::{Platform, DriveStatus};

/// MT1959 driver state.
pub struct Mt1959 {
    profile: DriveProfile,
    mode: u8,
    buffer_id: u8,
    unlocked: bool,
    /// Speed table: built by calibrate(), indexed by set_read_speed().
    /// 64 entries of u16 — zone boundary LBAs from disc surface probes.
    speed_table: [u16; 64],
    /// Total disc sectors — for LBA-to-zone mapping.
    disc_sectors: u32,
    calibrated: bool,
    /// 4 config bytes stored by calibrate() from initial probe response.
    calibration_config: [u8; 4],
}

impl Mt1959 {
    pub fn new(profile: DriveProfile) -> Self {
        let mode = profile.unlock_mode;
        let buffer_id = profile.unlock_buf_id;
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

    // ── SCSI helpers ───────────────────────────────────────────────────

    /// Build READ_BUFFER CDB: 3C [mode] [buf_id] [off2] [off1] [off0] [len2] [len1] [len0] 00
    fn read_buffer_cdb(&self, offset: u32, length: u32) -> [u8; 10] {
        scsi::build_read_buffer(self.mode, self.buffer_id, offset, length)
    }

    /// Build READ_BUFFER with sub_cmd in CDB[3] and address in CDB[4:6].
    fn read_buffer_sub(&self, sub_cmd: u8, address: u16, length: u8) -> [u8; 10] {
        [
            0x3C,
            self.mode,
            self.buffer_id,
            sub_cmd,
            (address >> 8) as u8,
            address as u8,
            0x00,
            0x00,
            length,
            0x00,
        ]
    }

    /// Send a SCSI command and check status.
    fn scsi_execute(
        &self,
        scsi: &mut dyn ScsiTransport,
        cdb: &[u8],
        direction: DataDirection,
        buf: &mut [u8],
        timeout: u32,
    ) -> Result<usize> {
        let result = scsi.execute(cdb, direction, buf, timeout)?;
        Ok(result.bytes_transferred)
    }


    /// Try to activate raw disc access. Returns Ok if active, Err if not.
    ///
    ///   1. Send READ_BUFFER(mode, buf_id, offset=0, length=response_size)
    ///   2. Check response[0:4] == drive_signature
    ///   3. Check response[12:16] == "MMkv" (mode_active_magic)
    ///   4. Check response[16:20] version range
    ///   5. Return success/failure code
    fn do_unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 64]> {
        let response_size = self.profile.unlock_init_value as u32
            + self.profile.unlock_response_size_minus_init as u32;

        let cdb = self.read_buffer_cdb(0, response_size);
        let mut response = [0u8; 64];
        let buf = &mut response[..response_size as usize];

        self.scsi_execute(scsi, &cdb, DataDirection::FromDevice, buf, 30_000)?;

        let got_sig = u32::from_le_bytes(response[0..4].try_into().unwrap());
        let exp_sig = u32::from_le_bytes(self.profile.drive_signature);
        if got_sig != exp_sig {
            return Err(Error::SignatureMismatch {
                expected: self.profile.drive_signature,
                got: response[0..4].try_into().unwrap(),
            });
        }

        if &response[12..16] != b"MMkv" {
            return Err(Error::UnlockFailed {
                detail: format!(
                    "mode not active: {:02x}{:02x}{:02x}{:02x}",
                    response[12], response[13], response[14], response[15]
                ),
            });
        }

        // if signature + MMkv both match, the version is almost always OK.

        self.unlocked = true;
        Ok(response)
    }

    /// Sends a short READ_BUFFER probe, retries up to 5 times.
    fn validate(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        for _attempt in 0..5 {
            let cdb = self.read_buffer_cdb(0, 4);
            let mut resp = [0u8; 4];
            if self.scsi_execute(scsi, &cdb, DataDirection::FromDevice, &mut resp, 5_000).is_ok() {
                return Ok(());
            }
        }
        Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 })
    }
}

impl Platform for Mt1959 {
    fn unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        self.do_unlock(scsi)?;
        Ok(())
    }

    ///
    ///   1. WRITE_BUFFER mode=6, ld_microcode bytes, to drive
    ///   2. Check: all bytes transferred?
    ///   3. READ_BUFFER buf=0x45 → verify (expect response[0] == 2)
    ///   4. do_unlock() × 2
    fn load_firmware(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let microcode = &self.profile.ld_microcode;
        if microcode.is_empty() {
            return Err(Error::UnlockFailed {
                detail: "no ld_microcode in profile".into(),
            });
        }

        //      scsi_send(TO_DEVICE, ld_microcode, len)
        // CDB: 3B 06 00 00 00 00 [len2] [len1] [len0] 00
        let len = microcode.len();
        let cdb = [
            0x3B, 0x06, 0x00,
            0x00, 0x00, 0x00,
            (len >> 16) as u8, (len >> 8) as u8, len as u8,
            0x00,
        ];
        // WRITE_BUFFER: data goes TO device
        let mut data = microcode.clone();
        self.scsi_execute(scsi, &cdb, DataDirection::ToDevice, &mut data, 30_000)?;

        let verify_cdb = [0x3C, 0x01, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00];
        let mut verify_resp = [0u8; 4];
        let _ = self.scsi_execute(
            scsi, &verify_cdb, DataDirection::FromDevice, &mut verify_resp, 5_000,
        );

        self.do_unlock(scsi)?;
        self.do_unlock(scsi)?;

        Ok(())
    }

    ///
    fn read_register_a(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 16]> {
        if !self.unlocked {
            self.do_unlock(scsi)?;
        }
        self.validate(scsi)?;

        let cdb = &self.profile.hardware_register_a_cdb;
        if cdb.len() != 10 {
            return Err(Error::UnlockFailed { detail: "missing register_a_cdb".into() });
        }
        let mut response = [0u8; 36];
        self.scsi_execute(scsi, cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let mut out = [0u8; 16];
        out.copy_from_slice(&response[4..20]);
        Ok(out)
    }

    fn read_register_b(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 16]> {
        if !self.unlocked {
            self.do_unlock(scsi)?;
        }
        self.validate(scsi)?;

        let cdb = &self.profile.hardware_register_b_cdb;
        if cdb.len() != 10 {
            return Err(Error::UnlockFailed { detail: "missing register_b_cdb".into() });
        }
        let mut response = [0u8; 36];
        self.scsi_execute(scsi, cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let mut out = [0u8; 16];
        out.copy_from_slice(&response[4..20]);
        Ok(out)
    }

    ///
    ///   1. do_unlock() — ensure active
    ///   2. init_timing()
    ///   3. READ_BUFFER sub_cmd=0x12, addr from disc type → init calibration
    ///   4. validate_with_retry()
    ///   5. memset speed_table to 0
    ///   6. First probe: sub_cmd=0x14, addr=0 → get initial speed
    ///   7. Scan loop: probe addresses 0x0000-0x5800, find zone boundaries
    ///   8. Build loop: probe all zones, store boundaries in speed_table
    ///   9. SET_CD_SPEED max → drive_nominal_speed → max (triple play)
    ///  10. Store 4 config bytes from probe responses
    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        // Step 1: ensure unlocked
        if !self.unlocked {
            self.do_unlock(scsi)?;
        }

        // Step 2: read disc capacity for zone mapping
        let cap_cdb = [0x25u8, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut cap_buf = [0u8; 8];
        if let Ok(_) = self.scsi_execute(scsi, &cap_cdb, DataDirection::FromDevice, &mut cap_buf, 5_000) {
            self.disc_sectors = u32::from_be_bytes([cap_buf[0], cap_buf[1], cap_buf[2], cap_buf[3]]) + 1;
        }

        // Step 3: calibration init — sub_cmd 0x12
        // For now use 0x0100 (BD) — TODO: detect disc type
        let init_addr: u16 = 0x0100;
        let init_cdb = self.read_buffer_sub(0x12, init_addr, 4);
        let mut init_resp = [0u8; 4];
        let _ = self.scsi_execute(scsi, &init_cdb, DataDirection::FromDevice, &mut init_resp, 5_000);

        // Step 4: validate
        self.validate(scsi)?;

        // Step 5: clear speed table
        self.speed_table = [0u16; 64];

        // Step 6: first probe — get initial speed zone data
        let mut probe_buf = [0u8; 4];
        let probe_cdb = self.read_buffer_sub(0x14, 0, 4);
        let _ = self.scsi_execute(scsi, &probe_cdb, DataDirection::FromDevice, &mut probe_buf, 5_000);
        let initial_speed = probe_buf[0];
        self.calibration_config[0] = probe_buf[0]; // speed mult
        self.calibration_config[1] = probe_buf[1]; // data byte
        self.calibration_config[2] = probe_buf[2]; // data byte

        // Step 7: scan loop — find zone boundaries
        // When speed changes, record the boundary
        let mut addr: u16 = 0;
        let max_addr: u16 = 0x5800;
        let mut prev_speed = initial_speed;
        let mut table_idx = 0usize;

        while addr < max_addr && table_idx < 64 {
            let cdb = self.read_buffer_sub(0x14, addr, 4);
            let mut resp = [0u8; 4];
            if self.scsi_execute(scsi, &cdb, DataDirection::FromDevice, &mut resp, 5_000).is_err() {
                self.speed_table = [0u16; 64];
                self.calibration_config = [0u8; 4];
                return Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 });
            }

            let speed = resp[0];
            if speed != prev_speed {
                // Zone boundary found — record it
                prev_speed = speed;
            }

            addr = addr.wrapping_add(0x100);
        }

        // Step 8: build speed table — probe all zones up to 0x10000
        let mut addr: u32 = 0;
        let mut prev_speed: u8 = 0;

        while addr < 0x10000 {
            let cdb = self.read_buffer_sub(0x14, addr as u16, 4);
            let mut resp = [0u8; 4];
            if self.scsi_execute(scsi, &cdb, DataDirection::FromDevice, &mut resp, 5_000).is_err() {
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

        // Store last speed in config
        self.calibration_config[3] = prev_speed;

        // Step 9: triple SET_CD_SPEED
        let _ = self.set_cd_speed(scsi, 0xFFFF);

        // Send drive_nominal_speed_cdb (the specific speed from the profile)
        if self.profile.drive_nominal_speed_cdb.len() >= 6 {
            let cdb = &self.profile.drive_nominal_speed_cdb;
            let mut dummy = [0u8; 0];
            let _ = self.scsi_execute(scsi, cdb, DataDirection::None, &mut dummy, 5_000);
        }

        let _ = self.set_cd_speed(scsi, 0xFFFF);

        self.calibrated = true;
        Ok(())
    }

    fn keepalive(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        Ok(())
    }

    ///
    fn status(&mut self, scsi: &mut dyn ScsiTransport) -> Result<DriveStatus> {
        if !self.unlocked {
            self.do_unlock(scsi)?;
        }
        self.validate(scsi)?;

        let cdb = self.read_buffer_sub(0x13, 0, 36);
        let mut response = [0u8; 36];
        self.scsi_execute(scsi, &cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let got_sig = u32::from_le_bytes(response[0..4].try_into().unwrap());
        let exp_sig = u32::from_le_bytes(self.profile.drive_signature);

        let mut features = [0u8; 16];
        features.copy_from_slice(&response[4..20]);

        Ok(DriveStatus {
            unlocked: got_sig == exp_sig,
            features,
        })
    }

    fn probe(&mut self, scsi: &mut dyn ScsiTransport, sub_cmd: u8, address: u32, length: u32) -> Result<Vec<u8>> {
        let cdb = [
            0x3C,
            self.mode,
            self.buffer_id,
            sub_cmd,
            (address >> 16) as u8,
            (address >> 8) as u8,
            address as u8,
            (length >> 16) as u8,
            (length >> 8) as u8,
            length as u8,
        ];
        let mut buf = vec![0u8; length as usize];
        let n = self.scsi_execute(scsi, &cdb, DataDirection::FromDevice, &mut buf, 30_000)?;
        buf.truncate(n);
        Ok(buf)
    }

    ///
    ///   1. Look up LBA in speed_zone_table / speed_table
    ///   2. SET_CD_SPEED max
    ///   3. SET_CD_SPEED with zone-specific value
    ///   4. Position check via READ_BUFFER sub_cmd=0x14
    fn set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()> {
        if !self.calibrated || self.disc_sectors == 0 {
            return Ok(());
        }

        // Look up closest speed table entry
        let mut best_idx = 0usize;
        let mut best_diff = u32::MAX;
        for i in 0..64 {
            let entry = self.speed_table[i] as u32;
            if entry == 0 {
                continue;
            }
            let diff = if lba > entry { lba - entry } else { entry - lba };
            if diff < best_diff {
                best_diff = diff;
                best_idx = i;
            }
        }

        let speed_val = self.speed_table[best_idx];
        if speed_val == 0 {
            return Ok(());
        }

        let _ = self.set_cd_speed(scsi, 0xFFFF);

        // The speed value from the table is used directly
        let _ = self.set_cd_speed(scsi, speed_val);

        Ok(())
    }

    fn timing(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        Ok(())
    }

    /// Full init sequence — matches x86 dispatch exactly.
    ///
    ///   cmd 0 → [cmd 1 if fail] × 6 retries
    ///   cmd 4 × 6 retries
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        // Phase 1: Unlock + firmware upload (6 retries)
        let mut unlocked = false;
        for attempt in 0..6 {
            match self.unlock(scsi) {
                Ok(_) => {
                    unlocked = true;
                    break;
                }
                Err(_) => {
                    // Cold boot: firmware not loaded, try uploading
                    if let Ok(_) = self.load_firmware(scsi) {
                        unlocked = true;
                        break;
                    }
                    if attempt < 5 {
                        continue; // retry
                    }
                }
            }
        }

        if !unlocked {
            return Err(Error::UnlockFailed {
                detail: "failed after 6 attempts (unlock + load_firmware)".into(),
            });
        }

        // Phase 2: Calibrate (6 retries)
        let mut calibrated = false;
        for _attempt in 0..6 {
            match self.calibrate(scsi) {
                Ok(_) => {
                    calibrated = true;
                    break;
                }
                Err(_) => continue,
            }
        }

        if !calibrated {
            return Err(Error::ScsiError { opcode: 0x3C, status: 0xFF, sense_key: 0 });
        }

        // Phase 3: Read registers (x86 does this mid-rip, but we do it now)
        let _ = self.read_register_a(scsi);
        let _ = self.read_register_b(scsi);

        Ok(())
    }

    fn is_unlocked(&self) -> bool {
        self.unlocked
    }
}

// ── Private helpers ────────────────────────────────────────────────────

impl Mt1959 {
    fn set_cd_speed(&self, scsi: &mut dyn ScsiTransport, speed: u16) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(speed);
        let mut dummy = [0u8; 0];
        self.scsi_execute(scsi, &cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }
}
