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
    /// Speed table: 64 × u16, built by calibrate().
    /// Stores zone boundary addresses from disc surface probes.
    speed_table: [u16; 64],
    /// Total disc sectors — from READ CAPACITY.
    disc_sectors: u32,
    calibrated: bool,
    /// 4 config bytes stored by calibrate() from initial probe response.
    /// [0]=speed_mult, [1]=data_byte, [2]=data_byte, [3]=last_speed
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


    /// Build READ_BUFFER with sub_cmd in CDB[3] and address in CDB[4:6].
    fn read_buffer_sub(&self, sub_cmd: u8, address: u16, length: u8) -> [u8; 10] {
        [
            0x3C, self.mode, self.buffer_id, sub_cmd,
            (address >> 8) as u8, address as u8,
            0x00, 0x00, length, 0x00,
        ]
    }

    /// Calls dynamic_read_buffer, validates response size matches expected.
    /// Returns Ok(bytes_transferred) or Err.
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

    /// Sends SET_CD_SPEED from CDB template at 0x9A78: BB 00 FF FF FF FF...
    fn set_cd_speed_max(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(0xFFFF);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }

    /// Build and send a custom SET_CD_SPEED with a specific speed value.
    fn set_cd_speed(&self, scsi: &mut dyn ScsiTransport, speed: u16) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(speed);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }


    /// Core unlock function. Returns the full response on success.
    ///
    ///   r3 = unlock_init_value (1)
    ///   sp[0x1C] = r3
    ///   r3 += unlock_response_size_minus_init (0x3F)
    ///   sp[0] = r3 (= 64 = response size)
    ///   scsi_cmd_wrapper(result, 0x0A, unlock_CDB, response)
    ///   check response[0:4] == drive_signature (LE u32)
    ///   check response[12:16] == "MMkv" (0x766B4D4D LE)
    ///   check response[16:20] version range
    fn do_unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
        let response_size = self.profile.unlock_init_value as u32
            + self.profile.unlock_response_size_minus_init as u32;

        let cdb = [
            0x3C, self.mode, self.buffer_id,
            0x00, 0x00, 0x00,
            0x00, 0x00, response_size as u8, 0x00,
        ];
        let mut response = vec![0u8; response_size as usize];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        if response.len() >= 4 {
            let got = &response[0..4];
            if got != self.profile.drive_signature {
                return Err(Error::SignatureMismatch {
                    expected: self.profile.drive_signature,
                    got: got.try_into().unwrap_or([0; 4]),
                });
            }
        }

        if response.len() >= 16 && &response[12..16] != b"MMkv" {
            return Err(Error::UnlockFailed {
                detail: format!(
                    "mode not active: {:02x}{:02x}{:02x}{:02x}",
                    response[12], response[13], response[14], response[15]
                ),
            });
        }

        // If signature + MMkv match, version is almost always OK.

        self.unlocked = true;
        Ok(response)
    }

    /// Sends a short READ_BUFFER probe, retries up to 5 times.
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

impl Platform for Mt1959 {
    /// Thin wrapper → do_unlock(). Returns Ok if mode active, Err if not.
    fn unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        self.do_unlock(scsi)?;
        Ok(())
    }

    ///
    /// Two variants with different upload sequences:
    ///
    ///   1. WRITE_BUFFER mode=6 with ld_microcode
    ///   2. READ_BUFFER buf=0x45 verify (expect response == 2)
    ///   3. do_unlock() × 2
    ///
    ///   1. WRITE_BUFFER with mode=2 buf=0x77 initial handshake (0x9C0 bytes)
    ///   2. Check response == 2
    ///   3. READ_BUFFER at offset 0x3000 (16 bytes, firmware metadata check)
    ///   4. WRITE_BUFFER mode=6 with ld_microcode (16 bytes from payload+16)
    ///   5. READ verify
    ///   6. do_unlock() × 5 retries
    fn load_firmware(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let microcode = &self.profile.ld_microcode;
        if microcode.is_empty() {
            return Err(Error::UnlockFailed {
                detail: "no ld_microcode in profile".into(),
            });
        }

        if self.mode == 0x01 {
            // ── MT1959-A path ──────────────────────────────────────────
            self.load_firmware_a(scsi)?;
        } else {
            // ── MT1959-B path ──────────────────────────────────────────
            self.load_firmware_b(scsi)?;
        }

        Ok(())
    }

    ///
    /// do_unlock → validate × 5 → send hardware_register_a_cdb (10B) →
    /// check 36B response → return response[4:20] (16 bytes)
    fn read_register_a(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 16]> {
        if !self.unlocked { self.do_unlock(scsi)?; }
        self.validate(scsi)?;

        let cdb = &self.profile.hardware_register_a_cdb;
        if cdb.len() != 10 {
            return Err(Error::UnlockFailed { detail: "missing register_a_cdb".into() });
        }
        let mut response = [0u8; 36];
        scsi.execute(cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let mut out = [0u8; 16];
        out.copy_from_slice(&response[4..20]);
        Ok(out)
    }

    fn read_register_b(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 16]> {
        if !self.unlocked { self.do_unlock(scsi)?; }
        self.validate(scsi)?;

        let cdb = &self.profile.hardware_register_b_cdb;
        if cdb.len() != 10 {
            return Err(Error::UnlockFailed { detail: "missing register_b_cdb".into() });
        }
        let mut response = [0u8; 36];
        scsi.execute(cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let mut out = [0u8; 16];
        out.copy_from_slice(&response[4..20]);
        Ok(out)
    }

    ///
    /// Full calibration sequence:
    ///   1. do_unlock()
    ///   2. init_timing()
    ///   3. read_buffer_probe(0x12, init_addr, 4) — calibration init
    ///   4. validate_with_retry()
    ///   5. memset speed_table to 0
    ///   6. First probe: sub_cmd=0x14, addr=0 → initial speed
    ///   7. Scan loop: addresses 0x0000-0x5800, step 0x100, detect zone boundaries
    ///   8. Build loop: scan all zones up to 0x10000, store in speed_table[(speed>>1)-1]
    ///   9. Triple SET_CD_SPEED: max → drive_nominal_speed → max
    ///  10. Store 4 calibration config bytes
    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        // Step 1: ensure unlocked
        if !self.unlocked { self.do_unlock(scsi)?; }

        // Step 2: read disc capacity
        let cap_cdb = [0x25u8, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut cap_buf = [0u8; 8];
        if scsi.execute(&cap_cdb, DataDirection::FromDevice, &mut cap_buf, 5_000).is_ok() {
            self.disc_sectors = u32::from_be_bytes([cap_buf[0], cap_buf[1], cap_buf[2], cap_buf[3]]) + 1;
        }

        // Step 3: calibration init — sub_cmd 0x12
        let init_addr: u16 = 0x0100; // TODO: detect disc type
        let mut init_resp = [0u8; 4];
        let _ = self.read_buffer_probe(scsi, 0x12, init_addr, &mut init_resp, 4);

        // Step 4: validate
        self.validate(scsi)?;

        // Step 5: clear speed table
        self.speed_table = [0u16; 64];

        // Step 6: first probe
        let mut probe_buf = [0u8; 4];
        let _ = self.read_buffer_probe(scsi, 0x14, 0, &mut probe_buf, 4);
        let initial_speed = probe_buf[0];
        self.calibration_config[0] = probe_buf[0];
        self.calibration_config[1] = probe_buf[1];
        self.calibration_config[2] = probe_buf[2];

        // Step 7: scan loop — find zone boundaries (0x0000-0x5800, step 0x100)
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

        // Step 8: build speed table — probe all zones up to 0x10000
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

        // Step 9: triple SET_CD_SPEED — max → nominal → max
        let _ = self.set_cd_speed_max(scsi);

        // Send drive_nominal_speed_cdb (the specific speed from the profile)
        if self.profile.drive_nominal_speed_cdb.len() >= 6 {
            let cdb = &self.profile.drive_nominal_speed_cdb;
            let mut dummy = [0u8; 0];
            let _ = scsi.execute(cdb, DataDirection::None, &mut dummy, 5_000);
        }

        let _ = self.set_cd_speed_max(scsi);

        self.calibrated = true;
        Ok(())
    }

    ///
    /// NOT a no-op. Sends 16 bytes from a data buffer to the host via host_write.
    /// In our context: the x86 reads 16 bytes back. We return the data.
    /// For now we just acknowledge — the x86 calls this to confirm VM is alive.
    fn keepalive(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        // In driver context: no host_write mechanism. This is a VM-to-host
        // communication that doesn't translate to a SCSI command.
        Ok(())
    }

    ///
    /// Full status check:
    ///   1. Read 4 bytes from host (param input)
    ///   2. Validate param == 4
    ///   3. do_unlock()
    ///   4. validate_with_retry()
    ///   6. read_buffer_probe(0x13, addr, response, 36)
    ///   7. validate_with_retry() again
    ///   8. Check REV32(response[0:4]) == 0x00220054
    ///   9. If mismatch: call fallback() with speed_zone_table
    ///  10. If match: host_write(response[4:20]) — 16 bytes features
    fn status(&mut self, scsi: &mut dyn ScsiTransport) -> Result<DriveStatus> {
        if !self.unlocked { self.do_unlock(scsi)?; }
        self.validate(scsi)?;

        let cdb = self.read_buffer_sub(0x13, 0, 36);
        let mut response = [0u8; 36];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        self.validate(scsi)?;

        let sig = u32::from_be_bytes(response[0..4].try_into().unwrap());
        let expected = 0x00220054;

        let mut features = [0u8; 16];
        features.copy_from_slice(&response[4..20]);

        Ok(DriveStatus {
            unlocked: sig == expected,
            features,
        })
    }

    ///
    /// Three code paths based on param count:
    ///   param=1 (5 bytes in): sub_cmd + nothing else → dynamic_read_buffer
    ///   param=5 (5+4 bytes in): sub_cmd + address → dynamic_read_buffer with addr
    ///   param=9 (9+ bytes in): builds full 12-byte CDB on stack:
    ///     [0x3C, mode, buf_id, sub_cmd, addr[2], addr[1], addr[0], len[2], len[1], len[0]]
    ///     then calls scsi_cmd_wrapper directly
    fn probe(
        &mut self, scsi: &mut dyn ScsiTransport,
        sub_cmd: u8, address: u32, length: u32,
    ) -> Result<Vec<u8>> {
        let cdb = [
            0x3C, self.mode, self.buffer_id, sub_cmd,
            (address >> 16) as u8, (address >> 8) as u8, address as u8,
            (length >> 16) as u8, (length >> 8) as u8, length as u8,
        ];
        let mut buf = vec![0u8; length as usize];
        let result = scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 30_000)?;
        buf.truncate(result.bytes_transferred);
        Ok(buf)
    }

    ///
    /// Speed management for content reads. Called per zone change.
    ///
    ///   1. Read 4-byte target LBA from host
    ///   2. REV(LBA) for big-endian comparison
    ///   3. Search speed_table[64] for closest entry to LBA
    ///      - Each entry is u16 zone boundary address
    ///      - Find entry with smallest |entry - LBA| distance
    ///   4. If no match found → call fallback() with speed_calc_table
    ///   5. If match:
    ///      a. r6 = speed_table[best_idx] (the zone address = speed value)
    ///      b. Position check: read_buffer_probe(0x14, 0x100 | rev16(r6), 4)
    ///      c. SET_CD_SPEED max (BB 00 FF FF FF FF)
    ///      d. Build custom SET_CD_SPEED: BB 00 [r6>>8] [r6&FF] FF FF 00...
    ///      e. Send custom SET_CD_SPEED
    ///      f. Return next speed_table entry to host
    fn set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()> {
        if !self.calibrated {
            return Ok(());
        }

        // the byte-swapped LBA against table entries. Since entries are
        // zone boundary addresses (u16), we compare the low 16 bits.

        // Step 2-3: search speed_table for closest entry
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
            // For now, skip speed adjustment when no table match
            return Ok(());
        }

        // Step 5a: get the matched speed value
        let speed_val = self.speed_table[best_idx];

        // Step 5b: position check probe
        let probe_addr = 0x0100 | (speed_val.swap_bytes() as u16);
        let mut probe_resp = [0u8; 4];
        let _ = self.read_buffer_probe(scsi, 0x14, probe_addr, &mut probe_resp, 4);

        // Step 5c: SET_CD_SPEED max
        let _ = self.set_cd_speed_max(scsi);

        // Step 5d-e: custom SET_CD_SPEED with the matched speed value
        let _ = self.set_cd_speed(scsi, speed_val);

        Ok(())
    }

    ///
    /// Reads 8 bytes from host, byte-swaps as 32-bit value, returns.
    /// Has a helper for unaligned 4-byte reads with manual byte copy.
    /// In driver context: no host communication, so this is a no-op.
    fn timing(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        // VM-to-host timing measurement, not a SCSI command.
        Ok(())
    }

    /// Full init sequence — matches x86 dispatch exactly.
    ///
    ///   Phase 1: cmd 0 → [cmd 1 if fail] × 6 retries (unlock + fw upload)
    ///   Phase 2: cmd 4 × 6 retries (calibrate)
    ///   Phase 3: cmd 7 → [cmd 5 fallback] (drive info)
    ///   Phase 4: cmd 2 + cmd 3 × 5 retries (registers)
    ///   Phase 5: cmd 9 × 6 retries (status)
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        // Phase 1: Unlock + firmware upload (6 retries)
        let mut unlocked = false;
        for _attempt in 0..6 {
            match self.unlock(scsi) {
                Ok(_) => { unlocked = true; break; }
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
                detail: "failed after 6 attempts (unlock + load_firmware)".into(),
            });
        }

        // Phase 2: Calibrate (6 retries)
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

        // If fails: cmd 5 fallback (keepalive)
        // In our context: this fetches a display string, not critical for reads
        let _ = self.probe(scsi, 0x00, 0, 0x3FF);

        for _attempt in 0..5 {
            let a_ok = self.read_register_a(scsi).is_ok();
            let b_ok = self.read_register_b(scsi).is_ok();
            if a_ok && b_ok { break; }
        }

        // Phase 5: Status — non-fatal, single attempt
        // Some drives reject sub_cmd 0x13. Not required for reads.
        let _ = self.status(scsi);

        Ok(())
    }

    fn is_unlocked(&self) -> bool {
        self.unlocked
    }
}

// ── Private firmware upload variants ───────────────────────────────────

impl Mt1959 {
    ///
    /// Simple: WRITE all microcode → verify buf=0x45 → unlock × 2.
    fn load_firmware_a(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let microcode = &self.profile.ld_microcode;
        let len = microcode.len();

        // WRITE_BUFFER mode=6: send entire microcode payload
        let cdb = [
            0x3B, 0x06, 0x00,
            0x00, 0x00, 0x00,
            (len >> 16) as u8, (len >> 8) as u8, len as u8,
            0x00,
        ];
        let mut data = microcode.clone();
        scsi.execute(&cdb, DataDirection::ToDevice, &mut data, 30_000)?;

        let verify_cdb = [0x3C, 0x01, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00];
        let mut verify_resp = [0u8; 4];
        let _ = scsi.execute(
            &verify_cdb, DataDirection::FromDevice, &mut verify_resp, 5_000,
        );

        // Unlock × 2
        self.do_unlock(scsi)?;
        self.do_unlock(scsi)?;
        Ok(())
    }

    ///
    ///   1. MODE SELECT (0x55) — send 2496 bytes from ld_microcode
    ///   2. Check result == 2 (firmware accepted)
    ///   3. READ_BUFFER mode=6 offset=0x3000 (16 bytes firmware metadata)
    ///   4. WRITE_BUFFER mode=6 (16 bytes from profile's fw_write_data)
    ///   5. Vendor verify command (from profile's verify_cdb)
    ///   6. do_unlock() × 5 retries + 1 final
    fn load_firmware_b(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let microcode = &self.profile.ld_microcode;

        // Step 1: MODE SELECT (0x55) with vendor mode page data
        // Transfer: 0x9C0 = 2496 bytes from the firmware payload
        let write_len = 0x9C0usize.min(microcode.len());
        let mode_select_cdb = [
            0x55, 0x10, 0x00,
            0x00, 0x00, 0x00,
            (write_len >> 16) as u8, (write_len >> 8) as u8, write_len as u8,
            0x00,
        ];
        let mut data = microcode[..write_len].to_vec();
        scsi.execute(&mode_select_cdb, DataDirection::ToDevice, &mut data, 30_000)?;

        // The execute above will Err on SCSI failure. If we get here, command accepted.

        // Step 3: READ_BUFFER mode=6, offset=0x3000, 16 bytes
        let read_meta_cdb = [0x3C, 0x06, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x10, 0x00];
        let mut meta_resp = [0u8; 16];
        let _ = scsi.execute(&read_meta_cdb, DataDirection::FromDevice, &mut meta_resp, 5_000);

        // Step 4: WRITE_BUFFER mode=6, 16 bytes
        // This data is stored in profile.fw_write_data (16 bytes)
        if self.profile.fw_write_data.len() >= 16 {
            let write2_cdb = [0x3B, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00];
            let mut data2 = self.profile.fw_write_data[..16].to_vec();
            let _ = scsi.execute(&write2_cdb, DataDirection::ToDevice, &mut data2, 5_000);
        }

        // Step 5: Vendor verify command
        if self.profile.verify_cdb.len() >= 10 {
            let mut dummy = [0u8; 0];
            let _ = scsi.execute(&self.profile.verify_cdb, DataDirection::None, &mut dummy, 5_000);
        }

        // Step 6: do_unlock() × 5 retries + 1 confirmation
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
