//! MT1959 platform implementation — covers all LG/ASUS MediaTek drives.
//!
//! Two variants share this code:
//!   MT1959-A: mode=0x01, buffer_id=0x44 (handlers 0-9)
//!   MT1959-B: mode=0x02, buffer_id=0x77 (handlers 4-9, 0-3 are no-ops)
//!
//! The logic is identical between A and B — only the SCSI READ BUFFER
//! mode and buffer ID differ. Per-drive data (signature, register offsets)
//! comes from the profile.

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
    speed_table: [u16; 64],
    calibrated: bool,
}

impl Mt1959 {
    pub fn new(profile: DriveProfile) -> Self {
        let mode = profile.platform.mode();
        let buffer_id = profile.platform.buffer_id();
        Mt1959 {
            profile,
            mode,
            buffer_id,
            unlocked: false,
            speed_table: [0u16; 64],
            calibrated: false,
        }
    }

    /// Build a READ BUFFER CDB for this platform's mode and buffer ID.
    fn read_buffer_cdb(&self, offset: u32, length: u32) -> [u8; 10] {
        scsi::build_read_buffer(self.mode, self.buffer_id, offset, length)
    }

    /// Build a READ BUFFER CDB with a sub-command byte in CDB[3].
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

    ///
    /// 1. Send READ BUFFER(mode, buffer_id, offset=0, length=64)
    /// 2. Check response[0:4] matches the profile signature
    /// 3. Check response[12:16] matches the verification bytes (0x4D4D6B76)
    fn do_unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 64]> {
        let cdb = self.read_buffer_cdb(0, 64);
        let mut response = [0u8; 64];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        // Check signature at response[0:4]
        let got_sig: [u8; 4] = response[0..4].try_into().unwrap();
        if got_sig != self.profile.signature {
            return Err(Error::SignatureMismatch {
                expected: self.profile.signature,
                got: got_sig,
            });
        }

        // Check verification bytes at response[12:16]
        if &response[12..16] != self.profile.verify.as_slice() {
            return Err(Error::UnlockFailed(format!(
                "verify mismatch at [12:16]: {:02x}{:02x}{:02x}{:02x}",
                response[12], response[13], response[14], response[15]
            )));
        }

        self.unlocked = true;
        Ok(response)
    }

    /// Ensure raw disc access is active, re-enabling if needed.
    fn ensure_unlocked(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if !self.unlocked {
            self.do_unlock(scsi)?;
        }
        Ok(())
    }

    /// Pre-operation validation with retry.
    ///
    /// Sends a short READ BUFFER probe, retries up to 5 times to confirm
    /// the drive is still responding to commands.
    fn validate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        for _attempt in 0..5 {
            let cdb = self.read_buffer_cdb(0, 4);
            let mut resp = [0u8; 4];
            match scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000) {
                Ok(_) => return Ok(()),
                Err(_) => continue,
            }
        }
        Err(Error::ScsiError {
            cdb: vec![0x3C],
            status: 0xFF,
            sense: vec![],
        })
    }

    /// Look up optimal read speed for a given LBA from the calibration table.
    fn lookup_speed(&self, lba: u32) -> u16 {
        if !self.calibrated {
            return 0;
        }
        let mut best_speed = 0u16;
        let mut best_diff = u32::MAX;
        for &entry in &self.speed_table {
            if entry == 0 {
                continue;
            }
            let entry_lba = entry as u32;
            let diff = if lba > entry_lba { lba - entry_lba } else { entry_lba - lba };
            if diff < best_diff {
                best_diff = diff;
                best_speed = entry;
            }
        }
        best_speed
    }

    /// Send SET CD SPEED command.
    fn set_cd_speed(&self, scsi: &mut dyn ScsiTransport, speed: u16) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(speed);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }
}

impl Platform for Mt1959 {
    fn unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        self.do_unlock(scsi)?;
        Ok(())
    }

    ///
    /// Primary read: 0x760 (1888) bytes of configuration data.
    /// Secondary read: 4-byte status appended to the result.
    fn read_config(&mut self, scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
        // Primary config read: 0x760 = 1888 bytes
        let cdb = self.read_buffer_cdb(0, 0x760);
        let mut buf = vec![0u8; 0x760];
        let result = scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 30_000)?;
        buf.truncate(result.bytes_transferred);

        // Secondary: 4-byte status read
        let cdb2 = self.read_buffer_cdb(0, 4);
        let mut status = [0u8; 4];
        scsi.execute(&cdb2, DataDirection::FromDevice, &mut status, 5_000)?;

        buf.extend_from_slice(&status);
        Ok(buf)
    }

    /// Handlers 2-3: Read hardware register at the profile-specified offset.
    ///
    /// Reads 36 bytes via READ BUFFER and extracts bytes [4:20] as the
    /// 16-byte register value.
    fn read_register(&mut self, scsi: &mut dyn ScsiTransport, index: u8) -> Result<[u8; 16]> {
        self.ensure_unlocked(scsi)?;
        self.validate(scsi)?;

        let offset = *self.profile.register_offsets.get(index as usize)
            .ok_or_else(|| Error::ScsiError {
                cdb: vec![],
                status: 0,
                sense: vec![],
            })?;

        let cdb = scsi::build_read_buffer(self.mode, self.buffer_id, offset, 36);
        let mut response = [0u8; 36];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let mut out = [0u8; 16];
        out.copy_from_slice(&response[4..20]);
        Ok(out)
    }

    ///
    /// Scans disc surface addresses via READ BUFFER sub-command 0x14 to
    /// build a 64-entry speed lookup table. Issues SET CD SPEED(max) when done.
    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        self.ensure_unlocked(scsi)?;
        self.validate(scsi)?;

        // Initial probe: READ BUFFER sub_cmd=0x12
        let cdb = self.read_buffer_sub(0x12, 0, 4);
        let mut resp = [0u8; 4];
        let _ = scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000);

        self.validate(scsi)?;

        // Clear speed table
        self.speed_table = [0u16; 64];

        // Scan disc surface — probe addresses up to 0x10000, 256 at a time
        let mut table_idx = 0usize;
        let mut addr: u32 = 0;
        while addr < 0x10000 && table_idx < 64 {
            let cdb = self.read_buffer_sub(0x14, addr as u16, 4);
            let mut resp = [0u8; 4];
            match scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000) {
                Ok(r) if r.bytes_transferred == 4 => {
                    let val = resp[0];
                    if val > 0 {
                        let speed_entry = ((resp[0] as u16) << 8) | (resp[1] as u16);
                        if speed_entry > 0 {
                            self.speed_table[table_idx] = speed_entry;
                            table_idx += 1;
                        }
                    }
                    addr += 256;
                }
                _ => {
                    addr += 256;
                }
            }
        }

        // Set max speed after calibration
        self.set_cd_speed(scsi, 0xFFFF)?;

        self.calibrated = true;
        Ok(())
    }

    fn keepalive(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        Ok(())
    }

    ///
    /// Sends READ BUFFER with sub-command 0x13, reads 36 bytes.
    /// Checks signature at [0:4], returns feature data from [4:20].
    fn status(&mut self, scsi: &mut dyn ScsiTransport) -> Result<DriveStatus> {
        self.ensure_unlocked(scsi)?;
        self.validate(scsi)?;

        // READ BUFFER with sub_cmd=0x13, 36 bytes response
        let cdb = self.read_buffer_sub(0x13, 0, 36);
        let mut response = [0u8; 36];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        // Verify response signature
        let got_sig = u32::from_be_bytes(response[0..4].try_into().unwrap());
        let expected_sig = u32::from_le_bytes(self.profile.signature);

        let mut features = [0u8; 16];
        features.copy_from_slice(&response[4..20]);

        Ok(DriveStatus {
            unlocked: got_sig == expected_sig,
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
        let result = scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 30_000)?;
        buf.truncate(result.bytes_transferred);
        Ok(buf)
    }

    ///
    /// Looks up the LBA in the speed table, issues SET CD SPEED if calibrated,
    /// then performs READ(10) with the raw read flag (0x08).
    fn read_sectors(
        &mut self,
        scsi: &mut dyn ScsiTransport,
        lba: u32,
        count: u16,
        buf: &mut [u8],
    ) -> Result<usize> {
        if !self.unlocked {
            return Err(Error::NotUnlocked);
        }

        // Speed optimization from calibration
        if self.calibrated {
            let speed = self.lookup_speed(lba);
            if speed > 0 {
                let _ = self.set_cd_speed(scsi, speed);
            }
        }

        // READ(10) with raw flag 0x08
        let cdb = scsi::build_read10_raw(lba, count);
        let result = scsi.execute(&cdb, DataDirection::FromDevice, buf, 30_000)?;
        Ok(result.bytes_transferred)
    }

    fn timing(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        Ok(())
    }

    fn is_unlocked(&self) -> bool {
        self.unlocked
    }
}
