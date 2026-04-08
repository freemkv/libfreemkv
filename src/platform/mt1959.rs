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

/// BD 1x speed in KB/s — used to convert speed multipliers to SET CD SPEED values.
const BD_1X_SPEED: u16 = 4500;

/// MT1959 driver state.
pub struct Mt1959 {
    profile: DriveProfile,
    mode: u8,
    buffer_id: u8,
    unlocked: bool,
    /// Speed table: maps disc zone (probe address >> 8) to speed in KB/s.
    /// Populated by calibrate(). Entry 0 = address 0x0000, entry 255 = address 0xFF00.
    speed_table: [u16; 256],
    /// Total disc sectors — for mapping LBA to zone index in speed table.
    disc_sectors: u32,
    calibrated: bool,
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
            speed_table: [0u16; 256],
            disc_sectors: 0,
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
            return Err(Error::UnlockFailed { detail: format!(
                "verify mismatch at [12:16]: {:02x}{:02x}{:02x}{:02x}",
                response[12], response[13], response[14], response[15]
            ) });
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
            opcode: 0x3C,
            status: 0xFF,
            sense_key: 0,
        })
    }

    /// Look up optimal read speed for a given LBA from the calibration table.
    /// Returns speed in KB/s for SET CD SPEED, or 0 if not calibrated.
    fn lookup_speed(&self, lba: u32, disc_sectors: u32) -> u16 {
        if !self.calibrated || disc_sectors == 0 {
            return 0;
        }
        // Map LBA to zone index (0-255). Probe address space is 0x0000-0xFF00.
        let zone = ((lba as u64 * 256) / disc_sectors as u64).min(255) as usize;
        self.speed_table[zone]
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
            .ok_or_else(|| Error::ProfileNotFound {
                vendor_id: self.profile.vendor_id.clone(),
                product_revision: self.profile.product_revision.clone(),
                vendor_specific: format!("register index {} out of range", index),
            })?;

        let cdb = scsi::build_read_buffer(self.mode, self.buffer_id, offset, 36);
        let mut response = [0u8; 36];
        scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        let mut out = [0u8; 16];
        out.copy_from_slice(&response[4..20]);
        Ok(out)
    }

    ///
    /// Probes the disc surface to build a speed profile. Each zone gets
    /// an optimal speed in KB/s. The drive firmware returns a speed
    /// multiplier (resp[0]) for each probe address.
    ///
    /// Probe address 0x0000-0xFF00 maps linearly to the disc's LBA range.
    /// resp[0] = speed multiplier (e.g. 6 = 6x BD, 12 = 12x BD).
    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        self.ensure_unlocked(scsi)?;
        self.validate(scsi)?;

        // Read disc capacity for LBA-to-zone mapping
        let cap_cdb = [0x25u8, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut cap_buf = [0u8; 8];
        if let Ok(_) = scsi.execute(&cap_cdb, DataDirection::FromDevice, &mut cap_buf, 5_000) {
            self.disc_sectors = u32::from_be_bytes([cap_buf[0], cap_buf[1], cap_buf[2], cap_buf[3]]) + 1;
        }

        // Step 1: Calibration init — sub_cmd 0x12 with address 0x0200
        // (primes the firmware for disc surface analysis)
        let cdb = self.read_buffer_sub(0x12, 0x0200, 4);
        let mut resp = [0u8; 4];
        let _ = scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000);

        // Step 2: Raw read primers — read a few sectors with 0x08 flag
        // to force the drive to spin up and measure disc characteristics.
        // Without these, the speed probes return stale data.
        let mut primer_buf = [0u8; 2048];
        let _ = scsi.execute(
            &scsi::build_read10_raw(0, 1), DataDirection::FromDevice, &mut primer_buf, 30_000);
        let _ = scsi.execute(
            &scsi::build_read10_raw(0x200, 1), DataDirection::FromDevice, &mut primer_buf, 30_000);
        let _ = scsi.execute(
            &scsi::build_read10_raw(0, 1), DataDirection::FromDevice, &mut primer_buf, 30_000);

        // Step 3: Speed probes — sub_cmd 0x14, addresses 0x00 through 0xFF
        self.speed_table = [0u16; 256];

        for zone in 0..256u16 {
            let cdb = self.read_buffer_sub(0x14, zone, 4);
            let mut resp = [0u8; 4];
            match scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000) {
                Ok(r) if r.bytes_transferred >= 1 && resp[0] > 0 => {
                    self.speed_table[zone as usize] = resp[0] as u16 * BD_1X_SPEED;
                }
                _ => {
                    self.speed_table[zone as usize] = 0xFFFF;
                }
            }
        }

        // Step 4: Set max speed
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

        // No per-read speed changes — calibrate + SET CD SPEED at open_title handles it.
        // READ(10) with raw flag 0x08
        let cdb = scsi::build_read10_raw(lba, count);
        let result = scsi.execute(&cdb, DataDirection::FromDevice, buf, 30_000)?;
        Ok(result.bytes_transferred)
    }

    fn timing(&mut self, _scsi: &mut dyn ScsiTransport) -> Result<()> {
        Ok(())
    }

    /// Continuous speed management — probes zone, reads registers, sets speed.
    ///
    /// MediaTek drives decay to 1x BD speed (~5 MB/s instead of 15-20 MB/s).
    ///
    /// Sequence (from strace analysis):
    ///   1. Speed probe (sub_cmd 0x14) for current LBA zone
    ///   2. Read register A (sub_cmd 0x10 at profile offset A)
    ///   3. Read register B (sub_cmd 0x11 at profile offset B)
    ///   4. SET CD SPEED: max → zone_speed → max
    fn maintain_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()> {
        if !self.unlocked || self.disc_sectors == 0 {
            return Ok(());
        }

        // 1. Probe current zone
        let zone = ((lba as u64 * 256) / self.disc_sectors as u64).min(255) as u16;
        let cdb = self.read_buffer_sub(0x14, zone, 4);
        let mut resp = [0u8; 4];
        let _ = scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000);
        let zone_speed = if resp[0] > 0 {
            resp[0] as u16 * BD_1X_SPEED
        } else {
            0xFFFF
        };

        // 2. Read drive registers (handlers 2 & 3)
        if self.profile.register_offsets.len() >= 2 {
            for i in 0..2 {
                let offset = self.profile.register_offsets[i];
                let sub_cmd = 0x10 + i as u8;
                let cdb = [
                    0x3C,
                    self.mode,
                    self.buffer_id,
                    sub_cmd,
                    (offset >> 16) as u8,
                    (offset >> 8) as u8,
                    offset as u8,
                    0x00,
                    0x24, // 36 bytes
                    0x00,
                ];
                let mut buf = [0u8; 36];
                let _ = scsi.execute(&cdb, DataDirection::FromDevice, &mut buf, 5_000);
            }
        }

        // 3. Triple SET CD SPEED: max → zone → max
        let _ = self.set_cd_speed(scsi, 0xFFFF);
        if zone_speed < 0xFFFF {
            let _ = self.set_cd_speed(scsi, zone_speed);
        }
        let _ = self.set_cd_speed(scsi, 0xFFFF);

        Ok(())
    }

    fn is_unlocked(&self) -> bool {
        self.unlocked
    }
}
