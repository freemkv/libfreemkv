//! MT1959 platform — shared logic for both variants.

mod variant_a;
mod variant_b;

use crate::error::{Error, Result};
use crate::profile::DriveProfile;
use crate::scsi::{self, DataDirection, ScsiTransport};
use super::PlatformDriver;

// ── Variant constants ──────────────────────────────────────────────────
// Every vendor command: 3C [mode] [buffer_id] [sub_cmd] [addr] ...
const MODE_A: u8 = 0x01;
const MODE_B: u8 = 0x02;
const BUFFER_ID_A: u8 = 0x44;
const BUFFER_ID_B: u8 = 0x77;

// ── SCSI opcodes ──────────────────────────────────────────────────────
const SCSI_READ_BUFFER: u8 = 0x3C;
const SCSI_READ_CAPACITY: u8 = 0x25;

// ── Sub-commands (shared A/B) ─────────────────────────────────────────
const SUB_CMD_UNLOCK: u8 = 0x00;
const SUB_CMD_INIT: u8 = 0x12;
const SUB_CMD_PROBE: u8 = 0x14;
const UNLOCK_RESPONSE_SIZE: u8 = 64;
const VALIDATE_RESPONSE_SIZE: u8 = 4;
const FIRMWARE_ACTIVE_OFFSET: usize = 12;
const FIRMWARE_ACTIVE_SIG: [u8; 4] = [0x4D, 0x4D, 0x6B, 0x76];

// ── Init address (per disc type) ──────────────────────────────────────
const INIT_ADDR_BD: u16 = 0x0100;
const INIT_ADDR_UHD: u16 = 0x0200;

// ── Probe scan ranges ─────────────────────────────────────────────────
const PROBE_COARSE_END: u16 = 0x5800;
const PROBE_FINE_END: u32 = 0x10000;
const PROBE_STEP: u16 = 0x0100;
const PROBE_RESPONSE_SIZE: u8 = 4;

// ── Disc type threshold ───────────────────────────────────────────────
const UHD_SECTOR_THRESHOLD: u32 = 25_000_000; // ~50 GB
const READ_CAPACITY_RESPONSE_SIZE: usize = 8;

pub struct Mt1959 {
    pub(crate) profile: DriveProfile,
    pub(crate) mode: u8,
    pub(crate) buffer_id: u8,
    pub(crate) unlocked: bool,
    probed: bool,
}

impl Mt1959 {
    pub fn new(profile: DriveProfile, is_variant_b: bool) -> Self {
        let (mode, buffer_id) = if is_variant_b {
            (MODE_B, BUFFER_ID_B)
        } else {
            (MODE_A, BUFFER_ID_A)
        };
        Mt1959 {
            profile, mode, buffer_id,
            unlocked: false,
            probed: false,
        }
    }

    // ── SCSI helpers (shared by both variants) ─────────────────────────

    pub(crate) fn read_buffer_sub(&self, sub_cmd: u8, address: u16, length: u8) -> [u8; 10] {
        [
            SCSI_READ_BUFFER, self.mode, self.buffer_id, sub_cmd,
            (address >> 8) as u8, address as u8,
            0x00, 0x00, length, 0x00,
        ]
    }

    pub(crate) fn read_buffer_probe(
        &self, scsi: &mut dyn ScsiTransport,
        sub_cmd: u8, address: u16, buf: &mut [u8], expected: usize,
    ) -> Result<usize> {
        let cdb = self.read_buffer_sub(sub_cmd, address, expected as u8);
        let result = scsi.execute(&cdb, DataDirection::FromDevice, buf, 5_000)?;
        if result.bytes_transferred != expected {
            return Err(Error::ScsiError { opcode: SCSI_READ_BUFFER, status: 0xFF, sense_key: 0 });
        }
        Ok(result.bytes_transferred)
    }

    pub(crate) fn set_cd_speed_max(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(0xFFFF);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }

    // ── Unlock (shared) ────────────────────────────────────────────────

    pub(crate) fn do_unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
        let cdb = [
            0x3C, self.mode, self.buffer_id,
            SUB_CMD_UNLOCK, 0x00, 0x00,
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

        if response.len() >= FIRMWARE_ACTIVE_OFFSET + 4
            && response[FIRMWARE_ACTIVE_OFFSET..FIRMWARE_ACTIVE_OFFSET + 4] != FIRMWARE_ACTIVE_SIG {
            return Err(Error::UnlockFailed);
        }

        self.unlocked = true;
        Ok(response)
    }

    fn validate(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        for _attempt in 0..5 {
            let cdb = [
                0x3C, self.mode, self.buffer_id,
                SUB_CMD_UNLOCK, 0x00, 0x00,
                0x00, 0x00, VALIDATE_RESPONSE_SIZE, 0x00,
            ];
            let mut resp = [0u8; 4];
            if scsi.execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000).is_ok() {
                return Ok(());
            }
        }
        Err(Error::ScsiError { opcode: SCSI_READ_BUFFER, status: 0xFF, sense_key: 0 })
    }

    // ── Init (unlock + firmware) ───────────────────────────────────────

    fn run_init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let mut unlocked = false;
        for _attempt in 0..6 {
            match self.do_unlock(scsi) {
                Ok(_) => { unlocked = true; break; }
                Err(Error::SignatureMismatch { .. }) => {
                    return Err(Error::UnlockFailed);
                }
                Err(_) => {
                    let ok = if self.mode == MODE_A {
                        variant_a::load_firmware(self, scsi).is_ok()
                    } else {
                        variant_b::load_firmware(self, scsi).is_ok()
                    };
                    if ok { unlocked = true; break; }
                }
            }
        }
        if !unlocked {
            return Err(Error::UnlockFailed);
        }
        Ok(())
    }

    // ── Probe disc ─────────────────────────────────────────────────────

    /// Probe the disc surface so the drive firmware learns optimal speeds
    /// per region. Two passes, then SET_CD_SPEED(max). After this the
    /// drive manages per-zone speeds internally.
    fn run_probe(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if !self.unlocked { self.do_unlock(scsi)?; }

        // Detect disc type from capacity to select probe mode.
        // BD:  3C 01 44 12 01 00 00 00 04 00  (init_addr = 0x0100)
        // UHD: 3C 01 44 12 02 00 00 00 04 00  (init_addr = 0x0200)
        // Verified from MakeMKV strace: BD and UHD use different init addresses.
        let cap_cdb = [SCSI_READ_CAPACITY, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut cap_buf = [0u8; READ_CAPACITY_RESPONSE_SIZE];
        let disc_sectors = if scsi.execute(&cap_cdb, DataDirection::FromDevice, &mut cap_buf, 5_000).is_ok() {
            u32::from_be_bytes([cap_buf[0], cap_buf[1], cap_buf[2], cap_buf[3]]) + 1
        } else {
            0
        };
        let init_addr = if disc_sectors > UHD_SECTOR_THRESHOLD { INIT_ADDR_UHD } else { INIT_ADDR_BD };
        let mut init_resp = [0u8; PROBE_RESPONSE_SIZE as usize];
        let _ = self.read_buffer_probe(scsi, SUB_CMD_INIT, init_addr, &mut init_resp, PROBE_RESPONSE_SIZE as usize);

        self.validate(scsi)?;

        // Pass 1: coarse scan
        let mut addr: u16 = 0;
        while addr < PROBE_COARSE_END {
            let mut resp = [0u8; PROBE_RESPONSE_SIZE as usize];
            if self.read_buffer_probe(scsi, SUB_CMD_PROBE, addr, &mut resp, PROBE_RESPONSE_SIZE as usize).is_err() {
                return Err(Error::ScsiError { opcode: SCSI_READ_BUFFER, status: 0xFF, sense_key: 0 });
            }
            addr = addr.wrapping_add(PROBE_STEP);
        }

        // Pass 2: fine scan
        let mut addr: u32 = 0;
        while addr < PROBE_FINE_END {
            let mut resp = [0u8; PROBE_RESPONSE_SIZE as usize];
            if self.read_buffer_probe(scsi, SUB_CMD_PROBE, addr as u16, &mut resp, PROBE_RESPONSE_SIZE as usize).is_err() {
                break;
            }
            addr += PROBE_STEP as u32;
        }

        // Set max speed — drive manages zones from here
        let _ = self.set_cd_speed_max(scsi);

        self.probed = true;
        Ok(())
    }
}

// ── PlatformDriver trait ───────────────────────────────────────────────

impl PlatformDriver for Mt1959 {
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if self.unlocked { return Ok(()); }
        self.run_init(scsi)
    }

    fn probe_disc(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if !self.unlocked { self.run_init(scsi)?; }
        if self.probed { return Ok(()); }
        self.run_probe(scsi)
    }

    fn is_ready(&self) -> bool {
        self.unlocked
    }
}
