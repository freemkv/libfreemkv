//! Platform-specific implementations of raw disc access commands.
//!
//! Each chipset family (MT1959, Pioneer) implements the Platform trait.
//! The trait methods correspond to the 10 command handlers in the per-drive
//!
//! x86 dispatch order (proven from code + hardware traces):
//!   1. unlock()        — try activate raw mode
//!   2. load_firmware() — if unlock fails, write ld_microcode then retry
//!   3. calibrate()     — probe disc zones, build speed table, triple SET_CD_SPEED
//!   5. read_register() — read hardware registers A and B (mid-rip, retried)
//!   6. status()        — query feature flags
//!   7. probe()         — parameterized register read
//!   8. set_read_speed()— per-zone SET_CD_SPEED during content reads
//!
//! The full init sequence is:
//!   unlock → [load_firmware if fail] × 6 → calibrate × 6 →
//!   drive_info → registers × 5 → status × 6 → probe

pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;

pub trait Platform {
    ///
    /// Sends READ_BUFFER with drive-specific mode/buf_id.
    /// Checks response against drive_signature and mode_active_magic ("MMkv").
    /// Returns Ok if mode already active (warm), Err if firmware needed (cold).
    fn unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    ///
    /// Sends WRITE_BUFFER mode=6 with ld_microcode (1888 bytes).
    /// Verifies with READ_BUFFER buf=0x45 (expects response == 2).
    /// Then calls unlock() twice to activate mode.
    /// Only called when unlock() fails (cold boot, firmware not in drive RAM).
    fn load_firmware(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    ///
    /// Sends pre-built hardware_register_a_cdb. Returns 16 bytes from
    /// the 36-byte response at offset [4:20].
    fn read_register_a(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 16]>;

    ///
    /// Sends pre-built hardware_register_b_cdb. Returns 16 bytes from
    /// the 36-byte response at offset [4:20].
    fn read_register_b(&mut self, scsi: &mut dyn ScsiTransport) -> Result<[u8; 16]>;

    ///
    /// Probes disc zones via READ_BUFFER sub_cmd=0x14, builds speed table,
    /// then commits with triple SET_CD_SPEED (max → nominal → max).
    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    fn keepalive(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    ///
    /// Returns 16 bytes of feature data via READ_BUFFER sub_cmd=0x13.
    fn status(&mut self, scsi: &mut dyn ScsiTransport) -> Result<DriveStatus>;

    fn probe(&mut self, scsi: &mut dyn ScsiTransport, sub_cmd: u8, address: u32, length: u32) -> Result<Vec<u8>>;

    ///
    /// Looks up LBA in speed_zone_table, sends SET_CD_SPEED.
    /// Called by x86 before each zone change during content reads.
    fn set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()>;

    fn timing(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Full init sequence — matches x86 dispatch order.
    ///
    /// unlock → [load_firmware if fail] × 6 → calibrate × 6
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Check if raw disc access mode is currently active.
    fn is_unlocked(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct DriveStatus {
    pub unlocked: bool,
    pub features: [u8; 16],
}
