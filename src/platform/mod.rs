//! Platform-specific implementations of raw disc access commands.
//!
//! Each chipset family (MT1959, Pioneer) implements the Platform trait.
//! accessed via SCSI READ BUFFER with platform-specific mode and buffer ID.

pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;

/// Platform trait — raw disc access commands implemented per chipset.
///
/// Command handlers accessed via READ BUFFER:
pub trait Platform {
    ///
    /// Sends the platform-specific READ BUFFER CDB and verifies
    /// the response signature bytes.
    fn unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    ///
    /// Performs a primary READ BUFFER for the configuration data,
    /// followed by a secondary 4-byte status read.
    fn read_config(&mut self, scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>>;

    /// Handlers 2-3: Read hardware register.
    ///
    /// `index` selects which register offset from the profile to use.
    /// Returns 16 bytes of register data extracted from a 36-byte response.
    fn read_register(&mut self, scsi: &mut dyn ScsiTransport, index: u8) -> Result<[u8; 16]>;

    ///
    /// Probes the disc surface via READ BUFFER sub-commands to build
    /// a 64-entry speed lookup table for optimal read performance.
    /// Issues SET CD SPEED at maximum after calibration completes.
    fn calibrate(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    ///
    /// Periodic command to maintain the raw access session.
    fn keepalive(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    ///
    /// Verifies the response signature and returns 16 bytes of
    /// feature/status data.
    fn status(&mut self, scsi: &mut dyn ScsiTransport) -> Result<DriveStatus>;

    ///
    /// Sends a READ BUFFER command with dynamic sub-command, address,
    /// and length. Used for disc structure reads and feature queries.
    fn probe(&mut self, scsi: &mut dyn ScsiTransport, sub_cmd: u8, address: u32, length: u32) -> Result<Vec<u8>>;

    ///
    /// Looks up the LBA in the speed table, issues SET CD SPEED,
    /// then performs a READ(10) with the raw read flag (0x08).
    fn read_sectors(&mut self, scsi: &mut dyn ScsiTransport, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize>;

    fn timing(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Check if raw disc access mode is currently enabled.
    fn is_unlocked(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct DriveStatus {
    pub unlocked: bool,
    pub features: [u8; 16],
}
