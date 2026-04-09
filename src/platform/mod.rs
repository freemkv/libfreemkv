//! Platform-specific drive initialization and speed management.
//!
//! The Platform trait is minimal by design. Callers use init() once,
//! then set_read_speed() during reads. Internal operations cannot be
//! called directly — this prevents out-of-sequence operations.

pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;

/// Platform trait — locked-down interface.
///
/// Only three operations exposed:
///   init()           — one-time initialization
///   set_read_speed() — per-zone speed during reads
///   is_ready()       — state check
pub(crate) trait Platform {
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;
    fn set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()>;
    fn is_ready(&self) -> bool;
}
