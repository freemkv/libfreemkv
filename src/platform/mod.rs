//! Platform-specific drive initialization and speed management.
//!
//! The Platform trait is minimal by design. Callers cannot access
//! individual handlers (unlock, firmware upload, calibrate) directly.
//! This prevents out-of-sequence operations that could damage drives.
//!
//! Pipeline: DriveSession::open() calls init() once. After that,
//! only set_read_speed() is available during reads.

pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;

/// Platform trait — locked-down interface.
///
/// Only three operations exposed:
///   init()           — called once by DriveSession::open()
///   set_read_speed() — called per zone during reads
///   is_ready()       — state check
///
/// All internal handlers (unlock, firmware, calibrate, registers)
/// are private to the implementation. Cannot be called externally.
pub(crate) trait Platform {
    /// One-time initialization. Called by DriveSession::open() only.
    /// Internally: unlock → [firmware if needed] → calibrate → registers.
    /// Safe to call on any drive state (warm, cold, OEM).
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Set read speed for a disc zone. Called during content reads.
    fn set_read_speed(&mut self, scsi: &mut dyn ScsiTransport, lba: u32) -> Result<()>;

    /// True after successful init().
    fn is_ready(&self) -> bool;
}
