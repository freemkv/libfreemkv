//! Platform-specific drive initialization and disc probing.

pub mod fs_type;
pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;

pub(crate) trait PlatformDriver: Send {
    /// Unlock drive + upload firmware if needed.
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Calibrate drive for this disc. Probes disc surface so the drive's
    /// firmware learns the optimal speed for each region. After probing
    /// the drive manages per-zone speeds internally — the host just reads
    /// at max speed.
    fn probe_disc(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// True after successful init().
    fn is_ready(&self) -> bool;

    /// True if the drive is currently in the extended-access state —
    /// per-drive runtime firmware uploaded AND the unlock response's
    /// marker bytes confirm the mode is live. When true:
    ///   - host can issue the per-drive OEM CDBs in
    ///     [`crate::profile::DriveProfile`]
    ///   - VID retrieval works via the OEM CDB path (no cert-based
    ///     mutual auth required)
    ///   - SCSI READ_10 returns plaintext sectors (no bus encryption)
    ///
    /// Default `false` — platforms without this mode always report
    /// inactive.
    fn is_unlocked(&self) -> bool {
        false
    }
}
