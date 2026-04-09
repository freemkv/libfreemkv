//! Platform-specific drive initialization and disc probing.

pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;

pub(crate) trait PlatformDriver {
    /// Unlock drive + upload firmware if needed.
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Calibrate drive for this disc. Probes disc surface so the drive's
    /// firmware learns the optimal speed for each region. After probing
    /// the drive manages per-zone speeds internally — the host just reads
    /// at max speed.
    fn probe_disc(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// True after successful init().
    fn is_ready(&self) -> bool;
}
