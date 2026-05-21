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

    /// True if the drive is currently in libredrive raw-read mode (the
    /// per-drive runtime firmware has been uploaded AND the drive
    /// confirms active mode via the `MMkv` / `LbDr` markers in the
    /// unlock response). When true the host can read sectors without
    /// AACS bus encryption and retrieve VID without cert-based mutual
    /// auth — the cert/HRL gate on the drive's standard AACS path is
    /// effectively bypassed by the alternate data path.
    ///
    /// Default `false` — platforms that don't implement this mode are
    /// always reported as inactive.
    fn is_libredrive_active(&self) -> bool {
        false
    }
}
