//! Platform-specific drive initialization and calibration.

pub mod mt1959;

use crate::error::Result;
use crate::scsi::ScsiTransport;
use crate::speed::SpeedTable;

pub(crate) trait PlatformDriver {
    /// Unlock drive + upload firmware if needed.
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()>;

    /// Read speed zones from disc surface, fill speed table.
    fn read_speed_table(&mut self, scsi: &mut dyn ScsiTransport, speed_table: &mut SpeedTable) -> Result<()>;

    /// True after successful init().
    fn is_ready(&self) -> bool;
}
