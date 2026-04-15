//! Event system for progress and status reporting.
//!
//! The lib fires events during operations like rip().
//! The app registers a callback to receive them.
//! No display logic, no English text — just data.
//!
//! ```rust,ignore
//! disc.rip(&mut session, 0, output, |event| {
//!     match event.kind {
//!         EventKind::BytesRead { bytes, total } => update_progress(bytes, total),
//!         EventKind::ReadError { sector, .. } => log_error(sector),
//!         _ => {}
//!     }
//! });
//! ```

use crate::error::Error;

/// An event fired by the lib during operations.
#[derive(Debug)]
pub struct Event {
    pub kind: EventKind,
}

/// Types of events the lib can fire.
#[derive(Debug)]
pub enum EventKind {
    // ── Init sequence events ────────────────────────────────────────

    /// Drive opened successfully.
    DriveOpened { device: String },

    /// Drive is ready (disc spun up).
    DriveReady,

    /// Firmware init completed.
    InitComplete { success: bool },

    /// Disc probe completed.
    ProbeComplete { success: bool },

    /// Disc scan completed.
    ScanComplete { titles: usize },

    // ── Read events ─────────────────────────────────────────────────

    /// Bytes successfully read and written to output.
    BytesRead {
        /// Bytes written so far.
        bytes: u64,
        /// Total bytes expected (0 if unknown).
        total: u64,
    },

    /// A read error occurred. The lib will retry automatically.
    ReadError {
        /// Sector that failed.
        sector: u64,
        /// Error code.
        error: Error,
    },

    /// Retrying a failed read.
    Retry {
        /// Current attempt number (1-based).
        attempt: u32,
    },

    /// Drive speed changed (error recovery or restoration).
    SpeedChange {
        /// New speed in KB/s (0xFFFF = max).
        speed_kbs: u16,
    },

    /// Starting a new disc extent.
    ExtentStart {
        /// Extent index (0-based).
        index: usize,
        /// First sector of extent.
        start_sector: u64,
        /// Number of sectors in extent.
        sector_count: u64,
    },

    /// Operation complete.
    Complete {
        /// Total bytes written.
        bytes: u64,
        /// Total read errors encountered.
        errors: u32,
    },
}

/// A no-op event handler. Ignores all events.
pub fn ignore(_event: Event) {}
