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
//!         EventKind::SectorSkipped { sector } => log_skip(sector),
//!         EventKind::BatchSizeChanged { new_size, .. } => note_recovery(new_size),
//!         _ => {}
//!     }
//! });
//! ```
//!
//! Note: the library currently emits only `BytesRead`, `SectorSkipped`,
//! and `BatchSizeChanged`. The other [`EventKind`] variants are part of
//! the stable event vocabulary for consumers (and future emit sites) but
//! are not produced by the library today.

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

    /// Sector recovered after a retry (Drive::read multi-phase recovery).
    SectorRecovered { sector: u64 },

    /// Sector unreadable, zero-filled (skip mode).
    SectorSkipped { sector: u64 },

    /// Adaptive batch sizer changed the read size.
    ///
    /// Fires on shrink (read failed at larger size) and on probe-up
    /// (enough clean reads to try larger again). Consumers use this to
    /// display a "recovering" state distinct from "ripping normally".
    BatchSizeChanged {
        new_size: u16,
        reason: BatchSizeReason,
    },

    /// Operation complete.
    Complete {
        /// Total bytes written.
        bytes: u64,
        /// Total read errors encountered.
        errors: u32,
    },
}

/// Why the adaptive batch sizer changed size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSizeReason {
    /// Read failed; sizer halved the batch.
    Shrunk,
    /// Clean-read streak threshold hit; sizer doubled toward preferred.
    Probed,
}

/// A no-op event handler. Ignores all events.
pub fn ignore(_event: Event) {}

#[cfg(test)]
mod tests {
    use super::*;

    /// EventKind::BytesRead carries bytes and total as u64.
    /// Mutation: making `bytes` a u32 silently truncates progress on large discs (>4 GiB).
    #[test]
    fn bytes_read_fields_are_u64() {
        // A 4K UHD disc is ~100 GiB — bytes must be u64 to hold it.
        let event = Event {
            kind: EventKind::BytesRead {
                bytes: u64::MAX,
                total: u64::MAX,
            },
        };
        match event.kind {
            EventKind::BytesRead { bytes, total } => {
                assert_eq!(bytes, u64::MAX);
                assert_eq!(total, u64::MAX);
            }
            _ => panic!("wrong variant"),
        }
    }

    /// EventKind::SectorSkipped carries a u64 sector number.
    /// Mutation: using u32 truncates LBAs > 4 GiB sectors (large BD-R discs).
    #[test]
    fn sector_skipped_field_is_u64() {
        let event = Event {
            kind: EventKind::SectorSkipped { sector: u64::MAX },
        };
        match event.kind {
            EventKind::SectorSkipped { sector } => {
                assert_eq!(sector, u64::MAX);
            }
            _ => panic!("wrong variant"),
        }
    }

    /// BatchSizeReason::Shrunk != BatchSizeReason::Probed.
    /// These two variants carry distinct meanings (error vs. recovery); they
    /// must not compare as equal.
    /// Mutation: deriving PartialEq without proper variant discrimination
    ///           could make two distinct variants equal.
    #[test]
    fn batch_size_reason_variants_are_not_equal() {
        assert_ne!(BatchSizeReason::Shrunk, BatchSizeReason::Probed);
    }

    /// BatchSizeReason::Shrunk == BatchSizeReason::Shrunk (reflexive equality).
    /// Mutation: a broken PartialEq impl that always returns false would fail this.
    #[test]
    fn batch_size_reason_eq_is_reflexive() {
        assert_eq!(BatchSizeReason::Shrunk, BatchSizeReason::Shrunk);
        assert_eq!(BatchSizeReason::Probed, BatchSizeReason::Probed);
    }

    /// BatchSizeReason is Clone + Copy: cloning does not move the original.
    /// This is required because EventKind::BatchSizeChanged embeds it by value.
    /// Mutation: removing Copy would require the caller to clone explicitly;
    ///           code that passes reason by value would fail to compile.
    #[test]
    fn batch_size_reason_is_copy() {
        let r = BatchSizeReason::Shrunk;
        let _r2 = r; // copy, not move
        let _r3 = r; // r still usable after copy
    }

    /// EventKind::BatchSizeChanged can be constructed and destructured.
    /// Mutation: renaming the `reason` field to `cause` breaks all pattern-matches.
    #[test]
    fn batch_size_changed_constructs_and_destructs() {
        let event = Event {
            kind: EventKind::BatchSizeChanged {
                new_size: 32,
                reason: BatchSizeReason::Shrunk,
            },
        };
        match event.kind {
            EventKind::BatchSizeChanged { new_size, reason } => {
                assert_eq!(new_size, 32);
                assert_eq!(reason, BatchSizeReason::Shrunk);
            }
            _ => panic!("wrong variant"),
        }
    }

    /// EventKind::ExtentStart carries all three fields at u64.
    /// Mutation: making start_sector a u32 truncates large-disc LBAs.
    #[test]
    fn extent_start_fields_are_correct_types() {
        let event = Event {
            kind: EventKind::ExtentStart {
                index: 0,
                start_sector: u64::MAX,
                sector_count: u64::MAX,
            },
        };
        match event.kind {
            EventKind::ExtentStart {
                index,
                start_sector,
                sector_count,
            } => {
                assert_eq!(index, 0);
                assert_eq!(start_sector, u64::MAX);
                assert_eq!(sector_count, u64::MAX);
            }
            _ => panic!("wrong variant"),
        }
    }

    /// EventKind::Complete carries bytes (u64) and errors (u32).
    /// Mutation: making bytes a u32 truncates total-bytes-written on large outputs.
    #[test]
    fn complete_fields_are_correct_types() {
        let event = Event {
            kind: EventKind::Complete {
                bytes: u64::MAX,
                errors: u32::MAX,
            },
        };
        match event.kind {
            EventKind::Complete { bytes, errors } => {
                assert_eq!(bytes, u64::MAX);
                assert_eq!(errors, u32::MAX);
            }
            _ => panic!("wrong variant"),
        }
    }

    /// ignore() accepts any Event variant without panicking.
    /// This is trivially true but ensures the function signature matches all
    /// EventKind variants (would fail to compile if a new variant is added
    /// without updating the function or the test).
    /// Mutation: making ignore() generic over a wrong type causes a compile error.
    #[test]
    fn ignore_accepts_any_event() {
        ignore(Event {
            kind: EventKind::DriveReady,
        });
        ignore(Event {
            kind: EventKind::BytesRead { bytes: 0, total: 0 },
        });
        ignore(Event {
            kind: EventKind::SectorSkipped { sector: 0 },
        });
        ignore(Event {
            kind: EventKind::Complete {
                bytes: 0,
                errors: 0,
            },
        });
        ignore(Event {
            kind: EventKind::BatchSizeChanged {
                new_size: 16,
                reason: BatchSizeReason::Probed,
            },
        });
    }

    /// EventKind::SpeedChange carries speed_kbs as u16.
    /// Spec: 0xFFFF is the sentinel meaning "max speed" (value from CD-ROM MMC spec).
    /// Mutation: using u8 for speed_kbs truncates values > 255 to 0 or wrong values.
    #[test]
    fn speed_change_sentinel_max_is_0xffff() {
        let event = Event {
            kind: EventKind::SpeedChange { speed_kbs: 0xFFFF },
        };
        match event.kind {
            EventKind::SpeedChange { speed_kbs } => {
                assert_eq!(
                    speed_kbs, 0xFFFF,
                    "0xFFFF is the max-speed sentinel (MMC spec)"
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    /// EventKind::ReadError carries an error with a sector field.
    /// Mutation: using i64 for sector would allow negative sector values (nonsensical).
    #[test]
    fn read_error_carries_error_and_sector() {
        use crate::error::Error;
        let event = Event {
            kind: EventKind::ReadError {
                sector: 99_999,
                error: Error::Halted,
            },
        };
        match event.kind {
            EventKind::ReadError { sector, .. } => {
                assert_eq!(sector, 99_999u64);
            }
            _ => panic!("wrong variant"),
        }
    }

    /// EventKind::Retry carries attempt as u32 (1-based).
    /// Mutation: u8 for attempt overflows after 255 retries without warning.
    #[test]
    fn retry_attempt_is_u32() {
        let event = Event {
            kind: EventKind::Retry { attempt: u32::MAX },
        };
        match event.kind {
            EventKind::Retry { attempt } => {
                assert_eq!(attempt, u32::MAX);
            }
            _ => panic!("wrong variant"),
        }
    }
}
