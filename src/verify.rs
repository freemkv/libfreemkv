//! Disc sector verification — read every sector and classify health.

use crate::disc::{Chapter, DiscTitle};
use crate::progress::{PassProgress, Progress};
use crate::sector::SectorSource;
use std::time::{Duration, Instant};

/// Pause before the single retry attempt on a failed sector, letting a
/// drive that briefly went NOT READY spin back up. Slept in
/// [`RETRY_POLL_INTERVAL`] increments so a cancel request (the progress
/// callback returning `false`) is observed within that interval rather
/// than after the full pause.
const RETRY_PAUSE: Duration = Duration::from_secs(2);

/// Polling cadence while sleeping out [`RETRY_PAUSE`].
const RETRY_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Sleep up to `total`, returning early with `false` if `cancelled`
/// observes a stop request mid-pause. Returns `true` if the full pause
/// elapsed without cancellation.
fn cancellable_pause(total: Duration, cancelled: &mut dyn FnMut() -> bool) -> bool {
    let deadline = Instant::now() + total;
    loop {
        if cancelled() {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        std::thread::sleep(RETRY_POLL_INTERVAL.min(deadline - now));
    }
}

/// Health status of a single sector read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SectorStatus {
    /// Read succeeded on first attempt, under 500ms
    Good,
    /// Read succeeded but took longer than expected (>500ms per sector)
    Slow,
    /// Read failed initially but succeeded on retry
    Recovered,
    /// Read failed all attempts — unrecoverable
    Bad,
}

/// A contiguous range of bad or slow sectors.
#[derive(Debug, Clone)]
pub struct SectorRange {
    pub start_lba: u32,
    pub count: u32,
    pub status: SectorStatus,
    /// Byte offset from start of title
    pub byte_offset: u64,
}

/// Result of a disc verification scan.
#[derive(Debug, Clone)]
pub struct VerifyResult {
    pub total_sectors: u64,
    pub good: u64,
    pub slow: u64,
    pub recovered: u64,
    pub bad: u64,
    /// Contiguous ranges of non-good sectors
    pub ranges: Vec<SectorRange>,
    pub elapsed_secs: f64,
}

impl VerifyResult {
    /// Percentage of sectors that are fully readable (good + slow + recovered).
    ///
    /// Returns `100.0` when there are no sectors. The subtraction is
    /// saturating so a caller-constructed `VerifyResult` with `bad` greater
    /// than `total_sectors` cannot wrap in release builds (it pins at 0%).
    pub fn readable_pct(&self) -> f64 {
        if self.total_sectors == 0 {
            return 100.0;
        }
        self.total_sectors.saturating_sub(self.bad) as f64 / self.total_sectors as f64 * 100.0
    }

    /// True if every sector read first-attempt clean — no bad, no
    /// recovered-on-retry, and no slow sectors. A `Recovered` or `Slow`
    /// sector means data was eventually returned but the surface is
    /// degraded, so `is_perfect()` is intentionally stricter than
    /// "fully readable" (which [`readable_pct`](Self::readable_pct)
    /// reports by counting recovered + slow as readable).
    pub fn is_perfect(&self) -> bool {
        self.bad == 0 && self.recovered == 0 && self.slow == 0
    }

    /// Map a bad sector range to a chapter timestamp.
    pub fn chapter_at_offset(
        chapters: &[Chapter],
        byte_offset: u64,
        duration_secs: f64,
        total_bytes: u64,
    ) -> Option<(usize, f64)> {
        if total_bytes == 0 || chapters.is_empty() {
            return None;
        }
        let time_secs = byte_offset as f64 / total_bytes as f64 * duration_secs;
        let mut chapter_idx = 0;
        for (i, ch) in chapters.iter().enumerate() {
            if ch.time_secs <= time_secs {
                chapter_idx = i;
            } else {
                break;
            }
        }
        Some((chapter_idx + 1, time_secs))
    }
}

/// Verify all sectors in a title's extents.
///
/// Reads in batches for speed, falling back to single-sector reads on a
/// batch failure. Each sector that fails its single read gets one retry
/// after a [`RETRY_PAUSE`] cool-down (cancellable — see below).
///
/// `batch_sectors` is the read granularity in 2048-byte sectors; a value
/// of `0` is treated as `1` (a `0` batch would make no forward progress).
///
/// `on_progress` is both the progress sink and the cancellation channel:
/// returning `false` from [`Progress::report`] requests an early stop. The
/// stop is honoured between sectors *and* mid-retry-pause, so a verify of a
/// disc with many bad sectors stays responsive instead of blocking the full
/// pause per sector. Timing classification uses a 500 ms-per-sector
/// threshold to mark a successful-but-slow read as [`SectorStatus::Slow`].
pub fn verify_title(
    reader: &mut dyn SectorSource,
    title: &DiscTitle,
    batch_sectors: u16,
    on_progress: Option<&dyn Progress>,
) -> VerifyResult {
    let start = Instant::now();
    let mut good: u64 = 0;
    let mut slow: u64 = 0;
    let mut recovered: u64 = 0;
    let mut bad: u64 = 0;
    let mut ranges: Vec<SectorRange> = Vec::new();
    let mut sectors_done: u64 = 0;
    let mut byte_offset: u64 = 0;

    // A zero batch size would never advance `offset` -> infinite loop.
    // Clamp to at least one sector per read.
    let batch_sectors = batch_sectors.max(1);

    let total_sectors: u64 = title.extents.iter().map(|e| e.sector_count as u64).sum();
    let mut buf = vec![0u8; batch_sectors as usize * 2048];

    'outer: for ext in &title.extents {
        let mut offset: u32 = 0;
        while offset < ext.sector_count {
            let remaining = ext.sector_count - offset;
            let count = remaining.min(batch_sectors as u32) as u16;
            // `start_lba + offset` stays within the extent's own LBA span by
            // construction, but a crafted/corrupt extent can push the sum past
            // u32::MAX; saturate rather than wrap (release) or panic (debug).
            let lba = ext.start_lba.saturating_add(offset);
            let bytes = count as usize * 2048;

            let batch_start = Instant::now();
            let batch_ok = reader
                .read_sectors(lba, count, &mut buf[..bytes], false)
                .is_ok();
            let batch_ms = batch_start.elapsed().as_millis();

            if batch_ok {
                // Batch succeeded — classify by speed
                let status = if batch_ms > count as u128 * 500 {
                    SectorStatus::Slow
                } else {
                    SectorStatus::Good
                };

                match status {
                    SectorStatus::Good => good += count as u64,
                    SectorStatus::Slow => {
                        slow += count as u64;
                        // Merge with the previous range when contiguous and the
                        // same status, mirroring the per-sector path so a run of
                        // slow batches coalesces into one range instead of one
                        // entry per batch.
                        let merged = ranges.last_mut().is_some_and(|last| {
                            if last.status == SectorStatus::Slow
                                && last.start_lba.saturating_add(last.count) == lba
                            {
                                last.count = last.count.saturating_add(count as u32);
                                true
                            } else {
                                false
                            }
                        });
                        if !merged {
                            ranges.push(SectorRange {
                                start_lba: lba,
                                count: count as u32,
                                status: SectorStatus::Slow,
                                byte_offset,
                            });
                        }
                    }
                    _ => {}
                }

                sectors_done += count as u64;
                if let Some(cb) = on_progress {
                    let pp = crate::progress::PassProgress {
                        kind: crate::progress::PassKind::Verify,
                        work_done: sectors_done,
                        work_total: total_sectors,
                        bytes_good_total: (good + slow + recovered) * 2048,
                        bytes_unreadable_total: bad * 2048,
                        bytes_pending_total: 0,
                        bytes_total_disc: total_sectors * 2048,
                        disc_duration_secs: Some(title.duration_secs),
                        bytes_bad_in_main_title: 0,
                        main_title_duration_secs: Some(title.duration_secs),
                        main_title_size_bytes: Some(total_sectors * 2048),
                    };
                    if !cb.report(&pp) {
                        break 'outer;
                    }
                }
            } else {
                // Batch failed — test each sector individually
                for i in 0..count {
                    let sector_lba = lba.saturating_add(i as u32);
                    let sector_offset = i as usize * 2048;
                    let sector_byte_offset = byte_offset + i as u64 * 2048;

                    let s1 = Instant::now();
                    let first_ok = reader
                        .read_sectors(
                            sector_lba,
                            1,
                            &mut buf[sector_offset..sector_offset + 2048],
                            false,
                        )
                        .is_ok();
                    let s1_ms = s1.elapsed().as_millis();

                    let status = if first_ok && s1_ms <= 500 {
                        good += 1;
                        SectorStatus::Good
                    } else if first_ok {
                        slow += 1;
                        SectorStatus::Slow
                    } else {
                        // Retry once more after a brief cool-down. The pause is
                        // cancellable via the progress callback so a long run of
                        // bad sectors doesn't pin the thread for 2s each with no
                        // way to stop. If cancelled mid-pause, skip the retry and
                        // count the sector as bad, then bail out of the scan.
                        let cancelled_during_pause = !cancellable_pause(RETRY_PAUSE, &mut || {
                            on_progress.is_some_and(|cb| {
                                !cb.report(&PassProgress {
                                    kind: crate::progress::PassKind::Verify,
                                    work_done: sectors_done,
                                    work_total: total_sectors,
                                    bytes_good_total: (good + slow + recovered) * 2048,
                                    bytes_unreadable_total: bad * 2048,
                                    bytes_pending_total: 0,
                                    bytes_total_disc: total_sectors * 2048,
                                    disc_duration_secs: Some(title.duration_secs),
                                    bytes_bad_in_main_title: 0,
                                    main_title_duration_secs: Some(title.duration_secs),
                                    main_title_size_bytes: Some(total_sectors * 2048),
                                })
                            })
                        });
                        if cancelled_during_pause {
                            bad += 1;
                            break 'outer;
                        }
                        if reader
                            .read_sectors(
                                sector_lba,
                                1,
                                &mut buf[sector_offset..sector_offset + 2048],
                                false,
                            )
                            .is_ok()
                        {
                            recovered += 1;
                            SectorStatus::Recovered
                        } else {
                            bad += 1;
                            SectorStatus::Bad
                        }
                    };

                    if status != SectorStatus::Good {
                        // Merge with previous range if contiguous and same status
                        if let Some(last) = ranges.last_mut() {
                            if last.status == status
                                && last.start_lba.saturating_add(last.count) == sector_lba
                            {
                                last.count += 1;
                            } else {
                                ranges.push(SectorRange {
                                    start_lba: sector_lba,
                                    count: 1,
                                    status,
                                    byte_offset: sector_byte_offset,
                                });
                            }
                        } else {
                            ranges.push(SectorRange {
                                start_lba: sector_lba,
                                count: 1,
                                status,
                                byte_offset: sector_byte_offset,
                            });
                        }
                    }

                    sectors_done += 1;
                    if let Some(cb) = on_progress {
                        let pp = crate::progress::PassProgress {
                            kind: crate::progress::PassKind::Verify,
                            work_done: sectors_done,
                            work_total: total_sectors,
                            bytes_good_total: (good + slow + recovered) * 2048,
                            bytes_unreadable_total: bad * 2048,
                            bytes_pending_total: 0,
                            bytes_total_disc: total_sectors * 2048,
                            disc_duration_secs: Some(title.duration_secs),
                            bytes_bad_in_main_title: 0,
                            main_title_duration_secs: Some(title.duration_secs),
                            main_title_size_bytes: Some(total_sectors * 2048),
                        };
                        if !cb.report(&pp) {
                            break 'outer;
                        }
                    }
                }
            }

            offset += count as u32;
            byte_offset += bytes as u64;
        }
    }

    VerifyResult {
        total_sectors,
        good,
        slow,
        recovered,
        bad,
        ranges,
        elapsed_secs: start.elapsed().as_secs_f64(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{ContentFormat, DiscTitle, Extent};
    use crate::error::Error;

    /// Synthetic source: every read succeeds, filling the buffer with zeros.
    struct AlwaysGood;
    impl SectorSource for AlwaysGood {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize, Error> {
            let n = count as usize * 2048;
            buf[..n].fill(0);
            Ok(n)
        }
    }

    /// Synthetic source: every read fails.
    struct AlwaysBad;
    impl SectorSource for AlwaysBad {
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize, Error> {
            Err(Error::ScsiError {
                opcode: 0x28,
                status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
                sense: None,
            })
        }
    }

    fn title_with_extent(start_lba: u32, sector_count: u32) -> DiscTitle {
        DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: vec![Extent {
                start_lba,
                sector_count,
            }],
            content_format: ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    #[test]
    fn batch_zero_does_not_hang() {
        // A zero batch size must be clamped to 1 internally; otherwise the
        // inner `while offset < sector_count` loop never advances. The
        // assertion only matters because the test returns at all.
        let title = title_with_extent(0, 4);
        let mut src = AlwaysGood;
        let r = verify_title(&mut src, &title, 0, None);
        assert_eq!(r.total_sectors, 4);
        assert_eq!(r.good, 4);
        assert!(r.is_perfect());
    }

    #[test]
    fn all_good_is_perfect() {
        let title = title_with_extent(100, 10);
        let mut src = AlwaysGood;
        let r = verify_title(&mut src, &title, 4, None);
        assert_eq!(r.good, 10);
        assert_eq!(r.bad, 0);
        assert!(r.ranges.is_empty());
        assert_eq!(r.readable_pct(), 100.0);
    }

    #[test]
    fn cancel_during_retry_pause_stops_promptly() {
        // AlwaysBad forces the single-sector retry path, which pauses before
        // its retry. The progress callback requests stop on its first call,
        // so the pause must return early and the scan must bail out rather
        // than sleeping RETRY_PAUSE * sector_count. A generous wall-clock
        // bound (well under a single full pause) proves cancellation worked.
        let title = title_with_extent(0, 8);
        let mut src = AlwaysBad;
        let cb = |_p: &PassProgress| false; // always request stop
        let started = Instant::now();
        let r = verify_title(&mut src, &title, 4, Some(&cb));
        assert!(
            started.elapsed() < RETRY_PAUSE,
            "verify did not honour cancel during retry pause"
        );
        // Cancelled mid-first-bad-sector: exactly one sector counted bad.
        assert_eq!(r.bad, 1);
    }

    #[test]
    fn readable_pct_saturates_on_inconsistent_counts() {
        let r = VerifyResult {
            total_sectors: 10,
            good: 0,
            slow: 0,
            recovered: 0,
            bad: 9999, // impossible in practice; must not wrap
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        assert_eq!(r.readable_pct(), 0.0);
    }

    #[test]
    fn high_lba_extent_does_not_overflow() {
        // An extent whose start_lba + offset would exceed u32::MAX must
        // saturate, not panic in debug builds.
        let title = title_with_extent(u32::MAX - 1, 4);
        let mut src = AlwaysGood;
        let r = verify_title(&mut src, &title, 2, None);
        assert_eq!(r.total_sectors, 4);
    }

    #[test]
    fn no_progress_callback_still_completes_on_bad_sectors() {
        // Without a callback there is no cancel signal, so each bad sector
        // takes the full RETRY_PAUSE. Keep the extent tiny (1 sector) so the
        // test stays fast while still exercising the bad+retry path.
        let title = title_with_extent(0, 1);
        let mut src = AlwaysBad;
        let r = verify_title(&mut src, &title, 1, None);
        assert_eq!(r.bad, 1);
        assert_eq!(r.ranges.len(), 1);
        assert_eq!(r.ranges[0].status, SectorStatus::Bad);
    }
}
