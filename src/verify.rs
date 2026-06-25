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
                        bytes_retryable_total: 0,
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
                                    bytes_retryable_total: 0,
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
                            bytes_retryable_total: 0,
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

    // ── New comprehensive tests ────────────────────────────────────────────────

    /// A SectorSource that fails exactly `fail_count` calls, then succeeds.
    struct FailFirst {
        remaining_fails: usize,
    }
    impl SectorSource for FailFirst {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize, Error> {
            if self.remaining_fails > 0 {
                self.remaining_fails -= 1;
                Err(Error::ScsiError {
                    opcode: 0x28,
                    status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
                    sense: None,
                })
            } else {
                let n = count as usize * 2048;
                buf[..n].fill(0);
                Ok(n)
            }
        }
    }

    fn title_with_two_extents(s1: u32, c1: u32, s2: u32, c2: u32) -> DiscTitle {
        DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: vec![
                Extent {
                    start_lba: s1,
                    sector_count: c1,
                },
                Extent {
                    start_lba: s2,
                    sector_count: c2,
                },
            ],
            content_format: ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    /// readable_pct on a fully-good result is 100.0 (not NaN or < 100).
    /// Mutation: changing the formula to `(total - bad - 1) / total` makes this fail.
    #[test]
    fn readable_pct_all_good_is_exactly_100() {
        let r = VerifyResult {
            total_sectors: 100,
            good: 100,
            slow: 0,
            recovered: 0,
            bad: 0,
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        assert_eq!(r.readable_pct(), 100.0);
    }

    /// readable_pct with zero total_sectors must return 100.0, not NaN/inf
    /// from a division by zero.
    /// Spec: the doc-comment explicitly states "Returns 100.0 when there are no sectors."
    /// Mutation: removing the `if self.total_sectors == 0` guard makes this panic or NaN.
    #[test]
    fn readable_pct_empty_disc_is_100() {
        let r = VerifyResult {
            total_sectors: 0,
            good: 0,
            slow: 0,
            recovered: 0,
            bad: 0,
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        assert_eq!(r.readable_pct(), 100.0);
    }

    /// readable_pct counts slow and recovered as readable (not bad).
    /// Spec: doc says "good + slow + recovered" are readable.
    /// Mutation: omitting `slow` or `recovered` from the numerator reduces the result.
    #[test]
    fn readable_pct_counts_slow_and_recovered_as_readable() {
        // 6 of 10 sectors are "lost" (bad); 4 are readable (slow + recovered).
        let r = VerifyResult {
            total_sectors: 10,
            good: 0,
            slow: 2,
            recovered: 2,
            bad: 6,
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        // saturating_sub(6) = 4; 4/10 = 40.0
        let got = r.readable_pct();
        assert!((got - 40.0).abs() < 1e-9, "expected 40.0, got {got}");
    }

    /// is_perfect() is false when there are slow sectors, even if bad == 0.
    /// Spec: doc says "no bad, no recovered-on-retry, and no slow sectors".
    /// Mutation: removing `self.slow == 0` from is_perfect() makes this pass when it should fail.
    #[test]
    fn is_perfect_false_when_slow_present() {
        let r = VerifyResult {
            total_sectors: 10,
            good: 9,
            slow: 1,
            recovered: 0,
            bad: 0,
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        assert!(!r.is_perfect());
    }

    /// is_perfect() is false when there are recovered sectors.
    /// Mutation: removing `self.recovered == 0` from is_perfect() makes this fail.
    #[test]
    fn is_perfect_false_when_recovered_present() {
        let r = VerifyResult {
            total_sectors: 10,
            good: 9,
            slow: 0,
            recovered: 1,
            bad: 0,
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        assert!(!r.is_perfect());
    }

    /// is_perfect() is false when there are bad sectors.
    /// Mutation: removing `self.bad == 0` from is_perfect() makes this fail.
    #[test]
    fn is_perfect_false_when_bad_present() {
        let r = VerifyResult {
            total_sectors: 5,
            good: 4,
            slow: 0,
            recovered: 0,
            bad: 1,
            ranges: Vec::new(),
            elapsed_secs: 0.0,
        };
        assert!(!r.is_perfect());
    }

    /// chapter_at_offset returns None when chapters is empty.
    /// Mutation: removing the `chapters.is_empty()` check makes this return Some.
    #[test]
    fn chapter_at_offset_empty_chapters_returns_none() {
        let result = VerifyResult::chapter_at_offset(&[], 1024, 120.0, 100_000);
        assert!(result.is_none());
    }

    /// chapter_at_offset returns None when total_bytes == 0 (division-by-zero guard).
    /// Mutation: removing the `total_bytes == 0` guard makes this panic or NaN.
    #[test]
    fn chapter_at_offset_zero_total_bytes_returns_none() {
        use crate::disc::Chapter;
        let chapters = vec![Chapter {
            time_secs: 0.0,
            name: String::new(),
        }];
        let result = VerifyResult::chapter_at_offset(&chapters, 1024, 120.0, 0);
        assert!(result.is_none());
    }

    /// chapter_at_offset maps byte offset to the correct chapter index (1-based).
    /// Spec: time_secs = byte_offset / total_bytes * duration; chapter = last chapter
    ///       whose time_secs <= computed time.
    /// Mutation: off-by-one on chapter_idx (using `i` instead of `chapter_idx`) changes result.
    #[test]
    fn chapter_at_offset_selects_correct_chapter() {
        use crate::disc::Chapter;
        // 3 chapters at 0s, 30s, 60s; disc is 120s, 120,000 bytes.
        let chapters = vec![
            Chapter {
                time_secs: 0.0,
                name: String::new(),
            },
            Chapter {
                time_secs: 30.0,
                name: String::new(),
            },
            Chapter {
                time_secs: 60.0,
                name: String::new(),
            },
        ];
        let total_bytes: u64 = 120_000;
        let duration = 120.0_f64;

        // Byte 90,000 → time 90.0s → chapter 3 (index 2, 1-based = 3).
        let (ch, t) =
            VerifyResult::chapter_at_offset(&chapters, 90_000, duration, total_bytes).unwrap();
        assert_eq!(ch, 3, "expected chapter 3, got {ch}");
        assert!((t - 90.0).abs() < 1e-6, "expected t≈90.0, got {t}");

        // Byte 0 → time 0s → chapter 1 (first chapter).
        let (ch0, _) =
            VerifyResult::chapter_at_offset(&chapters, 0, duration, total_bytes).unwrap();
        assert_eq!(ch0, 1);
    }

    /// A contiguous sequence of bad sectors in one extent must be coalesced
    /// into a single SectorRange (not one entry per sector).
    /// Mutation: removing the range-merge logic produces many small ranges.
    #[test]
    fn contiguous_bad_sectors_coalesce_into_one_range() {
        // 4-sector extent, all bad. Batch size 1 forces per-sector path.
        // With cancel-on-second-bad we'd stop early; we must let all 4 sectors
        // run to check coalescing. Use no callback so they all complete.
        // But AlwaysBad + no callback is slow (RETRY_PAUSE per sector × 4).
        // Limit to 2 sectors to keep wall-clock reasonable.
        let title = title_with_extent(100, 2);
        let mut src = AlwaysBad;
        // no callback → full 2 × RETRY_PAUSE, but only 2 sectors
        let r = verify_title(&mut src, &title, 1, None);
        // Both sectors are bad and contiguous; they must merge into one range.
        assert_eq!(r.bad, 2);
        assert_eq!(
            r.ranges.len(),
            1,
            "contiguous bad sectors must coalesce: got {} ranges",
            r.ranges.len()
        );
        assert_eq!(r.ranges[0].start_lba, 100);
        assert_eq!(r.ranges[0].count, 2);
        assert_eq!(r.ranges[0].status, SectorStatus::Bad);
    }

    /// A batch that fails, then each individual sector succeeds on the first
    /// per-sector read, is counted as Good (not Recovered).
    /// Mutation: counting per-sector-first-ok as Recovered instead of Good flips this.
    #[test]
    fn batch_fail_then_per_sector_ok_counts_as_good() {
        // First read fails (batch), then all per-sector reads succeed.
        let mut src = FailFirst { remaining_fails: 1 };
        let title = title_with_extent(0, 2);
        let r = verify_title(&mut src, &title, 2, None);
        // Both sectors should be Good (or Slow if the read was slow — but
        // FailFirst returns instantly, so they should be Good).
        assert_eq!(r.bad, 0);
        assert_eq!(r.recovered, 0);
        // Good ≥ 1: at least one sector was read clean on the per-sector path.
        assert!(r.good >= 1, "expected some good sectors, got {}", r.good);
    }

    /// A recovered sector (batch fails, per-sector fails, retry succeeds)
    /// appears in ranges with SectorStatus::Recovered.
    /// This is exercised by FailFirst with remaining_fails=2: batch fails (1),
    /// first per-sector attempt fails (2), retry succeeds (0 remaining).
    /// Mutation: mapping recovered to Good removes the Recovered range entry.
    #[test]
    fn recovered_sector_appears_in_ranges_as_recovered() {
        // Batch read of 1 sector: fail once (batch), fail once (per-sector),
        // then succeed (retry). That yields Recovered status.
        let mut src = FailFirst { remaining_fails: 2 };
        let title = title_with_extent(0, 1);
        // Provide a callback that never cancels so the retry pause can run.
        // But we want the test to be fast — FailFirst returns immediately,
        // so the cancel-during-pause path won't be hit. However, we still
        // sleep RETRY_PAUSE here (2s). Use no-cancel callback.
        let noop_cb = |_: &crate::progress::PassProgress| true;
        let r = verify_title(&mut src, &title, 1, Some(&noop_cb));
        assert_eq!(r.recovered, 1, "sector should be Recovered");
        assert_eq!(r.bad, 0);
        assert_eq!(r.ranges.len(), 1);
        assert_eq!(r.ranges[0].status, SectorStatus::Recovered);
    }

    /// total_sectors sums across multiple extents correctly.
    /// Mutation: only summing the first extent makes total wrong.
    #[test]
    fn total_sectors_sums_multiple_extents() {
        let title = title_with_two_extents(0, 3, 100, 5);
        let mut src = AlwaysGood;
        let r = verify_title(&mut src, &title, 4, None);
        assert_eq!(r.total_sectors, 8, "expected 3+5=8 sectors");
        assert_eq!(r.good, 8);
    }

    /// SectorRange byte_offset is zero for the very first sector of the first extent.
    /// Mutation: starting byte_offset at 2048 instead of 0 shifts all offsets.
    #[test]
    fn sector_range_byte_offset_starts_at_zero_for_first_sector() {
        // AlwaysBad with 1 sector: the range for LBA 0 must have byte_offset=0.
        let title = title_with_extent(0, 1);
        let mut src = AlwaysBad;
        let r = verify_title(&mut src, &title, 1, None);
        assert_eq!(r.ranges.len(), 1);
        assert_eq!(
            r.ranges[0].byte_offset, 0,
            "first sector byte_offset must be 0"
        );
    }

    /// SectorRange byte_offset for the second sector is 2048.
    /// Spec: sector size = 2048 bytes on Blu-ray (ISO 9660 / ECMA-119 §7).
    /// Mutation: computing offset as `i * 2049` changes the second sector offset.
    #[test]
    fn sector_range_byte_offset_increments_by_2048() {
        // Force the second sector to be bad: fail twice (first two reads
        // attempt LBA 0 in batch, then individually). That is complex.
        // Simpler: use a source that only fails on LBA 1.
        struct BadOnLba1;
        impl SectorSource for BadOnLba1 {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize, Error> {
                if lba == 1 && count == 1 {
                    Err(Error::ScsiError {
                        opcode: 0x28,
                        status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
                        sense: None,
                    })
                } else if lba == 0 && count == 2 {
                    // Batch of 2 fails when it includes LBA 1 — simulate batch
                    // failure so we fall to per-sector.
                    Err(Error::ScsiError {
                        opcode: 0x28,
                        status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
                        sense: None,
                    })
                } else {
                    let n = count as usize * 2048;
                    buf[..n].fill(0);
                    Ok(n)
                }
            }
        }
        // Extent: LBA 0..1 (2 sectors), batch size 2 so we get a batch attempt first.
        let title = title_with_extent(0, 2);
        let mut src = BadOnLba1;
        let noop_cb = |_: &crate::progress::PassProgress| true;
        let r = verify_title(&mut src, &title, 2, Some(&noop_cb));
        // We expect exactly one bad sector at LBA 1.
        assert_eq!(r.bad, 1);
        let bad_range = r
            .ranges
            .iter()
            .find(|rng| rng.status == SectorStatus::Bad)
            .unwrap();
        // LBA 1 is the second sector → byte_offset = 1 * 2048 = 2048.
        assert_eq!(
            bad_range.byte_offset, 2048,
            "second sector byte_offset must be 2048, got {}",
            bad_range.byte_offset
        );
    }

    /// cancellable_pause returns true when it completes the full duration
    /// without cancellation.
    /// Mutation: returning `false` unconditionally makes callers think they were cancelled.
    #[test]
    fn cancellable_pause_returns_true_when_not_cancelled() {
        // Use a very short duration so the test runs fast.
        let tiny = Duration::from_millis(10);
        let result = cancellable_pause(tiny, &mut || false);
        assert!(result, "not-cancelled pause must return true");
    }

    /// cancellable_pause returns false immediately when the callback requests cancel.
    /// Mutation: ignoring the callback return and sleeping the full duration breaks this.
    #[test]
    fn cancellable_pause_returns_false_when_cancelled() {
        let long = Duration::from_secs(60);
        let started = Instant::now();
        let result = cancellable_pause(long, &mut || true);
        assert!(!result, "cancelled pause must return false");
        // Must return well within the first poll interval, not after 60s.
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
