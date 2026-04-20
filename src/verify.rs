//! Disc sector verification — read every sector and classify health.

use crate::disc::{Chapter, DiscTitle, Extent};
use crate::sector::SectorReader;
use std::time::Instant;

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
    pub fn readable_pct(&self) -> f64 {
        if self.total_sectors == 0 {
            return 100.0;
        }
        (self.total_sectors - self.bad) as f64 / self.total_sectors as f64 * 100.0
    }

    /// True if every sector read successfully.
    pub fn is_perfect(&self) -> bool {
        self.bad == 0 && self.recovered == 0 && self.slow == 0
    }

    /// Map a bad sector range to a chapter timestamp.
    pub fn chapter_at_offset(chapters: &[Chapter], byte_offset: u64, duration_secs: f64, total_bytes: u64) -> Option<(usize, f64)> {
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

/// Progress callback: (sectors_done, total_sectors, current_status)
pub type ProgressFn = Box<dyn FnMut(u64, u64, SectorStatus)>;

/// Verify all sectors in a title's extents.
/// Reads in batches for speed, falls back to single-sector on failure.
pub fn verify_title(
    reader: &mut dyn SectorReader,
    title: &DiscTitle,
    batch_sectors: u16,
    mut on_progress: Option<ProgressFn>,
) -> VerifyResult {
    let start = Instant::now();
    let mut good: u64 = 0;
    let mut slow: u64 = 0;
    let mut recovered: u64 = 0;
    let mut bad: u64 = 0;
    let mut ranges: Vec<SectorRange> = Vec::new();
    let mut sectors_done: u64 = 0;
    let mut byte_offset: u64 = 0;

    let total_sectors: u64 = title.extents.iter().map(|e| e.sector_count as u64).sum();
    let mut buf = vec![0u8; batch_sectors as usize * 2048];

    for ext in &title.extents {
        let mut offset: u32 = 0;
        while offset < ext.sector_count {
            let remaining = ext.sector_count - offset;
            let count = remaining.min(batch_sectors as u32) as u16;
            let lba = ext.start_lba + offset;
            let bytes = count as usize * 2048;

            let batch_start = Instant::now();
            let batch_ok = reader
                .read_sectors_recover(lba, count, &mut buf[..bytes], false)
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
                        ranges.push(SectorRange {
                            start_lba: lba,
                            count: count as u32,
                            status: SectorStatus::Slow,
                            byte_offset,
                        });
                    }
                    _ => {}
                }

                sectors_done += count as u64;
                if let Some(ref mut cb) = on_progress {
                    cb(sectors_done, total_sectors, status);
                }
            } else {
                // Batch failed — test each sector individually
                for i in 0..count {
                    let sector_lba = lba + i as u32;
                    let sector_offset = i as usize * 2048;
                    let sector_byte_offset = byte_offset + i as u64 * 2048;

                    let s1 = Instant::now();
                    let first_ok = reader
                        .read_sectors_recover(sector_lba, 1, &mut buf[sector_offset..sector_offset + 2048], false)
                        .is_ok();
                    let s1_ms = s1.elapsed().as_millis();

                    let status = if first_ok && s1_ms <= 500 {
                        good += 1;
                        SectorStatus::Good
                    } else if first_ok {
                        slow += 1;
                        SectorStatus::Slow
                    } else {
                        // Retry once more after brief pause
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        if reader
                            .read_sectors_recover(sector_lba, 1, &mut buf[sector_offset..sector_offset + 2048], false)
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
                                && last.start_lba + last.count == sector_lba
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
                    if let Some(ref mut cb) = on_progress {
                        cb(sectors_done, total_sectors, status);
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
