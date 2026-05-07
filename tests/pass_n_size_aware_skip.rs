//! Pass N (Disc::patch) size-aware-skip targeted tests.
//!
//! The user's failure mode (2026-05-07): "what if we have a 100 sector zone
//! and its really 2 25 sector zones and we keep jumping over the good in
//! the middle." Today's pre-fix patch escalates skip-distance based on
//! `consecutive_skips_without_recovery` with hardcoded 32 → 4096 sector
//! caps. A 100-sector bad range whose actual layout is 25 bad + 50 good +
//! 25 bad would have the patch skip 32-4096 sectors after a couple of
//! failures, leaping over the entire range AND the good middle.
//!
//! The fix: cap each skip at `range_remaining/4`. These tests exercise
//! that boundary.

use libfreemkv::disc::CopyOptions;
use libfreemkv::disc::mapfile::{Mapfile, SectorStatus};
use libfreemkv::error::Result;
use libfreemkv::disc::DiscRegion;
use libfreemkv::{ContentFormat, Disc, DiscFormat, SectorReader};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

const SECTOR_SIZE: usize = 2048;

/// Reader where you specify exactly which LBAs return Err. Everything else
/// returns Ok with the LBA encoded in each byte for verification.
struct PatternedSectorReader {
    capacity: u32,
    bad_lbas: HashSet<u32>,
    /// Trace every read so tests can assert what was actually attempted.
    trace: Arc<Mutex<Vec<(u32, u16)>>>,
}

impl PatternedSectorReader {
    fn new(capacity: u32, bad_lbas: HashSet<u32>) -> (Self, Arc<Mutex<Vec<(u32, u16)>>>) {
        let trace = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                capacity,
                bad_lbas,
                trace: trace.clone(),
            },
            trace,
        )
    }
}

impl SectorReader for PatternedSectorReader {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], _recovery: bool) -> Result<usize> {
        self.trace.lock().unwrap().push((lba, count));
        // Whole-batch fails if ANY sector in the batch is bad. (Models a
        // real drive: a multi-sector READ aborts on the first ECC failure.)
        for offset in 0..count as u32 {
            if self.bad_lbas.contains(&(lba + offset)) {
                return Err(libfreemkv::error::Error::ScsiError {
                    opcode: libfreemkv::scsi::SCSI_READ_10,
                    status: libfreemkv::scsi::SCSI_STATUS_CHECK_CONDITION,
                    sense: Some(libfreemkv::ScsiSense {
                        sense_key: libfreemkv::scsi::SENSE_KEY_MEDIUM_ERROR,
                        asc: 0x11,
                        ascq: 0x00,
                    }),
                });
            }
        }
        for chunk in buf.chunks_mut(SECTOR_SIZE) {
            chunk.fill((lba & 0xff) as u8);
        }
        Ok(buf.len())
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}

fn synthetic_disc(capacity_sectors: u32) -> Disc {
    Disc {
        volume_id: String::new(),
        meta_title: None,
        format: DiscFormat::BluRay,
        capacity_sectors,
        capacity_bytes: capacity_sectors as u64 * SECTOR_SIZE as u64,
        layers: 1,
        titles: Vec::new(),
        region: DiscRegion::Free,
        aacs: None,
        css: None,
        encrypted: false,
        content_format: ContentFormat::BdTs,
    }
}

/// Pre-populate a mapfile with one large NonTrimmed range so patch's work-
/// list has something to do. Caller pre-allocates the ISO at `total_bytes`
/// so seeks don't fail.
fn prep_iso_and_mapfile(
    iso_path: &std::path::Path,
    total_bytes: u64,
    finished_ranges: &[(u64, u64)],
    nontrimmed_ranges: &[(u64, u64)],
) {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(iso_path)
        .unwrap();
    f.set_len(total_bytes).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&[]).unwrap();

    let map_path = libfreemkv::disc::mapfile_path_for(iso_path);
    let mut mf = Mapfile::create(&map_path, total_bytes, "test").unwrap();
    for &(pos, size) in finished_ranges {
        mf.record(pos, size, SectorStatus::Finished).unwrap();
    }
    for &(pos, size) in nontrimmed_ranges {
        mf.record(pos, size, SectorStatus::NonTrimmed).unwrap();
    }
}

/// THE critical test. A 100-sector "bad" range hides 50 good sectors in
/// the middle (LBAs 125-174). Pre-fix patch would skip-escalate at 32+
/// sectors and leap over the whole range. Post-fix: skip is capped at
/// range_remaining/4 (=25 sectors initially), which forces convergence.
#[test]
fn patch_recovers_good_middle_of_a_bad_range() {
    let capacity_sectors: u32 = 1024;
    let total_bytes: u64 = capacity_sectors as u64 * SECTOR_SIZE as u64;

    // Bad range layout: LBAs 100-124 bad, 125-174 GOOD, 175-199 bad.
    let mut bad_lbas = HashSet::new();
    for lba in 100..125 {
        bad_lbas.insert(lba);
    }
    for lba in 175..200 {
        bad_lbas.insert(lba);
    }

    let (mut reader, _trace) = PatternedSectorReader::new(capacity_sectors, bad_lbas);
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    // Pre-populate: 0..100 already Finished from an imagined Pass 1,
    //               100..200 NonTrimmed (the range we want patch to retry),
    //               200..1024 already Finished.
    let finished = [(0, 100 * 2048), (200 * 2048, (capacity_sectors as u64 - 200) * 2048)];
    let nontrimmed = [(100 * 2048, 100 * 2048)];
    prep_iso_and_mapfile(&iso_path, total_bytes, &finished, &nontrimmed);

    // Run patch.
    // disc.copy() with multipass=true auto-dispatches to patch when the
    // mapfile already covers the disc and has retryable ranges.
    let opts = CopyOptions {
        decrypt: false,
        multipass: true,
        ..Default::default()
    };
    let pr = disc.copy(&mut reader, &iso_path, &opts).expect("copy returns Ok");

    // Re-load mapfile and inspect.
    let map_path = libfreemkv::disc::mapfile_path_for(&iso_path);
    let map = Mapfile::load(&map_path).unwrap();

    // The good middle (125..175) MUST end up Finished. If size-aware skip
    // is not enabled, patch would skip 32+ sectors after a few failures
    // and leap clean over LBA 125 → middle stays NonTrimmed/Unreadable.
    let finished_ranges = map.ranges_with(&[SectorStatus::Finished]);
    let total_finished_in_middle: u64 = finished_ranges
        .iter()
        .map(|&(pos, sz)| {
            let start = pos.max(125 * 2048);
            let end = (pos + sz).min(175 * 2048);
            end.saturating_sub(start)
        })
        .sum();

    // Allow 2 sectors (4 KB) of boundary slop — patch's bisection may
    // not converge exactly on the good/bad boundary in a single pass,
    // and that's acceptable. The pre-fix behaviour would have left the
    // entire good middle as NonTrimmed (~0 bytes recovered).
    let good_middle_bytes: u64 = 50 * 2048;
    let min_acceptable: u64 = good_middle_bytes - 2 * 2048;

    // Cleanup before assertions
    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(&map_path);

    assert!(
        total_finished_in_middle >= min_acceptable,
        "size-aware skip should have discovered most of the 50 good sectors in the middle. \
         Recovered {} of {} good middle bytes (min acceptable {}). bytes_good={} bytes_total={}",
        total_finished_in_middle,
        good_middle_bytes,
        min_acceptable,
        pr.bytes_good,
        pr.bytes_total,
    );
}

/// A second test: a bad range that's actually 4 small bad sub-zones
/// separated by good sectors. Demonstrates the bisection behaviour
/// converges when zones are non-uniform.
#[test]
fn patch_recovers_multiple_good_middles() {
    let capacity_sectors: u32 = 2048;
    let total_bytes: u64 = capacity_sectors as u64 * SECTOR_SIZE as u64;

    // Bad pattern: 1000-1024 bad, 1025-1099 good, 1100-1124 bad,
    //              1125-1199 good, 1200-1224 bad, 1225-1299 good.
    let mut bad_lbas = HashSet::new();
    for lba in 1000..1025 {
        bad_lbas.insert(lba);
    }
    for lba in 1100..1125 {
        bad_lbas.insert(lba);
    }
    for lba in 1200..1225 {
        bad_lbas.insert(lba);
    }
    let (mut reader, _trace) = PatternedSectorReader::new(capacity_sectors, bad_lbas);
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let finished = [(0, 1000 * 2048), (1300 * 2048, (capacity_sectors as u64 - 1300) * 2048)];
    let nontrimmed = [(1000 * 2048, 300 * 2048)];
    prep_iso_and_mapfile(&iso_path, total_bytes, &finished, &nontrimmed);

    let opts = CopyOptions {
        decrypt: false,
        multipass: true,
        ..Default::default()
    };
    let pr = disc.copy(&mut reader, &iso_path, &opts).expect("copy returns Ok");

    let map_path = libfreemkv::disc::mapfile_path_for(&iso_path);
    let map = Mapfile::load(&map_path).unwrap();
    let finished_ranges = map.ranges_with(&[SectorStatus::Finished]);
    let recovered: u64 = finished_ranges
        .iter()
        .map(|&(pos, sz)| {
            let start = pos.max(1000 * 2048);
            let end = (pos + sz).min(1300 * 2048);
            end.saturating_sub(start)
        })
        .sum();

    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(&map_path);

    // Three good middles of 75 sectors each = 225 good sectors in the
    // bad range. Total bad = 75. So we want at least most of 225 sectors
    // (= 460800 bytes) to be Finished after patch.
    let target = 200 * 2048; // be generous — anything over 200 sectors is convincing
    assert!(
        recovered >= target,
        "size-aware skip should find most of the 3 good middles. \
         Recovered {} bytes; expected ≥ {}. bytes_good={} bytes_total={}",
        recovered,
        target,
        pr.bytes_good,
        pr.bytes_total,
    );
}
