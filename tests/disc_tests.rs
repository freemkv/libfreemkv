//! Disc scanning pipeline tests.

use libfreemkv::error::Result;
use libfreemkv::sector::SectorReader;
use libfreemkv::{Disc, DiscTitle, ScanOptions};
use std::collections::HashMap;

const SECTOR_SIZE: usize = 2048;

/// Minimal mock sector reader for disc scan tests.
struct MockSectorReader {
    sectors: HashMap<u32, Vec<u8>>,
}

impl MockSectorReader {
    fn new() -> Self {
        Self {
            sectors: HashMap::new(),
        }
    }
}

impl SectorReader for MockSectorReader {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let total = count as usize * SECTOR_SIZE;
        for i in 0..count as u32 {
            let offset = i as usize * SECTOR_SIZE;
            if let Some(data) = self.sectors.get(&(lba + i)) {
                buf[offset..offset + SECTOR_SIZE].copy_from_slice(data);
            } else {
                buf[offset..offset + SECTOR_SIZE].fill(0);
            }
        }
        Ok(total)
    }
}

// ── scan_image tests ───────────────────────────────────────────────────────

#[test]
fn scan_image_empty_reader() {
    // An empty reader has no AVDP at sector 256 -> UDF parse fails
    let mut reader = MockSectorReader::new();
    let opts = ScanOptions::default();
    let result = Disc::scan_image(&mut reader, 0, &opts);
    assert!(
        result.is_err(),
        "scan_image should fail with empty reader (no AVDP)"
    );
}

// ── DiscTitle tests ────────────────────────────────────────────────────────

#[test]
fn disc_title_empty() {
    let t = DiscTitle::empty();
    assert_eq!(t.playlist, "");
    assert_eq!(t.playlist_id, 0);
    assert_eq!(t.duration_secs, 0.0);
    assert_eq!(t.size_bytes, 0);
    assert!(t.clips.is_empty());
    assert!(t.streams.is_empty());
    assert!(t.extents.is_empty());
}

#[test]
fn disc_title_duration_display() {
    let mut t = DiscTitle::empty();

    // 2 hours 15 minutes = 8100 seconds
    t.duration_secs = 8100.0;
    assert_eq!(t.duration_display(), "2h 15m");

    // 0 hours 5 minutes = 300 seconds
    t.duration_secs = 300.0;
    assert_eq!(t.duration_display(), "0h 05m");

    // Exact hour boundary
    t.duration_secs = 3600.0;
    assert_eq!(t.duration_display(), "1h 00m");

    // Large value: 10 hours 30 minutes
    t.duration_secs = 37800.0;
    assert_eq!(t.duration_display(), "10h 30m");
}

#[test]
fn disc_title_size_gb() {
    let mut t = DiscTitle::empty();

    // Exactly 1 GiB
    t.size_bytes = 1024 * 1024 * 1024;
    assert!((t.size_gb() - 1.0).abs() < 0.001);

    // 50 GiB (typical BD)
    t.size_bytes = 50 * 1024 * 1024 * 1024;
    assert!((t.size_gb() - 50.0).abs() < 0.001);

    // Zero
    t.size_bytes = 0;
    assert_eq!(t.size_gb(), 0.0);
}

#[test]
fn disc_title_total_sectors() {
    let mut t = DiscTitle::empty();
    assert_eq!(t.total_sectors(), 0);

    t.extents.push(libfreemkv::Extent {
        start_lba: 0,
        sector_count: 100,
    });
    t.extents.push(libfreemkv::Extent {
        start_lba: 200,
        sector_count: 50,
    });
    assert_eq!(t.total_sectors(), 150);
}

// ── ScanOptions tests ──────────────────────────────────────────────────────

#[test]
fn scan_options_default() {
    let opts = ScanOptions::default();
    assert!(opts.keydb_path.is_none());
}

#[test]
fn scan_options_with_keydb() {
    let opts = ScanOptions::with_keydb("/tmp/KEYDB.cfg");
    assert_eq!(
        opts.keydb_path.as_ref().unwrap().to_str().unwrap(),
        "/tmp/KEYDB.cfg"
    );
}

#[test]
fn scan_options_with_keydb_pathbuf() {
    let path = std::path::PathBuf::from("/home/user/.config/aacs/KEYDB.cfg");
    let opts = ScanOptions::with_keydb(path.clone());
    assert_eq!(opts.keydb_path.unwrap(), path);
}
