//! Tests for the `libfreemkv::scan_iso` entry point — the file-backed scan seam
//! that replaced consumers hand-rolling `FileSectorSource::open` +
//! `capacity_sectors` + `Disc::scan_image`.
//!
//! Uses a minimal synthetic UDF image (the same byte-level fixture the
//! `disc_tests.rs` `scan_image` tests build, but materialised to a real file on
//! disk so the file-backed `FileSectorSource` path is exercised end to end).

use libfreemkv::{Disc, ScanOptions, SectorSource};
use std::collections::BTreeMap;
use std::io::Write;

const SECTOR_SIZE: usize = 2048;

// ── Minimal UDF sector builders (mirrors disc_tests.rs) ─────────────────────

fn make_avdp_sector(vds_lba: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&2u16.to_le_bytes());
    s[16..20].copy_from_slice(&vds_lba.to_le_bytes());
    s[20..24].copy_from_slice(&(6u32 * SECTOR_SIZE as u32).to_le_bytes());
    s
}

fn make_pvd_sector(volume_id: &str) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&1u16.to_le_bytes());
    if !volume_id.is_empty() {
        let id_bytes = volume_id.as_bytes();
        s[24] = 8;
        let copy_len = id_bytes.len().min(30);
        s[25..25 + copy_len].copy_from_slice(&id_bytes[..copy_len]);
        s[55] = (1 + copy_len) as u8;
    }
    s
}

fn make_partition_desc(partition_start: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&5u16.to_le_bytes());
    s[188..192].copy_from_slice(&partition_start.to_le_bytes());
    s
}

fn make_lvd_sector_simple() -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&6u16.to_le_bytes());
    s[268..272].copy_from_slice(&1u32.to_le_bytes());
    s
}

fn make_terminator() -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&8u16.to_le_bytes());
    s
}

fn make_fsd_sector(root_meta_lba: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&256u16.to_le_bytes());
    s[400..404].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
    s[404..408].copy_from_slice(&root_meta_lba.to_le_bytes());
    s
}

fn make_dir_icb(data_meta_lba: u32, data_len: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&266u16.to_le_bytes());
    s[56..64].copy_from_slice(&(data_len as u64).to_le_bytes());
    s[208..212].copy_from_slice(&0u32.to_le_bytes());
    s[212..216].copy_from_slice(&8u32.to_le_bytes());
    s[216..220].copy_from_slice(&data_len.to_le_bytes());
    s[220..224].copy_from_slice(&data_meta_lba.to_le_bytes());
    s
}

fn make_parent_fid() -> Vec<u8> {
    let fid_len = (38 + 3) & !3;
    let mut fid = vec![0u8; fid_len];
    fid[0..2].copy_from_slice(&257u16.to_le_bytes());
    fid[18] = 0x08;
    fid[19] = 0;
    fid
}

/// Build the minimal UDF image as an LBA→sector map (empty root directory).
fn minimal_udf_sectors() -> BTreeMap<u32, Vec<u8>> {
    let partition_start: u32 = 512;
    let mut sectors: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    sectors.insert(256, make_avdp_sector(32));
    sectors.insert(32, make_pvd_sector("TEST_DISC"));
    sectors.insert(33, make_partition_desc(partition_start));
    sectors.insert(34, make_lvd_sector_simple());
    sectors.insert(35, make_terminator());
    sectors.insert(partition_start, make_fsd_sector(1));

    let parent_fid = make_parent_fid();
    let dir_data_len = parent_fid.len() as u32;
    sectors.insert(partition_start + 1, make_dir_icb(2, dir_data_len));
    let mut sector = vec![0u8; SECTOR_SIZE];
    sector[..parent_fid.len()].copy_from_slice(&parent_fid);
    sectors.insert(partition_start + 2, sector);
    sectors
}

/// Materialise an LBA→sector map to a real ISO file (zero-filled gaps) and
/// return its path (kept alive by the returned tempfile handle).
fn write_iso(sectors: &BTreeMap<u32, Vec<u8>>) -> tempfile::NamedTempFile {
    let max_lba = *sectors.keys().max().unwrap();
    let mut image = vec![0u8; (max_lba as usize + 1) * SECTOR_SIZE];
    for (&lba, data) in sectors {
        let off = lba as usize * SECTOR_SIZE;
        image[off..off + SECTOR_SIZE].copy_from_slice(data);
    }
    let mut tmp = tempfile::Builder::new()
        .suffix(".iso")
        .tempfile()
        .expect("tempfile create");
    tmp.write_all(&image).expect("write iso");
    tmp.flush().expect("flush iso");
    tmp
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn scan_iso_matches_manual_scan_image_path() {
    let sectors = minimal_udf_sectors();
    let expected_capacity = *sectors.keys().max().unwrap() + 1;
    let tmp = write_iso(&sectors);

    // The new entry point.
    let (disc, mut reader) =
        libfreemkv::scan_iso(tmp.path(), ScanOptions::default()).expect("scan_iso succeeds");

    // Parity with the old hand-rolled triple: open a fresh reader and run the
    // exact composition scan_iso encapsulates. The resulting Disc must match.
    let mut manual_reader = libfreemkv::FileSectorSource::open(tmp.path()).expect("manual open");
    let manual_capacity = manual_reader.capacity_sectors();
    let manual = Disc::scan_image(&mut manual_reader, manual_capacity, &ScanOptions::default())
        .expect("manual scan_image succeeds");

    assert_eq!(disc.capacity_sectors, manual.capacity_sectors, "capacity");
    assert_eq!(disc.titles.len(), manual.titles.len(), "title count");
    assert_eq!(disc.encrypted, manual.encrypted, "encrypted flag");
    assert_eq!(disc.format, manual.format, "disc format");

    // Independent expectations, not parity against a re-run: the scanned Disc
    // must match KNOWN fixture properties (capacity, PVD volume id, unencrypted
    // with no /AACS) — these would fail even if both scan paths drifted together.
    assert_eq!(disc.capacity_sectors, expected_capacity, "capacity value");
    assert_eq!(
        disc.volume_id, "TEST_DISC",
        "PVD volume id from the fixture"
    );
    assert!(!disc.encrypted, "minimal UDF (no /AACS) is not encrypted");

    // The returned reader is usable: correct capacity and a real read of sector
    // 256 (the AVDP) returns the bytes we wrote — proves it is not consumed /
    // exhausted by the scan.
    assert_eq!(
        reader.capacity_sectors(),
        expected_capacity,
        "reader capacity"
    );
    let mut buf = vec![0u8; SECTOR_SIZE];
    let n = reader
        .read_sectors(256, 1, &mut buf, false)
        .expect("read AVDP sector");
    assert_eq!(n, SECTOR_SIZE);
    assert_eq!(&buf[..], &sectors[&256][..], "AVDP sector bytes round-trip");
}

#[test]
fn scan_iso_propagates_open_error() {
    // A path that does not exist must surface an Err (not a panic) — kills a
    // mutant that ignores the open failure.
    let missing = std::path::Path::new("/nonexistent/does-not-exist.iso");
    let result = libfreemkv::scan_iso(missing, ScanOptions::default());
    assert!(result.is_err(), "missing file must error");
}

#[test]
fn scan_iso_propagates_scan_error() {
    // A readable file with no valid UDF (no AVDP at sector 256) must surface the
    // scan failure — kills a mutant that swallows the scan_image error.
    let mut tmp = tempfile::Builder::new()
        .suffix(".iso")
        .tempfile()
        .expect("tempfile create");
    tmp.write_all(&vec![0u8; 8 * SECTOR_SIZE]).expect("write");
    tmp.flush().expect("flush");
    let result = libfreemkv::scan_iso(tmp.path(), ScanOptions::default());
    assert!(result.is_err(), "non-UDF image must error");
}
