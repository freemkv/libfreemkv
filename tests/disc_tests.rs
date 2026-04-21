//! Disc scanning pipeline tests.

use libfreemkv::error::Result;
use libfreemkv::SectorReader;
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
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], _recovery: bool) -> Result<usize> {
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

// ── detect_format integration tests ───────────────────────────────────────

use libfreemkv::{
    Codec, ColorSpace, ContentFormat, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
};

fn title_with_video(
    codec: Codec,
    resolution: Resolution,
    content_format: ContentFormat,
) -> DiscTitle {
    DiscTitle {
        playlist: "00800.mpls".into(),
        playlist_id: 800,
        duration_secs: 7200.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec,
            resolution,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            secondary: false,
            label: String::new(),
        })],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format,
        codec_privates: Vec::new(),
    }
}

#[test]
fn disc_title_duration_display_edge_cases() {
    let mut t = DiscTitle::empty();

    // 0 seconds
    t.duration_secs = 0.0;
    assert_eq!(t.duration_display(), "0h 00m");

    // 1 second
    t.duration_secs = 1.0;
    assert_eq!(t.duration_display(), "0h 00m");

    // 59 minutes
    t.duration_secs = 59.0 * 60.0;
    assert_eq!(t.duration_display(), "0h 59m");

    // 24 hours exactly
    t.duration_secs = 24.0 * 3600.0;
    assert_eq!(t.duration_display(), "24h 00m");
}

#[test]
fn content_format_default_bdts() {
    let t = title_with_video(Codec::H264, Resolution::R1080p, ContentFormat::BdTs);
    assert_eq!(t.content_format, ContentFormat::BdTs);
}

#[test]
fn content_format_dvd_mpegps() {
    let t = title_with_video(Codec::Mpeg2, Resolution::R480i, ContentFormat::MpegPs);
    assert_eq!(t.content_format, ContentFormat::MpegPs);
}

// ── UDF helpers for encryption resolution tests ───────────────────────────

/// Build an AVDP sector (tag_id=2) pointing to VDS at the given LBA.
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

fn make_fid(name: &str, icb_meta_lba: u32, is_dir: bool) -> Vec<u8> {
    let mut name_bytes = vec![8u8];
    name_bytes.extend_from_slice(name.as_bytes());
    let l_fi = name_bytes.len() as u8;
    let file_chars: u8 = if is_dir { 0x02 } else { 0x00 };
    let fid_len = (38 + l_fi as usize + 3) & !3;
    let mut fid = vec![0u8; fid_len];
    fid[0..2].copy_from_slice(&257u16.to_le_bytes());
    fid[18] = file_chars;
    fid[19] = l_fi;
    fid[20..24].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
    fid[24..28].copy_from_slice(&icb_meta_lba.to_le_bytes());
    fid[36..38].copy_from_slice(&0u16.to_le_bytes());
    fid[38..38 + name_bytes.len()].copy_from_slice(&name_bytes);
    fid
}

/// Build a minimal UDF image with an empty root directory (no /AACS).
fn build_minimal_udf(reader: &mut MockSectorReader) {
    let partition_start: u32 = 512;
    reader.sectors.insert(256, make_avdp_sector(32));
    reader.sectors.insert(32, make_pvd_sector("TEST_DISC"));
    reader
        .sectors
        .insert(33, make_partition_desc(partition_start));
    reader.sectors.insert(34, make_lvd_sector_simple());
    reader.sectors.insert(35, make_terminator());

    reader.sectors.insert(partition_start, make_fsd_sector(1));

    let parent_fid = make_parent_fid();
    let dir_data_len = parent_fid.len() as u32;
    reader
        .sectors
        .insert(partition_start + 1, make_dir_icb(2, dir_data_len));
    let mut sector = vec![0u8; SECTOR_SIZE];
    sector[..parent_fid.len()].copy_from_slice(&parent_fid);
    reader.sectors.insert(partition_start + 2, sector);
}

/// Build a UDF image with an /AACS directory (empty).
fn build_udf_with_aacs_dir(reader: &mut MockSectorReader) {
    let partition_start: u32 = 512;
    reader.sectors.insert(256, make_avdp_sector(32));
    reader.sectors.insert(32, make_pvd_sector("ENCRYPTED_DISC"));
    reader
        .sectors
        .insert(33, make_partition_desc(partition_start));
    reader.sectors.insert(34, make_lvd_sector_simple());
    reader.sectors.insert(35, make_terminator());

    reader.sectors.insert(partition_start, make_fsd_sector(1));

    // Root -> AACS (dir)
    let parent_fid = make_parent_fid();
    let aacs_fid = make_fid("AACS", 3, true);
    let mut root_data = Vec::new();
    root_data.extend_from_slice(&parent_fid);
    root_data.extend_from_slice(&aacs_fid);
    let root_data_len = root_data.len() as u32;

    reader
        .sectors
        .insert(partition_start + 1, make_dir_icb(2, root_data_len));
    let mut sector = vec![0u8; SECTOR_SIZE];
    sector[..root_data.len()].copy_from_slice(&root_data);
    reader.sectors.insert(partition_start + 2, sector);

    // AACS dir (empty)
    let aacs_parent = make_parent_fid();
    let aacs_data_len = aacs_parent.len() as u32;
    reader
        .sectors
        .insert(partition_start + 3, make_dir_icb(4, aacs_data_len));
    let mut sector2 = vec![0u8; SECTOR_SIZE];
    sector2[..aacs_parent.len()].copy_from_slice(&aacs_parent);
    reader.sectors.insert(partition_start + 4, sector2);
}

#[test]
fn resolve_encryption_no_aacs_dir() {
    // A UDF image with no /AACS directory should result in no encryption
    let mut reader = MockSectorReader::new();
    build_minimal_udf(&mut reader);

    let opts = ScanOptions::default();
    let disc = Disc::scan_image(&mut reader, 1000, &opts).unwrap();

    assert!(
        !disc.encrypted,
        "disc without /AACS should not be encrypted"
    );
    assert!(disc.aacs.is_none(), "aacs should be None without /AACS dir");
}

// ── Batch count arithmetic tests ──────────────────────────────────────────
// Regression tests for the u16 truncation bug: when (remaining as u16) was
// used instead of remaining.min(batch as u32) as u16, any remaining count
// that was a multiple of 65536 would truncate to 0, causing an infinite loop.

/// Simulates the fixed batch count calculation from pipe.rs / drive.rs
fn safe_batch_count(remaining: u32, batch_sectors: u16) -> u16 {
    remaining.min(batch_sectors as u32) as u16
}

/// Simulates the BUGGY calculation that caused the infinite loop
fn buggy_batch_count(remaining: u32, batch_sectors: u16) -> u16 {
    (remaining as u16).min(batch_sectors)
}

#[test]
fn batch_count_normal() {
    // Normal case: remaining > batch_sectors
    assert_eq!(safe_batch_count(1000, 60), 60);
    assert_eq!(safe_batch_count(47533152, 60), 60);
}

#[test]
fn batch_count_last_batch() {
    // Last batch: remaining < batch_sectors
    assert_eq!(safe_batch_count(30, 60), 30);
    assert_eq!(safe_batch_count(1, 60), 1);
}

#[test]
fn batch_count_exact_boundary() {
    // Exact boundary: remaining == batch_sectors
    assert_eq!(safe_batch_count(60, 60), 60);
}

#[test]
fn batch_count_u16_overflow_regression() {
    // THE BUG: remaining is a multiple of 65536 → truncates to 0
    // 47513600 = 725 * 65536, lower 16 bits = 0
    let remaining: u32 = 47533152 - 19552; // = 47513600
    assert_eq!(remaining, 47513600);
    assert_eq!(
        remaining % 65536,
        0,
        "remaining should be multiple of 65536"
    );

    // Buggy version produces 0 → infinite loop
    assert_eq!(buggy_batch_count(remaining, 60), 0);

    // Fixed version produces 60
    assert_eq!(safe_batch_count(remaining, 60), 60);
}

#[test]
fn batch_count_other_u16_overflow_values() {
    // Other multiples of 65536
    assert_eq!(safe_batch_count(65536, 60), 60);
    assert_eq!(safe_batch_count(131072, 60), 60);
    assert_eq!(safe_batch_count(65536 * 100, 60), 60);

    // Verify buggy version fails on all of these
    assert_eq!(buggy_batch_count(65536, 60), 0);
    assert_eq!(buggy_batch_count(131072, 60), 0);
    assert_eq!(buggy_batch_count(65536 * 100, 60), 0);
}

#[test]
fn batch_count_near_u16_boundary() {
    // Values just below and above 65536
    assert_eq!(safe_batch_count(65535, 60), 60);
    assert_eq!(safe_batch_count(65536, 60), 60);
    assert_eq!(safe_batch_count(65537, 60), 60);

    // Buggy: 65535 as u16 = 65535, min(60) = 60 (OK by accident)
    assert_eq!(buggy_batch_count(65535, 60), 60);
    // Buggy: 65536 as u16 = 0, min(60) = 0 (BUG)
    assert_eq!(buggy_batch_count(65536, 60), 0);
    // Buggy: 65537 as u16 = 1, min(60) = 1 (wrong but doesn't loop)
    assert_eq!(buggy_batch_count(65537, 60), 1);
}

#[test]
fn batch_count_real_disc_sizes() {
    let batch: u16 = 60;

    // DVD-5: ~2,295,104 sectors
    assert_eq!(safe_batch_count(2295104, batch), 60);

    // BD-25: ~12,219,392 sectors
    assert_eq!(safe_batch_count(12219392, batch), 60);

    // BD-50: ~24,438,784 sectors
    assert_eq!(safe_batch_count(24438784, batch), 60);

    // UHD BD-66: ~33,554,432 sectors
    assert_eq!(safe_batch_count(33554432, batch), 60);

    // UHD BD-100: ~47,533,152 sectors
    assert_eq!(safe_batch_count(47533152, batch), 60);

    // Last few sectors of each
    assert_eq!(safe_batch_count(52, batch), 52);
    assert_eq!(safe_batch_count(3, batch), 3);
}

#[test]
fn batch_count_zero_remaining() {
    // Zero remaining should produce 0 (loop exits before this)
    assert_eq!(safe_batch_count(0, 60), 0);
}

#[test]
fn batch_count_max_batch_sizes() {
    // Test with different batch sizes used by detect_max_batch_sectors
    for &batch in &[3u16, 6, 9, 30, 60, 120, 240, 510] {
        // Large remaining should always return batch
        assert_eq!(safe_batch_count(47533152, batch), batch);
        // Small remaining should return remaining
        assert_eq!(safe_batch_count(1, batch), 1);
    }
}

#[test]
fn resolve_encryption_no_keydb() {
    // A UDF image with /AACS directory but no keydb path -> aacs is None
    let mut reader = MockSectorReader::new();
    build_udf_with_aacs_dir(&mut reader);

    // No keydb configured and no standard keydb on the system
    let opts = ScanOptions::with_keydb("/nonexistent/path/KEYDB.cfg");
    let disc = Disc::scan_image(&mut reader, 1000, &opts).unwrap();

    // The disc detects encryption but can't resolve keys without a keydb
    assert!(
        disc.aacs.is_none(),
        "aacs should be None when keydb is unavailable"
    );
}
