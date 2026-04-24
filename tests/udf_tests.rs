//! UDF parser tests using a MockSectorReader.

use libfreemkv::error::Result;
use libfreemkv::{SectorReader, read_filesystem};
use std::collections::HashMap;

const SECTOR_SIZE: usize = 2048;

/// In-memory sector reader backed by a HashMap<LBA, sector_data>.
/// Any LBA not in the map returns zeroed sectors.
struct MockSectorReader {
    sectors: HashMap<u32, Vec<u8>>,
}

impl MockSectorReader {
    fn new() -> Self {
        Self {
            sectors: HashMap::new(),
        }
    }

    /// Write a full 2048-byte sector at the given LBA.
    fn set_sector(&mut self, lba: u32, data: Vec<u8>) {
        assert_eq!(
            data.len(),
            SECTOR_SIZE,
            "sector data must be exactly 2048 bytes"
        );
        self.sectors.insert(lba, data);
    }

    /// Write partial data into a sector (rest is zeroed).
    fn set_sector_partial(&mut self, lba: u32, data: &[u8]) {
        let mut sector = vec![0u8; SECTOR_SIZE];
        let len = data.len().min(SECTOR_SIZE);
        sector[..len].copy_from_slice(&data[..len]);
        self.sectors.insert(lba, sector);
    }
}

impl SectorReader for MockSectorReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let total = count as usize * SECTOR_SIZE;
        assert!(buf.len() >= total, "buffer too small");
        for i in 0..count as u32 {
            let offset = i as usize * SECTOR_SIZE;
            if let Some(data) = self.sectors.get(&(lba + i)) {
                buf[offset..offset + SECTOR_SIZE].copy_from_slice(data);
            } else {
                // Return zeros for unmapped sectors
                buf[offset..offset + SECTOR_SIZE].fill(0);
            }
        }
        Ok(total)
    }
}

// ── Helper: build raw sector data ──────────────────────────────────────────

/// Build an AVDP sector (tag_id=2) pointing to VDS at the given LBA.
fn make_avdp_sector(vds_lba: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    // Tag ID = 2 (AVDP) at bytes [0..2]
    s[0..2].copy_from_slice(&2u16.to_le_bytes());
    // Main VDS extent location at bytes [16..20]
    s[16..20].copy_from_slice(&vds_lba.to_le_bytes());
    // Main VDS extent length at bytes [20..24] (arbitrary, say 6 sectors)
    s[20..24].copy_from_slice(&(6u32 * SECTOR_SIZE as u32).to_le_bytes());
    s
}

/// Build a Primary Volume Descriptor (tag_id=1) with the given volume ID.
fn make_pvd_sector(volume_id: &str) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&1u16.to_le_bytes());
    // Volume ID at offset 24, d-string format:
    // compression_id(1 byte) + ASCII chars + length byte at position 55
    if !volume_id.is_empty() {
        let id_bytes = volume_id.as_bytes();
        s[24] = 8; // compression ID = ASCII
        let copy_len = id_bytes.len().min(30);
        s[25..25 + copy_len].copy_from_slice(&id_bytes[..copy_len]);
        // d-string length byte at end of 32-byte field (offset 55)
        s[55] = (1 + copy_len) as u8; // compression byte + chars
    }
    s
}

/// Build a Partition Descriptor (tag_id=5) with partition_start.
fn make_partition_desc(partition_start: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&5u16.to_le_bytes());
    // Partition start at bytes [188..192]
    s[188..192].copy_from_slice(&partition_start.to_le_bytes());
    s
}

/// Build a Logical Volume Descriptor (tag_id=6) with a single partition map
/// (no metadata partition -- simplest case).
fn make_lvd_sector_simple() -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&6u16.to_le_bytes());
    // num_partition_maps at bytes [268..272] = 1 (no metadata partition)
    s[268..272].copy_from_slice(&1u32.to_le_bytes());
    s
}

/// Build a Terminating Descriptor (tag_id=8).
fn make_terminator() -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&8u16.to_le_bytes());
    s
}

/// Build a File Set Descriptor (tag_id=256) with root ICB at the given meta LBA.
fn make_fsd_sector(root_meta_lba: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&256u16.to_le_bytes());
    // Root directory ICB long_ad at offset 400:
    //   [400..404] = extent_length (one sector = 2048)
    s[400..404].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
    //   [404..408] = extent_location (LBA within metadata partition)
    s[404..408].copy_from_slice(&root_meta_lba.to_le_bytes());
    s
}

/// Build an Extended File Entry (tag_id=266) for a directory with the given
/// allocation extent pointing to directory data.
fn make_dir_icb(data_meta_lba: u32, data_len: u32) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&266u16.to_le_bytes());
    // info_length at offset 56 (u64)
    s[56..64].copy_from_slice(&(data_len as u64).to_le_bytes());
    // L_EA at offset 208 = 0 (no extended attributes)
    s[208..212].copy_from_slice(&0u32.to_le_bytes());
    // L_AD at offset 212 = 8 (one short allocation descriptor)
    s[212..216].copy_from_slice(&8u32.to_le_bytes());
    // Short allocation descriptor at offset 216:
    //   [216..220] = extent_length (type=0 in top 2 bits, length in low 30 bits)
    s[216..220].copy_from_slice(&data_len.to_le_bytes());
    //   [220..224] = extent_position (LBA within metadata partition)
    s[220..224].copy_from_slice(&data_meta_lba.to_le_bytes());
    s
}

/// Build a File Identifier Descriptor (tag_id=257) for a named entry.
/// Returns raw bytes (not padded to full sector).
fn make_fid(name: &str, icb_meta_lba: u32, is_dir: bool) -> Vec<u8> {
    // FID name: compression_id(1) + ASCII bytes
    let mut name_bytes = vec![8u8]; // compression ID = 8 (ASCII)
    name_bytes.extend_from_slice(name.as_bytes());
    let l_fi = name_bytes.len() as u8;

    let file_chars: u8 = if is_dir { 0x02 } else { 0x00 };

    // Fixed header = 38 bytes, L_IU = 0
    let fid_len = (38 + l_fi as usize + 3) & !3; // 4-byte aligned
    let mut fid = vec![0u8; fid_len];
    // tag_id = 257
    fid[0..2].copy_from_slice(&257u16.to_le_bytes());
    // file_characteristics at offset 18
    fid[18] = file_chars;
    // L_FI at offset 19
    fid[19] = l_fi;
    // ICB long_ad at offset 20:
    //   [20..24] = extent_length (2048)
    fid[20..24].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
    //   [24..28] = extent_location (LBA)
    fid[24..28].copy_from_slice(&icb_meta_lba.to_le_bytes());
    // L_IU at offset 36 = 0
    fid[36..38].copy_from_slice(&0u16.to_le_bytes());
    // Name starts at offset 38
    fid[38..38 + name_bytes.len()].copy_from_slice(&name_bytes);
    fid
}

/// Build a parent FID (file_chars = 0x08, no name).
fn make_parent_fid() -> Vec<u8> {
    let fid_len = (38 + 3) & !3;
    let mut fid = vec![0u8; fid_len];
    fid[0..2].copy_from_slice(&257u16.to_le_bytes());
    fid[18] = 0x08; // parent
    fid[19] = 0; // L_FI = 0
    fid
}

/// Build a File Entry ICB (tag_id=261) for a file, with the given extent.
fn make_file_icb(data_lba: u32, data_len: u32, file_size: u64) -> Vec<u8> {
    let mut s = vec![0u8; SECTOR_SIZE];
    s[0..2].copy_from_slice(&261u16.to_le_bytes());
    // info_length at offset 56 (u64)
    s[56..64].copy_from_slice(&file_size.to_le_bytes());
    // L_EA at offset 168 = 0
    s[168..172].copy_from_slice(&0u32.to_le_bytes());
    // L_AD at offset 172 = 8
    s[172..176].copy_from_slice(&8u32.to_le_bytes());
    // Short allocation descriptor at offset 176:
    s[176..180].copy_from_slice(&data_len.to_le_bytes());
    s[180..184].copy_from_slice(&data_lba.to_le_bytes());
    s
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[test]
fn mock_sector_reader_roundtrip() {
    let mut reader = MockSectorReader::new();

    // Write a recognizable pattern to sector 100
    let mut data = vec![0u8; SECTOR_SIZE];
    data[0] = 0xAB;
    data[1] = 0xCD;
    data[2047] = 0xFF;
    reader.set_sector(100, data.clone());

    // Read it back
    let mut buf = vec![0u8; SECTOR_SIZE];
    let n = reader.read_sectors(100, 1, &mut buf, true).unwrap();
    assert_eq!(n, SECTOR_SIZE);
    assert_eq!(buf[0], 0xAB);
    assert_eq!(buf[1], 0xCD);
    assert_eq!(buf[2047], 0xFF);

    // Reading an unmapped sector returns zeros
    let mut buf2 = vec![0xFFu8; SECTOR_SIZE];
    let n2 = reader.read_sectors(999, 1, &mut buf2, true).unwrap();
    assert_eq!(n2, SECTOR_SIZE);
    assert_eq!(buf2[0], 0);
    assert_eq!(buf2[2047], 0);
}

#[test]
fn mock_sector_reader_multi_sector() {
    let mut reader = MockSectorReader::new();

    let mut s10 = vec![0u8; SECTOR_SIZE];
    s10[0] = 10;
    reader.set_sector(10, s10);

    let mut s11 = vec![0u8; SECTOR_SIZE];
    s11[0] = 11;
    reader.set_sector(11, s11);

    // Read 2 consecutive sectors
    let mut buf = vec![0u8; SECTOR_SIZE * 2];
    let n = reader.read_sectors(10, 2, &mut buf, true).unwrap();
    assert_eq!(n, SECTOR_SIZE * 2);
    assert_eq!(buf[0], 10);
    assert_eq!(buf[SECTOR_SIZE], 11);
}

#[test]
fn read_filesystem_no_avdp() {
    // Empty reader — sector 256 is all zeros, tag_id=0 != 2
    let mut reader = MockSectorReader::new();
    let result = read_filesystem(&mut reader);
    assert!(result.is_err(), "should fail when no AVDP at sector 256");
}

#[test]
fn read_filesystem_bad_avdp_tag() {
    // Put a sector at 256 with wrong tag_id
    let mut reader = MockSectorReader::new();
    let mut bad = vec![0u8; SECTOR_SIZE];
    bad[0..2].copy_from_slice(&99u16.to_le_bytes()); // tag_id=99, not 2
    reader.set_sector(256, bad);

    let result = read_filesystem(&mut reader);
    assert!(result.is_err(), "should fail when AVDP tag_id is not 2");
}

#[test]
fn read_filesystem_no_partition_descriptor() {
    // Valid AVDP but VDS has no Partition Descriptor (tag 5)
    let mut reader = MockSectorReader::new();
    reader.set_sector(256, make_avdp_sector(32));
    // Put a terminator immediately at sector 32
    reader.set_sector(32, make_terminator());

    let result = read_filesystem(&mut reader);
    assert!(
        result.is_err(),
        "should fail when no partition descriptor in VDS"
    );
}

#[test]
fn read_filesystem_bad_fsd_tag() {
    // Valid AVDP + VDS with partition desc + LVD, but FSD at metadata_start has wrong tag
    let mut reader = MockSectorReader::new();
    let partition_start = 512;

    reader.set_sector(256, make_avdp_sector(32));
    reader.set_sector(32, make_pvd_sector("TEST_DISC"));
    reader.set_sector(33, make_partition_desc(partition_start));
    reader.set_sector(34, make_lvd_sector_simple());
    reader.set_sector(35, make_terminator());
    // With 1 partition map, metadata_start = partition_start.
    // FSD should be at sector partition_start but we leave it as zeros (tag_id=0 != 256).

    let result = read_filesystem(&mut reader);
    assert!(result.is_err(), "should fail when FSD tag_id is not 256");
}

#[test]
fn read_filesystem_minimal_valid() {
    // Build a minimal valid UDF image: AVDP -> VDS -> FSD -> empty root dir
    let mut reader = MockSectorReader::new();
    let partition_start: u32 = 512;
    let root_icb_meta_lba: u32 = 1; // relative to metadata_start
    let root_data_meta_lba: u32 = 2;

    // Sector 256: AVDP
    reader.set_sector(256, make_avdp_sector(32));

    // VDS at sectors 32..35
    reader.set_sector(32, make_pvd_sector("MY_DISC"));
    reader.set_sector(33, make_partition_desc(partition_start));
    reader.set_sector(34, make_lvd_sector_simple());
    reader.set_sector(35, make_terminator());

    // FSD at partition_start (since single partition map, metadata_start = partition_start)
    reader.set_sector(partition_start, make_fsd_sector(root_icb_meta_lba));

    // Root directory ICB at metadata_start + root_icb_meta_lba
    // Points to directory data at root_data_meta_lba, length = 40 (one parent FID)
    let parent_fid = make_parent_fid();
    let dir_data_len = parent_fid.len() as u32;
    reader.set_sector(
        partition_start + root_icb_meta_lba,
        make_dir_icb(root_data_meta_lba, dir_data_len),
    );

    // Root directory data: just a parent FID (empty directory)
    reader.set_sector_partial(partition_start + root_data_meta_lba, &parent_fid);

    let fs = read_filesystem(&mut reader).expect("should parse minimal UDF");
    assert_eq!(fs.volume_id, "MY_DISC");
    assert!(fs.root.is_dir);
    assert!(fs.root.entries.is_empty(), "root should have no children");
}

#[test]
fn read_filesystem_with_subdirectory() {
    // Build a UDF image with root -> BDMV (dir) -> test.mpls (file)
    let mut reader = MockSectorReader::new();
    let partition_start: u32 = 512;

    // Layout (all relative to partition_start which equals metadata_start):
    //   meta LBA 0 = FSD
    //   meta LBA 1 = root ICB
    //   meta LBA 2 = root dir data
    //   meta LBA 3 = BDMV ICB
    //   meta LBA 4 = BDMV dir data
    //   meta LBA 5 = test.mpls file ICB
    //   meta LBA 10 = test.mpls file data (partition-relative)

    reader.set_sector(256, make_avdp_sector(32));
    reader.set_sector(32, make_pvd_sector("DISC_WITH_BDMV"));
    reader.set_sector(33, make_partition_desc(partition_start));
    reader.set_sector(34, make_lvd_sector_simple());
    reader.set_sector(35, make_terminator());

    // FSD -> root ICB at meta LBA 1
    reader.set_sector(partition_start, make_fsd_sector(1));

    // Root dir: parent FID + BDMV dir FID
    let parent_fid = make_parent_fid();
    let bdmv_fid = make_fid("BDMV", 3, true);
    let mut root_data = Vec::new();
    root_data.extend_from_slice(&parent_fid);
    root_data.extend_from_slice(&bdmv_fid);
    let root_data_len = root_data.len() as u32;

    reader.set_sector(partition_start + 1, make_dir_icb(2, root_data_len));
    reader.set_sector_partial(partition_start + 2, &root_data);

    // BDMV dir: parent FID + test.mpls file FID
    let bdmv_parent = make_parent_fid();
    let mpls_fid = make_fid("test.mpls", 5, false);
    let mut bdmv_data = Vec::new();
    bdmv_data.extend_from_slice(&bdmv_parent);
    bdmv_data.extend_from_slice(&mpls_fid);
    let bdmv_data_len = bdmv_data.len() as u32;

    reader.set_sector(partition_start + 3, make_dir_icb(4, bdmv_data_len));
    reader.set_sector_partial(partition_start + 4, &bdmv_data);

    // test.mpls file ICB (File Entry tag 261)
    reader.set_sector(partition_start + 5, make_file_icb(10, 1024, 1024));

    let fs = read_filesystem(&mut reader).expect("should parse UDF with subdir");
    assert_eq!(fs.volume_id, "DISC_WITH_BDMV");

    // Root should have one child: BDMV
    assert_eq!(fs.root.entries.len(), 1);
    let bdmv = &fs.root.entries[0];
    assert_eq!(bdmv.name, "BDMV");
    assert!(bdmv.is_dir);

    // BDMV should have one child: test.mpls
    assert_eq!(bdmv.entries.len(), 1);
    let mpls = &bdmv.entries[0];
    assert_eq!(mpls.name, "test.mpls");
    assert!(!mpls.is_dir);
    assert_eq!(mpls.size, 1024);
}

#[test]
fn find_dir_case_insensitive() {
    // Build a UDF image with BDMV/PLAYLIST directories, then search with various cases
    let mut reader = MockSectorReader::new();
    let partition_start: u32 = 512;

    reader.set_sector(256, make_avdp_sector(32));
    reader.set_sector(32, make_pvd_sector("CASE_TEST"));
    reader.set_sector(33, make_partition_desc(partition_start));
    reader.set_sector(34, make_lvd_sector_simple());
    reader.set_sector(35, make_terminator());

    // FSD
    reader.set_sector(partition_start, make_fsd_sector(1));

    // Root -> BDMV
    let root_data = [make_parent_fid(), make_fid("BDMV", 3, true)].concat();
    reader.set_sector(partition_start + 1, make_dir_icb(2, root_data.len() as u32));
    reader.set_sector_partial(partition_start + 2, &root_data);

    // BDMV -> PLAYLIST
    let bdmv_data = [make_parent_fid(), make_fid("PLAYLIST", 5, true)].concat();
    reader.set_sector(partition_start + 3, make_dir_icb(4, bdmv_data.len() as u32));
    reader.set_sector_partial(partition_start + 4, &bdmv_data);

    // PLAYLIST (empty)
    let playlist_data = make_parent_fid();
    reader.set_sector(
        partition_start + 5,
        make_dir_icb(6, playlist_data.len() as u32),
    );
    reader.set_sector_partial(partition_start + 6, &playlist_data);

    let fs = read_filesystem(&mut reader).expect("should parse");

    // Exact case
    assert!(fs.find_dir("BDMV/PLAYLIST").is_some());
    // Lower case
    assert!(fs.find_dir("bdmv/playlist").is_some());
    // Mixed case
    assert!(fs.find_dir("Bdmv/Playlist").is_some());
    assert!(fs.find_dir("BdMv/PlayList").is_some());
    // Leading/trailing slashes
    assert!(fs.find_dir("/BDMV/PLAYLIST/").is_some());
    // Nonexistent
    assert!(fs.find_dir("BDMV/STREAM").is_none());
    assert!(fs.find_dir("NONEXISTENT").is_none());
}

#[test]
fn sector_reader_is_object_safe() {
    // Verify SectorReader can be used as a trait object
    let mut reader = MockSectorReader::new();
    reader.set_sector(0, vec![42u8; SECTOR_SIZE]);

    let dyn_reader: &mut dyn SectorReader = &mut reader;
    let mut buf = vec![0u8; SECTOR_SIZE];
    let n = dyn_reader.read_sectors(0, 1, &mut buf, true).unwrap();
    assert_eq!(n, SECTOR_SIZE);
    assert_eq!(buf[0], 42);
}
