use super::fixture::{MemDisc, PART_START, build_udf_skeleton};
use super::*;
use std::collections::HashMap;

// Serves a generated File Entry for every LBA (`ads` short ADs at LBAs derived from the
// ICB's own LBA, so distinct ICBs never overlap); `over` replaces chosen sectors.
struct GenReader {
    ads: usize,
    reads: HashMap<u32, usize>,
    over: HashMap<u32, [u8; 2048]>,
    halt_from: Option<u32>,
}

impl GenReader {
    fn new(ads: usize) -> Self {
        Self {
            ads,
            reads: HashMap::new(),
            over: HashMap::new(),
            halt_from: None,
        }
    }
}

fn efe(l_ad: usize) -> [u8; 2048] {
    let mut s = [0u8; 2048];
    s[0..2].copy_from_slice(&266u16.to_le_bytes());
    s[212..216].copy_from_slice(&(l_ad as u32).to_le_bytes());
    s
}

fn put_ad(s: &mut [u8; 2048], i: usize, len: u32, lba: u32) {
    let o = 216 + i * 8;
    s[o..o + 4].copy_from_slice(&len.to_le_bytes());
    s[o + 4..o + 8].copy_from_slice(&lba.to_le_bytes());
}

impl SectorSource for GenReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        if self.halt_from.is_some_and(|h| lba >= h) {
            return Err(Error::Halted);
        }
        for i in 0..count as u32 {
            *self.reads.entry(lba + i).or_default() += 1;
            let s = match self.over.get(&(lba + i)) {
                Some(s) => *s,
                None => {
                    let mut s = efe(self.ads * 8);
                    for k in 0..self.ads {
                        put_ad(&mut s, k, 2048, (lba + i) * 1000 + k as u32 * 2);
                    }
                    s
                }
            };
            let off = i as usize * 2048;
            buf[off..off + 2048].copy_from_slice(&s);
        }
        Ok(count as usize * 2048)
    }
}

fn file(name: &str, meta_lba: u32) -> DirEntry {
    DirEntry {
        name: name.to_string(),
        is_dir: false,
        meta_lba,
        size: 2048,
        entries: Vec::new(),
    }
}

fn fs_of(meta_start: u32, metadata_sectors: u32, entries: Vec<DirEntry>) -> UdfFs {
    UdfFs {
        root: DirEntry {
            name: String::new(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries,
        },
        volume_id: String::new(),
        partition_start: 0,
        meta: MetaMap::contiguous(meta_start),
        metadata_sectors,
    }
}

#[test]
fn metadata_ranges_clamp_a_hostile_metadata_size() {
    let fs = fs_of(1_000_000, 500_000, Vec::new());
    let ranges = fs
        .metadata_sector_ranges(&mut MemDisc::new())
        .expect("ranges");
    assert!(
        ranges.iter().all(|r| r.1 <= MAX_STRUCT_SECTORS),
        "no range may exceed the structure cap: {ranges:?}"
    );
}

#[test]
fn range_walks_fail_instead_of_collecting_unbounded_extents() {
    let files: Vec<DirEntry> = (1..=400).map(|i| file(&format!("F{i}"), i)).collect();
    let fs = fs_of(0, 1, files);
    let a = fs.metadata_sector_ranges(&mut GenReader::new(228));
    assert!(a.is_err(), "collect_file_ranges must cap its list");
    let b = fs.non_stream_ranges(&mut GenReader::new(228));
    assert!(b.is_err(), "non_stream_ranges must cap its list");
}

#[test]
fn range_walks_read_a_shared_icb_once() {
    let files: Vec<DirEntry> = (0..50).map(|i| file(&format!("F{i}"), 5)).collect();
    let fs = fs_of(0, 1, files);
    let mut r = GenReader::new(3);
    fs.metadata_sector_ranges(&mut r).expect("ranges");
    assert_eq!(r.reads[&5], 1, "shared ICB read once (collect_file_ranges)");
    let mut r = GenReader::new(3);
    fs.non_stream_ranges(&mut r).expect("ranges");
    assert_eq!(r.reads[&5], 1, "shared ICB read once (non_stream_ranges)");
}

// A File Entry that fails to read must fail the staging plan (else the staged image
// silently lacks that file's data); an embedded-data entry is skipped, its data
// already being in the staged File Entry.
#[test]
fn non_stream_ranges_fail_on_an_unread_entry_but_skip_embedded_data() {
    struct FailAt(GenReader, u32);
    impl SectorSource for FailAt {
        fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            if lba == self.1 {
                return Err(Error::DiscRead {
                    sector: lba as u64,
                    status: None,
                    sense: None,
                });
            }
            self.0.read_sectors(lba, n, buf, r)
        }
    }
    let fs = fs_of(0, 1, vec![file("A", 5), file("B", 6)]);
    let err = fs.non_stream_ranges(&mut FailAt(GenReader::new(1), 6));
    assert!(
        matches!(err, Err(Error::DiscRead { sector: 6, .. })),
        "{err:?}"
    );

    let mut r = GenReader::new(1);
    let mut embedded = efe(0);
    embedded[34..36].copy_from_slice(&3u16.to_le_bytes());
    r.over.insert(6, embedded);
    fs.non_stream_ranges(&mut r)
        .expect("embedded data is not an error");
}

#[test]
fn metadata_ranges_propagate_a_stop() {
    let fs = fs_of(0, 1, vec![file("F", 7)]);
    let mut r = GenReader::new(1);
    r.halt_from = Some(1);
    assert!(matches!(
        fs.metadata_sector_ranges(&mut r),
        Err(Error::Halted)
    ));
}

#[test]
fn file_extents_refuse_an_extent_that_ends_past_the_lba_space() {
    let fs = fs_of(0, 1, vec![file("F", 7)]);
    let mut r = GenReader::new(0);
    let mut e = efe(8);
    put_ad(&mut e, 0, 4096, u32::MAX);
    r.over.insert(7, e);
    assert!(fs.file_extents(&mut r, "/F").is_err());
    assert!(fs.file_extents_addressing(&mut r, "/F").is_err());
}

#[test]
fn vds_fault_after_the_partition_descriptor_is_not_a_verdict() {
    struct Fault(MemDisc);
    impl SectorSource for Fault {
        fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
            if lba == 33 {
                return Err(Error::DiscRead {
                    sector: 33,
                    status: None,
                    sense: None,
                });
            }
            self.0.read_sectors(lba, count, buf, r)
        }
    }
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    // Block 0 of the partition is a File Entry, as on a metadata-partition disc.
    disc.put_bytes(PART_START, &266u16.to_le_bytes());
    let err = read_filesystem(&mut Fault(disc)).expect_err("fault must surface");
    assert!(matches!(err, Error::DiscRead { .. }), "{err:?}");
}

struct FaultAt(MemDisc, u32);
impl SectorSource for FaultAt {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        if lba == self.1 {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        self.0.read_sectors(lba, count, buf, r)
    }
}

#[test]
fn single_partition_disc_with_an_unreadable_lvd_still_mounts() {
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    fixture::lay_dir(
        &mut disc,
        &fixture::DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: Vec::new(),
        },
    );
    let fs = read_filesystem(&mut FaultAt(disc, 33)).expect("must still mount");
    assert_eq!(fs.partition_start, PART_START);
}

// Corrupt (non-FID) tag inside a subdirectory: that dir lists empty, the scan survives.
#[test]
fn a_corrupt_fid_tag_in_a_subdirectory_lists_it_empty() {
    let mut r = GenReader::new(0);
    let mut root = efe(8);
    let mut f = vec![0u8; 44];
    f[0..2].copy_from_slice(&257u16.to_le_bytes());
    f[18] = 0x02;
    f[19] = 4;
    f[24..28].copy_from_slice(&1000u32.to_le_bytes());
    f[38..42].copy_from_slice(&[8, b'D', b'A', b'B']);
    put_ad(&mut root, 0, f.len() as u32, 6);
    r.over.insert(5, root);
    let mut s = [0u8; 2048];
    s[..f.len()].copy_from_slice(&f);
    r.over.insert(6, s);
    let mut d = efe(8);
    put_ad(&mut d, 0, 64, 7);
    r.over.insert(1000, d);
    let mut bad = [0u8; 2048];
    bad[0..2].copy_from_slice(&266u16.to_le_bytes());
    r.over.insert(7, bad);
    let root = read_directory(
        &mut r,
        &mut 0,
        &MetaMap::contiguous(0),
        5,
        "",
        0,
        &mut 0,
        &mut HashSet::new(),
    )
    .expect("subdirectory corruption must not fail the walk");
    assert_eq!(root.entries.len(), 1);
    assert!(root.entries[0].entries.is_empty());
}

#[test]
fn directory_walk_caps_sectors_read_across_subdirectories() {
    // Root lists 100 subdirectories; each declares a 1 MiB extent of no FIDs.
    let mut fids = Vec::new();
    for k in 0..100u32 {
        let mut f = vec![0u8; 44];
        f[0..2].copy_from_slice(&257u16.to_le_bytes());
        f[18] = 0x02;
        f[19] = 4;
        f[24..28].copy_from_slice(&(1000 + k).to_le_bytes());
        f[38..42].copy_from_slice(&[8, b'D', b'0' + (k / 10) as u8, b'0' + (k % 10) as u8]);
        fids.extend_from_slice(&f);
    }
    let mut r = GenReader::new(0);
    let mut root = efe(8);
    put_ad(&mut root, 0, fids.len() as u32, 6);
    r.over.insert(5, root);
    for (i, c) in fids.chunks(2048).enumerate() {
        let mut s = [0u8; 2048];
        s[..c.len()].copy_from_slice(c);
        r.over.insert(6 + i as u32, s);
    }
    for k in 0..100u32 {
        let mut d = efe(8);
        put_ad(&mut d, 0, MAX_DIR_BYTES, 100_000);
        r.over.insert(1000 + k, d);
    }
    // Zeroed data: each subdirectory lists empty, so only the tree-wide budget can trip.
    for s in 100_000..100_000 + MAX_DIR_BYTES / 2048 {
        r.over.insert(s, [0u8; 2048]);
    }
    let res = read_directory(
        &mut r,
        &mut 0,
        &MetaMap::contiguous(0),
        5,
        "",
        0,
        &mut 0,
        &mut HashSet::new(),
    );
    assert!(res.is_err(), "tree-wide sector budget must trip");
    let data_reads: usize = (100_000..100_000 + MAX_DIR_BYTES / 2048)
        .map(|s| r.reads.get(&s).copied().unwrap_or(0))
        .sum();
    assert!(
        data_reads <= (MAX_TOTAL_DIR_SECTORS + MAX_DIR_BYTES / 2048) as usize,
        "sectors read must stay near the cap: {data_reads}"
    );
}

// Marks sector data by which entry point served it; forwards the trait's extras.
struct Probe {
    unmapped: Vec<crate::sector::bus_removal::UnmappedStreamFile>,
}

impl SectorSource for Probe {
    fn read_sectors(&mut self, _: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        buf[..count as usize * 2048].fill(0x11);
        Ok(count as usize * 2048)
    }
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        if fua {
            buf[..count as usize * 2048].fill(0x22);
            return Ok(count as usize * 2048);
        }
        self.read_sectors(lba, count, buf, recovery)
    }
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        &self.unmapped
    }
    fn random_access(&self) -> bool {
        false
    }
}

#[test]
fn buffered_reader_sends_a_fua_read_to_the_medium_and_forwards_the_source_traits() {
    let mut inner = Probe {
        unmapped: vec![crate::sector::bus_removal::UnmappedStreamFile::new(
            "/BDMV/STREAM/00001.m2ts".into(),
            7,
            &Error::Halted,
        )],
    };
    let mut br = BufferedSectorReader::new(&mut inner, 8);
    let mut buf = [0u8; 2048];
    br.read_sectors(100, 1, &mut buf, true).unwrap();
    assert_eq!(buf[0], 0x11);
    br.read_sectors_fua(100, 1, &mut buf, true, true).unwrap();
    assert_eq!(
        buf[0], 0x22,
        "a FUA read must not be answered from the cache"
    );
    assert!(!br.random_access());
    assert_eq!(br.unmapped_stream_files().len(), 1);
}

#[test]
fn non_stream_ranges_cover_structures_and_non_stream_data_only() {
    let mut r = GenReader::new(0);
    let mut a = efe(16);
    put_ad(&mut a, 0, 2048, 20);
    put_ad(&mut a, 1, 2048 | (1 << 30), 700);
    r.over.insert(300, a);
    let mut s = efe(8);
    put_ad(&mut s, 0, 10 * 2048, 9000);
    r.over.insert(103, s);
    let dir = |name: &str, meta_lba, entries| DirEntry {
        name: name.into(),
        is_dir: true,
        meta_lba,
        size: 0,
        entries,
    };
    let stream = dir("STREAM", 2, vec![file("00000.m2ts", 3)]);
    let bdmv = dir("BDMV", 1, vec![stream, file("a.bin", 4)]);
    let mut fs = fs_of(100, 8, vec![bdmv]);
    fs.meta = MetaMap(vec![(100, 4), (300, 4)]);
    fs.partition_start = 50;
    assert_eq!(
        fs.non_stream_ranges(&mut r).expect("ranges"),
        vec![(0, 50), (70, 1), (100, 8), (300, 4)]
    );
}

#[test]
fn utf16_cs0_combines_surrogate_pairs() {
    // U+1F600 is D83D DE00.
    assert_eq!(
        parse_udf_name(&[16, 0, b'A', 0xD8, 0x3D, 0xDE, 0x00]),
        "A\u{1F600}"
    );
}

#[test]
fn cs0_names_carry_no_control_characters() {
    assert_eq!(
        parse_udf_name(&[8, b'a', 0x1B, b'[', b'2', b'J', b'b']),
        "a[2Jb"
    );
}

#[test]
fn read_file_fails_when_extents_cover_less_than_the_declared_size() {
    let mut r = GenReader::new(0);
    let mut icb = efe(8);
    put_ad(&mut icb, 0, 2048, 10);
    r.over.insert(5, icb);
    let mut f = file("F", 5);
    f.size = 8192;
    let fs = fs_of(0, 1, vec![f]);
    assert!(matches!(
        fs.read_file(&mut r, "/F"),
        Err(Error::DiscRead { .. })
    ));
}

#[test]
fn non_stream_ranges_map_directory_data_through_the_metadata_partition() {
    let mut r = GenReader::new(0);
    let mut icb = efe(8);
    put_ad(&mut icb, 0, 2048, 3);
    r.over.insert(105, icb);
    let mut dir = file("D", 5);
    dir.is_dir = true;
    let mut fs = fs_of(100, 1, vec![dir]);
    fs.partition_start = 5000;
    let ranges = fs.non_stream_ranges(&mut r).expect("ranges");
    let covers = |lba: u32| ranges.iter().any(|&(s, n)| (s..s + n).contains(&lba));
    assert!(
        covers(103),
        "directory data sits at metadata block 3: {ranges:?}"
    );
    assert!(!covers(5003), "not at partition_start + 3: {ranges:?}");
}

#[test]
fn eight_bit_cs0_is_latin1() {
    assert_eq!(parse_udf_name(&[8, 0xE9, b'A']), "\u{e9}A");
    let mut field = [0u8; 32];
    field[..3].copy_from_slice(&[8, 0xC4, 0]);
    field[31] = 3;
    assert_eq!(parse_dstring(&field), "\u{c4}");
}
