use super::*;
use crate::udf::fixture::{
    DirSpec, MemDisc, PART_START, build_file_icb_ads, build_udf_skeleton, file, lay_dir,
};

struct CountingDisc {
    inner: MemDisc,
    reads: Vec<u32>,
}

impl SectorSource for CountingDisc {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.reads.extend(lba..lba + u32::from(count));
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

fn parsed(files: Vec<crate::udf::fixture::FileSpec>) -> (UdfFs, CountingDisc) {
    parsed_with(files, |_| {})
}

fn parsed_with(
    files: Vec<crate::udf::fixture::FileSpec>,
    prepare: impl FnOnce(&mut CountingDisc),
) -> (UdfFs, CountingDisc) {
    let mut inner = MemDisc::new();
    build_udf_skeleton(&mut inner, 10);
    lay_dir(
        &mut inner,
        &DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 20,
            files,
            subdirs: vec![],
        },
    );
    let mut disc = CountingDisc {
        inner,
        reads: vec![],
    };
    prepare(&mut disc);
    let fs = crate::udf::read_filesystem(&mut disc).expect("parse UDF tree");
    disc.reads.clear();
    (fs, disc)
}

// A valid terminating two-block short-AD chain, not a chain-limit error.
fn long_chain(disc: &mut CountingDisc, icb: u32) {
    let ads: Vec<_> = (0..200).map(|i| (10_000 + i * 2, 2048)).collect();
    let mut first = build_file_icb_ads(&ads, false);
    first[56..64].copy_from_slice(&(300u64 * 2048).to_le_bytes());
    first[212..216].copy_from_slice(&(201u32 * 8).to_le_bytes());
    first[1816..1820].copy_from_slice(&((3u32 << 30) | 2048).to_le_bytes());
    first[1820..1824].copy_from_slice(&9000u32.to_le_bytes());
    disc.inner.put_bytes(PART_START + icb, &first);
    let mut aed = [0u8; 2048];
    aed[..2].copy_from_slice(&258u16.to_le_bytes());
    aed[20..24].copy_from_slice(&(100u32 * 8).to_le_bytes());
    for i in 0..100usize {
        let off = 24 + i * 8;
        aed[off..off + 4].copy_from_slice(&2048u32.to_le_bytes());
        aed[off + 4..off + 8].copy_from_slice(&(10_400 + i as u32 * 2).to_le_bytes());
    }
    disc.inner.put_bytes(PART_START + 9000, &aed);
}

#[test]
fn many_aliases_expand_a_terminating_icb_once() {
    let files = (0..512)
        .map(|i| {
            file(
                &format!("VTS_01_alias{i:04}_1.VOB"),
                100,
                10_000,
                2048,
                false,
            )
        })
        .collect();
    let (fs, mut disc) = parsed_with(files, |disc| long_chain(disc, 100));
    assert_eq!(fs.root.entries.len(), 512);
    assert!(fs.root.entries.iter().all(|entry| entry.size == 300 * 2048));
    let keys = DiscKeys::new(&fs, &mut disc, &DecryptKeys::None, None).unwrap();
    assert_eq!(keys.groups.len(), 1);
    let icb_reads = disc
        .reads
        .iter()
        .filter(|&&at| at == PART_START + 100)
        .count();
    let aed_reads = disc
        .reads
        .iter()
        .filter(|&&at| at == PART_START + 9000)
        .count();
    assert_eq!(
        (keys.groups[0].spans.len(), icb_reads, aed_reads),
        (300, 1, 1)
    );
}

fn fragmented_files(
    groups: u32,
    files_per_group: u32,
    title: bool,
) -> Vec<crate::udf::fixture::FileSpec> {
    let ads: Vec<_> = (0..200).map(|i| (10_000 + i * 2, 2048)).collect();
    (1..=groups)
        .flat_map(|group| {
            let ads = &ads;
            (0..files_per_group).map(move |i| {
                let name = format!("VTS_{group:02}_alias{i:04}_{}.VOB", u8::from(title));
                crate::udf::fixture::file_ads(&name, 1000 + group * 500 + i, ads, false)
            })
        })
        .collect()
}

fn assert_budget_error(fs: &UdfFs, disc: &mut CountingDisc) {
    let result = DiscKeys::new(fs, disc, &DecryptKeys::None, None);
    assert!(
        matches!(result, Err(Error::DiscRead { sector, status: None, sense: None }) if sector == u64::from(PART_START)),
        "aggregate extent budget must fail with a metadata DiscRead error"
    );
}

#[test]
fn distinct_icbs_share_a_budget_across_groups() {
    // Each group is below 65,536 tuples; their retained membership exceeds it.
    let (fs, mut disc) = parsed(fragmented_files(2, 170, false));
    assert_budget_error(&fs, &mut disc);
}

#[test]
fn temporary_crack_candidates_share_the_membership_budget() {
    // 44,000 retained spans fit alone, and each group's span+candidate pair
    // fits alone. Previous groups plus the active candidates exceed the cap.
    let (fs, mut disc) = parsed(fragmented_files(2, 110, true));
    assert_budget_error(&fs, &mut disc);
}

#[test]
fn aliases_preserve_title_eligibility_and_title_traversal_order() {
    for files in [
        vec![
            file("VTS_01_0.VOB", 100, 9000, 2048, false),
            file("VTS_01_1.VOB", 101, 10_000, 2048, false),
            file("VTS_01_9.VOB", 100, 9000, 2048, false),
        ],
        vec![
            file("VTS_01_1.VOB", 101, 10_000, 2048, false),
            file("VTS_01_2.VOB", 100, 9000, 2048, false),
            file("VTS_01_z_0.VOB", 100, 9000, 2048, false),
        ],
    ] {
        let (fs, mut disc) = parsed(files);
        let keys = DiscKeys::new(&fs, &mut disc, &DecryptKeys::None, None).unwrap();
        let data_reads: Vec<_> = disc
            .reads
            .iter()
            .copied()
            .filter(|&at| at >= PART_START + 9000)
            .collect();
        assert_eq!(data_reads, [PART_START + 10_000, PART_START + 9000]);
        assert_eq!(keys.groups[0].spans.len(), 2);
        assert!(matches!(keys.groups[0].keys, DecryptKeys::None));
    }
}

#[test]
fn small_fragmented_clear_dvd_keeps_all_spans() {
    let (fs, mut disc) = parsed(fragmented_files(2, 3, true));
    let keys = DiscKeys::new(&fs, &mut disc, &DecryptKeys::None, None).unwrap();
    assert_eq!(keys.groups.len(), 2);
    assert!(keys.groups.iter().all(|group| group.spans.len() == 600));
    assert!(
        keys.groups
            .iter()
            .all(|group| matches!(group.keys, DecryptKeys::None))
    );
}

#[test]
fn temporary_candidates_are_released_between_groups() {
    // 40,000 retained spans + the active group's 20,000 candidates fit.
    // Keeping the previous group's candidates too would exceed the cap.
    let (fs, mut disc) = parsed(fragmented_files(2, 100, true));
    let keys = DiscKeys::new(&fs, &mut disc, &DecryptKeys::None, None).unwrap();
    assert_eq!(keys.groups.len(), 2);
    assert!(keys.groups.iter().all(|group| group.spans.len() == 20_000));
}
