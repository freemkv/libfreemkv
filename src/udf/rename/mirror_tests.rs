use super::*;
use crate::udf::fixture::{self, DirSpec, MemDisc, PART_START};
use std::collections::BTreeSet;

const MAIN: u32 = PART_START + 512;
const MAIN_TARGET: u32 = PART_START + 2048;
const MIRROR_FE: u32 = PART_START + 10;
const MIRROR_TARGET: u32 = PART_START + 3500 + 16;
const END: u32 = 5600;

struct Disc {
    inner: MemDisc,
    reads: BTreeSet<u32>,
    denied: BTreeSet<u32>,
    short: BTreeSet<u32>,
}

impl SectorSource for Disc {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        for sector in lba..lba + u32::from(count) {
            self.reads.insert(sector);
            if self.denied.contains(&sector) {
                return Err(bad_sector(sector));
            }
        }
        let bytes = self.inner.read_sectors(lba, count, buf, recovery)?;
        Ok(if self.short.contains(&lba) {
            bytes - 1
        } else {
            bytes
        })
    }
}

// Independent bitwise oracle; fixture checks do not use the production CRC helper.
fn crc(bytes: &[u8]) -> u16 {
    let mut value = 0u16;
    for &byte in bytes {
        value ^= u16::from(byte) << 8;
        for _ in 0..8 {
            value = if value & 0x8000 == 0 {
                value << 1
            } else {
                (value << 1) ^ 0x1021
            };
        }
    }
    value
}

fn tag(bytes: &mut [u8], size: usize, location: u32) {
    bytes[10..12].copy_from_slice(&((size - 16) as u16).to_le_bytes());
    bytes[12..16].copy_from_slice(&location.to_le_bytes());
    let value = crc(&bytes[16..size]);
    bytes[8..10].copy_from_slice(&value.to_le_bytes());
    bytes[4] = bytes[..16]
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 4)
        .fold(0u8, |sum, (_, &b)| sum.wrapping_add(b));
}

fn check_tag(bytes: &[u8], at: usize, location: u32) {
    let bytes = &bytes[at..];
    let n = u16::from_le_bytes([bytes[10], bytes[11]]) as usize;
    assert_eq!(
        u16::from_le_bytes([bytes[8], bytes[9]]),
        crc(&bytes[16..16 + n])
    );
    assert_eq!(
        bytes[4],
        bytes[..16]
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 4)
            .fold(0u8, |sum, (_, &b)| sum.wrapping_add(b))
    );
    assert_eq!(&bytes[12..16], &location.to_le_bytes());
}

fn sector(reader: &mut dyn SectorSource, lba: u32) -> [u8; SECTOR_BYTES] {
    let mut bytes = [0; SECTOR_BYTES];
    read_sector(reader, lba, &mut bytes).unwrap();
    bytes
}

fn fixture(duplicate: bool) -> Disc {
    let mut disc = MemDisc::new();
    fixture::build_udf_skeleton(&mut disc, 2);
    fixture::lay_dir(
        &mut disc,
        &DirSpec {
            name: String::new(),
            icb_lba: 2,
            dir_data_lba: 3,
            files: vec![],
            subdirs: vec![DirSpec {
                name: "AACS".into(),
                icb_lba: 4,
                dir_data_lba: 32,
                files: vec![fixture::file("Unit_Key_RO.inf", 5, 100, 7, false)],
                subdirs: vec![],
            }],
        },
    );
    disc.put_bytes(PART_START + 100, b"payload");
    for logical in 0..64 {
        let mut data = sector(&mut disc, PART_START + logical);
        if logical == 32 {
            tag(&mut data, 38, logical);
            tag(&mut data[40..], 54, logical);
        }
        let main = if logical < 32 {
            MAIN + logical
        } else {
            MAIN_TARGET + logical - 32
        };
        disc.put_bytes(main, &data);
        let mirror = if duplicate {
            if logical < 16 {
                PART_START + 3000 + logical
            } else {
                PART_START + 3500 + logical - 16
            }
        } else {
            main
        };
        disc.put_bytes(mirror, &data);
    }
    let mut lvd = sector(&mut disc, 33);
    lvd[268..272].copy_from_slice(&2u32.to_le_bytes());
    lvd[440] = 1;
    lvd[441] = 6;
    let map = 446;
    lvd[map] = 2;
    lvd[map + 1] = 64;
    lvd[map + 5..map + 28].copy_from_slice(b"*UDF Metadata Partition");
    lvd[map + 40..map + 44].copy_from_slice(&9u32.to_le_bytes());
    lvd[map + 44..map + 48].copy_from_slice(&10u32.to_le_bytes());
    lvd[map + 58] = u8::from(duplicate);
    disc.put_bytes(33, &lvd);
    let main_fe = fixture::build_file_icb_ads(&[(512, 32 * 2048), (2048, 32 * 2048)], false);
    disc.put_bytes(PART_START + 9, &main_fe);
    let mirror_fe = if duplicate {
        fixture::build_file_icb_ads(&[(3000, 16 * 2048), (3500, 48 * 2048)], false)
    } else {
        main_fe
    };
    disc.put_bytes(MIRROR_FE, &mirror_fe);
    Disc {
        inner: disc,
        reads: BTreeSet::new(),
        denied: BTreeSet::new(),
        short: BTreeSet::new(),
    }
}

fn snapshot(reader: &mut dyn SectorSource) -> Vec<u8> {
    let mut bytes = vec![0; END as usize * SECTOR_BYTES];
    for lba in 0..END {
        read_sector(
            reader,
            lba,
            &mut bytes[lba as usize * SECTOR_BYTES..][..SECTOR_BYTES],
        )
        .unwrap();
    }
    bytes
}

const RULES: &[NameRule] = &[NameRule {
    dir: "/AACS",
    from: "Unit_Key_RO.inf",
    to: "Unit_Key_RO.bak",
}];

#[test]
fn duplicate_mirror_projects_logical_edits_to_different_extent_boundaries() {
    let mut disc = fixture(true);
    let original = snapshot(&mut disc);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    let patches = fs.name_patches(&mut disc, RULES).unwrap();
    assert!(
        patches.iter().any(|p| p.lba == MIRROR_TARGET),
        "mirror must receive the rename"
    );
    let mut output =
        crate::whole_disc::UnitAligned::new(disc, Vec::new()).with_name_patches(patches);
    let changed = snapshot(&mut output);
    for lba in [MAIN_TARGET, MIRROR_TARGET] {
        let bytes = &changed[lba as usize * SECTOR_BYTES..][..SECTOR_BYTES];
        assert_eq!(&bytes[79..94], b"Unit_Key_RO.bak");
        check_tag(bytes, 0, 32);
        check_tag(bytes, 40, 32);
    }
    for (index, (&before, &after)) in original.iter().zip(&changed).enumerate() {
        if before != after {
            let lba = (index / SECTOR_BYTES) as u32;
            let offset = index % SECTOR_BYTES;
            assert!([MAIN_TARGET, MIRROR_TARGET].contains(&lba));
            assert!(
                [44, 48, 49, 91, 92, 93].contains(&offset),
                "unexpected edit at {lba}:{offset}"
            );
        }
    }
    let disc = fixture(true);
    assert_eq!(
        snapshot(&mut crate::whole_disc::raw_whole_disc_reader(disc)),
        original
    );
}

#[test]
fn shared_mirror_data_does_not_open_or_patch_its_file_entry() {
    let mut disc = fixture(false);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    disc.denied.insert(MIRROR_FE);
    disc.reads.clear();
    let patches = fs.name_patches(&mut disc, RULES).unwrap();
    assert!(patches.iter().all(|p| p.lba == MAIN_TARGET));
    assert!(!disc.reads.contains(&MIRROR_FE));
}

#[test]
fn duplicate_mirror_failures_never_return_a_main_only_plan() {
    for fault in 0..12 {
        let mut disc = fixture(true);
        match fault {
            0 => {
                disc.denied.insert(MIRROR_FE);
            }
            1 => {
                let mut fe = sector(&mut disc, MIRROR_FE);
                fe[212..216].copy_from_slice(&7u32.to_le_bytes());
                disc.inner.put_bytes(MIRROR_FE, &fe);
            }
            2 => {
                disc.inner
                    .put_bytes(MIRROR_FE, &fixture::build_file_icb(32 * 2048, 3000, false));
            }
            3 => {
                disc.denied.insert(MIRROR_TARGET);
            }
            4 => {
                let mut bytes = sector(&mut disc, MIRROR_TARGET);
                bytes[1000] = 9;
                disc.inner.put_bytes(MIRROR_TARGET, &bytes);
            }
            5 => {
                let mut lvd = sector(&mut disc, 33);
                lvd[490..494].copy_from_slice(&u32::MAX.to_le_bytes());
                disc.inner.put_bytes(33, &lvd);
            }
            6 => {
                let mut fe = sector(&mut disc, MIRROR_FE);
                fe[212..216].copy_from_slice(&u32::MAX.to_le_bytes());
                disc.inner.put_bytes(MIRROR_FE, &fe);
            }
            7 => {
                disc.inner.put_bytes(
                    MIRROR_FE,
                    &fixture::build_file_icb(32 * 2048 + 1, 3000, false),
                );
            }
            8 => {
                disc.short.insert(MIRROR_FE);
            }
            9 => {
                disc.short.insert(MIRROR_TARGET);
            }
            10 => {
                let mut fe = sector(&mut disc, MIRROR_FE);
                fe[56..64].copy_from_slice(&(32u64 * 2048).to_le_bytes());
                disc.inner.put_bytes(MIRROR_FE, &fe);
            }
            11 => {
                let mut fe = sector(&mut disc, MIRROR_FE);
                fe[224..228].copy_from_slice(&((3u32 << 30) | (48 * 2048)).to_le_bytes());
                disc.inner.put_bytes(MIRROR_FE, &fe);
            }
            _ => unreachable!(),
        }
        let fs = crate::udf::read_filesystem(&mut disc).unwrap();
        disc.reads.clear();
        assert!(
            matches!(
                fs.name_patches(&mut disc, RULES),
                Err(Error::DiscRead { .. })
            ),
            "fault {fault}"
        );
        if fault == 2 || fault == 7 {
            assert!(
                !disc.reads.contains(&(PART_START + 3032)),
                "short map must not authorize next sector"
            );
        }
        if fault == 11 {
            assert!(!disc.reads.contains(&(PART_START + 3016)));
        }
        disc.denied.clear();
        disc.short.clear();
        let original = snapshot(&mut disc);
        assert_eq!(
            snapshot(&mut crate::whole_disc::raw_whole_disc_reader(disc)),
            original
        );
    }
}

#[test]
fn empty_or_unmatched_rules_never_consult_a_broken_duplicate_mirror() {
    for short in [false, true] {
        let mut disc = fixture(true);
        if short {
            disc.inner
                .put_bytes(MIRROR_FE, &fixture::build_file_icb(2048, 3000, false));
        } else {
            disc.denied.insert(MIRROR_FE);
        }
        let fs = crate::udf::read_filesystem(&mut disc).unwrap();
        disc.reads.clear();
        let absent = [NameRule {
            dir: "/AACS",
            from: "absent.inf",
            to: "absent.bak",
        }];
        assert!(fs.name_patches(&mut disc, &absent).unwrap().is_empty());
        assert!(fs.name_patches(&mut disc, &[]).unwrap().is_empty());
        assert!(disc.reads.is_empty());
    }
}

#[test]
fn only_touched_mirror_blocks_are_consulted() {
    let mut disc = fixture(true);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    disc.denied.insert(PART_START + 3000);
    disc.denied.insert(PART_START + 3500);
    let mut bytes = sector(&mut disc, PART_START + 3501);
    bytes[777] ^= 1;
    disc.inner.put_bytes(PART_START + 3501, &bytes);
    disc.reads.clear();
    assert!(fs.name_patches(&mut disc, RULES).is_ok());
    assert!(!disc.reads.contains(&(PART_START + 3000)));
    assert!(!disc.reads.contains(&(PART_START + 3500)));
    assert!(!disc.reads.contains(&(PART_START + 3501)));
}

#[test]
fn bounded_mirror_decoder_uses_fe_and_efe_short_and_long_allocation_fields() {
    for tag_id in [261u16, 266] {
        for long in [false, true] {
            let mut disc = fixture(true);
            let mut fe = fixture::build_file_icb_ads(&[(3000, 16 * 2048), (3500, 48 * 2048)], long);
            if tag_id == 261 {
                fe.copy_within(208..216 + if long { 32 } else { 16 }, 168);
                fe[0..2].copy_from_slice(&tag_id.to_le_bytes());
            }
            disc.inner.put_bytes(MIRROR_FE, &fe);
            let fs = crate::udf::read_filesystem(&mut disc).unwrap();
            let patches = fs.name_patches(&mut disc, RULES).unwrap();
            assert!(patches.iter().any(|p| p.lba == MIRROR_TARGET));
        }
    }
}
