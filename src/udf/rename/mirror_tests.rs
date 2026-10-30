use super::*;
use crate::udf::fixture::{self, DirSpec, MemDisc, PART_START};
use std::collections::BTreeSet;

const MAIN: u32 = PART_START + 512;
const MAIN_TARGET: u32 = PART_START + 2048;
const MIRROR_FE: u32 = PART_START + 10;
const MIRROR_TARGET: u32 = PART_START + 3500 + 16;
const END: u32 = 5600;
// The root directory's FIDs (`AACS`), logical block 3, in each copy.
const ROOT_MAIN: u32 = MAIN + 3;
const ROOT_MIRROR: u32 = PART_START + 3000 + 3;
const FOLDER: &[NameRule] = crate::whole_disc::DECRYPTED_NAME_RULES;

struct Disc {
    inner: MemDisc,
    reads: BTreeSet<u32>,
    denied: BTreeSet<u32>,
    deny_with: fn(u32) -> Error,
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
                return Err((self.deny_with)(sector));
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
        if logical == 3 {
            tag(&mut data, 38, logical);
            tag(&mut data[40..], 43, logical);
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
        deny_with: bad_sector,
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
    for fault in [0, 1, 2, 4, 5, 6, 7, 8, 10, 11] {
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

// The plan's final bytes for `lba`: its fill (or the source sector), then its patches.
fn planned(plan: &NamePlan, disc: &mut Disc, lba: u32) -> [u8; SECTOR_BYTES] {
    let mut bytes = match plan.fills.iter().find(|(at, _)| *at == lba) {
        Some((_, fill)) => **fill,
        None => sector(&mut disc.inner, lba),
    };
    for p in plan.patches.iter().filter(|p| p.lba == lba) {
        bytes[p.offset] = p.value;
    }
    bytes
}

fn assert_mirror_filled(mut disc: Disc) {
    let main = sector(&mut disc.inner, MAIN_TARGET);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    let plan = fs.name_plan(&mut disc, RULES).unwrap();
    assert_eq!(plan.fills.len(), 1);
    assert_eq!(plan.fills[0].0, MIRROR_TARGET);
    assert_eq!(*plan.fills[0].1, main);
    for lba in [MAIN_TARGET, MIRROR_TARGET] {
        assert!(
            plan.patches.iter().any(|p| p.lba == lba),
            "patches at {lba}"
        );
    }
    let renamed = planned(&plan, &mut disc, MAIN_TARGET);
    assert_eq!(&renamed[79..94], b"Unit_Key_RO.bak");
    check_tag(&renamed, 40, 32);
    assert_eq!(planned(&plan, &mut disc, MIRROR_TARGET), renamed);
}

#[test]
fn unreadable_mirror_sector_is_planned_from_main_and_filled() {
    let mut disc = fixture(true);
    disc.denied.insert(MIRROR_TARGET);
    assert_mirror_filled(disc);
}

#[test]
fn short_mirror_sector_is_filled() {
    let mut disc = fixture(true);
    disc.short.insert(MIRROR_TARGET);
    assert_mirror_filled(disc);
}

// An all-zero mirror sector is what an earlier copy blanked, not a second opinion.
#[test]
fn blank_mirror_sector_is_filled() {
    let mut disc = fixture(true);
    disc.inner.put_bytes(MIRROR_TARGET, &[0; SECTOR_BYTES]);
    assert_mirror_filled(disc);
}

// Main is never rebuilt from the mirror: an unreadable main sector refuses the plan.
#[test]
fn unreadable_main_directory_refuses() {
    let mut disc = fixture(true);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    disc.denied.insert(MAIN_TARGET);
    assert!(matches!(
        fs.name_plan(&mut disc, RULES),
        Err(Error::DiscRead { .. })
    ));
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

fn prepare(disc: Disc) -> (Result<crate::whole_disc::PreparedReader<Disc>>, bool) {
    let (prepared, events) = crate::testlog::capture(|| {
        crate::whole_disc::prepare_decrypted_names(
            crate::whole_disc::raw_whole_disc_reader(disc),
            true,
        )
    });
    let warned = events
        .iter()
        .any(|e| e.target == "freemkv::udf" && e.level == tracing::Level::WARN);
    (prepared, warned)
}

// A root mirror sector that is unreadable, short or unequal skips the rename (1.8.4's image).
#[test]
fn damaged_or_mismatched_mirror_copies_without_the_rename() {
    for fault in 0..3 {
        let mut disc = fixture(true);
        let main = sector(&mut disc, ROOT_MAIN);
        match fault {
            0 => {
                disc.denied.insert(ROOT_MIRROR);
            }
            1 => {
                disc.short.insert(ROOT_MIRROR);
            }
            _ => {
                let mut bytes = sector(&mut disc, ROOT_MIRROR);
                bytes[1000] = 9;
                disc.inner.put_bytes(ROOT_MIRROR, &bytes);
            }
        }
        let (prepared, warned) = prepare(disc);
        let mut prepared = prepared.unwrap_or_else(|e| panic!("fault {fault}: {e:?}"));
        assert!(warned, "fault {fault}");
        assert_eq!(sector(&mut prepared, ROOT_MAIN), main, "fault {fault}");
    }
}

// A root `AACS` FID whose CRC length is out of range skips the rename instead of failing.
#[test]
fn unexpected_aacs_fid_copies_without_the_rename() {
    let mut disc = fixture(false);
    let mut dir = sector(&mut disc, ROOT_MAIN);
    dir[50..52].copy_from_slice(&u16::MAX.to_le_bytes());
    disc.inner.put_bytes(ROOT_MAIN, &dir);
    let original = snapshot(&mut disc);
    let (prepared, warned) = prepare(disc);
    let mut prepared = prepared.unwrap();
    assert!(warned);
    assert_eq!(snapshot(&mut prepared), original);
}

// CRC length 0 means no CRC to recompute: the rename proceeds, CRC bytes stay as they were.
#[test]
fn aacs_fid_with_no_crc_is_renamed_and_its_crc_bytes_kept() {
    let mut disc = fixture(false);
    let mut dir = sector(&mut disc, ROOT_MAIN);
    dir[50..52].copy_from_slice(&0u16.to_le_bytes());
    disc.inner.put_bytes(ROOT_MAIN, &dir);
    let (prepared, warned) = prepare(disc);
    let mut prepared = prepared.unwrap();
    assert!(!warned);
    let after = sector(&mut prepared, ROOT_MAIN);
    assert_eq!(&after[79..83], b"SCAA");
    assert_eq!(&after[40 + 8..40 + 12], &dir[40 + 8..40 + 12]);
    assert_eq!(&after[..79], &dir[..79]);
    assert_eq!(&after[83..], &dir[83..]);
}

// A Stop or a dead bus while planning still ends the copy.
#[test]
fn halt_and_transport_faults_during_the_rename_still_abort() {
    fn transport(sector: u32) -> Error {
        Error::DiscRead {
            sector: u64::from(sector),
            status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
            sense: None,
        }
    }
    for deny_with in [transport as fn(u32) -> Error, |_| Error::Halted] {
        let mut disc = fixture(true);
        disc.denied.insert(ROOT_MIRROR);
        disc.deny_with = deny_with;
        let (prepared, warned) = prepare(disc);
        let e = prepared.err().expect("fault must abort");
        assert!(matches!(e, Error::Halted) || e.is_scsi_transport_failure());
        assert!(!warned);
    }
}

// Only an unreadable sector is rebuilt: a host I/O error or a truncated image refuses.
#[test]
fn a_mirror_host_io_error_refuses_instead_of_filling() {
    fn eio(_: u32) -> Error {
        Error::IoError {
            source: std::io::Error::other("EIO"),
        }
    }
    fn ends(lba: u32) -> Error {
        Error::ImageEndsBeforeRead {
            lba,
            have: 0,
            want: 1,
        }
    }
    for deny_with in [eio as fn(u32) -> Error, ends] {
        let mut disc = fixture(true);
        let fs = crate::udf::read_filesystem(&mut disc).unwrap();
        disc.denied.insert(MIRROR_TARGET);
        disc.deny_with = deny_with;
        assert!(fs.name_plan(&mut disc, RULES).is_err());
    }
}

// The product rule edits the root directory's `AACS` FID in main and in the duplicate mirror,
// each at its own extent mapping; the AACS folder's own sectors never change.
#[test]
fn root_folder_rename_projects_to_the_duplicate_mirror() {
    let mut disc = fixture(true);
    let original = snapshot(&mut disc);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    let patches = fs.name_patches(&mut disc, FOLDER).unwrap();
    for lba in [ROOT_MAIN, ROOT_MIRROR] {
        assert!(patches.iter().any(|p| p.lba == lba), "patches at {lba}");
    }
    let mut output =
        crate::whole_disc::UnitAligned::new(disc, Vec::new()).with_name_patches(patches);
    let changed = snapshot(&mut output);
    for lba in [ROOT_MAIN, ROOT_MIRROR] {
        let bytes = &changed[lba as usize * SECTOR_BYTES..][..SECTOR_BYTES];
        assert_eq!(&bytes[79..83], b"SCAA");
        assert_eq!(bytes[40 + 18] & 0x02, 0x02, "still a directory FID");
        check_tag(bytes, 0, 3);
        check_tag(bytes, 40, 3);
    }
    for (index, (&before, &after)) in original.iter().zip(&changed).enumerate() {
        if before != after {
            let lba = (index / SECTOR_BYTES) as u32;
            let offset = index % SECTOR_BYTES;
            assert!([ROOT_MAIN, ROOT_MIRROR].contains(&lba), "edit at {lba}");
            assert!(
                [44, 48, 49, 79, 80, 81, 82].contains(&offset),
                "unexpected edit at {lba}:{offset}"
            );
        }
    }
    let renamed = crate::udf::read_filesystem(&mut output).unwrap();
    assert!(renamed.find_dir("/AACS").is_none());
    assert_eq!(
        renamed
            .read_file(&mut output, "/SCAA/Unit_Key_RO.inf")
            .unwrap(),
        b"payload"
    );
}

fn assert_root_mirror_filled(mut disc: Disc) {
    let main = sector(&mut disc.inner, ROOT_MAIN);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    let plan = fs.name_plan(&mut disc, FOLDER).unwrap();
    assert_eq!(plan.fills.len(), 1);
    assert_eq!(plan.fills[0].0, ROOT_MIRROR);
    assert_eq!(*plan.fills[0].1, main);
    let renamed = planned(&plan, &mut disc, ROOT_MAIN);
    assert_eq!(&renamed[79..83], b"SCAA");
    check_tag(&renamed, 40, 3);
    assert_eq!(planned(&plan, &mut disc, ROOT_MIRROR), renamed);
}

#[test]
fn unreadable_root_mirror_sector_is_filled_from_main() {
    let mut disc = fixture(true);
    disc.denied.insert(ROOT_MIRROR);
    assert_root_mirror_filled(disc);
}

#[test]
fn blank_root_mirror_sector_is_filled_from_main() {
    let mut disc = fixture(true);
    disc.inner.put_bytes(ROOT_MIRROR, &[0; SECTOR_BYTES]);
    assert_root_mirror_filled(disc);
}

// Two readable root copies that disagree are not repaired.
#[test]
fn a_root_mirror_that_differs_from_main_refuses() {
    let mut disc = fixture(true);
    let mut bytes = sector(&mut disc, ROOT_MIRROR);
    bytes[1000] = 9;
    disc.inner.put_bytes(ROOT_MIRROR, &bytes);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    assert!(matches!(
        fs.name_plan(&mut disc, FOLDER),
        Err(Error::DiscRead { .. })
    ));
}
