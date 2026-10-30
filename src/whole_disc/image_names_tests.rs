use super::*;
use crate::test_util::{BdFile, MetadataMirrorLayout, mirrored_aacs_image};

struct Iso {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
    layout: MetadataMirrorLayout,
}

fn iso(collision: bool) -> Iso {
    let mut files = vec![BdFile::new("BDMV/STREAM/00001.m2ts", 3, Some([7; 16]))];
    if collision {
        files.push(BdFile::new("SCAA/other.dat", 1, None));
    }
    let (img, layout) = mirrored_aacs_image(&files, b"UK-RO");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disc.iso");
    std::fs::write(&path, &img.image).unwrap();
    Iso {
        _dir: dir,
        path,
        layout,
    }
}

fn sector(path: &Path, lba: u32) -> Vec<u8> {
    std::fs::read(path).unwrap()[lba as usize * SECTOR_BYTES..][..SECTOR_BYTES].to_vec()
}

// The engine's write step: each planned sector, whole.
fn write(path: &Path, names: &ImageNames) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    for (lba, bytes) in &names.sectors {
        file.seek(SeekFrom::Start(*lba as u64 * SECTOR_BYTES as u64))
            .unwrap();
        file.write_all(&bytes[..]).unwrap();
    }
}

// The root directory's names.
fn names(path: &Path) -> Vec<String> {
    let mut src = FileSectorSource::open(path).unwrap();
    let fs = crate::udf::read_filesystem(&mut src).unwrap();
    let mut names: Vec<_> = fs.root.entries.iter().map(|e| e.name.clone()).collect();
    names.sort();
    names
}

// The AACS files, OEM: the folder rename leaves every byte of them as the disc has them.
fn aacs_files(path: &Path, dir: &str) -> Vec<(String, Vec<u8>)> {
    let mut src = FileSectorSource::open(path).unwrap();
    let fs = crate::udf::read_filesystem(&mut src).unwrap();
    let mut files: Vec<_> = fs
        .find_dir(dir)
        .unwrap()
        .entries
        .iter()
        .map(|e| e.name.clone())
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|n| {
            let bytes = fs.read_file(&mut src, &format!("{dir}/{n}")).unwrap();
            (n, bytes)
        })
        .collect()
}

#[test]
fn a_readable_iso_is_renamed_on_both_metadata_copies() {
    let iso = iso(false);
    let dir = iso.layout.root_dir;
    let before = aacs_files(&iso.path, "/AACS");
    assert!(before.iter().any(|(n, _)| n == "Unit_Key_RO.inf"));
    let plan = plan_image_names(&iso.path, &[]).unwrap().expect("a rename");
    assert!(plan.restored.is_empty());
    for lba in [dir, iso.layout.mirror_of(dir)] {
        assert!(plan.sectors.iter().any(|(at, _)| *at == lba), "{lba}");
    }
    // Only the root directory's sectors change; the AACS folder's own stay.
    assert!(
        plan.sectors
            .iter()
            .all(|(at, _)| *at == dir || *at == iso.layout.mirror_of(dir))
    );
    write(&iso.path, &plan);
    assert_eq!(names(&iso.path), ["BDMV", "SCAA"]);
    assert_eq!(aacs_files(&iso.path, "/SCAA"), before);
    assert_eq!(
        sector(&iso.path, dir),
        sector(&iso.path, iso.layout.mirror_of(dir))
    );
    assert_eq!(
        check_image_names(&iso.path, &[], true),
        Some(DecryptedNames::Renamed {
            mirror_sectors_restored: 0
        })
    );
    // A second run finds nothing left to rename.
    assert!(plan_image_names(&iso.path, &[]).unwrap().is_none());
}

// A rescue left the mirror's root sector unread (zero-filled): it is rebuilt from main.
#[test]
fn an_unread_mirror_sector_is_filled_from_main_and_renamed() {
    let iso = iso(false);
    let dir = iso.layout.root_dir;
    let mirror = iso.layout.mirror_of(dir);
    let mut bytes = std::fs::read(&iso.path).unwrap();
    bytes[mirror as usize * SECTOR_BYTES..][..SECTOR_BYTES].fill(0);
    std::fs::write(&iso.path, &bytes).unwrap();
    let mask = [(mirror, 1)];
    let plan = plan_image_names(&iso.path, &mask)
        .unwrap()
        .expect("a rename");
    assert_eq!(plan.restored, [mirror]);
    write(&iso.path, &plan);
    assert_eq!(names(&iso.path), ["BDMV", "SCAA"]);
    assert_eq!(sector(&iso.path, dir), sector(&iso.path, mirror));
    assert_eq!(
        check_image_names(&iso.path, &[], true),
        Some(DecryptedNames::Renamed {
            mirror_sectors_restored: 0
        })
    );
}

#[test]
fn an_unread_main_directory_keeps_the_names_as_damaged() {
    let iso = iso(false);
    let original = std::fs::read(&iso.path).unwrap();
    let mask = [(iso.layout.root_dir, 1)];
    let e = plan_image_names(&iso.path, &mask).unwrap_err();
    assert_eq!(NamesKept::from_plan_error(&e, &mask), NamesKept::Damaged);
    assert_eq!(
        check_image_names(&iso.path, &mask, true),
        Some(DecryptedNames::Kept {
            cause: NamesKept::Damaged
        })
    );
    assert_eq!(std::fs::read(&iso.path).unwrap(), original);
    // An unmountable image is damaged only for an AACS disc.
    let anchor = [(256, 1)];
    assert_eq!(
        check_image_names(&iso.path, &anchor, true),
        Some(DecryptedNames::Kept {
            cause: NamesKept::Damaged
        })
    );
    assert_eq!(check_image_names(&iso.path, &anchor, false), None);
}

#[test]
fn a_disc_with_both_names_keeps_them_as_a_collision() {
    let iso = iso(true);
    let e = plan_image_names(&iso.path, &[]).unwrap_err();
    assert_eq!(NamesKept::from_plan_error(&e, &[]), NamesKept::Collision);
    assert_eq!(
        check_image_names(&iso.path, &[], true),
        Some(DecryptedNames::Kept {
            cause: NamesKept::Collision
        })
    );
}

#[test]
fn masked_sectors_read_as_errors_not_zeros() {
    let iso = iso(false);
    let mut src = Masked::new(FileSectorSource::open(&iso.path).unwrap(), &[(300, 2)]);
    let mut buf = vec![0; 4 * SECTOR_BYTES];
    assert!(matches!(
        src.read_sectors(298, 4, &mut buf, true),
        Err(Error::DiscRead { sector: 300, .. })
    ));
    assert!(src.read_sectors(296, 4, &mut buf, true).is_ok());
}

// Main renamed, mirror still the original: a torn image is never reported renamed.
#[test]
fn a_main_renamed_over_an_original_mirror_is_kept_unexpected() {
    let iso = iso(false);
    let main = iso.layout.root_dir;
    let mirror = iso.layout.mirror_of(main);
    let original = sector(&iso.path, mirror);
    let plan = plan_image_names(&iso.path, &[]).unwrap().expect("a rename");
    write(&iso.path, &plan);
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&iso.path)
            .unwrap();
        file.seek(SeekFrom::Start(mirror as u64 * SECTOR_BYTES as u64))
            .unwrap();
        file.write_all(&original).unwrap();
    }
    assert_eq!(
        check_image_names(&iso.path, &[], true),
        Some(DecryptedNames::Kept {
            cause: NamesKept::Unexpected
        })
    );
    // An unreadable mirror is not a second opinion.
    assert_eq!(
        check_image_names(&iso.path, &[(mirror, 1)], true),
        Some(DecryptedNames::Renamed {
            mirror_sectors_restored: 0
        })
    );
}

// A reverse check that can't read the mirror's File Entry blames the damage, not the image.
#[test]
fn a_renamed_main_with_an_unreadable_mirror_entry_is_kept_damaged() {
    let iso = iso(false);
    let plan = plan_image_names(&iso.path, &[]).unwrap().expect("a rename");
    write(&iso.path, &plan);
    let mirror_fe = iso.layout.mirror - 1;
    assert_eq!(
        check_image_names(&iso.path, &[(mirror_fe, 1)], true),
        Some(DecryptedNames::Kept {
            cause: NamesKept::Damaged
        })
    );
}

// A disc whose AACS folder is in place but whose key file is not (a 1.8.6 copy renamed it to
// `Unit_Key_RO.bak`): resuming it renames the folder and leaves the `.bak` as it is.
#[test]
fn a_1_8_6_renamed_key_file_gets_the_folder_rename_too() {
    let iso = iso(false);
    let mut bytes = std::fs::read(&iso.path).unwrap();
    crate::test_util::rename_key_file_like_1_8_6(&mut bytes);
    std::fs::write(&iso.path, &bytes).unwrap();
    let before = aacs_files(&iso.path, "/AACS");
    assert!(before.iter().any(|(n, _)| n == "Unit_Key_RO.bak"));
    // 1.8.6's own check would call this image renamed; 1.8.7's does not yet.
    assert_eq!(
        check_image_names(&iso.path, &[], true),
        Some(DecryptedNames::Kept {
            cause: NamesKept::Unexpected
        })
    );

    let plan = plan_image_names(&iso.path, &[]).unwrap().expect("a rename");
    write(&iso.path, &plan);
    assert_eq!(names(&iso.path), ["BDMV", "SCAA"]);
    assert_eq!(aacs_files(&iso.path, "/SCAA"), before);
    assert_eq!(
        check_image_names(&iso.path, &[], true),
        Some(DecryptedNames::Renamed {
            mirror_sectors_restored: 0
        })
    );
}

// A clear disc has no AACS folder: nothing to rename, nothing to report.
#[test]
fn an_image_without_an_aacs_folder_has_no_names() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("BDMV")).unwrap();
    std::fs::write(temp.path().join("BDMV/index.bdmv"), b"INDX").unwrap();
    let mut img = crate::dirimage::DirImage::open(temp.path()).unwrap();
    let mut bytes = vec![0; img.capacity_sectors() as usize * SECTOR_BYTES];
    for (lba, s) in bytes.chunks_mut(SECTOR_BYTES).enumerate() {
        img.read_sectors(lba as u32, 1, s, false).unwrap();
    }
    let path = temp.path().join("clear.iso");
    std::fs::write(&path, &bytes).unwrap();
    assert!(plan_image_names(&path, &[]).unwrap().is_none());
    assert_eq!(check_image_names(&path, &[], true), None);
}
