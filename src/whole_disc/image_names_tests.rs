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
        files.push(BdFile::new("AACS/Unit_Key_RO.bak", 1, None));
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

fn names(path: &Path) -> Vec<String> {
    let mut src = FileSectorSource::open(path).unwrap();
    let fs = crate::udf::read_filesystem(&mut src).unwrap();
    let mut names: Vec<_> = fs
        .find_dir("/AACS")
        .unwrap()
        .entries
        .iter()
        .map(|e| e.name.clone())
        .collect();
    names.sort();
    names
}

#[test]
fn a_readable_iso_is_renamed_on_both_metadata_copies() {
    let iso = iso(false);
    let dir = iso.layout.aacs_dir;
    let plan = plan_image_names(&iso.path, &[]).unwrap().expect("a rename");
    assert!(plan.restored.is_empty());
    for lba in [dir, iso.layout.mirror_of(dir)] {
        assert!(plan.sectors.iter().any(|(at, _)| *at == lba), "{lba}");
    }
    write(&iso.path, &plan);
    assert_eq!(names(&iso.path), ["Unit_Key_RO.bak"]);
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

// A rescue left the mirror's /AACS sector unread (zero-filled): it is rebuilt from main.
#[test]
fn an_unread_mirror_sector_is_filled_from_main_and_renamed() {
    let iso = iso(false);
    let dir = iso.layout.aacs_dir;
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
    assert_eq!(names(&iso.path), ["Unit_Key_RO.bak"]);
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
    let mask = [(iso.layout.aacs_dir, 1)];
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
    let main = iso.layout.aacs_dir;
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
