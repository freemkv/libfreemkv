//! The public whole-disc reader applies one decrypted-name policy even without AACS keys.

use super::*;

fn clear_fixture(collision: bool) -> Fx {
    let mut files = vec![stream(1, 2, None)];
    if collision {
        files.push(BdFile::new("AACS/Unit_Key_RO.bak", 3, None));
    }
    let mut fx = fixture(&files, 1, &[&[0]]);
    fx.disc.aacs = None;
    fx.disc.encrypted = false;
    fx
}

#[test]
fn clear_whole_disc_names_sector_view_preserves_payload() {
    let fx = clear_fixture(false);
    let mut original = fx.img.source();
    let fs = crate::udf::read_filesystem(&mut original).unwrap();
    let metadata = fs
        .read_file(&mut original, "/AACS/Unit_Key_RO.inf")
        .unwrap();
    let payload = fs
        .read_file(&mut original, "/BDMV/STREAM/00001.m2ts")
        .unwrap();
    let mut reader = KeyRing::none()
        .whole_disc_reader(&fx.disc, fx.img.source(), None)
        .unwrap();
    let renamed = crate::udf::read_filesystem(&mut reader).unwrap();
    let names: Vec<_> = renamed
        .find_dir("/AACS")
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(names, ["Unit_Key_RO.bak"]);
    assert_eq!(
        renamed
            .read_file(&mut reader, "/AACS/Unit_Key_RO.bak")
            .unwrap(),
        metadata
    );
    assert_eq!(
        renamed
            .read_file(&mut reader, "/BDMV/STREAM/00001.m2ts")
            .unwrap(),
        payload
    );
}

#[test]
fn clear_whole_disc_names_extraction_preserves_payload() {
    let fx = clear_fixture(false);
    let mut original = fx.img.source();
    let fs = crate::udf::read_filesystem(&mut original).unwrap();
    let metadata = fs
        .read_file(&mut original, "/AACS/Unit_Key_RO.inf")
        .unwrap();
    let payload = fs
        .read_file(&mut original, "/BDMV/STREAM/00001.m2ts")
        .unwrap();

    let dest = tempfile::tempdir().unwrap();
    let result = fx
        .disc
        .extract_tree(
            &mut fx.img.source(),
            dest.path(),
            &Default::default(),
            &crate::Ctx::default(),
        )
        .unwrap();
    assert!(result.complete);
    assert_eq!(result.bytes_lost(), 0);
    assert_eq!(result.files.len(), 2);
    assert!(!dest.path().join("AACS/Unit_Key_RO.inf").exists());
    assert_eq!(
        std::fs::read(dest.path().join("AACS/Unit_Key_RO.bak")).unwrap(),
        metadata
    );
    assert_eq!(
        std::fs::read(dest.path().join("BDMV/STREAM/00001.m2ts")).unwrap(),
        payload
    );
}

#[test]
fn clear_whole_disc_names_raw_image_hash_is_unchanged() {
    use sha2::{Digest, Sha256};

    let fx = clear_fixture(false);
    let dest = tempfile::tempdir().unwrap();
    let raw = dest.path().join("raw.iso");
    let mut reader = crate::whole_disc::raw_whole_disc_reader(fx.img.source());
    crate::write_image(
        &mut reader,
        &raw,
        fx.disc.capacity_sectors,
        &crate::Ctx::default(),
    )
    .unwrap();
    let raw = std::fs::read(raw).unwrap();
    assert_eq!(Sha256::digest(&raw), Sha256::digest(&fx.img.image));
    assert_eq!(raw, fx.img.image);
}

// Name preparation needs FID names and directory ICBs, never leaf File Entries.
#[test]
fn clear_whole_disc_names_preparation_skips_leaf_file_entries() {
    let fx = clear_fixture(false);
    let mut raw = fx.img.source();
    let fs = crate::udf::read_filesystem(&mut raw).unwrap();
    let patches = fs
        .name_patches(
            &mut raw,
            &[crate::udf::NameRule {
                dir: "/AACS",
                from: "Unit_Key_RO.inf",
                to: "Unit_Key_RO.bak",
            }],
        )
        .unwrap();
    assert!(!patches.is_empty());
    let mut directories = vec![fs.metadata_start() + fs.root.meta_lba];
    let mut leaves = Vec::new();
    let mut pending = vec![&fs.root];
    while let Some(dir) = pending.pop() {
        for entry in &dir.entries {
            let lba = fs.metadata_start() + entry.meta_lba;
            if entry.is_dir {
                directories.push(lba);
                pending.push(entry);
            } else {
                leaves.push(lba);
            }
        }
    }
    assert_eq!(
        leaves.len(),
        2,
        "stream and AACS metadata are both leaf files"
    );
    for &leaf in &leaves {
        let mut source = ProbeSource::new(fx.img.image.clone(), fx.disc.capacity_sectors);
        source.fail = Some((leaf, 1));
        let mut reader = KeyRing::none()
            .whole_disc_reader(&fx.disc, &mut source as &mut dyn SectorSource, None)
            .expect("leaf damage must not prevent name preparation");
        for patch in &patches {
            let mut sector = [0; 2048];
            assert_eq!(
                reader
                    .read_sectors(patch.lba, 1, &mut sector, true)
                    .unwrap(),
                2048
            );
            assert_eq!(sector[patch.offset], patch.value);
        }
        drop(reader);
        assert!(directories.iter().all(|lba| source.reads.contains(lba)));
        assert!(
            leaves.iter().all(|lba| !source.touched.contains(lba)),
            "leaf FE reads: {:?}",
            source.reads
        );

        // The public reader still loads real file sizes and propagates FE failures.
        let mut full = ProbeSource::new(fx.img.image.clone(), fx.disc.capacity_sectors);
        full.fail = Some((leaf, 1));
        assert!(
            matches!(crate::udf::read_filesystem(&mut full), Err(Error::DiscRead { sector, .. }) if sector == leaf as u64)
        );
        assert!(full.reads.contains(&leaf));
    }
}

// Fail a particular metadata read, including the second pass made by name preparation.
struct ProbeSource {
    inner: MemSource,
    capacity: u32,
    reads: Vec<u32>,
    touched: std::collections::HashSet<u32>,
    fail: Option<(u32, usize)>,
    halted: bool,
}

impl ProbeSource {
    fn new(bytes: Vec<u8>, capacity: u32) -> Self {
        Self {
            inner: MemSource::new(bytes),
            capacity,
            reads: Vec::new(),
            touched: std::collections::HashSet::new(),
            fail: None,
            halted: false,
        }
    }
}

impl SectorSource for ProbeSource {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.reads.push(lba);
        self.touched.extend(lba..lba + u32::from(count));
        if self.fail.is_some_and(|(at, hit)| {
            at == lba && self.reads.iter().filter(|&&n| n == at).count() == hit
        }) {
            return Err(if self.halted {
                Error::Halted
            } else {
                Error::DiscRead {
                    sector: lba as u64,
                    status: None,
                    sense: None,
                }
            });
        }
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

#[test]
fn clear_whole_disc_names_generic_sources_are_unchanged() {
    let fx = clear_fixture(false);
    // Zero means unknown capacity: a readable non-anchor still permits a generic source.
    for (sectors, capacity) in [(1, 1), (256, 256), (257, 257), (257, 0)] {
        let bytes = vec![0x5a; sectors * 2048];
        let mut source = ProbeSource::new(bytes.clone(), capacity);
        let mut reader = KeyRing::none()
            .whole_disc_reader(&fx.disc, &mut source as &mut dyn SectorSource, None)
            .unwrap();
        let mut got = vec![0; bytes.len()];
        assert_eq!(
            reader
                .read_sectors(0, sectors as u16, &mut got, false)
                .unwrap(),
            bytes.len()
        );
        assert_eq!(got, bytes);
        if sectors <= 256 {
            assert_eq!(source.reads, [0], "a known-short source must not be probed");
        }
    }
}

fn aacs_names(reader: &mut dyn SectorSource) -> Vec<String> {
    let fs = crate::udf::read_filesystem(reader).unwrap();
    let mut names: Vec<_> = fs
        .find_dir("/AACS")
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    names.sort();
    names
}

// An unreadable anchor is still probed at unknown capacity; as in 1.8.4 it copies through.
#[test]
fn clear_whole_disc_names_unknown_capacity_probes_a_failing_anchor() {
    let fx = clear_fixture(false);
    let mut source = ProbeSource::new(vec![0; 2048], 0);
    KeyRing::none()
        .whole_disc_reader(&fx.disc, &mut source as &mut dyn SectorSource, None)
        .expect("a damaged anchor must not abort the copy");
    assert_eq!(source.reads, [256]);
}

#[test]
fn clear_whole_disc_names_metadata_damage_copies_and_halt_propagates() {
    let fx = clear_fixture(false);
    let mut raw = fx.img.source();
    let fs = crate::udf::read_filesystem(&mut raw).unwrap();
    let directory = fs.metadata_start() + fs.find_dir("/AACS").unwrap().meta_lba;
    for (lba, hit, halted) in [
        (256, 1, false),
        (directory, 1, false),
        (directory, 2, false),
        (directory, 3, false),
        (256, 1, true),
        (directory, 1, true),
        (directory, 2, true),
        (directory, 3, true),
    ] {
        let mut source = ProbeSource::new(fx.img.image.clone(), fx.disc.capacity_sectors);
        source.fail = Some((lba, hit));
        source.halted = halted;
        let got = KeyRing::none().whole_disc_reader(&fx.disc, source, None);
        match got {
            Err(Error::Halted) if halted => {}
            Ok(_) if !halted => {}
            Err(other) => panic!("unexpected metadata error: {other:?}"),
            Ok(_) => panic!("a Stop must end the copy"),
        }
    }
}

#[test]
fn clear_whole_disc_names_malformed_directory_keeps_original_names() {
    let fx = clear_fixture(false);
    let mut raw = fx.img.source();
    let fs = crate::udf::read_filesystem(&mut raw).unwrap();
    let directory = fs.metadata_start() + fs.find_dir("/AACS").unwrap().meta_lba;
    let mut bytes = fx.img.image.clone();
    bytes[directory as usize * 2048..directory as usize * 2048 + 2].fill(0);
    let mut reader = KeyRing::none()
        .whole_disc_reader(&fx.disc, MemSource::new(bytes.clone()), None)
        .expect("a malformed directory must not abort the copy");
    let mut got = vec![0; bytes.len()];
    for lba in 0..(bytes.len() / 2048) as u32 {
        reader
            .read_sectors(lba, 1, &mut got[lba as usize * 2048..][..2048], false)
            .unwrap();
    }
    assert_eq!(got, bytes);
}

#[test]
fn clear_whole_disc_names_collision_keeps_both_names() {
    let fx = clear_fixture(true);
    let mut reader = KeyRing::none()
        .whole_disc_reader(&fx.disc, fx.img.source(), None)
        .expect("a pre-existing target must not abort the copy");
    assert_eq!(
        aacs_names(&mut reader),
        ["Unit_Key_RO.bak", "Unit_Key_RO.inf"]
    );
}

#[test]
fn clear_whole_disc_names_aacs_preparation_stays_strict() {
    let fx = fixture(&[stream(1, 2, None)], 1, &[&[0]]);
    let keys = resolve(&fx, KeyScope::WholeDisc, &[]).unwrap();
    assert!(keys.is_aacs());
    assert!(matches!(
        keys.whole_disc_reader(&fx.disc, MemSource::new(vec![0; fx.img.image.len()]), None),
        Err(Error::UdfNotFilesystem)
    ));
}

// An ISO copy reads the disc's own names; it renames once after the copy.
#[test]
fn whole_disc_reader_keeping_names_reads_the_original_names() {
    let fx = clear_fixture(false);
    let mut reader = KeyRing::none()
        .whole_disc_reader_keeping_names(&fx.disc, fx.img.source(), None)
        .unwrap();
    let fs = crate::udf::read_filesystem(&mut reader).unwrap();
    let names: Vec<_> = fs
        .find_dir("/AACS")
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(names, ["Unit_Key_RO.inf"]);
}
