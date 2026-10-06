use super::*;

// A file above the 30-bit AD ceiling must split on a block boundary, or the next extent's
// bytes start mid-sector.
#[test]
fn a_file_past_the_ad_ceiling_splits_on_a_block_boundary() {
    let mut f = FileNode {
        name: "BIG.M2TS".into(),
        disc_path: "/BIG.M2TS".into(),
        host: PathBuf::new(),
        size: MAX_AD_BYTES + 4096,
        mtime: None,
        icb_lba: 0,
        unique_id: 0,
        extents: Vec::new(),
    };
    let end = place_file(&mut f, 1000).unwrap();
    assert_eq!(f.extents.len(), 2);
    assert_eq!(f.extents[0].bytes as u64, MAX_AD_BYTES);
    assert_eq!(f.extents[0].bytes % SECTOR as u32, 0, "block multiple");
    assert!(f.extents[0].bytes <= 0x3FFF_FFFF);
    assert_eq!(f.extents[1].bytes, 4096);
    assert_eq!(
        f.extents[1].lba,
        1000 + (MAX_AD_BYTES / SECTOR as u64) as u32,
        "the second extent starts where the first ends"
    );
    assert_eq!(end, f.extents[1].lba + 2);
    assert_eq!(
        f.extents.iter().map(|e| e.bytes as u64).sum::<u64>(),
        f.size,
        "no bytes lost or invented"
    );
}

/// The metadata ceiling must account for each directory's FID-list sectors, not just one
/// File Entry per node: a wide directory's FID list can span many sectors that the old
/// (dir_count + file_count) count ignored, so an oversized folder could slip past the guard
/// and be materialized in full.
#[test]
fn metadata_block_count_includes_the_fid_list_sectors() {
    let files: Vec<FileNode> = (0..300)
        .map(|i| FileNode {
            name: format!("A_REASONABLY_LONG_FILENAME_{i:04}.M2TS"),
            disc_path: format!("/{i}"),
            host: PathBuf::new(),
            size: 0,
            mtime: None,
            icb_lba: 0,
            unique_id: 0,
            extents: Vec::new(),
        })
        .collect();
    let root = DirNode {
        name: String::new(),
        icb_lba: 0,
        parent_icb_lba: 0,
        data_lba: 0,
        data_bytes: 0,
        unique_id: 0,
        dirs: Vec::new(),
        files,
    };

    let mut dir_count = 0;
    let mut file_count = 0;
    count_nodes(&root, &mut dir_count, &mut file_count);
    // Old formula: one File Entry per node only.
    let file_entry_only = dir_count as u64 + file_count as u64;
    let fid_sectors = dir_bytes(&root.dirs, &root.files).div_ceil(SECTOR) as u64;
    assert!(
        fid_sectors > 1,
        "300 files must fill more than one FID sector"
    );
    // 2 (FSD + Terminating Descriptor) + File Entries + FID-list sectors.
    assert_eq!(
        metadata_block_count(&root),
        2 + file_entry_only + fid_sectors
    );
    assert!(
        metadata_block_count(&root) > file_entry_only,
        "the ceiling must count FID-list sectors the old formula omitted"
    );
}

/// A zero-byte file records no allocation descriptors at all and consumes
/// no blocks.
#[test]
fn an_empty_file_gets_no_extents() {
    let mut f = FileNode {
        name: "EMPTY".into(),
        disc_path: "/EMPTY".into(),
        host: PathBuf::new(),
        size: 0,
        mtime: None,
        icb_lba: 0,
        unique_id: 0,
        extents: Vec::new(),
    };
    assert_eq!(place_file(&mut f, 500).unwrap(), 500);
    assert!(f.extents.is_empty());
}

#[test]
fn host_artefacts_are_not_disc_content() {
    assert!(is_excluded(".DS_Store"));
    assert!(is_excluded("._00000.m2ts"));
    assert!(is_excluded("00000.m2ts.partial"));
    assert!(!is_excluded("00000.m2ts"));
    assert!(!is_excluded("VTS_01_1.VOB"));
}

// Two host names the READER collapses into one must be refused by the planner. Calls `plan`
// on a real folder.
#[test]
fn two_names_the_reader_cannot_tell_apart_are_refused() {
    let dir = std::env::temp_dir().join(format!(
        "fmkv-shadow-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let stream = dir.join("BDMV/STREAM");
    std::fs::create_dir_all(&stream).expect("mkdir");
    std::fs::write(stream.join("00000.m2ts"), b"a").expect("write");
    // Same name to the reader: it trims the leading space.
    std::fs::write(stream.join(" 00000.m2ts"), b"b").expect("write");

    // Precondition — if this stops holding the fixture is wrong, not the code.
    assert_eq!(
        crate::udf::parse_udf_name(&crate::dirimage::encode::encode_cs0(" 00000.m2ts")),
        crate::udf::parse_udf_name(&crate::dirimage::encode::encode_cs0("00000.m2ts")),
        "fixture: these two host names must read back identically"
    );

    let got = plan(&dir);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        matches!(got, Err(Error::DirNameCollision { .. })),
        "a name the reader cannot distinguish must be refused, got {got:?}"
    );
}

// A name too long for the FID's one-byte length field is refused by the planner, on a real
// folder.
#[test]
fn an_over_long_name_is_refused_by_the_planner() {
    let dir = std::env::temp_dir().join(format!(
        "fmkv-longname-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(dir.join("BDMV/STREAM")).expect("mkdir");
    // 255 bytes: the exact length that used to narrow to zero.
    let name = "a".repeat(255);
    assert_eq!(
        crate::dirimage::encode::encode_cs0(&name).len(),
        256,
        "fixture: NAME_MAX encodes to 256 bytes with the compression byte"
    );
    std::fs::write(dir.join("BDMV/STREAM").join(&name), b"x").expect("write");

    let err = plan(&dir).expect_err("the planner must refuse this folder");
    assert!(
        matches!(err, Error::DirNameTooLong { .. }),
        "expected DirNameTooLong, got {err:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A name at the cap is accepted, so the guard rejects only what it must.
#[test]
fn a_name_at_the_cap_is_accepted() {
    let dir = std::env::temp_dir().join(format!(
        "fmkv-okname-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(dir.join("BDMV/STREAM")).expect("mkdir");
    // One byte of name shorter, so the encoding lands exactly on the cap.
    let name = "a".repeat(MAX_CS0_NAME_BYTES - 1);
    assert_eq!(
        crate::dirimage::encode::encode_cs0(&name).len(),
        MAX_CS0_NAME_BYTES
    );
    std::fs::write(dir.join("BDMV/STREAM").join(&name), b"x").expect("write");
    assert!(
        plan(&dir).is_ok(),
        "a name whose encoding equals the cap must be accepted"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// The subdirectory cap must keep the 16-bit link count representable. Pins the arithmetic;
// does NOT exercise `walk`.
#[test]
fn the_subdir_cap_refuses_a_folder_with_too_many_subdirectories() {
    // Executes the guard rather than restating the constant: link count (child
    // dirs + 1) is 16 bits, so exceeding it wraps and the image lies about
    // its own directory structure.
    let dir = std::env::temp_dir().join(format!("fmkv-fanout-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for i in 0..=MAX_SUBDIRS {
        std::fs::create_dir_all(dir.join(format!("d{i}"))).unwrap();
    }

    let err = plan(&dir).expect_err("more subdirectories than the link count can represent");
    assert!(
        matches!(err, Error::DirImageFanout { .. }),
        "expected DirImageFanout, got {err:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// `walk` counts every entry against `MAX_ENTRIES`: the entry that would pass it is refused.
#[test]
fn the_entry_cap_refuses_the_entry_past_max_entries() {
    let dir = std::env::temp_dir().join(format!("fmkv-entries-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.bin"), b"x").unwrap();
    let mut at_cap = MAX_ENTRIES - 1;
    assert!(
        walk(&dir, "/", 0, &mut at_cap).is_ok(),
        "the last allowed entry"
    );
    let mut past = MAX_ENTRIES;
    assert!(matches!(
        walk(&dir, "/", 0, &mut past),
        Err(Error::DirImageTooLarge)
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

// The nesting cap (`MAX_DEPTH`) refuses a tree deeper than the planner
// represents, before it recurses without bound. Executes `walk`'s depth
// guard on a real directory chain, not the constant.
#[test]
fn the_nesting_cap_refuses_a_tree_deeper_than_max_depth() {
    let base = std::env::temp_dir().join(format!("fmkv-depth-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    // MAX_DEPTH + 2 nested levels below the root guarantees a `walk` call at
    // a depth greater than MAX_DEPTH.
    let mut deep = base.clone();
    for _ in 0..(MAX_DEPTH as usize + 2) {
        deep = deep.join("d");
    }
    std::fs::create_dir_all(&deep).unwrap();

    let err = plan(&base).expect_err("a tree deeper than the nesting cap must be refused");
    assert!(
        matches!(err, Error::DirImageTooLarge),
        "expected DirImageTooLarge, got {err:?}"
    );

    let _ = std::fs::remove_dir_all(&base);
}

// Exactly MAX_DEPTH levels below the root is accepted; one more is refused.
#[test]
fn the_nesting_cap_accepts_exactly_max_depth() {
    let mk = |tag: &str, levels: usize| {
        let base = std::env::temp_dir().join(format!("fmkv-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        // BDMV is level 1, so the tree is a valid disc layout.
        let mut deep = base.join("BDMV");
        for _ in 1..levels {
            deep = deep.join("d");
        }
        std::fs::create_dir_all(&deep).unwrap();
        base
    };
    let ok = mk("depth-ok", MAX_DEPTH as usize);
    assert!(plan(&ok).is_ok(), "MAX_DEPTH levels must be accepted");
    let over = mk("depth-over", MAX_DEPTH as usize + 1);
    assert!(matches!(plan(&over), Err(Error::DirImageTooLarge)));
    let _ = std::fs::remove_dir_all(&ok);
    let _ = std::fs::remove_dir_all(&over);
}
