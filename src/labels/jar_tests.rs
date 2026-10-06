use super::*;

// A budget no test fixture can exhaust.
fn unbounded() -> u64 {
    u64::MAX
}

/// Smallest constant-pool-empty `.class`: magic, versions, cp_count=1
/// (zero real entries), then empty access/this/super/interfaces/
/// fields/methods/attributes.
const MINIMAL_CLASS: &[u8] = &[
    0xCA, 0xFE, 0xBA, 0xBE, // magic
    0x00, 0x00, // minor
    0x00, 0x00, // major
    0x00, 0x01, // constant_pool_count = 1 -> no entries
    0x00, 0x00, // access_flags
    0x00, 0x00, // this_class
    0x00, 0x00, // super_class
    0x00, 0x00, // interfaces_count
    0x00, 0x00, // fields_count
    0x00, 0x00, // methods_count
    0x00, 0x00, // attributes_count
];

// Build a raw, single-entry, Stored ZIP whose header declares
// `declared_size` as the uncompressed size while the actual stored
// payload is `payload` — forges an attacker-controlled size field.
fn build_stored_zip(name: &str, payload: &[u8], declared_size: u32) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let crc: u32 = {
        // CRC-32 (IEEE) over payload.
        let mut crc = 0xFFFF_FFFFu32;
        for &b in payload {
            crc ^= b as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
        !crc
    };
    let mut out = Vec::new();
    // ----- Local file header -----
    let lfh_offset = out.len() as u32;
    out.extend_from_slice(&0x0403_4b50u32.to_le_bytes()); // signature
    out.extend_from_slice(&20u16.to_le_bytes()); // version needed
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // method = Stored
    out.extend_from_slice(&0u16.to_le_bytes()); // mod time
    out.extend_from_slice(&0u16.to_le_bytes()); // mod date
    out.extend_from_slice(&crc.to_le_bytes()); // crc-32
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // compressed size
    out.extend_from_slice(&declared_size.to_le_bytes()); // uncompressed size (forged)
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // extra len
    out.extend_from_slice(name_bytes);
    out.extend_from_slice(payload);
    // ----- Central directory header -----
    let cd_offset = out.len() as u32;
    out.extend_from_slice(&0x0201_4b50u32.to_le_bytes()); // signature
    out.extend_from_slice(&20u16.to_le_bytes()); // version made by
    out.extend_from_slice(&20u16.to_le_bytes()); // version needed
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // method = Stored
    out.extend_from_slice(&0u16.to_le_bytes()); // mod time
    out.extend_from_slice(&0u16.to_le_bytes()); // mod date
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // compressed size
    out.extend_from_slice(&declared_size.to_le_bytes()); // uncompressed size (forged)
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // extra len
    out.extend_from_slice(&0u16.to_le_bytes()); // comment len
    out.extend_from_slice(&0u16.to_le_bytes()); // disk number start
    out.extend_from_slice(&0u16.to_le_bytes()); // internal attrs
    out.extend_from_slice(&0u32.to_le_bytes()); // external attrs
    out.extend_from_slice(&lfh_offset.to_le_bytes()); // local header offset
    out.extend_from_slice(name_bytes);
    let cd_size = out.len() as u32 - cd_offset;
    // ----- End of central directory -----
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes()); // signature
    out.extend_from_slice(&0u16.to_le_bytes()); // disk number
    out.extend_from_slice(&0u16.to_le_bytes()); // cd start disk
    out.extend_from_slice(&1u16.to_le_bytes()); // entries on this disk
    out.extend_from_slice(&1u16.to_le_bytes()); // total entries
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // comment len
    out
}

fn open(bytes: Vec<u8>) -> Jar {
    ZipArchive::new(Cursor::new(bytes)).expect("valid zip")
}

// Pins the exact 64 MiB value as a literal (not derived from the
// same expression under test) so an arithmetic mutation is caught
// even though no test builds a full 64 MiB buffer.
#[test]
fn max_class_bytes_is_64_mebibytes() {
    assert_eq!(MAX_CLASS_BYTES, 67_108_864);
}

#[test]
fn has_path_prefix_matches_only_declared_prefix() {
    let jar = open(build_stored_zip(
        "com/dbp/Loader.class",
        MINIMAL_CLASS,
        MINIMAL_CLASS.len() as u32,
    ));
    assert!(has_path_prefix(&jar, "com/dbp/"));
    assert!(!has_path_prefix(&jar, "com/bydeluxe/"));
}

#[test]
fn try_each_class_reads_minimal_class() {
    let mut jar = open(build_stored_zip(
        "Foo.class",
        MINIMAL_CLASS,
        MINIMAL_CLASS.len() as u32,
    ));
    let mut seen = Vec::new();
    let r: Option<()> = try_each_class_budgeted(&mut jar, &mut unbounded(), |name, _class| {
        seen.push(name.to_string());
        None
    });
    assert!(r.is_none());
    assert_eq!(seen, vec!["Foo.class".to_string()]);
}

#[test]
fn for_each_class_visits_every_class() {
    let mut jar = open(build_stored_zip(
        "Bar.class",
        MINIMAL_CLASS,
        MINIMAL_CLASS.len() as u32,
    ));
    let mut count = 0usize;
    for_each_class_budgeted(&mut jar, &mut unbounded(), |_, _| count += 1);
    assert_eq!(count, 1);
}

// Attacker-controlled size field (0xFFFF_FFFF ≈ 4 GiB) must not
// trigger a 4 GiB pre-allocation; incremental read completes and
// the real (small) payload parses fine.
#[test]
fn forged_huge_uncompressed_size_does_not_preallocate() {
    let mut jar = open(build_stored_zip("Evil.class", MINIMAL_CLASS, 0xFFFF_FFFF));
    let mut parsed = false;
    for_each_class_budgeted(&mut jar, &mut unbounded(), |name, _class| {
        assert_eq!(name, "Evil.class");
        parsed = true;
    });
    // Reached here without OOM/abort, and the real bytes parsed.
    assert!(parsed);
}

#[test]
fn read_is_bounded_by_cap() {
    // Padding past MINIMAL_CLASS is harmless trailing data the parser
    // ignores; the point is that read_to_end stops at the cap rather
    // than following a (potentially huge) declared size.
    let mut payload = MINIMAL_CLASS.to_vec();
    payload.extend(std::iter::repeat_n(0u8, 4096));
    let mut jar = open(build_stored_zip(
        "Padded.class",
        &payload,
        // Forge a size far larger than the real payload.
        0xFFFF_FFFF,
    ));
    let mut visited = 0usize;
    for_each_class_budgeted(&mut jar, &mut unbounded(), |_, _| visited += 1);
    assert_eq!(visited, 1);
}

// A Deflated entry far larger than the cap must stop inflating at the cap.
// The post-read budget check alone would return the same visible result.
#[test]
fn deflated_entry_is_inflated_only_up_to_the_cap() {
    use std::io::Write as _;
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        w.start_file("big.xml", opts).unwrap();
        w.write_all(&vec![7u8; 1 << 20]).unwrap();
        w.finish().unwrap();
    }
    let mut jar = open(buf);
    let mut budget = u64::MAX;
    let got = try_each_resource(
        &mut jar,
        |_| true,
        1000,
        &mut budget,
        |_, bytes| Some(bytes.len()),
    );
    assert_eq!(got, Some(1000));
    assert_eq!(budget, u64::MAX - 1000);
}

#[test]
fn try_each_resource_reads_non_class_entries_and_skips_classes() {
    let xml = b"<dcx><disc/></dcx>";
    // A .class entry must be skipped; the .xml resource must be surfaced.
    let mut jar = {
        use std::io::{Cursor, Write as _};
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            w.start_file("com/x/Menu.class", opts).unwrap();
            w.write_all(MINIMAL_CLASS).unwrap();
            w.start_file("00000/dcx.xml", opts).unwrap();
            w.write_all(xml).unwrap();
            w.finish().unwrap();
        }
        ZipArchive::new(Cursor::new(buf)).expect("valid zip")
    };

    let mut seen: Vec<String> = Vec::new();
    let mut budget = u64::MAX;
    let found: Option<Vec<u8>> = try_each_resource(
        &mut jar,
        |_| true,
        1024,
        &mut budget,
        |name, bytes| {
            seen.push(name.to_string());
            name.ends_with("dcx.xml").then(|| bytes.to_vec())
        },
    );
    assert_eq!(found.as_deref(), Some(&xml[..]));
    assert!(
        !seen.iter().any(|n| n.ends_with(".class")),
        "a .class entry must never be offered as a resource"
    );
}

#[test]
fn is_jar_matches_files_with_a_case_insensitive_extension() {
    let entry = |name: &str, is_dir| crate::udf::DirEntry {
        name: name.to_string(),
        is_dir,
        meta_lba: 0,
        size: 0,
        entries: Vec::new(),
    };
    assert!(is_jar(&entry("00000.jar", false)));
    assert!(is_jar(&entry("00000.JAR", false)));
    assert!(!is_jar(&entry("00000.jar", true)));
    assert!(!is_jar(&entry("00000.xml", false)));
}

// Directory entries and upper-case `.CLASS` bytecode are never offered as resources.
#[test]
fn try_each_resource_skips_directory_entries_and_upper_case_class() {
    use std::io::{Cursor, Write as _};
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        w.add_directory("assets/", opts).unwrap();
        w.start_file("com/x/Menu.CLASS", opts).unwrap();
        w.write_all(MINIMAL_CLASS).unwrap();
        w.start_file("assets/a.xml", opts).unwrap();
        w.write_all(b"<a/>").unwrap();
        w.finish().unwrap();
    }
    let mut jar = open(buf);
    let mut seen: Vec<String> = Vec::new();
    let mut budget = u64::MAX;
    let _: Option<()> = try_each_resource(
        &mut jar,
        |_| true,
        1024,
        &mut budget,
        |name, _| {
            seen.push(name.to_string());
            None
        },
    );
    assert_eq!(seen, ["assets/a.xml"]);
}

// An entry the name filter rejects is never inflated: it costs no budget.
#[test]
fn unwanted_resources_are_not_inflated() {
    let payload = vec![0u8; 64 * 1024];
    let mut jar = open(build_stored_zip(
        "menu/bg.png",
        &payload,
        payload.len() as u32,
    ));
    let mut budget = 1_000_000u64;
    let r: Option<()> = try_each_resource(
        &mut jar,
        |n| n.ends_with(".xml"),
        1024,
        &mut budget,
        |_, _| Some(()),
    );
    assert!(r.is_none());
    assert_eq!(budget, 1_000_000, "a filtered-out entry must not be read");
}

// Inflated bytes are charged to the shared budget; once it is spent the walk
// stops and the truncated entry is not offered.
#[test]
fn budget_bounds_total_inflation() {
    let mut payload = MINIMAL_CLASS.to_vec();
    payload.extend(std::iter::repeat_n(0u8, 4096));
    let mut jar = open(build_stored_zip(
        "Big.class",
        &payload,
        payload.len() as u32,
    ));
    let mut budget = 100u64;
    let mut visited = 0usize;
    let _: Option<()> = try_each_class_budgeted(&mut jar, &mut budget, |_, _| {
        visited += 1;
        None
    });
    assert_eq!((visited, budget), (0, 0));

    // An entry that exactly fills the remaining budget is complete: offered.
    let mut budget = payload.len() as u64;
    let _: Option<()> = try_each_class_budgeted(&mut jar, &mut budget, |_, _| {
        visited += 1;
        None
    });
    assert_eq!((visited, budget), (1, 0));

    let mut budget = 1_000_000u64;
    let _: Option<()> = try_each_class_budgeted(&mut jar, &mut budget, |_, _| {
        visited += 1;
        None
    });
    assert_eq!(visited, 2);
    assert_eq!(budget, 1_000_000 - payload.len() as u64);
}

// Counts sectors read through it.
struct Counting<'a>(&'a mut crate::udf::fixture::MemDisc, u64);

impl SectorSource for Counting<'_> {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        self.1 += u64::from(count);
        self.0.read_sectors(lba, count, buf, recovery)
    }
}

// Framework detection answers from each jar's central directory: a large jar's
// entry data is never read.
#[test]
fn framework_detect_reads_only_the_central_directory() {
    use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
    use std::io::Write as _;
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        w.start_file("assets/bg.png", opts).expect("start_file");
        w.write_all(&vec![0x5Au8; 1024 * 1024]).expect("write");
        for n in [
            "com/bydeluxe/A.class",
            "com/dbp/B.class",
            "com/foxbd/C.class",
        ] {
            w.start_file(n, opts).expect("start_file");
            w.write_all(MINIMAL_CLASS).expect("write");
        }
        w.finish().expect("finish");
    }
    let dir = |name: &str, icb, files, subdirs| DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: icb + 1,
        files,
        subdirs,
    };
    let jar_dir = dir(
        "JAR",
        30,
        vec![file_with("00000.jar", 32, 4000, buf, true)],
        vec![],
    );
    let root = dir("", 10, vec![], vec![dir("BDMV", 20, vec![], vec![jar_dir])]);
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    let detects: [fn(&mut dyn SectorSource, &UdfFs) -> bool; 3] = [
        super::super::deluxe::detect,
        super::super::dbp::detect,
        super::super::fox::detect,
    ];
    for detect in detects {
        let mut counting = Counting(&mut disc, 0);
        assert!(detect(&mut counting, &udf));
        assert!(
            counting.1 < 128,
            "read {} sectors of a 512-sector jar",
            counting.1
        );
    }
}

// A large jar whose tail the in-place check rejects (EOCD comment length overruns
// the file's ZIP64 sizes) but the zip reader opens still detects, as via `for_each_jar`.
#[test]
fn framework_detect_falls_back_when_in_place_open_fails() {
    use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
    use std::io::Write as _;
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        w.start_file("assets/bg.png", opts).expect("start_file");
        w.write_all(&vec![0x5Au8; 100 * 1024]).expect("write");
        w.start_file("com/dbp/B.class", opts).expect("start_file");
        w.write_all(MINIMAL_CLASS).expect("write");
        w.finish().expect("finish");
    }
    // Rewrite the tail as ZIP64: real sizes move to a ZIP64 EOCD + locator.
    let eocd = buf.len() - 22;
    let le32 = |o: usize| u32::from_le_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
    let (cd_size, cd_offset) = (le32(eocd + 12) as u64, le32(eocd + 16) as u64);
    let entries = u16::from_le_bytes([buf[eocd + 10], buf[eocd + 11]]) as u64;
    let mut z64 = Vec::new();
    z64.extend_from_slice(&0x0606_4b50u32.to_le_bytes());
    z64.extend_from_slice(&44u64.to_le_bytes());
    z64.extend_from_slice(&45u16.to_le_bytes());
    z64.extend_from_slice(&45u16.to_le_bytes());
    z64.extend_from_slice(&[0u8; 8]); // disk numbers
    for v in [entries, entries, cd_size, cd_offset] {
        z64.extend_from_slice(&v.to_le_bytes());
    }
    z64.extend_from_slice(&0x0706_4b50u32.to_le_bytes());
    z64.extend_from_slice(&0u32.to_le_bytes());
    z64.extend_from_slice(&(eocd as u64).to_le_bytes());
    z64.extend_from_slice(&1u32.to_le_bytes());
    buf[eocd + 12..eocd + 20].fill(0xFF);
    buf.splice(eocd..eocd, z64);
    assert!(ZipArchive::new(Cursor::new(buf.clone())).is_ok());
    let dir = |name: &str, icb, files, subdirs| DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: icb + 1,
        files,
        subdirs,
    };
    let jar_dir = dir(
        "JAR",
        30,
        vec![file_with("00000.jar", 32, 4000, buf, true)],
        vec![],
    );
    let root = dir("", 10, vec![], vec![dir("BDMV", 20, vec![], vec![jar_dir])]);
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    assert!(super::super::dbp::detect(&mut disc, &udf));
}

// High-ratio deflate entries (each under the per-entry cap) stop being offered
// past the per-parse cap.
#[test]
fn try_each_class_bounds_total_inflation() {
    use std::io::Write as _;
    let mut payload = MINIMAL_CLASS.to_vec();
    payload.extend(std::iter::repeat_n(0u8, 50 * 1024 * 1024));
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for n in ["A.class", "B.class", "C.class"] {
            w.start_file(n, opts).expect("start_file");
            w.write_all(&payload).expect("write");
        }
        w.finish().expect("finish");
    }
    let mut jar = open(buf);
    let mut visited = 0usize;
    let mut budget = PARSE_INFLATE_BUDGET;
    for_each_class_budgeted(&mut jar, &mut budget, |_, _| visited += 1);
    assert_eq!(visited, 2, "3 x 50 MiB exceeds the 128 MiB inflate budget");
}
