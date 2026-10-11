use super::*;
use crate::bdplus::table::tests::{encode, entry, seg};
use crate::bdplus::{ClipTable, MAX_TABLE_BYTES};
use crate::error::E_BDPLUS_TABLE_INVALID;

const HASH: &str = "f464ecd729172694cf0964bf17b4b6ab343b77ce";

fn table(clip: u32) -> ConversionTable {
    ConversionTable {
        clips: vec![ClipTable {
            clip,
            segments: vec![seg(vec![entry(1, 0x40, 0, 1, 9, 9, 1, 2)])],
        }],
    }
}

fn id(hash: &str, vid: Option<[u8; 16]>, mk: Option<[u8; 16]>) -> BdPlusDiscId {
    BdPlusDiscId {
        disc_hash: hash.into(),
        volume_id: vid,
        media_key: mk,
    }
}

#[test]
fn no_tables_resolves_nothing() {
    assert_eq!(NoTables.lookup(&id(HASH, None, None)).unwrap(), None);
    assert_eq!(NoTables.name(), "none");
}

#[test]
fn a_table_named_by_the_disc_hash_is_found() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(format!("{HASH}.bin")), encode(&table(1))).unwrap();
    let src = TableDir::new(dir.path());
    assert_eq!(src.lookup(&id(HASH, None, None)).unwrap(), Some(table(1)));
    assert_eq!(src.name(), "dir");
    let other = "00".repeat(20);
    assert_eq!(src.lookup(&id(&other, None, None)).unwrap(), None);
}

#[test]
fn an_uppercase_name_is_found() {
    let dir = tempfile::tempdir().unwrap();
    let upper = HASH.to_ascii_uppercase();
    std::fs::write(dir.path().join(format!("{upper}.bin")), encode(&table(2))).unwrap();
    let got = TableDir::new(dir.path())
        .lookup(&id(HASH, None, None))
        .unwrap();
    assert_eq!(got, Some(table(2)));
}

#[test]
fn libbdplus_cache_layouts_are_found_by_volume_id_and_media_key() {
    let dir = tempfile::tempdir().unwrap();
    let vid = [0xAB; 16];
    let mk = [0x01; 16];
    std::fs::create_dir_all(dir.path().join("ab".repeat(16))).unwrap();
    std::fs::write(
        dir.path().join("ab".repeat(16)).join("convtab.bin"),
        encode(&table(3)),
    )
    .unwrap();
    std::fs::write(
        dir.path().join(format!("{}.bin", "01".repeat(16))),
        encode(&table(4)),
    )
    .unwrap();
    let src = TableDir::new(dir.path());
    assert_eq!(
        src.lookup(&id("", Some(vid), None)).unwrap(),
        Some(table(3))
    );
    assert_eq!(src.lookup(&id("", None, Some(mk))).unwrap(), Some(table(4)));
    // The Volume ID layout wins over the media key; the disc hash over both.
    assert_eq!(
        src.lookup(&id("", Some(vid), Some(mk))).unwrap(),
        Some(table(3))
    );
    std::fs::write(dir.path().join(format!("{HASH}.bin")), encode(&table(5))).unwrap();
    assert_eq!(
        src.lookup(&id(HASH, Some(vid), Some(mk))).unwrap(),
        Some(table(5))
    );
}

#[test]
fn a_corrupt_or_oversized_table_is_an_error_not_a_miss() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{HASH}.bin"));
    std::fs::write(&path, [0, 1, 0]).unwrap();
    let src = TableDir::new(dir.path());
    assert_eq!(
        src.lookup(&id(HASH, None, None)).unwrap_err().code(),
        E_BDPLUS_TABLE_INVALID
    );
    std::fs::write(&path, vec![0u8; MAX_TABLE_BYTES + 1]).unwrap();
    assert_eq!(
        src.lookup(&id(HASH, None, None)).unwrap_err().code(),
        E_BDPLUS_TABLE_INVALID
    );
}

#[test]
fn a_malformed_disc_hash_names_no_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("..bin"), encode(&table(1))).unwrap();
    let src = TableDir::new(dir.path());
    assert_eq!(src.lookup(&id(".", None, None)).unwrap(), None);
    assert_eq!(src.lookup(&id("", None, None)).unwrap(), None);
}

#[test]
fn the_disc_id_comes_from_the_scan_and_redacts_key_material() {
    let mut disc = crate::bdplus::tests::bd_disc();
    disc.aacs = Some(
        crate::test_util::aacs_state()
            .disc_hash(format!("0x{}", HASH.to_ascii_uppercase()))
            .build(),
    );
    let id = BdPlusDiscId::of(&disc);
    assert_eq!(id.disc_hash, HASH);
    assert_eq!(id.volume_id, None, "an all-zero Volume ID was never read");
    disc.aacs = Some(crate::test_util::aacs_state().volume_id([7; 16]).build());
    let id = BdPlusDiscId::of(&disc).with_media_key([9; 16]);
    assert_eq!(id.volume_id, Some([7; 16]));
    let dbg = format!("{id:?}");
    assert!(!dbg.contains('7') && !dbg.contains('9'), "{dbg}");
}
