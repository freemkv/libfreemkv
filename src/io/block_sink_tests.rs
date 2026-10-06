use super::*;

#[test]
fn an_iso_sink_writes_sectors_where_addressed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.iso");
    let mut sink = open_block_sink(&format!("iso://{}", path.display())).unwrap();
    sink.write_at(0, &[1u8; 2048]).unwrap();
    sink.write_at(2, &[3u8; 2048]).unwrap();
    sink.write_at(1, &[2u8; 2048]).unwrap();
    assert_eq!(sink.finish(Finish::Complete).unwrap(), 3 * 2048);
    let got = std::fs::read(&path).unwrap();
    assert_eq!(got.len(), 3 * 2048);
    assert!(got[..2048].iter().all(|&b| b == 1));
    assert!(got[2048..4096].iter().all(|&b| b == 2));
    assert!(got[4096..].iter().all(|&b| b == 3));
}

#[test]
fn null_discards_and_counts() {
    let mut sink = open_block_sink("null://").unwrap();
    sink.write_at(7, &[0u8; 4096]).unwrap();
    assert_eq!(sink.finish(Finish::Complete).unwrap(), 4096);
}

#[test]
fn a_pes_scheme_is_not_a_block_sink() {
    assert!(open_block_sink("mkv:///tmp/x.mkv").is_err());
}

#[test]
fn the_null_device_is_recognised() {
    assert!(is_null_device(null_device()));
    assert!(is_null_device(Path::new("/dev/null")));
    assert!(!is_null_device(Path::new("/tmp/x.iso")));
}
