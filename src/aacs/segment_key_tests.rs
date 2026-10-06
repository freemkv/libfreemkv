use super::*;

/// Build a container with `record_size` and the given `index_space`, filling
/// each record with a distinguishable byte so lookups can be checked.
fn build(index_space: u16, record_size: u16) -> Vec<u8> {
    let count = if index_space == 0xffff {
        0x1_0000
    } else {
        index_space as usize
    };
    let mut v = Vec::with_capacity(HEADER_LEN + count * record_size as usize);
    v.extend_from_slice(&0x0100_0000u32.to_be_bytes()); // tag
    v.extend_from_slice(&index_space.to_be_bytes());
    v.extend_from_slice(&record_size.to_be_bytes());
    for i in 0..count {
        let mut rec = vec![(i & 0xff) as u8; record_size as usize];
        // sub-header, as seen on disc
        rec[..8].copy_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x20, 0x01, 0x02]);
        v.extend_from_slice(&rec);
    }
    v
}

#[test]
fn parses_retail_container_geometry() {
    // The real disc: 0xffff index space, 536-byte records, 35,127,304 total.
    let data = build(0xffff, 536);
    assert_eq!(
        data.len(),
        35_127_304,
        "matches the retail file size exactly"
    );
    let t = SegmentKeyTable::parse(&data).expect("parse");
    assert_eq!(t.record_count(), 65_536);
    assert_eq!(t.record_size(), 536);
    let rec = t.record(0x1234).expect("record");
    assert_eq!(rec.len(), 536);
    assert_eq!(rec[8], 0x34, "record 0x1234 carries its own fixture tag");
    assert_eq!(&rec[..8], &[0x01, 0x00, 0x00, 0x00, 0x00, 0x20, 0x01, 0x02]);
    assert_eq!(t.record_payload(0x1234).unwrap().len(), 528);
}

#[test]
fn small_index_space_bounds_lookups() {
    let data = build(4, 32);
    let t = SegmentKeyTable::parse(&data).expect("parse");
    assert_eq!(t.record_count(), 4);
    assert!(t.record(3).is_some());
    assert!(t.record(4).is_none(), "selector past the table is None");
}

#[test]
fn rejects_size_mismatch_and_truncation() {
    assert!(SegmentKeyTable::parse(&[0u8; 4]).is_none());
    let mut data = build(4, 32);
    data.truncate(data.len() - 1); // body no longer matches header
    assert!(SegmentKeyTable::parse(&data).is_none());
}
