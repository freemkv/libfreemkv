use super::*;

/// Build a table with the real on-disc layout: 8-byte header + N 16-byte
/// records. `recs` are `(index, start_spn, end_spn)`.
fn build_tbl(recs: &[(u16, u32, u32)]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&0x0100_0000u32.to_be_bytes()); // type
    v.extend_from_slice(&(recs.len() as u16).to_be_bytes()); // count
    v.extend_from_slice(&(SEGMENT_RECORD_LEN as u16).to_be_bytes()); // record_size
    for &(n, s, e) in recs {
        v.extend_from_slice(&0x0100_0000u32.to_be_bytes()); // marker
        v.extend_from_slice(&n.to_be_bytes());
        v.extend_from_slice(&1u16.to_be_bytes()); // flag
        v.extend_from_slice(&s.to_be_bytes());
        v.extend_from_slice(&e.to_be_bytes());
    }
    v
}

#[test]
fn fmts_key_ranges_maps_segments_to_lba_by_index() {
    use crate::disc::Extent;
    // One big clip extent starting at LBA 1000. Clip byte B lives at
    // LBA 1000 + B/2048.
    let extents = vec![Extent {
        start_lba: 1000,
        sector_count: 1_000_000,
    }];
    // Two segments, indexes 5 and 7 (spn ranges as on a real disc).
    let segs = vec![
        Segment {
            index: 5,
            start_spn: 100,
            end_spn: 199,
        },
        Segment {
            index: 7,
            start_spn: 10_000,
            end_spn: 10_099,
        },
    ];
    // Pool layout [base, idx1, idx2, …] → index N uses key slot N.
    let ranges = fmts_key_ranges(&segs, &extents, &|v| v as usize);
    assert_eq!(ranges.len(), 2, "one LBA range per segment");
    // Segment 0: spn 100..=199 → clip bytes [19200, 38400) → sectors 9..=18
    // → LBA 1009..1019, key index 5.
    assert_eq!(ranges[0], (1009, 1019, 5));
    // Segment 1: spn 10000..=10099 → bytes [1_920_000, 1_939_200) →
    // sectors 937..=946 → LBA 1937..1947, key index 7.
    assert_eq!(ranges[1], (1937, 1947, 7));

    // The ranges drive a positive AacsKeyMap: an LBA in no range has no key.
    let map = crate::decrypt::AacsKeyMap::from_ranges(ranges);
    assert_eq!(map.key_idx_for(500), None, "outside any segment → no key");
    assert_eq!(
        map.key_idx_for(1012),
        Some(5),
        "inside index-5 segment → key 5"
    );
    assert_eq!(
        map.key_idx_for(1940),
        Some(7),
        "inside index-7 segment → key 7"
    );
    assert_eq!(
        map.key_idx_for(1019),
        None,
        "segment end is exclusive → no key"
    );
}

#[test]
fn fmts_key_ranges_carries_a_non_2048_aligned_segment() {
    use crate::disc::Extent;
    let extents = vec![Extent {
        start_lba: 1000,
        sector_count: 1_000_000,
    }];
    // start_spn=1 → start_byte=192 (NOT 2048-aligned); end_spn=10 → end_byte-1=2111.
    // Correct carry floor(2111/2048)-floor(192/2048)=1 maps to LBA 1000..=1001; the
    // old aligned (2111-192)/2048=0 mismatched b-a=1 and DROPPED it to the Unit Key (empty map).
    let segs = vec![Segment {
        index: 5,
        start_spn: 1,
        end_spn: 10,
    }];
    let ranges = fmts_key_ranges(&segs, &extents, &|v| v as usize);
    assert_eq!(
        ranges.len(),
        1,
        "a non-2048-aligned segment must still carry, not be dropped"
    );
    assert_eq!(ranges[0], (1000, 1002, 5));
}

#[test]
fn fmts_key_ranges_skips_inverted_segment_without_underflow() {
    use crate::disc::Extent;
    let extents = vec![Extent {
        start_lba: 1000,
        sector_count: 1_000_000,
    }];
    // start_spn == end_spn + 1: `end_byte - 1 - start_byte` would underflow.
    // The record must be skipped rather than panic (debug) / wrap (release).
    let segs = vec![Segment {
        index: 5,
        start_spn: 200,
        end_spn: 199,
    }];
    let ranges = fmts_key_ranges(&segs, &extents, &|v| v as usize);
    assert!(ranges.is_empty(), "inverted segment yields no range");
}

#[test]
fn fmts_key_ranges_skips_a_segment_straddling_non_adjacent_extents() {
    use crate::disc::Extent;
    // Clip bytes [0, 20480) at LBA 100.., [20480, 40960) at LBA 500.. (or reversed).
    // Packets 104..=109 span bytes 19968..21119, crossing the extent boundary.
    let segs = vec![Segment {
        index: 5,
        start_spn: 104,
        end_spn: 109,
    }];
    for (first, second) in [(100, 500), (500, 100)] {
        let extents = vec![
            Extent {
                start_lba: first,
                sector_count: 10,
            },
            Extent {
                start_lba: second,
                sector_count: 10,
            },
        ];
        let ranges = fmts_key_ranges(&segs, &extents, &|v| v as usize);
        assert!(ranges.is_empty(), "{first}/{second}: {ranges:?}");
    }
}

#[test]
fn segment_edges_are_inclusive() {
    let seg = Segment {
        index: 1,
        start_spn: 100,
        end_spn: 200,
    };
    assert!(seg.contains_spn(100) && seg.contains_spn(200));
    assert!(!seg.contains_spn(99) && !seg.contains_spn(201));
    // A unit touching exactly one edge packet overlaps.
    assert!(seg.overlaps_spn(69, 100));
    assert!(seg.overlaps_spn(200, 231));
    assert!(!seg.overlaps_spn(68, 99));
    assert!(!seg.overlaps_spn(201, 232));
}

#[test]
fn clip_byte_to_lba_walks_extents() {
    use crate::disc::Extent;
    let extents = vec![
        Extent {
            start_lba: 100,
            sector_count: 10,
        }, // clip bytes [0, 20480)
        Extent {
            start_lba: 500,
            sector_count: 10,
        }, // clip bytes [20480, 40960)
    ];
    assert_eq!(clip_byte_to_lba(&extents, 0), Some(100));
    assert_eq!(clip_byte_to_lba(&extents, 2048), Some(101));
    assert_eq!(clip_byte_to_lba(&extents, 20480), Some(500)); // second extent
    assert_eq!(clip_byte_to_lba(&extents, 22528), Some(501));
    assert_eq!(clip_byte_to_lba(&extents, 40960), None); // past the clip
}

#[test]
fn parses_real_disc_layout() {
    // First three records observed on retail 2.1: the variant
    // field counts 1,2,3,… (it wraps at 32 further into the table — see
    // `index_field_cycles_one_to_thirty_two`), segments are 2560 packets.
    let tbl = build_tbl(&[
        (1, 343680, 346239),
        (2, 695616, 698175),
        (3, 1051840, 1054399),
    ]);
    let segs = parse_individual_segments(&tbl).expect("parse");
    assert_eq!(segs.len(), 3);
    assert_eq!(segs[0].index, 1);
    assert_eq!(segs[1].index, 2);
    assert_eq!(segs[2].index, 3);
    assert_eq!(segs[0].start_spn, 343680);
    assert_eq!(segs[0].end_spn, 346239);
    assert_eq!(segs[0].packet_count(), 2560);
    assert_eq!(segs[0].byte_len(), 2560 * 192);
    assert_eq!(segs[0].start_byte(), 343680 * 192);
    assert!(segs[0].contains_spn(345000));
    assert!(!segs[0].contains_spn(343679));
    assert!(!segs[0].contains_spn(346240));
}

#[test]
fn rejects_wrong_record_size() {
    let mut tbl = build_tbl(&[(1, 0, 10)]);
    tbl[6..8].copy_from_slice(&20u16.to_be_bytes()); // record_size != 16
    assert!(parse_individual_segments(&tbl).is_none());
}

#[test]
fn rejects_truncated_and_overrun() {
    assert!(parse_individual_segments(&[0u8; 4]).is_none()); // < header
    let mut tbl = build_tbl(&[(1, 0, 10)]);
    tbl[4..6].copy_from_slice(&99u16.to_be_bytes()); // claims 99 recs, has 1
    assert!(parse_individual_segments(&tbl).is_none());
}

#[test]
fn empty_table_is_empty_not_none() {
    let tbl = build_tbl(&[]);
    assert_eq!(parse_individual_segments(&tbl), Some(Vec::new()));
}

#[test]
fn packets_per_unit_is_thirty_two() {
    // 6144-byte aligned unit / 192-byte source packet.
    assert_eq!(PACKETS_PER_UNIT, 32);
}

#[test]
fn unit_inside_segment_routes_to_index() {
    // A real first-record segment: packets [343680, 346239].
    let segs = parse_individual_segments(&build_tbl(&[(1, 343680, 346239)])).unwrap();
    // A unit sitting squarely inside: start at packet 344000 → byte 344000*192.
    let off = 344000u64 * SOURCE_PACKET_LEN;
    let hit = segment_for_unit(&segs, off).expect("inside the segment");
    assert_eq!(hit.index, 1);
}

#[test]
fn index_field_cycles_one_to_thirty_two() {
    // Reality on a retail 2.1 disc: field@4 is the index, cycling 1..=32 in file
    // order (NOT a sequential segment id). Reproduce one-and-a-bit cycles.
    let mut recs = Vec::new();
    let mut spn = 1000u32;
    for row in 0..2 {
        for v in 1..=32u16 {
            recs.push((v, spn, spn + 2559));
            spn += 50_000; // ~one segment every ~67 MB
        }
        let _ = row;
    }
    let segs = parse_individual_segments(&build_tbl(&recs)).unwrap();
    assert_eq!(segs.len(), 64);
    assert_eq!(segs[31].index, 32); // end of first cycle
    assert_eq!(segs[32].index, 1); // wraps, does not become 33
    assert!(segs.iter().all(|s| (1..=32).contains(&s.index)));
}

#[test]
fn unit_outside_every_segment_is_unit_key_miss() {
    let segs = parse_individual_segments(&build_tbl(&[(1, 343680, 346239)])).unwrap();
    // A unit well before the segment is ordinary content → None (unit-key path).
    let off = 1000u64 * SOURCE_PACKET_LEN;
    assert!(segment_for_unit(&segs, off).is_none());
}

#[test]
fn unit_straddling_a_segment_edge_counts_as_forensic() {
    // Segment starts at packet 100. A unit that ENDS just inside it (its 32
    // packets straddle the boundary) must still route to the index key,
    // because part of its ciphertext is forensic-encrypted.
    let segs = parse_individual_segments(&build_tbl(&[(7, 100, 200)])).unwrap();
    // Unit covering packets [80, 111]: overlaps [100,200] at the tail.
    let off = 80u64 * SOURCE_PACKET_LEN;
    let hit = segment_for_unit(&segs, off).expect("straddles the start edge");
    assert_eq!(hit.index, 7);
    // A unit ending exactly at packet 99 (offset s.t. last = 99) does NOT overlap.
    let before = 68u64 * SOURCE_PACKET_LEN; // [68, 99]
    assert!(segment_for_unit(&segs, before).is_none());
}

#[test]
fn no_segments_never_routes_to_index() {
    // The 1.0 / 2.0 case: no forensic map, so every miss is a unit-key miss.
    assert!(segment_for_unit(&[], lba_byte_offset(0)).is_none());
    assert!(segment_for_unit(&[], lba_byte_offset(9_999_999)).is_none());
}

#[test]
fn lba_maps_to_the_packet_grid() {
    // A unit is 3 sectors (6144 bytes) = 32 packets. Clip-relative LBA 3 is
    // the second aligned unit, which starts at packet 32.
    let off = lba_byte_offset(3);
    assert_eq!(off / SOURCE_PACKET_LEN, 32);
}
