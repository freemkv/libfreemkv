use super::*;
use crate::aacs::content::ALIGNED_UNIT_LEN;
use crate::aacs::segment::{SOURCE_PACKET_LEN, parse_individual_segments};

/// Build a one-record segment table (index, start_spn, end_spn).
fn tbl(recs: &[(u16, u32, u32)]) -> Vec<Segment> {
    let mut v = Vec::new();
    v.extend_from_slice(&0x0100_0000u32.to_be_bytes());
    v.extend_from_slice(&(recs.len() as u16).to_be_bytes());
    v.extend_from_slice(&16u16.to_be_bytes());
    for &(n, s, e) in recs {
        v.extend_from_slice(&0x0100_0000u32.to_be_bytes());
        v.extend_from_slice(&n.to_be_bytes());
        v.extend_from_slice(&1u16.to_be_bytes());
        v.extend_from_slice(&s.to_be_bytes());
        v.extend_from_slice(&e.to_be_bytes());
    }
    parse_individual_segments(&v).expect("parse")
}

fn uk(idx: u32, index: u8) -> UnitKey {
    if index == 0 {
        UnitKey::new(idx, [0u8; 16])
    } else {
        UnitKey::forensic(idx, [index; 16], index)
    }
}

#[test]
fn resolve_picks_the_single_index_key() {
    // Default keys only → no index resolved.
    assert_eq!(resolve_disc_index(&[uk(0, 0)]), None);
    assert_eq!(resolve_disc_index(&[]), None);
    // One index key among defaults → that index.
    assert_eq!(resolve_disc_index(&[uk(0, 0), uk(1, 7)]), Some(7));
    // Defensive: lowest of several distinct indexes (deterministic).
    assert_eq!(resolve_disc_index(&[uk(0, 9), uk(1, 3)]), Some(3));
}

#[test]
fn unit_outside_segments_is_default() {
    let segs = tbl(&[(1, 343680, 346239)]);
    let off = 1000u64 * SOURCE_PACKET_LEN; // well before the segment
    assert_eq!(
        unit_disposition(off, &segs, Some(1)),
        UnitDisposition::Default
    );
    // With no segments at all (1.0 / 2.0), everything is Default.
    assert_eq!(
        unit_disposition(off, &[], Some(1)),
        UnitDisposition::Default
    );
}

#[test]
fn unit_in_our_index_decrypts() {
    let segs = tbl(&[(7, 100, 200)]);
    let off = 120u64 * SOURCE_PACKET_LEN;
    assert_eq!(
        unit_disposition(off, &segs, Some(7)),
        UnitDisposition::Index(7)
    );
}

#[test]
fn unit_in_foreign_index_drops() {
    // Segment tagged index 7, but our disc index is 3 → drop it.
    let segs = tbl(&[(7, 100, 200)]);
    let off = 120u64 * SOURCE_PACKET_LEN;
    assert_eq!(
        unit_disposition(off, &segs, Some(3)),
        UnitDisposition::DropForeignIndex(7)
    );
}

#[test]
fn forensic_unit_with_no_key_is_concealed() {
    // A forensic segment but we never resolved an index → conceal as loss.
    let segs = tbl(&[(7, 100, 200)]);
    let off = 120u64 * SOURCE_PACKET_LEN;
    assert_eq!(
        unit_disposition(off, &segs, None),
        UnitDisposition::ForensicNoKey(7)
    );
}

#[test]
fn out_of_range_index_does_not_truncate_into_ours() {
    // Index 288 (0x0120) truncates to 32 in a u8, which would alias our
    // resolved index 32 under the old `as u8` compare; u16 compare must
    // classify it as foreign instead.
    let segs = tbl(&[(288, 100, 200)]);
    let off = 120u64 * SOURCE_PACKET_LEN;
    assert_eq!(
        unit_disposition(off, &segs, Some(32)),
        UnitDisposition::DropForeignIndex(255)
    );
}

#[test]
fn straddling_unit_still_classified_as_its_segment() {
    // A unit whose 32-packet span only tails into the segment still routes
    // to the segment (matches segment_for_unit's span test).
    let segs = tbl(&[(5, 100, 200)]);
    let unit_packets = (ALIGNED_UNIT_LEN as u64 / SOURCE_PACKET_LEN) as u32; // 32
    // Start so the unit covers [80, 80+31] = [80, 111]: overlaps at 100.
    let off = 80u64 * SOURCE_PACKET_LEN;
    assert!(80 + unit_packets > 100, "sanity: unit tails into seg");
    assert_eq!(
        unit_disposition(off, &segs, Some(5)),
        UnitDisposition::Index(5)
    );
}
