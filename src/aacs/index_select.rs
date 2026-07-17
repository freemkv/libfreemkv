//! FMTS index selection — the pure decode-time decision for a 2.1 disc.
//!
//! A 2.1 disc resolves to exactly one forensic index (1..=32) for a given
//! rip. `IndividualSegment.tbl` tags each forensic segment with an index (see
//! [`super::segment`]); the decode keeps the segments matching our index,
//! drops the other 31, and treats everything outside a segment as ordinary
//! (index-0) content. This module owns that classification and nothing else —
//! no I/O, no keys, no cipher — so it is fully testable in isolation. The
//! decrypt pipeline consumes the [`UnitDisposition`] it returns.
//!
//! Where the resolved index comes from is a separate concern
//! ([`resolve_disc_index`]): today it is read off the index keys the key
//! source handed us; when Processing Keys are available it will come from the
//! VK derivation instead. Either way the disposition logic below is identical.

use super::segment::{Segment, segment_for_unit};
use super::types::UnitKey;

/// What the decode should do with one AACS aligned unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitDisposition {
    /// Outside every forensic segment: ordinary content, decrypt with the
    /// default (index-0) unit key.
    Default,
    /// Inside a forensic segment tagged with OUR resolved index: decrypt with
    /// that index's key.
    Index(u8),
    /// Inside a forensic segment tagged with a DIFFERENT index: not our
    /// watermark, so it is not part of our output — drop it.
    DropForeignIndex(u8),
    /// Inside a forensic segment but no index key is held (the disc's index
    /// was never resolved): the segment cannot be decoded, so it is concealed
    /// as loss. Carries the segment's index for diagnostics.
    ForensicNoKey(u8),
}

/// Resolve the disc's single forensic index from the keys we hold.
///
/// Scans for an index key (`index_number` in `1..=32`) and returns its
/// index. `None` when only default (index-0) keys are held — i.e. no
/// index source answered, so forensic segments are not decodable. A disc has
/// exactly one index, so the first non-zero key decides; if several distinct
/// index keys were somehow supplied the lowest wins (deterministic), which is
/// only a defensive tiebreak — the probe/derivation yields one.
pub fn resolve_disc_index(unit_keys: &[UnitKey]) -> Option<u8> {
    unit_keys
        .iter()
        .map(|k| k.index_number)
        .filter(|&v| v != 0)
        .min()
}

/// Classify the AACS aligned unit at `unit_offset` (clip-relative bytes) given
/// the forensic segment map and the disc's resolved index (`None` if no
/// index key is held).
pub fn unit_disposition(
    unit_offset: u64,
    segments: &[Segment],
    disc_index: Option<u8>,
) -> UnitDisposition {
    match segment_for_unit(segments, unit_offset) {
        // Not in any forensic segment → ordinary content.
        None => UnitDisposition::Default,
        // In a forensic segment → decide by whether it is our index.
        Some(seg) => {
            let seg_index = seg.index as u8;
            match disc_index {
                Some(v) if v == seg_index => UnitDisposition::Index(v),
                Some(_) => UnitDisposition::DropForeignIndex(seg_index),
                None => UnitDisposition::ForensicNoKey(seg_index),
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn straddling_unit_still_classified_as_its_segment() {
        // A unit whose 32-packet span only tails into the segment still routes
        // to the segment (matches segment_for_unit's span test).
        let segs = tbl(&[(5, 100, 200)]);
        let unit_packets = (ALIGNED_UNIT_LEN as u64 / SOURCE_PACKET_LEN) as u32; // 32
        // Start so the unit covers [80, 80+31] = [80, 111]: overlaps at 100.
        let off = 80u64 * SOURCE_PACKET_LEN;
        assert!(80 + unit_packets - 1 >= 100, "sanity: unit tails into seg");
        assert_eq!(
            unit_disposition(off, &segs, Some(5)),
            UnitDisposition::Index(5)
        );
    }
}
