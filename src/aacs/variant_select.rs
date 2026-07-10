//! FMTS variant selection — the pure decode-time decision for a 2.1 disc.
//!
//! A 2.1 disc resolves to exactly one forensic variant (1..=32) for a given
//! rip. `IndividualSegment.tbl` tags each forensic segment with a variant (see
//! [`super::segment`]); the decode keeps the segments matching our variant,
//! drops the other 31, and treats everything outside a segment as ordinary
//! (variant-0) content. This module owns that classification and nothing else —
//! no I/O, no keys, no cipher — so it is fully testable in isolation. The
//! decrypt pipeline consumes the [`UnitDisposition`] it returns.
//!
//! Where the resolved variant comes from is a separate concern
//! ([`resolve_disc_variant`]): today it is read off the variant keys the key
//! source handed us; when Processing Keys are available it will come from the
//! VK derivation instead. Either way the disposition logic below is identical.

use super::segment::{Segment, variant_segment_for_unit};
use super::types::UnitKey;

/// What the decode should do with one AACS aligned unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitDisposition {
    /// Outside every forensic segment: ordinary content, decrypt with the
    /// default (variant-0) unit key.
    Default,
    /// Inside a forensic segment tagged with OUR resolved variant: decrypt with
    /// that variant's key.
    Variant(u8),
    /// Inside a forensic segment tagged with a DIFFERENT variant: not our
    /// watermark, so it is not part of our output — drop it.
    DropForeignVariant(u8),
    /// Inside a forensic segment but no variant key is held (the disc's variant
    /// was never resolved): the segment cannot be decoded, so it is concealed
    /// as loss. Carries the segment's variant for diagnostics.
    ForensicNoKey(u8),
}

/// Resolve the disc's single forensic variant from the keys we hold.
///
/// Scans for a variant key (`variant_number` in `1..=32`) and returns its
/// variant. `None` when only default (variant-0) keys are held — i.e. no
/// variant source answered, so forensic segments are not decodable. A disc has
/// exactly one variant, so the first non-zero key decides; if several distinct
/// variant keys were somehow supplied the lowest wins (deterministic), which is
/// only a defensive tiebreak — the probe/derivation yields one.
pub fn resolve_disc_variant(unit_keys: &[UnitKey]) -> Option<u8> {
    unit_keys
        .iter()
        .map(|k| k.variant_number)
        .filter(|&v| v != 0)
        .min()
}

/// Classify the AACS aligned unit at `unit_offset` (clip-relative bytes) given
/// the forensic segment map and the disc's resolved variant (`None` if no
/// variant key is held).
pub fn unit_disposition(
    unit_offset: u64,
    segments: &[Segment],
    disc_variant: Option<u8>,
) -> UnitDisposition {
    match variant_segment_for_unit(segments, unit_offset) {
        // Not in any forensic segment → ordinary content.
        None => UnitDisposition::Default,
        // In a forensic segment → decide by whether it is our variant.
        Some(seg) => {
            let seg_variant = seg.variant as u8;
            match disc_variant {
                Some(v) if v == seg_variant => UnitDisposition::Variant(v),
                Some(_) => UnitDisposition::DropForeignVariant(seg_variant),
                None => UnitDisposition::ForensicNoKey(seg_variant),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::content::ALIGNED_UNIT_LEN;
    use crate::aacs::segment::{SOURCE_PACKET_LEN, parse_individual_segments};

    /// Build a one-record segment table (variant, start_spn, end_spn).
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

    fn uk(idx: u32, variant: u8) -> UnitKey {
        if variant == 0 {
            UnitKey::new(idx, [0u8; 16])
        } else {
            UnitKey::variant(idx, [variant; 16], variant)
        }
    }

    #[test]
    fn resolve_picks_the_single_variant_key() {
        // Default keys only → no variant resolved.
        assert_eq!(resolve_disc_variant(&[uk(0, 0)]), None);
        assert_eq!(resolve_disc_variant(&[]), None);
        // One variant key among defaults → that variant.
        assert_eq!(resolve_disc_variant(&[uk(0, 0), uk(1, 7)]), Some(7));
        // Defensive: lowest of several distinct variants (deterministic).
        assert_eq!(resolve_disc_variant(&[uk(0, 9), uk(1, 3)]), Some(3));
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
    fn unit_in_our_variant_decrypts() {
        let segs = tbl(&[(7, 100, 200)]);
        let off = 120u64 * SOURCE_PACKET_LEN;
        assert_eq!(
            unit_disposition(off, &segs, Some(7)),
            UnitDisposition::Variant(7)
        );
    }

    #[test]
    fn unit_in_foreign_variant_drops() {
        // Segment tagged variant 7, but our disc variant is 3 → drop it.
        let segs = tbl(&[(7, 100, 200)]);
        let off = 120u64 * SOURCE_PACKET_LEN;
        assert_eq!(
            unit_disposition(off, &segs, Some(3)),
            UnitDisposition::DropForeignVariant(7)
        );
    }

    #[test]
    fn forensic_unit_with_no_key_is_concealed() {
        // A forensic segment but we never resolved a variant → conceal as loss.
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
        // to the segment (matches variant_segment_for_unit's span test).
        let segs = tbl(&[(5, 100, 200)]);
        let unit_packets = (ALIGNED_UNIT_LEN as u64 / SOURCE_PACKET_LEN) as u32; // 32
        // Start so the unit covers [80, 80+31] = [80, 111]: overlaps at 100.
        let off = 80u64 * SOURCE_PACKET_LEN;
        assert!(80 + unit_packets - 1 >= 100, "sanity: unit tails into seg");
        assert_eq!(
            unit_disposition(off, &segs, Some(5)),
            UnitDisposition::Variant(5)
        );
    }
}
