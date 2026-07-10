//! AACS 2.1 FMTS forensic segment map — `AACS/IndividualSegment.tbl`.
//!
//! An FMTS main feature interleaves N "variant" segments — the sequence-key /
//! forensic-watermark mechanism. The same frames are authored as several
//! slightly different variants; each variant is encrypted under its own SEGMENT
//! key (from `SegmentKeyNNNNN.tbl`), NOT the CPS Unit Key. A player with the
//! right device keys can decrypt exactly one variant per segment, and which one
//! silently identifies the player (traitor tracing). Decrypting a variant
//! segment with the Unit Key yields garbage — broken HEVC reference frames
//! (empirically: `Could not find ref with POC …` on a plain unit-key rip).
//!
//! This table says WHERE the variant segments live so a decoder can decrypt
//! them with segment keys and select one coherent variant instead of muxing
//! unit-key garbage.
//!
//! Format (validated against a retail AACS 2.1 disc):
//! ```text
//!   header (8 bytes):  u32 type | u16 count | u16 record_size (= 16)
//!   record[count] (16 bytes each):
//!     u32 marker (= 0x01000000) | u16 variant | u16 flag (= 1)
//!     u32 start_spn | u32 end_spn        (source-packet numbers, inclusive)
//! ```
//! `variant` is the 1..32 forensic-variant tag, NOT a sequential segment id:
//! measured on a retail 2.1 disc (Zombieland) it cycles 1,2,…,32,1,2,… across
//! records in file order — 24 full cycles of 32 plus a final partial cycle of
//! 24 = 792 records. Source-packet numbers are the 192-byte BDAV packet index:
//! byte offset = `spn * 192`. Each segment is ~2560 packets (~480 KB), spread
//! across the entire 54 GB feature (one roughly every 67 MB).

/// Fixed size of one `IndividualSegment.tbl` record.
pub const SEGMENT_RECORD_LEN: usize = 16;
/// Bytes per BDAV source packet (188-byte TS + 4-byte arrival-time header).
pub const SOURCE_PACKET_LEN: u64 = 192;

/// Whether a 2.1 (FMTS) disc may rip WITHOUT segment (variant) keys.
///
/// `true` (today): the forensic variant segments are skipped as expected loss
/// and the bulk of the title decodes with the unit key, so a 2.1 disc rips
/// mostly-complete. A unit key (VUK) is still required, exactly as for any AACS
/// disc. `false`: the absence of a segment-key source is a hard, UPFRONT failure
/// ([`Error::FmtsKeyMissing`]) — the same policy as a missing unit key, so a
/// forensic-holed rip is refused rather than produced. No segment-key source
/// exists yet, so `true` is the only value under which a 2.1 disc rips at all;
/// flip to `false` once segment keys can be sourced and a partial rip should be
/// refused. Hardcoded on purpose — not a user setting.
///
/// [`Error::FmtsKeyMissing`]: crate::error::Error::FmtsKeyMissing
pub const BYPASS_FMTS_KEY: bool = true;

/// One forensic variant segment: the inclusive source-packet range it occupies
/// in the FMTS clip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// Forensic variant tag, 1..=32 (field@4 of the record). Cycles across the
    /// table rather than counting up — it selects WHICH variant this range is,
    /// which is what a variant-keyed decode routes on. (`0` is not used here;
    /// the default/non-forensic content carries no segment record at all.)
    pub variant: u16,
    /// First source packet of the segment (inclusive).
    pub start_spn: u32,
    /// Last source packet of the segment (inclusive).
    pub end_spn: u32,
}

impl Segment {
    /// Source-packet count in this (inclusive) segment.
    pub fn packet_count(&self) -> u32 {
        self.end_spn
            .saturating_sub(self.start_spn)
            .saturating_add(1)
    }

    /// Byte offset of the segment start within the clip (`start_spn * 192`).
    pub fn start_byte(&self) -> u64 {
        self.start_spn as u64 * SOURCE_PACKET_LEN
    }

    /// Byte length of the segment (`packet_count * 192`).
    pub fn byte_len(&self) -> u64 {
        self.packet_count() as u64 * SOURCE_PACKET_LEN
    }

    /// True when source packet `spn` falls inside this segment.
    pub fn contains_spn(&self, spn: u32) -> bool {
        spn >= self.start_spn && spn <= self.end_spn
    }

    /// True when the inclusive source-packet span `[first, last]` overlaps this
    /// segment. Used to decide whether an aligned unit (which spans several
    /// packets) touches the segment at all, not just whether one packet does.
    pub fn overlaps_spn(&self, first: u32, last: u32) -> bool {
        first <= self.end_spn && last >= self.start_spn
    }
}

/// Source packets spanned by one AACS aligned unit: `6144 / 192 = 32`.
pub const PACKETS_PER_UNIT: u32 =
    (crate::aacs::content::ALIGNED_UNIT_LEN as u64 / SOURCE_PACKET_LEN) as u32;

/// Byte offset within the clip of a clip-relative 2048-byte sector `lba`. The
/// FMTS decode reads the clip file directly, so `lba` 0 is the clip's first
/// byte and this offset lines up with the source-packet grid the segment map
/// uses.
pub fn lba_byte_offset(lba: u32) -> u64 {
    lba as u64 * 2048
}

/// The forensic segment an AACS aligned unit belongs to, if any, given the
/// unit's clip-relative byte offset.
///
/// This is the routing decision behind a 2.1 decrypt-miss: a unit that
/// overlaps a forensic segment must be opened with that segment's **variant
/// key** (from `SegmentKeyNNNNN.tbl`), not the CPS Unit Key. Opening it with
/// the Unit Key is exactly what yields the broken-reference-frame garbage a
/// plain unit-key rip produces. A unit outside every segment is ordinary
/// content and a miss on it is a Unit-Key miss, so this returns `None` and the
/// caller falls back to the normal unit-key fetch.
///
/// The unit is tested as a packet *span* (`[off/192, (off+6144-1)/192]`) so a
/// unit that only partly overlaps a segment edge is still classified as
/// variant; on the observed disc segments are unit-aligned, but the span test
/// does not rely on that.
pub fn variant_segment_for_unit(segments: &[Segment], unit_offset: u64) -> Option<&Segment> {
    let unit_len = crate::aacs::content::ALIGNED_UNIT_LEN as u64;
    let first = (unit_offset / SOURCE_PACKET_LEN) as u32;
    let last = ((unit_offset + unit_len - 1) / SOURCE_PACKET_LEN) as u32;
    segments.iter().find(|s| s.overlaps_spn(first, last))
}

/// Parse `IndividualSegment.tbl` into its forensic variant segments, in table
/// order. Returns `None` when the header is malformed, the record size is not
/// [`SEGMENT_RECORD_LEN`], or the declared record count overruns the buffer —
/// so a truncated / foreign table degrades to "no segment map" rather than
/// yielding bogus ranges.
pub fn parse_individual_segments(tbl: &[u8]) -> Option<Vec<Segment>> {
    if tbl.len() < 8 {
        return None;
    }
    let count = u16::from_be_bytes([tbl[4], tbl[5]]) as usize;
    let record_size = u16::from_be_bytes([tbl[6], tbl[7]]) as usize;
    if record_size != SEGMENT_RECORD_LEN {
        return None;
    }
    if 8usize.checked_add(count.checked_mul(record_size)?)? > tbl.len() {
        return None;
    }
    let mut segments = Vec::with_capacity(count);
    for i in 0..count {
        let o = 8 + i * record_size;
        // o+4..o+8 = variant (u16, 1..32) + flag (u16); o+8..o+16 = start/end SPN.
        let variant = u16::from_be_bytes([tbl[o + 4], tbl[o + 5]]);
        let start_spn = u32::from_be_bytes([tbl[o + 8], tbl[o + 9], tbl[o + 10], tbl[o + 11]]);
        let end_spn = u32::from_be_bytes([tbl[o + 12], tbl[o + 13], tbl[o + 14], tbl[o + 15]]);
        segments.push(Segment {
            variant,
            start_spn,
            end_spn,
        });
    }
    Some(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a table with the real on-disc layout: 8-byte header + N 16-byte
    /// records. `recs` are `(variant, start_spn, end_spn)`.
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
    fn parses_real_disc_layout() {
        // First three records observed on retail 2.1 (Zombieland): the variant
        // field counts 1,2,3,… (it wraps at 32 further into the table — see
        // `variant_field_cycles_one_to_thirty_two`), segments are 2560 packets.
        let tbl = build_tbl(&[
            (1, 343680, 346239),
            (2, 695616, 698175),
            (3, 1051840, 1054399),
        ]);
        let segs = parse_individual_segments(&tbl).expect("parse");
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[0].variant, 1);
        assert_eq!(segs[1].variant, 2);
        assert_eq!(segs[2].variant, 3);
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
    fn unit_inside_segment_routes_to_variant() {
        // A real first-record segment: packets [343680, 346239].
        let segs = parse_individual_segments(&build_tbl(&[(1, 343680, 346239)])).unwrap();
        // A unit sitting squarely inside: start at packet 344000 → byte 344000*192.
        let off = 344000u64 * SOURCE_PACKET_LEN;
        let hit = variant_segment_for_unit(&segs, off).expect("inside the segment");
        assert_eq!(hit.variant, 1);
    }

    #[test]
    fn variant_field_cycles_one_to_thirty_two() {
        // Reality on Zombieland: field@4 is the variant, cycling 1..=32 in file
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
        assert_eq!(segs[31].variant, 32); // end of first cycle
        assert_eq!(segs[32].variant, 1); // wraps, does not become 33
        assert!(segs.iter().all(|s| (1..=32).contains(&s.variant)));
    }

    #[test]
    fn unit_outside_every_segment_is_unit_key_miss() {
        let segs = parse_individual_segments(&build_tbl(&[(1, 343680, 346239)])).unwrap();
        // A unit well before the segment is ordinary content → None (unit-key path).
        let off = 1000u64 * SOURCE_PACKET_LEN;
        assert!(variant_segment_for_unit(&segs, off).is_none());
    }

    #[test]
    fn unit_straddling_a_segment_edge_counts_as_variant() {
        // Segment starts at packet 100. A unit that ENDS just inside it (its 32
        // packets straddle the boundary) must still route to the variant key,
        // because part of its ciphertext is variant-encrypted.
        let segs = parse_individual_segments(&build_tbl(&[(7, 100, 200)])).unwrap();
        // Unit covering packets [80, 111]: overlaps [100,200] at the tail.
        let off = 80u64 * SOURCE_PACKET_LEN;
        let hit = variant_segment_for_unit(&segs, off).expect("straddles the start edge");
        assert_eq!(hit.variant, 7);
        // A unit ending exactly at packet 99 (offset s.t. last = 99) does NOT overlap.
        let before = 68u64 * SOURCE_PACKET_LEN; // [68, 99]
        assert!(variant_segment_for_unit(&segs, before).is_none());
    }

    #[test]
    fn no_segments_never_routes_to_variant() {
        // The 1.0 / 2.0 case: no forensic map, so every miss is a unit-key miss.
        assert!(variant_segment_for_unit(&[], lba_byte_offset(0)).is_none());
        assert!(variant_segment_for_unit(&[], lba_byte_offset(9_999_999)).is_none());
    }

    #[test]
    fn lba_maps_to_the_packet_grid() {
        // A unit is 3 sectors (6144 bytes) = 32 packets. Clip-relative LBA 3 is
        // the second aligned unit, which starts at packet 32.
        let off = lba_byte_offset(3);
        assert_eq!(off / SOURCE_PACKET_LEN, 32);
    }
}
