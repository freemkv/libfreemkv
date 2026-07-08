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
//!     u32 marker (= 0x01000000) | u16 segment_number | u16 flag (= 1)
//!     u32 start_spn | u32 end_spn        (source-packet numbers, inclusive)
//! ```
//! Source-packet numbers are the 192-byte BDAV packet index: byte offset =
//! `spn * 192`. Observed on one disc: 792 segments, each ~2560 packets
//! (~480 KB), spread across the entire 54 GB feature (one roughly every 67 MB).

/// Fixed size of one `IndividualSegment.tbl` record.
pub const SEGMENT_RECORD_LEN: usize = 16;
/// Bytes per BDAV source packet (188-byte TS + 4-byte arrival-time header).
pub const SOURCE_PACKET_LEN: u64 = 192;

/// One forensic variant segment: the inclusive source-packet range it occupies
/// in the FMTS clip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// 1-based segment number (table order).
    pub number: u16,
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
        // o+4..o+8 = segment_number (u16) + flag (u16); o+8..o+16 = start/end SPN.
        let number = u16::from_be_bytes([tbl[o + 4], tbl[o + 5]]);
        let start_spn = u32::from_be_bytes([tbl[o + 8], tbl[o + 9], tbl[o + 10], tbl[o + 11]]);
        let end_spn = u32::from_be_bytes([tbl[o + 12], tbl[o + 13], tbl[o + 14], tbl[o + 15]]);
        segments.push(Segment {
            number,
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
    /// records. `recs` are `(segment_number, start_spn, end_spn)`.
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
        // First three records observed on a retail 2.1 disc: 2560-packet segments.
        let tbl = build_tbl(&[
            (1, 343680, 346239),
            (2, 695616, 698175),
            (3, 1051840, 1054399),
        ]);
        let segs = parse_individual_segments(&tbl).expect("parse");
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[0].number, 1);
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
}
