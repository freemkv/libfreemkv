//! AACS 2.1 FMTS forensic segment map — `AACS/IndividualSegment.tbl`.
//!
//! An FMTS main feature interleaves short forensic **segments**, each with an
//! **index** (1..32) selecting one of 32 forensic index keys to decrypt that
//! segment's units in place of the ordinary CPS Unit Key. This table says
//! WHERE the segments live and which index each carries.
//!
//! Format (validated against a retail AACS 2.1 disc):
//! ```text
//!   header (8 bytes):  u32 type | u16 count | u16 record_size (= 16)
//!   record[count] (16 bytes each):
//!     u32 marker (= 0x01000000) | u16 index | u16 flag (= 1)
//!     u32 start_spn | u32 end_spn        (source-packet numbers, inclusive)
//! ```

/// Fixed size of one `IndividualSegment.tbl` record.
pub const SEGMENT_RECORD_LEN: usize = 16;
/// Bytes per BDAV source packet (188-byte TS + 4-byte arrival-time header).
pub const SOURCE_PACKET_LEN: u64 = crate::consts::BD_SOURCE_PACKET_BYTES as u64;

/// One forensic segment: the inclusive source-packet range it occupies in the
/// FMTS clip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// Forensic index tag, 1..=32 (field@4 of the record). Cycles across the
    /// table rather than counting up — it selects WHICH of the 32 index keys
    /// decrypts this range. (`0` is not used here; the default/non-forensic
    /// content carries no segment record at all.)
    pub index: u16,
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
    lba as u64 * crate::consts::SECTOR_BYTES_U64
}

/// The forensic segment an AACS aligned unit belongs to, if any, given the
/// unit's clip-relative byte offset.
///
/// A unit overlapping a segment must be opened with that segment's index key (selected by
/// `index`), not the CPS Unit Key.
///
/// Tested as a packet *span* (`[off/192, (off+6144-1)/192]`) so a unit that
/// only partly overlaps a segment edge still counts as forensic.
pub fn segment_for_unit(segments: &[Segment], unit_offset: u64) -> Option<&Segment> {
    let unit_len = crate::aacs::content::ALIGNED_UNIT_LEN as u64;
    let first = (unit_offset / SOURCE_PACKET_LEN) as u32;
    let last = ((unit_offset + unit_len - 1) / SOURCE_PACKET_LEN) as u32;
    segments.iter().find(|s| s.overlaps_spn(first, last))
}

/// Parse `IndividualSegment.tbl` into its forensic segments, in table
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
        // o+4..o+8 = index (u16, 1..32) + flag (u16); o+8..o+16 = start/end SPN.
        let index = u16::from_be_bytes([tbl[o + 4], tbl[o + 5]]);
        let start_spn = u32::from_be_bytes([tbl[o + 8], tbl[o + 9], tbl[o + 10], tbl[o + 11]]);
        let end_spn = u32::from_be_bytes([tbl[o + 12], tbl[o + 13], tbl[o + 14], tbl[o + 15]]);
        segments.push(Segment {
            index,
            start_spn,
            end_spn,
        });
    }
    Some(segments)
}

/// Map a clip-relative byte offset to the absolute LBA that holds it, by walking
/// the title's extents (the `.fmts` clip's sectors in file order). Segment
/// offsets in [`Segment`] are clip-relative source-packet numbers, so this is how
/// a segment's `spn` range becomes disc LBAs. `None` if the offset is past the
/// clip.
pub fn clip_byte_to_lba(extents: &[crate::disc::Extent], clip_byte: u64) -> Option<u32> {
    let mut cum = 0u64;
    for e in extents {
        let len = e.sector_count as u64 * crate::consts::SECTOR_BYTES as u64;
        if clip_byte < cum + len {
            let sector_in_ext = ((clip_byte - cum) / crate::consts::SECTOR_BYTES as u64) as u32;
            return Some(e.start_lba.saturating_add(sector_in_ext));
        }
        cum += len;
    }
    None
}

/// Build the `[start_lba, end_lba) → key_idx` ranges for an FMTS forensic key map.
///
/// Each forensic segment's clip-relative source-packet span becomes an absolute LBA range
/// tagged with the key its `index` selects (via `index_to_key_idx`). Ranges outside every
/// segment are left for the map's default (the Unit Key). A segment straddling a UDF extent
/// boundary is skipped rather than emitting a wrong span.
///
/// Not used by the live FMTS path (`keys::fmts` + `mux::resolve`); kept as a public
/// helper for callers that build their own key map from the segment table.
#[doc(hidden)]
pub fn fmts_key_ranges(
    segments: &[Segment],
    extents: &[crate::disc::Extent],
    index_to_key_idx: &dyn Fn(u16) -> usize,
) -> Vec<(u32, u32, usize)> {
    let mut ranges = Vec::new();
    for s in segments {
        // SPNs are untrusted (from IndividualSegment.tbl); an inverted record
        // (start_spn > end_spn) would underflow `end_byte - 1 - start_byte` below.
        if s.start_spn > s.end_spn {
            continue;
        }
        let start_byte = s.start_spn as u64 * SOURCE_PACKET_LEN;
        let end_byte = (s.end_spn as u64 + 1) * SOURCE_PACKET_LEN; // exclusive
        // Map first/last sector to LBAs. Segments (~480 KB) rarely cross the
        // GB-sized extent boundary, but if the two ends land in different
        // extents, skip rather than emit a wrong span (falls back to Unit Key).
        let (Some(a), Some(b)) = (
            clip_byte_to_lba(extents, start_byte),
            clip_byte_to_lba(extents, end_byte - 1),
        ) else {
            continue;
        };
        // start_byte = start_spn*192 is generally NOT 2048-aligned, so the LBA carry
        // is floor((end_byte-1)/2048) - floor(start_byte/2048); the naive (end_byte-1
        // -start_byte)/2048 matches `b-a` only when sector-aligned, else wrongly drops the non-aligned segment back to the Unit Key.
        let sector = crate::consts::SECTOR_BYTES as u64;
        let expected_delta = (end_byte - 1) / sector - start_byte / sector;
        if b >= a && (b - a) as u64 == expected_delta {
            ranges.push((a, b + 1, index_to_key_idx(s.index)));
        }
    }
    ranges
}

#[cfg(test)]
#[path = "segment_tests.rs"]
mod tests;
