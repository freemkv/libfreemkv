//! AACS 2.1 FMTS forensic segment keys, `AACS/SegmentKeyNNNNN.tbl`.
//!
//! One file per CPS unit (`SegmentKey00001.tbl`, ...). It is the on-disc key
//! store for the forensic variant segments mapped by [`super::segment`]. A
//! device does not read a segment key directly. It derives a **16-bit variant
//! selector** from the Media Key Variant chain (see [`super::variant`]) and uses
//! that selector to index this table, which is how the device's position in the
//! key tree decides which variant it can decrypt (the traitor-tracing link).
//!
//! Container format (confirmed against a retail AACS 2.1 disc):
//! ```text
//!   header (8 bytes):  u32 tag | u16 index_space | u16 record_size
//!   record[index_space]  (record_size bytes each)
//! ```
//! On the reference disc: `index_space` = `0xffff` (the full 16-bit selector
//! space, 65536 records), `record_size` = `0x0218` = 536. Total
//! `8 + 65536 * 536 = 35,127,304` bytes, which matches the file exactly. Each
//! record begins with an 8-byte sub-header, then 528 bytes of encrypted key
//! material.
//!
//! **Not yet reversed:** the internal layout of a record's 528-byte payload, and
//! how it maps onto the segments of [`super::segment`]. One numeric coincidence
//! worth noting for whoever cracks it: the reference disc has 792 segments and
//! `528 = 33 * 16`, with `792 = 24 * 33`, so `33` appears on both sides. Until
//! the mapping and the key derivation are pinned, this module exposes only the
//! confirmed container: locate the record for a given 16-bit selector.

/// Bytes of the fixed file header.
pub const HEADER_LEN: usize = 8;

/// The on-disc segment-key table container. Borrows the file bytes; a record is
/// looked up by the 16-bit variant selector.
#[derive(Debug, Clone, Copy)]
pub struct SegmentKeyTable<'a> {
    data: &'a [u8],
    /// Number of records (the selector index space, e.g. 65536).
    count: usize,
    /// Bytes per record (e.g. 536).
    record_size: usize,
}

impl<'a> SegmentKeyTable<'a> {
    /// Parse and validate the container header against the buffer length.
    ///
    /// Returns `None` when the buffer is too small, or the declared
    /// `count * record_size` (plus header) does not match the buffer, so a
    /// truncated or foreign table degrades to "no segment keys" rather than
    /// handing back bogus records. `index_space` of `0xffff` is read as the full
    /// 65536-entry space (a device selector is a full 16-bit value).
    pub fn parse(data: &'a [u8]) -> Option<Self> {
        if data.len() < HEADER_LEN {
            return None;
        }
        let index_space = u16::from_be_bytes([data[4], data[5]]);
        let record_size = u16::from_be_bytes([data[6], data[7]]) as usize;
        // 0xffff means the full 16-bit selector space (65536 records).
        let count = if index_space == 0xffff {
            0x1_0000
        } else {
            index_space as usize
        };
        if record_size == 0 {
            return None;
        }
        let body = count.checked_mul(record_size)?;
        if HEADER_LEN.checked_add(body)? != data.len() {
            return None;
        }
        Some(Self {
            data,
            count,
            record_size,
        })
    }

    /// Number of records (the selector index space).
    pub fn record_count(&self) -> usize {
        self.count
    }

    /// Bytes per record.
    pub fn record_size(&self) -> usize {
        self.record_size
    }

    /// The raw record for a 16-bit variant `selector`, including its 8-byte
    /// sub-header. `None` if the selector is past the table (only possible when
    /// `index_space` was not the full 16-bit space).
    pub fn record(&self, selector: u16) -> Option<&'a [u8]> {
        let idx = selector as usize;
        if idx >= self.count {
            return None;
        }
        let start = HEADER_LEN + idx * self.record_size;
        self.data.get(start..start + self.record_size)
    }

    /// The encrypted key payload for a selector: the record with its 8-byte
    /// sub-header stripped. The internal layout of these bytes is not yet
    /// reversed (see module docs).
    pub fn record_payload(&self, selector: u16) -> Option<&'a [u8]> {
        self.record(selector).and_then(|r| r.get(HEADER_LEN..))
    }
}

#[cfg(test)]
mod tests {
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
}
