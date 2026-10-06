//! AACS 2.1 FMTS forensic segment keys, `AACS/SegmentKeyNNNNN.tbl`.
//!
//! One file per CPS unit. On-disc key store for the forensic variant segments
//! mapped by [`super::segment`]; a device derives a 16-bit variant selector
//! from the Media Key Variant chain (see [`super::variant`]) and uses it to
//! index this table.
//!
//! Container format:
//! ```text
//!   header (8 bytes):  u32 tag | u16 index_space | u16 record_size
//!   record[index_space]  (record_size bytes each)
//! ```
//! Record layout beyond an 8-byte sub-header is not yet reversed.

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
#[path = "segment_key_tests.rs"]
mod tests;
