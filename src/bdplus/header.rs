//! The 24-byte header of a `BDSVM/*.svm` file: raw facts, plus a date-derived estimate.

/// Bytes in an SVM file header.
pub const SVM_HEADER_LEN: usize = 24;

const MAGIC: &[u8; 8] = b"BDSVM_CC";

/// The header of a `BDSVM/*.svm` file, as recorded on the disc.
///
/// Big-endian: magic `BDSVM_CC` at 0x00, five bytes of unknown meaning at 0x08 (observed
/// `01 00 00 00 01`, presumably format/version), the authoring date at 0x0D (year u16,
/// month, day), three unread bytes, then the payload length u32 at 0x14. A wrong magic
/// still parses (`magic_ok == false`); nothing here is a BD+ generation, see
/// [`Self::estimated_generation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmHeader {
    /// The 24 header bytes as read.
    pub raw: [u8; SVM_HEADER_LEN],
    /// Bytes 0x00..0x08 are `BDSVM_CC`.
    pub magic_ok: bool,
    /// Bytes 0x08..0x0D, uninterpreted.
    pub format_bytes: [u8; 5],
    pub year: u16,
    pub month: u8,
    pub day: u8,
    /// Bytes of content code that follow the header (big-endian u32 at 0x14).
    pub payload_len: u32,
}

impl SvmHeader {
    /// Parse the first [`SVM_HEADER_LEN`] bytes of `bytes`; `None` when it is shorter.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let raw: [u8; SVM_HEADER_LEN] = bytes.get(..SVM_HEADER_LEN)?.try_into().ok()?;
        let mut format_bytes = [0u8; 5];
        format_bytes.copy_from_slice(&raw[0x08..0x0D]);
        Some(SvmHeader {
            raw,
            magic_ok: &raw[..8] == MAGIC,
            format_bytes,
            year: u16::from_be_bytes([raw[0x0D], raw[0x0E]]),
            month: raw[0x0F],
            day: raw[0x10],
            payload_len: u32::from_be_bytes([raw[0x14], raw[0x15], raw[0x16], raw[0x17]]),
        })
    }

    /// The header date bucketed by [`estimated_generation`]: a heuristic, not a disc field.
    pub fn estimated_generation(&self) -> u8 {
        estimated_generation(self.year, self.month, self.day)
    }

    /// The magic is `BDSVM_CC`, the date is a calendar date, and the payload fits the BD+
    /// VM's 4 MiB. `false` is a present but malformed header, still BD+ by presence.
    pub fn is_well_formed(&self) -> bool {
        self.magic_ok
            && (1..=12).contains(&self.month)
            && (1..=31).contains(&self.day)
            && self.payload_len < 0x40_0000
    }
}

// First header date of each libbdplus date bucket: 12 and 13 share a date, so no date
// yields 12 (libbdplus behaves the same).
const GENERATION_STARTS: [(u16, u8, u8); 16] = [
    (2007, 6, 8),
    (2007, 12, 20),
    (2008, 4, 14),
    (2008, 8, 22),
    (2009, 2, 2),
    (2009, 3, 31),
    (2009, 8, 18),
    (2010, 3, 6),
    (2010, 9, 3),
    (2011, 2, 28),
    (2011, 6, 1),
    (2011, 11, 16),
    (2011, 11, 16),
    (2012, 8, 7),
    (2013, 1, 25),
    (2013, 4, 30),
];

/// The libbdplus date bucket ("generation") of an SVM header date: 1 from 2007-06-08 up to
/// 16 from 2013-04-30, 0 before the first boundary, never 12. A community classification for
/// display and bug reports only: the disc records no generation, the bucket does not
/// identify a mechanism, and nothing should branch on it.
pub fn estimated_generation(year: u16, month: u8, day: u8) -> u8 {
    let date = (year, month, day);
    GENERATION_STARTS.iter().filter(|&&s| s <= date).count() as u8
}

#[cfg(test)]
#[path = "header_tests.rs"]
mod tests;
