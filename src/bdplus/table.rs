//! The BD+ conversion (fix-up) table: parse, validate, and turn into byte patches.

use crate::consts::BD_SOURCE_PACKET_BYTES;
use crate::error::{Error, Result};

/// Largest table accepted, in bytes: the BD+ VM's 4 MiB memory, which a table comes from.
pub const MAX_TABLE_BYTES: usize = 0x40_0000;

/// Bytes each patch overwrites.
pub const PATCH_LEN: usize = 5;

// Lowest and highest valid patch offset within a 192-byte source packet: inside the TS
// payload, never the TP_extra_header or the TS header.
const MIN_PACKET_OFFSET: u8 = 8;
const MAX_PACKET_OFFSET: u8 = 187;

const ENTRY_LEN: usize = 16;

/// What an entry's top two flag bits ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EntryKind {
    /// `00`: no transform (fill entries).
    Inactive,
    /// `01`: always applied.
    Apply,
    /// `10`: forensic; applied iff bit `bit` is set in the segment's mask.
    Forensic { bit: u8 },
    /// `11`: reserved; never applied.
    Invalid,
}

/// One 16-byte table entry and its source-packet index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// Source-packet index the entry's adjusts count from.
    pub index: u32,
    pub flags: u8,
    /// Packets from `index` to patch 0 (12 bits).
    pub adjust0: u16,
    /// Packets from patch 0's packet to patch 1's (12 bits).
    pub adjust1: u16,
    /// Byte offset of patch 0 within its source packet.
    pub offset0: u8,
    /// Byte offset of patch 1 within its source packet.
    pub offset1: u8,
    pub patch0: [u8; PATCH_LEN],
    pub patch1: [u8; PATCH_LEN],
}

impl Entry {
    /// The kind the flags select.
    pub fn kind(&self) -> EntryKind {
        match self.flags >> 6 {
            0 => EntryKind::Inactive,
            1 => EntryKind::Apply,
            2 => EntryKind::Forensic {
                bit: self.flags & 0x3F,
            },
            _ => EntryKind::Invalid,
        }
    }

    /// Byte offset of patch 0 within the clip file.
    pub fn patch0_offset(&self) -> u64 {
        let packet = u64::from(self.index) + u64::from(self.adjust0);
        packet * BD_SOURCE_PACKET_BYTES as u64 + u64::from(self.offset0)
    }

    /// Byte offset of patch 1 within the clip file.
    pub fn patch1_offset(&self) -> u64 {
        let packet = u64::from(self.index) + u64::from(self.adjust0) + u64::from(self.adjust1);
        packet * BD_SOURCE_PACKET_BYTES as u64 + u64::from(self.offset1)
    }

    // Both offsets inside the TS payload and patch 0 before patch 1.
    fn well_formed(&self) -> bool {
        let in_payload = |o: u8| (MIN_PACKET_OFFSET..=MAX_PACKET_OFFSET).contains(&o);
        in_payload(self.offset0)
            && in_payload(self.offset1)
            && self.patch0_offset() < self.patch1_offset()
    }
}

/// One segment of a clip's table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub entries: Vec<Entry>,
    /// The entries are still XORed with a segment key only the disc's BD+ program
    /// produces: none is applied.
    pub encrypted: bool,
    /// The forensic mask the program delivered with the segment key; `None` applies
    /// every forensic entry (an activated table).
    pub mask: Option<[u8; 8]>,
}

impl Segment {
    fn applies(&self, e: &Entry) -> bool {
        match e.kind() {
            EntryKind::Apply => true,
            EntryKind::Forensic { bit } => self
                .mask
                .is_none_or(|m| m[7 - usize::from(bit >> 3)] & (1 << (bit & 7)) != 0),
            EntryKind::Inactive | EntryKind::Invalid => false,
        }
    }
}

/// The table of one clip: `clip` is the `BDMV/STREAM/<clip>.m2ts` number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClipTable {
    pub clip: u32,
    pub segments: Vec<Segment>,
}

/// A 5-byte overwrite at `offset` bytes into a clip file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Patch {
    pub offset: u64,
    pub bytes: [u8; PATCH_LEN],
}

/// A clip table's patches in offset order, and what was left out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PatchSet {
    pub patches: Vec<Patch>,
    /// Entries applied (two patches each).
    pub entries: usize,
    /// Entries that asked to apply but are malformed: an offset outside 8..=187, patch 1
    /// not after patch 0, or patch 0 before the previous applied entry's patch 1.
    pub rejected: usize,
    /// Entries left in encrypted segments.
    pub encrypted: usize,
}

impl ClipTable {
    /// The patches this table applies. Malformed entries are dropped one by one, as a
    /// player deactivates them, never the whole table.
    pub fn patches(&self) -> PatchSet {
        let mut set = PatchSet::default();
        for seg in &self.segments {
            if seg.encrypted {
                set.encrypted += seg.entries.len();
                continue;
            }
            let mut prev_end: Option<u64> = None;
            for e in seg.entries.iter().filter(|e| seg.applies(e)) {
                if !e.well_formed() || prev_end.is_some_and(|p| e.patch0_offset() < p) {
                    set.rejected += 1;
                    continue;
                }
                prev_end = Some(e.patch1_offset());
                set.entries += 1;
                set.patches.push(Patch {
                    offset: e.patch0_offset(),
                    bytes: e.patch0,
                });
                set.patches.push(Patch {
                    offset: e.patch1_offset(),
                    bytes: e.patch1,
                });
            }
        }
        set.patches.sort_by_key(|p| p.offset);
        set
    }
}

/// A parsed conversion table: one [`ClipTable`] per clip, in file order.
///
/// The layout (big-endian) is libbdplus's `convtab.bin`: u16 table count; per table a u32
/// clip number, a u16 segment count and that many u32 absolute segment offsets, the next
/// table following the last segment; per segment a u32 entry count, that many u32
/// source-packet indexes, then the 16-byte entries (flags, two 12-bit adjusts, two patch
/// offsets, two 5-byte patches).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConversionTable {
    pub clips: Vec<ClipTable>,
}

impl ConversionTable {
    /// Parse an activated (cached) table: every segment already decrypted, forensic
    /// entries applied. [`Error::BdPlusTableInvalid`] when truncated or over
    /// [`MAX_TABLE_BYTES`].
    pub fn parse_activated(bytes: &[u8]) -> Result<Self> {
        parse(bytes, false)
    }

    /// Parse a table as the BD+ program hands it over: every non-empty segment is still
    /// encrypted, so nothing applies until a resolver decrypts it.
    pub fn parse_delivered(bytes: &[u8]) -> Result<Self> {
        parse(bytes, true)
    }

    /// The table of clip `clip`, if any.
    pub fn clip(&self, clip: u32) -> Option<&ClipTable> {
        self.clips.iter().find(|t| t.clip == clip)
    }
}

fn invalid(kind: &'static str) -> Error {
    Error::BdPlusTableInvalid { kind }
}

fn be16(b: &[u8], at: usize) -> Result<u16> {
    let s = b.get(at..at + 2).ok_or_else(|| invalid("truncated"))?;
    Ok(u16::from_be_bytes([s[0], s[1]]))
}

fn be32(b: &[u8], at: usize) -> Result<u32> {
    let s = b.get(at..at + 4).ok_or_else(|| invalid("truncated"))?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn parse(bytes: &[u8], delivered: bool) -> Result<ConversionTable> {
    if bytes.len() > MAX_TABLE_BYTES {
        return Err(invalid("too_large"));
    }
    let count = be16(bytes, 0)?;
    let mut at = 2usize;
    let mut clips = Vec::with_capacity(usize::from(count).min(1024));
    for _ in 0..count {
        let clip = be32(bytes, at)?;
        let nseg = usize::from(be16(bytes, at + 4)?);
        let offsets = at + 6;
        at = offsets;
        let mut segments = Vec::with_capacity(nseg.min(1024));
        for s in 0..nseg {
            let start = be32(bytes, offsets + 4 * s)? as usize;
            let (seg, end) = parse_segment(bytes, start, delivered)?;
            segments.push(seg);
            at = end;
        }
        clips.push(ClipTable { clip, segments });
    }
    Ok(ConversionTable { clips })
}

// The segment at `start` and the offset just past it.
fn parse_segment(bytes: &[u8], start: usize, delivered: bool) -> Result<(Segment, usize)> {
    let n = be32(bytes, start)? as usize;
    let index_at = start + 4;
    let entries_at = n
        .checked_mul(4)
        .and_then(|l| index_at.checked_add(l))
        .ok_or_else(|| invalid("truncated"))?;
    let end = n
        .checked_mul(ENTRY_LEN)
        .and_then(|l| entries_at.checked_add(l))
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| invalid("truncated"))?;
    let entries = (0..n)
        .map(|i| {
            let r = &bytes[entries_at + i * ENTRY_LEN..entries_at + (i + 1) * ENTRY_LEN];
            let adjust = u32::from_be_bytes([0, r[1], r[2], r[3]]);
            let mut patch0 = [0u8; PATCH_LEN];
            let mut patch1 = [0u8; PATCH_LEN];
            patch0.copy_from_slice(&r[6..11]);
            patch1.copy_from_slice(&r[11..16]);
            Ok(Entry {
                index: be32(bytes, index_at + 4 * i)?,
                flags: r[0],
                adjust0: (adjust >> 12) as u16,
                adjust1: (adjust & 0xFFF) as u16,
                offset0: r[4],
                offset1: r[5],
                patch0,
                patch1,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let encrypted = delivered && !entries.is_empty();
    let seg = Segment {
        entries,
        encrypted,
        mask: None,
    };
    Ok((seg, end))
}

#[cfg(test)]
#[path = "table_tests.rs"]
pub(crate) mod tests;
