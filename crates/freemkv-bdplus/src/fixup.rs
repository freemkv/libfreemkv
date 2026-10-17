//! BD+ conversion-table decoding and immutable M2TS repairs.
//! Submitted descriptors remain encoded until ComputeSP supplies their key/mask.
//!
//! The table model is cross-checked against patent publications describing
//! [masked FUT entries], [media transforms], [per-clip fix-up tables], detailed
//! [FUT blocks and forensic marks], and [per-segment secret parameters]. These
//! publications are architectural cross-checks; they do not by themselves
//! establish the BD+ ABI.
//!
//! [masked FUT entries]: ../docs/kr20070058288_bdplus_fut_descrambling.pdf "upstream: https://patentimages.storage.googleapis.com/d0/d2/b1/c3bb5fb225ac97/KR20070058288A.pdf"
//! [media transforms]: ../docs/kr20080012724_bdplus_fixup_media_transform.pdf "upstream: https://patentimages.storage.googleapis.com/5e/dd/22/1ceccd7eef9801/KR20080012724A.pdf"
//! [per-clip fix-up tables]: ../docs/kr20080027347_bdplus_contentcode_fixuptables.pdf "upstream: https://patentimages.storage.googleapis.com/92/4c/d2/65b68762374f23/KR20080027347A.pdf"
//! [FUT blocks and forensic marks]: ../docs/us8042188_bdplus_fut_forensic_marks.pdf "upstream: https://patentimages.storage.googleapis.com/ea/98/9b/3e356f6aa8f139/US8042188.pdf"
//! [per-segment secret parameters]: ../docs/us8625967_bdplus_segment_secret_parameters.pdf "upstream: https://patentimages.storage.googleapis.com/aa/30/8e/7e223065e263a8/US8625967.pdf"

use super::{BdPlusError, BdPlusResult};
use std::collections::BTreeMap;

const MAX_TABLE_BYTES: usize = 1024 * 1024;
const MAX_TABLES: usize = 4096;
const MAX_SEGMENTS: usize = 65536;
const MAX_ENTRIES: usize = MAX_TABLE_BYTES / 20;

/// Five replacement bytes at an absolute offset in the original M2TS clip.
#[derive(Clone, PartialEq, Eq)]
pub struct BdPlusPatch {
    offset: u64,
    bytes: [u8; 5],
}

impl BdPlusPatch {
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn bytes(&self) -> &[u8; 5] {
        &self.bytes
    }
}

impl std::fmt::Debug for BdPlusPatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusPatch")
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

/// Validated repairs for one clip, shareable between independent readers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BdPlusPatchSet {
    clip_id: u32,
    patches: Vec<BdPlusPatch>,
}

impl BdPlusPatchSet {
    /// Construct an empty patch set bound to a clip. Used by the
    /// disc-pipeline wiring when a clip has no BD+ repairs (clean clip or
    /// out-of-table clip) so the same patcher decorator can still be
    /// installed uniformly.
    pub fn empty(clip_id: u32) -> Self {
        Self {
            clip_id,
            patches: Vec::new(),
        }
    }

    pub fn clip_id(&self) -> u32 {
        self.clip_id
    }
    pub fn patches(&self) -> &[BdPlusPatch] {
        &self.patches
    }

    /// Apply every replacement fragment intersecting the original M2TS range.
    /// Seeks/retries are stateless; crossing repairs are applied in parts.
    /// Returns the number of repair fragments applied to this buffer.
    pub fn apply(&self, offset: u64, buffer: &mut [u8]) -> BdPlusResult<usize> {
        let end = offset
            .checked_add(buffer.len() as u64)
            .ok_or(BdPlusError::OffsetOverflow)?;
        let first = self
            .patches
            .partition_point(|patch| patch.offset + 5 <= offset);
        let mut count = 0;
        for patch in &self.patches[first..] {
            if patch.offset >= end {
                break;
            }
            let start = patch.offset.max(offset);
            let stop = (patch.offset + 5).min(end);
            if start < stop {
                let dst = (start - offset) as usize;
                let src = (start - patch.offset) as usize;
                let length = (stop - start) as usize;
                buffer[dst..dst + length].copy_from_slice(&patch.bytes[src..src + length]);
                count += 1;
            }
        }
        Ok(count)
    }
}

struct EncodedEntry {
    index: u32,
    descriptor: [u8; 16],
}
struct Segment {
    entries: Vec<EncodedEntry>,
    resolved: Option<Vec<BdPlusPatch>>,
}

/// Submitted classic BD+ tables. Unresolved segments cannot publish repairs.
pub struct BdPlusFixupTable {
    tables: BTreeMap<u32, Vec<Segment>>,
}

impl std::fmt::Debug for BdPlusFixupTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusFixupTable")
            .field("clips", &self.tables.len())
            .field("unresolved_segments", &self.unresolved_segments().count())
            .finish()
    }
}

fn bytes_at(bytes: &[u8], offset: usize, length: usize) -> BdPlusResult<&[u8]> {
    let end = offset
        .checked_add(length)
        .ok_or(BdPlusError::FixupFormat { offset })?;
    bytes
        .get(offset..end)
        .ok_or(BdPlusError::FixupFormat { offset })
}
fn word(bytes: &[u8], offset: usize) -> BdPlusResult<u32> {
    Ok(u32::from_be_bytes(
        bytes_at(bytes, offset, 4)?.try_into().unwrap(),
    ))
}
fn half(bytes: &[u8], offset: usize) -> BdPlusResult<usize> {
    Ok(u16::from_be_bytes(bytes_at(bytes, offset, 2)?.try_into().unwrap()) as usize)
}
fn bounded(requested: usize, limit: usize) -> BdPlusResult<()> {
    if requested > limit {
        Err(BdPlusError::ResourceLimit { requested, limit })
    } else {
        Ok(())
    }
}

impl BdPlusFixupTable {
    /// Parse a classic FixUpTableSend buffer with big-endian fields. Each
    /// segment has an index array followed by a separate descriptor array.
    /// This never assumes that submitted descriptor bytes are already clear.
    pub fn parse(bytes: &[u8]) -> BdPlusResult<Self> {
        bounded(bytes.len(), MAX_TABLE_BYTES)?;
        let tables = half(bytes, 0)?;
        bounded(tables, MAX_TABLES)?;
        let mut result = Self {
            tables: BTreeMap::new(),
        };
        let mut cursor = 2;
        let mut total_segments = 0;
        let mut total_entries = 0;
        for _ in 0..tables {
            let clip_id = word(bytes, cursor)?;
            let segment_count = half(bytes, cursor + 4)?;
            cursor += 6;
            total_segments += segment_count;
            bounded(total_segments, MAX_SEGMENTS)?;
            let directory_end = cursor + segment_count * 4;
            bytes_at(bytes, cursor, segment_count * 4)?;
            let mut segments = Vec::new();
            let mut previous_end = directory_end;
            for segment_index in 0..segment_count {
                let offset = word(bytes, cursor + segment_index * 4)? as usize;
                // The classic wire layout has non-overlapping segments in
                // directory order. Other layouts need an explicit new parser.
                if offset < previous_end {
                    return Err(BdPlusError::FixupFormat { offset });
                }
                let count = word(bytes, offset)? as usize;
                bounded(count, MAX_ENTRIES - total_entries)?;
                total_entries += count;
                // Validate before allocation; count is bounded above.
                let storage_len = count * 20;
                bytes_at(bytes, offset + 4, storage_len)?;
                let indexes = offset + 4;
                let descriptors = indexes + count * 4;
                let mut entries = Vec::with_capacity(count);
                for entry_index in 0..count {
                    let index = word(bytes, indexes + entry_index * 4)?;
                    let descriptor = bytes_at(bytes, descriptors + entry_index * 16, 16)?
                        .try_into()
                        .unwrap();
                    entries.push(EncodedEntry { index, descriptor });
                }
                previous_end = offset + 4 + storage_len;
                segments.push(Segment {
                    resolved: (count == 0).then(Vec::new),
                    entries,
                });
            }
            cursor = previous_end;
            if result.tables.insert(clip_id, segments).is_some() {
                return Err(BdPlusError::DuplicateTable { clip_id });
            }
        }
        if cursor != bytes.len() {
            return Err(BdPlusError::FixupFormat { offset: cursor });
        }
        Ok(result)
    }

    pub fn clip_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.tables.keys().copied()
    }

    /// Segments requiring ComputeSP, in stable clip/segment order.
    pub fn unresolved_segments(&self) -> impl Iterator<Item = (u32, usize)> + '_ {
        self.tables.iter().flat_map(|(&clip, segments)| {
            segments
                .iter()
                .enumerate()
                .filter_map(move |(index, segment)| {
                    segment.resolved.is_none().then_some((clip, index))
                })
        })
    }

    /// Resolve atomically using one ComputeSP key and forensic mask. A failure
    /// leaves the encoded segment intact and unresolved; never XOR in place.
    pub fn resolve_segment(
        &mut self,
        clip_id: u32,
        segment: usize,
        key: &[u8; 16],
        mask: &[u8; 8],
    ) -> BdPlusResult<()> {
        let segments = self
            .tables
            .get_mut(&clip_id)
            .ok_or(BdPlusError::MissingTable { clip_id })?;
        let source = segments
            .get_mut(segment)
            .ok_or(BdPlusError::MissingSegment { clip_id, segment })?;
        if source.resolved.is_some() {
            return Err(BdPlusError::SegmentAlreadyResolved { clip_id, segment });
        }
        let mut patches = Vec::new();
        for (entry, encoded) in source.entries.iter().enumerate() {
            let mut data = encoded.descriptor;
            for (byte, xor) in data.iter_mut().zip(key) {
                *byte ^= xor;
            }
            let kind = data[0] >> 6;
            let active = match kind {
                1 => true,
                2 => {
                    let bit = data[0] & 63;
                    mask[7 - (bit / 8) as usize] & (1 << (bit % 8)) != 0
                }
                0 => false,
                _ => {
                    return Err(BdPlusError::DescriptorType {
                        clip_id,
                        segment,
                        entry,
                        kind,
                    });
                }
            };
            if !active {
                continue;
            }
            let adjust0 = (u32::from(data[1]) << 4) | u32::from(data[2] >> 4);
            let adjust1 = (u32::from(data[2] & 15) << 8) | u32::from(data[3]);
            let packet0 = u64::from(encoded.index) + u64::from(adjust0);
            let packet1 = packet0 + u64::from(adjust1);
            if !(8..=187).contains(&data[4])
                || !(8..=187).contains(&data[5])
                || packet0 * 192 + u64::from(data[4]) >= packet1 * 192 + u64::from(data[5])
            {
                return Err(BdPlusError::DescriptorAddress {
                    clip_id,
                    segment,
                    entry,
                });
            }
            patches.push(BdPlusPatch {
                offset: packet0 * 192 + u64::from(data[4]),
                bytes: data[6..11].try_into().unwrap(),
            });
            patches.push(BdPlusPatch {
                offset: packet1 * 192 + u64::from(data[5]),
                bytes: data[11..16].try_into().unwrap(),
            });
        }
        sort_and_validate(&mut patches)?;
        source.resolved = Some(patches);
        Ok(())
    }

    /// Publish repairs after every segment resolves. A submitted empty clip
    /// yields an empty set; an absent clip is a distinct error.
    pub fn patches_for_clip(&self, clip_id: u32) -> BdPlusResult<BdPlusPatchSet> {
        let segments = self
            .tables
            .get(&clip_id)
            .ok_or(BdPlusError::MissingTable { clip_id })?;
        let mut patches = Vec::new();
        for (segment, source) in segments.iter().enumerate() {
            let resolved = source
                .resolved
                .as_ref()
                .ok_or(BdPlusError::UnresolvedSegment { clip_id, segment })?;
            patches.extend(resolved.iter().cloned());
        }
        sort_and_validate(&mut patches)?;
        Ok(BdPlusPatchSet { clip_id, patches })
    }
}

fn sort_and_validate(patches: &mut [BdPlusPatch]) -> BdPlusResult<()> {
    patches.sort_by_key(|patch| patch.offset);
    // Until overlap precedence is proven, reject ambiguous replacements.
    if let Some(pair) = patches
        .windows(2)
        .find(|pair| pair[0].offset + 5 > pair[1].offset)
    {
        return Err(BdPlusError::OverlappingPatches {
            offset: pair[1].offset,
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "fixup_tests.rs"]
mod tests;
