//! The pieces of the whole-disc decrypting reader
//! ([`ResolvedKeySet::whole_disc_reader`](crate::keys::ResolvedKeySet::whole_disc_reader))
//! behind every decrypted disc/image → ISO copy, so the GUI, the CLI and the image path
//! share one set of rules. AACS content is every content file (`/BDMV/STREAM`, HD DVD
//! `/HVDVD_TS/*.EVO`), not just the kept titles. Reads follow each file's own 3-sector
//! unit grid ([`UnitAligned`]).

use crate::error::{Error, Result};
use crate::sector::{DecryptingSectorSource, SectorSource};

/// Sectors in one AACS aligned unit (6144 bytes).
pub(crate) const UNIT: u64 =
    (crate::aacs::content::ALIGNED_UNIT_LEN / crate::consts::SECTOR_BYTES) as u64;

/// Units probed per unplayed content file: its first unit, then evenly across it.
const PROBES: u64 = 32;

/// The decrypting reader a whole-disc copy reads through.
pub type WholeDiscReader<S> = UnitAligned<DecryptingSectorSource<S>>;

/// The whole-disc reader for a raw (`--raw`) copy. It never decrypts: AACS ciphertext and
/// CSS-scrambled sectors pass through byte for byte. A decrypting copy reads through
/// [`ResolvedKeySet::whole_disc_reader`](crate::keys::ResolvedKeySet::whole_disc_reader).
pub fn raw_whole_disc_reader<S: SectorSource>(reader: S) -> WholeDiscReader<S> {
    UnitAligned::new(
        DecryptingSectorSource::new(reader, crate::decrypt::DecryptKeys::None),
        Vec::new(),
    )
}

// Every AACS content file's extents, one entry per file (contiguous extents joined):
// `/BDMV/STREAM` (m2ts before SSIF, which re-lists them) or else HD DVD `/HVDVD_TS/*.EVO`.
// An unreadable UDF or unmappable file fails loud: it would otherwise ship as ciphertext.
pub(crate) fn content_files(reader: &mut dyn SectorSource) -> Result<Vec<Vec<(u32, u32)>>> {
    let fs = crate::udf::read_filesystem(reader)?;
    content_files_in(&fs, reader)
}

// `content_files` over an already-read filesystem.
pub(crate) fn content_files_in(
    fs: &crate::udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> Result<Vec<Vec<(u32, u32)>>> {
    let (top, evo_only) = if fs.find_dir("/BDMV/STREAM").is_some() {
        ("/BDMV/STREAM", false)
    } else if fs.find_dir("/HVDVD_TS").is_some() {
        ("/HVDVD_TS", true)
    } else {
        return Ok(Vec::new());
    };
    let mut paths = Vec::new();
    let mut stack: Vec<_> = fs
        .find_dir(top)
        .map(|d| (top.to_string(), d))
        .into_iter()
        .collect();
    while let Some((dir, entry)) = stack.pop() {
        for e in &entry.entries {
            let path = format!("{dir}/{}", e.name);
            if e.is_dir {
                stack.push((path, e));
            } else if !evo_only || e.name.to_ascii_uppercase().ends_with(".EVO") {
                paths.push(path);
            }
        }
    }
    paths.sort_by_key(|p| (p.to_ascii_uppercase().contains("/SSIF/"), p.clone()));
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let mut extents: Vec<(u32, u32)> = Vec::new();
        for (lba, n) in fs.file_extents(reader, &path)? {
            push_extent(&mut extents, lba, n);
        }
        if !extents.is_empty() {
            files.push(extents);
        }
    }
    Ok(files)
}

// Append `(lba, n)`, joining it to a contiguous last extent unless the count would overflow.
fn push_extent(extents: &mut Vec<(u32, u32)>, lba: u32, n: u32) {
    if n == 0 {
        return;
    }
    let joined = extents.last_mut().and_then(|last| {
        let sum = last.1.checked_add(n)?;
        (last.0 as u64 + last.1 as u64 == lba as u64).then(|| last.1 = sum)
    });
    if joined.is_some() {
        return;
    }
    extents.push((lba, n));
}

/// A content extent `(start, count)` and the LBA its unit grid is anchored at: the
/// owning file's first sector, carried across extents by file offset.
pub(crate) type UnitSpan = (u32, u32, u64);

// The span holding `lba`, if any.
pub(crate) fn span_at(spans: &[UnitSpan], lba: u64) -> Option<UnitSpan> {
    let i = spans
        .partition_point(|&(s, _, _)| s as u64 <= lba)
        .checked_sub(1)?;
    let span = spans[i];
    (lba < span.0 as u64 + span.1 as u64).then_some(span)
}

// The first unit head at or after `lba` on the grid anchored at `anchor`.
pub(crate) fn unit_head(lba: u32, anchor: u64) -> u64 {
    lba as u64 + (UNIT - (lba as u64).saturating_sub(anchor) % UNIT) % UNIT
}

// Which of a piece's `units` to probe, in order: all of them when few, else the
// first unit, then `PROBES - 1` more spread evenly to the end.
pub(crate) fn probe_units(units: u64) -> Vec<u64> {
    if units <= PROBES {
        return (0..units).collect();
    }
    (0..PROBES).map(|p| units * p / PROBES).collect()
}

// `content` `(start, count)` ranges minus the sorted, disjoint `[start, end)` `keyed`
// ranges, as `[start, end)` pieces.
pub(crate) fn subtract_ranges(content: &[(u32, u32)], keyed: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    for &(start, count) in content {
        let end = start.saturating_add(count);
        let mut pos = start;
        for &(ks, ke) in keyed {
            if ke <= pos || ks >= end {
                continue;
            }
            if ks > pos {
                out.push((pos, ks));
            }
            pos = pos.max(ke);
        }
        if pos < end {
            out.push((pos, end));
        }
    }
    out
}

// Sort + coalesce overlapping/adjacent `(start, count)` ranges (the content gate's
// binary search needs them sorted and disjoint); empty ranges are dropped.
pub(crate) fn merge_ranges(mut ranges: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    ranges.retain(|&(_, count)| count > 0);
    ranges.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
    for (start, count) in ranges {
        let end = start as u64 + count as u64;
        if let Some(last) = out.last_mut() {
            let last_end = last.0 as u64 + last.1 as u64;
            if start as u64 <= last_end {
                let merged = last_end.max(end) - last.0 as u64;
                last.1 = u32::try_from(merged).unwrap_or(u32::MAX);
                continue;
            }
        }
        out.push((start, count));
    }
    out
}

/// Reads through `inner` on each content file's AACS unit grid: a read touching a span
/// is widened to whole units of that span (sweep batches, `write_image` batches and
/// patch's single-sector reads are not unit multiples). A unit whose head lies outside
/// its span fails loud.
pub struct UnitAligned<S> {
    inner: S,
    spans: Vec<UnitSpan>,
    scratch: Vec<u8>,
}

impl<S: SectorSource> UnitAligned<S> {
    pub(crate) fn new(inner: S, spans: Vec<UnitSpan>) -> Self {
        Self {
            inner,
            spans,
            scratch: Vec::new(),
        }
    }

    /// Where a block of sectors `[start, end)` should end so consecutive blocks tile each
    /// file's unit grid: `end` pulled back to its unit's head when that unit straddles it
    /// (else a bad sector there fails both neighbouring blocks). Never at or before `start`.
    pub fn unit_block_end(&self, start: u64, end: u64) -> u64 {
        match span_at(&self.spans, end) {
            Some((s, _, anchor)) if end > anchor => {
                let head = anchor + (end - anchor) / UNIT * UNIT;
                if head > start && head >= s as u64 {
                    head
                } else {
                    end
                }
            }
            _ => end,
        }
    }
}

impl<S: SectorSource> UnitAligned<crate::sector::DecryptingSectorSource<S>> {
    /// Damaged AACS units the decrypting reader blanked so far (see
    /// [`DecryptingSectorSource::blanked_units`](crate::sector::DecryptingSectorSource::blanked_units)).
    pub fn blanked_units(&self) -> u64 {
        self.inner.blanked_units()
    }
}

impl<S: SectorSource> SectorSource for UnitAligned<S> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }

    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.read_sectors_fua(lba, count, buf, recovery, false)
    }

    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        let end = lba as u64 + count as u64;
        let mut cur = lba as u64;
        while cur < end {
            let off = (cur - lba as u64) as usize * crate::consts::SECTOR_BYTES;
            let Some((s, n, anchor)) = span_at(&self.spans, cur) else {
                // Outside every content span: a plain read up to the next span.
                let i = self.spans.partition_point(|&(s, _, _)| s as u64 <= cur);
                let next = self
                    .spans
                    .get(i)
                    .map_or(end, |&(s, _, _)| end.min(s as u64));
                let want = (next - cur) as usize * crate::consts::SECTOR_BYTES;
                self.inner.set_unit_base(cur as u32);
                let got = self.inner.read_sectors_fua(
                    cur as u32,
                    (next - cur) as u16,
                    &mut buf[off..off + want],
                    recovery,
                    fua,
                )?;
                if got < want {
                    return Ok(off + got);
                }
                cur = next;
                continue;
            };
            let (s, e) = (s as u64, s as u64 + n as u64);
            let a0 = anchor + (cur - anchor) / UNIT * UNIT;
            if a0 < s {
                // The unit straddles a non-contiguous extent boundary: undecryptable here.
                return Err(Error::DecryptFailed);
            }
            // Capped so the widened read still fits one u16-count request.
            let piece_end = end.min(e).min(a0 + (u16::MAX as u64 / UNIT - 1) * UNIT);
            let a1 = e.min(a0 + (piece_end - a0).div_ceil(UNIT) * UNIT);
            let len = (a1 - a0) as usize * crate::consts::SECTOR_BYTES;
            self.scratch.resize(len, 0);
            self.inner.set_unit_base(a0 as u32);
            let got = self.inner.read_sectors_fua(
                a0 as u32,
                (a1 - a0) as u16,
                &mut self.scratch[..len],
                recovery,
                fua,
            )?;
            let skip = (cur - a0) as usize * crate::consts::SECTOR_BYTES;
            let want = (piece_end - cur) as usize * crate::consts::SECTOR_BYTES;
            let have = got.saturating_sub(skip).min(want);
            buf[off..off + have].copy_from_slice(&self.scratch[skip..skip + have]);
            if have < want {
                return Ok(off + have);
            }
            cur = piece_end;
        }
        Ok(count as usize * crate::consts::SECTOR_BYTES)
    }

    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }
}

#[cfg(test)]
#[path = "whole_disc_tests.rs"]
mod tests;
