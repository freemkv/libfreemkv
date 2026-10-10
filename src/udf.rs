//! UDF filesystem reader — read files from Blu-ray discs (and plain UDF images).
//!
//! Blu-ray discs use UDF 2.50 with metadata partitions. The read sequence
//! follows pointers through the disc structure: AVDP → VDS → Metadata
//! Partition → File Set Descriptor → Root Directory ICB → Directory data
//! → BDMV/PLAYLIST/*.mpls, BDMV/CLIPINF/*.clpi. Each step reads one or two
//! sectors; no bulk reads needed.

use crate::consts::{SECTOR_BYTES, SECTOR_BYTES_U64};
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use std::collections::HashSet;

// Cap on a single unbounded metadata file read (`read_file`); bounds the allocation a crafted
// ICB info_length/extent length can force.
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

// Cap on a single directory's on-disc data, well above any legitimate BD-ROM
// directory, so a corrupt 30-bit allocation length can't force a huge alloc.
pub(crate) const MAX_DIR_BYTES: u32 = 1024 * 1024;

// Cap on the UDF-structure range `metadata_sector_ranges` prefetches from LBA 0 (64 MiB); real
// volume structures and metadata partitions are a few MiB, and both ends are disc-controlled.
const MAX_STRUCT_SECTORS: u32 = 32_768;

// Cap on range tuples collected by one tree walk; real discs need a few thousand. Bounds the
// memory a crafted extent chain shared by many entries can force.
const MAX_WALK_RANGES: usize = 65_536;

// Cap on one contiguous `prefetch` (16 MiB), so a disc-controlled length can't drive a huge alloc.
const MAX_PREFETCH_RUN_SECTORS: u32 = 8192;

// Files larger than this are not cached by `metadata_sector_ranges` (MKB_RO.inf is ~134 MB).
const MAX_CACHED_FILE_BYTES: u64 = 50_000_000;

// Smallest Main VDS extent ECMA-167 3/10.2.1 permits an AVDP to record (16
// sectors). A smaller extent is unusable, so it's ignored in favour of
// VDS_FALLBACK_START.
const VDS_MIN_SECTORS: u32 = 16;

/// Sectors of the Volume Descriptor Sequence actually swept. The sequence is
/// a short run of descriptors terminated by a Terminating Descriptor (tag 8),
/// so this only bounds the work an oversized/corrupt ExtentLength can force.
const VDS_MAX_SECTORS: u32 = 32;

/// Where the Main VDS is swept from when the anchor's own extent is unusable.
/// The customary location on optical media, and what this reader assumed
/// unconditionally before it followed the anchor's pointer.
const VDS_FALLBACK_START: u32 = 32;

/// A UDF filesystem parsed from disc.
#[derive(Debug)]
pub struct UdfFs {
    /// Root directory with full tree
    pub root: DirEntry,
    /// UDF Volume Identifier from Primary Volume Descriptor
    pub volume_id: String,
    /// Physical partition start (absolute sector)
    partition_start: u32,
    /// Metadata partition block → absolute sector map.
    /// For UDF 2.50 discs, all file/directory references use metadata-relative LBAs
    meta: MetaMap,
    /// Sectors in the metadata partition's first extent
    metadata_sectors: u32,
}

/// Metadata-partition block → absolute sector, through the Metadata File's extents
/// `(abs_start, sectors)` in file order; the last extent is open-ended.
#[derive(Debug, Clone)]
pub(crate) struct MetaMap(Vec<(u32, u32)>);

impl MetaMap {
    /// One open-ended extent: the metadata partition is contiguous from `start`.
    pub(crate) fn contiguous(start: u32) -> Self {
        Self(vec![(start, 0)])
    }

    fn start(&self) -> u32 {
        self.0.first().map_or(0, |e| e.0)
    }

    fn to_abs(&self, rel: u32) -> Result<u32> {
        let mut rel = rel;
        let last = self.0.len().saturating_sub(1);
        for (i, &(start, n)) in self.0.iter().enumerate() {
            if rel < n || i == last {
                return start.checked_add(rel).ok_or(Error::DiscRead {
                    sector: start as u64,
                    status: None,
                    sense: None,
                });
            }
            rel -= n;
        }
        Err(Error::DiscRead {
            sector: 0,
            status: None,
            sense: None,
        })
    }
}

/// One allocation extent of a file, as recorded in its ICB.
///
/// `recorded` distinguishes the two extent types (ECMA-167 4/14.14.1.1):
/// type 0 — recorded and allocated: `len` bytes of real data live at `lba`.
/// Type 1 — allocated but NOT recorded: the space belongs to the file's byte
/// space but nothing was written, so its contents are zeros; `lba` is where
/// the space is allocated, not where readable data lives. Dropping a type-1
/// descriptor would slide later extents' data down, corrupting the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcbExtent {
    /// Partition-relative LBA of the extent.
    pub lba: u32,
    /// Declared length of the extent in bytes.
    pub len: u32,
    /// `false` for an ECMA-167 4/14.14.1.1 type-1 (allocated, not recorded)
    /// extent, whose bytes are logically zeros and must not be read off media.
    pub recorded: bool,
}

// [`IcbExtent`] with the partition offset applied to an absolute disc LBA.
// `recorded` travels with it: unrecorded means emit `len` zeros, don't read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub(crate): only reached via internal `extents_abs_at`; the crate's public
// surface is Drive/Disc/ScanOptions/input()/output()/numeric errors.
pub(crate) struct AbsExtent {
    /// Absolute disc LBA of the extent.
    pub lba: u32,
    /// Declared length of the extent in bytes.
    pub len: u32,
    /// `false` for an ECMA-167 4/14.14.1.1 type-1 (allocated, not recorded)
    /// extent — see [`IcbExtent::recorded`].
    pub recorded: bool,
}

/// A directory or file entry.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    /// Block within the metadata partition. Not `metadata_start() + meta_lba` when the
    /// Metadata File is fragmented: later extents live elsewhere on disc.
    pub meta_lba: u32,
    /// File size in bytes (from ICB info_length)
    pub size: u64,
    /// Child entries (if directory)
    pub entries: Vec<DirEntry>,
}

impl UdfFs {
    /// Test hook: relocate the physical partition (ICBs stay put via the metadata map).
    #[cfg(test)]
    pub(crate) fn set_partition_start(&mut self, start: u32) {
        self.partition_start = start;
    }

    /// Physical partition start sector.
    pub fn partition_start(&self) -> u32 {
        self.partition_start
    }

    /// Metadata partition start sector.
    pub fn metadata_start(&self) -> u32 {
        self.meta.start()
    }

    /// Metadata partition size in sectors.
    pub(crate) fn metadata_sectors(&self) -> u32 {
        self.metadata_sectors
    }

    /// Find a directory by path (e.g. "/BDMV/PLAYLIST").
    /// Path matching is case-insensitive.
    pub fn find_dir(&self, path: &str) -> Option<&DirEntry> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut current = &self.root;
        for part in &parts {
            current = current
                .entries
                .iter()
                .find(|e| e.is_dir && e.name.eq_ignore_ascii_case(part))?;
        }
        Some(current)
    }

    /// Get the absolute starting LBA of a file's first data extent on disc.
    /// Used by the rip pipeline to locate m2ts content sectors.
    pub fn file_start_lba(&self, reader: &mut dyn SectorSource, path: &str) -> Result<u32> {
        let entry = self.entry_at(path)?;
        let data_lba = self.read_icb_extent(reader, entry.meta_lba)?.lba;
        self.partition_start
            .checked_add(data_lba)
            .ok_or(Error::DiscRead {
                sector: self.partition_start as u64,
                status: None,
                sense: None,
            })
    }

    /// Read a file by path, returning its raw bytes.
    /// Reads all data extents sector by sector from disc — no buffering.
    pub fn read_file(&self, reader: &mut dyn SectorSource, path: &str) -> Result<Vec<u8>> {
        self.read_file_limited(reader, path, None)
    }

    /// Read at most `max_bytes` of a file (rounded up to a whole sector),
    /// stopping early rather than reading the whole file.
    ///
    /// Used to read only the real, record-length portion of the AACS
    /// `MKB_RO.inf` — allocated to a fixed ~128 MiB and zero-padded — instead
    /// of reading 100+ MiB of padding (and tripping `MAX_FILE_BYTES`). The
    /// caller trims the returned prefix to the MKB record length.
    pub fn read_file_prefix(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.read_file_limited(reader, path, Some(max_bytes))
    }

    // Shared impl of read_file/read_file_prefix. When max_bytes is Some, reads
    // at most that many bytes and skips the MAX_FILE_BYTES cap (already bounded).
    fn read_file_limited(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>> {
        let entry = self.entry_at(path)?;

        // `max_bytes == None` => read the whole file; `Some(n)` => read at most
        // n bytes (rounded up to a sector) and skip the anti-DoS caps below.
        let limit = max_bytes.unwrap_or(usize::MAX);

        // Tiny files (notably AACS `*.inf` key files) may store data embedded inline in
        // the ICB (AD type 3), with no out-of-line extents. Honor that before the extent
        // path, which would otherwise misparse embedded bytes as ADs and hard-error.
        if let Some(mut inline) = self.read_inline_data(reader, entry.meta_lba)? {
            let want = usize::try_from(entry.size).unwrap_or(usize::MAX).min(limit);
            if inline.len() < want {
                return Err(Error::DiscRead {
                    sector: u64::from(self.meta_to_abs(entry.meta_lba)?),
                    status: None,
                    sense: None,
                });
            }
            if inline.len() > want {
                inline.truncate(want);
            }
            return Ok(inline);
        }

        // Read the file's data extents. Multi-extent files (fragmented or split across
        // dual layers) would otherwise be silently truncated to the first extent, since
        // the buffer is sized to entry.size and truncate() can't grow it.
        let extents = self.read_icb_extents(reader, entry.meta_lba)?;

        // Reject an oversized declared total before allocating: entry.size is a raw u64
        // off the ICB, so a crafted file could force a GiB allocation. Only for the
        // UNBOUNDED path — a bounded read is already limited by `limit`.
        if max_bytes.is_none() && entry.size > MAX_FILE_BYTES {
            return Err(Error::DiscRead {
                sector: self.partition_start as u64,
                status: None,
                sense: None,
            });
        }

        // File DATA lives in the physical partition (partition_start + lba), NOT the
        // metadata partition where ICBs live. Pre-allocate to declared-size/prefix,
        // capped so a bogus entry.size can't force a GiB-scale reservation.
        let cap_hint = (entry.size as usize)
            .min(limit)
            .min(MAX_FILE_BYTES as usize);
        let mut data = Vec::with_capacity(cap_hint);
        let mut sector = [0u8; SECTOR_BYTES];
        'extents: for ext in extents {
            let (data_lba, data_len) = (ext.lba, ext.len);
            if max_bytes.is_none() {
                // Anti-DoS guard for the unbounded path: a crafted ICB could chain
                // extents whose running total (or a single extent) exceeds the cap.
                // Skipped when bounded — `limit` already caps the read.
                if data.len() as u64 + data_len as u64 > MAX_FILE_BYTES
                    || data_len as u64 > MAX_FILE_BYTES
                {
                    return Err(Error::DiscRead {
                        sector: self.partition_start as u64,
                        status: None,
                        sense: None,
                    });
                }
            }
            let sector_count = (data_len as u64).div_ceil(SECTOR_BYTES_U64) as u32;
            // ECMA-167 4/14.14.1.1 type 1: allocated-not-recorded. Its bytes are defined
            // as zeros, so emit zeros WITHOUT reading media. Skipping the extent instead
            // would slide every later extent's bytes down by this hole's length.
            if !ext.recorded {
                for _ in 0..sector_count {
                    if data.len() >= limit {
                        break 'extents;
                    }
                    data.extend_from_slice(&[0u8; SECTOR_BYTES]);
                }
                continue;
            }
            let abs_start = self
                .partition_start
                .checked_add(data_lba)
                .ok_or(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                })?;
            for i in 0..sector_count {
                if data.len() >= limit {
                    break 'extents;
                }
                let abs = abs_start.checked_add(i).ok_or(Error::DiscRead {
                    sector: abs_start as u64,
                    status: None,
                    sense: None,
                })?;
                read_sector(reader, abs, &mut sector)?;
                data.extend_from_slice(&sector);
            }
        }

        // Trim to the real file size, or to the requested prefix — whichever is
        // smaller. Extents covering less than that mean a damaged File Entry.
        let trim_to = (entry.size as usize).min(limit);
        if data.len() < trim_to {
            return Err(Error::DiscRead {
                sector: self.partition_start as u64,
                status: None,
                sense: None,
            });
        }
        if data.len() > trim_to {
            data.truncate(trim_to);
        }
        Ok(data)
    }

    /// Collect all sector ranges needed for disc-info and AACS.
    ///
    /// Returns a list of (start_lba, sector_count) ranges covering:
    ///   - UDF structure (AVDP, VDS, metadata partition, directories)
    ///   - every non-STREAM file the tree walk reaches that is <= 50 MB
    ///
    /// `STREAM` directories (case-insensitive) are not descended, and files over 50 MB are
    /// skipped.
    pub fn metadata_sector_ranges(&self, reader: &mut dyn SectorSource) -> Result<Vec<(u32, u32)>> {
        self.metadata_sector_ranges_for(reader, &|_| true)
    }

    /// [`Self::metadata_sector_ranges`] over the files `wanted` accepts, by absolute path
    /// (e.g. `/BDMV/PLAYLIST/00000.mpls`): a file it refuses gets no range and its File Entry
    /// is not read. The live scan's prefetch plan.
    pub(crate) fn metadata_sector_ranges_for(
        &self,
        reader: &mut dyn SectorSource,
        wanted: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<(u32, u32)>> {
        let mut ranges = Vec::new();

        // UDF structure: sector 0 through end of metadata partition
        // Covers AVDP, VDS, partition descriptor, metadata ICB, FSD, all directories
        // Both ends are disc-controlled: clamp, and cover the metadata start separately.
        let meta_end = self.meta.start().saturating_add(self.metadata_sectors);
        ranges.push((0, meta_end.min(MAX_STRUCT_SECTORS)));
        if meta_end > MAX_STRUCT_SECTORS {
            ranges.push((
                self.meta.start(),
                self.metadata_sectors.min(MAX_STRUCT_SECTORS),
            ));
        }
        // A fragmented Metadata File's later extents live elsewhere on disc.
        ranges.extend(self.meta.0.iter().skip(1).copied());

        // Walk tree, collect ranges for each metadata file
        let mut seen = HashSet::new();
        let mut path = String::new();
        self.collect_file_ranges(
            reader,
            &self.root,
            &mut path,
            wanted,
            &mut ranges,
            &mut seen,
        )?;

        // Merge overlapping/adjacent ranges and sort
        ranges.sort_by_key(|r| r.0);
        let merged = merge_ranges(&ranges);
        Ok(merged)
    }

    /// Every sector a scan of this filesystem reads that is NOT Clip AV stream data:
    /// the volume structures before the partition, the Metadata File, every directory
    /// and File Entry (stream files' included), and every file's data outside
    /// `/BDMV/STREAM` (any size).
    ///
    /// AACS BD Pre-recorded Book 0.953 §3.7: "the BEF shall be set to 0b for the sectors
    /// that do not correspond to Clip AV stream files under \BDMV\STREAM directory." None
    /// of these is bus-encrypted. A file whose File Entry cannot be read is left out.
    pub(crate) fn non_stream_ranges(
        &self,
        reader: &mut dyn SectorSource,
    ) -> Result<Vec<(u32, u32)>> {
        let mut ranges = vec![(0, self.partition_start)];
        ranges.push((self.meta.start(), self.metadata_sectors.max(1)));
        ranges.extend(self.meta.0.iter().copied().filter(|&(_, n)| n > 0));
        let stream = self.find_dir("/BDMV/STREAM");
        // (entry, inside /BDMV/STREAM): there only the File Entries are staged.
        let mut stack = vec![(&self.root, false)];
        let mut seen = HashSet::new();
        while let Some((e, in_stream)) = stack.pop() {
            if e.is_dir {
                for c in &e.entries {
                    stack.push((c, in_stream || stream.is_some_and(|s| std::ptr::eq(s, c))));
                }
            }
            ranges.push((self.meta_to_abs(e.meta_lba)?, 1));
            if !e.is_dir && in_stream {
                continue;
            }
            // Entries can share one ICB: read its extents once.
            if !seen.insert(e.meta_lba) {
                continue;
            }
            match self.data_extents_abs(reader, e) {
                Ok(exts) => ranges.extend(
                    exts.iter()
                        .filter(|x| x.recorded && x.len > 0)
                        .map(|x| (x.lba, (x.len as u64).div_ceil(SECTOR_BYTES_U64) as u32)),
                ),
                // Embedded data lives in the File Entry already staged above.
                Err(Error::UdfEmbeddedData) => {}
                // An unread File Entry would stage a tree missing that file's data.
                Err(e) => return Err(e),
            }
            compact_ranges(&mut ranges, self.meta.start())?;
        }
        ranges.sort_by_key(|r| r.0);
        Ok(merge_ranges(&ranges))
    }

    // A directory's FID data is metadata-partition-relative, a file's physical-partition-relative.
    fn data_extents_abs(
        &self,
        reader: &mut dyn SectorSource,
        e: &DirEntry,
    ) -> Result<Vec<AbsExtent>> {
        if !e.is_dir {
            return self.extents_abs_at(reader, e.meta_lba);
        }
        self.read_icb_extents(reader, e.meta_lba)?
            .into_iter()
            .map(|x| {
                Ok(AbsExtent {
                    lba: self.meta_to_abs(x.lba)?,
                    len: x.len,
                    recorded: x.recorded,
                })
            })
            .collect()
    }

    // `path` is `entry`'s absolute path ("" for the root), restored on return.
    fn collect_file_ranges(
        &self,
        reader: &mut dyn SectorSource,
        entry: &DirEntry,
        path: &mut String,
        wanted: &dyn Fn(&str) -> bool,
        ranges: &mut Vec<(u32, u32)>,
        seen: &mut HashSet<u32>,
    ) -> Result<()> {
        for child in &entry.entries {
            let at = path.len();
            path.push('/');
            path.push_str(&child.name);
            let r = self.collect_child_ranges(reader, child, path, wanted, ranges, seen);
            path.truncate(at);
            r?;
        }
        Ok(())
    }

    fn collect_child_ranges(
        &self,
        reader: &mut dyn SectorSource,
        child: &DirEntry,
        path: &mut String,
        wanted: &dyn Fn(&str) -> bool,
        ranges: &mut Vec<(u32, u32)>,
        seen: &mut HashSet<u32>,
    ) -> Result<()> {
        if child.is_dir {
            // Only skip STREAM — those are the multi-GB video files
            if child.name.eq_ignore_ascii_case("STREAM") {
                return Ok(());
            }
            return self.collect_file_ranges(reader, child, path, wanted, ranges, seen);
        }
        // A file the scan never reads is not prefetched (nor its File Entry read).
        if !wanted(path) {
            return Ok(());
        }
        // Include the ICB sector itself (in metadata partition)
        ranges.push((self.meta_to_abs(child.meta_lba)?, 1));
        // Include file data — skip only truly huge files (MKB_RO.inf = 134MB)
        if child.size > MAX_CACHED_FILE_BYTES {
            return Ok(());
        }
        // Push every extent (a fragmented AACS cert / MPLS / CLPI spans several);
        // entries can share one ICB, so read its extents once.
        if !seen.insert(child.meta_lba) {
            return Ok(());
        }
        match self.read_icb_extents(reader, child.meta_lba) {
            Err(Error::Halted) => return Err(Error::Halted),
            Err(_) => {}
            Ok(extents) => {
                for ext in extents {
                    // An unrecorded extent holds nothing to cache.
                    if !ext.recorded {
                        continue;
                    }
                    let Some(abs_start) = self.partition_start.checked_add(ext.lba) else {
                        continue;
                    };
                    let sector_count = (ext.len as u64).div_ceil(SECTOR_BYTES_U64) as u32;
                    ranges.push((abs_start, sector_count));
                }
                compact_ranges(ranges, self.meta.start())?;
            }
        }
        Ok(())
    }

    /// Convert a metadata-partition-relative LBA to an absolute sector number.
    /// `meta_lba` is disc-controlled, so the sum is checked to avoid a
    /// wrap-to-wrong-sector on a crafted ICB.
    fn meta_to_abs(&self, meta_lba: u32) -> Result<u32> {
        self.meta.to_abs(meta_lba)
    }

    // Returns the file's first RECORDED extent (not extents.first(), which may be an unrecorded
    // type-1 hole).
    fn read_icb_extent(&self, reader: &mut dyn SectorSource, meta_lba: u32) -> Result<IcbExtent> {
        let extents = self.read_icb_extents(reader, meta_lba)?;
        extents
            .iter()
            .find(|e| e.recorded)
            .copied()
            .ok_or(Error::DiscRead {
                // Diagnostic sector only; meta_to_abs can overflow on a crafted
                // meta_lba, in which case 0 is a harmless placeholder for the
                // error-context field.
                sector: self.meta_to_abs(meta_lba).unwrap_or(0) as u64,
                status: None,
                sense: None,
            })
    }

    // If this ICB embeds its data inline (ICB Tag flags low 3 bits == 3), return those bytes;
    // `Ok(None)` for the normal extent-backed case.
    fn read_inline_data(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Option<Vec<u8>>> {
        let icb_abs = self.meta_to_abs(meta_lba)?;
        let mut icb = [0u8; SECTOR_BYTES];
        read_sector(reader, icb_abs, &mut icb)?;
        let tag = u16::from_le_bytes([icb[0], icb[1]]);
        let (ad_offset, l_ad) = match tag {
            // Extended File Entry (266) / standard File Entry (261): the
            // allocation-descriptors field (which, for embedded files, holds
            // the data itself) begins after the extended attributes.
            266 => {
                let l_ea = u32::from_le_bytes([icb[208], icb[209], icb[210], icb[211]]) as usize;
                let l_ad = u32::from_le_bytes([icb[212], icb[213], icb[214], icb[215]]) as usize;
                (216 + l_ea, l_ad)
            }
            261 => {
                let l_ea = u32::from_le_bytes([icb[168], icb[169], icb[170], icb[171]]) as usize;
                let l_ad = u32::from_le_bytes([icb[172], icb[173], icb[174], icb[175]]) as usize;
                (176 + l_ea, l_ad)
            }
            _ => return Ok(None),
        };
        // ICB Tag flags: u16 at absolute offset 34, low 3 bits select the AD
        // type. 3 == data embedded inline in the ICB.
        let icb_flags = u16::from_le_bytes([icb[34], icb[35]]);
        if (icb_flags & 0x07) != 3 {
            return Ok(None);
        }
        if ad_offset > icb.len() || ad_offset + l_ad > icb.len() {
            return Err(Error::DiscRead {
                sector: icb_abs as u64,
                status: None,
                sense: None,
            });
        }
        Ok(Some(icb[ad_offset..ad_offset + l_ad].to_vec()))
    }

    // Read ALL allocation extents for a file from its ICB, in file order,
    // including multi-block continuation (extent_type 3) descriptors.
    // Unrecorded (type-1) extents are returned too — see [`IcbExtent`].
    fn read_icb_extents(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Vec<IcbExtent>> {
        let icb_abs = self.meta_to_abs(meta_lba)?;
        let mut icb = [0u8; SECTOR_BYTES];
        read_sector(reader, icb_abs, &mut icb)?;

        let tag = u16::from_le_bytes([icb[0], icb[1]]);

        // Get allocation descriptor offset and total length based on ICB type
        let (ad_offset, l_ad) = match tag {
            // Extended File Entry (UDF 2.50, used by BD-ROM)
            266 => {
                let l_ea = u32::from_le_bytes([icb[208], icb[209], icb[210], icb[211]]) as usize;
                let l_ad = u32::from_le_bytes([icb[212], icb[213], icb[214], icb[215]]) as usize;
                let ad_offset = 216 + l_ea;
                if ad_offset + l_ad > icb.len() {
                    return Err(Error::DiscRead {
                        sector: icb_abs as u64,
                        status: None,
                        sense: None,
                    });
                }
                (ad_offset, l_ad)
            }
            // Standard File Entry
            261 => {
                let l_ea = u32::from_le_bytes([icb[168], icb[169], icb[170], icb[171]]) as usize;
                let l_ad = u32::from_le_bytes([icb[172], icb[173], icb[174], icb[175]]) as usize;
                let ad_offset = 176 + l_ea;
                if ad_offset + l_ad > icb.len() {
                    return Err(Error::DiscRead {
                        sector: icb_abs as u64,
                        status: None,
                        sense: None,
                    });
                }
                (ad_offset, l_ad)
            }
            _ => {
                return Err(Error::DiscRead {
                    sector: icb_abs as u64,
                    status: None,
                    sense: None,
                });
            }
        };

        // AD type lives in ICB Tag flags (low 3 bits) at offset 34: 0=Short(8B),
        // 1=Long(16B), 2=Extended(20B), 3=embedded. Must be honoured — a hardcoded
        // 8-byte stride on Long-AD files reads impl_use garbage, truncating BD-ROM titles.
        let icb_flags = u16::from_le_bytes([icb[34], icb[35]]);
        let ad_type = (icb_flags & 0x07) as usize;
        // Type 3 is EMBEDDED data (field holds file content, not ADs); misreading it as
        // (length, LBA) pairs manufactures bogus extents a rip emits as stream data at
        // rc=0, so this must ERROR. Only tiny files use it (2 KiB cap, e.g. AACS keys).
        if ad_type == 3 {
            // ...except a zero-length file, legally encodable as embedded-with-nothing:
            // no extents and no content, so return the empty list rather than error,
            // matching what a zero-length file already produces today.
            if l_ad == 0 {
                return Ok(Vec::new());
            }
            return Err(Error::UdfEmbeddedData);
        }
        let ad_size: usize = match ad_type {
            0 => 8,  // Short AD
            1 => 16, // Long AD
            2 => 20, // Extended AD
            // Anything else is reserved by 4/14.6.8; fall back to the
            // historical 8-byte stride rather than fail the whole title.
            _ => 8,
        };

        let mut extents = Vec::new();

        // Parse the first allocation-descriptor list from the ICB. A type-3 descriptor
        // ("next extent of allocation descriptors") points at a continuation block in
        // the metadata partition; follow the chain, bounding hops to avoid looping.
        let mut block = icb;
        let mut ad_start = ad_offset;
        let mut ad_bytes = l_ad;
        const MAX_AD_BLOCKS: usize = 256;
        // Set only when a block ends the chain (no continuation pointer). Running out
        // of hops instead leaves `extents` describing only PART of the file, which is
        // worse than failing — see the error's own documentation.
        let mut chain_ended = false;
        let mut seen_aeds: Vec<u32> = Vec::new();

        for _ in 0..MAX_AD_BLOCKS {
            let num_descriptors = ad_bytes / ad_size;
            let mut next_block: Option<u32> = None;

            for i in 0..num_descriptors {
                let off = ad_start + i * ad_size;
                if off + ad_size > block.len() {
                    break;
                }

                let raw_len = u32::from_le_bytes([
                    block[off],
                    block[off + 1],
                    block[off + 2],
                    block[off + 3],
                ]);
                let extent_type = raw_len >> 30;
                let data_len = raw_len & 0x3FFF_FFFF;
                // Short and Long ADs carry the extent LBA at off+4. Extended
                // ADs (20 bytes) place their extent_location lb_addr after
                // three length fields, at off+12.
                let lba_off = if ad_size == 20 { off + 12 } else { off + 4 };
                let data_lba = u32::from_le_bytes([
                    block[lba_off],
                    block[lba_off + 1],
                    block[lba_off + 2],
                    block[lba_off + 3],
                ]);

                // Any AD whose 30-bit length is 0 ends the list (ECMA-167 4/12), whatever
                // its type; continuation blocks' trailing zero padding stops here too.
                if data_len == 0 {
                    break;
                }
                match extent_type {
                    0 => extents.push(IcbExtent {
                        lba: data_lba,
                        len: data_len,
                        recorded: true,
                    }),
                    // Types 1/2 (ECMA-167 4/14.14.1.1) are sparse holes: no on-disc data,
                    // but the extent occupies `data_len` bytes of file space, so it must
                    // be KEPT with `recorded: false` — dropping it silently truncates/corrupts.
                    1 | 2 => extents.push(IcbExtent {
                        lba: data_lba,
                        len: data_len,
                        recorded: false,
                    }),
                    3 => {
                        // Continuation: the rest of the ADs live in the block
                        // at data_lba (metadata-partition-relative). Stop
                        // scanning this block and follow the pointer.
                        next_block = Some(data_lba);
                        break;
                    }
                    // Unreachable: `extent_type` is 2-bit `raw_len >> 30`, 0-3 all
                    // handled above; kept as a conservative stop, not a panic. New
                    // types must be handled here, else this silently truncates (as type 2 once was).
                    _ => break,
                }
            }

            match next_block {
                Some(cont_lba) => {
                    let abs = self.meta_to_abs(cont_lba)?;
                    (ad_start, ad_bytes) = read_aed(reader, abs, &mut block, &mut seen_aeds)?;
                }
                None => {
                    chain_ended = true;
                    break;
                }
            }
        }

        if !chain_ended {
            return Err(Error::UdfAdChainTooLong);
        }

        Ok(extents)
    }

    /// If the ICB at `meta_lba` stores its data inline (embedded, AD type 3),
    /// return the embedded bytes; `Ok(None)` for the normal extent-backed case.
    /// Public wrapper over [`read_inline_data`](Self::read_inline_data) so the
    /// per-file tree extractor can honor inline nav files without re-walking a
    /// path. The caller trims to the entry's declared `size`.
    pub fn inline_data_at(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Option<Vec<u8>>> {
        self.read_inline_data(reader, meta_lba)
    }

    // Absolute disc extents (absolute_lba, byte_length) for the ICB at meta_lba, keyed by ICB
    // LBA rather than path. Unrecorded extents are kept but flagged.
    pub(crate) fn extents_abs_at(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Vec<AbsExtent>> {
        let alloc = self.read_icb_extents(reader, meta_lba)?;
        let mut out = Vec::with_capacity(alloc.len());
        for ext in alloc {
            let abs = self
                .partition_start
                .checked_add(ext.lba)
                .ok_or(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                })?;
            out.push(AbsExtent {
                lba: abs,
                len: ext.len,
                recorded: ext.recorded,
            });
        }
        Ok(out)
    }

    /// Absolute disc extents `(absolute_lba, sector_count)` for a file, for a
    /// caller that will READ those sectors as the file's content — a title's
    /// play plan.
    ///
    /// Refuses, with [`Error::UdfUnrecordedExtent`], a file containing an unrecorded (ECMA-167
    /// 4/14.14.1.1 type-1/type-2) extent.
    pub fn file_extents(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
    ) -> Result<Vec<(u32, u32)>> {
        let meta_lba = self.entry_at(path)?.meta_lba;
        let abs = self.extents_abs_at(reader, meta_lba)?;
        // Refuse only a hole that OCCUPIES byte space (zero-length displaces
        // nothing, so `!recorded` alone wrongly drops fine titles). The hazard is
        // LENGTH: reading it splices undefined sectors in; dropping shifts later extents.
        if abs.iter().any(|e| !e.recorded && e.len > 0) {
            return Err(Error::UdfUnrecordedExtent {
                path: path.to_string(),
            });
        }
        span_extents(&abs)
    }

    /// Resolve `path` to its directory entry. Shared by both extent
    /// resolvers so they cannot drift apart on lookup semantics.
    fn entry_at(&self, path: &str) -> Result<&DirEntry> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut current = &self.root;
        for part in &parts[..parts.len() - 1] {
            current = current
                .entries
                .iter()
                .find(|e| e.is_dir && e.name.eq_ignore_ascii_case(part))
                .ok_or_else(|| Error::UdfNotFound {
                    path: part.to_string(),
                })?;
        }
        let filename = match parts.last() {
            Some(f) => f,
            None => {
                return Err(Error::UdfNotFound {
                    path: path.to_string(),
                });
            }
        };
        current
            .entries
            .iter()
            .find(|e| !e.is_dir && e.name.eq_ignore_ascii_case(filename))
            .ok_or_else(|| Error::UdfNotFound {
                path: path.to_string(),
            })
    }

    // Absolute disc extents (absolute_lba, sector_count) for a file, for a caller using the
    // list purely as a byte-space address map — never reading the sectors as content.
    pub(crate) fn file_extents_addressing(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
    ) -> Result<Vec<(u32, u32)>> {
        let meta_lba = self.entry_at(path)?.meta_lba;
        span_extents(&self.extents_abs_at(reader, meta_lba)?)
    }
}

// `(lba, sectors)` per extent; an extent whose end passes the u32 LBA space is corrupt (callers
// add the two).
fn span_extents(abs: &[AbsExtent]) -> Result<Vec<(u32, u32)>> {
    abs.iter()
        .map(|e| {
            let n = (e.len as u64).div_ceil(SECTOR_BYTES_U64) as u32;
            e.lba
                .checked_add(n)
                .map(|_| (e.lba, n))
                .ok_or(Error::DiscRead {
                    sector: e.lba as u64,
                    status: None,
                    sense: None,
                })
        })
        .collect()
}

/// Read the UDF filesystem from a Blu-ray disc.
///
/// Follows the UDF pointer chain:
/// 1. AVDP (sector 256) → VDS location
/// 2. VDS → Partition Descriptor (physical partition start)
///    → Logical Volume Descriptor (FSD location + partition maps)
/// 3. Metadata partition file → metadata content location
/// 4. FSD → root directory ICB
/// 5. Root directory → file tree
pub fn read_filesystem(reader: &mut dyn SectorSource) -> Result<UdfFs> {
    // Step 1: Anchor Volume Descriptor Pointer at sector 256
    // ECMA-167 §10.2 — always at sector 256
    let mut avdp = [0u8; SECTOR_BYTES];
    read_sector(reader, 256, &mut avdp)?;

    let tag_id = u16::from_le_bytes([avdp[0], avdp[1]]);
    if tag_id != 2 {
        // Sector 256 read fine but carries no Anchor Volume Descriptor Pointer:
        // this is deterministically not a UDF disc, not a transient read fault.
        return Err(Error::UdfNotFilesystem);
    }

    // Step 2: find Partition Descriptor (tag 5) / Logical Volume Descriptor (tag
    // 6) in the Main VDS. ECMA-167 3/10.2.1 defines it via the AVDP's extent_ad
    // (length [16:20], LBA [20:24]), not a fixed sector 32 — follow the pointer.
    let vds_len_bytes = u32::from_le_bytes([avdp[16], avdp[17], avdp[18], avdp[19]]);
    let vds_lba = u32::from_le_bytes([avdp[20], avdp[21], avdp[22], avdp[23]]);
    // Candidates: recorded extent then customary location. Recorded is used only
    // if SHAPE is valid (>=16 sectors, non-zero, non-wrapping, ECMA-167 3/10.2.1);
    // fallback also retried on OUTCOME if recorded yields no Partition Descriptor.
    let recorded = match vds_len_bytes.div_ceil(SECTOR_BYTES as u32) {
        n if n >= VDS_MIN_SECTORS && vds_lba > 0 && vds_lba.checked_add(n).is_some() => {
            Some((vds_lba, n.min(VDS_MAX_SECTORS)))
        }
        _ => None,
    };
    let fallback = (VDS_FALLBACK_START, VDS_MAX_SECTORS);
    let mut candidates = Vec::with_capacity(2);
    candidates.extend(recorded);
    // Compare the whole extent, not just its start: an anchor can record the
    // customary LBA but declare a narrower window than the fallback's. Matching
    // on start alone would drop the WIDER sweep, missing anything past it.
    if recorded != Some(fallback) {
        candidates.push(fallback);
    }

    let mut partition_start: u32 = 0;
    let mut num_partition_maps: u32 = 0;
    let mut lvd_sector: Option<u32> = None;
    let mut fsd_block: Option<u32> = None;
    let mut volume_id = String::new();
    let mut metadata_size_bytes: u32 = 0;
    // A read fault inside a sweep must not abort before the next candidate is
    // tried, but it must not vanish either: if no candidate yields a partition,
    // the fault is the honest answer rather than "not a UDF disc".
    let mut sweep_err = None;

    for (vds_start, vds_sectors) in candidates {
        for i in vds_start..vds_start.saturating_add(vds_sectors) {
            let mut desc = [0u8; SECTOR_BYTES];
            if let Err(e) = read_sector(reader, i, &mut desc) {
                sweep_err = Some(e);
                break;
            }

            let desc_tag = u16::from_le_bytes([desc[0], desc[1]]);
            match desc_tag {
                // Primary Volume Descriptor — volume identifier at offset 24, 32-byte d-string
                1 => {
                    volume_id = parse_dstring(&desc[24..56]);
                }
                // Partition Descriptor — tells us where the physical partition starts
                5 => {
                    partition_start =
                        u32::from_le_bytes([desc[188], desc[189], desc[190], desc[191]]);
                }
                // Logical Volume Descriptor — contains FSD location and partition maps
                6 => {
                    num_partition_maps =
                        u32::from_le_bytes([desc[268], desc[269], desc[270], desc[271]]);
                    lvd_sector = Some(i);
                    // Logical Volume Contents Use: FSD long_ad (length @248, block @252).
                    fsd_block = Some(u32::from_le_bytes([
                        desc[252], desc[253], desc[254], desc[255],
                    ]))
                    .filter(|&b| b != 0);
                }
                // Terminating Descriptor — end of VDS
                8 => break,
                _ => continue,
            }
        }
        // A fault before the LVD leaves the next candidate to try; the fault is only
        // surfaced below where the disc would otherwise read as not UDF.
        if partition_start != 0 && (lvd_sector.is_some() || sweep_err.is_none()) {
            break;
        }
    }

    if partition_start == 0 {
        // No Partition Descriptor in any candidate sequence. A sweep read fault
        // is transient/retryable; "read fine, bytes say no" is a real verdict
        // that `mux::resolve` caches for the whole disc — don't conflate the two.
        if let Some(e) = sweep_err {
            return Err(e);
        }
        return Err(Error::UdfNotFilesystem);
    }

    // Step 3: Parse partition maps from LVD to find metadata partition
    // BD-ROM discs (UDF 2.50) use a metadata partition (Type 2 map with "*UDF Metadata Partition")
    // The metadata file is stored at lba=0 of the physical partition
    let mut meta_extents: Vec<(u32, u32)> = Vec::new();
    let metadata_start = if num_partition_maps >= 2 {
        let lvd_sec = lvd_sector.ok_or_else(|| {
            sweep_err.take().unwrap_or(Error::DiscRead {
                sector: 0,
                status: None,
                sense: None,
            })
        })?;

        // Read LVD to check partition map type
        let mut lvd = [0u8; SECTOR_BYTES];
        read_sector(reader, lvd_sec, &mut lvd)?;

        // Parse partition maps starting at offset 440
        // Map 0 = Type 1 (physical), Map 1 = Type 2 (metadata)
        let pm1_len = lvd[441] as usize;

        if pm1_len > 0 && 440 + pm1_len < SECTOR_BYTES {
            let pm2_map = 440 + pm1_len;
            let pm2_type = lvd[pm2_map]; // Second map type

            if pm2_type == 2 {
                // Type 2 = metadata partition. UDF 2.50 2.2.10 records the Metadata
                // File's location in the map (offset 40), not always block 0; trusted
                // only when type ID reads "*UDF Metadata Partition" (block 0 still tried as fallback).
                let recorded = metadata_file_location(&lvd, pm2_map)
                    .and_then(|loc| partition_start.checked_add(loc));
                let mut meta_file_lba = partition_start;
                let mut meta_icb = [0u8; SECTOR_BYTES];
                let mut meta_tag = 0u16;
                // Track whether ANY candidate was readable: "read fine, not a File
                // Entry" is deterministic, "could not read either" is transient —
                // collapsing them risks `mux::resolve` caching a flaky read as `UdfNotFilesystem`.
                let mut last_err = None;
                for cand in recorded.into_iter().chain(std::iter::once(partition_start)) {
                    match read_sector(reader, cand, &mut meta_icb) {
                        Ok(()) => {
                            meta_tag = u16::from_le_bytes([meta_icb[0], meta_icb[1]]);
                            meta_file_lba = cand;
                            if meta_tag == 266 {
                                break;
                            }
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                // Not `meta_tag == 0`: the RECORDED candidate can fault while block 0
                // reads fine as some other descriptor, leaving meta_tag non-zero and
                // not 266 — without this check the fault is laundered into a verdict.
                if meta_tag != 266
                    && let Some(e) = last_err
                {
                    return Err(e);
                }

                if meta_tag == 266 {
                    let (extents, first_bytes) =
                        metadata_file_extents(&meta_icb, partition_start, meta_file_lba)?;
                    metadata_size_bytes = first_bytes;
                    let start = extents.first().map_or(partition_start, |e| e.0);
                    meta_extents = extents;
                    start
                } else {
                    // Fallback: no metadata partition, use physical partition directly
                    partition_start
                }
            } else {
                partition_start
            }
        } else {
            partition_start
        }
    } else {
        // Single partition map — no metadata partition (older UDF)
        partition_start
    };

    let meta = if meta_extents.is_empty() {
        MetaMap::contiguous(metadata_start)
    } else {
        MetaMap(meta_extents)
    };

    // Step 4: File Set Descriptor at the block the LVD records (ECMA-167 3/10.6.13),
    // falling back to metadata block 0 (the customary location).
    let mut fsd = [0u8; SECTOR_BYTES];
    let mut fsd_found = false;
    let mut fsd_err = None;
    for cand in fsd_block.into_iter().chain(std::iter::once(0)) {
        match meta
            .to_abs(cand)
            .and_then(|abs| read_sector(reader, abs, &mut fsd))
        {
            Ok(()) if u16::from_le_bytes([fsd[0], fsd[1]]) == 256 => {
                fsd_found = true;
                break;
            }
            Ok(()) => {}
            Err(e) => fsd_err = Some(e),
        }
    }
    if !fsd_found {
        // A read fault is transient; "read fine, no FSD" is structurally not UDF.
        return Err(fsd_err.or(sweep_err).unwrap_or(Error::UdfNotFilesystem));
    }

    // Root Directory ICB: long_ad at FSD offset 400
    // long_ad = extent_length(4) + extent_location: lba(4) + part_ref(2) + impl_use(6)
    let root_lba = u32::from_le_bytes([fsd[404], fsd[405], fsd[406], fsd[407]]);

    // Step 5: Read root directory and build file tree.
    // Pre-seed visited with the root ICB so that any FID pointing back to
    // root_lba is detected as a cycle immediately.
    let root_icb_key = ((meta.start() as u64) << 32) | root_lba as u64;
    let mut visited: HashSet<u64> = HashSet::from([root_icb_key]);
    let root = read_directory(
        reader,
        &mut 0,
        &meta,
        root_lba,
        "",
        0,
        &mut 0usize,
        &mut visited,
    )?;

    let metadata_sectors = (metadata_size_bytes as u64).div_ceil(SECTOR_BYTES_U64) as u32;

    Ok(UdfFs {
        root,
        volume_id,
        partition_start,
        meta,
        metadata_sectors,
    })
}

// The Metadata File's recorded extents `(abs_start, sectors)` from its File Entry, plus the
// first extent's byte length. Only recorded (type 0) ADs can hold metadata blocks.
fn metadata_file_extents(
    fe: &[u8; SECTOR_BYTES],
    partition_start: u32,
    fe_lba: u32,
) -> Result<(Vec<(u32, u32)>, u32)> {
    let bad = Error::DiscRead {
        sector: fe_lba as u64,
        status: None,
        sense: None,
    };
    let l_ea = u32::from_le_bytes([fe[208], fe[209], fe[210], fe[211]]) as usize;
    let l_ad = u32::from_le_bytes([fe[212], fe[213], fe[214], fe[215]]) as usize;
    let ad_size = match fe[34] & 0x07 {
        1 => 16,
        2 => 20,
        3 => return Err(bad), // embedded: no extents at all
        _ => 8,
    };
    let ad_off = 216 + l_ea;
    if ad_off + ad_size > fe.len() {
        return Err(bad);
    }
    // At least the first AD, as before L_AD was honoured.
    let n = (l_ad / ad_size).clamp(1, (fe.len() - ad_off) / ad_size);
    let mut extents = Vec::new();
    let mut first_bytes = 0;
    for i in 0..n {
        let o = ad_off + i * ad_size;
        let raw = u32::from_le_bytes([fe[o], fe[o + 1], fe[o + 2], fe[o + 3]]);
        let len = raw & 0x3FFF_FFFF;
        // Metadata lives only in recorded (type 0) ADs: a bad first AD is fatal,
        // a later one ends the map.
        if raw >> 30 != 0 {
            if i == 0 {
                return Err(bad);
            }
            break;
        }
        if len == 0 && i > 0 {
            break;
        }
        let lba_off = if ad_size == 20 { o + 12 } else { o + 4 };
        let pos = u32::from_le_bytes([
            fe[lba_off],
            fe[lba_off + 1],
            fe[lba_off + 2],
            fe[lba_off + 3],
        ]);
        let abs = partition_start.checked_add(pos).ok_or(Error::DiscRead {
            sector: partition_start as u64,
            status: None,
            sense: None,
        })?;
        if i == 0 {
            first_bytes = len;
        }
        extents.push((abs, len.div_ceil(SECTOR_BYTES as u32)));
    }
    Ok((extents, first_bytes))
}

// UDF 2.50 2.2.10 Metadata File Location: partition-relative block of the Metadata File's File
// Entry, at offset 40 of the Type 2 map at `map`. None if the map doesn't fit, or isn't "*UDF
// Metadata Partition".
fn metadata_file_location(lvd: &[u8; SECTOR_BYTES], map: usize) -> Option<u32> {
    // ECMA-167 3/10.7.3 fixes the Type 2 map at 64 bytes.
    if map.checked_add(64)? > lvd.len() {
        return None;
    }
    // EntityID (ECMA-167 1/7.4): a flags byte then 23 identifier characters.
    if &lvd[map + 5..map + 28] != b"*UDF Metadata Partition" {
        return None;
    }
    Some(u32::from_le_bytes([
        lvd[map + 40],
        lvd[map + 41],
        lvd[map + 42],
        lvd[map + 43],
    ]))
}

/// Maximum directory nesting depth followed when building the tree.
/// Bounds recursion on a corrupt/looping disc; real BD-ROM and DVD trees
/// are far shallower (BDMV/BACKUP/BDJO is the deepest standard path at 3).
const MAX_DIR_DEPTH: u32 = 8;

// Global cap on directory entries (FIDs) visited across the tree walk. Real
// discs have at most a few thousand; 100_000 makes a deep/cyclic attack tree
// terminate in microseconds instead of an astronomical number of visits.
const MAX_TOTAL_DIR_ENTRIES: usize = 100_000;

// Tree-wide cap on directory sectors read (32 MiB); `sectors_read` below only bounds one
// directory, and distinct subdirectory ICBs can each declare a full-size extent.
const MAX_TOTAL_DIR_SECTORS: u32 = 16_384;

// Recursive UDF directory-tree walk (up to MAX_DIR_DEPTH); `budget` caps FIDs visited
// tree-wide, `dir_sectors` caps sectors read tree-wide, `visited` detects ICB-LBA cycles.
// Wide arg list is inherent to the walk, not a refactor smell.
#[allow(clippy::too_many_arguments)]
fn read_directory(
    reader: &mut dyn SectorSource,
    dir_sectors: &mut u32,
    meta: &MetaMap,
    meta_lba: u32,
    name: &str,
    depth: u32,
    budget: &mut usize,
    visited: &mut HashSet<u64>,
) -> Result<DirEntry> {
    // Read ICB for this directory
    let icb_abs = meta.to_abs(meta_lba)?;
    let mut icb = [0u8; SECTOR_BYTES];
    read_sector(reader, icb_abs, &mut icb)?;

    let tag = u16::from_le_bytes([icb[0], icb[1]]);

    // ECMA-167 4/14.6.8 ICB Tag flags: Uint16 at offset 34, low 3 bits pick
    // 0=short_ad, 1=long_ad, 2=extended_ad, 3=EMBEDDED (data in the entry
    // itself). Disc-controlled; same flags also drive `read_icb_extents`/`read_inline_data`.
    let ad_type = u16::from_le_bytes([icb[34], icb[35]]) & 0x07;

    // Where the allocation-descriptor field of this entry begins, and its
    // declared length. ECMA-167 4/14.17 (266): L_EA at 208, L_AD at 212, field
    // at 216 + L_EA. 4/14.9 (261): L_EA at 168, L_AD at 172, field at 176 + L_EA.
    let (ad_off, l_ad) = match tag {
        266 => {
            let l_ea = u32::from_le_bytes([icb[208], icb[209], icb[210], icb[211]]) as usize;
            let l_ad = u32::from_le_bytes([icb[212], icb[213], icb[214], icb[215]]) as usize;
            (216 + l_ea, l_ad)
        }
        261 => {
            let l_ea = u32::from_le_bytes([icb[168], icb[169], icb[170], icb[171]]) as usize;
            let l_ad = u32::from_le_bytes([icb[172], icb[173], icb[174], icb[175]]) as usize;
            (176 + l_ea, l_ad)
        }
        // Only tag 261/266 can be a dir's ICB; any other tag must fail (not
        // return an empty DirEntry, which would hide corruption as a genuinely
        // empty dir). `read_icb_extents` is equally strict for file ICBs.
        _ => {
            return Err(Error::DiscRead {
                sector: icb_abs as u64,
                status: None,
                sense: None,
            });
        }
    };

    // EMBEDDED (AD type 3): FIDs are the descriptor field itself, not an extent.
    // Decoding it as a length/LBA pair would enumerate an unrelated sector,
    // silently yielding an EMPTY directory instead of an error.
    let (dir_data, ad_len) = if ad_type == 3 {
        if ad_off > icb.len() || ad_off + l_ad > icb.len() {
            return Err(Error::DiscRead {
                sector: icb_abs as u64,
                status: None,
                sense: None,
            });
        }
        (icb[ad_off..ad_off + l_ad].to_vec(), l_ad as u32)
    } else {
        // Out-of-line: the FID list may span several ADs (only type 0 holds FIDs). Gather
        // every recorded extent into one buffer, following type-3 continuations.
        let ad_size: usize = match ad_type {
            0 => 8,  // short_ad
            1 => 16, // long_ad
            2 => 20, // extended_ad
            // Reserved (4/14.6.8); fall back to the short_ad stride rather than
            // fail the whole title, matching `read_icb_extents`.
            _ => 8,
        };
        // The ICB's own allocation-descriptor field must fit inside its 2048-byte
        // sector: a disc-controlled L_EA large enough to push the first
        // descriptor past the sector end is corruption, not an out-of-bounds read.
        if l_ad > 0 && ad_off.saturating_add(ad_size) > icb.len() {
            return Err(Error::DiscRead {
                sector: icb_abs as u64,
                status: None,
                sense: None,
            });
        }
        let mut dir_data: Vec<u8> = Vec::new();
        // Declared FID-list byte length (sum of the recorded extents' lengths).
        let mut total_len: u32 = 0;
        let mut sectors_read: u32 = 0;
        let mut seen_aeds: Vec<u32> = Vec::new();
        let mut chain_ended = false;
        let mut block = icb;
        let mut ad_start = ad_off;
        let mut ad_bytes = l_ad;
        const MAX_AD_BLOCKS: usize = 256;
        'chain: for _ in 0..MAX_AD_BLOCKS {
            let avail = block.len().saturating_sub(ad_start);
            let num = ad_bytes.min(avail) / ad_size;
            let mut next_block: Option<u32> = None;

            for i in 0..num {
                let off = ad_start + i * ad_size;
                if off + ad_size > block.len() {
                    break;
                }
                let raw_len = u32::from_le_bytes([
                    block[off],
                    block[off + 1],
                    block[off + 2],
                    block[off + 3],
                ]);
                let extent_type = raw_len >> 30;
                let data_len = raw_len & 0x3FFF_FFFF;
                // Extended ADs place the extent LBA after three length fields.
                let lba_off = if ad_size == 20 { off + 12 } else { off + 4 };
                let data_lba = u32::from_le_bytes([
                    block[lba_off],
                    block[lba_off + 1],
                    block[lba_off + 2],
                    block[lba_off + 3],
                ]);

                if extent_type == 3 {
                    // Continuation (ECMA-167 4/14.14.1.1 type 3): the REST of the ADs live
                    // in the block at data_lba, NOT FID data — reading its (length,LBA) as an
                    // extent (the old first-AD-only bug) enumerates an unrelated sector. Follow the pointer instead (bounded by MAX_AD_BLOCKS).
                    if data_len > 0 {
                        next_block = Some(data_lba);
                    }
                    break;
                }
                // A zero-length descriptor terminates the AD list.
                if data_len == 0 {
                    chain_ended = true;
                    break 'chain;
                }
                // Types 1/2 are unrecorded (4/14.14.1.1): no FID data at data_lba.
                if extent_type != 0 {
                    continue;
                }
                // Bound total sectors READ (not bytes kept): each tiny extent still
                // costs a whole sector, and `data_len` is a disc-controlled 30-bit value.
                let sector_count = data_len.div_ceil(SECTOR_BYTES as u32);
                let add = sector_count as usize * SECTOR_BYTES;
                sectors_read = sectors_read.saturating_add(sector_count);
                if sectors_read > MAX_DIR_BYTES / SECTOR_BYTES as u32 {
                    return Err(Error::DiscRead {
                        sector: meta.start() as u64,
                        status: None,
                        sense: None,
                    });
                }
                *dir_sectors = dir_sectors.saturating_add(sector_count);
                if *dir_sectors > MAX_TOTAL_DIR_SECTORS {
                    return Err(Error::DiscRead {
                        sector: meta.start() as u64,
                        status: None,
                        sense: None,
                    });
                }
                let base = dir_data.len();
                dir_data.resize(base + add, 0);
                for s in 0..sector_count {
                    let rel = data_lba.checked_add(s).ok_or(Error::DiscRead {
                        sector: meta.start() as u64,
                        status: None,
                        sense: None,
                    })?;
                    let abs = meta.to_abs(rel)?;
                    let o = base + s as usize * SECTOR_BYTES;
                    read_sector(reader, abs, &mut dir_data[o..o + SECTOR_BYTES])?;
                }
                // Drop the sector padding so the next extent's FIDs follow directly.
                dir_data.truncate(base + data_len as usize);
                total_len = total_len.saturating_add(data_len);
            }

            match next_block {
                Some(cont_lba) => {
                    let abs = meta.to_abs(cont_lba)?;
                    (ad_start, ad_bytes) = read_aed(reader, abs, &mut block, &mut seen_aeds)?;
                }
                None => {
                    chain_ended = true;
                    break;
                }
            }
        }
        if !chain_ended {
            return Err(Error::UdfAdChainTooLong);
        }
        (dir_data, total_len)
    };

    // Parse File Identifier Descriptors. `dir_data` holds exactly the declared
    // FID bytes (padding truncated above), so its length bounds the walk.
    let dir_end = dir_data.len();
    let mut entries = Vec::new();
    let mut pos = 0;

    // `<=`, not `<`: a header ending exactly at the declared boundary (pos + 38 == dir_end) is
    // fully present. A NAME spilling past `dir_end` is corruption and errors below.
    while pos + 38 <= dir_end {
        let fid_tag = u16::from_le_bytes([dir_data[pos], dir_data[pos + 1]]);
        // Zero is trailing padding; anything else inside the declared length is corruption.
        if fid_tag == 0 {
            break;
        }
        if fid_tag != 257 {
            tracing::warn!(target: "freemkv::udf", fid_tag, icb_abs, "corrupt directory: bad FID tag");
            // A subdirectory lists empty; the root keeps what parsed before the damage.
            if depth > 0 {
                entries.clear();
            }
            break;
        }

        let file_chars = dir_data[pos + 18];
        let l_fi = dir_data[pos + 19] as usize;

        // FID ICB is a long_ad at offset 20: extent_length[20:24],
        // extent_location/LBA[24:28], partition_ref[28:30], implementation_use[30:36].
        let icb_lba = u32::from_le_bytes([
            dir_data[pos + 24],
            dir_data[pos + 25],
            dir_data[pos + 26],
            dir_data[pos + 27],
        ]);
        let l_iu = u16::from_le_bytes([dir_data[pos + 36], dir_data[pos + 37]]) as usize;

        let is_dir = (file_chars & 0x02) != 0;
        let is_parent = (file_chars & 0x08) != 0;
        // ECMA-167 4/14.4.4 bit 2 = Deleted; its ICB may be a zero-length extent
        // (4/14.4.3) not pointing at a File Entry. Following it would fail whole
        // enumeration on a deleted dir, or fabricate a zero-byte entry for a file.
        let is_deleted = (file_chars & 0x04) != 0;

        if !is_parent && !is_deleted && l_fi > 0 {
            let name_start = pos + 38 + l_iu;
            let name_end = name_start + l_fi;
            if name_end > dir_end {
                tracing::warn!(target: "freemkv::udf", icb_abs, "corrupt directory: name overruns FID data");
                if depth > 0 {
                    entries.clear();
                }
                break;
            }
            let entry_name = parse_udf_name(&dir_data[name_start..name_end]);

            if !entry_name.is_empty() {
                // Global entry budget: abort if a crafted disc tries to
                // enumerate an astronomically large tree.
                *budget = budget.saturating_add(1);
                if *budget > MAX_TOTAL_DIR_ENTRIES {
                    return Err(Error::DiscRead {
                        sector: meta.start() as u64,
                        status: None,
                        sense: None,
                    });
                }

                // Read failures must propagate, not become size 0 (indistinguishable
                // from a real empty file). `read_file_size` still returns Ok(0) for
                // ICBs that aren't File/Extended File Entry tags (261/266) — genuine zero.
                let file_size = read_file_size(reader, meta, icb_lba)?;

                if is_dir && depth < MAX_DIR_DEPTH {
                    // Cycle guard: skip any ICB LBA we have already opened as
                    // a directory (self-referential or cross-linked dirs).
                    let icb_key = ((meta.start() as u64) << 32) | icb_lba as u64;
                    if visited.contains(&icb_key) {
                        // Emit as a leaf so the name is preserved but don't
                        // recurse into the cycle.
                        entries.push(DirEntry {
                            name: entry_name,
                            is_dir: true,
                            meta_lba: icb_lba,
                            size: file_size,
                            entries: Vec::new(),
                        });
                    } else {
                        visited.insert(icb_key);
                        // Recurse into subdirectory. The depth cap guards against pathological
                        // nesting on a corrupt disc while covering real BD-ROM nesting
                        // (e.g. BDMV/BACKUP/BDJO/*.bdjo is 3 levels deep).
                        let subdir = match read_directory(
                            reader,
                            dir_sectors,
                            meta,
                            icb_lba,
                            &entry_name,
                            depth + 1,
                            budget,
                            visited,
                        ) {
                            // Unfollowable AD chain: lose only this dir, as Linux/libudfread do.
                            Err(e @ Error::UdfAdChainTooLong) => {
                                tracing::warn!(target: "freemkv::udf", code = e.code(), icb_lba, "subdirectory listed empty");
                                DirEntry {
                                    name: entry_name,
                                    is_dir: true,
                                    meta_lba: icb_lba,
                                    size: file_size,
                                    entries: Vec::new(),
                                }
                            }
                            other => other?,
                        };
                        entries.push(subdir);
                    }
                } else {
                    entries.push(DirEntry {
                        name: entry_name,
                        is_dir,
                        meta_lba: icb_lba,
                        size: file_size,
                        entries: Vec::new(),
                    });
                }
            }
        }

        // Advance to next FID (4-byte aligned)
        let fid_len = (38 + l_iu + l_fi + 3) & !3;
        pos += fid_len;
    }

    Ok(DirEntry {
        name: name.to_string(),
        is_dir: true,
        meta_lba,
        size: ad_len as u64,
        entries,
    })
}

/// Read file size (info_length) from a File Entry (261) or Extended File Entry (266) ICB.
fn read_file_size(reader: &mut dyn SectorSource, meta: &MetaMap, meta_lba: u32) -> Result<u64> {
    let abs = meta.to_abs(meta_lba)?;
    let mut icb = [0u8; SECTOR_BYTES];
    read_sector(reader, abs, &mut icb)?;

    let tag = u16::from_le_bytes([icb[0], icb[1]]);
    match tag {
        // Both File Entry (261) and Extended File Entry (266) have
        // info_length as a u64 at offset 56
        261 | 266 => Ok(u64::from_le_bytes([
            icb[56], icb[57], icb[58], icb[59], icb[60], icb[61], icb[62], icb[63],
        ])),
        _ => Ok(0),
    }
}

// Decode an OSTA CS0 string: first byte is a compression ID (8 = one byte per code point,
// i.e. Latin-1; 16 = UTF-16BE, surrogate pairs combined). Control characters (NUL included)
// are dropped.
fn decode_cs0(data: &[u8]) -> String {
    let Some((&comp, rest)) = data.split_first() else {
        return String::new();
    };
    let s: String = match comp {
        16 => char::decode_utf16(
            rest.as_chunks::<2>()
                .0
                .iter()
                .map(|&c| u16::from_be_bytes(c)),
        )
        .filter_map(|c| c.ok())
        .collect(),
        8 => rest.iter().map(|&b| b as char).collect(),
        _ => String::from_utf8_lossy(rest).into_owned(),
    };
    // Names reach error text and logs: no control characters. A '/' is kept so the
    // structure-file filter still rejects the name.
    s.chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .to_string()
}

// Parse a UDF filename (an OSTA CS0 string).
pub(crate) fn parse_udf_name(data: &[u8]) -> String {
    decode_cs0(data)
}

// Keeps a walk's range list bounded: past MAX_WALK_RANGES, sort+merge in place, and fail (the
// list is disc-controlled) if it is still more than half the cap.
fn compact_ranges(ranges: &mut Vec<(u32, u32)>, meta_start: u32) -> Result<()> {
    if ranges.len() <= MAX_WALK_RANGES {
        return Ok(());
    }
    ranges.sort_by_key(|r| r.0);
    *ranges = merge_ranges(ranges);
    if ranges.len() > MAX_WALK_RANGES / 2 {
        return Err(Error::DiscRead {
            sector: meta_start as u64,
            status: None,
            sense: None,
        });
    }
    Ok(())
}

/// Merge overlapping or adjacent (start, count) ranges. Caller sorts by start
/// first; zero-length ranges are kept (unlike `whole_disc::merge_ranges`, which sorts and
/// drops them). Shared range utility — also used to build the disc's encrypted-content
/// extent map (see `Disc::encrypted_content_ranges`).
pub(crate) fn merge_ranges(ranges: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut result: Vec<(u32, u32)> = Vec::new();
    for &(start, count) in ranges {
        let Some(last) = result.last_mut() else {
            result.push((start, count));
            continue;
        };
        // Saturating arithmetic: ranges derive from disc-controlled ICB
        // LBAs/lengths, so a corrupt disc could otherwise overflow u32
        // (panic in debug, wrap in release).
        let last_end = last.0.saturating_add(last.1);
        // Half-open ranges touch exactly when `start == last_end`; accepting
        // `last_end + 1` too merged across a genuine one-sector hole, falsely
        // reporting coverage `Disc::encrypted_content_ranges` treats as real content.
        if start <= last_end {
            // Overlapping or adjacent — extend
            let new_end = start.saturating_add(count).max(last_end);
            last.1 = new_end - last.0;
        } else {
            result.push((start, count));
        }
    }
    result
}

/// Parse a UDF d-string (fixed-length field with length byte at the end).
/// Used for Volume Identifier and other UDF descriptor strings.
/// The first byte of content is a compression ID: 8 = Latin-1, 16 = UTF-16BE.
pub(crate) fn parse_dstring(data: &[u8]) -> String {
    let Some(&len) = data.last() else {
        return String::new();
    };
    let len = len as usize;
    if len == 0 || len > data.len() {
        return String::new();
    }
    decode_cs0(&data[..len])
}

/// Test-only view of [`parse_dstring`], so the `dirimage` encoder can assert
/// that the d-strings it writes are the ones this parser reads back rather
/// than re-implementing the decode in its own tests.
#[cfg(test)]
pub(crate) fn parse_dstring_for_test(data: &[u8]) -> String {
    parse_dstring(data)
}

// Buffered sector reader — coalesces single-sector reads into `batch`-sized SCSI commands,
// since per-command latency dominates on USB drives.
pub(crate) struct BufferedSectorReader<'a, S: SectorSource + ?Sized = dyn SectorSource> {
    inner: &'a mut S,
    cache_start: u32,
    cache: Vec<u8>,
    cache_sectors: u32,
    batch: u16,
    /// Pre-fetched sector data from bulk reads (sector ranges for AACS, MPLS, CLPI, etc.)
    prefetched: std::collections::HashMap<u32, Vec<u8>>,
}

impl<'a, S: SectorSource + ?Sized> BufferedSectorReader<'a, S> {
    pub(crate) fn new(inner: &'a mut S, batch: u16) -> Self {
        Self {
            inner,
            cache_start: u32::MAX,
            cache: Vec::new(),
            cache_sectors: 0,
            batch,
            prefetched: std::collections::HashMap::new(),
        }
    }

    // The wrapped source, so a live scan can run the handshake on the drive
    // without dropping the prefetched metadata cache.
    pub(crate) fn inner_mut(&mut self) -> &mut S {
        self.inner
    }

    // Ends the buffering and hands the source back (the live scan's bus wiring).
    pub(crate) fn into_inner(self) -> &'a mut S {
        self.inner
    }
}

impl<S: SectorSource + ?Sized> BufferedSectorReader<'_, S> {
    /// Pre-read a contiguous range of sectors into the sliding cache.
    /// Used to bulk-load the UDF metadata partition so subsequent reads are instant.
    /// A failed batch ends the prefetch (the reads fall back to the sliding window);
    /// a Stop is returned, so the scan never reads on past it (§2.8).
    pub(crate) fn prefetch(&mut self, start_lba: u32, count: u32) -> crate::error::Result<()> {
        let count = count.min(MAX_PREFETCH_RUN_SECTORS);
        // Clamp to u32 LBA space: `start_lba` is an unconstrained disc Uint32, so a
        // partition near the top overflowed `start_lba + offset` below (debug panic;
        // release wrap filled the cache with the wrong region). Unaddressable anyway.
        let count = count.min(u32::MAX - start_lba);
        let total = count as usize * SECTOR_BYTES;
        self.cache.resize(total, 0);
        let mut offset = 0u32;
        let mut halted = false;
        while offset < count {
            let batch = (count - offset).min(self.batch as u32) as u16;
            let buf_off = offset as usize * SECTOR_BYTES;
            match self.inner.read_sectors(
                start_lba + offset,
                batch,
                &mut self.cache[buf_off..buf_off + batch as usize * SECTOR_BYTES],
                true,
            ) {
                Ok(_) => offset += batch as u32,
                Err(e) => {
                    halted = matches!(e, crate::error::Error::Halted);
                    break;
                }
            }
        }
        self.cache_start = start_lba;
        self.cache_sectors = offset;
        if halted {
            return Err(crate::error::Error::Halted);
        }
        Ok(())
    }

    /// Pre-read multiple sector ranges into the permanent per-sector HashMap cache
    /// (bulk-loads AACS/MPLS/CLPI/META before scanning), capped at MAX_PREFETCH_SECTORS
    /// to bound RAM against a crafted UDF. A Stop ends it with `Halted` between (or within)
    /// ranges; any other failed batch skips the rest of that range.
    pub(crate) fn prefetch_ranges(&mut self, ranges: &[(u32, u32)]) -> crate::error::Result<()> {
        // 2048 bytes/sector → 128 Ki sectors ≈ 256 MiB of permanent cache.
        const MAX_PREFETCH_SECTORS: u64 = 128 * 1024;
        let mut tmp = vec![0u8; self.batch as usize * SECTOR_BYTES];
        let total: u64 = ranges.iter().map(|&(_, c)| c as u64).sum();
        let mut cached: u64 = 0;
        let mut done: u64 = 0;
        let mut hb = crate::progress::Heartbeat::new("udf_prefetch");
        for &(start, count) in ranges {
            // Clamp to u32 LBA space: unconstrained Uint32 ADs left `start + count`
            // unbounded, so a range near the top overflowed `start + offset` below
            // (debug panic; release wrap seeded the permanent cache at low LBAs).
            let count = count.min(u32::MAX - start);
            let mut offset = 0u32;
            while offset < count {
                hb.tick(done, total);
                let batch = (count - offset).min(self.batch as u32) as u16;
                let bytes = batch as usize * SECTOR_BYTES;
                match self
                    .inner
                    .read_sectors(start + offset, batch, &mut tmp[..bytes], true)
                {
                    Ok(_) => {}
                    Err(crate::error::Error::Halted) => return Err(crate::error::Error::Halted),
                    Err(_) => break,
                }
                for i in 0..batch as u32 {
                    if cached >= MAX_PREFETCH_SECTORS {
                        // Cache cap hit: stop seeding the permanent HashMap.
                        // Remaining LBAs are still served by the sliding-window
                        // read path below, just without the bulk pre-load.
                        return Ok(());
                    }
                    let s = i as usize * SECTOR_BYTES;
                    self.prefetched
                        .insert(start + offset + i, tmp[s..s + SECTOR_BYTES].to_vec());
                    cached += 1;
                }
                offset += batch as u32;
                done += batch as u64;
            }
        }
        Ok(())
    }
}

impl<S: SectorSource + ?Sized> SectorSource for BufferedSectorReader<'_, S> {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> std::result::Result<usize, crate::error::Error> {
        if count == 1 {
            // Contract: a single-sector read needs at least one sector of
            // destination. Return an error rather than panicking on the slice.
            if buf.len() < SECTOR_BYTES {
                return Err(crate::error::Error::UdfBufferTooSmall);
            }
            // Check permanent prefetch cache first (HashMap)
            if let Some(data) = self.prefetched.get(&lba) {
                buf[..SECTOR_BYTES].copy_from_slice(data);
                return Ok(SECTOR_BYTES);
            }
            // Tested as a DISTANCE from `cache_start`, not `cache_start + cache_sectors`:
            // `cache_start` is disc-controlled, so the sum overflowed near `u32::MAX`
            // (debug panic; release wrap silently disabled the cache).
            if lba >= self.cache_start && lba - self.cache_start < self.cache_sectors {
                let offset = (lba - self.cache_start) as usize * SECTOR_BYTES;
                buf[..SECTOR_BYTES].copy_from_slice(&self.cache[offset..offset + SECTOR_BYTES]);
                return Ok(SECTOR_BYTES);
            }
            let block = self.batch;
            // Invalidate before the buffer is touched: a failed read below would
            // otherwise leave the old window pointing at shrunk/overwritten bytes.
            self.cache_sectors = 0;
            self.cache.resize(block as usize * SECTOR_BYTES, 0);
            match self.inner.read_sectors(lba, block, &mut self.cache, true) {
                Ok(_) => {
                    self.cache_start = lba;
                    self.cache_sectors = block as u32;
                }
                Err(_) => {
                    // By design: a batch read past the last recorded sector fails as a
                    // unit, so retry just the one sector requested — a genuinely bad
                    // single sector still propagates via `?`.
                    self.cache.resize(SECTOR_BYTES, 0);
                    self.inner.read_sectors(lba, 1, &mut self.cache, true)?;
                    self.cache_start = lba;
                    self.cache_sectors = 1;
                }
            }
            buf[..SECTOR_BYTES].copy_from_slice(&self.cache[..SECTOR_BYTES]);
            Ok(SECTOR_BYTES)
        } else {
            // Multi-sector read — pass through
            self.inner.read_sectors(lba, count, buf, true)
        }
    }

    // A FUA read asks the medium, not a cache: bypass the prefetch and sliding caches.
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> std::result::Result<usize, crate::error::Error> {
        if fua {
            return self.inner.read_sectors_fua(lba, count, buf, recovery, true);
        }
        self.read_sectors(lba, count, buf, recovery)
    }

    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
    }
}

/// Read a single 2048-byte sector from the drive.
/// Uses standard READ(10) — no unlock required.
fn read_sector(reader: &mut dyn SectorSource, lba: u32, buf: &mut [u8]) -> Result<()> {
    reader.read_sectors(lba, 1, buf, true)?;
    Ok(())
}

// Load the continuation block a type-3 AD points at: an Allocation Extent Descriptor
// (ECMA-167 4/14.5, tag 258, L_AD @20, ADs from 24). Returns (ad_start, ad_bytes).
// A revisited, non-AED or overrunning block is an unfollowable chain (UdfAdChainTooLong).
fn read_aed(
    reader: &mut dyn SectorSource,
    abs: u32,
    block: &mut [u8; SECTOR_BYTES],
    seen: &mut Vec<u32>,
) -> Result<(usize, usize)> {
    if seen.contains(&abs) {
        return Err(Error::UdfAdChainTooLong);
    }
    seen.push(abs);
    read_sector(reader, abs, block)?;
    let l_ad = u32::from_le_bytes([block[20], block[21], block[22], block[23]]) as usize;
    if u16::from_le_bytes([block[0], block[1]]) != 258 || l_ad > block.len() - 24 {
        return Err(Error::UdfAdChainTooLong);
    }
    Ok((24, l_ad))
}

#[cfg(test)]
#[path = "udf_tests.rs"]
mod tests;

// Shared UDF image fixtures for tests across the disc::* format scanners:
// a MemDisc SectorSource plus a DirSpec tree via lay_dir/build_udf_skeleton.
// Format-agnostic — BD/HD-DVD/detector each build their own trees on top.
#[cfg(test)]
#[path = "udf_fixture_tests.rs"]
pub(crate) mod fixture;

#[cfg(test)]
#[path = "udf_audit_tests.rs"]
mod audit_tests;
