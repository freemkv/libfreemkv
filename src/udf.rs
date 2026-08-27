//! UDF filesystem reader — read files from Blu-ray discs.
//!
//! Blu-ray discs use UDF 2.50 with metadata partitions.
//! The read sequence follows pointers through the disc structure:
//!
//!   Sector 256 (AVDP)
//!     → VDS (Partition Descriptor + Logical Volume Descriptor)
//!       → Metadata Partition (virtual partition stored as a file)
//!         → File Set Descriptor (FSD)
//!           → Root Directory ICB
//!             → Directory data (File Identifier Descriptors)
//!               → BDMV/PLAYLIST/*.mpls, BDMV/CLIPINF/*.clpi
//!
//! Each step reads one or two sectors. No bulk reads needed.
//!
//! References:
//!   ECMA-167 (UDF base)
//!   UDF 2.50 (OSTA) — metadata partition extension
//!   BD-ROM Part 3 — Blu-ray filesystem profile

use crate::error::{Error, Result};
use crate::sector::SectorSource;
use std::collections::HashSet;

/// Upper bound on a single UNBOUNDED metadata file read (`read_file`).
/// BD-ROM metadata files (.mpls/.clpi/.bdmv/.inf) are a few KiB to a few
/// MiB; 64 MiB is a generous ceiling that bounds the allocation a crafted
/// ICB info_length / extent length can force.
///
/// The one legitimately huge file — the AACS `MKB_RO.inf`, allocated to a
/// fixed ~128 MiB and zero-padded — is NOT read through the unbounded path:
/// `read_aacs_inputs_from_reader` reads a bounded prefix via
/// `read_file_prefix` and trims to the real record length, so it never
/// trips this cap (and never reads 100+ MiB of padding). A 0.31.0
/// regression added this cap and read the MKB unbounded, so the cap
/// rejected it → `read_aacs_inputs` failed → autorip reported "could not
/// read this disc's key files" and never contacted the keyserver.
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Upper bound on a single directory's on-disc data. Real BD-ROM
/// directories are a few KiB; 1 MiB is well above any legitimate value.
/// Caps the allocation so a corrupt 30-bit directory ICB allocation
/// length cannot force a ~1 GiB zeroed allocation per recursion level.
const MAX_DIR_BYTES: u32 = 1024 * 1024;

/// Smallest Main Volume Descriptor Sequence extent that ECMA-167 3/10.2.1
/// permits an Anchor Volume Descriptor Pointer to record: 16 logical sectors
/// (32 768 bytes). An anchor declaring less is not describing a usable
/// sequence, so its extent is ignored in favour of [`VDS_FALLBACK_START`].
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
    /// Metadata partition start (absolute sector)
    /// For UDF 2.50 discs, all file/directory references use metadata-relative LBAs
    metadata_start: u32,
    /// Metadata partition size in sectors
    metadata_sectors: u32,
}

/// One allocation extent of a file, as recorded in its ICB.
///
/// `recorded` distinguishes the two extent types this reader can encounter in
/// an allocation descriptor (ECMA-167 4/14.14.1.1):
///
/// * type 0 — recorded and allocated: `len` bytes of real data live at `lba`.
/// * type 1 — allocated but NOT recorded: the space belongs to the file and
///   occupies `len` bytes of its byte space, but nothing was ever written
///   there, so its contents are defined to be zeros. Its `lba` is where the
///   space is allocated, not where readable data lives.
///
/// The distinction is load-bearing: dropping a type-1 descriptor slides every
/// later extent's data down by that hole's length, corrupting the file silently
/// rather than failing.
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

/// One extent of a file resolved to an ABSOLUTE disc LBA — [`IcbExtent`] with
/// the partition offset already applied. The `recorded` flag travels with it:
/// an unrecorded extent occupies the file's byte space but holds no bytes the
/// file wrote, so a caller must emit `len` zeros for it rather than read those
/// sectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `pub(crate)`: reachable only through `extents_abs_at`, which is itself
/// internal. The crate's public surface is Drive / Disc / ScanOptions /
/// input() / output() / numeric errors, and leaking this type would make any
/// future reshape of its fields a breaking change for consumers that never
/// needed it.
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
    /// LBA within the metadata partition (add metadata_start for absolute)
    pub meta_lba: u32,
    /// File size in bytes (from ICB info_length)
    pub size: u64,
    /// Child entries (if directory)
    pub entries: Vec<DirEntry>,
}

impl UdfFs {
    /// Physical partition start sector.
    pub fn partition_start(&self) -> u32 {
        self.partition_start
    }

    /// Metadata partition start sector.
    pub fn metadata_start(&self) -> u32 {
        self.metadata_start
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
        let entry = current
            .entries
            .iter()
            .find(|e| !e.is_dir && e.name.eq_ignore_ascii_case(filename))
            .ok_or_else(|| Error::UdfNotFound {
                path: path.to_string(),
            })?;
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

    /// Shared implementation of [`Self::read_file`] / [`Self::read_file_prefix`]. When
    /// `max_bytes` is `Some`, reads at most that many bytes and the whole-file
    /// `MAX_FILE_BYTES` anti-DoS cap on the declared size / extent lengths is
    /// not applied (the read is already bounded by `max_bytes`).
    fn read_file_limited(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut current = &self.root;

        // Navigate to parent directory
        for part in &parts[..parts.len() - 1] {
            current = current
                .entries
                .iter()
                .find(|e| e.is_dir && e.name.eq_ignore_ascii_case(part))
                .ok_or_else(|| Error::UdfNotFound {
                    path: part.to_string(),
                })?;
        }

        // Find the file
        let filename = match parts.last() {
            Some(f) => f,
            None => {
                return Err(Error::UdfNotFound {
                    path: path.to_string(),
                });
            }
        };
        let entry = current
            .entries
            .iter()
            .find(|e| !e.is_dir && e.name.eq_ignore_ascii_case(filename))
            .ok_or_else(|| Error::UdfNotFound {
                path: path.to_string(),
            })?;

        // `max_bytes == None` => read the whole file; `Some(n)` => read at most
        // n bytes (rounded up to a sector) and skip the anti-DoS caps below.
        let limit = max_bytes.unwrap_or(usize::MAX);

        // Tiny files (notably AACS `*.inf` key files) may store data embedded inline in
        // the ICB (AD type 3), with no out-of-line extents. Honor that before the extent
        // path, which would otherwise misparse embedded bytes as ADs and hard-error.
        if let Some(mut inline) = self.read_inline_data(reader, entry.meta_lba)? {
            let want = (entry.size as usize).min(limit);
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
        let mut sector = [0u8; 2048];
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
            let sector_count = (data_len as u64).div_ceil(2048) as u32;
            // ECMA-167 4/14.14.1.1 type 1: allocated-not-recorded. Its bytes are defined
            // as zeros, so emit zeros WITHOUT reading media. Skipping the extent instead
            // would slide every later extent's bytes down by this hole's length.
            if !ext.recorded {
                for _ in 0..sector_count {
                    if data.len() >= limit {
                        break 'extents;
                    }
                    data.extend_from_slice(&[0u8; 2048]);
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
        // smaller. If extents under-covered the file (e.g. sparse), leave what
        // we have rather than over-reporting.
        let trim_to = (entry.size as usize).min(limit);
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
    /// Skip policy (actual): directories named `STREAM` (case-insensitive)
    /// are not descended, and individual files larger than 50 MB are
    /// omitted. Nothing else is filtered by name — `BACKUP`/`DUPLICATE`
    /// are traversed, and `MKB_RO.inf` is excluded only because it exceeds
    /// the 50 MB cap.
    pub fn metadata_sector_ranges(&self, reader: &mut dyn SectorSource) -> Result<Vec<(u32, u32)>> {
        let mut ranges = Vec::new();

        // UDF structure: sector 0 through end of metadata partition
        // Covers AVDP, VDS, partition descriptor, metadata ICB, FSD, all directories
        let meta_end = self.metadata_start.saturating_add(self.metadata_sectors);
        ranges.push((0, meta_end));

        // Walk tree, collect ranges for each metadata file
        self.collect_file_ranges(reader, &self.root, &mut ranges)?;

        // Merge overlapping/adjacent ranges and sort
        ranges.sort_by_key(|r| r.0);
        let merged = merge_ranges(&ranges);
        Ok(merged)
    }

    fn collect_file_ranges(
        &self,
        reader: &mut dyn SectorSource,
        entry: &DirEntry,
        ranges: &mut Vec<(u32, u32)>,
    ) -> Result<()> {
        for child in &entry.entries {
            if child.is_dir {
                // Only skip STREAM — those are the multi-GB video files
                if child.name.eq_ignore_ascii_case("STREAM") {
                    continue;
                }
                self.collect_file_ranges(reader, child, ranges)?;
            } else {
                // Include the ICB sector itself (in metadata partition)
                ranges.push((self.meta_to_abs(child.meta_lba)?, 1));

                // Include file data — skip only truly huge files (MKB_RO.inf = 134MB)
                if child.size > 50_000_000 {
                    continue;
                }

                // Push every extent: a fragmented AACS cert / MPLS / CLPI can
                // span multiple extents, and key readers downstream need all
                // of them (mirror collect_all_file_ranges).
                if let Ok(extents) = self.read_icb_extents(reader, child.meta_lba) {
                    for ext in extents {
                        // An unrecorded extent holds nothing to cache.
                        if !ext.recorded {
                            continue;
                        }
                        let abs_start = match self.partition_start.checked_add(ext.lba) {
                            Some(v) => v,
                            None => continue,
                        };
                        let sector_count = (ext.len as u64).div_ceil(2048) as u32;
                        ranges.push((abs_start, sector_count));
                    }
                }
            }
        }
        Ok(())
    }

    /// Convert a metadata-partition-relative LBA to an absolute sector number.
    /// `meta_lba` is disc-controlled, so the sum is checked to avoid a
    /// wrap-to-wrong-sector on a crafted ICB.
    fn meta_to_abs(&self, meta_lba: u32) -> Result<u32> {
        self.metadata_start
            .checked_add(meta_lba)
            .ok_or(Error::DiscRead {
                sector: self.metadata_start as u64,
                status: None,
                sense: None,
            })
    }

    /// Read an Extended File Entry (tag 266) or File Entry (tag 261)
    /// and return its first allocation extent: (data_lba, data_length).
    /// The data_lba is partition-relative.
    /// The file's first RECORDED extent — where its readable data actually
    /// begins.
    ///
    /// Not `extents.first()`. `read_icb_extents` retains ECMA-167 4/14.14.1.1
    /// type-1 (allocated, not recorded) descriptors, because dropping one would
    /// slide every later extent's data down by the hole's length. But a type-1
    /// extent's `lba` is where SPACE is allocated, not where bytes live, and
    /// this accessor's one caller — `file_start_lba` — hands its result out as
    /// "the absolute starting LBA of a file's first data extent".
    ///
    /// `ifo.rs` then uses that as the base for every VTS VOB extent
    /// (`file_start_lba(IFO) + vtstt_vobs + cell.first_sector`), so a file whose
    /// FIRST descriptor is type-1 would put the entire video title set at the
    /// wrong place on disc, with no error anywhere.
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

    /// If this ICB stores its file data INLINE (embedded — ICB Tag flags low
    /// 3 bits == 3) rather than via out-of-line extents, return the embedded
    /// bytes. Tiny files such as the AACS `*.inf` key files are routinely
    /// embedded directly in the ICB; `read_icb_extents` finds no real extents
    /// for them (it would misparse the embedded payload as allocation
    /// descriptors), so `read_file` must read the inline payload here. Returns
    /// `Ok(None)` for the normal extent-backed case.
    ///
    /// (Regression guard: 0.31.0 added a per-extent `MAX_FILE_BYTES` cap that
    /// turned the misparsed-embedded case into a hard error, which surfaced as
    /// autorip "could not read this disc's key files" on discs whose AACS
    /// `.inf` files are ICB-embedded — the keyserver was then never called.)
    fn read_inline_data(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Option<Vec<u8>>> {
        let icb_abs = self.meta_to_abs(meta_lba)?;
        let mut icb = [0u8; 2048];
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

    /// Read ALL allocation extents for a file from its ICB, in file order.
    /// Handles files with many extents (e.g. 88 GB m2ts files have ~90 extents)
    /// including files whose allocation descriptors span multiple blocks via
    /// continuation (extent_type 3) descriptors.
    ///
    /// Unrecorded (type-1) extents are returned too, flagged `recorded: false`
    /// — see [`IcbExtent`]. They carry no readable data but they DO occupy the
    /// file's byte space, so a caller reconstructing file contents must emit
    /// `len` zeros for them rather than skip them.
    fn read_icb_extents(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Vec<IcbExtent>> {
        let icb_abs = self.meta_to_abs(meta_lba)?;
        let mut icb = [0u8; 2048];
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

                match extent_type {
                    // Recorded and allocated. A zero-length type-0 descriptor is the
                    // AD-list terminator: continuation blocks are scanned to the end of
                    // the sector, so trailing zero padding must not be read as extents.
                    0 if data_len == 0 => break,
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
                        if data_len > 0 {
                            next_block = Some(data_lba);
                        }
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
                    read_sector(reader, self.meta_to_abs(cont_lba)?, &mut block)?;
                    // A continuation block starts with a 24-byte Allocation Extent
                    // Descriptor (ECMA-167 4/14.5, tag 258); real ADs start at offset
                    // 24 (length @20) — reading from 0 truncates fragmented files (e.g. BD3D).
                    let aed_l_ad =
                        u32::from_le_bytes([block[20], block[21], block[22], block[23]]) as usize;
                    ad_start = 24;
                    ad_bytes = aed_l_ad.min(block.len().saturating_sub(24));
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

    /// Absolute disc extents `(absolute_lba, byte_length)` for the ICB at
    /// `meta_lba`. Like [`file_extents`](Self::file_extents) but keyed by ICB
    /// LBA (so the tree extractor can resolve a `DirEntry` it already holds
    /// without re-navigating a path) and preserving the per-extent byte length
    /// (so the last sector can be trimmed to the file's real size). Resolves
    /// multi-extent / Long-AD / continuation ICBs.
    ///
    /// Unrecorded (ECMA-167 4/14.14.1.1 type-1) extents are included: their
    /// space is allocated to the file at that location and occupies its byte
    /// space, so dropping them would slide every later extent's bytes down by
    /// the hole's length in a sequential extraction. They are RETURNED FLAGGED
    /// (`AbsExtent::recorded == false`), because keeping the extent while
    /// losing the flag is the same bug the other way round: the caller then
    /// reads sectors the file never wrote and ships whatever the media holds
    /// there. `read_file_limited` emits zeros for such an extent; every
    /// consumer of this list has to be able to do the same.
    /// `pub(crate)`: returns `AbsExtent`, which is internal — a public fn
    /// handing back a crate-private type would not compile, and neither is
    /// part of the crate's published surface.
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
    /// Refuses, with [`Error::UdfUnrecordedExtent`], a file that contains an
    /// unrecorded (ECMA-167 4/14.14.1.1 type-1/type-2) extent. Such an extent
    /// occupies the file's byte space but was never written, so the file's
    /// true content there is zeros while the media holds something else
    /// entirely. A `(lba, sector_count)` pair cannot say "this range is a
    /// hole", and both ways of pretending otherwise corrupt the rip: reading
    /// it splices undefined sectors into the stream as content, and dropping
    /// it slides every later extent's byte space. Until an extent can carry
    /// the flag end-to-end, refusing is the only truthful answer at this
    /// signature — and a title that is not offered is not a title that was
    /// silently mis-ripped.
    ///
    /// Callers that only need the list as a byte-space ADDRESS MAP (never
    /// reading the sectors as content) want
    /// [`file_extents_addressing`](Self::file_extents_addressing), which keeps
    /// the hole in place so offsets stay correct.
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
        Ok(abs
            .iter()
            .map(|e| (e.lba, (e.len as u64).div_ceil(2048) as u32))
            .collect())
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

    /// Absolute disc extents `(absolute_lba, sector_count)` for a file, for a
    /// caller that uses the list purely as a BYTE-SPACE ADDRESS MAP — mapping
    /// a file offset onto an LBA (see
    /// [`crate::aacs::segment::clip_byte_to_lba`]) — and never reads the
    /// sectors as the file's content.
    ///
    /// Unrecorded (ECMA-167 4/14.14.1.1 type-1/type-2) extents are INCLUDED
    /// here, unflagged: they occupy the file's byte space, so omitting one
    /// would slide every later offset by the hole's length and misaddress the
    /// rest of the file. That is the right trade only because nothing on this
    /// path treats the returned sectors as stream bytes. Anything that will
    /// read them must call [`file_extents`](Self::file_extents), which refuses
    /// the file instead.
    /// `pub(crate)`: this returns unrecorded (never-written) extents
    /// UNFLAGGED, in a shape identical to `file_extents`'s safe return. An
    /// external consumer reaching for the more general-sounding name would
    /// silently obtain a read plan over a hole. The doc below says so, but a
    /// doc comment is not a guard — scoping makes the misuse a compile error.
    pub(crate) fn file_extents_addressing(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
    ) -> Result<Vec<(u32, u32)>> {
        let meta_lba = self.entry_at(path)?.meta_lba;
        Ok(self
            .extents_abs_at(reader, meta_lba)?
            .iter()
            .map(|e| (e.lba, (e.len as u64).div_ceil(2048) as u32))
            .collect())
    }
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
    let mut avdp = [0u8; 2048];
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
    let recorded = match vds_len_bytes.div_ceil(2048) {
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
    let mut volume_id = String::new();
    let mut metadata_size_bytes: u32 = 0;
    // A read fault inside a sweep must not abort before the next candidate is
    // tried, but it must not vanish either: if no candidate yields a partition,
    // the fault is the honest answer rather than "not a UDF disc".
    let mut sweep_err = None;

    for (vds_start, vds_sectors) in candidates {
        for i in vds_start..vds_start.saturating_add(vds_sectors) {
            let mut desc = [0u8; 2048];
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
                }
                // Terminating Descriptor — end of VDS
                8 => break,
                _ => continue,
            }
        }
        if partition_start != 0 {
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
    let metadata_start = if num_partition_maps >= 2 {
        let lvd_sec = lvd_sector.ok_or(Error::DiscRead {
            sector: 0,
            status: None,
            sense: None,
        })?;

        // Read LVD to check partition map type
        let mut lvd = [0u8; 2048];
        read_sector(reader, lvd_sec, &mut lvd)?;

        // Parse partition maps starting at offset 440
        // Map 0 = Type 1 (physical), Map 1 = Type 2 (metadata)
        let _pm1_type = lvd[440]; // First map type
        let pm1_len = lvd[441] as usize;

        if pm1_len > 0 && 440 + pm1_len < 2048 {
            let pm2_map = 440 + pm1_len;
            let pm2_type = lvd[pm2_map]; // Second map type

            if pm2_type == 2 {
                // Type 2 = metadata partition. UDF 2.50 2.2.10 records the Metadata
                // File's location in the map (offset 40), not always block 0; trusted
                // only when type ID reads "*UDF Metadata Partition" (block 0 still tried as fallback).
                let recorded = metadata_file_location(&lvd, pm2_map)
                    .and_then(|loc| partition_start.checked_add(loc));
                let mut meta_file_lba = partition_start;
                let mut meta_icb = [0u8; 2048];
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
                    // Extended File Entry — get allocation extent
                    let l_ea = u32::from_le_bytes([
                        meta_icb[208],
                        meta_icb[209],
                        meta_icb[210],
                        meta_icb[211],
                    ]) as usize;
                    let ad_off = 216 + l_ea;
                    if ad_off + 8 > meta_icb.len() {
                        return Err(Error::DiscRead {
                            sector: meta_file_lba as u64,
                            status: None,
                            sense: None,
                        });
                    }
                    let ad_len = u32::from_le_bytes([
                        meta_icb[ad_off],
                        meta_icb[ad_off + 1],
                        meta_icb[ad_off + 2],
                        meta_icb[ad_off + 3],
                    ]) & 0x3FFF_FFFF;
                    metadata_size_bytes = ad_len;
                    let ad_pos = u32::from_le_bytes([
                        meta_icb[ad_off + 4],
                        meta_icb[ad_off + 5],
                        meta_icb[ad_off + 6],
                        meta_icb[ad_off + 7],
                    ]);
                    // Metadata content starts at partition_start + ad_pos
                    partition_start.checked_add(ad_pos).ok_or(Error::DiscRead {
                        sector: partition_start as u64,
                        status: None,
                        sense: None,
                    })?
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

    // Step 4: Read File Set Descriptor from metadata partition
    // FSD is at metadata-relative lba 0 (first sector of metadata content)
    let mut fsd = [0u8; 2048];
    read_sector(reader, metadata_start, &mut fsd)?;

    let fsd_tag = u16::from_le_bytes([fsd[0], fsd[1]]);
    if fsd_tag != 256 {
        // The metadata sector read fine but carries no File Set Descriptor:
        // structurally not a UDF disc, not a transient read fault.
        return Err(Error::UdfNotFilesystem);
    }

    // Root Directory ICB: long_ad at FSD offset 400
    // long_ad = extent_length(4) + extent_location: lba(4) + part_ref(2) + impl_use(6)
    let root_lba = u32::from_le_bytes([fsd[404], fsd[405], fsd[406], fsd[407]]);

    // Step 5: Read root directory and build file tree.
    // Pre-seed visited with the root ICB so that any FID pointing back to
    // root_lba is detected as a cycle immediately.
    let root_icb_key = ((metadata_start as u64) << 32) | root_lba as u64;
    let mut visited: HashSet<u64> = HashSet::from([root_icb_key]);
    let root = read_directory(
        reader,
        partition_start,
        metadata_start,
        root_lba,
        "",
        0,
        &mut 0usize,
        &mut visited,
    )?;

    let metadata_sectors = (metadata_size_bytes as u64).div_ceil(2048) as u32;

    Ok(UdfFs {
        root,
        volume_id,
        partition_start,
        metadata_start,
        metadata_sectors,
    })
}

/// UDF 2.50 2.2.10 Metadata File Location: the partition-relative logical
/// block of the Metadata File's File Entry, a Uint32 at offset 40 of the
/// Metadata Partition Map that starts at `map` within the Logical Volume
/// Descriptor `lvd`.
///
/// `None` when the map does not fit wholly inside the descriptor, or when its
/// partition type identifier is not "*UDF Metadata Partition" — a Virtual
/// (UDF 2.50 2.2.8) or Sparable (2.2.9) partition map is also ECMA-167
/// 3/10.7.3 Type 2 and records unrelated fields at that offset, so its bytes
/// must never be read as a location.
fn metadata_file_location(lvd: &[u8; 2048], map: usize) -> Option<u32> {
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

/// Global cap on the total number of directory entries (FIDs) visited across
/// the entire tree walk. Each named non-parent FID counts as one entry,
/// regardless of whether it is a file or directory.
///
/// Real BD-ROM discs have at most a few thousand entries; the largest real
/// partition (BDMV/STREAM/) typically holds a few hundred .m2ts FIDs.
/// 100 000 is well above any legitimate disc and makes the 8-level × 26k-dirs
/// attack (8^26k astronomical visits) terminate in microseconds.
const MAX_TOTAL_DIR_ENTRIES: usize = 100_000;

/// Read a UDF directory and its children (up to [`MAX_DIR_DEPTH`] levels).
///
/// Each directory is an ICB (Extended File Entry) pointing to directory data
/// containing File Identifier Descriptors (FIDs). Each FID names a file/subdir
/// and points to its ICB. Directories deeper than [`MAX_DIR_DEPTH`] are
/// recorded as entries but not descended into.
///
/// `budget` tracks total FID entries consumed across the whole tree; the walk
/// aborts with `Error::DiscRead` once it exceeds [`MAX_TOTAL_DIR_ENTRIES`].
/// `visited` is the set of metadata-relative ICB LBAs already opened as
/// directories; a repeated LBA is a cycle and is skipped.
// A recursive UDF directory-tree parser: the arg list (reader, partition/meta
// offsets, depth, plus the global entry budget and the cycle-detection
// visited-set) is inherent to the walk, not a refactor smell.
#[allow(clippy::only_used_in_recursion)]
#[allow(clippy::too_many_arguments)]
fn read_directory(
    reader: &mut dyn SectorSource,
    part_start: u32,
    meta_start: u32,
    meta_lba: u32,
    name: &str,
    depth: u32,
    budget: &mut usize,
    visited: &mut HashSet<u64>,
) -> Result<DirEntry> {
    // Read ICB for this directory
    let icb_abs = meta_start.checked_add(meta_lba).ok_or(Error::DiscRead {
        sector: meta_start as u64,
        status: None,
        sense: None,
    })?;
    let mut icb = [0u8; 2048];
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
        // Out-of-line: read the FIRST allocation descriptor. short_ad/long_ad both
        // start with 4-byte length + 4-byte LBA; extended_ad's lb_num is at offset
        // 12. Other flag values fall back to short_ad, matching `read_icb_extents`.
        let lba_at = if ad_type == 2 {
            ad_off + 12
        } else {
            ad_off + 4
        };
        if lba_at + 4 > icb.len() {
            return Err(Error::DiscRead {
                sector: icb_abs as u64,
                status: None,
                sense: None,
            });
        }
        let ad_len = u32::from_le_bytes([
            icb[ad_off],
            icb[ad_off + 1],
            icb[ad_off + 2],
            icb[ad_off + 3],
        ]) & 0x3FFF_FFFF;
        let ad_pos = u32::from_le_bytes([
            icb[lba_at],
            icb[lba_at + 1],
            icb[lba_at + 2],
            icb[lba_at + 3],
        ]);

        // Reject oversized dirs before allocating: ad_len is a disc-controlled
        // 30-bit length, so corruption could force a ~1 GiB alloc (amplified by
        // recursion). 1 MiB cap still covers a large STREAM/ dir's .m2ts FIDs.
        if ad_len > MAX_DIR_BYTES {
            return Err(Error::DiscRead {
                sector: meta_start as u64,
                status: None,
                sense: None,
            });
        }

        // Read directory data
        let dir_abs = meta_start.checked_add(ad_pos).ok_or(Error::DiscRead {
            sector: meta_start as u64,
            status: None,
            sense: None,
        })?;
        let sector_count = ad_len.div_ceil(2048);
        let mut dir_data = vec![0u8; sector_count as usize * 2048];
        for i in 0..sector_count {
            let abs = dir_abs.checked_add(i).ok_or(Error::DiscRead {
                sector: dir_abs as u64,
                status: None,
                sense: None,
            })?;
            read_sector(
                reader,
                abs,
                &mut dir_data[(i as usize) * 2048..(i as usize + 1) * 2048],
            )?;
        }
        (dir_data, ad_len)
    };

    // Parse File Identifier Descriptors
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + 38 < dir_data.len().min(ad_len as usize) {
        let fid_tag = u16::from_le_bytes([dir_data[pos], dir_data[pos + 1]]);
        if fid_tag != 257 {
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
            if name_end > dir_data.len() {
                break;
            }
            let entry_name = parse_udf_name(&dir_data[name_start..name_end]);

            if !entry_name.is_empty() {
                // Global entry budget: abort if a crafted disc tries to
                // enumerate an astronomically large tree.
                *budget = budget.saturating_add(1);
                if *budget > MAX_TOTAL_DIR_ENTRIES {
                    return Err(Error::DiscRead {
                        sector: meta_start as u64,
                        status: None,
                        sense: None,
                    });
                }

                // Read failures must propagate, not become size 0 (indistinguishable
                // from a real empty file). `read_file_size` still returns Ok(0) for
                // ICBs that aren't File/Extended File Entry tags (261/266) — genuine zero.
                let file_size = read_file_size(reader, meta_start, icb_lba)?;

                if is_dir && depth < MAX_DIR_DEPTH {
                    // Cycle guard: skip any ICB LBA we have already opened as
                    // a directory (self-referential or cross-linked dirs).
                    let icb_key = ((meta_start as u64) << 32) | icb_lba as u64;
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
                        let subdir = read_directory(
                            reader,
                            part_start,
                            meta_start,
                            icb_lba,
                            &entry_name,
                            depth + 1,
                            budget,
                            visited,
                        )?;
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

/// Read file size (info_length) from an Extended File Entry ICB.
fn read_file_size(reader: &mut dyn SectorSource, meta_start: u32, meta_lba: u32) -> Result<u64> {
    let abs = meta_start.checked_add(meta_lba).ok_or(Error::DiscRead {
        sector: meta_start as u64,
        status: None,
        sense: None,
    })?;
    let mut icb = [0u8; 2048];
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

/// Parse a UDF filename from raw bytes.
///
/// UDF uses a compression ID as the first byte:
///   8  = 8-bit characters (ASCII)
///   16 = 16-bit big-endian Unicode (UTF-16BE)
pub(crate) fn parse_udf_name(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }

    match data[0] {
        8 => {
            // 8-bit ASCII
            String::from_utf8_lossy(&data[1..]).trim().to_string()
        }
        16 => {
            // 16-bit big-endian Unicode
            let mut s = String::new();
            let chars = &data[1..];
            for i in (0..chars.len()).step_by(2) {
                if i + 1 < chars.len() {
                    let c = ((chars[i] as u16) << 8) | chars[i + 1] as u16;
                    if let Some(ch) = char::from_u32(c as u32) {
                        s.push(ch);
                    }
                }
            }
            s.trim().to_string()
        }
        _ => String::from_utf8_lossy(&data[1..]).trim().to_string(),
    }
}

/// Merge overlapping or adjacent (start, count) ranges. Caller sorts by start
/// first. Shared range utility — also used to build the disc's encrypted-content
/// extent map (see `Disc::encrypted_content_ranges`).
pub(crate) fn merge_ranges(ranges: &[(u32, u32)]) -> Vec<(u32, u32)> {
    if ranges.is_empty() {
        return Vec::new();
    }
    let mut result = vec![ranges[0]];
    for &(start, count) in &ranges[1..] {
        let last = result.last_mut().unwrap();
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
/// The first byte of content is a compression ID: 8 = ASCII, 16 = UTF-16BE.
fn parse_dstring(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }
    let len = *data.last().unwrap() as usize;
    if len == 0 || len > data.len() {
        return String::new();
    }
    let content = &data[..len];
    if content.is_empty() {
        return String::new();
    }
    match content[0] {
        8 => String::from_utf8_lossy(&content[1..])
            .trim_end_matches('\0')
            .trim()
            .to_string(),
        16 => {
            let mut s = String::new();
            let chars = &content[1..];
            for i in (0..chars.len()).step_by(2) {
                if i + 1 < chars.len() {
                    let c = ((chars[i] as u16) << 8) | chars[i + 1] as u16;
                    if c != 0
                        && let Some(ch) = char::from_u32(c as u32)
                    {
                        s.push(ch);
                    }
                }
            }
            s.trim().to_string()
        }
        _ => String::from_utf8_lossy(&content[1..])
            .trim_end_matches('\0')
            .trim()
            .to_string(),
    }
}

/// Test-only view of [`parse_dstring`], so the `dirimage` encoder can assert
/// that the d-strings it writes are the ones this parser reads back rather
/// than re-implementing the decode in its own tests.
#[cfg(test)]
pub(crate) fn parse_dstring_for_test(data: &[u8]) -> String {
    parse_dstring(data)
}

/// Buffered sector reader — reduces SCSI round-trips by coalescing
/// single-sector reads into `batch`-sized SCSI commands. Per-command
/// latency dominates on USB drives, so serving many adjacent single-sector
/// reads from one bulk read is substantially faster than issuing each
/// individually. `batch` is a runtime field, not a fixed count.
pub(crate) struct BufferedSectorReader<'a> {
    inner: &'a mut dyn SectorSource,
    cache_start: u32,
    cache: Vec<u8>,
    cache_sectors: u32,
    batch: u16,
    /// Pre-fetched sector data from bulk reads (sector ranges for AACS, MPLS, CLPI, etc.)
    prefetched: std::collections::HashMap<u32, Vec<u8>>,
}

impl<'a> BufferedSectorReader<'a> {
    pub(crate) fn new(inner: &'a mut dyn SectorSource, batch: u16) -> Self {
        Self {
            inner,
            cache_start: u32::MAX,
            cache: Vec::new(),
            cache_sectors: 0,
            batch,
            prefetched: std::collections::HashMap::new(),
        }
    }
}

impl BufferedSectorReader<'_> {
    /// Pre-read a contiguous range of sectors into the sliding cache.
    /// Used to bulk-load the UDF metadata partition so subsequent reads are instant.
    pub(crate) fn prefetch(&mut self, start_lba: u32, count: u32) {
        // Cap to 8192 sectors (16 MiB) so a disc-controlled ad_len cannot
        // drive a multi-hundred-MiB allocation before any sectors are read.
        let count = count.min(8192);
        // Clamp to u32 LBA space: `start_lba` is an unconstrained disc Uint32, so a
        // partition near the top overflowed `start_lba + offset` below (debug panic;
        // release wrap filled the cache with the wrong region). Unaddressable anyway.
        let count = count.min(u32::MAX - start_lba);
        let total = count as usize * 2048;
        self.cache.resize(total, 0);
        let mut offset = 0u32;
        while offset < count {
            let batch = (count - offset).min(self.batch as u32) as u16;
            let buf_off = offset as usize * 2048;
            if self
                .inner
                .read_sectors(
                    start_lba + offset,
                    batch,
                    &mut self.cache[buf_off..buf_off + batch as usize * 2048],
                    true,
                )
                .is_err()
            {
                break;
            }
            offset += batch as u32;
        }
        self.cache_start = start_lba;
        self.cache_sectors = offset;
    }

    /// Pre-read multiple sector ranges into the permanent cache.
    /// Each range is read in batch-sized chunks and stored per-sector in a HashMap.
    /// Used to bulk-load all small files (AACS, MPLS, CLPI, META) before scanning.
    ///
    /// Anti-DoS: the permanent cache holds one ~2 KB `Vec` per sector in a
    /// `HashMap`, so the total sector count bounds RAM. A crafted UDF (a
    /// bogus metadata-file size in `metadata_sector_ranges`) could otherwise
    /// drive that count to billions. Cap the cumulative prefetched sectors at
    /// `MAX_PREFETCH_SECTORS` (~1 GiB of cache); once exceeded, stop seeding
    /// the cache. The sliding-window read path still serves any LBA on
    /// demand, so this only forgoes the bulk speed-up — it never loses data.
    pub(crate) fn prefetch_ranges(&mut self, ranges: &[(u32, u32)]) {
        // 2048 bytes/sector → 512 Ki sectors ≈ 1 GiB of permanent cache.
        const MAX_PREFETCH_SECTORS: u64 = 512 * 1024;
        let mut tmp = vec![0u8; self.batch as usize * 2048];
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
                let bytes = batch as usize * 2048;
                if self
                    .inner
                    .read_sectors(start + offset, batch, &mut tmp[..bytes], true)
                    .is_err()
                {
                    break;
                }
                for i in 0..batch as u32 {
                    if cached >= MAX_PREFETCH_SECTORS {
                        // Cache cap hit: stop seeding the permanent HashMap.
                        // Remaining LBAs are still served by the sliding-window
                        // read path below, just without the bulk pre-load.
                        return;
                    }
                    let s = i as usize * 2048;
                    self.prefetched
                        .insert(start + offset + i, tmp[s..s + 2048].to_vec());
                    cached += 1;
                }
                offset += batch as u32;
                done += batch as u64;
            }
        }
    }
}

impl SectorSource for BufferedSectorReader<'_> {
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
            if buf.len() < 2048 {
                return Err(crate::error::Error::UdfBufferTooSmall);
            }
            // Check permanent prefetch cache first (HashMap)
            if let Some(data) = self.prefetched.get(&lba) {
                buf[..2048].copy_from_slice(data);
                return Ok(2048);
            }
            // Tested as a DISTANCE from `cache_start`, not `cache_start + cache_sectors`:
            // `cache_start` is disc-controlled, so the sum overflowed near `u32::MAX`
            // (debug panic; release wrap silently disabled the cache).
            if lba >= self.cache_start && lba - self.cache_start < self.cache_sectors {
                let offset = (lba - self.cache_start) as usize * 2048;
                buf[..2048].copy_from_slice(&self.cache[offset..offset + 2048]);
                return Ok(2048);
            }
            let block = self.batch;
            self.cache.resize(block as usize * 2048, 0);
            match self.inner.read_sectors(lba, block, &mut self.cache, true) {
                Ok(_) => {
                    self.cache_start = lba;
                    self.cache_sectors = block as u32;
                }
                Err(_) => {
                    // By design: a batch read past the last recorded sector fails as a
                    // unit, so retry just the one sector requested — a genuinely bad
                    // single sector still propagates via `?`.
                    self.cache.resize(2048, 0);
                    self.inner.read_sectors(lba, 1, &mut self.cache, true)?;
                    self.cache_start = lba;
                    self.cache_sectors = 1;
                }
            }
            buf[..2048].copy_from_slice(&self.cache[..2048]);
            Ok(2048)
        } else {
            // Multi-sector read — pass through
            self.inner.read_sectors(lba, count, buf, true)
        }
    }
}

/// Read a single 2048-byte sector from the drive.
/// Uses standard READ(10) — no unlock required.
fn read_sector(reader: &mut dyn SectorSource, lba: u32, buf: &mut [u8]) -> Result<()> {
    reader.read_sectors(lba, 1, buf, true)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// In-memory SectorSource backed by an explicit absolute-LBA → sector map.
    /// Unmapped sectors read as zeroes.
    struct MapReader {
        sectors: HashMap<u32, [u8; 2048]>,
    }

    impl MapReader {
        fn new() -> Self {
            Self {
                sectors: HashMap::new(),
            }
        }
        fn put(&mut self, lba: u32, data: [u8; 2048]) {
            self.sectors.insert(lba, data);
        }
    }

    impl SectorSource for MapReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let need = count as usize * 2048;
            if buf.len() < need {
                return Err(Error::UdfBufferTooSmall);
            }
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                // `wrapping_add`: the near-`u32::MAX` overflow regressions below
                // hand this harness LBAs at the top of the space on purpose, and
                // the harness's own bookkeeping must not be what panics.
                let s = self
                    .sectors
                    .get(&lba.wrapping_add(i))
                    .copied()
                    .unwrap_or([0u8; 2048]);
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(need)
        }
    }

    /// Build an Extended File Entry (tag 266) ICB sector with the given
    /// info_length and a list of (extent_type, data_len, data_lba) short ADs.
    fn build_efe(info_length: u64, ads: &[(u32, u32, u32)]) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // tag
        s[56..64].copy_from_slice(&info_length.to_le_bytes()); // info_length
        let l_ea: u32 = 0;
        let l_ad: u32 = (ads.len() * 8) as u32;
        s[208..212].copy_from_slice(&l_ea.to_le_bytes());
        s[212..216].copy_from_slice(&l_ad.to_le_bytes());
        let mut off = 216 + l_ea as usize;
        for &(etype, dlen, dlba) in ads {
            let raw_len = (etype << 30) | (dlen & 0x3FFF_FFFF);
            s[off..off + 4].copy_from_slice(&raw_len.to_le_bytes());
            s[off + 4..off + 8].copy_from_slice(&dlba.to_le_bytes());
            off += 8;
        }
        s
    }

    /// Build an Extended File Entry (tag 266) ICB whose allocation
    /// descriptors are LONG ADs (16 bytes: len(4) | lba(4) | part_ref(2) |
    /// impl_use(6)). Sets the ICB Tag flags (abs offset 34) low bits to 1
    /// so the parser must select the 16-byte stride. This is the layout
    /// large BD-ROM .m2ts streams actually use.
    fn build_efe_long(info_length: u64, ads: &[(u32, u32, u32)]) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // tag
        // ICB Tag flags at abs offset 34: AD type 1 = Long AD.
        s[34..36].copy_from_slice(&1u16.to_le_bytes());
        s[56..64].copy_from_slice(&info_length.to_le_bytes());
        let l_ea: u32 = 0;
        let l_ad: u32 = (ads.len() * 16) as u32;
        s[208..212].copy_from_slice(&l_ea.to_le_bytes());
        s[212..216].copy_from_slice(&l_ad.to_le_bytes());
        let mut off = 216 + l_ea as usize;
        for &(etype, dlen, dlba) in ads {
            let raw_len = (etype << 30) | (dlen & 0x3FFF_FFFF);
            s[off..off + 4].copy_from_slice(&raw_len.to_le_bytes());
            s[off + 4..off + 8].copy_from_slice(&dlba.to_le_bytes());
            // off+8..16 = partition ref + impl_use, left zero: this is what trips
            // the old 8-byte-stride parser into reading a bogus zero-length terminator.
            off += 16;
        }
        s
    }

    /// A continuation block: a bare list of short ADs from byte 0.
    fn build_cont_block(ads: &[(u32, u32, u32)]) -> [u8; 2048] {
        // Allocation Extent Descriptor (ECMA-167 4/14.5): 16-byte tag, then
        // prev_allocation_extent_location (@16) and length_of_allocation_descriptors
        // (@20); ADs begin at offset 24, which the parser skips past.
        let mut s = [0u8; 2048];
        let l_ad = (ads.len() * 8) as u32;
        s[20..24].copy_from_slice(&l_ad.to_le_bytes());
        let mut off = 24usize;
        for &(etype, dlen, dlba) in ads {
            let raw_len = (etype << 30) | (dlen & 0x3FFF_FFFF);
            s[off..off + 4].copy_from_slice(&raw_len.to_le_bytes());
            s[off + 4..off + 8].copy_from_slice(&dlba.to_le_bytes());
            off += 8;
        }
        s
    }

    /// Build a File Entry (ECMA-167 4/14.9, tag 261) or Extended File Entry
    /// (4/14.17, tag 266) carrying short ADs, with every disc-controlled
    /// field of the descriptor area exposed:
    ///
    /// * `l_ea`  — extended-attribute length. The descriptors begin at the
    ///   entry type's own base (176 for a 261, 216 for a 266) PLUS this. The
    ///   attribute area is filled with a recognisable pattern so a descriptor
    ///   read from the wrong offset cannot silently produce a usable value.
    /// * `l_ad`  — the DECLARED descriptor-area length, independent of how
    ///   many descriptors are actually written.
    /// * `extra` — descriptors written immediately after the declared area,
    ///   i.e. bytes the entry does not claim are descriptors at all.
    fn build_entry_ads(
        tag: u16,
        l_ea: usize,
        l_ad: u32,
        ads: &[(u32, u32, u32)],
        extra: &[(u32, u32, u32)],
    ) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&tag.to_le_bytes());
        let (l_ea_off, base) = if tag == 266 {
            (208usize, 216usize)
        } else {
            (168usize, 176usize)
        };
        s[l_ea_off..l_ea_off + 4].copy_from_slice(&(l_ea as u32).to_le_bytes());
        s[l_ea_off + 4..l_ea_off + 8].copy_from_slice(&l_ad.to_le_bytes());
        s[base..base + l_ea].fill(0xA5);
        let mut off = base + l_ea;
        for &(etype, dlen, dlba) in ads.iter().chain(extra) {
            if off + 8 > s.len() {
                break;
            }
            let raw_len = (etype << 30) | (dlen & 0x3FFF_FFFF);
            s[off..off + 4].copy_from_slice(&raw_len.to_le_bytes());
            s[off + 4..off + 8].copy_from_slice(&dlba.to_le_bytes());
            off += 8;
        }
        s
    }

    /// `file_start_lba` must skip a leading UNRECORDED extent and report where
    /// the file's DATA starts.
    ///
    /// ECMA-167 4/14.14.1.1 type 1 is allocated-but-not-recorded: the space
    /// belongs to the file and occupies its byte range, but nothing was written
    /// there and its `lba` is where SPACE lives, not bytes. `read_icb_extents`
    /// retains such descriptors — dropping one would slide every later extent's
    /// data down by the hole's length — so `.first()` can now be one.
    ///
    /// `ifo.rs` uses this value as the base for every VTS VOB extent
    /// (`file_start_lba(IFO) + vtstt_vobs + cell.first_sector`), so getting it
    /// wrong puts the entire video title set at the wrong place on disc, with
    /// no error anywhere — a silent wrong answer on an ordinary DVD.
    #[test]
    fn file_start_lba_skips_a_leading_unrecorded_extent() {
        use fixture::{DirSpec, MemDisc, PART_START, build_udf_skeleton, lay_dir};

        const HOLE_LBA: u32 = 900;
        const DATA_LBA: u32 = 40;

        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(
            &mut disc,
            &DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: vec![fixture::file("VIDEO_TS.IFO", 12, DATA_LBA, 2048, false)],
                subdirs: Vec::new(),
            },
        );

        // Rewrite the file's ICB: a type-1 hole FIRST, then the real data.
        let icb = build_entry_ads(266, 0, 16, &[(1, 2048, HOLE_LBA), (0, 2048, DATA_LBA)], &[]);
        disc.put_bytes(PART_START + 12, &icb);

        let fs = super::read_filesystem(&mut disc).expect("volume mounts");
        let lba = fs
            .file_start_lba(&mut disc, "/VIDEO_TS.IFO")
            .expect("a file whose first descriptor is a hole still has data");

        assert_eq!(
            lba,
            PART_START + DATA_LBA,
            "must report the first RECORDED extent; reporting the hole's LBA \\
             ({}) would rebase every VTS VOB extent onto unrelated sectors",
            PART_START + HOLE_LBA
        );
    }

    #[test]
    fn icb_extents_reads_both_entry_types_at_their_own_descriptor_offsets() {
        // ECMA-167 4/14.9 File Entry: L_EA @168, ADs @176+L_EA. 4/14.17 Extended
        // File Entry: @208, @216+L_EA (261 is UDF 1.02 DVD-Video). Offset must be
        // computed per-tag, not assumed, or real extended attributes get misread.
        let want = vec![(4096, 8192), (16384, 2048)];
        for tag in [261u16, 266u16] {
            for l_ea in [0usize, 88] {
                let icb = build_entry_ads(tag, l_ea, 16, &[(0, 8192, 4096), (0, 2048, 16384)], &[]);
                let mut reader = MapReader::new();
                reader.put(5, icb);
                let fs = fs_with(0, 0, file_entry("F", 5, 10240));
                let got = tuples(
                    &fs.read_icb_extents(&mut reader, 5)
                        .unwrap_or_else(|e| panic!("tag {tag}, L_EA {l_ea}: {e:?}")),
                );
                assert_eq!(got, want, "tag {tag}, L_EA {l_ea}");
            }
        }
    }

    #[test]
    fn icb_extents_reads_a_descriptor_list_flush_with_the_end_of_the_entry() {
        // A final short_ad may exactly fill the rest of the 2048-byte block; it's
        // still wholly inside the entry and must be read, or the file's only
        // extent is dropped and it reports no data instead of an error.
        for tag in [261u16, 266u16] {
            let base = if tag == 266 { 216 } else { 176 };
            let icb = build_entry_ads(tag, 2048 - base - 8, 8, &[(0, 2048, 4096)], &[]);
            let mut reader = MapReader::new();
            reader.put(5, icb);
            let fs = fs_with(0, 0, file_entry("F", 5, 2048));
            let got =
                tuples(&fs.read_icb_extents(&mut reader, 5).unwrap_or_else(|e| {
                    panic!("tag {tag}: a flush descriptor is in bounds: {e:?}")
                }));
            assert_eq!(got, vec![(4096, 2048)], "tag {tag}");
        }
    }

    #[test]
    fn icb_extents_refuses_a_descriptor_area_that_runs_past_the_entry() {
        // L_AD is a disc-controlled Uint32. One that puts the end of the
        // descriptor area past the end of the 2048-byte entry is malformed,
        // and reading it walks off the sector buffer.
        for tag in [261u16, 266u16] {
            let icb = build_entry_ads(tag, 0, 4000, &[(0, 2048, 4096)], &[]);
            let mut reader = MapReader::new();
            reader.put(5, icb);
            let fs = fs_with(0, 0, file_entry("F", 5, 2048));
            let err = fs
                .read_icb_extents(&mut reader, 5)
                .expect_err("a descriptor area larger than the entry cannot be read");
            assert!(matches!(err, Error::DiscRead { .. }), "tag {tag}: {err:?}");
        }
    }

    #[test]
    fn icb_extents_stops_at_the_declared_descriptor_area_length() {
        // L_AD bounds the allocation descriptors; what follows is padding or
        // nothing, never descriptors. Reading past it invents extents from
        // whatever bytes are there — silent corruption appended to the file.
        let icb = build_entry_ads(266, 0, 8, &[(0, 2048, 4096)], &[(0, 2048, 999_999)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("F", 5, 2048));
        let got = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(
            got,
            vec![(4096, 2048)],
            "bytes past the declared L_AD are not allocation descriptors"
        );
    }

    #[test]
    fn icb_extents_does_not_follow_a_zero_length_continuation_pointer() {
        // ECMA-167 4/14.14.1.1: a zero-length descriptor (incl. type-3 "next
        // extent" pointer) designates no extent. Following it anyway reads a
        // bogus sector and parses it as an AED, appending extents the file never had.
        let icb = build_efe(2048, &[(0, 2048, 10), (3, 0, 50)]);
        let mut reader = MapReader::new();
        reader.put(105, icb); // meta_start 100 + meta_lba 5
        reader.put(150, build_cont_block(&[(0, 4096, 700), (0, 4096, 800)]));
        let fs = fs_with(1000, 100, file_entry("F", 5, 2048));
        let got = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(
            got,
            vec![(10, 2048)],
            "a zero-length continuation pointer points at nothing"
        );
    }

    fn fs_with(part_start: u32, meta_start: u32, root: DirEntry) -> UdfFs {
        UdfFs {
            root,
            volume_id: String::new(),
            partition_start: part_start,
            metadata_start: meta_start,
            metadata_sectors: 0,
        }
    }

    /// `(lba, len)` of each extent, in file order, for the tests that care
    /// about placement rather than about the recorded/unrecorded distinction.
    fn tuples(extents: &[IcbExtent]) -> Vec<(u32, u32)> {
        extents.iter().map(|e| (e.lba, e.len)).collect()
    }

    fn file_entry(name: &str, meta_lba: u32, size: u64) -> DirEntry {
        DirEntry {
            name: name.to_string(),
            is_dir: false,
            meta_lba,
            size,
            entries: Vec::new(),
        }
    }

    #[test]
    fn icb_extents_follow_type3_continuation() {
        let part_start = 1000;
        let meta_start = 100;
        // ICB at meta_lba 5: one real extent + a type-3 continuation pointer.
        let icb = build_efe(
            6144,
            &[
                (0, 4096, 10), // recorded extent at part-rel lba 10
                (3, 2048, 50), // continuation block at meta-rel lba 50
            ],
        );
        // Continuation block holds the tail extent.
        let cont = build_cont_block(&[(0, 2048, 20)]);

        let mut reader = MapReader::new();
        reader.put(meta_start + 5, icb);
        reader.put(meta_start + 50, cont);

        let fs = fs_with(part_start, meta_start, file_entry("X", 5, 6144));
        let extents = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(extents, vec![(10, 4096), (20, 2048)]);
    }

    #[test]
    fn icb_extents_continuation_skips_aed_header_not_read_as_extent() {
        // Regression (Blu-ray 3D): a continuation block opens with a 24-byte AED whose tag holds
        // non-zero bytes (id 258, CRC); reading ADs from offset 0 parses garbage and breaks,
        // losing fragments past the first continuation (25.8 GB -> ~1.8 GB). Read ADs from +24.
        let icb = build_efe(6144, &[(0, 4096, 10), (3, 2048, 50)]);
        let mut cont = build_cont_block(&[(0, 2048, 20)]);
        // Stamp a realistic AED descriptor tag (id 258) into the header so a
        // regression that reads from offset 0 mis-parses it as a bogus extent
        // instead of finding the real (20, 2048) at offset 24.
        cont[0..2].copy_from_slice(&258u16.to_le_bytes());
        let mut reader = MapReader::new();
        reader.put(5, icb);
        reader.put(50, cont);
        let fs = fs_with(0, 0, file_entry("3D", 5, 6144));
        let extents = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(
            extents,
            vec![(10, 4096), (20, 2048)],
            "continuation ADs must be read past the 24-byte AED header, not from offset 0"
        );
    }

    #[test]
    fn icb_extents_long_ad_returns_all_extents_not_just_first() {
        // Regression: BD-ROM large .m2ts use 16-byte Long ADs. The pre-fix parser hardcoded an
        // 8-byte stride, misreading descriptor #1 from mid-Long-AD as a zero terminator, so
        // multi-extent titles truncated at ~1 GiB. Four near-max Long ADs; all four must return.
        let icb = build_efe_long(
            4 * 1_000_000_000,
            &[
                (0, 0x3FFF_F800, 100),     // ~1 GiB extent
                (0, 0x3FFF_F800, 600_000), // next extent
                (0, 0x3FFF_F800, 1_100_000),
                (0, 0x1000_0000, 1_600_000), // shorter tail extent
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("BIG", 5, 4 * 1_000_000_000));
        let extents = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(
            extents,
            vec![
                (100, 0x3FFF_F800),
                (600_000, 0x3FFF_F800),
                (1_100_000, 0x3FFF_F800),
                (1_600_000, 0x1000_0000),
            ],
            "Long-AD file must return ALL extents, not just the first"
        );
    }

    #[test]
    fn read_file_spans_multiple_extents() {
        let part_start = 0;
        let meta_start = 0;
        // Two extents of one sector each; distinct fill bytes per data sector.
        let icb = build_efe(4096, &[(0, 2048, 10), (0, 2048, 30)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        reader.put(10, [0xAA; 2048]);
        reader.put(30, [0xBB; 2048]);

        let root = DirEntry {
            name: String::new(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries: vec![file_entry("F", 5, 4096)],
        };
        let fs = fs_with(part_start, meta_start, root);
        let data = fs.read_file(&mut reader, "/F").expect("read");
        assert_eq!(data.len(), 4096);
        assert!(data[..2048].iter().all(|&b| b == 0xAA));
        assert!(data[2048..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn read_file_long_ad_returns_full_content_not_truncated() {
        // Regression for 0.31.0: `read_file` reads `/AACS/MKB_RO.inf` + `Unit_Key_RO.inf`. The
        // pre-fix Short-AD-only parser truncated Long-AD multi-extent files after extent #0, so
        // autorip's `key_files()` saw a garbage AACS file and skipped the key request.
        let icb = build_efe_long(6144, &[(0, 2048, 10), (0, 2048, 30), (0, 2048, 50)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        reader.put(10, [0xAA; 2048]);
        reader.put(30, [0xBB; 2048]);
        reader.put(50, [0xCC; 2048]);
        let root = DirEntry {
            name: String::new(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries: vec![file_entry("MKB", 5, 6144)],
        };
        let fs = fs_with(0, 0, root);
        let data = fs.read_file(&mut reader, "/MKB").expect("read");
        assert_eq!(
            data.len(),
            6144,
            "Long-AD file must not truncate at extent #0"
        );
        assert!(data[..2048].iter().all(|&b| b == 0xAA));
        assert!(data[2048..4096].iter().all(|&b| b == 0xBB));
        assert!(data[4096..].iter().all(|&b| b == 0xCC));
    }

    #[test]
    fn read_aacs_inputs_reads_long_ad_files_in_full() {
        // autorip reads /AACS/Unit_Key_RO.inf + MKB_RO.inf via Disc::read_aacs_inputs. On Long-AD
        // discs (UHD/large Blu-ray) the pre-0.31.1 Short-AD-only reader truncated these at the
        // first extent, breaking key derivation; assert the full multi-extent content returns.
        let aacs = DirEntry {
            name: "AACS".to_string(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries: vec![
                file_entry("Unit_Key_RO.inf", 5, 4096), // Long-AD, 2 extents
                file_entry("MKB_RO.inf", 7, 2048),
            ],
        };
        let root = DirEntry {
            name: String::new(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries: vec![aacs],
        };
        let mut reader = MapReader::new();
        // Unit_Key_RO.inf: Long-AD ICB with two recorded extents.
        reader.put(5, build_efe_long(4096, &[(0, 2048, 10), (0, 2048, 30)]));
        reader.put(10, [0xAA; 2048]);
        reader.put(30, [0xBB; 2048]);
        // MKB_RO.inf: single Long-AD extent (content is opaque to this test).
        reader.put(7, build_efe_long(2048, &[(0, 2048, 50)]));
        reader.put(50, [0xCC; 2048]);

        let fs = fs_with(0, 0, root);
        let (inf, _mkb, _version) =
            crate::disc::Disc::read_aacs_inputs_from_reader(&mut reader, &fs)
                .expect("read_aacs_inputs must succeed for a Long-AD disc");
        assert_eq!(
            inf.len(),
            4096,
            "Unit_Key_RO.inf (Long-AD, multi-extent) must read in full — the \
             pre-0.31.1 Short-AD parser truncated it to the first 2048-byte extent"
        );
        assert!(inf[..2048].iter().all(|&b| b == 0xAA));
        assert!(inf[2048..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn read_aacs_inputs_falls_through_to_hddvd_any_dir() {
        // HD DVD keeps AACS material under /ANY!/ (VTKF000.AACS + MKBROM.AACS),
        // not /AACS/Unit_Key_RO.inf + MKB_RO.inf, so candidate lookup must fall
        // through to /ANY!/ with no disc-type branch; the server classifies by magic.
        let any = DirEntry {
            name: "ANY!".to_string(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries: vec![
                file_entry("VTKF000.AACS", 5, 2048),
                file_entry("MKBROM.AACS", 7, 2048),
            ],
        };
        let root = DirEntry {
            name: String::new(),
            is_dir: true,
            meta_lba: 0,
            size: 0,
            entries: vec![any], // deliberately NO /AACS/ dir
        };
        let mut reader = MapReader::new();
        // VTKF000.AACS: one extent whose content opens with the HD DVD magic.
        let mut vtkf = [0u8; 2048];
        vtkf[..12].copy_from_slice(b"DVD_HD_V_TKF");
        reader.put(5, build_efe_long(2048, &[(0, 2048, 10)]));
        reader.put(10, vtkf);
        // MKBROM.AACS: one extent with a type-0x10 AACS-1.0 (HD DVD) version record.
        let mut mkb = [0u8; 2048];
        mkb[..12].copy_from_slice(&[
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x04, 0x10, 0x03, 0x00, 0x00, 0x00, 0x03,
        ]);
        reader.put(7, build_efe_long(2048, &[(0, 2048, 50)]));
        reader.put(50, mkb);

        let fs = fs_with(0, 0, root);
        let (inf, _mkb, _version) =
            crate::disc::Disc::read_aacs_inputs_from_reader(&mut reader, &fs)
                .expect("read_aacs_inputs must source the HD DVD /ANY!/ files");
        assert_eq!(
            &inf[..12],
            b"DVD_HD_V_TKF",
            "inf must be the HD DVD VTKF (its magic), sourced from /ANY!/ via the \
             candidate fall-through — not /AACS/Unit_Key_RO.inf"
        );
    }

    /// `merge_ranges` takes HALF-OPEN `(start, count)` ranges, so `start +
    /// count` is the EXCLUSIVE end and two ranges touch exactly when the next
    /// `start` equals the previous end. A range that starts one sector LATER
    /// than that has a genuine, untouched sector between them, and merging
    /// across it claims coverage of a sector neither input ever described.
    ///
    /// That matters because the merged output is a boundary, not a hint:
    /// `disc::merged_extents` feeds `Disc::encrypted_content_ranges`, which
    /// decides which sectors the decrypting source treats as content. A sector
    /// in a real gap belongs to no title extent — it is nav/UDF/padding — and
    /// must stay outside the content gate, not be folded into it.
    #[test]
    fn merge_ranges_keeps_a_genuine_one_sector_gap() {
        // [0,5) and [6,9): sector 5 is in neither. The ranges are NOT adjacent.
        assert_eq!(
            merge_ranges(&[(0, 5), (6, 3)]),
            vec![(0, 5), (6, 3)],
            "sector 5 is in neither input range, so the two must stay disjoint"
        );
        // Exactly touching ([0,5) then [5,3)) still merges — that is adjacency.
        assert_eq!(
            merge_ranges(&[(0, 5), (5, 3)]),
            vec![(0, 8)],
            "start == exclusive end is a true touch and must still merge"
        );
    }

    #[test]
    fn merge_ranges_saturates_near_u32_max() {
        // Adjacent ranges near u32::MAX must not panic (debug) or wrap.
        let ranges = [(u32::MAX - 1, 2), (u32::MAX, 5)];
        let merged = merge_ranges(&ranges);
        // No panic; result is a single merged range starting at the first.
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].0, u32::MAX - 1);
    }

    #[test]
    fn buffered_reader_short_buf_errors_not_panics() {
        let mut inner = MapReader::new();
        inner.put(0, [0u8; 2048]);
        let mut br = BufferedSectorReader::new(&mut inner, 8);
        let mut tiny = [0u8; 100];
        let err = br.read_sectors(0, 1, &mut tiny, true);
        assert!(matches!(err, Err(Error::UdfBufferTooSmall)));
    }

    /// Minimal in-memory SectorSource that serves pre-loaded 2048-byte
    /// sectors by LBA. Unmapped LBAs read as zeros.
    struct MemReader {
        sectors: HashMap<u32, [u8; 2048]>,
    }

    impl MemReader {
        fn new() -> Self {
            Self {
                sectors: HashMap::new(),
            }
        }
        fn put(&mut self, lba: u32, sector: [u8; 2048]) {
            self.sectors.insert(lba, sector);
        }
    }

    impl SectorSource for MemReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                let dst = &mut buf[off..off + 2048];
                match self.sectors.get(&(lba + i)) {
                    Some(s) => dst.copy_from_slice(s),
                    None => dst.fill(0),
                }
            }
            Ok(count as usize * 2048)
        }
    }

    /// Build an Extended File Entry (tag 266) ICB sector with a single
    /// short allocation descriptor declaring `data_len` bytes at `data_lba`.
    /// `info_length` (offset 56) is set to `info_len`.
    fn build_efe_icb(info_len: u64, data_len: u32, data_lba: u32) -> [u8; 2048] {
        let mut icb = [0u8; 2048];
        // tag identifier 266 (Extended File Entry)
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        // info_length at offset 56
        icb[56..64].copy_from_slice(&info_len.to_le_bytes());
        // l_ea = 0 at offset 208, l_ad = 8 (one short AD) at offset 212
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        icb[212..216].copy_from_slice(&8u32.to_le_bytes());
        // ad_offset = 216 + l_ea = 216. Short AD: len(4) | lba(4).
        // extent_type 0 (recorded) is the top 2 bits = 0, so raw == len.
        icb[216..220].copy_from_slice(&(data_len & 0x3FFF_FFFF).to_le_bytes());
        icb[220..224].copy_from_slice(&data_lba.to_le_bytes());
        icb
    }

    /// Build a UdfFs with a single file entry under root, for read_file tests.
    fn fs_with_file(meta_lba: u32, size: u64) -> UdfFs {
        UdfFs {
            root: DirEntry {
                name: String::new(),
                is_dir: true,
                meta_lba: 0,
                size: 0,
                entries: vec![DirEntry {
                    name: "F".to_string(),
                    is_dir: false,
                    meta_lba,
                    size,
                    entries: Vec::new(),
                }],
            },
            volume_id: String::new(),
            partition_start: 0,
            metadata_start: 0,
            metadata_sectors: 0,
        }
    }

    #[test]
    fn read_file_rejects_oversized_extent_before_allocating() {
        // data_len just over the 64 MiB cap must error, not allocate.
        let oversized = MAX_FILE_BYTES as u32 + 2048;
        let icb = build_efe_icb(oversized as u64, oversized, 100);
        let mut reader = MemReader::new();
        reader.put(10, icb); // ICB at meta_lba 10 (metadata_start 0)

        let fs = fs_with_file(10, oversized as u64);
        let err = fs.read_file(&mut reader, "/F").unwrap_err();
        assert!(matches!(err, Error::DiscRead { .. }));
    }

    /// Build an Extended File Entry ICB with multiple inline short ADs, each
    /// `(data_len, data_lba)`. Lets a test chain extents whose individual
    /// lengths are all under the per-extent cap but whose running total
    /// exceeds MAX_FILE_BYTES.
    fn build_efe_icb_multi(info_len: u64, ads: &[(u32, u32)]) -> [u8; 2048] {
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        icb[56..64].copy_from_slice(&info_len.to_le_bytes());
        let l_ad = (ads.len() * 8) as u32;
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        icb[212..216].copy_from_slice(&l_ad.to_le_bytes());
        for (i, (data_len, data_lba)) in ads.iter().enumerate() {
            let off = 216 + i * 8;
            icb[off..off + 4].copy_from_slice(&(data_len & 0x3FFF_FFFF).to_le_bytes());
            icb[off + 4..off + 8].copy_from_slice(&data_lba.to_le_bytes());
        }
        icb
    }

    #[test]
    fn read_file_rejects_cumulative_extents_over_cap() {
        // Two extents each within MAX_FILE_BYTES individually but exceeding it
        // together: first extent buffers 2048 bytes, second is exactly the cap,
        // so the cumulative guard must fire on the second before reading it.
        let big = MAX_FILE_BYTES as u32;
        let icb = build_efe_icb_multi(MAX_FILE_BYTES * 2, &[(2048, 100), (big, 200_000)]);
        let mut reader = MemReader::new();
        reader.put(10, icb);
        let mut data_sector = [0u8; 2048];
        data_sector[0] = 0xCD;
        reader.put(100, data_sector);

        // entry.size declared small so the entry.size cap passes; the
        // cumulative extent total is what must trip the guard.
        let fs = fs_with_file(10, 2048);
        let err = fs.read_file(&mut reader, "/F").unwrap_err();
        assert!(matches!(err, Error::DiscRead { .. }));
    }

    #[test]
    fn read_file_rejects_oversized_info_length() {
        // Small extent but a crafted huge info_length (entry.size) must also
        // be rejected before truncate could be reached.
        let icb = build_efe_icb(0, 2048, 100);
        let mut reader = MemReader::new();
        reader.put(10, icb);

        let fs = fs_with_file(10, MAX_FILE_BYTES + 1);
        let err = fs.read_file(&mut reader, "/F").unwrap_err();
        assert!(matches!(err, Error::DiscRead { .. }));
    }

    #[test]
    fn read_file_accepts_small_file() {
        // A 1-sector file within the cap reads back its declared size.
        let icb = build_efe_icb(2048, 2048, 100);
        let mut reader = MemReader::new();
        reader.put(10, icb);
        // file data sector at partition_start + data_lba = 0 + 100
        let mut data_sector = [0u8; 2048];
        data_sector[0] = 0xAB;
        reader.put(100, data_sector);

        let fs = fs_with_file(10, 2048);
        let data = fs
            .read_file(&mut reader, "/F")
            .expect("small file should read");
        assert_eq!(data.len(), 2048);
        assert_eq!(data[0], 0xAB);
    }

    #[test]
    fn read_directory_rejects_oversized_dir_before_allocating() {
        // A directory ICB declaring an allocation length above the 1 MiB
        // ceiling must error rather than allocate a huge buffer.
        let oversized = MAX_DIR_BYTES + 2048;
        let icb = build_efe_icb(oversized as u64, oversized, 50);
        let mut reader = MemReader::new();
        reader.put(5, icb); // directory ICB at meta_start(0) + meta_lba(5)

        let err = read_directory(&mut reader, 0, 0, 5, "DIR", 0, &mut 0, &mut HashSet::new())
            .unwrap_err();
        assert!(matches!(err, Error::DiscRead { .. }));
    }

    #[test]
    fn read_directory_accepts_small_empty_dir() {
        // ad_len within the cap, pointing at zeroed directory data → an empty
        // (no valid FID) directory parses without error.
        let icb = build_efe_icb(2048, 2048, 50);
        let mut reader = MemReader::new();
        reader.put(5, icb);
        // directory data at meta_start(0) + ad_pos(50) = 50 reads as zeros.
        let dir = read_directory(&mut reader, 0, 0, 5, "DIR", 0, &mut 0, &mut HashSet::new())
            .expect("small dir parses");
        assert!(dir.entries.is_empty());
        assert!(dir.is_dir);
    }

    // ---- added: spec-boundary coverage for AD strides, flags, FIDs ----

    /// Build an Extended File Entry (tag 266) ICB whose allocation
    /// descriptors are EXTENDED ADs (ECMA-167 §14.14.3, 20 bytes each):
    ///   ExtentLength(4) | RecordedLength(4) | InformationLength(4) |
    ///   ExtentLocation lb_addr { logicalBlockNumber(4) | partitionRef(2) } |
    ///   impl_use(2)
    /// The 30-bit length + 2-bit type live in ExtentLength (offset +0); the
    /// logical block number lives in ExtentLocation at offset +12. Sets ICB
    /// Tag flags (abs offset 34) low bits to 2 = Extended AD so the parser
    /// must select the 20-byte stride AND read the LBA from off+12, not off+4.
    fn build_efe_ext(info_length: u64, ads: &[(u32, u32, u32)]) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // tag
        // ICB Tag flags at abs offset 34: AD type 2 = Extended AD.
        s[34..36].copy_from_slice(&2u16.to_le_bytes());
        s[56..64].copy_from_slice(&info_length.to_le_bytes());
        let l_ea: u32 = 0;
        let l_ad: u32 = (ads.len() * 20) as u32;
        s[208..212].copy_from_slice(&l_ea.to_le_bytes());
        s[212..216].copy_from_slice(&l_ad.to_le_bytes());
        let mut off = 216 + l_ea as usize;
        for &(etype, dlen, dlba) in ads {
            let raw_len = (etype << 30) | (dlen & 0x3FFF_FFFF);
            // ExtentLength at +0 (carries type + 30-bit length).
            s[off..off + 4].copy_from_slice(&raw_len.to_le_bytes());
            // RecordedLength (+4) and InformationLength (+8) set to distinct
            // non-zero junk so a parser misreading the LBA at off+4 would
            // pick THESE up instead of the real LBA at off+12.
            s[off + 4..off + 8].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
            s[off + 8..off + 12].copy_from_slice(&0xCAFE_BABEu32.to_le_bytes());
            // ExtentLocation logicalBlockNumber at +12.
            s[off + 12..off + 16].copy_from_slice(&dlba.to_le_bytes());
            off += 20;
        }
        s
    }

    #[test]
    fn icb_extents_extended_ad_uses_20byte_stride_and_lba_at_off12() {
        // ECMA-167 §14.14.3: Extended AD is 20 bytes, extent LBA at +12 (not
        // +4, which is RecordedLength). Flags==2 selects this 20-byte stride.
        // Each LBA uses 4 distinct non-zero bytes so a wrong-offset read changes the result.
        let icb = build_efe_ext(
            3 * 2048,
            &[
                (0, 2048, 0x0102_0304),
                (0, 2048, 0x0506_0708),
                (0, 4096, 0x090A_0B0C),
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("EXT", 5, 3 * 2048));
        let extents = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        // If the stride were wrong (8 or 16) or lba_off were off+4, the LBAs
        // would be the 0xDEADBEEF junk or misaligned garbage, not these.
        assert_eq!(
            extents,
            vec![
                (0x0102_0304, 2048),
                (0x0506_0708, 2048),
                (0x090A_0B0C, 4096)
            ]
        );
    }

    #[test]
    fn icb_extents_short_ad_type1_sparse_extent_is_kept_and_flagged_unrecorded() {
        // ECMA-167 4/14.14.1.1: type 1 = allocated-not-recorded, no on-disc data,
        // but it still occupies file byte space, so it must be kept (flagged
        // unrecorded), not dropped — dropping it would shift later extents.
        let icb = build_efe(
            6144,
            &[
                (0, 2048, 10), // recorded
                (1, 2048, 20), // allocated, not recorded
                (0, 2048, 30), // recorded, after the hole
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("SP", 5, 6144));
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        assert_eq!(
            extents,
            vec![
                IcbExtent {
                    lba: 10,
                    len: 2048,
                    recorded: true
                },
                IcbExtent {
                    lba: 20,
                    len: 2048,
                    recorded: false
                },
                IcbExtent {
                    lba: 30,
                    len: 2048,
                    recorded: true
                },
            ]
        );
    }

    /// AD type 3 is EMBEDDED data, not a descriptor list.
    ///
    /// RED BEFORE GREEN: with the old `_ => 8` fallback this returned
    /// `Ok([IcbExtent { lba: 999, len: 2048 }])` — an extent decoded out of
    /// the file's own CONTENT bytes. A rip would then read sector 999, which
    /// holds something else entirely, and emit it as this title's stream at
    /// rc=0. That is the failure this refusal exists to prevent, so the
    /// fixture deliberately makes the content decode as a PLAUSIBLE extent
    /// rather than as garbage: garbage would have been caught by the
    /// terminator check anyway, and would prove nothing.
    #[test]
    fn icb_extents_embedded_data_is_refused_not_decoded_as_descriptors() {
        let mut icb = build_efe(2048, &[(0, 2048, 999)]);
        // ICB Tag flags at abs offset 34: low 3 bits = 3 (embedded).
        icb[34..36].copy_from_slice(&3u16.to_le_bytes());
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("EMB", 5, 2048));
        let err = fs
            .read_icb_extents(&mut reader, 5)
            .expect_err("an embedded ICB has no extents to hand out");
        assert!(
            matches!(err, Error::UdfEmbeddedData),
            "expected UdfEmbeddedData, got {err:?}"
        );
        assert_eq!(err.code(), crate::error::E_UDF_EMBEDDED_DATA);
    }

    /// ...but a ZERO-LENGTH file may legally be encoded as embedded-with-
    /// nothing-embedded. It has no content and no extents, so it is not a
    /// failure — refusing it would fail a rip over a legal empty file.
    #[test]
    fn icb_extents_embedded_but_empty_is_not_an_error() {
        let mut icb = build_efe(0, &[]);
        icb[34..36].copy_from_slice(&3u16.to_le_bytes());
        icb[212..216].copy_from_slice(&0u32.to_le_bytes()); // l_ad = 0
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("EMPTY", 5, 0));
        assert_eq!(
            fs.read_icb_extents(&mut reader, 5)
                .expect("legal empty file"),
            vec![]
        );
    }

    /// A ZERO-LENGTH unrecorded descriptor must not cost the file its plan.
    ///
    /// `file_extents` refuses a file whose extent list contains an unrecorded
    /// hole, because reading one splices undefined sectors into the rip and
    /// dropping one slides every later extent's byte space. Neither is true of
    /// a hole with LENGTH ZERO: it displaces nothing, every later extent sits
    /// at the same offset with or without it, and the callers' own
    /// `sectors > 0 && lba > 0` filter already discards it.
    ///
    /// Refusing on `!recorded` alone therefore dropped whole titles off discs
    /// that ripped correctly before — the reverse of the defect the refusal
    /// exists for, and just as silent, since the title simply vanishes.
    #[test]
    fn file_extents_accepts_a_zero_length_unrecorded_extent() {
        // A zero-length type-1 hole, then the file's real 4096 bytes.
        let icb = build_efe(4096, &[(1, 0, 4999), (0, 4096, 5000)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(
            0,
            0,
            DirEntry {
                name: String::new(),
                is_dir: true,
                meta_lba: 0,
                size: 0,
                entries: vec![file_entry("ZL", 5, 4096)],
            },
        );

        // Fixture check: the hole really is present and really is zero-length,
        // or this test proves nothing.
        assert_eq!(
            fs.extents_abs_at(&mut reader, 5).expect("extents"),
            vec![
                AbsExtent {
                    lba: 4999,
                    len: 0,
                    recorded: false
                },
                AbsExtent {
                    lba: 5000,
                    len: 4096,
                    recorded: true
                },
            ],
            "fixture must present a ZERO-LENGTH unrecorded extent"
        );

        let plan = fs
            .file_extents(&mut reader, "/ZL")
            .expect("a zero-length hole displaces nothing, so the file is readable");
        assert!(
            plan.iter().any(|&(lba, n)| lba == 5000 && n == 2),
            "the real 4096 bytes must still be in the read plan; got {plan:?}"
        );
    }

    #[test]
    fn icb_extents_short_ad_type2_sparse_extent_is_kept_and_flagged_unrecorded() {
        // ECMA-167 4/14.14.1.1 type 2 = "not recorded, not allocated", same
        // byte-space semantics as type 1: must be flagged unrecorded, not
        // dropped via the catch-all `_ => break` that used to truncate the list.
        let icb = build_efe(
            6144,
            &[
                (0, 2048, 10), // recorded
                (2, 2048, 20), // not recorded, not allocated
                (0, 2048, 30), // recorded, after the hole
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("SP", 5, 6144));
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        assert_eq!(
            extents,
            vec![
                IcbExtent {
                    lba: 10,
                    len: 2048,
                    recorded: true
                },
                IcbExtent {
                    lba: 20,
                    len: 2048,
                    recorded: false
                },
                IcbExtent {
                    lba: 30,
                    len: 2048,
                    recorded: true
                },
            ],
            "a type-2 hole must not truncate the list: the type-0 extent after \
             it is part of the file"
        );
    }

    /// `file_extents` builds the extent list a Blu-ray / HD-DVD TITLE is
    /// actually ripped through (`disc/bluray.rs`, `disc/hddvd.rs`,
    /// `mux/resolve.rs`). Its `(lba, sector_count)` tuple cannot say "this
    /// range is a hole", and both ways of pretending otherwise are wrong:
    ///
    /// * folding an unrecorded extent in makes the mux read undefined sectors
    ///   and splice whatever the media holds there into the stream as content;
    /// * dropping it slides every later extent's byte space, and that list IS
    ///   a byte-space map (`aacs::segment::clip_byte_to_lba`).
    ///
    /// So at this signature the only truthful answer is refusal.
    #[test]
    fn file_extents_refuses_a_file_with_an_unrecorded_extent() {
        let icb = build_efe(
            6144,
            &[
                (0, 2048, 10), // recorded
                (1, 2048, 20), // allocated, NOT recorded — undefined sectors
                (0, 2048, 30), // recorded, after the hole
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(
            0,
            0,
            DirEntry {
                name: String::new(),
                is_dir: true,
                meta_lba: 0,
                size: 0,
                entries: vec![file_entry("SP", 5, 6144)],
            },
        );
        let res = fs.file_extents(&mut reader, "/SP");
        assert!(
            res.is_err(),
            "an unrecorded extent must never reach the title's extent list as \
             readable content; got {res:?}"
        );
    }

    #[test]
    fn icb_extents_zero_length_type0_terminates_list() {
        // ECMA-167: a zero-length type-0 AD terminates the descriptor list, so
        // trailing zero padding must stop parsing before a stray non-zero AD
        // after it becomes a bogus extent.
        let icb = build_efe(
            2048,
            &[
                (0, 2048, 10),  // recorded extent
                (0, 0, 0),      // zero-length type-0 = terminator
                (0, 4096, 999), // must NOT be parsed
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("T", 5, 2048));
        let extents = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(
            extents,
            vec![(10, 2048)],
            "parsing must stop at the zero-length terminator"
        );
    }

    /// Hostile input: a type-3 continuation descriptor whose continuation block
    /// points back at itself (a cycle). The `MAX_AD_BLOCKS` bound must make this
    /// TERMINATE rather than loop forever — and terminating by exhausting the
    /// budget is not the same as reaching the end of the chain.
    ///
    /// It used to fall through to `Ok(extents)` with the list silently cut short
    /// at whatever the 256th hop had collected. `disc/extract.rs` then zero-pads
    /// the missing tail out to the entry's declared size, sets `complete = true`
    /// with `bytes_unreadable = 0`, and renames off `.partial` — a mostly-zero
    /// file delivered as a verified complete extraction. Exhausting the budget
    /// means "I do not know the rest of this file", and the only honest answer
    /// is an error.
    #[test]
    fn icb_extents_exhausted_continuation_budget_is_an_error_not_a_short_list() {
        let icb = build_efe(2048, &[(0, 2048, 10), (3, 2048, 50)]);
        let cont = build_cont_block(&[(0, 2048, 20), (3, 2048, 50)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        reader.put(50, cont);
        let fs = fs_with(0, 0, file_entry("LOOP", 5, 2048));
        // Bounded: must not hang or panic. And must not report success.
        match fs.read_icb_extents(&mut reader, 5) {
            Err(e) => assert!(
                matches!(e, Error::UdfAdChainTooLong),
                "the hop budget must report itself with its own code: {e:?}"
            ),
            Ok(extents) => panic!(
                "an unterminated AD chain returned Ok with {} extents — a \
                 SHORT extent list reported as the whole file; the caller \
                 zero-pads the tail and calls the result complete",
                extents.len()
            ),
        }
    }

    /// The honest path this fix must not break: a continuation chain that ENDS
    /// within the budget still returns its full extent list, in file order.
    #[test]
    fn a_terminating_continuation_chain_still_returns_every_extent() {
        // ICB → cont block at 50 → cont block at 51 → end.
        let icb = build_efe(2048, &[(0, 2048, 10), (3, 2048, 50)]);
        let cont_a = build_cont_block(&[(0, 2048, 20), (3, 2048, 51)]);
        let cont_b = build_cont_block(&[(0, 2048, 30), (0, 4096, 40)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        reader.put(50, cont_a);
        reader.put(51, cont_b);
        let fs = fs_with(0, 0, file_entry("CHAIN", 5, 2048));
        let got = tuples(&fs.read_icb_extents(&mut reader, 5).expect("extents"));
        assert_eq!(
            got,
            vec![(10, 2048), (20, 2048), (30, 2048), (40, 4096)],
            "a chain under the hop budget is a normal file, not an error"
        );
    }

    #[test]
    fn parse_udf_name_decodes_utf16be_compression_id_16() {
        // UDF dchar: compression ID 16 = 16-bit big-endian Unicode. A FID
        // whose filename uses ID 16 must decode correctly, not as mojibake.
        // Bytes: [16][00 'A'][00 'Z'].
        let raw = [16u8, 0x00, b'A', 0x00, b'Z'];
        assert_eq!(parse_udf_name(&raw), "AZ");
    }

    #[test]
    fn parse_udf_name_utf16be_uses_both_bytes_of_each_dchar_and_stops_on_an_odd_tail() {
        // ECMA-167 1/7.2.2: id 16 = 16-bit BIG-endian dchars (real discs carry
        // non-Latin-1 names). Byte run is deliberately ODD-length, as a truncated
        // field would be: the trailing lone byte must not pair with what follows.
        let raw = [16u8, 0x4E, 0x2D, 0x00, b'A', 0x42];
        assert_eq!(
            parse_udf_name(&raw),
            "\u{4E2D}A",
            "0x4E,0x2D is one dchar U+4E2D, and the lone 0x42 is not a character"
        );
    }

    #[test]
    fn parse_udf_name_8bit_compression_id_8() {
        // Compression ID 8 = 8-bit (OSTA CS0 / ASCII). "BDMV" must round-trip.
        let mut raw = vec![8u8];
        raw.extend_from_slice(b"BDMV");
        assert_eq!(parse_udf_name(&raw), "BDMV");
    }

    /// A disc read failure while fetching an entry's ICB must propagate, not
    /// become a file size of zero. A zero size is indistinguishable from a
    /// genuinely empty file, so an unreadable ICB on a damaged disc silently
    /// changed which titles a caller saw as present.
    #[test]
    fn read_directory_propagates_an_icb_read_failure_instead_of_size_zero() {
        /// Serves every sector from an inner MemReader except one, which fails
        /// the way a bad sector does.
        struct FailingAt {
            inner: MemReader,
            fail_lba: u32,
        }
        impl SectorSource for FailingAt {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                recovery: bool,
            ) -> Result<usize> {
                if lba == self.fail_lba {
                    return Err(Error::DiscRead {
                        sector: lba as u64,
                        status: None,
                        sense: None,
                    });
                }
                self.inner.read_sectors(lba, count, buf, recovery)
            }
        }

        // One directory holding one file entry whose ICB lives at LBA 7.
        let mut dir = [0u8; 2048];
        let mut name_bytes = vec![8u8];
        name_bytes.extend_from_slice(b"CLPI");
        dir[0..2].copy_from_slice(&257u16.to_le_bytes());
        dir[18] = 0x00; // a file, not a parent, not a dir
        dir[19] = name_bytes.len() as u8;
        dir[24..28].copy_from_slice(&7u32.to_le_bytes());
        dir[38..38 + name_bytes.len()].copy_from_slice(&name_bytes);

        let mut inner = MemReader::new();
        inner.put(5, build_efe_icb(2048, 2048, 60));
        inner.put(60, dir);
        inner.put(7, build_efe_icb(123, 2048, 0));

        // Sanity: with every sector readable the entry parses and carries its
        // real size, so the failure below is the ICB read and nothing else.
        let ok = read_directory(&mut inner, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses when every sector reads");
        assert_eq!(ok.entries.len(), 1);
        assert_eq!(ok.entries[0].size, 123);

        let mut reader = FailingAt {
            inner: MemReader::new(),
            fail_lba: 7,
        };
        reader.inner.put(5, build_efe_icb(2048, 2048, 60));
        reader.inner.put(60, dir);
        reader.inner.put(7, build_efe_icb(123, 2048, 0));

        let err = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect_err("an unreadable entry ICB must fail the scan, not report size 0");
        assert!(
            matches!(err, Error::DiscRead { sector: 7, .. }),
            "the propagated error must name the sector that failed, got {err:?}"
        );
    }

    #[test]
    fn read_directory_honors_l_iu_offset_for_fid_name() {
        // ECMA-167 §14.4 File Identifier: the name begins at offset 38 + L_IU.
        // A non-zero L_IU must shift the name read; ignoring it reads impl_use
        // bytes as the name instead.
        let mut dir = [0u8; 2048];
        let l_iu: u16 = 4;
        let mut name_bytes = vec![8u8]; // compression id 8
        name_bytes.extend_from_slice(b"CLPI");
        let l_fi = name_bytes.len() as u8;
        dir[0..2].copy_from_slice(&257u16.to_le_bytes());
        dir[18] = 0x00; // not parent, not dir → a file
        dir[19] = l_fi;
        dir[24..28].copy_from_slice(&7u32.to_le_bytes()); // child ICB LBA
        dir[36..38].copy_from_slice(&l_iu.to_le_bytes());
        dir[38..42].copy_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC]); // impl_use junk
        let name_start = 38 + l_iu as usize;
        dir[name_start..name_start + name_bytes.len()].copy_from_slice(&name_bytes);

        let dir_icb = build_efe_icb(2048, 2048, 60); // dir data at ad_pos 60
        let mut reader = MemReader::new();
        reader.put(5, dir_icb);
        reader.put(60, dir);
        reader.put(7, build_efe_icb(123, 2048, 0)); // child size ICB

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses");
        assert_eq!(parsed.entries.len(), 1, "exactly one FID entry");
        assert_eq!(
            parsed.entries[0].name, "CLPI",
            "name must be read at 38+L_IU, not from impl_use bytes"
        );
        assert!(!parsed.entries[0].is_dir);
    }

    #[test]
    fn read_directory_skips_parent_fid_entry() {
        // ECMA-167 §14.4.3: characteristics bit 0x08 = "parent" (the ".." link)
        // and must never appear as a named child. Fixture gives it a valid
        // non-zero L_FI and real name to isolate that gate from L_FI==0.
        let mut dir = [0u8; 2048];
        let mut name_bytes = vec![8u8];
        name_bytes.extend_from_slice(b"PARENT");
        let l_fi = name_bytes.len() as u8;
        dir[0..2].copy_from_slice(&257u16.to_le_bytes());
        dir[18] = 0x08 | 0x02; // parent + directory bits
        dir[19] = l_fi; // non-zero L_FI: name present but must be ignored
        dir[24..28].copy_from_slice(&9u32.to_le_bytes());
        dir[36..38].copy_from_slice(&0u16.to_le_bytes()); // L_IU = 0
        dir[38..38 + name_bytes.len()].copy_from_slice(&name_bytes);

        let dir_icb = build_efe_icb(2048, 2048, 60);
        let mut reader = MemReader::new();
        reader.put(5, dir_icb);
        reader.put(60, dir);
        reader.put(9, build_efe_icb(0, 2048, 0)); // child size ICB

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses");
        assert!(
            parsed.entries.is_empty(),
            "the parent (..) FID must not be emitted even with a valid name"
        );
    }

    #[test]
    fn parse_dstring_length_byte_caps_content() {
        // UDF d-string: the final byte of the fixed field is the length of
        // valid content (compression id + chars). Bytes past that length must
        // be ignored. Field: [8]['V']['O']['L'] ... last byte = 4.
        let mut field = [0u8; 32];
        field[0] = 8; // compression id 8
        field[1] = b'V';
        field[2] = b'O';
        field[3] = b'L';
        field[10] = b'X'; // garbage beyond declared length — must be ignored
        *field.last_mut().unwrap() = 4; // 4 valid bytes (id + 3 chars)
        assert_eq!(parse_dstring(&field), "VOL");
    }

    #[test]
    fn parse_dstring_decodes_a_utf16be_volume_identifier() {
        // ECMA-167 1/7.2.12 d-strings: same OSTA CS0 encoding as file names, used
        // byte count in the last field byte. Content covers a char above Latin-1,
        // an embedded NUL pair (padding, not a char), and an odd trailing byte.
        let mut field = [0u8; 32];
        let content: [u8; 10] = [16, 0x4E, 0x2D, 0x00, b'A', 0x00, 0x00, 0x00, b'B', 0x43];
        field[..content.len()].copy_from_slice(&content);
        *field.last_mut().unwrap() = content.len() as u8;
        assert_eq!(
            parse_dstring(&field),
            "\u{4E2D}AB",
            "big-endian pairs, no NUL padding, and no character made from the odd tail byte"
        );
    }

    #[test]
    fn parse_dstring_oversized_length_byte_returns_empty_not_panic() {
        // Hostile/corrupt input: a length byte larger than the field must not
        // index out of bounds. parse_dstring guards len > data.len() → "".
        let mut field = [0u8; 8];
        field[0] = 8;
        field[1] = b'A';
        *field.last_mut().unwrap() = 200; // way past the 8-byte field
        assert_eq!(parse_dstring(&field), "");
    }

    /// Build an Extended File Entry whose data is EMBEDDED in the ICB —
    /// ICB Tag flags (abs offset 34) low three bits == 3, ECMA-167 4/14.6.8.
    /// The allocation-descriptor area then holds the file's bytes, not
    /// descriptors. Tiny files (the AACS `*.inf` key files, small nav files)
    /// are routinely recorded this way.
    fn build_inline_icb(info_len: u64, payload: &[u8]) -> [u8; 2048] {
        build_inline_icb_tagged(266, 0, info_len, payload)
    }

    /// As [`build_inline_icb`], for either entry type and with an explicit
    /// extended-attribute length: a 261 File Entry keeps L_EA at 168 and its
    /// descriptor area at 176 + L_EA, a 266 at 208 and 216 + L_EA.
    fn build_inline_icb_tagged(tag: u16, l_ea: usize, info_len: u64, payload: &[u8]) -> [u8; 2048] {
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&tag.to_le_bytes());
        icb[34..36].copy_from_slice(&3u16.to_le_bytes()); // AD type 3 = embedded
        icb[56..64].copy_from_slice(&info_len.to_le_bytes());
        let (l_ea_off, base) = if tag == 266 {
            (208usize, 216usize)
        } else {
            (168usize, 176usize)
        };
        icb[l_ea_off..l_ea_off + 4].copy_from_slice(&(l_ea as u32).to_le_bytes());
        icb[l_ea_off + 4..l_ea_off + 8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        icb[base..base + l_ea].fill(0xA5);
        let at = base + l_ea;
        if at + payload.len() <= icb.len() {
            icb[at..at + payload.len()].copy_from_slice(payload);
        }
        icb
    }

    #[test]
    fn read_file_reads_embedded_data_from_both_entry_types_at_their_own_offsets() {
        // Embedded data starts at 176 + L_EA (File Entry, ECMA-167 4/14.9) or
        // 216 + L_EA (Extended File Entry, 4/14.17); a wrong offset doesn't fail
        // loudly, it just reads extended-attribute bytes as an AACS `*.inf` key.
        let payload: Vec<u8> = (0..48u8)
            .map(|i| i.wrapping_mul(11).wrapping_add(3))
            .collect();
        for tag in [261u16, 266u16] {
            for l_ea in [0usize, 24] {
                let mut reader = MapReader::new();
                reader.put(5, build_inline_icb_tagged(tag, l_ea, 48, &payload));
                let fs = fs_with_file(5, 48);
                let got = fs
                    .read_file(&mut reader, "/F")
                    .unwrap_or_else(|e| panic!("tag {tag}, L_EA {l_ea}: {e:?}"));
                assert_eq!(got, payload, "tag {tag}, L_EA {l_ea}");
            }
        }
    }

    #[test]
    fn read_file_reads_embedded_data_flush_with_the_end_of_the_entry() {
        // The embedded payload may run exactly to the end of the 2048-byte
        // logical block. Those bytes are inside the entry and are the file:
        // refusing them turns a readable file into a disc-read failure.
        let len = 2048 - 216;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8 + 1).collect();
        let mut reader = MapReader::new();
        reader.put(5, build_inline_icb(len as u64, &payload));
        let fs = fs_with_file(5, len as u64);

        let got = fs
            .read_file(&mut reader, "/F")
            .expect("a payload flush with the end of the entry is in bounds");
        assert_eq!(got, payload);
    }

    #[test]
    fn read_file_refuses_embedded_data_that_runs_past_the_entry() {
        // L_AD is disc-controlled; one claiming more embedded bytes than the
        // entry holds must be refused, not clamped to a short-but-valid-looking
        // prefix of a key file.
        let mut icb = build_inline_icb(64, &[0xAB; 64]);
        icb[212..216].copy_from_slice(&4000u32.to_le_bytes()); // L_AD past the sector
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with_file(5, 64);

        let err = fs
            .read_file(&mut reader, "/F")
            .expect_err("an embedded payload larger than the entry cannot be read");
        assert!(matches!(err, Error::DiscRead { .. }), "got {err:?}");
    }

    #[test]
    fn read_sectors_batches_a_cold_single_sector_read_into_a_full_window() {
        // One single-sector request should pull a `batch`-sector window so the
        // neighbouring requests UDF parsing issues constantly cost nothing; a
        // wrong window still returns correct bytes, so only the call count shows it.
        let mut inner = CountReader::new();
        {
            let mut br = BufferedSectorReader::new(&mut inner, 4);
            let mut buf = [0u8; 2048];
            for lba in [500u32, 501, 502, 503] {
                br.read_sectors(lba, 1, &mut buf, true)
                    .unwrap_or_else(|e| panic!("read of {lba}: {e:?}"));
                assert_eq!(buf, CountReader::expected(lba), "sector {lba}");
            }
        }
        assert_eq!(
            inner.calls, 1,
            "one batch command must serve all four sectors of the window"
        );
        assert_eq!(inner.sectors_read, 4);
    }

    #[test]
    fn read_file_trims_embedded_data_to_the_declared_information_length() {
        // ECMA-167 4/14.17: Information Length is the real size; the embedded
        // area (L_AD bytes) is alignment-padded and routinely longer. Returning
        // the padding makes an AACS `*.inf` or nav file parse with trailing garbage.
        let payload: Vec<u8> = (0..64u8)
            .map(|i| i.wrapping_mul(7).wrapping_add(1))
            .collect();
        let mut reader = MapReader::new();
        reader.put(5, build_inline_icb(20, &payload));
        let fs = fs_with_file(5, 20);

        let got = fs
            .read_file(&mut reader, "/F")
            .expect("embedded file reads");
        assert_eq!(
            got,
            payload[..20].to_vec(),
            "an embedded file is Information Length bytes long, not L_AD bytes"
        );
    }

    #[test]
    fn read_file_prefix_caps_embedded_data_at_the_requested_length() {
        // `read_file_prefix` bounds a read (e.g. the AACS `MKB_RO.inf`'s ~128 MiB
        // of padding, when only the leading record is wanted); that bound must
        // apply on the embedded path too, since nothing else limits it there.
        let payload: Vec<u8> = (0..64u8)
            .map(|i| i.wrapping_mul(7).wrapping_add(1))
            .collect();
        let mut reader = MapReader::new();
        reader.put(5, build_inline_icb(64, &payload));
        let fs = fs_with_file(5, 64);

        let got = fs
            .read_file_prefix(&mut reader, "/F", 8)
            .expect("embedded prefix reads");
        assert_eq!(got, payload[..8].to_vec(), "the requested prefix bounds it");
    }

    #[test]
    fn file_start_lba_descends_only_through_directories_that_match_the_path() {
        // Each path component must match a child that is BOTH a directory AND
        // named for it. Matching either alone walks into the first directory
        // met — e.g. `AACS/` next to `BDMV/` — and reports an LBA from the wrong subtree.
        let fs = fs_with(
            1000,
            0,
            DirEntry {
                name: String::new(),
                is_dir: true,
                meta_lba: 0,
                size: 0,
                entries: vec![
                    DirEntry {
                        name: "AACS".to_string(),
                        is_dir: true,
                        meta_lba: 0,
                        size: 0,
                        entries: vec![file_entry("INDEX.BDMV", 6, 2048)],
                    },
                    DirEntry {
                        name: "BDMV".to_string(),
                        is_dir: true,
                        meta_lba: 0,
                        size: 0,
                        entries: vec![file_entry("INDEX.BDMV", 7, 2048)],
                    },
                ],
            },
        );
        let mut reader = MapReader::new();
        reader.put(6, build_efe(2048, &[(0, 2048, 111)])); // the AACS/ copy
        reader.put(7, build_efe(2048, &[(0, 2048, 222)])); // the BDMV/ copy

        let lba = fs
            .file_start_lba(&mut reader, "/BDMV/INDEX.BDMV")
            .expect("the path resolves");
        assert_eq!(lba, 1000 + 222, "BDMV/INDEX.BDMV, not AACS/INDEX.BDMV");
    }

    #[test]
    fn read_file_accepts_a_file_exactly_at_the_size_ceiling() {
        // `MAX_FILE_BYTES` bounds what an unbounded read may allocate. A file
        // exactly at the ceiling is admissible: rejecting it makes the largest
        // legal read indistinguishable from a corrupt disc.
        let cap = MAX_FILE_BYTES as u32;
        let mut reader = MapReader::new();
        reader.put(5, build_efe(MAX_FILE_BYTES, &[(0, cap, 40)]));
        let fs = fs_with_file(5, MAX_FILE_BYTES);

        let got = fs
            .read_file(&mut reader, "/F")
            .expect("a file exactly at the ceiling must be readable");
        assert_eq!(got.len(), MAX_FILE_BYTES as usize);
    }

    #[test]
    fn read_file_sums_extent_lengths_rather_than_combining_them_some_other_way() {
        // The running-total guard stops many legal extents adding up to a GiB
        // allocation; it must compare the SUM read against the ceiling, or two
        // legal 16 MiB extents (32 MiB total, well within bounds) get refused.
        const EXT: u32 = 16 * 1024 * 1024;
        let mut reader = MapReader::new();
        reader.put(
            5,
            build_efe(2 * EXT as u64, &[(0, EXT, 100), (0, EXT, 20_000)]),
        );
        let fs = fs_with_file(5, 2 * EXT as u64);

        let got = fs
            .read_file(&mut reader, "/F")
            .expect("32 MiB across two extents is under the 64 MiB ceiling");
        assert_eq!(got.len(), 2 * EXT as usize);
    }

    #[test]
    fn read_inline_data_rejects_oversized_lea() {
        // AD type=3 with L_EA so large that ad_offset = 216 + L_EA overflows past
        // the 2048-byte ICB. Before the fix `.min(icb.len())` clamped to
        // start==end==2048, silently returning an empty file (e.g. a 0-byte key).
        let mut icb = [0u8; 2048];
        // tag = 266 (Extended File Entry)
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        // ICB Tag flags at offset 34: low 3 bits = 3 → inline data
        icb[34..36].copy_from_slice(&3u16.to_le_bytes());
        // L_EA = 2000 → ad_offset = 216 + 2000 = 2216 > 2048
        let l_ea: u32 = 2000;
        let l_ad: u32 = 4;
        icb[208..212].copy_from_slice(&l_ea.to_le_bytes());
        icb[212..216].copy_from_slice(&l_ad.to_le_bytes());

        let mut reader = MapReader::new();
        reader.put(0, icb); // meta_start=0 + meta_lba=0 → abs lba 0

        let fs = fs_with(0, 0, file_entry("inline", 0, l_ad as u64));
        let result = fs.read_inline_data(&mut reader, 0);
        assert!(
            result.is_err(),
            "oversized L_EA must return Err, not Ok(Some(empty vec))"
        );
    }

    /// A `SectorSource` that gives every sector a content derived from its
    /// own LBA and counts how many read commands it is issued. Both matter:
    /// the content says WHICH sector a caller actually got, the count says
    /// whether the cache served it without touching the media.
    struct CountReader {
        calls: usize,
        sectors_read: usize,
    }

    impl CountReader {
        fn new() -> Self {
            Self {
                calls: 0,
                sectors_read: 0,
            }
        }
        /// Sector `lba` is filled with a byte pattern unique to `lba`.
        fn expected(lba: u32) -> [u8; 2048] {
            let mut s = [0u8; 2048];
            s[0..4].copy_from_slice(&lba.to_le_bytes());
            s[2044..2048].copy_from_slice(&(!lba).to_le_bytes());
            s
        }
    }

    impl SectorSource for CountReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let need = count as usize * 2048;
            if buf.len() < need {
                return Err(Error::UdfBufferTooSmall);
            }
            self.calls += 1;
            self.sectors_read += count as usize;
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                buf[off..off + 2048].copy_from_slice(&Self::expected(lba.wrapping_add(i)));
            }
            Ok(need)
        }
    }

    #[test]
    fn prefetch_serves_every_sector_of_the_window_from_the_cache() {
        // `prefetch` turns thousands of single-sector metadata reads into a few
        // batched commands, but only if the window is actually loaded, placed so
        // each sector serves from its own offset, and consulted afterwards.
        let mut inner = CountReader::new();
        {
            let mut br = BufferedSectorReader::new(&mut inner, 3);
            br.prefetch(100, 8); // 8 sectors in batches of 3 → 3 commands
            let mut buf = [0u8; 2048];
            for lba in [100u32, 103, 107] {
                br.read_sectors(lba, 1, &mut buf, true)
                    .unwrap_or_else(|e| panic!("cached read of {lba}: {e:?}"));
                assert_eq!(
                    buf,
                    CountReader::expected(lba),
                    "sector {lba} came back as some other sector"
                );
            }
        }
        assert_eq!(
            inner.calls, 3,
            "the window is loaded in batch-sized commands and then answers reads without the drive"
        );
        assert_eq!(inner.sectors_read, 8);
    }

    #[test]
    fn prefetch_window_does_not_answer_for_the_sector_just_past_its_end() {
        // The window covers `count` sectors from `start_lba`; the sector at
        // start_lba + count belongs to whatever is next, and answering for it
        // would hand back bytes from beyond the loaded data.
        let mut inner = CountReader::new();
        let mut br = BufferedSectorReader::new(&mut inner, 8);
        br.prefetch(100, 8);
        let mut buf = [0u8; 2048];
        br.read_sectors(108, 1, &mut buf, true)
            .expect("the sector past the window is read from the drive");
        assert_eq!(buf, CountReader::expected(108));
    }

    #[test]
    fn prefetch_ranges_serves_every_sector_of_every_range_from_the_permanent_cache() {
        // `prefetch_ranges` seeds the per-sector cache from the scattered ranges
        // `metadata_sector_ranges` produced; each sector must be keyed at its
        // own LBA, or a wrong-base key answers unrelated later reads (e.g. AVDP/VDS).
        let mut inner = CountReader::new();
        {
            let mut br = BufferedSectorReader::new(&mut inner, 2);
            br.prefetch_ranges(&[(200, 5), (300, 2)]);
            let mut buf = [0u8; 2048];
            for lba in [200u32, 202, 204, 300, 301] {
                br.read_sectors(lba, 1, &mut buf, true)
                    .unwrap_or_else(|e| panic!("cached read of {lba}: {e:?}"));
                assert_eq!(
                    buf,
                    CountReader::expected(lba),
                    "sector {lba} came back as some other sector"
                );
            }
        }
        // 5 sectors in batches of 2 → 3 commands; 2 sectors → 1 command. The
        // five reads above add none.
        assert_eq!(inner.calls, 4);
        assert_eq!(inner.sectors_read, 7);
    }

    #[test]
    fn metadata_sector_ranges_covers_the_structure_and_each_small_files_extents() {
        // The prefetch plan decides what's bulk-read before scanning: UDF
        // structure, every non-STREAM ICB, RECORDED extents under the size cap,
        // but never STREAM payloads. A wrong plan only costs perf, never correctness.
        const PART: u32 = 1000;
        let fs = UdfFs {
            root: DirEntry {
                name: String::new(),
                is_dir: true,
                meta_lba: 0,
                size: 0,
                entries: vec![
                    DirEntry {
                        name: "BDMV".to_string(),
                        is_dir: true,
                        meta_lba: 1,
                        size: 0,
                        entries: vec![file_entry("INDEX.BDMV", 10, 4096)],
                    },
                    DirEntry {
                        name: "STREAM".to_string(),
                        is_dir: true,
                        meta_lba: 2,
                        size: 0,
                        entries: vec![file_entry("00000.M2TS", 11, 2048)],
                    },
                    // Exactly at the 50 MB ceiling — cacheable.
                    file_entry("EXACT.BIN", 12, 50_000_000),
                    // One byte over — its data is not cached, but its ICB is.
                    file_entry("HUGE.BIN", 13, 50_000_001),
                    file_entry("SPARSE.BIN", 14, 6144),
                ],
            },
            volume_id: String::new(),
            partition_start: PART,
            metadata_start: PART,
            metadata_sectors: 4,
        };

        let mut reader = MapReader::new();
        reader.put(PART + 10, build_efe(4096, &[(0, 4096, 500)]));
        reader.put(PART + 11, build_efe(2048, &[(0, 2048, 600)])); // STREAM: never read
        reader.put(PART + 12, build_efe(50_000_000, &[(0, 2048, 800)]));
        reader.put(PART + 13, build_efe(50_000_001, &[(0, 2048, 850)]));
        // An unrecorded extent holds nothing to cache; the recorded one does.
        reader.put(
            PART + 14,
            build_efe(6144, &[(1, 4096, 700), (0, 2048, 900)]),
        );

        let ranges = fs
            .metadata_sector_ranges(&mut reader)
            .expect("the plan is built");

        // (1010,1) is INDEX.BDMV's ICB alone, stopping short of 1011 (STREAM's
        // ICB, deliberately never descended) rather than merging across it; this
        // used to read (1010,5), a regression where `merge_ranges` closed that gap.
        assert_eq!(
            ranges,
            vec![
                (0, 1004),
                (1010, 1),
                (1012, 3),
                (1500, 2),
                (1800, 1),
                (1900, 1)
            ]
        );
        assert!(
            !ranges.iter().any(|&(s, c)| (s..s + c).contains(&1011)),
            "STREAM's ICB at 1011 is deliberately not descended, so no range \
             may cover it: {ranges:?}"
        );
    }

    #[test]
    fn prefetch_huge_count_is_capped() {
        // A disc-controlled sector count far exceeding the 8192-sector cap must
        // not allocate more than 8192 * 2048 bytes in the sliding cache.
        let mut inner = MapReader::new();
        let mut br = BufferedSectorReader::new(&mut inner, 32);
        // Pass a count that would allocate ~512 MiB if uncapped (262144 sectors).
        br.prefetch(0, 262_144);
        // The cache must be no larger than the cap: 8192 sectors × 2048 bytes.
        assert!(
            br.cache.len() <= 8192 * 2048,
            "prefetch cache exceeded cap: {} bytes",
            br.cache.len()
        );
    }

    /// `prefetch` advances the read LBA with `start_lba + offset`. A UDF whose
    /// metadata descriptor declares a partition near the top of the LBA space
    /// (ECMA-167 §14.1: `extent_location` is an unconstrained Uint32) drove that
    /// add past `u32::MAX` — an 'attempt to add with overflow' panic in debug
    /// inside the public `Disc::scan`, and in release a wrap to a low LBA that
    /// filled the sliding cache with a completely different region while
    /// `cache_start` still claimed the high one.
    #[test]
    fn prefetch_near_u32_max_does_not_overflow() {
        let mut inner = MapReader::new();
        let mut br = BufferedSectorReader::new(&mut inner, 60);
        // start + count = 0xFFFF_FFC0 + 200 > u32::MAX: the second batch
        // iteration evaluates 0xFFFF_FFC0 + 60.
        br.prefetch(0xFFFF_FFC0, 200);
        // Nothing read (MapReader serves no sector here), but crucially the
        // walk must never form an LBA above u32::MAX.
        assert!(
            br.cache_start.checked_add(br.cache_sectors).is_some(),
            "cache window must stay inside the u32 LBA space: {} + {}",
            br.cache_start,
            br.cache_sectors
        );
    }

    /// `prefetch_ranges` walks each disc-derived `(start_lba, sector_count)`
    /// range with `start + offset + i`. `collect_file_ranges` permits any LBA up
    /// to `u32::MAX` (ECMA-167 §14.14.1 allocation descriptors are unconstrained
    /// Uint32s) and nothing bounds `start + count`, so a range within one batch
    /// of the top of the space overflowed: debug panic inside `Disc::scan`, or in
    /// release a wrap that seeded the PERMANENT cache with this file's bytes keyed
    /// at LBA 0 — every later single-sector read of those low LBAs (the AVDP/VDS
    /// re-reads) then returned the wrong sector.
    #[test]
    fn prefetch_ranges_near_u32_max_does_not_overflow() {
        let mut inner = MapReader::new();
        let mut br = BufferedSectorReader::new(&mut inner, 60);
        br.prefetch_ranges(&[(0xFFFF_FFF0, 512)]);
        // Every key the permanent cache holds must be a real LBA, i.e. inside
        // the declared range — never a wrapped low sector.
        for &lba in br.prefetched.keys() {
            assert!(
                lba >= 0xFFFF_FFF0,
                "prefetch_ranges wrapped a near-u32::MAX LBA to {lba}"
            );
        }
        // Clamping must drop only unaddressable sectors — dropping addressable
        // ones too silently defeats the bulk read this function exists for.
        for lba in 0xFFFF_FFF0u32..=0xFFFF_FFFE {
            assert!(
                br.prefetched.contains_key(&lba),
                "addressable sector {lba} was dropped from the prefetch"
            );
        }
    }

    /// The sliding-cache hit test computed `cache_start + cache_sectors`. Once
    /// `cache_start` is a disc-controlled LBA near `u32::MAX` (set by the batch
    /// read below) that add overflowed: debug panic inside
    /// `SectorSource::read_sectors`, release wrap to a small value that silently
    /// disabled the cache.
    #[test]
    fn cache_hit_test_near_u32_max_does_not_overflow() {
        let mut inner = MapReader::new();
        let mut br = BufferedSectorReader::new(&mut inner, 60);
        let mut buf = [0u8; 2048];
        // First read seeds cache_start = 0xFFFF_FFF5, cache_sectors = 60.
        br.read_sectors(0xFFFF_FFF5, 1, &mut buf, true)
            .expect("seed read");
        // Second read of the same LBA evaluates the hit test: 0xFFFF_FFF5 + 60.
        br.read_sectors(0xFFFF_FFF5, 1, &mut buf, true)
            .expect("cache hit test must not overflow");
    }

    /// Build a 2048-byte directory sector containing `count` minimal file FIDs.
    ///
    /// Each FID uses a 2-byte name (compression-id `8` + `b'A'`), so l_fi=2
    /// and the total FID record is 40 bytes (already 4-byte aligned).
    /// A 2048-byte sector fits exactly 51 such FIDs.
    ///
    /// `icb_base` is the ICB LBA written into the first FID; each subsequent
    /// FID gets `icb_base + i`.
    fn build_dir_sector_with_file_fids(count: usize, icb_base: u32) -> [u8; 2048] {
        let mut sector = [0u8; 2048];
        let l_fi: u8 = 2;
        let name: [u8; 2] = [8, b'A'];
        let fid_stride = 40usize; // 38 + l_fi=2, already 4-byte aligned
        let mut pos = 0;
        for i in 0..count {
            if pos + fid_stride > sector.len() {
                break;
            }
            sector[pos..pos + 2].copy_from_slice(&257u16.to_le_bytes()); // FID tag
            sector[pos + 18] = 0x00; // file (not dir, not parent)
            sector[pos + 19] = l_fi;
            let lba = icb_base.wrapping_add(i as u32);
            sector[pos + 24..pos + 28].copy_from_slice(&lba.to_le_bytes());
            // l_iu = 0 at +36
            sector[pos + 38..pos + 40].copy_from_slice(&name);
            pos += fid_stride;
        }
        sector
    }

    #[test]
    fn read_directory_budget_exceeded_returns_err() {
        // A crafted disc emitting more FIDs than MAX_TOTAL_DIR_ENTRIES must be
        // rejected, not visited indefinitely: pre-load `budget` to within 10 of
        // the cap and feed 51 FIDs — the walk must error before consuming them all.
        let dir_sector = build_dir_sector_with_file_fids(51, 200);
        let dir_icb = build_efe_icb(2048, 2048, 50);
        let mut reader = MemReader::new();
        reader.put(5, dir_icb);
        reader.put(50, dir_sector);
        // MemReader returns zeros for unmapped ICB LBAs → tag=0 → read_file_size=0, fine.

        let mut budget: usize = MAX_TOTAL_DIR_ENTRIES - 10;
        let err = read_directory(
            &mut reader,
            0,
            0,
            5,
            "ROOT",
            0,
            &mut budget,
            &mut HashSet::new(),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::DiscRead { .. }),
            "budget exceeded must return DiscRead"
        );
    }

    #[test]
    fn read_directory_icb_cycle_does_not_recurse() {
        // A child FID pointing back at the parent's own ICB LBA (self-reference)
        // must NOT recurse — it must be emitted as a leaf. `visited` is seeded
        // with lba 5 (the root about to be descended into) so the cycle is caught.
        let mut dir = [0u8; 2048];
        let mut name_bytes = vec![8u8];
        name_bytes.extend_from_slice(b"LOOP");
        let l_fi = name_bytes.len() as u8;
        dir[0..2].copy_from_slice(&257u16.to_le_bytes()); // FID tag
        dir[18] = 0x02; // is_dir
        dir[19] = l_fi;
        dir[24..28].copy_from_slice(&5u32.to_le_bytes()); // ICB LBA = 5 (self)
        // l_iu = 0, name at offset 38
        dir[38..38 + name_bytes.len()].copy_from_slice(&name_bytes);

        let dir_icb = build_efe_icb(2048, 2048, 60);
        let mut reader = MemReader::new();
        reader.put(5, dir_icb);
        reader.put(60, dir);

        // Seed visited with the root ICB key so the child (lba 5) is
        // immediately recognised as a cycle.
        let mut visited: HashSet<u64> = HashSet::new();
        let root_key: u64 = 5u64; // meta_start=0 → key = (0 << 32) | 5
        visited.insert(root_key);

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut visited)
            .expect("cycle must not blow up");

        // The cyclic entry is emitted as a leaf (no children), not recursed into.
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].name, "LOOP");
        assert!(parsed.entries[0].is_dir);
        assert!(
            parsed.entries[0].entries.is_empty(),
            "cycle entry must be a leaf, not recursed"
        );
    }

    #[test]
    fn read_directory_rejects_an_icb_whose_tag_is_not_a_file_entry() {
        // Only tags 261/266 (ECMA-167 4/14.9, 4/14.17) can be a directory's ICB;
        // any other tag (here 258, a real but wrong descriptor) leaves contents
        // UNKNOWN — an empty DirEntry would make it look genuinely empty.
        let mut icb = build_efe_icb(2048, 2048, 60);
        icb[0..2].copy_from_slice(&258u16.to_le_bytes());
        let mut reader = MemReader::new();
        reader.put(5, icb);

        let err = read_directory(&mut reader, 0, 0, 5, "BDMV", 0, &mut 0, &mut HashSet::new())
            .expect_err("a non-File-Entry directory ICB must fail, not read as an empty directory");
        assert!(
            matches!(err, Error::DiscRead { sector: 5, .. }),
            "the error must name the ICB sector that carried the bad tag, got {err:?}"
        );

        // And the genuinely-empty case still succeeds: a real Extended File
        // Entry (266) whose directory data holds no FID is Ok and empty, so
        // this test is about the TAG and not about emptiness.
        let mut ok_reader = MemReader::new();
        ok_reader.put(5, build_efe_icb(2048, 2048, 60));
        let dir = read_directory(
            &mut ok_reader,
            0,
            0,
            5,
            "BDMV",
            0,
            &mut 0,
            &mut HashSet::new(),
        )
        .expect("a real Extended File Entry with no FIDs is a genuinely empty directory");
        assert!(dir.entries.is_empty());
    }

    #[test]
    fn read_file_unrecorded_extent_contributes_zeros_and_does_not_shift_later_extents() {
        // ECMA-167 4/14.14.1.1: type 1 = "allocated but not recorded" — logical
        // zeros that STILL OCCUPY file byte space. Dropping the descriptor slides
        // later extents down by the hole's length with no error at all.
        let icb = build_efe(
            3 * 2048,
            &[
                (0, 2048, 10), // recorded
                (1, 2048, 20), // allocated but NOT recorded
                (0, 2048, 30), // recorded, after the hole
            ],
        );
        let mut reader = MemReader::new();
        reader.put(5, icb);
        reader.put(10, [0xAAu8; 2048]);
        // LBA 20 is deliberately given NON-zero bytes: an unrecorded extent's
        // sectors must never be read, so 0xBB must not appear in the output.
        reader.put(20, [0xBBu8; 2048]);
        reader.put(30, [0xCCu8; 2048]);

        let fs = fs_with_file(5, 3 * 2048);
        let data = fs.read_file(&mut reader, "/F").expect("file reads");

        assert_eq!(data.len(), 6144, "the hole occupies file space");
        assert!(data[0..2048].iter().all(|&b| b == 0xAA));
        assert!(
            data[2048..4096].iter().all(|&b| b == 0x00),
            "an unrecorded extent reads as zeros, not as whatever is on the media"
        );
        assert!(
            data[4096..6144].iter().all(|&b| b == 0xCC),
            "the extent after the hole must land at byte 4096, not 2048"
        );
    }

    #[test]
    fn read_filesystem_follows_the_avdp_main_vds_extent_pointer() {
        // ECMA-167 3/10.2.1: the AVDP DEFINES the Main VDS via its extent_ad
        // (length at [16:20], location at [20:24]) — not fixed at sector 32.
        // A conformant volume recording its VDS elsewhere must still mount.
        use fixture::{DirSpec, MemDisc, PART_START, build_udf_skeleton, file, lay_dir};

        // Where this volume really keeps its Volume Descriptor Sequence.
        const VDS_LBA: u32 = 300;

        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(
            &mut disc,
            &DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: vec![file("INDEX.BDMV", 12, 13, 2048, false)],
                subdirs: Vec::new(),
            },
        );

        // Re-point the anchor at the real sequence and MOVE the descriptors
        // there, leaving 32..64 blank (as a conformant volume that never used
        // sector 32 would).
        let mut avdp = vec![0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        // 16 sectors is the ECMA-167 3/10.2.1 minimum VDS extent length.
        avdp[16..20].copy_from_slice(&(16u32 * 2048).to_le_bytes());
        avdp[20..24].copy_from_slice(&VDS_LBA.to_le_bytes());
        disc.put_bytes(256, &avdp);

        let mut pd = vec![0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        let mut lvd = vec![0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        let mut td = vec![0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put_bytes(VDS_LBA, &pd);
        disc.put_bytes(VDS_LBA + 1, &lvd);
        disc.put_bytes(VDS_LBA + 2, &td);
        // Blank the hardcoded window.
        disc.put_bytes(32, &vec![0u8; 3 * 2048]);

        let fs = super::read_filesystem(&mut disc)
            .expect("a volume whose AVDP points its VDS elsewhere must still mount");
        assert_eq!(fs.partition_start(), PART_START);
        assert_eq!(fs.root.entries.len(), 1);
        assert_eq!(fs.root.entries[0].name, "INDEX.BDMV");
    }

    /// An anchor whose recorded VDS extent passes every SHAPE check but holds
    /// no sequence must still mount, by sweeping the customary location.
    ///
    /// Length >= 16 sectors, non-zero location, no address wrap — all three
    /// hold, so the shape test selects the recorded extent and never looks
    /// again. But shape is a property of the FIELD, not of what is there: a
    /// mastering tool that wrote the reserve location, a stale anchor on a
    /// rewritten volume, or deliberate corruption all produce this. Selecting
    /// the fallback on shape alone means the branch a damaged disc actually
    /// takes has no recovery path at all.
    ///
    /// The sibling Metadata File Location chain in this same function already
    /// treats its recorded value as a CANDIDATE and falls back on outcome.
    /// This is that principle applied one level up.
    #[test]
    fn a_shape_valid_vds_pointer_that_holds_no_sequence_falls_back_to_the_customary_location() {
        use fixture::{DirSpec, MemDisc, PART_START, build_udf_skeleton, file, lay_dir};

        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(
            &mut disc,
            &DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: vec![file("INDEX.BDMV", 12, 13, 2048, false)],
                subdirs: Vec::new(),
            },
        );

        // Point the anchor at LBA 500 with a well-formed extent_ad but nothing written
        // there, so the sweep reads zeroed sectors (tag 0, no Partition Descriptor).
        // The real sequence stays at the customary location for the fallback to find.
        let mut avdp = vec![0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        avdp[16..20].copy_from_slice(&(16u32 * 2048).to_le_bytes());
        avdp[20..24].copy_from_slice(&500u32.to_le_bytes());
        disc.put_bytes(256, &avdp);

        let fs = super::read_filesystem(&mut disc).expect(
            "a well-formed pointer to nothing must not be the end of the road; \
             the customary sequence is still there and still valid",
        );
        assert_eq!(fs.partition_start(), PART_START);
        assert_eq!(fs.root.entries[0].name, "INDEX.BDMV");
    }

    /// The recorded extent and the customary fallback are two SWEEPS, not two
    /// locations: an anchor may point at the customary LBA and still declare a
    /// window narrower than the fallback's. De-duplicating them on start LBA
    /// alone then throws the wider sweep away and the sequence is never
    /// reached, even though it is exactly where the fallback would have looked.
    ///
    /// Fixture: the anchor records `(LBA 32, 16 sectors)` — the minimum
    /// ECMA-167 3/10.2.1 permits, so it passes every shape check — while the
    /// Partition and Logical Volume Descriptors sit at 50/51, inside the
    /// customary 32-sector window but past the declared 16. No Terminating
    /// Descriptor lies in 32..48, or both sweeps would stop at the same sector
    /// and the wider one would buy nothing.
    #[test]
    fn a_recorded_vds_narrower_than_the_customary_window_still_gets_the_wider_sweep() {
        use fixture::{DirSpec, MemDisc, PART_START, build_udf_skeleton, file, lay_dir};

        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(
            &mut disc,
            &DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: vec![file("INDEX.BDMV", 12, 13, 2048, false)],
                subdirs: Vec::new(),
            },
        );

        // Move the sequence from 32/33/34 to 50/51, past the declared 16-sector window.
        // 32..48 is left zeroed (tag 0, no descriptor), crucially with no tag-8
        // Terminating Descriptor to stop the wider sweep early.
        disc.put_bytes(32, &[0u8; 3 * 2048]);
        let mut pd = [0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put_bytes(50, &pd);
        let mut lvd = [0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        disc.put_bytes(51, &lvd);

        let mut avdp = vec![0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        avdp[16..20].copy_from_slice(&(VDS_MIN_SECTORS * 2048).to_le_bytes());
        avdp[20..24].copy_from_slice(&VDS_FALLBACK_START.to_le_bytes());
        disc.put_bytes(256, &avdp);

        let fs = super::read_filesystem(&mut disc).expect(
            "an anchor whose window is narrower than the customary one must \
             still get the customary sweep — it is a different sweep, not a \
             duplicate of the one already tried",
        );
        assert_eq!(fs.partition_start(), PART_START);
        assert_eq!(fs.root.entries[0].name, "INDEX.BDMV");
    }

    // ---- UDF 2.50 Metadata Partition coverage. Every BD-ROM's LVD carries TWO
    // partition maps (Type 1 physical + Type 2 metadata, UDF 2.50 2.2.10); the tree
    // lives in a Metadata File whose FE sits in the physical partition — untested.

    /// How one synthetic UDF 2.50 metadata-partition volume is laid out.
    /// Every field a disc controls is a knob so a single builder can produce
    /// the conformant case and each malformed variant.
    struct MetaVol {
        /// Map type byte of the first partition map (1 = physical).
        pm1_type: u8,
        /// Length byte of the Type 1 physical partition map (real value: 6).
        /// The Type 2 map begins at 440 + this.
        pm1_len: u8,
        /// Map type byte of the second partition map (2 = Type 2).
        pm2_type: u8,
        /// The Type 2 map's partition type identifier (UDF 2.50 2.2.10).
        ident: &'static [u8],
        /// UDF 2.50 2.2.10 Metadata File Location: the partition-relative
        /// block recorded in the map as holding the Metadata File's FE.
        meta_file_loc: u32,
        /// Where the Metadata File's File Entry is ACTUALLY written
        /// (partition-relative). Equal to `meta_file_loc` on a sane volume.
        meta_fe_at: u32,
        /// L_EA of that File Entry — its allocation descriptor starts at
        /// 216 + L_EA, not at 216.
        l_ea: usize,
        /// The Metadata File's single extent: byte length and its
        /// partition-relative LBA. Together these define the metadata
        /// partition, i.e. `metadata_start` and `metadata_sectors`.
        meta_bytes: u32,
        meta_pos: u32,
        /// A second, DIFFERENT Metadata-File-shaped File Entry planted at this
        /// partition-relative block. Following the wrong one yields a
        /// different metadata partition, so a reader that picks it up is
        /// caught by `metadata_start`, not merely by an error.
        decoy_meta_fe: Option<u32>,
    }

    /// A conformant UDF 2.50 metadata-partition volume, as a BD-ROM records
    /// it, with the Metadata File FE where the partition map says it is.
    ///
    /// The extent length and position are given distinct byte patterns, and
    /// distinct patterns from one another, so a descriptor byte sourced from
    /// the wrong offset — or the two fields transposed — changes the reported
    /// metadata partition rather than landing on a value that happens to
    /// work. The length is an exact multiple of the 2048-byte logical sector,
    /// as a real metadata partition is: its only observable is a sector
    /// COUNT, which rounds up, so an off-by-one byte in the low half is
    /// visible only when the true value sits exactly on the boundary.
    fn conformant_meta_vol() -> MetaVol {
        MetaVol {
            pm1_type: 1,
            pm1_len: 6,
            pm2_type: 2,
            ident: b"*UDF Metadata Partition",
            meta_file_loc: 9,
            meta_fe_at: 9,
            l_ea: 8,
            meta_bytes: 0x0102_0800, // 16 910 336 bytes == exactly 8257 sectors
            meta_pos: 0x0203_0405,
            decoy_meta_fe: None,
        }
    }

    /// Metadata-partition-relative LBAs of the tree inside the Metadata File.
    const MV_ROOT_ICB: u32 = 3;
    const MV_ROOT_DATA: u32 = 4;
    const MV_FILE_ICB: u32 = 5;
    const MV_VOLUME_ID: &str = "FREEMKV-META";
    const MV_FILE_SIZE: u64 = 100;

    /// Partition-relative LBAs of a SECOND, decoy tree rooted at block 0 of
    /// the physical partition — what a reader sees if it never resolves the
    /// metadata partition and treats the physical partition as the file set.
    /// Its file has a different name and a different size from the metadata
    /// partition's, so "fell back to the physical partition" and "read the
    /// metadata partition" are told apart by CONTENT, not by an error code.
    const MV_FB_ROOT_ICB: u32 = 40;
    const MV_FB_ROOT_DATA: u32 = 41;
    const MV_FB_FILE_ICB: u32 = 42;
    const MV_FB_FILE_SIZE: u64 = 777;

    /// Lay out `spec` as a complete disc image. Also returns the absolute LBA
    /// the metadata partition starts at, which is what `metadata_start()`
    /// must come out as when the volume is conformant.
    fn build_meta_vol(spec: &MetaVol) -> (fixture::MemDisc, u32) {
        use fixture::{MemDisc, PART_START};
        let mut disc = MemDisc::new();

        // Anchor Volume Descriptor Pointer with no usable extent → the
        // customary 32.. window is swept (exercised on its own elsewhere).
        let mut avdp = vec![0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put_bytes(256, &avdp);

        // Primary Volume Descriptor: Volume Identifier is a 32-byte d-string
        // (ECMA-167 1/7.2.12) at offset 24 — compression id, characters, and
        // the used length in the LAST byte of the field.
        let mut pvd = vec![0u8; 2048];
        pvd[0..2].copy_from_slice(&1u16.to_le_bytes());
        pvd[24] = 8; // OSTA CS0 compression id 8 (8-bit characters)
        pvd[25..25 + MV_VOLUME_ID.len()].copy_from_slice(MV_VOLUME_ID.as_bytes());
        pvd[55] = 1 + MV_VOLUME_ID.len() as u8;
        disc.put_bytes(32, &pvd);

        let mut pd = vec![0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put_bytes(33, &pd);

        // Logical Volume Descriptor with two partition maps (ECMA-167 3/10.6:
        // number of maps at 268, maps themselves from 440; 3/10.7 gives each
        // map a type byte then a length byte).
        let mut lvd = vec![0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&2u32.to_le_bytes());
        // Second map first, so a spec whose first map has length 0 puts both
        // maps in the same slot exactly as a corrupt descriptor would.
        let map2 = 440 + spec.pm1_len as usize;
        lvd[map2] = spec.pm2_type;
        lvd[map2 + 1] = 64; // UDF 2.50 2.2.10: the map is 64 bytes
        // EntityID (ECMA-167 1/7.4): flags byte then 23 identifier chars.
        lvd[map2 + 5..map2 + 5 + spec.ident.len()].copy_from_slice(spec.ident);
        lvd[map2 + 40..map2 + 44].copy_from_slice(&spec.meta_file_loc.to_le_bytes());
        lvd[440] = spec.pm1_type;
        lvd[441] = spec.pm1_len;
        disc.put_bytes(34, &lvd);

        let mut td = vec![0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put_bytes(35, &td);

        // The Metadata File's own FE, in the PHYSICAL partition — its single allocation
        // descriptor defines the whole metadata partition. The EA area is filled with a
        // recognisable pattern so a descriptor read from the wrong offset can't be usable.
        let mut meta_fe = vec![0u8; 2048];
        meta_fe[0..2].copy_from_slice(&266u16.to_le_bytes());
        meta_fe[208..212].copy_from_slice(&(spec.l_ea as u32).to_le_bytes());
        meta_fe[212..216].copy_from_slice(&8u32.to_le_bytes()); // L_AD: one short_ad
        meta_fe[216..216 + spec.l_ea].fill(0xA5);
        let ad = 216 + spec.l_ea;
        // A spec whose L_EA pushes the allocation descriptor off the end of
        // the File Entry records no descriptor at all — that is the point of
        // such a spec.
        if ad + 8 <= meta_fe.len() {
            meta_fe[ad..ad + 4].copy_from_slice(&spec.meta_bytes.to_le_bytes());
            meta_fe[ad + 4..ad + 8].copy_from_slice(&spec.meta_pos.to_le_bytes());
        }
        disc.put_bytes(PART_START + spec.meta_fe_at, &meta_fe);

        if let Some(block) = spec.decoy_meta_fe {
            let mut decoy = vec![0u8; 2048];
            decoy[0..2].copy_from_slice(&266u16.to_le_bytes());
            decoy[212..216].copy_from_slice(&8u32.to_le_bytes());
            decoy[216..220].copy_from_slice(&0x0011_2233u32.to_le_bytes());
            decoy[220..224].copy_from_slice(&(spec.meta_pos.wrapping_add(1000)).to_le_bytes());
            disc.put_bytes(PART_START + block, &decoy);
        }

        // Decoy file set at block 0, laid only when the Metadata File's FE isn't there.
        // A failed metadata-partition resolution falls back to `partition_start`, where
        // this tree mounts — turning a silent skip into a plausible, WRONG filesystem.
        if spec.meta_fe_at != 0 {
            let mut fb_fsd = vec![0u8; 2048];
            fb_fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
            fb_fsd[404..408].copy_from_slice(&MV_FB_ROOT_ICB.to_le_bytes());
            disc.put_bytes(PART_START, &fb_fsd);

            let mut fb_fids = Vec::new();
            push_fid_iu(&mut fb_fids, "", MV_FB_ROOT_ICB, true, true, 0);
            push_fid_iu(
                &mut fb_fids,
                "FALLBACK.BDMV",
                MV_FB_FILE_ICB,
                false,
                false,
                0,
            );
            disc.put_bytes(
                PART_START + MV_FB_ROOT_ICB,
                &build_dir_icb_tagged(266, 0, fb_fids.len() as u32, MV_FB_ROOT_DATA),
            );
            disc.put_bytes(PART_START + MV_FB_ROOT_DATA, &fb_fids);
            disc.put_bytes(
                PART_START + MV_FB_FILE_ICB,
                &build_efe_icb(MV_FB_FILE_SIZE, MV_FB_FILE_SIZE as u32, 60),
            );
        }

        // Inside the metadata partition: File Set Descriptor, root directory
        // ICB, its FID list, and one file's ICB.
        let meta_start = PART_START + spec.meta_pos;

        let mut fsd = vec![0u8; 2048];
        fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
        fsd[404..408].copy_from_slice(&MV_ROOT_ICB.to_le_bytes());
        disc.put_bytes(meta_start, &fsd);

        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", MV_ROOT_ICB, true, true, 0);
        push_fid_iu(&mut fids, "INDEX.BDMV", MV_FILE_ICB, false, false, 0);
        disc.put_bytes(
            meta_start + MV_ROOT_ICB,
            &build_dir_icb_tagged(266, 0, fids.len() as u32, MV_ROOT_DATA),
        );
        disc.put_bytes(meta_start + MV_ROOT_DATA, &fids);
        disc.put_bytes(
            meta_start + MV_FILE_ICB,
            &build_efe_icb(MV_FILE_SIZE, MV_FILE_SIZE as u32, 20),
        );

        (disc, meta_start)
    }

    /// A read fault while locating the Metadata File must surface as a read
    /// error, NOT as "this is not a UDF disc".
    ///
    /// The candidate chain tries the recorded Metadata File Location and then
    /// block 0. If neither can be READ — a marginal sector, a drive re-read, an
    /// ECC recovery that reports failure once — that is transient and
    /// retryable. If it were reported as a structural negative instead, the
    /// caller would get the deterministic verdict `UdfNotFilesystem`, and
    /// `mux::resolve` MEMOISES that negative for the whole disc: every
    /// remaining title would silently drop to the base-Unit-Key-only path, the
    /// AACS 2.1 forensic units would garble, and the mux would complete with
    /// less content and no error.
    ///
    /// This file draws the same distinction twice in prose already — at the
    /// AVDP check and the File Set Descriptor check — so the loop must not
    /// erase it.
    #[test]
    fn a_read_fault_locating_the_metadata_file_is_not_reported_as_a_non_udf_disc() {
        use fixture::PART_START;

        /// Wraps a built volume and fails every read of the two candidate LBAs.
        struct FailsMetaCandidates {
            inner: fixture::MemDisc,
            deny: Vec<u32>,
        }
        impl SectorSource for FailsMetaCandidates {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                recovery: bool,
            ) -> crate::error::Result<usize> {
                if (0..count as u32).any(|i| self.deny.contains(&(lba + i))) {
                    return Err(Error::DiscRead {
                        sector: lba as u64,
                        status: None,
                        sense: None,
                    });
                }
                self.inner.read_sectors(lba, count, buf, recovery)
            }
        }

        let spec = MetaVol {
            meta_file_loc: 9,
            meta_fe_at: 9,
            ..conformant_meta_vol()
        };
        let (disc, _meta_start) = build_meta_vol(&spec);

        // ONLY the recorded location faults; block 0 reads fine but holds Volume
        // Descriptor content, not a File Entry, so meta_tag is non-zero-not-266 — a
        // guard keyed on "nothing read" misses it. Denying both candidates wouldn't test this.
        let mut reader = FailsMetaCandidates {
            inner: disc,
            deny: vec![PART_START + 9],
        };

        match super::read_filesystem(&mut reader) {
            Err(Error::DiscRead { .. }) => {}
            Err(Error::UdfNotFilesystem) => panic!(
                "a transient read fault was reported as the deterministic verdict \
                 'not a UDF disc'; mux::resolve caches that for the whole disc"
            ),
            other => panic!("expected a read error, got {other:?}"),
        }
    }

    #[test]
    fn read_filesystem_locates_the_metadata_file_where_the_partition_map_records_it() {
        // UDF 2.50 2.2.10 Metadata Partition Map: Metadata File Location (Uint32 at
        // map offset 40) is the ONLY thing that says where the Metadata File's File
        // Entry lives; block 0 is common but not required, so it must mount elsewhere.
        use fixture::PART_START;
        let spec = MetaVol {
            meta_file_loc: 9,
            meta_fe_at: 9,
            ..conformant_meta_vol()
        };
        let (mut disc, meta_start) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc)
            .expect("a UDF 2.50 metadata partition whose File Entry is not at block 0 must mount");

        assert_eq!(fs.partition_start(), PART_START);
        assert_eq!(
            fs.metadata_start(),
            meta_start,
            "the metadata partition begins at the Metadata File's extent"
        );
        assert_eq!(
            spec.meta_bytes, 16_910_336,
            "the sector count below is computed from this byte length by hand"
        );
        assert_eq!(
            fs.metadata_sectors(),
            8257, // 16 910 336 / 2048, exactly
            "the metadata partition is as long as the Metadata File's extent"
        );
        assert_eq!(fs.volume_id, MV_VOLUME_ID);
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
        assert_eq!(fs.root.entries[0].size, MV_FILE_SIZE);
    }

    #[test]
    fn read_filesystem_counts_a_partial_final_metadata_sector() {
        // extent_length is a BYTE count (ECMA-167 4/14.14.1); metadata_sectors is a
        // sector COUNT, so a remainder must round UP or the final partly-used sector
        // (and its directory bytes) falls outside the range: 16910337 = 8257*2048+1 -> 8258.
        let spec = MetaVol {
            meta_bytes: 16_910_337,
            ..conformant_meta_vol()
        };
        let (mut disc, meta_start) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc)
            .expect("a metadata extent that is not a whole number of sectors must still mount");

        assert_eq!(fs.metadata_start(), meta_start);
        assert_eq!(
            fs.metadata_sectors(),
            8258,
            "the sector holding the final partial 1 byte belongs to the metadata partition"
        );
    }

    #[test]
    fn read_filesystem_still_finds_a_metadata_file_entry_recorded_at_block_zero() {
        // The overwhelmingly common layout: the Metadata File's File Entry is at block
        // 0 and the map's Metadata File Location says so. Honouring the recorded
        // location must not change this ordinary case.
        let spec = MetaVol {
            meta_file_loc: 0,
            meta_fe_at: 0,
            ..conformant_meta_vol()
        };
        let (mut disc, meta_start) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc).expect("the ordinary BD-ROM layout must mount");
        assert_eq!(fs.metadata_start(), meta_start);
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
    }

    #[test]
    fn read_filesystem_falls_back_to_block_zero_when_the_recorded_location_holds_no_file_entry() {
        // A Metadata File Location pointing at a block with no File Entry is broken,
        // not unmountable: the file is still discoverable at block 0. Trusting the
        // field blindly would refuse a disc that used to mount.
        let spec = MetaVol {
            meta_file_loc: 999, // nothing is recorded there
            meta_fe_at: 0,
            ..conformant_meta_vol()
        };
        let (mut disc, meta_start) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc)
            .expect("a wrong Metadata File Location must not lose a volume readable at block 0");
        assert_eq!(fs.metadata_start(), meta_start);
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
    }

    #[test]
    fn read_filesystem_reads_no_metadata_file_location_out_of_a_sparable_partition_map() {
        // UDF 2.2.9 Sparable / 2.2.8 Virtual maps are also Type 2 (ECMA-167 3/10.7.3)
        // but record unrelated fields where 2.2.10 puts the Metadata File Location —
        // misreading those bytes as a location would silently mount a decoy volume.
        let spec = MetaVol {
            ident: b"*UDF Sparable Partition",
            meta_file_loc: 9,
            decoy_meta_fe: Some(9),
            meta_fe_at: 0,
            ..conformant_meta_vol()
        };
        let (mut disc, meta_start) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc).expect(
            "a Type 2 map that is not a Metadata Partition Map must not misdirect the read",
        );
        assert_eq!(
            fs.metadata_start(),
            meta_start,
            "the Metadata File at block 0 defines the partition, not a byte read out of a sparable map"
        );
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
    }

    #[test]
    fn read_filesystem_reads_a_metadata_file_extent_that_ends_flush_with_the_file_entry() {
        // ECMA-167 4/14.17: allocation descriptors start at 216 + L_EA, and the EA area
        // may run right up to where the short_ad exactly fills the rest of the block.
        // That descriptor is still in-bounds and must be read, or the volume is lost.
        let spec = MetaVol {
            l_ea: 2048 - 216 - 8, // short_ad occupies the final 8 bytes
            ..conformant_meta_vol()
        };
        let (mut disc, meta_start) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc)
            .expect("an allocation descriptor flush with the end of the File Entry is in bounds");
        assert_eq!(fs.metadata_start(), meta_start);
        assert_eq!(fs.metadata_sectors(), 8257); // 16 910 336 / 2048, exactly
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
    }

    #[test]
    fn read_filesystem_refuses_a_metadata_file_whose_descriptor_runs_off_the_file_entry() {
        // L_EA is disc-controlled; if 216 + L_EA leaves under 8 bytes, reading the
        // short_ad anyway runs past the sector buffer. 1830 makes it STRADDLE the end
        // (216+1830+8==2054), defeating a bound check computed the other way round.
        let spec = MetaVol {
            l_ea: 1830,
            ..conformant_meta_vol()
        };
        let (mut disc, _) = build_meta_vol(&spec);

        let err = super::read_filesystem(&mut disc)
            .expect_err("a short_ad that does not fit inside the File Entry cannot be read");
        assert!(matches!(err, Error::DiscRead { .. }), "got {err:?}");
    }

    #[test]
    fn read_filesystem_ignores_a_zero_length_first_partition_map() {
        // ECMA-167 3/10.7 walks partition maps by each map's own length byte. A
        // length-0 first map can't advance the walk, so misreading the map at 440 as
        // the "second" one lets it hijack the mount onto a different, plausible fs.
        let spec = MetaVol {
            pm1_type: 2,
            pm1_len: 0,
            ..conformant_meta_vol()
        };
        let (mut disc, _) = build_meta_vol(&spec);

        let fs = super::read_filesystem(&mut disc)
            .expect("a zero-length partition map must not abort the mount");
        assert_eq!(fs.metadata_start(), fixture::PART_START);
        assert_eq!(child_names(&fs.root), vec!["FALLBACK.BDMV".to_string()]);
    }

    #[test]
    fn read_filesystem_seeds_the_cycle_guard_with_the_metadata_relative_root_key() {
        // The cycle guard keys on (metadata_start, ICB LBA), pre-seeded with root's key
        // so a self-referential FID is recognised. A wrongly composed key (e.g. ANDing
        // instead of ORing the halves) misses the seed, descending root twice.
        let spec = conformant_meta_vol();
        let (mut disc, meta_start) = build_meta_vol(&spec);

        // Re-lay the root's FID list with a directory entry pointing straight
        // back at the root ICB.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", MV_ROOT_ICB, true, true, 0);
        push_fid_iu(&mut fids, "LOOP", MV_ROOT_ICB, true, false, 0);
        disc.put_bytes(meta_start + MV_ROOT_DATA, &fids);

        let fs = super::read_filesystem(&mut disc).expect("a self-referential root must not hang");
        assert_eq!(child_names(&fs.root), vec!["LOOP".to_string()]);
        assert!(
            fs.root.entries[0].entries.is_empty(),
            "the root's own ICB is already visited, so LOOP is a leaf, not a second copy of the \
             root: got {:?}",
            child_names(&fs.root.entries[0])
        );
    }

    #[test]
    fn read_filesystem_stops_the_volume_descriptor_sweep_at_the_terminating_descriptor() {
        // ECMA-167 3/8.4.2: a Terminating Descriptor ends the VDS. Sectors past it are
        // not part of it — often a stale copy left by a rewritten volume — so reading on
        // lets a stale Partition Descriptor overwrite the live one and offset every LBA.
        use fixture::{DirSpec, MemDisc, PART_START, build_udf_skeleton, file, lay_dir};

        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(
            &mut disc,
            &DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: vec![file("INDEX.BDMV", 12, 13, 2048, false)],
                subdirs: Vec::new(),
            },
        );

        // Stale Partition Descriptor recorded after the Terminating
        // Descriptor the skeleton puts at sector 34.
        let mut stale_pd = vec![0u8; 2048];
        stale_pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        stale_pd[188..192].copy_from_slice(&(PART_START + 4096).to_le_bytes());
        disc.put_bytes(35, &stale_pd);

        let fs = super::read_filesystem(&mut disc)
            .expect("descriptors past the terminator must not be swept");
        assert_eq!(
            fs.partition_start(),
            PART_START,
            "the live Partition Descriptor is the one before the Terminating Descriptor"
        );
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
    }

    #[test]
    fn read_filesystem_ignores_an_anchor_whose_main_vds_extent_starts_at_block_zero() {
        // ECMA-167 3/10.2.1: block 0 is the reserved area, never a Volume Descriptor
        // Sequence, so an anchor pointing there yields no Partition Descriptor and the
        // disc looks unmountable — the customary window is the right fallback.
        use fixture::{DirSpec, MemDisc, PART_START, build_udf_skeleton, file, lay_dir};

        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(
            &mut disc,
            &DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: vec![file("INDEX.BDMV", 12, 13, 2048, false)],
                subdirs: Vec::new(),
            },
        );

        let mut avdp = vec![0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        // A length that satisfies the 16-sector minimum, at block 0.
        avdp[16..20].copy_from_slice(&(VDS_MIN_SECTORS * 2048).to_le_bytes());
        avdp[20..24].copy_from_slice(&0u32.to_le_bytes());
        disc.put_bytes(256, &avdp);

        let fs = super::read_filesystem(&mut disc)
            .expect("an anchor pointing its VDS at block 0 must fall back, not fail the mount");
        assert_eq!(fs.partition_start(), PART_START);
        assert_eq!(child_names(&fs.root), vec!["INDEX.BDMV".to_string()]);
    }

    // ---- directory-descriptor boundary coverage (ECMA-167 4/14.4, 4/14.9, 4/14.17).
    // Every field below is disc-controlled, so each boundary can push the parser off
    // the end of a buffer or turn a failure into a plausible-looking success.

    /// Build a directory ICB with an explicit descriptor tag, extended
    /// attribute length, RAW (unmasked) allocation-descriptor length field
    /// and allocation position.
    ///
    /// ECMA-167 4/14.17 Extended File Entry (tag 266): L_EA at byte 208,
    /// L_AD at 212, allocation descriptors start at 216 + L_EA.
    /// ECMA-167 4/14.9 File Entry (tag 261): L_EA at 168, L_AD at 172,
    /// allocation descriptors start at 176 + L_EA.
    /// The extended-attribute area is filled with a recognisable non-zero
    /// pattern so that reading the allocation descriptor from the wrong
    /// offset cannot silently produce a usable value.
    fn build_dir_icb_tagged(tag: u16, l_ea: usize, raw_len: u32, ad_pos: u32) -> [u8; 2048] {
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&tag.to_le_bytes());
        let (l_ea_off, ad_base) = if tag == 266 {
            (208usize, 216usize)
        } else {
            (168usize, 176usize)
        };
        icb[l_ea_off..l_ea_off + 4].copy_from_slice(&(l_ea as u32).to_le_bytes());
        // L_AD = 8: exactly one short_ad (ECMA-167 4/14.14.1).
        icb[l_ea_off + 4..l_ea_off + 8].copy_from_slice(&8u32.to_le_bytes());
        icb[ad_base..ad_base + l_ea].fill(0xA5);
        let ad = ad_base + l_ea;
        if ad + 8 <= icb.len() {
            icb[ad..ad + 4].copy_from_slice(&raw_len.to_le_bytes());
            icb[ad + 4..ad + 8].copy_from_slice(&ad_pos.to_le_bytes());
        }
        icb
    }

    /// Append one File Identifier Descriptor (ECMA-167 4/14.4) to `buf`,
    /// with an explicit L_IU implementation-use area (4/14.4.7) sitting
    /// between the 38-byte header and the File Identifier (4/14.4.8).
    /// The descriptor is padded to a 4-byte multiple per 4/14.4.9.
    fn push_fid_iu(
        buf: &mut Vec<u8>,
        name: &str,
        icb_lba: u32,
        is_dir: bool,
        is_parent: bool,
        l_iu: u16,
    ) {
        let start = buf.len();
        let name_field: Vec<u8> = if name.is_empty() {
            Vec::new()
        } else {
            let mut v = vec![0x08u8]; // OSTA CS0 compression id 8
            v.extend_from_slice(name.as_bytes());
            v
        };
        let mut fid = vec![0u8; 38];
        fid[0..2].copy_from_slice(&257u16.to_le_bytes());
        let mut fc = 0u8;
        if is_dir {
            fc |= 0x02;
        }
        if is_parent {
            fc |= 0x08;
        }
        fid[18] = fc;
        fid[19] = name_field.len() as u8;
        fid[24..28].copy_from_slice(&icb_lba.to_le_bytes());
        fid[36..38].copy_from_slice(&l_iu.to_le_bytes());
        buf.extend_from_slice(&fid);
        buf.extend_from_slice(&vec![0xC3u8; l_iu as usize]);
        buf.extend_from_slice(&name_field);
        let used = buf.len() - start;
        buf.resize(start + ((used + 3) & !3), 0);
    }

    /// Names of the children `read_directory` reported, in order.
    fn child_names(d: &DirEntry) -> Vec<String> {
        d.entries.iter().map(|e| e.name.clone()).collect()
    }

    #[test]
    fn read_directory_reads_a_plain_file_entry_tag_261_directory_icb() {
        // ECMA-167 4/14.9 File Entry (tag 261) is a valid directory ICB (UDF 1.02 DVD-Video),
        // vs the Extended File Entry (266) of UDF 2.50. Its L_EA is at byte 168 and ADs begin at
        // 176 + L_EA, NOT the 208/216 of a 266; mishandling it fails every UDF 1.02 directory.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", 5, true, true, 0); // parent (..)
        push_fid_iu(&mut fids, "VIDEO_TS.IFO", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(261, 0, fids.len() as u32, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(4096, 4096, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("a File Entry (261) directory ICB is valid and must be readable");
        assert_eq!(child_names(&parsed), vec!["VIDEO_TS.IFO".to_string()]);
        assert_eq!(
            parsed.entries[0].size, 4096,
            "the child's size must come from its own ICB info_length"
        );
    }

    /// Build a directory ICB whose ICB Tag flags (ECMA-167 4/14.6.8, Uint16 at
    /// offset 34) select `ad_type`, with `body` written where the allocation
    /// descriptors live (216 + L_EA for a 266) and L_AD set to its length.
    fn build_dir_icb_flagged(ad_type: u16, body: &[u8]) -> [u8; 2048] {
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        icb[34..36].copy_from_slice(&ad_type.to_le_bytes());
        icb[56..64].copy_from_slice(&(body.len() as u64).to_le_bytes());
        icb[208..212].copy_from_slice(&0u32.to_le_bytes()); // L_EA
        icb[212..216].copy_from_slice(&(body.len() as u32).to_le_bytes()); // L_AD
        icb[216..216 + body.len()].copy_from_slice(body);
        icb
    }

    #[test]
    fn read_directory_reads_a_directory_whose_fids_are_embedded_in_the_icb() {
        // ECMA-167 4/14.6.8 ICB Tag flags bits 0-2 select storage; 3 = EMBEDDED (bytes sit in
        // the entry's AD field, no out-of-line extent). Decoding it as a short_ad reads the first
        // FID's bytes as a length/LBA, walking an unrelated sector and returning empty.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", 5, true, true, 0); // parent (..)
        push_fid_iu(&mut fids, "INDEX.BDMV", 7, false, false, 0);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_flagged(3, &fids));
        reader.put(7, build_efe_icb(1024, 1024, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("an embedded (AD type 3) directory is valid and must be readable");
        assert_eq!(child_names(&parsed), vec!["INDEX.BDMV".to_string()]);
        assert_eq!(parsed.entries[0].size, 1024);
    }

    #[test]
    fn read_directory_reads_an_extended_ad_directorys_extent_location() {
        // ECMA-167 4/14.14.3 extended_ad (20B) keeps extent_location at offset 12, NOT offset 4
        // like short_ad/long_ad; ICB Tag flags = 2 selects it (4/14.6.8). Reading offset 4 gets
        // recorded_length (here 2048), reading an empty sector so the directory comes back empty.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", 5, true, true, 0);
        push_fid_iu(&mut fids, "INDEX.BDMV", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut ext_ad = [0u8; 20];
        ext_ad[0..4].copy_from_slice(&(fids.len() as u32).to_le_bytes()); // extent_length
        ext_ad[4..8].copy_from_slice(&2048u32.to_le_bytes()); // recorded_length
        ext_ad[8..12].copy_from_slice(&(fids.len() as u32).to_le_bytes()); // information_length
        ext_ad[12..16].copy_from_slice(&60u32.to_le_bytes()); // extent_location

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_flagged(2, &ext_ad));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(1024, 1024, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("an extended_ad directory must be readable");
        assert_eq!(child_names(&parsed), vec!["INDEX.BDMV".to_string()]);
    }

    #[test]
    fn read_directory_masks_the_extent_type_bits_out_of_the_ad_length() {
        // ECMA-167 4/14.14.1.1: a short_ad's head 32-bit field is a 30-bit length plus a 2-bit
        // extent TYPE (bits 30..31), which must be masked off before use as a byte count;
        // folding them in yields >= 1 GiB, tripping MAX_DIR_BYTES. Encoded here as type 1.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", 5, true, true, 0);
        push_fid_iu(&mut fids, "INDEX.BDMV", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);
        let raw = 0x4000_0000u32 | fids.len() as u32;

        for tag in [261u16, 266u16] {
            let mut reader = MemReader::new();
            reader.put(5, build_dir_icb_tagged(tag, 0, raw, 60));
            reader.put(60, dir);
            reader.put(7, build_efe_icb(1024, 1024, 0));

            let parsed =
                read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
                    .unwrap_or_else(|e| {
                        panic!("tag {tag}: extent-type bits must not inflate the length: {e:?}")
                    });
            assert_eq!(child_names(&parsed), vec!["INDEX.BDMV".to_string()]);
            assert_eq!(
                parsed.size,
                fids.len() as u64,
                "tag {tag}: the reported directory size is the 30-bit length only"
            );
        }
    }

    #[test]
    fn read_directory_ignores_fid_bytes_past_the_declared_directory_length() {
        // Information length (short_ad length) bounds the FID list; the sector-rounded buffer
        // has trailing bytes past it that aren't the directory, so over-scanning reads a stale
        // deleted FID as live. Layout: FID "A" (40B) at 0, "GHOST" at 40, declared length 78.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "A", 7, false, false, 0);
        assert_eq!(
            fids.len(),
            40,
            "first FID must be 40 bytes for this fixture"
        );
        push_fid_iu(&mut fids, "GHOST", 8, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, 78, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(11, 2048, 0));
        reader.put(8, build_efe_icb(22, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses");
        assert_eq!(
            child_names(&parsed),
            vec!["A".to_string()],
            "a FID header starting at the declared end of the directory is not an entry"
        );
    }

    #[test]
    fn read_directory_stops_at_a_descriptor_whose_tag_is_not_a_fid() {
        // ECMA-167 3/7.2.1: the tag identifier is a 16-bit LE field (257 = FID); the FID list
        // ends at the first non-FID. A single-byte compare would accept 0x0201 (513) as 257
        // since both share low byte 0x01, wrongly parsing post-terminator bytes as FIDs.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "REAL.MPLS", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);
        // A non-FID descriptor whose LOW tag byte still reads 0x01.
        let next = fids.len();
        dir[next..next + 2].copy_from_slice(&513u16.to_le_bytes());
        // ... followed by bytes that would parse as a named file FID if the
        // scan wrongly continued.
        dir[next + 18] = 0x00;
        dir[next + 19] = 6;
        dir[next + 38] = 0x08;
        dir[next + 39..next + 44].copy_from_slice(b"AFTER");

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, 2048, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(1, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses");
        assert_eq!(
            child_names(&parsed),
            vec!["REAL.MPLS".to_string()],
            "both bytes of the descriptor tag must be compared"
        );
    }

    #[test]
    fn read_directory_keeps_a_fid_name_that_ends_exactly_at_the_buffer_end() {
        // A FID ending on the buffer's last byte is fully present and must decode; treating
        // "ends exactly at end" as "runs past end" drops the last file of a sector-filling
        // directory. Parent FID ends at 2000, so the second FID's name runs 2038..2048.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", 5, true, true, 1959);
        assert_eq!(fids.len(), 2000, "parent FID must end at 2000");
        push_fid_iu(&mut fids, "ENDSATEND", 7, false, false, 0);
        assert_eq!(fids.len(), 2048, "second FID must end exactly at 2048");
        let mut dir = [0u8; 2048];
        dir.copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, 2048, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(99, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses");
        assert_eq!(child_names(&parsed), vec!["ENDSATEND".to_string()]);
        assert_eq!(parsed.entries[0].size, 99);
    }

    #[test]
    fn read_directory_stops_instead_of_panicking_when_a_name_runs_past_the_buffer() {
        // L_IU (ECMA-167 4/14.4.7) is disc-controlled, so a malformed FID can
        // place the name tens of KB past the buffer end. The scan must stop;
        // slicing there would panic out of the public API on a damaged disc.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "OK.MPLS", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);
        // Second FID: header inside the buffer, name far outside it.
        let next = fids.len();
        dir[next..next + 2].copy_from_slice(&257u16.to_le_bytes());
        dir[next + 18] = 0x00;
        dir[next + 19] = 8; // L_FI
        dir[next + 36..next + 38].copy_from_slice(&60000u16.to_le_bytes()); // L_IU

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, 2048, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(5, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("an out-of-range File Identifier must end the scan, not fail the read");
        assert_eq!(
            child_names(&parsed),
            vec!["OK.MPLS".to_string()],
            "entries read before the malformed FID are kept"
        );
    }

    #[test]
    fn read_directory_admits_a_tree_that_exactly_fills_the_entry_budget() {
        // The entry budget is an anti-DoS ceiling; a tree that exactly reaches
        // it is still legal and must be enumerated, or the largest admissible
        // disc becomes unreadable. Counter starts one below the cap.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "LAST.M2TS", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, fids.len() as u32, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(7, 2048, 0));

        let mut budget = MAX_TOTAL_DIR_ENTRIES - 1;
        let parsed = read_directory(
            &mut reader,
            0,
            0,
            5,
            "ROOT",
            0,
            &mut budget,
            &mut HashSet::new(),
        )
        .expect("a tree that exactly reaches the entry budget must still be enumerated");
        assert_eq!(child_names(&parsed), vec!["LAST.M2TS".to_string()]);
        assert_eq!(budget, MAX_TOTAL_DIR_ENTRIES, "the entry was counted");
    }

    /// Lay a chain of `levels` nested directories into `reader`: directory
    /// *n* has ICB at LBA `100 + n`, FID list at LBA `200 + n`, and one
    /// subdirectory FID naming `D{n+1}`.
    fn lay_dir_chain(reader: &mut MemReader, levels: u32) {
        for n in 0..levels {
            let mut fids = Vec::new();
            push_fid_iu(&mut fids, "", 100 + n, true, true, 0);
            push_fid_iu(
                &mut fids,
                &format!("D{}", n + 1),
                100 + n + 1,
                true,
                false,
                0,
            );
            let mut dir = [0u8; 2048];
            dir[..fids.len()].copy_from_slice(&fids);
            reader.put(
                100 + n,
                build_dir_icb_tagged(266, 0, fids.len() as u32, 200 + n),
            );
            reader.put(200 + n, dir);
        }
    }

    /// Length of the chain of first-children hanging off `root`, counting
    /// `root` itself.
    fn chain_len(root: &DirEntry) -> usize {
        let mut n = 1;
        let mut cur = root;
        while let Some(next) = cur.entries.first() {
            n += 1;
            cur = next;
        }
        n
    }

    #[test]
    fn read_directory_stops_descending_at_the_nesting_cap() {
        // MAX_DIR_DEPTH bounds recursion; the directory AT the cap must be a
        // leaf, not descended into (an off-by-one removes the termination
        // bound). Chain is deeper than the cap so depth is cap-decided.
        let deeper = MAX_DIR_DEPTH + 4;
        let mut reader = MemReader::new();
        lay_dir_chain(&mut reader, deeper);

        let parsed = read_directory(
            &mut reader,
            0,
            0,
            100,
            "ROOT",
            0,
            &mut 0,
            &mut HashSet::new(),
        )
        .expect("a deep but well-formed chain parses");

        // Root is read at depth 0 and descends only while d < MAX_DIR_DEPTH, so
        // the deepest directory actually READ is at MAX_DIR_DEPTH, with its
        // child FID emitted as a leaf: MAX_DIR_DEPTH + 2 nodes total.
        assert_eq!(
            chain_len(&parsed),
            MAX_DIR_DEPTH as usize + 2,
            "recursion must stop at the nesting cap, not before or after it"
        );
        // The deepest node is still reported (its NAME is known from the
        // FID) but carries no children.
        let mut cur = &parsed;
        while let Some(next) = cur.entries.first() {
            cur = next;
        }
        assert!(
            cur.is_dir,
            "the capped node is still known to be a directory"
        );
        assert!(cur.entries.is_empty());
    }

    #[test]
    fn read_directory_advances_past_a_fids_implementation_use_area() {
        // ECMA-167 4/14.4.9: next FID starts at (38 + L_IU + L_FI) rounded up
        // to 4. Omitting L_IU from the stride lands mid-descriptor, dropping
        // every file after one with a non-empty IU area (here: stride 48 vs 40).
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "AAAAA", 7, false, false, 4);
        assert_eq!(
            fids.len(),
            48,
            "first FID stride must be 48 for this fixture"
        );
        push_fid_iu(&mut fids, "BBBBB", 8, false, false, 4);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, fids.len() as u32, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(1, 2048, 0));
        reader.put(8, build_efe_icb(2, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("dir parses");
        assert_eq!(
            child_names(&parsed),
            vec!["AAAAA".to_string(), "BBBBB".to_string()],
            "the FID stride must include L_IU"
        );
    }

    #[test]
    fn read_directory_reads_an_ad_that_ends_exactly_at_the_icb_sector_end() {
        // AD sits at 216 + L_EA (tag 266) or 176 + L_EA (tag 261). A short_ad
        // whose last byte is the sector's last byte is entirely present, so
        // the guard must reject only ADs that run PAST it, not an exact fit.
        for (tag, ad_base) in [(266u16, 216usize), (261u16, 176usize)] {
            let l_ea = 2048 - 8 - ad_base; // ad_off + 8 == 2048 exactly
            let mut fids = Vec::new();
            push_fid_iu(&mut fids, "TIGHT.CLPI", 7, false, false, 0);
            let mut dir = [0u8; 2048];
            dir[..fids.len()].copy_from_slice(&fids);

            let mut reader = MemReader::new();
            reader.put(5, build_dir_icb_tagged(tag, l_ea, fids.len() as u32, 60));
            reader.put(60, dir);
            reader.put(7, build_efe_icb(3, 2048, 0));

            let parsed =
                read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
                    .unwrap_or_else(|e| panic!("tag {tag}: exact-fit AD must be read: {e:?}"));
            assert_eq!(child_names(&parsed), vec!["TIGHT.CLPI".to_string()]);
        }
    }

    #[test]
    fn read_directory_rejects_an_ad_running_past_the_icb_sector() {
        // L_EA is a disc-controlled 32-bit byte count. One large enough to
        // push the allocation descriptor past the end of the 2048-byte ICB
        // sector must be an error, not an out-of-bounds read.
        for (tag, ad_base) in [(266u16, 216usize), (261u16, 176usize)] {
            let l_ea = 2048 - 4 - ad_base; // ad_off + 8 == 2052 > 2048
            let mut reader = MemReader::new();
            reader.put(5, build_dir_icb_tagged(tag, l_ea, 2048, 60));

            let err = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
                .expect_err("an AD past the end of the ICB sector must fail");
            assert!(
                matches!(err, Error::DiscRead { sector: 5, .. }),
                "tag {tag}: the error must name the ICB sector, got {err:?}"
            );
        }
    }

    #[test]
    fn read_directory_locates_the_ad_after_a_non_empty_extended_attribute_area() {
        // ECMA-167 4/14.9 / 4/14.17: Extended Attributes occupy L_EA bytes
        // immediately before the ADs, so ADs begin at base + L_EA. Computing
        // the offset any other way reads attribute/header bytes as an extent.
        for (tag, _base) in [(266u16, 216usize), (261u16, 176usize)] {
            let mut fids = Vec::new();
            push_fid_iu(&mut fids, "WITHEA.MPLS", 7, false, false, 0);
            let mut dir = [0u8; 2048];
            dir[..fids.len()].copy_from_slice(&fids);

            let mut reader = MemReader::new();
            reader.put(5, build_dir_icb_tagged(tag, 88, fids.len() as u32, 60));
            reader.put(60, dir);
            reader.put(7, build_efe_icb(6, 2048, 0));

            let parsed =
                read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
                    .unwrap_or_else(|e| panic!("tag {tag}: L_EA offset must be honoured: {e:?}"));
            assert_eq!(child_names(&parsed), vec!["WITHEA.MPLS".to_string()]);
        }
    }

    #[test]
    fn read_directory_skips_a_deleted_fid_and_never_follows_its_icb() {
        // ECMA-167 4/14.4.4 Deleted bit: the ICB MAY be a zero-length extent,
        // so it must be skipped outright — following it reads whatever sits at
        // metadata LBA 0, a hard error (dir) or phantom zero-byte file (file).
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "", 5, true, true, 0); // parent (..)
        // Deleted directory FID with a zeroed ICB field, exactly as 4/14.4.3
        // permits.
        let del = fids.len();
        push_fid_iu(&mut fids, "OLDDIR", 0, true, false, 0);
        fids[del + 18] |= 0x04; // Deleted
        push_fid_iu(&mut fids, "LIVE.MPLS", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, fids.len() as u32, 60));
        reader.put(60, dir);
        reader.put(7, build_efe_icb(64, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("a deleted FID must not fail the enumeration of its directory");
        assert_eq!(
            child_names(&parsed),
            vec!["LIVE.MPLS".to_string()],
            "a deleted File Identifier Descriptor is not a file"
        );
    }

    #[test]
    fn read_directory_reads_every_byte_of_the_short_ad_length_and_location() {
        // ECMA-167 4/14.14.1: short_ad is two 32-bit LE fields (length, then
        // location), every byte distinct non-zero here — a wrong-offset read
        // changes the reported length or the sector the FID list comes from.
        let ad_len: u32 = 0x0002_012C;
        let ad_pos: u32 = 0x0102_033C;
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "BYTEWISE.CLPI", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        for tag in [261u16, 266u16] {
            let mut reader = MemReader::new();
            reader.put(5, build_dir_icb_tagged(tag, 0, ad_len, ad_pos));
            reader.put(ad_pos, dir);
            reader.put(7, build_efe_icb(12, 2048, 0));

            let parsed =
                read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
                    .unwrap_or_else(|e| panic!("tag {tag}: short_ad must be decoded: {e:?}"));
            assert_eq!(
                parsed.size, ad_len as u64,
                "tag {tag}: every byte of the ExtentLength field is significant"
            );
            assert_eq!(
                child_names(&parsed),
                vec!["BYTEWISE.CLPI".to_string()],
                "tag {tag}: every byte of the ExtentPosition field is significant"
            );
        }
    }

    #[test]
    fn read_directory_rejects_a_length_whose_high_byte_alone_exceeds_the_cap() {
        // The MSB alone of the 30-bit ExtentLength can demand a 16 MiB
        // allocation. It must be read and the directory refused; ignoring it
        // lets a crafted descriptor through as a legitimately empty directory.
        for tag in [261u16, 266u16] {
            let mut reader = MemReader::new();
            reader.put(5, build_dir_icb_tagged(tag, 0, 0x0100_0000, 60));

            let err = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
                .expect_err("a 16 MiB directory length must be refused, not read as empty");
            assert!(
                matches!(err, Error::DiscRead { .. }),
                "tag {tag}: got {err:?}"
            );
        }
    }

    #[test]
    fn read_directory_reads_a_directory_exactly_at_the_size_cap() {
        // MAX_DIR_BYTES bounds the allocation a corrupt 30-bit length can force.
        // A directory declared exactly at the ceiling is admissible and must be
        // read; rejecting it would make the largest legal STREAM/ dir unreadable.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "00000.M2TS", 7, false, false, 0);
        let mut dir = [0u8; 2048];
        dir[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, MAX_DIR_BYTES, 60));
        reader.put(60, dir); // remaining sectors read as zeros → scan ends
        reader.put(7, build_efe_icb(4, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("a directory exactly at the size ceiling must be readable");
        assert_eq!(child_names(&parsed), vec!["00000.M2TS".to_string()]);
    }

    #[test]
    fn read_directory_reads_each_sector_of_a_multi_sector_directory_into_its_own_slot() {
        // Multi-sector dirs are read one sector per own window; a later sector
        // overwriting an earlier one destroys already-read FIDs. Sector 1 is
        // blank — if its read lands at offset 0 it erases sector 0's entry.
        let mut fids = Vec::new();
        push_fid_iu(&mut fids, "FIRST.CLPI", 7, false, false, 0);
        let mut sector0 = [0u8; 2048];
        sector0[..fids.len()].copy_from_slice(&fids);

        let mut reader = MemReader::new();
        reader.put(5, build_dir_icb_tagged(266, 0, 4096, 60));
        reader.put(60, sector0);
        reader.put(61, [0u8; 2048]);
        reader.put(7, build_efe_icb(8, 2048, 0));

        let parsed = read_directory(&mut reader, 0, 0, 5, "ROOT", 0, &mut 0, &mut HashSet::new())
            .expect("a two-sector directory parses");
        assert_eq!(child_names(&parsed), vec!["FIRST.CLPI".to_string()]);
    }
}

/// Shared UDF image fixtures for tests across the `disc::*` format scanners.
///
/// Builds an in-memory disc image that [`read_filesystem`] can navigate — a
/// [`MemDisc`] SectorSource plus a [`DirSpec`] tree laid out via [`lay_dir`] and
/// [`build_udf_skeleton`]. Format-agnostic: BD (`bluray.rs`), HD-DVD
/// (`hddvd.rs`), and the format detector (`disc/mod.rs`) all build their own
/// trees (`BDMV/`, `HVDVD_TS/`, `AACS/…`) on top of these primitives, so each
/// format's tests live in that format's file, not piled into one.
#[cfg(test)]
pub(crate) mod fixture {
    use crate::sector::SectorSource;
    use std::collections::HashMap;

    /// PART_START == META_START: file LBAs (partition-relative) and ICB/dir LBAs
    /// (metadata-relative) share one address space (abs = PART_START + lba), so
    /// `read_filesystem` takes the single-partition path.
    pub(crate) const PART_START: u32 = 2000;

    /// In-memory `SectorSource` (absolute-LBA → 2048-byte sector map); unmapped
    /// sectors read as zeroes.
    pub(crate) struct MemDisc {
        sectors: HashMap<u32, [u8; 2048]>,
    }

    impl MemDisc {
        pub(crate) fn new() -> Self {
            Self {
                sectors: HashMap::new(),
            }
        }
        fn put(&mut self, lba: u32, data: [u8; 2048]) {
            self.sectors.insert(lba, data);
        }
        /// Write arbitrary-length bytes at `lba`, split across 2048-byte sectors.
        pub(crate) fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
            for (i, chunk) in bytes.chunks(2048).enumerate() {
                let mut s = [0u8; 2048];
                s[..chunk.len()].copy_from_slice(chunk);
                self.put(lba + i as u32, s);
            }
        }
    }

    impl SectorSource for MemDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let need = count as usize * 2048;
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(need)
        }
    }

    /// One file's placement: ICB metadata LBA, data-extent LBA, byte length,
    /// Long-AD (16-byte, real BD-ROM layout) vs Short-AD, optional contents.
    pub(crate) struct FileSpec {
        pub(crate) name: String,
        pub(crate) icb_lba: u32,
        pub(crate) data_lba: u32,
        pub(crate) size: u64,
        pub(crate) long_ad: bool,
        pub(crate) contents: Vec<u8>,
    }

    /// A directory node: ICB LBA, FID-list LBA, child files and subdirectories.
    pub(crate) struct DirSpec {
        pub(crate) name: String,
        pub(crate) icb_lba: u32,
        pub(crate) dir_data_lba: u32,
        pub(crate) files: Vec<FileSpec>,
        pub(crate) subdirs: Vec<DirSpec>,
    }

    /// Build an Extended File Entry ICB (tag 266) with one allocation descriptor.
    pub(crate) fn build_file_icb(size: u64, data_lba: u32, long_ad: bool) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
        if long_ad {
            s[34..36].copy_from_slice(&1u16.to_le_bytes()); // ICB flags → Long AD
        }
        s[56..64].copy_from_slice(&size.to_le_bytes()); // info_length
        s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
        let ad_size: u32 = if long_ad { 16 } else { 8 };
        s[212..216].copy_from_slice(&ad_size.to_le_bytes()); // l_ad
        // AD length field is 32-bit; a size beyond u32 can't be backed by it,
        // which is the disc-vs-reality mismatch a hostile fixture must express.
        let ad_len = (size.min(u32::MAX as u64) as u32) & 0x3FFF_FFFF;
        s[216..220].copy_from_slice(&ad_len.to_le_bytes());
        s[220..224].copy_from_slice(&data_lba.to_le_bytes());
        s
    }

    fn build_dir_icb(dir_data_lba: u32, dir_data_len: u32) -> [u8; 2048] {
        build_file_icb(dir_data_len as u64, dir_data_lba, false)
    }

    /// Append one File Identifier Descriptor (tag 257) to `buf`.
    fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
        let start = buf.len();
        let name_field: Vec<u8> = if is_parent {
            Vec::new()
        } else {
            let mut v = vec![0x08u8];
            v.extend_from_slice(name.as_bytes());
            v
        };
        let l_fi = name_field.len();
        let mut fid = vec![0u8; 38];
        fid[0..2].copy_from_slice(&257u16.to_le_bytes()); // FID tag
        let mut file_chars = 0u8;
        if is_dir {
            file_chars |= 0x02;
        }
        if is_parent {
            file_chars |= 0x08;
        }
        fid[18] = file_chars;
        fid[19] = l_fi as u8;
        fid[24..28].copy_from_slice(&icb_lba.to_le_bytes()); // ICB long_ad LBA @24
        fid[36..38].copy_from_slice(&0u16.to_le_bytes()); // l_iu @36
        buf.extend_from_slice(&fid);
        buf.extend_from_slice(&name_field);
        let used = buf.len() - start;
        let pad = (used + 3) & !3;
        buf.resize(start + pad, 0);
    }

    /// Recursively lay a [`DirSpec`] into the [`MemDisc`].
    pub(crate) fn lay_dir(disc: &mut MemDisc, dir: &DirSpec) {
        let mut fids = Vec::new();
        push_fid(&mut fids, "", dir.icb_lba, true, true);
        for f in &dir.files {
            push_fid(&mut fids, &f.name, f.icb_lba, false, false);
            disc.put(
                PART_START + f.icb_lba,
                build_file_icb(f.size, f.data_lba, f.long_ad),
            );
            if !f.contents.is_empty() {
                disc.put_bytes(PART_START + f.data_lba, &f.contents);
            }
        }
        for sub in &dir.subdirs {
            push_fid(&mut fids, &sub.name, sub.icb_lba, true, false);
        }
        disc.put(
            PART_START + dir.icb_lba,
            build_dir_icb(dir.dir_data_lba, fids.len() as u32),
        );
        disc.put_bytes(PART_START + dir.dir_data_lba, &fids);
        for sub in &dir.subdirs {
            lay_dir(disc, sub);
        }
    }

    /// Build the static UDF anchor/VDS/FSD so `read_filesystem` reaches
    /// `root_icb_lba` (single partition map → metadata_start == PART_START).
    pub(crate) fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
        let mut avdp = [0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put(256, avdp);

        let mut pd = [0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put(32, pd);

        let mut lvd = [0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        disc.put(33, lvd);

        let mut td = [0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put(34, td);

        let mut fsd = [0u8; 2048];
        fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
        fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
        disc.put(PART_START, fsd);
    }

    pub(crate) fn file(
        name: &str,
        icb_lba: u32,
        data_lba: u32,
        size: u64,
        long_ad: bool,
    ) -> FileSpec {
        FileSpec {
            name: name.to_string(),
            icb_lba,
            data_lba,
            size,
            long_ad,
            contents: Vec::new(),
        }
    }

    pub(crate) fn file_with(
        name: &str,
        icb_lba: u32,
        data_lba: u32,
        contents: Vec<u8>,
        long_ad: bool,
    ) -> FileSpec {
        FileSpec {
            name: name.to_string(),
            icb_lba,
            data_lba,
            size: contents.len() as u64,
            long_ad,
            contents,
        }
    }
}
