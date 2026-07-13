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
        let (data_lba, _) = self.read_icb_extent(reader, entry.meta_lba)?;
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

    /// Shared implementation of [`read_file`] / [`read_file_prefix`]. When
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

        // Tiny files (notably the AACS `*.inf` key files) may store their data
        // embedded inline in the ICB (AD type 3), with no out-of-line extents.
        // Honor that before the extent path, which would otherwise misparse the
        // embedded bytes as allocation descriptors and (since 0.31.0) hard-error
        // on the resulting bogus extent length.
        if let Some(mut inline) = self.read_inline_data(reader, entry.meta_lba)? {
            let want = (entry.size as usize).min(limit);
            if inline.len() > want {
                inline.truncate(want);
            }
            return Ok(inline);
        }

        // Read the file's data extents. Multi-extent files (fragmented or split
        // across dual layers) would otherwise be silently truncated to the
        // first extent, since the buffer is sized to entry.size and truncate()
        // can't grow it.
        let extents = self.read_icb_extents(reader, entry.meta_lba)?;

        // Reject an oversized declared total before allocating: entry.size is a
        // raw u64 off the ICB, so a crafted file could otherwise force a
        // multi-hundred-MiB / GiB allocation. Only for the UNBOUNDED path — a
        // bounded read is limited by `limit` regardless of the declared size.
        if max_bytes.is_none() && entry.size > MAX_FILE_BYTES {
            return Err(Error::DiscRead {
                sector: self.partition_start as u64,
                status: None,
                sense: None,
            });
        }

        // Read the file DATA from the physical partition (partition_start +
        // lba), NOT the metadata partition: ICBs are in metadata, data is in
        // physical. Pre-allocate to the smaller of declared size and the
        // requested prefix (capped so a bogus entry.size can't reserve GiB).
        let cap_hint = (entry.size as usize)
            .min(limit)
            .min(MAX_FILE_BYTES as usize);
        let mut data = Vec::with_capacity(cap_hint);
        let mut sector = [0u8; 2048];
        'extents: for (data_lba, data_len) in extents {
            if max_bytes.is_none() {
                // Anti-DoS guards for the unbounded path: a crafted ICB can
                // chain many extents whose running total grows `data` into GiB,
                // or declare a single oversized extent. (Skipped when bounded —
                // `limit` already caps the read.)
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
            let abs_start = self
                .partition_start
                .checked_add(data_lba)
                .ok_or(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                })?;
            let sector_count = (data_len as u64).div_ceil(2048) as u32;
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
                    for (data_lba, data_len) in extents {
                        let abs_start = match self.partition_start.checked_add(data_lba) {
                            Some(v) => v,
                            None => continue,
                        };
                        let sector_count = (data_len as u64).div_ceil(2048) as u32;
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
    fn read_icb_extent(&self, reader: &mut dyn SectorSource, meta_lba: u32) -> Result<(u32, u32)> {
        let extents = self.read_icb_extents(reader, meta_lba)?;
        extents.first().copied().ok_or(Error::DiscRead {
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

    /// Read ALL allocation extents for a file from its ICB.
    /// Returns Vec of (partition_relative_lba, byte_length) pairs.
    /// Handles files with many extents (e.g. 88 GB m2ts files have ~90 extents)
    /// including files whose allocation descriptors span multiple blocks via
    /// continuation (extent_type 3) descriptors.
    fn read_icb_extents(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Vec<(u32, u32)>> {
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

        // Allocation-descriptor type lives in the ICB Tag flags (low 3
        // bits). The ICB Tag immediately follows the 16-byte descriptor
        // tag, and its `flags` u16 is the last field at ICB-tag offset 18
        // → absolute offset 34, for both File Entry (261) and Extended
        // File Entry (266). 0 = Short AD (8 bytes), 1 = Long AD (16 bytes),
        // 2 = Extended AD (20 bytes), 3 = data embedded inline in the ICB.
        //
        // This MUST be honoured: a Short AD and a Long AD both carry
        // length+lba in their first 8 bytes, so hardcoding an 8-byte
        // stride reads descriptor #0 of a Long-AD file correctly but lands
        // descriptor #1 in the middle of the first Long AD (its impl_use
        // bytes) — garbage that trips the terminator/unknown-type break.
        // Large BD-ROM .m2ts streams use Long ADs, so that bug truncated
        // every multi-extent title at its first extent (~973 MB-1 GiB).
        let icb_flags = u16::from_le_bytes([icb[34], icb[35]]);
        let ad_type = (icb_flags & 0x07) as usize;
        let ad_size: usize = match ad_type {
            0 => 8,  // Short AD
            1 => 16, // Long AD
            2 => 20, // Extended AD
            // 3 = inline/embedded data (no out-of-line extents) — never
            // used for large stream files. Anything else is unexpected;
            // fall back to the historical 8-byte stride rather than fail
            // the whole title.
            _ => 8,
        };

        let mut extents = Vec::new();

        // Parse the first allocation-descriptor list from the ICB. A type-3
        // descriptor ("next extent of allocation descriptors") points at a
        // continuation block in the metadata partition holding more ADs; we
        // follow the chain. The hop count is bounded to avoid looping on a
        // crafted/corrupt disc.
        let mut block = icb;
        let mut ad_start = ad_offset;
        let mut ad_bytes = l_ad;
        const MAX_AD_BLOCKS: usize = 256;

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
                    // Recorded and allocated. A zero-length type-0
                    // descriptor is the AD-list terminator (continuation
                    // blocks are scanned to the end of the sector, so the
                    // trailing zero padding must not be read as extents).
                    0 if data_len == 0 => break,
                    0 => extents.push((data_lba, data_len)),
                    1 => {} // allocated but not recorded (sparse)
                    3 => {
                        // Continuation: the rest of the ADs live in the block
                        // at data_lba (metadata-partition-relative). Stop
                        // scanning this block and follow the pointer.
                        if data_len > 0 {
                            next_block = Some(data_lba);
                        }
                        break;
                    }
                    _ => break,
                }
            }

            match next_block {
                Some(cont_lba) => {
                    read_sector(reader, self.meta_to_abs(cont_lba)?, &mut block)?;
                    // A continuation block does NOT begin with allocation
                    // descriptors — it begins with a 24-byte Allocation Extent
                    // Descriptor (ECMA-167 4/14.5, tag id 258): the 16-byte
                    // descriptor tag, then prev_allocation_extent_location
                    // (Uint32 @16) and length_of_allocation_descriptors
                    // (Uint32 @20). The real ADs (same Short/Long/Extended type
                    // as the file) start at offset 24, and their total byte
                    // length is that @20 field.
                    //
                    // Reading from offset 0 parses the AED's own tag header as
                    // allocation descriptors → one garbage extent, then an
                    // unknown extent_type break — silently truncating every
                    // file whose ADs spill into a continuation block (a heavily
                    // fragmented file, e.g. a Blu-ray 3D interleaved base-view
                    // .m2ts with ~1600 fragments). A normal few-extent .m2ts
                    // fits inline and never reaches here, which is why this
                    // stayed hidden.
                    let aed_l_ad =
                        u32::from_le_bytes([block[20], block[21], block[22], block[23]]) as usize;
                    ad_start = 24;
                    ad_bytes = aed_l_ad.min(block.len().saturating_sub(24));
                }
                None => break,
            }
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
    pub fn extents_abs_at(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<Vec<(u32, u32)>> {
        let alloc = self.read_icb_extents(reader, meta_lba)?;
        let mut out = Vec::with_capacity(alloc.len());
        for (lba, byte_len) in alloc {
            let abs = self
                .partition_start
                .checked_add(lba)
                .ok_or(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                })?;
            out.push((abs, byte_len));
        }
        Ok(out)
    }

    /// Get all absolute disc sector extents for a file.
    /// Returns Vec of (absolute_lba, sector_count) covering the entire file.
    pub fn file_extents(
        &self,
        reader: &mut dyn SectorSource,
        path: &str,
    ) -> Result<Vec<(u32, u32)>> {
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

        let alloc_extents = self.read_icb_extents(reader, entry.meta_lba)?;
        let mut disc_extents = Vec::with_capacity(alloc_extents.len());
        for (lba, byte_len) in alloc_extents {
            let abs_lba = self
                .partition_start
                .checked_add(lba)
                .ok_or(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                })?;
            let sectors = (byte_len as u64).div_ceil(2048) as u32;
            disc_extents.push((abs_lba, sectors));
        }
        Ok(disc_extents)
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
        return Err(Error::DiscRead {
            sector: 256,
            status: None,
            sense: None,
        });
    }

    // Main VDS extent location: bytes [16:20] = LBA, [20:24] = length
    // (We use the VDS at sectors 32+, not the reserve copy at sector 32768+)

    // Step 2: Read Volume Descriptor Sequence (sectors 32-37 typically)
    // Find Partition Descriptor (tag 5) and Logical Volume Descriptor (tag 6)
    let mut partition_start: u32 = 0;
    let mut num_partition_maps: u32 = 0;
    let mut lvd_sector: Option<u32> = None;
    let mut volume_id = String::new();
    let mut metadata_size_bytes: u32 = 0;

    for i in 32..64 {
        let mut desc = [0u8; 2048];
        read_sector(reader, i, &mut desc)?;

        let desc_tag = u16::from_le_bytes([desc[0], desc[1]]);
        match desc_tag {
            // Primary Volume Descriptor — volume identifier at offset 24, 32-byte d-string
            1 => {
                volume_id = parse_dstring(&desc[24..56]);
            }
            // Partition Descriptor — tells us where the physical partition starts
            5 => {
                partition_start = u32::from_le_bytes([desc[188], desc[189], desc[190], desc[191]]);
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

    if partition_start == 0 {
        return Err(Error::DiscRead {
            sector: 0,
            status: None,
            sense: None,
        });
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
            let pm2_type = lvd[440 + pm1_len]; // Second map type

            if pm2_type == 2 {
                // Type 2 = metadata partition
                // The metadata file ICB is at physical partition lba 0
                // Read it to find where the metadata content starts
                let meta_file_lba = partition_start; // lba 0 of partition
                let mut meta_icb = [0u8; 2048];
                read_sector(reader, meta_file_lba, &mut meta_icb)?;

                let meta_tag = u16::from_le_bytes([meta_icb[0], meta_icb[1]]);
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
        return Err(Error::DiscRead {
            sector: metadata_start as u64,
            status: None,
            sense: None,
        });
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

    // Get allocation extent: where the directory data lives
    let (ad_len, ad_pos) = match tag {
        266 => {
            let l_ea = u32::from_le_bytes([icb[208], icb[209], icb[210], icb[211]]) as usize;
            let ad_off = 216 + l_ea;
            if ad_off + 8 > icb.len() {
                return Err(Error::DiscRead {
                    sector: icb_abs as u64,
                    status: None,
                    sense: None,
                });
            }
            let len = u32::from_le_bytes([
                icb[ad_off],
                icb[ad_off + 1],
                icb[ad_off + 2],
                icb[ad_off + 3],
            ]) & 0x3FFF_FFFF;
            let pos = u32::from_le_bytes([
                icb[ad_off + 4],
                icb[ad_off + 5],
                icb[ad_off + 6],
                icb[ad_off + 7],
            ]);
            (len, pos)
        }
        261 => {
            let l_ea = u32::from_le_bytes([icb[168], icb[169], icb[170], icb[171]]) as usize;
            let ad_off = 176 + l_ea;
            if ad_off + 8 > icb.len() {
                return Err(Error::DiscRead {
                    sector: icb_abs as u64,
                    status: None,
                    sense: None,
                });
            }
            let len = u32::from_le_bytes([
                icb[ad_off],
                icb[ad_off + 1],
                icb[ad_off + 2],
                icb[ad_off + 3],
            ]) & 0x3FFF_FFFF;
            let pos = u32::from_le_bytes([
                icb[ad_off + 4],
                icb[ad_off + 5],
                icb[ad_off + 6],
                icb[ad_off + 7],
            ]);
            (len, pos)
        }
        _ => {
            return Ok(DirEntry {
                name: name.to_string(),
                is_dir: true,
                meta_lba,
                size: 0,
                entries: Vec::new(),
            });
        }
    };

    // Reject an oversized directory before allocating: ad_len is the
    // disc-controlled 30-bit ICB allocation length, so a corrupt value
    // could otherwise force a ~1 GiB zeroed allocation (amplified by
    // recursion). Real directories are a few KiB; the 1 MiB cap still
    // covers a large STREAM/ dir with thousands of .m2ts FIDs.
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

        // FID ICB is a long_ad starting at offset 20:
        //   [20:24] = extent_length
        //   [24:28] = extent_location (LBA within metadata partition)
        //   [28:30] = partition_reference_number
        //   [30:36] = implementation_use
        let icb_lba = u32::from_le_bytes([
            dir_data[pos + 24],
            dir_data[pos + 25],
            dir_data[pos + 26],
            dir_data[pos + 27],
        ]);
        let l_iu = u16::from_le_bytes([dir_data[pos + 36], dir_data[pos + 37]]) as usize;

        let is_dir = (file_chars & 0x02) != 0;
        let is_parent = (file_chars & 0x08) != 0;

        if !is_parent && l_fi > 0 {
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

                // Read the ICB to get file size
                let file_size = read_file_size(reader, meta_start, icb_lba).unwrap_or(0);

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
                        // Recurse into subdirectory. The depth cap guards
                        // against pathological nesting on a corrupt disc while
                        // comfortably covering real BD-ROM nesting
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
fn parse_udf_name(data: &[u8]) -> String {
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
        if start <= last_end.saturating_add(1) {
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
                    if c != 0 {
                        if let Some(ch) = char::from_u32(c as u32) {
                            s.push(ch);
                        }
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
            // Check sliding cache
            if lba >= self.cache_start && lba < self.cache_start + self.cache_sectors {
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
                    // By design: a `block`-sector batch read that starts
                    // valid but runs past the last recorded sector fails as
                    // a unit. Retry the one sector actually requested so a
                    // batch overrunning the disc tail still serves the live
                    // LBA instead of erroring; a genuinely bad single sector
                    // then propagates via `?`.
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
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
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
            // off+8..off+10 = partition reference (0), off+10..off+16 =
            // impl_use (0). Leaving these zero is what trips the old
            // 8-byte-stride parser into reading a bogus zero-length
            // terminator as descriptor #1.
            off += 16;
        }
        s
    }

    /// A continuation block: a bare list of short ADs from byte 0.
    fn build_cont_block(ads: &[(u32, u32, u32)]) -> [u8; 2048] {
        // A continuation block is an Allocation Extent Descriptor (ECMA-167
        // 4/14.5): a 16-byte descriptor tag, then prev_allocation_extent_location
        // (Uint32 @16) and length_of_allocation_descriptors (Uint32 @20). The
        // actual allocation descriptors begin at offset 24. The parser skips the
        // 24-byte header and reads `l_ad` bytes of ADs from there.
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

    fn fs_with(part_start: u32, meta_start: u32, root: DirEntry) -> UdfFs {
        UdfFs {
            root,
            volume_id: String::new(),
            partition_start: part_start,
            metadata_start: meta_start,
            metadata_sectors: 0,
        }
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
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        assert_eq!(extents, vec![(10, 4096), (20, 2048)]);
    }

    #[test]
    fn icb_extents_continuation_skips_aed_header_not_read_as_extent() {
        // Regression (Blu-ray 3D): a continuation block begins with a 24-byte
        // Allocation Extent Descriptor whose 16-byte descriptor tag holds
        // NON-zero bytes (tag id 258, CRC, location, ...). Reading allocation
        // descriptors from offset 0 parses that tag header as an AD → a garbage
        // extent, then an unknown extent_type break → every fragment past the
        // first continuation is lost. On a 3D disc that truncated the 25.8 GB
        // interleaved base-view feature to ~1.8 GB (113 of ~1600 fragments) and
        // then choked the mux on the bogus, non-unit-aligned extent. The parser
        // MUST skip the 24-byte AED header and read the real ADs from offset 24.
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
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        assert_eq!(
            extents,
            vec![(10, 4096), (20, 2048)],
            "continuation ADs must be read past the 24-byte AED header, not from offset 0"
        );
    }

    #[test]
    fn icb_extents_long_ad_returns_all_extents_not_just_first() {
        // Regression: BD-ROM large .m2ts files use Long ADs (16-byte
        // descriptors). The pre-fix parser hardcoded an 8-byte stride, so
        // it read descriptor #0 (length+lba align in both layouts) then
        // misread descriptor #1 from the middle of the first Long AD —
        // a zero terminator — and returned ONLY the first extent. That
        // truncated every multi-extent title at ~973 MB-1 GiB.
        //
        // Four Long ADs, each a near-max Short-AD-sized extent. The fix
        // must return all four; the old code returned exactly one.
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
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
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
        // Regression for BOTH 0.31.0 bugs through `read_file`: this is the
        // exact path `Disc::read_aacs_inputs_from_reader` uses to read
        // `/AACS/MKB_RO.inf` + `Unit_Key_RO.inf`. A Long-AD, multi-extent
        // file (large UHD/Blu-ray layout) must return ALL its bytes. With the
        // pre-fix Short-AD-only parser this read stopped after the first
        // extent, which (a) truncated the mux and (b) made autorip's
        // `key_files()` see a short/garbage AACS file → `MissingInputs` →
        // the online key request was never sent.
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
        // PRECOMMIT proof for the autorip online-keyserver path (no disc / no
        // deploy). autorip's key request is gated on Disc::read_aacs_inputs
        // (keysource.rs key_files()): it reads /AACS/Unit_Key_RO.inf and
        // /AACS/MKB_RO.inf. On a Long-AD disc (UHD / large Blu-ray) the
        // pre-0.31.1 Short-AD-only reader truncated those files at their first
        // extent, breaking key derivation. This
        // fixture lays a Long-AD, multi-extent Unit_Key_RO.inf under /AACS and
        // asserts read_aacs_inputs returns its FULL content — i.e. the keyserver
        // inputs are complete, so the request is built correctly.
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
        // Two extents, each individually within MAX_FILE_BYTES, that together
        // exceed it. The cumulative guard must fire on the second extent
        // (before reading it) rather than growing `data` past the cap.
        // First extent: a single sector (read, data.len() = 2048). Second
        // extent: exactly MAX_FILE_BYTES (passes the per-extent cap) — the
        // 2048 already buffered pushes the running total over the cap.
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
        // ECMA-167 §14.14.3: an Extended AD is 20 bytes and its extent LBA
        // is at byte offset +12, NOT +4 (that's RecordedLength). The parser
        // branches on ICB-tag flags==2 to a 20-byte stride and lba_off=off+12.
        // Three extents must come back with the CORRECT LBAs and lengths.
        let icb = build_efe_ext(3 * 2048, &[(0, 2048, 700), (0, 2048, 800), (0, 4096, 900)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("EXT", 5, 3 * 2048));
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        // If the stride were wrong (8 or 16) or lba_off were off+4, the LBAs
        // would be the 0xDEADBEEF junk or misaligned garbage, not these.
        assert_eq!(extents, vec![(700, 2048), (800, 2048), (900, 4096)]);
    }

    #[test]
    fn icb_extents_short_ad_type1_sparse_extent_is_skipped_not_emitted() {
        // ECMA-167 §14.14.1.1: extent type 1 = "allocated but not recorded"
        // (a sparse hole). It carries no on-disc data, so it must NOT be
        // returned as a readable extent. A type-0 extent after it must still
        // be reached (the loop must continue past a type-1, not break).
        let icb = build_efe(
            6144,
            &[
                (0, 2048, 10), // recorded
                (1, 2048, 20), // sparse — allocated, not recorded
                (0, 2048, 30), // recorded, after the hole
            ],
        );
        let mut reader = MapReader::new();
        reader.put(5, icb);
        let fs = fs_with(0, 0, file_entry("SP", 5, 6144));
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        // The sparse (type-1) middle descriptor must be absent; the two
        // recorded extents must both be present and in order.
        assert_eq!(extents, vec![(10, 2048), (30, 2048)]);
    }

    #[test]
    fn icb_extents_zero_length_type0_terminates_list() {
        // ECMA-167: a zero-length type-0 AD terminates the descriptor list.
        // Trailing zero padding (all-zero ADs) MUST stop parsing — otherwise
        // a stray non-zero AD after the terminator becomes a bogus extent.
        // One real extent, then a zero AD, then an AD that must NEVER be read.
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
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        assert_eq!(
            extents,
            vec![(10, 2048)],
            "parsing must stop at the zero-length terminator"
        );
    }

    #[test]
    fn icb_extents_continuation_loop_terminates_without_hang_or_panic() {
        // Hostile input: a type-3 continuation descriptor whose continuation
        // block points back at itself (a cycle). The MAX_AD_BLOCKS bound must
        // make this terminate rather than loop forever. We assert it returns
        // a finite Vec and does not panic. The continuation block at meta-rel
        // lba 50 contains a recorded extent + a type-3 AD pointing to lba 50.
        let icb = build_efe(2048, &[(0, 2048, 10), (3, 2048, 50)]);
        let cont = build_cont_block(&[(0, 2048, 20), (3, 2048, 50)]);
        let mut reader = MapReader::new();
        reader.put(5, icb);
        reader.put(50, cont);
        let fs = fs_with(0, 0, file_entry("LOOP", 5, 2048));
        // Must return Ok (bounded), not hang or panic.
        let extents = fs.read_icb_extents(&mut reader, 5).expect("extents");
        // First block contributes extent (10,2048); each revisit of the
        // self-referential cont block adds (20,2048). The hop bound caps the
        // total, so the Vec is finite. (256 blocks max → < 600 extents.)
        assert!(extents.len() < 1024, "continuation chain must be bounded");
        assert_eq!(extents[0], (10, 2048));
        assert_eq!(extents[1], (20, 2048));
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
    fn parse_udf_name_8bit_compression_id_8() {
        // Compression ID 8 = 8-bit (OSTA CS0 / ASCII). "BDMV" must round-trip.
        let mut raw = vec![8u8];
        raw.extend_from_slice(b"BDMV");
        assert_eq!(parse_udf_name(&raw), "BDMV");
    }

    #[test]
    fn read_directory_honors_l_iu_offset_for_fid_name() {
        // ECMA-167 §14.4 File Identifier Descriptor: the File Identifier
        // begins at offset 38 + L_IU. A non-zero L_IU must shift the name
        // read; ignoring it would read impl_use bytes as the name.
        //   0..2  tag = 257   18 file chars   19 L_FI
        //   24..28 ICB LBA    36..38 L_IU      38.. impl_use[L_IU] then FI[L_FI]
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
        // ECMA-167 §14.4.3: file characteristics bit 3 (0x08) = "parent" (the
        // ".." back-link). It must NOT appear as a named child entry. To
        // isolate the parent-flag gate (rather than the L_FI==0 gate that
        // real parent FIDs also have), this fixture gives the parent FID a
        // VALID non-zero L_FI and a real name: the ONLY reason it must be
        // skipped is the parent characteristic bit.
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
    fn parse_dstring_oversized_length_byte_returns_empty_not_panic() {
        // Hostile/corrupt input: a length byte larger than the field must not
        // index out of bounds. parse_dstring guards len > data.len() → "".
        let mut field = [0u8; 8];
        field[0] = 8;
        field[1] = b'A';
        *field.last_mut().unwrap() = 200; // way past the 8-byte field
        assert_eq!(parse_dstring(&field), "");
    }

    #[test]
    fn read_inline_data_rejects_oversized_lea() {
        // AD type=3 (inline data) with an L_EA so large that ad_offset =
        // 216 + L_EA overflows past the 2048-byte ICB. Before the fix,
        // the `.min(icb.len())` clamp produced start==end==2048 and the
        // function returned Ok(Some(vec![])) — silently dropping the file
        // content. AACS key files (Unit_Key_RO.inf) read as 0 bytes and
        // decryption failed without a useful diagnostic.
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
        // rejected rather than visited indefinitely. `budget` is the running
        // global counter (threshold = MAX_TOTAL_DIR_ENTRIES); pre-load it to
        // within 10 of the cap and feed 51 file FIDs — the walk must error once
        // the counter crosses the cap, before consuming all of them.
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
        // A directory whose child FID (is_dir=true) points back to the same
        // ICB LBA as the parent (a self-referential cycle) must NOT recurse.
        // It must be emitted as a leaf entry instead.
        //
        // Layout:
        //   meta_lba 5  — root directory ICB, dir data at lba 60
        //   lba 60      — one FID: is_dir, ICB at lba 5 (self-reference)
        //
        // We seed visited with lba 5 (the root we are about to descend into),
        // so when the FID points back to lba 5 the cycle is detected immediately.
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
        pub(crate) size: u32,
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
    pub(crate) fn build_file_icb(size: u32, data_lba: u32, long_ad: bool) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
        if long_ad {
            s[34..36].copy_from_slice(&1u16.to_le_bytes()); // ICB flags → Long AD
        }
        s[56..64].copy_from_slice(&(size as u64).to_le_bytes()); // info_length
        s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
        let ad_size: u32 = if long_ad { 16 } else { 8 };
        s[212..216].copy_from_slice(&ad_size.to_le_bytes()); // l_ad
        s[216..220].copy_from_slice(&(size & 0x3FFF_FFFF).to_le_bytes());
        s[220..224].copy_from_slice(&data_lba.to_le_bytes());
        s
    }

    fn build_dir_icb(dir_data_lba: u32, dir_data_len: u32) -> [u8; 2048] {
        build_file_icb(dir_data_len, dir_data_lba, false)
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
        size: u32,
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
            size: contents.len() as u32,
            long_ad,
            contents,
        }
    }
}
