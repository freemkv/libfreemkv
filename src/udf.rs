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

/// Upper bound on a single metadata file read (`read_file`). BD-ROM
/// metadata files (.mpls/.clpi/.inf/.bdmv) are a few KiB to tens of MiB;
/// 64 MiB is a generous ceiling. Caps the allocation so a crafted ICB
/// info_length / extent length cannot force a huge zeroed reservation
/// before any data is read.
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

        // Read ALL the file's data extents. Multi-extent files (fragmented or
        // split across dual layers) would otherwise be silently truncated to
        // the first extent, since the buffer is sized to entry.size and
        // truncate() can't grow it.
        let extents = self.read_icb_extents(reader, entry.meta_lba)?;

        // Reject an oversized declared total before allocating: entry.size is
        // a raw u64 off the ICB, so a crafted file could otherwise force a
        // multi-hundred-MiB / GiB allocation across its extents.
        if entry.size > MAX_FILE_BYTES {
            return Err(Error::DiscRead {
                sector: self.partition_start as u64,
                status: None,
                sense: None,
            });
        }

        // Read ALL the file's data extents. File DATA is in the physical
        // partition (partition_start + lba), NOT the metadata partition: ICBs
        // are in metadata, data is in physical.
        let mut data = Vec::with_capacity(entry.size as usize);
        let mut sector = [0u8; 2048];
        for (data_lba, data_len) in extents {
            // Cumulative guard: entry.size and each per-extent data_len are
            // capped individually above, but a crafted ICB can chain many
            // small extents (read_icb_extents follows type-3 chains up to
            // MAX_AD_BLOCKS) whose running total grows `data` into GiB. Reject
            // once the accumulated bytes would exceed MAX_FILE_BYTES.
            if data.len() as u64 + data_len as u64 > MAX_FILE_BYTES {
                return Err(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                });
            }
            // data_len is the disc-controlled 30-bit extent length; reject an
            // oversized extent before reading so a crafted ICB can't grow the
            // buffer past MAX_FILE_BYTES.
            if data_len as u64 > MAX_FILE_BYTES {
                return Err(Error::DiscRead {
                    sector: self.partition_start as u64,
                    status: None,
                    sense: None,
                });
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
                let abs = abs_start.checked_add(i).ok_or(Error::DiscRead {
                    sector: abs_start as u64,
                    status: None,
                    sense: None,
                })?;
                read_sector(reader, abs, &mut sector)?;
                data.extend_from_slice(&sector);
            }
        }

        // Trim to the real file size; if extents under-covered the file (e.g.
        // sparse), leave what we have rather than over-reporting.
        if data.len() > entry.size as usize {
            data.truncate(entry.size as usize);
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

    /// All sector ranges that contain data (metadata + all files including STREAM).
    /// For full disc-to-ISO dumps — reads only allocated sectors, skips gaps.
    pub fn all_sector_ranges(&self, reader: &mut dyn SectorSource) -> Result<Vec<(u32, u32)>> {
        let mut ranges = Vec::new();

        // UDF structure sectors
        let meta_end = self.metadata_start.saturating_add(self.metadata_sectors);
        ranges.push((0, meta_end));

        // Walk entire tree including STREAM directories
        self.collect_all_file_ranges(reader, &self.root, &mut ranges)?;

        // Merge overlapping/adjacent ranges and sort
        ranges.sort_by_key(|r| r.0);
        let merged = merge_ranges(&ranges);
        Ok(merged)
    }

    fn collect_all_file_ranges(
        &self,
        reader: &mut dyn SectorSource,
        entry: &DirEntry,
        ranges: &mut Vec<(u32, u32)>,
    ) -> Result<()> {
        for child in &entry.entries {
            if child.is_dir {
                self.collect_all_file_ranges(reader, child, ranges)?;
            } else {
                // Include the ICB sector
                ranges.push((self.meta_to_abs(child.meta_lba)?, 1));

                // Include ALL file data extents (large m2ts files have many)
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
            let num_descriptors = ad_bytes / 8; // Short Allocation Descriptor = 8 bytes
            let mut next_block: Option<u32> = None;

            for i in 0..num_descriptors {
                let off = ad_start + i * 8;
                if off + 8 > block.len() {
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
                let data_lba = u32::from_le_bytes([
                    block[off + 4],
                    block[off + 5],
                    block[off + 6],
                    block[off + 7],
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
                    // A continuation block is a list of Short ADs from byte 0,
                    // spanning the whole sector.
                    ad_start = 0;
                    ad_bytes = block.len();
                }
                None => break,
            }
        }

        Ok(extents)
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

    // Step 5: Read root directory and build file tree
    let root = read_directory(reader, partition_start, metadata_start, root_lba, "", 0)?;

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

/// Read a UDF directory and its children (up to [`MAX_DIR_DEPTH`] levels).
///
/// Each directory is an ICB (Extended File Entry) pointing to directory data
/// containing File Identifier Descriptors (FIDs). Each FID names a file/subdir
/// and points to its ICB. Directories deeper than [`MAX_DIR_DEPTH`] are
/// recorded as entries but not descended into.
#[allow(clippy::only_used_in_recursion)]
fn read_directory(
    reader: &mut dyn SectorSource,
    part_start: u32,
    meta_start: u32,
    meta_lba: u32,
    name: &str,
    depth: u32,
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
                // Read the ICB to get file size
                let file_size = read_file_size(reader, meta_start, icb_lba).unwrap_or(0);

                if is_dir && depth < MAX_DIR_DEPTH {
                    // Recurse into subdirectory. The cap guards against
                    // pathological/looping directory trees on a corrupt disc
                    // while comfortably covering real BD-ROM nesting
                    // (e.g. BDMV/BACKUP/BDJO/*.bdjo is 3 levels deep).
                    let subdir = read_directory(
                        reader,
                        part_start,
                        meta_start,
                        icb_lba,
                        &entry_name,
                        depth + 1,
                    )?;
                    entries.push(subdir);
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

/// Merge overlapping or adjacent (start, count) ranges.
fn merge_ranges(ranges: &[(u32, u32)]) -> Vec<(u32, u32)> {
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
    pub(crate) fn prefetch_ranges(&mut self, ranges: &[(u32, u32)]) {
        let mut tmp = vec![0u8; self.batch as usize * 2048];
        for &(start, count) in ranges {
            let mut offset = 0u32;
            while offset < count {
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
                    let s = i as usize * 2048;
                    self.prefetched
                        .insert(start + offset + i, tmp[s..s + 2048].to_vec());
                }
                offset += batch as u32;
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

    /// A continuation block: a bare list of short ADs from byte 0.
    fn build_cont_block(ads: &[(u32, u32, u32)]) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        let mut off = 0usize;
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

        let err = read_directory(&mut reader, 0, 0, 5, "DIR", 0).unwrap_err();
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
        let dir = read_directory(&mut reader, 0, 0, 5, "DIR", 0).expect("small dir parses");
        assert!(dir.entries.is_empty());
        assert!(dir.is_dir);
    }
}
