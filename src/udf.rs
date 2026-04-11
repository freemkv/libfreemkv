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
use crate::sector::SectorReader;

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

    /// Read a file by path, returning its raw bytes.
    /// Reads sector by sector from disc — no buffering.
    /// Get the absolute starting LBA of a file on disc.
    /// Used by the rip pipeline to locate m2ts content sectors.
    pub fn file_start_lba(&self, reader: &mut dyn SectorReader, path: &str) -> Result<u32> {
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
                })
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
        Ok(self.partition_start + data_lba)
    }

    pub fn read_file(&self, reader: &mut dyn SectorReader, path: &str) -> Result<Vec<u8>> {
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
                })
            }
        };
        let entry = current
            .entries
            .iter()
            .find(|e| !e.is_dir && e.name.eq_ignore_ascii_case(filename))
            .ok_or_else(|| Error::UdfNotFound {
                path: path.to_string(),
            })?;

        // Read the file's ICB to get its data extent
        let (data_lba, data_len) = self.read_icb_extent(reader, entry.meta_lba)?;

        // Read file data sector by sector
        // File DATA is in the physical partition (partition_start + lba),
        // NOT the metadata partition. ICBs are in metadata, data is in physical.
        let sector_count = (data_len as u64).div_ceil(2048) as u32;
        let mut data = vec![0u8; (sector_count as usize) * 2048];
        let abs_start = self.partition_start + data_lba;

        for i in 0..sector_count {
            let offset = (i as usize) * 2048;
            read_sector(reader, abs_start + i, &mut data[offset..offset + 2048])?;
        }

        data.truncate(entry.size as usize);
        Ok(data)
    }

    /// Collect all sector ranges needed for disc-info and AACS.
    ///
    /// Returns a list of (start_lba, sector_count) ranges covering:
    ///   - UDF structure (AVDP, VDS, metadata partition, directories)
    ///   - BDMV/PLAYLIST/*.mpls, CLIPINF/*.clpi, JAR/*, META/*, *.bdmv
    ///   - AACS/* (Content*.cer, Unit_Key_RO.inf, CPSUnit*.cci)
    ///
    /// Skips: STREAM/ (video), BACKUP/, DUPLICATE/,
    ///   MKB_RO.inf, ContentHash*, ContentRevocation*
    pub fn metadata_sector_ranges(&self, reader: &mut dyn SectorReader) -> Result<Vec<(u32, u32)>> {
        let mut ranges = Vec::new();

        // UDF structure: sector 0 through end of metadata partition
        // Covers AVDP, VDS, partition descriptor, metadata ICB, FSD, all directories
        let meta_end = self.metadata_start + self.metadata_sectors;
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
        reader: &mut dyn SectorReader,
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
                ranges.push((self.meta_to_abs(child.meta_lba), 1));

                // Include file data — skip only truly huge files (MKB_RO.inf = 134MB)
                if child.size > 50_000_000 {
                    continue;
                }

                if let Ok((data_lba, data_len)) = self.read_icb_extent(reader, child.meta_lba) {
                    let abs_start = self.partition_start + data_lba;
                    let sector_count = (data_len as u64).div_ceil(2048) as u32;
                    ranges.push((abs_start, sector_count));
                }
            }
        }
        Ok(())
    }

    /// Convert a metadata-partition-relative LBA to an absolute sector number.
    fn meta_to_abs(&self, meta_lba: u32) -> u32 {
        self.metadata_start + meta_lba
    }

    /// Read an Extended File Entry (tag 266) or File Entry (tag 261)
    /// and return its first allocation extent: (data_lba, data_length).
    /// The data_lba is partition-relative.
    fn read_icb_extent(&self, reader: &mut dyn SectorReader, meta_lba: u32) -> Result<(u32, u32)> {
        let extents = self.read_icb_extents(reader, meta_lba)?;
        extents
            .first()
            .copied()
            .ok_or(Error::DiscRead { sector: 0 })
    }

    /// Read ALL allocation extents for a file from its ICB.
    /// Returns Vec of (partition_relative_lba, byte_length) pairs.
    /// Handles files with many extents (e.g. 88 GB m2ts files have ~90 extents).
    fn read_icb_extents(
        &self,
        reader: &mut dyn SectorReader,
        meta_lba: u32,
    ) -> Result<Vec<(u32, u32)>> {
        let mut icb = [0u8; 2048];
        read_sector(reader, self.meta_to_abs(meta_lba), &mut icb)?;

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
                        sector: self.meta_to_abs(meta_lba) as u64,
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
                        sector: self.meta_to_abs(meta_lba) as u64,
                    });
                }
                (ad_offset, l_ad)
            }
            _ => return Err(Error::DiscRead { sector: 0 }),
        };

        let mut extents = Vec::new();
        let num_descriptors = l_ad / 8; // Short Allocation Descriptor = 8 bytes

        for i in 0..num_descriptors {
            let off = ad_offset + i * 8;
            if off + 8 > icb.len() {
                break;
            }

            let raw_len = u32::from_le_bytes([icb[off], icb[off + 1], icb[off + 2], icb[off + 3]]);
            let extent_type = raw_len >> 30;
            let data_len = raw_len & 0x3FFFFFFF;
            let data_lba =
                u32::from_le_bytes([icb[off + 4], icb[off + 5], icb[off + 6], icb[off + 7]]);

            match extent_type {
                0 => extents.push((data_lba, data_len)), // recorded and allocated
                1 => {}     // allocated but not recorded (sparse) — skip
                3 => break, // next extent of allocation descriptors — TODO
                _ => break,
            }
        }

        Ok(extents)
    }

    /// Get all absolute disc sector extents for a file.
    /// Returns Vec of (absolute_lba, sector_count) covering the entire file.
    pub fn file_extents(
        &self,
        reader: &mut dyn SectorReader,
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
                })
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
        let mut disc_extents = Vec::new();
        for (lba, byte_len) in alloc_extents {
            let abs_lba = self.partition_start + lba;
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
pub fn read_filesystem(reader: &mut dyn SectorReader) -> Result<UdfFs> {
    // Step 1: Anchor Volume Descriptor Pointer at sector 256
    // ECMA-167 §10.2 — always at sector 256
    let mut avdp = [0u8; 2048];
    read_sector(reader, 256, &mut avdp)?;

    let tag_id = u16::from_le_bytes([avdp[0], avdp[1]]);
    if tag_id != 2 {
        return Err(Error::DiscRead { sector: 0 });
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
        return Err(Error::DiscRead { sector: 0 });
    }

    // Step 3: Parse partition maps from LVD to find metadata partition
    // BD-ROM discs (UDF 2.50) use a metadata partition (Type 2 map with "*UDF Metadata Partition")
    // The metadata file is stored at lba=0 of the physical partition
    let metadata_start = if num_partition_maps >= 2 {
        let lvd_sec = lvd_sector.ok_or(Error::DiscRead { sector: 0 })?;

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
                        });
                    }
                    let ad_len = u32::from_le_bytes([
                        meta_icb[ad_off],
                        meta_icb[ad_off + 1],
                        meta_icb[ad_off + 2],
                        meta_icb[ad_off + 3],
                    ]) & 0x3FFFFFFF;
                    metadata_size_bytes = ad_len;
                    let ad_pos = u32::from_le_bytes([
                        meta_icb[ad_off + 4],
                        meta_icb[ad_off + 5],
                        meta_icb[ad_off + 6],
                        meta_icb[ad_off + 7],
                    ]);
                    // Metadata content starts at partition_start + ad_pos
                    partition_start + ad_pos
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
        return Err(Error::DiscRead { sector: 0 });
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

/// Read a UDF directory and its children (up to max_depth levels).
///
/// Each directory is an ICB (Extended File Entry) pointing to directory data
/// containing File Identifier Descriptors (FIDs). Each FID names a file/subdir
/// and points to its ICB.
#[allow(clippy::only_used_in_recursion)]
fn read_directory(
    reader: &mut dyn SectorReader,
    part_start: u32,
    meta_start: u32,
    meta_lba: u32,
    name: &str,
    depth: u32,
) -> Result<DirEntry> {
    // Read ICB for this directory
    let mut icb = [0u8; 2048];
    read_sector(reader, meta_start + meta_lba, &mut icb)?;

    let tag = u16::from_le_bytes([icb[0], icb[1]]);

    // Get allocation extent: where the directory data lives
    let (ad_len, ad_pos) = match tag {
        266 => {
            let l_ea = u32::from_le_bytes([icb[208], icb[209], icb[210], icb[211]]) as usize;
            let ad_off = 216 + l_ea;
            if ad_off + 8 > icb.len() {
                return Err(Error::DiscRead {
                    sector: (meta_start + meta_lba) as u64,
                });
            }
            let len = u32::from_le_bytes([
                icb[ad_off],
                icb[ad_off + 1],
                icb[ad_off + 2],
                icb[ad_off + 3],
            ]) & 0x3FFFFFFF;
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
                    sector: (meta_start + meta_lba) as u64,
                });
            }
            let len = u32::from_le_bytes([
                icb[ad_off],
                icb[ad_off + 1],
                icb[ad_off + 2],
                icb[ad_off + 3],
            ]) & 0x3FFFFFFF;
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

    // Read directory data
    let dir_abs = meta_start + ad_pos;
    let sector_count = ad_len.div_ceil(2048).min(64);
    let mut dir_data = vec![0u8; sector_count as usize * 2048];
    for i in 0..sector_count {
        read_sector(
            reader,
            dir_abs + i,
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

                if is_dir && depth < 3 {
                    // Recurse into subdirectory (max 3 levels: BDMV/PLAYLIST/*.mpls)
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
fn read_file_size(reader: &mut dyn SectorReader, meta_start: u32, meta_lba: u32) -> Result<u64> {
    let mut icb = [0u8; 2048];
    read_sector(reader, meta_start + meta_lba, &mut icb)?;

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
        let last_end = last.0 + last.1;
        if start <= last_end + 1 {
            // Overlapping or adjacent — extend
            let new_end = (start + count).max(last_end);
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

/// Read a single 2048-byte sector from the drive.
/// Uses standard READ(10) — no unlock required.
fn read_sector(reader: &mut dyn SectorReader, lba: u32, buf: &mut [u8]) -> Result<()> {
    reader.read_sectors(lba, 1, buf)?;
    Ok(())
}
