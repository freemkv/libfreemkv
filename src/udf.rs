//! UDF filesystem reader — read files from Blu-ray discs.
//!
//! Minimal UDF implementation: just enough to find and read files
//! in the BDMV directory structure. Not a full UDF implementation.
//!
//! Reference: ECMA-167, UDF 2.50 (OSTA)

use crate::error::{Error, Result};
use crate::drive::DriveSession;
use crate::scsi::DataDirection;

/// A UDF filesystem parsed from disc.
#[derive(Debug)]
pub struct UdfFs {
    /// Root directory entries
    pub root: DirEntry,
    /// Partition start LBA
    partition_start: u32,
}

/// A directory entry (file or directory).
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    /// LBA of the file/directory data
    pub lba: u32,
    /// Size in bytes
    pub size: u32,
    /// Child entries (if directory)
    pub entries: Vec<DirEntry>,
}

impl UdfFs {
    /// Find a directory by path (e.g. "/BDMV/PLAYLIST").
    pub fn find_dir(&self, path: &str) -> Option<&DirEntry> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut current = &self.root;

        for part in &parts {
            current = current.entries.iter().find(|e| {
                e.is_dir && e.name.eq_ignore_ascii_case(part)
            })?;
        }

        Some(current)
    }

    /// Read a file by path, returning its contents.
    pub fn read_file(&self, session: &mut DriveSession, path: &str) -> Result<Vec<u8>> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut current = &self.root;

        // Navigate to parent directory
        for part in &parts[..parts.len() - 1] {
            current = current.entries.iter().find(|e| {
                e.is_dir && e.name.eq_ignore_ascii_case(part)
            }).ok_or_else(|| Error::DiscError {
                detail: format!("directory not found: {}", part),
            })?;
        }

        // Find the file
        let filename = parts.last().unwrap();
        let entry = current.entries.iter().find(|e| {
            !e.is_dir && e.name.eq_ignore_ascii_case(filename)
        }).ok_or_else(|| Error::DiscError {
            detail: format!("file not found: {}", path),
        })?;

        // Read the file sectors
        let sector_count = (entry.size + 2047) / 2048;
        let mut data = vec![0u8; (sector_count * 2048) as usize];

        for i in 0..sector_count {
            let lba = self.partition_start + entry.lba + i;
            let offset = (i * 2048) as usize;
            read_sector(session, lba, &mut data[offset..offset + 2048])?;
        }

        data.truncate(entry.size as usize);
        Ok(data)
    }
}

/// Read the UDF filesystem from a disc.
pub fn read_filesystem(session: &mut DriveSession) -> Result<UdfFs> {
    // UDF Anchor Volume Descriptor Pointer at sector 256
    let mut avdp = [0u8; 2048];
    read_sector(session, 256, &mut avdp)?;

    // Check descriptor tag (tag ID = 2 for AVDP)
    let tag_id = u16::from_le_bytes([avdp[0], avdp[1]]);
    if tag_id != 2 {
        return Err(Error::DiscError { detail: format!("not UDF: tag {} at sector 256", tag_id) });
    }

    // Main VDS extent: bytes 16-23
    let mvds_lba = u32::from_le_bytes([avdp[16], avdp[17], avdp[18], avdp[19]]);
    let mvds_len = u32::from_le_bytes([avdp[20], avdp[21], avdp[22], avdp[23]]);

    // Read Volume Descriptor Sequence to find Partition Descriptor and Logical Volume Descriptor
    let mut partition_start: u32 = 0;
    let mut root_icb_lba: u32 = 0;

    let mvds_sectors = (mvds_len + 2047) / 2048;
    for i in 0..mvds_sectors.min(32) {
        let mut desc = [0u8; 2048];
        read_sector(session, mvds_lba + i, &mut desc)?;

        let desc_tag = u16::from_le_bytes([desc[0], desc[1]]);
        match desc_tag {
            5 => {
                // Partition Descriptor
                partition_start = u32::from_le_bytes([desc[188], desc[189], desc[190], desc[191]]);
            }
            6 => {
                // Logical Volume Descriptor — contains root FSD location
                // LV Contents Use at offset 248: extent of File Set Descriptor
                let fsd_lba = u32::from_le_bytes([desc[248], desc[249], desc[250], desc[251]]);
                root_icb_lba = fsd_lba;
            }
            8 => break, // Terminating Descriptor
            _ => continue,
        }
    }

    if partition_start == 0 {
        return Err(Error::DiscError { detail: "UDF: no partition descriptor found".into() });
    }

    // Read File Set Descriptor to get root directory ICB
    let mut fsd = [0u8; 2048];
    read_sector(session, partition_start + root_icb_lba, &mut fsd)?;

    let fsd_tag = u16::from_le_bytes([fsd[0], fsd[1]]);
    if fsd_tag != 256 {
        return Err(Error::DiscError { detail: format!("UDF: expected FSD (256), got tag {}", fsd_tag) });
    }

    // Root Directory ICB at offset 400 in FSD
    let root_dir_lba = u32::from_le_bytes([fsd[400], fsd[401], fsd[402], fsd[403]]);

    // Read root directory
    let root = read_directory(session, partition_start, root_dir_lba, "")?;

    Ok(UdfFs {
        root,
        partition_start,
    })
}

/// Read a UDF directory and its immediate children.
fn read_directory(session: &mut DriveSession, part_start: u32, dir_lba: u32, name: &str) -> Result<DirEntry> {
    // Read the ICB (Information Control Block) for this directory
    let mut icb = [0u8; 2048];
    read_sector(session, part_start + dir_lba, &mut icb)?;

    let icb_tag = u16::from_le_bytes([icb[0], icb[1]]);

    // File Entry (tag 261) or Extended File Entry (tag 266)
    let (alloc_offset, alloc_len) = match icb_tag {
        261 => {
            // File Entry
            let l_ea = u32::from_le_bytes([icb[168], icb[169], icb[170], icb[171]]) as usize;
            let l_ad = u32::from_le_bytes([icb[172], icb[173], icb[174], icb[175]]) as usize;
            (176 + l_ea, l_ad)
        }
        266 => {
            // Extended File Entry
            let l_ea = u32::from_le_bytes([icb[208], icb[209], icb[210], icb[211]]) as usize;
            let l_ad = u32::from_le_bytes([icb[212], icb[213], icb[214], icb[215]]) as usize;
            (216 + l_ea, l_ad)
        }
        _ => {
            return Ok(DirEntry {
                name: name.to_string(),
                is_dir: true,
                lba: dir_lba,
                size: 0,
                entries: Vec::new(),
            });
        }
    };

    // Parse allocation descriptors to find directory data location
    // Short Allocation Descriptor: 8 bytes (4 length + 4 position)
    let data_lba = if alloc_offset + 8 <= icb.len() {
        u32::from_le_bytes([icb[alloc_offset + 4], icb[alloc_offset + 5],
                           icb[alloc_offset + 6], icb[alloc_offset + 7]])
    } else {
        dir_lba + 1 // assume data follows ICB
    };

    let data_len = if alloc_offset + 4 <= icb.len() {
        u32::from_le_bytes([icb[alloc_offset], icb[alloc_offset + 1],
                           icb[alloc_offset + 2], icb[alloc_offset + 3]]) & 0x3FFFFFFF
    } else {
        2048
    };

    // Read directory data
    let sectors = ((data_len + 2047) / 2048).min(64) as usize;
    let mut dir_data = vec![0u8; sectors * 2048];
    for i in 0..sectors {
        read_sector(session, part_start + data_lba + i as u32,
                   &mut dir_data[i * 2048..(i + 1) * 2048])?;
    }

    // Parse File Identifier Descriptors
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + 38 < dir_data.len().min(data_len as usize) {
        let fid_tag = u16::from_le_bytes([dir_data[pos], dir_data[pos + 1]]);
        if fid_tag != 257 {
            break; // not a FID
        }

        let file_chars = dir_data[pos + 18];
        let l_fi = dir_data[pos + 19] as usize; // filename length
        let icb_lba = u32::from_le_bytes([dir_data[pos + 20], dir_data[pos + 21],
                                          dir_data[pos + 22], dir_data[pos + 23]]);
        let l_iu = u16::from_le_bytes([dir_data[pos + 36], dir_data[pos + 37]]) as usize;

        let name_offset = pos + 38 + l_iu;
        let is_dir = (file_chars & 0x02) != 0;
        let is_parent = (file_chars & 0x08) != 0;

        if !is_parent && l_fi > 0 && name_offset + l_fi <= dir_data.len() {
            let raw_name = &dir_data[name_offset..name_offset + l_fi];
            let entry_name = parse_udf_name(raw_name);

            if !entry_name.is_empty() {
                if is_dir {
                    // Recurse into subdirectory (max 2 levels deep for BDMV)
                    let subdir = read_directory(session, part_start, icb_lba, &entry_name)?;
                    entries.push(subdir);
                } else {
                    // Get file size from its ICB
                    let file_size = read_file_size(session, part_start, icb_lba).unwrap_or(0);
                    entries.push(DirEntry {
                        name: entry_name,
                        is_dir: false,
                        lba: icb_lba,
                        size: file_size,
                        entries: Vec::new(),
                    });
                }
            }
        }

        // Advance to next FID (4-byte aligned)
        let fid_len = 38 + l_iu + l_fi;
        let padded = (fid_len + 3) & !3;
        pos += padded;
    }

    Ok(DirEntry {
        name: name.to_string(),
        is_dir: true,
        lba: dir_lba,
        size: data_len,
        entries,
    })
}

/// Read file size from a File Entry ICB.
fn read_file_size(session: &mut DriveSession, part_start: u32, icb_lba: u32) -> Result<u32> {
    let mut icb = [0u8; 2048];
    read_sector(session, part_start + icb_lba, &mut icb)?;

    let tag = u16::from_le_bytes([icb[0], icb[1]]);
    match tag {
        261 => {
            // File Entry: info length at offset 56 (8 bytes LE)
            Ok(u32::from_le_bytes([icb[56], icb[57], icb[58], icb[59]]))
        }
        266 => {
            // Extended File Entry: info length at offset 56
            Ok(u32::from_le_bytes([icb[56], icb[57], icb[58], icb[59]]))
        }
        _ => Ok(0),
    }
}

/// Parse a UDF filename from raw bytes.
/// UDF uses either 8-bit or 16-bit encoding (first byte = compression ID).
fn parse_udf_name(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }

    match data[0] {
        8 => {
            // 8-bit characters
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

/// Read a single 2048-byte sector from the drive.
fn read_sector(session: &mut DriveSession, lba: u32, buf: &mut [u8]) -> Result<()> {
    session.read_disc(lba, 1, buf)?;
    Ok(())
}
