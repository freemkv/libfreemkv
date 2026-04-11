//! UDF ISO writer — creates Blu-ray disc images.
//!
//! Writes a minimal UDF 2.50 filesystem containing BDMV/STREAM/*.m2ts.
//! The ISO can be mounted or read back via IsoStream.
//!
//! Layout:
//!   Sector 0-15:    System area (zeros)
//!   Sector 16-18:   Volume Recognition Sequence (BEA01, NSR03, TEA01)
//!   Sector 32-37:   Volume Descriptor Sequence
//!   Sector 256:     Anchor Volume Descriptor Pointer
//!   Sector 260-271: Metadata partition (FSD, ICBs, directories)
//!   Sector 288+:    File data (m2ts content)
//!   Last-256:       Reserve AVDP

use std::io::{self, Seek, SeekFrom, Write};

const SECTOR_SIZE: u64 = 2048;

// Layout constants
const VRS_START: u32 = 16; // Volume Recognition Sequence
const VDS_START: u32 = 32; // Volume Descriptor Sequence
const AVDP_SECTOR: u32 = 256; // Anchor Volume Descriptor Pointer
const PARTITION_START: u32 = 257; // Physical partition start
const METADATA_START: u32 = 260; // Metadata partition content
const FSD_SECTOR: u32 = 260; // File Set Descriptor
const ROOT_ICB_SECTOR: u32 = 261; // Root directory ICB
const ROOT_DIR_SECTOR: u32 = 262; // Root directory data
const BDMV_ICB_SECTOR: u32 = 263; // BDMV/ ICB
const BDMV_DIR_SECTOR: u32 = 264; // BDMV/ directory data
const STREAM_ICB_SECTOR: u32 = 265; // BDMV/STREAM/ ICB
const STREAM_DIR_SECTOR: u32 = 266; // BDMV/STREAM/ directory data
const M2TS_ICB_SECTOR: u32 = 267; // m2ts file ICB
const DATA_START: u32 = 288; // Start of file data (aligned)

/// Write a complete BD ISO image.
///
/// Writes UDF structure, then streams m2ts content from the writer.
/// Call `start()` first, then write BD-TS bytes, then call `finish()`.
pub struct IsoWriter<W: Write + Seek> {
    writer: W,
    volume_id: String,
    m2ts_name: String,
    data_start_sector: u32,
    bytes_written: u64,
}

impl<W: Write + Seek> IsoWriter<W> {
    /// Create a new ISO writer. Call `start()` to write the UDF header.
    pub fn new(writer: W, volume_id: &str, m2ts_name: &str) -> Self {
        Self {
            writer,
            volume_id: volume_id.to_string(),
            m2ts_name: m2ts_name.to_string(),
            data_start_sector: DATA_START,
            bytes_written: 0,
        }
    }

    /// Update volume ID and m2ts filename. Must be called before `start()`.
    pub fn with_names(mut self, volume_id: &str, m2ts_name: &str) -> Self {
        self.volume_id = volume_id.to_string();
        self.m2ts_name = m2ts_name.to_string();
        self
    }

    /// Write UDF filesystem header. After this, write m2ts content bytes.
    pub fn start(&mut self) -> io::Result<()> {
        // System area: sectors 0-15 (zeros)
        let zero_sector = [0u8; SECTOR_SIZE as usize];
        for _ in 0..VRS_START {
            self.writer.write_all(&zero_sector)?;
        }

        // Volume Recognition Sequence
        self.write_vrs()?;

        // Pad sectors 19-31
        for _ in 19..VDS_START {
            self.writer.write_all(&zero_sector)?;
        }

        // Volume Descriptor Sequence (sectors 32-37)
        self.write_vds()?;

        // Pad sectors 38-255
        for _ in 38..AVDP_SECTOR {
            self.writer.write_all(&zero_sector)?;
        }

        // AVDP at sector 256
        self.write_avdp()?;

        // Partition area: metadata file ICB at partition_start
        self.write_metadata_file_icb()?;

        // Pad to metadata start
        for _ in (PARTITION_START + 1)..METADATA_START {
            self.writer.write_all(&zero_sector)?;
        }

        // Metadata partition
        self.write_fsd()?;
        self.write_root_icb()?;
        self.write_root_dir()?;
        self.write_bdmv_icb()?;
        self.write_bdmv_dir()?;
        self.write_stream_icb()?;
        self.write_stream_dir()?;
        self.write_m2ts_icb(0)?; // placeholder size, updated in finish()

        // Pad to data start
        for _ in (M2TS_ICB_SECTOR + 1)..self.data_start_sector {
            self.writer.write_all(&zero_sector)?;
        }

        Ok(())
    }

    /// Write m2ts content bytes. Call after `start()`.
    pub fn write_data(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    /// Finalize the ISO: pad to sector boundary, update file sizes, write reserve AVDP.
    pub fn finish(&mut self) -> io::Result<()> {
        // Pad to sector boundary
        let remainder = (self.bytes_written % SECTOR_SIZE) as usize;
        if remainder > 0 {
            let pad = SECTOR_SIZE as usize - remainder;
            let zeros = vec![0u8; pad];
            self.writer.write_all(&zeros)?;
            self.bytes_written += pad as u64;
        }

        let total_data_sectors = (self.bytes_written / SECTOR_SIZE) as u32;
        let total_sectors = self.data_start_sector + total_data_sectors;

        // Seek back and update m2ts file ICB with actual size
        self.writer
            .seek(SeekFrom::Start(M2TS_ICB_SECTOR as u64 * SECTOR_SIZE))?;
        self.write_m2ts_icb(self.bytes_written)?;

        // Seek to end and write reserve AVDP
        let reserve_sector = total_sectors;
        self.writer
            .seek(SeekFrom::Start(reserve_sector as u64 * SECTOR_SIZE))?;
        self.write_avdp()?;

        self.writer.flush()?;
        Ok(())
    }

    // ── UDF structure writers ──────────────────────────────────────────────

    fn write_vrs(&mut self) -> io::Result<()> {
        // BEA01 at sector 16
        let mut bea = [0u8; SECTOR_SIZE as usize];
        bea[0] = 0; // structure type
        bea[1..6].copy_from_slice(b"BEA01");
        bea[6] = 1; // structure version
        self.writer.write_all(&bea)?;

        // NSR03 at sector 17 (UDF 2.50)
        let mut nsr = [0u8; SECTOR_SIZE as usize];
        nsr[0] = 0;
        nsr[1..6].copy_from_slice(b"NSR03");
        nsr[6] = 1;
        self.writer.write_all(&nsr)?;

        // TEA01 at sector 18
        let mut tea = [0u8; SECTOR_SIZE as usize];
        tea[0] = 0;
        tea[1..6].copy_from_slice(b"TEA01");
        tea[6] = 1;
        self.writer.write_all(&tea)?;

        Ok(())
    }

    fn write_vds(&mut self) -> io::Result<()> {
        // Primary Volume Descriptor (tag 1) at sector 32
        let mut pvd = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut pvd, 1, VDS_START);
        // Volume Identifier at offset 24 (32-byte d-string)
        write_dstring(&mut pvd[24..56], &self.volume_id);
        self.writer.write_all(&pvd)?;

        // Partition Descriptor (tag 5) at sector 33
        let mut pd = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut pd, 5, VDS_START + 1);
        // Partition starting location at offset 188
        pd[188..192].copy_from_slice(&PARTITION_START.to_le_bytes());
        // Partition length (large enough for everything)
        let part_len: u32 = 0xFFFFFFFF;
        pd[192..196].copy_from_slice(&part_len.to_le_bytes());
        self.writer.write_all(&pd)?;

        // Logical Volume Descriptor (tag 6) at sector 34
        let mut lvd = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut lvd, 6, VDS_START + 2);
        // Logical block size at offset 212
        lvd[212..216].copy_from_slice(&2048u32.to_le_bytes());
        // Number of partition maps at offset 268
        lvd[268..272].copy_from_slice(&2u32.to_le_bytes());
        // Partition map 1: Type 1 (physical), 6 bytes
        lvd[440] = 1; // type
        lvd[441] = 6; // length
                      // Partition map 2: Type 2 (metadata), 64 bytes
        lvd[446] = 2; // type
        lvd[447] = 64; // length
                       // Entity ID for metadata partition
        lvd[450..473].copy_from_slice(b"*UDF Metadata Partition");
        self.writer.write_all(&lvd)?;

        // Unallocated Space Descriptor (tag 7) at sector 35
        let mut usd = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut usd, 7, VDS_START + 3);
        self.writer.write_all(&usd)?;

        // Implementation Use Volume Descriptor (tag 4) at sector 36
        let mut iuvd = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut iuvd, 4, VDS_START + 4);
        self.writer.write_all(&iuvd)?;

        // Terminating Descriptor (tag 8) at sector 37
        let mut td = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut td, 8, VDS_START + 5);
        self.writer.write_all(&td)?;

        Ok(())
    }

    fn write_avdp(&mut self) -> io::Result<()> {
        let mut avdp = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut avdp, 2, AVDP_SECTOR);
        // Main VDS extent_ad: {length, location} per UDF spec
        avdp[16..20].copy_from_slice(&(6u32 * 2048).to_le_bytes()); // length
        avdp[20..24].copy_from_slice(&VDS_START.to_le_bytes()); // location
        // Reserve VDS extent_ad (same as main for simplicity)
        avdp[24..28].copy_from_slice(&(6u32 * 2048).to_le_bytes()); // length
        avdp[28..32].copy_from_slice(&VDS_START.to_le_bytes()); // location
        self.writer.write_all(&avdp)?;
        Ok(())
    }

    fn write_metadata_file_icb(&mut self) -> io::Result<()> {
        // Extended File Entry (tag 266) at partition_start
        // Points to metadata content at METADATA_START
        let mut icb = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut icb, 266, PARTITION_START);
        // ICB tag at offset 16
        icb[16..20].copy_from_slice(&0u32.to_le_bytes()); // prior recorded
        icb[20..22].copy_from_slice(&0u16.to_le_bytes()); // strategy type
        icb[22..24].copy_from_slice(&0u16.to_le_bytes()); // strategy parameter
                                                          // File type at offset 27: 250 = metadata file
        icb[27] = 250;
        // Information length at offset 56
        let meta_len: u64 = 12 * SECTOR_SIZE; // 12 sectors of metadata
        icb[56..64].copy_from_slice(&meta_len.to_le_bytes());
        // Extended attribute length at offset 208
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        // Allocation descriptor at offset 216: short_ad (length + position)
        let ad_len = meta_len as u32;
        let ad_pos = METADATA_START - PARTITION_START; // relative to partition
        icb[216..220].copy_from_slice(&ad_len.to_le_bytes());
        icb[220..224].copy_from_slice(&ad_pos.to_le_bytes());
        self.writer.write_all(&icb)?;
        Ok(())
    }

    fn write_fsd(&mut self) -> io::Result<()> {
        let mut fsd = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut fsd, 256, FSD_SECTOR);
        // Root Directory ICB (long_ad at offset 400)
        let root_lba = ROOT_ICB_SECTOR - METADATA_START; // metadata-relative
        fsd[400..404].copy_from_slice(&SECTOR_SIZE.to_le_bytes()[..4]); // extent length
        fsd[404..408].copy_from_slice(&root_lba.to_le_bytes());
        self.writer.write_all(&fsd)?;
        Ok(())
    }

    fn write_root_icb(&mut self) -> io::Result<()> {
        let mut icb = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut icb, 266, ROOT_ICB_SECTOR);
        icb[27] = 4; // file type: directory
        let dir_len: u64 = SECTOR_SIZE;
        icb[56..64].copy_from_slice(&dir_len.to_le_bytes());
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        let ad_pos = ROOT_DIR_SECTOR - METADATA_START;
        icb[216..220].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
        icb[220..224].copy_from_slice(&ad_pos.to_le_bytes());
        self.writer.write_all(&icb)?;
        Ok(())
    }

    fn write_root_dir(&mut self) -> io::Result<()> {
        let mut dir = [0u8; SECTOR_SIZE as usize];
        let mut offset = 0;
        // Parent entry (.. points to self)
        offset += write_fid(
            &mut dir[offset..],
            ROOT_ICB_SECTOR - METADATA_START,
            "",
            true,
        );
        // BDMV directory entry
        offset += write_fid(
            &mut dir[offset..],
            BDMV_ICB_SECTOR - METADATA_START,
            "BDMV",
            false,
        );
        let _ = offset;
        self.writer.write_all(&dir)?;
        Ok(())
    }

    fn write_bdmv_icb(&mut self) -> io::Result<()> {
        let mut icb = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut icb, 266, BDMV_ICB_SECTOR);
        icb[27] = 4; // directory
        let dir_len: u64 = SECTOR_SIZE;
        icb[56..64].copy_from_slice(&dir_len.to_le_bytes());
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        let ad_pos = BDMV_DIR_SECTOR - METADATA_START;
        icb[216..220].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
        icb[220..224].copy_from_slice(&ad_pos.to_le_bytes());
        self.writer.write_all(&icb)?;
        Ok(())
    }

    fn write_bdmv_dir(&mut self) -> io::Result<()> {
        let mut dir = [0u8; SECTOR_SIZE as usize];
        let mut offset = 0;
        offset += write_fid(
            &mut dir[offset..],
            ROOT_ICB_SECTOR - METADATA_START,
            "",
            true,
        );
        offset += write_fid(
            &mut dir[offset..],
            STREAM_ICB_SECTOR - METADATA_START,
            "STREAM",
            false,
        );
        let _ = offset;
        self.writer.write_all(&dir)?;
        Ok(())
    }

    fn write_stream_icb(&mut self) -> io::Result<()> {
        let mut icb = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut icb, 266, STREAM_ICB_SECTOR);
        icb[27] = 4; // directory
        let dir_len: u64 = SECTOR_SIZE;
        icb[56..64].copy_from_slice(&dir_len.to_le_bytes());
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        let ad_pos = STREAM_DIR_SECTOR - METADATA_START;
        icb[216..220].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
        icb[220..224].copy_from_slice(&ad_pos.to_le_bytes());
        self.writer.write_all(&icb)?;
        Ok(())
    }

    fn write_stream_dir(&mut self) -> io::Result<()> {
        let mut dir = [0u8; SECTOR_SIZE as usize];
        let mut offset = 0;
        offset += write_fid(
            &mut dir[offset..],
            BDMV_ICB_SECTOR - METADATA_START,
            "",
            true,
        );
        offset += write_fid(
            &mut dir[offset..],
            M2TS_ICB_SECTOR - METADATA_START,
            &self.m2ts_name,
            false,
        );
        let _ = offset;
        self.writer.write_all(&dir)?;
        Ok(())
    }

    fn write_m2ts_icb(&mut self, file_size: u64) -> io::Result<()> {
        let mut icb = [0u8; SECTOR_SIZE as usize];
        write_descriptor_tag(&mut icb, 266, M2TS_ICB_SECTOR);
        icb[27] = 5; // file type: regular file
        icb[56..64].copy_from_slice(&file_size.to_le_bytes());
        icb[208..212].copy_from_slice(&0u32.to_le_bytes());
        // Allocation: data starts at DATA_START in the physical partition
        let data_offset = self.data_start_sector - PARTITION_START;
        // Cap allocation length at u32::MAX for files >4GB (UDF short_ad limitation)
        let ad_len = if file_size > u32::MAX as u64 { u32::MAX } else { file_size as u32 };
        icb[216..220].copy_from_slice(&ad_len.to_le_bytes());
        icb[220..224].copy_from_slice(&data_offset.to_le_bytes());
        self.writer.write_all(&icb)?;
        Ok(())
    }
}

// ── UDF primitives ─────────────────────────────────────────────────────────

/// Write a UDF Descriptor Tag at the start of a sector.
fn write_descriptor_tag(buf: &mut [u8], tag_id: u16, sector: u32) {
    buf[0..2].copy_from_slice(&tag_id.to_le_bytes());
    // Descriptor version: 3 (UDF 2.50)
    buf[2..4].copy_from_slice(&3u16.to_le_bytes());
    // Tag serial number
    buf[4] = 0;
    // Descriptor CRC (simplified — set to 0, most implementations accept this)
    buf[8..10].copy_from_slice(&0u16.to_le_bytes());
    // Descriptor CRC length
    buf[10..12].copy_from_slice(&0u16.to_le_bytes());
    // Tag location
    buf[12..16].copy_from_slice(&sector.to_le_bytes());
}

/// Write a UDF d-string (compressed unicode string with length prefix).
fn write_dstring(buf: &mut [u8], s: &str) {
    let max = buf.len() - 1; // last byte is length
    let bytes = s.as_bytes();
    let len = bytes.len().min(max);
    if len > 0 {
        buf[0] = 8; // compression ID: 8 = Latin-1
        buf[1..1 + len].copy_from_slice(&bytes[..len]);
        buf[buf.len() - 1] = (len + 1) as u8; // d-string length including comp ID
    }
}

/// Write a File Identifier Descriptor. Returns bytes written (4-byte aligned).
fn write_fid(buf: &mut [u8], icb_lba: u32, name: &str, is_parent: bool) -> usize {
    // Tag 257 = File Identifier Descriptor
    let name_bytes = name.as_bytes();
    let name_len = if is_parent { 0 } else { name_bytes.len() + 1 }; // +1 for comp ID
    let fid_len = 38 + name_len; // fixed header + identifier
    let padded = (fid_len + 3) & !3; // 4-byte align

    if padded > buf.len() {
        return 0;
    }

    // Tag
    buf[0..2].copy_from_slice(&257u16.to_le_bytes());
    // File version number at offset 16
    buf[16..18].copy_from_slice(&1u16.to_le_bytes());
    // File characteristics at offset 18
    buf[18] = if is_parent { 0x0A } else { 0x02 }; // parent | directory
    if !is_parent && !name.contains('.') {
        buf[18] = 0x02; // directory
    } else if !is_parent {
        buf[18] = 0x00; // file
    }
    // ICB (long_ad at offset 20): extent length + location
    buf[20..24].copy_from_slice(&(SECTOR_SIZE as u32).to_le_bytes());
    buf[24..28].copy_from_slice(&icb_lba.to_le_bytes());
    // Identifier length at offset 36
    buf[36] = name_len as u8;
    // Implementation use length at offset 37
    buf[37] = 0;
    // File identifier at offset 38
    if !is_parent && !name_bytes.is_empty() {
        buf[38] = 8; // compression ID: Latin-1
        buf[39..39 + name_bytes.len()].copy_from_slice(name_bytes);
    }

    padded
}
