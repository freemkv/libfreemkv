//! IsoStream — read BD-TS data from a Blu-ray ISO image file.
//!
//! Read-only. Parses the UDF filesystem inside the ISO to find
//! BDMV/STREAM/*.m2ts files, then streams the BD-TS bytes.
//!
//! An ISO file is a flat image of 2048-byte sectors — the same
//! layout as on a real disc. Sector N starts at byte offset N * 2048.

use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::File;
use std::path::Path;
use super::IOStream;
use crate::disc::DiscTitle;

const SECTOR_SIZE: u64 = 2048;

/// Blu-ray ISO image stream. Read-only.
///
/// Opens an ISO file, parses UDF to locate BDMV playlists and streams,
/// then reads the m2ts content sectors in order.
pub struct IsoStream {
    disc_title: DiscTitle,
    file: File,
    /// Sector ranges to read: (start_lba, sector_count)
    extents: Vec<(u64, u64)>,
    /// Current extent index
    extent_idx: usize,
    /// Sectors remaining in current extent
    sectors_remaining: u64,
    /// Read buffer for one sector
    sector_buf: [u8; SECTOR_SIZE as usize],
    /// Position within current sector buffer
    buf_pos: usize,
    /// Bytes valid in sector buffer
    buf_len: usize,
    eof: bool,
}

impl IsoStream {
    /// Open an ISO file and scan its contents.
    ///
    /// Parses UDF filesystem, finds playlists and stream extents.
    /// The title_index selects which title to read (0-based, default: longest).
    pub fn open(path: &str, title_index: Option<usize>) -> io::Result<Self> {
        let file = File::open(Path::new(path))
            .map_err(|e| io::Error::new(e.kind(),
                format!("iso://{}: {}", path, e)))?;

        let mut stream = IsoStream {
            disc_title: DiscTitle::empty(),
            file,
            extents: Vec::new(),
            extent_idx: 0,
            sectors_remaining: 0,
            sector_buf: [0u8; SECTOR_SIZE as usize],
            buf_pos: 0,
            buf_len: 0,
            eof: false,
        };

        stream.scan_iso(title_index)?;
        Ok(stream)
    }

    /// Scan the ISO: parse UDF, build title metadata, extract extent map.
    fn scan_iso(&mut self, title_index: Option<usize>) -> io::Result<()> {
        // Read AVDP at sector 256 to verify this is a UDF disc image
        let avdp = self.read_sector(256)?;
        let tag_id = u16::from_le_bytes([avdp[0], avdp[1]]);
        if tag_id != 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                "not a valid UDF image — no AVDP at sector 256"));
        }

        // For now, scan BDMV/PLAYLIST and BDMV/STREAM directories
        // by searching for MPLS and M2TS markers in the UDF metadata.
        //
        // Full UDF parsing (AVDP → VDS → metadata → FSD → root → files)
        // will be refactored out of udf.rs to work with both DriveSession
        // and file-backed sector reads. For now, find the main m2ts file
        // by scanning for the stream file extents in the UDF file entries.

        // Find all .m2ts file extents from UDF metadata
        let disc_size = self.file.seek(SeekFrom::End(0))?;
        let total_sectors = disc_size / SECTOR_SIZE;
        self.file.seek(SeekFrom::Start(0))?;

        // Scan UDF partition for BDMV structure
        let titles = self.find_stream_extents(total_sectors)?;

        if titles.is_empty() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                "no BD stream files found in ISO image"));
        }

        // Select title
        let idx = title_index.unwrap_or(0).min(titles.len() - 1);
        let (title, extents) = &titles[idx];

        self.disc_title = title.clone();
        self.extents = extents.clone();

        if !self.extents.is_empty() {
            self.sectors_remaining = self.extents[0].1;
        }

        Ok(())
    }

    /// Read a single sector from the ISO file.
    fn read_sector(&mut self, lba: u64) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; SECTOR_SIZE as usize];
        self.file.seek(SeekFrom::Start(lba * SECTOR_SIZE))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Scan the ISO for BD stream file extents.
    ///
    /// Returns: Vec of (DiscTitle, Vec<(start_lba, sector_count)>)
    ///
    /// This is a simplified scanner that finds m2ts content by looking
    /// for 192-byte BD-TS packet boundaries (0x47 sync byte at offset 4).
    /// Full UDF parsing will replace this once udf.rs is decoupled from DriveSession.
    fn find_stream_extents(&mut self, total_sectors: u64) -> io::Result<Vec<(DiscTitle, Vec<(u64, u64)>)>> {
        // Strategy: scan the UDF file entry area for allocation descriptors
        // pointing to large contiguous regions (m2ts files are large).
        //
        // For a BD-ROM ISO, the main m2ts typically starts after the BDMV
        // metadata (around sector 1000-5000) and runs contiguously to the end.
        //
        // Quick approach: find first sector with BD-TS sync (0x47 at byte 4)
        // and treat everything from there to the end as one extent.

        let probe_start = 256u64; // skip lead-in
        let probe_end = total_sectors.min(10000); // probe first 20 MB

        let mut stream_start: Option<u64> = None;

        for lba in probe_start..probe_end {
            let sector = self.read_sector(lba)?;
            // BD-TS: 192-byte packets, sync byte 0x47 at offset 4 of each packet
            // A sector (2048 bytes) holds partial packets, but the sync pattern
            // should appear at regular intervals
            if sector.len() >= 196 && sector[4] == 0x47 {
                // Verify: check for another sync at offset 196 (4 + 192)
                if sector[196] == 0x47 {
                    stream_start = Some(lba);
                    break;
                }
            }
        }

        match stream_start {
            Some(start) => {
                let sector_count = total_sectors - start;
                let size_bytes = sector_count * SECTOR_SIZE;

                let mut title = DiscTitle::empty();
                title.playlist = "Main Title".into();
                title.size_bytes = size_bytes;

                Ok(vec![(title, vec![(start, sector_count)])])
            }
            None => Ok(Vec::new()),
        }
    }

    /// Read the next sector from the current extent.
    fn read_next_sector(&mut self) -> io::Result<bool> {
        if self.extent_idx >= self.extents.len() {
            return Ok(false);
        }

        let (start_lba, _) = self.extents[self.extent_idx];
        let offset = self.extents[self.extent_idx].1 - self.sectors_remaining;
        let lba = start_lba + offset;

        self.file.seek(SeekFrom::Start(lba * SECTOR_SIZE))?;
        self.file.read_exact(&mut self.sector_buf)?;
        self.buf_pos = 0;
        self.buf_len = SECTOR_SIZE as usize;

        self.sectors_remaining -= 1;
        if self.sectors_remaining == 0 {
            self.extent_idx += 1;
            if self.extent_idx < self.extents.len() {
                self.sectors_remaining = self.extents[self.extent_idx].1;
            }
        }

        Ok(true)
    }
}

impl IOStream for IsoStream {
    fn info(&self) -> &DiscTitle { &self.disc_title }
    fn finish(&mut self) -> io::Result<()> { Ok(()) }
}

impl Read for IsoStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.eof { return Ok(0); }

        // Drain current sector buffer
        if self.buf_pos < self.buf_len {
            let n = (self.buf_len - self.buf_pos).min(buf.len());
            buf[..n].copy_from_slice(&self.sector_buf[self.buf_pos..self.buf_pos + n]);
            self.buf_pos += n;
            return Ok(n);
        }

        // Read next sector
        if self.read_next_sector()? {
            let n = self.buf_len.min(buf.len());
            buf[..n].copy_from_slice(&self.sector_buf[..n]);
            self.buf_pos = n;
            Ok(n)
        } else {
            self.eof = true;
            Ok(0)
        }
    }
}

impl Write for IsoStream {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::Unsupported,
            "iso:// is read-only"))
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
