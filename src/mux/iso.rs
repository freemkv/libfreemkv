//! IsoStream — read/write Blu-ray ISO disc images.
//!
//! Read: parses UDF filesystem inside the ISO using the same pipeline as
//! DiscStream (titles, streams, labels, AACS). An ISO is a flat image of
//! 2048-byte sectors — sector N starts at byte offset N * 2048.
//!
//! Write: creates a sector-by-sector disc image from a SectorReader source.

use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::File;
use std::path::Path;
use super::IOStream;
use crate::disc::{Disc, DiscTitle, ScanOptions};
use crate::sector::SectorReader;
use crate::error::{Error, Result};

const SECTOR_SIZE: u64 = 2048;

/// File-backed sector reader for ISO images.
pub struct IsoSectorReader {
    file: File,
    capacity: u32,
}

impl IsoSectorReader {
    pub fn open(path: &str) -> io::Result<Self> {
        let file = File::open(Path::new(path))
            .map_err(|e| io::Error::new(e.kind(), format!("iso://{}: {}", path, e)))?;
        let size = file.metadata()?.len();
        let capacity = (size / SECTOR_SIZE) as u32;
        Ok(Self { file, capacity })
    }

    pub fn capacity(&self) -> u32 { self.capacity }
}

impl SectorReader for IsoSectorReader {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let bytes = count as usize * SECTOR_SIZE as usize;
        self.file.seek(SeekFrom::Start(lba as u64 * SECTOR_SIZE))
            .map_err(|e| Error::IoError { source: e })?;
        self.file.read_exact(&mut buf[..bytes])
            .map_err(|e| Error::IoError { source: e })?;
        Ok(bytes)
    }
}

/// Blu-ray ISO image stream.
///
/// Read: opens ISO, parses UDF (same as DiscStream), streams BD-TS content.
/// Write: receives sector data and writes to ISO file.
pub struct IsoStream {
    disc_title: DiscTitle,
    disc: Option<Disc>,
    reader: Option<IsoSectorReader>,
    writer: Option<io::BufWriter<File>>,
    /// Sector ranges to read: (start_lba, sector_count)
    extents: Vec<(u32, u32)>,
    extent_idx: usize,
    sectors_remaining: u32,
    sector_buf: [u8; SECTOR_SIZE as usize],
    buf_pos: usize,
    buf_len: usize,
    eof: bool,
}

impl IsoStream {
    /// Open an ISO file for reading. Parses UDF, scans titles, streams, labels.
    pub fn open(path: &str, title_index: Option<usize>, opts: &ScanOptions) -> io::Result<Self> {
        let mut reader = IsoSectorReader::open(path)?;
        let capacity = reader.capacity();

        let disc = Disc::scan_image(&mut reader, capacity, opts)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let idx = title_index.unwrap_or(0).min(disc.titles.len().saturating_sub(1));
        let disc_title = if disc.titles.is_empty() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no titles found in ISO image"));
        } else {
            disc.titles[idx].clone()
        };

        let extents: Vec<(u32, u32)> = disc_title.extents.iter()
            .map(|e| (e.start_lba, e.sector_count))
            .collect();

        let sectors_remaining = extents.first().map(|e| e.1).unwrap_or(0);

        Ok(IsoStream {
            disc_title,
            disc: Some(disc),
            reader: Some(reader),
            writer: None,
            extents,
            extent_idx: 0,
            sectors_remaining,
            sector_buf: [0u8; SECTOR_SIZE as usize],
            buf_pos: 0,
            buf_len: 0,
            eof: false,
        })
    }

    /// Create an ISO file for writing. Receives raw sector data.
    pub fn create(path: &str) -> io::Result<Self> {
        let file = File::create(Path::new(path))
            .map_err(|e| io::Error::new(e.kind(), format!("iso://{}: {}", path, e)))?;
        let writer = io::BufWriter::with_capacity(4 * 1024 * 1024, file);

        Ok(IsoStream {
            disc_title: DiscTitle::empty(),
            disc: None,
            reader: None,
            writer: Some(writer),
            extents: Vec::new(),
            extent_idx: 0,
            sectors_remaining: 0,
            sector_buf: [0u8; SECTOR_SIZE as usize],
            buf_pos: 0,
            buf_len: 0,
            eof: false,
        })
    }

    /// Set metadata (for write mode).
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    /// Get the full Disc (for listing all titles).
    pub fn disc(&self) -> Option<&Disc> { self.disc.as_ref() }

    /// Read the next sector from the current extent.
    fn read_next_sector(&mut self) -> io::Result<bool> {
        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => return Ok(false),
        };

        if self.extent_idx >= self.extents.len() {
            return Ok(false);
        }

        let (start_lba, total) = self.extents[self.extent_idx];
        let offset = total - self.sectors_remaining;
        let lba = start_lba + offset;

        reader.read_sectors(lba, 1, &mut self.sector_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
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
    fn finish(&mut self) -> io::Result<()> {
        if let Some(ref mut w) = self.writer {
            w.flush()?;
        }
        Ok(())
    }
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
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer.as_mut() {
            Some(w) => w.write(buf),
            None => Err(io::Error::new(io::ErrorKind::Unsupported,
                "iso:// opened for reading — cannot write")),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self.writer.as_mut() {
            Some(w) => w.flush(),
            None => Ok(()),
        }
    }
}
