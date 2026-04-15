//! SectorReader — trait for reading 2048-byte disc sectors.
//!
//! Implemented by Drive (SCSI) and IsoFile (file-backed).
//! Used by UDF parser, disc scanner, label parsers — anything that
//! reads sectors doesn't need to know where they come from.

use crate::error::Result;

/// Read 2048-byte sectors from a disc or disc image.
pub trait SectorReader: Send {
    /// Read `count` sectors starting at `lba` into `buf`.
    /// `buf` must be at least `count * 2048` bytes.
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize>;

    /// Total capacity in sectors, if known.
    fn capacity(&self) -> u32 { 0 }
}

/// SectorReader backed by a file (ISO image).
/// Seeks to lba * 2048, reads count * 2048 bytes.
pub struct FileSectorReader {
    file: std::io::BufReader<std::fs::File>,
    capacity: u32,
}

impl FileSectorReader {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        let capacity = (len / 2048) as u32;
        Ok(Self {
            file: std::io::BufReader::with_capacity(4 * 1024 * 1024, file),
            capacity,
        })
    }
}

impl SectorReader for FileSectorReader {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        use std::io::{Read, Seek, SeekFrom};
        let offset = lba as u64 * 2048;
        let bytes = count as usize * 2048;
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| crate::error::Error::IoError { source: e })?;
        self.file.read_exact(&mut buf[..bytes])
            .map_err(|e| crate::error::Error::IoError { source: e })?;
        Ok(bytes)
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}
