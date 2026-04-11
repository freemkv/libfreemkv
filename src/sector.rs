//! SectorReader — trait for reading 2048-byte disc sectors.
//!
//! Implemented by DriveSession (SCSI) and IsoFile (file-backed).
//! Used by UDF parser, disc scanner, label parsers — anything that
//! reads sectors doesn't need to know where they come from.

use crate::error::Result;

/// Read 2048-byte sectors from a disc or disc image.
pub trait SectorReader {
    /// Read `count` sectors starting at `lba` into `buf`.
    /// `buf` must be at least `count * 2048` bytes.
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize>;
}
