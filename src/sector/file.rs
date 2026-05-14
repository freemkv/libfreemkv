//! File-backed sector sink — write 2048-byte sectors to an ISO image
//! on disk.
//!
//! The read-side counterpart ([`crate::io::file_sector_source::FileSectorSource`])
//! lives under `io/` because its internals (read-ahead buffer, per-OS
//! `fadvise`/`F_RDADVISE` hints) are I/O infrastructure rather than
//! sector-trait business logic. Both types remain re-exported at
//! [`crate::sector`] for ergonomic imports.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};

use super::SectorSink;

/// SectorSink backed by a file (ISO image).
///
/// Writes go through [`crate::io::WritebackFile`], which on Linux drives
/// continuous `sync_file_range` + `posix_fadvise(DONTNEED)` to keep
/// the kernel dirty page cache bounded during multi-GB sequential
/// writes. macOS / Windows fall through to a no-op pipeline.
///
/// `finish` runs `sync_all` before dropping the underlying file.
pub struct FileSectorSink {
    inner: crate::io::WritebackFile,
}

impl FileSectorSink {
    /// Create a new ISO file at `path`, truncating any existing
    /// file. The file is opened read-write so the same handle can
    /// later be reused for verification reads if needed (sweep
    /// doesn't, but it costs nothing here).
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            inner: crate::io::WritebackFile::new(file)?,
        })
    }

    /// Open an existing ISO file for in-place updates (e.g. patch
    /// pass writing recovered sectors over zero-filled holes).
    /// Does not truncate.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self {
            inner: crate::io::WritebackFile::new(file)?,
        })
    }
}

impl SectorSink for FileSectorSink {
    fn write_sectors(&mut self, lba: u32, buf: &[u8]) -> Result<()> {
        debug_assert!(
            buf.len() % 2048 == 0,
            "FileSectorSink::write_sectors: buf len {} not a multiple of 2048",
            buf.len()
        );
        let offset = lba as u64 * 2048;
        self.inner
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::IoError { source: e })?;
        self.inner
            .write_all(buf)
            .map_err(|e| Error::IoError { source: e })?;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.inner
            .sync_all()
            .map_err(|e| Error::IoError { source: e })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FileSectorSink;
    use crate::io::file_sector_source::FileSectorSource;
    use crate::sector::{SectorSink, SectorSource};
    use tempfile::tempdir;

    #[test]
    fn round_trip_single_sector() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("rt.iso");

        let mut sink = FileSectorSink::create(&path).unwrap();
        // Pre-extend the file to 4 sectors of zeros so we can write
        // sector 2 in place. Easiest way: write zeros first.
        let zeros = [0u8; 4 * 2048];
        sink.write_sectors(0, &zeros).unwrap();

        let mut payload = [0u8; 2048];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17);
        }
        sink.write_sectors(2, &payload).unwrap();
        Box::new(sink).finish().unwrap();

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), 4);

        let mut got = [0u8; 2048];
        let n = src.read_sectors(2, 1, &mut got, false).unwrap();
        assert_eq!(n, 2048);
        assert_eq!(got, payload);

        // Sectors 0,1,3 still zero.
        let mut z = [0xffu8; 2048];
        src.read_sectors(0, 1, &mut z, false).unwrap();
        assert!(z.iter().all(|b| *b == 0));
    }

    #[test]
    fn round_trip_multi_sector() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.iso");

        let mut sink = FileSectorSink::create(&path).unwrap();
        let mut payload = vec![0u8; 8 * 2048];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = ((i * 31) ^ (i >> 7)) as u8;
        }
        sink.write_sectors(0, &payload).unwrap();
        Box::new(sink).finish().unwrap();

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), 8);

        let mut got = vec![0u8; 8 * 2048];
        let n = src.read_sectors(0, 8, &mut got, false).unwrap();
        assert_eq!(n, 8 * 2048);
        assert_eq!(got, payload);
    }

    #[test]
    fn open_existing_does_not_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open.iso");

        // Create with 4 sectors of pattern A.
        let mut sink = FileSectorSink::create(&path).unwrap();
        let pat_a = [0xaau8; 4 * 2048];
        sink.write_sectors(0, &pat_a).unwrap();
        Box::new(sink).finish().unwrap();

        // Reopen and overwrite sector 1 only.
        let mut sink = FileSectorSink::open(&path).unwrap();
        let pat_b = [0xbbu8; 2048];
        sink.write_sectors(1, &pat_b).unwrap();
        Box::new(sink).finish().unwrap();

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), 4);
        let mut got = [0u8; 2048];

        src.read_sectors(0, 1, &mut got, false).unwrap();
        assert_eq!(got, [0xaau8; 2048]);

        src.read_sectors(1, 1, &mut got, false).unwrap();
        assert_eq!(got, [0xbbu8; 2048]);

        src.read_sectors(2, 1, &mut got, false).unwrap();
        assert_eq!(got, [0xaau8; 2048]);
    }
}
