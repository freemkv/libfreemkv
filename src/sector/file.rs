//! File-backed sector I/O — read and write 2048-byte sectors against
//! an ISO image on disk.
//!
//! [`FileSectorSource`] is the read side (open-only). [`FileSectorSink`]
//! is the write side (create or open-rw); writes go through
//! [`crate::io::Writer`] so big sequential ISO writes share the
//! same bounded-cache writeback pipeline used by sweep / patch /
//! mux. `Writer` is the 0.17 name; the 0.18 redesign renames it
//! to `WritebackFile` in a separate slice — this file deliberately
//! imports through the `crate::io::Writer` path so the rename can
//! be applied independently.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};

use super::{SectorReader, SectorSink};

/// SectorSource backed by a file (ISO image).
///
/// Seeks to `lba * 2048`, reads `count * 2048` bytes per call. The
/// underlying file is wrapped in a 4 MiB `BufReader` so adjacent
/// small reads coalesce into single syscalls.
pub struct FileSectorSource {
    file: BufReader<File>,
    capacity: u32,
}

impl FileSectorSource {
    /// Open an existing ISO file for reading. Capacity is derived
    /// from `metadata().len() / 2048`. Returns
    /// [`Error::IsoTooLarge`] if the file would exceed the 32-bit
    /// LBA address space (~8 TB).
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let sectors = len / 2048;
        if sectors > u32::MAX as u64 {
            return Err(Error::IsoTooLarge {
                path: path.to_string(),
            }
            .into());
        }
        let capacity = sectors as u32;
        Ok(Self {
            file: BufReader::with_capacity(4 * 1024 * 1024, file),
            capacity,
        })
    }
}

// Implement the legacy `SectorReader` trait. The blanket impl in
// `super` produces the `SectorSource` impl automatically — no need
// to write both, and writing both would conflict. This keeps the
// 0.17 method-resolution path intact (callers with `SectorReader`
// in scope can still write `fsr.read_sectors(..)` against a
// `FileSectorSource`).
impl SectorReader for FileSectorSource {
    fn capacity(&self) -> u32 {
        self.capacity
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let offset = lba as u64 * 2048;
        let bytes = count as usize * 2048;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::IoError { source: e })?;
        self.file
            .read_exact(&mut buf[..bytes])
            .map_err(|e| Error::IoError { source: e })?;
        Ok(bytes)
    }
}

/// SectorSink backed by a file (ISO image).
///
/// Writes go through [`crate::io::Writer`], which on Linux drives
/// continuous `sync_file_range` + `posix_fadvise(DONTNEED)` to keep
/// the kernel dirty page cache bounded during multi-GB sequential
/// writes. macOS / Windows fall through to a no-op pipeline.
///
/// `finish` runs `sync_all` before dropping the underlying file.
pub struct FileSectorSink {
    inner: crate::io::Writer,
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
            inner: crate::io::Writer::new(file)?,
        })
    }

    /// Open an existing ISO file for in-place updates (e.g. patch
    /// pass writing recovered sectors over zero-filled holes).
    /// Does not truncate.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self {
            inner: crate::io::Writer::new(file)?,
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
    // Bring the 0.18 trait into scope (not super::*: the super
    // module also re-exports the legacy `SectorReader`, and
    // having both `SectorReader::read_sectors` and
    // `SectorSource::read_sectors` visible would force every
    // call site to disambiguate). External consumers see the
    // same surface this test exercises.
    use super::{FileSectorSink, FileSectorSource};
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

        let mut src = FileSectorSource::open(path.to_str().unwrap()).unwrap();
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

        let mut src = FileSectorSource::open(path.to_str().unwrap()).unwrap();
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

        let mut src = FileSectorSource::open(path.to_str().unwrap()).unwrap();
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
