//! ISO sector reader — file-backed SectorReader for Blu-ray ISO images.
//!
//! An ISO is a flat image of 2048-byte sectors. Sector N starts at byte offset N * 2048.
//! Used by DiscStream::open_iso() and Disc::scan_image().

use crate::error::{Error, Result};
use crate::sector::SectorReader;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const SECTOR_SIZE: u64 = 2048;

/// File-backed sector reader for ISO images.
pub struct IsoSectorReader {
    file: File,
    capacity: u32,
}

impl IsoSectorReader {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = File::open(Path::new(path))
            .map_err(|e| std::io::Error::new(e.kind(), format!("iso://{path}: {e}")))?;
        let size = file.metadata()?.len();
        let sectors = size / SECTOR_SIZE;
        if sectors > u32::MAX as u64 {
            return Err(crate::error::Error::IsoTooLarge {
                path: path.to_string(),
            }
            .into());
        }
        let capacity = sectors as u32;
        Ok(Self { file, capacity })
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }
}

impl SectorReader for IsoSectorReader {
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8]) -> Result<usize> {
        let bytes = count as usize * SECTOR_SIZE as usize;
        self.file
            .seek(SeekFrom::Start(lba as u64 * SECTOR_SIZE))
            .map_err(|e| Error::IoError { source: e })?;
        self.file
            .read_exact(&mut buf[..bytes])
            .map_err(|e| Error::IoError { source: e })?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_reader_read_sectors() {
        let mut data = vec![0u8; 4 * SECTOR_SIZE as usize];
        for i in 0..4u8 {
            let offset = i as usize * SECTOR_SIZE as usize;
            data[offset] = i + 1;
            data[offset + 2047] = i + 100;
        }

        let dir = std::env::temp_dir().join("freemkv_test_iso_read");
        std::fs::write(&dir, &data).unwrap();

        let mut reader = IsoSectorReader::open(dir.to_str().unwrap()).unwrap();
        assert_eq!(reader.capacity(), 4);

        let mut buf = [0u8; 2048];
        reader.read_sectors(0, 1, &mut buf).unwrap();
        assert_eq!(buf[0], 1);
        assert_eq!(buf[2047], 100);

        reader.read_sectors(2, 1, &mut buf).unwrap();
        assert_eq!(buf[0], 3);
        assert_eq!(buf[2047], 102);

        std::fs::remove_file(&dir).ok();
    }

    #[test]
    fn iso_reader_capacity() {
        let data = vec![0u8; 10 * SECTOR_SIZE as usize];
        let dir = std::env::temp_dir().join("freemkv_test_iso_cap");
        std::fs::write(&dir, &data).unwrap();

        let reader = IsoSectorReader::open(dir.to_str().unwrap()).unwrap();
        assert_eq!(reader.capacity(), 10);

        std::fs::remove_file(&dir).ok();
    }
}
