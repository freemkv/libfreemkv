//! IsoStream — read/write Blu-ray ISO disc images.
//!
//! Read: parses UDF filesystem inside the ISO using the same pipeline as
//! DiscStream (titles, streams, labels, AACS). An ISO is a flat image of
//! 2048-byte sectors — sector N starts at byte offset N * 2048.
//!
//! Write: creates a UDF 2.50 filesystem containing the m2ts stream data.
//! The resulting ISO can be mounted or read back via IsoStream.

use super::isowriter::IsoWriter;
use super::IOStream;
use crate::decrypt::DecryptKeys;
use crate::disc::{Disc, DiscTitle, ScanOptions};
use crate::error::{Error, Result};
use crate::sector::SectorReader;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const SECTOR_SIZE: u64 = 2048;

/// Maximum sectors to batch-read at once (64 sectors = 128 KB).
const BATCH_SECTORS: usize = 64;

/// File-backed sector reader for ISO images.
pub struct IsoSectorReader {
    file: File,
    capacity: u32,
}

impl IsoSectorReader {
    pub fn open(path: &str) -> io::Result<Self> {
        let file = File::open(Path::new(path))
            .map_err(|e| io::Error::new(e.kind(), format!("iso://{path}: {e}")))?;
        let size = file.metadata()?.len();
        let capacity = (size / SECTOR_SIZE) as u32;
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

/// Blu-ray ISO image stream.
///
/// Read: opens ISO, parses UDF (same as DiscStream), streams BD-TS content.
/// Write: creates UDF 2.50 ISO with BDMV/STREAM/*.m2ts.
pub struct IsoStream {
    disc_title: DiscTitle,
    disc: Option<Disc>,
    // Read side
    reader: Option<IsoSectorReader>,
    extents: Vec<(u32, u32)>,
    extent_idx: usize,
    sectors_remaining: u32,
    /// Batch buffer: holds up to BATCH_SECTORS sectors (128 KB) at once.
    batch_buf: Vec<u8>,
    buf_pos: usize,
    buf_len: usize,
    eof: bool,
    /// Decrypt on read — auto-detected from disc scan.
    decrypt_keys: DecryptKeys,
    // Write side
    iso_writer: Option<IsoWriter<io::BufWriter<File>>>,
    write_started: bool,
}

impl IsoStream {
    /// Open an ISO file for reading. Parses UDF, scans titles, streams, labels.
    pub fn open(path: &str, title_index: Option<usize>, opts: &ScanOptions) -> io::Result<Self> {
        let mut reader = IsoSectorReader::open(path)?;
        let capacity = reader.capacity();

        let disc = Disc::scan_image(&mut reader, capacity, opts)
            .map_err(|e| io::Error::other(e.to_string()))?;

        if disc.titles.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no titles found in ISO image",
            ));
        }
        let idx = title_index.unwrap_or(0);
        if idx >= disc.titles.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "title {} out of range (disc has {})",
                    idx + 1,
                    disc.titles.len()
                ),
            ));
        }
        let disc_title = disc.titles[idx].clone();

        let decrypt_keys = disc.decrypt_keys();

        let extents: Vec<(u32, u32)> = disc_title
            .extents
            .iter()
            .map(|e| (e.start_lba, e.sector_count))
            .collect();
        let sectors_remaining = extents.first().map(|e| e.1).unwrap_or(0);

        Ok(IsoStream {
            disc_title,
            disc: Some(disc),
            reader: Some(reader),
            extents,
            extent_idx: 0,
            sectors_remaining,
            batch_buf: vec![0u8; BATCH_SECTORS * SECTOR_SIZE as usize],
            buf_pos: 0,
            buf_len: 0,
            eof: false,
            decrypt_keys,
            iso_writer: None,
            write_started: false,
        })
    }

    /// Create an ISO file for writing.
    pub fn create(path: &str) -> io::Result<Self> {
        let file = File::create(Path::new(path))
            .map_err(|e| io::Error::new(e.kind(), format!("iso://{path}: {e}")))?;
        let buf_writer = io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        let iso_writer = IsoWriter::new(buf_writer, "FREEMKV", "00001.m2ts");

        Ok(IsoStream {
            disc_title: DiscTitle::empty(),
            disc: None,
            decrypt_keys: DecryptKeys::None,
            reader: None,
            extents: Vec::new(),
            extent_idx: 0,
            sectors_remaining: 0,
            batch_buf: Vec::new(),
            buf_pos: 0,
            buf_len: 0,
            eof: false,
            iso_writer: Some(iso_writer),
            write_started: false,
        })
    }

    /// Set metadata (for write mode). Must be called before writing data.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        // Update the ISO writer's volume ID and m2ts filename from title metadata
        if let Some(writer) = self.iso_writer.take() {
            let vol_id = if dt.playlist.is_empty() {
                "FREEMKV".to_string()
            } else {
                dt.playlist
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ' ')
                    .collect::<String>()
            };
            let m2ts_name = format!("{:05}.m2ts", dt.playlist_id.max(1));
            self.iso_writer = Some(writer.with_names(&vol_id, &m2ts_name));
        }
        self
    }

    /// Get the full Disc (for listing all titles).
    pub fn disc(&self) -> Option<&Disc> {
        self.disc.as_ref()
    }

    /// Read up to BATCH_SECTORS sectors at once into the batch buffer.
    fn read_next_batch(&mut self) -> io::Result<bool> {
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

        // Read up to BATCH_SECTORS, but no more than remaining in this extent
        let count = (self.sectors_remaining as usize).min(BATCH_SECTORS) as u16;

        reader
            .read_sectors(lba, count, &mut self.batch_buf)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let bytes = count as usize * SECTOR_SIZE as usize;

        self.buf_pos = 0;
        self.buf_len = bytes;

        self.sectors_remaining -= count as u32;
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
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Some(ref mut w) = self.iso_writer {
            w.finish()?;
        }
        Ok(())
    }
    fn total_bytes(&self) -> Option<u64> {
        // Read mode: size is known from disc scan
        if self.reader.is_some() {
            Some(self.disc_title.size_bytes)
        } else {
            None
        }
    }
}

impl Read for IsoStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.eof {
            return Ok(0);
        }

        if self.buf_pos < self.buf_len {
            let n = (self.buf_len - self.buf_pos).min(buf.len());
            buf[..n].copy_from_slice(&self.batch_buf[self.buf_pos..self.buf_pos + n]);
            self.buf_pos += n;
            return Ok(n);
        }

        if self.read_next_batch()? {
            let n = self.buf_len.min(buf.len());
            buf[..n].copy_from_slice(&self.batch_buf[..n]);
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
        let w = match self.iso_writer.as_mut() {
            Some(w) => w,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "iso:// opened for reading — cannot write",
                ))
            }
        };

        if !self.write_started {
            w.start()?;
            self.write_started = true;
        }

        w.write_data(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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

    #[test]
    fn iso_write_creates_valid_udf() {
        let path = std::env::temp_dir().join("freemkv_test_iso_write.iso");
        let mut stream = IsoStream::create(path.to_str().unwrap()).unwrap();

        // Write some fake BD-TS content
        let mut content = Vec::new();
        for i in 0..100u8 {
            let mut pkt = [0u8; 192];
            pkt[4] = 0x47;
            pkt[5] = i;
            content.extend_from_slice(&pkt);
        }

        stream.write_all(&content).unwrap();
        stream.finish().unwrap();

        // Verify the ISO has valid UDF structure
        let file = File::open(&path).unwrap();
        let size = file.metadata().unwrap().len();
        assert!(size > 288 * SECTOR_SIZE); // at least header + some data

        // Read back and verify AVDP at sector 256
        let mut reader = IsoSectorReader::open(path.to_str().unwrap()).unwrap();
        let mut avdp = [0u8; 2048];
        reader.read_sectors(256, 1, &mut avdp).unwrap();
        let tag_id = u16::from_le_bytes([avdp[0], avdp[1]]);
        assert_eq!(tag_id, 2, "AVDP tag should be 2");

        // Verify VRS at sector 16
        let mut vrs = [0u8; 2048];
        reader.read_sectors(16, 1, &mut vrs).unwrap();
        assert_eq!(&vrs[1..6], b"BEA01");

        std::fs::remove_file(&path).ok();
    }
}
