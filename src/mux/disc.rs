//! DiscStream — read BD-TS data from an optical disc drive.
//!
//! Read-only stream. Wraps Drive + Disc.
//! Handles drive init, AACS decryption, and sector reading.
//!
//! Reading state (extent index, offset, batch size, error recovery) is stored
//! directly on the struct so that successive `read()` calls advance through
//! the disc instead of restarting from byte 0.

use super::IOStream;
use crate::disc::{
    detect_max_batch_sectors, ContentFormat, Disc, DiscTitle, Extent, MIN_BATCH_SECTORS,
    RAMP_BATCH_AFTER, RAMP_SPEED_AFTER, SLOW_SPEED_AFTER,
};
use crate::drive::Drive;
use crate::error::Error;
use crate::speed::DriveSpeed;
use std::io::{self, Read, Write};
use std::path::Path;

/// AACS decryption parameters needed at read time.
/// Extracted from `AacsState` so we don't need `Clone` on the full struct.
struct AacsDecrypt {
    unit_keys: Vec<(u32, [u8; 16])>,
    read_data_key: Option<[u8; 16]>,
}

/// Options for opening a disc stream.
#[derive(Default)]
pub struct DiscOptions {
    /// Device path (e.g. "/dev/sg4"). None = auto-detect.
    pub device: Option<std::path::PathBuf>,
    /// KEYDB.cfg path. None = search standard locations.
    pub keydb_path: Option<std::path::PathBuf>,
    /// Which title to read (0-based). None = longest title.
    pub title_index: Option<usize>,
}

/// Optical disc stream. Read-only — yields decrypted BD-TS bytes.
///
/// Embeds the reading state that `ContentReader` would normally hold, so that
/// successive `read()` calls advance through the disc correctly.
pub struct DiscStream {
    disc_title: DiscTitle,
    disc: Disc,
    session: Drive,
    // Read buffer: holds one decoded batch
    batch_buf: Vec<u8>,
    batch_pos: usize,
    eof: bool,

    // ── Reading state (replaces ContentReader) ──
    extents: Vec<Extent>,
    current_extent: usize,
    current_offset: u32,
    #[allow(dead_code)]
    content_format: ContentFormat,
    aacs: Option<AacsDecrypt>,
    css: Option<crate::css::CssState>,
    unit_key_idx: usize,
    read_buf: Vec<u8>,
    /// Current batch size in sectors (adapts on errors)
    batch_sectors: u16,
    /// Maximum batch size detected from kernel limits
    max_batch_sectors: u16,
    /// Consecutive successful batch reads
    ok_streak: u32,
    /// Consecutive errors at current position
    error_streak: u32,
    /// Total read errors encountered
    pub errors: u32,
}

impl DiscStream {
    /// Open the disc drive and scan disc metadata.
    pub fn open(opts: DiscOptions) -> Result<Self, Error> {
        let mut session = match opts.device {
            Some(ref d) => Drive::open(d)?,
            None => crate::drive::find_drive().ok_or_else(|| Error::DeviceNotFound {
                path: String::new(),
            })?,
        };
        session.wait_ready()?;
        let _ = session.init();
        let _ = session.probe_disc();

        let scan_opts = match opts.keydb_path {
            Some(ref kp) => crate::disc::ScanOptions::with_keydb(kp.clone()),
            None => crate::disc::ScanOptions::default(),
        };
        let disc = Disc::scan(&mut session, &scan_opts)?;

        let title_index = opts.title_index.unwrap_or(0);
        if title_index >= disc.titles.len() {
            return Err(Error::DiscTitleRange {
                index: title_index,
                count: disc.titles.len(),
            });
        }
        let disc_title = disc.titles[title_index].clone();
        let extents = disc_title.extents.clone();
        let content_format = disc_title.content_format;
        let aacs = disc.aacs.as_ref().map(|a| AacsDecrypt {
            unit_keys: a.unit_keys.clone(),
            read_data_key: a.read_data_key,
        });
        let css = disc.css.clone();

        let max_batch = detect_max_batch_sectors(session.device_path());

        Ok(Self {
            disc_title,
            disc,
            session,
            batch_buf: Vec::new(),
            batch_pos: 0,
            eof: false,
            extents,
            current_extent: 0,
            current_offset: 0,
            content_format,
            aacs,
            css,
            unit_key_idx: 0,
            read_buf: Vec::with_capacity(max_batch as usize * 2048),
            batch_sectors: max_batch,
            max_batch_sectors: max_batch,
            ok_streak: 0,
            error_streak: 0,
            errors: 0,
        })
    }

    /// Get the full Disc (for listing all titles, etc.)
    pub fn disc(&self) -> &Disc {
        &self.disc
    }

    /// Read sectors from the drive into `self.read_buf`.
    fn read_sectors(&mut self, lba: u32, count: u16) -> Result<(), Error> {
        self.session.read_content(lba, count, &mut self.read_buf)?;
        Ok(())
    }

    /// Fill the internal read buffer with the next batch of sectors,
    /// handling error recovery (halve batch, slow drive, retry, skip).
    ///
    /// Returns `true` if data was read, `false` at end-of-title.
    fn fill_buffer(&mut self) -> Result<bool, Error> {
        loop {
            if self.current_extent >= self.extents.len() {
                return Ok(false);
            }

            let ext_start = self.extents[self.current_extent].start_lba;
            let ext_sectors = self.extents[self.current_extent].sector_count;
            let remaining = ext_sectors.saturating_sub(self.current_offset);

            // Align to 3 sectors (one aligned unit)
            let sectors_to_read = remaining.min(self.batch_sectors as u32) as u16;
            let sectors_to_read = sectors_to_read - (sectors_to_read % 3);
            if sectors_to_read == 0 {
                self.current_extent += 1;
                self.current_offset = 0;
                continue;
            }

            let lba = ext_start + self.current_offset;
            let byte_count = sectors_to_read as usize * 2048;
            self.read_buf.resize(byte_count, 0);

            match self.read_sectors(lba, sectors_to_read) {
                Ok(_) => {
                    self.current_offset += sectors_to_read as u32;
                    self.error_streak = 0;

                    if self.current_offset >= ext_sectors {
                        self.current_extent += 1;
                        self.current_offset = 0;
                    }

                    // Ramp up batch size after consecutive successes
                    self.ok_streak += 1;
                    if self.batch_sectors < self.max_batch_sectors
                        && self.ok_streak >= RAMP_BATCH_AFTER
                    {
                        self.batch_sectors = (self.batch_sectors * 2).min(self.max_batch_sectors);
                        self.ok_streak = 0;
                    }

                    // Restore max speed after sustained success at full batch
                    if self.batch_sectors == self.max_batch_sectors
                        && self.ok_streak >= RAMP_SPEED_AFTER
                    {
                        self.session.set_speed(0xFFFF);
                        self.ok_streak = 0;
                    }

                    return Ok(true);
                }
                Err(_) => {
                    self.errors += 1;
                    self.error_streak += 1;
                    self.ok_streak = 0;

                    // First error: re-init (drive may have re-locked)
                    if self.error_streak == 1 {
                        let _ = self.session.init();
                        let _ = self.session.probe_disc();
                    }

                    // Repeated errors: slow down
                    if self.error_streak >= SLOW_SPEED_AFTER {
                        self.session.set_speed(DriveSpeed::BD2x.to_kbps());
                        self.error_streak = 0;
                    }

                    if self.batch_sectors > MIN_BATCH_SECTORS {
                        self.batch_sectors = (self.batch_sectors / 2).max(MIN_BATCH_SECTORS);
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    } else {
                        // At minimum batch -- retry once with longer pause
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        self.read_buf.resize(MIN_BATCH_SECTORS as usize * 2048, 0);
                        if self.read_sectors(lba, MIN_BATCH_SECTORS).is_ok() {
                            self.error_streak = 0;
                            self.current_offset += MIN_BATCH_SECTORS as u32;
                            if self.current_offset >= ext_sectors {
                                self.current_extent += 1;
                                self.current_offset = 0;
                            }
                            return Ok(true);
                        }
                        // Still failing -- skip this unit (zero-fill)
                        self.current_offset += 3;
                        if self.current_offset >= ext_sectors {
                            self.current_extent += 1;
                            self.current_offset = 0;
                        }
                        self.read_buf.resize(crate::aacs::ALIGNED_UNIT_LEN, 0);
                        self.read_buf.fill(0);
                        return Ok(true);
                    }
                }
            }
        }
    }

    /// Decrypt the contents of `self.read_buf` in-place and copy the
    /// decrypted data into `self.batch_buf`.
    fn decrypt_and_buffer(&mut self) {
        let unit_len = crate::aacs::ALIGNED_UNIT_LEN;
        let total_bytes = self.read_buf.len();

        if let Some(ref aacs) = self.aacs {
            let uk = aacs
                .unit_keys
                .get(self.unit_key_idx)
                .map(|(_, k)| *k)
                .unwrap_or([0u8; 16]);
            let rdk = aacs.read_data_key.as_ref();

            let num_units = total_bytes / unit_len;
            for i in 0..num_units {
                let start = i * unit_len;
                let end = start + unit_len;
                let unit = &mut self.read_buf[start..end];
                if crate::aacs::is_unit_encrypted(unit) {
                    crate::aacs::decrypt_unit_full(unit, &uk, rdk);
                }
            }
        } else if let Some(ref css) = self.css {
            for chunk in self.read_buf[..total_bytes].chunks_mut(2048) {
                crate::css::lfsr::descramble_sector(&css.title_key, chunk);
            }
        }
        // No encryption: read_buf is already plaintext

        // Swap buffers instead of copying — the old batch_buf becomes
        // read_buf and will be overwritten on the next read.
        std::mem::swap(&mut self.batch_buf, &mut self.read_buf);
        self.batch_pos = 0;
    }
}

impl IOStream for DiscStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn total_bytes(&self) -> Option<u64> {
        Some(self.disc_title.size_bytes)
    }
}

impl Read for DiscStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Drain buffer first
        if self.batch_pos < self.batch_buf.len() {
            let n = (self.batch_buf.len() - self.batch_pos).min(buf.len());
            buf[..n].copy_from_slice(&self.batch_buf[self.batch_pos..self.batch_pos + n]);
            self.batch_pos += n;
            return Ok(n);
        }

        if self.eof {
            return Ok(0);
        }

        // Fill the read buffer with the next batch of sectors
        let has_data = self
            .fill_buffer()
            .map_err(|e| io::Error::other(e.to_string()))?;

        if !has_data {
            self.eof = true;
            return Ok(0);
        }

        // Decrypt in-place and move to batch_buf
        self.decrypt_and_buffer();

        // Now drain into the caller's buffer
        let n = self.batch_buf.len().min(buf.len());
        buf[..n].copy_from_slice(&self.batch_buf[..n]);
        self.batch_pos = n;
        Ok(n)
    }
}

impl Write for DiscStream {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "disc is read-only",
        ))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::disc::Extent;

    /// Build a minimal DiscStream with fake extents for testing state advancement.
    /// We cannot call `DiscStream::open()` without a real drive, so we construct
    /// one manually and then call the internal `fill_buffer` / `read` path
    /// through a helper that simulates the session reads.
    ///
    /// Instead we test the state-machine logic directly: given a set of extents
    /// and a current_extent/current_offset, verify that repeated reads advance
    /// through the extents correctly.
    #[test]
    fn state_advances_across_extents() {
        // Simulate two extents of 6 sectors each (2 aligned units each).
        let extents = [
            Extent {
                start_lba: 100,
                sector_count: 6,
            },
            Extent {
                start_lba: 200,
                sector_count: 6,
            },
        ];

        // Walk through the extents manually using the same arithmetic
        // that fill_buffer uses, and verify we visit every sector.
        let batch_sectors: u16 = 6;
        let mut current_extent: usize = 0;
        let mut current_offset: u32 = 0;
        let mut lbas_read = Vec::new();

        while current_extent < extents.len() {
            let ext_start = extents[current_extent].start_lba;
            let ext_sectors = extents[current_extent].sector_count;
            let remaining = ext_sectors.saturating_sub(current_offset);
            let sectors_to_read = remaining.min(batch_sectors as u32) as u16;
            let sectors_to_read = sectors_to_read - (sectors_to_read % 3);
            if sectors_to_read == 0 {
                current_extent += 1;
                current_offset = 0;
                continue;
            }
            let lba = ext_start + current_offset;
            lbas_read.push((lba, sectors_to_read));
            current_offset += sectors_to_read as u32;
            if current_offset >= ext_sectors {
                current_extent += 1;
                current_offset = 0;
            }
        }

        assert_eq!(lbas_read.len(), 2, "should read two batches");
        assert_eq!(lbas_read[0], (100, 6), "first batch starts at LBA 100");
        assert_eq!(lbas_read[1], (200, 6), "second batch starts at LBA 200");
    }

    /// Verify that small extents that are not aligned to 3 sectors are skipped
    /// (moved past) rather than causing an infinite loop.
    #[test]
    fn unaligned_extent_is_skipped() {
        let extents = [
            Extent {
                start_lba: 50,
                sector_count: 2, // < 3, cannot form an aligned unit
            },
            Extent {
                start_lba: 300,
                sector_count: 9,
            },
        ];

        let batch_sectors: u16 = 9;
        let mut current_extent: usize = 0;
        let mut current_offset: u32 = 0;
        let mut lbas_read = Vec::new();

        while current_extent < extents.len() {
            let ext_start = extents[current_extent].start_lba;
            let ext_sectors = extents[current_extent].sector_count;
            let remaining = ext_sectors.saturating_sub(current_offset);
            let sectors_to_read = remaining.min(batch_sectors as u32) as u16;
            let sectors_to_read = sectors_to_read - (sectors_to_read % 3);
            if sectors_to_read == 0 {
                current_extent += 1;
                current_offset = 0;
                continue;
            }
            let lba = ext_start + current_offset;
            lbas_read.push((lba, sectors_to_read));
            current_offset += sectors_to_read as u32;
            if current_offset >= ext_sectors {
                current_extent += 1;
                current_offset = 0;
            }
        }

        assert_eq!(lbas_read.len(), 1, "only second extent is readable");
        assert_eq!(lbas_read[0], (300, 9));
    }

    /// Verify that multiple reads from the same extent produce advancing offsets.
    #[test]
    fn multiple_batches_within_one_extent() {
        let extents = [Extent {
            start_lba: 1000,
            sector_count: 18, // 6 aligned units = 3 batches of 6 sectors
        }];

        let batch_sectors: u16 = 6;
        let mut current_extent: usize = 0;
        let mut current_offset: u32 = 0;
        let mut lbas_read = Vec::new();

        while current_extent < extents.len() {
            let ext_start = extents[current_extent].start_lba;
            let ext_sectors = extents[current_extent].sector_count;
            let remaining = ext_sectors.saturating_sub(current_offset);
            let sectors_to_read = remaining.min(batch_sectors as u32) as u16;
            let sectors_to_read = sectors_to_read - (sectors_to_read % 3);
            if sectors_to_read == 0 {
                current_extent += 1;
                current_offset = 0;
                continue;
            }
            let lba = ext_start + current_offset;
            lbas_read.push((lba, sectors_to_read));
            current_offset += sectors_to_read as u32;
            if current_offset >= ext_sectors {
                current_extent += 1;
                current_offset = 0;
            }
        }

        assert_eq!(lbas_read.len(), 3, "three batches from one extent");
        assert_eq!(lbas_read[0], (1000, 6));
        assert_eq!(lbas_read[1], (1006, 6));
        assert_eq!(lbas_read[2], (1012, 6));
    }
}
