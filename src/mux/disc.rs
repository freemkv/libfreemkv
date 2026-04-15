//! DiscStream — read sectors from an optical disc drive.
//!
//! `DiscStream::open()` does the full init sequence:
//!   drive open → wait_ready → init → probe_disc → scan
//!
//! Then reads title extents or full-disc sequentially.
//! No decryption — that's a caller concern.

use super::IOStream;
use crate::disc::{
    detect_max_batch_sectors, Disc, DiscTitle, Extent, ScanOptions,
};
use crate::drive::Drive;
use crate::error::{Error, Result};
use crate::event::{Event, EventKind};
use std::io::{self, Read, Write};
use std::path::Path;

/// Optical disc stream. Read-only — yields raw sector bytes.
///
/// Created from an initialized Drive + title extents or full-disc mode.
/// Error recovery (batch reduction, retry, zero-fill) is handled internally.
pub struct DiscStream {
    drive: Drive,
    title: DiscTitle,
    decrypt_keys: crate::decrypt::DecryptKeys,

    // What to read
    mode: ReadMode,

    // Position
    current_lba: u32,
    current_extent: usize,
    current_offset: u32,

    // Buffer
    read_buf: Vec<u8>,
    buf_valid: usize,
    buf_cursor: usize,

    // Batch size for reads
    batch_sectors: u16,
    pub errors: u64,
    eof: bool,
}

enum ReadMode {
    /// Read title extents (for MKV, M2TS, etc.)
    Extents(Vec<Extent>),
    /// Read LBA 0 to capacity (for ISO)
    Sequential { capacity: u32 },
}

/// Result of opening a DiscStream.
pub struct DiscOpenResult {
    pub stream: DiscStream,
    pub disc: Disc,
}

impl DiscStream {
    /// Open a disc drive, init, scan, and prepare to read a title.
    ///
    /// Steps (each does one thing):
    ///   1. Drive::open (or find_drive)
    ///   2. wait_ready
    ///   3. init (non-fatal)
    ///   4. probe_disc (non-fatal)
    ///   5. Disc::scan
    ///
    /// Pass an event callback for status reporting, or None.
    pub fn open(
        device: Option<&Path>,
        keydb_path: Option<&str>,
        title_index: usize,
        on_event: Option<&dyn Fn(Event)>,
    ) -> Result<DiscOpenResult> {
        let emit = |kind: EventKind| {
            if let Some(cb) = &on_event {
                cb(Event { kind });
            }
        };

        // 1. Open
        let mut drive = match device {
            Some(d) => Drive::open(d)?,
            None => crate::drive::find_drive().ok_or_else(|| Error::DeviceNotFound {
                path: String::new(),
            })?,
        };
        emit(EventKind::DriveOpened {
            device: drive.device_path().to_string(),
        });

        // 2. Wait
        let _ = drive.wait_ready();
        emit(EventKind::DriveReady);

        // 3. Init
        let init_ok = drive.init().is_ok();
        emit(EventKind::InitComplete { success: init_ok });

        // 4. Probe
        let probe_ok = drive.probe_disc().is_ok();
        emit(EventKind::ProbeComplete { success: probe_ok });

        // 5. Scan
        let scan_opts = match keydb_path {
            Some(kp) => ScanOptions::with_keydb(kp),
            None => ScanOptions::default(),
        };
        let disc = Disc::scan(&mut drive, &scan_opts)?;
        emit(EventKind::ScanComplete {
            titles: disc.titles.len(),
        });

        if title_index >= disc.titles.len() {
            return Err(Error::DiscTitleRange {
                index: title_index,
                count: disc.titles.len(),
            });
        }

        let title = disc.titles[title_index].clone();
        let keys = disc.decrypt_keys();
        let mut stream = Self::title(drive, title);
        stream.decrypt_keys = keys;

        Ok(DiscOpenResult { stream, disc })
    }

    /// Create a stream that reads a title's extents.
    /// Use this when you already have an initialized Drive.
    pub fn title(drive: Drive, title: DiscTitle) -> Self {
        let max_batch = detect_max_batch_sectors(drive.device_path());
        let extents = title.extents.clone();
        Self::new(drive, title, ReadMode::Extents(extents), max_batch)
    }

    /// Create a stream that reads the full disc sequentially (for ISO).
    pub fn full_disc(drive: Drive, title: DiscTitle, capacity: u32) -> Self {
        let max_batch = detect_max_batch_sectors(drive.device_path());
        Self::new(drive, title, ReadMode::Sequential { capacity }, max_batch)
    }

    /// Resume a full disc read from a given LBA (for ISO resume).
    /// Use after checking an existing partial file:
    ///   start_lba = (file_size / 2048) - safety_margin
    pub fn full_disc_resume(drive: Drive, title: DiscTitle, capacity: u32, start_lba: u32) -> Self {
        let max_batch = detect_max_batch_sectors(drive.device_path());
        let mut stream = Self::new(drive, title, ReadMode::Sequential { capacity }, max_batch);
        stream.current_lba = start_lba;
        stream
    }

    /// Set SCSI read timeout (default 30s).

    fn new(drive: Drive, title: DiscTitle, mode: ReadMode, max_batch: u16) -> Self {
        Self {
            drive,
            title,
            decrypt_keys: crate::decrypt::DecryptKeys::None,
            mode,
            current_lba: 0,
            current_extent: 0,
            current_offset: 0,
            read_buf: Vec::with_capacity(max_batch as usize * 2048),
            buf_valid: 0,
            buf_cursor: 0,
            batch_sectors: max_batch,
            errors: 0,
            eof: false,
        }
    }

    /// Skip decryption — return raw encrypted bytes.
    pub fn set_raw(&mut self) {
        self.decrypt_keys = crate::decrypt::DecryptKeys::None;
    }

    /// Lock the tray.
    pub fn lock_tray(&mut self) {
        self.drive.lock_tray();
    }

    /// Unlock the tray.
    pub fn unlock_tray(&mut self) {
        self.drive.unlock_tray();
    }

    /// Recover the drive (for batch: switch to another title).
    pub fn into_drive(self) -> Drive {
        self.drive
    }

    // ── Fill ─────────────────────────────────────────────────────────────

    fn fill(&mut self) -> bool {
        match &self.mode {
            ReadMode::Extents(_) => self.fill_extents(),
            ReadMode::Sequential { .. } => self.fill_sequential(),
        }
    }

    fn fill_extents(&mut self) -> bool {
        let (ext_start, ext_sectors) = match &self.mode {
            ReadMode::Extents(exts) => {
                if self.current_extent >= exts.len() {
                    return false;
                }
                (
                    exts[self.current_extent].start_lba,
                    exts[self.current_extent].sector_count,
                )
            }
            _ => unreachable!(),
        };

        let remaining = ext_sectors.saturating_sub(self.current_offset);
        let sectors = remaining.min(self.batch_sectors as u32) as u16;
        let sectors = sectors - (sectors % 3);
        if sectors == 0 {
            self.current_extent += 1;
            self.current_offset = 0;
            return self.fill_extents(); // next extent
        }

        let lba = ext_start + self.current_offset;
        let bytes = sectors as usize * 2048;
        self.read_buf.resize(bytes, 0);

        // Drive handles all error recovery internally.
        match self.drive.read(
            lba,
            sectors,
            &mut self.read_buf[..bytes],
        ) {
            Ok(_) => {
                self.buf_valid = bytes;
                self.buf_cursor = 0;
                self.current_offset += sectors as u32;
                if self.current_offset >= ext_sectors {
                    self.current_extent += 1;
                    self.current_offset = 0;
                }
                true
            }
            Err(_) => false, // drive gone — EOF
        }
    }

    fn fill_sequential(&mut self) -> bool {
        let capacity = match &self.mode {
            ReadMode::Sequential { capacity } => *capacity,
            _ => unreachable!(),
        };

        if self.current_lba >= capacity {
            return false;
        }

        let remaining = capacity - self.current_lba;
        let count = remaining.min(self.batch_sectors as u32) as u16;
        let bytes = count as usize * 2048;
        self.read_buf.resize(bytes, 0);

        // Drive handles all error recovery internally —
        // retries, speed changes, zero-fill on unreadable sectors.
        match self.drive.read(
            self.current_lba,
            count,
            &mut self.read_buf[..bytes],
        ) {
            Ok(_) => {
                self.buf_valid = bytes;
                self.buf_cursor = 0;
                self.current_lba += count as u32;
                true
            }
            Err(_) => false, // drive gone — EOF
        }
    }

}

// ── IOStream ─────────────────────────────────────────────────────────────────

impl IOStream for DiscStream {
    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn finish(&mut self) -> io::Result<()> {
        self.drive.unlock_tray();
        Ok(())
    }

    fn total_bytes(&self) -> Option<u64> {
        match &self.mode {
            ReadMode::Extents(extents) => {
                Some(extents.iter().map(|e| e.sector_count as u64 * 2048).sum())
            }
            ReadMode::Sequential { capacity } => Some(*capacity as u64 * 2048),
        }
    }

    fn keys(&self) -> crate::decrypt::DecryptKeys {
        self.decrypt_keys.clone()
    }
}

impl Read for DiscStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Drain current buffer
        if self.buf_cursor < self.buf_valid {
            let n = (self.buf_valid - self.buf_cursor).min(buf.len());
            buf[..n].copy_from_slice(&self.read_buf[self.buf_cursor..self.buf_cursor + n]);
            self.buf_cursor += n;
            return Ok(n);
        }

        if self.eof {
            return Ok(0);
        }

        // Fill next batch
        if self.fill() {
            let n = self.buf_valid.min(buf.len());
            buf[..n].copy_from_slice(&self.read_buf[..n]);
            self.buf_cursor = n;
            Ok(n)
        } else {
            self.eof = true;
            Ok(0)
        }
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
