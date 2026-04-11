//! DiscStream — read BD-TS data from an optical disc drive.
//!
//! Read-only stream. Wraps DriveSession + Disc.
//! Handles drive init, AACS decryption, and sector reading.

use super::IOStream;
use crate::disc::{Disc, DiscTitle};
use crate::drive::DriveSession;
use crate::error::Error;
use std::io::{self, Read, Write};
use std::path::Path;

/// Options for opening a disc stream.
#[derive(Default)]
pub struct DiscOptions {
    /// Device path (e.g. "/dev/sg4"). None = auto-detect.
    pub device: Option<String>,
    /// KEYDB.cfg path. None = search standard locations.
    pub keydb_path: Option<String>,
    /// Which title to read (0-based). None = longest title.
    pub title_index: Option<usize>,
}


/// Optical disc stream. Read-only — yields decrypted BD-TS bytes.
pub struct DiscStream {
    disc_title: DiscTitle,
    disc: Disc,
    session: DriveSession,
    title_index: usize,
    // Read buffer: holds one batch from ContentReader
    batch_buf: Vec<u8>,
    batch_pos: usize,
    started: bool,
    eof: bool,
}

impl DiscStream {
    /// Open the disc drive and scan disc metadata.
    pub fn open(opts: DiscOptions) -> Result<Self, Error> {
        let device = match opts.device {
            Some(ref d) => crate::drive::resolve_device(d)?.0,
            None => crate::drive::find_drive().ok_or_else(|| Error::DeviceNotFound {
                path: String::new(),
            })?,
        };

        let mut session = DriveSession::open(Path::new(&device))?;
        session.wait_ready()?;
        let _ = session.init();
        let _ = session.probe_disc();

        let scan_opts = match opts.keydb_path {
            Some(ref kp) => crate::disc::ScanOptions::with_keydb(kp),
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

        Ok(Self {
            disc_title,
            disc,
            session,
            title_index,
            batch_buf: Vec::new(),
            batch_pos: 0,
            started: false,
            eof: false,
        })
    }

    /// Get the full Disc (for listing all titles, etc.)
    pub fn disc(&self) -> &Disc {
        &self.disc
    }
}

impl IOStream for DiscStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
    fn finish(&mut self) -> io::Result<()> {
        Ok(())
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

        // Open reader on first call
        if !self.started {
            self.started = true;
        }

        // Read next batch via a temporary ContentReader
        // ContentReader borrows session and disc, so we create it inline
        let mut reader = self
            .disc
            .open_title(&mut self.session, self.title_index)
            .map_err(|e| io::Error::other(e.to_string()))?;

        match reader.read_batch() {
            Ok(Some(batch)) => {
                let n = batch.len().min(buf.len());
                buf[..n].copy_from_slice(&batch[..n]);
                if batch.len() > n {
                    self.batch_buf = batch.to_vec();
                    self.batch_pos = n;
                } else {
                    self.batch_buf.clear();
                    self.batch_pos = 0;
                }
                Ok(n)
            }
            Ok(None) => {
                self.eof = true;
                Ok(0)
            }
            Err(e) => Err(io::Error::other(e.to_string())),
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
