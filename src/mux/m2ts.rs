//! M2tsStream — BD transport stream with embedded metadata header.
//!
//! Write: prepends FMKV metadata header, then passes through BD-TS bytes.
//! Read: extracts metadata header (or scans PMT), then yields BD-TS bytes.

use super::{meta, ts, IOStream, ReadSeek};
use crate::disc::{DiscTitle, Stream as DiscStream};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// Size of initial scan buffer for PMT/stream detection.
const SCAN_SIZE: usize = 1024 * 1024;

enum Mode {
    Write {
        writer: Box<dyn Write>,
        header_written: bool,
    },
    Read {
        reader: Box<dyn ReadSeek>,
    },
}

/// BD transport stream with embedded metadata.
pub struct M2tsStream {
    disc_title: DiscTitle,
    mode: Mode,
    finished: bool,
}

impl M2tsStream {
    /// Create for writing. Metadata header is written on first write().
    pub fn new(writer: impl Write + 'static) -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write {
                writer: Box::new(writer),
                header_written: false,
            },
            finished: false,
        }
    }

    /// Set stream metadata. Returns self for chaining.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    /// Open an m2ts file for reading.
    ///
    /// Tries FMKV metadata header first. Falls back to PMT scan + PTS duration.
    pub fn open(mut reader: impl Read + Seek + 'static) -> io::Result<Self> {
        // Try FMKV metadata header
        if let Ok(Some(m)) = meta::read_header(&mut reader) {
            return Ok(Self {
                disc_title: m.to_title(),
                mode: Mode::Read {
                    reader: Box::new(reader),
                },
                finished: false,
            });
        }

        // Fallback: scan PMT for streams, PTS for duration
        reader.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; SCAN_SIZE];
        let n = reader.read(&mut buf)?;

        let streams = ts::scan_streams(&buf[..n])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no streams found"))?;

        let video_pid = streams.iter().find_map(|s| match s {
            DiscStream::Video(v) => Some(v.pid),
            _ => None,
        });
        let duration = video_pid
            .and_then(|pid| ts::scan_duration(&mut reader, pid))
            .unwrap_or(0.0);

        reader.seek(SeekFrom::Start(0))?;

        Ok(Self {
            disc_title: DiscTitle {
                duration_secs: duration,
                streams,
                ..DiscTitle::empty()
            },
            mode: Mode::Read {
                reader: Box::new(reader),
            },
            finished: false,
        })
    }
}

impl IOStream for M2tsStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

impl Write for M2tsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Write {
                ref mut writer,
                ref mut header_written,
            } => {
                if !*header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(&mut *writer, &m)?;
                    }
                    *header_written = true;
                }
                writer.write(buf)
            }
            Mode::Read { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for reading",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

impl Read for M2tsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Read { ref mut reader } => reader.read(buf),
            Mode::Write { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for writing",
            )),
        }
    }
}
