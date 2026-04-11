//! StdioStream — raw byte pipe via stdin/stdout. Format-agnostic.

use std::io::{self, Read, Write};
use super::IOStream;
use crate::disc::DiscTitle;

/// Stdio stream — reads from stdin, writes to stdout.
///
/// No headers, no metadata, no format opinions. Just bytes.
/// The format is determined by whatever is on the other end.
pub struct StdioStream {
    disc_title: DiscTitle,
    reader: Option<io::Stdin>,
    writer: Option<io::Stdout>,
}

impl StdioStream {
    /// Create a stdio stream for reading (stdin).
    pub fn input() -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            reader: Some(io::stdin()),
            writer: None,
        }
    }

    /// Create a stdio stream for writing (stdout).
    pub fn output() -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            reader: None,
            writer: Some(io::stdout()),
        }
    }

    /// Set metadata (for output — passed through from input side).
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }
}

impl IOStream for StdioStream {
    fn info(&self) -> &DiscTitle { &self.disc_title }
    fn finish(&mut self) -> io::Result<()> {
        if let Some(ref mut w) = self.writer {
            w.flush()?;
        }
        Ok(())
    }
}

impl Read for StdioStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.reader {
            Some(ref mut r) => r.read(buf),
            None => Err(io::Error::new(io::ErrorKind::Unsupported,
                "stdio:// opened for output — cannot read")),
        }
    }
}

impl Write for StdioStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer {
            Some(ref mut w) => w.write(buf),
            None => Err(io::Error::new(io::ErrorKind::Unsupported,
                "stdio:// opened for input — cannot write")),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self.writer {
            Some(ref mut w) => w.flush(),
            None => Ok(()),
        }
    }
}
