//! NullStream — discards all data. Write-only. For benchmarking.

use std::io::{self, Read, Write};
use super::IOStream;
use crate::disc::DiscTitle;

/// Null stream — accepts writes, discards data. For benchmarking rip speed.
pub struct NullStream {
    disc_title: DiscTitle,
    bytes_written: u64,
}

impl NullStream {
    pub fn new() -> Self {
        Self { disc_title: DiscTitle::empty(), bytes_written: 0 }
    }

    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    pub fn bytes_written(&self) -> u64 { self.bytes_written }
}

impl IOStream for NullStream {
    fn info(&self) -> &DiscTitle { &self.disc_title }
    fn finish(&mut self) -> io::Result<()> { Ok(()) }
}

impl Write for NullStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes_written += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl Read for NullStream {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "null stream is write-only"))
    }
}
