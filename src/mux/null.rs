//! NullStream — discards all data. Write-only. For benchmarking.

use super::IOStream;
use crate::disc::DiscTitle;
use std::io::{self, Read, Write};

/// Null stream — accepts writes, discards data. For benchmarking rip speed.
pub struct NullStream {
    disc_title: DiscTitle,
    bytes_written: u64,
}

impl Default for NullStream {
    fn default() -> Self {
        Self::new()
    }
}

impl NullStream {
    pub fn new() -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            bytes_written: 0,
        }
    }

    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl IOStream for NullStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for NullStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes_written += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for NullStream {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "null stream is write-only",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn null_counts_bytes() {
        let mut ns = NullStream::new();
        assert_eq!(ns.bytes_written(), 0);
        ns.write_all(&[0u8; 100]).unwrap();
        assert_eq!(ns.bytes_written(), 100);
        ns.write_all(&[1u8; 50]).unwrap();
        assert_eq!(ns.bytes_written(), 150);
        // Single write returns correct count
        let n = ns.write(&[0u8; 200]).unwrap();
        assert_eq!(n, 200);
        assert_eq!(ns.bytes_written(), 350);
    }

    #[test]
    fn null_read_errors() {
        let mut ns = NullStream::new();
        let mut buf = [0u8; 10];
        let err = ns.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn null_finish_ok() {
        let mut ns = NullStream::new();
        ns.write_all(&[0u8; 1000]).unwrap();
        ns.finish().unwrap();
    }

    #[test]
    fn null_implements_iostream() {
        let ns = NullStream::new();
        let mut boxed: Box<dyn IOStream> = Box::new(ns);
        boxed.write_all(&[0u8; 50]).unwrap();
        let info = boxed.info();
        assert_eq!(info.streams.len(), 0);
        boxed.finish().unwrap();
    }

    #[test]
    fn null_total_bytes_returns_none() {
        let ns = NullStream::new();
        assert_eq!(ns.total_bytes(), None);
    }
}
