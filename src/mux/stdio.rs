//! StdioStream — raw byte pipe via stdin/stdout. Format-agnostic.

use super::IOStream;
use crate::disc::DiscTitle;
use std::io::{self, Read, Write};

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

impl crate::pes::Stream for StdioStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        match &mut self.reader {
            Some(r) => crate::pes::PesFrame::deserialize(r),
            None => Err(io::Error::new(io::ErrorKind::Unsupported, "stdio opened for writing")),
        }
    }
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.writer {
            Some(w) => frame.serialize(w),
            None => Err(io::Error::new(io::ErrorKind::Unsupported, "stdio opened for reading")),
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.writer { w.flush()?; }
        Ok(())
    }
    fn info(&self) -> &DiscTitle { &self.disc_title }
}

impl IOStream for StdioStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }
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
            None => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stdio:// opened for output — cannot read",
            )),
        }
    }
}

impl Write for StdioStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer {
            Some(ref mut w) => w.write(buf),
            None => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stdio:// opened for input — cannot write",
            )),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self.writer {
            Some(ref mut w) => w.flush(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn stdio_output_write_errors_on_read() {
        let mut stream = StdioStream::output();
        let mut buf = [0u8; 10];
        let err = stream.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(err.to_string().contains("cannot read"), "got: {}", err);
    }

    #[test]
    fn stdio_input_read_errors_on_write() {
        let mut stream = StdioStream::input();
        let err = stream.write(&[0u8; 10]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(err.to_string().contains("cannot write"), "got: {}", err);
    }

    #[test]
    fn stdio_total_bytes_returns_none() {
        let input = StdioStream::input();
        assert_eq!(input.total_bytes(), None);
        let output = StdioStream::output();
        assert_eq!(output.total_bytes(), None);
    }
}
