//! StdioStream — PES frames via stdin/stdout.

use crate::disc::DiscTitle;
use std::io::{self, Write};

/// Stdio stream — reads PES from stdin, writes PES to stdout.
pub struct StdioStream {
    disc_title: DiscTitle,
    reader: Option<io::Stdin>,
    writer: Option<io::BufWriter<io::Stdout>>,
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
    pub fn output(title: &DiscTitle) -> Self {
        Self {
            disc_title: title.clone(),
            reader: None,
            writer: Some(io::BufWriter::new(io::stdout())),
        }
    }
}

impl crate::pes::Stream for StdioStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        match &mut self.reader {
            Some(r) => crate::pes::PesFrame::deserialize(r),
            None => Err(crate::error::Error::StreamWriteOnly.into()),
        }
    }
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.writer {
            Some(w) => frame.serialize(w),
            None => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.writer { w.flush()?; }
        Ok(())
    }
    fn info(&self) -> &DiscTitle { &self.disc_title }
}
