//! StdioStream — PES frames via stdin/stdout with FMKV metadata header.
//!
//! The FMKV header carries stream metadata (PIDs, codecs, languages, codec_privates)
//! so the receiving end can set up muxing without scanning the content.

use super::meta;
use crate::disc::DiscTitle;
use std::io::{self, Write};

/// Stdio stream — reads PES from stdin, writes PES to stdout.
/// FMKV metadata header is written/read automatically.
pub struct StdioStream {
    disc_title: DiscTitle,
    reader: Option<io::Stdin>,
    writer: Option<io::BufWriter<io::Stdout>>,
    header_written: bool,
    header_read: bool,
    stored_codec_privates: Vec<Option<Vec<u8>>>,
}

impl StdioStream {
    /// Create a stdio stream for reading (stdin).
    pub fn input() -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            reader: Some(io::stdin()),
            writer: None,
            header_written: false,
            header_read: false,
            stored_codec_privates: Vec::new(),
        }
    }

    /// Create a stdio stream for writing (stdout).
    pub fn output(title: &DiscTitle) -> Self {
        Self {
            disc_title: title.clone(),
            reader: None,
            writer: Some(io::BufWriter::new(io::stdout())),
            header_written: false,
            header_read: false,
            stored_codec_privates: Vec::new(),
        }
    }

    /// Read the FMKV metadata header from stdin on first read.
    fn ensure_header_read(&mut self) -> io::Result<()> {
        if self.header_read {
            return Ok(());
        }
        self.header_read = true;
        if let Some(ref mut r) = self.reader {
            if let Ok(Some(m)) = meta::read_header(r) {
                let title = m.to_title();
                self.stored_codec_privates = title.codec_privates.clone();
                self.disc_title = title;
            }
        }
        Ok(())
    }
}

impl crate::pes::Stream for StdioStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        self.ensure_header_read()?;
        match &mut self.reader {
            Some(r) => crate::pes::PesFrame::deserialize(r),
            None => Err(crate::error::Error::StreamWriteOnly.into()),
        }
    }
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.writer {
            Some(ref mut w) => {
                if !self.header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(w, &m)?;
                    }
                    self.header_written = true;
                }
                frame.serialize(w)
            }
            None => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.writer { w.flush()?; }
        Ok(())
    }
    fn info(&self) -> &DiscTitle { &self.disc_title }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        self.stored_codec_privates.get(track).and_then(|c| c.clone())
    }

    fn headers_ready(&self) -> bool {
        // After first read(), header is parsed and codec_privates populated
        self.header_read || self.writer.is_some()
    }
}
