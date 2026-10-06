//! StdioStream — PES frames via stdin/stdout with FMKV metadata header.
//!
//! The FMKV header carries stream metadata (PIDs, codecs, languages, codec_privates)
//! so the receiving end can set up muxing without scanning the content.

use super::meta;
use crate::disc::DiscTitle;
use std::io::{self, Read, Write};

/// Stdio stream — reads PES from stdin, writes PES to stdout.
/// FMKV metadata header is written/read automatically.
pub struct StdioStream {
    disc_title: DiscTitle,
    // Boxed so tests can drive the header logic without real stdin/stdout.
    reader: Option<Box<dyn Read + Send + Sync>>,
    writer: Option<io::BufWriter<Box<dyn Write + Send + Sync>>>,
    header_written: bool,
    header_read: bool,
    /// True once an FMKV header was actually parsed on the read side
    /// (set only inside the `Some(meta)` arm). Distinct from
    /// `header_read`, which is true after the first read attempt even
    /// when no header was present — `headers_ready()` must gate on the
    /// metadata actually being available, not merely on having looked.
    meta_parsed: bool,
    /// Write side: per-track decoder timing, sent in the header. Read side: the
    /// header's timing.
    timings: Vec<crate::pes::TrackTiming>,
    /// Frames carry the DiscardPadding extension (FMKV header v2).
    padded: bool,
}

impl StdioStream {
    /// Create a stdio stream for reading (stdin).
    pub fn input() -> Self {
        Self::input_staged(false)
    }

    // `input` whose decryption stage passes ciphertext when `raw`.
    pub(crate) fn input_staged(raw: bool) -> Self {
        Self::from_reader(Box::new(crate::sector::stage::Stage::lazy(
            io::stdin(),
            raw,
        )))
    }

    fn from_reader(reader: Box<dyn Read + Send + Sync>) -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            reader: Some(reader),
            writer: None,
            header_written: false,
            header_read: false,
            meta_parsed: false,
            timings: Vec::new(),
            padded: false,
        }
    }

    /// Create a stdio stream for writing (stdout).
    pub fn output(title: &DiscTitle) -> Self {
        Self::from_writer(title, Box::new(io::stdout()))
    }

    fn from_writer(title: &DiscTitle, writer: Box<dyn Write + Send + Sync>) -> Self {
        Self {
            disc_title: title.clone(),
            reader: None,
            writer: Some(io::BufWriter::new(writer)),
            header_written: false,
            header_read: false,
            meta_parsed: false,
            timings: Vec::new(),
            padded: false,
        }
    }

    // Write the FMKV header to stdout exactly once, even for zero-frame
    // output, keeping the wire protocol symmetric with read_header().
    fn ensure_header_written(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.writer
            && !self.header_written
        {
            let m = meta::M2tsMeta::from_title(&self.disc_title).with_timings(&self.timings);
            meta::write_header(w, &m)?;
            self.padded = m.frame_padding;
            self.header_written = true;
        }
        Ok(())
    }

    /// Read the FMKV metadata header from stdin on first read.
    fn ensure_header_read(&mut self) -> io::Result<()> {
        if self.header_read {
            return Ok(());
        }
        if let Some(ref mut r) = self.reader {
            // stdin cannot rewind, so any header failure after bytes were consumed is an error
            // (never a misaligned frame read); only a clean zero-byte EOF is headerless.
            let mut counted = Counted { inner: r, n: 0 };
            let header = meta::read_header(&mut counted)?;
            if header.is_none() && counted.n > 0 {
                return Err(crate::error::Error::NoMetadata.into());
            }
            if let Some(m) = header {
                self.disc_title = m.to_title();
                self.timings = (0..self.disc_title.streams.len())
                    .map(|i| m.timing(i))
                    .collect();
                self.padded = m.frame_padding;
                self.meta_parsed = true;
            }
        }
        // Set only on success: a failed header leaves the next read() failing, not parsing
        // mid-header bytes as frames.
        self.header_read = true;
        Ok(())
    }
}

// Counts the bytes a header probe consumed from a non-rewindable reader.
struct Counted<'a, R: ?Sized> {
    inner: &'a mut R,
    n: u64,
}

impl<R: Read + ?Sized> Read for Counted<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let got = self.inner.read(buf)?;
        self.n += got as u64;
        Ok(got)
    }
}

impl StdioStream {
    /// The title being read or written (both directions open this type).
    pub fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    // Read the FMKV header now, so `info()` is the stream's title before the first frame
    // (a stream selection is resolved against it at open).
    pub(crate) fn prime(&mut self) -> io::Result<()> {
        self.ensure_header_read()
    }
}

impl crate::pes::PesSource for StdioStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        self.ensure_header_read()?;
        match &mut self.reader {
            Some(r) => crate::pes::PesFrame::deserialize_ext(r, self.padded),
            None => Err(crate::error::Error::StreamWriteOnly.into()),
        }
    }

    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    // Read side: the header's timing, available once the first read parsed it.
    fn track_timing(&self, track: usize) -> crate::pes::TrackTiming {
        self.timings.get(track).copied().unwrap_or_default()
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        // Single source of truth: the title's own codec_privates. (The
        // previous `stored_codec_privates` field was a redundant clone
        // of exactly this, populated from the same header.)
        self.disc_title
            .codec_privates
            .get(track)
            .and_then(|c| c.clone())
    }

    fn headers_ready(&self) -> bool {
        // Write side: title supplied up front, always ready. Read side: ready only once an
        // FMKV header was parsed — gating on `header_read` alone would claim readiness for a
        // headerless stream (codec_private() all None), starving the MKV writer of init data.
        self.writer.is_some() || self.meta_parsed
    }
}

impl crate::pes::PesSink for StdioStream {
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        if self.writer.is_none() {
            return Err(crate::error::Error::StreamReadOnly.into());
        }
        self.ensure_header_written()?;
        match &mut self.writer {
            Some(w) => frame.serialize_ext(w, self.padded),
            None => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        // Emit the header even when write() was never called, so a zero-frame
        // title still produces the FMKV magic + metadata header on stdout
        // (symmetric with the read side's read_header()).
        self.ensure_header_written()?;
        if let Some(w) = &mut self.writer {
            w.flush()?;
        }
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn set_track_timing(
        &mut self,
        track: usize,
        timing: crate::pes::TrackTiming,
    ) -> io::Result<()> {
        if self.writer.is_none() {
            return Err(crate::error::Error::StreamReadOnly.into());
        }
        if self.header_written {
            return Err(crate::error::Error::StreamHeaderWritten.into());
        }
        let tracks = self.disc_title.streams.len();
        super::network::set_timing(&mut self.timings, track, timing, tracks)
    }
}

#[cfg(test)]
#[path = "stdio_tests.rs"]
mod tests;
