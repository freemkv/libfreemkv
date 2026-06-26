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
    /// True once an FMKV header was actually parsed on the read side
    /// (set only inside the `Some(meta)` arm). Distinct from
    /// `header_read`, which is true after the first read attempt even
    /// when no header was present — `headers_ready()` must gate on the
    /// metadata actually being available, not merely on having looked.
    meta_parsed: bool,
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
            meta_parsed: false,
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
            meta_parsed: false,
        }
    }

    /// Write the FMKV metadata header to stdout exactly once, before any
    /// frames. Always writes (even when the title has no streams) so a
    /// zero-frame output stream still emits the magic + metadata header,
    /// keeping the wire protocol symmetric with the read side's read_header().
    fn ensure_header_written(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.writer {
            if !self.header_written {
                let m = meta::M2tsMeta::from_title(&self.disc_title);
                meta::write_header(w, &m)?;
                self.header_written = true;
            }
        }
        Ok(())
    }

    /// Read the FMKV metadata header from stdin on first read.
    fn ensure_header_read(&mut self) -> io::Result<()> {
        if self.header_read {
            return Ok(());
        }
        self.header_read = true;
        if let Some(ref mut r) = self.reader {
            // Propagate real header errors. read_header consumes bytes
            // from the unbuffered stdin BEFORE it can fail (oversized
            // length, bad JSON, partial read), so swallowing the Err
            // would leave the stream misaligned and PesFrame::deserialize
            // would then read garbage. `?` surfaces the true error;
            // Ok(None) (genuine magic mismatch / clean EOF) stays a
            // non-error and leaves the empty default title in place.
            if let Some(m) = meta::read_header(r)? {
                self.disc_title = m.to_title();
                self.meta_parsed = true;
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
        if self.writer.is_none() {
            return Err(crate::error::Error::StreamReadOnly.into());
        }
        self.ensure_header_written()?;
        match &mut self.writer {
            Some(w) => frame.serialize(w),
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
        // Write side: caller supplied the title up front, so headers are
        // always ready. Read side: ready only once an FMKV header was
        // actually parsed — gating on `header_read` alone would claim
        // readiness for a headerless stream whose codec_private() is None
        // for every track, starving the downstream MKV writer of init
        // data. A genuinely headerless stream never flips ready (the
        // caller must then fall back to its own codec detection).
        self.writer.is_some() || self.meta_parsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pes::Stream as _;

    fn title_with_codec_privates() -> DiscTitle {
        use crate::disc::{Codec, Stream, VideoStream};
        let mut t = DiscTitle::empty();
        t.playlist = "StdioTitle".into();
        t.streams.push(Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: crate::disc::Resolution::R2160p,
            frame_rate: crate::disc::FrameRate::F23_976,
            hdr: crate::disc::HdrFormat::Hdr10,
            color_space: crate::disc::ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }));
        // Index 0 = the video stream's codec init data.
        t.codec_privates = vec![Some(vec![0xDE, 0xAD, 0xBE, 0xEF])];
        t
    }

    /// write() on a read-opened (input) stdio stream must return
    /// StreamReadOnly WITHOUT touching stdin/stdout — the writer.is_none()
    /// guard returns before any header logic runs. (Returning Ok would let a
    /// caller silently discard frames into a read-only stream.)
    #[test]
    fn write_on_input_stream_is_read_only_error() {
        let mut s = StdioStream::input();
        let frame = crate::pes::PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![1, 2, 3],
            duration_ns: None,
        };
        let err = s.write(&frame).expect_err("write on input must error");
        // E_STREAM_READ_ONLY (9000) maps to Unsupported.
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    /// read() on a write-opened (output) stdio stream must return
    /// StreamWriteOnly. ensure_header_read is a no-op when reader is None,
    /// so this never blocks on real stdin.
    #[test]
    fn read_on_output_stream_is_write_only_error() {
        let mut s = StdioStream::output(&DiscTitle::empty());
        let err = s.read().expect_err("read on output must error");
        // E_STREAM_WRITE_ONLY (9001) maps to Unsupported.
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    /// The write side has the title up front, so headers_ready() must be
    /// true immediately — the downstream MKV writer needs this to start
    /// writing the container header without waiting for a (nonexistent)
    /// read-side header parse.
    #[test]
    fn output_headers_ready_immediately() {
        let s = StdioStream::output(&DiscTitle::empty());
        assert!(s.headers_ready(), "write side is always header-ready");
    }

    /// A fresh read (input) side has NOT parsed any header yet, so
    /// headers_ready() must be false (meta_parsed=false, writer=None).
    /// Claiming readiness before the header is parsed would starve the MKV
    /// writer of codec init data.
    #[test]
    fn input_not_header_ready_before_any_read() {
        let s = StdioStream::input();
        assert!(
            !s.headers_ready(),
            "read side not ready until header parsed"
        );
    }

    /// codec_private(track) on the write side returns the title's own
    /// codec_private for that track (single source of truth = the title).
    #[test]
    fn output_codec_private_comes_from_title() {
        let s = StdioStream::output(&title_with_codec_privates());
        assert_eq!(
            s.codec_private(0).as_deref(),
            Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]),
            "track 0 codec_private must mirror title.codec_privates[0]"
        );
        // Out-of-range track → None (no panic, no wrong-track data).
        assert_eq!(s.codec_private(99), None);
    }

    /// A fresh input stream defaults to an empty title until a header is
    /// parsed — info() must not invent stream metadata.
    #[test]
    fn input_default_title_is_empty() {
        let s = StdioStream::input();
        assert!(s.info().streams.is_empty());
        assert_eq!(s.codec_private(0), None);
    }
}
