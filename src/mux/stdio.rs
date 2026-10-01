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
mod tests {
    use super::*;
    use crate::pes::{PesSink as _, PesSource as _};

    // The pub type keeps the auto traits it had when it held Stdin/Stdout.
    #[test]
    fn stdio_stream_is_send_and_sync() {
        fn check<T: Send + Sync>() {}
        check::<StdioStream>();
    }

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

    // write() on an input stream must error StreamReadOnly without touching
    // stdin/stdout (writer.is_none() guard short-circuits header logic),
    // not silently discard frames.
    #[test]
    fn write_on_input_stream_is_read_only_error() {
        let mut s = StdioStream::input();
        let frame = crate::pes::PesFrame {
            discard_padding_ns: 0,
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

    // read() on an output stream must error StreamWriteOnly;
    // ensure_header_read is a no-op when reader is None, so this
    // never blocks on real stdin.
    #[test]
    fn read_on_output_stream_is_write_only_error() {
        let mut s = StdioStream::output(&DiscTitle::empty());
        let err = s.read().expect_err("read on output must error");
        // E_STREAM_WRITE_ONLY (9001) maps to Unsupported.
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    // The write side has the title up front, so headers_ready() must be
    // true immediately — the MKV writer starts the container header
    // without waiting for a (nonexistent) read-side header parse.
    #[test]
    fn output_headers_ready_immediately() {
        let s = StdioStream::output(&DiscTitle::empty());
        assert!(s.headers_ready(), "write side is always header-ready");
    }

    // A fresh input side has not parsed any header yet, so headers_ready()
    // must be false; claiming readiness early would starve the MKV
    // writer of codec init data.
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

    fn frame(data: &[u8]) -> crate::pes::PesFrame {
        crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 7,
            keyframe: true,
            data: data.to_vec(),
            duration_ns: None,
        }
    }

    fn reader(bytes: Vec<u8>) -> StdioStream {
        StdioStream::from_reader(Box::new(io::Cursor::new(bytes)))
    }

    fn code(e: &io::Error) -> Option<u16> {
        crate::error::error_code(e)
    }

    #[derive(Clone, Default)]
    struct Shared(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl Write for Shared {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // Write side -> read side over the FMKV wire: header parsed on the first read (title,
    // codec_private, timing), headers_ready only after it, timing frozen once written.
    #[test]
    fn header_round_trips_and_gates_readiness() {
        let out = Shared::default();
        let mut w = StdioStream::from_writer(&title_with_codec_privates(), Box::new(out.clone()));
        let timing = crate::pes::TrackTiming {
            codec_delay_ns: 5,
            seek_preroll_ns: 9,
        };
        w.set_track_timing(0, timing).unwrap();
        w.write(&frame(&[1, 2, 3])).unwrap();
        let e = w.set_track_timing(0, timing).unwrap_err();
        assert_eq!(code(&e), Some(crate::error::E_STREAM_HEADER_WRITTEN));
        w.finish().unwrap();

        let mut r = reader(out.0.lock().unwrap().clone());
        assert!(!r.headers_ready());
        let f = r.read().unwrap().unwrap();
        assert_eq!((f.pts, f.data), (7, vec![1, 2, 3]));
        assert!(r.headers_ready());
        assert_eq!(r.info().playlist, "StdioTitle");
        assert_eq!(r.codec_private(0), Some(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(r.track_timing(0), timing);
        assert!(r.read().unwrap().is_none());
    }

    // prime() reads the header up front: info() is the stream's title before any frame,
    // and the first read still returns the first frame.
    #[test]
    fn prime_reads_the_header_before_the_first_frame() {
        let out = Shared::default();
        let mut w = StdioStream::from_writer(&title_with_codec_privates(), Box::new(out.clone()));
        w.write(&frame(&[1, 2, 3])).unwrap();
        w.finish().unwrap();

        let mut r = reader(out.0.lock().unwrap().clone());
        assert_eq!(r.info().streams.len(), 0);
        r.prime().unwrap();
        assert_eq!(r.info().playlist, "StdioTitle");
        assert_eq!(
            r.info().streams.len(),
            title_with_codec_privates().streams.len()
        );
        let f = r.read().unwrap().unwrap();
        assert_eq!(f.data, vec![1, 2, 3]);
    }

    // A zero-byte stdin is a clean headerless end, and never header-ready.
    #[test]
    fn empty_input_is_clean_eof_not_ready() {
        let mut r = reader(Vec::new());
        assert!(r.read().unwrap().is_none());
        assert!(!r.headers_ready(), "no header was parsed");
    }

    // Bytes that are not an FMKV header were consumed from a non-rewindable stream, so the
    // frames after them cannot be aligned: NoMetadata, not a misread frame.
    #[test]
    fn non_fmkv_input_is_no_metadata() {
        for lead in [&[0x00u8][..], b"FMKX\0\x01\0\0"] {
            let mut bytes = lead.to_vec();
            frame(&[4, 5]).serialize(&mut bytes).unwrap();
            let e = reader(bytes).read().unwrap_err();
            assert_eq!(code(&e), Some(crate::error::E_NO_METADATA), "{lead:?}");
        }
    }

    // A header that failed mid-way is not skipped on a retry: the next read must not parse
    // the rest of the header region as frames.
    #[test]
    fn failed_header_is_not_retried_as_frames() {
        let mut bytes = b"FMKV\0\x01\0\0".to_vec();
        bytes.extend_from_slice(&u32::MAX.to_be_bytes()); // over the JSON size cap
        frame(&[4, 5]).serialize(&mut bytes).unwrap();
        let mut r = reader(bytes);
        assert_eq!(
            code(&r.read().unwrap_err()),
            Some(crate::error::E_NO_METADATA)
        );
        assert!(r.read().is_err(), "retry must not yield a frame");
    }
}
