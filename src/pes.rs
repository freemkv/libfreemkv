//! Stream — read PES frames in, write PES frames out.
//!
//! A stream is a stream. You read() from it or write() to it.
//! The stream handles its own format internally.
//!
//! disc.read()  → PES frame (sectors → decrypt → demux internally)
//! mkv.write(frame) → MKV file (mux internally)

/// One frame of elementary stream data.
#[derive(Debug, Clone)]
pub struct PesFrame {
    /// Track index (0-based, matches stream info track order).
    pub track: usize,
    /// Presentation timestamp in nanoseconds.
    pub pts: i64,
    /// True if this is a keyframe (IDR for video).
    pub keyframe: bool,
    /// Raw elementary stream data (NAL units, audio samples, etc).
    pub data: Vec<u8>,
}

impl PesFrame {
    /// Serialize to bytes: track(1) | pts(8) | keyframe(1) | len(4) | data
    pub fn serialize(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        if self.track > 255 {
            return Err(crate::error::Error::PesInvalidMagic.into());
        }
        if self.data.len() > u32::MAX as usize {
            return Err(crate::error::Error::PesFrameTooLarge {
                size: self.data.len(),
            }
            .into());
        }
        w.write_all(&[self.track as u8])?;
        w.write_all(&self.pts.to_le_bytes())?;
        w.write_all(&[if self.keyframe { 1 } else { 0 }])?;
        w.write_all(&(self.data.len() as u32).to_le_bytes())?;
        w.write_all(&self.data)
    }

    /// Deserialize from bytes. Returns None at EOF.
    pub fn deserialize(r: &mut dyn std::io::Read) -> std::io::Result<Option<Self>> {
        const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024; // 256 MB

        let mut header = [0u8; 14]; // 1 + 8 + 1 + 4
        match r.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let track = header[0] as usize;
        let pts = i64::from_le_bytes([
            header[1], header[2], header[3], header[4], header[5], header[6], header[7], header[8],
        ]);
        let keyframe = header[9] != 0;
        let len = u32::from_le_bytes([header[10], header[11], header[12], header[13]]) as usize;
        if len > MAX_FRAME_SIZE {
            return Err(crate::error::Error::PesFrameTooLarge { size: len }.into());
        }
        let mut data = vec![0u8; len];
        r.read_exact(&mut data)?;
        Ok(Some(Self {
            track,
            pts,
            keyframe,
            data,
        }))
    }

    /// Create from a codec::Frame with a track index.
    pub fn from_codec_frame(track: usize, frame: crate::mux::codec::Frame) -> Self {
        Self {
            track,
            pts: frame.pts_ns,
            keyframe: frame.keyframe,
            data: frame.data,
        }
    }
}

/// A PES frame stream. One trait per format — same type opens for read
/// (`open()` / `listen()` / `input()`) or write (`create()` / `connect()` /
/// `output()`). Calling the wrong-direction method returns a typed
/// `StreamReadOnly` / `StreamWriteOnly` error.
///
/// `Send` is required so streams can move across the producer/consumer
/// threads in autorip's mux pipeline.
pub trait Stream: Send {
    /// Read the next frame, or `Ok(None)` at end of stream. Returns
    /// `StreamWriteOnly` on a stream opened for writing.
    fn read(&mut self) -> std::io::Result<Option<PesFrame>>;

    /// Write a frame to the sink. Returns `StreamReadOnly` on a stream
    /// opened for reading.
    fn write(&mut self, frame: &PesFrame) -> std::io::Result<()>;

    /// Finalize the stream: flush buffered frames, write any container
    /// index (MKV `Cues`), close the underlying file/socket. Idempotent
    /// for read-only streams (no-op).
    fn finish(&mut self) -> std::io::Result<()>;

    /// Stream metadata. Stable across reads — implementors must return a
    /// consistent reference for the lifetime of the stream.
    fn info(&self) -> &crate::disc::DiscTitle;

    /// Codec initialization data for a track (SPS/PPS, AC-3 fscod, etc.).
    /// `None` for tracks that don't need codec_private (raw passthrough).
    fn codec_private(&self, _track: usize) -> Option<Vec<u8>> {
        None
    }

    /// True when `codec_private` is available for every video track —
    /// callers buffer input frames until this flips, since some output
    /// formats (MKV) can't write frames without codec init data.
    fn headers_ready(&self) -> bool {
        true
    }
}

/// Wraps any output stream and counts bytes written.
///
/// Progress tracking is a CLI concern — streams don't know their size.
/// Wrap the output with `CountingStream`, then query `bytes_written()`.
///
/// ```text
/// let mut output = CountingStream::new(libfreemkv::output(dest, &title)?);
/// while let Ok(Some(frame)) = input.read() {
///     output.write(&frame)?;
///     let pct = output.bytes_written() as f64 / total as f64;
/// }
/// ```
pub struct CountingStream {
    inner: Box<dyn Stream>,
    written: u64,
}

impl CountingStream {
    pub fn new(inner: Box<dyn Stream>) -> Self {
        Self { inner, written: 0 }
    }

    /// Total bytes of PES frame data written through this stream.
    pub fn bytes_written(&self) -> u64 {
        self.written
    }
}

impl Stream for CountingStream {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        self.inner.read()
    }

    fn write(&mut self, frame: &PesFrame) -> std::io::Result<()> {
        self.written += frame.data.len() as u64;
        self.inner.write(frame)
    }

    fn finish(&mut self) -> std::io::Result<()> {
        self.inner.finish()
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        self.inner.info()
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        self.inner.codec_private(track)
    }

    fn headers_ready(&self) -> bool {
        self.inner.headers_ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::DiscTitle;

    fn make_frame(track: usize, pts: i64) -> PesFrame {
        PesFrame {
            track,
            pts,
            keyframe: track == 0 && pts == 0,
            data: vec![track as u8, (pts & 0xff) as u8, 0xAA],
        }
    }

    /// Minimal in-memory `Stream` for trait-shape tests. `read` replays
    /// pre-seeded frames; `write` collects them.
    struct MockStream {
        read_queue: std::vec::IntoIter<PesFrame>,
        written: Vec<PesFrame>,
        title: DiscTitle,
    }

    impl MockStream {
        fn new(read_frames: Vec<PesFrame>) -> Self {
            Self {
                read_queue: read_frames.into_iter(),
                written: Vec::new(),
                title: DiscTitle::empty(),
            }
        }
    }

    impl Stream for MockStream {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            Ok(self.read_queue.next())
        }

        fn write(&mut self, frame: &PesFrame) -> std::io::Result<()> {
            self.written.push(frame.clone());
            Ok(())
        }

        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        fn info(&self) -> &DiscTitle {
            &self.title
        }
    }

    #[test]
    fn stream_read_yields_frames_then_eof() {
        let frames = vec![make_frame(0, 0), make_frame(1, 1_000), make_frame(0, 2_000)];
        let mut s = MockStream::new(frames.clone());

        let f0 = s.read().unwrap().expect("first frame");
        assert_eq!(f0.track, frames[0].track);
        assert_eq!(f0.pts, frames[0].pts);
        assert!(f0.keyframe);

        let f1 = s.read().unwrap().expect("second frame");
        assert_eq!(f1.pts, frames[1].pts);

        let f2 = s.read().unwrap().expect("third frame");
        assert_eq!(f2.pts, frames[2].pts);

        assert!(s.read().unwrap().is_none());
        assert!(s.read().unwrap().is_none()); // idempotent at EOF
    }

    #[test]
    fn stream_write_collects_then_finishes() {
        let mut s = MockStream::new(Vec::new());
        let frames = [make_frame(0, 0), make_frame(1, 100), make_frame(2, 200)];

        for f in &frames {
            s.write(f).unwrap();
        }
        assert_eq!(s.written.len(), 3);
        s.finish().unwrap();
    }

    #[test]
    fn stream_via_dyn_object() {
        let mut s: Box<dyn Stream> = Box::new(MockStream::new(vec![make_frame(0, 0)]));
        let frame = s.read().unwrap().expect("first frame");
        s.write(&frame).unwrap();
        let _ = s.info();
        s.finish().unwrap();
    }
}
