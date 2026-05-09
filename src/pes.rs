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

/// Deprecated; use [`FrameSource`] for read-only sources or [`FrameSink`]
/// for write-only sinks. The runtime direction-error semantics
/// (`StreamReadOnly` / `StreamWriteOnly` from a wrong-direction call) are
/// removed in 0.18 — direction is type-checked.
#[deprecated(
    since = "0.18.0",
    note = "use FrameSource (read-only) or FrameSink (write-only) instead"
)]
pub trait Stream {
    /// Read the next frame, or `Ok(None)` at end of stream.
    fn read(&mut self) -> std::io::Result<Option<PesFrame>>;

    /// Write a frame to the sink.
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

/// Read-only source of PES frames.
///
/// Replaces the read half of the deprecated [`Stream`] trait. Implementors
/// produce frames via [`read`](FrameSource::read) and never accept writes —
/// passing a `FrameSource` where a sink is expected is a compile error,
/// not a runtime `E9001`.
///
/// `info()` returns the source's `DiscTitle` metadata (track list, codec
/// info, duration). It must be stable across the lifetime of the source.
///
/// `codec_private(track)` exposes per-track codec initialization data
/// (H.264 SPS/PPS, HEVC VPS/SPS/PPS, AC-3 fscod, etc.) that downstream
/// muxers may need before any frame is written. `headers_ready()` returns
/// false until enough input frames have been seen to populate every video
/// track's codec-private blob — callers buffer frames they read until
/// `headers_ready()` returns true.
pub trait FrameSource: Send {
    /// Read the next frame, or `Ok(None)` at end of stream.
    fn read(&mut self) -> std::io::Result<Option<PesFrame>>;

    /// Source metadata. Stable across reads — implementors must return a
    /// consistent reference for the lifetime of the source.
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

/// Write-only sink of PES frames.
///
/// Replaces the write half of the deprecated [`Stream`] trait. Implementors
/// accept frames via [`write`](FrameSink::write) and finalize via
/// [`finish`](FrameSink::finish) — passing a `FrameSink` where a source is
/// expected is a compile error, not a runtime `E9000`.
///
/// `finish` takes `Box<Self>` (rather than `&mut self` like `Stream::finish`)
/// so that finalization is a one-shot terminal operation: callers cannot use
/// the sink after `finish` returns. This is the standard idiom for terminal
/// methods on `dyn Trait` objects.
pub trait FrameSink: Send {
    /// Write a frame to the sink.
    fn write(&mut self, frame: &PesFrame) -> std::io::Result<()>;

    /// Finalize the sink: flush buffered frames, write any container index
    /// (e.g. MKV `Cues`), close the underlying file/socket. Consumes the
    /// sink — callers cannot use it afterwards.
    fn finish(self: Box<Self>) -> std::io::Result<()>;

    /// Sink metadata. Stable across writes — implementors must return a
    /// consistent reference for the lifetime of the sink.
    fn info(&self) -> &crate::disc::DiscTitle;
}

// Bridge: any **`Send`** type implementing the deprecated `Stream` trait
// is also a `FrameSource`. This lets existing concrete `Stream` impls in
// `mux/*` satisfy `FrameSource` bounds without per-type migration during
// the 0.18 deprecation window.
//
// **Send caveat (read me before tightening `Stream` itself).** This
// blanket carries a `T: Send` bound rather than promoting `Send` to a
// supertrait of `Stream`, because not every concrete in-tree `Stream`
// impl is `Send`: `MkvStream` and `M2tsStream` carry `Box<dyn Read>`
// and `Box<dyn Write>` fields whose trait objects don't include `Send`.
// Adding `Stream: Send` would force a wider audit (every `Box<dyn Read>`
// becomes `Box<dyn Read + Send>`) than this commit is taking on, and
// the type-level migration target is `FrameSource` / `FrameSink`
// directly anyway. Consequence: coercing a non-Send `Box<dyn Stream>`
// (the return shape of `crate::mux::input` / `output`) to
// `Box<dyn FrameSource>` will fail with a `T: Send` trait-bound error.
// The fix on the consumer side is to construct a Send-compliant
// `FrameSource` / `FrameSink` directly rather than relying on this
// bridge for non-Send streams.
//
// Note: `FrameSink` cannot be blanket-impl'd from `Stream` because
// `Stream::finish` takes `&mut self` while `FrameSink::finish` takes
// `Box<Self>`; concrete types will be migrated in a follow-up commit.
#[allow(deprecated)]
impl<T: Stream + Send + ?Sized> FrameSource for T {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        <Self as Stream>::read(self)
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        <Self as Stream>::info(self)
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        <Self as Stream>::codec_private(self, track)
    }

    fn headers_ready(&self) -> bool {
        <Self as Stream>::headers_ready(self)
    }
}

/// Wraps any output stream and counts bytes written.
///
/// Progress tracking is a CLI concern — streams don't know their size.
/// Wrap the output with CountingStream, then query bytes_written().
///
/// ```text
/// let mut output = CountingStream::new(libfreemkv::output(dest, &title)?);
/// while let Ok(Some(frame)) = input.read() {
///     output.write(&frame)?;
///     let pct = output.bytes_written() as f64 / total as f64;
/// }
/// ```
#[allow(deprecated)]
pub struct CountingStream {
    inner: Box<dyn Stream>,
    written: u64,
}

#[allow(deprecated)]
impl CountingStream {
    pub fn new(inner: Box<dyn Stream>) -> Self {
        Self { inner, written: 0 }
    }

    /// Total bytes of PES frame data written through this stream.
    pub fn bytes_written(&self) -> u64 {
        self.written
    }
}

#[allow(deprecated)]
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

    /// Direct `FrameSource` impl (not via the deprecated `Stream` blanket).
    struct MockSource {
        frames: std::vec::IntoIter<PesFrame>,
        title: DiscTitle,
    }

    impl MockSource {
        fn new(frames: Vec<PesFrame>) -> Self {
            Self {
                frames: frames.into_iter(),
                title: DiscTitle::empty(),
            }
        }
    }

    impl FrameSource for MockSource {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            Ok(self.frames.next())
        }

        fn info(&self) -> &DiscTitle {
            &self.title
        }
    }

    /// Direct `FrameSink` impl (not via the deprecated `Stream` blanket).
    struct MockSink {
        collected: Vec<PesFrame>,
        title: DiscTitle,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                collected: Vec::new(),
                title: DiscTitle::empty(),
            }
        }
    }

    impl FrameSink for MockSink {
        fn write(&mut self, frame: &PesFrame) -> std::io::Result<()> {
            self.collected.push(frame.clone());
            Ok(())
        }

        fn finish(self: Box<Self>) -> std::io::Result<()> {
            // Drop self; in real sinks this is where flush/fsync/close happens.
            Ok(())
        }

        fn info(&self) -> &DiscTitle {
            &self.title
        }
    }

    /// Variant of `MockSink` whose `finish` returns the collected frames so
    /// the test can assert on them after the consuming `Box<Self>` call.
    struct CollectingSink {
        collected: Vec<PesFrame>,
        title: DiscTitle,
    }

    impl CollectingSink {
        fn new() -> Self {
            Self {
                collected: Vec::new(),
                title: DiscTitle::empty(),
            }
        }
    }

    impl FrameSink for CollectingSink {
        fn write(&mut self, frame: &PesFrame) -> std::io::Result<()> {
            self.collected.push(frame.clone());
            Ok(())
        }

        fn finish(self: Box<Self>) -> std::io::Result<()> {
            // Real CollectingSink consumers would expose `take()` before
            // finish; this trait method just confirms the boxed signature
            // compiles and runs.
            Ok(())
        }

        fn info(&self) -> &DiscTitle {
            &self.title
        }
    }

    #[test]
    fn frame_source_yields_frames_then_eof() {
        let frames = vec![make_frame(0, 0), make_frame(1, 1_000), make_frame(0, 2_000)];
        let mut src = MockSource::new(frames.clone());

        let f0 = src.read().unwrap().expect("first frame");
        assert_eq!(f0.track, frames[0].track);
        assert_eq!(f0.pts, frames[0].pts);
        assert!(f0.keyframe);

        let f1 = src.read().unwrap().expect("second frame");
        assert_eq!(f1.track, frames[1].track);
        assert_eq!(f1.pts, frames[1].pts);

        let f2 = src.read().unwrap().expect("third frame");
        assert_eq!(f2.track, frames[2].track);
        assert_eq!(f2.pts, frames[2].pts);

        assert!(src.read().unwrap().is_none());
        assert!(src.read().unwrap().is_none()); // idempotent at EOF
    }

    #[test]
    fn frame_sink_collects_then_finishes() {
        let mut sink = MockSink::new();
        let frames = [make_frame(0, 0), make_frame(1, 100), make_frame(2, 200)];

        for f in &frames {
            sink.write(f).unwrap();
        }
        assert_eq!(sink.collected.len(), 3);
        assert_eq!(sink.collected[0].pts, 0);
        assert_eq!(sink.collected[1].pts, 100);
        assert_eq!(sink.collected[2].pts, 200);

        // Box-and-finish — the `self: Box<Self>` shape must compile and run.
        Box::new(sink).finish().unwrap();
    }

    #[test]
    fn frame_sink_via_dyn_object() {
        let frames = [make_frame(0, 0), make_frame(0, 33)];
        let mut sink: Box<dyn FrameSink> = Box::new(CollectingSink::new());

        for f in &frames {
            sink.write(f).unwrap();
        }
        // info() routes through the trait object.
        let _ = sink.info();
        sink.finish().unwrap();
    }

    /// The deprecated blanket impl: any concrete `Stream` should also act as
    /// a `FrameSource`. `NullStream` has the simplest constructor of every
    /// concrete `Stream` impl in `mux/*`, so it's the smallest credible
    /// witness that the bridge compiles and dispatches correctly.
    #[test]
    #[allow(deprecated)]
    fn deprecated_stream_satisfies_frame_source() {
        let title = DiscTitle::empty();
        let mut null = crate::mux::NullStream::new(&title);

        let src: &mut dyn FrameSource = &mut null;
        // NullStream::read returns Ok(None) — it's a write-only sink.
        assert!(src.read().unwrap().is_none());
        // info() forwards through the blanket impl.
        let _ = src.info();
        assert!(src.headers_ready());
    }
}
