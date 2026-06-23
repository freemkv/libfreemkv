//! Stream — read PES frames in, write PES frames out.
//!
//! A stream is a stream. You read() from it or write() to it.
//! The stream handles its own format internally.
//!
//! disc.read()  → PES frame (sectors → decrypt → demux internally)
//! mkv.write(frame) → MKV file (mux internally)

/// Maximum frame payload size, shared by `serialize` and `deserialize`
/// so the wire format round-trips: any frame that serializes can be read
/// back. A frame larger than this is rejected on write rather than written
/// and then hard-erroring mid-stream on read.
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024; // 256 MiB

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
    /// Optional duration in nanoseconds. Carried on the wire (8 bytes
    /// little-endian, `u64::MAX` as the sentinel for `None`). Set by
    /// the PGS parser so the MKV muxer can emit `BlockDuration`; also
    /// preserved across network:// and stdio:// hops.
    pub duration_ns: Option<u64>,
}

/// Sentinel value for `duration_ns` on the wire: `u64::MAX` means `None`.
/// Valid durations are always much smaller (u64::MAX ns ≈ 584 years).
const DURATION_NONE_SENTINEL: u64 = u64::MAX;

impl PesFrame {
    /// Serialize to bytes:
    /// track(1) | pts(8 LE) | keyframe(1) | duration_ns(8 LE) | len(4 LE) | data
    ///
    /// `duration_ns` is encoded as `u64::MAX` when `None`, or the value when `Some`.
    pub fn serialize(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        if self.track > 255 {
            return Err(crate::error::Error::PesTrackTooLarge { track: self.track }.into());
        }
        // Enforce the same ceiling the reader uses, so a frame that writes
        // can always be read back (round-trippable wire format).
        if self.data.len() > MAX_FRAME_SIZE {
            return Err(crate::error::Error::PesFrameTooLarge {
                size: self.data.len(),
            }
            .into());
        }
        let duration_wire = self.duration_ns.unwrap_or(DURATION_NONE_SENTINEL);
        w.write_all(&[self.track as u8])?;
        w.write_all(&self.pts.to_le_bytes())?;
        w.write_all(&[if self.keyframe { 1 } else { 0 }])?;
        w.write_all(&duration_wire.to_le_bytes())?;
        w.write_all(&(self.data.len() as u32).to_le_bytes())?;
        w.write_all(&self.data)
    }

    /// Deserialize from bytes. Returns None at a clean end of stream.
    ///
    /// A clean EOF is exactly zero bytes available before the next frame.
    /// A partial header (1-21 bytes, e.g. a crash or short write) is a real
    /// error (`UnexpectedEof`), not silently treated as EOF — otherwise
    /// truncated `.pes` data would be accepted as a graceful end.
    pub fn deserialize(r: &mut dyn std::io::Read) -> std::io::Result<Option<Self>> {
        // Probe one byte first to distinguish clean EOF from a truncated
        // header.
        let mut first = [0u8; 1];
        match r.read(&mut first) {
            Ok(0) => return Ok(None), // clean EOF, no frame started
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                // Retry-once on EINTR before committing to the header read.
                match r.read(&mut first) {
                    Ok(0) => return Ok(None),
                    Ok(_) => {}
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        }

        let mut header = [0u8; 22]; // 1 + 8 + 1 + 8 + 4
        header[0] = first[0];
        // The remaining 21 header bytes must be present; a short read here is
        // a truncated frame, propagated as UnexpectedEof.
        r.read_exact(&mut header[1..])?;
        let track = header[0] as usize;
        let pts = i64::from_le_bytes([
            header[1], header[2], header[3], header[4], header[5], header[6], header[7], header[8],
        ]);
        let keyframe = header[9] != 0;
        let duration_wire = u64::from_le_bytes([
            header[10], header[11], header[12], header[13], header[14], header[15], header[16],
            header[17],
        ]);
        let duration_ns = if duration_wire == DURATION_NONE_SENTINEL {
            None
        } else {
            Some(duration_wire)
        };
        let len = u32::from_le_bytes([header[18], header[19], header[20], header[21]]) as usize;
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
            duration_ns,
        }))
    }

    /// Create from a codec::Frame with a track index.
    ///
    /// `pub(crate)`: takes the internal `mux::codec::Frame` type, so it
    /// can't be part of the public API surface.
    pub(crate) fn from_codec_frame(track: usize, frame: crate::mux::codec::Frame) -> Self {
        Self {
            track,
            pts: frame.pts_ns,
            keyframe: frame.keyframe,
            data: frame.data,
            duration_ns: frame.duration_ns,
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

    /// Cumulative count of read errors the stream skipped past (e.g.
    /// zero-filled bad sectors on a live drive). Default `0` for
    /// streams that don't have a notion of skip-on-error (file ISO,
    /// network, stdio, the pipeline highway, etc.); concrete impls
    /// with adaptive retry (`DiscStream` on the drive single-pass
    /// path) override.
    fn errors(&self) -> u64 {
        0
    }

    /// Cumulative bytes actually skipped (zero-filled) past read errors.
    /// Distinct from [`errors`](Self::errors), which counts skip *events*:
    /// a single AACS skip event covers a whole 6144-byte unit, so
    /// `errors * 2048` understates real loss. Consumers estimating lost
    /// video time must scale by this byte count, not the event count.
    /// Default `0` for streams with no skip-on-error notion; `DiscStream`
    /// overrides.
    fn lost_bytes(&self) -> u64 {
        0
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
        // Only count bytes that actually made it to the inner sink, so a
        // failed write doesn't permanently inflate bytes_written().
        self.inner.write(frame)?;
        self.written += frame.data.len() as u64;
        Ok(())
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

    fn errors(&self) -> u64 {
        self.inner.errors()
    }

    fn lost_bytes(&self) -> u64 {
        self.inner.lost_bytes()
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
            duration_ns: None,
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

    #[test]
    fn frame_roundtrips_through_bytes() {
        let frame = make_frame(3, 123_456);
        let mut buf = Vec::new();
        frame.serialize(&mut buf).expect("serialize");
        let mut cursor = std::io::Cursor::new(buf);
        let got = PesFrame::deserialize(&mut cursor)
            .expect("deserialize")
            .expect("frame present");
        assert_eq!(got.track, frame.track);
        assert_eq!(got.pts, frame.pts);
        assert_eq!(got.keyframe, frame.keyframe);
        assert_eq!(got.data, frame.data);
        // Next read is a clean EOF.
        assert!(PesFrame::deserialize(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn empty_input_is_clean_eof() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        assert!(PesFrame::deserialize(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn truncated_header_is_error_not_eof() {
        // A partial 22-byte header (here 5 bytes) must surface as an error,
        // not be swallowed as a graceful end of stream.
        let mut cursor = std::io::Cursor::new(vec![1u8, 2, 3, 4, 5]);
        let err = PesFrame::deserialize(&mut cursor).expect_err("partial header must error");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn oversize_track_rejected_on_serialize() {
        let frame = make_frame(256, 0);
        let mut buf = Vec::new();
        let err = frame
            .serialize(&mut buf)
            .expect_err("track > 255 must fail");
        let code = format!("E{}", crate::error::E_PES_TRACK_TOO_LARGE);
        assert!(err.to_string().contains(&code), "got: {err}");
    }

    /// Output stream whose `write` always fails — for CountingStream tests.
    struct FailingWriteStream {
        title: DiscTitle,
    }

    impl Stream for FailingWriteStream {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            Ok(None)
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.title
        }
    }

    #[test]
    fn counting_stream_does_not_count_failed_writes() {
        let mut cs = CountingStream::new(Box::new(FailingWriteStream {
            title: DiscTitle::empty(),
        }));
        let frame = make_frame(0, 0);
        assert!(cs.write(&frame).is_err());
        // Failed write must not inflate the byte count.
        assert_eq!(cs.bytes_written(), 0);
    }

    #[test]
    fn counting_stream_counts_successful_writes() {
        let frame = make_frame(0, 0);
        let payload = frame.data.len() as u64;
        let mut cs = CountingStream::new(Box::new(MockStream::new(Vec::new())));
        cs.write(&frame).unwrap();
        assert_eq!(cs.bytes_written(), payload);
    }

    // ── New comprehensive tests ────────────────────────────────────────────────

    /// PesFrame serialize layout:
    /// track(1) | pts(8 LE) | keyframe(1) | duration_ns(8 LE) | len(4 LE) | data.
    /// Mutation: using big-endian for pts changes bytes [1..9] and deserialization fails.
    #[test]
    fn serialize_wire_format_matches_spec() {
        // Wire format: [track(1)][pts_le(8)][keyframe(1)][duration_le(8)][len_le(4)][data...]
        let frame = PesFrame {
            track: 2,
            pts: 0x0102030405060708_i64,
            keyframe: true,
            data: vec![0xAA, 0xBB, 0xCC],
            duration_ns: Some(0xDEADBEEF_u64),
        };
        let mut buf = Vec::new();
        frame.serialize(&mut buf).unwrap();
        // Byte 0: track
        assert_eq!(buf[0], 2, "byte 0 must be track");
        // Bytes 1..9: pts as little-endian i64
        let pts_bytes = 0x0102030405060708_i64.to_le_bytes();
        assert_eq!(
            &buf[1..9],
            &pts_bytes,
            "bytes 1..9 must be pts in little-endian"
        );
        // Byte 9: keyframe flag (1 = true)
        assert_eq!(buf[9], 1, "byte 9 must be 1 for keyframe=true");
        // Bytes 10..18: duration_ns as little-endian u64
        let dur_bytes = 0xDEADBEEF_u64.to_le_bytes();
        assert_eq!(
            &buf[10..18],
            &dur_bytes,
            "bytes 10..18 must be duration_ns in little-endian"
        );
        // Bytes 18..22: data length as little-endian u32
        let len_bytes = 3_u32.to_le_bytes();
        assert_eq!(
            &buf[18..22],
            &len_bytes,
            "bytes 18..22 must be data length LE u32"
        );
        // Bytes 22..: data
        assert_eq!(
            &buf[22..],
            &[0xAA, 0xBB, 0xCC],
            "data must follow header verbatim"
        );
    }

    /// serialize encodes keyframe=false as byte 0 at offset 9.
    /// Mutation: encoding keyframe as `!self.keyframe` flips the flag on the wire.
    #[test]
    fn serialize_keyframe_false_encodes_as_zero() {
        let frame = PesFrame {
            track: 0,
            pts: 0,
            keyframe: false,
            data: vec![1],
            duration_ns: None,
        };
        let mut buf = Vec::new();
        frame.serialize(&mut buf).unwrap();
        // Byte 9 is the keyframe byte.
        assert_eq!(
            buf[9], 0,
            "keyframe=false must encode as 0 at wire offset 9"
        );
    }

    /// serialize rejects track > 255 (1-byte wire field).
    /// Spec: wire format reserves 1 byte for track; track 256 cannot be encoded.
    /// Mutation: casting track to u8 with truncation silently drops the high bit.
    #[test]
    fn serialize_track_255_is_ok_track_256_is_err() {
        let ok_frame = PesFrame {
            track: 255,
            pts: 0,
            keyframe: false,
            data: vec![],
            duration_ns: None,
        };
        let mut buf = Vec::new();
        ok_frame.serialize(&mut buf).unwrap();
        assert_eq!(buf[0], 255, "track 255 must serialize to 0xFF");

        let too_large = PesFrame {
            track: 256,
            pts: 0,
            keyframe: false,
            data: vec![],
            duration_ns: None,
        };
        let mut buf2 = Vec::new();
        assert!(
            too_large.serialize(&mut buf2).is_err(),
            "track 256 must be rejected"
        );
    }

    /// deserialize round-trips pts=0 and pts=i64::MAX correctly.
    /// Mutation: off-by-one in byte indices [1..9] shifts the pts value.
    #[test]
    fn deserialize_round_trips_pts_boundaries() {
        for pts in [0_i64, i64::MAX, i64::MIN] {
            let frame = PesFrame {
                track: 0,
                pts,
                keyframe: false,
                data: vec![1],
                duration_ns: None,
            };
            let mut buf = Vec::new();
            frame.serialize(&mut buf).unwrap();
            let mut cursor = std::io::Cursor::new(buf);
            let got = PesFrame::deserialize(&mut cursor).unwrap().unwrap();
            assert_eq!(got.pts, pts, "pts={pts} must survive round-trip");
        }
    }

    /// deserialize: a frame with empty data (len=0) is valid.
    /// Spec: the wire format allows zero-length data fields (len=0 in u32 field).
    /// Mutation: treating len=0 as EOF condition instead of a valid frame drops them.
    #[test]
    fn deserialize_accepts_zero_length_data() {
        let frame = PesFrame {
            track: 3,
            pts: 99,
            keyframe: false,
            data: vec![],
            duration_ns: None,
        };
        let mut buf = Vec::new();
        frame.serialize(&mut buf).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let got = PesFrame::deserialize(&mut cursor).unwrap().unwrap();
        assert_eq!(got.track, 3);
        assert!(
            got.data.is_empty(),
            "zero-length data must round-trip as empty"
        );
    }

    /// duration_ns is serialized as 8 LE bytes; None encodes as u64::MAX sentinel.
    /// Spec: duration_ns is part of the wire format so network/stdio hops preserve it.
    /// Mutation: dropping duration_ns from serialize would zero-fill the field and
    ///           silently lose PGS subtitle durations on the network:// path.
    #[test]
    fn deserialize_duration_ns_roundtrips() {
        // None encodes as u64::MAX sentinel and decodes back to None.
        let frame_none = PesFrame {
            track: 0,
            pts: 0,
            keyframe: false,
            data: vec![1, 2, 3],
            duration_ns: None,
        };
        let mut buf = Vec::new();
        frame_none.serialize(&mut buf).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let got = PesFrame::deserialize(&mut cursor).unwrap().unwrap();
        assert!(
            got.duration_ns.is_none(),
            "None duration_ns must round-trip as None"
        );

        // Some(0) must survive — 0 is a valid zero-length duration, not the sentinel.
        let frame_zero = PesFrame {
            track: 1,
            pts: 1000,
            keyframe: false,
            data: vec![4, 5],
            duration_ns: Some(0),
        };
        let mut buf2 = Vec::new();
        frame_zero.serialize(&mut buf2).unwrap();
        let mut cursor2 = std::io::Cursor::new(buf2);
        let got2 = PesFrame::deserialize(&mut cursor2).unwrap().unwrap();
        assert_eq!(
            got2.duration_ns,
            Some(0),
            "Some(0) duration_ns must round-trip as Some(0)"
        );

        // Some(N) for a typical PGS duration (~3 seconds).
        let frame_n = PesFrame {
            track: 2,
            pts: 5_000_000_000,
            keyframe: false,
            data: vec![6],
            duration_ns: Some(3_000_000_000),
        };
        let mut buf3 = Vec::new();
        frame_n.serialize(&mut buf3).unwrap();
        let mut cursor3 = std::io::Cursor::new(buf3);
        let got3 = PesFrame::deserialize(&mut cursor3).unwrap().unwrap();
        assert_eq!(
            got3.duration_ns,
            Some(3_000_000_000),
            "Some(3_000_000_000) duration_ns must round-trip"
        );
    }

    /// Two sequential frames serialize and deserialize back independently.
    /// Mutation: reading one extra byte for the first frame's data corrupts
    ///           the second frame's header offset.
    #[test]
    fn deserialize_two_sequential_frames() {
        let f1 = PesFrame {
            track: 0,
            pts: 100,
            keyframe: true,
            data: vec![1, 2],
            duration_ns: None,
        };
        let f2 = PesFrame {
            track: 1,
            pts: 200,
            keyframe: false,
            data: vec![3, 4, 5],
            duration_ns: None,
        };
        let mut buf = Vec::new();
        f1.serialize(&mut buf).unwrap();
        f2.serialize(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let got1 = PesFrame::deserialize(&mut cursor).unwrap().unwrap();
        let got2 = PesFrame::deserialize(&mut cursor).unwrap().unwrap();
        assert_eq!(got1.track, 0);
        assert_eq!(got1.pts, 100);
        assert_eq!(got1.data, vec![1, 2]);
        assert_eq!(got2.track, 1);
        assert_eq!(got2.pts, 200);
        assert_eq!(got2.data, vec![3, 4, 5]);
        // Confirm clean EOF after both frames.
        assert!(PesFrame::deserialize(&mut cursor).unwrap().is_none());
    }

    /// CountingStream accumulates bytes across multiple successful writes.
    /// Mutation: resetting written to 0 on each write loses the running total.
    #[test]
    fn counting_stream_accumulates_across_multiple_writes() {
        let f1 = PesFrame {
            track: 0,
            pts: 0,
            keyframe: false,
            data: vec![1, 2, 3],
            duration_ns: None,
        };
        let f2 = PesFrame {
            track: 0,
            pts: 1,
            keyframe: false,
            data: vec![4, 5],
            duration_ns: None,
        };
        let mut cs = CountingStream::new(Box::new(MockStream::new(Vec::new())));
        cs.write(&f1).unwrap();
        cs.write(&f2).unwrap();
        assert_eq!(cs.bytes_written(), 5, "must accumulate 3+2=5 bytes");
    }
}
