use super::*;
use crate::disc::DiscTitle;

fn make_frame(track: usize, pts: i64) -> PesFrame {
    PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
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

impl crate::pes::PesSource for MockStream {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        Ok(self.read_queue.next())
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

impl crate::pes::PesSink for MockStream {
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

// A sink implementing only the required methods must get neutral defaults.
#[test]
fn stream_trait_defaults_are_neutral() {
    let mut s = MockStream::new(Vec::new());
    assert_eq!(s.track_timing(0), TrackTiming::default());
    assert!(s.set_track_timing(0, TrackTiming::default()).is_ok());
    assert!(s.codec_private(0).is_none());
    assert!(s.headers_ready());
    assert!(s.config_changes().is_empty());
    assert!(!s.set_codec_private(0, &[1]).unwrap());
    assert_eq!(s.errors(), 0);
    assert_eq!(s.lost_bytes(), 0);
    assert!(s.undelivered_streams().is_empty());
}

// CountingStream forwards finish and sums bytes over several writes.
#[test]
fn counting_stream_sums_writes() {
    let frames = vec![make_frame(0, 0), make_frame(1, 1_000)];
    let mut cs = CountingStream::new(Box::new(MockStream::new(frames.clone())));
    for f in &frames {
        cs.write(f).unwrap();
    }
    assert_eq!(cs.bytes_written(), 6);
    cs.finish().unwrap();
    let _ = PesSink::info(&cs);
}

// The 256 MiB frame ceiling applies on write and on read (before any allocation).
#[test]
fn frame_size_ceiling_enforced_on_write_and_read() {
    let code = format!("E{}", crate::error::E_PES_FRAME_TOO_LARGE);
    let mut big = make_frame(0, 0);
    big.data = vec![0u8; MAX_FRAME_SIZE + 1];
    let err = big.serialize(&mut Vec::new()).expect_err("over ceiling");
    assert!(err.to_string().contains(&code), "got: {err}");

    let mut header = vec![0u8; 22];
    header[18..22].copy_from_slice(&((MAX_FRAME_SIZE + 1) as u32).to_le_bytes());
    let err =
        PesFrame::deserialize(&mut std::io::Cursor::new(header.clone())).expect_err("over ceiling");
    assert!(err.to_string().contains(&code), "got: {err}");

    // Exactly at the ceiling passes the check and then hits the missing payload.
    header[18..22].copy_from_slice(&(MAX_FRAME_SIZE as u32).to_le_bytes());
    let err =
        PesFrame::deserialize(&mut std::io::Cursor::new(header)).expect_err("payload missing");
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
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

impl crate::pes::PesSource for FailingWriteStream {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        Ok(None)
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

impl crate::pes::PesSink for FailingWriteStream {
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
            discard_padding_ns: 0,
            coding: None,
            source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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

// duration_ns is 8 LE bytes on the wire (None = u64::MAX sentinel) so
// network/stdio hops preserve it; dropping it would silently lose PGS
// subtitle durations on the network:// path.
#[test]
fn deserialize_duration_ns_roundtrips() {
    // None encodes as u64::MAX sentinel and decodes back to None.
    let frame_none = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 100,
        keyframe: true,
        data: vec![1, 2],
        duration_ns: None,
    };
    let f2 = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
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
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: false,
        data: vec![1, 2, 3],
        duration_ns: None,
    };
    let f2 = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
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

/// A frame larger than the incremental read chunk (1 MiB) must assemble
/// byte-for-byte across chunk boundaries — the grow-as-you-read path must
/// not drop, duplicate, or corrupt bytes where one chunk ends and the next
/// begins. Uses a position-dependent pattern so any boundary slip shows up.
#[test]
fn large_multi_chunk_frame_assembles_correctly() {
    // 2 MiB + a tail, so at least three read chunks are exercised.
    let size = 2 * 1024 * 1024 + 12_345;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let frame = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 7,
        pts: 42,
        keyframe: true,
        data: payload.clone(),
        duration_ns: Some(5),
    };
    let mut buf = Vec::new();
    frame.serialize(&mut buf).expect("serialize large frame");
    let mut cursor = std::io::Cursor::new(buf);
    let got = PesFrame::deserialize(&mut cursor)
        .expect("deserialize")
        .expect("frame present");
    assert_eq!(got.data.len(), payload.len(), "length must survive");
    assert_eq!(got.data, payload, "every byte must survive across chunks");
    assert!(PesFrame::deserialize(&mut cursor).unwrap().is_none());
}

/// A header claiming a large frame with the data truncated must surface as
/// UnexpectedEof (not silently succeed) — and, crucially, the incremental
/// reader must not pre-allocate the full declared length before failing.
#[test]
fn truncated_large_frame_body_errors() {
    // Header advertises a 4 MiB frame, but only 10 payload bytes follow.
    let declared = 4 * 1024 * 1024u32;
    let mut buf = Vec::new();
    buf.push(0u8); // track
    buf.extend_from_slice(&0i64.to_le_bytes()); // pts
    buf.push(0u8); // keyframe
    buf.extend_from_slice(&u64::MAX.to_le_bytes()); // duration None sentinel
    buf.extend_from_slice(&declared.to_le_bytes()); // len
    buf.extend_from_slice(&[0xAB; 10]); // far fewer than declared
    // Record the largest buffer the reader is handed: a pre-sized body read
    // would ask for the whole declared length in one go.
    struct MaxAsk(std::io::Cursor<Vec<u8>>, usize);
    impl std::io::Read for MaxAsk {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            self.1 = self.1.max(b.len());
            self.0.read(b)
        }
    }
    let mut src = MaxAsk(std::io::Cursor::new(buf), 0);
    let err = PesFrame::deserialize(&mut src).expect_err("truncated body must error");
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(src.1 <= 1024 * 1024, "asked for {} bytes at once", src.1);
}
