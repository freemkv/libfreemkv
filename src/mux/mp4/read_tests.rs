use super::*;
use crate::pes::PesSink as _;

// `Read + Seek` backed by a small crafted prefix + endless zeros, reporting `len` bytes on
// `seek(End)` — exercises MAX_ALLOC_BYTES-scale reads without a real multi-GiB source
// allocation.
struct FakeBigReader {
    prefix: Vec<u8>,
    pos: u64,
    len: u64,
}
impl Read for FakeBigReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let remaining = self.len.saturating_sub(self.pos);
        let n = (out.len() as u64).min(remaining) as usize;
        for (i, byte) in out[..n].iter_mut().enumerate() {
            let idx = self.pos + i as u64;
            *byte = if idx < self.prefix.len() as u64 {
                self.prefix[idx as usize]
            } else {
                0 // endless zero fill past the crafted prefix
            };
        }
        self.pos += n as u64;
        Ok(n)
    }
}
impl Seek for FakeBigReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.pos = match from {
            SeekFrom::Start(p) => p,
            SeekFrom::End(off) => (self.len as i64 + off) as u64,
            SeekFrom::Current(off) => (self.pos as i64 + off) as u64,
        };
        Ok(self.pos)
    }
}

// Regression: a child box in the 64-bit largesize form (size==1) must not
// stop the sibling walk — old code saw size 1 < 8 and broke, so every box
// after it (here `moov`) vanished and its track was silently dropped.
#[test]
fn find_box_walks_past_a_largesize_sibling() {
    let mut payload = Vec::new();
    // `free` as an empty largesize box: size32=1, u64 total size = 16.
    payload.extend_from_slice(&1u32.to_be_bytes());
    payload.extend_from_slice(b"free");
    payload.extend_from_slice(&16u64.to_be_bytes());
    // `moov` (32-bit) with a 4-byte payload immediately after it.
    payload.extend_from_slice(&12u32.to_be_bytes());
    payload.extend_from_slice(b"moov");
    payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

    assert_eq!(
        find_box(&payload, b"moov"),
        Some(&[0xDEu8, 0xAD, 0xBE, 0xEF][..]),
        "moov after a largesize `free` sibling must still be found",
    );
}

// Positive-path test for `parse_stss`: two distinct entries in a buffer sized to exactly
// fit them, catching offset/stride arithmetic mutants.
#[test]
fn parse_stss_reads_two_distinct_entries_from_their_own_offsets() {
    let mut b = vec![0u8, 0, 0, 0]; // version+flags
    b.extend_from_slice(&2u32.to_be_bytes()); // entry_count = 2
    b.extend_from_slice(&100u32.to_be_bytes());
    b.extend_from_slice(&200u32.to_be_bytes());
    assert_eq!(b.len(), 16, "sized to fit exactly 2 entries, no slack");
    let set = parse_stss(&b);
    assert_eq!(
        set,
        [100u32, 200u32].into_iter().collect(),
        "both distinct sync sample numbers must be present"
    );
}

// A declared `count` larger than the box can hold must be bounded by the per-entry guard,
// not trusted — same contract as stco/stsc/stts.
#[test]
fn parse_stss_count_lie_is_bounded_by_the_box_not_trusted() {
    let mut b = vec![0u8, 0, 0, 0];
    b.extend_from_slice(&3u32.to_be_bytes()); // declares 3, lying
    b.extend_from_slice(&100u32.to_be_bytes()); // only 2 real entries
    b.extend_from_slice(&200u32.to_be_bytes());
    assert_eq!(b.len(), 16, "room for exactly 2 real entries, not 3");
    let set = parse_stss(&b);
    assert_eq!(
        set,
        [100u32, 200u32].into_iter().collect(),
        "only the entries the box actually holds, no panic on the lie"
    );
}

#[test]
fn stsc_offsets_one_sample_per_chunk() {
    // Our writer's layout: 1 sample/chunk, co64 lists every offset.
    let sizes = vec![100u32, 200, 300];
    let chunks = vec![1000u64, 1100, 1300];
    let stsc = vec![(1u32, 1u32)];
    assert_eq!(
        sample_offsets(&sizes, &chunks, &stsc),
        vec![1000, 1100, 1300]
    );
}

#[test]
fn stsc_offsets_multi_sample_chunks() {
    // 2 samples in chunk 1, 1 in chunk 2.
    let sizes = vec![10u32, 20, 30];
    let chunks = vec![500u64, 900];
    let stsc = vec![(1u32, 2u32), (2u32, 1u32)];
    // chunk1@500: s0@500, s1@510; chunk2@900: s2@900.
    assert_eq!(sample_offsets(&sizes, &chunks, &stsc), vec![500, 510, 900]);
}

#[test]
fn sample_offsets_saturates_on_huge_chunk_offset() {
    // A co64 chunk offset near u64::MAX plus a sample size must saturate, not
    // overflow-panic (debug) / wrap (release) — the read() EOF guard rejects
    // the resulting out-of-range offset later.
    let sizes = vec![10u32, 20];
    let chunks = vec![u64::MAX - 5];
    let stsc = vec![(1u32, 2u32)]; // 2 samples in the single chunk
    let offs = sample_offsets(&sizes, &chunks, &stsc);
    assert_eq!(offs[0], u64::MAX - 5);
    assert_eq!(offs[1], u64::MAX, "(MAX-5)+10 saturates to MAX");
}

// `sidx` must advance once PER SAMPLE PLACED; a chunk of only two samples can't expose a
// stuck index, so this uses three distinctly-sized samples.
#[test]
fn sample_offsets_advances_through_distinct_sizes_within_one_chunk() {
    let sizes = [10u32, 20, 30];
    let chunks = [1000u64];
    let stsc = [(1u32, 3u32)]; // all 3 samples in the one chunk
    assert_eq!(
        sample_offsets(&sizes, &chunks, &stsc),
        vec![1000, 1010, 1030],
        "1000, then +10, then +20 — each sample's OWN size, in order"
    );
}

#[test]
fn read_rejects_sample_offset_past_eof() {
    use crate::disc::{
        Codec, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStreamE, VideoStream,
    };
    use crate::mux::mp4::Mp4Sink;
    use crate::pes::PesFrame;
    use std::io::Cursor;

    // The MP4 writer requires a primary video track, so build an HEVC one.
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStreamE::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R1080p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Sdr,
        color_space: crate::disc::ColorSpace::Unknown,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE])];
    let mut buf = Vec::new();
    {
        let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
        sink.write(&PesFrame {
            discard_padding_ns: 0,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x11u8; 700],
            duration_ns: None,
            source: None,
            coding: None,
        })
        .unwrap();
        sink.finish().unwrap();
    }
    // Faststart puts `moov` near the front, so a 64 KiB truncation keeps the
    // parseable moov but drops mdat; read() must reject the resulting
    // out-of-range sample rather than allocate for it or read past EOF.
    buf.truncate(64 * 1024);
    let mut rd = Mp4Reader::from_reader(Cursor::new(buf), "trunc".into()).unwrap();
    let err = rd.read().unwrap_err();
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidData,
        "a sample offset past EOF must be rejected"
    );
}

// `Stream::read`'s own `s.size as u64 > MAX_ALLOC_BYTES` cap — a separate call site from
// `read_moov`'s. Exactly-the-cap must still be allowed.
#[test]
fn stream_read_allows_a_sample_of_exactly_max_alloc_bytes() {
    use crate::disc::DiscTitle;

    let mut rd = Mp4Reader {
        file: FakeBigReader {
            prefix: Vec::new(),
            pos: 0,
            len: MAX_ALLOC_BYTES + 4096,
        },
        file_len: MAX_ALLOC_BYTES + 4096,
        title: DiscTitle::empty(),
        samples: vec![SampleRef {
            track: 0,
            offset: 0,
            size: MAX_ALLOC_BYTES as u32,
            pts_ns: 0,
            dts_ns: 0,
            keyframe: true,
        }],
        cursor: 0,
    };
    let frame = rd
        .read()
        .expect("exactly MAX_ALLOC_BYTES must be allowed")
        .expect("one sample is queued");
    assert_eq!(frame.data.len() as u64, MAX_ALLOC_BYTES);
}

// The same cap's other edge: one byte OVER it must still be rejected, not just values
// exactly at the cap.
#[test]
fn stream_read_rejects_a_sample_one_byte_over_max_alloc_bytes() {
    use crate::disc::DiscTitle;

    let mut rd = Mp4Reader {
        file: FakeBigReader {
            prefix: Vec::new(),
            pos: 0,
            len: MAX_ALLOC_BYTES + 4096,
        },
        file_len: MAX_ALLOC_BYTES + 4096,
        title: DiscTitle::empty(),
        samples: vec![SampleRef {
            track: 0,
            offset: 0,
            size: (MAX_ALLOC_BYTES + 1) as u32,
            pts_ns: 0,
            dts_ns: 0,
            keyframe: true,
        }],
        cursor: 0,
    };
    let err = rd.read().unwrap_err();
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidData,
        "one byte over MAX_ALLOC_BYTES must still be rejected, not just \
             the exact cap value"
    );
}

#[test]
// Underscores mark BITFIELD boundaries in the bitstream header being built
// (e.g. 5-bit then 3-bit field), not thousands-style digit groups.
#[allow(clippy::unusual_byte_groupings)]
fn write_then_read_round_trip() {
    // Mux a small A/V title to an in-memory MP4, then demux it back and
    // check the streams, codec_private, and sample payloads survive.
    use crate::disc::{
        AudioChannels, AudioStream, Codec, DiscTitle, FrameRate, HdrFormat, LabelPurpose,
        SampleRate, Stream as DiscStreamE, VideoStream,
    };
    use crate::mux::mp4::Mp4Sink;
    use crate::pes::PesFrame;
    use std::io::Cursor;

    let mut t = DiscTitle::empty();
    t.streams = vec![
        DiscStreamE::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: crate::disc::ColorSpace::Unknown,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }),
        DiscStreamE::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }),
    ];
    let hvcc = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    t.codec_privates = vec![Some(hvcc.clone()), None];

    let ac3 = vec![
        0x0B,
        0x77,
        0x00,
        0x00,
        0b00_010110,
        0b01000_000,
        0b111_00_00_1,
        0x00,
        0xFF,
        0xFF,
    ];
    let d = 41_708_333i64;
    let vid0 = vec![0x11u8; 700];
    let vid1 = vec![0x22u8; 350];
    // Same-length audio samples with distinct bytes, so a wrong co64 offset shows.
    let mut ac3b = ac3.clone();
    ac3b[8] = 0x5A;
    ac3b[9] = 0xA5;

    let mut buf = Vec::new();
    {
        let mk = |track, pts, key, data: Vec<u8>| PesFrame {
            discard_padding_ns: 0,
            track,
            pts,
            keyframe: key,
            data,
            duration_ns: None,
            source: None,
            coding: None,
        };
        let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
        sink.write(&mk(0, 0, true, vid0.clone())).unwrap();
        sink.write(&mk(1, 0, true, ac3.clone())).unwrap();
        sink.write(&mk(0, d, false, vid1.clone())).unwrap();
        sink.write(&mk(1, 32_000_000, true, ac3b.clone())).unwrap();
        sink.finish().unwrap();
    }

    let path = std::env::temp_dir().join(format!("fmkv-mp4-rt-{}.mp4", std::process::id()));
    std::fs::write(&path, &buf).unwrap();
    let mut rd = Mp4Reader::from_reader(Cursor::new(buf), "rt".into()).unwrap();
    // Two streams: HEVC video + AC-3 audio, and the hvcC round-trips.
    assert_eq!(rd.info().streams.len(), 2);
    assert!(matches!(rd.info().streams[0], DiscStreamE::Video(ref v) if v.codec == Codec::Hevc));
    assert!(matches!(rd.info().streams[1], DiscStreamE::Audio(ref a) if a.codec == Codec::Ac3));
    assert_eq!(rd.codec_private(0), Some(hvcc));

    // Read all frames back; match them to what we wrote by (track, size).
    let mut got = Vec::new();
    while let Some(f) = rd.read().unwrap() {
        got.push((f.track, f.data.len(), f.keyframe, f.data));
    }
    assert_eq!(got.len(), 4, "4 samples round-trip");
    let vids: Vec<_> = got.iter().filter(|(t, ..)| *t == 0).collect();
    let auds: Vec<_> = got.iter().filter(|(t, ..)| *t == 1).collect();
    assert_eq!(vids.len(), 2);
    assert_eq!(auds.len(), 2);
    // Payload bytes, not just sizes, must come back from the right offsets.
    assert_eq!(vids[0].3, vid0);
    assert_eq!(vids[1].3, vid1);
    assert_eq!(auds[0].3, ac3);
    assert_eq!(auds[1].3, ac3b);
    assert_eq!(vids[0].1, 700, "first video sample size");
    assert_eq!(vids[1].1, 350, "second video sample size");
    assert!(vids[0].2, "first video frame is a keyframe");
    assert_eq!(auds[0].1, ac3.len());

    // `mp4://` reads the same file through the decryption stage: a clear container
    // (Opaque) is handed through untouched, seeks included.
    let url = format!("mp4://{}", path.display());
    let mut staged =
        crate::mux::resolve::input(&url, &Default::default(), &crate::ctx::Ctx::default()).unwrap();
    let mut again = Vec::new();
    while let Some(f) = staged.read().unwrap() {
        again.push((f.track, f.data.len(), f.keyframe, f.data));
    }
    let _ = std::fs::remove_file(&path);
    assert_eq!(again, got);
}

// `stts` run-length expansion (ISO/IEC 14496-12 §8.6.1.2) must expand `(sample_count,
// sample_delta)` runs to one delta PER SAMPLE, in order.
#[test]
fn stts_expands_runs_to_per_sample_deltas_in_order() {
    // Three runs with DISTINCT deltas/lengths, so a parser that drops a run,
    // reuses the first delta, or misorders runs cannot agree. A trailing
    // 0-length run must contribute nothing (legal per §8.6.1.2).
    let mut stts = Vec::new();
    stts.extend_from_slice(&[0, 0, 0, 0]); // version + flags
    stts.extend_from_slice(&4u32.to_be_bytes()); // entry_count
    for (n, delta) in [(3u32, 1001u32), (1, 2002), (0, 7777), (2, 1002)] {
        stts.extend_from_slice(&n.to_be_bytes());
        stts.extend_from_slice(&delta.to_be_bytes());
    }
    assert_eq!(
        parse_stts(&stts, MAX_SAMPLE_COUNT),
        vec![1001, 1001, 1001, 2002, 1002, 1002],
    );

    // A truncated box (entry_count claims more runs than the bytes hold)
    // must yield the runs actually present, never read past the end.
    let truncated = &stts[..stts.len() - 6];
    assert_eq!(
        parse_stts(truncated, MAX_SAMPLE_COUNT),
        vec![1001, 1001, 1001, 2002],
    );

    // Too short to hold version/flags + entry_count → no samples.
    assert!(parse_stts(&stts[..7], MAX_SAMPLE_COUNT).is_empty());
}

// ── Composition offsets (`ctts`) — ISO/IEC 14496-12 §8.6.1.3. `ctts` is the
// ONLY place a reordered (B-frame) track's presentation time survives the
// write→read cycle; wrong/absent, every frame's PTS collapses onto its DTS.

#[test]
fn ctts_build_and_parse_are_exact_inverses_over_signed_offsets() {
    // Includes a negative offset (version 1 only), a repeated run, and a
    // value repeated non-adjacently — so a parser that loses the sign, drops
    // the run expansion, or returns a constant cannot agree.
    let offsets: Vec<i32> = vec![0, 2, -1, -1, 0, 3003];
    let boxed = crate::mux::mp4::build_ctts(&offsets);

    assert_eq!(&boxed[4..8], b"ctts", "box type");
    assert_eq!(
        boxed[8], 1,
        "signed composition offsets require ctts version 1 (§8.6.1.3)"
    );
    // Run-length coalescing: the two adjacent -1s share one entry, so the
    // six offsets become five runs. Size = 8 hdr + 4 ver/flags + 4 count + 5×8.
    assert_eq!(
        u32::from_be_bytes(boxed[0..4].try_into().unwrap()) as usize,
        8 + 4 + 4 + 5 * 8,
        "equal ADJACENT offsets coalesce into a single run"
    );
    assert_eq!(
        u32::from_be_bytes(boxed[12..16].try_into().unwrap()),
        5,
        "entry_count is the run count, not the sample count"
    );

    // The box header is not part of the parser's input: it takes the payload
    // from version/flags onward.
    assert_eq!(parse_ctts(&boxed[8..], MAX_SAMPLE_COUNT), offsets);
}

#[test]
fn b_frame_presentation_order_survives_the_mp4_round_trip() {
    // Four samples in DECODE order, classic I-P-B-B reorder: at 25 fps the
    // composition offsets are [0, +2, -1, -1] — negative, exercising the signed
    // version-1 path (prior tests left composition time unconstrained).
    use crate::disc::{
        Codec, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStreamE, VideoStream,
    };
    use crate::mux::mp4::Mp4Sink;
    use crate::pes::PesFrame;
    use std::io::Cursor;

    const FRAME_NS: i64 = 40_000_000; // 25 fps, exact

    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStreamE::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R1080p,
        frame_rate: FrameRate::F25,
        hdr: HdrFormat::Sdr,
        color_space: crate::disc::ColorSpace::Unknown,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE])];

    // (presentation time, payload byte) in decode order.
    let plan: [(i64, u8); 4] = [
        (0, 0x10),            // I, presents first
        (3 * FRAME_NS, 0x20), // P, presents last
        (FRAME_NS, 0x30),     // B
        (2 * FRAME_NS, 0x40), // B
    ];

    let mut buf = Vec::new();
    {
        let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
        for (i, &(pts, fill)) in plan.iter().enumerate() {
            sink.write(&PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts,
                keyframe: i == 0,
                data: vec![fill; 64 + i * 8],
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
        }
        sink.finish().unwrap();
    }

    let mut rd = Mp4Reader::from_reader(Cursor::new(buf), "reorder".into()).unwrap();
    let mut got = Vec::new();
    while let Some(f) = rd.read().unwrap() {
        got.push((f.pts, f.data[0]));
    }
    assert_eq!(got.len(), 4);

    // Frames come back in decode order (sorted by DTS), each still carrying
    // the presentation time it was written with — identified by payload, so
    // a reordering of the samples themselves cannot be mistaken for success.
    assert_eq!(
        got,
        vec![
            (0, 0x10),
            (3 * FRAME_NS, 0x20),
            (FRAME_NS, 0x30),
            (2 * FRAME_NS, 0x40),
        ],
        "composition times must survive the write→read cycle"
    );

    // The property that matters to a player: presentation order differs from
    // decode order, and sorting by PTS recovers the display sequence.
    let mut by_pts = got.clone();
    by_pts.sort_by_key(|&(pts, _)| pts);
    assert_eq!(
        by_pts.iter().map(|&(_, b)| b).collect::<Vec<_>>(),
        vec![0x10, 0x30, 0x40, 0x20],
        "display order is I,B,B,P — not the decode order I,P,B,B"
    );
}

// A track's declared duration must be its real length: `mdhd.duration` in MEDIA timescale,
// `mvhd.duration` in movie timescale (§8.2.2, §8.4.2).
#[test]
fn declared_track_duration_equals_frame_count_times_frame_duration() {
    use crate::disc::{
        Codec, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStreamE, VideoStream,
    };
    use crate::mux::mp4::Mp4Sink;
    use crate::pes::PesFrame;
    use std::io::Cursor;

    const FRAME_NS: i64 = 40_000_000; // 25 fps, exact
    const FRAMES: usize = 5;

    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStreamE::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R1080p,
        frame_rate: FrameRate::F25,
        hdr: HdrFormat::Sdr,
        color_space: crate::disc::ColorSpace::Unknown,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE])];

    let mut buf = Vec::new();
    {
        let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
        for i in 0..FRAMES {
            sink.write(&PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts: i as i64 * FRAME_NS,
                keyframe: i == 0,
                data: vec![0x55u8; 128],
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
        }
        sink.finish().unwrap();
    }

    let moov = find_box(&buf, b"moov").expect("moov");

    // mvhd is a version-1 FullBox here: vflags(4) creation(8) modification(8)
    // timescale(4) duration(8).
    let mvhd = find_box(moov, b"mvhd").expect("mvhd");
    assert_eq!(mvhd[0], 1, "mvhd version 1");
    let movie_ts = be32(mvhd, 20);
    let movie_dur = u64::from_be_bytes(mvhd[24..32].try_into().unwrap());
    assert_eq!(movie_ts, 90_000);
    assert_eq!(
        movie_dur, 18_000,
        "5 frames at 25 fps is 0.2 s = 18000 ticks at 90 kHz"
    );

    // mdhd, same version-1 layout, but in the media timescale the writer chose.
    let mdhd = find_box(
        find_box(find_box(moov, b"trak").expect("trak"), b"mdia").expect("mdia"),
        b"mdhd",
    )
    .expect("mdhd");
    assert_eq!(mdhd[0], 1, "mdhd version 1");
    let media_ts = be32(mdhd, 20);
    let media_dur = u64::from_be_bytes(mdhd[24..32].try_into().unwrap());
    assert_eq!(media_ts, 25, "25 fps snaps to a 25-tick timescale");
    assert_eq!(
        media_dur, FRAMES as u64,
        "duration is one tick per frame in this timescale"
    );
    assert_eq!(
        media_dur as f64 / media_ts as f64,
        movie_dur as f64 / movie_ts as f64,
        "the two declared durations must describe the same wall-clock length"
    );
}

// The `moov` tree must carry the boxes ISO/IEC 14496-12 makes mandatory for a playable
// track. Lives here (not the writer's tests) so assertions go through `find_box`.
#[test]
// Underscores mark BITFIELD boundaries in the bitstream header being built
// (e.g. 5-bit then 3-bit field), not thousands-style digit groups.
#[allow(clippy::unusual_byte_groupings)]
fn moov_tree_carries_the_mandatory_track_header_and_media_boxes() {
    use crate::disc::{
        AudioChannels, AudioStream, Codec, DiscTitle, FrameRate, HdrFormat, LabelPurpose,
        Resolution, SampleRate, Stream as DiscStreamE, VideoStream,
    };
    use crate::mux::mp4::Mp4Sink;
    use crate::pes::PesFrame;
    use std::io::Cursor;

    let mut t = DiscTitle::empty();
    t.streams = vec![
        DiscStreamE::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: crate::disc::ColorSpace::Unknown,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }),
        DiscStreamE::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }),
    ];
    t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE]), None];

    let ac3 = vec![
        0x0B,
        0x77,
        0x00,
        0x00,
        0b00_010110,
        0b01000_000,
        0b111_00_00_1,
        0x00,
        0xFF,
        0xFF,
    ];

    let mut buf = Vec::new();
    {
        let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
        for i in 0..4i64 {
            sink.write(&PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts: i * 40_000_000,
                keyframe: i == 0,
                data: vec![0x55u8; 128],
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
            sink.write(&PesFrame {
                discard_padding_ns: 0,
                track: 1,
                pts: i * 32_000_000,
                keyframe: true,
                data: ac3.clone(),
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
        }
        sink.finish().unwrap();
    }

    let moov = find_box(&buf, b"moov").expect("moov");
    let traks = find_boxes_capped(moov, b"trak", usize::MAX);
    assert_eq!(traks.len(), 2, "one video trak + one audio trak");

    // ── tkhd (§8.3.2). Mandatory in every trak. flags bit 0 = track_enabled;
    // a track with flags 0 is ignored by a conforming player. Width/height are
    // 16.16 fixed point.
    let vid_tkhd = find_box(traks[0], b"tkhd").expect("video tkhd");
    assert_eq!(vid_tkhd[0], 1, "tkhd version 1");
    let flags = u32::from_be_bytes([0, vid_tkhd[1], vid_tkhd[2], vid_tkhd[3]]);
    assert_eq!(flags & 0x1, 0x1, "track_enabled must be set");
    assert_eq!(be32(vid_tkhd, 20), 1, "first track_id is 1");
    assert_eq!(
        be32(vid_tkhd, 88) >> 16,
        1920,
        "tkhd width is 16.16 fixed point"
    );
    assert_eq!(be32(vid_tkhd, 92) >> 16, 1080, "tkhd height");

    let aud_tkhd = find_box(traks[1], b"tkhd").expect("audio tkhd");
    assert_eq!(be32(aud_tkhd, 20), 2, "second track_id is 2");
    assert_eq!(
        be16(aud_tkhd, 48),
        0x0100,
        "an audio track's tkhd volume is 1.0 (8.8 fixed), not muted"
    );
    assert_eq!(
        (be32(aud_tkhd, 88), be32(aud_tkhd, 92)),
        (0, 0),
        "a sound track declares zero visual dimensions"
    );

    // ── minf media headers: vmhd for video, smhd for audio (§12.1.2, §12.2.2).
    // Exactly one of them, and never the wrong one for the handler.
    let minf = |trak: &[u8]| -> Vec<u8> {
        find_box(find_box(trak, b"mdia").expect("mdia"), b"minf")
            .expect("minf")
            .to_vec()
    };
    let vid_minf = minf(traks[0]);
    let aud_minf = minf(traks[1]);

    let vmhd = find_box(&vid_minf, b"vmhd").expect("video minf must carry vmhd");
    assert!(
        find_box(&vid_minf, b"smhd").is_none(),
        "a video minf must not carry smhd"
    );
    // §12.1.2 fixes vmhd flags to 1.
    assert_eq!(
        u32::from_be_bytes([0, vmhd[1], vmhd[2], vmhd[3]]),
        1,
        "vmhd flags must be 1"
    );
    assert_eq!(
        (be16(vmhd, 4), &vmhd[6..12]),
        (0u16, &[0u8; 6][..]),
        "graphicsmode 0 (copy) with a zero opcolor"
    );

    let smhd = find_box(&aud_minf, b"smhd").expect("audio minf must carry smhd");
    assert!(
        find_box(&aud_minf, b"vmhd").is_none(),
        "an audio minf must not carry vmhd"
    );
    assert_eq!(be16(smhd, 4), 0, "smhd balance is centre");

    // ── dinf > dref > "url " with flags 1 = media is in THIS file (§8.7.2).
    // Both tracks need it; a missing/empty dref makes the samples unreachable.
    for (name, m) in [("video", &vid_minf), ("audio", &aud_minf)] {
        let dinf = find_box(m, b"dinf").unwrap_or_else(|| panic!("{name} dinf"));
        let dref = find_box(dinf, b"dref").unwrap_or_else(|| panic!("{name} dref"));
        assert_eq!(be32(dref, 4), 1, "{name} dref entry_count");
        let url = find_box(&dref[8..], b"url ").unwrap_or_else(|| panic!("{name} url "));
        // `url ` payload is version(1)+flags(3) only when self-contained.
        assert_eq!(
            u32::from_be_bytes([0, url[1], url[2], url[3]]),
            1,
            "{name} url flags=1 (self-contained), so no external name follows"
        );
        assert_eq!(
            url.len(),
            4,
            "{name} self-contained url carries no location"
        );
    }
}

// ── Untrusted-input hardening: a crafted MP4 must never panic or over-allocate.

#[test]
fn parse_stsz_fixed_size_caps_hostile_count() {
    // sample_size != 0, count = u32::MAX: a 12-byte box must not allocate ~16 GiB.
    let mut b = Vec::new();
    b.extend_from_slice(&[0, 0, 0, 0]); // version+flags
    b.extend_from_slice(&1u32.to_be_bytes()); // sample_size
    b.extend_from_slice(&u32::MAX.to_be_bytes()); // count
    let out = parse_stsz(&b, MAX_SAMPLE_COUNT);
    assert_eq!(out.len(), MAX_SAMPLE_COUNT);
    // And a smaller budget (the cumulative multi-track cap) bounds it tighter.
    assert_eq!(parse_stsz(&b, 100).len(), 100);
}

#[test]
fn parse_stsz_table_count_bounded_by_box() {
    // sample_size == 0, count huge, but only two real entries in the buffer.
    let mut b = Vec::new();
    b.extend_from_slice(&[0, 0, 0, 0]);
    b.extend_from_slice(&0u32.to_be_bytes()); // sample_size == 0 → table
    b.extend_from_slice(&u32::MAX.to_be_bytes()); // count (lie)
    b.extend_from_slice(&10u32.to_be_bytes());
    b.extend_from_slice(&20u32.to_be_bytes());
    assert_eq!(parse_stsz(&b, MAX_SAMPLE_COUNT), vec![10, 20]);
}

#[test]
fn parse_stco_stsc_count_bounded_by_box() {
    // stco: count lie, one real 32-bit offset.
    let mut stco = Vec::new();
    stco.extend_from_slice(&[0, 0, 0, 0]);
    stco.extend_from_slice(&u32::MAX.to_be_bytes());
    stco.extend_from_slice(&4096u32.to_be_bytes());
    assert_eq!(parse_stco(&stco, false), vec![4096]);
    // stsc: count lie, one real (first_chunk, per) tuple.
    let mut stsc = Vec::new();
    stsc.extend_from_slice(&[0, 0, 0, 0]);
    stsc.extend_from_slice(&u32::MAX.to_be_bytes());
    stsc.extend_from_slice(&1u32.to_be_bytes());
    stsc.extend_from_slice(&7u32.to_be_bytes());
    stsc.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx (unused)
    assert_eq!(parse_stsc(&stsc), vec![(1, 7)]);
}

// Every one of these five table parsers must reject every buffer length below the 8-byte
// version+flags+count header, not just exactly `< 8`.
#[test]
fn table_parsers_reject_every_length_up_to_the_header_size() {
    for len in 0..=8 {
        let b = vec![0u8; len];
        assert!(
            parse_stco(&b, false).is_empty(),
            "parse_stco(is64=false) len={len}"
        );
        assert!(
            parse_stco(&b, true).is_empty(),
            "parse_stco(is64=true) len={len}"
        );
        assert!(parse_stsc(&b).is_empty(), "parse_stsc len={len}");
        assert!(
            parse_stts(&b, MAX_SAMPLE_COUNT).is_empty(),
            "parse_stts len={len}"
        );
        assert!(
            parse_ctts(&b, MAX_SAMPLE_COUNT).is_empty(),
            "parse_ctts len={len}"
        );
        assert!(parse_stss(&b).is_empty(), "parse_stss len={len}");
    }
}

// `co64`'s 8-byte offsets decode byte-by-byte, a separate path from the 32-bit
// `stco`/`be32` case; every byte here is distinct to catch index slips.
#[test]
fn parse_stco_co64_reads_each_byte_from_its_own_offset() {
    let mut b = vec![0u8; 4]; // version+flags (unused by this parser)
    b.extend_from_slice(&1u32.to_be_bytes()); // count = 1
    b.extend_from_slice(&0x1122334455667788u64.to_be_bytes());
    assert_eq!(parse_stco(&b, true), vec![0x1122334455667788u64]);
}

#[test]
fn parse_stts_caps_hostile_runlength() {
    // One entry with a u32::MAX run-length must cap, not push billions.
    let mut stts = Vec::new();
    stts.extend_from_slice(&[0, 0, 0, 0]);
    stts.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    stts.extend_from_slice(&u32::MAX.to_be_bytes()); // n
    stts.extend_from_slice(&33u32.to_be_bytes()); // delta
    assert_eq!(parse_stts(&stts, MAX_SAMPLE_COUNT).len(), MAX_SAMPLE_COUNT);
    // A tight per-track cap (the real sample count) bounds the run-length too.
    assert_eq!(parse_stts(&stts, 5).len(), 5);
}

#[test]
fn parse_stsd_short_size_does_not_panic() {
    // stsd sample entry declaring size = 0 must not panic on `entry[8..<8]`.
    let mut b = Vec::new();
    b.extend_from_slice(&[0, 0, 0, 0]); // version+flags
    b.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    b.extend_from_slice(&0u32.to_be_bytes()); // sample entry size = 0
    b.extend_from_slice(b"avc1"); // fourcc
    // Recognised codec but empty body → None (no dimensions), no panic.
    assert!(parse_stsd(&b).is_none());
}

// Every buffer shorter than the 8-byte header must return `None`, never fall through to
// `&b[8..]` and panic.
#[test]
fn parse_stsd_rejects_every_length_below_the_header_size() {
    for len in 0..8 {
        assert!(
            parse_stsd(&vec![0u8; len]).is_none(),
            "len={len} must be None, not a panic"
        );
    }
}

#[test]
fn read_moov_oversize_is_rejected() {
    use std::io::Cursor;
    // A tiny file whose first box claims to be a `moov` far larger than the file.
    let mut b = Vec::new();
    b.extend_from_slice(&0xFFFF_FFF0u32.to_be_bytes()); // size32 (~4 GiB)
    b.extend_from_slice(b"moov");
    b.extend_from_slice(&[0u8; 16]); // a few real bytes, nowhere near the claim
    assert!(read_moov(&mut Cursor::new(b)).is_err());
}

#[test]
fn parse_esds_extracts_aac_asc() {
    // esds: version/flags + ES_Descriptor(0x03) + DecoderConfigDescriptor(0x04)
    // + DecoderSpecificInfo(0x05) carrying a 2-byte AudioSpecificConfig.
    let esds = vec![
        0, 0, 0, 0, // version+flags
        0x03, 0x19, 0x00, 0x00, 0x00, // ES_Descriptor: tag,len, ES_ID(2), flags(0)
        0x04, 0x11, // DecoderConfigDescriptor: tag,len
        0x40, // objectTypeIndication (AAC)
        0x15, 0, 0, 0, // streamType/bufferSizeDB
        0, 0, 0, 0, // maxBitrate
        0, 0, 0, 0, // avgBitrate
        0x05, 0x02, // DecoderSpecificInfo: tag,len
        0x12, 0x10, // AudioSpecificConfig (AAC-LC 44.1k stereo)
    ];
    assert_eq!(parse_esds_asc(&esds), Some(vec![0x12, 0x10]));
    // A truncated esds must return None, never panic.
    assert_eq!(parse_esds_asc(&esds[..12]), None);
}

// The final guard `asc_len == 0 || end > b.len()` is two independent rejection reasons, not
// one condition needing both; pins each half.
#[test]
fn parse_esds_asc_boundary_checks_are_independent() {
    // Shared prefix through the DecoderSpecificInfo tag (byte 24 = 0x05);
    // the length byte and payload follow.
    let prefix = || {
        vec![
            0, 0, 0, 0, // version+flags
            0x03, 0x19, 0x00, 0x00, 0x00, // ES_Descriptor: tag,len,ES_ID,flags
            0x04, 0x11, // DecoderConfigDescriptor: tag,len
            0x40, // objectTypeIndication (AAC)
            0x15, 0, 0, 0, // streamType/bufferSizeDB
            0, 0, 0, 0, // maxBitrate
            0, 0, 0, 0,    // avgBitrate
            0x05, // DecoderSpecificInfo tag
        ]
    };

    let mut zero_len = prefix();
    zero_len.push(0x00); // length = 0
    assert_eq!(
        parse_esds_asc(&zero_len),
        None,
        "a zero-length ASC must be None, not Some(vec![])"
    );

    let mut with_trailer = prefix();
    with_trailer.push(0x02); // length = 2
    with_trailer.extend_from_slice(&[0x12, 0x10]); // the ASC itself
    with_trailer.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // trailing bytes
    assert_eq!(
        parse_esds_asc(&with_trailer),
        Some(vec![0x12, 0x10]),
        "bytes AFTER a complete ASC must not cause rejection"
    );
}

#[test]
fn read_moov_over_cap_rejected_despite_inflated_file_len() {
    use std::io::{Read, Seek, SeekFrom};
    // Reader forges an 8 GiB `seek(End)` length (like a sparse file) backed by a
    // tiny header + endless zeros, so a 512 MiB `moov` passes the plain EOF
    // check; only the absolute MAX_ALLOC_BYTES cap can reject it.
    struct InflatedReader {
        data: Vec<u8>,
        pos: u64,
        fake_len: u64,
    }
    impl Read for InflatedReader {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let remaining = self.fake_len.saturating_sub(self.pos);
            let n = (out.len() as u64).min(remaining) as usize;
            for (i, byte) in out[..n].iter_mut().enumerate() {
                let idx = self.pos + i as u64;
                *byte = if idx < self.data.len() as u64 {
                    self.data[idx as usize]
                } else {
                    0 // endless zero fill past the crafted header
                };
            }
            self.pos += n as u64;
            Ok(n)
        }
    }
    impl Seek for InflatedReader {
        fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
            self.pos = match from {
                SeekFrom::Start(p) => p,
                SeekFrom::End(off) => (self.fake_len as i64 + off) as u64,
                SeekFrom::Current(off) => (self.pos as i64 + off) as u64,
            };
            Ok(self.pos)
        }
    }

    // Only the 8-byte box header is served; the 512 MiB payload is never read.
    let box_size: u32 = (512 << 20) + 8; // 512 MiB > 256 MiB cap, < 8 GiB len
    let mut data = Vec::new();
    data.extend_from_slice(&box_size.to_be_bytes());
    data.extend_from_slice(b"moov");
    let mut rd = InflatedReader {
        data,
        pos: 0,
        fake_len: 8 << 30, // 8 GiB
    };
    // Sanity: the EOF check would pass (claim < inflated len), so a rejection
    // can only come from the MAX_ALLOC_BYTES cap.
    assert!((box_size as u64) < rd.fake_len);
    assert!(read_moov(&mut rd).is_err());
}

#[test]
fn read_moov_size_zero_spans_to_eof() {
    use std::io::Cursor;
    // size32 == 0 means "box extends to end of file"; the moov body is the rest.
    let mut b = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes()); // size = 0 → to EOF
    b.extend_from_slice(b"moov");
    b.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // body
    assert_eq!(
        read_moov(&mut Cursor::new(b)).unwrap(),
        vec![0xAA, 0xBB, 0xCC]
    );
}

#[test]
fn read_moov_largesize_overflow_is_rejected() {
    use std::io::Cursor;
    // A 64-bit largesize near u64::MAX must not wrap the `pos + box_size` EOF
    // guard (it would otherwise pass and drive an exabyte allocation).
    let mut b = Vec::new();
    b.extend_from_slice(&16u32.to_be_bytes()); // ftyp box, size 16
    b.extend_from_slice(b"ftyp");
    b.extend_from_slice(&[0u8; 8]);
    b.extend_from_slice(&1u32.to_be_bytes()); // moov, size==1 → largesize
    b.extend_from_slice(b"moov");
    b.extend_from_slice(&0xFFFF_FFFF_FFFF_FFF8u64.to_be_bytes());
    assert!(read_moov(&mut Cursor::new(b)).is_err());
}

#[test]
fn read_moov_undersized_box_does_not_hang() {
    use std::io::Cursor;
    // A box whose declared size is < 8 (here 3) must be rejected, not spin the
    // loop in place forever (size.saturating_sub(8) == 0 → no forward progress).
    let mut b = Vec::new();
    b.extend_from_slice(&3u32.to_be_bytes());
    b.extend_from_slice(b"free");
    assert!(read_moov(&mut Cursor::new(b)).is_err());
}

// `box_size == header_len` (8) is the legal empty-box boundary: an empty `moov` must parse
// to `Ok(vec![])`, not be rejected by `<`→`<=`/`==`.
#[test]
fn read_moov_exactly_header_sized_is_a_valid_empty_box() {
    use std::io::Cursor;
    let mut b = Vec::new();
    b.extend_from_slice(&8u32.to_be_bytes());
    b.extend_from_slice(b"moov");
    assert_eq!(
        read_moov(&mut Cursor::new(b)).unwrap(),
        Vec::<u8>::new(),
        "a size-8 moov has an 8-byte header and zero payload bytes"
    );
}

// The forward-progress/EOF guard is an `||` of two clauses; an input where exactly one is
// true must still be rejected as `Mp4Invalid`, not fall through to a bare `UnexpectedEof`.
#[test]
fn read_moov_rejects_a_box_that_overruns_the_file_via_the_or_not_and_guard() {
    use std::io::Cursor;
    let mut b = Vec::new();
    b.extend_from_slice(&1000u32.to_be_bytes()); // claims 1000 bytes total
    b.extend_from_slice(b"moov");
    // No more bytes: the file ends right after the 8-byte header, 992
    // bytes short of the claim.
    let err = read_moov(&mut Cursor::new(b)).unwrap_err();
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidData,
        "an overrunning box must be rejected by the guard itself (Mp4Invalid), \
             not fall through to a bare EOF error from read_exact"
    );
}

// `payload_len > MAX_ALLOC_BYTES` is a separate cap from the EOF check; a payload of
// exactly the cap must be allowed.
#[test]
fn read_moov_allows_a_payload_of_exactly_max_alloc_bytes() {
    let box_size = MAX_ALLOC_BYTES as u32 + 8; // header + exactly the cap
    let mut prefix = Vec::new();
    prefix.extend_from_slice(&box_size.to_be_bytes());
    prefix.extend_from_slice(b"moov");
    let mut rd = FakeBigReader {
        prefix,
        pos: 0,
        len: MAX_ALLOC_BYTES + 1024, // real enough bytes to satisfy read_exact
    };
    let moov = read_moov(&mut rd).expect("exactly MAX_ALLOC_BYTES must be allowed");
    assert_eq!(moov.len() as u64, MAX_ALLOC_BYTES);
}

/// Wrap `payload` in an ISO-BMFF box with the given 4-byte type.
fn mp4_box(typ: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    let size = (payload.len() + 8) as u32;
    let mut v = Vec::with_capacity(payload.len() + 8);
    v.extend_from_slice(&size.to_be_bytes());
    v.extend_from_slice(typ);
    v.extend_from_slice(payload);
    v
}

// Build a minimal-but-complete audio `trak` (one AC-3 sample) with the
// given media timescale, enough boxes to reach per-sample `to_ns` conversion.
fn audio_trak(timescale: u32) -> Vec<u8> {
    let mdhd = {
        // v0: version+flags(4) creation(4) modification(4) timescale(4) duration(4).
        let mut p = vec![0u8; 24];
        p[12..16].copy_from_slice(&timescale.to_be_bytes());
        mp4_box(b"mdhd", &p)
    };
    let hdlr = {
        // version+flags(4) pre_defined(4) handler_type(4).
        let mut p = vec![0u8; 12];
        p[8..12].copy_from_slice(b"soun");
        mp4_box(b"hdlr", &p)
    };
    let stsd = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&8u32.to_be_bytes()); // sample entry size (header only)
        p.extend_from_slice(b"ac-3"); // fourcc → Codec::Ac3
        mp4_box(b"stsd", &p)
    };
    let stsz = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&10u32.to_be_bytes()); // sample_size (fixed) = 10
        p.extend_from_slice(&1u32.to_be_bytes()); // count = 1
        mp4_box(b"stsz", &p)
    };
    let stco = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
        mp4_box(b"stco", &p)
    };
    let stsc = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
        p.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
        p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
        mp4_box(b"stsc", &p)
    };
    let stts = {
        // Mandatory time-to-sample box: 1 entry → 1 sample × 1000 ticks.
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&1u32.to_be_bytes()); // sample_count
        p.extend_from_slice(&1000u32.to_be_bytes()); // sample_delta
        mp4_box(b"stts", &p)
    };
    let mut stbl = Vec::new();
    stbl.extend_from_slice(&stsd);
    stbl.extend_from_slice(&stsz);
    stbl.extend_from_slice(&stco);
    stbl.extend_from_slice(&stsc);
    stbl.extend_from_slice(&stts);
    let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd);
    mdia.extend_from_slice(&hdlr);
    mdia.extend_from_slice(&minf);
    mp4_box(b"trak", &mp4_box(b"mdia", &mdia))
}

#[test]
fn mdhd_timescale_zero_does_not_divide_by_zero() {
    use std::io::Cursor;
    // Without the `.filter(|&t| t != 0)` guard the per-sample `to_ns` closure
    // divides by the zero timescale and panics; with it the track falls back
    // to the 90 kHz default and is indexed normally.
    let moov = mp4_box(b"moov", &audio_trak(0));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "ts0".into());
    assert!(
        rd.is_ok(),
        "timescale 0 must be handled via fallback, no divide-by-zero panic"
    );
    assert_eq!(
        rd.unwrap().info().streams.len(),
        1,
        "the timescale-0 track is still indexed"
    );
}

/// Regression: audio sample rate must come from the AudioSampleEntry 16.16
/// samplerate field, NOT the mdhd media timescale. Here the two DIFFER
/// (mdhd timescale = 90_000, AudioSampleEntry samplerate = 48_000); the
/// entry value must win. Old code read the rate from the timescale, so a
/// 90 kHz mdhd yielded SampleRate::Unknown, dropping the real 48 kHz.
// One-sample audio moov whose mdhd timescale and sample entry are given.
fn audio_moov(timescale: u32, fourcc: &[u8; 4], entry: &[u8]) -> Vec<u8> {
    let mdhd = {
        // v0: version+flags(4) creation(4) modification(4) timescale(4) duration(4).
        let mut p = vec![0u8; 24];
        p[12..16].copy_from_slice(&timescale.to_be_bytes());
        mp4_box(b"mdhd", &p)
    };
    let hdlr = {
        let mut p = vec![0u8; 12];
        p[8..12].copy_from_slice(b"soun");
        mp4_box(b"hdlr", &p)
    };
    let stsd = mp4_box(b"stsd", &stsd_with(fourcc, entry));
    // stsz: sample_size=10, count=1 (one 10-byte sample).
    let stsz = {
        let mut p = vec![0u8, 0, 0, 0];
        p.extend_from_slice(&10u32.to_be_bytes());
        p.extend_from_slice(&1u32.to_be_bytes());
        mp4_box(b"stsz", &p)
    };
    let stco = {
        let mut p = vec![0u8, 0, 0, 0];
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
        mp4_box(b"stco", &p)
    };
    let stsc = {
        let mut p = vec![0u8, 0, 0, 0];
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
        p.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
        p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
        mp4_box(b"stsc", &p)
    };
    let stts = {
        let mut p = vec![0u8, 0, 0, 0];
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&1u32.to_be_bytes()); // sample_count
        p.extend_from_slice(&1000u32.to_be_bytes()); // sample_delta
        mp4_box(b"stts", &p)
    };
    let mut stbl = Vec::new();
    for b in [&stsd, &stsz, &stco, &stsc, &stts] {
        stbl.extend_from_slice(b);
    }
    let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd);
    mdia.extend_from_slice(&hdlr);
    mdia.extend_from_slice(&minf);
    let trak = mp4_box(b"trak", &mp4_box(b"mdia", &mdia));
    mp4_box(b"moov", &trak)
}

fn read_audio(moov: Vec<u8>) -> AudioStream {
    let rd = Mp4Reader::from_reader(std::io::Cursor::new(moov), "sr".into()).unwrap();
    match &rd.info().streams[0] {
        DiscStream::Audio(a) => a.clone(),
        other => panic!("expected an audio stream, got {other:?}"),
    }
}

/// Regression: audio sample rate must come from the AudioSampleEntry 16.16
/// samplerate field, NOT the mdhd media timescale, when they merely differ
/// (a 90 kHz mdhd is not an audio rate).
#[test]
fn audio_sample_entry_samplerate_wins_over_mdhd_timescale() {
    let a = read_audio(audio_moov(90_000, b"ac-3", &audio_entry(6, 0, &[])));
    assert_eq!(a.sample_rate, SampleRate::S48);
}

// The 16-bit entry field cannot hold 96 kHz: our own Mp4Sink saturates it to
// 65535, and a DTS-HD entry may carry the 48 kHz core rate. mdhd wins then.
#[test]
fn a_high_rate_mdhd_timescale_beats_an_unrepresentable_entry_rate() {
    for entry_rate in [0xFFFFu32, 48_000] {
        let mut e = audio_entry(6, 0, &[]);
        e[24..28].copy_from_slice(&(entry_rate << 16).to_be_bytes());
        let a = read_audio(audio_moov(96_000, b"dtsh", &e));
        assert_eq!(a.sample_rate, SampleRate::S96, "entry {entry_rate}");
    }
}

// Without ddts, only a DTS-HD entry takes a 2x/4x mdhd timescale; dtsc keeps its rate.
#[test]
fn a_dts_timescale_counts_only_as_2x_or_4x_on_dts_hd_entries() {
    let entry = |hz: u32| {
        let mut e = audio_entry(6, 0, &[]);
        e[24..28].copy_from_slice(&(hz << 16).to_be_bytes());
        e
    };
    for (fourcc, entry_hz, ts, want) in [
        (b"dtsc", 48_000, 96_000, SampleRate::S48),
        (b"dtsh", 48_000, 192_000, SampleRate::S192),
        (b"dtsl", 44_100, 88_200, SampleRate::S88_2),
        (b"dtsh", 32_000, 96_000, SampleRate::from_hz(32_000)),
    ] {
        let a = read_audio(audio_moov(ts, fourcc, &entry(entry_hz)));
        assert_eq!(a.sample_rate, want, "{fourcc:?} {entry_hz} {ts}");
    }
}

// ETSI TS 102 114 E.2.2.3: ddts DTSSamplingFrequency is the stream's maximum rate.
#[test]
fn a_dts_ddts_max_rate_beats_the_family_entry_rate() {
    let mut ddts = 96_000u32.to_be_bytes().to_vec();
    ddts.extend_from_slice(&[0u8; 15]);
    let e = audio_entry(6, 0, &mp4_box(b"ddts", &ddts));
    let a = read_audio(audio_moov(90_000, b"dtsh", &e));
    assert_eq!(a.sample_rate, SampleRate::S96);
}

// A ddts rate SampleRate cannot name reads as Unknown, never the entry's family base.
#[test]
fn a_dts_ddts_rate_outside_sample_rate_is_not_read_as_its_family_base() {
    for hz in [12_000u32, 24_000, 32_000] {
        let mut ddts = hz.to_be_bytes().to_vec();
        ddts.extend_from_slice(&[0u8; 15]);
        // audio_entry's samplerate is 48000, the family base of 12/24 kHz.
        let e = audio_entry(6, 0, &mp4_box(b"ddts", &ddts));
        let a = read_audio(audio_moov(48_000, b"dtsc", &e));
        assert_eq!(a.sample_rate, SampleRate::Unknown, "{hz}");
    }
}

// A 96 kHz DTS-HD MA track written by Mp4Sink reads back as 96 kHz.
#[test]
fn a_96k_dts_hd_track_round_trips_its_rate() {
    use crate::disc::{AudioChannels, AudioStream, Codec, DiscTitle, LabelPurpose};
    use crate::mux::mp4::Mp4Sink;
    use crate::pes::PesFrame;
    use std::io::Cursor;

    let mut t = DiscTitle::empty();
    t.streams = vec![
        DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R1080p,
            frame_rate: crate::disc::FrameRate::F23_976,
            hdr: crate::disc::HdrFormat::Sdr,
            color_space: crate::disc::ColorSpace::Unknown,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }),
        DiscStream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::DtsHdMa,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S96,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }),
    ];
    t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC]), None];
    // 48 kHz DTS core (SFREQ=13), as on a 96 kHz DTS-HD MA disc track.
    let core = vec![
        0x7F, 0xFE, 0x80, 0x01, 0x00, 0x3C, 0x05, 0xF2, 0x77, 0x00, 0x02, 0x00,
    ];
    let mut buf = Vec::new();
    {
        let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
        for i in 0..2i64 {
            sink.write(&PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts: i * 41_708_333,
                keyframe: i == 0,
                data: vec![0x11; 64],
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
            sink.write(&PesFrame {
                discard_padding_ns: 0,
                track: 1,
                pts: i * 5_333_333,
                keyframe: true,
                data: core.clone(),
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
        }
        sink.finish().unwrap();
    }
    let rd = Mp4Reader::from_reader(Cursor::new(buf), "dts96".into()).unwrap();
    match &rd.info().streams[1] {
        DiscStream::Audio(a) => assert_eq!(a.sample_rate, SampleRate::S96),
        other => panic!("expected audio, got {other:?}"),
    }
}

// A valid known entry rate is kept even when mdhd uses a higher timescale.
#[test]
fn a_known_entry_rate_beats_a_higher_timescale() {
    let a = read_audio(audio_moov(96_000, b"ac-3", &audio_entry(2, 0, &[])));
    assert_eq!(a.sample_rate, SampleRate::S48);
}

// ISO AudioSampleEntryV1 (stsd version 1): not a QuickTime layout; children
// follow the 28-byte part and `srat` carries a rate above 65535.
#[test]
fn an_iso_v1_sample_entry_reads_srat_and_keeps_its_children() {
    let asc = [0x11u8, 0x90];
    let mut esds = vec![0u8, 0, 0, 0, 0x03, 22, 0, 1, 0, 0x04, 17, 0x40, 0x15];
    esds.extend_from_slice(&[0u8; 11]);
    esds.extend_from_slice(&[0x05, 2]);
    esds.extend_from_slice(&asc);
    let mut srat = vec![0u8; 4];
    srat.extend_from_slice(&96_000u32.to_be_bytes());
    let mut e = audio_entry(2, 0, &[]);
    e[8..10].copy_from_slice(&1u16.to_be_bytes());
    e[24..28].copy_from_slice(&(1u32 << 16).to_be_bytes());
    e.extend_from_slice(&mp4_box(b"esds", &esds));
    e.extend_from_slice(&mp4_box(b"srat", &srat));
    let mut stsd = stsd_with(b"mp4a", &e);
    stsd[0] = 1;
    let info = parse_stsd(&stsd).expect("mp4a parses");
    assert_eq!(info.config.as_deref(), Some(&asc[..]));
    assert_eq!(info.sample_rate, 96_000);
}

// QuickTime SoundDescription v2: the 16.16 field is fixed at 1.0 and the real
// rate is a float64 at body offset 32; channels at 40; children at 64.
#[test]
fn a_quicktime_v2_sound_description_reads_its_float_rate_and_channels() {
    let mut e = vec![0u8; 64];
    e[8..10].copy_from_slice(&2u16.to_be_bytes());
    e[16..18].copy_from_slice(&3u16.to_be_bytes());
    e[24..28].copy_from_slice(&(1u32 << 16).to_be_bytes());
    e[32..40].copy_from_slice(&44_100f64.to_bits().to_be_bytes());
    e[40..44].copy_from_slice(&6u32.to_be_bytes());
    let a = read_audio(audio_moov(1_000, b"ac-3", &e));
    assert_eq!(a.sample_rate, SampleRate::S44_1);
    assert_eq!(a.channels, AudioChannels::from_count(6));
}

// QuickTime SoundDescription v1 inserts 16 bytes before the child boxes, so an
// mp4a esds must be looked up at body offset 44, not 28.
#[test]
fn a_quicktime_v1_sound_description_finds_its_esds_after_the_v1_fields() {
    let asc = [0x12u8, 0x10];
    let mut esds = vec![0u8, 0, 0, 0, 0x03, 22, 0, 1, 0, 0x04, 17, 0x40, 0x15];
    esds.extend_from_slice(&[0u8; 11]);
    esds.extend_from_slice(&[0x05, 2]);
    esds.extend_from_slice(&asc);
    let mut e = audio_entry(2, 0, &[]);
    e[8..10].copy_from_slice(&1u16.to_be_bytes());
    e.extend_from_slice(&[0u8; 16]);
    e.extend_from_slice(&mp4_box(b"esds", &esds));
    let info = parse_stsd(&stsd_with(b"mp4a", &e)).expect("mp4a parses");
    assert_eq!(info.config.as_deref(), Some(&asc[..]));
}

// ============================================================
// Edit lists — ISO/IEC 14496-12 §8.6.5 (`edts`) / §8.6.6 (`elst`).
// ============================================================

/// A `mvhd` payload declaring the movie timescale (ISO/IEC 14496-12 §8.2.2).
fn mvhd_box(timescale: u32) -> Vec<u8> {
    // v0: version+flags(4) creation(4) modification(4) timescale(4) duration(4) …
    let mut p = vec![0u8; 100];
    p[12..16].copy_from_slice(&timescale.to_be_bytes());
    mp4_box(b"mvhd", &p)
}

/// A version-0 `elst` payload: `(segment_duration, media_time)` per entry,
/// each at media_rate 1.
fn elst_v0(entries: &[(u32, i32)]) -> Vec<u8> {
    let mut p = vec![0u8, 0, 0, 0]; // version 0 + flags
    p.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for &(seg, media_time) in entries {
        p.extend_from_slice(&seg.to_be_bytes());
        p.extend_from_slice(&media_time.to_be_bytes());
        p.extend_from_slice(&1i16.to_be_bytes()); // media_rate_integer
        p.extend_from_slice(&0i16.to_be_bytes()); // media_rate_fraction
    }
    p
}

/// Insert an `edts > elst` into an existing `trak` box.
fn trak_with_elst(trak: &[u8], elst_payload: &[u8]) -> Vec<u8> {
    let mut payload = mp4_box(b"edts", &mp4_box(b"elst", elst_payload));
    payload.extend_from_slice(&trak[8..]); // the original trak's children
    mp4_box(b"trak", &payload)
}

// Regression (silent A/V desync): a non-empty edit with `media_time = 1024` (encoder delay)
// must shift the track's presentation, not be ignored.
#[test]
fn edit_list_media_time_shifts_the_presentation_timeline() {
    use std::io::Cursor;
    let trak = trak_with_elst(&audio_trak(48_000), &elst_v0(&[(0, 1024)]));
    let moov = mp4_box(b"moov", &trak);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "elst".into()).unwrap();
    assert_eq!(rd.samples.len(), 1);
    // media_time 1024 at 48 kHz trims 1024 ticks off the front, so the first
    // sample sits 1024 ticks BEFORE the presentation origin.
    let want = -(1024i128 * NS / 48_000) as i64;
    assert_eq!(rd.samples[0].pts_ns, want, "media_time must shift the pts");
    assert_eq!(rd.samples[0].dts_ns, want, "and the dts with it");
    assert_ne!(want, 0, "the shift is observable");
}

/// An EMPTY edit (`media_time == -1`) delays presentation by its
/// `segment_duration`, which is in MOVIE timescale ticks and must be
/// converted to the track's media timescale before it is applied.
#[test]
fn empty_edit_delays_presentation_in_movie_timescale() {
    use std::io::Cursor;
    // Movie timescale 1000 → segment_duration 40 = 40 ms of blank leader,
    // then the media edit itself.
    let trak = trak_with_elst(&audio_trak(48_000), &elst_v0(&[(40, -1), (0, 0)]));
    let mut moov_payload = mvhd_box(1000);
    moov_payload.extend_from_slice(&trak);
    let moov = mp4_box(b"moov", &moov_payload);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "empty-edit".into()).unwrap();
    assert_eq!(rd.samples.len(), 1);
    assert_eq!(
        rd.samples[0].pts_ns, 40_000_000,
        "a 40 ms empty edit delays the track by 40 ms"
    );
}

/// A track with no `edts` is untouched — the shift only ever comes from a
/// declared edit list.
#[test]
fn no_edit_list_leaves_the_timeline_at_zero() {
    use std::io::Cursor;
    let moov = mp4_box(b"moov", &audio_trak(48_000));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "no-elst".into()).unwrap();
    assert_eq!(rd.samples[0].pts_ns, 0);
    assert_eq!(rd.samples[0].dts_ns, 0);
}

/// `elst` decoding: both versions, the entry count bounded by the box's own
/// bytes, and the offset arithmetic in isolation.
#[test]
fn parse_elst_and_offset_arithmetic() {
    // Version 0, two entries: an empty edit then a media edit.
    let v0 = elst_v0(&[(40, -1), (0, 1024)]);
    let entries = parse_elst(&v0);
    assert_eq!(entries, vec![(40, -1, 1), (0, 1024, 1)]);

    // Version 1: 64-bit segment_duration and media_time.
    let mut v1 = vec![1u8, 0, 0, 0];
    v1.extend_from_slice(&1u32.to_be_bytes());
    v1.extend_from_slice(&5_000u64.to_be_bytes());
    v1.extend_from_slice(&(-1i64).to_be_bytes());
    v1.extend_from_slice(&1i16.to_be_bytes());
    v1.extend_from_slice(&0i16.to_be_bytes());
    assert_eq!(parse_elst(&v1), vec![(5_000, -1, 1)]);

    // A declared count larger than the box can hold is clamped by the bytes.
    let mut lying = elst_v0(&[(0, 0)]);
    lying[4..8].copy_from_slice(&9_999u32.to_be_bytes());
    assert_eq!(parse_elst(&lying).len(), 1, "bounded by the box bytes");
    // Too short to hold even the header → no entries, no panic.
    assert!(parse_elst(&[0, 0, 0, 0]).is_empty());

    // Offset: the empty edit's 40 movie ticks at movie timescale 1000 is
    // 40 ms = 1920 ticks at 48 kHz, minus a media_time trim of 1024.
    assert_eq!(
        elst_offset_ticks(&[(40, -1, 1), (0, 1024, 1)], Some(1000), 48_000, 0),
        1920 - 1024
    );
    // No movie timescale → the empty edit's delay cannot be converted, so
    // only the trim applies (and it is logged, not silently dropped).
    assert_eq!(
        elst_offset_ticks(&[(40, -1, 1), (0, 1024, 1)], None, 48_000, 0),
        -1024
    );
    // An empty list, or a single identity edit, shifts nothing.
    assert_eq!(elst_offset_ticks(&[], Some(1000), 48_000, 0), 0);
    assert_eq!(elst_offset_ticks(&[(1000, 0, 1)], Some(1000), 48_000, 0), 0);
    // Only the FIRST media edit's media_time is applied; trailing empty
    // edits do not add to the leading delay.
    assert_eq!(
        elst_offset_ticks(&[(0, 512, 1), (40, -1, 1)], Some(1000), 48_000, 0),
        -512
    );
    // A hostile segment_duration cannot overflow the tick conversion.
    assert_eq!(
        elst_offset_ticks(&[(u64::MAX, -1, 1)], Some(1), 48_000, 0),
        i64::MAX,
    );
}

// Every byte of a version-1 entry's fields must come from its own offset, not a
// neighbour's; every byte here is distinct and nonzero.
#[test]
fn parse_elst_v1_entry_bytes_come_from_their_own_offsets() {
    let mut v1 = vec![1u8, 0, 0, 0]; // version 1 + flags
    v1.extend_from_slice(&1u32.to_be_bytes()); // entry_count = 1
    v1.extend_from_slice(&0x1122334455667788u64.to_be_bytes()); // segment_duration
    v1.extend_from_slice(&0x0102030405060708i64.to_be_bytes()); // media_time
    v1.extend_from_slice(&0x2A3Bi16.to_be_bytes()); // media_rate_integer
    v1.extend_from_slice(&0x5C6Di16.to_be_bytes()); // media_rate_fraction (unread)
    assert_eq!(
        parse_elst(&v1),
        vec![(0x1122334455667788u64, 0x0102030405060708i64, 0x2A3Bi16)]
    );
}

// At `empty_movie_ticks == 0` the function must short-circuit before a `movie_timescale ==
// Some(0)` is ever used as a divisor, independent of what the one real caller happens to
// pass.
#[test]
fn elst_offset_ticks_stays_safe_at_zero_empty_ticks_even_with_a_zero_movie_timescale() {
    assert_eq!(
        elst_offset_ticks(&[], Some(0), 48_000, 0),
        0,
        "no empty edit (empty_movie_ticks == 0) must short-circuit before \
             the timescale is ever used as a divisor"
    );
    // Sanity: a REAL empty edit with a zero movie timescale is a separate,
    // already-latent, unreachable-from-from_reader situation this test does
    // not claim to fix — it is outside the empty_movie_ticks == 0 boundary pinned here.
}

#[test]
fn trak_loop_stops_at_max_tracks() {
    use std::io::Cursor;
    // A crafted moov packing more than MAX_TRACKS trak boxes must not index
    // past the cap — the per-track PID `0x1011 + idx` overflows u16 past ~61k
    // tracks. Without the cap this indexes all MAX_TRACKS + 50 tracks.
    let mut traks = Vec::new();
    for _ in 0..(MAX_TRACKS + 50) {
        traks.extend_from_slice(&audio_trak(48_000));
    }
    let moov = mp4_box(b"moov", &traks);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "many".into()).unwrap();
    assert_eq!(
        rd.info().streams.len(),
        MAX_TRACKS,
        "trak loop must stop at MAX_TRACKS"
    );
}

// A minimal audio `trak` whose fixed-size `stsz` LIES about its sample count (`u32::MAX`);
// stsc/stts are wide enough to place whatever count survives the budget.
fn audio_trak_hostile_count() -> Vec<u8> {
    let mdhd = {
        let mut p = vec![0u8; 24];
        p[12..16].copy_from_slice(&48_000u32.to_be_bytes()); // timescale
        mp4_box(b"mdhd", &p)
    };
    let hdlr = {
        let mut p = vec![0u8; 12];
        p[8..12].copy_from_slice(b"soun");
        mp4_box(b"hdlr", &p)
    };
    let stsd = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&8u32.to_be_bytes()); // sample entry size (header only)
        p.extend_from_slice(b"ac-3"); // fourcc → Codec::Ac3
        mp4_box(b"stsd", &p)
    };
    let stsz = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&10u32.to_be_bytes()); // sample_size != 0 (fixed)
        p.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes()); // count = u32::MAX (lie)
        mp4_box(b"stsz", &p)
    };
    let stco = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
        mp4_box(b"stco", &p)
    };
    let stsc = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
        // Deliberately large: stsz COUNT is the lie under test, so stsc/stts must
        // still cover whatever count survives the file_len bound (both clamp
        // to the real sample count when expanded).
        p.extend_from_slice(&0xFFFFu32.to_be_bytes()); // samples_per_chunk
        p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
        mp4_box(b"stsc", &p)
    };
    let stts = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&0xFFFFu32.to_be_bytes()); // sample_count (see stsc)
        p.extend_from_slice(&1000u32.to_be_bytes()); // sample_delta
        mp4_box(b"stts", &p)
    };
    let mut stbl = Vec::new();
    stbl.extend_from_slice(&stsd);
    stbl.extend_from_slice(&stsz);
    stbl.extend_from_slice(&stco);
    stbl.extend_from_slice(&stsc);
    stbl.extend_from_slice(&stts);
    let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd);
    mdia.extend_from_slice(&hdlr);
    mdia.extend_from_slice(&minf);

    mp4_box(b"trak", &mp4_box(b"mdia", &mdia))
}

#[test]
fn stsz_sample_count_bounded_by_file_len() {
    use std::io::Cursor;
    let trak = audio_trak_hostile_count();
    let moov = mp4_box(b"moov", &trak);

    let file_len = moov.len() as u64;
    assert!(
        file_len < 1024,
        "fixture stays a few hundred bytes ({file_len})"
    );
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "hostile".into()).unwrap();
    // Bounded by FILE BYTES PER SAMPLE, not one sample per byte: each indexed
    // sample costs ~60 bytes of RAM, so one-sample-per-byte still let a 16 MiB
    // crafted file force a ~1 GiB allocation (64x amplification).
    assert!(
        (rd.samples.len() as u64) <= file_len / MIN_FILE_BYTES_PER_SAMPLE,
        "sample count {} must be bounded by file_len/{MIN_FILE_BYTES_PER_SAMPLE} \
             ({}), not by the count lie",
        rd.samples.len(),
        file_len / MIN_FILE_BYTES_PER_SAMPLE
    );
    assert!(
        rd.samples.len() < MAX_SAMPLE_COUNT,
        "a tiny file must not allocate the 16M MAX_SAMPLE_COUNT ceiling"
    );
}

// The RAM amplification the byte-per-sample budget bounds: a crafted `stsz` claiming
// u32::MAX samples must not force an eager multi-hundred-MB index.
#[test]
fn sample_index_ram_is_bounded_by_a_multiple_of_the_file() {
    use std::io::Cursor;
    // ~64 KiB of `free` padding so file_len is large enough for the ratio to
    // be meaningful, with the same lying stsz as above.
    let mut traks = Vec::new();
    traks.extend_from_slice(&audio_trak_hostile_count());
    let mut file = mp4_box(b"moov", &traks);
    file.extend_from_slice(&mp4_box(b"free", &vec![0u8; 64 * 1024]));
    let file_len = file.len() as u64;

    let rd = Mp4Reader::from_reader(Cursor::new(file), "amp".into()).unwrap();
    // ~60 bytes of RAM per sample; assert the index cannot exceed ~4x the file.
    const RAM_PER_SAMPLE: u64 = 60;
    let ram = rd.samples.len() as u64 * RAM_PER_SAMPLE;
    assert!(
        ram <= file_len * 4,
        "sample index RAM {ram} B from a {file_len} B file is more than 4x \
             amplification ({} samples)",
        rd.samples.len()
    );
}

// Build an audio `trak` like `audio_trak(48_000)` but with the named stbl
// child box omitted, to reach the untrusted-input guards that drop a
// track whose `stsz` says samples exist yet stco/co64/stsc is missing.
fn audio_trak_missing(omit: &[u8; 4]) -> Vec<u8> {
    let mdhd = {
        let mut p = vec![0u8; 24];
        p[12..16].copy_from_slice(&48_000u32.to_be_bytes()); // timescale
        mp4_box(b"mdhd", &p)
    };
    let hdlr = {
        let mut p = vec![0u8; 12];
        p[8..12].copy_from_slice(b"soun");
        mp4_box(b"hdlr", &p)
    };
    let stsd = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&8u32.to_be_bytes()); // sample entry size (header only)
        p.extend_from_slice(b"ac-3"); // fourcc → Codec::Ac3
        mp4_box(b"stsd", &p)
    };
    let stsz = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&10u32.to_be_bytes()); // sample_size (fixed) = 10
        p.extend_from_slice(&3u32.to_be_bytes()); // count = 3 (samples exist)
        mp4_box(b"stsz", &p)
    };
    // Compact form of the same three 10-byte samples (omit == "stz2" swaps it in).
    let stz2 = {
        let mut p = vec![0u8; 4]; // version+flags
        p.extend_from_slice(&[0, 0, 0, 8]); // reserved, field_size = 8
        p.extend_from_slice(&3u32.to_be_bytes()); // count
        p.extend_from_slice(&[10, 10, 10]);
        mp4_box(b"stz2", &p)
    };
    let stco = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
        mp4_box(b"stco", &p)
    };
    let stsc = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
        // All 3 samples live in the single chunk; MUST match stsz count, since
        // an stsc placing fewer samples than stsz declares gets its track dropped.
        p.extend_from_slice(&3u32.to_be_bytes()); // samples_per_chunk
        p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
        mp4_box(b"stsc", &p)
    };
    let stts = {
        // Mandatory time-to-sample box: 3 entries' worth via one run (3×1000).
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&3u32.to_be_bytes()); // sample_count
        p.extend_from_slice(&1000u32.to_be_bytes()); // sample_delta
        mp4_box(b"stts", &p)
    };
    let mut stbl = Vec::new();
    stbl.extend_from_slice(&stsd);
    if omit != b"stsz" && omit != b"stz2" {
        stbl.extend_from_slice(&stsz);
    }
    if omit == b"stz2" {
        stbl.extend_from_slice(&stz2);
    }
    if omit != b"stco" {
        stbl.extend_from_slice(&stco);
    }
    if omit != b"stsc" {
        stbl.extend_from_slice(&stsc);
    }
    if omit != b"stts" {
        stbl.extend_from_slice(&stts);
    }
    let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd);
    mdia.extend_from_slice(&hdlr);
    mdia.extend_from_slice(&minf);
    mp4_box(b"trak", &mp4_box(b"mdia", &mdia))
}

// Samples come out in global decode order, interleaved across tracks.
#[test]
fn samples_from_two_tracks_are_interleaved_by_decode_time() {
    use std::io::Cursor;
    let mut body = audio_trak_missing(b"____");
    body.extend(audio_trak_missing(b"____"));
    let rd = Mp4Reader::from_reader(Cursor::new(mp4_box(b"moov", &body)), "two".into()).unwrap();
    let order: Vec<usize> = rd.samples.iter().map(|s| s.track).collect();
    assert_eq!(order, vec![0, 1, 0, 1, 0, 1]);
}

// The title duration is the longest track, whatever order the tracks come in.
#[test]
fn title_duration_is_the_longest_track() {
    use std::io::Cursor;
    let mut body = audio_trak(1_000); // 1 s
    body.extend(audio_trak(48_000)); // ~21 ms
    let rd = Mp4Reader::from_reader(Cursor::new(mp4_box(b"moov", &body)), "dur".into()).unwrap();
    assert!((rd.info().duration_secs - 1.0).abs() < 1e-9);
}

// A 16.16 samplerate whose integer part was truncated to u16 resolves to the mdhd rate.
#[test]
fn audio_rate_recovers_a_u16_truncated_entry_rate() {
    assert_eq!(audio_rate(96_000 & 0xFFFF, 96_000, None), 96_000);
    assert_eq!(audio_rate(48_000, 48_000, None), 48_000);
    assert_eq!(audio_rate(44_100, 96_000, None), 44_100);
}

// A compact `stz2` size table carries the track's samples like `stsz` does.
#[test]
fn stz2_sizes_populate_the_track_samples() {
    use std::io::Cursor;
    let moov = mp4_box(b"moov", &audio_trak_missing(b"stz2"));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "stz2".into()).unwrap();
    assert_eq!(rd.samples.len(), 3);
    assert_eq!(rd.samples[0].size, 10);
}

#[test]
fn parse_stz2_decodes_each_field_size() {
    let hdr = |fs: u8, n: u32| {
        let mut b = vec![0, 0, 0, 0, 0, 0, 0, fs];
        b.extend_from_slice(&n.to_be_bytes());
        b
    };
    let with = |fs, n, body: &[u8]| [hdr(fs, n), body.to_vec()].concat();
    assert_eq!(parse_stz2(&with(4, 3, &[0x12, 0x30]), 99), vec![1, 2, 3]);
    assert_eq!(parse_stz2(&with(8, 2, &[7, 9]), 99), vec![7, 9]);
    assert_eq!(parse_stz2(&with(16, 1, &[1, 2]), 99), vec![0x0102]);
    assert!(parse_stz2(&with(12, 1, &[1, 2]), 99).is_empty());
    assert_eq!(parse_stz2(&with(8, 5, &[7, 9]), 99), vec![7, 9]);
}

// A track with samples (`stsz`) but no chunk-offset table (`stco`/`co64`) must be DROPPED,
// not indexed with offsets near file byte 0.
#[test]
fn missing_stco_drops_track_all_dropped_is_invalid() {
    use std::io::Cursor;
    let moov = mp4_box(b"moov", &audio_trak_missing(b"stco"));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "no-stco".into());
    assert!(
        rd.is_err(),
        "a track with stsz but no stco/co64 must be dropped; all-dropped → Mp4Invalid"
    );
}

// A track with samples + chunk offsets but no sample-to-chunk map (`stsc`) must be DROPPED.
#[test]
fn missing_stsc_drops_track_all_dropped_is_invalid() {
    use std::io::Cursor;
    let moov = mp4_box(b"moov", &audio_trak_missing(b"stsc"));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "no-stsc".into());
    assert!(
        rd.is_err(),
        "a track with stsz + stco but no stsc must be dropped; all-dropped → Mp4Invalid"
    );
}

// A track with samples but no time-to-sample table (`stts`, mandatory per §8.6.1) must be
// DROPPED.
#[test]
fn missing_stts_drops_track_all_dropped_is_invalid() {
    use std::io::Cursor;
    let moov = mp4_box(b"moov", &audio_trak_missing(b"stts"));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "no-stts".into());
    assert!(
        rd.is_err(),
        "a track with stsz but no stts must be dropped; all-dropped → Mp4Invalid"
    );
}

// An `stsc` that passes the non-empty guard but places fewer samples than `stsz` declares
// must still drop the track, not invent positions for the tail.
#[test]
fn stsc_placing_fewer_samples_than_stsz_drops_the_track() {
    use std::io::Cursor;
    // audio_trak_missing(b"____") is the complete, consistent fixture: stsz=3
    // and stsc samples_per_chunk=3. Knock samples_per_chunk down to 1 so the
    // stsc is present and non-empty yet places only sample 1 of 3.
    let mut trak = audio_trak_missing(b"____");
    let stsc_tag = b"stsc";
    let pos = trak
        .windows(4)
        .position(|w| w == stsc_tag)
        .expect("fixture has an stsc");
    // stsc payload: version+flags(4) entry_count(4) first_chunk(4)
    // samples_per_chunk(4) — so samples_per_chunk starts 16 bytes past the tag.
    let spc = pos + 4 + 12;
    assert_eq!(
        &trak[spc..spc + 4],
        &3u32.to_be_bytes(),
        "expected the consistent fixture's samples_per_chunk = 3"
    );
    trak[spc..spc + 4].copy_from_slice(&1u32.to_be_bytes());

    let moov = mp4_box(b"moov", &trak);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "short-stsc".into());
    assert!(
        rd.is_err(),
        "an stsc that places 1 of 3 samples must drop the track; \
             all-dropped → Mp4Invalid"
    );
}

// A SHORT `stts` covering fewer samples than `stsz` declares must drop the track just like
// an absent one, not collapse the tail onto dur=0.
#[test]
fn short_stts_drops_the_track_like_an_absent_one() {
    use std::io::Cursor;
    let mut trak = audio_trak_missing(b"____");
    let pos = trak
        .windows(4)
        .position(|w| w == b"stts")
        .expect("fixture has an stts");
    // stts payload: version+flags(4) entry_count(4) sample_count(4) delta(4)
    let sample_count = pos + 4 + 8;
    assert_eq!(
        &trak[sample_count..sample_count + 4],
        &3u32.to_be_bytes(),
        "expected the consistent fixture's stts sample_count = 3"
    );
    trak[sample_count..sample_count + 4].copy_from_slice(&1u32.to_be_bytes());

    let moov = mp4_box(b"moov", &trak);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "short-stts".into());
    assert!(
        rd.is_err(),
        "an stts covering 1 of 3 samples must drop the track; \
             all-dropped → Mp4Invalid"
    );
}

/// Sanity companion: the SAME builder WITH both tables present yields a valid,
/// indexed single-track file — proving the two Err results above come from the
/// missing table, not from some unrelated defect in the fixture builder.
#[test]
fn audio_trak_missing_none_is_valid() {
    use std::io::Cursor;
    // omit a box that isn't in the stbl → nothing omitted, fixture is complete.
    let moov = mp4_box(b"moov", &audio_trak_missing(b"____"));
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "complete".into())
        .expect("complete stbl (stsz+stco+stsc) must index");
    assert_eq!(rd.info().streams.len(), 1, "the complete track is indexed");
}

#[test]
fn mdhd_language_offsets_per_version() {
    // "eng" packed = 0x15C7. v0 carries it at byte 20, v1 (64-bit times) at 32.
    let packed = [0x15u8, 0xC7];
    let mut v0 = vec![0u8; 22];
    v0[0] = 0; // version 0
    v0[20..22].copy_from_slice(&packed);
    assert_eq!(mdhd_language(&v0).as_deref(), Some("eng"));
    let mut v1 = vec![0u8; 34];
    v1[0] = 1; // version 1
    v1[32..34].copy_from_slice(&packed);
    assert_eq!(mdhd_language(&v1).as_deref(), Some("eng"));
}

// The length guard is `b.len() < off + 2` — reject too SHORT, not "not exactly `off + 2`";
// pins both edges of that boundary.
#[test]
fn mdhd_language_boundary_rejects_short_not_merely_non_exact() {
    let short = vec![0u8; 20]; // == off, zero room for the language field
    assert_eq!(
        mdhd_language(&short),
        None,
        "no room for the packed language field must be None, not a panic"
    );

    let packed = [0x15u8, 0xC7]; // "eng"
    let mut longer = vec![0u8; 23]; // == off + 3, one byte past the minimum
    longer[20..22].copy_from_slice(&packed);
    assert_eq!(
        mdhd_language(&longer).as_deref(),
        Some("eng"),
        "a buffer LARGER than the minimum must still succeed"
    );
}

// ── Sample-entry field offsets (ISO/IEC 14496-12 §12.1.3, §12.2.3) ────────

/// Build an `stsd` payload holding ONE sample entry of type `fourcc`.
fn stsd_with(fourcc: &[u8; 4], entry_body: &[u8]) -> Vec<u8> {
    let mut p = vec![0u8, 0, 0, 0]; // version + flags
    p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    p.extend_from_slice(&mp4_box(fourcc, entry_body));
    p
}

// A `VisualSampleEntry` body (§12.1.3) with DISTINCT width/height, plus
// optional child boxes. Fixed part is 78 bytes: width(2) at 24, height(2) at 26.
fn visual_entry(width: u16, height: u16, children: &[u8]) -> Vec<u8> {
    let mut b = vec![0u8; 78];
    b[24..26].copy_from_slice(&width.to_be_bytes());
    b[26..28].copy_from_slice(&height.to_be_bytes());
    b.extend_from_slice(children);
    b
}

// An `AudioSampleEntry` body (§12.2.3): channelcount(2) at 16, 28-byte
// fixed part, then child boxes. `decoy` at 14 makes an offset slip visible.
fn audio_entry(channels: u16, decoy: u16, children: &[u8]) -> Vec<u8> {
    let mut b = vec![0u8; 28];
    b[14..16].copy_from_slice(&decoy.to_be_bytes());
    b[16..18].copy_from_slice(&channels.to_be_bytes());
    b[18..20].copy_from_slice(&16u16.to_be_bytes()); // samplesize
    b[24..28].copy_from_slice(&(48_000u32 << 16).to_be_bytes());
    b.extend_from_slice(children);
    b
}

// `height` is the SECOND 16-bit dimension in a VisualSampleEntry, at byte 26 (width at 24);
// reading the wrong one is silent, so width != height here.
#[test]
fn parse_stsd_takes_height_from_its_own_field_not_the_width_beside_it() {
    let stsd = stsd_with(b"avc1", &visual_entry(1920, 1080, &[]));
    let info = parse_stsd(&stsd).expect("an avc1 entry parses");
    assert!(matches!(info.codec, Codec::H264), "avc1 is H.264");
    assert_eq!(info.height, 1080, "height is at byte 26, width at 24");
    assert_eq!(info.channels, 0, "a video entry declares no channel count");

    // And a VisualSampleEntry too short to hold the fixed part is refused
    // rather than read out of a shorter buffer.
    let short = stsd_with(b"avc1", &[0u8; 40]);
    assert!(
        parse_stsd(&short).is_none(),
        "a truncated VisualSampleEntry has no dimensions to read"
    );
}

// `channelcount` is at byte 16 of an AudioSampleEntry; an offset slip reads a
// conventionally-zero reserved field instead. Also pins the short-entry fallback of 2.
#[test]
fn parse_stsd_reads_channelcount_from_its_own_field_and_defaults_a_short_entry() {
    let stsd = stsd_with(b"ac-3", &audio_entry(6, 0xBEEF, &[]));
    let info = parse_stsd(&stsd).expect("an ac-3 entry parses");
    assert!(matches!(info.codec, Codec::Ac3), "ac-3 is AC-3");
    assert_eq!(
        info.channels, 6,
        "channelcount is at byte 16 — 0xBEEF at 14 is the reserved field"
    );
    assert_eq!(info.height, 0, "an audio entry declares no height");

    // Too short for the 28-byte fixed part: fall back to stereo, not to 0.
    let short = stsd_with(b"ac-3", &[0u8; 12]);
    let info = parse_stsd(&short).expect("a short audio entry still names a codec");
    assert_eq!(
        info.channels, 2,
        "an entry with no readable channelcount defaults to stereo"
    );
}

// `mp4a` codec_private is only read when the entry is `Codec::Aac` AND its body is >= the
// 28-byte fixed part, else `find_box(&body[28..],...)` would slice past the end.
#[test]
fn parse_stsd_extracts_aac_codec_private_only_when_the_body_is_long_enough() {
    let asc = vec![0x12u8, 0x10]; // AAC-LC 44.1 kHz stereo
    let esds = vec![
        0, 0, 0, 0, // version+flags
        0x03, 0x19, 0x00, 0x00, 0x00, // ES_Descriptor: tag,len, ES_ID(2), flags(0)
        0x04, 0x11, // DecoderConfigDescriptor: tag,len
        0x40, // objectTypeIndication (AAC)
        0x15, 0, 0, 0, // streamType/bufferSizeDB
        0, 0, 0, 0, // maxBitrate
        0, 0, 0, 0, // avgBitrate
        0x05, 0x02, // DecoderSpecificInfo: tag,len
        0x12, 0x10, // AudioSpecificConfig
    ];

    // Full-length (28+ byte) AudioSampleEntry: the esds ASC must come back.
    let stsd = stsd_with(b"mp4a", &audio_entry(2, 0, &mp4_box(b"esds", &esds)));
    let info = parse_stsd(&stsd).expect("an mp4a entry parses");
    assert!(matches!(info.codec, Codec::Aac));
    assert_eq!(
        info.config,
        Some(asc),
        "a full-length mp4a entry must extract its esds AudioSpecificConfig"
    );

    // A SHORT (< 28 byte) AudioSampleEntry, still AAC: config must be
    // None without ever touching body[28..] — no panic.
    let short_stsd = stsd_with(b"mp4a", &[0u8; 12]);
    let short_info = parse_stsd(&short_stsd).expect("still names a codec");
    assert_eq!(
        short_info.config, None,
        "a short mp4a entry has no room for an esds child box"
    );
}

// Every recognised audio fourcc must map to its own `Codec`; a deleted match arm falls
// through to `_ => return None`, dropping the track.
#[test]
fn parse_stsd_recognises_every_audio_fourcc() {
    let cases: &[(&[u8; 4], Codec)] = &[
        (b"ac-3", Codec::Ac3),
        (b"ec-3", Codec::Ac3Plus),
        (b"mp4a", Codec::Aac),
        (b"dtsc", Codec::Dts),
        (b"dtse", Codec::Dts),
        (b"dtsh", Codec::Dts),
        (b"dtsl", Codec::Dts),
    ];
    for (fourcc, want) in cases {
        let stsd = stsd_with(fourcc, &audio_entry(2, 0, &[]));
        let info = parse_stsd(&stsd).unwrap_or_else(|| {
            panic!(
                "{:?} must be recognised, not fall through to None",
                std::str::from_utf8(*fourcc)
            )
        });
        assert_eq!(
            info.codec,
            *want,
            "{:?} must map to {want:?}",
            std::str::from_utf8(*fourcc)
        );
    }
}

// ── MPEG-4 expandable descriptors (ISO/IEC 14496-1 §8.3.3) ────────────────

// Base-128 varint, max FOUR bytes.
#[test]
fn read_descriptor_len_is_a_four_byte_base_128_varint() {
    let read = |b: &[u8]| {
        let mut pos = 0usize;
        let n = read_descriptor_len(b, &mut pos);
        (n, pos)
    };
    assert_eq!(read(&[0x02]), (2, 1), "a short length is one byte");
    assert_eq!(
        read(&[0x7F]),
        (127, 1),
        "127 is the largest one-byte length"
    );
    assert_eq!(
        read(&[0x81, 0x00]),
        (128, 2),
        "128 continues into a second byte, 7 bits at a time"
    );
    assert_eq!(
        read(&[0x81, 0x80, 0x80, 0x01]),
        ((1usize << 21) | 1, 4),
        "four bytes contribute 7 bits each"
    );
    // A fifth continuation byte is NOT consumed: the encoding is capped at 4.
    let mut pos = 0usize;
    read_descriptor_len(&[0x80, 0x80, 0x80, 0x80, 0x7F], &mut pos);
    assert_eq!(pos, 4, "the walk stops after four bytes, whatever follows");
    // Truncated input stops at the end rather than reading past it.
    assert_eq!(read(&[0x81]), (1, 1), "a dangling continuation just ends");
}

// The optional `ES_Descriptor` fields (§7.2.6.5) are selected by three flag bits; skipping
// them wrongly loses the AAC track's CodecPrivate.
#[test]
fn parse_esds_asc_steps_over_every_optional_es_descriptor_field() {
    let asc = vec![0x12u8, 0x10]; // AAC-LC 44.1 kHz stereo
    let build = |flags: u8, extra: &[u8]| {
        let mut v = vec![0u8, 0, 0, 0]; // FullBox version + flags
        v.push(0x03); // ES_Descriptor
        v.push(0x19); // length (unused by the parser)
        v.extend_from_slice(&[0x00, 0x01]); // ES_ID
        v.push(flags);
        v.extend_from_slice(extra);
        v.push(0x04); // DecoderConfigDescriptor
        v.push(0x11);
        v.push(0x40); // objectTypeIndication = AAC
        v.extend_from_slice(&[0x15, 0, 0, 0]); // streamType + bufferSizeDB
        v.extend_from_slice(&[0, 0, 0, 0]); // maxBitrate
        v.extend_from_slice(&[0, 0, 0, 0]); // avgBitrate
        v.push(0x05); // DecoderSpecificInfo
        v.push(asc.len() as u8);
        v.extend_from_slice(&asc);
        v
    };

    // streamDependenceFlag alone: 2 bytes of dependsOn_ES_ID.
    assert_eq!(
        parse_esds_asc(&build(0x80, &[0xAA, 0xBB])).as_deref(),
        Some(&asc[..]),
        "dependsOn_ES_ID must be stepped over"
    );
    // URL_flag alone: a length byte plus that many URL bytes.
    assert_eq!(
        parse_esds_asc(&build(0x40, b"\x05hello")).as_deref(),
        Some(&asc[..]),
        "URLlength + URLstring must be stepped over"
    );
    // OCRstreamFlag alone: 2 bytes of OCR_ES_Id.
    assert_eq!(
        parse_esds_asc(&build(0x20, &[0xCC, 0xDD])).as_deref(),
        Some(&asc[..]),
        "OCR_ES_Id must be stepped over"
    );
    // All three together, in the order the standard lists them.
    assert_eq!(
        parse_esds_asc(&build(0xE0, b"\xAA\xBB\x03abc\xCC\xDD")).as_deref(),
        Some(&asc[..]),
        "all three optional fields present at once"
    );
}

// A box header is 8 bytes, so a declared `size` below 8 cannot describe a box; taking it at
// face value would slice past the start and panic.
#[test]
fn find_boxes_capped_refuses_a_box_smaller_than_its_own_header() {
    // size = 4, type = 'avcC': shorter than the header that declares it.
    let payload = [0u8, 0, 0, 4, b'a', b'v', b'c', b'C'];
    assert!(
        find_boxes_capped(&payload, b"avcC", 8).is_empty(),
        "a sub-header-size box is not a box"
    );
    assert!(find_box(&payload, b"avcC").is_none());
    // Size 8 exactly IS a box — an empty one — so the bound is not off by one.
    let empty = [0u8, 0, 0, 8, b'a', b'v', b'c', b'C'];
    assert_eq!(
        find_box(&empty, b"avcC"),
        Some(&[][..]),
        "an 8-byte box is a valid empty box"
    );
}

// `find_boxes_capped` stops at exactly `cap` matches, not one past it,
// so a caller never forces the scan further into a crafted payload.
#[test]
fn find_boxes_capped_stops_exactly_at_cap_not_one_past() {
    let mut payload = Vec::new();
    for _ in 0..3 {
        payload.extend_from_slice(&mp4_box(b"test", &[]));
    }
    let out = find_boxes_capped(&payload, b"test", 2);
    assert_eq!(
        out.len(),
        2,
        "three matches exist in the payload; cap=2 must return exactly 2"
    );
}

// The declared box size decodes from four specific bytes; a size with distinct bytes
// catches an index slip a small, mostly-zero size would hide.
#[test]
fn find_boxes_capped_decodes_the_size_field_from_its_own_bytes() {
    let want_size: u32 = 0x0001_0010; // byte0=0x00 byte1=0x01 byte2=0x00 byte3=0x10
    let mut payload = vec![0u8; want_size as usize];
    payload[0..4].copy_from_slice(&want_size.to_be_bytes());
    payload[4..8].copy_from_slice(b"test");
    let out = find_boxes_capped(&payload, b"test", 1);
    assert_eq!(out.len(), 1, "the declared-size box must be found");
    assert_eq!(
        out[0].len() as u32,
        want_size - 8,
        "the matched payload slice's length must match the size field \
             decoded from its own four bytes"
    );
}

/// A crafted 64-bit `largesize` near u64::MAX at a nonzero `pos` must not wrap `pos +
/// box_size` past the length guard: the scan must reject it and return without a release
/// panic (slice) or an infinite loop (advance).
#[test]
fn find_boxes_capped_rejects_a_hostile_largesize_near_u64_max() {
    let mut payload = Vec::new();
    // A first, valid empty box so `pos` is nonzero when the hostile box is
    // reached — that is what makes `pos + box_size` wrap rather than merely
    // overshoot the length.
    payload.extend_from_slice(&mp4_box(b"free", &[]));
    // size32 == 1 selects the 16-byte largesize header; largesize = u64::MAX-3
    // so `pos + box_size` overflows usize on a 64-bit target.
    payload.extend_from_slice(&[0, 0, 0, 1, b't', b'e', b's', b't']);
    payload.extend_from_slice(&(u64::MAX - 3).to_be_bytes());
    // Must terminate without panicking and without matching the hostile box.
    let out = find_boxes_capped(&payload, b"test", 8);
    assert!(
        out.is_empty(),
        "a box whose largesize wraps the position must be rejected, not matched"
    );
    assert!(find_box(&payload, b"test").is_none());
}

// An `stsc` entry naming a `first_chunk` beyond what `stco` declares must be clamped, not
// index `spc` past its length and panic.
#[test]
fn sample_offsets_clamps_an_stsc_run_that_outruns_the_chunk_table() {
    let sizes = [10u32, 20];
    let chunk_offsets = [1000u64, 2000];
    // Three entries, two chunks: entry 0's run would end at chunk 3.
    let stsc = [(1u32, 1u32), (4, 1), (9, 1)];
    let offsets = sample_offsets(&sizes, &chunk_offsets, &stsc);
    assert_eq!(
        offsets,
        vec![1000, 2000],
        "one sample per existing chunk; the out-of-range runs place nothing"
    );
}

// §8.6.6: an edit list may hold several media edits; this frame model can only express a
// constant shift, so it honours the LEADING one, not the last.
#[test]
fn elst_offset_ticks_honours_the_first_media_edit_not_the_last() {
    // Two non-empty edits with different media_time; no empty edit.
    let entries = vec![(1000u64, 500i64, 1i16), (1000, 9000, 1)];
    assert_eq!(
        elst_offset_ticks(&entries, Some(1000), 48_000, 0),
        -500,
        "the leading media edit's trim is the one applied"
    );

    // A leading EMPTY edit still delays, and only the empty edits BEFORE the
    // first media edit count — one that trails a media edit does not.
    let entries = vec![
        (100u64, -1i64, 1i16), // empty: 100 movie ticks of delay
        (1000, 200, 1),        // media edit: trims 200 media ticks
        (5000, -1, 1),         // a LATER empty edit — not a start delay
    ];
    assert_eq!(
        elst_offset_ticks(&entries, Some(1000), 48_000, 0),
        100 * 48 - 200,
        "leading empty edits delay (converted to media ticks); trailing ones do not"
    );
}

// A version-1 `mvhd` carries 64-bit creation/modification times, so its `timescale` sits at
// byte 20 rather than 12 (§8.2.2); writers emit version 1 whenever movie duration doesn't
// fit 32 bits.
#[test]
fn mvhd_timescale_version_1_reads_past_the_64_bit_times() {
    let mut v1 = vec![0u8; 24];
    v1[0] = 1; // version 1
    v1[12..20].copy_from_slice(&0xDEAD_BEEF_CAFE_F00Du64.to_be_bytes()); // mod time
    v1[20..24].copy_from_slice(&90_000u32.to_be_bytes());
    assert_eq!(mvhd_timescale(&v1), Some(90_000), "v1 timescale is at 20");
    assert_eq!(
        mvhd_timescale(&v1[..23]),
        None,
        "a v1 mvhd too short to hold it yields nothing, not a partial read"
    );

    let mut v0 = vec![0u8; 16];
    v0[0] = 0;
    v0[4..12].copy_from_slice(&0xDEAD_BEEF_CAFE_F00Du64.to_be_bytes());
    v0[12..16].copy_from_slice(&600u32.to_be_bytes());
    assert_eq!(mvhd_timescale(&v0), Some(600), "v0 timescale is at 12");
}

// Only the leading empty edits and the FIRST media edit shape the offset, so
// `MAX_ELST_ENTRIES` bounds what a crafted `elst` can allocate.
#[test]
fn parse_elst_entry_count_is_capped() {
    let n = MAX_ELST_ENTRIES + 500;
    let mut p = vec![0u8, 0, 0, 0]; // version 0 + flags
    p.extend_from_slice(&(n as u32).to_be_bytes());
    for i in 0..n {
        p.extend_from_slice(&(i as u32).to_be_bytes()); // segment_duration
        p.extend_from_slice(&0u32.to_be_bytes()); // media_time
        p.extend_from_slice(&1i16.to_be_bytes());
        p.extend_from_slice(&0i16.to_be_bytes());
    }
    let entries = parse_elst(&p);
    assert_eq!(
        entries.len(),
        MAX_ELST_ENTRIES,
        "capped, not truncated short"
    );
    assert_eq!(entries[0].0, 0, "and the entries kept are the LEADING ones");
    assert_eq!(entries[1].0, 1);
}

// ── from_reader: per-track handler routing, PID arithmetic, and the shared
// sample budget across tracks. ─────────────────────────────────────────

/// A minimal-but-complete video `trak` (one H.264 sample), mirroring
/// `audio_trak` but for the `vide` handler and an `avc1` sample entry.
fn video_trak(timescale: u32) -> Vec<u8> {
    let mdhd = {
        let mut p = vec![0u8; 24];
        p[12..16].copy_from_slice(&timescale.to_be_bytes());
        mp4_box(b"mdhd", &p)
    };
    let hdlr = {
        let mut p = vec![0u8; 12];
        p[8..12].copy_from_slice(b"vide");
        mp4_box(b"hdlr", &p)
    };
    let stsd = {
        // VisualSampleEntry's fixed part is 78 bytes; dimensions don't
        // matter for this test, so the body is all zero.
        let entry_body = vec![0u8; 78];
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&mp4_box(b"avc1", &entry_body));
        mp4_box(b"stsd", &p)
    };
    let stsz = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&10u32.to_be_bytes()); // sample_size (fixed) = 10
        p.extend_from_slice(&1u32.to_be_bytes()); // count = 1
        mp4_box(b"stsz", &p)
    };
    let stco = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
        mp4_box(b"stco", &p)
    };
    let stsc = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // count
        p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
        p.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
        p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
        mp4_box(b"stsc", &p)
    };
    let stts = {
        let mut p = Vec::new();
        p.extend_from_slice(&[0, 0, 0, 0]);
        p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        p.extend_from_slice(&1u32.to_be_bytes()); // sample_count
        p.extend_from_slice(&1000u32.to_be_bytes()); // sample_delta
        mp4_box(b"stts", &p)
    };
    let mut stbl = Vec::new();
    stbl.extend_from_slice(&stsd);
    stbl.extend_from_slice(&stsz);
    stbl.extend_from_slice(&stco);
    stbl.extend_from_slice(&stsc);
    stbl.extend_from_slice(&stts);
    let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd);
    mdia.extend_from_slice(&hdlr);
    mdia.extend_from_slice(&minf);
    mp4_box(b"trak", &mp4_box(b"mdia", &mdia))
}

// A `hdlr` of anything other than `vide`/`soun` (e.g. hint/subtitle) must be dropped, not
// folded into the audio branch.
#[test]
fn non_av_handler_track_is_dropped_not_folded_into_audio() {
    use std::io::Cursor;
    let mut trak = audio_trak(48_000);
    let pos = trak
        .windows(4)
        .position(|w| w == b"soun")
        .expect("fixture has a soun handler");
    trak[pos..pos + 4].copy_from_slice(b"hint");
    let moov = mp4_box(b"moov", &trak);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "hint".into());
    assert!(
        rd.is_err(),
        "a hint (non-audio/video) handler must not become a stream; \
             all-dropped → Mp4Invalid"
    );
}

// Per-track PID is `0x1011 + track_idx` (video) / `0x1100 + track_idx` (audio); at
// track_idx==0 a `+`↔`-`/`*` mutant is invisible, so this pushes track_idx past 0.
#[test]
fn per_track_pid_arithmetic_is_exact_past_the_first_track() {
    use crate::disc::Stream as DiscStreamE;
    use std::io::Cursor;

    let mut traks = Vec::new();
    traks.extend_from_slice(&video_trak(25)); // track_idx 0
    traks.extend_from_slice(&video_trak(25)); // track_idx 1
    traks.extend_from_slice(&audio_trak(48_000)); // track_idx 2
    traks.extend_from_slice(&audio_trak(48_000)); // track_idx 3
    let moov = mp4_box(b"moov", &traks);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "pids".into()).unwrap();
    assert_eq!(rd.info().streams.len(), 4);

    let pid = |s: &crate::disc::Stream| -> u16 {
        match s {
            DiscStreamE::Video(v) => v.pid,
            DiscStreamE::Audio(a) => a.pid,
            DiscStreamE::Subtitle(_) => panic!("mp4 from_reader never emits subtitles"),
        }
    };
    assert_eq!(pid(&rd.info().streams[0]), 0x1011, "video track_idx 0");
    assert_eq!(
        pid(&rd.info().streams[1]),
        0x1011 + 1,
        "video track_idx 1 — the boundary + vs - vs * separates on"
    );
    assert_eq!(
        pid(&rd.info().streams[2]),
        0x1100 + 2,
        "audio track_idx 2 (track_idx is a single counter shared by \
             every track, video or audio)"
    );
    assert_eq!(
        pid(&rd.info().streams[3]),
        0x1100 + 3,
        "audio track_idx 3 — the + vs - boundary"
    );
}

// A track whose `stsz` is absent still occupies a `track_idx` slot and must advance the
// counter, else two tracks collide on the same PID.
#[test]
fn track_idx_advances_past_a_sample_less_track() {
    use crate::disc::Stream as DiscStreamE;
    use std::io::Cursor;

    let mut traks = audio_trak_missing(b"stsz"); // track_idx 0: no samples
    traks.extend_from_slice(&audio_trak(48_000)); // track_idx 1: has samples
    let moov = mp4_box(b"moov", &traks);
    let rd = Mp4Reader::from_reader(Cursor::new(moov), "skip-idx".into()).unwrap();
    assert_eq!(
        rd.info().streams.len(),
        2,
        "the sample-less track is still kept as a (silent) stream"
    );
    match &rd.info().streams[1] {
        DiscStreamE::Audio(a) => assert_eq!(
            a.pid,
            0x1100 + 1,
            "the second track's PID must reflect track_idx having \
                 advanced past the sample-less first track"
        ),
        _ => panic!("expected an audio stream"),
    }
}

// The global `sample_budget` is DECREMENTED by each track's real sample count and SHARED
// across tracks; a late track gets only what's left.
#[test]
fn sample_budget_is_shared_and_exhausted_across_tracks() {
    use std::io::Cursor;

    let mut traks = audio_trak_hostile_count(); // track_idx 0
    traks.extend_from_slice(&audio_trak_hostile_count()); // track_idx 1
    let moov = mp4_box(b"moov", &traks);
    let file_len = moov.len() as u64;
    let expected_initial_budget =
        MAX_SAMPLE_COUNT.min((file_len / MIN_FILE_BYTES_PER_SAMPLE) as usize);
    // The fixture's own stsc/stts windows (0xFFFF) must not be the binding
    // constraint, or the budget's effect is untestable.
    assert!(
        expected_initial_budget < 0xFFFF,
        "fixture assumption: the shared byte budget must bind before the \
             per-track stsc/stts window does"
    );

    let rd = Mp4Reader::from_reader(Cursor::new(moov), "budget".into()).unwrap();
    assert_eq!(
        rd.samples.len(),
        expected_initial_budget,
        "the first track alone must exhaust the whole initial budget"
    );
    assert_eq!(
        rd.samples.iter().filter(|s| s.track == 1).count(),
        0,
        "the second track must be left with zero budget, not a grown or \
             partially-decremented one"
    );
}

#[test]
fn dropped_track_does_not_consume_the_shared_sample_budget() {
    use std::io::Cursor;
    // Track 0: lying stsz count, stco renamed away so it is dropped.
    let mut bad = audio_trak_hostile_count();
    let pos = bad.windows(4).position(|w| w == b"stco").unwrap();
    bad[pos..pos + 4].copy_from_slice(b"free");
    let mut traks = bad;
    traks.extend_from_slice(&audio_trak(48_000));
    let rd = Mp4Reader::from_reader(Cursor::new(mp4_box(b"moov", &traks)), "b".into()).unwrap();
    assert_eq!(rd.info().streams.len(), 1, "the stco-less track is dropped");
    assert_eq!(rd.samples.len(), 1, "the valid track must keep its sample");
}

#[test]
fn truncated_largesize_is_mp4_invalid_not_eof() {
    let mut f = 1u32.to_be_bytes().to_vec();
    f.extend_from_slice(b"moov");
    f.extend_from_slice(&[0, 0, 0, 0]); // only 4 of the 8 largesize bytes
    let err = read_moov(&mut std::io::Cursor::new(f)).unwrap_err();
    assert!(
        err.to_string()
            .starts_with(&format!("E{}", crate::error::E_MP4_INVALID)),
        "got {err}"
    );
}

#[test]
fn mp4a_with_mpeg_audio_object_type_is_not_labelled_aac() {
    let mut esds = vec![0u8, 0, 0, 0, 0x03, 19, 0, 1, 0, 0x04, 13, 0x6B, 0x15];
    esds.extend_from_slice(&[0u8; 11]);
    let entry = audio_entry(2, 0, &mp4_box(b"esds", &esds));
    let info = parse_stsd(&stsd_with(b"mp4a", &entry)).unwrap();
    assert_eq!(info.codec, Codec::Mp3);
    assert!(info.config.is_none());
}

// A file whose first box is a `free` box holding `sample`, then `moov` whose single
// audio sample points at it.
fn mp4_with_first_sample(mut moov: Vec<u8>, sample: &[u8; 10]) -> Vec<u8> {
    let pos = moov.windows(4).position(|w| w == b"stco").unwrap();
    moov[pos + 12..pos + 16].copy_from_slice(&8u32.to_be_bytes());
    let mut f = mp4_box(b"free", sample);
    f.extend_from_slice(&moov);
    f
}

#[test]
fn mpeg_audio_layer_bits_pick_mp2_or_mp3() {
    let mut esds = vec![0u8, 0, 0, 0, 0x03, 19, 0, 1, 0, 0x04, 13, 0x6B, 0x15];
    esds.extend_from_slice(&[0u8; 11]);
    let entry = audio_entry(2, 0, &mp4_box(b"esds", &esds));
    let codec_for = |b1: u8| {
        let moov = audio_moov(48_000, b"mp4a", &entry);
        let mut s = [0u8; 10];
        s[..2].copy_from_slice(&[0xFF, b1]);
        let f = mp4_with_first_sample(moov, &s);
        read_audio(f).codec
    };
    assert_eq!(codec_for(0xFD), Codec::Mp2, "layer bits 10 = Layer II");
    assert_eq!(codec_for(0xFB), Codec::Mp3, "layer bits 01 = Layer III");
    assert_eq!(codec_for(0x00), Codec::Mp3, "no sync: keep the OTI label");
}

#[test]
fn ac3_and_ec3_channels_come_from_dac3_dec3_not_channelcount() {
    // dac3: acmod 7 (3/2), lfeon 1 -> 6 channels.
    let dac3 = mp4_box(b"dac3", &[0x10, (7 << 3) | (1 << 2), 0]);
    let entry = audio_entry(2, 0, &dac3);
    let info = parse_stsd(&stsd_with(b"ac-3", &entry)).unwrap();
    assert_eq!(info.channels, 6);
    // dec3: acmod 7, lfeon 1, one dependent substream with chan_loc Lrs/Rrs -> 8.
    let dec3 = mp4_box(b"dec3", &[0, 0, 0, (7 << 1) | 1, (1 << 1), 0x80]);
    let entry = audio_entry(2, 0, &dec3);
    let info = parse_stsd(&stsd_with(b"ec-3", &entry)).unwrap();
    assert_eq!(info.channels, 8);
    // Without dec3 the entry ChannelCount is the fallback.
    let info = parse_stsd(&stsd_with(b"ec-3", &audio_entry(2, 0, &[]))).unwrap();
    assert_eq!(info.channels, 2);
}

#[test]
fn video_entry_colr_nclx_and_dolby_vision_are_read() {
    let mut colr = b"nclx".to_vec();
    for v in [9u16, 16, 9] {
        colr.extend_from_slice(&v.to_be_bytes());
    }
    colr.push(0);
    let entry = visual_entry(3840, 2160, &mp4_box(b"colr", &colr));
    let info = parse_stsd(&stsd_with(b"hvc1", &entry)).unwrap();
    let c = info.cicp.expect("nclx parsed");
    assert_eq!((c.primaries, c.transfer, c.matrix, c.range), (9, 16, 9, 1));
    assert_eq!(hdr_from_cicp(info.cicp, false), HdrFormat::Hdr10);
    assert_eq!(hdr_from_cicp(info.cicp, true), HdrFormat::DolbyVision);
    assert_eq!(color_space_from_primaries(c.primaries), ColorSpace::Bt2020);
    // full_range_flag (bit 7 of the last byte) -> range 2.
    *colr.last_mut().unwrap() = 0x80;
    let entry = visual_entry(3840, 2160, &mp4_box(b"colr", &colr));
    let c = parse_stsd(&stsd_with(b"hvc1", &entry))
        .unwrap()
        .cicp
        .unwrap();
    assert_eq!(c.range, 2);
}

#[test]
fn frame_rate_and_duration_are_derived() {
    assert_eq!(frame_rate_from_stts(24000, &[1001; 5]), FrameRate::F23_976);
    assert_eq!(frame_rate_from_stts(25, &[1; 5]), FrameRate::F25);
    assert_eq!(frame_rate_from_stts(90_000, &[1234; 5]), FrameRate::Unknown);
    // Millisecond timescale: 23.976 / 29.97 arrive as 41/42 and 33/34 ms patterns.
    // 24 frames span 1001 ms (7 x 41 + 17 x 42); 30 frames span 1001 ms (19 x 33 + 11 x 34).
    let d = [vec![41; 7], vec![42; 17]].concat();
    assert_eq!(frame_rate_from_stts(1000, &d), FrameRate::F23_976);
    let d = [vec![33; 19], vec![34; 11]].concat();
    assert_eq!(frame_rate_from_stts(1000, &d), FrameRate::F29_97);
    assert_eq!(frame_rate_from_stts(1000, &[40; 9]), FrameRate::F25);
    // A long final sample must not drag exact 24 fps onto 23.976.
    let mut d = vec![1000u32; 14_400];
    d.push(13_000);
    assert_eq!(frame_rate_from_stts(24000, &d), FrameRate::F24);
    assert_eq!(frame_rate_from_stts(1000, &[41; 9]), FrameRate::Unknown);
    let rd = Mp4Reader::from_reader(
        std::io::Cursor::new(audio_moov(1000, b"ac-3", &audio_entry(6, 0, &[]))),
        "d".into(),
    )
    .unwrap();
    assert!(
        (rd.info().duration_secs - 1.0).abs() < 1e-9,
        "1000 ticks at 1000 Hz"
    );
}
