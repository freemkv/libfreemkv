use super::*;

fn video_mux() -> Mux<Vec<u8>> {
    let spec = StreamSpec {
        stream_id: 0xE0,
        payload: Payload::Plain,
        buffer: 0,
        sparse: false,
        av: true,
    };
    let buf = BufferSpec {
        stream_id: 0xE0,
        scale_1024: true,
        size: 232,
    };
    Mux::new(Vec::new(), vec![spec], vec![buf], Vec::new(), 25_200)
}

fn au(pts: u64, len: usize, mark: usize) -> Au {
    Au {
        pts,
        dts: None,
        mark,
        data: vec![0x55; len],
        lpcm_bits: 0,
    }
}

fn scrs(out: &[u8]) -> Vec<u64> {
    let mut v = Vec::new();
    for i in (0..out.len().saturating_sub(10)).step_by(pack::PACK_BYTES) {
        let h = &out[i..];
        let b = |k: usize| u64::from(h[k]);
        let base = ((b(4) >> 3) & 7) << 30
            | (b(4) & 3) << 28
            | b(5) << 20
            | (b(6) >> 3) << 15
            | (b(6) & 3) << 13
            | b(7) << 5
            | b(8) >> 3;
        v.push(base * 300 + ((b(8) & 3) << 7 | b(9) >> 1));
    }
    v
}

// A far-future timestamp is re-based, not bridged with millions of padding packs.
#[test]
fn a_huge_forward_gap_is_rebased() {
    let mut m = video_mux();
    m.push(0, au(9_000, 100, 0));
    m.push(0, au(9_000 + 2 * 3_600 * 90_000, 100, 0));
    m.finish().unwrap();
    assert_eq!(m.counters().rebased_gaps, 1);
    let out = m.into_writer();
    assert!(out.len() < 1 << 20, "{}", out.len());
    let s = scrs(&out);
    assert!(
        s.windows(2)
            .all(|w| w[1] >= w[0] && w[1] - w[0] <= MAX_SCR_GAP27)
    );
}

// Many 59-minute steps: padding stays a small multiple of the input.
#[test]
fn repeated_huge_gaps_stay_bounded() {
    let mut m = video_mux();
    for k in 0..2_000u64 {
        m.push(0, au(9_000 + k * 59 * 60 * 90_000, 10, 0));
    }
    m.finish().unwrap();
    let out = m.into_writer();
    assert!(out.len() < 8 << 20, "{}", out.len());
}

// Video-only stills 10 s apart are real content: padded to full length, never re-based.
#[test]
fn ten_second_video_stills_keep_their_timing() {
    let mut m = video_mux();
    for k in 0..20u64 {
        m.push(0, au(90_000 + k * 900_000, 8_000, 0));
    }
    m.finish().unwrap();
    let c = m.counters();
    assert_eq!(c.rebased_gaps, 0);
    assert!(c.padding_packs >= 19 * 14, "{}", c.padding_packs);
    let s = scrs(&m.into_writer());
    assert!(s.iter().max().copied().unwrap_or(0) >= 190 * HZ27);
}

// Many gaps under the re-base threshold: the padding budget alone bounds the output.
#[test]
fn many_sub_threshold_gaps_stay_within_the_padding_budget() {
    let mut m = video_mux();
    for k in 0..100u64 {
        m.push(0, au(9_000 + k * 60 * 90_000, 10, 0));
    }
    m.finish().unwrap();
    let c = m.counters();
    assert!(c.rebased_gaps > 0);
    assert!(c.padding_packs * 2_048 <= PAD_FLOOR_BYTES + PAD_RATIO * 1_000);
}

// Over budget, a wait on a written AU's removal is padded, not re-based: no re-base
// can move that removal, so re-basing it would loop forever.
#[test]
fn an_over_budget_removal_wait_still_progresses() {
    let d = 100 * HZ27;
    let mut m = video_mux();
    m.first = None;
    m.entries.push(Entry {
        stream: 0,
        id: u64::MAX,
        dec27: d,
        bytes: 232 * 1_024,
        complete: true,
    });
    m.last_scr = Some(d - HZ27 * 8 / 10);
    m.t = m.last_scr;
    m.counters.padding_packs = 1 << 20;
    m.push(0, au(d / 300 + 3_600, 1_000, 0));
    m.finish().unwrap();
    assert!(scrs(&m.into_writer()).iter().all(|&s| s <= d + HZ27));
}

// A forced EOF drain writes AUs ahead of the lead; a later wait on their removal must
// still re-base and finish, not spin (run on a thread so a regression fails, not hangs).
#[test]
fn a_forced_drain_then_huge_gaps_still_finishes() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut m = video_mux();
        m.push(0, au(9_000, 0, 0));
        for k in 0..3u64 {
            m.push(0, au(12_600 + k * 400 * 90_000, 200_000, 0));
        }
        let _ = tx.send(m.finish().is_ok());
    });
    let done = rx.recv_timeout(std::time::Duration::from_secs(20));
    assert_eq!(done, Ok(true), "the forced drain never finished");
}

// A re-base lands the next SCR at most 0.7 s after the last one (MS-17), whatever
// the alignment of the last SCR.
#[test]
fn a_rebase_never_steps_scr_past_the_limit() {
    for len in (100..4_000).step_by(37) {
        let mut m = video_mux();
        m.push(0, au(9_000, len, 0));
        m.push(0, au(9_000 + 2 * 3_600 * 90_000, 100, 0));
        m.finish().unwrap();
        let s = scrs(&m.into_writer());
        let step = s.windows(2).map(|w| w[1] - w[0]).max().unwrap_or(0);
        assert!(step <= MAX_SCR_GAP27, "len {len}: step {step}");
    }
}

// AUs already written keep their decoding times: a re-base must not let the next AU
// into a buffer they still occupy (MS-16).
#[test]
fn a_rebase_keeps_written_aus_in_the_buffer() {
    let size = 232 * 1_024;
    let mut m = video_mux();
    m.push(0, au(90_000, 200_000, 0));
    m.push(0, au(90_000 + 2 * 3_600 * 90_000, 200_000, 0));
    m.finish().unwrap();
    assert_eq!(m.counters().rebased_gaps, 1);
    let out = m.into_writer();
    let (mut sent, mut a_dec) = (0usize, None);
    for p in out
        .chunks(pack::PACK_BYTES)
        .filter(|p| p.len() == pack::PACK_BYTES)
    {
        let scr = scrs(p)[0];
        let at = pack::PACK_HEADER_BYTES + usize::from(p[13] & 7);
        if p[at + 3] != 0xE0 {
            continue;
        }
        let len = usize::from(u16::from_be_bytes([p[at + 4], p[at + 5]]));
        if a_dec.is_none() && p[at + 7] & 0x80 != 0 {
            let t = &p[at + 9..];
            let pts = (u64::from(t[0]) >> 1 & 7) << 30
                | u64::from(t[1]) << 22
                | (u64::from(t[2]) >> 1) << 15
                | u64::from(t[3]) << 7
                | u64::from(t[4]) >> 1;
            a_dec = Some(pts * 300);
        }
        sent += len - 3 - usize::from(p[at + 8]);
        if a_dec.is_some_and(|d| scr < d) {
            assert!(
                sent <= size,
                "{sent} bytes in a {size}-byte buffer at {scr}"
            );
        }
    }
}

// Nit (r2): the forced EOF pass that lifts the 0.95 s lead is counted, not silent.
#[test]
fn a_forced_eof_drain_is_counted() {
    let mut m = video_mux();
    m.push(0, au(9_000, 0, 0));
    m.push(0, au(12_600, 100, 0));
    m.finish().unwrap();
    assert_eq!(m.counters().forced_eof, 1);
    let mut clean = video_mux();
    clean.push(0, au(9_000, 100, 0));
    clean.finish().unwrap();
    assert_eq!(clean.counters().forced_eof, 0);
}

// What no pack can ever take, even with the waits lifted, is an error at EOF, never a
// silent Ok with the AUs dropped: an LPCM AU shorter than one packing unit.
#[test]
fn eof_with_untakeable_aus_is_an_error() {
    let spec = StreamSpec {
        stream_id: pack::PRIVATE_STREAM_1,
        payload: Payload::Lpcm {
            sub_id: 0xA0,
            channels: 2,
            rate: 48_000,
        },
        buffer: 0,
        sparse: false,
        av: true,
    };
    let buf = BufferSpec {
        stream_id: pack::PRIVATE_STREAM_1,
        scale_1024: true,
        size: 232,
    };
    let mut m = Mux::new(Vec::new(), vec![spec], vec![buf], Vec::new(), 25_200);
    m.push(
        0,
        Au {
            lpcm_bits: 16,
            ..au(9_000, 3, 0)
        },
    );
    let e = m.finish().expect_err("an AU that cannot be packetized");
    assert!(
        e.to_string()
            .contains(&crate::error::Error::MpgUnpacketized.to_string()),
        "{e}"
    );
}

// A declared audio track that never delivers is passed once the others lead it by the
// interleave cap, counted; before the cap the clock waits for it.
#[test]
fn a_silent_audio_track_is_passed_at_the_interleave_cap() {
    let video = StreamSpec {
        stream_id: 0xE0,
        payload: Payload::Plain,
        buffer: 0,
        sparse: false,
        av: true,
    };
    let audio = StreamSpec {
        stream_id: 0xC0,
        buffer: 1,
        ..video.clone()
    };
    let bufs = vec![
        BufferSpec {
            stream_id: 0xE0,
            scale_1024: true,
            size: 232,
        },
        BufferSpec {
            stream_id: 0xC0,
            scale_1024: false,
            size: 128,
        },
    ];
    let mut m = Mux::new(Vec::new(), vec![video, audio], bufs, Vec::new(), 25_200);
    m.push(0, au(90_000, 100, 0));
    for k in 1..=7u64 {
        m.push(0, au(90_000 + k * 90_000, 100, 0));
        m.pump(false).unwrap();
    }
    assert_eq!(m.counters().interleave_cap, 0, "7 s: still waiting");
    assert!(m.writer.is_empty(), "nothing is written while audio lags");
    for k in 8..=12u64 {
        m.push(0, au(90_000 + k * 90_000, 100, 0));
        m.pump(false).unwrap();
    }
    assert_eq!(m.counters().interleave_cap, 1);
    assert!(!m.writer.is_empty());
}

// B1: at EOF nothing is left behind. A zero-length AU used to wedge its stream and
// every AU after it; `finish` returned Ok with them queued.
#[test]
fn eof_never_leaves_aus_behind() {
    let mut m = video_mux();
    m.push(0, au(9_000, 0, 0));
    m.push(0, au(12_600, 100, 0));
    m.finish().unwrap();
    assert!(
        m.streams.iter().all(|s| s.queue.is_empty()),
        "AUs left queued"
    );
}

// B1: a picture start code no PES of a fresh pack can reach (a long sequence header or
// user data ahead of it): the bytes before it go first, without a PTS.
#[test]
fn a_commencement_past_a_packs_reach_still_goes() {
    let mut m = video_mux();
    m.push(0, au(9_000, 5_000, 3_000));
    m.push(0, au(12_600, 100, 0));
    m.finish().unwrap();
    assert!(
        m.streams.iter().all(|s| s.queue.is_empty()),
        "AUs left queued"
    );
}

// D1: a tail that ends short of the picture start never crosses it without a PTS: with
// 494 bytes left and the mark 484 away, the commencing PES cannot fit (cap 480) and the
// tail stops at the mark.
#[test]
fn a_tail_never_crosses_the_picture_start() {
    let mut m = video_mux();
    m.push(0, au(9_000, 5_000, 3_000));
    m.streams[0].first_pes_done = true;
    m.streams[0].queue[0].sent = 3_000 - 484;
    let p = m
        .plan_pes(0, 494, 0)
        .expect("the tail is sent, not stalled");
    assert_eq!((p.len, p.start), (484, None));
}

// An AU's first byte and its commencement byte share one PES: a PES ends before the next
// AU's first byte, never between its sequence header and its picture start code (MS-15).
#[test]
fn a_pes_never_splits_an_au_from_its_picture_start() {
    let mut m = video_mux();
    m.push(0, au(9_000, 100, 0));
    m.push(0, au(12_600, 1_000, 20));
    let p = m.plan_pes(0, 200, 0).unwrap();
    assert_eq!(p.start.map(|s| s.0), Some(0));
    assert_eq!(
        p.len, 100,
        "stops at the next AU's first byte, not its picture start"
    );
    m.streams[0].queue[0].sent = 90;
    let tail = m.plan_pes(0, 30, 0).unwrap();
    assert_eq!((tail.len, tail.start.map(|s| s.0)), (10, None));
}

// A PES that carries a PTS begins at its AU's first byte, as a DVD encoder writes it: PS
// readers (our AuAssembler) give a PES's PTS to the AU holding its first byte, so a tail
// of the previous AU in front would take the PTS.
#[test]
fn a_pts_pes_begins_at_its_au() {
    let mut m = video_mux();
    m.push(0, au(9_000, 100, 0));
    m.push(0, au(12_600, 1_000, 20));
    m.streams[0].queue[0].sent = 90;
    let p = m.plan_pes(0, 1_000, 0).unwrap();
    assert_eq!((p.len, p.start), (10, None), "the tail goes alone");
}

// An audio frame that fits one PES goes whole: PS readers time a frame by the PES its
// first byte is in, and some (our Ac3Parser) only when it completes there.
#[test]
fn an_audio_frame_that_fits_goes_whole() {
    let spec = StreamSpec {
        stream_id: 0xBD,
        payload: Payload::Frames { sub_id: 0x80 },
        buffer: 0,
        sparse: false,
        av: true,
    };
    let buf = BufferSpec {
        stream_id: 0xBD,
        scale_1024: true,
        size: 8191,
    };
    let mut m = Mux::new(Vec::new(), vec![spec], vec![buf], Vec::new(), 25_200);
    m.push(0, au(9_000, 1_792, 0));
    assert!(
        m.plan_pes(0, 500, 0).is_none(),
        "no 480-byte start: wait for a pack"
    );
    assert_eq!(m.plan_pes(0, 2_034, 0).map(|p| p.len), Some(1_792));
    let mut big = Mux::new(
        Vec::new(),
        vec![m.streams[0].spec.clone()],
        vec![buf],
        Vec::new(),
        25_200,
    );
    big.push(0, au(9_000, 2_560, 0));
    assert!(
        big.plan_pes(0, 2_034, 0).is_some_and(|p| p.len < 2_560),
        "a frame no PES holds spans"
    );
}
