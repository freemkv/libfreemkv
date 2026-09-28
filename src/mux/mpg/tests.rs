//! `mpg://` sink tests (mpg-output-design v5 §7): synthetic DVD-like titles through the
//! sink, then an independent parse and P-STD replay, ES and PTS round trips, and a read
//! back through the crate's own program stream demuxer.

use super::replay::{self, Aus, Key, Parsed};
use super::*;
use crate::disc::{
    AudioChannels, AudioStream, ColorSpace, FrameRate, HdrFormat, LabelPurpose, LabelQualifier,
    Resolution, SampleRate, SubtitleStream, VideoStream,
};
use crate::mux::codec::CodecParser;
use crate::mux::decode_ts::test_es;
use std::collections::BTreeMap;

const MS: i64 = 1_000_000;

// A 13818-2 sequence header (4:3, bit_rate 0x3FFFF) with its sequence_extension (MP@ML).
fn seq_header(w: u16, h: u16, frc: u8, vbv: u16, low_delay: bool) -> Vec<u8> {
    let bits: u64 = (u64::from(w) << 52)
        | (u64::from(h) << 40)
        | (2 << 36)
        | (u64::from(frc) << 32)
        | (0x3FFFF << 14)
        | (1 << 13)
        | (u64::from(vbv & 0x3FF) << 3);
    let mut v = vec![0, 0, 1, 0xB3];
    v.extend_from_slice(&bits.to_be_bytes());
    // extension id 1, profile_and_level 0x48, progressive 0, 4:2:0, no size/rate
    // extensions, marker, vbv extension 0, low_delay, frame rate extensions 0.
    v.extend_from_slice(&[
        0,
        0,
        1,
        0xB5,
        0x14,
        0x82,
        0x00,
        0x01,
        0x00,
        u8::from(low_delay) << 7,
    ]);
    v
}

fn gop_header() -> Vec<u8> {
    vec![0, 0, 1, 0xB8, 0x00, 0x08, 0x00, 0x00]
}

#[derive(Clone)]
struct Opts {
    secs: i64,
    audio_lead_ms: i64,
    /// Display milliseconds with no video (a damage gap), `(from, len)`.
    gap: Option<(i64, i64)>,
    i_size: usize,
    spu_tracks: usize,
    lpcm: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            secs: 6,
            audio_lead_ms: 0,
            gap: None,
            i_size: 40_000,
            spu_tracks: 1,
            lpcm: true,
        }
    }
}

struct Fx {
    title: DiscTitle,
    frames: Vec<PesFrame>,
    /// Input ES per track, frame by frame.
    input: BTreeMap<usize, Vec<(i64, Vec<u8>)>>,
    aus: Aus,
}

const VIDEO_START_NS: i64 = 500 * MS;

fn fixture(o: &Opts) -> Fx {
    let mut streams = vec![DiscStream::Video(VideoStream {
        pid: 0xE0,
        codec: Codec::Mpeg2,
        resolution: Resolution::R576i,
        frame_rate: FrameRate::F25,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt470bg,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    let audio = |pid: u16, codec: Codec, lang: &str, label: &str| {
        DiscStream::Audio(AudioStream {
            pid,
            codec,
            channels: AudioChannels::Stereo,
            language: lang.into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: label.into(),
        })
    };
    streams.push(audio(0x00C0, Codec::Mp2, "eng", ""));
    streams.push(audio(
        0x00D0,
        Codec::Mp2,
        "eng",
        crate::disc::MP2_EXTENSION_LABEL,
    ));
    streams.push(audio(0xBD80, Codec::Ac3, "fra", ""));
    streams.push(audio(0xBD88, Codec::Dts, "deu", ""));
    if o.lpcm {
        streams.push(audio(0xBDA0, Codec::Lpcm, "spa", ""));
        streams.push(audio(0xBDA1, Codec::Lpcm, "ita", ""));
    }
    let palette = crate::mux::codec::dvdsub::format_palette(&[[0, 0x80, 0x80, 0x80]; 16], 720, 576);
    for k in 0..o.spu_tracks {
        streams.push(DiscStream::Subtitle(SubtitleStream {
            pid: 0x20 + k as u16,
            codec: Codec::DvdSub,
            language: if k == 0 { "eng".into() } else { "nld".into() },
            forced: k == 0,
            qualifier: LabelQualifier::None,
            codec_data: Some(palette.clone()),
        }));
    }
    let title = DiscTitle {
        codec_privates: vec![None; streams.len()],
        streams,
        ..DiscTitle::empty()
    };
    let mut ev: Vec<(i64, usize, PesFrame)> = Vec::new();
    let mut input: BTreeMap<usize, Vec<(i64, Vec<u8>)>> = BTreeMap::new();
    let mut aus: Aus = BTreeMap::new();
    let mut add = |ev: &mut Vec<(i64, usize, PesFrame)>,
                   key_ns: i64,
                   track: usize,
                   pts: i64,
                   key: bool,
                   data: Vec<u8>,
                   k: Option<(Key, usize)>| {
        input.entry(track).or_default().push((pts, data.clone()));
        if let Some((kk, mark)) = k {
            aus.entry(kk).or_default().push((data.len(), mark));
        }
        let seq = ev.len();
        let _ = seq;
        ev.push((
            key_ns,
            track,
            PesFrame {
                track,
                pts,
                keyframe: key,
                data,
                duration_ns: None,
                discard_padding_ns: 0,
                source: None,
                coding: None,
            },
        ));
    };
    // Video: closed GOPs of 10, decode I0 P3 B1 B2 P6 B4 B5 P9 B7 B8, 25 fps.
    let order: [(usize, u8); 10] = [
        (0, 1),
        (3, 2),
        (1, 3),
        (2, 3),
        (6, 2),
        (4, 3),
        (5, 3),
        (9, 2),
        (7, 3),
        (8, 3),
    ];
    let gops = o.secs * 25 / 10;
    for g in 0..gops {
        let display0 = g * 10;
        let t0 = display0 * 40;
        if o.gap
            .is_some_and(|(from, len)| t0 + 400 > from && t0 < from + len)
        {
            continue;
        }
        for (k, &(d, coding)) in order.iter().enumerate() {
            let mut data = Vec::new();
            if k == 0 {
                data.extend(seq_header(720, 576, 3, 112, false));
                data.extend(gop_header());
            }
            let mark = data.len();
            data.extend(test_es::mpeg2_pic(coding, 3));
            let size = match coding {
                1 => o.i_size,
                2 => 15_000,
                _ => 6_000,
            };
            data.resize(size.max(data.len()), 0x55);
            let pts = VIDEO_START_NS + (display0 + d as i64) * 40 * MS;
            let arrive = VIDEO_START_NS + (display0 + k as i64 - 1) * 40 * MS;
            add(
                &mut ev,
                arrive,
                0,
                pts,
                coding == 1,
                data,
                Some(((0xE0, None), mark)),
            );
        }
    }
    let a0 = VIDEO_START_NS - o.audio_lead_ms * MS;
    let end = VIDEO_START_NS + o.secs * 1_000 * MS;
    // MPEG-1 Layer II, 24 ms, and its 13818-3 extension frame with the same PTS (MS-20).
    let mut t = a0;
    while t < end {
        let mut f = vec![0xFF, 0xFD, 0x94, 0x00];
        f.resize(768, 0x33);
        add(&mut ev, t, 1, t, true, f, Some(((0xC0, None), 0)));
        let mut x = vec![0x7F, 0xF0, 0x12, 0x34];
        x.resize(256, 0x44);
        add(&mut ev, t, 2, t, true, x, Some(((0xD0, None), 0)));
        t += 24 * MS;
    }
    // AC-3 32 ms / 1792 B; DTS 512 samples / 2012 B.
    let mut k = 0i64;
    while a0 + k * 32 * MS < end {
        let mut f = vec![0x0B, 0x77];
        f.resize(1_792, 0x66);
        let t = a0 + k * 32 * MS;
        add(&mut ev, t, 3, t, true, f, Some(((0xBD, Some(0x80)), 0)));
        k += 1;
    }
    let mut k = 0i64;
    while a0 + k * 512 * 1_000_000_000 / 48_000 < end {
        let mut f = vec![0x7F, 0xFE, 0x80, 0x01];
        f.resize(2_012, 0x77);
        let t = a0 + k * 512 * 1_000_000_000 / 48_000;
        add(&mut ev, t, 4, t, true, f, Some(((0xBD, Some(0x88)), 0)));
        k += 1;
    }
    let mut next = 5;
    if o.lpcm {
        // LPCM IR: 10 ms of 24-bit stereo; track 5 from a 16-bit source, 6 truly 24-bit.
        let mut k = 0i64;
        while a0 + k * 10 * MS < end {
            let t = a0 + k * 10 * MS;
            for (track, low) in [(5, 0u8), (6, 0x21u8)] {
                let mut f = Vec::with_capacity(480 * 6);
                for s in 0..960u32 {
                    f.extend_from_slice(&[(s >> 3) as u8, (s * 7) as u8, low]);
                }
                add(&mut ev, t, track, t, true, f, None);
            }
            k += 1;
        }
        next = 7;
    }
    for sp in 0..o.spu_tracks {
        let mut t = VIDEO_START_NS + 200 * MS + sp as i64 * 7 * MS;
        while t < end {
            let mut f = vec![0x0B, 0xB8];
            f.resize(3_000, 0x11);
            add(
                &mut ev,
                t,
                next + sp,
                t,
                true,
                f,
                Some(((0xBD, Some(0x20 + sp as u8)), 0)),
            );
            t += 2_000 * MS;
        }
    }
    ev.sort_by_key(|e| (e.0, e.1));
    Fx {
        title,
        frames: ev.into_iter().map(|e| e.2).collect(),
        input,
        aus,
    }
}

struct Run {
    out: Vec<u8>,
    counters: MpgCounters,
    parsed: Parsed,
}

fn run(fx: &Fx) -> Run {
    let mut sink = MpgSink::create(Vec::new(), &fx.title).expect("create");
    for f in &fx.frames {
        sink.write(f).expect("write");
    }
    sink.finish().expect("finish");
    let counters = sink.counters();
    let out = sink.mux.take().expect("mux").into_writer();
    let parsed = replay::parse(&out).expect("parses as 2048-byte packs");
    Run {
        out,
        counters,
        parsed,
    }
}

fn es_of(p: &Parsed, key: Key) -> Vec<u8> {
    p.pes
        .iter()
        .filter(|x| x.key == key)
        .flat_map(|x| x.es.iter().copied())
        .collect()
}

// The replay finds exactly what the sink counted, never more (design §7).
fn assert_replays(r: &Run, fx: &Fx) {
    let found = replay::replay(&r.parsed, &fx.aus).unwrap_or_else(|e| panic!("replay: {e}"));
    assert!(
        found.late_aus <= r.counters.pstd.late_aus,
        "{found:?} vs {:?}",
        r.counters
    );
    assert!(
        found.pts_gaps <= r.counters.pstd.pts_gaps,
        "{found:?} vs {:?}",
        r.counters
    );
}

// B1: an empty frame (a Matroska empty Block) is no AU: it never wedges its stream, and
// every frame after it is written.
#[test]
fn an_empty_frame_never_wedges_its_stream() {
    let mut fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    // An empty AC-3 frame, and an empty video frame after the first keyframe.
    for (track, key) in [(3, true), (0, false)] {
        let at = fx
            .frames
            .iter()
            .position(|f| f.track == track && f.keyframe == key)
            .unwrap();
        let mut e = fx.frames[at].clone();
        e.data.clear();
        fx.frames.insert(at + 1, e);
    }
    let r = run(&fx);
    for (track, key) in [(0usize, (0xE0u8, None)), (3, (0xBD, Some(0x80u8)))] {
        let want: Vec<u8> = fx.input[&track]
            .iter()
            .flat_map(|f| f.1.iter().copied())
            .collect();
        assert_eq!(es_of(&r.parsed, key).len(), want.len(), "{key:?} ES bytes");
    }
    assert_replays(&r, &fx);
}

// B1: user data ahead of the picture start code puts the commencement byte past any PES of
// a fresh pack; the AU is still written, its PTS in the PES that holds the picture start.
#[test]
fn a_picture_start_past_one_pack_is_written() {
    let mut fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    for f in fx.frames.iter_mut().filter(|f| f.track == 0 && f.keyframe) {
        let at = picture_start(&f.data);
        let mut user = vec![0, 0, 1, 0xB2];
        user.resize(3_000, 0x5A);
        f.data.splice(at..at, user);
    }
    let fx = rebuild_aus(fx);
    let r = run(&fx);
    let want: Vec<u8> = fx.input[&0]
        .iter()
        .flat_map(|f| f.1.iter().copied())
        .collect();
    assert_eq!(es_of(&r.parsed, (0xE0, None)), want, "video ES bytes");
    assert_replays(&r, &fx);
}

// Design §2.3 pack fill: 1-5 spare bytes go as PES-header stuffing on the last PES (MS-13
// "No more than 32 stuffing bytes"), never pack stuffing: a stuffed pack header moves byte
// 0x14 off the first PES's flags, where CSS scrambling detection reads a VOB pack.
#[test]
fn no_pack_header_carries_stuffing() {
    for o in [
        Opts::default(),
        Opts {
            spu_tracks: 12,
            ..Opts::default()
        },
        Opts {
            i_size: 180_000,
            ..Opts::default()
        },
    ] {
        let fx = fixture(&o);
        let r = run(&fx);
        assert_replays(&r, &fx);
        for pk in r.out.as_chunks::<{ pack::PACK_BYTES }>().0 {
            assert_eq!(pk[13] & 7, 0, "pack_stuffing_length");
            assert!(
                !crate::css::is_scrambled_pack(pk),
                "a clear pack reads as scrambled"
            );
        }
    }
}

#[test]
fn a_dvd_like_title_replays_clean_with_nothing_counted() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    assert_replays(&r, &fx);
    assert_eq!(
        r.counters,
        MpgCounters::default(),
        "a clean title counts nothing"
    );
    assert!(r.out.len().is_multiple_of(pack::PACK_BYTES) || r.out.ends_with(&pack::PROGRAM_END));
}

// J16/§2.3: ES bytes kept, PTS exact modulo one integer-tick origin.
#[test]
fn es_bytes_and_pts_round_trip_exactly() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    let keys: [(usize, Key); 6] = [
        (0, (0xE0, None)),
        (1, (0xC0, None)),
        (2, (0xD0, None)),
        (3, (0xBD, Some(0x80))),
        (4, (0xBD, Some(0x88))),
        (7, (0xBD, Some(0x20))),
    ];
    let mut offset = None;
    for (track, key) in keys {
        let want: Vec<u8> = fx.input[&track]
            .iter()
            .flat_map(|f| f.1.iter().copied())
            .collect();
        assert_eq!(es_of(&r.parsed, key), want, "{key:?} ES bytes");
        let pts: Vec<u64> = r
            .parsed
            .pes
            .iter()
            .filter(|x| x.key == key)
            .filter_map(|x| x.pts)
            .collect();
        let mut inp: Vec<i64> = fx.input[&track].iter().map(|f| ns_to_ticks(f.0)).collect();
        if track == 0 {
            // Video PTS arrive in decode order, as written.
            assert_eq!(pts.len(), inp.len());
        }
        inp.truncate(pts.len());
        for (o, i) in pts.iter().zip(&inp) {
            let d = *o as i64 - i;
            assert_eq!(
                *offset.get_or_insert(d),
                d,
                "{key:?}: one constant tick offset"
            );
        }
    }
    // LPCM: the DVD re-pack decodes back to the IR exactly (G8).
    for (track, sub) in [(5usize, 0xA0u8), (6, 0xA1)] {
        let mut parser = crate::mux::codec::lpcm::LpcmParser::new_dvd();
        let mut ir = Vec::new();
        for x in r.parsed.pes.iter().filter(|x| x.key == (0xBD, Some(sub))) {
            let mut data = x.sub_hdr[4..7].to_vec();
            data.extend_from_slice(&x.es);
            let pes = crate::mux::ts::PesPacket {
                source: None,
                pid: 0,
                pts: x.pts.map(|p| p as i64),
                dts: None,
                data,
                discontinuity: false,
            };
            for f in parser.parse(&pes) {
                ir.extend(f.data);
            }
        }
        let want: Vec<u8> = fx.input[&track]
            .iter()
            .flat_map(|f| f.1.iter().copied())
            .collect();
        assert_eq!(ir, want, "LPCM {sub:#x} round trip");
    }
}

// MS-19 §2.7.5 guard: "A decoding_timestamp (DTS) shall appear … if and only if … the
// decoding time differs from the presentation time." IBBP: I and P carry DTS, B PTS only.
#[test]
fn dts_appears_iff_it_differs_from_the_pts() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    let video: Vec<&replay::Pes> = r
        .parsed
        .pes
        .iter()
        .filter(|x| x.key.0 == 0xE0 && x.pts.is_some())
        .collect();
    for (k, x) in video.iter().enumerate() {
        let coding = [1, 2, 3, 3, 2, 3, 3, 2, 3, 3][k % 10];
        assert_eq!(
            x.dts.is_some(),
            coding != 3,
            "picture {k} (coding type {coding})"
        );
    }
    for x in r.parsed.pes.iter().filter(|x| x.key.0 != 0xE0) {
        assert!(x.dts.is_none(), "audio and subpictures decode at their PTS");
    }
}

// MS-20 §2.7.6 guard: "corresponding decoding/presentation units in the two layers shall have
// identical PTS values"; MPG4-7: each extension frame is exactly one whole PES.
#[test]
fn the_extension_shares_its_base_pts_one_frame_per_pes() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    let base: Vec<u64> = r
        .parsed
        .pes
        .iter()
        .filter(|x| x.key.0 == 0xC0)
        .filter_map(|x| x.pts)
        .collect();
    let ext: Vec<&replay::Pes> = r.parsed.pes.iter().filter(|x| x.key.0 == 0xD0).collect();
    assert_eq!(ext.len(), fx.input[&2].len(), "one PES per extension frame");
    for (x, b) in ext.iter().zip(&base) {
        assert_eq!(x.pts, Some(*b));
        assert_eq!(x.es.len(), 256);
    }
}

// MPG4-6: "each LPCM PES carries whole sample groups".
#[test]
fn every_lpcm_pes_holds_whole_sample_groups() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    for (sub, bits) in [(0xA0u8, 16u8), (0xA1, 24)] {
        let unit = super::pstd::lpcm_unit_for_test(2, bits);
        for x in r.parsed.pes.iter().filter(|x| x.key == (0xBD, Some(sub))) {
            assert_eq!(x.es.len() % unit, 0, "LPCM {sub:#x}: {} B", x.es.len());
            assert_eq!(
                x.sub_hdr[5] >> 6,
                (bits - 16) / 4,
                "quantization in the header"
            );
        }
    }
}

// MS-22/MS-23/MS-24 and design §2.2: the map's entries, the FMKV table and palette.
#[test]
fn the_stream_map_describes_every_stream() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    let m = &r.parsed.psms[0].1;
    let info_len = usize::from(u16::from_be_bytes([m[8], m[9]]));
    let info = &m[10..10 + info_len];
    let es_at = 10 + info_len + 2;
    let mut entries = Vec::new();
    let mut i = es_at;
    while i < m.len() - 4 {
        let n = usize::from(u16::from_be_bytes([m[i + 2], m[i + 3]]));
        entries.push((m[i], m[i + 1], m[i + 4..i + 4 + n].to_vec()));
        i += 4 + n;
    }
    assert_eq!(entries[0], (0x02, 0xE0, vec![]));
    assert_eq!(
        entries[1].0, 0x04,
        "a base with its extension is 13818-3 audio"
    );
    assert_eq!(entries[1].1, 0xC0);
    assert_eq!(&entries[1].2[..6], &[10, 4, b'e', b'n', b'g', 0]);
    assert_eq!(
        &entries[1].2[6..],
        &pack::hierarchy_descriptor(pack::HIERARCHY_BASE, 0, 0)
    );
    assert_eq!(
        entries[2],
        (
            0x04,
            0xD0,
            pack::hierarchy_descriptor(pack::HIERARCHY_EXTENSION, 1, 0).to_vec()
        )
    );
    assert_eq!(entries[3], (0x06, 0xBD, vec![]));
    assert_eq!(entries.len(), 4);
    assert_eq!(info[0], pack::FMKV_TAG);
    let rows = &info[8..2 + usize::from(info[1])];
    let table: Vec<(u8, [u8; 3], u8)> = rows
        .chunks(5)
        .map(|r| (r[0], [r[1], r[2], r[3]], r[4]))
        .collect();
    assert_eq!(
        table,
        vec![
            (0x80, *b"fra", 0),
            (0x88, *b"deu", 0),
            (0xA0, *b"spa", 0),
            (0xA1, *b"ita", 0),
            (0x20, *b"eng", 1)
        ]
    );
    let pal = &info[2 + usize::from(info[1])..];
    assert_eq!(pal[0], pack::FMKV_TAG);
    assert_eq!(pal[7], pack::FMKV_PALETTE);
    assert_eq!(pal.len(), 2 + 6 + 48);
}

// Design §0: "No carriable video → MpgNoVideoTrack (E9074)"; J24: H.264 is not carriable yet.
#[test]
fn no_carriable_video_is_e9074() {
    let mut fx = fixture(&Opts::default());
    let e = MpgSink::create(
        Vec::new(),
        &DiscTitle {
            streams: fx.title.streams[1..].to_vec(),
            ..DiscTitle::empty()
        },
    )
    .err()
    .expect("audio only");
    assert_eq!(
        crate::error::error_code(&e),
        Some(crate::error::E_MPG_NO_VIDEO_TRACK)
    );
    if let DiscStream::Video(v) = &mut fx.title.streams[0] {
        v.codec = Codec::H264;
    }
    let e = MpgSink::create(Vec::new(), &fx.title)
        .err()
        .expect("H.264 only");
    assert_eq!(
        crate::error::error_code(&e),
        Some(crate::error::E_MPG_NO_VIDEO_TRACK)
    );
}

// MS-17 §2.7.1 guard: a damage gap over 0.7 s is bridged by padding packs; the PTS gap
// itself is the content's, counted (MPG2-11).
#[test]
fn a_damage_gap_is_bridged_with_padding_packs() {
    let fx = fixture(&Opts {
        gap: Some((2_000, 2_000)),
        lpcm: false,
        ..Opts::default()
    });
    // Audio keeps flowing through the gap here, so blank the audio too for a real silence.
    let fx = Fx {
        frames: fx
            .frames
            .into_iter()
            .filter(|f| !(f.pts > 2_100 * MS && f.pts < 3_900 * MS))
            .collect(),
        ..fx
    };
    let fx = rebuild_aus(fx);
    let r = run(&fx);
    assert_replays(&r, &fx);
    assert!(r.counters.pstd.padding_packs > 0, "{:?}", r.counters);
    assert!(r.counters.pstd.pts_gaps > 0);
}

// Recompute the fixture's AU tables after frames were filtered out.
fn rebuild_aus(mut fx: Fx) -> Fx {
    let mut aus: Aus = BTreeMap::new();
    let mut input: BTreeMap<usize, Vec<(i64, Vec<u8>)>> = BTreeMap::new();
    for f in &fx.frames {
        input
            .entry(f.track)
            .or_default()
            .push((f.pts, f.data.clone()));
        let key = match f.track {
            0 => Some(((0xE0, None), picture_start(&f.data))),
            1 => Some(((0xC0, None), 0)),
            2 => Some(((0xD0, None), 0)),
            3 => Some(((0xBD, Some(0x80)), 0)),
            4 => Some(((0xBD, Some(0x88)), 0)),
            t if matches!(fx.title.streams[t], DiscStream::Subtitle(_)) => {
                let DiscStream::Subtitle(s) = &fx.title.streams[t] else {
                    unreachable!()
                };
                Some(((0xBD, Some(s.pid as u8)), 0))
            }
            _ => None,
        };
        if let Some((k, mark)) = key {
            aus.entry(k).or_default().push((f.data.len(), mark));
        }
    }
    fx.aus = aus;
    fx.input = input;
    fx
}

// Design §2.3 (MPG3-8): the origin is the lowest first timestamp over ALL tracks; audio 2 s
// ahead of video maps to 1.5 s and the video keeps its offset.
#[test]
fn audio_leading_video_sets_the_origin() {
    let fx = fixture(&Opts {
        audio_lead_ms: 2_000,
        ..Opts::default()
    });
    let r = run(&fx);
    assert_replays(&r, &fx);
    let first_audio = r
        .parsed
        .pes
        .iter()
        .find(|x| x.key.0 == 0xC0)
        .and_then(|x| x.pts);
    assert_eq!(first_audio, Some(ORIGIN_TICKS as u64));
    let first_video = r
        .parsed
        .pes
        .iter()
        .find(|x| x.key.0 == 0xE0)
        .and_then(|x| x.pts);
    assert_eq!(first_video, Some(ORIGIN_TICKS as u64 + 2 * 90_000));
}

// Design §2.4 (MPG2-6d): many subpictures with coincident PTS share the one 0xBD buffer.
#[test]
fn many_subpicture_streams_share_the_private_buffer() {
    let fx = fixture(&Opts {
        spu_tracks: 12,
        ..Opts::default()
    });
    let r = run(&fx);
    assert_replays(&r, &fx);
}

// A high-rate burst: 250 KB I pictures raise the per-pack rate, never the delay past 1 s.
#[test]
fn a_high_rate_burst_raises_the_pack_rate() {
    let fx = fixture(&Opts {
        i_size: 180_000,
        ..Opts::default()
    });
    let r = run(&fx);
    assert_replays(&r, &fx);
    assert!(
        r.parsed.packs.iter().any(|p| p.rate > R0_SD),
        "the rate rose above R0"
    );
}

// The crate's own demuxer reads the output back, stream for stream.
#[test]
fn the_ps_demuxer_reads_the_output_back() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    let mut d = crate::mux::ps::PsDemuxer::new();
    let mut got: BTreeMap<(u8, Option<u8>), Vec<u8>> = BTreeMap::new();
    for p in d.feed(&r.out).into_iter().chain(d.flush()) {
        got.entry((p.stream_id, p.sub_stream_id))
            .or_default()
            .extend(p.data);
    }
    for (track, key) in [
        (0usize, (0xE0u8, None)),
        (1, (0xC0, None)),
        (2, (0xD0, None)),
        (3, (0xBD, Some(0x80u8))),
        (7, (0xBD, Some(0x20))),
    ] {
        let want: Vec<u8> = fx.input[&track]
            .iter()
            .flat_map(|f| f.1.iter().copied())
            .collect();
        assert_eq!(got.get(&key), Some(&want), "{key:?}");
    }
}

// Design §2.3 EOF: "A short input with ≤ R units is written, not failed."
#[test]
fn a_short_input_is_written_and_nothing_is_mux_empty() {
    let fx = fixture(&Opts::default());
    let mut sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    sink.write(&fx.frames.iter().find(|f| f.track == 0).unwrap().clone())
        .unwrap();
    sink.finish().unwrap();
    let out = sink.mux.take().unwrap().into_writer();
    let p = replay::parse(&out).unwrap();
    assert_eq!(
        p.pes
            .iter()
            .filter(|x| x.key.0 == 0xE0 && x.pts.is_some())
            .count(),
        1
    );
    let mut empty = MpgSink::create(Vec::new(), &fx.title).unwrap();
    let e = empty.finish().unwrap_err();
    assert_eq!(
        crate::error::error_code(&e),
        Some(crate::error::E_MUX_EMPTY)
    );
}

// Video before the first keyframe is dropped (nothing decodes without it), as tsmux does.
#[test]
fn video_before_the_first_keyframe_is_dropped() {
    let fx = fixture(&Opts::default());
    let mut sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    let mut p = fx
        .frames
        .iter()
        .find(|f| f.track == 0 && !f.keyframe)
        .unwrap()
        .clone();
    p.pts = 0;
    sink.write(&p).unwrap();
    for f in &fx.frames {
        sink.write(f).unwrap();
    }
    sink.finish().unwrap();
    let out = sink.mux.take().unwrap().into_writer();
    let parsed = replay::parse(&out).unwrap();
    let want: Vec<u8> = fx.input[&0]
        .iter()
        .flat_map(|f| f.1.iter().copied())
        .collect();
    assert_eq!(es_of(&parsed, (0xE0, None)), want);
}

// A Matroska-style source keeps the sequence header in codec_private: it is put back ahead
// of the first picture.
#[test]
fn a_sequence_header_from_codec_private_is_restored() {
    let mut fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let seq = seq_header(720, 576, 3, 112, false);
    fx.title.codec_privates[0] = Some(seq.clone());
    for f in fx.frames.iter_mut().filter(|f| f.track == 0 && f.keyframe) {
        let at = picture_start(&f.data);
        f.data.drain(..at);
    }
    let r = run(&fx);
    let video = es_of(&r.parsed, (0xE0, None));
    assert!(video.starts_with(&seq));
}

#[test]
fn mpg_urls_parse_and_input_waits_for_l3() {
    let u = crate::mux::resolve::parse_url("mpg:///out/x.mpg");
    assert_eq!(u.scheme(), "mpg");
    assert_eq!(u.path_str(), "/out/x.mpg");
    let e = crate::mux::resolve::input("mpg:///out/x.mpg", &Default::default())
        .err()
        .unwrap();
    assert_eq!(
        crate::error::error_code(&e),
        Some(crate::error::E_STREAM_WRITE_ONLY)
    );
}

#[test]
fn the_generic_fit_report_is_the_mpg_plan() {
    let fx = fixture(&Opts::default());
    let r = crate::mux::fit::fit_report(&crate::mux::resolve::parse_url("mpg:///o.mpg"), &fx.title);
    assert_eq!(r.included, (0..fx.title.streams.len()).collect::<Vec<_>>());
    assert!(r.skipped.is_empty());
}

// §7 unit: vbv_buffer_size and the palette line parse.
#[test]
fn vbv_and_palette_parse() {
    assert_eq!(
        vbv_bytes(&seq_header(720, 576, 3, 112, false)),
        Some(112 * 2048)
    );
    let p = crate::mux::codec::dvdsub::format_palette(&[[0, 0x80, 0x80, 0x80]; 16], 720, 576);
    assert!(idx_palette(&p).is_some());
    assert_eq!(idx_palette(b"palette: 000000"), None);
}

// Design §2.3 (MPG4-1b) and MPG2-11: a slideshow DVD, I at 0 s and P at 5 s with audio between.
// The hold cap releases, the P gets DTS_last + 1 (no duplicate), both counted; the replay
// allows exactly the counted AUs.
#[test]
fn a_slideshow_is_counted_not_refused() {
    let base = fixture(&Opts {
        secs: 6,
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let mut frames: Vec<PesFrame> = base
        .frames
        .iter()
        .filter(|f| f.track != 0)
        .cloned()
        .collect();
    let pic = |coding: u8, seq: bool| {
        let mut d = if seq {
            seq_header(720, 576, 3, 112, false)
        } else {
            Vec::new()
        };
        d.extend(test_es::mpeg2_pic(coding, 3));
        d.resize(20_000, 0x55);
        d
    };
    let v = |pts: i64, key: bool, data: Vec<u8>| PesFrame {
        track: 0,
        pts,
        keyframe: key,
        data,
        duration_ns: None,
        discard_padding_ns: 0,
        source: None,
        coding: None,
    };
    frames.push(v(VIDEO_START_NS, true, pic(1, true)));
    frames.push(v(VIDEO_START_NS + 5_000 * MS, false, pic(2, false)));
    frames.sort_by_key(|f| f.pts);
    let fx = rebuild_aus(Fx { frames, ..base });
    let r = run(&fx);
    assert_replays(&r, &fx);
    assert!(r.counters.pstd.pts_gaps >= 1, "{:?}", r.counters);
    let dts: Vec<Option<u64>> = r
        .parsed
        .pes
        .iter()
        .filter(|x| x.key.0 == 0xE0 && x.pts.is_some())
        .map(|x| x.dts)
        .collect();
    assert_eq!(dts.len(), 2);
}

// MPEG-1 video is carried as stream_type 0x01 (MS-22 "ISO/IEC 11172 Video"), the output
// always an MPEG-2 program stream (design §4, J6).
#[test]
fn mpeg1_video_is_stream_type_1() {
    let mut fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    if let DiscStream::Video(v) = &mut fx.title.streams[0] {
        v.codec = Codec::Mpeg1;
    }
    let r = run(&fx);
    assert_replays(&r, &fx);
    let m = &r.parsed.psms[0].1;
    let info = usize::from(u16::from_be_bytes([m[8], m[9]]));
    assert_eq!(&m[12 + info..14 + info], &[0x01, 0xE0]);
    assert!(r.parsed.packs.iter().all(|_| true));
}

// G8: the LPCM quantization is sticky and never mixes inside one PES (the PES header states
// one depth for its whole payload).
#[test]
fn an_lpcm_depth_rise_never_splits_a_pes() {
    let mut fx = fixture(&Opts::default());
    let half = fx.frames.len() / 2;
    for f in fx.frames.iter_mut().skip(half).filter(|f| f.track == 5) {
        for s in f.data.as_chunks_mut::<3>().0 {
            s[2] = 0x81;
        }
    }
    let fx = Fx {
        input: BTreeMap::new(),
        ..fx
    };
    let fx = rebuild_aus(fx);
    let r = run(&fx);
    assert_replays(&r, &fx);
    let bits: Vec<u8> = r
        .parsed
        .pes
        .iter()
        .filter(|x| x.key == (0xBD, Some(0xA0)))
        .map(|x| x.sub_hdr[5] >> 6)
        .collect();
    assert_eq!(bits.first(), Some(&0), "16-bit first");
    assert_eq!(bits.last(), Some(&2), "24-bit once needed");
    assert!(bits.windows(2).all(|w| w[0] <= w[1]), "never back down");
}

// Design §2.4 step 1 (MPG2-6c): a Matroska source whose audio lags its video in the
// interleave by 3 s is held until the audio catches up; nothing is late.
#[test]
fn a_lagging_audio_interleave_is_waited_for() {
    let base = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let mut frames = base.frames.clone();
    let key = |f: &PesFrame| {
        if f.track == 0 {
            f.pts
        } else {
            f.pts + 3_000 * MS
        }
    };
    frames.sort_by_key(key);
    let fx = rebuild_aus(Fx { frames, ..base });
    let r = run(&fx);
    assert_replays(&r, &fx);
    assert_eq!(r.counters.pstd.late_aus, 0, "{:?}", r.counters);
}

// Tracks the plan skips are dropped silently at write (the pre-mux note lists them); an
// extension whose base is not carried is reported once its packets arrive (J23).
#[test]
fn skipped_tracks_are_dropped_and_an_orphan_extension_is_reported() {
    let mut fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    fx.title.streams.push(DiscStream::Subtitle(SubtitleStream {
        pid: 0x1200,
        codec: Codec::Pgs,
        language: "eng".into(),
        forced: false,
        qualifier: LabelQualifier::None,
        codec_data: None,
    }));
    fx.title.codec_privates.push(None);
    if let DiscStream::Audio(a) = &mut fx.title.streams[1] {
        a.codec = Codec::TrueHd; // the base is not carried, so neither is 0xD0
    }
    let mut sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    for f in &fx.frames {
        sink.write(f).unwrap();
    }
    sink.write(&PesFrame {
        track: 5,
        ..fx.frames[0].clone()
    })
    .unwrap();
    sink.finish().unwrap();
    assert_eq!(sink.undelivered_streams(), vec![2]);
    let out = sink.mux.take().unwrap().into_writer();
    let p = replay::parse(&out).unwrap();
    assert!(p.pes.iter().all(|x| x.key.0 != 0xC0 && x.key.0 != 0xD0));
}

// The replayer is not vacuous: tampered output fails it (design §7 "never more").
#[test]
fn the_replayer_rejects_tampered_streams() {
    let fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let r = run(&fx);
    // MS-16: shrink the video bound in the system header (and first PES) to 16 KiB.
    let mut p = replay::parse(&r.out).unwrap();
    let sh = &mut p.system_headers[0].1;
    let at = sh[12..].chunks(3).position(|e| e[0] == 0xE0).unwrap() * 3 + 12;
    sh[at + 1] = 0xE0;
    sh[at + 2] = 16;
    p.pes.iter_mut().find(|x| x.key.0 == 0xE0).unwrap().pstd = Some((true, 16));
    assert!(
        replay::replay(&p, &fx.aus)
            .unwrap_err()
            .contains("overflows")
    );
    // MS-17: an SCR gap over 0.7 s.
    let mut p = replay::parse(&r.out).unwrap();
    let n = p.packs.len();
    for k in n / 2..n {
        p.packs[k].scr += super::pstd::MAX_SCR_GAP27 + 1;
    }
    assert!(replay::replay(&p, &fx.aus).unwrap_err().contains("SCR gap"));
    // MS-19: a DTS equal to its PTS.
    let mut p = replay::parse(&r.out).unwrap();
    let x = p.pes.iter_mut().find(|x| x.dts.is_some()).unwrap();
    x.dts = x.pts;
    assert!(replay::replay(&p, &fx.aus).unwrap_err().contains("DTS"));
}
