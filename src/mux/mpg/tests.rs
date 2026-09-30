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

// group_of_pictures_header: time code 0 (marker set), closed_gop 1 (no leading B pictures).
fn gop_header() -> Vec<u8> {
    vec![0, 0, 1, 0xB8, 0x00, 0x08, 0x00, 0x40]
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
            let mut pic = test_es::mpeg2_pic(coding, 3);
            // temporal_reference (10 bits): the picture's display index in its GOP (13818-2).
            pic[4] = (d >> 2) as u8;
            pic[5] = ((d as u8 & 3) << 6) | (coding << 3);
            data.extend(pic);
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
        // MPEG-1 Layer II, 256 kbit/s, 48 kHz, stereo, no CRC: 144·256000/48000 = 768 B.
        let mut f = vec![0xFF, 0xFD, 0xC4, 0x00];
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
        // A decodable AC-3 syncframe: 48 kHz, frmsizecod 30 (448 kbit/s, 1792 B), bsid 8, CRC.
        let mut f = vec![0x0B, 0x77, 0, 0, 0x1E, 0x40];
        f.resize(1_792, 0x66);
        let c = crate::mux::codec::crc::crc16_ansi(&f[2..1_790]);
        f[1_790..].copy_from_slice(&c.to_be_bytes());
        let t = a0 + k * 32 * MS;
        add(&mut ev, t, 3, t, true, f, Some(((0xBD, Some(0x80)), 0)));
        k += 1;
    }
    let mut k = 0i64;
    while a0 + k * 512 * 1_000_000_000 / 48_000 < end {
        // A DTS core frame: normal, 512 samples (NBLKS 15), 48 kHz, FSIZE 2011.
        let mut f = vec![0u8; 2_012];
        f[..4].copy_from_slice(&[0x7F, 0xFE, 0x80, 0x01]);
        let fsize = 2_011usize;
        f[4] = 0x80 | (31 << 2);
        f[5] = (15 << 2) | ((fsize >> 12) & 3) as u8;
        f[6] = (fsize >> 4) as u8;
        f[7] = ((fsize & 0x0F) << 4) as u8;
        f[8] = 13 << 2;
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

// The replay finds exactly what the sink counted (design §7).
fn assert_replays(r: &Run, fx: &Fx) {
    let found = replay::replay(&r.parsed, &fx.aus).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(
        (found.late_aus, found.pts_gaps),
        (r.counters.pstd.late_aus, r.counters.pstd.pts_gaps),
        "replayed {found:?} vs counted {:?}",
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
        let at = replay::picture_start(&f.data);
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
}

// Design §2.4 (MPG3-7): an AU larger than its buffer takes the late path rather than
// deadlocking; it is counted in pstd_late_aus, and the replay allows exactly that count.
#[test]
fn an_au_larger_than_its_buffer_is_counted_late() {
    let fx = fixture(&Opts {
        i_size: 300_000,
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let r = run(&fx);
    assert_replays(&r, &fx);
    assert!(r.counters.pstd.late_aus >= 15, "{:?}", r.counters);
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
            0 => Some(((0xE0, None), replay::picture_start(&f.data))),
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
        let at = replay::picture_start(&f.data);
        f.data.drain(..at);
    }
    let r = run(&fx);
    let video = es_of(&r.parsed, (0xE0, None));
    assert!(video.starts_with(&seq));
}

#[test]
fn mpg_urls_parse() {
    let u = crate::mux::resolve::parse_url("mpg:///out/x.mpg");
    assert_eq!(u.scheme(), "mpg");
    assert_eq!(u.path_str(), "/out/x.mpg");
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
    // Design §2.3 "Cap: 1 s of IR timeline or 64 MiB": 5 s of audio behind the held I.
    assert_eq!(r.counters.dts.hold_overflow, 1, "{:?}", r.counters);
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

// G8 / §2.3: a 20/24-bit LPCM AU cut on a frame takes that frame's PTS less the carried
// samples' duration, so the output follows the source clock, not a running sample count.
#[test]
fn lpcm_pts_reanchor_on_every_frame() {
    let fx = fixture(&Opts {
        spu_tracks: 0,
        ..Opts::default()
    });
    let mut sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    let out = sink.route[6].unwrap();
    // 477 sample frames stamped every 10 ms (480 frames): a source clock ahead of its samples.
    let data: Vec<u8> = (0..477 * 6).map(|i| (i as u8) | 1).collect();
    let mut checked = 0;
    for k in 0..400i64 {
        let rel = k * 900;
        let carried = (sink.lpcm[out].carry.len() / 6) as i64;
        for au in sink.lpcm_aus(out, rel, &data, false) {
            let want = rel - (carried * 90_000 + 24_000) / 48_000;
            assert!(
                (au.pts - want).abs() <= 1,
                "frame {k}: {} vs {want}",
                au.pts
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 400);
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

// J23: IFO coding mode 3 only declares the extension. One whose packets never arrive holds
// nothing back (no interleave cap, no warning) and is not described; one first seen after
// the origin window is left out and reported, as the map no longer can describe it.
#[test]
fn a_declared_only_extension_is_neither_waited_for_nor_described() {
    let base = fixture(&Opts {
        secs: 14,
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let fx = rebuild_aus(Fx {
        frames: base
            .frames
            .iter()
            .filter(|f| f.track != 2)
            .cloned()
            .collect(),
        ..base
    });
    let r = run(&fx);
    assert_eq!(
        r.counters,
        MpgCounters::default(),
        "a clean disc counts nothing"
    );
    assert_replays(&r, &fx);
    let m = &r.parsed.psms[0].1;
    let info = usize::from(u16::from_be_bytes([m[8], m[9]]));
    let es = &m[12 + info..m.len() - 4];
    assert!(
        !es.windows(2).any(|w| w == [0x04, 0xD0]),
        "0xD0 is not mapped"
    );
    assert_eq!(
        &es[4..6],
        &[0x03, 0xC0],
        "a base with no extension is 11172 audio"
    );
    let sh = &r.parsed.system_headers[0].1;
    assert!(sh[12..].chunks(3).all(|e| e[0] != 0xD0), "no 0xD0 bound");

    // First seen after the origin window: left out, and reported once its packets arrive.
    let late = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let frames: Vec<PesFrame> = late
        .frames
        .iter()
        .filter(|f| f.track != 2 || f.pts > VIDEO_START_NS + 3_000 * MS)
        .cloned()
        .collect();
    let mut sink = MpgSink::create(Vec::new(), &late.title).unwrap();
    for f in &frames {
        sink.write(f).unwrap();
    }
    sink.finish().unwrap();
    assert_eq!(sink.undelivered_streams(), vec![2]);
    let out = sink.mux.take().unwrap().into_writer();
    assert!(
        replay::parse(&out)
            .unwrap()
            .pes
            .iter()
            .all(|x| x.key.0 != 0xD0)
    );
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
    // MS-2: a pack header marker bit cleared.
    let mut out = r.out.clone();
    out[pack::PACK_BYTES + 4] &= !0x04;
    assert!(replay::parse(&out).unwrap_err().contains("marker"));
    // MS-10: a PTS marker bit cleared in the first video PES.
    let first = replay::parse(&r.out).unwrap();
    let v = first.pes.iter().find(|x| x.key.0 == 0xE0).unwrap();
    let at = r.out[..v.data_off]
        .windows(4)
        .rposition(|w| w == [0, 0, 1, 0xE0])
        .unwrap();
    let mut out = r.out.clone();
    out[at + 9 + 4] &= !0x01;
    assert!(replay::parse(&out).unwrap_err().contains("marker"));
}

// ── L3: `mpg://` as a source (design §4) ────────────────────────────────────────────

fn temp_path(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("fmkv-mpg-{tag}-{}-{n}.mpg", std::process::id()))
}

fn read_all(path: &std::path::Path) -> std::io::Result<(DiscTitle, Vec<PesFrame>)> {
    // The sector pipeline spawns a prefetcher (a Drive holder).
    let _g = crate::sector::prefetched::holder_test_lock();
    let url = format!("mpg://{}", path.display());
    let mut input = crate::mux::resolve::input(&url, &Default::default())?;
    let mut frames = Vec::new();
    while let Some(f) = input.read()? {
        frames.push(f);
    }
    Ok((input.info().clone(), frames))
}

fn by_track(frames: &[PesFrame]) -> BTreeMap<usize, Vec<&PesFrame>> {
    let mut m: BTreeMap<usize, Vec<&PesFrame>> = BTreeMap::new();
    for f in frames {
        m.entry(f.track).or_default().push(f);
    }
    m
}

fn tracks_match(
    fx_frames: &BTreeMap<usize, Vec<(i64, Vec<u8>)>>,
    got: &[PesFrame],
    lpcm: &[usize],
) {
    let got = by_track(got);
    let mut offset = None;
    for (t, want) in fx_frames {
        let g = got
            .get(t)
            .unwrap_or_else(|| panic!("track {t} read nothing"));
        if lpcm.contains(t) {
            let a: Vec<u8> = g.iter().flat_map(|f| f.data.iter().copied()).collect();
            let b: Vec<u8> = want.iter().flat_map(|f| f.1.iter().copied()).collect();
            assert_eq!(a, b, "LPCM track {t}");
            continue;
        }
        let mut w: Vec<&(i64, Vec<u8>)> = want.iter().collect();
        let mut g: Vec<&&PesFrame> = g.iter().collect();
        w.sort_by_key(|f| f.0);
        g.sort_by_key(|f| f.pts);
        assert_eq!(g.len(), w.len(), "track {t} frame count");
        for (a, b) in g.iter().zip(&w) {
            assert_eq!(a.data, b.1, "track {t} frame bytes");
            let d = ns_to_ticks(a.pts) - ns_to_ticks(b.0);
            assert_eq!(
                *offset.get_or_insert(d),
                d,
                "track {t}: one constant tick offset (J16)"
            );
        }
    }
}

// Design §4 steps 2-3 and §7 round trips: our own program stream reads back, stream for
// stream, with the metadata the map carries (ISO 639, the extension, FMKV languages,
// forced flag and palette) and every frame's bytes and PTS (modulo one tick offset).
#[test]
fn an_mpg_file_reads_back_through_the_ps_pipeline() {
    let fx = fixture(&Opts::default());
    let r = run(&fx);
    let path = temp_path("rt");
    std::fs::write(&path, &r.out).unwrap();
    let (title, frames) = read_all(&path).unwrap();
    let kinds: Vec<(u16, Codec, String)> = title
        .streams
        .iter()
        .map(|s| match s {
            DiscStream::Video(v) => (v.pid, v.codec, String::new()),
            DiscStream::Audio(a) => (a.pid, a.codec, a.language.clone()),
            DiscStream::Subtitle(t) => (t.pid, t.codec, t.language.clone()),
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            (0xE0, Codec::Mpeg2, String::new()),
            (0xC0, Codec::Mp2, "eng".into()),
            (0xD0, Codec::Mp2, "eng".into()),
            (0xBD80, Codec::Ac3, "fra".into()),
            (0xBD88, Codec::Dts, "deu".into()),
            (0xBDA0, Codec::Lpcm, "spa".into()),
            (0xBDA1, Codec::Lpcm, "ita".into()),
            (0x20, Codec::DvdSub, "eng".into()),
        ]
    );
    assert!(matches!(&title.streams[2], DiscStream::Audio(a) if a.is_mp2_extension()));
    let DiscStream::Subtitle(sub) = &title.streams[7] else {
        panic!()
    };
    assert!(sub.forced);
    assert!(
        sub.codec_data
            .as_deref()
            .is_some_and(|c| idx_palette(c).is_some())
    );
    tracks_match(&fx.input, &frames, &[5, 6]);
    let _ = std::fs::remove_file(&path);
}

// §7 round trip 1: "PS → IR → mpg → IR: per-track (pts − const, keyframe, data) exactly".
#[test]
fn ps_to_ir_to_mpg_to_ir_is_exact() {
    let fx = fixture(&Opts::default());
    let p1 = temp_path("a");
    std::fs::write(&p1, run(&fx).out).unwrap();
    let (t1, f1) = read_all(&p1).unwrap();
    let mut sink = MpgSink::create(Vec::new(), &t1).unwrap();
    for f in &f1 {
        sink.write(f).unwrap();
    }
    sink.finish().unwrap();
    let p2 = temp_path("b");
    std::fs::write(&p2, sink.mux.take().unwrap().into_writer()).unwrap();
    let (t2, f2) = read_all(&p2).unwrap();
    assert_eq!(t1.streams.len(), t2.streams.len());
    let (a, b) = (by_track(&f1), by_track(&f2));
    let mut offset = None;
    for (t, fa) in &a {
        let fb = &b[t];
        if matches!(&t1.streams[*t], DiscStream::Audio(x) if x.codec == Codec::Lpcm) {
            // LPCM IR frames follow the PES they came in; the samples are what round-trips.
            let cat = |f: &Vec<&PesFrame>| {
                f.iter()
                    .flat_map(|x| x.data.iter().copied())
                    .collect::<Vec<u8>>()
            };
            assert_eq!(cat(fa), cat(fb), "LPCM track {t}");
            continue;
        }
        assert_eq!(fa.len(), fb.len(), "track {t}");
        for (x, y) in fa.iter().zip(fb) {
            assert_eq!((x.keyframe, &x.data), (y.keyframe, &y.data), "track {t}");
            let d = ns_to_ticks(y.pts) - ns_to_ticks(x.pts);
            assert_eq!(*offset.get_or_insert(d), d);
        }
    }
    let _ = (std::fs::remove_file(&p1), std::fs::remove_file(&p2));
}

// Design §4 step 2 (MPG-11, J3): a CSS-scrambled `.vob` is cracked keylessly and read.
#[test]
fn a_css_scrambled_vob_is_descrambled() {
    let fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let clear = run(&fx).out;
    let mut vob = clear.clone();
    let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    let mut scrambled = 0;
    for pk in vob.as_chunks_mut::<{ pack::PACK_BYTES }>().0 {
        // Only a whole-sector video pack with no stuffing, as a DVD encoder writes them.
        if pk[13] & 7 == 0 && pk[17] == 0xE0 && pk[0x14] & 0x30 == 0 {
            pk[0x14] |= 0x10;
            crate::css::lfsr::scramble_sector(&key, pk);
            scrambled += 1;
        }
    }
    assert!(scrambled > 10);
    let path = temp_path("css");
    std::fs::write(&path, &vob).unwrap();
    let (_, frames) = read_all(&path).unwrap();
    let want: Vec<u8> = fx.input[&0]
        .iter()
        .flat_map(|f| f.1.iter().copied())
        .collect();
    let got: Vec<u8> = frames
        .iter()
        .filter(|f| f.track == 0)
        .flat_map(|f| f.data.iter().copied())
        .collect();
    assert_eq!(got, want, "descrambled video is byte-identical");
    let _ = std::fs::remove_file(&path);
}

// A CSS-scrambled copy of the fixture's output: every whole-sector video pack scrambled.
fn scrambled_vob(fx: &Fx) -> Vec<u8> {
    let mut vob = run(fx).out;
    let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    for pk in vob.as_chunks_mut::<{ pack::PACK_BYTES }>().0 {
        if pk[13] & 7 == 0 && pk[17] == 0xE0 && pk[0x14] & 0x30 == 0 {
            pk[0x14] |= 0x10;
            crate::css::lfsr::scramble_sector(&key, pk);
        }
    }
    vob
}

// Design §4 step 2.1: "css::resolve_dvd_title_key(… raw = false, halt)". A stop reaches the
// crack, and `--raw` still cracks so the head scan reads the streams (the mux stays raw).
#[test]
fn the_crack_honours_halt_and_runs_under_raw() {
    let fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let path = temp_path("halt");
    std::fs::write(&path, scrambled_vob(&fx)).unwrap();
    // The sequence header past the first 0x80 bytes of its pack, where CSS scrambles.
    let (lead, _) = clear_ps_with_lead(false, 0, 200);
    let mut vob = lead;
    let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    for pk in vob.as_chunks_mut::<{ pack::PACK_BYTES }>().0 {
        if pk[0x11] == 0xE0 {
            pk[0x14] |= 0x10;
            crate::css::lfsr::scramble_sector(&key, pk);
        }
    }
    let lead_path = temp_path("lead");
    std::fs::write(&lead_path, vob).unwrap();
    let url = format!("mpg://{}", path.display());
    let _g = crate::sector::prefetched::holder_test_lock();
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let e = crate::mux::resolve::input_with_halt(&url, &Default::default(), Some(&halt))
        .err()
        .expect("a stopped crack is Halted");
    assert_eq!(crate::error::error_code(&e), Some(crate::error::E_HALTED));
    let raw = crate::mux::resolve::InputOptions {
        raw: true,
        ..Default::default()
    };
    let lead_url = format!("mpg://{}", lead_path.display());
    let input = crate::mux::resolve::input(&lead_url, &raw).expect("raw input");
    assert!(
        matches!(&input.info().streams[0], DiscStream::Video(v) if v.resolution == Resolution::R1080p),
        "the head scan read the descrambled sequence header: {:?}",
        input.info().streams.first()
    );
    drop(input);
    let _ = (
        std::fs::remove_file(&path),
        std::fs::remove_file(&lead_path),
    );
}

// Design §4 step 2 (J13): a truncated rip (size not a multiple of 2048) is read, its cut-off
// final packet dropped by the demuxer.
#[test]
fn a_truncated_file_is_read() {
    let fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let mut out = run(&fx).out;
    out.truncate(out.len() - 1_000);
    let path = temp_path("cut");
    std::fs::write(&path, &out).unwrap();
    let (_, frames) = read_all(&path).unwrap();
    assert!(frames.iter().filter(|f| f.track == 0).count() > 100);
    let _ = std::fs::remove_file(&path);
}

// Design §4 (J6): an ISO/IEC 11172-1 system stream is read, not refused; its MPEG-1 video
// goes out as stream_type 0x01 in an MPEG-2 program stream.
#[test]
fn an_mpeg1_system_stream_is_read_and_remuxed() {
    fn pack1() -> Vec<u8> {
        vec![
            0, 0, 1, 0xBA, 0x21, 0x00, 0x01, 0x00, 0x01, 0x80, 0x1B, 0x83,
        ]
    }
    fn packet(id: u8, pts: Option<u64>, payload: &[u8]) -> Vec<u8> {
        let mut body = vec![0xFF];
        match pts {
            Some(t) => body.extend_from_slice(&[
                0x21 | (((t >> 29) & 0x0E) as u8),
                (t >> 22) as u8,
                1 | (((t >> 14) & 0xFE) as u8),
                (t >> 7) as u8,
                1 | (((t << 1) & 0xFE) as u8),
            ]),
            None => body.push(0x0F),
        }
        body.extend_from_slice(payload);
        let mut p = vec![0, 0, 1, id];
        p.extend_from_slice(&(body.len() as u16).to_be_bytes());
        p.extend(body);
        p
    }
    let mut ps = Vec::new();
    let mut video = Vec::new();
    for k in 0..50u64 {
        let mut pic = if k % 10 == 0 {
            test_es::mpeg1_seq(3)
        } else {
            Vec::new()
        };
        pic.extend(test_es::mpeg2_pic(if k % 10 == 0 { 1 } else { 2 }, 3));
        pic.resize(3_000, 0x55);
        video.push(pic.clone());
        let pts = 45_000 + k * 3_600;
        for (i, chunk) in pic.chunks(2_000).enumerate() {
            ps.extend(pack1());
            ps.extend(packet(0xE0, (i == 0).then_some(pts), chunk));
        }
        let mut a = vec![0xFF, 0xFD, 0xC4, 0x00];
        a.resize(768, 0x33);
        ps.extend(pack1());
        ps.extend(packet(0xC0, Some(pts), &a));
    }
    ps.extend_from_slice(&[0, 0, 1, 0xB9]);
    let path = temp_path("vcd");
    std::fs::write(&path, &ps).unwrap();
    let (title, frames) = read_all(&path).unwrap();
    assert!(matches!(&title.streams[0], DiscStream::Video(v) if v.codec == Codec::Mpeg1));
    let got: Vec<&[u8]> = frames
        .iter()
        .filter(|f| f.track == 0)
        .map(|f| &f.data[..])
        .collect();
    assert_eq!(
        got.len(),
        video.len(),
        "one IR frame per picture (Mpeg2Parser)"
    );
    let mut sink = MpgSink::create(Vec::new(), &title).unwrap();
    for f in &frames {
        sink.write(f).unwrap();
    }
    sink.finish().unwrap();
    let out = sink.mux.take().unwrap().into_writer();
    let p = replay::parse(&out).unwrap();
    let m = &p.psms[0].1;
    let info = usize::from(u16::from_be_bytes([m[8], m[9]]));
    assert_eq!(
        &m[12 + info..14 + info],
        &[0x01, 0xE0],
        "MPEG-1 video is stream_type 0x01"
    );
    let _ = std::fs::remove_file(&path);
}

// A clear program stream of 2048-byte packs carrying `pics` as one video stream: 11172-1
// packs whose packets carry the STD buffer field and a PTS (as FFmpeg writes them), or
// 13818-1 packs with `stuffing` pack-stuffing bytes. Returns the file and the video ES.
fn clear_ps(mpeg1: bool, stuffing: usize) -> (Vec<u8>, Vec<u8>) {
    clear_ps_with_lead(mpeg1, stuffing, 0)
}

// `clear_ps` with `lead` bytes of user data ahead of the first sequence header.
fn clear_ps_with_lead(mpeg1: bool, stuffing: usize, lead: usize) -> (Vec<u8>, Vec<u8>) {
    let mut es = Vec::new();
    if lead > 0 {
        es.extend_from_slice(&[0, 0, 1, 0xB2]);
        es.resize(lead, 0x5A);
    }
    let mut starts = Vec::new();
    for k in 0..30u64 {
        starts.push((es.len(), 45_000 + k * 3_600));
        if k == 0 {
            es.extend(if mpeg1 {
                test_es::mpeg1_seq(3)
            } else {
                test_es::mpeg2_seq(3, false)
            });
        }
        es.extend(test_es::mpeg2_pic(if k == 0 { 1 } else { 2 }, 3));
        es.resize(es.len() + 5_000, 0x55);
    }
    let ts = |prefix: u8, t: u64| {
        [
            (prefix << 4) | (((t >> 29) & 0x0E) as u8) | 1,
            (t >> 22) as u8,
            1 | (((t >> 14) & 0xFE) as u8),
            (t >> 7) as u8,
            1 | (((t << 1) & 0xFE) as u8),
        ]
    };
    let mut out = Vec::new();
    let mut at = 0;
    let mut scr = 0u64;
    while at < es.len() {
        // The PTS of a picture whose first byte this packet will hold.
        let pts = |room: usize| {
            starts
                .iter()
                .find(|(o, _)| *o >= at && *o < at + room)
                .map(|&(_, t)| t)
        };
        let mut pk = Vec::new();
        if mpeg1 {
            pk.extend_from_slice(&[0, 0, 1, 0xBA, 0x21, 0, 1, 0, 1, 0x80, 0x1B, 0x83]);
            let head = 6 + 2 + 5;
            let t = pts(2048 - 12 - head);
            let room = 2048 - 12 - 6 - 2 - if t.is_some() { 5 } else { 1 };
            let n = room.min(es.len() - at);
            let mut body = vec![0x40 | 0x20, 46]; // STD buffer: scale 1, 46 KiB
            match t {
                Some(t) => body.extend_from_slice(&ts(0b0010, t)),
                None => body.push(0x0F),
            }
            body.extend_from_slice(&es[at..at + n]);
            pk.extend_from_slice(&[0, 0, 1, 0xE0]);
            pk.extend_from_slice(&(body.len() as u16).to_be_bytes());
            pk.extend(body);
            at += n;
        } else {
            pk.extend(pack::pack_header(scr, 25_200, stuffing));
            let t = pts(2048 - pk.len() - 14);
            let f = pack::PesFields {
                pts: t,
                ..Default::default()
            };
            let n = (2048 - pk.len() - pack::pes_header_len(&f)).min(es.len() - at);
            pk.extend(pack::pes_header(0xE0, &f, n));
            pk.extend_from_slice(&es[at..at + n]);
            at += n;
        }
        if pk.len() < 2048 {
            let pad = 2048 - pk.len();
            pk.extend(pack::padding_pes(pad));
        }
        assert_eq!(pk.len(), 2048);
        out.extend(pk);
        scr += 27_000;
    }
    out.extend_from_slice(&[0, 0, 1, 0xB9]);
    (out, es)
}

// B2: an 11172-1 stream cannot be CSS, and a stuffed 13818-1 pack header moves bytes 0x11
// and 0x14 off the stream_id and PES flags: neither clear file is refused with E7023.
#[test]
fn clear_mpeg1_and_stuffed_mpeg2_are_never_read_as_scrambled() {
    for (mpeg1, stuffing) in [(true, 0), (false, 1), (false, 3)] {
        let (file, es) = clear_ps(mpeg1, stuffing);
        let path = temp_path("clear");
        std::fs::write(&path, &file).unwrap();
        let got = read_all(&path);
        let _ = std::fs::remove_file(&path);
        let (_, frames) = got.unwrap_or_else(|e| panic!("mpeg1={mpeg1} stuffing={stuffing}: {e}"));
        let video: Vec<u8> = frames
            .iter()
            .filter(|f| f.track == 0)
            .flat_map(|f| f.data.iter().copied())
            .collect();
        assert_eq!(video, es, "mpeg1={mpeg1} stuffing={stuffing}");
    }
}

// Scramble every video pack of `ps` whose PES flags sit at 0x14 + its pack stuffing.
fn scramble_video_packs(ps: &mut [u8]) -> usize {
    let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    let mut n = 0;
    for pk in ps.as_chunks_mut::<{ pack::PACK_BYTES }>().0 {
        let at = 0x14 + usize::from(pk[13] & 7);
        if pk[at - 3] == 0xE0 {
            // The test scrambler flags byte 0x14; a stuffed pack's flags are at `at`.
            let b14 = pk[0x14];
            pk[at] |= 0x10;
            crate::css::lfsr::scramble_sector(&key, pk);
            if at != 0x14 {
                pk[0x14] = b14;
            }
            n += 1;
        }
    }
    n
}

// Where two byte strings first differ (their lengths when one is a prefix), for a short
// failure message.
fn first_diff(a: &[u8], b: &[u8]) -> Option<(usize, usize, usize)> {
    let at = a.iter().zip(b).position(|(x, y)| x != y);
    (at.is_some() || a.len() != b.len())
        .then(|| (at.unwrap_or(a.len().min(b.len())), a.len(), b.len()))
}

// The video ES a file reads back as, or its error code.
fn read_video(file: &[u8], tag: &str) -> Result<Vec<u8>, Option<u16>> {
    let path = temp_path(tag);
    std::fs::write(&path, file).unwrap();
    let got = read_all(&path);
    let _ = std::fs::remove_file(&path);
    got.map(|(_, frames)| {
        frames
            .iter()
            .filter(|f| f.track == 0)
            .flat_map(|f| f.data.iter().copied())
            .collect()
    })
    .map_err(|e| crate::error::error_code(&e))
}

// B-1: a clear file whose first pack begins with a program stream map (byte 0x14 holds the
// map's version byte) opens, and reads back unchanged.
#[test]
fn a_clear_file_opening_with_a_map_opens() {
    let (body, es) = clear_ps(false, 0);
    let e = [pack::PsmEntry {
        stream_type: 0x02,
        stream_id: 0xE0,
        descriptors: vec![],
    }];
    let map = pack::psm(&[], &e).unwrap();
    let mut first = pack::pack_header(0, 25_200, 0);
    first.extend_from_slice(&map);
    first.extend(pack::padding_pes(pack::PACK_BYTES - first.len()));
    assert!(first[0x14] & 0x30 != 0, "fixture: 0x14 reads as scrambled");
    let file = [first, body].concat();
    let video = read_video(&file, "map-first").map(|v| v.len());
    assert_eq!(video, Ok(es.len()));
}

// m1: an MPEG-2 file whose scrambled packs are all stuffed is judged by the flags where its
// stuffing puts them: descrambled, or refused with E7023, never muxed as ciphertext.
#[test]
fn stuffed_scrambled_packs_are_never_muxed_as_ciphertext() {
    let (mut file, es) = clear_ps(false, 2);
    assert!(scramble_video_packs(&mut file) > 10);
    match read_video(&file, "stuffed-css") {
        Ok(video) => assert_eq!(first_diff(&video, &es), None, "descrambled"),
        Err(code) => assert_eq!(code, Some(crate::error::E_CSS_KEY_MISSING)),
    }
}

// m1: scrambling first met past the crack's 50 000-sector budget is refused with E7023 when
// it is read, never muxed as ciphertext.
#[test]
fn scrambling_past_the_crack_budget_is_refused() {
    let (head, _) = clear_ps(false, 0);
    let mut file = head[..head.len() - 4].to_vec();
    let mut pad = pack::pack_header(0, 25_200, 0);
    pad.extend(pack::padding_pes(pack::PACK_BYTES - pad.len()));
    for _ in 0..50_000 {
        file.extend_from_slice(&pad);
    }
    let (mut tail, _) = clear_ps(false, 0);
    scramble_video_packs(&mut tail);
    file.extend(tail);
    let got = read_video(&file, "late-css").map(|v| v.len());
    assert_eq!(got, Err(Some(crate::error::E_CSS_KEY_MISSING)));
}

// m2: in a CSS file, a clear stuffed pack whose byte 0x14 happens to hold set bits (here the
// low byte of PES_packet_length) is read as clear, not "descrambled".
#[test]
fn a_clear_stuffed_pack_in_a_css_file_is_left_clear() {
    let (mut first, es1) = clear_ps(false, 0);
    scramble_video_packs(&mut first);
    let (second, es2) = clear_ps(false, 1);
    assert!(
        second
            .as_chunks::<{ pack::PACK_BYTES }>()
            .0
            .iter()
            .any(|pk| pk[0x14] & 0x30 != 0),
        "fixture: a clear stuffed pack reads as scrambled at 0x14"
    );
    let mut file = first[..first.len() - 4].to_vec();
    file.extend(second);
    let video = read_video(&file, "css-then-stuffed").expect("reads");
    assert_eq!(first_diff(&video, &[es1, es2].concat()), None);
}

// D3: a clear file is crack-scanned once; the pipeline is handed that verdict ("clear")
// rather than scanning again because its keys are None.
#[test]
fn a_clear_file_is_crack_scanned_once() {
    let (file, _) = clear_ps(false, 0);
    let path = temp_path("once");
    std::fs::write(&path, &file).unwrap();
    crate::css::CRACK_SCANS.with(|n| n.set(0));
    let got = read_all(&path);
    let _ = std::fs::remove_file(&path);
    got.unwrap();
    assert_eq!(crate::css::CRACK_SCANS.with(|n| n.get()), 1);
}

// Design §4: "A file with no pack start code in its head is not a PS; it fails with E6009".
#[test]
fn a_file_with_no_pack_is_no_streams() {
    let path = temp_path("junk");
    std::fs::write(&path, vec![0x5Au8; 10_000]).unwrap();
    let e = read_all(&path).unwrap_err();
    assert_eq!(
        crate::error::error_code(&e),
        Some(crate::error::E_NO_STREAMS)
    );
    let _ = std::fs::remove_file(&path);
}

// Design §4 step 3: without a map, 0xD0-0xD7 are classified by sync — `0x7FF` ext_syncword
// is an extension, `0xFFF` an ordinary MPEG audio stream (a foreign PS with audio at 0xD2).
#[test]
fn without_a_map_d0_streams_are_classified_by_sync() {
    let mut ps = Vec::new();
    let pack2 = [0, 0, 1, 0xBA, 0x44, 0, 4, 0, 4, 1, 0, 0, 3, 0xF8];
    let pes = |id: u8, data: &[u8]| {
        let mut p = vec![0, 0, 1, id, 0, 0, 0x80, 0x80, 5, 0x21, 0, 1, 0, 1];
        p.extend_from_slice(data);
        let len = (p.len() - 6) as u16;
        p[4..6].copy_from_slice(&len.to_be_bytes());
        p
    };
    ps.extend_from_slice(&pack2);
    let mut v = seq_header(720, 576, 3, 112, false);
    v.extend(test_es::mpeg2_pic(1, 3));
    ps.extend(pes(0xE0, &v));
    ps.extend(pes(0xC0, &[0xFF, 0xFD, 0xC4, 0x00, 0, 0]));
    ps.extend(pes(0xD0, &[0x7F, 0xF1, 0x23, 0x45]));
    ps.extend(pes(0xD2, &[0xFF, 0xFD, 0xC4, 0xC0, 0, 0]));
    let s = scan::scan_streams(&ps).unwrap();
    let ids: Vec<(u16, bool)> = s
        .iter()
        .filter_map(|x| match x {
            DiscStream::Audio(a) => Some((a.pid, a.is_mp2_extension())),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![(0xC0, false), (0xD0, true), (0xD2, false)]);
    assert!(matches!(&s[3], DiscStream::Audio(a) if a.channels == AudioChannels::Mono));
}

// A pack of one MPEG-2 PES per `(stream_id, payload)`, PTS 90 000, for scan tests.
fn scan_pack(pes: &[(u8, Vec<u8>)], map: Option<&[u8]>) -> Vec<u8> {
    let mut ps = vec![0, 0, 1, 0xBA, 0x44, 0, 4, 0, 4, 1, 0, 0, 3, 0xF8];
    if let Some(m) = map {
        ps.extend_from_slice(m);
    }
    for (id, data) in pes {
        let mut p = vec![0, 0, 1, *id, 0, 0, 0x80, 0x80, 5, 0x21, 0, 1, 0, 1];
        p.extend_from_slice(data);
        let len = (p.len() - 6) as u16;
        p[4..6].copy_from_slice(&len.to_be_bytes());
        ps.extend(p);
    }
    ps
}

fn scan_video() -> Vec<u8> {
    let mut v = seq_header(720, 576, 3, 112, false);
    v.extend(test_es::mpeg2_pic(1, 3));
    v
}

// scan() carries the sequence header's aspect (code 2, 4:3) onto the video stream.
#[test]
fn scan_sets_the_video_display_aspect() {
    let ps = scan_pack(&[(0xE0, scan_video())], None);
    let s = scan::scan_streams(&ps).unwrap();
    assert!(
        matches!(&s[0], DiscStream::Video(v) if v.display_aspect == Some((4, 3))),
        "{:?}",
        s[0]
    );
}

// Design §4 step 3: without a map, a `0xD0-0xD7` packet that starts mid-frame is classified
// by the first sync word in its bytes, not by its first two bytes.
#[test]
fn sync_classification_finds_the_first_sync() {
    let ps = scan_pack(
        &[
            (0xE0, scan_video()),
            (0xC0, vec![0xFF, 0xFD, 0xC4, 0x00, 0, 0]),
            (0xD0, vec![0x12, 0x34, 0x00, 0x7F, 0xF1, 0x23, 0x45]),
        ],
        None,
    );
    let s = scan::scan_streams(&ps).unwrap();
    assert!(
        matches!(&s[2], DiscStream::Audio(a) if a.is_mp2_extension()),
        "{:?}",
        s[2]
    );
}

// MS-23 / design §4 step 3: the map pairs an extension with its base by
// hierarchy_embedded_layer_index, wherever the base sits in the map.
#[test]
fn the_map_pairs_an_extension_by_its_embedded_layer_index() {
    let e = [
        pack::PsmEntry {
            stream_type: 0x02,
            stream_id: 0xE0,
            descriptors: vec![],
        },
        pack::PsmEntry {
            stream_type: 0x04,
            stream_id: 0xD1,
            descriptors: pack::hierarchy_descriptor(pack::HIERARCHY_EXTENSION, 3, 2).to_vec(),
        },
        pack::PsmEntry {
            stream_type: 0x04,
            stream_id: 0xC1,
            descriptors: pack::hierarchy_descriptor(pack::HIERARCHY_BASE, 2, 0).to_vec(),
        },
    ];
    let m = pack::psm(&[], &e).unwrap();
    let ps = scan_pack(
        &[
            (0xE0, scan_video()),
            (0xD1, vec![0x7F, 0xF1, 0x23, 0x45]),
            (0xC1, vec![0xFF, 0xFD, 0xC4, 0x00, 0, 0]),
        ],
        Some(&m),
    );
    let s = scan::scan_streams(&ps).unwrap();
    let audio: Vec<(u16, bool)> = s
        .iter()
        .filter_map(|x| match x {
            DiscStream::Audio(a) => Some((a.pid, a.is_mp2_extension())),
            _ => None,
        })
        .collect();
    assert_eq!(audio, vec![(0xD1, true), (0xC1, false)]);
}

// Design §4 step 3 (MPEG audio: "header + mc_header"): a 13818-3 multichannel base reports
// its main programme's channel count, not the 2 of its MPEG-1 header.
#[test]
fn mpeg_audio_channels_come_from_the_mc_header() {
    use crate::mux::codec::mp2_channels::tests::{MC_3_2_LFE, STEREO_256, Spec, write};
    let f = write(Spec {
        mc: Some(MC_3_2_LFE),
        ..STEREO_256
    })
    .0;
    let ps = scan_pack(&[(0xE0, scan_video()), (0xC0, f.repeat(4))], None);
    let s = scan::scan_streams(&ps).unwrap();
    assert!(
        matches!(&s[1], DiscStream::Audio(a) if a.channels == AudioChannels::Surround51),
        "{:?}",
        s[1]
    );
}

// Design §4: one video stream is carried; a second video stream_id is not interleaved into it.
#[test]
fn only_the_chosen_video_stream_is_routed() {
    let (a, es_a) = clear_ps(false, 0);
    let (b, _) = clear_ps(false, 0);
    let mut file = Vec::new();
    let packs_a = a.as_chunks::<{ pack::PACK_BYTES }>().0;
    let packs_b = b.as_chunks::<{ pack::PACK_BYTES }>().0;
    for (pa, pb) in packs_a.iter().zip(packs_b) {
        file.extend_from_slice(pa);
        let mut pb = *pb;
        if pb[0x11] == 0xE0 {
            pb[0x11] = 0xE1;
            for x in &mut pb[0x80..] {
                *x ^= 0x0F;
            }
        }
        file.extend_from_slice(&pb);
    }
    file.extend_from_slice(&[0, 0, 1, 0xB9]);
    let path = temp_path("two-video");
    std::fs::write(&path, &file).unwrap();
    let got = read_all(&path);
    let _ = std::fs::remove_file(&path);
    let (title, frames) = got.unwrap();
    assert_eq!(
        title
            .streams
            .iter()
            .filter(|s| matches!(s, DiscStream::Video(_)))
            .count(),
        1
    );
    let video: Vec<u8> = frames
        .iter()
        .filter(|f| f.track == 0)
        .flat_map(|f| f.data.iter().copied())
        .collect();
    assert_eq!(video, es_a, "0xE0 only");
}

#[test]
fn a_map_with_a_bad_crc_is_ignored() {
    let e = [pack::PsmEntry {
        stream_type: 0x02,
        stream_id: 0xE0,
        descriptors: vec![],
    }];
    let mut m = pack::psm(&[], &e).unwrap();
    assert!(scan::parse_map(&m).is_some());
    let n = m.len();
    m[n - 1] ^= 1;
    assert!(scan::parse_map(&m).is_none());
}

// §7 round trip (mpg → mkv): the driver remuxes an `mpg://` source to `mkv://`; the video
// and AC-3 frames read back from the mkv equal those read from the mpg (modulo one tick
// offset); the extension is the mkv sink's to exclude (M1, J23).
#[test]
fn an_mpg_source_remuxes_to_mkv() {
    use crate::mux::driver::{MuxOptions, MuxSource, NoopEvents, mux_with_keys};
    let fx = fixture(&Opts {
        spu_tracks: 0,
        ..Opts::default()
    });
    let mpg = temp_path("src");
    std::fs::write(&mpg, run(&fx).out).unwrap();
    let (_, from_mpg) = read_all(&mpg).unwrap();
    let mkv = std::env::temp_dir().join(format!("fmkv-mpg-out-{}.mkv", std::process::id()));
    let url = format!("mpg://{}", mpg.display());
    let opts = MuxOptions {
        skip_errors: false,
        batch_sectors: 8192,
        raw: false,
        selection: Default::default(),
    };
    let out = {
        let _g = crate::sector::prefetched::holder_test_lock();
        mux_with_keys(
            MuxSource::Url {
                url: &url,
                opts: Default::default(),
            },
            None,
            &format!("mkv://{}", mkv.display()),
            &opts,
            &crate::halt::Halt::new(),
            std::sync::Arc::new(NoopEvents),
        )
        .unwrap()
    };
    assert!(out.completed);
    assert_eq!(
        out.undelivered_streams,
        vec![2],
        "the extension, once seen (J23)"
    );
    let mut input =
        crate::mux::resolve::input(&format!("mkv://{}", mkv.display()), &Default::default())
            .unwrap();
    let mut from_mkv = Vec::new();
    while let Some(f) = input.read().unwrap() {
        from_mkv.push(f);
    }
    let codec_of = |t: &DiscTitle, i: usize| match &t.streams[i] {
        DiscStream::Video(v) => v.codec,
        DiscStream::Audio(a) => a.codec,
        DiscStream::Subtitle(s) => s.codec,
    };
    let mkv_title = input.info().clone();
    let pick = |frames: &[PesFrame], title: &DiscTitle, c: Codec| -> Vec<(i64, Vec<u8>)> {
        frames
            .iter()
            .filter(|f| codec_of(title, f.track) == c)
            .map(|f| (ns_to_ticks(f.pts), f.data.clone()))
            .collect()
    };
    let (_, mpg_title) = (0, read_all(&mpg).unwrap().0);
    let mut offsets: Vec<i64> = Vec::new();
    for c in [Codec::Mpeg2, Codec::Ac3] {
        let (a, b) = (
            pick(&from_mpg, &mpg_title, c),
            pick(&from_mkv, &mkv_title, c),
        );
        assert_eq!(a.len(), b.len(), "{c:?}");
        assert!(a.iter().zip(&b).all(|(x, y)| x.1 == y.1), "{c:?} bytes");
        offsets.extend(a.iter().zip(&b).map(|(x, y)| y.0 - x.0));
    }
    // One origin shift for every frame of every track: no per-track drift or skew.
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), 1, "timestamp offsets differ: {offsets:?}");
    let _ = (std::fs::remove_file(&mpg), std::fs::remove_file(&mkv));
}

// An extension listed before its base still routes to the base's output, so the base keeps
// its hierarchy descriptor.
#[test]
fn an_extension_ahead_of_its_base_still_finds_it() {
    let mut fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    fx.title.streams.swap(1, 2);
    let sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    let ext = sink
        .outs
        .iter()
        .find_map(|o| match o.kind {
            OutKind::Extension { base_out } => Some(base_out),
            _ => None,
        })
        .unwrap();
    assert_eq!(sink.outs[ext].spec.stream_id, 0xC0);
    assert!(matches!(
        sink.outs[ext].kind,
        OutKind::MpegAudio { has_ext: true }
    ));
}

// A source that never spans the 1 s origin window cannot grow the window without bound.
#[test]
fn the_origin_window_is_byte_capped() {
    let fx = fixture(&Opts {
        lpcm: false,
        spu_tracks: 0,
        ..Opts::default()
    });
    let mut sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    let f = PesFrame {
        track: 1,
        pts: VIDEO_START_NS,
        keyframe: true,
        data: vec![0x55; 1 << 20],
        duration_ns: None,
        discard_padding_ns: 0,
        source: None,
        coding: None,
    };
    for _ in 0..(HOLD_CAP_BYTES >> 20) + 2 {
        sink.write(&f).unwrap();
    }
    assert!(sink.offset.is_some(), "the origin was never forced");
    assert!(sink.window_bytes <= HOLD_CAP_BYTES);
}

// A failed finish stays failed: a retry is not an Ok on a truncated stream.
#[test]
fn a_second_finish_after_an_error_is_an_error() {
    let fx = fixture(&Opts::default());
    let mut sink = MpgSink::create(Vec::new(), &fx.title).unwrap();
    assert!(sink.finish().is_err());
    assert!(sink.finish().is_err());
}

// One 0xBD entry in the map expands to the FMKV sub-streams once, however often it is listed.
#[test]
fn repeated_private_map_entries_do_not_multiply_tracks() {
    let mut e = vec![pack::PsmEntry {
        stream_type: 0x02,
        stream_id: 0xE0,
        descriptors: vec![],
    }];
    for _ in 0..40 {
        e.push(pack::PsmEntry {
            stream_type: 0x06,
            stream_id: 0xBD,
            descriptors: vec![],
        });
    }
    let subs: Vec<pack::SubStreamInfo> = (0x80..0x84)
        .map(|sub_id| pack::SubStreamInfo {
            sub_id,
            lang: *b"eng",
            forced: false,
        })
        .collect();
    let info = pack::fmkv_descriptors(&subs, None);
    let m = pack::psm(&info, &e).unwrap();
    let ps = scan_pack(&[(0xE0, scan_video())], Some(&m));
    let s = scan::scan_streams(&ps).unwrap();
    assert_eq!(s.len(), 1 + subs.len(), "{}", s.len());
}

// SD colour follows the line count as the DVD scan does: 480/240 is SMPTE 170M, 576/288
// BT.470BG, and only HD is BT.709.
#[test]
fn scanned_sd_video_takes_its_standards_colour() {
    let colour = |h: u16| {
        let mut v = seq_header(720, h, 3, 112, false);
        v.extend(test_es::mpeg2_pic(1, 3));
        let s = scan::scan_streams(&scan_pack(&[(0xE0, v)], None)).unwrap();
        match &s[0] {
            DiscStream::Video(v) => v.color_space,
            _ => unreachable!(),
        }
    };
    assert_eq!(colour(480), ColorSpace::Smpte170m);
    assert_eq!(colour(576), ColorSpace::Bt470bg);
    assert_eq!(colour(288), ColorSpace::Bt470bg);
    assert_eq!(colour(1080), ColorSpace::Bt709);
}

// A sequence_display_extension that signals BT.709 wins over the SD line count.
#[test]
fn scanned_sd_video_keeps_its_signalled_colour() {
    let mut v = seq_header(720, 480, 3, 112, false);
    v.extend([0, 0, 1, 0xB5, 0x23, 1, 1, 1, 0, 0, 0, 0]);
    v.extend(test_es::mpeg2_pic(1, 3));
    let s = scan::scan_streams(&scan_pack(&[(0xE0, v)], None)).unwrap();
    match &s[0] {
        DiscStream::Video(v) => assert_eq!(v.color_space, ColorSpace::Bt709),
        _ => unreachable!(),
    }
}
