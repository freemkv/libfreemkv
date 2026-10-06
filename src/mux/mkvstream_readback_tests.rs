use super::*;
use crate::pes::PesSource as _;
use std::io::Cursor;

const DISPLAY_UNIT: u32 = 0x54B2;

fn el(id: u32, body: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    ebml::write_id(&mut v, id).unwrap();
    ebml::write_size(&mut v, body.len() as u64).unwrap();
    v.extend_from_slice(body);
    v
}

// EditionFlagDefault is the two-byte ID 0x45DB mkvmerge writes; the default edition wins.
#[test]
fn the_default_edition_is_read_by_its_registered_id() {
    let atom = |name: &str| {
        let mut a = Vec::new();
        ebml::write_uint(&mut a, ebml::CHAPTER_TIME_START, 0).unwrap();
        let mut d = Vec::new();
        ebml::write_string(&mut d, ebml::CHAP_STRING, name).unwrap();
        a.extend(el(ebml::CHAPTER_DISPLAY, &d));
        el(ebml::CHAPTER_ATOM, &a)
    };
    let first = el(ebml::EDITION_ENTRY, &atom("first"));
    let mut second = vec![0x45, 0xDB, 0x81, 0x01];
    second.extend(atom("second"));
    let mut body = first;
    body.extend(el(ebml::EDITION_ENTRY, &second));
    let chapters = parse_chapters(&body).unwrap();
    assert_eq!(chapters.len(), 1);
    assert_eq!(chapters[0].name, "second");
}

fn encoding(order: u64, algo: u64, settings: &[u8]) -> Vec<u8> {
    let mut comp = Vec::new();
    ebml::write_uint(&mut comp, CONTENT_COMP_ALGO, algo).unwrap();
    if !settings.is_empty() {
        ebml::write_binary(&mut comp, CONTENT_COMP_SETTINGS, settings).unwrap();
    }
    let mut body = Vec::new();
    ebml::write_uint(&mut body, CONTENT_ENCODING_ORDER, order).unwrap();
    body.extend(el(CONTENT_COMPRESSION, &comp));
    el(CONTENT_ENCODING, &body)
}

fn parse_encodings(list: &[Vec<u8>]) -> io::Result<Encodings> {
    let body = list.concat();
    parse_content_encodings(&mut Cursor::new(&body), body.len() as u64)
}

#[test]
fn content_encodings_decode_highest_order_first() {
    let enc = parse_encodings(&[encoding(0, 0, &[]), encoding(1, 3, b"AB")]).unwrap();
    assert_eq!(
        enc.frames,
        vec![Decode::Prefix(b"AB".to_vec()), Decode::Zlib]
    );
}

#[test]
fn content_comp_algos_other_than_zlib_and_header_strip_are_undecodable() {
    for algo in [1, 2] {
        let enc = parse_encodings(&[encoding(0, algo, &[])]).unwrap();
        assert!(enc.undecodable, "algo {algo}");
        assert!(enc.frames.is_empty());
    }
}

#[test]
fn content_encodings_per_track_are_capped() {
    let n = MAX_CONTENT_ENCODINGS as u64;
    let list = |n| (0..n).map(|o| encoding(o, 0, &[])).collect::<Vec<_>>();
    assert!(parse_encodings(&list(n)).is_ok());
    assert!(parse_encodings(&list(n + 1)).is_err());
}

#[test]
fn display_unit_unknown_ignores_the_declared_display_size() {
    let meta = |unit| VideoMeta {
        pixel_width: 1920,
        pixel_height: 1080,
        display_width: Some(4),
        display_height: Some(3),
        display_unit: unit,
        ..VideoMeta::default()
    };
    let res = Resolution::R1080p;
    assert_eq!(meta(0).display_aspect(res), Some((4, 3)));
    assert_eq!(meta(DISPLAY_UNIT_UNKNOWN).display_aspect(res), None);
}

#[test]
fn primaries_map_to_their_color_spaces() {
    assert_eq!(color_space_from_primaries(5), ColorSpace::Bt470bg);
    assert_eq!(color_space_from_primaries(6), ColorSpace::Smpte170m);
}

#[test]
fn default_duration_matches_only_within_a_hundredth_of_a_percent() {
    for (ns, want) in [
        (41_708_333, FrameRate::F23_976),
        (41_666_667, FrameRate::F24),
        (33_366_667, FrameRate::F29_97),
        (33_333_333, FrameRate::F30),
        (16_683_350, FrameRate::F59_94),
        (16_666_667, FrameRate::F60),
    ] {
        assert_eq!(frame_rate_from_ns(ns), want, "{ns} ns");
    }
}

fn uint(id: u32, val: u64) -> Vec<u8> {
    let mut v = Vec::new();
    ebml::write_uint(&mut v, id, val).unwrap();
    v
}

fn string(id: u32, val: &str) -> Vec<u8> {
    let mut v = Vec::new();
    ebml::write_string(&mut v, id, val).unwrap();
    v
}

fn entry(tnum: u64, ttype: u64, codec_id: &str, extra: &[u8]) -> Vec<u8> {
    let body = [
        uint(ebml::TRACK_NUMBER, tnum),
        uint(ebml::TRACK_TYPE, ttype),
        string(ebml::CODEC_ID, codec_id),
        extra.to_vec(),
    ]
    .concat();
    el(ebml::TRACK_ENTRY, &body)
}

fn mkv(entries: &[Vec<u8>], cluster: &[u8]) -> Vec<u8> {
    let mut out = el(ebml::EBML, &[]);
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    out.extend(el(ebml::INFO, &[]));
    out.extend(el(ebml::TRACKS, &entries.concat()));
    out.extend_from_slice(cluster);
    out
}

fn open(bytes: Vec<u8>) -> MkvStream {
    MkvStream::open(Cursor::new(bytes)).expect("opens")
}

fn video_with(video_children: &[u8], extra: &[u8]) -> VideoStream {
    let body = [el(ebml::VIDEO, video_children), extra.to_vec()].concat();
    let s = open(mkv(&[entry(1, 1, ebml::CODEC_H264, &body)], &[]));
    match &s.info().streams[0] {
        Stream::Video(v) => v.clone(),
        other => panic!("expected video, got {other:?}"),
    }
}

fn dims(w: u64, h: u64) -> Vec<u8> {
    [uint(ebml::PIXEL_WIDTH, w), uint(ebml::PIXEL_HEIGHT, h)].concat()
}

#[test]
fn a_scope_frame_keeps_its_shape_on_remux() {
    let v = video_with(&dims(1920, 800), &[]);
    let t = MkvTrack::video(&v);
    assert_eq!(
        u64::from(t.display_width) * 800,
        u64::from(t.display_height) * 1920,
        "display {}x{} must keep the 2.4:1 source shape",
        t.display_width,
        t.display_height
    );
}

#[test]
fn an_anamorphic_display_size_survives_remux() {
    let video = [
        dims(720, 576),
        uint(ebml::DISPLAY_WIDTH, 1024),
        uint(ebml::DISPLAY_HEIGHT, 576),
    ]
    .concat();
    let t = MkvTrack::video(&video_with(&video, &[]));
    assert_eq!((t.display_width, t.display_height), (1024, 576));
}

#[test]
fn a_dar_unit_display_size_is_read_as_a_ratio() {
    let video = [
        dims(720, 480),
        uint(ebml::DISPLAY_WIDTH, 4),
        uint(ebml::DISPLAY_HEIGHT, 3),
        uint(DISPLAY_UNIT, 3),
    ]
    .concat();
    let t = MkvTrack::video(&video_with(&video, &[]));
    assert_eq!((t.display_width, t.display_height), (640, 480));
}

#[test]
fn square_pixel_hd_declares_no_display_aspect() {
    assert_eq!(video_with(&dims(1920, 1080), &[]).display_aspect, None);
}

#[test]
fn an_absurd_pixel_height_is_not_truncated_to_a_real_one() {
    let v = video_with(&dims(1920, (1u64 << 32) + 1080), &[]);
    assert_ne!(v.resolution, Resolution::R1080p);
}

fn colour(m: u64, t: u64, p: u64, r: u64) -> Vec<u8> {
    let body = [
        uint(ebml::MATRIX_COEFFICIENTS, m),
        uint(ebml::TRANSFER_CHARACTERISTICS, t),
        uint(ebml::PRIMARIES, p),
        uint(ebml::RANGE, r),
    ]
    .concat();
    el(ebml::COLOUR, &body)
}

#[test]
fn hdr10_colour_is_read_back_and_rewritten() {
    let video = [dims(3840, 2160), colour(9, 16, 9, 1)].concat();
    let v = video_with(&video, &[]);
    assert_eq!(
        v.measured_cicp,
        Some(MeasuredCicp {
            matrix: 9,
            transfer: 16,
            primaries: 9,
            range: 1
        })
    );
    assert_eq!(v.hdr, HdrFormat::Hdr10);
    assert_eq!(v.color_space, ColorSpace::Bt2020);
    assert_eq!(super::super::mkv::cicp_for_video(&v), (9, 16, 9, 1));
}

#[test]
fn an_hlg_transfer_reads_as_hlg() {
    let video = [dims(3840, 2160), colour(9, 18, 9, 1)].concat();
    assert_eq!(video_with(&video, &[]).hdr, HdrFormat::Hlg);
}

#[test]
fn no_colour_element_measures_nothing() {
    let v = video_with(&dims(1920, 1080), &[]);
    assert_eq!(v.measured_cicp, None);
    assert_eq!(v.hdr, HdrFormat::Sdr);
}

#[test]
fn default_duration_is_read_back_as_the_frame_rate() {
    for (ns, rate) in [
        (41_708_333, FrameRate::F23_976),
        (40_000_000, FrameRate::F25),
        (16_683_333, FrameRate::F59_94),
    ] {
        let extra = uint(ebml::DEFAULT_DURATION, ns);
        let v = video_with(&dims(1920, 1080), &extra);
        assert_eq!(v.frame_rate, rate, "{ns} ns");
        assert_eq!(MkvTrack::video(&v).default_duration_ns, ns);
    }
    let odd = uint(ebml::DEFAULT_DURATION, 12_345_678);
    assert_eq!(
        video_with(&dims(1920, 1080), &odd).frame_rate,
        FrameRate::Unknown
    );
}

// ContentEncodings (RFC 9559 5.1.4.1.31): header stripping and zlib are undone on read.
const CONTENT_ENCODINGS: u32 = 0x6D80;
const CONTENT_ENCODING: u32 = 0x6240;
const CONTENT_ENCODING_SCOPE: u32 = 0x5032;
const CONTENT_ENCODING_TYPE: u32 = 0x5033;
const CONTENT_COMPRESSION: u32 = 0x5034;
const CONTENT_COMP_ALGO: u32 = 0x4254;
const CONTENT_COMP_SETTINGS: u32 = 0x4255;
const CONTENT_ENCRYPTION: u32 = 0x5035;

fn encodings(encoding_children: &[u8]) -> Vec<u8> {
    el(CONTENT_ENCODINGS, &el(CONTENT_ENCODING, encoding_children))
}

fn compression(algo: Option<u64>, settings: &[u8]) -> Vec<u8> {
    let mut body = algo.map_or_else(Vec::new, |a| uint(CONTENT_COMP_ALGO, a));
    if !settings.is_empty() {
        ebml::write_binary(&mut body, CONTENT_COMP_SETTINGS, settings).unwrap();
    }
    el(CONTENT_COMPRESSION, &body)
}

fn one_block_cluster(track: u8, payload: &[u8]) -> Vec<u8> {
    let mut block = vec![0x80 | track, 0x00, 0x00, 0x80];
    block.extend_from_slice(payload);
    let mut c = el(ebml::CLUSTER, &[]);
    c.truncate(c.len() - 1);
    ebml::write_unknown_size(&mut c).unwrap();
    c.extend(uint(ebml::CLUSTER_TIMESTAMP, 0));
    c.extend(el(ebml::SIMPLE_BLOCK, &block));
    c
}

fn zlib(data: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

#[test]
fn header_stripped_frames_get_their_header_back() {
    let enc = encodings(&compression(Some(3), &[0x0B, 0x77]));
    let bytes = mkv(
        &[entry(1, 2, ebml::CODEC_AC3, &enc)],
        &one_block_cluster(1, &[0x11, 0x22]),
    );
    let frames = drain_all(&mut open(bytes));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, vec![0x0B, 0x77, 0x11, 0x22]);
}

#[test]
fn zlib_compressed_vobsub_frames_are_inflated() {
    let packet = b"a vobsub packet, repeated repeated repeated".to_vec();
    let enc = encodings(&compression(None, &[]));
    let bytes = mkv(
        &[entry(1, 17, ebml::CODEC_VOBSUB, &enc)],
        &one_block_cluster(1, &zlib(&packet)),
    );
    let frames = drain_all(&mut open(bytes));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, packet);
}

#[test]
fn a_zlib_scoped_codec_private_is_inflated() {
    let idx = b"size: 720x480\npalette: 000000".to_vec();
    let mut extra =
        encodings(&[uint(CONTENT_ENCODING_SCOPE, 3), compression(Some(0), &[])].concat());
    ebml::write_binary(&mut extra, ebml::CODEC_PRIVATE, &zlib(&idx)).unwrap();
    let s = open(mkv(&[entry(1, 17, ebml::CODEC_VOBSUB, &extra)], &[]));
    assert_eq!(s.codec_private(0), Some(idx));
}

#[test]
fn an_encrypted_track_is_dropped_and_its_blocks_counted() {
    let enc = encodings(&[uint(CONTENT_ENCODING_TYPE, 1), el(CONTENT_ENCRYPTION, &[])].concat());
    let bytes = mkv(
        &[entry(1, 2, ebml::CODEC_AC3, &enc)],
        &one_block_cluster(1, &[0xDE, 0xAD]),
    );
    let mut s = open(bytes);
    assert!(
        s.info().streams.is_empty(),
        "no stream for an undecodable track"
    );
    assert!(drain_all(&mut s).is_empty());
    assert_eq!(s.errors(), 1);
    assert!(s.lost_bytes() > 0);
}

#[test]
fn a_zlib_bomb_is_refused_past_the_block_cap() {
    let big = zlib(&vec![0u8; 4096]);
    assert!(inflate_capped(&big, 4096).is_ok());
    assert!(inflate_capped(&big, 4095).is_err());
}

// A BitDepth-less PCM track next to a flood of empty blocks of another track.
#[test]
fn a_pcm_depth_probe_stops_buffering_at_a_frame_cap() {
    let n = 3 * PCM_PROBE_MAX_FRAMES;
    let mut c = el(ebml::CLUSTER, &[]);
    c.truncate(c.len() - 1);
    ebml::write_unknown_size(&mut c).unwrap();
    c.extend(uint(ebml::CLUSTER_TIMESTAMP, 0));
    for _ in 0..n {
        c.extend(el(ebml::SIMPLE_BLOCK, &[0x82, 0x00, 0x00, 0x80]));
    }
    let bytes = mkv(
        &[
            entry(1, 2, ebml::CODEC_PCM_LE, &[]),
            entry(2, 2, ebml::CODEC_AC3, &[]),
        ],
        &c,
    );
    let s = open(bytes);
    let Mode::Read(rs) = &s.mode else {
        panic!("read mode")
    };
    assert!(
        rs.pending.len() <= PCM_PROBE_MAX_FRAMES,
        "{} frames buffered by the probe",
        rs.pending.len()
    );
}

#[test]
fn a_tracks_element_past_the_entry_cap_is_rejected() {
    let entries: Vec<_> = (1..=MAX_TRACK_ENTRIES as u64 + 1)
        .map(|n| entry(n, 99, "X", &[]))
        .collect();
    let e = MkvStream::open(Cursor::new(mkv(&entries, &[]))).err();
    assert!(e.is_some(), "{} entries accepted", entries.len());
    let at_cap: Vec<_> = (1..=MAX_TRACK_ENTRIES as u64)
        .map(|n| entry(n, 99, "X", &[]))
        .collect();
    assert!(MkvStream::open(Cursor::new(mkv(&at_cap, &[]))).is_ok());
}

#[test]
fn a_duplicate_track_number_is_rejected() {
    let entries = [
        entry(1, 2, ebml::CODEC_AC3, &[]),
        entry(1, 2, ebml::CODEC_DTS, &[]),
    ];
    assert!(MkvStream::open(Cursor::new(mkv(&entries, &[]))).is_err());
}

#[test]
fn a_high_track_number_on_a_dropped_track_type_does_not_fail_the_file() {
    let entries = [
        entry(1, 1, ebml::CODEC_H264, &[]),
        entry(0x1000, 0x21, "D_WEBVTT/METADATA", &[]),
    ];
    let s = MkvStream::open(Cursor::new(mkv(&entries, &[]))).expect("opens");
    assert_eq!(s.info().streams.len(), 1);
}

fn audio_language(extra: &[u8]) -> String {
    let s = open(mkv(&[entry(1, 2, ebml::CODEC_AC3, extra)], &[]));
    match &s.info().streams[0] {
        Stream::Audio(a) => a.language.clone(),
        other => panic!("expected audio, got {other:?}"),
    }
}

#[test]
fn a_missing_language_reads_as_the_matroska_default_english() {
    assert_eq!(audio_language(&[]), "eng");
}

#[test]
fn language_bcp47_overrides_the_legacy_language() {
    let extra = [
        string(ebml::LANGUAGE, "eng"),
        string(LANGUAGE_BCP47, "fr-CA"),
    ]
    .concat();
    assert_eq!(audio_language(&extra), "fra");
    assert_eq!(audio_language(&string(LANGUAGE_BCP47, "deu")), "deu");
    assert_eq!(audio_language(&string(LANGUAGE_BCP47, "x-klingon")), "und");
}

fn probe_info(info: &[u8]) -> MkvProbe {
    let mut out = el(ebml::EBML, &[]);
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    out.extend(el(ebml::INFO, info));
    out.extend(el(ebml::TRACKS, &entry(1, 1, ebml::CODEC_H264, &[])));
    probe_mkv(Cursor::new(out)).unwrap()
}

fn duration(ticks: f64) -> Vec<u8> {
    el(ebml::DURATION, &ticks.to_be_bytes())
}

#[test]
fn a_zero_timestamp_scale_times_the_duration_like_the_frames() {
    let p = probe_info(&[uint(ebml::TIMESTAMP_SCALE, 0), duration(5000.0)].concat());
    assert_eq!(p.timestamp_scale, 1_000_000);
    assert_eq!(p.duration_secs, Some(5.0));
}

#[test]
fn a_non_finite_or_negative_duration_is_absent() {
    for bad in [f64::NAN, f64::INFINITY, -1.0] {
        assert_eq!(probe_info(&duration(bad)).duration_secs, None, "{bad}");
    }
}

fn cluster_of(blocks: &[&[u8]]) -> Vec<u8> {
    let mut c = el(ebml::CLUSTER, &[]);
    c.truncate(c.len() - 1);
    ebml::write_unknown_size(&mut c).unwrap();
    c.extend(uint(ebml::CLUSTER_TIMESTAMP, 0));
    for b in blocks {
        c.extend(el(ebml::SIMPLE_BLOCK, b));
    }
    c
}

#[test]
fn a_long_form_track_number_vint_is_decoded() {
    let block = [0x08, 0, 0, 0, 1, 0x00, 0x00, 0x80, 0xAB];
    let bytes = mkv(&[entry(1, 2, ebml::CODEC_AC3, &[])], &cluster_of(&[&block]));
    let frames = drain_all(&mut open(bytes));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, [0xAB]);
}

#[test]
fn a_malformed_block_is_counted_not_silently_skipped() {
    let bytes = mkv(
        &[entry(1, 2, ebml::CODEC_AC3, &[])],
        &cluster_of(&[
            &[0x81, 0x00],
            &[0x80, 0, 0, 0x80, 1],
            &[0x81, 0, 0, 0x80, 2],
        ]),
    );
    let mut s = open(bytes);
    assert_eq!(drain_all(&mut s).len(), 1);
    assert_eq!(s.errors(), 2);
    assert_eq!(s.lost_bytes(), 7);
}

#[test]
fn a_sampling_frequency_maps_only_to_its_own_standard_rate() {
    for (hz, want) in [
        (48000.0, SampleRate::S48),
        (47999.99, SampleRate::S48),
        (44100.0, SampleRate::S44_1),
        (96000.0, SampleRate::S96),
        (64000.0, SampleRate::Unknown),
        (128000.0, SampleRate::Unknown),
        (32000.0, SampleRate::Unknown),
    ] {
        let audio = el(
            ebml::AUDIO,
            &el(ebml::SAMPLING_FREQUENCY, &f64::to_be_bytes(hz)),
        );
        let s = open(mkv(&[entry(1, 2, ebml::CODEC_AC3, &audio)], &[]));
        let Stream::Audio(a) = &s.info().streams[0] else {
            panic!("audio")
        };
        assert_eq!(a.sample_rate, want, "{hz}");
    }
}

#[test]
fn a_corrupt_zlib_frame_is_dropped_and_counted_not_fatal() {
    let good = b"a vobsub packet".to_vec();
    let mut bad_block = vec![0x81, 0x00, 0x00, 0x80];
    bad_block.extend_from_slice(&[0x78, 0x9C, 0xFF, 0xFF, 0xFF]);
    let mut good_block = vec![0x81, 0x00, 0x00, 0x80];
    good_block.extend(zlib(&good));
    let enc = encodings(&compression(None, &[]));
    let bytes = mkv(
        &[entry(1, 17, ebml::CODEC_VOBSUB, &enc)],
        &cluster_of(&[&bad_block, &good_block]),
    );
    let mut s = open(bytes);
    let frames = drain_all(&mut s);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, good);
    assert_eq!(s.errors(), 1);
    assert_eq!(s.lost_bytes(), 5);
}

// The decode cap is one budget per block: two laced frames that each fit the cap
// alone cannot both inflate (a 256-frame block would otherwise hold 256x the cap).
#[test]
fn laced_zlib_frames_share_one_block_budget() {
    let frame = zlib(&vec![0u8; MAX_BLOCK_SIZE as usize * 5 / 8]);
    // Fixed-size lacing (flags 0x04): count-1, then equal-size frames.
    let mut block = vec![0x81, 0x00, 0x00, 0x84, 0x01];
    block.extend_from_slice(&frame);
    block.extend_from_slice(&frame);
    let enc = encodings(&compression(None, &[]));
    let bytes = mkv(
        &[entry(1, 17, ebml::CODEC_VOBSUB, &enc)],
        &cluster_of(&[&block]),
    );
    let mut s = open(bytes);
    let frames = drain_all(&mut s);
    assert_eq!(frames.len(), 1, "the second frame is past the block budget");
    assert_eq!(s.errors(), 1);
}

#[test]
fn a_corrupt_compressed_codec_private_drops_the_track_not_the_file() {
    let mut extra =
        encodings(&[uint(CONTENT_ENCODING_SCOPE, 2), compression(Some(0), &[])].concat());
    ebml::write_binary(&mut extra, ebml::CODEC_PRIVATE, &[0x78, 0x9C, 0xFF]).unwrap();
    let entries = [
        entry(1, 1, ebml::CODEC_H264, &[]),
        entry(2, 17, ebml::CODEC_VOBSUB, &extra),
    ];
    let s = MkvStream::open(Cursor::new(mkv(&entries, &[]))).expect("file still opens");
    assert_eq!(s.info().streams.len(), 1);
    let p = probe_mkv(Cursor::new(mkv(&entries, &[]))).expect("probe still works");
    assert_eq!(p.tracks.len(), 2);
}

#[test]
fn a_block_group_of_a_dropped_track_is_counted_once() {
    let enc = encodings(&[uint(CONTENT_ENCODING_TYPE, 1), el(CONTENT_ENCRYPTION, &[])].concat());
    let group = [
        el(ebml::BLOCK, &[0x81, 0x00, 0x00, 0x00, 0xDE]),
        el(ebml::BLOCK_ADDITIONS, &[0u8; 4]),
    ]
    .concat();
    let mut c = el(ebml::CLUSTER, &[]);
    c.truncate(c.len() - 1);
    ebml::write_unknown_size(&mut c).unwrap();
    c.extend(uint(ebml::CLUSTER_TIMESTAMP, 0));
    c.extend(el(ebml::BLOCK_GROUP, &group));
    let mut s = open(mkv(&[entry(1, 2, ebml::CODEC_AC3, &enc)], &c));
    assert!(drain_all(&mut s).is_empty());
    assert_eq!(s.errors(), 1);
}

#[test]
fn a_cropped_frame_without_display_size_takes_the_cropped_shape() {
    let video = [
        dims(1920, 1080),
        uint(PIXEL_CROP_TOP, 140),
        uint(PIXEL_CROP_BOTTOM, 140),
    ]
    .concat();
    assert_eq!(video_with(&video, &[]).display_aspect, Some((12, 5)));
}

fn drain_all(s: &mut MkvStream) -> Vec<crate::pes::PesFrame> {
    let mut out = Vec::new();
    while let Some(f) = s.read().expect("no read error") {
        out.push(f);
    }
    out
}
