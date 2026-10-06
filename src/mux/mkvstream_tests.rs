// Empty-body fixed lace declares n frames, carries none: must be rejected,
// not silently yield zero frames (0 % n == 0 passes divisibility, and
// `chunks` on an empty slice yields nothing — the lace would vanish).
#[test]
fn fixed_lacing_with_an_empty_body_is_malformed_not_zero_frames() {
    // Lacing Head only: count_minus_one = 2, i.e. three frames declared,
    // followed by no payload at all.
    assert_eq!(super::split_lacing(super::LACING_FIXED, &[2u8]), None);
    // Same shape for a single declared frame.
    assert_eq!(super::split_lacing(super::LACING_FIXED, &[0u8]), None);
}

/// The non-degenerate fixed lace still splits evenly, so the guard above did
/// not tighten the valid case.
#[test]
fn fixed_lacing_splits_an_evenly_divisible_body() {
    let laced = super::split_lacing(super::LACING_FIXED, &[2u8, 1, 2, 3, 4, 5, 6])
        .expect("three 2-byte frames is a well-formed fixed lace");
    assert_eq!(laced, vec![&[1u8, 2][..], &[3, 4][..], &[5, 6][..]]);
}
use super::*;
use crate::pes::{PesSink as _, PesSource as _};
use std::io::Cursor;

/// Length-prefix (4-byte big-endian) each NAL, as the H.264 parser emits.
fn lp(nals: &[&[u8]]) -> Vec<u8> {
    let mut v = Vec::new();
    for n in nals {
        v.extend_from_slice(&(n.len() as u32).to_be_bytes());
        v.extend_from_slice(n);
    }
    v
}

fn mvc_frame(track: usize, pts: i64, keyframe: bool, data: Vec<u8>) -> crate::pes::PesFrame {
    crate::pes::PesFrame {
        discard_padding_ns: 0,
        track,
        pts,
        keyframe,
        data,
        duration_ns: None,
        source: None,
        coding: None,
    }
}

// A subset SPS (NAL type 15), a PPS (type 8), and a coded-slice-extension
// (type 20) — the shape of a dependent-view access unit.
const SUBSET_SPS: [u8; 5] = [0x6F, 0x80, 0x00, 0x33, 0xAA]; // 0x6F & 0x1F = 15
const DEP_PPS: [u8; 3] = [0x68, 0xEE, 0x3C]; // 0x68 & 0x1F = 8
const DEP_SLICE: [u8; 3] = [0x74, 0x11, 0x22]; // 0x74 & 0x1F = 20

#[test]
fn extract_mvc_params_finds_subset_sps_and_pps() {
    let data = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);
    let (s, p) = extract_mvc_params(&data).expect("both param sets present");
    assert_eq!(s, SUBSET_SPS, "subset SPS (NAL 15) captured verbatim");
    assert_eq!(p, DEP_PPS, "PPS (NAL 8) captured verbatim");
    // Missing PPS → None (the serializer then emits no mvcC mapping).
    assert!(extract_mvc_params(&lp(&[&SUBSET_SPS, &DEP_SLICE])).is_none());
    // Missing subset SPS → None.
    assert!(extract_mvc_params(&lp(&[&DEP_PPS, &DEP_SLICE])).is_none());
}

fn empty_merge() -> MvcMerge {
    MvcMerge {
        base_stream_idx: 0,
        dep_stream_idx: 2,
        base_track_idx: 0,
        stream_to_track: vec![Some(0), Some(1), None],
        pending_base: std::collections::VecDeque::new(),
        dep_by_pts: std::collections::HashMap::new(),
        captured_params: None,
        orphan_deps: 0,
    }
}

#[test]
fn mvc_merge_pairs_base_and_dependent_by_pts() {
    let mut m = empty_merge();
    let dep = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);

    // Base arrives first (SSIF order): buffered, nothing emitted yet.
    let e = m.ingest(&mvc_frame(0, 100, true, lp(&[&[0x65, 1, 2]])));
    assert!(e.is_empty(), "base held until its dependent arrives");

    // Dependent arrives → base is emitted, remapped to the base track, with
    // the dependent AU as its BlockAdditional; params are captured.
    let e = m.ingest(&mvc_frame(2, 100, false, dep.clone()));
    assert_eq!(e.len(), 1, "the paired base frame is emitted");
    assert_eq!(e[0].0.track, 0, "remapped to the base muxer track");
    assert_eq!(
        e[0].1.as_deref(),
        Some(dep.as_slice()),
        "dependent attached"
    );
    assert!(m.captured_params.is_some(), "mvcC params captured");

    // Audio passes straight through (remapped, no additional).
    let e = m.ingest(&mvc_frame(1, 100, true, vec![0xAA]));
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].0.track, 1);
    assert!(e[0].1.is_none());

    // Dependent-before-base (reordered) also pairs.
    let dep2 = lp(&[&DEP_SLICE]);
    assert!(m.ingest(&mvc_frame(2, 200, false, dep2.clone())).is_empty());
    let e = m.ingest(&mvc_frame(0, 200, false, lp(&[&[0x61, 3, 4]])));
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].1.as_deref(), Some(dep2.as_slice()));
}

#[test]
fn mvc_merge_flushes_unpaired_base_at_eof() {
    let mut m = empty_merge();
    // Base with no dependent ever → held, then flushed unpaired at EOF.
    assert!(
        m.ingest(&mvc_frame(0, 10, true, vec![0, 0, 0, 1]))
            .is_empty()
    );
    let tail = m.flush();
    assert_eq!(tail.len(), 1, "unpaired base still emitted");
    assert!(tail[0].1.is_none(), "no BlockAdditional when unpaired");
}

#[test]
fn extract_mvc_params_no_panic_on_truncated_or_empty() {
    // Empty, sub-header, zero-length NAL, and a length prefix claiming more
    // than is present must all return None without panicking (untrusted AU).
    assert!(extract_mvc_params(&[]).is_none());
    assert!(extract_mvc_params(&[0, 0, 0]).is_none());
    assert!(
        extract_mvc_params(&[0, 0, 0, 0]).is_none(),
        "lone zero-length NAL yields no params"
    );
    assert!(
        extract_mvc_params(&[0, 0, 0, 10, 0x6F]).is_none(),
        "length prefix past end breaks, no slice panic"
    );
    // A zero-length NAL is SKIPPED, not fatal: valid param sets that follow
    // are still found (a stray length prefix must not abandon the whole AU).
    let mut d = vec![0, 0, 0, 0];
    d.extend_from_slice(&lp(&[&SUBSET_SPS, &DEP_PPS]));
    let (s, p) = extract_mvc_params(&d).expect("params found past the zero-length NAL");
    assert_eq!(s, SUBSET_SPS);
    assert_eq!(p, DEP_PPS);
}

#[test]
fn mvc_merge_flushes_oldest_base_once_past_window() {
    let mut m = empty_merge();
    // Push more unpaired base frames than the window; the excess flush as
    // plain (unpaired) blocks in FIFO order once len exceeds MVC_PAIR_WINDOW.
    let n = MVC_PAIR_WINDOW + 8;
    let mut emitted = 0usize;
    for pts in 0..n {
        emitted += m
            .ingest(&mvc_frame(0, pts as i64, false, vec![0, 0, 0, 1]))
            .len();
    }
    assert_eq!(emitted, 8, "the {n} bases beyond the window flush unpaired");
    assert_eq!(m.pending_base.len(), MVC_PAIR_WINDOW, "window still held");
    assert!(m.flush().iter().all(|(_, add)| add.is_none()));
}

// The base parser drops a leading picture it cannot decode; the dependent access unit
// of that PTS pairs with no base and is never written.
#[test]
fn mvc_merge_drops_the_dependent_of_a_dropped_base() {
    let mut m = empty_merge();
    assert!(
        m.ingest(&mvc_frame(2, 1, false, lp(&[&DEP_SLICE])))
            .is_empty()
    );
    let dep = lp(&[&DEP_SLICE, &DEP_PPS]);
    assert!(m.ingest(&mvc_frame(2, 3, false, dep.clone())).is_empty());
    let e = m.ingest(&mvc_frame(0, 3, true, vec![0x65, 1]));
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].1.as_deref(), Some(dep.as_slice()));
    assert!(m.flush().is_empty());
    assert_eq!(m.orphan_deps, 1);
}

#[test]
fn mvc_merge_dep_overflow_drops_old_keeps_newest() {
    let mut m = empty_merge();
    // Fill dep_by_pts to the bound with unpaired dependents (unique PTS).
    for pts in 0..(MVC_PAIR_WINDOW * 4) {
        assert!(
            m.ingest(&mvc_frame(2, pts as i64, false, lp(&[&DEP_SLICE])))
                .is_empty()
        );
    }
    assert_eq!(m.dep_by_pts.len(), MVC_PAIR_WINDOW * 4);
    // One more overflows: the drifted buffer is cleared BUT the newest survives
    // so its (soon-to-arrive) base can still pair.
    let dep_new = lp(&[&DEP_SLICE]);
    m.ingest(&mvc_frame(2, 9_999, false, dep_new.clone()));
    assert_eq!(m.dep_by_pts.len(), 1, "old cleared, newest kept");
    assert!(m.dep_by_pts.contains_key(&9_999));
    assert_eq!(
        m.orphan_deps,
        (MVC_PAIR_WINDOW * 4) as u64,
        "old buffer counted once"
    );
    // The surviving dependent pairs with its base.
    let e = m.ingest(&mvc_frame(0, 9_999, false, vec![0x61, 1]));
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].1.as_deref(), Some(dep_new.as_slice()));
}

#[test]
fn create_does_not_panic_when_only_video_is_mvc_dependent() {
    // A (malformed / hand-built) title whose single video IS the dependent
    // must NOT panic: base_stream_idx is None, so no merge is set up and the
    // dependent is muxed as an ordinary track.
    use crate::disc::{
        Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    let dep = VideoStream {
        pid: 0x1012,
        codec: Codec::H264,
        resolution: Resolution::R1080p,
        frame_rate: FrameRate::F24,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt709,
        display_aspect: None,
        secondary: true,
        label: crate::disc::MVC_DEPENDENT_LABEL.to_string(),
        measured_cicp: None,
    };
    let title = DiscTitle {
        streams: vec![Stream::Video(dep)],
        ..DiscTitle::empty()
    };
    let s = MkvStream::create(Box::new(Cursor::new(Vec::new())), &title, None)
        .expect("create must succeed, not panic");
    assert!(
        s.mvc.is_none(),
        "no merge when there is no distinct base view"
    );
    // And with no merge to fold it into, the dependent is muxed as an
    // ORDINARY track — it must not be skipped as though a base existed to
    // carry it, which would leave the title with no video track at all.
    assert_eq!(
        pending_tracks(&s).len(),
        1,
        "the lone dependent view still gets its own track"
    );
}

// Blu-ray 3D title: base (0), MVC dependent (1) and AC-3 audio (2).
fn mvc_title() -> crate::disc::DiscTitle {
    use crate::disc::{
        AudioStream, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    let view = |mvc_dependent: bool| {
        Stream::Video(VideoStream {
            pid: if mvc_dependent { 0x1012 } else { 0x1011 },
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: mvc_dependent,
            label: if mvc_dependent {
                crate::disc::MVC_DEPENDENT_LABEL.to_string()
            } else {
                String::new()
            },
            measured_cicp: None,
        })
    };
    DiscTitle {
        streams: vec![
            view(false),
            view(true),
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::Ac3,
                channels: crate::disc::AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: crate::disc::SampleRate::S48,
                secondary: false,
                purpose: crate::disc::LabelPurpose::Normal,
                label: String::new(),
            }),
        ],
        ..DiscTitle::empty()
    }
}

// write()-level: an audio frame on a 3D title takes the pass-through fast
// path and lands on its remapped muxer track with its payload intact.
#[test]
fn mvc_title_audio_passes_through_to_its_remapped_track() {
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &mvc_title(), None).unwrap();
    s.write(&av_frame(0, 0, true, vec![0x11; 16])).unwrap();
    s.write(&av_frame(2, 10_000_000, true, vec![0xA5; 8]))
        .unwrap();
    s.write(&av_frame(2, 42_000_000, true, vec![0xA6; 8]))
        .unwrap();
    s.finish().unwrap();
    let back = drain(&mut MkvStream::open(Cursor::new(out.bytes())).unwrap());
    let audio: Vec<_> = back.iter().filter(|f| f.track == 1).collect();
    assert_eq!(audio.len(), 2);
    assert_eq!(audio[0].data, vec![0xA5; 8]);
    assert_eq!(audio[1].pts, 42_000_000);
}

// End to end 3D: the written file declares the mvcC mapping and carries each
// dependent AU as a BlockAdditional on its paired base frame.
#[test]
fn a_3d_title_writes_the_mvc_mapping_and_block_additions() {
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &mvc_title(), None).unwrap();
    let dep = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);
    for (i, pts) in [0i64, 41_708_333].into_iter().enumerate() {
        s.write(&mvc_frame(0, pts, i == 0, vec![0x65, 0x88, i as u8]))
            .unwrap();
        s.write(&mvc_frame(1, pts, i == 0, dep.clone())).unwrap();
    }
    s.finish().unwrap();
    let bytes = out.bytes();
    let has = |needle: &[u8]| bytes.windows(needle.len()).any(|w| w == needle);
    assert!(has(&[0x41, 0xE4]), "BlockAdditionMapping declared");
    assert!(has(&SUBSET_SPS), "mvcC carries the dependent subset SPS");
    let mut r = MkvStream::open(Cursor::new(bytes)).unwrap();
    let base: Vec<_> = drain(&mut r).into_iter().filter(|f| f.track == 0).collect();
    assert_eq!(base.len(), 2, "both base frames written once");
    assert_eq!(r.errors(), 2, "one BlockAdditions per paired base frame");
    assert!(r.lost_bytes() >= 2 * dep.len() as u64);
}

// set_codec_private takes a stream index: through the MVC map before the header
// (pending track) and after it (a reserved AAC CodecPrivate filled in place).
#[test]
fn set_codec_private_translates_stream_indices_on_a_3d_title() {
    let mut title = mvc_title();
    if let crate::disc::Stream::Audio(a) = &mut title.streams[2] {
        a.codec = Codec::Aac;
    }
    let s = MkvStream::create(Box::new(Cursor::new(Vec::new())), &title, None).unwrap();
    let mut s = s;
    assert!(
        !s.set_codec_private(1, &[9]).unwrap(),
        "the dependent has no track"
    );
    assert!(s.set_codec_private(0, &[1, 2]).unwrap());
    assert_eq!(
        pending_tracks(&s)[0].codec_private.as_deref(),
        Some(&[1, 2][..])
    );
    assert_eq!(pending_tracks(&s)[1].codec_private, None);

    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    let dep = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);
    s.write(&mvc_frame(0, 0, true, vec![0x65, 0x88])).unwrap();
    s.write(&mvc_frame(1, 0, true, dep)).unwrap();
    assert!(matches!(s.mode, Mode::Write(WriteMode::Active(_))));
    let asc = [0x11, 0x90];
    assert!(
        s.set_codec_private(2, &asc).unwrap(),
        "audio stream 2 is track 1"
    );
    s.finish().unwrap();
    let r = MkvStream::open(Cursor::new(out.bytes())).unwrap();
    assert_eq!(r.codec_private(1), Some(asc.to_vec()));
}

// A 16-bit LPCM source stays 16-bit: BitDepth follows the parser's declared depth.
#[test]
fn lpcm_bit_depth_follows_the_parser_codec_private() {
    let mut title = h264_title();
    for cp in [
        None,
        Some(b"DVLP\x10".to_vec()),
        Some(b"BDLP\x31\x18".to_vec()),
    ] {
        title
            .streams
            .push(crate::disc::Stream::Audio(crate::disc::AudioStream {
                pid: 0x1100,
                codec: crate::disc::Codec::Lpcm,
                channels: crate::disc::AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: crate::disc::SampleRate::S48,
                secondary: false,
                purpose: crate::disc::LabelPurpose::Normal,
                label: String::new(),
            }));
        title.codec_privates.push(cp);
    }
    let s = MkvStream::create(Box::new(Cursor::new(Vec::new())), &title, None).unwrap();
    let depths: Vec<u8> = pending_tracks(&s)
        .iter()
        .skip(1)
        .map(|t| t.bit_depth)
        .collect();
    assert_eq!(depths, [24, 16, 24]);
}

#[test]
fn set_codec_private_skips_a_left_out_stream_and_remaps_the_rest() {
    let mut title = h264_title();
    let mp2 = |pid, label: &str| {
        crate::disc::Stream::Audio(crate::disc::AudioStream {
            pid,
            codec: crate::disc::Codec::Mp2,
            channels: crate::disc::AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: crate::disc::SampleRate::S48,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: label.into(),
        })
    };
    title
        .streams
        .push(mp2(0x00D0, crate::disc::MP2_EXTENSION_LABEL));
    title.streams.push(mp2(0x00C0, ""));
    title.codec_privates.extend([None, None]);
    let mut s = MkvStream::create(Box::new(Cursor::new(Vec::new())), &title, None).unwrap();
    assert!(!s.set_codec_private(1, &[7]).unwrap(), "left-out stream");
    assert!(s.set_codec_private(2, &[8]).unwrap());
    let tracks = pending_tracks(&s);
    assert_eq!(tracks.len(), 2);
    assert_eq!(tracks[1].codec_private.as_deref(), Some(&[8][..]));
}

// The dependent must NOT become its own track (it folds into the base as
// BlockAdditional): a silently-disabled merge yields two H.264 tracks, not 3D.
#[test]
fn a_base_and_dependent_video_pair_builds_the_mvc_merge_and_skips_the_dependent_track() {
    let s = MkvStream::create(Box::new(Cursor::new(Vec::new())), &mvc_title(), None).unwrap();
    let mvc = s
        .mvc
        .as_ref()
        .expect("a base + dependent pair must build the MVC merge");
    assert_eq!(mvc.base_stream_idx, 0);
    assert_eq!(mvc.dep_stream_idx, 1);
    assert_eq!(mvc.base_track_idx, 0);
    assert_eq!(
        mvc.stream_to_track,
        vec![Some(0), None, Some(1)],
        "the dependent maps to no track; the audio shifts down into its slot"
    );
    assert_eq!(
        pending_tracks(&s).len(),
        2,
        "two tracks (base video + audio), not three — the dependent is folded in"
    );
}

#[test]
fn apply_coding_to_track_sets_measured_field_order_never_guesses() {
    use crate::disc::{Codec, ColorSpace, FrameRate, HdrFormat, Resolution, VideoStream};
    use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};

    let interlaced_track = || {
        MkvTrack::video(&VideoStream {
            pid: 0xE0,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i, // interlaced
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    };
    let pic = |tff: bool, pf: bool| {
        PictureInfo::mpeg2(
            CodingType::I,
            Mpeg2Coding {
                top_field_first: tff,
                repeat_first_field: false,
                progressive_frame: pf,
                progressive_sequence: false,
                frame_picture: true,
            },
        )
    };

    // A freshly built interlaced track has no field order — UNDETERMINED,
    // never a scan-time guess.
    assert_eq!(
        interlaced_track().field_order,
        ebml::FIELD_ORDER_UNDETERMINED
    );

    // MEASURED bottom-field-first → BFF (6). The red-flag fix.
    let mut t = interlaced_track();
    apply_coding_to_track(&mut t, Some(pic(false, false)), true);
    assert_eq!(
        t.field_order,
        ebml::FIELD_ORDER_BFF,
        "measured BFF → FieldOrder=6"
    );

    // MEASURED top-field-first → TFF (1).
    let mut t = interlaced_track();
    apply_coding_to_track(&mut t, Some(pic(true, false)), true);
    assert_eq!(
        t.field_order,
        ebml::FIELD_ORDER_TFF,
        "measured TFF → FieldOrder=1"
    );

    // Interlaced track, a video picture but NO usable field order →
    // UNDETERMINED (logged loudly, never faked).
    let mut t = interlaced_track();
    apply_coding_to_track(&mut t, None, true);
    assert_eq!(
        t.field_order,
        ebml::FIELD_ORDER_UNDETERMINED,
        "no measured value → UNDETERMINED, never a guess"
    );

    // Interlaced track activated with NO video picture (empty/buffered-only
    // title) → UNDETERMINED, logged quietly (not a parser defect).
    let mut t = interlaced_track();
    apply_coding_to_track(&mut t, None, false);
    assert_eq!(
        t.field_order,
        ebml::FIELD_ORDER_UNDETERMINED,
        "empty title → UNDETERMINED, never a guess"
    );

    // Progressive picture on an interlaced-flagged track → UNDETERMINED (not
    // faked to TFF/BFF) AND the declared 480i/576i scan type is corrected.
    let mut t = interlaced_track();
    assert!(t.interlaced, "the resolution declared it interlaced");
    apply_coding_to_track(&mut t, Some(pic(true, true)), true);
    assert_eq!(t.field_order, ebml::FIELD_ORDER_UNDETERMINED);
    assert!(
        !t.interlaced,
        "a MEASURED progressive picture must clear the DECLARED interlaced \
             flag — leaving it set makes players deinterlace progressive frames"
    );

    // A PROGRESSIVE track is never touched — field order stays UNDETERMINED.
    let mut prog = MkvTrack::video(&VideoStream {
        pid: 0xE0,
        codec: Codec::H264,
        resolution: Resolution::R1080p, // progressive
        frame_rate: FrameRate::F24,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt709,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    });
    assert!(!prog.interlaced);
    apply_coding_to_track(&mut prog, Some(pic(false, false)), true);
    assert_eq!(prog.field_order, ebml::FIELD_ORDER_UNDETERMINED);
}

/// `apply_coding_to_track` routes MEASURED HDR10 static metadata from the
/// first coded picture onto the track (independent of interlace), and leaves
/// it `None` when the picture carried none — never fabricated.
#[test]
fn apply_coding_to_track_plumbs_measured_hdr10() {
    use crate::disc::{Codec, ColorSpace, FrameRate, HdrFormat, Resolution, VideoStream};
    use crate::mux::codec::Hdr10Metadata;
    use crate::mux::codec::coding::{CodingType, PictureInfo};

    let make = || {
        MkvTrack::video(&VideoStream {
            pid: 0xE0,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p, // progressive UHD
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    };
    let h = Hdr10Metadata {
        display_primaries_x: [8500, 6550, 35400],
        display_primaries_y: [39850, 2300, 14600],
        white_point_x: 15635,
        white_point_y: 16450,
        max_display_mastering_luminance: 10_000_000,
        min_display_mastering_luminance: 1,
        max_content_light_level: Some(1000),
        max_pic_average_light_level: Some(400),
    };

    // Picture carries HDR10 → plumbed onto the track.
    let mut t = make();
    assert!(t.hdr10.is_none(), "fresh track has no HDR10");
    let pic = PictureInfo::coding_type_only(CodingType::I).with_hdr10(Some(h));
    apply_coding_to_track(&mut t, Some(pic), true);
    assert_eq!(t.hdr10, Some(h), "measured HDR10 must reach the track");

    // Picture without HDR10 → track stays None (never fabricated).
    let mut t = make();
    let pic = PictureInfo::coding_type_only(CodingType::I);
    apply_coding_to_track(&mut t, Some(pic), true);
    assert!(t.hdr10.is_none(), "no measured HDR10 → track stays None");

    // No coding at all → None.
    let mut t = make();
    apply_coding_to_track(&mut t, None, true);
    assert!(t.hdr10.is_none());
}

// `From<Error> for io::Error` encodes the numeric code into the
// Display string as "E{code}: ...". Check the prefix.
/// Extract the error from a `MkvStream::open` result without requiring
/// `MkvStream: Debug` (which `unwrap_err` would).
fn open_err(r: io::Result<MkvStream>) -> io::Error {
    match r {
        Ok(_) => panic!("expected MkvStream::open to fail"),
        Err(e) => e,
    }
}

// Whether the error is `E_MKV_SOURCE_INVALID`, not the historical
// `E_MKV_INVALID` (a no-muxable-frames stub code that
// `is_skippable_title_stub` treats as skippable — wrong for a corrupt source).
fn is_mkv_source_invalid(e: &io::Error) -> bool {
    has_code(e, crate::error::E_MKV_SOURCE_INVALID) && !crate::error::is_skippable_title_stub(e)
}

/// Whether an error carries the given numeric code (the crate's errors
/// render as `E<code>` with no English text).
fn has_code(e: &io::Error, code: u16) -> bool {
    e.kind() == io::ErrorKind::InvalidData && e.to_string().starts_with(&format!("E{code}"))
}

#[test]
fn ts_pid_for_track_maps_and_rejects_overflow() {
    // Track 1 → video PID; track 2 → first audio PID base.
    assert_eq!(ts_pid_for_track(1).unwrap(), 0x1011);
    assert_eq!(ts_pid_for_track(2).unwrap(), 0x1100);
    assert_eq!(ts_pid_for_track(3).unwrap(), 0x1101);
    // Highest track that still lands inside the 13-bit PID space.
    // 0x1100 + (tnum-2) <= 0x1FFF  ⇒  tnum <= 0xF01.
    assert_eq!(ts_pid_for_track(0xF01).unwrap(), 0x1FFF);
    // One past the edge must be rejected, not wrap u16.
    assert!(is_mkv_source_invalid(&ts_pid_for_track(0xF02).unwrap_err()));
    // Former overflow case (debug panic / release garbage PID) is rejected.
    assert!(is_mkv_source_invalid(
        &ts_pid_for_track(u16::MAX).unwrap_err()
    ));
    // Track 0 is invalid (1-based) and would underflow tnum-2.
    assert!(is_mkv_source_invalid(&ts_pid_for_track(0).unwrap_err()));
}

#[test]
fn checked_size_rejects_over_cap() {
    // Within cap → Ok with usize value.
    assert_eq!(checked_size(100, 256).unwrap(), 100);
    assert_eq!(checked_size(256, 256).unwrap(), 256);
    // Over cap → MkvSourceInvalid, never a giant allocation.
    let e = checked_size(257, 256).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
    // A hostile multi-GB block size is rejected as MkvSourceInvalid.
    let e = checked_size(4 * 1024 * 1024 * 1024, MAX_BLOCK_SIZE).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn read_uint_bounded_rejects_oversized_int() {
    // size > 8 would index out of the fixed 8-byte buffer in
    // read_uint_val (panic / OOB). The guard turns it into a clean
    // MkvSourceInvalid error instead.
    let mut data = Cursor::new(vec![0u8; 16]);
    let e = read_uint_bounded(&mut data, 9).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn read_uint_bounded_accepts_valid_width() {
    // 8 bytes is the max legal EBML uint width and must still work.
    let mut data = Cursor::new(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02]);
    assert_eq!(read_uint_bounded(&mut data, 8).unwrap(), 0x0102);
}

#[test]
fn read_string_bounded_rejects_huge_string() {
    // Claimed string length far above the cap must not allocate.
    let mut data = Cursor::new(vec![0u8; 16]);
    let e = read_string_bounded(&mut data, MAX_STRING_LEN + 1).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

/// Build a minimal MKV (EBML header + Segment + Info + Tracks) so the
/// reader is positioned in the cluster body, then append the given
/// cluster bytes. Returns the full byte stream.
fn minimal_mkv_with_cluster(cluster_body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // EBML header (empty body).
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    // Segment (unknown size so the reader streams children).
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    // Empty Info.
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    // Empty Tracks.
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    out.extend_from_slice(cluster_body);
    out
}

#[test]
fn simple_block_oversized_size_is_rejected() {
    // Cluster with a SIMPLE_BLOCK claiming a 2 GiB payload: must be rejected
    // (MkvSourceInvalid), not trigger a multi-GB allocation.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, 2 * 1024 * 1024 * 1024).unwrap();
    // No payload follows — but we must fail on the size check, before
    // any read of the body.
    let bytes = minimal_mkv_with_cluster(&cluster);

    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let e = stream.read().unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn well_formed_simple_block_round_trips() {
    // A small, well-formed SIMPLE_BLOCK must still parse into a frame.
    // We need at least one stream so the track index is in range, so
    // give Tracks one video TRACK_ENTRY (track number 1).
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();

    // Tracks → one TRACK_ENTRY (track number 1, type 1 = video).
    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    let mut track_entry = Vec::new();
    ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
    track_entry.extend_from_slice(&entry);
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
    out.extend_from_slice(&track_entry);

    // Cluster with a SIMPLE_BLOCK: track vint=0x81 (track 1),
    // rel_ts=0x0000, flags=0x80 (keyframe), then 4 bytes of data.
    ebml::write_id(&mut out, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA, 0xBB, 0xCC, 0xDD];
    ebml::write_id(&mut out, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut out, block.len() as u64).unwrap();
    out.extend_from_slice(&block);

    let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
    let frame = stream.read().unwrap().expect("expected a frame");
    assert_eq!(frame.track, 0);
    assert!(frame.keyframe);
    assert_eq!(frame.data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
}
#[test]
fn truncated_simple_block_body_errors_not_panics() {
    // A SIMPLE_BLOCK declaring a 64-byte payload but supplying none must surface
    // a clean typed MkvSourceInvalid, never panic, never allocate the full size.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, 64).unwrap();
    // No body bytes follow → short read.
    let bytes = minimal_mkv_with_cluster(&cluster);

    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let e = stream.read().unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

/// Build a minimal MKV header + Segment + Info, then a Tracks element with a
/// single TRACK_ENTRY of the given track number/type, then the cluster bytes.
fn mkv_with_track_and_cluster(tnum: u64, ttype: u64, cluster_body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();

    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, tnum).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, ttype).unwrap();
    let mut track_entry = Vec::new();
    ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
    track_entry.extend_from_slice(&entry);
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
    out.extend_from_slice(&track_entry);

    out.extend_from_slice(cluster_body);
    out
}

#[test]
fn oversized_codec_private_is_rejected() {
    // A TRACK_ENTRY whose CODEC_PRIVATE declares a payload above
    // MAX_CODEC_PRIVATE must be rejected (MkvSourceInvalid) before any
    // multi-MB allocation, while parsing the header.
    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    // CODEC_PRIVATE header claiming a huge size (no body needed — the
    // size check fires first).
    ebml::write_id(&mut entry, ebml::CODEC_PRIVATE).unwrap();
    ebml::write_size(&mut entry, MAX_CODEC_PRIVATE + 1).unwrap();
    let mut track_entry = Vec::new();
    ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
    track_entry.extend_from_slice(&entry);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
    out.extend_from_slice(&track_entry);

    let e = match MkvStream::open(Cursor::new(out)) {
        Ok(_) => panic!("expected MkvSourceInvalid, got Ok"),
        Err(e) => e,
    };
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn block_group_frame_round_trips_with_duration() {
    // MkvMuxer emits AC3/PGS and every MPEG-2 frame as a BlockGroup; the reader
    // must descend and yield it, not skip it. Inside a BlockGroup the SimpleBlock
    // 0x80 bit is RESERVED (always 0), so keyframe-ness is ReferenceBlock's absence.
    let block = [0x82u8, 0x00, 0x05, 0x00, 0x11, 0x22, 0x33]; // track 2, rel 5, reserved bit 0, 3 data
    let mut bg_body = Vec::new();
    ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
    ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
    bg_body.extend_from_slice(&block);
    ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 40).unwrap(); // 40 ms

    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    // CLUSTER_TIMESTAMP = 100 ms so pts = (100 + 5) ms.
    ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
    ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
    ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
    cluster.extend_from_slice(&bg_body);

    // Track 2 (audio) so track_idx 1 needs two streams; give two TRACK_ENTRYs.
    // Reuse the helper for track 1, then a manual second entry would be
    // simpler — instead build directly with two entries.
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    let mut tracks = Vec::new();
    for (n, t) in [(1u64, 1u64), (2u64, 2u64)] {
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, n).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, t).unwrap();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
    }
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out.extend_from_slice(&cluster);

    let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
    let frame = stream
        .read()
        .unwrap()
        .expect("BlockGroup frame must be read");
    assert_eq!(frame.track, 1, "track 2 → index 1");
    assert!(
        frame.keyframe,
        "a BlockGroup with no ReferenceBlock is a keyframe (the 0x80 bit is reserved here)"
    );
    assert_eq!(frame.data, vec![0x11, 0x22, 0x33]);
    assert_eq!(frame.pts, 105 * 1_000_000, "pts = (cluster 100 + rel 5) ms");
    assert_eq!(frame.duration_ns, Some(40 * 1_000_000));
}

// A BlockGroup's `BlockAdditions` subtree (MVC dependent view) can't be carried by
// `PesFrame`, so read-back drops it — a LOSSY outcome that must be counted, never silent.
#[test]
fn block_additions_dropped_on_read_back_is_counted_not_silent() {
    // The dependent-view payload: big enough that a byte count is unambiguous.
    let dependent_au = vec![0x5Au8; 512];

    // BlockAdditions > BlockMore > { BlockAddID = 2, BlockAdditional }.
    let mut more = Vec::new();
    ebml::write_uint(&mut more, ebml::BLOCK_ADD_ID, 2).unwrap();
    ebml::write_binary(&mut more, ebml::BLOCK_ADDITIONAL, &dependent_au).unwrap();
    let mut adds = Vec::new();
    ebml::write_id(&mut adds, ebml::BLOCK_MORE).unwrap();
    ebml::write_size(&mut adds, more.len() as u64).unwrap();
    adds.extend_from_slice(&more);

    // BlockGroup > { Block(base view), BlockAdditions }.
    let block = [0x81u8, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC];
    let mut bg_body = Vec::new();
    ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
    ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
    bg_body.extend_from_slice(&block);
    ebml::write_id(&mut bg_body, ebml::BLOCK_ADDITIONS).unwrap();
    ebml::write_size(&mut bg_body, adds.len() as u64).unwrap();
    bg_body.extend_from_slice(&adds);

    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
    ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
    cluster.extend_from_slice(&bg_body);

    // One video TRACK_ENTRY (track number 1) so track index 0 is in range.
    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    let mut tracks = Vec::new();
    ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
    tracks.extend_from_slice(&entry);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out.extend_from_slice(&cluster);

    let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
    assert_eq!(stream.errors(), 0, "no BlockAdditions seen before the read");
    assert_eq!(stream.lost_bytes(), 0);

    let frame = stream
        .read()
        .unwrap()
        .expect("the base-view BlockGroup frame must still be read");
    assert_eq!(frame.track, 0);
    assert_eq!(
        frame.data,
        vec![0xAA, 0xBB, 0xCC],
        "the frame carries the BASE view only — the dependent view is lost"
    );
    assert!(
        !frame.data.contains(&0x5A),
        "PesFrame has no side-payload field, so the dependent AU is NOT in the frame"
    );

    // The loss is now reported.
    assert_eq!(
        stream.errors(),
        1,
        "one dropped BlockAdditions subtree must be counted as a loss event"
    );
    assert!(
        stream.lost_bytes() >= dependent_au.len() as u64,
        "dropped bytes ({}) must cover the {}-byte dependent AU",
        stream.lost_bytes(),
        dependent_au.len()
    );

    // EOF, and the counters survive it (the driver samples them after the run).
    assert!(stream.read().unwrap().is_none());
    assert_eq!(stream.errors(), 1);
}

// A BlockGroup carrying a ReferenceBlock is NOT a keyframe (the only non-keyframe signal a
// BlockGroup has); a past regression silently dropped MPEG-2 video.
#[test]
fn reference_block_marks_block_group_frame_as_non_keyframe() {
    // Same construction as the test above, plus a ReferenceBlock child.
    let block = [0x82u8, 0x00, 0x05, 0x00, 0x11, 0x22, 0x33];
    let mut bg_body = Vec::new();
    ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
    ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
    bg_body.extend_from_slice(&block);
    ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 40).unwrap();
    // References a keyframe 40 ms earlier ⇒ this Block is not a seek point.
    ebml::write_int(&mut bg_body, ebml::REFERENCE_BLOCK, -40).unwrap();

    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
    ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
    ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
    cluster.extend_from_slice(&bg_body);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    let mut tracks = Vec::new();
    for (n, t) in [(1u64, 1u64), (2u64, 2u64)] {
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, n).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, t).unwrap();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
    }
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out.extend_from_slice(&cluster);

    let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
    let frame = stream
        .read()
        .unwrap()
        .expect("BlockGroup frame must be read");
    assert!(
        !frame.keyframe,
        "a BlockGroup WITH a ReferenceBlock must read back as a non-keyframe"
    );
    assert_eq!(frame.data, vec![0x11, 0x22, 0x33]);
    assert_eq!(frame.duration_ns, Some(40 * 1_000_000));
}

#[test]
fn track_number_zero_is_rejected() {
    // A TRACK_ENTRY with TRACK_NUMBER 0 must be rejected (the ts_pid
    // computation would underflow `tnum - 2`).
    let bytes = mkv_with_track_and_cluster(0, 1, &[]);
    let e = open_err(MkvStream::open(Cursor::new(bytes)));
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn track_number_above_u16_is_rejected() {
    // 65536 would truncate to 0 via `as u16` and then underflow.
    let bytes = mkv_with_track_and_cluster(65536, 1, &[]);
    let e = open_err(MkvStream::open(Cursor::new(bytes)));
    assert!(is_mkv_source_invalid(&e));
    // 65537 is the case the guard really exists for: it truncates onto the
    // PERFECTLY VALID TrackNumber 1, so a check for merely "not 65536" would
    // route its blocks to another track's stream. Reject the whole class.
    let bytes = mkv_with_track_and_cluster(65537, 1, &[]);
    let e = open_err(MkvStream::open(Cursor::new(bytes)));
    assert!(is_mkv_source_invalid(&e));
    // 65535 fits u16 exactly; it's rejected by the PID guard, not the width
    // guard (0x1100 + 65533 is outside the 13-bit TS PID space). Pinning WHICH
    // boundary each guard owns keeps a widened width check from becoming load-bearing.
    assert!(ts_pid_for_track(65535).is_err());
    assert_eq!(
        TrackTable {
            nums: vec![65535],
            default_durations: vec![None],
            timings: vec![Default::default()],
            pcm: vec![None],
            pcm_infer: vec![None],
            ..Default::default()
        }
        .index_of(65535),
        Some(0),
        "65535 is a representable TrackNumber, not an over-width one"
    );
    assert_eq!(
        TrackTable::contiguous(1).index_of(65536),
        None,
        "a TrackNumber past u16 resolves to no stream rather than aliasing onto one"
    );
    assert_eq!(
        TrackTable::contiguous(1).index_of(65537),
        None,
        "65537 truncates onto TrackNumber 1 — it must be rejected before the cast, \
             or a block for a track this file does not declare is routed to track 1"
    );
    assert_eq!(TrackTable::contiguous(1).index_of(0), None);
}

#[test]
fn unknown_size_inner_child_in_tracks_is_rejected() {
    // A TRACK_ENTRY child declaring EBML unknown size (cs == u64::MAX) must be
    // rejected, not used in `hlen + cs` (overflow -> debug panic).
    let mut entry = Vec::new();
    ebml::write_id(&mut entry, ebml::TRACK_NUMBER).unwrap();
    ebml::write_unknown_size(&mut entry).unwrap(); // child size = unknown

    let mut tracks = Vec::new();
    ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
    tracks.extend_from_slice(&entry);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);

    let e = open_err(MkvStream::open(Cursor::new(out)));
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn oversized_title_string_is_rejected() {
    // INFO/TITLE declaring a string above MAX_STRING_LEN must be
    // rejected during header parse, not allocated.
    let mut info = Vec::new();
    ebml::write_id(&mut info, ebml::TITLE).unwrap();
    ebml::write_size(&mut info, MAX_STRING_LEN + 1).unwrap();

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, info.len() as u64).unwrap();
    out.extend_from_slice(&info);

    let e = match MkvStream::open(Cursor::new(out)) {
        Ok(_) => panic!("expected MkvSourceInvalid, got Ok"),
        Err(e) => e,
    };
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn read_uint_val_len_nine_errors_not_panics() {
    // Direct helper test: an EBML uint cannot exceed 8 bytes. len=9
    // would index past the fixed 8-byte stack buffer and panic on
    // untrusted input; it must return MkvSourceInvalid instead.
    let mut data = Cursor::new(vec![0u8; 16]);
    let e = ebml::read_uint_val(&mut data, 9).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn read_float_val_bad_width_errors() {
    // EBML floats are exactly 0, 4, or 8 bytes. Any other width is
    // malformed and must error rather than over- or under-read.
    let mut data = Cursor::new(vec![0u8; 16]);
    let e = ebml::read_float_val(&mut data, 5).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
    // 0/4/8 remain valid widths.
    let mut z = Cursor::new(vec![0u8; 16]);
    assert_eq!(ebml::read_float_val(&mut z, 0).unwrap(), 0.0);
    let mut f4 = Cursor::new(vec![0u8; 16]);
    assert!(ebml::read_float_val(&mut f4, 4).is_ok());
    let mut f8 = Cursor::new(vec![0u8; 16]);
    assert!(ebml::read_float_val(&mut f8, 8).is_ok());
}

#[test]
fn non_utf8_string_element_is_rejected() {
    // A string element with invalid UTF-8 bytes must surface a numeric
    // MkvSourceInvalid error, not an io::Error wrapping the FromUtf8Error
    // English message (library no-English rule).
    let mut data = Cursor::new(vec![0xFF, 0xFE, 0xFD, 0xFC]);
    let e = ebml::read_string_val(&mut data, 4).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn simple_block_track_zero_is_skipped() {
    // A SimpleBlock with track vint 0 must be skipped, not attributed to
    // track 0. Build one track, then a cluster whose only block is track 0
    // followed by a valid track-1 block; read() must return the track-1 one.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    // track vint 0 is not directly encodable (0x80 is track 0 → block_vint
    // returns (0,1)); use 0x80 as the track byte.
    let bad = [0x80u8, 0x00, 0x00, 0x80, 0xEE];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, bad.len() as u64).unwrap();
    cluster.extend_from_slice(&bad);
    let good = [0x81u8, 0x00, 0x00, 0x80, 0xAB, 0xCD];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, good.len() as u64).unwrap();
    cluster.extend_from_slice(&good);

    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frame = stream.read().unwrap().expect("track-1 frame expected");
    assert_eq!(frame.track, 0);
    assert_eq!(frame.data, vec![0xAB, 0xCD]);
}

// ============================================================
// block_vint — Block track-number VINT (§6.2); a width bug mis-attributes blocks.
// ============================================================

#[test]
fn block_vint_width_selection_and_values() {
    // 1-byte: 0x81 → track 1 (high bit is the marker, low 7 = value).
    assert_eq!(block_vint(&[0x81]), (1, 1));
    assert_eq!(block_vint(&[0xFF]), (0x7F, 1)); // max 1-byte track
    // 2-byte: 0x40 marker, 14-bit value. 0x40 0x80 → 0x80.
    assert_eq!(block_vint(&[0x40, 0x80]), (0x80, 2));
    assert_eq!(block_vint(&[0x7F, 0xFF]), (0x3FFF, 2)); // max 2-byte
    // 3-byte: 0x20 marker, 21-bit value.
    assert_eq!(block_vint(&[0x20, 0x00, 0x01]), (1, 3));
    assert_eq!(block_vint(&[0x3F, 0xFF, 0xFF]), (0x1F_FFFF, 3));
    // 4-byte: 0x10 marker, 28-bit value.
    assert_eq!(block_vint(&[0x10, 0x00, 0x00, 0x01]), (1, 4));
    assert_eq!(block_vint(&[0x1F, 0xFF, 0xFF, 0xFF]), (0x0FFF_FFFF, 4));
}

#[test]
fn block_vint_unsupported_and_truncated_forms() {
    // Empty input → (0, 0).
    assert_eq!(block_vint(&[]), (0, 0));
    // A 2-byte marker but only 1 byte available falls through to the
    // catch-all (0, 1) — treated as track 0 (skipped by parse_block).
    assert_eq!(block_vint(&[0x40]), (0, 1));
    // 5..=8-octet VINTs decode (RFC 8794); a 0-valued one is track 0 (skipped).
    assert_eq!(block_vint(&[0x08, 0, 0, 0, 0]), (0, 5));
    assert_eq!(block_vint(&[0x01, 0, 0, 0, 0, 0, 0, 7]), (7, 8));
    // 0x00 first byte: wider than 8 octets → unsupported → (0, 1).
    assert_eq!(block_vint(&[0x00, 0x11]), (0, 1));
}

// ============================================================
// parse_block — Block payload into PesFrames; guards len<4, track 0, unknown track.
// ============================================================

/// `parse_block` for an UNLACED block on a file whose TrackNumbers are
/// `1..=streams_len` (the layout this crate's own writer emits): the single
/// frame, or `None` when the block was skipped.
fn parse_block_one(
    block: &[u8],
    cluster_ts_ticks: i64,
    ts_scale_ns: i64,
    streams_len: usize,
    duration_ns: Option<u64>,
) -> Option<crate::pes::PesFrame> {
    let frames = parse_block(
        block,
        cluster_ts_ticks,
        ts_scale_ns,
        &TrackTable::contiguous(streams_len),
        duration_ns,
    )
    .expect("unlaced block never errors");
    assert!(
        frames.len() <= 1,
        "an unlaced block yields at most one frame"
    );
    frames.into_iter().next()
}

#[test]
fn parse_block_too_short_is_none() {
    // Fewer than 4 bytes can't hold vint(1)+ts(2)+flags(1); must be None.
    assert!(parse_block_one(&[0x81, 0x00, 0x00], 0, 1_000_000, 1, None).is_none());
    assert!(parse_block_one(&[], 0, 1_000_000, 1, None).is_none());
}

#[test]
fn parse_block_header_longer_than_payload_is_none() {
    // A 2-byte track VINT (0x40 0x01) needs vl(2)+3 = 5 bytes minimum, but
    // only 4 are supplied → vl+3 > len → None (no OOB index of data slice).
    let block = [0x40u8, 0x01, 0x00, 0x00]; // len 4, vl 2 → 2+3=5 > 4
    assert!(parse_block_one(&block, 0, 1_000_000, 2, None).is_none());
}

#[test]
fn parse_block_track_index_out_of_range_is_none() {
    // track 2 → index 1, but only 1 stream exists → must skip (None),
    // never index past the streams slice.
    let block = [0x82u8, 0x00, 0x00, 0x80, 0xAA]; // track 2
    assert!(parse_block_one(&block, 0, 1_000_000, 1, None).is_none());
    // With 2 streams it resolves to index 1.
    let f = parse_block_one(&block, 0, 1_000_000, 2, None).unwrap();
    assert_eq!(f.track, 1);
}

#[test]
fn parse_block_pts_honours_timestamp_scale() {
    // PTS = (cluster_ts_ticks + rel_ts) * ts_scale_ns. With a non-1ms scale
    // the result must scale accordingly (foreign MKVs). rel_ts = 10 here.
    let block = [0x81u8, 0x00, 0x0A, 0x80, 0xAA]; // track 1, rel 10, kf
    // ts_scale 1_000_000 (1ms): cluster 100 + rel 10 = 110 ticks → 110ms.
    let f = parse_block_one(&block, 100, 1_000_000, 1, None).unwrap();
    assert_eq!(f.pts, 110 * 1_000_000);
    assert!(f.keyframe);
    // ts_scale 90_000 (90kHz): (100+10) * 90_000.
    let f = parse_block_one(&block, 100, 90_000, 1, None).unwrap();
    assert_eq!(f.pts, 110 * 90_000);
}

#[test]
fn parse_block_negative_rel_ts_is_signed() {
    // rel_ts is a SIGNED 16-bit big-endian value. 0xFFFF = -1. The pts must
    // go DOWN from the cluster timestamp, not jump to +65535.
    let block = [0x81u8, 0xFF, 0xFF, 0x80, 0xAA]; // rel_ts = -1
    let f = parse_block_one(&block, 100, 1_000_000, 1, None).unwrap();
    assert_eq!(f.pts, 99 * 1_000_000, "rel_ts -1 must subtract one tick");
}

#[test]
fn parse_block_keyframe_flag_and_duration_propagate() {
    // flags bit 0x80 = keyframe; a clear bit = delta frame. duration_ns is
    // passed through unchanged (BlockGroup path supplies it).
    let kf = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
    let nkf = [0x81u8, 0x00, 0x00, 0x00, 0xAA];
    assert!(
        parse_block_one(&kf, 0, 1_000_000, 1, None)
            .unwrap()
            .keyframe
    );
    assert!(
        !parse_block_one(&nkf, 0, 1_000_000, 1, None)
            .unwrap()
            .keyframe
    );
    let f = parse_block_one(&kf, 0, 1_000_000, 1, Some(40_000_000)).unwrap();
    assert_eq!(f.duration_ns, Some(40_000_000));
}

#[test]
fn parse_block_pts_saturates_no_overflow() {
    // A hostile cluster timestamp near i64::MAX must not panic on the
    // ticks→ns multiply; saturating_mul caps it. (Guards the debug-build
    // overflow the source comment calls out.)
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
    let f = parse_block_one(&block, i64::MAX, 1_000_000, 1, None).unwrap();
    assert_eq!(f.pts, i64::MAX, "ticks→ns must saturate, not wrap/panic");
}

#[test]
fn parse_block_cluster_ts_plus_rel_ts_saturates_no_overflow() {
    // Regression: CLUSTER_TIMESTAMP near i64::MAX plus a POSITIVE rel_ts overflows
    // `cluster_ts + rel_ts`; a plain `+` panics in debug and wraps negative in
    // release. rel_ts = +0x7FFF = 32767 (max positive signed 16-bit).
    let block = [0x81u8, 0x7F, 0xFF, 0x80, 0xAA];
    let f = parse_block_one(&block, i64::MAX, 1_000_000, 1, None).unwrap();
    // The add saturates at i64::MAX, then the mul saturates too.
    assert_eq!(
        f.pts,
        i64::MAX,
        "cluster_ts + rel_ts must saturate, not panic/wrap"
    );
}

// ts_pid_for_track — mid-range mapping locking the 0x1100 + (tnum-2) formula.

// CLUSTER_TIMESTAMP overflow guard: a value above i64::MAX would cast to a large
// negative i64 and poison every block PTS in the cluster; the reader must reject it.

#[test]
fn cluster_timestamp_above_i64_max_is_rejected() {
    // CLUSTER_TIMESTAMP encoded as an 8-byte uint with the top bit set
    // (> i64::MAX). The reader must surface MkvSourceInvalid on read().
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::CLUSTER_TIMESTAMP).unwrap();
    ebml::write_size(&mut cluster, 8).unwrap();
    cluster.extend_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());
    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let e = stream.read().unwrap_err();
    assert!(is_mkv_source_invalid(&e));

    // `i64::MAX` itself is the last representable value and must be ACCEPTED —
    // the guard is against a u64 that goes NEGATIVE on the cast, not against a
    // large timestamp; rejecting it too would drop a legal cluster.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::CLUSTER_TIMESTAMP).unwrap();
    ebml::write_size(&mut cluster, 8).unwrap();
    cluster.extend_from_slice(&(i64::MAX as u64).to_be_bytes());
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAB];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(&block);
    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let f = stream
        .read()
        .expect("a cluster timestamp of exactly i64::MAX is in range")
        .expect("one frame");
    assert_eq!(
        f.pts,
        i64::MAX,
        "the tick→ns multiply saturates, never wraps"
    );
}

// A malformed mkv:// SOURCE must never be classified as a skippable title stub:
// raising `Error::MkvInvalid` here made `is_skippable_title_stub` treat it as an
// empty nav/menu stub, so an all-titles rip silently passed over corrupt input.

#[test]
fn corrupt_source_is_not_classified_as_a_skippable_title_stub() {
    // Same corrupt fixture as above, driven through the real reader and asserted
    // against the public classifier. Mutation: raising `MkvInvalid` instead of
    // `MkvSourceInvalid` at that guard turns this red.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::CLUSTER_TIMESTAMP).unwrap();
    ebml::write_size(&mut cluster, 8).unwrap();
    cluster.extend_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());
    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let e = stream.read().unwrap_err();
    assert!(
        !crate::error::is_skippable_title_stub(&e),
        "a corrupt mkv:// source must be a failure, not a skippable stub: {e}"
    );
    assert_eq!(
        e.to_string(),
        format!("E{}", crate::error::E_MKV_SOURCE_INVALID)
    );
    // A truncated element body (the EBML read primitive) is the same verdict,
    // proving the classification is not specific to one guard.
    let short = ebml::read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), 100).unwrap_err();
    assert!(!crate::error::is_skippable_title_stub(&short));
    assert_eq!(
        short.to_string(),
        format!("E{}", crate::error::E_MKV_SOURCE_INVALID)
    );
}

// parse_mkv_header — TimestampScale threading/clamping: PTS multiplies by
// ts_scale_ns, so a zero or absurd scale must clamp to the 1ms default.

#[test]
fn zero_timestamp_scale_clamps_to_default() {
    // A foreign/corrupt INFO with TimestampScale 0 must clamp to 1_000_000
    // (1ms), so a rel_ts 5 block at cluster 100 still yields 105ms — not 0.
    let mut info = Vec::new();
    ebml::write_uint(&mut info, ebml::TIMESTAMP_SCALE, 0).unwrap();

    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    let mut track_entry = Vec::new();
    ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
    track_entry.extend_from_slice(&entry);

    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
    let block = [0x81u8, 0x00, 0x05, 0x80, 0xAA];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(&block);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, info.len() as u64).unwrap();
    out.extend_from_slice(&info);
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
    out.extend_from_slice(&track_entry);
    out.extend_from_slice(&cluster);

    let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
    let f = stream.read().unwrap().expect("frame");
    assert_eq!(f.pts, 105 * 1_000_000, "zero scale must clamp to 1ms");
}

#[test]
fn duration_uses_timestamp_scale_for_seconds() {
    // DURATION is a float in TimestampScale TICKS, not ms. With scale
    // 1_000_000 (1ms) and duration 5000 ticks → 5.0 s. The header parser
    // must convert via ticks * scale_ns / 1e9.
    let mut info = Vec::new();
    ebml::write_uint(&mut info, ebml::TIMESTAMP_SCALE, 1_000_000).unwrap();
    ebml::write_float(&mut info, ebml::DURATION, 5000.0).unwrap();

    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    let mut track_entry = Vec::new();
    ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
    track_entry.extend_from_slice(&entry);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, info.len() as u64).unwrap();
    out.extend_from_slice(&info);
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
    out.extend_from_slice(&track_entry);

    let stream = MkvStream::open(Cursor::new(out)).unwrap();
    assert_eq!(stream.info().duration_secs, 5.0);
}

#[test]
fn info_child_size_exceeding_remaining_is_rejected() {
    // Crafted INFO whose sole child (TITLE) declares a 1000-byte body while the INFO
    // parent is sized to just the child header, so header+body overruns `remaining`.
    // Old code saturated it to 0 and read the oversized child (EOF/garbage); the guard rejects as MkvSourceInvalid.
    let mut info = Vec::new();
    ebml::write_id(&mut info, ebml::TITLE).unwrap();
    ebml::write_size(&mut info, 1000).unwrap(); // declares 1000 bytes, provides none

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    // Parent INFO sized to ONLY the child header — its child then overruns it.
    ebml::write_size(&mut out, info.len() as u64).unwrap();
    out.extend_from_slice(&info);

    let e = open_err(MkvStream::open(Cursor::new(out)));
    assert!(
        is_mkv_source_invalid(&e),
        "a child body larger than the INFO parent's remaining must be rejected"
    );
}

#[test]
fn missing_ebml_header_is_rejected() {
    // A stream whose first element is not the EBML header (0x1A45DFA3) is
    // not a Matroska file and must be rejected.
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap(); // wrong first element
    ebml::write_size(&mut out, 0).unwrap();
    let e = open_err(MkvStream::open(Cursor::new(out)));
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn segment_must_follow_ebml_header() {
    // After a valid EBML header the next element must be the Segment; a
    // different element is malformed.
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap(); // not SEGMENT
    ebml::write_size(&mut out, 0).unwrap();
    let e = open_err(MkvStream::open(Cursor::new(out)));
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn track_type_to_codec_and_pid_mapping_round_trips() {
    // A video TRACK_ENTRY (type 1, codec HEVC) must map to a VideoStream
    // with the V_MPEGH/ISO/HEVC → Codec::Hevc translation and track 1 → PID
    // 0x1011. Confirms parse_track wiring end to end.
    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    ebml::write_string(&mut entry, ebml::CODEC_ID, ebml::CODEC_HEVC).unwrap();
    let mut track_entry = Vec::new();
    ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
    track_entry.extend_from_slice(&entry);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
    out.extend_from_slice(&track_entry);

    let stream = MkvStream::open(Cursor::new(out)).unwrap();
    match &stream.info().streams[0] {
        crate::disc::Stream::Video(v) => {
            assert_eq!(v.codec, Codec::Hevc);
            assert_eq!(v.pid, 0x1011);
        }
        _ => panic!("expected video stream"),
    }
}

#[test]
fn block_group_unknown_size_is_rejected() {
    // A BLOCK_GROUP declaring unknown size (u64::MAX) would loop draining
    // the stream; the reader must reject it as MkvSourceInvalid.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap(); // size = unknown
    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let e = stream.read().unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn read_then_eof_returns_none() {
    // After the last block, a clean EOF on the next element header must
    // return Ok(None) (end of stream), not an error.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(&block);
    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert!(stream.read().unwrap().is_some(), "first frame");
    assert!(stream.read().unwrap().is_none(), "clean EOF → None");
}

// A skipped element whose declared size runs PAST EOF is a truncated element, reported as
// `MkvSourceInvalid`, not a clean end of stream.
#[test]
fn a_skip_past_eof_is_an_error_not_a_clean_end_of_stream() {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    // Frame 1 — read normally.
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(&block);
    // A VOID whose size field is corrupt: it claims 1 MiB, and the file
    // holds only the handful of bytes below. This is the "corrupt size
    // field mid-Clusters" case.
    ebml::write_id(&mut cluster, ebml::VOID).unwrap();
    ebml::write_size(&mut cluster, 1024 * 1024).unwrap();
    // Frame 2 — the rest of the title, swallowed by the bad skip.
    let block2 = [0x81u8, 0x00, 0x01, 0x80, 0xBB];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block2.len() as u64).unwrap();
    cluster.extend_from_slice(&block2);

    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert!(stream.read().unwrap().is_some(), "first frame reads");
    let e = match stream.read() {
        Err(e) => e,
        Ok(None) => panic!(
            "a skip that hit EOF was reported as a CLEAN END OF STREAM: the \
                 rest of the title is gone and the caller sees errors = 0, \
                 complete = true"
        ),
        Ok(Some(_)) => panic!("the truncated skip must not yield a frame"),
    };
    assert!(is_mkv_source_invalid(&e), "{e:?}");
}

/// The honest path this fix must not break: a skipped element whose declared
/// size is exactly satisfied by the bytes present is still skipped cleanly,
/// and the genuine EOF that follows is still `Ok(None)`.
#[test]
fn a_fully_satisfied_skip_still_ends_at_a_clean_eof() {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    // A VOID that is fully present.
    ebml::write_id(&mut cluster, ebml::VOID).unwrap();
    ebml::write_size(&mut cluster, 8).unwrap();
    cluster.extend_from_slice(&[0u8; 8]);
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(&block);

    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let f = stream.read().unwrap().expect("the frame after the VOID");
    assert_eq!(f.data, vec![0xAA]);
    assert!(
        stream.read().unwrap().is_none(),
        "a genuine EOF at a record boundary is still a clean end"
    );
}

// Block LACING (RFC 9559 §10.3) and TrackNumber->stream routing (§5.1.4.1.1).

/// One TrackEntry description for `mkv_with_tracks_and_cluster`:
/// (TrackNumber, TrackType, DefaultDuration ns, CodecPrivate).
struct TrackSpec {
    tnum: u64,
    ttype: u64,
    default_duration_ns: Option<u64>,
    codec_private: Option<Vec<u8>>,
}

impl TrackSpec {
    fn new(tnum: u64, ttype: u64) -> Self {
        Self {
            tnum,
            ttype,
            default_duration_ns: None,
            codec_private: None,
        }
    }
    fn with_default_duration(mut self, ns: u64) -> Self {
        self.default_duration_ns = Some(ns);
        self
    }
    fn with_codec_private(mut self, cp: &[u8]) -> Self {
        self.codec_private = Some(cp.to_vec());
        self
    }
}

/// Build an MKV header with an arbitrary set of TrackEntries — arbitrary
/// TrackNumbers, in arbitrary order — followed by `cluster_body`.
fn mkv_with_tracks_and_cluster(specs: &[TrackSpec], cluster_body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();

    let mut tracks = Vec::new();
    for s in specs {
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, s.tnum).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, s.ttype).unwrap();
        if let Some(ns) = s.default_duration_ns {
            ebml::write_uint(&mut entry, ebml::DEFAULT_DURATION, ns).unwrap();
        }
        if let Some(cp) = &s.codec_private {
            ebml::write_binary(&mut entry, ebml::CODEC_PRIVATE, cp).unwrap();
        }
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
    }
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out.extend_from_slice(cluster_body);
    out
}

/// Wrap one raw (Simple)Block payload in a Cluster with the given timestamp.
fn cluster_with_simple_block(cluster_ts: u64, block: &[u8]) -> Vec<u8> {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, cluster_ts).unwrap();
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(block);
    cluster
}

/// Drain every frame a reader will yield.
fn drain(stream: &mut MkvStream) -> Vec<crate::pes::PesFrame> {
    let mut out = Vec::new();
    while let Some(f) = stream.read().expect("no read error") {
        out.push(f);
    }
    out
}

// EBML lacing (RFC 9559 §10.3.3): three frames of 3/4/5 octets must come out as THREE
// byte-exact frames, not one Block-verbatim frame with the lacing header as garbage
// payload.
#[test]
fn ebml_laced_block_yields_every_frame_with_exact_payloads() {
    // size 3 → 0x83 (VINT, value 3). size delta 4-3 = +1 → unsigned 1 + bias
    // (2^6-1 = 63) = 64 → 0xC0 with the VINT_MARKER.
    let mut block = vec![
        0x81, // TrackNumber 1
        0x00, 0x00, // rel_ts 0
        0x86, // KEY | LACING = 11b (EBML)
        0x02, // Lacing Head: 3 frames minus 1
        0x83, // first frame size = 3
        0xC0, // second frame size = previous + 1 = 4
    ];
    block.extend_from_slice(&[0xAA; 3]);
    block.extend_from_slice(&[0xBB; 4]);
    block.extend_from_slice(&[0xCC; 5]);

    // DefaultDuration 24 ms/frame is what §10.3.5 leaves the reader to space
    // the second and later frames by.
    let specs = [TrackSpec::new(1, 2).with_default_duration(24_000_000)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(100, &block));
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frames = drain(&mut stream);

    assert_eq!(frames.len(), 3, "one laced Block carries three frames");
    assert_eq!(frames[0].data, vec![0xAA; 3], "frame 1 payload byte-exact");
    assert_eq!(frames[1].data, vec![0xBB; 4], "frame 2 payload byte-exact");
    assert_eq!(frames[2].data, vec![0xCC; 5], "frame 3 payload byte-exact");
    for f in &frames {
        assert_eq!(f.track, 0);
        assert!(f.keyframe, "the KEY flag covers the whole lace");
        assert_eq!(f.duration_ns, Some(24_000_000));
    }
    // The Block timestamp applies to the FIRST frame; the rest are spaced by
    // DefaultDuration (§10.3.5).
    assert_eq!(frames[0].pts, 100 * 1_000_000);
    assert_eq!(frames[1].pts, 100 * 1_000_000 + 24_000_000);
    assert_eq!(frames[2].pts, 100 * 1_000_000 + 48_000_000);
}

/// Xiph lacing (RFC 9559 §10.3.2): sizes are runs of 0xFF octets terminated
/// by an octet below 255, and a size that is a multiple of 255 ends in a 0.
#[test]
fn xiph_laced_block_splits_on_255_coded_sizes() {
    let mut block = vec![
        0x81, // TrackNumber 1
        0x00, 0x00, // rel_ts 0
        0x82, // KEY | LACING = 01b (Xiph)
        0x01, // Lacing Head: 2 frames minus 1
        0xFF, 0x00, // first frame size = 255 (a multiple of 255 → trailing 0)
    ];
    block.extend_from_slice(&[0xAA; 255]);
    block.extend_from_slice(&[0xBB; 2]);

    let specs = [TrackSpec::new(1, 2)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frames = drain(&mut stream);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, vec![0xAA; 255]);
    assert_eq!(
        frames[1].data,
        vec![0xBB; 2],
        "the last frame's size is the Block remainder"
    );
}

/// Fixed-size lacing (RFC 9559 §10.3.4): no sizes are stored; every frame is
/// the Block remainder divided by the frame count.
#[test]
fn fixed_size_laced_block_splits_evenly() {
    let mut block = vec![
        0x81, // TrackNumber 1
        0x00, 0x00, // rel_ts 0
        0x84, // KEY | LACING = 10b (fixed-size)
        0x02, // Lacing Head: 3 frames minus 1
    ];
    block.extend_from_slice(&[0x11, 0x11, 0x22, 0x22, 0x33, 0x33]);

    let specs = [TrackSpec::new(1, 2)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frames = drain(&mut stream);
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].data, vec![0x11, 0x11]);
    assert_eq!(frames[1].data, vec![0x22, 0x22]);
    assert_eq!(frames[2].data, vec![0x33, 0x33]);
}

/// A lacing header whose declared sizes do not fit in the Block leaves the
/// frame boundaries unknowable. That MUST be an error, never a pass-through
/// of the raw payload as one frame.
#[test]
fn malformed_lacing_header_is_rejected_not_passed_through() {
    // Xiph, 2 frames, first size declared as 200 but only 4 payload octets
    // follow → the remainder for the last frame underflows.
    let block = [
        0x81, 0x00, 0x00, 0x82, // KEY | Xiph lacing
        0x01, // 2 frames
        0xC8, // first frame size = 200
        0xAA, 0xBB, 0xCC, 0xDD,
    ];
    let specs = [TrackSpec::new(1, 2)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let e = stream.read().unwrap_err();
    assert!(
        has_code(&e, crate::error::E_MKV_LACING_INVALID),
        "malformed lacing must be rejected"
    );
    assert!(
        !crate::error::is_skippable_title_stub(&e),
        "a track whose frames cannot be separated is NOT an empty nav stub"
    );

    // Fixed-size lacing whose body does not divide evenly by the frame count.
    let block = [
        0x81, 0x00, 0x00, 0x84, // KEY | fixed-size lacing
        0x02, // 3 frames
        0xAA, 0xBB, 0xCC, 0xDD, // 4 octets — not divisible by 3
    ];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert!(has_code(
        &stream.read().unwrap_err(),
        crate::error::E_MKV_LACING_INVALID
    ));
}

/// A laced Block on a track with no DefaultDuration falls back to spreading
/// the BlockGroup's BlockDuration across the lace: the Block's duration covers
/// the WHOLE lace (RFC 9559 §5.1.3.5), not each frame.
#[test]
fn laced_block_duration_is_divided_across_the_lace() {
    let mut block = vec![0x81u8, 0x00, 0x00, 0x04, 0x01]; // fixed-size, 2 frames, no KEY
    block.extend_from_slice(&[0x11, 0x22]);
    let mut bg_body = Vec::new();
    ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
    ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
    bg_body.extend_from_slice(&block);
    // 48 ms for the pair → 24 ms per frame.
    ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 48).unwrap();

    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
    ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
    cluster.extend_from_slice(&bg_body);

    let specs = [TrackSpec::new(1, 2)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frames = drain(&mut stream);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, vec![0x11]);
    assert_eq!(frames[1].data, vec![0x22]);
    assert_eq!(frames[0].duration_ns, Some(24_000_000));
    assert_eq!(frames[1].pts, 24_000_000, "spaced by the derived duration");
}

// TrackNumber gaps must not divide the stream index (`TrackNumber - 1` was a past
// regression); a sub-44100 SamplingFrequency must map to Unknown, not silently 48 kHz.
#[test]
fn a_sub_44100_sampling_frequency_is_unknown_not_48k() {
    /// One TrackEntry body: an audio track with the given sampling frequency.
    fn audio_track_body(freq: f64) -> Vec<u8> {
        let mut audio = Vec::new();
        audio.push(super::ebml::SAMPLING_FREQUENCY as u8);
        audio.push(0x88); // 8-byte float payload
        audio.extend_from_slice(&freq.to_be_bytes());
        audio.push(super::ebml::CHANNELS as u8);
        audio.extend_from_slice(&[0x81, 0x02]);

        let mut body = Vec::new();
        body.push(super::ebml::TRACK_NUMBER as u8);
        body.extend_from_slice(&[0x81, 0x01]);
        body.push(super::ebml::TRACK_TYPE as u8);
        body.extend_from_slice(&[0x81, super::ebml::TRACK_TYPE_AUDIO as u8]);
        body.push(super::ebml::CODEC_ID as u8);
        let cid = b"A_AC3";
        body.push(0x80 | cid.len() as u8);
        body.extend_from_slice(cid);
        body.push(super::ebml::AUDIO as u8);
        body.push(0x80 | audio.len() as u8);
        body.extend_from_slice(&audio);
        body
    }

    for (freq, want) in [
        (32000.0f64, SampleRate::Unknown),
        (16000.0, SampleRate::Unknown),
        (44100.0, SampleRate::S44_1),
        (48000.0, SampleRate::S48),
        (96000.0, SampleRate::S96),
    ] {
        let body = audio_track_body(freq);
        let mut cur = std::io::Cursor::new(body.clone());
        let parsed = super::parse_track(&mut cur, body.len() as u64)
            .unwrap_or_else(|e| panic!("track with {freq} Hz must parse: {e}"));
        let got = match parsed
            .stream
            .as_ref()
            .expect("an audio track yields a stream")
        {
            Stream::Audio(a) => a.sample_rate,
            other => panic!("expected an audio stream, got {other:?}"),
        };
        assert_eq!(got, want, "{freq} Hz must map to {want:?}, got {got:?}");
    }
}

#[test]
fn sparse_track_numbers_route_to_the_right_stream() {
    let video = [0x81u8, 0x00, 0x00, 0x80, 0x11]; // TrackNumber 1
    let audio = [0x83u8, 0x00, 0x0A, 0x80, 0x22]; // TrackNumber 3, rel_ts 10
    let buttons = [0x82u8, 0x00, 0x00, 0x80, 0x33]; // TrackNumber 2 — dropped track

    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    for b in [video.as_slice(), buttons.as_slice(), audio.as_slice()] {
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, b.len() as u64).unwrap();
        cluster.extend_from_slice(b);
    }

    let specs = [
        TrackSpec::new(1, 1),  // video   → stream 0
        TrackSpec::new(2, 18), // buttons → dropped, no stream
        TrackSpec::new(3, 2),  // audio   → stream 1
    ];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(
        stream.info().streams.len(),
        2,
        "the buttons track is dropped"
    );
    let frames = drain(&mut stream);
    assert_eq!(
        frames.len(),
        2,
        "the video and audio blocks both survive; only the dropped track's do not"
    );
    assert_eq!(frames[0].track, 0, "TrackNumber 1 → stream 0");
    assert_eq!(frames[0].data, vec![0x11]);
    assert_eq!(frames[1].track, 1, "TrackNumber 3 → stream 1, not dropped");
    assert_eq!(frames[1].data, vec![0x22]);
}

/// A descending TrackEntry order is legal too: the map is by number, not by
/// position, and a block must never be attributed to the wrong codec parser.
#[test]
fn descending_track_numbers_route_by_number_not_position() {
    let first = [0x87u8, 0x00, 0x00, 0x80, 0xAA]; // TrackNumber 7
    let second = [0x84u8, 0x00, 0x00, 0x80, 0xBB]; // TrackNumber 4
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    for b in [first.as_slice(), second.as_slice()] {
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, b.len() as u64).unwrap();
        cluster.extend_from_slice(b);
    }
    let specs = [TrackSpec::new(7, 1), TrackSpec::new(4, 2)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frames = drain(&mut stream);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].track, 0, "TrackNumber 7 is the FIRST TrackEntry");
    assert_eq!(frames[1].track, 1, "TrackNumber 4 is the second");
}

/// `codec_private(stream_idx)` is keyed by TrackNumber internally, so it must
/// translate through the same map — not assume `stream_idx + 1`.
#[test]
fn codec_private_resolves_through_the_track_number_map() {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    let specs = [
        TrackSpec::new(1, 1).with_codec_private(&[0x01, 0x02]),
        TrackSpec::new(2, 18).with_codec_private(&[0xDE, 0xAD]),
        TrackSpec::new(3, 2).with_codec_private(&[0x03, 0x04]),
    ];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
    let stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream.codec_private(0), Some(vec![0x01, 0x02]));
    assert_eq!(
        stream.codec_private(1),
        Some(vec![0x03, 0x04]),
        "stream 1 is TrackNumber 3 — not TrackNumber 2 (the dropped track)"
    );
    assert_eq!(stream.codec_private(2), None, "no third stream");
}

/// The signed-VINT bias of RFC 9559 §10.3.3 exactly as the spec's own EBML
/// lacing example encodes it (800 then 500 → a delta of -300).
#[test]
fn lace_vint_matches_the_spec_worked_example() {
    // 800 = 0x320, encoded as a 2-octet VINT: 0x43 0x20.
    assert_eq!(lace_vint(&[0x43, 0x20]), Some((800, 2)));
    // -300 as a 2-octet signed VINT: 0x5E 0xD3 (value 0x1ED3 minus bias 8191).
    assert_eq!(lace_svint(&[0x5E, 0xD3]), Some((-300, 2)));
    // 1-octet forms: 0x81 → 1; signed 0x80 → -(2^6-1) = -63.
    assert_eq!(lace_vint(&[0x81]), Some((1, 1)));
    assert_eq!(lace_svint(&[0x80]), Some((-63, 1)));
    // A first octet of 0 has no VINT_MARKER within 8 octets → unrepresentable.
    assert!(lace_vint(&[0x00, 0x01]).is_none());
    // Truncated: a 2-octet marker with only one octet available.
    assert!(lace_vint(&[0x43]).is_none());
}

// ── finish(): the only thing that produces a valid file ───────────────

/// A `Cursor<Vec<u8>>` the test still owns after `MkvStream` takes it, so the
/// bytes the writer actually produced can be inspected (and re-opened).
#[derive(Clone)]
struct SharedOut(std::sync::Arc<std::sync::Mutex<Cursor<Vec<u8>>>>);

impl SharedOut {
    fn new() -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(Cursor::new(
            Vec::new(),
        ))))
    }
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().get_ref().clone()
    }
}

impl io::Write for SharedOut {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

impl io::Seek for SharedOut {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.0.lock().unwrap().seek(pos)
    }
}

fn h264_title() -> crate::disc::DiscTitle {
    use crate::disc::{
        Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    let mut t = DiscTitle {
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })],
        ..DiscTitle::empty()
    };
    t.playlist = "FinishTitle".into();
    // A minimal avcC so the written TrackEntry carries a CodecPrivate.
    t.codec_privates = vec![Some(vec![0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1])];
    t
}

// Past MAX_PENDING_FRAMES with no video picture the muxer is built anyway (the audio
// prefix precedes any keyframe cluster, so the muxer drops it); later frames must mux.
#[test]
fn the_pending_frame_cap_builds_the_muxer_and_keeps_later_frames() {
    let out = SharedOut::new();
    let mut title = h264_title();
    title
        .streams
        .push(crate::disc::Stream::Audio(crate::disc::AudioStream {
            pid: 0x1100,
            codec: crate::disc::Codec::Ac3,
            channels: crate::disc::AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: crate::disc::SampleRate::S48,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: String::new(),
        }));
    title.codec_privates.push(None);
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    let frame = |track, pts: i64, keyframe, byte| crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track,
        pts,
        keyframe,
        data: vec![byte; 8],
        duration_ns: None,
    };
    let n = MAX_PENDING_FRAMES as i64;
    for i in 0..=n {
        s.write(&frame(1, i * 1_000_000, true, 0xA0)).unwrap();
    }
    assert!(
        matches!(s.mode, Mode::Write(WriteMode::Active(_))),
        "cap builds"
    );
    for i in 0..10 {
        let pts = (n + 1 + i) * 1_000_000;
        s.write(&frame(0, pts, i == 0, 0xB0)).unwrap();
        s.write(&frame(1, pts, true, 0xA1)).unwrap();
    }
    s.finish().unwrap();
    let mut r = MkvStream::open(Cursor::new(out.bytes())).unwrap();
    let (mut video, mut late_audio) = (0, 0);
    while let Some(f) = r.read().unwrap() {
        match (f.track, f.data[0]) {
            (0, 0xB0) => video += 1,
            (1, 0xA1) => late_audio += 1,
            (1, 0xA0) => {}
            other => panic!("frame on the wrong track: {other:?}"),
        }
    }
    assert_eq!((video, late_audio), (10, 10));
}

// Writer that errors while `fail` is set (a transient disk/pipe failure).
struct Flaky(SharedOut, std::sync::Arc<std::sync::atomic::AtomicBool>);

impl io::Write for Flaky {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.1.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::ErrorKind::Other.into());
        }
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl io::Seek for Flaky {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

#[test]
fn a_failed_frame_write_fails_the_later_finish() {
    let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = Flaky(SharedOut::new(), fail.clone());
    let mut s = MkvStream::create(Box::new(w), &h264_title(), None).unwrap();
    let frame = |pts, len| crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts,
        keyframe: true,
        data: vec![0; len],
        duration_ns: None,
    };
    s.write(&frame(0, 16)).unwrap();
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(s.write(&frame(40_000_000, 4 << 20)).is_err());
    fail.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(s.finish().is_err(), "a torn cluster must not finish as Ok");
}

/// An MPEG-2 multichannel extension track (DVD `0xD0|n`) has no Matroska mapping: it is
/// reported as excluded once its packets arrive, its frames and timing are ignored, and the
/// tracks after it keep their frames.
#[test]
fn mp2_extension_track_is_left_out_and_later_tracks_still_mux() {
    let out = SharedOut::new();
    let mut title = h264_title();
    let mp2 = |pid, label: &str| {
        crate::disc::Stream::Audio(crate::disc::AudioStream {
            pid,
            codec: crate::disc::Codec::Mp2,
            channels: crate::disc::AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: crate::disc::SampleRate::S48,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: label.into(),
        })
    };
    title
        .streams
        .push(mp2(0x00D0, crate::disc::MP2_EXTENSION_LABEL));
    title.streams.push(mp2(0x00C0, ""));
    title.codec_privates.extend([None, None]);
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    // Declared only (IFO coding mode 3): nothing lost yet, nothing reported.
    assert!(s.undelivered_streams().is_empty());
    s.set_track_timing(1, Default::default()).unwrap();
    let frame = |track, data: Vec<u8>, keyframe| crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track,
        pts: 0,
        keyframe,
        data,
        duration_ns: None,
    };
    s.write(&frame(0, vec![0xA1; 48], true)).unwrap();
    s.write(&frame(1, vec![0x7F, 0xF0, 0x01], true)).unwrap();
    s.write(&frame(2, vec![0xB2; 16], true)).unwrap();
    assert_eq!(
        s.undelivered_streams(),
        vec![1],
        "reported once its packets arrived"
    );
    s.finish().unwrap();
    let mut back = MkvStream::open(Cursor::new(out.bytes())).unwrap();
    assert_eq!(back.info().streams.len(), 2, "video + MP2 base only");
    let mut got = Vec::new();
    while let Some(f) = back.read().unwrap() {
        got.push((f.track, f.data));
    }
    assert_eq!(got, vec![(0, vec![0xA1; 48]), (1, vec![0xB2; 16])]);
}

// `finish()` turns a stream of frames into a FILE (activates + finalizes the muxer); proven
// by reading the output back through this crate's own reader.
#[test]
fn finish_produces_a_readable_mkv_with_every_written_frame() {
    let out = SharedOut::new();
    let title = h264_title();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();

    let frames = [
        (0i64, true, vec![0xA1u8; 48]),
        (41_708_333i64, false, vec![0xB2u8; 24]),
        (83_416_666i64, false, vec![0xC3u8; 96]),
    ];
    for (pts, keyframe, data) in &frames {
        s.write(&crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: *pts,
            keyframe: *keyframe,
            data: data.clone(),
            duration_ns: None,
        })
        .unwrap();
    }
    s.finish().unwrap();

    let bytes = out.bytes();
    assert!(!bytes.is_empty(), "finish must have produced a file");

    let mut back = MkvStream::open(Cursor::new(bytes)).unwrap();
    let mut got = Vec::new();
    while let Some(f) = back.read().unwrap() {
        got.push(f);
    }
    assert_eq!(
        got.len(),
        frames.len(),
        "every frame survives the round trip"
    );
    for (i, (pts, keyframe, data)) in frames.iter().enumerate() {
        assert_eq!(&got[i].data, data, "frame {i} payload");
        assert_eq!(got[i].keyframe, *keyframe, "frame {i} keyframe flag");
        // Matroska block timestamps are milliseconds at the default
        // TimestampScale (RFC 9559 §5.1.2.6), so the ns PTS round-trips to
        // the nearest ms.
        assert_eq!(
            got[i].pts / 1_000_000,
            pts / 1_000_000,
            "frame {i} timestamp"
        );
    }
    assert_eq!(
        back.info().playlist,
        "FinishTitle",
        "the Segment Title written at finish survives"
    );
    assert_eq!(
        back.codec_private(0).as_deref(),
        Some(&[0x01u8, 0x64, 0x00, 0x1F, 0xFF, 0xE1][..]),
        "the TrackEntry CodecPrivate written at finish survives"
    );
}

// A title that produced NO frames must NOT finish successfully — MkvMuxer's zero-frame
// guard raises E6008, not a clusterless-but-"complete" MKV.
#[test]
fn finish_refuses_a_zero_frame_title_instead_of_reporting_success() {
    let out = SharedOut::new();
    let title = h264_title();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();

    let err = s
        .finish()
        .expect_err("a title that muxed no frames must not finish successfully");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let code = format!("E{}", crate::error::Error::MkvInvalid.code());
    assert!(
        err.to_string().contains(&code),
        "expected the empty-mux code {code}, got {err}"
    );
    assert!(
        crate::error::is_skippable_title_stub(&err),
        "the code raised must be the one the title loop classifies as a stub"
    );
}

// `headers_ready()` is unconditionally true for Matroska (Tracks precedes the first
// Cluster); the untrusted-size caps' magnitude, not just existence, is pinned here.
#[test]
fn the_untrusted_size_caps_admit_real_discs_and_reject_hostile_ones() {
    // A UHD HEVC keyframe runs to a few MB — that has to get through.
    assert_eq!(
        checked_size(2 * 1024 * 1024, MAX_BLOCK_SIZE).unwrap(),
        2 * 1024 * 1024
    );
    assert!(checked_size(65 * 1024 * 1024, MAX_BLOCK_SIZE).is_err());
    // hvcC/avcC/setup blobs are a few KB, but the cap must leave real
    // headroom above them.
    assert!(checked_size(2 * 1024 * 1024, MAX_CODEC_PRIVATE).is_ok());
    assert!(checked_size(17 * 1024 * 1024, MAX_CODEC_PRIVATE).is_err());
    // A 4 KB Title / TrackName is unremarkable; 64 KB is the ceiling.
    assert!(checked_size(4096, MAX_STRING_LEN).is_ok());
    assert!(checked_size(65 * 1024, MAX_STRING_LEN).is_err());
    // An EBML unsigned int is at most 8 octets wide (RFC 8794).
    assert!(checked_size(8, MAX_UINT_LEN).is_ok());
    assert!(checked_size(9, MAX_UINT_LEN).is_err());
}

/// Build a header whose Tracks carries one TrackEntry per
/// `(TrackNumber, TrackType, CodecID)` triple.
fn mkv_with_codec_ids(entries: &[(u64, u64, &str)]) -> Vec<u8> {
    let mut tracks = Vec::new();
    for (tnum, ttype, codec_id) in entries {
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, *tnum).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, *ttype).unwrap();
        ebml::write_string(&mut entry, ebml::CODEC_ID, codec_id).unwrap();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
    }
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out
}

fn stream_codec(s: &crate::disc::Stream) -> Codec {
    match s {
        crate::disc::Stream::Video(v) => v.codec,
        crate::disc::Stream::Audio(a) => a.codec,
        crate::disc::Stream::Subtitle(s) => s.codec,
    }
}

// CodecID is the only thing in a TrackEntry that says which parser the
// elementary stream belongs to; every branch of the codec ladder is
// exercised so a mis-wired one can't silently ship with the wrong parser.
#[test]
fn every_matroska_codec_id_decodes_to_its_codec_on_read_back() {
    let cases: &[(u64, &str, Codec)] = &[
        (1, ebml::CODEC_HEVC, Codec::Hevc),
        (1, ebml::CODEC_H264, Codec::H264),
        (1, ebml::CODEC_VC1, Codec::Vc1),
        (1, ebml::CODEC_MPEG2, Codec::Mpeg2),
        (2, ebml::CODEC_AC3, Codec::Ac3),
        (2, ebml::CODEC_EAC3, Codec::Ac3Plus),
        (2, ebml::CODEC_TRUEHD, Codec::TrueHd),
        (2, ebml::CODEC_DTS, Codec::Dts),
        // No BitDepth and no blocks to infer it from: not presented as LPCM.
        (2, ebml::CODEC_PCM_BE, Codec::Unknown(0)),
        (2, ebml::CODEC_AAC, Codec::Aac),
        (2, ebml::CODEC_MP2, Codec::Mp2),
        (2, ebml::CODEC_MP3, Codec::Mp3),
        (2, ebml::CODEC_FLAC, Codec::Flac),
        (2, ebml::CODEC_OPUS, Codec::Opus),
        (17, ebml::CODEC_PGS, Codec::Pgs),
        (17, ebml::CODEC_VOBSUB, Codec::DvdSub),
        // An ID this crate does not carry stays Unknown — never silently
        // aliased onto a neighbouring codec.
        (2, "A_VORBIS", Codec::Unknown(0)),
    ];
    let entries: Vec<(u64, u64, &str)> = cases
        .iter()
        .enumerate()
        .map(|(i, (ttype, codec_id, _))| (i as u64 + 1, *ttype, *codec_id))
        .collect();
    let stream = MkvStream::open(Cursor::new(mkv_with_codec_ids(&entries))).unwrap();
    let streams = &stream.info().streams;
    assert_eq!(streams.len(), cases.len(), "every TrackEntry kept a stream");
    for (i, (_, codec_id, expected)) in cases.iter().enumerate() {
        assert_eq!(
            stream_codec(&streams[i]),
            *expected,
            "CodecID {codec_id} must decode to {expected:?}"
        );
    }
}

// A TrackType this crate cannot carry (18 = buttons) is DROPPED; the three
// it can carry each build the matching stream kind (subtitle, TrackType
// 17, was previously untested and could silently drop every subtitle).
#[test]
fn track_types_map_to_stream_kinds_and_unsupported_ones_are_dropped() {
    let bytes = mkv_with_codec_ids(&[
        (1, 1, ebml::CODEC_H264),
        (2, 2, ebml::CODEC_AC3),
        (3, 17, ebml::CODEC_PGS),
        (4, 18, "B_BUTTONS"),
    ]);
    let stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let streams = &stream.info().streams;
    assert_eq!(streams.len(), 3, "the buttons track carries no stream");
    assert!(matches!(streams[0], crate::disc::Stream::Video(_)));
    assert!(matches!(streams[1], crate::disc::Stream::Audio(_)));
    assert!(
        matches!(streams[2], crate::disc::Stream::Subtitle(_)),
        "TrackType 17 must produce a SubtitleStream, not vanish"
    );
}

// Matroska with one TrackEntry per `(number, type, CodecID, extra)`, `extra` being
// pre-encoded children appended after the number, type and CodecID.
fn mkv_with_tracks(entries: &[(u64, u64, &str, &[u8])]) -> Vec<u8> {
    let mut tracks = Vec::new();
    for (tnum, ttype, codec_id, extra) in entries {
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, *tnum).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, *ttype).unwrap();
        ebml::write_string(&mut entry, ebml::CODEC_ID, codec_id).unwrap();
        entry.extend_from_slice(extra);
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
    }
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out
}

fn mkv_with_track_extra(ttype: u64, codec_id: &str, extra: &[u8]) -> Vec<u8> {
    mkv_with_tracks(&[(1, ttype, codec_id, extra)])
}

// Retained CodecPrivate across all tracks is bounded, not just each blob.
#[test]
fn total_codec_private_across_tracks_is_bounded() {
    let mut extra = Vec::new();
    ebml::write_binary(
        &mut extra,
        ebml::CODEC_PRIVATE,
        &vec![0u8; MAX_CODEC_PRIVATE as usize],
    )
    .unwrap();
    let ok = mkv_with_tracks(&[
        (1, 2, ebml::CODEC_AC3, &extra),
        (2, 2, ebml::CODEC_AC3, &extra),
    ]);
    assert!(MkvStream::open(Cursor::new(ok)).is_ok());
    let many: Vec<_> = (1..=3)
        .map(|n| (n, 2, ebml::CODEC_AC3, &extra[..]))
        .collect();
    let err = MkvStream::open(Cursor::new(mkv_with_tracks(&many))).err();
    let code = format!("E{}", crate::error::E_MKV_SOURCE_INVALID);
    assert!(err.is_some_and(|e| e.to_string().starts_with(&code)));
}

// V_MS/VFW/FOURCC is any VFW codec: DivX / XviD must not be relabelled VC-1.
#[test]
fn vfw_track_is_vc1_only_when_the_fourcc_says_so() {
    for (fourcc, want) in [
        (b"WVC1", Codec::Vc1),
        (b"wvc1", Codec::Vc1),
        (b"DIVX", Codec::Unknown(0)),
        (b"XVID", Codec::Unknown(0)),
    ] {
        let mut bih = vec![0u8; 40];
        bih[16..20].copy_from_slice(fourcc);
        let mut extra = Vec::new();
        ebml::write_binary(&mut extra, ebml::CODEC_PRIVATE, &bih).unwrap();
        let bytes = mkv_with_track_extra(1, ebml::CODEC_VC1, &extra);
        let stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let got = stream.info().streams.first().map(stream_codec);
        assert_eq!(got, Some(want), "fourcc {fourcc:?}");
    }
}

// RFC 9559: an Audio master without Channels means one channel.
#[test]
fn audio_without_channels_element_reads_as_mono() {
    let mut audio = Vec::new();
    ebml::write_float(&mut audio, ebml::SAMPLING_FREQUENCY, 48000.0).unwrap();
    let mut extra = Vec::new();
    ebml::write_id(&mut extra, ebml::AUDIO).unwrap();
    ebml::write_size(&mut extra, audio.len() as u64).unwrap();
    extra.extend_from_slice(&audio);
    let bytes = mkv_with_track_extra(2, ebml::CODEC_AC3, &extra);
    let stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let Some(crate::disc::Stream::Audio(a)) = stream.info().streams.first() else {
        panic!("audio stream expected");
    };
    assert_eq!(a.channels, crate::disc::AudioChannels::Mono);
}

fn three_track_title() -> crate::disc::DiscTitle {
    use crate::disc::{
        AudioStream, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream,
        SubtitleStream, VideoStream,
    };
    DiscTitle {
        streams: vec![
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::H264,
                resolution: Resolution::R720p,
                frame_rate: FrameRate::F24,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                display_aspect: None,
                secondary: false,
                label: "Feature".into(),
                measured_cicp: None,
            }),
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::Ac3,
                channels: crate::disc::AudioChannels::Surround51,
                language: "eng".into(),
                sample_rate: crate::disc::SampleRate::S48,
                secondary: false,
                purpose: crate::disc::LabelPurpose::Normal,
                label: "English (Dolby Digital 5.1)".into(),
            }),
            Stream::Subtitle(SubtitleStream {
                pid: 0x1200,
                codec: Codec::DvdSub,
                language: "fra".into(),
                forced: true,
                qualifier: crate::disc::LabelQualifier::None,
                codec_data: None,
            }),
        ],
        ..DiscTitle::empty()
    }
}

// Round-trip a real three-track title and check the TrackEntry metadata (language, name,
// forced flag, resolution, channels) survived — none of it was previously asserted.
#[test]
fn track_entry_metadata_survives_a_write_read_round_trip() {
    let out = SharedOut::new();
    let title = three_track_title();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xA1; 32],
        duration_ns: None,
    })
    .unwrap();
    s.finish().unwrap();

    let back = MkvStream::open(Cursor::new(out.bytes())).unwrap();
    let streams = &back.info().streams;
    assert_eq!(streams.len(), 3, "all three tracks come back");

    match &streams[0] {
        crate::disc::Stream::Video(v) => {
            assert_eq!(v.codec, Codec::H264);
            // PixelHeight is the only dimension the reader reconstructs from.
            // Without the Video arm it would read 0 and report R480p.
            assert_eq!(
                v.resolution,
                crate::disc::Resolution::R720p,
                "the resolution must come from the written PixelHeight (720)"
            );
            assert_eq!(v.label, "Feature", "TrackName survives as the label");
        }
        other => panic!("expected a video stream, got {other:?}"),
    }
    match &streams[1] {
        crate::disc::Stream::Audio(a) => {
            assert_eq!(a.codec, Codec::Ac3);
            assert_eq!(a.language, "eng", "the audio Language must survive");
            assert_eq!(
                a.channels,
                crate::disc::AudioChannels::Surround51,
                "the Channels element must survive (5.1, not the Matroska default of 1)"
            );
            assert_eq!(a.sample_rate, crate::disc::SampleRate::S48);
            assert_eq!(a.label, "English (Dolby Digital 5.1)");
        }
        other => panic!("expected an audio stream, got {other:?}"),
    }
    match &streams[2] {
        crate::disc::Stream::Subtitle(sub) => {
            assert_eq!(sub.codec, Codec::DvdSub);
            assert_eq!(sub.language, "fra", "the subtitle Language must survive");
            assert!(
                sub.forced,
                "FlagForced must survive — a forced-narrative subtitle that \
                     round-trips as optional stops being shown at all"
            );
        }
        other => panic!("expected a subtitle stream, got {other:?}"),
    }
    // A non-forced subtitle must come back non-forced (the flag is read, not
    // assumed): rebuild with forced = false and check the other direction.
    let mut relaxed = three_track_title();
    if let crate::disc::Stream::Subtitle(sub) = &mut relaxed.streams[2] {
        sub.forced = false;
    }
    let out2 = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out2.clone()), &relaxed, None).unwrap();
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xA1; 32],
        duration_ns: None,
    })
    .unwrap();
    s.finish().unwrap();
    let back = MkvStream::open(Cursor::new(out2.bytes())).unwrap();
    match &back.info().streams[2] {
        crate::disc::Stream::Subtitle(sub) => assert!(!sub.forced),
        other => panic!("expected a subtitle stream, got {other:?}"),
    }
}

/// Reach into a still-pending write stream's track list.
fn pending_tracks(s: &MkvStream) -> &[MkvTrack] {
    match &s.mode {
        Mode::Write(WriteMode::Pending(p)) => &p.tracks,
        _ => panic!("expected a pending write stream"),
    }
}

// FlagDefault: only ONE video and ONE audio track may carry it; this de-duplication (not
// the muxer) is the only thing that enforces that.
#[test]
fn only_the_first_video_and_first_audio_track_are_default() {
    use crate::disc::{
        AudioStream, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    let video = |label: &str| {
        Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: label.into(),
            measured_cicp: None,
        })
    };
    let audio = |label: &str| {
        Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: crate::disc::AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: crate::disc::SampleRate::S48,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: label.into(),
        })
    };
    let title = DiscTitle {
        streams: vec![
            video("Angle 1"),
            video("Angle 2"),
            audio("English"),
            audio("French"),
        ],
        ..DiscTitle::empty()
    };
    let s = MkvStream::create(Box::new(SharedOut::new()), &title, None).unwrap();
    let flags: Vec<bool> = pending_tracks(&s).iter().map(|t| t.is_default).collect();
    assert_eq!(
        flags,
        vec![true, false, true, false],
        "exactly the first video and the first audio are default"
    );
}

// Deferred activation waits for the PRIMARY VIDEO track's first frame (the
// first track whose type is video, not track 0) — audio commonly comes
// first in a Blu-ray PMT, so picking track 0 would drop the measured order.
#[test]
fn the_activation_trigger_is_the_first_video_track_not_the_first_track() {
    use crate::disc::{
        AudioStream, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    let title = DiscTitle {
        streams: vec![
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::Ac3,
                channels: crate::disc::AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: crate::disc::SampleRate::S48,
                secondary: false,
                purpose: crate::disc::LabelPurpose::Normal,
                label: String::new(),
            }),
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Mpeg2,
                resolution: Resolution::R576i,
                frame_rate: FrameRate::F25,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt470bg,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            }),
        ],
        ..DiscTitle::empty()
    };
    let s = MkvStream::create(Box::new(SharedOut::new()), &title, None).unwrap();
    match &s.mode {
        Mode::Write(WriteMode::Pending(p)) => assert_eq!(
            p.video_track,
            Some(1),
            "the video track is index 1, not index 0"
        ),
        _ => panic!("expected a pending write stream"),
    }
}

// Bytes before the first Cluster (Tracks header lives there); bounding the
// scans below to this region stops a coincidental byte pair in a
// SimpleBlock payload from spoofing a match. Cluster ID = 0x1F43B675.
fn tracks_region(data: &[u8]) -> &[u8] {
    data.windows(4)
        .position(|w| w == [0x1F, 0x43, 0xB6, 0x75])
        .map_or(data, |p| &data[..p])
}

/// Locate the first TrackEntry's `FieldOrder` (0x9D) inside the Video master
/// of a muxed file, or `None` when the element was omitted.
fn muxed_field_order(data: &[u8]) -> Option<u8> {
    // FieldOrder is a 1-byte uint child of Video: ID 0x9D, size 0x81, value.
    tracks_region(data)
        .windows(3)
        .find(|w| w[0] == 0x9D && w[1] == 0x81)
        .map(|w| w[2])
}

/// Locate the first TrackEntry's `FlagInterlaced` (0x9A) inside the Video
/// master of a muxed file. Always written, so `None` means the element is
/// missing entirely.
fn muxed_flag_interlaced(data: &[u8]) -> Option<u64> {
    tracks_region(data)
        .windows(3)
        .find(|w| w[0] == 0x9A && w[1] == 0x81)
        .map(|w| w[2] as u64)
}

// A DVD declared 480i/576i but CODED progressive must ship `FlagInterlaced=progressive` —
// the measured bitstream overrides the declared resolution.
#[test]
fn a_progressive_picture_on_a_declared_interlaced_disc_ships_as_progressive() {
    use crate::disc::{
        ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};
    let title = DiscTitle {
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            // What the IFO declares. The bitstream below disagrees, and the
            // bitstream is the source of truth.
            resolution: Resolution::R576i,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })],
        ..DiscTitle::empty()
    };
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: Some(PictureInfo::mpeg2(
            CodingType::I,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                // The measurement that matters — exactly what a real
                // animation DVD carries (progressive_frame set on every
                // picture while progressive_sequence stays 0).
                progressive_frame: true,
                progressive_sequence: false,
                frame_picture: true,
            },
        )),
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xBB; 16],
        duration_ns: None,
    })
    .unwrap();
    s.finish().unwrap();

    let data = out.bytes();
    assert_eq!(
        muxed_flag_interlaced(&data),
        Some(ebml::INTERLACED_PROGRESSIVE),
        "measured-progressive content must ship FlagInterlaced=progressive; \
             shipping the IFO's declared 576i makes players deinterlace it"
    );
    assert_eq!(
        muxed_field_order(&data),
        None,
        "progressive content has no field order — the element must be omitted, \
             not written as TFF from the (meaningless) top_field_first bit"
    );
}

// Whole-stream FlagInterlaced correction, PROMOTE direction: a genuinely
// interlaced feature whose FIRST picture is a progressive leader must not
// flip the whole track — the majority scan wins at finish().
#[test]
fn a_progressive_first_picture_on_a_mostly_interlaced_title_ships_interlaced() {
    use crate::disc::{
        ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};
    let title = DiscTitle {
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })],
        ..DiscTitle::empty()
    };
    let pic = |progressive: bool, ct: CodingType| {
        Some(PictureInfo::mpeg2(
            ct,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                progressive_frame: progressive,
                progressive_sequence: false,
                frame_picture: true,
            },
        ))
    };
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    // First picture: a lone progressive leader.
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: pic(true, CodingType::I),
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xBB; 16],
        duration_ns: None,
    })
    .unwrap();
    // The feature itself: genuinely interlaced pictures dominate.
    for i in 1..6 {
        s.write(&crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: pic(false, CodingType::P),
            source: None,
            track: 0,
            pts: i * 40_000_000,
            keyframe: false,
            data: vec![0xCC; 16],
            duration_ns: None,
        })
        .unwrap();
    }
    s.finish().unwrap();
    assert_eq!(
        muxed_flag_interlaced(&out.bytes()),
        Some(ebml::INTERLACED_INTERLACED),
        "a progressive FIRST picture on a mostly-interlaced title must not flip \
             the whole track to progressive — the majority scan wins"
    );
    assert_eq!(
        muxed_field_order(&out.bytes()),
        Some(ebml::FIELD_ORDER_TFF),
        "the promoted track carries the measured majority field order"
    );
}

// Whole-stream FlagInterlaced correction, DEMOTE direction: progressive
// film mis-declared 576i whose FIRST picture is interlaced-coded. Majority
// (progressive) wins: track ships progressive, FieldOrder is Void'd.
#[test]
fn an_interlaced_first_picture_on_a_mostly_progressive_title_ships_progressive() {
    use crate::disc::{
        ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};
    let title = DiscTitle {
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })],
        ..DiscTitle::empty()
    };
    let pic = |progressive: bool, ct: CodingType| {
        Some(PictureInfo::mpeg2(
            ct,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                progressive_frame: progressive,
                progressive_sequence: false,
                frame_picture: true,
            },
        ))
    };
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    // First picture: interlaced (TFF) → provisional interlaced + FieldOrder written.
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: pic(false, CodingType::I),
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xBB; 16],
        duration_ns: None,
    })
    .unwrap();
    // The feature itself: progressive pictures dominate.
    for i in 1..6 {
        s.write(&crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: pic(true, CodingType::P),
            source: None,
            track: 0,
            pts: i * 40_000_000,
            keyframe: false,
            data: vec![0xCC; 16],
            duration_ns: None,
        })
        .unwrap();
    }
    s.finish().unwrap();
    let data = out.bytes();
    assert_eq!(
        muxed_flag_interlaced(&data),
        Some(ebml::INTERLACED_PROGRESSIVE),
        "a mostly-progressive title must ship progressive even when its first \
             picture was interlaced-coded"
    );
    assert_eq!(
        muxed_field_order(&data),
        None,
        "a track demoted to progressive must not keep its provisional FieldOrder"
    );
}

// FlagInterlaced TIE case: equal progressive/interlaced counts resolve to
// PROGRESSIVE (a tie is not a majority); pins the strict-`>` tie-break.
#[test]
fn an_even_split_of_scan_types_ships_progressive_not_interlaced() {
    use crate::disc::{
        ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};
    let title = DiscTitle {
        streams: vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })],
        ..DiscTitle::empty()
    };
    let pic = |progressive: bool, ct: CodingType| {
        Some(PictureInfo::mpeg2(
            ct,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                progressive_frame: progressive,
                progressive_sequence: false,
                frame_picture: true,
            },
        ))
    };
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    // First picture progressive (provisional progressive), then an exact 2:2
    // split → a tie.
    let scans = [true, true, false, false];
    for (i, prog) in scans.iter().enumerate() {
        s.write(&crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: pic(*prog, if i == 0 { CodingType::I } else { CodingType::P }),
            source: None,
            track: 0,
            pts: i as i64 * 40_000_000,
            keyframe: i == 0,
            data: vec![0xBB; 16],
            duration_ns: None,
        })
        .unwrap();
    }
    s.finish().unwrap();
    assert_eq!(
        muxed_flag_interlaced(&out.bytes()),
        Some(ebml::INTERLACED_PROGRESSIVE),
        "a 2:2 tie is not a majority — the track must stay progressive"
    );
}

// THE deferred-activation contract, end to end: the field order MEASURED from the first
// coded picture must reach the FILE, not just `apply_coding_to_track` in isolation.
#[test]
fn the_measured_field_order_reaches_the_written_file() {
    use crate::disc::{
        AudioStream, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
    };
    use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};
    let title = DiscTitle {
        streams: vec![
            Stream::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::Ac3,
                channels: crate::disc::AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: crate::disc::SampleRate::S48,
                secondary: false,
                purpose: crate::disc::LabelPurpose::Normal,
                label: String::new(),
            }),
            Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Mpeg2,
                resolution: Resolution::R576i, // interlaced → FieldOrder matters
                frame_rate: FrameRate::F25,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt470bg,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            }),
        ],
        ..DiscTitle::empty()
    };
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    // Audio arrives first and carries NO coding — it must be buffered, not
    // used to build the header.
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xAA; 16],
        duration_ns: None,
    })
    .unwrap();
    // The first coded picture on the VIDEO track measures top-field-first.
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: Some(PictureInfo::mpeg2(
            CodingType::I,
            Mpeg2Coding {
                top_field_first: true,
                repeat_first_field: false,
                progressive_frame: false,
                progressive_sequence: false,
                frame_picture: true,
            },
        )),
        source: None,
        track: 1,
        pts: 0,
        keyframe: true,
        data: vec![0xBB; 16],
        duration_ns: None,
    })
    .unwrap();
    s.finish().unwrap();

    assert_eq!(
        muxed_field_order(&out.bytes()),
        Some(ebml::FIELD_ORDER_TFF),
        "the MEASURED top-field-first must be written to the file; an omitted \
             or UNDETERMINED FieldOrder means the measurement never got there"
    );
    // Pin the interlaced direction at the muxed-byte level too: a swap of the
    // INTERLACED_INTERLACED/INTERLACED_PROGRESSIVE constants in the writer
    // would otherwise pass every other test in this file.
    assert_eq!(
        muxed_flag_interlaced(&out.bytes()),
        Some(ebml::INTERLACED_INTERLACED),
        "a MEASURED-interlaced track must ship FlagInterlaced=interlaced"
    );
}

// The dependent (right-eye) view is matched to its base frame BY PTS; any
// other rule attaches the wrong eye to the wrong frame (view-swap 3D bug).
#[test]
fn a_dependent_view_pairs_only_with_the_base_frame_of_the_same_pts() {
    let mut m = empty_merge();
    let dep = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);
    // Two base frames buffered, oldest first.
    assert!(m.ingest(&mvc_frame(0, 100, true, vec![0x11])).is_empty());
    assert!(m.ingest(&mvc_frame(0, 200, false, vec![0x22])).is_empty());
    // A dependent for the SECOND one arrives. It must attach to the pts=200
    // base — not to the oldest unpaired base it happens to find first.
    let e = m.ingest(&mvc_frame(2, 200, false, dep.clone()));
    assert!(
        e.is_empty(),
        "the pts=100 base is still unpaired, so nothing drains yet"
    );
    assert_eq!(
        m.pending_base[0].additional, None,
        "the pts=100 base must NOT have taken the pts=200 dependent"
    );
    assert_eq!(
        m.pending_base[1].additional.as_deref(),
        Some(dep.as_slice()),
        "the dependent belongs to the base with the matching PTS"
    );
}

// TimestampScale above `i64::MAX` casts to a negative scale (inverts the
// whole timeline), so it clamps to the 1 ms default like a declared zero.
#[test]
fn a_timestamp_scale_above_i64_max_falls_back_to_one_millisecond() {
    let build = |scale: u64| {
        let mut info = Vec::new();
        ebml::write_uint(&mut info, ebml::TIMESTAMP_SCALE, scale).unwrap();
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, info.len() as u64).unwrap();
        out.extend_from_slice(&info);
        // One track and a block at cluster tick 5 so the scale is observable
        // in the frame PTS.
        let block = [0x81u8, 0x00, 0x00, 0x80, 0xAB];
        let cluster = cluster_with_simple_block(5, &block);
        let mut tracks = Vec::new();
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        out.extend_from_slice(&cluster);
        out
    };
    // A scale that does not fit in i64 is nonsense; fall back to 1 ms so the
    // block at tick 5 lands at 5 ms, never at a huge negative PTS.
    let mut s = MkvStream::open(Cursor::new(build(u64::MAX))).unwrap();
    let f = s.read().unwrap().expect("one frame");
    assert_eq!(f.pts, 5 * 1_000_000, "clamped to the 1 ms default scale");
    // `i64::MAX` still fits a positive i64, so it's taken as the scale (the
    // multiply saturates rather than overflows) — the clamp is against values
    // that go NEGATIVE, not against large ones.
    let mut s = MkvStream::open(Cursor::new(build(i64::MAX as u64))).unwrap();
    let f = s.read().unwrap().expect("one frame");
    assert_eq!(
        f.pts,
        i64::MAX,
        "the tick→ns multiply saturates, never wraps"
    );
    // A legal scale is honoured verbatim.
    let mut s = MkvStream::open(Cursor::new(build(100_000))).unwrap();
    let f = s.read().unwrap().expect("one frame");
    assert_eq!(f.pts, 5 * 100_000, "a legal 0.1 ms scale is honoured");
}

// An EBML lace of exactly TWO frames stores exactly ONE size (the first);
// the second is the Block remainder — the boundary case where an off-by-one
// in the "read n-2 more sizes" loop would eat frame payload as size table.
#[test]
fn an_ebml_lace_of_exactly_two_frames_stores_one_size() {
    let mut block = vec![
        0x81, // TrackNumber 1
        0x00, 0x00, // rel_ts 0
        0x86, // KEY | LACING = 11b (EBML)
        0x01, // Lacing Head: 2 frames minus 1
        0x83, // first frame size = 3
    ];
    block.extend_from_slice(&[0xAA; 3]);
    block.extend_from_slice(&[0xBB; 5]);
    let specs = [TrackSpec::new(1, 2)];
    let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
    let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
    let frames = drain(&mut stream);
    assert_eq!(frames.len(), 2, "a two-frame lace yields two frames");
    assert_eq!(frames[0].data, vec![0xAA; 3]);
    assert_eq!(
        frames[1].data,
        vec![0xBB; 5],
        "the second frame is the whole Block remainder"
    );
}

// Several EBML size deltas in a row, mixed signs: each is relative to the PREVIOUS size,
// and a delta that drives a size below zero rejects the whole lace.
#[test]
fn an_ebml_lace_accumulates_signed_deltas_and_rejects_negative_sizes() {
    // Sizes 3, +2, -3, +2 = 3, 5, 2, 4; the last frame is the remainder (6).
    // 1-octet svint: 0x80 | (delta + 63).
    let mut body = vec![0x04, 0x83, 0xC1, 0xBC, 0xC1];
    let sizes = [3usize, 5, 2, 4, 6];
    for (i, n) in sizes.iter().enumerate() {
        body.extend(std::iter::repeat_n(i as u8 + 1, *n));
    }
    let frames = split_lacing(LACING_EBML, &body).expect("well-formed lace");
    assert_eq!(frames.len(), 5);
    for (i, (f, n)) in frames.iter().zip(sizes).enumerate() {
        assert_eq!(*f, vec![i as u8 + 1; n].as_slice(), "frame {i}");
    }
    // 3 then -4 = -1: negative size.
    assert_eq!(
        split_lacing(LACING_EBML, &[0x02, 0x83, 0xBB, 0, 0, 0]),
        None
    );
    // The delta VINT is missing.
    assert_eq!(split_lacing(LACING_EBML, &[0x02, 0x83]), None);
}

// The shortest usable (Simple)Block is 4 bytes (track VINT + rel-ts + flags,
// empty payload) — exactly the boundary of the short-block guards, where an
// off-by-one drops a legal frame or indexes past the buffer end.
#[test]
fn a_four_byte_block_is_the_shortest_legal_one_and_is_not_dropped() {
    let tracks = TrackTable::contiguous(1);
    let four = [0x81u8, 0x00, 0x00, 0x80];
    let frames = parse_block(&four, 0, 1_000_000, &tracks, None).unwrap();
    assert_eq!(
        frames.len(),
        1,
        "a header-only block is short, not absent — dropping it loses a frame"
    );
    assert!(frames[0].data.is_empty());
    // Three bytes cannot hold the header at all and must be skipped, not
    // indexed into.
    assert!(
        parse_block(&four[..3], 0, 1_000_000, &tracks, None)
            .unwrap()
            .is_empty()
    );
    assert!(
        parse_block(&[], 0, 1_000_000, &tracks, None)
            .unwrap()
            .is_empty()
    );
    // Same boundary with a WIDER track VINT: a 2-octet track number makes the
    // shortest legal block five bytes, unlike every other test's 1-octet form.
    let five = [0x40u8, 0x01, 0x00, 0x00, 0x80];
    let frames = parse_block(&five, 0, 1_000_000, &tracks, None).unwrap();
    assert_eq!(
        frames.len(),
        1,
        "a 2-octet track VINT still leaves a legal, if empty, block"
    );
    assert_eq!(frames[0].track, 0, "0x4001 is TrackNumber 1 → stream 0");
}

// The `Video` master's child walk must consume EXACTLY its declared bytes, not over-run
// into a following field (fine for this crate's own writer, but breaks on foreign MKVs).
#[test]
fn the_video_master_walk_stops_at_its_own_end_not_inside_the_next_field() {
    let mut video = Vec::new();
    ebml::write_uint(&mut video, ebml::PIXEL_HEIGHT, 720).unwrap();
    // A one-byte value: header 2 bytes, body 1 byte. Any accounting that
    // mixes up "header plus body" with anything else drifts here.
    ebml::write_uint(&mut video, ebml::FLAG_INTERLACED, 1).unwrap();

    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    ebml::write_string(&mut entry, ebml::CODEC_ID, ebml::CODEC_HEVC).unwrap();
    ebml::write_id(&mut entry, ebml::VIDEO).unwrap();
    ebml::write_size(&mut entry, video.len() as u64).unwrap();
    entry.extend_from_slice(&video);
    // Deliberately AFTER the Video master.
    ebml::write_string(&mut entry, ebml::TRACK_NAME, "After Video").unwrap();

    let mut tracks = Vec::new();
    ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
    tracks.extend_from_slice(&entry);

    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);

    let s = MkvStream::open(Cursor::new(out)).unwrap();
    match &s.info().streams[0] {
        crate::disc::Stream::Video(v) => {
            assert_eq!(v.resolution, crate::disc::Resolution::R720p);
            assert_eq!(
                v.label, "After Video",
                "a field following the Video master must still be read"
            );
        }
        other => panic!("expected a video stream, got {other:?}"),
    }
}

// A Dolby Vision enhancement layer is marked SECONDARY (never default video)
// when recognised by either of two label spellings; both must hold.
#[test]
fn a_dolby_vision_enhancement_layer_track_is_marked_secondary_by_either_label() {
    let build = |name: &str| {
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        ebml::write_string(&mut entry, ebml::CODEC_ID, ebml::CODEC_HEVC).unwrap();
        ebml::write_string(&mut entry, ebml::TRACK_NAME, name).unwrap();
        let mut tracks = Vec::new();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        let s = MkvStream::open(Cursor::new(out)).unwrap();
        match &s.info().streams[0] {
            crate::disc::Stream::Video(v) => v.secondary,
            other => panic!("expected a video stream, got {other:?}"),
        }
    };
    assert!(
        build("Dolby Vision EL"),
        "the long spelling marks secondary"
    );
    assert!(build("DV EL"), "the short spelling marks secondary too");
    assert!(
        !build("Main Feature"),
        "an ordinary video track is not secondary"
    );
}

// `DefaultDuration` spaces a laced Block's later frames (RFC 9559 §10.3.5);
// the reader treats 0 and absurd values as ABSENT (not a real duration).
#[test]
fn a_zero_or_absurd_default_duration_is_treated_as_absent() {
    // A two-frame EBML lace: frame 2's timestamp comes only from the track's
    // DefaultDuration, so the filter's verdict is directly observable.
    let laced = || {
        let mut block = vec![0x81u8, 0x00, 0x00, 0x86, 0x01, 0x83];
        block.extend_from_slice(&[0xAA; 3]);
        block.extend_from_slice(&[0xBB; 3]);
        block
    };
    let spacing = |ns: u64| -> (i64, Option<u64>) {
        let specs = [TrackSpec::new(1, 2).with_default_duration(ns)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &laced()));
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frames = drain(&mut stream);
        assert_eq!(frames.len(), 2);
        (frames[1].pts - frames[0].pts, frames[1].duration_ns)
    };
    assert_eq!(
        spacing(40_000_000),
        (40_000_000, Some(40_000_000)),
        "a real 40 ms frame period spaces the lace"
    );
    assert_eq!(
        spacing(0),
        (0, None),
        "DefaultDuration 0 is absent, not a zero-length frame period"
    );
    assert_eq!(
        spacing(2_000_000_000),
        (2_000_000_000, Some(2_000_000_000)),
        "a long-but-plausible 2 s period is still honoured"
    );
    assert_eq!(
        spacing(61 * 1_000_000_000),
        (0, None),
        "a period over a minute is nonsense for a frame and is discarded"
    );
}

/// A reader that hands back `size` bytes and then fails with a NON-EOF error
/// — a bad sector, a dropped network mount, a drive that wedges mid-read.
struct FailAfter {
    data: Vec<u8>,
    pos: usize,
}

impl Read for FailAfter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.data.len() {
            return Err(io::Error::other("device failure"));
        }
        let n = buf.len().min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

// Only a CLEAN end of stream ends a read; a disc-read failure partway
// through must PROPAGATE, not be mistaken for a complete rip.
#[test]
fn a_mid_stream_io_failure_propagates_instead_of_ending_the_stream() {
    // Header parses cleanly, then the cluster read hits a device error.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    let block = [0x81u8, 0x00, 0x00, 0x80, 0xAB];
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, block.len() as u64).unwrap();
    cluster.extend_from_slice(&block);
    let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
    let mut stream = MkvStream::open(FailAfter {
        data: bytes,
        pos: 0,
    })
    .expect("the header itself is intact");
    assert!(stream.read().unwrap().is_some(), "the one good frame");
    let e = stream
        .read()
        .expect_err("a device failure is NOT end of stream");
    assert_eq!(e.kind(), io::ErrorKind::Other);
}

// Same distinction while parsing the HEADER: a clean-EOF Segment stops the
// scan with whatever was found, but a device failure must surface.
#[test]
fn a_header_io_failure_propagates_instead_of_ending_the_scan() {
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    // The stream ends here with a device error rather than cleanly, before
    // Tracks was ever seen.
    let e = open_err(MkvStream::open(FailAfter { data: out, pos: 0 }));
    assert_eq!(
        e.kind(),
        io::ErrorKind::Other,
        "a device failure during the header scan must not look like EOF"
    );

    // ...whereas a clean truncation at the same point IS end of scan: the
    // title comes back with no tracks, no error.
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    let s = MkvStream::open(Cursor::new(out)).expect("a clean EOF ends the scan");
    assert!(s.info().streams.is_empty());
}

// The first subset SPS and first PPS win; a repeated parameter set (routine
// after a discontinuity) must not overwrite the one already captured.
#[test]
fn extract_mvc_params_keeps_the_first_parameter_set_of_each_kind() {
    const SECOND_SPS: [u8; 5] = [0x6F, 0x99, 0x11, 0x22, 0x33];
    const SECOND_PPS: [u8; 3] = [0x68, 0x77, 0x66];
    // The repeat has to appear BEFORE the other kind is found — the scan
    // stops as soon as it holds one of each.
    let (s, _) = extract_mvc_params(&lp(&[&SUBSET_SPS, &SECOND_SPS, &DEP_PPS]))
        .expect("both param sets present");
    assert_eq!(s, SUBSET_SPS, "the FIRST subset SPS is kept");
    let (_, p) = extract_mvc_params(&lp(&[&DEP_PPS, &SECOND_PPS, &SUBSET_SPS]))
        .expect("both param sets present");
    assert_eq!(p, DEP_PPS, "the FIRST PPS is kept");
}

#[test]
fn headers_are_ready_at_open_because_matroska_front_loads_them() {
    let out = SharedOut::new();
    let title = h264_title();
    let mut s = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
    s.write(&crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xA1; 48],
        duration_ns: None,
    })
    .unwrap();
    s.finish().unwrap();

    let back = MkvStream::open(Cursor::new(out.bytes())).unwrap();
    assert!(
        back.headers_ready(),
        "Matroska carries Tracks before the first Cluster; open() already has them"
    );
    assert!(
        back.codec_private(0).is_some(),
        "and the readiness claim is honest: the codec private IS available \
             before any frame has been read"
    );
}
#[test]
fn container_timing_and_signed_padding_survive_roundtrip() {
    for padding in [-10_666_667, 0, 10_666_667] {
        let out = SharedOut::new();
        let title = h264_title();
        let timing = crate::pes::TrackTiming {
            codec_delay_ns: 5_333_333,
            seek_preroll_ns: 80_000_000,
        };
        let mut writer = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
        writer.set_track_timing(0, timing).unwrap();
        let mut frame = mvc_frame(0, 0, true, vec![1, 2, 3]);
        frame.discard_padding_ns = padding;
        writer.write(&frame).unwrap();
        assert!(writer.set_track_timing(0, timing).is_err());
        writer.finish().unwrap();
        let mut reader = MkvStream::open(Cursor::new(out.bytes())).unwrap();
        assert_eq!(reader.track_timing(0), timing);
        let back = reader.read().unwrap().unwrap();
        assert_eq!(back.discard_padding_ns, padding);
        assert_eq!(back.data, frame.data);
        assert!(reader.read().unwrap().is_none());
    }
}

// Timing set on one track lands on that track only; the others read default.
#[test]
fn track_timing_is_per_track() {
    let out = SharedOut::new();
    let timing = crate::pes::TrackTiming {
        codec_delay_ns: 6_500_000,
        seek_preroll_ns: 80_000_000,
    };
    let mut w = MkvStream::create(Box::new(out.clone()), &three_track_title(), None).unwrap();
    w.set_track_timing(1, timing).unwrap();
    w.write(&av_frame(0, 0, true, vec![1, 2, 3])).unwrap();
    w.finish().unwrap();
    let r = MkvStream::open(Cursor::new(out.bytes())).unwrap();
    assert_eq!(r.track_timing(1), timing);
    assert_eq!(r.track_timing(0), Default::default());
    assert_eq!(r.track_timing(2), Default::default());
}

#[test]
fn laced_padding_applies_only_to_the_appropriate_edge_frame() {
    for padding in [-1_000_000i64, 1_000_000] {
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 0).unwrap();
        let mut group = Vec::new();
        // Fixed lace: two one-byte frames.
        ebml::write_binary(&mut group, ebml::BLOCK, &[0x81, 0, 0, 4, 1, 0xaa, 0xbb]).unwrap();
        ebml::write_int(&mut group, ebml::DISCARD_PADDING, padding).unwrap();
        ebml::write_binary(&mut cluster, ebml::BLOCK_GROUP, &group).unwrap();
        let bytes = mkv_with_tracks_and_cluster(
            &[TrackSpec::new(1, 2).with_default_duration(32_000_000)],
            &cluster,
        );
        let frames = drain(&mut MkvStream::open(Cursor::new(bytes)).unwrap());
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[0].discard_padding_ns,
            if padding < 0 { padding } else { 0 }
        );
        assert_eq!(
            frames[1].discard_padding_ns,
            if padding > 0 { padding } else { 0 }
        );
    }
}

#[test]
fn malformed_discard_padding_size_is_rejected() {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    let mut group = Vec::new();
    ebml::write_binary(&mut group, ebml::BLOCK, &[0x81, 0, 0, 0, 0xaa]).unwrap();
    ebml::write_binary(&mut group, ebml::DISCARD_PADDING, &[0; 9]).unwrap();
    ebml::write_binary(&mut cluster, ebml::BLOCK_GROUP, &group).unwrap();
    let bytes = mkv_with_tracks_and_cluster(&[TrackSpec::new(1, 2)], &cluster);
    let e = MkvStream::open(Cursor::new(bytes))
        .unwrap()
        .read()
        .unwrap_err();
    assert!(is_mkv_source_invalid(&e), "{e:?}");
}

fn av_frame(track: usize, pts: i64, keyframe: bool, data: Vec<u8>) -> crate::pes::PesFrame {
    crate::pes::PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track,
        pts,
        keyframe,
        data,
        duration_ns: None,
    }
}

#[test]
fn a_zero_length_discard_padding_is_value_zero_not_malformed() {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    let mut group = Vec::new();
    ebml::write_binary(&mut group, ebml::BLOCK, &[0x81, 0, 0, 0, 0xaa]).unwrap();
    ebml::write_binary(&mut group, ebml::DISCARD_PADDING, &[]).unwrap();
    ebml::write_binary(&mut cluster, ebml::BLOCK_GROUP, &group).unwrap();
    let bytes = mkv_with_tracks_and_cluster(&[TrackSpec::new(1, 2)], &cluster);
    let frames = drain(&mut MkvStream::open(Cursor::new(bytes)).unwrap());
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].discard_padding_ns, 0);
}

fn pts_of(frames: &[crate::pes::PesFrame], track: usize) -> Vec<i64> {
    frames
        .iter()
        .filter(|f| f.track == track)
        .map(|f| f.pts)
        .collect()
}

// mkvmerge-style origin: the earliest audio/video frame, not the first IDR.
// Audio 40 ms before the IDR keeps its spacing; an earlier subtitle cue is
// kept and sets the origin: every offset is exact against it.
#[test]
fn frames_before_the_first_idr_are_kept_relative_to_the_earliest() {
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &three_track_title(), None).unwrap();
    s.write(&av_frame(
        2,
        100_000_000,
        true,
        vec![0, 8, 0, 4, 0xAA, 0xBB, 0, 4],
    ))
    .unwrap();
    s.write(&av_frame(1, 120_000_000, true, vec![0xA0; 8]))
        .unwrap();
    s.write(&av_frame(0, 160_000_000, true, vec![0x11; 16]))
        .unwrap();
    s.write(&av_frame(1, 192_000_000, true, vec![0xB0; 8]))
        .unwrap();
    s.finish().unwrap();
    let back = drain(&mut MkvStream::open(Cursor::new(out.bytes())).unwrap());
    assert_eq!(pts_of(&back, 0), vec![60_000_000]);
    assert_eq!(pts_of(&back, 1), vec![20_000_000, 92_000_000]);
    assert_eq!(pts_of(&back, 2), vec![0], "the earliest sample is t=0");
}

// DVD/HD-DVD clip marks run on another clock: the origin ignores them, and no
// kept frame lands before the origin (squashed to t=0).
#[test]
fn an_mpeg_ps_origin_ignores_clip_marks_and_squashes_nothing() {
    let mut t = three_track_title();
    t.content_format = crate::disc::ContentFormat::MpegPs;
    t.clips = vec![crate::disc::Clip {
        clip_id: String::new(),
        in_time: 36_000,
        out_time: 450_000,
        duration_secs: 9.2,
        source_packets: 0,
        feed_span: None,
    }];
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &t, None).unwrap();
    for (track, pts) in [(1, 500), (1, 900), (0, 1_000), (1, 1_032)] {
        s.write(&av_frame(track, pts * 1_000_000, true, vec![0xA0; 8]))
            .unwrap();
    }
    s.finish().unwrap();
    let back = drain(&mut MkvStream::open(Cursor::new(out.bytes())).unwrap());
    assert_eq!(pts_of(&back, 1), vec![0, 400_000_000, 532_000_000]);
    assert_eq!(pts_of(&back, 0), vec![500_000_000]);
}

// Blu-ray with unusable marks (no seam plan): A/V before the clip IN is
// dropped, not squashed onto the origin.
#[test]
fn a_bd_frame_before_the_clip_in_is_dropped_without_a_seam_plan() {
    let mut t = three_track_title();
    t.content_format = crate::disc::ContentFormat::BdTs;
    t.clips = vec![crate::disc::Clip {
        clip_id: String::new(),
        in_time: 36_000,
        out_time: 0,
        duration_secs: 0.0,
        source_packets: 0,
        feed_span: None,
    }];
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &t, None).unwrap();
    for (track, pts) in [(1, 500), (1, 900), (0, 1_000), (1, 1_032)] {
        s.write(&av_frame(track, pts * 1_000_000, true, vec![0xA0; 8]))
            .unwrap();
    }
    s.finish().unwrap();
    let back = drain(&mut MkvStream::open(Cursor::new(out.bytes())).unwrap());
    assert_eq!(pts_of(&back, 1), vec![0, 132_000_000]);
    assert_eq!(pts_of(&back, 0), vec![100_000_000]);
}

// Audio further before the IDR than one block reaches is still kept at its
// true offset, and cluster timestamps stay ascending.
#[test]
fn audio_far_before_the_first_idr_is_kept_at_its_true_offset() {
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &three_track_title(), None).unwrap();
    s.write(&av_frame(1, 0, true, vec![0xA0; 8])).unwrap();
    s.write(&av_frame(1, 4_000_000_000, true, vec![0xA1; 8]))
        .unwrap();
    s.write(&av_frame(0, 5_000_000_000, true, vec![0x11; 16]))
        .unwrap();
    s.finish().unwrap();
    let bytes = out.bytes();
    let back = drain(&mut MkvStream::open(Cursor::new(bytes.clone())).unwrap());
    assert_eq!(pts_of(&back, 1), vec![0, 4_000_000_000]);
    assert_eq!(pts_of(&back, 0), vec![5_000_000_000]);
    // Cluster = ID(4) + 8-byte size, then Timestamp: E7, size byte, value.
    let mut last = 0u64;
    for i in (0..bytes.len() - 16).filter(|&i| bytes[i..].starts_with(&[0x1F, 0x43, 0xB6, 0x75])) {
        assert_eq!(bytes[i + 12], 0xE7);
        let n = (bytes[i + 13] & 0x0F) as usize;
        let ts = bytes[i + 14..i + 14 + n]
            .iter()
            .fold(0u64, |a, &b| (a << 8) | b as u64);
        assert!(ts >= last, "clusters must ascend");
        last = ts;
    }
}

struct FailingWriter;
impl io::Write for FailingWriter {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::StorageFull.into())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl io::Seek for FailingWriter {
    fn seek(&mut self, _: io::SeekFrom) -> io::Result<u64> {
        Ok(0)
    }
}

#[test]
fn a_failed_header_write_fails_every_later_write_and_finish() {
    let mut s = MkvStream::create(Box::new(FailingWriter), &h264_title(), None).unwrap();
    let kf = av_frame(0, 0, true, vec![1, 2, 3]);
    assert!(s.write(&kf).is_err(), "the header write failure surfaces");
    let err = s.write(&kf).unwrap_err();
    let code = format!("E{}", crate::error::E_STREAM_CLOSED);
    assert!(err.to_string().starts_with(&code), "{err}");
    assert!(s.finish().is_err(), "a truncated MKV must not finish Ok");
}

// A buffered frame for a track the muxer lacks is rejected on its own at activation;
// it must not fail the video frame that triggered the replay or truncate the file.
#[test]
fn an_out_of_range_buffered_frame_does_not_fail_activation() {
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out.clone()), &h264_title(), None).unwrap();
    s.write(&av_frame(5, 0, true, vec![9])).unwrap();
    s.write(&av_frame(0, 0, true, vec![1, 2, 3])).unwrap();
    s.write(&av_frame(0, 1_000_000, false, vec![4])).unwrap();
    s.finish().unwrap();
}

// A saturated IDR pts against a far-negative buffered pts must not overflow the lead.
#[test]
fn origin_lead_saturates_on_extreme_timestamps() {
    let mut p = PendingMux {
        writer: Box::new(Cursor::new(Vec::new())),
        tracks: Vec::new(),
        timings: Vec::new(),
        video_track: None,
        opening_capture_path: None,
        buffered: vec![(av_frame(1, i64::MIN / 2, false, vec![1]), None)],
        buffered_bytes: 1,
        origin_lead_ns: 0,
        dropped_pre_origin: 0,
    };
    p.set_origin(i64::MAX, None);
    assert_eq!(p.origin_lead_ns, i64::MAX);
}

#[test]
fn set_track_timing_rejects_bad_tracks_and_late_calls_with_codes() {
    let out = SharedOut::new();
    let mut s = MkvStream::create(Box::new(out), &h264_title(), None).unwrap();
    let timing = crate::pes::TrackTiming {
        codec_delay_ns: 1,
        seek_preroll_ns: 2,
    };
    let err = s.set_track_timing(5, timing).unwrap_err();
    let code = format!("E{}", crate::error::E_MUX_TRACK_RANGE);
    assert!(err.to_string().starts_with(&code), "{err}");
    s.write(&av_frame(0, 0, true, vec![1])).unwrap();
    let err = s.set_track_timing(0, timing).unwrap_err();
    let code = format!("E{}", crate::error::E_STREAM_HEADER_WRITTEN);
    assert!(err.to_string().starts_with(&code), "{err}");
}

// mkv -> mkv: an A_FLAC track must come back as FLAC with its STREAMINFO
// CodecPrivate, not as an A_AC3-labelled track.
#[test]
fn a_flac_track_survives_an_mkv_to_mkv_remux() {
    use crate::disc::{AudioChannels, AudioStream, DiscTitle, SampleRate, Stream};
    let streaminfo = b"fLaC\x80\x00\x00\x22streaminfo".to_vec();
    let mut title = DiscTitle {
        streams: vec![Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Flac,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: crate::labels::LabelPurpose::Normal,
            label: String::new(),
        })],
        ..DiscTitle::empty()
    };
    title.codec_privates = vec![Some(streaminfo.clone())];
    let mut bytes = Vec::new();
    for _pass in 0..2 {
        let out = SharedOut::new();
        let mut w = MkvStream::create(Box::new(out.clone()), &title, None).unwrap();
        w.write(&av_frame(0, 0, true, vec![0xFF, 0xF8, 0x69, 0x08]))
            .unwrap();
        w.finish().unwrap();
        bytes = out.bytes();
        let r = MkvStream::open(Cursor::new(bytes.clone())).unwrap();
        title = r.info().clone();
        title.codec_privates = vec![r.codec_private(0)];
    }
    let r = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream_codec(&r.info().streams[0]), Codec::Flac);
    assert_eq!(r.codec_private(0), Some(streaminfo));
}

// One PCM audio track (`codec_id`, optional BitDepth) with one SimpleBlock.
fn pcm_mkv(codec_id: &str, bit_depth: Option<u64>, samples: &[u8]) -> Vec<u8> {
    pcm_mkv_blocks(codec_id, bit_depth, &[(0, samples.to_vec())])
}

// Stereo 48 kHz PCM track with one SimpleBlock per `(timestamp_ms, samples)`.
fn pcm_mkv_blocks(codec_id: &str, bit_depth: Option<u64>, blocks: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 2).unwrap();
    ebml::write_string(&mut entry, ebml::CODEC_ID, codec_id).unwrap();
    let mut audio = Vec::new();
    ebml::write_uint(&mut audio, ebml::CHANNELS, 2).unwrap();
    ebml::write_id(&mut audio, ebml::SAMPLING_FREQUENCY).unwrap();
    ebml::write_size(&mut audio, 8).unwrap();
    audio.extend_from_slice(&48_000f64.to_be_bytes());
    if let Some(d) = bit_depth {
        ebml::write_uint(&mut audio, ebml::BIT_DEPTH, d).unwrap();
    }
    ebml::write_binary(&mut entry, ebml::AUDIO, &audio).unwrap();
    let mut tracks = Vec::new();
    ebml::write_binary(&mut tracks, ebml::TRACK_ENTRY, &entry).unwrap();
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_binary(&mut out, ebml::TRACKS, &tracks).unwrap();
    for (ms, samples) in blocks {
        let mut block = vec![0x81, 0, 0, 0x80];
        block.extend_from_slice(samples);
        out.extend(cluster_with_simple_block(*ms, &block));
    }
    out
}

// No BitDepth: the depth is inferred from bytes per block over its duration;
// an undecidable track is not presented as LPCM.
#[test]
fn pcm_without_bit_depth_infers_the_depth_from_block_sizes() {
    // Blocks 2 ms apart: 96 stereo sample frames at 48 kHz.
    let b24: Vec<u8> = (0..96 * 2 * 3).map(|i| i as u8).collect();
    let blocks: Vec<_> = (0..10u64).map(|i| (i * 2, b24.clone())).collect();
    let bytes = pcm_mkv_blocks(ebml::CODEC_PCM_BE, None, &blocks);
    let mut s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream_codec(&s.info().streams[0]), Codec::Lpcm);
    let frames = drain(&mut s);
    assert_eq!(frames.len(), 10);
    assert_eq!(frames[0].data, b24, "24-bit big-endian passes through");

    let b16 = [0x12u8, 0x34].repeat(96 * 2);
    let blocks: Vec<_> = (0..10u64).map(|i| (i * 2, b16.clone())).collect();
    let bytes = pcm_mkv_blocks(ebml::CODEC_PCM_BE, None, &blocks);
    let mut s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(drain(&mut s)[0].data[..3], [0x12, 0x34, 0]);

    // Two blocks 2 ms apart are within timestamp error: no evidence, not LPCM.
    let bytes = pcm_mkv_blocks(ebml::CODEC_PCM_BE, None, &[(0, b16.clone()), (2, b16)]);
    let s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream_codec(&s.info().streams[0]), Codec::Unknown(0));

    let odd = vec![0u8; 250];
    let bytes = pcm_mkv_blocks(ebml::CODEC_PCM_BE, None, &[(0, odd.clone()), (2, odd)]);
    let s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream_codec(&s.info().streams[0]), Codec::Unknown(0));
}

// Header for video track 1 (HEVC) + stereo 48 kHz PCM track 2 without BitDepth.
fn video_then_pcm_header() -> Vec<u8> {
    let mut video = Vec::new();
    ebml::write_uint(&mut video, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut video, ebml::TRACK_TYPE, 1).unwrap();
    ebml::write_string(&mut video, ebml::CODEC_ID, ebml::CODEC_HEVC).unwrap();
    let mut pcm = Vec::new();
    ebml::write_uint(&mut pcm, ebml::TRACK_NUMBER, 2).unwrap();
    ebml::write_uint(&mut pcm, ebml::TRACK_TYPE, 2).unwrap();
    ebml::write_string(&mut pcm, ebml::CODEC_ID, ebml::CODEC_PCM_BE).unwrap();
    let mut audio = Vec::new();
    ebml::write_uint(&mut audio, ebml::CHANNELS, 2).unwrap();
    ebml::write_id(&mut audio, ebml::SAMPLING_FREQUENCY).unwrap();
    ebml::write_size(&mut audio, 8).unwrap();
    audio.extend_from_slice(&48_000f64.to_be_bytes());
    ebml::write_binary(&mut pcm, ebml::AUDIO, &audio).unwrap();
    let mut tracks = Vec::new();
    ebml::write_binary(&mut tracks, ebml::TRACK_ENTRY, &video).unwrap();
    ebml::write_binary(&mut tracks, ebml::TRACK_ENTRY, &pcm).unwrap();
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_binary(&mut out, ebml::TRACKS, &tracks).unwrap();
    out
}

fn simple_block(track: u8, ms: u64, payload: &[u8]) -> Vec<u8> {
    let mut block = vec![0x80 | track, 0, 0, 0x80];
    block.extend_from_slice(payload);
    cluster_with_simple_block(ms, &block)
}

fn pending_bytes(s: &MkvStream) -> usize {
    match &s.mode {
        Mode::Read(rs) => rs.pending.iter().map(|f| f.data.len()).sum(),
        Mode::Write(_) => panic!("expected a read stream"),
    }
}

// 72-sample blocks with 1 ms timestamps: one block alone reads as 24-bit.
#[test]
fn short_16_bit_pcm_blocks_with_quantised_timestamps_infer_16_bit() {
    let b16 = [0x12u8, 0x34].repeat(72 * 2);
    let blocks: Vec<_> = (0..30u64).map(|i| (i * 3 / 2, b16.clone())).collect();
    let bytes = pcm_mkv_blocks(ebml::CODEC_PCM_BE, None, &blocks);
    let mut s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream_codec(&s.info().streams[0]), Codec::Lpcm);
    let f = drain(&mut s);
    assert_eq!(f[0].data.len(), 72 * 2 * 3, "widened from 16-bit");
    assert_eq!(f[0].data[..3], [0x12, 0x34, 0]);
}

// A PCM track that starts after 600 video frames still resolves its depth.
#[test]
fn a_late_starting_pcm_track_still_resolves_its_depth() {
    let mut bytes = video_then_pcm_header();
    for i in 0..600u64 {
        bytes.extend(simple_block(1, i, &[0u8; 100]));
    }
    let b16 = [0x12u8, 0x34].repeat(96 * 2);
    for i in 0..30u64 {
        bytes.extend(simple_block(2, 600 + i * 2, &b16));
    }
    let mut s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(stream_codec(&s.info().streams[1]), Codec::Lpcm);
    let pcm: Vec<_> = drain(&mut s).into_iter().filter(|f| f.track == 1).collect();
    assert_eq!(pcm.len(), 30);
    assert_eq!(pcm[0].data[..3], [0x12, 0x34, 0]);
}

// Big video blocks ahead of a PCM track: open() stops buffering at the byte cap.
#[test]
fn pcm_depth_probe_is_memory_bounded() {
    struct Lazy {
        head: Cursor<Vec<u8>>,
        block: Vec<u8>,
        left: usize,
        at: usize,
    }
    impl Read for Lazy {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.head.read(buf)?;
            if n > 0 {
                return Ok(n);
            }
            if self.at == self.block.len() {
                if self.left == 0 {
                    return Ok(0);
                }
                self.left -= 1;
                self.at = 0;
            }
            let n = buf.len().min(self.block.len() - self.at);
            buf[..n].copy_from_slice(&self.block[self.at..self.at + n]);
            self.at += n;
            Ok(n)
        }
    }
    let block = simple_block(1, 0, &vec![0u8; 256 << 10]);
    // No PCM frame, or one whose size fits 16 and 24 bit: no evidence, so
    // the track is not presented as LPCM rather than guessed.
    for pcm in [None, Some(vec![0u8; 96 * 2 * 3])] {
        let mut head = video_then_pcm_header();
        if let Some(p) = &pcm {
            head.extend(simple_block(2, 0, p));
        }
        let r = Lazy {
            head: Cursor::new(head),
            at: block.len(),
            block: block.clone(),
            left: 1000,
        };
        let s = MkvStream::open(r).unwrap();
        assert!(pending_bytes(&s) <= PCM_PROBE_BYTES + (256 << 10) + (1 << 10));
        assert_eq!(stream_codec(&s.info().streams[1]), Codec::Unknown(0));
    }
}

// LPCM frames downstream are 24-bit big-endian: 16-bit and little-endian
// PCM sources are widened/reordered on read, unsupported layouts are not LPCM.
// (codec id, BitDepth, input samples, expected 24-bit BE samples or None = not LPCM)
type PcmCase<'a> = (&'a str, Option<u64>, &'a [u8], Option<&'a [u8]>);

#[test]
fn pcm_tracks_are_read_as_24_bit_big_endian() {
    let cases: &[PcmCase] = &[
        (
            ebml::CODEC_PCM_BE,
            Some(16),
            &[0x12, 0x34, 0xAB, 0xCD],
            Some(&[0x12, 0x34, 0, 0xAB, 0xCD, 0]),
        ),
        (ebml::CODEC_PCM_BE, Some(24), &[1, 2, 3], Some(&[1, 2, 3])),
        (
            ebml::CODEC_PCM_LE,
            Some(16),
            &[0x34, 0x12],
            Some(&[0x12, 0x34, 0]),
        ),
        (ebml::CODEC_PCM_LE, Some(24), &[3, 2, 1], Some(&[1, 2, 3])),
        (ebml::CODEC_PCM_BE, Some(32), &[1, 2, 3, 4], None),
        ("A_PCM/FLOAT/IEEE", Some(32), &[1, 2, 3, 4], None),
    ];
    for (cid, depth, input, want) in cases {
        let mut s = MkvStream::open(Cursor::new(pcm_mkv(cid, *depth, input))).unwrap();
        let codec = stream_codec(&s.info().streams[0]);
        let frames = drain(&mut s);
        match want {
            Some(w) => {
                assert_eq!(codec, Codec::Lpcm, "{cid} {depth:?}");
                assert_eq!(frames[0].data, w.to_vec(), "{cid} {depth:?}");
            }
            None => assert_eq!(codec, Codec::Unknown(0), "{cid} {depth:?}"),
        }
    }
}

// ── probe_mkv ─────────────────────────────────────────────

fn master(id: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ebml::write_id(&mut out, id).unwrap();
    ebml::write_size(&mut out, body.len() as u64).unwrap();
    out.extend_from_slice(body);
    out
}

// Info + Tracks (HEVC video, French TrueHD audio) as Segment children.
fn info_and_tracks(info: &[u8]) -> Vec<u8> {
    let mut video = Vec::new();
    ebml::write_uint(&mut video, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut video, ebml::TRACK_TYPE, 1).unwrap();
    ebml::write_string(&mut video, ebml::CODEC_ID, ebml::CODEC_HEVC).unwrap();
    let mut audio = Vec::new();
    ebml::write_uint(&mut audio, ebml::TRACK_NUMBER, 2).unwrap();
    ebml::write_uint(&mut audio, ebml::TRACK_TYPE, 2).unwrap();
    ebml::write_string(&mut audio, ebml::CODEC_ID, ebml::CODEC_TRUEHD).unwrap();
    ebml::write_string(&mut audio, ebml::LANGUAGE, "fre").unwrap();
    let mut tracks = master(ebml::TRACK_ENTRY, &video);
    tracks.extend(master(ebml::TRACK_ENTRY, &audio));
    let mut out = master(ebml::INFO, info);
    out.extend(master(ebml::TRACKS, &tracks));
    out
}

fn segment(children: &[&[u8]]) -> Vec<u8> {
    let mut out = master(ebml::EBML, &[]);
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    for c in children {
        out.extend_from_slice(c);
    }
    out
}

fn probe_fixture(info: &[u8], after_tracks: &[u8]) -> Vec<u8> {
    segment(&[&info_and_tracks(info), after_tracks])
}

#[test]
fn probe_reads_apps_duration_title_and_tracks() {
    let mut info = Vec::new();
    ebml::write_uint(&mut info, ebml::TIMESTAMP_SCALE, 100_000).unwrap();
    ebml::write_float(&mut info, ebml::DURATION, 72_000.0).unwrap();
    ebml::write_string(&mut info, ebml::MUXING_APP, "freemkv 1.7.7 (gc8e67f1)").unwrap();
    ebml::write_string(&mut info, ebml::WRITING_APP, "other 2.0").unwrap();
    ebml::write_string(&mut info, ebml::TITLE, "Feature").unwrap();
    // A cluster whose block claims more bytes than exist: never read by the probe.
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut cluster).unwrap();
    ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
    ebml::write_size(&mut cluster, 4096).unwrap();

    let p = probe_mkv(Cursor::new(probe_fixture(&info, &cluster))).unwrap();
    assert_eq!(p.muxing_app.as_deref(), Some("freemkv 1.7.7 (gc8e67f1)"));
    assert_eq!(p.writing_app.as_deref(), Some("other 2.0"));
    assert_eq!(p.title.as_deref(), Some("Feature"));
    assert_eq!(p.timestamp_scale, 100_000);
    assert!((p.duration_secs.unwrap() - 7.2).abs() < 1e-9);
    assert_eq!(p.last_cue_secs, None);
    assert_eq!(p.tracks.len(), 2);
    assert_eq!(p.tracks[0].kind, MkvTrackKind::Video);
    assert_eq!(p.tracks[0].codec_id, ebml::CODEC_HEVC);
    assert_eq!(p.tracks[0].language, "eng", "RFC 9559 default");
    assert_eq!(p.tracks[1].number, 2);
    assert_eq!(p.tracks[1].kind, MkvTrackKind::Audio);
    assert_eq!(p.tracks[1].codec_id, ebml::CODEC_TRUEHD);
    assert_eq!(p.tracks[1].language, "fre");
}

#[test]
fn probe_leaves_absent_fields_none() {
    let p = probe_mkv(Cursor::new(probe_fixture(&[], &[]))).unwrap();
    assert_eq!(p.muxing_app, None);
    assert_eq!(p.writing_app, None);
    assert_eq!(p.duration_secs, None);
    assert_eq!(p.title, None);
    assert_eq!(p.timestamp_scale, 1_000_000);
    assert_eq!(p.tracks.len(), 2);
}

#[test]
fn probe_rejects_non_matroska() {
    let e = probe_mkv(Cursor::new(vec![0x47u8; 188])).unwrap_err();
    assert!(is_mkv_source_invalid(&e));
}

#[test]
fn probe_with_cues_follows_the_seekhead() {
    // SeekHead → Cues placed after the (skipped) cluster; the largest CueTime wins.
    let cue = |t: u64| {
        let mut b = Vec::new();
        ebml::write_uint(&mut b, ebml::CUE_TIME, t).unwrap();
        master(ebml::CUE_POINT, &b)
    };
    let mut cues_body = cue(0);
    cues_body.extend(cue(9_000));
    cues_body.extend(cue(4_000));
    let cues = master(ebml::CUES, &cues_body);
    let cluster = master(ebml::CLUSTER, &[0u8; 64]);

    let seekhead = |pos: u64| {
        let mut seek = Vec::new();
        ebml::write_binary(&mut seek, ebml::SEEK_ID, &ebml::CUES.to_be_bytes()).unwrap();
        ebml::write_id(&mut seek, ebml::SEEK_POSITION).unwrap();
        ebml::write_size(&mut seek, 8).unwrap();
        seek.extend_from_slice(&pos.to_be_bytes());
        master(ebml::SEEK_HEAD, &master(ebml::SEEK, &seek))
    };
    let it = info_and_tracks(&[]);
    // Fixed-width SeekPosition: the SeekHead's length does not depend on the value.
    let cues_rel = (seekhead(0).len() + it.len() + cluster.len()) as u64;
    let out = segment(&[&seekhead(cues_rel), &it, &cluster, &cues]);

    let p = probe_mkv_with_cues(Cursor::new(out)).unwrap();
    assert_eq!(p.tracks.len(), 2);
    assert!((p.last_cue_secs.unwrap() - 9.0).abs() < 1e-9);
    // No Cues at all: still a probe, just no cue runtime.
    let p = probe_mkv_with_cues(Cursor::new(probe_fixture(&[], &cluster))).unwrap();
    assert_eq!(p.last_cue_secs, None);
}

// An unfinished file (SeekPosition 0) or a SeekHead pointing past EOF must be an
// error, never "no Cues": verify then falls back to the header Duration.
#[test]
fn probe_with_cues_rejects_a_bad_seekhead_target_and_takes_a_direct_hit() {
    let seekhead = |pos: u64| {
        let mut seek = Vec::new();
        ebml::write_binary(&mut seek, ebml::SEEK_ID, &ebml::CUES.to_be_bytes()).unwrap();
        ebml::write_id(&mut seek, ebml::SEEK_POSITION).unwrap();
        ebml::write_size(&mut seek, 8).unwrap();
        seek.extend_from_slice(&pos.to_be_bytes());
        master(ebml::SEEK_HEAD, &master(ebml::SEEK, &seek))
    };
    let it = info_and_tracks(&[]);
    let cluster = master(ebml::CLUSTER, &[0u8; 64]);
    for pos in [0, 1 << 20] {
        let out = segment(&[&seekhead(pos), &it, &cluster]);
        assert!(
            probe_mkv_with_cues(Cursor::new(out)).is_err(),
            "SeekPosition {pos}"
        );
    }
    let mut cp = Vec::new();
    ebml::write_uint(&mut cp, ebml::CUE_TIME, 3_000).unwrap();
    let cues = master(ebml::CUES, &master(ebml::CUE_POINT, &cp));
    let out = segment(&[&it, &cues, &cluster]);
    let p = probe_mkv_with_cues(Cursor::new(out)).unwrap();
    assert_eq!(p.last_cue_secs, Some(3.0));
}

#[test]
fn parse_freemkv_version_accepts_freemkv_stamps_only() {
    assert_eq!(
        parse_freemkv_version("freemkv 1.7.7 (gc8e67f1)"),
        Some((1, 7, 7))
    );
    assert_eq!(parse_freemkv_version("freemkv 1.10.0"), Some((1, 10, 0)));
    assert_eq!(
        parse_freemkv_version("freemkv 1.8.0-rc.1 (gabc1234)"),
        Some((1, 8, 0))
    );
    assert_eq!(parse_freemkv_version("freemkv"), None);
    assert_eq!(parse_freemkv_version("freemkv 1.7"), None);
    assert_eq!(parse_freemkv_version("freemkv x.7.7"), None);
    assert_eq!(
        parse_freemkv_version("libebml v1.4.4 + libmatroska v1.7.1"),
        None
    );
    assert_eq!(parse_freemkv_version("mkvmerge v80.0 ('x') 64-bit"), None);
}

// Info + Tracks (one entry with `codec_id`) + optional Chapters + one Cluster
// holding `cluster_body`, as raw EBML.
fn synthetic_mkv(codec_id: &str, chapters: &[u8], cluster_body: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
    ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
    ebml::write_string(&mut entry, ebml::CODEC_ID, codec_id).unwrap();
    let mut tracks = Vec::new();
    ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
    ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
    tracks.extend_from_slice(&entry);
    let mut out = Vec::new();
    ebml::write_id(&mut out, ebml::EBML).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    ebml::write_id(&mut out, ebml::INFO).unwrap();
    ebml::write_size(&mut out, 0).unwrap();
    ebml::write_id(&mut out, ebml::TRACKS).unwrap();
    ebml::write_size(&mut out, tracks.len() as u64).unwrap();
    out.extend_from_slice(&tracks);
    out.extend_from_slice(chapters);
    ebml::write_id(&mut out, ebml::CLUSTER).unwrap();
    ebml::write_unknown_size(&mut out).unwrap();
    out.extend_from_slice(cluster_body);
    out
}

#[test]
fn read_back_keeps_chapters() {
    let mut atoms = Vec::new();
    for (t, name) in [(0u64, "1"), (5_000_000_000, "Two")] {
        let mut body = Vec::new();
        ebml::write_uint(&mut body, ebml::CHAPTER_TIME_START, t).unwrap();
        let mut disp = Vec::new();
        ebml::write_string(&mut disp, ebml::CHAP_STRING, name).unwrap();
        ebml::write_id(&mut body, ebml::CHAPTER_DISPLAY).unwrap();
        ebml::write_size(&mut body, disp.len() as u64).unwrap();
        body.extend_from_slice(&disp);
        ebml::write_id(&mut atoms, ebml::CHAPTER_ATOM).unwrap();
        ebml::write_size(&mut atoms, body.len() as u64).unwrap();
        atoms.extend_from_slice(&body);
    }
    let mut edition = Vec::new();
    ebml::write_id(&mut edition, ebml::EDITION_ENTRY).unwrap();
    ebml::write_size(&mut edition, atoms.len() as u64).unwrap();
    edition.extend_from_slice(&atoms);
    let mut chapters = Vec::new();
    ebml::write_id(&mut chapters, ebml::CHAPTERS).unwrap();
    ebml::write_size(&mut chapters, edition.len() as u64).unwrap();
    chapters.extend_from_slice(&edition);

    let stream = MkvStream::open(Cursor::new(synthetic_mkv("V_MPEG2", &chapters, &[]))).unwrap();
    let got: Vec<(f64, String)> = crate::pes::PesSource::info(&stream)
        .chapters
        .iter()
        .map(|c| (c.time_secs, c.name.clone()))
        .collect();
    assert_eq!(got, vec![(0.0, "1".to_string()), (5.0, "Two".to_string())]);
}

fn el(id: u32, body: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    ebml::write_id(&mut v, id).unwrap();
    ebml::write_size(&mut v, body.len() as u64).unwrap();
    v.extend_from_slice(body);
    v
}

fn atom(t: u64, name: &str, flags: &[(u32, u64)]) -> Vec<u8> {
    let mut b = Vec::new();
    ebml::write_uint(&mut b, ebml::CHAPTER_TIME_START, t).unwrap();
    for &(id, v) in flags {
        ebml::write_uint(&mut b, id, v).unwrap();
    }
    let mut d = Vec::new();
    ebml::write_string(&mut d, ebml::CHAP_STRING, name).unwrap();
    b.extend(el(ebml::CHAPTER_DISPLAY, &d));
    el(ebml::CHAPTER_ATOM, &b)
}

fn chapter_names(chapters: &[u8]) -> Vec<String> {
    let s = MkvStream::open(Cursor::new(synthetic_mkv("V_MPEG2", chapters, &[]))).unwrap();
    crate::pes::PesSource::info(&s)
        .chapters
        .iter()
        .map(|c| c.name.clone())
        .collect()
}

#[test]
fn chapters_use_default_edition_and_skip_hidden_disabled() {
    let mut e1 = atom(0, "first-ed", &[]);
    e1.extend(atom(1, "x", &[]));
    let mut e2 = Vec::new();
    ebml::write_uint(&mut e2, ebml::EDITION_FLAG_DEFAULT, 1).unwrap();
    e2.extend(atom(0, "keep", &[]));
    e2.extend(atom(1, "hid", &[(ebml::CHAPTER_FLAG_HIDDEN, 1)]));
    e2.extend(atom(2, "off", &[(ebml::CHAPTER_FLAG_ENABLED, 0)]));
    e2.extend(atom(3, "keep2", &[(ebml::CHAPTER_FLAG_ENABLED, 1)]));
    let mut body = el(ebml::EDITION_ENTRY, &e1);
    body.extend(el(ebml::EDITION_ENTRY, &e2));
    let ch = el(ebml::CHAPTERS, &body);
    assert_eq!(chapter_names(&ch), vec!["keep", "keep2"]);
}

#[test]
fn malformed_chapters_still_open_and_mux() {
    let mut bad_name = Vec::new();
    ebml::write_id(&mut bad_name, ebml::CHAP_STRING).unwrap();
    ebml::write_size(&mut bad_name, 2).unwrap();
    bad_name.extend_from_slice(&[0xFF, 0xFE]);
    let mut a = Vec::new();
    ebml::write_uint(&mut a, ebml::CHAPTER_TIME_START, 0).unwrap();
    a.extend(el(ebml::CHAPTER_DISPLAY, &bad_name));
    let ch = el(
        ebml::CHAPTERS,
        &el(ebml::EDITION_ENTRY, &el(ebml::CHAPTER_ATOM, &a)),
    );
    // Oversized ChapString (> 64 KiB) inside otherwise well-formed elements.
    let mut big = Vec::new();
    ebml::write_id(&mut big, ebml::CHAP_STRING).unwrap();
    ebml::write_size(&mut big, MAX_STRING_LEN + 1).unwrap();
    big.resize(big.len() + MAX_STRING_LEN as usize + 1, b'a');
    let mut a2 = Vec::new();
    a2.extend(el(ebml::CHAPTER_DISPLAY, &big));
    let ch2 = el(
        ebml::CHAPTERS,
        &el(ebml::EDITION_ENTRY, &el(ebml::CHAPTER_ATOM, &a2)),
    );
    for c in [ch, ch2] {
        let bytes = synthetic_mkv("V_MPEG2", &c, &[]);
        assert!(chapter_names(&c).is_empty());
        assert!(probe_mkv(Cursor::new(bytes.clone())).is_ok());
        let mut s = MkvStream::open(Cursor::new(bytes)).unwrap();
        assert!(crate::pes::PesSource::read(&mut s).unwrap().is_none());
    }
}

#[test]
fn truncated_tags_before_cluster_still_opens() {
    let mut tags = Vec::new();
    ebml::write_id(&mut tags, ebml::TAGS).unwrap();
    ebml::write_size(&mut tags, 1000).unwrap();
    tags.extend_from_slice(&[0u8; 10]);
    // Tags claims 1000 bytes but the file ends: no Cluster follows.
    let mut bytes = synthetic_mkv("V_MPEG2", &[], &[]);
    let cut = bytes.len() - 12; // drop the Cluster header (ID 4 + unknown size 8)
    bytes.truncate(cut);
    bytes.extend_from_slice(&tags);
    assert!(probe_mkv(Cursor::new(bytes.clone())).is_ok());
    let s = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert_eq!(crate::pes::PesSource::info(&s).streams.len(), 1);
}

#[test]
fn read_back_maps_mpeg1_and_av1_codec_ids() {
    for (id, want) in [("V_MPEG1", Codec::Mpeg1), ("V_AV1", Codec::Av1)] {
        let stream = MkvStream::open(Cursor::new(synthetic_mkv(id, &[], &[]))).unwrap();
        match &crate::pes::PesSource::info(&stream).streams[0] {
            crate::disc::Stream::Video(v) => assert_eq!(v.codec, want, "{id}"),
            _ => panic!("expected a video stream"),
        }
    }
}

#[test]
fn blockless_block_group_is_counted_not_silent() {
    let mut cluster = Vec::new();
    ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
    let mut body = Vec::new();
    ebml::write_uint(&mut body, ebml::BLOCK_DURATION, 40).unwrap();
    ebml::write_size(&mut cluster, body.len() as u64).unwrap();
    cluster.extend_from_slice(&body);
    let mut stream = MkvStream::open(Cursor::new(synthetic_mkv("V_MPEG2", &[], &cluster))).unwrap();
    assert!(crate::pes::PesSource::read(&mut stream).unwrap().is_none());
    assert_eq!(crate::pes::PesSource::errors(&stream), 1);
    assert!(crate::pes::PesSource::lost_bytes(&stream) > 0);
}
