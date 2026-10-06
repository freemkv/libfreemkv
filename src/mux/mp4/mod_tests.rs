use super::*;
use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, LabelPurpose,
    Resolution, SampleRate, Stream as DiscStream, SubtitleStream, VideoStream,
};
use crate::labels::LabelQualifier;

fn hevc_video() -> DiscStream {
    DiscStream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R2160p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Hdr10,
        color_space: ColorSpace::Bt2020,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })
}

fn audio(codec: Codec, lang: &str) -> DiscStream {
    DiscStream::Audio(AudioStream {
        pid: 0x1100,
        codec,
        channels: AudioChannels::Surround51,
        language: lang.into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })
}

fn subtitle() -> DiscStream {
    DiscStream::Subtitle(SubtitleStream {
        pid: 0x1200,
        codec: Codec::Pgs,
        language: "eng".into(),
        forced: false,
        qualifier: LabelQualifier::None,
        codec_data: None,
    })
}

fn title(streams: Vec<DiscStream>, cps: Vec<Option<Vec<u8>>>) -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams = streams;
    t.codec_privates = cps;
    t
}

// An AAC track the esds cannot describe (no ASC, PCE layout) is planned out, not
// promised and then dropped at finish().
#[test]
fn fit_report_includes_aac_only_with_a_describable_config() {
    let t = title(
        vec![
            hevc_video(),
            audio(Codec::Aac, "eng"),
            audio(Codec::Aac, "eng"),
            audio(Codec::Aac, "eng"),
            audio(Codec::Mp3, "eng"),
        ],
        vec![
            Some(vec![1, 2, 3]),
            Some(vec![0x11, 0x90]),
            None,
            Some(vec![0x11, 0x80]),
            None,
        ],
    );
    let r = fit_report(&t);
    assert_eq!(r.included, vec![0, 1, 4]);
    let skip = Mp4SkipReason::UnmappableAudio;
    assert_eq!(r.skipped, vec![(2, skip), (3, skip)]);
}

#[test]
fn fit_report_includes_video_and_dolby_only() {
    let t = title(
        vec![
            hevc_video(),
            audio(Codec::TrueHd, "eng"),
            audio(Codec::Ac3, "eng"),
            audio(Codec::Ac3Plus, "fra"),
            subtitle(),
        ],
        vec![Some(vec![1, 2, 3]), None, None, None, None],
    );
    let r = fit_report(&t);
    assert_eq!(r.included, vec![0, 2, 3], "video + AC3 + EAC3");
    // TrueHD (unmappable audio) and PGS (bitmap subtitle) are skipped.
    assert!(r.skipped.contains(&(1, Mp4SkipReason::UnmappableAudio)));
    assert!(r.skipped.contains(&(4, Mp4SkipReason::BitmapSubtitle)));
}

#[test]
fn fit_report_labels_unsupported_primary_video() {
    // A primary video whose codec the MP4 writer can't carry (VC-1) must be
    // skipped as UnmappableVideo, NOT SecondaryVideo (which means an MVC view).
    let mut vc1 = match hevc_video() {
        DiscStream::Video(v) => v,
        _ => unreachable!(),
    };
    vc1.codec = Codec::Vc1;
    let t = title(
        vec![DiscStream::Video(vc1), audio(Codec::Ac3, "eng")],
        vec![None, None],
    );
    let r = fit_report(&t);
    assert!(r.skipped.contains(&(0, Mp4SkipReason::UnmappableVideo)));
    assert_eq!(r.included, vec![1], "only the AC-3 audio is carried");
}

// A video track whose resolution never resolved must FAIL the mux, not be written as a 0x0
// track (ISO/IEC 14496-12 mandates width/height).
#[test]
fn a_video_track_with_no_resolved_resolution_is_an_error_not_a_zero_sized_track() {
    let DiscStream::Video(mut v) = hevc_video() else {
        unreachable!("hevc_video builds a video stream")
    };
    v.resolution = Resolution::Unknown;
    let t = title(
        vec![DiscStream::Video(v), audio(Codec::Ac3, "eng")],
        vec![Some(vec![0x01, 0x02, 0x03]), None],
    );

    let err = match Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t) {
        Ok(_) => panic!("an unrenderable 0x0 track must not be written silently"),
        Err(e) => e,
    };
    // `From<Error> for io::Error` stringifies as "E<code>[: ...]", so the
    // code round-trips in the message.
    assert!(
        err.to_string()
            .starts_with(&format!("E{}", crate::error::E_MP4_UNKNOWN_RESOLUTION)),
        "the failure must name the missing dimensions, not a generic mux \
             error; got {err}"
    );
}

#[test]
fn no_video_track_is_an_error() {
    let t = title(vec![audio(Codec::Ac3, "eng")], vec![None]);
    let err = match Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t) {
        Ok(_) => panic!("expected no-video-track error"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

fn frame(track: usize, pts_ns: i64, key: bool, data: Vec<u8>) -> PesFrame {
    PesFrame {
        discard_padding_ns: 0,
        track,
        pts: pts_ns,
        keyframe: key,
        data,
        duration_ns: None,
        source: None,
        coding: None,
    }
}

// A minimal AC-3 5.1 frame the audio parser accepts. Underscores mark
// BITFIELD boundaries in the header (e.g. 5-bit then 3-bit fields), not
// thousands-grouping; regrouping them uniformly would destroy that meaning.
#[allow(clippy::unusual_byte_groupings)]
fn ac3_frame() -> Vec<u8> {
    vec![
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
    ]
}

fn walk(buf: &[u8]) -> Vec<([u8; 4], usize, usize)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 8 <= buf.len() {
        let size = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
        let size = if size == 1 {
            u64::from_be_bytes([
                buf[pos + 8],
                buf[pos + 9],
                buf[pos + 10],
                buf[pos + 11],
                buf[pos + 12],
                buf[pos + 13],
                buf[pos + 14],
                buf[pos + 15],
            ]) as usize
        } else {
            size as usize
        };
        let bt = [buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]];
        assert!(size >= 8 && pos + size <= buf.len(), "box {bt:?} bad size");
        out.push((bt, pos, size));
        pos += size;
    }
    assert_eq!(pos, buf.len(), "top-level boxes tile exactly");
    out
}

// Direct child lookup by box type, one level, returning its payload. A
// minimal box walker duplicated here for tests — the reader's own
// box-walking (find_box) lives in read.rs and is private to that module.
fn find_child<'a>(buf: &'a [u8], want: &[u8; 4]) -> Option<&'a [u8]> {
    let mut pos = 0;
    while pos + 8 <= buf.len() {
        let size =
            u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        if size < 8 || pos + size > buf.len() {
            break;
        }
        if &buf[pos + 4..pos + 8] == want {
            return Some(&buf[pos + 8..pos + size]);
        }
        pos += size;
    }
    None
}

/// An MPEG-2 multichannel extension track (IFO coding mode 3) is reported only once its
/// `0xD0|n` packets arrive; declared alone it is in neither the plan nor the report.
#[test]
fn mp2_extension_is_reported_only_once_its_packets_arrive() {
    let ext = || {
        let mut a = audio(Codec::Mp2, "eng");
        if let DiscStream::Audio(x) = &mut a {
            x.pid = 0x00D0;
            x.label = crate::disc::MP2_EXTENSION_LABEL.into();
        }
        a
    };
    let t = title(
        vec![hevc_video(), ext()],
        vec![Some(vec![1, 2, 3, 4]), None],
    );
    assert!(fit_report(&t).skipped.is_empty(), "declared only: no note");
    let mut quiet = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    quiet.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    quiet.finish().unwrap();
    assert!(quiet.undelivered_streams().is_empty());
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    s.write(&frame(1, 0, true, vec![0x7F, 0xF0, 0x00])).unwrap();
    s.write(&frame(1, 0, true, vec![0x7F, 0xF0, 0x00])).unwrap();
    s.finish().unwrap();
    assert_eq!(s.undelivered_streams(), vec![1]);
    assert_eq!(
        s.final_report().skipped,
        vec![(1, Mp4SkipReason::Mp2Extension)]
    );
}

// A multi-clip playlist's source PTS restarts at a join: the sink must place the next
// clip after the first (the timeline corrector the MKV muxer uses), never on top of it.
#[test]
fn a_clip_join_pts_reset_continues_the_timeline() {
    let t = title(vec![hevc_video()], vec![Some(vec![1, 2, 3, 4])]);
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    for _clip in 0..2 {
        for i in 0..150i64 {
            let key = i % 25 == 0;
            s.write(&frame(0, i * 40_000_000, key, vec![0xAB; 64]))
                .unwrap();
        }
    }
    let pts: Vec<i64> = s.tracks[0].samples.iter().map(|x| x.pts_ns).collect();
    assert_eq!(pts.len(), 300);
    assert!(
        pts.windows(2).all(|w| w[1] > w[0]),
        "clip 2 must follow clip 1: {:?}",
        &pts[148..152]
    );
}

// An audio track whose frames never yield a parseable sample entry must be dropped from
// moov, not written as an stsd around an empty entry, while the frames still reach mdat.
#[test]
fn audio_track_with_no_parseable_sample_entry_is_dropped_not_emitted_empty() {
    let t = title(
        vec![hevc_video(), audio(Codec::Ac3, "eng")],
        vec![Some(vec![1, 2, 3, 4]), None],
    );
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    // Not an AC-3 syncframe: dolby_sample_entry cannot parse it, ever.
    let junk = vec![0x5Au8; 64];
    s.write(&frame(1, 0, true, junk.clone())).unwrap();
    s.write(&frame(1, 32_000_000, true, junk.clone())).unwrap();
    s.finish().unwrap();
    let buf = s.writer.into_inner();

    let boxes = walk(&buf);
    let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
    let moov = &buf[ms + 8..ms + msz];
    let mut traks = 0;
    let mut pos = 0;
    while pos + 8 <= moov.len() {
        let size =
            u32::from_be_bytes([moov[pos], moov[pos + 1], moov[pos + 2], moov[pos + 3]]) as usize;
        if &moov[pos + 4..pos + 8] == b"trak" {
            traks += 1;
        }
        if size < 8 {
            break;
        }
        pos += size;
    }
    assert_eq!(
        traks, 1,
        "only the video trak may be described; the undescribable audio track is dropped"
    );

    // The audio bytes were still WRITTEN (no silent frame loss) — they simply
    // end up unreferenced in mdat rather than being discarded at write time.
    assert!(
        buf.windows(junk.len()).any(|w| w == &junk[..]),
        "audio frames must reach mdat rather than being dropped by write()"
    );
}

// Dropping the undescribable audio track keeps the export succeeding, but
// final_report()/undelivered_streams() must stop claiming that stream afterward.
#[test]
fn dropped_audio_track_is_reported_not_just_logged() {
    let t = title(
        vec![hevc_video(), audio(Codec::Ac3, "eng")],
        vec![Some(vec![1, 2, 3, 4]), None],
    );

    // The PRE-mux plan promises the audio stream. It cannot know better: the
    // codec fits, only the frames turn out to be unparseable.
    let plan = fit_report(&t);
    assert_eq!(plan.included, vec![0, 1], "the plan promises both streams");

    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    // Not an AC-3 syncframe — `dolby_sample_entry` can never parse it.
    s.write(&frame(1, 0, true, vec![0x5Au8; 64])).unwrap();
    assert!(
        s.undelivered_streams().is_empty(),
        "nothing is decided before finish()"
    );
    s.finish().unwrap();

    let actual = s.final_report();
    assert_eq!(
        actual.included,
        vec![0],
        "the post-mux report must list only the video the file actually carries"
    );
    assert!(
        actual
            .skipped
            .contains(&(1, Mp4SkipReason::UndescribableAudio)),
        "the dropped audio stream must appear as skipped with its reason: {:?}",
        actual.skipped
    );
    assert_eq!(
        s.undelivered_streams(),
        vec![1],
        "the driver's programmatic loss signal must name stream 1"
    );
}

// mvhd.next_track_id must EXCEED every track_ID in the file (ISO/IEC 14496-12 §8.2.2), not
// be derived from the retained track count.
#[test]
fn mvhd_next_track_id_exceeds_every_retained_track_id() {
    let t = title(
        vec![
            hevc_video(),
            audio(Codec::Ac3, "eng"), // track_id 2 — gets no samples, dropped
            audio(Codec::Ac3, "fra"), // track_id 3 — survives
        ],
        vec![Some(vec![1, 2, 3, 4]), None, None],
    );
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    // Nothing for stream 1; stream 2 gets real AC-3.
    s.write(&frame(2, 0, true, ac3_frame())).unwrap();
    s.write(&frame(2, 32_000_000, true, ac3_frame())).unwrap();
    s.finish().unwrap();

    // The middle track really was dropped (ids 1 and 3 retained).
    assert_eq!(s.undelivered_streams(), vec![1]);
    assert!(
        s.final_report()
            .skipped
            .contains(&(1, Mp4SkipReason::NoSamples))
    );
    let retained_ids: Vec<u32> = s.tracks.iter().map(|t| t.track_id).collect();
    assert_eq!(retained_ids, vec![1, 3]);

    let buf = s.writer.into_inner();
    let boxes = walk(&buf);
    let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
    let moov = &buf[ms + 8..ms + msz];
    // mvhd is moov's first child; next_track_id is its last 4 bytes.
    let mvhd_size = u32::from_be_bytes([moov[0], moov[1], moov[2], moov[3]]) as usize;
    assert_eq!(&moov[4..8], b"mvhd");
    let next_id = u32::from_be_bytes([
        moov[mvhd_size - 4],
        moov[mvhd_size - 3],
        moov[mvhd_size - 2],
        moov[mvhd_size - 1],
    ]);
    assert!(
        retained_ids.iter().all(|&id| next_id > id),
        "next_track_id {next_id} must exceed every used id {retained_ids:?}"
    );
    assert_eq!(next_id, 4);
}

#[test]
fn av_mux_has_two_traks_and_tiles() {
    let t = title(
        vec![hevc_video(), audio(Codec::Ac3, "eng")],
        vec![Some(vec![1, 2, 3, 4]), None],
    );
    let d = 41_708_333;
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    // Two video frames (track 0) + two AC-3 frames (track 1).
    s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    s.write(&frame(1, 0, true, ac3_frame())).unwrap();
    s.write(&frame(0, d, false, vec![0xCD; 400])).unwrap();
    s.write(&frame(1, 32_000_000, true, ac3_frame())).unwrap();
    s.finish().unwrap();
    let buf = s.writer.into_inner();
    let boxes = walk(&buf);
    let types: Vec<[u8; 4]> = boxes.iter().map(|(t, _, _)| *t).collect();
    // Faststart layout: ftyp, moov, free (reserve slack), mdat — moov BEFORE mdat.
    assert_eq!(
        types,
        vec![*b"ftyp", *b"moov", *b"free", *b"mdat"],
        "faststart: moov precedes mdat"
    );
    // moov must contain exactly two trak boxes.
    let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
    let moov = &buf[ms + 8..ms + msz];
    let trak_count = {
        let mut n = 0;
        let mut pos = 0;
        while pos + 8 <= moov.len() {
            let size = u32::from_be_bytes([moov[pos], moov[pos + 1], moov[pos + 2], moov[pos + 3]])
                as usize;
            if &moov[pos + 4..pos + 8] == b"trak" {
                n += 1;
            }
            if size < 8 {
                break;
            }
            pos += size;
        }
        n
    };
    assert_eq!(trak_count, 2, "one video + one audio trak");
    // mdat = header + 800+400 video + two AC-3 frames.
    let (_, _, mdat_sz) = *boxes.iter().find(|(t, _, _)| t == b"mdat").unwrap();
    assert_eq!(mdat_sz, 16 + 800 + 400 + ac3_frame().len() * 2);
}

#[test]
fn reserve_rounds_to_4mb_plus_buffer() {
    // round_up_4MB(x) + 4 MiB, floored at 8 MiB. Must saturate rather than wrap:
    // div_ceil(GRAIN) * GRAIN overflows within one grain of u64::MAX, and a
    // wrapped product is SMALL, turning the largest estimate into a tiny reserve.
    assert!(
        round_up_grain(u64::MAX) >= u64::MAX - (4 << 20),
        "round_up_grain must saturate near u64::MAX, not wrap to a small value"
    );
    // The reserve the writer emits must fit the `free` box's 32-bit size field.
    assert!(RESERVE_CAP <= u32::MAX as u64);
    assert_eq!(RESERVE_CAP % RESERVE_GRAIN, 0);
    assert_eq!(round_up_grain(1), 4 << 20);
    assert_eq!(round_up_grain(4 << 20), 4 << 20);
    assert_eq!(round_up_grain((4 << 20) + 1), 8 << 20);
    // A 2 hr feature, 24 fps video + one AC-3 track: ~173k + ~225k samples
    // × 16 B ≈ 6.4 MB → round to 8 MB → +4 MB buffer = 12 MB (floor also 8+4).
    let mut t = title(vec![hevc_video(), audio(Codec::Ac3, "eng")], vec![]);
    t.duration_secs = 7200.0;
    let r = estimate_reserve(&t, &[0, 1]);
    assert!(
        r.is_multiple_of(4 << 20),
        "reserve is 4 MiB-aligned + 4 MiB buffer"
    );
    assert!(
        (12 << 20..=20 << 20).contains(&r),
        "≈12-16 MB for a 2h feature, got {r}"
    );

    // The case above is dominated by the floor+buffer (per-sample term ~6.4 MB
    // is under the floor), so BYTES_PER_SAMPLE=0 stays green there. Pin a case
    // (2h HEVC + 8 AC-3 tracks) where per-sample dominates: ~30.1 MiB → 36 MiB.
    let mut streams = vec![hevc_video()];
    streams.extend((0..8).map(|_| audio(Codec::Ac3, "eng")));
    let mut t = title(streams, vec![]);
    t.duration_secs = 7200.0;
    let included: Vec<usize> = (0..9).collect();
    let r = estimate_reserve(&t, &included);
    assert_eq!(
        r,
        36 << 20,
        "per-sample term must dominate: 1.97M samples × 16 B → 32 MiB + 4 MiB buffer"
    );
    assert!(
        r > RESERVE_FLOOR + RESERVE_BUFFER,
        "this case must NOT be reachable from the floor alone"
    );
}

#[test]
fn detect_rate_snaps_23_976() {
    let d = 41_708_333;
    let samples: Vec<Sample> = (0..10)
        .map(|i| Sample {
            offset: 0,
            size: 1,
            pts_ns: i as i64 * d,
            keyframe: i == 0,
        })
        .collect();
    assert_eq!(detect_rate(&samples), (24000, 1001));
}

// ── colr (ITU-T H.273 / CICP) ────────────────────────────────────────────

/// Decode `(primaries, transfer, matrix, full_range)` back out of the `colr`
/// nclx box of an emitted visual sample entry, so the assertion is on the
/// bytes that reach the file. `None` when no `colr` box was written.
fn colr_of(v: &VideoStream) -> Option<(u16, u16, u16, bool)> {
    // `codec_private` is a byte pattern that cannot itself contain "colr".
    let stsd = build_visual_stsd(
        Codec::Hevc,
        &[0u8; 8],
        1920,
        1080,
        video_colr(&DiscStream::Video(v.clone())),
    );
    let i = stsd.windows(4).position(|w| w == b"colr")?;
    let p = &stsd[i + 4..];
    assert_eq!(&p[..4], b"nclx", "only the nclx colour type is written");
    Some((
        u16::from_be_bytes([p[4], p[5]]),
        u16::from_be_bytes([p[6], p[7]]),
        u16::from_be_bytes([p[8], p[9]]),
        p[10] & 0x80 != 0,
    ))
}

fn video_stream() -> VideoStream {
    match hevc_video() {
        DiscStream::Video(v) => v,
        _ => unreachable!(),
    }
}

#[test]
fn colr_transfer_is_hlg_for_an_hlg_title_not_pq() {
    // ITU-T H.273 Table 3: transfer 18 = HLG, 16 = PQ. `video_colr` used to
    // hardcode 16 for every BT.2020 stream, wrongly applying PQ EOTF to HLG.
    let mut v = video_stream();
    v.hdr = HdrFormat::Hlg;
    v.color_space = ColorSpace::Bt2020;
    assert_eq!(
        colr_of(&v).expect("colr written"),
        (9, 18, 9, false),
        "BT.2020 primaries/matrix (9) with the HLG transfer (18)"
    );
}

#[test]
fn colr_transfer_is_bt470bg_for_a_pal_dvd_not_bt601() {
    // ITU-T H.273: transfer 5 = ITU-R BT.470-6 System B/G, 6 = BT.601.
    // A PAL DVD is System B/G in all three code points.
    let mut v = video_stream();
    v.hdr = HdrFormat::Sdr;
    v.color_space = ColorSpace::Bt470bg;
    assert_eq!(colr_of(&v).expect("colr written"), (5, 5, 5, false));
}

#[test]
fn colr_agrees_with_the_shared_cicp_resolver_for_every_color_space() {
    // One resolver, every sink: the `colr` box must carry exactly what
    // `mkv::cicp_for_video` returns for the same stream, so an mp4:// rip and
    // an mkv:// rip of one title can never describe different colour.
    for cs in [
        ColorSpace::Bt709,
        ColorSpace::Bt2020,
        ColorSpace::Bt470bg,
        ColorSpace::Smpte170m,
    ] {
        for hdr in [
            HdrFormat::Sdr,
            HdrFormat::Hdr10,
            HdrFormat::Hdr10Plus,
            HdrFormat::Hlg,
            HdrFormat::DolbyVision,
        ] {
            let mut v = video_stream();
            v.color_space = cs;
            v.hdr = hdr;
            let (m, t, p, r) = crate::mux::mkv::cicp_for_video(&v);
            assert_eq!(
                colr_of(&v).expect("colr written"),
                (p as u16, t as u16, m as u16, r == 2),
                "colr disagrees with the shared resolver for {cs:?} / {hdr:?}"
            );
        }
    }
    // Unknown colorimetry: no usable colour info, so no `colr` box at all —
    // an absent box and an "unspecified" (2/2/2) box mean the same thing, and
    // writing nothing is what this sink has always done.
    let mut v = video_stream();
    v.color_space = ColorSpace::Unknown;
    assert!(colr_of(&v).is_none());
}

// ── detect_rate ──────────────────────────────────────────────────────────

/// Mux a video-only MP4 whose samples are exactly `delta_ns` apart and return
/// the `(mdhd.timescale, stts.sample_delta)` decoded out of the emitted file.
fn muxed_video_timing(delta_ns: i64) -> (u32, u32) {
    let t = title(vec![hevc_video()], vec![Some(vec![1, 2, 3, 4])]);
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    for i in 0..10i64 {
        s.write(&frame(0, i * delta_ns, i == 0, vec![0xAB; 16]))
            .unwrap();
    }
    s.finish().unwrap();
    let buf = s.writer.into_inner();

    // One trak → exactly one `mdhd` and one `stts`.
    let i = buf.windows(4).position(|w| w == b"mdhd").expect("mdhd");
    // After the type: version+flags(4), creation(8), modification(8), timescale(4).
    let timescale = u32::from_be_bytes(buf[i + 24..i + 28].try_into().unwrap());
    let j = buf.windows(4).position(|w| w == b"stts").expect("stts");
    // After the type: version+flags(4), entry_count(4), sample_count(4), sample_delta(4).
    let delta = u32::from_be_bytes(buf[j + 16..j + 20].try_into().unwrap());
    (timescale, delta)
}

#[test]
fn exact_integer_frame_rates_are_not_declared_as_their_fractional_twins() {
    // `detect_rate` used to return the FIRST STD_RATES entry within 0.5 fps, so
    // 24/30/60 fps were always written as their 1000/1001 twins. Read the
    // declared timescale/sample_delta back out of the muxed file to check.
    for (delta_ns, want) in [
        (41_666_667i64, (24u32, 1u32)), // 24.000
        (33_333_333, (30, 1)),          // 30.000
        (16_666_667, (60, 1)),          // 60.000
        (40_000_000, (25, 1)),          // 25.000
        (20_000_000, (50, 1)),          // 50.000
        (41_708_333, (24_000, 1001)),   // 23.976
        (33_366_667, (30_000, 1001)),   // 29.97
        (16_683_333, (60_000, 1001)),   // 59.94
    ] {
        assert_eq!(
            muxed_video_timing(delta_ns),
            want,
            "{delta_ns} ns/frame must be declared as {want:?}"
        );
    }
}

// ── pack_language ────────────────────────────────────────────────────────

// Bit-twiddles pinned against hand-computed values; three mutants proven equivalent.
#[test]
fn pack_language_packs_three_lowercase_letters_into_15_bits() {
    // "bcd": b=2, c=3, d=4 → (2<<10)|(3<<5)|4 = 2048+96+4 = 0x0864.
    assert_eq!(pack_language("bcd"), [0x08, 0x64]);
    // ISO 639-2 "eng", cross-checked against read.rs's `mdhd_language`
    // fixture, which decodes the same packed constant back to "eng".
    assert_eq!(pack_language("eng"), [0x15, 0xC7]);
}

/// Every rejection path falls back to the fixed "und" encoding: wrong
/// length (both shorter and longer than 3) and not-all-lowercase (upper
/// case, a digit, a non-ASCII byte).
#[test]
fn pack_language_falls_back_to_und_for_anything_not_three_lowercase_letters() {
    const UND: [u8; 2] = [0x55, 0xC4];
    assert_eq!(pack_language("en"), UND, "too short");
    assert_eq!(pack_language("engl"), UND, "too long");
    assert_eq!(pack_language("ENG"), UND, "not lowercase");
    assert_eq!(pack_language("e1g"), UND, "not all letters");
    assert_eq!(pack_language(""), UND, "empty");
}

// ── faststart reserve sizing constants and guards ──────────────────────────

#[test]
fn reserve_floor_is_8_mebibytes() {
    // A shifted-the-wrong-way `8 << 20` silently becomes 0, collapsing the
    // floor. Every other reserve test builds on this constant, so pin it
    // directly rather than only through their derived numbers.
    assert_eq!(RESERVE_FLOOR, 8 * 1024 * 1024);
}

// estimate_reserve's per-stream fps uses the stream's own rate only when n > 0 && d > 0,
// else a flat 24.0 fallback; a film-rate (23.976) fixture is the one that can tell the two
// apart.
#[test]
fn estimate_reserve_uses_the_streams_own_fps_not_the_24fps_fallback() {
    let mut t = title(vec![hevc_video()], vec![]);
    t.duration_secs = 76_500.0;
    let r = estimate_reserve(&t, &[0]);
    assert_eq!(
        r, 33_554_432,
        "23.976 fps must be used verbatim, not rounded up to a flat 24.0"
    );
}

// Complementary case: FrameRate::Unknown (n=0) must take the 24.0 fallback branch, not
// compute 0/1 = 0.0 fps.
#[test]
fn estimate_reserve_unknown_frame_rate_falls_back_to_24fps_not_zero() {
    let mut vc1 = match hevc_video() {
        DiscStream::Video(v) => v,
        _ => unreachable!(),
    };
    vc1.frame_rate = FrameRate::Unknown;
    let mut t = title(vec![DiscStream::Video(vc1)], vec![]);
    t.duration_secs = 76_500.0;
    let r = estimate_reserve(&t, &[0]);
    // Same duration, taking the 24.0 fallback: lands in a DIFFERENT grain
    // than the 23.976 fps case above (37_748_736 vs 33_554_432), and far
    // above the floor+buffer a 0-fps collapse would produce (12_582_912).
    assert_eq!(
        r, 37_748_736,
        "an unknown frame rate must estimate as if it were 24 fps, not 0"
    );
}

// estimate_reserve models DTS at 512 samples/AU, a third of the 1536 (E-)AC-3 default;
// losing the DTS arm under-reserves DTS titles 3x.
#[test]
fn estimate_reserve_models_dts_at_512_samples_per_frame_not_1536() {
    let t_dts = {
        let mut t = title(vec![audio(Codec::Dts, "eng")], vec![]);
        t.duration_secs = 100_000.0;
        t
    };
    let r_dts = estimate_reserve(&t_dts, &[0]);
    assert_eq!(
        r_dts, 155_189_248,
        "a DTS track must be modelled at 512 samples/frame"
    );

    let t_ac3 = {
        let mut t = title(vec![audio(Codec::Ac3, "eng")], vec![]);
        t.duration_secs = 100_000.0;
        t
    };
    let r_ac3 = estimate_reserve(&t_ac3, &[0]);
    assert_eq!(
        r_ac3, 54_525_952,
        "an AC-3 track (the `_` arm) is modelled at 1536 samples/frame"
    );
    assert_ne!(
        r_dts, r_ac3,
        "DTS and AC-3 must not be modelled identically — one is a third of the other"
    );
}

// Track timing arithmetic: tests above never pin a NUMBER out of the
// PTS→ticks/duration/tkhd_dur/ctts chain, so any operator there (`+`↔`-`,
// `*`↔`/`) could flip with the suite green. These pin exact chosen values.

/// `VideoTiming::derive`'s composition ticks are `(pts - min_pts) * ts / NS`.
/// `min_pts` is deliberately non-zero: with min_pts == 0 a mutated `+` gives
/// the same answer as `-` and the mutant survives for free.
#[test]
fn video_timing_derive_subtracts_the_minimum_pts_not_adds_it() {
    let offset = 5_000_000_000i64; // 5 s, so min_pts != 0
    let d = 40_000_000i64; // 40 ms/frame — exact 25 fps
    // Decode order carries a classic I-P-B-B reorder (P presents last).
    let samples = vec![
        Sample {
            offset: 0,
            size: 1,
            pts_ns: offset,
            keyframe: true,
        },
        Sample {
            offset: 0,
            size: 1,
            pts_ns: offset + 3 * d,
            keyframe: false,
        },
        Sample {
            offset: 0,
            size: 1,
            pts_ns: offset + d,
            keyframe: false,
        },
        Sample {
            offset: 0,
            size: 1,
            pts_ns: offset + 2 * d,
            keyframe: false,
        },
    ];
    let timing = VideoTiming::derive(&samples);
    assert_eq!(timing.timescale, 25, "exact 25 fps snaps to timescale 25");
    assert_eq!(timing.sample_dur, 1);
    assert_eq!(
        timing.cts,
        vec![0, 3, 1, 2],
        "cts must be (pts - min_pts) * ts / NS, in decode order"
    );
}

/// `VideoTiming::total_duration` is `sample_count * sample_dur`. `sample_dur`
/// is deliberately not 1 — `*` and `/` by 1 are the same function, so a
/// mutated `/` would survive a `sample_dur == 1` fixture for free.
#[test]
fn video_timing_total_duration_multiplies_count_by_sample_duration() {
    let timing = VideoTiming {
        timescale: 24_000,
        sample_dur: 1001,
        cts: vec![0, 1001, 2002, 3003],
    };
    assert_eq!(timing.total_duration(), 4 * 1001);
}

/// `VideoTiming::ctts` is `cts[i] - i * sample_dur`. `sample_dur` is again
/// not 1, so `*` and `/` disagree.
#[test]
fn video_timing_ctts_subtracts_index_times_sample_duration() {
    let timing = VideoTiming {
        timescale: 25,
        sample_dur: 2,
        cts: vec![10, 5, 20],
    };
    // [10 - 0*2, 5 - 1*2, 20 - 2*2] = [10, 3, 16].
    assert_eq!(timing.ctts(), vec![10, 3, 16]);
}

// tkhd.duration = secs * MOVIE_TIMESCALE, a separate 90 kHz computation from mdhd's
// media-timescale duration (ISO/IEC 14496-12 §8.3.2). Read straight from emitted tkhd
// bytes.
#[test]
fn tkhd_duration_is_seconds_times_movie_timescale_for_both_media_types() {
    fn tkhd_duration(trak: &[u8]) -> u64 {
        let tkhd = find_child(&trak[8..], b"tkhd").expect("tkhd");
        u64::from_be_bytes(tkhd[28..36].try_into().unwrap())
    }

    // Video: 4 samples at exact 25 fps → total_duration 4 ticks @ ts 25 →
    // secs = 0.16 → tkhd_dur = 0.16 * 90_000 = 14_400 exactly.
    let video = Track {
        media: Media::Video,
        track_id: 1,
        stream_idx: 0,
        codec: Codec::Hevc,
        codec_private: vec![0xAA, 0xBB],
        width: 1920,
        height: 1080,
        colr: None,
        language: [0x55, 0xC4],
        audio_entry: None,
        audio_timescale: 0,
        samples: (0..4)
            .map(|i| Sample {
                offset: 0,
                size: 1,
                pts_ns: i * 40_000_000,
                keyframe: i == 0,
            })
            .collect(),
    };
    let (vtrak, vsecs) = build_trak_at(&video, 0);
    assert_eq!(vsecs, 0.16);
    assert_eq!(
        tkhd_duration(&vtrak),
        14_400,
        "video tkhd.duration must be secs * MOVIE_TIMESCALE"
    );

    // Audio: 2 samples 32 ms apart at 48 kHz → per-sample duration 1536
    // ticks (exact), media_dur = 2 * 1536 = 3072, secs = 3072/48000 = 0.064
    // → tkhd_dur = 0.064 * 90_000 = 5_760 exactly.
    let audio = Track {
        media: Media::Audio,
        track_id: 2,
        stream_idx: 1,
        codec: Codec::Ac3,
        codec_private: Vec::new(),
        width: 0,
        height: 0,
        colr: None,
        language: [0x55, 0xC4],
        audio_entry: Some(vec![0x0B, 0x77]),
        audio_timescale: 48_000,
        samples: vec![
            Sample {
                offset: 0,
                size: 10,
                pts_ns: 0,
                keyframe: true,
            },
            Sample {
                offset: 10,
                size: 10,
                pts_ns: 32_000_000,
                keyframe: true,
            },
        ],
    };
    let (atrak, asecs) = build_trak_at(&audio, 0);
    assert_eq!(
        asecs,
        3072.0 / 48_000.0,
        "audio secs must be media_dur / timescale, not any other combination"
    );
    assert_eq!(
        tkhd_duration(&atrak),
        5_760,
        "audio tkhd.duration must be secs * MOVIE_TIMESCALE"
    );
}

// The earliest sample across tracks is t=0; a track that starts later keeps its offset
// through an empty edit, and video before its first keyframe is not stored.
#[test]
fn a_later_starting_track_keeps_its_offset_through_an_empty_edit() {
    let mk = |media, id, first_ns: i64| Track {
        media,
        track_id: id,
        stream_idx: 0,
        codec: Codec::Hevc,
        codec_private: vec![1],
        width: 16,
        height: 16,
        colr: None,
        language: [0x55, 0xC4],
        audio_entry: Some(vec![0x0B, 0x77]),
        audio_timescale: 48_000,
        samples: (0..2)
            .map(|i| Sample {
                offset: 0,
                size: 1,
                pts_ns: first_ns + i * 40_000_000,
                keyframe: i == 0,
            })
            .collect(),
    };
    let tracks = [mk(Media::Video, 1, 40_000_000), mk(Media::Audio, 2, 0)];
    let delays = start_delays_ns(&tracks);
    assert_eq!(delays, vec![40_000_000, 0]);
    let elst = |t: &Track, d| {
        let (trak, _) = build_trak_at(t, d);
        let edts = find_child(&trak[8..], b"edts")?;
        let e = find_child(edts, b"elst")?;
        Some(e.to_vec())
    };
    let e = elst(&tracks[0], delays[0]).expect("video track carries an edit list");
    // version 1: entry_count, then (segment_duration u64, media_time i64, rate)
    assert_eq!(u32::from_be_bytes(e[4..8].try_into().unwrap()), 2);
    assert_eq!(u64::from_be_bytes(e[8..16].try_into().unwrap()), 3_600);
    assert_eq!(i64::from_be_bytes(e[16..24].try_into().unwrap()), -1);
    assert_eq!(i64::from_be_bytes(e[36..44].try_into().unwrap()), 0);
    assert!(
        elst(&tracks[1], delays[1]).is_none(),
        "the earliest track needs no edit"
    );
}

// Frames lost to a damaged span leave the last CTS past n*d; the edit list must still
// cover the whole presentation or players cut the tail.
#[test]
fn edit_list_covers_the_last_frame_when_frames_were_lost() {
    let mut samples: Vec<Sample> = (0..4)
        .map(|i| Sample {
            offset: 0,
            size: 1,
            pts_ns: i * 40_000_000,
            keyframe: i == 0,
        })
        .collect();
    samples.push(Sample {
        offset: 0,
        size: 1,
        pts_ns: 10_000_000_000,
        keyframe: false,
    });
    let t = Track {
        media: Media::Video,
        track_id: 1,
        stream_idx: 0,
        codec: Codec::Hevc,
        codec_private: vec![1],
        width: 16,
        height: 16,
        colr: None,
        language: [0x55, 0xC4],
        audio_entry: None,
        audio_timescale: 0,
        samples,
    };
    let (trak, secs) = build_trak_at(&t, 40_000_000);
    let edts = find_child(&trak[8..], b"edts").unwrap();
    let e = find_child(edts, b"elst").unwrap();
    let seg = u64::from_be_bytes(e[28..36].try_into().unwrap());
    assert!(seg >= 10 * MOVIE_TIMESCALE as u64, "segment {seg}");
    assert!(secs >= 10.04, "{secs}");
}

// AAC / MP3 entries come from the MPEG branch; a rate the title could not name is
// taken from the entry for the mdhd timescale.
#[test]
fn mp3_track_gets_an_entry_and_its_rate_from_the_first_frame() {
    let mut mp3 = audio(Codec::Mp3, "eng");
    if let DiscStream::Audio(a) = &mut mp3 {
        a.sample_rate = SampleRate::Unknown;
    }
    let t = title(vec![hevc_video(), mp3], vec![Some(vec![1, 2, 3, 4]), None]);
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
    // MPEG-2 Layer III header, 22.05 kHz, mono.
    let mp3_frame = vec![0xFF, 0xF3, 0x90, 0xC4, 0, 0, 0, 0];
    s.write(&frame(1, 0, true, mp3_frame.clone())).unwrap();
    s.write(&frame(1, 26_122_449, true, mp3_frame)).unwrap();
    s.finish().unwrap();
    assert!(s.undelivered_streams().is_empty());
    assert_eq!(s.tracks[1].audio_timescale, 22_050);
    assert!(s.tracks[1].audio_entry.is_some());
}

// Extreme first timestamps (a clamped reader) must not overflow the start delay.
#[test]
fn start_delays_saturate_on_extreme_timestamps() {
    let mk = |id, pts_ns| Track {
        media: Media::Audio,
        track_id: id,
        stream_idx: 0,
        codec: Codec::Ac3,
        codec_private: Vec::new(),
        width: 0,
        height: 0,
        colr: None,
        language: [0x55, 0xC4],
        audio_entry: None,
        audio_timescale: 48_000,
        samples: vec![Sample {
            offset: 0,
            size: 1,
            pts_ns,
            keyframe: true,
        }],
    };
    let delays = start_delays_ns(&[mk(1, i64::MAX), mk(2, i64::MIN)]);
    assert_eq!(delays, vec![i64::MAX, 0]);
}

#[test]
fn video_before_its_first_keyframe_is_not_stored() {
    let t = title(vec![hevc_video()], vec![Some(vec![1, 2, 3, 4])]);
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, false, vec![1; 8])).unwrap();
    s.write(&frame(0, 40_000_000, true, vec![2; 8])).unwrap();
    s.write(&frame(0, 80_000_000, false, vec![3; 8])).unwrap();
    assert_eq!(s.tracks[0].samples.len(), 2);
    assert_eq!(s.tracks[0].samples[0].pts_ns, 40_000_000);
}

// audio_sample_durations' per-sample ticks are ns*ts/NS; inter-sample delta is
// ticks(next)-ticks(prev), last duration repeated for the trailing sample.
#[test]
fn audio_sample_durations_computes_exact_tick_deltas_and_repeats_the_last() {
    let samples = vec![
        Sample {
            offset: 0,
            size: 1,
            pts_ns: 0,
            keyframe: true,
        },
        Sample {
            offset: 0,
            size: 1,
            pts_ns: 32_000_000, // 1536 ticks @ 48 kHz, exact
            keyframe: true,
        },
        Sample {
            offset: 0,
            size: 1,
            pts_ns: 64_000_000, // 3072 ticks @ 48 kHz, exact
            keyframe: true,
        },
    ];
    let durs = audio_sample_durations(&samples, 48_000);
    assert_eq!(
        durs,
        vec![1536, 1536, 1536],
        "two 1536-tick deltas, and the trailing sample repeats the last"
    );
}

// Single-sample fallback pushes timescale/30, guarded by !samples.is_empty(); windows(2)
// yields nothing so it's the only source of a duration.
#[test]
fn audio_sample_durations_single_sample_uses_timescale_over_30_fallback() {
    let samples = vec![Sample {
        offset: 0,
        size: 1,
        pts_ns: 0,
        keyframe: true,
    }];
    assert_eq!(
        audio_sample_durations(&samples, 48_000),
        vec![48_000 / 30],
        "a lone sample gets one fallback duration, timescale/30"
    );
}

#[test]
fn detect_rate_picks_the_nearest_std_rate_regardless_of_table_order() {
    // Order-independence: every entry must resolve to itself when its own exact
    // rate is measured, regardless of position in STD_RATES (unlike first-match).
    for &(ts, dur, rate) in STD_RATES {
        let d = (NS as f64 / rate).round() as i64;
        let samples: Vec<Sample> = (0..10)
            .map(|i| Sample {
                offset: 0,
                size: 1,
                pts_ns: i as i64 * d,
                keyframe: i == 0,
            })
            .collect();
        assert_eq!(
            detect_rate(&samples),
            (ts, dur),
            "{rate} fps must resolve to its own STD_RATES entry"
        );
    }
}

// Fewer than 2 samples can't measure a delta: fixed 90 kHz/3003 fallback. Exactly 2 samples
// is the boundary, not just "fewer than 2".
#[test]
fn detect_rate_needs_at_least_two_samples_not_more() {
    assert_eq!(detect_rate(&[]), (90_000, 3_003));
    assert_eq!(
        detect_rate(&[Sample {
            offset: 0,
            size: 1,
            pts_ns: 0,
            keyframe: true
        }]),
        (90_000, 3_003)
    );
    let two = vec![
        Sample {
            offset: 0,
            size: 1,
            pts_ns: 0,
            keyframe: true,
        },
        Sample {
            offset: 0,
            size: 1,
            pts_ns: 40_000_000, // exact 25 fps
            keyframe: false,
        },
    ];
    assert_eq!(
        detect_rate(&two),
        (25, 1),
        "exactly 2 samples is enough to measure a real rate, not the fallback"
    );
}

// Only positive deltas are considered (filter(|&d| d > 0)); letting zero deltas through
// shifts the median index and collapses fps to infinity.
#[test]
fn detect_rate_filters_zero_deltas_not_just_negative_ones() {
    let samples: Vec<Sample> = vec![0, 0, 0, 40_000_000]
        .into_iter()
        .map(|pts_ns| Sample {
            offset: 0,
            size: 1,
            pts_ns,
            keyframe: pts_ns == 0,
        })
        .collect();
    assert_eq!(
        detect_rate(&samples),
        (25, 1),
        "the three duplicate zero deltas must be filtered out entirely, \
             leaving the one real 25 fps delta as the (only, hence median) value"
    );
}

// A rate with no nearby STD_RATES entry takes the fallback: timescale 90_000, duration =
// (median * 90_000) / NS. 5 fps gives an exact multiple.
#[test]
fn detect_rate_fallback_duration_is_median_times_90khz_over_ns() {
    let samples: Vec<Sample> = (0..10)
        .map(|i| Sample {
            offset: 0,
            size: 1,
            pts_ns: i as i64 * 200_000_000, // 5 fps — far outside every STD_RATES window
            keyframe: i == 0,
        })
        .collect();
    assert_eq!(
        detect_rate(&samples),
        (90_000, 18_000),
        "median 200_000_000 ns * 90_000 / 1e9 = 18_000 exactly"
    );
}

// ── build_ftyp ───────────────────────────────────────────────────────────

#[test]
fn build_ftyp_names_the_codec_specific_compatible_brand() {
    let hevc = build_ftyp(Codec::Hevc);
    assert!(
        hevc.windows(4).any(|w| w == b"hvc1"),
        "an HEVC title must declare the hvc1 compatible brand"
    );
    let h264 = build_ftyp(Codec::H264);
    assert!(
        h264.windows(4).any(|w| w == b"avc1"),
        "an H.264 title must declare the avc1 compatible brand"
    );
    assert_ne!(
        hevc, h264,
        "the two codec brands must not collapse to the same ftyp"
    );
}

// ── build_audio_stbl run-length coalescing ─────────────────────────────────

#[test]
fn build_audio_stbl_coalesces_equal_adjacent_durations_only() {
    let samples: Vec<Sample> = (0..5)
        .map(|i| Sample {
            offset: i as u64 * 10,
            size: 10,
            pts_ns: 0,
            keyframe: true,
        })
        .collect();
    // Two distinct adjacent runs: [5,5] then [7,7,7]. Catches a coalescing
    // guard that always (mis)matches or an inverted `==`/`!=`.
    let durs = vec![5u32, 5, 7, 7, 7];
    let stbl = build_audio_stbl(vec![0u8; 4], &samples, &durs);
    let stts = find_child(&stbl[8..], b"stts").expect("stts");
    let entry_count = u32::from_be_bytes(stts[4..8].try_into().unwrap());
    assert_eq!(
        entry_count, 2,
        "5,5,7,7,7 must coalesce into exactly two runs"
    );
    assert_eq!(
        &stts[8..16],
        &[0, 0, 0, 2, 0, 0, 0, 5][..],
        "first run: count=2 value=5"
    );
    assert_eq!(
        &stts[16..24],
        &[0, 0, 0, 3, 0, 0, 0, 7][..],
        "second run: count=3 value=7"
    );
}

// ── faststart_fits ───────────────────────────────────────────────────────

#[test]
fn faststart_fits_zero_or_at_least_eight_bytes_only() {
    assert!(faststart_fits(0), "an exact fill needs no free box");
    for g in 1..8 {
        assert!(
            !faststart_fits(g),
            "{g} bytes cannot be expressed as any box (min header is 8)"
        );
    }
    assert!(faststart_fits(8), "8 bytes is exactly one empty free box");
    assert!(faststart_fits(1_000_000));
}

// ── Stream::read is write-only ──────────────────────────────────────────

#[test]
fn write_after_finish_is_rejected() {
    let t = title(vec![hevc_video()], vec![Some(vec![1, 2, 3])]);
    let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
    s.write(&frame(0, 0, true, vec![1, 2, 3])).unwrap();
    s.finish().unwrap();
    let err = s.write(&frame(0, 40_000_000, false, vec![4])).unwrap_err();
    assert!(
        err.to_string()
            .starts_with(&format!("E{}", crate::error::E_STREAM_CLOSED)),
        "write after finish must be StreamClosed; got {err}"
    );
}

#[test]
fn video_cts_rounds_to_nearest_tick() {
    let samples: Vec<Sample> = (0..5i64)
        .map(|i| Sample {
            offset: 0,
            size: 1,
            pts_ns: i * 1001 * 1_000_000_000 / 24000,
            keyframe: true,
        })
        .collect();
    let timing = VideoTiming::derive(&samples);
    assert_eq!(timing.timescale, 24000);
    assert!(
        timing.ctts().iter().all(|&c| c == 0),
        "CFR video must have zero ctts; got {:?}",
        timing.ctts()
    );
}
