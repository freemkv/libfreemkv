use super::*;
use crate::disc::{
    AudioChannels, AudioStream, ColorSpace, ContentFormat, FrameRate, HdrFormat, LabelPurpose,
    Resolution, SampleRate, VideoStream,
};

// ── The language component is disc bytes ──────────────────────────────
// Language is three raw STN bytes via `from_utf8_lossy`, unsanitised. `00 00 00`
// (real Blu-ray "undefined") put a NUL in the path, failing `File::create`.

/// The stem stays one usable filename component whatever the disc says.
#[test]
fn a_hostile_language_code_cannot_break_the_output_filename() {
    let opts = DemuxOptions {
        base: "Movie".to_string(),
        ..Default::default()
    };
    assert!(matches!(opts.naming, Naming::Friendly), "default naming");

    for lang in ["\u{0}\u{0}\u{0}", "a/b", "..", "a\nb", "e:s"] {
        let stem = DemuxSink::stem_for(&opts, 1, 0x1100, lang, Codec::Ac3);
        assert!(
            !stem.chars().any(|c| c.is_control()),
            "control character survived into the filename for {lang:?}: {stem:?}"
        );
        assert!(
            !stem.contains('/') && !stem.contains('\\') && !stem.contains(':'),
            "a path separator survived for {lang:?}: {stem:?}"
        );
        assert!(
            std::path::Path::new(&stem).components().count() == 1,
            "the stem must stay ONE component for {lang:?}: {stem:?}"
        );
    }
}

/// A legitimate language is still carried through untouched — the
/// sanitiser must not be so lossy that it stops naming the track.
#[test]
fn an_ordinary_language_code_survives_sanitising() {
    let opts = DemuxOptions {
        base: "Movie".to_string(),
        ..Default::default()
    };
    let stem = DemuxSink::stem_for(&opts, 1, 0x1100, "eng", Codec::Ac3);
    assert!(stem.contains("eng"), "got {stem:?}");
}

fn video_stream(codec: Codec) -> DiscStream {
    DiscStream::Video(VideoStream {
        pid: 0x1011,
        codec,
        resolution: Resolution::R1080p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt709,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })
}

fn audio_stream(codec: Codec, lang: &str) -> DiscStream {
    DiscStream::Audio(AudioStream {
        pid: 0x1100,
        codec,
        channels: AudioChannels::Stereo,
        language: lang.to_string(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })
}

fn subtitle_stream(codec: Codec, lang: &str) -> DiscStream {
    DiscStream::Subtitle(crate::disc::SubtitleStream {
        pid: 0x1200,
        codec,
        language: lang.to_string(),
        forced: false,
        qualifier: crate::disc::LabelQualifier::None,
        codec_data: None,
    })
}

fn title_with(streams: Vec<DiscStream>, privates: Vec<Option<Vec<u8>>>) -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams = streams;
    t.codec_privates = privates;
    t.content_format = ContentFormat::BdTs;
    t
}

/// `audio://` and `sub://` are `demux://` with a kind filter: only tracks of
/// the selected class get a file; every other track is skipped entirely.
#[test]
fn kind_filter_keeps_only_the_selected_class() {
    let title = title_with(
        vec![
            video_stream(Codec::H264),
            audio_stream(Codec::Ac3, "eng"),
            subtitle_stream(Codec::Pgs, "eng"),
        ],
        vec![None, None, None],
    );
    let sub_opts = DemuxOptions {
        kind_filter: Some(TrackKind::Subtitle),
        export_chapters: false,
        ..Default::default()
    };
    let sub = DemuxSink::create(&tempdir(), &title, &sub_opts).unwrap();
    assert!(
        sub.tracks[0].is_none() && sub.tracks[1].is_none() && sub.tracks[2].is_some(),
        "sub:// keeps only the subtitle track"
    );
    let audio_opts = DemuxOptions {
        kind_filter: Some(TrackKind::Audio),
        export_chapters: false,
        ..Default::default()
    };
    let audio = DemuxSink::create(&tempdir(), &title, &audio_opts).unwrap();
    assert!(
        audio.tracks[0].is_none() && audio.tracks[1].is_some() && audio.tracks[2].is_none(),
        "audio:// keeps only the audio track"
    );
}

// `audio://` filters the video file out, but video is still the DELAY
// reference; must measure against its actual first PTS, not zero.
#[test]
fn audio_only_sink_delays_against_filtered_video_reference() {
    let dir = tempdir();
    let title = title_with(
        vec![video_stream(Codec::Mpeg2), audio_stream(Codec::Ac3, "eng")],
        vec![None, None],
    );
    let opts = DemuxOptions {
        base: "Ao".to_string(),
        kind_filter: Some(TrackKind::Audio),
        export_chapters: false,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    // Video starts at 500ms, audio at 600ms → true delay is +100ms.
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 500_000_000,
        keyframe: true,
        data: vec![0x00, 0x00, 0x01, 0xB3],
        duration_ns: None,
    })
    .unwrap();
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 1,
        pts: 600_000_000,
        keyframe: true,
        data: vec![0x0B, 0x77],
        duration_ns: None,
    })
    .unwrap();
    sink.finish().unwrap();

    let names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "Ao t01 eng AC3 DELAY 100ms.ac3"),
        "audio delay must be relative to the filtered video reference \
             (500ms), got {names:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// With no video reference at all (audio-only title), there is nothing to
/// measure the delay against. Omit the DELAY tag rather than emit one
/// computed against a fabricated zero reference.
#[test]
fn no_video_reference_omits_delay_tag() {
    let dir = tempdir();
    let title = title_with(vec![audio_stream(Codec::Ac3, "eng")], vec![None]);
    let opts = DemuxOptions {
        base: "NoRef".to_string(),
        export_chapters: false,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 600_000_000,
        keyframe: true,
        data: vec![0x0B, 0x77],
        duration_ns: None,
    })
    .unwrap();
    sink.finish().unwrap();

    let names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().all(|n| !n.to_lowercase().contains("delay")),
        "no video reference → no DELAY tag, got {names:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Annex-B reframing: covered in `crate::mux::hevc`; here we only assert
// sink-level wiring — param-set prepend and that a zero-length NAL mid-frame
// no longer truncates the rest of the AU.

#[test]
fn zero_length_nal_midframe_does_not_truncate_access_unit() {
    // The OLD local reframer `break`d on a zero-length NAL, dropping every
    // NAL after it. The canonical `append_length_prefixed_as_annex_b` skips
    // just the empty NAL and keeps going. Frame: NAL(2) | NAL(0) | NAL(3).
    let mut w = AnnexBWriter::new(Codec::H264, None);
    let mut out = Vec::new();
    let f = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![
            0, 0, 0, 2, 0xAA, 0xBB, // NAL #1 (len 2)
            0, 0, 0, 0, // zero-length NAL — must be skipped, not fatal
            0, 0, 0, 3, 0x01, 0x02, 0x03, // NAL #3 (len 3) — must survive
        ],
        duration_ns: None,
    };
    w.write_frame(&mut out, &f, 0).unwrap();
    // Both real NALs present; the empty NAL emitted nothing.
    assert_eq!(
        out,
        vec![0, 0, 0, 1, 0xAA, 0xBB, 0, 0, 0, 1, 0x01, 0x02, 0x03],
        "trailing NAL after a zero-length NAL must NOT be dropped"
    );
}

// Regression: an avcC may declare `lengthSizeMinusOne = 1` (2-octet NAL
// prefixes, ISO/IEC 14496-15 §5.3.3.1.2); assuming 4 octets silently
// wrote raw prefixed bytes as if Annex-B, with no start codes or error.
#[test]
fn annexb_writer_honours_the_records_declared_nal_length_size() {
    // avcC with byte 4 = 0xFD → lengthSizeMinusOne 1 → 2-octet prefixes.
    // numSPS = 1 (0xE1), SPS len 2 = [0x67 0x42], numPPS = 1, PPS len 1.
    let rec = [
        1, 0x42, 0x00, 0x1F, 0xFD, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 1, 0x68,
    ];
    assert_eq!(nal_length_size(Codec::H264, Some(&rec)), 2);
    let mut w = AnnexBWriter::new(Codec::H264, Some(&rec));
    let mut out = Vec::new();
    let f = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        // Two NALs with 2-octet length prefixes.
        data: vec![0, 2, 0xAA, 0xBB, 0, 3, 0x01, 0x02, 0x03],
        duration_ns: None,
    };
    w.write_frame(&mut out, &f, 0).unwrap();
    assert_eq!(
        out,
        vec![
            0, 0, 0, 1, 0x67, 0x42, // SPS
            0, 0, 0, 1, 0x68, // PPS
            0, 0, 0, 1, 0xAA, 0xBB, // frame NAL #1
            0, 0, 0, 1, 0x01, 0x02, 0x03, // frame NAL #2
        ],
        "2-octet-prefixed NALs must reach the ES as Annex B"
    );
}

#[test]
fn annexb_writer_prepends_params_once() {
    let rec = [
        1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 1, 0x68,
    ];
    let mut w = AnnexBWriter::new(Codec::H264, Some(&rec));
    let mut out = Vec::new();
    let f1 = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0, 0, 0, 2, 0xAA, 0xBB],
        duration_ns: None,
    };
    w.write_frame(&mut out, &f1, 0).unwrap();
    // params (SPS+PPS as annexb) then the frame NAL.
    assert_eq!(
        out,
        vec![
            0, 0, 0, 1, 0x67, 0x42, // SPS
            0, 0, 0, 1, 0x68, // PPS
            0, 0, 0, 1, 0xAA, 0xBB // frame NAL
        ]
    );
    // Second frame: NO param re-prepend.
    let mut out2 = Vec::new();
    let f2 = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: false,
        data: vec![0, 0, 0, 1, 0xCC],
        duration_ns: None,
    };
    w.write_frame(&mut out2, &f2, 0).unwrap();
    assert_eq!(out2, vec![0, 0, 0, 1, 0xCC]);
}

// ── Delay ────────────────────────────────────────────────────────────────

#[test]
fn delay_ms_sign_and_rounding() {
    // Audio later than video → positive (must be delayed).
    assert_eq!(delay_ms(1_000_000_000, 0), 1000);
    // Audio earlier than video → negative (must be advanced).
    assert_eq!(delay_ms(0, 248_000_000), -248);
    // Rounding to nearest ms.
    assert_eq!(delay_ms(1_600_000, 0), 2);
    assert_eq!(delay_ms(1_400_000, 0), 1);
    assert_eq!(delay_ms(-1_600_000, 0), -2);
}

#[test]
fn delay_token_matches_mkvmerge_regex() {
    // Convention: case-insensitive /delay\s+(-?\d+)/.
    let re = regex_lite_delay;
    assert_eq!(re("Movie eng AC3 DELAY -248ms.ac3"), Some(-248));
    assert_eq!(re(&format!("x {}.dts", delay_token(1000))), Some(1000));
    assert_eq!(re(&format!("x {}.thd", delay_token(0))), Some(0));
    assert_eq!(re(&format!("x {}.eac3", delay_token(-5))), Some(-5));
}

/// Minimal stand-in for the `delay\s+(-?\d+)` convention (case-insensitive).
fn regex_lite_delay(name: &str) -> Option<i64> {
    let lower = name.to_lowercase();
    let idx = lower.find("delay")?;
    let after = &name[idx + 5..];
    let after = after.trim_start();
    let mut chars = after.chars().peekable();
    let mut num = String::new();
    if chars.peek() == Some(&'-') {
        num.push('-');
        chars.next();
    }
    for c in chars {
        if c.is_ascii_digit() {
            num.push(c);
        } else {
            break;
        }
    }
    num.parse().ok()
}

// ── PGS .sup framing ─────────────────────────────────────────────────────

#[test]
fn pgs_sup_frames_each_segment_with_pg_header() {
    // One segment: type=0x16, size=2, payload=[0xDE,0xAD].
    let payload = [SEG_PCS, 0x00, 0x02, 0xDE, 0xAD];
    let mut out = Vec::new();
    let (written, _) = PgsSupWriter::emit_segments(&payload, 0x10, 0x10, &mut out).unwrap();
    assert_eq!(&out[0..2], &SUP_MAGIC);
    assert_eq!(&out[2..6], &0x10u32.to_be_bytes()); // PTS
    assert_eq!(&out[6..10], &0x10u32.to_be_bytes()); // DTS
    assert_eq!(&out[SUP_HEADER_LEN..], &payload); // segment body verbatim
    assert_eq!(written, out.len());
}

#[test]
fn pgs_frame_with_duration_emits_clear_segment() {
    // A display set with a real PCS (type 0x16) carrying 1920x1080, and a
    // duration → the writer must append a synthetic clear display set
    // (empty PCS + END) timestamped at pts + duration.
    let mut pcs = vec![SEG_PCS, 0x00, 0x0B];
    pcs.extend_from_slice(&[0x07, 0x80, 0x04, 0x38]); // 1920x1080
    pcs.extend_from_slice(&[0x10, 0x00, 0x00, 0x80, 0x00, 0x00, 0x01]); // 1 object
    let f = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 1_000_000_000, // 1s
        keyframe: true,
        data: pcs,
        duration_ns: Some(2_000_000_000), // 2s display → clear at 3s
    };
    let mut out = Vec::new();
    let mut w = PgsSupWriter::default();
    w.write_frame(&mut out, &f, f.pts).unwrap();
    w.finish(&mut out).unwrap();

    // Parse out every PG-framed segment: PG(2) PTS(4) DTS(4) type(1) size(2).
    let mut segs: Vec<(u8, u32)> = Vec::new();
    let mut pos = 0;
    while pos + SUP_HEADER_LEN <= out.len() {
        assert_eq!(
            &out[pos..pos + 2],
            &SUP_MAGIC,
            "each segment carries PG magic"
        );
        let pts = u32::from_be_bytes([out[pos + 2], out[pos + 3], out[pos + 4], out[pos + 5]]);
        let seg_type = out[pos + SUP_HEADER_LEN];
        let size =
            u16::from_be_bytes([out[pos + SUP_HEADER_LEN + 1], out[pos + SUP_HEADER_LEN + 2]])
                as usize;
        segs.push((seg_type, pts));
        pos += SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + size;
    }
    // Display PCS at 1s (90k), then a clear PCS + END at 3s.
    let clear90 = ns_to_90k(3_000_000_000);
    assert!(
        segs.iter().any(|&(t, p)| t == SEG_PCS && p == clear90),
        "a clear PCS must be emitted at pts+duration, got {segs:?}"
    );
    assert!(
        segs.iter().any(|&(t, p)| t == SEG_END && p == clear90),
        "an END segment must terminate the clear display set, got {segs:?}"
    );
}

#[test]
fn pgs_frame_without_duration_emits_no_clear() {
    // No duration → no synthetic clear (the subtitle's wipe time is unknown).
    let f = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![SEG_PCS, 0x00, 0x02, 0xDE, 0xAD],
        duration_ns: None,
    };
    let mut out = Vec::new();
    let mut w = PgsSupWriter::default();
    w.write_frame(&mut out, &f, 0).unwrap();
    // Exactly one PG-framed segment (the display), no clear appended.
    // Output = `.sup` header (10) + the on-wire segment (type+size 3 + 2
    // payload = 5) → 15 bytes, with no trailing clear.
    assert_eq!(&out[0..2], &SUP_MAGIC);
    assert_eq!(
        out.len(),
        SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + 2,
        "only the display segment, no clear"
    );
}

fn sup_frame(pts: i64, duration_ns: Option<u64>, visible: bool) -> PesFrame {
    let mut data = PgsSupWriter::synthetic_clear_display_set(1920, 1080);
    if visible {
        data[13] = 1;
        data[2] += 8;
        data.splice(14..14, [0, 0, 0, 0x40, 0, 0, 0, 0]);
    }
    PesFrame {
        discard_padding_ns: 0,
        track: 0,
        pts,
        duration_ns,
        data,
        keyframe: true,
        coding: None,
        source: None,
    }
}

fn sup_compositions(bytes: &[u8]) -> Vec<(u32, u8)> {
    let mut compositions = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        assert_eq!(&bytes[pos..pos + 2], &SUP_MAGIC);
        let pts = u32::from_be_bytes(bytes[pos + 2..pos + 6].try_into().unwrap());
        let data = &bytes[pos + SUP_HEADER_LEN..];
        let size = usize::from(u16::from_be_bytes([data[1], data[2]]));
        if data[0] == SEG_PCS {
            compositions.push((pts, data[13]));
        }
        pos += SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + size;
    }
    assert_eq!(pos, bytes.len());
    compositions
}

// A visible PCS with no duration has no known wipe time: neither write nor finish adds a clear.
#[test]
fn pgs_sup_visible_pcs_without_duration_gets_no_clear_at_finish() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    let f = sup_frame(1_000_000_000, None, true);
    writer.write_frame(&mut bytes, &f, f.pts).unwrap();
    writer.finish(&mut bytes).unwrap();
    assert_eq!(sup_compositions(&bytes), [(90_000, 1)]);
}

#[test]
fn pgs_sup_preserves_original_clear_without_a_duplicate_or_later_clear() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    for frame in [
        sup_frame(1_000_000_000, Some(2_000_000_000), true),
        sup_frame(3_000_000_000, Some(100_000), false),
    ] {
        let before = bytes.len();
        let written = writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
        assert_eq!(written, bytes.len() - before);
    }
    writer.finish(&mut bytes).unwrap();
    assert_eq!(sup_compositions(&bytes), [(90_000, 1), (270_000, 0)]);
}

#[test]
fn pgs_sup_replacement_cancels_old_clear_before_or_at_its_deadline() {
    for replace_ns in [2_000_000_000, 3_000_000_000] {
        let mut writer = PgsSupWriter::default();
        let mut bytes = Vec::new();
        for frame in [
            sup_frame(1_000_000_000, Some(2_000_000_000), true),
            sup_frame(replace_ns, Some(4_000_000_000), true),
        ] {
            writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
        }
        writer.finish(&mut bytes).unwrap();
        assert_eq!(
            sup_compositions(&bytes),
            [
                (90_000, 1),
                (ns_to_90k(replace_ns), 1),
                (ns_to_90k(replace_ns + 4_000_000_000), 0)
            ]
        );
    }
}

// The parser caps a display at 30 s; an authored clear later than that is the real end.
#[test]
fn pgs_sup_authored_clear_beyond_a_capped_duration_is_not_preempted() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    for frame in [
        sup_frame(0, Some(30_000_000_000), true),
        sup_frame(40_000_000_000, Some(0), false),
    ] {
        writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
    }
    writer.finish(&mut bytes).unwrap();
    assert_eq!(sup_compositions(&bytes), [(0, 1), (3_600_000, 0)]);
}

// A continuation segment stamped exactly at the wipe time follows the clear, not precedes it.
#[test]
fn pgs_sup_clear_precedes_a_non_pcs_frame_at_the_wipe_time() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    let display = sup_frame(0, Some(2_000_000_000), true);
    writer.write_frame(&mut bytes, &display, 0).unwrap();
    let mut end = sup_frame(2_000_000_000, None, false);
    end.data = vec![SEG_END, 0, 0];
    writer.write_frame(&mut bytes, &end, end.pts).unwrap();
    writer.finish(&mut bytes).unwrap();
    let mut types = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let size = usize::from(u16::from_be_bytes([
            bytes[pos + SUP_HEADER_LEN + 1],
            bytes[pos + SUP_HEADER_LEN + 2],
        ]));
        types.push(bytes[pos + SUP_HEADER_LEN]);
        pos += SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + size;
    }
    // display PCS + END, clear PCS + END, then the continuation END.
    assert_eq!(types, [SEG_PCS, SEG_END, SEG_PCS, SEG_END, SEG_END]);
}

// sub:// / demux:// finish() must flush the writer so the last subtitle is cleared.
#[test]
fn sink_finish_writes_the_last_pgs_subtitles_synthetic_clear() {
    let dir = tempdir();
    let title = title_with(vec![subtitle_stream(Codec::Pgs, "eng")], vec![None]);
    let opts = DemuxOptions {
        base: "Sub".to_string(),
        export_chapters: false,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    sink.write(&sup_frame(0, Some(2_000_000_000), true))
        .unwrap();
    sink.finish().unwrap();
    let sup = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "sup"))
        .expect("a .sup file");
    let bytes = std::fs::read(&sup).unwrap();
    assert_eq!(sup_compositions(&bytes), [(0, 1), (180_000, 0)]);
    let _ = std::fs::remove_dir_all(&dir);
}

// Hostile chapter names must not forge OGM lines or make the XML ill-formed.
#[test]
fn chapter_names_with_control_chars_are_neutralised() {
    let chaps = vec![Chapter {
        time_secs: 0.0,
        name: "x\nCHAPTER99=00:00:00.000\u{1}y".to_string(),
    }];
    let ogm = chapters_ogm(&chaps);
    assert_eq!(ogm.lines().count(), 2, "{ogm:?}");
    assert!(!ogm.lines().any(|l| l.starts_with("CHAPTER99")));
    let xml = chapters_xml(&chaps);
    assert!(!xml.contains('\u{1}'), "{xml:?}");
}

#[test]
fn pgs_sup_legacy_duration_only_input_clears_before_a_later_display() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    for frame in [
        sup_frame(0, Some(2_000_000_000), true),
        sup_frame(3_600_000_000_000, Some(3_000_000_000), true),
    ] {
        writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
    }
    writer.finish(&mut bytes).unwrap();
    assert_eq!(
        sup_compositions(&bytes),
        [(0, 1), (180_000, 0), (324_000_000, 1), (324_270_000, 0)]
    );
}

#[test]
fn pgs_sup_clear_only_input_never_synthesizes_another_clear() {
    for duration in [None, Some(0), Some(100_000), Some(5_000_000_000)] {
        let mut writer = PgsSupWriter::default();
        let mut bytes = Vec::new();
        let frame = sup_frame(1_000_000_000, duration, false);
        writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
        writer.finish(&mut bytes).unwrap();
        assert_eq!(sup_compositions(&bytes), [(90_000, 0)]);
    }
}

#[test]
fn pgs_sup_continuation_does_not_cancel_or_create_a_pending_clear() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    let frame = sup_frame(1_000_000_000, Some(2_000_000_000), true);
    writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
    let mut continuation = sup_frame(2_000_000_000, Some(5_000_000_000), false);
    continuation.data = vec![SEG_END, 0, 0];
    writer
        .write_frame(&mut bytes, &continuation, continuation.pts)
        .unwrap();
    writer.finish(&mut bytes).unwrap();
    assert_eq!(sup_compositions(&bytes), [(90_000, 1), (270_000, 0)]);
}

#[test]
fn pgs_sup_finish_is_idempotent() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    let frame = sup_frame(0, Some(2_000_000_000), true);
    writer.write_frame(&mut bytes, &frame, 0).unwrap();
    writer.finish(&mut bytes).unwrap();
    let once = bytes.clone();
    writer.finish(&mut bytes).unwrap();
    assert_eq!(bytes, once);
}

#[test]
fn pgs_sup_huge_duration_saturates_instead_of_wrapping_into_the_past() {
    let mut writer = PgsSupWriter::default();
    let mut bytes = Vec::new();
    let frame = sup_frame(1_000_000_000, Some(u64::MAX), true);
    writer.write_frame(&mut bytes, &frame, frame.pts).unwrap();
    writer.finish(&mut bytes).unwrap();
    assert_eq!(sup_compositions(&bytes), [(90_000, 1), (u32::MAX, 0)]);
}

#[test]
fn pgs_sup_truncated_segment_is_not_partially_written() {
    for data in [&[SEG_PCS][..], &[SEG_PCS, 0], &[SEG_PCS, 0, 2, 0xff]] {
        let mut bytes = Vec::new();
        assert_eq!(
            PgsSupWriter::emit_segments(data, 0, 0, &mut bytes).unwrap(),
            (0, data.len())
        );
        assert!(bytes.is_empty());
    }
    // A frame ending in a truncated segment is accounted, not silently lost.
    let mut writer = PgsSupWriter::default();
    let mut frame = sup_frame(0, None, true);
    frame.data.extend_from_slice(&[SEG_PCS, 0, 9, 1]);
    writer
        .write_frame(&mut Vec::new(), &frame, frame.pts)
        .unwrap();
    assert_eq!(writer.truncated_bytes, 4);
}

#[test]
fn ns_to_90k_conversion() {
    assert_eq!(ns_to_90k(0), 0);
    // 1 second = 90000 ticks.
    assert_eq!(ns_to_90k(1_000_000_000), 90_000);
    assert_eq!(ns_to_90k(-5), 0);
}

// Per design J16 (S0b); do not change without a spec citation proving otherwise.
// The `.sup` header carries the disc's own tick for every residue mod 9, keeps the
// ≤ 0 → 0 clamp and saturates at u32::MAX.
#[test]
fn sup_ticks_round_trip_disc_ticks_exactly() {
    use crate::mux::codec::pts_to_ns;
    for p in (1..20_000i64).chain([u32::MAX as i64 - 9, u32::MAX as i64]) {
        assert_eq!(
            ns_to_90k(pts_to_ns(p)) as i64,
            p,
            "tick {p} (residue {})",
            p % 9
        );
    }
    assert_eq!(ns_to_90k(pts_to_ns(-4)), 0, "≤ 0 clamps to 0");
    assert_eq!(ns_to_90k(i64::MAX), u32::MAX, "saturates at u32::MAX");
}

// ── VobSub .idx ──────────────────────────────────────────────────────────

#[test]
fn vobsub_idx_synthesis() {
    let dir = tempdir();
    let idx = dir.join("sub.idx");
    let mut w = VobSubWriter::new(idx.clone(), Some(b"palette: 000000, ffffff"), "eng");
    let mut sub = Vec::new();
    let f1 = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0xAA; 10],
        duration_ns: None,
    };
    let f2 = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 1_000_000_000,
        keyframe: true,
        data: vec![0xBB; 20],
        duration_ns: None,
    };
    w.write_frame(&mut sub, &f1, 0).unwrap();
    w.write_frame(&mut sub, &f2, 1_000_000_000).unwrap();
    w.finish(&mut sub).unwrap();
    let idx_text = std::fs::read_to_string(&idx).unwrap();
    assert!(idx_text.contains("palette: 000000, ffffff"));
    // The conventional `id:` line downstream muxers read to assign the language.
    assert!(
        idx_text.contains("id: en, index: 0"),
        "missing id: line, got:\n{idx_text}"
    );
    assert!(idx_text.contains("timestamp: 00:00:00:000, filepos: 000000000"));
    // Second SPU at 1s, filepos = 10.
    assert!(idx_text.contains("timestamp: 00:00:01:000, filepos: 00000000a"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn idx_timestamp_format() {
    assert_eq!(fmt_idx_timestamp(0), "00:00:00:000");
    assert_eq!(fmt_idx_timestamp(3_661_500_000_000), "01:01:01:500");
}

// ── Chapters ─────────────────────────────────────────────────────────────

#[test]
fn chapter_xml_and_ogm() {
    let chaps = vec![
        Chapter {
            time_secs: 0.0,
            name: "1".to_string(),
        },
        Chapter {
            time_secs: 65.5,
            name: "2".to_string(),
        },
    ];
    let xml = chapters_xml(&chaps);
    assert!(xml.contains("<ChapterTimeStart>00:00:00.000000000</ChapterTimeStart>"));
    assert!(xml.contains("<ChapterTimeStart>00:01:05.500000000</ChapterTimeStart>"));
    let ogm = chapters_ogm(&chaps);
    assert!(ogm.contains("CHAPTER01=00:00:00.000"));
    assert!(ogm.contains("CHAPTER02=00:01:05.500"));
    assert!(ogm.contains("CHAPTER02NAME=2"));
}

// Disc-sourced chapter names go straight into XML (`<ChapterString>`);
// `&`/`<`/`>` must be escaped, and `&` FIRST or the `<`/`>` replacements'
// ampersands get double-escaped (`<` rendering as literal `&lt;`).
#[test]
fn chapter_names_are_xml_escaped_so_a_disc_cannot_inject_markup() {
    let chaps = vec![Chapter {
        time_secs: 0.0,
        name: "</ChapterString><Injected/> Tom & Jerry <3 >:(".to_string(),
    }];
    let xml = chapters_xml(&chaps);

    assert!(
        xml.contains(
            "<ChapterString>&lt;/ChapterString&gt;&lt;Injected/&gt; \
                 Tom &amp; Jerry &lt;3 &gt;:(</ChapterString>"
        ),
        "every metacharacter escaped, and `&` escaped first so nothing is \
             double-escaped; got:\n{xml}"
    );
    // The injected element must not survive as markup anywhere in the file.
    assert!(
        !xml.contains("<Injected/>"),
        "a chapter name must not be able to open a new element"
    );
    // Exactly one ChapterString element pair — the name did not close it early.
    assert_eq!(xml.matches("<ChapterString>").count(), 1);
    assert_eq!(xml.matches("</ChapterString>").count(), 1);

    // A name with no metacharacters passes through byte-identical: escaping
    // must not rewrite ordinary text.
    let plain = chapters_xml(&[Chapter {
        time_secs: 0.0,
        name: "Opening Credits".to_string(),
    }]);
    assert!(plain.contains("<ChapterString>Opening Credits</ChapterString>"));
}

// ── Timeline continuity: the corrector itself is tested in `crate::mux::timeline`.
// Here we only confirm the sink drives it with the right `drives_epoch`: track 0
// is the epoch driver, every other track rides the same offset.

#[test]
fn timeline_track0_drives_epoch_others_ride() {
    let mut tl = TimelineContinuity::new();
    // Clip 1: video 0..10s (track 0 drives the epoch).
    assert_eq!(tl.adjust(0, true, 0), 0);
    assert_eq!(tl.adjust(0, false, 1), 0); // audio rides the same offset
    assert_eq!(tl.adjust(10_000_000_000, true, 0), 10_000_000_000);
    // Clip 2 seam: video PTS jumps back to ~0 (> 3s back) → new epoch.
    let out = tl.adjust(0, true, 0);
    assert!(out >= 10_000_000_000, "epoch must advance past prev high");
    // Audio in clip 2 (non-epoch) gets the SAME offset (A/V sync preserved).
    let a = tl.adjust(0, false, 1);
    assert_eq!(a, out);
}

// Regression: the epoch driver must follow `ref_video_track` (dynamic), not a hardcoded
// `frame.track == 0` — a PMT can list audio before video.
#[test]
fn epoch_driver_follows_ref_video_not_track_zero() {
    let dir = tempdir();
    // Audio FIRST (index 0), video SECOND (index 1).
    let title = title_with(
        vec![audio_stream(Codec::Ac3, "eng"), video_stream(Codec::H264)],
        vec![None, None],
    );
    let mut sink = DemuxSink::create(&dir, &title, &DemuxOptions::default()).unwrap();
    assert_eq!(
        sink.ref_video_track,
        Some(1),
        "video reference must be the first VIDEO stream (index 1), not 0"
    );

    let vid = |pts: i64, data: u8| PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 1, // VIDEO is track 1 here
        pts,
        keyframe: true,
        data: vec![0x00, 0x00, 0x00, 0x01, data],
        duration_ns: None,
    };
    // Clip 1 video: 0s then 10s — advances the frontier.
    sink.write(&vid(0, 0xAA)).unwrap();
    sink.write(&vid(10_000_000_000, 0xBB)).unwrap();
    // Clip 2 seam: video PTS jumps back to ~0 (> 3s back) → NEW epoch. The
    // video track (index 1) must drive this, bumping the offset.
    sink.write(&vid(0, 0xCC)).unwrap();

    assert!(
        sink.timeline.offset_ns >= 10_000_000_000,
        "video (track 1) must drive the epoch: offset_ns should have advanced \
             past the previous high, got {}",
        sink.timeline.offset_ns
    );
    sink.finish().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// A second video track (Dolby Vision EL) rides the base layer's epoch: its
// old-clip straggler after a seam must not open a spurious epoch.
#[test]
fn second_video_track_does_not_drive_epochs() {
    let dir = tempdir();
    let title = title_with(
        vec![video_stream(Codec::H264), video_stream(Codec::H264)],
        vec![None, None],
    );
    let mut sink = DemuxSink::create(&dir, &title, &DemuxOptions::default()).unwrap();
    let fr = |track: usize, pts: i64| PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track,
        pts,
        keyframe: true,
        data: vec![0x00, 0x00, 0x00, 0x01, 0xAA],
        duration_ns: None,
    };
    let s = 1_000_000_000i64;
    for f in [fr(0, 0), fr(1, 0), fr(0, 600 * s), fr(1, 600 * s), fr(0, 0)] {
        sink.write(&f).unwrap();
    }
    let seam = sink.timeline.offset_ns;
    assert!(seam >= 600 * s, "base layer reset opens the epoch");
    // Clip 1's EL tail, EL at the new clip's start, then BL continues.
    for f in [fr(1, 599 * s + s / 2), fr(1, 0), fr(0, 5 * s)] {
        sink.write(&f).unwrap();
    }
    assert_eq!(
        sink.timeline.offset_ns, seam,
        "the EL straggler must not open another epoch"
    );
    sink.finish().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── End-to-end sink ──────────────────────────────────────────────────────

#[test]
fn sink_keys_files_by_track_and_writes_all() {
    let dir = tempdir();
    let title = title_with(
        vec![video_stream(Codec::Mpeg2), audio_stream(Codec::Ac3, "eng")],
        vec![None, None],
    );
    let opts = DemuxOptions {
        base: "Test".to_string(),
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();

    // Video frame (track 0) and audio frame (track 1).
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0x00, 0x00, 0x01, 0xB3, 0xDE],
        duration_ns: None,
    })
    .unwrap();
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 1,
        pts: 100_000_000, // audio 100ms late
        keyframe: true,
        data: vec![0x0B, 0x77, 0x01, 0x02],
        duration_ns: None,
    })
    .unwrap();
    sink.finish().unwrap();

    // Video file written verbatim (passthrough).
    let v = std::fs::read(dir.join("Test t00 MPEG2.m2v")).unwrap();
    assert_eq!(v, vec![0x00, 0x00, 0x01, 0xB3, 0xDE]);
    // Audio file renamed with the delay token (100ms → DELAY 100ms).
    let a = std::fs::read(dir.join("Test t01 eng AC3 DELAY 100ms.ac3")).unwrap();
    assert_eq!(a, vec![0x0B, 0x77, 0x01, 0x02]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sink_respects_track_selection() {
    let dir = tempdir();
    let title = title_with(
        vec![video_stream(Codec::Mpeg2), audio_stream(Codec::Ac3, "eng")],
        vec![None, None],
    );
    let opts = DemuxOptions {
        base: "Sel".to_string(),
        selection: Some(vec![0]), // video only
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 1,
        pts: 0,
        keyframe: true,
        data: vec![0xFF],
        duration_ns: None,
    })
    .unwrap(); // dropped — track 1 not selected
    sink.finish().unwrap();
    assert!(dir.join("Sel t00 MPEG2.m2v").exists());
    // No audio file created.
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "ac3").unwrap_or(false))
        .collect();
    assert!(entries.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

// A demux export whose seam plan drops every frame must FAIL, not write empty track files
// and report success.
#[test]
fn a_demux_export_the_seam_plan_emptied_fails() {
    let dir = tempdir();
    let mut title = title_with(vec![video_stream(Codec::H264)], vec![None]);
    // Two clips (SeamPlan needs >= 2 with strictly increasing IN marks)
    // covering 100s..200s and 200s..300s in 45 kHz ticks.
    title.clips = vec![
        crate::disc::Clip {
            feed_span: None,
            clip_id: "00000".into(),
            in_time: 100 * 45_000,
            out_time: 200 * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        },
        crate::disc::Clip {
            feed_span: None,
            clip_id: "00001".into(),
            in_time: 200 * 45_000,
            out_time: 300 * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        },
    ];
    let mut sink = DemuxSink::create(&dir, &title, &DemuxOptions::default()).unwrap();

    // Frames far before the first IN mark: the plan places none of them.
    for (i, pts) in [0i64, 1_000_000_000].iter().enumerate() {
        let f = PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: *pts,
            keyframe: true,
            data: vec![0x00, 0x00, 0x00, 0x01, 0x09, 0x10, i as u8],
            duration_ns: None,
        };
        let _ = PesSink::write(&mut sink, &f);
    }

    let err = PesSink::finish(&mut sink).expect_err("an emptied export must fail");
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_SINK_WROTE_NOTHING),
        "a fully-dropped demux export must report SinkWroteNothing, not success"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A demux export where the seam plan drops MORE frames than it persists must report
/// SeamPlanDroppedMost — even though at least one frame did persist (frames_mapped > 0).
/// The denominator counts persisted frames only, so a mostly-emptied export can no longer
/// slip past as success.
#[test]
fn a_demux_export_the_seam_plan_mostly_emptied_fails() {
    let dir = tempdir();
    let mut title = title_with(vec![video_stream(Codec::H264)], vec![None]);
    title.clips = vec![
        crate::disc::Clip {
            feed_span: None,
            clip_id: "00000".into(),
            in_time: 100 * 45_000,
            out_time: 200 * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        },
        crate::disc::Clip {
            feed_span: None,
            clip_id: "00001".into(),
            in_time: 200 * 45_000,
            out_time: 300 * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        },
    ];
    let mut sink = DemuxSink::create(&dir, &title, &DemuxOptions::default()).unwrap();

    // Two frames before the first IN mark (dropped) …
    for (i, pts) in [0i64, 1_000_000_000].iter().enumerate() {
        let f = PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: *pts,
            keyframe: true,
            data: vec![0x00, 0x00, 0x00, 0x01, 0x09, 0x10, i as u8],
            duration_ns: None,
        };
        let _ = PesSink::write(&mut sink, &f);
    }
    // … and one frame inside the first clip (150 s, in ns) that DOES persist,
    // so frames_mapped > 0 while seam_dropped (2) still exceeds it (1).
    let inside = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 150_000_000_000,
        keyframe: true,
        data: vec![0x00, 0x00, 0x00, 0x01, 0x09, 0x10, 0x42],
        duration_ns: None,
    };
    PesSink::write(&mut sink, &inside).unwrap();

    let err = PesSink::finish(&mut sink).expect_err("a mostly-emptied export must fail");
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_SEAM_PLAN_DROPPED_MOST),
        "more frames dropped than persisted must report SeamPlanDroppedMost"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A FILTERED export (`audio://`) must NOT trip the drop gates on drops that belong to a
/// track it never persists. The video track is dropped hard at the clip join (2 frames, all
/// outside the marks), while the persisted audio track loses nothing. `dropped_total()`
/// would fold the video drops (2) into the numerator and, exceeding the persisted count
/// (1), wrongly report SeamPlanDroppedMost; `dropped_for(persisted_tracks)` sees 0 and
/// passes.
#[test]
fn a_filtered_export_ignores_drops_on_non_persisted_tracks() {
    let dir = tempdir();
    let mut title = title_with(
        vec![video_stream(Codec::H264), audio_stream(Codec::Ac3, "eng")],
        vec![None, None],
    );
    title.clips = vec![
        crate::disc::Clip {
            feed_span: None,
            clip_id: "00000".into(),
            in_time: 100 * 45_000,
            out_time: 200 * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        },
        crate::disc::Clip {
            feed_span: None,
            clip_id: "00001".into(),
            in_time: 200 * 45_000,
            out_time: 300 * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        },
    ];
    // `audio://`: only the audio track is persisted; video is filtered out.
    let opts = DemuxOptions {
        base: "Filt".to_string(),
        kind_filter: Some(TrackKind::Audio),
        export_chapters: false,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    assert!(
        sink.tracks[0].is_none() && sink.tracks[1].is_some(),
        "audio:// must not persist the video track"
    );

    // Two VIDEO frames before the first IN mark: the plan drops both, but the
    // video track has no file — these drops are not this export's shortfall.
    for (i, pts) in [0i64, 1_000_000_000].iter().enumerate() {
        let f = PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: *pts,
            keyframe: true,
            data: vec![0x00, 0x00, 0x00, 0x01, 0x09, 0x10, i as u8],
            duration_ns: None,
        };
        let _ = PesSink::write(&mut sink, &f);
    }
    // One AUDIO frame inside the first clip (150 s): placed and persisted.
    PesSink::write(
        &mut sink,
        &PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 1,
            pts: 150_000_000_000,
            keyframe: true,
            data: vec![0x0B, 0x77],
            duration_ns: None,
        },
    )
    .unwrap();

    // Gate must PASS: the only drops were on the filtered-out video track.
    PesSink::finish(&mut sink)
        .expect("a filtered export must not fail on drops it never persisted");
    // And the audio file was actually written with its payload.
    let wrote_audio = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.path().extension().map(|x| x == "ac3").unwrap_or(false)
                && std::fs::metadata(e.path())
                    .map(|m| m.len() > 0)
                    .unwrap_or(false)
        });
    assert!(
        wrote_audio,
        "the persisted audio track must have received bytes"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// `audio://` where every AUDIO frame falls outside the marks but video frames
// map: filtered video must not count as written, so the export fails loudly.
#[test]
fn a_filtered_export_whose_persisted_track_was_emptied_fails() {
    let dir = tempdir();
    let mut title = title_with(
        vec![video_stream(Codec::H264), audio_stream(Codec::Ac3, "eng")],
        vec![None, None],
    );
    title.clips = (0..2u32)
        .map(|i| crate::disc::Clip {
            feed_span: None,
            clip_id: format!("0000{i}"),
            in_time: (100 + 100 * i) * 45_000,
            out_time: (200 + 100 * i) * 45_000,
            duration_secs: 100.0,
            source_packets: 0,
        })
        .collect();
    let opts = DemuxOptions {
        base: "Empty".to_string(),
        kind_filter: Some(TrackKind::Audio),
        export_chapters: false,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    let frame = |track: usize, pts: i64| PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track,
        pts,
        keyframe: true,
        data: vec![0x0B, 0x77],
        duration_ns: None,
    };
    for s in [150i64, 151, 152] {
        let _ = PesSink::write(&mut sink, &frame(0, s * 1_000_000_000));
    }
    for s in [0i64, 1] {
        let _ = PesSink::write(&mut sink, &frame(1, s * 1_000_000_000));
    }
    let err = PesSink::finish(&mut sink).expect_err("no audio was written");
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_SINK_WROTE_NOTHING)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tiny unique temp dir helper (avoids a dev-dependency on `tempfile`).
fn tempdir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("fmkv_demux_test_{}_{}", std::process::id(), n));
    std::fs::create_dir_all(&p).unwrap();
    p
}

// `write_chapters` returning `Ok(())` without writing would report success
// with no chapter files. Both formats requested + checked with distinct
// names/timestamps, so a missing/wrong/empty writer output can't pass.
#[test]
fn finish_exports_both_chapter_formats_with_real_content() {
    let dir = tempdir();
    let mut title = title_with(vec![video_stream(Codec::Mpeg2)], vec![None]);
    title.chapters = vec![
        crate::disc::Chapter {
            time_secs: 0.0,
            name: "Opening".into(),
        },
        crate::disc::Chapter {
            time_secs: 62.5,
            name: "Second".into(),
        },
    ];
    let opts = DemuxOptions {
        base: "ChapTitle".into(),
        export_chapters: true,
        chapters_fmt: ChaptersFmt::Both,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    sink.finish().unwrap();

    let xml = std::fs::read_to_string(dir.join("ChapTitle chapters.xml"))
        .expect("chapters.xml must exist after finish");
    let ogm = std::fs::read_to_string(dir.join("ChapTitle chapters.txt"))
        .expect("chapters.txt must exist after finish");

    // Content, not merely existence: both chapters, both names, and the
    // 62.5 s timestamp formatted per its format.
    assert!(
        xml.contains("Opening") && xml.contains("Second"),
        "xml: {xml}"
    );
    assert!(
        xml.contains("00:01:02.500"),
        "xml must carry the real chapter time: {xml}"
    );
    assert!(
        ogm.contains("CHAPTER01=") && ogm.contains("CHAPTER02="),
        "ogm: {ogm}"
    );
    assert!(
        ogm.contains("Opening") && ogm.contains("Second"),
        "ogm names: {ogm}"
    );
    assert!(
        ogm.contains("00:01:02.500"),
        "ogm must carry the real chapter time: {ogm}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The other side of the gate: with the export switched off, no chapter
/// file is written at all. Without this the test above would also pass for
/// a `write_chapters` that ignored `opts.export_chapters`.
#[test]
fn chapters_are_not_exported_when_the_option_is_off() {
    let dir = tempdir();
    let mut title = title_with(vec![video_stream(Codec::Mpeg2)], vec![None]);
    title.chapters = vec![crate::disc::Chapter {
        time_secs: 0.0,
        name: "Opening".into(),
    }];
    let opts = DemuxOptions {
        base: "ChapTitle".into(),
        export_chapters: false,
        chapters_fmt: ChaptersFmt::Both,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    sink.finish().unwrap();
    assert!(!dir.join("ChapTitle chapters.xml").exists());
    assert!(!dir.join("ChapTitle chapters.txt").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

// Filenames are built from disc-controlled text (volume/track labels);
// `sanitize` is all that stands between that text and the filesystem — a
// surviving `/` escapes the output dir, other chars break Windows/SMB.
#[test]
fn sanitize_neutralises_every_path_hostile_character() {
    for c in ['/', '\\', ':', '*', '?', '"', '<', '>', '|'] {
        let got = sanitize(&format!("a{c}b"));
        assert_eq!(got, "a_b", "{c:?} must be replaced, got {got:?}");
    }
    // A traversal attempt in a disc label cannot escape the output directory:
    // no separator survives, so the whole thing stays ONE component.
    let escaped = sanitize("../../etc/passwd");
    assert_eq!(escaped, ".._.._etc_passwd");
    assert!(
        !std::path::Path::new(&escaped)
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir)),
        "the sanitized name must not decompose into a parent-directory hop"
    );
    // Ordinary characters — including spaces, dots, unicode and other
    // punctuation — are preserved, so the replacement is targeted, not a
    // blanket scrub that would mangle real titles.
    assert_eq!(
        sanitize("Amélie (2001) - Chapter 1.5 [Director's Cut]"),
        "Amélie (2001) - Chapter 1.5 [Director's Cut]"
    );
}

/// End-to-end witness that `sanitize` is actually applied on the write path:
/// a disc label containing a separator must produce ONE file inside the
/// chosen directory, never a write into a sibling/parent path.
#[test]
fn a_disc_label_with_a_separator_cannot_write_outside_the_output_directory() {
    let dir = tempdir();
    let title = title_with(vec![video_stream(Codec::Mpeg2)], vec![None]);
    let opts = DemuxOptions {
        base: "../evil/Title".into(),
        export_chapters: false,
        ..Default::default()
    };
    let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
    sink.write(&PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: vec![0x00, 0x00, 0x01, 0xB3, 0xAA],
        duration_ns: None,
    })
    .unwrap();
    sink.finish().unwrap();

    let names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 1, "exactly one output file; got {names:?}");
    assert!(
        names[0].starts_with(".._evil_Title"),
        "the separators must be neutralised in the real filename; got {names:?}"
    );
    assert!(
        !dir.parent().unwrap().join("evil").exists(),
        "nothing may be created outside the output directory"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
