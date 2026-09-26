//! Decoder-valid synthetic PGS fixtures: no disc data or external assets.
//! Unit tests run without FFmpeg; the explicitly ignored interoperability tests
//! require ffmpeg/ffprobe and are also run by CI.

use super::*;
use crate::disc::{Codec, SubtitleStream};
use crate::mux::mkv::{MkvMuxer, MkvTrack};
use crate::mux::mkvstream::MkvStream;
use crate::pes::{PesFrame, SourcePos, Stream};
use std::io::Cursor;
use std::path::Path;
use std::process::Command;

fn segment(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut data = vec![kind];
    data.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
    data.extend_from_slice(payload);
    data
}

fn pcs(objects: u8, forced: bool, number: u16) -> Vec<u8> {
    let mut data = vec![0x07, 0x80, 0x04, 0x38, 0x10]; // 1920x1080
    data.extend_from_slice(&number.to_be_bytes());
    data.extend_from_slice(&[if objects == 0 { 0 } else { 0x80 }, 0, 0, objects]);
    for object in 0..objects {
        data.extend_from_slice(&[0, object, 0, if forced { 0x40 } else { 0 }, 0, 0, 0, 0]);
    }
    segment(0x16, &data)
}

fn window() -> Vec<u8> {
    segment(0x17, &[1, 0, 0, 0, 0, 0, 0, 2, 0, 2])
}

fn end() -> Vec<u8> {
    segment(0x80, &[])
}

fn display(forced: bool, number: u16) -> Vec<u8> {
    [
        pcs(1, forced, number),
        window(),
        // Palette 0, version 0, colour 1: opaque white.
        segment(0x14, &[0, 0, 1, 235, 128, 128, 255]),
        // Object 0, first+last, 2x2, RLE: two white pixels + EOL per row.
        segment(
            0x15,
            &[0, 0, 0, 0xc0, 0, 0, 12, 0, 2, 0, 2, 1, 1, 0, 0, 1, 1, 0, 0],
        ),
        end(),
    ]
    .concat()
}

fn clear(number: u16) -> Vec<u8> {
    [pcs(0, false, number), window(), end()].concat()
}

fn packet(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
    PesPacket {
        pid: 0x1200,
        pts,
        dts: None,
        data,
        source: None,
        discontinuity: false,
    }
}

fn subtitle_track() -> MkvTrack {
    MkvTrack::subtitle(&SubtitleStream {
        pid: 0x1200,
        codec: Codec::Pgs,
        language: "eng".into(),
        forced: false,
        qualifier: crate::labels::LabelQualifier::None,
        codec_data: None,
    })
}

fn mux(frames: &[Frame]) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    let mut muxer = MkvMuxer::new(&mut bytes, &[subtitle_track()], None, 0.0, &[]).unwrap();
    for frame in frames {
        muxer
            .write_frame(
                0,
                frame.pts_ns,
                frame.keyframe,
                &frame.data,
                frame.duration_ns,
                None,
            )
            .unwrap();
    }
    muxer.finish().unwrap();
    bytes.into_inner()
}

fn read_mkv(bytes: Vec<u8>) -> Vec<PesFrame> {
    let mut reader = MkvStream::open(Cursor::new(bytes)).unwrap();
    let mut frames = Vec::new();
    while let Some(frame) = reader.read().unwrap() {
        frames.push(frame);
    }
    frames
}

fn sparse_sequence() -> Vec<Frame> {
    let mut parser = PgsParser::new();
    let mut frames = Vec::new();
    for (start, number) in [(180, 0), (3780, 2)] {
        frames.extend(parser.parse(&packet(display(true, number), Some(start * 90_000))));
        frames.extend(parser.parse(&packet(
            pcs(0, false, number + 1),
            Some((start + 3) * 90_000),
        )));
        // Separate WDS and END PES packets reproduce the old orphaned segments.
        frames.extend(parser.parse(&packet(window(), Some((start + 3) * 90_000))));
        frames.extend(parser.parse(&packet(end(), Some((start + 3) * 90_000))));
    }
    frames.extend(parser.flush());
    frames
}

#[test]
fn forced_subtitles_clear_before_an_hour_long_gap() {
    let frames = sparse_sequence();
    assert_eq!(frames.len(), 4);
    assert_eq!(
        frames.iter().map(|f| f.pts_ns).collect::<Vec<_>>(),
        vec![
            180_000_000_000,
            183_000_000_000,
            3_780_000_000_000,
            3_783_000_000_000
        ]
    );
    for (index, frame) in frames.iter().enumerate() {
        assert_eq!(
            frame.data,
            if index % 2 == 0 {
                display(true, index as u16)
            } else {
                clear(index as u16)
            }
        );
        assert_eq!(
            frame.duration_ns,
            Some(if index % 2 == 0 { 3_000_000_000 } else { 0 })
        );
    }
}

#[test]
fn clear_emits_at_end_without_waiting_for_another_subtitle() {
    let mut parser = PgsParser::new();
    assert!(
        parser
            .parse(&packet(display(true, 0), Some(90_000)))
            .is_empty()
    );
    let visible = parser.parse(&packet(pcs(0, false, 1), Some(360_000)));
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].duration_ns, Some(3_000_000_000));
    assert!(parser.parse(&packet(window(), None)).is_empty());
    let wipe = parser.parse(&packet(end(), None));
    assert_eq!(wipe.len(), 1);
    assert_eq!(wipe[0].data, clear(1));
    assert_eq!(wipe[0].pts_ns, 4_000_000_000);
    assert!(parser.flush().is_empty());
}

#[test]
fn complete_clear_in_one_pes_emits_display_and_clear_separately() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(display(false, 0), Some(90_000)));
    let frames = parser.parse(&packet(clear(1), Some(270_000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, display(false, 0));
    assert_eq!(frames[1].data, clear(1));
    assert_eq!(frames[1].pts_ns, 3_000_000_000);
}

#[test]
fn every_clear_continuation_split_preserves_bytes_and_opening_timestamp() {
    let tail = [window(), end()].concat();
    for split in 0..=tail.len() {
        let mut parser = PgsParser::new();
        let mut opening = packet(pcs(0, false, 7), Some(900_000));
        opening.source = Some(SourcePos::at_byte(777));
        let mut frames = parser.parse(&opening);
        frames.extend(parser.parse(&packet(tail[..split].to_vec(), None)));
        frames.extend(parser.parse(&packet(tail[split..].to_vec(), Some(999_000))));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 1, "split {split}");
        assert_eq!(frames[0].data, clear(7), "split {split}");
        assert_eq!(frames[0].pts_ns, 10_000_000_000, "split {split}");
        assert_eq!(frames[0].source.unwrap().byte, 777, "split {split}");
    }
}

#[test]
fn leading_and_consecutive_clears_are_preserved() {
    let mut parser = PgsParser::new();
    for number in 0..3 {
        let frames = parser.parse(&packet(clear(number), Some(i64::from(number) * 90_000)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, clear(number));
        assert_eq!(frames[0].duration_ns, Some(0));
    }
    assert!(parser.flush().is_empty());
}

#[test]
fn incomplete_clear_is_flushed_once_at_eof() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(pcs(0, false, 1), Some(90_000)));
    parser.parse(&packet(window(), None));
    let frames = parser.flush();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, [pcs(0, false, 1), window()].concat());
    assert_eq!(frames[0].duration_ns, Some(0));
    assert!(parser.flush().is_empty());
}

#[test]
fn incomplete_clear_does_not_absorb_the_next_display() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(pcs(0, false, 0), Some(90_000)));
    let frames = parser.parse(&packet(display(true, 1), Some(180_000)));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, pcs(0, false, 0));
    assert_eq!(frames[0].duration_ns, Some(0));
    assert_eq!(parser.flush()[0].data, display(true, 1));
}

#[test]
fn end_marker_inside_segment_payload_does_not_complete_clear() {
    let mut parser = PgsParser::new();
    let data = [pcs(0, false, 0), segment(0x17, &[0x80, 0, 0])].concat();
    assert!(parser.parse(&packet(data.clone(), Some(90_000))).is_empty());
    let frames = parser.parse(&packet(end(), None));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, [data, end()].concat());
}

#[test]
fn truncated_segment_length_cannot_fabricate_a_complete_clear() {
    let mut parser = PgsParser::new();
    let bad = [pcs(0, false, 0), vec![0x17, 0xff, 0xff], end()].concat();
    assert!(parser.parse(&packet(bad, Some(90_000))).is_empty());
    // Resync at the next PCS and preserve the new valid clear.
    let frames = parser.parse(&packet(clear(1), Some(180_000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[1].data, clear(1));
}

#[test]
fn clear_without_pts_never_uses_zero_as_its_timestamp() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(display(true, 0), Some(90_000)));
    let frames = parser.parse(&packet(clear(1), None));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].data, display(true, 0));
    assert_eq!(frames[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
    assert!(parser.flush().is_empty());
}

#[test]
fn clear_timestamp_and_source_come_from_clear_not_display() {
    let mut parser = PgsParser::new();
    let mut visible = packet(display(true, 0), Some(90_000));
    visible.source = Some(SourcePos::at_byte(111));
    parser.parse(&visible);
    let mut wipe = packet(clear(1), Some(270_000));
    wipe.source = Some(SourcePos::at_byte(222));
    let frames = parser.parse(&wipe);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].source.unwrap().byte, 111);
    assert_eq!(frames[1].source.unwrap().byte, 222);
    assert_eq!(frames[1].pts_ns, 3_000_000_000);
}

#[test]
fn clear_sets_do_not_demote_a_forced_track() {
    let mut tracker = ForcedTracker::new();
    for frame in sparse_sequence() {
        tracker.observe(&frame.data);
    }
    assert!(tracker.is_forced());
    assert_eq!(
        tracker.facts(),
        ForcedFacts {
            displays: 2,
            forced_displays: 2
        }
    );
}

#[test]
fn clear_only_track_is_not_mistaken_for_a_forced_track() {
    let mut tracker = ForcedTracker::new();
    for number in 0..10 {
        tracker.observe(&clear(number));
    }
    assert!(!tracker.observed());
    assert!(!tracker.is_forced());
    assert_eq!(tracker.facts(), ForcedFacts::default());
}

#[test]
fn ordinary_and_forced_subtitles_share_the_same_clear_lifecycle() {
    for forced in [false, true] {
        let mut parser = PgsParser::new();
        parser.parse(&packet(display(forced, 0), Some(90_000)));
        let frames = parser.parse(&packet(clear(1), Some(270_000)));
        assert_eq!(frames.len(), 2);
        assert_eq!(display_set_is_forced(&frames[0].data), Some(forced));
        assert_eq!(display_set_is_forced(&frames[1].data), None);
        assert_eq!(frames[0].duration_ns, Some(2_000_000_000));
    }
}

#[test]
fn mkv_roundtrip_preserves_display_clear_bytes_and_timestamps() {
    let original = sparse_sequence();
    let bytes = mux(&original);
    let decoded = read_mkv(bytes.clone());
    assert_eq!(decoded.len(), 4);
    for (before, after) in original.iter().zip(decoded) {
        // A subtitle-only MKV is rebased to its first input timestamp.
        assert_eq!(before.pts_ns - original[0].pts_ns, after.pts);
        assert_eq!(before.data, after.data);
        assert_eq!(
            after.duration_ns,
            Some(before.duration_ns.unwrap().max(100_000))
        );
    }
    let reader = MkvStream::open(Cursor::new(bytes)).unwrap();
    assert!(reader.info().subtitle_streams().next().unwrap().forced);
}

#[test]
fn display_segment_pes_boundaries_do_not_change_the_bitmap_or_duration() {
    let data = display(true, 0);
    let mut parser = PgsParser::new();
    let mut pos = 0;
    while pos < data.len() {
        let size = 3 + usize::from(u16::from_be_bytes([data[pos + 1], data[pos + 2]]));
        let pts = if pos == 0 { Some(90_000) } else { None };
        assert!(
            parser
                .parse(&packet(data[pos..pos + size].to_vec(), pts))
                .is_empty()
        );
        pos += size;
    }
    let frames = parser.parse(&packet(clear(1), Some(270_000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, data);
    assert_eq!(frames[0].duration_ns, Some(2_000_000_000));
}

#[test]
fn empty_pes_does_not_flush_or_retime_a_pending_clear() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(pcs(0, false, 0), Some(90_000)));
    assert!(
        parser
            .parse(&packet(Vec::new(), Some(9_000_000)))
            .is_empty()
    );
    let frames = parser.parse(&packet(end(), None));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

#[test]
fn malformed_next_pcs_does_not_give_an_incomplete_clear_a_visible_duration() {
    for bad in [
        packet(vec![0x16, 0], Some(180_000)),
        packet(display(true, 1), None),
    ] {
        let mut parser = PgsParser::new();
        parser.parse(&packet(pcs(0, false, 0), Some(90_000)));
        let frames = parser.parse(&bad);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, pcs(0, false, 0));
        assert_eq!(frames[0].duration_ns, Some(0));
        assert!(parser.flush().is_empty());
    }
}

#[test]
fn pending_clear_memory_is_bounded_and_next_pcs_resynchronizes() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(pcs(0, false, 0), Some(90_000)));
    // A corrupt clear that keeps appending WDS-like bytes must remain bounded.
    for _ in 0..20 {
        parser.parse(&packet(vec![0x17; 256 * 1024], None));
    }
    assert!(parser.pending.as_ref().unwrap().1.len() <= MAX_PGS_PENDING_BYTES);
    let frames = parser.parse(&packet(clear(1), Some(180_000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[1].data, clear(1));
    assert!(parser.pending.is_none());
}

#[test]
fn duration_fallback_does_not_move_the_original_clear_timestamp() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(display(true, 0), Some(90_000)));
    let frames = parser.parse(&packet(clear(1), Some(41 * 90_000)));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
    assert_eq!(frames[1].pts_ns, 41_000_000_000);
    assert_eq!(frames[1].data, clear(1));
}

#[test]
fn negative_source_origin_keeps_relative_subtitle_timing() {
    let mut parser = PgsParser::new();
    parser.parse(&packet(display(true, 0), Some(-90_000)));
    let frames = parser.parse(&packet(clear(1), Some(180_000)));
    // pts_to_ns has sub-tick integer rounding for negative source timestamps.
    assert_eq!(frames[0].pts_ns, pts_to_ns(-90_000));
    assert_eq!(
        frames[0].duration_ns,
        Some((2_000_000_000 - pts_to_ns(-90_000)) as u64)
    );
    assert_eq!(frames[1].pts_ns, 2_000_000_000);
}

#[test]
fn dense_dialogue_keeps_every_clear_and_does_not_accumulate_between_cues() {
    let mut parser = PgsParser::new();
    let mut tracker = ForcedTracker::new();
    for cue in 0..100u16 {
        assert!(
            parser
                .parse(&packet(
                    display(false, cue * 2),
                    Some(i64::from(cue) * 270_000)
                ))
                .is_empty()
        );
        let frames = parser.parse(&packet(
            clear(cue * 2 + 1),
            Some(i64::from(cue) * 270_000 + 180_000),
        ));
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].duration_ns, Some(2_000_000_000));
        assert_eq!(frames[1].data, clear(cue * 2 + 1));
        assert!(parser.pending.is_none());
        for frame in frames {
            tracker.observe(&frame.data);
        }
    }
    assert_eq!(
        tracker.facts(),
        ForcedFacts {
            displays: 100,
            forced_displays: 0
        }
    );
    assert!(!tracker.is_forced());
}

#[test]
fn two_subtitle_tracks_keep_independent_clear_times_and_forced_flags_in_mkv() {
    let mut parsers = [PgsParser::new(), PgsParser::new()];
    let mut frames = Vec::new();
    for (track, start, stop, forced) in [(0, 0, 270_000, true), (1, 90_000, 450_000, false)] {
        parsers[track].parse(&packet(display(forced, 0), Some(start)));
        for frame in parsers[track].parse(&packet(clear(1), Some(stop))) {
            frames.push((track, frame));
        }
    }
    frames.sort_by_key(|(_, f)| f.pts_ns);
    let mut bytes = Cursor::new(Vec::new());
    let mut muxer = MkvMuxer::new(
        &mut bytes,
        &[subtitle_track(), subtitle_track()],
        None,
        0.0,
        &[],
    )
    .unwrap();
    for (track, frame) in &frames {
        muxer
            .write_frame(
                *track,
                frame.pts_ns,
                true,
                &frame.data,
                frame.duration_ns,
                None,
            )
            .unwrap();
    }
    muxer.finish().unwrap();
    let mut reader = MkvStream::open(Cursor::new(bytes.into_inner())).unwrap();
    assert_eq!(
        reader
            .info()
            .subtitle_streams()
            .map(|s| s.forced)
            .collect::<Vec<_>>(),
        [true, false]
    );
    for (track, expected) in frames {
        let actual = reader.read().unwrap().unwrap();
        assert_eq!(actual.track, track);
        assert_eq!(actual.pts, expected.pts_ns);
        assert_eq!(actual.data, expected.data);
    }
    assert!(reader.read().unwrap().is_none());
}

#[test]
fn pes_wire_roundtrip_preserves_clear_payload_and_zero_duration() {
    let original = sparse_sequence();
    for frame in original {
        let pes = PesFrame {
            discard_padding_ns: 0,
            track: 2,
            pts: frame.pts_ns,
            data: frame.data.clone(),
            keyframe: true,
            duration_ns: frame.duration_ns,
            source: None,
            coding: None,
        };
        let mut wire = Vec::new();
        pes.serialize(&mut wire).unwrap();
        let result = PesFrame::deserialize(&mut wire.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(result.track, 2);
        assert_eq!(result.pts, frame.pts_ns);
        assert_eq!(result.data, frame.data);
        assert_eq!(result.duration_ns, frame.duration_ns);
    }
}

fn run(command: &mut Command) -> std::process::Output {
    let output = command
        .output()
        .expect("install ffmpeg and ffprobe to run this test");
    assert!(
        output.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn probe(path: &Path, frames: bool) -> serde_json::Value {
    let output = run(Command::new("ffprobe")
        .args(["-v", "warning", "-of", "json"])
        .arg(if frames {
            "-show_frames"
        } else {
            "-show_packets"
        })
        .arg(path));
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
#[ignore = "requires ffmpeg and ffprobe; run explicitly in CI"]
fn ffmpeg_decodes_timed_empty_compositions() {
    let dir = crate::mux::interop_tests::fixture_dir("pgs-decode");
    let input = dir.path().join("forced.mkv");
    std::fs::write(&input, mux(&sparse_sequence())).unwrap();
    let json = probe(&input, true);
    let subtitles = json["frames"].as_array().unwrap();
    assert_eq!(subtitles.len(), 4, "{json}");
    for (index, (pts, rects)) in [(0.0, 1), (3.0, 0), (3600.0, 1), (3603.0, 0)]
        .iter()
        .enumerate()
    {
        assert_eq!(subtitles[index]["num_rects"], *rects, "{json}");
        assert_eq!(
            subtitles[index]["pts_time"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap(),
            *pts
        );
    }
}

#[test]
#[ignore = "requires ffmpeg and ffprobe; run explicitly in CI"]
fn ffmpeg_remux_keeps_all_subtitle_timestamps_without_warnings() {
    let dir = crate::mux::interop_tests::fixture_dir("pgs-remux");
    let input = dir.path().join("input.mkv");
    let output = dir.path().join("remux.mkv");
    std::fs::write(&input, mux(&sparse_sequence())).unwrap();
    let result = run(Command::new("ffmpeg")
        .args(["-nostdin", "-y", "-v", "warning", "-copyts", "-i"])
        .arg(&input)
        .args(["-map", "0", "-c", "copy"])
        .arg(&output));
    assert!(
        result.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let json = probe(&output, false);
    let packets = json["packets"].as_array().unwrap();
    assert_eq!(packets.len(), 4, "{json}");
    for (packet, pts) in packets.iter().zip([0.0, 3.0, 3600.0, 3603.0]) {
        assert_eq!(
            packet["pts_time"].as_str().unwrap().parse::<f64>().unwrap(),
            pts
        );
        assert!(!packet["flags"].as_str().unwrap().contains('C'), "{packet}");
    }
    // The clear commands must survive FFmpeg's automatic pgs_frame_merge BSF.
    let json = probe(&output, true);
    assert_eq!(json["frames"][1]["num_rects"], 0, "{json}");
    assert_eq!(json["frames"][3]["num_rects"], 0, "{json}");
}
