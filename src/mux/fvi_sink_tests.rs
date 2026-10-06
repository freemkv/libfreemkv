use super::*;
use crate::disc::{
    Codec, ColorSpace, ContentFormat, FrameRate, HdrFormat, Resolution, VideoStream,
};
use crate::mux::codec::PictureInfo;
use crate::mux::codec::coding::{CodingType, Mpeg2Coding};
use crate::mux::videomap::Medium;
use crate::pes::SourcePos;

fn mpeg2_title() -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Mpeg2,
        resolution: Resolution::R480i,
        frame_rate: FrameRate::F29_97,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Smpte170m,
        display_aspect: Some((16, 9)),
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.content_format = ContentFormat::MpegPs;
    t
}

fn hevc_title() -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R2160p,
        frame_rate: FrameRate::F23_976,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Bt2020,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.content_format = ContentFormat::BdTs;
    t
}

fn i_pic() -> PictureInfo {
    // Interlaced (tff) MPEG-2 I-frame picture.
    PictureInfo::mpeg2(
        CodingType::I,
        Mpeg2Coding {
            top_field_first: true,
            repeat_first_field: false,
            progressive_frame: false,
            progressive_sequence: false,
            frame_picture: true,
        },
    )
}

fn vframe(track: usize, coding: Option<PictureInfo>, source: Option<SourcePos>) -> PesFrame {
    let keyframe = coding.map(|c| c.keyframe()).unwrap_or(false);
    vframe_kf(track, coding, keyframe, source)
}

fn vframe_kf(
    track: usize,
    coding: Option<PictureInfo>,
    keyframe: bool,
    source: Option<SourcePos>,
) -> PesFrame {
    PesFrame {
        discard_padding_ns: 0,
        track,
        pts: 0,
        keyframe,
        data: vec![0u8; 4],
        duration_ns: None,
        source,
        coding,
    }
}

// Writer whose writes fail while `fail` is set.
struct Flaky(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Write for Flaky {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::ErrorKind::Other.into());
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_failed_finish_is_not_reported_ok_on_retry() {
    let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let w = Box::new(Flaky(fail.clone()));
    let mut sink = FviSink::with_writer(w, &mpeg2_title(), SourceInfo::default());
    assert!(sink.finish().is_err());
    assert!(
        sink.finish().is_err(),
        "second finish must not claim success"
    );
    fail.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(sink.finish().is_ok(), "a retry that flushes succeeds");
}

#[test]
fn record_numbers_stay_contiguous_across_interleaved_audio() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.fvi");
    let mut sink = FviSink::create(&path, &mpeg2_title(), SourceInfo::default()).unwrap();
    sink.write(&vframe(0, Some(i_pic()), None)).unwrap();
    sink.write(&vframe(1, None, None)).unwrap();
    sink.write(&vframe(0, Some(i_pic()), None)).unwrap();
    sink.finish().unwrap();
    let n: Vec<u64> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .skip(1)
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["n"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(n, [0, 1]);
}

// Known provenance (playlist, volume id) and picture count are carried into the header.
#[test]
fn header_carries_playlist_volume_id_and_picture_count_when_known() {
    let source = SourceInfo {
        playlist: "00800.mpls".into(),
        volume_id: "DISC_LABEL".into(),
        ..SourceInfo::default()
    };
    let mut header = MapHeader::from_title(&mpeg2_title(), source);
    header.picture_count = Some(7);
    let mut out = Vec::new();
    write_fvi_header(&mut out, &header).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["source"]["playlist"], "00800.mpls");
    assert_eq!(v["source"]["volume_id"], "DISC_LABEL");
    assert_eq!(v["picture_count"], 7);
}

#[test]
fn sink_writes_header_and_only_video_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("movie.fvi");
    let mut sink = FviSink::create(
        &path,
        &mpeg2_title(),
        SourceInfo {
            medium: Medium::Iso,
            path: "iso://m.iso".into(),
            title: 1,
            ..SourceInfo::default()
        },
    )
    .unwrap();
    // Video frame on track 0 → indexed. Offset 2148 = sector 1, byte 100
    // within that sector (exercises the within-sector `src.byte`, §9).
    sink.write(&vframe(0, Some(i_pic()), Some(SourcePos::at_byte(2148))))
        .unwrap();
    // Audio frame on a non-video track → ignored.
    sink.write(&vframe(7, None, Some(SourcePos::at_byte(9999))))
        .unwrap();
    sink.finish().unwrap();

    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 2, "header + one video record only");

    let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(header["format"], "freemkv/video-index");
    assert_eq!(header["fvi_version"], 1);
    assert_eq!(header["stream"]["dar"], serde_json::json!([16, 9])); // anamorphic
    assert_eq!(header["stream"]["scan"], "interlaced"); // 480i
    assert_eq!(header["stream"]["codec"], "mpeg2video");
    assert_eq!(header["timescale"], 1_000_000_000u64);
    // The provenance is carried through verbatim: the caller's medium, not a
    // default, and the caller's title index.
    assert_eq!(header["source"]["title"], 1);
    assert_eq!(header["source"]["medium"], "iso");
    assert_eq!(header["source"]["path"], "iso://m.iso");

    let rec: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(rec["n"], 0);
    assert_eq!(rec["type"], "I");
    assert_eq!(rec["key"], true); // I-picture (frame keyframe) → random-access
    // Interlaced tff frame → field_order "tff", progressive false, 2 fields.
    assert_eq!(rec["field_order"], "tff");
    assert_eq!(rec["progressive"], false);
    assert_eq!(rec["nb_fields"], 2);
    assert_eq!(rec["pts"], 0);
    assert_eq!(rec["src"]["sector"], 1);
    assert_eq!(rec["src"]["byte"], 100); // 2148 % 2048 → within-sector (§9)
    assert!(rec.get("dts").is_none(), "no DTS on a frame → omitted");
    assert!(
        rec.get("gop").is_none(),
        "no GOP-closure signal → gop omitted"
    );
}

#[test]
fn secondary_video_stream_is_not_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pip.fvi");
    let mut title = mpeg2_title();
    let mut second = title.streams[0].clone();
    if let DiscStream::Video(v) = &mut second {
        v.pid = 0x1012;
        v.secondary = true;
    }
    title.streams.push(second);
    let mut sink = FviSink::create(&path, &title, SourceInfo::default()).unwrap();
    sink.write(&vframe(0, Some(i_pic()), Some(SourcePos::at_byte(0))))
        .unwrap();
    sink.write(&vframe(1, Some(i_pic()), Some(SourcePos::at_byte(2048))))
        .unwrap();
    sink.finish().unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text.lines().count(), 2, "header + primary record only");
}

#[test]
fn empty_title_still_emits_valid_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.fvi");
    let mut sink = FviSink::create(&path, &mpeg2_title(), SourceInfo::default()).unwrap();
    sink.finish().unwrap(); // no frames
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text.lines().count(), 1, "header only");
    let header: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(header["format"], "freemkv/video-index");
}

#[test]
fn extension_jsonl_is_still_json_lines() {
    // Output is always JSON Lines regardless of extension (one format today).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idx.jsonl");
    let mut sink = FviSink::create(&path, &mpeg2_title(), SourceInfo::default()).unwrap();
    sink.write(&vframe(0, Some(i_pic()), None)).unwrap();
    sink.finish().unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    // null src on a provenance-absent frame.
    let rec: serde_json::Value = serde_json::from_str(text.lines().nth(1).unwrap()).unwrap();
    assert_eq!(rec["src"], serde_json::Value::Null);
}

#[test]
fn codec_agnostic_non_mpeg2_records() {
    // A non-MPEG2 stream (coding None) whose parser sets keyframe must still
    // produce USEFUL records: key/type from the frame keyframe flag, src +
    // pts populated, and NO mpeg2-only field members.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("uhd.fvi");
    let mut sink = FviSink::create(
        &path,
        &hevc_title(),
        SourceInfo {
            medium: Medium::Disc,
            path: "disc://".into(),
            ..SourceInfo::default()
        },
    )
    .unwrap();
    // HEVC IDR (keyframe) with real provenance.
    sink.write(&vframe_kf(0, None, true, Some(SourcePos::at_byte(12288))))
        .unwrap();
    // Non-key HEVC picture.
    sink.write(&vframe_kf(0, None, false, Some(SourcePos::at_byte(20480))))
        .unwrap();
    sink.finish().unwrap();

    let text = std::fs::read_to_string(&path).unwrap();
    let recs: Vec<serde_json::Value> = text
        .lines()
        .skip(1)
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(recs[0]["key"], true, "HEVC IDR → key from frame.keyframe");
    assert_eq!(recs[0]["type"], "I");
    assert_eq!(recs[0]["src"]["sector"], 6); // 12288 / 2048
    assert!(
        recs[0].get("field_order").is_none() && recs[0].get("nb_fields").is_none(),
        "coding-absent frame omits field_order/progressive/nb_fields"
    );
    assert_eq!(recs[1]["key"], false);
    assert_eq!(recs[1]["type"], "P");
    assert_eq!(recs[1]["src"]["sector"], 10); // 20480 / 2048
}
