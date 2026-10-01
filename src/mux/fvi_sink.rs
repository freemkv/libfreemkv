//! `fvi://` sink — freemkv's own native video-index output.
//!
//! A write-only [`crate::pes::Stream`] that emits one machine-readable
//! *video-index* record per coded picture of the title's primary video
//! track, instead of muxing frames into a container.
//!
//! On-disk shape: the freemkv FVI format — JSON Lines, a header object on line 1, then one
//! record per picture. The sink is purely additive: it does NOT touch the MKV mux path.

use crate::disc::{DiscTitle, Stream as DiscStream};
use crate::mux::videomap::{
    FVI_FORMAT, FVI_GENERATOR, FVI_SECTOR_SIZE, FVI_TIMESCALE, FVI_VERSION, MapHeader,
    PictureRecord, SourceInfo, field_order_label, is_random_access, type_label,
};
use crate::pes::{PesFrame, PesSink};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

/// Write the FVI header row into `w`.
fn write_fvi_header(w: &mut dyn Write, h: &MapHeader) -> io::Result<()> {
    let mut source = serde_json::json!({
        "medium": h.source.medium.as_str(),
        "path": h.source.path,
        "title": h.source.title,
        "sector_size": FVI_SECTOR_SIZE,
    });
    // playlist / volume_id are MAY — emit only when known.
    if !h.source.playlist.is_empty() {
        source["playlist"] = serde_json::Value::String(h.source.playlist.clone());
    }
    if !h.source.volume_id.is_empty() {
        source["volume_id"] = serde_json::Value::String(h.source.volume_id.clone());
    }

    let mut obj = serde_json::json!({
        "format": FVI_FORMAT,
        "fvi_version": FVI_VERSION,
        "generator": FVI_GENERATOR,
        "stream": {
            "codec": h.stream.codec,
            "width": h.stream.width,
            "height": h.stream.height,
            "dar": [h.stream.dar.0, h.stream.dar.1],
            "frame_rate": [h.stream.frame_rate.0, h.stream.frame_rate.1],
            "scan": h.stream.scan.as_str(),
            "colour": {
                "primaries": h.stream.colour.primaries,
                "transfer": h.stream.colour.transfer,
                "matrix": h.stream.colour.matrix,
                "range": if h.stream.colour.full_range { "full" } else { "limited" },
            },
        },
        "source": source,
        "timescale": FVI_TIMESCALE,
    });
    // picture_count is MAY — omitted when streaming (unknown at header time).
    if let Some(pc) = h.picture_count {
        obj["picture_count"] = serde_json::json!(pc);
    }
    serde_json::to_writer(&mut *w, &obj)?;
    w.write_all(b"\n")
}

// Write one FVI per-picture record. `type`/`key` are always emitted; coding-derived members are
// emitted only when the codec measured them — an honest absence, never a guessed default.
fn write_fvi_record(w: &mut dyn Write, r: &PictureRecord) -> io::Result<()> {
    // `src` is REQUIRED (Appendix A); null when provenance absent = "position
    // unknown". Per §9, `src.byte` is the offset WITHIN the sector, so reduce the
    // absolute `SourcePos.byte` modulo sector size; `sector` is the whole count.
    let src = match r.source {
        Some(s) => serde_json::json!({
            "sector": s.sector,
            "byte": s.byte % u64::from(FVI_SECTOR_SIZE),
        }),
        None => serde_json::Value::Null,
    };

    let mut obj = serde_json::json!({
        "n": r.n,
        "src": src,
        "type": type_label(r.coding, r.keyframe),
        "key": is_random_access(r.coding, r.keyframe),
    });

    // pts is SHOULD — emit when present.
    if let Some(pts) = r.pts_ns {
        obj["pts"] = serde_json::json!(pts);
    }
    // dts: MAY, always omitted.

    if let Some(c) = r.coding {
        if let Some(fo) = field_order_label(r.coding) {
            obj["field_order"] = serde_json::json!(fo);
        }
        if let Some(prog) = c.progressive() {
            obj["progressive"] = serde_json::json!(prog);
        }
        obj["nb_fields"] = serde_json::json!(c.nb_fields());
    }

    serde_json::to_writer(&mut *w, &obj)?;
    w.write_all(b"\n")
}

/// `fvi://` sink: streams the title's primary-video per-picture index to a
/// `.fvi` (or `.jsonl` / `.json`) file as JSON Lines.
pub struct FviSink {
    title: DiscTitle,
    /// Index of the title's primary video track — only frames on this track are
    /// indexed; audio / subtitle / secondary-video frames are ignored.
    video_track: Option<usize>,
    /// The sink owns the destination (a file outside tests).
    w: BufWriter<Box<dyn Write + Send>>,
    /// The header row, written lazily on the first `write`/`finish` so an
    /// empty / audio-only title still emits a valid single-line file.
    header: MapHeader,
    /// 0-based picture counter (the record `n`), incremented per indexed frame.
    next_n: u64,
    header_written: bool,
    finished: bool,
}

impl FviSink {
    /// Create the sink at `path`, assembling the header from `title`'s primary
    /// video stream.
    ///
    /// `source` records where the index was built FROM — the input medium, URL, title index and
    /// (when known) playlist / volume id. It is carried verbatim into the header's `source`
    /// object, which describes the INPUT, never `path` (the destination this sink writes). A
    /// caller with no provenance to declare passes `SourceInfo::default()`; the empty members
    /// are then omitted from the header rather than guessed.
    pub fn create(path: &Path, title: &DiscTitle, source: SourceInfo) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self::with_writer(Box::new(file), title, source))
    }

    fn with_writer(w: Box<dyn Write + Send>, title: &DiscTitle, source: SourceInfo) -> Self {
        let video_track = title
            .streams
            .iter()
            .position(|s| matches!(s, DiscStream::Video(_)));
        let header = MapHeader::from_title(title, source);

        Self {
            title: title.clone(),
            video_track,
            w: BufWriter::new(w),
            header,
            next_n: 0,
            header_written: false,
            finished: false,
        }
    }

    /// Write the header row once, lazily.
    fn ensure_header(&mut self) -> io::Result<()> {
        if self.header_written {
            return Ok(());
        }
        write_fvi_header(&mut self.w, &self.header)?;
        self.header_written = true;
        Ok(())
    }
}

impl PesSink for FviSink {
    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        // Only index pictures of the primary video track. Audio / subtitle /
        // secondary-video frames carry no PictureInfo and are not part of the
        // video index.
        if Some(frame.track) != self.video_track {
            return Ok(());
        }
        self.ensure_header()?;
        let rec = PictureRecord {
            n: self.next_n,
            coding: frame.coding,
            keyframe: frame.keyframe,
            pts_ns: Some(frame.pts),
            source: frame.source,
        };
        write_fvi_record(&mut self.w, &rec)?;
        self.next_n += 1;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        // Emit the header even for a title that produced no records, so the
        // output is always a valid (if record-less) `.fvi` file. JSON Lines has
        // no footer. Finished only once flushed, so a failed flush is not retried as Ok.
        self.ensure_header()?;
        self.w.flush()?;
        self.finished = true;
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

#[cfg(test)]
mod tests {
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
}
