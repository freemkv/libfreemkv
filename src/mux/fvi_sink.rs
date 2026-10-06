//! `fvi://` sink — freemkv's own native video-index output.
//!
//! A [`crate::pes::PesSink`] that emits one machine-readable
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
#[path = "fvi_sink_tests.rs"]
mod tests;
