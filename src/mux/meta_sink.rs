//! `chapters://` and `json://` metadata sinks.
//!
//! Both ignore the PES stream entirely: everything they emit is already known
//! from the [`DiscTitle`] at construction, so each writes its whole file at
//! `create()` and treats every `write()` frame as a no-op. They are wired
//! through [`super::resolve::output`] like the other write-only sinks; the
//! ISO/disc scan that builds the title is all they need.

use crate::disc::{Chapter, DiscTitle, Stream as DiscStream};
use crate::pes::{PesFrame, Stream};
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

// ── chapters:// ──────────────────────────────────────────────────────────────

/// `HH:MM:SS.mmm` for a WebVTT cue timestamp.
fn vtt_time(secs: f64) -> String {
    let total_ms = (secs.max(0.0) * 1000.0).round() as u64;
    let ms = total_ms % 1000;
    let total_s = total_ms / 1000;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        total_s / 3600,
        (total_s / 60) % 60,
        total_s % 60,
        ms
    )
}

/// WebVTT chapter cues (`.vtt`). Each chapter spans until the next one starts
/// (the last runs to its own start — length is unknown without the title tail).
fn chapters_vtt(chapters: &[Chapter]) -> String {
    let mut s = String::from("WEBVTT\n\n");
    for (i, c) in chapters.iter().enumerate() {
        let start = c.time_secs.max(0.0);
        let end = chapters
            .get(i + 1)
            .map(|n| n.time_secs.max(0.0))
            .unwrap_or(start);
        s.push_str(&format!(
            "{}\n{} --> {}\nChapter {}\n\n",
            i + 1,
            vtt_time(start),
            vtt_time(end),
            c.name
        ));
    }
    s
}

/// Chapter content in the format the output extension selects: `.txt`/`.ogm`
/// (OGM simple), `.vtt` (WebVTT), else Matroska XML (`.xml` / default).
pub(crate) fn chapters_content(chapters: &[Chapter], ext: Option<&str>) -> String {
    match ext.map(|e| e.to_ascii_lowercase()).as_deref() {
        Some("txt") | Some("ogm") => super::demux_sink::chapters_ogm(chapters),
        Some("vtt") => chapters_vtt(chapters),
        _ => super::demux_sink::chapters_xml(chapters),
    }
}

/// `chapters://` sink: writes the title's chapter markers at construction; the
/// PES stream is ignored.
pub struct ChaptersSink {
    title: DiscTitle,
}

impl ChaptersSink {
    pub fn create(path: &Path, title: &DiscTitle) -> io::Result<Self> {
        let ext = path.extension().and_then(|e| e.to_str());
        let content = chapters_content(&title.chapters, ext);
        File::create(path)?.write_all(content.as_bytes())?;
        Ok(Self {
            title: title.clone(),
        })
    }
}

impl Stream for ChaptersSink {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(crate::error::Error::StreamWriteOnly.into())
    }
    fn write(&mut self, _frame: &PesFrame) -> io::Result<()> {
        Ok(()) // whole file written at create()
    }
    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

// ── json:// ──────────────────────────────────────────────────────────────────

/// Serialization id for an audio stream's editorial purpose.
fn purpose_id(p: crate::labels::LabelPurpose) -> &'static str {
    use crate::labels::LabelPurpose::*;
    match p {
        Normal => "normal",
        Commentary => "commentary",
        Descriptive => "descriptive",
        Score => "score",
        Ime => "ime",
    }
}

/// Serialization id for a subtitle stream's qualifier.
fn qualifier_id(q: crate::labels::LabelQualifier) -> &'static str {
    use crate::labels::LabelQualifier::*;
    match q {
        None => "none",
        Sdh => "sdh",
        DescriptiveService => "descriptive_service",
        Forced => "forced",
    }
}

/// One stream as JSON — every field the scan resolved, nothing dropped. This is
/// the complete per-stream model (`disc::Stream`), not a summary: consumers get
/// resolution/HDR/aspect for video, channels/sample-rate/purpose for audio, and
/// the qualifier for subtitles, all in machine-readable form.
fn stream_json(s: &DiscStream) -> serde_json::Value {
    use super::demux_sink::codec_label;
    use serde_json::json;
    match s {
        DiscStream::Video(v) => {
            let (w, h) = v.resolution.pixels();
            let (fps_num, fps_den) = v.frame_rate.as_fraction();
            let mut o = json!({
                "kind": "video",
                "codec": codec_label(v.codec),
                "pid": v.pid,
                "resolution": v.resolution.to_string(),
                "width": w,
                "height": h,
                "interlaced": v.resolution.is_interlaced(),
                "frame_rate": v.frame_rate.to_string(),
                "frame_rate_num": fps_num,
                "frame_rate_den": fps_den,
                "hdr": v.hdr.id(),
                "color_space": v.color_space.id(),
                "secondary": v.secondary,
                "mvc_dependent": v.is_mvc_dependent(),
            });
            if let Some((num, den)) = v.display_aspect {
                o["display_aspect"] = json!(format!("{num}:{den}"));
            }
            if let Some(c) = v.measured_cicp {
                o["measured_cicp"] = json!({
                    "matrix": c.matrix,
                    "transfer": c.transfer,
                    "primaries": c.primaries,
                    "range": c.range,
                });
            }
            if !v.label.is_empty() {
                o["label"] = json!(v.label);
            }
            o
        }
        DiscStream::Audio(a) => {
            let mut o = json!({
                "kind": "audio",
                "codec": codec_label(a.codec),
                "pid": a.pid,
                "language": a.language,
                "channels": a.channels.to_string(),
                "channel_count": a.channels.count(),
                "sample_rate": a.sample_rate.to_string(),
                "sample_rate_hz": a.sample_rate.hz(),
                "secondary": a.secondary,
                "purpose": purpose_id(a.purpose),
            });
            if !a.label.is_empty() {
                o["label"] = json!(a.label);
            }
            o
        }
        DiscStream::Subtitle(t) => json!({
            "kind": "subtitle",
            "codec": codec_label(t.codec),
            "pid": t.pid,
            "language": t.language,
            "forced": t.forced,
            "qualifier": qualifier_id(t.qualifier),
        }),
    }
}

/// The `json://` document for one title: identity, duration/size, its clips,
/// its complete stream models, and its chapter points. A stable, machine-
/// readable view of one title — the same information the scan resolved, no loss.
pub(crate) fn title_json(title: &DiscTitle) -> serde_json::Value {
    use serde_json::json;
    let streams: Vec<_> = title.streams.iter().map(stream_json).collect();
    let clips: Vec<_> = title
        .clips
        .iter()
        .map(|c| {
            json!({
                "clip_id": c.clip_id,
                "duration_secs": c.duration_secs,
                "source_packets": c.source_packets,
            })
        })
        .collect();
    let chapters: Vec<_> = title
        .chapters
        .iter()
        .enumerate()
        .map(|(i, c)| json!({ "n": i + 1, "start_secs": c.time_secs, "name": c.name }))
        .collect();
    json!({
        "playlist": title.playlist,
        "playlist_id": title.playlist_id,
        "duration_secs": title.duration_secs,
        "size_bytes": title.size_bytes,
        "format": format!("{:?}", title.content_format),
        "clips": clips,
        "streams": streams,
        "chapters": chapters,
    })
}

/// `json://` sink: writes the title's structured metadata at construction; the
/// PES stream is ignored.
pub struct JsonSink {
    title: DiscTitle,
}

impl JsonSink {
    pub fn create(path: &Path, title: &DiscTitle) -> io::Result<Self> {
        let doc =
            serde_json::to_string_pretty(&title_json(title)).unwrap_or_else(|_| "{}".to_string());
        let mut f = File::create(path)?;
        f.write_all(doc.as_bytes())?;
        f.write_all(b"\n")?;
        Ok(Self {
            title: title.clone(),
        })
    }
}

impl Stream for JsonSink {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(crate::error::Error::StreamWriteOnly.into())
    }
    fn write(&mut self, _frame: &PesFrame) -> io::Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::Chapter;

    fn chaps() -> Vec<Chapter> {
        vec![
            Chapter {
                time_secs: 0.0,
                name: "1".into(),
            },
            Chapter {
                time_secs: 62.5,
                name: "2".into(),
            },
        ]
    }

    #[test]
    fn chapters_format_selected_by_extension() {
        let xml = chapters_content(&chaps(), Some("xml"));
        assert!(xml.contains("<Chapters>"), "xml chosen for .xml");
        let ogm = chapters_content(&chaps(), Some("txt"));
        assert!(ogm.contains("CHAPTER01="), "ogm chosen for .txt");
        let vtt = chapters_content(&chaps(), Some("vtt"));
        assert!(
            vtt.starts_with("WEBVTT") && vtt.contains("00:01:02.500"),
            "vtt chosen for .vtt, with cue timing"
        );
        // Unknown / missing extension defaults to XML.
        assert!(chapters_content(&chaps(), None).contains("<Chapters>"));
    }

    #[test]
    fn title_json_carries_streams_and_chapters() {
        use crate::disc::{AudioChannels, AudioStream, Codec, DiscTitle};
        use crate::disc::{LabelPurpose, SampleRate, Stream as DiscStream};
        let mut t = DiscTitle::empty();
        t.playlist = "MAIN".into();
        t.chapters = chaps();
        t.streams = vec![DiscStream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::TrueHd,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })];
        let v = title_json(&t);
        assert_eq!(v["playlist"], "MAIN");
        let a = &v["streams"][0];
        assert_eq!(a["kind"], "audio");
        assert_eq!(a["codec"], "TrueHD");
        assert_eq!(a["language"], "eng");
        // Completeness: audio carries channels + sample rate + purpose, not just codec.
        assert_eq!(a["channels"], "stereo");
        assert_eq!(a["channel_count"], 2);
        assert_eq!(a["sample_rate"], "48kHz");
        assert_eq!(a["sample_rate_hz"], 48000.0);
        assert_eq!(a["purpose"], "normal");
        assert_eq!(v["chapters"][1]["n"], 2);
        assert_eq!(v["chapters"][1]["start_secs"], 62.5);
        assert_eq!(v["chapters"][1]["name"], "2");
    }

    #[test]
    fn video_json_carries_resolution_and_hdr() {
        use crate::disc::Codec;
        use crate::disc::{
            ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStream,
            VideoStream,
        };
        let mut t = DiscTitle::empty();
        t.streams = vec![DiscStream::Video(VideoStream {
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
        })];
        let vid = &title_json(&t)["streams"][0];
        assert_eq!(vid["kind"], "video");
        assert_eq!(vid["resolution"], "2160p");
        assert_eq!(vid["width"], 3840);
        assert_eq!(vid["height"], 2160);
        assert_eq!(vid["frame_rate"], "23.976");
        assert_eq!(vid["frame_rate_num"], 24000);
        assert_eq!(vid["hdr"], "hdr10");
        assert_eq!(vid["color_space"], "bt2020");
    }
}
