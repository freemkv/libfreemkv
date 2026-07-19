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

fn stream_json(s: &DiscStream) -> serde_json::Value {
    use super::demux_sink::codec_label;
    use serde_json::json;
    match s {
        DiscStream::Video(v) => json!({
            "kind": "video",
            "codec": codec_label(v.codec),
            "pid": v.pid,
        }),
        DiscStream::Audio(a) => json!({
            "kind": "audio",
            "codec": codec_label(a.codec),
            "language": a.language,
            "pid": a.pid,
        }),
        DiscStream::Subtitle(t) => json!({
            "kind": "subtitle",
            "codec": codec_label(t.codec),
            "language": t.language,
            "forced": t.forced,
            "pid": t.pid,
        }),
    }
}

/// The `json://` document for one title: identity, duration/size, its streams,
/// and its chapter points. A stable, machine-readable view of one title.
pub(crate) fn title_json(title: &DiscTitle) -> serde_json::Value {
    use serde_json::json;
    let streams: Vec<_> = title.streams.iter().map(stream_json).collect();
    let chapters: Vec<_> = title
        .chapters
        .iter()
        .enumerate()
        .map(|(i, c)| json!({ "n": i + 1, "start_secs": c.time_secs }))
        .collect();
    json!({
        "playlist": title.playlist,
        "playlist_id": title.playlist_id,
        "duration_secs": title.duration_secs,
        "size_bytes": title.size_bytes,
        "format": format!("{:?}", title.content_format),
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
        assert_eq!(v["streams"][0]["kind"], "audio");
        assert_eq!(v["streams"][0]["codec"], "TrueHD");
        assert_eq!(v["streams"][0]["language"], "eng");
        assert_eq!(v["chapters"][1]["n"], 2);
        assert_eq!(v["chapters"][1]["start_secs"], 62.5);
    }
}
