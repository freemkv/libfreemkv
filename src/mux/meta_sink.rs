//! `chapters://` and `json://` metadata sinks.
//!
//! Both ignore the PES stream entirely: everything they emit is already known
//! from the [`DiscTitle`] at construction, so each writes its whole file at
//! `create()` and treats every `write()` frame as a no-op. They are wired
//! through [`super::resolve::output`] like the other write-only sinks; the
//! ISO/disc scan that builds the title is all they need.

use crate::disc::{Chapter, DiscTitle, TitleProfile};
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
        // Each cue runs until the next chapter. WebVTT drops a cue whose end is not
        // strictly after its start, so the last chapter (and any degenerate
        // equal-timestamp pair) gets a 1 s minimum duration rather than being lost.
        let end = chapters
            .get(i + 1)
            .map(|n| n.time_secs.max(0.0))
            .filter(|&e| e > start)
            .unwrap_or(start + 1.0);
        // No localized prose in the library (see Chapter::name): emit the bare
        // name, or a plain ordinal when unnamed — the app prepends any "Chapter "
        // prefix in the user's language. Matches chapters_xml / chapters_ogm.
        let name = if c.name.is_empty() {
            (i + 1).to_string()
        } else {
            c.name.clone()
        };
        s.push_str(&format!(
            "{}\n{} --> {}\n{}\n\n",
            i + 1,
            vtt_time(start),
            vtt_time(end),
            name
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

/// The `json://` document for one title.
///
/// SINGLE SERIALIZATION SOURCE OF TRUTH: the per-title / per-stream schema comes
/// from the normalized [`TitleProfile`] (the same typed view `Disc::profile`
/// exposes), serialized directly — `json://` and any downstream consumer share
/// one schema rather than a hand-built one that can drift.
///
/// SCHEMA NOTE. This replaces the previous hand-built `streams` array. The
/// stable editorial meaning is preserved, but the shape is normalized: the one
/// `streams` array (tagged with `kind`) becomes three typed arrays (`video` /
/// `audio` / `subtitles`); subtitle `qualifier` becomes the booleans `forced` +
/// `sdh`; audio `purpose` becomes `commentary` + `descriptive`; and per-track
/// `default` is now hoisted (first non-secondary video/audio). Per-stream fields
/// the normalized profile intentionally omits (`pid`, `width`/`height`,
/// `color_space`, `measured_cicp`, `sample_rate`, `channel_count`,
/// `mvc_dependent`) are no longer emitted; they live on the richer scan model,
/// not on this format-agnostic view.
///
/// `json://`-only extras that are NOT part of the normalized profile — the
/// title's `clips` and the full `chapter_marks` list — are spliced back on so
/// those consumers do not regress. (`chapters` from the profile is the marker
/// COUNT; the full list is under `chapter_marks`.) `json://` is a per-title sink
/// with no disc context, so `index` is `0` and `is_main` is `false`; the
/// disc-level [`crate::disc::DiscProfile`] populates those correctly.
pub(crate) fn title_json(title: &DiscTitle) -> serde_json::Value {
    use serde_json::json;
    let profile = TitleProfile::from_title(title, 0, false);
    // Serializing a plain-scalar struct is infallible; fall back to an empty
    // object rather than panic if that ever changes (the create() path then
    // surfaces the empty doc as a NoMetadata error).
    let mut doc = serde_json::to_value(&profile).unwrap_or_else(|_| json!({}));
    doc["format"] = json!(format!("{:?}", title.content_format));
    doc["playlist_id"] = json!(title.playlist_id);
    doc["clips"] = json!(
        title
            .clips
            .iter()
            .map(|c| json!({
                "clip_id": c.clip_id,
                "duration_secs": c.duration_secs,
                "source_packets": c.source_packets,
            }))
            .collect::<Vec<_>>()
    );
    doc["chapter_marks"] = json!(
        title
            .chapters
            .iter()
            .enumerate()
            .map(|(i, c)| json!({ "n": i + 1, "start_secs": c.time_secs, "name": c.name }))
            .collect::<Vec<_>>()
    );
    doc
}

/// `json://` sink: writes the title's structured metadata at construction; the
/// PES stream is ignored.
pub struct JsonSink {
    title: DiscTitle,
}

impl JsonSink {
    pub fn create(path: &Path, title: &DiscTitle) -> io::Result<Self> {
        // Infallible in practice, but propagate rather than silently write "{}".
        // `NoMetadata` (E9008), not `MkvInvalid` — the latter's skippable-stub
        // ruling would wrongly swallow a real encode failure as an empty nav stub.
        let doc = serde_json::to_string_pretty(&title_json(title))
            .map_err(|_| crate::error::Error::NoMetadata)?;
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
        // Normalized profile schema: streams split into typed arrays.
        let a = &v["audio"][0];
        assert_eq!(a["codec"], "truehd");
        assert_eq!(a["language"], "eng");
        assert_eq!(a["channels"], "stereo");
        // Editorial purpose is decomposed into booleans.
        assert!(!a["commentary"].as_bool().unwrap());
        assert!(!a["descriptive"].as_bool().unwrap());
        // First non-secondary audio is the hoisted default.
        assert!(a["default"].as_bool().unwrap());
        // Chapter COUNT from the profile; full list under chapter_marks.
        assert_eq!(v["chapters"], 2);
        assert_eq!(v["chapter_marks"][1]["n"], 2);
        assert_eq!(v["chapter_marks"][1]["start_secs"], 62.5);
        assert_eq!(v["chapter_marks"][1]["name"], "2");
    }

    /// An audio stream whose channel layout is genuinely unknown reports the
    /// honest `"unknown"` string (the normalized profile carries the layout label,
    /// not a fabricated numeric channel count).
    #[test]
    fn unknown_audio_layout_reports_unknown_channels() {
        use crate::disc::{AudioChannels, AudioStream, Codec, DiscTitle};
        use crate::disc::{LabelPurpose, SampleRate, Stream as DiscStream};
        let mut t = DiscTitle::empty();
        t.streams = vec![DiscStream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::DtsHdMa,
            channels: AudioChannels::Unknown,
            language: "eng".into(),
            sample_rate: SampleRate::Unknown,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })];
        let v = title_json(&t);
        let a = &v["audio"][0];
        // Unknown layout must serialize as the string "unknown" — never a
        // fabricated numeric layout. (The schema has no separate channel_count
        // field; `channels` is the only channel signal, so this is the guard.)
        assert_eq!(a["channels"], "unknown");
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
        let vid = &title_json(&t)["video"][0];
        assert_eq!(vid["codec"], "hevc");
        assert_eq!(vid["resolution"], "2160p");
        assert_eq!(vid["frame_rate"], "23.976");
        assert_eq!(vid["hdr"], "hdr10");
        assert!(vid["default"].as_bool().unwrap());
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("fmkv_meta_sink_{}_{n}_{name}", std::process::id()))
    }

    fn sink_title() -> crate::disc::DiscTitle {
        let mut t = crate::disc::DiscTitle::empty();
        t.playlist = "MAIN".into();
        t.chapters = chaps();
        t
    }

    /// `chapters://` and `json://` are WRITE-ONLY sinks: the whole file is
    /// emitted at `create()` and there is nothing to demux back. `read()`
    /// returning `Ok(None)` instead of the write-only error makes a caller that
    /// pointed a mux INPUT at one of these URLs see a clean empty stream — the
    /// exact shape of the shipped "empty title, exit code 0" defect. It must
    /// refuse with the numeric code `E_STREAM_WRITE_ONLY`.
    #[test]
    fn metadata_sinks_refuse_to_be_read_from() {
        let code = format!("E{}", crate::error::Error::StreamWriteOnly.code());

        let cpath = temp_path("chapters.xml");
        let mut c = ChaptersSink::create(&cpath, &sink_title()).unwrap();
        let err = c
            .read()
            .expect_err("chapters:// is write-only; read must not report a clean EOF");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(
            err.to_string().contains(&code),
            "expected {code}, got {err}"
        );
        let _ = std::fs::remove_file(&cpath);

        let jpath = temp_path("meta.json");
        let mut j = JsonSink::create(&jpath, &sink_title()).unwrap();
        let err = j
            .read()
            .expect_err("json:// is write-only; read must not report a clean EOF");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(
            err.to_string().contains(&code),
            "expected {code}, got {err}"
        );
        let _ = std::fs::remove_file(&jpath);
    }
}
