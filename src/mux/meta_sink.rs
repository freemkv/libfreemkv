//! `chapters://` and `json://` metadata sinks.
//!
//! Both ignore the PES stream entirely: everything they emit is already known
//! from the [`DiscTitle`] at construction, so each writes its whole file at
//! `create()` and treats every `write()` frame as a no-op. They are wired
//! through [`super::resolve::output`] like the other write-only sinks; the
//! ISO/disc scan that builds the title is all they need.

use crate::disc::{Chapter, DiscTitle, TitleProfile};
use crate::pes::{PesFrame, PesSink};
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

/// One-line WebVTT cue text: `&`, `<`, `>` escaped (so no `-->`), line breaks collapsed.
fn vtt_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' | '\n' => out.push(' '),
            c => out.push(c),
        }
    }
    out
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
            vtt_text(&c.name)
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

impl PesSink for ChaptersSink {
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

// The `json://` document for one title, built from [`TitleProfile`].
pub(crate) fn title_json(title: &DiscTitle) -> serde_json::Value {
    use serde_json::json;
    // `index`/`is_main` are disc-level (title's position in the sorted list, whether
    // it's the selected main feature). A `json://` sink sees ONE `DiscTitle` with no
    // disc view, so both are fixed `0`/`false`; authoritative values live on `DiscProfile::from_disc`.
    let profile = TitleProfile::from_title(title, 0, false);
    // Serializing a plain-scalar struct is infallible; fall back to an empty
    // object rather than panic if that ever changes (the create() path then
    // surfaces the empty doc as a NoMetadata error).
    let mut doc = serde_json::to_value(&profile).unwrap_or_else(|_| json!({}));
    // Restore per-stream detail the flat `TitleProfile` VIEW omits (pid, colour
    // signalling, sample rate, raw purpose/qualifier, etc.). Rather than widen that
    // shared type, enrich straight from the source Title, in `from_title`'s kind order.
    enrich_streams(&mut doc, title);
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

// Every rate a stream carries, in Hz: a BD combo (48/96, 48/192) lists both,
// Unknown is empty.
fn sample_rates(rate: crate::disc::SampleRate) -> Vec<u32> {
    use crate::disc::SampleRate::*;
    match rate {
        S48_96 => vec![48_000, 96_000],
        S48_192 => vec![48_000, 192_000],
        Unknown => Vec::new(),
        r => vec![r.hz() as u32],
    }
}

/// Compact id for an audio stream's editorial purpose (no localized prose —
/// the app maps it to display text). Mirrors the `Codec::id()` convention.
pub(crate) fn purpose_id(p: crate::disc::LabelPurpose) -> &'static str {
    use crate::disc::LabelPurpose::*;
    match p {
        Normal => "normal",
        Commentary => "commentary",
        Descriptive => "descriptive",
        Score => "score",
        Ime => "ime",
    }
}

// `doc[key][i]` as a mutable object, or `dummy` (reset to an object) if absent.
fn entry<'a>(
    doc: &'a mut serde_json::Value,
    key: &str,
    i: usize,
    dummy: &'a mut serde_json::Value,
) -> &'a mut serde_json::Value {
    match doc.get_mut(key).and_then(|v| v.get_mut(i)) {
        Some(e) if e.is_object() => e,
        _ => {
            *dummy = serde_json::json!({});
            dummy
        }
    }
}

/// Merge the per-stream fields `TitleProfile` drops back into the `json://`
/// document, straight from the source Title. Walks `title.streams` in declared
/// order with a per-kind cursor so each raw stream lines up with the profile's
/// `video`/`audio`/`subtitles` array element it produced.
fn enrich_streams(doc: &mut serde_json::Value, title: &DiscTitle) {
    use crate::disc::Stream;
    use serde_json::json;
    let (mut vi, mut ai, mut si) = (0usize, 0usize, 0usize);
    // `doc[key][i]` panics on a missing array; skip an entry the profile lacks.
    let mut dummy = serde_json::Value::Null;
    for s in &title.streams {
        match s {
            Stream::Video(v) => {
                let e = entry(doc, "video", vi, &mut dummy);
                e["pid"] = json!(v.pid);
                e["color_space"] = json!(v.color_space.id());
                e["display_aspect"] = match v.display_aspect {
                    Some((n, d)) => json!([n, d]),
                    None => json!(null),
                };
                e["measured_cicp"] = match v.measured_cicp {
                    Some(c) => json!({
                        "matrix": c.matrix,
                        "transfer": c.transfer,
                        "primaries": c.primaries,
                        "range": c.range,
                    }),
                    None => json!(null),
                };
                match v.resolution.pixels() {
                    Some((w, h)) => {
                        e["width"] = json!(w);
                        e["height"] = json!(h);
                    }
                    None => {
                        e["width"] = json!(null);
                        e["height"] = json!(null);
                    }
                }
                e["interlaced"] = json!(v.resolution.is_interlaced());
                e["mvc"] = json!(v.is_mvc_dependent());
                vi += 1;
            }
            Stream::Audio(a) => {
                let e = entry(doc, "audio", ai, &mut dummy);
                e["pid"] = json!(a.pid);
                e["secondary"] = json!(a.secondary);
                // Numeric Hz (null when unknown), not the "48kHz" display form.
                let hz = a.sample_rate.hz() as u32;
                e["sample_rate"] = if hz == 0 { json!(null) } else { json!(hz) };
                e["sample_rates"] = json!(sample_rates(a.sample_rate));
                e["purpose"] = json!(purpose_id(a.purpose));
                ai += 1;
            }
            Stream::Subtitle(sub) => {
                let e = entry(doc, "subtitles", si, &mut dummy);
                e["pid"] = json!(sub.pid);
                e["descriptive_service"] =
                    json!(sub.qualifier == crate::disc::LabelQualifier::DescriptiveService);
                si += 1;
            }
        }
    }
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

impl PesSink for JsonSink {
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
#[path = "meta_sink_tests.rs"]
mod tests;
