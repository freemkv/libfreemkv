//! Normalized, format-agnostic disc profile — a thin typed VIEW over the scanned
//! [`Disc`] model.
//!
//! Every source (DVD/BD/UHD/HD-DVD) normalizes into the same [`Disc`] /
//! [`DiscTitle`] / [`Stream`] shapes; this module hoists those into a flat,
//! serde-friendly surface (`profile.titles[i].subtitles[j].forced`, etc.) with
//! every field always populated — no per-format conditionals, no bare `Option`.

use serde::{Deserialize, Serialize};

use super::{
    AudioStream, Disc, DiscFormat, DiscTitle, LabelPurpose, LabelQualifier, Stream, SubtitleStream,
    VideoStream,
};

/// A disc's complete normalized profile: identity plus every title's typed
/// track breakdown, with the main-feature selection hoisted to `main_title`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscProfile {
    /// Disc container family (`"bluray"`, `"uhd"`, `"dvd"`, `"hddvd"`, …).
    pub format: String,
    /// Best available display name (disc metadata title, else volume id).
    pub disc_name: String,
    /// Stable disc identifier (the UDF volume identifier).
    pub disc_id: String,
    /// Index into `titles` of the selected main feature. The scan pre-sorts
    /// titles so `titles[0]` is the main feature, so this is `0` whenever any
    /// title exists.
    pub main_title: usize,
    /// Every title, in the scan's main-feature order.
    pub titles: Vec<TitleProfile>,
    /// Whether the source disc is encrypted (AACS or CSS). A clear disc and a
    /// disc whose key resolution FAILED would otherwise serialize identically;
    /// this and [`Self::key_error`] disambiguate them.
    #[serde(default)]
    pub encrypted: bool,
    /// Numeric code of the key-resolution failure, or `None` when keys resolved
    /// (or the disc is unencrypted): the `aacs_error` code if present, else the
    /// `css_error` code. Numeric per the project's no-English error convention
    /// (see [`crate::error::Error::code`]).
    #[serde(default)]
    pub key_error: Option<u32>,
}

/// One title's normalized profile: identity, duration/size, chapter count, the
/// main-feature flag, and its streams split into typed per-kind vectors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TitleProfile {
    /// Position of this title within [`DiscProfile::titles`].
    pub index: usize,
    /// Playlist / program identifier (e.g. `"00800.mpls"`).
    pub playlist: String,
    /// Duration in seconds.
    pub duration_secs: f64,
    /// Total size in bytes.
    pub size_bytes: u64,
    /// Number of chapter points.
    pub chapters: usize,
    /// Whether this is the disc's main feature (`index == main_title`).
    pub is_main: bool,
    /// Video tracks, in declared order.
    pub video: Vec<VideoTrack>,
    /// Audio tracks, in declared order.
    pub audio: Vec<AudioTrack>,
    /// Subtitle tracks, in declared order.
    pub subtitles: Vec<SubtitleTrack>,
}

/// A normalized video track.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoTrack {
    /// Compact codec id (e.g. `"hevc"`, `"h264"`, `"vc1"`, `"mpeg2"`).
    pub codec: String,
    /// Resolution label (e.g. `"2160p"`, `"1080p"`, `"576i"`).
    pub resolution: String,
    /// HDR format id (e.g. `"sdr"`, `"hdr10"`, `"hdr10+"`, `"dv"`, `"hlg"`).
    pub hdr: String,
    /// Frame-rate label (e.g. `"23.976"`, `"25"`).
    pub frame_rate: String,
    /// Whether this is the title's default video track (first non-secondary).
    pub default: bool,
    /// Whether this is a secondary stream (PiP / dependent view).
    pub secondary: bool,
}

/// A normalized audio track.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioTrack {
    /// ISO 639-2 language code, `"und"` when the source stated none.
    pub language: String,
    /// Compact codec id (e.g. `"truehd"`, `"dtshd_ma"`, `"ac3"`).
    pub codec: String,
    /// Channel layout label (e.g. `"stereo"`, `"5.1"`, `"unknown"`).
    pub channels: String,
    /// Whether this is the title's default audio track (first non-secondary).
    pub default: bool,
    /// Editorial purpose flag: commentary track.
    pub commentary: bool,
    /// Editorial purpose flag: descriptive / audio-description track.
    pub descriptive: bool,
    /// Codec / variant text label; empty when the source stated none.
    pub name: String,
}

/// A normalized subtitle track.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubtitleTrack {
    /// ISO 639-2 language code, `"und"` when the source stated none.
    pub language: String,
    /// Compact codec id (e.g. `"pgs"`, `"dvdsub"`).
    pub codec: String,
    /// Forced-narrative flag.
    pub forced: bool,
    /// SDH (subtitles for the deaf / hard-of-hearing) flag.
    pub sdh: bool,
    /// Whether this is the title's default subtitle track. Always `false`: the
    /// mux never marks a subtitle default, and the normalized view mirrors that.
    pub default: bool,
    /// Track name; empty (the subtitle model carries no free-text label).
    pub name: String,
}

/// Container family as a compact, stable id. Kept here (not on [`DiscFormat`])
/// because it is a serialization concern of this view.
fn format_id(format: DiscFormat) -> &'static str {
    match format {
        DiscFormat::Uhd => "uhd",
        DiscFormat::Fmts => "uhd_fmts",
        DiscFormat::BluRay => "bluray",
        DiscFormat::HdDvd => "hddvd",
        DiscFormat::Dvd => "dvd",
        DiscFormat::Unknown => "unknown",
    }
}

/// `"und"` for an empty language code, else the code unchanged. Mirrors the
/// muxer's `language_or_und`: the one place a source with no language table has
/// to fall back to the ISO 639-2 "undetermined" code.
fn language_or_und(lang: &str) -> String {
    if lang.is_empty() {
        "und".to_string()
    } else {
        lang.to_string()
    }
}

impl VideoTrack {
    fn from_stream(v: &VideoStream, default: bool) -> Self {
        Self {
            codec: v.codec.id().to_string(),
            resolution: v.resolution.to_string(),
            hdr: v.hdr.id().to_string(),
            frame_rate: v.frame_rate.to_string(),
            default,
            secondary: v.secondary,
        }
    }
}

impl AudioTrack {
    fn from_stream(a: &AudioStream, default: bool) -> Self {
        Self {
            language: language_or_und(&a.language),
            codec: a.codec.id().to_string(),
            channels: a.channels.to_string(),
            default,
            commentary: a.purpose == LabelPurpose::Commentary,
            descriptive: a.purpose == LabelPurpose::Descriptive,
            name: a.label.clone(),
        }
    }
}

impl SubtitleTrack {
    fn from_stream(s: &SubtitleStream) -> Self {
        Self {
            language: language_or_und(&s.language),
            codec: s.codec.id().to_string(),
            // The authoritative forced flag (`forced`, set by the content probe /
            // STN) OR the label-derived qualifier — either is sufficient.
            forced: s.forced || s.qualifier == LabelQualifier::Forced,
            sdh: s.qualifier == LabelQualifier::Sdh,
            // The mux never defaults a subtitle track; mirror that here.
            default: false,
            name: String::new(),
        }
    }
}

impl TitleProfile {
    /// Build a title's profile. `index` / `is_main` are disc-level facts the
    /// caller supplies (see [`DiscProfile::from_disc`]); the streams are split
    /// and the per-kind default track is hoisted here.
    pub fn from_title(title: &DiscTitle, index: usize, is_main: bool) -> Self {
        let mut video = Vec::new();
        let mut audio = Vec::new();
        let mut subtitles = Vec::new();
        // "First non-secondary is the default; keep only the first" — the same
        // rule the Matroska path applies (`is_default = !secondary`, then only
        // the first video and first audio survive as default).
        let mut video_default_taken = false;
        let mut audio_default_taken = false;
        for s in &title.streams {
            match s {
                Stream::Video(v) => {
                    let default = !v.secondary && !video_default_taken;
                    video_default_taken |= default;
                    video.push(VideoTrack::from_stream(v, default));
                }
                Stream::Audio(a) => {
                    let default = !a.secondary && !audio_default_taken;
                    audio_default_taken |= default;
                    audio.push(AudioTrack::from_stream(a, default));
                }
                Stream::Subtitle(t) => subtitles.push(SubtitleTrack::from_stream(t)),
            }
        }
        Self {
            index,
            playlist: title.playlist.clone(),
            duration_secs: title.duration_secs,
            size_bytes: title.size_bytes,
            chapters: title.chapters.len(),
            is_main,
            video,
            audio,
            subtitles,
        }
    }

    /// The title's video tracks.
    pub fn video(&self) -> &[VideoTrack] {
        &self.video
    }

    /// The title's audio tracks.
    pub fn audio(&self) -> &[AudioTrack] {
        &self.audio
    }

    /// The title's subtitle tracks.
    pub fn subtitles(&self) -> &[SubtitleTrack] {
        &self.subtitles
    }
}

impl DiscProfile {
    /// Build the normalized profile from a scanned [`Disc`].
    pub fn from_disc(disc: &Disc) -> Self {
        // The scan pre-sorts titles so `titles[0]` is the selected main feature
        // (`sort_titles_by_main_feature`), so the main title is index 0 whenever
        // any title exists.
        let main_title = 0;
        let has_titles = !disc.titles.is_empty();
        let titles = disc
            .titles
            .iter()
            .enumerate()
            .map(|(i, t)| TitleProfile::from_title(t, i, has_titles && i == main_title))
            .collect();
        Self {
            format: format_id(disc.format).to_string(),
            disc_name: disc
                .meta_title
                .clone()
                .unwrap_or_else(|| disc.volume_id.clone()),
            disc_id: disc.volume_id.clone(),
            main_title,
            titles,
            encrypted: disc.encrypted,
            // AACS takes precedence over CSS: a disc is one format or the other,
            // and `from_disc` mirrors the same aacs-then-css order callers use.
            key_error: disc
                .aacs_error
                .as_ref()
                .or(disc.css_error.as_ref())
                .map(|e| u32::from(e.code())),
        }
    }

    /// The selected main-feature title, or `None` when the disc scanned to zero
    /// titles (a data-only image with no /BDMV, /HVDVD_TS or /VIDEO_TS). Never
    /// panics — mirrors the `Option`-returning main-feature accessors elsewhere
    /// (`titles.first()`, `dvdnav::resolve_main_title`).
    pub fn main_title(&self) -> Option<&TitleProfile> {
        self.titles.get(self.main_title)
    }
}

impl From<&Disc> for DiscProfile {
    fn from(disc: &Disc) -> Self {
        DiscProfile::from_disc(disc)
    }
}

impl Disc {
    /// This disc's normalized, format-agnostic [`DiscProfile`].
    pub fn profile(&self) -> DiscProfile {
        DiscProfile::from_disc(self)
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
