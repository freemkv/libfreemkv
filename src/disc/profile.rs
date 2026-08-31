//! Normalized, format-agnostic disc profile — a thin typed VIEW over the scanned
//! [`Disc`] model.
//!
//! The scan already normalizes every source (DVD/BD/UHD/HD-DVD) into the same
//! [`Disc`] / [`DiscTitle`] / [`Stream`] shapes, carrying language / forced /
//! qualifier / purpose / secondary / label per stream. This module hoists those
//! into a flat, serde-friendly surface so a consumer reads
//! `profile.titles[i].subtitles[j].forced` (and the analogous audio/video flags)
//! for ANY disc with no per-format conditionals.
//!
//! Everything here is DERIVED from the model at construction: the enum `Stream`
//! is split into three typed vectors, the `qualifier` / `purpose` enums are
//! decomposed into booleans, and the "default track" selection is precomputed so
//! downstream never recomputes it. Every field is always populated — a sensible
//! default (`"und"` language, empty `name`, `false` flag) stands in where the
//! model has nothing, so consumers never handle a bare `Option`.

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
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, ContentFormat, DiscRegion, DiscTitle,
        FrameRate, HdrFormat, Resolution, SampleRate, Stream, SubtitleStream, VideoStream,
    };

    /// A minimal titleless [`Disc`] for the disc-level tests.
    fn test_disc() -> Disc {
        Disc {
            volume_id: String::new(),
            meta_title: None,
            format: DiscFormat::BluRay,
            capacity_sectors: 0,
            capacity_bytes: 0,
            layers: 1,
            titles: Vec::new(),
            region: DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: false,
            aacs_error: None,
            css_error: None,
            content_format: ContentFormat::BdTs,
        }
    }

    #[test]
    fn main_title_is_none_on_empty_disc() {
        // A data-only image (no /BDMV, /HVDVD_TS, /VIDEO_TS) scans to zero
        // titles; main_title() must return None, not panic on titles[0].
        let profile = DiscProfile::from_disc(&test_disc());
        assert!(profile.titles.is_empty());
        assert!(profile.main_title().is_none());
    }

    fn video(codec: Codec, res: Resolution, secondary: bool) -> Stream {
        Stream::Video(VideoStream {
            pid: 0x1011,
            codec,
            resolution: res,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary,
            label: String::new(),
            measured_cicp: None,
        })
    }

    fn audio(lang: &str, secondary: bool, purpose: LabelPurpose, label: &str) -> Stream {
        Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::TrueHd,
            channels: AudioChannels::Stereo,
            language: lang.into(),
            sample_rate: SampleRate::S48,
            secondary,
            purpose,
            label: label.into(),
        })
    }

    fn subtitle(lang: &str, forced: bool, qualifier: LabelQualifier) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: lang.into(),
            forced,
            qualifier,
            codec_data: None,
        })
    }

    /// A BD transport-stream title with a mix of streams: two audio tracks
    /// (main + commentary), two subtitles (forced + SDH), a video track.
    fn bdts_title() -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.content_format = ContentFormat::BdTs;
        t.playlist = "00800.mpls".into();
        t.duration_secs = 7200.0;
        t.size_bytes = 30_000_000_000;
        t.streams = vec![
            video(Codec::Hevc, Resolution::R2160p, false),
            audio("eng", false, LabelPurpose::Normal, "Dolby TrueHD 5.1"),
            audio("eng", true, LabelPurpose::Commentary, "Director"),
            subtitle("eng", true, LabelQualifier::Forced),
            subtitle("eng", false, LabelQualifier::Sdh),
        ];
        t
    }

    /// A DVD (MPEG program stream) title: two audio tracks (both non-secondary,
    /// second is descriptive), one language-less subtitle, one video track.
    fn dvd_title() -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.content_format = ContentFormat::MpegPs;
        t.playlist = "VTS_01".into();
        t.duration_secs = 5400.0;
        t.size_bytes = 4_000_000_000;
        t.streams = vec![
            video(Codec::Mpeg2, Resolution::R576i, false),
            audio("eng", false, LabelPurpose::Normal, ""),
            audio("eng", false, LabelPurpose::Descriptive, ""),
            subtitle("", false, LabelQualifier::None),
        ];
        t
    }

    #[test]
    fn splits_streams_into_typed_vectors_across_formats() {
        for (title, expect_subs) in [(bdts_title(), 2), (dvd_title(), 1)] {
            let p = TitleProfile::from_title(&title, 0, true);
            assert_eq!(p.video.len(), 1, "one video track");
            assert_eq!(p.audio.len(), 2, "two audio tracks");
            assert_eq!(p.subtitles.len(), expect_subs, "subtitle count per format");
            // Accessors return the same vectors.
            assert_eq!(p.video(), p.video.as_slice());
            assert_eq!(p.audio(), p.audio.as_slice());
            assert_eq!(p.subtitles(), p.subtitles.as_slice());
        }
    }

    #[test]
    fn hoists_default_flags_first_non_secondary() {
        let p = TitleProfile::from_title(&bdts_title(), 0, true);
        // First (non-secondary) video/audio is the default.
        assert!(p.video[0].default, "first video defaults");
        assert!(p.audio[0].default, "first non-secondary audio defaults");
        // The commentary track is secondary → never default.
        assert!(
            !p.audio[1].default,
            "secondary/commentary audio not default"
        );
        // Subtitles are never default.
        assert!(p.subtitles.iter().all(|s| !s.default));
    }

    #[test]
    fn second_default_cleared_when_two_non_secondary_audio() {
        // DVD title: BOTH audio tracks are non-secondary. Only the first keeps
        // the default flag (mirrors the muxer's "keep only first default").
        let p = TitleProfile::from_title(&dvd_title(), 0, true);
        assert!(p.audio[0].default, "first audio default");
        assert!(
            !p.audio[1].default,
            "second non-secondary audio default cleared"
        );
        assert!(p.audio[1].descriptive, "second audio is descriptive");
        assert!(!p.audio[1].commentary);
    }

    #[test]
    fn maps_qualifier_and_purpose_and_language_defaults() {
        let p = TitleProfile::from_title(&bdts_title(), 0, true);
        // Audio purpose → commentary/descriptive booleans.
        assert!(!p.audio[0].commentary && !p.audio[0].descriptive);
        assert!(p.audio[1].commentary);
        assert_eq!(p.audio[1].name, "Director", "label maps to name");
        // Subtitle qualifier → forced/sdh booleans.
        assert!(p.subtitles[0].forced && !p.subtitles[0].sdh);
        assert!(p.subtitles[1].sdh && !p.subtitles[1].forced);
        // Language default: DVD title's blank subtitle language → "und".
        let dvd = TitleProfile::from_title(&dvd_title(), 0, true);
        assert_eq!(dvd.subtitles[0].language, "und");
        assert_eq!(dvd.audio[0].language, "eng");
        // Codec/resolution/hdr flattened as compact ids/labels.
        assert_eq!(p.video[0].codec, "hevc");
        assert_eq!(p.video[0].resolution, "2160p");
        assert_eq!(p.video[0].hdr, "hdr10");
        assert_eq!(dvd.video[0].codec, "mpeg2");
    }

    #[test]
    fn from_disc_hoists_main_and_is_main() {
        let mut disc = test_disc();
        disc.format = DiscFormat::BluRay;
        disc.meta_title = Some("SOME MOVIE".into());
        disc.volume_id = "VOL_ID".into();
        disc.titles = vec![bdts_title(), dvd_title()];
        let profile = disc.profile();
        assert_eq!(profile.format, "bluray");
        assert_eq!(profile.disc_name, "SOME MOVIE");
        assert_eq!(profile.disc_id, "VOL_ID");
        assert_eq!(profile.main_title, 0);
        assert!(profile.titles[0].is_main, "titles[0] is the main feature");
        assert!(!profile.titles[1].is_main);
        assert_eq!(profile.titles[1].index, 1);
        assert_eq!(profile.main_title().unwrap().playlist, "00800.mpls");
    }

    #[test]
    fn disc_name_falls_back_to_volume_id() {
        let mut disc = test_disc();
        disc.meta_title = None;
        disc.volume_id = "PLAIN_VOLUME".into();
        assert_eq!(disc.profile().disc_name, "PLAIN_VOLUME");
    }

    #[test]
    fn encryption_status_and_key_error_surface_numerically() {
        // Encrypted disc whose key resolution FAILED: the encrypted flag is set
        // and the numeric aacs_error code surfaces (never English text), so it
        // no longer serializes identically to a rippable disc.
        let mut disc = test_disc();
        disc.encrypted = true;
        let err = crate::error::Error::AacsVidUnavailable;
        let aacs_code = u32::from(err.code());
        disc.aacs_error = Some(err);
        let p = disc.profile();
        assert!(p.encrypted);
        assert_eq!(p.key_error, Some(aacs_code));

        // AACS takes precedence: with BOTH errors set, the aacs code wins.
        let mut both = test_disc();
        both.encrypted = true;
        both.aacs_error = Some(crate::error::Error::AacsVidUnavailable);
        both.css_error = Some(crate::error::Error::CssKeyMissing);
        assert_eq!(both.profile().key_error, Some(aacs_code));

        // A CSS-only failure surfaces the css code.
        let mut css = test_disc();
        css.encrypted = true;
        let css_err = crate::error::Error::CssKeyMissing;
        let css_code = u32::from(css_err.code());
        css.css_error = Some(css_err);
        assert_eq!(css.profile().key_error, Some(css_code));

        // A clean, rippable disc: not encrypted, no key error.
        let clean = test_disc().profile();
        assert!(!clean.encrypted);
        assert_eq!(clean.key_error, None);
    }

    #[test]
    fn serde_round_trip() {
        let mut disc = test_disc();
        disc.format = DiscFormat::Dvd;
        disc.volume_id = "RT".into();
        disc.titles = vec![bdts_title(), dvd_title()];
        let profile = disc.profile();
        let json = serde_json::to_string(&profile).expect("serialize");
        let back: DiscProfile = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(profile, back, "profile round-trips through serde");
    }
}
