use super::*;
use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, ContentFormat, DiscRegion, DiscTitle, FrameRate,
    HdrFormat, Resolution, SampleRate, Stream, SubtitleStream, VideoStream,
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
fn secondary_first_or_only_track_is_never_default() {
    let mut t = DiscTitle::empty();
    t.streams = vec![
        video(Codec::Hevc, Resolution::R2160p, true),
        video(Codec::Hevc, Resolution::R2160p, false),
        audio("eng", true, LabelPurpose::Commentary, "Director"),
        audio("eng", false, LabelPurpose::Normal, ""),
    ];
    let p = TitleProfile::from_title(&t, 0, true);
    assert!(!p.video[0].default && p.video[1].default);
    assert!(!p.audio[0].default && p.audio[1].default);
    // Only a secondary track of each kind: nothing is default.
    t.streams = vec![
        video(Codec::Hevc, Resolution::R2160p, true),
        audio("eng", true, LabelPurpose::Commentary, ""),
    ];
    let p = TitleProfile::from_title(&t, 0, true);
    assert!(!p.video[0].default && !p.audio[0].default);
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

// Forced is the probe/STN flag OR the label qualifier: either alone is enough.
#[test]
fn subtitle_forced_is_the_flag_or_the_qualifier() {
    for (flag, qualifier, want) in [
        (false, LabelQualifier::None, false),
        (true, LabelQualifier::None, true),
        (false, LabelQualifier::Forced, true),
        (true, LabelQualifier::Forced, true),
    ] {
        let Stream::Subtitle(s) = subtitle("eng", flag, qualifier) else {
            unreachable!()
        };
        assert_eq!(
            SubtitleTrack::from_stream(&s).forced,
            want,
            "{flag} {qualifier:?}"
        );
    }
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
