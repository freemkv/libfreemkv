//! Per-title audio/subtitle stream selection — the pure primitive.
//!
//! [`StreamSelection::apply`] prunes `DiscTitle.streams` (video always kept)
//! BEFORE the mux path finalizes the title, so track headers, `codec_privates`,
//! PID routing, and frame emission all follow from the pruned list by
//! construction. Language-agnostic: PIDs, not languages.

use crate::disc::{DiscTitle, Stream};
use crate::error::{Error, Result};

/// Which PIDs to keep for one stream class (audio or subtitle). Video is always
/// kept, so it has no filter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PidFilter {
    /// Keep every stream of this class. The default; [`StreamSelection::apply`]
    /// is a no-op for an All/All selection, so the no-selection path is
    /// byte-identical to no selection at all.
    #[default]
    All,
    /// Keep only the streams whose PID is listed. `Only(vec![])` is legal and
    /// means keep none (a video-only output when both classes are `Only([])`).
    Only(Vec<u16>),
}

/// A per-title stream selection: which audio and which subtitle PIDs to keep.
/// Video is always retained (it is implicit and never pruned).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamSelection {
    pub audio: PidFilter,
    pub subtitle: PidFilter,
}

impl StreamSelection {
    /// True for the All/All default. Apply sites gate on `!is_all()` so the
    /// no-selection path never even clones the title.
    pub fn is_all(&self) -> bool {
        matches!(self.audio, PidFilter::All) && matches!(self.subtitle, PidFilter::All)
    }

    /// Prune `title.streams` in place: keep every [`Stream::Video`]
    /// unconditionally; keep an [`Stream::Audio`]/[`Stream::Subtitle`] iff its
    /// PID passes the corresponding [`PidFilter`]; drop the rest. A DVD MPEG-2
    /// multichannel extension track follows its base's PID; listing it alone keeps both. Declared
    /// order is preserved. The parallel `codec_privates` vec is pruned in
    /// lockstep, by index, when populated.
    ///
    /// Errors [`Error::SelectionPidUnknown`] if a filter lists a PID absent
    /// from `title.streams`; the title is left unmodified on error.
    pub fn apply(&self, title: &mut DiscTitle) -> Result<()> {
        if self.is_all() {
            return Ok(());
        }
        let keep = self.keeps_by_index(title)?;
        let mut i = 0;
        title.streams.retain(|_| {
            let k = keep[i];
            i += 1;
            k
        });
        // Prune `codec_privates` by the SAME index decision, whatever its length. Old
        // bug: guarding on `len == streams.len()` let a trailing extra entry skip the
        // prune, so streams after a dropped one got the PREVIOUS codec_private (wrong SPS).
        let extra = title.codec_privates.len().saturating_sub(keep.len());
        if extra > 0 {
            tracing::debug!(
                target: "mux",
                codec_privates = title.codec_privates.len(),
                streams = keep.len(),
                "stream selection: dropping {extra} codec_private entry/entries that describe \
                 no declared stream"
            );
        }
        let mut j = 0;
        title.codec_privates.retain(|_| {
            let k = keep.get(j).copied().unwrap_or(false);
            j += 1;
            k
        });
        Ok(())
    }

    /// Whether each of `title.streams` is kept, by index; the same validation as
    /// [`apply`](Self::apply) ([`Error::SelectionPidUnknown`] for a PID the title lacks).
    pub(crate) fn keeps_by_index(&self, title: &DiscTitle) -> Result<Vec<bool>> {
        if self.is_all() {
            return Ok(vec![true; title.streams.len()]);
        }

        // Validate every listed PID before mutating (unknown PID → no partial prune),
        // PER CLASS: scanning both classes let a PID in the WRONG filter pass, then
        // `keeps` matched it only against its own class, silently dropping the track.
        let check = |filter: &PidFilter, is_class: fn(&Stream) -> bool| -> Result<()> {
            if let PidFilter::Only(pids) = filter {
                for &pid in pids {
                    let present = title
                        .streams
                        .iter()
                        .any(|s| is_class(s) && stream_pid(s) == Some(pid));
                    if !present {
                        return Err(Error::SelectionPidUnknown { pid });
                    }
                }
            }
            Ok(())
        };
        check(&self.audio, |s| matches!(s, Stream::Audio(_)))?;
        check(&self.subtitle, |s| matches!(s, Stream::Subtitle(_)))?;

        let effective = self.with_mp2_extension_bases(title);
        Ok(title.streams.iter().map(|s| effective.keeps(s)).collect())
    }

    // An extension PID listed without its base pulls the base in (13818-3 2nd ed. §2.5.2.13:
    // the extension holds "a remainder of the multichannel ... information").
    fn with_mp2_extension_bases(&self, title: &DiscTitle) -> StreamSelection {
        let PidFilter::Only(listed) = &self.audio else {
            return self.clone();
        };
        let mut audio = listed.clone();
        for s in &title.streams {
            if let Stream::Audio(a) = s
                && a.is_mp2_extension()
                && listed.contains(&a.pid)
            {
                let base = 0x00C0 | (a.pid & 0x07);
                if !audio.contains(&base) {
                    tracing::info!(
                        target: "mux",
                        "stream selection: MPEG-2 multichannel extension {:#04x} listed without \
                         its base {base:#04x}; keeping the base too",
                        a.pid,
                    );
                    audio.push(base);
                }
            }
        }
        StreamSelection {
            audio: PidFilter::Only(audio),
            subtitle: self.subtitle.clone(),
        }
    }

    /// Whether this selection keeps `stream`.
    fn keeps(&self, stream: &Stream) -> bool {
        match stream {
            Stream::Video(_) => true,
            // 13818-3 2nd ed. §2.5.2.13: the extension "contains a remainder of the
            // multichannel and multilingual audio information", so it follows its base 0xC0|n.
            Stream::Audio(a) if a.is_mp2_extension() => {
                filter_keeps(&self.audio, 0x00C0 | (a.pid & 0x07))
            }
            Stream::Audio(a) => filter_keeps(&self.audio, a.pid),
            Stream::Subtitle(s) => filter_keeps(&self.subtitle, s.pid),
        }
    }
}

fn filter_keeps(filter: &PidFilter, pid: u16) -> bool {
    match filter {
        PidFilter::All => true,
        PidFilter::Only(pids) => pids.contains(&pid),
    }
}

/// The PID of an audio/subtitle stream; `None` for video (which is never
/// filtered, so its PID is irrelevant to selection).
fn stream_pid(stream: &Stream) -> Option<u16> {
    match stream {
        Stream::Audio(a) => Some(a.pid),
        Stream::Subtitle(s) => Some(s.pid),
        Stream::Video(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, LabelPurpose,
        LabelQualifier, Resolution, SampleRate, SubtitleStream, VideoStream,
    };

    fn video(pid: u16) -> Stream {
        Stream::Video(VideoStream {
            pid,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    }
    fn audio(pid: u16, lang: &str) -> Stream {
        Stream::Audio(AudioStream {
            pid,
            codec: Codec::TrueHd,
            channels: AudioChannels::Stereo,
            language: lang.into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    }
    fn subtitle(pid: u16, lang: &str) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid,
            codec: Codec::Pgs,
            language: lang.into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }

    // video + 3 audio (eng/spa/fra) + 2 subs (eng/spa).
    fn title() -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams = vec![
            video(0x1011),
            audio(0x1100, "eng"),
            audio(0x1101, "spa"),
            audio(0x1102, "fra"),
            subtitle(0x1200, "eng"),
            subtitle(0x1201, "spa"),
        ];
        t
    }

    fn pids(t: &DiscTitle) -> Vec<u16> {
        t.streams
            .iter()
            .map(|s| match s {
                Stream::Video(v) => v.pid,
                Stream::Audio(a) => a.pid,
                Stream::Subtitle(s) => s.pid,
            })
            .collect()
    }

    #[test]
    fn apply_all_is_identity_and_untouched() {
        let sel = StreamSelection::default();
        assert!(sel.is_all());
        let mut t = title();
        let before = pids(&t);
        sel.apply(&mut t).unwrap();
        assert_eq!(pids(&t), before, "All/All must not change the stream list");
    }

    #[test]
    fn apply_only_retains_listed_audio_pids_in_declared_order() {
        // Keep eng+fra audio (skip spa); leave subtitles alone.
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x1100, 0x1102]),
            subtitle: PidFilter::All,
        };
        let mut t = title();
        sel.apply(&mut t).unwrap();
        assert_eq!(
            pids(&t),
            vec![0x1011, 0x1100, 0x1102, 0x1200, 0x1201],
            "video + eng/fra audio (order preserved) + both subs"
        );
    }

    #[test]
    fn apply_only_empty_yields_video_only() {
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![]),
            subtitle: PidFilter::Only(vec![]),
        };
        let mut t = title();
        sel.apply(&mut t).unwrap();
        assert_eq!(pids(&t), vec![0x1011], "only the video stream survives");
    }

    #[test]
    fn apply_subtitle_filter_does_not_touch_audio() {
        let sel = StreamSelection {
            audio: PidFilter::All,
            subtitle: PidFilter::Only(vec![0x1200]),
        };
        let mut t = title();
        sel.apply(&mut t).unwrap();
        assert_eq!(
            pids(&t),
            vec![0x1011, 0x1100, 0x1101, 0x1102, 0x1200],
            "all audio kept, only eng subtitle kept"
        );
    }

    #[test]
    fn apply_unknown_pid_errors_and_leaves_title_untouched() {
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x9999]),
            subtitle: PidFilter::All,
        };
        let mut t = title();
        let before = pids(&t);
        let err = sel.apply(&mut t).unwrap_err();
        assert!(matches!(err, Error::SelectionPidUnknown { pid: 0x9999 }));
        assert_eq!(pids(&t), before, "title unmodified on error");
    }

    #[test]
    fn apply_prunes_codec_privates_in_lockstep_when_populated() {
        // A caller that pre-filled codec_privates parallel to streams: pruning
        // must keep the two vecs aligned.
        let mut t = title();
        t.codec_privates = vec![
            Some(vec![0xAA]), // video 0x1011
            Some(vec![0x11]), // audio 0x1100 eng
            Some(vec![0x22]), // audio 0x1101 spa
            Some(vec![0x33]), // audio 0x1102 fra
            None,             // sub 0x1200
            None,             // sub 0x1201
        ];
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x1100]),
            subtitle: PidFilter::Only(vec![]),
        };
        sel.apply(&mut t).unwrap();
        assert_eq!(pids(&t), vec![0x1011, 0x1100]);
        assert_eq!(
            t.codec_privates,
            vec![Some(vec![0xAA]), Some(vec![0x11])],
            "codec_privates pruned to match the retained streams, in order"
        );
    }

    // codec_privates longer than streams must still prune in lockstep by index (regression: a
    // trailing extra entry once skipped the prune, misattaching codec-private to the wrong
    // track).
    #[test]
    fn apply_prunes_codec_privates_even_when_length_does_not_match_streams() {
        let mut t = title();
        t.streams.truncate(4); // video + eng + spa + fra audio
        t.codec_privates = vec![
            Some(vec![0xAA]), // video 0x1011
            Some(vec![0x11]), // audio 0x1100 eng
            Some(vec![0x22]), // audio 0x1101 spa
            Some(vec![0x33]), // audio 0x1102 fra
            Some(vec![0xEE]), // trailing extra — describes no declared stream
        ];
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x1102]),
            subtitle: PidFilter::All,
        };
        sel.apply(&mut t).unwrap();

        assert_eq!(pids(&t), vec![0x1011, 0x1102], "video + fra audio");
        assert_eq!(
            t.codec_privates,
            vec![Some(vec![0xAA]), Some(vec![0x33])],
            "the retained fra track must keep ITS OWN codec_private, and the \
             trailing entry that describes no stream must not survive the prune"
        );
        assert_eq!(
            t.codec_privates.len(),
            t.streams.len(),
            "the two positional vecs must be aligned after apply()"
        );
    }
    // A PID in the wrong class's filter must fail loud, not silently vanish (validation once
    // scanned both classes, letting it pass then get dropped by `keeps`).
    #[test]
    fn a_pid_listed_in_the_wrong_class_filter_is_rejected() {
        let mut t = title();
        let before = t.streams.len();

        // 0x1200 is a SUBTITLE pid, listed here in the AUDIO filter.
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x1200]),
            subtitle: PidFilter::All,
        };
        assert!(
            sel.apply(&mut t).is_err(),
            "a subtitle PID in the audio filter must be rejected"
        );
        assert_eq!(
            t.streams.len(),
            before,
            "a rejected selection must not prune"
        );

        // And the mirror case: an audio pid listed in the subtitle filter.
        let sel = StreamSelection {
            audio: PidFilter::All,
            subtitle: PidFilter::Only(vec![0x1100]),
        };
        assert!(
            sel.apply(&mut t).is_err(),
            "an audio PID in the subtitle filter must be rejected"
        );

        // Sanity: each PID in its OWN class still validates.
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x1100]),
            subtitle: PidFilter::Only(vec![0x1200]),
        };
        assert!(
            sel.apply(&mut t).is_ok(),
            "correctly-classed PIDs must apply"
        );
    }

    // DVD title: video, MP2 base 0xC0 + its extension 0xD0, MP2 0xC1.
    fn mp2_ext_title() -> DiscTitle {
        let mp2 = |pid: u16, label: &str| {
            Stream::Audio(AudioStream {
                pid,
                codec: Codec::Mp2,
                channels: AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: LabelPurpose::Normal,
                label: label.into(),
            })
        };
        let mut t = DiscTitle::empty();
        t.streams = vec![
            video(0x00E0),
            mp2(0x00C0, ""),
            mp2(0x00D0, crate::disc::MP2_EXTENSION_LABEL),
            mp2(0x00C1, ""),
        ];
        t
    }

    /// An MPEG-2 extension track (`0xD0|n`) is kept iff its base (`0xC0|n`) is, whatever the
    /// filter lists: it holds "a remainder of the multichannel ... information" (13818-3 2nd ed.
    /// §2.5.2.13), meaningless alone. Per spec; do not change without a spec citation otherwise.
    #[test]
    fn an_mp2_extension_follows_its_base_whatever_the_filter_lists() {
        let keep = |audio: Vec<u16>| {
            let mut t = mp2_ext_title();
            StreamSelection {
                audio: PidFilter::Only(audio),
                subtitle: PidFilter::All,
            }
            .apply(&mut t)
            .unwrap();
            pids(&t)
        };
        assert_eq!(keep(vec![0x00C0]), vec![0x00E0, 0x00C0, 0x00D0]);
        assert_eq!(keep(vec![0x00C1]), vec![0x00E0, 0x00C1]);
        assert_eq!(keep(vec![0x00C0, 0x00D0]), vec![0x00E0, 0x00C0, 0x00D0]);
    }

    /// Listing an extension PID without its base pulls the base in (logged), never a silent
    /// drop of what was asked for: the extension is "a remainder" of the base's multichannel
    /// information (13818-3 2nd ed. §2.5.2.13), useless alone.
    #[test]
    fn an_mp2_extension_listed_alone_pulls_its_base_in() {
        let (kept, ev) = crate::testlog::capture(|| {
            let mut t = mp2_ext_title();
            StreamSelection {
                audio: PidFilter::Only(vec![0x00D0, 0x00C1]),
                subtitle: PidFilter::All,
            }
            .apply(&mut t)
            .unwrap();
            pids(&t)
        });
        assert_eq!(kept, vec![0x00E0, 0x00C0, 0x00D0, 0x00C1]);
        assert!(
            ev.iter()
                .any(|e| e.message().contains("0xc0") && e.message().contains("0xd0")),
            "the added base is logged"
        );
    }
}
