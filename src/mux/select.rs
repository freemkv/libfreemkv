//! Per-title audio/subtitle stream selection — the pure primitive.
//!
//! The demux pipeline is **declaration-driven**: every demux table
//! (`build_demux_state` in `mux/resolve.rs`, `DiscStream::new` in `mux/disc.rs`)
//! is built from the input [`DiscTitle`]'s `streams` list, and the MKV writer
//! builds its track headers + `codec_privates` from that same list. A PID not
//! declared there is never tracked, extracted, or written. So "which streams to
//! keep" is already a capability of the pipeline — it just has no public knob.
//!
//! [`StreamSelection::apply`] is that knob: prune the `DiscTitle.streams` list
//! (video always kept) BEFORE the mux path finalizes the title, and everything
//! downstream — track headers, `codec_privates`, PID routing, frame emission —
//! follows from the pruned list by construction, with zero scattered PID
//! checks. This is language-agnostic: PIDs, not languages (the language→PID
//! mapping is the caller's/engine's policy).

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
    /// PID passes the corresponding [`PidFilter`]; drop the rest. Declared order
    /// is preserved. The parallel `codec_privates` vec is pruned in lockstep
    /// when it is populated (it is empty on a freshly-scanned title, non-empty
    /// only if a caller pre-filled it).
    ///
    /// Errors [`Error::SelectionPidUnknown`] if a filter lists a PID that does
    /// not exist in `title.streams` — a caller bug (e.g. a stale scan). Fail
    /// loud rather than silently emit an MKV missing a requested track. On
    /// error the title is left unmodified.
    pub fn apply(&self, title: &mut DiscTitle) -> Result<()> {
        if self.is_all() {
            return Ok(());
        }

        // Validate every listed PID exists in the title before mutating, so an
        // unknown PID leaves the title untouched (no partial prune).
        for pid in self.listed_pids() {
            let present = title.streams.iter().any(|s| stream_pid(s) == Some(pid));
            if !present {
                return Err(Error::SelectionPidUnknown { pid });
            }
        }

        let codec_privates_aligned = title.codec_privates.len() == title.streams.len();

        // Retain by index so we can prune the parallel codec_privates in lockstep.
        let keep: Vec<bool> = title
            .streams
            .iter()
            .map(|s| self.keeps(s))
            .collect::<Vec<_>>();

        let mut i = 0;
        title.streams.retain(|_| {
            let k = keep[i];
            i += 1;
            k
        });
        if codec_privates_aligned {
            let mut j = 0;
            title.codec_privates.retain(|_| {
                let k = keep[j];
                j += 1;
                k
            });
        }
        Ok(())
    }

    /// Whether this selection keeps `stream`.
    fn keeps(&self, stream: &Stream) -> bool {
        match stream {
            Stream::Video(_) => true,
            Stream::Audio(a) => filter_keeps(&self.audio, a.pid),
            Stream::Subtitle(s) => filter_keeps(&self.subtitle, s.pid),
        }
    }

    /// Every PID explicitly listed across both filters (for existence checking).
    fn listed_pids(&self) -> Vec<u16> {
        let mut v = Vec::new();
        if let PidFilter::Only(pids) = &self.audio {
            v.extend_from_slice(pids);
        }
        if let PidFilter::Only(pids) = &self.subtitle {
            v.extend_from_slice(pids);
        }
        v
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
            .filter_map(|s| match s {
                Stream::Video(v) => Some(v.pid),
                Stream::Audio(a) => Some(a.pid),
                Stream::Subtitle(s) => Some(s.pid),
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
}
