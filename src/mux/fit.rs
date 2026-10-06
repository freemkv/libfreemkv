//! The container-neutral pre-mux fit plan: which `title.streams` a destination
//! scheme carries and which it leaves out, with a reason, so a lossy mux is never
//! silent (mpg-output-design v5 §1.1 G3, L1).
//!
//! The plan is a PREDICTION made before any frame moves. Exclusions that only the
//! mux can see (a DVD MPEG-2 multichannel extension track whose packets arrive, an
//! mp4 track with no sample) are reported post-mux through
//! [`Stream::undelivered_streams`](crate::pes::PesSink::undelivered_streams) and
//! [`Mp4Sink::final_report`](super::mp4::Mp4Sink::final_report), never here: a
//! declared-only extension track is never planned out (J23).

use super::mp4::Mp4SkipReason;
use super::resolve::StreamUrl;
use crate::disc::{Codec, DiscTitle, Stream};

/// Why a stream is left out of a mux, for the never-silent note.
///
/// `#[non_exhaustive]`: reasons are added as sinks learn to tell more cases apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// A bitmap subtitle (PGS/VobSub) the container has no mapping for.
    BitmapSubtitle,
    /// A text subtitle (SRT/SSA) the container has no mapping for.
    UnmappableSubtitle,
    /// An audio codec (or LPCM layout/rate) the container has no mapping for.
    UnmappableAudio,
    /// A secondary/dependent video view, or a second primary video.
    SecondaryVideo,
    /// A primary video track whose codec the container cannot carry.
    UnmappableVideo,
    /// A DVD MPEG-2 multichannel extension track. Post-mux only: reported once its
    /// `0xD0|n` packets arrive, never from a declaration alone (J23).
    Mp2Extension,
    /// The container has no stream number left in this track's id range.
    NoStreamId,
    /// Planned, but the stream delivered no sample at all (post-mux).
    NoSamples,
    /// Planned, but no frame yielded a describable audio sample entry (post-mux).
    UndescribableAudio,
}

impl From<Mp4SkipReason> for SkipReason {
    fn from(r: Mp4SkipReason) -> Self {
        match r {
            Mp4SkipReason::BitmapSubtitle => SkipReason::BitmapSubtitle,
            Mp4SkipReason::UnmappableAudio => SkipReason::UnmappableAudio,
            Mp4SkipReason::SecondaryVideo => SkipReason::SecondaryVideo,
            Mp4SkipReason::UnmappableVideo => SkipReason::UnmappableVideo,
            Mp4SkipReason::Mp2Extension => SkipReason::Mp2Extension,
            Mp4SkipReason::NoSamples => SkipReason::NoSamples,
            Mp4SkipReason::UndescribableAudio => SkipReason::UndescribableAudio,
        }
    }
}

/// The pre-mux plan for muxing `title` to one destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FitReport {
    /// `title.streams` indices the destination will carry.
    pub included: Vec<usize>,
    /// Excluded `(stream index, reason)`, in stream order.
    pub skipped: Vec<(usize, SkipReason)>,
}

/// The pre-mux fit plan of `title` for `dest`. Every stream is either included or
/// skipped with a reason, except a declared-only MPEG-2 extension track, which is
/// in neither: whether it is lost is known only once its packets arrive (J23).
/// `mkv://` skips only codecs Matroska has no CodecID for; the FMKV wire skips nothing.
pub fn fit_report(dest: &StreamUrl, title: &DiscTitle) -> FitReport {
    if let StreamUrl::Mpg { .. } = dest {
        return super::mpg::plan(title).report;
    }
    if let StreamUrl::Mp4 { .. } = dest {
        let r = super::mp4::fit_report(title);
        return FitReport {
            included: r.included,
            skipped: r.skipped.into_iter().map(|(i, s)| (i, s.into())).collect(),
        };
    }
    let mut report = FitReport {
        included: Vec::new(),
        skipped: Vec::new(),
    };
    for (i, s) in title.streams.iter().enumerate() {
        if matches!(s, Stream::Audio(a) if a.is_mp2_extension()) {
            continue;
        }
        let class_kept = match dest {
            StreamUrl::Video { .. } => matches!(s, Stream::Video(_)),
            StreamUrl::Audio { .. } => matches!(s, Stream::Audio(_)),
            StreamUrl::Sub { .. } => matches!(s, Stream::Subtitle(_)),
            _ => true,
        };
        if !class_kept {
            continue;
        }
        let refused = match (dest, s) {
            (StreamUrl::M2ts { .. }, Stream::Audio(a)) if a.codec == Codec::Aac => {
                let cp = title.codec_privates.get(i).and_then(|c| c.as_deref());
                super::m2ts::aac_adts_template(cp)
                    .is_none()
                    .then_some(SkipReason::UnmappableAudio)
            }
            (StreamUrl::M2ts { .. }, Stream::Audio(a)) if a.codec == Codec::Lpcm => {
                let cp = title.codec_privates.get(i).and_then(|c| c.as_deref());
                super::m2ts::lpcm_bd_header(a, cp)
                    .is_none()
                    .then_some(SkipReason::UnmappableAudio)
            }
            (StreamUrl::Mkv { .. }, s) if !super::mkv::is_mappable(s) => Some(match s {
                Stream::Video(_) => SkipReason::UnmappableVideo,
                Stream::Audio(_) => SkipReason::UnmappableAudio,
                Stream::Subtitle(_) => SkipReason::UnmappableSubtitle,
            }),
            _ => None,
        };
        match refused {
            Some(r) => report.skipped.push((i, r)),
            None => report.included.push(i),
        }
    }
    report
}

#[cfg(test)]
#[path = "fit_tests.rs"]
mod tests;
