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

use super::demux_sink::TrackKind;
use super::resolve::StreamUrl;
use crate::disc::{DiscTitle, Stream};

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
    match super::pes_output::PesOutput::of(dest) {
        Ok(out) => out.fit(title),
        // Not a PES output: nothing to refuse.
        Err(_) => keep_fit(title, None, |_| None),
    }
}

// The plan of a sink that keeps every stream of class `class` (`None`: all) that `refuse`
// gives no reason for; a declared-only MPEG-2 extension track is in neither list (J23).
pub(crate) fn keep_fit(
    title: &DiscTitle,
    class: Option<TrackKind>,
    refuse: impl Fn(usize) -> Option<SkipReason>,
) -> FitReport {
    let mut report = FitReport {
        included: Vec::new(),
        skipped: Vec::new(),
    };
    for (i, s) in title.streams.iter().enumerate() {
        if matches!(s, Stream::Audio(a) if a.is_mp2_extension()) {
            continue;
        }
        let kind = match s {
            Stream::Video(_) => TrackKind::Video,
            Stream::Audio(_) => TrackKind::Audio,
            Stream::Subtitle(_) => TrackKind::Subtitle,
        };
        if class.is_some_and(|c| c != kind) {
            continue;
        }
        match refuse(i) {
            Some(r) => report.skipped.push((i, r)),
            None => report.included.push(i),
        }
    }
    report
}

#[cfg(test)]
#[path = "fit_tests.rs"]
mod tests;
