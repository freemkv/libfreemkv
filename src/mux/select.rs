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
#[path = "select_tests.rs"]
mod tests;
