//! Shared `headers_ready` rule for the PES read streams.

use crate::disc::{Codec, DiscTitle, Stream};
use crate::pes::PesFrame;

// Source time an AAC (ASC) or BD LPCM (layout byte) track may take to deliver its
// first frame, the only source of that config, before headers finalise without it.
const CONFIG_WAIT_NS: i64 = 5_000_000_000;
// Larger PTS steps are discontinuities/wraps, not elapsed time.
const MAX_PTS_STEP_NS: i64 = 1_000_000_000;
// Backstops, well inside the driver's cap: per-track frames that did not
// advance PTS (a stuck clock), and total buffered bytes.
const CONFIG_WAIT_FRAMES: u32 = 2048;
const CONFIG_WAIT_BYTES: usize = super::driver::HEADER_BUFFER_CAP_BYTES / 2;

// One track's view of source time, so interleaved tracks far apart in PTS
// neither rebase nor double-count each other.
#[derive(Debug)]
struct TrackClock {
    track: usize,
    max_pts: i64,
    elapsed_ns: i64,
    stalled: u32,
}

/// Tracks how long the header pump has waited on in-band codec configs.
#[derive(Debug, Default)]
pub(crate) struct HeaderGate {
    clocks: Vec<TrackClock>,
    bytes: usize,
    expired: bool,
}

impl HeaderGate {
    /// Account for a frame handed to the caller.
    pub(crate) fn observe(&mut self, frame: &PesFrame) {
        self.account(frame.track, frame.pts, frame.data.len());
    }

    // Per track, elapsed time sums plausible forward PTS steps past the highest
    // PTS seen: small backward steps are reordering; large steps either way rebase.
    fn account(&mut self, track: usize, pts: i64, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        let clock = match self.clocks.iter().position(|c| c.track == track) {
            Some(i) => &mut self.clocks[i],
            None => {
                self.clocks.push(TrackClock {
                    track,
                    max_pts: pts,
                    elapsed_ns: 0,
                    stalled: 0,
                });
                let last = self.clocks.len() - 1;
                &mut self.clocks[last]
            }
        };
        let step = pts.saturating_sub(clock.max_pts);
        if step > 0 && step <= MAX_PTS_STEP_NS {
            clock.elapsed_ns = clock.elapsed_ns.saturating_add(step);
            clock.max_pts = pts;
        } else {
            clock.stalled = clock.stalled.saturating_add(1);
            if step.saturating_abs() > MAX_PTS_STEP_NS {
                clock.max_pts = pts;
            }
        }
        if clock.stalled >= CONFIG_WAIT_FRAMES
            || clock.elapsed_ns >= CONFIG_WAIT_NS
            || self.bytes >= CONFIG_WAIT_BYTES
        {
            self.expired = true;
        }
    }

    /// End of stream: no further frame can supply a missing config.
    pub(crate) fn expire(&mut self) {
        self.expired = true;
    }

    /// Primary video always needs its config; AAC (ASC, mandatory for ADTS-stripped
    /// A_AAC) and LPCM (BD layout byte: playlists say "5.1"; output depth) until the wait expires.
    pub(crate) fn ready(
        &self,
        title: &DiscTitle,
        codec_private: impl Fn(usize) -> Option<Vec<u8>>,
    ) -> bool {
        title.streams.iter().enumerate().all(|(idx, s)| {
            let needs = match s {
                Stream::Video(v) => !v.secondary,
                // Secondary (commentary) AAC is not exempt: without its ASC it is
                // undecodable too, and the wait is bounded anyway.
                Stream::Audio(a) => {
                    (a.codec == Codec::Aac || a.codec == Codec::Lpcm) && !self.expired
                }
                Stream::Subtitle(_) => false,
            };
            !needs || codec_private(idx).is_some()
        })
    }
}

#[cfg(test)]
#[path = "header_gate_tests.rs"]
mod tests;
