//! B1 drop-to-IRAP gate — keep a muxed elementary stream decode-clean across a
//! mid-stream gap (e.g. an undecryptable unit the mux concealed as NULL TS).
//!
//! After a packet-loss gap, inter-coded video frames referencing the lost
//! frame would fault a decoder, so this gate drops forward past a gap on a
//! video track until the next IRAP/IDR keyframe and resumes there.
//!
//! Audio/subtitle frames are independently decodable, so the gate is a no-op for them;
//! TrueHD/MLP is the one exception and is handled in `codec::truehd` instead.

/// Per-track keyframe-resync state. One gate per elementary stream; a video
/// track's gate stays "armed" from a discontinuity until the next keyframe.
#[derive(Debug, Default)]
pub(crate) struct ResyncGate {
    /// True while dropping post-gap inter-coded frames until the next keyframe.
    armed: bool,
    /// Count of frames dropped in the CURRENT armed run. Reset at each resync,
    /// so it answers "how expensive was this gap" and nothing else.
    dropped: u64,
    /// Every frame this gate has ever dropped, across all runs. NOT reset on
    /// resync.
    ///
    /// `dropped` alone cannot report loss: it is zeroed the moment a keyframe
    /// disarms the gate, so a mid-title gap that resolves before EOF leaves no
    /// trace at all. That is the common case — most gaps do resolve — which
    /// made concealed video loss invisible to every consumer.
    dropped_total: u64,
    /// The run's loss counters, bumped per dropped frame.
    stats: Option<std::sync::Arc<crate::ctx::Stats>>,
}

impl ResyncGate {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    // A gate that also counts its drops into the run's `stats`.
    pub(crate) fn counted(stats: std::sync::Arc<crate::ctx::Stats>) -> Self {
        Self {
            stats: Some(stats),
            ..Self::default()
        }
    }

    // Decide whether a parsed frame should be EMITTED (`true`) or DROPPED (`false`). Video arms
    // on discontinuity, drops until the next keyframe.
    pub(crate) fn admit(&mut self, is_video: bool, discontinuity: bool, keyframe: bool) -> bool {
        if !is_video {
            return true;
        }
        if discontinuity {
            self.armed = true;
        }
        if self.armed {
            if keyframe {
                self.armed = false;
                self.dropped = 0;
                true
            } else {
                self.dropped += 1;
                self.dropped_total += 1;
                if let Some(s) = &self.stats {
                    s.add_resync_dropped(1);
                }
                false
            }
        } else {
            true
        }
    }

    /// Frames dropped so far in the CURRENT armed run (0 when not armed / just
    /// resynced). Lets the consumer log the resync cost once at the keyframe.
    pub(crate) fn dropped_in_run(&self) -> u64 {
        self.dropped
    }

    /// Every frame this gate has dropped, across all armed runs. Survives
    /// resync, so it is the number a caller reports loss from.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.dropped_total
    }

    /// Whether the gate is currently dropping frames (armed, awaiting keyframe).
    pub(crate) fn is_armed(&self) -> bool {
        self.armed
    }
}

#[cfg(test)]
#[path = "resync_tests.rs"]
mod tests;
