//! B1 drop-to-IRAP gate — keep a muxed elementary stream decode-clean across a
//! mid-stream gap (e.g. an undecryptable unit the mux concealed as NULL TS,
//! P3/A2).
//!
//! When packets are lost, the affected access unit is already dropped at the TS
//! layer (the assembler drops the partial PES on the continuity gap). But for
//! INTER-CODED video the frames that follow reference the lost frame (and each
//! other) until the next IRAP/IDR keyframe — emitting them yields an ffmpeg
//! "missing reference / non-existing PPS" deep-scan error and visibly broken
//! decode. So after a gap on a video track we DROP FORWARD to the next keyframe
//! and resume cleanly there. The gap rounds up to (at most) one GOP — the price
//! of never emitting a dangling reference; it is logged.
//!
//! Audio and subtitle frames are independent (no inter-frame references), so a
//! gap there costs only the single already-dropped frame; the gate is a no-op
//! for non-video tracks (it always admits).

/// Per-track keyframe-resync state. One gate per elementary stream; a video
/// track's gate stays "armed" from a discontinuity until the next keyframe.
#[derive(Debug, Default)]
pub(crate) struct ResyncGate {
    /// True while dropping post-gap inter-coded frames until the next keyframe.
    armed: bool,
    /// Count of frames dropped while armed (for a single summary log on resync).
    dropped: u64,
}

impl ResyncGate {
    pub(crate) fn new() -> Self {
        Self {
            armed: false,
            dropped: 0,
        }
    }

    /// Decide whether a parsed frame should be EMITTED (`true`) or DROPPED
    /// (`false`).
    ///
    /// * `is_video` — inter-coded video track (the only kind with cross-frame
    ///   references); `false` for audio/subtitle, which always admit.
    /// * `discontinuity` — this frame's source PES followed a TS continuity gap.
    /// * `keyframe` — this frame is a self-contained IRAP/IDR.
    ///
    /// A non-video track always admits. A video track arms on a discontinuity
    /// and then drops every non-keyframe until (and excluding the drop of) the
    /// next keyframe, which disarms and is emitted.
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

    /// Whether the gate is currently dropping frames (armed, awaiting keyframe).
    pub(crate) fn is_armed(&self) -> bool {
        self.armed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_video_always_admits_even_on_discontinuity() {
        let mut g = ResyncGate::new();
        // Audio/subtitle: a gap drops only the (already TS-dropped) frame; every
        // frame the parser still emits is independent and must pass.
        assert!(g.admit(false, true, false));
        assert!(g.admit(false, true, false));
        assert!(!g.is_armed(), "non-video never arms");
    }

    #[test]
    fn video_drops_inter_frames_until_next_keyframe() {
        let mut g = ResyncGate::new();
        // Clean run: everything admits.
        assert!(g.admit(true, false, true)); // IDR
        assert!(g.admit(true, false, false)); // P
        // Gap arrives on the next frame (a P referencing lost data) → drop it
        // and every inter frame until the next keyframe.
        assert!(!g.admit(true, true, false), "post-gap P dropped");
        assert!(!g.admit(true, false, false), "still dropping (no key yet)");
        assert!(!g.admit(true, false, false));
        assert_eq!(g.dropped_in_run(), 3);
        // Next keyframe resyncs and is emitted.
        assert!(g.admit(true, false, true), "keyframe resumes the stream");
        assert!(!g.is_armed());
        assert_eq!(g.dropped_in_run(), 0);
        // Back to a clean run.
        assert!(g.admit(true, false, false), "post-resync P admits");
    }

    #[test]
    fn discontinuity_landing_on_a_keyframe_emits_immediately() {
        let mut g = ResyncGate::new();
        // If the first surviving frame after the gap is itself an IRAP, there is
        // nothing to drop — it is self-contained.
        assert!(g.admit(true, true, true), "gap+keyframe emits, no drop");
        assert!(!g.is_armed());
        assert_eq!(g.dropped_in_run(), 0);
    }
}
