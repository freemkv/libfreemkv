//! Shared "keep what decodes, drop what doesn't" bookkeeping for the audio
//! codec parsers.
//!
//! The user's rule: a clean mux keeps every frame it can and drops the ones it
//! can't — video always survives (it's inter-frame predicted; a per-frame drop
//! would cascade, so video resyncs/conceals instead), audio keeps every
//! decodable access unit, and a damaged audio AU is dropped rather than shipped
//! as a decoder-choking glitch.
//!
//! The DETECTION is inherently per-codec — each format carries its own
//! authoritative corruption check (DTS: ffmpeg's core-header parse; AC-3: the
//! header CRC; FLAC: the frame CRC-16; …). This type only carries the UNIFORM
//! response so every audio parser behaves identically:
//!
//! 1. **Count** kept vs dropped AUs and the dropped duration.
//! 2. **Log** every drop (fail-loud, never silent) — a per-drop trace plus a
//!    once-per-track aggregate at `warn` so it surfaces without debug logging.
//! 3. **Whole-track fallback**: once a track is judged mostly undecodable, latch
//!    a poison flag so the remainder is dropped too (a track that damaged isn't
//!    worth muxing).
//!
//! **Sync preservation is the caller's responsibility**, not this type's: the
//! parser must advance its PTS clock across a dropped AU exactly as it would for
//! an emitted one, so a drop becomes a silence gap and never a shift of the
//! following audio. See `DtsParser`'s `stamp_pts` call ordering for the pattern.

/// Minimum access units observed before the whole-track drop verdict can fire.
/// Below this, a short damaged burst can't poison an otherwise-good track.
const TRACK_VERDICT_MIN_AUS: u64 = 200;

/// Per-track drop bookkeeping shared by the audio codec parsers.
pub(crate) struct DropTally {
    /// Static codec label for log lines (e.g. `"dts"`, `"ac3"`).
    codec: &'static str,
    kept: u64,
    dropped: u64,
    /// AUs dropped because they were INDIVIDUALLY verified undecodable (a failed
    /// CRC/header/parity check). Only these feed the whole-track poison verdict.
    /// Distinct from `dropped`, which also counts *collateral* drops — AUs
    /// discarded as a consequence of one corruption (TrueHD's resync-forward run,
    /// or a poisoned track), which must NOT amplify a few real errors into a
    /// false whole-track loss.
    verified_dropped: u64,
    dropped_dur_ns: u64,
    poisoned: bool,
}

impl DropTally {
    pub(crate) fn new(codec: &'static str) -> Self {
        Self {
            codec,
            kept: 0,
            dropped: 0,
            verified_dropped: 0,
            dropped_dur_ns: 0,
            poisoned: false,
        }
    }

    /// Whether the track has been judged too damaged to mux. Once `true`, the
    /// caller should drop every remaining AU (passing them to [`record_drop`]
    /// with a poison reason) rather than emit them.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Access units dropped as undecodable so far — surfaced to the CLI/mux.
    pub(crate) fn dropped_frames(&self) -> u64 {
        self.dropped
    }

    /// Total decoded duration (ns) of dropped AUs — the audio silence introduced.
    pub(crate) fn dropped_duration_ns(&self) -> u64 {
        self.dropped_dur_ns
    }

    /// Record an emitted (decodable) access unit.
    pub(crate) fn record_kept(&mut self) {
        self.kept += 1;
    }

    /// Record a dropped access unit that was INDIVIDUALLY verified undecodable
    /// (a failed CRC/header/parity check). Counts toward the whole-track poison
    /// verdict. `reason` is a short static label for the check that failed.
    pub(crate) fn record_drop(&mut self, pts_ns: i64, dur_ns: i64, bytes: usize, reason: &str) {
        self.verified_dropped += 1;
        self.record_drop_common(pts_ns, dur_ns, bytes, reason);
        self.maybe_poison();
    }

    /// Record a COLLATERAL drop — an AU discarded as a consequence of another
    /// corruption rather than being individually undecodable (TrueHD's
    /// resync-forward run to the next major sync, or an already-poisoned track).
    /// Counted and logged for the drop report, but deliberately does NOT feed the
    /// poison verdict, so one corruption event can't amplify into a false
    /// whole-track loss.
    pub(crate) fn record_collateral_drop(
        &mut self,
        pts_ns: i64,
        dur_ns: i64,
        bytes: usize,
        reason: &str,
    ) {
        self.record_drop_common(pts_ns, dur_ns, bytes, reason);
    }

    fn record_drop_common(&mut self, pts_ns: i64, dur_ns: i64, bytes: usize, reason: &str) {
        self.dropped += 1;
        self.dropped_dur_ns += dur_ns.max(0) as u64;
        tracing::debug!(
            target: "mux",
            "{}: dropped undecodable AU #{} pts_ns={} dur_ns={} bytes={} reason={}",
            self.codec,
            self.dropped,
            pts_ns,
            dur_ns,
            bytes,
            reason
        );
    }

    /// Whole-track fallback: after enough AUs to judge, if more than half were
    /// dropped the track is too damaged to be worth muxing — latch `poisoned`
    /// and log it loudly once. The minimum-sample gate keeps a short damaged
    /// burst from poisoning an otherwise-good track.
    fn maybe_poison(&mut self) {
        if self.poisoned {
            return;
        }
        // Judge on VERIFIED drops vs all AUs seen: a track is only poisoned when
        // a majority of its access units are individually undecodable — not when
        // a couple of corruption events forced long collateral resync runs.
        let total = self.kept + self.dropped;
        if total >= TRACK_VERDICT_MIN_AUS && self.verified_dropped * 2 > total {
            self.poisoned = true;
            tracing::warn!(
                target: "mux",
                "{}: track too damaged to mux — {}/{} AUs individually undecodable (>50%); dropping the whole track",
                self.codec,
                self.verified_dropped,
                total
            );
        }
    }

    /// End-of-stream aggregate report, logged at `warn` so a track's dropped
    /// audio is never hidden even without debug logging. No-op if nothing was
    /// dropped.
    pub(crate) fn log_summary(&self) {
        if self.dropped > 0 {
            tracing::warn!(
                target: "mux",
                "{}: dropped {} undecodable AU(s) totaling {} ns of audio ({} kept)",
                self.codec,
                self.dropped,
                self.dropped_dur_ns,
                self.kept
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_kept_and_dropped() {
        let mut t = DropTally::new("test");
        t.record_kept();
        t.record_drop(0, 1000, 512, "bad");
        t.record_kept();
        assert_eq!(t.dropped_frames(), 1);
        assert_eq!(t.dropped_duration_ns(), 1000);
        assert!(!t.is_poisoned());
    }

    #[test]
    fn poisons_after_min_aus_over_half_dropped() {
        let mut t = DropTally::new("test");
        // 199 AUs, all dropped: below the min-AU gate, must NOT poison yet.
        for _ in 0..199 {
            t.record_drop(0, 1000, 512, "bad");
        }
        assert!(!t.is_poisoned(), "below the 200-AU minimum, no verdict");
        // The 200th drop reaches the minimum with >50% dropped → poison.
        t.record_drop(0, 1000, 512, "bad");
        assert!(t.is_poisoned());
    }

    #[test]
    fn does_not_poison_a_mostly_good_track() {
        let mut t = DropTally::new("test");
        // 400 AUs, 1 dropped: nowhere near 50%.
        t.record_drop(0, 1000, 512, "bad");
        for _ in 0..399 {
            t.record_kept();
        }
        assert!(!t.is_poisoned());
    }
}
