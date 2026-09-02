//! Shared "keep what decodes, drop what doesn't" bookkeeping for the audio
//! codec parsers: video always survives a damaged frame, audio drops only
//! individually verified-undecodable AUs.
//!
//! Responsibilities: count kept/dropped AUs and dropped duration; log every
//! drop (per-drop trace plus a once-per-track `warn` aggregate); and latch a
//! poison flag once a track is judged mostly undecodable so the rest drops too.
//!
//! See `docs/dropgate.md` for full rationale (per-codec detection, sync
//! preservation, TrueHD collateral drops).

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
    /// caller should drop every remaining AU (passing them to [`Self::record_drop`]
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

    // Collateral: caused by another AU's corruption (TrueHD resync-forward, or
    // an already-poisoned track), not individually verified undecodable.
    // Deliberately excluded from the poison verdict — see docs/dropgate.md.
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

    // Whole-track fallback: past the min-AU gate, >50% dropped latches `poisoned`
    // and logs once. See docs/dropgate.md for the full rationale.
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

    // Puts the kept count on the critical path (unlike the test above, which
    // never exercises it): losing it would silently discard a healthy track.
    // See docs/dropgate.md for the full rationale.
    #[test]
    fn interleaved_keeps_are_in_the_poison_denominator() {
        let mut t = DropTally::new("test");
        // 2 kept per 1 dropped, well past the minimum-AU gate: a third of the
        // track is undecodable, which is bad but nowhere near the >50% threshold.
        for _ in 0..(TRACK_VERDICT_MIN_AUS * 3) {
            t.record_kept();
            t.record_kept();
            t.record_drop(0, 1000, 512, "bad");
            assert!(
                !t.is_poisoned(),
                "33% dropped must never poison, at any point in the run"
            );
        }
        assert_eq!(t.dropped_frames(), TRACK_VERDICT_MIN_AUS * 3);
    }

    #[test]
    fn collateral_drops_never_poison_the_track() {
        // A TrueHD resync-forward run collaterally drops a long burst of AUs, but
        // none are individually undecodable — the whole-track verdict must stay
        // clean so one corruption event can't amplify into a false total loss.
        let mut t = DropTally::new("test");
        for _ in 0..(TRACK_VERDICT_MIN_AUS * 3) {
            t.record_collateral_drop(0, 1000, 512, "resync-forward");
        }
        assert!(t.dropped_frames() >= TRACK_VERDICT_MIN_AUS, "drops counted");
        assert!(!t.is_poisoned(), "collateral drops must not poison");
    }
}
