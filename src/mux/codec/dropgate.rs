//! Shared "keep what decodes, drop what doesn't" bookkeeping for the audio
//! codec parsers: video always survives a damaged frame, audio drops only
//! individually verified-undecodable AUs.
//!
//! Responsibilities: count kept/dropped AUs and dropped duration; log every
//! drop (per-drop trace plus a once-per-track `warn` aggregate); and latch a
//! poison flag once a track is judged mostly undecodable so the rest drops too.
//!
//! ADTS and MPEG audio (`audio_frames`) count one verified fault per corruption event, so
//! verified faults cannot outnumber kept frames plus runs ended by a gap or before any frame
//! was measured: the verdict fires only for a track with nothing decodable. Per project
//! principle ("rip bad discs": keep a damaged track's genuine frames); do not change without
//! a user decision.

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

    #[cfg(test)]
    pub(crate) fn verified_dropped(&self) -> u64 {
        self.verified_dropped
    }

    /// Add measured lost time (from timestamps) for AUs already counted as dropped.
    pub(crate) fn add_dropped_duration(&mut self, ns: u64) {
        self.dropped_dur_ns += ns;
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

    // Collateral: caused by another AU's corruption (TrueHD resync-forward, or an
    // already-poisoned track), not individually verified undecodable. Deliberately excluded
    // from the poison verdict.
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

    // Whole-track fallback: past the min-AU gate, >50% dropped latches `poisoned` and logs
    // once.
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
#[path = "dropgate_tests.rs"]
mod tests;
