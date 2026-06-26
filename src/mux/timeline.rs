//! Shared clip-boundary timeline-continuity corrector.
//!
//! A BD/UHD title's clips are read as one concatenated sector stream (clip
//! boundaries / mpls connection_condition are not plumbed to the mux), so at a
//! non-seamless boundary the source PES PTS jumps backward. Left uncorrected,
//! that produces a sustained band of non-monotonic block timestamps. Every
//! muxer/sink that consumes the interleaved per-track PES stream and emits a
//! monotonic timeline (the MKV muxer, the `demux://` elementary-stream sink)
//! uses [`TimelineContinuity`] so the correction lives in exactly one place.

/// A backward PTS step larger than this is treated as a clip-boundary
/// discontinuity (a non-seamless BD clip / dual-layer-break where the source
/// PES PTS resets), NOT as B-frame reorder. HEVC/H.264 reorder depth tops out
/// around 16 frames (<1s at 24 fps); 3s sits comfortably above any legitimate
/// reorder window and far below any real clip's duration, so it never
/// false-triggers within a clip.
pub(crate) const DISCONTINUITY_BACKSTEP_NS: i64 = 3_000_000_000;
/// Sub-frame gap inserted after a rebased discontinuity so the first frame of
/// the new clip lands strictly after the previous timeline high (1 ms).
pub(crate) const DISCONTINUITY_GAP_NS: i64 = 1_000_000;

/// Global timeline-continuity corrector. freemkv reads a BD title's clips as
/// one concatenated sector stream (clip boundaries / mpls connection_condition
/// are not plumbed to the mux), so at a non-seamless boundary the source PES
/// PTS jumps backward. Left uncorrected, that produces a sustained band of
/// non-monotonic block timestamps (ffmpeg then derives non-monotonic DTS).
///
/// A single running `offset_ns` is applied to EVERY track, so the concatenated
/// clips form one monotonic timeline AND A/V sync is preserved (all tracks at a
/// boundary shift by the same amount). It is global, not per-track: a clip
/// boundary resets every stream together by the same delta.
///
/// **Only the VIDEO track drives epoch decisions.** A title carries one video
/// track plus many interleaved audio + subtitle tracks (Top Gun UHD: 2 video,
/// 11 audio, 32 PGS). Those non-video tracks are sparse and lag the video by
/// seconds, so their raw PTS swing well over the 3 s discontinuity threshold
/// against a shared frontier even within a SINGLE clip — a late subtitle PTS
/// would ratchet `high_ns` up, then the next normal video frame would sit >3 s
/// below it and be misread as a clip boundary, permanently bumping `offset_ns`.
/// That false-positive ratchet (firing thousands of times on a one-clip title)
/// inflated Top Gun's cluster/Cue timestamps into the billions of ms and
/// destroyed its seek index. The clip-boundary INFERENCE is therefore keyed on
/// video PTS alone: video establishes and advances the frontier and is the only
/// track that can open a new epoch. Non-video frames are remapped under the
/// CURRENT offset and never touch the frontier or the offset — they ride the
/// timeline the video defines, preserving A/V sync (all tracks at a boundary
/// shift by the same delta) without ever triggering a rebase themselves.
///
/// The demuxer interleaves the tracks, so at a real (multi-clip) boundary the
/// streams do NOT all reset on the same frame — a lagging audio/PGS frame from
/// the just-ended clip's tail can arrive AFTER the next clip's video has already
/// reset the epoch. Such a "straggler" carries an old-epoch raw PTS; adding the
/// new (clip-sized) offset to it would fling it far past the frontier and force
/// a forward-dated split cluster. A non-video frame whose mapped position lands
/// more than a backstep past the frontier is therefore clamped to the frontier
/// (the seam) — it never perturbs the offset or the frontier and never
/// forward-dates a cluster. Genuine multi-clip seamless rebasing (the design
/// that is correct for real HEVC/H.264 multi-clip titles) is preserved: it is
/// the video back-jump that opens a new epoch, exactly as before.
pub(crate) struct TimelineContinuity {
    /// Offset (ns) added to raw PTS for the CURRENT epoch.
    pub(crate) offset_ns: i64,
    /// Offset (ns) of the immediately previous epoch — used to recognise and
    /// remap a non-video tail straggler at a boundary (an old-epoch frame whose
    /// current-offset mapping flies forward but whose previous-offset mapping
    /// lands at the seam). Equals `offset_ns` until the first boundary.
    pub(crate) prev_offset_ns: i64,
    /// Highest adjusted VIDEO PTS (ns) accepted onto the timeline so far — the
    /// running frontier. `None` until the first video frame. Only video advances
    /// it; non-video tracks never touch it.
    pub(crate) high_ns: Option<i64>,
}

impl TimelineContinuity {
    pub(crate) fn new() -> Self {
        Self {
            offset_ns: 0,
            prev_offset_ns: 0,
            high_ns: None,
        }
    }

    /// Map a raw PES PTS (ns) onto the continuous output timeline.
    ///
    /// `drives_epoch` gates EVERY epoch decision. It is `true` for the PRIMARY
    /// video track (base layer, track 0) ONLY. Every other track — audio, PGS
    /// subtitle, and a second video track such as a Dolby Vision enhancement
    /// layer — passes `false` and is a passive rider. (The DV EL is video but
    /// runs its own PTS timeline interleaved with the base layer's; letting it
    /// drive epochs would false-trigger a reset on every GOP.)
    ///
    /// **Passive tracks** (`drives_epoch == false`). Always remapped under the
    /// CURRENT offset. They never advance `high_ns`, never trigger a clip-boundary
    /// reset, and never bump `offset_ns`. This is what kills the single-clip
    /// ratchet: a sparse/lagging subtitle/audio PTS, or an interleaved EL frame,
    /// can no longer push the frontier up and make the next base-video frame look
    /// like a boundary. A/V sync is preserved because the offset they ride is the
    /// same one the base video established for the epoch.
    ///
    /// **Primary video** (`drives_epoch == true`):
    /// - **Backward jump > `DISCONTINUITY_BACKSTEP_NS`** vs the frontier =
    ///   clip-boundary reset: open a new epoch (bump the offset so this frame
    ///   continues just after the frontier). This is the genuine multi-clip
    ///   seamless rebasing, now driven only by real base-video back-jumps.
    /// - **Everything else** (normal progression + sub-threshold B-frame reorder
    ///   dips) passes through with the current offset and advances the frontier,
    ///   preserving PTS.
    pub(crate) fn adjust(&mut self, raw_pts_ns: i64, drives_epoch: bool) -> i64 {
        // Passive track: ride the current epoch's offset. Never advance the
        // frontier and never open an epoch — these tracks each run on their own
        // (sparse/laggy/independent) timeline and would false-trigger the ratchet.
        if !drives_epoch {
            let mapped = raw_pts_ns.saturating_add(self.offset_ns);
            // Tail-straggler remap: at a REAL (base-video-driven) multi-clip
            // boundary the offset has just jumped forward by ~a whole clip, but a
            // lagging tail frame from the just-ended clip still carries an
            // OLD-epoch raw PTS. Adding the NEW offset flings it ~a clip past the
            // frontier and would force a forward-dated split cluster (breaking
            // cluster monotonicity). Such a straggler is recognised precisely: its
            // current-offset mapping lands more than a backstep PAST the frontier
            // AND its PREVIOUS-offset mapping lands at/below the frontier (i.e. it
            // belongs to the prior epoch). Remap it with the previous offset so
            // it lands at its true seam position. This is what distinguishes a
            // tail straggler from a frame that legitimately runs ahead of the
            // (base-video-only) frontier — a long audio-only tail, a sparse
            // subtitle, or an EL frame — which is left on the current offset.
            if let Some(high) = self.high_ns {
                if mapped > high + DISCONTINUITY_BACKSTEP_NS {
                    let prev_mapped = raw_pts_ns.saturating_add(self.prev_offset_ns);
                    if prev_mapped <= high {
                        return prev_mapped;
                    }
                }
            }
            return mapped;
        }

        let Some(high) = self.high_ns else {
            let adj = raw_pts_ns.saturating_add(self.offset_ns);
            self.high_ns = Some(adj);
            return adj;
        };
        let adj = raw_pts_ns.saturating_add(self.offset_ns);
        if adj < high - DISCONTINUITY_BACKSTEP_NS {
            // Clip-boundary reset (real multi-clip seam): continue just after the
            // frontier. Save the previous offset so a lagging non-video tail
            // frame can be recognised and remapped to the seam (see above).
            self.prev_offset_ns = self.offset_ns;
            let bump = (high - adj).saturating_add(DISCONTINUITY_GAP_NS);
            self.offset_ns = self.offset_ns.saturating_add(bump);
            let adj2 = raw_pts_ns.saturating_add(self.offset_ns);
            self.high_ns = Some(high.max(adj2));
            adj2
        } else {
            // Normal progression / sub-threshold B-frame reorder: keep true PTS.
            self.high_ns = Some(high.max(adj));
            adj
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000_000_000; // 1 second in ns

    // Convenience: a video frame drives epoch decisions; non-video rides the
    // current offset. These wrappers make the test intent explicit.
    fn adj_video(tc: &mut TimelineContinuity, p: i64) -> i64 {
        tc.adjust(p, true)
    }
    fn adj_other(tc: &mut TimelineContinuity, p: i64) -> i64 {
        tc.adjust(p, false)
    }

    /// Characterization of the BUG: a BD title's two clips concatenated with a
    /// PTS reset at the boundary. WITHOUT correction the raw VIDEO timeline goes
    /// hard backward at clip 2 (what produced the non-monotonic-DTS band on
    /// Dune / Top Gun). WITH `TimelineContinuity` the output is monotonic and
    /// continuous across the boundary. The boundary is driven by VIDEO.
    #[test]
    fn continuity_rebases_clip_boundary_reset() {
        // Clip1 video rising to 10s, then clip2 RESETS near 0 — non-seamless.
        let clip1: Vec<i64> = (0..=10).map(|i| i * S).collect(); // 0..10s
        let clip2: Vec<i64> = (0..=10).map(|i| i * S).collect(); // resets to 0..10s
        let raw: Vec<i64> = clip1.iter().chain(clip2.iter()).copied().collect();

        // Uncorrected (the bug): the sequence is NOT monotonic — clip2's first
        // frame (0) is 10s below clip1's last (10s).
        assert!(
            raw.windows(2).any(|w| w[1] < w[0]),
            "precondition: raw clip-reset sequence is non-monotonic"
        );

        // Corrected: strictly non-decreasing, and clip2 continues AFTER clip1.
        let mut tc = TimelineContinuity::new();
        let out: Vec<i64> = raw.iter().map(|&p| adj_video(&mut tc, p)).collect();
        assert!(
            out.windows(2).all(|w| w[1] >= w[0]),
            "corrected timeline must be monotonic non-decreasing, got {out:?}"
        );
        // Clip2's first frame lands just after clip1's last (10s) + the gap.
        assert_eq!(out[11], 10 * S + DISCONTINUITY_GAP_NS);
        // Clip2's last frame is offset by the whole of clip1, not back near 0.
        assert!(out[21] > 19 * S);
    }

    /// Regression guard: NORMAL B-frame reorder (a small backward dip, well
    /// under the discontinuity threshold) on VIDEO must pass through UNCHANGED.
    #[test]
    fn continuity_preserves_bframe_reorder() {
        let mut tc = TimelineContinuity::new();
        // I, P(+3 frames), B, B, B — presentation PTS dips backward by ~2
        // frames (~83ms), far under the 3s threshold.
        let raw = [0i64, 125_000_000, 42_000_000, 83_000_000, 250_000_000];
        let out: Vec<i64> = raw.iter().map(|&p| adj_video(&mut tc, p)).collect();
        assert_eq!(out, raw, "B-frame reorder must pass through unchanged");
        assert_eq!(tc.offset_ns, 0, "no rebase for sub-threshold reorder");
    }

    /// A legitimate FORWARD gap (a real timing gap within a clip) on VIDEO must
    /// be PRESERVED, not clamped — only backward video clip-boundary jumps are
    /// rebased.
    #[test]
    fn continuity_preserves_forward_gap() {
        let mut tc = TimelineContinuity::new();
        let raw = [0i64, S, 2 * S + 500_000_000, 4 * S]; // a 1.5s gap mid-stream
        let out: Vec<i64> = raw.iter().map(|&p| adj_video(&mut tc, p)).collect();
        assert_eq!(out, raw, "forward gap preserved verbatim");
        assert_eq!(tc.offset_ns, 0, "no rebase on forward progression");
    }

    /// PRIMARY rc3 regression: a sparse, lagging NON-VIDEO track (PGS subtitle /
    /// trailing audio) on a SINGLE-clip title must NOT inflate `offset_ns`. This
    /// is the exact false-positive that destroyed Top Gun's seek index: with a
    /// shared frontier, a late subtitle PTS ratcheted the frontier up, then the
    /// next normal video frame sat >3s below it and was misread as a clip
    /// boundary, permanently bumping the offset — thousands of times, until the
    /// Cue/cluster timestamps inflated into the billions of ms.
    ///
    /// Correct behaviour: non-video frames ride the current offset and NEVER
    /// touch the frontier or the offset, so no amount of subtitle/audio lag can
    /// trigger a rebase on a one-clip title.
    #[test]
    fn single_clip_late_subtitle_does_not_inflate_offset() {
        let mut tc = TimelineContinuity::new();
        // One continuous clip: video advances steadily 0..60s.
        // Interleaved, a subtitle track is sparse — it emits a cue at 0s, then
        // nothing for a long stretch, then a late cue, then jumps around. Each
        // subtitle PTS swings many seconds against the video frontier.
        // Drive a realistic interleave.
        let mut max_out = i64::MIN;
        for sec in 0..=60 {
            // Video frame every second.
            let v = adj_video(&mut tc, sec * S);
            max_out = max_out.max(v);
            // Every 7th second, a subtitle appears whose raw PTS lags the video
            // frontier by ~5s (a late display-set delivered by the interleaver)
            // — far more than the 3s discontinuity threshold.
            if sec % 7 == 0 && sec >= 7 {
                let sub_raw = (sec - 5) * S;
                let s = adj_other(&mut tc, sub_raw);
                // The subtitle maps under the current (zero) offset, near its
                // true time — it does NOT fling the timeline forward.
                assert_eq!(s, sub_raw, "subtitle rides the current offset");
            }
        }
        // The crux: a single-clip title must NEVER open an epoch. Offset stays 0
        // and the timeline never inflates.
        assert_eq!(
            tc.offset_ns, 0,
            "single-clip interleave must not ratchet offset (was {})",
            tc.offset_ns
        );
        // And the video frontier is exactly 60s — not billions.
        assert_eq!(tc.high_ns, Some(60 * S), "frontier tracks video only");
        assert!(max_out <= 60 * S, "no timeline inflation, max={max_out}");
    }

    /// PRIMARY rc3 regression (Dolby Vision dual-layer): a SECOND video track —
    /// the DV enhancement layer — runs its OWN PTS timeline interleaved with the
    /// base layer's, so the two video PTS sequences OVERLAP. The EL must be a
    /// PASSIVE rider (drives_epoch == false): if it drove epochs, every EL GOP
    /// would look like a multi-second backward jump against the base-layer
    /// frontier and false-trigger a clip-boundary reset — the exact ratchet that
    /// inflated Top Gun's 1-clip 1h49m timeline to ~7 h. Here the base layer
    /// advances 0..60s while the EL re-emits the SAME 0..60s interleaved; the
    /// timeline must stay at 60s with offset 0.
    #[test]
    fn dv_enhancement_layer_does_not_drive_epochs() {
        let mut tc = TimelineContinuity::new();
        let mut max_out = i64::MIN;
        for sec in 0..=60 {
            // Base layer (track 0) drives the epoch.
            let bl = adj_video(&mut tc, sec * S);
            // EL (track 1) re-emits the same time — a passive rider. Its raw PTS
            // equals the base layer's, but it arrives just AFTER the base frame
            // for the NEXT second sometimes; simulate the overlap by feeding the
            // PREVIOUS second's time, which is a backward swing vs the frontier.
            let el_raw = if sec > 0 { (sec - 1) * S } else { 0 };
            let el = adj_other(&mut tc, el_raw);
            assert_eq!(el, el_raw, "EL rides current offset, true PTS preserved");
            max_out = max_out.max(bl).max(el);
        }
        assert_eq!(
            tc.offset_ns, 0,
            "DV EL interleave must not ratchet offset (was {})",
            tc.offset_ns
        );
        assert_eq!(tc.high_ns, Some(60 * S), "frontier tracks base video only");
        assert!(max_out <= 60 * S, "no timeline inflation, max={max_out}");
    }

    /// Companion: a non-video frame must never ADVANCE the frontier. Even a
    /// non-video PTS far ABOVE the current video frontier (a subtitle/audio
    /// timestamp that leads the video momentarily) leaves `high_ns` untouched,
    /// so a subsequent normal video frame is not misread as a boundary.
    #[test]
    fn non_video_never_advances_frontier() {
        let mut tc = TimelineContinuity::new();
        adj_video(&mut tc, 0);
        adj_video(&mut tc, 5 * S);
        let frontier = tc.high_ns.unwrap();
        // A subtitle leading the video by 20s.
        let s = adj_other(&mut tc, 25 * S);
        assert_eq!(s, 25 * S, "non-video maps under current offset");
        assert_eq!(
            tc.high_ns.unwrap(),
            frontier,
            "non-video must NOT advance the frontier"
        );
        // The next normal video frame (6s) is well below 25s but is NOT treated
        // as a boundary, because the frontier is still 5s (video-only).
        let v = adj_video(&mut tc, 6 * S);
        assert_eq!(v, 6 * S, "video continues normally, no false boundary");
        assert_eq!(
            tc.offset_ns, 0,
            "no rebase triggered by the leading subtitle"
        );
    }

    /// Regression for the original Top Gun band: a LARGE, real-magnitude
    /// clip-boundary back-jump on VIDEO (clip 1 ≈ 13 min, clip 2 resets to 0)
    /// must STILL be rebased to one continuous monotonic timeline — the genuine
    /// multi-clip seamless behaviour is preserved, now keyed on real video
    /// back-jumps.
    #[test]
    fn continuity_large_clip_boundary_backjump_rebased() {
        let mut tc = TimelineContinuity::new();
        // Clip 1: 0 .. 780s (13 min) at 1s steps.
        let clip1: Vec<i64> = (0..=780).map(|i| i * S).collect();
        // Clip 2: resets to 0 .. 120s — the ~ -780s discontinuity.
        let clip2: Vec<i64> = (0..=120).map(|i| i * S).collect();
        let mut last = i64::MIN;
        let mut max = i64::MIN;
        for &p in clip1.iter().chain(clip2.iter()) {
            let a = adj_video(&mut tc, p);
            assert!(
                a >= last,
                "rebased timeline must be monotonic, got {a} < {last}"
            );
            last = a;
            max = max.max(a);
        }
        // Offset ≈ the whole of clip 1 (one boundary, no ratchet).
        assert_eq!(tc.offset_ns, 780 * S + DISCONTINUITY_GAP_NS);
        // Timeline spans clip1+clip2 (~900s), proving clip 2 is reachable past
        // the boundary — not capped at it, and not ratcheted far beyond.
        assert!(
            (900 * S..901 * S).contains(&max),
            "timeline must span ~900s (clip1+clip2), got {max}"
        );
    }

    /// At a REAL video-driven boundary, a lagging NON-VIDEO tail frame from the
    /// just-ended clip (an old-epoch raw PTS arriving interleaved after the
    /// reset) must be REMAPPED to its true seam position with the PREVIOUS
    /// offset — not flung ~a clip past the frontier by the freshly-bumped
    /// offset. Otherwise it would force a forward-dated split cluster and break
    /// cluster monotonicity.
    #[test]
    fn non_video_straggler_remapped_to_seam_at_boundary() {
        let mut tc = TimelineContinuity::new();
        // Clip1 video rises to 600s.
        for i in 0..=600 {
            adj_video(&mut tc, i * S);
        }
        let frontier = tc.high_ns.unwrap();
        assert_eq!(frontier, 600 * S);
        // Clip2 video resets to 0 → boundary, offset bumps by ~600s.
        let c2 = adj_video(&mut tc, 0);
        assert_eq!(c2, 600 * S + DISCONTINUITY_GAP_NS);
        // Straggler: clip1's tail audio (raw 599.5s) arrives now. Under the new
        // offset it would map to ~1199.5s; it must instead remap with the
        // previous (zero) offset to its true seam position 599.5s.
        let straggler_raw = 599 * S + 500_000_000;
        let straggler = adj_other(&mut tc, straggler_raw);
        assert_eq!(
            straggler, straggler_raw,
            "straggler must remap to its seam position via the previous offset"
        );
        assert!(
            straggler <= frontier,
            "straggler must land at/below the frontier, got {straggler}"
        );
        // It must NOT have perturbed the offset or the frontier.
        assert_eq!(
            tc.high_ns.unwrap(),
            c2,
            "straggler must not move the frontier"
        );
        // A NORMAL clip2 audio frame (raw ~1s, current epoch) is NOT remapped —
        // it rides the new offset to ~601s, just past the frontier but within a
        // backstep (its previous-offset mapping ~1s is below the frontier but the
        // current-offset mapping is not a backstep past it, so it is not treated
        // as a straggler).
        let normal = adj_other(&mut tc, S);
        assert_eq!(normal, S + 600 * S + DISCONTINUITY_GAP_NS);
    }
}
