//! Shared clip-boundary timeline corrector.
//!
//! A BD/UHD title's clips are read as one concatenated sector stream, so the
//! source PES PTS does not run continuously across a clip join. [`SeamPlan`]
//! places frames exactly from the playlist's marks when present;
//! [`TimelineContinuity::adjust`] infers seams from PTS jumps otherwise, and
//! [`TimelineContinuity::map`] picks between them so every muxer/sink shares
//! one correction path. See docs/mux-timeline.md for the full rationale.

// A backward PTS step larger than this is a clip-boundary discontinuity
// (source PES PTS reset), NOT B-frame reorder (HEVC/H.264 tops out ~16
// frames, <1s @24fps). See docs/mux-timeline.md#discontinuity_backstep_ns.
pub(crate) const DISCONTINUITY_BACKSTEP_NS: i64 = 3_000_000_000;
/// Sub-frame gap inserted after a rebased discontinuity so the first frame of
/// the new clip lands strictly after the previous timeline high (1 ms).
pub(crate) const DISCONTINUITY_GAP_NS: i64 = 1_000_000;

// How close a frame's PTS must be to a clip's IN mark to be recognised as
// that clip's opening frame, vs. the previous clip's overlapping tail. See
// docs/mux-timeline.md#clip_start_tolerance_ns.
pub(crate) const CLIP_START_TOLERANCE_NS: i64 = 250_000_000;

/// MPLS 45 kHz tick → nanoseconds. PlayItem `in_time`/`out_time` are 45 kHz
/// (`disc::Clip`, and `disc/bluray.rs` divides by 45000.0 for the same reason).
fn mpls_ticks_to_ns(ticks: u32) -> i64 {
    // 1e9 / 45_000 = 22_222.22…, so scale first and divide once to avoid
    // accumulating a per-clip rounding error across an 11-clip title.
    (ticks as i64).saturating_mul(1_000_000_000) / 45_000
}

/// One clip's placement on the output timeline, derived from its PlayItem marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeamClip {
    /// Clip IN mark in the shared source clock (ns).
    pub(crate) in_ns: i64,
    /// Clip OUT mark in the shared source clock (ns).
    pub(crate) out_ns: i64,
    /// Added to a raw PTS inside this clip to place it on the output timeline.
    /// Equals (sum of every earlier clip's playable duration) − `in_ns`.
    pub(crate) offset_ns: i64,
    // Byte range this clip occupies in the title's feed, when known. A byte
    // offset resolves an overlap that timestamps alone cannot. See
    // docs/mux-timeline.md#seamclipfeed_span.
    pub(crate) feed_span: Option<(u64, u64)>,
}

// The playlist's own answer to "where does each clip belong on the
// timeline": a seamless-branching title's PlayItems can overlap or skip in
// the shared clock. See docs/mux-timeline.md#seamplan-struct-doc.
pub(crate) struct SeamPlan {
    // Whether the per-clip feed spans tile the feed contiguously from 0, so
    // a byte offset can be trusted to identify a clip. If not, provenance is
    // disabled. See docs/mux-timeline.md#seamplanspans_trusted.
    spans_trusted: bool,
    clips: Vec<SeamClip>,
    // Frames dropped (per track) for falling outside every clip's marks.
    // Counted so an unexpected volume is visible instead of silent. See
    // docs/mux-timeline.md#seamplandropped.
    dropped: Vec<u64>,
    // Per-track position: (clip index, last raw PTS seen). Each track
    // crosses a join on its OWN frame, since overlap tails arrive after the
    // next clip's video. See docs/mux-timeline.md#seamplancursors.
    cursors: Vec<TrackPos>,
}

/// Where one track currently sits in the clip list.
#[derive(Debug, Clone, Copy)]
struct TrackPos {
    clip: usize,
    last_raw_ns: Option<i64>,
    /// The last OUTPUT timestamp emitted for this track.
    ///
    /// The placement rules are heuristics over marks; this is the invariant
    /// they exist to serve — a track's output must never run backwards. Three
    /// successive audit rounds found a different hole in the heuristics, each
    /// silently losing content or rewinding, so the invariant is now checked
    /// directly rather than inferred from which rule happened to fire.
    last_out_ns: Option<i64>,
}

impl SeamPlan {
    // Build a plan, or `None` when there's nothing to place (no clips, or
    // unusable marks). A single clip still gets a plan, to trim to
    // `[in, out]`. See docs/mux-timeline.md#seamplanfrom_clips.
    pub(crate) fn from_clips(clips: &[crate::disc::Clip]) -> Option<Self> {
        if clips.is_empty() {
            return None;
        }
        // Trust the spans only if they tile the feed contiguously from 0 (a repeated
        // clip reusing its first span is fine). Any gap/overlap/missing span/nonzero
        // start means scan and mux disagree, so a byte offset would pick the wrong clip.
        let mut spans_trusted = true;
        let mut expect: u64 = 0;
        let mut prev_span: Option<(u64, u64)> = None;
        for c in clips {
            match c.feed_span {
                Some(sp) if Some(sp) == prev_span => {}
                Some((s, e)) if s == expect && e > s => {
                    expect = e;
                    prev_span = Some((s, e));
                }
                _ => {
                    spans_trusted = false;
                    break;
                }
            }
        }
        if !spans_trusted {
            tracing::debug!(
                target: "freemkv::mux",
                "clip feed spans do not tile the title's feed; placing by marks instead"
            );
        }

        let mut out: Vec<SeamClip> = Vec::with_capacity(clips.len());
        let mut cum: i64 = 0;
        for c in clips {
            let in_ns = mpls_ticks_to_ns(c.in_time);
            let out_ns = mpls_ticks_to_ns(c.out_time);
            if out_ns <= in_ns {
                tracing::info!(
                    target: "freemkv::mux",
                    in_ns, out_ns,
                    "no seam plan: a clip's marks are empty or inverted"
                );
                return None;
            }
            // Non-advancing marks across clips are normal (each clip has its own STC), so
            // timestamp inference refuses them, but provenance places by byte offset +
            // each clip's `offset_ns`, needing marks only to test `[in, out]` membership.
            if !spans_trusted
                && let Some(prev) = out.last()
                && in_ns <= prev.in_ns
            {
                tracing::info!(
                    target: "freemkv::mux",
                    "no seam plan: the clips' marks do not advance and the feed \
                     spans cannot be trusted, so nothing can place them"
                );
                return None;
            }
            out.push(SeamClip {
                in_ns,
                out_ns,
                offset_ns: cum.saturating_sub(in_ns),
                feed_span: c.feed_span,
            });
            cum = cum.saturating_add(out_ns - in_ns);
        }
        // Log both flags once here: which placement strategy a title got was
        // previously invisible, and `distinct < clips` reveals when clips share a
        // feed span (a seamlessly branched title re-referencing one clip file).
        let mut distinct_spans = 0usize;
        let mut seen: Option<(u64, u64)> = None;
        for c in &out {
            if c.feed_span != seen {
                distinct_spans += 1;
                seen = c.feed_span;
            }
        }

        tracing::info!(
            target: "freemkv::mux",
            clips = out.len(),
            distinct_spans,
            spans_trusted,
            total_ns = cum,
            "seam plan built"
        );

        Some(Self {
            spans_trusted,
            clips: out,
            cursors: Vec::new(),
            dropped: Vec::new(),
        })
    }

    /// How many frames this track has had dropped for falling outside every
    /// clip's marks.
    #[cfg(test)]
    pub(crate) fn dropped_for(&self, track: usize) -> u64 {
        self.dropped.get(track).copied().unwrap_or(0)
    }

    /// Frames dropped across every track.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.dropped.iter().fold(0u64, |a, b| a.saturating_add(*b))
    }

    /// Total playable duration (ns) — the sum of every clip's `out − in`. This
    /// is the length the title actually is, and what the output timeline must
    /// end at.
    #[cfg(test)]
    pub(crate) fn total_ns(&self) -> i64 {
        self.clips
            .iter()
            .map(|c| c.out_ns - c.in_ns)
            .fold(0i64, |a, b| a.saturating_add(b))
    }

    // Which clip owns feed byte `b`, by BINARY SEARCH (spans tile the feed
    // contiguously, so a scan would be ~900 comparisons per frame on a large
    // disc). See docs/mux-timeline.md#seamplanclip_at_byte.
    fn clip_at_byte(&self, b: u64) -> Option<usize> {
        let i = self
            .clips
            .partition_point(|c| c.feed_span.is_some_and(|(s, _)| s <= b));
        let cand = i.checked_sub(1)?;
        // Walk back over any earlier entries sharing this span (a repeated
        // clip) so the answer is stable and is always the first of them.
        let (s, e) = self.clips[cand].feed_span?;
        if b < s || b >= e {
            return None;
        }
        let mut first = cand;
        while first > 0 && self.clips[first - 1].feed_span == Some((s, e)) {
            first -= 1;
        }
        Some(first)
    }

    // Pick the member of a shared-span run whose marks contain `raw_ns`
    // (several PlayItems can reference one file). Falls back to `first`.
    // See docs/mux-timeline.md#seamplanclip_in_run_for.
    fn clip_in_run_for(&self, first: usize, raw_ns: i64) -> usize {
        let span = self.clips[first].feed_span;
        let mut i = first;
        while i < self.clips.len() && self.clips[i].feed_span == span {
            let c = self.clips[i];
            if raw_ns >= c.in_ns && raw_ns <= c.out_ns {
                return i;
            }
            i += 1;
        }
        first
    }

    /// Place a raw PTS for `track`, advancing that track's own clip cursor.
    /// `None` means DROP: the frame lies outside every clip's marks, so the
    /// playlist does not include it.
    fn place(
        &mut self,
        raw_ns: i64,
        track: usize,
        has_reorder: bool,
        src_byte: Option<u64>,
    ) -> Option<i64> {
        if self.cursors.len() <= track {
            self.cursors.resize(
                track + 1,
                TrackPos {
                    clip: 0,
                    last_raw_ns: None,
                    last_out_ns: None,
                },
            );
            self.dropped.resize(track + 1, 0);
        }
        let pos = self.cursors[track];
        let mut clip = pos.clip;

        // Advance on a SKIP (past this OUT) or an OVERLAP (backward PTS step to next
        // clip's IN, since PTS only runs forward within a clip) — per track. Provenance
        // (byte offset) beats inference for overlaps; heuristics below are the fallback.
        if self.spans_trusted
            && let Some(b) = src_byte
            && let Some(found) = self.clip_at_byte(b)
        {
            // One FILE can be referenced by several PlayItems sharing one feed span, so
            // provenance narrows only to the RUN; timestamp then picks the reference —
            // without it, frames past the first PlayItem's OUT were wrongly dropped.
            let found = self.clip_in_run_for(found, raw_ns);
            let c = self.clips[found];
            let placed = raw_ns >= c.in_ns && raw_ns <= c.out_ns;
            self.cursors[track] = TrackPos {
                clip: found,
                last_raw_ns: Some(raw_ns),
                last_out_ns: if placed {
                    Some(raw_ns.saturating_add(c.offset_ns))
                } else {
                    self.cursors[track].last_out_ns
                },
            };
            if !placed {
                // Outside its own clip's marks: material the playlist excludes
                // (a clip's file is not trimmed to its marks). Counted, so the
                // volume gates in the sinks can see it.
                self.dropped[track] = self.dropped[track].saturating_add(1);
                // Log only the first drop per track, not per-frame noise; the
                // provenance path previously logged nothing here, leaving a
                // volume-gate failure with no clue which frame/clip/marks caused it.
                if self.dropped[track] == 1 {
                    tracing::info!(
                        target: "freemkv::mux",
                        track,
                        clip = found,
                        byte = b,
                        raw_ns,
                        in_ns = c.in_ns,
                        out_ns = c.out_ns,
                        "frame outside its clip's marks (by provenance); dropping"
                    );
                }
                return None;
            }
            return Some(raw_ns.saturating_add(c.offset_ns));
        }

        // No byte offset means placing by marks across clips, which isn't expected
        // under a plan (every demuxed source stamps provenance) and can strand a
        // track if the marks don't advance. Log once per track, not silently.
        if src_byte.is_none() && self.cursors[track].last_raw_ns.is_none() {
            tracing::info!(
                target: "freemkv::mux",
                track,
                "track has no source offset under a seam plan; placing it from \
                 timestamps, which is only reliable while the marks advance"
            );
        }

        // Bounded by the clip count, so a wild PTS cannot spin here.
        while clip + 1 < self.clips.len() {
            let cur = self.clips[clip];
            let next_in = self.clips[clip + 1].in_ns;
            let past_out = raw_ns > cur.out_ns;
            // Must land ON/after next IN or a backward step walks the cursor to the
            // list's end. Reorder tracks (e.g. a DV enhancement layer) require landing
            // ON the mark so a reorder dip isn't misread as a crossing; sparse tracks accept any step.
            let stepped_back = pos.last_raw_ns.is_some_and(|last| raw_ns < last)
                && if has_reorder {
                    // saturating_abs, not abs: `abs()` panics on i64::MIN, which
                    // saturating_sub can produce exactly, taking down the mux
                    // thread on one bad frame instead of comparing false.
                    (raw_ns.saturating_sub(next_in)).saturating_abs() <= CLIP_START_TOLERANCE_NS
                } else {
                    raw_ns >= next_in.saturating_sub(CLIP_START_TOLERANCE_NS)
                        && raw_ns <= self.clips[clip + 1].out_ns
                };
            if past_out || stepped_back {
                clip += 1;
            } else {
                break;
            }
        }

        // Invariant enforced directly, not just via heuristics: output must never run
        // backwards (a stale cursor once caused 65s of uncounted rewind). Tolerance is
        // DISCONTINUITY_BACKSTEP_NS (B-frame jitter); a failing candidate is rejected, or dropped.
        let rewinds = |cand: usize| -> bool {
            match self.cursors[track].last_out_ns {
                Some(last) => {
                    let out = raw_ns.saturating_add(self.clips[cand].offset_ns);
                    last.saturating_sub(out) > DISCONTINUITY_BACKSTEP_NS
                }
                None => false,
            }
        };
        if rewinds(clip) {
            // Move the cursor only if a later clip actually accepts this frame; leaving
            // it in place keeps a bad frame recoverable instead of stranding the track.
            let mut found = None;
            let mut cand = clip;
            while cand + 1 < self.clips.len() {
                cand += 1;
                let cc = self.clips[cand];
                if raw_ns >= cc.in_ns && raw_ns <= cc.out_ns && !rewinds(cand) {
                    found = Some(cand);
                    break;
                }
            }
            if let Some(c) = found {
                clip = c;
            }
        }

        let c = self.clips[clip];
        let placed = raw_ns >= c.in_ns && raw_ns <= c.out_ns && !rewinds(clip);
        self.cursors[track] = TrackPos {
            clip,
            last_raw_ns: Some(raw_ns),
            last_out_ns: if placed {
                Some(raw_ns.saturating_add(c.offset_ns))
            } else {
                self.cursors[track].last_out_ns
            },
        };
        if !placed {
            self.dropped[track] = self.dropped[track].saturating_add(1);
            // Once per track, and only on the first drop: a join legitimately
            // drops a handful of frames, so this must not become per-frame
            // noise on a normal title. The total is available to callers.
            if self.dropped[track] == 1 {
                tracing::debug!(
                    target: "freemkv::mux",
                    track,
                    clip,
                    raw_ns,
                    in_ns = c.in_ns,
                    out_ns = c.out_ns,
                    "frame outside the playlist's clip marks; dropping"
                );
            }
            return None;
        }
        Some(raw_ns.saturating_add(c.offset_ns))
    }
}

// Global timeline corrector: holds a SeamPlan when usable, else falls back to
// PTS-jump inference. Only the VIDEO track drives epoch decisions. See
// docs/mux-timeline.md#timelinecontinuity-struct-doc.
pub(crate) struct TimelineContinuity {
    /// Offset (ns) added to raw PTS for the CURRENT epoch.
    pub(crate) offset_ns: i64,
    // Offset (ns) of the immediately previous epoch, used to remap a
    // non-video tail straggler at a boundary. See
    // docs/mux-timeline.md#timelinecontinuityprev_offset_ns.
    pub(crate) prev_offset_ns: i64,
    /// Highest adjusted VIDEO PTS (ns) accepted onto the timeline so far — the
    /// running frontier. `None` until the first video frame. Only video advances
    /// it; non-video tracks never touch it.
    pub(crate) high_ns: Option<i64>,
    /// The playlist's clip placement, when the source has one. Present = the
    /// marks are known and are used verbatim; absent = fall back to inferring
    /// seams from PTS jumps, which is all any non-BD source has ever had.
    pub(crate) seams: Option<SeamPlan>,
    // Every epoch already left behind, oldest first, as (offset, frontier
    // when it closed) — a single prev_offset_ns can't name a straggler's
    // epoch. See docs/mux-timeline.md#timelinecontinuityepoch_offsets.
    epoch_offsets: Vec<(i64, i64)>,
    // Last raw PTS seen per track, for spotting a track's own discontinuity
    // (distinct from the shared frontier). See
    // docs/mux-timeline.md#timelinecontinuitylast_raw_ns.
    last_raw_ns: Vec<Option<i64>>,
    // Per-track provisional offset for frames arriving before the video
    // frame that opens their epoch. Never written to offset_ns/high_ns and
    // never retires an epoch. See docs/mux-timeline.md#timelinecontinuityprovisional.
    provisional: Vec<Option<(usize, i64)>>,
}

/// Most epochs retained for straggler resolution. A title has a handful; this
/// only bounds a pathological source that rebases without end.
const MAX_EPOCHS: usize = 64;

impl TimelineContinuity {
    pub(crate) fn new() -> Self {
        Self {
            epoch_offsets: Vec::new(),
            last_raw_ns: Vec::new(),
            provisional: Vec::new(),
            offset_ns: 0,
            prev_offset_ns: 0,
            high_ns: None,
            seams: None,
        }
    }

    // Corrector driven by a title's PlayItem marks where they exist, else
    // `Self::new`'s inference. See docs/mux-timeline.md#timelinecontinuitywith_clips.
    pub(crate) fn with_clips(
        clips: &[crate::disc::Clip],
        content_format: crate::disc::ContentFormat,
    ) -> Self {
        // Only Blu-ray: its PlayItem IN/OUT share the 45 kHz clock the PES PTS runs on.
        // HD-DVD/DVD marks come from a different clock (XPL times, cell tables) — a
        // plan from those would drop content the PTS wasn't measured against, so they keep inference.
        let seams = match content_format {
            crate::disc::ContentFormat::BdTs => SeamPlan::from_clips(clips),
            crate::disc::ContentFormat::MpegPs => None,
        };
        Self {
            epoch_offsets: Vec::new(),
            last_raw_ns: Vec::new(),
            provisional: Vec::new(),
            offset_ns: 0,
            prev_offset_ns: 0,
            high_ns: None,
            seams,
        }
    }

    // Total frames dropped for falling outside the playlist's clip marks.
    // Zero without a seam plan. See docs/mux-timeline.md#timelinecontinuitydropped_total.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.seams.as_ref().map_or(0, |p| p.dropped_total())
    }

    // Map a raw PES PTS onto the output timeline, or `None` to drop the frame
    // (only ever happens under a SeamPlan). See docs/mux-timeline.md#timelinecontinuitymap.
    pub(crate) fn map(
        &mut self,
        raw_pts_ns: i64,
        drives_epoch: bool,
        track: usize,
        has_reorder: bool,
        // Byte offset this frame was read from (`PesFrame::source`), when the
        // source stamps it. Under a seam plan this identifies the clip
        // directly; without it the mark heuristics are used instead.
        src_byte: Option<u64>,
    ) -> Option<i64> {
        if self.seams.is_some() {
            // Take the plan out for the call so `place` can borrow `self`
            // mutably without fighting the borrow checker over the whole struct.
            let mut plan = self.seams.take().expect("checked is_some");
            let placed = plan.place(raw_pts_ns, track, has_reorder, src_byte);
            self.seams = Some(plan);
            if let Some(p) = placed {
                // Keep the frontier meaningful for anything that reads it, and
                // keep `offset_ns` reporting the correction actually applied.
                if drives_epoch {
                    self.high_ns = Some(self.high_ns.map_or(p, |h| h.max(p)));
                }
                self.offset_ns = p.saturating_sub(raw_pts_ns);
            }
            return placed;
        }
        Some(self.adjust(raw_pts_ns, drives_epoch, track))
    }

    // The offset a passive frame should ride: normally the current epoch's,
    // unless the frame arrived ahead of the video that opens its epoch. See
    // docs/mux-timeline.md#timelinecontinuitypassive_offset.
    fn passive_offset(&mut self, track: usize, raw_pts_ns: i64) -> i64 {
        if self.last_raw_ns.len() <= track {
            self.last_raw_ns.resize(track + 1, None);
            self.provisional.resize(track + 1, None);
        }
        let prev_raw = self.last_raw_ns[track].replace(raw_pts_ns);
        let retired = self.epoch_offsets.len();

        // A provisional only survives until the video opens the epoch for real.
        if let Some((taken_at, _)) = self.provisional[track]
            && taken_at != retired
        {
            self.provisional[track] = None;
        }
        let effective = self.provisional[track].map_or(self.offset_ns, |(_, o)| o);

        if let Some(high) = self.high_ns
            && self.provisional[track].is_none()
            && let Some(pr) = prev_raw
            && raw_pts_ns < pr.saturating_sub(DISCONTINUITY_BACKSTEP_NS)
        {
            let mapped = raw_pts_ns.saturating_add(effective);
            if mapped < high.saturating_sub(DISCONTINUITY_BACKSTEP_NS) {
                let off = high
                    .saturating_sub(mapped)
                    .saturating_add(DISCONTINUITY_GAP_NS);
                let off = effective.saturating_add(off);
                self.provisional[track] = Some((retired, off));
                return off;
            }
        }
        effective
    }

    // Retire the current epoch and open a new one continuing just after the
    // frontier, recording where this epoch closed for straggler lookups. See
    // docs/mux-timeline.md#timelinecontinuityopen_epoch.
    fn open_epoch(&mut self, high: i64, mapped_now: i64) {
        self.prev_offset_ns = self.offset_ns;
        if self.epoch_offsets.len() == MAX_EPOCHS {
            self.epoch_offsets.remove(0);
        }
        self.epoch_offsets.push((self.offset_ns, high));
        let bump = high
            .saturating_sub(mapped_now)
            .saturating_add(DISCONTINUITY_GAP_NS);
        self.offset_ns = self.offset_ns.saturating_add(bump);
    }

    // The offset of the epoch a straggler actually belongs to: the retained
    // epoch that lands it CLOSEST BELOW its own frontier. See
    // docs/mux-timeline.md#timelinecontinuitystraggler_offset.
    fn straggler_offset(&self, raw_pts_ns: i64) -> Option<i64> {
        self.epoch_offsets
            .iter()
            .map(|(o, closing)| (raw_pts_ns.saturating_add(*o), *closing))
            // In the TAIL of that epoch (at/just below where it ended); far below
            // is a later epoch's normal frame, and demoting it mis-times it by a clip.
            .filter(|(m, closing)| {
                *m <= *closing && *m >= closing.saturating_sub(DISCONTINUITY_BACKSTEP_NS)
            })
            .map(|(m, _)| m)
            .max()
    }

    // Map a raw PES PTS (ns) onto the continuous timeline. `drives_epoch` is
    // true only for the primary video track. See
    // docs/mux-timeline.md#timelinecontinuityadjust.
    pub(crate) fn adjust(&mut self, raw_pts_ns: i64, drives_epoch: bool, track: usize) -> i64 {
        // Passive track: ride the current epoch's offset. Never advance the
        // frontier and never open an epoch — these tracks each run on their own
        // (sparse/laggy/independent) timeline and would false-trigger the ratchet.
        if !drives_epoch {
            let effective = self.passive_offset(track, raw_pts_ns);
            let mapped = raw_pts_ns.saturating_add(effective);
            // Tail-straggler remap: a lagging old-epoch frame under the new offset would
            // fling past the frontier, breaking monotonicity; recognised by current mapping
            // past frontier + prev mapping in a bounded seam tail (bound avoids wrongly demoting a normal sparse-leading frame). All ops saturate: `high`/`raw_pts_ns` can be adversarial/negative.
            if let Some(high) = self.high_ns
                && mapped > high.saturating_add(DISCONTINUITY_BACKSTEP_NS)
                && let Some(placed) = self.straggler_offset(raw_pts_ns)
            {
                return placed;
            }
            return mapped;
        }

        let Some(high) = self.high_ns else {
            let adj = raw_pts_ns.saturating_add(self.offset_ns);
            self.high_ns = Some(adj);
            return adj;
        };
        let adj = raw_pts_ns.saturating_add(self.offset_ns);
        if adj < high.saturating_sub(DISCONTINUITY_BACKSTEP_NS) {
            // Clip-boundary reset: continue past the frontier, saving the previous
            // offset so a lagging tail frame can be remapped (see above). Both `high`
            // and `adj` are untrusted, so `open_epoch` saturates rather than panic.
            self.open_epoch(high, adj);
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
        tc.adjust(p, true, 0)
    }
    fn adj_other(tc: &mut TimelineContinuity, p: i64) -> i64 {
        tc.adjust(p, false, 1)
    }

    // Characterization of the BUG: two clips concatenated with a PTS reset
    // at the boundary must come out monotonic and continuous. See
    // docs/mux-timeline.md#continuity_rebases_clip_boundary_reset.
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

    // The output timeline must never go BACKWARDS at a clip join. Audit
    // finding, measured against the real 00801.mpls marks. See
    // docs/mux-timeline.md#a_clip_join_never_rewinds_the_output_timeline.
    #[test]
    fn a_clip_join_never_rewinds_the_output_timeline() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("the real table must plan");
        let a = plan
            .place(7_910_000_000_000, 0, true, None)
            .expect("a frame inside clip 5 is placed");
        // Clip 6's file opens 8s below its own IN mark.
        let b = plan.place(7_845_000_000_000, 0, true, None);
        if let Some(b) = b {
            assert!(
                b >= a,
                "output rewound {}s at a clip join (from {a} to {b})",
                (a - b) as f64 / 1e9
            );
        } else {
            // Dropped as pre-mark material — correct, and it must be COUNTED so
            // the volume gates in the sinks can see it.
            assert!(
                plan.dropped_total() > 0,
                "a dropped frame must be counted, or the finish() gates are blind to it"
            );
        }
    }

    // A glitched PTS must not strand a track on a later clip forever. Audit
    // finding against the large-backstep branch. See
    // docs/mux-timeline.md#a_glitched_pts_does_not_strand_a_track_on_a_later_clip.
    #[test]
    fn a_glitched_pts_does_not_strand_a_track_on_a_later_clip() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        // Clip 0 is (188955000, 271486824) -> 4199.0s .. 6033.04s
        let good = plan
            .place(5_000_000_000_000, 1, false, None)
            .expect("a frame inside clip 0 is placed");
        // A damaged PTS well below clip 0's IN: a >3s backstep that is NOT a
        // clip change. It should be dropped, and the cursor must not move.
        assert!(
            plan.place(4_000_000_000_000, 1, false, None).is_none(),
            "a frame before the first clip's IN is not placeable"
        );
        // The very next good frame must still be placed, on the same clip.
        let after = plan
            .place(5_001_000_000_000, 1, false, None)
            .expect("the track must recover on the next good frame, not be stranded");
        assert_eq!(
            after - good,
            1_000_000_000,
            "the recovered frame must land 1s after the last good one, on the same clip"
        );
    }

    // The three placements audit round 7 enumerated. A track's output never
    // runs backwards, and a bad frame never strands the cursor. See
    // docs/mux-timeline.md#output_never_rewinds_and_a_bad_frame_never_strands.
    #[test]
    fn output_never_rewinds_and_a_bad_frame_never_strands() {
        let clips = seamless_branching_clips();

        // (1) A clip opening ON its IN mark after a tail frame past the previous
        //     clip's OUT: the old guard misread this as a crossing, dropping ~28 min.
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let tail = plan
            .place(7_910_830_000_000, 0, true, None)
            .expect("tail placed");
        let open = plan.place(7_853_000_000_000, 0, true, None);
        if let Some(open) = open {
            assert!(open >= tail, "clip 6 opening rewound the output");
        }
        // Whatever happened to that frame, the clip must still play: a frame
        // well inside clip 6 has to be placed, not stranded.
        let mid = plan.place(9_000_000_000_000, 0, true, None);
        assert!(mid.is_some(), "clip 6 was stranded and 28 minutes lost");

        // (2) The LAST clip. The old guard skipped itself there (`clip + 1 <
        //     len`), leaving the rewind completely unguarded at the 9->10 seam.
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let a = plan
            .place(10_030_000_000_000, 1, false, None)
            .expect("clip 9 tail");
        if let Some(b) = plan.place(9_954_375_000_000, 1, false, None) {
            assert!(
                b >= a,
                "the last clip rewound the output by {}s",
                (a - b) as f64 / 1e9
            );
        }

        // (3) A glitch in the MIDDLE of a long clip. It satisfies "inside the
        //     current clip", so the old guard advanced and stranded the rest.
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let good = plan
            .place(9_000_000_000_000, 0, true, None)
            .expect("clip 6 frame");
        let _ = plan.place(8_995_000_000_000, 0, true, None); // glitched, may drop
        let after = plan
            .place(9_001_000_000_000, 0, true, None)
            .expect("the clip must keep playing after one bad timestamp");
        assert!(after >= good, "a mid-clip glitch rewound the output");
    }

    // The output offset `from_clips` computes for a clip: the sum of every
    // earlier clip's playable duration, minus its own IN.
    fn plan_offset_for(clips: &[crate::disc::Clip], want: &crate::disc::Clip) -> i64 {
        let mut cum = 0i64;
        for c in clips {
            let in_ns = mpls_ticks_to_ns(c.in_time);
            let out_ns = mpls_ticks_to_ns(c.out_time);
            if c.clip_id == want.clip_id && c.in_time == want.in_time {
                return cum;
            }
            cum += out_ns - in_ns;
        }
        cum
    }

    fn clips_with_spans() -> Vec<crate::disc::Clip> {
        let mut clips = seamless_branching_clips();
        // Each clip's stream occupies a contiguous run of the feed. Sizes are
        // arbitrary but ordered and contiguous, which is what the plan checks.
        let mut pos = 0u64;
        for c in clips.iter_mut() {
            let len = 1_000_000u64;
            c.feed_span = Some((pos, pos + len));
            pos += len;
        }
        clips
    }

    /// The case four rounds of mark heuristics could not get right: inside an
    /// overlap, clip k's OUT is AFTER clip k+1's IN, so one timestamp is valid
    /// in both. The byte offset says which clip the frame actually came from.
    #[test]
    fn provenance_picks_the_clip_the_frame_came_from_inside_an_overlap() {
        let clips = clips_with_spans();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        // 7900s is inside clip 5 [7707.875, 7910.786] AND clip 6 [7853, 9539].
        let raw = 7_900_000_000_000i64;
        let in5 = plan
            .place(raw, 0, true, Some(5_500_000))
            .expect("clip 5 byte");
        let mut plan2 = SeamPlan::from_clips(&clips).expect("plan");
        let in6 = plan2
            .place(raw, 0, true, Some(6_500_000))
            .expect("clip 6 byte");
        assert_ne!(
            in5, in6,
            "the same timestamp from different clips must place differently — \
             that difference is exactly what marks alone cannot see"
        );
    }

    // Every track of a clip lives in the SAME stream file, so provenance
    // makes video, audio and subtitles agree by construction. See
    // docs/mux-timeline.md#all_tracks_of_one_clip_agree_under_provenance.
    #[test]
    fn all_tracks_of_one_clip_agree_under_provenance() {
        let clips = clips_with_spans();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let raw = 7_900_000_000_000i64;
        let byte = Some(5_500_000);
        let video = plan.place(raw, 0, true, byte).expect("video");
        let audio = plan.place(raw, 1, false, byte).expect("audio");
        let subs = plan.place(raw, 2, false, byte).expect("subtitle");
        assert_eq!(video, audio, "audio must land where video did");
        assert_eq!(video, subs, "subtitles must land where video did");
    }

    /// A clip's file is not trimmed to its marks, so it can carry material the
    /// playlist excludes. That material is dropped — and COUNTED, or the volume
    /// gates in the sinks are blind to it.
    #[test]
    fn material_outside_its_own_clips_marks_is_dropped_and_counted() {
        let clips = clips_with_spans();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        // Clip 5 spans [7707.875, 7910.786]; 7700s is before its IN.
        assert!(
            plan.place(7_700_000_000_000, 0, true, Some(5_500_000))
                .is_none(),
            "pre-mark material must not be emitted"
        );
        assert_eq!(plan.dropped_total(), 1, "and it must be counted");
    }

    /// If the spans do not tile the feed, an offset means nothing — so the plan
    /// must IGNORE provenance rather than trust a broken map, and fall back to
    /// the mark heuristics (the 1.6.0 behaviour).
    #[test]
    fn a_broken_span_map_is_not_trusted() {
        let mut clips = clips_with_spans();
        clips[3].feed_span = None; // a hole
        let plan = SeamPlan::from_clips(&clips).expect("plan");
        assert!(
            !plan.spans_trusted,
            "a gap in the spans must disable provenance, not select a wrong clip"
        );

        let mut gapped = clips_with_spans();
        gapped[4].feed_span = Some((99_000_000, 99_100_000)); // discontiguous
        let plan = SeamPlan::from_clips(&gapped).expect("plan");
        assert!(
            !plan.spans_trusted,
            "a discontiguous span must not be trusted"
        );

        let good = SeamPlan::from_clips(&clips_with_spans()).expect("plan");
        assert!(
            good.spans_trusted,
            "contiguous spans from 0 must be trusted"
        );
    }

    // A clip FILE referenced by two adjacent PlayItems shares one feed span
    // but each carries its OWN marks; both halves must be kept. See
    // docs/mux-timeline.md#a_clip_split_across_two_play_items_keeps_both_halves.
    #[test]
    fn a_clip_split_across_two_play_items_keeps_both_halves() {
        // One file, one span. Two PlayItems: [0s,10s) then [10s,20s).
        const SPAN: (u64, u64) = (0, 4_000_000);
        let mk = |in_t: u32, out_t: u32| crate::disc::Clip {
            feed_span: Some(SPAN),
            clip_id: "00001".into(),
            in_time: in_t,
            out_time: out_t,
            duration_secs: 10.0,
            source_packets: 0,
        };
        // 45 kHz ticks: 10 s = 450_000, 20 s = 900_000.
        let clips = vec![mk(0, 450_000), mk(450_000, 900_000)];
        let mut plan = SeamPlan::from_clips(&clips).expect("two PlayItems, one file");
        assert!(plan.spans_trusted, "one file legitimately has one span");

        // A frame from the SECOND PlayItem's range, read from within the file.
        let raw = mpls_ticks_to_ns(600_000); // 13.33 s — inside [10s, 20s)
        let placed = plan.place(raw, 0, false, Some(2_000_000));
        assert!(
            placed.is_some(),
            "a frame in the second PlayItem's marks must be kept — it resolves \
             to the same span as the first, so only its timestamp can say which \
             half of the file it belongs to"
        );
        assert_eq!(
            plan.dropped_total(),
            0,
            "nothing from a legitimately referenced half may be dropped"
        );
    }

    #[test]
    fn a_repeated_clip_shares_one_span_and_is_still_trusted() {
        let mut clips = clips_with_spans();
        let dup = clips[2].feed_span;
        clips[3].feed_span = dup;
        // Re-tile the rest so the run stays contiguous after the duplicate.
        let (_, end) = dup.unwrap();
        let mut pos = end;
        for c in clips.iter_mut().skip(4) {
            c.feed_span = Some((pos, pos + 1_000_000));
            pos += 1_000_000;
        }
        let plan = SeamPlan::from_clips(&clips).expect("plan");
        assert!(
            plan.spans_trusted,
            "a repeated clip reusing its first reference's span is legitimate"
        );
    }

    // A source that stamps no provenance (a mkv:// remux) must still work by
    // falling back to the mark heuristics. See
    // docs/mux-timeline.md#no_provenance_still_places_by_marks.
    #[test]
    fn no_provenance_still_places_by_marks() {
        let clips = clips_with_spans();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        // `is_some()` alone would pass even for the WRONG clip. This timestamp sits
        // in a real OVERLAP, so pinning one clip would assert a coin-flip; the
        // invariant checked instead is that the placement matches SOME clip whose marks contain it.
        let raw = 7_900_000_000_000i64;
        let placed = plan
            .place(raw, 0, true, None)
            .expect("a frame with no source offset must still be placed");

        let candidates: Vec<i64> = clips
            .iter()
            .filter(|c| raw >= mpls_ticks_to_ns(c.in_time) && raw <= mpls_ticks_to_ns(c.out_time))
            .map(|c| raw - mpls_ticks_to_ns(c.in_time) + plan_offset_for(&clips, c))
            .collect();
        assert!(
            !candidates.is_empty(),
            "fixture check: the probe timestamp must sit inside at least one clip"
        );
        assert!(
            candidates.contains(&placed),
            "placed at {placed}, but no clip containing this timestamp maps it there \
             (candidates {candidates:?}) — a frame was given a clip it does not belong to"
        );
    }

    // On frames that are NOT ambiguous, provenance and the mark heuristics
    // must give the SAME answer — the strongest available cross-check. See
    // docs/mux-timeline.md#provenance_agrees_with_marks_wherever_marks_are_unambiguous.
    #[test]
    fn provenance_agrees_with_marks_wherever_marks_are_unambiguous() {
        let clips = clips_with_spans();
        let plain = seamless_branching_clips();
        let mut disagreements = Vec::new();

        for (i, c) in clips.iter().enumerate() {
            let in_ns = mpls_ticks_to_ns(c.in_time);
            let out_ns = mpls_ticks_to_ns(c.out_time);
            // Sample across the clip.
            for f in [1u32, 2, 4, 8] {
                let raw = in_ns + (out_ns - in_ns) / f as i64;
                // Unambiguous = inside THIS clip's marks and no other's.
                let owners = clips
                    .iter()
                    .filter(|o| {
                        raw >= mpls_ticks_to_ns(o.in_time) && raw <= mpls_ticks_to_ns(o.out_time)
                    })
                    .count();
                if owners != 1 {
                    continue;
                }
                let (s, _) = c.feed_span.expect("span");
                let mut a = SeamPlan::from_clips(&clips).expect("plan");
                let mut b = SeamPlan::from_clips(&plain).expect("plan");
                let by_bytes = a.place(raw, 0, true, Some(s + 10));
                let by_marks = b.place(raw, 0, true, None);
                if by_bytes != by_marks {
                    disagreements.push((i, raw, by_bytes, by_marks));
                }
            }
        }
        assert!(
            disagreements.is_empty(),
            "provenance and marks disagreed on unambiguous frames: {disagreements:?}"
        );
    }

    // Walk a whole title through the plan with provenance: output never
    // moves backwards, and the total span matches the declared duration. See
    // docs/mux-timeline.md#a_full_pass_over_the_real_table_is_monotonic_and_totals_correctly.
    #[test]
    fn a_full_pass_over_the_real_table_is_monotonic_and_totals_correctly() {
        let clips = clips_with_spans();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let declared: i64 = clips
            .iter()
            .map(|c| mpls_ticks_to_ns(c.out_time) - mpls_ticks_to_ns(c.in_time))
            .sum();

        let mut last: Option<i64> = None;
        let mut first: Option<i64> = None;
        let mut placed = 0u64;
        for c in &clips {
            let (s, e) = c.feed_span.expect("span");
            let in_ns = mpls_ticks_to_ns(c.in_time);
            let out_ns = mpls_ticks_to_ns(c.out_time);
            // 40 frames per clip, evenly spaced, each stamped from its own span.
            for k in 0..40i64 {
                let raw = in_ns + (out_ns - in_ns) * k / 40;
                let byte = s + (e - s) * k as u64 / 40;
                if let Some(out) = plan.place(raw, 0, true, Some(byte)) {
                    if let Some(l) = last {
                        assert!(
                            out >= l,
                            "output moved backwards: {l} -> {out} (clip {}, raw {raw})",
                            c.clip_id
                        );
                    }
                    first.get_or_insert(out);
                    last = Some(out);
                    placed += 1;
                }
            }
        }
        assert!(placed > 400, "most frames must be placed, got {placed}");
        let span = last.expect("last") - first.expect("first");
        // Within one clip's frame spacing of the declared duration.
        let slack = declared / 40;
        assert!(
            (span - declared).abs() <= slack,
            "output span {span} differs from declared {declared} by more than {slack}"
        );
    }

    /// A heavily branched title — the hoard has discs with 900 PlayItems —
    /// must resolve every frame to the right clip, and must do it by binary
    /// search rather than by scanning 900 entries per frame per track.
    #[test]
    fn a_nine_hundred_clip_title_resolves_every_frame_correctly() {
        const N: u32 = 900;
        const CLIP_TICKS: u32 = 10 * 45_000; // 10s each
        const CLIP_BYTES: u64 = 5_000_000;
        let clips: Vec<crate::disc::Clip> = (0..N)
            .map(|i| crate::disc::Clip {
                clip_id: format!("{i:05}"),
                in_time: i * CLIP_TICKS,
                out_time: (i + 1) * CLIP_TICKS,
                duration_secs: 10.0,
                source_packets: 0,
                feed_span: Some((i as u64 * CLIP_BYTES, (i as u64 + 1) * CLIP_BYTES)),
            })
            .collect();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        assert!(
            plan.spans_trusted,
            "a contiguous 900-clip table must be trusted"
        );

        // Every clip, sampled at its middle, must place — and monotonically.
        let mut last: Option<i64> = None;
        for i in 0..N {
            let raw = mpls_ticks_to_ns(i * CLIP_TICKS + CLIP_TICKS / 2);
            let byte = i as u64 * CLIP_BYTES + CLIP_BYTES / 2;
            let out = plan
                .place(raw, 0, true, Some(byte))
                .unwrap_or_else(|| panic!("clip {i} did not place"));
            if let Some(l) = last {
                assert!(out >= l, "output moved backwards at clip {i}: {l} -> {out}");
            }
            last = Some(out);
        }

        // The lookup must find the right clip from an arbitrary byte, not just
        // in ascending order — a track can arrive out of step with another.
        for i in [0u32, 1, 449, 898, 899] {
            let byte = i as u64 * CLIP_BYTES + 7;
            assert_eq!(
                plan.clip_at_byte(byte),
                Some(i as usize),
                "byte {byte} must resolve to clip {i}"
            );
        }
        assert_eq!(
            plan.clip_at_byte(N as u64 * CLIP_BYTES + 1),
            None,
            "a byte past the last clip belongs to no clip"
        );
    }

    // A title whose PlayItems all reference ONE clip file: spans are
    // TRUSTED but carry no info on which PlayItem a byte belongs to. See
    // docs/mux-timeline.md#one_clip_file_behind_every_play_item_is_not_distinguishable_by_byte.
    #[test]
    fn one_clip_file_behind_every_play_item_is_not_distinguishable_by_byte() {
        const N: u32 = 8;
        const SPAN: (u64, u64) = (0, 40_000_000_000);
        const SEG: u32 = 600 * 45_000; // 10 min per sub-range
        let clips: Vec<crate::disc::Clip> = (0..N)
            .map(|i| crate::disc::Clip {
                clip_id: "00001".to_string(), // the SAME file every time
                in_time: i * SEG,
                out_time: (i + 1) * SEG,
                duration_secs: 600.0,
                source_packets: 0,
                feed_span: Some(SPAN),
            })
            .collect();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        assert!(
            plan.spans_trusted,
            "identical spans pass the tiling check -- one file has one span"
        );
        // A byte offset alone cannot say which of the 8 ranges a frame belongs
        // to: every byte in the file resolves to the FIRST play item.
        for b in [0u64, 1_000_000, 20_000_000_000, 39_999_999_999] {
            assert_eq!(
                plan.clip_at_byte(b),
                Some(0),
                "byte {b} resolves to the first play item, always"
            );
        }

        // ...which is why placement must finish the job with the timestamp: this
        // used to drop 7/8 of the title (frames judged against the wrong range's marks).
        for i in 0..N {
            let mid = mpls_ticks_to_ns(i * SEG + SEG / 2);
            let placed = plan
                .place(mid, 0, false, Some(1_000_000))
                .unwrap_or_else(|| panic!("a frame in play item {i}'s marks must be kept"));
            assert!(placed >= 0, "play item {i} placed at {placed}");
        }
        assert_eq!(
            plan.dropped_total(),
            0,
            "no range of a legitimately split file may be dropped"
        );
    }

    // Marks that do not advance across the title are normal (each clip has
    // its own STC) and are PLACED, since every track carries a byte offset.
    // See docs/mux-timeline.md#marks_that_do_not_advance_are_placed_by_provenance.
    #[test]
    fn marks_that_do_not_advance_are_placed_by_provenance() {
        const N: u32 = 4;
        const SEG: u32 = 600 * 45_000;
        const CLIP_BYTES: u64 = 4_000_000_000;
        let clips: Vec<crate::disc::Clip> = (0..N)
            .map(|i| crate::disc::Clip {
                clip_id: format!("{i:05}"),
                in_time: 0,
                out_time: SEG,
                duration_secs: 600.0,
                source_packets: 0,
                feed_span: Some((i as u64 * CLIP_BYTES, (i as u64 + 1) * CLIP_BYTES)),
            })
            .collect();
        let mut plan =
            SeamPlan::from_clips(&clips).expect("a non-advancing table is placeable by byte");

        // The SAME raw timestamp appears in every clip — unresolvable from
        // timestamps, exact from the byte offset.
        let seg_ns = mpls_ticks_to_ns(SEG);
        let raw = mpls_ticks_to_ns(SEG / 2);
        let mut last: Option<i64> = None;
        for i in 0..N {
            let byte = i as u64 * CLIP_BYTES + CLIP_BYTES / 2;
            let out = plan
                .place(raw, 0, true, Some(byte))
                .unwrap_or_else(|| panic!("clip {i} did not place"));
            assert_eq!(out, i as i64 * seg_ns + seg_ns / 2, "clip {i}");
            if let Some(l) = last {
                assert!(out > l, "output moved backwards at clip {i}");
            }
            last = Some(out);
        }
        assert_eq!(plan.total_ns(), N as i64 * seg_ns);
    }

    /// Without usable spans there is no byte offset to place by, so inference
    /// is all that is left — and a table inference cannot read must still be
    /// refused rather than silently truncating the title.
    #[test]
    fn marks_that_do_not_advance_are_still_refused_without_spans() {
        const N: u32 = 4;
        const SEG: u32 = 600 * 45_000;
        let clips: Vec<crate::disc::Clip> = (0..N)
            .map(|i| crate::disc::Clip {
                clip_id: format!("{i:05}"),
                in_time: 0,
                out_time: SEG,
                duration_secs: 600.0,
                source_packets: 0,
                feed_span: None,
            })
            .collect();
        assert!(SeamPlan::from_clips(&clips).is_none());
    }

    // A non-monotonic table whose spans cannot be trusted has neither a
    // usable clock nor byte offset, so inference is the only safe path.
    #[test]
    fn a_restarting_clock_table_without_spans_is_still_refused() {
        const N: u32 = 6;
        const CLIP_TICKS: u32 = 600 * 45_000;
        let clips: Vec<crate::disc::Clip> = (0..N)
            .map(|i| crate::disc::Clip {
                clip_id: format!("{i:05}"),
                in_time: 0,
                out_time: CLIP_TICKS,
                duration_secs: 600.0,
                source_packets: 0,
                feed_span: None,
            })
            .collect();
        assert!(
            SeamPlan::from_clips(&clips).is_none(),
            "without spans a restarting-clock table must fall back to inference"
        );
    }

    /// Build a real seamless-branching clip table (`00801.mpls`, 11 PlayItems, marks
    /// read off the disc) so the arithmetic is pinned to measured ground truth
    /// rather than to invented numbers.
    fn seamless_branching_clips() -> Vec<crate::disc::Clip> {
        // (in_time, out_time) in 45kHz ticks, verbatim from the disc.
        const MARKS: [(u32, u32); 11] = [
            (188955000, 271486824),
            (271406250, 283868700),
            (283417500, 309378435),
            (309791250, 312660991),
            (312219375, 346451698),
            (346854375, 355985371),
            (353385000, 429270810),
            (431786250, 440217172),
            (437326875, 442467635),
            (445344375, 451271546),
            (447946875, 540576286),
        ];
        MARKS
            .iter()
            .enumerate()
            .map(|(i, &(in_time, out_time))| crate::disc::Clip {
                feed_span: None,
                clip_id: format!("{i:05}"),
                in_time,
                out_time,
                duration_secs: (out_time - in_time) as f64 / 45_000.0,
                source_packets: 0,
            })
            .collect()
    }

    // The plan's total must equal the title's declared duration. See
    // docs/mux-timeline.md#seam_plan_total_matches_the_declared_duration.
    #[test]
    fn seam_plan_total_matches_the_declared_duration() {
        let plan = SeamPlan::from_clips(&seamless_branching_clips()).expect("plan");
        let total = plan.total_ns();
        // 7893.3854s, the duration freemkv itself reports for this title.
        assert!(
            (total - 7_893_385_400_000).abs() < 1_000_000,
            "plan total {total} ns is not the declared 7893.3854 s"
        );
    }

    /// Clips are laid end to end: each starts exactly where the previous ended,
    /// so a forward skip in the source clock never becomes dead timeline.
    #[test]
    fn seam_plan_lays_clips_end_to_end() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let mut expected_start = 0i64;
        for (i, c) in clips.iter().enumerate() {
            let in_ns = mpls_ticks_to_ns(c.in_time);
            let out_ns = mpls_ticks_to_ns(c.out_time);
            // First frame of the clip lands at the running total.
            let got = plan
                .place(in_ns, 0, true, None)
                .expect("clip start is inside its marks");
            assert_eq!(got, expected_start, "clip {i} start misplaced");
            // Last frame lands at the running total plus the clip's length.
            let end = plan
                .place(out_ns, 0, true, None)
                .expect("clip end is inside its marks");
            assert_eq!(
                end,
                expected_start + (out_ns - in_ns),
                "clip {i} end misplaced"
            );
            expected_start += out_ns - in_ns;
        }
        assert!(
            (expected_start - 7_893_385_400_000).abs() < 1_000_000,
            "clips do not sum to the declared duration"
        );
    }

    // The 9.174 s forward skip between clip 2 and clip 3 must vanish. See
    // docs/mux-timeline.md#seam_plan_closes_the_forward_skip.
    #[test]
    fn seam_plan_closes_the_forward_skip() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c2_out = mpls_ticks_to_ns(clips[2].out_time);
        let c3_in = mpls_ticks_to_ns(clips[3].in_time);
        assert!(
            c3_in - c2_out > 9_000_000_000,
            "fixture should contain the ~9.17s skip"
        );
        let end_of_2 = plan.place(c2_out, 0, true, None).expect("in clip 2");
        let start_of_3 = plan.place(c3_in, 0, true, None).expect("in clip 3");
        assert_eq!(
            start_of_3, end_of_2,
            "clip 3 must begin exactly where clip 2 ended — the skip is not content"
        );
    }

    // The 1.79 s overlap at seam 1 must JOIN cleanly, not rewind the
    // timeline. See docs/mux-timeline.md#seam_plan_joins_an_overlap_without_rewinding.
    #[test]
    fn seam_plan_joins_an_overlap_without_rewinding() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);
        let c1_in = mpls_ticks_to_ns(clips[1].in_time);
        assert!(c1_in < c0_out, "fixture should contain the overlap");
        // Play clip 0 through to its OUT mark.
        let last_of_0 = plan
            .place(c0_out, 0, true, None)
            .expect("clip 0 OUT is inside clip 0");
        // Old inference saw this 1.79s backward step as below the reorder threshold,
        // missing the seam and emitting colliding duplicate content. With marks known,
        // clip 1 continues exactly where clip 0 ended: monotonic, no rewind or collision.
        let first_of_1 = plan.place(c1_in, 0, true, None).expect("clip 1 IN");
        assert_eq!(
            first_of_1, last_of_0,
            "clip 1 must continue from clip 0's end, not rewind by the overlap"
        );
        // And the timeline keeps moving forward from there.
        let into_1 = plan
            .place(c1_in + 1_000_000_000, 0, true, None)
            .expect("1s into clip 1");
        assert_eq!(
            into_1,
            first_of_1 + 1_000_000_000,
            "clip 1 advances normally"
        );
    }

    /// A lagging audio/subtitle frame from the clip that just ended is placed in
    /// that clip, not dropped — the tracks do not switch on the same frame.
    #[test]
    fn seam_plan_places_a_lagging_passive_frame_in_the_previous_clip() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);
        let c1_in = mpls_ticks_to_ns(clips[1].in_time);
        // Video crosses into clip 1.
        plan.place(c1_in + 500_000_000, 0, true, None)
            .expect("in clip 1");
        // A straggler from clip 0's tail arrives afterwards.
        let tail = c0_out - 50_000_000; // 50ms before clip 0's OUT
        let placed = plan
            .place(tail, 1, false, None)
            .expect("straggler must be placed");
        let expected = tail + (0i64 - mpls_ticks_to_ns(clips[0].in_time));
        assert_eq!(placed, expected, "straggler must ride clip 0's offset");
    }

    // `map()`'s seam-plan branch (the frontier/offset bookkeeping glue) was
    // untested. See docs/mux-timeline.md#map_under_a_seam_plan_tracks_offset_and_frontier.
    #[test]
    fn map_under_a_seam_plan_tracks_offset_and_frontier() {
        let clips = seamless_branching_clips();
        let mut tc = TimelineContinuity::with_clips(&clips, crate::disc::ContentFormat::BdTs);
        assert!(tc.seams.is_some(), "a multi-clip title must get a plan");

        let c0_in = mpls_ticks_to_ns(clips[0].in_time);
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);

        let first = tc.map(c0_in, true, 0, true, None).expect("first frame");
        assert_eq!(first, 0, "clip 0 starts the output timeline at zero");
        assert_eq!(
            tc.offset_ns, -c0_in,
            "offset is the correction actually applied"
        );
        assert_eq!(tc.high_ns, Some(0), "video advances the frontier");

        let later = tc
            .map(c0_in + 5_000_000_000, true, 0, true, None)
            .expect("later frame");
        assert_eq!(later, 5_000_000_000);
        assert_eq!(tc.high_ns, Some(5_000_000_000), "frontier follows video");

        // A passive track must NOT advance the frontier.
        let before = tc.high_ns;
        tc.map(c0_in + 1_000_000_000, false, 1, false, None)
            .expect("audio");
        assert_eq!(tc.high_ns, before, "passive tracks never move the frontier");

        // Across the join the frontier keeps rising, never rewinds.
        let across = tc.map(c0_out, true, 0, true, None).expect("clip 0 OUT");
        assert!(
            across >= 5_000_000_000,
            "timeline must not rewind at a join"
        );
    }

    /// A frame outside every clip's marks is dropped, and the drop is COUNTED.
    /// Audit finding: an uncounted drop is the silent-wrong-output shape this
    /// project has shipped before.
    #[test]
    fn frames_outside_the_marks_are_dropped_and_counted() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_in = mpls_ticks_to_ns(clips[0].in_time);
        assert_eq!(
            plan.place(c0_in - 5_000_000_000, 0, true, None),
            None,
            "dropped"
        );
        assert_eq!(plan.dropped_for(0), 1, "and counted");
        assert_eq!(plan.dropped_for(1), 0, "counted per track, not globally");
        plan.place(c0_in, 0, true, None).expect("inside");
        assert_eq!(
            plan.dropped_for(0),
            1,
            "a placed frame must not count as dropped"
        );
    }

    // A SPARSE passive track crosses even when its first frame after the
    // join lands well past the mark. See
    // docs/mux-timeline.md#a_sparse_passive_track_crosses_late.
    #[test]
    fn a_sparse_passive_track_crosses_late() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_in = mpls_ticks_to_ns(clips[0].in_time);
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);
        let c1_in = mpls_ticks_to_ns(clips[1].in_time);

        // The track's last event in clip 0, near its OUT.
        let tail = c0_out - 100_000_000;
        let placed_tail = plan.place(tail, 1, false, None).expect("clip 0 tail");
        assert_eq!(placed_tail, tail - c0_in, "tail rides clip 0's offset");

        // First event in clip 1 steps back (overlap) but lands 400ms past IN (sparse
        // track). The old +/-250ms window missed this, mistiming it on clip 0.
        let late = c1_in + 400_000_000;
        assert!(late < tail, "fixture: this is a backward step");
        assert!(
            (late - c1_in) > CLIP_START_TOLERANCE_NS,
            "fixture: further past the mark than the video window allows"
        );
        let got = plan
            .place(late, 1, false, None)
            .expect("late event must be placed");

        let clip1_offset = (c0_out - c0_in) - c1_in;
        assert_eq!(
            got,
            late + clip1_offset,
            "sparse track must cross to clip 1"
        );
        assert!(got > placed_tail, "and must not rewind the timeline");
    }

    /// A video reorder dip inside the overlap window must NOT be read as a
    /// crossing. This is the other side of the sparse-track fix: video keeps
    /// the tight window precisely because its backward steps are also reorder.
    #[test]
    fn a_video_reorder_dip_in_the_overlap_is_not_a_crossing() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);
        let c1_in = mpls_ticks_to_ns(clips[1].in_time);
        // Video near the end of clip 0 — inside the overlap, so these PTS are
        // also inside clip 1's range.
        let a = c0_out - 300_000_000;
        let base = plan.place(a, 0, true, None).expect("in clip 0");
        // A reorder dip of ~42ms: backward, and >= clip 1's IN.
        let dip = a - 42_000_000;
        assert!(
            dip >= c1_in,
            "fixture: the dip is inside clip 1's range too"
        );
        let got = plan.place(dip, 0, true, None).expect("dip placed");
        assert_eq!(
            got,
            base - 42_000_000,
            "a reorder dip must stay on clip 0, not jump to clip 1's offset"
        );
    }

    // Each track crosses a join on its OWN frame (regression for a shared
    // cursor). See docs/mux-timeline.md#each_track_crosses_a_join_on_its_own_frame.
    #[test]
    fn each_track_crosses_a_join_on_its_own_frame() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_in = mpls_ticks_to_ns(clips[0].in_time);
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);
        let c1_in = mpls_ticks_to_ns(clips[1].in_time);

        // Audio (track 1) runs up to near clip 0's OUT.
        let tail = c0_out - 200_000_000;
        plan.place(c0_in, 1, false, None).expect("audio start");
        let a_tail = plan.place(tail, 1, false, None).expect("audio tail");

        // Video (track 0) crosses into clip 1 first.
        plan.place(c0_out, 0, true, None)
            .expect("video at clip 0 OUT");
        plan.place(c1_in, 0, true, None)
            .expect("video at clip 1 IN");

        // Audio's NEXT tail frame still belongs to clip 0 and must stay there —
        // contiguous with the previous one, not thrown forward by the overlap.
        let a_tail2 = plan
            .place(tail + 10_000_000, 1, false, None)
            .expect("audio tail continues");
        assert_eq!(
            a_tail2 - a_tail,
            10_000_000,
            "audio tail must stay on clip 0's offset while video is already in clip 1"
        );

        // When audio itself steps back to clip 1's IN, it crosses — and lands
        // after its own tail, with no rewind and no collision.
        let a_new = plan.place(c1_in, 1, false, None).expect("audio crosses");
        assert!(
            a_new > a_tail2,
            "audio must not rewind at the join (got {a_new} after {a_tail2})"
        );
    }

    // Only Blu-ray gets a mark-driven plan: HD-DVD/DVD marks are not PES-clock
    // positions. See docs/mux-timeline.md#only_blu_ray_gets_a_mark_driven_plan.
    #[test]
    fn only_blu_ray_gets_a_mark_driven_plan() {
        let clips = seamless_branching_clips();
        let bd = TimelineContinuity::with_clips(&clips, crate::disc::ContentFormat::BdTs);
        assert!(bd.seams.is_some(), "Blu-ray marks are PES-clock positions");

        // The same table under the program-stream formats (DVD, HD-DVD).
        let ps = TimelineContinuity::with_clips(&clips, crate::disc::ContentFormat::MpegPs);
        assert!(
            ps.seams.is_none(),
            "DVD and HD-DVD must keep the inference path"
        );

        // And an HD-DVD-shaped table — contiguous, title-relative, strictly
        // increasing, so the shared-clock check alone would have accepted it.
        let hddvd: Vec<crate::disc::Clip> = [(0u32, 132_690_000u32), (132_690_000, 288_489_000)]
            .iter()
            .enumerate()
            .map(|(i, &(in_time, out_time))| crate::disc::Clip {
                feed_span: None,
                clip_id: format!("{i}"),
                in_time,
                out_time,
                duration_secs: 0.0,
                source_packets: 0,
            })
            .collect();
        assert!(
            SeamPlan::from_clips(&hddvd).is_some(),
            "fixture: the shared-clock check does NOT reject this table"
        );
        assert!(
            TimelineContinuity::with_clips(&hddvd, crate::disc::ContentFormat::MpegPs)
                .seams
                .is_none(),
            "so the format gate is what keeps HD-DVD off the plan"
        );
    }

    // A second VIDEO track (a Dolby Vision EL) must get the reorder-safe
    // crossing window even though it does not drive epochs. See
    // docs/mux-timeline.md#a_second_video_track_keeps_the_reorder_safe_window.
    #[test]
    fn a_second_video_track_keeps_the_reorder_safe_window() {
        let clips = seamless_branching_clips();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        let c0_out = mpls_ticks_to_ns(clips[0].out_time);
        let c1_in = mpls_ticks_to_ns(clips[1].in_time);

        // Track 2 is the enhancement layer: video, does not drive epochs.
        let a = c0_out - 300_000_000;
        let base = plan.place(a, 2, true, None).expect("EL in clip 0");
        let dip = a - 42_000_000;
        assert!(
            dip >= c1_in,
            "fixture: the dip lies inside clip 1's range too"
        );
        let got = plan.place(dip, 2, true, None).expect("dip placed");
        assert_eq!(
            got,
            base - 42_000_000,
            "an enhancement layer's reorder dip must not be read as a join"
        );
    }

    // A clip table that is not one advancing clock must fall back to
    // inference rather than be placed (a missed crossing strands a track).
    // See docs/mux-timeline.md#a_restarting_clock_falls_back_to_inference.
    #[test]
    fn a_restarting_clock_falls_back_to_inference() {
        let mk = |marks: &[(u32, u32)]| -> Vec<crate::disc::Clip> {
            marks
                .iter()
                .enumerate()
                .map(|(i, &(in_time, out_time))| crate::disc::Clip {
                    feed_span: None,
                    clip_id: format!("{i}"),
                    in_time,
                    out_time,
                    duration_secs: 0.0,
                    source_packets: 0,
                })
                .collect()
        };
        // Clip 1 restarts near zero instead of continuing clip 0's clock.
        let restarting = mk(&[(188_955_000, 271_486_824), (0, 12_462_450)]);
        assert!(
            SeamPlan::from_clips(&restarting).is_none(),
            "a restarting clock must not be placed from marks"
        );
        // Equal IN marks are just as unplaceable.
        let repeated = mk(&[(1_000, 2_000), (1_000, 3_000)]);
        assert!(
            SeamPlan::from_clips(&repeated).is_none(),
            "duplicate IN marks"
        );
        // The real advancing table still gets a plan.
        assert!(
            SeamPlan::from_clips(&seamless_branching_clips()).is_some(),
            "an advancing table must still be placed"
        );
    }

    // Clips whose marks chain contiguously must come out byte-identical to
    // the old behaviour. See docs/mux-timeline.md#contiguous_clips_produce_a_constant_offset.
    #[test]
    fn contiguous_clips_produce_a_constant_offset() {
        // 0..2948.6667s, 2948.6667..6410.8667s, chained exactly.
        let marks = [(0u32, 132_690_000u32), (132_690_000, 288_489_000)];
        let clips: Vec<crate::disc::Clip> = marks
            .iter()
            .enumerate()
            .map(|(i, &(in_time, out_time))| crate::disc::Clip {
                feed_span: None,
                clip_id: format!("{i}"),
                in_time,
                out_time,
                duration_secs: (out_time - in_time) as f64 / 45_000.0,
                source_packets: 0,
            })
            .collect();
        let mut plan = SeamPlan::from_clips(&clips).expect("plan");
        // Every frame maps to itself: offset 0 throughout, no discontinuity.
        for &t in &[
            0i64,
            1_000_000_000,
            2_948_000_000_000,
            2_949_000_000_000,
            6_410_000_000_000,
        ] {
            assert_eq!(
                plan.place(t, 0, true, None),
                Some(t),
                "contiguous clips must not move a frame (t={t})"
            );
        }
    }

    /// No plan for sources that have no PlayItem marks — DVD, HD-DVD, and file
    /// sources keep the inference path they have always used.
    #[test]
    fn no_seam_plan_without_usable_marks() {
        assert!(SeamPlan::from_clips(&[]).is_none(), "no clips");
        let mut bad = seamless_branching_clips();
        bad[3].out_time = bad[3].in_time; // zero-length span
        assert!(
            SeamPlan::from_clips(&bad).is_none(),
            "unusable marks must fall back to inference, not guess"
        );
    }

    // A SINGLE-clip title still gets a plan: trimming to `[in, out]` matters
    // too. See docs/mux-timeline.md#single_clip_trims_content_outside_its_marks.
    #[test]
    fn single_clip_trims_content_outside_its_marks() {
        let clips = seamless_branching_clips(); // clip 0: in 4199.0s, out 6033.04s
        let mut plan = SeamPlan::from_clips(&clips[..1]).expect("a single clip is planned");

        let in_ns = mpls_ticks_to_ns(clips[0].in_time);
        let out_ns = mpls_ticks_to_ns(clips[0].out_time);

        // In-mark frames are kept and rebased to 0 at IN — and drop nothing.
        assert_eq!(plan.place(in_ns, 0, true, None), Some(0), "IN rebases to 0");
        assert_eq!(
            plan.place(in_ns + 10 * S, 1, false, None),
            Some(10 * S),
            "a mid-clip audio frame keeps its offset from IN"
        );
        assert_eq!(
            plan.place(out_ns, 2, false, None),
            Some(out_ns - in_ns),
            "a frame AT the OUT mark is inside and kept"
        );
        assert_eq!(
            plan.dropped_total(),
            0,
            "no in-mark frame may be dropped (output is unchanged for a clean disc)"
        );

        // Trailing audio past OUT is dropped AND counted, so the finish() gates
        // in the sinks can see the volume.
        assert!(
            plan.place(out_ns + 30 * S, 1, false, None).is_none(),
            "trailing audio 30s past OUT must be dropped, not emitted past the end"
        );
        assert!(
            plan.dropped_for(1) >= 1,
            "the dropped tail frame must be counted"
        );
    }

    /// Without a plan, `map` is exactly the old `adjust` and never drops.
    #[test]
    fn map_without_a_plan_is_the_old_behaviour() {
        let mut tc = TimelineContinuity::new();
        assert_eq!(tc.map(0, true, 0, true, None), Some(0));
        assert_eq!(tc.map(5 * S, true, 0, true, None), Some(5 * S));
        assert_eq!(tc.map(25 * S, false, 1, false, None), Some(25 * S));
        assert_eq!(tc.offset_ns, 0);
    }

    // PRIMARY rc3 regression: a sparse, lagging NON-VIDEO track on a
    // SINGLE-clip title must NOT inflate `offset_ns`. See
    // docs/mux-timeline.md#single_clip_late_subtitle_does_not_inflate_offset.
    #[test]
    fn single_clip_late_subtitle_does_not_inflate_offset() {
        let mut tc = TimelineContinuity::new();
        // One continuous clip: video advances steadily 0..60s, interleaved with a
        // sparse subtitle track whose PTS swings many seconds against the frontier.
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

    // PRIMARY rc3 regression (Dolby Vision dual-layer): the EL must be a
    // PASSIVE rider or every EL GOP false-triggers a clip-boundary reset. See
    // docs/mux-timeline.md#dv_enhancement_layer_does_not_drive_epochs.
    #[test]
    fn dv_enhancement_layer_does_not_drive_epochs() {
        let mut tc = TimelineContinuity::new();
        let mut max_out = i64::MIN;
        for sec in 0..=60 {
            // Base layer (track 0) drives the epoch.
            let bl = adj_video(&mut tc, sec * S);
            // EL (track 1) is a passive rider re-emitting the base layer's PTS but
            // arriving late; simulate by feeding the previous second's time (a backward swing).
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

    // Companion: a non-video frame must never ADVANCE the frontier, even one
    // far ABOVE it. See docs/mux-timeline.md#non_video_never_advances_frontier.
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

    // Regression for the originally-reported band: a LARGE, real-magnitude
    // clip-boundary back-jump on VIDEO must still rebase to one continuous
    // timeline. See docs/mux-timeline.md#continuity_large_clip_boundary_backjump_rebased.
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

    // At a REAL video-driven boundary, a lagging NON-VIDEO tail frame must be
    // REMAPPED to its true seam position with the PREVIOUS offset. See
    // docs/mux-timeline.md#non_video_straggler_remapped_to_seam_at_boundary.
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
        // A normal clip2 audio frame (raw ~1s) rides the new offset to ~601s, within
        // a backstep of the frontier, so it is not misclassified as a straggler.
        let normal = adj_other(&mut tc, S);
        assert_eq!(normal, S + 600 * S + DISCONTINUITY_GAP_NS);
    }

    // Regression for the over-eager straggler clamp: a NORMAL new-epoch
    // non-video frame leading the frontier must ride the CURRENT offset, not
    // be demoted. See docs/mux-timeline.md#normal_new_epoch_frame_leading_frontier_is_not_clamped.
    #[test]
    fn normal_new_epoch_frame_leading_frontier_is_not_clamped() {
        let mut tc = TimelineContinuity::new();
        // Clip1 video rises to 600s, then clip2 resets to 0 → boundary.
        for i in 0..=600 {
            adj_video(&mut tc, i * S);
        }
        let frontier = tc.high_ns.unwrap();
        assert_eq!(frontier, 600 * S);
        let c2 = adj_video(&mut tc, 0);
        assert_eq!(c2, 600 * S + DISCONTINUITY_GAP_NS);

        // Raw ~5s frame maps to ~605s, >backstep past the 600s frontier, but its
        // prev-offset mapping (~595s) is far outside the epoch's tail, so it's a
        // legitimate new-epoch frame, not a straggler, and must ride the current offset.
        let raw = 5 * S;
        let out = adj_other(&mut tc, raw);
        assert_eq!(
            out,
            raw + 600 * S + DISCONTINUITY_GAP_NS,
            "a normal new-epoch frame leading the frontier by >3s must ride the \
             current offset, not be clamped back into the previous clip"
        );
        // And it must NOT have been demoted near the previous clip's tail (~5s).
        assert!(
            out > frontier,
            "frame must stay in the new epoch (> frontier), got {out}"
        );
    }

    // MEASURED on a real DVD title: audio arriving before its cell's video
    // must continue after the frontier, then rejoin the real offset. See
    // docs/mux-timeline.md#frames_arriving_before_their_epochs_video_ride_a_provisional_offset.
    #[test]
    fn frames_arriving_before_their_epochs_video_ride_a_provisional_offset() {
        let mut tc = TimelineContinuity::new();
        for i in 0..=600 {
            adj_video(&mut tc, i * S);
        }
        let frontier = tc.high_ns.expect("frontier");
        adj_other(&mut tc, 599 * S);

        // The next cell's audio arrives first, raw restarted at 0.
        let step = 32_000_000; // 32 ms, the measured cadence
        let a0 = adj_other(&mut tc, 0);
        let a1 = adj_other(&mut tc, step);
        let a2 = adj_other(&mut tc, 2 * step);
        assert_eq!(
            a0,
            frontier + DISCONTINUITY_GAP_NS,
            "an early frame must continue after the frontier, not land an epoch \
             in the past where the monotonic nudge crushes it"
        );
        assert_eq!(a1 - a0, step, "the run keeps its cadence");
        assert_eq!(a2 - a1, step);

        // Critically: the video timeline must be untouched. Letting a passive
        // track open a real epoch inflated a 476.776 s title to 656.216 s.
        assert_eq!(tc.offset_ns, 0, "a provisional must not move the offset");
        assert_eq!(tc.high_ns, Some(frontier), "nor the frontier");
        assert!(tc.epoch_offsets.is_empty(), "nor retire an epoch");

        // Now the video for that cell arrives and opens the epoch for real.
        let v = adj_video(&mut tc, 0);
        assert_eq!(v, frontier + DISCONTINUITY_GAP_NS);
        assert_eq!(
            tc.epoch_offsets.len(),
            1,
            "exactly one epoch, opened by video"
        );

        // The next audio frame rejoins the real offset with no seam: both are
        // `frontier - mapping + gap`, so they agree.
        let a3 = adj_other(&mut tc, 3 * step);
        assert_eq!(a3 - a2, step, "the run continues across the handover");
    }

    /// A provisional must not outlive its epoch: once the video has retired an
    /// epoch, later frames of that track take the real offset. Otherwise a track
    /// would drift away from every other one for the rest of the title.
    #[test]
    fn a_provisional_is_dropped_once_the_epoch_is_real() {
        let mut tc = TimelineContinuity::new();
        for i in 0..=600 {
            adj_video(&mut tc, i * S);
        }
        adj_other(&mut tc, 599 * S);
        adj_other(&mut tc, 0); // takes a provisional
        assert!(tc.provisional[1].is_some());

        adj_video(&mut tc, 0); // video opens the epoch for real
        adj_other(&mut tc, S); // next frame of that track
        assert!(
            tc.provisional[1].is_none(),
            "the provisional must be dropped once the real epoch exists"
        );
        assert_eq!(
            adj_other(&mut tc, 2 * S),
            2 * S + tc.offset_ns,
            "the track now rides the real offset like every other"
        );
    }

    // MEASURED on a real HD-DVD title: a straggler must be judged against
    // its OWN epoch's end, not the current frontier. See
    // docs/mux-timeline.md#a_straggler_is_judged_against_its_own_epochs_end_not_the_frontier.
    #[test]
    fn a_straggler_is_judged_against_its_own_epochs_end_not_the_frontier() {
        let mut tc = TimelineContinuity::new();
        for i in 0..=600 {
            adj_video(&mut tc, i * S);
        }
        adj_video(&mut tc, 0); // clip 2 opens; epoch 0 closed at 600 s
        for i in 0..=23 {
            adj_video(&mut tc, i * S);
        }
        let frontier = tc.high_ns.expect("frontier");

        // A clip-1 tail frame, 0.2 s before that clip ended, arriving late.
        let raw = 599 * S + 800_000_000;
        let out = adj_other(&mut tc, raw);
        assert_eq!(
            out, raw,
            "a tail straggler belongs to the epoch its raw PTS came from"
        );
        assert!(
            out < frontier,
            "it must never be flung past the frontier, got {out} vs {frontier}"
        );
    }

    /// Why the epoch HISTORY is needed and one `prev_offset_ns` is not enough:
    /// with three epochs, a straggler from the FIRST cannot be named by the
    /// immediately-previous offset at all.
    #[test]
    fn a_straggler_from_an_epoch_before_last_is_still_placed() {
        let mut tc = TimelineContinuity::new();
        for i in 0..=600 {
            adj_video(&mut tc, i * S);
        }
        adj_video(&mut tc, 0); // epoch 0 closes at 600 s
        for i in 0..=300 {
            adj_video(&mut tc, i * S);
        }
        adj_video(&mut tc, 0); // epoch 1 closes at ~900 s
        for i in 0..=50 {
            adj_video(&mut tc, i * S);
        }

        // Tail frame of epoch 0 — two epochs back.
        let raw = 599 * S + 500_000_000;
        assert_eq!(
            adj_other(&mut tc, raw),
            raw,
            "the straggler belongs to epoch 0, which `prev_offset_ns` no longer names"
        );
    }

    // A saturated frontier (`high_ns` at `i64::MAX`, from a hostile mkv://
    // timestamp) must not panic the muxer. See
    // docs/mux-timeline.md#a_saturated_frontier_does_not_overflow_on_passive_frame.
    #[test]
    fn saturated_frontier_does_not_overflow_on_passive_frame() {
        let mut tc = TimelineContinuity::new();
        // Video establishes the frontier at the saturation point.
        assert_eq!(adj_video(&mut tc, i64::MAX), i64::MAX);
        assert_eq!(tc.high_ns, Some(i64::MAX));
        // Passive frame: `high + BACKSTEP` overflowed here.
        let out = adj_other(&mut tc, 0);
        assert_eq!(out, 0, "a passive frame keeps its own mapping");
        // And a passive frame AT the frontier: `high - BACKSTEP` is the other
        // unchecked side of the straggler discriminator.
        assert_eq!(adj_other(&mut tc, i64::MAX), i64::MAX);
    }

    // The epoch-decision side of the same arithmetic: both untrusted ends
    // (i64::MIN/MAX) are reachable from container data. See
    // docs/mux-timeline.md#extreme_video_pts_does_not_overflow_the_epoch_bump.
    #[test]
    fn extreme_video_pts_does_not_overflow_the_epoch_bump() {
        let mut tc = TimelineContinuity::new();
        assert_eq!(adj_video(&mut tc, i64::MAX), i64::MAX);
        // Hard backward jump to the negative extreme: `high - adj` overflowed.
        let out = adj_video(&mut tc, i64::MIN);
        // Saturated bump (`i64::MAX`) applied to `i64::MIN` → -1, and the
        // frontier never regresses.
        assert_eq!(out, -1);
        assert_eq!(tc.high_ns, Some(i64::MAX));
    }
}
