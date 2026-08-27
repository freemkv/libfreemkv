//! Shared clip-boundary timeline corrector.
//!
//! A BD/UHD title's clips are read as one concatenated sector stream, so the
//! source PES PTS does not run continuously across a clip join. There are two
//! ways to place them, and this module holds both:
//!
//! - **From the playlist's marks** ([`SeamPlan`](crate::mux::timeline::SeamPlan)) — when the title carries
//!   PlayItem IN/OUT times, each clip contributes exactly `out - in` and the
//!   clips are laid end to end. This is exact: it closes forward skips, joins
//!   overlaps without rewinding, and drops material the playlist excludes.
//! - **By inference** ([`TimelineContinuity::adjust`](crate::mux::timeline::TimelineContinuity::adjust)) — when there are no
//!   usable marks (DVD, HD-DVD, `mkv://` / `m2ts://` sources), a backward PTS
//!   jump larger than [`DISCONTINUITY_BACKSTEP_NS`](crate::mux::timeline::DISCONTINUITY_BACKSTEP_NS) is read as a join and
//!   rebased. Inference cannot see a forward skip, because a forward gap is
//!   indistinguishable from frames lost to damaged media, and cannot see an
//!   overlap smaller than the reorder threshold.
//!
//! [`TimelineContinuity::map`](crate::mux::timeline::TimelineContinuity::map) picks between them: marks when present,
//! inference otherwise. Every muxer/sink that consumes the interleaved per-track
//! PES stream and emits a monotonic timeline (the MKV muxer, the `demux://`
//! elementary-stream sink) goes through it, so the correction lives in exactly
//! one place.

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

/// How close a frame's PTS must be to a clip's IN mark to be recognised as that
/// clip's opening frame.
///
/// At an OVERLAP join the next clip's IN sits inside the current clip's range,
/// so "past the current OUT" never fires and the two clips share a PTS band.
/// The clips are concatenated in file order, though, so the new clip opens ON
/// its IN mark — this window is what tells that opening frame apart from the
/// old clip's tail. One video frame is ~42 ms at 24 fps; 250 ms allows for a
/// clip whose first frame sits a few frames past its mark without ever reaching
/// the next join.
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
    /// Byte range this clip occupies in the title's feed, when known.
    ///
    /// When a frame carries its source offset this makes clip assignment a
    /// LOOKUP. Inside an overlap two clips' mark ranges both contain the same
    /// timestamp, so no rule over timestamps alone can say which clip a frame
    /// came from — four audit rounds each fixed one such rule and broke
    /// another. The byte offset falls in exactly one span.
    pub(crate) feed_span: Option<(u64, u64)>,
}

/// The playlist's own answer to "where does each clip belong on the timeline".
///
/// A seamless-branching title's PlayItems do NOT chain contiguously in the
/// shared clock: one clip's OUT may sit *after* the next clip's IN (overlap —
/// the disc stores the join twice so a player can switch without a gap), or
/// *before* it (skip — the playlist jumps over material). Measured on one real
/// UHD title, `00801.mpls`, 11 PlayItems:
///
/// ```text
/// clip 0  in 4199.0000  out 6033.0405   cum_start 0.0000
/// clip 1  in 6031.2500  out 6308.1933   cum_start 1834.0405   <- 1.79s OVERLAP
/// clip 2  in 6298.1667  out 6875.0763   cum_start 2110.9839
/// clip 3  in 6884.2500  out 6948.0220   cum_start 2687.8935   <- 9.17s SKIP
/// ```
///
/// Inferring seams from PTS jumps cannot recover this. A forward jump is
/// ambiguous — it means "the playlist skipped" OR "we lost frames to damaged
/// media", and compressing the latter would silently falsify timing on exactly
/// the rips that most need it faithful. An overlap smaller than the B-frame
/// reorder threshold is invisible to inference entirely, and its duplicated
/// content then collides in the muxer.
///
/// So the marks are read rather than guessed. Each clip contributes exactly
/// `out − in` to the output, laid end to end: gaps never become dead timeline,
/// and material outside a clip's marks is dropped rather than emitted twice.
pub(crate) struct SeamPlan {
    /// Whether the per-clip feed spans can be trusted to identify a clip from a
    /// frame's byte offset.
    ///
    /// True only when the spans tile the feed contiguously from 0 with no gap
    /// or overlap. Anything else means the scan's view of the extents and the
    /// mux's differ, and a byte offset would then select a confidently WRONG
    /// clip for every frame — a worse failure than the mark heuristics, which
    /// are at least approximately right. In that case provenance is disabled
    /// and the heuristics are used, which is the 1.6.0 behaviour.
    spans_trusted: bool,
    clips: Vec<SeamClip>,
    /// Frames dropped because they fell outside every clip's marks, per track.
    ///
    /// A drop is correct — the playlist does not include that material — but a
    /// SILENT drop is how this codebase has produced complete-looking, wrong
    /// output before. Counting them means an unexpected volume shows up in the
    /// log instead of in someone's file, and gives a caller something to assert
    /// on. Indexed by track alongside `cursors`.
    dropped: Vec<u64>,
    /// Per-track position: (clip index, last raw PTS seen).
    ///
    /// Each track crosses a join on ITS OWN frame, not on video's. The demuxer
    /// interleaves the tracks, so when video enters the next clip the previous
    /// clip's audio and subtitle tails are still arriving — and at an OVERLAP
    /// join those tail frames fall inside BOTH clips' mark ranges, so there is
    /// no way to place them from the PTS alone. Sharing one cursor gave the
    /// tail the new clip's offset, which threw it forward by the overlap and
    /// made it collide with the new clip's own frames; the muxer's monotonic
    /// nudge then flattened the collision onto the tick floor, which is exactly
    /// the audio-ahead-of-picture symptom this type exists to remove.
    ///
    /// Indexed by track; grows on demand.
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
    /// Build a plan from a title's clips, or `None` when there is nothing to
    /// place: no clips (DVD, HD-DVD, `mkv://`/`m2ts://` sources — none of which
    /// carry PlayItem marks), or marks that are not usable (a zero/inverted span
    /// means the playlist is not telling us anything we can act on, and guessing
    /// is what this type exists to avoid).
    ///
    /// A **single** clip still gets a plan. Joining is not the marks' only job —
    /// trimming to `[in, out]` is — and it matters whenever a clip's physical
    /// extent runs past its OUT mark. Real discs author trailing audio (a fade
    /// after the last video frame) beyond OUT in the m2ts; without a plan the
    /// inference path keeps it, leaving audio seconds past the declared duration.
    /// One clip needs no cross-clip placement, so the loop below reduces to the
    /// `[in, out]` drop filter plus the standard `offset = −in_ns` rebase — the
    /// same one clip 0 of a multi-clip title already gets, which the MKV muxer
    /// then re-anchors, so every KEPT frame is byte-identical to the no-plan
    /// path. The only change is that out-of-mark frames are now dropped.
    ///
    /// Returning `None` leaves [`TimelineContinuity`] on its PTS-jump inference,
    /// which is what every non-BD path has always used.
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

    /// Which clip owns feed byte `b`, by BINARY SEARCH.
    ///
    /// `spans_trusted` guarantees the spans tile the feed contiguously in
    /// order, so this is a partition point rather than a scan. That matters:
    /// discs in the test hoard reach 900 clips, and this runs once per frame
    /// per track — a linear scan would be ~900 comparisons on every one of
    /// millions of frames, which is real time spent for no reason.
    ///
    /// A repeated clip reuses its first reference's span, so the search lands
    /// on the FIRST entry with that span. The bytes are only read once, so the
    /// material is emitted once, at that entry's offset.
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

    /// Pick the member of a shared-span run whose marks contain `raw_ns`.
    ///
    /// `clip_at_byte` deliberately answers with the FIRST entry of a run so the
    /// lookup is stable. That is the right answer when the run is one clip; when
    /// several PlayItems reference one file it is only the right STARTING point,
    /// because they differ solely in their marks. Falls back to the first when
    /// none contains the timestamp, which keeps the existing drop behaviour for
    /// material genuinely outside every reference.
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

/// Global timeline corrector.
///
/// Holds a [`SeamPlan`] when the title's PlayItem marks are usable, and falls
/// back to the PTS-jump inference described below when they are not. The
/// inference documentation that follows applies to the FALLBACK path only —
/// under a plan, placement comes from the marks and none of the epoch/frontier
/// reasoning below decides anything.
///
/// freemkv reads a BD title's clips as one concatenated sector stream, so at a
/// non-seamless boundary the source PES PTS jumps backward. Left uncorrected, that produces a sustained band of
/// non-monotonic block timestamps (a downstream muxer then derives
/// non-monotonic DTS from them).
///
/// A single running `offset_ns` is applied to EVERY track, so the concatenated
/// clips form one monotonic timeline AND A/V sync is preserved (all tracks at a
/// boundary shift by the same amount). It is global, not per-track: a clip
/// boundary resets every stream together by the same delta.
///
/// **Only the VIDEO track drives epoch decisions.** A title carries one video
/// track plus many interleaved audio + subtitle tracks (one UHD title: 2 video,
/// 11 audio, 32 PGS). Those non-video tracks are sparse and lag the video by
/// seconds, so their raw PTS swing well over the 3 s discontinuity threshold
/// against a shared frontier even within a SINGLE clip — a late subtitle PTS
/// would ratchet `high_ns` up, then the next normal video frame would sit >3 s
/// below it and be misread as a clip boundary, permanently bumping `offset_ns`.
/// That false-positive ratchet (firing thousands of times on a one-clip title)
/// inflated that title's cluster/Cue timestamps into the billions of ms and
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
    /// The playlist's clip placement, when the source has one. Present = the
    /// marks are known and are used verbatim; absent = fall back to inferring
    /// seams from PTS jumps, which is all any non-BD source has ever had.
    pub(crate) seams: Option<SeamPlan>,
    /// Every epoch already left behind, oldest first, as
    /// `(offset, frontier when it closed)`. A straggler carries a raw PTS from
    /// one of THESE, and a single `prev_offset_ns` cannot name the right one
    /// once a title has more than two epochs. The closing frontier is what makes
    /// the test meaningful: a straggler sits at the TAIL of its own epoch, which
    /// is only recognisable against that epoch's end — not against the current
    /// frontier, which by then may be a whole title away. Bounded: a source that
    /// rebases forever must not grow this.
    epoch_offsets: Vec<(i64, i64)>,
    /// Last raw PTS seen per track, for spotting a track's OWN discontinuity.
    /// Within an epoch a passive track's PTS only advances (audio and subtitles
    /// do not reorder, and a passive video track's B-frame dip is far under the
    /// backstep), so a large BACKWARD step is unambiguous. This is a different
    /// signal from the shared frontier, which is what the old false-positive
    /// ratchet keyed on.
    last_raw_ns: Vec<Option<i64>>,
    /// Per-track provisional offset for frames that arrive BEFORE the video
    /// frame opening their epoch, as `(epochs retired when it was taken,
    /// offset)`.
    ///
    /// It is deliberately private to one track and never written to
    /// `offset_ns`, never advances `high_ns`, and never retires an epoch — so
    /// it cannot move the video timeline. Letting a passive track open a real
    /// epoch was tried and inflated a 476.776 s title to 656.216 s, because
    /// every track observes a boundary at its own pace and the video path
    /// rebased again on top of whatever they had done.
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

    /// Corrector driven by a title's PlayItem marks where they exist.
    ///
    /// Falls back to [`Self::new`]'s inference when the title has no clips or
    /// its marks are unusable — so DVD, HD-DVD, `mkv://` and `m2ts://` sources
    /// behave exactly as before. A single BD clip DOES get a plan: its marks
    /// still trim trailing/pre-roll material outside `[in, out]`.
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

    /// Total frames dropped for falling outside the playlist's clip marks.
    ///
    /// Zero for a title without a seam plan. A muxer reports this when it
    /// finishes so a drop is never invisible: dropping is correct at a join,
    /// but an unexpected VOLUME of drops is how output ends up quietly short.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.seams.as_ref().map_or(0, |p| p.dropped_total())
    }

    /// Map a raw PES PTS onto the output timeline, or `None` to drop the frame.
    ///
    /// Dropping only ever happens under a [`SeamPlan`]: it is material outside
    /// the playlist's marks, which the title does not include.
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

    /// The offset a passive frame should ride, and the bookkeeping around it.
    ///
    /// Returns the effective offset for THIS frame. Normally that is the
    /// current epoch's. It differs only for a frame that arrived ahead of the
    /// video that opens its epoch: such a frame's own raw PTS has just jumped
    /// backwards AND its current-epoch mapping lands a whole epoch below the
    /// frontier, which no in-epoch frame ever does.
    ///
    /// A provisional is dropped the moment the video actually retires an epoch,
    /// so the run rejoins the real offset with no seam — the two agree because
    /// both are `frontier - mapping + gap`.
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

    /// Retire the current epoch and open a new one continuing just after the
    /// frontier.
    ///
    /// Retiring records the offset AND the frontier the epoch closed at. The
    /// closing frontier is what later makes a straggler recognisable: it says
    /// where that epoch's tail was, which the current frontier cannot, since by
    /// then it may be a whole title further on.
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

    /// The offset of the epoch a straggler actually belongs to.
    ///
    /// A frame whose current-epoch mapping flies past the frontier carries a raw
    /// PTS from an epoch already left behind. Pick the retained epoch that lands
    /// it CLOSEST BELOW the frontier: that is where the just-ended epoch's tail
    /// was, and it can never forward-date a cluster. `None` when no retained
    /// epoch places it sanely, in which case the caller keeps the current
    /// mapping — which is exactly the pre-existing behaviour, so a title that
    /// never rebased is unaffected.
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

    /// Characterization of the BUG: a BD title's two clips concatenated with a
    /// PTS reset at the boundary. WITHOUT correction the raw VIDEO timeline goes
    /// hard backward at clip 2 (what produced the non-monotonic-DTS band on
    /// multi-clip UHD titles). WITH `TimelineContinuity` the output is monotonic and
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

    /// The output timeline must never go BACKWARDS at a clip join.
    ///
    /// Audit finding, measured against the real 00801.mpls marks below: with
    /// the cursor on clip 5 (7708.99..7910.79) a frame at 7845.00 — clip 6's
    /// pre-mark lead-in, 8s below clip 6's IN of 7853.00 — was more than the
    /// 250ms tolerance from that mark, so no crossing rule fired; the cursor
    /// stayed on clip 5, and 7845.00 IS inside clip 5's range, so the frame was
    /// PLACED with clip 5's offset. Output went backwards 65s and dropped
    /// stayed 0 — no counter, no gate, nothing noticed — while the entire
    /// overlap band was emitted a second time over clip 5's written tail.
    ///
    /// A backward step larger than DISCONTINUITY_BACKSTEP_NS cannot be B-frame
    /// reorder, so it is a new clip's file starting. Such a frame is either
    /// placed on the clip that contains it, or dropped as pre-mark material the
    /// playlist excludes — never emitted behind the frame before it.
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

    /// A glitched PTS must not strand a track on a later clip forever.
    ///
    /// Audit finding against the large-backstep branch: advancing on ANY >3s
    /// backward step also fires for a corrupt PTS, or a legitimate STC
    /// discontinuity inside one clip. Nothing moves the cursor back — a forward
    /// step matches neither `past_out` nor `stepped_back` — so every later
    /// frame sits below the new clip's IN and is dropped. On this table that is
    /// ~17 minutes of one track, and the only volume gate compares total drops
    /// against ALL tracks' frames, so it exits 0.
    ///
    /// The branch now requires the frame to be INSIDE the current clip's marks,
    /// which is the only case that would otherwise be wrongly placed.
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

    /// The three placements audit round 7 enumerated, which the previous
    /// mark-heuristic guards all got wrong in different ways.
    ///
    /// The property asserted is the one that matters and the one the heuristics
    /// only approximated: a track's output never runs backwards, and a bad
    /// frame never strands the cursor. Numbers are from the real 00801.mpls
    /// table below.
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

    /// Clips with byte spans, built from the real mark table so provenance and
    /// marks can be tested against each other.
    /// The output offset `from_clips` computes for a clip: the sum of every
    /// earlier clip's playable duration, minus its own IN.
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

    /// Every track of a clip lives in the SAME stream file, so provenance makes
    /// video, audio and subtitles agree by construction. Divergence between
    /// them — each track guessing separately under its own rule — is how audio
    /// and video ended up on different clips and drifted apart.
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

    /// A playlist may reference the same clip twice (a looped segment). The
    /// bytes are read once, so both entries share one span — that must not be
    /// read as a broken map.
    /// A clip FILE referenced by two adjacent PlayItems — a seamless split, a
    /// looped segment, multi-angle — is one file with one set of bytes, so it
    /// has ONE feed span. But each PlayItem carries its OWN marks, and the two
    /// cover different halves of it.
    ///
    /// Provenance alone cannot tell those halves apart: every frame of the file
    /// resolves to the same span. `clip_at_byte` answers with the FIRST of the
    /// run, so without the timestamp to disambiguate, every frame past the
    /// first PlayItem's OUT is judged against marks it was never inside and
    /// dropped — the second half of that clip silently missing from the rip,
    /// with the timeline still charged for its duration.
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

    /// A source that stamps no provenance (a mkv:// remux, the deserialize hop)
    /// must still work — it takes the mark heuristics, which is what it has
    /// always used and where it has always been correct, because such sources
    /// have no overlapping clips to be ambiguous about.
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

    /// On frames that are NOT ambiguous, provenance and the mark heuristics
    /// must give the SAME answer.
    ///
    /// This is the strongest available cross-check. The heuristics are wrong
    /// only inside an overlap, where two clips' mark ranges both contain the
    /// timestamp; everywhere else they are the behaviour that shipped and was
    /// verified on real discs. So for a frame that falls inside exactly one
    /// clip, the two methods disagreeing means the NEW path is wrong.
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

    /// Walk a whole title through the plan with provenance and assert the two
    /// properties that decide whether a rip is watchable: output never moves
    /// backwards for a track, and the total span matches the title's declared
    /// duration.
    ///
    /// A title can have the right TOTAL duration while being wrong in the
    /// middle, which is why monotonicity is checked per frame rather than only
    /// at the ends.
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

    /// A title whose PlayItems all reference ONE clip file: every span is
    /// identical, so the tiling check's "equal to previous is allowed" arm
    /// matches every entry and the spans are TRUSTED — while carrying no
    /// information at all about which PlayItem a byte belongs to.
    ///
    /// That combination is the dangerous one: provenance looks authoritative
    /// and is actually blind, so every frame resolves to the FIRST PlayItem and
    /// everything past its mark range is dropped.
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

    /// Marks that do not advance across the title are normal — each clip file
    /// carries its own STC — and are now PLACED, because every track carries a
    /// source byte offset and the clip comes from that, not from the marks.
    ///
    /// This was refused before, which dropped exactly the branched titles the
    /// seam plan exists for onto the inference path that cannot read them.
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

    /// The refusal must SURVIVE where it is actually load-bearing: a
    /// non-monotonic table whose spans cannot be trusted has neither a usable
    /// clock nor a usable byte offset, so there is nothing to place with and
    /// inference remains the only safe path.
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

    /// The plan's total must equal the title's declared duration.
    ///
    /// This is the whole bug in one assertion: the delivered file declared
    /// 7893.385 s and carried packets to 8029.298 s — 135.91 s of timeline the
    /// playlist says does not exist.
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

    /// The 9.174 s forward skip between clip 2 and clip 3 must vanish.
    ///
    /// Measured in the delivered file as a 20 s window holding 257 video packets
    /// where it should hold 480.
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

    /// The 1.79 s overlap at seam 1 must JOIN cleanly, not rewind the timeline.
    ///
    /// Clip 1's IN (6031.250 s) precedes clip 0's OUT (6033.041 s): the disc
    /// stores that join twice. Emitting both copies is what collided in the
    /// muxer and flattened 169 audio packets onto the 0.1 ms tick floor,
    /// putting audio ~1.8 s ahead of picture for the rest of the film.
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

    /// `map()`'s seam-plan branch — the glue between `SeamPlan::place` and the
    /// frontier/offset bookkeeping — was untested. Audit finding: a wrong
    /// operand in `offset_ns = p - raw_pts_ns`, or a stale `high_ns` across a
    /// join, would corrupt downstream cluster timing and no test would notice.
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

    /// A SPARSE passive track crosses even when its first frame after the join
    /// lands well past the mark.
    ///
    /// Audit finding. A PGS subtitle track may have no event near a clip's IN
    /// at all. Holding it to the dense-video window (250ms either side of the
    /// mark) left it on the PREVIOUS clip's offset until its PTS finally passed
    /// that clip's OUT — mistiming every subtitle in between by the overlap,
    /// 1.79s on the measured title.
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

    /// Each track crosses a join on its OWN frame.
    ///
    /// This is the regression for the first attempt at this fix, which gave
    /// every track the cursor the video had moved. At an overlap the previous
    /// clip's audio tail is still arriving after video has crossed, and those
    /// tail frames sit inside BOTH clips' ranges — so a shared cursor gave them
    /// the new clip's offset, threw them forward by the overlap, and made them
    /// collide with the new clip's own audio. Measured on a real remux: 169
    /// audio packets flattened onto the 0.1 ms tick floor and a 1.80 s jump,
    /// i.e. the original symptom, still present after the timeline length was
    /// already correct.
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

    /// Only Blu-ray gets a mark-driven plan.
    ///
    /// Audit finding, and a regression this nearly shipped: HD-DVD `Clip` marks
    /// come from the XPL's title-relative times, and a DVD's from cell tables —
    /// neither is a position in the PES clock. A plan built from them is an
    /// identity map with a drop filter: it suppresses the layer-break rebase
    /// `adjust` performs, and drops whatever falls outside marks the PTS was
    /// never measured against. Both formats must stay on inference.
    ///
    /// An earlier reading of this concluded HD-DVD was safe because its marks
    /// happen to be contiguous, so the computed offsets were all zero. That is
    /// true and irrelevant: the offsets were zero in the WRONG CLOCK.
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

    /// A second VIDEO track (a Dolby Vision enhancement layer) must get the
    /// reorder-safe crossing window even though it does not drive epochs.
    ///
    /// Audit finding: the rule was keyed on `drives`, so the EL took the branch
    /// whose premise is "no reorder". Its ordinary reorder dip near the end of a
    /// clip — a backward step that, during an overlap, also lands inside the
    /// next clip's range — was then read as a join, and the EL was placed on the
    /// next clip's offset: out of step with the base-layer frame it must be
    /// co-timed with, by the width of the overlap.
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

    /// A clip table that is not one advancing clock must fall back to
    /// inference rather than be placed.
    ///
    /// Audit finding. Each clip is validated in isolation (span > 0) but the
    /// placement rules assume the marks are points on a single clock. Under a
    /// table whose clips each restart their own base, a crossing can be missed
    /// — and a missed crossing STRANDS the track on its current clip, so every
    /// later frame falls outside that clip's marks and is dropped for the rest
    /// of the title. Silent truncation, which is what this type exists to
    /// prevent.
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

    /// Clips whose marks chain contiguously must come out byte-identical to the
    /// old behaviour: a constant offset, nothing moved, nothing dropped.
    ///
    /// This is the no-regression guarantee for every title that is multi-clip
    /// but not seamless-branching — HD-DVD's feature is chaptered this way (one
    /// real title measured: 3 clips, each IN equal to the previous OUT).
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

    /// A SINGLE-clip title still gets a plan: joining is not the marks' only job,
    /// trimming to `[in, out]` is. Real discs author trailing audio (a fade after
    /// the last video frame) PAST the OUT mark in the m2ts; the no-plan inference
    /// path kept it, leaving audio seconds past the declared duration — the
    /// audio-drift defect found on a real disc (audio +35.6 s past a single-clip title's
    /// end). The plan drops it, and every KEPT frame is placed exactly as the
    /// no-plan path would (raw rebased by `−in_ns`, which the MKV muxer already
    /// does), so a disc with no out-of-mark content is byte-identical.
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

    /// PRIMARY rc3 regression: a sparse, lagging NON-VIDEO track (PGS subtitle /
    /// trailing audio) on a SINGLE-clip title must NOT inflate `offset_ns`. This
    /// is the exact false-positive that destroyed a real title's seek index: with a
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

    /// PRIMARY rc3 regression (Dolby Vision dual-layer): a SECOND video track —
    /// the DV enhancement layer — runs its OWN PTS timeline interleaved with the
    /// base layer's, so the two video PTS sequences OVERLAP. The EL must be a
    /// PASSIVE rider (drives_epoch == false): if it drove epochs, every EL GOP
    /// would look like a multi-second backward jump against the base-layer
    /// frontier and false-trigger a clip-boundary reset — the exact ratchet that
    /// inflated a 1-clip 1h49m timeline to ~7 h. Here the base layer
    /// advances 0..60s while the EL re-emits the SAME 0..60s interleaved; the
    /// timeline must stay at 60s with offset 0.
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

    /// Regression for the originally-reported band: a LARGE, real-magnitude
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
        // A normal clip2 audio frame (raw ~1s) rides the new offset to ~601s, within
        // a backstep of the frontier, so it is not misclassified as a straggler.
        let normal = adj_other(&mut tc, S);
        assert_eq!(normal, S + 600 * S + DISCONTINUITY_GAP_NS);
    }

    /// Regression for the over-eager straggler clamp: a NORMAL new-epoch
    /// non-video frame that leads the (sparse, video-only) frontier by MORE than
    /// one backstep must ride the CURRENT offset — it must NOT be demoted into
    /// the just-ended clip's epoch. Such a frame satisfies BOTH of the old
    /// discriminator's conditions (current-map > frontier+backstep AND
    /// prev-map <= frontier), so the old `prev_mapped <= high` test wrongly
    /// clamped it back ~a whole clip. The tightened lower bound
    /// (`prev_mapped >= high - backstep`) fixes it.
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

    /// MEASURED on a real DVD title with 8 cell boundaries. The demuxer hands
    /// the muxer ~18 audio frames of the NEXT cell before that cell's first
    /// video frame. Riding the just-ended epoch's offset put them ~21 s in the
    /// past, and the MKV writer's strictly-monotonic nudge then crushed the run
    /// onto one instant 0.1 ms apart — half a second of audio as a click,
    /// eight times in an 8-minute title.
    ///
    /// They must instead continue after the frontier, and must rejoin the real
    /// offset seamlessly once the video opens the epoch.
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

    /// MEASURED on a real HD-DVD title. Its second audio track's LAST frame
    /// carries a clip-1 raw PTS but arrives after clip 2's video opened the
    /// epoch, so it took clip 2's offset and landed at 12834.587 s in a
    /// 6434.100 s title — one packet, exactly double.
    ///
    /// The old remap existed for this and refused it: the frame sits 23 s below
    /// the CURRENT frontier, outside the 3 s window. Judged against the frontier
    /// of the epoch it actually belongs to, it is 0.15 s from that epoch's end.
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

    /// A saturated frontier must not panic the muxer. An `mkv://` source's
    /// tick→ns multiply saturates at `i64::MAX` (mkvstream's `parse_block`), so a
    /// hostile TimestampScale/CLUSTER_TIMESTAMP puts `high_ns` AT `i64::MAX`.
    /// Every subsequent PASSIVE frame then evaluated `high + BACKSTEP`, which
    /// panicked ("attempt to add with overflow") out of the public
    /// `Stream::write` path in any overflow-checked build.
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

    /// The epoch-decision side of the same arithmetic: `adj < high - BACKSTEP`
    /// and the `high - adj` bump both took untrusted ends. A frontier at
    /// `i64::MIN`-adjacent values (a negative SimpleBlock-relative timestamp) and
    /// a `i64::MAX` frontier are both reachable from container data.
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
