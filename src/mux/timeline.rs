//! Shared clip-boundary timeline corrector.
//!
//! A BD/UHD title's clips are read as one concatenated sector stream, so the source PES PTS
//! does not run continuously across a clip join. [`SeamPlan`] places frames exactly from the
//! playlist's marks when present; [`TimelineContinuity::adjust`] infers seams from PTS jumps
//! otherwise, and [`TimelineContinuity::map_picture`] picks between them so every muxer/sink shares one
//! correction path.

// A backward PTS step larger than this is a clip-boundary discontinuity (source PES PTS reset),
// NOT B-frame reorder (HEVC/H.264 tops out ~16 frames, <1s @24fps).
pub(crate) const DISCONTINUITY_BACKSTEP_NS: i64 = 3_000_000_000;
/// Sub-frame gap inserted after a rebased discontinuity so the first frame of
/// the new clip lands strictly after the previous timeline high (1 ms).
pub(crate) const DISCONTINUITY_GAP_NS: i64 = 1_000_000;

// How close a frame's PTS must be to a clip's IN mark to be recognised as that clip's opening
// frame, vs. the previous clip's overlapping tail.
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
    // Byte range this clip occupies in the title's feed, when known. A byte offset resolves an
    // overlap that timestamps alone cannot.
    pub(crate) feed_span: Option<(u64, u64)>,
}

// The playlist's own answer to "where does each clip belong on the timeline": a
// seamless-branching title's PlayItems can overlap or skip in the shared clock.
pub(crate) struct SeamPlan {
    // Whether the per-clip feed spans tile the feed contiguously from 0, so a byte offset can
    // be trusted to identify a clip. If not, provenance is disabled.
    spans_trusted: bool,
    clips: Vec<SeamClip>,
    // Frames dropped (per track) for falling outside every clip's marks, or for predicting
    // from a picture that did. Counted so an unexpected volume is visible instead of silent.
    dropped: Vec<u64>,
    // Per-track position: (clip index, last raw PTS seen). Each track crosses a join on its OWN
    // frame, since overlap tails arrive after the next clip's video.
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
    /// What this video track's trims took from the pictures after them.
    refs: TrimRefs,
}

/// A video picture as the seam plan sees it: enough to keep its trims decodable.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SeamPic {
    pub(crate) keyframe: bool,
    pub(crate) coding: Option<crate::mux::codec::PictureInfo>,
}

/// Whether a video track's kept pictures still have their references after a trim. A mark
/// need not sit on a random-access picture: a player decodes from it and shows from the
/// mark, but this output can only drop, so a picture predicting from a trimmed one goes too.
#[derive(Debug, Clone, Copy, Default)]
struct TrimRefs {
    /// A picture later ones may reference was not kept: drop up to the next keyframe.
    broken: bool,
    /// The keyframe that ended a broken run (PTS): its leading pictures predict from before it.
    resumed_at: Option<i64>,
    /// The last keyframe's PTS: a picture displayed before it is leading, and no trailing
    /// picture predicts from a leading one.
    key_ns: Option<i64>,
}

impl TrimRefs {
    // Decide a picture (decode order) the marks `placed`; true keeps it.
    fn admit(&mut self, raw_ns: i64, placed: bool, pic: SeamPic) -> bool {
        if pic.keyframe {
            self.resumed_at = self.broken.then_some(raw_ns);
            self.broken = false;
            self.key_ns = Some(raw_ns);
        }
        let leading = !pic.keyframe && self.key_ns.is_some_and(|k| raw_ns < k);
        let keep = placed && !self.broken && !(leading && self.resumed_at.is_some());
        // Unknown coding may be a reference; an MPEG-2 B-picture never is.
        let referenced = pic.coding.is_none_or(|c| c.may_be_referenced());
        if !keep && !leading && referenced {
            self.broken = true;
        }
        keep
    }
}

impl SeamPlan {
    // Build a plan, or `None` when there's nothing to place (no clips, or unusable marks). A
    // single clip still gets a plan, to trim to `[in, out]`.
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

    // Which clip owns feed byte `b`, by BINARY SEARCH (spans tile the feed contiguously, so a
    // scan would be ~900 comparisons per frame on a large disc).
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

    // Pick the member of a shared-span run whose marks contain `raw_ns` (several PlayItems can
    // reference one file). Falls back to `first`.
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

    /// [`Self::place`] a video picture, also dropping one that predicts from a picture
    /// the marks dropped (see [`TrimRefs`]).
    fn place_picture(
        &mut self,
        raw_ns: i64,
        track: usize,
        has_reorder: bool,
        src_byte: Option<u64>,
        pic: SeamPic,
    ) -> Option<i64> {
        let placed = self.place(raw_ns, track, has_reorder, src_byte);
        let keep = self.cursors[track]
            .refs
            .admit(raw_ns, placed.is_some(), pic);
        if placed.is_some() && !keep {
            self.dropped[track] = self.dropped[track].saturating_add(1);
            return None;
        }
        placed
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
                    refs: TrimRefs::default(),
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
                refs: self.cursors[track].refs,
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
            refs: self.cursors[track].refs,
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

// Global timeline corrector: holds a SeamPlan when usable, else falls back to PTS-jump
// inference. Only the VIDEO track drives epoch decisions.
pub(crate) struct TimelineContinuity {
    /// Offset (ns) added to raw PTS for the CURRENT epoch.
    pub(crate) offset_ns: i64,
    // Offset (ns) of the immediately previous epoch, used to remap a non-video tail straggler
    // at a boundary.
    pub(crate) prev_offset_ns: i64,
    /// Highest adjusted VIDEO PTS (ns) accepted onto the timeline so far — the
    /// running frontier. `None` until the first video frame. Only video advances
    /// it; non-video tracks never touch it.
    pub(crate) high_ns: Option<i64>,
    /// The playlist's clip placement, when the source has one. Present = the
    /// marks are known and are used verbatim; absent = fall back to inferring
    /// seams from PTS jumps, which is all any non-BD source has ever had.
    pub(crate) seams: Option<SeamPlan>,
    // Every epoch already left behind, oldest first, as (offset, frontier when it closed) — a
    // single prev_offset_ns can't name a straggler's epoch.
    epoch_offsets: Vec<(i64, i64)>,
    // Last raw PTS seen per track, for spotting a track's own discontinuity (distinct from the
    // shared frontier).
    last_raw_ns: Vec<Option<i64>>,
    /// Per-track provisional offset for frames arriving before the video frame that opens their
    /// epoch, tagged with the epoch sequence it was captured in (see `epoch_seq`). Never
    /// written to offset_ns/high_ns and never retires an epoch.
    provisional: Vec<Option<(u64, i64)>>,
    /// Monotonic epoch counter, bumped once per `open_epoch`. Used as the epoch
    /// IDENTITY a provisional is tagged with, instead of `epoch_offsets.len()`:
    /// that length is capped at MAX_EPOCHS, so past the cap it stops changing and
    /// a provisional captured then would compare equal forever and never retire —
    /// outliving its epoch and mis-offsetting later passive frames. A monotonic
    /// counter always advances. (Same root as the disc.rs slice-identity finding.)
    epoch_seq: u64,
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
            epoch_seq: 0,
        }
    }

    // Corrector driven by a title's PlayItem marks where they exist, else `Self::new`'s
    // inference.
    pub(crate) fn with_clips(
        clips: &[crate::disc::Clip],
        content_format: crate::disc::ContentFormat,
    ) -> Self {
        // Only Blu-ray: its PlayItem IN/OUT share the 45 kHz clock the PES PTS runs on.
        // HD-DVD/DVD marks come from a different clock (XPL times, cell tables) — a
        // plan from those would drop content the PTS wasn't measured against, so they keep inference.
        let seams = match content_format {
            crate::disc::ContentFormat::BdTs => SeamPlan::from_clips(clips),
            crate::disc::ContentFormat::MpegPs | crate::disc::ContentFormat::DvdPs => None,
        };
        Self {
            epoch_offsets: Vec::new(),
            last_raw_ns: Vec::new(),
            provisional: Vec::new(),
            offset_ns: 0,
            prev_offset_ns: 0,
            high_ns: None,
            seams,
            epoch_seq: 0,
        }
    }

    // Total frames dropped for falling outside the playlist's clip marks. Zero without a seam
    // plan.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.seams.as_ref().map_or(0, |p| p.dropped_total())
    }

    /// Frames dropped for falling outside the playlist's clip marks, counted
    /// ONLY over `tracks` — the numerator/denominator alignment a filtered sink
    /// needs. A `demux://` variant that persists a subset of tracks (e.g.
    /// `audio://`) counts its written denominator over just those tracks, so the
    /// drop count it gates on must cover the SAME set: a video track dropped at a
    /// clip join during an `audio://` export is not evidence the audio files came
    /// up short. Zero without a seam plan.
    pub(crate) fn dropped_for(&self, tracks: &[usize]) -> u64 {
        self.seams.as_ref().map_or(0, |p| {
            tracks
                .iter()
                .fold(0u64, |a, &t| a.saturating_add(p.dropped_for(t)))
        })
    }

    // Map a raw PES PTS onto the output timeline, or `None` to drop the frame (only ever
    // happens under a SeamPlan). Sinks call `map_picture`.
    #[cfg(test)]
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
        self.map_picture(raw_pts_ns, drives_epoch, track, has_reorder, src_byte, None)
    }

    // `map`, and for a video picture (`pic`) a seam plan also drops it when it predicts
    // from a picture the marks dropped.
    pub(crate) fn map_picture(
        &mut self,
        raw_pts_ns: i64,
        drives_epoch: bool,
        track: usize,
        has_reorder: bool,
        src_byte: Option<u64>,
        pic: Option<SeamPic>,
    ) -> Option<i64> {
        if self.seams.is_some() {
            // Take the plan out for the call so `place` can borrow `self`
            // mutably without fighting the borrow checker over the whole struct.
            let mut plan = self.seams.take().expect("checked is_some");
            let placed = match pic {
                Some(pic) => plan.place_picture(raw_pts_ns, track, has_reorder, src_byte, pic),
                None => plan.place(raw_pts_ns, track, has_reorder, src_byte),
            };
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

    // The offset a passive frame should ride: normally the current epoch's, unless the frame
    // arrived ahead of the video that opens its epoch.
    fn passive_offset(&mut self, track: usize, raw_pts_ns: i64) -> i64 {
        if self.last_raw_ns.len() <= track {
            self.last_raw_ns.resize(track + 1, None);
            self.provisional.resize(track + 1, None);
        }
        let prev_raw = self.last_raw_ns[track].replace(raw_pts_ns);
        // Monotonic epoch identity, NOT epoch_offsets.len() (which is capped at
        // MAX_EPOCHS and would stop distinguishing epochs past the cap).
        let current_seq = self.epoch_seq;

        // A provisional only survives until the video opens the epoch for real.
        if let Some((taken_at, _)) = self.provisional[track]
            && taken_at != current_seq
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
                self.provisional[track] = Some((current_seq, off));
                return off;
            }
        }
        effective
    }

    // Retire the current epoch and open a new one continuing just after the frontier, recording
    // where this epoch closed for straggler lookups.
    fn open_epoch(&mut self, high: i64, mapped_now: i64) {
        self.prev_offset_ns = self.offset_ns;
        if self.epoch_offsets.len() == MAX_EPOCHS {
            self.epoch_offsets.remove(0);
        }
        self.epoch_offsets.push((self.offset_ns, high));
        // Advance the monotonic epoch identity so any provisional captured in the
        // just-closed epoch retires, regardless of the ring buffer's length cap.
        self.epoch_seq = self.epoch_seq.saturating_add(1);
        let bump = high
            .saturating_sub(mapped_now)
            .saturating_add(DISCONTINUITY_GAP_NS);
        self.offset_ns = self.offset_ns.saturating_add(bump);
    }

    // The offset of the epoch a straggler actually belongs to: the retained epoch that lands it
    // CLOSEST BELOW its own frontier.
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

    // Map a raw PES PTS (ns) onto the continuous timeline. `drives_epoch` is true only for the
    // primary video track.
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
#[path = "timeline_tests.rs"]
mod tests;
