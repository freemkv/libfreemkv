//! Shared clip-boundary timeline corrector.
//!
//! A BD/UHD title's clips are read as one concatenated sector stream, so the
//! source PES PTS does not run continuously across a clip join. There are two
//! ways to place them, and this module holds both:
//!
//! - **From the playlist's marks** ([`SeamPlan`]) — when the title carries
//!   PlayItem IN/OUT times, each clip contributes exactly `out - in` and the
//!   clips are laid end to end. This is exact: it closes forward skips, joins
//!   overlaps without rewinding, and drops material the playlist excludes.
//! - **By inference** ([`TimelineContinuity::adjust`]) — when there are no
//!   usable marks (DVD, HD-DVD, `mkv://` / `m2ts://` sources), a backward PTS
//!   jump larger than [`DISCONTINUITY_BACKSTEP_NS`] is read as a join and
//!   rebased. Inference cannot see a forward skip, because a forward gap is
//!   indistinguishable from frames lost to damaged media, and cannot see an
//!   overlap smaller than the reorder threshold.
//!
//! [`TimelineContinuity::map`] picks between them: marks when present,
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
}

impl SeamPlan {
    /// Build a plan from a title's clips, or `None` when there is nothing to
    /// place: no clips (DVD, HD-DVD, `mkv://`/`m2ts://` sources — none of which
    /// carry PlayItem marks), a single clip (nothing to join), or marks that are
    /// not usable (a zero/inverted span means the playlist is not telling us
    /// anything we can act on, and guessing is what this type exists to avoid).
    ///
    /// Returning `None` leaves [`TimelineContinuity`] on its PTS-jump inference,
    /// which is what every non-BD path has always used.
    pub(crate) fn from_clips(clips: &[crate::disc::Clip]) -> Option<Self> {
        if clips.len() < 2 {
            return None;
        }
        let mut out: Vec<SeamClip> = Vec::with_capacity(clips.len());
        let mut cum: i64 = 0;
        for c in clips {
            let in_ns = mpls_ticks_to_ns(c.in_time);
            let out_ns = mpls_ticks_to_ns(c.out_time);
            if out_ns <= in_ns {
                return None;
            }
            // Every placement rule below assumes the marks are points on ONE
            // clock that advances across the whole title: a join is recognised
            // by a frame landing at the next clip's IN, and a frame is assigned
            // to a clip by falling inside its [in, out].
            //
            // Not every title is like that — a playlist whose clips each restart
            // their own STC has marks that all cover the same low values. Under
            // such a table a crossing can be missed, and a missed crossing
            // STRANDS the track on the clip it was on: every later frame then
            // falls outside that clip's marks and is dropped, silently, for the
            // rest of the title. That is precisely the truncation this type was
            // written to fix, so a table we cannot place must not be placed at
            // all — it falls back to inference, which is the documented safe
            // path for exactly these titles.
            if let Some(prev) = out.last()
                && in_ns <= prev.in_ns
            {
                return None;
            }
            out.push(SeamClip {
                in_ns,
                out_ns,
                offset_ns: cum.saturating_sub(in_ns),
            });
            cum = cum.saturating_add(out_ns - in_ns);
        }
        Some(Self {
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

    /// Place a raw PTS for `track`, advancing that track's own clip cursor.
    ///
    /// `None` means DROP: the frame lies outside every clip's marks, so the
    /// playlist does not include it.
    fn place(&mut self, raw_ns: i64, track: usize, has_reorder: bool) -> Option<i64> {
        if self.cursors.len() <= track {
            self.cursors.resize(
                track + 1,
                TrackPos {
                    clip: 0,
                    last_raw_ns: None,
                },
            );
            self.dropped.resize(track + 1, 0);
        }
        let pos = self.cursors[track];
        let mut clip = pos.clip;

        // Advance this track to the next clip when THIS track's frames say it
        // has crossed. Two signatures, because a join is either a skip or an
        // overlap:
        //
        // - SKIP (next IN after this OUT): the frame is simply past this clip's
        //   OUT mark.
        // - OVERLAP (next IN before this OUT, the disc storing the join twice):
        //   the frame is still inside this clip's range, so "past OUT" never
        //   fires. But a track's own PTS only ever runs forward inside a clip,
        //   so the backward step to the next clip's IN is the crossing — and it
        //   is per track, which is why the cursor has to be per track too.
        //
        // A backward step too large to be B-frame reorder is a NEW CLIP'S FILE
        // starting, and it must be handled before the per-track rules below.
        //
        // Those rules require the frame to land at (or just below) the next
        // clip's IN mark, which is right for the dense case and wrong here:
        // nothing upstream trims a clip's stream to its marks, so a clip's file
        // can open with material from BEFORE its IN. Measured on the real
        // 00801.mpls table in this file: with the cursor on clip 5
        // (7708.99..7910.79) a frame at 7845.00 — clip 6's pre-mark lead-in,
        // 8s below clip 6's IN of 7853.00 — is more than the 250ms tolerance
        // from that mark, so neither rule fires, the cursor stays on clip 5,
        // and 7845.00 is INSIDE clip 5's range so the frame is placed with clip
        // 5's offset. Output went BACKWARDS 65s and `dropped` stayed 0: no
        // counter, no gate, nothing noticed, and the whole overlap band was
        // emitted a second time on top of clip 5's already-written tail.
        //
        // A jump this large cannot be reorder, so advance to the first clip
        // that could still contain the frame. That terminates on its own — it
        // stops at containment rather than running to the end of the list,
        // which is what the `next_in` bound below exists to prevent — and any
        // frame left before its clip's IN is material the playlist EXCLUDES, so
        // the containment check at the end drops and counts it.
        let big_backstep = pos
            .last_raw_ns
            .is_some_and(|last| last.saturating_sub(raw_ns) > DISCONTINUITY_BACKSTEP_NS);
        // A track's own PTS only ever runs forward INSIDE a clip, so a backward
        // step this large is always a clip change: advance at least one, then
        // skip any clip the frame is already past. Terminates at the first clip
        // that could contain it; a frame still below that clip's IN is
        // pre-mark material and the containment check drops and counts it.
        if big_backstep && clip + 1 < self.clips.len() {
            clip += 1;
            while clip + 1 < self.clips.len() && raw_ns > self.clips[clip].out_ns {
                clip += 1;
            }
        }
        // Bounded by the clip count, so a wild PTS cannot spin here.
        while clip + 1 < self.clips.len() {
            let cur = self.clips[clip];
            let next_in = self.clips[clip + 1].in_ns;
            let past_out = raw_ns > cur.out_ns;
            // The step must land ON the next clip's IN mark, not merely below
            // the current position: a bound only from above is satisfied by
            // every later clip's IN too, and the cursor would run to the end of
            // the list on a single backward step.
            // A backward step means this track has restarted at the next
            // clip's IN. What counts as "at" differs by track, and getting it
            // wrong strands a track on the previous clip's offset:
            //
            // - A track WITH B-frame reorder (any video track) is dense — a
            //   frame every ~42ms at 24fps — so its first frame of the new clip
            //   lands ON the mark. Requiring that is what keeps a reorder dip
            //   near the end of a clip, which is also a backward step and lies
            //   inside the next clip's range during an overlap, from being read
            //   as a crossing. This is keyed on REORDER, not on driving epochs:
            //   a Dolby Vision enhancement layer is a second video track that
            //   does not drive epochs but does reorder, and giving it the
            //   permissive rule made its reorder dip look like a join and threw
            //   it out of step with the base layer it must be co-timed with.
            // - Tracks WITHOUT reorder (audio, subtitles) can take any backward
            //   step as a crossing. They are also sparse: a subtitle may have
            //   no event near the mark, so its first frame after the join can
            //   land well past it. Holding those to the video window left them
            //   on the old clip until their PTS passed its OUT, mistiming them
            //   by the overlap in between.
            //
            // Both forms still require the frame to land at or after the next
            // IN, which is what stops one backward step walking the cursor to
            // the end of the list.
            let stepped_back = pos.last_raw_ns.is_some_and(|last| raw_ns < last)
                && if has_reorder {
                    // saturating_abs, not abs: every other comparison in this
                    // module is saturating because these timestamps come off a
                    // disc and are not trusted. `abs()` is the one exception
                    // and it panics on i64::MIN — which saturating_sub can
                    // produce exactly — taking down the mux thread on one bad
                    // frame instead of comparing false like its neighbours.
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

        let c = self.clips[clip];
        self.cursors[track] = TrackPos {
            clip,
            last_raw_ns: Some(raw_ns),
        };
        if raw_ns < c.in_ns || raw_ns > c.out_ns {
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
}

impl TimelineContinuity {
    pub(crate) fn new() -> Self {
        Self {
            offset_ns: 0,
            prev_offset_ns: 0,
            high_ns: None,
            seams: None,
        }
    }

    /// Corrector driven by a title's PlayItem marks where they exist.
    ///
    /// Falls back to [`Self::new`]'s inference when the title has fewer than two
    /// clips or its marks are unusable — so DVD, HD-DVD, `mkv://` and `m2ts://`
    /// sources behave exactly as before.
    pub(crate) fn with_clips(
        clips: &[crate::disc::Clip],
        content_format: crate::disc::ContentFormat,
    ) -> Self {
        // ONLY Blu-ray. A PlayItem's IN/OUT are positions in the same 45 kHz
        // clock the PES PTS runs on, which is what makes placing frames by them
        // meaningful.
        //
        // The other formats' clips carry marks from a different clock entirely:
        // HD-DVD fills them from the XPL's title-relative `titleTimeBegin` /
        // `titleTimeEnd`, and a DVD's come from cell tables. Those are elapsed
        // times within a title, not PES positions — so a plan built from them
        // is an identity map with a drop filter, it stops the layer-break
        // rebase `adjust` exists to perform, and it drops whatever falls
        // outside marks the PTS was never measured against. Those formats keep
        // the inference path, which is what they have always used.
        let seams = match content_format {
            crate::disc::ContentFormat::BdTs => SeamPlan::from_clips(clips),
            crate::disc::ContentFormat::MpegPs => None,
        };
        Self {
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
    ) -> Option<i64> {
        if self.seams.is_some() {
            // Take the plan out for the call so `place` can borrow `self`
            // mutably without fighting the borrow checker over the whole struct.
            let mut plan = self.seams.take().expect("checked is_some");
            let placed = plan.place(raw_pts_ns, track, has_reorder);
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
        Some(self.adjust(raw_pts_ns, drives_epoch))
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
            // AND its PREVIOUS-offset mapping lands in the seam TAIL — at/below the
            // frontier but no more than one backstep below it (i.e. it ended just
            // before the seam, in the prior epoch). The lower bound is essential:
            // a NORMAL new-epoch frame that merely leads the sparse (video-only)
            // frontier by >3s ALSO has `prev_mapped <= high` (its prev-offset
            // mapping lands ~a whole clip below the frontier), and clamping it
            // would demote it into the just-ended clip's epoch, mis-timing that
            // audio/subtitle by a whole clip. Requiring `prev_mapped` to sit
            // within a backstep below the frontier keeps the remap to genuine
            // tail stragglers; a long audio-only tail, a sparse subtitle, or an
            // EL frame that simply runs ahead is left on the current offset.
            //
            // Every comparison below saturates. `high` is derived from an
            // untrusted container timestamp (an `mkv://` source's
            // CLUSTER_TIMESTAMP × TimestampScale is clamped only against
            // `i64::MAX`, so a hostile file can put the frontier AT `i64::MAX`),
            // and `raw_pts_ns` can be negative (a SimpleBlock's signed relative
            // timestamp). Plain `high + BACKSTEP` / `high - BACKSTEP` would then
            // overflow: a panic out of the public `Stream::write` path in an
            // overflow-checked build, and in release a wrap to the opposite sign
            // that fires the straggler clamp on essentially every passive frame.
            if let Some(high) = self.high_ns
                && mapped > high.saturating_add(DISCONTINUITY_BACKSTEP_NS)
            {
                let prev_mapped = raw_pts_ns.saturating_add(self.prev_offset_ns);
                if prev_mapped <= high
                    && prev_mapped >= high.saturating_sub(DISCONTINUITY_BACKSTEP_NS)
                {
                    return prev_mapped;
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
        if adj < high.saturating_sub(DISCONTINUITY_BACKSTEP_NS) {
            // Clip-boundary reset (real multi-clip seam): continue just after the
            // frontier. Save the previous offset so a lagging non-video tail
            // frame can be recognised and remapped to the seam (see above).
            self.prev_offset_ns = self.offset_ns;
            // `high - adj` is a backward step, so positive — but both ends are
            // untrusted (`high` up to i64::MAX, `adj` down to i64::MIN), so
            // saturate rather than panic in a checked build.
            let bump = high
                .saturating_sub(adj)
                .saturating_add(DISCONTINUITY_GAP_NS);
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
            .place(7_910_000_000_000, 0, true)
            .expect("a frame inside clip 5 is placed");
        // Clip 6's file opens 8s below its own IN mark.
        let b = plan.place(7_845_000_000_000, 0, true);
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
                .place(in_ns, 0, true)
                .expect("clip start is inside its marks");
            assert_eq!(got, expected_start, "clip {i} start misplaced");
            // Last frame lands at the running total plus the clip's length.
            let end = plan
                .place(out_ns, 0, true)
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
        let end_of_2 = plan.place(c2_out, 0, true).expect("in clip 2");
        let start_of_3 = plan.place(c3_in, 0, true).expect("in clip 3");
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
            .place(c0_out, 0, true)
            .expect("clip 0 OUT is inside clip 0");
        // The next clip opens ON its IN mark. Under the old inference this was a
        // 1.79s backward step, below the reorder threshold, so no seam was
        // recognised and the join was emitted as duplicate content whose
        // timestamps then collided. With the marks known, clip 1 is placed to
        // continue exactly where clip 0 ended: one monotonic timeline, no
        // rewind, and no collision for the muxer to flatten.
        let first_of_1 = plan.place(c1_in, 0, true).expect("clip 1 IN");
        assert_eq!(
            first_of_1, last_of_0,
            "clip 1 must continue from clip 0's end, not rewind by the overlap"
        );
        // And the timeline keeps moving forward from there.
        let into_1 = plan
            .place(c1_in + 1_000_000_000, 0, true)
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
        plan.place(c1_in + 500_000_000, 0, true).expect("in clip 1");
        // A straggler from clip 0's tail arrives afterwards.
        let tail = c0_out - 50_000_000; // 50ms before clip 0's OUT
        let placed = plan
            .place(tail, 1, false)
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

        let first = tc.map(c0_in, true, 0, true).expect("first frame");
        assert_eq!(first, 0, "clip 0 starts the output timeline at zero");
        assert_eq!(
            tc.offset_ns, -c0_in,
            "offset is the correction actually applied"
        );
        assert_eq!(tc.high_ns, Some(0), "video advances the frontier");

        let later = tc
            .map(c0_in + 5_000_000_000, true, 0, true)
            .expect("later frame");
        assert_eq!(later, 5_000_000_000);
        assert_eq!(tc.high_ns, Some(5_000_000_000), "frontier follows video");

        // A passive track must NOT advance the frontier.
        let before = tc.high_ns;
        tc.map(c0_in + 1_000_000_000, false, 1, false)
            .expect("audio");
        assert_eq!(tc.high_ns, before, "passive tracks never move the frontier");

        // Across the join the frontier keeps rising, never rewinds.
        let across = tc.map(c0_out, true, 0, true).expect("clip 0 OUT");
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
        assert_eq!(plan.place(c0_in - 5_000_000_000, 0, true), None, "dropped");
        assert_eq!(plan.dropped_for(0), 1, "and counted");
        assert_eq!(plan.dropped_for(1), 0, "counted per track, not globally");
        plan.place(c0_in, 0, true).expect("inside");
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
        let placed_tail = plan.place(tail, 1, false).expect("clip 0 tail");
        assert_eq!(placed_tail, tail - c0_in, "tail rides clip 0's offset");

        // Its first event in clip 1 steps BACK (the clips overlap) but lands
        // 400ms past the IN mark, because the track is sparse and had no event
        // sitting on the mark. The old window was +/-250ms, so this was missed
        // and the event stayed on clip 0 — mistimed by the overlap.
        let late = c1_in + 400_000_000;
        assert!(late < tail, "fixture: this is a backward step");
        assert!(
            (late - c1_in) > CLIP_START_TOLERANCE_NS,
            "fixture: further past the mark than the video window allows"
        );
        let got = plan
            .place(late, 1, false)
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
        let base = plan.place(a, 0, true).expect("in clip 0");
        // A reorder dip of ~42ms: backward, and >= clip 1's IN.
        let dip = a - 42_000_000;
        assert!(
            dip >= c1_in,
            "fixture: the dip is inside clip 1's range too"
        );
        let got = plan.place(dip, 0, true).expect("dip placed");
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
        plan.place(c0_in, 1, false).expect("audio start");
        let a_tail = plan.place(tail, 1, false).expect("audio tail");

        // Video (track 0) crosses into clip 1 first.
        plan.place(c0_out, 0, true).expect("video at clip 0 OUT");
        plan.place(c1_in, 0, true).expect("video at clip 1 IN");

        // Audio's NEXT tail frame still belongs to clip 0 and must stay there —
        // contiguous with the previous one, not thrown forward by the overlap.
        let a_tail2 = plan
            .place(tail + 10_000_000, 1, false)
            .expect("audio tail continues");
        assert_eq!(
            a_tail2 - a_tail,
            10_000_000,
            "audio tail must stay on clip 0's offset while video is already in clip 1"
        );

        // When audio itself steps back to clip 1's IN, it crosses — and lands
        // after its own tail, with no rewind and no collision.
        let a_new = plan.place(c1_in, 1, false).expect("audio crosses");
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
        let base = plan.place(a, 2, true).expect("EL in clip 0");
        let dip = a - 42_000_000;
        assert!(
            dip >= c1_in,
            "fixture: the dip lies inside clip 1's range too"
        );
        let got = plan.place(dip, 2, true).expect("dip placed");
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
                plan.place(t, 0, true),
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
        assert!(
            SeamPlan::from_clips(&seamless_branching_clips()[..1]).is_none(),
            "a single clip has nothing to join"
        );
        let mut bad = seamless_branching_clips();
        bad[3].out_time = bad[3].in_time; // zero-length span
        assert!(
            SeamPlan::from_clips(&bad).is_none(),
            "unusable marks must fall back to inference, not guess"
        );
    }

    /// Without a plan, `map` is exactly the old `adjust` and never drops.
    #[test]
    fn map_without_a_plan_is_the_old_behaviour() {
        let mut tc = TimelineContinuity::new();
        assert_eq!(tc.map(0, true, 0, true), Some(0));
        assert_eq!(tc.map(5 * S, true, 0, true), Some(5 * S));
        assert_eq!(tc.map(25 * S, false, 1, false), Some(25 * S));
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
        // A NORMAL clip2 audio frame (raw ~1s, current epoch) is NOT remapped —
        // it rides the new offset to ~601s, just past the frontier but within a
        // backstep (its previous-offset mapping ~1s is below the frontier but the
        // current-offset mapping is not a backstep past it, so it is not treated
        // as a straggler).
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

        // A NORMAL clip-2 audio frame at raw ~5s. Current-offset mapping is
        // ~605s, which IS more than a backstep (3s) past the 600s frontier — but
        // its previous-offset mapping (~5s) lands ~595s BELOW the frontier, far
        // outside the seam tail. It is a legitimate new-epoch frame, NOT a tail
        // straggler, and must ride the current offset.
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
