# `src/mux/timeline.rs` — design notes and audit history

Long-form rationale relocated here by the comment guard. Each section is
pointed to from a short `//`/`///` comment at the matching site in
`timeline.rs`.

## Module overview

There are two ways to place a clip's frames on the output timeline:

- **From the playlist's marks** (`SeamPlan`) — when the title carries
  PlayItem IN/OUT times, each clip contributes exactly `out - in` and the
  clips are laid end to end. This is exact: it closes forward skips, joins
  overlaps without rewinding, and drops material the playlist excludes.
- **By inference** (`TimelineContinuity::adjust`) — when there are no usable
  marks (DVD, HD-DVD, `mkv://` / `m2ts://` sources), a backward PTS jump
  larger than `DISCONTINUITY_BACKSTEP_NS` is read as a join and rebased.
  Inference cannot see a forward skip, because a forward gap is
  indistinguishable from frames lost to damaged media, and cannot see an
  overlap smaller than the reorder threshold.

`TimelineContinuity::map` picks between them: marks when present, inference
otherwise. Every muxer/sink that consumes the interleaved per-track PES
stream and emits a monotonic timeline (the MKV muxer, the `demux://`
elementary-stream sink) goes through it, so the correction lives in exactly
one place.

## `DISCONTINUITY_BACKSTEP_NS`

A backward PTS step larger than this is treated as a clip-boundary
discontinuity (a non-seamless BD clip / dual-layer-break where the source
PES PTS resets), NOT as B-frame reorder. HEVC/H.264 reorder depth tops out
around 16 frames (<1s at 24 fps); 3s sits comfortably above any legitimate
reorder window and far below any real clip's duration, so it never
false-triggers within a clip.

## `CLIP_START_TOLERANCE_NS`

How close a frame's PTS must be to a clip's IN mark to be recognised as that
clip's opening frame.

At an OVERLAP join the next clip's IN sits inside the current clip's range,
so "past the current OUT" never fires and the two clips share a PTS band.
The clips are concatenated in file order, though, so the new clip opens ON
its IN mark — this window is what tells that opening frame apart from the
old clip's tail. One video frame is ~42 ms at 24 fps; 250 ms allows for a
clip whose first frame sits a few frames past its mark without ever reaching
the next join.

## `SeamClip::feed_span`

Byte range this clip occupies in the title's feed, when known.

When a frame carries its source offset this makes clip assignment a LOOKUP.
Inside an overlap two clips' mark ranges both contain the same timestamp, so
no rule over timestamps alone can say which clip a frame came from — four
audit rounds each fixed one such rule and broke another. The byte offset
falls in exactly one span.

## `SeamPlan` (struct doc)

The playlist's own answer to "where does each clip belong on the timeline".

A seamless-branching title's PlayItems do NOT chain contiguously in the
shared clock: one clip's OUT may sit *after* the next clip's IN (overlap —
the disc stores the join twice so a player can switch without a gap), or
*before* it (skip — the playlist jumps over material). Measured on one real
UHD title, `00801.mpls`, 11 PlayItems:

```text
clip 0  in 4199.0000  out 6033.0405   cum_start 0.0000
clip 1  in 6031.2500  out 6308.1933   cum_start 1834.0405   <- 1.79s OVERLAP
clip 2  in 6298.1667  out 6875.0763   cum_start 2110.9839
clip 3  in 6884.2500  out 6948.0220   cum_start 2687.8935   <- 9.17s SKIP
```

Inferring seams from PTS jumps cannot recover this. A forward jump is
ambiguous — it means "the playlist skipped" OR "we lost frames to damaged
media", and compressing the latter would silently falsify timing on exactly
the rips that most need it faithful. An overlap smaller than the B-frame
reorder threshold is invisible to inference entirely, and its duplicated
content then collides in the muxer.

So the marks are read rather than guessed. Each clip contributes exactly
`out − in` to the output, laid end to end: gaps never become dead timeline,
and material outside a clip's marks is dropped rather than emitted twice.

## `SeamPlan::spans_trusted`

Whether the per-clip feed spans can be trusted to identify a clip from a
frame's byte offset.

True only when the spans tile the feed contiguously from 0 with no gap or
overlap. Anything else means the scan's view of the extents and the mux's
differ, and a byte offset would then select a confidently WRONG clip for
every frame — a worse failure than the mark heuristics, which are at least
approximately right. In that case provenance is disabled and the heuristics
are used, which is the 1.6.0 behaviour.

## `SeamPlan::dropped`

Frames dropped because they fell outside every clip's marks, per track.

A drop is correct — the playlist does not include that material — but a
SILENT drop is how this codebase has produced complete-looking, wrong output
before. Counting them means an unexpected volume shows up in the log instead
of in someone's file, and gives a caller something to assert on. Indexed by
track alongside `cursors`.

## `SeamPlan::cursors`

Per-track position: (clip index, last raw PTS seen).

Each track crosses a join on ITS OWN frame, not on video's. The demuxer
interleaves the tracks, so when video enters the next clip the previous
clip's audio and subtitle tails are still arriving — and at an OVERLAP join
those tail frames fall inside BOTH clips' mark ranges, so there is no way to
place them from the PTS alone. Sharing one cursor gave the tail the new
clip's offset, which threw it forward by the overlap and made it collide
with the new clip's own frames; the muxer's monotonic nudge then flattened
the collision onto the tick floor, which is exactly the audio-ahead-of-picture
symptom this type exists to remove.

## `SeamPlan::from_clips`

Build a plan from a title's clips, or `None` when there is nothing to place:
no clips (DVD, HD-DVD, `mkv://`/`m2ts://` sources — none of which carry
PlayItem marks), or marks that are not usable (a zero/inverted span means the
playlist is not telling us anything we can act on, and guessing is what this
type exists to avoid).

A **single** clip still gets a plan. Joining is not the marks' only job —
trimming to `[in, out]` is — and it matters whenever a clip's physical extent
runs past its OUT mark. Real discs author trailing audio (a fade after the
last video frame) beyond OUT in the m2ts; without a plan the inference path
keeps it, leaving audio seconds past the declared duration. One clip needs no
cross-clip placement, so the loop below reduces to the `[in, out]` drop
filter plus the standard `offset = −in_ns` rebase — the same one clip 0 of a
multi-clip title already gets, which the MKV muxer then re-anchors, so every
KEPT frame is byte-identical to the no-plan path. The only change is that
out-of-mark frames are now dropped.

Returning `None` leaves `TimelineContinuity` on its PTS-jump inference, which
is what every non-BD path has always used.

## `SeamPlan::clip_at_byte`

Which clip owns feed byte `b`, by BINARY SEARCH.

`spans_trusted` guarantees the spans tile the feed contiguously in order, so
this is a partition point rather than a scan. That matters: discs in the test
hoard reach 900 clips, and this runs once per frame per track — a linear scan
would be ~900 comparisons on every one of millions of frames, which is real
time spent for no reason.

A repeated clip reuses its first reference's span, so the search lands on the
FIRST entry with that span. The bytes are only read once, so the material is
emitted once, at that entry's offset.

## `SeamPlan::clip_in_run_for`

Pick the member of a shared-span run whose marks contain `raw_ns`.

`clip_at_byte` deliberately answers with the FIRST entry of a run so the
lookup is stable. That is the right answer when the run is one clip; when
several PlayItems reference one file it is only the right STARTING point,
because they differ solely in their marks. Falls back to the first when none
contains the timestamp, which keeps the existing drop behaviour for material
genuinely outside every reference.

## `TimelineContinuity` (struct doc)

Global timeline corrector.

Holds a `SeamPlan` when the title's PlayItem marks are usable, and falls back
to the PTS-jump inference described below when they are not. The inference
documentation that follows applies to the FALLBACK path only — under a plan,
placement comes from the marks and none of the epoch/frontier reasoning below
decides anything.

freemkv reads a BD title's clips as one concatenated sector stream, so at a
non-seamless boundary the source PES PTS jumps backward. Left uncorrected,
that produces a sustained band of non-monotonic block timestamps (a
downstream muxer then derives non-monotonic DTS from them).

A single running `offset_ns` is applied to EVERY track, so the concatenated
clips form one monotonic timeline AND A/V sync is preserved (all tracks at a
boundary shift by the same amount). It is global, not per-track: a clip
boundary resets every stream together by the same delta.

**Only the VIDEO track drives epoch decisions.** A title carries one video
track plus many interleaved audio + subtitle tracks (one UHD title: 2 video,
11 audio, 32 PGS). Those non-video tracks are sparse and lag the video by
seconds, so their raw PTS swing well over the 3 s discontinuity threshold
against a shared frontier even within a SINGLE clip — a late subtitle PTS
would ratchet `high_ns` up, then the next normal video frame would sit >3 s
below it and be misread as a clip boundary, permanently bumping `offset_ns`.
That false-positive ratchet (firing thousands of times on a one-clip title)
inflated that title's cluster/Cue timestamps into the billions of ms and
destroyed its seek index. The clip-boundary INFERENCE is therefore keyed on
video PTS alone: video establishes and advances the frontier and is the only
track that can open a new epoch. Non-video frames are remapped under the
CURRENT offset and never touch the frontier or the offset — they ride the
timeline the video defines, preserving A/V sync (all tracks at a boundary
shift by the same delta) without ever triggering a rebase themselves.

The demuxer interleaves the tracks, so at a real (multi-clip) boundary the
streams do NOT all reset on the same frame — a lagging audio/PGS frame from
the just-ended clip's tail can arrive AFTER the next clip's video has already
reset the epoch. Such a "straggler" carries an old-epoch raw PTS; adding the
new (clip-sized) offset to it would fling it far past the frontier and force
a forward-dated split cluster. A non-video frame whose mapped position lands
more than a backstep past the frontier is therefore clamped to the frontier
(the seam) — it never perturbs the offset or the frontier and never
forward-dates a cluster. Genuine multi-clip seamless rebasing (the design
that is correct for real HEVC/H.264 multi-clip titles) is preserved: it is
the video back-jump that opens a new epoch, exactly as before.

## `TimelineContinuity::prev_offset_ns`

Offset (ns) of the immediately previous epoch — used to recognise and remap a
non-video tail straggler at a boundary (an old-epoch frame whose
current-offset mapping flies forward but whose previous-offset mapping lands
at the seam). Equals `offset_ns` until the first boundary.

## `TimelineContinuity::epoch_offsets`

Every epoch already left behind, oldest first, as
`(offset, frontier when it closed)`. A straggler carries a raw PTS from one
of THESE, and a single `prev_offset_ns` cannot name the right one once a
title has more than two epochs. The closing frontier is what makes the test
meaningful: a straggler sits at the TAIL of its own epoch, which is only
recognisable against that epoch's end — not against the current frontier,
which by then may be a whole title away. Bounded: a source that rebases
forever must not grow this.

## `TimelineContinuity::last_raw_ns`

Last raw PTS seen per track, for spotting a track's OWN discontinuity. Within
an epoch a passive track's PTS only advances (audio and subtitles do not
reorder, and a passive video track's B-frame dip is far under the backstep),
so a large BACKWARD step is unambiguous. This is a different signal from the
shared frontier, which is what the old false-positive ratchet keyed on.

## `TimelineContinuity::provisional`

Per-track provisional offset for frames that arrive BEFORE the video frame
opening their epoch, as `(epochs retired when it was taken, offset)`.

It is deliberately private to one track and never written to `offset_ns`,
never advances `high_ns`, and never retires an epoch — so it cannot move the
video timeline. Letting a passive track open a real epoch was tried and
inflated a 476.776 s title to 656.216 s, because every track observes a
boundary at its own pace and the video path rebased again on top of whatever
they had done.

## `TimelineContinuity::with_clips`

Falls back to `Self::new`'s inference when the title has no clips or its
marks are unusable — so DVD, HD-DVD, `mkv://` and `m2ts://` sources behave
exactly as before. A single BD clip DOES get a plan: its marks still trim
trailing/pre-roll material outside `[in, out]`.

## `TimelineContinuity::dropped_total`

Zero for a title without a seam plan. A muxer reports this when it finishes
so a drop is never invisible: dropping is correct at a join, but an
unexpected VOLUME of drops is how output ends up quietly short.

## `TimelineContinuity::map`

Dropping only ever happens under a `SeamPlan`: it is material outside the
playlist's marks, which the title does not include.

## `TimelineContinuity::passive_offset`

The offset a passive frame should ride, and the bookkeeping around it.

Returns the effective offset for THIS frame. Normally that is the current
epoch's. It differs only for a frame that arrived ahead of the video that
opens its epoch: such a frame's own raw PTS has just jumped backwards AND its
current-epoch mapping lands a whole epoch below the frontier, which no
in-epoch frame ever does.

A provisional is dropped the moment the video actually retires an epoch, so
the run rejoins the real offset with no seam — the two agree because both are
`frontier - mapping + gap`.

## `TimelineContinuity::open_epoch`

Retiring records the offset AND the frontier the epoch closed at. The closing
frontier is what later makes a straggler recognisable: it says where that
epoch's tail was, which the current frontier cannot, since by then it may be
a whole title further on.

## `TimelineContinuity::straggler_offset`

The offset of the epoch a straggler actually belongs to.

A frame whose current-epoch mapping flies past the frontier carries a raw PTS
from an epoch already left behind. Pick the retained epoch that lands it
CLOSEST BELOW the frontier: that is where the just-ended epoch's tail was,
and it can never forward-date a cluster. `None` when no retained epoch places
it sanely, in which case the caller keeps the current mapping — which is
exactly the pre-existing behaviour, so a title that never rebased is
unaffected.

## `TimelineContinuity::adjust`

`drives_epoch` gates EVERY epoch decision. It is `true` for the PRIMARY video
track (base layer, track 0) ONLY. Every other track — audio, PGS subtitle,
and a second video track such as a Dolby Vision enhancement layer — passes
`false` and is a passive rider. (The DV EL is video but runs its own PTS
timeline interleaved with the base layer's; letting it drive epochs would
false-trigger a reset on every GOP.)

**Passive tracks** (`drives_epoch == false`). Always remapped under the
CURRENT offset. They never advance `high_ns`, never trigger a clip-boundary
reset, and never bump `offset_ns`. This is what kills the single-clip
ratchet: a sparse/lagging subtitle/audio PTS, or an interleaved EL frame, can
no longer push the frontier up and make the next base-video frame look like a
boundary. A/V sync is preserved because the offset they ride is the same one
the base video established for the epoch.

**Primary video** (`drives_epoch == true`):
- **Backward jump > `DISCONTINUITY_BACKSTEP_NS`** vs the frontier = clip-
  boundary reset: open a new epoch (bump the offset so this frame continues
  just after the frontier). This is the genuine multi-clip seamless
  rebasing, now driven only by real base-video back-jumps.
- **Everything else** (normal progression + sub-threshold B-frame reorder
  dips) passes through with the current offset and advances the frontier,
  preserving PTS.

## Test module — audit findings and measured regressions

The `#[cfg(test)]` module below documents, per test, an audit finding or a
measured real-disc regression that motivated it. Each test's full rationale:

### `continuity_rebases_clip_boundary_reset`
Characterization of the BUG: a BD title's two clips concatenated with a PTS
reset at the boundary. WITHOUT correction the raw VIDEO timeline goes hard
backward at clip 2 (what produced the non-monotonic-DTS band on multi-clip
UHD titles). WITH `TimelineContinuity` the output is monotonic and continuous
across the boundary. The boundary is driven by VIDEO.

### `a_clip_join_never_rewinds_the_output_timeline`
Audit finding, measured against the real 00801.mpls marks: with the cursor on
clip 5 (7708.99..7910.79) a frame at 7845.00 — clip 6's pre-mark lead-in, 8s
below clip 6's IN of 7853.00 — was more than the 250ms tolerance from that
mark, so no crossing rule fired; the cursor stayed on clip 5, and 7845.00 IS
inside clip 5's range, so the frame was PLACED with clip 5's offset. Output
went backwards 65s and dropped stayed 0 — no counter, no gate, nothing
noticed — while the entire overlap band was emitted a second time over clip
5's written tail.

A backward step larger than `DISCONTINUITY_BACKSTEP_NS` cannot be B-frame
reorder, so it is a new clip's file starting. Such a frame is either placed
on the clip that contains it, or dropped as pre-mark material the playlist
excludes — never emitted behind the frame before it.

### `a_glitched_pts_does_not_strand_a_track_on_a_later_clip`
Audit finding against the large-backstep branch: advancing on ANY >3s
backward step also fires for a corrupt PTS, or a legitimate STC
discontinuity inside one clip. Nothing moves the cursor back — a forward step
matches neither `past_out` nor `stepped_back` — so every later frame sits
below the new clip's IN and is dropped. On this table that is ~17 minutes of
one track, and the only volume gate compares total drops against ALL tracks'
frames, so it exits 0.

The branch now requires the frame to be INSIDE the current clip's marks,
which is the only case that would otherwise be wrongly placed.

### `output_never_rewinds_and_a_bad_frame_never_strands`
The three placements audit round 7 enumerated, which the previous
mark-heuristic guards all got wrong in different ways.

The property asserted is the one that matters and the one the heuristics
only approximated: a track's output never runs backwards, and a bad frame
never strands the cursor. Numbers are from the real 00801.mpls table.

### `plan_offset_for` (test helper)
The output offset `from_clips` computes for a clip: the sum of every earlier
clip's playable duration, minus its own IN.

### `provenance_picks_the_clip_the_frame_came_from_inside_an_overlap`
The case four rounds of mark heuristics could not get right: inside an
overlap, clip k's OUT is AFTER clip k+1's IN, so one timestamp is valid in
both. The byte offset says which clip the frame actually came from.

### `all_tracks_of_one_clip_agree_under_provenance`
Every track of a clip lives in the SAME stream file, so provenance makes
video, audio and subtitles agree by construction. Divergence between them —
each track guessing separately under its own rule — is how audio and video
ended up on different clips and drifted apart.

### `a_clip_split_across_two_play_items_keeps_both_halves`
A playlist may reference the same clip twice (a looped segment). The bytes
are read once, so both entries share one span — that must not be read as a
broken map.

A clip FILE referenced by two adjacent PlayItems — a seamless split, a
looped segment, multi-angle — is one file with one set of bytes, so it has
ONE feed span. But each PlayItem carries its OWN marks, and the two cover
different halves of it.

Provenance alone cannot tell those halves apart: every frame of the file
resolves to the same span. `clip_at_byte` answers with the FIRST of the run,
so without the timestamp to disambiguate, every frame past the first
PlayItem's OUT is judged against marks it was never inside and dropped — the
second half of that clip silently missing from the rip, with the timeline
still charged for its duration.

### `no_provenance_still_places_by_marks`
A source that stamps no provenance (a mkv:// remux, the deserialize hop)
must still work — it takes the mark heuristics, which is what it has always
used and where it has always been correct, because such sources have no
overlapping clips to be ambiguous about.

### `provenance_agrees_with_marks_wherever_marks_are_unambiguous`
On frames that are NOT ambiguous, provenance and the mark heuristics must
give the SAME answer.

This is the strongest available cross-check. The heuristics are wrong only
inside an overlap, where two clips' mark ranges both contain the timestamp;
everywhere else they are the behaviour that shipped and was verified on real
discs. So for a frame that falls inside exactly one clip, the two methods
disagreeing means the NEW path is wrong.

### `a_full_pass_over_the_real_table_is_monotonic_and_totals_correctly`
Walk a whole title through the plan with provenance and assert the two
properties that decide whether a rip is watchable: output never moves
backwards for a track, and the total span matches the title's declared
duration.

A title can have the right TOTAL duration while being wrong in the middle,
which is why monotonicity is checked per frame rather than only at the ends.

### `one_clip_file_behind_every_play_item_is_not_distinguishable_by_byte`
A title whose PlayItems all reference ONE clip file: every span is
identical, so the tiling check's "equal to previous is allowed" arm matches
every entry and the spans are TRUSTED — while carrying no information at all
about which PlayItem a byte belongs to.

That combination is the dangerous one: provenance looks authoritative and is
actually blind, so every frame resolves to the FIRST PlayItem and everything
past its mark range is dropped.

### `marks_that_do_not_advance_are_placed_by_provenance`
Marks that do not advance across the title are normal — each clip file
carries its own STC — and are now PLACED, because every track carries a
source byte offset and the clip comes from that, not from the marks.

This was refused before, which dropped exactly the branched titles the seam
plan exists for onto the inference path that cannot read them.

### `a_restarting_clock_table_without_spans_is_still_refused`
The refusal must SURVIVE where it is actually load-bearing: a non-monotonic
table whose spans cannot be trusted has neither a usable clock nor a usable
byte offset, so there is nothing to place with and inference remains the
only safe path.

### `seam_plan_total_matches_the_declared_duration`
This is the whole bug in one assertion: the delivered file declared 7893.385
s and carried packets to 8029.298 s — 135.91 s of timeline the playlist says
does not exist.

### `seam_plan_closes_the_forward_skip`
The 9.174 s forward skip between clip 2 and clip 3 must vanish. Measured in
the delivered file as a 20 s window holding 257 video packets where it
should hold 480.

### `seam_plan_joins_an_overlap_without_rewinding`
The 1.79 s overlap at seam 1 must JOIN cleanly, not rewind the timeline.

Clip 1's IN (6031.250 s) precedes clip 0's OUT (6033.041 s): the disc stores
that join twice. Emitting both copies is what collided in the muxer and
flattened 169 audio packets onto the 0.1 ms tick floor, putting audio ~1.8 s
ahead of picture for the rest of the film.

### `map_under_a_seam_plan_tracks_offset_and_frontier`
`map()`'s seam-plan branch — the glue between `SeamPlan::place` and the
frontier/offset bookkeeping — was untested. Audit finding: a wrong operand in
`offset_ns = p - raw_pts_ns`, or a stale `high_ns` across a join, would
corrupt downstream cluster timing and no test would notice.

### `a_sparse_passive_track_crosses_late`
A SPARSE passive track crosses even when its first frame after the join
lands well past the mark.

Audit finding. A PGS subtitle track may have no event near a clip's IN at
all. Holding it to the dense-video window (250ms either side of the mark)
left it on the PREVIOUS clip's offset until its PTS finally passed that
clip's OUT — mistiming every subtitle in between by the overlap, 1.79s on
the measured title.

### `each_track_crosses_a_join_on_its_own_frame`
This is the regression for the first attempt at this fix, which gave every
track the cursor the video had moved. At an overlap the previous clip's
audio tail is still arriving after video has crossed, and those tail frames
sit inside BOTH clips' ranges — so a shared cursor gave them the new clip's
offset, threw them forward by the overlap, and made them collide with the
new clip's own audio. Measured on a real remux: 169 audio packets flattened
onto the 0.1 ms tick floor and a 1.80 s jump, i.e. the original symptom,
still present after the timeline length was already correct.

### `only_blu_ray_gets_a_mark_driven_plan`
Audit finding, and a regression this nearly shipped: HD-DVD `Clip` marks
come from the XPL's title-relative times, and a DVD's from cell tables —
neither is a position in the PES clock. A plan built from them is an
identity map with a drop filter: it suppresses the layer-break rebase
`adjust` performs, and drops whatever falls outside marks the PTS was never
measured against. Both formats must stay on inference.

An earlier reading of this concluded HD-DVD was safe because its marks
happen to be contiguous, so the computed offsets were all zero. That is true
and irrelevant: the offsets were zero in the WRONG CLOCK.

### `a_second_video_track_keeps_the_reorder_safe_window`
Audit finding: the rule was keyed on `drives`, so the EL took the branch
whose premise is "no reorder". Its ordinary reorder dip near the end of a
clip — a backward step that, during an overlap, also lands inside the next
clip's range — was then read as a join, and the EL was placed on the next
clip's offset: out of step with the base-layer frame it must be co-timed
with, by the width of the overlap.

### `a_restarting_clock_falls_back_to_inference`
Audit finding. Each clip is validated in isolation (span > 0) but the
placement rules assume the marks are points on a single clock. Under a table
whose clips each restart their own base, a crossing can be missed — and a
missed crossing STRANDS the track on its current clip, so every later frame
falls outside that clip's marks and is dropped for the rest of the title.
Silent truncation, which is what this type exists to prevent.

### `contiguous_clips_produce_a_constant_offset`
This is the no-regression guarantee for every title that is multi-clip but
not seamless-branching — HD-DVD's feature is chaptered this way (one real
title measured: 3 clips, each IN equal to the previous OUT).

### `single_clip_trims_content_outside_its_marks`
A SINGLE-clip title still gets a plan: joining is not the marks' only job,
trimming to `[in, out]` is. Real discs author trailing audio (a fade after
the last video frame) PAST the OUT mark in the m2ts; the no-plan inference
path kept it, leaving audio seconds past the declared duration — the
audio-drift defect found on a real disc (audio +35.6 s past a single-clip
title's end). The plan drops it, and every KEPT frame is placed exactly as
the no-plan path would (raw rebased by `−in_ns`, which the MKV muxer already
does), so a disc with no out-of-mark content is byte-identical.

### `single_clip_late_subtitle_does_not_inflate_offset`
PRIMARY rc3 regression: a sparse, lagging NON-VIDEO track (PGS subtitle /
trailing audio) on a SINGLE-clip title must NOT inflate `offset_ns`. This is
the exact false-positive that destroyed a real title's seek index: with a
shared frontier, a late subtitle PTS ratcheted the frontier up, then the
next normal video frame sat >3s below it and was misread as a clip
boundary, permanently bumping the offset — thousands of times, until the
Cue/cluster timestamps inflated into the billions of ms.

Correct behaviour: non-video frames ride the current offset and NEVER touch
the frontier or the offset, so no amount of subtitle/audio lag can trigger a
rebase on a one-clip title.

### `dv_enhancement_layer_does_not_drive_epochs`
PRIMARY rc3 regression (Dolby Vision dual-layer): a SECOND video track — the
DV enhancement layer — runs its OWN PTS timeline interleaved with the base
layer's, so the two video PTS sequences OVERLAP. The EL must be a PASSIVE
rider (drives_epoch == false): if it drove epochs, every EL GOP would look
like a multi-second backward jump against the base-layer frontier and
false-trigger a clip-boundary reset — the exact ratchet that inflated a
1-clip 1h49m timeline to ~7 h.

### `non_video_never_advances_frontier`
Companion: a non-video frame must never ADVANCE the frontier. Even a
non-video PTS far ABOVE the current video frontier (a subtitle/audio
timestamp that leads the video momentarily) leaves `high_ns` untouched, so a
subsequent normal video frame is not misread as a boundary.

### `continuity_large_clip_boundary_backjump_rebased`
Regression for the originally-reported band: a LARGE, real-magnitude
clip-boundary back-jump on VIDEO (clip 1 ≈ 13 min, clip 2 resets to 0) must
STILL be rebased to one continuous monotonic timeline — the genuine
multi-clip seamless behaviour is preserved, now keyed on real video
back-jumps.

### `non_video_straggler_remapped_to_seam_at_boundary`
At a REAL video-driven boundary, a lagging NON-VIDEO tail frame from the
just-ended clip (an old-epoch raw PTS arriving interleaved after the reset)
must be REMAPPED to its true seam position with the PREVIOUS offset — not
flung ~a clip past the frontier by the freshly-bumped offset. Otherwise it
would force a forward-dated split cluster and break cluster monotonicity.

### `normal_new_epoch_frame_leading_frontier_is_not_clamped`
Regression for the over-eager straggler clamp: a NORMAL new-epoch non-video
frame that leads the (sparse, video-only) frontier by MORE than one backstep
must ride the CURRENT offset — it must NOT be demoted into the just-ended
clip's epoch. Such a frame satisfies BOTH of the old discriminator's
conditions (current-map > frontier+backstep AND prev-map <= frontier), so
the old `prev_mapped <= high` test wrongly clamped it back ~a whole clip. The
tightened lower bound (`prev_mapped >= high - backstep`) fixes it.

### `frames_arriving_before_their_epochs_video_ride_a_provisional_offset`
MEASURED on a real DVD title with 8 cell boundaries. The demuxer hands the
muxer ~18 audio frames of the NEXT cell before that cell's first video
frame. Riding the just-ended epoch's offset put them ~21 s in the past, and
the MKV writer's strictly-monotonic nudge then crushed the run onto one
instant 0.1 ms apart — half a second of audio as a click, eight times in an
8-minute title.

They must instead continue after the frontier, and must rejoin the real
offset seamlessly once the video opens the epoch.

### `a_straggler_is_judged_against_its_own_epochs_end_not_the_frontier`
MEASURED on a real HD-DVD title. Its second audio track's LAST frame carries
a clip-1 raw PTS but arrives after clip 2's video opened the epoch, so it
took clip 2's offset and landed at 12834.587 s in a 6434.100 s title — one
packet, exactly double.

The old remap existed for this and refused it: the frame sits 23 s below the
CURRENT frontier, outside the 3 s window. Judged against the frontier of the
epoch it actually belongs to, it is 0.15 s from that epoch's end.

### `a_saturated_frontier_does_not_overflow_on_passive_frame` (named `saturated_frontier_does_not_overflow_on_passive_frame`)
A saturated frontier must not panic the muxer. An `mkv://` source's
tick→ns multiply saturates at `i64::MAX` (mkvstream's `parse_block`), so a
hostile TimestampScale/CLUSTER_TIMESTAMP puts `high_ns` AT `i64::MAX`. Every
subsequent PASSIVE frame then evaluated `high + BACKSTEP`, which panicked
("attempt to add with overflow") out of the public `Stream::write` path in
any overflow-checked build.

### `extreme_video_pts_does_not_overflow_the_epoch_bump`
The epoch-decision side of the same arithmetic: `adj < high - BACKSTEP` and
the `high - adj` bump both took untrusted ends. A frontier at
`i64::MIN`-adjacent values (a negative SimpleBlock-relative timestamp) and a
`i64::MAX` frontier are both reachable from container data.
</content>
