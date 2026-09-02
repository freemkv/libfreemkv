# `resync` — B1 drop-to-IRAP gate rationale

## Why drop forward instead of just dropping the lost frame

When packets are lost, the affected access unit is already dropped at the TS
layer (the assembler drops the partial PES on the continuity gap). But for
INTER-CODED video the frames that follow reference the lost frame (and each
other) until the next IRAP/IDR keyframe — emitting them makes any decoder
fault on the "missing reference / non-existing PPS" condition and visibly
break decode. So after a gap on a video track this gate DROPS FORWARD to the
keyframe and resumes cleanly there. The gap rounds up to (at most) one GOP —
the price of never emitting a dangling reference; it is logged.

## Why this is not simply "audio vs video"

Most audio and all subtitle frames are independently decodable (each frame
re-inits on its own header), so a gap there costs only the single
already-dropped frame and this gate is a no-op for them (it always admits).
The one exception is TrueHD/MLP, whose predictor + restart state spans access
units: its re-init point is a codec-specific major-sync AU (not a generic
keyframe), so it runs the equivalent drop-forward-to-major-sync inside
`codec::truehd` rather than through this gate. In short: this gate is NOT
keyed on "audio vs video" but on "needs a re-init point after a gap" — video
here, TrueHD in its own parser, everything else genuinely independent.

## `admit` parameters and behavior

* `is_video` — inter-coded video track (the only kind with cross-frame
  references); `false` for audio/subtitle, which always admit.
* `discontinuity` — this frame's source PES followed a TS continuity gap.
* `keyframe` — this frame is a self-contained IRAP/IDR.

A non-video track always admits. A video track arms on a discontinuity and
then drops every non-keyframe until (and excluding the drop of) the next
keyframe, which disarms and is emitted.

## Test: `a_resolved_gap_still_reports_its_dropped_frames`

`dropped` is per-run and `dropped_total` is cumulative, and only the second
can report loss. A gap that RESOLVES — the common case — disarms the gate at
the next keyframe and zeroes `dropped`. Anything reading that counter
afterwards sees nothing happened, which is how concealed video loss reached
no caller: the only EOF warning fired for gates STILL armed, i.e. exactly the
gaps that did not resolve.
