# `demux://` sink — per-codec ES output

The `demux://` sink is a write-only `crate::pes::Stream`. Instead of muxing
the per-track `PesFrame` stream into a single container (as `MkvStream`
does), it routes each frame's payload to the file for `frame.track`,
post-processing where the internal codec `Frame` form differs from the
standalone on-disk ES form:

- **HEVC / H.264**: the codec parsers emit hvcC/avcC-style 4-byte
  length-prefixed NALs (start codes stripped) and carry the parameter sets
  out-of-band in `codec_private`. A standalone `.hevc`/`.h264` needs Annex-B
  framing with the parameter sets prepended — see `AnnexBWriter`.
- **PGS**: the parser collapses a display/clear PCS pair into one
  duration-bearing `Frame` with the raw segment payload but no `PG` magic /
  timestamp header. A `.sup` needs the HDMV segment framing rebuilt — see
  `PgsSupWriter`.
- **VobSub**: the `.sub` is the raw SPU stream; the `.idx` sidecar is
  synthesized from per-SPU PTS + byte offsets — see `VobSubWriter`.

Every other codec writes `frame.data` verbatim (`PassthroughWriter`).

The sink does NOT touch the MKV mux path; it is purely additive.

## `PgsSupWriter`: rebuilding HDMV segment framing

The parser hands the demux sink the concatenated PGS segments of a display
set in `frame.data` (segment_type + segment_size + payload, repeated), with
no `PG` magic and no PTS/DTS. A `.sup` prefixes each segment with a 13-byte
header: `0x50 0x47` ("PG") | PTS u32 BE | DTS u32 BE | (segment_type|size
already present in the payload). When the parser folded a trailing clear
(`duration_ns` set), the writer re-emits it as an empty composition at
`pts + duration` so players time the subtitle out.

## `synthetic_clear_display_set`: synthesizing the clear PCS

Builds a synthetic "clear" display set: an empty PCS (0 composition objects)
followed by an END segment. The parser folds the original clear/end PCS
pair's wipe time into the display frame's `duration_ns` and drops the clear
bytes, so a faithful `.sup` re-emits one here at `display_pts + duration`;
without it every subtitle lingers to EOF. `width`/`height` are carried from
the display set's PCS so the clear PCS advertises the same video geometry —
they don't affect the wipe but keep the segment well-formed. Returned bytes
are concatenated `type(1)+size(2 BE)+payload` segments, the same shape
`emit_segments` consumes.

## Test: `epoch_driver_follows_ref_video_not_track_zero`

Regression for the hardcoded `frame.track == 0` epoch driver: on an M2TS/PMT
title the PMT can list an AUDIO ES before the VIDEO ES, so the video lands
at stream index 1. The sink already resolves the video reference
dynamically (`ref_video_track`); the epoch driver must use that SAME
reference, not the literal 0. In the test, track 0 is audio and track 1 is
video; a video-only clip boundary (track 1) must open a new epoch (bump
`offset_ns`). With the bug (only `frame.track == 0` drives epochs) the
video back-jump would be treated as a passive rider and `offset_ns` would
stay 0 — corrupting every track's rebased timeline.

## Test: `a_demux_export_the_seam_plan_emptied_fails`

Audit finding: this sink builds its timeline with
`TimelineContinuity::with_clips` but no test ever gave it a title with
usable PlayItem marks, so `frames_mapped` and `dropped_total()` were always
zero and BOTH gates in `finish` were unreachable. Deleting either left the
whole suite green while a `demux://` export of a seamless-branching title
wrote zero-byte track files beside a populated chapters document, at exit 0.

## `sanitize`: path-hostile filename characters

Control characters are replaced, NUL above all. Every string this touches is
disc bytes, and a language code is three raw STN bytes run through
`from_utf8_lossy` with no validation — `00 00 00` is the ordinary
"undefined" encoding on real Blu-rays. A NUL in a path aborts
`File::create` with `InvalidInput`, which took the whole demux export down
before a single track file was opened.
