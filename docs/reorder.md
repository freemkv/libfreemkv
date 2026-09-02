# Display-order PTS reconstruction for sparse-PTS program-stream video

MPEG program streams (DVD VOB, HD-DVD EVO) timestamp video at GOP
granularity: only one access unit per GOP carries a PES PTS, and the rest
arrive with none. The H.264 / HEVC / VC-1 parsers collapse a missing PTS to
`0` (`pes.pts.or(dts).unwrap_or(0)`), so on such a source every non-anchor
frame lands on the same block timestamp. A decoder then cannot order them and
reports "non monotonically increasing dts". (The MPEG-2 parser already avoids
this by reconstructing per-picture PTS from `temporal_reference`; these three
codecs carry no such field.)

`SparsePtsReorder` reconstructs a display-order PTS for every frame from two
signals the parsers already provide — the coded picture type (I/P/B) and the
sparse anchor PTS — plus a per-frame duration self-calibrated from the spacing
between consecutive GOP anchors (no external frame-rate needed). It mirrors
the MPEG-2 parser's GOP-buffered origin-locking, but derives display order via
the classic single-anchor-delay rule instead of `temporal_reference`:

- In DECODE order an anchor (I/P) is stored before the B-frames that
  reference it forward, so decode `I P B P B` displays as `I B P B P`.
- The rule that produces that mapping: an anchor is displayed only after the
  previously-held anchor; a B-frame displays immediately. This is exact for
  the classic (non-hierarchical) GOP structures HD-DVD H.264/VC-1 use.

This reconstruction is applied ONLY on the program-stream path
(`ContentFormat::MpegPs`). BD/UHD transport streams carry a per-frame PTS and
are never routed through it, so the primary decode path is untouched.

## `FALLBACK_FRAME_DUR_NS`

Fallback per-frame duration (ns) when the anchor spacing cannot calibrate one
(a stream with a single GOP, or no anchor PTS at all): 24000/1001 fps film,
the dominant HD-DVD cadence. Only affects intra-GOP spacing — each GOP's
origin is re-locked to its own anchor PTS, so a wrong fallback cannot drift
the timeline across GOPs.

## `MAX_GOP_FRAMES`

Force-complete the current GOP once it reaches this many buffered pictures
even without a keyframe. A GOP is normally a few dozen frames; a stream that
never signals a keyframe (open-GOP recovery-point coding, or crafted/corrupt
disc bytes) would otherwise buffer every access unit — the whole title — in
RAM. Mirrors the MPEG-2 parser's `MAX_PENDING_FRAMES` backstop so no
reassembly buffer grows unbounded on disc-controlled input.

## `MAX_GOP_BYTES`

Byte cap on the buffered GOP, complementing `MAX_GOP_FRAMES`. A GOP holds a
couple hundred MB at most in practice; this force-completes a run of
few-but-huge access units so a crafted/corrupt stream cannot over-allocate
(the AU assembler caps each frame at 8 MiB, so 600 frames alone could reach
~5 GiB without this).

## `display_indices`

Display index (0-based, decode order in → decode order out) for a GOP's coded
picture types via the classic single-anchor-delay reorder: an anchor (I/P) is
displayed only after the previously-held anchor; a B displays immediately.
Decode `I P B P B` → display indices `[0, 2, 1, 4, 3]` (display `I B P B P`).
