# `src/mux/codec/mod.rs` — extended notes

Relocated prose from doc comments in `mod.rs`, trimmed there to stay within
the comment-guard's prose caps. Each `//`/`///` pointer in the source names
the section here it corresponds to.

## `Frame::discontinuity`

Carried per-FRAME (not per-PES) because buffering parsers decouple the frame
from the PES that carried the gap signal: MPEG-2 emits whole GOPs, H.264/HEVC
lag one access unit behind the PES that produced them. A clean rip leaves
every frame `false`; the flag is only ever set on the degraded/conceal path
(P3/B1), when a frame's data begins after packets the demuxer never received
— an undecryptable unit concealed as NULL-TS upstream, or a continuity break
in a damaged source.

## Drop-on-undecodable policy across codecs ("clean muxes always")

- **Audio with independent access units** (DTS, AC-3/E-AC-3, …) gates each AU
  through a per-codec corruption check and drops the ones that fail, keeping
  A/V sync (a drop is a silence gap, never a shift) and logging every drop
  via the shared `dropgate::DropTally`. DTS validates via its core-frame
  header (ETSI TS 102 114); AC-3 uses its native frame CRC.
- **LPCM is excluded on purpose**: raw PCM carries no framing or integrity
  data, so a corrupt sample is indistinguishable from a quiet one — there is
  nothing to detect, so nothing can be honestly dropped.
- **Video is excluded on purpose**: H.264/HEVC/MPEG-2/VC-1 are inter-frame
  predicted, so dropping one frame corrupts every frame that references it
  until the next keyframe. Video instead resyncs at GOP/IDR boundaries (the
  ResyncGate) and lets the decoder conceal — a fundamentally different model
  than per-frame audio dropping.
- TrueHD/MLP, FLAC, MP2/MP3 and AAC-ADTS also gate undecodable frames via a
  `DropTally` (poison/drop-forward for MLP's inter-AU restart state on a
  major-sync boundary; CRC/sync-verdict drops for the passthrough codecs).

## `codec_private()` — why "absent" is the only truthful answer for some parsers

A codec parser's `codec_private()` feeds `DiscStream::codec_private`, which
the MKV muxer turns directly into the track's `CodecPrivate` element (RFC
9559 §5.1.4.1.24): `Some(bytes)` writes an element holding exactly those
bytes, `None` omits the element entirely. The two are NOT interchangeable —
a zero-length `CodecPrivate` asserts that the codec's initialisation data IS
empty, which is not true of any codec, and a one-byte one asserts a config no
decoder can parse.

The gating parsers (ADTS, MPEG audio, FLAC) and the passthrough parser
extract no configuration at all: they validate and forward frames whose
configuration is carried in band (ADTS headers, MPEG-1 audio frame headers,
FLAC frame headers) or supplied by the source container. Having derived
nothing, the only truthful answer they can give is "absent" — any `Some`
would be a value they invented. Nothing else in the test suite distinguished
the two, so each of these impls could have returned a fabricated `Some` and
produced a malformed track header unnoticed.

## `provenance_guard` test module

Every emitted frame must carry the source byte offset of the packet it came
from. That invariant held only for video for as long as it existed: `dts`,
`ac3`, `adts`, `truehd`, `pgs`, `dvdsub`, `flac`, `lpcm`, `mpegaudio` and the
passthrough parser all built frames with `source: None`, so a multi-clip
title could not place audio or subtitles by byte and fell back to inferring
the clip from timestamps — which is what made branched titles run minutes
long.

Nothing asserted it, so nothing caught it. Finding it took a brace-balanced
scan of the tree by hand, which also turned up five sites a regex had
missed. `provenance_guard` is that scan, as a test.

### `code_only` / `frame_literals` design

`frame_literals` walks `Frame { .. }` literals with balanced braces. A regex
cannot do this — `Frame` blocks contain nested braces (`coding: Some(..)`,
closures) and a non-greedy match stops at the first `}`, which is how five
sites survived the first pass.

`code_only` strips line comments before scanning. A comment that MENTIONS
`source: None` is prose, not a construction — this guard's own doc comment
tripped it on the first run, which is the same mistake as grepping a file
and matching its commentary.
