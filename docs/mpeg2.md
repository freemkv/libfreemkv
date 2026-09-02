# MPEG-2 video parser notes

## Module overview

**One PES is NOT one frame.** On a DVD the video elementary stream is sliced
into ~2 KB Program-Stream PES packets (one per 2048-byte pack), so a single
coded picture (~10-100 KB) spans many PES packets and only the first carries
a PTS. Emitting one MKV block per PES would write frame *fragments* — the
decoder then sees truncated pictures (`ac-tex damaged`) and picture-coding
extensions detached from their picture header (`ignoring pic cod ext`). So
this parser buffers ES bytes across PES packets and emits exactly one Frame
per coded picture. (Blu-ray aligns one access unit per PES and would not need
this, but DVD MPEG-2 PS does.)

Access-unit model (ISO/IEC 13818-2): an AU is an optional sequence header +
optional GOP header + one picture header + its coding extension + slices. A
new AU begins at the next picture / sequence / GOP start code *once the
current AU already contains a picture* — leading sequence/GOP headers attach
to the picture that follows them.

Start codes:
- Picture header:     00 00 01 00
- Slice:              00 00 01 01 .. AF
- Sequence header:    00 00 01 B3
- Extension (seq/pic):00 00 01 B5
- GOP header:         00 00 01 B8

## `MAX_PENDING_BYTES`

`MAX_PENDING_FRAMES` alone bounds the *count* of frames held awaiting the
first PES PTS anchor, but 600 full HD/UHD intra pictures can be ~1 GiB. This
mirrors the AC-3/DTS/PGS byte caps: once the held data exceeds this, release
on the 0 base instead of accumulating further. 8 MiB is roughly a few large
I-frames, far more than the ~15 frames a well-formed DVD buffers before its
first PTS.

## `Mpeg2Parser::process_au`

Processes one reassembled access unit (from
[`AuAssembler`](crate::mux::au_assembly::AuAssembler)): decodes its
per-picture coding info, captures a new sequence header, and buffers the
picture into the current GOP for display-order timestamping. The AU's
timing / source / discontinuity were already attributed by the assembler.

## `Mpeg2Parser::flush_gop`

Emits the buffered GOP. Each frame's PTS is the display-order prefix-sum of
field durations from the timeline origin; its block duration is its own
`nb_fields × field_period`. Frames are emitted in DECODE (buffer) order —
B-frames keep their position with a correctly LOWER PTS, never reordered
(reordering emitted blocks is what corrupts the picture). The origin is
(re-)locked to the GOP's PES PTS; because that is a *presentation*
timestamp, backing out the carrying frame's display-field offset keeps the
timeline continuous and monotonic across GOP boundaries.

## `extract_seq_header`

Extracts the sequence header (+ any B5 extensions / user-data, up to the
first GOP or picture start code) from a fully-assembled access unit —
exactly the extradata an MPEG-2 decoder expects as codecPrivate. Returns
None if the access unit carries no sequence header. A NEW header replaces
the stored one (title boundary / channel change), so its extension is
always re-captured.

## `picture_coding_flags`

Extracts the picture-coding-extension field/pulldown flags
`(top_field_first, repeat_first_field, progressive_frame, frame_picture)`
from a coded access unit (`00 00 01 B5`, ext-id `1000`), per ISO/IEC
13818-2 §6.3.10. The four bits feed the codec-agnostic `PictureInfo`.
Returns a progressive whole-frame default `(false, false, true, true)` when
no picture coding extension is present (MPEG-1 / no interlace signalling),
so the muxer omits `FieldOrder` rather than asserting a guess.

## `picture_nb_fields`

Returns the number of field-display periods a coded picture occupies, from
its picture coding extension (`00 00 01 B5`, ext-id `1000`), per ISO/IEC
13818-2 §6.3.10 (`nb_fields = repeat_pict + 2`, the field count the spec's
repeat rules yield). This is what times soft-telecined (2:3 pulldown) DVD
video correctly: a `repeat_first_field` frame occupies 3 fields, a normal
frame 2, so honoring it spreads the ~23.976 coded frames across the 29.97
display span with no gap (the "play, pause, play" judder). `progressive_sequence`
comes from the sequence extension. Returns 2 (a normal frame) when no
picture coding extension is present.

## `parse_progressive_sequence`

Reads `progressive_sequence` from a captured sequence header's sequence
extension (`00 00 01 B5`, ext-id `0001`). False when absent (MPEG-1 / no
extension) — the interlaced default. Bit layout after the start code:
ext-id(4) profile_and_level(8) **progressive_sequence(1)** … so it is bit 3
of the second extension byte (`hdr[q+5]`).

## Test: `discontinuity_offset_mark_stamps_post_gap_picture_not_previous`

B1 hole-2 regression: MPEG-2 buffers a GOP and emits asynchronously, so a
concealed gap must be associated by OFFSET (like PTS), landing on the
picture whose own bytes begin after the gap — NOT the previous picture
that completes when the discontinuity PES arrives. pic1 (I) is pre-gap;
pic2 (P), carried by a `discontinuity` PES, is the first post-gap AU.
