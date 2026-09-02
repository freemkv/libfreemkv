# tsmux — BD Transport Stream muxer (`src/mux/tsmux.rs`)

PES frames in, 192-byte BD-TS (Blu-ray transport stream) packets out. Each
PES frame is wrapped in a PES header, split into TS packets, and prepended
with the 4-byte TP_extra_header.

## `TsMuxer` framing rules

PIDs in `0x1011..=0x101F` are treated as video (length-prefixed NALUs in,
Annex B out, with parameter-set prepend and RAI on keyframes); every other
PID is carried as `private_stream_1` (`0xBD`) audio/subtitle. All tracks
share one PTS origin seeded from the first video frame, so audio/video PTS
offsets are preserved.

## `video_codec` field

Decides BOTH how the ES is framed and how its `codec_private` parameter-set
record is parsed. One fact, one field: carrying NAL-ness separately from the
codec is what let a track be treated as NAL video while its avcC record was
handed to the hvcC parser.

- HEVC / H.264 arrive length-prefixed (MKV/PES NALU convention) and need
  Annex-B conversion; their parameter sets live in an hvcC / avcC record
  respectively, and the two layouts are NOT interchangeable.
- MPEG-2 and VC-1 are already plain start-code ES; running
  `length_prefixed_to_annex_b` over them mangles the frame into
  empty/garbage output while `frame_count` still increments, so the mux
  "succeeds" and silently produces a video-less file.

Defaults to `Codec::Hevc` — the prior, only behaviour — so a caller that
never calls `TsMuxer::set_video_codec` is unaffected. Ignored for non-video
tracks.

## `build_pes_header` — PTS on continuations

ISO/IEC 13818-1 §2.4.3.7 puts the PTS in the header of the PES packet that
contains the FIRST byte of the access unit; repeating it on the
continuations makes each of them look like a new access unit at the same
timestamp, so a demuxer re-reading the stream splits one display set into
two blocks with identical timestamps. `pts_90k` is `None` for a
CONTINUATION PES packet (one carrying the rest of an access unit too large
for a single bounded-length private_stream_1 PES).

## Test rationale

### `non_nal_video_es_passes_through_unconverted`

A non-NAL codec must pass the ES through byte-for-byte: MPEG-2 and VC-1 are
not NAL-based, so their ES already IS the wire format and
`length_prefixed_to_annex_b` would mangle it. The payload is deliberately
length-prefix SHAPED (a big-endian length followed by that many bytes) so a
wrongly-applied conversion rewrites the leading four bytes into a
`00 00 00 01` start code, making the two paths produce visibly different
bytes — a payload the converter happened to leave alone would let a mutant
pass. Mutation: delete the `set_video_codec` call, or make Mpeg2 report as
NAL, and the emitted ES gains a start code -> this fails.

### `split_access_unit_carries_pts_only_on_the_first_pes`

An oversized private_stream_1 access unit is split across several PES
packets, and every one of them used to carry the SAME PTS (PTS_DTS_flags =
0b10) even though only the first holds the start of the AU. On read-back a
demuxer treats each PUSI as a new access unit, so e.g. the second half of a
full-screen PGS display set arrived as an independent segment at an
identical timestamp — the display set was emitted as TWO blocks with the
same timestamp instead of one.

### `annex_b_conversion_buffer_is_reused_across_frames`

MEASURED: the Annex-B conversion buffer must be REUSED across video frames,
not allocated per frame. Both the allocation's address and its capacity are
unchanged after the second and third same-sized frames — if the conversion
allocated a fresh `Vec` per frame (the old
`Vec::with_capacity(data.len() + 1024)`), the buffer left on the muxer would
be empty with zero capacity, and each frame would pay an
allocate/first-touch/free cycle over the whole ~310 KB frame.
