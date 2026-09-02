# mkvstream internals

Relocated rationale/history for comments in `src/mux/mkvstream.rs` that
exceeded the comment-guard's internal-comment cap. Each section is pointed
to from a short `// See docs/mkvstream.md#<anchor>` comment at its call site.

## ebml-size-caps

`MAX_BLOCK_SIZE`: largest accepted SIMPLE_BLOCK payload. A block is a small
vint track header + 2-byte rel-ts + 1-byte flags + one frame of elementary
data. UHD HEVC keyframes run a few MB; 64 MiB is generously above any real
single-frame block while still bounding a hostile allocation.

## apply-coding-to-track

Set a video track's `FieldOrder` from the MEASURED coding of the first coded
picture — the parser's value, the first time, never a guess.

A progressive track — or a progressive picture on an interlaced-flagged track
— has no field order (left UNDETERMINED — expected); the latter case ALSO
clears the track's `interlaced` flag, since the declared scan type came from
the IFO/MPLS resolution and the measurement supersedes it. An INTERLACED
track that reaches here WITH a video picture but no measured field order is a
parser/source gap (MPEG-2 carries `top_field_first` on every interlaced
picture, so it should never be missing): LOG it loudly so the source can be
debugged, and leave UNDETERMINED — a muxer never fabricates a source fact.
`video_picture_seen == false` (an empty title finalized with no frames, or a
cap-triggered build that never saw the video frame) is NOT a defect — the
missing coding is expected there, so log it quietly.

## track-table

Read-side map from Matroska TrackNumber to the index of the corresponding
entry in `DiscTitle::streams`.

RFC 9559 §5.1.4.1.1 constrains TrackNumber only to be non-zero ("range: not
0"); NOTHING in the specification requires the numbers to be `1..=N`, to be
contiguous, or to appear in ascending TrackEntry order. `parse_track` also
DROPS every TrackEntry whose TrackType this crate cannot carry (anything but
1/2/17 — e.g. a TrackType 18 buttons track), so the TrackNumber space and the
stream vector diverge for perfectly legal inputs.

The reader used to derive the stream index as `TrackNumber - 1`, which routes
blocks to the WRONG stream (parsed by the wrong codec parser) or drops them
entirely. This table records the real TrackNumber for each retained stream,
in stream order, and is the only thing allowed to translate between the two.

## split-lacing

Split the body of a LACED (Simple)Block — the bytes after the flags octet,
beginning with the Lacing Head — into its individual frame payloads, per
RFC 9559 §10.3.

`lacing` is the 2-bit LACING field value (`LACING_XIPH`, `LACING_EBML` or
`LACING_FIXED`). Returns `None` when the lacing header is malformed — the
frame boundaries are then unknown, and the caller MUST reject the block
rather than hand a concatenation of frames plus lacing header downstream as
though it were one frame (which is exactly the silent corruption this
function exists to end).

The Lacing Head is "number of frames in the lace minus 1" on one octet, so
the frame count is bounded by 256 and no allocation here is attacker-scaled.

## parse-block

Parse a (Simple)Block payload into zero or more PesFrames.

Zero frames means the block was SKIPPED — too short, track 0, or a
TrackNumber this file does not (retainedly) declare. More than one frame
means the Block was LACED (RFC 9559 §10.3): one Block legitimately carries
several frames, and handing the raw payload downstream as a single frame
feeds the codec parser a concatenation of frames plus lacing header. An
`Err` means the lacing header is malformed, so the frame boundaries are
unknowable — the block is rejected rather than mangled.

`cluster_ts_ticks` is the open cluster's timestamp in TimestampScale ticks
and `ts_scale_ns` is that scale (ns per tick); the block PTS is computed as
`(cluster_ts_ticks + rel_ts) * ts_scale_ns` so foreign MKVs whose scale
isn't 1 ms are honoured (freemkv's own output uses 1_000_000 and round-trips
unchanged). `tracks` resolves the TrackNumber to a stream index; `duration_ns`
is propagated for BlockGroup blocks (None for SimpleBlock).

## block-additions-dropped-test

A BlockGroup's `BlockAdditions` subtree (BlockAddID=2 — the MVC
dependent/right-eye access unit this crate's 3D writer emits, see
`mkv.rs::build_block_group`) cannot be carried by `PesFrame`, so read-back
drops it. That is a LOSSY outcome, and this crate's rule is that a lossy
outcome is never silent.

Regression: the arm did not exist, so the subtree fell into the `_ =>`
skip arm — an `mkv://` → `mkv://` re-mux of a 3D rip lost one whole eye
with no error, no warning and `lost_bytes == 0`, i.e. the mux reported a
clean, complete, loss-free copy of a file it had halved. The base view
must still read back intact, and the dropped payload must now be counted
so it reaches `MuxOutcome.lost_bytes` / `.errors`.

## reference-block-keyframe-test

A BlockGroup carrying a ReferenceBlock is NOT a keyframe — that element's
presence is the only non-keyframe signal a BlockGroup has (the SimpleBlock
0x80 flag bit is reserved and always 0 inside one).

Regression: the reader used to `skip_bytes` past REFERENCE_BLOCK and read
the reserved bit instead, so EVERY BlockGroup frame came back as a
non-keyframe. Since the MPEG-2 parser stamps a per-frame duration, all
MPEG-2 video takes the BlockGroup path — so no video frame ever looked
like a keyframe on re-mux. That silently dropped all video on
`mkv://`→`m2ts://` and failed `mkv://`→`mkv://` with E6008.

## ebml-lacing-test

EBML lacing (RFC 9559 §10.3.3): the Lacing Head, the first frame's size as
an unsigned VINT, then each later size as a SIGNED VINT difference from
the previous one. Three frames of 3/4/5 octets must come out as THREE
frames with byte-exact payloads.

Regression (silent corruption): the reader took the Block payload verbatim
and never looked at the LACING bits, so this Block became ONE 15-byte frame
whose first three bytes are the lacing header — garbage handed to the codec
parser with no error, and one timestamp for three frames.

## track-number-gap-and-sub-44100-sampling-test

RFC 9559 §5.1.4.1.1 constrains TrackNumber only to be non-zero — nothing
requires `1..=N` in TrackEntry order. A file with a TrackType this reader
drops (18 = buttons) between two carried tracks makes the TrackNumber
space and the stream vector diverge.

Regression (silent corruption): the reader computed the stream index as
`TrackNumber - 1`, so the audio blocks of TrackNumber 3 resolved to index
2 in a 2-stream title and were DISCARDED — a remux with no audio, reported
as success.
A legal SamplingFrequency below the lowest rate this enum maps must come
back as Unknown, not silently as 48 kHz.

The ladder's final `else` was `SampleRate::S48`, so a 32000 Hz AC-3 or DTS
track — legal, and common in broadcast-sourced content — was recorded as
48 kHz and the wrong rate propagated into the reconstructed AudioStream.
The crate's canonical mapping, `SampleRate::from_hz`, returns Unknown for
32000; this ladder disagreed with it.

## finish-produces-readable-mkv-test

`finish()` is what turns a stream of frames into a FILE. It activates a
still-pending muxer (writing EBML header, Segment, Info, Tracks), then
finalizes it (Cues, SeekHead, the backpatched Segment size). A `finish`
that returned `Ok(())` without doing any of that leaves the caller with a
zero-byte or truncated `.mkv` and an exit code of 0 — a rip that reports
success and produced nothing.

Proven by reading the output back through this crate's own MKV reader:
the frames must come out in order, with their real payloads, timestamps
and keyframe flags.

## finish-refuses-zero-frame-title-test

A title that produced NO frames must NOT finish successfully. `finish()`
activates the still-pending muxer (so the header/Tracks are written) and
then hands off to `MkvMuxer::finish`, whose zero-frame guard raises
`Error::MkvInvalid` (E6008) rather than emitting a structurally valid but
clusterless MKV.

A `finish` that returned `Ok(())` would report a completed rip for a
title that muxed nothing — precisely the "empty title, exit code 0"
outcome the guard exists to prevent — and `error::is_skippable_title_stub`
would never get the code it classifies on.

## headers-ready-and-untrusted-size-caps-test

`headers_ready()` gates the CLI's wait-for-codec-private loop. For
Matroska it is unconditionally true because RFC 9559 §5.1 places the
Tracks element (carrying every CodecPrivate) in the Segment header,
ahead of the first Cluster — `MkvStream::open` has therefore already
parsed them by the time it returns. Returning `false` would hang the
mux forever on a source whose headers are, by construction, present.

Pinned as an implication rather than a bare constant: readiness is
asserted TOGETHER with the codec private actually being retrievable, on
a freshly opened stream that has read no frame yet.
The untrusted-size caps ARE the OOM guard: every EBML element size is
checked against one before it is used to allocate. Only their existence
was pinned, never their magnitude — so a cap that collapsed to a few
kilobytes would still look guarded while rejecting ordinary discs, and one
that ballooned would allocate whatever a hostile container asks for. Both
ends need a number.

## track-entry-metadata-round-trip-test

Write a real three-track title through this crate's own muxer, then read
it back through this crate's own reader and check the TrackEntry metadata
survived. Language, track name, the forced flag, pixel height and channel
count each had a dedicated arm in `parse_track` and NONE of them was
asserted — every one could have been deleted and the suite stayed green
while a re-mux quietly lost the audio language, the subtitle forced flag,
the track labels, the resolution and the channel layout.

## default-track-dedup-test

FlagDefault says "play this track unless the viewer picks another". Only
ONE video and ONE audio track may carry it; `MkvTrack::video`/`audio` set
it from `!secondary` alone, so a disc with two ordinary video angles or
two ordinary audio tracks arrives here with the flag on all of them and
this de-duplication is the only thing that fixes it. It lives here and
nowhere else — the muxer just writes what it is handed — and it had no
test at all.

## progressive-on-declared-interlaced-test

A DVD whose IFO declares 480i/576i but whose pictures are CODED
progressive — film and animation on NTSC discs routinely are — must ship
`FlagInterlaced=progressive`, because that is what the bitstream says.

`MkvTrack::video` sets `interlaced` from the DECLARED resolution, which
on a DVD is 480i essentially always. Shipping that unchecked marks
progressive content interlaced, and every player that honours the flag
then runs a deinterlacer over progressive frames — softening every frame
of an otherwise bit-exact remux. Measured against a real disc: `idet`
reports 100% progressive on titles this shipped as `FlagInterlaced=1`.

The sibling test above pins the interlaced direction, so together they
constrain both: a measurement of TFF keeps the track interlaced, and a
measurement of progressive corrects it.

## deferred-activation-end-to-end-test

THE deferred-activation contract, end to end: the field order MEASURED
from the first coded picture has to reach the FILE.

`apply_coding_to_track` was tested in isolation, which proves nothing
about whether the caller ever reaches it with a real measurement — and
the route there runs through the pending-buffer cap, the
"is this the video frame" test and the activation trigger, none of which
were observed from the outside. Any of them mis-set and the file ships
FieldOrder omitted: an interlaced DVD that players then deinterlace with
the fields in the wrong order (visible combing on motion).

## video-master-walk-bounds-test

The `Video` master's child walk must consume EXACTLY the bytes its
children declare. This crate's own writer happens to place every other
TrackEntry field ahead of `Video`, so an over-run there costs nothing —
but a foreign MKV (mkvmerge orders children differently) puts fields
after it, and an over-running walk then swallows them: the reader reports
language `und` for a track that declared one, on a file that is perfectly
well formed.

## skip-bytes

A skip that runs out of input before `n` bytes is a TRUNCATED element, and is
reported the same way `ebml::read_binary_val` reports a truncated body: as
`MkvSourceInvalid`. Discarding `io::copy`'s byte count instead made a skip
that hit EOF look like a success, so one corrupt size field mid-Clusters
drained the rest of the file, the next element header raised
`UnexpectedEof`, and `Stream::read` mapped that to `Ok(None)` — half the
title missing, `errors = 0`, `completed = true`.
