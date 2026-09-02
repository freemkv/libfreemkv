# `src/mux/au_assembly.rs` — access-unit assembly

## Why this exists

The contract a codec parser converts is `PES → access units (Frames)`. A
*transport* stream hands the parser one AU per PES for free (BD aligns one
access unit per PES; the TS demuxer reassembles to the
`payload_unit_start_indicator`). A *program* stream does not — the PS muxer
chops the elementary stream into fixed-size PES fragments with no AU
alignment, and only the first fragment of an AU carries a PTS. So a parser
that assumes one-AU-per-PES (h264/hevc/vc1, written against TS) mis-frames a
program stream, while `mpeg2` — the DVD/PS codec — must reassemble across PES.

`AuAssembler` is that reassembly, factored out so EVERY program-stream video
parser shares one implementation instead of hand-rolling the buffer. The
h264/hevc/vc1 parsers (`Mode::StartCode` / `Mode::Vc1`) and the MPEG-2 parser
(`Mode::Mpeg2`, via `AuAssembler::mpeg2`) all drive it. It buffers PES-fragment
bytes and emits one AU per codec AU boundary, carrying the AU-start
timing/source forward. Since the boundary is a codec start code, it lives with
the codec parser (which picks the marker); only the generic buffering +
timing-carry is shared here.

This is *inside* the parser, not a pipeline stage: the pipeline stays
`Demuxer → PES → Parser → Frames`, and the demuxer stays codec-agnostic. Every
stream a parser sees runs through one of these — self-framing codecs (MPEG-2,
audio) use `Mode::Passthrough` so the parser code path is uniform.

## `MAX_AU_BUFFER`

Safety cap on a single in-progress access unit. A real coded picture is far
below this; a stream that never yields a second AU boundary is force-flushed
at the cap rather than buffering without bound on hostile/corrupt input.

## `MAX_MARKS`

Cap on buffered timing/discontinuity marks. A real access unit spans a few
hundred PES fragments at most; this bounds the mark deques so a run of
zero-length (or start-code-free) timed fragments — which grow no buffer bytes
and so never trip the `MAX_AU_BUFFER` mark-prune — cannot accumulate marks
without bound on hostile/corrupt disc input.

## `AuAssembler::for_codec`

Video codecs whose parsers assume AU-complete PES (H.264 / HEVC / VC-1) get a
`Mode::StartCode` assembler; MPEG-2 (self-reassembles) and audio/subtitle
codecs (self-framing) get `Mode::Passthrough` so callers can run every stream
through this uniformly.

## `AuAssembler::mpeg2`

The MPEG-2 parser owns one of these directly (rather than hand-rolling the
buffer): the demux layer runs MPEG-2 through `Mode::Passthrough` and hands
each fragment to the parser, which feeds them here to be reframed on picture
boundaries.

## `AuAssembler::push`/`push_owned`

For a self-framing (`Passthrough`) stream the payload is MOVED straight into
the emitted unit with no copy — the common DVD/HD-DVD case (MPEG-2 video, all
audio). A buffering mode copies into `buf` exactly as `push`.

## `take_front`

Detaches `buf[..end]` as the emitted AU's own `Vec` and leaves `buf` holding
the tail.

The AU's bytes are HANDED OVER — `buf`'s allocation becomes the returned `Vec`
and a fresh buffer (pre-sized to the same capacity, so the next AU accumulates
without re-growing) takes its place holding only the short tail.
`buf[..end].to_vec()` + `drain(..end)` instead copied every AU out in full: on
a UHD HEVC title that is a whole-frame memcpy (hundreds of KB) per coded
picture, ~200k times, for bytes that are about to be discarded from `buf`
anyway.

The allocation COUNT is unchanged (one per AU either way — the frame `Vec`
before, the replacement buffer now), so the only difference is the copy that
no longer happens. Nothing depends on `buf` keeping its identity: the only
state tied to `buf[0]`'s position is `base`/`scan_pos`/`opener_pos`, which the
caller updates immediately after.

Falls back to a copy when the buffer's capacity is far larger than the AU (a
small AU after a multi-MB one): handing over would otherwise attach an
oversized idle allocation to a small frame for as long as the frame queues
downstream, trading a copy for resident memory.

That fallback must not become permanent. `buf`'s capacity used to be a one-way
high-water mark — the replacement buffer was created with `cap.max(tail_len)`,
and the copy path's `drain` also preserves `cap` — so once ONE large AU had
been assembled, every later smaller AU satisfied `cap > 2*end` and took the
copy path forever. On a UHD HEVC title the first IDR grows `buf` to ~4-8 MB,
after which each ~200-400 KB P/B AU paid a whole-AU allocation plus a
whole-AU memcpy plus a tail memmove for ~99% of the ~200,000 coded pictures —
tens of GB of exactly the memcpy this handover exists to remove. So the copy
path now also RELEASES the high-water capacity, which re-arms the handover
for the next AU: one copy after a size step down, not one per frame forever.

## `drop_marks_before`

This is the STREAM-START case: bytes ahead of the first AU boundary are the
tail of an access unit that began before we had sync, and there is no prior
AU for them to be discontinuous *from*. Carrying a mark forward here would arm
the resync gate at the head of every title and drop its first GOP.

## `discard_gap_before`

This is the BACKSTOP case: `MAX_AU_BUFFER` bytes accumulated with no AU start
code in them, so the run is unusable and gets thrown away. Unlike the
stream-start trim above, there IS a prior AU here, and whatever follows
definitively does not continue it — a decoder handed the next picture would
resolve its references against frames separated from it by megabytes of
discarded data.

So the discard is itself a discontinuity, whether or not the source signalled
one. It is recorded as a sticky flag rather than an offset mark because a
mark placed at the new base would be retired moments later by the pre-sync
trim that follows resync — the gap has to outlive the bytes that caused it.
It arms the resync gate, which drops to the next keyframe instead of emitting
a picture with dangling references.

Timing marks before `off` are still retired — they describe bytes that no
longer exist, and the AU that eventually emits takes its PTS from the
fragment that actually opened it.

## Tests

### `a_backstop_discard_marks_the_next_au_discontinuous`

The 8 MiB backstop throws away a start-code-free run as unusable. The AU that
eventually emits after that discard MUST be marked discontinuous, whether or
not the source ever signalled a discontinuity: megabytes of the stream are
simply gone, so the next picture cannot resolve its references against the
last one that was emitted.

`discontinuity` is what arms the resync gate downstream (`resync.rs`, driven
from `mux/disc.rs`), which drops to the next keyframe rather than emitting a
picture with dangling references. If the flag is retired with the discarded
bytes, the gate never arms and the broken picture goes out — a silent
corruption, which is the one class of loss this crate refuses to have.

### `a_stream_start_trim_does_not_mark_the_first_au_discontinuous`

The opposite case, and the reason the two call sites are separate. Bytes
ahead of the FIRST access-unit delimiter are the tail of an AU that began
before we had sync. There is no prior AU for them to be discontinuous from, so
retiring the marks there is right — and necessary: marking the first AU of
every title discontinuous would arm the resync gate at the head of each one
and drop its opening GOP.

### `a_source_signalled_discontinuity_reaches_the_au_it_opens`

A source-signalled discontinuity reaches the AU it opens. This is the
`disc_marks` path — the ORIGINAL mechanism, distinct from the sticky
`pending_gap` the backstop sets. Nothing else pins it: the two tests above
drive `pending_gap`, and a mark placed on a fragment that is later discarded
is retired by design.

Deliberately NOT combined with the backstop. A previous version of this test
signalled the discontinuity on the first over-cap push and asserted the flag
on the AU after the discard — but that first run still has the next AU's
delimiter at `buf[0]`, so it force-flushes as an over-long AU, and THAT AU
consumes the mark. The assertion was then satisfied entirely by
`pending_gap`, making the test a duplicate of the one above it under a name
promising something else. The two mechanisms cannot be isolated in one
fixture, so they get one test each.

### `drained_au_takes_over_the_buffer_allocation_without_copying`

MEASURED: a drained AU must be HANDED the accumulation buffer's allocation,
not copied out of it. The emitted `Vec`'s data pointer is the buffer's own
pointer — which is only true if no full-frame copy happened.
(`buf[..end].to_vec()` allocates fresh, so the pointers differ.) One
whole-AU memcpy per coded picture is ~200k memcpys of a few hundred KB each
on a UHD feature.

### `handover_survives_a_large_au_instead_of_copying_every_later_one`

MEASURED: `take_front`'s copy fallback must not become permanent.

`buf`'s capacity used to be a one-way high-water mark, and the copy path's
`drain` preserves it, so after ONE large AU every later smaller AU satisfied
`cap > 2*end` and copied forever. On a UHD HEVC title the first IDR grows
`buf` to multiple MB, after which ~99% of the ~200,000 coded pictures each
paid a whole-AU allocation + whole-AU memcpy + tail memmove — tens of GB of
exactly the copy the handover exists to remove. Counted at the copy path
itself: one copy is expected right after the size step down; a per-frame
copy is the bug.

### `discarded_pre_sync_marks_do_not_time_the_first_access_unit`

After the pre-sync bytes are discarded, the emitted AU must take the timing
of the fragment that ACTUALLY opened it. `drop_marks_before` is what retires
the discarded fragment's marks; a no-op there stamps the first real access
unit with the PTS and source of bytes that were thrown away — a whole-title
A/V sync offset, since every later frame is timed relative to it.

### AU-opener detection: the per-mode start-code rule

`au_opener_from` duplicates a rule each codec parser also encodes; the tests
below pin it to the normative byte values so a drift between copies shows up.

### `au_opener_from_locates_the_real_start_code_per_codec`

The opener offset must be the position of the real start code, never a fixed
0. A constant `Some(0)` makes every pre-sync run of junk bytes look like the
head of an access unit, so the first AU of every stream that does not begin
exactly on a start code is emitted with junk glued to its front.

### `for_codec_routes_each_video_codec_to_its_reassembly_mode`

`for_codec` is the dispatch that decides whether a stream is REASSEMBLED
across PES fragments or passed straight through. Getting it wrong is silent:
an H.264/HEVC/VC-1 stream routed to `Passthrough` on a program source emits
one "frame" per PES fragment — a few hundred bytes of a coded picture, framed
as a whole access unit — and the output plays as corruption, not as an error.

Each mode is identified BEHAVIOURALLY (feed a two-AU stream in two halves and
see whether it reassembles), so the case cannot pass by matching a constant.
