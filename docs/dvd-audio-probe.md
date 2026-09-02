# DVD AC-3 physical sub-stream probing

## Why this exists (a real-disc wrong-substream bug)

A DVD VTS IFO declares its audio streams in a fixed table, and freemkv's
scan assigns each declared stream a `private_stream_1` sub-stream id purely
by per-codec ordinal — the first AC-3 stream becomes `0x80`, the second
`0x81`, and so on (`ifo::assign_audio_sub_stream_ids`). That assumes the
physical sub-stream order on the wire matches the IFO declaration order.

On some discs it does NOT. A real R2 PAL DVD release's feature declares
ONE AC-3 audio stream the IFO nibble marks as 5.1 (6 channels), but the
physical VOB carries the 5.1 main mix and a 2.0 down-mix on DIFFERENT
`0x8x` sub-stream ids, and the 2.0 is the one that happens to land at the
ordinal `0x80` slot. Routing the declared 5.1 stream to `0x80` by ordinal
therefore muxes the 2.0 down-mix while labelling it 5.1 — the wrong
physical track.

The robust fix is data-driven and codec/disc agnostic: read each physical
AC-3 sub-stream's REAL channel count from the VOB (the `acmod`/`lfeon` of
its first frame after the `0x0B77` sync) and route each IFO-declared AC-3
stream to the physical sub-stream whose actual channel count matches the
IFO's declared count — instead of trusting the ordinal. This never
re-reads the disc beyond a bounded head-of-feature probe and degrades to
the original ordinal mapping when the probe yields nothing
(unreadable/short VOB).

## `PROBE_SECTORS` sizing

How many 2048-byte sectors of the first feature extent to probe. The head
of a DVD feature opens with logos/warnings whose audio is frequently a
thin 2.0 bed on the FIRST sub-stream only — the other physical `0x8x`
sub-streams and the main 5.1 mix do not appear until a sector or two
further in. 512 sectors (1 MiB) was too short: on one real disc it saw
ONLY `0x80`, and only its opening 2.0 frames. 1024 sectors (2 MiB)
reliably contains at least one frame of every physical AC-3 sub-stream
AND enough of `0x80` to reach its 5.1 frames. Still bounded so a live
drive is never hammered (see the project "don't hammer the live drive"
rule).

## Why the maximum channel count, not the first frame

The first frame of a sub-stream at the head of a feature is NOT
representative. A DVD opens with logos/warnings, and the main `0x80`
sub-stream there frequently carries a thin 2.0 bed before transitioning
to its real 5.1 main mix a fraction of a second later (observed on a real
disc: `0x80`'s first frames are acmod=2 → 2 channels, then it becomes
acmod=7+lfe → 6 channels within the same 2 MiB window). Recording only
the FIRST frame read `0x80=2` and missed the 5.1 entirely, defeating the
channel-match routing. The 5.1 capability of a sub-stream is the
*maximum* channel count any of its frames carries, so
`probe_ac3_substream_channels` scans them all and keeps the max.

## `remap_audio_pids` conservatism

Conservative — it only ever REASSIGNS among the physical sub-streams the
probe actually saw, and only when a better (exact-channel) match exists
than the stream's current assignment. A stream whose current sub-stream
already matches is left alone; a stream with no matching physical
sub-stream keeps its ordinal assignment. So a normal disc (physical order
== IFO order) is a no-op.

## Test helper notes (`dvd_audio_probe::tests`)

- `ac3_frame`: builds a single, correctly-SIZED AC-3 frame whose
  `acmod`/`lfeon` encode a known channel count. `byte4` is
  `fscod=0 | frmsizecod=0`, so `ac3_frame_size` reports 128 bytes and the
  frame is zero-padded to exactly that — this lets `max_substream_channels`
  advance frame-by-frame over a multi-frame payload exactly as it does on
  real VOB data. The BSI bits are laid down with a writer so the test
  never hand-miscomputes the lfeon offset, matching `acmod_channels`'
  reader.
- `ps_ac3_frames`: builds a minimal `private_stream_1` PES carrying
  `frames` for `sub_id`, each preceded only by the 4-byte AC-3 sub-header
  at the PES head. Mirrors the on-disc layout the PS demux expects: PES
  start `0x000001BD`, length, PES header (no PTS), sub-header
  `[sub_id, frame_count, ptr_hi, ptr_lo]`, then the concatenated AC-3
  frames.

### `probe_reads_max_channels_no_cross_contamination`

Real-disc regression — the probe must read each sub-stream's TRUE
(max-mix) channel count, not be poisoned by an unrepresentative head
frame, and must NOT cross-contaminate between sub-streams. Mirrors the
real on-disc layout that caused the mis-read: the feature head carries
`0x80` opening with a 2.0 frame and THEN a 5.1 frame (its real main mix),
interleaved with `0x81` carrying only 2.0. The old first-frame probe read
`0x80=2` (the logo bed) and missed the 5.1; the max-over-frames probe
must report `0x80=6` and `0x81=2`.

### `max_substream_channels_locates_sync_after_leading_non_sync_bytes`

Regression guard for a hand-checked mutation (`+` → `-` at the
`pos + rel` offset computation): with `pos` starting at 0 and the first
sync found 3 bytes in, `pos - rel` would underflow a `usize` and panic,
or (if it somehow didn't) index the wrong start entirely. `pos + rel` is
the only computation that is always in-bounds, since `rel` is itself
bounded by the length of the slice searched from `pos`.

### Unmappable-size fallback tests

When an AC-3 header's `fscod`/`frmsizecod` is unmappable (reserved
`fscod == 3`), `max_substream_channels` must fall back to stepping
`start + 2` bytes past the sync to re-lock onto the next genuine sync,
and must keep making forward progress doing so (never revisit the same
sync, which would loop forever, and never jump so far that it skips the
very next real frame).

- `max_substream_channels_unmappable_size_steps_forward_by_two` lays a
  bogus-sized header at absolute offset 4 (so `start == 4`,
  `start + 2 == 6`) immediately followed, at offset 6, by a real, fully
  decodable 2.0 frame — the position the `+ 2` fallback must land on
  exactly.
- `max_substream_channels_unmappable_size_at_start_steps_forward_not_back`
  puts the unmappable-size sync at absolute offset 0 (`start == 0`) so
  that stepping backward instead of forward (`start - 2`) would underflow
  rather than merely land on the wrong byte. Also proves the real frame is
  still found 6 bytes further in, confirming forward progress past the
  bogus header.

### `remap_reads_current_substream_via_and_not_or_or_xor`

`remap_audio_pids` must read a stream's CURRENT physical sub-stream id
from the low byte of its PID via `pid & 0x00FF` — not `|` or `^` with
`0x00FF`, both of which force the low byte to `0xFF` regardless of the
real PID and so always miss the "already matches" shortcut. That matters
observably when TWO physical sub-streams share the same probed channel
count: with a correct read, a stream already sitting on a matching
sub-stream is left alone (conservative, per the module's documented
behaviour); with the low byte forced to `0xFF`, `probed.get(&0xFF)` is
always `None`, so the code falls through to the "find any unclaimed
match" path and picks the FIRST (lowest-keyed, BTreeMap-ordered) matching
physical sub-stream instead — which here is a *different* sub-stream
(0x80) than the one the PID already correctly names (0x81), producing a
spurious PID change.

### `probe_and_remap_reroutes_swapped_substream_scenario_end_to_end`

End-to-end `probe_and_remap`: a real-disc-shaped MpegPs title (one
declared 5.1 AC-3 stream ordinally assigned 0x80) whose physical VOB
bytes carry the 2.0 down-mix on 0x80 and the real 5.1 on 0x81. This must
reach the `remap_audio_pids` call and re-route the stream to 0xBD81. It
also, by construction, proves each of the guards along the way lets a
real, positive case through: the content-format check must NOT bail on
`MpegPs` (only on non-`MpegPs`), the AC-3 presence check must NOT bail
when AC-3 IS present, and the sector-count check must NOT bail when the
count is nonzero — any one of those inverted would skip the probe
entirely and leave the PID at its untouched ordinal value (0xBD80), which
the test's assertion would catch.
