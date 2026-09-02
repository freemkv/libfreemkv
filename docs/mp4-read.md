# mp4 read (`src/mux/mp4/read.rs`) — internal notes

## `MAX_TRACKS`

Track count is otherwise unbounded (a crafted `moov` can pack tens of
thousands of `trak` boxes), and the per-track PID is `0x1011 + track_idx`,
which overflows `u16` past ~61k tracks. Real titles have well under a
hundred tracks.

## `MAX_SAMPLE_COUNT`

MP4 sample-table fields (`stsz` sample_count, `stts`/`ctts` run-lengths) are
untrusted 32-bit values; a crafted box can declare billions of entries in a
few bytes. Real titles stay far under this (a 10 h/60 fps track is ~2M
samples), so clamping to it caps a hostile file's allocation without
truncating any legitimate track.

## `MIN_FILE_BYTES_PER_SAMPLE`

Only `vide` and `soun` tracks are indexed here, and no real coded video or
audio access unit is anywhere near this small: the shortest legal AC-3
frame is 128 bytes, an AAC frame is hundreds, and a video sample carries at
least a NAL header plus slice data. A 2-hour title runs to thousands of
file bytes per sample, so this cannot truncate a genuine track — it only
stops a crafted sample table from claiming more samples than the file
could possibly hold.

## Test rationale (`mod tests`)

### `FakeBigReader`

A `Read + Seek` backed by a small crafted prefix followed by an endless run
of zeros, reporting `len` bytes total on `seek(End)`. Lets a test exercise
a hundred-MiB-to-multi-GiB boundary (`MAX_ALLOC_BYTES`, a sparse-file-inflated
`file_len`) without a real backing allocation of that size for the SOURCE
side — only the destination buffer the code under test allocates is real,
which is the point of the test.

### `parse_stss_reads_two_distinct_entries_from_their_own_offsets`

`parse_stss` had no direct positive-path test anywhere in this file — only
its short-buffer safety was pinned. Two real, distinct entries in a buffer
sized to exactly fit them (`b.len() == 16`, so the second entry's
`o + 4 == b.len()` exactly) separates every arithmetic mutant at once: an
`o = 8 + i*4` → `8 - i*4` slip reads the `count` field instead of the
second entry; an `i*4` → `i/4` slip re-reads the FIRST entry for the
second (integer division collapses `i=1` back to the same offset as
`i=0`); and an `o + 4 > b.len()` → `o + 4 < b.len()` slip breaks out on
the very first entry, because there IS more room after it (`12 < 16`) —
leaving the set empty instead of holding both.

### `parse_stss_count_lie_is_bounded_by_the_box_not_trusted`

A declared `count` larger than the box can hold (a lie, or a truncated
box) must be bounded by the per-entry guard, not by trusting the count —
the SAME contract `parse_stco`/`parse_stsc`/`parse_stts` pin with their
own "count lie" tests. Bounded HERE means the loop must stop, not read
past the end: an `o + 4 > b.len()` → `o - 4 > b.len()` mutant almost
never fires (`o - 4` stays small for every realistic `o`), so it lets
the loop walk straight past a truncated box into an out-of-bounds `be32`
read.

### `sample_offsets_advances_through_distinct_sizes_within_one_chunk`

`sidx` must advance by exactly one PER SAMPLE PLACED, so the third (and
every later) sample in a chunk picks up its own size, not the first
sample's reused over and over. With only two samples in a chunk this bug
is invisible — the second sample's pushed offset was already computed
from `sizes[0]` before `sidx` had a chance to matter — so this test needs
a chunk with THREE distinctly-sized samples to expose it.

### `stream_read_allows_a_sample_of_exactly_max_alloc_bytes`

`Stream::read`'s own `s.size as u64 > MAX_ALLOC_BYTES` cap — a SEPARATE
call site from `read_moov`'s (see
`read_moov_allows_a_payload_of_exactly_max_alloc_bytes` / the module doc's
note on the same policy hardened at only one of two sites elsewhere in
this crate). A `>`↔`>=` mutant here would reject a sample of exactly the
cap, which must be allowed. Builds an `Mp4Reader` directly (its fields are
private but visible to this module's tests) over a `FakeBigReader` so the
256 MiB sample doesn't need a real backing file — only the destination
buffer `read()` allocates is real.

### `stream_read_rejects_a_sample_one_byte_over_max_alloc_bytes`

The same cap's other edge: one byte OVER it must still be rejected. A
`>`↔`==` mutant would only catch a sample of EXACTLY the cap and let
anything larger through — the worse direction to get wrong, since it
turns the cap into a no-op for every real oversized/hostile sample.

### `stts_expands_runs_to_per_sample_deltas_in_order`

`stts` run-length expansion — ISO/IEC 14496-12 §8.6.1.2. The box stores
`(sample_count, sample_delta)` runs; the reader must expand them back to
one delta PER SAMPLE, in order, or every sample after the first run lands
on the wrong decode time.

(This test used to be called `stts_and_ctts_expand` while touching no
`ctts` box at all. The composition-offset half now lives in
`ctts_build_and_parse_are_exact_inverses_over_signed_offsets` and
`b_frame_presentation_order_survives_the_mp4_round_trip`.)

### `declared_track_duration_equals_frame_count_times_frame_duration`

A track's declared duration must be its real length. `mdhd.duration` is in
the MEDIA timescale and `mvhd.duration` in the movie timescale
(ISO/IEC 14496-12 §8.2.2, §8.4.2); a player uses them to draw the seek bar
and to decide when the title ends, so a zeroed or constant duration makes
a correct file unseekable and apparently empty.

### `moov_tree_carries_the_mandatory_track_header_and_media_boxes`

The `moov` tree must carry the boxes ISO/IEC 14496-12 makes mandatory for
a playable track, with the field values the spec fixes. These live in
this module rather than the writer's because the box-walking helpers
(`find_box`) are here — asserting through them means the test reads the
file the way the demuxer does, instead of re-deriving the layout.

Nothing in the demux path needs `tkhd`/`vmhd`/`smhd`/`dinf`, so an empty
one of any of them round-trips through this crate unnoticed while making
the file unplayable elsewhere.

### `table_parsers_reject_every_length_up_to_the_header_size`

Every one of these five table parsers opens with `if b.len() < 8 { return
<empty> }` before reading `count = be32(b, 4)`, which needs `b.len() >= 8`.
No existing fixture ever called any of them with a buffer shorter than 8
bytes — every test builds a complete box — so a `<`→`==` mutant of that
guard (which only early-returns at EXACTLY `b.len() == 8`, letting every
length below it fall through to the out-of-bounds `be32` read) had
nothing to fail against. Pins the entire boundary at once, every length
from 0 through 8, for all five.

### `parse_stco_co64_reads_each_byte_from_its_own_offset`

`co64`'s 8-byte offsets are decoded byte-by-byte
(`u64::from_be_bytes([b[o], b[o+1], ..., b[o+7]])`) — a SEPARATE code path
from the 32-bit `stco` case, which goes through `be32` instead and was
never itself built with a co64 fixture anywhere in this file. Every byte
here is distinct, so an index slip (off by one, negated, or scaled) that
reads the wrong byte cannot agree by coincidence.

### `parse_stsd_rejects_every_length_below_the_header_size`

Every buffer shorter than the 8-byte version+flags+entry_count header
must return `None` — never fall through to `&b[8..]`, which would slice
past the end and panic. `b.len() == 8` is the boundary itself (see the
equivalence note on `parse_stsd`'s guard); everything strictly below it
must be rejected by THIS check, not rely on a later one that isn't
reached yet.

### `parse_esds_asc_boundary_checks_are_independent`

The final guard is `asc_len == 0 || end > b.len()` — two independent
rejection reasons, not one condition that needs both. Pins each half:
  * `asc_len == 0` must be rejected even though `end == pos` is trivially
    in bounds (an `||`→`&&` mutant would let a useless zero-length ASC
    through as `Some(vec![])`);
  * `end > b.len()` means TRUNCATED, not "anything before the buffer's
    end" — trailing bytes after a complete ASC (`end < b.len()`) must
    still succeed (a `>`→`<` mutant rejects exactly this, the common case
    of an esds embedded inside a larger box with more child boxes or
    padding after it).

### `read_moov_exactly_header_sized_is_a_valid_empty_box`

`box_size < header_len` rejects a box that cannot even hold its own
header. A box whose size is EXACTLY `header_len` (8, no 64-bit largesize)
is the boundary itself: legal (an empty box), so an empty `moov` — size
8, zero payload bytes — must parse to `Ok(vec![])`, not be rejected by a
`<`→`<=`/`==` mutant that also rejects the boundary.

### `read_moov_rejects_a_box_that_overruns_the_file_via_the_or_not_and_guard`

The forward-progress / EOF guard is `box_size < header_len ||
pos.checked_add(box_size).is_none_or(|end| end > file_end)` — an `||`↔`&&`
mutant only shows up on an input where EXACTLY ONE side is true. A box
that claims to run 992 bytes past a file that ends right after its header
satisfies only the second clause (`box_size >= header_len`, so the first
is false); under `&&` the guard would not fire, and `read_moov` would
instead fail later inside `read_exact` with a plain `UnexpectedEof` — a
different, uncoded `io::Error` rather than this crate's `Mp4Invalid`
(`ErrorKind::InvalidData`), which the guard exists to produce uniformly
for every malformed box.

### `read_moov_allows_a_payload_of_exactly_max_alloc_bytes`

`payload_len > MAX_ALLOC_BYTES` is a SEPARATE, sparse-file-proof cap from
the EOF check above (see
`read_moov_over_cap_rejected_despite_inflated_file_len`). A `>`→`>=`
mutant would reject a `moov` whose payload is EXACTLY `MAX_ALLOC_BYTES`,
which must be allowed.

### `edit_list_media_time_shifts_the_presentation_timeline`

Regression (silent A/V desync): the sample timeline was built purely from
stts/ctts starting at tick 0 and no `edts`/`elst` was ever parsed, so the
presentation timeline an edit list defines was discarded. A non-empty
edit with `media_time = 1024` — the standard way encoder delay is
expressed — must move the track's presentation, not be ignored.

### `parse_elst_v1_entry_bytes_come_from_their_own_offsets`

Every byte of a version-1 entry's `segment_duration` (8 bytes) and
`media_time` (8 bytes) at its own offset — not a neighbour's. The
existing fixtures elsewhere use `5_000u64` and `-1i64`, which are almost
all zero/`0xFF` bytes, so an index slip that reads an adjacent byte (or a
header byte outside the entry) often reads the SAME value and the test
cannot tell. Every byte here is distinct and nonzero, and the header
bytes are zero, so any wrong index — off by one, negated, or scaled —
pulls in a value that cannot match by coincidence.

### `elst_offset_ticks_stays_safe_at_zero_empty_ticks_even_with_a_zero_movie_timescale`

`Some(mts) if empty_movie_ticks > 0` and the `None` branch's `if
empty_movie_ticks > 0` are both guards around a `tracing::warn!`/delay
computation that, at `empty_movie_ticks == 0`, must produce the exact
same answer as skipping it (0 ticks of delay — there is no empty edit to
convert). The one case where the two are NOT interchangeable is
`movie_timescale == Some(0)`: skipping the arithmetic returns 0, but
entering it divides by that zero timescale and panics. Real callers
never pass this — `from_reader` filters `mvhd`'s timescale through
`.filter(|&t| t != 0)` before it ever reaches this function (see
`mdhd_timescale_zero_does_not_divide_by_zero` for the sibling guard on
the media timescale) — but the function itself has to stay panic-free at
its own boundary, independent of what its one caller happens to do
today, because `libfreemkv` runs inside the long-lived `autorip` service
and a panic there is downtime.

### `audio_trak_hostile_count`

A crafted file with a *fixed-size* `stsz` (sample_size != 0) claiming
count = 0xFFFFFFFF must not inflate the sample index past the file's own
byte length. `from_reader` sets `sample_budget =
MAX_SAMPLE_COUNT.min(file_len)`, so a few-hundred-byte file bounds the
`Vec<SampleRef>` to a few hundred — NOT the 16M `MAX_SAMPLE_COUNT`
ceiling. Mutation check: revert the budget to a bare `MAX_SAMPLE_COUNT`
and this file yields ~16M samples, failing the `<= file_len` (and `<
MAX_SAMPLE_COUNT`) assertions below. Built by a minimal-but-complete
audio `trak` whose fixed-size `stsz` LIES about its sample count
(`u32::MAX`), with an stsc/stts wide enough to place whatever count
survives the budget.

### `sample_index_ram_is_bounded_by_a_multiple_of_the_file`

The RAM amplification the byte-per-sample budget actually bounds: a
small crafted file whose `stsz` claims `u32::MAX` samples must not force
an eager multi-hundred-MB index. At ~60 bytes of RAM per indexed sample,
a budget of one sample per file byte gave ~60x the input size; the
assertion pins the amplification factor rather than a raw count, so it
fails if the budget ever goes back to counting samples per byte.

### `audio_trak_missing`

Build an audio `trak` identical to `audio_trak(48_000)` but with the
named stbl child box omitted. Used to reach the untrusted-input guards
that drop a track whose `stsz` says samples exist yet whose `stco`/`co64`
(chunk offsets) or `stsc` (sample-to-chunk map) is missing — without such
a table every sample offset would resolve near file byte 0.

### `missing_stco_drops_track_all_dropped_is_invalid`

A track with samples (`stsz`) but no chunk-offset table (`stco`/`co64`)
must be DROPPED, not indexed with offsets that resolve near file byte 0.
With it the only track, the whole file fails `Mp4Invalid`. Mutation
check: delete the `if chunk_offsets.is_empty() { continue; }` guard and
`from_reader` returns `Ok` (garbage samples), flipping this to FAIL.

### `missing_stsc_drops_track_all_dropped_is_invalid`

A track with samples (`stsz`) and chunk offsets (`stco`) but no
sample-to-chunk map (`stsc`) must be DROPPED — without `stsc` the
samples cannot be placed against the chunk offsets and would pack from
byte 0. Mutation check: delete the `if stsc.is_empty() { continue; }`
guard and `from_reader` returns `Ok`, flipping this to FAIL.

### `missing_stts_drops_track_all_dropped_is_invalid`

A track with samples (`stsz`) but no time-to-sample table (`stts`,
mandatory per ISO/IEC 14496-12 §8.6.1) must be DROPPED — without it every
sample takes dur=0, collapsing the whole track onto one instant
(all-zero timestamps). With it the only track, the file fails
`Mp4Invalid`. Mutation check: delete the `if durations.is_empty() {
continue; }` guard and `from_reader` returns `Ok` (all-zero-timestamp
samples), flipping this to FAIL.

### `stsc_placing_fewer_samples_than_stsz_drops_the_track`

An `stsc` that PASSES the non-empty guard but places fewer samples than
`stsz` declares must drop the track. `sample_offsets` used to pack the
unplaced tail after the last known offset, inventing a position, so
those frames were read from arbitrary file bytes — the same "emit
garbage" outcome the stco/stsc presence guards exist to refuse.

Mutation check: restore the trailing `while offsets.len() < sizes.len()`
pack-after-last loop and this file indexes 3 samples instead of erroring.

### `short_stts_drops_the_track_like_an_absent_one`

A SHORT `stts` — present and non-empty, but covering fewer samples than
`stsz` declares — must drop the track just like an absent one. The tail
samples took `dur = 0`, collapsing the whole tail onto a single
timestamp, which is the exact degenerate timing the absent-stts guard
refuses.

Mutation check: weaken the guard back to `durations.is_empty()` and this
file indexes 3 samples whose last two share one timestamp.

### `mdhd_language_boundary_rejects_short_not_merely_non_exact`

The length guard is `b.len() < off + 2` — reject too SHORT, not "not
exactly `off + 2`". Pins both edges of that boundary for the version-0
offset (`off == 20`):
  * `b.len() == off` (no room for the packed field at all) must return
    `None`, not read two bytes past the end;
  * `b.len() == off + 3` (one byte MORE than the minimum) must still
    succeed — a `<`→`>` mutant of the guard would reject this case, since
    `off + 3 > off + 2`.

### `parse_stsd_takes_height_from_its_own_field_not_the_width_beside_it`

`height` is the SECOND of the two 16-bit dimensions in a
VisualSampleEntry, at byte 26 — width sits at 24. Reading the wrong one
is silent: the value is still a plausible dimension, so the track's
seeded resolution simply comes out wrong (1920 read as a height would
classify a 1080p title as UHD). The two are deliberately different here;
a fixture with width == height would pass under either offset.

### `parse_stsd_reads_channelcount_from_its_own_field_and_defaults_a_short_entry`

`channelcount` is at byte 16 of an AudioSampleEntry, after the 8 reserved
bytes that follow `data_reference_index`. An offset slip reads a reserved
field, and reserved fields are conventionally zero — which would make
every audio track come out as 0 channels rather than fail. The
short-entry fallback of 2 is pinned in the same test: an entry with no
room for the fixed part still has to name SOME channel count, and 0 is
not a usable one.

### `parse_stsd_extracts_aac_codec_private_only_when_the_body_is_long_enough`

`mp4a`'s codec_private (the AudioSpecificConfig inside a child `esds`
box) is only read when the entry is `Codec::Aac` AND its body is at
least the 28-byte fixed AudioSampleEntry part — `find_box(&body[28..],
...)` would slice past the end otherwise. No existing test built an
actual `mp4a` entry, so neither half of that guard (`&&`, or the `>= 28`
on its right) was constrained: an `&&`→`||` mutant reaches the same
`&body[28..]` on a codec-only match, and a `>=`→`<` mutant loses a real
title's AAC CodecPrivate outright by only running on entries too short
to have one.

### `parse_stsd_recognises_every_audio_fourcc`

Every recognised audio fourcc must map to its own `Codec`, checked one by
one. `ec-3`/`dtsc`/`dtse`/`dtsh`/`dtsl` had no coverage at all before
this test — a deleted match arm for any of them falls through to the
catch-all `_ => return None`, silently dropping every E-AC-3 or DTS
variant track from the title instead of remuxing it.

### `read_descriptor_len_is_a_four_byte_base_128_varint`

A descriptor length is a base-128 varint: 7 bits per byte, continued
while the top bit is set, to a maximum of FOUR bytes. Every existing
esds fixture uses a single-byte length, so the continuation path is
unconstrained by them — yet a multi-byte length is exactly what an
`esds` carrying a long AudioSpecificConfig (or one written by a tool
that always pads to 4 bytes, which is common) uses.

### `parse_esds_asc_steps_over_every_optional_es_descriptor_field`

The optional `ES_Descriptor` fields (ISO/IEC 14496-1 §7.2.6.5) are
selected by three flag bits, and each one that is set inserts bytes
before the `DecoderConfigDescriptor`. Skipping them wrongly does not
corrupt anything — the tag check fails and `parse_esds_asc` returns
`None`, so the AAC track simply loses its CodecPrivate and the remux
emits AAC no decoder can initialise.

The existing fixture has flags = 0, so all three skips are
unconstrained. This one sets all three at once and still has to reach
the same ASC.

### `find_boxes_capped_refuses_a_box_smaller_than_its_own_header`

A box header is 8 bytes, so a declared `size` below 8 cannot describe a
box — and taking it at face value slices `payload[pos + 8 .. pos +
size]` with the start past the end, which panics. A crafted `moov` is
untrusted input read straight off a user's file.

### `find_boxes_capped_decodes_the_size_field_from_its_own_bytes`

The declared box size is decoded from four specific bytes
(`payload[pos..pos+4]`, big-endian). A size deliberately chosen so byte
1 differs from byte 0 catches an index slip that rereads byte 0 (or any
other wrong offset) instead of the byte the field actually occupies —
with a small size every byte but the last is zero, so such a slip would
go unnoticed.

### `sample_offsets_clamps_an_stsc_run_that_outruns_the_chunk_table`

An `stsc` entry names a `first_chunk` that may exceed the chunk count
the `stco` actually declares (a truncated or crafted table). The run it
would fill has to be clamped to the chunks that exist — indexing `spc`
past its length is a panic on a file the user merely opened.

The clamp is only reachable through a NON-final entry: the final entry's
end is `n_chunks` by construction, so a fixture whose only over-range
entry is last never reaches the line.

### `elst_offset_ticks_honours_the_first_media_edit_not_the_last`

ISO/IEC 14496-12 §8.6.6: an edit list may hold several media edits. This
frame model can only express a constant shift, so it honours the
LEADING one and logs the rest — taking the last instead would shift the
whole track by a trim that belongs to a later segment, i.e. silent A/V
desync of exactly the size of the difference.

### `mvhd_timescale_version_1_reads_past_the_64_bit_times`

A version-1 `mvhd` carries 64-bit creation/modification times, so its
`timescale` sits at byte 20 rather than 12 (ISO/IEC 14496-12 §8.2.2).
Every existing fixture is version 0, so the version-1 offset is
unconstrained by them — and it is not a hypothetical: writers emit
version 1 whenever the movie duration does not fit 32 bits.

A wrong offset reads part of the 64-bit modification time, which is a
large arbitrary number — and the movie timescale is the denominator
that converts an empty edit's delay into media ticks, so the A/V offset
it produces is arbitrary too.

### `parse_elst_entry_count_is_capped`

Only the leading empty edits and the FIRST media edit shape the offset,
so `MAX_ELST_ENTRIES` bounds what a crafted `elst` can allocate. Without
it a box declaring millions of entries — and carrying the bytes for
them, inside a `moov` already capped at 256 MiB — expands to a Vec of
20-byte tuples for no benefit at all.

### `non_av_handler_track_is_dropped_not_folded_into_audio`

A `hdlr` of anything other than `vide`/`soun` (e.g. a hint or subtitle
track) must be dropped, not folded into the audio branch. Before this
guard existed as a literal `&h == b"soun"` comparison, weakening it to
"anything that isn't vide" would silently turn a hint track's `stsd`
entry into a fabricated `AudioStream` — with whatever codec its sample
entry happened to name.

### `per_track_pid_arithmetic_is_exact_past_the_first_track`

The per-track PID is `0x1011 + track_idx` for video and `0x1100 +
track_idx` for audio. At `track_idx == 0` a `+`↔`-`/`*` mutant of either
is invisible (`0x1011 + 0 == 0x1011 - 0 == 0x1011 * 0`… no — `* 0` is 0,
already different — but `+`↔`-` specifically needs `track_idx != 0` to
separate). Four tracks (two video, two audio) push `track_idx` to 1 and
3 respectively, so every one of `+`→`*`, `+`→`-` is forced to disagree
with the correct PID on the second track of its kind.

### `track_idx_advances_past_a_sample_less_track`

A track whose `stsz` is absent (`n == 0`) still occupies a `track_idx`
slot and must still advance the counter for the NEXT track — otherwise
two tracks collide on the same PID, or (worse, if the increment
direction is reversed) `track_idx` underflows a `usize` and panics on
the very first sample-less track.

### `sample_budget_is_shared_and_exhausted_across_tracks`

The global `sample_budget` is DECREMENTED by each track's real sample
count, and it is SHARED across tracks — a track late in the file gets
only what earlier tracks left behind. Build two tracks that both LIE
about their sample count (`stsz` fixed-size, count = `u32::MAX`, as in
`audio_trak_hostile_count`): the file is tiny, so the first track's
clamped count exactly exhausts the whole initial budget, and the second
track — clamped against what's left — must come back with ZERO samples.
An `+=`/`/=` mutant of the decrement either grows the budget or leaves
it nonzero, letting the second track claim samples it must not have.

## `elst_offset_ticks`

Presentation-time offset an edit list imposes on a track's samples, in the
track's MEDIA timescale ticks (ISO/IEC 14496-12 §8.6.5-§8.6.6).

Two constructs cover essentially every real edit list, and both reduce to a
constant shift of the whole track:
  * an EMPTY edit (`media_time == -1`) before the media edit, whose
    `segment_duration` — in MOVIE timescale ticks — delays presentation;
  * a non-empty edit whose `media_time` trims that much media off the front.

So the offset is `(sum of leading empty segment_durations) - media_time`.
A list with several non-empty edits, or a non-empty edit at a rate other
than 1, describes a timeline this frame model cannot express (it would
need samples dropped, reordered or repeated); the leading edit is still
honoured, and the part that is not is LOGGED rather than passed off as a
faithful copy.

## `MAX_ALLOC_BYTES`

The EOF check alone is not enough: `file_len` is cheaply inflatable with a
sparse file (`truncate -s 8G`), so a crafted stsz size or moov box size
just under an 8 GiB apparent length would otherwise force a multi-GiB
allocation. No real sample or moov approaches this.
